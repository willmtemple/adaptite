use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::rc::{Rc, Weak};
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::error::Error;
use core::fmt;
use core::panic::Location;
use core::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use hashbrown::{HashMap, HashSet};

use runite::queue_microtask;

use crate::stats::{FlushAccounting, FlushStats, GraphCounters};
use crate::{
    DiagnosticEvent, DiagnosticSubscription, InvalidationCause, InvalidationLevel, NodeId,
    NodeKind, ReactorId, trace_targets,
};

type Job = Box<dyn FnOnce() + 'static>;
type DiagnosticCallback = Rc<dyn Fn(DiagnosticEvent)>;

static NEXT_REACTOR_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_REACTOR: RefCell<Weak<ReactorInner>> = const { RefCell::new(Weak::new()) };
    /// Strong reference held for the lifetime of an [`EnterGuard`], so an explicitly entered
    /// reactor stays the thread default even when the caller holds no other handle.
    static ANCHORED_REACTOR: RefCell<Option<Rc<ReactorInner>>> = const { RefCell::new(None) };
    /// Whether this thread has *ever* had a default reactor, by any route — an explicit
    /// [`Reactor::enter`] or an implicit install by [`Reactor::current`].
    ///
    /// This, rather than a count of implicit installs, is what makes the warning cover the case
    /// a UI framework actually hits. A framework that scopes `enter` to renders and callbacks —
    /// the correct thing for it to do — leaves the thread with *no* default in between, so a
    /// signal created from a timer, a task, a `Drop`, or a test body is a **first** implicit
    /// install on an empty slot rather than a replacement of an expired one. Counting installs
    /// misses every one of those; remembering that a default once existed catches them all,
    /// while a thread that never entered (a script, a doctest) stays quiet.
    static HAS_HAD_DEFAULT: Cell<bool> = const { Cell::new(false) };
    static UNTRACKED_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[cfg(debug_assertions)]
thread_local! {
    /// Pointer identity of the reactor whose computation is currently on top of the call stack.
    /// Used to detect reads of one reactor's nodes from inside another reactor's computation.
    static RUNNING_REACTOR: Cell<*const ()> = const { Cell::new(core::ptr::null()) };
}

/// Returns the current thread's default reactor, installing one if there is none.
///
/// See [`Reactor::current`] for the installation rules, and [`try_current`] for the
/// non-installing variant.
pub fn current() -> Reactor {
    Reactor::current()
}

/// Returns the current thread's default reactor, or `None` if no reactor is installed.
///
/// Unlike [`current`], this never installs one. Use it when landing on a fresh, unflushed graph
/// would be a bug rather than a convenience — see [`Reactor::enter`].
pub fn try_current() -> Option<Reactor> {
    Reactor::try_current()
}

/// Restores the previous thread-default reactor when dropped.
///
/// Created by [`Reactor::enter`]. While alive, the guard holds a strong reference to the entered
/// reactor, so it cannot expire and be silently replaced by a fresh graph.
///
/// The guard is not `Send`: a reactor and its default installation are thread-local.
pub struct EnterGuard {
    previous_default: Weak<ReactorInner>,
    previous_anchor: Option<Rc<ReactorInner>>,
}

impl fmt::Debug for EnterGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnterGuard").finish_non_exhaustive()
    }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        // `try_with`, never `with`/`replace`: a host that parks its reactor handle in a
        // thread-local — the shape `Reactor::current`'s own warning recommends — releases this
        // guard from a thread-local destructor, and destructors run in reverse registration order,
        // so these two slots are routinely destroyed first. `LocalKey::replace` *panics* then, and
        // a panic escaping a `Drop` during thread shutdown is a non-unwinding abort: the process
        // dies with SIGABRT on the way out of `main`. Failing to restore a default nobody can look
        // up again costs nothing; aborting costs everything. Same trade as
        // `scope::with_owner_entry` and `ownership::with_counters`.
        let _ = CURRENT_REACTOR
            .try_with(|slot| slot.replace(core::mem::take(&mut self.previous_default)));
        let _ = ANCHORED_REACTOR.try_with(|slot| slot.replace(self.previous_anchor.take()));
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "reactor_exit",
            "restored the previous thread default reactor"
        );
    }
}

/// Runs `f` with dependency tracking suspended.
///
/// Reads made while `f` executes do not record dependencies for the currently running observer,
/// so the observer will not re-run when those values change. Cycle detection remains active.
/// Tracking resumes when `f` returns; nested `untrack` calls are permitted.
///
/// Untracked reads are also exempt from the debug-build cross-reactor check, making `untrack`
/// the sanctioned way to read one reactor's nodes from inside another reactor's computation.
///
/// # Examples
///
/// ```rust
/// use adaptite::{signal, thunk, untrack};
///
/// let count = signal(0);
/// let label = signal("count");
///
/// let display = thunk({
///     let count = count.clone();
///     let label = label.clone();
///     move || format!("{}: {}", untrack(|| label.get()), count.get())
/// });
///
/// assert_eq!(display.get(), "count: 0");
///
/// // `label` is not a dependency: changing it does not invalidate the thunk.
/// label.set("hits");
/// assert_eq!(display.get(), "count: 0");
///
/// // The next genuine recomputation observes the new label.
/// count.set(3);
/// assert_eq!(display.get(), "hits: 3");
/// ```
pub fn untrack<T>(f: impl FnOnce() -> T) -> T {
    // `try_with` throughout: the crate runs consumer callbacks untracked — cleanups among them —
    // so this is reachable from a `Drop` and therefore from a thread-local destructor. `with`
    // panics once `UNTRACKED_DEPTH` is destroyed, and the panic from the `Drop` guard below would
    // arrive while already unwinding, which aborts. An increment that never happened must not be
    // decremented, so the guard records whether it took effect.
    let entered = UNTRACKED_DEPTH
        .try_with(|depth| depth.set(depth.get() + 1))
        .is_ok();

    struct Guard {
        entered: bool,
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            if !self.entered {
                return;
            }
            let _ = UNTRACKED_DEPTH.try_with(|depth| depth.set(depth.get().saturating_sub(1)));
        }
    }

    let _guard = Guard { entered };
    f()
}

fn is_untracked() -> bool {
    // A destroyed slot reads as "tracking", which is the conservative answer: it records an edge
    // rather than silently dropping one. Nothing observes either outcome during teardown.
    UNTRACKED_DEPTH
        .try_with(|depth| depth.get() > 0)
        .unwrap_or(false)
}

/// How stale an observer has become after an upstream write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Mark {
    /// A transitive dependency may have changed; the observer must verify its direct
    /// dependencies before recomputing.
    Check,
    /// A direct dependency definitely changed; the observer must recompute.
    Dirty,
}

/// Staleness of a computed node or effect, ordered from freshest to stalest.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum State {
    Clean,
    Check,
    Dirty,
}

impl From<Mark> for State {
    fn from(mark: Mark) -> Self {
        match mark {
            Mark::Check => State::Check,
            Mark::Dirty => State::Dirty,
        }
    }
}

impl From<Mark> for InvalidationLevel {
    fn from(mark: Mark) -> Self {
        match mark {
            Mark::Check => Self::Check,
            Mark::Dirty => Self::Dirty,
        }
    }
}

/// Unwind guard used by computed nodes: restores the dirty mark when a compute closure panics,
/// so the node is retried on its next read instead of being treated as clean.
pub(crate) struct DirtyOnUnwind<'a> {
    pub(crate) state: &'a Cell<State>,
    pub(crate) armed: bool,
}

impl Drop for DirtyOnUnwind<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.state.set(State::Dirty);
        }
    }
}

pub(crate) trait ObserverHook {
    /// Records that this observer's inputs may have changed.
    fn mark(&self, mark: Mark, cause: Option<InvalidationCause>);

    /// Brings a computed node up to date, recomputing if its inputs actually changed.
    ///
    /// The default implementation is a no-op; it is used by observers that are never read as
    /// dependencies (effects).
    fn refresh(&self) {}

    /// Reports how stale this observer currently is, for [`Reactor::graph_snapshot`].
    ///
    /// Staleness lives on each node's own inner struct rather than in the reactor's maps, so a
    /// snapshot has to ask. Read-only, and must not recompute: an inspection that brought nodes
    /// up to date would change the thing it is inspecting.
    fn state(&self) -> State;
}

/// Error type for cycles detected in the reactive graph. Contains the path of nodes that form the cycle.
///
/// Returned by [`Reactor::try_observe`]. The panicking read paths ([`Reactor::observe`], and
/// thunk/memo reads) panic with this error's `Display` output rather than the error value
/// itself.
///
/// # Examples
///
/// ```rust
/// use adaptite::{Reactor, source_in};
///
/// let reactor = Reactor::new();
/// let node = source_in(&reactor);
///
/// // Reading a node from inside its own computation closes a cycle.
/// let error = reactor
///     .run_in_context(node.id(), || reactor.try_observe(node.id()))
///     .expect_err("self-read is a cycle");
///
/// // The path starts and ends with the node that closed the cycle, and each entry
/// // carries the source location where that node was created.
/// assert_eq!(error.cycle().first(), error.cycle().last());
/// assert!(error.origins().iter().all(|origin| origin.is_some()));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactCycleError {
    cycle: Vec<NodeId>,
    origins: Vec<Option<&'static Location<'static>>>,
}

impl ReactCycleError {
    fn new(cycle: Vec<NodeId>, origins: Vec<Option<&'static Location<'static>>>) -> Self {
        Self { cycle, origins }
    }

    /// Returns the cycle path that was detected.
    ///
    /// The path starts and ends with the same node — the one whose read closed the cycle.
    pub fn cycle(&self) -> &[NodeId] {
        &self.cycle
    }

    /// Returns the source locations where the nodes in the cycle path were created, in the same
    /// order as [`cycle`](Self::cycle). An entry is `None` when the node has already been
    /// disposed.
    pub fn origins(&self) -> &[Option<&'static Location<'static>>] {
        &self.origins
    }
}

impl fmt::Display for ReactCycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reactive cycle detected: ")?;
        for (index, node) in self.cycle.iter().enumerate() {
            if index != 0 {
                write!(f, " -> ")?;
            }
            write!(f, "{node}")?;
            if let Some(origin) = self.origins.get(index).copied().flatten() {
                write!(f, " (created at {origin})")?;
            }
        }
        Ok(())
    }
}

impl Error for ReactCycleError {}

/// Single-threaded coordinator for a reactive graph.
///
/// A reactor tracks dependency edges between reactive nodes, manages the currently executing
/// observer stack, and schedules deferred jobs onto the runtime microtask queue.
///
/// Most programs never touch a reactor directly: the free constructors ([`crate::signal`],
/// [`crate::thunk`], [`crate::memo`], [`crate::effect`], [`crate::event`]) use the thread's
/// default reactor from [`current`]. Construct explicit reactors (and use the `*_in`
/// constructor variants) to keep several independent graphs on one thread. Nodes from
/// different reactors must not observe each other; debug builds panic on such reads.
///
/// Nodes hold their reactor alive, so dropping a `Reactor` handle does not tear down the graph.
///
/// # Examples
///
/// ```rust
/// use adaptite::{Reactor, memo_in, signal_in};
///
/// let reactor = Reactor::new();
///
/// let celsius = signal_in(&reactor, 20.0f64);
/// let fahrenheit = memo_in(&reactor, {
///     let celsius = celsius.clone();
///     move || celsius.get() * 9.0 / 5.0 + 32.0
/// });
///
/// assert_eq!(fahrenheit.get(), 68.0);
/// celsius.set(25.0);
/// assert_eq!(fahrenheit.get(), 77.0);
/// ```
#[derive(Clone)]
pub struct Reactor {
    pub(crate) inner: Rc<ReactorInner>,
}

impl Reactor {
    /// Creates a new empty reactor.
    pub fn new() -> Self {
        let reactor = Self {
            inner: Rc::new(ReactorInner::new()),
        };
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "reactor_new",
            "created reactive reactor"
        );
        reactor
    }

    /// Subscribes to this reactor's reactive causality events.
    ///
    /// Unlike the `tracing` instrumentation, these events are emitted in
    /// release builds as well as debug builds.
    ///
    /// The stream is dormant when there are no subscribers. Delivery is
    /// synchronous on this reactor's thread; callbacks should append to a
    /// trace sink and return without reading or mutating the graph.
    pub fn subscribe_diagnostics(
        &self,
        callback: impl Fn(DiagnosticEvent) + 'static,
    ) -> DiagnosticSubscription {
        let token = self.inner.next_diagnostic.get();
        self.inner.next_diagnostic.set(token.wrapping_add(1).max(1));
        self.inner
            .diagnostics
            .borrow_mut()
            .push((token, Rc::new(callback)));
        self.inner.diagnostics_active.set(true);
        // The subscription keeps the graph alive. This matters when a tool
        // subscribes to the thread-default reactor before its first node is
        // created; otherwise the default reactor's weak cache would expire
        // between subscription and node construction.
        let inner = Rc::clone(&self.inner);
        DiagnosticSubscription::new(move || {
            inner
                .diagnostics
                .borrow_mut()
                .retain(|(candidate, _)| *candidate != token);
            let still_active = !inner.diagnostics.borrow().is_empty();
            inner.diagnostics_active.set(still_active);
            if !still_active {
                // Drop the part-accumulated flush so a later subscriber never inherits totals
                // from a window it could not observe.
                inner.flushes.reset();
            }
        })
    }

    /// Returns the current thread's default reactor, installing one if there is none.
    ///
    /// The free constructors ([`crate::signal`], [`crate::effect`], and friends) call this, so
    /// this is the reactor that reactive state created outside any explicit reactor lands on.
    ///
    /// # Installation and lifetime
    ///
    /// The thread's default is cached weakly: it lives only as long as some node, `Reactor`
    /// handle, or [`EnterGuard`] keeps it alive. When nothing does, the next call installs a
    /// *fresh, unrelated* reactor — and nodes created before and after that point cannot observe
    /// each other. Because writes to a node on an unflushed graph mark dependents stale without
    /// ever scheduling anything, the symptom is "this value changes and nothing reacts", with no
    /// panic and nothing pointing at the cause.
    ///
    /// So: **an implicit install logs at `warn` on the `adaptite::graph` target whenever this
    /// thread has had a default reactor at any earlier point**, by any route. The first install
    /// on a thread that has never entered one stays at `debug`, so scripts, doctests and tests
    /// that simply want a graph are not nagged.
    ///
    /// The distinction matters more than it looks. A framework that scopes [`enter`](Self::enter)
    /// to renders and callbacks — the correct thing for it to do — leaves the thread with *no*
    /// default in between, so state created from a timer, a task, a `Drop`, or a test body is a
    /// first install on an empty slot rather than a replacement of an expired one. Warning only
    /// about expiry would miss every one of those, which is the common case.
    ///
    /// Applications that own a long-lived graph should not rely on that cache at all. Hold the
    /// reactor alive explicitly with [`enter`](Self::enter), which anchors it as the thread
    /// default for the guard's lifetime, and reach for [`try_current`](Self::try_current) where
    /// a missing reactor should be an error rather than a new graph.
    pub fn current() -> Self {
        if let Some(reactor) = Self::try_current() {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::GRAPH,
                event = "current_reactor_reuse",
                "reusing current thread default reactor"
            );
            return reactor;
        }

        let reactor = Self::new();
        // `try_with` for both slots. `try_current` above returns `None` once `CURRENT_REACTOR` is
        // destroyed, so thread-local teardown funnels straight into this branch: hardening only
        // the read would relocate the abort here rather than remove it. The `Err` case changes
        // more than bookkeeping, so: the reactor is fine either way — an `Rc` graph with no
        // thread-local state — and only *caching* it as the thread default becomes impossible,
        // which costs nothing on a thread that will never look the default up again. The warning
        // is suppressed there too, since it would be pure noise.
        let installed = CURRENT_REACTOR
            .try_with(|slot| slot.replace(Rc::downgrade(&reactor.inner)))
            .is_ok();
        let had_default = HAS_HAD_DEFAULT
            .try_with(|flag| flag.replace(true))
            .unwrap_or(false);
        if installed && had_default {
            // This thread has had a default before and does not have one now, so whatever owned
            // it is out of scope. Nothing is broken *yet* — but nodes created from here on are on
            // a different graph than the ones created before, and the two can never interact:
            // reads and writes will work perfectly and nothing will ever re-render.
            tracing::warn!(
                target: trace_targets::GRAPH,
                event = "current_reactor_reinstall",
                reactor_id = reactor.inner.id.get(),
                "this thread had a default reactor earlier and has none now, so a fresh, \
                 unrelated one was installed; nodes created from here on are on a separate graph \
                 from the ones created before. Hold the reactor alive with Reactor::enter for as \
                 long as ambient constructors may run, create state with reactor.signal(..) and \
                 friends, or use try_current to make the absence an error"
            );
        } else {
            tracing::debug!(
                target: trace_targets::GRAPH,
                event = "current_reactor_install",
                "installed current thread default reactor"
            );
        }
        reactor
    }

    /// Returns the current thread's default reactor, or `None` if none is installed.
    ///
    /// Unlike [`current`](Self::current), this never installs one, so it distinguishes "the
    /// application's reactor" from "a fresh graph nobody flushes" — a distinction the caller
    /// otherwise cannot make.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::Reactor;
    ///
    /// let reactor = Reactor::new();
    /// let _guard = reactor.enter();
    /// let current = Reactor::try_current().expect("the entered reactor is the thread default");
    /// assert_eq!(current.id(), reactor.id());
    /// ```
    pub fn try_current() -> Option<Self> {
        // `try_with`: this is reachable from a `Drop`, and therefore from a thread-local
        // destructor, by which point `CURRENT_REACTOR` may already have been destroyed, where
        // `with` panics and the panic aborts. `None` is the honest answer rather than a swallowed
        // error: once the slot is gone the thread has no reachable default and never will again,
        // which is exactly what `None` means to every caller.
        CURRENT_REACTOR
            .try_with(|r| r.borrow().upgrade())
            .ok()
            .flatten()
            .map(|inner| Self { inner })
    }

    /// Installs this reactor as the thread's default until the returned guard drops.
    ///
    /// The guard holds a *strong* reference, so "the current reactor" becomes a fact for its
    /// lifetime rather than a race with whoever happens to hold the last handle. This is what a
    /// host framework should do once, for the lifetime of the application: reactive state created
    /// outside any component — a registry of long-lived signals, say — then provably joins the
    /// same graph the framework flushes, instead of joining it by coincidence.
    ///
    /// Entering nests. Dropping the guard restores whatever default was installed before it,
    /// including none. Guards must be dropped in reverse order of creation; dropping them out of
    /// order restores an older default and is a bug, though not an unsound one.
    ///
    /// **The guard must be bound.** `reactor.enter();` in statement position, or
    /// `let _ = reactor.enter();`, drops it at the end of that statement and installs nothing —
    /// every ambient constructor afterwards lands on a *different* graph, reads and writes work
    /// perfectly, and nothing ever re-renders. `#[must_use]` catches the first form; nothing can
    /// catch the second, since `let _ =` is how one deliberately discards a `must_use` value.
    /// Bind it: `let _guard = reactor.enter();`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::{Reactor, signal};
    ///
    /// let reactor = Reactor::new();
    /// let guard = reactor.enter();
    ///
    /// // Created far from any component, with no handle to the reactor in scope.
    /// let title = signal(String::from("shell"));
    /// title.set(String::from("editor"));
    /// assert_eq!(title.with(|title| title.clone()), "editor");
    ///
    /// drop(guard);
    /// ```
    #[must_use = "the reactor is the thread default only while the guard is held; \
                  `reactor.enter();` in statement position installs nothing"]
    pub fn enter(&self) -> EnterGuard {
        // `try_with` for the same reason `EnterGuard::drop` uses it: a host may enter a reactor
        // from teardown code running in a thread-local destructor. An entry that could not be
        // recorded leaves the guard with nothing to restore, which is what `Weak::new()`/`None`
        // already mean, so the guard needs no extra state to stay correct.
        let previous_default = CURRENT_REACTOR
            .try_with(|slot| slot.replace(Rc::downgrade(&self.inner)))
            .unwrap_or_default();
        let previous_anchor = ANCHORED_REACTOR
            .try_with(|slot| slot.replace(Some(Rc::clone(&self.inner))))
            .unwrap_or_default();
        // Entering counts as the thread having had a default, so that ambient state created after
        // the guard drops is reported. Without this the warning only covers a default that
        // expired, and misses the far more common case of one that is simply not held right now.
        let _ = HAS_HAD_DEFAULT.try_with(|flag| flag.set(true));
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "reactor_enter",
            reactor_id = self.inner.id.get(),
            "entered reactor as the thread default"
        );
        EnterGuard {
            previous_default,
            previous_anchor,
        }
    }

    /// Runs `f` in the dependency-tracking scope of `observer`.
    ///
    /// Existing dependencies for `observer` are cleared before `f` runs. Any calls to
    /// [`observe`](Self::observe) made while `f` executes will become the observer's new
    /// dependencies.
    ///
    /// Contexts nest: dependencies are recorded for the innermost observer. Debug builds
    /// assert that `observer` is not already running further up the stack.
    pub fn run_in_context<T>(&self, observer: NodeId, f: impl FnOnce() -> T) -> T {
        let _span = tracing::debug_span!(
            target: trace_targets::GRAPH,
            "reactor.run_in_context",
            observer_id = observer.0
        )
        .entered();
        // Checked before anything is mutated, and checked in *every* build. The insert happens
        // in release regardless — only the assertion was being stripped — so this costs nothing
        // and replaces a silent corruption with a diagnosis: re-entering a running observer used
        // to fall through to `clear_observer_dependencies` below, wiping the dependency set of a
        // computation that was still using it, and the node would emerge with whatever subset of
        // its inputs it happened to re-read.
        let inserted = self.inner.active_computations.borrow_mut().insert(observer);
        assert!(
            inserted,
            "adaptite: a reactive computation re-entered itself, which cannot be tracked \
             coherently — the inner run would clear the dependencies the outer run is still \
             recording. This usually means a computation, or a callback it invoked, wrote state \
             it depends on and then forced a synchronous flush"
        );
        self.clear_observer_dependencies(observer, true);
        self.inner.stack.borrow_mut().push(observer);

        // Entering a computation starts a fresh tracking scope. `untrack` says "do not record
        // this read for whoever is currently observing" — it must not mean "and also record
        // nothing for any node that happens to recompute inside it". Without this, a computed
        // node first refreshed inside an untracked region records *zero* dependencies, settles
        // clean, and is never invalidated again: silently and permanently stale. That is
        // reachable from ordinary code, because the crate itself runs consumer callbacks
        // untracked — `watch` and `Event` handlers, cleanups, comparators, `Resource` fetches —
        // and any of them may be the first to read a stale memo.
        //
        // `try_with` on both slots, here and in the guard below: an effect or memo can be driven
        // from a cleanup, so this is reachable from a `Drop` and therefore from a thread-local
        // destructor. `with` panics once the slot is destroyed, and the guard's matching panic
        // would arrive during unwinding, which aborts. `None` means nothing was saved, and the
        // guard then restores nothing.
        let previous_untracked = UNTRACKED_DEPTH.try_with(|depth| depth.replace(0)).ok();
        #[cfg(debug_assertions)]
        let previous_running = RUNNING_REACTOR
            .try_with(|running| running.replace(Rc::as_ptr(&self.inner).cast::<()>()))
            .ok();

        struct Guard<'a> {
            inner: &'a ReactorInner,
            previous_untracked: Option<u32>,
            #[cfg(debug_assertions)]
            previous_running: Option<*const ()>,
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                let popped = self.inner.stack.borrow_mut().pop();
                debug_assert!(popped.is_some(), "reactor observer stack underflow");
                if let Some(node) = popped {
                    let removed = self.inner.active_computations.borrow_mut().remove(&node);
                    debug_assert!(removed, "observer should have been active");
                }
                if let Some(previous) = self.previous_untracked {
                    let _ = UNTRACKED_DEPTH.try_with(|depth| depth.set(previous));
                }
                #[cfg(debug_assertions)]
                if let Some(previous) = self.previous_running {
                    let _ = RUNNING_REACTOR.try_with(|running| running.set(previous));
                }
            }
        }

        let _guard = Guard {
            inner: &self.inner,
            previous_untracked,
            #[cfg(debug_assertions)]
            previous_running,
        };
        f()
    }

    /// Returns an error if reading `observable` right now would close a dependency cycle, i.e.
    /// if `observable` is currently being computed further up the call stack.
    pub(crate) fn cycle_check(&self, observable: NodeId) -> Result<(), ReactCycleError> {
        if self
            .inner
            .active_computations
            .borrow()
            .contains(&observable)
        {
            let stack = self.inner.stack.borrow();
            let start = stack
                .iter()
                .position(|node| *node == observable)
                .expect("active computation should appear in observer stack");
            let mut cycle = stack[start..].to_vec();
            cycle.push(observable);
            let origins = cycle.iter().map(|node| self.node_origin(*node)).collect();
            tracing::debug!(
                target: trace_targets::GRAPH,
                event = "cycle_detected",
                observable_id = observable.0,
                cycle_len = cycle.len(),
                "reactive cycle detected"
            );
            return Err(ReactCycleError::new(cycle, origins));
        }
        Ok(())
    }

    /// Panicking variant of [`cycle_check`](Self::cycle_check), used on read paths before
    /// refreshing a computed node.
    pub(crate) fn assert_no_cycle(&self, observable: NodeId) {
        if let Err(e) = self.cycle_check(observable) {
            panic!("{e}");
        }
    }

    /// Attempts to record a dependency on `observable` for the currently running observer, returning an
    /// error if doing so would create a dependency cycle.
    ///
    /// # Panics
    ///
    /// In debug builds, panics when called while a computation belonging to a *different*
    /// reactor is running on this thread: such a dependency cannot be tracked, so the observer
    /// would never re-run. Wrap the read in [`untrack`] if the cross-reactor read is
    /// intentional.
    pub fn try_observe(&self, observable: NodeId) -> Result<(), ReactCycleError> {
        self.cycle_check(observable)?;

        // Untracked reads record nothing, and are also the sanctioned way to read one reactor's
        // nodes from inside another reactor's computation.
        if is_untracked() {
            return Ok(());
        }

        #[cfg(debug_assertions)]
        self.assert_running_reactor();

        let current = self.inner.stack.borrow().last().copied();
        let Some(observer) = current else {
            return Ok(());
        };

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "observe",
            observer_id = observer.0,
            observable_id = observable.0,
            "recording reactive dependency"
        );

        let existing = self
            .inner
            .dependencies
            .borrow_mut()
            .entry(observer)
            .or_default()
            .insert(observable, self.version(observable));
        if existing.is_none() {
            self.inner.counters.edge_added();
            self.record_flush(|stats| stats.edges_added = stats.edges_added.saturating_add(1));
        }
        let became_observed = {
            let mut dependents = self.inner.dependents.borrow_mut();
            let observers = dependents.entry(observable).or_default();
            let was_unobserved = observers.is_empty();
            observers.insert(observer);
            was_unobserved
        };
        if became_observed {
            self.inner
                .observed_nodes
                .set(self.inner.observed_nodes.get() + 1);
        }
        // Deliver the transition only after the graph maps are released: a hook is consumer code
        // that may read or write the graph.
        if became_observed {
            self.note_observation_change(observable, true);
        }
        Ok(())
    }

    /// Records a dependency on `observable` for the currently running observer.
    ///
    /// # Panics
    ///
    /// Panics with the formatted [`ReactCycleError`] if recording the dependency would close a
    /// cycle in the reactive graph, and under the same debug-build cross-reactor condition as
    /// [`try_observe`](Self::try_observe).
    pub fn observe(&self, observable: NodeId) {
        if let Err(e) = self.try_observe(observable) {
            panic!("{e}");
        }
    }

    /// Records that `observable`'s value changed and marks its dependents stale.
    ///
    /// The node's version is bumped (this is what dependency verification compares against),
    /// direct dependents are marked dirty, and transitive dependents are marked so that they
    /// verify their inputs before recomputing. Nothing recomputes inline: computed nodes
    /// refresh on their next read, and affected effects are queued for the next microtask
    /// flush.
    ///
    /// # Examples
    ///
    /// Together with [`observe`](Self::observe), this is the raw interface custom primitives
    /// are built on ([`crate::Source`] is a thin wrapper over exactly this pair):
    ///
    /// ```rust
    /// use std::cell::Cell;
    /// use std::rc::Rc;
    ///
    /// use adaptite::{Reactor, source_in, thunk_in};
    ///
    /// let reactor = Reactor::new();
    /// let node = source_in(&reactor);          // provides a NodeId
    /// let external = Rc::new(Cell::new(1u32)); // state living outside the graph
    ///
    /// let view = thunk_in(&reactor, {
    ///     let reactor = reactor.clone();
    ///     let node = node.clone();
    ///     let external = Rc::clone(&external);
    ///     move || {
    ///         reactor.observe(node.id()); // reads of `external` depend on `node`
    ///         external.get() * 10
    ///     }
    /// });
    ///
    /// assert_eq!(view.get(), 10);
    /// assert!(reactor.is_observed(node.id()));
    ///
    /// external.set(2);
    /// assert_eq!(view.get(), 10); // the graph has not been told about the write
    ///
    /// reactor.trigger(node.id());
    /// assert_eq!(view.get(), 20); // now the thunk recomputes
    /// ```
    #[track_caller]
    pub fn trigger(&self, observable: NodeId) {
        self.bump_version(observable);
        let write_origin = Location::caller();
        let cause = if self.diagnostics_enabled() {
            self.node_origin(observable)
                .map(|node_origin| InvalidationCause {
                    node: observable,
                    version: self.version(observable),
                    node_origin,
                    write_origin,
                })
        } else {
            None
        };
        if let Some(cause) = cause {
            self.inner.emit(DiagnosticEvent::ReactiveWrite {
                reactor: self.inner.id,
                // Same map lookup that produced `cause`, so a live node always has a kind here.
                kind: self.node_kind(observable).unwrap_or(NodeKind::Source),
                cause,
            });
            self.record_flush(|stats| stats.root_writes = stats.root_writes.saturating_add(1));
        }
        self.mark_dependents(observable, Mark::Dirty, cause);
    }

    /// Marks every dependent of `observable` with `mark`.
    pub(crate) fn mark_dependents(
        &self,
        observable: NodeId,
        mark: Mark,
        cause: Option<InvalidationCause>,
    ) {
        // The set must be copied out before delivering: `deliver_mark` re-enters the graph, and
        // the borrow could not be held across it. The copy comes from a pool rather than a fresh
        // allocation — this runs once per node per propagation step, so a write reaching depth D
        // used to allocate D times whether or not anything ever read the result.
        let mut dependents;
        {
            let map = self.inner.dependents.borrow();
            let Some(nodes) = map.get(&observable).filter(|nodes| !nodes.is_empty()) else {
                // The common case for a leaf signal. Returning before touching the pool keeps
                // this path exactly as cheap as it was when it allocated nothing either.
                return;
            };
            // Borrows `node_scratch`, not `dependents`, so this is safe inside the borrow.
            dependents = self.take_node_buffer();
            dependents.extend(nodes.iter().copied());
        }

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "mark_dependents",
            observable_id = observable.0,
            dependent_count = dependents.len(),
            ?mark,
            "marking reactive dependents"
        );

        // Propagation is hot, and depth tracking is only ever reported through `FlushStats`,
        // which is subscription-gated — so the dormant path stays exactly what it was in 0.2,
        // with no counter and no drop obligation. (Measured: adding an unconditional depth guard
        // here cost 15% on a bare signal write.)
        if !self.inner.diagnostics_active.get() {
            for &dependent in &dependents {
                self.deliver_mark(dependent, mark, cause);
            }
            self.give_node_buffer(dependents);
            return;
        }

        // Marking recurses through `ObserverHook::mark`, so depth is tracked here rather than
        // threaded through every hook. The guard keeps it correct when a hook unwinds.
        let depth = self.inner.mark_depth.get() + 1;
        self.inner.mark_depth.set(depth);

        struct DepthGuard<'a>(&'a Cell<u32>);

        impl Drop for DepthGuard<'_> {
            fn drop(&mut self) {
                self.0.set(self.0.get().saturating_sub(1));
            }
        }

        let _depth_guard = DepthGuard(&self.inner.mark_depth);

        for &dependent in &dependents {
            if self.deliver_mark(dependent, mark, cause) {
                self.record_flush(|stats| {
                    match mark {
                        Mark::Check => {
                            stats.nodes_marked_check = stats.nodes_marked_check.saturating_add(1);
                        }
                        Mark::Dirty => {
                            stats.nodes_marked_dirty = stats.nodes_marked_dirty.saturating_add(1);
                        }
                    }
                    stats.max_propagation_depth = stats.max_propagation_depth.max(depth);
                });
            }
        }

        // The dormant path above returns its buffer before its early return, and this path has to
        // as well. Dropping it here instead emptied the pool permanently the first time anything
        // subscribed, so every later mark step fell through `take_node_buffer`'s
        // `unwrap_or_default()` and allocated — one `Vec` per node per propagation step, the exact
        // cost 129968d removed, reintroduced by having a subscription installed. Safe after the
        // recursion: `deliver_mark` re-enters `mark_dependents`, which pops a *different* buffer,
        // because this one is not in the pool while it is owned here.
        self.give_node_buffer(dependents);
    }

    /// Delivers one mark, dropping the observer's registration if it is gone. Returns whether a
    /// live observer actually received it.
    fn deliver_mark(
        &self,
        dependent: NodeId,
        mark: Mark,
        cause: Option<InvalidationCause>,
    ) -> bool {
        let hook = self
            .inner
            .observers
            .borrow()
            .get(&dependent)
            .cloned()
            .and_then(|weak| weak.upgrade());
        match hook {
            Some(hook) => {
                hook.mark(mark, cause);
                true
            }
            None => {
                self.inner.observers.borrow_mut().remove(&dependent);
                false
            }
        }
    }

    /// Brings `node` up to date if it is a computed node that is currently registered.
    ///
    /// # Panics
    ///
    /// Panics with a [`ReactCycleError`] message when `node` is currently mid-computation: a
    /// dependency cycle discovered through verification rather than a direct read.
    pub(crate) fn refresh_node(&self, node: NodeId) {
        self.assert_no_cycle(node);
        let hook = self
            .inner
            .observers
            .borrow()
            .get(&node)
            .cloned()
            .and_then(|weak| weak.upgrade());
        if let Some(hook) = hook {
            hook.refresh();
        }
    }

    /// Returns `true` if any of `observer`'s recorded dependencies has a different value than the
    /// one observed during the observer's last run.
    ///
    /// Computed dependencies are refreshed before comparison, so unchanged memos suppress
    /// downstream recomputation.
    pub(crate) fn dependencies_changed(&self, observer: NodeId) -> bool {
        // Deliberately not `dependencies_of`: that is the public inspection API and allocates a
        // fresh Vec, and this runs on every verification of every Check-marked node. The copy is
        // still required — `refresh_node` recomputes user code, which may re-enter the graph.
        let mut recorded = self.take_edge_buffer();
        if let Some(edges) = self.inner.dependencies.borrow().get(&observer) {
            recorded.extend(
                edges
                    .iter()
                    .map(|(node, version)| crate::RecordedDependency {
                        node: *node,
                        version: *version,
                    }),
            );
        }
        let mut changed = false;
        for &entry in &recorded {
            self.refresh_node(entry.node);
            if self.version(entry.node) != entry.version {
                changed = true;
                break;
            }
        }
        self.give_edge_buffer(recorded);
        changed
    }

    /// Disposes all graph bookkeeping for `node`.
    ///
    /// Idempotent: a node that is already gone is a silent no-op, and the
    /// [`NodeDisposed`](DiagnosticEvent::NodeDisposed) event is delivered exactly once.
    ///
    /// # Warning: this does not consult the handle that owns the node
    ///
    /// This is the low-level teardown a primitive's `Drop` calls once its last handle is gone,
    /// not the way to dispose a [`crate::Signal`], [`crate::Memo`], [`crate::Thunk`],
    /// [`crate::Event`] or [`crate::EffectHandle`]. Since 0.3 publishes `id()` on all of them,
    /// `reactor.dispose(handle.id())` compiles warning-free and leaves a **zombie**: the handle
    /// stays usable and reads and writes still succeed, but the node has no edges, observers or
    /// metadata, so nothing downstream updates again — a memo frozen at its last value, an effect
    /// that never runs, [`node_kind`](Self::node_kind) reporting `None`, and no later query able
    /// to tell that apart from a node that never existed. Nothing panics and no counter drifts;
    /// only the reactivity is gone, and only a live subscription (which sees `NodeDisposed`)
    /// records it. Reach for this only when you allocated the id yourself.
    pub fn dispose(&self, node: NodeId) {
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "dispose_node",
            node_id = node.0,
            "disposing reactive node bookkeeping"
        );
        // Sampled before anything is torn down: the counts a leak report wants are the ones the
        // node died holding, and both maps are emptied below. Only the diagnostic needs them —
        // the counters ride on the removals themselves — so the two extra lookups stay gated.
        let edges_held = if self.diagnostics_enabled() {
            Some((
                self.inner
                    .dependencies
                    .borrow()
                    .get(&node)
                    .map_or(0, hashbrown::HashMap::len),
                self.inner
                    .dependents
                    .borrow()
                    .get(&node)
                    .map_or(0, hashbrown::HashSet::len),
            ))
        } else {
            None
        };

        self.clear_observer_dependencies(node, false);

        let removed_dependents = self.inner.dependents.borrow_mut().remove(&node);
        if removed_dependents
            .as_ref()
            .is_some_and(|nodes| !nodes.is_empty())
        {
            self.inner
                .observed_nodes
                .set(self.inner.observed_nodes.get().saturating_sub(1));
        }
        let incoming = removed_dependents
            .map(|nodes| nodes.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        self.inner.counters.edges_removed(incoming.len());
        let removed = incoming.len();
        self.record_flush(|stats| {
            stats.edges_removed = stats.edges_removed.saturating_add(removed as u32);
        });
        for observer in incoming {
            let mut dependencies = self.inner.dependencies.borrow_mut();
            if let Some(observed) = dependencies.get_mut(&observer) {
                observed.remove(&node);
                if observed.is_empty() {
                    dependencies.remove(&observer);
                }
            }
        }

        self.inner.observers.borrow_mut().remove(&node);
        // The removed metadata is both the "was it live" answer and the kind, so neither costs a
        // second lookup. Disposal is idempotent and is reached from several `Drop` impls; gating
        // on it is what keeps the gauge and the event from counting a node twice.
        let removed = self.inner.meta.borrow_mut().remove(&node);
        self.unregister_observation_hooks(node);

        let Some(meta) = removed else {
            return;
        };
        self.inner.counters.node_disposed(meta.kind);
        if let Some((dependencies, dependents)) = edges_held {
            self.inner.emit(DiagnosticEvent::NodeDisposed {
                reactor: self.inner.id,
                node,
                kind: meta.kind,
                origin: meta.origin,
                dependencies,
                observers: dependents,
            });
        }
    }

    /// Schedules a job to run in the reactor's microtask-backed job queue.
    pub fn schedule(&self, job: impl FnOnce() + 'static) {
        let pending = {
            let mut jobs = self.inner.pending_jobs.borrow_mut();
            jobs.push_back(Box::new(job));
            jobs.len()
        };
        self.inner.counters.job_queued(pending);
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "schedule_job",
            pending_jobs = self.inner.pending_jobs.borrow().len(),
            "queued reactive job for microtask flush"
        );
        self.inner.ensure_flush_scheduled();
    }

    /// Flushes queued reactive jobs immediately on the calling thread.
    ///
    /// The queue is drained until empty, so jobs queued *during* the flush — such as effect
    /// re-runs triggered by writes made from other effects — also run before this returns. If
    /// a job panics, the panic propagates to the caller and any remaining jobs are handed to a
    /// fresh microtask flush, so one panicking effect cannot silently disable the reactor.
    ///
    /// This is useful when host integrations need synchronous propagation (for example,
    /// during native resize loops) and in tests that drive effects without a runtime tick.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    ///
    /// use adaptite::{Reactor, signal_in};
    ///
    /// let reactor = Reactor::new();
    /// let value = signal_in(&reactor, 1);
    /// let seen = Rc::new(RefCell::new(Vec::new()));
    ///
    /// let effect = reactor.effect({
    ///     let value = value.clone();
    ///     let seen = Rc::clone(&seen);
    ///     move || seen.borrow_mut().push(value.get())
    /// });
    ///
    /// // The initial run is queued, not inline; flush it synchronously.
    /// assert!(seen.borrow().is_empty());
    /// reactor.flush_now();
    /// assert_eq!(*seen.borrow(), [1]);
    ///
    /// value.set(2);
    /// reactor.flush_now();
    /// assert_eq!(*seen.borrow(), [1, 2]);
    /// # effect.dispose();
    /// ```
    pub fn flush_now(&self) {
        Rc::clone(&self.inner).flush_jobs();
    }

    /// Runs `f` as one flush of this reactor, for consumers draining their own effect lane.
    ///
    /// A custom [`crate::EffectScheduler`] decides *when* its effects run, so adaptite cannot see
    /// where one drain ends and the next begins. Wrapping a drain in `external_flush` supplies
    /// that boundary: every [`crate::EffectRun`] executed inside `f` shares one flush epoch, so
    /// the divergence guard — which counts an effect's runs within one logical *drain*, and which
    /// is enforced in every build — keeps working across the drain, and diagnostic consumers see
    /// one `FlushStarted`/`FlushFinished` pair rather than one per effect.
    ///
    /// Without it, each externally scheduled run opens and closes its own single-run flush. That
    /// is correct but blind: an effect that re-schedules itself into the same drain forever is
    /// then a livelock the guard cannot see. Draining inside `external_flush` is therefore the
    /// recommended shape.
    ///
    /// Nesting is permitted, and the two kinds of nesting differ:
    ///
    /// - A nested `external_flush` **joins** the enclosing flush. The consumer already declared a
    ///   boundary; a second one inside it is the same drain, and it opens no new epoch.
    /// - A re-entrant [`flush_now`](Self::flush_now) from inside `f` opens its own *diagnostic*
    ///   flush, so its work is reported separately rather than folded into the enclosing totals.
    ///
    /// Both stay inside the same **logical drain**, which is what the divergence guard counts
    /// against. That separation is deliberate: an effect that writes its own dependency and then
    /// re-flushes would otherwise hand itself a fresh epoch on every run and never trip the
    /// guard. Diagnostic flush identity is for attribution; drain identity is for the guard.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    ///
    /// use adaptite::{EffectRun, Reactor, signal_in};
    ///
    /// // A render lane: the consumer decides when queued effects run.
    /// let lane: Rc<RefCell<Vec<EffectRun>>> = Rc::new(RefCell::new(Vec::new()));
    ///
    /// let reactor = Reactor::new();
    /// let value = signal_in(&reactor, 1);
    /// let seen = Rc::new(RefCell::new(Vec::new()));
    ///
    /// let effect = reactor.effect_with(
    ///     {
    ///         let lane = Rc::clone(&lane);
    ///         move |ready: EffectRun| lane.borrow_mut().push(ready)
    ///     },
    ///     {
    ///         let value = value.clone();
    ///         let seen = Rc::clone(&seen);
    ///         move || seen.borrow_mut().push(value.get())
    ///     },
    /// );
    ///
    /// // Nothing runs until the lane is drained — not even the initial run.
    /// reactor.flush_now();
    /// assert!(seen.borrow().is_empty());
    ///
    /// value.set(2);
    /// reactor.external_flush(|| {
    ///     for ready in lane.borrow_mut().drain(..) {
    ///         ready.run();
    ///     }
    /// });
    /// assert_eq!(*seen.borrow(), [2]);
    /// # effect.dispose();
    /// ```
    pub fn external_flush<T>(&self, f: impl FnOnce() -> T) -> T {
        struct Guard<'a>(&'a ReactorInner);

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.end_flush();
            }
        }

        // Armed *before* the flush is opened, not after. `begin_flush` increments `flush_depth`
        // as its first statement and then calls consumer code — the `FlushStarted` subscriber —
        // as its last. Constructing the guard afterwards meant a panic escaping that subscriber
        // stranded `flush_depth` at 1 for the rest of the process: `flush_jobs` only advances
        // `drain_epoch` while the depth is zero, so the logical drain froze, and the divergence
        // guard (enforced in every build) then panicked on the 101st ordinary run of whatever
        // innocent effect happened to be next. Arming first costs nothing — the guard's only job
        // is the matching decrement, and `end_flush` saturates.
        let _guard = Guard(&self.inner);
        self.inner.begin_flush();
        f()
    }

    /// Returns `true` while a flush of this reactor is in progress, including a drain opened by
    /// [`external_flush`](Self::external_flush).
    pub(crate) fn in_flush(&self) -> bool {
        self.inner.flush_depth.get() > 0
    }

    /// Opens a flush this reactor is not itself driving. Pair with [`end_flush`](Self::end_flush).
    pub(crate) fn begin_flush(&self) {
        self.inner.begin_flush();
    }

    /// Closes a flush opened by [`begin_flush`](Self::begin_flush).
    pub(crate) fn end_flush(&self) {
        self.inner.end_flush();
    }

    #[track_caller]
    pub(crate) fn allocate_node(&self, kind: NodeKind) -> NodeId {
        let raw = self.inner.next_node.get();
        self.inner.next_node.set(raw.wrapping_add(1));
        let id = NodeId::new(raw);
        let origin = Location::caller();
        self.inner.meta.borrow_mut().insert(
            id,
            NodeMeta {
                version: 0,
                origin,
                kind,
            },
        );
        self.inner
            .counters
            .node_created(kind, self.inner.meta.borrow().len());
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "allocate_node",
            node_id = id.0,
            ?kind,
            "allocated reactive node id"
        );
        if self.diagnostics_enabled() {
            self.inner.emit(DiagnosticEvent::NodeCreated {
                reactor: self.inner.id,
                node: id,
                kind,
                origin,
            });
        }
        id
    }

    pub(crate) fn register_observer(&self, id: NodeId, observer: Rc<dyn ObserverHook>) {
        self.inner
            .observers
            .borrow_mut()
            .insert(id, Rc::downgrade(&observer));
    }

    pub(crate) fn unregister_observer(&self, id: NodeId) {
        self.inner.observers.borrow_mut().remove(&id);
    }

    /// Increments the version of `node`, recording that its value changed.
    pub(crate) fn bump_version(&self, node: NodeId) {
        if let Some(meta) = self.inner.meta.borrow_mut().get_mut(&node) {
            meta.version = meta.version.wrapping_add(1);
        }
    }

    /// Returns the current version of `node`, or 0 if the node is unknown.
    ///
    /// The internal counterpart to [`node_version`](Self::node_version): verification compares
    /// versions on every dependency of every observer it checks, and an absent node comparing
    /// equal to a never-written one is the behaviour that path wants.
    pub(crate) fn version(&self, node: NodeId) -> u64 {
        self.node_version(node).unwrap_or(0)
    }

    /// Returns the identity of the logical drain in progress: the outermost flush and everything
    /// nested inside it. Used by the divergence guard, which must not be resettable by a
    /// re-entrant `flush_now`.
    ///
    /// The guard is enforced in every build — a runaway loop in a release build is a frozen
    /// application with no output — so this has a caller in an optimized build too. It used to be
    /// debug-only, which is what the removed `allow(dead_code)` was for.
    pub(crate) fn drain_epoch(&self) -> u64 {
        self.inner.drain_epoch.get()
    }

    /// Returns the number of the currently running (or most recent) job flush.
    pub(crate) fn flush_epoch(&self) -> u64 {
        self.inner.flush_epoch.get()
    }

    pub(crate) fn emit_diagnostic(&self, event: DiagnosticEvent) {
        self.inner.emit(event);
    }

    /// Adds to the totals of whichever flush this work belongs to.
    ///
    /// Dormant without a subscription — `FlushStats` is only ever observed by being delivered in
    /// an event — so the cost on every hot path that calls this is one cell load. The closure is
    /// not evaluated when dormant.
    #[inline]
    pub(crate) fn record_flush(&self, f: impl FnOnce(&mut FlushStats)) {
        if self.inner.diagnostics_active.get() {
            self.inner.flushes.record(f);
        }
    }

    /// Returns whether `observer`'s computation is currently on the call stack.
    ///
    /// Used to defer a run rather than re-enter one: re-entry cannot be tracked coherently, so a
    /// nested flush that reaches an already-running effect must leave it for afterwards.
    pub(crate) fn is_computation_active(&self, observer: NodeId) -> bool {
        self.inner.active_computations.borrow().contains(&observer)
    }

    pub(crate) fn counters(&self) -> &GraphCounters {
        &self.inner.counters
    }

    /// Counts live edges by walking both indexes, for tests that check the maintained counter
    /// against the graph it claims to describe.
    ///
    /// Returns `(from dependencies, from dependents)`. The two must agree with each other and
    /// with `GraphStats::live_edges`; a maintained counter is only as good as the assertion that
    /// it has not drifted.
    /// Recounts observed nodes by walking `dependents`, for the same reason as
    /// [`walk_edge_counts`](Self::walk_edge_counts).
    ///
    /// `observed_nodes` stopped being `dependents.len()` when emptied entries began to be
    /// retained, so it is now a maintained counter with the drift risk that implies.
    #[cfg(test)]
    pub(crate) fn walk_observed_nodes(&self) -> usize {
        self.inner
            .dependents
            .borrow()
            .values()
            .filter(|observers| !observers.is_empty())
            .count()
    }

    /// Number of scratch buffers parked in the node-id pool — the whole mechanism behind
    /// "propagation does not allocate", and the only way to observe it without a counting
    /// allocator. A path that takes a buffer and never gives it back drains the pool to zero and
    /// then allocates forever.
    #[cfg(test)]
    pub(crate) fn node_scratch_pool_len(&self) -> usize {
        self.inner.node_scratch.borrow().len()
    }

    #[cfg(test)]
    pub(crate) fn walk_edge_counts(&self) -> (usize, usize) {
        let outgoing = self
            .inner
            .dependencies
            .borrow()
            .values()
            .map(HashMap::len)
            .sum();
        let incoming = self
            .inner
            .dependents
            .borrow()
            .values()
            .map(HashSet::len)
            .sum();
        (outgoing, incoming)
    }

    pub(crate) fn diagnostics_enabled(&self) -> bool {
        self.inner.diagnostics_active.get()
    }

    /// Returns this reactor's process-local identifier.
    ///
    /// Two `Reactor` handles address the same graph exactly when their ids are equal — which is
    /// how a caller confirms that ambient constructors landed on the reactor it expected.
    pub fn id(&self) -> ReactorId {
        self.inner.id
    }

    pub(crate) fn diagnostic_id(&self) -> ReactorId {
        self.inner.id
    }

    #[cfg(debug_assertions)]
    fn assert_running_reactor(&self) {
        // `try_with`: reads are reachable from a cleanup, hence from a thread-local destructor.
        // With the slot destroyed there is no running computation to be inconsistent with, so
        // skipping the check is both safe and the only option.
        let _ = RUNNING_REACTOR.try_with(|running| {
            let running = running.get();
            if !running.is_null() && running != Rc::as_ptr(&self.inner).cast::<()>() {
                panic!(
                    "adaptite: a node belonging to one reactor was read inside a computation \
                     running in a different reactor on the same thread; this dependency cannot \
                     be tracked and the observer will not re-run when the node changes"
                );
            }
        });
    }

    /// Drops every edge `observer` recorded during its last run.
    ///
    /// `retain_table` keeps the emptied dependency table allocated, which is what a rerunning
    /// observer wants: it is about to refill exactly that table, and removing it returned the
    /// allocation only for `try_observe` to build a new one immediately afterwards. A node being
    /// disposed never reruns, so disposal passes `false` and gets the memory back.
    fn clear_observer_dependencies(&self, observer: NodeId, retain_table: bool) {
        let mut observed;
        {
            let mut map = self.inner.dependencies.borrow_mut();
            let Some(edges) = map.get_mut(&observer) else {
                return;
            };
            if edges.is_empty() {
                // An observer that recorded nothing — a fresh node, or one that read nothing on
                // its last run. Returning here keeps it off the scratch pool entirely.
                if !retain_table {
                    map.remove(&observer);
                }
                return;
            }
            // Borrows `node_scratch`, not `dependencies`, so this is safe inside the borrow.
            observed = self.take_node_buffer();
            observed.extend(edges.keys().copied());
            if retain_table {
                edges.clear();
            } else {
                map.remove(&observer);
            }
        }
        self.inner.counters.edges_removed(observed.len());
        let removed = observed.len();
        self.record_flush(|stats| {
            stats.edges_removed = stats.edges_removed.saturating_add(removed as u32);
        });

        let mut unobserved = self.take_node_buffer();
        for &observable in &observed {
            let mut dependents = self.inner.dependents.borrow_mut();
            if let Some(observers) = dependents.get_mut(&observable) {
                observers.remove(&observer);
                if observers.is_empty() {
                    // The emptied set is retained for the same reason the dependency table is:
                    // an observable that loses its last observer during a rerun is usually about
                    // to regain one. `observed_nodes` is therefore a maintained counter rather
                    // than `dependents.len()`.
                    self.inner
                        .observed_nodes
                        .set(self.inner.observed_nodes.get().saturating_sub(1));
                    unobserved.push(observable);
                }
            }
        }
        self.give_node_buffer(observed);

        // This runs during an observer's rerun or disposal, with graph maps borrowed; the
        // notification is deferred, so hooks never observe a half-updated graph.
        for &observable in &unobserved {
            self.note_observation_change(observable, false);
        }
        self.give_node_buffer(unobserved);
    }

    /// Takes a reusable node-id buffer from the pool.
    ///
    /// A pool rather than one buffer because both users nest: `mark_dependents` recurses through
    /// `ObserverHook::mark`, and `clear_observer_dependencies` holds two at once.
    fn take_node_buffer(&self) -> Vec<NodeId> {
        self.inner
            .node_scratch
            .borrow_mut()
            .pop()
            .unwrap_or_default()
    }

    fn give_node_buffer(&self, mut buffer: Vec<NodeId>) {
        buffer.clear();
        let mut pool = self.inner.node_scratch.borrow_mut();
        if pool.len() < SCRATCH_POOL_LIMIT {
            pool.push(buffer);
        }
    }

    fn take_edge_buffer(&self) -> Vec<crate::RecordedDependency> {
        self.inner
            .edge_scratch
            .borrow_mut()
            .pop()
            .unwrap_or_default()
    }

    fn give_edge_buffer(&self, mut buffer: Vec<crate::RecordedDependency>) {
        buffer.clear();
        let mut pool = self.inner.edge_scratch.borrow_mut();
        if pool.len() < SCRATCH_POOL_LIMIT {
            pool.push(buffer);
        }
    }

    /// Records that `node` gained its first observer or lost its last, and queues delivery of the
    /// corresponding hook.
    ///
    /// Delivery is deferred to a reactor job for two reasons: the graph maps are borrowed at every
    /// call site, and deferring lets a leave/arrive pair within one flush collapse to nothing.
    fn note_observation_change(&self, node: NodeId, observed: bool) {
        let hooks = self.inner.observation_hooks.borrow().get(&node).cloned();
        let Some(hooks) = hooks else {
            return;
        };

        hooks.observed.set(observed);
        if hooks.queued.replace(true) {
            return;
        }

        self.schedule(move || {
            hooks.queued.set(false);
            if hooks.cancelled.get() {
                return;
            }
            let observed = hooks.observed.get();
            // Coalesce: only an actual change from the last delivered state is worth a callback,
            // so a source observed, dropped, and observed again within one flush stays quiet.
            if hooks.delivered.replace(observed) == observed {
                return;
            }
            if observed {
                (hooks.on_watch)();
            } else {
                (hooks.on_unwatch)();
            }
        });
    }

    /// Registers the observation hooks for `node`, retained until the node is disposed.
    ///
    /// The registry holds the closures **strongly** for the node's whole lifetime; only
    /// `unregister_observation_hooks`, reached from disposal, drops them. A hook that captures the
    /// `Source` it belongs to therefore closes
    /// `ReactorInner -> observation_hooks -> on_watch -> Source -> Reactor -> ReactorInner` and
    /// retains the whole reactor for the process lifetime — invisible to every 0.3 gauge, since
    /// `OwnershipStats` counts no owner frame for a `Source` and `GraphStats` is reachable only
    /// through the very handle that leaked. It is easy to reach by accident: the node does not
    /// exist when the hooks are supplied, so the natural shape is an `Rc<RefCell<Option<Source>>>`
    /// slot filled afterwards. Capturing a `Weak` of that slot breaks the cycle at no cost, and is
    /// what `source_with_hooks` documents. When testing this, note that a queued-but-undelivered
    /// hook job holds its own `Rc<ObservationHooks>`: a check made without flushing after the last
    /// observer leaves reports a leak that is not there.
    pub(crate) fn register_observation_hooks(
        &self,
        node: NodeId,
        on_watch: impl Fn() + 'static,
        on_unwatch: impl Fn() + 'static,
    ) {
        self.inner.observation_hooks.borrow_mut().insert(
            node,
            Rc::new(ObservationHooks {
                on_watch: Box::new(on_watch),
                on_unwatch: Box::new(on_unwatch),
                observed: Cell::new(false),
                delivered: Cell::new(false),
                queued: Cell::new(false),
                cancelled: Cell::new(false),
            }),
        );
    }

    pub(crate) fn unregister_observation_hooks(&self, node: NodeId) {
        if let Some(hooks) = self.inner.observation_hooks.borrow_mut().remove(&node) {
            // A delivery job may already be queued and holding its own reference.
            hooks.cancelled.set(true);
        }
    }
}

/// Callbacks fired when a node gains its first observer or loses its last.
struct ObservationHooks {
    on_watch: Box<dyn Fn()>,
    on_unwatch: Box<dyn Fn()>,
    /// Latest observed state, updated synchronously at the edge transition.
    observed: Cell<bool>,
    /// State most recently delivered to the consumer, used to suppress no-op deliveries.
    delivered: Cell<bool>,
    queued: Cell<bool>,
    cancelled: Cell<bool>,
}

impl Default for Reactor {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Reactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reactor")
            .field("ptr", &Rc::as_ptr(&self.inner))
            .finish()
    }
}

pub(crate) struct NodeMeta {
    pub(crate) version: u64,
    pub(crate) origin: &'static Location<'static>,
    pub(crate) kind: NodeKind,
}

/// Upper bound on retained scratch buffers.
///
/// The pool must be at least as deep as the graph nests, or the excess silently reverts to
/// allocating; this is set far above any plausible nesting depth rather than tuned.
const SCRATCH_POOL_LIMIT: usize = 512;

pub(crate) struct ReactorInner {
    pub(crate) id: ReactorId,
    next_node: Cell<u64>,
    pub(crate) meta: RefCell<HashMap<NodeId, NodeMeta>>,
    pub(crate) dependencies: RefCell<HashMap<NodeId, HashMap<NodeId, u64>>>,
    pub(crate) dependents: RefCell<HashMap<NodeId, HashSet<NodeId>>>,
    /// Nodes with at least one observer.
    ///
    /// Maintained rather than read off `dependents.len()`: emptied entries are retained across an
    /// observer's rerun, so the map's length counts nodes that *have ever been* observed.
    pub(crate) observed_nodes: Cell<usize>,
    /// Reusable node-id buffers. Both users must copy a set out of a borrowed map before calling
    /// code that may re-enter the graph, and both nest, so this is a pool rather than one buffer.
    node_scratch: RefCell<Vec<Vec<NodeId>>>,
    /// Reusable recorded-edge buffers, for dependency verification.
    edge_scratch: RefCell<Vec<Vec<crate::RecordedDependency>>>,
    pub(crate) observers: RefCell<HashMap<NodeId, Weak<dyn ObserverHook>>>,
    stack: RefCell<Vec<NodeId>>,
    active_computations: RefCell<HashSet<NodeId>>,
    pub(crate) pending_jobs: RefCell<VecDeque<Job>>,
    flush_scheduled: Cell<bool>,
    pub(crate) flush_epoch: Cell<u64>,
    /// Epoch pinned by the outermost `begin_flush`, so `end_flush` closes the flush it opened
    /// rather than whichever one happens to be current.
    ///
    /// A re-entrant `flush_now` inside a consumer-declared flush bumps the shared epoch, so
    /// reading the live value at close reports the *inner* flush a second time and never
    /// terminates the outer one. `flush_jobs` pins the same way in its `FlushGuard`; this is that
    /// pin for the `begin_flush`/`end_flush` pair, which had none.
    open_flush_epoch: Cell<u64>,
    /// Identifies one *logical drain*: the outermost flush and everything nested inside it.
    ///
    /// Distinct from `flush_epoch`, which identifies each flush for diagnostics. A re-entrant
    /// `flush_now` opens its own diagnostic flush so its totals are separable, but it stays
    /// inside the enclosing drain — otherwise an effect that re-flushes could give itself a fresh
    /// epoch on every run and walk straight past the divergence guard.
    pub(crate) drain_epoch: Cell<u64>,
    /// Nesting depth of active flushes, including drains a consumer opened with
    /// [`Reactor::external_flush`]. Non-zero means the current `flush_epoch` is live, so an
    /// externally scheduled effect run joins it instead of opening one of its own.
    pub(crate) flush_depth: Cell<u32>,
    next_diagnostic: Cell<u64>,
    diagnostics_active: Cell<bool>,
    diagnostics: RefCell<Vec<(u64, DiagnosticCallback)>>,
    observation_hooks: RefCell<HashMap<NodeId, Rc<ObservationHooks>>>,
    pub(crate) counters: GraphCounters,
    flushes: FlushAccounting,
    /// Depth of the mark propagation currently running, for `max_propagation_depth`.
    mark_depth: Cell<u32>,
}

impl ReactorInner {
    fn new() -> Self {
        Self {
            id: ReactorId(NEXT_REACTOR_ID.fetch_add(1, AtomicOrdering::Relaxed)),
            next_node: Cell::new(1),
            meta: RefCell::new(HashMap::new()),
            dependencies: RefCell::new(HashMap::new()),
            dependents: RefCell::new(HashMap::new()),
            observed_nodes: Cell::new(0),
            node_scratch: RefCell::new(Vec::new()),
            edge_scratch: RefCell::new(Vec::new()),
            observers: RefCell::new(HashMap::new()),
            stack: RefCell::new(Vec::new()),
            active_computations: RefCell::new(HashSet::new()),
            pending_jobs: RefCell::new(VecDeque::new()),
            flush_scheduled: Cell::new(false),
            flush_epoch: Cell::new(0),
            flush_depth: Cell::new(0),
            next_diagnostic: Cell::new(1),
            diagnostics_active: Cell::new(false),
            diagnostics: RefCell::new(Vec::new()),
            observation_hooks: RefCell::new(HashMap::new()),
            counters: GraphCounters::default(),
            open_flush_epoch: Cell::new(0),
            drain_epoch: Cell::new(0),
            flushes: FlushAccounting::default(),
            mark_depth: Cell::new(0),
        }
    }

    /// Emits a closing span event from a `Drop`, where a panicking subscriber must not abort.
    ///
    /// The pairing contract is explicit that the close is delivered on the unwind path too — a
    /// consumer timing a flush needs it whether or not the flush succeeded — so this cannot fall
    /// silent while `panicking()`. What it must not do is let the subscriber's own panic escape
    /// into an unwind already in progress: that is a non-unwinding panic, and it aborts. One
    /// panicking effect plus one panicking subscriber otherwise took the process down instead of
    /// producing two reportable bugs. Same precedent as `OwnerFrame::reset`: while unwinding the
    /// second failure is logged and discarded so the first still reaches the caller.
    fn emit_from_drop(&self, event: DiagnosticEvent) {
        if !self.diagnostics_active.get() {
            return;
        }
        if !std::thread::panicking() {
            self.emit(event);
            return;
        }
        let delivered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.emit(event)));
        if delivered.is_err() {
            tracing::error!(
                target: trace_targets::GRAPH,
                event = "diagnostic_subscriber_panic_during_unwind",
                "a diagnostic subscriber panicked while the thread was already unwinding; the \
                 event was dropped so the original panic could propagate instead of aborting"
            );
        }
    }

    fn emit(&self, event: DiagnosticEvent) {
        if !self.diagnostics_active.get() {
            return;
        }
        let callbacks = self.diagnostics.borrow();
        if callbacks.len() == 1 {
            let callback = Rc::clone(&callbacks[0].1);
            drop(callbacks);
            callback(event);
            return;
        }
        let snapshot = callbacks
            .iter()
            .map(|(_, callback)| Rc::clone(callback))
            .collect::<Vec<_>>();
        drop(callbacks);
        for callback in snapshot {
            callback(event);
        }
    }

    /// Opens a flush that adaptite is not itself driving, bumping the epoch only when this is
    /// the outermost one so a nested drain joins the flush already in progress.
    fn begin_flush(&self) {
        let depth = self.flush_depth.get();
        self.flush_depth.set(depth + 1);
        if depth > 0 {
            return;
        }

        // Past the early return above, so this is the outermost flush: a new logical drain.
        self.drain_epoch.set(self.drain_epoch.get().wrapping_add(1));
        self.flush_epoch.set(self.flush_epoch.get().wrapping_add(1));
        self.counters.flush_opened();
        let epoch = self.flush_epoch.get();
        self.open_flush_epoch.set(epoch);
        if self.diagnostics_active.get() {
            self.flushes.open_flush(self.pending_jobs.borrow().len());
        }
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "begin_external_flush",
            flush_epoch = epoch,
            "opened an externally driven flush"
        );
        if self.diagnostics_active.get() {
            // Bind the count before emitting: written inline it is a temporary whose borrow lives
            // until the end of the statement, i.e. across the subscriber call. A subscriber that
            // schedules reactor work — an entirely reasonable thing to do from `FlushStarted` —
            // would then hit a bare `BorrowMutError` from inside adaptite.
            let pending_jobs = self.pending_jobs.borrow().len();
            self.emit(DiagnosticEvent::FlushStarted {
                reactor: self.id,
                flush_epoch: epoch,
                pending_jobs,
            });
        }
    }

    fn end_flush(&self) {
        let depth = self.flush_depth.get().saturating_sub(1);
        self.flush_depth.set(depth);
        if depth > 0 {
            return;
        }

        if self.diagnostics_active.get() {
            let remaining_jobs = self.pending_jobs.borrow().len();
            // `None` means this subscriber was not here when the flush opened, so there is no
            // pair to close. Emitting anyway — what `unwrap_or_default()` did — handed a
            // mid-flush subscriber a `FlushFinished` with all-zero stats and no `FlushStarted`
            // before it, breaking the documented duration recipe on the first event it ever saw.
            // Staying quiet makes setup match teardown, where dropping the last subscription
            // mid-flush already yields an unpaired *open* and nothing more.
            let Some(stats) = self
                .flushes
                .close_flush(remaining_jobs, self.counters.queued_effects())
            else {
                return;
            };
            self.emit_from_drop(DiagnosticEvent::FlushFinished {
                reactor: self.id,
                // The pinned epoch, not the live one: a re-entrant `flush_now` inside this flush
                // has already moved `flush_epoch` on, so reading it here would close the inner
                // flush a second time and never terminate this one.
                flush_epoch: self.open_flush_epoch.get(),
                remaining_jobs,
                stats,
            });
        }
    }

    fn ensure_flush_scheduled(self: &Rc<Self>) {
        if self.flush_scheduled.replace(true) {
            return;
        }

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "schedule_flush",
            pending_jobs = self.pending_jobs.borrow().len(),
            "scheduling reactive microtask flush"
        );
        let reactor = Rc::clone(self);
        queue_microtask(move || {
            reactor.flush_jobs();
        });
    }

    fn flush_jobs(self: Rc<Self>) {
        // A drain with nothing to drain is not a flush. `flush_now` runs the queue directly but
        // cannot unqueue the microtask that `ensure_flush_scheduled` already handed to the
        // runtime, so that microtask arrives later with an empty queue — and every such arrival
        // used to open an epoch, emit a Started/Finished pair, and report an empty `FlushStats`.
        // For an application asking "does my idle window flush?", that turned a settled graph
        // into a stream of empty flushes with no cause, which is exactly the signal being looked
        // for. Returning here makes "no flush at all" the honest signature of idle.
        //
        // Deliberately not applied to `begin_flush`: `external_flush` is a boundary a consumer
        // declared, and it should be reported whether or not the drain found work.
        if self.pending_jobs.borrow().is_empty() {
            self.flush_scheduled.set(false);
            return;
        }

        let _span = tracing::debug_span!(
            target: trace_targets::GRAPH,
            "reactor.flush_jobs"
        )
        .entered();
        // A re-entrant `flush_now` is nested inside a drain that is already open, so it takes a
        // fresh diagnostic epoch but keeps the enclosing drain identity.
        if self.flush_depth.get() == 0 {
            self.drain_epoch.set(self.drain_epoch.get().wrapping_add(1));
        }
        self.flush_epoch.set(self.flush_epoch.get().wrapping_add(1));
        self.counters.flush_opened();
        let epoch = self.flush_epoch.get();
        if self.diagnostics_active.get() {
            self.flushes.open_flush(self.pending_jobs.borrow().len());
        }
        if self.diagnostics_active.get() {
            // Bind the count before emitting: written inline it is a temporary whose borrow lives
            // until the end of the statement, i.e. across the subscriber call. A subscriber that
            // schedules reactor work — an entirely reasonable thing to do from `FlushStarted` —
            // would then hit a bare `BorrowMutError` from inside adaptite.
            let pending_jobs = self.pending_jobs.borrow().len();
            self.emit(DiagnosticEvent::FlushStarted {
                reactor: self.id,
                flush_epoch: epoch,
                pending_jobs,
            });
        }

        // If a job panics, reset the flush flag and hand any remaining jobs to a fresh flush so
        // one panicking effect cannot silently disable the reactor.
        struct FlushGuard {
            inner: Rc<ReactorInner>,
            // Pinned at construction: a job may call `flush_now`, whose nested flush bumps the
            // shared epoch. Reporting the live value here would close the wrong flush.
            epoch: u64,
        }

        impl Drop for FlushGuard {
            fn drop(&mut self) {
                self.inner
                    .flush_depth
                    .set(self.inner.flush_depth.get().saturating_sub(1));
                if self.inner.diagnostics_active.get() {
                    let remaining_jobs = self.inner.pending_jobs.borrow().len();
                    // `None` when this subscriber arrived after the flush opened: there is no
                    // pair to close, so there is no close to report. See `end_flush`.
                    if let Some(stats) = self
                        .inner
                        .flushes
                        .close_flush(remaining_jobs, self.inner.counters.queued_effects())
                    {
                        self.inner.emit_from_drop(DiagnosticEvent::FlushFinished {
                            reactor: self.inner.id,
                            flush_epoch: self.epoch,
                            remaining_jobs,
                            stats,
                        });
                    }
                }
                self.inner.flush_scheduled.set(false);
                if !self.inner.pending_jobs.borrow().is_empty() {
                    self.inner.ensure_flush_scheduled();
                }
            }
        }

        self.flush_depth.set(self.flush_depth.get() + 1);
        let guard = FlushGuard {
            inner: Rc::clone(&self),
            epoch,
        };

        loop {
            let job = guard.inner.pending_jobs.borrow_mut().pop_front();
            let Some(job) = job else {
                break;
            };
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::GRAPH,
                event = "run_job",
                remaining_jobs = guard.inner.pending_jobs.borrow().len(),
                "running reactive scheduled job"
            );
            job();
        }
    }
}

#[cfg(test)]
mod tests;
