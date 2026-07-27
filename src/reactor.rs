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

use crate::{
    DiagnosticEvent, DiagnosticSubscription, InvalidationCause, InvalidationLevel, NodeId,
    ReactorId, trace_targets,
};

type Job = Box<dyn FnOnce() + 'static>;
type DiagnosticCallback = Rc<dyn Fn(DiagnosticEvent)>;

static NEXT_REACTOR_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_REACTOR: RefCell<Weak<ReactorInner>> = const { RefCell::new(Weak::new()) };
    /// Strong reference held for the lifetime of an [`EnterGuard`], so an explicitly entered
    /// reactor stays the thread default even when the caller holds no other handle.
    static ANCHORED_REACTOR: RefCell<Option<Rc<ReactorInner>>> = const { RefCell::new(None) };
    /// How many times a default reactor has been installed on this thread. A second install
    /// means the previous default expired, so nodes created before and after it are on
    /// different graphs — the silent failure [`Reactor::current`] warns about.
    static DEFAULT_INSTALL_COUNT: Cell<u32> = const { Cell::new(0) };
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
        CURRENT_REACTOR.replace(core::mem::take(&mut self.previous_default));
        ANCHORED_REACTOR.replace(self.previous_anchor.take());
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
    UNTRACKED_DEPTH.with(|depth| depth.set(depth.get() + 1));

    struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            UNTRACKED_DEPTH.with(|depth| depth.set(depth.get() - 1));
        }
    }

    let _guard = Guard;
    f()
}

fn is_untracked() -> bool {
    UNTRACKED_DEPTH.with(|depth| depth.get() > 0)
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
    inner: Rc<ReactorInner>,
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
            inner
                .diagnostics_active
                .set(!inner.diagnostics.borrow().is_empty());
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
    /// panic and nothing pointing at the cause. A re-install therefore logs at `warn` level on
    /// the `adaptite::graph` target.
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
        CURRENT_REACTOR.replace(Rc::downgrade(&reactor.inner));
        let installs = DEFAULT_INSTALL_COUNT.with(|count| {
            let installs = count.get().wrapping_add(1);
            count.set(installs);
            installs
        });
        if installs > 1 {
            // The previous default expired while this thread was still using the ambient
            // constructors. Nothing is broken *yet* — but nodes created from here on are on a
            // different graph than the ones created before, and the two can never interact.
            tracing::warn!(
                target: trace_targets::GRAPH,
                event = "current_reactor_reinstall",
                installs,
                "the thread's default reactor expired and a fresh, unrelated one was installed; \
                 nodes created before and after this point are on separate graphs. Hold the \
                 reactor alive with Reactor::enter, or use try_current to make the absence an \
                 error"
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
        CURRENT_REACTOR
            .with(|r| r.borrow().upgrade())
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
    pub fn enter(&self) -> EnterGuard {
        let previous_default = CURRENT_REACTOR.replace(Rc::downgrade(&self.inner));
        let previous_anchor = ANCHORED_REACTOR.replace(Some(Rc::clone(&self.inner)));
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
        self.clear_observer_dependencies(observer);
        self.inner.stack.borrow_mut().push(observer);
        let inserted = self.inner.active_computations.borrow_mut().insert(observer);
        debug_assert!(inserted, "observer should not already be active");
        #[cfg(debug_assertions)]
        let previous_running =
            RUNNING_REACTOR.with(|running| running.replace(Rc::as_ptr(&self.inner).cast::<()>()));

        struct Guard<'a> {
            inner: &'a ReactorInner,
            #[cfg(debug_assertions)]
            previous_running: *const (),
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                let popped = self.inner.stack.borrow_mut().pop();
                debug_assert!(popped.is_some(), "reactor observer stack underflow");
                if let Some(node) = popped {
                    let removed = self.inner.active_computations.borrow_mut().remove(&node);
                    debug_assert!(removed, "observer should have been active");
                }
                #[cfg(debug_assertions)]
                RUNNING_REACTOR.with(|running| running.set(self.previous_running));
            }
        }

        let _guard = Guard {
            inner: &self.inner,
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
            let origins = cycle.iter().map(|node| self.origin(*node)).collect();
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

        self.inner
            .dependencies
            .borrow_mut()
            .entry(observer)
            .or_default()
            .insert(observable, self.version(observable));
        let became_observed = {
            let mut dependents = self.inner.dependents.borrow_mut();
            let observers = dependents.entry(observable).or_default();
            let was_unobserved = observers.is_empty();
            observers.insert(observer);
            was_unobserved
        };
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
            self.origin(observable)
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
                cause,
            });
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
        let dependents = self
            .inner
            .dependents
            .borrow()
            .get(&observable)
            .map(|nodes| nodes.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "mark_dependents",
            observable_id = observable.0,
            dependent_count = dependents.len(),
            ?mark,
            "marking reactive dependents"
        );

        for dependent in dependents {
            let hook = self
                .inner
                .observers
                .borrow()
                .get(&dependent)
                .cloned()
                .and_then(|weak| weak.upgrade());
            if let Some(hook) = hook {
                hook.mark(mark, cause);
            } else {
                self.inner.observers.borrow_mut().remove(&dependent);
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
        for (dependency, seen_version) in self.dependencies_of(observer) {
            self.refresh_node(dependency);
            if self.version(dependency) != seen_version {
                return true;
            }
        }
        false
    }

    /// Returns `true` if any live observer currently records a dependency on `node`.
    ///
    /// This reflects the edges recorded by each observer's most recent run: an observer that
    /// stopped reading `node` still counts until it next re-runs (or is disposed). The primary
    /// use is garbage collection in fine-grained data structures — dropping per-key
    /// [`crate::Source`] nodes that no longer have readers.
    pub fn is_observed(&self, node: NodeId) -> bool {
        self.inner
            .dependents
            .borrow()
            .get(&node)
            .is_some_and(|observers| !observers.is_empty())
    }

    /// Disposes all graph bookkeeping for `node`.
    pub fn dispose(&self, node: NodeId) {
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "dispose_node",
            node_id = node.0,
            "disposing reactive node bookkeeping"
        );
        self.clear_observer_dependencies(node);

        let incoming = self
            .inner
            .dependents
            .borrow_mut()
            .remove(&node)
            .map(|nodes| nodes.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
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
        self.inner.meta.borrow_mut().remove(&node);
        self.unregister_observation_hooks(node);
    }

    /// Schedules a job to run in the reactor's microtask-backed job queue.
    pub fn schedule(&self, job: impl FnOnce() + 'static) {
        self.inner
            .pending_jobs
            .borrow_mut()
            .push_back(Box::new(job));
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
    /// the debug divergence guard — which counts an effect's runs within a flush — keeps working
    /// across the drain, and diagnostic consumers see one `FlushStarted`/`FlushFinished` pair
    /// rather than one per effect.
    ///
    /// Without it, each externally scheduled run opens and closes its own single-run flush. That
    /// is correct but blind: an effect that re-schedules itself into the same drain forever is
    /// then a livelock the guard cannot see. Draining inside `external_flush` is therefore the
    /// recommended shape.
    ///
    /// Nesting is permitted: a nested call (or a [`flush_now`](Self::flush_now) from inside `f`)
    /// joins the enclosing flush rather than starting one.
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
        self.inner.begin_flush();

        struct Guard<'a>(&'a ReactorInner);

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.end_flush();
            }
        }

        let _guard = Guard(&self.inner);
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
    pub(crate) fn allocate_node(&self) -> NodeId {
        let raw = self.inner.next_node.get();
        self.inner.next_node.set(raw.wrapping_add(1));
        let id = NodeId::new(raw);
        self.inner.meta.borrow_mut().insert(
            id,
            NodeMeta {
                version: 0,
                origin: Location::caller(),
            },
        );
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "allocate_node",
            node_id = id.0,
            "allocated reactive node id"
        );
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
    pub(crate) fn version(&self, node: NodeId) -> u64 {
        self.inner
            .meta
            .borrow()
            .get(&node)
            .map(|meta| meta.version)
            .unwrap_or(0)
    }

    /// Returns the source location at which `node` was created.
    pub(crate) fn origin(&self, node: NodeId) -> Option<&'static Location<'static>> {
        self.inner.meta.borrow().get(&node).map(|meta| meta.origin)
    }

    /// Returns the dependencies recorded during `observer`'s last run, with the version of each
    /// dependency observed at that time.
    pub(crate) fn dependencies_of(&self, observer: NodeId) -> Vec<(NodeId, u64)> {
        self.inner
            .dependencies
            .borrow()
            .get(&observer)
            .map(|edges| edges.iter().map(|(id, version)| (*id, *version)).collect())
            .unwrap_or_default()
    }

    /// Returns the number of the currently running (or most recent) job flush.
    pub(crate) fn flush_epoch(&self) -> u64 {
        self.inner.flush_epoch.get()
    }

    pub(crate) fn emit_diagnostic(&self, event: DiagnosticEvent) {
        self.inner.emit(event);
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
        RUNNING_REACTOR.with(|running| {
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

    fn clear_observer_dependencies(&self, observer: NodeId) {
        let observed = self
            .inner
            .dependencies
            .borrow_mut()
            .remove(&observer)
            .map(|edges| edges.into_keys().collect::<Vec<_>>())
            .unwrap_or_default();

        let mut unobserved = Vec::new();
        for observable in observed {
            let mut dependents = self.inner.dependents.borrow_mut();
            if let Some(observers) = dependents.get_mut(&observable) {
                observers.remove(&observer);
                if observers.is_empty() {
                    dependents.remove(&observable);
                    unobserved.push(observable);
                }
            }
        }

        // This runs during an observer's rerun or disposal, with graph maps borrowed; the
        // notification is deferred, so hooks never observe a half-updated graph.
        for observable in unobserved {
            self.note_observation_change(observable, false);
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

struct NodeMeta {
    version: u64,
    origin: &'static Location<'static>,
}

struct ReactorInner {
    id: ReactorId,
    next_node: Cell<u64>,
    meta: RefCell<HashMap<NodeId, NodeMeta>>,
    dependencies: RefCell<HashMap<NodeId, HashMap<NodeId, u64>>>,
    dependents: RefCell<HashMap<NodeId, HashSet<NodeId>>>,
    observers: RefCell<HashMap<NodeId, Weak<dyn ObserverHook>>>,
    stack: RefCell<Vec<NodeId>>,
    active_computations: RefCell<HashSet<NodeId>>,
    pending_jobs: RefCell<VecDeque<Job>>,
    flush_scheduled: Cell<bool>,
    flush_epoch: Cell<u64>,
    /// Nesting depth of active flushes, including drains a consumer opened with
    /// [`Reactor::external_flush`]. Non-zero means the current `flush_epoch` is live, so an
    /// externally scheduled effect run joins it instead of opening one of its own.
    flush_depth: Cell<u32>,
    next_diagnostic: Cell<u64>,
    diagnostics_active: Cell<bool>,
    diagnostics: RefCell<Vec<(u64, DiagnosticCallback)>>,
    observation_hooks: RefCell<HashMap<NodeId, Rc<ObservationHooks>>>,
}

impl ReactorInner {
    fn new() -> Self {
        Self {
            id: ReactorId(NEXT_REACTOR_ID.fetch_add(1, AtomicOrdering::Relaxed)),
            next_node: Cell::new(1),
            meta: RefCell::new(HashMap::new()),
            dependencies: RefCell::new(HashMap::new()),
            dependents: RefCell::new(HashMap::new()),
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

        self.flush_epoch.set(self.flush_epoch.get().wrapping_add(1));
        let epoch = self.flush_epoch.get();
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "begin_external_flush",
            flush_epoch = epoch,
            "opened an externally driven flush"
        );
        if self.diagnostics_active.get() {
            self.emit(DiagnosticEvent::FlushStarted {
                reactor: self.id,
                flush_epoch: epoch,
                pending_jobs: self.pending_jobs.borrow().len(),
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
            self.emit(DiagnosticEvent::FlushFinished {
                reactor: self.id,
                flush_epoch: self.flush_epoch.get(),
                remaining_jobs: self.pending_jobs.borrow().len(),
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
        let _span = tracing::debug_span!(
            target: trace_targets::GRAPH,
            "reactor.flush_jobs"
        )
        .entered();
        self.flush_epoch.set(self.flush_epoch.get().wrapping_add(1));
        let epoch = self.flush_epoch.get();
        if self.diagnostics_active.get() {
            self.emit(DiagnosticEvent::FlushStarted {
                reactor: self.id,
                flush_epoch: epoch,
                pending_jobs: self.pending_jobs.borrow().len(),
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
                    self.inner.emit(DiagnosticEvent::FlushFinished {
                        reactor: self.inner.id,
                        flush_epoch: self.epoch,
                        remaining_jobs: self.inner.pending_jobs.borrow().len(),
                    });
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
mod tests {
    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;

    use runite::{queue_macrotask, run};

    use super::{Reactor, current};

    #[test]
    fn current_reactor_is_thread_local_singleton() {
        let one = current();
        let two = current();
        assert!(Rc::ptr_eq(&one.inner, &two.inner));
    }

    #[test]
    fn try_current_reports_absence_instead_of_installing_a_reactor() {
        // The test harness gives each test a fresh thread, so nothing is installed yet.
        assert!(
            super::try_current().is_none(),
            "try_current must not install a reactor"
        );
        assert!(
            super::try_current().is_none(),
            "and must still report absence after being asked once"
        );

        let installed = current();
        let observed = super::try_current().expect("current installed a default");
        assert_eq!(observed.id(), installed.id());
    }

    #[test]
    fn entering_anchors_the_reactor_as_the_thread_default() {
        let reactor = Reactor::new();
        let expected = reactor.id();
        let guard = reactor.enter();

        // Drop every other handle: the guard alone must keep this reactor current. Without the
        // strong anchor, `current()` here would install a fresh, unrelated graph.
        drop(reactor);

        assert_eq!(
            current().id(),
            expected,
            "the entered reactor stays current with no other handle alive"
        );

        drop(guard);
        assert!(
            super::try_current().is_none(),
            "dropping the guard restores the absent default"
        );
    }

    #[test]
    fn entering_nests_and_restores_the_previous_default() {
        let outer = Reactor::new();
        let inner = Reactor::new();
        let (outer_id, inner_id) = (outer.id(), inner.id());

        let outer_guard = outer.enter();
        assert_eq!(current().id(), outer_id);

        let inner_guard = inner.enter();
        assert_eq!(current().id(), inner_id);

        drop(inner_guard);
        assert_eq!(
            current().id(),
            outer_id,
            "leaving the inner reactor restores the outer one"
        );

        drop(outer_guard);
        assert!(super::try_current().is_none());
    }

    #[test]
    fn an_expired_default_is_replaced_by_an_unrelated_reactor() {
        // This is the failure mode `current()` warns about, pinned so the warning keeps
        // describing something real: nodes created on either side of the expiry cannot interact.
        let first = current().id();
        // No node, handle, or guard survives, so the weak cache expires.
        let second = current().id();

        assert_ne!(
            first, second,
            "an unanchored default is replaced once nothing keeps it alive"
        );

        // Holding any handle is enough to keep it stable.
        let held = current();
        assert_eq!(current().id(), held.id());
    }

    #[test]
    fn observe_records_dependency_edges_with_versions() {
        let reactor = Reactor::new();
        let observer = reactor.allocate_node();
        let observable = reactor.allocate_node();
        reactor.trigger(observable);

        reactor.run_in_context(observer, || {
            reactor
                .try_observe(observable)
                .expect("should not detect cycle")
        });

        assert_eq!(
            reactor.dependencies_of(observer),
            vec![(observable, reactor.version(observable))]
        );
        assert_eq!(
            reactor.inner.dependents.borrow().get(&observable),
            Some(&[observer].into_iter().collect())
        );
    }

    #[test]
    fn cycle_detection_panics_with_path_and_origins() {
        let reactor = Reactor::new();
        let a = reactor.allocate_node();
        let b = reactor.allocate_node();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            reactor.run_in_context(a, || {
                reactor.observe(b);
                reactor.run_in_context(b, || {
                    reactor.observe(a);
                });
            });
        }))
        .expect_err("cycle should panic");

        let Some(cycle_error) = panic.downcast_ref::<String>() else {
            panic!("panic should be a string");
        };

        assert!(
            cycle_error.contains("reactive cycle detected"),
            "panic should indicate cycle detected"
        );

        assert!(
            cycle_error.contains("1 (created at")
                && cycle_error.contains("-> 2 (created at")
                && cycle_error.contains("reactor.rs"),
            "panic should include the cycle path with node origins, got: {cycle_error}"
        );
    }

    #[test]
    fn scheduled_jobs_flush_on_runtime_microtask_queue() {
        let observed = Rc::new(Cell::new(0usize));

        queue_macrotask({
            let observed = Rc::clone(&observed);
            move || {
                let reactor = Reactor::new();
                reactor.schedule({
                    let observed = Rc::clone(&observed);
                    move || observed.set(1)
                });
                assert_eq!(observed.get(), 0);
            }
        });

        run();

        assert_eq!(observed.get(), 1);
    }

    #[test]
    fn graph_survives_dropping_the_reactor_handle() {
        let seen = Rc::new(std::cell::RefCell::new(Vec::new()));
        let keep_alive = Rc::new(std::cell::RefCell::new(None::<crate::EffectHandle>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let keep_alive = Rc::clone(&keep_alive);
            move || {
                let reactor = Reactor::new();
                let source = crate::signal_in(&reactor, 1usize);
                let effect = reactor.effect({
                    let seen = Rc::clone(&seen);
                    let source = source.clone();
                    move || seen.borrow_mut().push(source.get())
                });
                *keep_alive.borrow_mut() = Some(effect);

                // Nodes hold the reactor alive; the user's handle is not load-bearing.
                drop(reactor);

                runite::queue_macrotask(move || {
                    source.set(2);
                });
            }
        });

        run();

        assert_eq!(&*seen.borrow(), &[1, 2]);
    }

    #[test]
    fn flush_recovers_after_a_panicking_job() {
        let observed = Rc::new(Cell::new(0usize));

        queue_macrotask({
            let observed = Rc::clone(&observed);
            move || {
                let reactor = Reactor::new();
                reactor.schedule(|| panic!("job panics"));
                // Swallow the panic that propagates out of the microtask flush so the test can
                // observe the reactor's recovery.
                let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
                assert!(result.is_err(), "flush should propagate the job panic");

                reactor.schedule({
                    let observed = Rc::clone(&observed);
                    move || observed.set(1)
                });
                reactor.flush_now();
            }
        });

        run();

        assert_eq!(observed.get(), 1);
    }
}
