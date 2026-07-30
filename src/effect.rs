use alloc::boxed::Box;
use alloc::rc::{Rc, Weak};
use core::cell::{Cell, RefCell};

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::reactor::{Mark, ObserverHook, State};
use crate::scope::{
    OwnedDisposable, OwnerFrame, adopt_into_current, build_error_info, error_handler_for,
    with_owner,
};
use crate::{
    DiagnosticEvent, InvalidationCause, NodeId, NodeKind, Reactor, ReactorId, current,
    trace_targets,
};

/// Maximum number of times a single effect may run within one job flush before the reactor
/// assumes it is caught in a divergent feedback loop.
///
/// Enforced in every build. Convergent feedback settles far below this.
const MAX_RUNS_PER_FLUSH: u32 = 100;

/// Creates an effect in the current thread's default reactor.
///
/// The effect is scheduled immediately and then re-scheduled whenever one of its dependencies
/// changes. Effects never run inline with the write that triggered them: they are flushed on
/// the runtime's microtask queue, so consecutive writes within one task coalesce into a single
/// run. Use [`effect_with`] to route runs to a lane of your own instead.
///
/// A queued run first verifies its inputs: when only equality-suppressed memo updates
/// occurred upstream, the run is skipped without executing the body. If an upstream
/// computation panics during that verification, the panic propagates out of the flush and the
/// effect re-marks itself dirty and re-queues, so it recovers once the underlying cause is
/// fixed.
///
/// Each run is an ownership scope: [`crate::on_cleanup`] callbacks and nested effects
/// registered during a run are disposed before the next run, and again when the effect itself
/// is disposed.
///
/// # Panics
///
/// Panics — in **every** build, release included — when the effect runs more than 100 times
/// within a single *drain*: the outermost flush and everything nested inside it, so a re-entrant
/// [`Reactor::flush_now`] cannot reset the count. That indicates a divergent feedback loop: the
/// effect writes state it (transitively) depends on with a value that never converges. The panic
/// message names the effect's creation site. Convergent feedback (for example clamping, where the
/// rewritten value is suppressed by the signal's equality check) is legal and settles well below
/// the limit.
///
/// The guard is deliberately not scoped to `debug_assertions`. The alternative to panicking in a
/// release build is not that the application carries on — it is a flush that never returns, with
/// no panic, no log and nothing for a user to report.
///
/// Both behaviors above describe an effect with no enclosing error boundary. Under a
/// [`crate::scope_catch`], any panic from this effect — from its body or from dependency
/// verification — is instead delivered to that boundary's handler, and the effect is disposed
/// rather than re-queued.
///
/// # Examples
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{effect, signal};
/// use runite::{queue_macrotask, run};
///
/// let seen = Rc::new(RefCell::new(Vec::new()));
///
/// queue_macrotask({
///     let seen = Rc::clone(&seen);
///     move || {
///         let value = signal(1);
///         effect({
///             let seen = Rc::clone(&seen);
///             let value = value.clone();
///             move || seen.borrow_mut().push(value.get())
///         })
///         .leak();
///
///         // Coalesces with the initial run: the effect observes only the final value.
///         value.set(2);
///     }
/// });
/// run();
///
/// assert_eq!(*seen.borrow(), [2]);
/// ```
#[track_caller]
pub fn effect(f: impl Fn() + 'static) -> EffectHandle {
    current().effect(f)
}

/// Creates an effect associated with `reactor`.
#[track_caller]
pub fn effect_in(reactor: &Reactor, f: impl Fn() + 'static) -> EffectHandle {
    reactor.effect(f)
}

/// Creates an effect in the current thread's default reactor that is scheduled by `scheduler`
/// instead of the reactor's microtask lane.
///
/// See [`Reactor::effect_with`] for the contract.
#[track_caller]
pub fn effect_with(
    scheduler: impl EffectScheduler + 'static,
    f: impl Fn() + 'static,
) -> EffectHandle {
    current().effect_with(scheduler, f)
}

/// Creates an effect associated with `reactor` that is scheduled by `scheduler`.
#[track_caller]
pub fn effect_with_in(
    reactor: &Reactor,
    scheduler: impl EffectScheduler + 'static,
    f: impl Fn() + 'static,
) -> EffectHandle {
    reactor.effect_with(scheduler, f)
}

/// Decides when a ready effect actually runs.
///
/// Adaptite marks effects, coalesces repeat marks, and verifies dependencies; a scheduler
/// controls only *where and when* the resulting run happens. That is enough to build effect
/// phases — state-propagation before DOM writes before after-paint — without adaptite hardcoding
/// a phase list: create one queue per phase and drain them in the order and at the moment you
/// choose. A render lane can drain inside the host's paint callback rather than on the microtask
/// queue.
///
/// Any `Fn(EffectRun)` is a scheduler, so a closure over a queue is usually all a consumer needs.
///
/// # Contract
///
/// - [`schedule`](Self::schedule) is called when the effect becomes ready — on creation and on
///   each invalidation that is not coalesced into a pending run. It must not run the effect
///   inline; store the [`EffectRun`] and run it later.
/// - An [`EffectRun`] that is never run means the effect never runs. Dropping one is legal (the
///   effect stays marked and is re-scheduled on the next invalidation), but a scheduler that
///   routinely drops runs silently starves its effects.
/// - [`EffectRun::run`] must be called on the reactor's thread. Verification and the effect body
///   always execute there; adaptite provides no cross-thread hand-off.
/// - Drain inside [`Reactor::external_flush`] to give a whole drain one flush epoch.
pub trait EffectScheduler {
    /// Called when `ready` becomes runnable. Store it and run it when the consumer's phase is due.
    fn schedule(&self, ready: EffectRun);
}

impl<F: Fn(EffectRun)> EffectScheduler for F {
    fn schedule(&self, ready: EffectRun) {
        self(ready);
    }
}

/// A ready effect, handed to an [`EffectScheduler`] for the consumer to run when its phase is due.
///
/// Running performs the same verify-then-body sequence the reactor's own lane performs: a run
/// whose dependencies turn out not to have changed (an equality-suppressed memo upstream) skips
/// the body.
///
/// An `EffectRun` holds only a weak reference, so it never keeps a disposed effect alive; running
/// one whose effect has been disposed or dropped is a no-op.
pub struct EffectRun {
    effect: Weak<EffectInner>,
}

impl EffectRun {
    /// Verifies the effect's dependencies and runs its body if they actually changed.
    ///
    /// Must be called on the reactor's thread. When called outside any flush, the run opens a
    /// flush of its own; see [`Reactor::external_flush`] for draining a batch as one flush.
    pub fn run(mut self) {
        let Some(effect) = self.effect.upgrade() else {
            return;
        };
        // Disarm the discard handling below: `run_scheduled` clears `scheduled` itself, and it
        // must stay set until then so marks arriving mid-run coalesce instead of queueing a
        // second run.
        self.effect = Weak::new();

        if effect.reactor.in_flush() {
            effect.run_scheduled();
            return;
        }

        // Outside any flush, this single run *is* the flush: it needs its own epoch so the
        // divergence guard does not accumulate unrelated runs into whichever epoch the reactor's
        // last microtask flush happened to leave behind.
        let reactor = effect.reactor.clone();

        struct Guard<'a>(&'a Reactor);

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.end_flush();
            }
        }

        // Armed *before* the flush is opened, for the same reason as `Reactor::external_flush`:
        // `begin_flush` increments `flush_depth` first and calls consumer code — the
        // `FlushStarted` subscriber — last. Constructing the guard afterwards meant a panic
        // escaping that subscriber stranded `flush_depth` at 1 for the rest of the process, and
        // a stranded depth freezes `drain_epoch`, so the divergence guard then panicked on the
        // 101st ordinary run of whatever innocent effect came next. Arming first costs nothing:
        // the guard's only job is the matching decrement, and `end_flush` saturates.
        let _guard = Guard(&reactor);
        reactor.begin_flush();
        effect.run_scheduled();
    }

    /// Returns the effect's node id, for a scheduler that keys queues or diagnostics by node.
    pub fn id(&self) -> Option<NodeId> {
        self.effect.upgrade().map(|effect| effect.id)
    }

    /// Returns `true` when the effect has been disposed or dropped, so running this would be a
    /// no-op. A scheduler may use it to drop stale entries from a queue.
    pub fn is_stale(&self) -> bool {
        self.effect
            .upgrade()
            .is_none_or(|effect| effect.disposed.get())
    }
}

impl Drop for EffectRun {
    /// Releases the effect's "already scheduled" latch when a scheduler discards a run.
    ///
    /// The latch is what makes repeat invalidations coalesce into one pending run. It is cleared
    /// when a run executes; without clearing it here too, a discarded run would leave the effect
    /// latched forever and every later invalidation would coalesce into a run that no longer
    /// exists — permanent, silent starvation. The effect keeps its dirty mark, so the next
    /// invalidation schedules it afresh and the missed change is not lost.
    fn drop(&mut self) {
        if let Some(effect) = self.effect.upgrade() {
            effect.unlatch_scheduled();
        }
    }
}

impl core::fmt::Debug for EffectRun {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EffectRun")
            .field("id", &self.id())
            .field("stale", &self.is_stale())
            .finish()
    }
}

/// Disposable handle for a reactive effect.
///
/// An effect created outside any owner is disposed when the last clone of its handle is
/// dropped. An effect created inside an owner (another effect's run, or a [`crate::scope`]) is
/// kept alive by that owner instead and is disposed with it, so its handle may be discarded.
#[derive(Clone)]
#[must_use = "an unowned effect is disposed when its handle is dropped; hold the handle, leak it, or create the effect inside a scope"]
pub struct EffectHandle {
    inner: Rc<EffectInner>,
}

impl Reactor {
    /// Creates an effect associated with this reactor.
    ///
    /// The effect is scheduled immediately and then re-scheduled whenever one of its dependencies
    /// changes.
    ///
    /// # Panics
    ///
    /// In every build, release included, when the effect runs more than 100 times within a single
    /// drain — a divergent feedback loop. The free function `effect` documents the guard in full.
    #[track_caller]
    pub fn effect(&self, f: impl Fn() + 'static) -> EffectHandle {
        EffectHandle::new(self.clone(), None, f)
    }

    /// Creates an effect scheduled by `scheduler` rather than by this reactor's microtask lane.
    ///
    /// Marking, coalescing, and dependency verification are unchanged; the scheduler decides only
    /// when the ready run happens. Consumers build effect phases this way — one queue per phase,
    /// drained in whatever order and at whatever moment suits the host — instead of adaptite
    /// imposing a phase list. See [`EffectScheduler`] for the contract and
    /// [`external_flush`](Self::external_flush) for draining a queue as one flush.
    ///
    /// # Panics
    ///
    /// In every build, release included, when the effect runs more than 100 times within a single
    /// drain — a divergent feedback loop. The free function `effect` documents the guard in full.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    ///
    /// use adaptite::{EffectRun, Reactor, signal_in};
    ///
    /// let reactor = Reactor::new();
    /// let lane: Rc<RefCell<Vec<EffectRun>>> = Rc::new(RefCell::new(Vec::new()));
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
    /// // The reactor's own lane never runs this effect.
    /// reactor.flush_now();
    /// assert!(seen.borrow().is_empty());
    ///
    /// reactor.external_flush(|| {
    ///     for ready in lane.borrow_mut().drain(..) {
    ///         ready.run();
    ///     }
    /// });
    /// assert_eq!(*seen.borrow(), [1]);
    /// # effect.dispose();
    /// ```
    #[track_caller]
    pub fn effect_with(
        &self,
        scheduler: impl EffectScheduler + 'static,
        f: impl Fn() + 'static,
    ) -> EffectHandle {
        EffectHandle::new(self.clone(), Some(Rc::new(scheduler)), f)
    }
}

impl EffectHandle {
    #[track_caller]
    fn new(
        reactor: Reactor,
        scheduler: Option<Rc<dyn EffectScheduler>>,
        effect: impl Fn() + 'static,
    ) -> Self {
        let id = reactor.allocate_node(NodeKind::Effect);
        let inner = Rc::new(EffectInner {
            reactor: reactor.clone(),
            id,
            effect: Box::new(effect),
            scheduler,
            state: Cell::new(State::Dirty),
            scheduled: Cell::new(false),
            rerun_after_current: Cell::new(false),
            running: Cell::new(false),
            disposed: Cell::new(false),
            self_ref: RefCell::new(Weak::new()),
            owner: OwnerFrame::new(),
            last_drain_epoch: Cell::new(u64::MAX),
            runs_this_drain: Cell::new(0),
        });
        *inner.self_ref.borrow_mut() = Rc::downgrade(&inner);
        tracing::debug!(
            target: trace_targets::EFFECT,
            event = "create_effect",
            node_id = id.0,
            "created reactive effect"
        );

        let observer: Rc<dyn ObserverHook> = inner.clone();
        reactor.register_observer(id, observer);
        // If an owner (an enclosing effect run or scope) is active, it keeps this effect alive
        // and disposes it; otherwise the handle alone manages the effect's lifetime.
        let owned: Rc<dyn OwnedDisposable> = inner.clone();
        let _ = adopt_into_current(owned);
        inner.schedule();
        Self { inner }
    }

    /// Consumes the handle without disposing the effect, letting the effect run for the
    /// remainder of the program.
    ///
    /// This forfeits only the handle's lifetime management: an effect created inside an owner
    /// (another effect's run or a [`crate::scope`]) is still disposed with that owner. The
    /// handle cannot be recovered afterwards, so only leak effects you will never need to
    /// dispose explicitly.
    pub fn leak(self) {
        core::mem::forget(self);
    }

    /// Disposes the effect immediately: runs cleanups registered during its last run, disposes
    /// nested effects and scopes it owns, and unhooks it from the graph. A run already queued
    /// for the next flush is skipped. Disposing an already-disposed effect is a no-op.
    ///
    /// # Panics
    ///
    /// Teardown is total, so a panicking cleanup does not strand the rest: every cleanup and
    /// every owned child gets an attempt regardless, and the effect is unhooked from the graph
    /// either way. But the failure is not swallowed — the **first** panic captured during
    /// teardown is re-raised out of this call once teardown has finished, and later ones are
    /// logged at `error` and dropped. So `dispose` can unwind with a payload thrown by consumer
    /// code, not by adaptite.
    ///
    /// The exception is teardown that begins while the thread is *already* unwinding — this
    /// method reached from a `Drop` during a panic. Re-raising there would abort the process, so
    /// the payload is logged instead and the original panic continues to propagate. See
    /// `docs/MIGRATING-0.3.md` ("Behaviour change: teardown is total") for the full contract.
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
    /// reactor.flush_now();
    /// assert_eq!(*seen.borrow(), [1]);
    ///
    /// effect.dispose();
    /// assert!(effect.is_disposed());
    ///
    /// value.set(2); // no longer observed: nothing is queued
    /// reactor.flush_now();
    /// assert_eq!(*seen.borrow(), [1]);
    /// ```
    pub fn dispose(&self) {
        self.inner.dispose();
    }

    /// Returns `true` if the effect has been disposed.
    pub fn is_disposed(&self) -> bool {
        self.inner.disposed.get()
    }

    /// Returns the effect's node identity, stable for as long as the effect exists.
    ///
    /// This is the same id [`EffectRun::id`] reports, available from the moment the effect is
    /// created rather than from its first scheduled run — which is what lets a consumer key a
    /// retained structure by effect without bookkeeping around the initial run.
    ///
    /// Node ids are process-local and **never reused**: the allocator is a monotonic counter and
    /// disposal does not return an id to it. An id kept past disposal therefore dangles, but it
    /// can never come to mean a different node. Pair it with [`is_disposed`](Self::is_disposed)
    /// when liveness matters. The id grants no access to the graph.
    ///
    /// [`NodeId`] is unique only within one reactor, so aggregate on
    /// `(`[`reactor_id`](Self::reactor_id)`, id)`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::Reactor;
    ///
    /// let reactor = Reactor::new();
    /// let effect = reactor.effect(|| {});
    ///
    /// // Stable across runs and across disposal.
    /// let id = effect.id();
    /// reactor.flush_now();
    /// assert_eq!(effect.id(), id);
    /// effect.dispose();
    /// assert_eq!(effect.id(), id);
    ///
    /// assert_eq!(effect.reactor_id(), reactor.id());
    /// ```
    pub fn id(&self) -> NodeId {
        self.inner.id
    }

    /// Returns the identity of the reactor this effect belongs to.
    ///
    /// Diagnostic payloads are scoped `(ReactorId, NodeId)`; this is the half a handle could not
    /// supply before.
    pub fn reactor_id(&self) -> ReactorId {
        self.inner.reactor.id()
    }
}

impl core::fmt::Debug for EffectHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EffectHandle")
            .field("id", &self.inner.id)
            .field("disposed", &self.inner.disposed.get())
            .finish()
    }
}

struct EffectInner {
    reactor: Reactor,
    id: NodeId,
    effect: Box<dyn Fn() + 'static>,
    /// When set, ready runs are handed to this scheduler instead of the reactor's job queue.
    scheduler: Option<Rc<dyn EffectScheduler>>,
    state: Cell<State>,
    scheduled: Cell<bool>,
    /// Set when a run was requested while this effect was already running, so the run can be
    /// re-queued once the current one finishes instead of re-entering it.
    rerun_after_current: Cell<bool>,
    /// Set for the whole of a run — teardown *and* body — so re-entry is deferred across both.
    ///
    /// The reactor's tracked-computation window opens at the body, but `OwnerFrame::reset` runs
    /// consumer cleanups before it, and a cleanup that writes a dependency and flushes reaches
    /// this effect while its previous generation of children and cleanups is still being taken
    /// down. Re-entering there is as incoherent as re-entering the body: both generations end up
    /// live at once.
    running: Cell<bool>,
    disposed: Cell<bool>,
    self_ref: RefCell<Weak<EffectInner>>,
    /// Ownership frame for cleanups and nested effects created during this effect's runs.
    owner: Rc<OwnerFrame>,
    /// Divergence guard state. Deliberately *not* `cfg(debug_assertions)`: a runaway loop in a
    /// release build is a frozen application with no output, which is the worst failure mode a UI
    /// host can have and the one configuration where adaptite used to say nothing.
    last_drain_epoch: Cell<u64>,
    runs_this_drain: Cell<u32>,
}

impl EffectInner {
    /// Claims the "a run is pending" latch, returning `true` if one already was.
    ///
    /// The latch and the reactor's queued-effect gauge are the same fact, so they move together
    /// here rather than at each of the four sites that touch the latch — the gauge cannot drift
    /// from the thing it reports.
    fn latch_scheduled(&self) -> bool {
        let already = self.scheduled.replace(true);
        if !already {
            self.reactor.counters().effect_queued();
        }
        already
    }

    /// Releases the latch, whether the pending run was executed or discarded.
    fn unlatch_scheduled(&self) {
        if self.scheduled.replace(false) {
            self.reactor.counters().effect_unqueued();
        }
    }

    fn schedule(&self) {
        let skipped = self.disposed.get() || self.latch_scheduled();
        self.reactor.record_flush(|stats| {
            if skipped {
                stats.effects_coalesced = stats.effects_coalesced.saturating_add(1);
            } else {
                stats.effects_queued = stats.effects_queued.saturating_add(1);
            }
        });
        if self.reactor.diagnostics_enabled()
            && let Some(effect_origin) = self.reactor.node_origin(self.id)
        {
            self.reactor
                .emit_diagnostic(DiagnosticEvent::EffectScheduled {
                    reactor: self.reactor.diagnostic_id(),
                    effect: self.id,
                    effect_origin,
                    queued: !skipped,
                    flush_epoch: self.reactor.flush_epoch(),
                });
        }
        if skipped {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::EFFECT,
                event = "schedule_effect",
                node_id = self.id.0,
                queued = false,
                disposed = self.disposed.get(),
                already_scheduled = self.scheduled.get(),
                "effect scheduling skipped"
            );
            return;
        }

        let weak = self.self_ref.borrow().clone();

        if let Some(scheduler) = &self.scheduler {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::EFFECT,
                event = "schedule_effect",
                node_id = self.id.0,
                queued = true,
                external = true,
                "handed ready effect to a consumer scheduler"
            );
            // The scheduler is free to run this inline, which would re-enter `schedule` through a
            // write in the body; `scheduled` is already set, so that re-entry coalesces rather
            // than recursing.
            scheduler.schedule(EffectRun { effect: weak });
            return;
        }

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::EFFECT,
            event = "schedule_effect",
            node_id = self.id.0,
            queued = true,
            external = false,
            "queued effect for microtask flush"
        );
        let reactor = self.reactor.clone();
        reactor.schedule(move || {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            inner.run_scheduled();
        });
    }

    /// Runs the effect, routing a panic to the nearest enclosing error boundary when there is one.
    ///
    /// The whole run is covered, not just the body: dependency verification executes upstream
    /// computations, and a panic there is exactly the kind of failure a boundary exists to
    /// contain.
    fn run_scheduled(self: &Rc<Self>) {
        let Some(handler) = error_handler_for(&self.owner) else {
            self.run_scheduled_inner();
            return;
        };

        // `AssertUnwindSafe` is the honest choice here rather than a workaround: the reactor is
        // already built to survive a panicking effect — the flush guard hands remaining jobs to a
        // fresh flush, and computed nodes restore their dirty mark on unwind — and containing
        // exactly that damage is what a boundary is for.
        let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.run_scheduled_inner())) else {
            return;
        };

        let origin = self.reactor.node_origin(self.id);
        tracing::warn!(
            target: trace_targets::EFFECT,
            event = "effect_panicked",
            node_id = self.id.0,
            "effect panicked and was disposed by an enclosing error boundary"
        );
        // Dispose before reporting. A panicking effect had its dependency tracking cut short
        // mid-run, and a panic during verification re-queues it, so leaving it live would re-run
        // and re-panic immediately. Disposal makes the failure terminal for this effect and leaves
        // the handler to decide what replaces it.
        self.dispose();
        handler(build_error_info(payload, self.id, origin));
    }

    fn run_scheduled_inner(self: &Rc<Self>) {
        if self.disposed.get() {
            self.unlatch_scheduled();
            return;
        }

        self.unlatch_scheduled();

        // A nested flush can reach an effect that is already running — an effect that writes a
        // dependency and then calls `flush_now` does exactly that, and both halves of it are
        // documented as legal. Re-entering cannot be tracked coherently, because the inner run
        // clears the dependency set the outer run is still recording. So defer: remember that a
        // run is owed and re-queue it once the current run finishes. The state mark is left
        // alone, so the deferred run still sees why it was scheduled.
        //
        // `running` covers the whole run and `is_computation_active` only the tracked window
        // inside it; the wider one is the load-bearing test, because teardown runs consumer
        // cleanups before the tracked window opens and re-entry from a cleanup would otherwise
        // slip through and leave two generations of children and cleanups live at once.
        if self.running.get() || self.reactor.is_computation_active(self.id) {
            self.rerun_after_current.set(true);
            return;
        }

        let state = self.state.get();
        self.state.set(State::Clean);

        // A Check mark means only computed dependencies may have changed; verify them so that
        // equality-suppressed memo updates do not rerun the effect.
        let should_run = match state {
            State::Dirty => true,
            State::Check => {
                // Verification runs upstream memo computations. If one of them unwinds, the
                // memo stays stale, so its next upstream write will not re-propagate a mark —
                // without recovery this effect would be silently stranded as Clean forever.
                // Restore the mark and re-queue on unwind.
                struct RecoverOnUnwind<'a> {
                    inner: &'a EffectInner,
                    armed: bool,
                }

                impl Drop for RecoverOnUnwind<'_> {
                    fn drop(&mut self) {
                        if self.armed && !self.inner.disposed.get() {
                            self.inner.state.set(State::Dirty);
                            self.inner.schedule();
                        }
                    }
                }

                let mut guard = RecoverOnUnwind {
                    inner: self,
                    armed: true,
                };
                let changed = self.reactor.dependencies_changed(self.id);
                guard.armed = false;
                changed
            }
            State::Clean => false,
        };

        // Verification runs user computations, which may have disposed this effect; do not
        // reset the owner or run the body after disposal.
        if self.disposed.get() {
            return;
        }

        if !should_run {
            if self.reactor.diagnostics_enabled() {
                self.reactor
                    .emit_diagnostic(DiagnosticEvent::EffectRunSkipped {
                        reactor: self.reactor.diagnostic_id(),
                        effect: self.id,
                        flush_epoch: self.reactor.flush_epoch(),
                    });
            }
            self.reactor.record_flush(|stats| {
                stats.effects_skipped = stats.effects_skipped.saturating_add(1);
            });
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::EFFECT,
                event = "skip_effect",
                node_id = self.id.0,
                "skipping effect run; no dependency actually changed"
            );
            return;
        }

        self.check_divergence();

        let _span = tracing::debug_span!(
            target: trace_targets::EFFECT,
            "effect.run",
            node_id = self.id.0
        )
        .entered();
        let reactor_id = self.reactor.diagnostic_id();
        let flush_epoch = self.reactor.flush_epoch();
        let diagnostics_enabled = self.reactor.diagnostics_enabled();
        if diagnostics_enabled && let Some(effect_origin) = self.reactor.node_origin(self.id) {
            self.reactor
                .emit_diagnostic(DiagnosticEvent::EffectRunStarted {
                    reactor: reactor_id,
                    effect: self.id,
                    effect_origin,
                    flush_epoch,
                });
        }
        self.reactor
            .record_flush(|stats| stats.effects_run = stats.effects_run.saturating_add(1));
        struct DiagnosticRunGuard<'a> {
            reactor: &'a Reactor,
            reactor_id: crate::ReactorId,
            effect: NodeId,
            flush_epoch: u64,
            enabled: bool,
        }
        impl Drop for DiagnosticRunGuard<'_> {
            fn drop(&mut self) {
                if self.enabled {
                    self.reactor
                        .emit_diagnostic(DiagnosticEvent::EffectRunFinished {
                            reactor: self.reactor_id,
                            effect: self.effect,
                            flush_epoch: self.flush_epoch,
                        });
                }
            }
        }
        let _diagnostic_guard = DiagnosticRunGuard {
            reactor: &self.reactor,
            reactor_id,
            effect: self.id,
            flush_epoch,
            enabled: diagnostics_enabled,
        };
        // The run window opens *here*, before teardown, not at `run_in_context` below: `reset`
        // runs consumer cleanups and child disposals, and a run that arrives during those has the
        // same reason to be deferred as one that arrives during the body. Cleared by the guard so
        // an unwinding cleanup or body still closes the window.
        struct RunWindow<'a>(&'a Cell<bool>);

        impl Drop for RunWindow<'_> {
            fn drop(&mut self) {
                self.0.set(false);
            }
        }

        self.running.set(true);
        let _run_window = RunWindow(&self.running);

        // Run cleanups from the previous run and dispose nested effects it created, then run
        // with this effect as the innermost owner so new cleanups and children register here.
        self.owner.reset();

        // Teardown runs user cleanups, which may have disposed this effect — a cleanup that
        // disposes the scope owning it, or the effect's own handle. The owner is torn down and
        // rejects children by then, so a body run here would register cleanups that fire
        // immediately and children that are disposed on the spot. Same reason as the check above
        // the verification block; `reset` is just as capable of disposing us as verification is.
        //
        // `EffectRunStarted` and `effects_run` are already accounted for above and are left
        // alone: the run did start and did perform its teardown, and the run pair still closes
        // through the guard. Only the body is abandoned.
        if self.disposed.get() {
            return;
        }

        with_owner(&self.owner, || {
            self.reactor.run_in_context(self.id, || (self.effect)())
        });

        // A run requested while this one was in progress was deferred rather than re-entered.
        // Queue it now. `schedule` coalesces, so a write that also scheduled normally does not
        // produce two runs.
        if self.rerun_after_current.replace(false) && !self.disposed.get() {
            self.schedule();
        }
    }

    /// Panics when this effect keeps re-running within a single drain, which indicates a
    /// divergent feedback loop: the effect writes state it (transitively) depends on with a
    /// value that never converges.
    ///
    /// Convergent feedback (for example clamping, where the rewritten value is suppressed by the
    /// signal's equality check on the next round) is legal and settles well below this limit.
    ///
    /// Checked in **every** build. The alternative to panicking here is not "the application
    /// carries on" — it is `flush_now` never returning, with no panic, no log and nothing for the
    /// user to report. A panic is strictly more recoverable and more attributable than a freeze.
    fn check_divergence(&self) {
        // Drain rather than flush: a re-entrant `flush_now` opens a new diagnostic epoch but must
        // not reset this counter, or an effect that re-flushes could evade the guard entirely.
        let epoch = self.reactor.drain_epoch();
        if self.last_drain_epoch.get() != epoch {
            self.last_drain_epoch.set(epoch);
            self.runs_this_drain.set(1);
            return;
        }

        let runs = self.runs_this_drain.get().saturating_add(1);
        self.runs_this_drain.set(runs);
        if runs > MAX_RUNS_PER_FLUSH {
            let origin = self
                .reactor
                .node_origin(self.id)
                .map(|location| location.to_string())
                .unwrap_or_else(|| "<unknown>".into());
            panic!(
                "adaptite: effect created at {origin} ran more than {MAX_RUNS_PER_FLUSH} times \
                 in a single drain; this suggests a divergent reactive feedback loop (the effect \
                 writes state it depends on without converging)"
            );
        }
    }

    fn dispose(&self) {
        if self.disposed.replace(true) {
            return;
        }

        tracing::debug!(
            target: trace_targets::EFFECT,
            event = "dispose_effect",
            node_id = self.id.0,
            "disposed reactive effect"
        );
        if self.reactor.diagnostics_enabled() {
            self.reactor
                .emit_diagnostic(DiagnosticEvent::EffectDisposed {
                    reactor: self.reactor.diagnostic_id(),
                    effect: self.id,
                });
        }
        self.reactor.record_flush(|stats| {
            stats.effects_disposed = stats.effects_disposed.saturating_add(1);
        });

        // Release the pending-run latch here rather than leaving it to the queued run. That run
        // holds only a `Weak`, so if this effect is dropped before the run is reached — an
        // unowned handle dropped before the first flush is the common case — the upgrade fails
        // and nothing ever decrements the reactor's queued-effect gauge. Idempotent: a later
        // `run_scheduled_inner` unlatch becomes a no-op.
        self.unlatch_scheduled();

        // Unhook from the graph even if owner teardown unwinds. `disposed` is already set, so
        // there is no second attempt, and an effect left registered keeps its node metadata and
        // every edge it recorded for the reactor's lifetime — which is exactly the retention the
        // 0.3 gauges are supposed to make visible, manufactured by adaptite itself.
        struct UnhookOnUnwind<'a> {
            reactor: &'a Reactor,
            id: NodeId,
        }

        impl Drop for UnhookOnUnwind<'_> {
            fn drop(&mut self) {
                self.reactor.unregister_observer(self.id);
                self.reactor.dispose(self.id);
            }
        }

        let _unhook = UnhookOnUnwind {
            reactor: &self.reactor,
            id: self.id,
        };
        self.owner.dispose();
    }
}

impl OwnedDisposable for EffectInner {
    fn dispose_owned(&self) {
        self.dispose();
    }

    /// An effect disposed through its own handle is still held by its owner until the owner says
    /// otherwise, and `dispose` does not drop `effect` — the closure and everything it captured
    /// live as long as this `EffectInner` does. Reporting disposal lets a long-lived owner
    /// release it. See `OwnerFrame::release_disposed_children`.
    fn is_disposed_owned(&self) -> bool {
        self.disposed.get()
    }
}

impl ObserverHook for EffectInner {
    fn mark(&self, mark: Mark, cause: Option<InvalidationCause>) {
        if let Some(cause) = cause
            && let Some(effect_origin) = self.reactor.node_origin(self.id)
        {
            self.reactor
                .emit_diagnostic(DiagnosticEvent::EffectInvalidated {
                    reactor: self.reactor.diagnostic_id(),
                    effect: self.id,
                    effect_origin,
                    cause,
                    level: mark.into(),
                });
        }
        let target = State::from(mark);
        if self.state.get() < target {
            self.state.set(target);
        }
        self.schedule();
    }

    fn state(&self) -> State {
        self.state.get()
    }
}

impl Drop for EffectInner {
    fn drop(&mut self) {
        self.dispose();
    }
}

#[cfg(test)]
mod tests {
    /// `EffectRun::run` opens its own flush when it runs outside one, and `begin_flush` calls
    /// consumer code (the `FlushStarted` subscriber) as its last act. The guard that closes the
    /// flush used to be constructed *after* that call returned, so a panic escaping the
    /// subscriber stranded `flush_depth` at 1 for the rest of the process — freezing
    /// `drain_epoch` and eventually making the divergence guard accuse an innocent effect. This
    /// is the custom-scheduler twin of the `external_flush` case, and it survived the fix to
    /// that one because the two live in different files.
    #[test]
    fn a_panicking_flush_started_subscriber_does_not_strand_a_scheduled_run() {
        let reactor = Reactor::new();
        let pending: std::rc::Rc<std::cell::RefCell<Vec<crate::EffectRun>>> =
            std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let signal = reactor.signal(0_u32);
        let handle = reactor.effect_with(
            {
                let pending = std::rc::Rc::clone(&pending);
                move |run: crate::EffectRun| pending.borrow_mut().push(run)
            },
            {
                let signal = signal.clone();
                move || {
                    let _ = signal.get();
                }
            },
        );
        // Execute the creation run so the effect actually records its dependency on `signal`.
        for run in pending.borrow_mut().drain(..).collect::<Vec<_>>() {
            run.run();
        }

        // Queue the run BEFORE subscribing, so the only `FlushStarted` the subscriber ever sees
        // is the one `EffectRun::run` opens for itself.
        signal.set(1);
        reactor.flush_now();
        let queued: Vec<_> = pending.borrow_mut().drain(..).collect();
        assert_eq!(queued.len(), 1, "the scheduler received the ready run");

        let boom = std::cell::Cell::new(true);
        let sub = reactor.subscribe_diagnostics(move |event| {
            if matches!(event, crate::DiagnosticEvent::FlushStarted { .. }) && boom.replace(false) {
                panic!("subscriber panicked");
            }
        });
        for run in queued {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run.run()));
        }
        drop(sub);
        assert!(
            !reactor.in_flush(),
            "flush_depth was stranded at 1 by the panic"
        );
        let _ = handle;
    }

    use std::cell::{Cell as Counter, RefCell};
    use std::rc::Rc;

    use runite::{queue_macrotask, run, spawn, yield_now};

    use crate::{Reactor, signal_in};

    use super::{EffectHandle, EffectRun};

    /// A consumer-defined lane: effects queue here and run only when the consumer drains.
    #[derive(Clone, Default)]
    struct Lane(Rc<RefCell<Vec<EffectRun>>>);

    impl Lane {
        fn scheduler(&self) -> impl Fn(EffectRun) + 'static {
            let queue = Rc::clone(&self.0);
            move |ready| queue.borrow_mut().push(ready)
        }

        fn len(&self) -> usize {
            self.0.borrow().len()
        }

        fn drain(&self, reactor: &Reactor) {
            reactor.external_flush(|| {
                // Take the queue rather than iterating it: a run may re-schedule its own effect,
                // which pushes onto the same queue while we hold it.
                let ready = core::mem::take(&mut *self.0.borrow_mut());
                for run in ready {
                    run.run();
                }
            });
        }
    }

    #[test]
    fn effect_identity_is_the_same_from_the_handle_the_lane_and_the_diagnostics() {
        use crate::DiagnosticEvent;

        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let lane = Lane::default();
        let value = signal_in(&reactor, 1);
        let effect = reactor.effect_with(lane.scheduler(), {
            let value = value.clone();
            move || {
                let _ = value.get();
            }
        });

        // The point of the addition: the id is known before anything has run, so a consumer can
        // key a retained structure by effect at creation rather than at first run.
        let id = effect.id();
        assert_eq!(effect.reactor_id(), reactor.id());

        let queued = lane.0.borrow().first().and_then(|run| run.id());
        assert_eq!(queued, Some(id), "the lane sees the same node");

        lane.drain(&reactor);
        assert_eq!(effect.id(), id, "running does not change identity");

        value.set(2);
        lane.drain(&reactor);
        effect.dispose();
        assert_eq!(
            effect.id(),
            id,
            "a disposed effect keeps its id; ids are never reused"
        );

        // Every effect-shaped event in the stream is attributable to the handle without any
        // private access, which is the whole point of exposing the id there.
        let effect_events = events
            .borrow()
            .iter()
            .filter_map(|event| match event {
                DiagnosticEvent::EffectScheduled { effect, .. }
                | DiagnosticEvent::EffectRunStarted { effect, .. }
                | DiagnosticEvent::EffectRunFinished { effect, .. }
                | DiagnosticEvent::EffectDisposed { effect, .. } => Some(*effect),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(!effect_events.is_empty());
        assert!(effect_events.iter().all(|reported| *reported == id));
    }

    #[test]
    fn a_custom_scheduler_owns_when_the_effect_runs() {
        let reactor = Reactor::new();
        let lane = Lane::default();
        let value = signal_in(&reactor, 1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let effect = reactor.effect_with(lane.scheduler(), {
            let value = value.clone();
            let seen = Rc::clone(&seen);
            move || seen.borrow_mut().push(value.get())
        });

        // Even the initial run belongs to the lane, and the reactor's own flush never claims it.
        assert_eq!(lane.len(), 1);
        reactor.flush_now();
        assert!(seen.borrow().is_empty(), "the reactor lane must not run it");

        lane.drain(&reactor);
        assert_eq!(*seen.borrow(), [1]);

        value.set(2);
        assert_eq!(lane.len(), 1, "an invalidation re-enters the scheduler");
        assert_eq!(*seen.borrow(), [1], "and still does not run inline");

        lane.drain(&reactor);
        assert_eq!(*seen.borrow(), [1, 2]);

        effect.dispose();
    }

    #[test]
    fn scheduled_runs_coalesce_between_drains() {
        let reactor = Reactor::new();
        let lane = Lane::default();
        let value = signal_in(&reactor, 0);
        let runs = Rc::new(Counter::new(0));

        let effect = reactor.effect_with(lane.scheduler(), {
            let value = value.clone();
            let runs = Rc::clone(&runs);
            move || {
                value.get();
                runs.set(runs.get() + 1);
            }
        });
        lane.drain(&reactor);
        assert_eq!(runs.get(), 1);

        for next in 1..=5 {
            value.set(next);
        }
        assert_eq!(
            lane.len(),
            1,
            "repeat invalidations coalesce into the pending run"
        );

        lane.drain(&reactor);
        assert_eq!(runs.get(), 2, "the effect observes only the final value");

        effect.dispose();
    }

    #[test]
    fn separate_lanes_run_in_the_order_the_consumer_drains_them() {
        // Phases are the consumer's to define and order; adaptite imposes none.
        let reactor = Reactor::new();
        let (state, render) = (Lane::default(), Lane::default());
        let value = signal_in(&reactor, 1);
        let order = Rc::new(RefCell::new(Vec::new()));

        let write_dom = reactor.effect_with(render.scheduler(), {
            let value = value.clone();
            let order = Rc::clone(&order);
            move || {
                value.get();
                order.borrow_mut().push("render");
            }
        });
        let propagate = reactor.effect_with(state.scheduler(), {
            let value = value.clone();
            let order = Rc::clone(&order);
            move || {
                value.get();
                order.borrow_mut().push("state");
            }
        });

        // Drained state-first, though the render effect was created first and marked first.
        state.drain(&reactor);
        render.drain(&reactor);
        assert_eq!(*order.borrow(), ["state", "render"]);

        write_dom.dispose();
        propagate.dispose();
    }

    #[test]
    fn discarding_a_run_does_not_starve_the_effect_forever() {
        // A render lane that drops queued work — for a pane that got hidden, say — must not
        // permanently latch the effect as "already scheduled".
        let reactor = Reactor::new();
        let lane = Lane::default();
        let value = signal_in(&reactor, 1);
        let seen = Rc::new(RefCell::new(Vec::new()));

        let effect = reactor.effect_with(lane.scheduler(), {
            let value = value.clone();
            let seen = Rc::clone(&seen);
            move || seen.borrow_mut().push(value.get())
        });
        lane.drain(&reactor);
        assert_eq!(*seen.borrow(), [1], "the dependency edge is now recorded");

        // Invalidate, then discard the queued run instead of running it.
        value.set(2);
        assert_eq!(lane.len(), 1);
        lane.0.borrow_mut().clear();
        assert_eq!(*seen.borrow(), [1], "the discarded run never executed");

        // The next invalidation must reach the scheduler again.
        value.set(3);
        assert_eq!(
            lane.len(),
            1,
            "a discarded run must leave the effect schedulable"
        );

        lane.drain(&reactor);
        assert_eq!(
            *seen.borrow(),
            [1, 3],
            "the effect resumes at the current value; the skipped one is not replayed"
        );

        effect.dispose();
    }

    #[test]
    fn running_a_disposed_effect_is_a_no_op() {
        let reactor = Reactor::new();
        let lane = Lane::default();
        let runs = Rc::new(Counter::new(0));

        let effect = reactor.effect_with(lane.scheduler(), {
            let runs = Rc::clone(&runs);
            move || runs.set(runs.get() + 1)
        });

        assert_eq!(lane.len(), 1);
        effect.dispose();

        let ready = lane.0.borrow_mut().pop().expect("a run was queued");
        assert!(ready.is_stale(), "a disposed effect reports its run stale");
        ready.run();
        assert_eq!(runs.get(), 0);
    }

    #[test]
    fn external_flush_gives_a_whole_drain_one_epoch() {
        let reactor = Reactor::new();
        let lane = Lane::default();
        let epochs = Rc::new(RefCell::new(Vec::new()));

        // Two independent effects in one lane must share the drain's epoch, so the divergence
        // guard counts their runs against the same flush rather than against one flush each.
        for _ in 0..2 {
            reactor
                .effect_with(lane.scheduler(), {
                    let reactor = reactor.clone();
                    let epochs = Rc::clone(&epochs);
                    move || epochs.borrow_mut().push(reactor.flush_epoch())
                })
                .leak();
        }

        lane.drain(&reactor);
        let observed = epochs.borrow().clone();
        assert_eq!(observed.len(), 2);
        assert_eq!(
            observed[0], observed[1],
            "one drain is one flush: {observed:?}"
        );

        // A second drain is a second flush.
        epochs.borrow_mut().clear();
        lane.drain(&reactor);
        assert!(epochs.borrow().is_empty(), "nothing was invalidated");
    }

    #[test]
    fn a_run_outside_any_flush_opens_its_own() {
        let reactor = Reactor::new();
        let lane = Lane::default();
        let epochs = Rc::new(RefCell::new(Vec::new()));
        let value = signal_in(&reactor, 0);

        let effect = reactor.effect_with(lane.scheduler(), {
            let reactor = reactor.clone();
            let value = value.clone();
            let epochs = Rc::clone(&epochs);
            move || {
                value.get();
                epochs.borrow_mut().push(reactor.flush_epoch());
            }
        });

        // Run bare, with no enclosing external_flush.
        lane.0.borrow_mut().pop().expect("initial run queued").run();
        value.set(1);
        lane.0.borrow_mut().pop().expect("rerun queued").run();

        let observed = epochs.borrow().clone();
        assert_eq!(observed.len(), 2);
        assert_ne!(
            observed[0], observed[1],
            "each bare run is its own flush: {observed:?}"
        );

        effect.dispose();
    }

    #[test]
    fn effects_flush_through_microtasks_and_coalesce() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let handle_slot = Rc::new(RefCell::new(None::<EffectHandle>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let handle_slot = Rc::clone(&handle_slot);
            move || {
                let reactor = Reactor::new();
                let source = signal_in(&reactor, 0usize);
                let effect = reactor.effect({
                    let seen = Rc::clone(&seen);
                    let source = source.clone();
                    move || seen.borrow_mut().push(source.get())
                });

                source.set(1);
                source.set(2);
                assert!(seen.borrow().is_empty(), "effect should not run inline");

                *handle_slot.borrow_mut() = Some(effect);
            }
        });

        run();
        assert_eq!(&*seen.borrow(), &[2]);

        let reruns = Rc::new(Counter::new(0usize));
        queue_macrotask({
            let reruns = Rc::clone(&reruns);
            let seen = Rc::clone(&seen);
            let handle_slot = Rc::clone(&handle_slot);
            move || {
                let reactor = Reactor::new();
                let source = signal_in(&reactor, 2usize);
                let effect = reactor.effect({
                    let reruns = Rc::clone(&reruns);
                    let seen = Rc::clone(&seen);
                    let source = source.clone();
                    move || {
                        reruns.set(reruns.get() + 1);
                        seen.borrow_mut().push(source.get());
                    }
                });
                source.set(3);
                source.set(4);
                *handle_slot.borrow_mut() = Some(effect);
            }
        });
        run();
        assert_eq!(reruns.get(), 1);
    }

    #[test]
    fn effects_rerun_after_async_future_updates_a_dependency() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let handle_slot = Rc::new(RefCell::new(None::<EffectHandle>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let handle_slot = Rc::clone(&handle_slot);
            move || {
                let reactor = Reactor::new();
                let source = signal_in(&reactor, 0usize);
                let effect = reactor.effect({
                    let seen = Rc::clone(&seen);
                    let source = source.clone();
                    move || seen.borrow_mut().push(source.get())
                });
                *handle_slot.borrow_mut() = Some(effect);

                std::mem::drop(spawn({
                    let source = source.clone();
                    async move {
                        yield_now().await;
                        let _ = source.set(1);
                    }
                }));
            }
        });

        run();
        assert_eq!(&*seen.borrow(), &[0, 1]);
    }

    #[test]
    fn effect_recovers_after_a_panicking_dependency_verification() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        use crate::memo_in;

        let seen = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            move || {
                let reactor = Reactor::new();
                let source = signal_in(&reactor, 1i32);
                let doubled = memo_in(&reactor, {
                    let source = source.clone();
                    move || {
                        let value = source.get();
                        assert!(value != 13, "unlucky number");
                        value * 2
                    }
                });
                let effect = reactor.effect({
                    let doubled = doubled.clone();
                    let seen = Rc::clone(&seen);
                    move || seen.borrow_mut().push(doubled.get())
                });

                reactor.flush_now();
                assert_eq!(&*seen.borrow(), &[2]);

                // The memo panics while the effect *verifies* its dependencies. Without
                // recovery, the effect would be left clean and never re-scheduled, because the
                // still-dirty memo no longer propagates marks.
                source.set(13);
                let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
                assert!(result.is_err(), "verification should propagate the panic");

                source.set(7);
                reactor.flush_now();
                assert_eq!(&*seen.borrow(), &[2, 14], "the effect must recover");

                effect.leak();
            }
        });

        run();
    }

    #[test]
    fn comparator_reads_do_not_become_effect_dependencies() {
        use crate::memo_by_in;

        let runs = Rc::new(Counter::new(0usize));

        queue_macrotask({
            let runs = Rc::clone(&runs);
            move || {
                let reactor = Reactor::new();
                let tuning = signal_in(&reactor, 0i32);
                let source = signal_in(&reactor, 1i32);

                // A comparator that (questionably) reads reactive state. When the memo
                // refreshes inside the effect's run, those reads must not become the effect's
                // dependencies.
                let value = memo_by_in(
                    &reactor,
                    {
                        let tuning = tuning.clone();
                        move |old: &i32, new: &i32| {
                            let _ = tuning.get();
                            old == new
                        }
                    },
                    {
                        let source = source.clone();
                        move || source.get()
                    },
                );

                let effect = reactor.effect({
                    let value = value.clone();
                    let source = source.clone();
                    let runs = Rc::clone(&runs);
                    move || {
                        runs.set(runs.get() + 1);
                        let _ = source.get();
                        let _ = value.get();
                    }
                });

                reactor.flush_now();
                assert_eq!(runs.get(), 1);

                // Forces the memo to refresh (running the comparator) inside the effect body.
                source.set(2);
                reactor.flush_now();
                assert_eq!(runs.get(), 2);

                // If the comparator's read had been tracked, this write would rerun the effect.
                tuning.set(99);
                reactor.flush_now();
                assert_eq!(runs.get(), 2, "comparator reads must not be tracked");

                effect.leak();
            }
        });

        run();
    }

    #[test]
    fn disposing_a_queued_effect_prevents_its_run() {
        let b_runs = Rc::new(Counter::new(0usize));
        let keep_alive = Rc::new(RefCell::new(None::<EffectHandle>));

        queue_macrotask({
            let b_runs = Rc::clone(&b_runs);
            let keep_alive = Rc::clone(&keep_alive);
            move || {
                let reactor = Reactor::new();
                let trigger_a = signal_in(&reactor, 0usize);
                let trigger_b = signal_in(&reactor, 0usize);
                let b_slot = Rc::new(RefCell::new(None::<EffectHandle>));

                let b = reactor.effect({
                    let b_runs = Rc::clone(&b_runs);
                    let trigger_b = trigger_b.clone();
                    move || {
                        let _ = trigger_b.get();
                        b_runs.set(b_runs.get() + 1);
                    }
                });
                *b_slot.borrow_mut() = Some(b);

                let a = reactor.effect({
                    let trigger_a = trigger_a.clone();
                    let b_slot = Rc::clone(&b_slot);
                    move || {
                        if trigger_a.get() == 1
                            && let Some(b) = b_slot.borrow().as_ref()
                        {
                            b.dispose();
                        }
                    }
                });
                *keep_alive.borrow_mut() = Some(a);

                runite::queue_macrotask(move || {
                    // Queue A's rerun (which disposes B) ahead of B's rerun: B's queued job
                    // must observe the disposal and skip.
                    trigger_a.set(1);
                    trigger_b.set(1);
                });
            }
        });

        run();
        assert_eq!(
            b_runs.get(),
            1,
            "an effect disposed while queued must not run"
        );
    }

    #[test]
    fn effect_writes_propagate_to_other_effects_in_the_same_flush() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let keep_alive = Rc::new(RefCell::new(Vec::<EffectHandle>::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let keep_alive = Rc::clone(&keep_alive);
            move || {
                let reactor = Reactor::new();
                let input = signal_in(&reactor, 0usize);
                let mirrored = signal_in(&reactor, 0usize);

                // One effect mirrors `input` into `mirrored`; another observes `mirrored`.
                let mirror = reactor.effect({
                    let input = input.clone();
                    let mirrored = mirrored.clone();
                    move || {
                        let _ = mirrored.set(input.get());
                    }
                });
                let observe = reactor.effect({
                    let mirrored = mirrored.clone();
                    let seen = Rc::clone(&seen);
                    move || seen.borrow_mut().push(mirrored.get())
                });
                keep_alive.borrow_mut().extend([mirror, observe]);

                runite::queue_macrotask(move || {
                    input.set(5);
                });
            }
        });

        run();
        assert_eq!(
            &*seen.borrow(),
            &[0, 5],
            "the observer must settle on the mirrored value within the flush"
        );
    }

    #[test]
    fn untracked_and_peeked_reads_do_not_create_dependencies() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let handle_slot = Rc::new(RefCell::new(None::<EffectHandle>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let handle_slot = Rc::clone(&handle_slot);
            move || {
                let reactor = Reactor::new();
                let tracked = signal_in(&reactor, 1usize);
                let untracked_via_fn = signal_in(&reactor, 10usize);
                let untracked_via_peek = signal_in(&reactor, 100usize);

                let effect = reactor.effect({
                    let seen = Rc::clone(&seen);
                    let tracked = tracked.clone();
                    let untracked_via_fn = untracked_via_fn.clone();
                    let untracked_via_peek = untracked_via_peek.clone();
                    move || {
                        let total = tracked.get()
                            + crate::untrack(|| untracked_via_fn.get())
                            + untracked_via_peek.peek();
                        seen.borrow_mut().push(total);
                    }
                });
                *handle_slot.borrow_mut() = Some(effect);

                runite::queue_macrotask({
                    let untracked_via_fn = untracked_via_fn.clone();
                    let untracked_via_peek = untracked_via_peek.clone();
                    let tracked = tracked.clone();
                    move || {
                        // Neither untracked write may rerun the effect...
                        untracked_via_fn.set(20);
                        untracked_via_peek.set(200);

                        runite::queue_macrotask(move || {
                            // ...but a tracked write reruns it, observing the untracked values.
                            tracked.set(2);
                        });
                    }
                });
            }
        });

        run();
        assert_eq!(&*seen.borrow(), &[111, 222]);
    }

    #[test]
    fn convergent_feedback_loops_settle() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let handle_slot = Rc::new(RefCell::new(None::<EffectHandle>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let handle_slot = Rc::clone(&handle_slot);
            move || {
                let reactor = Reactor::new();
                let value = signal_in(&reactor, 5i64);

                // A clamp: the effect writes the signal it reads. The rewrite converges because
                // the second run writes an equal value, which the signal suppresses.
                let effect = reactor.effect({
                    let value = value.clone();
                    let seen = Rc::clone(&seen);
                    move || {
                        let current = value.get();
                        seen.borrow_mut().push(current);
                        if current > 10 {
                            value.set(10);
                        }
                    }
                });

                value.set(25);
                *handle_slot.borrow_mut() = Some(effect);
            }
        });

        run();
        assert_eq!(&*seen.borrow(), &[25, 10]);
    }

    /// Not `cfg(debug_assertions)`: the guard is enforced in every build, and a test gated to
    /// debug is exactly how the release hang survived unnoticed in the first place.
    #[test]
    fn divergent_feedback_loops_panic_instead_of_hanging() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let handle_slot = Rc::new(RefCell::new(None::<EffectHandle>));
        let panic_message = Rc::new(RefCell::new(None::<String>));
        // The creation site is in this file, so `contains("effect.rs")` is satisfied by any
        // adaptite-internal location too — including the one the message degrades to when
        // `#[track_caller]` is missing. Pin the line as well as the file.
        let creation_line = Rc::new(Counter::new(0u32));

        queue_macrotask({
            let handle_slot = Rc::clone(&handle_slot);
            let panic_message = Rc::clone(&panic_message);
            let creation_line = Rc::clone(&creation_line);
            move || {
                let reactor = Reactor::new();
                let counter = signal_in(&reactor, 0u64);

                // A counter increment: every run changes the value, so the loop never converges.
                creation_line.set(line!() + 1);
                let effect = reactor.effect({
                    let counter = counter.clone();
                    move || {
                        let next = counter.get() + 1;
                        counter.set(next);
                    }
                });
                *handle_slot.borrow_mut() = Some(effect);

                let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
                let panic = result.expect_err("divergent loop should panic");
                *panic_message.borrow_mut() = panic.downcast_ref::<String>().cloned();
            }
        });

        run();

        let message = panic_message.borrow();
        let message = message
            .as_ref()
            .expect("panic payload should be a formatted string");
        assert!(
            message.contains("divergent reactive feedback loop"),
            "panic should describe the divergence, got: {message}"
        );
        let site = format!("effect.rs:{}:", creation_line.get());
        assert!(
            message.contains(&site),
            "panic should name the effect's creation site ({site}), got: {message}"
        );
    }
}
