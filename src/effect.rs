use alloc::boxed::Box;
use alloc::rc::{Rc, Weak};
use core::cell::{Cell, RefCell};

use crate::reactor::{Mark, ObserverHook, State};
use crate::scope::{OwnedDisposable, OwnerFrame, adopt_into_current, with_owner};
use crate::{NodeId, Reactor, current, trace_targets};

/// Maximum number of times a single effect may run within one job flush before the reactor
/// assumes it is caught in a divergent feedback loop (debug builds only).
#[cfg(debug_assertions)]
const MAX_RUNS_PER_FLUSH: u32 = 100;

/// Creates an effect in the current thread's default reactor.
///
/// The effect is scheduled immediately and then re-scheduled whenever one of its dependencies
/// changes.
#[track_caller]
pub fn effect(f: impl Fn() + 'static) -> EffectHandle {
    current().effect(f)
}

/// Creates an effect associated with `reactor`.
#[track_caller]
pub fn effect_in(reactor: &Reactor, f: impl Fn() + 'static) -> EffectHandle {
    reactor.effect(f)
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
    #[track_caller]
    pub fn effect(&self, f: impl Fn() + 'static) -> EffectHandle {
        EffectHandle::new(self.clone(), f)
    }
}

impl EffectHandle {
    #[track_caller]
    fn new(reactor: Reactor, effect: impl Fn() + 'static) -> Self {
        let id = reactor.allocate_node();
        let inner = Rc::new(EffectInner {
            reactor: reactor.clone(),
            id,
            effect: Box::new(effect),
            state: Cell::new(State::Dirty),
            scheduled: Cell::new(false),
            disposed: Cell::new(false),
            self_ref: RefCell::new(Weak::new()),
            owner: OwnerFrame::new(),
            #[cfg(debug_assertions)]
            last_flush_epoch: Cell::new(u64::MAX),
            #[cfg(debug_assertions)]
            runs_this_flush: Cell::new(0),
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

    /// Consumes the effect and leaks it, allowing it to run for the remainder of the program without automatically
    /// disposing when dropped. Use this for effects that you want to run for the lifetime of the program. You CANNOT
    /// recover an EffectHandle after calling this method, so be sure to call it on an EffectHandle that you don't need
    /// to later dispose of.
    pub fn leak(self) {
        core::mem::forget(self);
    }

    /// Disposes the effect immediately.
    pub fn dispose(&self) {
        self.inner.dispose();
    }

    /// Returns `true` if the effect has been disposed.
    pub fn is_disposed(&self) -> bool {
        self.inner.disposed.get()
    }
}

struct EffectInner {
    reactor: Reactor,
    id: NodeId,
    effect: Box<dyn Fn() + 'static>,
    state: Cell<State>,
    scheduled: Cell<bool>,
    disposed: Cell<bool>,
    self_ref: RefCell<Weak<EffectInner>>,
    /// Ownership frame for cleanups and nested effects created during this effect's runs.
    owner: Rc<OwnerFrame>,
    #[cfg(debug_assertions)]
    last_flush_epoch: Cell<u64>,
    #[cfg(debug_assertions)]
    runs_this_flush: Cell<u32>,
}

impl EffectInner {
    fn schedule(&self) {
        if self.disposed.get() || self.scheduled.replace(true) {
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

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::EFFECT,
            event = "schedule_effect",
            node_id = self.id.0,
            queued = true,
            "queued effect for microtask flush"
        );
        let weak = self.self_ref.borrow().clone();
        let reactor = self.reactor.clone();
        reactor.schedule(move || {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            inner.run_scheduled();
        });
    }

    fn run_scheduled(self: &Rc<Self>) {
        if self.disposed.get() {
            self.scheduled.set(false);
            return;
        }

        self.scheduled.set(false);
        let state = self.state.get();
        self.state.set(State::Clean);

        // A Check mark means only computed dependencies may have changed; verify them so that
        // equality-suppressed memo updates do not rerun the effect.
        let should_run = match state {
            State::Dirty => true,
            State::Check => self.reactor.dependencies_changed(self.id),
            State::Clean => false,
        };
        if !should_run {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::EFFECT,
                event = "skip_effect",
                node_id = self.id.0,
                "skipping effect run; no dependency actually changed"
            );
            return;
        }

        #[cfg(debug_assertions)]
        self.check_divergence();

        let _span = tracing::debug_span!(
            target: trace_targets::EFFECT,
            "effect.run",
            node_id = self.id.0
        )
        .entered();
        // Run cleanups from the previous run and dispose nested effects it created, then run
        // with this effect as the innermost owner so new cleanups and children register here.
        self.owner.reset();
        with_owner(&self.owner, || {
            self.reactor.run_in_context(self.id, || (self.effect)())
        });
    }

    /// Panics when this effect keeps re-running within a single flush, which indicates a
    /// divergent feedback loop: the effect writes state it (transitively) depends on with a
    /// value that never converges.
    ///
    /// Convergent feedback (for example clamping, where the rewritten value is suppressed by the
    /// signal's equality check on the next round) is legal and settles well below this limit.
    #[cfg(debug_assertions)]
    fn check_divergence(&self) {
        let epoch = self.reactor.flush_epoch();
        if self.last_flush_epoch.get() != epoch {
            self.last_flush_epoch.set(epoch);
            self.runs_this_flush.set(1);
            return;
        }

        let runs = self.runs_this_flush.get().saturating_add(1);
        self.runs_this_flush.set(runs);
        if runs > MAX_RUNS_PER_FLUSH {
            let origin = self
                .reactor
                .origin(self.id)
                .map(|location| location.to_string())
                .unwrap_or_else(|| "<unknown>".into());
            panic!(
                "adaptite: effect created at {origin} ran more than {MAX_RUNS_PER_FLUSH} times \
                 in a single flush; this suggests a divergent reactive feedback loop (the effect \
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
        self.owner.dispose();
        self.reactor.unregister_observer(self.id);
        self.reactor.dispose(self.id);
    }
}

impl OwnedDisposable for EffectInner {
    fn dispose_owned(&self) {
        self.dispose();
    }
}

impl ObserverHook for EffectInner {
    fn mark(&self, mark: Mark) {
        let target = State::from(mark);
        if self.state.get() < target {
            self.state.set(target);
        }
        self.schedule();
    }
}

impl Drop for EffectInner {
    fn drop(&mut self) {
        self.dispose();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell as Counter, RefCell};
    use std::rc::Rc;

    use runite::{queue_macrotask, run, spawn, yield_now};

    use crate::{Reactor, signal_in};

    use super::EffectHandle;

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

    #[cfg(debug_assertions)]
    #[test]
    fn divergent_feedback_loops_panic_instead_of_hanging() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let handle_slot = Rc::new(RefCell::new(None::<EffectHandle>));
        let panic_message = Rc::new(RefCell::new(None::<String>));

        queue_macrotask({
            let handle_slot = Rc::clone(&handle_slot);
            let panic_message = Rc::clone(&panic_message);
            move || {
                let reactor = Reactor::new();
                let counter = signal_in(&reactor, 0u64);

                // A counter increment: every run changes the value, so the loop never converges.
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
        assert!(
            message.contains("effect.rs"),
            "panic should name the effect's creation site, got: {message}"
        );
    }
}
