use alloc::boxed::Box;
use alloc::rc::{Rc, Weak};
use core::cell::{Cell, RefCell};

use crate::reactor::ObserverHook;
use crate::{NodeId, Reactor, current, trace_targets};

/// Creates an effect in the current thread's default reactor.
///
/// The effect is scheduled immediately and then re-scheduled whenever one of its dependencies
/// changes.
pub fn effect(f: impl Fn() + 'static) -> EffectHandle {
    current().effect(f)
}

/// Creates an effect associated with `reactor`.
pub fn effect_in(reactor: &Reactor, f: impl Fn() + 'static) -> EffectHandle {
    reactor.effect(f)
}

/// Disposable handle for a reactive effect.
#[derive(Clone)]
#[must_use = "effects are automatically disposed when dropped, so you must use the handle or explicitly leak it"]
pub struct EffectHandle {
    inner: Rc<EffectInner>,
}

impl Reactor {
    /// Creates an effect associated with this reactor.
    ///
    /// The effect is scheduled immediately and then re-scheduled whenever one of its dependencies
    /// changes.
    pub fn effect(&self, f: impl Fn() + 'static) -> EffectHandle {
        EffectHandle::new(self.clone(), f)
    }
}

impl EffectHandle {
    fn new(reactor: Reactor, effect: impl Fn() + 'static) -> Self {
        let id = reactor.allocate_node();
        let inner = Rc::new(EffectInner {
            reactor: reactor.clone(),
            id,
            effect: Box::new(effect),
            scheduled: Cell::new(false),
            disposed: Cell::new(false),
            self_ref: RefCell::new(Weak::new()),
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
    scheduled: Cell<bool>,
    disposed: Cell<bool>,
    self_ref: RefCell<Weak<EffectInner>>,
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
            if inner.disposed.get() {
                inner.scheduled.set(false);
                return;
            }

            inner.scheduled.set(false);
            let _span = tracing::debug_span!(
                target: trace_targets::EFFECT,
                "effect.run",
                node_id = inner.id.0
            )
            .entered();
            inner.reactor.run_in_context(inner.id, || (inner.effect)());
        });
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
        self.reactor.unregister_observer(self.id);
        self.reactor.dispose(self.id);
    }
}

impl ObserverHook for EffectInner {
    fn notify(&self) {
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
}
