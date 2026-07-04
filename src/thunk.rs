use alloc::boxed::Box;
use alloc::rc::Rc;
use core::cell::{Cell, RefCell};

use crate::reactor::ObserverHook;
use crate::{NodeId, Reactor, current, trace_targets};

type ComputeFn<T> = dyn Fn() -> T + 'static;
type EqualsFn<T> = dyn Fn(&T, &T) -> bool + 'static;

/// Creates a [`Thunk`] in the current thread's default reactor.
pub fn thunk<T: 'static>(compute: impl Fn() -> T + 'static) -> Thunk<T> {
    current().thunk(compute)
}

/// Creates a [`Thunk`] associated with `reactor`.
pub fn thunk_in<T: 'static>(reactor: &Reactor, compute: impl Fn() -> T + 'static) -> Thunk<T> {
    reactor.thunk(compute)
}

/// Creates an equality-aware memo in the current thread's default reactor.
pub fn memo<T: PartialEq + 'static>(compute: impl Fn() -> T + 'static) -> Memo<T> {
    current().memo(compute)
}

/// Creates an equality-aware memo associated with `reactor`.
pub fn memo_in<T: PartialEq + 'static>(
    reactor: &Reactor,
    compute: impl Fn() -> T + 'static,
) -> Memo<T> {
    reactor.memo(compute)
}

/// Creates a comparator-aware memo in the current thread's default reactor.
pub fn memo_by<T: 'static>(
    equals: impl Fn(&T, &T) -> bool + 'static,
    compute: impl Fn() -> T + 'static,
) -> Memo<T> {
    current().memo_by(equals, compute)
}

/// Creates a comparator-aware memo associated with `reactor`.
pub fn memo_by_in<T: 'static>(
    reactor: &Reactor,
    equals: impl Fn(&T, &T) -> bool + 'static,
    compute: impl Fn() -> T + 'static,
) -> Memo<T> {
    reactor.memo_by(equals, compute)
}

/// Lazy computed node in the reactive graph.
#[derive(Clone)]
pub struct Thunk<T> {
    inner: Rc<ThunkInner<T>>,
}

/// Equality/comparator-aware computed node.
#[derive(Clone)]
pub struct Memo<T> {
    inner: Rc<MemoInner<T>>,
}

impl Reactor {
    /// Creates a lazy computed thunk associated with this reactor.
    pub fn thunk<T: 'static>(&self, compute: impl Fn() -> T + 'static) -> Thunk<T> {
        Thunk::new(self.clone(), compute)
    }

    /// Creates an equality-aware memo associated with this reactor.
    pub fn memo<T: PartialEq + 'static>(&self, compute: impl Fn() -> T + 'static) -> Memo<T> {
        self.memo_by(|left, right| left == right, compute)
    }

    /// Creates a comparator-aware memo associated with this reactor.
    pub fn memo_by<T: 'static>(
        &self,
        equals: impl Fn(&T, &T) -> bool + 'static,
        compute: impl Fn() -> T + 'static,
    ) -> Memo<T> {
        Memo::new(self.clone(), equals, compute)
    }
}

impl<T: 'static> Thunk<T> {
    fn new(reactor: Reactor, compute: impl Fn() -> T + 'static) -> Self {
        let id = reactor.allocate_node();
        let inner = Rc::new(ThunkInner {
            reactor: reactor.clone(),
            id,
            compute: Box::new(compute),
            value: RefCell::new(None),
            dirty: Cell::new(true),
        });

        let observer: Rc<dyn ObserverHook> = inner.clone();
        reactor.register_observer(id, observer);
        tracing::debug!(
            target: trace_targets::THUNK,
            event = "create_thunk",
            node_id = id.0,
            "created reactive thunk"
        );
        Self { inner }
    }

    /// Runs `f` with a shared reference to the current computed value.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::THUNK,
            event = "read_thunk",
            node_id = self.inner.id.0,
            "reading thunk value"
        );
        self.inner.reactor.observe(self.inner.id);
        self.inner.ensure_value();
        let value = self.inner.value.borrow();
        f(value
            .as_ref()
            .expect("thunk should have a cached value after recomputing"))
    }
}

impl<T: Clone + 'static> Thunk<T> {
    /// Clones and returns the current computed value.
    pub fn get(&self) -> T {
        self.with(T::clone)
    }
}

impl<T: 'static> Memo<T> {
    fn new(
        reactor: Reactor,
        equals: impl Fn(&T, &T) -> bool + 'static,
        compute: impl Fn() -> T + 'static,
    ) -> Self {
        let id = reactor.allocate_node();
        let inner = Rc::new(MemoInner {
            reactor: reactor.clone(),
            id,
            compute: Box::new(compute),
            equals: Box::new(equals),
            value: RefCell::new(None),
            dirty: Cell::new(true),
        });

        let observer: Rc<dyn ObserverHook> = inner.clone();
        reactor.register_observer(id, observer);
        tracing::debug!(
            target: trace_targets::MEMO,
            event = "create_memo",
            node_id = id.0,
            "created reactive memo"
        );
        Self { inner }
    }

    /// Runs `f` with a shared reference to the current computed value.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::MEMO,
            event = "read_memo",
            node_id = self.inner.id.0,
            "reading memo value"
        );
        self.inner.reactor.observe(self.inner.id);
        self.inner.ensure_value();
        let value = self.inner.value.borrow();
        f(value
            .as_ref()
            .expect("memo should have a cached value after recomputing"))
    }
}

impl<T: Clone + 'static> Memo<T> {
    /// Clones and returns the current computed value.
    pub fn get(&self) -> T {
        self.with(T::clone)
    }
}

struct ThunkInner<T> {
    reactor: Reactor,
    id: NodeId,
    compute: Box<ComputeFn<T>>,
    value: RefCell<Option<T>>,
    dirty: Cell<bool>,
}

impl<T> ThunkInner<T> {
    fn ensure_value(&self) {
        if !self.dirty.get() {
            return;
        }

        let _span = tracing::debug_span!(
            target: trace_targets::THUNK,
            "thunk.recompute",
            node_id = self.id.0
        )
        .entered();
        let next = self.reactor.run_in_context(self.id, || (self.compute)());
        *self.value.borrow_mut() = Some(next);
        self.dirty.set(false);
    }
}

struct MemoInner<T> {
    reactor: Reactor,
    id: NodeId,
    compute: Box<ComputeFn<T>>,
    equals: Box<EqualsFn<T>>,
    value: RefCell<Option<T>>,
    dirty: Cell<bool>,
}

impl<T> MemoInner<T> {
    fn ensure_value(&self) {
        if !self.dirty.get() {
            return;
        }
        let _ = self.recompute();
    }

    fn recompute(&self) -> bool {
        let _span = tracing::debug_span!(
            target: trace_targets::MEMO,
            "memo.recompute",
            node_id = self.id.0
        )
        .entered();
        let next = self.reactor.run_in_context(self.id, || (self.compute)());
        let mut value = self.value.borrow_mut();
        let changed = match value.as_ref() {
            Some(current) => !(self.equals)(current, &next),
            None => true,
        };
        *value = Some(next);
        self.dirty.set(false);
        tracing::debug!(
            target: trace_targets::MEMO,
            event = "memo_recompute",
            node_id = self.id.0,
            changed,
            "recomputed memo"
        );
        changed
    }
}

impl<T: 'static> ObserverHook for ThunkInner<T> {
    fn notify(&self) {
        let already_dirty = self.dirty.replace(true);
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::THUNK,
            event = "invalidate_thunk",
            node_id = self.id.0,
            already_dirty,
            "invalidating thunk"
        );
        if already_dirty {
            return;
        }

        let _ = self.value.borrow_mut().take();
        self.reactor.trigger(self.id);
    }
}

impl<T> Drop for ThunkInner<T> {
    fn drop(&mut self) {
        self.reactor.unregister_observer(self.id);
        self.reactor.dispose(self.id);
    }
}

impl<T: 'static> ObserverHook for MemoInner<T> {
    fn notify(&self) {
        if self.value.borrow().is_none() {
            self.dirty.set(true);
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::MEMO,
                event = "invalidate_memo",
                node_id = self.id.0,
                eagerly_recomputed = false,
                "marked uninitialized memo dirty"
            );
            return;
        }

        self.dirty.set(true);
        let changed = self.recompute();
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::MEMO,
            event = "invalidate_memo",
            node_id = self.id.0,
            eagerly_recomputed = true,
            changed,
            "invalidated memo"
        );
        if changed {
            self.reactor.trigger(self.id);
        }
    }
}

impl<T> Drop for MemoInner<T> {
    fn drop(&mut self) {
        self.reactor.unregister_observer(self.id);
        self.reactor.dispose(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell as Counter, RefCell};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;

    use runite::{queue_macrotask, run};

    use super::Thunk;
    use crate::{Cell, EffectHandle, Memo, Reactor, cell_in, memo_by_in, memo_in, thunk_in};

    #[test]
    fn thunk_caches_until_invalidated() {
        let reactor = Reactor::new();
        let source = cell_in(&reactor, 2usize);
        let compute_count = Rc::new(Counter::new(0usize));
        let thunk = thunk_in(&reactor, {
            let source = source.clone();
            let compute_count = Rc::clone(&compute_count);
            move || {
                compute_count.set(compute_count.get() + 1);
                source.get() * 2
            }
        });

        assert_eq!(thunk.get(), 4);
        assert_eq!(thunk.get(), 4);
        assert_eq!(compute_count.get(), 1);

        source.set(3);
        assert_eq!(thunk.get(), 6);
        assert_eq!(compute_count.get(), 2);
    }

    #[test]
    fn nested_thunks_recompute_only_affected_layers() {
        let reactor = Reactor::new();
        let base = cell_in(&reactor, 5usize);
        let extra = cell_in(&reactor, 1usize);

        let double_count = Rc::new(Counter::new(0usize));
        let label_count = Rc::new(Counter::new(0usize));

        let doubled = thunk_in(&reactor, {
            let base = base.clone();
            let double_count = Rc::clone(&double_count);
            move || {
                double_count.set(double_count.get() + 1);
                base.get() * 2
            }
        });
        let label = thunk_in(&reactor, {
            let doubled = doubled.clone();
            let extra = extra.clone();
            let label_count = Rc::clone(&label_count);
            move || {
                label_count.set(label_count.get() + 1);
                format!("{} + {}", doubled.get(), extra.get())
            }
        });

        assert_eq!(label.get(), "10 + 1");
        assert_eq!(double_count.get(), 1);
        assert_eq!(label_count.get(), 1);

        extra.set(2);
        assert_eq!(label.get(), "10 + 2");
        assert_eq!(double_count.get(), 1);
        assert_eq!(label_count.get(), 2);

        base.set(6);
        assert_eq!(label.get(), "12 + 2");
        assert_eq!(double_count.get(), 2);
        assert_eq!(label_count.get(), 3);
    }

    #[test]
    fn cycle_detection_reports_two_thunks_reading_each_other() {
        let reactor = Reactor::new();
        let left_slot = Rc::new(RefCell::new(None::<Thunk<i32>>));
        let right_slot = Rc::new(RefCell::new(None::<Thunk<i32>>));

        let left = thunk_in(&reactor, {
            let right_slot = Rc::clone(&right_slot);
            move || {
                right_slot
                    .borrow()
                    .as_ref()
                    .expect("right thunk should exist")
                    .get()
                    + 1
            }
        });
        let right = thunk_in(&reactor, {
            let left_slot = Rc::clone(&left_slot);
            move || {
                left_slot
                    .borrow()
                    .as_ref()
                    .expect("left thunk should exist")
                    .get()
                    + 1
            }
        });

        *left_slot.borrow_mut() = Some(left.clone());
        *right_slot.borrow_mut() = Some(right.clone());

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = left.get();
        }))
        .expect_err("mutually recursive thunks should panic");

        let error = panic
            .downcast_ref::<String>()
            .expect("panic should be a string");

        assert!(
            error.contains("reactive cycle detected"),
            "panic should indicate cycle detected"
        );
    }

    #[test]
    fn memo_suppresses_unchanged_results_and_memo_by_uses_custom_comparator() {
        let reactive_seen = Rc::new(RefCell::new(Vec::new()));
        let effect_slot = Rc::new(RefCell::new(None::<EffectHandle>));
        let source_slot = Rc::new(RefCell::new(None::<Cell<usize>>));
        let parity_slot = Rc::new(RefCell::new(None::<Memo<usize>>));
        let bucket_slot = Rc::new(RefCell::new(None::<Memo<usize>>));

        queue_macrotask({
            let reactive_seen = Rc::clone(&reactive_seen);
            let effect_slot = Rc::clone(&effect_slot);
            let source_slot = Rc::clone(&source_slot);
            let parity_slot = Rc::clone(&parity_slot);
            let bucket_slot = Rc::clone(&bucket_slot);
            move || {
                let reactor = Reactor::new();
                let source = cell_in(&reactor, 1usize);
                let parity = memo_in(&reactor, {
                    let source = source.clone();
                    move || source.get() % 2
                });
                let bucket = memo_by_in(
                    &reactor,
                    |left: &usize, right: &usize| left / 10 == right / 10,
                    {
                        let source = source.clone();
                        move || source.get()
                    },
                );

                let effect = reactor.effect({
                    let reactive_seen = Rc::clone(&reactive_seen);
                    let parity = parity.clone();
                    let bucket = bucket.clone();
                    move || {
                        reactive_seen
                            .borrow_mut()
                            .push((parity.get(), bucket.get()))
                    }
                });

                *source_slot.borrow_mut() = Some(source);
                *parity_slot.borrow_mut() = Some(parity);
                *bucket_slot.borrow_mut() = Some(bucket);
                *effect_slot.borrow_mut() = Some(effect);
            }
        });

        run();
        assert_eq!(&*reactive_seen.borrow(), &[(1, 1)]);

        queue_macrotask({
            let source_slot = Rc::clone(&source_slot);
            move || {
                let source = source_slot
                    .borrow()
                    .as_ref()
                    .expect("source cell should still be alive")
                    .clone();
                source.set(3);
                source.set(9);
                source.set(10);
            }
        });
        run();
        assert_eq!(&*reactive_seen.borrow(), &[(1, 1), (0, 10)]);
    }
}
