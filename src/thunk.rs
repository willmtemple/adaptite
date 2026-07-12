use alloc::boxed::Box;
use alloc::rc::Rc;
use core::cell::{Cell, RefCell};

use crate::reactor::{Mark, ObserverHook, State};
use crate::{NodeId, Reactor, current, trace_targets};

type ComputeFn<T> = dyn Fn() -> T + 'static;
type ComputePrevFn<T> = dyn Fn(Option<&T>) -> T + 'static;
type EqualsFn<T> = dyn Fn(&T, &T) -> bool + 'static;

/// Creates a [`Thunk`] in the current thread's default reactor.
///
/// # Examples
///
/// ```rust
/// use adaptite::{signal, thunk};
///
/// let base = signal(2);
/// let doubled = thunk({
///     let base = base.clone();
///     move || base.get() * 2
/// });
///
/// assert_eq!(doubled.get(), 4);
/// base.set(5); // marks `doubled` stale; nothing recomputes until it is read
/// assert_eq!(doubled.get(), 10);
/// ```
#[track_caller]
pub fn thunk<T: 'static>(compute: impl Fn() -> T + 'static) -> Thunk<T> {
    current().thunk(compute)
}

/// Creates a [`Thunk`] associated with `reactor`.
#[track_caller]
pub fn thunk_in<T: 'static>(reactor: &Reactor, compute: impl Fn() -> T + 'static) -> Thunk<T> {
    reactor.thunk(compute)
}

/// Creates an equality-aware memo in the current thread's default reactor.
///
/// A memo recomputes like a [`Thunk`], but when the new value equals the old one (under
/// `PartialEq`), downstream observers are not invalidated.
///
/// # Examples
///
/// ```rust
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// use adaptite::{memo, signal, thunk};
///
/// let n = signal(1i32);
/// let parity = memo({
///     let n = n.clone();
///     move || n.get() % 2
/// });
///
/// let label_computes = Rc::new(Cell::new(0));
/// let label = thunk({
///     let parity = parity.clone();
///     let label_computes = Rc::clone(&label_computes);
///     move || {
///         label_computes.set(label_computes.get() + 1);
///         format!("parity: {}", parity.get())
///     }
/// });
///
/// assert_eq!(label.get(), "parity: 1");
///
/// // 1 -> 3 keeps the parity at 1: the memo recomputes, sees an equal result,
/// // and the downstream thunk's cache stays valid.
/// n.set(3);
/// assert_eq!(label.get(), "parity: 1");
/// assert_eq!(label_computes.get(), 1);
///
/// n.set(4);
/// assert_eq!(label.get(), "parity: 0");
/// assert_eq!(label_computes.get(), 2);
/// ```
#[track_caller]
pub fn memo<T: PartialEq + 'static>(compute: impl Fn() -> T + 'static) -> Memo<T> {
    current().memo(compute)
}

/// Creates an equality-aware memo associated with `reactor`.
#[track_caller]
pub fn memo_in<T: PartialEq + 'static>(
    reactor: &Reactor,
    compute: impl Fn() -> T + 'static,
) -> Memo<T> {
    reactor.memo(compute)
}

/// Creates a comparator-aware memo in the current thread's default reactor.
///
/// Like [`memo`], but "unchanged" is decided by the `equals` closure instead of `PartialEq` —
/// useful for coarser notions of change than value equality.
///
/// # Examples
///
/// ```rust
/// use adaptite::{memo_by, signal};
///
/// let price = signal(104u32);
/// // Downstream observers only care which $10 bucket the price is in.
/// let bucket = memo_by(
///     |old: &u32, new: &u32| old / 10 == new / 10,
///     {
///         let price = price.clone();
///         move || price.get()
///     },
/// );
///
/// assert_eq!(bucket.get(), 104);
/// price.set(109); // same bucket: dependents of `bucket` are not invalidated
/// price.set(112); // new bucket: they are
/// assert_eq!(bucket.get(), 112);
/// ```
#[track_caller]
pub fn memo_by<T: 'static>(
    equals: impl Fn(&T, &T) -> bool + 'static,
    compute: impl Fn() -> T + 'static,
) -> Memo<T> {
    current().memo_by(equals, compute)
}

/// Creates a comparator-aware memo associated with `reactor`.
#[track_caller]
pub fn memo_by_in<T: 'static>(
    reactor: &Reactor,
    equals: impl Fn(&T, &T) -> bool + 'static,
    compute: impl Fn() -> T + 'static,
) -> Memo<T> {
    reactor.memo_by(equals, compute)
}

/// Creates an equality-aware memo whose compute closure receives the memo's previous value, in
/// the current thread's default reactor.
///
/// The previous value is `None` on the first computation. This is the supported way to express
/// reduction-style computations ("fold the new inputs into the last result") without creating a
/// dependency cycle — a memo that read *itself* would panic with a cycle error.
///
/// # Examples
///
/// ```rust
/// use adaptite::{memo_with_prev, signal};
///
/// let sample = signal(5i64);
///
/// // A running maximum over every value the memo observes.
/// let max_seen = memo_with_prev({
///     let sample = sample.clone();
///     move |previous: Option<&i64>| {
///         let current = sample.get();
///         previous.map_or(current, |&best| best.max(current))
///     }
/// });
///
/// assert_eq!(max_seen.get(), 5);
/// sample.set(9);
/// assert_eq!(max_seen.get(), 9);
/// sample.set(3);
/// assert_eq!(max_seen.get(), 9);
/// ```
#[track_caller]
pub fn memo_with_prev<T: PartialEq + 'static>(
    compute: impl Fn(Option<&T>) -> T + 'static,
) -> Memo<T> {
    current().memo_with_prev(compute)
}

/// Creates an equality-aware memo whose compute closure receives the memo's previous value,
/// associated with `reactor`.
#[track_caller]
pub fn memo_with_prev_in<T: PartialEq + 'static>(
    reactor: &Reactor,
    compute: impl Fn(Option<&T>) -> T + 'static,
) -> Memo<T> {
    reactor.memo_with_prev(compute)
}

/// Creates a comparator-aware memo whose compute closure receives the memo's previous value, in
/// the current thread's default reactor.
#[track_caller]
pub fn memo_by_with_prev<T: 'static>(
    equals: impl Fn(&T, &T) -> bool + 'static,
    compute: impl Fn(Option<&T>) -> T + 'static,
) -> Memo<T> {
    current().memo_by_with_prev(equals, compute)
}

/// Creates a comparator-aware memo whose compute closure receives the memo's previous value,
/// associated with `reactor`.
#[track_caller]
pub fn memo_by_with_prev_in<T: 'static>(
    reactor: &Reactor,
    equals: impl Fn(&T, &T) -> bool + 'static,
    compute: impl Fn(Option<&T>) -> T + 'static,
) -> Memo<T> {
    reactor.memo_by_with_prev(equals, compute)
}

/// Lazy computed node in the reactive graph.
///
/// A thunk caches the result of its compute closure and recomputes on read after any of its
/// dependencies change. It is both an observer (its computation records dependencies) and an
/// observable (reading it from another observer records a dependency on it). Every
/// recomputation counts as a change; use [`Memo`] to stop propagation of equal results.
///
/// Clones share the same underlying node.
///
/// # Panics
///
/// Reading a thunk whose computation (transitively) reads itself panics with a
/// [`crate::ReactCycleError`] describing the cycle path.
pub struct Thunk<T> {
    inner: Rc<ThunkInner<T>>,
}

// Manual impl: cloning the handle shares the node and must not require `T: Clone`.
impl<T> Clone for Thunk<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

/// Equality/comparator-aware computed node.
///
/// Like [`Thunk`], but a recomputation that produces an unchanged value (under `PartialEq` for
/// [`memo`], or a custom comparator for [`memo_by`]) does not invalidate downstream observers.
/// Effects depending only on unchanged memos skip their re-runs entirely.
///
/// Clones share the same underlying node.
///
/// # Panics
///
/// Reading a memo whose computation (transitively) reads itself panics with a
/// [`crate::ReactCycleError`]. To fold the memo's own previous value into the next one, use
/// [`memo_with_prev`].
pub struct Memo<T> {
    inner: Rc<MemoInner<T>>,
}

// Manual impl: cloning the handle shares the node and must not require `T: Clone`.
impl<T> Clone for Memo<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl Reactor {
    /// Creates a lazy computed thunk associated with this reactor.
    #[track_caller]
    pub fn thunk<T: 'static>(&self, compute: impl Fn() -> T + 'static) -> Thunk<T> {
        Thunk::new(self.clone(), compute)
    }

    /// Creates an equality-aware memo associated with this reactor.
    #[track_caller]
    pub fn memo<T: PartialEq + 'static>(&self, compute: impl Fn() -> T + 'static) -> Memo<T> {
        self.memo_by(|left, right| left == right, compute)
    }

    /// Creates a comparator-aware memo associated with this reactor.
    #[track_caller]
    pub fn memo_by<T: 'static>(
        &self,
        equals: impl Fn(&T, &T) -> bool + 'static,
        compute: impl Fn() -> T + 'static,
    ) -> Memo<T> {
        Memo::new(self.clone(), equals, move |_| compute())
    }

    /// Creates an equality-aware memo whose compute closure receives the memo's previous value
    /// (`None` on the first computation), associated with this reactor.
    #[track_caller]
    pub fn memo_with_prev<T: PartialEq + 'static>(
        &self,
        compute: impl Fn(Option<&T>) -> T + 'static,
    ) -> Memo<T> {
        self.memo_by_with_prev(|left, right| left == right, compute)
    }

    /// Creates a comparator-aware memo whose compute closure receives the memo's previous value
    /// (`None` on the first computation), associated with this reactor.
    #[track_caller]
    pub fn memo_by_with_prev<T: 'static>(
        &self,
        equals: impl Fn(&T, &T) -> bool + 'static,
        compute: impl Fn(Option<&T>) -> T + 'static,
    ) -> Memo<T> {
        Memo::new(self.clone(), equals, compute)
    }
}

impl<T: 'static> Thunk<T> {
    #[track_caller]
    fn new(reactor: Reactor, compute: impl Fn() -> T + 'static) -> Self {
        let id = reactor.allocate_node();
        let inner = Rc::new(ThunkInner {
            reactor: reactor.clone(),
            id,
            compute: Box::new(compute),
            value: RefCell::new(None),
            state: Cell::new(State::Dirty),
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
    ///
    /// # Panics
    ///
    /// Panics with a [`crate::ReactCycleError`] message when this thunk's computation
    /// (transitively) reads itself. A shared borrow of the cached value is held while `f`
    /// runs, so invalidating this thunk *and reading it again* from inside `f` also panics.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::THUNK,
            event = "read_thunk",
            node_id = self.inner.id.0,
            "reading thunk value"
        );
        self.inner.reactor.assert_no_cycle(self.inner.id);
        self.inner.refresh();
        self.inner.reactor.observe(self.inner.id);
        let value = self.inner.value.borrow();
        f(value
            .as_ref()
            .expect("thunk should have a cached value after recomputing"))
    }
}

impl<T: 'static> Thunk<T> {
    /// Runs `f` with a shared reference to the current computed value without recording a
    /// dependency. The thunk is still brought up to date before `f` runs.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`with`](Thunk::with).
    pub fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.inner.reactor.assert_no_cycle(self.inner.id);
        self.inner.refresh();
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

    /// Clones and returns the current computed value without recording a dependency.
    pub fn peek(&self) -> T {
        self.with_peek(T::clone)
    }
}

impl<T: 'static> Memo<T> {
    #[track_caller]
    fn new(
        reactor: Reactor,
        equals: impl Fn(&T, &T) -> bool + 'static,
        compute: impl Fn(Option<&T>) -> T + 'static,
    ) -> Self {
        let id = reactor.allocate_node();
        let inner = Rc::new(MemoInner {
            reactor: reactor.clone(),
            id,
            compute: Box::new(compute),
            equals: Box::new(equals),
            value: RefCell::new(None),
            state: Cell::new(State::Dirty),
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
    ///
    /// # Panics
    ///
    /// Panics with a [`crate::ReactCycleError`] message when this memo's computation
    /// (transitively) reads itself (use [`memo_with_prev`] for access to the previous value).
    /// A shared borrow of the cached value is held while `f` runs, so invalidating this memo
    /// *and reading it again* from inside `f` also panics.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::MEMO,
            event = "read_memo",
            node_id = self.inner.id.0,
            "reading memo value"
        );
        self.inner.reactor.assert_no_cycle(self.inner.id);
        self.inner.refresh();
        self.inner.reactor.observe(self.inner.id);
        let value = self.inner.value.borrow();
        f(value
            .as_ref()
            .expect("memo should have a cached value after recomputing"))
    }
}

impl<T: 'static> Memo<T> {
    /// Runs `f` with a shared reference to the current computed value without recording a
    /// dependency. The memo is still brought up to date before `f` runs.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`with`](Memo::with).
    pub fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.inner.reactor.assert_no_cycle(self.inner.id);
        self.inner.refresh();
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

    /// Clones and returns the current computed value without recording a dependency.
    pub fn peek(&self) -> T {
        self.with_peek(T::clone)
    }
}

/// Shared marking behavior for computed nodes: record the strongest staleness seen, and when the
/// node first leaves the clean state, tell downstream observers to verify their inputs.
fn mark_computed(reactor: &Reactor, id: NodeId, state: &Cell<State>, mark: Mark) {
    let target = State::from(mark);
    let previous = state.get();
    if previous >= target {
        return;
    }
    state.set(target);
    if previous == State::Clean {
        reactor.mark_dependents(id, Mark::Check);
    }
}

impl<T> core::fmt::Debug for Thunk<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Thunk")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

impl<T> core::fmt::Debug for Memo<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Memo")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

struct ThunkInner<T> {
    reactor: Reactor,
    id: NodeId,
    compute: Box<ComputeFn<T>>,
    value: RefCell<Option<T>>,
    state: Cell<State>,
}

impl<T> ThunkInner<T> {
    fn refresh(&self) {
        match self.state.get() {
            State::Clean => {}
            State::Check => {
                if self.reactor.dependencies_changed(self.id) {
                    self.recompute();
                } else {
                    self.state.set(State::Clean);
                }
            }
            State::Dirty => self.recompute(),
        }
    }

    fn recompute(&self) {
        let _span = tracing::debug_span!(
            target: trace_targets::THUNK,
            "thunk.recompute",
            node_id = self.id.0
        )
        .entered();
        // Clear staleness before computing so a (discouraged) write from inside the compute to
        // one of its own dependencies re-marks this node instead of being lost; restore the
        // mark if the compute unwinds so the next read retries.
        self.state.set(State::Clean);
        let mut guard = crate::reactor::DirtyOnUnwind {
            state: &self.state,
            armed: true,
        };
        let next = self.reactor.run_in_context(self.id, || (self.compute)());
        guard.armed = false;
        *self.value.borrow_mut() = Some(next);
        // A thunk has no equality comparator, so every recomputation counts as a change.
        self.reactor.bump_version(self.id);
    }
}

struct MemoInner<T> {
    reactor: Reactor,
    id: NodeId,
    compute: Box<ComputePrevFn<T>>,
    equals: Box<EqualsFn<T>>,
    value: RefCell<Option<T>>,
    state: Cell<State>,
}

impl<T> MemoInner<T> {
    fn refresh(&self) {
        match self.state.get() {
            State::Clean => {}
            State::Check => {
                if self.reactor.dependencies_changed(self.id) {
                    self.recompute();
                } else {
                    self.state.set(State::Clean);
                }
            }
            State::Dirty => {
                self.recompute();
            }
        }
    }

    fn recompute(&self) {
        let _span = tracing::debug_span!(
            target: trace_targets::MEMO,
            "memo.recompute",
            node_id = self.id.0
        )
        .entered();
        // Clear staleness before computing so a (discouraged) write from inside the compute to
        // one of its own dependencies re-marks this node instead of being lost; restore the
        // mark if the compute unwinds so the next read retries.
        self.state.set(State::Clean);
        let mut guard = crate::reactor::DirtyOnUnwind {
            state: &self.state,
            armed: true,
        };
        let next = {
            let previous = self.value.borrow();
            self.reactor
                .run_in_context(self.id, || (self.compute)(previous.as_ref()))
        };
        guard.armed = false;
        // Compare under a shared borrow (so a comparator that reads this memo cannot hit a
        // borrow conflict) and without tracking (so a comparator's reads never become
        // dependencies of whatever observer is currently running).
        let changed = {
            let value = self.value.borrow();
            match value.as_ref() {
                Some(current) => crate::untrack(|| !(self.equals)(current, &next)),
                None => true,
            }
        };
        *self.value.borrow_mut() = Some(next);
        if changed {
            self.reactor.bump_version(self.id);
        }
        tracing::debug!(
            target: trace_targets::MEMO,
            event = "memo_recompute",
            node_id = self.id.0,
            changed,
            "recomputed memo"
        );
    }
}

impl<T: 'static> ObserverHook for ThunkInner<T> {
    fn mark(&self, mark: Mark) {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::THUNK,
            event = "mark_thunk",
            node_id = self.id.0,
            ?mark,
            "marking thunk stale"
        );
        mark_computed(&self.reactor, self.id, &self.state, mark);
    }

    fn refresh(&self) {
        ThunkInner::refresh(self);
    }
}

impl<T> Drop for ThunkInner<T> {
    fn drop(&mut self) {
        self.reactor.unregister_observer(self.id);
        self.reactor.dispose(self.id);
    }
}

impl<T: 'static> ObserverHook for MemoInner<T> {
    fn mark(&self, mark: Mark) {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::MEMO,
            event = "mark_memo",
            node_id = self.id.0,
            ?mark,
            "marking memo stale"
        );
        mark_computed(&self.reactor, self.id, &self.state, mark);
    }

    fn refresh(&self) {
        MemoInner::refresh(self);
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
    use crate::{
        EffectHandle, Memo, Reactor, Signal, memo_by_in, memo_in, memo_with_prev_in, signal_in,
        thunk_in,
    };

    #[test]
    fn thunk_caches_until_invalidated() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 2usize);
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
        let base = signal_in(&reactor, 5usize);
        let extra = signal_in(&reactor, 1usize);

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
    fn diamond_memos_do_not_glitch_or_recompute_redundantly() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 1i64);
        let sum_computes = Rc::new(Counter::new(0usize));

        // Diamond: source -> (left, right) -> sum.
        let left = memo_in(&reactor, {
            let source = source.clone();
            move || source.get() * 10
        });
        let right = memo_in(&reactor, {
            let source = source.clone();
            move || source.get() * 100
        });
        let sum = memo_in(&reactor, {
            let left = left.clone();
            let right = right.clone();
            let sum_computes = Rc::clone(&sum_computes);
            move || {
                sum_computes.set(sum_computes.get() + 1);
                left.get() + right.get()
            }
        });

        assert_eq!(sum.get(), 110);
        assert_eq!(sum_computes.get(), 1);

        // With eager invalidation this produced a transient glitch value (fresh right + stale
        // left) and computed sum twice. Lazy verification must compute it exactly once, from
        // consistent inputs.
        source.set(2);
        assert_eq!(sum.get(), 220);
        assert_eq!(sum_computes.get(), 2);
    }

    #[test]
    fn unchanged_memo_suppresses_downstream_recomputation() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 1usize);
        let downstream_computes = Rc::new(Counter::new(0usize));

        let parity = memo_in(&reactor, {
            let source = source.clone();
            move || source.get() % 2
        });
        let label = memo_in(&reactor, {
            let parity = parity.clone();
            let downstream_computes = Rc::clone(&downstream_computes);
            move || {
                downstream_computes.set(downstream_computes.get() + 1);
                format!("parity: {}", parity.get())
            }
        });

        assert_eq!(label.get(), "parity: 1");
        assert_eq!(downstream_computes.get(), 1);

        // 1 -> 3 keeps parity at 1: the memo recomputes but reports no change, so the
        // downstream memo must not recompute.
        source.set(3);
        assert_eq!(label.get(), "parity: 1");
        assert_eq!(downstream_computes.get(), 1);

        source.set(4);
        assert_eq!(label.get(), "parity: 0");
        assert_eq!(downstream_computes.get(), 2);
    }

    #[test]
    fn memo_with_prev_receives_previous_value() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 5i64);

        // A running maximum: reads its own previous value without a dependency cycle.
        let max_seen = memo_with_prev_in(&reactor, {
            let source = source.clone();
            move |previous: Option<&i64>| {
                let current = source.get();
                match previous {
                    Some(&best) if best >= current => best,
                    _ => current,
                }
            }
        });

        assert_eq!(max_seen.get(), 5);
        source.set(9);
        assert_eq!(max_seen.get(), 9);
        source.set(3);
        assert_eq!(max_seen.get(), 9);
        source.set(12);
        assert_eq!(max_seen.get(), 12);
    }

    #[test]
    fn cycles_discovered_through_verification_report_cycle_errors() {
        let reactor = Reactor::new();
        let flag = signal_in(&reactor, false);
        let source = signal_in(&reactor, 1i32);
        let v_slot = Rc::new(RefCell::new(None::<Thunk<i32>>));

        // x reads v only when flag is set; v always reads x. The cycle exists only after the
        // flip, and is then discovered through v's Check-state dependency verification (which
        // re-enters x mid-compute) rather than through a direct read.
        let x = thunk_in(&reactor, {
            let flag = flag.clone();
            let source = source.clone();
            let v_slot = Rc::clone(&v_slot);
            move || {
                if flag.get() {
                    v_slot.borrow().as_ref().expect("v should exist").get()
                } else {
                    source.get()
                }
            }
        });
        let v = thunk_in(&reactor, {
            let x = x.clone();
            move || x.get()
        });
        *v_slot.borrow_mut() = Some(v.clone());

        assert_eq!(v.get(), 1);
        flag.set(true); // x becomes dirty, v becomes check

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = x.get();
        }))
        .expect_err("verification-path cycle should panic");
        let error = panic
            .downcast_ref::<String>()
            .expect("panic should be a formatted cycle error");
        assert!(
            error.contains("reactive cycle detected"),
            "expected a cycle error, got: {error}"
        );
    }

    #[test]
    fn a_write_from_inside_a_compute_re_marks_the_thunk() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 0i32);

        // An impure compute that writes its own dependency. The first read observes the
        // pre-write value, but the self-inflicted invalidation must not be lost.
        let clamped = thunk_in(&reactor, {
            let source = source.clone();
            move || {
                let value = source.get();
                if value < 10 {
                    source.set(10);
                }
                value
            }
        });

        assert_eq!(clamped.get(), 0);
        assert_eq!(clamped.get(), 10, "the self-write must re-mark the thunk");
        assert_eq!(clamped.get(), 10);
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
        assert!(
            error.contains("created at") && error.contains("thunk.rs"),
            "panic should name the source locations of the nodes in the cycle, got: {error}"
        );
    }

    #[test]
    fn memo_suppresses_unchanged_results_and_memo_by_uses_custom_comparator() {
        let reactive_seen = Rc::new(RefCell::new(Vec::new()));
        let effect_slot = Rc::new(RefCell::new(None::<EffectHandle>));
        let source_slot = Rc::new(RefCell::new(None::<Signal<usize>>));
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
                let source = signal_in(&reactor, 1usize);
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
                    .expect("source signal should still be alive")
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
