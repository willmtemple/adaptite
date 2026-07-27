use alloc::boxed::Box;
use alloc::rc::Rc;

use crate::{Memo, Observable, Reactor, Signal, current, memo_in, untrack};

/// An [`Observable`] that can also be assigned.
///
/// [`Signal`] and [`Writable`] both implement it, so a component API can accept "something I can
/// read and write" without caring whether the caller holds raw state or a derived view of it — the
/// same role Vue's writable computed plays for `v-model` targets.
///
/// # Examples
///
/// ```rust
/// use adaptite::{Observable, WritableObservable, signal, writable};
///
/// fn bump(field: &impl WritableObservable<Item = i32>) {
///     field.set(field.get() + 1);
/// }
///
/// let count = signal(1);
/// bump(&count);
/// assert_eq!(count.get(), 2);
///
/// // A derived view of the same state, and `bump` cannot tell the difference.
/// let offset = writable(
///     { let count = count.clone(); move || count.get() + 10 },
///     { let count = count.clone(); move |value: i32| { count.set(value - 10); } },
/// );
/// assert_eq!(offset.get(), 12);
///
/// bump(&offset);
/// assert_eq!(offset.get(), 13);
/// assert_eq!(count.get(), 3, "the write was translated back to the source");
/// ```
pub trait WritableObservable: Observable {
    /// Assigns a new value.
    fn set(&self, value: Self::Item);
}

impl<T: PartialEq + 'static> WritableObservable for Signal<T> {
    fn set(&self, value: T) {
        Signal::set(self, value);
    }
}

/// Creates a writable computed in the current thread's default reactor.
///
/// See [`Writable`] for the semantics.
#[track_caller]
pub fn writable<T: PartialEq + 'static>(
    get: impl Fn() -> T + 'static,
    set: impl Fn(T) + 'static,
) -> Writable<T> {
    Writable::new(&current(), get, set)
}

/// Creates a writable computed associated with `reactor`.
#[track_caller]
pub fn writable_in<T: PartialEq + 'static>(
    reactor: &Reactor,
    get: impl Fn() -> T + 'static,
    set: impl Fn(T) + 'static,
) -> Writable<T> {
    Writable::new(reactor, get, set)
}

/// A two-way bindable derived value: a memo bundled with a setter that writes back upstream.
///
/// Form bindings want one handle that is both readable and assignable, rather than a
/// `(memo, callback)` pair threaded through every component signature.
///
/// # Semantics
///
/// There is no new dependency machinery here, which is the point. Reading is an ordinary memo
/// read: tracked, cached, and equality-suppressed. Writing runs the setter [`untrack`]ed, so
/// translating an assignment into upstream signal writes never records dependencies for whichever
/// observer happened to be running. Those upstream writes invalidate the memo through the normal
/// graph, so a value-identical round trip is absorbed by the memo's equality check and propagates
/// nothing.
///
/// The setter is not required to be an exact inverse of the getter. A lossy round trip (clamping,
/// rounding, rejecting) simply means the next read reports what the source actually holds rather
/// than what was assigned — the same behavior a controlled input has.
///
/// # Examples
///
/// ```rust
/// use adaptite::{Observable, WritableObservable, signal, writable};
///
/// let celsius = signal(100.0f64);
/// let fahrenheit = writable(
///     { let celsius = celsius.clone(); move || celsius.get() * 9.0 / 5.0 + 32.0 },
///     { let celsius = celsius.clone(); move |value: f64| { celsius.set((value - 32.0) * 5.0 / 9.0); } },
/// );
///
/// assert_eq!(fahrenheit.get(), 212.0);
///
/// // Assigning the derived value writes through to the source.
/// fahrenheit.set(32.0);
/// assert_eq!(celsius.get(), 0.0);
/// assert_eq!(fahrenheit.get(), 32.0);
///
/// // And the source still drives the derived value.
/// celsius.set(37.0);
/// assert_eq!(fahrenheit.get(), 98.6);
/// ```
pub struct Writable<T> {
    memo: Memo<T>,
    set: Rc<dyn Fn(T)>,
}

// Manual impl: cloning the handle shares the node and must not require `T: Clone`.
impl<T> Clone for Writable<T> {
    fn clone(&self) -> Self {
        Self {
            memo: self.memo.clone(),
            set: Rc::clone(&self.set),
        }
    }
}

impl<T: PartialEq + 'static> Writable<T> {
    #[track_caller]
    fn new(reactor: &Reactor, get: impl Fn() -> T + 'static, set: impl Fn(T) + 'static) -> Self {
        Self {
            memo: memo_in(reactor, get),
            set: Rc::from(Box::new(set) as Box<dyn Fn(T)>),
        }
    }

    /// Returns the underlying memo, for APIs that want a read-only view.
    pub fn as_memo(&self) -> &Memo<T> {
        &self.memo
    }
}

impl<T: PartialEq + 'static> Observable for Writable<T> {
    type Item = T;

    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.memo.with(f)
    }

    fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.memo.with_peek(f)
    }

    fn reactor(&self) -> Option<Reactor> {
        Some(self.memo.reactor())
    }
}

impl<T: PartialEq + 'static> WritableObservable for Writable<T> {
    /// Translates `value` into upstream writes by running the setter untracked.
    fn set(&self, value: T) {
        untrack(|| (self.set)(value));
    }
}

impl<T> core::fmt::Debug for Writable<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Writable").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::{Writable, WritableObservable, writable_in};
    use crate::{Observable, Reactor, memo_in, signal_in};

    #[test]
    fn writes_translate_upstream_and_reads_stay_derived() {
        let reactor = Reactor::new();
        let celsius = signal_in(&reactor, 0.0f64);
        let fahrenheit = writable_in(
            &reactor,
            {
                let celsius = celsius.clone();
                move || celsius.get() * 9.0 / 5.0 + 32.0
            },
            {
                let celsius = celsius.clone();
                move |value: f64| {
                    celsius.set((value - 32.0) * 5.0 / 9.0);
                }
            },
        );

        assert_eq!(fahrenheit.get(), 32.0);

        fahrenheit.set(212.0);
        assert_eq!(celsius.get(), 100.0, "the setter wrote through");
        assert_eq!(fahrenheit.get(), 212.0);

        celsius.set(0.0);
        assert_eq!(fahrenheit.get(), 32.0, "the source still drives the memo");
    }

    #[test]
    fn a_value_identical_round_trip_propagates_nothing() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 10);
        let doubled = writable_in(
            &reactor,
            {
                let source = source.clone();
                move || source.get() * 2
            },
            {
                let source = source.clone();
                move |value: i32| {
                    source.set(value / 2);
                }
            },
        );

        let recomputes = Rc::new(RefCell::new(0));
        let downstream = memo_in(&reactor, {
            let doubled = doubled.clone();
            let recomputes = Rc::clone(&recomputes);
            move || {
                *recomputes.borrow_mut() += 1;
                doubled.get()
            }
        });
        assert_eq!(downstream.get(), 20);
        assert_eq!(*recomputes.borrow(), 1);

        // Assigning the value it already holds: the setter runs, but the signal's equality check
        // suppresses the write, so nothing downstream recomputes.
        doubled.set(20);
        assert_eq!(downstream.get(), 20);
        assert_eq!(*recomputes.borrow(), 1, "no downstream recomputation");

        doubled.set(40);
        assert_eq!(downstream.get(), 40);
        assert_eq!(*recomputes.borrow(), 2);
    }

    #[test]
    fn the_setter_runs_untracked() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 1);
        // The setter reads a second signal. That read must not become a dependency of whichever
        // observer happens to be running when the assignment occurs.
        let offset = signal_in(&reactor, 100);
        let view = writable_in(
            &reactor,
            {
                let source = source.clone();
                move || source.get()
            },
            {
                let source = source.clone();
                let offset = offset.clone();
                move |value: i32| {
                    source.set(value + offset.get());
                }
            },
        );

        let recomputes = Rc::new(RefCell::new(0));
        let observer = memo_in(&reactor, {
            let view = view.clone();
            let recomputes = Rc::clone(&recomputes);
            move || {
                *recomputes.borrow_mut() += 1;
                // Assign from inside a tracked computation.
                view.set(0);
                view.get()
            }
        });

        assert_eq!(observer.get(), 100);
        assert_eq!(*recomputes.borrow(), 1);

        // Changing the setter-only input must not invalidate the observer.
        offset.set(500);
        assert_eq!(observer.get(), 100);
        assert_eq!(
            *recomputes.borrow(),
            1,
            "reads made by the setter are not dependencies"
        );
    }

    #[test]
    fn a_lossy_setter_reports_what_the_source_holds() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 0);
        let clamped: Writable<i32> = writable_in(
            &reactor,
            {
                let source = source.clone();
                move || source.get()
            },
            {
                let source = source.clone();
                move |value: i32| {
                    source.set(value.clamp(0, 10));
                }
            },
        );

        clamped.set(50);
        assert_eq!(
            clamped.get(),
            10,
            "the next read reports the source, not the assignment"
        );
    }

    #[test]
    fn signals_and_writable_computeds_are_interchangeable() {
        fn bump(field: &impl WritableObservable<Item = i32>) {
            field.set(field.get() + 1);
        }

        let reactor = Reactor::new();
        let count = signal_in(&reactor, 1);
        bump(&count);
        assert_eq!(count.get(), 2);

        let doubled = writable_in(
            &reactor,
            {
                let count = count.clone();
                move || count.get() * 2
            },
            {
                let count = count.clone();
                move |value: i32| {
                    count.set(value / 2);
                }
            },
        );
        assert_eq!(doubled.get(), 4);
        bump(&doubled);
        assert_eq!(count.get(), 2, "5 / 2 truncates back onto the source");
        assert_eq!(doubled.get(), 4);
    }
}
