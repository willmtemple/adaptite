use alloc::rc::Rc;

use crate::{Memo, Signal, Thunk};

/// A reactive value that records a dependency when read.
///
/// `Observable` is the common vocabulary for [`Signal`], [`Thunk`], [`Memo`], and
/// [`crate::Resource`]: anything that holds a value and participates in dependency tracking.
/// APIs that consume "some reactive value" without caring how it is produced should accept
/// `impl Observable<Item = T>` (or a [`DynObservable<T>`] where type erasure is needed, for
/// example in struct fields).
///
/// # Examples
///
/// ```rust
/// use adaptite::{Observable, memo, signal};
///
/// fn shout(text: &impl Observable<Item = String>) -> String {
///     text.with(|value| value.to_uppercase())
/// }
///
/// let name = signal(String::from("ada"));
/// let greeting = memo({
///     let name = name.clone();
///     move || format!("hello, {}", name.get())
/// });
///
/// // The same function accepts a signal or a memo.
/// assert_eq!(shout(&name), "ADA");
/// assert_eq!(shout(&greeting), "HELLO, ADA");
/// ```
pub trait Observable {
    /// The type of value this observable holds.
    type Item;

    /// Runs `f` with a shared reference to the current value, recording a dependency for the
    /// currently running observer.
    fn with<R>(&self, f: impl FnOnce(&Self::Item) -> R) -> R;

    /// Runs `f` with a shared reference to the current value without recording a dependency.
    fn with_peek<R>(&self, f: impl FnOnce(&Self::Item) -> R) -> R;

    /// Clones and returns the current value, recording a dependency.
    fn get(&self) -> Self::Item
    where
        Self::Item: Clone,
    {
        self.with(Self::Item::clone)
    }

    /// Clones and returns the current value without recording a dependency.
    fn peek(&self) -> Self::Item
    where
        Self::Item: Clone,
    {
        self.with_peek(Self::Item::clone)
    }

    /// Returns the reactor this observable's node belongs to, when it has one.
    ///
    /// Implementations backed by a graph node report their reactor so that combinators like
    /// [`map`](Self::map) build derived nodes on the *same* graph rather than on the thread
    /// default. Observables with no node — [`DynObservable::constant`], for instance — return
    /// `None`.
    fn reactor(&self) -> Option<crate::Reactor> {
        None
    }

    /// Derives a memo from this observable, cloning the receiver's handle internally.
    ///
    /// This removes the most common `let x = x.clone();` before a closure: the combinator
    /// captures the handle for you, and the receiver stays usable.
    ///
    /// The memo is created on this observable's [`reactor`](Self::reactor), so mapping a node
    /// from an explicit reactor stays on that reactor. As with any memo, an unchanged result is
    /// equality-suppressed and does not propagate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::{Observable, signal};
    ///
    /// let base = signal(2);
    /// let doubled = base.map(|value| value * 2);
    ///
    /// assert_eq!(doubled.get(), 4);
    ///
    /// // `base` was not moved.
    /// base.set(5);
    /// assert_eq!(doubled.get(), 10);
    /// ```
    fn map<U, F>(&self, f: F) -> Memo<U>
    where
        Self: Clone + Sized + 'static,
        U: PartialEq + 'static,
        F: Fn(&Self::Item) -> U + 'static,
    {
        let reactor = self.reactor().unwrap_or_else(crate::current);
        let source = self.clone();
        crate::memo_in(&reactor, move || source.with(&f))
    }

    /// Erases this observable's concrete type behind a cheaply-cloneable handle.
    fn into_dyn(self) -> DynObservable<Self::Item>
    where
        Self: Sized + 'static,
    {
        DynObservable {
            inner: Rc::new(self),
        }
    }
}

impl<T: 'static> Observable for Signal<T> {
    type Item = T;

    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        Signal::with(self, f)
    }

    fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        Signal::with_peek(self, f)
    }

    fn reactor(&self) -> Option<crate::Reactor> {
        Some(Signal::reactor(self))
    }
}

impl<T: 'static> Observable for Thunk<T> {
    type Item = T;

    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        Thunk::with(self, f)
    }

    fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        Thunk::with_peek(self, f)
    }

    fn reactor(&self) -> Option<crate::Reactor> {
        Some(Thunk::reactor(self))
    }
}

impl<T: 'static> Observable for Memo<T> {
    type Item = T;

    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        Memo::with(self, f)
    }

    fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        Memo::with_peek(self, f)
    }

    fn reactor(&self) -> Option<crate::Reactor> {
        Some(Memo::reactor(self))
    }
}

/// Object-safe core used by [`DynObservable`] to erase a concrete [`Observable`].
trait ErasedObservable<T> {
    fn with_erased(&self, f: &mut dyn FnMut(&T));
    fn with_peek_erased(&self, f: &mut dyn FnMut(&T));
    fn reactor_erased(&self) -> Option<crate::Reactor>;
}

impl<O: Observable> ErasedObservable<O::Item> for O {
    fn with_erased(&self, f: &mut dyn FnMut(&O::Item)) {
        self.with(|value| f(value));
    }

    fn with_peek_erased(&self, f: &mut dyn FnMut(&O::Item)) {
        self.with_peek(|value| f(value));
    }

    fn reactor_erased(&self) -> Option<crate::Reactor> {
        self.reactor()
    }
}

/// A type-erased, cheaply-cloneable [`Observable`].
///
/// Use this where a concrete observable type cannot appear — struct fields, collections of
/// heterogeneous reactive inputs, or component APIs that accept "a reactive `T`" regardless of
/// whether the caller has a [`Signal`], [`Memo`], [`Thunk`], or a constant.
///
/// # Examples
///
/// ```rust
/// use adaptite::{DynObservable, Observable, signal};
///
/// struct Label {
///     text: DynObservable<String>,
/// }
///
/// let static_label = Label {
///     text: DynObservable::constant(String::from("fixed")),
/// };
/// let dynamic = signal(String::from("live"));
/// let dynamic_label = Label {
///     text: dynamic.clone().into_dyn(),
/// };
///
/// assert_eq!(static_label.text.get(), "fixed");
/// assert_eq!(dynamic_label.text.get(), "live");
/// dynamic.set(String::from("updated"));
/// assert_eq!(dynamic_label.text.get(), "updated");
/// ```
pub struct DynObservable<T> {
    inner: Rc<dyn ErasedObservable<T>>,
}

impl<T> Clone for DynObservable<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T: 'static> DynObservable<T> {
    /// Wraps a plain value as an observable that never changes and records no dependencies.
    ///
    /// Useful for APIs that accept `DynObservable<T>` when the caller has a static value.
    pub fn constant(value: T) -> Self {
        struct Constant<T>(T);

        impl<T> Observable for Constant<T> {
            type Item = T;

            fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
                f(&self.0)
            }

            fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
                f(&self.0)
            }
        }

        Constant(value).into_dyn()
    }
}

impl<T: 'static> Observable for DynObservable<T> {
    type Item = T;

    // Already erased: avoid wrapping a second Rc around the handle.
    fn into_dyn(self) -> DynObservable<T> {
        self
    }

    // Forward to the erased observable so `map` on a type-erased handle still builds the derived
    // memo on the underlying node's reactor.
    fn reactor(&self) -> Option<crate::Reactor> {
        self.inner.reactor_erased()
    }

    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let mut f = Some(f);
        let mut output = None;
        self.inner.with_erased(&mut |value| {
            if let Some(f) = f.take() {
                output = Some(f(value));
            }
        });
        output.expect("erased observable must invoke the reader exactly once")
    }

    fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let mut f = Some(f);
        let mut output = None;
        self.inner.with_peek_erased(&mut |value| {
            if let Some(f) = f.take() {
                output = Some(f(value));
            }
        });
        output.expect("erased observable must invoke the reader exactly once")
    }
}

impl<T> core::fmt::Debug for DynObservable<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DynObservable").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use alloc::rc::Rc;
    use core::cell::Cell;

    use super::{DynObservable, Observable};
    use crate::{Reactor, memo_in, signal_in, thunk_in};

    #[test]
    fn signals_thunks_and_memos_share_the_observable_interface() {
        fn read<O: Observable<Item = i32>>(observable: &O) -> i32 {
            observable.get()
        }

        let reactor = Reactor::new();
        let base = signal_in(&reactor, 7);
        let doubled = thunk_in(&reactor, {
            let base = base.clone();
            move || base.get() * 2
        });
        let capped = memo_in(&reactor, {
            let base = base.clone();
            move || base.get().min(10)
        });

        assert_eq!(read(&base), 7);
        assert_eq!(read(&doubled), 14);
        assert_eq!(read(&capped), 7);
    }

    #[test]
    fn map_derives_a_memo_without_a_manual_clone() {
        let reactor = Reactor::new();
        let base = signal_in(&reactor, 2);

        // No `let base = base.clone()` before the closure, and `base` stays usable after.
        let doubled = base.map(|value| value * 2);
        assert_eq!(doubled.get(), 4);

        base.set(5);
        assert_eq!(doubled.get(), 10);
    }

    #[test]
    fn map_builds_on_the_receivers_reactor_not_the_thread_default() {
        // Mapping a node from an explicit reactor must stay on that reactor; landing on the
        // thread default would make the derived memo unreadable from its own source.
        let reactor = Reactor::new();
        let base = signal_in(&reactor, 1);
        let mapped = base.map(|value| value + 1);

        assert_eq!(mapped.reactor().id(), reactor.id());

        // A memo on the same reactor can compose with it without tripping the cross-reactor check.
        let composed = memo_in(&reactor, {
            let mapped = mapped.clone();
            move || mapped.get() * 10
        });
        assert_eq!(composed.get(), 20);
    }

    #[test]
    fn map_suppresses_equal_results() {
        let reactor = Reactor::new();
        let base = signal_in(&reactor, 1);
        let parity = base.map(|value| value % 2);

        let recomputes = std::rc::Rc::new(std::cell::Cell::new(0));
        let downstream = memo_in(&reactor, {
            let parity = parity.clone();
            let recomputes = std::rc::Rc::clone(&recomputes);
            move || {
                recomputes.set(recomputes.get() + 1);
                parity.get()
            }
        });
        assert_eq!(downstream.get(), 1);
        assert_eq!(recomputes.get(), 1);

        // Same parity: the mapped memo's equality check absorbs the change.
        base.set(3);
        assert_eq!(downstream.get(), 1);
        assert_eq!(recomputes.get(), 1);

        base.set(4);
        assert_eq!(downstream.get(), 0);
        assert_eq!(recomputes.get(), 2);
    }

    #[test]
    fn map_chains_and_works_through_erasure() {
        let reactor = Reactor::new();
        let base = signal_in(&reactor, 3);
        let chained = base.map(|value| value * 2).map(|value| value + 1);
        assert_eq!(chained.get(), 7);

        let erased: DynObservable<i32> = base.clone().into_dyn();
        let from_erased = erased.map(|value| value * 100);
        assert_eq!(
            from_erased.reactor().id(),
            reactor.id(),
            "erasure must not lose the reactor"
        );
        assert_eq!(from_erased.get(), 300);

        base.set(4);
        assert_eq!(chained.get(), 9);
        assert_eq!(from_erased.get(), 400);
    }

    #[test]
    fn dyn_observables_erase_and_track() {
        let reactor = Reactor::new();
        let base = signal_in(&reactor, 1);

        let erased: Vec<DynObservable<i32>> = vec![
            base.clone().into_dyn(),
            DynObservable::constant(100),
            memo_in(&reactor, {
                let base = base.clone();
                move || base.get() * 10
            })
            .into_dyn(),
        ];

        let total = || erased.iter().map(DynObservable::get).sum::<i32>();
        assert_eq!(total(), 111);

        base.set(2);
        assert_eq!(total(), 122, "erased handles still track updates");
    }

    /// `with_peek` exists to read without recording a dependency. Every implementor had it
    /// untested — including the type-erased path, where a mistake would be invisible.
    #[test]
    fn with_peek_reads_without_recording_a_dependency() {
        use crate::{DynObservable, Reactor, memo_in, signal_in, thunk_in};

        let reactor = Reactor::new();
        let value = signal_in(&reactor, 7_u32);
        let computed = thunk_in(&reactor, {
            let value = value.clone();
            move || value.get() * 2
        });
        let gated = memo_in(&reactor, {
            let value = value.clone();
            move || value.get() % 2
        });

        // Values first: peeking must still see the current value.
        assert_eq!(value.with_peek(|v| *v), 7);
        assert_eq!(computed.with_peek(|v| *v), 14);
        assert_eq!(gated.with_peek(|v| *v), 1);

        let erased: DynObservable<u32> = value.clone().into_dyn();
        assert_eq!(erased.with_peek(|v| *v), 7);
        assert!(format!("{erased:?}").contains("DynObservable"));

        // Now the property that matters: an observer that only peeks records nothing, so it is
        // never re-run. Four peeks, one per implementor, including the erased one.
        let runs = Rc::new(Cell::new(0));
        let effect = reactor.effect({
            let value = value.clone();
            let computed = computed.clone();
            let gated = gated.clone();
            let erased = erased.clone();
            let runs = Rc::clone(&runs);
            move || {
                runs.set(runs.get() + 1);
                value.with_peek(|_| {});
                computed.with_peek(|_| {});
                gated.with_peek(|_| {});
                erased.with_peek(|_| {});
            }
        });
        reactor.flush_now();
        assert_eq!(runs.get(), 1);
        assert_eq!(
            reactor.observer_count(value.id()),
            2,
            "the thunk and the memo read it; the peeking effect did not"
        );

        value.set(9);
        reactor.flush_now();
        assert_eq!(
            runs.get(),
            1,
            "an observer that only peeks must never be re-run"
        );

        effect.dispose();
    }
}
