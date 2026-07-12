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
}

impl<T: 'static> Observable for Thunk<T> {
    type Item = T;

    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        Thunk::with(self, f)
    }

    fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        Thunk::with_peek(self, f)
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
}

/// Object-safe core used by [`DynObservable`] to erase a concrete [`Observable`].
trait ErasedObservable<T> {
    fn with_erased(&self, f: &mut dyn FnMut(&T));
    fn with_peek_erased(&self, f: &mut dyn FnMut(&T));
}

impl<O: Observable> ErasedObservable<O::Item> for O {
    fn with_erased(&self, f: &mut dyn FnMut(&O::Item)) {
        self.with(|value| f(value));
    }

    fn with_peek_erased(&self, f: &mut dyn FnMut(&O::Item)) {
        self.with_peek(|value| f(value));
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
}
