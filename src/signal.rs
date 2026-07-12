use alloc::rc::Rc;
use core::cell::RefCell;

use crate::{NodeId, Reactor, current, trace_targets};

/// Creates a [`Signal`] in the current thread's default reactor.
///
/// # Examples
///
/// ```rust
/// use adaptite::signal;
///
/// let value = signal(10);
/// assert_eq!(value.get(), 10);
///
/// // Equal writes are suppressed and do not notify dependents.
/// assert_eq!(value.set(10), None);
/// assert_eq!(value.set(11), Some(10));
///
/// // Mutate in place, and read without recording a dependency.
/// value.update(|v| *v += 1);
/// assert_eq!(value.peek(), 12);
/// ```
#[track_caller]
pub fn signal<T: 'static>(initial: T) -> Signal<T> {
    current().signal(initial)
}

/// Creates a [`Signal`] associated with `reactor`.
#[track_caller]
pub fn signal_in<T: 'static>(reactor: &Reactor, initial: T) -> Signal<T> {
    reactor.signal(initial)
}

/// Mutable source node in the reactive graph.
///
/// Reading a signal from inside an observer (an effect, thunk, or memo computation) records a
/// dependency; writing to it invalidates those observers. Clones share the same underlying
/// node, so a signal can be captured by any number of closures. The node lives as long as any
/// clone does.
///
/// Writes through [`set`](Signal::set) are suppressed when the new value equals the old one,
/// which is what allows convergent feedback (e.g. an effect clamping a value it reads) to
/// settle. [`replace`](Signal::replace) and [`update`](Signal::update) always notify.
pub struct Signal<T> {
    inner: Rc<SignalInner<T>>,
}

// Manual impl: cloning the handle shares the node and must not require `T: Clone`.
impl<T> Clone for Signal<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl Reactor {
    /// Creates a mutable source signal associated with this reactor.
    #[track_caller]
    pub fn signal<T: 'static>(&self, initial: T) -> Signal<T> {
        Signal::new(self.clone(), initial)
    }
}

impl<T: 'static> Signal<T> {
    #[track_caller]
    fn new(reactor: Reactor, initial: T) -> Self {
        let id = reactor.allocate_node();
        tracing::debug!(
            target: trace_targets::SIGNAL,
            event = "create_signal",
            node_id = id.0,
            "created reactive signal"
        );
        Self {
            inner: Rc::new(SignalInner {
                reactor,
                id,
                value: RefCell::new(initial),
            }),
        }
    }

    /// Runs `f` with a shared reference to the current value.
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::SIGNAL,
            event = "read_signal",
            node_id = self.inner.id.0,
            "reading reactive signal"
        );
        self.inner.reactor.observe(self.inner.id);
        let value = self.inner.value.borrow();
        f(&value)
    }

    /// Runs `f` with a shared reference to the current value without recording a dependency.
    pub fn with_peek<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let value = self.inner.value.borrow();
        f(&value)
    }

    /// Replaces the current value and marks dependents stale, even when the new value equals
    /// the old one (compare [`set`](Signal::set)).
    pub fn replace(&self, value: T) -> T {
        let previous = self.inner.value.replace(value);
        tracing::debug!(
            target: trace_targets::SIGNAL,
            event = "replace_signal",
            node_id = self.inner.id.0,
            "replaced signal value"
        );
        self.inner.reactor.trigger(self.inner.id);
        previous
    }

    /// Mutates the current value in place and marks dependents stale, regardless of whether
    /// `f` actually changed anything (compare [`set`](Signal::set)).
    ///
    /// # Panics
    ///
    /// `f` runs while the value is mutably borrowed: calling `get`, `with`, `peek`, or `set`
    /// on the *same* signal from inside `f` panics with a borrow error.
    pub fn update<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let output = {
            let mut value = self.inner.value.borrow_mut();
            f(&mut value)
        };
        tracing::debug!(
            target: trace_targets::SIGNAL,
            event = "update_signal",
            node_id = self.inner.id.0,
            "updated signal value in place"
        );
        self.inner.reactor.trigger(self.inner.id);
        output
    }
}

impl<T: Clone + 'static> Signal<T> {
    /// Clones and returns the current value.
    pub fn get(&self) -> T {
        self.with(T::clone)
    }

    /// Clones and returns the current value without recording a dependency.
    pub fn peek(&self) -> T {
        self.with_peek(T::clone)
    }
}

impl<T: PartialEq + 'static> Signal<T> {
    /// Sets the signal to `value`, suppressing unchanged writes.
    ///
    /// Returns the previous value if the signal changed, or `None` when the new value was equal to
    /// the old one.
    pub fn set(&self, value: T) -> Option<T> {
        // Compare under a shared borrow, without tracking: a `PartialEq` impl that reads
        // reactive state must neither conflict with this signal's own borrow nor record
        // dependencies for whatever observer is performing the write.
        let unchanged = {
            let current = self.inner.value.borrow();
            crate::untrack(|| *current == value)
        };
        if unchanged {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::SIGNAL,
                event = "set_signal",
                node_id = self.inner.id.0,
                changed = false,
                "suppressed unchanged signal write"
            );
            return None;
        }

        let previous = self.inner.value.replace(value);
        tracing::debug!(
            target: trace_targets::SIGNAL,
            event = "set_signal",
            node_id = self.inner.id.0,
            changed = true,
            "set signal value"
        );
        self.inner.reactor.trigger(self.inner.id);
        Some(previous)
    }
}

impl<T> core::fmt::Debug for Signal<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Signal")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

struct SignalInner<T> {
    reactor: Reactor,
    id: NodeId,
    value: RefCell<T>,
}

impl<T> Drop for SignalInner<T> {
    fn drop(&mut self) {
        self.reactor.dispose(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::Signal;
    use crate::Reactor;

    #[test]
    fn set_suppresses_unchanged_writes() {
        let reactor = Reactor::new();
        let value = reactor.signal(10usize);

        assert_eq!(value.set(10), None);
        assert_eq!(value.get(), 10);
        assert_eq!(value.set(11), Some(10));
        assert_eq!(value.get(), 11);
    }

    #[test]
    fn replace_and_update_write_values() {
        let reactor = Reactor::new();
        let value: Signal<Vec<usize>> = reactor.signal(vec![1, 2]);

        let old = value.replace(vec![3]);
        assert_eq!(old, vec![1, 2]);
        assert_eq!(value.with(|items| items.clone()), vec![3]);

        value.update(|items| items.push(4));
        assert_eq!(value.with(|items| items.clone()), vec![3, 4]);
    }
}
