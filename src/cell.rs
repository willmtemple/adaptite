use alloc::rc::Rc;
use core::cell::RefCell;

use crate::{NodeId, Reactor, current, trace_targets};

/// Creates a [`Cell`] in the current thread's default reactor.
pub fn cell<T: 'static>(initial: T) -> Cell<T> {
    current().cell(initial)
}

/// Creates a [`Cell`] associated with `reactor`.
pub fn cell_in<T: 'static>(reactor: &Reactor, initial: T) -> Cell<T> {
    reactor.cell(initial)
}

/// Mutable source node in the reactive graph.
#[derive(Clone)]
pub struct Cell<T> {
    inner: Rc<CellInner<T>>,
}

impl Reactor {
    /// Creates a mutable source cell associated with this reactor.
    pub fn cell<T: 'static>(&self, initial: T) -> Cell<T> {
        Cell::new(self.clone(), initial)
    }
}

impl<T: 'static> Cell<T> {
    fn new(reactor: Reactor, initial: T) -> Self {
        let id = reactor.allocate_node();
        tracing::debug!(
            target: trace_targets::CELL,
            event = "create_cell",
            node_id = id.0,
            "created reactive cell"
        );
        Self {
            inner: Rc::new(CellInner {
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
            target: trace_targets::CELL,
            event = "read_cell",
            node_id = self.inner.id.0,
            "reading reactive cell"
        );
        self.inner.reactor.observe(self.inner.id);
        let value = self.inner.value.borrow();
        f(&value)
    }

    /// Replaces the current value and notifies dependents.
    pub fn replace(&self, value: T) -> T {
        let previous = self.inner.value.replace(value);
        tracing::debug!(
            target: trace_targets::CELL,
            event = "replace_cell",
            node_id = self.inner.id.0,
            "replaced cell value"
        );
        self.inner.reactor.trigger(self.inner.id);
        previous
    }

    /// Mutates the current value in place and notifies dependents.
    pub fn update<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let output = {
            let mut value = self.inner.value.borrow_mut();
            f(&mut value)
        };
        tracing::debug!(
            target: trace_targets::CELL,
            event = "update_cell",
            node_id = self.inner.id.0,
            "updated cell value in place"
        );
        self.inner.reactor.trigger(self.inner.id);
        output
    }
}

impl<T: Clone + 'static> Cell<T> {
    /// Clones and returns the current value.
    pub fn get(&self) -> T {
        self.with(T::clone)
    }
}

impl<T: PartialEq + 'static> Cell<T> {
    /// Sets the cell to `value`, suppressing unchanged writes.
    ///
    /// Returns the previous value if the cell changed, or `None` when the new value was equal to
    /// the old one.
    pub fn set(&self, value: T) -> Option<T> {
        let mut current = self.inner.value.borrow_mut();
        if *current == value {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::CELL,
                event = "set_cell",
                node_id = self.inner.id.0,
                changed = false,
                "suppressed unchanged cell write"
            );
            return None;
        }

        let previous = core::mem::replace(&mut *current, value);
        drop(current);
        tracing::debug!(
            target: trace_targets::CELL,
            event = "set_cell",
            node_id = self.inner.id.0,
            changed = true,
            "set cell value"
        );
        self.inner.reactor.trigger(self.inner.id);
        Some(previous)
    }
}

struct CellInner<T> {
    reactor: Reactor,
    id: NodeId,
    value: RefCell<T>,
}

impl<T> Drop for CellInner<T> {
    fn drop(&mut self) {
        self.reactor.dispose(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::Cell;
    use crate::Reactor;

    #[test]
    fn set_suppresses_unchanged_writes() {
        let reactor = Reactor::new();
        let value = reactor.cell(10usize);

        assert_eq!(value.set(10), None);
        assert_eq!(value.get(), 10);
        assert_eq!(value.set(11), Some(10));
        assert_eq!(value.get(), 11);
    }

    #[test]
    fn replace_and_update_write_values() {
        let reactor = Reactor::new();
        let value: Cell<Vec<usize>> = reactor.cell(vec![1, 2]);

        let old = value.replace(vec![3]);
        assert_eq!(old, vec![1, 2]);
        assert_eq!(value.with(|items| items.clone()), vec![3]);

        value.update(|items| items.push(4));
        assert_eq!(value.with(|items| items.clone()), vec![3, 4]);
    }
}
