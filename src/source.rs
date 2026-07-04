use alloc::rc::Rc;

use crate::{NodeId, Reactor, current, trace_targets};

/// Creates a low-level reactive source node in the current reactor.
#[track_caller]
pub fn source() -> Source {
    current().source()
}

/// Creates a low-level reactive source node associated with `reactor`.
#[track_caller]
pub fn source_in(reactor: &Reactor) -> Source {
    reactor.source()
}

/// Low-level observable source node.
///
/// `Source` is useful for advanced data structures that want precise control over when reads
/// observe and writes trigger invalidation without storing their state in a [`crate::Signal`].
///
/// # Examples
///
/// Wrapping state that lives outside the graph:
///
/// ```rust
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// use adaptite::{source, thunk};
///
/// let external = Rc::new(Cell::new(1));
/// let node = source();
///
/// let view = thunk({
///     let node = node.clone();
///     let external = Rc::clone(&external);
///     move || {
///         node.observe(); // reads of `external` depend on `node`
///         external.get() * 10
///     }
/// });
///
/// assert_eq!(view.get(), 10);
///
/// external.set(2);
/// assert_eq!(view.get(), 10); // the graph has not been told about the write
///
/// node.trigger();
/// assert_eq!(view.get(), 20); // now the thunk recomputes
/// ```
#[derive(Clone)]
pub struct Source {
    inner: Rc<SourceInner>,
}

impl Reactor {
    /// Creates a low-level source node associated with this reactor.
    #[track_caller]
    pub fn source(&self) -> Source {
        Source::new(self.clone())
    }
}

impl Source {
    #[track_caller]
    fn new(reactor: Reactor) -> Self {
        let id = reactor.allocate_node();
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "create_source",
            node_id = id.0,
            "created low-level reactive source"
        );
        Self {
            inner: Rc::new(SourceInner { reactor, id }),
        }
    }

    /// Records a dependency on this source for the currently running observer.
    pub fn observe(&self) {
        self.inner.reactor.observe(self.inner.id);
    }

    /// Triggers this source's dependents.
    pub fn trigger(&self) {
        self.inner.reactor.trigger(self.inner.id);
    }

    /// Returns the source node id.
    pub fn id(&self) -> NodeId {
        self.inner.id
    }
}

struct SourceInner {
    reactor: Reactor,
    id: NodeId,
}

impl Drop for SourceInner {
    fn drop(&mut self) {
        self.reactor.dispose(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use runite::{queue_macrotask, run};

    use crate::{EffectHandle, Reactor, Source, source_in};

    #[test]
    fn sources_have_distinct_ids() {
        let reactor = Reactor::new();
        let one = source_in(&reactor);
        let two = source_in(&reactor);
        assert_ne!(one.id(), two.id());
    }

    #[test]
    fn observe_and_trigger_drive_an_effect_around_external_state() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let keep_alive = Rc::new(RefCell::new(None::<(Source, EffectHandle)>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let keep_alive = Rc::clone(&keep_alive);
            move || {
                let reactor = Reactor::new();
                let source = source_in(&reactor);
                // State managed outside the reactive graph; the source stands in for it.
                let external = Rc::new(Cell::new(1usize));

                let effect = reactor.effect({
                    let source = source.clone();
                    let external = Rc::clone(&external);
                    let seen = Rc::clone(&seen);
                    move || {
                        source.observe();
                        seen.borrow_mut().push(external.get());
                    }
                });
                *keep_alive.borrow_mut() = Some((source.clone(), effect));

                runite::queue_macrotask(move || {
                    external.set(2);
                    source.trigger();
                });
            }
        });

        run();
        assert_eq!(&*seen.borrow(), &[1, 2]);
    }
}
