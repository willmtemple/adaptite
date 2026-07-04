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
