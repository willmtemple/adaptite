use core::fmt;

/// Stable identifier for a node in a reactive graph.
///
/// Ids are unique within a single [`crate::Reactor`]; nodes belonging to different reactors
/// may share the same id.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub(crate) u64);

impl NodeId {
    pub(crate) const fn new(raw: u64) -> Self {
        Self(raw)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
