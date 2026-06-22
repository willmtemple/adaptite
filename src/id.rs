use core::fmt;

/// Stable identifier for a node in a reactive graph.
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
