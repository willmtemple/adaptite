//! Reading the shape of a live graph.
//!
//! Adaptite records a dependency edge, a creation site and a version for every node because
//! propagation needs them. This module publishes what it already knows, so that a consumer can
//! answer "why did this update", find a node that is accumulating observers, or assert that a
//! deterministic workload retained nothing — none of which should require a diagnostic
//! subscription or a fork.
//!
//! Everything here reads *recorded* state. Nothing refreshes a computed node, evaluates a
//! computation, or records a dependency of its own, so an inspection can never perturb the graph
//! it is inspecting.

use alloc::vec::Vec;
use core::panic::Location;

use crate::reactor::State;
use crate::stats::GraphStats;
use crate::{NodeId, NodeKind, Reactor, ReactorId};

/// How stale a node is.
///
/// Sources are never stale — they *are* the truth — so only computed nodes and effects report
/// one.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeState {
    /// Up to date.
    Clean,
    /// A computed dependency may have changed; the node must verify its inputs before deciding
    /// whether to recompute.
    Check,
    /// A direct dependency definitely changed.
    Dirty,
}

impl From<State> for NodeState {
    fn from(state: State) -> Self {
        match state {
            State::Clean => Self::Clean,
            State::Check => Self::Check,
            State::Dirty => Self::Dirty,
        }
    }
}

/// One node in a [`GraphSnapshot`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphNode {
    /// Process-local identity, unique within this reactor.
    pub id: NodeId,
    /// The primitive the node was created as.
    pub kind: NodeKind,
    /// Where it was created.
    pub origin: &'static Location<'static>,
    /// Current version. Increments whenever the node's value changes.
    pub version: u64,
    /// How stale the node is, or `None` for a node that is never stale — a source, a signal, an
    /// event: anything with no computation to bring up to date.
    pub state: Option<NodeState>,
    /// Dependencies recorded during this node's last run.
    pub dependencies: usize,
    /// Observers currently recording a dependency on this node.
    pub dependents: usize,
}

/// A recorded dependency: `observer` read `observable`.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GraphEdge {
    /// The node that performed the read.
    pub observer: NodeId,
    /// The node that was read.
    pub observable: NodeId,
}

/// Everything a reactor is holding, walked, plus what this thread's ownership tree is holding.
///
/// The counterpart to [`GraphStats`], and the distinction between them is the point:
/// `graph_stats` is `O(1)` and answers *how much*, safe to call every frame; this walks every
/// node and edge and answers *what*, for a human, an inspector, or a post-mortem. Calling this
/// one per frame is a mistake.
///
/// Reading a snapshot never refreshes a computed node or evaluates a computation, so an
/// inspection cannot perturb the graph it is inspecting — including the `state` field, which
/// reports staleness rather than resolving it.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct GraphSnapshot {
    /// Graph this describes.
    pub reactor: ReactorId,
    /// Every live node, ordered by id, so two snapshots can be diffed directly.
    pub nodes: Vec<GraphNode>,
    /// Every recorded dependency, ordered.
    pub edges: Vec<GraphEdge>,
    /// The `O(1)` account, taken at the same moment.
    pub stats: GraphStats,
    /// What this thread's owner tree is holding, taken at the same moment.
    ///
    /// Thread-scoped rather than per-reactor, because adaptite's ownership is — see
    /// [`crate::OwnershipStats`]. Included here because the two questions are almost always asked
    /// together: a graph that looks clean and an owner tree that is still holding a subtree is a
    /// leak, and reading the two from separate calls invites reading them at separate moments.
    pub ownership: crate::OwnershipStats,
}

impl GraphSnapshot {
    /// Returns the node with `id`, if it is live.
    pub fn node(&self, id: NodeId) -> Option<&GraphNode> {
        self.nodes
            .binary_search_by_key(&id, |node| node.id)
            .ok()
            .map(|index| &self.nodes[index])
    }

    /// Returns the nodes that are not [`Clean`](NodeState::Clean).
    ///
    /// On a settled graph this is empty. When it is not, and nothing is scheduled, something is
    /// holding staleness nobody will resolve.
    pub fn stale(&self) -> impl Iterator<Item = &GraphNode> {
        self.nodes
            .iter()
            .filter(|node| !matches!(node.state, None | Some(NodeState::Clean)))
    }
}

impl Reactor {
    /// Returns `true` if any live observer currently records a dependency on `node`.
    ///
    /// This reflects the edges recorded by each observer's most recent run: an observer that
    /// stopped reading `node` still counts until it next re-runs (or is disposed). The primary
    /// use is garbage collection in fine-grained data structures — dropping per-key
    /// [`crate::Source`] nodes that no longer have readers.
    pub fn is_observed(&self, node: NodeId) -> bool {
        self.observer_count(node) > 0
    }

    /// Returns how many observers currently record a dependency on `node`.
    ///
    /// `O(1)` and allocation-free — the dependent set is already indexed by node — so this is the
    /// query to reach for on a hot path or in a per-frame assertion. A reactive graph leaks by
    /// accumulating observers that never detach, and this is the number that says so.
    ///
    /// Carries the same recorded-edge semantics as [`is_observed`](Self::is_observed): the count
    /// can be late (an observer that stopped reading `node` still counts until it re-runs or is
    /// disposed) but never early.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::{Reactor, memo_in, signal_in};
    ///
    /// let reactor = Reactor::new();
    /// let value = signal_in(&reactor, 1);
    /// assert_eq!(reactor.observer_count(value.id()), 0);
    ///
    /// let doubled = memo_in(&reactor, {
    ///     let value = value.clone();
    ///     move || value.get() * 2
    /// });
    /// assert_eq!(doubled.get(), 2);
    /// assert_eq!(reactor.observer_count(value.id()), 1);
    /// ```
    pub fn observer_count(&self, node: NodeId) -> usize {
        self.inner
            .dependents
            .borrow()
            .get(&node)
            .map_or(0, |observers| observers.len())
    }

    /// Walks the whole graph and returns everything in it.
    ///
    /// See [`GraphSnapshot`]. This is `O(nodes + edges)` and allocates — the tool for a human, an
    /// inspector, or a post-mortem, not for a per-frame assertion. Reach for
    /// [`graph_stats`](Self::graph_stats) when the question is *how much* rather than *what*.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::{NodeKind, Reactor, memo_in, signal_in};
    ///
    /// let reactor = Reactor::new();
    /// let value = signal_in(&reactor, 1_u32);
    /// let doubled = memo_in(&reactor, {
    ///     let value = value.clone();
    ///     move || value.get() * 2
    /// });
    /// assert_eq!(doubled.get(), 2);
    ///
    /// let snapshot = reactor.debug_graph();
    /// assert_eq!(snapshot.nodes.len(), 2);
    /// assert_eq!(snapshot.edges.len(), 1);
    /// assert_eq!(snapshot.edges[0].observer, doubled.id());
    /// assert_eq!(snapshot.edges[0].observable, value.id());
    ///
    /// // A source is never stale; a memo that has been read is clean.
    /// assert_eq!(snapshot.node(value.id()).unwrap().kind, NodeKind::Signal);
    /// assert!(snapshot.node(value.id()).unwrap().state.is_none());
    /// assert_eq!(snapshot.stale().count(), 0);
    ///
    /// // Writing leaves the memo stale until something reads it again.
    /// value.set(2);
    /// let snapshot = reactor.debug_graph();
    /// assert_eq!(snapshot.stale().count(), 1);
    /// ```
    pub fn debug_graph(&self) -> GraphSnapshot {
        let meta = self.inner.meta.borrow();
        let dependencies = self.inner.dependencies.borrow();
        let dependents = self.inner.dependents.borrow();
        let observers = self.inner.observers.borrow();

        let mut nodes = Vec::with_capacity(meta.len());
        let mut edges = Vec::new();
        for (id, entry) in meta.iter() {
            let outgoing = dependencies.get(id);
            if let Some(outgoing) = outgoing {
                edges.extend(outgoing.keys().map(|observable| GraphEdge {
                    observer: *id,
                    observable: *observable,
                }));
            }
            nodes.push(GraphNode {
                id: *id,
                kind: entry.kind,
                origin: entry.origin,
                version: entry.version,
                // Absent for anything with no computation to bring up to date, and for an
                // observer whose hook has been dropped but whose metadata is still live.
                state: observers
                    .get(id)
                    .and_then(|weak| weak.upgrade())
                    .map(|hook| hook.state().into()),
                dependencies: outgoing.map_or(0, hashbrown::HashMap::len),
                dependents: dependents.get(id).map_or(0, hashbrown::HashSet::len),
            });
        }

        // Hash-map iteration order is arbitrary and varies run to run; sorting is what lets two
        // snapshots be compared, diffed, or asserted against.
        nodes.sort_unstable_by_key(|node| node.id);
        edges.sort_unstable();

        drop((meta, dependencies, dependents, observers));
        GraphSnapshot {
            reactor: self.inner.id,
            nodes,
            edges,
            stats: self.graph_stats(),
            ownership: crate::ownership_stats(),
        }
    }

    /// Returns how many dependencies `node` recorded during its last run.
    ///
    /// The `O(1)`, allocation-free counterpart to
    /// [`dependencies_of`](Self::dependencies_of). A computation whose count climbs run over run
    /// is reading more of the graph each time, which is the shape behind a component that gets
    /// slower the longer it lives.
    pub fn dependency_count(&self, node: NodeId) -> usize {
        self.inner
            .dependencies
            .borrow()
            .get(&node)
            .map_or(0, hashbrown::HashMap::len)
    }

    /// Returns the observers that currently record a dependency on `node`.
    ///
    /// The enumerating counterpart to [`observer_count`](Self::observer_count), for an inspector
    /// or a post-mortem that needs to name the observers rather than count them. It copies the
    /// set out, so prefer `observer_count` when only the number is wanted.
    pub fn dependents_of(&self, node: NodeId) -> Vec<NodeId> {
        self.inner
            .dependents
            .borrow()
            .get(&node)
            .map(|observers| observers.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Returns the dependencies recorded during `node`'s last run, each with the version of that
    /// dependency observed at the time.
    ///
    /// This is the edge set that dependency verification compares against, and reading it is how
    /// a consumer answers "why did this update": the dependency whose current
    /// [`version`](Self::node_version) differs from the version recorded here is the one that
    /// invalidated `node`.
    ///
    /// A snapshot, copied out so no borrow is held across graph mutation. Nothing is refreshed
    /// and no reactive computation runs — this is not a read and never records a dependency of
    /// its own.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::{Reactor, memo_in, signal_in};
    ///
    /// let reactor = Reactor::new();
    /// let value = signal_in(&reactor, 1);
    /// let doubled = memo_in(&reactor, {
    ///     let value = value.clone();
    ///     move || value.get() * 2
    /// });
    /// assert_eq!(doubled.get(), 2);
    ///
    /// let dependencies = reactor.dependencies_of(doubled.id());
    /// assert_eq!(dependencies.len(), 1);
    /// assert_eq!(dependencies[0].0, value.id());
    ///
    /// // The recorded version is what a later write is compared against.
    /// value.set(2);
    /// assert_ne!(reactor.node_version(value.id()), Some(dependencies[0].1));
    /// ```
    pub fn dependencies_of(&self, node: NodeId) -> Vec<(NodeId, u64)> {
        self.inner
            .dependencies
            .borrow()
            .get(&node)
            .map(|edges| edges.iter().map(|(id, version)| (*id, *version)).collect())
            .unwrap_or_default()
    }

    /// Returns the source location at which `node` was created, or `None` if it is not live.
    ///
    /// Every node records its creation site via `#[track_caller]`. Until now that origin was
    /// reachable only when adaptite chose to hand it over — in a [`ReactCycleError`](crate::ReactCycleError), in the
    /// divergence panic, or attached to a diagnostic event. This answers for any node, which is
    /// what an inspector, a leak report, or a post-mortem needs.
    ///
    /// `None` means the node has been disposed or never existed; ids are never reused, so it
    /// cannot mean "some other node now".
    pub fn node_origin(&self, node: NodeId) -> Option<&'static Location<'static>> {
        self.inner.meta.borrow().get(&node).map(|meta| meta.origin)
    }

    /// Returns the primitive `node` was created as, or `None` if it is not live.
    ///
    /// See [`NodeKind`] for what "created as" means — the kind is declared at construction, not
    /// inferred from how the node is used.
    pub fn node_kind(&self, node: NodeId) -> Option<NodeKind> {
        self.inner.meta.borrow().get(&node).map(|meta| meta.kind)
    }

    /// Returns `node`'s current version, or `None` if it is not live.
    ///
    /// The version increments whenever the node's value changes — every write for a source, and
    /// every recomputation that a memo's comparator does not suppress. Comparing it against the
    /// version recorded in [`dependencies_of`](Self::dependencies_of) is how verification decides
    /// whether an observer must re-run, and comparing two samples is how a consumer detects
    /// change without subscribing.
    pub fn node_version(&self, node: NodeId) -> Option<u64> {
        self.inner.meta.borrow().get(&node).map(|meta| meta.version)
    }

    /// Returns an `O(1)` account of what this reactor is currently holding.
    ///
    /// See [`GraphStats`] for the cost contract and the intended before/after use. Nothing here
    /// walks the graph, so this is safe to call every frame.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::{Reactor, memo_in, signal_in};
    ///
    /// let reactor = Reactor::new();
    /// let value = signal_in(&reactor, 1_u32);
    /// let doubled = memo_in(&reactor, {
    ///     let value = value.clone();
    ///     move || value.get() * 2
    /// });
    /// assert_eq!(doubled.get(), 2);
    ///
    /// let stats = reactor.graph_stats();
    /// assert_eq!(stats.live_nodes, 2);
    /// assert_eq!(stats.live_edges, 1, "the memo reads the signal");
    /// assert_eq!(stats.observed_nodes, 1, "only the signal has an observer");
    /// assert_eq!(stats.reactor, reactor.id());
    /// ```
    pub fn graph_stats(&self) -> GraphStats {
        self.inner.counters.snapshot(
            self.inner.id,
            self.inner.meta.borrow().len(),
            // Entries are removed as soon as a node's last observer leaves, so the map's length
            // *is* the observed-node count.
            self.inner.dependents.borrow().len(),
            self.inner.pending_jobs.borrow().len(),
            self.inner.flush_depth.get(),
            self.inner.flush_epoch.get(),
        )
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use crate::{NodeKind, NodeState, Reactor, memo_in, signal_in, source_in};

    #[test]
    fn a_snapshot_describes_the_whole_graph_and_is_ordered() {
        let reactor = Reactor::new();
        let left = signal_in(&reactor, 1_u32);
        let right = signal_in(&reactor, 2_u32);
        let total = memo_in(&reactor, {
            let left = left.clone();
            let right = right.clone();
            move || left.get() + right.get()
        });
        let effect = reactor.effect({
            let total = total.clone();
            move || {
                let _ = total.get();
            }
        });
        reactor.flush_now();

        let snapshot = reactor.debug_graph();
        assert_eq!(snapshot.reactor, reactor.id());
        assert_eq!(snapshot.nodes.len(), 4);
        assert_eq!(snapshot.stats.live_nodes, 4);

        let ids = snapshot
            .nodes
            .iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(
            ids, sorted,
            "nodes must be ordered so snapshots can be diffed"
        );

        let memo = snapshot.node(total.id()).expect("the memo is live");
        assert_eq!(memo.kind, NodeKind::Memo);
        assert_eq!(memo.dependencies, 2);
        assert_eq!(memo.dependents, 1);
        assert_eq!(memo.state, Some(NodeState::Clean));
        assert!(memo.origin.file().ends_with("inspect.rs"));

        // Sources have no computation to bring up to date, so they report no staleness at all
        // rather than a misleading "clean".
        assert_eq!(snapshot.node(left.id()).expect("live").state, None);

        // memo->left, memo->right, effect->memo.
        assert_eq!(snapshot.edges.len(), 3);
        assert_eq!(snapshot.stats.live_edges, 3);
        assert!(
            snapshot
                .edges
                .iter()
                .any(|edge| edge.observer == total.id() && edge.observable == left.id())
        );

        effect.dispose();
    }

    #[test]
    fn staleness_is_reported_without_being_resolved() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 1_u32);
        let doubled = memo_in(&reactor, {
            let source = source.clone();
            move || source.get() * 2
        });
        assert_eq!(doubled.get(), 2);
        assert_eq!(reactor.debug_graph().stale().count(), 0);

        source.set(5);

        // Twice: an inspection that recomputed on the way past would answer its own question and
        // hide the thing being investigated.
        for _ in 0..2 {
            let snapshot = reactor.debug_graph();
            let stale = snapshot.stale().collect::<Vec<_>>();
            assert_eq!(stale.len(), 1);
            assert_eq!(stale[0].id, doubled.id());
            assert_eq!(stale[0].state, Some(NodeState::Dirty));
        }

        assert_eq!(doubled.get(), 10);
        assert_eq!(reactor.debug_graph().stale().count(), 0);
    }

    #[test]
    fn a_graph_nobody_flushes_is_visible_as_a_graph_nobody_flushes() {
        // Kiln's second bug: signals created outside a render landed on a reactor nobody
        // flushed, so reads and writes worked and nothing re-rendered. From inside that reactor
        // the failure is invisible; from outside, the whole graph is one unobserved node.
        let application = Reactor::new();
        let stranded = Reactor::new();

        let held = signal_in(&stranded, 0_u32);
        let effect = application.effect(|| {});
        application.flush_now();

        let orphan = stranded.debug_graph();
        assert_eq!(orphan.nodes.len(), 1);
        assert_eq!(orphan.edges.len(), 0);
        assert_eq!(orphan.stats.observed_nodes, 0);
        assert_eq!(
            orphan.stats.flushes, 0,
            "the telltale: this graph has never been flushed"
        );
        assert_ne!(orphan.reactor, application.debug_graph().reactor);

        drop(held);
        effect.dispose();
    }

    #[test]
    fn an_unobserved_source_still_appears() {
        let reactor = Reactor::new();
        let node = source_in(&reactor);

        let snapshot = reactor.debug_graph();
        assert_eq!(snapshot.nodes.len(), 1);
        assert_eq!(
            snapshot.node(node.id()).expect("live").kind,
            NodeKind::Source
        );
        assert_eq!(snapshot.node(node.id()).expect("live").dependents, 0);
        assert!(snapshot.edges.is_empty());
    }
}
