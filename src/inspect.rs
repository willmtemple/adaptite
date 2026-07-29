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

use crate::stats::GraphStats;
use crate::{NodeId, NodeKind, Reactor};

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
    /// reachable only when adaptite chose to hand it over — in a [`ReactCycleError`], in the
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
