//! Graph accounting: what a reactor is holding, and how much of it it has held.
//!
//! The diagnostic event stream explains *causality* — which write reached which effect. It does
//! not quantify the graph, and quantifying it by walking is exactly what a consumer cannot afford
//! to do every frame. This module maintains the counts as the graph changes, so a snapshot is a
//! handful of loads.

use core::cell::Cell;

use crate::{NodeKind, ReactorId};

/// A point-in-time account of one reactor's graph.
///
/// # Cost
///
/// Taking a snapshot **never walks the graph and never evaluates a reactive computation**. Every
/// field is either a maintained counter or an `O(1)` length, so this is safe to call every frame
/// and safe to call from a hot assertion. Every counter is maintained in ordinary builds, whether
/// or not diagnostics are subscribed: there is no capture to start and no mode in which the
/// numbers are absent.
///
/// # Use
///
/// `GraphStats` is `Copy` and its cumulative fields never decrease, so the intended use is a
/// difference between two snapshots:
///
/// ```rust
/// use adaptite::{Reactor, signal_in};
///
/// let reactor = Reactor::new();
/// let before = reactor.graph_stats();
///
/// let value = signal_in(&reactor, 0_u32);
/// drop(value);
///
/// let after = reactor.graph_stats();
/// assert_eq!(after.nodes_created - before.nodes_created, 1);
/// assert_eq!(after.nodes_disposed - before.nodes_disposed, 1);
/// assert_eq!(after.live_nodes, before.live_nodes, "and nothing was retained");
/// ```
///
/// A deterministic workload whose before/after node, edge and observer counts do not match is
/// retaining graph, which is what a leak in a reactive system looks like.
///
/// # Scope
///
/// Adaptite reports the structures it owns. It does not estimate application or renderer memory,
/// and it does not read a clock — a consumer that wants durations timestamps the paired events in
/// [`crate::DiagnosticEvent`] itself.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphStats {
    /// Graph these numbers describe.
    pub reactor: ReactorId,

    /// Nodes currently live.
    pub live_nodes: usize,
    /// Logical dependency edges currently recorded.
    ///
    /// One edge per observer/observable pair, counted once even though the graph indexes it from
    /// both ends.
    pub live_edges: usize,
    /// Nodes with at least one observer.
    ///
    /// The denominator for [`Reactor::observer_count`](crate::Reactor::observer_count): nodes
    /// nothing reads are not counted here.
    pub observed_nodes: usize,
    /// Effects with a run pending — scheduled and neither run nor discarded.
    ///
    /// Counts effects on a consumer-defined lane too, since the latch that makes repeat
    /// invalidations coalesce is the same one either way.
    pub queued_effects: usize,
    /// Jobs waiting in the reactor's own queue.
    pub pending_jobs: usize,
    /// Nesting depth of flushes currently open. Zero means the reactor is idle.
    pub flush_depth: u32,
    /// Most recently opened flush number. Zero means no flush has run yet.
    pub flush_epoch: u64,

    /// Highest [`live_nodes`](Self::live_nodes) reached.
    pub peak_nodes: usize,
    /// Highest [`live_edges`](Self::live_edges) reached.
    pub peak_edges: usize,
    /// Highest [`pending_jobs`](Self::pending_jobs) reached.
    pub peak_pending_jobs: usize,

    /// Nodes allocated over this reactor's life.
    pub nodes_created: u64,
    /// Nodes disposed over this reactor's life.
    pub nodes_disposed: u64,
    /// Dependency edges recorded over this reactor's life.
    ///
    /// An observer re-records its whole edge set on every run, so this grows with reactive work
    /// rather than with graph size. Compared against
    /// [`edges_removed`](Self::edges_removed) it describes churn; the difference is
    /// [`live_edges`](Self::live_edges).
    pub edges_added: u64,
    /// Dependency edges retracted over this reactor's life.
    pub edges_removed: u64,
    /// Flushes opened over this reactor's life, including nested ones.
    pub flushes: u64,

    /// Private so that adding a [`NodeKind`] stays additive; read it with
    /// [`live_nodes_of_kind`](Self::live_nodes_of_kind).
    live_nodes_by_kind: [usize; NodeKind::COUNT],
}

impl GraphStats {
    /// Returns how many live nodes were created as `kind`.
    ///
    /// This is a method rather than a public array because [`NodeKind`] is `#[non_exhaustive]`:
    /// exposing a fixed-size array would make adding a kind a breaking change for anyone who
    /// destructured or sized by it.
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
    /// let stats = reactor.graph_stats();
    /// assert_eq!(stats.live_nodes_of_kind(NodeKind::Signal), 1);
    /// assert_eq!(stats.live_nodes_of_kind(NodeKind::Memo), 1);
    /// assert_eq!(stats.live_nodes_of_kind(NodeKind::Effect), 0);
    /// ```
    pub fn live_nodes_of_kind(&self, kind: NodeKind) -> usize {
        self.live_nodes_by_kind[kind.index()]
    }
}

/// The maintained half of [`GraphStats`].
///
/// Every field here is updated where the graph changes rather than computed on demand. The
/// updates ride on operations that already hash or allocate, which is what makes the
/// always-maintained contract affordable; `benches/graph.rs` is what keeps that honest.
#[derive(Default)]
pub(crate) struct GraphCounters {
    live_nodes_by_kind: [Cell<usize>; NodeKind::COUNT],
    live_edges: Cell<usize>,
    queued_effects: Cell<usize>,
    peak_nodes: Cell<usize>,
    peak_edges: Cell<usize>,
    peak_pending_jobs: Cell<usize>,
    nodes_created: Cell<u64>,
    nodes_disposed: Cell<u64>,
    edges_added: Cell<u64>,
    edges_removed: Cell<u64>,
    flushes: Cell<u64>,
}

impl GraphCounters {
    /// Records an allocation. `live_nodes` is the node count *including* the new node.
    pub(crate) fn node_created(&self, kind: NodeKind, live_nodes: usize) {
        bump(&self.live_nodes_by_kind[kind.index()], 1);
        bump(&self.nodes_created, 1);
        raise(&self.peak_nodes, live_nodes);
    }

    /// Records a disposal. Called only when the node was actually live, so the by-kind gauge
    /// cannot go negative on a repeated dispose.
    pub(crate) fn node_disposed(&self, kind: NodeKind) {
        drop_by(&self.live_nodes_by_kind[kind.index()], 1);
        bump(&self.nodes_disposed, 1);
    }

    /// Records one newly recorded dependency edge.
    pub(crate) fn edge_added(&self) {
        let live = self.live_edges.get() + 1;
        self.live_edges.set(live);
        bump(&self.edges_added, 1);
        raise(&self.peak_edges, live);
    }

    /// Records `count` edges retracted at once, as observer teardown does.
    pub(crate) fn edges_removed(&self, count: usize) {
        drop_by(&self.live_edges, count);
        bump(&self.edges_removed, count as u64);
    }

    /// Records an effect acquiring a pending run.
    pub(crate) fn effect_queued(&self) {
        bump(&self.queued_effects, 1);
    }

    /// Records a pending run being executed or discarded.
    pub(crate) fn effect_unqueued(&self) {
        drop_by(&self.queued_effects, 1);
    }

    /// Records a flush opening.
    pub(crate) fn flush_opened(&self) {
        bump(&self.flushes, 1);
    }

    /// Records the queue depth after a job was pushed.
    pub(crate) fn job_queued(&self, pending_jobs: usize) {
        raise(&self.peak_pending_jobs, pending_jobs);
    }

    /// Assembles the public snapshot from the maintained counters plus the `O(1)` lengths the
    /// caller reads out of the reactor's own state.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn snapshot(
        &self,
        reactor: ReactorId,
        live_nodes: usize,
        observed_nodes: usize,
        pending_jobs: usize,
        flush_depth: u32,
        flush_epoch: u64,
    ) -> GraphStats {
        GraphStats {
            reactor,
            live_nodes,
            live_edges: self.live_edges.get(),
            observed_nodes,
            queued_effects: self.queued_effects.get(),
            pending_jobs,
            flush_depth,
            flush_epoch,
            peak_nodes: self.peak_nodes.get(),
            peak_edges: self.peak_edges.get(),
            peak_pending_jobs: self.peak_pending_jobs.get(),
            nodes_created: self.nodes_created.get(),
            nodes_disposed: self.nodes_disposed.get(),
            edges_added: self.edges_added.get(),
            edges_removed: self.edges_removed.get(),
            flushes: self.flushes.get(),
            live_nodes_by_kind: core::array::from_fn(|i| self.live_nodes_by_kind[i].get()),
        }
    }
}

fn bump<T: Counter>(cell: &Cell<T>, by: T) {
    cell.set(cell.get().saturating_add(by));
}

/// Saturating, so a gauge can never wrap into a nonsense reading if an accounting path is ever
/// missed. A stuck-at-zero gauge is a visible bug; a gauge reading `usize::MAX` looks like a leak.
fn drop_by<T: Counter>(cell: &Cell<T>, by: T) {
    cell.set(cell.get().saturating_sub(by));
}

fn raise(cell: &Cell<usize>, value: usize) {
    if value > cell.get() {
        cell.set(value);
    }
}

/// Saturating arithmetic over the two counter widths, so `bump`/`drop_by` stay generic.
pub(crate) trait Counter: Copy {
    fn saturating_add(self, other: Self) -> Self;
    fn saturating_sub(self, other: Self) -> Self;
}

impl Counter for usize {
    fn saturating_add(self, other: Self) -> Self {
        usize::saturating_add(self, other)
    }
    fn saturating_sub(self, other: Self) -> Self {
        usize::saturating_sub(self, other)
    }
}

impl Counter for u64 {
    fn saturating_add(self, other: Self) -> Self {
        u64::saturating_add(self, other)
    }
    fn saturating_sub(self, other: Self) -> Self {
        u64::saturating_sub(self, other)
    }
}

#[cfg(test)]
mod tests {
    use alloc::rc::Rc;
    use core::cell::RefCell;

    use crate::{EffectRun, NodeKind, Reactor, memo_in, signal_in, source_in};

    /// Asserts the maintained edge counter against a walk of both indexes.
    ///
    /// This is the assertion the "always maintained" contract rests on: a counter nobody checks
    /// drifts, and a drifted leak gauge is worse than no gauge.
    #[track_caller]
    fn assert_edges_consistent(reactor: &Reactor) {
        let (outgoing, incoming) = reactor.walk_edge_counts();
        let stats = reactor.graph_stats();
        assert_eq!(outgoing, incoming, "the two indexes disagree");
        assert_eq!(
            stats.live_edges, outgoing,
            "the maintained edge counter drifted from the graph"
        );
        assert_eq!(
            stats.edges_added - stats.edges_removed,
            stats.live_edges as u64,
            "cumulative edge counters do not reconcile to the live count"
        );
        assert_eq!(
            stats.nodes_created - stats.nodes_disposed,
            stats.live_nodes as u64
        );
    }

    #[test]
    fn counters_survive_a_workload_with_churn_and_disposal() {
        let reactor = Reactor::new();
        assert_edges_consistent(&reactor);

        let toggle = signal_in(&reactor, true);
        let left = signal_in(&reactor, 1_u32);
        let right = signal_in(&reactor, 2_u32);

        // A memo whose dependency *set* changes between runs, so edges are genuinely retracted
        // and re-recorded rather than only accumulating.
        let chosen = memo_in(&reactor, {
            let toggle = toggle.clone();
            let left = left.clone();
            let right = right.clone();
            move || {
                if toggle.get() {
                    left.get()
                } else {
                    right.get()
                }
            }
        });
        let seen = Rc::new(RefCell::new(Vec::new()));
        let effect = reactor.effect({
            let chosen = chosen.clone();
            let seen = Rc::clone(&seen);
            move || seen.borrow_mut().push(chosen.get())
        });
        reactor.flush_now();

        let stats = reactor.graph_stats();
        assert_eq!(stats.live_nodes, 5);
        assert_eq!(stats.live_nodes_of_kind(NodeKind::Signal), 3);
        assert_eq!(stats.live_nodes_of_kind(NodeKind::Memo), 1);
        assert_eq!(stats.live_nodes_of_kind(NodeKind::Effect), 1);
        // toggle→memo, left→memo, memo→effect. `right` is not read on this branch.
        assert_eq!(stats.live_edges, 3);
        assert_eq!(stats.observed_nodes, 3);
        assert_edges_consistent(&reactor);

        toggle.set(false);
        reactor.flush_now();
        assert_eq!(*seen.borrow(), [1, 2]);

        let after = reactor.graph_stats();
        assert_eq!(after.live_edges, 3, "one input swapped for another");
        assert!(
            after.edges_added > stats.edges_added,
            "re-running re-records the edge set, and the churn is visible"
        );
        assert!(after.edges_removed > stats.edges_removed);
        assert_edges_consistent(&reactor);

        // Peaks never retreat.
        assert!(after.peak_nodes >= after.live_nodes);
        assert!(after.peak_edges >= after.live_edges);

        // Disposal unhooks the effect from the graph but does not release what its closure
        // captured — the effect's body holds a `Memo` clone, so the memo and its inputs stay
        // live even after the local handle is dropped. This is exactly the retention shape a
        // leak assertion exists to catch, so assert it rather than assume it away.
        effect.dispose();
        drop(chosen);
        assert_edges_consistent(&reactor);
        let disposed = reactor.graph_stats();
        assert_eq!(disposed.live_edges, 2, "the memo still reads its inputs");
        assert_eq!(disposed.live_nodes_of_kind(NodeKind::Memo), 1);
        assert_eq!(
            disposed.live_nodes_of_kind(NodeKind::Effect),
            0,
            "the effect's own node leaves the graph immediately; what outlives it is the Rust \
             value holding its closure, and that is the distinction a leak report needs"
        );

        // Dropping the handle drops the closure, and the graph finally lets go.
        drop(effect);
        assert_edges_consistent(&reactor);
        let torn_down = reactor.graph_stats();
        assert_eq!(torn_down.live_edges, 0);
        assert_eq!(torn_down.observed_nodes, 0);
        assert_eq!(torn_down.live_nodes, 3, "the three signals are still held");
        assert_eq!(torn_down.live_nodes_of_kind(NodeKind::Memo), 0);
        assert_eq!(torn_down.live_nodes_of_kind(NodeKind::Effect), 0);
        assert_eq!(torn_down.peak_nodes, 5, "but the peak remembers");
    }

    #[test]
    fn queued_effects_follows_the_pending_run_latch() {
        let reactor = Reactor::new();
        let value = signal_in(&reactor, 0_u32);
        let lane = Rc::new(RefCell::new(Vec::<EffectRun>::new()));

        let effect = reactor.effect_with(
            {
                let lane = Rc::clone(&lane);
                move |ready| lane.borrow_mut().push(ready)
            },
            {
                let value = value.clone();
                move || {
                    let _ = value.get();
                }
            },
        );
        assert_eq!(reactor.graph_stats().queued_effects, 1, "the initial run");

        // Repeat invalidations coalesce into the one pending run, and so does the gauge.
        value.set(1);
        value.set(2);
        assert_eq!(reactor.graph_stats().queued_effects, 1);

        reactor.external_flush(|| {
            for run in core::mem::take(&mut *lane.borrow_mut()) {
                run.run();
            }
        });
        assert_eq!(reactor.graph_stats().queued_effects, 0);

        // A discarded run releases the latch too, or the effect would be starved and the gauge
        // would report a run that is never coming.
        value.set(3);
        assert_eq!(reactor.graph_stats().queued_effects, 1);
        lane.borrow_mut().clear();
        assert_eq!(reactor.graph_stats().queued_effects, 0);

        effect.dispose();
    }

    #[test]
    fn a_settled_graph_reports_no_pending_work() {
        let reactor = Reactor::new();
        let value = signal_in(&reactor, 0_u32);
        let effect = reactor.effect({
            let value = value.clone();
            move || {
                let _ = value.get();
            }
        });
        reactor.flush_now();

        // "Idle is idle" as an assertion rather than as a CPU percentage.
        let idle = reactor.graph_stats();
        assert_eq!(idle.queued_effects, 0);
        assert_eq!(idle.pending_jobs, 0);
        assert_eq!(idle.flush_depth, 0);

        // A write that changes nothing still costs a flush, but leaves nothing behind.
        let flushes_before = idle.flushes;
        value.set(0);
        reactor.flush_now();
        let after = reactor.graph_stats();
        assert!(after.flushes > flushes_before);
        assert_eq!(after.queued_effects, 0);
        assert_eq!(after.pending_jobs, 0);

        effect.dispose();
    }

    #[test]
    fn an_unobserved_source_is_counted_as_a_node_but_not_as_an_edge() {
        let reactor = Reactor::new();
        let node = source_in(&reactor);

        let stats = reactor.graph_stats();
        assert_eq!(stats.live_nodes, 1);
        assert_eq!(stats.live_nodes_of_kind(NodeKind::Source), 1);
        assert_eq!(stats.live_edges, 0);
        assert_eq!(stats.observed_nodes, 0);

        drop(node);
        let after = reactor.graph_stats();
        assert_eq!(after.live_nodes, 0);
        assert_eq!(after.nodes_disposed, 1);
    }
}
