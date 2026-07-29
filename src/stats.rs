//! Graph accounting: what a reactor is holding, and how much of it it has held.
//!
//! The diagnostic event stream explains *causality* — which write reached which effect. It does
//! not quantify the graph, and quantifying it by walking is exactly what a consumer cannot afford
//! to do every frame. This module maintains the counts as the graph changes, so a snapshot is a
//! handful of loads.

use core::cell::{Cell, RefCell};

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

/// What one flush did.
///
/// Delivered on [`DiagnosticEvent::FlushFinished`](crate::DiagnosticEvent::FlushFinished). An
/// idle window's flush should be all zeroes, and that is the point: without this, "a flush
/// happened" and "a flush did work" are indistinguishable, and an idle application's cost is a
/// CPU percentage rather than a number.
///
/// # Availability
///
/// Unlike [`GraphStats`], these counters are maintained **only while a diagnostic subscription is
/// active**. The distinction is not arbitrary: `GraphStats` backs a query that can be called at
/// any moment, so it must always be true, whereas `FlushStats` is only ever observed by being
/// delivered in an event, and an event nobody subscribed to is not delivered. Counters that back
/// a query are always maintained; counters that back an event follow the event.
///
/// # Attribution
///
/// Work is attributed to **the next flush that closes**, and counted exactly once.
///
/// - Work performed during a flush belongs to the innermost flush open at the time. Flushes nest
///   — a re-entrant [`Reactor::flush_now`](crate::Reactor::flush_now) from inside a job opens a
///   genuine inner epoch — and an inner flush's totals are *not* rolled up into the enclosing
///   one, so summing the flushes in a capture double-counts nothing.
/// - Work performed outside any flush — most importantly the writes that scheduled it — is
///   handed to the flush that drains it. A write and the effect run it causes therefore appear
///   in the same totals, which is what makes `root_writes` answer "what set this flush off".
///   The corollary: work never followed by a flush is never reported. Disposing an effect and
///   then stopping, for example, accumulates a disposal that no flush arrives to carry. That is
///   deliberate — a drain with an empty queue is not a flush, and making one happen because
///   diagnostics are subscribed would break the rule that subscribing never changes behaviour.
///   In an application, where flushes keep coming, it is invisible.
///
/// # A settled graph does not flush
///
/// A drain with nothing to drain is not a flush: it opens no epoch and reports nothing. So the
/// signature of an idle application is **no flushes at all**, not a stream of empty ones. An
/// empty `FlushStats` still occurs for a boundary a consumer declared with
/// [`Reactor::external_flush`](crate::Reactor::external_flush), which is reported whether or not
/// the drain found work — see `examples/idle_audit.rs`.
///
/// # No durations
///
/// Adaptite reads no clock. Every field here is a count. A consumer that wants a duration
/// timestamps the `FlushStarted`/`FlushFinished` pair itself.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FlushStats {
    /// Writes and explicit triggers on source nodes that reached the graph.
    pub root_writes: u32,
    /// Writes discarded by a source's own equality check before reaching the graph.
    ///
    /// Work that happened and produced nothing. `root_writes + writes_suppressed` is how often
    /// something tried to write; `root_writes` alone is how often it mattered. A large ratio
    /// between them is a producer running more often than it needs to, which is invisible from
    /// the propagation side because a suppressed write propagates nothing by definition.
    pub writes_suppressed: u32,
    /// Marks delivered to observers saying a computed input *may* have changed.
    pub nodes_marked_check: u32,
    /// Marks delivered to observers saying a direct dependency definitely changed.
    pub nodes_marked_dirty: u32,
    /// Deepest chain of marking walked in one propagation.
    ///
    /// One write reaching an effect directly is depth 1; through two memos, depth 3.
    pub max_propagation_depth: u32,
    /// Effects that acquired a pending run.
    pub effects_queued: u32,
    /// Invalidations absorbed by an effect that already had a run pending.
    pub effects_coalesced: u32,
    /// Effect bodies executed.
    pub effects_run: u32,
    /// Effects whose verification proved their inputs unchanged, so the body was skipped.
    pub effects_skipped: u32,
    /// Effects disposed.
    pub effects_disposed: u32,
    /// Effects still holding a pending run when the flush closed.
    pub effects_pending: u32,
    /// Check-marked computed nodes that verified their inputs.
    pub computed_verified: u32,
    /// Computations that ran.
    ///
    /// `computed_changed + computed_suppressed` is **at most** this, not equal to it: a
    /// computation that unwound published nothing and is neither. The difference is the number
    /// that failed.
    pub computed_recomputed: u32,
    /// Computations that published a new value.
    pub computed_changed: u32,
    /// Computations whose comparator judged the value unchanged, sparing everything downstream.
    pub computed_suppressed: u32,
    /// Dependency edges recorded.
    pub edges_added: u32,
    /// Dependency edges retracted.
    pub edges_removed: u32,
    /// Jobs queued when the flush opened.
    pub jobs_at_start: u32,
    /// Jobs still queued when the flush closed.
    ///
    /// Non-zero after a panicking job: the flush hands what is left to a fresh one.
    pub jobs_at_finish: u32,
}

impl FlushStats {
    /// Returns `true` when the flush did no reactive work at all.
    ///
    /// A settled graph does not flush at all, so an idle application usually asserts that no
    /// flush arrived. This is for the flush an [`Reactor::external_flush`](crate::Reactor::external_flush)
    /// reports over a settled graph — the boundary was declared, but there was nothing to do.
    ///
    /// Deliberately ignores [`jobs_at_start`](Self::jobs_at_start),
    /// [`jobs_at_finish`](Self::jobs_at_finish) and [`effects_pending`](Self::effects_pending),
    /// which describe outstanding state rather than work this flush did. An empty flush with a
    /// non-zero `effects_pending` is a real and meaningful combination: nothing happened here,
    /// and an effect is still sitting unrun in a consumer-owned lane.
    pub fn is_empty(&self) -> bool {
        self.root_writes == 0
            && self.writes_suppressed == 0
            && self.nodes_marked_check == 0
            && self.nodes_marked_dirty == 0
            && self.effects_queued == 0
            && self.effects_coalesced == 0
            && self.effects_run == 0
            && self.effects_skipped == 0
            && self.effects_disposed == 0
            && self.computed_verified == 0
            && self.computed_recomputed == 0
            && self.edges_added == 0
            && self.edges_removed == 0
    }
}

/// Accumulates [`FlushStats`] and decides which flush each unit of work belongs to.
///
/// `pending` holds work performed outside any flush; opening the outermost flush takes it, so the
/// writes that scheduled a flush are counted in it. `open` is the stack of flushes in progress,
/// and work always lands on its top, which is what keeps a nested flush's totals out of its
/// parent's.
#[derive(Default)]
pub(crate) struct FlushAccounting {
    pending: RefCell<FlushStats>,
    open: RefCell<Vec<FlushStats>>,
}

impl FlushAccounting {
    pub(crate) fn record(&self, f: impl FnOnce(&mut FlushStats)) {
        let mut open = self.open.borrow_mut();
        match open.last_mut() {
            Some(stats) => f(stats),
            None => f(&mut self.pending.borrow_mut()),
        }
    }

    pub(crate) fn open_flush(&self, jobs_at_start: usize) {
        // Taking `pending` hands the writes that scheduled this flush to the flush itself. Inside
        // a flush `pending` is always empty, so a nested open takes nothing and starts clean.
        let mut stats = core::mem::take(&mut *self.pending.borrow_mut());
        stats.jobs_at_start = saturating_u32(jobs_at_start);
        self.open.borrow_mut().push(stats);
    }

    pub(crate) fn close_flush(
        &self,
        jobs_at_finish: usize,
        effects_pending: usize,
    ) -> Option<FlushStats> {
        // `None` when a subscription arrived mid-flush, so there is no slot to close.
        let mut stats = self.open.borrow_mut().pop()?;
        stats.jobs_at_finish = saturating_u32(jobs_at_finish);
        stats.effects_pending = saturating_u32(effects_pending);
        Some(stats)
    }

    /// Drops everything accumulated so far. Called when the last subscription goes away, so a
    /// later subscriber does not inherit totals from a window it could not see.
    pub(crate) fn reset(&self) {
        *self.pending.borrow_mut() = FlushStats::default();
        self.open.borrow_mut().clear();
    }
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
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
        add(&self.live_nodes_by_kind[kind.index()], 1);
        tick(&self.nodes_created, 1);
        raise(&self.peak_nodes, live_nodes);
    }

    /// Records a disposal. Called only when the node was actually live, so the by-kind gauge
    /// cannot go negative on a repeated dispose.
    pub(crate) fn node_disposed(&self, kind: NodeKind) {
        sub(&self.live_nodes_by_kind[kind.index()], 1);
        tick(&self.nodes_disposed, 1);
    }

    /// Records one newly recorded dependency edge.
    pub(crate) fn edge_added(&self) {
        let live = self.live_edges.get() + 1;
        self.live_edges.set(live);
        tick(&self.edges_added, 1);
        raise(&self.peak_edges, live);
    }

    /// Records `count` edges retracted at once, as observer teardown does.
    pub(crate) fn edges_removed(&self, count: usize) {
        sub(&self.live_edges, count);
        tick(&self.edges_removed, count as u64);
    }

    /// Records an effect acquiring a pending run.
    pub(crate) fn effect_queued(&self) {
        add(&self.queued_effects, 1);
    }

    /// Records a pending run being executed or discarded.
    pub(crate) fn effect_unqueued(&self) {
        sub(&self.queued_effects, 1);
    }

    /// Effects currently holding a pending run.
    pub(crate) fn queued_effects(&self) -> usize {
        self.queued_effects.get()
    }

    /// Records a flush opening.
    pub(crate) fn flush_opened(&self) {
        tick(&self.flushes, 1);
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

/// Adds to a live gauge.
///
/// Saturating, like every counter helper here, so a gauge can never wrap into a nonsense reading
/// if an accounting path is ever missed. A stuck-at-zero gauge is a visible bug; a gauge reading
/// `usize::MAX` looks like a leak.
fn add(cell: &Cell<usize>, n: usize) {
    cell.set(cell.get().saturating_add(n));
}

/// Subtracts from a live gauge.
fn sub(cell: &Cell<usize>, n: usize) {
    cell.set(cell.get().saturating_sub(n));
}

/// Advances a cumulative total, which only ever grows.
fn tick(cell: &Cell<u64>, n: u64) {
    cell.set(cell.get().saturating_add(n));
}

/// Raises a high-water mark.
fn raise(cell: &Cell<usize>, value: usize) {
    if value > cell.get() {
        cell.set(value);
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
            stats.observed_nodes,
            reactor.walk_observed_nodes(),
            "the maintained observed-node counter drifted from the graph"
        );
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
    fn dropping_a_latched_effect_releases_the_queued_gauge() {
        // The latch is normally released by the pending run, or by the `EffectRun` being dropped
        // — but both reach the effect through a `Weak`. An effect dropped before its first run
        // (an unowned handle going out of scope is the ordinary way) made both upgrades fail, so
        // nothing ever decremented the gauge and it climbed without bound.
        let reactor = Reactor::new();
        let baseline = reactor.graph_stats().queued_effects;

        for _ in 0..100 {
            let _effect = reactor.effect(|| {});
        }
        reactor.flush_now();

        let stats = reactor.graph_stats();
        assert_eq!(
            stats.queued_effects, baseline,
            "an effect dropped while latched must not leave a run pending forever"
        );
        assert_eq!(stats.live_nodes, 0, "and its node must be gone too");
    }

    #[test]
    fn disposal_unhooks_the_node_even_when_a_cleanup_panics() {
        use std::panic::{AssertUnwindSafe, catch_unwind};

        // Teardown of the owner frame is total, but disposal used to stop one level short: the
        // graph-side unhook sat after `owner.dispose()` unprotected, so a panicking cleanup left
        // the effect's node metadata and every edge it recorded in the reactor for good — with
        // `is_disposed()` reporting true. The gauges made it visible; nothing made it not happen.
        let reactor = Reactor::new();
        let value = signal_in(&reactor, 0_u32);
        let effect = reactor.effect({
            let value = value.clone();
            move || {
                let _ = value.get();
                crate::on_cleanup(|| panic!("cleanup failed"));
            }
        });
        reactor.flush_now();
        assert_eq!(reactor.graph_stats().live_nodes, 2);

        let result = catch_unwind(AssertUnwindSafe(|| effect.dispose()));
        assert!(
            result.is_err(),
            "the cleanup panic still reaches the caller"
        );

        let stats = reactor.graph_stats();
        assert_eq!(
            stats.live_nodes_of_kind(NodeKind::Effect),
            0,
            "the effect's node must leave the graph even though teardown failed"
        );
        assert_eq!(stats.live_edges, 0, "and so must the edges it recorded");
        assert_eq!(reactor.observer_count(value.id()), 0);
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

        // `set` compares before writing, so an unchanged value never reaches the graph and
        // cannot cost a flush.
        let flushes_before = idle.flushes;
        value.set(0);
        reactor.flush_now();
        assert_eq!(
            reactor.graph_stats().flushes,
            flushes_before,
            "an equal `set` is suppressed at the signal, before the graph hears about it"
        );

        // A real write costs exactly one flush and leaves nothing behind.
        value.set(1);
        reactor.flush_now();
        let after = reactor.graph_stats();
        assert_eq!(after.flushes, flushes_before + 1);
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
