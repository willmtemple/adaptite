use alloc::boxed::Box;
use core::panic::Location;

use crate::NodeId;
use crate::stats::FlushStats;

/// Stable identifier for one reactive graph during the process lifetime.
///
/// [`NodeId`] values are unique only within a reactor. Diagnostic consumers
/// must use `(ReactorId, NodeId)` when aggregating several graphs.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReactorId(pub(crate) u64);

impl ReactorId {
    /// Returns the process-local numeric identifier.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for ReactorId {
    /// Matches [`NodeId`]'s formatting, so the `(reactor, node)` pair every diagnostic is scoped
    /// by can be printed without reaching for [`get`](Self::get) on one half of it.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The primitive a reactive node was created as.
///
/// The kind is **declared at construction, not inferred from behaviour**. A primitive built on
/// [`crate::source`] reports [`Source`](NodeKind::Source), because a raw source is exactly what
/// the graph was handed; a [`crate::Writable`] is a memo bundled with a setter and reports
/// [`Memo`](NodeKind::Memo).
///
/// Composite primitives contribute no kind of their *own*, but they do contribute nodes: a
/// [`crate::Resource`] allocates five — three signals (value, loading, refetch tick), the memo
/// that gates refetching on the fetch input actually changing, and the effect that drives the
/// fetch — and [`crate::watch`] two, a memo and an effect. Each is counted individually under
/// the kind it was allocated as, so a consumer budgeting by composite must budget five per
/// resource and will see memos it did not write. Read this as "what adaptite was asked to
/// allocate", not as a claim about what the consumer built with it.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum NodeKind {
    /// A bare dependency node with no value of its own ([`crate::Source`]).
    Source,
    /// A mutable value ([`crate::Signal`]).
    Signal,
    /// A multicast value stream ([`crate::Event`]).
    Event,
    /// A cached computation that invalidates on every recomputation ([`crate::Thunk`]).
    Thunk,
    /// A cached computation whose comparator can suppress propagation ([`crate::Memo`]).
    Memo,
    /// A scheduled side effect ([`crate::EffectHandle`]).
    Effect,
}

impl NodeKind {
    /// Number of kinds currently defined.
    ///
    /// Deliberately not public: the enum is `#[non_exhaustive]`, and a public count would make
    /// adding a kind a breaking change for anyone who sized an array by it.
    pub(crate) const COUNT: usize = 6;

    /// Returns every kind, so a consumer can iterate them.
    ///
    /// [`NodeKind`] is `#[non_exhaustive]`, which stops a downstream crate matching it
    /// exhaustively — without this, the only way to write "break these counts down by kind" is to
    /// hardcode the variants, and that silently under-reports the day a kind is added. Iterate
    /// this instead and a new kind appears on its own.
    pub fn all() -> impl ExactSizeIterator<Item = Self> + Clone {
        [
            Self::Source,
            Self::Signal,
            Self::Event,
            Self::Thunk,
            Self::Memo,
            Self::Effect,
        ]
        .into_iter()
    }

    /// Dense index into the per-kind counter arrays.
    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Source => 0,
            Self::Signal => 1,
            Self::Event => 2,
            Self::Thunk => 3,
            Self::Memo => 4,
            Self::Effect => 5,
        }
    }
}

/// Root mutation that caused reactive invalidation.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidationCause {
    /// Source node whose version changed.
    pub node: NodeId,
    /// Source node version after the write.
    pub version: u64,
    /// Location at which the source node was created.
    pub node_origin: &'static Location<'static>,
    /// Location of the write or explicit trigger.
    pub write_origin: &'static Location<'static>,
}

/// How a computed node's recomputation ended.
///
/// A dependency cycle discovered during a computation surfaces as [`Panicked`](Self::Panicked),
/// because that is what it is — the cycle check panics with a [`crate::ReactCycleError`] message
/// naming the path, and it unwinds through the computation like any other panic. The enum is
/// `#[non_exhaustive]` so a distinguishable outcome can be added without a break; variants that
/// adaptite cannot actually produce are deliberately absent.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComputeOutcome {
    /// The computation returned a value.
    Completed,
    /// The computation unwound. The node keeps its stale mark and the next read retries.
    Panicked,
}

/// Strength of an invalidation propagated to an observer.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidationLevel {
    /// A computed dependency may have changed and must be verified.
    Check,
    /// A direct dependency definitely changed.
    Dirty,
}

/// Opt-in event stream for explaining reactive scheduling.
///
/// Events are delivered synchronously on the reactor thread. A callback must
/// not mutate the same reactive graph or add/remove diagnostic subscriptions;
/// it should copy the fields it needs into an external trace sink.
///
/// Both the enum and every variant are `#[non_exhaustive]`, so a `match` needs a
/// wildcard arm *and* each variant pattern needs a trailing `..`. Adding a variant and
/// adding a field to an existing variant are then both additive changes, which matters
/// because these payloads grow as the graph learns to report more about itself.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticEvent {
    /// A reactive node was allocated.
    #[non_exhaustive]
    NodeCreated {
        /// Graph the node joined.
        reactor: ReactorId,
        /// The new node.
        node: NodeId,
        /// Primitive the node was created as.
        kind: NodeKind,
        /// Location at which the node was created.
        origin: &'static Location<'static>,
    },
    /// A reactive node was removed from the graph and will never participate again.
    ///
    /// Delivered exactly once per node, even though disposal is idempotent: a node that is
    /// already gone produces nothing. For an effect this accompanies
    /// [`EffectDisposed`](Self::EffectDisposed), which reports the effect's own lifecycle
    /// transition; this one reports the graph bookkeeping being torn down.
    #[non_exhaustive]
    NodeDisposed {
        /// Graph the node left.
        reactor: ReactorId,
        /// The disposed node.
        node: NodeId,
        /// Primitive the node was created as.
        kind: NodeKind,
        /// Location at which the node was created.
        origin: &'static Location<'static>,
        /// Edges this node recorded on its inputs at the moment of disposal.
        dependencies: usize,
        /// Observers still recording an edge on this node at the moment of disposal.
        observers: usize,
    },
    /// A write to a source node was suppressed because the value had not changed.
    ///
    /// The write never reached the graph — no version bump, no propagation, no flush — so it does
    /// **not** also appear as a [`ReactiveWrite`](Self::ReactiveWrite). It is reported anyway
    /// because the producer still ran: something computed a value and threw it away, and a
    /// producer running far more often than it publishes is invisible from the propagation side
    /// by construction. That gap is the difference between "this signal changed 14 times" and
    /// "the thing writing it ran 80 times", and only the second says to slow the producer down.
    ///
    /// Suppression here is the *source's* equality check. A memo whose comparator suppresses
    /// propagation reports [`ComputedRecomputeFinished`](Self::ComputedRecomputeFinished) with
    /// `changed: false` instead.
    #[non_exhaustive]
    WriteSuppressed {
        /// Graph containing the node.
        reactor: ReactorId,
        /// Node that was written.
        node: NodeId,
        /// Primitive the node was created as.
        kind: NodeKind,
        /// Location at which the node was created.
        node_origin: &'static Location<'static>,
        /// Location of the write that was discarded — the site worth attributing this to.
        write_origin: &'static Location<'static>,
    },
    /// A source node changed.
    #[non_exhaustive]
    ReactiveWrite {
        /// Graph containing the node.
        reactor: ReactorId,
        /// Primitive the written node was created as. Present here as well as on
        /// [`WriteSuppressed`](Self::WriteSuppressed) so writes can be broken down by kind
        /// whether or not they reached the graph.
        kind: NodeKind,
        /// Root mutation and its source locations.
        cause: InvalidationCause,
    },
    /// A root mutation reached a computed node.
    ///
    /// Emitted for every mark delivered to the node, including one that coalesces into staleness
    /// it already had — the event reports propagation *reaching* the node, which is what makes
    /// the path from a write to an effect visible rather than only its endpoints. Use
    /// [`state_changed`](Self::ComputedInvalidated::state_changed) to tell the two apart: a run of
    /// deliveries that mostly do not change state is propagation amplification, and it is exactly
    /// what coalesced writes look like from inside the graph.
    #[non_exhaustive]
    ComputedInvalidated {
        /// Graph containing the node.
        reactor: ReactorId,
        /// Invalidated computed node.
        node: NodeId,
        /// Whether the node is a thunk or a memo.
        kind: NodeKind,
        /// Location at which the node was created.
        node_origin: &'static Location<'static>,
        /// Root mutation responsible for this invalidation.
        cause: InvalidationCause,
        /// Whether the node is definitely dirty or must verify its own inputs.
        level: InvalidationLevel,
        /// Flush in progress when the mark was delivered, or the most recent one. Marking can
        /// happen outside any flush — a write from a task does exactly that.
        flush_epoch: u64,
        /// Whether this mark actually made the node staler.
        ///
        /// `false` means the node was already at least this stale and the mark coalesced into
        /// what it had — the propagation reached the node and changed nothing. Marks that do not
        /// change state also stop here: nothing is forwarded to *this* node's dependents.
        state_changed: bool,
    },
    /// A check-marked computed node verified its inputs.
    ///
    /// This is the event that distinguishes a verification resolved from cache from one that
    /// forced work: `recomputed` is `false` when every input turned out to be unchanged, so the
    /// node returned to clean without running its computation. Nodes that were definitely dirty
    /// do not verify and so do not appear here — they go straight to a recomputation.
    #[non_exhaustive]
    ComputedVerified {
        /// Graph containing the node.
        reactor: ReactorId,
        /// Verified computed node.
        node: NodeId,
        /// Whether the node is a thunk or a memo.
        kind: NodeKind,
        /// Location at which the node was created.
        node_origin: &'static Location<'static>,
        /// Flush that performed the verification, or the most recent one.
        flush_epoch: u64,
        /// Whether verification found a changed input and forced a recomputation.
        recomputed: bool,
    },
    /// A computed node's computation is about to run.
    #[non_exhaustive]
    ComputedRecomputeStarted {
        /// Graph containing the node.
        reactor: ReactorId,
        /// Recomputing node.
        node: NodeId,
        /// Whether the node is a thunk or a memo.
        kind: NodeKind,
        /// Location at which the node was created.
        node_origin: &'static Location<'static>,
        /// Flush the recomputation belongs to, or the most recent one.
        flush_epoch: u64,
        /// Dependencies recorded by the previous run.
        dependencies_before: usize,
    },
    /// A computed node's computation returned or unwound.
    ///
    /// Always paired with a [`ComputedRecomputeStarted`](Self::ComputedRecomputeStarted), including
    /// on the unwind path, where `outcome` is [`ComputeOutcome::Panicked`].
    ///
    /// Comparing `dependencies_before` with `dependencies_after` detects a computation whose read
    /// set **changes size** — one that grows every run is the shape behind a component that gets
    /// slower the longer it lives.
    ///
    /// It does **not** detect a read set of constant size whose *members* change: swapping 200
    /// dependencies for 200 different ones reports 200 before and 200 after. Nor do the per-flush
    /// `edges_added`/`edges_removed` totals, because every recomputation clears and re-records its
    /// whole edge set, so churn and stability look identical there. For that question, sample
    /// [`Reactor::dependencies_of`](crate::Reactor::dependencies_of) either side of a
    /// recomputation, or diff two [`Reactor::graph_snapshot`](crate::Reactor::graph_snapshot) snapshots
    /// — both are targeted-investigation tools rather than something to run per frame.
    ///
    /// Adaptite deliberately does not report individual edge additions and removals: edge
    /// recording is the hottest path in the graph — one call per tracked read — and a wide node
    /// would emit more diagnostic events than it does reactive work.
    #[non_exhaustive]
    ComputedRecomputeFinished {
        /// Graph containing the node.
        reactor: ReactorId,
        /// Recomputed node.
        node: NodeId,
        /// Whether the node is a thunk or a memo.
        kind: NodeKind,
        /// Location at which the node was created.
        node_origin: &'static Location<'static>,
        /// Flush the recomputation belonged to.
        flush_epoch: u64,
        /// Dependencies recorded by this run.
        dependencies_after: usize,
        /// Whether the new value propagated.
        ///
        /// `false` means a memo's comparator judged the value unchanged and suppressed
        /// propagation, so downstream observers were spared. A thunk has no comparator and always
        /// reports `true`. Always `false` when `outcome` is not
        /// [`Completed`](ComputeOutcome::Completed), since nothing was published.
        changed: bool,
        /// How the computation ended.
        outcome: ComputeOutcome,
    },
    /// A root mutation reached an effect, directly or through computed nodes.
    #[non_exhaustive]
    EffectInvalidated {
        /// Graph containing the effect.
        reactor: ReactorId,
        /// Invalidated effect.
        effect: NodeId,
        /// Location at which the effect was created.
        effect_origin: &'static Location<'static>,
        /// Root mutation responsible for this invalidation.
        cause: InvalidationCause,
        /// Whether the effect is definitely dirty or must verify computed inputs.
        level: InvalidationLevel,
    },
    /// An attempt to place an effect in the next reactive flush.
    #[non_exhaustive]
    EffectScheduled {
        /// Graph containing the effect.
        reactor: ReactorId,
        /// Scheduled effect.
        effect: NodeId,
        /// Location at which the effect was created.
        effect_origin: &'static Location<'static>,
        /// `true` when a new job was queued; `false` when an existing job
        /// coalesced this request.
        queued: bool,
        /// Current flush epoch. Zero means no flush has run yet.
        flush_epoch: u64,
    },
    /// An effect body is about to execute.
    #[non_exhaustive]
    EffectRunStarted {
        /// Graph containing the effect.
        reactor: ReactorId,
        /// Running effect.
        effect: NodeId,
        /// Location at which the effect was created.
        effect_origin: &'static Location<'static>,
        /// Flush that is executing the effect.
        flush_epoch: u64,
    },
    /// An effect body returned or unwound.
    #[non_exhaustive]
    EffectRunFinished {
        /// Graph containing the effect.
        reactor: ReactorId,
        /// Effect whose execution ended.
        effect: NodeId,
        /// Flush that executed the effect.
        flush_epoch: u64,
    },
    /// Verification proved that a check-marked effect did not need to run.
    #[non_exhaustive]
    EffectRunSkipped {
        /// Graph containing the effect.
        reactor: ReactorId,
        /// Skipped effect.
        effect: NodeId,
        /// Flush that verified the effect.
        flush_epoch: u64,
    },
    /// An effect was disposed and will never run again.
    #[non_exhaustive]
    EffectDisposed {
        /// Graph containing the effect.
        reactor: ReactorId,
        /// Disposed effect.
        effect: NodeId,
    },
    /// A reactor began draining its queued jobs.
    #[non_exhaustive]
    FlushStarted {
        /// Graph being flushed.
        reactor: ReactorId,
        /// Monotonically increasing flush number.
        flush_epoch: u64,
        /// Jobs queued when the flush started.
        pending_jobs: usize,
    },
    /// A reactor stopped draining jobs, including unwind paths.
    #[non_exhaustive]
    FlushFinished {
        /// Graph that was flushed.
        reactor: ReactorId,
        /// Flush number.
        flush_epoch: u64,
        /// Jobs still pending when the flush ended.
        remaining_jobs: usize,
        /// What this flush did. See [`FlushStats`] for how work is attributed when flushes nest.
        stats: FlushStats,
    },
}

impl DiagnosticEvent {
    /// Returns the graph this event describes.
    ///
    /// Every variant carries it, but both the enum and its variants are `#[non_exhaustive]`, so a
    /// downstream crate cannot destructure it generically — without this, reading a field that is
    /// present on all of them means a match arm per variant, re-audited on every release.
    /// Adaptite can match exhaustively because `#[non_exhaustive]` does not bind the defining
    /// crate, so these accessors stay correct as variants are added.
    pub fn reactor(&self) -> ReactorId {
        match self {
            Self::NodeCreated { reactor, .. }
            | Self::NodeDisposed { reactor, .. }
            | Self::WriteSuppressed { reactor, .. }
            | Self::ReactiveWrite { reactor, .. }
            | Self::ComputedInvalidated { reactor, .. }
            | Self::ComputedVerified { reactor, .. }
            | Self::ComputedRecomputeStarted { reactor, .. }
            | Self::ComputedRecomputeFinished { reactor, .. }
            | Self::EffectInvalidated { reactor, .. }
            | Self::EffectScheduled { reactor, .. }
            | Self::EffectRunStarted { reactor, .. }
            | Self::EffectRunFinished { reactor, .. }
            | Self::EffectRunSkipped { reactor, .. }
            | Self::EffectDisposed { reactor, .. }
            | Self::FlushStarted { reactor, .. }
            | Self::FlushFinished { reactor, .. } => *reactor,
        }
    }

    /// Returns the node this event concerns, or `None` for the flush boundaries, which concern
    /// the whole graph rather than one node.
    ///
    /// Pair it with [`reactor`](Self::reactor) to get the `(ReactorId, NodeId)` every diagnostic
    /// payload is scoped by.
    pub fn node(&self) -> Option<NodeId> {
        match self {
            Self::NodeCreated { node, .. }
            | Self::NodeDisposed { node, .. }
            | Self::WriteSuppressed { node, .. }
            | Self::ComputedInvalidated { node, .. }
            | Self::ComputedVerified { node, .. }
            | Self::ComputedRecomputeStarted { node, .. }
            | Self::ComputedRecomputeFinished { node, .. } => Some(*node),
            Self::ReactiveWrite { cause, .. } => Some(cause.node),
            Self::EffectInvalidated { effect, .. }
            | Self::EffectScheduled { effect, .. }
            | Self::EffectRunStarted { effect, .. }
            | Self::EffectRunFinished { effect, .. }
            | Self::EffectRunSkipped { effect, .. }
            | Self::EffectDisposed { effect, .. } => Some(*effect),
            Self::FlushStarted { .. } | Self::FlushFinished { .. } => None,
        }
    }

    /// Returns where the node this event concerns was created, when the event carries it.
    ///
    /// Worth preferring over [`Reactor::node_origin`](crate::Reactor::node_origin) in a trace
    /// sink: that query answers only for *live* nodes, and a sink processing events after the fact
    /// is exactly the case where the node is already gone.
    pub fn node_origin(&self) -> Option<&'static Location<'static>> {
        match self {
            Self::NodeCreated { origin, .. } | Self::NodeDisposed { origin, .. } => Some(origin),
            Self::WriteSuppressed { node_origin, .. }
            | Self::ComputedInvalidated { node_origin, .. }
            | Self::ComputedVerified { node_origin, .. }
            | Self::ComputedRecomputeStarted { node_origin, .. }
            | Self::ComputedRecomputeFinished { node_origin, .. } => Some(node_origin),
            Self::ReactiveWrite { cause, .. } => Some(cause.node_origin),
            Self::EffectInvalidated { effect_origin, .. }
            | Self::EffectScheduled { effect_origin, .. }
            | Self::EffectRunStarted { effect_origin, .. } => Some(effect_origin),
            Self::EffectRunFinished { .. }
            | Self::EffectRunSkipped { .. }
            | Self::EffectDisposed { .. }
            | Self::FlushStarted { .. }
            | Self::FlushFinished { .. } => None,
        }
    }

    /// Returns the flush this event belongs to, when it belongs to one.
    ///
    /// `None` on the events that are not a flush's work: node creation and disposal, writes
    /// (including suppressed ones), and the invalidation a write propagates. A write and the
    /// marks it delivers ordinarily happen *outside* any flush — the flush that drains them has
    /// not opened yet — so there is no flush they belong to. That matches how the aggregate
    /// attributes the same work: [`FlushStats`] hands out-of-flush work to the flush that
    /// *drains* it, which is never the epoch that was current when the write happened.
    ///
    /// [`ComputedInvalidated`](Self::ComputedInvalidated) is the one `None` variant that does
    /// carry a `flush_epoch` field, because a mark can also be delivered from inside a flush and
    /// the field says which one was open. It is deliberately not reported here: read it by
    /// matching the variant, and read
    /// [its field doc](Self::ComputedInvalidated::flush_epoch) first, because outside a flush the
    /// field holds the most recently *opened* flush, which by then has closed — the flush before
    /// the write, not the one that will drain it — so bucketing by it would file the mark under
    /// a flush that had already finished when the write happened.
    ///
    /// The same caveat is why a returned epoch is a weaker guarantee than it looks: every
    /// variant records the epoch of the most recently opened flush, so an event that genuinely
    /// occurred outside a flush — a stale memo pulled directly by a consumer, say, which reports
    /// [`ComputedRecomputeStarted`](Self::ComputedRecomputeStarted) — reports the last flush to
    /// have opened rather than one that contains it. A consumer that needs certainty brackets on
    /// the [`FlushStarted`](Self::FlushStarted)/[`FlushFinished`](Self::FlushFinished) pair, or
    /// checks [`GraphStats::flush_depth`](crate::GraphStats::flush_depth), instead of trusting
    /// the number alone.
    pub fn flush_epoch(&self) -> Option<u64> {
        match self {
            Self::ComputedVerified { flush_epoch, .. }
            | Self::ComputedRecomputeStarted { flush_epoch, .. }
            | Self::ComputedRecomputeFinished { flush_epoch, .. }
            | Self::EffectScheduled { flush_epoch, .. }
            | Self::EffectRunStarted { flush_epoch, .. }
            | Self::EffectRunFinished { flush_epoch, .. }
            | Self::EffectRunSkipped { flush_epoch, .. }
            | Self::FlushStarted { flush_epoch, .. }
            | Self::FlushFinished { flush_epoch, .. } => Some(*flush_epoch),
            Self::NodeCreated { .. }
            | Self::NodeDisposed { .. }
            | Self::WriteSuppressed { .. }
            | Self::ReactiveWrite { .. }
            | Self::ComputedInvalidated { .. }
            | Self::EffectInvalidated { .. }
            | Self::EffectDisposed { .. } => None,
        }
    }
}

/// Keeps a diagnostic callback subscribed to one reactor.
///
/// Dropping the value unsubscribes. Cloning is deliberately unsupported so
/// callback lifetime has one unambiguous owner.
#[must_use = "dropping the subscription disables reactive diagnostics"]
pub struct DiagnosticSubscription {
    unsubscribe: Option<Box<dyn FnOnce()>>,
}

impl DiagnosticSubscription {
    pub(crate) fn new(unsubscribe: impl FnOnce() + 'static) -> Self {
        Self {
            unsubscribe: Some(Box::new(unsubscribe)),
        }
    }
}

impl Drop for DiagnosticSubscription {
    fn drop(&mut self) {
        if let Some(unsubscribe) = self.unsubscribe.take() {
            unsubscribe();
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::rc::Rc;
    use core::cell::RefCell;

    use crate::{
        DiagnosticEvent, InvalidationLevel, Reactor, current, memo_in, signal, signal_in, source_in,
    };

    #[test]
    fn diagnostics_preserve_root_write_through_computed_dependencies() {
        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let source = signal_in(&reactor, 1_u32);
        let doubled = memo_in(&reactor, {
            let source = source.clone();
            move || source.get() * 2
        });
        let _effect = reactor.effect({
            let doubled = doubled.clone();
            move || {
                let _ = doubled.get();
            }
        });
        reactor.flush_now();
        events.borrow_mut().clear();

        source.set(2);
        reactor.flush_now();

        let events = events.borrow();
        let write = events.iter().find_map(|event| match event {
            DiagnosticEvent::ReactiveWrite { cause, .. } => Some(*cause),
            _ => None,
        });
        let write = write.expect("source write should be diagnosed");
        assert!(
            events.iter().any(|event| matches!(
                event,
                DiagnosticEvent::EffectInvalidated {
                    cause,
                    level: InvalidationLevel::Check,
                    ..
                } if *cause == write
            )),
            "the effect should retain the source write through the memo"
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DiagnosticEvent::EffectRunStarted { .. }))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DiagnosticEvent::EffectRunFinished { .. }))
        );
    }

    #[test]
    fn effect_schedule_events_report_coalesced_direct_writes() {
        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });
        let first = signal_in(&reactor, 0_u32);
        let second = signal_in(&reactor, 0_u32);
        let _effect = reactor.effect({
            let first = first.clone();
            let second = second.clone();
            move || {
                let _ = (first.get(), second.get());
            }
        });
        reactor.flush_now();
        events.borrow_mut().clear();

        first.set(1);
        second.set(1);

        let queued = events
            .borrow()
            .iter()
            .filter_map(|event| match event {
                DiagnosticEvent::EffectScheduled { queued, .. } => Some(*queued),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(queued, [true, false]);
    }

    #[test]
    fn subscription_keeps_an_empty_default_reactor_alive() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let subscription = current().subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let value = signal(0_u32);
        value.set(1);
        assert!(
            events
                .borrow()
                .iter()
                .any(|event| matches!(event, DiagnosticEvent::ReactiveWrite { .. }))
        );
        drop(subscription);
    }

    #[test]
    fn disposing_an_effect_is_reported() {
        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });
        let effect = reactor.effect(|| {});
        reactor.flush_now();
        let effect_id = events
            .borrow()
            .iter()
            .find_map(|event| match event {
                DiagnosticEvent::EffectRunStarted { effect, .. } => Some(*effect),
                _ => None,
            })
            .expect("initial effect run should be diagnosed");
        events.borrow_mut().clear();

        effect.dispose();

        assert!(events.borrow().iter().any(|event| matches!(
            event,
            DiagnosticEvent::EffectDisposed { effect, .. } if *effect == effect_id
        )));
    }

    #[test]
    fn a_write_is_followed_through_equality_suppressed_verification_to_the_effect() {
        use crate::{ComputeOutcome, NodeKind};

        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        // signal -> parity -> label -> effect. A write that flips the signal without flipping the
        // parity must be visible end to end: both memos verify, the first recomputes and reports
        // `changed: false`, the second never runs, and the effect is skipped.
        let source = signal_in(&reactor, 1_u32);
        let parity = memo_in(&reactor, {
            let source = source.clone();
            move || source.get() % 2
        });
        let label = memo_in(&reactor, {
            let parity = parity.clone();
            move || parity.get() * 10
        });
        let _effect = reactor.effect({
            let label = label.clone();
            move || {
                let _ = label.get();
            }
        });
        reactor.flush_now();
        events.borrow_mut().clear();

        source.set(3);
        reactor.flush_now();

        let events = events.borrow();
        let write = events
            .iter()
            .find_map(|event| match event {
                DiagnosticEvent::ReactiveWrite { cause, .. } => Some(*cause),
                _ => None,
            })
            .expect("the write opens the causal chain");
        assert_eq!(write.node, source.id());

        // The middle of the chain is now visible, and each link still names the original write.
        let invalidated = events
            .iter()
            .filter_map(|event| match event {
                DiagnosticEvent::ComputedInvalidated {
                    node, kind, cause, ..
                } => Some((*node, *kind, *cause)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            invalidated
                .iter()
                .any(|(node, kind, cause)| *node == parity.id()
                    && *kind == NodeKind::Memo
                    && *cause == write),
            "the first memo reports the write that reached it"
        );
        assert!(
            invalidated
                .iter()
                .any(|(node, _, cause)| *node == label.id() && *cause == write),
            "and so does the second, rather than blaming the memo above it"
        );

        // The equality-suppressed recomputation is reported as such.
        let suppressed = events
            .iter()
            .find_map(|event| match event {
                DiagnosticEvent::ComputedRecomputeFinished {
                    node,
                    changed,
                    outcome,
                    ..
                } if *node == parity.id() => Some((*changed, *outcome)),
                _ => None,
            })
            .expect("the parity memo recomputes");
        assert_eq!(suppressed, (false, ComputeOutcome::Completed));

        // The downstream memo verifies and resolves from cache — no recomputation at all.
        let label_verified = events
            .iter()
            .find_map(|event| match event {
                DiagnosticEvent::ComputedVerified {
                    node, recomputed, ..
                } if *node == label.id() => Some(*recomputed),
                _ => None,
            })
            .expect("the label memo verifies its inputs");
        assert!(!label_verified, "verification resolved from cache");
        assert!(
            !events.iter().any(|event| matches!(
                event,
                DiagnosticEvent::ComputedRecomputeStarted { node, .. } if *node == label.id()
            )),
            "so it must not have recomputed"
        );

        assert!(
            events
                .iter()
                .any(|event| matches!(event, DiagnosticEvent::EffectRunSkipped { .. })),
            "and the effect body is spared"
        );
    }

    #[test]
    fn the_generic_accessors_agree_with_every_variant_they_cover() {
        use core::panic::Location;

        use crate::{NodeId, thunk_in};

        // These exist so a consumer does not need a match arm per variant to read a field every
        // variant carries. They were added without a test, which is precisely the shape that
        // rots: a variant added later would silently return the wrong thing, or `None`.
        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let source = signal_in(&reactor, 1_u32);
        let doubled = thunk_in(&reactor, {
            let source = source.clone();
            move || source.get() * 2
        });
        let effect = reactor.effect({
            let doubled = doubled.clone();
            move || {
                let _ = doubled.get();
            }
        });
        reactor.flush_now();
        source.set(2);
        source.set(2); // suppressed at the source
        reactor.flush_now();
        effect.dispose();

        let events = events.borrow();
        assert!(events.len() > 10, "the workload should be varied");

        for event in events.iter() {
            // Present on every variant without exception.
            assert_eq!(event.reactor(), reactor.id());

            // Stated per variant against the payload, and matched exhaustively: the catch-all
            // this replaced asserted only `node().is_some()` for two thirds of the variants, so
            // a `node_origin()` or `flush_epoch()` arm that returned the wrong thing — or
            // nothing — passed. `#[non_exhaustive]` does not bind this crate, so a variant added
            // later fails to compile here until its mapping is written down.
            #[allow(clippy::type_complexity)]
            let expected: (
                Option<NodeId>,
                Option<&'static Location<'static>>,
                Option<u64>,
            ) = match event {
                DiagnosticEvent::NodeCreated { node, origin, .. }
                | DiagnosticEvent::NodeDisposed { node, origin, .. } => {
                    (Some(*node), Some(*origin), None)
                }
                DiagnosticEvent::WriteSuppressed {
                    node, node_origin, ..
                } => (Some(*node), Some(*node_origin), None),
                DiagnosticEvent::ReactiveWrite { cause, .. } => {
                    (Some(cause.node), Some(cause.node_origin), None)
                }
                // The mark a write propagates belongs to no flush, even though it records the
                // epoch that was open — see `flush_epoch`'s own documentation for why reporting
                // it would bucket the mark under the flush *before* the one that drains it.
                DiagnosticEvent::ComputedInvalidated {
                    node, node_origin, ..
                } => (Some(*node), Some(*node_origin), None),
                DiagnosticEvent::ComputedVerified {
                    node,
                    node_origin,
                    flush_epoch,
                    ..
                }
                | DiagnosticEvent::ComputedRecomputeStarted {
                    node,
                    node_origin,
                    flush_epoch,
                    ..
                }
                | DiagnosticEvent::ComputedRecomputeFinished {
                    node,
                    node_origin,
                    flush_epoch,
                    ..
                } => (Some(*node), Some(*node_origin), Some(*flush_epoch)),
                DiagnosticEvent::EffectInvalidated {
                    effect,
                    effect_origin,
                    ..
                } => (Some(*effect), Some(*effect_origin), None),
                DiagnosticEvent::EffectScheduled {
                    effect,
                    effect_origin,
                    flush_epoch,
                    ..
                }
                | DiagnosticEvent::EffectRunStarted {
                    effect,
                    effect_origin,
                    flush_epoch,
                    ..
                } => (Some(*effect), Some(*effect_origin), Some(*flush_epoch)),
                DiagnosticEvent::EffectRunFinished {
                    effect,
                    flush_epoch,
                    ..
                }
                | DiagnosticEvent::EffectRunSkipped {
                    effect,
                    flush_epoch,
                    ..
                } => (Some(*effect), None, Some(*flush_epoch)),
                DiagnosticEvent::EffectDisposed { effect, .. } => (Some(*effect), None, None),
                DiagnosticEvent::FlushStarted { flush_epoch, .. }
                | DiagnosticEvent::FlushFinished { flush_epoch, .. } => {
                    (None, None, Some(*flush_epoch))
                }
            };
            let (node, node_origin, flush_epoch) = expected;
            assert_eq!(event.node(), node, "node() disagrees with {event:?}");
            assert_eq!(
                event.node_origin(),
                node_origin,
                "node_origin() disagrees with {event:?}"
            );
            assert_eq!(
                event.flush_epoch(),
                flush_epoch,
                "flush_epoch() disagrees with {event:?}"
            );

            // A reported epoch must be a flush that actually happened.
            if let Some(epoch) = event.flush_epoch() {
                assert!(epoch <= reactor.graph_stats().flush_epoch);
            }
        }

        // The mapping above is only worth as much as the variants the workload reaches, and the
        // effect variants are exactly the ones the previous catch-all left unchecked.
        let reached = |name: &str| {
            events.iter().any(|event| match event {
                DiagnosticEvent::EffectInvalidated { .. } => name == "EffectInvalidated",
                DiagnosticEvent::EffectScheduled { .. } => name == "EffectScheduled",
                DiagnosticEvent::EffectRunStarted { .. } => name == "EffectRunStarted",
                DiagnosticEvent::EffectRunFinished { .. } => name == "EffectRunFinished",
                DiagnosticEvent::EffectDisposed { .. } => name == "EffectDisposed",
                DiagnosticEvent::ComputedInvalidated { .. } => name == "ComputedInvalidated",
                DiagnosticEvent::ComputedRecomputeStarted { .. } => {
                    name == "ComputedRecomputeStarted"
                }
                _ => false,
            })
        };
        for name in [
            "EffectInvalidated",
            "EffectScheduled",
            "EffectRunStarted",
            "EffectRunFinished",
            "EffectDisposed",
            "ComputedInvalidated",
            "ComputedRecomputeStarted",
        ] {
            assert!(
                reached(name),
                "{name} is absent, so its mapping is untested"
            );
        }

        // The accessors reach the kinds the workload produced, not just one of them.
        let nodes = events
            .iter()
            .filter_map(DiagnosticEvent::node)
            .collect::<Vec<_>>();
        assert!(nodes.contains(&source.id()));
        assert!(nodes.contains(&doubled.id()));
        assert!(
            events.iter().any(|e| e.flush_epoch().is_some()),
            "some events carry a flush"
        );
        assert!(
            events.iter().any(|e| e.flush_epoch().is_none()),
            "and some legitimately do not"
        );

        // Ids format as a pair without `.get()`. What `all()` enumerates is pinned by
        // `node_kind_all_and_count_track_the_variant_set` rather than by a literal here.
        assert_eq!(
            format!("{}:{}", reactor.id(), source.id()),
            format!("{}:{}", reactor.id().get(), source.id().get())
        );
    }

    #[test]
    fn node_kind_all_and_count_track_the_variant_set() {
        use crate::NodeKind;

        // A roll call the compiler checks. Only `index()` is forced to grow when a kind is added:
        // `all()` can silently under-report, and `COUNT` sizes the per-kind counter arrays, so a
        // stale `COUNT` turns the first node of a new kind into an out-of-bounds panic. The
        // exhaustive match below is what makes adding a kind a compile error here first.
        let declared = [
            NodeKind::Source,
            NodeKind::Signal,
            NodeKind::Event,
            NodeKind::Thunk,
            NodeKind::Memo,
            NodeKind::Effect,
        ];
        for kind in declared {
            match kind {
                NodeKind::Source
                | NodeKind::Signal
                | NodeKind::Event
                | NodeKind::Thunk
                | NodeKind::Memo
                | NodeKind::Effect => {}
            }
        }

        assert_eq!(
            declared.len(),
            NodeKind::COUNT,
            "COUNT drifted from the variant set, and it sizes the per-kind counter arrays"
        );
        assert_eq!(
            NodeKind::all().count(),
            NodeKind::COUNT,
            "all() drifted from the variant set: the old assertion compared it against a literal, \
             so it fired when all() was updated and stayed silent when it was forgotten"
        );

        // `index()` must be a bijection onto the counter slots, or a kind is either invisible in
        // `live_nodes_of_kind` or sharing another kind's tally.
        let mut seen = [false; NodeKind::COUNT];
        for kind in NodeKind::all() {
            let index = kind.index();
            assert!(
                index < NodeKind::COUNT,
                "{kind:?} indexes past the counters"
            );
            assert!(
                !seen[index],
                "{kind:?} shares index {index} with another kind"
            );
            seen[index] = true;
        }
        assert!(
            seen.iter().all(|slot| *slot),
            "all() does not cover every counter slot"
        );
        for kind in declared {
            assert!(
                NodeKind::all().any(|reported| reported == kind),
                "{kind:?} is missing from all()"
            );
        }
    }

    #[test]
    fn a_coalesced_mark_is_reported_and_says_it_changed_nothing() {
        // The contract is that `ComputedInvalidated` fires for *every* mark delivered, including
        // one that coalesces into staleness the node already had — that is what exposes
        // propagation amplification. `state_changed` is what separates the two, and neither
        // half had a test.
        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let source = signal_in(&reactor, 0_u32);
        let doubled = memo_in(&reactor, {
            let source = source.clone();
            move || source.get() * 2
        });
        assert_eq!(doubled.get(), 0);
        events.borrow_mut().clear();

        // Two writes with no read in between: the first makes the memo dirty, the second finds
        // it already dirty and changes nothing.
        source.set(1);
        source.set(2);

        let marks = events
            .borrow()
            .iter()
            .filter_map(|event| match event {
                DiagnosticEvent::ComputedInvalidated {
                    node,
                    state_changed,
                    ..
                } if *node == doubled.id() => Some(*state_changed),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            marks,
            [true, false],
            "both marks must be reported, and only the first changed the node's state"
        );
    }

    #[test]
    fn a_panicking_computation_closes_its_pair() {
        use crate::{ComputeOutcome, thunk_in};
        use std::panic::{AssertUnwindSafe, catch_unwind};

        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let boom = thunk_in(&reactor, || -> u32 { panic!("compute failed") });
        let id = boom.id();
        let result = catch_unwind(AssertUnwindSafe(|| boom.get()));
        assert!(result.is_err());

        let events = events.borrow();
        let started = events
            .iter()
            .filter(|event| {
                matches!(event, DiagnosticEvent::ComputedRecomputeStarted { node, .. } if *node == id)
            })
            .count();
        let finished = events
            .iter()
            .filter_map(|event| match event {
                DiagnosticEvent::ComputedRecomputeFinished {
                    node,
                    outcome,
                    changed,
                    ..
                } if *node == id => Some((*outcome, *changed)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(started, 1);
        assert_eq!(
            finished,
            [(ComputeOutcome::Panicked, false)],
            "an unwinding computation still closes its pair, and publishes nothing"
        );
    }

    #[test]
    fn dependency_counts_show_a_computation_whose_read_set_grows() {
        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let width = signal_in(&reactor, 1_usize);
        let inputs = (0..4)
            .map(|i| signal_in(&reactor, i as u32))
            .collect::<Vec<_>>();
        let sum = memo_in(&reactor, {
            let width = width.clone();
            let inputs = inputs.clone();
            move || {
                inputs
                    .iter()
                    .take(width.get())
                    .map(|s| s.get())
                    .sum::<u32>()
            }
        });
        assert_eq!(sum.get(), 0);
        events.borrow_mut().clear();

        // The read set widens: this is the shape behind a component that gets slower the longer
        // it lives, and the before/after counts are what make it visible without per-edge events.
        width.set(4);
        assert_eq!(sum.get(), 6);

        let span = events
            .borrow()
            .iter()
            .find_map(|event| match event {
                DiagnosticEvent::ComputedRecomputeFinished {
                    node,
                    dependencies_after,
                    ..
                } if *node == sum.id() => Some(*dependencies_after),
                _ => None,
            })
            .expect("the memo recomputes");
        let before = events
            .borrow()
            .iter()
            .find_map(|event| match event {
                DiagnosticEvent::ComputedRecomputeStarted {
                    node,
                    dependencies_before,
                    ..
                } if *node == sum.id() => Some(*dependencies_before),
                _ => None,
            })
            .expect("and reports what it started from");
        assert_eq!(before, 2, "width plus the first input");
        assert_eq!(span, 5, "width plus all four inputs");
    }

    #[test]
    fn every_node_kind_reports_its_creation_and_disposal() {
        use crate::{NodeKind, event_in, thunk_in};

        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let source = source_in(&reactor);
        let signal = signal_in(&reactor, 0_u32);
        let stream = event_in::<u32>(&reactor);
        let thunk = thunk_in(&reactor, || 1_u32);
        let memo = memo_in(&reactor, || 2_u32);
        let effect = reactor.effect(|| {});

        let created = |kind| {
            events.borrow().iter().any(|event| {
                matches!(event, DiagnosticEvent::NodeCreated { kind: reported, .. } if *reported == kind)
            })
        };
        // Iterating rather than listing: a kind added later is covered here automatically, which
        // is the whole reason `NodeKind::all` exists.
        for kind in NodeKind::all() {
            assert!(created(kind), "{kind:?} should report its creation");
        }

        // The kind is what the node was allocated as, and the reactor agrees with the stream.
        assert_eq!(reactor.node_kind(source.id()), Some(NodeKind::Source));
        assert_eq!(reactor.node_kind(signal.id()), Some(NodeKind::Signal));
        assert_eq!(reactor.node_kind(stream.id()), Some(NodeKind::Event));
        assert_eq!(reactor.node_kind(thunk.id()), Some(NodeKind::Thunk));
        assert_eq!(reactor.node_kind(memo.id()), Some(NodeKind::Memo));
        assert_eq!(reactor.node_kind(effect.id()), Some(NodeKind::Effect));

        let signal_id = signal.id();
        events.borrow_mut().clear();
        drop(signal);

        let disposals = events
            .borrow()
            .iter()
            .filter(|event| {
                matches!(event, DiagnosticEvent::NodeDisposed { node, .. } if *node == signal_id)
            })
            .count();
        assert_eq!(disposals, 1);
        assert_eq!(reactor.node_kind(signal_id), None);

        // Disposal is idempotent and reached from several `Drop` impls; the event is not.
        events.borrow_mut().clear();
        reactor.dispose(signal_id);
        assert!(
            events.borrow().is_empty(),
            "a node already gone reports nothing"
        );
    }

    #[test]
    fn disposal_reports_the_edges_the_node_died_holding() {
        use crate::NodeKind;

        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        let left = signal_in(&reactor, 1_u32);
        let right = signal_in(&reactor, 2_u32);
        let total = memo_in(&reactor, {
            let left = left.clone();
            let right = right.clone();
            move || left.get() + right.get()
        });
        assert_eq!(total.get(), 3);

        let memo_id = total.id();
        events.borrow_mut().clear();
        drop(total);

        let epitaph = events
            .borrow()
            .iter()
            .find_map(|event| match event {
                DiagnosticEvent::NodeDisposed {
                    node,
                    kind,
                    dependencies,
                    observers,
                    ..
                } if *node == memo_id => Some((*kind, *dependencies, *observers)),
                _ => None,
            })
            .expect("the memo should report its disposal");
        assert_eq!(
            epitaph,
            (NodeKind::Memo, 2, 0),
            "counts are sampled before teardown empties the maps"
        );
    }

    #[test]
    fn diagnostics_do_not_change_stale_node_trigger_behavior() {
        let reactor = Reactor::new();
        let stale_id = {
            let source = source_in(&reactor);
            source.id()
        };

        reactor.trigger(stale_id);

        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });
        reactor.trigger(stale_id);
        assert!(
            events.borrow().is_empty(),
            "a stale node remains a silent no-op when diagnostics are active"
        );
    }

    /// Runs a workload that produces every `DiagnosticEvent` variant.
    fn exercise(reactor: &Reactor) {
        use crate::thunk_in;

        let seen = Rc::new(RefCell::new(Vec::new()));
        let source = signal_in(reactor, 1_u32);
        let parity = memo_in(reactor, {
            let source = source.clone();
            move || source.get() % 2
        });
        let doubled = thunk_in(reactor, {
            let parity = parity.clone();
            move || parity.get() * 2
        });
        let effect = reactor.effect({
            let doubled = doubled.clone();
            let seen = Rc::clone(&seen);
            move || seen.borrow_mut().push(doubled.get())
        });
        reactor.flush_now();

        source.set(2); // changes parity: recompute publishes
        source.set(4); // coalesces into the pending run
        reactor.flush_now();
        source.set(6); // parity unchanged: memo suppresses, effect skipped
        reactor.flush_now();
        source.set(6); // value unchanged: the *source* suppresses, nothing reaches the graph
        reactor.flush_now();
        effect.dispose();
        drop(doubled);
        reactor.flush_now();
    }

    #[test]
    fn every_event_stops_when_the_last_subscription_drops() {
        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });

        exercise(&reactor);

        // Every variant the release added must actually be reachable, or the test below proves
        // nothing about it.
        // Matched exhaustively on purpose: `#[non_exhaustive]` binds downstream crates, not this
        // one, so adding a variant fails to compile here until it is covered below.
        let names = |events: &Vec<DiagnosticEvent>| {
            events
                .iter()
                .map(|event| match event {
                    DiagnosticEvent::NodeCreated { .. } => "NodeCreated",
                    DiagnosticEvent::NodeDisposed { .. } => "NodeDisposed",
                    DiagnosticEvent::WriteSuppressed { .. } => "WriteSuppressed",
                    DiagnosticEvent::ReactiveWrite { .. } => "ReactiveWrite",
                    DiagnosticEvent::ComputedInvalidated { .. } => "ComputedInvalidated",
                    DiagnosticEvent::ComputedVerified { .. } => "ComputedVerified",
                    DiagnosticEvent::ComputedRecomputeStarted { .. } => "ComputedRecomputeStarted",
                    DiagnosticEvent::ComputedRecomputeFinished { .. } => {
                        "ComputedRecomputeFinished"
                    }
                    DiagnosticEvent::EffectInvalidated { .. } => "EffectInvalidated",
                    DiagnosticEvent::EffectScheduled { .. } => "EffectScheduled",
                    DiagnosticEvent::EffectRunStarted { .. } => "EffectRunStarted",
                    DiagnosticEvent::EffectRunFinished { .. } => "EffectRunFinished",
                    DiagnosticEvent::EffectRunSkipped { .. } => "EffectRunSkipped",
                    DiagnosticEvent::EffectDisposed { .. } => "EffectDisposed",
                    DiagnosticEvent::FlushStarted { .. } => "FlushStarted",
                    DiagnosticEvent::FlushFinished { .. } => "FlushFinished",
                })
                .collect::<Vec<_>>()
        };

        let seen = names(&events.borrow());
        for expected in [
            "NodeCreated",
            "NodeDisposed",
            "WriteSuppressed",
            "ReactiveWrite",
            "ComputedInvalidated",
            "ComputedVerified",
            "ComputedRecomputeStarted",
            "ComputedRecomputeFinished",
            "EffectInvalidated",
            "EffectScheduled",
            "EffectRunStarted",
            "EffectRunFinished",
            "EffectRunSkipped",
            "EffectDisposed",
            "FlushStarted",
            "FlushFinished",
        ] {
            assert!(
                seen.contains(&expected),
                "{expected} is unreachable in the workload, so its dormancy is untested"
            );
        }
        // Now the actual claim: dropping the subscription stops all of it.
        drop(subscription);
        assert!(!reactor.diagnostics_enabled());
        events.borrow_mut().clear();

        exercise(&reactor);
        assert!(
            events.borrow().is_empty(),
            "delivery continued after the subscription was dropped: {:?}",
            names(&events.borrow())
        );
    }

    #[test]
    fn flush_totals_do_not_survive_an_unsubscribed_window() {
        let reactor = Reactor::new();
        let source = signal_in(&reactor, 0_u32);
        let effect = reactor.effect({
            let source = source.clone();
            move || {
                let _ = source.get();
            }
        });
        reactor.flush_now();

        // Accumulate into the pending slot *while subscribed*, and outside any flush: a write
        // schedules a job but does not drain it, so this lands in `FlushAccounting::pending` and
        // is exactly what a later subscriber must not inherit. Accumulating while unsubscribed
        // would prove nothing — `record_flush` is gated, so nothing is collected in the first
        // place, and the test would pass with `reset` deleted.
        let first = reactor.subscribe_diagnostics(|_| {});
        source.set(1);
        drop(first);

        let flushes = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let flushes = Rc::clone(&flushes);
            move |event| {
                if let DiagnosticEvent::FlushFinished { stats, .. } = event {
                    flushes.borrow_mut().push(stats);
                }
            }
        });
        // Real work, so a flush actually happens: a settled graph does not flush at all.
        source.set(99);
        reactor.flush_now();

        let flushes = flushes.borrow();
        assert_eq!(flushes.len(), 1);
        assert_eq!(
            flushes[0].root_writes, 1,
            "the write made under the previous subscription must have been discarded, not \
             carried into this flush"
        );
        assert_eq!(
            flushes[0].effects_run, 1,
            "a new subscriber must not inherit totals from a window it could not observe"
        );

        effect.dispose();
    }

    #[test]
    fn dropping_the_subscription_stops_delivery_and_disables_diagnostics() {
        let reactor = Reactor::new();
        let source = source_in(&reactor);
        let events = Rc::new(RefCell::new(Vec::new()));
        let subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });
        assert!(reactor.diagnostics_enabled());

        source.trigger();
        assert!(!events.borrow().is_empty());
        events.borrow_mut().clear();
        drop(subscription);

        assert!(!reactor.diagnostics_enabled());
        source.trigger();
        assert!(
            events.borrow().is_empty(),
            "dropping the subscription must stop all later delivery"
        );
    }

    #[test]
    fn flush_boundaries_and_verified_skips_are_reported() {
        let reactor = Reactor::new();
        let events = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let events = Rc::clone(&events);
            move |event| events.borrow_mut().push(event)
        });
        let source = signal_in(&reactor, 1_u32);
        let parity = memo_in(&reactor, {
            let source = source.clone();
            move || source.get() % 2
        });
        let _effect = reactor.effect({
            let parity = parity.clone();
            move || {
                let _ = parity.get();
            }
        });
        reactor.flush_now();
        events.borrow_mut().clear();

        source.set(3);
        reactor.flush_now();

        let events = events.borrow();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DiagnosticEvent::FlushStarted { .. }))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DiagnosticEvent::EffectRunSkipped { .. }))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, DiagnosticEvent::FlushFinished { .. }))
        );
    }

    #[test]
    fn a_nested_flush_closes_its_own_epoch() {
        let reactor = Reactor::new();
        let boundaries = Rc::new(RefCell::new(Vec::new()));
        let _subscription = reactor.subscribe_diagnostics({
            let boundaries = Rc::clone(&boundaries);
            move |event| match event {
                DiagnosticEvent::FlushStarted { flush_epoch, .. } => {
                    boundaries.borrow_mut().push(("start", flush_epoch));
                }
                DiagnosticEvent::FlushFinished { flush_epoch, .. } => {
                    boundaries.borrow_mut().push(("finish", flush_epoch));
                }
                _ => {}
            }
        });

        // A job that queues more work and then flushes re-entrantly bumps the shared epoch
        // mid-flush. The inner job matters: a drain with an empty queue is no longer a flush at
        // all, so without it there would be no nesting to observe.
        reactor.schedule({
            let reactor = reactor.clone();
            move || {
                reactor.schedule(|| {});
                reactor.flush_now();
            }
        });
        reactor.flush_now();

        assert_eq!(
            &*boundaries.borrow(),
            &[("start", 1), ("start", 2), ("finish", 2), ("finish", 1)],
            "each flush must close the epoch it opened"
        );
    }
}
