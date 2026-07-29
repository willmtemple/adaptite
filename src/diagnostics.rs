use alloc::boxed::Box;
use core::panic::Location;

use crate::NodeId;

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

/// The primitive a reactive node was created as.
///
/// The kind is **declared at construction, not inferred from behaviour**. A primitive built on
/// [`crate::source`] reports [`Source`](NodeKind::Source), because a raw source is exactly what
/// the graph was handed; a [`crate::Writable`] is a memo bundled with a setter and reports
/// [`Memo`](NodeKind::Memo); [`crate::Resource`] and [`crate::watch`] compose existing nodes and
/// contribute no node of their own. Read this as "what adaptite was asked to allocate", not as a
/// claim about what the consumer built with it.
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
        dependents: usize,
    },
    /// A source node changed.
    #[non_exhaustive]
    ReactiveWrite {
        /// Graph containing the node.
        reactor: ReactorId,
        /// Root mutation and its source locations.
        cause: InvalidationCause,
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
    },
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
        for kind in [
            NodeKind::Source,
            NodeKind::Signal,
            NodeKind::Event,
            NodeKind::Thunk,
            NodeKind::Memo,
            NodeKind::Effect,
        ] {
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
                    dependents,
                    ..
                } if *node == memo_id => Some((*kind, *dependencies, *dependents)),
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

        // A job that flushes re-entrantly bumps the shared epoch mid-flush.
        reactor.schedule({
            let reactor = reactor.clone();
            move || reactor.flush_now()
        });
        reactor.flush_now();

        assert_eq!(
            &*boundaries.borrow(),
            &[("start", 1), ("start", 2), ("finish", 2), ("finish", 1)],
            "each flush must close the epoch it opened"
        );
    }
}
