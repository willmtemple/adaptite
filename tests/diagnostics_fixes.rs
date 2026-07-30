//! Regression tests for the diagnostics, inspection and accounting surfaces.
//!
//! Each test here pins something a release review found either wrong or unpinned: a snapshot
//! lookup that depended on an invariant the consumer is free to break, the composite-to-primitive
//! node mapping the `NodeKind` documentation is the only statement of, and the flush attribution
//! the `flush_epoch()` accessor deliberately refuses to guess at.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use adaptite::{DiagnosticEvent, NodeKind, Reactor, memo_in, signal_in};
use runite::time::sleep;
use runite::{queue_macrotask, run};

#[test]
fn a_snapshot_lookup_survives_the_consumer_reordering_the_public_node_list() {
    // `GraphSnapshot::nodes` is public and documented as ordered by id, but that ordering is a
    // property of what adaptite hands back, not an invariant the caller promised to preserve.
    // `node()` binary-searched it, so a consumer that sorted the list in place — to rank by
    // version, say — got `None` for nodes that were very much live, and `None` is documented to
    // mean "not in this snapshot". A wrong answer indistinguishable from a legitimate one.
    let reactor = Reactor::new();
    let signals = (0..16)
        .map(|i| signal_in(&reactor, u32::try_from(i).unwrap()))
        .collect::<Vec<_>>();
    // Distinct versions, so sorting by version really does reorder the list.
    for (i, signal) in signals.iter().enumerate() {
        for j in 0..=i {
            signal.set(u32::try_from(1000 + i * 32 + j).unwrap());
        }
    }

    let target = signals[5].id();
    let mut snapshot = reactor.graph_snapshot();
    assert_eq!(
        snapshot.node(target).map(|node| node.id),
        Some(target),
        "the lookup works on the snapshot as handed over"
    );

    snapshot
        .nodes
        .sort_by_key(|node| std::cmp::Reverse(node.version));
    assert!(
        snapshot
            .nodes
            .windows(2)
            .any(|pair| pair[0].id > pair[1].id),
        "sanity: the sort really did break the id ordering the lookup relied on"
    );

    let found = snapshot
        .node(target)
        .expect("a live node must still be found after the caller reorders the list");
    assert_eq!(found.id, target);
    assert_eq!(found.kind, NodeKind::Signal);

    // And a genuinely absent node still reports absent rather than something nearby.
    drop(signals);
    let snapshot = reactor.graph_snapshot();
    assert!(snapshot.node(target).is_none());
}

#[test]
fn a_resource_allocates_five_nodes_and_watch_two() {
    // The `NodeKind` doc is the only place the composite-to-primitive mapping is written down,
    // and it said a resource allocates "three signals and an effect" — four, one kind short of
    // the truth. A consumer budgeting `4 * resources` for leak accounting is wrong per resource,
    // and sees memos it never wrote. Nothing pinned the composition, so pin it here.
    let report = Rc::new(RefCell::new(Vec::new()));
    queue_macrotask({
        let report = Rc::clone(&report);
        move || {
            let reactor = Reactor::new();
            let id = signal_in(&reactor, 1u32);

            let before = reactor.graph_stats();
            let fetched = reactor.resource(
                {
                    let id = id.clone();
                    move || id.get()
                },
                |id| async move { id * 10 },
            );
            reactor.flush_now();
            let after = reactor.graph_stats();

            let mut deltas = Vec::new();
            for kind in NodeKind::all() {
                deltas.push((
                    kind,
                    after.live_nodes_of_kind(kind) - before.live_nodes_of_kind(kind),
                ));
            }
            report
                .borrow_mut()
                .push(("resource", deltas, after.live_nodes - before.live_nodes));

            // The control: the same sentence's claim about `watch`, which is correct.
            let before = reactor.graph_stats();
            let watcher = reactor.watch(
                {
                    let id = id.clone();
                    move || id.get()
                },
                |_, _| {},
            );
            reactor.flush_now();
            let after = reactor.graph_stats();
            let mut deltas = Vec::new();
            for kind in NodeKind::all() {
                deltas.push((
                    kind,
                    after.live_nodes_of_kind(kind) - before.live_nodes_of_kind(kind),
                ));
            }
            report
                .borrow_mut()
                .push(("watch", deltas, after.live_nodes - before.live_nodes));

            watcher.leak();
            drop(fetched);
            drop(runite::spawn(async move {
                sleep(Duration::from_millis(5)).await;
            }));
        }
    });
    run();

    let report = report.borrow();
    let resource = report
        .iter()
        .find(|(what, _, _)| *what == "resource")
        .expect("the resource ran");
    assert_eq!(
        resource.2, 5,
        "a resource allocates five nodes, not the four the docs used to claim"
    );
    let expected = [
        (NodeKind::Source, 0),
        (NodeKind::Signal, 3),
        (NodeKind::Event, 0),
        (NodeKind::Thunk, 0),
        (NodeKind::Memo, 1),
        (NodeKind::Effect, 1),
    ];
    assert_eq!(
        resource.1, expected,
        "value, loading and refetch-tick signals, the equality-gate memo, and the driving effect"
    );

    let watch = report
        .iter()
        .find(|(what, _, _)| *what == "watch")
        .expect("the watch ran");
    assert_eq!(watch.2, 2);
    assert_eq!(
        watch.1,
        [
            (NodeKind::Source, 0),
            (NodeKind::Signal, 0),
            (NodeKind::Event, 0),
            (NodeKind::Thunk, 0),
            (NodeKind::Memo, 1),
            (NodeKind::Effect, 1),
        ]
    );
}

#[test]
fn an_invalidation_reports_no_flush_because_it_belongs_to_none() {
    // `ComputedInvalidated` is the one variant carrying a `flush_epoch` field that
    // `DiagnosticEvent::flush_epoch()` reports as `None`, and it looks like an oversight until
    // the number is measured: a write outside any flush records the epoch of the *previous*
    // flush, while the flush that drains it is the next one. Reporting the field would bucket
    // the mark under a flush that had already closed before the write happened — which also
    // contradicts `FlushStats`, whose totals hand out-of-flush work to the flush that drains it.
    // This test is what stops a future reader "fixing" the accessor into that wrong bucket.
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
    let effect = reactor.effect({
        let doubled = doubled.clone();
        move || {
            let _ = doubled.get();
        }
    });
    reactor.flush_now();
    let closed_epoch = reactor.graph_stats().flush_epoch;
    assert_eq!(reactor.graph_stats().flush_depth, 0, "no flush is open");
    events.borrow_mut().clear();

    source.set(1);

    let marks = events
        .borrow()
        .iter()
        .filter_map(|event| match event {
            DiagnosticEvent::ComputedInvalidated { flush_epoch, .. } => {
                Some((*flush_epoch, event.flush_epoch()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!marks.is_empty(), "the write reached the memo");
    for (payload, reported) in marks {
        assert_eq!(
            payload, closed_epoch,
            "the field records the most recently opened flush, which here is one that closed \
             before the write"
        );
        assert_eq!(
            reported, None,
            "and the accessor declines to call that the flush the mark belongs to"
        );
    }

    reactor.flush_now();
    assert_eq!(
        reactor.graph_stats().flush_epoch,
        closed_epoch + 1,
        "the flush that actually drained the write is the *next* epoch, not the recorded one"
    );

    effect.dispose();
}
