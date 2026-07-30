//! Guards for claims the shipped documentation makes.
//!
//! `README.md` is pulled into the crate root by `src/lib.rs`, and `docs/diagnostics.md` and
//! `docs/MIGRATING-0.3.md` ship inside the `.crate`. All three freeze onto a per-version docs.rs
//! page at publish, so a sentence that is wrong there stays wrong for that version. The 0.3
//! release-readiness review found two shapes of rot in exactly those files: prose that described
//! a `cfg`-gated behaviour after the gate was removed, and reference sections that named an API
//! item that no longer (or did not yet) exist under that name.
//!
//! This file pins the claims that a test *can* pin: the profile the divergence guard is enforced
//! in, and the existence and shape of the public items the reference sections now list. It is
//! deliberately not a second copy of the diagnostics suite — it asserts only what the docs say.

use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use adaptite::{
    DiagnosticEvent, NodeKind, NodeState, Reactor, RecordedDependency, memo_in, ownership_stats,
    signal_in,
};

/// `README.md` ("Feedback loops") and `docs/MIGRATING-0.3.md` ("a divergent loop now panics in
/// release") both promise that the divergence guard is enforced in *every* build. Before 0.3 it
/// was `cfg(debug_assertions)`, and the README still said so — under a heading a consumer reads
/// to find out what their release build does.
///
/// This test carries no `cfg`, so it runs under `cargo test` and `cargo test --release` alike
/// (`mise run check` runs both). Re-gating the guard to debug builds turns the release run into a
/// hang, which is the failure the docs were describing as impossible.
#[test]
fn a_divergent_loop_panics_in_this_build_profile() {
    let reactor = Reactor::new();
    let counter = signal_in(&reactor, 0_u64);

    // Every run changes the value, so the loop never converges.
    let effect = reactor.effect({
        let counter = counter.clone();
        move || {
            let next = counter.get() + 1;
            counter.set(next);
        }
    });

    let panic = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()))
        .expect_err("a divergent feedback loop must panic rather than hang, in every profile");
    let message = panic
        .downcast_ref::<String>()
        .cloned()
        .expect("the divergence panic payload is a formatted String");
    assert!(
        message.contains("divergent reactive feedback loop"),
        "the panic must diagnose the divergence, got: {message}"
    );
    assert!(
        message.contains("in a single drain"),
        "the guard counts per drain, not per flush, and the message says so: {message}"
    );

    effect.dispose();
}

/// `docs/diagnostics.md`'s Reference section is presented as the index for the whole contract, and
/// `CHANGELOG.md` lists these items by name. Seven of them shipped in 0.3 named nowhere at all;
/// having written them down, this pins the names, so a rename cannot leave the reference pointing
/// at nothing.
///
/// It exercises rather than merely mentions each one: a name that compiles but reports nothing is
/// the same documentation failure one step later.
#[test]
fn the_reference_section_names_items_that_exist_and_answer() {
    let reactor = Reactor::new();
    let source = signal_in(&reactor, 1_u32);
    let doubled = memo_in(&reactor, {
        let source = source.clone();
        move || source.get() * 2
    });

    // A node id that is no longer live, for the "not live" halves below. Ids are never reused,
    // so this can never come to mean a different node.
    let dead = {
        let short_lived = signal_in(&reactor, 0_u8);
        short_lived.id()
    };
    assert_eq!(reactor.node_kind(dead), None, "the id is genuinely dead");

    let seen = Rc::new(Cell::new(0_u32));
    let effect = reactor.effect({
        let doubled = doubled.clone();
        let seen = Rc::clone(&seen);
        move || seen.set(doubled.get())
    });
    reactor.flush_now();

    // `Reactor::node_state` — the O(1), per-node counterpart to walking a snapshot.
    assert_eq!(
        reactor.node_state(doubled.id()),
        Some(NodeState::Clean),
        "a memo that has just been read is clean"
    );
    source.set(2);
    assert_ne!(
        reactor.node_state(doubled.id()),
        Some(NodeState::Clean),
        "a write upstream must show up as staleness on the node itself"
    );
    assert_eq!(
        reactor.node_state(dead),
        None,
        "a node that is not live has no state to report"
    );
    reactor.flush_now();

    // `GraphSnapshot::node` — the lookup for a snapshot already taken.
    let snapshot = reactor.graph_snapshot();
    let node = snapshot
        .node(doubled.id())
        .expect("a live memo appears in a snapshot of its own reactor");
    assert_eq!(node.id, doubled.id());
    assert_eq!(node.kind, NodeKind::Memo);
    assert_eq!(
        snapshot.node(dead),
        None,
        "looking up an id that is not in the snapshot answers None, not a wrong node"
    );

    // `RecordedDependency` — what `dependencies_of` returns per edge.
    let dependencies: Vec<RecordedDependency> = reactor.dependencies_of(doubled.id());
    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0].node, source.id());
    assert_eq!(
        Some(dependencies[0].version),
        reactor.node_version(source.id()),
        "the recorded version matches the dependency's current version once it is verified"
    );

    // `NodeKind::all` — the six kinds, enumerated so a per-kind breakdown is a loop.
    let kinds: Vec<NodeKind> = NodeKind::all().collect();
    assert_eq!(kinds.len(), 6, "the documented kind list is the whole set");
    let stats = reactor.graph_stats();
    let per_kind: usize = kinds
        .iter()
        .map(|kind| stats.live_nodes_of_kind(*kind))
        .sum();
    assert_eq!(
        per_kind, stats.live_nodes,
        "the per-kind counts must account for every live node, or the breakdown lies"
    );

    // `OwnershipStats::is_empty` — nothing retained.
    assert!(
        !ownership_stats().is_empty(),
        "a live effect holds an owner frame"
    );

    // `ReactorId: Display` — printable without reaching for `get()`.
    assert_eq!(
        format!("{}", reactor.id()),
        format!("{}", reactor.id().get())
    );

    effect.dispose();
}

/// The four `DiagnosticEvent` accessors exist so a downstream crate can read the fields every
/// variant carries *without* destructuring a `#[non_exhaustive]` variant. `docs/MIGRATING-0.3.md`
/// now tells a consumer repairing their match arms to delete them in favour of these, so the
/// promise that they cover the whole stream has to hold.
#[test]
fn the_generic_accessors_cover_the_whole_event_stream() {
    let reactor = Reactor::new();
    let events = Rc::new(std::cell::RefCell::new(Vec::new()));
    let subscription = reactor.subscribe_diagnostics({
        let events = Rc::clone(&events);
        move |event| events.borrow_mut().push(event)
    });

    let source = signal_in(&reactor, 1_u32);
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
    source.set(2);
    reactor.flush_now();
    effect.dispose();
    reactor.flush_now();
    drop(subscription);

    let events = events.borrow();
    assert!(
        events.len() > 8,
        "the workload must produce a real stream, got {}",
        events.len()
    );

    let reactor_id = reactor.id();
    let mut with_node = 0_usize;
    for event in events.iter() {
        // `reactor()` is total: documented as answering for every variant.
        assert_eq!(
            event.reactor(),
            reactor_id,
            "every event is scoped to the graph it came from: {event:?}"
        );
        // `node_origin()` is documented as the origin of "the node this event concerns", so it
        // can only answer where `node()` does. (The converse does not hold: several effect
        // events carry the node without repeating its origin.)
        assert!(
            event.node_origin().is_none() || event.node().is_some(),
            "an origin without a node would name nothing: {event:?}"
        );
        if event.node().is_some() {
            with_node += 1;
        }
        // The flush boundaries are the events that concern the whole graph rather than one node.
        let is_boundary = matches!(
            event,
            DiagnosticEvent::FlushStarted { .. } | DiagnosticEvent::FlushFinished { .. }
        );
        assert_eq!(
            is_boundary,
            event.node().is_none(),
            "only the flush boundaries have no node: {event:?}"
        );
        // `flush_epoch()` is an Option, and this test deliberately does not pin which variants
        // answer — that mapping is the subject of its own review finding. What it does pin is
        // that a reported epoch is a real one.
        if let Some(epoch) = event.flush_epoch() {
            assert!(
                epoch <= reactor.graph_stats().flush_epoch,
                "an event cannot belong to a flush that has not happened: {event:?}"
            );
        }
    }
    assert!(
        with_node > 0,
        "the workload must include node-scoped events for the check above to mean anything"
    );
}
