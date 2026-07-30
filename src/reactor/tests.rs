//! Tests for the reactor core.
//!
//! A child module rather than an inline `mod tests`, so `reactor.rs` stays inside the
//! 2000-line budget `cop-checks/module-size.cop` enforces. Child modules see their parent's
//! private items, so nothing here needed widening to move.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use runite::{queue_macrotask, run};

use super::{Reactor, current};
use crate::NodeKind;

#[test]
fn current_reactor_is_thread_local_singleton() {
    let one = current();
    let two = current();
    assert!(Rc::ptr_eq(&one.inner, &two.inner));
}

#[test]
fn try_current_reports_absence_instead_of_installing_a_reactor() {
    // The test harness gives each test a fresh thread, so nothing is installed yet.
    assert!(
        super::try_current().is_none(),
        "try_current must not install a reactor"
    );
    assert!(
        super::try_current().is_none(),
        "and must still report absence after being asked once"
    );

    let installed = current();
    let observed = super::try_current().expect("current installed a default");
    assert_eq!(observed.id(), installed.id());
}

#[test]
fn entering_anchors_the_reactor_as_the_thread_default() {
    let reactor = Reactor::new();
    let expected = reactor.id();
    let guard = reactor.enter();

    // Drop every other handle: the guard alone must keep this reactor current. Without the
    // strong anchor, `current()` here would install a fresh, unrelated graph.
    drop(reactor);

    assert_eq!(
        current().id(),
        expected,
        "the entered reactor stays current with no other handle alive"
    );

    drop(guard);
    assert!(
        super::try_current().is_none(),
        "dropping the guard restores the absent default"
    );
}

#[test]
fn entering_nests_and_restores_the_previous_default() {
    let outer = Reactor::new();
    let inner = Reactor::new();
    let (outer_id, inner_id) = (outer.id(), inner.id());

    let outer_guard = outer.enter();
    assert_eq!(current().id(), outer_id);

    let inner_guard = inner.enter();
    assert_eq!(current().id(), inner_id);

    drop(inner_guard);
    assert_eq!(
        current().id(),
        outer_id,
        "leaving the inner reactor restores the outer one"
    );

    drop(outer_guard);
    assert!(super::try_current().is_none());
}

#[test]
fn an_expired_default_is_replaced_by_an_unrelated_reactor() {
    // This is the failure mode `current()` warns about, pinned so the warning keeps
    // describing something real: nodes created on either side of the expiry cannot interact.
    let first = current().id();
    // No node, handle, or guard survives, so the weak cache expires.
    let second = current().id();

    assert_ne!(
        first, second,
        "an unanchored default is replaced once nothing keeps it alive"
    );

    // Holding any handle is enough to keep it stable.
    let held = current();
    assert_eq!(current().id(), held.id());
}

#[test]
fn observe_records_dependency_edges_with_versions() {
    let reactor = Reactor::new();
    let observer = reactor.allocate_node(NodeKind::Source);
    let observable = reactor.allocate_node(NodeKind::Source);
    reactor.trigger(observable);

    reactor.run_in_context(observer, || {
        reactor
            .try_observe(observable)
            .expect("should not detect cycle")
    });

    let recorded = reactor.dependencies_of(observer);
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].node, observable);
    assert_eq!(recorded[0].version, reactor.version(observable));
    assert_eq!(
        reactor.inner.dependents.borrow().get(&observable),
        Some(&[observer].into_iter().collect())
    );
}

#[test]
fn the_public_queries_answer_why_did_this_update() {
    use crate::{memo_in, signal_in};

    let reactor = Reactor::new();
    let left = signal_in(&reactor, 1_u32);
    let right = signal_in(&reactor, 10_u32);
    let total = memo_in(&reactor, {
        let left = left.clone();
        let right = right.clone();
        move || left.get() + right.get()
    });
    assert_eq!(total.get(), 11);

    // Both inputs are observed once, by the memo.
    assert_eq!(reactor.observer_count(left.id()), 1);
    assert_eq!(reactor.observer_count(right.id()), 1);
    assert_eq!(reactor.observers_of(left.id()), vec![total.id()]);

    // Snapshot the edges as the memo last saw them, then move one input.
    let recorded = reactor.dependencies_of(total.id());
    assert_eq!(recorded.len(), 2);
    right.set(20);

    // The culprit is the dependency whose live version no longer matches the recorded one.
    // That comparison is the whole "why did this update" mechanism, and it needs both
    // `dependencies_of` and `node_version` to be reachable.
    let moved = recorded
        .iter()
        .filter(|edge| reactor.node_version(edge.node) != Some(edge.version))
        .map(|edge| edge.node)
        .collect::<Vec<_>>();
    assert_eq!(moved, vec![right.id()]);
}

#[test]
fn observer_counts_are_late_but_never_early() {
    use crate::{signal_in, source_in};

    let reactor = Reactor::new();
    let toggle = signal_in(&reactor, true);
    let watched = source_in(&reactor);

    let effect = reactor.effect({
        let toggle = toggle.clone();
        let watched = watched.clone();
        let reactor = reactor.clone();
        move || {
            if toggle.get() {
                reactor.observe(watched.id());
            }
        }
    });
    reactor.flush_now();
    assert_eq!(reactor.observer_count(watched.id()), 1);

    // Stopping the read does not retract the edge until the observer actually re-runs —
    // documented as "late, never early", and the property GC sweeps depend on.
    toggle.set(false);
    assert_eq!(
        reactor.observer_count(watched.id()),
        1,
        "the edge survives until the observer re-runs"
    );
    reactor.flush_now();
    assert_eq!(reactor.observer_count(watched.id()), 0);
    assert!(reactor.observers_of(watched.id()).is_empty());

    // Disposal retracts the observer's own edges, which is what a leak test watches.
    toggle.set(true);
    reactor.flush_now();
    assert_eq!(reactor.observer_count(watched.id()), 1);
    effect.dispose();
    assert_eq!(reactor.observer_count(watched.id()), 0);
}

#[test]
fn node_origin_reports_the_creation_site_and_forgets_a_disposed_node() {
    use crate::signal_in;

    let reactor = Reactor::new();
    let line = line!() + 1;
    let value = signal_in(&reactor, 0_u32);

    let origin = reactor
        .node_origin(value.id())
        .expect("a live node reports where it was created");
    assert_eq!(origin.line(), line);
    // `file!()` rather than a literal: origins move with the file, and a test that
    // hard-codes a name breaks on every refactor for no benefit.
    assert_eq!(origin.file(), file!());
    assert_eq!(reactor.node_version(value.id()), Some(0));

    let id = value.id();
    drop(value);
    assert_eq!(
        reactor.node_origin(id),
        None,
        "a disposed node is absent, not misattributed — ids are never reused"
    );
    assert_eq!(reactor.node_version(id), None);
}

#[test]
fn a_nested_flush_takes_a_new_epoch_but_stays_in_the_same_drain() {
    // Two identities, deliberately: `flush_epoch` separates diagnostic totals so a capture
    // can be summed, while `drain_epoch` identifies the whole logical drain and is what the
    // divergence guard counts against. Collapsing them either way breaks something — sharing
    // the epoch loses per-flush attribution, and bumping the drain lets an effect that
    // re-flushes reset the guard on every run.
    let reactor = Reactor::new();
    let seen = Rc::new(RefCell::new(Vec::new()));

    reactor.schedule({
        let reactor = reactor.clone();
        let seen = Rc::clone(&seen);
        move || {
            seen.borrow_mut()
                .push(("outer", reactor.drain_epoch(), reactor.flush_epoch()));
            // Give the nested drain something to find: an empty drain is not a flush.
            reactor.schedule({
                let reactor = reactor.clone();
                let seen = Rc::clone(&seen);
                move || {
                    seen.borrow_mut().push((
                        "nested",
                        reactor.drain_epoch(),
                        reactor.flush_epoch(),
                    ));
                }
            });
            reactor.flush_now();
        }
    });
    reactor.flush_now();

    let seen = seen.borrow();
    assert_eq!(seen.len(), 2);
    let (_, outer_drain, outer_flush) = seen[0];
    let (_, nested_drain, nested_flush) = seen[1];
    assert_eq!(
        nested_drain, outer_drain,
        "a re-entrant flush stays inside the enclosing drain"
    );
    assert_ne!(
        nested_flush, outer_flush,
        "but takes its own diagnostic epoch so its totals are separable"
    );
}

#[test]
fn a_computation_that_first_runs_untracked_still_records_its_dependencies() {
    use crate::{memo_in, signal_in, untrack, watch_in};
    use std::cell::RefCell;

    // `untrack` means "do not record this read for whoever is currently observing". It must not
    // leak into a computed node that happens to recompute inside it: such a node would record
    // zero dependencies, settle clean, and never be invalidated again — permanently and silently
    // stale, for every reader, not just the untracked one.
    let reactor = Reactor::new();
    let base = signal_in(&reactor, 1_u64);
    let doubled = memo_in(&reactor, {
        let base = base.clone();
        move || base.get() * 2
    });

    untrack(|| assert_eq!(doubled.get(), 2));
    assert_eq!(
        reactor.dependency_count(doubled.id()),
        1,
        "the memo's own read of `base` belongs to the memo, untracked caller or not"
    );

    base.set(50);
    assert_eq!(doubled.get(), 100, "the memo must still be reactive");

    // The path a consumer actually takes: handlers run untracked by design, so any of them may be
    // the first to touch a stale memo. `watch`, `Event::on`, cleanups, comparators and `Resource`
    // fetch closures are all in this position.
    let tick = signal_in(&reactor, 0_u64);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let watcher = watch_in(
        &reactor,
        {
            let tick = tick.clone();
            move || tick.get()
        },
        {
            let doubled = doubled.clone();
            let seen = Rc::clone(&seen);
            move |_, _| seen.borrow_mut().push(doubled.get())
        },
    );
    reactor.flush_now();

    base.set(21);
    tick.set(1);
    reactor.flush_now();
    assert_eq!(
        *seen.borrow(),
        [100, 42],
        "a memo first read from an untracked handler must keep updating"
    );

    watcher.dispose();
}

#[test]
fn a_computation_that_reads_itself_is_refused_by_the_cycle_detector_in_every_build() {
    use crate::{memo_in, signal_in};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    // Re-entry cannot be tracked coherently: the inner run clears the dependency set the outer
    // run is still recording.
    //
    // This test used to be named for the `assert!` in `run_in_context` and to accept either
    // message, which hid the fact that it never reached that assert. On the *read* path — the one
    // a consumer takes — the refusal comes from the cycle detector, which `refresh_node` and
    // `try_observe` both consult before `run_in_context` is entered, and which panics
    // unconditionally in every profile. That is the mechanism behind the release note, so this
    // asserts on it exactly. The backstop assert has its own test below.
    let reactor = Reactor::new();
    let source = signal_in(&reactor, 1_u64);
    let reader: Rc<RefCell<Option<crate::Memo<u64>>>> = Rc::new(RefCell::new(None));

    let memo = memo_in(&reactor, {
        let source = source.clone();
        let reader = Rc::clone(&reader);
        move || {
            let value = source.get();
            // Read itself from inside its own computation.
            if let Some(me) = reader.borrow().as_ref() {
                let _ = me.peek();
            }
            value
        }
    });
    *reader.borrow_mut() = Some(memo.clone());

    let result = catch_unwind(AssertUnwindSafe(|| memo.get()));
    let payload = result.expect_err("re-entry must be refused");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default();
    assert!(
        message.contains("reactive cycle detected"),
        "the refusal should explain itself, got: {message}"
    );

    *reader.borrow_mut() = None;
}

#[test]
fn re_entering_run_in_context_directly_is_refused_in_every_build() {
    // The backstop the test above does *not* reach. `run_in_context` is public, so a consumer
    // building a custom primitive can re-enter a tracking scope without going through a read,
    // which is the one route the cycle detector does not stand in front of. Without the assert
    // the inner entry falls through to `clear_observer_dependencies` and wipes the dependency set
    // the outer run is still recording, and the node emerges with whatever subset of its inputs
    // it happens to re-read afterwards — silent, and shape-dependent.
    //
    // This was a `debug_assert`, so release builds fell through; the `insert` it checks already
    // ran in release, which is what makes refusing in every build free. Nothing pinned that, and
    // the whole assert could be deleted with the suite green.
    let reactor = Reactor::new();
    let node = reactor.allocate_node(NodeKind::Source);

    let payload = catch_unwind(AssertUnwindSafe(|| {
        reactor.run_in_context(node, || reactor.run_in_context(node, || {}));
    }))
    .expect_err("re-entering a running computation must be refused");

    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default();
    assert!(
        message.contains("re-entered itself"),
        "the refusal should name the re-entry, got: {message}"
    );
}

#[test]
fn propagation_returns_its_scratch_buffer_while_a_subscription_is_installed() {
    use crate::{memo_in, signal_in};

    // 0.3's headline performance claim is that a write no longer allocates as it propagates, and
    // the mechanism is the scratch pool. `mark_dependents` returned its buffer on the dormant
    // path and dropped it on the diagnostics-active path, so the pool emptied permanently the
    // first time anything subscribed and every later mark step allocated a fresh `Vec` — one per
    // node per propagation step. Nothing in the suite could see it: the benches never subscribe.
    let reactor = Reactor::new();
    let head = signal_in(&reactor, 0_u64);
    let mut tail = memo_in(&reactor, {
        let head = head.clone();
        move || head.get() + 1
    });
    for _ in 0..3 {
        tail = memo_in(&reactor, {
            let previous = tail.clone();
            move || previous.get() + 1
        });
    }
    assert_eq!(tail.get(), 4, "a depth-4 chain, so propagation recurses");

    let _subscription = reactor.subscribe_diagnostics(|_| {});

    // One warm write to bring the pool to its steady state. Nothing is read afterwards: memos are
    // lazy, so this exercises mark propagation and nothing else.
    head.set(1);
    let pooled = reactor.node_scratch_pool_len();
    assert!(
        pooled > 0,
        "propagation must hand its scratch buffers back to the pool, not drop them"
    );

    for _ in 0..10 {
        head.set(head.get() + 1);
        assert_eq!(
            reactor.node_scratch_pool_len(),
            pooled,
            "a subscribed propagation must be pool-neutral, exactly like a dormant one"
        );
    }
}

#[test]
fn cycle_detection_panics_with_path_and_origins() {
    let reactor = Reactor::new();
    let a = reactor.allocate_node(NodeKind::Source);
    let b = reactor.allocate_node(NodeKind::Source);

    let panic = catch_unwind(AssertUnwindSafe(|| {
        reactor.run_in_context(a, || {
            reactor.observe(b);
            reactor.run_in_context(b, || {
                reactor.observe(a);
            });
        });
    }))
    .expect_err("cycle should panic");

    let Some(cycle_error) = panic.downcast_ref::<String>() else {
        panic!("panic should be a string");
    };

    assert!(
        cycle_error.contains("reactive cycle detected"),
        "panic should indicate cycle detected"
    );

    assert!(
        cycle_error.contains("1 (created at")
            && cycle_error.contains("-> 2 (created at")
            && cycle_error.contains(file!()),
        "panic should include the cycle path with node origins, got: {cycle_error}"
    );
}

#[test]
fn scheduled_jobs_flush_on_runtime_microtask_queue() {
    let observed = Rc::new(Cell::new(0usize));

    queue_macrotask({
        let observed = Rc::clone(&observed);
        move || {
            let reactor = Reactor::new();
            reactor.schedule({
                let observed = Rc::clone(&observed);
                move || observed.set(1)
            });
            assert_eq!(observed.get(), 0);
        }
    });

    run();

    assert_eq!(observed.get(), 1);
}

#[test]
fn graph_survives_dropping_the_reactor_handle() {
    let seen = Rc::new(std::cell::RefCell::new(Vec::new()));
    let keep_alive = Rc::new(std::cell::RefCell::new(None::<crate::EffectHandle>));

    queue_macrotask({
        let seen = Rc::clone(&seen);
        let keep_alive = Rc::clone(&keep_alive);
        move || {
            let reactor = Reactor::new();
            let source = crate::signal_in(&reactor, 1usize);
            let effect = reactor.effect({
                let seen = Rc::clone(&seen);
                let source = source.clone();
                move || seen.borrow_mut().push(source.get())
            });
            *keep_alive.borrow_mut() = Some(effect);

            // Nodes hold the reactor alive; the user's handle is not load-bearing.
            drop(reactor);

            runite::queue_macrotask(move || {
                source.set(2);
            });
        }
    });

    run();

    assert_eq!(&*seen.borrow(), &[1, 2]);
}

#[test]
fn flush_recovers_after_a_panicking_job() {
    let observed = Rc::new(Cell::new(0usize));

    queue_macrotask({
        let observed = Rc::clone(&observed);
        move || {
            let reactor = Reactor::new();
            reactor.schedule(|| panic!("job panics"));
            // Swallow the panic that propagates out of the microtask flush so the test can
            // observe the reactor's recovery.
            let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
            assert!(result.is_err(), "flush should propagate the job panic");

            reactor.schedule({
                let observed = Rc::clone(&observed);
                move || observed.set(1)
            });
            reactor.flush_now();
        }
    });

    run();

    assert_eq!(observed.get(), 1);
}
