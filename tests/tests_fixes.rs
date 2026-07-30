//! Repairs to the test suite's own gate, from the 0.3 release-readiness review.
//!
//! # The defect these tests exist for
//!
//! runite wraps every scheduled task in `catch_unwind`
//! (`runite-0.3.0/src/platform/runtime_shared/scheduler.rs:862` for macrotasks,
//! `future_task.rs:122` for spawned futures). A failing assertion inside a `queue_macrotask` or
//! `spawn` closure therefore never reaches the test harness: the panic is swallowed, `run()`
//! returns normally, and the test passes no matter what it asserted. The review found five shipped
//! `#[test]` functions with *every* assertion inside such a closure, so none of them could fail.
//!
//! [`a_panic_inside_a_macrotask_does_not_reach_the_test_harness`] pins that mechanism, so the
//! record-then-assert-after-`run()` idiom used throughout this suite has a machine-checked reason
//! rather than a comment. The remaining tests are enforceable integration-level cover for
//! behaviours whose only existing tests were among the vacuous five — all four of those live in
//! `#[cfg(test)]` modules inside `src/`, which this file cannot edit.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use adaptite::{Reactor, event_in, memo_by_in, memo_in, on_in, signal_in};
use runite::{queue_macrotask, run};

/// The mechanism that made five shipped tests vacuous, stated as an executable fact.
///
/// If runite ever starts propagating a task panic out of `run()`, this test fails and the whole
/// record-then-assert-after-`run()` dance in this suite can be simplified. Until then it cannot.
#[test]
fn a_panic_inside_a_macrotask_does_not_reach_the_test_harness() {
    let started = Rc::new(Cell::new(false));
    let finished = Rc::new(Cell::new(false));

    queue_macrotask({
        let started = Rc::clone(&started);
        let finished = Rc::clone(&finished);
        move || {
            started.set(true);
            assert_eq!(1, 2, "deliberately false, and deliberately never observed");
            finished.set(true);
        }
    });

    // Reaching this line at all is half the result: the panic above did not unwind out of `run()`.
    run();

    assert!(started.get(), "the macrotask must actually have run");
    assert!(
        !finished.get(),
        "the macrotask must have died at the false assertion — if it did not, this test is not \
         measuring what it claims"
    );
}

/// Cover for `src/effect.rs::effect_recovers_after_a_panicking_dependency_verification`, whose
/// every assertion sits inside a `queue_macrotask` closure and so cannot fail.
///
/// A memo that panics while an effect is *verifying* its dependencies leaves the memo dirty. If
/// the effect were left clean, the still-dirty memo would no longer propagate marks and the effect
/// would never be scheduled again — a permanently wedged effect with no error attached to it.
#[test]
fn an_effect_recovers_after_a_dependency_verification_panics() {
    let reactor = Reactor::new();
    let seen = Rc::new(RefCell::new(Vec::new()));

    let source = signal_in(&reactor, 1i32);
    let doubled = memo_in(&reactor, {
        let source = source.clone();
        move || {
            let value = source.get();
            assert!(value != 13, "unlucky number");
            value * 2
        }
    });
    let effect = reactor.effect({
        let doubled = doubled.clone();
        let seen = Rc::clone(&seen);
        move || seen.borrow_mut().push(doubled.get())
    });

    reactor.flush_now();
    assert_eq!(&*seen.borrow(), &[2]);

    source.set(13);
    let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
    assert!(result.is_err(), "verification should propagate the panic");

    source.set(7);
    reactor.flush_now();
    assert_eq!(
        &*seen.borrow(),
        &[2, 14],
        "a panicking verification must not wedge the effect"
    );

    effect.dispose();
}

/// Cover for `src/effect.rs::comparator_reads_do_not_become_effect_dependencies`, likewise entirely
/// inside a macrotask closure.
///
/// A `memo_by` comparator runs while the memo refreshes, which can happen inside an effect's body.
/// Reactive reads the comparator makes must not be attributed to whoever happened to trigger the
/// refresh.
#[test]
fn comparator_reads_do_not_become_the_reading_effects_dependencies() {
    let reactor = Reactor::new();
    let runs = Rc::new(Cell::new(0usize));

    let tuning = signal_in(&reactor, 0i32);
    let source = signal_in(&reactor, 1i32);

    let value = memo_by_in(
        &reactor,
        {
            let tuning = tuning.clone();
            move |old: &i32, new: &i32| {
                let _ = tuning.get();
                old == new
            }
        },
        {
            let source = source.clone();
            move || source.get()
        },
    );

    let effect = reactor.effect({
        let value = value.clone();
        let source = source.clone();
        let runs = Rc::clone(&runs);
        move || {
            runs.set(runs.get() + 1);
            let _ = source.get();
            let _ = value.get();
        }
    });

    reactor.flush_now();
    assert_eq!(runs.get(), 1);

    // Forces the memo to refresh — and so the comparator to run — inside the effect's body.
    source.set(2);
    reactor.flush_now();
    assert_eq!(runs.get(), 2);

    // If the comparator's read had been tracked, this write would re-run the effect.
    tuning.set(99);
    reactor.flush_now();
    assert_eq!(runs.get(), 2, "comparator reads must not be tracked");

    effect.dispose();
}

/// Cover for `src/event.rs::on_handlers_run_untracked`, likewise entirely inside a macrotask
/// closure.
#[test]
fn event_handlers_do_not_subscribe_to_what_they_read() {
    let reactor = Reactor::new();
    let drains = Rc::new(Cell::new(0usize));

    let event = event_in::<usize>(&reactor);
    let unrelated = signal_in(&reactor, 0usize);

    let subscription = on_in(&reactor, &event, {
        let drains = Rc::clone(&drains);
        let unrelated = unrelated.clone();
        move |_value| {
            // The handler reads a signal it must not subscribe to.
            let _ = unrelated.get();
            drains.set(drains.get() + 1);
        }
    });

    event.emit(1);
    reactor.flush_now();
    assert_eq!(drains.get(), 1);

    // If the handler's read had been tracked, this write would re-run the draining effect.
    unrelated.set(99);
    reactor.flush_now();
    assert_eq!(drains.get(), 1, "handler reads must not be tracked");

    event.emit(2);
    reactor.flush_now();
    assert_eq!(drains.get(), 2, "and delivery still works afterwards");

    drop(subscription);
}

/// Cover for `src/watch.rs::watch_source_is_equality_gated_and_handler_is_untracked`, likewise
/// entirely inside a macrotask closure. Two independent guarantees in one test, matching the
/// original: the tracked source is equality-gated, and the handler's reads are untracked.
#[test]
fn watch_is_equality_gated_on_its_source_and_its_handler_is_untracked() {
    let reactor = Reactor::new();
    let runs = Rc::new(Cell::new(0usize));

    let numerator = signal_in(&reactor, 4i32);
    let side_input = signal_in(&reactor, 0i32);

    let handle = reactor.watch(
        {
            // The tracked source: parity of the numerator.
            let numerator = numerator.clone();
            move || numerator.get() % 2
        },
        {
            // The untracked handler reads a signal it must not subscribe to.
            let runs = Rc::clone(&runs);
            let side_input = side_input.clone();
            move |_new: &i32, _old: Option<&i32>| {
                let _ = side_input.get();
                runs.set(runs.get() + 1);
            }
        },
    );

    reactor.flush_now();
    assert_eq!(runs.get(), 1, "immediate first invocation");

    // The source recomputes but its value (parity) is unchanged: no invocation.
    numerator.set(6);
    reactor.flush_now();
    assert_eq!(
        runs.get(),
        1,
        "an unchanged source must not fire the handler"
    );

    // The handler read this, but did not subscribe to it: no invocation.
    side_input.set(99);
    reactor.flush_now();
    assert_eq!(runs.get(), 1, "handler reads must not be tracked");

    // A genuine change in the tracked value does fire it.
    numerator.set(7);
    reactor.flush_now();
    assert_eq!(runs.get(), 2, "a changed source must fire the handler");

    handle.dispose();
}
