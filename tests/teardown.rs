//! Teardown is total: every cleanup and every child gets an attempt, even when one panics.
//!
//! Teardown is what releases subscriptions, timers, handles and child subtrees. A single
//! misbehaving cleanup stranding the rest produces a leak with no error attached to it, which is
//! the hardest kind to find — so the contract is that nothing is skipped, the first panic is the
//! one that propagates, and teardown that begins during an existing unwind never aborts.
//!
//! Regression coverage for #28.

use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use adaptite::{Reactor, on_cleanup, ownership_stats, scope, signal_in};

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("<non-string payload>")
}

#[test]
fn a_panicking_cleanup_does_not_strand_its_siblings() {
    let log = Rc::new(RefCell::new(Vec::new()));

    let (handle, ()) = scope({
        let log = Rc::clone(&log);
        move || {
            // Registered first, torn down last.
            on_cleanup({
                let log = Rc::clone(&log);
                move || log.borrow_mut().push("first")
            });
            on_cleanup({
                let log = Rc::clone(&log);
                move || {
                    log.borrow_mut().push("middle");
                    panic!("middle cleanup failed");
                }
            });
            on_cleanup({
                let log = Rc::clone(&log);
                move || log.borrow_mut().push("last")
            });
        }
    });

    let result = catch_unwind(AssertUnwindSafe(|| handle.dispose()));
    let payload = result.expect_err("the panic reaches the caller");
    assert_eq!(panic_message(&payload), "middle cleanup failed");

    // Reverse registration order, and nothing skipped: the cleanup registered *before* the
    // panicking one still ran.
    assert_eq!(*log.borrow(), ["last", "middle", "first"]);
}

#[test]
fn a_panicking_cleanup_does_not_strand_the_owners_children() {
    let reactor = Reactor::new();
    let child_disposed = Rc::new(RefCell::new(false));

    let (handle, ()) = scope({
        let reactor = reactor.clone();
        let child_disposed = Rc::clone(&child_disposed);
        move || {
            // An effect owned by this scope. Its cleanup records that it was torn down.
            reactor
                .effect(move || {
                    let child_disposed = Rc::clone(&child_disposed);
                    on_cleanup(move || *child_disposed.borrow_mut() = true);
                })
                .leak();
            on_cleanup(|| panic!("cleanup failed"));
        }
    });
    reactor.flush_now();

    let result = catch_unwind(AssertUnwindSafe(|| handle.dispose()));
    assert!(result.is_err());

    // Cleanups run before children are disposed, so before the fix a panicking cleanup meant the
    // children were never even *taken*, let alone disposed. This is the half of #28 that leaked
    // whole subtrees.
    assert!(
        *child_disposed.borrow(),
        "the owned effect must still be torn down"
    );
}

#[test]
fn the_first_panic_is_the_one_that_propagates() {
    let (handle, ()) = scope(|| {
        on_cleanup(|| panic!("third"));
        on_cleanup(|| panic!("second"));
        on_cleanup(|| panic!("first"));
    });

    let payload = catch_unwind(AssertUnwindSafe(|| handle.dispose()))
        .expect_err("teardown failed, so the caller hears about it");

    // Teardown is reverse-order, so "first" is registered last and torn down first. The earliest
    // failure is the one preserved — later ones are commonly caused by it.
    assert_eq!(panic_message(&payload), "first");
}

#[test]
fn teardown_during_an_existing_unwind_does_not_abort() {
    // If adaptite re-raised a cleanup panic while the thread was already unwinding, the process
    // would abort and this test would take the whole suite with it. Reaching the assertions at
    // all is a meaningful part of what is being tested.
    let cleanup_ran = Rc::new(RefCell::new(false));

    let result = catch_unwind(AssertUnwindSafe({
        let cleanup_ran = Rc::clone(&cleanup_ran);
        move || {
            let (_handle, ()) = scope({
                let cleanup_ran = Rc::clone(&cleanup_ran);
                move || {
                    on_cleanup(move || {
                        *cleanup_ran.borrow_mut() = true;
                        panic!("cleanup failed during unwind");
                    });
                }
            });
            // The handle drops while this panic unwinds, so teardown starts mid-unwind.
            panic!("original failure");
        }
    }));

    let payload = result.expect_err("the original panic propagates");
    assert_eq!(
        panic_message(&payload),
        "original failure",
        "the cleanup's panic must not displace the one already in flight"
    );
    assert!(
        *cleanup_ran.borrow(),
        "and teardown still happened rather than being skipped"
    );
}

#[test]
fn a_panicking_cleanup_still_releases_the_ownership_gauges() {
    let before = ownership_stats();

    let (handle, ()) = scope(|| {
        on_cleanup(|| {});
        on_cleanup(|| panic!("cleanup failed"));
        on_cleanup(|| {});
    });

    let _ = catch_unwind(AssertUnwindSafe(|| handle.dispose()));
    drop(handle);

    let after = ownership_stats();
    assert_eq!(
        after.cleanup_registrations, before.cleanup_registrations,
        "every registration was released even though teardown failed"
    );
    assert_eq!(after.cleanups_run - before.cleanups_run, 3);
    assert_eq!(after.owned_children, before.owned_children);
    adaptite::debug_assert_ownership_consistent();
}

#[test]
fn an_effect_that_panics_in_cleanup_still_re_runs_cleanly_afterwards() {
    // The teardown path is reached on every effect re-run, not only on disposal, so a bad cleanup
    // must not wedge the effect.
    let reactor = Reactor::new();
    let value = signal_in(&reactor, 0_u32);
    let runs = Rc::new(RefCell::new(Vec::new()));

    let effect = reactor.effect({
        let value = value.clone();
        let runs = Rc::clone(&runs);
        move || {
            let seen = value.get();
            runs.borrow_mut().push(seen);
            on_cleanup(move || {
                if seen == 1 {
                    panic!("cleanup for run 1 failed");
                }
            });
        }
    });
    reactor.flush_now();
    assert_eq!(*runs.borrow(), [0]);

    value.set(1);
    reactor.flush_now();
    assert_eq!(*runs.borrow(), [0, 1]);

    // Re-running now tears down run 1's cleanup, which panics.
    value.set(2);
    let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
    assert!(result.is_err(), "the cleanup panic surfaces");

    // The effect recovers: a later change still re-runs it.
    value.set(3);
    reactor.flush_now();
    assert_eq!(
        runs.borrow().last().copied(),
        Some(3),
        "a failed teardown must not wedge the effect"
    );

    effect.dispose();
}
