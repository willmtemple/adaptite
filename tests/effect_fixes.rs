//! Effect re-entrancy and divergence-guard regressions found by the 0.3 release review.
//!
//! The common shape is an effect reached from inside its *own* cleanup — the window between
//! `OwnerFrame::reset` (which runs arbitrary consumer code) and the effect body. Re-entry there
//! must be deferred and re-queued exactly as it is during the body, and disposal there must stop
//! the run, not produce a ghost body run against a torn-down owner.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use adaptite::{EffectHandle, Reactor, on_cleanup, ownership_stats, signal_in};

/// A cleanup that writes one of its own effect's dependencies and then flushes synchronously
/// re-enters the effect *before* the body opens the tracked window. The run must be deferred and
/// re-queued (the 0.3 decision), not performed inline: performing it inline runs the body twice
/// and leaves the effect owning two live generations of children and cleanups.
#[test]
fn an_effect_re_entered_from_its_own_cleanup_defers_the_run() {
    let reactor = Reactor::new();
    let tick = signal_in(&reactor, 0u32);
    let gate = signal_in(&reactor, 0u32);
    let child_dep = signal_in(&reactor, 0u32);
    let log = Rc::new(RefCell::new(Vec::<String>::new()));

    let before = ownership_stats();

    let handle = reactor.effect({
        let reactor = reactor.clone();
        let tick = tick.clone();
        let gate = gate.clone();
        let child_dep = child_dep.clone();
        let log = Rc::clone(&log);
        move || {
            log.borrow_mut()
                .push(format!("outer t{} g{}", tick.get(), gate.get()));

            // A nested effect: owned by this run, disposed by the next `reset`.
            let child = reactor.effect({
                let child_dep = child_dep.clone();
                let log = Rc::clone(&log);
                move || log.borrow_mut().push(format!("child {}", child_dep.get()))
            });
            drop(child);

            on_cleanup({
                let reactor = reactor.clone();
                let gate = gate.clone();
                let log = Rc::clone(&log);
                move || {
                    log.borrow_mut().push("cleanup".into());
                    // Both halves are documented as legal on their own.
                    gate.set(10);
                    reactor.flush_now();
                }
            });
        }
    });

    reactor.flush_now();
    let settled = ownership_stats();
    assert_eq!(
        settled.owned_children - before.owned_children,
        1,
        "one nested child after the first run"
    );

    tick.set(1);
    reactor.flush_now();

    // The owed run happens — deferral is not rejection — but it happens *after* the current run,
    // with its own teardown in front of it. Before the fix the log was
    // ["outer t0 g0", "child 0", "cleanup", "outer t1 g10", "child 0", "outer t1 g10", "child 0"]:
    // two body runs with a single teardown, so the first run's child and cleanup were never
    // disposed and a second generation was stacked on top of them.
    assert_eq!(
        *log.borrow(),
        [
            "outer t0 g0",
            "child 0",
            "cleanup",
            "outer t1 g10",
            "child 0",
            "cleanup",
            "outer t1 g10",
            "child 0",
        ],
        "every body run must be preceded by its own teardown"
    );

    let after = ownership_stats();
    assert_eq!(
        after.owned_children - before.owned_children,
        1,
        "the effect must hold one generation of children, not two; log: {:?}",
        log.borrow()
    );
    assert_eq!(
        after.cleanup_registrations - before.cleanup_registrations,
        1,
        "the effect must hold one generation of cleanups, not two"
    );

    // The duplicate generation is not merely a gauge artifact: it keeps firing.
    log.borrow_mut().clear();
    child_dep.set(99);
    reactor.flush_now();
    assert_eq!(
        log.borrow().len(),
        1,
        "exactly one child should observe the write; log: {:?}",
        log.borrow()
    );

    handle.dispose();
}

/// The deferral is a deferral, not a rejection (0.3 semantics, `docs/0.3-consumer-responses.md`
/// §6d): the owed run still happens once the current run finishes, and it observes the cleanup's
/// write. A "fix" that dropped the re-entrant run instead of re-queueing it would leave the
/// effect on `(1, 10)` and never produce the third entry.
#[test]
fn the_run_deferred_from_a_cleanup_is_still_re_queued() {
    let reactor = Reactor::new();
    let tick = signal_in(&reactor, 0u32);
    let gate = signal_in(&reactor, 0u32);
    let seen = Rc::new(RefCell::new(Vec::<(u32, u32)>::new()));

    let handle = reactor.effect({
        let reactor = reactor.clone();
        let tick = tick.clone();
        let gate = gate.clone();
        let seen = Rc::clone(&seen);
        move || {
            seen.borrow_mut().push((tick.get(), gate.get()));
            on_cleanup({
                let reactor = reactor.clone();
                let gate = gate.clone();
                move || {
                    // Convergent: the second teardown's write is equality-suppressed, so the
                    // loop settles. (A cleanup that moved the value every time would be a
                    // divergent loop and is caught by the divergence guard.)
                    gate.set(10);
                    reactor.flush_now();
                }
            });
        }
    });

    reactor.flush_now();
    tick.set(1);
    reactor.flush_now();

    assert_eq!(
        *seen.borrow(),
        [(0, 0), (1, 10), (1, 10)],
        "the deferred run must still happen and observe the cleanup's write"
    );

    handle.dispose();
}

/// An effect disposed from inside its own cleanup must not go on to run its body: the owner it
/// would run under has already been torn down, so any `on_cleanup` it registered would fire
/// immediately and any nested effect it created would be disposed at once.
#[test]
fn an_effect_disposed_from_its_own_cleanup_does_not_run_its_body() {
    let reactor = Reactor::new();
    let tick = signal_in(&reactor, 0u32);
    let log = Rc::new(RefCell::new(Vec::<String>::new()));
    let slot: Rc<RefCell<Option<EffectHandle>>> = Rc::new(RefCell::new(None));

    let handle = reactor.effect({
        let tick = tick.clone();
        let log = Rc::clone(&log);
        let slot = Rc::clone(&slot);
        move || {
            let disposed = slot
                .borrow()
                .as_ref()
                .map(EffectHandle::is_disposed)
                .unwrap_or(false);
            log.borrow_mut()
                .push(format!("body t{} disposed={disposed}", tick.get()));
            on_cleanup({
                let log = Rc::clone(&log);
                let slot = Rc::clone(&slot);
                move || {
                    log.borrow_mut().push("cleanup".into());
                    if let Some(handle) = slot.borrow().as_ref() {
                        handle.dispose();
                        log.borrow_mut().push("self-disposed".into());
                    }
                }
            });
        }
    });
    *slot.borrow_mut() = Some(handle.clone());

    reactor.flush_now();
    tick.set(1);
    reactor.flush_now();

    assert_eq!(
        *log.borrow(),
        ["body t0 disposed=false", "cleanup", "self-disposed"],
        "the body must not run after the cleanup disposed the effect"
    );
    assert!(handle.is_disposed());
}

/// The divergence guard counts runs per *drain*, not per flush epoch. An effect that writes a
/// dependency and then calls `flush_now` opens a new flush epoch on every run; if the guard read
/// `flush_epoch` its counter would reset every time and a divergent loop would never terminate.
///
/// The effect below converges after `MAX_RUNS_PER_FLUSH` runs, so a guard that resets per flush
/// leaves the test green-but-wrong rather than hanging: it finishes without a panic.
#[test]
fn the_divergence_guard_counts_runs_per_drain_not_per_flush_epoch() {
    let reactor = Reactor::new();
    let value = signal_in(&reactor, 0u32);
    let runs = Rc::new(Cell::new(0u32));

    // Well above the guard's limit of 100, so a working guard fires first; low enough that a
    // broken guard terminates and fails the assertion instead of hanging the suite.
    const CONVERGES_AT: u32 = 150;

    let handle = reactor.effect({
        let reactor = reactor.clone();
        let value = value.clone();
        let runs = Rc::clone(&runs);
        move || {
            runs.set(runs.get() + 1);
            let seen = value.get();
            if seen < CONVERGES_AT {
                value.set(seen + 1);
                // A re-entrant flush: a new flush epoch, the same drain.
                reactor.flush_now();
            }
        }
    });

    let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));

    let payload = match result {
        Ok(()) => panic!(
            "the divergence guard did not fire: the effect ran {} times inside one drain, each \
             run opening a nested flush epoch",
            runs.get()
        ),
        Err(payload) => payload,
    };
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .expect("panic payload should be a formatted string");
    assert!(
        message.contains("divergent reactive feedback loop"),
        "got: {message}"
    );
    assert!(
        runs.get() < CONVERGES_AT,
        "the guard should fire before the loop converges, ran {} times",
        runs.get()
    );

    handle.dispose();
}
