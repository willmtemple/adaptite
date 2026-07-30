//! Re-entrancy: a nested flush that reaches a computation which is already running.
//!
//! Two documented-legal things compose into re-entry — an effect may write state it depends on
//! when the loop converges, and `flush_now` exists for hosts that need synchronous propagation.
//! Doing both re-enters the effect. Re-entry cannot be tracked (the inner run clears the
//! dependency set the outer run is still recording), so it is deferred, not performed and not
//! rejected. Also covers diagnostic subscribers that re-enter the reactor.

use adaptite::Reactor;

/// An effect that writes a dependency and then forces a synchronous flush combines two things the
/// README documents as legal on their own. The inner flush reaches the effect while it is still
/// running; re-entering it would clear the dependency set the outer run is recording, so the run
/// must be deferred rather than re-entered — and must still happen.
#[test]
fn a_convergent_self_write_followed_by_a_synchronous_flush_converges() {
    use std::cell::Cell;
    use std::rc::Rc;

    let reactor = Reactor::new();
    let value = adaptite::signal_in(&reactor, 0u32);
    let runs = Rc::new(Cell::new(0u32));

    let handle = reactor.effect({
        let value = value.clone();
        let reactor = reactor.clone();
        let runs = Rc::clone(&runs);
        move || {
            runs.set(runs.get() + 1);
            let seen = value.get();
            if seen < 2 {
                value.set(seen + 1);
            }
            // Host integrations do this to propagate synchronously.
            reactor.flush_now();
        }
    });

    reactor.flush_now();

    assert_eq!(value.get(), 2, "the feedback loop should have converged");
    assert_eq!(
        runs.get(),
        3,
        "each write should produce exactly one further run"
    );
    assert_eq!(
        reactor.dependency_count(handle.id()),
        1,
        "the deferred run must not have clobbered the dependency the outer run recorded"
    );
    drop(handle);
}

/// `FlushStarted` is emitted while the reactor holds no borrow a subscriber could collide with.
/// Scheduling reactive work from a diagnostic subscriber is reasonable — it must not produce a
/// bare `BorrowMutError` from inside adaptite.
///
/// The write has to reach the *effect queue* to exercise this: a write with no dependents never
/// calls `pending_jobs.borrow_mut()` and so never collides.
#[test]
fn a_diagnostic_subscriber_may_schedule_work_while_a_flush_is_opening() {
    use adaptite::DiagnosticEvent;
    use std::cell::Cell;
    use std::rc::Rc;

    let reactor = Reactor::new();
    let observed = adaptite::signal_in(&reactor, 0u32);
    let trigger = adaptite::signal_in(&reactor, 0u32);
    let scheduled = Rc::new(Cell::new(0u32));

    // Gives the subscriber's write somewhere to propagate, so it reaches the job queue.
    let observer = reactor.effect({
        let observed = observed.clone();
        move || {
            observed.get();
        }
    });
    let driver = reactor.effect({
        let trigger = trigger.clone();
        move || {
            trigger.get();
        }
    });
    reactor.flush_now();

    let _subscription = reactor.subscribe_diagnostics({
        let observed = observed.clone();
        let scheduled = Rc::clone(&scheduled);
        move |event| {
            if matches!(event, DiagnosticEvent::FlushStarted { .. }) && scheduled.get() < 2 {
                scheduled.set(scheduled.get() + 1);
                // Marks `observer`, which schedules it — `pending_jobs.borrow_mut()`.
                observed.set(observed.get() + 1);
            }
        }
    });

    trigger.set(1);
    reactor.flush_now();

    assert!(
        scheduled.get() > 0,
        "the subscriber should have observed a flush opening"
    );
    drop((observer, driver));
}

/// A closure passed to `with` holds a borrow of the cached value for its whole body, so
/// invalidating that node and reading it back from inside the closure cannot work. That is
/// documented — but it used to surface as a bare `RefCell already borrowed`, naming neither the
/// node, its origin, nor `with`. In a release build that is close to nothing to go on.
#[test]
fn a_thunk_that_recomputes_while_borrowed_names_itself() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let reactor = Reactor::new();
    let source = adaptite::signal_in(&reactor, 0u32);
    let thunk = adaptite::thunk_in(&reactor, {
        let source = source.clone();
        move || source.get()
    });
    let _ = thunk.get();

    let payload = catch_unwind(AssertUnwindSafe(|| {
        thunk.with(|_value| {
            source.set(1);
            let _ = thunk.get();
        });
    }))
    .expect_err("invalidating and re-reading inside `with` should panic");

    let message = payload
        .downcast_ref::<String>()
        .expect("the diagnosis is formatted, so the payload is a String");
    assert!(
        // `file!()` rather than a literal path: `Location::file` renders in the host's own
        // form, so a hard-coded `tests/reentrancy.rs` fails on Windows against the identical
        // and entirely correct `tests\reentrancy.rs`.
        message.contains("thunk created at") && message.contains(file!()),
        "the message should name the thunk and its origin, got: {message}"
    );
    assert!(
        message.contains("`with`"),
        "the message should name the API that holds the borrow, got: {message}"
    );
}

/// The same for `Memo`, which stores its value through a different path.
#[test]
fn a_memo_that_recomputes_while_borrowed_names_itself() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let reactor = Reactor::new();
    let source = adaptite::signal_in(&reactor, 0u32);
    let memo = adaptite::memo_in(&reactor, {
        let source = source.clone();
        move || source.get()
    });
    let _ = memo.get();

    let payload = catch_unwind(AssertUnwindSafe(|| {
        memo.with(|_value| {
            source.set(1);
            let _ = memo.get();
        });
    }))
    .expect_err("invalidating and re-reading inside `with` should panic");

    let message = payload
        .downcast_ref::<String>()
        .expect("the diagnosis is formatted, so the payload is a String");
    assert!(
        // See the note in the thunk test above: `file!()`, not a literal path.
        message.contains("memo created at") && message.contains(file!()),
        "the message should name the memo and its origin, got: {message}"
    );
}
