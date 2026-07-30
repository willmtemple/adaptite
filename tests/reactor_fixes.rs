//! Regression tests for the reactor fixes in the 0.3 release-readiness review.

use std::cell::RefCell;
use std::process::Command;

use adaptite::{EnterGuard, Reactor};

/// Runs `payload` in a freshly spawned copy of this test binary.
///
/// The failure this guards against is a **process abort**: a panic escaping a `Drop` that runs
/// during thread-local teardown is a non-unwinding panic, which `catch_unwind` cannot see and
/// which kills the test harness along with everything else. The only way to observe it is from
/// outside the process, so the test re-executes itself with a marker in the environment and
/// asserts on the child's exit status.
fn in_a_child_process(test_name: &str, payload: impl FnOnce()) {
    const MARKER: &str = "ADAPTITE_REACTOR_FIXES_CHILD";

    if std::env::var_os(MARKER).is_some() {
        payload();
        return;
    }

    let exe = std::env::current_exe().expect("test binary path");
    let status = Command::new(exe)
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(MARKER, "1")
        .status()
        .expect("re-run this test binary");
    assert!(
        status.success(),
        "the child process did not exit cleanly ({status}); a `Drop` running during thread-local \
         teardown aborted it"
    );
}

thread_local! {
    /// Touched *before* any adaptite thread-local, so it is registered for destruction first and
    /// therefore destroyed *last* — its contents drop when adaptite's own slots are already gone.
    static PARKED_GUARD: RefCell<Option<(Reactor, EnterGuard)>> = const { RefCell::new(None) };

    /// Same trick, for a value whose `Drop` reaches back into adaptite.
    static PARKED_TEARDOWN: RefCell<Option<AmbientOnDrop>> = const { RefCell::new(None) };
}

/// A host-shaped teardown: reactive work performed from a `Drop` that runs at thread exit.
struct AmbientOnDrop;

impl Drop for AmbientOnDrop {
    fn drop(&mut self) {
        // `try_current` reads `CURRENT_REACTOR`; `current` additionally writes `CURRENT_REACTOR`
        // and `HAS_HAD_DEFAULT`; `run_in_context` and `untrack` touch `UNTRACKED_DEPTH` (and, in
        // debug builds, `RUNNING_REACTOR`). All of them are ordinary things for a host's teardown
        // code to reach, and none of them may abort the process.
        assert!(
            adaptite::try_current().is_none(),
            "the thread default is unreachable once its slot is destroyed"
        );
        let reactor = adaptite::current();
        let value = reactor.signal(1u32);
        let doubled = reactor.memo({
            let value = value.clone();
            move || value.get() * 2
        });
        assert_eq!(doubled.get(), 2);
        value.set(5);
        reactor.flush_now();
        assert_eq!(doubled.get(), 10);
        adaptite::untrack(|| assert_eq!(value.get(), 5));
    }
}

#[test]
fn an_enter_guard_released_during_thread_local_teardown_does_not_abort() {
    in_a_child_process(
        "an_enter_guard_released_during_thread_local_teardown_does_not_abort",
        || {
            // Register this slot's destructor before adaptite touches any of its own.
            PARKED_GUARD.with(|parked| assert!(parked.borrow().is_none()));

            let reactor = Reactor::new();
            let guard = reactor.enter();
            PARKED_GUARD.with(|parked| *parked.borrow_mut() = Some((reactor, guard)));

            // Returning drops the test thread, whose thread-local destructors run in reverse
            // registration order: adaptite's `CURRENT_REACTOR`/`ANCHORED_REACTOR` first, then
            // `PARKED_GUARD` — so `EnterGuard::drop` runs with both already destroyed.
        },
    );
}

#[test]
fn reactive_work_from_a_drop_during_thread_local_teardown_does_not_abort() {
    in_a_child_process(
        "reactive_work_from_a_drop_during_thread_local_teardown_does_not_abort",
        || {
            // Registered first, destroyed last.
            PARKED_TEARDOWN.with(|parked| assert!(parked.borrow().is_none()));

            // Touch adaptite's slots so they are registered after `PARKED_TEARDOWN`.
            let reactor = Reactor::new();
            drop(reactor.enter());

            PARKED_TEARDOWN.with(|parked| *parked.borrow_mut() = Some(AmbientOnDrop));
        },
    );
}
