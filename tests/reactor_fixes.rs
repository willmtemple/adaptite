//! Regression tests for the reactor fixes in the 0.3 release-readiness review.

use std::cell::RefCell;
use std::process::Command;
use std::rc::Rc;

use adaptite::{DiagnosticEvent, DiagnosticSubscription, EnterGuard, Reactor};

/// The flush span events a subscriber saw, as `("start" | "finish", flush_epoch)`.
type Spans = Rc<RefCell<Vec<(&'static str, u64)>>>;

fn record_flush_spans(reactor: &Reactor, into: &Spans) -> DiagnosticSubscription {
    let into = Rc::clone(into);
    reactor.subscribe_diagnostics(move |event| match event {
        DiagnosticEvent::FlushStarted { flush_epoch, .. } => {
            into.borrow_mut().push(("start", flush_epoch));
        }
        DiagnosticEvent::FlushFinished { flush_epoch, .. } => {
            into.borrow_mut().push(("finish", flush_epoch));
        }
        _ => {}
    })
}

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
        "the child process did not exit cleanly ({status}); a panic escaping a `Drop` aborted it"
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

#[test]
fn subscribing_from_inside_a_consumer_drain_does_not_produce_an_unpaired_finish() {
    // docs/diagnostics.md hands consumers a duration recipe built on the `FlushStarted` /
    // `FlushFinished` pair. A subscription installed while a flush is already open has no
    // `FlushStarted` to pair with, and a bare `FlushFinished` — carrying all-zero `FlushStats`,
    // because there was never an accounting slot — was the very first event such a subscriber
    // ever received. The documented recipe panics on it, and a defensive consumer records a
    // phantom flush instead.
    let reactor = Reactor::new();
    let spans: Spans = Rc::new(RefCell::new(Vec::new()));
    let held: Rc<RefCell<Option<DiagnosticSubscription>>> = Rc::new(RefCell::new(None));

    reactor.external_flush(|| {
        *held.borrow_mut() = Some(record_flush_spans(&reactor, &spans));
    });

    assert!(
        spans.borrow().is_empty(),
        "a mid-flush subscriber must not be handed a close it never saw open, got {:?}",
        spans.borrow()
    );

    // The *next* flush is a complete, correctly ordered pair, so suppressing the orphan does not
    // cost the subscriber the flushes it can genuinely observe.
    reactor.external_flush(|| {});
    let seen = spans.borrow().clone();
    assert_eq!(
        seen.len(),
        2,
        "the next flush reports a full pair: {seen:?}"
    );
    assert_eq!(seen[0].0, "start");
    assert_eq!(seen[1].0, "finish");
    assert_eq!(seen[0].1, seen[1].1, "and both halves share one epoch");

    drop(held.borrow_mut().take());
}

#[test]
fn subscribing_from_inside_a_job_flush_does_not_produce_an_unpaired_finish() {
    // The same defect on the `flush_now` path, where the subscription is installed by a job.
    let reactor = Reactor::new();
    let spans: Spans = Rc::new(RefCell::new(Vec::new()));
    let held: Rc<RefCell<Option<DiagnosticSubscription>>> = Rc::new(RefCell::new(None));

    reactor.schedule({
        let reactor = reactor.clone();
        let spans = Rc::clone(&spans);
        let held = Rc::clone(&held);
        move || *held.borrow_mut() = Some(record_flush_spans(&reactor, &spans))
    });
    reactor.flush_now();

    assert!(
        spans.borrow().is_empty(),
        "a subscriber installed mid-flush must not see an unpaired finish, got {:?}",
        spans.borrow()
    );

    drop(held.borrow_mut().take());
}

#[test]
fn a_panicking_flush_started_subscriber_does_not_strand_the_flush_depth() {
    // `begin_flush` increments `flush_depth` first and calls the `FlushStarted` subscriber last.
    // With `external_flush`'s guard constructed *after* `begin_flush` returned, a panic escaping
    // that subscriber stranded the depth at 1 for the rest of the process: `flush_jobs` advances
    // `drain_epoch` only while the depth is zero, so the logical drain froze, and the divergence
    // guard — enforced in every build — then panicked on the 101st ordinary run of whatever
    // effect happened to be next. The accusation named an innocent, convergent effect.
    let reactor = Reactor::new();
    let boom = std::cell::Cell::new(true);
    let subscription = reactor.subscribe_diagnostics(move |event| {
        if matches!(event, DiagnosticEvent::FlushStarted { .. }) && boom.replace(false) {
            panic!("a diagnostic subscriber panicked");
        }
    });

    let escaped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        reactor.external_flush(|| {});
    }));
    assert!(
        escaped.is_err(),
        "the subscriber's panic reaches the caller"
    );
    drop(subscription);

    // The proof that matters: an ordinary convergent effect survives well past the divergence
    // guard's 100-run ceiling across separate top-level flushes.
    let tick = reactor.signal(0_u32);
    let runs = Rc::new(std::cell::Cell::new(0_u32));
    let effect = reactor.effect({
        let tick = tick.clone();
        let runs = Rc::clone(&runs);
        move || {
            let _ = tick.get();
            runs.set(runs.get() + 1);
        }
    });
    for value in 1..=150 {
        tick.set(value);
        reactor.flush_now();
    }
    // 150, not 151: the queued initial run and the first write land in the same flush.
    assert_eq!(runs.get(), 150, "every run is an ordinary, convergent one");
    effect.dispose();
}

#[test]
fn a_subscriber_that_panics_on_flush_finished_does_not_abort_an_unwinding_flush() {
    in_a_child_process(
        "a_subscriber_that_panics_on_flush_finished_does_not_abort_an_unwinding_flush",
        || {
            // `FlushFinished` is emitted from a `Drop`, deliberately: the pairing contract says
            // the close is delivered on the unwind path too. Unguarded, a subscriber that panics
            // on it while a flush is *already* unwinding raised a panic during unwinding, which
            // is a non-unwinding panic and aborts the process — turning one reportable effect
            // panic plus one reportable subscriber panic into a dead process.
            let reactor = Reactor::new();
            let subscription = reactor.subscribe_diagnostics(|event| {
                if matches!(event, DiagnosticEvent::FlushFinished { .. }) {
                    panic!("a diagnostic subscriber panicked on the close");
                }
            });

            let effect = reactor.effect(|| panic!("the effect panicked"));
            let escaped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reactor.flush_now();
            }));
            assert!(
                escaped.is_err(),
                "the effect's panic — the first and more informative one — reaches the caller"
            );

            drop(subscription);
            effect.dispose();
        },
    );
}

#[test]
fn write_origin_names_the_consumer_line_that_triggered_the_write() {
    // `InvalidationCause::write_origin` is what makes a propagated write attributable to the
    // consumer's source line, and it is the headline of the 0.3 diagnostics contract. Nothing
    // asserted it: `Reactor::trigger` is its only producer, and the suite's one `write_origin`
    // assertion reads `WriteSuppressed`, whose origin is captured in `Signal::set` and never
    // reaches `trigger` at all. Removing `#[track_caller]` from `trigger` left the suite green.
    let reactor = Reactor::new();
    let source = reactor.source();
    let observed: Rc<RefCell<Vec<(&'static str, u32)>>> = Rc::new(RefCell::new(Vec::new()));

    let _subscription = reactor.subscribe_diagnostics({
        let observed = Rc::clone(&observed);
        move |event| {
            if let DiagnosticEvent::ReactiveWrite { cause, .. } = event {
                observed
                    .borrow_mut()
                    .push((cause.write_origin.file(), cause.write_origin.line()));
            }
        }
    });

    // A reader, so the write has somewhere to propagate.
    let view = reactor.thunk({
        let reactor = reactor.clone();
        let source = source.clone();
        move || {
            reactor.observe(source.id());
            1_u32
        }
    });
    assert_eq!(view.get(), 1);

    let line = line!() + 1;
    source.trigger();

    assert_eq!(
        *observed.borrow(),
        [(file!(), line)],
        "the write must be attributed to the caller's line, not to a line inside adaptite"
    );
}

#[test]
fn a_subscriber_may_schedule_work_while_a_consumer_drain_is_opening() {
    // The mirror of `tests/reentrancy.rs`'s coverage of the `flush_now` path. Both `FlushStarted`
    // emit sites bind the pending-job count before emitting, because written inline it is a
    // temporary whose borrow lives across the subscriber call — and scheduling reactor work from
    // `FlushStarted` is an entirely reasonable thing to do. Only the `flush_now` site was tested,
    // while `external_flush` is the documented host-integration boundary.
    let reactor = Reactor::new();
    let ran = Rc::new(std::cell::Cell::new(0_u32));
    let armed = std::cell::Cell::new(true);

    let _subscription = reactor.subscribe_diagnostics({
        let reactor = reactor.clone();
        let ran = Rc::clone(&ran);
        move |event| {
            if matches!(event, DiagnosticEvent::FlushStarted { .. }) && armed.replace(false) {
                reactor.schedule({
                    let ran = Rc::clone(&ran);
                    move || ran.set(ran.get() + 1)
                });
            }
        }
    });

    reactor.external_flush(|| {});
    reactor.flush_now();
    assert_eq!(
        ran.get(),
        1,
        "the job the subscriber scheduled must have run"
    );
}

#[test]
fn an_observation_hook_that_captures_its_source_weakly_releases_the_reactor() {
    // `ReactorInner` holds observation hooks strongly for the node's whole lifetime, so a hook
    // that captures the `Source` it belongs to retains the entire reactor for the process
    // lifetime — a leak none of 0.3's gauges can see. The escape needs no API from adaptite: the
    // consumer keeps the slot and the hook captures a `Weak` of it. This pins that the escape
    // works, and the flush-after-the-last-observer-leaves step it needs.
    struct Sentinel(Rc<std::cell::Cell<bool>>);

    impl Drop for Sentinel {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    let released = Rc::new(std::cell::Cell::new(false));
    {
        let reactor = Reactor::new();
        let slot: Rc<RefCell<Option<adaptite::Source>>> = Rc::new(RefCell::new(None));
        let weak = Rc::downgrade(&slot);
        let sentinel = Sentinel(Rc::clone(&released));

        let node = reactor.source_with_hooks(
            move || {
                // Captures the sentinel and only a `Weak` handle on the slot.
                let _ = &sentinel;
                let _ = weak.upgrade();
            },
            || {},
        );
        *slot.borrow_mut() = Some(node.clone());

        let reader = reactor.thunk({
            let reactor = reactor.clone();
            let node = node.clone();
            move || {
                reactor.observe(node.id());
                1_u32
            }
        });
        assert_eq!(reader.get(), 1);
        reactor.flush_now();

        drop(reader);
        // The deferred unwatch job holds its own `Rc<ObservationHooks>` until it runs.
        reactor.flush_now();
        drop(node);
        drop(slot);
        reactor.flush_now();
    }

    assert!(
        released.get(),
        "a hook capturing a `Weak` slot must not retain the reactor"
    );
}
