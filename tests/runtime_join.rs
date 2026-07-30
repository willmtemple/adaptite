//! Joining adaptite's diagnostic events to runite's turn identity.
//!
//! `docs/diagnostics.md` tells a consumer to stamp individual *events* with the runtime's turn
//! identifier rather than stamping the per-flush aggregate, on the grounds that one `FlushStats`
//! routinely spans two runtime turns. That was an argument, not a measurement: it was written
//! when runite had no turn identifier to check it against. runite 0.3 has `current_turn`, so
//! these tests measure the claim the documentation makes instead of restating it.

use std::cell::RefCell;
use std::rc::Rc;

use adaptite::{DiagnosticEvent, DiagnosticSubscription, Reactor, signal_in};
use runite::{TurnId, queue_macrotask, run, spawn};

/// A diagnostic event, reduced to its name and the runtime turn it was delivered in.
type Record = (&'static str, Option<TurnId>);

fn label(event: &DiagnosticEvent) -> &'static str {
    match event {
        DiagnosticEvent::NodeCreated { .. } => "NodeCreated",
        DiagnosticEvent::ReactiveWrite { .. } => "ReactiveWrite",
        DiagnosticEvent::EffectScheduled { .. } => "EffectScheduled",
        DiagnosticEvent::EffectRunStarted { .. } => "EffectRunStarted",
        DiagnosticEvent::EffectRunFinished { .. } => "EffectRunFinished",
        DiagnosticEvent::FlushStarted { .. } => "FlushStarted",
        DiagnosticEvent::FlushFinished { .. } => "FlushFinished",
        _ => "other",
    }
}

/// Stamps every event with `runite::current_turn()` at the moment of delivery — exactly what
/// `docs/diagnostics.md` tells a consumer to do.
fn install(reactor: &Reactor) -> (Rc<RefCell<Vec<Record>>>, DiagnosticSubscription) {
    let log = Rc::new(RefCell::new(Vec::new()));
    let subscription = reactor.subscribe_diagnostics({
        let log = Rc::clone(&log);
        move |event| {
            log.borrow_mut()
                .push((label(&event), runite::current_turn()))
        }
    });
    (log, subscription)
}

/// Drives `body` under a runtime with diagnostics collecting, and returns every event stamped
/// with the turn it was delivered in.
fn turns_of(body: impl FnOnce(&Reactor) + 'static) -> Vec<Record> {
    let out = Rc::new(RefCell::new(Vec::new()));

    queue_macrotask({
        let out = Rc::clone(&out);
        move || {
            let reactor = Reactor::new();
            let (events, subscription) = install(&reactor);
            body(&reactor);

            // Runs after everything above has settled, and keeps the subscription alive until
            // it does — dropping it early would stop delivery mid-measurement.
            queue_macrotask(move || {
                let _keep = &subscription;
                *out.borrow_mut() = events.borrow().clone();
            });
        }
    });

    run();

    let recorded = out.borrow().clone();
    assert!(!recorded.is_empty(), "no diagnostic events were delivered");
    recorded
}

/// A signal with one effect observing it, both leaked so the graph outlives the closure.
fn observed_signal(reactor: &Reactor) -> adaptite::Signal<usize> {
    let source = signal_in(reactor, 1usize);
    let effect = reactor.effect({
        let source = source.clone();
        move || {
            let _ = source.get();
        }
    });
    std::mem::forget(effect);
    source
}

/// The turn of the last `ReactiveWrite`, and of the `FlushStarted` that follows it.
fn write_and_following_flush(records: &[Record]) -> (Option<TurnId>, Option<TurnId>) {
    let write = records
        .iter()
        .rposition(|(name, _)| *name == "ReactiveWrite")
        .expect("a write");
    let flush = records[write..]
        .iter()
        .find(|(name, _)| *name == "FlushStarted")
        .expect("a flush after the write");
    (records[write].1, flush.1)
}

#[test]
fn a_write_from_a_task_and_its_flush_share_one_turn() {
    let records = turns_of(|reactor| {
        let source = observed_signal(reactor);
        std::mem::drop(spawn(async move {
            source.set(2);
        }));
    });

    let (write, flush) = write_and_following_flush(&records);

    // A spawned task is polled *inside* the microtask checkpoint, and the checkpoint runs to
    // quiescence — so the flush this write queues drains in the same checkpoint that polled the
    // task. `docs/diagnostics.md` claimed the opposite before runite 0.3 gave us a turn id to
    // check it with.
    assert_eq!(
        write, flush,
        "a task's write and its flush should be one turn"
    );
    assert!(
        write.is_some(),
        "reactive work under the runtime should carry a turn"
    );
}

#[test]
fn a_write_from_a_macrotask_lands_in_the_turn_before_its_flush() {
    let records = turns_of(|reactor| {
        let source = observed_signal(reactor);
        queue_macrotask(move || {
            source.set(2);
        });
    });

    let (write, flush) = write_and_following_flush(&records);

    // This is the real two-turn boundary. A macrotask runs at the *end* of a turn, after that
    // turn's microtask checkpoint has already drained, so the flush it queues cannot run until
    // the next turn opens. Since adaptite folds the write into that flush's `FlushStats`, the
    // aggregate spans two turns and cannot honestly carry one turn id — which is why
    // `docs/diagnostics.md` sends a consumer to the event stream for attribution.
    assert_ne!(
        write, flush,
        "a macrotask's write and its flush should be different turns"
    );
    assert!(write.is_some() && flush.is_some());
}

#[test]
fn every_event_within_one_flush_reports_the_same_turn() {
    let records = turns_of(|reactor| {
        let source = observed_signal(reactor);
        queue_macrotask(move || {
            source.set(2);
        });
    });

    let mut flushes = 0usize;
    let mut open: Option<Option<TurnId>> = None;
    for (name, turn) in &records {
        match *name {
            "FlushStarted" => open = Some(*turn),
            "FlushFinished" => {
                let started = open.take().expect("a flush was open");
                assert_eq!(started, *turn, "a flush spanned a turn boundary");
                flushes += 1;
            }
            _ => {
                if let Some(started) = open {
                    assert_eq!(
                        started, *turn,
                        "an event inside a flush reported another turn"
                    );
                }
            }
        }
    }
    assert!(
        flushes >= 2,
        "expected the creation flush and the write's flush, saw {flushes}"
    );
}

#[test]
fn every_event_delivered_under_the_runtime_carries_a_turn() {
    let records = turns_of(|reactor| {
        let source = observed_signal(reactor);
        queue_macrotask(move || {
            source.set(2);
        });
    });

    // The join key is only useful if it is never absent for work the runtime drove. A `None`
    // here would mean a consumer's timeline silently loses records rather than failing.
    let missing: Vec<_> = records.iter().filter(|(_, turn)| turn.is_none()).collect();
    assert!(
        missing.is_empty(),
        "events delivered without a turn: {missing:?}"
    );
}
