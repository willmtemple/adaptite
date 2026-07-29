//! Per-flush totals, checked against the detailed event stream.
//!
//! Two mechanisms now describe the same work — individual events and an aggregate — and the way
//! that arrangement fails is silent drift between them. So the central test here does not assert
//! hand-written expected numbers: it tallies the event stream and asserts the tally *equals* the
//! `FlushStats`, across nested flushes, panics, disposal, coalescing and equality suppression.
//! A counter added to one and not the other fails immediately.

use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use adaptite::{
    ComputeOutcome, DiagnosticEvent, FlushStats, InvalidationLevel, Reactor, memo_in, signal_in,
    thunk_in,
};

/// Collects every event, and the `FlushStats` of every flush that closed.
#[derive(Default)]
struct Capture {
    events: Vec<DiagnosticEvent>,
    flushes: Vec<FlushStats>,
}

impl Capture {
    fn install(reactor: &Reactor) -> (Rc<RefCell<Self>>, adaptite::DiagnosticSubscription) {
        let capture = Rc::new(RefCell::new(Self::default()));
        let subscription = reactor.subscribe_diagnostics({
            let capture = Rc::clone(&capture);
            move |event| {
                let mut capture = capture.borrow_mut();
                if let DiagnosticEvent::FlushFinished { stats, .. } = event {
                    capture.flushes.push(stats);
                }
                capture.events.push(event);
            }
        });
        (capture, subscription)
    }

    /// Sums the per-flush aggregates.
    fn totals(&self) -> FlushStats {
        let mut total = FlushStats::default();
        for stats in &self.flushes {
            total.root_writes += stats.root_writes;
            total.nodes_marked_check += stats.nodes_marked_check;
            total.nodes_marked_dirty += stats.nodes_marked_dirty;
            total.effects_queued += stats.effects_queued;
            total.effects_coalesced += stats.effects_coalesced;
            total.effects_run += stats.effects_run;
            total.effects_skipped += stats.effects_skipped;
            total.effects_disposed += stats.effects_disposed;
            total.computed_verified += stats.computed_verified;
            total.computed_recomputed += stats.computed_recomputed;
            total.computed_changed += stats.computed_changed;
            total.computed_suppressed += stats.computed_suppressed;
            total.edges_added += stats.edges_added;
            total.edges_removed += stats.edges_removed;
        }
        total
    }

    /// Counts the same things from the individual events.
    fn tallied(&self) -> FlushStats {
        let mut tally = FlushStats::default();
        for event in &self.events {
            match event {
                DiagnosticEvent::ReactiveWrite { .. } => tally.root_writes += 1,
                DiagnosticEvent::ComputedInvalidated { level, .. }
                | DiagnosticEvent::EffectInvalidated { level, .. } => match level {
                    InvalidationLevel::Check => tally.nodes_marked_check += 1,
                    InvalidationLevel::Dirty => tally.nodes_marked_dirty += 1,
                    _ => {}
                },
                DiagnosticEvent::EffectScheduled { queued, .. } => {
                    if *queued {
                        tally.effects_queued += 1;
                    } else {
                        tally.effects_coalesced += 1;
                    }
                }
                DiagnosticEvent::EffectRunStarted { .. } => tally.effects_run += 1,
                DiagnosticEvent::EffectRunSkipped { .. } => tally.effects_skipped += 1,
                DiagnosticEvent::EffectDisposed { .. } => tally.effects_disposed += 1,
                DiagnosticEvent::ComputedVerified { .. } => tally.computed_verified += 1,
                DiagnosticEvent::ComputedRecomputeStarted { .. } => tally.computed_recomputed += 1,
                // A recomputation that unwound published nothing, so it is neither a change nor a
                // suppression. `computed_changed + computed_suppressed` is therefore <=
                // `computed_recomputed`, and the difference is the computations that failed.
                DiagnosticEvent::ComputedRecomputeFinished {
                    changed, outcome, ..
                } if *outcome == ComputeOutcome::Completed => {
                    if *changed {
                        tally.computed_changed += 1;
                    } else {
                        tally.computed_suppressed += 1;
                    }
                }
                _ => {}
            }
        }
        tally
    }
}

/// Everything the aggregate claims must be visible in the stream, and vice versa.
#[track_caller]
fn assert_consistent(capture: &Rc<RefCell<Capture>>) {
    let capture = capture.borrow();
    let totals = capture.totals();
    let tallied = capture.tallied();
    assert_eq!(
        totals.root_writes, tallied.root_writes,
        "root writes disagree"
    );
    assert_eq!(
        (totals.nodes_marked_check, totals.nodes_marked_dirty),
        (tallied.nodes_marked_check, tallied.nodes_marked_dirty),
        "marks disagree"
    );
    assert_eq!(
        (
            totals.effects_queued,
            totals.effects_coalesced,
            totals.effects_run,
            totals.effects_skipped,
            totals.effects_disposed
        ),
        (
            tallied.effects_queued,
            tallied.effects_coalesced,
            tallied.effects_run,
            tallied.effects_skipped,
            tallied.effects_disposed
        ),
        "effect totals disagree"
    );
    assert_eq!(
        (
            totals.computed_verified,
            totals.computed_recomputed,
            totals.computed_changed,
            totals.computed_suppressed
        ),
        (
            tallied.computed_verified,
            tallied.computed_recomputed,
            tallied.computed_changed,
            tallied.computed_suppressed
        ),
        "computed totals disagree"
    );
}

#[test]
fn totals_agree_with_the_stream_through_suppression_and_coalescing() {
    let reactor = Reactor::new();
    let (capture, _subscription) = Capture::install(&reactor);

    let source = signal_in(&reactor, 1_u32);
    let other = signal_in(&reactor, 0_u32);
    let parity = memo_in(&reactor, {
        let source = source.clone();
        move || source.get() % 2
    });
    let effect = reactor.effect({
        let parity = parity.clone();
        let other = other.clone();
        move || {
            let _ = (parity.get(), other.get());
        }
    });
    reactor.flush_now();

    // Two writes in one turn coalesce into one run.
    source.set(3);
    other.set(1);
    reactor.flush_now();

    // A write that the memo's comparator suppresses.
    source.set(5);
    reactor.flush_now();

    effect.dispose();

    // The disposal happened outside any flush, so it sits in the pending accumulator until a
    // flush actually runs — and a drain with an empty queue is no longer a flush. Creating an
    // effect schedules a job, which gives the pending work somewhere to land. Without this the
    // aggregate legitimately trails the event stream by one disposal.
    let keeper = reactor.effect(|| {});
    reactor.flush_now();
    assert_consistent(&capture);
    keeper.dispose();
}

#[test]
fn totals_agree_with_the_stream_across_a_panicking_job() {
    let reactor = Reactor::new();
    let (capture, _subscription) = Capture::install(&reactor);

    let source = signal_in(&reactor, 1_u32);
    let boom = thunk_in(&reactor, {
        let source = source.clone();
        move || {
            if source.get() > 1 {
                panic!("compute failed");
            }
            source.get()
        }
    });
    let effect = reactor.effect({
        let boom = boom.clone();
        move || {
            let _ = boom.get();
        }
    });
    reactor.flush_now();

    source.set(2);
    let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
    assert!(result.is_err(), "the panic propagates out of the flush");

    // The flush that unwound still closed its own totals, and the reactor recovered.
    assert_consistent(&capture);
    assert!(
        capture.borrow().flushes.len() >= 2,
        "the unwinding flush still reported"
    );

    effect.dispose();
}

#[test]
fn a_nested_flush_keeps_its_work_out_of_the_enclosing_one() {
    let reactor = Reactor::new();
    let (capture, _subscription) = Capture::install(&reactor);

    let inner_source = signal_in(&reactor, 0_u32);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let inner_effect = reactor.effect({
        let inner_source = inner_source.clone();
        let seen = Rc::clone(&seen);
        move || seen.borrow_mut().push(inner_source.get())
    });
    reactor.flush_now();
    capture.borrow_mut().flushes.clear();
    capture.borrow_mut().events.clear();

    // A job that writes and then flushes re-entrantly. The inner flush runs the effect; the outer
    // one must not also claim it.
    reactor.schedule({
        let reactor = reactor.clone();
        let inner_source = inner_source.clone();
        move || {
            inner_source.set(1);
            reactor.flush_now();
        }
    });
    reactor.flush_now();
    assert_eq!(*seen.borrow(), [0, 1]);

    let capture_ref = capture.borrow();
    assert_eq!(capture_ref.flushes.len(), 2, "an inner and an outer flush");
    let inner = capture_ref.flushes[0];
    let outer = capture_ref.flushes[1];
    assert_eq!(inner.effects_run, 1, "the inner flush ran the effect");
    assert_eq!(
        outer.effects_run, 0,
        "and the outer flush must not count it again"
    );
    drop(capture_ref);
    assert_consistent(&capture);

    inner_effect.dispose();
}

#[test]
fn the_writes_that_scheduled_a_flush_are_counted_in_it() {
    let reactor = Reactor::new();
    let (capture, _subscription) = Capture::install(&reactor);

    let source = signal_in(&reactor, 0_u32);
    let effect = reactor.effect({
        let source = source.clone();
        move || {
            let _ = source.get();
        }
    });
    reactor.flush_now();
    capture.borrow_mut().flushes.clear();

    // The write happens outside any flush. Attributing it to nothing would make `root_writes`
    // useless for answering "what set this flush off".
    source.set(1);
    reactor.flush_now();

    let flushes = capture.borrow().flushes.clone();
    assert_eq!(flushes.len(), 1);
    assert_eq!(flushes[0].root_writes, 1);
    assert_eq!(flushes[0].effects_run, 1);

    effect.dispose();
}

#[test]
fn a_settled_graph_reports_an_empty_flush() {
    let reactor = Reactor::new();
    let (capture, _subscription) = Capture::install(&reactor);

    let source = signal_in(&reactor, 0_u32);
    let effect = reactor.effect({
        let source = source.clone();
        move || {
            let _ = source.get();
        }
    });
    reactor.flush_now();
    capture.borrow_mut().flushes.clear();

    // Nothing has changed, so there is nothing to drain — and a drain with nothing to drain is
    // not a flush. "No flush at all" is the signature of idle, which is the assertion an idle
    // application makes instead of watching a CPU percentage.
    reactor.flush_now();
    assert!(
        capture.borrow().flushes.is_empty(),
        "a settled graph must not flush at all, got {:?}",
        capture.borrow().flushes
    );

    // A consumer-declared boundary is different: `external_flush` is reported whether or not the
    // drain found work, because the consumer said a drain happened. That is where an *empty*
    // `FlushStats` still shows up, and what `is_empty()` is for.
    reactor.external_flush(|| {});
    let flushes = capture.borrow().flushes.clone();
    assert_eq!(flushes.len(), 1, "the declared boundary is still reported");
    assert!(
        flushes[0].is_empty(),
        "and it did no work, got {:?}",
        flushes[0]
    );

    effect.dispose();
}

#[test]
fn a_producer_that_runs_more_often_than_it_publishes_is_visible() {
    // The shape this exists for: a sampler that writes its signal far more often than the value
    // moves. The equality gate saves the re-render and hides the work, so the propagation stream —
    // which by construction only sees writes that propagated — reports the changes and nothing
    // else. `writes_suppressed` and `WriteSuppressed` make the rest attributable to the site that
    // produced them.
    let reactor = Reactor::new();
    let (capture, _subscription) = Capture::install(&reactor);

    // Starts outside the sampled range, so each of the four groups genuinely changes it.
    let sampled = signal_in(&reactor, 99_u32);
    let effect = reactor.effect({
        let sampled = sampled.clone();
        move || {
            let _ = sampled.get();
        }
    });
    reactor.flush_now();
    capture.borrow_mut().flushes.clear();
    capture.borrow_mut().events.clear();

    // Twenty attempts, four of which actually change the value.
    for step in 0..20 {
        sampled.set(step / 5);
        reactor.flush_now();
    }

    let tally = |capture: &std::cell::RefMut<'_, Capture>| {
        (
            capture.flushes.iter().map(|s| s.root_writes).sum::<u32>(),
            capture
                .flushes
                .iter()
                .map(|s| s.writes_suppressed)
                .sum::<u32>(),
        )
    };

    let (published, discarded) = tally(&capture.borrow_mut());
    assert_eq!(published, 4, "only four attempts moved the value");
    assert_eq!(
        discarded, 12,
        "twelve of the sixteen discarded writes have been carried by a flush; the last four \
         happened after the final flush and are still pending, because a suppressed write \
         schedules nothing and a drain with an empty queue is not a flush"
    );

    // They are not lost, only waiting: the next flush that happens for any reason carries them.
    sampled.set(1_000);
    reactor.flush_now();
    let (published, discarded) = tally(&capture.borrow_mut());
    assert_eq!(published, 5);
    assert_eq!(discarded, 16, "and now all sixteen are accounted for");

    let capture = capture.borrow();

    // Every discarded write names the site that made it, which is what turns the number into an
    // action rather than a mystery.
    let sites = capture
        .events
        .iter()
        .filter_map(|event| match event {
            DiagnosticEvent::WriteSuppressed {
                node, write_origin, ..
            } if *node == sampled.id() => Some(write_origin.line()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(sites.len(), 16);
    assert!(
        sites.windows(2).all(|pair| pair[0] == pair[1]),
        "all attributed to the one call site"
    );

    // A suppressed write is not a `ReactiveWrite`: nothing propagated, and claiming otherwise
    // would double-count the graph's actual work.
    let propagated = capture
        .events
        .iter()
        .filter(|event| matches!(event, DiagnosticEvent::ReactiveWrite { .. }))
        .count();
    assert_eq!(propagated, 5);

    drop(capture);
    effect.dispose();
}

#[test]
fn propagation_depth_counts_the_chain() {
    let reactor = Reactor::new();
    let (capture, _subscription) = Capture::install(&reactor);

    // signal -> a -> b -> effect: marking walks signal's dependents (a), a's (b), b's (effect).
    let source = signal_in(&reactor, 1_u32);
    let a = memo_in(&reactor, {
        let source = source.clone();
        move || source.get() + 1
    });
    let b = memo_in(&reactor, {
        let a = a.clone();
        move || a.get() + 1
    });
    let effect = reactor.effect({
        let b = b.clone();
        move || {
            let _ = b.get();
        }
    });
    reactor.flush_now();
    capture.borrow_mut().flushes.clear();

    source.set(2);
    reactor.flush_now();

    let depth = capture
        .borrow()
        .flushes
        .iter()
        .map(|stats| stats.max_propagation_depth)
        .max()
        .expect("a flush closed");
    assert_eq!(depth, 3, "three links from the write to the effect");

    effect.dispose();
}
