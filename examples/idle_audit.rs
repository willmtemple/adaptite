//! Proving an application is idle, rather than inferring it from a CPU percentage.
//!
//! "An idle window costs about 0.25% of a core, and whether any of that is a flush is still
//! unknown" is not a question a profiler answers well — the number varies more between runs of
//! the same build than the effect being measured. It is a question a counter answers exactly.
//!
//! `IdleAudit` below is the whole technique, and it is deliberately small enough to copy into an
//! application rather than being something adaptite ships as API: what counts as "settled" is an
//! application's policy, not a reactive graph's. Install it, let the application sit doing
//! nothing, and read the verdict.
//!
//! Run with `cargo run --example idle_audit`.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use adaptite::{DiagnosticEvent, DiagnosticSubscription, FlushStats, Reactor, signal_in};
use runite::time::sleep;

/// Watches a reactor and records every flush that closes while it is installed.
///
/// Drop it to stop watching. Everything it needs is on the 0.3 diagnostic stream; there is no
/// feature flag to enable and nothing to rebuild.
struct IdleAudit {
    flushes: Rc<RefCell<Vec<FlushStats>>>,
    _subscription: DiagnosticSubscription,
}

impl IdleAudit {
    fn install(reactor: &Reactor) -> Self {
        let flushes = Rc::new(RefCell::new(Vec::new()));
        let subscription = reactor.subscribe_diagnostics({
            let flushes = Rc::clone(&flushes);
            // Callbacks are synchronous on the reactor thread and must not touch the graph, so
            // this copies the one field it wants and returns.
            move |event| {
                if let DiagnosticEvent::FlushFinished { stats, .. } = event {
                    flushes.borrow_mut().push(stats);
                }
            }
        });
        Self {
            flushes,
            _subscription: subscription,
        }
    }

    /// Forgets everything recorded so far. Call this once the application has settled, so start-up
    /// work is not counted against the idle window.
    fn mark_settled(&self) {
        self.flushes.borrow_mut().clear();
    }

    fn report(&self) -> Verdict {
        let flushes = self.flushes.borrow();
        Verdict {
            flushes: flushes.len(),
            // A flush that closed having done nothing. Cheap, but not free — something scheduled
            // a job, and at idle nothing should have.
            empty: flushes.iter().filter(|stats| stats.is_empty()).count(),
            effects_run: flushes.iter().map(|stats| stats.effects_run).sum(),
            computed_recomputed: flushes.iter().map(|stats| stats.computed_recomputed).sum(),
            root_writes: flushes.iter().map(|stats| stats.root_writes).sum(),
        }
    }
}

#[derive(Debug)]
struct Verdict {
    flushes: usize,
    empty: usize,
    effects_run: u32,
    computed_recomputed: u32,
    root_writes: u32,
}

impl Verdict {
    /// The assertion an idle application should be able to make.
    ///
    /// Note what this does *not* claim: a truly quiet application produces **no flushes at all**,
    /// because nothing schedules a job. Flushes that are individually empty are a weaker and more
    /// interesting result — something is waking the reactor to do nothing, which is a bug with a
    /// cause worth finding.
    fn is_quiet(&self) -> bool {
        self.flushes == 0
    }

    fn explain(&self) -> &'static str {
        if self.flushes == 0 {
            "quiet: the reactive graph did not flush at all. Whatever cost remains is below \
             adaptite — look at the runtime and the framework."
        } else if self.flushes == self.empty {
            "waking without working: flushes ran and did nothing. Something is scheduling a \
             reactive job at idle. Subscribe to EffectScheduled to find out what."
        } else {
            "working: the graph is genuinely recomputing at idle. root_writes says how much of \
             it started with a write; ComputedRecomputeStarted names the nodes."
        }
    }
}

#[runite::main]
async fn main() {
    let reactor = Reactor::new();
    let _guard = reactor.enter();

    // A small application: one input, one derived value, one effect that "renders".
    let temperature = signal_in(&reactor, 20_i32);
    let label = {
        let temperature = temperature.clone();
        reactor.memo(move || format!("{}C", temperature.get()))
    };
    let rendered = Rc::new(RefCell::new(Vec::new()));
    let render = reactor.effect({
        let label = label.clone();
        let rendered = Rc::clone(&rendered);
        move || rendered.borrow_mut().push(label.get())
    });

    let audit = IdleAudit::install(&reactor);

    // Start-up: the effect runs once.
    reactor.flush_now();
    println!("after start-up: {:?}", audit.report());

    // === Case 1: genuinely idle. ===
    audit.mark_settled();
    sleep(Duration::from_millis(20)).await;
    let verdict = audit.report();
    println!("\nidle for 20ms   -> {}", verdict.explain());
    println!("                   {verdict:?}");
    assert!(
        verdict.is_quiet(),
        "nothing happened, so nothing should flush"
    );

    // === Case 2: writing the same value every tick. ===
    //
    // Worth knowing which of adaptite's setters you are holding. `set` compares first and does
    // nothing when the value is unchanged, so this shape costs *nothing* through it — no write,
    // no flush, no wake:
    audit.mark_settled();
    for _ in 0..5 {
        temperature.set(20);
        reactor.flush_now();
    }
    assert!(
        audit.report().is_quiet(),
        "`set` suppresses an unchanged write before it reaches the graph"
    );

    // `replace` does not compare — it is the "I know this changed" setter. Through it the same
    // loop wakes the reactor five times, recomputes the memo five times, and is saved from
    // re-rendering only by the *memo's* equality check further downstream. Cheap, but not free,
    // and invisible from outside. This is the shape behind "a pane re-rendered at the frame rate
    // because an unchanged value was written every tick".
    audit.mark_settled();
    for _ in 0..5 {
        temperature.replace(20);
        reactor.flush_now();
    }
    let verdict = audit.report();
    println!("\nreplace(same)    -> {}", verdict.explain());
    println!("                   {verdict:?}");
    assert!(!verdict.is_quiet());
    assert_eq!(verdict.root_writes, 5, "five writes reached the graph");
    assert_eq!(
        verdict.effects_run, 0,
        "and none of them was worth re-rendering for"
    );
    assert_eq!(
        verdict.computed_recomputed, 5,
        "the memo recomputed each time; only equality kept the effect from re-running"
    );

    // === Case 3: real work, for contrast. ===
    audit.mark_settled();
    temperature.set(21);
    reactor.flush_now();
    let verdict = audit.report();
    println!("\nreal change      -> {}", verdict.explain());
    println!("                   {verdict:?}");
    assert_eq!(verdict.effects_run, 1);

    println!("\nrendered: {:?}", rendered.borrow());
    render.dispose();
}
