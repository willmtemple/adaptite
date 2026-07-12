//! A guided tour of lazy, glitch-free propagation through two signals and two chained thunks:
//!
//! - **Reads compute; writes only mark.** Thunks recompute during `get()`, never during
//!   `set()`.
//! - **Caches hold until a dependency changes.** Re-reading an up-to-date summary runs
//!   nothing.
//! - **Equal `set` calls are suppressed**, while `replace`/`update` always propagate.
//! - **Verification refreshes upstream first.** After a name change, the greeting thunk
//!   recomputes while `summary` is checking its dependencies, before `summary` does.
//!
//! Each log line pairs the order it actually ran in (`actual`) with the order predicted in
//! the source (`expected`); the columns always agree.
//!
//! Run with `cargo run --example signal_thunk_showcase`.

use adaptite::{signal, thunk};
use std::cell::Cell as Counter;
use std::fmt;
use std::rc::Rc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

static START: OnceLock<Instant> = OnceLock::new();
static ACTUAL_ORDER: AtomicUsize = AtomicUsize::new(1);

macro_rules! log_event {
    ($expected:expr, $($arg:tt)*) => {{
        log_event_impl($expected, format_args!($($arg)*));
    }};
}

fn log_event_impl(expected: usize, message: fmt::Arguments<'_>) {
    let actual = ACTUAL_ORDER.fetch_add(1, Ordering::SeqCst);
    let elapsed = START
        .get()
        .expect("showcase start time should be initialized")
        .elapsed()
        .as_millis();
    println!(
        "[actual {actual:02} | expected {expected:02} | +{elapsed:04}ms | ts {}] {message}",
        unix_timestamp_millis(),
    );
}

fn unix_timestamp_millis() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after the Unix epoch");
    format!("{}.{:03}", now.as_secs(), now.subsec_millis())
}

#[runite::main]
fn main() {
    START.get_or_init(Instant::now);

    let first = signal(String::from("Adaptite"));
    let visits = signal(1usize);
    let greeting_compute = Rc::new(Counter::new(0usize));
    let summary_compute = Rc::new(Counter::new(0usize));

    let greeting = thunk({
        let first = first.clone();
        let greeting_compute = Rc::clone(&greeting_compute);
        move || {
            let expected = match greeting_compute.get() {
                0 => 3,
                // Lazy verification refreshes this thunk while `summary` is checking its
                // dependencies, so it recomputes *before* summary does.
                _ => 17,
            };
            greeting_compute.set(greeting_compute.get() + 1);
            log_event!(expected, "[compute] greeting thunk recomputes");
            format!("Hello, {}!", first.get())
        }
    });

    let summary = thunk({
        let greeting = greeting.clone();
        let visits = visits.clone();
        let summary_compute = Rc::clone(&summary_compute);
        move || {
            let expected = match summary_compute.get() {
                0 => 2,
                1 => 9,
                2 => 18,
                _ => 22,
            };
            summary_compute.set(summary_compute.get() + 1);
            log_event!(expected, "[compute] summary thunk recomputes");
            format!("{} Visits: {}", greeting.get(), visits.get())
        }
    });

    log_event!(1, "[main] read summary for the first time");
    log_event!(4, "[main] summary = {}", summary.get());

    log_event!(5, "[main] read summary again (should hit caches)");
    log_event!(6, "[main] summary = {}", summary.get());

    log_event!(7, "[main] set visits to 2");
    visits
        .set(2)
        .expect("visit count should report the previous value");

    log_event!(8, "[main] read summary after visits change");
    log_event!(10, "[main] summary = {}", summary.get());

    log_event!(11, "[main] set first to the same value");
    assert!(
        first.set(String::from("Adaptite")).is_none(),
        "same-value set should be suppressed",
    );

    log_event!(12, "[main] read summary after unchanged write");
    log_event!(13, "[main] summary = {}", summary.get());

    log_event!(14, "[main] replace first with `Reactive`");
    let old = first.replace(String::from("Reactive"));
    log_event!(15, "[main] replace returned old value `{old}`");

    log_event!(16, "[main] read summary after name change");
    log_event!(19, "[main] summary = {}", summary.get());

    log_event!(20, "[main] update visits in place");
    visits.update(|count| *count += 1);

    log_event!(21, "[main] read summary after update()");
    log_event!(23, "[main] summary = {}", summary.get());
}
