//! A streaming price ticker demonstrating how async work feeds a reactive graph from the edges:
//!
//! - **Events bridge async to reactive.** A spawned task emits `Quote` events; a draining
//!   subscription (`on`) folds them into a signal on the reactive side.
//! - **`memo_with_prev` expresses reductions.** Running count/min/max/average read their own
//!   previous value without creating a dependency cycle.
//! - **Memos gate propagation.** The alert memo computes a `bool` per quote, but the alert
//!   effect runs only when that bool *changes* — it prints on threshold crossings, not ticks.
//!
//! Run with `cargo run --example stock_ticker`.

use std::time::Duration;

use adaptite::{effect, event, memo, memo_with_prev, on, signal};
use runite::time::sleep;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Quote {
    price_cents: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Stats {
    count: u32,
    min: i64,
    max: i64,
    sum: i64,
}

const ALERT_CENTS: i64 = 10500; // alert when the price exceeds $105.00

fn dollars(cents: i64) -> f64 {
    cents as f64 / 100.0
}

#[runite::main]
async fn main() {
    // The bridge between the async feed and the reactive graph.
    let quotes = event::<Quote>();

    // Reactive state fed by the event stream.
    let latest = signal(None::<Quote>);

    on(&quotes, {
        let latest = latest.clone();
        move |quote| {
            let _ = latest.set(Some(*quote));
        }
    })
    .leak();

    // A running reduction: each recomputation receives the previous Stats and folds the newest
    // quote into it. No cycle: the memo depends on `latest`, not on itself.
    let stats = memo_with_prev({
        let latest = latest.clone();
        move |previous: Option<&Stats>| {
            let Some(quote) = latest.get() else {
                return Stats {
                    count: 0,
                    min: i64::MAX,
                    max: i64::MIN,
                    sum: 0,
                };
            };
            let previous = previous.copied().unwrap_or(Stats {
                count: 0,
                min: i64::MAX,
                max: i64::MIN,
                sum: 0,
            });
            Stats {
                count: previous.count + 1,
                min: previous.min.min(quote.price_cents),
                max: previous.max.max(quote.price_cents),
                sum: previous.sum + quote.price_cents,
            }
        }
    });

    // Recomputes on every quote, but *propagates* only when the boolean flips.
    let above_threshold = memo({
        let latest = latest.clone();
        move || matches!(latest.get(), Some(quote) if quote.price_cents > ALERT_CENTS)
    });

    // The ticker line: re-renders once per quote.
    effect({
        let latest = latest.clone();
        let stats = stats.clone();
        move || {
            let (Some(quote), stats) = (latest.get(), stats.get()) else {
                println!("[ticker] waiting for quotes...");
                return;
            };
            println!(
                "[ticker] ACME {:>7.2} | n={} min={:.2} max={:.2} avg={:.2}",
                dollars(quote.price_cents),
                stats.count,
                dollars(stats.min),
                dollars(stats.max),
                dollars(stats.sum / i64::from(stats.count)),
            );
        }
    })
    .leak();

    // The alert line: runs only when `above_threshold` changes value.
    effect({
        let above_threshold = above_threshold.clone();
        move || {
            if above_threshold.get() {
                println!("[alert]  ACME rose above {:.2}!", dollars(ALERT_CENTS));
            } else {
                println!("[alert]  ACME is at or below {:.2}", dollars(ALERT_CENTS));
            }
        }
    })
    .leak();

    // The market: an async task pushing quotes into the graph, one per turn. When it finishes,
    // the task queue drains and the program exits on its own.
    let feed = [10250, 10380, 10520, 10610, 10470, 10330, 10560];
    for price_cents in feed {
        sleep(Duration::from_millis(20)).await;
        quotes.emit(Quote { price_cents });
    }

    sleep(Duration::from_millis(20)).await;
    println!("market closed");
}
