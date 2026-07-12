//! Example tracing setup for runite runtime + reactivity.
//!
//! Try:
//!
//! - `cargo run --example tracing_subscriber_showcase`
//! - `RUST_LOG=info,runite::runtime=debug,adaptite::graph=debug cargo run --example tracing_subscriber_showcase`
//! - `RUST_LOG=info,runite::scheduler=trace,adaptite::event=trace,adaptite::effect=debug cargo run --example tracing_subscriber_showcase`

use adaptite::{effect, event, on, signal, thunk};
use runite::time::sleep;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

fn install_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,\
             runite::runtime=debug,\
             runite::scheduler=debug,\
             adaptite::graph=debug,\
             adaptite::effect=debug,\
             adaptite::event=debug",
        )
    });

    let fmt_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .compact();

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init();
}

#[runite::main]
async fn main() {
    install_tracing();

    tracing::info!(
        event = "showcase_start",
        note = "override RUST_LOG to see more or less detail",
        "starting tracing subscriber showcase"
    );

    let count = signal(0usize);
    let doubled = thunk({
        let count = count.clone();
        move || count.get() * 2
    });
    let clicks = event::<usize>();

    let _drain = on(&clicks, {
        let count = count.clone();
        move |delta| {
            tracing::info!(
                event = "apply_click_delta",
                delta = *delta,
                "draining queued event into signal update"
            );
            count.update(|value| *value += *delta);
        }
    });

    let _view = effect({
        let count = count.clone();
        let doubled = doubled.clone();
        move || {
            tracing::info!(
                target: "demo::view",
                count = count.get(),
                doubled = doubled.get(),
                "derived view state updated"
            );
        }
    });

    tracing::info!(
        event = "emit_clicks",
        count = 3,
        "emitting queued click deltas"
    );
    clicks.emit(1);
    clicks.emit(2);
    clicks.emit(0);

    tracing::info!(event = "manual_set", value = 10, "setting count directly");
    let _ = count.set(10);

    tracing::info!(
        event = "showcase_done",
        hint = "see runite::* and adaptite::* targets in the filter",
        "example body completed; awaiting once so scheduled microtasks can flush"
    );

    sleep(Duration::from_millis(1)).await;
}
