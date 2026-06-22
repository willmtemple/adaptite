//! Example tracing setup for RUIN runtime + reactivity.
//!
//! Try:
//!
//! - `cargo run -p ruin_reactivity --example tracing_subscriber_showcase`
//! - `RUST_LOG=info,ruin_runtime::runtime=debug,ruin_reactivity::graph=debug cargo run -p ruin_reactivity --example tracing_subscriber_showcase`
//! - `RUST_LOG=info,ruin_runtime::scheduler=trace,ruin_reactivity::event=trace,ruin_reactivity::effect=debug cargo run -p ruin_reactivity --example tracing_subscriber_showcase`

use ruin_reactivity::{cell, effect, event, on, thunk};
use ruin_runtime::time::sleep;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

fn install_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,\
             ruin_runtime::runtime=debug,\
             ruin_runtime::scheduler=debug,\
             ruin_reactivity::graph=debug,\
             ruin_reactivity::effect=debug,\
             ruin_reactivity::event=debug",
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

#[ruin_runtime::async_main]
async fn main() {
    install_tracing();

    tracing::info!(
        event = "showcase_start",
        note = "override RUST_LOG to see more or less detail",
        "starting tracing subscriber showcase"
    );

    let count = cell(0usize);
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
                "draining queued event into cell update"
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
        hint = "see ruin_runtime::* and ruin_reactivity::* targets in the filter",
        "example body completed; awaiting once so scheduled microtasks can flush"
    );

    sleep(Duration::from_millis(1)).await;
}
