//! A screen "router" demonstrating ownership: reactive subtrees that mount and unmount as a
//! whole, without any manual handle bookkeeping.
//!
//! - **Effects own their children.** The router effect below creates per-screen effects on each
//!   run. When the route changes and the router re-runs, the previous screen's effects are
//!   disposed automatically first — the old screen simply stops reacting.
//! - **`on_cleanup` is the unmount hook.** It runs before the router re-runs (and on disposal),
//!   here logging the teardown of the outgoing screen.
//! - **`peek` reads without subscribing.** The About screen shows the tick counter at mount
//!   time but does not react to it.
//!
//! Run with `cargo run --example screen_router`.

use std::time::Duration;

use adaptite::{effect, on_cleanup, signal};
use runite::time::sleep;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Route {
    Home,
    Counter,
    About,
}

#[runite::main]
async fn main() {
    let route = signal(Route::Home);
    let username = signal("ada");
    let tick = signal(0usize);

    // The router. Reading `route` makes it re-run on navigation; each run mounts one screen by
    // creating its effects. Note that the nested effect handles are discarded (`let _ = ...`):
    // the running router effect owns them, keeps them alive, and disposes them before its next
    // run.
    effect({
        let route = route.clone();
        let username = username.clone();
        let tick = tick.clone();
        move || {
            let screen = route.get();
            println!("[router]  mounting {screen:?}");
            on_cleanup(move || println!("[router]  unmounted {screen:?}"));

            match screen {
                Route::Home => {
                    let _ = effect({
                        let username = username.clone();
                        move || println!("[home]    hello, {}!", username.get())
                    });
                }
                Route::Counter => {
                    let _ = effect({
                        let tick = tick.clone();
                        move || println!("[counter] tick = {}", tick.get())
                    });
                }
                Route::About => {
                    // A snapshot, not a subscription: `peek` reads the tick without making it a
                    // dependency, so this screen never re-renders on ticks.
                    let _ = effect({
                        let tick = tick.clone();
                        move || {
                            println!(
                                "[about]   adaptite demo (tick was {} at mount)",
                                tick.peek()
                            )
                        }
                    });
                }
            }
        }
    })
    .leak();

    let turn = |label: &'static str| async move {
        sleep(Duration::from_millis(10)).await;
        println!("-- {label}");
    };

    turn("tick twice; only mounted screens that read `tick` react (Home does not)").await;
    tick.update(|t| *t += 1);

    turn("").await;
    tick.update(|t| *t += 1);

    turn("rename the user; the Home screen re-renders").await;
    username.set("grace");

    turn("navigate to Counter; Home unmounts and stops reacting").await;
    route.set(Route::Counter);

    turn("rename the user again; nothing happens -- Home is gone").await;
    username.set("linus");

    turn("tick; the Counter screen reacts").await;
    tick.update(|t| *t += 1);

    turn("navigate to About").await;
    route.set(Route::About);

    turn("tick twice more; About took a snapshot and stays silent").await;
    tick.update(|t| *t += 1);

    turn("").await;
    tick.update(|t| *t += 1);

    sleep(Duration::from_millis(10)).await;
    println!("-- done");
}
