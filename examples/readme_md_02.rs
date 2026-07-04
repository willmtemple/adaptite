use std::time::Duration;

use adaptite::{effect, signal, thunk};
use runite::{
    main,
    time::{set_interval, set_timeout},
};

#[main]
fn main() {
    // Two primitive observable values.
    let x = signal(5);
    let y = signal(10);

    // A derived observable value that depends on `x` and `y`. The closure will only run when `x` or `y` change, and the
    // result will be cached until then.
    let z = thunk({
        let x = x.clone();
        let y = y.clone();
        move || {
            println!("calculating z...");
            x.get() + y.get()
        }
    });

    // The effect observes `z`, so it will run whenever `z` changes. Because `z` depends on `x` and `y`, the effect will
    // run whenever `x` or `y` change.
    effect({
        let z = z.clone();
        move || {
            println!("z is: {}", z.get());
        }
    })
    .leak();

    // Update `x` and `y` every second. This will trigger the effect to run and print the new value of `z`.
    let interval = set_interval(Duration::from_secs(1), {
        let x = x.clone();
        let y = y.clone();
        move || {
            x.update(|value| *value += 1);
            y.update(|value| *value += 2);
        }
    });

    // After 10 seconds, clear the interval to stop updating `x` and `y`. Once the interval is cleared, the queue will
    // empty and the program will exit since there are no more pending tasks.
    set_timeout(Duration::from_secs(10), move || {
        println!("clearing interval...");
        interval.cancel();
    });
}
