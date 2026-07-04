use std::time::Duration;

use adaptite::{effect, signal};
use runite::{main, time::set_timeout};

#[main]
fn main() {
    // Creates an observable value. Calling `.get` from within an observer will create a dependency, and calling `.set`
    // will trigger updates to any dependent observers.
    let v = signal(5);

    // Creates an observer that prints the value of `v` whenever it changes.
    // Calling `.leak()` on the effect handle allows it to run for the lifetime of the program without automatically
    // disposing when dropped.
    effect({
        let v = v.clone();
        move || {
            println!("v is: {}", v.get());
        }
    })
    .leak();

    // Queue a future to wait 5 seconds and then update `v`. This will trigger
    // the effect to run again and print the new value.
    set_timeout(Duration::from_secs(5), {
        let v = v.clone();
        move || {
            v.set(v.get() + 20);
        }
    });
}
