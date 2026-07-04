# adaptite

Automatic thread-stack-based reactivity for runite programs.

Adaptite provides an implementation of fine-grained reactivity primitives
for dependency tracking and incremental computation. Those primitives are:

- `Signal<T>`: a tracked-state value cell, primitively observable (the same
  concept is called a "signal" in most other reactor implementations).
- `effect`: a primitive observer that runs once, observes its dependencies, and
  runs again whenever its dependencies change.
- `Thunk<T>`: a tracked-state recomputable value defined by a closure.
  Invalidated if any of its dependencies change. A `Thunk` is both an observer
  and an observable.
- `Event<T>`: a push-style source of events of type `T`. Supports subscription
  and cancellation of interest in events.

Adaptite requires the runite runtime to function, and cannot be used on
threads not managed by the runite runtime.

Adaptite does not function across thread boundaries. It tracks
dependencies between entities on the same thread only.

## Examples

### Observe a signal using an effect

```rs
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
```

### Observe a thunk using an effect

```rs
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
```

### Use an event to handle intra-thread messaging

```rs
use std::{cell::Cell, rc::Rc, time::Duration};

use adaptite::event;
use runite::{
    main,
    time::{set_interval, sleep},
};

#[main]
fn main() {
    let my_event = event::<String>();

    // Subscriptions are cancelled when dropped; leak this one so it lives for the whole program.
    my_event
        .subscribe(|message| {
            println!("got event with message: {message}");
        })
        .leak();

    // Emit an event every 250ms with an incrementing count.
    let interval = set_interval(Duration::from_millis(250), {
        let counter = Rc::new(Cell::new(0));
        move || {
            let count = counter.get();
            my_event.emit(format!("the count is {}", count));
            counter.set(count + 1);
        }
    });

    // After 5 seconds, clear the interval to stop emitting events.
    runite::spawn(async move {
        sleep(Duration::from_secs(5)).await;
        interval.cancel();
    });
}
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE))
- MIT license ([LICENSE-MIT](./LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
