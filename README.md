# adaptite

Fine-grained reactivity for [runite](https://github.com/willmtemple/runite) programs.

Adaptite provides reactivity primitives for dependency tracking and incremental
computation. Those primitives are:

- `Signal<T>`: a tracked-state value cell, primitively observable — the
  "signal" familiar from other fine-grained reactivity libraries.
- `effect`: a primitive observer that runs once, observes its dependencies, and
  runs again whenever its dependencies change.
- `Thunk<T>`: a tracked-state recomputable value defined by a closure,
  recomputed on read after any of its dependencies change. A `Thunk` is both an
  observer and an observable.
- `Memo<T>`: a `Thunk` with an equality (or custom comparator) gate — if a
  recomputation produces an equal value, downstream observers do not re-run.
- `Event<T>`: a push-style source of events of type `T`. Immediate subscribers
  (`subscribe`) run inline with `emit`; draining subscriptions (`on`) queue
  values and deliver them in order on the microtask flush. Subscriptions
  cancel on drop.
- `Resource<T>`: reactive async state — a value fetched by a future,
  re-fetched (with stale fetches aborted) whenever its tracked inputs change.
- `watch`: an explicitly-scoped observer — only its source closure is tracked,
  and its handler runs untracked with the new and previous values.
- `scope`/`on_cleanup`/`owner`: ownership for reactive subgraphs — dispose a
  whole tree of effects at once, register teardown that runs before an effect
  re-runs, and re-attach async work to its owner after an `.await`.
- `Observable`: the common trait over everything readable (`Signal`, `Thunk`,
  `Memo`, `Resource`), with `DynObservable<T>` for type-erased handles in
  component APIs.
- `Source`: a low-level observable node for building custom reactive data
  structures with sub-container granularity (per-key, per-field), including
  `is_observed` for garbage-collecting dependency units nobody reads.

Adaptite is built for the runite runtime: effects, draining subscriptions,
and resources are flushed or spawned on runite's queues, so anything that
schedules work must run on a runtime-managed thread. (Pure signal, thunk, and
memo graphs can be read and written without a runtime; nothing that *reacts*
can.)

Adaptite does not function across thread boundaries. It tracks dependencies
between entities on the same thread only. Async work feeds the graph from the
edges by updating signals or emitting events.

## The reactivity model

### Lazy, glitch-free propagation

Writes are cheap: setting a signal marks its direct dependents dirty and their
transitive dependents "check", and nothing recomputes until it is read.
On read, a computed node verifies whether its recorded inputs actually changed
(refreshing them first) and recomputes only if so. This makes propagation
glitch-free — a computation can never observe a half-updated ("torn") view of
the graph — and guarantees each node recomputes at most once per change, even
in diamond-shaped graphs.

`Signal::set` suppresses writes of equal values entirely. A `Memo` whose
recomputation produces an equal value (under `PartialEq` or a custom
comparator via `memo_by`) does not propagate further, so downstream effects
skip their re-runs.

### Effects and scheduling

Effects never run inline with the write that triggered them. They are queued
on the reactor's job queue and flushed on the runtime's microtask queue, so
consecutive writes within one task coalesce into a single effect run — batching
is implicit. Host integrations that need synchronous propagation (for example,
native resize loops) can call `Reactor::flush_now`.

### Feedback loops

An effect may write state it depends on, as long as the loop converges — for
example clamping a value, normalizing input, or syncing two representations.
Convergence is reached when the rewritten value is equal to the existing one
and the write is suppressed. A loop that never converges is a bug: in debug
builds, an effect that runs more than 100 times in a single flush panics with
the effect's creation site instead of hanging the event loop.

Synchronous read cycles (a thunk whose computation reads itself, directly or
transitively) have no convergent interpretation and always panic, reporting
the cycle path with the source location of each node. For "reduction"-style
computations that want their own previous value, use `memo_with_prev`, which
passes the last value into the compute closure without creating a cycle.

### Ownership and cleanup

Effects and event subscriptions created during another effect's run (or inside
`scope(...)`) are owned by it: they stay alive without their handles being held
and are disposed when the owner re-runs or is disposed. `on_cleanup` registers
teardown against the innermost owner; it runs before the owning effect's next
run and on disposal. Top-level effects are owned by their `EffectHandle` —
dropping the last handle disposes the effect, and `leak()` opts out.
`Subscription` handles follow the same rules.

Ownership is established by where code *runs*, which async work escapes: after
an `.await`, the original owner is no longer on the stack. Capture it first
with `owner()` and re-enter with `Owner::run_in` so effects created after the
suspension are still disposed with their scope.

### Async data

`resource(source, fetch)` connects the graph to runite's async side: `source`
runs tracked and produces the fetch input; `fetch` returns a future that is
spawned on the runtime. When the input changes (equality-gated) or `refetch()`
is called, a new fetch starts and the superseded one is aborted; a stale
completion can never overwrite a newer value. The resource exposes the latest
value (`None` until first completion) and a separately-tracked `loading` flag,
so a UI can render stale data with a spinner during refetch.

### Untracked reads

`untrack(|| ...)` suspends dependency recording, and `signal.peek()` /
`with_peek(...)` read a single value without recording. Computed nodes are
still brought up to date before an untracked read.

## Examples

### Observe a signal using an effect

```rust,no_run
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

    // Schedule a callback to run after 5 seconds and update `v`. This will trigger
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

```rust,no_run
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

```rust,no_run
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

More complete demonstrations live in [`examples/`](./examples): a
dependency-switching spreadsheet, a streaming stock ticker with running
statistics and threshold alerts, and an ownership-based screen router.

## Tracing

Adaptite emits [`tracing`](https://docs.rs/tracing) diagnostics under the
targets `adaptite::graph`, `adaptite::signal`, `adaptite::thunk`,
`adaptite::memo`, `adaptite::effect`, `adaptite::event`, `adaptite::scope`,
and `adaptite::resource`. See `examples/tracing_subscriber_showcase.rs` for a
suggested subscriber setup.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE))
- MIT license ([LICENSE-MIT](./LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
