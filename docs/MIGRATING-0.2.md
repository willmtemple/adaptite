# Migrating from adaptite 0.1 to 0.2

adaptite 0.2 adds extension points and changes no existing API. The one required
change is the runtime version underneath.

```toml
[dependencies]
adaptite = "0.2"
```

## Required change: runite 0.2

adaptite 0.2 depends on `runite = "0.2"`. adaptite and your application must
resolve the **same** runite — they share its thread-local microtask queue, so two
copies in one dependency tree means adaptite's reactive work is flushed by a
runtime nobody is driving.

If your application depends on runite directly, move it to `runite = "0.2"` in
the same commit. If it reaches runite only through adaptite, there is nothing to
do; do not pin runite yourself.

adaptite's own code touches only `queue_microtask` and `spawn`, neither of which
changed, and no runite type appears in adaptite's public API — so nothing in
*adaptite's* surface moved. Your own runite usage may still need an audit. Read
[runite's 0.1 → 0.2 guide](https://github.com/willmtemple/runite/blob/main/docs/MIGRATING-0.2.md);
the changes that produce no compiler diagnostic are:

- `run()` now cancels tasks still pending when the loop reaches quiescence.
- `select!` rotates its starting arm instead of polling in lexical order. Add
  `biased;` wherever lexical priority was intentional.

Owned-resource adoption (`File::from`, `TcpStream::from`, …) becoming fallible
*is* a compile error, so the compiler will find those for you.

adaptite tracks one runite minor at a time, so reaching a newer runite minor
requires an adaptite release against it. If a runite version you need is out of
reach, that is the thing to ask for.

## No source changes in adaptite's own API

Every 0.1 program compiles unchanged. Everything below is additive.

## Worth adopting

### Anchor your reactor explicitly

`current()` installs a reactor implicitly when the thread has none, and its cache
is weak — if nothing keeps the reactor alive, the next call silently installs a
*fresh, unrelated* one. Nodes created on either side of that point can never
interact, and the symptom is "this value changes and nothing reacts", with no
panic.

If your application owns a long-lived graph, say so:

```rust
let reactor = adaptite::Reactor::new();
let guard = reactor.enter();   // hold for the life of the application
```

The guard holds a strong reference, so the ambient reactor is a fact rather than
a race with whoever holds the last handle. `Reactor::try_current()` returns
`Option<Reactor>` without installing anything, for code where a missing reactor
should be an error. A replacement install now logs at `warn` on the
`adaptite::graph` target.

Creating long-lived signals outside any component — a registry that owns a signal
per background process, read by components that come and go — is supported and
always was. `enter()` is how you guarantee *which* reactor they join.

### Schedule effects yourself

`effect_with(scheduler, f)` hands each ready run to any `Fn(EffectRun)` instead
of the microtask lane, which is how you build effect phases without adaptite
imposing a list:

```rust
let lane: Rc<RefCell<Vec<EffectRun>>> = Rc::new(RefCell::new(Vec::new()));

let effect = effect_with(
    { let lane = Rc::clone(&lane); move |ready: EffectRun| lane.borrow_mut().push(ready) },
    move || redraw(size.get()),
);

// Inside the host's paint callback:
reactor.external_flush(|| {
    for ready in lane.borrow_mut().drain(..) {
        ready.run();
    }
});
```

Drain inside `Reactor::external_flush` so the whole drain counts as one flush
epoch; otherwise each run opens a single-run flush and the debug divergence guard
cannot see a self-rescheduling livelock. Runs must execute on the reactor's
thread. Discarding a run is legal — the effect keeps its dirty mark and is
scheduled again on its next invalidation.

### Contain panics

`scope_catch(f, on_error)` catches panics from the effects a scope owns and
delivers them as an `ErrorInfo` rather than unwinding the whole flush. The
panicking effect is disposed before the handler runs, so the failure is terminal
for it and the handler decides what replaces it; siblings keep running.

Boundaries are for bugs. Recoverable failures — a fetch that 404s, a parse that
fails — still belong in the graph as `Result` values. Under `panic = "abort"`
there is nothing to catch.

### Release resources when nobody is watching

`source_with_hooks(on_watch, on_unwatch)` fires when a source gains its first
observer and loses its last, so a socket or file watcher can be torn down
promptly instead of by an `is_observed` sweep. Delivery is deferred to a reactor
job, which means a rerunning reader's leave/arrive pair collapses to nothing and
neither hook is delivered twice in a row. `on_unwatch` can be late but never
early.

### Smaller conveniences

- `writable(get, set)` gives a form binding one handle that is both readable and
  assignable. `WritableObservable` (`Observable` + `set`) is implemented by both
  `Signal` and `Writable`.
- `Observable::map(f)` derives a memo without the `let x = x.clone();` line.
- `Reactor::id()` and `Observable::reactor()` let a consumer assert that its
  state landed on the reactor it expected.
