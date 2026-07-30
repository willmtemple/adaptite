# Migrating from adaptite 0.2 to 0.3

adaptite 0.3 is about making the reactive graph explain itself: what caused a
render, what a flush cost, what the graph is holding, and what it is retaining
that it should not be. The full contract is in
[`diagnostics.md`](diagnostics.md).

```toml
[dependencies]
adaptite = "0.3"
```

Two changes need action. Everything else is additive, and four behaviour changes
are worth knowing about even though none of them needs a source edit:

- [a settled graph no longer flushes](#behaviour-change-a-settled-graph-no-longer-flushes)
- [a divergent loop now panics in release](#behaviour-change-a-divergent-loop-now-panics-in-release)
  — the one that can change what a *shipped* application does
- [teardown is total](#behaviour-change-teardown-is-total)
- [the ambient-reactor warning fires more often](#behaviour-change-the-ambient-reactor-warning-fires-more-often)

## Required: `DiagnosticEvent` variant patterns need `..`

Every variant of `DiagnosticEvent` is now `#[non_exhaustive]`, not just the enum.

```rust
// Before
DiagnosticEvent::FlushFinished { reactor, flush_epoch, remaining_jobs } => …

// After
DiagnosticEvent::FlushFinished { reactor, flush_epoch, remaining_jobs, .. } => …
```

`#[non_exhaustive]` on an enum forbids exhaustive matching of *variants*; the
fields of a struct variant still matched exhaustively, so any field added to a
payload was a breaking change. 0.3 adds fields to most of them, and will add
more. Paying for it once here is the point.

The compiler finds every site. If you only ever matched with `..`, there is
nothing to do.

Some of those sites are better deleted than repaired. An arm that destructures a
variant only to read the fields every event has now has an accessor:
`DiagnosticEvent::reactor()`, `node()`, `node_origin()` and `flush_epoch()` are
new in 0.3 and cover the whole enum, so a `match` written to pull the reactor and
node out of sixteen variants collapses to two calls. The accessors are also the
supported way to read those fields generically: `#[non_exhaustive]` is exactly
what stops you writing that `match` yourself in a downstream crate. `reactor()`
is total; `node()`, `node_origin()` and `flush_epoch()` return `Option`, and each
one's rustdoc says which events it answers for.

## Required: runite 0.3

adaptite 0.3 depends on `runite = "0.3"`. As in 0.2, adaptite and your
application must resolve the **same** runite — they share its thread-local
microtask queue, so two copies means adaptite's reactive work is flushed by a
runtime nobody is driving. If your application depends on runite directly, move
it in the same commit; if it reaches runite only through adaptite, do not pin
runite yourself.

adaptite's own runite surface is still exactly `queue_microtask` and `spawn`,
and no runite type appears in adaptite's public API, so nothing in *adaptite's*
surface moved. Your own runite usage may need an audit — read
[runite's 0.2 → 0.3 guide](https://github.com/willmtemple/runite/blob/main/docs/MIGRATING-0.3.md).

One runite 0.3 guarantee is worth knowing because adaptite's batching depends on
it: **a microtask queued during a turn runs before the next macrotask.** That is
why consecutive writes within one task coalesce into a single effect run. It is
verified by tests in runite and will not be relaxed silently.

Two runite 0.3 changes are worth an audit even though adaptite absorbs them for
you.

**runite now emits `tracing` events in release builds.** In 0.2 its steady-state
trace sites were `#[cfg(debug_assertions)]`, so a release binary emitted nothing
per-turn or per-task. They are unconditional now. If your release build installs
a subscriber that accepts everything, it will start receiving runite's events —
and a filter that answers "sometimes" rather than a definite no makes hot sites
pay per emission. Filter runite's targets off explicitly if you are not
collecting them. This does not change adaptite: its own diagnostics are the
`DiagnosticEvent` stream, which is dormant until something subscribes.

**`runite::current_turn()` is the join key between adaptite's records and the
runtime's,** and adaptite deliberately does not call it — see
[`diagnostics.md`](diagnostics.md#synchronous-delivery-is-a-feature-not-an-implementation-detail).
Because diagnostic callbacks run synchronously at the moment the work happens,
you stamp each event yourself and get finer attribution than adaptite could bake
in. Note where the turn boundary actually falls: a write from a **spawned task**
shares a turn with the flush that drains it, while a write from a **macrotask**
does not. Both are pinned by `tests/runtime_join.rs`.

## Behaviour change: a settled graph no longer flushes

`flush_now` runs the job queue directly but cannot unqueue the microtask already
handed to the runtime, so that microtask used to arrive later with an empty
queue — and every such arrival opened an epoch, emitted a
`FlushStarted`/`FlushFinished` pair, and reported an all-zero `FlushStats`.

A drain with nothing to drain is no longer a flush. **The signature of an idle
application is now no flushes at all**, rather than a stream of empty ones. If
you asserted on flush counts, the numbers move.

`external_flush` is unchanged: a boundary you declared is reported whether or not
the drain found work, and that is where an empty `FlushStats` still appears.

One corollary: work performed outside a flush is carried by the next flush that
actually runs, so work never followed by a flush is never reported. Disposing an
effect and then stopping accumulates a disposal that no flush arrives to carry.
Making a flush happen because diagnostics are subscribed would break the rule
that subscribing never changes behaviour, so this is the honest trade. In an
application, where flushes keep coming, it is invisible.

## Behaviour change: a divergent loop now panics in release

An effect that writes state it depends on without converging used to panic in debug builds and
**hang forever** in release. The guard is now enforced in every build, so a release build panics
with the same message debug always gave:

```
adaptite: effect created at src/ui.rs:9:16 ran more than 100 times in a single drain; this
suggests a divergent reactive feedback loop (the effect writes state it depends on without
converging)
```

Convergent feedback is unaffected and settles far below the limit — this only fires on a loop
that was never going to terminate. If a shipped application starts panicking here, it was
previously freezing at the same point.

The measured cost is below the noise floor of an effect run, so this is not a performance
trade. It is the same argument the crate makes against `cfg`-gated diagnostics generally: the
build that omits the safety net is the build that has the problem.

## Behaviour change: teardown is total

`OwnerFrame::reset` documented that a panicking cleanup does not strand its
siblings. It did: the panic abandoned the teardown loop, remaining cleanups were
dropped rather than run, and because cleanups ran before children were taken,
**the owner's children were never disposed at all**.

Now every cleanup and every child receives an attempt, in reverse registration
order, each under its own `catch_unwind`. Two consequences for code that already
had a panicking cleanup:

- Cleanups that used to be skipped now run. If one of them was quietly relying on
  never executing after a failure, it will now execute.
- The panic surfaces **after** teardown finishes rather than immediately, and the
  **first** panic is the one that propagates; later ones are logged and dropped.

Teardown reached from `Drop` while the thread is *already* unwinding logs the
captured panic instead of re-raising it, because re-raising there aborts the
process. That is the one case where a cleanup panic is not observable as a
panic, and the only alternative is an abort.

## Behaviour change: the ambient-reactor warning fires more often

`Reactor::current()` now warns whenever it installs a default implicitly on a
thread that has had one **at any earlier point**, rather than only when a
previously installed default expired.

The old rule missed the case a UI framework produces. A framework that scopes
`enter` to renders and callbacks leaves the thread with no default in between, so
state created from a timer, a task, a `Drop`, or a test body was a *first*
install on an empty slot — silently joining a graph nobody flushes, which is
exactly the failure the warning exists to catch.

A thread that never entered a reactor stays quiet, so scripts, doctests and tests
are unaffected. If the warning is now firing in your application, it is telling
you about state that will never re-render: hold the reactor with
`Reactor::enter` for as long as ambient constructors may run, create state with
`reactor.signal(..)` and friends, or use `Reactor::try_current` to make the
absence an error.

## Worth adopting

### Ask the graph what it is holding

`Reactor::graph_stats()` is `O(1)` — no walk, no allocation, safe to call every
frame — and is maintained whether or not anything is subscribed. The intended use
is the difference between two snapshots, which turns a leak into an assertion:

```rust
let before = reactor.graph_stats();
run_one_iteration();
let after = reactor.graph_stats();
assert_eq!(after.live_nodes, before.live_nodes, "nothing was retained");
```

`Reactor::graph_snapshot()` is the walking counterpart: every node with its kind,
origin, version and staleness, plus every edge, sorted so two snapshots diff
directly. For a human or an inspector, not for a per-frame check.
`GraphSnapshot::node(id)` looks one node up in a snapshot you already took
(binary search over the sorted nodes) rather than scanning the `nodes` vector.

If the question is about **one** node, do not take a snapshot at all:
`Reactor::node_state(id)` answers "how stale is this?" in `O(1)` with no
allocation and no walk, which is what a per-frame assertion wants.
`Reactor::node_kind`, `node_origin`, `node_version`, `observer_count` and
`dependency_count` are the same shape.

`NodeKind::all()` enumerates the six kinds, so a report that breaks
`GraphStats::live_nodes_of_kind` down per kind is a loop rather than a hand-kept
list that goes stale when a kind is added.

### Ask what a flush cost

`FlushFinished` now carries a `FlushStats`: root writes, suppressed writes, marks
delivered, propagation depth, effects queued/run/skipped/coalesced/disposed,
computed nodes verified/recomputed/changed/suppressed, and edge churn. See
`examples/idle_audit.rs` for the whole technique in about forty lines.

### Follow a write through the middle of the graph

0.2 reported which write reached which effect. 0.3 reports what happened in
between: `ComputedInvalidated` (with `state_changed`, distinguishing a mark that
made a node staler from one that coalesced), `ComputedVerified` (cache hit versus
forced recomputation), and the `ComputedRecomputeStarted`/`ComputedRecomputeFinished`
pair with `changed`, an outcome, and before/after dependency counts.

### See work that produced nothing

`WriteSuppressed` reports a write a source's equality check discarded. Such a
write never reaches the graph, so it appears nowhere in the propagation stream —
but the producer still ran. A signal written eighty times and changed fourteen is
a producer running too often, and only this says so.

### Find what ownership is retaining

`ownership_stats()` reports live owner frames, pending cleanup registrations and
owned children. A graph can be clean while ownership leaks: an effect that never
re-runs keeps every cleanup it registered.

These are **thread-scoped**, not per-reactor, because adaptite's ownership is —
a `scope` has no reactor and never did.

In tests, `debug_assert_ownership_consistent()` recomputes every live gauge by
walking the owner tree and fails on disagreement. It is named and gated like
`debug_assert!`: it compiles to nothing when `debug_assertions` is off, so a
suite that calls it still builds under `--release`.

### Name your nodes

`Signal::id()`, `Thunk::id()`, `Memo::id()`, `Event::id()` and
`EffectHandle::id()`/`reactor_id()` join the `Source::id` that already existed.
Everything in the query surface takes a `NodeId`, so without these it was
unreachable for most kinds.

## Not in 0.3, deliberately

- **No clock.** adaptite measures no durations and will not. Every field is a
  count; timestamp the paired events yourself.
- **No per-edge dependency events.** Edge recording is the hottest path in the
  graph. `dependencies_before`/`dependencies_after` detect a read set that
  changes *size*; for one that swaps members, sample
  `Reactor::dependencies_of` either side of a recomputation or diff two
  `graph_snapshot` snapshots.
- **No node labels or `serde` export.** `#[track_caller]` origins already give the
  human anchor at no runtime cost.
- **`tracing` is still not a contract.** The targets are private and several
  events are debug-only, so they are absent from the builds worth measuring. Use
  the typed diagnostics stream.
