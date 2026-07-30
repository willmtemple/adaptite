# The diagnostics contract

adaptite can explain itself: which write caused which effect to run, what a flush cost, what the
graph is holding, and where every node came from. This document is the contract that surface
comes with — what is guaranteed, what it costs, and what is deliberately absent.

Four surfaces, with different rules:

| | What it answers | Cost | Availability |
|---|---|---|---|
| [`DiagnosticEvent`] stream, including [`FlushStats`] | *Why* — causality and ordering, and what each flush cost | Dormant without a subscriber | Opt-in via `subscribe_diagnostics` |
| [`GraphStats`] | *How much* — what the graph holds | `O(1)`, no walk | **Always maintained** |
| [`OwnershipStats`] | *What is retained* — owner frames, cleanups, children | `O(1)`, no walk | **Always maintained**, thread-scoped |
| [`GraphSnapshot`] | *What* — every node and edge | `O(nodes + edges)`, allocates | On demand |

One rule decides that last column:

> **Counters that back a query are always maintained. Counters that back an event follow the
> event.**

`GraphStats` and `OwnershipStats` back queries that can be called at any moment, so their numbers
must always be true. `FlushStats` is only ever *observed* by being delivered on `FlushFinished`,
and an event nobody subscribed to is not delivered — so those counters are maintained only while a
subscription is active.

---

## Ownership is thread-scoped, not per-reactor

[`OwnershipStats`] is the one thing here that is not keyed by reactor, and that is not an
oversight. Ownership in adaptite is a thread-local stack: a [`scope`] has no reactor and never
did, and a frame's parent is whatever was innermost when it was created. Reporting per-reactor
would mean inventing an attribution the implementation does not have. An application that owns one
reactor per thread — the shape adaptite is built for — gets the same answer either way.

It matters because the graph can be clean while ownership leaks. An effect that never re-runs
keeps every cleanup it registered; a scope nobody disposed keeps its children; a component frame
held one generation too long keeps a whole subtree. The nodes are gone and the closures are not,
so [`GraphStats`] cannot see any of it.

## Keeping a gauge honest

A gauge nobody checks drifts, and a drifted leak gauge is worse than none — it makes a real leak
look fine. Two mechanisms, and the choice between them is worth copying for any counter added
later:

1. **Where a count is the population of a live object, make the count that object's lifetime.**
   An `OwnerFrame` holds a tally that increments when constructed and decrements when dropped.
   `live_owners` cannot disagree with reality, not because every call site was updated but
   because there is no call site to forget.
2. **Where it is not** — cleanups and adopted children live in `Vec`s — maintain it explicitly
   and then *audit* it. `audit_ownership()` recomputes every live gauge by walking a registry of
   live frames; `debug_assert_ownership_consistent()` fails on disagreement. The ownership tests
   call it after every operation, including after each of 400 steps of a deterministically
   shuffled workload, so a path added without its bookkeeping fails the suite rather than shipping.

The audit is gated like `debug_assert!` and named for it: the registry is not built when
`debug_assertions` is off, because making every application pay a `Weak` push per owner frame to
hold a proof nobody reads would be the tail wagging the dog. `audit_ownership()` answers `None`
there rather than a misleading empty result, and the assertion compiles to nothing — so a test
suite that calls it still builds under `--release`, exactly as one full of `debug_assert!` would.

## Identity

Everything is scoped `(ReactorId, NodeId)`.

- `NodeId` is unique **within one reactor**. Aggregating several graphs without the `ReactorId`
  will alias nodes.
- Ids are **process-local and never reused**. The allocator is a monotonic counter and disposal
  does not return an id to it, so an id retained past disposal dangles but can never come to mean
  a different node. That is what makes it safe to key a retained structure by node id and clean
  up lazily.
- `ReactorId` is process-local and monotonic for the same reason.
- Every handle can name its node: `Source::id`, `Signal::id`, `Thunk::id`, `Memo::id`,
  `Event::id`, `EffectHandle::id`, and `EffectRun::id` (which is `Option`, because it holds a weak
  reference). `EffectHandle::reactor_id` supplies the other half of the pair.

## Origins

Every node records its creation site via `#[track_caller]`. It is available from
`Reactor::node_origin(id)` for any live node, on the `NodeCreated`/`NodeDisposed` events, on
`InvalidationCause` for the node written and the site of the write, and on `GraphNode::origin`.

This is deliberately the *only* naming mechanism. An optional debug label on node constructors
has been considered and deferred: no consumer has asked, `(ReactorId, NodeId)` is what they
correlate on, and a `&'static str` per node is steady-state cost in every application that would
never use it.

## No clock, ever

**adaptite does not measure time and will not acquire the ability to.** It is `core` + `alloc`
over runite; taking a monotonic timestamp per event would cost more than emitting the event, and
a reactive graph is the wrong layer to own a clock.

Every event is ordered and every span is paired, so a consumer that wants a duration timestamps
the pair itself — `FlushStarted`/`FlushFinished`, `EffectRunStarted`/`EffectRunFinished`,
`ComputedRecomputeStarted`/`ComputedRecomputeFinished`. Every field of `FlushStats` and
`GraphStats` is a count.

## Work that produces nothing is still work

Two places where adaptite does something and then throws the result away, both reported, because
in each case the *producer ran* and the propagation stream cannot show it — nothing propagated:

| | Reported by | Says |
|---|---|---|
| A source write the value did not change | `WriteSuppressed`, `FlushStats::writes_suppressed` | Something is writing more often than the value moves |
| A recomputation the comparator suppressed | `ComputedRecomputeFinished { changed: false }`, `FlushStats::computed_suppressed` | A computation is running more often than its result moves |

Both are cheap by design — that is the point of the equality checks — but cheap is not free, and a
gate that saves the downstream work also hides the upstream work from anything watching
propagation. `root_writes + writes_suppressed` is how often something *tried*; `root_writes` alone
is how often it mattered.

## What adaptite does not report

- Application or renderer memory. adaptite accounts for the structures it owns; a consumer
  accounts for component output, scenes, caches and GPU resources separately.
- Individual dependency edge additions and removals. Edge recording is the hottest path in the
  graph — one call per tracked read — so a wide node would emit more diagnostic events than it
  performs reactive work.
  Be precise about what replaces it: `dependencies_before`/`dependencies_after` detect a read set
  that changes **size**, and nothing detects a read set of constant size whose *members* change.
  The per-flush `edges_added`/`edges_removed` totals cannot either, because every recomputation
  clears and re-records its whole edge set, so churn and stability are indistinguishable there.
  For identity churn, sample `Reactor::dependencies_of` either side of a recomputation or diff two
  `Reactor::graph_snapshot` snapshots — targeted-investigation tools, not per-frame ones.
- Anything through `tracing`. See [Tracing is not a contract](#tracing-is-not-a-contract).

---

## The event stream

`Reactor::subscribe_diagnostics(callback)` returns a `DiagnosticSubscription`. Dropping it
unsubscribes.

### Callback contract

- Delivered **synchronously, on the reactor's thread**, in the order the work happened.
- A callback **must not** mutate the same reactive graph, and **must not** add or remove
  diagnostic subscriptions. Copy the fields you need into an external sink.
- A callback that panics unwinds through whatever reactive work was in progress. Do not panic in
  a callback.

### Synchronous delivery is a feature, not an implementation detail

Because a callback runs on the reactor thread at the moment the work happens, a consumer can
attribute events to whatever ambient context it has — including context adaptite knows nothing
about. The motivating case is joining adaptite's records to the runtime's: a `ReactiveWrite`
callback runs *during* the write and a `FlushFinished` callback runs *during* the flush, so a
consumer that stamps each event with `runite::current_turn()` gets per-event turn attribution
without adaptite holding a runtime type in its API.

```rust,ignore
reactor.subscribe_diagnostics(move |event| {
    sink.push(Record { turn: runite::current_turn(), event });
});
```

This matters more than it looks, because a `FlushStats` **can** span two runtime turns, and which
writes do so is not the obvious answer. The boundary is the microtask checkpoint, and it falls in
one place only:

| Where the write happens | Write and flush |
| --- | --- |
| A spawned task | **The same turn.** A task is polled inside the microtask checkpoint, and the checkpoint drains to quiescence, so the flush the write queues runs before that same turn closes. |
| A macrotask, or any callback the runtime invokes after the checkpoint | **Different turns.** The checkpoint has already drained, so the queued flush waits for the next turn to open. |
| A synchronous `flush_now` | The same turn as its caller, wherever that is. |

Adaptite deliberately folds the write into the flush's totals so cause and effect stay in one
record. In the macrotask row that record therefore straddles a turn boundary, and stamping the
aggregate with a single turn id would be wrong in a way that looks right.

Both rows are pinned by `tests/runtime_join.rs`, because they are properties of runite's
scheduling rather than of adaptite's, and a change to them would otherwise make this section
quietly false rather than loudly wrong.

The division to hold onto:

> Use the **event stream** for attribution, and the **aggregate** for volume.

Which is the same division as everywhere else here: events explain causality, stats quantify it.

### Subscribing never changes behaviour

Turning diagnostics on must not turn a stale-node no-op, a disposal, a panic, or a cycle into
something different. This is tested (`diagnostics_do_not_change_stale_node_trigger_behavior`) and
is a hard rule for any future event: if reporting something would require changing when it
happens, the event does not get added.

### Dormancy

- No subscriber means no event construction beyond one boolean check.
- The single-subscriber delivery path does not allocate.
- Dropping the last subscription stops delivery immediately and resets the partly-accumulated
  `FlushStats`, so a later subscriber never inherits totals from a window it could not observe.

The third bullet is what the suite tests, in two parts.
`every_event_stops_when_the_last_subscription_drops` produces every variant, drops the
subscription, and asserts that a second run of the same workload delivers nothing; it matches
every variant exhaustively — `#[non_exhaustive]` binds downstream crates, not adaptite itself, so
**adding a variant fails to compile until it is covered there**.
`flush_totals_do_not_survive_an_unsubscribed_window` accumulates into the pending `FlushStats`
while subscribed, drops that subscription, and asserts a later subscriber sees only its own
totals.

The first two bullets are properties of the code path rather than assertions: nothing in the
suite counts allocations, and the dormant path is defended by the benchmark A/B described under
[Cost](#cost-and-how-to-keep-it-honest) — which is what caught the `Drop`-guard trap twice.

### Pairing and panic semantics

| Started | Finished | On unwind |
|---|---|---|
| `EffectRunStarted` | `EffectRunFinished` | Still emitted; the guard fires on the unwind path |
| `ComputedRecomputeStarted` | `ComputedRecomputeFinished` | Still emitted, with `outcome: Panicked` and `changed: false` |
| `FlushStarted` | `FlushFinished` | Still emitted; remaining jobs are handed to a fresh flush |

A pair always closes. `EffectRunSkipped` is *not* paired — it replaces the run entirely, reporting
that verification proved the body unnecessary.

`ComputeOutcome` has two variants, `Completed` and `Panicked`. A dependency cycle surfaces as
`Panicked`, because that is what it is: the cycle check panics with a `ReactCycleError` message
naming the path and unwinds like any other panic. Variants adaptite cannot actually produce are
deliberately absent, and the enum is `#[non_exhaustive]` so a distinguishable outcome can be
added later.

### Exhaustiveness

Both `DiagnosticEvent` and **every one of its variants** are `#[non_exhaustive]`. A downstream
`match` needs a wildcard arm *and* a trailing `..` in each variant pattern. This is what makes
adding a field additive rather than breaking, and these payloads are expected to grow.

---

## Two flush identities

Flushes nest, and two different questions are asked of the nesting, so adaptite keeps two
identities:

| | Changes when | Used for |
|---|---|---|
| **Diagnostic flush epoch** (`flush_epoch`) | Every flush, including a re-entrant `flush_now` | Attribution — keeping a nested flush's totals separable |
| **Logical drain** | Only the outermost flush | The divergence guard |

A re-entrant `flush_now` therefore takes a fresh diagnostic epoch but stays inside the enclosing
drain. Collapsing them either way breaks something: sharing the epoch loses per-flush attribution,
and bumping the drain would let an effect that writes its own dependency and then re-flushes hand
itself a new epoch on every run and never trip the guard.

`external_flush` nested inside `external_flush` is different again — it joins outright and opens
no epoch at all, because the consumer already declared that boundary.

## Flush attribution

`FlushStats` arrives on `FlushFinished`. Work is attributed to **the next flush that closes**,
and counted exactly once.

- Work performed **during** a flush belongs to the innermost flush open at the time. Flushes
  nest: a re-entrant `flush_now` from inside a job opens a genuine inner epoch. An inner flush's
  totals are **not** rolled up into the enclosing one, so summing every flush in a capture
  double-counts nothing.
- Work performed **outside** any flush — most importantly the writes that scheduled it — is
  handed to the flush that drains it. A write and the effect run it causes therefore land in the
  same totals, which is what makes `root_writes` answer "what set this flush off".
- A nested `external_flush` **joins** the enclosing flush rather than opening a new epoch, by
  design, so it contributes no separate totals. Only the outermost `external_flush` opens one.

`FlushStats::is_empty()` ignores the job-queue depths and asks only whether any reactive work
happened. A settled graph does not flush at all, so the usual idle assertion is that no flush
arrived; `is_empty()` is for the flush an `external_flush` reports over a settled graph, where the
boundary was declared but there was nothing to do.

One arithmetic caveat worth knowing: `computed_changed + computed_suppressed` is **at most**
`computed_recomputed`, not equal to it. A computation that unwound published nothing and is
neither; the difference is the number that failed.

---

## Cost, and how to keep it honest

The dormant path is the load-bearing claim, because a consumer that cannot afford diagnostics in
ordinary builds has to choose between measuring and shipping. RUIN already declines designs over
steady-state reactive cost, so "negligible when off" is a requirement rather than a nicety.

### Measured

Diagnostics off, against the same benchmarks with the feature removed:

| Feature | Cost |
|---|---|
| Graph counters (`GraphStats`) | Within noise; one bench measured the *uncounted* build 3.9% slower |
| Computed-work events | Within noise; uncounted measures 0.7–3.1% slower on three of four benches |
| Flush totals | Within noise on the graph-walking benches; **+3.0%** on the 18 ns `signal_write_read` microbenchmark |
| `graph_stats()` itself | 7.1 ns over a 1,000-node graph, flat in graph size |

### The trap, hit twice

**A guard that exists and does nothing is not free.**

The first implementation of computed-work diagnostics cost **~9%** on recompute-heavy benchmarks
with diagnostics off, and the first implementation of flush totals cost **15.7%** on a bare
signal write. Both for the same reason: a `Drop` type was constructed on a hot path regardless of
whether anyone was subscribed. A drop obligation is not free even when its body is a no-op.

Both were fixed the same way. This is contributor guidance rather than consumer API — the gate
it shows, `Reactor::diagnostics_enabled`, is `pub(crate)` — but it is the pattern to follow for
any diagnostic added to adaptite itself:

```rust,ignore
// Wrong: the guard exists whether or not anyone is listening.
let mut span = Span::open(reactor, node);   // has a Drop impl
do_the_work();
span.completed();

// Right: the branch is at the call site, the guard lives on the cold side of it.
if !reactor.diagnostics_enabled() {
    do_the_work();
    return;
}
let mut span = Span::open(reactor, node);
do_the_work();
span.completed();
```

### Measuring a change

Criterion's rolling comparison is **too noisy for this** — it produced a bogus +21.8% reading
during 0.3's development that had to be bisected to disprove. Use a saved baseline and an
explicit A/B:

```sh
# 1. Baseline the tree with your change in it.
cargo bench --bench graph -- --warm-up-time 1 --measurement-time 3 --save-baseline mine

# 2. Take the change out and compare against it.
git stash
cargo bench --bench graph -- --warm-up-time 1 --measurement-time 3 --baseline mine
git stash pop
```

A change is inside the noise floor when the signs are inconsistent across benchmarks — in
particular, when *removing* the feature measures slower on some of them, which happened
repeatedly at ±4%.

`benches/graph.rs` covers the paths that matter: `signal_write_read` (the floor),
`edge_churn_32_rerecord` (the maintained edge counters, on the hottest path in the graph),
`node_create_and_dispose` (per-kind gauges and lifecycle events), the propagation shapes, and
`graph_stats_1000_nodes` (which must not scale with graph size).

### Why there is no CI regression gate

Deliberately not attempted on shared runners. Both real regressions above were 9% and 15.7%,
which is *inside* the run-to-run variance of a GitHub-hosted runner — a threshold loose enough
not to flake would not have caught either, and a threshold tight enough to catch them would flake
constantly. A perf gate that cries wolf gets disabled, and then it protects nothing.

CI runs the benchmarks so they cannot rot, without asserting on timings. The A/B above is the
gate, and it is a human step before changing a hot path. If this becomes a recurring problem the
answer is a dedicated runner, not a tighter threshold.

---

## Tracing is not a contract

adaptite emits `tracing` events under per-subsystem targets. **These are for humans reading logs
and may change in any release, including a patch.** They are not a machine-readable interface:

- `trace_targets` is `pub(crate)` (`src/lib.rs`) and always has been. The target strings have
  never been public API.
- Many of the most interesting events are `#[cfg(debug_assertions)]` and **do not exist in
  optimized builds** — precisely the builds worth measuring.

A consumer that wants counts should read them off `FlushStats`, which gives the same numbers
(effect runs, memo recomputations, verifications, marks, edge churn) directly, in optimized
builds, without parsing, and with semver behind it.

---

## Reference

- Events and payloads: [`DiagnosticEvent`], [`InvalidationCause`], [`InvalidationLevel`],
  [`ComputeOutcome`], [`NodeKind`] (and `NodeKind::all`, which enumerates the six kinds)
- Reading an event generically, without destructuring a `#[non_exhaustive]` variant:
  `DiagnosticEvent::reactor`, `DiagnosticEvent::node`, `DiagnosticEvent::node_origin`,
  `DiagnosticEvent::flush_epoch`
- Aggregates: [`GraphStats`], [`FlushStats`], [`OwnershipStats`]
- Queries: `Reactor::graph_stats`, `Reactor::graph_snapshot`, `Reactor::observer_count`,
  `Reactor::dependency_count`, `Reactor::dependencies_of`, `Reactor::observers_of`,
  `Reactor::node_origin`, `Reactor::node_kind`, `Reactor::node_state`, `Reactor::node_version`,
  `Reactor::is_observed`. All but `graph_snapshot`, `dependencies_of` and `observers_of` are
  `O(1)` and allocation-free — `node_state` in particular answers a per-node staleness question
  without the walk `graph_snapshot` performs.
- Snapshot types: [`GraphSnapshot`] (plus `GraphSnapshot::node` and `GraphSnapshot::stale`),
  [`GraphNode`], [`GraphEdge`], [`NodeState`], [`RecordedDependency`] (the per-edge element
  `Reactor::dependencies_of` returns: the dependency's node id and the version observed when the
  edge was recorded)
- Ownership: `ownership_stats`, `audit_ownership`, `debug_assert_ownership_consistent`,
  [`OwnershipAudit`], [`OwnershipDrift`], [`OwnershipGauge`]

[`DiagnosticEvent`]: https://docs.rs/adaptite/latest/adaptite/enum.DiagnosticEvent.html
[`InvalidationCause`]: https://docs.rs/adaptite/latest/adaptite/struct.InvalidationCause.html
[`InvalidationLevel`]: https://docs.rs/adaptite/latest/adaptite/enum.InvalidationLevel.html
[`ComputeOutcome`]: https://docs.rs/adaptite/latest/adaptite/enum.ComputeOutcome.html
[`NodeKind`]: https://docs.rs/adaptite/latest/adaptite/enum.NodeKind.html
[`GraphStats`]: https://docs.rs/adaptite/latest/adaptite/struct.GraphStats.html
[`FlushStats`]: https://docs.rs/adaptite/latest/adaptite/struct.FlushStats.html
[`GraphSnapshot`]: https://docs.rs/adaptite/latest/adaptite/struct.GraphSnapshot.html
[`GraphNode`]: https://docs.rs/adaptite/latest/adaptite/struct.GraphNode.html
[`GraphEdge`]: https://docs.rs/adaptite/latest/adaptite/struct.GraphEdge.html
[`NodeState`]: https://docs.rs/adaptite/latest/adaptite/enum.NodeState.html
[`RecordedDependency`]: https://docs.rs/adaptite/latest/adaptite/struct.RecordedDependency.html
[`OwnershipStats`]: https://docs.rs/adaptite/latest/adaptite/struct.OwnershipStats.html
[`OwnershipAudit`]: https://docs.rs/adaptite/latest/adaptite/enum.OwnershipAudit.html
[`OwnershipDrift`]: https://docs.rs/adaptite/latest/adaptite/struct.OwnershipDrift.html
[`OwnershipGauge`]: https://docs.rs/adaptite/latest/adaptite/enum.OwnershipGauge.html
[`scope`]: https://docs.rs/adaptite/latest/adaptite/fn.scope.html
