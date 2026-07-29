# The diagnostics contract

adaptite can explain itself: which write caused which effect to run, what a flush cost, what the
graph is holding, and where every node came from. This document is the contract that surface
comes with — what is guaranteed, what it costs, and what is deliberately absent.

Three mechanisms, with different rules:

| | What it answers | Cost | Availability |
|---|---|---|---|
| [`DiagnosticEvent`] stream | *Why* — causality, ordering | Dormant without a subscriber | Opt-in via `subscribe_diagnostics` |
| [`GraphStats`] | *How much* — what the graph holds | `O(1)`, no walk | **Always maintained** |
| [`GraphSnapshot`] | *What* — every node and edge | `O(nodes + edges)`, allocates | On demand |

The governing rule for the second and third rows:

> **Counters that back a query are always maintained. Counters that back an event follow the
> event.**

`GraphStats` backs `Reactor::graph_stats()`, which can be called at any moment, so its numbers
must always be true. `FlushStats` is only ever *observed* by being delivered on
`FlushFinished`, and an event nobody subscribed to is not delivered — so those counters are
maintained only while a subscription is active.

---

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

## What adaptite does not report

- Application or renderer memory. adaptite accounts for the structures it owns; a consumer
  accounts for component output, scenes, caches and GPU resources separately.
- Individual dependency edge additions and removals. Edge recording is the hottest path in the
  graph — one call per tracked read — so a wide node would emit more diagnostic events than it
  performs reactive work. `dependencies_before`/`dependencies_after` on the recompute pair and
  `edges_added`/`edges_removed` per flush answer the question that motivated the request, at
  `O(1)`.
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

Both are tested: `every_event_stops_when_the_last_subscription_drops` matches every variant
exhaustively — `#[non_exhaustive]` binds downstream crates, not adaptite itself, so **adding a
variant fails to compile until it is covered there**.

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
happened. A settled graph produces either no flush or an empty one — the assertion an idle
application should make instead of watching a CPU percentage.

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

Both were fixed the same way, and this is the pattern to follow for any new diagnostic:

```rust
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
  [`ComputeOutcome`], [`NodeKind`]
- Aggregates: [`GraphStats`], [`FlushStats`]
- Queries: `Reactor::graph_stats`, `Reactor::debug_graph`, `Reactor::observer_count`,
  `Reactor::dependency_count`, `Reactor::dependencies_of`, `Reactor::dependents_of`,
  `Reactor::node_origin`, `Reactor::node_kind`, `Reactor::node_version`, `Reactor::is_observed`
- Snapshot types: [`GraphSnapshot`], [`GraphNode`], [`GraphEdge`], [`NodeState`]

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
