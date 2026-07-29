# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Public graph queries on `Reactor`: `observer_count`, `dependents_of`,
  `dependencies_of`, `node_origin`, and `node_version`. All five existed internally;
  none was reachable. Together they answer "why did this update" without a
  subscription — `dependencies_of` returns each recorded edge with the version observed
  when it was recorded, so the dependency whose current `node_version` no longer matches
  is the one that invalidated the observer. `observer_count` is `O(1)` and
  allocation-free (the dependent set is already indexed by node), which makes it usable
  in a per-frame leak assertion; `is_observed` is now defined in terms of it and keeps
  its documented "late, never early" semantics. `node_origin` exposes the
  `#[track_caller]` creation site that until now surfaced only inside a
  `ReactCycleError`, the divergence panic, or a diagnostic event.
- Ownership accounting. `ownership_stats()` returns an `OwnershipStats`: live owner frames,
  pending cleanup registrations, owned children, and cumulative totals for owners created
  and disposed and cleanups registered and run. A reactive graph can be perfectly clean and
  still leak, because ownership retains what the graph never sees — an effect that never
  re-runs keeps every cleanup it registered, and a scope nobody disposed keeps its children.
  `GraphSnapshot` now carries these alongside the graph counters, since the two questions
  are almost always asked together.
  These are **thread-scoped rather than per-reactor**, because adaptite's ownership is: a
  `scope` has no reactor and never did, and a frame's parent is whatever was innermost when
  it was created. Reporting per-reactor would mean inventing an attribution the
  implementation does not have.
  Two mechanisms keep the numbers honest. Where a count is the population of a live object,
  **the count is that object's lifetime** — an `OwnerFrame` holds a tally that increments on
  construction and decrements on drop, so `live_owners` cannot disagree with reality, not
  because every call site was updated but because there is no call site. Where a count is
  not an object lifetime — cleanups and children live in `Vec`s — it is maintained
  explicitly and then audited: `audit_ownership()` recomputes every live gauge by walking a
  registry of live frames, and `debug_assert_ownership_consistent()` fails on any
  disagreement. Both are named and gated after `debug_assert!`: the registry is not built
  when `debug_assertions` is off, so the audit answers `None` there and the assertion
  compiles to nothing, which means a test suite that calls it still builds under `--release`.
  The ownership tests call it after every operation, including after each of 400 steps of a
  deterministically-shuffled workload.
- `Reactor::debug_graph()` returns a `GraphSnapshot`: every live node with its id, kind,
  creation origin, version, staleness and edge counts, plus every recorded edge, plus a
  `GraphStats` taken at the same moment. The walking counterpart to `graph_stats()`, and
  the distinction is the point — `graph_stats` is `O(1)` and answers *how much*, safe to
  call every frame; this walks the graph and answers *what*, for a human, an inspector, or
  a post-mortem. Nodes and edges are sorted, so two snapshots can be diffed directly.
  Reading a snapshot never refreshes a computed node, so `state` reports staleness rather
  than resolving it and an inspection cannot perturb what it is inspecting; `stale()`
  filters to the nodes that are not clean. Sources report `state: None` rather than a
  misleading `Clean`, since they have no computation to bring up to date. Node naming and
  a `serde` export remain deferred — no consumer has asked, and `#[track_caller]` origins
  already give the human anchor at no runtime cost.
- Per-flush work totals. `DiagnosticEvent::FlushFinished` now carries a `FlushStats`:
  root writes, nodes marked (split check/dirty), maximum propagation depth, effects
  queued/coalesced/run/skipped/disposed/pending, computed nodes
  verified/recomputed/changed/suppressed, edges added and removed, and the job queue depth
  at both ends. `FlushStats::is_empty()` is the assertion an idle application wants: a
  settled graph produces either no flush or an empty one, so "idle is idle" stops being a
  CPU percentage that varies between runs of the same build.
  Work is attributed to **the next flush that closes**, exactly once. An inner flush's
  totals are not rolled up into the enclosing one, so summing a capture double-counts
  nothing; and work performed outside any flush — the writes that scheduled it — is handed
  to the flush that drains it, so a write and the effect run it causes land in the same
  totals. Note that `computed_changed + computed_suppressed` is *at most*
  `computed_recomputed`: a computation that unwound published nothing and is neither.
  Unlike `GraphStats`, these counters are maintained **only while a diagnostic
  subscription is active**. The rule is that counters backing a query must always be true,
  while counters backing an event follow the event — and `FlushStats` is only ever observed
  by being delivered in one. Measured cost with diagnostics off, against the same
  benchmarks without the feature: within noise on the three graph-walking benchmarks (one
  of them measures 4.4% *faster* with the feature) and about +3% on the 18 ns
  `signal_write_read` microbenchmark, which is the single predictable branch now guarding
  propagation-depth tracking. An earlier cut that tracked depth unconditionally cost 15.7%
  there, for the same reason the first computed-work cut was expensive: a drop obligation
  on a hot path.
- Computed-work diagnostics. Four new events make the middle of a propagation visible, where
  before only its endpoints were: `ComputedInvalidated` (every mark that reaches a thunk
  or memo, still carrying the original root write rather than blaming the node above
  it), `ComputedVerified` (whether a check-marked node resolved from cache or was forced
  to recompute), and the `ComputedRecomputeStarted`/`ComputedRecomputeFinished` pair. The
  pair closes even when a computation unwinds, reporting `ComputeOutcome::Panicked`; a
  dependency cycle surfaces as `Panicked`, because that is what it is. `changed` on the
  finish event distinguishes a memo whose comparator suppressed propagation from one that
  published, and `dependencies_before`/`dependencies_after` show a computation whose
  reactive read set grows or churns.
  Adaptite deliberately does **not** report individual edge additions and removals.
  Edge recording is the hottest path in the graph — one call per tracked read — so a
  wide node would emit more diagnostic events than it does reactive work, to answer a
  question the two dependency counts already answer at `O(1)`.
  The first implementation of this cost about 9% on the recompute-heavy benchmarks *with
  diagnostics off*, because the paired-event guard put a drop obligation on the
  recomputation path whether or not anything was listening. Moving the guard behind the
  subscription check, so the dormant path constructs nothing, brought that back inside
  run-to-run noise: the without-the-feature build now measures 0.7–3.1% *slower* on three
  of the four benchmarks, which is the noise floor rather than a real difference.
- `Reactor::dependency_count(node)` — the `O(1)`, allocation-free counterpart to
  `dependencies_of`.
- `Reactor::graph_stats()` returns a `GraphStats`: an `O(1)`, `Copy` account of what a
  reactor is holding. Current gauges (live nodes, live nodes per kind, live edges,
  observed nodes, queued effects, pending jobs, flush depth and epoch), peaks (nodes,
  edges, pending jobs), and cumulative totals (nodes created and disposed, edges added
  and removed, flushes). Taking a snapshot never walks the graph and never evaluates a
  reactive computation, so it is safe to call every frame; the intended use is the
  difference between two snapshots, which turns a leak into an assertion.
  **Every counter is maintained in ordinary builds, always** — there is no capture to
  start and no mode in which the numbers are absent. That was a deliberate choice over
  scoping peaks and cumulative counts to an active diagnostic session, on the grounds
  that one mode is cheaper to document than two modes are to explain, and it is defended
  by `benches/graph.rs` rather than by assertion: against the same benchmarks without
  the counters, the difference sits inside run-to-run noise (`signal_write_read` and
  `wide_fanout` show no change at p > 0.05, `deep_chain` -0.5%, and `layered_diamonds`
  measures the *uncounted* build as 3.9% slower, which is the noise floor talking).
  `graph_stats` itself measures 7.1 ns over a 1,000-node graph.
  Per-kind counts are read with `live_nodes_of_kind(NodeKind)` rather than a public
  array, so that adding a `NodeKind` stays additive.
- Node kinds and node lifecycle diagnostics. The public `NodeKind` names the primitive a
  node was allocated as — `Source`, `Signal`, `Event`, `Thunk`, `Memo`, `Effect` — and
  `Reactor::node_kind` reports it for any live node. Two new diagnostic events,
  `NodeCreated` and `NodeDisposed`, give creation and disposal evidence for *every* node
  kind rather than only for effects, which is what leak and graph-growth attribution
  needs; `NodeDisposed` carries the dependency and dependent counts sampled before
  teardown empties the maps, so a leak report sees the edges the node died holding.
  Disposal is idempotent but the event is delivered exactly once, even though several
  `Drop` impls reach it. The kind is declared at construction, not inferred: a primitive
  built on `source()` reports `Source`, a `Writable` reports `Memo`, and `Resource` and
  `watch` compose existing nodes rather than contributing one of their own.
- `Signal::id()`, `Thunk::id()`, `Memo::id()`, and `Event::id()` report a handle's node
  id, joining the `Source::id` that already existed. Without them the queries above were
  unreachable for every node kind except sources and effects — a consumer holding a
  `Signal` had no way to name it.

- `EffectHandle::id()` and `EffectHandle::reactor_id()` report an effect's node identity
  and the graph it belongs to. `EffectRun::id()` already exposed the same `NodeId`, but
  only from the first *scheduled* run — one run later than a consumer that wants to key
  a retained structure by effect at creation. Node ids are process-local and never
  reused (the allocator is a monotonic counter and disposal does not return an id), so
  an id kept past disposal dangles but can never come to mean a different node; pair it
  with `is_disposed` when liveness matters. `reactor_id` completes the
  `(ReactorId, NodeId)` pair that every diagnostic payload is scoped by.

- [`docs/diagnostics.md`](docs/diagnostics.md) states the whole contract in one place:
  identity and id-reuse rules, the callback contract, dormancy, pairing and panic
  semantics for every started/finished pair, flush attribution under nesting, which
  counters are always maintained and which follow the event stream, the measured costs,
  and the A/B procedure for changing a hot path. It also records the trap this release hit
  twice — a `Drop` guard constructed on a hot path is not free even when its body is a
  no-op — with the fix pattern, so the next diagnostic added does not rediscover it.

### Changed

- `Reactor::current()` now warns whenever it installs a default implicitly on a thread
  that has had one **at any earlier point**, rather than only when a previously installed
  default expired. The old rule missed the case a UI framework actually hits: a framework
  that scopes `enter` to renders and callbacks leaves the thread with no default in
  between, so state created from a timer, a task, a `Drop`, or a test body was a *first*
  install on an empty slot — silently joining a graph nobody flushes, which is exactly the
  failure the warning exists to catch. `enter()` now records that the thread has had a
  default, and the new rule is a superset of the old one. A thread that never entered a
  reactor stays quiet, so scripts, doctests and tests are unaffected.

### Breaking

- Every variant of `DiagnosticEvent` is now `#[non_exhaustive]`, not just the enum
  itself. `#[non_exhaustive]` on an enum forbids exhaustive matching of *variants*; the
  fields of a struct variant still matched exhaustively, so
  `DiagnosticEvent::FlushFinished { reactor, flush_epoch, remaining_jobs } => …`
  compiled and would have broken the moment a field was added. Variant patterns now
  need a trailing `..`, which is the whole fix. Done first in this release because the
  diagnostics work that follows adds fields to existing variants; without it each of
  those additions would be a separate breaking change.

## [0.2.0] - 2026-07-28

This release moves adaptite onto runite 0.2, makes the ambient reactor an
explicit contract, and adds the extension points a UI framework needs from the
reactive core: consumer-defined effect scheduling, error boundaries, and
observation lifecycle hooks. See [MIGRATING-0.2.md](docs/MIGRATING-0.2.md).

### Breaking

- Adaptite now requires runite 0.2 (`runite = "0.2"`). Adaptite and the
  application must resolve the same runite — they share its thread-local
  microtask queue — so an application on runite 0.1 must move in lockstep. The
  `^0.1` requirement adaptite 0.1.2 declared made runite 0.2 unreachable from
  every application in the tree. No adaptite API changed: adaptite's library
  code touches exactly `queue_microtask` and `spawn`, neither of which changed,
  and no runite type appears in adaptite's public API. Applications that use
  runite directly should read runite's
  [0.1 → 0.2 migration guide](https://github.com/willmtemple/runite/blob/main/docs/MIGRATING-0.2.md);
  the changes that need an audit there are fallible owned-resource adoption,
  `run()` cancelling tasks still pending at quiescence, and `select!` no longer
  polling in lexical order.

### Added

- `scope_catch(f, on_error)` creates an ownership scope that catches panics from
  the effects it owns, at any depth, and delivers them to the handler as an
  `ErrorInfo` (payload, message, failing node, and the effect's creation site)
  instead of unwinding out of the flush. The nearest enclosing boundary wins and
  boundaries nest; with no boundary above it, a panic propagates exactly as
  before. The whole run is covered, including dependency verification, since
  that executes upstream computations. The panicking effect is disposed before
  the handler runs — its dependency tracking was cut short mid-run, and a panic
  during verification re-queues it, so leaving it live would re-run and re-panic
  immediately — which makes the failure terminal for that effect and leaves the
  handler to decide what replaces it. Siblings are unaffected. Coverage follows
  ownership, so an effect created inside `unowned` sits outside every boundary
  above it. Boundaries are for bugs; recoverable failures still belong in the
  graph as `Result` values, and under `panic = "abort"` there is nothing to
  catch.
- `source_with_hooks(on_watch, on_unwatch)` (plus `source_with_hooks_in` and
  `Reactor::source_with_hooks`) fires when a source gains its first observer and
  loses its last, so an external resource can be acquired and released promptly
  rather than swept. `Source::is_observed` answers the same question by polling
  and remains the right tool for GC sweeps. Delivery is deferred to a reactor
  job — the "last observer left" transition occurs while the reactor holds its
  graph maps borrowed — which also means a leave/arrive pair inside one flush
  (an observer rerunning) collapses to nothing, and neither hook is ever
  delivered twice in a row. "Observed" means any recorded dependency edge, so
  `on_unwatch` can be late but never early; the finer TC39
  `Signal.subtle.watched` notion of transitive liveness is deliberately not
  implemented yet.
- `writable(get, set)` (plus `writable_in`) creates a two-way bindable derived
  value: a normal memo bundled with a setter that translates an assignment into
  upstream writes, run untracked. No new dependency semantics — the upstream
  write invalidates the getter through the ordinary graph, and a value-identical
  round trip is absorbed by equality suppression. The new `WritableObservable`
  trait (`Observable` + `set`) is implemented by both `Signal` and `Writable`,
  so component APIs can accept either.
- `Observable::map(f)` derives a `Memo` while cloning the receiver's handle
  internally, removing the `let x = x.clone();` line before the closure in the
  dominant derive-a-value case. The derived memo is built on the receiver's own
  reactor, so mapping a node from an explicit reactor stays on that reactor.
  (A `clone!` macro remains deliberately deferred.)
- `Observable::reactor()` reports the reactor backing an observable, defaulting
  to `None` for implementations with no graph node. `Signal::reactor`,
  `Thunk::reactor`, and `Memo::reactor` expose the same on the concrete handles.
- Consumer-defined effect scheduling. `effect_with(scheduler, f)` (plus
  `effect_with_in` and `Reactor::effect_with`) hands each ready run to an
  `EffectScheduler` — any `Fn(EffectRun)` — which decides when it runs. Marking,
  coalescing, and dependency verification stay in the reactor; only *where the
  ready effect runs* moves. Consumers build effect phases from this (one queue
  per phase, drained in the order they choose), so a render lane can run inside
  a host's paint callback instead of on the microtask queue, and adaptite ships
  no opinion about what the phases are.
- `Reactor::external_flush(f)` marks a consumer's drain as one flush: every
  `EffectRun` executed inside shares a flush epoch, keeping the debug divergence
  guard meaningful across the drain and reporting it to diagnostic consumers as
  a single `FlushStarted`/`FlushFinished` pair. A run executed outside any flush
  opens one of its own. Nesting joins the enclosing flush.
- `EffectRun` exposes `id()` and `is_stale()` for schedulers that key queues by
  node or prune entries for disposed effects. Discarding a run instead of
  running it is supported: the effect keeps its dirty mark and is scheduled
  again on its next invalidation, so a lane may drop work for a subtree that is
  no longer visible without stranding it.
- `Reactor::try_current()` (and the free `try_current()`) returns
  `Option<Reactor>` without installing a reactor, so code that must run on an
  existing graph can tell "the application's reactor" from "a fresh graph
  nobody flushes" instead of silently getting the latter.
- `Reactor::enter()` installs a reactor as the thread default and returns an
  `EnterGuard` holding a *strong* reference for its lifetime. The ambient
  reactor becomes a fact rather than a race with whoever holds the last handle.
  Entering nests; dropping a guard restores the previous default, including
  none.
- `Reactor::id()` exposes the process-local `ReactorId`. Two handles address the
  same graph exactly when their ids match, which is how a consumer confirms that
  ambient constructors landed on the reactor it expected.

### Changed

- `Reactor::current()` logs at `warn` on the `adaptite::graph` target when it
  has to install a *replacement* default — that is, when a previously installed
  default expired. Nodes created on either side of that point are on separate
  graphs and can never interact, and because writes on an unflushed graph mark
  dependents stale without scheduling anything, the failure is otherwise silent.
  The first install on a thread stays a `debug`-level event; implicit
  installation remains the default for scripts and tests.
- Documented the contract for reactive state created outside a component: such
  nodes join the ambient reactor and this is supported, with `enter()` as the
  supported way for a host framework to guarantee which reactor that is.
- Documented the runite version contract: adaptite tracks one runite minor at a
  time, and an application should take whatever runite adaptite resolves rather
  than pinning its own. `mise run runite-current` reports when a newer runite
  minor has shipped and is therefore unreachable downstream; CI runs it
  advisory-only.

## [0.1.2] - 2026-07-25

### Added

- Opt-in reactive diagnostics through `Reactor::subscribe_diagnostics`, emitted
  in release builds as well as debug builds. The stream reports root writes with source
  creation and mutation locations, carries those root causes through computed
  dependencies, and reports effect scheduling and coalescing, flushes, runs,
  verification skips, and disposal.
- Process-local `ReactorId` and public numeric accessors for `ReactorId` and
  `NodeId`, allowing diagnostic consumers to correlate independent graphs.

### Changed

- Signal, event, and source mutation entry points preserve their caller
  locations in diagnostic events. Without a subscriber, diagnostic event
  construction and delivery remain dormant.

## [0.1.1] - 2026-07-17

### Added

- `unowned(|| ...)`: runs a closure with no current reactive owner. Effects,
  scopes, and subscriptions created inside are not adopted by an enclosing
  effect or scope — they are kept alive by their handles and disposed when the
  last handle drops. Lets facades and background work opt out of adoption
  without creating a root scope.

## [0.1.0] - 2026-07-13

Initial release.

### Added

- `Signal<T>` tracked-state cells with equality-suppressed `set`, plus
  `replace`, `update`, `with`, `get`, and untracked `peek`/`with_peek`.
- Lazy computed nodes: `Thunk<T>` (always propagates) and `Memo<T>`
  (equality- or comparator-gated). `memo_with_prev` passes the previous value
  into the compute closure for reduction-style computations.
- Glitch-free, lazy invalidation: writes mark the graph; computed nodes verify
  recorded dependency versions on read and recompute at most once per change,
  even in diamond-shaped graphs.
- `effect` observers scheduled on the runite microtask queue with implicit
  batching; unchanged memo results suppress downstream effect runs.
- Ownership: effects created inside another effect's run (or inside
  `scope(...)`) are disposed with their owner; `on_cleanup` registers teardown
  that runs before re-runs and on disposal.
- `Event<T>` push-style events with immediate subscribers and reactive
  draining subscriptions (`on`); subscriptions cancel on drop.
- `Source` low-level observable nodes for custom reactive data structures.
- `untrack` for dependency-free reads.
- `Observable` trait unifying reads across `Signal`/`Thunk`/`Memo`/`Resource`,
  with `DynObservable<T>` for type-erased reactive handles (including
  `DynObservable::constant`).
- `Resource<T>`: reactive async state fetched by a future, equality-gated
  refetch on input change, explicit `refetch()`, a tracked `loading` flag, and
  abort-on-supersede/dispose with stale-completion protection.
- `watch(source, handler)`: explicitly-scoped observation — the source closure
  is tracked and equality-gated; the handler runs untracked with new and
  previous values.
- `owner()` / `Owner::run_in` / `ScopeHandle::owner`: capture the current
  reactive owner and re-enter it after async suspension, so late-created
  effects are still disposed with their scope.
- `Reactor::is_observed` / `Source::is_observed` for garbage-collecting
  per-key dependency units in fine-grained data structures.
- Explicit reactors: `Reactor::new` and `*_in` constructor variants keep
  several independent graphs on one thread; `Reactor::flush_now` flushes
  queued reactive jobs synchronously for host integrations.
- Handle types (`Signal`, `Thunk`, `Memo`, `Event`) are cloneable without
  requiring `T: Clone`.
- Diagnostics: reactive cycle errors report the cycle path with each node's
  creation site; debug builds panic (instead of hanging) on divergent effect
  feedback loops and detect cross-reactor reads.

[Unreleased]: https://github.com/willmtemple/adaptite/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/willmtemple/adaptite/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/willmtemple/adaptite/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/willmtemple/adaptite/releases/tag/v0.1.0
