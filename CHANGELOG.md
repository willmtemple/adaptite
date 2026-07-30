# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-07-30

This release is about making the reactive graph explain itself: what caused a render, what a
flush cost, what the graph is holding, and what it is retaining that it should not be. The
diagnostics contract is stated once, in full, in
[`docs/diagnostics.md`](docs/diagnostics.md); [`MIGRATING-0.3.md`](docs/MIGRATING-0.3.md) covers
the two changes that need action. Adaptite now requires `runite = "0.3"`.

### Added

- Public graph queries on `Reactor`: `observer_count`, `observers_of`,
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
- `Reactor::graph_snapshot()` returns a `GraphSnapshot`: every live node with its id, kind,
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
  settled graph does not flush at all (see **A drain with nothing to drain is no longer a
  flush**, under `Changed`), and an `external_flush` over one reports an empty `FlushStats` —
  either way "idle is idle" stops being a CPU percentage that varies between runs of the same
  build.
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
- `DiagnosticEvent::WriteSuppressed` and `FlushStats::writes_suppressed` report a write a
  source's own equality check threw away. Such a write never reaches the graph — no version
  bump, no propagation, no flush — so it appears nowhere in the propagation stream by
  construction, and until now the only record was a `tracing` event behind
  `debug_assertions`, absent from exactly the builds worth measuring. But the producer still
  ran: something computed a value and discarded it. Kiln's case is the argument — a sampler
  whose signal changed 14 times had *run* about 80, and only the second number says to slow
  the sampler down. `root_writes + writes_suppressed` is how often something tried to write;
  `root_writes` alone is how often it mattered. The event carries the write's origin, so the
  discarded work is attributable to the call site that produced it. Reported in ordinary
  builds; measured cost with diagnostics off is unmeasurable — a suppressed write is 4.33 ns
  and the build *without* the reporting benchmarks 5.1% slower, which is the noise floor.
- Computed-work diagnostics. Four new events make the middle of a propagation visible, where
  before only its endpoints were: `ComputedInvalidated` (every mark that reaches a thunk
  or memo, still carrying the original root write rather than blaming the node above
  it), `ComputedVerified` (whether a check-marked node resolved from cache or was forced
  to recompute), and the `ComputedRecomputeStarted`/`ComputedRecomputeFinished` pair. The
  pair closes even when a computation unwinds, reporting `ComputeOutcome::Panicked`; a
  dependency cycle surfaces as `Panicked`, because that is what it is. `changed` on the
  finish event distinguishes a memo whose comparator suppressed propagation from one that
  published, and `dependencies_before`/`dependencies_after` show a computation whose
  reactive read set changes size. They do *not* detect a read set of constant size whose
  members change — swapping 200 dependencies for 200 different ones reports 200 either
  side, and the per-flush edge totals cannot distinguish it either, because every
  recomputation clears and re-records its whole set. Sampling `dependencies_of` either side
  of a recomputation, or diffing two `graph_snapshot` snapshots, is the tool for that.
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

- Generic accessors on `DiagnosticEvent`: `reactor()`, `node()`, `node_origin()` and
  `flush_epoch()`. Both the enum *and* every variant are `#[non_exhaustive]` (see `Breaking`),
  which is exactly what stops a downstream crate destructuring a field that every variant
  happens to carry — without these, reading the reactor off an event means one match arm per
  variant, re-audited every release. Adaptite can match exhaustively because
  `#[non_exhaustive]` does not bind the defining crate, so the accessors stay correct as
  variants are added. `reactor()` is total; the other three return `Option`, and each one's
  rustdoc says which events it answers for. `node_origin()` is worth preferring over
  `Reactor::node_origin` in a trace sink, because that query answers only for *live* nodes and a
  sink processing events after the fact is exactly the case where the node is already gone.
- `NodeKind::all()` enumerates the six kinds, so a per-kind breakdown of
  `GraphStats::live_nodes_of_kind` is a loop rather than a hand-kept list that silently goes
  stale when a kind is added.
- `Reactor::node_state(node)` reports how stale one node is — the `O(1)`, allocation-free
  counterpart to walking `graph_snapshot()` and finding the node in it. `GraphSnapshot::node(id)`
  is the lookup for a snapshot you already took (binary search over the sorted nodes).
- `OwnershipStats::is_empty()` — nothing retained: no live owners, no pending cleanup
  registrations, no owned children. The assertion a teardown test wants.
- `ReactorId` implements `Display`, matching `NodeId`'s formatting, so the `(reactor, node)`
  pair every diagnostic is scoped by can be printed without reaching for `get()` on one half.
- Three type names worth knowing because they appear in the signatures above:
  `RecordedDependency` (what `dependencies_of` returns per edge — the dependency's node id and
  the version observed when the edge was recorded), and `OwnershipAudit` / `OwnershipGauge`
  (what `audit_ownership()` returns, and which gauge a reported drift is about).

- `examples/idle_audit.rs` shows how to prove an application is idle rather than inferring
  it from a CPU percentage: subscribe, mark the point at which start-up has settled, and read
  the flushes. It also pins the difference between `Signal::set`, which compares and
  suppresses an unchanged write before it reaches the graph, and `Signal::replace`, which does
  not — the shape behind "a pane re-rendered at the frame rate because an unchanged value was
  written every tick". Deliberately small enough to copy into an application: what counts as
  "settled" is an application's policy, not a reactive graph's.
- [`docs/MIGRATING-0.3.md`](docs/MIGRATING-0.3.md) covers the two changes that need action
  (variant patterns needing `..`, and runite 0.3) and the four behaviour changes that need no
  source edit but are worth knowing: a settled graph no longer flushes, **a divergent feedback
  loop now panics in release builds instead of hanging**, teardown is total, and the
  ambient-reactor warning fires in more cases.
- [`docs/diagnostics.md`](docs/diagnostics.md) states the whole contract in one place:
  identity and id-reuse rules, the callback contract, dormancy, pairing and panic
  semantics for every started/finished pair, flush attribution under nesting, which
  counters are always maintained and which follow the event stream, the measured costs,
  and the A/B procedure for changing a hot path. It also records the trap this release hit
  twice — a `Drop` guard constructed on a hot path is not free even when its body is a
  no-op — with the fix pattern, so the next diagnostic added does not rediscover it.

### Fixed

- The reactive graph no longer allocates during propagation, verification, or edge
  re-recording. Writing to the head of a 64-deep memo chain and reading the tail cost 383
  allocations and 18.8 kB; a 64-wide fan-out cost 129. Both are now zero. The allocations
  were copies made to release a borrow before calling code that may re-enter the graph —
  now taken from a pool — and hash tables discarded by an observer that was about to refill
  them, which are now emptied in place. `OwnerFrame::reset` likewise keeps the capacity of
  its cleanup and child lists. Effect runs went from 7 allocations to 2 (a boxed job and
  runite's microtask closure), and an effect with a cleanup from 8 to 2. Throughput,
  release, A/B against a saved baseline: edge re-recording -56.9%, deep-chain invalidation
  -43.4%, wide fan-out -32.1%, layered diamonds -31.5%, node create/dispose -10.7%, and no
  benchmark regressed. One consequence worth knowing: `GraphStats::observed_nodes` is now a
  maintained counter rather than the length of the dependents index, because emptied entries
  are retained across an observer's rerun. The value is unchanged and is asserted against a
  walk of the graph in the test suite.
- Teardown now installs an owner *barrier*, closing a silent leak. `OwnerFrame::reset` ran
  cleanups and child disposals without establishing an owner, so they saw whatever owner happened
  to enclose the teardown. A host that calls `flush_now` from inside a `scope` leaves that scope
  on the owner stack while an effect re-runs and tears down — so a cleanup that registered a
  cleanup had it silently adopted by the enclosing scope, outliving the effect it belonged to and
  running when that outer scope died, which for an application root is never. Registering a
  cleanup during teardown is now reported rather than redirected.
- `on_cleanup` called from inside a cleanup says so. It previously claimed the caller was
  "outside a reactive owner", which is false when the caller is demonstrably inside one being
  torn down, and sent the reader after a missing `scope` that was never the problem.
- A thunk or memo that must recompute while its cached value is borrowed now names itself. A
  closure passed to `with` or `with_peek` holds that borrow for its whole body, so invalidating
  the node and reading it back from inside the closure cannot work — documented, but it surfaced
  as a bare `RefCell already borrowed` naming neither the node, its origin, nor `with`. The
  diagnosis now carries all three and says what to do instead.
- The divergence guard is now enforced in **every** build, not only debug. A non-convergent
  feedback loop used to panic with a precise diagnosis in debug and hang `flush_now` forever in
  release — no panic, no log, nothing for a user to report, and for a GUI host a permanently
  frozen application. The builds were backwards relative to where the failure hurts: the
  configuration that said nothing was the one shipped to users. Release now produces the same
  panic as debug, naming the effect and its origin. Cost is below the noise floor of a 235 ns
  effect run (two `Cell` reads, a compare and a branch); the A/B measured the guarded build
  marginally *faster* than the baseline. The regression test was itself `cfg(debug_assertions)`,
  which is how the hang survived unnoticed — it now runs in both profiles.
- An effect that writes state it depends on and then calls `flush_now` no longer re-enters
  itself. Both halves are documented as legal — convergent self-feedback, and synchronous
  propagation for host integrations — but together the nested flush found the job the write
  had just queued and ran the effect from inside its own body, clearing the dependency set
  the outer run was still recording. A run requested while the effect is already running is
  now deferred and queued once the current run finishes, so the loop converges as documented
  and the dependency set survives.
- `FlushStarted` no longer holds a borrow of the job queue across the diagnostic subscriber.
  A subscriber that scheduled reactive work — the obvious thing to do from a flush boundary,
  and something `FlushFinished` already permitted — got a bare `RefCell already borrowed`
  originating inside adaptite.
- **`Event` delivered to subscribers in hash order, contradicting a documented guarantee.**
  `Event::on` says values are drained "in emission order". Its queue is fed by an ordinary
  immediate subscriber, and subscribers were stored in a hash map with a randomly seeded
  hasher — so when another subscriber re-emitted, the nested value could be queued *first*
  and delivered before the value that caused it. Two `on` subscriptions on the same event
  could disagree with each other about the order of the same emit sequence, differently on
  every process run. Subscribers are now stored in a `BTreeMap` keyed by the existing
  monotonic subscription id, so iteration order **is** registration order. This also makes
  immediate-subscriber order deterministic, which was never specified and never stable.
- **Re-entering a running computation is now refused in every build.** It was a
  `debug_assert`, so release builds fell through into `clear_observer_dependencies` and
  wiped the dependency set of a computation that was still recording it — the node emerged
  with whatever subset of its inputs the inner run happened to re-read, silently. The check
  now runs before anything is mutated and in all profiles; the `insert` it tests already ran
  in release, so refusing costs nothing.
- **`watch` held a `RefCell` borrow across its handler.** A handler that wrote the watched
  source and forced a flush — a combination the README explicitly sanctions — re-entered the
  effect and panicked with a bare `BorrowMutError` in release while debug hit the reactor's
  re-entrancy assert, so the two profiles disagreed about what went wrong. The previous value
  is now cloned out before the handler runs. A panicking handler still leaves `previous` at
  the last value it handled.

- **`untrack` leaked into computed nodes, freezing them permanently.** `UNTRACKED_DEPTH` is
  a thread-global counter and `run_in_context` never reset it, so a `Thunk` or `Memo` whose
  first recomputation happened inside an untracked region recorded **zero dependencies**,
  settled clean, and was never invalidated again — silently and permanently stale, for every
  reader, not only the untracked one. `untrack` means "do not record this read for whoever is
  currently observing"; entering a computation now starts a fresh tracking scope, which is
  what Solid and Leptos do.
  This was reachable from entirely ordinary code, because the crate runs consumer callbacks
  untracked in seven places — `watch` handlers, `Event` draining and immediate subscribers,
  cleanups, memo comparators, `Signal::set`'s equality check, and `Resource` fetch closures.
  A `watch` handler reading a memo was enough: the memo froze at its first value and
  `.get()` returned it forever, everywhere. Nothing in the diagnostics could distinguish a
  frozen node from a healthy constant one except a dependency count of zero.

- **A cleanup panicking during thread teardown aborted the process.** Every ownership
  counter reached its thread-local with `LocalKey::with`, which *panics* once that value has
  been destroyed — and a panic in a destructor is a non-unwinding abort. Thread-local
  destructors run in reverse registration order, so any host parking an adaptite handle in
  its own thread-local (a component registry, a task queue) had the counters destroyed first
  and took the process down at exit. Debug builds only, because the audit registry is what
  makes the counters need dropping. All accounting now uses `try_with` and no-ops when the
  counters are already gone: losing a decrement during teardown costs nothing, aborting costs
  everything.
- **`GraphStats::queued_effects` climbed without bound.** The pending-run latch was released
  by the queued run or by a discarded `EffectRun`, both of which reach the effect through a
  `Weak`. An effect dropped before its run — an unowned handle going out of scope is the
  ordinary way — made both upgrades fail, so the gauge was never decremented: 1,000
  create-and-drop cycles left it reading 1,000 on an empty graph. `dispose` now releases the
  latch itself, and `FlushStats::effects_pending`, which inherited the same lie, is fixed
  with it.
- **Disposal stranded a node when a cleanup panicked.** `EffectHandle::dispose` ran owner
  teardown and then unhooked from the graph, unprotected — so a panicking cleanup left the
  effect's node metadata and every edge it had recorded in the reactor permanently, while
  `is_disposed()` reported true. The unhook now happens even when teardown unwinds. This
  predates 0.3 but contradicts the teardown contract this release introduced, and the new
  gauges are what made it visible.
- **`external_flush` closed the wrong flush.** It never pinned the epoch it opened, so a
  re-entrant `flush_now` inside it moved the shared epoch on and the outer close reported the
  inner number — one epoch finished twice, another never finished at all. Fatal for a
  consumer keying totals by `flush_epoch`, which is the documented aggregation key.
  `flush_jobs` already pinned its epoch for exactly this reason; the `begin_flush`/`end_flush`
  pair now does too.
- **The audit registry retained every owner frame ever created.** A `Weak` keeps the whole
  allocation alive, and nothing pruned except `audit_ownership`, which applications never
  call: 200,000 scopes grew a debug build by 30 MB while `live_owners` correctly read 0 — a
  leak invisible to the very gauge meant to expose leaks. The registry now compacts as it
  grows.

### Changed

- **A subscription installed while a flush is already open no longer receives that flush's
  `FlushFinished`.** It never received the matching `FlushStarted` — it did not exist yet — so the
  contract that the two are always paired was false in exactly this window, and what arrived was
  an all-zero `FlushStats` for work the subscriber could not have observed. A consumer that
  subscribes from inside an effect, a cleanup, or a diagnostic callback will see one fewer event
  than before; the event it loses carried no information. Pairing now holds unconditionally.
- **A cleanup that moves one of its own effect's dependencies on every teardown now runs until
  the divergence guard fires**, where it used to terminate. The termination was an artifact of the
  re-entrancy bug fixed above: the inline re-entrant run found the cleanup list already taken by
  the outer teardown and silently skipped a whole generation of cleanups, which made a genuinely
  non-convergent loop look like it settled. Every run now gets its teardown, so the loop is
  revealed for what it is and the guard is right to name it. Convergent cleanups — an idempotent
  write that the source's equality check suppresses on the second pass — settle in one extra run,
  as they always did.
  arrived later with an empty queue — and every such arrival opened an epoch, emitted a
  `FlushStarted`/`FlushFinished` pair, and reported an all-zero `FlushStats`. **The signature of
  an idle application is now no flushes at all**, rather than a stream of empty ones; code that
  asserted on flush counts sees different numbers. `external_flush` is unchanged — a boundary
  the consumer declared is reported whether or not the drain found work, and that is where an
  empty `FlushStats` still appears. One corollary: work performed outside a flush is carried by
  the next flush that actually runs, so work never followed by a flush is never reported —
  disposing an effect and then stopping accumulates a disposal no flush arrives to carry. Making
  a flush happen because diagnostics are subscribed would break the rule that subscribing never
  changes behaviour, so this is the honest trade. Pinned by
  `tests/flush_stats.rs::a_settled_graph_reports_an_empty_flush`.
- **Depends on `runite = "0.3"`.** Adaptite tracks one runite minor at a time and cuts a
  release for each, because it is coupled to runite's *scheduler semantics* and must resolve
  the same runite the application does. None of runite's five breaking changes touch adaptite:
  its entire runite surface is `queue_microtask` and `spawn`, and no runite type appears in
  adaptite's public API, so the bump forced no source change. The guarantee adaptite's batching
  rests on — a microtask queued during a turn runs before the next macrotask — is now verified
  by tests in runite rather than merely believed, and a cooperative-scheduling budget that
  could have split a flush in half was considered and declined upstream. Closes #26.
- **Corrected: which writes span a runtime turn.** `docs/diagnostics.md` told consumers to
  stamp diagnostic *events* with the runtime's turn id rather than the per-flush aggregate,
  and justified it with "a write made from a task and the flush that drains it are in
  different turns". The advice was right; the justification was backwards, and runite 0.3's
  `current_turn()` made it measurable for the first time. A spawned task is polled *inside*
  the microtask checkpoint, which drains to quiescence, so a task's write and its flush share
  one turn. It is a **macrotask** write — running after the checkpoint has already drained —
  whose flush lands in the next turn. `tests/runtime_join.rs` pins both, because these are
  properties of runite's scheduling that nothing in adaptite's own suite would notice changing.
- **Teardown is total.** `OwnerFrame::reset` documented that a panicking cleanup does not
  strand its siblings; it did. A panic abandoned the teardown loop, the remaining cleanups
  were dropped rather than run, and because cleanups ran before children were taken, **the
  owner's children were never disposed at all** — a leak with no error attached, on the path
  reached by every effect re-run. Every cleanup and every child now receives an attempt, in
  reverse registration order, each under its own `catch_unwind`. The **first** panic is
  preserved and re-raised once teardown finishes, later ones logged and dropped: subsequent
  failures are commonly caused by the first. Teardown reached from `Drop` while the thread is
  *already* unwinding logs the captured panic instead of re-raising it, because re-raising
  there aborts the process — the one case where a cleanup panic is not observable as a panic,
  and the only alternative to an abort. Fixes #28.
- **Nested flush semantics are no longer self-contradictory.** `external_flush` documented
  that a re-entrant `flush_now` joins the enclosing flush, while the implementation, the
  diagnostics contract and the tests all gave it a distinct epoch. Adaptite now maintains two
  identities: a *diagnostic flush epoch*, which a re-entrant `flush_now` does take afresh so
  its totals stay separable, and a *logical drain*, which it does not — and the divergence
  guard counts against the drain. Without that separation an effect that re-flushes could
  hand itself a new epoch on every run and walk past the guard. `external_flush` nested in
  `external_flush` still joins outright, opening no new epoch.

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

[Unreleased]: https://github.com/willmtemple/adaptite/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/willmtemple/adaptite/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/willmtemple/adaptite/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/willmtemple/adaptite/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/willmtemple/adaptite/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/willmtemple/adaptite/releases/tag/v0.1.0
