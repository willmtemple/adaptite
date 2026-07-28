# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
