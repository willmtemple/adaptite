# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/willmtemple/adaptite/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/willmtemple/adaptite/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/willmtemple/adaptite/releases/tag/v0.1.0
