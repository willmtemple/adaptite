# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

Initial release.

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
- Diagnostics: reactive cycle errors report the cycle path with each node's
  creation site; debug builds panic (instead of hanging) on divergent effect
  feedback loops and detect cross-reactor reads.

[Unreleased]: https://github.com/willmtemple/adaptite/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/willmtemple/adaptite/releases/tag/v0.1.0
