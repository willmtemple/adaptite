# What the other reactive systems do, and what adaptite should take from them

A survey done while 0.3 waited on runite, to make the roadmap something other than bug-driven.
Systems read: **Leptos** (Rust), **Vue 3.5**, **SolidJS**, **Angular signals**, **Svelte 5 runes**,
the **TC39 Signals proposal**, and **futures-signals** (Rust).

This is a design record, not a plan. Each candidate is filed as its own issue; the value of having
it in one place is the comparison.

---

## 1. Where adaptite is already ahead

Worth stating first, because it bounds what is worth copying.

| Capability | adaptite | Others |
|---|---|---|
| Observation lifecycle hooks (`source_with_hooks`) | yes | Only TC39 `Signal.subtle.watched`/`unwatched`. Vue, Solid, Leptos: none |
| Consumer-defined effect scheduling (`effect_with`) | yes — arbitrary phases | Vue: fixed `pre`/`post`/`sync`. Solid/Leptos: fixed |
| Typed diagnostic stream, flush totals, ownership accounting | yes | Nothing comparable anywhere. TC39 has `introspectSources`/`Sinks`; the rest have devtools protocols |
| Explicit multiple reactors | yes | Leptos: one arena (or sandboxed per-task). Vue/Solid/Angular: global |
| Divergence guard on feedback loops | yes (debug) | None. Solid/Vue can hang |
| Cycle detection reporting the path with origins | yes | Solid throws; Vue warns |
| Writable computed (`writable`) | yes | Vue `computed` get/set; Angular `linkedSignal` (different shape); Solid/Leptos: none |
| No clock, no allocator hooks, no global runtime | contractual | — |

The observation-hook and effect-scheduling points are the two where adaptite is meaningfully
*ahead of the field*, not merely on par.

## 2. The one big idea worth taking: Leptos's arena

Leptos stores every signal in `SlotMap<NodeId, Box<dyn Any + Send + Sync>>` and hands out the key.
A `RwSignal<T>` is therefore an integer — `Copy`, movable into any number of closures with no
`.clone()` and no refcount traffic. Leptos's own framing: *"a signal is essentially an index into a
data structure held elsewhere… a cheap-to-copy integer type that does not do reference counting."*

This is the single largest ergonomic difference between adaptite and Leptos. adaptite's
`Signal<T>` is `Rc<SignalInner<T>>`, so every closure capture needs an explicit clone — the
complaint recorded from Kiln at 36 sites, 21 of them in one file, and the reason
[#6](https://github.com/willmtemple/adaptite/issues/6) existed.

**adaptite is closer to this than it looks.** It already has:

- `NodeId` and `ReactorId`, both `Copy`;
- a reactor that already owns per-node maps keyed by `NodeId`;
- **monotonic, never-reused ids** — stronger than SlotMap's generational reuse, because there is no
  ABA case at all, only "absent".

What is missing is only that the *value* lives in the handle rather than in the reactor.

### What Leptos pays for it

Both failure modes are documented in their own book, and both are real:

- **Use-after-dispose.** `.get()` on a disposed signal panics; `try_get()` returns `None`. Their
  guidance is essentially "do not store a signal above its owner".
- **Leaks.** A signal created high and stored in a collection is not disposed when removed from
  that collection — it lives as long as its owner.

Leptos's answer is a *parallel type for every primitive*: `ArcRwSignal`, `ArcReadSignal`,
`ArcMemo`, refcounted and safe, alongside the arena ones. Two families of every type, and the
unsafe-for-lifetime one is the default.

### What adaptite should do differently

Ship the same capability with **the defaults inverted**. `Signal<T>` stays refcounted and stays the
default; the `Copy` handle is opt-in and named so that its lifetime rule is obvious. A consumer
that never opts in cannot hit either failure mode, and a consumer that wants ergonomics in a
component tree opts in where the owner tree genuinely governs lifetime.

Design sketch and the open questions are on the issue.

## 3. Smaller ideas worth taking

**Solid's `createSelector`** is the most elegant thing in the survey. Selecting one row out of *n*
naively means *n* comparisons against the selected id on every change, because every row's memo
depends on it. `createSelector` inverts the subscription: it keeps a map from key to subscribers,
so changing the selection notifies exactly the two affected rows. O(1) instead of O(n).

adaptite already has the machinery — a per-key `Source` plus `is_observed` for GC is precisely
what it needs, and `tests/fine_grained.rs` already proves the pattern works on the public API. This
is a small, self-contained primitive with an outsized payoff for list UIs, and unlike
[#1](https://github.com/willmtemple/adaptite/issues/1) it does not need a new crate or a persistent
data structure.

**Vue 3.5's `pause`/`resume`** on watchers and effect scopes. Vue added it deliberately after
shipping only `stop` for years: an offscreen subtree wants its effects quiet without losing its
state, and tearing it down and rebuilding is both expensive and lossy. adaptite has `dispose` and
nothing between "running" and "gone". A terminal with a hidden pane is exactly the case.

**Angular's `linkedSignal`** — writable state that *resets* when a source changes, with the
previous source and previous value available to the computation. The pattern is a form field that
should reset when the selected record changes but stay editable in between. adaptite can express
it with `memo_with_prev` plus a signal, awkwardly; `writable` is a different thing (two-way binding
to an upstream, no local override).

**Vue's `readonly()` / Leptos's `signal()` returning `(ReadSignal, WriteSignal)`.** adaptite has
`Observable` as the read trait, but no way to hand a component a handle that *cannot* write.
`DynObservable` type-erases but does not restrict. This is API hygiene rather than capability.

**A custom comparator on a source.** `Signal::set` is `impl<T: PartialEq>`; a type without
`PartialEq` can only use `replace`, which never suppresses. Memos already have `memo_by`. The
symmetric `signal_by(equals, initial)` is missing, and equality control at the *source* is where
suppression is cheapest — 0.3's `WriteSuppressed` exists precisely because that suppression is
load-bearing.

**Suspense-style aggregate loading.** Leptos `Suspense`, Solid `Transition`, Angular `resource`.
adaptite has `Resource` per-value but no way to ask "is anything under this scope still loading".
Given ownership scopes already exist, this is a scope-scoped counter rather than a new subsystem.

## 4. Ideas considered and not proposed

- **Deep reactive proxies** (Vue `reactive`, Solid stores, Svelte `$state`). Already
  [#2](https://github.com/willmtemple/adaptite/issues/2); Rust has no `Proxy`, so the ergonomics
  need a derive, and nothing has changed to make that more urgent.
- **`batch()` / transactions** (Solid, Preact, MobX). adaptite dropped this at 0.1 because
  microtask coalescing makes it redundant. The survey did not turn up a case that coalescing does
  not already cover, and re-adding it would create two ways to say the same thing.
- **`Send`/`Sync` storage split** (Leptos `SyncStorage`/`LocalStorage`). adaptite is
  single-threaded by contract and that is a feature, not an omission.
- **Reactivity Transform / compiler magic** (Vue dropped it; Svelte runes replaced it). Needs a
  proc-macro and the crate deliberately has none.

## Sources

- [Leptos: The Life Cycle of a Signal](https://book.leptos.dev/appendix_life_cycle.html)
- [Leptos `reactive_graph` signals and effects](https://deepwiki.com/leptos-rs/leptos/2.1-signals-and-effects)
- [Leptos `RwSignal`](https://docs.rs/leptos/latest/leptos/prelude/struct.RwSignal.html)
- [Announcing Vue 3.5](https://blog.vuejs.org/posts/vue-3-5)
- [Vue Reactivity API: Core](https://vuejs.org/api/reactivity-core) and [Advanced](https://vuejs.org/api/reactivity-advanced)
- [Vue RFC: add pause/resume](https://github.com/vuejs/rfcs/discussions/599)
- [Solid `createSelector`](https://docs.solidjs.com/reference/secondary-primitives/create-selector)
- [Solid `reconcile`](https://docs.solidjs.com/reference/store-utilities/reconcile)
- [Angular `linkedSignal`](https://blog.angular-university.io/angular-linkedsignal/)
- [TC39 Signals proposal](https://github.com/tc39/proposal-signals)
