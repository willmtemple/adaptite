//! Ownership accounting: what the owner tree is retaining.
//!
//! A reactive graph can be perfectly clean and still leak, because ownership retains things the
//! graph never sees. An effect that is never re-run keeps every cleanup it registered; a scope
//! nobody disposed keeps its children alive; a component frame held one generation too long keeps
//! a whole subtree. None of that shows up in [`crate::GraphStats`] — the nodes are gone, the
//! closures are not.
//!
//! # Why these are thread-scoped
//!
//! Unlike graph counters, these are **not** per-reactor. Ownership in adaptite is a thread-local
//! stack: an [`crate::scope`] has no reactor and never did, and a frame's parent is whatever was
//! innermost when it was created. Reporting per-reactor would mean inventing an attribution the
//! implementation does not have. An application that owns one reactor per thread — which is the
//! shape adaptite is built for — gets the same answer either way.
//!
//! # How these stay accurate
//!
//! Hand-maintained counters drift: someone adds a path that creates the thing and forgets the
//! increment, and the gauge is quietly wrong forever, which is worse than having no gauge. Two
//! mechanisms guard against it here.
//!
//! 1. **Where a count is the population of a live object, the count is that object's lifetime.**
//!    [`OwnerTally`] increments when it is created and decrements when it is dropped, and an
//!    `OwnerFrame` holds one. There is no way to make a frame without making a tally and no way
//!    to destroy one without dropping it, so `live_owners` cannot disagree with reality — not
//!    because every call site was updated, but because there is no call site.
//!
//! 2. **Where a count is not an object lifetime — cleanups and adopted children live in `Vec`s —
//!    it is maintained explicitly and then *audited*.** Debug builds keep a registry of live
//!    frames, and [`audit_ownership`] recomputes every gauge by walking it. The registry costs nothing in
//!    release; the audit runs in every ownership test, so a missed decrement fails the suite
//!    rather than shipping.

use alloc::rc::Rc;
use core::cell::Cell;

// The audit registry, and everything that walks it, exists in debug builds only.
#[cfg(debug_assertions)]
use alloc::rc::Weak;
#[cfg(debug_assertions)]
use alloc::vec::Vec;
#[cfg(debug_assertions)]
use core::cell::RefCell;

use crate::scope::OwnerFrame;

/// What this thread's owner tree is holding.
///
/// Ownership is thread-local in adaptite, so these describe the calling thread rather than any
/// one reactor — see "thread-scoped" in `docs/diagnostics.md`. Counters are maintained in
/// ordinary builds, always;
/// they back a query, and a query must always be true.
///
/// `Copy`, and the cumulative fields never decrease, so the intended use is the difference
/// between two snapshots:
///
/// ```rust
/// use adaptite::{on_cleanup, ownership_stats, scope};
///
/// let before = ownership_stats();
///
/// let (handle, ()) = scope(|| on_cleanup(|| {}));
/// let during = ownership_stats();
/// assert_eq!(during.live_owners - before.live_owners, 1);
/// assert_eq!(during.cleanup_registrations - before.cleanup_registrations, 1);
///
/// handle.dispose();
/// let after = ownership_stats();
/// assert_eq!(after.cleanup_registrations, before.cleanup_registrations);
/// assert_eq!(after.cleanups_run - before.cleanups_run, 1);
/// ```
///
/// A workload whose `live_owners` or `cleanup_registrations` climbs across repetitions is
/// retaining ownership, which is the leak the graph counters cannot see.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OwnershipStats {
    /// Owner frames currently alive — one per live effect, plus one per live scope.
    pub live_owners: usize,
    /// Cleanups registered and not yet run.
    ///
    /// An effect re-registers its cleanups on every run, so this is a steady-state number for a
    /// settled application. A climbing one means cleanups are being registered against an owner
    /// that never resets.
    pub cleanup_registrations: usize,
    /// Effects and scopes currently held by an owner, which will be disposed with it.
    pub owned_children: usize,

    /// Owner frames created over this thread's life.
    pub owners_created: u64,
    /// Owner frames terminally disposed. A frame dropped without an explicit dispose is disposed
    /// by its `Drop`, so this counts those too.
    pub owners_disposed: u64,
    /// Cleanups registered over this thread's life.
    pub cleanups_registered: u64,
    /// Cleanups executed over this thread's life, by a reset, a disposal, or immediate execution
    /// against an already-disposed owner.
    pub cleanups_run: u64,
}

impl OwnershipStats {
    /// Returns `true` when nothing is retained: no live owners, registrations or children.
    ///
    /// The assertion a teardown test wants — that a workload gave everything back.
    #[must_use = "this answers a question, it does not assert it; wrap it in `assert!`"]
    pub fn is_empty(&self) -> bool {
        self.live_owners == 0 && self.cleanup_registrations == 0 && self.owned_children == 0
    }
}

/// Returns what this thread's owner tree is holding. See [`OwnershipStats`].
#[must_use = "this only reads the gauges; in statement position it checks nothing"]
pub fn ownership_stats() -> OwnershipStats {
    OWNERSHIP.try_with(Counters::snapshot).unwrap_or_default()
}

#[derive(Default)]
struct Counters {
    live_owners: Cell<usize>,
    cleanup_registrations: Cell<usize>,
    owned_children: Cell<usize>,
    owners_created: Cell<u64>,
    owners_disposed: Cell<u64>,
    cleanups_registered: Cell<u64>,
    cleanups_run: Cell<u64>,
    /// Live frames, for [`audit_ownership`]. Debug builds only: it exists to prove the gauges above, and a
    /// release build should not pay a `Weak` push per effect to hold a proof nobody reads.
    #[cfg(debug_assertions)]
    registry: RefCell<Vec<Weak<OwnerFrame>>>,
}

impl Counters {
    fn snapshot(&self) -> OwnershipStats {
        OwnershipStats {
            live_owners: self.live_owners.get(),
            cleanup_registrations: self.cleanup_registrations.get(),
            owned_children: self.owned_children.get(),
            owners_created: self.owners_created.get(),
            owners_disposed: self.owners_disposed.get(),
            cleanups_registered: self.cleanups_registered.get(),
            cleanups_run: self.cleanups_run.get(),
        }
    }
}

thread_local! {
    static OWNERSHIP: Counters = Counters::default();
}

/// Runs `f` against this thread's counters, doing nothing if they are already gone.
///
/// Every accounting call here is reachable from a `Drop` — an `OwnerFrame` released during thread
/// teardown is the ordinary case for a host that parks handles in a thread-local. `LocalKey::with`
/// *panics* once its value has been destroyed, and a panic in a destructor is a non-unwinding
/// abort, so this must never use it. Thread-local destructors run in reverse registration order,
/// so `OWNERSHIP` is routinely destroyed before the frames it counts.
///
/// Losing a decrement while the thread is being torn down costs nothing: nobody can observe the
/// counters afterwards. Aborting the process costs everything.
fn with_counters(f: impl FnOnce(&Counters)) {
    let _ = OWNERSHIP.try_with(f);
}

fn add(cell: &Cell<usize>, n: usize) {
    cell.set(cell.get().saturating_add(n));
}

fn sub(cell: &Cell<usize>, n: usize) {
    cell.set(cell.get().saturating_sub(n));
}

fn tick(cell: &Cell<u64>, n: u64) {
    cell.set(cell.get().saturating_add(n));
}

/// A live owner, counted by existing.
///
/// An `OwnerFrame` holds one. The gauge it maintains cannot drift from the population it
/// describes, because creating and destroying a tally is the only way to change it and both are
/// the frame's own lifetime — there is no call site to forget.
pub(crate) struct OwnerTally;

impl OwnerTally {
    pub(crate) fn new() -> Self {
        with_counters(|counters| {
            add(&counters.live_owners, 1);
            tick(&counters.owners_created, 1);
        });
        Self
    }
}

impl Drop for OwnerTally {
    fn drop(&mut self) {
        with_counters(|counters| sub(&counters.live_owners, 1));
    }
}

/// Records a frame in the audit registry. Debug builds only.
///
/// Prunes as it goes. A `Weak` keeps the whole `RcBox<OwnerFrame>` allocation alive, not just its
/// own header, so a registry that only compacted when [`audit_ownership`] happened to be called
/// would retain every frame a debug-built *application* ever created — and applications do not
/// call the audit. Compacting whenever the registry has grown to twice the live count keeps it
/// `O(live owners)` with amortised `O(1)` pushes.
pub(crate) fn register(frame: &Rc<OwnerFrame>) {
    #[cfg(debug_assertions)]
    with_counters(|counters| {
        let mut registry = counters.registry.borrow_mut();
        if registry.len() >= 8 && registry.len() >= counters.live_owners.get().saturating_mul(2) {
            registry.retain(|weak| weak.strong_count() > 0);
        }
        registry.push(Rc::downgrade(frame));
    });
    #[cfg(not(debug_assertions))]
    let _ = frame;
}

pub(crate) fn cleanup_registered() {
    with_counters(|counters| {
        add(&counters.cleanup_registrations, 1);
        tick(&counters.cleanups_registered, 1);
    });
}

/// Records `count` cleanups leaving the pending set to be executed.
pub(crate) fn cleanups_taken(count: usize) {
    with_counters(|counters| {
        sub(&counters.cleanup_registrations, count);
        tick(&counters.cleanups_run, count as u64);
    });
}

/// Records a cleanup that ran without ever being pending — registered against an owner that was
/// already disposed, so it executed immediately.
pub(crate) fn cleanup_run_immediately() {
    with_counters(|counters| {
        tick(&counters.cleanups_registered, 1);
        tick(&counters.cleanups_run, 1);
    });
}

pub(crate) fn child_adopted() {
    with_counters(|counters| add(&counters.owned_children, 1));
}

pub(crate) fn children_taken(count: usize) {
    with_counters(|counters| sub(&counters.owned_children, count));
}

pub(crate) fn owner_disposed() {
    with_counters(|counters| tick(&counters.owners_disposed, 1));
}

/// Number of entries in the audit registry, including any not yet pruned.
///
/// Test-only: the registry is an implementation detail of [`audit_ownership`], but its *size* is
/// the thing that regressed once, so it needs to be assertable.
#[cfg(all(test, debug_assertions))]
pub(crate) fn registry_len() -> usize {
    OWNERSHIP.with(|counters| counters.registry.borrow().len())
}

/// A gauge in [`OwnershipStats`] that the audit can check against the live owner tree.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OwnershipGauge {
    /// [`OwnershipStats::live_owners`].
    LiveOwners,
    /// [`OwnershipStats::cleanup_registrations`].
    CleanupRegistrations,
    /// [`OwnershipStats::owned_children`].
    OwnedChildren,
}

/// A disagreement between a maintained gauge and the live owner tree.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OwnershipDrift {
    /// Which gauge disagreed.
    pub gauge: OwnershipGauge,
    /// What the maintained counter says.
    pub reported: usize,
    /// What walking the live owner frames says.
    pub actual: usize,
}

/// The outcome of [`audit_ownership`].
///
/// Three states rather than an `Option<Vec<_>>`, because the difference between "nothing is wrong"
/// and "this build cannot tell you" must not be expressible as the same value. With an `Option`,
/// the natural `audit_ownership().unwrap_or_default().is_empty()` reads as a pass in a build with
/// `debug_assertions` off — silently, and exactly where a silent pass is least wanted.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnershipAudit {
    /// No registry to walk: this build has `debug_assertions` off, so the gauges cannot be
    /// checked. **Not** an assertion that they are correct.
    Unavailable,
    /// Every gauge agrees with the live owner tree.
    Consistent,
    /// At least one gauge disagrees. Never empty.
    Drifted(Vec<OwnershipDrift>),
}

/// Recomputes every live gauge by walking the owner tree and reports any disagreement.
///
/// This is the proof behind the gauges that are *not* maintained by an object's lifetime — cleanups and
/// adopted children live in `Vec`s, so their counts are maintained by hand and can drift if a
/// path is added without its decrement. Walking is the only honest check, and the registry that
/// makes walking possible exists in debug builds only.
///
/// See [`OwnershipAudit`] for why the three outcomes are a named enum rather than an
/// `Option<Vec<_>>`. Prefer [`debug_assert_ownership_consistent`] in tests, which formats the
/// failure and handles all three.
///
/// The registry this walks is not built in release: the proof is for the test suite, and making
/// every application pay a `Weak` push per owner frame to hold a proof nobody reads would be the
/// tail wagging the dog.
///
/// Prunes dead registry entries as it goes, so calling it repeatedly is cheap and the registry
/// does not grow without bound across a long test.
#[must_use = "the audit reports, it does not assert: `audit_ownership();` checks nothing. Use \
              `debug_assert_ownership_consistent()` for the assertion form"]
pub fn audit_ownership() -> OwnershipAudit {
    #[cfg(not(debug_assertions))]
    return OwnershipAudit::Unavailable;

    #[cfg(debug_assertions)]
    OWNERSHIP
        .try_with(|counters| {
            let mut live = Vec::new();
            counters.registry.borrow_mut().retain(|weak| {
                let Some(frame) = weak.upgrade() else {
                    return false;
                };
                live.push(frame);
                true
            });

            let mut drift = Vec::new();
            let mut check = |gauge: OwnershipGauge, reported: usize, actual: usize| {
                if reported != actual {
                    drift.push(OwnershipDrift {
                        gauge,
                        reported,
                        actual,
                    });
                }
            };
            check(
                OwnershipGauge::LiveOwners,
                counters.live_owners.get(),
                live.len(),
            );
            check(
                OwnershipGauge::CleanupRegistrations,
                counters.cleanup_registrations.get(),
                live.iter().map(|frame| frame.pending_cleanups()).sum(),
            );
            check(
                OwnershipGauge::OwnedChildren,
                counters.owned_children.get(),
                live.iter().map(|frame| frame.owned_children()).sum(),
            );
            if drift.is_empty() {
                OwnershipAudit::Consistent
            } else {
                OwnershipAudit::Drifted(drift)
            }
        })
        .unwrap_or(OwnershipAudit::Unavailable)
}

/// Panics if any ownership gauge has drifted from the live owner tree.
///
/// Call this after anything that creates or tears down owners. It is the mechanism that keeps the
/// counters honest: a path that forgets its bookkeeping fails here rather than shipping a gauge
/// that quietly lies.
///
/// Named after [`debug_assert!`], and compiled out under the same condition: in a build with
/// `debug_assertions` off there is no registry to walk, so this does nothing. The symbol still
/// exists in every build, so a test suite that calls it compiles under `--release` — it simply
/// stops checking, exactly as a `debug_assert!` would.
#[track_caller]
pub fn debug_assert_ownership_consistent() {
    if let OwnershipAudit::Drifted(drift) = audit_ownership() {
        panic!("ownership counters drifted from the live owner tree: {drift:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Reactor, on_cleanup, scope};

    // The audit's positive path only exists where the registry does. In a build with
    // `debug_assertions` off, `audit_ownership` correctly answers `Unavailable`, so asserting
    // `Consistent` here would be asserting that the build is a debug build.
    #[cfg(debug_assertions)]
    #[test]
    fn the_audit_detects_a_gauge_that_has_drifted() {
        // Every other ownership test asserts the audit stays quiet, which cannot distinguish "the
        // counters are right" from "the audit never fires". Induce a drift by hand and prove it
        // is caught, named, and quantified.
        std::thread::spawn(|| {
            let (_handle, ()) = scope(|| {});
            assert_eq!(audit_ownership(), OwnershipAudit::Consistent);

            let before = ownership_stats().owned_children;
            child_adopted(); // a child counted but never actually adopted

            match audit_ownership() {
                OwnershipAudit::Drifted(drift) => assert_eq!(
                    drift,
                    vec![OwnershipDrift {
                        gauge: OwnershipGauge::OwnedChildren,
                        reported: before + 1,
                        actual: before,
                    }]
                ),
                other => panic!("the audit missed a drift it was built to catch: {other:?}"),
            }

            children_taken(1); // put it back
            assert_eq!(audit_ownership(), OwnershipAudit::Consistent);
        })
        .join()
        .expect("test thread panicked");
    }

    #[cfg(debug_assertions)]
    #[test]
    fn the_audit_registry_does_not_grow_with_owners_long_dead() {
        // A `Weak` keeps the whole `RcBox<OwnerFrame>` alive, so a registry that only compacted
        // when the audit happened to run retained every frame a debug-built application ever
        // created — 30 MB over 200k scopes, invisible to `live_owners`, which correctly read 0.
        std::thread::spawn(|| {
            for _ in 0..2_000 {
                let (handle, ()) = scope(|| on_cleanup(|| {}));
                handle.dispose();
            }
            let live = ownership_stats().live_owners;
            assert!(
                registry_len() < 64,
                "registry retained {} entries for {live} live owners",
                registry_len()
            );
        })
        .join()
        .expect("test thread panicked");
    }

    #[test]
    fn the_audit_reports_unavailable_rather_than_consistent_without_a_registry() {
        // `Unavailable` and `Consistent` must never be confused: the whole reason the result is a
        // named enum rather than `Option<Vec<_>>` is that "cannot say" must not read as "nothing
        // wrong". This asserts whichever answer this build owes, so it is meaningful in both.
        std::thread::spawn(|| {
            let (_handle, ()) = scope(|| {});
            let audit = audit_ownership();
            if cfg!(debug_assertions) {
                assert_eq!(audit, OwnershipAudit::Consistent);
            } else {
                assert_eq!(
                    audit,
                    OwnershipAudit::Unavailable,
                    "without a registry the honest answer is `cannot say`"
                );
            }
            // Either way the assertion helper must not panic on a healthy graph.
            debug_assert_ownership_consistent();
        })
        .join()
        .expect("test thread panicked");
    }

    #[test]
    fn accounting_survives_a_thread_local_destroyed_before_the_frames_it_counts() {
        // Thread-local destructors run in reverse registration order, so a host that parks a
        // handle in its own thread-local — a component registry, a task queue — has its holder
        // destroyed *after* the counters. Reaching for the counters there panics, and a panic in
        // a destructor is a non-unwinding abort: this used to take the whole process down.
        //
        // If it regresses, this test does not fail — the process dies and the suite goes with it.
        // Reaching the assertion at all is the point.
        thread_local! {
            static HOLDER: std::cell::RefCell<Option<crate::ScopeHandle>> =
                const { std::cell::RefCell::new(None) };
        }

        std::thread::spawn(|| {
            HOLDER.with(|holder| drop(holder.borrow())); // registers this holder's dtor first
            let reactor = Reactor::new();
            let _guard = reactor.enter();
            let (handle, ()) = scope(|| on_cleanup(|| {}));
            HOLDER.with(|holder| *holder.borrow_mut() = Some(handle));
        })
        .join()
        .expect("the thread must unwind cleanly rather than abort");
    }
}
