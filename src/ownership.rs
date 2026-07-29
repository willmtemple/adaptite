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
    pub fn is_empty(&self) -> bool {
        self.live_owners == 0 && self.cleanup_registrations == 0 && self.owned_children == 0
    }
}

/// Returns what this thread's owner tree is holding. See [`OwnershipStats`].
pub fn ownership_stats() -> OwnershipStats {
    OWNERSHIP.with(Counters::snapshot)
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
        OWNERSHIP.with(|counters| {
            add(&counters.live_owners, 1);
            tick(&counters.owners_created, 1);
        });
        Self
    }
}

impl Drop for OwnerTally {
    fn drop(&mut self) {
        OWNERSHIP.with(|counters| sub(&counters.live_owners, 1));
    }
}

/// Records a frame in the audit registry. Debug builds only.
pub(crate) fn register(frame: &Rc<OwnerFrame>) {
    #[cfg(debug_assertions)]
    OWNERSHIP.with(|counters| counters.registry.borrow_mut().push(Rc::downgrade(frame)));
    #[cfg(not(debug_assertions))]
    let _ = frame;
}

pub(crate) fn cleanup_registered() {
    OWNERSHIP.with(|counters| {
        add(&counters.cleanup_registrations, 1);
        tick(&counters.cleanups_registered, 1);
    });
}

/// Records `count` cleanups leaving the pending set to be executed.
pub(crate) fn cleanups_taken(count: usize) {
    OWNERSHIP.with(|counters| {
        sub(&counters.cleanup_registrations, count);
        tick(&counters.cleanups_run, count as u64);
    });
}

/// Records a cleanup that ran without ever being pending — registered against an owner that was
/// already disposed, so it executed immediately.
pub(crate) fn cleanup_run_immediately() {
    OWNERSHIP.with(|counters| {
        tick(&counters.cleanups_registered, 1);
        tick(&counters.cleanups_run, 1);
    });
}

pub(crate) fn child_adopted() {
    OWNERSHIP.with(|counters| add(&counters.owned_children, 1));
}

pub(crate) fn children_taken(count: usize) {
    OWNERSHIP.with(|counters| sub(&counters.owned_children, count));
}

pub(crate) fn owner_disposed() {
    OWNERSHIP.with(|counters| tick(&counters.owners_disposed, 1));
}

/// A disagreement between a maintained gauge and the live owner tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnershipDrift {
    /// Which gauge disagreed.
    pub gauge: &'static str,
    /// What the maintained counter says.
    pub reported: usize,
    /// What walking the live owner frames says.
    pub actual: usize,
}

/// Recomputes every live gauge by walking the owner tree and reports any disagreement.
///
/// This is the proof behind the gauges that are *not* maintained by an object's lifetime — cleanups and
/// adopted children live in `Vec`s, so their counts are maintained by hand and can drift if a
/// path is added without its decrement. Walking is the only honest check, and the registry that
/// makes walking possible exists in debug builds only.
///
/// Returns `Some` with an empty vector when everything agrees, and `None` in a build with
/// `debug_assertions` off — there is no registry to walk there, so the honest answer is "cannot
/// say" rather than "nothing wrong". Prefer [`debug_assert_ownership_consistent`] in tests, which
/// formats the failure and handles both.
///
/// The registry this walks is not built in release: the proof is for the test suite, and making
/// every application pay a `Weak` push per owner frame to hold a proof nobody reads would be the
/// tail wagging the dog.
///
/// Prunes dead registry entries as it goes, so calling it repeatedly is cheap and the registry
/// does not grow without bound across a long test.
pub fn audit_ownership() -> Option<Vec<OwnershipDrift>> {
    #[cfg(not(debug_assertions))]
    return None;

    #[cfg(debug_assertions)]
    OWNERSHIP.with(|counters| {
        let mut live = Vec::new();
        counters.registry.borrow_mut().retain(|weak| {
            let Some(frame) = weak.upgrade() else {
                return false;
            };
            live.push(frame);
            true
        });

        let mut drift = Vec::new();
        let mut check = |gauge, reported: usize, actual: usize| {
            if reported != actual {
                drift.push(OwnershipDrift {
                    gauge,
                    reported,
                    actual,
                });
            }
        };
        check("live_owners", counters.live_owners.get(), live.len());
        check(
            "cleanup_registrations",
            counters.cleanup_registrations.get(),
            live.iter().map(|frame| frame.pending_cleanups()).sum(),
        );
        check(
            "owned_children",
            counters.owned_children.get(),
            live.iter().map(|frame| frame.owned_children()).sum(),
        );
        Some(drift)
    })
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
    if let Some(drift) = audit_ownership() {
        assert!(
            drift.is_empty(),
            "ownership counters drifted from the live owner tree: {drift:?}"
        );
    }
}
