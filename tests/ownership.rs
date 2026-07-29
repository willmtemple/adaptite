//! Ownership accounting, and the mechanism that keeps it accurate.
//!
//! A gauge nobody checks drifts, and a drifted leak gauge is worse than no gauge — it makes a real
//! leak look fine. So these tests do not merely assert expected numbers: after every operation
//! they call `debug_assert_ownership_consistent`, which recomputes every live gauge by walking the
//! owner tree and fails on any disagreement.
//!
//! The last test does that against a deterministically-shuffled workload, so the counters are
//! checked across orderings nobody wrote by hand.

use std::cell::{Cell, RefCell};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use adaptite::{
    Reactor, debug_assert_ownership_consistent, on_cleanup, ownership_stats, scope, signal_in,
    unowned,
};

/// Ownership counters are thread-local, so every test runs on its own thread and measures deltas
/// from a baseline rather than absolute values.
fn on_own_thread(body: impl FnOnce() + Send + 'static) {
    std::thread::spawn(body)
        .join()
        .expect("test thread panicked");
}

#[test]
fn a_scope_accounts_for_itself_and_gives_everything_back() {
    on_own_thread(|| {
        let before = ownership_stats();
        debug_assert_ownership_consistent();

        let ran = Rc::new(Cell::new(0));
        let (handle, ()) = scope({
            let ran = Rc::clone(&ran);
            move || {
                on_cleanup({
                    let ran = Rc::clone(&ran);
                    move || ran.set(ran.get() + 1)
                });
                on_cleanup(move || {});
            }
        });

        let during = ownership_stats();
        assert_eq!(during.live_owners - before.live_owners, 1);
        assert_eq!(
            during.cleanup_registrations - before.cleanup_registrations,
            2
        );
        assert_eq!(during.owners_created - before.owners_created, 1);
        debug_assert_ownership_consistent();

        handle.dispose();
        assert_eq!(ran.get(), 1);

        let after = ownership_stats();
        assert_eq!(
            after.cleanup_registrations, before.cleanup_registrations,
            "disposal returns every registration"
        );
        assert_eq!(after.cleanups_run - before.cleanups_run, 2);
        assert_eq!(after.owners_disposed - before.owners_disposed, 1);
        debug_assert_ownership_consistent();

        drop(handle);
        let dropped = ownership_stats();
        assert_eq!(
            dropped.live_owners, before.live_owners,
            "the frame itself is gone once nothing holds it"
        );
        debug_assert_ownership_consistent();
    });
}

#[test]
fn an_effect_re_registers_its_cleanups_without_accumulating_them() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let before = ownership_stats();

        let value = signal_in(&reactor, 0_u32);
        let effect = reactor.effect({
            let value = value.clone();
            move || {
                let _ = value.get();
                on_cleanup(|| {});
                on_cleanup(|| {});
            }
        });
        reactor.flush_now();

        let settled = ownership_stats();
        assert_eq!(
            settled.cleanup_registrations - before.cleanup_registrations,
            2
        );
        debug_assert_ownership_consistent();

        // The steady-state property: a hundred re-runs must not leave a hundred generations of
        // cleanups behind. This is the leak the graph counters cannot see, because the node count
        // never moves.
        for next in 1..=100 {
            value.set(next);
            reactor.flush_now();
            debug_assert_ownership_consistent();
        }

        let after = ownership_stats();
        assert_eq!(
            after.cleanup_registrations, settled.cleanup_registrations,
            "cleanups must not accumulate across re-runs"
        );
        assert_eq!(after.cleanups_run - settled.cleanups_run, 200);
        assert_eq!(after.live_owners, settled.live_owners);

        effect.dispose();
        debug_assert_ownership_consistent();
        assert_eq!(
            ownership_stats().cleanup_registrations,
            before.cleanup_registrations
        );
    });
}

#[test]
fn nested_owners_are_counted_as_children_and_released_together() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let before = ownership_stats();

        let (outer, ()) = scope(|| {
            // A scope created inside another is adopted by it, and an effect created inside a
            // scope is too.
            let (_inner, ()) = scope(|| on_cleanup(|| {}));
            reactor.effect(|| {}).leak();
        });
        reactor.flush_now();

        let nested = ownership_stats();
        assert_eq!(nested.live_owners - before.live_owners, 3);
        assert_eq!(
            nested.owned_children - before.owned_children,
            2,
            "the outer scope owns the inner scope and the effect"
        );
        debug_assert_ownership_consistent();

        outer.dispose();
        let torn_down = ownership_stats();
        assert_eq!(
            torn_down.owned_children, before.owned_children,
            "disposing the outer owner releases what it held"
        );
        assert_eq!(
            torn_down.cleanup_registrations,
            before.cleanup_registrations
        );
        debug_assert_ownership_consistent();
    });
}

#[test]
fn a_panicking_cleanup_does_not_strand_the_gauge() {
    on_own_thread(|| {
        let before = ownership_stats();
        let ran = Rc::new(Cell::new(0));

        let (handle, ()) = scope({
            let ran = Rc::clone(&ran);
            move || {
                // Registered first, so it runs *last*: teardown is most-recent-first.
                on_cleanup({
                    let ran = Rc::clone(&ran);
                    move || ran.set(ran.get() + 1)
                });
                on_cleanup(|| panic!("cleanup failed"));
            }
        });
        debug_assert_ownership_consistent();

        let result = catch_unwind(AssertUnwindSafe(|| handle.dispose()));
        assert!(result.is_err(), "the panic propagates to the disposer");

        assert_eq!(
            ran.get(),
            1,
            "the sibling cleanup still runs: teardown is total"
        );

        // The gauges hold regardless of how teardown goes: the pending set is accounted at the
        // moment it is taken, not as each entry completes, so a panic partway through cannot
        // leave a gauge overstated.
        let after = ownership_stats();
        assert_eq!(after.cleanup_registrations, before.cleanup_registrations);
        assert_eq!(after.cleanups_run - before.cleanups_run, 2);
        debug_assert_ownership_consistent();
    });
}

#[test]
fn a_cleanup_registered_against_a_dead_owner_runs_without_ever_pending() {
    on_own_thread(|| {
        let before = ownership_stats();
        let ran = Rc::new(Cell::new(false));

        let (handle, owner) = scope(adaptite::owner);
        let owner = owner.expect("a scope is an owner");
        handle.dispose();

        // Registering against an already-disposed owner runs the cleanup immediately, so it is
        // registered and run without ever being pending.
        owner.run_in({
            let ran = Rc::clone(&ran);
            move || {
                on_cleanup(move || ran.set(true));
            }
        });
        assert!(ran.get());

        let after = ownership_stats();
        assert_eq!(
            after.cleanup_registrations, before.cleanup_registrations,
            "it was never pending"
        );
        assert_eq!(after.cleanups_registered - before.cleanups_registered, 1);
        assert_eq!(after.cleanups_run - before.cleanups_run, 1);
        debug_assert_ownership_consistent();
    });
}

#[test]
fn unowned_work_is_not_adopted() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let before = ownership_stats();

        let (handle, escaped) = scope(|| unowned(|| reactor.effect(|| {})));
        reactor.flush_now();

        let stats = ownership_stats();
        assert_eq!(
            stats.owned_children, before.owned_children,
            "an unowned effect is nobody's child"
        );
        debug_assert_ownership_consistent();

        handle.dispose();
        assert!(
            !escaped.is_disposed(),
            "and it survives the scope it escaped from"
        );
        debug_assert_ownership_consistent();

        escaped.dispose();
        debug_assert_ownership_consistent();
    });
}

/// A deterministic shuffle, so the workload below covers orderings nobody wrote by hand while
/// staying reproducible. Nothing here needs to be a good PRNG — it needs to be varied and fixed.
struct Shuffle(u64);

impl Shuffle {
    fn next(&mut self, modulo: usize) -> usize {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33) as usize % modulo
    }
}

#[test]
fn the_gauges_survive_a_randomised_workload() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let before = ownership_stats();
        let mut shuffle = Shuffle(0x9E37_79B9_7F4A_7C15);

        let signals: Vec<_> = (0..4).map(|i| signal_in(&reactor, i as u32)).collect();
        let scopes: Rc<RefCell<Vec<adaptite::ScopeHandle>>> = Rc::new(RefCell::new(Vec::new()));
        let effects = Rc::new(RefCell::new(Vec::new()));

        // Every operation is followed by an audit, so a drift is attributed to the step that
        // caused it rather than discovered at the end.
        for step in 0..400 {
            match shuffle.next(6) {
                // Create a scope that registers a cleanup and may adopt an effect.
                0 => {
                    let adopt = shuffle.next(2) == 0;
                    let reactor = reactor.clone();
                    let adopted = Rc::new(RefCell::new(None));
                    let (handle, ()) = scope({
                        let adopted = Rc::clone(&adopted);
                        move || {
                            on_cleanup(|| {});
                            if adopt {
                                // Deliberately *not* `leak()`: leaking forgets the handle's `Rc`,
                                // so the effect's owner frame is retained for the life of the
                                // process and `live_owners` correctly never comes back down. That
                                // is real behaviour worth knowing, but it would make the
                                // gave-everything-back assertion below meaningless.
                                *adopted.borrow_mut() = Some(reactor.effect(|| {}));
                            }
                        }
                    });
                    if let Some(handle) = adopted.borrow_mut().take() {
                        effects.borrow_mut().push(handle);
                    }
                    scopes.borrow_mut().push(handle);
                }
                // Create an effect that registers cleanups and reads a signal.
                1 => {
                    let input = signals[shuffle.next(signals.len())].clone();
                    let count = shuffle.next(3);
                    let handle = reactor.effect(move || {
                        let _ = input.get();
                        for _ in 0..count {
                            on_cleanup(|| {});
                        }
                    });
                    effects.borrow_mut().push(handle);
                }
                // Invalidate, forcing re-runs and therefore cleanup churn.
                2 => {
                    let index = shuffle.next(signals.len());
                    signals[index].set(step as u32);
                    reactor.flush_now();
                }
                // Dispose a scope.
                3 => {
                    let mut scopes = scopes.borrow_mut();
                    if !scopes.is_empty() {
                        let index = shuffle.next(scopes.len());
                        scopes.remove(index).dispose();
                    }
                }
                // Dispose an effect.
                4 => {
                    let mut effects = effects.borrow_mut();
                    if !effects.is_empty() {
                        let index = shuffle.next(effects.len());
                        let handle: adaptite::EffectHandle = effects.remove(index);
                        handle.dispose();
                    }
                }
                // Drop a handle without disposing, so teardown happens via `Drop` instead.
                _ => {
                    let mut effects = effects.borrow_mut();
                    if !effects.is_empty() {
                        let index = shuffle.next(effects.len());
                        drop(effects.remove(index));
                    }
                }
            }
            reactor.flush_now();
            debug_assert_ownership_consistent();
        }

        // Give everything back and confirm the thread is where it started.
        for handle in scopes.borrow_mut().drain(..) {
            handle.dispose();
        }
        effects.borrow_mut().clear();
        reactor.flush_now();
        debug_assert_ownership_consistent();

        let after = ownership_stats();
        assert_eq!(
            (
                after.live_owners,
                after.cleanup_registrations,
                after.owned_children
            ),
            (
                before.live_owners,
                before.cleanup_registrations,
                before.owned_children
            ),
            "a workload that gave everything back must leave no retention"
        );
        assert!(
            after.owners_created > before.owners_created,
            "and the workload must actually have done something"
        );
        assert_eq!(
            after.cleanups_registered - after.cleanups_run,
            (after.cleanup_registrations - before.cleanup_registrations) as u64,
            "every registration is accounted for as either pending or run"
        );
    });
}
