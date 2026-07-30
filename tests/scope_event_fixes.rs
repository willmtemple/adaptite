//! Retention regressions on the ordinary lifecycle paths: an owner that is never reset, and a
//! subscription that is cancelled but still held.
//!
//! Every test here asserts a *count returning to (or staying near) its baseline*, because that is
//! the only shape that distinguishes "the leak is fixed" from "nothing panicked".

use std::cell::Cell;
use std::rc::Rc;

use adaptite::{
    Reactor, debug_assert_ownership_consistent, event_in, on_cleanup, on_in, ownership_stats, scope,
};

/// Ownership counters are thread-local, so every test runs on its own thread and measures deltas
/// from a baseline rather than absolute values.
fn on_own_thread(body: impl FnOnce() + Send + 'static) {
    std::thread::spawn(body)
        .join()
        .expect("test thread panicked");
}

/// A stand-in for whatever a closure captured: it is only interesting because its `Drop` says
/// when it was released.
struct Payload {
    live: Rc<Cell<usize>>,
}

impl Payload {
    fn new(live: &Rc<Cell<usize>>) -> Self {
        live.set(live.get() + 1);
        Self {
            live: Rc::clone(live),
        }
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        self.live.set(self.live.get() - 1);
    }
}

/// The application-root shape: one owner that is never disposed and never re-runs, re-entered to
/// build and tear down a component on every frame. Before the fix, every effect ever created
/// under it stayed in its `children` list — with its whole captured environment — for the life of
/// the process.
#[test]
fn a_long_lived_owner_releases_effects_that_were_individually_disposed() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let before = ownership_stats();
        let (root, ()) = scope(|| {});
        let root_owner = root.owner();
        let live = Rc::new(Cell::new(0usize));

        for _ in 0..1_000 {
            let payload = Payload::new(&live);
            let effect = root_owner.run_in(|| {
                reactor.effect(move || {
                    let _ = &payload;
                })
            });
            effect.dispose();
            assert!(effect.is_disposed());
            drop(effect);
        }

        let during = ownership_stats();
        assert!(
            live.get() <= 32,
            "the owner is still holding {} disposed effects' captured state",
            live.get()
        );
        assert!(
            during.owned_children - before.owned_children <= 32,
            "owned_children climbed to {} across 1000 create+dispose cycles",
            during.owned_children - before.owned_children
        );
        debug_assert_ownership_consistent();

        root.dispose();
        drop(root);
        drop(root_owner); // the re-entry token also holds the frame
        let after = ownership_stats();
        assert_eq!(live.get(), 0, "disposing the root must release everything");
        assert_eq!(after.owned_children, before.owned_children);
        assert_eq!(after.live_owners, before.live_owners);
        debug_assert_ownership_consistent();
    });
}

/// Same shape with scopes rather than effects. A disposed scope child is cheaper — its own
/// cleanups and children were taken by its reset — but the owner still accumulates one stripped
/// frame per component generation, and `live_owners` never comes back down.
#[test]
fn a_long_lived_owner_releases_scopes_that_were_individually_disposed() {
    on_own_thread(|| {
        let before = ownership_stats();
        let (root, ()) = scope(|| {});
        let root_owner = root.owner();
        let live = Rc::new(Cell::new(0usize));

        for _ in 0..1_000 {
            let payload = Payload::new(&live);
            let (child, ()) = root_owner.run_in(|| {
                scope(move || {
                    on_cleanup(move || drop(payload));
                })
            });
            child.dispose();
            drop(child);
        }

        let during = ownership_stats();
        assert!(
            during.owned_children - before.owned_children <= 32,
            "owned_children climbed to {} across 1000 create+dispose cycles",
            during.owned_children - before.owned_children
        );
        assert!(
            during.live_owners - before.live_owners <= 33,
            "live_owners climbed to {} across 1000 create+dispose cycles",
            during.live_owners - before.live_owners
        );
        debug_assert_ownership_consistent();

        root.dispose();
        drop(root);
        drop(root_owner); // the re-entry token also holds the frame
        let after = ownership_stats();
        assert_eq!(after.owned_children, before.owned_children);
        assert_eq!(after.live_owners, before.live_owners);
        debug_assert_ownership_consistent();
    });
}

/// Releasing a disposed child must not disturb the reverse-registration teardown order of the
/// live ones, and must not release anything still live.
#[test]
fn releasing_disposed_children_preserves_teardown_order_of_the_survivors() {
    on_own_thread(|| {
        let log = Rc::new(std::cell::RefCell::new(Vec::new()));
        let (root, ()) = scope(|| {});
        let root_owner = root.owner();

        // Interleave survivors and casualties, and push past the sweep threshold.
        let mut casualties = Vec::new();
        for i in 0..12 {
            let (live_child, ()) = root_owner.run_in(|| {
                let log = Rc::clone(&log);
                scope(move || on_cleanup(move || log.borrow_mut().push(format!("live {i}"))))
            });
            // Owned by root, so dropping every handle must not disturb it — and a sweep must
            // never mistake "no handle" for "disposed".
            drop(live_child);
            let (dead_child, ()) = root_owner.run_in(|| {
                let log = Rc::clone(&log);
                scope(move || on_cleanup(move || log.borrow_mut().push(format!("dead {i}"))))
            });
            dead_child.dispose();
            casualties.push(dead_child);
        }

        // The disposed children ran their cleanups at disposal, in creation order.
        let after_disposals = log.borrow().clone();
        assert_eq!(
            after_disposals,
            (0..12).map(|i| format!("dead {i}")).collect::<Vec<_>>()
        );

        log.borrow_mut().clear();
        root.dispose();
        let expected = (0..12)
            .rev()
            .map(|i| format!("live {i}"))
            .collect::<Vec<_>>();
        assert_eq!(
            *log.borrow(),
            expected,
            "survivors must still tear down in reverse registration order"
        );
        drop(casualties);
    });
}

/// `unsubscribe` is documented as cancelling the subscription. A handle the consumer believes
/// inert must not go on pinning the event's graph node through the cancel closure.
#[test]
fn a_cancelled_subscription_handle_stops_pinning_its_event() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let before = reactor.graph_stats().live_nodes;

        let mut cancelled = Vec::new();
        for _ in 0..500 {
            let stream = event_in::<u32>(&reactor);
            let subscription = stream.subscribe(|_| {});
            subscription.unsubscribe();
            assert!(!subscription.is_active());
            drop(stream); // the consumer is done with the event entirely
            cancelled.push(subscription);
        }

        assert_eq!(
            reactor.graph_stats().live_nodes,
            before,
            "cancelled subscription handles are still pinning their events' nodes"
        );
        drop(cancelled);
    });
}

/// The `on` case is the expensive one: the cancel closure holds the draining `EffectHandle` and
/// the handler, so a cancelled handle retains the whole handler environment as well as the event.
#[test]
fn a_cancelled_on_subscription_releases_its_handler_and_its_event() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let live = Rc::new(Cell::new(0usize));
        let before = reactor.graph_stats().live_nodes;

        let mut cancelled = Vec::new();
        for _ in 0..200 {
            let stream = event_in::<u32>(&reactor);
            let payload = Payload::new(&live);
            let subscription = on_in(&reactor, &stream, move |_| {
                let _ = &payload;
            });
            subscription.unsubscribe();
            assert!(!subscription.is_active());
            drop(stream);
            cancelled.push(subscription);
        }

        assert_eq!(
            live.get(),
            0,
            "cancelled `on` subscriptions are still holding their handlers"
        );
        assert_eq!(
            reactor.graph_stats().live_nodes,
            before,
            "cancelled `on` subscriptions are still pinning their events' nodes"
        );
        drop(cancelled);
    });
}

/// Cancelling must stay idempotent and must not resurrect anything: the second `unsubscribe`, the
/// handle's `Drop`, and an owner's teardown all reach the same already-cancelled subscription.
#[test]
fn cancelling_twice_is_still_a_no_op() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let seen = Rc::new(Cell::new(0usize));
        let stream = event_in::<u32>(&reactor);

        let subscription = stream.subscribe({
            let seen = Rc::clone(&seen);
            move |_| seen.set(seen.get() + 1)
        });
        stream.emit(1);
        assert_eq!(seen.get(), 1);

        subscription.unsubscribe();
        subscription.unsubscribe();
        stream.emit(2);
        assert_eq!(seen.get(), 1, "a cancelled subscriber must not fire");
        drop(subscription);
        stream.emit(3);
        assert_eq!(seen.get(), 1);
    });
}

/// `leak` forfeits the *handle's* lifetime management and nothing else: a subscription created
/// inside an owner is still cancelled with that owner. This pins the documented carve-out on
/// `Subscription::leak`, which previously promised the remainder of the program without
/// qualification.
#[test]
fn leaking_a_subscription_does_not_detach_it_from_its_owner() {
    on_own_thread(|| {
        let reactor = Reactor::new();
        let seen = Rc::new(Cell::new(0usize));
        let stream = event_in::<u32>(&reactor);

        let (owner, ()) = scope({
            let stream = stream.clone();
            let seen = Rc::clone(&seen);
            move || {
                stream.subscribe(move |_| seen.set(seen.get() + 1)).leak();
            }
        });

        stream.emit(1);
        assert_eq!(seen.get(), 1);

        owner.dispose();
        stream.emit(2);
        assert_eq!(
            seen.get(),
            1,
            "the owner cancels a leaked subscription: `leak` is not a detach"
        );
    });
}

/// The same carve-out for `ScopeHandle::leak`.
#[test]
fn leaking_a_scope_handle_does_not_detach_it_from_its_owner() {
    on_own_thread(|| {
        let torn_down = Rc::new(Cell::new(false));

        let (outer, ()) = scope({
            let torn_down = Rc::clone(&torn_down);
            move || {
                let (inner, ()) = scope(move || {
                    on_cleanup(move || torn_down.set(true));
                });
                inner.leak();
            }
        });

        assert!(!torn_down.get());
        outer.dispose();
        assert!(
            torn_down.get(),
            "the enclosing owner disposes a leaked nested scope: `leak` is not a detach"
        );
    });
}
