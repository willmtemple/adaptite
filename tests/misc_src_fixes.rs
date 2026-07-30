//! Regression tests from the 0.3 release-readiness review, for `signal`, `watch` and `ownership`.

use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;

use adaptite::Reactor;

/// `Signal::set` and `Signal::replace` grew a `# Panics` section in 0.3 naming the borrow
/// collisions they can raise. This pins every case that section claims, so the documentation
/// cannot quietly become false in either direction — the panic disappearing would falsify it just
/// as surely as a new one appearing.
#[test]
fn the_signal_writes_documented_as_panicking_do_panic() {
    let reactor = Reactor::new();
    let value = reactor.signal(1u32);

    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    /// A named write that the `# Panics` section says cannot succeed here.
    type Case = (&'static str, Box<dyn Fn()>);

    let cases: Vec<Case> = vec![
        ("set inside with", {
            let value = value.clone();
            Box::new(move || value.with(|_| _ = value.set(2)))
        }),
        ("replace inside with", {
            let value = value.clone();
            Box::new(move || value.with(|_| _ = value.replace(3)))
        }),
        ("set inside with_peek", {
            let value = value.clone();
            Box::new(move || value.with_peek(|_| _ = value.set(4)))
        }),
        ("replace inside with_peek", {
            let value = value.clone();
            Box::new(move || value.with_peek(|_| _ = value.replace(5)))
        }),
        ("set inside update", {
            let value = value.clone();
            Box::new(move || value.update(|_| _ = value.set(6)))
        }),
        ("replace inside update", {
            let value = value.clone();
            Box::new(move || value.update(|_| _ = value.replace(7)))
        }),
    ];

    let mut failures = Vec::new();
    for (name, case) in cases {
        if catch_unwind(AssertUnwindSafe(case)).is_ok() {
            failures.push(name);
        }
    }

    std::panic::set_hook(hook);
    assert!(
        failures.is_empty(),
        "documented under `# Panics`, but returned normally: {failures:?}"
    );
}

/// The other half of that `# Panics` section: a `PartialEq` implementation that writes the very
/// signal it was handed. `set` compares under a shared borrow, so the comparator's own write
/// collides with it — the one case a reader cannot deduce from "do not write from inside `with`",
/// because the borrow belongs to `set` itself rather than to an enclosing closure.
#[test]
fn a_comparator_that_writes_the_signal_it_compares_panics() {
    thread_local! {
        static SELF_WRITER: RefCell<Option<adaptite::Signal<Reentrant>>> =
            const { RefCell::new(None) };
    }

    #[derive(Clone)]
    struct Reentrant(u32);

    impl PartialEq for Reentrant {
        fn eq(&self, other: &Self) -> bool {
            SELF_WRITER.with(|slot| {
                if let Some(signal) = slot.borrow().as_ref() {
                    let _ = signal.replace(Reentrant(99));
                }
            });
            self.0 == other.0
        }
    }

    let reactor = Reactor::new();
    let value = reactor.signal(Reentrant(0));
    SELF_WRITER.with(|slot| *slot.borrow_mut() = Some(value.clone()));

    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = catch_unwind(AssertUnwindSafe(|| _ = value.set(Reentrant(1))));
    std::panic::set_hook(hook);

    SELF_WRITER.with(|slot| *slot.borrow_mut() = None);
    outcome.expect_err("a comparator that writes the signal under comparison must panic");
}

/// A `watch` handler that writes the watched source and forces a synchronous flush. Both halves
/// are documented as legal, and together they used to re-enter the watch effect from inside its
/// own body.
///
/// Read this as a test of the *deferral*, not of the borrow discipline in `Reactor::watch`: the
/// borrow of `previous` no longer spans the handler, but that shape is unobservable from here
/// while `run_scheduled_inner` defers the re-entrant run — holding the borrow across the handler
/// keeps this test green in both profiles (measured). What this does pin is that the combination
/// converges and delivers each transition exactly once. Remove the deferral and it fails on the
/// reactor's re-entrancy assert instead.
#[test]
fn a_watch_handler_may_write_the_watched_source_and_flush() {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let reactor = Reactor::new();
    let value = reactor.signal(0i32);

    let handle = reactor.watch(
        {
            let value = value.clone();
            move || value.get()
        },
        {
            let seen = Rc::clone(&seen);
            let value = value.clone();
            let reactor = reactor.clone();
            move |new: &i32, old: Option<&i32>| {
                seen.borrow_mut().push((old.copied(), *new));
                if *new < 2 {
                    value.set(*new + 1);
                    reactor.flush_now();
                }
            }
        },
    );

    reactor.flush_now();
    handle.leak();

    assert_eq!(&*seen.borrow(), &[(None, 0), (Some(0), 1), (Some(1), 2)]);
}
