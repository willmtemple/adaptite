use alloc::rc::Rc;
use core::cell::RefCell;

use crate::{EffectHandle, Reactor, current};

/// Runs `handler` whenever the value produced by `source` changes, in the current thread's
/// default reactor.
///
/// This is the "explicitly scoped" counterpart to [`crate::effect`]: only reads made by
/// `source` are tracked, and `handler` runs **untracked**, so it can freely read other reactive
/// state (or write signals) without subscribing to it. The source value is equality-gated —
/// writes that leave it unchanged do not invoke the handler.
///
/// The handler receives the new value and the previous one (`None` on the first invocation).
/// Like [`crate::effect`], the first invocation does not run inline with `watch`: it is
/// flushed on the runtime's microtask queue, as is every subsequent invocation.
///
/// # Examples
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{signal, watch};
/// use runite::{queue_macrotask, run};
///
/// let transitions = Rc::new(RefCell::new(Vec::new()));
///
/// queue_macrotask({
///     let transitions = Rc::clone(&transitions);
///     move || {
///         let page = signal("home");
///         let visits = signal(0usize);
///
///         watch(
///             {
///                 let page = page.clone();
///                 move || page.get()
///             },
///             {
///                 let transitions = transitions.clone();
///                 let visits = visits.clone();
///                 move |new: &&str, old: Option<&&str>| {
///                     // Untracked: reading (and writing) `visits` does not subscribe to it.
///                     visits.update(|count| *count += 1);
///                     transitions.borrow_mut().push((old.copied(), *new));
///                 }
///             },
///         )
///         .leak();
///
///         runite::queue_macrotask({
///             let page = page.clone();
///             move || {
///                 page.set("settings");
///                 runite::queue_macrotask(move || {
///                     page.set("settings"); // unchanged: handler must not run
///                 });
///             }
///         });
///     }
/// });
/// run();
///
/// assert_eq!(
///     *transitions.borrow(),
///     [(None, "home"), (Some("home"), "settings")]
/// );
/// ```
#[track_caller]
pub fn watch<S>(
    source: impl Fn() -> S + 'static,
    handler: impl Fn(&S, Option<&S>) + 'static,
) -> EffectHandle
where
    S: Clone + PartialEq + 'static,
{
    current().watch(source, handler)
}

/// Runs `handler` whenever the value produced by `source` changes, in `reactor`.
///
/// See [`watch`].
#[track_caller]
pub fn watch_in<S>(
    reactor: &Reactor,
    source: impl Fn() -> S + 'static,
    handler: impl Fn(&S, Option<&S>) + 'static,
) -> EffectHandle
where
    S: Clone + PartialEq + 'static,
{
    reactor.watch(source, handler)
}

impl Reactor {
    /// Runs `handler` whenever the value produced by `source` changes.
    ///
    /// See [`watch`].
    #[track_caller]
    pub fn watch<S>(
        &self,
        source: impl Fn() -> S + 'static,
        handler: impl Fn(&S, Option<&S>) + 'static,
    ) -> EffectHandle
    where
        S: Clone + PartialEq + 'static,
    {
        // The memo provides both the tracked scope for `source` and the equality gate: when the
        // source recomputes to an equal value, the effect below is not re-run at all.
        let gated = self.memo(source);
        let previous: Rc<RefCell<Option<S>>> = Rc::new(RefCell::new(None));

        self.effect(move || {
            let new = gated.get();
            crate::untrack(|| {
                // The borrow does not span the handler, and the write-back happens afterwards so a
                // panicking handler leaves `previous` at the last value it actually handled.
                //
                // Belt-and-braces as of 0.3, and worth saying so rather than leaving a defence
                // whose threat has moved: a handler that writes the watched source and flushes can
                // no longer re-enter this effect at all. `run_scheduled_inner` defers a run
                // requested while this one is in progress, and `run_in_context` asserts — in
                // *every* build now, not only debug — that an observer cannot re-enter itself.
                // Measured both ways: holding the borrow across the handler keeps the whole suite
                // green in both profiles, and with the deferral also removed the re-entrancy
                // assert fires before the inner run ever reaches this line. So there is no test
                // that can distinguish the two shapes today; this one is kept because it costs one
                // clone of a value the API already requires to be `Clone`, and it stops being
                // decoration the moment `watch` can be given a scheduler that runs the effect
                // inline, or either guard is relaxed.
                let prior = previous.borrow().clone();
                handler(&new, prior.as_ref());
                *previous.borrow_mut() = Some(new);
            });
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use runite::{queue_macrotask, run};

    use crate::{Reactor, signal_in};

    #[test]
    fn watch_fires_on_change_with_old_and_new_values() {
        let seen = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            move || {
                let reactor = Reactor::new();
                let value = signal_in(&reactor, 1i32);

                let handle = reactor.watch(
                    {
                        let value = value.clone();
                        move || value.get()
                    },
                    {
                        let seen = Rc::clone(&seen);
                        move |new: &i32, old: Option<&i32>| {
                            seen.borrow_mut().push((old.copied(), *new));
                        }
                    },
                );

                reactor.flush_now();
                value.set(2);
                reactor.flush_now();
                value.set(2); // unchanged: no invocation
                reactor.flush_now();
                value.set(5);
                reactor.flush_now();

                handle.leak();
            }
        });

        run();
        assert_eq!(&*seen.borrow(), &[(None, 1), (Some(1), 2), (Some(2), 5)],);
    }

    #[test]
    fn watch_source_is_equality_gated_and_handler_is_untracked() {
        let runs = Rc::new(RefCell::new(0usize));
        // Record inside the macrotask, assert outside it. runite wraps each scheduled task in
        // `catch_unwind`, so an `assert!` that fires inside this closure is swallowed and `run()`
        // returns normally — the test would report `ok` however wrong the counts were. Measured:
        // with the first assertion below rewritten to expect 4242, this test still passed. The
        // recorded log doubles as the completion sentinel, because a closure that panicked early
        // or never ran leaves it short.
        let observed = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let runs = Rc::clone(&runs);
            let observed = Rc::clone(&observed);
            move || {
                let reactor = Reactor::new();
                let numerator = signal_in(&reactor, 4i32);
                let side_input = signal_in(&reactor, 0i32);

                let handle = reactor.watch(
                    {
                        // The tracked source: parity of the numerator.
                        let numerator = numerator.clone();
                        move || numerator.get() % 2
                    },
                    {
                        // The untracked handler reads a signal it must not subscribe to.
                        let runs = Rc::clone(&runs);
                        let side_input = side_input.clone();
                        move |_new: &i32, _old: Option<&i32>| {
                            let _ = side_input.get();
                            *runs.borrow_mut() += 1;
                        }
                    },
                );

                let record = |stage: &'static str| {
                    observed.borrow_mut().push((stage, *runs.borrow()));
                };

                reactor.flush_now();
                record("first invocation");

                // Source recomputes but its value (parity) is unchanged: no invocation.
                numerator.set(6);
                reactor.flush_now();
                record("source recomputed to an equal value");

                // The handler read this, but did not subscribe to it: no invocation.
                side_input.set(99);
                reactor.flush_now();
                record("handler's untracked read was written");

                numerator.set(7);
                reactor.flush_now();
                record("parity changed");

                handle.leak();
            }
        });

        run();

        assert_eq!(
            &*observed.borrow(),
            &[
                ("first invocation", 1),
                ("source recomputed to an equal value", 1),
                ("handler's untracked read was written", 1),
                ("parity changed", 2),
            ]
        );
    }
}
