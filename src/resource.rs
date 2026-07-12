use alloc::rc::Rc;
use core::cell::Cell;
use core::future::Future;

use crate::observable::Observable;
use crate::{EffectHandle, Reactor, Signal, current, trace_targets};

/// Creates a [`Resource`] in the current thread's default reactor.
///
/// See [`Resource`] for semantics.
#[track_caller]
pub fn resource<S, T, Fut>(
    source: impl Fn() -> S + 'static,
    fetch: impl Fn(S) -> Fut + 'static,
) -> Resource<T>
where
    S: Clone + PartialEq + 'static,
    T: 'static,
    Fut: Future<Output = T> + 'static,
{
    current().resource(source, fetch)
}

/// Creates a [`Resource`] associated with `reactor`.
///
/// See [`Resource`] for semantics.
#[track_caller]
pub fn resource_in<S, T, Fut>(
    reactor: &Reactor,
    source: impl Fn() -> S + 'static,
    fetch: impl Fn(S) -> Fut + 'static,
) -> Resource<T>
where
    S: Clone + PartialEq + 'static,
    T: 'static,
    Fut: Future<Output = T> + 'static,
{
    reactor.resource(source, fetch)
}

/// Reactive async state: a value fetched by a future, re-fetched when its inputs change.
///
/// A resource bridges the reactive graph and runite's async side in the inward direction:
///
/// - `source` runs **tracked** and produces the fetch input. Whenever the input actually
///   changes (it is equality-gated like a memo), a new fetch starts.
/// - `fetch` receives the input and returns a future, which is spawned on the runite runtime
///   as a task local to the current thread (it need not be `Send`); a runite runtime must be
///   driving this thread for fetches to make progress. When the future completes, the
///   resource's value updates and dependents re-run.
/// - A fetch that is superseded (its input changed again) or whose resource is disposed is
///   **aborted**, and a late completion from a stale fetch can never overwrite a newer value.
///
/// The value is `None` until the first fetch completes; [`loading`](Resource::loading) is
/// `true` while a fetch is in flight (the previous value is retained during a refetch, so UIs
/// can render stale data with a spinner). The first fetch does not start inline with the
/// constructor: it begins when the driving effect first runs, on the next microtask flush.
/// Errors are not given special treatment: make `T` a `Result` if the fetch can fail.
///
/// A resource created inside an owner (an effect run or [`crate::scope`]) is disposed with it,
/// cancelling any in-flight fetch. Otherwise it stops fetching when its last handle is dropped.
///
/// # Examples
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{effect, resource, signal};
/// use runite::{queue_macrotask, run};
///
/// let seen = Rc::new(RefCell::new(Vec::new()));
///
/// queue_macrotask({
///     let seen = Rc::clone(&seen);
///     move || {
///         let user_id = signal(1u32);
///
///         let user_name = resource(
///             {
///                 let user_id = user_id.clone();
///                 move || user_id.get()
///             },
///             // In a real application this would await I/O.
///             |id| async move { format!("user-{id}") },
///         );
///
///         effect({
///             let user_name = user_name.clone();
///             let seen = Rc::clone(&seen);
///             move || seen.borrow_mut().push(user_name.get())
///         })
///         .leak();
///     }
/// });
/// run();
///
/// assert_eq!(*seen.borrow(), [None, Some(String::from("user-1"))]);
/// ```
pub struct Resource<T> {
    value: Signal<Option<T>>,
    loading: Signal<bool>,
    refetch_tick: Signal<u64>,
    /// Keeps the driving effect alive when the resource is unowned.
    _effect: EffectHandle,
}

impl<T> Clone for Resource<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            loading: self.loading.clone(),
            refetch_tick: self.refetch_tick.clone(),
            _effect: self._effect.clone(),
        }
    }
}

impl Reactor {
    /// Creates a [`Resource`] associated with this reactor.
    ///
    /// See [`Resource`] for semantics.
    #[track_caller]
    pub fn resource<S, T, Fut>(
        &self,
        source: impl Fn() -> S + 'static,
        fetch: impl Fn(S) -> Fut + 'static,
    ) -> Resource<T>
    where
        S: Clone + PartialEq + 'static,
        T: 'static,
        Fut: Future<Output = T> + 'static,
    {
        let value = self.signal(None::<T>);
        let loading = self.signal(false);
        let refetch_tick = self.signal(0u64);
        // Guards against a stale fetch completing after a newer one already wrote: only the
        // most recently started generation may publish its result.
        let generation = Rc::new(Cell::new(0u64));

        // The equality gate: the driving effect re-runs only when the fetch input actually
        // changed (or a refetch was requested), not whenever some dependency of `source` moved.
        let gated_input = self.memo(source);

        let effect = self.effect({
            let value = value.clone();
            let loading = loading.clone();
            let refetch_tick = refetch_tick.clone();
            let generation = Rc::clone(&generation);
            move || {
                // Tracked reads: the fetch input and the refetch counter.
                let input = gated_input.get();
                let _ = refetch_tick.get();

                crate::untrack(|| {
                    let fetch_generation = generation.get().wrapping_add(1);
                    generation.set(fetch_generation);
                    let _ = loading.set(true);

                    tracing::debug!(
                        target: trace_targets::RESOURCE,
                        event = "resource_fetch",
                        generation = fetch_generation,
                        "starting resource fetch"
                    );

                    let future = fetch(input);
                    let handle = runite::spawn({
                        let value = value.clone();
                        let loading = loading.clone();
                        let generation = Rc::clone(&generation);
                        async move {
                            let result = future.await;
                            if generation.get() == fetch_generation {
                                tracing::debug!(
                                    target: trace_targets::RESOURCE,
                                    event = "resource_ready",
                                    generation = fetch_generation,
                                    "resource fetch completed"
                                );
                                value.replace(Some(result));
                                let _ = loading.set(false);
                            } else {
                                tracing::debug!(
                                    target: trace_targets::RESOURCE,
                                    event = "resource_stale",
                                    generation = fetch_generation,
                                    "discarding stale resource fetch"
                                );
                            }
                        }
                    });

                    // Cancel this fetch when the effect re-runs (a newer fetch supersedes it)
                    // or when the resource's owner is disposed. Clear `loading` too: an aborted
                    // fetch never reports completion, and on supersession the next run re-sets
                    // it before anything can observe the gap.
                    let abort = handle.abort_handle();
                    let loading = loading.clone();
                    crate::on_cleanup(move || {
                        abort.abort();
                        let _ = loading.set(false);
                    });
                });
            }
        });

        Resource {
            value,
            loading,
            refetch_tick,
            _effect: effect,
        }
    }
}

impl<T: 'static> Resource<T> {
    /// Runs `f` with a shared reference to the current value (`None` until the first fetch
    /// completes), recording a dependency.
    pub fn with<R>(&self, f: impl FnOnce(&Option<T>) -> R) -> R {
        self.value.with(f)
    }

    /// Runs `f` with a shared reference to the current value without recording a dependency.
    pub fn with_peek<R>(&self, f: impl FnOnce(&Option<T>) -> R) -> R {
        self.value.with_peek(f)
    }

    /// Returns `true` while a fetch is in flight, recording a dependency.
    ///
    /// `loading` is tracked separately from the value: an observer that reads only `loading`
    /// re-runs on loading transitions, not when the value updates (and vice versa). It is
    /// `false` until the first fetch starts (on the first microtask flush after creation), and
    /// it is cleared when the resource's owner disposes an in-flight fetch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    ///
    /// use adaptite::{effect, resource, signal};
    /// use runite::{queue_macrotask, run};
    ///
    /// let frames = Rc::new(RefCell::new(Vec::new()));
    ///
    /// queue_macrotask({
    ///     let frames = Rc::clone(&frames);
    ///     move || {
    ///         let query = signal("adaptite");
    ///         let results = resource(
    ///             {
    ///                 let query = query.clone();
    ///                 move || query.get()
    ///             },
    ///             |q| async move { format!("results for {q}") },
    ///         );
    ///
    ///         effect({
    ///             let results = results.clone();
    ///             let frames = Rc::clone(&frames);
    ///             move || frames.borrow_mut().push((results.get(), results.loading()))
    ///         })
    ///         .leak();
    ///     }
    /// });
    /// run();
    ///
    /// assert_eq!(
    ///     *frames.borrow(),
    ///     [
    ///         (None, true), // fetch in flight: render a spinner
    ///         (Some(String::from("results for adaptite")), false),
    ///     ]
    /// );
    /// ```
    pub fn loading(&self) -> bool {
        self.loading.get()
    }

    /// Starts a new fetch with the current input, even though the input has not changed.
    ///
    /// Has no effect once the resource has been disposed.
    pub fn refetch(&self) {
        self.refetch_tick
            .update(|tick| *tick = tick.wrapping_add(1));
    }
}

impl<T: Clone + 'static> Resource<T> {
    /// Clones and returns the current value, recording a dependency.
    pub fn get(&self) -> Option<T> {
        self.with(Clone::clone)
    }

    /// Clones and returns the current value without recording a dependency.
    pub fn peek(&self) -> Option<T> {
        self.with_peek(Clone::clone)
    }
}

impl<T: 'static> Observable for Resource<T> {
    type Item = Option<T>;

    fn with<R>(&self, f: impl FnOnce(&Option<T>) -> R) -> R {
        Resource::with(self, f)
    }

    fn with_peek<R>(&self, f: impl FnOnce(&Option<T>) -> R) -> R {
        Resource::with_peek(self, f)
    }
}

impl<T> core::fmt::Debug for Resource<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Resource").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    use runite::time::sleep;
    use runite::{queue_macrotask, run};

    use crate::{Reactor, scope, signal_in};

    #[test]
    fn resource_fetches_refetches_on_input_change_and_on_request() {
        let seen = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            move || {
                let reactor = Reactor::new();
                let id = signal_in(&reactor, 1u32);

                let doubled = reactor.resource(
                    {
                        let id = id.clone();
                        move || id.get()
                    },
                    |id| async move { id * 10 },
                );

                reactor
                    .effect({
                        let doubled = doubled.clone();
                        let seen = Rc::clone(&seen);
                        move || seen.borrow_mut().push(doubled.get())
                    })
                    .leak();

                std::mem::drop(runite::spawn(async move {
                    sleep(Duration::from_millis(15)).await;
                    // A dependency write that leaves the input unchanged must not refetch.
                    let _ = id.set(1);
                    sleep(Duration::from_millis(15)).await;
                    id.set(2);
                    sleep(Duration::from_millis(15)).await;
                    // Same input, explicit refetch.
                    doubled.refetch();
                }));
            }
        });

        run();
        assert_eq!(
            &*seen.borrow(),
            &[None, Some(10), Some(20), Some(20)],
            "initial fetch, input-change refetch, and explicit refetch"
        );
    }

    #[test]
    fn superseded_fetches_are_aborted_and_never_publish() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let loading_mid_flight = Rc::new(RefCell::new(None::<bool>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let loading_mid_flight = Rc::clone(&loading_mid_flight);
            move || {
                let reactor = Reactor::new();
                let input = signal_in(&reactor, "slow");

                // The slow fetch takes 60ms; the fast one 5ms.
                let fetched = reactor.resource(
                    {
                        let input = input.clone();
                        move || input.get()
                    },
                    |kind: &'static str| async move {
                        let delay = if kind == "slow" { 60 } else { 5 };
                        sleep(Duration::from_millis(delay)).await;
                        kind
                    },
                );

                reactor
                    .effect({
                        let fetched = fetched.clone();
                        let seen = Rc::clone(&seen);
                        move || seen.borrow_mut().push(fetched.get())
                    })
                    .leak();

                std::mem::drop(runite::spawn(async move {
                    sleep(Duration::from_millis(15)).await;
                    // The slow fetch is still in flight.
                    *loading_mid_flight.borrow_mut() = Some(fetched.loading());
                    input.set("fast");
                    sleep(Duration::from_millis(80)).await;
                    assert!(!fetched.loading(), "no fetch should remain in flight");
                }));
            }
        });

        run();
        assert!(
            loading_mid_flight.borrow().unwrap(),
            "a fetch should have been in flight mid-test"
        );
        assert_eq!(
            &*seen.borrow(),
            &[None, Some("fast")],
            "the superseded slow fetch must never publish"
        );
    }

    #[test]
    fn disposing_the_owner_aborts_the_in_flight_fetch() {
        let completed = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let completed = Rc::clone(&completed);
            move || {
                let reactor = Reactor::new();

                let (handle, fetched) = scope({
                    let reactor = reactor.clone();
                    let completed = Rc::clone(&completed);
                    move || {
                        reactor.resource(
                            || (),
                            move |()| {
                                let completed = Rc::clone(&completed);
                                async move {
                                    sleep(Duration::from_millis(40)).await;
                                    completed.borrow_mut().push("fetch finished");
                                    42u32
                                }
                            },
                        )
                    }
                });

                std::mem::drop(runite::spawn(async move {
                    sleep(Duration::from_millis(10)).await;
                    assert!(fetched.loading(), "the fetch should be in flight");
                    handle.dispose();
                    sleep(Duration::from_millis(60)).await;
                    assert_eq!(fetched.peek(), None, "aborted fetch must not publish");
                    assert!(
                        !fetched.loading(),
                        "loading must clear when the owner disposes the in-flight fetch"
                    );
                }));
            }
        });

        run();
        assert!(
            completed.borrow().is_empty(),
            "the in-flight fetch must be aborted on owner disposal"
        );
    }
}
