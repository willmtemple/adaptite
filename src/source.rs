use alloc::rc::Rc;

use crate::{NodeId, Reactor, current, trace_targets};

/// Creates a low-level reactive source node in the current reactor.
#[track_caller]
pub fn source() -> Source {
    current().source()
}

/// Creates a low-level reactive source node associated with `reactor`.
#[track_caller]
pub fn source_in(reactor: &Reactor) -> Source {
    reactor.source()
}

/// Creates a source that reports when it gains its first observer and loses its last, in the
/// current reactor.
///
/// See [`Reactor::source_with_hooks`] for the delivery contract.
#[track_caller]
pub fn source_with_hooks(on_watch: impl Fn() + 'static, on_unwatch: impl Fn() + 'static) -> Source {
    current().source_with_hooks(on_watch, on_unwatch)
}

/// Creates a source with observation hooks associated with `reactor`.
#[track_caller]
pub fn source_with_hooks_in(
    reactor: &Reactor,
    on_watch: impl Fn() + 'static,
    on_unwatch: impl Fn() + 'static,
) -> Source {
    reactor.source_with_hooks(on_watch, on_unwatch)
}

/// Low-level observable source node.
///
/// `Source` is useful for advanced data structures that want precise control over when reads
/// observe and writes trigger invalidation without storing their state in a [`crate::Signal`].
///
/// # Examples
///
/// Wrapping state that lives outside the graph:
///
/// ```rust
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// use adaptite::{source, thunk};
///
/// let external = Rc::new(Cell::new(1));
/// let node = source();
///
/// let view = thunk({
///     let node = node.clone();
///     let external = Rc::clone(&external);
///     move || {
///         node.observe(); // reads of `external` depend on `node`
///         external.get() * 10
///     }
/// });
///
/// assert_eq!(view.get(), 10);
///
/// external.set(2);
/// assert_eq!(view.get(), 10); // the graph has not been told about the write
///
/// node.trigger();
/// assert_eq!(view.get(), 20); // now the thunk recomputes
/// ```
#[derive(Clone)]
pub struct Source {
    inner: Rc<SourceInner>,
}

impl Reactor {
    /// Creates a low-level source node associated with this reactor.
    #[track_caller]
    pub fn source(&self) -> Source {
        Source::new(self.clone())
    }

    /// Creates a source that calls `on_watch` when it gains its first observer and `on_unwatch`
    /// when it loses its last.
    ///
    /// This ties an external resource's lifetime to whether anyone is actually reading the node:
    /// connect a websocket, file watcher, or upstream subscription on the first observer and tear
    /// it down when the last one leaves. [`Source::is_observed`] answers the same question by
    /// polling, which suffices for sweep-based garbage collection; hooks are for the case where
    /// the release must be prompt rather than swept.
    ///
    /// # Delivery
    ///
    /// Callbacks are **deferred**, not inline. The "last observer left" transition happens while
    /// the reactor is clearing dependencies during an observer's rerun or disposal, with graph
    /// maps borrowed, so hooks are queued as reactor jobs and run on the next flush. Two
    /// consequences follow:
    ///
    /// - A leave/arrive pair within one flush — exactly what an observer's rerun looks like —
    ///   collapses to nothing, so a rerunning reader does not churn the resource.
    /// - Only a genuine change from the last delivered state fires a callback. Neither hook is
    ///   ever called twice in a row.
    ///
    /// Hooks are unregistered when the `Source` is dropped, and a callback already queued at that
    /// point is cancelled.
    ///
    /// # Staleness
    ///
    /// "Observed" here means *any recorded dependency edge*, which reflects each observer's most
    /// recent run. A reader that stopped reading this source still counts until it reruns or is
    /// disposed, so `on_unwatch` can be late — but it is never early, which is the safe direction
    /// for resource lifetimes. It also counts a cold cached memo nobody has pulled recently, which
    /// TC39's `Signal.subtle.watched` would not; that finer notion needs liveness counts
    /// propagated through every edge, and this ships the coarse one until real usage shows the
    /// difference matters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    ///
    /// use adaptite::{Reactor, source_with_hooks_in, thunk_in};
    ///
    /// let reactor = Reactor::new();
    /// let log = Rc::new(RefCell::new(Vec::new()));
    ///
    /// let node = source_with_hooks_in(
    ///     &reactor,
    ///     { let log = Rc::clone(&log); move || log.borrow_mut().push("connect") },
    ///     { let log = Rc::clone(&log); move || log.borrow_mut().push("disconnect") },
    /// );
    ///
    /// let reader = thunk_in(&reactor, {
    ///     let node = node.clone();
    ///     move || { node.observe(); 1 }
    /// });
    ///
    /// let _ = reader.get();
    /// reactor.flush_now();
    /// assert_eq!(*log.borrow(), ["connect"]);
    ///
    /// // The last reader goes away, and the resource is released.
    /// drop(reader);
    /// reactor.flush_now();
    /// assert_eq!(*log.borrow(), ["connect", "disconnect"]);
    /// ```
    #[track_caller]
    pub fn source_with_hooks(
        &self,
        on_watch: impl Fn() + 'static,
        on_unwatch: impl Fn() + 'static,
    ) -> Source {
        let source = Source::new(self.clone());
        self.register_observation_hooks(source.id(), on_watch, on_unwatch);
        source
    }
}

impl Source {
    #[track_caller]
    fn new(reactor: Reactor) -> Self {
        let id = reactor.allocate_node();
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "create_source",
            node_id = id.0,
            "created low-level reactive source"
        );
        Self {
            inner: Rc::new(SourceInner { reactor, id }),
        }
    }

    /// Records a dependency on this source for the currently running observer.
    pub fn observe(&self) {
        self.inner.reactor.observe(self.inner.id);
    }

    /// Triggers this source's dependents.
    #[track_caller]
    pub fn trigger(&self) {
        self.inner.reactor.trigger(self.inner.id);
    }

    /// Returns the source node id.
    pub fn id(&self) -> NodeId {
        self.inner.id
    }

    /// Returns `true` if any live observer currently records a dependency on this source.
    ///
    /// The answer reflects each observer's most recent run: an observer that stopped reading
    /// this source still counts until it next re-runs or is disposed, so `is_observed` can
    /// return `true` for a source that will never be read again — but never `false` for one
    /// that is still depended on. Fine-grained data structures use this to garbage-collect
    /// per-key sources nobody reads anymore; see [`Reactor::is_observed`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use adaptite::{source, thunk};
    ///
    /// let node = source();
    /// let view = thunk({
    ///     let node = node.clone();
    ///     move || {
    ///         node.observe();
    ///         1
    ///     }
    /// });
    ///
    /// assert!(!node.is_observed(), "nothing has read through the source yet");
    /// let _ = view.get();
    /// assert!(node.is_observed());
    /// ```
    pub fn is_observed(&self) -> bool {
        self.inner.reactor.is_observed(self.inner.id)
    }
}

impl core::fmt::Debug for Source {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Source")
            .field("id", &self.inner.id)
            .finish()
    }
}

struct SourceInner {
    reactor: Reactor,
    id: NodeId,
}

impl Drop for SourceInner {
    fn drop(&mut self) {
        self.reactor.dispose(self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use runite::{queue_macrotask, run};

    use crate::{EffectHandle, Reactor, Source, source_in, source_with_hooks_in, thunk_in};

    /// Records watch/unwatch deliveries for a hooked source.
    fn hooked(reactor: &Reactor) -> (Source, Rc<RefCell<Vec<&'static str>>>) {
        let log = Rc::new(RefCell::new(Vec::new()));
        let node = source_with_hooks_in(
            reactor,
            {
                let log = Rc::clone(&log);
                move || log.borrow_mut().push("watch")
            },
            {
                let log = Rc::clone(&log);
                move || log.borrow_mut().push("unwatch")
            },
        );
        (node, log)
    }

    #[test]
    fn hooks_fire_on_the_first_observer_and_the_last_departure() {
        let reactor = Reactor::new();
        let (node, log) = hooked(&reactor);

        reactor.flush_now();
        assert!(log.borrow().is_empty(), "an unread source is not watched");

        let reader = thunk_in(&reactor, {
            let node = node.clone();
            move || {
                node.observe();
                1
            }
        });
        // The edge is recorded on read, not on construction.
        assert!(log.borrow().is_empty());
        let _ = reader.get();
        reactor.flush_now();
        assert_eq!(*log.borrow(), ["watch"]);

        drop(reader);
        reactor.flush_now();
        assert_eq!(*log.borrow(), ["watch", "unwatch"]);
    }

    #[test]
    fn hooks_are_deferred_not_inline() {
        // The unwatch transition happens while the reactor holds its graph maps borrowed, so a
        // hook that touches the graph must not run there.
        let reactor = Reactor::new();
        let (node, log) = hooked(&reactor);

        let reader = thunk_in(&reactor, {
            let node = node.clone();
            move || {
                node.observe();
                1
            }
        });
        let _ = reader.get();
        assert!(
            log.borrow().is_empty(),
            "delivery waits for the flush, even for the watch edge"
        );
        reactor.flush_now();
        assert_eq!(*log.borrow(), ["watch"]);
    }

    #[test]
    fn a_leave_and_arrive_within_one_flush_collapses() {
        // An observer's rerun clears and re-records its edges. That must not churn the resource.
        let reactor = Reactor::new();
        let (node, log) = hooked(&reactor);
        let toggle = crate::signal_in(&reactor, 0);

        let reader = thunk_in(&reactor, {
            let node = node.clone();
            let toggle = toggle.clone();
            move || {
                toggle.get();
                node.observe();
                1
            }
        });
        let _ = reader.get();
        reactor.flush_now();
        assert_eq!(*log.borrow(), ["watch"]);

        // Force a rerun: the edge is dropped and immediately re-recorded.
        toggle.set(1);
        let _ = reader.get();
        reactor.flush_now();
        assert_eq!(
            *log.borrow(),
            ["watch"],
            "a rerunning reader must not disconnect and reconnect"
        );

        drop(reader);
        reactor.flush_now();
        assert_eq!(*log.borrow(), ["watch", "unwatch"]);
    }

    #[test]
    fn neither_hook_is_delivered_twice_in_a_row() {
        let reactor = Reactor::new();
        let (node, log) = hooked(&reactor);

        // Two independent readers: only the first arrival and the last departure are transitions.
        let readers: Vec<_> = (0..2)
            .map(|_| {
                thunk_in(&reactor, {
                    let node = node.clone();
                    move || {
                        node.observe();
                        1
                    }
                })
            })
            .collect();
        for reader in &readers {
            let _ = reader.get();
        }
        reactor.flush_now();
        assert_eq!(*log.borrow(), ["watch"], "one watch, not two");

        let mut readers = readers;
        drop(readers.pop());
        reactor.flush_now();
        assert_eq!(*log.borrow(), ["watch"], "one reader remains");

        drop(readers);
        reactor.flush_now();
        assert_eq!(*log.borrow(), ["watch", "unwatch"]);
    }

    #[test]
    fn dropping_the_source_cancels_a_queued_hook() {
        let reactor = Reactor::new();
        let (node, log) = hooked(&reactor);

        let reader = thunk_in(&reactor, {
            let node = node.clone();
            move || {
                node.observe();
                1
            }
        });
        let _ = reader.get();

        // The watch delivery is queued but not yet flushed.
        assert!(log.borrow().is_empty());
        drop(node);
        drop(reader);
        reactor.flush_now();
        assert!(
            log.borrow().is_empty(),
            "a disposed source must not deliver hooks"
        );
    }

    #[test]
    fn is_observed_agrees_with_the_hooks_but_answers_immediately() {
        // The polled and pushed views of the same edge state: `is_observed` is synchronous,
        // hooks are deferred. Both must describe the same graph.
        let reactor = Reactor::new();
        let (node, log) = hooked(&reactor);

        let reader = thunk_in(&reactor, {
            let node = node.clone();
            move || {
                node.observe();
                1
            }
        });
        let _ = reader.get();

        assert!(node.is_observed(), "the edge exists immediately");
        assert!(log.borrow().is_empty(), "the hook has not been delivered");

        reactor.flush_now();
        assert!(node.is_observed());
        assert_eq!(*log.borrow(), ["watch"]);
    }

    #[test]
    fn sources_have_distinct_ids() {
        let reactor = Reactor::new();
        let one = source_in(&reactor);
        let two = source_in(&reactor);
        assert_ne!(one.id(), two.id());
    }

    #[test]
    fn observe_and_trigger_drive_an_effect_around_external_state() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let keep_alive = Rc::new(RefCell::new(None::<(Source, EffectHandle)>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let keep_alive = Rc::clone(&keep_alive);
            move || {
                let reactor = Reactor::new();
                let source = source_in(&reactor);
                // State managed outside the reactive graph; the source stands in for it.
                let external = Rc::new(Cell::new(1usize));

                let effect = reactor.effect({
                    let source = source.clone();
                    let external = Rc::clone(&external);
                    let seen = Rc::clone(&seen);
                    move || {
                        source.observe();
                        seen.borrow_mut().push(external.get());
                    }
                });
                *keep_alive.borrow_mut() = Some((source.clone(), effect));

                runite::queue_macrotask(move || {
                    external.set(2);
                    source.trigger();
                });
            }
        });

        run();
        assert_eq!(&*seen.borrow(), &[1, 2]);
    }
}
