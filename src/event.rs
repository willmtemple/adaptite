use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};

use hashbrown::HashMap;

use crate::{NodeId, Reactor, current, trace_targets};

type SubscriberFn<T> = dyn Fn(&T) + 'static;

/// Creates an event in the current thread's default reactor.
pub fn event<T: 'static>() -> Event<T> {
    current().event()
}

/// Creates an event associated with `reactor`.
pub fn event_in<T: 'static>(reactor: &Reactor) -> Event<T> {
    reactor.event()
}

/// Creates a reactive draining subscription for `event` in the current reactor.
#[must_use = "subscriptions created with `on` must be used to stay active"]
pub fn on<T: Clone + 'static>(event: &Event<T>, handler: impl Fn(&T) + 'static) -> Subscription {
    current().on(event, handler)
}

/// Creates a reactive draining subscription for `event` associated with `reactor`.
#[must_use = "subscriptions created with `on_in` must be used to stay active"]
pub fn on_in<T: Clone + 'static>(
    reactor: &Reactor,
    event: &Event<T>,
    handler: impl Fn(&T) + 'static,
) -> Subscription {
    reactor.on(event, handler)
}

/// Push-style event source.
#[derive(Clone)]
pub struct Event<T> {
    inner: Rc<EventInner<T>>,
}

/// Disposable subscription handle.
#[derive(Clone)]
pub struct Subscription {
    inner: Rc<SubscriptionInner>,
}

impl Reactor {
    /// Creates an event source associated with this reactor.
    pub fn event<T: 'static>(&self) -> Event<T> {
        Event::new(self.clone())
    }

    /// Creates a reactive draining subscription for `event`.
    #[must_use = "subscriptions created with `on` must be used to stay active"]
    pub fn on<T: Clone + 'static>(
        &self,
        event: &Event<T>,
        handler: impl Fn(&T) + 'static,
    ) -> Subscription {
        let queue = Rc::new(RefCell::new(Vec::new()));
        let direct = event.subscribe({
            let queue = Rc::clone(&queue);
            move |value| queue.borrow_mut().push(value.clone())
        });
        let effect = self.effect({
            let event = event.clone();
            let queue = Rc::clone(&queue);
            move || {
                event.observe();
                let drained = {
                    let mut queued = queue.borrow_mut();
                    queued.drain(..).collect::<Vec<_>>()
                };
                #[cfg(debug_assertions)]
                tracing::trace!(
                    target: trace_targets::EVENT,
                    event = "drain_event_queue",
                    event_id = event.inner.id.0,
                    drained = drained.len(),
                    "draining queued event values reactively"
                );
                for value in &drained {
                    handler(value);
                }
            }
        });

        Subscription::new(move || {
            direct.unsubscribe();
            effect.dispose();
        })
    }
}

impl<T: 'static> Event<T> {
    fn new(reactor: Reactor) -> Self {
        let id = reactor.allocate_node();
        tracing::debug!(
            target: trace_targets::EVENT,
            event = "create_event",
            node_id = id.0,
            "created reactive event"
        );
        Self {
            inner: Rc::new(EventInner {
                reactor,
                id,
                next_subscriber: Cell::new(1),
                subscribers: RefCell::new(Default::default()),
            }),
        }
    }

    fn observe(&self) {
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::EVENT,
            event = "observe_event",
            event_id = self.inner.id.0,
            "observing event reactively"
        );
        self.inner.reactor.observe(self.inner.id);
    }

    /// Emits a value to immediate subscribers, then notifies reactive dependents.
    pub fn emit(&self, value: T) {
        let subscribers = self
            .inner
            .subscribers
            .borrow()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        tracing::debug!(
            target: trace_targets::EVENT,
            event = "emit_event",
            event_id = self.inner.id.0,
            subscriber_count = subscribers.len(),
            "emitting event value"
        );
        for subscriber in subscribers {
            subscriber(&value);
        }
        self.inner.reactor.trigger(self.inner.id);
    }

    /// Adds an immediate subscriber to this event.
    pub fn subscribe(&self, handler: impl Fn(&T) + 'static) -> Subscription {
        let id = self.inner.next_subscriber.get();
        self.inner.next_subscriber.set(id.wrapping_add(1));
        self.inner
            .subscribers
            .borrow_mut()
            .insert(id, Rc::new(handler) as Rc<SubscriberFn<T>>);
        tracing::debug!(
            target: trace_targets::EVENT,
            event = "subscribe_event",
            event_id = self.inner.id.0,
            subscription_id = id,
            subscriber_count = self.inner.subscribers.borrow().len(),
            "added event subscriber"
        );

        let inner = Rc::clone(&self.inner);
        Subscription::new(move || {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::EVENT,
                event = "unsubscribe_event",
                event_id = inner.id.0,
                subscription_id = id,
                "removing event subscriber"
            );
            inner.subscribers.borrow_mut().remove(&id);
        })
    }
}

impl<T> Drop for EventInner<T> {
    fn drop(&mut self) {
        self.reactor.dispose(self.id);
    }
}

impl Subscription {
    fn new(cancel: impl Fn() + 'static) -> Self {
        Self {
            inner: Rc::new(SubscriptionInner {
                active: Cell::new(true),
                cancel: Box::new(cancel),
            }),
        }
    }

    /// Cancels the subscription immediately.
    pub fn unsubscribe(&self) {
        self.inner.unsubscribe();
    }

    /// Returns `true` if the subscription is still active.
    pub fn is_active(&self) -> bool {
        self.inner.active.get()
    }
}

struct EventInner<T> {
    reactor: Reactor,
    id: NodeId,
    next_subscriber: Cell<usize>,
    subscribers: RefCell<HashMap<usize, Rc<SubscriberFn<T>>>>,
}

struct SubscriptionInner {
    active: Cell<bool>,
    cancel: Box<dyn Fn() + 'static>,
}

impl SubscriptionInner {
    fn unsubscribe(&self) {
        if !self.active.replace(false) {
            return;
        }
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::EVENT,
            event = "unsubscribe",
            "cancelling subscription"
        );
        (self.cancel)();
    }
}

// TODO: it's actually kind of hard to use events if subcscriptions have to be manually managed.
// impl Drop for SubscriptionInner {
//     fn drop(&mut self) {
//         self.unsubscribe();
//     }
// }

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use runite::{queue_task, run};

    use crate::{Reactor, event_in};

    use super::{Subscription, on_in};

    #[test]
    fn emit_delivers_immediately_and_on_drains_reactively() {
        let immediate = Rc::new(RefCell::new(Vec::new()));
        let reactive = Rc::new(RefCell::new(Vec::new()));
        let keep_alive = Rc::new(RefCell::new(Vec::<Subscription>::new()));

        queue_task({
            let immediate = Rc::clone(&immediate);
            let reactive = Rc::clone(&reactive);
            let keep_alive = Rc::clone(&keep_alive);
            move || {
                let reactor = Reactor::new();
                let event = event_in::<usize>(&reactor);

                let direct = event.subscribe({
                    let immediate = Rc::clone(&immediate);
                    move |value| immediate.borrow_mut().push(*value)
                });
                let draining = on_in(&reactor, &event, {
                    let reactive = Rc::clone(&reactive);
                    move |value| reactive.borrow_mut().push(*value)
                });

                event.emit(1);
                event.emit(2);

                assert_eq!(&*immediate.borrow(), &[1, 2]);
                assert!(
                    reactive.borrow().is_empty(),
                    "reactive drain should be deferred"
                );

                keep_alive.borrow_mut().extend([direct, draining]);
            }
        });

        run();
        assert_eq!(&*reactive.borrow(), &[1, 2]);
    }
}
