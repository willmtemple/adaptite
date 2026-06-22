use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::rc::{Rc, Weak};
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::error::Error;
use core::fmt;

use hashbrown::{HashMap, HashSet};

use runite::queue_microtask;

use crate::{NodeId, trace_targets};

type Job = Box<dyn FnOnce() + 'static>;

thread_local! {
    static CURRENT_REACTOR: RefCell<Weak<ReactorInner>> = const { RefCell::new(Weak::new()) };
}

/// Returns the current thread's default reactor.
///
/// The first call on a thread creates a new reactor for that thread and caches it in thread-local
/// storage.
pub fn current() -> Reactor {
    Reactor::current()
}

#[allow(dead_code)]
pub(crate) trait ObserverHook {
    fn notify(&self);
}

/// Error type for cycles detected in the reactive graph. Contains the path of nodes that form the cycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactCycleError {
    cycle: Vec<NodeId>,
}

impl ReactCycleError {
    fn new(cycle: Vec<NodeId>) -> Self {
        Self { cycle }
    }

    /// Returns the cycle path that was detected.
    pub fn cycle(&self) -> &[NodeId] {
        &self.cycle
    }
}

impl fmt::Display for ReactCycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reactive cycle detected: ")?;
        for (index, node) in self.cycle.iter().enumerate() {
            if index != 0 {
                write!(f, " -> ")?;
            }
            write!(f, "{node}")?;
        }
        Ok(())
    }
}

impl Error for ReactCycleError {}

/// Single-threaded coordinator for a reactive graph.
///
/// A reactor tracks dependency edges between reactive nodes, manages the currently executing
/// observer stack, and schedules deferred jobs onto the runtime microtask queue.
#[derive(Clone)]
pub struct Reactor {
    inner: Rc<ReactorInner>,
}

impl Reactor {
    /// Creates a new empty reactor.
    pub fn new() -> Self {
        let reactor = Self {
            inner: Rc::new(ReactorInner::new()),
        };
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "reactor_new",
            "created reactive reactor"
        );
        reactor
    }

    /// Returns the current thread's default reactor.
    pub fn current() -> Self {
        if let Some(inner) = CURRENT_REACTOR.with(|r| r.borrow().upgrade()) {
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::GRAPH,
                event = "current_reactor_reuse",
                "reusing current thread default reactor"
            );
            return Self { inner };
        }

        let reactor = Self::new();
        CURRENT_REACTOR.replace(Rc::downgrade(&reactor.inner));
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "current_reactor_install",
            "installed current thread default reactor"
        );
        reactor
    }

    /// Runs `f` in the dependency-tracking scope of `observer`.
    ///
    /// Existing dependencies for `observer` are cleared before `f` runs. Any calls to
    /// [`observe`](Self::observe) made while `f` executes will become the observer's new
    /// dependencies.
    pub fn run_in_context<T>(&self, observer: NodeId, f: impl FnOnce() -> T) -> T {
        let _span = tracing::debug_span!(
            target: trace_targets::GRAPH,
            "reactor.run_in_context",
            observer_id = observer.0
        )
        .entered();
        self.clear_observer_dependencies(observer);
        self.inner.stack.borrow_mut().push(observer);
        let inserted = self.inner.active_computations.borrow_mut().insert(observer);
        debug_assert!(inserted, "observer should not already be active");

        struct Guard<'a> {
            inner: &'a ReactorInner,
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                let popped = self.inner.stack.borrow_mut().pop();
                debug_assert!(popped.is_some(), "reactor observer stack underflow");
                if let Some(node) = popped {
                    let removed = self.inner.active_computations.borrow_mut().remove(&node);
                    debug_assert!(removed, "observer should have been active");
                }
            }
        }

        let _guard = Guard { inner: &self.inner };
        f()
    }

    /// Attempts to record a dependency on `observable` for the currently running observer, returning an
    /// error if doing so would create a dependency cycle.
    pub fn try_observe(&self, observable: NodeId) -> Result<(), ReactCycleError> {
        if self
            .inner
            .active_computations
            .borrow()
            .contains(&observable)
        {
            let stack = self.inner.stack.borrow();
            let start = stack
                .iter()
                .position(|node| *node == observable)
                .expect("active computation should appear in observer stack");
            let mut cycle = stack[start..].to_vec();
            cycle.push(observable);
            tracing::debug!(
                target: trace_targets::GRAPH,
                event = "cycle_detected",
                observable_id = observable.0,
                cycle_len = cycle.len(),
                "reactive cycle detected"
            );
            return Err(ReactCycleError::new(cycle));
        }

        let current = self.inner.stack.borrow().last().copied();
        let Some(observer) = current else {
            return Ok(());
        };

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "observe",
            observer_id = observer.0,
            observable_id = observable.0,
            "recording reactive dependency"
        );

        self.inner
            .dependencies
            .borrow_mut()
            .entry(observer)
            .or_default()
            .insert(observable);
        self.inner
            .dependents
            .borrow_mut()
            .entry(observable)
            .or_default()
            .insert(observer);
        Ok(())
    }

    /// Records a dependency on `observable` for the currently running observer.
    ///
    /// # Panics
    ///
    /// Panics if recording the dependency would create a cycle in the reactive graph.
    pub fn observe(&self, observable: NodeId) {
        if let Err(e) = self.try_observe(observable) {
            panic!("reactive cycle detected: {e}");
        }
    }

    /// Notifies dependents of `observable`.
    pub fn trigger(&self, observable: NodeId) {
        let dependents = self
            .inner
            .dependents
            .borrow()
            .get(&observable)
            .map(|nodes| nodes.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "trigger",
            observable_id = observable.0,
            dependent_count = dependents.len(),
            "triggering reactive dependents"
        );

        for dependent in dependents {
            let hook = self
                .inner
                .observers
                .borrow()
                .get(&dependent)
                .cloned()
                .and_then(|weak| weak.upgrade());
            if let Some(hook) = hook {
                hook.notify();
            } else {
                self.inner.observers.borrow_mut().remove(&dependent);
            }
        }
    }

    /// Disposes all graph bookkeeping for `node`.
    pub fn dispose(&self, node: NodeId) {
        tracing::debug!(
            target: trace_targets::GRAPH,
            event = "dispose_node",
            node_id = node.0,
            "disposing reactive node bookkeeping"
        );
        self.clear_observer_dependencies(node);

        let incoming = self
            .inner
            .dependents
            .borrow_mut()
            .remove(&node)
            .map(|nodes| nodes.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        for observer in incoming {
            let mut dependencies = self.inner.dependencies.borrow_mut();
            if let Some(observed) = dependencies.get_mut(&observer) {
                observed.remove(&node);
                if observed.is_empty() {
                    dependencies.remove(&observer);
                }
            }
        }

        self.inner.observers.borrow_mut().remove(&node);
    }

    /// Schedules a job to run in the reactor's microtask-backed job queue.
    pub fn schedule(&self, job: impl FnOnce() + 'static) {
        self.inner
            .pending_jobs
            .borrow_mut()
            .push_back(Box::new(job));
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "schedule_job",
            pending_jobs = self.inner.pending_jobs.borrow().len(),
            "queued reactive job for microtask flush"
        );
        self.inner.ensure_flush_scheduled();
    }

    /// Flushes currently queued reactive jobs immediately on the calling thread.
    ///
    /// This is useful when host integrations need synchronous propagation
    /// (for example, during native resize loops).
    pub fn flush_now(&self) {
        Rc::clone(&self.inner).flush_jobs();
    }

    pub(crate) fn allocate_node(&self) -> NodeId {
        let raw = self.inner.next_node.get();
        self.inner.next_node.set(raw.wrapping_add(1));
        let id = NodeId::new(raw);
        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "allocate_node",
            node_id = id.0,
            "allocated reactive node id"
        );
        id
    }

    pub(crate) fn register_observer(&self, id: NodeId, observer: Rc<dyn ObserverHook>) {
        self.inner
            .observers
            .borrow_mut()
            .insert(id, Rc::downgrade(&observer));
    }

    pub(crate) fn unregister_observer(&self, id: NodeId) {
        self.inner.observers.borrow_mut().remove(&id);
    }

    fn clear_observer_dependencies(&self, observer: NodeId) {
        let observed = self
            .inner
            .dependencies
            .borrow_mut()
            .remove(&observer)
            .map(|nodes| nodes.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();

        for observable in observed {
            let mut dependents = self.inner.dependents.borrow_mut();
            if let Some(observers) = dependents.get_mut(&observable) {
                observers.remove(&observer);
                if observers.is_empty() {
                    dependents.remove(&observable);
                }
            }
        }
    }
}

impl Default for Reactor {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Reactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reactor")
            .field("ptr", &Rc::as_ptr(&self.inner))
            .finish()
    }
}

struct ReactorInner {
    next_node: Cell<u64>,
    dependencies: RefCell<HashMap<NodeId, HashSet<NodeId>>>,
    dependents: RefCell<HashMap<NodeId, HashSet<NodeId>>>,
    observers: RefCell<HashMap<NodeId, Weak<dyn ObserverHook>>>,
    stack: RefCell<Vec<NodeId>>,
    active_computations: RefCell<HashSet<NodeId>>,
    pending_jobs: RefCell<VecDeque<Job>>,
    flush_scheduled: Cell<bool>,
}

impl ReactorInner {
    fn new() -> Self {
        Self {
            next_node: Cell::new(1),
            dependencies: RefCell::new(HashMap::new()),
            dependents: RefCell::new(HashMap::new()),
            observers: RefCell::new(HashMap::new()),
            stack: RefCell::new(Vec::new()),
            active_computations: RefCell::new(HashSet::new()),
            pending_jobs: RefCell::new(VecDeque::new()),
            flush_scheduled: Cell::new(false),
        }
    }

    fn ensure_flush_scheduled(self: &Rc<Self>) {
        if self.flush_scheduled.replace(true) {
            return;
        }

        #[cfg(debug_assertions)]
        tracing::trace!(
            target: trace_targets::GRAPH,
            event = "schedule_flush",
            pending_jobs = self.pending_jobs.borrow().len(),
            "scheduling reactive microtask flush"
        );
        let reactor = Rc::clone(self);
        queue_microtask(move || {
            reactor.flush_jobs();
        });
    }

    fn flush_jobs(self: Rc<Self>) {
        let _span = tracing::debug_span!(
            target: trace_targets::GRAPH,
            "reactor.flush_jobs"
        )
        .entered();
        loop {
            let job = self.pending_jobs.borrow_mut().pop_front();
            let Some(job) = job else {
                break;
            };
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::GRAPH,
                event = "run_job",
                remaining_jobs = self.pending_jobs.borrow().len(),
                "running reactive scheduled job"
            );
            job();
        }

        self.flush_scheduled.set(false);
        if !self.pending_jobs.borrow().is_empty() {
            self.ensure_flush_scheduled();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;

    use runite::{queue_task, run};

    use super::{Reactor, current};

    #[test]
    fn current_reactor_is_thread_local_singleton() {
        let one = current();
        let two = current();
        assert!(Rc::ptr_eq(&one.inner, &two.inner));
    }

    #[test]
    fn observe_records_dependency_edges() {
        let reactor = Reactor::new();
        let observer = reactor.allocate_node();
        let observable = reactor.allocate_node();

        reactor.run_in_context(observer, || {
            reactor
                .try_observe(observable)
                .expect("should not detect cycle")
        });

        assert_eq!(
            reactor.inner.dependencies.borrow().get(&observer),
            Some(&[observable].into_iter().collect())
        );
        assert_eq!(
            reactor.inner.dependents.borrow().get(&observable),
            Some(&[observer].into_iter().collect())
        );
    }

    #[test]
    fn cycle_detection_panics_with_expected() {
        let reactor = Reactor::new();
        let a = reactor.allocate_node();
        let b = reactor.allocate_node();

        let panic = catch_unwind(AssertUnwindSafe(|| {
            reactor.run_in_context(a, || {
                reactor.observe(b);
                reactor.run_in_context(b, || {
                    reactor.observe(a);
                });
            });
        }))
        .expect_err("cycle should panic");

        let Some(cycle_error) = panic.downcast_ref::<String>() else {
            panic!("panic should be a string");
        };

        assert!(
            cycle_error.contains("reactive cycle detected"),
            "panic should indicate cycle detected"
        );

        assert!(
            cycle_error.contains("1 -> 2 -> 1"),
            "panic should include cycle path"
        );
    }

    #[test]
    fn scheduled_jobs_flush_on_runtime_microtask_queue() {
        let observed = Rc::new(Cell::new(0usize));

        queue_task({
            let observed = Rc::clone(&observed);
            move || {
                let reactor = Reactor::new();
                reactor.schedule({
                    let observed = Rc::clone(&observed);
                    move || observed.set(1)
                });
                assert_eq!(observed.get(), 0);
            }
        });

        run();

        assert_eq!(observed.get(), 1);
    }
}
