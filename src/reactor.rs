use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::rc::{Rc, Weak};
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::error::Error;
use core::fmt;
use core::panic::Location;

use hashbrown::{HashMap, HashSet};

use runite::queue_microtask;

use crate::{NodeId, trace_targets};

type Job = Box<dyn FnOnce() + 'static>;

thread_local! {
    static CURRENT_REACTOR: RefCell<Weak<ReactorInner>> = const { RefCell::new(Weak::new()) };
    static UNTRACKED_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[cfg(debug_assertions)]
thread_local! {
    /// Pointer identity of the reactor whose computation is currently on top of the call stack.
    /// Used to detect reads of one reactor's nodes from inside another reactor's computation.
    static RUNNING_REACTOR: Cell<*const ()> = const { Cell::new(core::ptr::null()) };
}

/// Returns the current thread's default reactor.
///
/// The first call on a thread creates a new reactor for that thread and caches it in thread-local
/// storage.
pub fn current() -> Reactor {
    Reactor::current()
}

/// Runs `f` with dependency tracking suspended.
///
/// Reads made while `f` executes do not record dependencies for the currently running observer,
/// so the observer will not re-run when those values change. Cycle detection remains active.
/// Tracking resumes when `f` returns; nested `untrack` calls are permitted.
pub fn untrack<T>(f: impl FnOnce() -> T) -> T {
    UNTRACKED_DEPTH.with(|depth| depth.set(depth.get() + 1));

    struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            UNTRACKED_DEPTH.with(|depth| depth.set(depth.get() - 1));
        }
    }

    let _guard = Guard;
    f()
}

fn is_untracked() -> bool {
    UNTRACKED_DEPTH.with(|depth| depth.get() > 0)
}

/// How stale an observer has become after an upstream write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Mark {
    /// A transitive dependency may have changed; the observer must verify its direct
    /// dependencies before recomputing.
    Check,
    /// A direct dependency definitely changed; the observer must recompute.
    Dirty,
}

/// Staleness of a computed node or effect, ordered from freshest to stalest.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum State {
    Clean,
    Check,
    Dirty,
}

impl From<Mark> for State {
    fn from(mark: Mark) -> Self {
        match mark {
            Mark::Check => State::Check,
            Mark::Dirty => State::Dirty,
        }
    }
}

pub(crate) trait ObserverHook {
    /// Records that this observer's inputs may have changed.
    fn mark(&self, mark: Mark);

    /// Brings a computed node up to date, recomputing if its inputs actually changed.
    ///
    /// The default implementation is a no-op; it is used by observers that are never read as
    /// dependencies (effects).
    fn refresh(&self) {}
}

/// Error type for cycles detected in the reactive graph. Contains the path of nodes that form the cycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactCycleError {
    cycle: Vec<NodeId>,
    origins: Vec<Option<&'static Location<'static>>>,
}

impl ReactCycleError {
    fn new(cycle: Vec<NodeId>, origins: Vec<Option<&'static Location<'static>>>) -> Self {
        Self { cycle, origins }
    }

    /// Returns the cycle path that was detected.
    pub fn cycle(&self) -> &[NodeId] {
        &self.cycle
    }

    /// Returns the source locations where the nodes in the cycle path were created, in the same
    /// order as [`cycle`](Self::cycle). An entry is `None` when the node has already been
    /// disposed.
    pub fn origins(&self) -> &[Option<&'static Location<'static>>] {
        &self.origins
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
            if let Some(origin) = self.origins.get(index).copied().flatten() {
                write!(f, " (created at {origin})")?;
            }
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
        #[cfg(debug_assertions)]
        let previous_running =
            RUNNING_REACTOR.with(|running| running.replace(Rc::as_ptr(&self.inner).cast::<()>()));

        struct Guard<'a> {
            inner: &'a ReactorInner,
            #[cfg(debug_assertions)]
            previous_running: *const (),
        }

        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                let popped = self.inner.stack.borrow_mut().pop();
                debug_assert!(popped.is_some(), "reactor observer stack underflow");
                if let Some(node) = popped {
                    let removed = self.inner.active_computations.borrow_mut().remove(&node);
                    debug_assert!(removed, "observer should have been active");
                }
                #[cfg(debug_assertions)]
                RUNNING_REACTOR.with(|running| running.set(self.previous_running));
            }
        }

        let _guard = Guard {
            inner: &self.inner,
            #[cfg(debug_assertions)]
            previous_running,
        };
        f()
    }

    /// Returns an error if reading `observable` right now would close a dependency cycle, i.e.
    /// if `observable` is currently being computed further up the call stack.
    pub(crate) fn cycle_check(&self, observable: NodeId) -> Result<(), ReactCycleError> {
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
            let origins = cycle.iter().map(|node| self.origin(*node)).collect();
            tracing::debug!(
                target: trace_targets::GRAPH,
                event = "cycle_detected",
                observable_id = observable.0,
                cycle_len = cycle.len(),
                "reactive cycle detected"
            );
            return Err(ReactCycleError::new(cycle, origins));
        }
        Ok(())
    }

    /// Panicking variant of [`cycle_check`](Self::cycle_check), used on read paths before
    /// refreshing a computed node.
    pub(crate) fn assert_no_cycle(&self, observable: NodeId) {
        if let Err(e) = self.cycle_check(observable) {
            panic!("{e}");
        }
    }

    /// Attempts to record a dependency on `observable` for the currently running observer, returning an
    /// error if doing so would create a dependency cycle.
    pub fn try_observe(&self, observable: NodeId) -> Result<(), ReactCycleError> {
        self.cycle_check(observable)?;

        // Untracked reads record nothing, and are also the sanctioned way to read one reactor's
        // nodes from inside another reactor's computation.
        if is_untracked() {
            return Ok(());
        }

        #[cfg(debug_assertions)]
        self.assert_running_reactor();

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
            .insert(observable, self.version(observable));
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
            panic!("{e}");
        }
    }

    /// Notifies dependents of `observable` that its value changed.
    ///
    /// Direct dependents are marked dirty; transitive dependents are marked so that they verify
    /// their inputs before recomputing.
    pub fn trigger(&self, observable: NodeId) {
        self.bump_version(observable);
        self.mark_dependents(observable, Mark::Dirty);
    }

    /// Marks every dependent of `observable` with `mark`.
    pub(crate) fn mark_dependents(&self, observable: NodeId, mark: Mark) {
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
            event = "mark_dependents",
            observable_id = observable.0,
            dependent_count = dependents.len(),
            ?mark,
            "marking reactive dependents"
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
                hook.mark(mark);
            } else {
                self.inner.observers.borrow_mut().remove(&dependent);
            }
        }
    }

    /// Brings `node` up to date if it is a computed node that is currently registered.
    pub(crate) fn refresh_node(&self, node: NodeId) {
        let hook = self
            .inner
            .observers
            .borrow()
            .get(&node)
            .cloned()
            .and_then(|weak| weak.upgrade());
        if let Some(hook) = hook {
            hook.refresh();
        }
    }

    /// Returns `true` if any of `observer`'s recorded dependencies has a different value than the
    /// one observed during the observer's last run.
    ///
    /// Computed dependencies are refreshed before comparison, so unchanged memos suppress
    /// downstream recomputation.
    pub(crate) fn dependencies_changed(&self, observer: NodeId) -> bool {
        for (dependency, seen_version) in self.dependencies_of(observer) {
            self.refresh_node(dependency);
            if self.version(dependency) != seen_version {
                return true;
            }
        }
        false
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
        self.inner.meta.borrow_mut().remove(&node);
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

    #[track_caller]
    pub(crate) fn allocate_node(&self) -> NodeId {
        let raw = self.inner.next_node.get();
        self.inner.next_node.set(raw.wrapping_add(1));
        let id = NodeId::new(raw);
        self.inner.meta.borrow_mut().insert(
            id,
            NodeMeta {
                version: 0,
                origin: Location::caller(),
            },
        );
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

    /// Increments the version of `node`, recording that its value changed.
    pub(crate) fn bump_version(&self, node: NodeId) {
        if let Some(meta) = self.inner.meta.borrow_mut().get_mut(&node) {
            meta.version = meta.version.wrapping_add(1);
        }
    }

    /// Returns the current version of `node`, or 0 if the node is unknown.
    pub(crate) fn version(&self, node: NodeId) -> u64 {
        self.inner
            .meta
            .borrow()
            .get(&node)
            .map(|meta| meta.version)
            .unwrap_or(0)
    }

    /// Returns the source location at which `node` was created.
    pub(crate) fn origin(&self, node: NodeId) -> Option<&'static Location<'static>> {
        self.inner.meta.borrow().get(&node).map(|meta| meta.origin)
    }

    /// Returns the dependencies recorded during `observer`'s last run, with the version of each
    /// dependency observed at that time.
    pub(crate) fn dependencies_of(&self, observer: NodeId) -> Vec<(NodeId, u64)> {
        self.inner
            .dependencies
            .borrow()
            .get(&observer)
            .map(|edges| edges.iter().map(|(id, version)| (*id, *version)).collect())
            .unwrap_or_default()
    }

    /// Returns the number of the currently running (or most recent) job flush.
    pub(crate) fn flush_epoch(&self) -> u64 {
        self.inner.flush_epoch.get()
    }

    #[cfg(debug_assertions)]
    fn assert_running_reactor(&self) {
        RUNNING_REACTOR.with(|running| {
            let running = running.get();
            if !running.is_null() && running != Rc::as_ptr(&self.inner).cast::<()>() {
                panic!(
                    "adaptite: a node belonging to one reactor was read inside a computation \
                     running in a different reactor on the same thread; this dependency cannot \
                     be tracked and the observer will not re-run when the node changes"
                );
            }
        });
    }

    fn clear_observer_dependencies(&self, observer: NodeId) {
        let observed = self
            .inner
            .dependencies
            .borrow_mut()
            .remove(&observer)
            .map(|edges| edges.into_keys().collect::<Vec<_>>())
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

struct NodeMeta {
    version: u64,
    origin: &'static Location<'static>,
}

struct ReactorInner {
    next_node: Cell<u64>,
    meta: RefCell<HashMap<NodeId, NodeMeta>>,
    dependencies: RefCell<HashMap<NodeId, HashMap<NodeId, u64>>>,
    dependents: RefCell<HashMap<NodeId, HashSet<NodeId>>>,
    observers: RefCell<HashMap<NodeId, Weak<dyn ObserverHook>>>,
    stack: RefCell<Vec<NodeId>>,
    active_computations: RefCell<HashSet<NodeId>>,
    pending_jobs: RefCell<VecDeque<Job>>,
    flush_scheduled: Cell<bool>,
    flush_epoch: Cell<u64>,
}

impl ReactorInner {
    fn new() -> Self {
        Self {
            next_node: Cell::new(1),
            meta: RefCell::new(HashMap::new()),
            dependencies: RefCell::new(HashMap::new()),
            dependents: RefCell::new(HashMap::new()),
            observers: RefCell::new(HashMap::new()),
            stack: RefCell::new(Vec::new()),
            active_computations: RefCell::new(HashSet::new()),
            pending_jobs: RefCell::new(VecDeque::new()),
            flush_scheduled: Cell::new(false),
            flush_epoch: Cell::new(0),
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
        self.flush_epoch.set(self.flush_epoch.get().wrapping_add(1));

        // If a job panics, reset the flush flag and hand any remaining jobs to a fresh flush so
        // one panicking effect cannot silently disable the reactor.
        struct FlushGuard {
            inner: Rc<ReactorInner>,
        }

        impl Drop for FlushGuard {
            fn drop(&mut self) {
                self.inner.flush_scheduled.set(false);
                if !self.inner.pending_jobs.borrow().is_empty() {
                    self.inner.ensure_flush_scheduled();
                }
            }
        }

        let guard = FlushGuard {
            inner: Rc::clone(&self),
        };

        loop {
            let job = guard.inner.pending_jobs.borrow_mut().pop_front();
            let Some(job) = job else {
                break;
            };
            #[cfg(debug_assertions)]
            tracing::trace!(
                target: trace_targets::GRAPH,
                event = "run_job",
                remaining_jobs = guard.inner.pending_jobs.borrow().len(),
                "running reactive scheduled job"
            );
            job();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;

    use runite::{queue_macrotask, run};

    use super::{Reactor, current};

    #[test]
    fn current_reactor_is_thread_local_singleton() {
        let one = current();
        let two = current();
        assert!(Rc::ptr_eq(&one.inner, &two.inner));
    }

    #[test]
    fn observe_records_dependency_edges_with_versions() {
        let reactor = Reactor::new();
        let observer = reactor.allocate_node();
        let observable = reactor.allocate_node();
        reactor.trigger(observable);

        reactor.run_in_context(observer, || {
            reactor
                .try_observe(observable)
                .expect("should not detect cycle")
        });

        assert_eq!(
            reactor.dependencies_of(observer),
            vec![(observable, reactor.version(observable))]
        );
        assert_eq!(
            reactor.inner.dependents.borrow().get(&observable),
            Some(&[observer].into_iter().collect())
        );
    }

    #[test]
    fn cycle_detection_panics_with_path_and_origins() {
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
            cycle_error.contains("1 (created at")
                && cycle_error.contains("-> 2 (created at")
                && cycle_error.contains("reactor.rs"),
            "panic should include the cycle path with node origins, got: {cycle_error}"
        );
    }

    #[test]
    fn scheduled_jobs_flush_on_runtime_microtask_queue() {
        let observed = Rc::new(Cell::new(0usize));

        queue_macrotask({
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

    #[test]
    fn graph_survives_dropping_the_reactor_handle() {
        let seen = Rc::new(std::cell::RefCell::new(Vec::new()));
        let keep_alive = Rc::new(std::cell::RefCell::new(None::<crate::EffectHandle>));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let keep_alive = Rc::clone(&keep_alive);
            move || {
                let reactor = Reactor::new();
                let source = crate::signal_in(&reactor, 1usize);
                let effect = reactor.effect({
                    let seen = Rc::clone(&seen);
                    let source = source.clone();
                    move || seen.borrow_mut().push(source.get())
                });
                *keep_alive.borrow_mut() = Some(effect);

                // Nodes hold the reactor alive; the user's handle is not load-bearing.
                drop(reactor);

                runite::queue_macrotask(move || {
                    source.set(2);
                });
            }
        });

        run();

        assert_eq!(&*seen.borrow(), &[1, 2]);
    }

    #[test]
    fn flush_recovers_after_a_panicking_job() {
        let observed = Rc::new(Cell::new(0usize));

        queue_macrotask({
            let observed = Rc::clone(&observed);
            move || {
                let reactor = Reactor::new();
                reactor.schedule(|| panic!("job panics"));
                // Swallow the panic that propagates out of the microtask flush so the test can
                // observe the reactor's recovery.
                let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
                assert!(result.is_err(), "flush should propagate the job panic");

                reactor.schedule({
                    let observed = Rc::clone(&observed);
                    move || observed.set(1)
                });
                reactor.flush_now();
            }
        });

        run();

        assert_eq!(observed.get(), 1);
    }
}
