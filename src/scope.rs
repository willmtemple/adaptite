use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};

use crate::trace_targets;

thread_local! {
    static OWNER_STACK: RefCell<Vec<Rc<OwnerFrame>>> = const { RefCell::new(Vec::new()) };
}

/// A reactive resource that an owner can dispose when it is itself disposed or re-run.
pub(crate) trait OwnedDisposable {
    fn dispose_owned(&self);
}

/// Ownership bookkeeping for a reactive owner (an effect or a scope): the children created
/// during its execution and the cleanups registered against it.
#[derive(Default)]
pub(crate) struct OwnerFrame {
    children: RefCell<Vec<Rc<dyn OwnedDisposable>>>,
    cleanups: RefCell<Vec<Box<dyn FnOnce()>>>,
    disposed: Cell<bool>,
}

impl OwnerFrame {
    pub(crate) fn new() -> Rc<Self> {
        Rc::new(Self::default())
    }

    /// Takes ownership of `child`, disposing it when this owner is reset or disposed. If the
    /// owner is already disposed, the child is disposed immediately.
    pub(crate) fn adopt(&self, child: Rc<dyn OwnedDisposable>) {
        if self.disposed.get() {
            child.dispose_owned();
            return;
        }
        self.children.borrow_mut().push(child);
    }

    /// Registers a cleanup to run before the owner next re-runs or when it is disposed. If the
    /// owner is already disposed, the cleanup runs immediately.
    pub(crate) fn add_cleanup(&self, cleanup: Box<dyn FnOnce()>) {
        if self.disposed.get() {
            cleanup();
            return;
        }
        self.cleanups.borrow_mut().push(cleanup);
    }

    /// Runs registered cleanups and disposes owned children, most recent first. Called before an
    /// effect re-runs and as part of disposal.
    pub(crate) fn reset(&self) {
        let cleanups = core::mem::take(&mut *self.cleanups.borrow_mut());
        for cleanup in cleanups.into_iter().rev() {
            cleanup();
        }
        let children = core::mem::take(&mut *self.children.borrow_mut());
        for child in children.into_iter().rev() {
            child.dispose_owned();
        }
    }

    /// Terminally disposes the owner: resets it and rejects future children.
    pub(crate) fn dispose(&self) {
        if self.disposed.replace(true) {
            return;
        }
        tracing::debug!(
            target: trace_targets::SCOPE,
            event = "dispose_owner",
            "disposing reactive owner"
        );
        self.reset();
    }

    pub(crate) fn is_disposed(&self) -> bool {
        self.disposed.get()
    }
}

/// Runs `f` with `frame` as the innermost reactive owner on this thread.
pub(crate) fn with_owner<T>(frame: &Rc<OwnerFrame>, f: impl FnOnce() -> T) -> T {
    OWNER_STACK.with(|stack| stack.borrow_mut().push(Rc::clone(frame)));

    struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            OWNER_STACK.with(|stack| {
                stack.borrow_mut().pop();
            });
        }
    }

    let _guard = Guard;
    f()
}

/// Returns the innermost reactive owner on this thread, if any.
pub(crate) fn current_owner() -> Option<Rc<OwnerFrame>> {
    OWNER_STACK.with(|stack| stack.borrow().last().cloned())
}

/// Hands `child` to the innermost owner on this thread, returning `false` when no owner is
/// active (the child stays exclusively handle-managed).
pub(crate) fn adopt_into_current(child: Rc<dyn OwnedDisposable>) -> bool {
    match current_owner() {
        Some(owner) => {
            owner.adopt(child);
            true
        }
        None => false,
    }
}

/// Registers `cleanup` against the innermost reactive owner (the currently running effect, or
/// the scope currently being constructed).
///
/// The cleanup runs before the owning effect next re-runs, and when the owner is disposed. Use
/// it to release resources acquired during an effect run.
///
/// # Panics
///
/// Panics when called outside a reactive owner, since the cleanup could never run.
pub fn on_cleanup(cleanup: impl FnOnce() + 'static) {
    let Some(owner) = current_owner() else {
        panic!(
            "adaptite: on_cleanup called outside a reactive owner (an effect run or a scope); \
             the cleanup would never execute"
        );
    };
    owner.add_cleanup(Box::new(cleanup));
}

/// Runs `f` inside a new ownership scope and returns its result along with a handle to the
/// scope.
///
/// Effects (and nested scopes) created while `f` executes are owned by the scope: they stay
/// alive without their handles being held, and they are disposed together when the scope is
/// disposed. Scopes created inside another owner are disposed with that owner.
///
/// The scope is disposed when the last clone of the handle is dropped; call
/// [`leak`](ScopeHandle::leak) to keep it alive for the remainder of the program.
pub fn scope<T>(f: impl FnOnce() -> T) -> (ScopeHandle, T) {
    let frame = OwnerFrame::new();
    tracing::debug!(
        target: trace_targets::SCOPE,
        event = "create_scope",
        "created reactive scope"
    );
    let result = with_owner(&frame, f);
    let handle = ScopeHandle {
        inner: Rc::new(ScopeInner { frame }),
    };
    let inner: Rc<dyn OwnedDisposable> = handle.inner.clone();
    let _ = adopt_into_current(inner);
    (handle, result)
}

/// Disposable handle for an ownership scope created with [`scope`].
#[derive(Clone)]
#[must_use = "scopes are disposed when dropped, so you must keep the handle or explicitly leak it"]
pub struct ScopeHandle {
    inner: Rc<ScopeInner>,
}

impl ScopeHandle {
    /// Disposes the scope immediately: runs its cleanups and disposes everything it owns.
    pub fn dispose(&self) {
        self.inner.frame.dispose();
    }

    /// Returns `true` if the scope has been disposed.
    pub fn is_disposed(&self) -> bool {
        self.inner.frame.is_disposed()
    }

    /// Consumes the handle and leaks it, keeping the scope alive for the remainder of the
    /// program. You CANNOT recover a `ScopeHandle` after calling this method.
    pub fn leak(self) {
        core::mem::forget(self);
    }
}

struct ScopeInner {
    frame: Rc<OwnerFrame>,
}

impl OwnedDisposable for ScopeInner {
    fn dispose_owned(&self) {
        self.frame.dispose();
    }
}

impl Drop for ScopeInner {
    fn drop(&mut self) {
        self.frame.dispose();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;

    use runite::{queue_macrotask, run};

    use super::{ScopeHandle, on_cleanup, scope};
    use crate::{Reactor, signal_in};

    #[test]
    fn on_cleanup_outside_an_owner_panics() {
        let result = catch_unwind(AssertUnwindSafe(|| on_cleanup(|| {})));
        let panic = result.expect_err("on_cleanup should panic outside an owner");
        let message = panic
            .downcast_ref::<&str>()
            .expect("panic payload should be a string");
        assert!(message.contains("outside a reactive owner"));
    }

    #[test]
    fn disposing_a_scope_disposes_owned_effects_and_runs_cleanups() {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let events = Rc::new(RefCell::new(Vec::<&'static str>::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            let events = Rc::clone(&events);
            move || {
                let reactor = Reactor::new();
                let source = signal_in(&reactor, 1usize);

                let (handle, source) = scope({
                    let seen = Rc::clone(&seen);
                    let events = Rc::clone(&events);
                    move || {
                        // The scope owns this effect: no handle is kept, yet it stays alive
                        // until the scope is disposed.
                        let _ = reactor.effect({
                            let seen = Rc::clone(&seen);
                            let source = source.clone();
                            move || seen.borrow_mut().push(source.get())
                        });
                        on_cleanup({
                            let events = Rc::clone(&events);
                            move || events.borrow_mut().push("scope cleanup")
                        });
                        source
                    }
                });

                runite::queue_macrotask({
                    let events = Rc::clone(&events);
                    move || {
                        source.set(2);

                        runite::queue_macrotask(move || {
                            assert!(!handle.is_disposed());
                            handle.dispose();
                            assert!(handle.is_disposed());
                            events.borrow_mut().push("disposed");

                            // Writes after disposal must not rerun the effect.
                            source.set(3);
                        });
                    }
                });
            }
        });

        run();
        assert_eq!(&*seen.borrow(), &[1, 2]);
        assert_eq!(&*events.borrow(), &["scope cleanup", "disposed"]);
    }

    #[test]
    fn effect_reruns_dispose_nested_effects_and_run_cleanups() {
        let events = Rc::new(RefCell::new(Vec::<String>::new()));
        let scope_slot = Rc::new(RefCell::new(None::<ScopeHandle>));

        queue_macrotask({
            let events = Rc::clone(&events);
            let scope_slot = Rc::clone(&scope_slot);
            move || {
                let reactor = Reactor::new();
                let outer_dep = signal_in(&reactor, 0usize);
                let inner_dep = signal_in(&reactor, 0usize);

                let (handle, ()) = scope({
                    let events = Rc::clone(&events);
                    let outer_dep = outer_dep.clone();
                    let inner_dep = inner_dep.clone();
                    let inner_reactor = reactor.clone();
                    move || {
                        let _ = reactor.effect({
                            let events = Rc::clone(&events);
                            let outer_dep = outer_dep.clone();
                            let inner_dep = inner_dep.clone();
                            move || {
                                let generation = outer_dep.get();
                                events.borrow_mut().push(format!("outer {generation}"));
                                on_cleanup({
                                    let events = Rc::clone(&events);
                                    move || {
                                        events.borrow_mut().push(format!("cleanup {generation}"))
                                    }
                                });
                                // A nested effect, re-created on every outer run. The previous
                                // generation must be disposed before this one is created.
                                let _ = crate::effect_in(&inner_reactor, {
                                    let events = Rc::clone(&events);
                                    let inner_dep = inner_dep.clone();
                                    move || {
                                        events.borrow_mut().push(format!(
                                            "inner {generation}: {}",
                                            inner_dep.get()
                                        ));
                                    }
                                });
                            }
                        });
                    }
                });
                *scope_slot.borrow_mut() = Some(handle);

                runite::queue_macrotask({
                    let inner_dep = inner_dep.clone();
                    move || {
                        inner_dep.set(1);

                        runite::queue_macrotask({
                            let outer_dep = outer_dep.clone();
                            move || {
                                outer_dep.set(1);

                                runite::queue_macrotask(move || {
                                    // Only the generation-1 inner effect may respond now.
                                    inner_dep.set(2);
                                });
                            }
                        });
                    }
                });
            }
        });

        run();
        assert_eq!(
            &*events.borrow(),
            &[
                "outer 0",
                "inner 0: 0",
                "inner 0: 1",
                "cleanup 0",
                "outer 1",
                "inner 1: 1",
                "inner 1: 2",
            ]
        );
    }

    #[test]
    fn nested_scopes_are_disposed_with_their_parent() {
        let (parent, child) = scope(|| {
            let (child, ()) = scope(|| {});
            child
        });

        assert!(!child.is_disposed());
        parent.dispose();
        assert!(
            child.is_disposed(),
            "disposing the parent scope must dispose nested scopes"
        );
    }
}
