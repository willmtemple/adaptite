use alloc::boxed::Box;
use alloc::rc::{Rc, Weak};
use alloc::vec::Vec;
use core::any::Any;
use core::cell::{Cell, RefCell};
use core::panic::Location;

use crate::{NodeId, trace_targets};

thread_local! {
    /// `None` entries are barriers pushed by [`unowned`]: they shadow any enclosing owner
    /// without establishing a new one.
    static OWNER_STACK: RefCell<Vec<Option<Rc<OwnerFrame>>>> = const { RefCell::new(Vec::new()) };
}

/// Handler installed by [`scope_catch`], invoked for panics from the effects a scope owns.
type ErrorHandler = Rc<dyn Fn(ErrorInfo)>;

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
    /// The owner that was innermost when this frame was created.
    ///
    /// The thread-local owner stack describes the *dynamic* chain, which is empty by the time a
    /// scheduled effect actually runs. An error boundary has to be found from the effect's
    /// *lexical* ownership instead, so the link is recorded at construction. Weak, because the
    /// parent owns the child.
    parent: RefCell<Option<Weak<OwnerFrame>>>,
    error_handler: RefCell<Option<ErrorHandler>>,
}

impl OwnerFrame {
    pub(crate) fn new() -> Rc<Self> {
        let frame = Rc::new(Self::default());
        *frame.parent.borrow_mut() = current_owner().as_ref().map(Rc::downgrade);
        frame
    }

    /// Returns the nearest error handler at or above this frame.
    fn error_handler(self: &Rc<Self>) -> Option<ErrorHandler> {
        let mut frame = Rc::clone(self);
        loop {
            if let Some(handler) = frame.error_handler.borrow().as_ref() {
                return Some(Rc::clone(handler));
            }
            let parent = frame.parent.borrow().as_ref().and_then(Weak::upgrade);
            frame = parent?;
        }
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
    ///
    /// Cleanups and disposals run untracked (their reads must not become dependencies of
    /// whatever observer triggered the reset), and a panicking cleanup does not strand its
    /// siblings: the remaining teardown still runs during unwinding.
    pub(crate) fn reset(&self) {
        struct RunRemaining(Vec<Box<dyn FnOnce()>>);

        impl Drop for RunRemaining {
            fn drop(&mut self) {
                while let Some(cleanup) = self.0.pop() {
                    cleanup();
                }
            }
        }

        struct DisposeRemaining(Vec<Rc<dyn OwnedDisposable>>);

        impl Drop for DisposeRemaining {
            fn drop(&mut self) {
                while let Some(child) = self.0.pop() {
                    child.dispose_owned();
                }
            }
        }

        crate::untrack(|| {
            drop(RunRemaining(core::mem::take(
                &mut *self.cleanups.borrow_mut(),
            )));
            drop(DisposeRemaining(core::mem::take(
                &mut *self.children.borrow_mut(),
            )));
        });
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

impl Drop for OwnerFrame {
    fn drop(&mut self) {
        // Covers the case where the closure passed to `scope` panics before a handle exists:
        // registered cleanups still run when the frame unwinds.
        self.dispose();
    }
}

/// Runs `f` with `frame` as the innermost reactive owner on this thread.
pub(crate) fn with_owner<T>(frame: &Rc<OwnerFrame>, f: impl FnOnce() -> T) -> T {
    with_owner_entry(Some(Rc::clone(frame)), f)
}

fn with_owner_entry<T>(entry: Option<Rc<OwnerFrame>>, f: impl FnOnce() -> T) -> T {
    OWNER_STACK.with(|stack| stack.borrow_mut().push(entry));

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
    OWNER_STACK.with(|stack| stack.borrow().last().cloned().flatten())
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

/// A non-owning handle to a reactive owner (an effect or scope), used to re-enter it later.
///
/// Ownership is established by *where code runs*: an effect or subscription is adopted by the
/// owner active at its creation. Async work breaks that link — after an `.await`, the original
/// owner is no longer on the stack, so anything created there would be unowned and would
/// outlive the screen or component that spawned it. Capture an `Owner` before suspending and
/// use [`run_in`](Owner::run_in) to re-attach later work:
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{effect, owner, scope, signal};
/// use runite::{queue_macrotask, run, yield_now};
///
/// let seen = Rc::new(RefCell::new(Vec::new()));
///
/// queue_macrotask({
///     let seen = Rc::clone(&seen);
///     move || {
///         let data = signal(0);
///         let (handle, ()) = scope({
///             let data = data.clone();
///             let seen = Rc::clone(&seen);
///             move || {
///                 // Capture the owner before suspending...
///                 let scope_owner = owner().expect("scope is the current owner");
///                 std::mem::drop(runite::spawn(async move {
///                     yield_now().await; // ... await a fetch ...
///                     // ...and re-enter it afterwards: the effect is owned again.
///                     scope_owner.run_in(|| {
///                         let _ = effect({
///                             let data = data.clone();
///                             let seen = Rc::clone(&seen);
///                             move || seen.borrow_mut().push(data.get())
///                         });
///                     });
///                 }));
///             }
///         });
///
///         runite::queue_macrotask({
///             let data = data.clone();
///             move || {
///                 handle.dispose();
///                 data.set(1); // disposed with the scope: not observed
///             }
///         });
///     }
/// });
/// run();
///
/// assert_eq!(*seen.borrow(), [0]);
/// ```
///
/// Unlike [`ScopeHandle`], dropping an `Owner` does **not** dispose anything: it is a
/// re-entry token, not a lifetime manager. If the owner has already been disposed when
/// [`run_in`](Owner::run_in) executes, children created inside are disposed immediately and
/// cleanups run immediately.
#[derive(Clone)]
pub struct Owner {
    frame: Rc<OwnerFrame>,
}

impl Owner {
    /// Runs `f` with this owner as the innermost reactive owner on the current thread.
    pub fn run_in<T>(&self, f: impl FnOnce() -> T) -> T {
        with_owner(&self.frame, f)
    }

    /// Returns `true` if the underlying owner has been disposed.
    pub fn is_disposed(&self) -> bool {
        self.frame.is_disposed()
    }
}

impl core::fmt::Debug for Owner {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Owner")
            .field("disposed", &self.frame.is_disposed())
            .finish()
    }
}

/// Captures the innermost reactive owner (the currently running effect, or the scope currently
/// being constructed) for later re-entry, or `None` when no owner is active.
///
/// See [`Owner`] for the async re-attachment pattern this enables.
pub fn owner() -> Option<Owner> {
    current_owner().map(|frame| Owner { frame })
}

/// Runs `f` with no current reactive owner.
///
/// Effects, scopes, and subscriptions created inside are unowned even when `unowned` is called
/// from within an effect run or a scope: they are kept alive by their handles and disposed when
/// the last handle drops (or kept for the remainder of the program via `leak`). Use it to start
/// background work from inside a component or effect without tying its lifetime to the
/// enclosing owner.
///
/// The suppression applies only at the boundary: an owner established *inside* `f` (a scope, or
/// an effect's own runs) works normally. [`owner`] returns `None` directly inside `f`, and
/// [`on_cleanup`] there panics, since no owner could ever run the cleanup.
///
/// # Examples
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{effect, scope, signal, unowned};
/// use runite::{queue_macrotask, run};
///
/// let seen = Rc::new(RefCell::new(Vec::new()));
///
/// queue_macrotask({
///     let seen = Rc::clone(&seen);
///     move || {
///         let data = signal(0);
///         let (handle, background) = scope({
///             let data = data.clone();
///             let seen = Rc::clone(&seen);
///             move || {
///                 // Created inside the scope, but deliberately not owned by it.
///                 unowned(|| {
///                     effect({
///                         let data = data.clone();
///                         let seen = Rc::clone(&seen);
///                         move || seen.borrow_mut().push(data.get())
///                     })
///                 })
///             }
///         });
///
///         runite::queue_macrotask({
///             let data = data.clone();
///             move || {
///                 handle.dispose();
///                 data.set(1); // the scope is gone, but the unowned effect still reacts
///
///                 runite::queue_macrotask(move || {
///                     drop(background); // last handle dropped: now it is disposed
///                     data.set(2); // not observed
///                 });
///             }
///         });
///     }
/// });
/// run();
///
/// assert_eq!(*seen.borrow(), [0, 1]);
/// ```
pub fn unowned<T>(f: impl FnOnce() -> T) -> T {
    with_owner_entry(None, f)
}

/// Registers `cleanup` against the innermost reactive owner (the currently running effect, or
/// the scope currently being constructed).
///
/// The cleanup runs before the owning effect next re-runs, and when the owner is disposed. Use
/// it to release resources acquired during an effect run.
///
/// Cleanups run untracked (their reads do not become dependencies of whatever triggered the
/// teardown) and in reverse registration order: the most recently registered cleanup runs
/// first, before any children of the owner are disposed.
///
/// # Panics
///
/// Panics when called outside a reactive owner, since the cleanup could never run.
///
/// # Examples
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{on_cleanup, scope};
///
/// let log = Rc::new(RefCell::new(Vec::new()));
///
/// let (handle, ()) = scope({
///     let log = Rc::clone(&log);
///     move || {
///         log.borrow_mut().push("setup");
///         on_cleanup({
///             let log = Rc::clone(&log);
///             move || log.borrow_mut().push("teardown")
///         });
///     }
/// });
///
/// assert_eq!(*log.borrow(), ["setup"]);
/// handle.dispose();
/// assert_eq!(*log.borrow(), ["setup", "teardown"]);
/// ```
///
/// Cleanups pair naturally with effect re-runs for subscribe/unsubscribe patterns:
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{effect, on_cleanup, signal};
/// use runite::{queue_macrotask, run};
///
/// let log = Rc::new(RefCell::new(Vec::new()));
///
/// queue_macrotask({
///     let log = Rc::clone(&log);
///     move || {
///         let channel = signal("news");
///         effect({
///             let channel = channel.clone();
///             let log = Rc::clone(&log);
///             move || {
///                 let name = channel.get();
///                 log.borrow_mut().push(format!("subscribe {name}"));
///                 on_cleanup({
///                     let log = Rc::clone(&log);
///                     move || log.borrow_mut().push(format!("unsubscribe {name}"))
///                 });
///             }
///         })
///         .leak();
///
///         runite::queue_macrotask(move || {
///             channel.set("sports");
///         });
///     }
/// });
/// run();
///
/// assert_eq!(
///     *log.borrow(),
///     ["subscribe news", "unsubscribe news", "subscribe sports"]
/// );
/// ```
pub fn on_cleanup(cleanup: impl FnOnce() + 'static) {
    let Some(owner) = current_owner() else {
        panic!(
            "adaptite: on_cleanup called outside a reactive owner (an effect run or a scope); \
             the cleanup would never execute"
        );
    };
    owner.add_cleanup(Box::new(cleanup));
}

/// A panic caught by an error boundary, delivered to the handler registered with [`scope_catch`].
pub struct ErrorInfo {
    payload: Box<dyn Any + Send>,
    node: NodeId,
    origin: Option<&'static Location<'static>>,
}

impl ErrorInfo {
    /// Returns the panic payload.
    pub fn payload(&self) -> &(dyn Any + Send) {
        &*self.payload
    }

    /// Consumes this value and returns the panic payload, for re-raising with
    /// [`std::panic::resume_unwind`].
    pub fn into_payload(self) -> Box<dyn Any + Send> {
        self.payload
    }

    /// Returns the panic message when the payload is a string, which covers `panic!` with a
    /// format string and every panic the standard library raises.
    pub fn message(&self) -> Option<&str> {
        if let Some(message) = self.payload.downcast_ref::<&'static str>() {
            return Some(message);
        }
        self.payload
            .downcast_ref::<alloc::string::String>()
            .map(alloc::string::String::as_str)
    }

    /// Returns the node id of the effect that panicked.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// Returns the source location where the failing effect was created, which is usually more
    /// useful than the panic site for finding the buggy subtree.
    pub fn origin(&self) -> Option<&'static Location<'static>> {
        self.origin
    }
}

impl core::fmt::Debug for ErrorInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ErrorInfo")
            .field("node", &self.node)
            .field("message", &self.message())
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

impl core::fmt::Display for ErrorInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "effect {} panicked", self.node.get())?;
        if let Some(origin) = self.origin {
            write!(f, " (created at {origin})")?;
        }
        if let Some(message) = self.message() {
            write!(f, ": {message}")?;
        }
        Ok(())
    }
}

pub(crate) fn build_error_info(
    payload: Box<dyn Any + Send>,
    node: NodeId,
    origin: Option<&'static Location<'static>>,
) -> ErrorInfo {
    ErrorInfo {
        payload,
        node,
        origin,
    }
}

/// Finds the error handler governing `frame`, if any.
pub(crate) fn error_handler_for(frame: &Rc<OwnerFrame>) -> Option<ErrorHandler> {
    frame.error_handler()
}

/// Creates an ownership scope that catches panics from the effects it owns.
///
/// One buggy widget's panicking effect otherwise unwinds out of the whole flush. A boundary
/// confines the blast radius: a panic from any effect owned by this scope — at any depth — is
/// delivered to `on_error` instead of propagating, and the rest of the application keeps running.
///
/// # What happens on a panic
///
/// The panicking effect is **disposed** before `on_error` runs: its cleanups run, the children it
/// owns are torn down, and it is unhooked from the graph. This is deliberate. An effect that
/// panicked has already had its dependency tracking cut short, and leaving it live would re-run
/// and re-panic on the next invalidation — or immediately, since a panic during dependency
/// verification re-queues the effect. Disposing it makes the failure terminal for that effect and
/// leaves the handler to decide what replaces it, which is the same shape as Solid's
/// `ErrorBoundary`. The rest of the scope survives.
///
/// The nearest enclosing handler wins; boundaries nest. With no handler anywhere above the
/// effect, the panic propagates exactly as it does today.
///
/// # Caveats
///
/// - This catches *panics*, which are bugs. Recoverable failures — a fetch that 404s, a parse that
///   fails — belong in the graph as `Result` values so that downstream nodes can react to them;
///   an error boundary is not a substitute for that.
/// - Under `panic = "abort"` there is nothing to catch and the process aborts. The boundary
///   compiles and is simply never invoked.
/// - The handler runs on the reactor's thread, inside the flush that ran the failing effect. It
///   should tear down or replace the subtree, not do lengthy work.
/// - A panic raised by `f` itself — the scope's own body, run synchronously — is *not* caught.
///   Only effects owned by the scope are covered.
/// - Coverage follows ownership, so an effect created inside [`unowned`] is outside every
///   boundary above it and its panics propagate as usual. That is the same opt-out `unowned`
///   already applies to disposal; take an [`Owner`] and [`run_in`](Owner::run_in) to keep async
///   work under the boundary that created it.
///
/// # Examples
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{Reactor, scope_catch, signal_in};
///
/// let reactor = Reactor::new();
/// let errors = Rc::new(RefCell::new(Vec::new()));
/// let healthy_runs = Rc::new(RefCell::new(0));
///
/// let value = signal_in(&reactor, 1);
/// let (boundary, ()) = scope_catch(
///     {
///         let reactor = reactor.clone();
///         let value = value.clone();
///         let healthy_runs = Rc::clone(&healthy_runs);
///         move || {
///             // A buggy widget...
///             reactor.effect({
///                 let value = value.clone();
///                 move || {
///                     if value.get() == 2 {
///                         panic!("widget exploded");
///                     }
///                 }
///             }).leak();
///
///             // ...and a healthy sibling that must survive it.
///             reactor.effect({
///                 let value = value.clone();
///                 let healthy_runs = Rc::clone(&healthy_runs);
///                 move || {
///                     value.get();
///                     *healthy_runs.borrow_mut() += 1;
///                 }
///             }).leak();
///         }
///     },
///     {
///         let errors = Rc::clone(&errors);
///         move |error: adaptite::ErrorInfo| {
///             errors.borrow_mut().push(error.message().unwrap_or("?").to_string());
///         }
///     },
/// );
///
/// reactor.flush_now();
/// assert!(errors.borrow().is_empty());
/// assert_eq!(*healthy_runs.borrow(), 1);
///
/// // The panic is contained: reported, not propagated.
/// value.set(2);
/// reactor.flush_now();
/// assert_eq!(*errors.borrow(), ["widget exploded"]);
/// assert_eq!(*healthy_runs.borrow(), 2, "the sibling still ran");
///
/// // And the application keeps working afterwards.
/// value.set(3);
/// reactor.flush_now();
/// assert_eq!(*healthy_runs.borrow(), 3);
/// # boundary.dispose();
/// ```
pub fn scope_catch<T>(
    f: impl FnOnce() -> T,
    on_error: impl Fn(ErrorInfo) + 'static,
) -> (ScopeHandle, T) {
    let frame = OwnerFrame::new();
    *frame.error_handler.borrow_mut() = Some(Rc::new(on_error));
    tracing::debug!(
        target: trace_targets::SCOPE,
        event = "create_scope",
        catching = true,
        "created reactive scope with an error boundary"
    );
    let result = with_owner(&frame, f);
    let handle = ScopeHandle {
        inner: Rc::new(ScopeInner { frame }),
    };
    let inner: Rc<dyn OwnedDisposable> = handle.inner.clone();
    let _ = adopt_into_current(inner);
    (handle, result)
}

/// Runs `f` inside a new ownership scope and returns its result along with a handle to the
/// scope.
///
/// Effects (and nested scopes) created while `f` executes are owned by the scope: they stay
/// alive without their handles being held, and they are disposed together when the scope is
/// disposed.
///
/// A scope created inside another owner (an effect's run, or an enclosing scope) is kept alive
/// by that owner — even if every [`ScopeHandle`] clone is dropped — and is disposed with it. An
/// unowned scope is disposed when the last clone of its handle is dropped; call
/// [`leak`](ScopeHandle::leak) to keep it alive for the remainder of the program.
///
/// # Examples
///
/// ```rust
/// use std::cell::RefCell;
/// use std::rc::Rc;
///
/// use adaptite::{on_cleanup, scope};
///
/// let torn_down = Rc::new(RefCell::new(false));
///
/// let (handle, greeting) = scope({
///     let torn_down = Rc::clone(&torn_down);
///     move || {
///         // Effects created here would be owned by the scope and disposed with it.
///         on_cleanup(move || *torn_down.borrow_mut() = true);
///         "hello"
///     }
/// });
///
/// assert_eq!(greeting, "hello");
/// assert!(!handle.is_disposed());
///
/// handle.dispose();
/// assert!(*torn_down.borrow());
/// ```
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
///
/// Clones share the same scope. An unowned scope is disposed when the last clone is dropped; a
/// scope created inside another owner is kept alive by that owner regardless of handles and is
/// disposed with it. [`dispose`](ScopeHandle::dispose) tears the scope down immediately;
/// [`leak`](ScopeHandle::leak) keeps an unowned scope alive for the remainder of the program.
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

    /// Returns a non-owning [`Owner`] for this scope, for re-entering it later (for example
    /// from async work). Dropping the returned owner does not dispose the scope.
    pub fn owner(&self) -> Owner {
        Owner {
            frame: Rc::clone(&self.inner.frame),
        }
    }
}

impl core::fmt::Debug for ScopeHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ScopeHandle")
            .field("disposed", &self.inner.frame.is_disposed())
            .finish()
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

    use super::{ErrorInfo, ScopeHandle, on_cleanup, scope, scope_catch, unowned};
    use crate::{Reactor, signal_in};

    /// Collects the messages a boundary reports.
    fn error_log() -> (Rc<RefCell<Vec<String>>>, impl Fn(ErrorInfo) + 'static) {
        let log = Rc::new(RefCell::new(Vec::new()));
        let handler = {
            let log = Rc::clone(&log);
            move |error: ErrorInfo| {
                log.borrow_mut()
                    .push(error.message().unwrap_or("<opaque>").to_string());
            }
        };
        (log, handler)
    }

    #[test]
    fn a_boundary_contains_a_panicking_effect_and_spares_its_siblings() {
        let reactor = Reactor::new();
        let (errors, handler) = error_log();
        let value = signal_in(&reactor, 1);
        let sibling_runs = Rc::new(RefCell::new(0));

        let (boundary, ()) = scope_catch(
            {
                let reactor = reactor.clone();
                let value = value.clone();
                let sibling_runs = Rc::clone(&sibling_runs);
                move || {
                    reactor
                        .effect({
                            let value = value.clone();
                            move || {
                                if value.get() == 2 {
                                    panic!("boom");
                                }
                            }
                        })
                        .leak();
                    reactor
                        .effect({
                            let value = value.clone();
                            let sibling_runs = Rc::clone(&sibling_runs);
                            move || {
                                value.get();
                                *sibling_runs.borrow_mut() += 1;
                            }
                        })
                        .leak();
                }
            },
            handler,
        );

        reactor.flush_now();
        assert!(errors.borrow().is_empty());
        assert_eq!(*sibling_runs.borrow(), 1);

        value.set(2);
        reactor.flush_now();
        assert_eq!(*errors.borrow(), ["boom"]);
        assert_eq!(
            *sibling_runs.borrow(),
            2,
            "the sibling ran despite the panic"
        );

        // The graph keeps working, and the failed effect stays down rather than re-panicking.
        value.set(3);
        reactor.flush_now();
        assert_eq!(*sibling_runs.borrow(), 3);
        assert_eq!(
            *errors.borrow(),
            ["boom"],
            "reported once, not on every run"
        );

        boundary.dispose();
    }

    #[test]
    fn the_panicking_effect_is_disposed_and_its_cleanups_run() {
        let reactor = Reactor::new();
        let (errors, handler) = error_log();
        let value = signal_in(&reactor, 1);
        let torn_down = Rc::new(RefCell::new(false));

        let (boundary, ()) = scope_catch(
            {
                let reactor = reactor.clone();
                let value = value.clone();
                let torn_down = Rc::clone(&torn_down);
                move || {
                    reactor
                        .effect({
                            let value = value.clone();
                            let torn_down = Rc::clone(&torn_down);
                            move || {
                                on_cleanup({
                                    let torn_down = Rc::clone(&torn_down);
                                    move || *torn_down.borrow_mut() = true
                                });
                                if value.get() == 2 {
                                    panic!("boom");
                                }
                            }
                        })
                        .leak();
                }
            },
            handler,
        );

        reactor.flush_now();
        assert!(!*torn_down.borrow());

        value.set(2);
        reactor.flush_now();
        assert_eq!(*errors.borrow(), ["boom"]);
        assert!(
            *torn_down.borrow(),
            "disposal must run the failed effect's cleanups"
        );

        boundary.dispose();
    }

    #[test]
    fn without_a_boundary_a_panic_still_propagates() {
        let reactor = Reactor::new();
        let value = signal_in(&reactor, 1);

        let effect = reactor.effect({
            let value = value.clone();
            move || {
                if value.get() == 2 {
                    panic!("boom");
                }
            }
        });
        reactor.flush_now();

        value.set(2);
        let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
        assert!(result.is_err(), "an uncaught panic must not be swallowed");

        effect.dispose();
    }

    #[test]
    fn the_nearest_boundary_wins_and_boundaries_nest() {
        let reactor = Reactor::new();
        let (outer_errors, outer_handler) = error_log();
        let (inner_errors, inner_handler) = error_log();
        let value = signal_in(&reactor, 1);

        let (boundary, ()) = scope_catch(
            {
                let reactor = reactor.clone();
                let value = value.clone();
                move || {
                    // An effect governed by the outer boundary only.
                    reactor
                        .effect({
                            let value = value.clone();
                            move || {
                                if value.get() == 3 {
                                    panic!("outer child");
                                }
                            }
                        })
                        .leak();

                    let (inner, ()) = scope_catch(
                        {
                            let reactor = reactor.clone();
                            let value = value.clone();
                            move || {
                                reactor
                                    .effect({
                                        let value = value.clone();
                                        move || {
                                            if value.get() == 2 {
                                                panic!("inner child");
                                            }
                                        }
                                    })
                                    .leak();
                            }
                        },
                        inner_handler,
                    );
                    inner.leak();
                }
            },
            outer_handler,
        );

        reactor.flush_now();

        value.set(2);
        reactor.flush_now();
        assert_eq!(*inner_errors.borrow(), ["inner child"]);
        assert!(
            outer_errors.borrow().is_empty(),
            "the inner boundary caught it"
        );

        value.set(3);
        reactor.flush_now();
        assert_eq!(*outer_errors.borrow(), ["outer child"]);
        assert_eq!(*inner_errors.borrow(), ["inner child"], "unchanged");

        boundary.dispose();
    }

    #[test]
    fn a_boundary_catches_effects_nested_below_it() {
        // The handler is found through the *lexical* owner chain, which must survive the effect
        // being run later from an empty owner stack.
        let reactor = Reactor::new();
        let (errors, handler) = error_log();
        let value = signal_in(&reactor, 1);

        let (boundary, ()) = scope_catch(
            {
                let reactor = reactor.clone();
                let value = value.clone();
                move || {
                    let (nested, ()) = scope({
                        let reactor = reactor.clone();
                        let value = value.clone();
                        move || {
                            reactor
                                .effect({
                                    let value = value.clone();
                                    move || {
                                        if value.get() == 2 {
                                            panic!("deep");
                                        }
                                    }
                                })
                                .leak();
                        }
                    });
                    nested.leak();
                }
            },
            handler,
        );

        reactor.flush_now();
        value.set(2);
        reactor.flush_now();
        assert_eq!(
            *errors.borrow(),
            ["deep"],
            "a plain scope between the effect and the boundary is transparent"
        );

        boundary.dispose();
    }

    #[test]
    fn error_info_reports_the_effects_creation_site() {
        let reactor = Reactor::new();
        type Captured = Rc<RefCell<Option<(u64, Option<String>, Option<u32>)>>>;
        let captured: Captured = Rc::new(RefCell::new(None));
        let value = signal_in(&reactor, 1);

        let (boundary, creation_line) = scope_catch(
            {
                let reactor = reactor.clone();
                let value = value.clone();
                move || {
                    // Hoisted so the creation call below is a single short statement: it keeps
                    // the panic site on a different line from the `#[track_caller]` call site,
                    // which is the distinction this test exists to check.
                    let body = move || {
                        if value.get() == 2 {
                            panic!("located");
                        }
                    };
                    let creation_line = line!() + 1;
                    reactor.effect(body).leak();
                    creation_line
                }
            },
            {
                let captured = Rc::clone(&captured);
                move |error: ErrorInfo| {
                    *captured.borrow_mut() = Some((
                        error.node().get(),
                        // `Location::file` uses the host's path separator, so compare the file
                        // name rather than a hardcoded path.
                        error.origin().and_then(|origin| {
                            origin
                                .file()
                                .rsplit(['/', '\\'])
                                .next()
                                .map(alloc::string::ToString::to_string)
                        }),
                        error.origin().map(core::panic::Location::line),
                    ));
                }
            },
        );

        reactor.flush_now();
        value.set(2);
        reactor.flush_now();

        let captured = captured.borrow();
        let (node, file, line) = captured.as_ref().expect("the handler ran");
        assert!(*node > 0);
        assert_eq!(file.as_deref(), Some("scope.rs"));
        assert_eq!(
            *line,
            Some(creation_line),
            "the origin points at the effect's creation site, not where it panicked"
        );

        boundary.dispose();
    }

    #[test]
    fn a_panic_during_dependency_verification_reaches_the_boundary() {
        // Verification executes upstream computations, so it is a distinct failure path from the
        // effect body — and one an unguarded effect recovers from by re-queueing, which would
        // spin forever behind a boundary if disposal did not stop it.
        let reactor = Reactor::new();
        let (errors, handler) = error_log();
        let value = signal_in(&reactor, 1);

        let upstream = reactor.memo({
            let value = value.clone();
            move || {
                if value.get() == 2 {
                    panic!("upstream exploded");
                }
                value.get()
            }
        });

        let (boundary, ()) = scope_catch(
            {
                let reactor = reactor.clone();
                let upstream = upstream.clone();
                move || {
                    reactor
                        .effect(move || {
                            upstream.get();
                        })
                        .leak();
                }
            },
            handler,
        );

        reactor.flush_now();
        assert!(errors.borrow().is_empty());

        value.set(2);
        reactor.flush_now();
        assert_eq!(*errors.borrow(), ["upstream exploded"]);

        // Crucially, it does not spin: the effect was disposed rather than re-queued.
        reactor.flush_now();
        assert_eq!(*errors.borrow(), ["upstream exploded"]);

        boundary.dispose();
    }

    #[test]
    fn an_unowned_effect_is_outside_every_boundary() {
        // Coverage follows ownership, and `unowned` opts out of it. Pinned because the escape is
        // surprising enough to be worth a deliberate decision rather than an accident.
        let reactor = Reactor::new();
        let (errors, handler) = error_log();
        let value = signal_in(&reactor, 1);

        let (boundary, effect) = scope_catch(
            {
                let reactor = reactor.clone();
                let value = value.clone();
                move || {
                    unowned(|| {
                        reactor.effect(move || {
                            if value.get() == 2 {
                                panic!("escaped");
                            }
                        })
                    })
                }
            },
            handler,
        );
        reactor.flush_now();

        value.set(2);
        let result = catch_unwind(AssertUnwindSafe(|| reactor.flush_now()));
        assert!(result.is_err(), "the panic propagates past the boundary");
        assert!(errors.borrow().is_empty(), "the handler was not consulted");

        effect.dispose();
        boundary.dispose();
    }

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
    fn a_panicking_scope_closure_still_runs_registered_cleanups() {
        use std::cell::Cell;

        let ran = Rc::new(Cell::new(false));

        let result = catch_unwind(AssertUnwindSafe({
            let ran = Rc::clone(&ran);
            move || {
                let _ = scope(move || {
                    on_cleanup(move || ran.set(true));
                    panic!("scope body panics");
                });
            }
        }));

        assert!(result.is_err());
        assert!(
            ran.get(),
            "cleanups registered before the panic must still run"
        );
    }

    #[test]
    fn problem_effects_created_after_an_await_escape_their_scope() {
        // Validates the problem Owner exists to solve: ownership is established by the owner
        // stack at creation time, so an effect created in a spawned task -- after the scope
        // closure has returned -- is NOT owned by the scope, and disposing the scope does not
        // stop it. (See `owner_run_in_reattaches_async_work_to_its_scope` for the fix.)
        let seen = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            move || {
                let reactor = Reactor::new();
                let data = signal_in(&reactor, 0usize);

                let (handle, ()) = scope({
                    let reactor = reactor.clone();
                    let data = data.clone();
                    let seen = Rc::clone(&seen);
                    move || {
                        std::mem::drop(runite::spawn(async move {
                            runite::yield_now().await;
                            // The scope closure returned long ago: this effect is unowned.
                            crate::effect_in(&reactor, {
                                let data = data.clone();
                                let seen = Rc::clone(&seen);
                                move || seen.borrow_mut().push(data.get())
                            })
                            .leak();
                        }));
                    }
                });

                runite::queue_macrotask({
                    let data = data.clone();
                    move || {
                        handle.dispose();
                        // The scope is gone, but the escaped effect still reacts.
                        data.set(1);
                    }
                });
            }
        });

        run();
        assert_eq!(
            &*seen.borrow(),
            &[0, 1],
            "without owner capture, the effect escapes its scope (this documents the problem)"
        );
    }

    #[test]
    fn owner_run_in_reattaches_async_work_to_its_scope() {
        let seen = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            move || {
                let reactor = Reactor::new();
                let data = signal_in(&reactor, 0usize);

                let (handle, ()) = scope({
                    let reactor = reactor.clone();
                    let data = data.clone();
                    let seen = Rc::clone(&seen);
                    move || {
                        // Capture the owner before suspending...
                        let scope_owner = super::owner().expect("scope should be the owner");
                        std::mem::drop(runite::spawn(async move {
                            runite::yield_now().await;
                            // ...and re-enter it after the await: the effect is owned again.
                            scope_owner.run_in(|| {
                                let _ = crate::effect_in(&reactor, {
                                    let data = data.clone();
                                    let seen = Rc::clone(&seen);
                                    move || seen.borrow_mut().push(data.get())
                                });
                            });
                        }));
                    }
                });

                runite::queue_macrotask({
                    let data = data.clone();
                    move || {
                        handle.dispose();
                        // Disposed with the scope: this write must not be observed.
                        data.set(1);
                    }
                });
            }
        });

        run();
        assert_eq!(
            &*seen.borrow(),
            &[0],
            "the re-attached effect must be disposed with its scope"
        );
    }

    #[test]
    fn run_in_on_a_disposed_owner_disposes_children_immediately() {
        let seen = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            move || {
                let reactor = Reactor::new();
                let data = signal_in(&reactor, 0usize);

                let (handle, ()) = scope(|| {});
                let scope_owner = handle.owner();
                handle.dispose();
                assert!(scope_owner.is_disposed());

                scope_owner.run_in(|| {
                    let _ = crate::effect_in(&reactor, {
                        let data = data.clone();
                        let seen = Rc::clone(&seen);
                        move || seen.borrow_mut().push(data.get())
                    });
                });
                reactor.flush_now();
            }
        });

        run();
        assert!(
            seen.borrow().is_empty(),
            "children created under a disposed owner must never run"
        );
    }

    #[test]
    fn unowned_suppresses_the_enclosing_owner_at_the_boundary_only() {
        let (_handle, ()) = scope(|| {
            assert!(super::owner().is_some(), "the scope is the current owner");
            super::unowned(|| {
                assert!(
                    super::owner().is_none(),
                    "no owner is current inside unowned"
                );
                // An owner established inside `unowned` works normally.
                let (_inner, ()) = scope(|| {
                    assert!(
                        super::owner().is_some(),
                        "a scope created inside unowned is the current owner"
                    );
                });
            });
            assert!(
                super::owner().is_some(),
                "the enclosing owner is restored after unowned returns"
            );
        });
    }

    #[test]
    fn unowned_effects_escape_their_scope_and_die_with_their_handle() {
        let seen = Rc::new(RefCell::new(Vec::new()));

        queue_macrotask({
            let seen = Rc::clone(&seen);
            move || {
                let reactor = Reactor::new();
                let data = signal_in(&reactor, 0usize);

                let (handle, background) = scope({
                    let reactor = reactor.clone();
                    let data = data.clone();
                    let seen = Rc::clone(&seen);
                    move || {
                        super::unowned(|| {
                            crate::effect_in(&reactor, {
                                let data = data.clone();
                                let seen = Rc::clone(&seen);
                                move || seen.borrow_mut().push(data.get())
                            })
                        })
                    }
                });

                runite::queue_macrotask({
                    let data = data.clone();
                    move || {
                        handle.dispose();
                        // The scope is gone, but the unowned effect still reacts.
                        data.set(1);

                        runite::queue_macrotask(move || {
                            // The last handle is the effect's only tether.
                            drop(background);
                            data.set(2);
                        });
                    }
                });
            }
        });

        run();
        assert_eq!(
            &*seen.borrow(),
            &[0, 1],
            "the effect must survive scope disposal and die when its handle drops"
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
