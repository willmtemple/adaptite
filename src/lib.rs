#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

extern crate alloc;

pub(crate) mod trace_targets {
    pub const GRAPH: &str = "adaptite::graph";
    pub const SIGNAL: &str = "adaptite::signal";
    pub const THUNK: &str = "adaptite::thunk";
    pub const MEMO: &str = "adaptite::memo";
    pub const EFFECT: &str = "adaptite::effect";
    pub const EVENT: &str = "adaptite::event";
    pub const SCOPE: &str = "adaptite::scope";
    pub const RESOURCE: &str = "adaptite::resource";
}

mod diagnostics;
mod effect;
mod event;
mod id;
mod inspect;
mod observable;
mod ownership;
mod reactor;
mod resource;
mod scope;
mod signal;
mod source;
mod stats;
mod thunk;
mod watch;
mod writable;

pub use diagnostics::{
    ComputeOutcome, DiagnosticEvent, DiagnosticSubscription, InvalidationCause, InvalidationLevel,
    NodeKind, ReactorId,
};
pub use effect::{
    EffectHandle, EffectRun, EffectScheduler, effect, effect_in, effect_with, effect_with_in,
};
pub use event::{Event, Subscription, event, event_in, on, on_in};
pub use id::NodeId;
pub use inspect::{GraphEdge, GraphNode, GraphSnapshot, NodeState, RecordedDependency};
pub use observable::{DynObservable, Observable};
pub use ownership::{
    OwnershipAudit, OwnershipDrift, OwnershipGauge, OwnershipStats, audit_ownership,
    debug_assert_ownership_consistent, ownership_stats,
};
pub use reactor::{EnterGuard, ReactCycleError, Reactor, current, try_current, untrack};
pub use resource::{Resource, resource, resource_in};
pub use scope::{ErrorInfo, Owner, ScopeHandle, on_cleanup, owner, scope, scope_catch, unowned};
pub use signal::{Signal, signal, signal_in};
pub use source::{Source, source, source_in, source_with_hooks, source_with_hooks_in};
pub use stats::{FlushStats, GraphStats};
pub use thunk::{
    Memo, Thunk, memo, memo_by, memo_by_in, memo_by_with_prev, memo_by_with_prev_in, memo_in,
    memo_with_prev, memo_with_prev_in, thunk, thunk_in,
};
pub use watch::{watch, watch_in};
pub use writable::{Writable, WritableObservable, writable, writable_in};
