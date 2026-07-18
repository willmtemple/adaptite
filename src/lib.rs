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

mod effect;
mod event;
mod id;
mod observable;
mod reactor;
mod resource;
mod scope;
mod signal;
mod source;
mod thunk;
mod watch;

pub use effect::{EffectHandle, effect, effect_in};
pub use event::{Event, Subscription, event, event_in, on, on_in};
pub use id::NodeId;
pub use observable::{DynObservable, Observable};
pub use reactor::{ReactCycleError, Reactor, current, untrack};
pub use resource::{Resource, resource, resource_in};
pub use scope::{Owner, ScopeHandle, on_cleanup, owner, scope, unowned};
pub use signal::{Signal, signal, signal_in};
pub use source::{Source, source, source_in};
pub use thunk::{
    Memo, Thunk, memo, memo_by, memo_by_in, memo_by_with_prev, memo_by_with_prev_in, memo_in,
    memo_with_prev, memo_with_prev_in, thunk, thunk_in,
};
pub use watch::{watch, watch_in};
