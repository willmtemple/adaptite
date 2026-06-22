//! Fine-grained reactivity primitives for [runite](https://github.com/willmtemple/runite).
//!
//! This crate is intentionally layered on top of `runite`. The reactive graph is
//! single-threaded and designed to live on a runtime-managed thread, while async work feeds it
//! from the edges by updating state or emitting events.

extern crate alloc;

pub(crate) mod trace_targets {
    pub const GRAPH: &str = "adaptite::graph";
    pub const CELL: &str = "adaptite::cell";
    pub const THUNK: &str = "adaptite::thunk";
    pub const MEMO: &str = "adaptite::memo";
    pub const EFFECT: &str = "adaptite::effect";
    pub const EVENT: &str = "adaptite::event";
}

mod cell;
mod effect;
mod event;
mod id;
mod reactor;
mod source;
mod thunk;

pub use cell::{Cell, cell, cell_in};
pub use effect::{EffectHandle, effect, effect_in};
pub use event::{Event, Subscription, event, event_in, on, on_in};
pub use id::NodeId;
pub use reactor::{ReactCycleError, Reactor, current};
pub use source::{Source, source, source_in};
pub use thunk::{Memo, Thunk, memo, memo_by, memo_by_in, memo_in, thunk, thunk_in};
