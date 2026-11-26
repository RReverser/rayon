//! Propagation of thread-local context (for example the current `tracing`
//! span) from the code that creates a job to the thread that runs it.
//!
//! Without the `context-propagation` feature, `disabled.rs` stands in
//! for `enabled.rs`, so the job paths compile exactly as before.

#[cfg_attr(feature = "context-propagation", path = "enabled.rs")]
#[cfg_attr(not(feature = "context-propagation"), path = "disabled.rs")]
mod imp;

// `CapturedContext` is the only public item, and only exists with the feature:
// a `pub` glob would re-export nothing without it, which `unreachable_pub`
// rejects.
#[cfg(feature = "context-propagation")]
pub use imp::CapturedContext;
pub(crate) use imp::*;

#[cfg(all(test, feature = "context-propagation"))]
mod test;
