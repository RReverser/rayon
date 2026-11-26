//! Stand-in for `enabled.rs` without the `context-propagation` feature: a
//! zero-sized `JobContext` whose methods compile to nothing. It mirrors the
//! crate-visible API of `enabled.rs` that the job types use.

use crate::registry::Registry;

pub(crate) struct JobContext;

impl JobContext {
    #[inline(always)]
    pub(crate) fn capture(_registry: &Registry) -> Self {
        JobContext
    }

    #[inline(always)]
    pub(crate) fn capture_scoped(_registry: &Registry) -> Self {
        JobContext
    }

    #[inline(always)]
    pub(crate) fn run(&self, f: impl FnOnce()) {
        f()
    }
}
