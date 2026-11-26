//! [`TracingSpan`], a [`CapturedContext`] for `tracing` spans.

use crate::CapturedContext;

/// The current [`tracing`] span, as a [`CapturedContext`].
///
/// Requires the `tracing` cargo feature. With it, every job of the pool runs
/// inside the span that was current where the job was created, so spans and
/// events recorded by parallel work nest under the code that started it:
///
/// ```
/// use rayon_core::{ThreadPoolBuilder, TracingSpan};
///
/// let pool = ThreadPoolBuilder::new()
///     .propagate_context::<TracingSpan>()
///     .build()
///     .unwrap();
///
/// // Any subscriber: without one, spans aren't recorded.
/// let _subscriber = tracing::subscriber::set_default(tracing_subscriber::registry());
///
/// let request = tracing::info_span!("request", id = 42);
/// request.in_scope(|| {
///     pool.scope(|s| {
///         for item in ["header", "body", "footer"] {
///             let request = &request;
///             // Recorded inside `request`, whichever thread renders it.
///             s.spawn(move |_| {
///                 tracing::info!(item, "rendered");
///                 assert_eq!(tracing::Span::current().id(), request.id());
///             });
///         }
///     })
/// });
/// ```
///
/// For scoped jobs (every [`join`](crate::join)) it captures only the id of
/// the current span and its dispatcher, without cloning the span: the creating
/// thread stays inside that span until the job has finished, so the span stays
/// alive. Other jobs ([`spawn`](crate::spawn) and friends) also take a reference
/// to the span, as cloning a [`tracing::Span`] would, which keeps it alive. It
/// is released when the job is dropped, which can be just after whatever waits
/// for the job has seen it finish.
///
/// The job also gets the span's dispatcher as its default, as on the creating
/// thread, so this works with a dispatcher set by
/// [`tracing::dispatcher::set_default`] too. When no span is current, nothing
/// is captured, so the job runs with its own thread's default dispatcher.
///
/// Costs: scoped capture clones the dispatcher, an atomic increment unless it
/// is the global one, and setting it as the default for a job has `tracing`
/// look up the default dispatcher through thread-local state, on every thread,
/// for as long as any such job runs, instead of using the global one directly.
///
/// Limitation: [`tracing_subscriber::Registry`] ignores entering a span that is
/// already on the thread's stack. A thread inside span `A` that steals a job
/// of `A` while blocked within a nested span `B` therefore still reports `B`
/// as current.
///
/// [`tracing_subscriber::Registry`]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/registry/struct.Registry.html
#[derive(Debug)]
pub struct TracingSpan {
    id: tracing::span::Id,
    dispatch: tracing::Dispatch,
    /// Whether this holds a reference to the span, keeping it alive for jobs
    /// that may outlive the code creating them.
    owned: bool,
}

impl CapturedContext for TracingSpan {
    fn capture() -> Option<Self> {
        // Like `capture_scoped`, plus a reference that keeps the span open
        // until this is dropped, as `Span::current()` would take.
        let mut span = Self::capture_scoped()?;
        span.id = span.dispatch.clone_span(&span.id);
        span.owned = true;
        Some(span)
    }

    fn capture_scoped() -> Option<Self> {
        // The dispatcher's current span, without the reference count that
        // `Span::current()` would add.
        tracing::dispatcher::get_default(|dispatch| {
            let (id, _) = dispatch.current_span().into_inner()?;
            Some(TracingSpan {
                id,
                dispatch: dispatch.clone(),
                owned: false,
            })
        })
    }

    fn run(&self, f: impl FnOnce()) {
        let Self { id, dispatch, .. } = self;
        // As the default, the dispatcher is also what jobs created inside `f`
        // capture from. `f` doesn't panic, so the span needs no guard to exit.
        tracing::dispatcher::with_default(dispatch, || {
            dispatch.enter(id);
            f();
            dispatch.exit(id);
        })
    }
}

impl Drop for TracingSpan {
    fn drop(&mut self) {
        // Closing the span also closes its parents, which
        // `tracing_subscriber`'s `Registry` does through the default
        // dispatcher, so that has to be the span's own.
        if self.owned {
            tracing::dispatcher::with_default(&self.dispatch, || {
                self.dispatch.try_close(self.id.clone());
            });
        }
    }
}
