//! The `context-propagation` implementation of the `context` module (see `mod.rs`).

use crate::registry::Registry;

/// Thread-local context carried from the thread that creates a job to the
/// thread that runs it.
///
/// Rayon jobs can run on any thread of the pool, so anything a program keeps
/// in thread-local storage is lost when work is split. Installing a type with
/// [`ThreadPoolBuilder::propagate_context`] fixes that for every job of the
/// pool (and so every parallel iterator): [`capture`](Self::capture) runs
/// where a job is created, and [`run`](Self::run) runs the job with it. Only a
/// [`join`](crate::join) half that the thread creating it runs itself skips
/// `run`, as that thread is still where the context was captured.
///
/// # Cost
///
/// Without the `context-propagation` cargo feature, none of this exists and
/// jobs are compiled exactly as before. With the feature, every job is two
/// pointers larger, and a pool without a context type pays a branch where each
/// job is created, run and dropped. When one is installed,
/// [`capture`](Self::capture) runs for every job, including the many that end
/// up running on the same thread, so it should be cheap; when it returns
/// [`Some`], the value is boxed.
///
/// # Example
///
/// A server keeps the id of the request each thread is handling in a scoped
/// thread-local (here from the [`scoped-tls`] crate), for example to tag log
/// lines. With `RequestId` installed, that still works when a request's work
/// is split across the pool:
///
/// ```
/// use rayon_core::{CapturedContext, ThreadPoolBuilder};
///
/// scoped_tls::scoped_thread_local!(static REQUEST_ID: u64);
///
/// struct RequestId(u64);
///
/// impl CapturedContext for RequestId {
///     fn capture() -> Option<Self> {
///         REQUEST_ID.is_set().then(|| REQUEST_ID.with(|&id| RequestId(id)))
///     }
///
///     fn run(&self, f: impl FnOnce()) {
///         REQUEST_ID.set(&self.0, f)
///     }
/// }
///
/// let pool = ThreadPoolBuilder::new()
///     .propagate_context::<RequestId>()
///     .build()
///     .unwrap();
///
/// // Handle request 42 by rendering its parts in parallel.
/// REQUEST_ID.set(&42, || {
///     pool.scope(|s| {
///         for part in ["header", "body", "footer"] {
///             // Whichever thread renders it, it knows its request.
///             s.spawn(move |_| REQUEST_ID.with(|&id| assert_eq!(id, 42, "{part}")));
///         }
///     })
/// });
/// ```
///
/// It is [`Sync`] because [`spawn_broadcast`] shares one captured value
/// between all threads of the pool.
///
/// [`ThreadPoolBuilder::propagate_context`]: crate::ThreadPoolBuilder::propagate_context
/// [`scoped-tls`]: https://docs.rs/scoped-tls
/// [`spawn_broadcast`]: crate::spawn_broadcast
pub trait CapturedContext: Sized + Send + Sync + 'static {
    /// Captures the context to carry into a job created on this thread.
    ///
    /// Returning [`None`] means there is nothing to carry, and the job runs as
    /// if no context type was installed. Some jobs ([`spawn`](crate::spawn)
    /// and friends) can run after the code that created them has returned, so
    /// the value must keep whatever it refers to alive by itself.
    fn capture() -> Option<Self>;

    /// Runs `f` with this context made current on this thread. Must call `f`
    /// exactly once, then restore this thread's previous state, as the thread
    /// goes on to run other jobs.
    ///
    /// `f` doesn't panic: rayon catches panics from the job inside it. `run`
    /// must not panic either.
    fn run(&self, f: impl FnOnce());
}

/// Captures the context of the type installed on a pool, type-erased. A plain
/// function pointer, so the registry doesn't need to be generic over that type.
pub(crate) type CaptureFn = fn() -> JobContext;

/// Object-safe form of [`CapturedContext`], whose `run` is generic over `f`
/// and so can't be called on a trait object.
trait DynContext: Send + Sync {
    fn run(&self, f: &mut dyn FnMut());
}

impl<C: CapturedContext> DynContext for C {
    fn run(&self, f: &mut dyn FnMut()) {
        // `&mut dyn FnMut()` is itself an `FnOnce()`.
        CapturedContext::run(self, f)
    }
}

/// The context captured for one job, if any. [`None`] (no context type
/// installed, or nothing to carry) allocates nothing.
pub(crate) struct JobContext(Option<Box<dyn DynContext>>);

impl JobContext {
    /// Type-erases a captured context. (`Option::map(Box::new)` wouldn't coerce
    /// the box to `dyn DynContext`, hence the `match`.)
    pub(crate) fn new(context: Option<impl CapturedContext>) -> Self {
        JobContext(match context {
            Some(context) => Some(Box::new(context)),
            None => None,
        })
    }

    /// Captures the current context with the context type of `registry`, if
    /// it has one.
    #[inline]
    pub(crate) fn capture(registry: &Registry) -> Self {
        registry
            .capture_context
            .map_or(JobContext(None), |capture| capture())
    }

    /// Runs `f` with the captured context, if any, made current.
    #[inline]
    pub(crate) fn run(&self, f: impl FnOnce()) {
        match &self.0 {
            None => f(),
            Some(context) => {
                // A `dyn FnOnce` can't be called through a reference, so pass
                // `f` as a `dyn FnMut` that calls it once.
                let mut f = Some(f);
                context.run(&mut || f.take().unwrap()());
            }
        }
    }
}
