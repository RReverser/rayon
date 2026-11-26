//! Tests for context propagation, through the public API.

use crate::{CapturedContext, ThreadPool, ThreadPoolBuilder};
use scoped_tls::scoped_thread_local;
use std::cell::Cell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::thread;
use std::time::{Duration, Instant};

/// Runs `join(a, b)` with `a` blocked until `b` has started, so `b` can only
/// run by being stolen by another thread. Returns `b`'s result.
fn forced_steal<R: Send>(b: impl FnOnce() -> R + Send) -> R {
    let started = AtomicBool::new(false);
    let (a_thread, (b_thread, r)) = crate::join(
        || {
            let start = Instant::now();
            while !started.load(Ordering::SeqCst) {
                assert!(start.elapsed() < Duration::from_secs(10), "never stolen");
                std::hint::spin_loop();
            }
            thread::current().id()
        },
        || {
            started.store(true, Ordering::SeqCst);
            (thread::current().id(), b())
        },
    );
    assert_ne!(a_thread, b_thread);
    r
}

// The running job's `Probe` value, set by `Probe::run`.
scoped_thread_local!(static PROBE: &'static str);
// How many times `Probe::run` has been called on this thread.
thread_local!(static PROBE_RUNS: Cell<usize> = const { Cell::new(0) });

/// A context that records which capture method created it.
struct Probe(&'static str);

impl CapturedContext for Probe {
    fn capture() -> Option<Self> {
        Some(Probe("capture"))
    }

    fn capture_scoped() -> Option<Self> {
        Some(Probe("capture_scoped"))
    }

    fn run(&self, f: impl FnOnce()) {
        PROBE_RUNS.set(PROBE_RUNS.get() + 1);
        PROBE.set(&self.0, f)
    }
}

fn probe_pool(threads: usize) -> ThreadPool {
    ThreadPoolBuilder::new()
        .num_threads(threads)
        .propagate_context::<Probe>()
        .build()
        .unwrap()
}

/// Every kind of job runs with the context: those that finish before the
/// call creating them returns use `capture_scoped`, the others `capture`.
#[test]
fn every_job_kind() {
    const SCOPED: &str = "capture_scoped";
    const DETACHED: &str = "capture";
    let pool = probe_pool(2);
    let other = probe_pool(1);
    // The value of the `Probe` the running job got. `PROBE.with` panics if
    // there is none, so each check also fails if a job ran without context.
    let seen = || PROBE.with(|&probe| probe);

    // From outside any pool: the job is injected into the pool.
    assert_eq!(pool.install(seen), SCOPED);
    // From a worker of another pool: the cross-pool path.
    assert_eq!(other.install(|| pool.install(seen)), SCOPED);
    pool.install(|| {
        // A `join` half stolen by another worker.
        assert_eq!(forced_steal(seen), SCOPED);
        // One job per worker.
        assert_eq!(crate::broadcast(|_| seen()), [SCOPED; 2]);

        // Scope spawns, collected once the scopes have waited for them.
        // `spawn_broadcast` runs one job on each of the 2 workers, hence 4.
        // They take `capture` although the scope waits for them: the frame
        // spawning them can return, or leave its span, before they run.
        let got = Mutex::new(Vec::new());
        crate::scope(|s| s.spawn(|_| got.lock().unwrap().push(seen())));
        crate::scope_fifo(|s| s.spawn_fifo(|_| got.lock().unwrap().push(seen())));
        crate::scope(|s| s.spawn_broadcast(|_, _| got.lock().unwrap().push(seen())));
        assert_eq!(got.into_inner().unwrap(), [DETACHED; 4]);
    });

    // Pool spawns from outside, which nothing waits for, so they report over a
    // channel: one sender each, the last shared by `spawn_broadcast`'s 2 jobs.
    // `rx.iter()` ends once every job has dropped its sender.
    let (tx, rx) = channel();
    let tx2 = tx.clone();
    pool.spawn(move || tx2.send(seen()).unwrap());
    let tx2 = tx.clone();
    pool.spawn_fifo(move || tx2.send(seen()).unwrap());
    pool.spawn_broadcast(move |_| tx.send(seen()).unwrap());
    assert_eq!(rx.iter().collect::<Vec<_>>(), [DETACHED; 4]);

    // A spawn run by the thread that created it (here through `yield_local`,
    // in the one-thread pool) still runs with its context.
    let own = other.install(|| {
        let mut got = None;
        crate::scope(|s| {
            s.spawn(|_| got = Some(seen()));
            assert_eq!(crate::yield_local(), Some(crate::Yield::Executed));
        });
        got
    });
    assert_eq!(own, Some(DETACHED));
}

/// `run` is skipped for a `join` half that the thread creating it runs
/// itself: in a one-thread pool, that is every one.
#[test]
fn inline_jobs_skip_run() {
    fn tree(depth: u32) {
        if depth > 0 {
            crate::join(|| tree(depth - 1), || tree(depth - 1));
        }
    }

    // `PROBE_RUNS` is per thread, so it is read inside `install`, on the
    // pool's only thread, where all of its jobs run.
    let runs = probe_pool(1).install(|| {
        let before = PROBE_RUNS.get();
        tree(6);
        PROBE_RUNS.get() - before
    });
    assert_eq!(runs, 0);
}

/// The built-in `TracingSpan` context. Jobs are checked with `forced_steal`, so
/// they provably ran on another, idle thread (see the limitation documented
/// on `TracingSpan`).
#[cfg(feature = "tracing")]
mod tracing_span {
    use super::forced_steal;
    use crate::{ThreadPool, ThreadPoolBuilder};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::{Duration, Instant};
    use tracing::span::{Attributes, Id};
    use tracing::{Dispatch, Span, Subscriber};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;

    /// Counts the open spans of one dispatcher, which it also identifies.
    #[derive(Clone, Default)]
    struct Open(Arc<AtomicUsize>);

    impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Open {
        fn on_new_span(&self, _: &Attributes<'_>, _: &Id, _: Context<'_, S>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn on_close(&self, _: Id, _: Context<'_, S>) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// The current span, and whether the default dispatcher is `open`'s.
    fn current(open: &Open) -> (Option<Id>, bool) {
        let dispatch_ok = tracing::dispatcher::get_default(|d| {
            d.downcast_ref::<Open>()
                .is_some_and(|o| Arc::ptr_eq(&o.0, &open.0))
        });
        (Span::current().id(), dispatch_ok)
    }

    /// Blocks until `flag` is set.
    fn wait_for(flag: &AtomicBool) {
        let start = Instant::now();
        while !flag.load(Ordering::SeqCst) {
            assert!(start.elapsed() < Duration::from_secs(10), "never set");
            std::hint::spin_loop();
        }
    }

    /// Checks that jobs see the creator's span and default dispatcher (the one
    /// with `open`), and that no captured context keeps a span open.
    fn check(pool: &ThreadPool, other: &ThreadPool, open: &Open) {
        let here = || current(open);

        let outer = tracing::info_span!("outer");
        let want = (outer.id(), true);
        // Entered before `install`, so that its job captures the span (with no
        // current span, nothing is captured, not even the dispatcher).
        outer.in_scope(|| {
            pool.install(|| {
                assert_eq!(here(), want);
                assert_eq!(forced_steal(here), want);
                // From a worker of another pool: the cross-pool path.
                assert_eq!(other.install(here), want);

                // A span entered inside a job is what jobs created there capture,
                // over several hops.
                forced_steal(|| {
                    let inner = tracing::info_span!("inner");
                    let want = (inner.id(), true);
                    inner.in_scope(|| assert_eq!(forced_steal(|| forced_steal(here)), want));
                });

                // A scope spawn keeps its span alive after the creator drops its
                // handle (it only starts once that has happened), and jobs it
                // creates capture the span too.
                let detached = tracing::info_span!("detached");
                let want = (detached.id(), true);
                let dropped = AtomicBool::new(false);
                crate::scope(|s| {
                    detached.in_scope(|| {
                        s.spawn(|_| {
                            wait_for(&dropped);
                            assert_eq!(here(), want);
                            assert_eq!(crate::join(here, here), (want.clone(), want.clone()));
                        })
                    });
                    drop(detached);
                    dropped.store(true, Ordering::SeqCst);
                });
            })
        });
        drop(outer);

        // So do pool spawns, which nothing waits for, here starting only after
        // the creating call has returned and its span has been dropped. One
        // `spawn`, and a `spawn_broadcast` job on each of the pool's 4 threads.
        let spawned = tracing::info_span!("spawned");
        let want = (spawned.id(), true);
        let (tx, rx) = channel();
        let go = Arc::new(AtomicBool::new(false));
        spawned.in_scope(|| {
            let (tx, go, open) = (tx.clone(), go.clone(), open.clone());
            pool.spawn(move || {
                wait_for(&go);
                tx.send(current(&open)).unwrap();
            });
        });
        spawned.in_scope(|| {
            let (go, open) = (go.clone(), open.clone());
            pool.spawn_broadcast(move |_| {
                wait_for(&go);
                tx.send(current(&open)).unwrap();
            });
        });
        drop(spawned);
        go.store(true, Ordering::SeqCst);
        // `rx.iter()` ends once every job has dropped its sender.
        assert_eq!(rx.iter().collect::<Vec<_>>(), vec![want; 5]);

        // A finished job drops its context just after whatever waits for it has
        // seen it finish.
        let start = Instant::now();
        while open.0.load(Ordering::SeqCst) != 0 {
            assert!(start.elapsed() < Duration::from_secs(10), "a span leaked");
            thread::yield_now();
        }
    }

    /// One test function, because the global subscriber can only be set once.
    #[test]
    fn spans() {
        let global = Open::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(global.clone()),
        )
        .unwrap();
        let pool = |threads| {
            ThreadPoolBuilder::new()
                .num_threads(threads)
                .propagate_context::<crate::TracingSpan>()
                .build()
                .unwrap()
        };
        let (pool, other) = (pool(4), pool(1));
        check(&pool, &other, &global);
        // With no current span, nothing is captured: jobs run with their own
        // thread's default dispatcher, here the global one.
        assert_eq!(pool.install(|| current(&global)), (None, true));

        // With a dispatcher set by `with_default` rather than globally, jobs run
        // with it as their default, so spans keep propagating through nested jobs.
        let scoped = Open::default();
        let dispatch = Dispatch::new(tracing_subscriber::registry().with(scoped.clone()));
        tracing::dispatcher::with_default(&dispatch, || {
            check(&pool, &other, &scoped);
            // Not even the dispatcher, so jobs don't see `scoped`.
            assert_eq!(pool.install(|| current(&scoped)), (None, false));
        });
    }
}
