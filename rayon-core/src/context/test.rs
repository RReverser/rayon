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

/// A context that jobs can see on the thread they run on.
struct Probe(&'static str);

impl CapturedContext for Probe {
    fn capture() -> Option<Self> {
        Some(Probe("capture"))
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

/// Every kind of job runs with the context.
#[test]
fn every_job_kind() {
    const CAPTURED: &str = "capture";
    let pool = probe_pool(2);
    let other = probe_pool(1);
    // The value of the `Probe` the running job got. `PROBE.with` panics if
    // there is none, so each check also fails if a job ran without context.
    let seen = || PROBE.with(|&probe| probe);

    // From outside any pool: the job is injected into the pool.
    assert_eq!(pool.install(seen), CAPTURED);
    // From a worker of another pool: the cross-pool path.
    assert_eq!(other.install(|| pool.install(seen)), CAPTURED);
    pool.install(|| {
        // A `join` half stolen by another worker.
        assert_eq!(forced_steal(seen), CAPTURED);
        // One job per worker.
        assert_eq!(crate::broadcast(|_| seen()), [CAPTURED; 2]);

        // Scope spawns, collected once the scopes have waited for them.
        // `spawn_broadcast` runs one job on each of the 2 workers, hence 4.
        let got = Mutex::new(Vec::new());
        crate::scope(|s| s.spawn(|_| got.lock().unwrap().push(seen())));
        crate::scope_fifo(|s| s.spawn_fifo(|_| got.lock().unwrap().push(seen())));
        crate::scope(|s| s.spawn_broadcast(|_, _| got.lock().unwrap().push(seen())));
        assert_eq!(got.into_inner().unwrap(), [CAPTURED; 4]);
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
    assert_eq!(rx.iter().collect::<Vec<_>>(), [CAPTURED; 4]);

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
    assert_eq!(own, Some(CAPTURED));
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
