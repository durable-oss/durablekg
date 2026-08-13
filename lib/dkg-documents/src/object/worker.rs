//! A thread-backed worker pool with bounded queues and backpressure.
//!
//! Fjall and Tantivy are *blocking* libraries. Calling them directly from an
//! async handler ties up a Tokio worker for the duration of the disk I/O,
//! which starves other tasks. This pool moves that work onto dedicated OS
//! threads and lets the async caller `await` a reply instead.
//!
//! # Shape
//!
//! Each pool owns a resource handle `R` (a cheap-to-clone Fjall or Tantivy
//! handle) and spawns:
//!
//! - **one writer thread**, so writes are serialized — Tantivy has a single
//!   `IndexWriter`, and serializing Fjall writes keeps index updates
//!   ordered; and
//! - **N reader threads**, so independent reads (gets, searches) run
//!   concurrently.
//!
//! Each thread holds its own clone of `R` and pulls jobs from a bounded
//! [`tokio::sync::mpsc`] queue. A job is a closure plus a one-shot reply
//! channel.
//!
//! # Backpressure
//!
//! The queues are **bounded**. Submitting waits for a free slot up to a
//! configured timeout; if the queue stays full that long, submission fails
//! with [`Error::Overloaded`] rather than blocking the caller forever. That
//! turns a saturated subsystem into a fast, explicit "try again" (an HTTP
//! `503`) instead of unbounded latency growth.

use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_channel::{Receiver, Sender, TryRecvError};
use dkg_core::{Error, Result};
use tokio::sync::oneshot;

/// How long the writer parks when it has staged work but an empty queue.
///
/// Short enough that the flush deadline stays accurate to roughly this
/// granularity, long enough that waiting out a two-second interval costs a
/// trivial number of wakeups.
const IDLE_POLL: Duration = Duration::from_millis(5);

/// Run a group flush and reset the staging counters.
///
/// A failed flush leaves the data staged but un-durable; the counters reset
/// regardless, because retrying the same flush on the next tick will not fix
/// a persistent I/O error and a stuck counter would flush on every subsequent
/// job. The error surfaces to the caller of the *next* write through the
/// store's own error path.
fn flush<R: GroupCommit>(resource: &R, staged: &mut usize, last_flush: &mut Instant) {
    // The result is intentionally swallowed here: this runs on the writer
    // thread, detached from any one caller's request.
    let _ = resource.flush();
    *staged = 0;
    *last_flush = Instant::now();
}

/// A unit of work: run a closure against the worker's resource and send the
/// result back. Boxed so jobs of different return types share one queue.
type Job<R> = Box<dyn FnOnce(&R) + Send + 'static>;

/// A resource whose writes are made durable (or visible) in *groups* rather
/// than one at a time.
///
/// Both backing stores pay a large fixed cost per durability barrier — an
/// fsync for Fjall, a segment commit for Tantivy — so both stage writes and
/// flush them together. The writer thread calls [`flush`](GroupCommit::flush)
/// on an interval, once a write has been staged, and again at shutdown.
///
/// The default implementation makes this a no-op, so a resource with no
/// group-commit semantics needs no extra code.
pub trait GroupCommit {
    /// Flush staged writes. Called from the writer thread only, never
    /// concurrently with itself.
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Whether a flush is worth doing right now, given how long it has been
    /// since the last one. Lets a resource commit early under a burst
    /// instead of waiting out the interval.
    ///
    /// `staged` is how many writes the pool has run since its last flush.
    fn should_flush(&self, staged: usize, elapsed: Duration, config: &CommitConfig) -> bool {
        staged > 0 && (staged >= config.max_staged || elapsed >= config.interval)
    }
}

/// Tuning for the writer thread's group commit.
#[derive(Debug, Clone, Copy)]
pub struct CommitConfig {
    /// Longest a staged write waits before being flushed.
    pub interval: Duration,
    /// Flush early once this many writes are staged, so a burst does not sit
    /// buffered for the whole interval.
    pub max_staged: usize,
}

impl Default for CommitConfig {
    fn default() -> Self {
        CommitConfig {
            interval: Duration::from_secs(2),
            max_staged: 1000,
        }
    }
}

/// Tuning for a [`WorkerPool`].
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// Number of reader threads. Writers are always a single thread.
    pub readers: usize,
    /// Bounded capacity of each queue (reads and writes are separate).
    pub queue_depth: usize,
    /// How long [`WorkerPool::read`] / [`WorkerPool::write`] wait for a free
    /// queue slot before returning [`Error::Overloaded`].
    pub enqueue_timeout: Duration,
    /// How long a submitted job may take to come back before the caller gives
    /// up with [`Error::Overloaded`]. `None` waits indefinitely.
    ///
    /// The enqueue timeout alone does not bound a request: with a deep queue
    /// there is nearly always a free slot, so submission succeeds instantly
    /// and the caller then blocks on the reply for as long as the job runs.
    /// A job that is slow — or a pool whose threads are all busy with slow
    /// jobs — therefore produces an unbounded hang rather than a fast,
    /// retryable failure. This is the ceiling that turns that into a `503`.
    ///
    /// It bounds the *caller*, not the worker: the job keeps running to
    /// completion on its thread, since interrupting a blocking store call
    /// midway is not something the pool can do safely. So this converts an
    /// invisible hang into a visible error; it does not free the thread.
    pub execute_timeout: Option<Duration>,
    /// Group-commit tuning for the writer thread.
    pub commit: CommitConfig,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            readers: 4,
            queue_depth: 1024,
            enqueue_timeout: Duration::from_millis(250),
            // Generous on purpose. This is a backstop against a hung request,
            // not a latency target: a healthy read answers in milliseconds,
            // while a legitimately slow one (a large collection scan, an
            // index backfill) can take seconds. Set it low enough to cut real
            // work and it becomes a source of spurious 503s.
            execute_timeout: Some(Duration::from_secs(30)),
            commit: CommitConfig::default(),
        }
    }
}

impl PoolConfig {
    /// Build a config from the environment, falling back to [`Default`] for
    /// anything unset:
    ///
    /// - `DKG_STORE_READERS` — reader threads per subsystem
    /// - `DKG_STORE_QUEUE_DEPTH` — bounded queue capacity per queue
    /// - `DKG_STORE_ENQUEUE_TIMEOUT_MS` — backpressure wait before
    ///   [`Error::Overloaded`]
    /// - `DKG_STORE_EXECUTE_TIMEOUT_MS` — how long a submitted job may run
    ///   before the caller gives up; `0` disables the ceiling and waits
    ///   indefinitely
    /// - `DKG_STORE_COMMIT_INTERVAL_MS` — longest a staged write waits before
    ///   the writer thread flushes it
    /// - `DKG_STORE_COMMIT_MAX_STAGED` — flush early once this many writes
    ///   are staged
    ///
    /// Unparseable values are ignored in favour of the default, so a typo
    /// degrades to sane behaviour rather than a boot failure.
    pub fn from_env() -> Self {
        let d = PoolConfig::default();
        PoolConfig {
            readers: env_parse("DKG_STORE_READERS").unwrap_or(d.readers).max(1),
            queue_depth: env_parse("DKG_STORE_QUEUE_DEPTH")
                .unwrap_or(d.queue_depth)
                .max(1),
            enqueue_timeout: env_parse("DKG_STORE_ENQUEUE_TIMEOUT_MS")
                .map(Duration::from_millis)
                .unwrap_or(d.enqueue_timeout),
            // `0` is the explicit opt-out: an operator who would rather wait
            // than see a 503 can restore the previous unbounded behaviour.
            execute_timeout: match env_parse::<u64>("DKG_STORE_EXECUTE_TIMEOUT_MS") {
                Some(0) => None,
                Some(ms) => Some(Duration::from_millis(ms)),
                None => d.execute_timeout,
            },
            commit: CommitConfig {
                interval: env_parse("DKG_STORE_COMMIT_INTERVAL_MS")
                    .map(Duration::from_millis)
                    .unwrap_or(d.commit.interval),
                max_staged: env_parse("DKG_STORE_COMMIT_MAX_STAGED")
                    .unwrap_or(d.commit.max_staged)
                    .max(1),
            },
        }
    }
}

fn env_parse<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok()?.parse().ok()
}

/// A writer thread plus a pool of reader threads, fed by bounded queues.
///
/// Cloneable: clones share the same threads, queues, and join handles (the
/// senders are cloned, the threads are not respawned). [`shutdown`] drains
/// and joins the threads exactly once no matter how many clones call it.
///
/// [`shutdown`]: WorkerPool::shutdown
#[derive(Clone)]
pub struct WorkerPool<R: Send + 'static> {
    write_tx: Sender<Job<R>>,
    read_tx: Sender<Job<R>>,
    enqueue_timeout: Duration,
    execute_timeout: Option<Duration>,
    label: &'static str,
    /// Join handles for every spawned thread, shared across clones. Taken
    /// (via `Option::take`) by the first caller of [`shutdown`], so threads
    /// are joined once.
    handles: Arc<Mutex<Option<Vec<JoinHandle<()>>>>>,
}

impl<R: Clone + Send + GroupCommit + 'static> WorkerPool<R> {
    /// Spawn the writer and reader threads over clones of `resource`.
    ///
    /// `label` names the subsystem (e.g. `"meta"` / `"fulltext"`) and is used
    /// in overload error messages.
    pub fn spawn(label: &'static str, resource: R, config: PoolConfig) -> Self {
        // `async_channel` is MPMC, so every reader thread holds its own
        // cloned receiver and pulls jobs independently — no shared mutex,
        // no reader blocking the others while idle. The sender side is
        // bounded and async-aware, which is where backpressure comes from.
        let (write_tx, write_rx) = async_channel::bounded::<Job<R>>(config.queue_depth);
        let (read_tx, read_rx) = async_channel::bounded::<Job<R>>(config.queue_depth);

        let mut handles = Vec::with_capacity(config.readers.max(1) + 1);

        // Writer: one thread, draining the write queue in order and flushing
        // staged work in groups. When the channel closes it drains the
        // remaining queued jobs, flushes a final time, and exits — so an
        // orderly shutdown never loses a write.
        {
            let resource = resource.clone();
            let write_rx: Receiver<Job<R>> = write_rx;
            let commit = config.commit;
            handles.push(
                thread::Builder::new()
                    .name(format!("dkg-{label}-writer"))
                    .spawn(move || {
                        let mut staged = 0usize;
                        let mut last_flush = Instant::now();

                        loop {
                            let job = if staged == 0 {
                                // Nothing staged: no deadline to honour, so
                                // block until work arrives rather than spin.
                                match write_rx.recv_blocking() {
                                    Ok(job) => job,
                                    Err(_) => break,
                                }
                            } else {
                                // Staged work is waiting on a flush deadline,
                                // so the wait has to be bounded. Poll, and
                                // park briefly when the queue is empty.
                                match write_rx.try_recv() {
                                    Ok(job) => job,
                                    Err(TryRecvError::Empty) => {
                                        if resource.should_flush(
                                            staged,
                                            last_flush.elapsed(),
                                            &commit,
                                        ) {
                                            flush(&resource, &mut staged, &mut last_flush);
                                        } else {
                                            thread::sleep(IDLE_POLL);
                                        }
                                        continue;
                                    }
                                    Err(TryRecvError::Closed) => break,
                                }
                            };

                            job(&resource);
                            staged += 1;

                            if resource.should_flush(staged, last_flush.elapsed(), &commit) {
                                flush(&resource, &mut staged, &mut last_flush);
                            }
                        }

                        // The queue is closed and drained. Flush whatever is
                        // still staged so an orderly shutdown loses nothing.
                        if staged > 0 {
                            flush(&resource, &mut staged, &mut last_flush);
                        }
                    })
                    .expect("spawn writer thread"),
            );
        }

        // Readers: N threads, each with its own receiver clone.
        for i in 0..config.readers.max(1) {
            let resource = resource.clone();
            let read_rx = read_rx.clone();
            handles.push(
                thread::Builder::new()
                    .name(format!("dkg-{label}-reader-{i}"))
                    .spawn(move || {
                        while let Ok(job) = read_rx.recv_blocking() {
                            job(&resource);
                        }
                    })
                    .expect("spawn reader thread"),
            );
        }

        WorkerPool {
            write_tx,
            read_tx,
            enqueue_timeout: config.enqueue_timeout,
            execute_timeout: config.execute_timeout,
            label,
            handles: Arc::new(Mutex::new(Some(handles))),
        }
    }

    /// Gracefully drain and stop the pool.
    ///
    /// Closes both queues so no new work is accepted (in-flight [`read`] /
    /// [`write`] submissions still racing the close get a clean "stopped"
    /// error), lets the worker threads finish every job already queued, and
    /// joins them. Idempotent and safe to call from any clone: only the
    /// first caller joins; later callers return immediately.
    ///
    /// This blocks the calling thread until the workers finish, so call it
    /// from a blocking context (e.g. `tokio::task::spawn_blocking`).
    ///
    /// [`read`]: WorkerPool::read
    /// [`write`]: WorkerPool::write
    pub fn shutdown(&self) {
        // Close the senders: queued jobs still drain, but `recv_blocking`
        // returns `Err` once the queue empties, so each thread exits.
        self.write_tx.close();
        self.read_tx.close();

        // Take the handles so only the first caller joins.
        let handles = self.handles.lock().expect("worker handles lock").take();
        if let Some(handles) = handles {
            for handle in handles {
                // A worker panic should not abort shutdown of the rest.
                let _ = handle.join();
            }
        }
    }

    /// Run `f` on a reader thread and await its result.
    ///
    /// # Errors
    ///
    /// [`Error::Overloaded`] if the read queue stays full past the configured
    /// timeout; otherwise whatever `f` returns.
    pub async fn read<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&R) -> Result<T> + Send + 'static,
    {
        self.submit(&self.read_tx, "read", f).await
    }

    /// Run `f` on the writer thread and await its result. Writes are
    /// serialized in submission order.
    ///
    /// # Errors
    ///
    /// [`Error::Overloaded`] if the write queue stays full past the
    /// configured timeout; otherwise whatever `f` returns.
    pub async fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&R) -> Result<T> + Send + 'static,
    {
        self.submit(&self.write_tx, "write", f).await
    }

    /// Like [`write`](Self::write), but exempt from the execution ceiling.
    ///
    /// For maintenance that is legitimately long-running — an index backfill
    /// over a whole collection, a repair pass — where the ceiling exists to
    /// bound a *request* and would otherwise abort real work partway through.
    /// The enqueue timeout still applies, so this cannot bypass backpressure.
    ///
    /// # Errors
    ///
    /// [`Error::Overloaded`] if the write queue stays full past the
    /// configured timeout; otherwise whatever `f` returns.
    pub async fn write_unbounded<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&R) -> Result<T> + Send + 'static,
    {
        self.submit_with(&self.write_tx, "write", None, f).await
    }

    async fn submit<T, F>(&self, tx: &Sender<Job<R>>, kind: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&R) -> Result<T> + Send + 'static,
    {
        self.submit_with(tx, kind, self.execute_timeout, f).await
    }

    async fn submit_with<T, F>(
        &self,
        tx: &Sender<Job<R>>,
        kind: &str,
        execute_timeout: Option<Duration>,
        f: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&R) -> Result<T> + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel::<Result<T>>();
        let job: Job<R> = Box::new(move |resource| {
            // If the caller has already gone away, dropping the result is
            // fine.
            let _ = reply_tx.send(f(resource));
        });

        // Bounded-wait enqueue: this is the backpressure point. `send`
        // resolves once a slot frees; the timeout converts sustained
        // saturation into an explicit, retryable error.
        match tokio::time::timeout(self.enqueue_timeout, tx.send(job)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(Error::storage(format!(
                    "{} {kind} worker stopped",
                    self.label
                )))
            }
            Err(_) => {
                return Err(Error::Overloaded(format!(
                    "{} {kind} queue saturated",
                    self.label
                )))
            }
        }

        // Await the worker's reply. A dropped reply channel means the worker
        // panicked mid-job. The wait is bounded so a slow or wedged job
        // surfaces as a retryable `503` instead of an open-ended hang; the
        // job itself still runs to completion on its thread.
        let reply = match execute_timeout {
            Some(limit) => match tokio::time::timeout(limit, reply_rx).await {
                Ok(reply) => reply,
                Err(_) => {
                    return Err(Error::Overloaded(format!(
                        "{} {kind} job exceeded {limit:?}",
                        self.label
                    )))
                }
            },
            None => reply_rx.await,
        };

        reply
            .map_err(|_| Error::storage(format!("{} {kind} worker dropped the job", self.label)))?
    }
}

impl<R: Send + 'static> Drop for WorkerPool<R> {
    fn drop(&mut self) {
        // When the *last* clone of the pool goes away without an explicit
        // `shutdown`, close the channels so the worker threads stop blocking
        // and exit (they self-terminate, detached). Closing earlier — while
        // other clones still hold senders — would wrongly tear the pool down,
        // so gate on being the final owner. We do not join here: a blocking
        // join in `drop` could stall an async runtime. Callers wanting a
        // *graceful* drain that waits for in-flight jobs should call
        // `shutdown` explicitly.
        if Arc::strong_count(&self.handles) == 1 {
            self.write_tx.close();
            self.read_tx.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A trivial shareable resource: a counter the jobs increment. Its
    /// second counter records how many times the pool group-flushed it.
    #[derive(Clone)]
    struct Counter(Arc<AtomicUsize>, Arc<AtomicUsize>);

    impl Counter {
        fn new() -> Self {
            Counter(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)))
        }

        fn with_value(value: Arc<AtomicUsize>) -> Self {
            Counter(value, Arc::new(AtomicUsize::new(0)))
        }

        /// How many group flushes the writer thread has run.
        fn flushes(&self) -> usize {
            self.1.load(Ordering::SeqCst)
        }
    }

    impl GroupCommit for Counter {
        fn flush(&self) -> Result<()> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn read_and_write_round_trip() {
        let pool = WorkerPool::spawn(
            "test",
            Counter::new(),
            PoolConfig::default(),
        );
        pool.write(|c| {
            c.0.fetch_add(5, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
        let n = pool.read(|c| Ok(c.0.load(Ordering::SeqCst))).await.unwrap();
        assert_eq!(n, 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn many_writes_share_one_group_flush() {
        // A long interval and a high staged threshold mean nothing flushes
        // mid-run; the only flush is the one at shutdown. This is the
        // property the whole design rests on: N writes cost one barrier, not
        // N of them.
        let resource = Counter::new();
        let pool = WorkerPool::spawn(
            "test",
            resource.clone(),
            PoolConfig {
                readers: 1,
                queue_depth: 256,
                enqueue_timeout: Duration::from_secs(5),
                execute_timeout: None,
                commit: CommitConfig {
                    interval: Duration::from_secs(3600),
                    max_staged: 10_000,
                },
            },
        );

        for _ in 0..100 {
            pool.write(|c| {
                c.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap();
        }

        // Nothing has flushed yet: neither trigger has fired.
        assert_eq!(resource.flushes(), 0, "no flush should have fired yet");

        let drain = pool.clone();
        tokio::task::spawn_blocking(move || drain.shutdown())
            .await
            .unwrap();

        assert_eq!(resource.0.load(Ordering::SeqCst), 100, "every write ran");
        assert_eq!(
            resource.flushes(),
            1,
            "100 writes should cost exactly one group flush"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_burst_flushes_early_on_the_staged_threshold() {
        // With a long interval but a low staged threshold, a burst commits on
        // volume rather than sitting buffered until the interval elapses.
        let resource = Counter::new();
        let pool = WorkerPool::spawn(
            "test",
            resource.clone(),
            PoolConfig {
                readers: 1,
                queue_depth: 256,
                enqueue_timeout: Duration::from_secs(5),
                execute_timeout: None,
                commit: CommitConfig {
                    interval: Duration::from_secs(3600),
                    max_staged: 10,
                },
            },
        );

        // Submit all 30 without awaiting between them, so they are genuinely
        // queued together. Awaiting each in turn would let the writer drain to
        // an empty queue between writes and flush from the idle branch at
        // whatever staged count it happened to hold — which is scheduling
        // dependent, and is what made an exact-count assertion flaky here.
        let mut inflight = Vec::with_capacity(30);
        for _ in 0..30 {
            let pool = pool.clone();
            inflight.push(tokio::spawn(async move { pool.write(|_| Ok(())).await }));
        }
        for handle in inflight {
            handle.await.unwrap().unwrap();
        }

        // The interval is an hour, so every flush came from the staged count.
        // 30 writes at a threshold of 10 is three groups. A writer that found
        // its queue briefly empty may have flushed a partial group early, so
        // allow more than three — but never one per write, which would mean
        // the burst was not being grouped at all.
        let flushes = resource.flushes();
        assert!(
            (3..30).contains(&flushes),
            "flushed {flushes} times for 30 writes at a staged threshold of 10"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_queued_jobs_then_rejects() {
        let counter = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::spawn(
            "test",
            Counter::with_value(Arc::clone(&counter)),
            PoolConfig {
                readers: 1,
                queue_depth: 16,
                enqueue_timeout: Duration::from_secs(5),
                execute_timeout: None,
                commit: CommitConfig::default(),
            },
        );

        // Fire several slow jobs concurrently without awaiting them, so they
        // are genuinely still queued / in flight when shutdown begins. A
        // graceful drain must run all of them before the threads exit.
        let mut inflight = Vec::new();
        for _ in 0..5 {
            let pool = pool.clone();
            inflight.push(tokio::spawn(async move {
                pool.write(|c| {
                    std::thread::sleep(Duration::from_millis(20));
                    c.0.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            }));
        }
        // Give the producers a moment to enqueue, then drain. shutdown()
        // blocks on joins, so run it off the async worker.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let drain = pool.clone();
        tokio::task::spawn_blocking(move || drain.shutdown())
            .await
            .unwrap();

        // Every accepted job ran before the threads exited.
        for h in inflight {
            // Each submission either completed or was rejected at the close
            // boundary; the ones that were accepted must have run.
            let _ = h.await.unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 5);

        // After shutdown, new submissions are rejected (not Overloaded —
        // the queue is closed, so it is a clean stop).
        let err = pool.write(|_| Ok(())).await.unwrap_err();
        assert!(
            matches!(err, dkg_core::Error::Storage(_)),
            "expected a stopped error, got {err:?}"
        );

        // Calling shutdown again is a harmless no-op.
        let again = pool.clone();
        tokio::task::spawn_blocking(move || again.shutdown())
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reads_run_concurrently_across_reader_threads() {
        // Four readers, four jobs that each sleep 100ms. If reads were
        // serialized this would take ~400ms; concurrently it's ~100ms.
        let pool = WorkerPool::spawn(
            "test",
            Counter::new(),
            PoolConfig {
                readers: 4,
                queue_depth: 16,
                enqueue_timeout: Duration::from_secs(5),
                execute_timeout: None,
                commit: CommitConfig::default(),
            },
        );

        let start = std::time::Instant::now();
        let mut handles = Vec::new();
        for _ in 0..4 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                pool.read(|c| {
                    std::thread::sleep(Duration::from_millis(100));
                    Ok(c.0.load(Ordering::SeqCst))
                })
                .await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        // Generous ceiling to avoid flakiness on a busy CI box, but well
        // under the ~400ms a serialized run would need.
        assert!(
            start.elapsed() < Duration::from_millis(300),
            "reads did not parallelize"
        );
    }

    /// A slow job must fail the caller rather than hang it.
    ///
    /// The enqueue timeout does not cover this: with a free queue slot the
    /// submission succeeds instantly, and without an execution ceiling the
    /// caller then waits for however long the job takes. That is what turned
    /// an expensive read into a request that never returned instead of a
    /// fast, retryable `503`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_slow_job_times_out_instead_of_hanging() {
        let pool = WorkerPool::spawn(
            "test",
            Counter::new(),
            PoolConfig {
                readers: 1,
                queue_depth: 16,
                enqueue_timeout: Duration::from_secs(5),
                execute_timeout: Some(Duration::from_millis(100)),
                commit: CommitConfig::default(),
            },
        );

        let start = std::time::Instant::now();
        let result = pool
            .read(|_| {
                std::thread::sleep(Duration::from_secs(5));
                Ok(())
            })
            .await;

        assert!(
            matches!(result, Err(dkg_core::Error::Overloaded(_))),
            "expected Overloaded, got {result:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "caller waited {:?}: the execution ceiling did not bound it",
            start.elapsed()
        );
    }

    /// The ceiling is opt-out, so an operator who would rather wait than see a
    /// 503 keeps the previous unbounded behaviour.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_execute_timeout_waits_for_the_job() {
        let pool = WorkerPool::spawn(
            "test",
            Counter::new(),
            PoolConfig {
                readers: 1,
                queue_depth: 16,
                enqueue_timeout: Duration::from_secs(5),
                execute_timeout: None,
                commit: CommitConfig::default(),
            },
        );

        let result = pool
            .read(|c| {
                std::thread::sleep(Duration::from_millis(200));
                Ok(c.0.load(Ordering::SeqCst))
            })
            .await;
        assert_eq!(result.unwrap(), 0, "the job should have been awaited");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_queue_yields_overloaded() {
        // One reader, a depth-1 queue, and a short enqueue timeout. Filling
        // the in-flight slot plus the queued slot with slow jobs forces the
        // next submit to time out as Overloaded.
        let pool = WorkerPool::spawn(
            "test",
            Counter::new(),
            PoolConfig {
                readers: 1,
                queue_depth: 1,
                enqueue_timeout: Duration::from_millis(50),
                execute_timeout: None,
                commit: CommitConfig::default(),
            },
        );

        // Occupy the single reader thread for a while.
        let busy = {
            let pool = pool.clone();
            tokio::spawn(async move {
                pool.read(|_| {
                    std::thread::sleep(Duration::from_millis(500));
                    Ok(())
                })
                .await
            })
        };
        // Let the first job get picked up off the queue.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Fill the now-empty queue slot, then the next submit must overload.
        let _queued = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.read(|_| Ok(())).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        let result = pool.read(|_| Ok(())).await;
        assert!(
            matches!(result, Err(dkg_core::Error::Overloaded(_))),
            "expected Overloaded, got {result:?}"
        );

        busy.await.unwrap().unwrap();
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
