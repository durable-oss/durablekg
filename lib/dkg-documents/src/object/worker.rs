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
use std::time::Duration;

use async_channel::{Receiver, Sender};
use dkg_core::{Error, Result};
use tokio::sync::oneshot;

/// A unit of work: run a closure against the worker's resource and send the
/// result back. Boxed so jobs of different return types share one queue.
type Job<R> = Box<dyn FnOnce(&R) + Send + 'static>;

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
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            readers: 4,
            queue_depth: 1024,
            enqueue_timeout: Duration::from_millis(250),
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
    label: &'static str,
    /// Join handles for every spawned thread, shared across clones. Taken
    /// (via `Option::take`) by the first caller of [`shutdown`], so threads
    /// are joined once.
    handles: Arc<Mutex<Option<Vec<JoinHandle<()>>>>>,
}

impl<R: Clone + Send + 'static> WorkerPool<R> {
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

        // Writer: one thread, draining the write queue in order. When the
        // channel closes it drains the remaining queued jobs and then exits.
        {
            let resource = resource.clone();
            let write_rx: Receiver<Job<R>> = write_rx;
            handles.push(
                thread::Builder::new()
                    .name(format!("dkg-{label}-writer"))
                    .spawn(move || {
                        while let Ok(job) = write_rx.recv_blocking() {
                            job(&resource);
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

    async fn submit<T, F>(&self, tx: &Sender<Job<R>>, kind: &str, f: F) -> Result<T>
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
        // panicked mid-job.
        reply_rx
            .await
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

    /// A trivial shareable resource: a counter the jobs increment.
    #[derive(Clone)]
    struct Counter(Arc<AtomicUsize>);

    #[tokio::test]
    async fn read_and_write_round_trip() {
        let pool = WorkerPool::spawn(
            "test",
            Counter(Arc::new(AtomicUsize::new(0))),
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
    async fn shutdown_drains_queued_jobs_then_rejects() {
        let counter = Arc::new(AtomicUsize::new(0));
        let pool = WorkerPool::spawn(
            "test",
            Counter(Arc::clone(&counter)),
            PoolConfig {
                readers: 1,
                queue_depth: 16,
                enqueue_timeout: Duration::from_secs(5),
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
            Counter(Arc::new(AtomicUsize::new(0))),
            PoolConfig {
                readers: 4,
                queue_depth: 16,
                enqueue_timeout: Duration::from_secs(5),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_queue_yields_overloaded() {
        // One reader, a depth-1 queue, and a short enqueue timeout. Filling
        // the in-flight slot plus the queued slot with slow jobs forces the
        // next submit to time out as Overloaded.
        let pool = WorkerPool::spawn(
            "test",
            Counter(Arc::new(AtomicUsize::new(0))),
            PoolConfig {
                readers: 1,
                queue_depth: 1,
                enqueue_timeout: Duration::from_millis(50),
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
