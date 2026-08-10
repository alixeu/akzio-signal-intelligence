//! Durable local worker pool.
//!
//! The pool owns no business policy.  It only turns SQLite-backed task leases
//! into concurrently running handlers and stops cleanly when its supervisor
//! asks it to.  `TaskRuntime` remains the only owner of attempts, heartbeats,
//! timeouts, retries, and terminal task events.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use akzio_runtime::legacy::{RuntimeError, TaskCompletion, TaskRuntime};
use akzio_store::legacy::ClaimedTask;
use chrono::Utc;
use tokio::sync::watch;

pub type TaskHandler =
    Arc<dyn Fn(ClaimedTask) -> Pin<Box<dyn Future<Output = TaskCompletion> + Send>> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPoolConfig {
    pub worker_count: usize,
    pub idle_poll: Duration,
    pub worker_prefix: String,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            worker_count: 2,
            idle_poll: Duration::from_millis(250),
            worker_prefix: "akzio-local".to_owned(),
        }
    }
}

impl WorkerPoolConfig {
    fn normalized_worker_count(&self) -> usize {
        self.worker_count.max(1)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerPool {
    runtime: TaskRuntime,
    config: WorkerPoolConfig,
}

impl WorkerPool {
    pub fn new(runtime: TaskRuntime, config: WorkerPoolConfig) -> Self {
        Self { runtime, config }
    }

    /// Recover work owned by a process that stopped without finishing its
    /// leases.  It is safe to call before every pool start and is idempotent.
    pub fn recover_abandoned(&self) -> Result<u64, RuntimeError> {
        self.runtime
            .store()
            .recover_expired_tasks(Utc::now())
            .map_err(RuntimeError::from)
    }

    /// Run the configured worker count until `shutdown` becomes true.
    ///
    /// A task is never run outside `TaskRuntime`: handler failures must be
    /// converted to a `TaskCompletion` by the business runtime, which keeps
    /// retry policy and every state transition durable.
    pub async fn serve(
        &self,
        handler: TaskHandler,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), RuntimeError> {
        self.recover_abandoned()?;
        let mut workers = tokio::task::JoinSet::new();
        for index in 0..self.config.normalized_worker_count() {
            let runtime = self.runtime.clone();
            let handler = handler.clone();
            let shutdown = shutdown.clone();
            let worker_id = format!("{}-{index}", self.config.worker_prefix);
            let idle_poll = self.config.idle_poll;
            workers.spawn(async move {
                worker_loop(runtime, worker_id, handler, shutdown, idle_poll).await
            });
        }

        while let Some(result) = workers.join_next().await {
            result.map_err(|error| {
                RuntimeError::Handler(format!("worker task panicked: {error}"))
            })??;
        }
        Ok(())
    }

    pub fn worker_ids(&self) -> Vec<String> {
        (0..self.config.normalized_worker_count())
            .map(|index| format!("{}-{index}", self.config.worker_prefix))
            .collect()
    }
}

async fn worker_loop(
    runtime: TaskRuntime,
    worker_id: String,
    handler: TaskHandler,
    mut shutdown: watch::Receiver<bool>,
    idle_poll: Duration,
) -> Result<(), RuntimeError> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        let did_run = runtime
            .run_one_async(&worker_id, |task| handler(task))
            .await?;
        if did_run {
            continue;
        }

        tokio::select! {
            _ = tokio::time::sleep(idle_poll) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use akzio_domain::{RunId, RunPurpose, TaskId, TaskKind};
    use akzio_store::legacy::V2Store;
    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn pool_runs_ready_tasks_across_workers_and_stops_cleanly() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let run = RunId::new();
        store
            .create_run(&run, RunPurpose::Debug, "test", Utc::now())
            .unwrap();
        for _ in 0..4 {
            store
                .enqueue_task(&run, &TaskId::new(), TaskKind::Evaluate, Utc::now())
                .unwrap();
        }

        let completed = Arc::new(AtomicUsize::new(0));
        let handler: TaskHandler = {
            let completed = completed.clone();
            Arc::new(move |_| {
                let completed = completed.clone();
                Box::pin(async move {
                    completed.fetch_add(1, Ordering::SeqCst);
                    TaskCompletion::Succeeded
                })
            })
        };
        let pool = WorkerPool::new(
            TaskRuntime::new(store.clone()),
            WorkerPoolConfig {
                worker_count: 2,
                idle_poll: Duration::from_millis(5),
                worker_prefix: "test".to_owned(),
            },
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn({
            let pool = pool.clone();
            async move { pool.serve(handler, shutdown_rx).await }
        });

        while completed.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 4);
        assert!(store.verify_integrity().is_ok());
        assert_eq!(
            pool.worker_ids().into_iter().collect::<BTreeSet<_>>().len(),
            2
        );
    }
}
