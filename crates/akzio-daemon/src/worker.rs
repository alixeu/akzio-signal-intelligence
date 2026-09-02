//! Durable local worker pool.
//!
//! The pool owns no business policy.  It only turns SQLite-backed task leases
//! into concurrently running handlers and stops cleanly when its supervisor
//! asks it to.  `TaskRuntime` remains the only owner of attempts, heartbeats,
//! timeouts, retries, and terminal task events.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use akzio_runtime::{RuntimeError, TaskCompletion, TaskRuntime};
use akzio_store::{ClaimedAttempt, Store, StoreError};
use chrono::Utc;
use tokio::sync::watch;

pub type TaskHandler = Arc<
    dyn Fn(ClaimedAttempt) -> Pin<Box<dyn Future<Output = TaskCompletion> + Send>> + Send + Sync,
>;

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
    lesson_revalidation_store: Option<Store>,
}

impl WorkerPool {
    pub fn new(runtime: TaskRuntime, config: WorkerPoolConfig) -> Self {
        Self {
            runtime,
            config,
            lesson_revalidation_store: None,
        }
    }

    pub fn with_lesson_revalidation(mut self, store: Store) -> Self {
        self.lesson_revalidation_store = Some(store);
        self
    }

    /// Recover work owned by a process that stopped without finishing its
    /// leases.  It is safe to call before every pool start and is idempotent.
    pub async fn recover_abandoned(&self) -> Result<u64, RuntimeError> {
        self.runtime.recover_expired_tasks(Utc::now()).await
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
        let runtime = self.runtime.clone();
        let lesson_revalidation_store = self.lesson_revalidation_store.clone();
        self.serve_with_recovery(handler, shutdown, move || {
            recover_expired_tasks(runtime.clone(), lesson_revalidation_store.clone())
        })
        .await
    }

    async fn serve_with_recovery<F, Fut>(
        &self,
        handler: TaskHandler,
        shutdown: watch::Receiver<bool>,
        mut recover: F,
    ) -> Result<(), RuntimeError>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<u64, RuntimeError>> + Send + 'static,
    {
        recover().await?;
        let recovery_interval = self.runtime.recovery_interval()?;
        let mut workers = tokio::task::JoinSet::new();
        workers.spawn(recovery_loop(recover, recovery_interval, shutdown.clone()));
        for index in 0..self.config.normalized_worker_count() {
            let runtime = self.runtime.clone();
            let handler = handler.clone();
            let shutdown = shutdown.clone();
            let worker_id = format!("{}-{index}", self.config.worker_prefix);
            let idle_poll = self.config.idle_poll;
            let reserved = if self.config.normalized_worker_count() == 1 {
                akzio_store::TaskWorkload::Any
            } else if index == 0 {
                akzio_store::TaskWorkload::Session
            } else if index == 1 {
                akzio_store::TaskWorkload::Outcome
            } else {
                akzio_store::TaskWorkload::Any
            };
            workers.spawn(async move {
                worker_loop(runtime, worker_id, handler, shutdown, idle_poll, reserved).await
            });
        }

        while let Some(result) = workers.join_next().await {
            result.map_err(|error| {
                RuntimeError::Store(StoreError::Integrity(format!(
                    "worker task panicked: {error}"
                )))
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

async fn recover_expired_tasks(
    runtime: TaskRuntime,
    lesson_revalidation_store: Option<Store>,
) -> Result<u64, RuntimeError> {
    let recovered = runtime.recover_expired_tasks(Utc::now()).await?;
    if let Some(store) = lesson_revalidation_store {
        tokio::task::spawn_blocking(move || store.contest_lessons_due_for_revalidation(Utc::now()))
            .await
            .map_err(|error| {
                RuntimeError::Store(StoreError::Integrity(format!(
                    "lesson revalidation maintenance panicked: {error}"
                )))
            })??;
    }
    Ok(recovered)
}

async fn recovery_loop<F, Fut>(
    mut recover: F,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RuntimeError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<u64, RuntimeError>>,
{
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                recover().await?;
            }
        }
    }
}

async fn worker_loop(
    runtime: TaskRuntime,
    worker_id: String,
    handler: TaskHandler,
    mut shutdown: watch::Receiver<bool>,
    idle_poll: Duration,
    reserved: akzio_store::TaskWorkload,
) -> Result<(), RuntimeError> {
    let mut prefer_outcome = false;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        // Shared workers alternate preference and can use the other queue when
        // idle. Reserved workers provide capacity while the opposite class blocks.
        let preferred = if reserved == akzio_store::TaskWorkload::Any {
            prefer_outcome = !prefer_outcome;
            if prefer_outcome {
                akzio_store::TaskWorkload::Outcome
            } else {
                akzio_store::TaskWorkload::Session
            }
        } else {
            reserved
        };
        let result = runtime
            .run_one_for_workload(&worker_id, preferred, |task| handler(task))
            .await;
        let result = if matches!(result, Ok(false)) && reserved == akzio_store::TaskWorkload::Any {
            runtime.run_one(&worker_id, |task| handler(task)).await
        } else {
            result
        };
        let did_run = match result {
            Ok(did_run) => did_run,
            Err(RuntimeError::Store(StoreError::StalePermit(task_id))) => {
                tracing::warn!(%worker_id, %task_id, "task lease superseded; worker remains available");
                false
            }
            Err(error) => return Err(error),
        };
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
