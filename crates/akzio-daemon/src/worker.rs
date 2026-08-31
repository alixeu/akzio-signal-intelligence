//! Durable local worker pool.
//!
//! The pool owns no business policy.  It only turns SQLite-backed task leases
//! into concurrently running handlers and stops cleanly when its supervisor
//! asks it to.  `TaskRuntime` remains the only owner of attempts, heartbeats,
//! timeouts, retries, and terminal task events.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use akzio_runtime::v2::{RuntimeError, TaskCompletion, TaskRuntime};
use akzio_store::v2::{ClaimedAttempt, StoreError};
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
}

impl WorkerPool {
    pub fn new(runtime: TaskRuntime, config: WorkerPoolConfig) -> Self {
        Self { runtime, config }
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
        self.serve_with_recovery(handler, shutdown, move || {
            recover_expired_tasks(runtime.clone())
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
            workers.spawn(async move {
                worker_loop(runtime, worker_id, handler, shutdown, idle_poll).await
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

async fn recover_expired_tasks(runtime: TaskRuntime) -> Result<u64, RuntimeError> {
    runtime.recover_expired_tasks(Utc::now()).await
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
) -> Result<(), RuntimeError> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        let did_run = runtime.run_one(&worker_id, |task| handler(task)).await?;
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

    use akzio_domain::{
        Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, FailureDisposition,
        RetryPolicy, RunId, RunPurpose, TaskBudget, TaskId, TaskRecipeId, WorkflowGraph,
        WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
    };
    use akzio_runtime::v2::{StoreExecutor, StoreMaintenanceKind};
    use akzio_store::v2::{StoredRun, V2Store, WorkflowCommit};
    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 64,
            max_output_tokens: 64,
            max_wall_time_secs: 30,
            max_tool_calls: 1,
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: false,
            retry_rate_limited: false,
            retry_invalid_output: false,
        }
    }

    fn workflow() -> WorkflowGraph {
        WorkflowGraph {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "worker-pool-fixture".to_owned(),
            nodes: (0..4)
                .map(|index| WorkflowNode {
                    task_id: TaskId::new(),
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    contract_hash: None,
                    objective: format!("fixture task {index}"),
                    dependencies: vec![],
                    input_artifacts: vec![],
                    priority: 100,
                    budget: budget(),
                    retry: retry(),
                    on_failure: FailureDisposition::FailRun,
                    parent_task_id: None,
                })
                .collect(),
        }
    }

    fn provenance(now: chrono::DateTime<Utc>) -> ArtifactProvenance {
        ArtifactProvenance {
            source_family: "fixture.worker".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        }
    }

    #[tokio::test]
    async fn pool_runs_ready_tasks_across_workers_and_stops_cleanly() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let now = Utc::now();
        let graph = workflow();
        let run_id = RunId::new();
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture.workflow",
            ArtifactLifecycle::RunScoped,
            provenance(now),
            None,
            vec![],
            now,
        )
        .unwrap();
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id: run_id.clone(),
                    purpose: RunPurpose::Debug,
                    topology_id: graph.topology_id.clone(),
                    graph_artifact_id: graph_artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();

        let completed = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let handler: TaskHandler = {
            let completed = completed.clone();
            let active = active.clone();
            let maximum_active = maximum_active.clone();
            Arc::new(move |task| {
                let completed = completed.clone();
                let active = active.clone();
                let maximum_active = maximum_active.clone();
                Box::pin(async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum_active.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                    drop(task);
                    TaskCompletion::NoOutput
                })
            })
        };
        let pool = WorkerPool::new(
            TaskRuntime::new(store.clone())
                .with_lease_duration(chrono::Duration::milliseconds(30))
                .unwrap(),
            WorkerPoolConfig {
                worker_count: 4,
                idle_poll: Duration::from_millis(5),
                worker_prefix: "test".to_owned(),
            },
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn({
            let pool = pool.clone();
            async move { pool.serve(handler, shutdown_rx).await }
        });

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while completed.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 4);
        assert_eq!(maximum_active.load(Ordering::SeqCst), 4);
        assert_eq!(
            store
                .events_after(&run_id, 0, 100)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "task.succeeded")
                .count(),
            4
        );
        assert!(store.verify_integrity().is_ok());
        assert_eq!(
            pool.worker_ids().into_iter().collect::<BTreeSet<_>>().len(),
            4
        );
    }

    #[tokio::test]
    async fn drained_maintenance_preserves_heartbeat_and_blocks_false_recovery() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path().join("store")).unwrap();
        let backup_root = directory.path().join("backup");
        let now = Utc::now();
        let graph = workflow();
        let run_id = RunId::new();
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture.workflow",
            ArtifactLifecycle::RunScoped,
            provenance(now),
            None,
            vec![],
            now,
        )
        .unwrap();
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id: run_id.clone(),
                    purpose: RunPurpose::Debug,
                    topology_id: graph.topology_id.clone(),
                    graph_artifact_id: graph_artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let executor = StoreExecutor::new(store.clone());
        let runtime = TaskRuntime::new(store.clone())
            .with_store_executor(executor.clone())
            .with_lease_duration(chrono::Duration::milliseconds(30))
            .unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let worker_runtime = runtime.clone();
        let worker = tokio::spawn(async move {
            worker_runtime
                .run_one("maintenance-worker", move |_| async move {
                    started_tx.send(()).unwrap();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    TaskCompletion::NoOutput
                })
                .await
        });
        started_rx.await.unwrap();

        executor
            .execute_maintenance(StoreMaintenanceKind::Backup, move |store| {
                std::thread::sleep(Duration::from_millis(70));
                store.backup_to(backup_root)?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(runtime.recover_expired_tasks(Utc::now()).await.unwrap(), 0);
        assert!(worker.await.unwrap().unwrap());
        assert!(!store
            .events_after(&run_id, 0, 100)
            .unwrap()
            .iter()
            .any(|event| event.event_type == "task.recovered"));
        store.verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn idle_workers_do_not_drive_recovery() {
        let directory = tempdir().unwrap();
        let runtime = TaskRuntime::new(V2Store::open(directory.path()).unwrap())
            .with_lease_duration(chrono::Duration::seconds(3))
            .unwrap();
        let pool = WorkerPool::new(
            runtime,
            WorkerPoolConfig {
                worker_count: 8,
                idle_poll: Duration::from_millis(1),
                worker_prefix: "idle".to_owned(),
            },
        );
        let recoveries = Arc::new(AtomicUsize::new(0));
        let observed = recoveries.clone();
        let handler: TaskHandler = Arc::new(|_| Box::pin(async { TaskCompletion::Failed }));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            pool.serve_with_recovery(handler, shutdown_rx, move || {
                let recoveries = recoveries.clone();
                async move {
                    recoveries.fetch_add(1, Ordering::SeqCst);
                    Ok(0)
                }
            })
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while observed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();

        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn recovery_ticker_requeues_expired_tasks() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let mut graph = workflow();
        graph
            .nodes
            .iter_mut()
            .for_each(|node| node.retry.max_attempts = 2);
        let run_id = RunId::new();
        let now = Utc::now();
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "fixture.workflow",
            ArtifactLifecycle::RunScoped,
            provenance(now),
            None,
            vec![],
            now,
        )
        .unwrap();
        store
            .commit_workflow(&WorkflowCommit {
                run: StoredRun {
                    run_id: run_id.clone(),
                    purpose: RunPurpose::Debug,
                    topology_id: graph.topology_id.clone(),
                    graph_artifact_id: graph_artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        store
            .claim_next_task("crashed", now, chrono::Duration::milliseconds(1))
            .unwrap()
            .unwrap();

        let completed = Arc::new(AtomicUsize::new(0));
        let handler: TaskHandler = {
            let completed = completed.clone();
            Arc::new(move |_| {
                let completed = completed.clone();
                Box::pin(async move {
                    completed.fetch_add(1, Ordering::SeqCst);
                    TaskCompletion::NoOutput
                })
            })
        };
        let pool = WorkerPool::new(
            TaskRuntime::new(store.clone())
                .with_lease_duration(chrono::Duration::milliseconds(30))
                .unwrap(),
            WorkerPoolConfig {
                worker_count: 4,
                idle_poll: Duration::from_millis(1),
                worker_prefix: "recovery".to_owned(),
            },
        );
        let recovery_calls = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn({
            let recovery_calls = recovery_calls.clone();
            let recovery_store = store.clone();
            async move {
                pool.serve_with_recovery(handler, shutdown_rx, move || {
                    let call = recovery_calls.fetch_add(1, Ordering::SeqCst);
                    let store = recovery_store.clone();
                    async move {
                        let at = now + chrono::Duration::milliseconds(i64::from(call > 0) * 2);
                        Ok(store.recover_expired_tasks(at)?)
                    }
                })
                .await
            }
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();

        assert_eq!(
            store
                .events_after(&run_id, 0, 100)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "task.recovered")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn recovery_error_stops_the_pool() {
        let directory = tempdir().unwrap();
        let runtime = TaskRuntime::new(V2Store::open(directory.path()).unwrap())
            .with_lease_duration(chrono::Duration::milliseconds(3))
            .unwrap();
        let pool = WorkerPool::new(runtime, WorkerPoolConfig::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let handler: TaskHandler = Arc::new(|_| Box::pin(async { TaskCompletion::Failed }));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let result = tokio::time::timeout(Duration::from_secs(1), {
            let calls = calls.clone();
            pool.serve_with_recovery(handler, shutdown_rx, move || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call == 0 {
                        Ok(0)
                    } else {
                        Err(RuntimeError::Store(StoreError::Integrity(
                            "fixture recovery failure".to_owned(),
                        )))
                    }
                }
            })
        })
        .await
        .unwrap();

        assert!(matches!(
            result,
            Err(RuntimeError::Store(StoreError::Integrity(message)))
                if message == "fixture recovery failure"
        ));
    }
}
