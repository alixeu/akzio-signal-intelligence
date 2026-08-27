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
    pub fn recover_abandoned(&self) -> Result<u64, RuntimeError> {
        self.runtime.recover_expired_tasks(Utc::now())
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
        Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance,
        FailureDisposition, RetryPolicy, RunId, RunPurpose, TaskBudget, TaskId, TaskRecipeId,
        TaskStatus, WorkflowGraph, WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
    };
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
        let handler: TaskHandler = {
            let completed = completed.clone();
            let handler_store = store.clone();
            Arc::new(move |task| {
                let completed = completed.clone();
                let store = handler_store.clone();
                Box::pin(async move {
                    let now = Utc::now();
                    let artifact = Artifact::new(
                        ArtifactKind::AgentTurn,
                        store
                            .put_bytes(b"fixture worker completion", "text/plain")
                            .unwrap(),
                        "fixture.worker",
                        ArtifactLifecycle::RunScoped,
                        ArtifactProvenance {
                            source_family: "fixture.worker".to_owned(),
                            observed_at: Some(now),
                            retrieved_at: now,
                            source_uri: None,
                            confidence_ppm: 1_000_000,
                            producer_contract_hash: task.permit.contract_hash.clone(),
                        },
                        Some(ArtifactOrigin {
                            run_id: Some(task.run_id.clone()),
                            task_id: Some(task.node.task_id.clone()),
                            attempt_id: Some(task.permit.attempt_id.clone()),
                            contract_hash: task.permit.contract_hash.clone(),
                        }),
                        vec![],
                        now,
                    )
                    .unwrap();
                    store
                        .commit_attempt(&task.permit, &[artifact], TaskStatus::Succeeded, now)
                        .unwrap();
                    completed.fetch_add(1, Ordering::SeqCst);
                    TaskCompletion::Committed
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
            2
        );
    }
}
