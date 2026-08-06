//! Task router.  Business transitions live in focused sibling modules.

mod decision_execution;
mod learning_shadow;
mod research_ingest;
mod value;

use akzio_context::{ContextBroker, NewJsonDocument};
use akzio_domain::{
    DocumentKind, DocumentLifecycle, DocumentOrigin, DocumentRecord, RunId, TaskKind,
};
use akzio_research::execute_research_task;
use akzio_runtime::TaskCompletion;
use akzio_store::ClaimedTask;
use chrono::{DateTime, Duration, Utc};

use crate::{Daemon, DaemonError, Result};

impl Daemon {
    pub(crate) async fn execute_task(&self, task: ClaimedTask) -> TaskCompletion {
        let now = Utc::now();
        match self.execute_task_inner(&task, now).await {
            Ok(()) => TaskCompletion::Succeeded,
            Err(error) if error.is_retryable() => TaskCompletion::Retry {
                retry_at: now + Duration::seconds(1),
                error: error.to_string(),
            },
            Err(error) => TaskCompletion::Failed {
                error: error.to_string(),
            },
        }
    }
    async fn execute_task_inner(&self, task: &ClaimedTask, now: DateTime<Utc>) -> Result<()> {
        let purpose = self.store.run_purpose(&task.run_id)?;
        let broker = ContextBroker::new(self.store.clone());
        let origin = task_origin(task);
        match task.kind {
            TaskKind::Ingest => self.seal_research_input(&broker, task, purpose, now).await,
            TaskKind::MemoryOverlay => {
                self.record_memory_overlay(&broker, &task.run_id, origin, now)
            }
            TaskKind::Plan
            | TaskKind::Investigate
            | TaskKind::Challenge
            | TaskKind::SynthesizeDecision => {
                execute_research_task(
                    &broker,
                    &self.workflow,
                    &self.model,
                    &self.contracts,
                    task,
                    now,
                )
                .await?;
                Ok(())
            }
            TaskKind::DecisionGate => {
                self.finalize_decision(&broker, &task.run_id, purpose, origin, now)
            }
            TaskKind::ExecutionGate => {
                self.build_execution_plan(&broker, &task.run_id, purpose, origin.clone(), now)
                    .await?;
                if purpose == akzio_domain::RunPurpose::Shadow {
                    self.complete_shadow_pair(&broker, &task.run_id, origin, now)?;
                }
                Ok(())
            }
            TaskKind::ExecutePaper => {
                self.submit_paper(&broker, &task.run_id, purpose, origin, now)
                    .await
            }
            TaskKind::Reconcile => {
                self.reconcile_paper(&broker, &task.run_id, purpose, origin, now)
                    .await
            }
            TaskKind::Evaluate => {
                self.create_experience_and_schedule(&broker, &task.run_id, purpose, origin, now)
            }
            TaskKind::Shadow => self.spawn_shadow_run(&broker, &task.run_id, purpose, origin, now),
        }?;
        self.workflow.advance_after_task(task, now)?;
        Ok(())
    }
    fn record_task_result(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        origin: DocumentOrigin,
        outcome: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        broker.record_json(NewJsonDocument {
            kind: DocumentKind::TaskResult,
            producer: "daemon.dispatch".to_owned(),
            run_id: Some(run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: vec![],
            origin: Some(origin),
            value: &serde_json::json!({"outcome": outcome}),
            created_at: now,
        })?;
        Ok(())
    }
    fn first_document(&self, run_id: &RunId, kind: DocumentKind) -> Result<DocumentRecord> {
        self.store
            .documents_for_run(run_id)?
            .into_iter()
            .filter(|document| document.kind == kind)
            .min_by(|left, right| {
                (left.created_at, &left.document_id).cmp(&(right.created_at, &right.document_id))
            })
            .ok_or_else(|| DaemonError::MissingRunDocument {
                run_id: run_id.clone(),
                kind,
            })
    }
    fn latest_document(&self, run_id: &RunId, kind: DocumentKind) -> Result<DocumentRecord> {
        self.latest_document_optional(run_id, kind)?.ok_or_else(|| {
            DaemonError::MissingRunDocument {
                run_id: run_id.clone(),
                kind,
            }
        })
    }
    fn latest_document_optional(
        &self,
        run_id: &RunId,
        kind: DocumentKind,
    ) -> Result<Option<DocumentRecord>> {
        Ok(self
            .store
            .documents_for_run(run_id)?
            .into_iter()
            .filter(|document| document.kind == kind)
            .max_by(|left, right| {
                (left.created_at, &left.document_id).cmp(&(right.created_at, &right.document_id))
            }))
    }
    fn source_document(
        &self,
        document: &DocumentRecord,
        kind: DocumentKind,
    ) -> Result<DocumentRecord> {
        for source_id in &document.source_refs {
            let source = self.store.read_document(source_id)?;
            if source.kind == kind {
                return Ok(source);
            }
        }
        Err(DaemonError::MissingRunDocument {
            run_id: document.run_id.clone().unwrap_or_else(RunId::new),
            kind,
        })
    }
}

pub(super) fn task_origin(task: &ClaimedTask) -> DocumentOrigin {
    DocumentOrigin::task(
        task.task_id.clone(),
        task.attempt_id.clone(),
        task.contract_hash.clone(),
    )
}
