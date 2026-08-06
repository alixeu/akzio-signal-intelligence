//! Memory overlay, outcome scheduling, and paired shadow launch.

use akzio_context::{ContextBroker, NewJsonDocument};
use akzio_domain::{
    DocumentKind, DocumentLifecycle, DocumentOrigin, PortfolioDecision, RunId, RunPurpose, TaskKind,
};
use akzio_learning::{LearningLedger, StoredMemory, TopologyLedger};
use akzio_research::{bootstrap_workflow, shadow_topology};
use chrono::{DateTime, Utc};

use crate::{Daemon, DaemonError, Result};

impl Daemon {
    pub(super) fn record_memory_overlay(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let prior_documents = LearningLedger::new(broker.clone()).research_prior_documents()?;
        let source_refs = prior_documents
            .iter()
            .map(|document| document.document_id.clone())
            .collect::<Vec<_>>();
        let priors = prior_documents
            .iter()
            .map(|document| broker.read_json(document))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        broker.record_json(NewJsonDocument {
            kind: DocumentKind::SemanticDetail,
            producer: "memory.overlay".to_owned(),
            run_id: Some(run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs,
            origin: Some(origin),
            value: &serde_json::json!({
                "kind": "research_priors",
                "memory": priors,
            }),
            created_at: now,
        })?;
        Ok(())
    }
    pub(super) fn create_experience_and_schedule(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        purpose: RunPurpose,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<()> {
        if purpose != RunPurpose::Paper {
            return self.record_task_result(broker, run_id, origin, "learning.noncanonical", now);
        }
        let decision_document = self.latest_document(run_id, DocumentKind::Decision)?;
        let execution_context = self.latest_document(run_id, DocumentKind::ExecutionContext)?;
        let decision: PortfolioDecision =
            serde_json::from_value(broker.read_json(&decision_document)?)?;
        let ledger = LearningLedger::for_task(broker.clone(), origin.clone());
        let existing_memory = self
            .store
            .documents_for_run(run_id)?
            .into_iter()
            .filter(|document| document.kind == DocumentKind::Memory)
            .max_by(|left, right| {
                (left.created_at, &left.document_id).cmp(&(right.created_at, &right.document_id))
            });
        let memory = if let Some(memory) = existing_memory {
            memory
        } else {
            let experience = broker.record_json(NewJsonDocument {
                kind: DocumentKind::Experience,
                producer: "learning.experience".to_owned(),
                run_id: Some(run_id.clone()),
                lifecycle: DocumentLifecycle::RunScoped,
                source_refs: vec![
                    decision_document.document_id.clone(),
                    execution_context.document_id.clone(),
                ],
                origin: Some(origin.clone()),
                value: &serde_json::json!({
                    "schema_version": 1,
                    "state": "awaiting_outcome",
                    "summary": decision.draft.summary,
                    "decision_id": decision.decision_id,
                }),
                created_at: now,
            })?;
            ledger.create_candidate(
                purpose,
                run_id,
                decision.draft.summary,
                vec![experience.document_id],
                now,
            )?
        };
        let stored: StoredMemory = serde_json::from_value(broker.read_json(&memory)?)?;
        let schedules = ledger.schedule_outcomes(
            purpose,
            run_id,
            &stored.item.memory_id,
            &memory.document_id,
            &decision_document.document_id,
            &execution_context.document_id,
            "unknown",
            now,
        )?;
        broker.record_json(NewJsonDocument {
            kind: DocumentKind::Evaluation,
            producer: "learning.schedule".to_owned(),
            run_id: Some(run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs: schedules
                .iter()
                .map(|document| document.document_id.clone())
                .collect(),
            origin: Some(origin),
            value: &serde_json::json!({
                "kind": "outcome_schedule",
                "memory_id": stored.item.memory_id,
                "horizons": [1, 3, 5],
            }),
            created_at: now,
        })?;
        Ok(())
    }
    pub(super) fn spawn_shadow_run(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        purpose: RunPurpose,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<()> {
        if purpose != RunPurpose::Paper {
            return self.record_task_result(broker, run_id, origin, "shadow.noncanonical", now);
        }
        let parent_topology = self.store.run_topology_id(run_id)?;
        let Some(candidate_topology) = shadow_topology(&parent_topology) else {
            self.record_task_result(broker, run_id, origin, "shadow.no_candidate", now)?;
            return Ok(());
        };
        let decision = self.latest_document(run_id, DocumentKind::Decision)?;
        let execution_context = self.latest_document(run_id, DocumentKind::ExecutionContext)?;
        let input = self.first_document(run_id, DocumentKind::NormalizedEvidence)?;
        let proposed = RunId::new();
        let (shadow_run_id, reserved) = self
            .store
            .reserve_child_run(run_id, "shadow", &proposed, now)?;
        if reserved || !self.store.run_exists(&shadow_run_id)? {
            let mut plan = bootstrap_workflow(
                RunPurpose::Shadow,
                candidate_topology.clone(),
                &self.contracts.installed(),
            );
            let ingest = plan
                .tasks
                .iter_mut()
                .find(|task| task.kind == TaskKind::Ingest)
                .expect("shadow bootstrap always contains ingest");
            ingest.input_refs = vec![input.document_id.clone()];
            self.workflow
                .submit(&shadow_run_id, RunPurpose::Shadow, plan, now)?;
        }
        let record = TopologyLedger::for_task(broker.clone(), origin.clone()).queue_shadow_pair(
            run_id,
            &shadow_run_id,
            parent_topology,
            candidate_topology,
            decision.document_id,
            execution_context.document_id,
            now,
        )?;
        self.store.append_event(&akzio_domain::EventEnvelope {
            schema_version: akzio_domain::V2_SCHEMA_VERSION,
            run_id: run_id.clone(),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
            causation_id: Some(shadow_run_id.0.clone()),
            event_type: "shadow.queued".to_owned(),
            payload_document_id: Some(record.document_id.clone()),
            payload: Some(record.blob),
            created_at: now,
        })?;
        Ok(())
    }

    pub(super) fn complete_shadow_pair(
        &self,
        broker: &ContextBroker,
        shadow_run_id: &RunId,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let parent_run_id = self
            .store
            .parent_run(shadow_run_id, "shadow")?
            .ok_or_else(|| {
                DaemonError::InvalidInput(format!("shadow run {shadow_run_id} has no parent"))
            })?;
        let candidate_decision = self.latest_document(shadow_run_id, DocumentKind::Decision)?;
        let record = TopologyLedger::for_task(broker.clone(), origin).complete_shadow_pair(
            shadow_run_id,
            candidate_decision.document_id,
            now,
        )?;
        self.store.append_event(&akzio_domain::EventEnvelope {
            schema_version: akzio_domain::V2_SCHEMA_VERSION,
            run_id: parent_run_id,
            task_id: None,
            attempt_id: None,
            contract_hash: None,
            causation_id: Some(shadow_run_id.0.clone()),
            event_type: "shadow.paired".to_owned(),
            payload_document_id: Some(record.document_id.clone()),
            payload: Some(record.blob),
            created_at: now,
        })?;
        Ok(())
    }
}
