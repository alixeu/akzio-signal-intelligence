//! Durable Paper outcome scheduling.
//!
//! Scheduling is deliberately separate from outcome materialization: a Paper
//! terminal chain records the exact decision/execution lineage now, while only
//! later governed market observations may seal an `Outcome` and affect policy.

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    Decision, DecisionContext, DomainError, ExecutionContext, ExecutionVerdict,
    OutcomeExecutionLineage, OutcomeId, OutcomeSchedule, PaperCommitment, Reconciliation,
    RunPurpose, TaskStatus, TaskWritePermit, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, NaiveDate, Utc};
use serde::de::DeserializeOwned;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OutcomeScheduleError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("outcome schedule requires Paper run, got {0:?}")]
    NonPaperRun(RunPurpose),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("outcome schedule artifact lineage is invalid: {0}")]
    InvalidLineage(&'static str),
}

pub type OutcomeScheduleResult<T> = std::result::Result<T, OutcomeScheduleError>;

#[derive(Debug, Clone)]
pub struct OutcomeScheduleInput {
    pub permit: TaskWritePermit,
    pub decision: ArtifactRef,
    pub decision_context: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub execution: OutcomeExecutionLineage,
    pub baseline_trading_day: NaiveDate,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct OutcomeScheduleOutput {
    pub schedule: Artifact,
}

/// Owns the immutable schedule written after a Paper terminal chain. It does
/// not materialize outcomes, mutate memory, or select policy transitions.
#[derive(Debug, Clone)]
pub struct OutcomeSchedulingRuntime {
    store: V2Store,
    enqueue_worker: bool,
}

impl OutcomeSchedulingRuntime {
    pub fn new(store: V2Store) -> Self {
        Self {
            store,
            enqueue_worker: false,
        }
    }

    pub fn with_worker_enabled(mut self, enabled: bool) -> Self {
        self.enqueue_worker = enabled;
        self
    }

    pub fn schedule(
        &self,
        input: &OutcomeScheduleInput,
    ) -> OutcomeScheduleResult<OutcomeScheduleOutput> {
        self.store.validate_task_permit(&input.permit)?;
        let purpose = self.store.run_purpose(&input.permit.run_id)?;
        if purpose != RunPurpose::Paper {
            return Err(OutcomeScheduleError::NonPaperRun(purpose));
        }

        let decision = self.load_expected(&input.decision, ArtifactKind::Decision)?;
        let decision_payload: Decision = self.read_payload(&decision)?;
        decision_payload.validate()?;
        if decision_payload.decision_context != input.decision_context
            || !decision.source_refs.contains(&input.decision_context)
        {
            return Err(OutcomeScheduleError::InvalidLineage("decision_context"));
        }

        let context = self.load_expected(&input.decision_context, ArtifactKind::DecisionContext)?;
        let context_payload: DecisionContext = self.read_payload(&context)?;
        context_payload.validate()?;
        if context_payload.run_id != input.permit.run_id {
            return Err(OutcomeScheduleError::InvalidLineage("decision_run"));
        }

        let execution_context =
            self.load_expected(&input.execution_context, ArtifactKind::ExecutionContext)?;
        let execution_context_payload: ExecutionContext = self.read_payload(&execution_context)?;
        execution_context_payload.validate()?;
        if execution_context_payload.run_id != input.permit.run_id
            || execution_context_payload.decision_context != input.decision_context
            || !execution_context
                .source_refs
                .contains(&input.decision_context)
        {
            return Err(OutcomeScheduleError::InvalidLineage("execution_context"));
        }

        self.validate_execution_lineage(&input.execution, &input.execution_context)?;
        let payload = OutcomeSchedule {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            decision: input.decision.clone(),
            decision_context: input.decision_context.clone(),
            execution_context: input.execution_context.clone(),
            execution: input.execution.clone(),
            baseline_trading_day: input.baseline_trading_day,
            created_at: input.now,
        };
        payload.validate()?;

        let mut source_refs = vec![
            input.decision.clone(),
            input.decision_context.clone(),
            input.execution_context.clone(),
        ];
        match &input.execution {
            OutcomeExecutionLineage::NoOrder { execution_verdict } => {
                source_refs.push(execution_verdict.clone());
            }
            OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict,
                commitment,
                reconciliation,
            } => {
                source_refs.extend([
                    execution_verdict.clone(),
                    commitment.clone(),
                    reconciliation.clone(),
                ]);
            }
        }
        let schedule = Artifact::new(
            ArtifactKind::OutcomeSchedule,
            self.store.put_json(&payload)?,
            "learning.outcome_schedule",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio-learning".to_owned(),
                observed_at: Some(input.now),
                retrieved_at: input.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: input.permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(input.permit.run_id.clone()),
                task_id: Some(input.permit.task_id.clone()),
                attempt_id: Some(input.permit.attempt_id.clone()),
                contract_hash: input.permit.contract_hash.clone(),
            }),
            source_refs,
            input.now,
        )?;
        Ok(OutcomeScheduleOutput { schedule })
    }

    pub fn commit(
        &self,
        permit: &TaskWritePermit,
        output: &OutcomeScheduleOutput,
        now: DateTime<Utc>,
    ) -> OutcomeScheduleResult<()> {
        if self.enqueue_worker {
            self.store
                .commit_outcome_schedule_with_worker(permit, &output.schedule, now)?;
        } else {
            self.store.commit_attempt(
                permit,
                std::slice::from_ref(&output.schedule),
                TaskStatus::Succeeded,
                now,
            )?;
        }
        Ok(())
    }

    fn validate_execution_lineage(
        &self,
        lineage: &OutcomeExecutionLineage,
        execution_context: &ArtifactRef,
    ) -> OutcomeScheduleResult<()> {
        match lineage {
            OutcomeExecutionLineage::NoOrder { execution_verdict } => {
                let verdict =
                    self.load_expected(execution_verdict, ArtifactKind::ExecutionVerdict)?;
                let payload: ExecutionVerdict = self.read_payload(&verdict)?;
                payload.validate()?;
                let ExecutionVerdict::NoOrder { no_order } = payload else {
                    return Err(OutcomeScheduleError::InvalidLineage("no_order_verdict"));
                };
                if no_order.execution_context != *execution_context
                    || !verdict.source_refs.contains(execution_context)
                {
                    return Err(OutcomeScheduleError::InvalidLineage("no_order_context"));
                }
            }
            OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict,
                commitment,
                reconciliation,
            } => {
                let verdict =
                    self.load_expected(execution_verdict, ArtifactKind::ExecutionVerdict)?;
                let payload: ExecutionVerdict = self.read_payload(&verdict)?;
                payload.validate()?;
                let ExecutionVerdict::Accepted {
                    execution_context: accepted_context,
                } = payload
                else {
                    return Err(OutcomeScheduleError::InvalidLineage("accepted_verdict"));
                };
                if accepted_context != *execution_context
                    || !verdict.source_refs.contains(execution_context)
                {
                    return Err(OutcomeScheduleError::InvalidLineage("accepted_context"));
                }

                let commitment_artifact =
                    self.load_expected(commitment, ArtifactKind::ExecutionCommitment)?;
                let commitment_payload: PaperCommitment =
                    self.read_payload(&commitment_artifact)?;
                commitment_payload.validate()?;
                if commitment_payload.execution_context != *execution_context
                    || !commitment_artifact.source_refs.contains(execution_verdict)
                {
                    return Err(OutcomeScheduleError::InvalidLineage("commitment"));
                }

                let reconciliation_artifact =
                    self.load_expected(reconciliation, ArtifactKind::Reconciliation)?;
                let reconciliation_payload: Reconciliation =
                    self.read_payload(&reconciliation_artifact)?;
                reconciliation_payload.validate()?;
                if reconciliation_payload.commitment != *commitment
                    || !reconciliation_artifact.source_refs.contains(commitment)
                {
                    return Err(OutcomeScheduleError::InvalidLineage("reconciliation"));
                }
            }
        }
        Ok(())
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> OutcomeScheduleResult<Artifact> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(OutcomeScheduleError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }

    fn read_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> OutcomeScheduleResult<T> {
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }
}
