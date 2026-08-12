//! Append-only durable event envelope.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::ArtifactRef, ids::EventId, AttemptId, DomainError, RunId, TaskId,
    V2_DOMAIN_SCHEMA_VERSION,
};

/// Rust-owned allowlist for the string event column used by the v2 Store.
///
/// The SQLite schema keeps its historical string representation for replay
/// and HTTP compatibility; this type is the validation boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEventType {
    AgentTurn,
    AgentTurnCompleted,
    AgentTurnFailed,
    AgentTurnRetryableFailed,
    ArtifactCommitted,
    ClaimCreated,
    ContextChildManifestCreated,
    ContextManifest,
    ContextManifestCreated,
    ContextRepaired,
    Evidence,
    EvidenceNormalized,
    EvidenceRaw,
    ExecutionAllocationCreated,
    ExecutionCommitted,
    ExecutionCommitmentRecovered,
    ExecutionContextCreated,
    ExecutionContextCreatedLegacy,
    ExecutionPlanCreated,
    ExecutionRepriceCommitted,
    ExecutionRepriceRecovered,
    ExecutionVerdictCreated,
    ExecutionVerdictNoOrder,
    ExecutionVerdictCreatedLegacy,
    FixtureExecutionSourceCreated,
    FixtureGenericWrite,
    FixtureSourceCreated,
    LearningOverlay,
    OutcomeEvidence,
    OutcomeNeed,
    OutcomeWorkerEnqueued,
    PaperSeedArtifactCreated,
    PlannerEvidenceNeed,
    PlannerEvidenceNeedCreated,
    PolicyEvaluated,
    PolicyTransitioned,
    RunCancelRequested,
    ShadowDecisionCreated,
    ShadowOutcomeScheduleCreated,
    ShadowPairCompleted,
    TaskCancelled,
    TaskFailed,
    TaskRecovered,
    TaskRecoveryExhausted,
    TaskRetryExhausted,
    TaskRetryScheduled,
    TaskSkipped,
    TaskStarted,
    TaskSucceeded,
    ToolCalled,
    ToolCompleted,
    ToolFailed,
    WorkflowCreated,
    WorkflowPatched,
}

impl LifecycleEventType {
    pub fn parse(value: &str) -> Result<Self, DomainError> {
        let event = match value {
            "agent.turn" => Self::AgentTurn,
            "agent.turn_completed" => Self::AgentTurnCompleted,
            "agent.turn_failed" => Self::AgentTurnFailed,
            "agent.turn_retryable_failed" => Self::AgentTurnRetryableFailed,
            "artifact.committed" => Self::ArtifactCommitted,
            "claim.created" => Self::ClaimCreated,
            "context.child_manifest_created" => Self::ContextChildManifestCreated,
            "context.manifest" => Self::ContextManifest,
            "context.manifest_created" => Self::ContextManifestCreated,
            "context.repaired" => Self::ContextRepaired,
            "evidence" => Self::Evidence,
            "evidence.normalized" => Self::EvidenceNormalized,
            "evidence.raw" => Self::EvidenceRaw,
            "execution.allocation_created" => Self::ExecutionAllocationCreated,
            "execution.committed" => Self::ExecutionCommitted,
            "execution.commitment.recovered" => Self::ExecutionCommitmentRecovered,
            "execution.context.created" => Self::ExecutionContextCreated,
            "execution.context_created" => Self::ExecutionContextCreatedLegacy,
            "execution.plan.created" => Self::ExecutionPlanCreated,
            "execution.reprice.committed" => Self::ExecutionRepriceCommitted,
            "execution.reprice.recovered" => Self::ExecutionRepriceRecovered,
            "execution.verdict.created" => Self::ExecutionVerdictCreated,
            "execution.verdict.no_order" => Self::ExecutionVerdictNoOrder,
            "execution.verdict_created" => Self::ExecutionVerdictCreatedLegacy,
            "fixture.execution_source_created" => Self::FixtureExecutionSourceCreated,
            "fixture.generic_write" => Self::FixtureGenericWrite,
            "fixture.source.created" => Self::FixtureSourceCreated,
            "learning.overlay" => Self::LearningOverlay,
            "outcome.evidence" => Self::OutcomeEvidence,
            "outcome.need" => Self::OutcomeNeed,
            "outcome.worker.enqueued" => Self::OutcomeWorkerEnqueued,
            "paper.seed_artifact.created" => Self::PaperSeedArtifactCreated,
            "planner.evidence_need" => Self::PlannerEvidenceNeed,
            "planner.evidence_need_created" => Self::PlannerEvidenceNeedCreated,
            "policy.evaluated" => Self::PolicyEvaluated,
            "policy.transitioned" => Self::PolicyTransitioned,
            "run.cancel_requested" => Self::RunCancelRequested,
            "shadow.decision.created" => Self::ShadowDecisionCreated,
            "shadow.outcome_schedule.created" => Self::ShadowOutcomeScheduleCreated,
            "shadow_pair.completed" => Self::ShadowPairCompleted,
            "task.cancelled" => Self::TaskCancelled,
            "task.failed" => Self::TaskFailed,
            "task.recovered" => Self::TaskRecovered,
            "task.recovery_exhausted" => Self::TaskRecoveryExhausted,
            "task.retry_exhausted" => Self::TaskRetryExhausted,
            "task.retry_scheduled" => Self::TaskRetryScheduled,
            "task.skipped" => Self::TaskSkipped,
            "task.started" => Self::TaskStarted,
            "task.succeeded" => Self::TaskSucceeded,
            "tool.called" => Self::ToolCalled,
            "tool.completed" => Self::ToolCompleted,
            "tool.failed" => Self::ToolFailed,
            "workflow.created" => Self::WorkflowCreated,
            "workflow.patched" => Self::WorkflowPatched,
            other => return Err(DomainError::UnknownLifecycleEventType(other.to_owned())),
        };
        Ok(event)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AgentTurn => "agent.turn",
            Self::AgentTurnCompleted => "agent.turn_completed",
            Self::AgentTurnFailed => "agent.turn_failed",
            Self::AgentTurnRetryableFailed => "agent.turn_retryable_failed",
            Self::ArtifactCommitted => "artifact.committed",
            Self::ClaimCreated => "claim.created",
            Self::ContextChildManifestCreated => "context.child_manifest_created",
            Self::ContextManifest => "context.manifest",
            Self::ContextManifestCreated => "context.manifest_created",
            Self::ContextRepaired => "context.repaired",
            Self::Evidence => "evidence",
            Self::EvidenceNormalized => "evidence.normalized",
            Self::EvidenceRaw => "evidence.raw",
            Self::ExecutionAllocationCreated => "execution.allocation_created",
            Self::ExecutionCommitted => "execution.committed",
            Self::ExecutionCommitmentRecovered => "execution.commitment.recovered",
            Self::ExecutionContextCreated => "execution.context.created",
            Self::ExecutionContextCreatedLegacy => "execution.context_created",
            Self::ExecutionPlanCreated => "execution.plan.created",
            Self::ExecutionRepriceCommitted => "execution.reprice.committed",
            Self::ExecutionRepriceRecovered => "execution.reprice.recovered",
            Self::ExecutionVerdictCreated => "execution.verdict.created",
            Self::ExecutionVerdictNoOrder => "execution.verdict.no_order",
            Self::ExecutionVerdictCreatedLegacy => "execution.verdict_created",
            Self::FixtureExecutionSourceCreated => "fixture.execution_source_created",
            Self::FixtureGenericWrite => "fixture.generic_write",
            Self::FixtureSourceCreated => "fixture.source.created",
            Self::LearningOverlay => "learning.overlay",
            Self::OutcomeEvidence => "outcome.evidence",
            Self::OutcomeNeed => "outcome.need",
            Self::OutcomeWorkerEnqueued => "outcome.worker.enqueued",
            Self::PaperSeedArtifactCreated => "paper.seed_artifact.created",
            Self::PlannerEvidenceNeed => "planner.evidence_need",
            Self::PlannerEvidenceNeedCreated => "planner.evidence_need_created",
            Self::PolicyEvaluated => "policy.evaluated",
            Self::PolicyTransitioned => "policy.transitioned",
            Self::RunCancelRequested => "run.cancel_requested",
            Self::ShadowDecisionCreated => "shadow.decision.created",
            Self::ShadowOutcomeScheduleCreated => "shadow.outcome_schedule.created",
            Self::ShadowPairCompleted => "shadow_pair.completed",
            Self::TaskCancelled => "task.cancelled",
            Self::TaskFailed => "task.failed",
            Self::TaskRecovered => "task.recovered",
            Self::TaskRecoveryExhausted => "task.recovery_exhausted",
            Self::TaskRetryExhausted => "task.retry_exhausted",
            Self::TaskRetryScheduled => "task.retry_scheduled",
            Self::TaskSkipped => "task.skipped",
            Self::TaskStarted => "task.started",
            Self::TaskSucceeded => "task.succeeded",
            Self::ToolCalled => "tool.called",
            Self::ToolCompleted => "tool.completed",
            Self::ToolFailed => "tool.failed",
            Self::WorkflowCreated => "workflow.created",
            Self::WorkflowPatched => "workflow.patched",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableEventKind {
    WorkflowCreated,
    TaskClaimed,
    TaskRetried,
    TaskCompleted,
    ArtifactCreated,
    ContextManifested,
    ContextRepaired,
    AgentTurnStarted,
    AgentTurnCompleted,
    ToolCalled,
    ToolReturned,
    DecisionGated,
    ExecutionGated,
    PaperCommitted,
    PaperReconciled,
    OutcomeSealed,
    PolicyTransitioned,
    FreezeChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableEvent {
    pub schema_version: u32,
    pub event_id: EventId,
    pub kind: DurableEventKind,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<AttemptId>,
    pub payload: ArtifactRef,
    pub emitted_at: DateTime<Utc>,
}

impl DurableEvent {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "durable_event.schema_version",
            });
        }
        if self.event_id.0.trim().is_empty() || self.run_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "durable_event.identity",
            });
        }
        if self.attempt_id.is_some() && self.task_id.is_none() {
            return Err(DomainError::AttemptOriginWithoutTask);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{DurableEvent, DurableEventKind, LifecycleEventType};
    use crate::{
        artifact::{ArtifactId, ArtifactKind, ArtifactRef},
        ids::EventId,
        ContentHash, RunId,
    };

    #[test]
    fn event_with_attempt_requires_task() {
        let event = DurableEvent {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            event_id: EventId::new(),
            kind: DurableEventKind::TaskCompleted,
            run_id: RunId::new(),
            task_id: None,
            attempt_id: Some(crate::AttemptId::new()),
            payload: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"payload")),
                kind: ArtifactKind::AgentTurn,
            },
            emitted_at: Utc::now(),
        };

        assert_eq!(
            event.validate(),
            Err(crate::DomainError::AttemptOriginWithoutTask)
        );
    }

    #[test]
    fn lifecycle_event_types_round_trip_all_allowlisted_values() {
        let values = [
            "agent.turn",
            "agent.turn_completed",
            "agent.turn_failed",
            "agent.turn_retryable_failed",
            "artifact.committed",
            "claim.created",
            "context.child_manifest_created",
            "context.manifest",
            "context.manifest_created",
            "context.repaired",
            "evidence",
            "evidence.normalized",
            "evidence.raw",
            "execution.allocation_created",
            "execution.committed",
            "execution.commitment.recovered",
            "execution.context.created",
            "execution.context_created",
            "execution.plan.created",
            "execution.reprice.committed",
            "execution.reprice.recovered",
            "execution.verdict.created",
            "execution.verdict.no_order",
            "execution.verdict_created",
            "fixture.execution_source_created",
            "fixture.generic_write",
            "fixture.source.created",
            "learning.overlay",
            "outcome.evidence",
            "outcome.need",
            "outcome.worker.enqueued",
            "paper.seed_artifact.created",
            "planner.evidence_need",
            "planner.evidence_need_created",
            "policy.evaluated",
            "policy.transitioned",
            "run.cancel_requested",
            "shadow.decision.created",
            "shadow.outcome_schedule.created",
            "shadow_pair.completed",
            "task.cancelled",
            "task.failed",
            "task.recovered",
            "task.recovery_exhausted",
            "task.retry_exhausted",
            "task.retry_scheduled",
            "task.skipped",
            "task.started",
            "task.succeeded",
            "tool.called",
            "tool.completed",
            "tool.failed",
            "workflow.created",
            "workflow.patched",
        ];

        for value in values {
            assert_eq!(LifecycleEventType::parse(value).unwrap().as_str(), value);
        }
        assert!(matches!(
            LifecycleEventType::parse("unknown.event"),
            Err(crate::DomainError::UnknownLifecycleEventType(value))
                if value == "unknown.event"
        ));
    }
}
