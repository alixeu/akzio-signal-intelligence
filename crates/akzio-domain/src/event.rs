//! Append-only durable event envelope.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::ArtifactRef, ids::EventId, AttemptId, DomainError, RunId, TaskId,
    V2_DOMAIN_SCHEMA_VERSION,
};

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

    use super::{DurableEvent, DurableEventKind};
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
}
