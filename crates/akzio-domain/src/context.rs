//! Context-manifest and read-grant domain vocabulary.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact::{ArtifactId, ArtifactKind, ArtifactOrigin, ArtifactRef};
use crate::contract::ContextPolicy;
use crate::schema::V2_SCHEMA_VERSION;
use crate::{ContentHash, DomainError, LeaseId, RunId, TaskId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSelection {
    pub artifact: ArtifactRef,
    pub reason: String,
    pub estimated_tokens: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextManifestPayload {
    pub schema_version: u32,
    pub contract_hash: ContentHash,
    pub selections: Vec<ContextSelection>,
    pub total_bytes: u64,
    pub estimated_tokens: u32,
    pub input_hash: ContentHash,
}

impl ContextManifestPayload {
    pub fn validate(&self, policy: &ContextPolicy) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION
            || self.selections.len() < usize::from(policy.min_artifacts)
            || self.selections.len() > usize::from(policy.max_artifacts)
            || self.total_bytes > policy.max_bytes
            || self.estimated_tokens > policy.max_tokens
        {
            return Err(DomainError::InvalidBudget {
                field: "context_manifest",
            });
        }
        if self.selections.iter().any(|selection| {
            selection.reason.trim().is_empty()
                || selection.estimated_tokens == 0
                || !policy.permitted_kinds.contains(&selection.artifact.kind)
        }) {
            return Err(DomainError::EmptyField {
                field: "context_manifest.selection",
            });
        }
        Ok(())
    }
}

/// Rust-owned attenuation contract for projecting one persisted context
/// manifest into a child task. It carries artifact references only; raw
/// evidence and model transcripts are never delegation inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextProjection {
    pub parent_manifest: ArtifactRef,
    pub allowed: Vec<ArtifactRef>,
    pub reason: String,
}

impl ContextProjection {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.parent_manifest.kind != ArtifactKind::ContextManifest
            || self.reason.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "context_projection",
            });
        }

        let mut seen = BTreeSet::new();
        if self.allowed.iter().any(|reference| {
            reference.kind == ArtifactKind::RawEvidence
                || !seen.insert(reference.artifact_id.clone())
        }) {
            return Err(DomainError::EmptyField {
                field: "context_projection.allowed",
            });
        }

        Ok(())
    }
}

/// Ephemeral, task-scoped authorization derived from a persisted manifest. It is
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadGrant {
    pub manifest_artifact_id: ArtifactId,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: crate::AttemptId,
    pub lease_id: LeaseId,
    pub epoch: u64,
    pub contract_hash: ContentHash,
    pub readable: BTreeSet<ArtifactId>,
    pub raw_source_closure: BTreeSet<ArtifactId>,
    pub expires_at: DateTime<Utc>,
}

impl ReadGrant {
    pub fn matches_permit(&self, permit: &TaskWritePermit) -> bool {
        self.run_id == permit.run_id
            && self.task_id == permit.task_id
            && self.attempt_id == permit.attempt_id
            && self.lease_id == permit.lease_id
            && self.epoch == permit.epoch
            && permit.contract_hash.as_ref() == Some(&self.contract_hash)
    }

    pub fn permits(&self, artifact_id: &ArtifactId, raw: bool, now: DateTime<Utc>) -> bool {
        now < self.expires_at
            && if raw {
                self.raw_source_closure.contains(artifact_id)
            } else {
                self.readable.contains(artifact_id)
            }
    }
}

/// Authorizes exactly one running attempt to create an artifact or commit a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskWritePermit {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: crate::AttemptId,
    pub lease_id: LeaseId,
    pub epoch: u64,
    pub contract_hash: Option<ContentHash>,
}

impl TaskWritePermit {
    pub fn artifact_origin(&self) -> ArtifactOrigin {
        ArtifactOrigin {
            run_id: Some(self.run_id.clone()),
            task_id: Some(self.task_id.clone()),
            attempt_id: Some(self.attempt_id.clone()),
            contract_hash: self.contract_hash.clone(),
        }
    }
}
