//! Immutable, content-addressed artifact vocabulary.

use std::{collections::BTreeSet, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::schema::V2_SCHEMA_VERSION;
use crate::{content_hash_json, BlobRef, ContentHash, DomainError, RunId, TaskId};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactId(pub ContentHash);

impl fmt::Display for ArtifactId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    RawEvidence,
    NormalizedEvidence,
    SemanticDetail,
    ContextManifest,
    ContextRepair,
    EvidenceNeed,
    Contract,
    WorkflowProposalDraft,
    WorkflowProposal,
    WorkflowGraph,
    RuntimeManifest,
    PaperLaunchApproval,
    AgentTurn,
    DeliberationNote,
    ToolCall,
    ToolResult,
    Claim,
    Critique,
    Resolution,
    DecisionProposal,
    DecisionContext,
    Decision,
    ExecutionContext,
    ExecutionVerdict,
    ExecutionPlan,
    ExecutionCommitment,
    ExecutionReprice,
    OrderReceipt,
    Reconciliation,
    OutcomeSchedule,
    RetrospectiveDraft,
    Retrospective,
    AttemptRelation,
    Experience,
    Outcome,
    Evaluation,
    CandidatePolicy,
    Lesson,
    FreezeState,
}

impl ArtifactKind {
    pub const fn can_be_canonical(self) -> bool {
        matches!(
            self,
            Self::RawEvidence
                | Self::NormalizedEvidence
                | Self::SemanticDetail
                | Self::Contract
                | Self::Claim
                | Self::Critique
                | Self::DecisionContext
                | Self::Decision
                | Self::ExecutionContext
                | Self::ExecutionVerdict
                | Self::ExecutionPlan
                | Self::ExecutionCommitment
                | Self::ExecutionReprice
                | Self::OrderReceipt
                | Self::Reconciliation
                | Self::OutcomeSchedule
                | Self::Retrospective
                | Self::Experience
                | Self::Outcome
                | Self::Evaluation
                | Self::CandidatePolicy
                | Self::Lesson
                | Self::FreezeState
                | Self::RuntimeManifest
                | Self::PaperLaunchApproval
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLifecycle {
    Ephemeral,
    RunScoped,
    Canonical,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactProvenance {
    /// Rust-owned adapter/source family, never a model-provided URL or provider.
    pub source_family: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub retrieved_at: DateTime<Utc>,
    pub source_uri: Option<String>,
    pub confidence_ppm: u32,
    pub producer_contract_hash: Option<ContentHash>,
}

impl ArtifactProvenance {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.source_family.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "artifact.provenance.source_family",
            });
        }
        if self.confidence_ppm > 1_000_000 {
            return Err(DomainError::InvalidBudget {
                field: "artifact.provenance.confidence_ppm",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactOrigin {
    pub run_id: Option<RunId>,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<crate::AttemptId>,
    pub contract_hash: Option<ContentHash>,
}

impl ArtifactOrigin {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.attempt_id.is_some() && self.task_id.is_none() {
            return Err(DomainError::AttemptOriginWithoutTask);
        }
        Ok(())
    }
}

/// Immutable typed metadata for a CAS blob. The identity covers the metadata and
/// payload reference, therefore a caller cannot substitute provenance under an
/// existing artifact ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub schema_version: u32,
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub blob: BlobRef,
    pub producer: String,
    pub lifecycle: ArtifactLifecycle,
    pub provenance: ArtifactProvenance,
    pub origin: Option<ArtifactOrigin>,
    pub source_refs: Vec<ArtifactRef>,
    pub created_at: DateTime<Utc>,
}

impl Artifact {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: ArtifactKind,
        blob: BlobRef,
        producer: impl Into<String>,
        lifecycle: ArtifactLifecycle,
        provenance: ArtifactProvenance,
        origin: Option<ArtifactOrigin>,
        source_refs: Vec<ArtifactRef>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        let producer = producer.into();
        let mut source_refs = source_refs;
        source_refs.sort();
        let mut artifact = Self {
            schema_version: V2_SCHEMA_VERSION,
            artifact_id: ArtifactId(ContentHash::of_bytes(b"uninitialized artifact")),
            kind,
            blob,
            producer,
            lifecycle,
            provenance,
            origin,
            source_refs,
            created_at,
        };
        artifact.artifact_id = ArtifactId(artifact.expected_hash()?);
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn expected_hash(&self) -> Result<ContentHash, DomainError> {
        let mut canonical = self.clone();
        canonical.source_refs.sort();
        let mut value = serde_json::to_value(canonical).map_err(|_| DomainError::EmptyField {
            field: "artifact.serialize",
        })?;
        value
            .as_object_mut()
            .expect("artifact serializes to object")
            .remove("artifact_id");
        content_hash_json(&value).map_err(|_| DomainError::EmptyField {
            field: "artifact.serialize",
        })
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "artifact.schema_version",
            });
        }
        if self.producer.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "artifact.producer",
            });
        }
        self.blob.validate()?;
        self.provenance.validate()?;
        if let Some(origin) = &self.origin {
            origin.validate()?;
        }
        self.validate_source_refs()?;
        if self.lifecycle == ArtifactLifecycle::Canonical && !self.kind.can_be_canonical() {
            return Err(DomainError::EmptyField {
                field: "artifact.canonical_kind",
            });
        }
        let lifecycle_allowed = match self.kind {
            ArtifactKind::DeliberationNote
            | ArtifactKind::RetrospectiveDraft
            | ArtifactKind::AttemptRelation => self.lifecycle == ArtifactLifecycle::RunScoped,
            ArtifactKind::Retrospective => {
                matches!(
                    self.lifecycle,
                    ArtifactLifecycle::RunScoped | ArtifactLifecycle::Canonical
                )
            }
            _ => true,
        };
        if !lifecycle_allowed {
            return Err(DomainError::EmptyField {
                field: "artifact.lifecycle",
            });
        }
        if self.artifact_id.0 != self.expected_hash()? {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }

    fn validate_source_refs(&self) -> Result<(), DomainError> {
        if self.source_refs.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(DomainError::EmptyField {
                field: "artifact.source_refs",
            });
        }
        let mut seen = BTreeSet::new();

        if self.source_refs.iter().any(|reference| {
            reference.artifact_id == self.artifact_id || !seen.insert(reference.artifact_id.clone())
        }) {
            return Err(DomainError::EmptyField {
                field: "artifact.source_refs",
            });
        }

        match self.kind {
            ArtifactKind::RawEvidence if !self.source_refs.is_empty() => {
                Err(DomainError::EmptyField {
                    field: "artifact.raw_source_refs",
                })
            }
            ArtifactKind::NormalizedEvidence
                if !self.source_refs.is_empty()
                    && !self
                        .source_refs
                        .iter()
                        .any(|reference| reference.kind == ArtifactKind::RawEvidence) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.normalized_source_refs",
                })
            }
            ArtifactKind::SemanticDetail
                if !self.source_refs.is_empty()
                    && !self.source_refs.iter().any(|reference| {
                        matches!(
                            reference.kind,
                            ArtifactKind::RawEvidence | ArtifactKind::NormalizedEvidence
                        )
                    }) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.detail_source_refs",
                })
            }
            ArtifactKind::Claim
                if !self.source_refs.iter().any(|reference| {
                    matches!(
                        reference.kind,
                        ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                    )
                }) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.claim_source_refs",
                })
            }
            ArtifactKind::Critique
                if !self
                    .source_refs
                    .iter()
                    .any(|reference| reference.kind == ArtifactKind::Claim) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.critique_source_refs",
                })
            }
            ArtifactKind::Resolution
                if !self
                    .source_refs
                    .iter()
                    .any(|reference| reference.kind == ArtifactKind::Claim)
                    || !self
                        .source_refs
                        .iter()
                        .any(|reference| reference.kind == ArtifactKind::Critique) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.resolution_source_refs",
                })
            }
            _ => Ok(()),
        }
    }
}
