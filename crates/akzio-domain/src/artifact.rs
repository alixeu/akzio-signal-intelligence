//! Immutable, content-addressed artifact vocabulary.

use std::{collections::BTreeSet, fmt};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::schema::SCHEMA_VERSION;
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
    /// Durable runtime boundary, never authorized research evidence.
    RuntimeCheckpoint,
    /// Run-scoped operational audit; never research evidence or canonical learning.
    DebugRecord,
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
    ProposalReview,
    DecisionContext,
    Decision,
    ExecutionContext,
    ExecutionVerdict,
    ExecutionPlan,
    ExecutionCommitment,
    ExecutionCancel,
    ExecutionReprice,
    OrderReceipt,
    Reconciliation,
    OutcomeSchedule,
    RiskGroundTruthAssessment,
    RetrospectiveDraft,
    Retrospective,
    AttemptRelation,
    Experience,
    Outcome,
    Evaluation,
    ExperimentTrial,
    SearchBiasCertificate,
    CandidatePolicy,
    /// Frozen offline calibration consumed by DecisionGate. This is distinct
    /// from CandidatePolicy, which governs learned Contract/Topology changes.
    DecisionPolicy,
    /// Operator-owned risk limits and canonical Outcome-derived calibration inputs.
    CalibrationRiskLimits,
    CalibrationDataset,
    Lesson,
    FreezeState,
    QualificationStageReceipt,
    PostOutcomeResearchApproval,
    BehaviorBundleManifest,
    RegimeSnapshot,
    ReleaseEvidenceBundle,
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
                | Self::ExecutionCancel
                | Self::ExecutionReprice
                | Self::OrderReceipt
                | Self::Reconciliation
                | Self::OutcomeSchedule
                | Self::RiskGroundTruthAssessment
                | Self::Retrospective
                | Self::Experience
                | Self::Outcome
                | Self::Evaluation
                | Self::ExperimentTrial
                | Self::SearchBiasCertificate
                | Self::CandidatePolicy
                | Self::DecisionPolicy
                | Self::CalibrationRiskLimits
                | Self::CalibrationDataset
                | Self::Lesson
                | Self::FreezeState
                | Self::RuntimeManifest
                | Self::PaperLaunchApproval
                | Self::QualificationStageReceipt
                | Self::PostOutcomeResearchApproval
                | Self::BehaviorBundleManifest
                | Self::RegimeSnapshot
                | Self::ReleaseEvidenceBundle
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
            schema_version: SCHEMA_VERSION,
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
        if self.schema_version != SCHEMA_VERSION {
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
            ArtifactKind::RuntimeCheckpoint => {
                if self.lifecycle != ArtifactLifecycle::RunScoped
                    || self.producer != "runtime.checkpoint"
                    || self.provenance.source_family != "akzio.runtime"
                    || !self.origin.as_ref().is_some_and(|o| {
                        o.run_id.is_some()
                            && o.task_id.is_none()
                            && o.attempt_id.is_none()
                            && o.contract_hash.is_none()
                    })
                    || !self
                        .source_refs
                        .iter()
                        .any(|r| r.kind == ArtifactKind::WorkflowGraph)
                {
                    return Err(DomainError::EmptyField {
                        field: "artifact.runtime_checkpoint_scope",
                    });
                }
                Ok(())
            }
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
            ArtifactKind::DebugRecord => {
                if self.lifecycle != ArtifactLifecycle::RunScoped
                    || !matches!(
                        self.producer.as_str(),
                        "debug.session_identity"
                            | "debug.control"
                            | "debug.stage_acceptance"
                            | "debug.budget_snapshot"
                    )
                    || self
                        .source_refs
                        .iter()
                        .any(|r| r.kind == ArtifactKind::RawEvidence)
                {
                    return Err(DomainError::EmptyField {
                        field: "artifact.debug_record_scope",
                    });
                }
                Ok(())
            }
            ArtifactKind::SemanticDetail => {
                // A collection status describes attempted needs, including
                // total provider failure where no raw document exists.
                let collection_status = self.producer == "evidence.collection_status"
                    && self.provenance.source_family == "akzio.ingest"
                    && self.lifecycle == ArtifactLifecycle::RunScoped
                    && self
                        .origin
                        .as_ref()
                        .is_some_and(|origin| origin.task_id.is_some())
                    && self
                        .source_refs
                        .iter()
                        .all(|reference| reference.kind == ArtifactKind::EvidenceNeed);
                let has_evidence = self.source_refs.iter().any(|reference| {
                    matches!(
                        reference.kind,
                        ArtifactKind::RawEvidence | ArtifactKind::NormalizedEvidence
                    )
                });
                let research_audit = match self.producer.as_str() {
                    "research.revision.stop" => self
                        .source_refs
                        .iter()
                        .any(|r| r.kind == ArtifactKind::ProposalReview),
                    "learning.revalidation.suggestion" | "learning.retrieval.audit" => self
                        .source_refs
                        .iter()
                        .any(|r| r.kind == ArtifactKind::Lesson),
                    "context.coverage" => self
                        .source_refs
                        .iter()
                        .any(|r| r.kind == ArtifactKind::ContextManifest),
                    "research.supplement.started"
                    | "research.supplement.disposition"
                    | "research.supplement.result" => self
                        .source_refs
                        .iter()
                        .any(|r| matches!(r.kind, ArtifactKind::Claim | ArtifactKind::Critique)),
                    _ => false,
                };
                if self.source_refs.is_empty()
                    || collection_status
                    || has_evidence
                    || research_audit
                {
                    Ok(())
                } else {
                    Err(DomainError::EmptyField {
                        field: "artifact.detail_source_refs",
                    })
                }
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
            ArtifactKind::SearchBiasCertificate
                if !self
                    .source_refs
                    .iter()
                    .any(|reference| reference.kind == ArtifactKind::ExperimentTrial) =>
            {
                Err(DomainError::EmptyField {
                    field: "artifact.search_bias_source_refs",
                })
            }
            _ => Ok(()),
        }
    }
}
