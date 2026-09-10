//! Manifest-and-grant context broker for the runtime.

use std::collections::{BTreeSet, VecDeque};

use akzio_domain::{
    content_hash_json, estimate_tokens_from_bytes, manifest_input_hash, AgentContract, Artifact,
    ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    Asset, BlobRef, CandidatePolicy, ContentHash, ContextManifestPayload, ContextPolicy,
    ContextProjection, ContextQuarantine, ContextQuarantineReason, ContextSelection, ContextTrust,
    DecisionHorizon, DomainError, Experience, FinancialContentAssessment, FinancialContentPolicy,
    Lesson, LessonLifecycle, LessonScope, LifecycleEventType, PolicyState, ReadGrant,
    RegimeClassificationKind, RegimeSnapshot, ResearchClaim, ResearchCritique, TaskBudget,
    TaskWritePermit, DOMAIN_SCHEMA_VERSION, RESEARCH_ANALYST_RECIPE_ID, RESEARCH_CRITIC_RECIPE_ID,
    RESEARCH_SYNTHESIZER_RECIPE_ID,
};
use akzio_store::{Store, StoreError, SucceededAttemptProof};
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ContextError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("artifact {artifact_id} is not permitted by the contract context policy")]
    ForbiddenArtifact { artifact_id: ArtifactId },
    #[error("raw evidence cannot appear directly in a manifest")]
    RawEvidenceInManifest,
    #[error("artifact {artifact_id} is not granted by manifest {manifest_id}")]
    GrantDenied {
        manifest_id: ArtifactId,
        artifact_id: ArtifactId,
    },
    #[error("raw read requested for a non-raw artifact")]
    ExpectedRawEvidence,
    #[error("non-raw read requested for raw evidence")]
    RawEvidenceRequiresExplicitRead,
    #[error("context budget is exhausted")]
    BudgetExceeded,
    #[error("{purpose} context is missing required input: {requirement}")]
    MissingRequiredInput {
        purpose: String,
        requirement: String,
    },
    #[error("context manifest closure is invalid")]
    InvalidManifestClosure,
    #[error("contract authority blob is not declared")]
    AuthorityBlobNotDeclared,
    #[error("context byte range is invalid")]
    InvalidRange,
    #[error(
        "document exceeds 32 KiB tool response limit; use read_range with at most 32768 bytes"
    )]
    DocumentRequiresRange,
    #[error("context search request is invalid")]
    InvalidSearch,
    #[error("context source comparison is invalid")]
    InvalidComparison,
    #[error("claim evidence read requires a claim artifact")]
    ExpectedClaim,
    /// The Lesson-off arm of a paired experiment was requested against a
    /// baseline that carried no Lesson. The two arms would be byte-identical, so
    /// the comparison would report a null effect from a treatment that never
    /// varied.
    #[error("lesson ablation requires a baseline manifest that selected a lesson")]
    NoLessonToAblate,
}

pub type ContextResult<T> = Result<T, ContextError>;

#[derive(Debug, Clone)]
pub struct ContextBroker {
    store: Store,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifest {
    pub artifact: Artifact,
    pub payload: ContextManifestPayload,
    pub grant: ReadGrant,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContextDocumentMetadata {
    pub document_id: ArtifactId,
    pub kind: ArtifactKind,
    pub source: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub published_at: Option<DateTime<Utc>>,
    pub estimated_tokens: u32,
    pub relevance: u32,
    pub reason: String,
    pub must_read: bool,
    pub read_grant_identity: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContextMustReadDocument {
    pub class: String,
    pub metadata: ContextDocumentMetadata,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContextMaterialization {
    pub manifest_artifact_id: ArtifactId,
    pub read_grant_identity: ContentHash,
    pub materialization_identity: ContentHash,
    pub task_contract: Value,
    pub ledger: Vec<ContextDocumentMetadata>,
    pub must_read: Vec<ContextMustReadDocument>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextReadResult {
    pub artifacts: Vec<Artifact>,
    pub value: Value,
}

struct ParentContextProof<'a> {
    manifest: &'a ArtifactRef,
    readable: &'a BTreeSet<ArtifactRef>,
    raw_closure: &'a BTreeSet<ArtifactId>,
    permit: &'a TaskWritePermit,
    contract: &'a AgentContract,
}

include!("context_broker/manifest.rs");
include!("context_broker/selection.rs");
include!("context_broker/grants.rs");
include!("context_broker/materialization.rs");
include!("context_broker/reads.rs");
include!("context_broker/policy.rs");
include!("context_broker/helpers.rs");

#[path = "selection.rs"]
mod selection;
use selection::{
    context_trust, derive_child_projection, instruction_indicators, is_safe_deliberation_summary,
    is_trace_kind, overlay_state_is_eligible, purpose_rank, selection_reason,
};
