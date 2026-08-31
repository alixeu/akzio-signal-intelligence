//! Manifest-and-grant context broker for the v2 runtime.

use std::collections::{BTreeSet, VecDeque};

use akzio_domain::{
    content_hash_json, estimate_tokens_from_bytes, AgentContract, Artifact, ArtifactId,
    ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef, Asset,
    BlobRef, CandidatePolicy, ContentHash, ContextManifestPayload, ContextPolicy,
    ContextProjection, ContextSelection, DecisionHorizon, DomainError, Experience, Lesson,
    LessonLifecycle, LessonScope, LifecycleEventType, PolicyState, ReadGrant, ResearchClaim,
    TaskWritePermit, RESEARCH_ANALYST_RECIPE_ID, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoreError, SucceededAttemptProof, V2Store};
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
    #[error("context manifest closure is invalid")]
    InvalidManifestClosure,
    #[error("contract authority blob is not declared")]
    AuthorityBlobNotDeclared,
    #[error("context byte range is invalid")]
    InvalidRange,
    #[error("context search request is invalid")]
    InvalidSearch,
    #[error("context source comparison is invalid")]
    InvalidComparison,
    #[error("claim evidence read requires a claim artifact")]
    ExpectedClaim,
}

pub type ContextResult<T> = Result<T, ContextError>;

#[derive(Debug, Clone)]
pub struct ContextBroker {
    store: V2Store,
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

include!("broker_parts/manifest.rs");
include!("broker_parts/selection.rs");
include!("broker_parts/grants.rs");
include!("broker_parts/materialization.rs");
include!("broker_parts/reads.rs");
include!("broker_parts/policy.rs");
include!("broker_parts/helpers.rs");

#[path = "selection.rs"]
mod selection;
#[cfg(test)]
use selection::projection_artifact_ids;
use selection::{
    context_rank, derive_child_projection, is_safe_deliberation_summary, is_trace_kind,
    manifest_input_hash, overlay_state_is_eligible, selection_reason,
};
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
