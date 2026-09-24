//! Manifest-and-grant context broker for the runtime.

use std::collections::{BTreeSet, VecDeque};

use akzio_domain::{
    content_hash_json, estimate_tokens_from_bytes, manifest_input_hash, AgentContract, Artifact,
    ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    Asset, BlobRef, CandidatePolicy, ContentHash, ContextManifestPayload, ContextPolicy,
    ContextProjection, ContextQuarantine, ContextQuarantineReason, ContextQueryScope,
    ContextSelection, ContextTrust, DomainError, Experience, FinancialContentAssessment,
    FinancialContentPolicy, Lesson, LessonLifecycle, LifecycleEventType, PolicyState, ReadGrant,
    RegimeClassificationKind, RegimeSnapshot, ResearchClaim, ResearchCritique, TaskBudget,
    TaskWritePermit, DOMAIN_SCHEMA_VERSION, RESEARCH_ANALYST_RECIPE_ID, RESEARCH_CRITIC_RECIPE_ID,
    RESEARCH_SYNTHESIZER_RECIPE_ID,
};
use akzio_store::{Store, StoreError, SucceededAttemptProof};
use chrono::{DateTime, Duration, Utc};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use thiserror::Error;

// ContextBroker 是 Agent 唯一的受控材料入口：Store/CAS、Manifest、ReadGrant 和 ContextPolicy 在这里汇合。
#[derive(Debug, Error)]
pub enum ContextError {
    // 错误枚举区分 Store/Domain/JSON、grant/closure、预算和读取类型错误，调用方不能用 UI fallback 绕过权限失败。
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
}

pub type ContextResult<T> = Result<T, ContextError>;

#[derive(Debug, Clone)]
pub struct ContextBroker {
    // Broker 只持有 Store handle；权限和 provenance 每次由持久化数据重新验证，不依赖进程内可变缓存。
    store: Store,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextManifest {
    // Manifest、payload 和 ReadGrant 作为不可变值一起返回，后续读取必须继续满足同一 Attempt/Contract 身份。
    pub artifact: Artifact,
    pub payload: ContextManifestPayload,
    pub grant: ReadGrant,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContextDocumentMetadata {
    // metadata 是受控投影 ledger，不暴露任意 raw blob；read_grant_identity 让 materialization 可审计。
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
    // must_read 同时携带 class、metadata 和已授权 Value，不能由调用方再扩展来源集合。
    pub class: String,
    pub metadata: ContextDocumentMetadata,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ContextMaterialization {
    // materialization 固定 manifest/grant/task contract 与读取 ledger，作为模型 Context 的值语义快照。
    pub manifest_artifact_id: ArtifactId,
    pub read_grant_identity: ContentHash,
    pub materialization_identity: ContentHash,
    pub task_contract: Value,
    pub ledger: Vec<ContextDocumentMetadata>,
    pub must_read: Vec<ContextMustReadDocument>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextReadResult {
    // read result 保留 Artifact provenance 与转换后的 Value，读取失败通过 ContextResult 向上返回。
    pub artifacts: Vec<Artifact>,
    pub value: Value,
}

struct ParentContextProof<'a> {
    // 父证明把 Manifest、readable/raw closure、写 permit 和 Contract 绑定，子 Attempt 不能只凭单个 source ref 继承权限。
    manifest: &'a ArtifactRef,
    readable: &'a BTreeSet<ArtifactRef>,
    raw_closure: &'a BTreeSet<ArtifactId>,
    permit: &'a TaskWritePermit,
    contract: &'a AgentContract,
}

// 以下 include 是 Broker 的实现分区：manifest/selection 负责选择，grants/reads 负责授权，materialization 负责受控输出。
include!("context_broker/manifest.rs");
include!("context_broker/coverage.rs");
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

#[path = "context_broker/guidance.rs"]
mod guidance;
