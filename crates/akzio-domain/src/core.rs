//! The versioned, Rust-owned language shared by every Akzio v2 module.
//!
//! This crate deliberately contains no database, model, network, or filesystem
//! dependency.  It is the single source of truth for contracts, workflow
//! state, evidence references, and execution intent.

//! Foundational scalar types and legacy records pending replacement by their
//! owner phases.

use std::{collections::BTreeMap, fmt};

use chrono::{DateTime, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub const V2_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("asset {0:?} is not executable by Akzio v2")]
    UnsupportedAsset(String),
    #[error("content hash must be a lowercase sha256 hex digest")]
    InvalidContentHash,
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("task graph contains a duplicate task id {0}")]
    DuplicateTaskId(TaskId),
    #[error("task {task} references unknown dependency {dependency}")]
    UnknownDependency { task: TaskId, dependency: TaskId },
    #[error("evidence source {0} is not allowed by the installed recipe")]
    EvidenceSourceNotAllowed(String),
    #[error("task graph contains a cycle")]
    CyclicPlan,
    #[error("budget {field} must be positive")]
    InvalidBudget { field: &'static str },
    #[error("target portfolio must include exactly TQQQ, QQQ, SOXX, and SOXL")]
    InvalidTargetUniverse,
    #[error("decision confidence must be at most one million ppm")]
    InvalidDecisionConfidence,
    #[error("decision forecasts must cover 1, 3, and 5 trading days exactly")]
    InvalidDecisionForecastHorizons,
    #[error("decision forecast probability must be at most one million ppm")]
    InvalidDecisionForecastProbability,
    #[error("a document attempt origin requires a task origin")]
    AttemptOriginWithoutTask,
    #[error("raw evidence may only be read through a Rust-controlled tool")]
    RawEvidenceDirectContext,
    #[error("Paper reprice must be the single deterministic r0 to r1 lineage")]
    InvalidRepriceLineage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Asset {
    Tqqq,
    Qqq,
    Soxx,
    Soxl,
}

impl Asset {
    pub const EXECUTABLE: [Self; 4] = [Self::Tqqq, Self::Qqq, Self::Soxx, Self::Soxl];

    pub const fn symbol(self) -> &'static str {
        match self {
            Self::Tqqq => "TQQQ",
            Self::Qqq => "QQQ",
            Self::Soxx => "SOXX",
            Self::Soxl => "SOXL",
        }
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.symbol())
    }
}

impl TryFrom<&str> for Asset {
    type Error = DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_uppercase().as_str() {
            "TQQQ" => Ok(Self::Tqqq),
            "QQQ" => Ok(Self::Qqq),
            "SOXX" => Ok(Self::Soxx),
            "SOXL" => Ok(Self::Soxl),
            other => Err(DomainError::UnsupportedAsset(other.to_owned())),
        }
    }
}

impl<'de> Deserialize<'de> for Asset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value.as_str()).map_err(D::Error::custom)
    }
}

/// Exact portfolio weight, expressed in parts per million.
///
/// Integer weights keep model JSON, execution policy, hashing, and replay on
/// the same arithmetic surface.  Floats are intentionally not admitted to
/// canonical decision documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WeightPpm(pub u32);

impl WeightPpm {
    pub const ZERO: Self = Self(0);
    pub const SCALE: u32 = 1_000_000;

    pub fn from_ratio(value: f64) -> Option<Self> {
        (value.is_finite() && (0.0..=1.0).contains(&value))
            .then(|| Self((value * f64::from(Self::SCALE)).round() as u32))
    }

    pub const fn as_ratio(self) -> f64 {
        self.0 as f64 / Self::SCALE as f64
    }
}

/// Signed money in millionths of a USD.  Execution accepts integer money only
/// so an order plan has a stable content hash across platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MoneyMicros(pub i64);

impl MoneyMicros {
    pub const ZERO: Self = Self(0);

    pub const fn from_usd_cents(cents: i64) -> Self {
        Self(cents.saturating_mul(10_000))
    }

    pub const fn as_usd(self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentHash(String);

impl ContentHash {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(Self(value))
    }

    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(format!("{:x}", Sha256::digest(bytes)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    fn canonicalize(value: &Value) -> Value {
        match value {
            Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
            Value::Object(items) => Value::Object(
                items
                    .iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect(),
            ),
            value => value.clone(),
        }
    }

    serde_json::to_vec(&canonicalize(value))
}

pub fn content_hash_json(value: &Value) -> Result<ContentHash, serde_json::Error> {
    canonical_json_bytes(value).map(|bytes| ContentHash::of_bytes(&bytes))
}

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4().to_string())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

id_type!(RunId);
id_type!(TaskId);
id_type!(AttemptId);
id_type!(LeaseId);
id_type!(ContractId);
id_type!(TopologyId);
id_type!(DecisionId);
id_type!(ExecutionPlanId);
id_type!(MemoryId);
id_type!(DocumentId);

/// Canonical target portfolio.  Cash is intentionally implicit: every
/// executable asset must be present, and unallocated equity remains cash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetPortfolio {
    pub weights: BTreeMap<Asset, WeightPpm>,
}

impl TargetPortfolio {
    pub fn zeroed() -> Self {
        Self {
            weights: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| (asset, WeightPpm::ZERO))
                .collect(),
        }
    }

    pub fn validate_universe(&self) -> Result<(), DomainError> {
        if self.weights.len() != Asset::EXECUTABLE.len()
            || !Asset::EXECUTABLE
                .into_iter()
                .all(|asset| self.weights.contains_key(&asset))
        {
            return Err(DomainError::InvalidTargetUniverse);
        }
        Ok(())
    }

    pub fn gross_weight_ppm(&self) -> u64 {
        self.weights
            .values()
            .map(|weight| u64::from(weight.0))
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HorizonForecast {
    pub trading_days: u8,
    pub positive_return_probability_ppm: u32,
    pub expected_return_ppm: i64,
}

/// Model-produced proposal before Rust binds it to a run, context manifest,
/// policy revision, and validity window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyDecisionDraft {
    pub summary: String,
    pub targets: TargetPortfolio,
    pub confidence_ppm: u32,
    pub forecasts: Vec<HorizonForecast>,
    pub blockers: Vec<String>,
    pub claim_refs: Vec<DocumentId>,
}

impl LegacyDecisionDraft {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.summary.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision.summary",
            });
        }
        if self.confidence_ppm > WeightPpm::SCALE {
            return Err(DomainError::InvalidDecisionConfidence);
        }
        let horizons = self
            .forecasts
            .iter()
            .map(|forecast| forecast.trading_days)
            .collect::<std::collections::BTreeSet<_>>();
        if self.forecasts.len() != 3 || horizons != std::collections::BTreeSet::from([1, 3, 5]) {
            return Err(DomainError::InvalidDecisionForecastHorizons);
        }
        if self
            .forecasts
            .iter()
            .any(|forecast| forecast.positive_return_probability_ppm > WeightPpm::SCALE)
        {
            return Err(DomainError::InvalidDecisionForecastProbability);
        }
        self.targets.validate_universe()
    }
}

/// Rust-finalized decision used by the execution and learning runtimes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioDecision {
    pub schema_version: u32,
    pub decision_id: DecisionId,
    pub run_id: RunId,
    pub source_document_id: DocumentId,
    pub context_manifest_id: DocumentId,
    pub memory_refs: Vec<DocumentId>,
    pub policy_hash: ContentHash,
    pub created_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub draft: LegacyDecisionDraft,
}

impl PortfolioDecision {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "portfolio_decision.schema_version",
            });
        }
        if self.valid_until <= self.created_at {
            return Err(DomainError::InvalidBudget {
                field: "portfolio_decision.valid_until",
            });
        }
        self.draft.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: ContentHash,
    pub media_type: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentKind {
    RawEvidence,
    NormalizedEvidence,
    SemanticDetail,
    ContextManifest,
    PlannerProposal,
    WorkflowPlan,
    TaskResult,
    ToolCall,
    ToolResult,
    AgentClaim,
    Challenge,
    DecisionDraft,
    DecisionContext,
    Decision,
    ExecutionContext,
    ExecutionPlan,
    ExecutionCommitment,
    OrderState,
    Outcome,
    Experience,
    Evaluation,
    Memory,
    CompactedContext,
    ContractBundle,
    AgentTurn,
}

impl DocumentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RawEvidence => "raw_evidence",
            Self::NormalizedEvidence => "normalized_evidence",
            Self::SemanticDetail => "semantic_detail",
            Self::ContextManifest => "context_manifest",
            Self::PlannerProposal => "planner_proposal",
            Self::WorkflowPlan => "workflow_plan",
            Self::TaskResult => "task_result",
            Self::ToolCall => "tool_call",
            Self::ToolResult => "tool_result",
            Self::AgentClaim => "agent_claim",
            Self::Challenge => "challenge",
            Self::DecisionDraft => "decision_draft",
            Self::DecisionContext => "decision_context",
            Self::Decision => "decision",
            Self::ExecutionContext => "execution_context",
            Self::ExecutionPlan => "execution_plan",
            Self::ExecutionCommitment => "execution_commitment",
            Self::OrderState => "order_state",
            Self::Outcome => "outcome",
            Self::Experience => "experience",
            Self::Evaluation => "evaluation",
            Self::Memory => "memory",
            Self::CompactedContext => "compacted_context",
            Self::ContractBundle => "contract_bundle",
            Self::AgentTurn => "agent_turn",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentLifecycle {
    Ephemeral,
    RunScoped,
    Canonical,
}

/// Immutable source metadata carried by every v2 document.  `BlobRef` is the
/// content address; this value explains where that content came from and how
/// much the producing subsystem trusts it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    pub source: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub retrieved_at: DateTime<Utc>,
    pub source_uri: Option<String>,
    pub confidence_ppm: u32,
    pub contract_hash: Option<ContentHash>,
}

/// The durable execution context that emitted a document. Source provenance
/// answers where facts came from; this answers which runtime attempt produced
/// the derived object. Canonical source records intentionally have no origin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentOrigin {
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<AttemptId>,
    pub contract_hash: Option<ContentHash>,
}

impl DocumentOrigin {
    pub fn task(
        task_id: TaskId,
        attempt_id: AttemptId,
        contract_hash: Option<ContentHash>,
    ) -> Self {
        Self {
            task_id: Some(task_id),
            attempt_id: Some(attempt_id),
            contract_hash,
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.attempt_id.is_some() && self.task_id.is_none() {
            return Err(DomainError::AttemptOriginWithoutTask);
        }
        Ok(())
    }
}

impl Provenance {
    pub fn local(source: impl Into<String>, retrieved_at: DateTime<Utc>) -> Self {
        Self {
            source: source.into(),
            observed_at: None,
            retrieved_at,
            source_uri: None,
            confidence_ppm: 1_000_000,
            contract_hash: None,
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.source.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "provenance.source",
            });
        }
        if self.confidence_ppm > 1_000_000 {
            return Err(DomainError::InvalidBudget {
                field: "provenance.confidence_ppm",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentRecord {
    pub document_id: DocumentId,
    pub kind: DocumentKind,
    pub blob: BlobRef,
    pub producer: String,
    pub run_id: Option<RunId>,
    pub lifecycle: DocumentLifecycle,
    pub source_refs: Vec<DocumentId>,
    pub provenance: Provenance,
    pub origin: Option<DocumentOrigin>,
    pub created_at: DateTime<Utc>,
}

impl DocumentRecord {
    pub fn validate(&self) -> Result<(), DomainError> {
        self.blob.validate()?;
        if self.producer.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "document.producer",
            });
        }
        self.provenance.validate()?;
        if let Some(origin) = &self.origin {
            origin.validate()?;
        }
        Ok(())
    }
}

impl BlobRef {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.media_type.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "blob_ref.media_type",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPurpose {
    Debug,
    Paper,
    PaperDryRun,
    Shadow,
}

impl RunPurpose {
    pub const fn is_canonical_learning(self) -> bool {
        matches!(self, Self::Paper)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Ingest,
    Plan,
    Investigate,
    Challenge,
    SynthesizeDecision,
    DecisionGate,
    MemoryOverlay,
    ExecutionGate,
    ExecutePaper,
    Reconcile,
    Evaluate,
    Shadow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Leased,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Skipped,
}

impl TaskStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Queued,
    Leased,
    Running,
    DecisionCompleted,
    Completed,
    CompletedWithExecutionRejection,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskBudget {
    pub max_input_tokens: u32,
    pub max_output_tokens: u32,
    pub max_wall_time_secs: u32,
    pub max_tool_calls: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    ReadEvidence,
    ReadRawEvidence,
    FetchWebEvidence,
    ReadMarketData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolGrant {
    pub kind: ToolKind,
    /// Empty means the tool is source-agnostic.  Otherwise every requested
    /// source must be explicitly present in this allowlist.
    pub allowed_sources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u8,
    pub initial_backoff_ms: u64,
    pub retry_transport: bool,
    pub retry_rate_limited: bool,
    pub retry_invalid_output: bool,
}

impl RetryPolicy {
    pub const fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff_ms: 0,
            retry_transport: false,
            retry_rate_limited: false,
            retry_invalid_output: false,
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.max_attempts == 0 {
            return Err(DomainError::InvalidBudget {
                field: "retry.max_attempts",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminationPolicy {
    pub max_child_tasks: u16,
    pub max_depth: u16,
    pub require_evidence: bool,
    pub stop_when_evidence_complete: bool,
}

impl TerminationPolicy {
    pub const fn leaf() -> Self {
        Self {
            max_child_tasks: 0,
            max_depth: 0,
            require_evidence: true,
            stop_when_evidence_complete: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureDisposition {
    FailRun,
    FailTask,
    SkipTask,
}

impl TaskBudget {
    pub fn validate(&self) -> Result<(), DomainError> {
        for (field, value) in [
            ("max_input_tokens", self.max_input_tokens),
            ("max_output_tokens", self.max_output_tokens),
            ("max_wall_time_secs", self.max_wall_time_secs),
        ] {
            if value == 0 {
                return Err(DomainError::InvalidBudget { field });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyAgentContract {
    pub schema_version: u32,
    pub contract_id: ContractId,
    pub version: u32,
    pub agent_kind: String,
    pub responsibility: String,
    pub prompt: BlobRef,
    /// Concrete document kinds accepted by this contract.  This is typed at
    /// the policy seam so a malformed persisted contract cannot expand an
    /// agent's evidence surface through string parsing at runtime.
    pub input_context_kinds: Vec<DocumentKind>,
    pub tool_grants: Vec<ToolGrant>,
    pub output_type: String,
    pub output_schema: BlobRef,
    pub budget: TaskBudget,
    pub retry: RetryPolicy,
    pub termination: TerminationPolicy,
    pub on_failure: FailureDisposition,
    pub contract_hash: ContentHash,
}

impl LegacyAgentContract {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "contract.schema_version",
            });
        }
        for (field, value) in [
            ("agent_kind", self.agent_kind.as_str()),
            ("responsibility", self.responsibility.as_str()),
            ("output_type", self.output_type.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(DomainError::EmptyField { field });
            }
        }
        self.prompt.validate()?;
        self.output_schema.validate()?;
        self.budget.validate()?;
        self.retry.validate()?;
        if self
            .input_context_kinds
            .contains(&DocumentKind::RawEvidence)
        {
            return Err(DomainError::RawEvidenceDirectContext);
        }
        for grant in &self.tool_grants {
            if grant
                .allowed_sources
                .iter()
                .any(|source| source.trim().is_empty())
            {
                return Err(DomainError::EmptyField {
                    field: "tool_grant.allowed_sources",
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub task_id: TaskId,
    pub kind: TaskKind,
    /// Immutable, human-readable work item persisted in the workflow plan.
    /// Dynamic agents must receive this exact text rather than infer intent
    /// from a parent planner document.
    pub objective: String,
    pub contract_hash: Option<ContentHash>,
    pub dependencies: Vec<TaskId>,
    pub input_refs: Vec<DocumentId>,
    pub budget: TaskBudget,
    /// Rust-owned behavior once this task has exhausted its retry budget.
    /// This replaces the old implicit `required` flag: downstream tasks can
    /// distinguish a recorded failure from an intentionally skipped branch,
    /// while only `FailRun` is allowed to terminate the whole workflow.
    pub on_failure: FailureDisposition,
    /// Higher values are claimed first when independent tasks are ready.
    pub priority: u8,
    pub max_attempts: u8,
    pub parent_task_id: Option<TaskId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowPlan {
    pub schema_version: u32,
    pub topology_id: TopologyId,
    pub tasks: Vec<TaskSpec>,
}

impl WorkflowPlan {
    pub fn validate(&self) -> Result<(), DomainError> {
        let mut tasks = BTreeMap::new();
        for task in &self.tasks {
            task.budget.validate()?;
            if task.objective.trim().is_empty() {
                return Err(DomainError::EmptyField {
                    field: "task.objective",
                });
            }
            if task.max_attempts == 0 {
                return Err(DomainError::InvalidBudget {
                    field: "task.max_attempts",
                });
            }
            if task.priority > 100 {
                return Err(DomainError::InvalidBudget {
                    field: "task.priority",
                });
            }
            if tasks.insert(task.task_id.clone(), task).is_some() {
                return Err(DomainError::DuplicateTaskId(task.task_id.clone()));
            }
        }
        for task in &self.tasks {
            for dependency in &task.dependencies {
                if !tasks.contains_key(dependency) {
                    return Err(DomainError::UnknownDependency {
                        task: task.task_id.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }
        let mut visiting = BTreeMap::new();
        fn visit(
            id: &TaskId,
            tasks: &BTreeMap<TaskId, &TaskSpec>,
            visiting: &mut BTreeMap<TaskId, u8>,
        ) -> Result<(), DomainError> {
            match visiting.get(id).copied() {
                Some(1) => return Err(DomainError::CyclicPlan),
                Some(2) => return Ok(()),
                _ => {}
            }
            visiting.insert(id.clone(), 1);
            for dependency in &tasks[id].dependencies {
                visit(dependency, tasks, visiting)?;
            }
            visiting.insert(id.clone(), 2);
            Ok(())
        }
        for id in tasks.keys() {
            visit(id, &tasks, &mut visiting)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: u32,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<AttemptId>,
    pub contract_hash: Option<ContentHash>,
    pub causation_id: Option<String>,
    pub event_type: String,
    /// Stable link to the document that carries event details. `payload` is
    /// retained for small error blobs that have no semantic document.
    pub payload_document_id: Option<DocumentId>,
    pub payload: Option<BlobRef>,
    pub created_at: DateTime<Utc>,
}

impl EventEnvelope {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.event_type.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "event_type",
            });
        }
        if let Some(payload) = &self.payload {
            payload.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Paper,
    DryRun,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 1,
            max_output_tokens: 1,
            max_wall_time_secs: 1,
            max_tool_calls: 0,
        }
    }

    #[test]
    fn executable_assets_are_exactly_the_v2_universe() {
        assert_eq!(Asset::EXECUTABLE.len(), 4);
        assert_eq!(Asset::try_from("SOXL").unwrap(), Asset::Soxl);
        assert!(Asset::try_from("VIX").is_err());
    }

    #[test]
    fn plan_rejects_cycles() {
        let first = TaskId::new();
        let second = TaskId::new();
        let plan = WorkflowPlan {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: TopologyId::new(),
            tasks: vec![
                TaskSpec {
                    task_id: first.clone(),
                    kind: TaskKind::Plan,
                    objective: "first".to_owned(),
                    contract_hash: None,
                    dependencies: vec![second.clone()],
                    input_refs: vec![],
                    budget: budget(),
                    on_failure: FailureDisposition::FailRun,
                    priority: 100,
                    max_attempts: 1,
                    parent_task_id: None,
                },
                TaskSpec {
                    task_id: second,
                    kind: TaskKind::Plan,
                    objective: "second".to_owned(),
                    contract_hash: None,
                    dependencies: vec![first],
                    input_refs: vec![],
                    budget: budget(),
                    on_failure: FailureDisposition::FailRun,
                    priority: 100,
                    max_attempts: 1,
                    parent_task_id: None,
                },
            ],
        };
        assert_eq!(plan.validate(), Err(DomainError::CyclicPlan));
    }
}
