use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use akzio_domain::AttemptRelationKind;
use akzio_domain::{
    AgentContract, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, Asset, AttemptId, AttemptRelation, BlobRef, CandidatePolicy,
    CandidatePolicyState, ContentHash, ContractId, ContractPurpose, DeliberationSummary,
    DomainError, Evaluation, ExecutionContext, ExecutionPlan, ExecutionVerdict, Experience,
    FailureDisposition, FreezeState, LeaseId, Lesson, LessonId, LessonLifecycle,
    LifecycleEventType, OrderReceipt, Outcome, OutcomeExecutionLineage,
    OutcomeHorizon, OutcomeId, OutcomeSchedule, PaperCommitment, PaperLaunchApproval, PaperReprice,
    PolicyState, PolicySubject, PolicyTransition, PolicyTransitionId, Reconciliation,
    Retrospective, RetrospectiveDraft, RetrospectiveStatus, RetryPolicy, RunId, RunPurpose,
    RuntimeManifest, TaskBudget, TaskId, TaskRecipeId, TaskStatus, TaskWritePermit, WorkflowGraph,
    WorkflowNode, WorkflowProposal, WorkflowStatus, V2_DOMAIN_SCHEMA_VERSION,
};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{
    params, Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;


const DATABASE_FILE: &str = "akzio.sqlite3";
const EXPORT_DATABASE_FILE: &str = "akzio-export.sqlite3";
const POST_TERMINAL_WORKER_RECIPE_ID: &str = akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID;
const STORE_SCHEMA_VERSION: u32 = 12;
const BLOB_ENCODING_IDENTITY: &str = "identity";
const BLOB_ENCODING_ZSTD: &str = "zstd";
const BLOB_COMPRESSION_THRESHOLD: usize = 1_024;
const BLOB_COMPRESSION_MIN_SAVINGS: usize = 64;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error("I/O at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("incompatible Store Root {0}; create a new rebuilt-v2 root")]
    IncompatibleStoreRoot(PathBuf),
    #[error("artifact {0} does not exist")]
    MissingArtifact(ArtifactId),
    #[error("artifact {0} has an invalid source closure")]
    InvalidArtifactClosure(ArtifactId),
    #[error("workflow graph artifact must have kind workflow_graph")]
    InvalidWorkflowGraphArtifact,
    #[error("planner output artifact must have kind workflow_proposal")]
    InvalidWorkflowProposalArtifact,
    #[error("workflow graph differs from persisted task graph")]
    WorkflowGraphMismatch,
    #[error("workflow patch is based on a stale graph artifact")]
    StaleWorkflowGraph,
    #[error("Paper workflow {0} is immutable after submission")]
    FrozenPaperWorkflow(RunId),
    #[error("task {0} already exists")]
    DuplicateTask(TaskId),
    #[error("run {0} already exists")]
    DuplicateRun(RunId),
    #[error("run {0} does not exist")]
    MissingRun(RunId),
    #[error("workflow {run_id} revision {revision} does not exist")]
    MissingWorkflowRevision { run_id: RunId, revision: u64 },
    #[error("task write permit is stale for {0}")]
    StalePermit(TaskId),
    #[error("task write permit origin does not match artifact")]
    PermitOriginMismatch,
    #[error("task artifact lifecycle {lifecycle:?} is not allowed for {purpose:?} run")]
    InvalidTaskArtifactLifecycle {
        purpose: RunPurpose,
        lifecycle: ArtifactLifecycle,
    },
    #[error("task {0} has unresolved dependencies")]
    UnresolvedDependencies(TaskId),
    #[error("task {0} is not runnable")]
    TaskNotRunnable(TaskId),
    #[error("task {0} deferral must be in the future")]
    InvalidTaskDeferral(TaskId),
    #[error("attempt {attempt_id} is not a succeeded output attempt for task {task_id}")]
    CommittedOutputAttempt {
        task_id: TaskId,
        attempt_id: AttemptId,
    },
    #[error("task {task_id} in run {run_id} has no succeeded output attempt")]
    CommittedOutputTask { run_id: RunId, task_id: TaskId },
    #[error("task {0} does not exist")]
    MissingTask(TaskId),
    #[error("blob {0} is missing or corrupt")]
    MissingBlob(ContentHash),
    #[error("daemon lease {0} is fenced")]
    SchedulerFenced(String),
    #[error("invalid daemon lease {0}")]
    InvalidDaemonLease(String),
    #[error("invalid Paper session slot {0}")]
    InvalidSessionSlot(String),
    #[error("Paper session {0} already has a different commitment")]
    DuplicateExecutionCommitment(String),
    #[error("invalid Paper reprice intent")]
    InvalidExecutionReprice,
    #[error("invalid Paper effect artifact {0}")]
    InvalidPaperEffect(ArtifactId),
    #[error("lifecycle event {event_type} requires task, attempt and artifact lineage")]
    InvalidLifecycleEventShape { event_type: String },
    #[error("Paper effect {0} has no durable intent")]
    MissingPaperEffectIntent(ArtifactId),
    #[error("Paper effect {0} already has a terminal settlement")]
    PaperEffectAlreadySettled(ArtifactId),
    #[error("canonical learning requires a Paper run, got {0:?}")]
    NonCanonicalLearningPurpose(RunPurpose),
    #[error("outcome artifact {0} is not sealed")]
    UnsealedOutcome(ArtifactId),
    #[error("invalid canonical learning commit: {0}")]
    InvalidLearningCommit(&'static str),
    #[error("contract {0} is not installed")]
    MissingContractInstallation(ContentHash),
    #[error("contract identity {contract_id:?} version {version} is already installed")]
    DuplicateContractVersion {
        contract_id: ContractId,
        version: u32,
    },
    #[error("candidate contract {candidate} exceeds active contract {active}'s capability")]
    ContractCapabilityExpansion {
        active: ContentHash,
        candidate: ContentHash,
    },
    #[error("contract catalogue activation conflicts for purpose {0:?}")]
    ContractActivationConflict(ContractPurpose),
    #[error("contract upgrade from {active} is blocked by {blockers}")]
    ContractUpgradeBlocked {
        active: ContentHash,
        blockers: String,
    },
    #[error("policy head for {0} does not match transition predecessor")]
    PolicyHeadMismatch(String),
    #[error("policy transition {0} conflicts with a prior immutable transition")]
    PolicyTransitionConflict(String),
    #[error("policy evaluation {0} conflicts with prior immutable evaluation")]
    PolicyEvaluationConflict(String),
    #[error("shadow pair {0} conflicts with a prior immutable completion")]
    ShadowPairConflict(String),
    #[error("canary campaign {0} conflicts with the current campaign")]
    CanaryCampaignConflict(String),
    #[error("canary campaign {0} does not exist")]
    MissingCanaryCampaign(String),
    #[error("Store Doctor: {0}")]
    Integrity(String),
    #[error("backup target already exists: {0}")]
    BackupTargetExists(PathBuf),
    #[error("backup target cannot be inside Store Root: {0}")]
    BackupInsideStoreRoot(PathBuf),
    #[error("invalid backup source: {0}")]
    InvalidBackup(PathBuf),
    #[error("raw model export is only allowed for Debug runs, got {0:?}")]
    RawModelExportNotAllowed(RunPurpose),
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone)]
pub struct V2Store {
    root: Arc<PathBuf>,
    connection: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub schema_version: u32,
    pub database_hash: ContentHash,
    pub database_bytes: u64,
    pub blob_count: u64,
    pub blob_bytes: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageInventory {
    pub artifact_count: u64,
    pub blob_count: u64,
    pub logical_blob_bytes: u64,
    pub stored_blob_bytes: u64,
    pub compressed_blob_count: u64,
    pub direct_blob_count: u64,
    pub embedded_blob_count: u64,
    pub unreferenced_blob_count: u64,
    pub unreferenced_blob_bytes: u64,
}

/// Immutable Contract installation. `activated_at` is derived from the
/// catalogue head; the installation row itself is never rewritten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContract {
    pub contract: AgentContract,
    pub artifact: Artifact,
    pub baseline_contract_hash: Option<ContentHash>,
    pub installed_at: DateTime<Utc>,
    pub activated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRun {
    pub run_id: RunId,
    pub purpose: RunPurpose,
    pub topology_id: String,
    pub graph_artifact_id: ArtifactId,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredTask {
    pub run_id: RunId,
    pub node: WorkflowNode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowCommit {
    pub run: StoredRun,
    pub graph: Artifact,
    pub nodes: Vec<WorkflowNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowPatchCommit {
    pub permit: TaskWritePermit,
    pub previous_graph_artifact_id: ArtifactId,
    pub planner_output: Artifact,
    pub evidence_needs: Vec<Artifact>,
    pub proposal: Artifact,
    pub next_graph: Artifact,
    pub added_nodes: Vec<WorkflowNode>,
    pub updated_nodes: Vec<WorkflowNode>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkflowRevision {
    pub revision: u64,
    pub graph_artifact: Artifact,
    pub graph: WorkflowGraph,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredActiveAttempt {
    #[serde(skip)]
    pub permit: TaskWritePermit,
    pub worker_id: String,
    pub lease_until: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredTaskSnapshot {
    pub node: WorkflowNode,
    pub status: TaskStatus,
    pub ready_at: DateTime<Utc>,
    pub active_attempt: Option<StoredActiveAttempt>,
    pub attempt_count: u64,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkflowSnapshot {
    pub run: StoredRun,
    pub status: WorkflowStatus,
    pub finished_at: Option<DateTime<Utc>>,
    pub revision: WorkflowRevision,
    pub tasks: Vec<StoredTaskSnapshot>,
    pub event_cursor: i64,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedAttempt {
    pub run_id: RunId,
    pub node: WorkflowNode,
    pub permit: TaskWritePermit,
}

/// Read-only proof of the task attempt that currently owns the succeeded
/// task state. This is deliberately not a [`TaskWritePermit`]: completed
/// parent attempts must never be revived as write authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SucceededAttemptProof {
    pub run_id: RunId,
    pub task_id: TaskId,
    pub attempt_id: AttemptId,
    pub lease_id: LeaseId,
    pub epoch: u64,
    pub contract_hash: Option<ContentHash>,
    pub context_manifest: Option<ArtifactRef>,
    pub outputs: Vec<Artifact>,
}

/// Result of atomically closing a failed attempt. The Store—not a handler—
/// decides whether the retry budget allows another attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryTaskResult {
    Requeued,
    Terminal(TaskStatus),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredEvent {
    pub cursor: i64,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<akzio_domain::AttemptId>,
    pub event_type: String,
    pub artifact_id: Option<ArtifactId>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunExportArtifact {
    pub artifact: Artifact,
    pub payload_file: Option<String>,
    pub raw_model: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunExportManifest {
    pub schema_version: u32,
    pub exported_at: DateTime<Utc>,
    pub include_raw_model: bool,
    pub workflow: WorkflowSnapshot,
    pub events: Vec<StoredEvent>,
    pub trajectory: Vec<TrajectoryEntry>,
    pub artifacts: Vec<RunExportArtifact>,
}

/// Read-only, redacted projection of one durable agent trajectory fact.
/// Provider request/result bodies and tool arguments/results never cross this
/// boundary; only bounded metadata and structured deliberation summaries do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrajectoryEntry {
    pub cursor: i64,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<AttemptId>,
    pub turn: Option<u32>,
    pub phase: Option<String>,
    pub assistant_text: Option<String>,
    pub event_type: String,
    pub artifact_id: Option<ArtifactId>,
    pub artifact_kind: Option<ArtifactKind>,
    pub model: Option<TrajectoryModelMetadata>,
    pub latency_millis: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub tool: Option<TrajectoryToolLifecycle>,
    pub deliberation: Option<DeliberationSummary>,
    pub output_refs: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TrajectoryModelMetadata {
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub reasoning_effort: Option<String>,
    pub supports_tool_calls: Option<bool>,
    pub supports_stateless_continuation: Option<bool>,
    pub native_web_tool: Option<bool>,
    pub streaming: Option<bool>,
    pub declared_context_limit: Option<u32>,
    pub declared_max_output_tokens: Option<u32>,
    pub source: Option<String>,
    pub contract_hash: Option<ContentHash>,
    pub request_hash: Option<ContentHash>,
    pub capability_snapshot_hash: Option<ContentHash>,
    pub tool_set_hash: Option<ContentHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrajectoryToolLifecycle {
    pub call_id: Option<String>,
    pub name: Option<String>,
    pub lifecycle: String,
}

#[derive(Debug, Deserialize)]
struct StoredTrajectoryTurn {
    turn: Option<u32>,
    contract_hash: Option<ContentHash>,
    request_hash: Option<ContentHash>,
    capability_snapshot: Option<TrajectoryModelMetadata>,
    capability_snapshot_hash: Option<ContentHash>,
    tool_set_hash: Option<ContentHash>,
    request: Option<StoredTrajectoryRequest>,
    telemetry: Option<StoredTrajectoryTelemetry>,
    response: Option<StoredTrajectoryResponse>,
}

#[derive(Debug, Deserialize)]
struct StoredTrajectoryRequest {
    phase: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StoredTrajectoryResponse {
    assistant_text: Option<String>,
    telemetry: Option<StoredTrajectoryTelemetry>,
}

#[derive(Debug, Deserialize)]
struct StoredTrajectoryTelemetry {
    latency_millis: Option<u64>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct StoredTrajectoryToolCall {
    call_id: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct StoredTrajectoryToolArtifact {
    call: Option<StoredTrajectoryToolCall>,
    call_id: Option<String>,
    name: Option<String>,
}

impl StoredEvent {
    pub fn lifecycle_kind(&self) -> Result<LifecycleEventType, DomainError> {
        LifecycleEventType::parse(&self.event_type)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreMetrics {
    pub run_counts: BTreeMap<String, u64>,
    pub task_counts: BTreeMap<String, u64>,
    pub attempt_counts: BTreeMap<String, u64>,
    pub event_count: u64,
    pub active_daemon_leases: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertSeverity {
    Warning,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreAlert {
    pub code: String,
    pub severity: AlertSeverity,
    pub count: u64,
}

impl StoreMetrics {
    pub fn alerts(&self) -> Vec<StoreAlert> {
        let mut alerts = Vec::new();
        push_alert(
            &mut alerts,
            "failed_runs",
            "failed",
            AlertSeverity::Critical,
            &self.run_counts,
        );
        push_alert(
            &mut alerts,
            "failed_tasks",
            "failed",
            AlertSeverity::Critical,
            &self.task_counts,
        );
        push_alert(
            &mut alerts,
            "failed_attempts",
            "failed",
            AlertSeverity::Warning,
            &self.attempt_counts,
        );
        alerts
    }
}
