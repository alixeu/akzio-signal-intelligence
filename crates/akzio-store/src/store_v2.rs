//! Store implementation for the source-incompatible Akzio v2 authority.
//!
//! `V2Store` deliberately uses a different database filename and metadata
//! marker from `V2Store`; callers must choose a new Store Root rather than run a
//! silent in-place migration.

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
    LifecycleEventType, OrderReceipt, OrderReceiptState, Outcome, OutcomeExecutionLineage,
    OutcomeHorizon, OutcomeId, OutcomeSchedule, PaperCommitment, PaperLaunchApproval, PaperReprice,
    PolicyState, PolicySubject, PolicyTransition, PolicyTransitionId, Reconciliation,
    Retrospective, RetrospectiveDraft, RetrospectiveStatus, RetryPolicy, RunId, RunPurpose,
    RuntimeManifest, TaskBudget, TaskId, TaskRecipeId, TaskStatus, TaskWritePermit, WorkflowGraph,
    WorkflowNode, WorkflowProposal, WorkflowStatus, V2_DOMAIN_SCHEMA_VERSION, V2_SCHEMA_VERSION,
};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{
    params, Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

mod blob;
mod doctor;
mod execution;
mod learning;
mod lease;
mod lesson;
mod schema;
mod trajectory;
mod workflow;

pub use lesson::{LessonUsage, LessonWriteResult, StoredLesson};

const DATABASE_FILE: &str = "akzio.sqlite3";
const EXPORT_DATABASE_FILE: &str = "akzio-export.sqlite3";
const POST_TERMINAL_WORKER_RECIPE_ID: &str = "learning.outcome_worker";
const STORE_SCHEMA_VERSION: u32 = 11;
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
    #[error("Paper commitment lineage {0} already has a different reprice")]
    DuplicateExecutionReprice(String),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

fn push_alert(
    alerts: &mut Vec<StoreAlert>,
    code: &str,
    status: &str,
    severity: AlertSeverity,
    counts: &BTreeMap<String, u64>,
) {
    if let Some(&count) = counts.get(status).filter(|count| **count > 0) {
        alerts.push(StoreAlert {
            code: code.to_owned(),
            severity,
            count,
        });
    }
}

/// Fenced singleton lease for daemon-owned scheduling work. Task attempts use
/// their own permits; this lease exclusively authorizes session slots and
/// broker-visible commitment transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonLease {
    pub lease_name: String,
    pub owner_id: String,
    pub epoch: u64,
    pub expires_at: DateTime<Utc>,
}

/// Exact Paper workflow frozen before its run is installed. A recovery must
/// reuse this graph and its task IDs instead of recompiling a new plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReservation {
    pub session_key: String,
    pub workflow: WorkflowCommit,
    /// Immutable scheduler-owned `EvidenceNeed` inputs installed atomically
    /// with the frozen Paper graph, before any Run becomes visible.
    pub setup_artifacts: Vec<Artifact>,
    pub reserved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSlot {
    pub session_key: String,
    pub workflow: WorkflowCommit,
    pub scheduler_epoch: u64,
    pub reserved_at: DateTime<Utc>,
    pub commitment_artifact_id: Option<ArtifactId>,
    pub committed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSlotReservation {
    pub slot: SessionSlot,
    pub newly_reserved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCommit {
    pub session_key: String,
    pub permit: TaskWritePermit,
    pub commitment: Artifact,
    pub committed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionCommitResult {
    pub commitment_artifact_id: ArtifactId,
    pub newly_committed: bool,
}

/// Fenced, durable one-time r0 -> r1 Paper replacement intent. The intent is
/// committed before adapter I/O so a crash can replay the same replacement ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepriceCommit {
    pub permit: TaskWritePermit,
    pub reprice: Artifact,
    pub committed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepriceCommitResult {
    pub reprice_artifact_id: ArtifactId,
    pub newly_committed: bool,
}

/// Current immutable-history head for a candidate memory, contract, or topology.
/// The transition table remains the source of history; this row is only a
/// transactionally maintained reconstruction cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyHead {
    pub subject: PolicySubject,
    pub state: PolicyState,
    pub revision: u64,
    pub transition_id: PolicyTransitionId,
    /// Durable event cursor for the transition that produced this head.
    pub transition_cursor: i64,
    pub updated_at: DateTime<Utc>,
}

/// Every canonical evaluation is recorded, even when it leaves the policy
/// state unchanged. This closes the freshness cursor so the same shadow pair
/// cannot be reconsidered after a no-op evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluationCommit {
    pub permit: TaskWritePermit,
    pub outcome: Artifact,
    pub final_retrospective: Artifact,
    pub experience: Artifact,
    pub evaluation: Artifact,
    pub candidate_policy: Option<Artifact>,
    pub subject: PolicySubject,
    pub from: PolicyState,
    pub to: PolicyState,
    /// Store-issued immutable cutoff and counts used to derive this
    /// evaluation. Pairs completed after `through_cursor` remain fresh.
    pub pair_snapshot: PolicyShadowPairSnapshot,
    /// Present only for an actual state transition. No-op evaluations retain
    /// immutable evidence history but do not manufacture an invalid
    /// `PolicyTransition { from == to }`.
    pub transition: Option<PolicyTransition>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyShadowPairSnapshot {
    pub after_cursor: i64,
    pub through_cursor: i64,
    pub counts_by_horizon: [u64; 3],
}

impl PolicyShadowPairSnapshot {
    pub const fn count(self, horizon: OutcomeHorizon) -> u64 {
        self.counts_by_horizon[match horizon {
            OutcomeHorizon::T1 => 0,
            OutcomeHorizon::T3 => 1,
            OutcomeHorizon::T5 => 2,
        }]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluationResult {
    pub policy_head: Option<PolicyHead>,
    pub consumed_pair_cursor: i64,
    pub evaluation_cursor: i64,
    pub newly_recorded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredPolicyEvaluation {
    subject: PolicySubject,
    outcome_artifact_id: ArtifactId,
    experience_artifact_id: ArtifactId,
    evaluation_artifact_id: ArtifactId,
    candidate_policy_artifact_id: Option<ArtifactId>,
    from: PolicyState,
    to: PolicyState,
    transition_id: Option<PolicyTransitionId>,
    run_id: RunId,
    consumed_pair_cursor: i64,
    event_cursor: i64,
    completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PolicyConsumptionHead {
    subject: PolicySubject,
    consumed_pair_cursor: i64,
    evaluation_artifact_id: ArtifactId,
    evaluation_cursor: i64,
    updated_at: DateTime<Utc>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyTransitionRecord {
    pub transition: PolicyTransition,
    pub run_id: RunId,
    pub revision: u64,
    pub transition_cursor: i64,
}

/// One completed, outcome-backed comparison between the production decision
/// and a candidate. The key intentionally excludes `completed_at`: retries at
/// the same timestamp, or at a later timestamp after a crash, must remain
/// idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShadowPairCompletion {
    pub subject: PolicySubject,
    pub parent_decision: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub candidate_decision: ArtifactRef,
    pub candidate_contract_hash: ContentHash,
    pub candidate_topology_id: String,
    pub horizon: OutcomeHorizon,
    pub parent_outcome: ArtifactRef,
    pub candidate_outcome: ArtifactRef,
    pub completed_at: DateTime<Utc>,
}

impl ShadowPairCompletion {
    pub fn pair_key(&self) -> StoreResult<ContentHash> {
        let key = serde_json::json!({
            "subject": &self.subject,
            "parent_decision": &self.parent_decision,
            "execution_context": &self.execution_context,
            "candidate_decision": &self.candidate_decision,
            "candidate_contract_hash": &self.candidate_contract_hash,
            "candidate_topology_id": &self.candidate_topology_id,
            "horizon": self.horizon,
        });
        Ok(akzio_domain::content_hash_json(&key)?)
    }

    fn validate(&self) -> StoreResult<()> {
        self.subject.validate()?;
        if self.candidate_topology_id.trim().is_empty() {
            return Err(StoreError::InvalidLearningCommit("shadow_pair.identity"));
        }
        match &self.subject {
            PolicySubject::Contract(contract_hash)
                if contract_hash != &self.candidate_contract_hash =>
            {
                return Err(StoreError::InvalidLearningCommit(
                    "shadow_pair.contract_subject",
                ));
            }
            PolicySubject::Topology(topology_id) if topology_id.0 != self.candidate_topology_id => {
                return Err(StoreError::InvalidLearningCommit(
                    "shadow_pair.topology_subject",
                ));
            }
            _ => {}
        }
        if self.parent_decision.kind != ArtifactKind::Decision
            || self.execution_context.kind != ArtifactKind::ExecutionContext
            || self.candidate_decision.kind != ArtifactKind::Decision
            || self.parent_outcome.kind != ArtifactKind::Outcome
            || self.candidate_outcome.kind != ArtifactKind::Outcome
        {
            return Err(StoreError::InvalidLearningCommit("shadow_pair.references"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredShadowPair {
    pub pair_key: ContentHash,
    pub completion: ShadowPairCompletion,
    /// Durable event cursor for the idempotent pair completion.
    pub completion_cursor: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShadowPairWriteResult {
    Inserted(StoredShadowPair),
    Existing(StoredShadowPair),
}

impl V2Store {
    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    fn connection(&self) -> StoreResult<std::sync::MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| StoreError::Integrity("store connection poisoned".to_owned()))
    }

    pub fn observatory_configuration<T: DeserializeOwned>(&self) -> StoreResult<Option<T>> {
        let payload = self
            .connection()?
            .query_row(
                "SELECT configuration_json FROM rebuild_observatory_configuration WHERE singleton = 1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        payload
            .map(|payload| serde_json::from_slice(&payload))
            .transpose()
            .map_err(StoreError::from)
    }

    pub fn set_observatory_configuration<T: Serialize>(
        &self,
        configuration: &T,
    ) -> StoreResult<()> {
        let payload = serde_json::to_vec(configuration)?;
        self.connection()?.execute(
            "INSERT INTO rebuild_observatory_configuration (singleton, configuration_json) VALUES (1, ?1) ON CONFLICT(singleton) DO UPDATE SET configuration_json = excluded.configuration_json",
            params![payload],
        )?;
        Ok(())
    }

    pub fn clear_observatory_configuration(&self) -> StoreResult<bool> {
        Ok(self.connection()?.execute(
            "DELETE FROM rebuild_observatory_configuration WHERE singleton = 1",
            [],
        )? > 0)
    }

    fn read_all_events(&self, run_id: &RunId) -> StoreResult<Vec<StoredEvent>> {
        const PAGE_SIZE: usize = 256;
        let mut after = 0_i64;
        let mut events = Vec::new();
        loop {
            let page = self.events_after(run_id, after, PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after = page.last().expect("non-empty event page").cursor;
            events.extend(page);
            if events.len() < PAGE_SIZE {
                break;
            }
        }
        Ok(events)
    }

    /// Writes a root artifact such as an installed Contract. Bootstrap is deliberately
    /// narrow: a task-origin artifact must use `write_task_artifact` instead.
    pub fn write_bootstrap_artifact(&self, artifact: &Artifact) -> StoreResult<()> {
        artifact.validate()?;
        if artifact.origin.is_some()
            || !matches!(
                artifact.kind,
                ArtifactKind::Contract
                    | ArtifactKind::FreezeState
                    | ArtifactKind::RuntimeManifest
                    | ArtifactKind::PaperLaunchApproval
            )
        {
            return Err(StoreError::PermitOriginMismatch);
        }
        self.read_blob(&artifact.blob)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_artifact(&transaction, artifact)?;
        transaction.commit()?;
        Ok(())
    }

    /// Return the immutable Contract currently selected for a purpose.
    /// The mutable head is only a reconstruction cursor; each activation stays
    /// in `rebuild_contract_activations` for Doctor and restart recovery.
    /// Persist an immutable operator freeze transition. There is no mutable
    /// switch: execution consults the latest canonical `FreezeState` artifact.
    pub fn write_freeze_state(
        &self,
        frozen: bool,
        reason: impl Into<String>,
        changed_at: DateTime<Utc>,
    ) -> StoreResult<Artifact> {
        let payload = FreezeState {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            frozen,
            reason: reason.into(),
            changed_at,
        };
        payload.validate()?;
        let artifact = Artifact::new(
            ArtifactKind::FreezeState,
            self.put_json(&payload)?,
            "store.freeze_state",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".to_owned(),
                observed_at: Some(changed_at),
                retrieved_at: changed_at,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            Vec::new(),
            changed_at,
        )?;
        self.write_bootstrap_artifact(&artifact)?;
        Ok(artifact)
    }

    fn contract_artifact(
        &self,
        contract: &AgentContract,
        now: DateTime<Utc>,
    ) -> StoreResult<Artifact> {
        Ok(Artifact::new(
            ArtifactKind::Contract,
            self.put_json(contract)?,
            "research.contract_catalogue",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.contract_catalogue".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )?)
    }

    fn stored_contract_with_connection(
        &self,
        connection: &Connection,
        contract_hash: &ContentHash,
    ) -> StoreResult<Option<StoredContract>> {
        let row = connection
            .query_row(
                r#"SELECT contract_artifact_id, baseline_contract_hash, installed_at,
                          activation.activated_at
                   FROM rebuild_contract_installations AS installation
                   LEFT JOIN rebuild_contract_catalogue_heads AS head
                     ON head.contract_hash = installation.contract_hash
                   LEFT JOIN rebuild_contract_activations AS activation
                     ON activation.activation_id = head.activation_id
                   WHERE installation.contract_hash = ?1"#,
                params![contract_hash.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(artifact_id, baseline, installed_at, activated_at)| {
            let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(artifact_id)?))?;
            if artifact.kind != ArtifactKind::Contract
                || artifact.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::Integrity(format!(
                    "contract {contract_hash} has an invalid artifact"
                )));
            }
            let contract: AgentContract = self.read_artifact_payload(&artifact)?;
            contract.validate()?;
            if contract.contract_hash != *contract_hash {
                return Err(StoreError::Integrity(format!(
                    "contract installation {contract_hash} payload hash diverges"
                )));
            }
            Ok(StoredContract {
                contract,
                artifact,
                baseline_contract_hash: baseline.map(ContentHash::new).transpose()?,
                installed_at: parse_time(&installed_at)?,
                activated_at: activated_at.map(|value| parse_time(&value)).transpose()?,
            })
        })
        .transpose()
    }

    fn apply_contract_catalogue_transition(
        &self,
        transaction: &Transaction<'_>,
        commit: &PolicyEvaluationCommit,
        transition: &PolicyTransition,
    ) -> StoreResult<()> {
        let PolicySubject::Contract(candidate_hash) = &commit.subject else {
            return Ok(());
        };
        let candidate = self
            .stored_contract_with_connection(transaction, candidate_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(candidate_hash.clone()))?;
        let Some(baseline_hash) = candidate.baseline_contract_hash.as_ref() else {
            return Err(StoreError::ContractActivationConflict(
                candidate.contract.purpose.clone(),
            ));
        };

        match (transition.from, transition.to) {
            (_, PolicyState::Contract(CandidatePolicyState::Active)) => {
                let candidate_policy_artifact =
                    commit
                        .candidate_policy
                        .as_ref()
                        .ok_or(StoreError::InvalidLearningCommit(
                            "contract_catalogue.candidate_policy",
                        ))?;
                let candidate_policy: CandidatePolicy =
                    self.read_artifact_payload(candidate_policy_artifact)?;
                if candidate_policy.candidate.artifact_id != candidate.artifact.artifact_id
                    || candidate_policy.baseline.kind != ArtifactKind::Contract
                    || candidate_policy.subject != commit.subject
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "contract_catalogue.candidate_policy_binding",
                    ));
                }
                let Some((current_hash, _)) =
                    contract_catalogue_head(transaction, &candidate.contract.purpose)?
                else {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                };
                let current = self
                    .stored_contract_with_connection(transaction, &current_hash)?
                    .ok_or_else(|| StoreError::MissingContractInstallation(current_hash.clone()))?;
                if current.contract.contract_hash != *baseline_hash
                    || candidate_policy.baseline.artifact_id != current.artifact.artifact_id
                    || !candidate_is_bounded(&current.contract, &candidate.contract)
                {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let activation_id = append_contract_activation(
                    transaction,
                    &candidate.contract.purpose,
                    Some(&current_hash),
                    candidate_hash,
                    Some(&transition.transition_id),
                    transition.created_at,
                )?;
                set_contract_catalogue_head(
                    transaction,
                    &candidate.contract.purpose,
                    candidate_hash,
                    activation_id,
                )?;
            }
            (PolicyState::Contract(CandidatePolicyState::Active), PolicyState::Contract(_)) => {
                let Some((current_hash, _)) =
                    contract_catalogue_head(transaction, &candidate.contract.purpose)?
                else {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                };
                if current_hash != *candidate_hash {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let baseline = self
                    .stored_contract_with_connection(transaction, baseline_hash)?
                    .ok_or_else(|| {
                        StoreError::MissingContractInstallation(baseline_hash.clone())
                    })?;
                if baseline.contract.purpose != candidate.contract.purpose {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let activation_id = append_contract_activation(
                    transaction,
                    &candidate.contract.purpose,
                    Some(candidate_hash),
                    baseline_hash,
                    Some(&transition.transition_id),
                    transition.created_at,
                )?;
                set_contract_catalogue_head(
                    transaction,
                    &candidate.contract.purpose,
                    baseline_hash,
                    activation_id,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn reserve_paper_session_with_binding(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        binding: Option<(&Artifact, &Artifact)>,
    ) -> StoreResult<SessionSlotReservation> {
        if reservation.session_key.trim().is_empty()
            || reservation.workflow.run.purpose != RunPurpose::Paper
            || reservation.workflow.graph.kind != ArtifactKind::WorkflowGraph
            || reservation.workflow.graph.artifact_id != reservation.workflow.run.graph_artifact_id
            || proposal.kind != ArtifactKind::WorkflowProposal
            || proposal.producer != "runtime.paper_provisioning"
            || proposal.lifecycle != ArtifactLifecycle::RunScoped
            || proposal
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&reservation.workflow.run.run_id)
            || reservation.setup_artifacts.iter().any(|artifact| {
                artifact.kind != ArtifactKind::EvidenceNeed
                    || artifact.lifecycle != ArtifactLifecycle::RunScoped
                    || artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref())
                        != Some(&reservation.workflow.run.run_id)
            })
        {
            return Err(StoreError::InvalidSessionSlot(
                reservation.session_key.clone(),
            ));
        }
        reservation.workflow.graph.validate()?;
        let graph: WorkflowGraph =
            serde_json::from_slice(&self.read_blob(&reservation.workflow.graph.blob)?)?;
        graph.validate()?;
        if graph.nodes != reservation.workflow.nodes
            || graph.topology_id != reservation.workflow.run.topology_id
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        let proposal_payload: WorkflowProposal =
            serde_json::from_slice(&self.read_blob(&proposal.blob)?)?;
        let expected_sources = reservation
            .setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            })
            .collect::<BTreeSet<_>>();
        let actual_sources = proposal
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let payload_needs = proposal_payload
            .tasks
            .values()
            .flat_map(|task| task.evidence_needs.iter().cloned())
            .collect::<BTreeSet<_>>();
        let expected_sources = expected_sources
            .into_iter()
            .chain(payload_needs)
            .collect::<BTreeSet<_>>();
        if actual_sources != expected_sources {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        if proposal_payload.topology_id != reservation.workflow.run.topology_id {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        proposal.validate()?;
        for artifact in &reservation.setup_artifacts {
            artifact.validate()?;
            self.read_blob(&artifact.blob)?;
        }

        let newly_reserved = {
            let mut connection = self.connection()?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            assert_daemon_lease(&transaction, lease, reservation.reserved_at)?;
            let exists = transaction
                .query_row(
                    "SELECT 1 FROM rebuild_session_slots WHERE session_key = ?1",
                    params![reservation.session_key],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            if exists.is_some() {
                transaction.commit()?;
                false
            } else {
                for artifact in &reservation.setup_artifacts {
                    insert_artifact(&transaction, artifact)?;
                }
                insert_artifact(&transaction, proposal)?;
                if let Some((runtime_manifest, approval)) = binding {
                    insert_artifact(&transaction, runtime_manifest)?;
                    insert_artifact(&transaction, approval)?;
                }
                Self::commit_workflow_transaction(&transaction, &reservation.workflow)?;
                transaction.execute(
                    "INSERT INTO rebuild_session_slots (session_key, run_id, topology_id, graph_artifact_id, run_created_at, scheduler_epoch, reserved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        reservation.session_key,
                        reservation.workflow.run.run_id.0,
                        reservation.workflow.run.topology_id,
                        reservation.workflow.run.graph_artifact_id.0.as_str(),
                        reservation.workflow.run.created_at.to_rfc3339(),
                        lease.epoch,
                        reservation.reserved_at.to_rfc3339(),
                    ],
                )?;
                if let Some((runtime_manifest, approval)) = binding {
                    transaction.execute(
                        "INSERT INTO rebuild_paper_approval_consumptions (approval_artifact_id, runtime_manifest_artifact_id, session_key, consumed_at) VALUES (?1, ?2, ?3, ?4)",
                        params![
                            approval.artifact_id.0.as_str(),
                            runtime_manifest.artifact_id.0.as_str(),
                            reservation.session_key,
                            reservation.reserved_at.to_rfc3339(),
                        ],
                    )?;
                }
                transaction.commit()?;
                true
            }
        };
        let slot = self
            .session_slot(&reservation.session_key)?
            .ok_or_else(|| StoreError::Integrity("session slot missing after commit".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved,
        })
    }

    fn commit_workflow_transaction(
        transaction: &Transaction<'_>,
        commit: &WorkflowCommit,
    ) -> StoreResult<()> {
        assert_workflow_input_artifacts(transaction, &commit.nodes)?;
        insert_artifact(transaction, &commit.graph)?;
        let inserted = transaction.execute(
            r#"INSERT INTO rebuild_runs
                (run_id, purpose, topology_id, graph_artifact_id, status, created_at)
                VALUES (?1, ?2, ?3, ?4, 'queued', ?5)"#,
            params![
                commit.run.run_id.0,
                enum_name(commit.run.purpose),
                commit.run.topology_id,
                commit.run.graph_artifact_id.0.as_str(),
                commit.run.created_at.to_rfc3339(),
            ],
        )?;
        if inserted != 1 {
            return Err(StoreError::DuplicateRun(commit.run.run_id.clone()));
        }
        for node in &commit.nodes {
            insert_task_node(transaction, &commit.run.run_id, node, commit.run.created_at)?;
        }
        for node in &commit.nodes {
            insert_node_dependencies(transaction, node)?;
        }
        transaction.execute(
            r#"INSERT INTO rebuild_workflow_revisions
                (run_id, revision, graph_artifact_id, created_at)
                VALUES (?1, 0, ?2, ?3)"#,
            params![
                commit.run.run_id.0,
                commit.run.graph_artifact_id.0.as_str(),
                commit.run.created_at.to_rfc3339(),
            ],
        )?;
        append_event(
            transaction,
            &commit.run.run_id,
            None,
            None,
            LifecycleEventType::WorkflowCreated,
            Some(&commit.graph.artifact_id),
            commit.run.created_at,
        )?;
        Ok(())
    }

    fn validate_execution_commitment_lineage(
        &self,
        connection: &Connection,
        commitment_artifact: &Artifact,
        commitment: &PaperCommitment,
        run_id: &RunId,
        session_key: &str,
    ) -> StoreResult<()> {
        let invalid = || StoreError::InvalidSessionSlot(session_key.to_owned());
        if commitment_artifact.kind != ArtifactKind::ExecutionCommitment
            || commitment_artifact.lifecycle != ArtifactLifecycle::Canonical
            || commitment.broker_session != session_key
            || commitment_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }

        let verdict_refs = commitment_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionVerdict)
            .cloned()
            .collect::<Vec<_>>();
        let context_refs = commitment_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionContext)
            .cloned()
            .collect::<Vec<_>>();
        if verdict_refs.len() != 1
            || context_refs.len() != 1
            || context_refs[0] != commitment.execution_context
            || !has_exact_source_refs(
                commitment_artifact,
                &[verdict_refs[0].clone(), context_refs[0].clone()],
            )
        {
            return Err(invalid());
        }

        let context_ref = &context_refs[0];
        let verdict_artifact = read_artifact(connection, &verdict_refs[0].artifact_id)?;
        if verdict_artifact.kind != ArtifactKind::ExecutionVerdict
            || verdict_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            || !has_exact_source_refs(&verdict_artifact, std::slice::from_ref(context_ref))
        {
            return Err(invalid());
        }
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&self.read_blob(&verdict_artifact.blob)?)?;
        let ExecutionVerdict::Accepted { execution_context } = verdict else {
            return Err(invalid());
        };
        if execution_context != *context_ref {
            return Err(invalid());
        }

        let context_artifact = read_artifact(connection, &context_ref.artifact_id)?;
        if context_artifact.kind != ArtifactKind::ExecutionContext
            || context_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }
        let context: ExecutionContext =
            serde_json::from_slice(&self.read_blob(&context_artifact.blob)?)?;
        context.validate_complete_plan_closure()?;
        if context.run_id != *run_id
            || context.broker_session.as_deref() != Some(session_key)
            || context.plan_hash.as_ref() != Some(&commitment.plan_hash)
        {
            return Err(invalid());
        }

        let context_sources = [
            context.decision_context.clone(),
            context.account_snapshot.clone().expect("validated closure"),
            context.quote_snapshot.clone().expect("validated closure"),
            context
                .market_clock_snapshot
                .clone()
                .expect("validated closure"),
            context.execution_plan.clone().expect("validated closure"),
        ];
        if !has_exact_source_refs(&context_artifact, &context_sources) {
            return Err(invalid());
        }

        let plan_refs = context_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionPlan)
            .collect::<Vec<_>>();
        if plan_refs.len() != 1 {
            return Err(invalid());
        }
        if context.execution_plan.as_ref() != Some(plan_refs[0]) {
            return Err(invalid());
        }
        let plan_artifact = read_artifact(connection, &plan_refs[0].artifact_id)?;
        if plan_artifact.kind != ArtifactKind::ExecutionPlan
            || plan_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }
        let plan: ExecutionPlan = serde_json::from_slice(&self.read_blob(&plan_artifact.blob)?)?;
        plan.validate()?;
        if !has_exact_source_refs(
            &plan_artifact,
            &[
                plan.decision_context.clone(),
                plan.account_snapshot.clone(),
                plan.quote_snapshot.clone(),
                plan.market_clock_snapshot.clone(),
            ],
        ) || plan.decision_context != context.decision_context
            || Some(&plan.account_snapshot) != context.account_snapshot.as_ref()
            || Some(&plan.quote_snapshot) != context.quote_snapshot.as_ref()
            || Some(&plan.market_clock_snapshot) != context.market_clock_snapshot.as_ref()
            || plan.broker_session != session_key
            || context.plan_hash.as_ref() != Some(&plan.plan_hash)
            || plan.plan_hash != commitment.plan_hash
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn validate_specialized_artifact(&self, artifact: &Artifact) -> StoreResult<()> {
        match artifact.kind {
            ArtifactKind::DeliberationNote => {
                let summary: akzio_domain::DeliberationSummary =
                    self.read_artifact_payload(artifact)?;
                summary.validate()?;
            }
            ArtifactKind::RetrospectiveDraft => {
                let draft: RetrospectiveDraft = self.read_artifact_payload(artifact)?;
                draft.validate()?;
                if artifact.lifecycle != ArtifactLifecycle::RunScoped {
                    return Err(StoreError::InvalidLearningCommit(
                        "retrospective_draft.lifecycle",
                    ));
                }
                let run_id = artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    .ok_or(StoreError::PermitOriginMismatch)?;
                for source in &artifact.source_refs {
                    let source_artifact = self.artifact(&source.artifact_id)?;
                    if source_artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref())
                        .is_some_and(|source_run| source_run != run_id)
                    {
                        return Err(StoreError::InvalidLearningCommit(
                            "retrospective_draft.cross_run_source",
                        ));
                    }
                }
            }
            ArtifactKind::Retrospective => {
                let retrospective: Retrospective = self.read_artifact_payload(artifact)?;
                retrospective.validate()?;
            }
            ArtifactKind::AttemptRelation => {
                let relation: AttemptRelation = self.read_artifact_payload(artifact)?;
                relation.validate()?;
                if artifact.lifecycle != ArtifactLifecycle::RunScoped {
                    return Err(StoreError::InvalidLearningCommit(
                        "attempt_relation.lifecycle",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_paper_effect_artifact(
        &self,
        effect: &ArtifactRef,
        run_id: &RunId,
    ) -> StoreResult<()> {
        let artifact = self.artifact(&effect.artifact_id)?;
        if effect.kind != artifact.kind
            || !matches!(
                artifact.kind,
                ArtifactKind::ExecutionCommitment | ArtifactKind::ExecutionReprice
            )
            || artifact.lifecycle != ArtifactLifecycle::Canonical
            || artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(StoreError::InvalidPaperEffect(effect.artifact_id.clone()));
        }
        match artifact.kind {
            ArtifactKind::ExecutionCommitment => {
                let payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                payload.validate()?;
            }
            ArtifactKind::ExecutionReprice => {
                let payload: PaperReprice =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                payload.validate()?;
            }
            _ => unreachable!("validated Paper effect kind"),
        }
        Ok(())
    }

    fn validate_attempt_commit(
        &self,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
    ) -> StoreResult<()> {
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        if status == TaskStatus::Succeeded && artifacts.is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "commit_attempt.artifacts",
            }));
        }
        for artifact in artifacts {
            artifact.validate()?;
            reject_generic_learning_artifact(artifact)?;
            self.read_blob(&artifact.blob)?;
            self.validate_specialized_artifact(artifact)?;
        }
        Ok(())
    }

    pub fn artifact(&self, artifact_id: &ArtifactId) -> StoreResult<Artifact> {
        let connection = self.connection()?;
        read_artifact(&connection, artifact_id)
    }

    pub fn artifacts_referencing(
        &self,
        source_artifact_id: &ArtifactId,
        kind: Option<ArtifactKind>,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection()?;
        let kind = kind.map(enum_name);
        self.verify_contract_catalogue_history(&connection)?;
        self.verify_policy_evaluation_history(&connection)?;

        let mut statement = connection.prepare(
            r#"SELECT r.artifact_id
               FROM rebuild_artifact_refs AS r
               JOIN rebuild_artifacts AS a ON a.artifact_id = r.artifact_id
               WHERE r.source_artifact_id = ?1 AND (?2 IS NULL OR a.kind = ?2)
               ORDER BY r.artifact_id ASC"#,
        )?;
        let ids = statement
            .query_map(params![source_artifact_id.0.as_str(), kind], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        ids.into_iter()
            .map(|id| read_artifact(&connection, &ArtifactId(ContentHash::new(id)?)))
            .collect()
    }

    /// Return the newest immutable artifact of a kind. Mutable state such as
    /// execution freeze is represented as an append-only artifact history;
    /// callers never receive a writable row handle.
    pub fn latest_artifact_by_kind(&self, kind: ArtifactKind) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let artifact_id = connection
            .query_row(
            "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY CASE WHEN lifecycle = 'canonical' THEN 0 ELSE 1 END, created_at DESC, artifact_id DESC LIMIT 1",
                params![enum_name(kind)],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        artifact_id
            .map(ContentHash::new)
            .transpose()?
            .map(ArtifactId)
            .map(|artifact_id| read_artifact(&connection, &artifact_id))
            .transpose()
    }

    /// Return newest immutable artifacts of one kind, newest first.
    /// Observer callers cannot request an unbounded Store scan.
    pub fn recent_artifacts_by_kind(
        &self,
        kind: ArtifactKind,
        limit: usize,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection()?;
        let limit = i64::try_from(limit.clamp(1, 500)).expect("bounded artifact limit fits i64");
        let mut statement = connection.prepare(
            "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY created_at DESC, artifact_id DESC LIMIT ?2",
        )?;
        let ids = statement
            .query_map(params![enum_name(kind), limit], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| read_artifact(&connection, &ArtifactId(ContentHash::new(id)?)))
            .collect()
    }

    fn workflow_revision_with_connection(
        &self,
        connection: &Connection,
        run_id: &RunId,
        revision: u64,
    ) -> StoreResult<WorkflowRevision> {
        let row = connection
            .query_row(
                r#"SELECT revision, graph_artifact_id, created_at
                   FROM rebuild_workflow_revisions
                   WHERE run_id = ?1 AND revision = ?2"#,
                params![run_id.0, revision],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::MissingWorkflowRevision {
                run_id: run_id.clone(),
                revision,
            })?;
        self.hydrate_workflow_revision(connection, row)
    }

    fn workflow_snapshot_with_connection(
        &self,
        connection: &Connection,
        run_id: &RunId,
    ) -> StoreResult<WorkflowSnapshot> {
        let run_row = connection
            .query_row(
                r#"SELECT purpose, topology_id, graph_artifact_id, status, created_at, finished_at
                   FROM rebuild_runs WHERE run_id = ?1"#,
                params![run_id.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::MissingRun(run_id.clone()))?;
        let (purpose, topology_id, graph_artifact_id, status, created_at, finished_at) = run_row;
        let run = StoredRun {
            run_id: run_id.clone(),
            purpose: parse_enum(&purpose)?,
            topology_id,
            graph_artifact_id: ArtifactId(ContentHash::new(graph_artifact_id)?),
            created_at: parse_time(&created_at)?,
        };
        let revision_row = connection
            .query_row(
                r#"SELECT revision, graph_artifact_id, created_at
                   FROM rebuild_workflow_revisions
                   WHERE run_id = ?1 ORDER BY revision DESC LIMIT 1"#,
                params![run_id.0],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::Integrity(format!("run {run_id} has no workflow revision"))
            })?;
        let revision = self.hydrate_workflow_revision(connection, revision_row)?;
        if revision.graph_artifact.artifact_id != run.graph_artifact_id
            || revision.graph.topology_id != run.topology_id
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }

        let raw_tasks = connection
            .prepare(
                r#"SELECT t.task_id, t.run_id, t.recipe_id, t.objective, t.contract_hash,
                          t.priority, t.budget_json, t.retry_json, t.on_failure,
                          t.parent_task_id, t.input_artifacts_json, t.status, t.ready_at,
                          t.lease_id, t.lease_epoch, t.active_attempt_id, t.lease_until,
                          t.worker_id, t.finished_at,
                          (SELECT COUNT(*) FROM rebuild_attempts AS a WHERE a.task_id = t.task_id)
                   FROM rebuild_tasks AS t
                   WHERE t.run_id = ?1 ORDER BY t.task_id ASC"#,
            )?
            .query_map(params![run_id.0], |row| {
                Ok((
                    row_to_node(row)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, u64>(14)?,
                    row.get::<_, Option<String>>(15)?,
                    row.get::<_, Option<String>>(16)?,
                    row.get::<_, Option<String>>(17)?,
                    row.get::<_, Option<String>>(18)?,
                    row.get::<_, u64>(19)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut tasks = Vec::with_capacity(raw_tasks.len());
        for (
            (task_run_id, mut node),
            task_status,
            ready_at,
            lease_id,
            epoch,
            active_attempt_id,
            lease_until,
            worker_id,
            task_finished_at,
            attempt_count,
        ) in raw_tasks
        {
            if task_run_id != *run_id {
                return Err(StoreError::WorkflowGraphMismatch);
            }
            node.dependencies = task_dependencies(connection, &node.task_id)?;
            let task_status = parse_task_status(&task_status)?;
            let active_attempt = match (lease_id, active_attempt_id, lease_until, worker_id) {
                (Some(lease_id), Some(attempt_id), Some(lease_until), Some(worker_id))
                    if task_status == TaskStatus::Running =>
                {
                    let attempt = connection
                        .query_row(
                            r#"SELECT run_id, task_id, lease_id, epoch, worker_id, status, started_at
                               FROM rebuild_attempts WHERE attempt_id = ?1"#,
                            params![attempt_id],
                            |row| {
                                Ok((
                                    row.get::<_, String>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, String>(2)?,
                                    row.get::<_, u64>(3)?,
                                    row.get::<_, String>(4)?,
                                    row.get::<_, String>(5)?,
                                    row.get::<_, String>(6)?,
                                ))
                            },
                        )
                        .optional()?
                        .ok_or_else(|| {
                            StoreError::Integrity(format!(
                                "active attempt {attempt_id} does not exist"
                            ))
                        })?;
                    if attempt.0 != run_id.0
                        || attempt.1 != node.task_id.0
                        || attempt.2 != lease_id
                        || attempt.3 != epoch
                        || attempt.4 != worker_id
                        || attempt.5 != "running"
                    {
                        return Err(StoreError::Integrity(format!(
                            "active attempt {attempt_id} does not match task {}",
                            node.task_id
                        )));
                    }
                    Some(StoredActiveAttempt {
                        permit: TaskWritePermit {
                            run_id: run_id.clone(),
                            task_id: node.task_id.clone(),
                            attempt_id: AttemptId(attempt_id),
                            lease_id: LeaseId(lease_id),
                            epoch,
                            contract_hash: node.contract_hash.clone(),
                        },
                        worker_id,
                        lease_until: parse_time(&lease_until)?,
                        started_at: parse_time(&attempt.6)?,
                    })
                }
                (None, None, None, None) if task_status != TaskStatus::Running => None,
                _ => {
                    return Err(StoreError::Integrity(format!(
                        "task {} has partial active attempt state",
                        node.task_id
                    )))
                }
            };
            tasks.push(StoredTaskSnapshot {
                node,
                status: task_status,
                ready_at: parse_time(&ready_at)?,
                active_attempt,
                attempt_count,
                finished_at: task_finished_at.as_deref().map(parse_time).transpose()?,
            });
        }
        let graph_nodes = revision
            .graph
            .nodes
            .iter()
            .cloned()
            .map(canonical_workflow_node)
            .map(|node| (node.task_id.clone(), node))
            .collect::<std::collections::BTreeMap<_, _>>();
        let stored_nodes = tasks
            .iter()
            .filter(|task| task.node.recipe_id.as_str() != POST_TERMINAL_WORKER_RECIPE_ID)
            .map(|task| canonical_workflow_node(task.node.clone()))
            .map(|node| (node.task_id.clone(), node))
            .collect::<std::collections::BTreeMap<_, _>>();
        if graph_nodes != stored_nodes {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        let event_cursor = connection.query_row(
            "SELECT COALESCE(MAX(event_id), 0) FROM rebuild_events WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, i64>(0),
        )?;
        let cancel_requested = connection
            .query_row(
                "SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1",
                params![run_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(WorkflowSnapshot {
            run,
            status: parse_enum(&status)?,
            finished_at: finished_at.as_deref().map(parse_time).transpose()?,
            revision,
            tasks,
            event_cursor,
            cancel_requested,
        })
    }

    fn hydrate_workflow_revision(
        &self,
        connection: &Connection,
        row: (i64, String, String),
    ) -> StoreResult<WorkflowRevision> {
        let revision = u64::try_from(row.0)
            .map_err(|_| StoreError::Integrity(format!("invalid workflow revision {}", row.0)))?;
        let graph_artifact = read_artifact(connection, &ArtifactId(ContentHash::new(row.1)?))?;
        if graph_artifact.kind != ArtifactKind::WorkflowGraph {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
        graph.validate()?;
        Ok(WorkflowRevision {
            revision,
            graph_artifact,
            graph,
            created_at: parse_time(&row.2)?,
        })
    }

    fn verify_workflow_history(
        &self,
        connection: &Connection,
        snapshot: &WorkflowSnapshot,
    ) -> StoreResult<()> {
        let mut previous: Option<WorkflowRevision> = None;
        for revision_number in 0..=snapshot.revision.revision {
            let revision = self.workflow_revision_with_connection(
                connection,
                &snapshot.run.run_id,
                revision_number,
            )?;
            if revision.graph.topology_id != snapshot.run.topology_id {
                return Err(StoreError::WorkflowGraphMismatch);
            }
            if let Some(previous) = &previous {
                if revision.created_at < previous.created_at
                    || revision.graph_artifact.source_refs.len() != 2
                    || !revision.graph_artifact.source_refs.iter().any(|reference| {
                        reference.artifact_id == previous.graph_artifact.artifact_id
                            && reference.kind == ArtifactKind::WorkflowGraph
                    })
                    || !revision
                        .graph_artifact
                        .source_refs
                        .iter()
                        .any(|reference| reference.kind == ArtifactKind::WorkflowProposal)
                {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
            }
            previous = Some(revision);
        }
        if previous.as_ref() != Some(&snapshot.revision) {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        Ok(())
    }

    fn trajectory_entry(&self, event: &StoredEvent) -> StoreResult<Option<TrajectoryEntry>> {
        let lifecycle = event.lifecycle_kind()?;
        let base = |artifact: Option<&Artifact>| TrajectoryEntry {
            cursor: event.cursor,
            task_id: event.task_id.clone(),
            attempt_id: event.attempt_id.clone(),
            turn: None,
            phase: None,
            assistant_text: None,
            event_type: event.event_type.clone(),
            artifact_id: event.artifact_id.clone(),
            artifact_kind: artifact.map(|value| value.kind),
            model: None,
            latency_millis: None,
            input_tokens: None,
            output_tokens: None,
            tool: None,
            deliberation: None,
            output_refs: Vec::new(),
        };

        match lifecycle {
            LifecycleEventType::AgentTurnStarted => Ok(Some(base(None))),
            LifecycleEventType::AgentTurn
            | LifecycleEventType::AgentTurnCompleted
            | LifecycleEventType::AgentTurnFailed
            | LifecycleEventType::AgentTurnRetryableFailed => {
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    return Ok(Some(base(None)));
                };
                let artifact = self.artifact(artifact_id)?;
                if artifact.kind != ArtifactKind::AgentTurn {
                    return Err(StoreError::Integrity(format!(
                        "trajectory event {} references {:?}, expected agent_turn",
                        event.cursor, artifact.kind
                    )));
                }
                let payload: StoredTrajectoryTurn =
                    match serde_json::from_slice(&self.read_blob(&artifact.blob)?) {
                        Ok(payload) => payload,
                        Err(_) => return Ok(Some(base(Some(&artifact)))),
                    };
                let mut model = payload.capability_snapshot.unwrap_or_default();
                model.contract_hash = payload.contract_hash;
                model.request_hash = payload.request_hash;
                model.capability_snapshot_hash = payload.capability_snapshot_hash;
                model.tool_set_hash = payload.tool_set_hash;
                let mut entry = base(Some(&artifact));
                entry.turn = payload.turn;
                entry.phase = payload.request.and_then(|request| request.phase);
                entry.assistant_text = payload
                    .response
                    .as_ref()
                    .and_then(|response| response.assistant_text.clone());
                entry.model = Some(model);
                let telemetry = payload
                    .response
                    .as_ref()
                    .and_then(|response| response.telemetry.as_ref())
                    .or(payload.telemetry.as_ref());
                entry.latency_millis = telemetry.and_then(|telemetry| telemetry.latency_millis);
                entry.input_tokens = telemetry.and_then(|telemetry| telemetry.input_tokens);
                entry.output_tokens = telemetry.and_then(|telemetry| telemetry.output_tokens);
                Ok(Some(entry))
            }
            LifecycleEventType::ToolCalled
            | LifecycleEventType::ToolCompleted
            | LifecycleEventType::ToolFailed => {
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    return Ok(None);
                };
                let artifact = self.artifact(artifact_id)?;
                if !matches!(
                    artifact.kind,
                    ArtifactKind::ToolCall | ArtifactKind::ToolResult
                ) {
                    return Err(StoreError::Integrity(format!(
                        "trajectory event {} references {:?}, expected tool artifact",
                        event.cursor, artifact.kind
                    )));
                }
                let payload: StoredTrajectoryToolArtifact =
                    match serde_json::from_slice(&self.read_blob(&artifact.blob)?) {
                        Ok(payload) => payload,
                        Err(_) => return Ok(Some(base(Some(&artifact)))),
                    };
                let call_id = payload
                    .call_id
                    .or_else(|| payload.call.as_ref().and_then(|call| call.call_id.clone()));
                let name = payload
                    .name
                    .or_else(|| payload.call.as_ref().and_then(|call| call.name.clone()));
                let mut entry = base(Some(&artifact));
                entry.tool = Some(TrajectoryToolLifecycle {
                    call_id,
                    name,
                    lifecycle: event.event_type.clone(),
                });
                Ok(Some(entry))
            }
            LifecycleEventType::DeliberationNoteCreated => {
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    return Ok(None);
                };
                let artifact = self.artifact(artifact_id)?;
                if artifact.kind != ArtifactKind::DeliberationNote {
                    return Err(StoreError::Integrity(format!(
                        "trajectory event {} references {:?}, expected deliberation_note",
                        event.cursor, artifact.kind
                    )));
                }
                let deliberation: DeliberationSummary =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                deliberation.validate()?;
                let mut entry = base(Some(&artifact));
                entry.deliberation = Some(deliberation);
                Ok(Some(entry))
            }
            LifecycleEventType::ArtifactCommitted => {
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    return Ok(None);
                };
                let artifact = self.artifact(artifact_id)?;
                if is_trajectory_redacted_kind(artifact.kind) {
                    return Ok(None);
                }
                let mut entry = base(Some(&artifact));
                entry.output_refs = trajectory_output_refs(&artifact);
                Ok(Some(entry))
            }
            _ => Ok(None),
        }
    }

    fn verify_outcome_schedule_history(&self, connection: &Connection) -> StoreResult<()> {
        let artifact_ids = connection
            .prepare(
                "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY artifact_id",
            )?
            .query_map(params![enum_name(ArtifactKind::OutcomeSchedule)], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for value in artifact_ids {
            let artifact_id = ArtifactId(ContentHash::new(value)?);
            let artifact = read_artifact(connection, &artifact_id)?;
            let (expected_lifecycle, allowed_purposes) =
                match artifact_run_purpose(connection, &artifact)? {
                    RunPurpose::Paper => (ArtifactLifecycle::Canonical, vec![RunPurpose::Paper]),
                    RunPurpose::Shadow => (
                        ArtifactLifecycle::RunScoped,
                        vec![RunPurpose::Paper, RunPurpose::Shadow],
                    ),
                    purpose => {
                        return Err(StoreError::Integrity(format!(
                            "outcome schedule {artifact_id} has invalid run purpose {purpose:?}"
                        )));
                    }
                };
            if artifact.lifecycle != expected_lifecycle {
                return Err(StoreError::Integrity(format!(
                    "outcome schedule {artifact_id} has invalid lifecycle"
                )));
            }
            let schedule: OutcomeSchedule =
                serde_json::from_slice(&self.read_blob(&artifact.blob)?).map_err(|error| {
                    StoreError::Integrity(format!(
                        "outcome schedule {artifact_id} has invalid payload: {error}"
                    ))
                })?;
            schedule.validate().map_err(|error| {
                StoreError::Integrity(format!(
                    "outcome schedule {artifact_id} fails validation: {error}"
                ))
            })?;
            let expected_sources = outcome_schedule_source_refs(&schedule);
            if !has_exact_source_refs(&artifact, &expected_sources) {
                return Err(StoreError::Integrity(format!(
                    "outcome schedule {artifact_id} has invalid source closure"
                )));
            }
            for reference in &expected_sources {
                let source = read_artifact(connection, &reference.artifact_id)?;
                if source.kind != reference.kind {
                    return Err(StoreError::Integrity(format!(
                        "outcome schedule {artifact_id} source kind is invalid"
                    )));
                }
                assert_artifact_from_allowed_purposes(connection, &source, &allowed_purposes)
                    .map_err(|error| {
                        StoreError::Integrity(format!(
                            "outcome schedule {artifact_id} source origin is invalid: {error}"
                        ))
                    })?;
            }
            self.validate_outcome_schedule_execution_lineage(
                connection,
                &schedule,
                &allowed_purposes,
            )
            .map_err(|error| {
                StoreError::Integrity(format!(
                    "outcome schedule {artifact_id} execution lineage is invalid: {error}"
                ))
            })?;
        }
        Ok(())
    }

    fn verify_contract_catalogue_history(&self, connection: &Connection) -> StoreResult<()> {
        let installations = connection
            .prepare(
                "SELECT contract_hash, contract_artifact_id, contract_id, contract_version, purpose, baseline_contract_hash FROM rebuild_contract_installations ORDER BY installed_at, contract_hash",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut contracts = BTreeMap::new();
        for (hash, artifact_id, contract_id, version, purpose, baseline) in installations {
            let contract_hash = ContentHash::new(hash)?;
            let stored = self
                .stored_contract_with_connection(connection, &contract_hash)?
                .ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "contract installation {contract_hash} disappeared"
                    ))
                })?;
            if stored.artifact.artifact_id.0.as_str() != artifact_id
                || stored.contract.contract_id.0 != contract_id
                || i64::from(stored.contract.version) != version
                || stored.contract.purpose.as_str() != purpose
                || stored
                    .baseline_contract_hash
                    .as_ref()
                    .map(ContentHash::as_str)
                    != baseline.as_deref()
            {
                return Err(StoreError::Integrity(format!(
                    "contract installation {contract_hash} metadata disagrees with payload"
                )));
            }
            if let Some(baseline_hash) = &stored.baseline_contract_hash {
                let baseline_contract = self
                    .stored_contract_with_connection(connection, baseline_hash)?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "candidate contract {contract_hash} has missing baseline {baseline_hash}"
                        ))
                    })?;
                if !candidate_is_bounded(&baseline_contract.contract, &stored.contract) {
                    return Err(StoreError::Integrity(format!(
                        "candidate contract {contract_hash} exceeds its installed baseline"
                    )));
                }
            }
            contracts.insert(contract_hash, stored);
        }

        let activations = connection
            .prepare(
                "SELECT activation_id, purpose, previous_contract_hash, contract_hash, policy_transition_id FROM rebuild_contract_activations ORDER BY activation_id",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut latest = BTreeMap::<String, (i64, ContentHash)>::new();
        for (activation_id, purpose, previous, hash, transition_id) in activations {
            let contract_hash = ContentHash::new(hash)?;
            let previous = previous.map(ContentHash::new).transpose()?;
            let expected_previous = latest.get(&purpose).map(|(_, hash)| hash.clone());
            if previous != expected_previous {
                return Err(StoreError::Integrity(format!(
                    "contract activation {activation_id} is not the next history entry for {purpose}"
                )));
            }
            let contract = contracts.get(&contract_hash).ok_or_else(|| {
                StoreError::Integrity(format!(
                    "contract activation {activation_id} references unknown contract {contract_hash}"
                ))
            })?;
            if contract.contract.purpose.as_str() != purpose {
                return Err(StoreError::Integrity(format!(
                    "contract activation {activation_id} purpose disagrees with its contract"
                )));
            }
            match (previous.as_ref(), transition_id) {
                (None, None) if contract.baseline_contract_hash.is_none() => {}
                (Some(previous_hash), None) => {
                    let previous_contract = contracts.get(previous_hash).ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "contract activation {activation_id} canonical upgrade has no previous contract"
                        ))
                    })?;
                    if contract.baseline_contract_hash.as_ref() != Some(previous_hash)
                        || contract.contract.contract_id != previous_contract.contract.contract_id
                        || contract.contract.version <= previous_contract.contract.version
                        || !candidate_is_bounded(&previous_contract.contract, &contract.contract)
                    {
                        return Err(StoreError::Integrity(format!(
                            "contract activation {activation_id} is not a valid canonical upgrade"
                        )));
                    }
                }
                (Some(previous_hash), Some(transition_id)) => {
                    let transition =
                        read_policy_transition(connection, &PolicyTransitionId(transition_id))?
                            .ok_or_else(|| {
                                StoreError::Integrity(format!(
                                    "contract activation {activation_id} has no policy transition"
                                ))
                            })?;
                    let promoted = transition.transition.subject
                        == PolicySubject::Contract(contract_hash.clone())
                        && transition.transition.to
                            == PolicyState::Contract(CandidatePolicyState::Active)
                        && contract.baseline_contract_hash.as_ref() == Some(previous_hash);
                    let rolled_back = transition.transition.subject
                        == PolicySubject::Contract(previous_hash.clone())
                        && transition.transition.from
                            == PolicyState::Contract(CandidatePolicyState::Active)
                        && contract_hash
                            == contracts
                                .get(previous_hash)
                                .and_then(|candidate| candidate.baseline_contract_hash.as_ref())
                                .cloned()
                                .ok_or_else(|| {
                                    StoreError::Integrity(format!(
                                        "contract activation {activation_id} rollback has no baseline"
                                    ))
                                })?;
                    if !promoted && !rolled_back {
                        return Err(StoreError::Integrity(format!(
                            "contract activation {activation_id} is not a valid promotion or rollback"
                        )));
                    }
                }
                _ => {
                    return Err(StoreError::Integrity(format!(
                        "contract activation {activation_id} has an invalid history binding"
                    )));
                }
            }
            latest.insert(purpose, (activation_id, contract_hash));
        }

        let heads = connection
            .prepare(
                "SELECT purpose, contract_hash, activation_id FROM rebuild_contract_catalogue_heads ORDER BY purpose",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if heads.len() != latest.len() {
            return Err(StoreError::Integrity(
                "contract catalogue head count disagrees with activation history".to_owned(),
            ));
        }
        for (purpose, contract_hash, activation_id) in heads {
            let contract_hash = ContentHash::new(contract_hash)?;
            if latest.get(&purpose) != Some(&(activation_id, contract_hash)) {
                return Err(StoreError::Integrity(format!(
                    "contract catalogue head for {purpose} is stale"
                )));
            }
        }
        Ok(())
    }

    fn verify_policy_evaluation_history(&self, connection: &Connection) -> StoreResult<()> {
        let evaluation_ids = connection
            .prepare(
                "SELECT evaluation_artifact_id FROM rebuild_policy_evaluations \
                 ORDER BY event_cursor",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut subject_history: BTreeMap<String, (i64, PolicyState)> = BTreeMap::new();

        for value in evaluation_ids {
            let evaluation_artifact_id = ArtifactId(ContentHash::new(value)?);
            let stored =
                read_policy_evaluation(connection, &evaluation_artifact_id)?.ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} disappeared"
                    ))
                })?;
            stored.subject.validate()?;
            if !stored.subject.accepts_state(stored.from)
                || !stored.subject.accepts_state(stored.to)
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} has incompatible subject state"
                )));
            }
            let subject_id = stored.subject.subject_id();
            let (previous_consumed_cursor, expected_from) = subject_history
                .get(&subject_id)
                .copied()
                .unwrap_or((0, stored.subject.initial_state()));
            if stored.from != expected_from
                || stored.consumed_pair_cursor < previous_consumed_cursor
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} breaks subject history"
                )));
            }

            let outcome_artifact = read_artifact(connection, &stored.outcome_artifact_id)?;
            let experience_artifact = read_artifact(connection, &stored.experience_artifact_id)?;
            let evaluation_artifact = read_artifact(connection, &stored.evaluation_artifact_id)?;
            for (artifact, expected_kind) in [
                (&outcome_artifact, ArtifactKind::Outcome),
                (&experience_artifact, ArtifactKind::Experience),
                (&evaluation_artifact, ArtifactKind::Evaluation),
            ] {
                if artifact.kind != expected_kind
                    || artifact.lifecycle != ArtifactLifecycle::Canonical
                    || artifact_run_purpose(connection, artifact)? != RunPurpose::Paper
                {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} has invalid canonical artifact"
                    )));
                }
            }

            let outcome: Outcome =
                serde_json::from_slice(&self.read_blob(&outcome_artifact.blob)?)?;
            outcome.validate_sealed()?;
            let schedule = self.read_outcome_schedule_with_connection(
                connection,
                &outcome,
                &[RunPurpose::Paper],
            )?;
            let experience: Experience =
                serde_json::from_slice(&self.read_blob(&experience_artifact.blob)?)?;
            experience.validate()?;
            let evaluation: Evaluation =
                serde_json::from_slice(&self.read_blob(&evaluation_artifact.blob)?)?;
            evaluation.validate()?;

            let outcome_ref = ArtifactRef {
                artifact_id: outcome_artifact.artifact_id.clone(),
                kind: ArtifactKind::Outcome,
            };
            let experience_ref = ArtifactRef {
                artifact_id: experience_artifact.artifact_id.clone(),
                kind: ArtifactKind::Experience,
            };
            if experience.subject != stored.subject
                || experience.policy_state != stored.from
                || experience.outcome != outcome_ref
                || experience.decision != schedule.decision
                || experience.decision_context != schedule.decision_context
                || experience.execution_context != schedule.execution_context
                || evaluation.outcome != outcome_ref
                || evaluation.experience != experience_ref
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} lineage is invalid"
                )));
            }

            match (&stored.subject, &stored.candidate_policy_artifact_id) {
                (PolicySubject::Memory(_), None) => {}
                (PolicySubject::Memory(_), Some(_)) => {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} binds a memory candidate"
                    )));
                }
                (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} has no candidate policy"
                    )));
                }
                (_, Some(candidate_policy_artifact_id)) => {
                    let candidate = read_artifact(connection, candidate_policy_artifact_id)?;
                    if candidate.kind != ArtifactKind::CandidatePolicy
                        || candidate.lifecycle != ArtifactLifecycle::Canonical
                        || artifact_run_purpose(connection, &candidate)? != RunPurpose::Paper
                    {
                        return Err(StoreError::Integrity(format!(
                            "policy evaluation {evaluation_artifact_id} has invalid candidate policy"
                        )));
                    }
                }
            }

            match &stored.transition_id {
                Some(transition_id) => {
                    let transition = read_policy_transition(connection, transition_id)?
                        .ok_or_else(|| {
                            StoreError::Integrity(format!(
                                "policy evaluation {evaluation_artifact_id} references missing transition {transition_id}"
                            ))
                        })?;
                    if transition.transition.subject != stored.subject
                        || transition.transition.from != stored.from
                        || transition.transition.to != stored.to
                        || transition.transition.evaluation.artifact_id
                            != stored.evaluation_artifact_id
                        || transition.run_id != stored.run_id
                    {
                        return Err(StoreError::Integrity(format!(
                            "policy evaluation {evaluation_artifact_id} disagrees with transition {transition_id}"
                        )));
                    }
                }
                None if stored.from != stored.to => {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} changed state without transition"
                    )));
                }
                None => {}
            }

            let event = connection
                .query_row(
                    "SELECT run_id, event_type, artifact_id, created_at \
                     FROM rebuild_events WHERE event_id = ?1",
                    params![stored.event_cursor],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .optional()?
                .ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} has no durable event"
                    ))
                })?;
            if event.0 != stored.run_id.0
                || event.1 != "policy.evaluated"
                || event.2.as_deref() != Some(stored.evaluation_artifact_id.0.as_str())
                || parse_time(&event.3)? != stored.completed_at
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} event is invalid"
                )));
            }

            if stored.consumed_pair_cursor < 0
                || (stored.consumed_pair_cursor != 0
                    && stored.consumed_pair_cursor >= stored.event_cursor)
            {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} consumed invalid shadow cursor"
                )));
            }
            if stored.consumed_pair_cursor > previous_consumed_cursor {
                let boundary_exists = connection
                    .query_row(
                        "SELECT 1 FROM rebuild_shadow_pairs \
                         WHERE subject_id = ?1 AND pair_event_cursor = ?2",
                        params![subject_id, stored.consumed_pair_cursor],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if !boundary_exists {
                    return Err(StoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} consumed non-pair cursor"
                    )));
                }
            }
            subject_history.insert(subject_id, (stored.consumed_pair_cursor, stored.to));
        }

        let head_subjects = connection
            .prepare(
                "SELECT subject_json FROM rebuild_policy_consumption_heads ORDER BY subject_id",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for subject_json in head_subjects {
            let subject: PolicySubject = serde_json::from_str(&subject_json)?;
            subject.validate()?;
            let head = read_policy_consumption_head(connection, &subject)?.ok_or_else(|| {
                StoreError::Integrity(format!(
                    "policy consumption head {} disappeared",
                    subject.subject_id()
                ))
            })?;
            let latest_id = connection.query_row(
                "SELECT evaluation_artifact_id FROM rebuild_policy_evaluations \
                 WHERE subject_id = ?1 ORDER BY event_cursor DESC LIMIT 1",
                params![subject.subject_id()],
                |row| row.get::<_, String>(0),
            )?;
            let latest =
                read_policy_evaluation(connection, &ArtifactId(ContentHash::new(latest_id)?))?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "policy consumption head {} has no evaluation",
                            subject.subject_id()
                        ))
                    })?;
            if head.subject != latest.subject
                || head.consumed_pair_cursor != latest.consumed_pair_cursor
                || head.evaluation_artifact_id != latest.evaluation_artifact_id
                || head.evaluation_cursor != latest.event_cursor
                || head.updated_at != latest.completed_at
            {
                return Err(StoreError::Integrity(format!(
                    "policy consumption head {} does not match latest evaluation",
                    subject.subject_id()
                )));
            }
        }

        let orphan_evaluation = connection
            .query_row(
                r#"SELECT e.evaluation_artifact_id
                   FROM rebuild_policy_evaluations AS e
                   LEFT JOIN rebuild_policy_consumption_heads AS h
                     ON h.subject_id = e.subject_id
                   WHERE h.subject_id IS NULL LIMIT 1"#,
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(evaluation_id) = orphan_evaluation {
            return Err(StoreError::Integrity(format!(
                "policy evaluation {evaluation_id} has no consumption head"
            )));
        }

        Ok(())
    }

    fn verify_candidate_policy_history(&self, connection: &Connection) -> StoreResult<()> {
        let artifact_ids = connection
            .prepare(
                "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY artifact_id",
            )?
            .query_map(params![enum_name(ArtifactKind::CandidatePolicy)], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for value in artifact_ids {
            let artifact_id = ArtifactId(ContentHash::new(value)?);
            let artifact = read_artifact(connection, &artifact_id)?;
            if artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} is noncanonical"
                )));
            }
            assert_artifact_from_paper_with_connection(connection, &artifact).map_err(|error| {
                StoreError::Integrity(format!(
                    "candidate policy {artifact_id} has invalid origin: {error}"
                ))
            })?;
            let policy: CandidatePolicy = self.read_artifact_payload(&artifact)?;
            policy.validate()?;
            if !has_exact_source_refs(
                &artifact,
                &[
                    policy.baseline.clone(),
                    policy.candidate.clone(),
                    policy.source_evaluation.clone(),
                ],
            ) {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} has invalid source closure"
                )));
            }
            let evaluation =
                read_policy_evaluation(connection, &policy.source_evaluation.artifact_id)?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "candidate policy {artifact_id} has no source evaluation"
                        ))
                    })?;
            if evaluation.subject != policy.subject
                || evaluation.completed_at != policy.created_at
                || evaluation.candidate_policy_artifact_id.as_ref() != Some(&artifact_id)
            {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} disagrees with source evaluation"
                )));
            }
            self.validate_candidate_policy_sources(connection, &policy)
                .map_err(|error| {
                    StoreError::Integrity(format!(
                        "candidate policy {artifact_id} has invalid binding: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    fn validate_policy_evaluation_commit_with_connection(
        &self,
        connection: &Connection,
        commit: &PolicyEvaluationCommit,
    ) -> StoreResult<()> {
        for (artifact, kind) in [
            (&commit.outcome, ArtifactKind::Outcome),
            (&commit.final_retrospective, ArtifactKind::Retrospective),
            (&commit.experience, ArtifactKind::Experience),
            (&commit.evaluation, ArtifactKind::Evaluation),
        ] {
            artifact.validate()?;
            self.read_blob(&artifact.blob)?;
            if artifact.kind != kind || artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.kind_or_lifecycle",
                ));
            }
        }
        if let Some(candidate_policy) = &commit.candidate_policy {
            candidate_policy.validate()?;
            self.read_blob(&candidate_policy.blob)?;
            if candidate_policy.kind != ArtifactKind::CandidatePolicy
                || candidate_policy.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.kind_or_lifecycle",
                ));
            }
        }
        let outcome: Outcome = self.read_artifact_payload(&commit.outcome)?;
        outcome.validate()?;
        if !outcome.is_sealed() {
            return Err(StoreError::UnsealedOutcome(
                commit.outcome.artifact_id.clone(),
            ));
        }
        let schedule =
            self.read_outcome_schedule_with_connection(connection, &outcome, &[RunPurpose::Paper])?;
        let final_retrospective: akzio_domain::Retrospective =
            self.read_artifact_payload(&commit.final_retrospective)?;
        final_retrospective.validate()?;
        if final_retrospective.horizon != OutcomeHorizon::T5
            || final_retrospective.status != akzio_domain::RetrospectiveStatus::Complete
            || final_retrospective.outcome.artifact_id != commit.outcome.artifact_id
            || final_retrospective.outcome.kind != ArtifactKind::Outcome
        {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.final_retrospective",
            ));
        }
        let experience: Experience = self.read_artifact_payload(&commit.experience)?;
        experience.validate()?;
        let evaluation: Evaluation = self.read_artifact_payload(&commit.evaluation)?;
        evaluation.validate()?;

        for reference in std::iter::once(&outcome.schedule)
            .chain(outcome.market_evidence.iter())
            .chain([
                &experience.decision,
                &experience.decision_context,
                &experience.execution_context,
                &experience.policy_verdict,
            ])
        {
            let source = read_artifact(connection, &reference.artifact_id)?;
            if source.kind != reference.kind {
                return Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.source_kind",
                ));
            }
            assert_artifact_from_paper_with_connection(connection, &source)?;
        }

        let outcome_ref = ArtifactRef {
            artifact_id: commit.outcome.artifact_id.clone(),
            kind: ArtifactKind::Outcome,
        };
        let experience_ref = ArtifactRef {
            artifact_id: commit.experience.artifact_id.clone(),
            kind: ArtifactKind::Experience,
        };
        let evaluation_ref = ArtifactRef {
            artifact_id: commit.evaluation.artifact_id.clone(),
            kind: ArtifactKind::Evaluation,
        };
        let retrospective_ref = ArtifactRef {
            artifact_id: commit.final_retrospective.artifact_id.clone(),
            kind: ArtifactKind::Retrospective,
        };
        if !commit
            .final_retrospective
            .source_refs
            .contains(&outcome_ref)
        {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.final_retrospective_source_refs",
            ));
        }
        match (&commit.subject, &commit.candidate_policy) {
            (PolicySubject::Memory(_), None) => {}
            (PolicySubject::Memory(_), Some(_)) => {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.memory_subject",
                ));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.missing",
                ));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), Some(artifact)) => {
                let candidate_policy: CandidatePolicy = self.read_artifact_payload(artifact)?;
                candidate_policy.validate()?;
                if candidate_policy.subject != commit.subject
                    || candidate_policy.source_evaluation != evaluation_ref
                    || candidate_policy.created_at != commit.completed_at
                    || !has_exact_source_refs(
                        artifact,
                        &[
                            candidate_policy.baseline.clone(),
                            candidate_policy.candidate.clone(),
                            candidate_policy.source_evaluation.clone(),
                        ],
                    )
                {
                    return Err(StoreError::InvalidLearningCommit("candidate_policy.links"));
                }
                self.validate_candidate_policy_sources(connection, &candidate_policy)?;
            }
        }
        commit.subject.validate()?;
        if !commit.subject.accepts_state(commit.from) || !commit.subject.accepts_state(commit.to) {
            return Err(StoreError::InvalidLearningCommit(
                "policy_evaluation.subject_state",
            ));
        }
        let transition_matches = match &commit.transition {
            Some(transition) => {
                transition.validate()?;
                transition.subject == commit.subject
                    && transition.from == commit.from
                    && transition.to == commit.to
                    && transition.evaluation == evaluation_ref
                    && transition.created_at == commit.completed_at
            }
            None => commit.from == commit.to,
        };
        if experience.outcome != outcome_ref
            || evaluation.outcome != outcome_ref
            || evaluation.experience != experience_ref
            || !transition_matches
            || experience.subject != commit.subject
            || experience.policy_state != commit.from
            || experience.decision != schedule.decision
            || experience.decision_context != schedule.decision_context
            || experience.execution_context != schedule.execution_context
        {
            return Err(StoreError::InvalidLearningCommit("learning_artifact.links"));
        }
        if !has_exact_source_refs(
            &commit.outcome,
            &std::iter::once(outcome.schedule.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .collect::<Vec<_>>(),
        ) || !has_exact_source_refs(
            &commit.experience,
            &[
                experience.decision.clone(),
                experience.decision_context.clone(),
                experience.execution_context.clone(),
                experience.policy_verdict.clone(),
                experience.outcome.clone(),
                retrospective_ref.clone(),
            ],
        ) || !has_exact_source_refs(
            &commit.evaluation,
            &[
                evaluation.outcome.clone(),
                evaluation.experience.clone(),
                retrospective_ref,
            ],
        ) {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.source_refs",
            ));
        }
        Ok(())
    }

    fn validate_candidate_policy_sources(
        &self,
        connection: &Connection,
        policy: &CandidatePolicy,
    ) -> StoreResult<()> {
        let baseline =
            read_required_artifact(connection, &policy.baseline, "candidate_policy.baseline")?;
        let candidate =
            read_required_artifact(connection, &policy.candidate, "candidate_policy.candidate")?;
        match &policy.subject {
            PolicySubject::Memory(_) => Err(StoreError::InvalidLearningCommit(
                "candidate_policy.memory_subject",
            )),
            PolicySubject::Contract(candidate_hash) => {
                if baseline.lifecycle != ArtifactLifecycle::Canonical
                    || candidate.lifecycle != ArtifactLifecycle::Canonical
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.contract_lifecycle",
                    ));
                }
                let baseline_contract: AgentContract = self.read_artifact_payload(&baseline)?;
                let candidate_contract: AgentContract = self.read_artifact_payload(&candidate)?;
                baseline_contract.validate()?;
                candidate_contract.validate()?;
                if &candidate_contract.contract_hash != candidate_hash
                    || !baseline_contract.permits_candidate(&candidate_contract)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.contract_binding",
                    ));
                }
                Ok(())
            }
            PolicySubject::Topology(topology_id) => {
                let baseline_graph: WorkflowGraph = self.read_artifact_payload(&baseline)?;
                let candidate_graph: WorkflowGraph = self.read_artifact_payload(&candidate)?;
                baseline_graph.validate()?;
                candidate_graph.validate()?;
                if candidate_graph.topology_id != topology_id.0
                    || workflow_graph_run_purpose(connection, &baseline.artifact_id)?
                        != RunPurpose::Paper
                    || workflow_graph_run_purpose(connection, &candidate.artifact_id)?
                        != RunPurpose::Shadow
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.topology_binding",
                    ));
                }
                Ok(())
            }
        }
    }

    fn assert_shadow_pair_sources_with_connection(
        &self,
        connection: &Connection,
        completion: &ShadowPairCompletion,
    ) -> StoreResult<()> {
        let parent_decision = read_required_artifact(
            connection,
            &completion.parent_decision,
            "shadow_pair.parent_decision",
        )?;
        let execution_context = read_required_artifact(
            connection,
            &completion.execution_context,
            "shadow_pair.execution_context",
        )?;
        let candidate_decision = read_required_artifact(
            connection,
            &completion.candidate_decision,
            "shadow_pair.candidate_decision",
        )?;
        let parent_outcome_artifact = read_required_artifact(
            connection,
            &completion.parent_outcome,
            "shadow_pair.parent_outcome",
        )?;
        let candidate_outcome_artifact = read_required_artifact(
            connection,
            &completion.candidate_outcome,
            "shadow_pair.candidate_outcome",
        )?;

        assert_canonical_paper_artifact(connection, &parent_decision)?;
        assert_artifact_from_paper_with_connection(connection, &execution_context)?;
        assert_canonical_paper_artifact(connection, &parent_outcome_artifact)?;
        assert_shadow_candidate_artifact(connection, &candidate_decision)?;
        assert_shadow_candidate_artifact(connection, &candidate_outcome_artifact)?;
        assert_candidate_decision_binding(connection, &candidate_decision, completion)?;

        let parent_outcome: Outcome =
            serde_json::from_slice(&self.read_blob(&parent_outcome_artifact.blob)?)?;
        let candidate_outcome: Outcome =
            serde_json::from_slice(&self.read_blob(&candidate_outcome_artifact.blob)?)?;
        parent_outcome.validate_sealed()?;
        candidate_outcome.validate_sealed()?;

        let parent_schedule = self.read_outcome_schedule_with_connection(
            connection,
            &parent_outcome,
            &[RunPurpose::Paper],
        )?;
        let candidate_schedule = self.read_outcome_schedule_with_connection(
            connection,
            &candidate_outcome,
            &[RunPurpose::Paper, RunPurpose::Shadow],
        )?;
        if parent_schedule.decision != completion.parent_decision
            || candidate_schedule.decision != completion.candidate_decision
            || parent_schedule.execution_context != completion.execution_context
            || candidate_schedule.execution_context != completion.execution_context
        {
            return Err(StoreError::InvalidLearningCommit(
                "shadow_pair.schedule_binding",
            ));
        }
        Ok(())
    }

    fn read_artifact_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> StoreResult<T> {
        Ok(serde_json::from_slice(&self.read_blob(&artifact.blob)?)?)
    }

    fn read_outcome_schedule_with_connection(
        &self,
        connection: &Connection,
        outcome: &Outcome,
        allowed_purposes: &[RunPurpose],
    ) -> StoreResult<OutcomeSchedule> {
        if outcome.schedule.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit("outcome.schedule_kind"));
        }
        let schedule_artifact = read_artifact(connection, &outcome.schedule.artifact_id)?;
        if schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_artifact",
            ));
        }
        let schedule_purpose = artifact_run_purpose(connection, &schedule_artifact)?;
        let expected_lifecycle = match schedule_purpose {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            RunPurpose::Shadow => ArtifactLifecycle::RunScoped,
            _ => {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome.schedule_artifact",
                ));
            }
        };
        if schedule_artifact.lifecycle != expected_lifecycle {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_artifact",
            ));
        }
        assert_artifact_from_allowed_purposes(connection, &schedule_artifact, allowed_purposes)?;
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&self.read_blob(&schedule_artifact.blob)?)?;
        schedule.validate()?;
        if schedule.outcome_id != outcome.outcome_id {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_identity",
            ));
        }

        let expected = outcome_schedule_source_refs(&schedule);
        if !has_exact_source_refs(&schedule_artifact, &expected) {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_schedule.source_refs",
            ));
        }
        for reference in &expected {
            let artifact = read_artifact(connection, &reference.artifact_id)?;
            if artifact.kind != reference.kind {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome_schedule.source_kind",
                ));
            }
            assert_artifact_from_allowed_purposes(connection, &artifact, allowed_purposes)?;
        }
        self.validate_outcome_schedule_execution_lineage(connection, &schedule, allowed_purposes)?;
        Ok(schedule)
    }

    fn validate_outcome_schedule_execution_lineage(
        &self,
        connection: &Connection,
        schedule: &OutcomeSchedule,
        allowed_purposes: &[RunPurpose],
    ) -> StoreResult<()> {
        let verdict_ref = match &schedule.execution {
            OutcomeExecutionLineage::NoOrder { execution_verdict } => execution_verdict,
            OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict, ..
            } => execution_verdict,
        };
        let verdict_artifact = read_artifact(connection, &verdict_ref.artifact_id)?;
        if verdict_artifact.kind != ArtifactKind::ExecutionVerdict {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_schedule.execution_verdict_kind",
            ));
        }
        assert_artifact_from_allowed_purposes(connection, &verdict_artifact, allowed_purposes)?;
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&self.read_blob(&verdict_artifact.blob)?)?;
        verdict.validate()?;

        match (&schedule.execution, verdict) {
            (
                OutcomeExecutionLineage::NoOrder { execution_verdict },
                ExecutionVerdict::NoOrder { no_order },
            ) if execution_verdict == verdict_ref
                && no_order.execution_context == schedule.execution_context =>
            {
                if !verdict_artifact
                    .source_refs
                    .iter()
                    .any(|reference| reference == &schedule.execution_context)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.no_order_context",
                    ));
                }
            }
            (
                OutcomeExecutionLineage::ReconciledPaper {
                    execution_verdict,
                    commitment,
                    reconciliation,
                },
                ExecutionVerdict::Accepted { execution_context },
            ) if execution_verdict == verdict_ref
                && execution_context == schedule.execution_context =>
            {
                let commitment_artifact = read_artifact(connection, &commitment.artifact_id)?;
                if commitment_artifact.kind != ArtifactKind::ExecutionCommitment {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.commitment_kind",
                    ));
                }
                assert_artifact_from_allowed_purposes(
                    connection,
                    &commitment_artifact,
                    allowed_purposes,
                )?;
                let commitment_payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&commitment_artifact.blob)?)?;
                commitment_payload.validate()?;
                if commitment_payload.execution_context != schedule.execution_context
                    || !commitment_artifact
                        .source_refs
                        .iter()
                        .any(|reference| reference == execution_verdict)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.commitment_lineage",
                    ));
                }

                let reconciliation_artifact =
                    read_artifact(connection, &reconciliation.artifact_id)?;
                if reconciliation_artifact.kind != ArtifactKind::Reconciliation {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.reconciliation_kind",
                    ));
                }
                assert_artifact_from_allowed_purposes(
                    connection,
                    &reconciliation_artifact,
                    allowed_purposes,
                )?;
                let reconciliation_payload: Reconciliation =
                    serde_json::from_slice(&self.read_blob(&reconciliation_artifact.blob)?)?;
                reconciliation_payload.validate()?;
                if reconciliation_payload.commitment != *commitment
                    || !reconciliation_artifact
                        .source_refs
                        .iter()
                        .any(|reference| reference == commitment)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.reconciliation_lineage",
                    ));
                }
            }
            _ => {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome_schedule.execution_lineage",
                ));
            }
        }
        Ok(())
    }
}

fn sync_file(path: &Path) -> StoreResult<()> {
    let file = fs::File::open(path).map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    file.sync_all().map_err(|source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn secure_directory(path: &Path) -> StoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)
            .map_err(|source| StoreError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

fn secure_file(path: &Path) -> StoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)
            .map_err(|source| StoreError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(path, permissions).map_err(|source| StoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    Ok(())
}

fn initialize(connection: &mut Connection, root: &Path) -> StoreResult<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS rebuild_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
    )?;
    let version = connection
        .query_row(
            "SELECT value FROM rebuild_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if version.is_some()
        && !table_has_column(
            connection,
            "rebuild_policy_evaluations",
            "candidate_policy_artifact_id",
        )?
    {
        return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }
    if let Some(value) = version.as_deref() {
        if value != STORE_SCHEMA_VERSION.to_string() {
            return Err(StoreError::IncompatibleStoreRoot(PathBuf::from(
                DATABASE_FILE,
            )));
        }
    }
    connection.execute_batch(
        "BEGIN;
        CREATE TABLE IF NOT EXISTS rebuild_blobs (
           blob_hash TEXT PRIMARY KEY,
           logical_bytes INTEGER NOT NULL,
           stored_bytes INTEGER NOT NULL,
           encoding TEXT NOT NULL,
           payload BLOB NOT NULL
         );
        CREATE TABLE IF NOT EXISTS rebuild_artifacts (
           artifact_id TEXT PRIMARY KEY,
           kind TEXT NOT NULL,
           blob_hash TEXT NOT NULL REFERENCES rebuild_blobs(blob_hash),
           media_type TEXT NOT NULL,
           bytes INTEGER NOT NULL,
           producer TEXT NOT NULL,
           lifecycle TEXT NOT NULL,
           provenance_json TEXT NOT NULL,
           origin_json TEXT,
           created_at TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS rebuild_artifact_refs (
           artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           source_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           source_kind TEXT NOT NULL,
           PRIMARY KEY (artifact_id, source_artifact_id)
         );
         CREATE TABLE IF NOT EXISTS rebuild_embedded_blob_refs (
           artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           role TEXT NOT NULL,
           ordinal INTEGER NOT NULL,
           blob_hash TEXT NOT NULL REFERENCES rebuild_blobs(blob_hash),
           PRIMARY KEY (artifact_id, role, ordinal)
         );
CREATE TABLE IF NOT EXISTS rebuild_runs (
    run_id TEXT PRIMARY KEY,
    purpose TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    graph_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS rebuild_run_cancellations (
    run_id TEXT PRIMARY KEY REFERENCES rebuild_runs(run_id),
    reason TEXT NOT NULL,
    requested_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_workflow_revisions (
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           revision INTEGER NOT NULL,
           graph_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
           created_at TEXT NOT NULL,
           PRIMARY KEY (run_id, revision)
         );
 CREATE TABLE IF NOT EXISTS rebuild_tasks (
           task_id TEXT PRIMARY KEY,
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           recipe_id TEXT NOT NULL,
           objective TEXT NOT NULL,
           contract_hash TEXT,
           priority INTEGER NOT NULL,
           budget_json TEXT NOT NULL,
           retry_json TEXT NOT NULL,
 on_failure TEXT NOT NULL,
 parent_task_id TEXT,
 input_artifacts_json TEXT NOT NULL,
 status TEXT NOT NULL,
           ready_at TEXT NOT NULL,
           lease_id TEXT,
           lease_epoch INTEGER NOT NULL DEFAULT 0,
           active_attempt_id TEXT,
           lease_until TEXT,
           worker_id TEXT,
           finished_at TEXT
         );
         CREATE TABLE IF NOT EXISTS rebuild_task_dependencies (
           task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           depends_on_task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           PRIMARY KEY (task_id, depends_on_task_id)
         );
         CREATE TABLE IF NOT EXISTS rebuild_attempts (
           attempt_id TEXT PRIMARY KEY,
           task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
           run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
           lease_id TEXT NOT NULL,
           epoch INTEGER NOT NULL,
           worker_id TEXT NOT NULL,
           status TEXT NOT NULL,
           started_at TEXT NOT NULL,
           finished_at TEXT
         );
CREATE TABLE IF NOT EXISTS rebuild_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    task_id TEXT REFERENCES rebuild_tasks(task_id),
    attempt_id TEXT REFERENCES rebuild_attempts(attempt_id),
    event_type TEXT NOT NULL,
    artifact_id TEXT REFERENCES rebuild_artifacts(artifact_id),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_attempt_outputs (
    attempt_id TEXT NOT NULL REFERENCES rebuild_attempts(attempt_id),
    task_id TEXT NOT NULL REFERENCES rebuild_tasks(task_id),
    artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    event_id INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    PRIMARY KEY (attempt_id, artifact_id)
);
CREATE TABLE IF NOT EXISTS rebuild_daemon_leases (
  lease_name TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  expires_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_session_slots (
    session_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
  topology_id TEXT NOT NULL,
  graph_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
  run_created_at TEXT NOT NULL,
  scheduler_epoch INTEGER NOT NULL,
  reserved_at TEXT NOT NULL,
    commitment_artifact_id TEXT REFERENCES rebuild_artifacts(artifact_id),
    committed_at TEXT
);
CREATE TABLE IF NOT EXISTS rebuild_paper_approval_consumptions (
    approval_artifact_id TEXT PRIMARY KEY REFERENCES rebuild_artifacts(artifact_id),
    runtime_manifest_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    session_key TEXT NOT NULL UNIQUE REFERENCES rebuild_session_slots(session_key),
    consumed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_execution_reprices (
    commitment_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    asset TEXT NOT NULL,
    reprice_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (commitment_artifact_id, asset),
    UNIQUE (reprice_artifact_id)
);
CREATE TABLE IF NOT EXISTS rebuild_policy_transitions (
    transition_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    subject_json TEXT NOT NULL,
    from_state_json TEXT NOT NULL,
    to_state_json TEXT NOT NULL,
    evaluation_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    revision INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    UNIQUE(subject_id, revision)
);
CREATE TABLE IF NOT EXISTS rebuild_contract_installations (
    contract_hash TEXT PRIMARY KEY,
    contract_artifact_id TEXT NOT NULL UNIQUE REFERENCES rebuild_artifacts(artifact_id),
    contract_id TEXT NOT NULL,
    contract_version INTEGER NOT NULL,
    purpose TEXT NOT NULL,
    baseline_contract_hash TEXT REFERENCES rebuild_contract_installations(contract_hash),
    installed_at TEXT NOT NULL,
    UNIQUE(contract_id, contract_version)
);
CREATE TABLE IF NOT EXISTS rebuild_contract_activations (
    activation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    purpose TEXT NOT NULL,
    previous_contract_hash TEXT REFERENCES rebuild_contract_installations(contract_hash),
    contract_hash TEXT NOT NULL REFERENCES rebuild_contract_installations(contract_hash),
    policy_transition_id TEXT UNIQUE REFERENCES rebuild_policy_transitions(transition_id),
    activated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_contract_catalogue_heads (
    purpose TEXT PRIMARY KEY,
    contract_hash TEXT NOT NULL REFERENCES rebuild_contract_installations(contract_hash),
    activation_id INTEGER NOT NULL UNIQUE REFERENCES rebuild_contract_activations(activation_id)
);
CREATE TABLE IF NOT EXISTS rebuild_policy_evaluations (
    evaluation_artifact_id TEXT PRIMARY KEY REFERENCES rebuild_artifacts(artifact_id),
    subject_id TEXT NOT NULL,
    subject_json TEXT NOT NULL,
    outcome_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    experience_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    candidate_policy_artifact_id TEXT UNIQUE REFERENCES rebuild_artifacts(artifact_id),
    from_state_json TEXT NOT NULL,
    to_state_json TEXT NOT NULL,
    transition_id TEXT UNIQUE REFERENCES rebuild_policy_transitions(transition_id),
    run_id TEXT NOT NULL REFERENCES rebuild_runs(run_id),
    consumed_pair_cursor INTEGER NOT NULL,
    event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    completed_at TEXT NOT NULL,
    UNIQUE(subject_id, event_cursor)
);
CREATE TABLE IF NOT EXISTS rebuild_policy_consumption_heads (
    subject_id TEXT PRIMARY KEY,
    subject_json TEXT NOT NULL,
    consumed_pair_cursor INTEGER NOT NULL,
    evaluation_artifact_id TEXT NOT NULL REFERENCES rebuild_policy_evaluations(evaluation_artifact_id),
    evaluation_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_policy_heads (
    subject_id TEXT PRIMARY KEY,
    subject_json TEXT NOT NULL,
    state_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    transition_id TEXT NOT NULL REFERENCES rebuild_policy_transitions(transition_id),
    transition_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rebuild_shadow_pairs (
    pair_key TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    subject_json TEXT NOT NULL,
    parent_decision_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    execution_context_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    candidate_decision_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    candidate_contract_hash TEXT NOT NULL,
    candidate_topology_id TEXT NOT NULL,
    horizon TEXT NOT NULL,
    parent_outcome_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    candidate_outcome_artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
    completed_at TEXT NOT NULL,
    pair_event_cursor INTEGER NOT NULL UNIQUE REFERENCES rebuild_events(event_id)
);
CREATE TABLE IF NOT EXISTS rebuild_observatory_configuration (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    configuration_json BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS rebuild_tasks_claimable
    ON rebuild_tasks(status, ready_at, priority);
CREATE INDEX IF NOT EXISTS rebuild_events_cursor
    ON rebuild_events(run_id, event_id);
CREATE INDEX IF NOT EXISTS rebuild_attempt_outputs_cursor
    ON rebuild_attempt_outputs(attempt_id, event_id);
CREATE INDEX IF NOT EXISTS rebuild_policy_transitions_subject
    ON rebuild_policy_transitions(subject_id, revision);
CREATE INDEX IF NOT EXISTS rebuild_policy_evaluations_subject
    ON rebuild_policy_evaluations(subject_id, event_cursor);
CREATE INDEX IF NOT EXISTS rebuild_shadow_pairs_freshness
    ON rebuild_shadow_pairs(subject_id, horizon, pair_event_cursor);
COMMIT;",
    )?;
    if !table_has_column(
        connection,
        "rebuild_policy_evaluations",
        "candidate_policy_artifact_id",
    )? {
        return Err(StoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }
    if version.is_none() {
        connection.execute(
            "INSERT INTO rebuild_metadata (key, value) VALUES ('schema_version', ?1)",
            params![STORE_SCHEMA_VERSION.to_string()],
        )?;
    }
    Ok(())
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    required_column: &str,
) -> StoreResult<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|column| column == required_column))
}

fn contract_catalogue_head(
    connection: &Connection,
    purpose: &ContractPurpose,
) -> StoreResult<Option<(ContentHash, i64)>> {
    let row = connection
        .query_row(
            "SELECT contract_hash, activation_id FROM rebuild_contract_catalogue_heads WHERE purpose = ?1",
            params![purpose.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(hash, activation_id)| Ok((ContentHash::new(hash)?, activation_id)))
        .transpose()
}

fn assert_contract_identity_available(
    connection: &Connection,
    contract: &AgentContract,
) -> StoreResult<()> {
    let existing = connection
        .query_row(
            "SELECT contract_hash FROM rebuild_contract_installations WHERE contract_id = ?1 AND contract_version = ?2",
            params![contract.contract_id.0.as_str(), i64::from(contract.version)],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if existing.is_some() {
        return Err(StoreError::DuplicateContractVersion {
            contract_id: contract.contract_id.clone(),
            version: contract.version,
        });
    }
    Ok(())
}

fn insert_contract_installation(
    transaction: &Transaction<'_>,
    contract: &AgentContract,
    artifact: &Artifact,
    baseline_contract_hash: Option<&ContentHash>,
    installed_at: DateTime<Utc>,
) -> StoreResult<()> {
    transaction.execute(
        r#"INSERT INTO rebuild_contract_installations
           (contract_hash, contract_artifact_id, contract_id, contract_version, purpose,
            baseline_contract_hash, installed_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
        params![
            contract.contract_hash.as_str(),
            artifact.artifact_id.0.as_str(),
            contract.contract_id.0.as_str(),
            i64::from(contract.version),
            contract.purpose.as_str(),
            baseline_contract_hash.map(ContentHash::as_str),
            installed_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

fn append_contract_activation(
    transaction: &Transaction<'_>,
    purpose: &ContractPurpose,
    previous_contract_hash: Option<&ContentHash>,
    contract_hash: &ContentHash,
    policy_transition_id: Option<&PolicyTransitionId>,
    activated_at: DateTime<Utc>,
) -> StoreResult<i64> {
    transaction.execute(
        r#"INSERT INTO rebuild_contract_activations
           (purpose, previous_contract_hash, contract_hash, policy_transition_id, activated_at)
           VALUES (?1, ?2, ?3, ?4, ?5)"#,
        params![
            purpose.as_str(),
            previous_contract_hash.map(ContentHash::as_str),
            contract_hash.as_str(),
            policy_transition_id.map(|id| id.0.as_str()),
            activated_at.to_rfc3339(),
        ],
    )?;
    Ok(transaction.last_insert_rowid())
}

fn set_contract_catalogue_head(
    transaction: &Transaction<'_>,
    purpose: &ContractPurpose,
    contract_hash: &ContentHash,
    activation_id: i64,
) -> StoreResult<()> {
    transaction.execute(
        r#"INSERT INTO rebuild_contract_catalogue_heads (purpose, contract_hash, activation_id)
           VALUES (?1, ?2, ?3)
           ON CONFLICT(purpose) DO UPDATE SET
             contract_hash = excluded.contract_hash,
             activation_id = excluded.activation_id"#,
        params![purpose.as_str(), contract_hash.as_str(), activation_id],
    )?;
    Ok(())
}

fn candidate_is_bounded(active: &AgentContract, candidate: &AgentContract) -> bool {
    active.permits_candidate(candidate)
        && active.purpose == candidate.purpose
        && active.output.artifact_kind == candidate.output.artifact_kind
        && (!active.termination.require_evidence || candidate.termination.require_evidence)
        && candidate.termination.max_child_tasks <= active.termination.max_child_tasks
        && candidate.termination.max_depth <= active.termination.max_depth
}

fn insert_artifact(transaction: &Transaction<'_>, artifact: &Artifact) -> StoreResult<()> {
    artifact.validate()?;
    blob::read_blob_bytes(transaction, &artifact.blob.hash, artifact.blob.bytes)?;
    for source in &artifact.source_refs {
        let exists = transaction
            .query_row(
                "SELECT kind FROM rebuild_artifacts WHERE artifact_id = ?1",
                params![source.artifact_id.0.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if exists.as_deref() != Some(&enum_name(source.kind)) {
            return Err(StoreError::InvalidArtifactClosure(
                artifact.artifact_id.clone(),
            ));
        }
    }
    let inserted = transaction.execute(
        r#"INSERT OR IGNORE INTO rebuild_artifacts
           (artifact_id, kind, blob_hash, media_type, bytes, producer, lifecycle, provenance_json, origin_json, created_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
        params![
            artifact.artifact_id.0.as_str(),
            enum_name(artifact.kind),
            artifact.blob.hash.as_str(),
            artifact.blob.media_type,
            artifact.blob.bytes,
            artifact.producer,
            enum_name(artifact.lifecycle),
            serde_json::to_string(&artifact.provenance)?,
            serde_json::to_string(&artifact.origin)?,
            artifact.created_at.to_rfc3339(),
        ],
    )?;
    if inserted == 0 {
        let existing = read_artifact(transaction, &artifact.artifact_id)?;
        if &existing != artifact {
            return Err(StoreError::Integrity(format!(
                "artifact hash collision {}",
                artifact.artifact_id.0
            )));
        }
        return Ok(());
    }
    for source in &artifact.source_refs {
        transaction.execute(
            r#"INSERT INTO rebuild_artifact_refs
               (artifact_id, source_artifact_id, source_kind)
               VALUES (?1, ?2, ?3)"#,
            params![
                artifact.artifact_id.0.as_str(),
                source.artifact_id.0.as_str(),
                enum_name(source.kind),
            ],
        )?;
    }
    index_embedded_blob_refs(transaction, artifact)?;
    Ok(())
}

fn index_embedded_blob_refs(connection: &Connection, artifact: &Artifact) -> StoreResult<()> {
    for (role, ordinal, blob) in embedded_blob_refs(connection, artifact)? {
        connection.execute(
            r#"INSERT INTO rebuild_embedded_blob_refs
               (artifact_id, role, ordinal, blob_hash)
               VALUES (?1, ?2, ?3, ?4)"#,
            params![
                artifact.artifact_id.0.as_str(),
                role,
                ordinal,
                blob.hash.as_str(),
            ],
        )?;
    }
    Ok(())
}

fn embedded_blob_refs(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<Vec<(String, u64, BlobRef)>> {
    if artifact.kind != ArtifactKind::Contract {
        return Ok(Vec::new());
    }
    let payload = blob::read_blob_bytes(connection, &artifact.blob.hash, artifact.blob.bytes)?;
    let contract: AgentContract = serde_json::from_slice(&payload)?;
    contract.validate()?;
    let mut refs = vec![
        (
            "prompt.governance".to_owned(),
            0,
            contract.prompt.governance,
        ),
        ("prompt.role".to_owned(), 0, contract.prompt.role),
        ("output.schema".to_owned(), 0, contract.output.schema),
    ];
    refs.extend(
        contract
            .tool_specs
            .into_iter()
            .enumerate()
            .map(|(ordinal, tool)| {
                (
                    "tool.input_schema".to_owned(),
                    ordinal as u64,
                    tool.input_schema,
                )
            }),
    );
    for (_, _, blob) in &refs {
        blob::read_blob_bytes(connection, &blob.hash, blob.bytes)?;
    }
    Ok(refs)
}

/// Inserts a completion batch in source-closure order. A task may create a
/// RawEvidence artifact and its NormalizedEvidence dependent in the same
/// atomic attempt; callers need not rely on input ordering for correctness.
fn insert_artifact_batch(transaction: &Transaction<'_>, artifacts: &[Artifact]) -> StoreResult<()> {
    let mut pending = BTreeMap::<ArtifactId, &Artifact>::new();
    for artifact in artifacts {
        artifact.validate()?;
        if let Some(existing) = pending.insert(artifact.artifact_id.clone(), artifact) {
            if existing != artifact {
                return Err(StoreError::Integrity(format!(
                    "conflicting completion artifacts for {}",
                    artifact.artifact_id
                )));
            }
        }
    }

    while !pending.is_empty() {
        let ready = pending
            .iter()
            .find(|(_, artifact)| {
                artifact
                    .source_refs
                    .iter()
                    .all(|reference| !pending.contains_key(&reference.artifact_id))
            })
            .map(|(artifact_id, _)| artifact_id.clone());
        let Some(artifact_id) = ready else {
            return Err(StoreError::InvalidArtifactClosure(
                pending
                    .first_key_value()
                    .expect("pending batch is non-empty")
                    .0
                    .clone(),
            ));
        };
        let artifact = pending
            .remove(&artifact_id)
            .expect("ready artifact is still pending");
        insert_artifact(transaction, artifact)?;
    }
    Ok(())
}

fn assert_workflow_input_artifacts(
    transaction: &Transaction<'_>,
    nodes: &[WorkflowNode],
) -> StoreResult<()> {
    let mut visited = BTreeSet::new();
    for reference in nodes.iter().flat_map(|node| &node.input_artifacts) {
        assert_artifact_reference_closure(transaction, reference, &mut visited)?;
    }
    Ok(())
}

fn assert_artifact_reference_closure(
    transaction: &Transaction<'_>,
    reference: &ArtifactRef,
    visited: &mut BTreeSet<ArtifactId>,
) -> StoreResult<()> {
    let artifact = read_artifact(transaction, &reference.artifact_id)?;
    if artifact.kind != reference.kind {
        return Err(StoreError::InvalidArtifactClosure(
            reference.artifact_id.clone(),
        ));
    }
    if !visited.insert(reference.artifact_id.clone()) {
        return Ok(());
    }
    for source in &artifact.source_refs {
        assert_artifact_reference_closure(transaction, source, visited)?;
    }
    Ok(())
}

fn insert_task_node(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    node: &WorkflowNode,
    created_at: DateTime<Utc>,
) -> StoreResult<()> {
    let inserted = transaction.execute(
        r#"INSERT INTO rebuild_tasks
 (task_id, run_id, recipe_id, objective, contract_hash, priority, budget_json, retry_json, on_failure,
 parent_task_id, input_artifacts_json, status, ready_at)
 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'queued', ?12)"#,
        params![
            node.task_id.0,
            run_id.0,
            node.recipe_id.as_str(),
            node.objective,
            node.contract_hash.as_ref().map(ContentHash::as_str),
            node.priority,
            serde_json::to_string(&node.budget)?,
            serde_json::to_string(&node.retry)?,
            enum_name(node.on_failure),
            node.parent_task_id.as_ref().map(|id| id.0.as_str()),
            serde_json::to_string(&node.input_artifacts)?,
            created_at.to_rfc3339(),
        ],
    )?;
    if inserted != 1 {
        return Err(StoreError::DuplicateTask(node.task_id.clone()));
    }
    Ok(())
}

fn insert_node_dependencies(transaction: &Transaction<'_>, node: &WorkflowNode) -> StoreResult<()> {
    for dependency in &node.dependencies {
        transaction.execute(
            "INSERT INTO rebuild_task_dependencies (task_id, depends_on_task_id) VALUES (?1, ?2)",
            params![node.task_id.0, dependency.0],
        )?;
    }
    Ok(())
}

fn task_dependencies(connection: &Connection, task_id: &TaskId) -> StoreResult<Vec<TaskId>> {
    let dependencies = connection
        .prepare(
            "SELECT depends_on_task_id FROM rebuild_task_dependencies \
             WHERE task_id = ?1 ORDER BY depends_on_task_id ASC",
        )?
        .query_map(params![task_id.0], |row| Ok(TaskId(row.get(0)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(dependencies)
}

fn canonical_workflow_node(mut node: WorkflowNode) -> WorkflowNode {
    node.dependencies.sort();
    node
}

fn assert_permit(transaction: &Transaction<'_>, permit: &TaskWritePermit) -> StoreResult<()> {
    let current = transaction
        .query_row(
            r#"SELECT run_id, status, lease_id, lease_epoch, active_attempt_id, contract_hash
               FROM rebuild_tasks WHERE task_id = ?1"#,
            params![permit.task_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((run_id, status, lease_id, epoch, attempt_id, contract_hash)) = current else {
        return Err(StoreError::MissingTask(permit.task_id.clone()));
    };
    if run_id != permit.run_id.0
        || status != "running"
        || lease_id.as_deref() != Some(permit.lease_id.0.as_str())
        || epoch != permit.epoch
        || attempt_id.as_deref() != Some(permit.attempt_id.0.as_str())
        || contract_hash.as_deref().map(ContentHash::new).transpose()? != permit.contract_hash
    {
        return Err(StoreError::StalePermit(permit.task_id.clone()));
    }
    Ok(())
}

fn assert_daemon_lease(
    transaction: &Transaction<'_>,
    lease: &DaemonLease,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let current = transaction
        .query_row(
            "SELECT owner_id, epoch, expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1",
            params![lease.lease_name],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((owner_id, epoch, expires_at)) = current else {
        return Err(StoreError::SchedulerFenced(lease.lease_name.clone()));
    };
    if owner_id != lease.owner_id || epoch != lease.epoch || parse_time(&expires_at)? <= now {
        return Err(StoreError::SchedulerFenced(lease.lease_name.clone()));
    }
    Ok(())
}

fn assert_paper_effect_artifact(
    transaction: &Transaction<'_>,
    effect: &ArtifactRef,
    run_id: &RunId,
) -> StoreResult<()> {
    let artifact = read_artifact(transaction, &effect.artifact_id)?;
    if effect.kind != artifact.kind
        || !matches!(
            artifact.kind,
            ArtifactKind::ExecutionCommitment | ArtifactKind::ExecutionReprice
        )
        || artifact.lifecycle != ArtifactLifecycle::Canonical
        || artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(run_id)
    {
        return Err(StoreError::InvalidPaperEffect(effect.artifact_id.clone()));
    }
    Ok(())
}

fn paper_effect_intent_exists(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    effect_id: &ArtifactId,
) -> StoreResult<bool> {
    let found = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE run_id = ?1 AND event_type = ?2 AND artifact_id = ?3)",
        params![
            run_id.0,
            LifecycleEventType::ExecutionEffectIntent.as_str(),
            effect_id.0.as_str(),
        ],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(found != 0)
}

fn validate_paper_effect_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
             AND artifact_id IS NOT NULL
             AND event_type IN (?2, ?3, ?4)
           ORDER BY event_id ASC"#,
    )?;
    let rows =
        statement.query_map(
            params![
                run_id.map(|value| value.0.as_str()),
                LifecycleEventType::ExecutionEffectIntent.as_str(),
                LifecycleEventType::ExecutionEffectSettled.as_str(),
                LifecycleEventType::ExecutionEffectRecovered.as_str(),
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    RunId(row.get::<_, String>(1)?),
                    row.get::<_, String>(2)?,
                    ArtifactId(ContentHash::new(row.get::<_, String>(3)?).map_err(|error| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                    })?),
                ))
            },
        )?;
    let mut intents = BTreeMap::<(RunId, ArtifactId), i64>::new();
    let mut terminals = BTreeMap::<(RunId, ArtifactId), i64>::new();
    for row in rows {
        let (cursor, event_run_id, event_type, effect_id) = row?;
        let key = (event_run_id, effect_id.clone());
        match event_type.as_str() {
            value if value == LifecycleEventType::ExecutionEffectIntent.as_str() => {
                if terminals.contains_key(&key) {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} has intent after terminal event at cursor {cursor}"
                    )));
                }
                if intents.insert(key, cursor).is_some() {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} has duplicate intent at cursor {cursor}"
                    )));
                }
            }
            value
                if value == LifecycleEventType::ExecutionEffectSettled.as_str()
                    || value == LifecycleEventType::ExecutionEffectRecovered.as_str() =>
            {
                let Some(intent_cursor) = intents.get(&key).copied() else {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} terminal event at cursor {cursor} has no prior intent"
                    )));
                };
                if cursor <= intent_cursor {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} terminal cursor {cursor} is not after intent cursor {intent_cursor}"
                    )));
                }
                if terminals.insert(key, cursor).is_some() {
                    return Err(StoreError::Integrity(format!(
                        "Paper effect {effect_id} has duplicate terminal event at cursor {cursor}"
                    )));
                }
            }
            _ => unreachable!("effect query emits fixed lifecycle types"),
        }
    }
    Ok(())
}

struct LifecycleRow {
    cursor: i64,
    run_id: RunId,
    task_id: Option<TaskId>,
    attempt_id: Option<akzio_domain::AttemptId>,
    event_type: LifecycleEventType,
    artifact_id: Option<ArtifactId>,
}

fn decode_lifecycle_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LifecycleRow> {
    Ok(LifecycleRow {
        cursor: row.get(0)?,
        run_id: RunId(row.get(1)?),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId),
        attempt_id: row
            .get::<_, Option<String>>(3)?
            .map(akzio_domain::AttemptId),
        event_type: LifecycleEventType::parse(&row.get::<_, String>(4)?)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
        artifact_id: row
            .get::<_, Option<String>>(5)?
            .map(|value| {
                ContentHash::new(value)
                    .map(ArtifactId)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })
            .transpose()?,
    })
}

fn validate_tool_lifecycle_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
                 AND event_type IN (?2, ?3, ?4, ?5)
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::ToolCalled.as_str(),
            LifecycleEventType::ToolCompleted.as_str(),
            LifecycleEventType::ToolFailed.as_str(),
            LifecycleEventType::TaskSucceeded.as_str(),
        ],
        decode_lifecycle_row,
    )?;

    #[derive(Clone)]
    struct CalledEvent {
        cursor: i64,
        run_id: RunId,
        task_id: TaskId,
        attempt_id: akzio_domain::AttemptId,
    }

    let mut called_by_key =
        BTreeMap::<(RunId, TaskId, akzio_domain::AttemptId, ArtifactId), CalledEvent>::new();
    let mut terminal_by_call = BTreeSet::new();
    let mut pending_by_task =
        BTreeMap::<(RunId, TaskId, akzio_domain::AttemptId), BTreeSet<ArtifactId>>::new();
    let mut succeeded_tasks = BTreeSet::<(RunId, TaskId, akzio_domain::AttemptId)>::new();

    for row in rows {
        let LifecycleRow {
            cursor,
            run_id: event_run_id,
            task_id,
            attempt_id,
            event_type,
            artifact_id,
        } = row?;
        let Some(task_id) = task_id else {
            return Err(StoreError::Integrity(format!(
                "tool event at cursor {cursor} has no task"
            )));
        };
        let Some(attempt_id) = attempt_id else {
            return Err(StoreError::Integrity(format!(
                "tool event at cursor {cursor} has no attempt"
            )));
        };
        let task_key = (event_run_id.clone(), task_id.clone(), attempt_id.clone());
        match event_type {
            LifecycleEventType::TaskSucceeded => {
                if pending_by_task
                    .get(&task_key)
                    .is_some_and(|pending| !pending.is_empty())
                {
                    return Err(StoreError::Integrity(format!(
                        "task.succeeded cursor {cursor} has pending tool calls"
                    )));
                }
                if !succeeded_tasks.insert(task_key) {
                    return Err(StoreError::Integrity(format!(
                        "task.succeeded cursor {cursor} repeats task terminal"
                    )));
                }
            }
            LifecycleEventType::ToolCalled => {
                let Some(artifact_id) = artifact_id else {
                    return Err(StoreError::Integrity(format!(
                        "tool event at cursor {cursor} has no artifact"
                    )));
                };
                if succeeded_tasks.contains(&task_key) {
                    return Err(StoreError::Integrity(format!(
                        "tool.called cursor {cursor} occurs after task.succeeded"
                    )));
                }
                let event_key = (
                    event_run_id.clone(),
                    task_id.clone(),
                    attempt_id.clone(),
                    artifact_id.clone(),
                );
                let artifact = read_artifact(connection, &artifact_id)?;
                if artifact.kind != ArtifactKind::ToolCall {
                    return Err(StoreError::Integrity(format!(
                        "tool.called cursor {cursor} references {:?}, expected tool_call",
                        artifact.kind
                    )));
                }
                if called_by_key
                    .insert(
                        event_key,
                        CalledEvent {
                            cursor,
                            run_id: event_run_id,
                            task_id,
                            attempt_id,
                        },
                    )
                    .is_some()
                {
                    return Err(StoreError::Integrity(format!(
                        "duplicate tool.called event for {} at cursor {cursor}",
                        artifact_id.0
                    )));
                }
                pending_by_task
                    .entry(task_key.clone())
                    .or_default()
                    .insert(artifact_id.clone());
            }
            LifecycleEventType::ToolCompleted | LifecycleEventType::ToolFailed => {
                let Some(artifact_id) = artifact_id else {
                    return Err(StoreError::Integrity(format!(
                        "tool event at cursor {cursor} has no artifact"
                    )));
                };
                if succeeded_tasks.contains(&task_key) {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} occurs after task.succeeded",
                        event_type.as_str()
                    )));
                }
                let artifact = read_artifact(connection, &artifact_id)?;
                if artifact.kind != ArtifactKind::ToolResult {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} references {:?}, expected tool_result",
                        event_type.as_str(),
                        artifact.kind
                    )));
                }
                let tool_call_refs = artifact
                    .source_refs
                    .iter()
                    .filter(|reference| reference.kind == ArtifactKind::ToolCall)
                    .collect::<Vec<_>>();
                if tool_call_refs.len() != 1 {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} must reference exactly one tool_call",
                        event_type.as_str()
                    )));
                }
                let call_artifact_id = tool_call_refs[0].artifact_id.clone();
                let call_key = (
                    event_run_id.clone(),
                    task_id.clone(),
                    attempt_id.clone(),
                    call_artifact_id.clone(),
                );
                let Some(called) = called_by_key.get(&call_key) else {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} has no prior tool.called for {}",
                        event_type.as_str(),
                        call_artifact_id.0
                    )));
                };
                if called.cursor >= cursor
                    || called.run_id != event_run_id
                    || called.task_id != task_id
                    || called.attempt_id != attempt_id
                {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} does not match its tool.called lineage",
                        event_type.as_str()
                    )));
                }
                let terminal_key = (event_run_id, task_id, attempt_id, call_artifact_id.clone());
                if !terminal_by_call.insert(terminal_key) {
                    return Err(StoreError::Integrity(format!(
                        "tool call already has a terminal event at cursor {cursor}"
                    )));
                }
                let Some(pending) = pending_by_task.get_mut(&task_key) else {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} has no pending tool call",
                        event_type.as_str()
                    )));
                };
                if !pending.remove(&call_artifact_id) {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} repeats or misses pending tool call",
                        event_type.as_str()
                    )));
                }
                if pending.is_empty() {
                    pending_by_task.remove(&task_key);
                }
            }
            _ => unreachable!("tool lifecycle query emits fixed event types"),
        }
    }
    Ok(())
}

fn validate_agent_turn_lifecycle_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
               AND event_type IN (?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::AgentTurnStarted.as_str(),
            LifecycleEventType::AgentTurnCompleted.as_str(),
            LifecycleEventType::AgentTurnRetryableFailed.as_str(),
            LifecycleEventType::AgentTurnFailed.as_str(),
            LifecycleEventType::TaskDeferred.as_str(),
            LifecycleEventType::TaskRetryScheduled.as_str(),
            LifecycleEventType::TaskRetryExhausted.as_str(),
            LifecycleEventType::TaskRecovered.as_str(),
            LifecycleEventType::TaskRecoveryExhausted.as_str(),
            LifecycleEventType::TaskCancelled.as_str(),
            LifecycleEventType::TaskSucceeded.as_str(),
            LifecycleEventType::TaskFailed.as_str(),
            LifecycleEventType::TaskSkipped.as_str(),
        ],
        decode_lifecycle_row,
    )?;

    #[derive(Default)]
    struct TurnState {
        pending_start: bool,
        saw_started: bool,
        terminal_artifacts: BTreeSet<ArtifactId>,
        last_terminal: Option<LifecycleEventType>,
    }

    let mut states = BTreeMap::<(RunId, TaskId, akzio_domain::AttemptId), TurnState>::new();
    for row in rows {
        let LifecycleRow {
            cursor,
            run_id: event_run_id,
            task_id,
            attempt_id,
            event_type,
            artifact_id,
        } = row?;
        let key = match (&task_id, &attempt_id) {
            (Some(task_id), Some(attempt_id)) => {
                (event_run_id.clone(), task_id.clone(), attempt_id.clone())
            }
            _ => {
                if matches!(
                    event_type,
                    LifecycleEventType::AgentTurnStarted
                        | LifecycleEventType::AgentTurnCompleted
                        | LifecycleEventType::AgentTurnRetryableFailed
                        | LifecycleEventType::AgentTurnFailed
                ) {
                    return Err(StoreError::Integrity(format!(
                        "agent lifecycle event at cursor {cursor} has incomplete task attempt lineage"
                    )));
                }
                continue;
            }
        };
        let state = states.entry(key.clone()).or_default();
        match event_type {
            LifecycleEventType::AgentTurnStarted => {
                if artifact_id.is_some() {
                    return Err(StoreError::Integrity(format!(
                        "agent.turn_started cursor {cursor} unexpectedly has an artifact"
                    )));
                }
                if state.pending_start {
                    return Err(StoreError::Integrity(format!(
                        "agent.turn_started cursor {cursor} follows an unresolved model turn"
                    )));
                }
                state.pending_start = true;
                state.saw_started = true;
            }
            LifecycleEventType::AgentTurnCompleted
            | LifecycleEventType::AgentTurnRetryableFailed
            | LifecycleEventType::AgentTurnFailed => {
                let Some(artifact_id) = artifact_id else {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} has no AgentTurn artifact",
                        event_type.as_str()
                    )));
                };
                let artifact = read_artifact(connection, &artifact_id)?;
                if artifact.kind != ArtifactKind::AgentTurn {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} references {:?}, expected agent_turn",
                        event_type.as_str(),
                        artifact.kind
                    )));
                }
                let origin = artifact.origin.as_ref().ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "{} cursor {cursor} AgentTurn artifact has no origin",
                        event_type.as_str()
                    ))
                })?;
                if origin.run_id.as_ref() != Some(&key.0)
                    || origin.task_id.as_ref() != Some(&key.1)
                    || origin.attempt_id.as_ref() != Some(&key.2)
                {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} AgentTurn artifact origin does not match task attempt",
                        event_type.as_str()
                    )));
                }
                if !state.terminal_artifacts.insert(artifact_id) {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} repeats an AgentTurn terminal artifact",
                        event_type.as_str()
                    )));
                }
                // Legacy/audit terminal artifacts without a started event remain
                // readable for existing v2 stores.  The current no-model retry
                // path is a capability preflight failure after a retryable
                // terminal; retain that one compatibility exception without
                // coupling the store to research artifact payloads.
                let capability_preflight_retry = state.last_terminal
                    == Some(LifecycleEventType::AgentTurnRetryableFailed)
                    && event_type == LifecycleEventType::AgentTurnFailed;
                if !state.pending_start && state.saw_started && !capability_preflight_retry {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} has no pending AgentTurn start",
                        event_type.as_str()
                    )));
                }
                state.pending_start = false;
                state.last_terminal = Some(event_type);
            }
            LifecycleEventType::TaskDeferred
            | LifecycleEventType::TaskRetryScheduled
            | LifecycleEventType::TaskRetryExhausted
            | LifecycleEventType::TaskRecovered
            | LifecycleEventType::TaskRecoveryExhausted
            | LifecycleEventType::TaskCancelled => {
                // These events abandon the in-flight attempt during retry or
                // recovery; they are the durable close for a crashed turn.
                state.pending_start = false;
                state.last_terminal = None;
            }
            LifecycleEventType::TaskSucceeded
            | LifecycleEventType::TaskFailed
            | LifecycleEventType::TaskSkipped => {
                if state.pending_start {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} closes a task with a pending AgentTurn",
                        event_type.as_str()
                    )));
                }
            }
            _ => unreachable!("agent lifecycle query emits fixed event types"),
        }
    }
    Ok(())
}

fn validate_context_lifecycle_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
                 AND event_type IN (?2, ?3, ?4)
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::ContextManifestCreated.as_str(),
            LifecycleEventType::ContextChildManifestCreated.as_str(),
            LifecycleEventType::ContextRepaired.as_str(),
        ],
        decode_lifecycle_row,
    )?;
    let mut seen = BTreeSet::<ArtifactId>::new();

    for row in rows {
        let LifecycleRow {
            cursor,
            run_id: event_run_id,
            task_id,
            attempt_id,
            event_type,
            artifact_id,
        } = row?;
        let task_id = task_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no task lineage",
                event_type.as_str()
            ))
        })?;
        let attempt_id = attempt_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no attempt lineage",
                event_type.as_str()
            ))
        })?;
        let artifact_id = artifact_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no artifact",
                event_type.as_str()
            ))
        })?;
        if !seen.insert(artifact_id.clone()) {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} repeats artifact {}",
                event_type.as_str(),
                artifact_id.0
            )));
        }
        let artifact = read_artifact(connection, &artifact_id)?;
        let expected_kind = if event_type == LifecycleEventType::ContextRepaired {
            ArtifactKind::ContextRepair
        } else {
            ArtifactKind::ContextManifest
        };
        if artifact.kind != expected_kind {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} references {:?}, expected {:?}",
                event_type.as_str(),
                artifact.kind,
                expected_kind
            )));
        }
        let origin = artifact.origin.as_ref().ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} artifact has no origin",
                event_type.as_str()
            ))
        })?;
        if origin.run_id.as_ref() != Some(&event_run_id)
            || origin.task_id.as_ref() != Some(&task_id)
            || origin.attempt_id.as_ref() != Some(&attempt_id)
        {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} artifact origin does not match task attempt",
                event_type.as_str()
            )));
        }
        if event_type == LifecycleEventType::ContextChildManifestCreated {
            let parents = artifact
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
                .collect::<Vec<_>>();
            if parents.len() != 1 {
                return Err(StoreError::Integrity(format!(
                    "{} cursor {cursor} must reference exactly one parent context manifest",
                    event_type.as_str()
                )));
            }
            let parent = read_artifact(connection, &parents[0].artifact_id)?;
            let parent_origin = parent.origin.as_ref().ok_or_else(|| {
                StoreError::Integrity(format!(
                    "{} cursor {cursor} parent context manifest has no origin",
                    event_type.as_str()
                ))
            })?;
            if parent.kind != ArtifactKind::ContextManifest
                || parent_origin.run_id.as_ref() != Some(&event_run_id)
            {
                return Err(StoreError::Integrity(format!(
                    "{} cursor {cursor} parent context manifest is from another run",
                    event_type.as_str()
                )));
            }
        }
        if event_type == LifecycleEventType::ContextRepaired && artifact.source_refs.is_empty() {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} repair has no source refs",
                event_type.as_str()
            )));
        }
    }
    Ok(())
}

fn validate_gate_lifecycle_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
             AND event_type IN (
                    ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10
             )
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::ExecutionAllocationCreated.as_str(),
            LifecycleEventType::ExecutionCommitted.as_str(),
            LifecycleEventType::ExecutionCommitmentRecovered.as_str(),
            LifecycleEventType::ExecutionContextCreated.as_str(),
            LifecycleEventType::ExecutionPlanCreated.as_str(),
            LifecycleEventType::ExecutionRepriceCommitted.as_str(),
            LifecycleEventType::ExecutionRepriceRecovered.as_str(),
            LifecycleEventType::ExecutionVerdictCreated.as_str(),
            LifecycleEventType::ExecutionVerdictNoOrder.as_str(),
        ],
        decode_lifecycle_row,
    )?;

    for row in rows {
        let LifecycleRow {
            cursor,
            run_id: event_run_id,
            task_id,
            attempt_id,
            event_type,
            artifact_id,
        } = row?;
        let task_id = task_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no task lineage",
                event_type.as_str()
            ))
        })?;
        let attempt_id = attempt_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no attempt lineage",
                event_type.as_str()
            ))
        })?;
        let artifact_id = artifact_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no artifact",
                event_type.as_str()
            ))
        })?;
        let expected_kind = match event_type {
            LifecycleEventType::ExecutionAllocationCreated
            | LifecycleEventType::ExecutionPlanCreated => ArtifactKind::ExecutionPlan,
            LifecycleEventType::ExecutionContextCreated => ArtifactKind::ExecutionContext,
            LifecycleEventType::ExecutionVerdictCreated
            | LifecycleEventType::ExecutionVerdictNoOrder => ArtifactKind::ExecutionVerdict,
            LifecycleEventType::ExecutionCommitted
            | LifecycleEventType::ExecutionCommitmentRecovered => ArtifactKind::ExecutionCommitment,
            LifecycleEventType::ExecutionRepriceCommitted
            | LifecycleEventType::ExecutionRepriceRecovered => ArtifactKind::ExecutionReprice,
            _ => unreachable!("gate lifecycle query emits fixed event types"),
        };
        let artifact = read_artifact(connection, &artifact_id)?;
        artifact.validate()?;
        if artifact.kind != expected_kind {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} references {:?}, expected {:?}",
                event_type.as_str(),
                artifact.kind,
                expected_kind
            )));
        }
        let origin = artifact.origin.as_ref().ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} artifact has no origin",
                event_type.as_str()
            ))
        })?;
        let recovered = matches!(
            event_type,
            LifecycleEventType::ExecutionCommitmentRecovered
                | LifecycleEventType::ExecutionRepriceRecovered
        );
        if origin.run_id.as_ref() != Some(&event_run_id)
            || origin.task_id.as_ref() != Some(&task_id)
            || (!recovered && origin.attempt_id.as_ref() != Some(&attempt_id))
        {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} artifact origin does not match event lineage",
                event_type.as_str()
            )));
        }
        for source in &artifact.source_refs {
            let source_artifact = read_artifact(connection, &source.artifact_id)?;
            if source_artifact.kind != source.kind {
                return Err(StoreError::Integrity(format!(
                    "{} cursor {cursor} source {} kind {:?} disagrees with ref {:?}",
                    event_type.as_str(),
                    source.artifact_id.0,
                    source_artifact.kind,
                    source.kind
                )));
            }
        }
    }
    Ok(())
}

fn ensure_no_pending_tool_calls(
    connection: &Connection,
    run_id: &RunId,
    task_id: &TaskId,
    attempt_id: &akzio_domain::AttemptId,
) -> StoreResult<()> {
    let called = connection.query_row(
        r#"SELECT COUNT(*)
               FROM rebuild_events
               WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3
                 AND event_type = ?4"#,
        params![
            run_id.0,
            task_id.0,
            attempt_id.0,
            LifecycleEventType::ToolCalled.as_str(),
        ],
        |row| row.get::<_, u64>(0),
    )?;
    let terminal = connection.query_row(
        r#"SELECT COUNT(*)
               FROM rebuild_events AS terminal
               JOIN rebuild_artifact_refs AS reference
                 ON reference.artifact_id = terminal.artifact_id
               WHERE terminal.run_id = ?1
                 AND terminal.task_id = ?2
                 AND terminal.attempt_id = ?3
                 AND terminal.event_type IN (?4, ?5)
                 AND reference.source_kind = ?6"#,
        params![
            run_id.0,
            task_id.0,
            attempt_id.0,
            LifecycleEventType::ToolCompleted.as_str(),
            LifecycleEventType::ToolFailed.as_str(),
            enum_name(ArtifactKind::ToolCall),
        ],
        |row| row.get::<_, u64>(0),
    )?;
    if called > terminal {
        return Err(StoreError::Integrity(format!(
            "attempt {attempt_id} has pending tool calls"
        )));
    }
    Ok(())
}

fn paper_effect_terminal_exists(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    effect_id: &ArtifactId,
) -> StoreResult<bool> {
    let found = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE run_id = ?1 AND artifact_id = ?2 AND event_type IN (?3, ?4))",
        params![
            run_id.0,
            effect_id.0.as_str(),
            LifecycleEventType::ExecutionEffectSettled.as_str(),
            LifecycleEventType::ExecutionEffectRecovered.as_str(),
        ],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(found != 0)
}

fn assert_idempotent_outcome_schedule_commit(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    schedule: &Artifact,
) -> StoreResult<()> {
    let attempt = transaction
        .query_row(
            r#"SELECT a.run_id, a.task_id, a.lease_id, a.epoch, a.status,
                      t.status, t.contract_hash
                 FROM rebuild_attempts AS a
                 JOIN rebuild_tasks AS t ON t.task_id = a.task_id
                WHERE a.attempt_id = ?1"#,
            params![permit.attempt_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((run_id, task_id, lease_id, epoch, attempt_status, task_status, contract_hash)) =
        attempt
    else {
        return Err(StoreError::StalePermit(permit.task_id.clone()));
    };
    if run_id != permit.run_id.0
        || task_id != permit.task_id.0
        || lease_id != permit.lease_id.0
        || epoch != permit.epoch
        || attempt_status != "succeeded"
        || task_status != "succeeded"
        || contract_hash.as_deref().map(ContentHash::new).transpose()? != permit.contract_hash
    {
        return Err(StoreError::StalePermit(permit.task_id.clone()));
    }
    assert_origin_matches(schedule.origin.as_ref(), permit)?;
    let stored = read_artifact(transaction, &schedule.artifact_id)?;
    if stored != *schedule {
        return Err(StoreError::Integrity(
            "outcome schedule retry does not match committed artifact".to_owned(),
        ));
    }
    let output_count = transaction.query_row(
        r#"SELECT COUNT(*) FROM rebuild_attempt_outputs
           WHERE attempt_id = ?1 AND task_id = ?2 AND artifact_id = ?3"#,
        params![
            permit.attempt_id.0,
            permit.task_id.0,
            schedule.artifact_id.0.as_str()
        ],
        |row| row.get::<_, u64>(0),
    )?;
    if output_count != 1 {
        return Err(StoreError::CommittedOutputAttempt {
            task_id: permit.task_id.clone(),
            attempt_id: permit.attempt_id.clone(),
        });
    }
    Ok(())
}

fn assert_origin_matches(
    origin: Option<&ArtifactOrigin>,
    permit: &TaskWritePermit,
) -> StoreResult<()> {
    let Some(origin) = origin else {
        return Err(StoreError::PermitOriginMismatch);
    };
    if origin.run_id.as_ref() != Some(&permit.run_id)
        || origin.task_id.as_ref() != Some(&permit.task_id)
        || origin.attempt_id.as_ref() != Some(&permit.attempt_id)
        || origin.contract_hash != permit.contract_hash
    {
        return Err(StoreError::PermitOriginMismatch);
    }
    Ok(())
}

fn task_retry_policy(
    transaction: &Transaction<'_>,
    task_id: &TaskId,
) -> StoreResult<(RetryPolicy, FailureDisposition)> {
    let (retry_json, on_failure) = transaction
        .query_row(
            "SELECT retry_json, on_failure FROM rebuild_tasks WHERE task_id = ?1",
            params![task_id.0],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .ok_or_else(|| StoreError::MissingTask(task_id.clone()))?;
    Ok((serde_json::from_str(&retry_json)?, parse_enum(&on_failure)?))
}

fn commit_attempt_transaction(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    artifacts: &[Artifact],
    status: TaskStatus,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    commit_attempt_transaction_with_effect(transaction, permit, artifacts, status, None, now)
}

fn commit_attempt_transaction_with_effect(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    artifacts: &[Artifact],
    status: TaskStatus,
    effect_event: Option<(&ArtifactRef, LifecycleEventType)>,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    assert_permit(transaction, permit)?;
    for artifact in artifacts {
        assert_task_artifact_lifecycle(transaction, &permit.run_id, artifact)?;
    }
    let (_, on_failure) = task_retry_policy(transaction, &permit.task_id)?;
    for artifact in artifacts {
        assert_origin_matches(artifact.origin.as_ref(), permit)?;
    }
    if !artifacts.is_empty() {
        insert_artifact_batch(transaction, artifacts)?;
    }
    for artifact in artifacts {
        let event_id = append_event(
            transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&artifact.artifact_id),
            now,
        )?;
        if status == TaskStatus::Succeeded {
            record_attempt_output(transaction, permit, &artifact.artifact_id, event_id)?;
        }
    }
    if let Some((effect, event_type)) = effect_event {
        append_event(
            transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            event_type,
            Some(&effect.artifact_id),
            now,
        )?;
    }
    finish_permitted_task(
        transaction,
        permit,
        status,
        on_failure,
        artifacts.last().map(|artifact| &artifact.artifact_id),
        now,
    )?;
    Ok(())
}

fn finish_permitted_task(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    requested_status: TaskStatus,
    on_failure: FailureDisposition,
    terminal_artifact_id: Option<&ArtifactId>,
    now: DateTime<Utc>,
) -> StoreResult<TaskStatus> {
    let post_terminal_worker = transaction.query_row(
        "SELECT recipe_id = ?1 FROM rebuild_tasks WHERE task_id = ?2",
        params![POST_TERMINAL_WORKER_RECIPE_ID, permit.task_id.0],
        |row| row.get::<_, bool>(0),
    )?;
    let status =
        if requested_status == TaskStatus::Failed && on_failure == FailureDisposition::SkipTask {
            TaskStatus::Skipped
        } else {
            requested_status
        };
    let terminal_event = match status {
        TaskStatus::Succeeded => LifecycleEventType::TaskSucceeded,
        TaskStatus::Failed => LifecycleEventType::TaskFailed,
        TaskStatus::Cancelled => LifecycleEventType::TaskCancelled,
        TaskStatus::Skipped => LifecycleEventType::TaskSkipped,
        _ => unreachable!("terminal status checked above"),
    };
    // Append the task terminal inside this transaction before lifecycle
    // validation.  The validator must see the terminal event itself so it can
    // reject a normal terminal status that leaves an AgentTurnStarted pending;
    // cancellation is the explicit abort-close for a crashed turn.
    append_event(
        transaction,
        &permit.run_id,
        Some(&permit.task_id),
        Some(&permit.attempt_id),
        terminal_event,
        terminal_artifact_id,
        now,
    )?;
    validate_tool_lifecycle_events(transaction, Some(&permit.run_id))?;
    validate_agent_turn_lifecycle_events(transaction, Some(&permit.run_id))?;
    validate_context_lifecycle_events(transaction, Some(&permit.run_id))?;
    validate_gate_lifecycle_events(transaction, Some(&permit.run_id))?;
    if status == TaskStatus::Succeeded {
        ensure_no_pending_tool_calls(
            transaction,
            &permit.run_id,
            &permit.task_id,
            &permit.attempt_id,
        )?;
    }
    transaction.execute(
        r#"UPDATE rebuild_tasks
           SET status = ?1, lease_id = NULL, active_attempt_id = NULL, worker_id = NULL,
               lease_until = NULL, finished_at = ?2
           WHERE task_id = ?3"#,
        params![enum_name(status), now.to_rfc3339(), permit.task_id.0],
    )?;
    transaction.execute(
        "UPDATE rebuild_attempts SET status = ?1, finished_at = ?2 WHERE attempt_id = ?3",
        params![enum_name(status), now.to_rfc3339(), permit.attempt_id.0],
    )?;
    if status == TaskStatus::Failed && !post_terminal_worker {
        match on_failure {
            FailureDisposition::FailRun => cancel_queued_tasks(transaction, &permit.run_id, now)?,
            FailureDisposition::FailTask => {
                cancel_failed_dependents(transaction, &permit.run_id, now)?
            }
            FailureDisposition::SkipTask => unreachable!("failed status is converted to skipped"),
        }
    }
    if !post_terminal_worker {
        refresh_run_status(transaction, &permit.run_id, now)?;
    }
    Ok(status)
}

fn cancel_queued_tasks(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let task_ids = {
        let mut statement = transaction.prepare(
            "SELECT task_id FROM rebuild_tasks WHERE run_id = ?1 AND status = 'queued' ORDER BY task_id",
        )?;
        let rows = statement
            .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
            .map(|row| row.map(TaskId))
            .collect::<Result<Vec<_>, _>>()?;
        rows
    };
    for task_id in task_ids {
        let changed = transaction.execute(
            "UPDATE rebuild_tasks SET status = 'cancelled', finished_at = ?1 WHERE task_id = ?2 AND status = 'queued'",
            params![now.to_rfc3339(), task_id.0],
        )?;
        if changed == 1 {
            append_event(
                transaction,
                run_id,
                Some(&task_id),
                None,
                LifecycleEventType::TaskCancelled,
                None,
                now,
            )?;
        }
    }
    Ok(())
}

fn cancel_failed_dependents(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    loop {
        let task_ids = {
            let mut statement = transaction.prepare(
                r#"SELECT DISTINCT child.task_id
                   FROM rebuild_tasks AS child
                   JOIN rebuild_task_dependencies AS dependency
                     ON dependency.task_id = child.task_id
                   JOIN rebuild_tasks AS parent
                     ON parent.task_id = dependency.depends_on_task_id
                   WHERE child.run_id = ?1
                     AND child.status = 'queued'
                     AND parent.status IN ('failed', 'cancelled')
                   ORDER BY child.task_id"#,
            )?;
            let rows = statement
                .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
                .map(|row| row.map(TaskId))
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        if task_ids.is_empty() {
            return Ok(());
        }
        for task_id in task_ids {
            let changed = transaction.execute(
                "UPDATE rebuild_tasks SET status = 'cancelled', finished_at = ?1 WHERE task_id = ?2 AND status = 'queued'",
                params![now.to_rfc3339(), task_id.0],
            )?;
            if changed == 1 {
                append_event(
                    transaction,
                    run_id,
                    Some(&task_id),
                    None,
                    LifecycleEventType::TaskCancelled,
                    None,
                    now,
                )?;
            }
        }
    }
}

fn append_event(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    task_id: Option<&TaskId>,
    attempt_id: Option<&akzio_domain::AttemptId>,
    event_type: LifecycleEventType,
    artifact_id: Option<&ArtifactId>,
    created_at: DateTime<Utc>,
) -> StoreResult<i64> {
    validate_event_shape(
        event_type,
        task_id.is_some(),
        attempt_id.is_some(),
        artifact_id.is_some(),
    )?;
    transaction.execute(
        r#"INSERT INTO rebuild_events
           (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
        params![
            run_id.0,
            task_id.map(|id| id.0.as_str()),
            attempt_id.map(|id| id.0.as_str()),
            event_type.as_str(),
            artifact_id.map(|id| id.0.as_str()),
            created_at.to_rfc3339(),
        ],
    )?;
    Ok(transaction.last_insert_rowid())
}

fn append_task_event(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    event_type: LifecycleEventType,
    created_at: DateTime<Utc>,
) -> StoreResult<i64> {
    if event_type != LifecycleEventType::AgentTurnStarted {
        return Err(StoreError::InvalidLifecycleEventShape {
            event_type: event_type.as_str().to_owned(),
        });
    }
    append_event(
        transaction,
        &permit.run_id,
        Some(&permit.task_id),
        Some(&permit.attempt_id),
        event_type,
        None,
        created_at,
    )
}

fn validate_event_shape(
    event_type: LifecycleEventType,
    has_task_id: bool,
    has_attempt_id: bool,
    has_artifact_id: bool,
) -> StoreResult<()> {
    let effect_event = matches!(
        event_type,
        LifecycleEventType::ExecutionEffectIntent
            | LifecycleEventType::ExecutionEffectRecovered
            | LifecycleEventType::ExecutionEffectSettled
    );
    if effect_event && !(has_task_id && has_attempt_id && has_artifact_id) {
        return Err(StoreError::InvalidLifecycleEventShape {
            event_type: event_type.as_str().to_owned(),
        });
    }
    if has_attempt_id && !has_task_id {
        return Err(StoreError::Domain(DomainError::AttemptOriginWithoutTask));
    }

    let valid = match event_type {
        LifecycleEventType::WorkflowCreated => !has_task_id && !has_attempt_id && has_artifact_id,
        LifecycleEventType::RunCancelRequested => {
            !has_task_id && !has_attempt_id && !has_artifact_id
        }
        LifecycleEventType::OutcomeWorkerEnqueued => {
            has_task_id && !has_attempt_id && has_artifact_id
        }
        LifecycleEventType::TaskCancelled => has_task_id && (!has_artifact_id || has_attempt_id),
        LifecycleEventType::TaskStarted
        | LifecycleEventType::AgentTurnStarted
        | LifecycleEventType::TaskDeferred
        | LifecycleEventType::TaskRecovered
        | LifecycleEventType::TaskRecoveryExhausted
        | LifecycleEventType::TaskRetryExhausted
        | LifecycleEventType::TaskRetryScheduled => {
            has_task_id && has_attempt_id && !has_artifact_id
        }
        LifecycleEventType::TaskFailed
        | LifecycleEventType::TaskSkipped
        | LifecycleEventType::TaskSucceeded => has_task_id && has_attempt_id,
        _ => has_task_id && has_attempt_id && has_artifact_id,
    };

    if !valid {
        return Err(StoreError::InvalidLifecycleEventShape {
            event_type: event_type.as_str().to_owned(),
        });
    }

    Ok(())
}

impl V2Store {
    fn record_attempt_relation_in_transaction(
        &self,
        transaction: &Transaction<'_>,
        permit: &TaskWritePermit,
        parent_attempt_id: &AttemptId,
        relation: AttemptRelationKind,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let payload = AttemptRelation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            run_id: permit.run_id.clone(),
            task_id: permit.task_id.clone(),
            parent_attempt_id: parent_attempt_id.clone(),
            child_attempt_id: permit.attempt_id.clone(),
            relation,
            created_at: now,
        };
        payload.validate()?;
        let artifact = Artifact::new(
            ArtifactKind::AttemptRelation,
            blob::put_blob_bytes(
                transaction,
                &serde_json::to_vec(&payload)?,
                "application/json".to_owned(),
            )?,
            "akzio-store.attempt_relation",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio-store".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            Vec::new(),
            now,
        )?;
        insert_artifact(transaction, &artifact)?;
        append_event(
            transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::AttemptRelationCreated,
            Some(&artifact.artifact_id),
            now,
        )?;
        Ok(())
    }
}

fn record_attempt_output(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    artifact_id: &ArtifactId,
    event_id: i64,
) -> StoreResult<()> {
    transaction.execute(
        r#"INSERT OR IGNORE INTO rebuild_attempt_outputs
            (attempt_id, task_id, artifact_id, event_id)
          VALUES (?1, ?2, ?3, ?4)"#,
        params![
            permit.attempt_id.0,
            permit.task_id.0,
            artifact_id.0.as_str(),
            event_id,
        ],
    )?;
    Ok(())
}

fn read_committed_attempt_outputs(
    connection: &Connection,
    expected_run_id: Option<&RunId>,
    task_id: &TaskId,
    attempt_id: &AttemptId,
) -> StoreResult<Vec<Artifact>> {
    let attempt = connection
        .query_row(
            r#"SELECT a.run_id, a.task_id, a.status, t.status
                 FROM rebuild_attempts AS a
                 JOIN rebuild_tasks AS t ON t.task_id = a.task_id
                WHERE a.attempt_id = ?1"#,
            params![attempt_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((attempt_run_id, attempt_task_id, attempt_status, task_status)) = attempt else {
        return Err(StoreError::CommittedOutputAttempt {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        });
    };
    if attempt_task_id != task_id.0
        || attempt_status != "succeeded"
        || task_status != "succeeded"
        || expected_run_id.is_some_and(|run_id| attempt_run_id != run_id.0)
    {
        return Err(StoreError::CommittedOutputAttempt {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        });
    }

    let mut statement = connection.prepare(
        r#"SELECT o.artifact_id
              FROM rebuild_attempt_outputs AS o
              JOIN rebuild_events AS e ON e.event_id = o.event_id
             WHERE o.attempt_id = ?1
               AND o.task_id = ?2
               AND e.run_id = ?3
               AND e.task_id = o.task_id
               AND e.attempt_id = o.attempt_id
               AND e.event_type = 'artifact.committed'
               AND e.artifact_id = o.artifact_id
             ORDER BY o.event_id ASC"#,
    )?;
    let ids = statement
        .query_map(params![attempt_id.0, task_id.0, attempt_run_id], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if ids.is_empty() {
        return Err(StoreError::CommittedOutputAttempt {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        });
    }
    drop(statement);
    ids.into_iter()
        .map(|id| read_artifact(connection, &ArtifactId(ContentHash::new(id)?)))
        .collect()
}

fn refresh_run_status(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let statuses = transaction
        .prepare("SELECT status FROM rebuild_tasks WHERE run_id = ?1")?
        .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if statuses.is_empty()
        || statuses
            .iter()
            .any(|status| status == "running" || status == "queued")
    {
        return Ok(());
    }
    let status = if statuses.iter().any(|status| status == "failed") {
        "failed"
    } else if statuses.iter().all(|status| status == "cancelled") {
        "cancelled"
    } else {
        "completed"
    };
    transaction.execute(
        "UPDATE rebuild_runs SET status = ?1, finished_at = ?2 WHERE run_id = ?3",
        params![status, now.to_rfc3339(), run_id.0],
    )?;
    Ok(())
}

fn read_artifact(connection: &Connection, artifact_id: &ArtifactId) -> StoreResult<Artifact> {
    let row = connection
        .query_row(
            r#"SELECT kind, blob_hash, media_type, bytes, producer, lifecycle, provenance_json, origin_json, created_at
               FROM rebuild_artifacts WHERE artifact_id = ?1"#,
            params![artifact_id.0.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                ))
            },
        )
        .optional()?;
    let Some((kind, hash, media_type, bytes, producer, lifecycle, provenance, origin, created_at)) =
        row
    else {
        return Err(StoreError::MissingArtifact(artifact_id.clone()));
    };
    let mut statement = connection.prepare(
        r#"SELECT source_artifact_id, source_kind
           FROM rebuild_artifact_refs WHERE artifact_id = ?1
           ORDER BY source_artifact_id"#,
    )?;
    let source_refs = statement
        .query_map(params![artifact_id.0.as_str()], |row| {
            Ok(ArtifactRef {
                artifact_id: ArtifactId(
                    ContentHash::new(row.get::<_, String>(0)?).map_err(|error| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                    })?,
                ),
                kind: parse_enum(&row.get::<_, String>(1)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Artifact {
        schema_version: V2_SCHEMA_VERSION,
        artifact_id: artifact_id.clone(),
        kind: parse_enum(&kind)?,
        blob: BlobRef {
            hash: ContentHash::new(hash)?,
            media_type,
            bytes,
        },
        producer,
        lifecycle: parse_enum(&lifecycle)?,
        provenance: serde_json::from_str(&provenance)?,
        origin: origin
            .map(|encoded| serde_json::from_str::<Option<ArtifactOrigin>>(&encoded))
            .transpose()?
            .flatten(),
        source_refs,
        created_at: parse_time(&created_at)?,
    })
}

fn read_kind_artifacts(connection: &Connection, kind: ArtifactKind) -> StoreResult<Vec<Artifact>> {
    let mut statement = connection.prepare(
        "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY created_at ASC, artifact_id ASC",
    )?;
    let ids = statement
        .query_map(params![enum_name(kind)], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    ids.into_iter()
        .map(|id| read_artifact(connection, &ArtifactId(ContentHash::new(id)?)))
        .collect()
}

fn verify_retrospective_history(store: &V2Store, connection: &Connection) -> StoreResult<()> {
    let mut identities = BTreeSet::new();
    for artifact in read_kind_artifacts(connection, ArtifactKind::Retrospective)? {
        artifact.validate()?;
        let payload: Retrospective = store.read_artifact_payload(&artifact)?;
        payload.validate()?;
        let run_id = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            .ok_or_else(|| StoreError::Integrity("retrospective has no run lineage".to_owned()))?;
        let identity = (run_id.clone(), payload.outcome_id.clone(), payload.horizon);
        if !identities.insert(identity) {
            return Err(StoreError::Integrity(
                "duplicate retrospective identity".to_owned(),
            ));
        }
        if payload.horizon == OutcomeHorizon::T5
            && artifact.lifecycle != ArtifactLifecycle::Canonical
        {
            return Err(StoreError::Integrity(
                "T5 retrospective is not canonical".to_owned(),
            ));
        }
        if payload.horizon != OutcomeHorizon::T5
            && artifact.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::Integrity(
                "intermediate retrospective is not RunScoped".to_owned(),
            ));
        }
        let outcome = read_artifact(connection, &payload.outcome.artifact_id)?;
        if outcome.kind != ArtifactKind::Outcome {
            return Err(StoreError::Integrity(
                "retrospective outcome closure is invalid".to_owned(),
            ));
        }
        let outcome_payload: Outcome = store.read_artifact_payload(&outcome)?;
        if payload.horizon == OutcomeHorizon::T5 {
            outcome_payload.validate_sealed().map_err(|error| {
                StoreError::Integrity(format!("sealed outcome is invalid: {error}"))
            })?;
            if outcome.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(
                    "T5 retrospective points to non-canonical outcome".to_owned(),
                ));
            }
        } else {
            outcome_payload.validate().map_err(|error| {
                StoreError::Integrity(format!("partial outcome is invalid: {error}"))
            })?;
            if outcome.lifecycle != ArtifactLifecycle::RunScoped
                || outcome_payload.sealed_at.is_some()
            {
                return Err(StoreError::Integrity(
                    "intermediate retrospective points to sealed outcome".to_owned(),
                ));
            }
        }
        if payload.horizon == OutcomeHorizon::T5
            && payload.status == RetrospectiveStatus::Complete
            && artifact.lifecycle != ArtifactLifecycle::Canonical
        {
            return Err(StoreError::Integrity(
                "complete T5 retrospective is not canonical".to_owned(),
            ));
        }
    }
    Ok(())
}

fn verify_attempt_relation_history(store: &V2Store, connection: &Connection) -> StoreResult<()> {
    let mut parent_by_child = BTreeMap::<(RunId, TaskId, AttemptId), AttemptId>::new();
    for artifact in read_kind_artifacts(connection, ArtifactKind::AttemptRelation)? {
        artifact.validate()?;
        let relation: AttemptRelation = store.read_artifact_payload(&artifact)?;
        relation.validate()?;
        let parent_exists = connection
            .query_row(
                "SELECT 1 FROM rebuild_attempts WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3",
                params![relation.run_id.0, relation.task_id.0, relation.parent_attempt_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !parent_exists {
            return Err(StoreError::Integrity(
                "attempt relation parent is missing".to_owned(),
            ));
        }
        let child_exists = connection
            .query_row(
                "SELECT 1 FROM rebuild_attempts WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3",
                params![relation.run_id.0, relation.task_id.0, relation.child_attempt_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !child_exists {
            return Err(StoreError::Integrity(
                "attempt relation child missing".to_owned(),
            ));
        }
        let key = (
            relation.run_id.clone(),
            relation.task_id.clone(),
            relation.child_attempt_id.clone(),
        );
        if parent_by_child
            .insert(key.clone(), relation.parent_attempt_id.clone())
            .is_some()
        {
            return Err(StoreError::Integrity(
                "attempt relation child has multiple parents".to_owned(),
            ));
        }
    }
    for (run_id, task_id, child) in parent_by_child.keys() {
        let mut cursor = child.clone();
        let mut hops = 0_u16;
        while let Some(parent) =
            parent_by_child.get(&(run_id.clone(), task_id.clone(), cursor.clone()))
        {
            cursor = parent.clone();
            hops = hops.saturating_add(1);
            if cursor == *child || hops > 1_024 {
                return Err(StoreError::Integrity("attempt relation cycle".to_owned()));
            }
        }
    }
    Ok(())
}

fn row_to_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<(RunId, WorkflowNode)> {
    let task_id = TaskId(row.get(0)?);
    let run_id = RunId(row.get(1)?);
    let recipe_id = TaskRecipeId::new(row.get::<_, String>(2)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let budget = serde_json::from_str(&row.get::<_, String>(6)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let retry = serde_json::from_str(&row.get::<_, String>(7)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let on_failure = parse_enum(&row.get::<_, String>(8)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    Ok((
        run_id,
        WorkflowNode {
            task_id,
            recipe_id,
            contract_hash: row
                .get::<_, Option<String>>(4)?
                .map(ContentHash::new)
                .transpose()
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            objective: row.get(3)?,
            dependencies: Vec::new(),
            input_artifacts: serde_json::from_str(&row.get::<_, String>(10)?)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            priority: row.get(5)?,
            budget,
            retry,
            on_failure,
            parent_task_id: row.get::<_, Option<String>>(9)?.map(TaskId),
        },
    ))
}

fn parse_time(value: &str) -> StoreResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| StoreError::Integrity(format!("invalid time {value}: {error}")))
}

/// The indexed subject ID is derived from this typed JSON, never accepted as
/// an independent authority. A corrupt or hand-edited row must therefore
/// fail closed rather than silently changing a policy namespace.
fn parse_persisted_subject(subject_id: &str, subject_json: &str) -> StoreResult<PolicySubject> {
    let subject: PolicySubject = serde_json::from_str(subject_json)?;
    subject.validate()?;
    if subject.subject_id() != subject_id {
        return Err(StoreError::Integrity(format!(
            "policy subject JSON does not match indexed identity {subject_id}"
        )));
    }
    Ok(subject)
}

fn read_policy_evaluation(
    connection: &Connection,
    evaluation_artifact_id: &ArtifactId,
) -> StoreResult<Option<StoredPolicyEvaluation>> {
    let row = connection
        .query_row(
            r#"SELECT subject_id, subject_json, outcome_artifact_id, experience_artifact_id,
                      candidate_policy_artifact_id, from_state_json, to_state_json,
                      transition_id, run_id, consumed_pair_cursor, event_cursor, completed_at
               FROM rebuild_policy_evaluations WHERE evaluation_artifact_id = ?1"#,
            params![evaluation_artifact_id.0.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, String>(11)?,
                ))
            },
        )
        .optional()?;
    let Some((
        subject_id,
        subject_json,
        outcome_artifact_id,
        experience_artifact_id,
        candidate_policy_artifact_id,
        from,
        to,
        transition_id,
        run_id,
        consumed_pair_cursor,
        event_cursor,
        completed_at,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(StoredPolicyEvaluation {
        subject: parse_persisted_subject(&subject_id, &subject_json)?,
        outcome_artifact_id: ArtifactId(ContentHash::new(outcome_artifact_id)?),
        experience_artifact_id: ArtifactId(ContentHash::new(experience_artifact_id)?),
        evaluation_artifact_id: evaluation_artifact_id.clone(),
        candidate_policy_artifact_id: candidate_policy_artifact_id
            .map(ContentHash::new)
            .transpose()?
            .map(ArtifactId),
        from: serde_json::from_str(&from)?,
        to: serde_json::from_str(&to)?,
        transition_id: transition_id.map(PolicyTransitionId),
        run_id: RunId(run_id),
        consumed_pair_cursor,
        event_cursor,
        completed_at: parse_time(&completed_at)?,
    }))
}

fn read_policy_consumption_head(
    connection: &Connection,
    expected_subject: &PolicySubject,
) -> StoreResult<Option<PolicyConsumptionHead>> {
    let subject_id = expected_subject.subject_id();
    let row = connection
        .query_row(
            r#"SELECT subject_json, consumed_pair_cursor, evaluation_artifact_id,
                       evaluation_event_cursor, updated_at
                FROM rebuild_policy_consumption_heads WHERE subject_id = ?1"#,
            params![subject_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((
        subject_json,
        consumed_pair_cursor,
        evaluation_artifact_id,
        evaluation_cursor,
        updated_at,
    )) = row
    else {
        return Ok(None);
    };
    let subject = parse_persisted_subject(&subject_id, &subject_json)?;
    if &subject != expected_subject {
        return Err(StoreError::Integrity(format!(
            "policy consumption head {subject_id} subject identity disagrees with lookup"
        )));
    }
    Ok(Some(PolicyConsumptionHead {
        subject,
        consumed_pair_cursor,
        evaluation_artifact_id: ArtifactId(ContentHash::new(evaluation_artifact_id)?),
        evaluation_cursor,
        updated_at: parse_time(&updated_at)?,
    }))
}

fn max_shadow_pair_cursor(connection: &Connection, subject: &PolicySubject) -> StoreResult<i64> {
    connection
        .query_row(
            "SELECT COALESCE(MAX(pair_event_cursor), 0) FROM rebuild_shadow_pairs WHERE subject_id = ?1",
            params![subject.subject_id()],
            |row| row.get::<_, i64>(0),
        )
        .map_err(Into::into)
}

fn shadow_pair_counts_between(
    connection: &Connection,
    subject: &PolicySubject,
    after_cursor: i64,
    through_cursor: i64,
) -> StoreResult<[u64; 3]> {
    if after_cursor < 0 || through_cursor < after_cursor {
        return Err(StoreError::InvalidLearningCommit(
            "shadow_pair.snapshot_cursor",
        ));
    }
    let mut counts = [0; 3];
    for (index, horizon) in OutcomeHorizon::ALL.into_iter().enumerate() {
        counts[index] = connection.query_row(
            "SELECT COUNT(*) FROM rebuild_shadow_pairs \
             WHERE subject_id = ?1 AND horizon = ?2 \
               AND pair_event_cursor > ?3 AND pair_event_cursor <= ?4",
            params![
                subject.subject_id(),
                enum_name(horizon),
                after_cursor,
                through_cursor
            ],
            |row| row.get(0),
        )?;
    }
    Ok(counts)
}

fn validate_policy_shadow_pair_snapshot(
    connection: &Connection,
    subject: &PolicySubject,
    snapshot: PolicyShadowPairSnapshot,
) -> StoreResult<()> {
    let current_after = read_policy_consumption_head(connection, subject)?
        .map_or(0, |head| head.consumed_pair_cursor);
    if snapshot.after_cursor != current_after {
        return Err(StoreError::InvalidLearningCommit(
            "policy_evaluation.pair_snapshot_stale",
        ));
    }
    let current_max = max_shadow_pair_cursor(connection, subject)?;
    if snapshot.through_cursor < snapshot.after_cursor || snapshot.through_cursor > current_max {
        return Err(StoreError::InvalidLearningCommit(
            "policy_evaluation.pair_snapshot_boundary",
        ));
    }
    if snapshot.through_cursor > snapshot.after_cursor {
        let boundary_exists = connection
            .query_row(
                "SELECT 1 FROM rebuild_shadow_pairs \
                 WHERE subject_id = ?1 AND pair_event_cursor = ?2",
                params![subject.subject_id(), snapshot.through_cursor],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !boundary_exists {
            return Err(StoreError::InvalidLearningCommit(
                "policy_evaluation.pair_snapshot_boundary",
            ));
        }
    }
    if shadow_pair_counts_between(
        connection,
        subject,
        snapshot.after_cursor,
        snapshot.through_cursor,
    )? != snapshot.counts_by_horizon
    {
        return Err(StoreError::InvalidLearningCommit(
            "policy_evaluation.pair_snapshot_counts",
        ));
    }
    Ok(())
}

fn reject_generic_learning_artifact(artifact: &Artifact) -> StoreResult<()> {
    if matches!(
        artifact.kind,
        ArtifactKind::Outcome
            | ArtifactKind::Experience
            | ArtifactKind::Evaluation
            | ArtifactKind::CandidatePolicy
    ) {
        return Err(StoreError::InvalidLearningCommit(
            "learning_artifact.atomic_commit_required",
        ));
    }
    Ok(())
}

fn same_policy_evaluation(
    existing: &StoredPolicyEvaluation,
    commit: &PolicyEvaluationCommit,
) -> bool {
    existing.subject == commit.subject
        && existing.outcome_artifact_id == commit.outcome.artifact_id
        && existing.experience_artifact_id == commit.experience.artifact_id
        && existing.evaluation_artifact_id == commit.evaluation.artifact_id
        && existing.candidate_policy_artifact_id
            == commit
                .candidate_policy
                .as_ref()
                .map(|artifact| artifact.artifact_id.clone())
        && existing.from == commit.from
        && existing.to == commit.to
        && existing.transition_id
            == commit
                .transition
                .as_ref()
                .map(|transition| transition.transition_id.clone())
        && existing.run_id == commit.permit.run_id
        && existing.consumed_pair_cursor == commit.pair_snapshot.through_cursor
        && existing.completed_at == commit.completed_at
}

fn read_policy_head(
    connection: &Connection,
    expected_subject: &PolicySubject,
) -> StoreResult<Option<PolicyHead>> {
    let subject_id = expected_subject.subject_id();
    let row = connection
        .query_row(
            "SELECT subject_json, state_json, revision, transition_id, transition_event_cursor, updated_at FROM rebuild_policy_heads WHERE subject_id = ?1",
            params![subject_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((subject_json, state, revision, transition_id, transition_cursor, updated_at)) = row
    else {
        return Ok(None);
    };
    let subject = parse_persisted_subject(&subject_id, &subject_json)?;
    if &subject != expected_subject {
        return Err(StoreError::Integrity(format!(
            "policy head {subject_id} subject identity disagrees with lookup"
        )));
    }
    Ok(Some(PolicyHead {
        subject,
        state: serde_json::from_str(&state)?,
        revision,
        transition_id: PolicyTransitionId(transition_id),
        transition_cursor,
        updated_at: parse_time(&updated_at)?,
    }))
}

fn read_policy_transition(
    connection: &Connection,
    transition_id: &PolicyTransitionId,
) -> StoreResult<Option<PolicyTransitionRecord>> {
    let row = connection
        .query_row(
            r#"SELECT subject_id, subject_json, from_state_json, to_state_json,
                      evaluation_artifact_id, run_id, revision, created_at, event_cursor
               FROM rebuild_policy_transitions WHERE transition_id = ?1"#,
            params![transition_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, u64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()?;
    let Some((
        subject_id,
        subject_json,
        from,
        to,
        evaluation_id,
        run_id,
        revision,
        created_at,
        transition_cursor,
    )) = row
    else {
        return Ok(None);
    };
    let subject = parse_persisted_subject(&subject_id, &subject_json)?;
    Ok(Some(PolicyTransitionRecord {
        transition: PolicyTransition {
            schema_version: V2_SCHEMA_VERSION,
            transition_id: transition_id.clone(),
            subject,
            from: serde_json::from_str(&from)?,
            to: serde_json::from_str(&to)?,
            evaluation: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(evaluation_id)?),
                kind: ArtifactKind::Evaluation,
            },
            created_at: parse_time(&created_at)?,
        },
        run_id: RunId(run_id),
        revision,
        transition_cursor,
    }))
}

fn read_policy_transitions(
    connection: &Connection,
    expected_subject: &PolicySubject,
) -> StoreResult<Vec<PolicyTransitionRecord>> {
    let subject_id = expected_subject.subject_id();
    let mut statement = connection.prepare(
        r#"SELECT transition_id, subject_json, from_state_json, to_state_json,
                  evaluation_artifact_id, run_id, revision, created_at, event_cursor
           FROM rebuild_policy_transitions WHERE subject_id = ?1 ORDER BY revision ASC"#,
    )?;
    let rows = statement
        .query_map(params![subject_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, u64>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(transition_id, subject_json, from, to, evaluation_id, run_id, revision, created_at, transition_cursor)| {
            let subject = parse_persisted_subject(&subject_id, &subject_json)?;
            if &subject != expected_subject {
                return Err(StoreError::Integrity(format!(
                    "policy transition {transition_id} subject identity disagrees with key {subject_id}"
                )));
            }
            Ok(PolicyTransitionRecord {
                transition: PolicyTransition {
                    schema_version: V2_SCHEMA_VERSION,
                    transition_id: PolicyTransitionId(transition_id),
                    subject,
                    from: serde_json::from_str(&from)?,
                    to: serde_json::from_str(&to)?,
                    evaluation: ArtifactRef {
                        artifact_id: ArtifactId(ContentHash::new(evaluation_id)?),
                        kind: ArtifactKind::Evaluation,
                    },
                    created_at: parse_time(&created_at)?,
                },
                run_id: RunId(run_id),
                revision,
                transition_cursor,
            })
        })
        .collect()
}

fn read_shadow_pair(
    connection: &Connection,
    pair_key: &ContentHash,
) -> StoreResult<Option<StoredShadowPair>> {
    let row = connection
        .query_row(
            r#"SELECT subject_id, subject_json, parent_decision_artifact_id, execution_context_artifact_id,
                      candidate_decision_artifact_id, candidate_contract_hash, candidate_topology_id,
                      horizon, parent_outcome_artifact_id, candidate_outcome_artifact_id, completed_at,
                      pair_event_cursor
               FROM rebuild_shadow_pairs WHERE pair_key = ?1"#,
            params![pair_key.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, i64>(11)?,
                ))
            },
        )
        .optional()?;
    let Some((
        subject_id,
        subject_json,
        parent_decision,
        execution_context,
        candidate_decision,
        candidate_contract_hash,
        candidate_topology_id,
        horizon,
        parent_outcome,
        candidate_outcome,
        completed_at,
        completion_cursor,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(StoredShadowPair {
        pair_key: pair_key.clone(),
        completion: ShadowPairCompletion {
            subject: parse_persisted_subject(&subject_id, &subject_json)?,
            parent_decision: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(parent_decision)?),
                kind: ArtifactKind::Decision,
            },
            execution_context: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(execution_context)?),
                kind: ArtifactKind::ExecutionContext,
            },
            candidate_decision: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(candidate_decision)?),
                kind: ArtifactKind::Decision,
            },
            candidate_contract_hash: ContentHash::new(candidate_contract_hash)?,
            candidate_topology_id,
            horizon: parse_enum(&horizon)?,
            parent_outcome: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(parent_outcome)?),
                kind: ArtifactKind::Outcome,
            },
            candidate_outcome: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::new(candidate_outcome)?),
                kind: ArtifactKind::Outcome,
            },
            completed_at: parse_time(&completed_at)?,
        },
        completion_cursor,
    }))
}

fn same_shadow_pair(left: &ShadowPairCompletion, right: &ShadowPairCompletion) -> bool {
    left.subject == right.subject
        && left.parent_decision == right.parent_decision
        && left.execution_context == right.execution_context
        && left.candidate_decision == right.candidate_decision
        && left.candidate_contract_hash == right.candidate_contract_hash
        && left.candidate_topology_id == right.candidate_topology_id
        && left.horizon == right.horizon
        && left.parent_outcome == right.parent_outcome
        && left.candidate_outcome == right.candidate_outcome
}

fn run_purpose_from_connection(connection: &Connection, run_id: &RunId) -> StoreResult<RunPurpose> {
    let purpose = connection
        .query_row(
            "SELECT purpose FROM rebuild_runs WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::MissingRun(run_id.clone()))?;
    parse_enum(&purpose)
}

fn assert_task_artifact_lifecycle(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    artifact: &Artifact,
) -> StoreResult<()> {
    let purpose = run_purpose_from_connection(transaction, run_id)?;
    let allowed = match artifact.lifecycle {
        ArtifactLifecycle::Ephemeral => false,
        ArtifactLifecycle::RunScoped => true,
        ArtifactLifecycle::Canonical => purpose == RunPurpose::Paper,
    };
    if allowed {
        return Ok(());
    }
    Err(StoreError::InvalidTaskArtifactLifecycle {
        purpose,
        lifecycle: artifact.lifecycle,
    })
}

fn workflow_graph_run_purpose(
    connection: &Connection,
    artifact_id: &ArtifactId,
) -> StoreResult<RunPurpose> {
    let purpose = connection
        .query_row(
            "SELECT purpose FROM rebuild_runs WHERE graph_artifact_id = ?1",
            params![artifact_id.0.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::MissingArtifact(artifact_id.clone()))?;
    parse_enum(&purpose)
}

fn artifact_run_purpose(connection: &Connection, artifact: &Artifact) -> StoreResult<RunPurpose> {
    let run_id = artifact
        .origin
        .as_ref()
        .and_then(|origin| origin.run_id.as_ref())
        .ok_or(StoreError::InvalidLearningCommit(
            "learning_artifact.origin",
        ))?;
    run_purpose_from_connection(connection, run_id)
}

fn assert_artifact_from_allowed_purposes(
    connection: &Connection,
    artifact: &Artifact,
    allowed_purposes: &[RunPurpose],
) -> StoreResult<()> {
    let purpose = artifact_run_purpose(connection, artifact)?;
    if allowed_purposes.contains(&purpose) {
        return Ok(());
    }
    if allowed_purposes == [RunPurpose::Paper] {
        return Err(StoreError::NonCanonicalLearningPurpose(purpose));
    }
    Err(StoreError::InvalidLearningCommit(
        "learning_artifact.run_purpose",
    ))
}

fn assert_artifact_from_paper_with_connection(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<()> {
    assert_artifact_from_allowed_purposes(connection, artifact, &[RunPurpose::Paper])
}

fn assert_paper_run(transaction: &Transaction<'_>, run_id: &RunId) -> StoreResult<()> {
    let purpose = run_purpose_from_connection(transaction, run_id)?;
    if purpose != RunPurpose::Paper {
        return Err(StoreError::NonCanonicalLearningPurpose(purpose));
    }
    Ok(())
}

fn read_required_artifact(
    connection: &Connection,
    reference: &ArtifactRef,
    error: &'static str,
) -> StoreResult<Artifact> {
    let artifact = read_artifact(connection, &reference.artifact_id)?;
    if artifact.kind != reference.kind {
        return Err(StoreError::InvalidLearningCommit(error));
    }
    Ok(artifact)
}

fn assert_canonical_paper_artifact(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<()> {
    if artifact.lifecycle != ArtifactLifecycle::Canonical {
        return Err(StoreError::InvalidLearningCommit(
            "shadow_pair.parent_lifecycle",
        ));
    }
    assert_artifact_from_paper_with_connection(connection, artifact)
}

fn assert_shadow_candidate_artifact(
    connection: &Connection,
    artifact: &Artifact,
) -> StoreResult<()> {
    match artifact_run_purpose(connection, artifact)? {
        RunPurpose::Paper => Ok(()),
        RunPurpose::Shadow if artifact.lifecycle != ArtifactLifecycle::Canonical => Ok(()),
        RunPurpose::Shadow => Err(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_shadow_canonical",
        )),
        _ => Err(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_purpose",
        )),
    }
}

fn assert_candidate_decision_binding(
    connection: &Connection,
    candidate_decision: &Artifact,
    completion: &ShadowPairCompletion,
) -> StoreResult<()> {
    let origin = candidate_decision
        .origin
        .as_ref()
        .ok_or(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_origin",
        ))?;
    if origin.contract_hash.as_ref() != Some(&completion.candidate_contract_hash) {
        return Err(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_contract",
        ));
    }
    let run_id = origin
        .run_id
        .as_ref()
        .ok_or(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_run",
        ))?;
    let topology_id = connection
        .query_row(
            "SELECT topology_id FROM rebuild_runs WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::MissingRun(run_id.clone()))?;
    if topology_id != completion.candidate_topology_id {
        return Err(StoreError::InvalidLearningCommit(
            "shadow_pair.candidate_topology",
        ));
    }
    Ok(())
}

fn outcome_schedule_source_refs(schedule: &OutcomeSchedule) -> Vec<ArtifactRef> {
    let mut references = vec![
        schedule.decision.clone(),
        schedule.decision_context.clone(),
        schedule.execution_context.clone(),
    ];
    match &schedule.execution {
        OutcomeExecutionLineage::NoOrder { execution_verdict } => {
            references.push(execution_verdict.clone());
        }
        OutcomeExecutionLineage::ReconciledPaper {
            execution_verdict,
            commitment,
            reconciliation,
        } => {
            references.push(execution_verdict.clone());
            references.push(commitment.clone());
            references.push(reconciliation.clone());
        }
    }
    references
}

#[allow(clippy::match_like_matches_macro)]
fn is_allowed_policy_transition(from: PolicyState, to: PolicyState) -> bool {
    use akzio_domain::{CandidatePolicyState as Candidate, MemoryLifecycle as Memory};

    match (from, to) {
        (
            PolicyState::Memory(Memory::Candidate),
            PolicyState::Memory(Memory::Active | Memory::Contested | Memory::Retired),
        )
        | (
            PolicyState::Memory(Memory::Active),
            PolicyState::Memory(Memory::Proven | Memory::Contested | Memory::Retired),
        )
        | (
            PolicyState::Memory(Memory::Proven),
            PolicyState::Memory(Memory::Contested | Memory::Retired),
        )
        | (
            PolicyState::Memory(Memory::Contested),
            PolicyState::Memory(Memory::Active | Memory::Retired),
        )
        | (
            PolicyState::Contract(Candidate::Candidate),
            PolicyState::Contract(Candidate::Canary10),
        )
        | (
            PolicyState::Contract(Candidate::Canary10),
            PolicyState::Contract(Candidate::Canary25 | Candidate::Candidate),
        )
        | (
            PolicyState::Contract(Candidate::Canary25),
            PolicyState::Contract(Candidate::Canary50 | Candidate::Candidate),
        )
        | (
            PolicyState::Contract(Candidate::Canary50),
            PolicyState::Contract(Candidate::Active | Candidate::Candidate),
        )
        | (PolicyState::Contract(Candidate::Active), PolicyState::Contract(Candidate::Candidate))
        | (
            PolicyState::Topology(Candidate::Candidate),
            PolicyState::Topology(Candidate::Canary10),
        )
        | (
            PolicyState::Topology(Candidate::Canary10),
            PolicyState::Topology(Candidate::Canary25 | Candidate::Candidate),
        )
        | (
            PolicyState::Topology(Candidate::Canary25),
            PolicyState::Topology(Candidate::Canary50 | Candidate::Candidate),
        )
        | (
            PolicyState::Topology(Candidate::Canary50),
            PolicyState::Topology(Candidate::Active | Candidate::Candidate),
        )
        | (PolicyState::Topology(Candidate::Active), PolicyState::Topology(Candidate::Candidate)) => {
            true
        }
        _ => false,
    }
}

fn has_exact_source_refs(artifact: &Artifact, expected: &[ArtifactRef]) -> bool {
    let actual = artifact
        .source_refs
        .iter()
        .map(source_ref_fingerprint)
        .collect::<BTreeSet<_>>();
    let expected_len = expected.len();
    let expected = expected
        .iter()
        .map(source_ref_fingerprint)
        .collect::<BTreeSet<_>>();
    actual.len() == artifact.source_refs.len()
        && expected.len() == expected_len
        && actual == expected
}

fn source_ref_fingerprint(reference: &ArtifactRef) -> (String, String) {
    (
        reference.artifact_id.0.as_str().to_owned(),
        enum_name(reference.kind),
    )
}

fn same_paper_commitment(left: &PaperCommitment, right: &PaperCommitment) -> bool {
    left.plan_hash == right.plan_hash
        && left.execution_context == right.execution_context
        && left.broker_session == right.broker_session
        && left.client_order_ids == right.client_order_ids
}

fn same_paper_reprice(left: &PaperReprice, right: &PaperReprice) -> bool {
    left.commitment == right.commitment
        && left.prior_receipt == right.prior_receipt
        && left.asset == right.asset
        && left.prior_client_order_id == right.prior_client_order_id
        && left.replacement_client_order_id == right.replacement_client_order_id
        && left.prior_broker_order_id == right.prior_broker_order_id
        && left.replacement_limit_price == right.replacement_limit_price
}

fn enum_name<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .expect("enum serializes")
        .as_str()
        .expect("enum serializes as string")
        .to_owned()
}

fn status_counts(connection: &Connection, table: &str) -> StoreResult<BTreeMap<String, u64>> {
    let sql = format!("SELECT status, COUNT(*) FROM {table} GROUP BY status ORDER BY status");
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
    })?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(Into::into)
}

fn parse_enum<T: for<'de> serde::Deserialize<'de>>(value: &str) -> StoreResult<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(StoreError::Json)
}

fn parse_task_status(value: &str) -> StoreResult<TaskStatus> {
    match value {
        "queued" => Ok(TaskStatus::Pending),
        "running" => Ok(TaskStatus::Running),
        "succeeded" => Ok(TaskStatus::Succeeded),
        "failed" => Ok(TaskStatus::Failed),
        "cancelled" => Ok(TaskStatus::Cancelled),
        "skipped" => Ok(TaskStatus::Skipped),
        other => Err(StoreError::Integrity(format!(
            "invalid task status {other}"
        ))),
    }
}

fn is_trajectory_redacted_kind(kind: ArtifactKind) -> bool {
    matches!(
        kind,
        ArtifactKind::AgentTurn | ArtifactKind::ToolCall | ArtifactKind::ToolResult
    )
}

fn trajectory_output_refs(artifact: &Artifact) -> Vec<ArtifactRef> {
    let mut refs = vec![ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }];
    refs.extend(
        artifact
            .source_refs
            .iter()
            .filter(|reference| {
                reference.kind != ArtifactKind::RawEvidence
                    && !is_trajectory_redacted_kind(reference.kind)
            })
            .cloned(),
    );
    refs.sort();
    refs.dedup();
    refs
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
#[cfg(test)]
#[allow(clippy::items_after_test_module)]
fn retrospective_artifact(
    store: &V2Store,
    permit: &TaskWritePermit,
    outcome: &Artifact,
    now: DateTime<Utc>,
) -> Artifact {
    let outcome_ref = ArtifactRef {
        artifact_id: outcome.artifact_id.clone(),
        kind: ArtifactKind::Outcome,
    };
    let payload = akzio_domain::Retrospective {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id: akzio_domain::OutcomeId::new(),
        horizon: OutcomeHorizon::T5,
        status: akzio_domain::RetrospectiveStatus::Complete,
        summary: "fixture retrospective".to_owned(),
        findings: Vec::new(),
        counterfactuals: Vec::new(),
        lesson_candidates: Vec::new(),
        diagnostic_gaps: Vec::new(),
        source_refs: vec![outcome_ref.clone()],
        outcome: outcome_ref.clone(),
        created_at: now,
        sealed_at: Some(now),
    };
    Artifact::new(
        ArtifactKind::Retrospective,
        store
            .put_json(&payload)
            .expect("fixture retrospective payload"),
        "fixture.policy",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "fixture.policy".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        }),
        vec![outcome_ref],
        now,
    )
    .expect("fixture retrospective artifact")
}
