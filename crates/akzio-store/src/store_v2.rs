//! Store implementation for the source-incompatible Akzio v2 authority.
//!
//! `V2Store` deliberately uses a different database filename and metadata
//! marker from `V2Store`; callers must choose a new Store Root rather than run a
//! silent in-place migration.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use akzio_domain::{
    AgentContract, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, Asset, AttemptId, BlobRef, CandidatePolicy,
    CandidatePolicyState, ContentHash, ContractId, ContractPurpose, DomainError, Evaluation,
    ExecutionContext, ExecutionPlan, ExecutionVerdict, Experience, FailureDisposition, FreezeState,
    LeaseId, LifecycleEventType, OrderReceipt, OrderReceiptState, Outcome, OutcomeExecutionLineage,
    OutcomeHorizon, OutcomeSchedule, PaperCommitment, PaperReprice, PolicyState, PolicySubject,
    PolicyTransition, PolicyTransitionId, Reconciliation, RetryPolicy, RunId, RunPurpose,
    TaskBudget, TaskId, TaskRecipeId, TaskStatus, TaskWritePermit, WorkflowGraph, WorkflowNode,
    WorkflowProposal, WorkflowStatus, V2_DOMAIN_SCHEMA_VERSION, V2_SCHEMA_VERSION,
};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

const DATABASE_FILE: &str = "akzio.sqlite3";
const INCOMPATIBLE_DATABASE_FILE: &str = "control.sqlite3";
const POST_TERMINAL_WORKER_RECIPE_ID: &str = "learning.outcome_worker";

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
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone)]
pub struct V2Store {
    root: Arc<PathBuf>,
    blobs: Arc<PathBuf>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRun {
    pub run_id: RunId,
    pub purpose: RunPurpose,
    pub topology_id: String,
    pub graph_artifact_id: ArtifactId,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTask {
    pub run_id: RunId,
    pub node: WorkflowNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRevision {
    pub revision: u64,
    pub graph_artifact: Artifact,
    pub graph: WorkflowGraph,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredActiveAttempt {
    pub permit: TaskWritePermit,
    pub worker_id: String,
    pub lease_until: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTaskSnapshot {
    pub node: WorkflowNode,
    pub status: TaskStatus,
    pub ready_at: DateTime<Utc>,
    pub active_attempt: Option<StoredActiveAttempt>,
    pub attempt_count: u64,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub cursor: i64,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<akzio_domain::AttemptId>,
    pub event_type: String,
    pub artifact_id: Option<ArtifactId>,
    pub created_at: DateTime<Utc>,
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
    pub fn open(root: impl AsRef<Path>) -> StoreResult<Self> {
        let root = root.as_ref().to_path_buf();
        if root.join(INCOMPATIBLE_DATABASE_FILE).exists() && !root.join(DATABASE_FILE).exists() {
            return Err(StoreError::IncompatibleStoreRoot(root));
        }
        fs::create_dir_all(root.join("blobs")).map_err(|source| StoreError::Io {
            path: root.join("blobs"),
            source,
        })?;
        let database = root.join(DATABASE_FILE);
        let mut connection = Connection::open(&database)?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        initialize(&mut connection, &root)?;
        Ok(Self {
            blobs: Arc::new(root.join("blobs")),
            root: Arc::new(root),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    /// Create a consistent SQLite snapshot plus immutable CAS blobs.
    ///
    /// The target must be new and outside the active Store Root. The
    /// database snapshot is produced by SQLite's `VACUUM INTO`, so a backup
    /// never observes a partially committed transaction.
    pub fn backup_to(&self, target: impl AsRef<Path>) -> StoreResult<BackupManifest> {
        self.verify_integrity()?;
        let target = target.as_ref().to_path_buf();
        if target.starts_with(self.root()) {
            return Err(StoreError::BackupInsideStoreRoot(target));
        }
        if target.exists() {
            return Err(StoreError::BackupTargetExists(target));
        }
        fs::create_dir_all(target.join("blobs")).map_err(|source| StoreError::Io {
            path: target.join("blobs"),
            source,
        })?;

        let database = target.join(DATABASE_FILE);
        {
            let connection = self.connection.lock().expect("store connection poisoned");
            let database_sql = database.to_string_lossy().into_owned();
            connection.execute("VACUUM INTO ?1", [&database_sql])?;
        }
        let (blob_count, blob_bytes) = copy_blob_tree(self.blobs(), &target.join("blobs"))?;
        let database_bytes = fs::metadata(&database)
            .map_err(|source| StoreError::Io {
                path: database.clone(),
                source,
            })?
            .len();
        let database_content = fs::read(&database).map_err(|source| StoreError::Io {
            path: database.clone(),
            source,
        })?;
        let manifest = BackupManifest {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            database_hash: ContentHash::of_bytes(&database_content),
            database_bytes,
            blob_count,
            blob_bytes,
            created_at: Utc::now(),
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        fs::write(target.join("manifest.json"), manifest_bytes).map_err(|source| {
            StoreError::Io {
                path: target.join("manifest.json"),
                source,
            }
        })?;
        sync_file(&database)?;
        sync_file(&target.join("manifest.json"))?;
        Ok(manifest)
    }

    /// Restore a backup into a new Store Root and run Store Doctor before
    /// returning. Existing targets are rejected to avoid overwriting a
    /// user's durable state.
    pub fn restore_from(source: impl AsRef<Path>, target: impl AsRef<Path>) -> StoreResult<Self> {
        let source = source.as_ref().to_path_buf();
        let target = target.as_ref().to_path_buf();
        if target.exists() {
            return Err(StoreError::BackupTargetExists(target));
        }
        let manifest_path = source.join("manifest.json");
        let database = source.join(DATABASE_FILE);
        let blobs = source.join("blobs");
        if !manifest_path.is_file() || !database.is_file() || !blobs.is_dir() {
            return Err(StoreError::InvalidBackup(source));
        }
        let manifest: BackupManifest =
            serde_json::from_slice(&fs::read(&manifest_path).map_err(|source_error| {
                StoreError::Io {
                    path: manifest_path.clone(),
                    source: source_error,
                }
            })?)?;
        if manifest.schema_version != V2_DOMAIN_SCHEMA_VERSION {
            return Err(StoreError::InvalidBackup(source));
        }
        let database_bytes = fs::read(&database).map_err(|source_error| StoreError::Io {
            path: database.clone(),
            source: source_error,
        })?;
        if database_bytes.len() as u64 != manifest.database_bytes
            || ContentHash::of_bytes(&database_bytes) != manifest.database_hash
        {
            return Err(StoreError::InvalidBackup(source));
        }
        fs::create_dir_all(target.join("blobs")).map_err(|source_error| StoreError::Io {
            path: target.join("blobs"),
            source: source_error,
        })?;
        fs::copy(&database, target.join(DATABASE_FILE)).map_err(|source_error| StoreError::Io {
            path: target.join(DATABASE_FILE),
            source: source_error,
        })?;
        let copied = copy_blob_tree(&blobs, &target.join("blobs"))?;
        if copied != (manifest.blob_count, manifest.blob_bytes) {
            return Err(StoreError::InvalidBackup(source));
        }
        fs::copy(&manifest_path, target.join("manifest.json")).map_err(|source_error| {
            StoreError::Io {
                path: target.join("manifest.json"),
                source: source_error,
            }
        })?;
        let store = Self::open(&target)?;
        store.verify_integrity()?;
        Ok(store)
    }

    fn blobs(&self) -> &Path {
        self.blobs.as_ref()
    }

    pub fn put_bytes(&self, bytes: &[u8], media_type: impl Into<String>) -> StoreResult<BlobRef> {
        let media_type = media_type.into();
        if media_type.trim().is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "blob_ref.media_type",
            }));
        }
        let hash = ContentHash::of_bytes(bytes);
        let path = self.blob_path(&hash);
        if !path.exists() {
            let parent = path.parent().expect("content addressed blob has parent");
            fs::create_dir_all(parent).map_err(|source| StoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(bytes).map_err(|source| StoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                    file.sync_all().map_err(|source| StoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => return Err(StoreError::Io { path, source }),
            }
        }
        Ok(BlobRef {
            hash,
            media_type,
            bytes: bytes.len() as u64,
        })
    }

    pub fn put_json<T: Serialize>(&self, value: &T) -> StoreResult<BlobRef> {
        self.put_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    pub fn read_blob(&self, blob: &BlobRef) -> StoreResult<Vec<u8>> {
        let path = self.blob_path(&blob.hash);
        let bytes = fs::read(&path).map_err(|source| StoreError::Io {
            path: path.clone(),
            source,
        })?;
        if bytes.len() as u64 != blob.bytes || ContentHash::of_bytes(&bytes) != blob.hash {
            return Err(StoreError::MissingBlob(blob.hash.clone()));
        }
        Ok(bytes)
    }

    /// Writes a root artifact such as an installed Contract. Bootstrap is deliberately
    /// narrow: a task-origin artifact must use `write_task_artifact` instead.
    pub fn write_bootstrap_artifact(&self, artifact: &Artifact) -> StoreResult<()> {
        artifact.validate()?;
        if artifact.origin.is_some()
            || !matches!(
                artifact.kind,
                ArtifactKind::Contract | ArtifactKind::FreezeState
            )
        {
            return Err(StoreError::PermitOriginMismatch);
        }
        self.read_blob(&artifact.blob)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
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

    pub fn active_contract(
        &self,
        purpose: &ContractPurpose,
    ) -> StoreResult<Option<StoredContract>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let Some((contract_hash, _)) = contract_catalogue_head(&connection, purpose)? else {
            return Ok(None);
        };
        self.stored_contract_with_connection(&connection, &contract_hash)
    }

    /// Return an installed Contract, whether it is an active head or a bounded
    /// candidate awaiting Paper-backed promotion.
    pub fn contract_installation(
        &self,
        contract_hash: &ContentHash,
    ) -> StoreResult<Option<StoredContract>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        self.stored_contract_with_connection(&connection, contract_hash)
    }

    /// Install the first Rust-defined active Contract for a purpose. A later
    /// version must enter through `install_candidate_contract` and a canonical
    /// policy transition; this prevents a restart from silently replacing it.
    pub fn install_active_contract(
        &self,
        contract: &AgentContract,
        now: DateTime<Utc>,
    ) -> StoreResult<StoredContract> {
        contract.validate()?;
        let artifact = self.contract_artifact(contract, now)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) =
            self.stored_contract_with_connection(&transaction, &contract.contract_hash)?
        {
            if existing.contract != *contract || existing.activated_at.is_none() {
                return Err(StoreError::ContractActivationConflict(
                    contract.purpose.clone(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }
        assert_contract_identity_available(&transaction, contract)?;
        if contract_catalogue_head(&transaction, &contract.purpose)?.is_some() {
            return Err(StoreError::ContractActivationConflict(
                contract.purpose.clone(),
            ));
        }
        insert_artifact(&transaction, &artifact)?;
        insert_contract_installation(&transaction, contract, &artifact, None, now)?;
        let activation_id = append_contract_activation(
            &transaction,
            &contract.purpose,
            None,
            &contract.contract_hash,
            None,
            now,
        )?;
        set_contract_catalogue_head(
            &transaction,
            &contract.purpose,
            &contract.contract_hash,
            activation_id,
        )?;
        transaction.commit()?;
        drop(connection);
        self.contract_installation(&contract.contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(contract.contract_hash.clone()))
    }

    /// Persist a candidate relative to the current active Contract. This is an
    /// immutable install only: activation is coupled atomically to the
    /// candidate's canonical PolicyTransition in `record_policy_evaluation`.
    pub fn install_candidate_contract(
        &self,
        active_contract_hash: &ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> StoreResult<StoredContract> {
        candidate.validate()?;
        let artifact = self.contract_artifact(candidate, now)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = self
            .stored_contract_with_connection(&transaction, active_contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(active_contract_hash.clone()))?;
        if active.activated_at.is_none() || !candidate_is_bounded(&active.contract, candidate) {
            return Err(StoreError::ContractCapabilityExpansion {
                active: active_contract_hash.clone(),
                candidate: candidate.contract_hash.clone(),
            });
        }
        if let Some(existing) =
            self.stored_contract_with_connection(&transaction, &candidate.contract_hash)?
        {
            if existing.contract == *candidate
                && existing.baseline_contract_hash.as_ref() == Some(active_contract_hash)
                && existing.activated_at.is_none()
            {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::ContractActivationConflict(
                candidate.purpose.clone(),
            ));
        }
        assert_contract_identity_available(&transaction, candidate)?;
        insert_artifact(&transaction, &artifact)?;
        insert_contract_installation(
            &transaction,
            candidate,
            &artifact,
            Some(active_contract_hash),
            now,
        )?;
        transaction.commit()?;
        drop(connection);
        self.contract_installation(&candidate.contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(candidate.contract_hash.clone()))
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

    /// Commits the frozen workflow graph, Run row, nodes, dependencies, and creation
    /// event as one transaction. A process cannot observe a half-submitted graph.
    /// Atomically installs the approved Paper workflow, its proposal, its
    /// run-scoped inputs, and the broker session slot.
    pub fn reserve_paper_session_with_proposal(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
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
            let mut connection = self.connection.lock().expect("store connection poisoned");
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

    pub fn commit_workflow(&self, commit: &WorkflowCommit) -> StoreResult<()> {
        if commit.graph.kind != ArtifactKind::WorkflowGraph
            || commit.graph.artifact_id != commit.run.graph_artifact_id
        {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        commit.graph.validate()?;
        self.read_blob(&commit.graph.blob)?;
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&commit.graph.blob)?)?;
        graph.validate()?;
        if graph.nodes != commit.nodes || graph.topology_id != commit.run.topology_id {
            return Err(StoreError::WorkflowGraphMismatch);
        }

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::commit_workflow_transaction(&transaction, commit)?;
        transaction.commit()?;
        Ok(())
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

    /// Atomically elect one daemon scheduler. A successor always receives a
    /// higher epoch so stale leaders cannot mutate a Paper session slot.
    pub fn acquire_daemon_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<Option<DaemonLease>> {
        if lease_name.trim().is_empty() || owner_id.trim().is_empty() || expires_at <= now {
            return Err(StoreError::InvalidDaemonLease(lease_name.to_owned()));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1",
                params![lease_name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?, row.get::<_, String>(2)?)),
            )
            .optional()?;
        let lease = match current {
            None => {
                transaction.execute(
                    "INSERT INTO rebuild_daemon_leases (lease_name, owner_id, epoch, expires_at, heartbeat_at) VALUES (?1, ?2, 1, ?3, ?4)",
                    params![lease_name, owner_id, expires_at.to_rfc3339(), now.to_rfc3339()],
                )?;
                DaemonLease {
                    lease_name: lease_name.to_owned(),
                    owner_id: owner_id.to_owned(),
                    epoch: 1,
                    expires_at,
                }
            }
            Some((_, _, current_expires_at)) if parse_time(&current_expires_at)? > now => {
                transaction.commit()?;
                return Ok(None);
            }
            Some((_, epoch, _)) => {
                let epoch = epoch.saturating_add(1);
                transaction.execute(
                    "UPDATE rebuild_daemon_leases SET owner_id = ?1, epoch = ?2, expires_at = ?3, heartbeat_at = ?4 WHERE lease_name = ?5",
                    params![owner_id, epoch, expires_at.to_rfc3339(), now.to_rfc3339(), lease_name],
                )?;
                DaemonLease {
                    lease_name: lease_name.to_owned(),
                    owner_id: owner_id.to_owned(),
                    epoch,
                    expires_at,
                }
            }
        };
        transaction.commit()?;
        Ok(Some(lease))
    }

    pub fn heartbeat_daemon_lease(
        &self,
        lease: &DaemonLease,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<bool> {
        if expires_at <= now {
            return Err(StoreError::InvalidDaemonLease(lease.lease_name.clone()));
        }
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "UPDATE rebuild_daemon_leases SET expires_at = ?1, heartbeat_at = ?2 WHERE lease_name = ?3 AND owner_id = ?4 AND epoch = ?5 AND expires_at > ?2",
            params![expires_at.to_rfc3339(), now.to_rfc3339(), lease.lease_name, lease.owner_id, lease.epoch],
        )?;
        Ok(changed == 1)
    }

    pub fn release_daemon_lease(&self, lease: &DaemonLease) -> StoreResult<bool> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "DELETE FROM rebuild_daemon_leases WHERE lease_name = ?1 AND owner_id = ?2 AND epoch = ?3",
            params![lease.lease_name, lease.owner_id, lease.epoch],
        )?;
        Ok(changed == 1)
    }

    pub fn daemon_lease(&self, lease_name: &str) -> StoreResult<Option<DaemonLease>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        connection
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1",
                params![lease_name],
                |row| {
                    Ok(DaemonLease {
                        lease_name: lease_name.to_owned(),
                        owner_id: row.get(0)?,
                        epoch: row.get(1)?,
                        expires_at: parse_time(&row.get::<_, String>(2)?).map_err(|error| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Verify that the caller still owns the current, unexpired daemon epoch.
    /// Broker adapters call this immediately before external Paper I/O.
    pub fn validate_daemon_lease(
        &self,
        lease: &DaemonLease,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction()?;
        assert_daemon_lease(&transaction, lease, now)?;
        transaction.commit()?;
        Ok(())
    }

    /// Freeze the exact Paper graph before its Run is installed. A duplicate
    /// session returns the original graph and task IDs without recording the
    /// caller's replacement proposal.
    pub fn reserve_session_slot(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
    ) -> StoreResult<SessionSlotReservation> {
        if reservation.session_key.trim().is_empty()
            || reservation.workflow.run.purpose != RunPurpose::Paper
            || reservation.workflow.graph.kind != ArtifactKind::WorkflowGraph
            || reservation.workflow.graph.artifact_id != reservation.workflow.run.graph_artifact_id
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
        for artifact in &reservation.setup_artifacts {
            artifact.validate()?;
            if artifact.kind != ArtifactKind::EvidenceNeed
                || artifact.lifecycle != ArtifactLifecycle::RunScoped
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&reservation.workflow.run.run_id)
            {
                return Err(StoreError::InvalidSessionSlot(
                    reservation.session_key.clone(),
                ));
            }
            self.read_blob(&artifact.blob)?;
        }

        let newly_reserved = {
            let mut connection = self.connection.lock().expect("store connection poisoned");
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
                transaction.commit()?;
                true
            }
        };
        let slot = self
            .session_slot(&reservation.session_key)?
            .ok_or_else(|| StoreError::Integrity("session slot disappeared".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved,
        })
    }

    pub fn session_slot(&self, session_key: &str) -> StoreResult<Option<SessionSlot>> {
        let row = {
            let connection = self.connection.lock().expect("store connection poisoned");
            connection
                .query_row(
                    "SELECT run_id, topology_id, graph_artifact_id, run_created_at, scheduler_epoch, reserved_at, commitment_artifact_id, committed_at FROM rebuild_session_slots WHERE session_key = ?1",
                    params![session_key],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, u64>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, Option<String>>(6)?,
                            row.get::<_, Option<String>>(7)?,
                        ))
                    },
                )
                .optional()?
        };
        row.map(
            |(
                run_id,
                topology_id,
                graph_artifact_id,
                run_created_at,
                scheduler_epoch,
                reserved_at,
                commitment_artifact_id,
                committed_at,
            )| {
                let graph_artifact_id = ArtifactId(ContentHash::new(graph_artifact_id)?);
                let graph_artifact = self.artifact(&graph_artifact_id)?;
                if graph_artifact.kind != ArtifactKind::WorkflowGraph {
                    return Err(StoreError::InvalidSessionSlot(session_key.to_owned()));
                }
                let graph: WorkflowGraph =
                    serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
                graph.validate()?;
                if graph.topology_id != topology_id {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
                Ok(SessionSlot {
                    session_key: session_key.to_owned(),
                    workflow: WorkflowCommit {
                        run: StoredRun {
                            run_id: RunId(run_id),
                            purpose: RunPurpose::Paper,
                            topology_id,
                            graph_artifact_id,
                            created_at: parse_time(&run_created_at)?,
                        },
                        graph: graph_artifact,
                        nodes: graph.nodes,
                    },
                    scheduler_epoch,
                    reserved_at: parse_time(&reserved_at)?,
                    commitment_artifact_id: commitment_artifact_id
                        .map(ContentHash::new)
                        .transpose()?
                        .map(ArtifactId),
                    committed_at: committed_at.as_deref().map(parse_time).transpose()?,
                })
            },
        )
        .transpose()
    }

    /// Durably reserve the single broker-visible commitment for a Paper
    /// session and terminally completes the active task attempt in the same
    /// transaction. A crash therefore cannot leave a committed session slot
    /// paired with an active commitment task.
    /// Returns the frozen broker-session slot for one scheduler-owned Paper
    /// run. A run may never have more than one such slot.
    pub fn session_slot_for_run(&self, run_id: &RunId) -> StoreResult<Option<SessionSlot>> {
        let session_key = {
            let connection = self.connection.lock().expect("store connection poisoned");
            connection
                .query_row(
                    "SELECT session_key FROM rebuild_session_slots WHERE run_id = ?1",
                    params![run_id.0],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
        };
        Ok(session_key
            .as_deref()
            .map(|session_key| self.session_slot(session_key))
            .transpose()?
            .flatten())
    }

    pub fn commit_execution(
        &self,
        lease: &DaemonLease,
        commit: &ExecutionCommit,
    ) -> StoreResult<ExecutionCommitResult> {
        if commit.session_key.trim().is_empty()
            || commit.commitment.kind != ArtifactKind::ExecutionCommitment
        {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }
        commit.commitment.validate()?;
        let payload: PaperCommitment =
            serde_json::from_slice(&self.read_blob(&commit.commitment.blob)?)?;
        payload.validate()?;
        if payload.broker_session != commit.session_key
            || !commit
                .commitment
                .source_refs
                .iter()
                .any(|source| source == &payload.execution_context)
        {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, commit.committed_at)?;
        assert_permit(&transaction, &commit.permit)?;
        assert_paper_run(&transaction, &commit.permit.run_id)?;
        assert_origin_matches(commit.commitment.origin.as_ref(), &commit.permit)?;
        self.validate_execution_commitment_lineage(
            &transaction,
            &commit.commitment,
            &payload,
            &commit.permit.run_id,
            &commit.session_key,
        )?;
        let (_, on_failure) = task_retry_policy(&transaction, &commit.permit.task_id)?;
        let slot = transaction
            .query_row(
                "SELECT run_id, commitment_artifact_id FROM rebuild_session_slots WHERE session_key = ?1",
                params![commit.session_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        let Some((run_id, existing_commitment)) = slot else {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        };
        if run_id != commit.permit.run_id.0 {
            return Err(StoreError::InvalidSessionSlot(commit.session_key.clone()));
        }
        if let Some(existing_commitment) = existing_commitment {
            if existing_commitment == commit.commitment.artifact_id.0.as_str() {
                let existing_artifact = read_artifact(
                    &transaction,
                    &ArtifactId(ContentHash::new(existing_commitment)?),
                )?;
                let existing_payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&existing_artifact.blob)?)?;
                self.validate_execution_commitment_lineage(
                    &transaction,
                    &existing_artifact,
                    &existing_payload,
                    &commit.permit.run_id,
                    &commit.session_key,
                )?;
                let event_id = append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    LifecycleEventType::ArtifactCommitted,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                record_attempt_output(
                    &transaction,
                    &commit.permit,
                    &commit.commitment.artifact_id,
                    event_id,
                )?;
                append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    LifecycleEventType::ExecutionCommitmentRecovered,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                finish_permitted_task(
                    &transaction,
                    &commit.permit,
                    TaskStatus::Succeeded,
                    on_failure,
                    Some(&commit.commitment.artifact_id),
                    commit.committed_at,
                )?;
                transaction.commit()?;
                return Ok(ExecutionCommitResult {
                    commitment_artifact_id: commit.commitment.artifact_id.clone(),
                    newly_committed: false,
                });
            }
            let existing_artifact_id = ArtifactId(ContentHash::new(existing_commitment)?);
            let existing_artifact = read_artifact(&transaction, &existing_artifact_id)?;
            if existing_artifact.kind == ArtifactKind::ExecutionCommitment {
                let existing_payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&existing_artifact.blob)?)?;
                self.validate_execution_commitment_lineage(
                    &transaction,
                    &existing_artifact,
                    &existing_payload,
                    &commit.permit.run_id,
                    &commit.session_key,
                )?;
                if same_paper_commitment(&existing_payload, &payload) {
                    let event_id = append_event(
                        &transaction,
                        &commit.permit.run_id,
                        Some(&commit.permit.task_id),
                        Some(&commit.permit.attempt_id),
                        LifecycleEventType::ArtifactCommitted,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    record_attempt_output(
                        &transaction,
                        &commit.permit,
                        &existing_artifact_id,
                        event_id,
                    )?;
                    append_event(
                        &transaction,
                        &commit.permit.run_id,
                        Some(&commit.permit.task_id),
                        Some(&commit.permit.attempt_id),
                        LifecycleEventType::ExecutionCommitmentRecovered,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    finish_permitted_task(
                        &transaction,
                        &commit.permit,
                        TaskStatus::Succeeded,
                        on_failure,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    transaction.commit()?;
                    return Ok(ExecutionCommitResult {
                        commitment_artifact_id: existing_artifact_id,
                        newly_committed: false,
                    });
                }
            }
            return Err(StoreError::DuplicateExecutionCommitment(
                commit.session_key.clone(),
            ));
        }
        insert_artifact(&transaction, &commit.commitment)?;
        transaction.execute(
            "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3 AND commitment_artifact_id IS NULL",
            params![
                commit.commitment.artifact_id.0.as_str(),
                commit.committed_at.to_rfc3339(),
                commit.session_key,
            ],
        )?;
        let event_id = append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        record_attempt_output(
            &transaction,
            &commit.permit,
            &commit.commitment.artifact_id,
            event_id,
        )?;
        append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::ExecutionCommitted,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        finish_permitted_task(
            &transaction,
            &commit.permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&commit.commitment.artifact_id),
            commit.committed_at,
        )?;
        transaction.commit()?;
        Ok(ExecutionCommitResult {
            commitment_artifact_id: commit.commitment.artifact_id.clone(),
            newly_committed: true,
        })
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

    /// Return the one durable r0 -> r1 intent for an order in a committed
    /// Paper session. The table is only an immutable-history index; callers
    /// still consume the returned artifact and its provenance.
    pub fn reprice_for(
        &self,
        commitment: &ArtifactRef,
        asset: Asset,
    ) -> StoreResult<Option<Artifact>> {
        if commitment.kind != ArtifactKind::ExecutionCommitment {
            return Err(StoreError::InvalidExecutionReprice);
        }
        let connection = self.connection.lock().expect("store connection poisoned");
        let artifact_id = connection
            .query_row(
                "SELECT reprice_artifact_id FROM rebuild_execution_reprices \
                 WHERE commitment_artifact_id = ?1 AND asset = ?2",
                params![commitment.artifact_id.0.as_str(), asset.symbol()],
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

    pub fn metrics(&self, now: DateTime<Utc>) -> StoreResult<StoreMetrics> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let run_counts = status_counts(&connection, "rebuild_runs")?;
        let task_counts = status_counts(&connection, "rebuild_tasks")?;
        let attempt_counts = status_counts(&connection, "rebuild_attempts")?;
        let event_count =
            connection.query_row("SELECT COUNT(*) FROM rebuild_events", [], |row| {
                row.get::<_, u64>(0)
            })?;
        let active_daemon_leases = connection.query_row(
            "SELECT COUNT(*) FROM rebuild_daemon_leases WHERE expires_at > ?1",
            params![now.to_rfc3339()],
            |row| row.get::<_, u64>(0),
        )?;
        Ok(StoreMetrics {
            run_counts,
            task_counts,
            attempt_counts,
            event_count,
            active_daemon_leases,
        })
    }

    /// Atomically installs the single Rust-owned reprice intent for one
    /// commitment/asset lineage and terminally completes its task. The broker
    /// adapter may receive only the returned immutable intent afterwards.
    pub fn commit_reprice(
        &self,
        lease: &DaemonLease,
        commit: &RepriceCommit,
    ) -> StoreResult<RepriceCommitResult> {
        if commit.reprice.kind != ArtifactKind::ExecutionReprice {
            return Err(StoreError::InvalidExecutionReprice);
        }
        commit.reprice.validate()?;
        let payload: PaperReprice = serde_json::from_slice(&self.read_blob(&commit.reprice.blob)?)?;
        payload.validate()?;
        if !commit
            .reprice
            .source_refs
            .iter()
            .any(|source| source == &payload.commitment)
            || !commit
                .reprice
                .source_refs
                .iter()
                .any(|source| source == &payload.prior_receipt)
        {
            return Err(StoreError::InvalidExecutionReprice);
        }

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, commit.committed_at)?;
        assert_permit(&transaction, &commit.permit)?;
        assert_paper_run(&transaction, &commit.permit.run_id)?;
        assert_origin_matches(commit.reprice.origin.as_ref(), &commit.permit)?;
        let (_, on_failure) = task_retry_policy(&transaction, &commit.permit.task_id)?;

        let commitment_artifact = read_artifact(&transaction, &payload.commitment.artifact_id)?;
        if commitment_artifact.kind != ArtifactKind::ExecutionCommitment {
            return Err(StoreError::InvalidExecutionReprice);
        }
        let commitment: PaperCommitment =
            serde_json::from_slice(&self.read_blob(&commitment_artifact.blob)?)?;
        commitment.validate()?;
        let prior_receipt_artifact =
            read_artifact(&transaction, &payload.prior_receipt.artifact_id)?;
        if prior_receipt_artifact.kind != ArtifactKind::OrderReceipt
            || !prior_receipt_artifact
                .source_refs
                .iter()
                .any(|source| source == &payload.commitment)
        {
            return Err(StoreError::InvalidExecutionReprice);
        }
        let prior_receipt: OrderReceipt =
            serde_json::from_slice(&self.read_blob(&prior_receipt_artifact.blob)?)?;
        prior_receipt.validate()?;
        if prior_receipt.plan_hash != commitment.plan_hash
            || prior_receipt.asset != payload.asset
            || prior_receipt.client_order_id != payload.prior_client_order_id
            || prior_receipt.broker_order_id != payload.prior_broker_order_id
            || commitment.client_order_ids.get(&payload.asset)
                != Some(&payload.prior_client_order_id)
            || !matches!(
                prior_receipt.state,
                OrderReceiptState::Accepted | OrderReceiptState::PartiallyFilled
            )
        {
            return Err(StoreError::InvalidExecutionReprice);
        }

        let slot = transaction
            .query_row(
                "SELECT run_id, commitment_artifact_id FROM rebuild_session_slots WHERE session_key = ?1",
                params![commitment.broker_session],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        let Some((run_id, commitment_artifact_id)) = slot else {
            return Err(StoreError::InvalidSessionSlot(
                commitment.broker_session.clone(),
            ));
        };
        if run_id != commit.permit.run_id.0
            || commitment_artifact_id.as_deref() != Some(payload.commitment.artifact_id.0.as_str())
        {
            return Err(StoreError::InvalidSessionSlot(
                commitment.broker_session.clone(),
            ));
        }

        let existing = transaction
            .query_row(
                "SELECT reprice_artifact_id FROM rebuild_execution_reprices \
                 WHERE commitment_artifact_id = ?1 AND asset = ?2",
                params![
                    payload.commitment.artifact_id.0.as_str(),
                    payload.asset.symbol()
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            let existing_artifact_id = ArtifactId(ContentHash::new(existing)?);
            let existing_artifact = read_artifact(&transaction, &existing_artifact_id)?;
            if existing_artifact.kind == ArtifactKind::ExecutionReprice {
                let existing_payload: PaperReprice =
                    serde_json::from_slice(&self.read_blob(&existing_artifact.blob)?)?;
                if same_paper_reprice(&existing_payload, &payload) {
                    append_event(
                        &transaction,
                        &commit.permit.run_id,
                        Some(&commit.permit.task_id),
                        Some(&commit.permit.attempt_id),
                        LifecycleEventType::ExecutionRepriceRecovered,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    finish_permitted_task(
                        &transaction,
                        &commit.permit,
                        TaskStatus::Succeeded,
                        on_failure,
                        Some(&existing_artifact_id),
                        commit.committed_at,
                    )?;
                    transaction.commit()?;
                    return Ok(RepriceCommitResult {
                        reprice_artifact_id: existing_artifact_id,
                        newly_committed: false,
                    });
                }
            }
            return Err(StoreError::DuplicateExecutionReprice(format!(
                "{}:{}",
                payload.commitment.artifact_id,
                payload.asset.symbol()
            )));
        }

        insert_artifact(&transaction, &commit.reprice)?;
        transaction.execute(
            "INSERT INTO rebuild_execution_reprices \
             (commitment_artifact_id, asset, reprice_artifact_id, created_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                payload.commitment.artifact_id.0.as_str(),
                payload.asset.symbol(),
                commit.reprice.artifact_id.0.as_str(),
                commit.committed_at.to_rfc3339(),
            ],
        )?;
        append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::ExecutionRepriceCommitted,
            Some(&commit.reprice.artifact_id),
            commit.committed_at,
        )?;
        finish_permitted_task(
            &transaction,
            &commit.permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&commit.reprice.artifact_id),
            commit.committed_at,
        )?;
        transaction.commit()?;
        Ok(RepriceCommitResult {
            reprice_artifact_id: commit.reprice.artifact_id.clone(),
            newly_committed: true,
        })
    }

    /// Commit a Planner proposal and the graph revision it lowers. The proposal,
    /// graph, task rows, events, and Planner completion become visible together.
    pub fn commit_workflow_patch(&self, commit: &WorkflowPatchCommit) -> StoreResult<()> {
        let permit = &commit.permit;
        let planner_output = &commit.planner_output;
        let evidence_needs = &commit.evidence_needs;
        let proposal_artifact = &commit.proposal;
        let previous_graph_artifact_id = &commit.previous_graph_artifact_id;
        let next_graph = &commit.next_graph;
        let added_nodes = &commit.added_nodes;
        let updated_nodes = &commit.updated_nodes;
        let now = commit.completed_at;
        if planner_output.kind != ArtifactKind::WorkflowProposalDraft
            || proposal_artifact.kind != ArtifactKind::WorkflowProposal
            || evidence_needs
                .iter()
                .any(|artifact| artifact.kind != ArtifactKind::EvidenceNeed)
        {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        if next_graph.kind != ArtifactKind::WorkflowGraph {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        if planner_output.lifecycle != ArtifactLifecycle::RunScoped
            || evidence_needs
                .iter()
                .any(|artifact| artifact.lifecycle != ArtifactLifecycle::RunScoped)
            || proposal_artifact.lifecycle != ArtifactLifecycle::RunScoped
            || next_graph.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        planner_output.validate()?;
        proposal_artifact.validate()?;
        next_graph.validate()?;
        self.read_blob(&planner_output.blob)?;
        for evidence_need in evidence_needs {
            evidence_need.validate()?;
            self.read_blob(&evidence_need.blob)?;
        }
        self.read_blob(&proposal_artifact.blob)?;
        let proposal: WorkflowProposal =
            serde_json::from_slice(&self.read_blob(&proposal_artifact.blob)?)?;
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&next_graph.blob)?)?;
        graph.validate()?;
        if proposal.topology_id != graph.topology_id {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        let expected_proposal_sources = std::iter::once(ArtifactRef {
            artifact_id: planner_output.artifact_id.clone(),
            kind: ArtifactKind::WorkflowProposalDraft,
        })
        .chain(evidence_needs.iter().map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        }))
        .collect::<std::collections::BTreeSet<_>>();
        let proposal_sources = proposal_artifact
            .source_refs
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if planner_output.provenance.producer_contract_hash != permit.contract_hash
            || proposal_artifact.provenance.producer_contract_hash != permit.contract_hash
            || expected_proposal_sources.len() != evidence_needs.len() + 1
            || proposal_sources != expected_proposal_sources
            || next_graph.source_refs.len() != 2
            || !next_graph.source_refs.iter().any(|reference| {
                reference.artifact_id == *previous_graph_artifact_id
                    && reference.kind == ArtifactKind::WorkflowGraph
            })
            || !next_graph.source_refs.iter().any(|reference| {
                reference.artifact_id == proposal_artifact.artifact_id
                    && reference.kind == ArtifactKind::WorkflowProposal
            })
        {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        let added_ids = added_nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let updated_ids = updated_nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if added_ids.len() != added_nodes.len()
            || updated_ids.len() != updated_nodes.len()
            || !added_nodes
                .iter()
                .all(|node| graph.nodes.iter().any(|item| item == node))
            || !updated_nodes
                .iter()
                .all(|node| graph.nodes.iter().any(|item| item == node))
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        assert_origin_matches(planner_output.origin.as_ref(), permit)?;
        for evidence_need in evidence_needs {
            assert_origin_matches(evidence_need.origin.as_ref(), permit)?;
        }
        assert_origin_matches(proposal_artifact.origin.as_ref(), permit)?;
        let run_id = &permit.run_id;
        insert_artifact(&transaction, planner_output)?;
        append_event(
            &transaction,
            run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&planner_output.artifact_id),
            now,
        )?;
        for evidence_need in evidence_needs {
            insert_artifact(&transaction, evidence_need)?;
            append_event(
                &transaction,
                run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                LifecycleEventType::ArtifactCommitted,
                Some(&evidence_need.artifact_id),
                now,
            )?;
        }
        assert_workflow_input_artifacts(&transaction, &graph.nodes)?;
        let current = transaction
            .query_row(
                "SELECT graph_artifact_id, purpose FROM rebuild_runs WHERE run_id = ?1",
                params![run_id.0],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((current, purpose)) = current else {
            return Err(StoreError::MissingRun(run_id.clone()));
        };
        if parse_enum::<RunPurpose>(&purpose)? == RunPurpose::Paper {
            return Err(StoreError::FrozenPaperWorkflow(run_id.clone()));
        }
        if current != previous_graph_artifact_id.0.as_str() {
            return Err(StoreError::StaleWorkflowGraph);
        }
        let previous_graph_artifact = read_artifact(&transaction, previous_graph_artifact_id)?;
        if previous_graph_artifact.kind != ArtifactKind::WorkflowGraph {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        let previous_graph: WorkflowGraph =
            serde_json::from_slice(&self.read_blob(&previous_graph_artifact.blob)?)?;
        previous_graph.validate()?;
        let previous_nodes = previous_graph
            .nodes
            .iter()
            .map(|node| (node.task_id.clone(), node))
            .collect::<std::collections::BTreeMap<_, _>>();
        if added_ids.iter().any(|id| previous_nodes.contains_key(id))
            || updated_ids
                .iter()
                .any(|id| !previous_nodes.contains_key(id))
            || !added_ids.is_disjoint(&updated_ids)
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        for previous in &previous_graph.nodes {
            let Some(next) = graph
                .nodes
                .iter()
                .find(|node| node.task_id == previous.task_id)
            else {
                return Err(StoreError::WorkflowGraphMismatch);
            };
            if next != previous {
                if !updated_ids.contains(&previous.task_id) {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
                let mut permitted_update = previous.clone();
                permitted_update.dependencies = next.dependencies.clone();
                permitted_update.input_artifacts = next.input_artifacts.clone();
                if permitted_update != *next {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
            }
        }
        let existing_ids = transaction
            .prepare("SELECT task_id FROM rebuild_tasks WHERE run_id = ?1")?
            .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        let next_ids = graph
            .nodes
            .iter()
            .map(|node| node.task_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let expected = existing_ids
            .iter()
            .cloned()
            .chain(added_ids.iter().map(|id| id.0.clone()))
            .collect::<std::collections::BTreeSet<_>>();
        if next_ids != expected {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        insert_artifact(&transaction, proposal_artifact)?;
        let proposal_event_id = append_event(
            &transaction,
            run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&proposal_artifact.artifact_id),
            now,
        )?;
        record_attempt_output(
            &transaction,
            permit,
            &proposal_artifact.artifact_id,
            proposal_event_id,
        )?;
        insert_artifact(&transaction, next_graph)?;
        for node in added_nodes {
            insert_task_node(&transaction, run_id, node, now)?;
        }
        for node in added_nodes {
            insert_node_dependencies(&transaction, node)?;
        }
        for node in updated_nodes {
            let status = transaction.query_row(
                "SELECT status FROM rebuild_tasks WHERE task_id = ?1",
                params![node.task_id.0],
                |row| row.get::<_, String>(0),
            )?;
            if status != "queued" {
                return Err(StoreError::TaskNotRunnable(node.task_id.clone()));
            }
            transaction.execute(
                "UPDATE rebuild_tasks SET input_artifacts_json = ?1 WHERE task_id = ?2",
                params![
                    serde_json::to_string(&node.input_artifacts)?,
                    node.task_id.0
                ],
            )?;
            transaction.execute(
                "DELETE FROM rebuild_task_dependencies WHERE task_id = ?1",
                params![node.task_id.0],
            )?;
            for dependency in &node.dependencies {
                transaction.execute(
                    "INSERT INTO rebuild_task_dependencies (task_id, depends_on_task_id) VALUES (?1, ?2)",
                    params![node.task_id.0, dependency.0],
                )?;
            }
        }
        transaction.execute(
            "UPDATE rebuild_runs SET graph_artifact_id = ?1 WHERE run_id = ?2",
            params![next_graph.artifact_id.0.as_str(), run_id.0],
        )?;
        let revision = transaction.query_row(
            "SELECT COALESCE(MAX(revision), -1) + 1 FROM rebuild_workflow_revisions WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, i64>(0),
        )?;
        transaction.execute(
            r#"INSERT INTO rebuild_workflow_revisions
               (run_id, revision, graph_artifact_id, created_at)
               VALUES (?1, ?2, ?3, ?4)"#,
            params![
                run_id.0,
                revision,
                next_graph.artifact_id.0.as_str(),
                now.to_rfc3339(),
            ],
        )?;
        append_event(
            &transaction,
            run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::WorkflowPatched,
            Some(&next_graph.artifact_id),
            now,
        )?;
        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        finish_permitted_task(
            &transaction,
            permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&proposal_artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Request cancellation once. Queued tasks are durably cancelled in the
    /// same transaction; running attempts observe this request through
    /// [`Self::run_cancel_requested`] and finish through their permit.
    pub fn request_run_cancel(
        &self,
        run_id: &RunId,
        reason: &str,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        if reason.trim().is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "run_cancel.reason",
            }));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM rebuild_runs WHERE run_id = ?1",
                params![run_id.0],
                |_| Ok(()),
            )
            .optional()?;
        if exists.is_none() {
            return Err(StoreError::MissingRun(run_id.clone()));
        }
        let inserted = transaction.execute(
            r#"INSERT OR IGNORE INTO rebuild_run_cancellations (run_id, reason, requested_at)
               VALUES (?1, ?2, ?3)"#,
            params![run_id.0, reason, now.to_rfc3339()],
        )?;
        if inserted == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        append_event(
            &transaction,
            run_id,
            None,
            None,
            LifecycleEventType::RunCancelRequested,
            None,
            now,
        )?;
        cancel_queued_tasks(&transaction, run_id, now)?;
        refresh_run_status(&transaction, run_id, now)?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn run_cancel_requested(&self, run_id: &RunId) -> StoreResult<bool> {
        let connection = self.connection.lock().expect("store connection poisoned");
        Ok(connection
            .query_row(
                "SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1",
                params![run_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// Close the active attempt as retried or terminal. The policy and
    /// attempt count are read from the durable task record, so a handler
    /// cannot make itself retryable or extend its retry budget.
    pub fn retry_task(
        &self,
        permit: &TaskWritePermit,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> StoreResult<RetryTaskResult> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let (retry, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        let attempt_count = transaction.query_row(
            "SELECT COUNT(*) FROM rebuild_attempts WHERE task_id = ?1",
            params![permit.task_id.0],
            |row| row.get::<_, u64>(0),
        )?;
        if attempt_count < u64::from(retry.max_attempts) {
            transaction.execute(
                r#"UPDATE rebuild_tasks
                   SET status = 'queued', lease_id = NULL, active_attempt_id = NULL,
                       worker_id = NULL, lease_until = NULL, ready_at = ?1
                   WHERE task_id = ?2"#,
                params![retry_at.to_rfc3339(), permit.task_id.0],
            )?;
            transaction.execute(
                "UPDATE rebuild_attempts SET status = 'retried', finished_at = ?1 WHERE attempt_id = ?2",
                params![now.to_rfc3339(), permit.attempt_id.0],
            )?;
            append_event(
                &transaction,
                &permit.run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                LifecycleEventType::TaskRetryScheduled,
                None,
                now,
            )?;
            transaction.commit()?;
            return Ok(RetryTaskResult::Requeued);
        }

        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::TaskRetryExhausted,
            None,
            now,
        )?;
        let status = finish_permitted_task(
            &transaction,
            permit,
            TaskStatus::Failed,
            on_failure,
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(RetryTaskResult::Terminal(status))
    }

    pub fn claim_next_task(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> StoreResult<Option<ClaimedAttempt>> {
        if worker_id.trim().is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "worker_id",
            }));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let selected = transaction
            .query_row(
        r#"SELECT t.task_id, t.run_id, t.recipe_id, t.objective, t.contract_hash, t.priority,
        t.budget_json, t.retry_json, t.on_failure, t.parent_task_id, t.input_artifacts_json
                    FROM rebuild_tasks AS t
                    JOIN rebuild_runs AS r ON r.run_id = t.run_id
            WHERE t.status = 'queued' AND t.ready_at <= ?1 AND r.status IN ('queued', 'running')
              AND NOT EXISTS (
                  SELECT 1 FROM rebuild_run_cancellations AS c WHERE c.run_id = t.run_id
              )
              AND NOT EXISTS (
                        SELECT 1 FROM rebuild_task_dependencies AS d
                        JOIN rebuild_tasks AS p ON p.task_id = d.depends_on_task_id
                        WHERE d.task_id = t.task_id AND p.status NOT IN ('succeeded', 'skipped')
                      )
                    ORDER BY t.priority DESC, t.task_id ASC LIMIT 1"#,
                params![now.to_rfc3339()],
            row_to_node,
            )
            .optional()?;
        let Some((run_id, mut node)) = selected else {
            transaction.commit()?;
            return Ok(None);
        };
        node.dependencies = task_dependencies(&transaction, &node.task_id)?;
        let permit = TaskWritePermit {
            run_id: run_id.clone(),
            task_id: node.task_id.clone(),
            attempt_id: akzio_domain::AttemptId::new(),
            lease_id: akzio_domain::LeaseId::new(),
            epoch: transaction.query_row(
                "SELECT lease_epoch + 1 FROM rebuild_tasks WHERE task_id = ?1",
                params![node.task_id.0],
                |row| row.get(0),
            )?,
            contract_hash: node.contract_hash.clone(),
        };
        let updated = transaction.execute(
            r#"UPDATE rebuild_tasks
               SET status = 'running', lease_id = ?1, lease_epoch = ?2, active_attempt_id = ?3,
                   lease_until = ?4, worker_id = ?5
               WHERE task_id = ?6 AND status = 'queued'"#,
            params![
                permit.lease_id.0,
                permit.epoch,
                permit.attempt_id.0,
                (now + lease_for).to_rfc3339(),
                worker_id,
                permit.task_id.0,
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::TaskNotRunnable(permit.task_id));
        }
        transaction.execute(
            r#"INSERT INTO rebuild_attempts
               (attempt_id, task_id, run_id, lease_id, epoch, worker_id, status, started_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)"#,
            params![
                permit.attempt_id.0,
                permit.task_id.0,
                permit.run_id.0,
                permit.lease_id.0,
                permit.epoch,
                worker_id,
                now.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            "UPDATE rebuild_runs SET status = 'running' WHERE run_id = ?1 AND status = 'queued'",
            params![permit.run_id.0],
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::TaskStarted,
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(Some(ClaimedAttempt {
            run_id,
            node,
            permit,
        }))
    }

    pub fn heartbeat_task(
        &self,
        permit: &TaskWritePermit,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let updated = connection.execute(
            r#"UPDATE rebuild_tasks SET lease_until = ?1
               WHERE task_id = ?2 AND status = 'running' AND lease_id = ?3 AND lease_epoch = ?4
                 AND active_attempt_id = ?5"#,
            params![
                expires_at.to_rfc3339(),
                permit.task_id.0,
                permit.lease_id.0,
                permit.epoch,
                permit.attempt_id.0,
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        }
        Ok(())
    }

    /// Verifies that a handler still owns the active task attempt without
    /// creating an artifact or changing task state. External adapters use
    /// this immediately before side effects; final persistence rechecks the
    /// same permit in its own transaction.
    pub fn validate_task_permit(&self, permit: &TaskWritePermit) -> StoreResult<()> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        transaction.commit()?;
        Ok(())
    }

    /// Append a task-scoped lifecycle fact without creating an artifact.
    /// The permit check and event insert share one transaction so a stale
    /// attempt cannot publish an AgentTurnStarted fact after takeover.
    pub fn append_task_event(
        &self,
        permit: &TaskWritePermit,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        append_task_event(&transaction, permit, event_type, now)?;
        validate_agent_turn_lifecycle_events(&transaction, Some(&permit.run_id))?;
        transaction.commit()?;
        Ok(())
    }

    /// Verify a handler-owned transaction already closed this exact attempt.
    /// A merely stale permit is insufficient: task and attempt terminal state,
    /// run, lease, epoch, and contract must all still identify the caller.
    pub fn verify_attempt_terminal(
        &self,
        permit: &TaskWritePermit,
        status: TaskStatus,
    ) -> StoreResult<()> {
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let current = transaction
            .query_row(
                r#"SELECT t.run_id, t.status, t.active_attempt_id, t.contract_hash,
                          a.task_id, a.run_id, a.lease_id, a.epoch, a.status
                   FROM rebuild_attempts AS a
                   JOIN rebuild_tasks AS t ON t.task_id = a.task_id
                   WHERE a.attempt_id = ?1"#,
                params![permit.attempt_id.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, u64>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()?;
        let Some(current) = current else {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        };
        let expected_contract = permit.contract_hash.as_ref().map(ContentHash::as_str);
        if current.0 != permit.run_id.0
            || current.1 != enum_name(status)
            || current.2.is_some()
            || current.3.as_deref() != expected_contract
            || current.4 != permit.task_id.0
            || current.5 != permit.run_id.0
            || current.6 != permit.lease_id.0
            || current.7 != permit.epoch
            || current.8 != enum_name(status)
        {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        }
        validate_tool_lifecycle_events(&transaction, Some(&permit.run_id))?;
        if status == TaskStatus::Succeeded {
            ensure_no_pending_tool_calls(
                &transaction,
                &permit.run_id,
                &permit.task_id,
                &permit.attempt_id,
            )?;
        }
        Ok(())
    }

    pub fn write_task_artifact(
        &self,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.write_task_artifact_fenced(None, permit, artifact, event_type, now)
    }

    /// Persist a task artifact while optionally fencing a daemon-owned worker.
    /// The lease check is in the same transaction as the artifact/event write,
    /// so a takeover cannot leave a stale worker's output committed.
    pub fn write_task_artifact_fenced(
        &self,
        lease: Option<&DaemonLease>,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        artifact.validate()?;
        reject_generic_learning_artifact(artifact)?;
        self.read_blob(&artifact.blob)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(lease) = lease {
            assert_daemon_lease(&transaction, lease, Utc::now())?;
        }
        assert_permit(&transaction, permit)?;
        assert_task_artifact_lifecycle(&transaction, &permit.run_id, artifact)?;
        assert_origin_matches(artifact.origin.as_ref(), permit)?;
        insert_artifact(&transaction, artifact)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            event_type,
            Some(&artifact.artifact_id),
            now,
        )?;
        validate_tool_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_agent_turn_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_context_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_gate_lifecycle_events(&transaction, Some(&permit.run_id))?;
        transaction.commit()?;
        Ok(())
    }

    /// Commit the final artifacts and terminal task state together. A reader
    /// cannot observe a completed attempt without every committed output and
    /// its corresponding durable events.
    pub fn commit_attempt(
        &self,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, status)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        commit_attempt_transaction(&transaction, permit, artifacts, status, now)?;
        transaction.commit()?;
        Ok(())
    }

    /// Commits the terminal `OutcomeSchedule` and installs the scheduler-owned
    /// learning task in the same SQLite transaction. The learning task is not
    /// part of the frozen research graph; it is a post-terminal durable worker
    /// attached to the Paper run and cannot be created by a planner or agent.
    pub fn commit_outcome_schedule_with_worker(
        &self,
        permit: &TaskWritePermit,
        schedule: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        if schedule.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_schedule.worker_kind",
            ));
        }
        schedule.validate()?;
        self.read_blob(&schedule.blob)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let schedule_ref = ArtifactRef {
            artifact_id: schedule.artifact_id.clone(),
            kind: ArtifactKind::OutcomeSchedule,
        };
        self.validate_attempt_commit(
            permit,
            std::slice::from_ref(schedule),
            TaskStatus::Succeeded,
        )?;
        let existing_worker = transaction
            .query_row(
                r#"SELECT task_id FROM rebuild_tasks
                   WHERE run_id = ?1 AND recipe_id = ?2
                     AND input_artifacts_json = ?3"#,
                params![
                    permit.run_id.0,
                    POST_TERMINAL_WORKER_RECIPE_ID,
                    serde_json::to_string(std::slice::from_ref(&schedule_ref))?,
                ],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if existing_worker.is_some() {
            assert_idempotent_outcome_schedule_commit(&transaction, permit, schedule)?;
            transaction.commit()?;
            return Ok(());
        }
        let worker = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("learning.outcome_worker")?,
            contract_hash: schedule.provenance.producer_contract_hash.clone(),
            objective: "Seal governed T+1/T+3/T+5 Paper outcome and record evaluation.".to_owned(),
            dependencies: Vec::new(),
            input_artifacts: vec![schedule_ref],
            priority: 100,
            budget: TaskBudget {
                max_input_tokens: 1_024,
                max_output_tokens: 1_024,
                max_wall_time_secs: 120,
                max_tool_calls: 0,
            },
            retry: RetryPolicy {
                max_attempts: u8::MAX,
                initial_backoff_ms: 3_600_000,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        insert_task_node(&transaction, &permit.run_id, &worker, now)?;
        commit_attempt_transaction(
            &transaction,
            permit,
            std::slice::from_ref(schedule),
            TaskStatus::Succeeded,
            now,
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&worker.task_id),
            None,
            LifecycleEventType::OutcomeWorkerEnqueued,
            Some(&schedule.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically persist broker-visible task outputs only while both the
    /// daemon epoch and task attempt permit remain current.
    pub fn commit_fenced_attempt(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, status)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        commit_attempt_transaction(&transaction, permit, artifacts, status, now)?;
        transaction.commit()?;
        Ok(())
    }

    /// Record a broker effect intent before any Paper adapter I/O. The event
    /// is audit-only; it never grants broker authority or claims exactly-once.
    pub fn record_paper_effect_intent(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        effect: &ArtifactRef,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        self.validate_paper_effect_artifact(effect, &permit.run_id)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        assert_paper_effect_artifact(&transaction, effect, &permit.run_id)?;
        validate_paper_effect_events(&transaction, Some(&permit.run_id))?;
        if paper_effect_terminal_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::PaperEffectAlreadySettled(
                effect.artifact_id.clone(),
            ));
        }
        let already_recorded =
            paper_effect_intent_exists(&transaction, &permit.run_id, &effect.artifact_id)?;
        if already_recorded {
            transaction.commit()?;
            return Ok(true);
        }
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ExecutionEffectIntent,
            Some(&effect.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(false)
    }

    /// Commit Paper reconciliation artifacts and the effect settlement marker
    /// under the same daemon lease/attempt fence and SQLite transaction.
    pub fn commit_fenced_attempt_with_effect(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        effect: &ArtifactRef,
        recovered: bool,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, TaskStatus::Succeeded)?;
        self.validate_paper_effect_artifact(effect, &permit.run_id)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        assert_paper_effect_artifact(&transaction, effect, &permit.run_id)?;
        validate_paper_effect_events(&transaction, Some(&permit.run_id))?;
        if paper_effect_terminal_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::PaperEffectAlreadySettled(
                effect.artifact_id.clone(),
            ));
        }
        if !paper_effect_intent_exists(&transaction, &permit.run_id, &effect.artifact_id)? {
            return Err(StoreError::MissingPaperEffectIntent(
                effect.artifact_id.clone(),
            ));
        }
        commit_attempt_transaction_with_effect(
            &transaction,
            permit,
            artifacts,
            TaskStatus::Succeeded,
            Some((
                effect,
                if recovered {
                    LifecycleEventType::ExecutionEffectRecovered
                } else {
                    LifecycleEventType::ExecutionEffectSettled
                },
            )),
            now,
        )?;
        transaction.commit()?;
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
        }
        Ok(())
    }

    /// Commits sealed Paper or Shadow outcomes through a purpose-aware path.
    /// Generic task artifact APIs reject Outcome so learning lineage cannot be
    /// created without these checks.
    pub fn commit_outcomes(
        &self,
        permit: &TaskWritePermit,
        outcomes: &[Artifact],
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        if outcomes.is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "commit_outcomes.outcomes",
            }));
        }
        let payloads = outcomes
            .iter()
            .map(|artifact| {
                artifact.validate()?;
                self.read_blob(&artifact.blob)?;
                if artifact.kind != ArtifactKind::Outcome {
                    return Err(StoreError::InvalidLearningCommit("commit_outcomes.kind"));
                }
                let outcome: Outcome = self.read_artifact_payload(artifact)?;
                outcome.validate_sealed()?;
                Ok(outcome)
            })
            .collect::<StoreResult<Vec<_>>>()?;

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let purpose = run_purpose_from_connection(&transaction, &permit.run_id)?;
        let expected_lifecycle = match purpose {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            RunPurpose::Shadow => ArtifactLifecycle::RunScoped,
            _ => return Err(StoreError::NonCanonicalLearningPurpose(purpose)),
        };
        let allowed_schedule_purposes: &[RunPurpose] = match purpose {
            RunPurpose::Paper => &[RunPurpose::Paper],
            RunPurpose::Shadow => &[RunPurpose::Paper, RunPurpose::Shadow],
            _ => unreachable!("non-learning purpose rejected above"),
        };
        for (artifact, outcome) in outcomes.iter().zip(&payloads) {
            if artifact.lifecycle != expected_lifecycle {
                return Err(StoreError::InvalidLearningCommit(
                    "commit_outcomes.lifecycle",
                ));
            }
            let schedule_artifact = read_artifact(&transaction, &outcome.schedule.artifact_id)?;
            assert_artifact_from_allowed_purposes(&transaction, &schedule_artifact, &[purpose])?;
            self.read_outcome_schedule_with_connection(
                &transaction,
                outcome,
                allowed_schedule_purposes,
            )?;
            if !has_exact_source_refs(
                artifact,
                &std::iter::once(outcome.schedule.clone())
                    .chain(outcome.market_evidence.iter().cloned())
                    .collect::<Vec<_>>(),
            ) {
                return Err(StoreError::InvalidLearningCommit(
                    "commit_outcomes.source_refs",
                ));
            }
        }

        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        insert_artifact_batch(&transaction, outcomes)?;
        for artifact in outcomes {
            assert_origin_matches(artifact.origin.as_ref(), permit)?;
            let event_id = append_event(
                &transaction,
                &permit.run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                LifecycleEventType::ArtifactCommitted,
                Some(&artifact.artifact_id),
                now,
            )?;
            record_attempt_output(&transaction, permit, &artifact.artifact_id, event_id)?;
        }
        finish_permitted_task(
            &transaction,
            permit,
            TaskStatus::Succeeded,
            on_failure,
            outcomes.last().map(|artifact| &artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Records an immutable outcome-backed comparison. Completion is keyed by
    /// the compared decisions/context/candidate/horizon, never by wall-clock
    /// time, so a recovered attempt cannot create a second pair.
    pub fn complete_shadow_pair(
        &self,
        permit: &TaskWritePermit,
        completion: &ShadowPairCompletion,
    ) -> StoreResult<ShadowPairWriteResult> {
        completion.validate()?;
        let pair_key = completion.pair_key()?;
        let subject_id = completion.subject.subject_id();
        let subject_json = serde_json::to_string(&completion.subject)?;

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        assert_permit(&transaction, permit)?;
        assert_paper_run(&transaction, &permit.run_id)?;
        self.assert_shadow_pair_sources_with_connection(&transaction, completion)?;

        if let Some(existing) = read_shadow_pair(&transaction, &pair_key)? {
            if same_shadow_pair(&existing.completion, completion) {
                transaction.commit()?;
                return Ok(ShadowPairWriteResult::Existing(existing));
            }
            return Err(StoreError::ShadowPairConflict(pair_key.to_string()));
        }

        let completion_cursor = append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ShadowPairCompleted,
            Some(&completion.candidate_outcome.artifact_id),
            completion.completed_at,
        )?;
        transaction.execute(
            r#"INSERT INTO rebuild_shadow_pairs
            (pair_key, subject_id, subject_json, parent_decision_artifact_id, execution_context_artifact_id,
             candidate_decision_artifact_id, candidate_contract_hash, candidate_topology_id,
             horizon, parent_outcome_artifact_id, candidate_outcome_artifact_id, completed_at,
             pair_event_cursor)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
            params![
                pair_key.as_str(),
                subject_id,
                subject_json,
                completion.parent_decision.artifact_id.0.as_str(),
                completion.execution_context.artifact_id.0.as_str(),
                completion.candidate_decision.artifact_id.0.as_str(),
                completion.candidate_contract_hash.as_str(),
                completion.candidate_topology_id,
                enum_name(completion.horizon),
                completion.parent_outcome.artifact_id.0.as_str(),
                completion.candidate_outcome.artifact_id.0.as_str(),
                completion.completed_at.to_rfc3339(),
                completion_cursor,
            ],
        )?;
        transaction.commit()?;
        Ok(ShadowPairWriteResult::Inserted(StoredShadowPair {
            pair_key,
            completion: completion.clone(),
            completion_cursor,
        }))
    }

    /// Commits every canonical outcome-backed evaluation. A no-op still closes
    /// the subject's durable pair-consumption cursor, so one completed shadow
    /// pair cannot be used by more than one canonical evaluation.
    pub fn record_policy_evaluation(
        &self,
        commit: &PolicyEvaluationCommit,
    ) -> StoreResult<PolicyEvaluationResult> {
        self.record_policy_evaluation_fenced(None, commit)
    }

    /// Commit canonical learning while fencing an optional daemon worker in
    /// the same SQLite transaction as the policy/evaluation writes.
    pub fn record_policy_evaluation_fenced(
        &self,
        lease: Option<&DaemonLease>,
        commit: &PolicyEvaluationCommit,
    ) -> StoreResult<PolicyEvaluationResult> {
        commit.subject.validate()?;
        if !commit.subject.accepts_state(commit.from) || !commit.subject.accepts_state(commit.to) {
            return Err(StoreError::InvalidLearningCommit(
                "policy_evaluation.subject_state",
            ));
        }
        let subject_id = commit.subject.subject_id();
        let subject_json = serde_json::to_string(&commit.subject)?;

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(lease) = lease {
            assert_daemon_lease(&transaction, lease, Utc::now())?;
        }
        self.validate_policy_evaluation_commit_with_connection(&transaction, commit)?;

        if let Some(existing) =
            read_policy_evaluation(&transaction, &commit.evaluation.artifact_id)?
        {
            if !same_policy_evaluation(&existing, commit) {
                return Err(StoreError::PolicyEvaluationConflict(
                    commit.evaluation.artifact_id.to_string(),
                ));
            }
            if let Some(candidate_policy) = &commit.candidate_policy {
                let stored = read_artifact(&transaction, &candidate_policy.artifact_id)?;
                if stored != *candidate_policy {
                    return Err(StoreError::PolicyEvaluationConflict(
                        commit.evaluation.artifact_id.to_string(),
                    ));
                }
            }
            let consumption = read_policy_consumption_head(&transaction, &commit.subject)?
                .ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "policy evaluation {} has no consumption head",
                        commit.evaluation.artifact_id
                    ))
                })?;
            if consumption.consumed_pair_cursor < existing.consumed_pair_cursor {
                return Err(StoreError::Integrity(format!(
                    "policy evaluation {} consumption cursor regressed",
                    commit.evaluation.artifact_id
                )));
            }
            let policy_head = read_policy_head(&transaction, &commit.subject)?;
            transaction.commit()?;
            return Ok(PolicyEvaluationResult {
                policy_head,
                consumed_pair_cursor: existing.consumed_pair_cursor,
                evaluation_cursor: existing.event_cursor,
                newly_recorded: false,
            });
        }

        assert_permit(&transaction, &commit.permit)?;
        assert_paper_run(&transaction, &commit.permit.run_id)?;
        let previous = read_policy_head(&transaction, &commit.subject)?;
        match &previous {
            Some(head) if head.state != commit.from => {
                return Err(StoreError::PolicyHeadMismatch(subject_id));
            }
            None if commit.subject.initial_state() != commit.from => {
                return Err(StoreError::PolicyHeadMismatch(subject_id));
            }
            _ => {}
        }
        match &commit.transition {
            Some(transition) => {
                if commit.from == commit.to || !is_allowed_policy_transition(commit.from, commit.to)
                {
                    return Err(StoreError::InvalidLearningCommit("policy_transition.path"));
                }
                if read_policy_transition(&transaction, &transition.transition_id)?.is_some() {
                    return Err(StoreError::PolicyTransitionConflict(
                        transition.transition_id.to_string(),
                    ));
                }
            }
            None if commit.from != commit.to => {
                return Err(StoreError::InvalidLearningCommit(
                    "policy_evaluation.noop_state",
                ));
            }
            None => {}
        }
        validate_policy_shadow_pair_snapshot(&transaction, &commit.subject, commit.pair_snapshot)?;

        let (_, on_failure) = task_retry_policy(&transaction, &commit.permit.task_id)?;
        for artifact in [&commit.outcome, &commit.experience, &commit.evaluation]
            .into_iter()
            .chain(commit.candidate_policy.iter())
        {
            assert_origin_matches(artifact.origin.as_ref(), &commit.permit)?;
            insert_artifact(&transaction, artifact)?;
            let event_id = append_event(
                &transaction,
                &commit.permit.run_id,
                Some(&commit.permit.task_id),
                Some(&commit.permit.attempt_id),
                LifecycleEventType::ArtifactCommitted,
                Some(&artifact.artifact_id),
                commit.completed_at,
            )?;
            record_attempt_output(
                &transaction,
                &commit.permit,
                &artifact.artifact_id,
                event_id,
            )?;
        }

        let consumed_pair_cursor = commit.pair_snapshot.through_cursor;
        let evaluation_cursor = append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            LifecycleEventType::PolicyEvaluated,
            Some(&commit.evaluation.artifact_id),
            commit.completed_at,
        )?;

        let policy_head = if let Some(transition) = &commit.transition {
            let revision = previous
                .as_ref()
                .map_or(1, |head| head.revision.saturating_add(1));
            let transition_cursor = append_event(
                &transaction,
                &commit.permit.run_id,
                Some(&commit.permit.task_id),
                Some(&commit.permit.attempt_id),
                LifecycleEventType::PolicyTransitioned,
                Some(&commit.evaluation.artifact_id),
                commit.completed_at,
            )?;
            transaction.execute(
                r#"INSERT INTO rebuild_policy_transitions
                   (transition_id, subject_id, subject_json, from_state_json, to_state_json,
                    evaluation_artifact_id, run_id, revision, created_at, event_cursor)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
                params![
                    transition.transition_id.0,
                    subject_id,
                    subject_json,
                    serde_json::to_string(&commit.from)?,
                    serde_json::to_string(&commit.to)?,
                    commit.evaluation.artifact_id.0.as_str(),
                    commit.permit.run_id.0,
                    revision,
                    transition.created_at.to_rfc3339(),
                    transition_cursor,
                ],
            )?;
            match previous {
                Some(_) => {
                    transaction.execute(
                        "UPDATE rebuild_policy_heads SET subject_json = ?1, state_json = ?2, revision = ?3, transition_id = ?4, transition_event_cursor = ?5, updated_at = ?6 WHERE subject_id = ?7",
                        params![
                            serde_json::to_string(&commit.subject)?,
                            serde_json::to_string(&commit.to)?,
                            revision,
                            transition.transition_id.0,
                            transition_cursor,
                            transition.created_at.to_rfc3339(),
                            commit.subject.subject_id(),
                        ],
                    )?;
                }
                None => {
                    transaction.execute(
                        r#"INSERT INTO rebuild_policy_heads
                           (subject_id, subject_json, state_json, revision, transition_id,
                            transition_event_cursor, updated_at)
                           VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
                        params![
                            commit.subject.subject_id(),
                            serde_json::to_string(&commit.subject)?,
                            serde_json::to_string(&commit.to)?,
                            revision,
                            transition.transition_id.0,
                            transition_cursor,
                            transition.created_at.to_rfc3339(),
                        ],
                    )?;
                }
            }
            Some(PolicyHead {
                subject: commit.subject.clone(),
                state: commit.to,
                revision,
                transition_id: transition.transition_id.clone(),
                transition_cursor,
                updated_at: transition.created_at,
            })
        } else {
            previous
        };

        if let Some(transition) = &commit.transition {
            self.apply_contract_catalogue_transition(&transaction, commit, transition)?;
        }

        transaction.execute(
            r#"INSERT INTO rebuild_policy_evaluations
                (evaluation_artifact_id, subject_id, subject_json, outcome_artifact_id,
                 experience_artifact_id, candidate_policy_artifact_id, from_state_json,
                 to_state_json, transition_id, run_id, consumed_pair_cursor, event_cursor,
                 completed_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
            params![
                commit.evaluation.artifact_id.0.as_str(),
                commit.subject.subject_id(),
                serde_json::to_string(&commit.subject)?,
                commit.outcome.artifact_id.0.as_str(),
                commit.experience.artifact_id.0.as_str(),
                commit
                    .candidate_policy
                    .as_ref()
                    .map(|artifact| artifact.artifact_id.0.as_str()),
                serde_json::to_string(&commit.from)?,
                serde_json::to_string(&commit.to)?,
                commit
                    .transition
                    .as_ref()
                    .map(|transition| transition.transition_id.0.as_str()),
                commit.permit.run_id.0,
                consumed_pair_cursor,
                evaluation_cursor,
                commit.completed_at.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            r#"INSERT INTO rebuild_policy_consumption_heads
               (subject_id, subject_json, consumed_pair_cursor, evaluation_artifact_id,
                evaluation_event_cursor, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6)
               ON CONFLICT(subject_id) DO UPDATE SET
                   subject_json = excluded.subject_json,
                   consumed_pair_cursor = excluded.consumed_pair_cursor,
                   evaluation_artifact_id = excluded.evaluation_artifact_id,
                   evaluation_event_cursor = excluded.evaluation_event_cursor,
                   updated_at = excluded.updated_at"#,
            params![
                commit.subject.subject_id(),
                serde_json::to_string(&commit.subject)?,
                consumed_pair_cursor,
                commit.evaluation.artifact_id.0.as_str(),
                evaluation_cursor,
                commit.completed_at.to_rfc3339(),
            ],
        )?;
        finish_permitted_task(
            &transaction,
            &commit.permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&commit.evaluation.artifact_id),
            commit.completed_at,
        )?;
        transaction.commit()?;
        Ok(PolicyEvaluationResult {
            policy_head,
            consumed_pair_cursor,
            evaluation_cursor,
            newly_recorded: true,
        })
    }

    pub fn finish_task(
        &self,
        permit: &TaskWritePermit,
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        finish_permitted_task(&transaction, permit, status, on_failure, None, now)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn recover_expired_tasks(&self, now: DateTime<Utc>) -> StoreResult<u64> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let expired = {
            let mut statement = transaction.prepare(
                r#"SELECT task_id, run_id, active_attempt_id, lease_id, lease_epoch, contract_hash
                   FROM rebuild_tasks
                   WHERE status = 'running' AND lease_until < ?1
                   ORDER BY task_id"#,
            )?;
            let rows = statement
                .query_map(params![now.to_rfc3339()], |row| {
                    Ok((
                        TaskId(row.get::<_, String>(0)?),
                        RunId(row.get::<_, String>(1)?),
                        akzio_domain::AttemptId(row.get::<_, String>(2)?),
                        akzio_domain::LeaseId(row.get::<_, String>(3)?),
                        row.get::<_, u64>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        for (task_id, run_id, attempt_id, lease_id, epoch, contract_hash) in &expired {
            let permit = TaskWritePermit {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
                lease_id: lease_id.clone(),
                epoch: *epoch,
                contract_hash: contract_hash.as_deref().map(ContentHash::new).transpose()?,
            };
            let cancelled = transaction
                .query_row(
                    "SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1",
                    params![run_id.0],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            let (retry, on_failure) = task_retry_policy(&transaction, task_id)?;
            if cancelled {
                finish_permitted_task(
                    &transaction,
                    &permit,
                    TaskStatus::Cancelled,
                    on_failure,
                    None,
                    now,
                )?;
                continue;
            }
            let attempts = transaction.query_row(
                "SELECT COUNT(*) FROM rebuild_attempts WHERE task_id = ?1",
                params![task_id.0],
                |row| row.get::<_, u64>(0),
            )?;
            if attempts < u64::from(retry.max_attempts) {
                transaction.execute(
                    r#"UPDATE rebuild_tasks
                       SET status = 'queued', lease_id = NULL, active_attempt_id = NULL,
                           worker_id = NULL, lease_until = NULL, ready_at = ?1
                       WHERE task_id = ?2"#,
                    params![now.to_rfc3339(), task_id.0],
                )?;
                transaction.execute(
                    "UPDATE rebuild_attempts SET status = 'abandoned', finished_at = ?1 WHERE attempt_id = ?2",
                    params![now.to_rfc3339(), attempt_id.0],
                )?;
                append_event(
                    &transaction,
                    run_id,
                    Some(task_id),
                    Some(attempt_id),
                    LifecycleEventType::TaskRecovered,
                    None,
                    now,
                )?;
            } else {
                append_event(
                    &transaction,
                    run_id,
                    Some(task_id),
                    Some(attempt_id),
                    LifecycleEventType::TaskRecoveryExhausted,
                    None,
                    now,
                )?;
                finish_permitted_task(
                    &transaction,
                    &permit,
                    TaskStatus::Failed,
                    on_failure,
                    None,
                    now,
                )?;
            }
        }
        transaction.commit()?;
        Ok(expired.len() as u64)
    }

    pub fn artifact(&self, artifact_id: &ArtifactId) -> StoreResult<Artifact> {
        let connection = self.connection.lock().expect("store connection poisoned");
        read_artifact(&connection, artifact_id)
    }

    /// Returns final artifacts for the only succeeded attempt of an exact task
    /// in an exact run. Intermediate Agent/Tool artifacts are deliberately
    /// absent: only the atomic completion surface records attempt outputs.
    pub fn committed_task_outputs(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let attempt_id = connection
            .query_row(
                r#"SELECT a.attempt_id
                   FROM rebuild_tasks AS t
                   JOIN rebuild_attempts AS a ON a.task_id = t.task_id
                  WHERE t.run_id = ?1
                    AND t.task_id = ?2
                    AND t.status = 'succeeded'
                    AND a.status = 'succeeded'
                  ORDER BY a.finished_at DESC, a.attempt_id DESC
                  LIMIT 1"#,
                params![run_id.0, task_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::CommittedOutputTask {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
            })?;
        read_committed_attempt_outputs(&connection, Some(run_id), task_id, &AttemptId(attempt_id))
    }

    /// Returns final artifacts for one exact succeeded task attempt. This is
    /// intentionally stricter than an event-log query so callers cannot feed
    /// an AgentTurn, ToolCall, or failed-attempt artifact into another task.
    /// As [`Self::committed_task_outputs`], but permits an explicitly
    /// successful no-output gate. The task/attempt still had to reach durable
    /// `succeeded`; callers must never use this for arbitrary running work.
    pub fn succeeded_task_outputs_or_empty(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<Vec<Artifact>> {
        match self.committed_task_outputs(run_id, task_id) {
            Ok(artifacts) => Ok(artifacts),
            Err(StoreError::CommittedOutputAttempt { .. }) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    pub fn committed_attempt_outputs(
        &self,
        task_id: &TaskId,
        attempt_id: &AttemptId,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        read_committed_attempt_outputs(&connection, None, task_id, attempt_id)
    }

    /// Returns the latest succeeded attempt for the task, including only
    /// artifacts committed by that exact attempt. The query is intentionally
    /// task-level and attempt-level in one read so an older parent attempt
    /// cannot be projected after a later retry succeeds.
    pub fn current_succeeded_attempt(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<SucceededAttemptProof> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let current = transaction
            .query_row(
                r#"SELECT t.status, t.contract_hash, a.attempt_id, a.lease_id, a.epoch
                   FROM rebuild_tasks AS t
                   JOIN rebuild_attempts AS a ON a.task_id = t.task_id
                   WHERE t.run_id = ?1 AND t.task_id = ?2
                     AND t.status = 'succeeded' AND a.status = 'succeeded'
                   ORDER BY a.finished_at DESC, a.attempt_id DESC
                   LIMIT 1"#,
                params![run_id.0, task_id.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, u64>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::CommittedOutputTask {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
            })?;
        let attempt_id = AttemptId(current.2);
        let outputs =
            read_committed_attempt_outputs(&transaction, Some(run_id), task_id, &attempt_id)?;
        let context_manifest = transaction
            .query_row(
                r#"SELECT artifact_id
                   FROM rebuild_events
                   WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3
                     AND event_type IN ('context.manifest',
                                        'context.manifest_created',
                                        'context.child_manifest_created')
                     AND artifact_id IS NOT NULL
                   ORDER BY event_id DESC
                   LIMIT 1"#,
                params![run_id.0, task_id.0, attempt_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|artifact_id| {
                ContentHash::new(artifact_id).map(|artifact_id| ArtifactRef {
                    artifact_id: ArtifactId(artifact_id),
                    kind: ArtifactKind::ContextManifest,
                })
            })
            .transpose()?;
        let proof = SucceededAttemptProof {
            run_id: run_id.clone(),
            task_id: task_id.clone(),
            attempt_id,
            lease_id: LeaseId(current.3),
            epoch: current.4,
            contract_hash: current.1.map(ContentHash::new).transpose()?,
            context_manifest,
            outputs,
        };
        drop(transaction);
        Ok(proof)
    }

    pub fn artifacts_referencing(
        &self,
        source_artifact_id: &ArtifactId,
        kind: Option<ArtifactKind>,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection.lock().expect("store connection poisoned");
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
        let connection = self.connection.lock().expect("store connection poisoned");
        let artifact_id = connection
            .query_row(
                "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY created_at DESC, artifact_id DESC LIMIT 1",
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

    /// Returns the durable purpose recorded with a run. Learning uses this
    /// instead of accepting a caller-provided purpose flag.
    pub fn run_purpose(&self, run_id: &RunId) -> StoreResult<RunPurpose> {
        let connection = self.connection.lock().expect("store connection poisoned");
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

    pub fn workflow_revision(
        &self,
        run_id: &RunId,
        revision: u64,
    ) -> StoreResult<WorkflowRevision> {
        let connection = self.connection.lock().expect("store connection poisoned");
        self.workflow_revision_with_connection(&connection, run_id, revision)
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

    pub fn workflow_snapshot(&self, run_id: &RunId) -> StoreResult<WorkflowSnapshot> {
        let connection = self.connection.lock().expect("store connection poisoned");
        self.workflow_snapshot_with_connection(&connection, run_id)
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
            .map(|node| (node.task_id.clone(), node))
            .collect::<std::collections::BTreeMap<_, _>>();
        let stored_nodes = tasks
            .iter()
            .filter(|task| task.node.recipe_id.as_str() != POST_TERMINAL_WORKER_RECIPE_ID)
            .map(|task| (task.node.task_id.clone(), task.node.clone()))
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

    /// Reads the current policy head without exposing mutable storage to
    /// callers. Previous policy versions remain in `rebuild_policy_transitions`.
    pub fn policy_head(&self, subject: &PolicySubject) -> StoreResult<Option<PolicyHead>> {
        subject.validate()?;
        let connection = self.connection.lock().expect("store connection poisoned");
        read_policy_head(&connection, subject)
    }

    /// Captures one durable freshness window for all horizons. The returned
    /// cutoff is later committed verbatim; pairs completed after it remain
    /// fresh even if they arrive before evaluation persistence.
    pub fn policy_shadow_pair_snapshot(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<PolicyShadowPairSnapshot> {
        subject.validate()?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let after_cursor = read_policy_consumption_head(&transaction, subject)?
            .map_or(0, |head| head.consumed_pair_cursor);
        let through_cursor = max_shadow_pair_cursor(&transaction, subject)?;
        let counts_by_horizon =
            shadow_pair_counts_between(&transaction, subject, after_cursor, through_cursor)?;
        transaction.commit()?;
        Ok(PolicyShadowPairSnapshot {
            after_cursor,
            through_cursor,
            counts_by_horizon,
        })
    }

    /// Resolves only policy influences that were durably committed by a
    /// canonical evaluation. Arbitrary Experience/CandidatePolicy artifacts
    /// therefore cannot enter Context or Execution provenance.
    pub fn recorded_policy_influence_subject(
        &self,
        artifact_id: &ArtifactId,
    ) -> StoreResult<Option<PolicySubject>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let mut statement = connection.prepare(
            r#"SELECT subject_id, subject_json, 'experience'
               FROM rebuild_policy_evaluations WHERE experience_artifact_id = ?1
               UNION ALL
               SELECT subject_id, subject_json, 'candidate_policy'
               FROM rebuild_policy_evaluations WHERE candidate_policy_artifact_id = ?1"#,
        )?;
        let rows = statement
            .query_map(params![artifact_id.0.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        if rows.is_empty() {
            return Ok(None);
        }
        let artifact = read_artifact(&connection, artifact_id)?;
        let mut resolved = None;
        for (subject_id, subject_json, influence_kind) in rows {
            let expected_kind = match influence_kind.as_str() {
                "experience" => ArtifactKind::Experience,
                "candidate_policy" => ArtifactKind::CandidatePolicy,
                _ => unreachable!("query emits fixed influence kinds"),
            };
            if artifact.kind != expected_kind {
                return Err(StoreError::Integrity(format!(
                    "policy influence {artifact_id} has invalid kind"
                )));
            }
            let subject = parse_persisted_subject(&subject_id, &subject_json)?;
            if resolved.as_ref().is_some_and(|current| current != &subject) {
                return Err(StoreError::Integrity(format!(
                    "policy influence {artifact_id} has conflicting subjects"
                )));
            }
            resolved = Some(subject);
        }
        Ok(resolved)
    }

    /// Replays immutable policy transitions in revision order. Consumers use
    /// this for audit/replay; mutations remain limited to
    /// `record_policy_evaluation`.
    pub fn policy_transitions(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<Vec<PolicyTransitionRecord>> {
        subject.validate()?;
        let connection = self.connection.lock().expect("store connection poisoned");
        read_policy_transitions(&connection, subject)
    }

    pub fn events_after(
        &self,
        run_id: &RunId,
        after: i64,
        limit: usize,
    ) -> StoreResult<Vec<StoredEvent>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events WHERE run_id = ?1 AND event_id > ?2
               ORDER BY event_id ASC LIMIT ?3"#,
        )?;
        let rows = statement.query_map(params![run_id.0, after, limit as i64], |row| {
            Ok(StoredEvent {
                cursor: row.get(0)?,
                run_id: RunId(row.get(1)?),
                task_id: row.get::<_, Option<String>>(2)?.map(TaskId),
                attempt_id: row
                    .get::<_, Option<String>>(3)?
                    .map(akzio_domain::AttemptId),
                event_type: row.get(4)?,
                artifact_id: row
                    .get::<_, Option<String>>(5)?
                    .map(ContentHash::new)
                    .transpose()
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
                    .map(ArtifactId),
                created_at: parse_time(&row.get::<_, String>(6)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            })
        })?;
        let events = rows.collect::<Result<Vec<_>, _>>()?;
        for event in &events {
            let event_type = event.lifecycle_kind()?;
            validate_event_shape(
                event_type,
                event.task_id.is_some(),
                event.attempt_id.is_some(),
                event.artifact_id.is_some(),
            )?;
        }
        validate_tool_lifecycle_events(&connection, Some(run_id))?;
        validate_agent_turn_lifecycle_events(&connection, Some(run_id))?;
        validate_context_lifecycle_events(&connection, Some(run_id))?;
        validate_gate_lifecycle_events(&connection, Some(run_id))?;
        validate_paper_effect_events(&connection, Some(run_id))?;
        Ok(events)
    }

    pub fn verify_integrity(&self) -> StoreResult<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let mut event_statement = connection
            .prepare(
                "SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id FROM rebuild_events ORDER BY event_id ASC",
            )?;
        let event_rows = event_statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        for row in event_rows {
            let (cursor, run_id, task_id, attempt_id, event_type, artifact_id) = row?;
            let event_type = LifecycleEventType::parse(&event_type).map_err(|error| {
                StoreError::Integrity(format!(
                    "event {cursor} has invalid lifecycle type: {error}"
                ))
            })?;
            validate_event_shape(
                event_type,
                task_id.is_some(),
                attempt_id.is_some(),
                artifact_id.is_some(),
            )
            .map_err(|error| {
                StoreError::Integrity(format!(
                    "event {cursor} in run {run_id} has invalid shape: {error}"
                ))
            })?;
        }
        validate_tool_lifecycle_events(&connection, None)?;
        validate_agent_turn_lifecycle_events(&connection, None)?;
        validate_context_lifecycle_events(&connection, None)?;
        validate_gate_lifecycle_events(&connection, None)?;
        validate_paper_effect_events(&connection, None)?;
        let fk = connection
            .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
            .optional()?;
        if fk.is_some() {
            return Err(StoreError::Integrity("foreign key check failed".to_owned()));
        }
        let invalid_attempt_output = connection
            .query_row(
                r#"SELECT o.event_id
                     FROM rebuild_attempt_outputs AS o
                     JOIN rebuild_attempts AS a ON a.attempt_id = o.attempt_id
                     JOIN rebuild_tasks AS t ON t.task_id = o.task_id
                     JOIN rebuild_events AS e ON e.event_id = o.event_id
                    WHERE o.task_id != a.task_id
                       OR a.status != 'succeeded'
                       OR t.status != 'succeeded'
                       OR e.run_id != a.run_id
                       OR e.task_id != o.task_id
                       OR e.attempt_id != o.attempt_id
                       OR e.event_type != 'artifact.committed'
                       OR e.artifact_id != o.artifact_id
                    LIMIT 1"#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if invalid_attempt_output.is_some() {
            return Err(StoreError::Integrity(
                "attempt output has invalid terminal-event lineage".to_owned(),
            ));
        }
        let mut statement = connection.prepare(
            "SELECT artifact_id, blob_hash, media_type, bytes FROM rebuild_artifacts ORDER BY artifact_id",
        )?;
        let artifacts = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (artifact_id, hash, media_type, bytes) in artifacts {
            let artifact_id = ArtifactId(ContentHash::new(artifact_id)?);
            self.read_blob(&BlobRef {
                hash: ContentHash::new(hash)?,
                media_type,
                bytes,
            })?;
            let artifact = read_artifact(&connection, &artifact_id)?;
            artifact.validate()?;
        }
        let mut statement = connection.prepare(
            "SELECT lease_name, owner_id, epoch, expires_at, heartbeat_at FROM rebuild_daemon_leases ORDER BY lease_name",
        )?;
        let leases = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (lease_name, owner_id, epoch, expires_at, heartbeat_at) in leases {
            if lease_name.trim().is_empty() || owner_id.trim().is_empty() || epoch == 0 {
                return Err(StoreError::Integrity(format!(
                    "invalid daemon lease {lease_name}"
                )));
            }
            let expires_at = parse_time(&expires_at)?;
            if parse_time(&heartbeat_at)? > expires_at {
                return Err(StoreError::Integrity(format!(
                    "daemon lease {lease_name} heartbeat exceeds expiry"
                )));
            }
        }

        let mut statement = connection.prepare(
            "SELECT session_key, run_id, topology_id, graph_artifact_id, run_created_at, scheduler_epoch, reserved_at, commitment_artifact_id, committed_at FROM rebuild_session_slots ORDER BY session_key",
        )?;
        let slots = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, u64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (
            session_key,
            run_id,
            topology_id,
            graph_artifact_id,
            run_created_at,
            scheduler_epoch,
            reserved_at,
            commitment_artifact_id,
            committed_at,
        ) in slots
        {
            if session_key.trim().is_empty() || scheduler_epoch == 0 {
                return Err(StoreError::Integrity(format!(
                    "invalid session slot {session_key}"
                )));
            }
            let graph_artifact_id = ArtifactId(ContentHash::new(graph_artifact_id)?);
            let graph_artifact = read_artifact(&connection, &graph_artifact_id)?;
            if graph_artifact.kind != ArtifactKind::WorkflowGraph {
                return Err(StoreError::Integrity(format!(
                    "session slot {session_key} graph kind is invalid"
                )));
            }
            let graph: WorkflowGraph =
                serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
            graph.validate()?;
            if graph.topology_id != topology_id {
                return Err(StoreError::Integrity(format!(
                    "session slot {session_key} graph topology mismatch"
                )));
            }
            parse_time(&run_created_at)?;
            parse_time(&reserved_at)?;
            match (commitment_artifact_id, committed_at) {
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => {
                    return Err(StoreError::Integrity(format!(
                        "session slot {session_key} has incomplete commitment state"
                    )));
                }
                (Some(commitment_artifact_id), Some(committed_at)) => {
                    let commitment_artifact_id =
                        ArtifactId(ContentHash::new(commitment_artifact_id)?);
                    let commitment_artifact = read_artifact(&connection, &commitment_artifact_id)?;
                    if commitment_artifact.kind != ArtifactKind::ExecutionCommitment {
                        return Err(StoreError::Integrity(format!(
                            "session slot {session_key} commitment kind is invalid"
                        )));
                    }
                    let payload: PaperCommitment =
                        serde_json::from_slice(&self.read_blob(&commitment_artifact.blob)?)?;
                    payload.validate()?;
                    self.validate_execution_commitment_lineage(
                        &connection,
                        &commitment_artifact,
                        &payload,
                        &RunId(run_id.clone()),
                        &session_key,
                    )
                    .map_err(|error| {
                        StoreError::Integrity(format!(
                            "session slot {session_key} commitment lineage is invalid: {error}"
                        ))
                    })?;
                    parse_time(&committed_at)?;
                }
            }
        }

        let mut statement = connection.prepare(
            "SELECT commitment_artifact_id, asset, reprice_artifact_id, created_at \
             FROM rebuild_execution_reprices ORDER BY commitment_artifact_id, asset",
        )?;
        let reprices = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (commitment_artifact_id, asset, reprice_artifact_id, created_at) in reprices {
            let commitment_artifact_id = ArtifactId(ContentHash::new(commitment_artifact_id)?);
            let reprice_artifact_id = ArtifactId(ContentHash::new(reprice_artifact_id)?);
            let asset = Asset::try_from(asset.as_str())?;
            let commitment_artifact = read_artifact(&connection, &commitment_artifact_id)?;
            let reprice_artifact = read_artifact(&connection, &reprice_artifact_id)?;
            if commitment_artifact.kind != ArtifactKind::ExecutionCommitment
                || reprice_artifact.kind != ArtifactKind::ExecutionReprice
            {
                return Err(StoreError::Integrity(
                    "execution reprice artifact kind is invalid".to_owned(),
                ));
            }
            let commitment: PaperCommitment =
                serde_json::from_slice(&self.read_blob(&commitment_artifact.blob)?)?;
            let reprice: PaperReprice =
                serde_json::from_slice(&self.read_blob(&reprice_artifact.blob)?)?;
            commitment.validate()?;
            reprice.validate()?;
            if reprice.commitment.artifact_id != commitment_artifact_id
                || reprice.asset != asset
                || !reprice_artifact
                    .source_refs
                    .iter()
                    .any(|source| source == &reprice.commitment)
                || !reprice_artifact
                    .source_refs
                    .iter()
                    .any(|source| source == &reprice.prior_receipt)
            {
                return Err(StoreError::Integrity(
                    "execution reprice provenance is invalid".to_owned(),
                ));
            }
            let prior_artifact = read_artifact(&connection, &reprice.prior_receipt.artifact_id)?;
            if prior_artifact.kind != ArtifactKind::OrderReceipt
                || !prior_artifact
                    .source_refs
                    .iter()
                    .any(|source| source == &reprice.commitment)
            {
                return Err(StoreError::Integrity(
                    "execution reprice prior receipt is invalid".to_owned(),
                ));
            }
            let prior: OrderReceipt =
                serde_json::from_slice(&self.read_blob(&prior_artifact.blob)?)?;
            if prior.plan_hash != commitment.plan_hash
                || prior.asset != reprice.asset
                || prior.client_order_id != reprice.prior_client_order_id
                || prior.broker_order_id != reprice.prior_broker_order_id
                || commitment.client_order_ids.get(&reprice.asset)
                    != Some(&reprice.prior_client_order_id)
            {
                return Err(StoreError::Integrity(
                    "execution reprice receipt lineage is invalid".to_owned(),
                ));
            }
            let durable = connection
                .query_row(
                    "SELECT 1 FROM rebuild_session_slots \
                     WHERE commitment_artifact_id = ?1",
                    params![commitment_artifact_id.0.as_str()],
                    |_| Ok(()),
                )
                .optional()?;
            if durable.is_none() {
                return Err(StoreError::Integrity(
                    "execution reprice commitment is not durable".to_owned(),
                ));
            }
            parse_time(&created_at)?;
        }

        let mut statement = connection.prepare(
            "SELECT subject_id, state_json, revision, transition_id, \
                    transition_event_cursor, updated_at \
             FROM rebuild_policy_heads ORDER BY subject_id",
        )?;
        let heads = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (subject_id, state_json, revision, transition_id, transition_cursor, updated_at) in
            heads
        {
            if subject_id.trim().is_empty() || revision == 0 {
                return Err(StoreError::Integrity(format!(
                    "policy head {subject_id} is invalid"
                )));
            }
            let state: PolicyState = serde_json::from_str(&state_json)?;
            let transition =
                read_policy_transition(&connection, &PolicyTransitionId(transition_id.clone()))?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                    "policy head {subject_id} references missing transition {transition_id}"
                ))
                    })?;
            if transition.transition.subject.subject_id() != subject_id
                || transition.revision != revision
                || transition.transition.to != state
                || transition.transition_cursor != transition_cursor
                || transition.transition.created_at != parse_time(&updated_at)?
            {
                return Err(StoreError::Integrity(format!(
                    "policy head {subject_id} disagrees with its transition"
                )));
            }
            let latest = connection.query_row(
                "SELECT transition_id, revision FROM rebuild_policy_transitions \
                 WHERE subject_id = ?1 ORDER BY revision DESC LIMIT 1",
                params![subject_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )?;
            if latest != (transition_id.clone(), revision) {
                return Err(StoreError::Integrity(format!(
                    "policy head {subject_id} is stale"
                )));
            }
            let evaluation =
                read_artifact(&connection, &transition.transition.evaluation.artifact_id)?;
            if evaluation.kind != ArtifactKind::Evaluation
                || artifact_run_purpose(&connection, &evaluation)? != RunPurpose::Paper
            {
                return Err(StoreError::Integrity(format!(
                    "policy transition {transition_id} is not Paper-backed"
                )));
            }
        }
        let orphan_transition = connection
            .query_row(
                r#"SELECT t.transition_id FROM rebuild_policy_transitions AS t
                   LEFT JOIN rebuild_policy_heads AS h ON h.subject_id = t.subject_id
                   WHERE h.subject_id IS NULL LIMIT 1"#,
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(transition_id) = orphan_transition {
            return Err(StoreError::Integrity(format!(
                "policy transition {transition_id} has no head"
            )));
        }

        let mut statement =
            connection.prepare("SELECT pair_key FROM rebuild_shadow_pairs ORDER BY pair_key")?;
        let pair_keys = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for value in pair_keys {
            let pair_key = ContentHash::new(value)?;
            let pair = read_shadow_pair(&connection, &pair_key)?.ok_or_else(|| {
                StoreError::Integrity(format!("shadow pair {pair_key} disappeared"))
            })?;
            pair.completion.validate()?;
            if pair.completion.pair_key()? != pair_key {
                return Err(StoreError::Integrity(format!(
                    "shadow pair {pair_key} key mismatch"
                )));
            }
            self.assert_shadow_pair_sources_with_connection(&connection, &pair.completion)
                .map_err(|error| {
                    StoreError::Integrity(format!(
                        "shadow pair {pair_key} lineage is invalid: {error}"
                    ))
                })?;
        }

        let orphan = connection
            .query_row(
                r#"SELECT t.task_id FROM rebuild_tasks AS t
                    LEFT JOIN rebuild_runs AS r ON r.run_id = t.run_id
                    WHERE r.run_id IS NULL LIMIT 1"#,
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(task_id) = orphan {
            return Err(StoreError::Integrity(format!("task {task_id} has no run")));
        }
        let run_ids = connection
            .prepare("SELECT run_id FROM rebuild_runs ORDER BY run_id")?
            .query_map([], |row| Ok(RunId(row.get(0)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        for run_id in run_ids {
            let snapshot = self.workflow_snapshot_with_connection(&connection, &run_id)?;
            self.verify_workflow_history(&connection, &snapshot)?;
        }
        self.verify_outcome_schedule_history(&connection)?;
        self.verify_contract_catalogue_history(&connection)?;
        self.verify_policy_evaluation_history(&connection)?;
        self.verify_candidate_policy_history(&connection)?;
        Ok(())
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
            ],
        ) || !has_exact_source_refs(
            &commit.evaluation,
            &[evaluation.outcome.clone(), evaluation.experience.clone()],
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

    fn blob_path(&self, hash: &ContentHash) -> PathBuf {
        self.blobs.join(&hash.as_str()[..2]).join(hash.as_str())
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

fn copy_blob_tree(source: &Path, target: &Path) -> StoreResult<(u64, u64)> {
    let mut blob_count = 0_u64;
    let mut blob_bytes = 0_u64;
    let entries = fs::read_dir(source).map_err(|source_error| StoreError::Io {
        path: source.to_path_buf(),
        source: source_error,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source_error| StoreError::Io {
            path: source.to_path_buf(),
            source: source_error,
        })?;
        let source_path = entry.path();
        if !source_path.is_dir() {
            continue;
        }
        let shard = entry.file_name();
        let target_shard = target.join(shard);
        fs::create_dir_all(&target_shard).map_err(|source_error| StoreError::Io {
            path: target_shard.clone(),
            source: source_error,
        })?;
        for blob in fs::read_dir(&source_path).map_err(|source_error| StoreError::Io {
            path: source_path.clone(),
            source: source_error,
        })? {
            let blob = blob.map_err(|source_error| StoreError::Io {
                path: source_path.clone(),
                source: source_error,
            })?;
            let blob_path = blob.path();
            if !blob_path.is_file() {
                continue;
            }
            let bytes = fs::metadata(&blob_path)
                .map_err(|source_error| StoreError::Io {
                    path: blob_path.clone(),
                    source: source_error,
                })?
                .len();
            let target_blob = target_shard.join(blob.file_name());
            fs::copy(&blob_path, &target_blob).map_err(|source_error| StoreError::Io {
                path: target_blob,
                source: source_error,
            })?;
            blob_count = blob_count
                .checked_add(1)
                .ok_or_else(|| StoreError::Integrity("backup blob count overflow".to_owned()))?;
            blob_bytes = blob_bytes
                .checked_add(bytes)
                .ok_or_else(|| StoreError::Integrity("backup blob bytes overflow".to_owned()))?;
        }
    }
    Ok((blob_count, blob_bytes))
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
        if value != V2_SCHEMA_VERSION.to_string() {
            return Err(StoreError::IncompatibleStoreRoot(PathBuf::from(
                DATABASE_FILE,
            )));
        }
    }
    connection.execute_batch(
        "BEGIN;
        CREATE TABLE IF NOT EXISTS rebuild_artifacts (
           artifact_id TEXT PRIMARY KEY,
           kind TEXT NOT NULL,
           blob_hash TEXT NOT NULL,
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
            params![V2_SCHEMA_VERSION.to_string()],
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
    Ok(())
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
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                RunId(row.get::<_, String>(1)?),
                row.get::<_, Option<String>>(2)?.map(TaskId),
                row.get::<_, Option<String>>(3)?
                    .map(akzio_domain::AttemptId),
                LifecycleEventType::parse(&row.get::<_, String>(4)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
                row.get::<_, Option<String>>(5)?
                    .map(|value| {
                        ContentHash::new(value).map(ArtifactId).map_err(|error| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })
                    })
                    .transpose()?,
            ))
        },
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
        let (cursor, event_run_id, task_id, attempt_id, event_type, artifact_id) = row?;
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
             AND event_type IN (?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::AgentTurnStarted.as_str(),
            LifecycleEventType::AgentTurnCompleted.as_str(),
            LifecycleEventType::AgentTurnRetryableFailed.as_str(),
            LifecycleEventType::AgentTurnFailed.as_str(),
            LifecycleEventType::TaskRetryScheduled.as_str(),
            LifecycleEventType::TaskRetryExhausted.as_str(),
            LifecycleEventType::TaskRecovered.as_str(),
            LifecycleEventType::TaskRecoveryExhausted.as_str(),
            LifecycleEventType::TaskCancelled.as_str(),
            LifecycleEventType::TaskSucceeded.as_str(),
            LifecycleEventType::TaskFailed.as_str(),
            LifecycleEventType::TaskSkipped.as_str(),
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                RunId(row.get::<_, String>(1)?),
                row.get::<_, Option<String>>(2)?.map(TaskId),
                row.get::<_, Option<String>>(3)?
                    .map(akzio_domain::AttemptId),
                LifecycleEventType::parse(&row.get::<_, String>(4)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
                row.get::<_, Option<String>>(5)?
                    .map(|value| {
                        ContentHash::new(value).map(ArtifactId).map_err(|error| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })
                    })
                    .transpose()?,
            ))
        },
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
        let (cursor, event_run_id, task_id, attempt_id, event_type, artifact_id) = row?;
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
            LifecycleEventType::TaskRetryScheduled
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
             AND event_type IN (?2, ?3, ?4, ?5)
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::ContextManifest.as_str(),
            LifecycleEventType::ContextManifestCreated.as_str(),
            LifecycleEventType::ContextChildManifestCreated.as_str(),
            LifecycleEventType::ContextRepaired.as_str(),
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                RunId(row.get::<_, String>(1)?),
                row.get::<_, Option<String>>(2)?.map(TaskId),
                row.get::<_, Option<String>>(3)?
                    .map(akzio_domain::AttemptId),
                LifecycleEventType::parse(&row.get::<_, String>(4)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
                row.get::<_, Option<String>>(5)?
                    .map(|value| {
                        ContentHash::new(value).map(ArtifactId).map_err(|error| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })
                    })
                    .transpose()?,
            ))
        },
    )?;
    let mut seen = BTreeSet::<ArtifactId>::new();

    for row in rows {
        let (cursor, event_run_id, task_id, attempt_id, event_type, artifact_id) = row?;
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
                 ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12
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
            LifecycleEventType::ExecutionContextCreatedLegacy.as_str(),
            LifecycleEventType::ExecutionPlanCreated.as_str(),
            LifecycleEventType::ExecutionRepriceCommitted.as_str(),
            LifecycleEventType::ExecutionRepriceRecovered.as_str(),
            LifecycleEventType::ExecutionVerdictCreated.as_str(),
            LifecycleEventType::ExecutionVerdictNoOrder.as_str(),
            LifecycleEventType::ExecutionVerdictCreatedLegacy.as_str(),
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                RunId(row.get::<_, String>(1)?),
                row.get::<_, Option<String>>(2)?.map(TaskId),
                row.get::<_, Option<String>>(3)?
                    .map(akzio_domain::AttemptId),
                LifecycleEventType::parse(&row.get::<_, String>(4)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
                row.get::<_, Option<String>>(5)?
                    .map(|value| {
                        ContentHash::new(value).map(ArtifactId).map_err(|error| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })
                    })
                    .transpose()?,
            ))
        },
    )?;

    for row in rows {
        let (cursor, event_run_id, task_id, attempt_id, event_type, artifact_id) = row?;
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
            LifecycleEventType::ExecutionContextCreated
            | LifecycleEventType::ExecutionContextCreatedLegacy => ArtifactKind::ExecutionContext,
            LifecycleEventType::ExecutionVerdictCreated
            | LifecycleEventType::ExecutionVerdictNoOrder
            | LifecycleEventType::ExecutionVerdictCreatedLegacy => ArtifactKind::ExecutionVerdict,
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
    if status == TaskStatus::Failed {
        match on_failure {
            FailureDisposition::FailRun => cancel_queued_tasks(transaction, &permit.run_id, now)?,
            FailureDisposition::FailTask => {
                cancel_failed_dependents(transaction, &permit.run_id, now)?
            }
            FailureDisposition::SkipTask => unreachable!("failed status is converted to skipped"),
        }
    }
    refresh_run_status(transaction, &permit.run_id, now)?;
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

#[cfg(test)]
mod tests {
    use akzio_domain::{
        ArtifactLifecycle, ArtifactProvenance, Asset, ContextPolicy, ExecutionPlan, FactorExposure,
        FailureDisposition, HardBlocker, MoneyMicros, NoOrder, OrderIntent, OrderSide,
        OutputContract, PaperCommitment, PaperCommitmentId, PromptBundle, RetryPolicy,
        TargetPortfolio, TaskBudget, TaskRecipeId, TerminationPolicy, ToolGrant, ToolKind,
        ToolSpec, WeightPpm, WorkflowProposalTask,
    };
    use tempfile::tempdir;

    use super::*;

    fn budget() -> TaskBudget {
        TaskBudget {
            max_input_tokens: 32,
            max_output_tokens: 16,
            max_wall_time_secs: 10,
            max_tool_calls: 1,
        }
    }

    fn retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        }
    }

    fn contract(store: &V2Store, version: u32) -> AgentContract {
        AgentContract::new(
            ContractId::new(),
        version,
        ContractPurpose::new("research.fixture").unwrap(),
        "fixture contract",
        PromptBundle {
            version: 1,
            governance: store.put_bytes(b"fixture governance", "text/plain").unwrap(),
            role: store.put_bytes(b"fixture prompt", "text/plain").unwrap(),
        },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["fixture".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 4096,
                max_tokens: 1024,
                allow_raw_reread: false,
            },
        vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["fixture".to_owned()],
        }],
        vec![ToolSpec {
            name: "read_artifact".to_owned(),
            description: "read fixture artifact".to_owned(),
            kind: ToolKind::ReadEvidence,
            input_schema: store.put_bytes(b"fixture tool schema", "application/json").unwrap(),
            strict: true,
        }],
        OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: store
                    .put_bytes(
                        br#"{"type":"object","properties":{"summary":{"type":"string"}},"required":["summary"],"additionalProperties":false}"#,
                        "application/json",
                    )
                    .unwrap(),
            },
            budget(),
            retry(),
            TerminationPolicy::leaf(),
            FailureDisposition::FailRun,
        )
        .unwrap()
    }

    #[test]
    fn contract_catalogue_rejects_duplicate_or_expanded_installations_and_doctor_corruption() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let active = contract(&store, 1);
        store.install_active_contract(&active, now).unwrap();

        let mut duplicate = active.clone();
        duplicate.responsibility = "same identity, different contract".to_owned();
        duplicate.contract_hash = duplicate.expected_hash().unwrap();
        duplicate.validate().unwrap();
        assert!(matches!(
            store.install_active_contract(&duplicate, now),
            Err(StoreError::DuplicateContractVersion { .. })
        ));

        let mut expanded = active.clone();
        expanded.version = 2;
        expanded
            .context
            .permitted_source_families
            .insert("unapproved".to_owned());
        expanded.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
            context: expanded.context.clone(),
            tool_grants: expanded.tool_grants.clone(),
        };
        expanded.contract_hash = expanded.expected_hash().unwrap();
        expanded.validate().unwrap();
        assert!(matches!(
            store.install_candidate_contract(&active.contract_hash, &expanded, now),
            Err(StoreError::ContractCapabilityExpansion { .. })
        ));

        let mut candidate = active.clone();
        candidate.version = 2;
        candidate.contract_hash = candidate.expected_hash().unwrap();
        candidate.validate().unwrap();
        let stored_candidate = store
            .install_candidate_contract(&active.contract_hash, &candidate, now)
            .unwrap();
        assert_eq!(stored_candidate.contract, candidate);
        assert_eq!(
            store
                .active_contract(&active.purpose)
                .unwrap()
                .unwrap()
                .contract
                .contract_hash,
            active.contract_hash
        );
        store.verify_integrity().unwrap();

        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE rebuild_contract_installations \
                 SET contract_id = ?1 WHERE contract_hash = ?2",
                params!["forged-contract-id", active.contract_hash.as_str()],
            )
            .unwrap();
        assert!(matches!(
            store.verify_integrity(),
            Err(StoreError::Integrity(_))
        ));
    }

    #[test]
    fn contract_policy_transitions_activate_and_rollback_catalogue_history() {
        let fixture = PolicyCommitFixture::memory();
        let active = contract(&fixture.store, 1);
        let active_installation = fixture
            .store
            .install_active_contract(&active, fixture.now)
            .unwrap();
        let mut candidate = active.clone();
        candidate.version = 2;
        candidate.contract_hash = candidate.expected_hash().unwrap();
        candidate.validate().unwrap();
        let candidate_installation = fixture
            .store
            .install_candidate_contract(&active.contract_hash, &candidate, fixture.now)
            .unwrap();
        let subject = PolicySubject::Contract(candidate.contract_hash.clone());
        let fresh_permit = |label: &str, now: DateTime<Utc>| {
            let mut workflow = graph();
            workflow.topology_id = format!("contract-policy-{label}");
            let graph_artifact = artifact(
                &fixture.store,
                ArtifactKind::WorkflowGraph,
                &serde_json::to_string(&workflow).unwrap(),
                None,
            );
            let run = StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: workflow.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            };
            fixture
                .store
                .commit_workflow(&WorkflowCommit {
                    run,
                    graph: graph_artifact,
                    nodes: workflow.nodes,
                })
                .unwrap();
            fixture
                .store
                .claim_next_task(
                    &format!("contract-policy-{label}"),
                    now,
                    Duration::seconds(30),
                )
                .unwrap()
                .unwrap()
                .permit
        };
        let fresh_outcome = |permit: &TaskWritePermit, now: DateTime<Utc>| {
            let mut provenance = fixture.outcome.provenance.clone();
            provenance.producer_contract_hash = permit.contract_hash.clone();
            Artifact::new(
                ArtifactKind::Outcome,
                fixture.outcome.blob.clone(),
                fixture.outcome.producer.clone(),
                ArtifactLifecycle::Canonical,
                provenance,
                Some(ArtifactOrigin {
                    run_id: Some(permit.run_id.clone()),
                    task_id: Some(permit.task_id.clone()),
                    attempt_id: Some(permit.attempt_id.clone()),
                    contract_hash: permit.contract_hash.clone(),
                }),
                fixture.outcome.source_refs.clone(),
                now,
            )
            .unwrap()
        };
        let promotion_permit = fresh_permit("promotion", fixture.now);
        let promotion_outcome = fresh_outcome(&promotion_permit, fixture.now);

        let mut promoted_experience: Experience = fixture
            .store
            .read_artifact_payload(&fixture.experience)
            .unwrap();
        promoted_experience.experience_id = akzio_domain::ExperienceId::new();
        promoted_experience.subject = subject.clone();
        promoted_experience.contract_hash = candidate.contract_hash.clone();
        promoted_experience.outcome = artifact_ref(&promotion_outcome);
        promoted_experience.policy_state =
            PolicyState::Contract(akzio_domain::CandidatePolicyState::Canary50);
        promoted_experience.validate().unwrap();
        let promoted_experience_artifact = permit_artifact(
            &fixture.store,
            &promotion_permit,
            ArtifactKind::Experience,
            &promoted_experience,
            vec![
                promoted_experience.decision.clone(),
                promoted_experience.decision_context.clone(),
                promoted_experience.execution_context.clone(),
                promoted_experience.policy_verdict.clone(),
                promoted_experience.outcome.clone(),
            ],
            ArtifactLifecycle::Canonical,
            fixture.now,
        );
        let mut promoted_evaluation: Evaluation = fixture
            .store
            .read_artifact_payload(&fixture.evaluation)
            .unwrap();
        promoted_evaluation.evaluation_id = akzio_domain::EvaluationId::new();
        promoted_evaluation.outcome = artifact_ref(&promotion_outcome);
        promoted_evaluation.experience = artifact_ref(&promoted_experience_artifact);
        promoted_evaluation.validate().unwrap();
        let promoted_evaluation_artifact = permit_artifact(
            &fixture.store,
            &promotion_permit,
            ArtifactKind::Evaluation,
            &promoted_evaluation,
            vec![
                promoted_evaluation.outcome.clone(),
                promoted_evaluation.experience.clone(),
            ],
            ArtifactLifecycle::Canonical,
            fixture.now,
        );
        let candidate_policy = CandidatePolicy {
            schema_version: V2_SCHEMA_VERSION,
            subject: subject.clone(),
            baseline: artifact_ref(&active_installation.artifact),
            candidate: artifact_ref(&candidate_installation.artifact),
            source_evaluation: artifact_ref(&promoted_evaluation_artifact),
            created_at: fixture.now,
        };
        candidate_policy.validate().unwrap();
        let candidate_policy_artifact = permit_artifact(
            &fixture.store,
            &promotion_permit,
            ArtifactKind::CandidatePolicy,
            &candidate_policy,
            vec![
                candidate_policy.baseline.clone(),
                candidate_policy.candidate.clone(),
                candidate_policy.source_evaluation.clone(),
            ],
            ArtifactLifecycle::Canonical,
            fixture.now,
        );
        let record_canary = |from, to, completed_at| -> StoreResult<()> {
            let permit = fresh_permit(&format!("canary-{from:?}-{to:?}"), completed_at);
            let outcome = fresh_outcome(&permit, completed_at);
            let mut experience: Experience =
                fixture.store.read_artifact_payload(&fixture.experience)?;
            experience.experience_id = akzio_domain::ExperienceId::new();
            experience.subject = subject.clone();
            experience.contract_hash = candidate.contract_hash.clone();
            experience.outcome = artifact_ref(&outcome);
            experience.policy_state = PolicyState::Contract(from);
            experience.created_at = completed_at;
            experience.validate()?;
            let experience_artifact = permit_artifact(
                &fixture.store,
                &permit,
                ArtifactKind::Experience,
                &experience,
                vec![
                    experience.decision.clone(),
                    experience.decision_context.clone(),
                    experience.execution_context.clone(),
                    experience.policy_verdict.clone(),
                    experience.outcome.clone(),
                ],
                ArtifactLifecycle::Canonical,
                completed_at,
            );
            let mut evaluation: Evaluation =
                fixture.store.read_artifact_payload(&fixture.evaluation)?;
            evaluation.evaluation_id = akzio_domain::EvaluationId::new();
            evaluation.outcome = artifact_ref(&outcome);
            evaluation.experience = artifact_ref(&experience_artifact);
            evaluation.created_at = completed_at;
            evaluation.validate()?;
            let evaluation_artifact = permit_artifact(
                &fixture.store,
                &permit,
                ArtifactKind::Evaluation,
                &evaluation,
                vec![evaluation.outcome.clone(), evaluation.experience.clone()],
                ArtifactLifecycle::Canonical,
                completed_at,
            );
            let candidate_policy = CandidatePolicy {
                schema_version: V2_SCHEMA_VERSION,
                subject: subject.clone(),
                baseline: artifact_ref(&active_installation.artifact),
                candidate: artifact_ref(&candidate_installation.artifact),
                source_evaluation: artifact_ref(&evaluation_artifact),
                created_at: completed_at,
            };
            candidate_policy.validate()?;
            let candidate_policy_artifact = permit_artifact(
                &fixture.store,
                &permit,
                ArtifactKind::CandidatePolicy,
                &candidate_policy,
                vec![
                    candidate_policy.baseline.clone(),
                    candidate_policy.candidate.clone(),
                    candidate_policy.source_evaluation.clone(),
                ],
                ArtifactLifecycle::Canonical,
                completed_at,
            );
            let transition = PolicyTransition {
                schema_version: V2_SCHEMA_VERSION,
                transition_id: PolicyTransitionId::new(),
                subject: subject.clone(),
                from: PolicyState::Contract(from),
                to: PolicyState::Contract(to),
                evaluation: artifact_ref(&evaluation_artifact),
                created_at: completed_at,
            };
            fixture
                .store
                .record_policy_evaluation(&PolicyEvaluationCommit {
                    permit,
                    outcome,
                    experience: experience_artifact,
                    evaluation: evaluation_artifact,
                    candidate_policy: Some(candidate_policy_artifact),
                    subject: subject.clone(),
                    from: transition.from,
                    to: transition.to,
                    pair_snapshot: fixture.store.policy_shadow_pair_snapshot(&subject)?,
                    transition: Some(transition),
                    completed_at,
                })?;
            Ok(())
        };
        record_canary(
            akzio_domain::CandidatePolicyState::Candidate,
            akzio_domain::CandidatePolicyState::Canary10,
            fixture.now,
        )
        .unwrap();
        record_canary(
            akzio_domain::CandidatePolicyState::Canary10,
            akzio_domain::CandidatePolicyState::Canary25,
            fixture.now + Duration::microseconds(1),
        )
        .unwrap();
        record_canary(
            akzio_domain::CandidatePolicyState::Canary25,
            akzio_domain::CandidatePolicyState::Canary50,
            fixture.now + Duration::microseconds(2),
        )
        .unwrap();
        let promote_transition = PolicyTransition {
            schema_version: V2_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from: PolicyState::Contract(akzio_domain::CandidatePolicyState::Canary50),
            to: PolicyState::Contract(akzio_domain::CandidatePolicyState::Active),
            evaluation: artifact_ref(&promoted_evaluation_artifact),
            created_at: fixture.now,
        };
        let promoted_outcome_payload: Outcome = fixture
            .store
            .read_artifact_payload(&promotion_outcome)
            .unwrap();
        let schedule_artifact = fixture
            .store
            .artifact(&promoted_outcome_payload.schedule.artifact_id)
            .unwrap();
        let schedule_payload: OutcomeSchedule = fixture
            .store
            .read_artifact_payload(&schedule_artifact)
            .unwrap();
        assert_eq!(
            promoted_experience.outcome,
            artifact_ref(&promotion_outcome)
        );
        assert_eq!(
            promoted_evaluation.outcome,
            artifact_ref(&promotion_outcome)
        );
        assert_eq!(
            promoted_evaluation.experience,
            artifact_ref(&promoted_experience_artifact)
        );
        assert_eq!(promoted_experience.decision, schedule_payload.decision);
        assert_eq!(
            promoted_experience.decision_context,
            schedule_payload.decision_context
        );
        assert_eq!(
            promoted_experience.execution_context,
            schedule_payload.execution_context
        );
        fixture
            .store
            .record_policy_evaluation(&PolicyEvaluationCommit {
                permit: promotion_permit,
                outcome: promotion_outcome,
                experience: promoted_experience_artifact,
                evaluation: promoted_evaluation_artifact,
                candidate_policy: Some(candidate_policy_artifact),
                subject: subject.clone(),
                from: promote_transition.from,
                to: promote_transition.to,
                pair_snapshot: fixture.store.policy_shadow_pair_snapshot(&subject).unwrap(),
                transition: Some(promote_transition),
                completed_at: fixture.now,
            })
            .unwrap();
        assert_eq!(
            fixture
                .store
                .active_contract(&active.purpose)
                .unwrap()
                .unwrap()
                .contract
                .contract_hash,
            candidate.contract_hash
        );

        let rollback_at = fixture.now + Duration::microseconds(4);
        let rollback_permit = fresh_permit("rollback", rollback_at);
        let rollback_outcome = fresh_outcome(&rollback_permit, rollback_at);
        let mut rollback_experience: Experience = fixture
            .store
            .read_artifact_payload(&fixture.experience)
            .unwrap();
        rollback_experience.experience_id = akzio_domain::ExperienceId::new();
        rollback_experience.subject = subject.clone();
        rollback_experience.contract_hash = candidate.contract_hash.clone();
        rollback_experience.outcome = artifact_ref(&rollback_outcome);
        rollback_experience.policy_state =
            PolicyState::Contract(akzio_domain::CandidatePolicyState::Active);
        rollback_experience.validate().unwrap();
        let rollback_experience_artifact = permit_artifact(
            &fixture.store,
            &rollback_permit,
            ArtifactKind::Experience,
            &rollback_experience,
            vec![
                rollback_experience.decision.clone(),
                rollback_experience.decision_context.clone(),
                rollback_experience.execution_context.clone(),
                rollback_experience.policy_verdict.clone(),
                rollback_experience.outcome.clone(),
            ],
            ArtifactLifecycle::Canonical,
            fixture.now + Duration::microseconds(1),
        );
        let mut rollback_evaluation: Evaluation = fixture
            .store
            .read_artifact_payload(&fixture.evaluation)
            .unwrap();
        rollback_evaluation.evaluation_id = akzio_domain::EvaluationId::new();
        rollback_evaluation.outcome = artifact_ref(&rollback_outcome);
        rollback_evaluation.experience = artifact_ref(&rollback_experience_artifact);
        rollback_evaluation.created_at = rollback_at;
        rollback_evaluation.validate().unwrap();
        let rollback_evaluation_artifact = permit_artifact(
            &fixture.store,
            &rollback_permit,
            ArtifactKind::Evaluation,
            &rollback_evaluation,
            vec![
                rollback_evaluation.outcome.clone(),
                rollback_evaluation.experience.clone(),
            ],
            ArtifactLifecycle::Canonical,
            rollback_at,
        );
        let rollback_candidate_policy = CandidatePolicy {
            schema_version: V2_SCHEMA_VERSION,
            subject: subject.clone(),
            baseline: artifact_ref(&active_installation.artifact),
            candidate: artifact_ref(&candidate_installation.artifact),
            source_evaluation: artifact_ref(&rollback_evaluation_artifact),
            created_at: rollback_at,
        };
        rollback_candidate_policy.validate().unwrap();
        let rollback_candidate_policy_artifact = permit_artifact(
            &fixture.store,
            &rollback_permit,
            ArtifactKind::CandidatePolicy,
            &rollback_candidate_policy,
            vec![
                rollback_candidate_policy.baseline.clone(),
                rollback_candidate_policy.candidate.clone(),
                rollback_candidate_policy.source_evaluation.clone(),
            ],
            ArtifactLifecycle::Canonical,
            rollback_at,
        );
        let rollback_transition = PolicyTransition {
            schema_version: V2_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from: PolicyState::Contract(akzio_domain::CandidatePolicyState::Active),
            to: PolicyState::Contract(akzio_domain::CandidatePolicyState::Candidate),
            evaluation: artifact_ref(&rollback_evaluation_artifact),
            created_at: rollback_at,
        };
        fixture
            .store
            .record_policy_evaluation(&PolicyEvaluationCommit {
                permit: rollback_permit,
                outcome: rollback_outcome,
                experience: rollback_experience_artifact,
                evaluation: rollback_evaluation_artifact,
                candidate_policy: Some(rollback_candidate_policy_artifact),
                subject: subject.clone(),
                from: rollback_transition.from,
                to: rollback_transition.to,
                pair_snapshot: fixture.store.policy_shadow_pair_snapshot(&subject).unwrap(),
                transition: Some(rollback_transition),
                completed_at: rollback_at,
            })
            .unwrap();
        assert_eq!(
            fixture
                .store
                .active_contract(&active.purpose)
                .unwrap()
                .unwrap()
                .contract
                .contract_hash,
            active.contract_hash
        );
        assert_eq!(fixture.store.policy_transitions(&subject).unwrap().len(), 5);
        fixture.store.verify_integrity().unwrap();
    }

    fn artifact(
        store: &V2Store,
        kind: ArtifactKind,
        value: &str,
        origin: Option<ArtifactOrigin>,
    ) -> Artifact {
        artifact_with_refs(store, kind, value, origin, vec![])
    }

    fn artifact_with_refs(
        store: &V2Store,
        kind: ArtifactKind,
        value: &str,
        origin: Option<ArtifactOrigin>,
        source_refs: Vec<ArtifactRef>,
    ) -> Artifact {
        let producer_contract_hash = origin
            .as_ref()
            .and_then(|origin| origin.contract_hash.clone());
        Artifact::new(
            kind,
            store
                .put_bytes(value.as_bytes(), "application/json")
                .unwrap(),
            "fixture",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: None,
                retrieved_at: Utc::now(),
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash,
            },
            origin,
            source_refs,
            Utc::now(),
        )
        .unwrap()
    }

    fn graph() -> WorkflowGraph {
        WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            nodes: vec![WorkflowNode {
                task_id: TaskId::new(),
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                contract_hash: None,
                objective: "analyze".to_owned(),
                dependencies: vec![],
                input_artifacts: vec![],
                priority: 50,
                budget: budget(),
                retry: retry(),
                on_failure: FailureDisposition::FailRun,
                parent_task_id: None,
            }],
        }
    }

    fn permit_artifact<T: Serialize>(
        store: &V2Store,
        permit: &TaskWritePermit,
        kind: ArtifactKind,
        payload: &T,
        source_refs: Vec<ArtifactRef>,
        lifecycle: ArtifactLifecycle,
        now: DateTime<Utc>,
    ) -> Artifact {
        Artifact::new(
            kind,
            store.put_json(payload).unwrap(),
            "fixture.policy",
            lifecycle,
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
            source_refs,
            now,
        )
        .unwrap()
    }

    struct TaskArtifactFixture {
        _root: tempfile::TempDir,
        store: V2Store,
        run: StoredRun,
        permit: TaskWritePermit,
        now: DateTime<Utc>,
    }

    fn task_artifact_fixture(purpose: RunPurpose) -> TaskArtifactFixture {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let permit = store
            .claim_next_task("lifecycle-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        TaskArtifactFixture {
            _root: root,
            store,
            run,
            permit,
            now,
        }
    }

    fn lifecycle_test_artifact(
        fixture: &TaskArtifactFixture,
        lifecycle: ArtifactLifecycle,
        label: &str,
    ) -> Artifact {
        permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::Decision,
            &serde_json::json!({"label": label}),
            vec![],
            lifecycle,
            fixture.now,
        )
    }

    #[test]
    fn task_artifact_lifecycle_matrix_is_enforced_without_partial_writes() {
        for purpose in [
            RunPurpose::Debug,
            RunPurpose::PaperDryRun,
            RunPurpose::Replay,
            RunPurpose::Shadow,
        ] {
            for lifecycle in [ArtifactLifecycle::Ephemeral, ArtifactLifecycle::Canonical] {
                let fixture = task_artifact_fixture(purpose);
                let artifact = lifecycle_test_artifact(&fixture, lifecycle, "rejected");
                let event_count = fixture
                    .store
                    .events_after(&fixture.run.run_id, 0, 100)
                    .unwrap()
                    .len();

                assert!(matches!(
                    fixture.store.write_task_artifact(
                        &fixture.permit,
                        &artifact,
                        LifecycleEventType::ClaimCreated,
                        fixture.now,
                    ),
                    Err(StoreError::InvalidTaskArtifactLifecycle { purpose: actual, lifecycle: rejected })
                        if actual == purpose && rejected == lifecycle
                ));
                assert!(matches!(
                    fixture.store.artifact(&artifact.artifact_id),
                    Err(StoreError::MissingArtifact(_))
                ));
                assert_eq!(
                    fixture
                        .store
                        .events_after(&fixture.run.run_id, 0, 100)
                        .unwrap()
                        .len(),
                    event_count
                );
                fixture.store.verify_integrity().unwrap();
            }
        }

        for purpose in [
            RunPurpose::Debug,
            RunPurpose::Paper,
            RunPurpose::PaperDryRun,
            RunPurpose::Replay,
            RunPurpose::Shadow,
        ] {
            let fixture = task_artifact_fixture(purpose);
            let artifact =
                lifecycle_test_artifact(&fixture, ArtifactLifecycle::RunScoped, "accepted");
            fixture
                .store
                .write_task_artifact(
                    &fixture.permit,
                    &artifact,
                    LifecycleEventType::ClaimCreated,
                    fixture.now,
                )
                .unwrap();
            assert_eq!(
                fixture.store.artifact(&artifact.artifact_id).unwrap(),
                artifact
            );
            fixture.store.verify_integrity().unwrap();
        }

        let fixture = task_artifact_fixture(RunPurpose::Paper);
        let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "paper");
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &artifact,
                LifecycleEventType::ClaimCreated,
                fixture.now,
            )
            .unwrap();
        assert_eq!(
            fixture.store.artifact(&artifact.artifact_id).unwrap(),
            artifact
        );
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn attempt_commit_lifecycle_rejection_is_atomic_and_paper_canonical_is_allowed() {
        for purpose in [
            RunPurpose::Debug,
            RunPurpose::PaperDryRun,
            RunPurpose::Replay,
            RunPurpose::Shadow,
        ] {
            let fixture = task_artifact_fixture(purpose);
            let artifact =
                lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "rejected");
            let event_count = fixture
                .store
                .events_after(&fixture.run.run_id, 0, 100)
                .unwrap()
                .len();

            assert!(matches!(
                fixture.store.commit_attempt(
                    &fixture.permit,
                    std::slice::from_ref(&artifact),
                    TaskStatus::Succeeded,
                    fixture.now,
                ),
                Err(StoreError::InvalidTaskArtifactLifecycle { purpose: actual, lifecycle: ArtifactLifecycle::Canonical })
                    if actual == purpose
            ));
            assert!(matches!(
                fixture.store.artifact(&artifact.artifact_id),
                Err(StoreError::MissingArtifact(_))
            ));
            assert!(matches!(
                fixture
                    .store
                    .committed_task_outputs(&fixture.run.run_id, &fixture.permit.task_id),
                Err(StoreError::CommittedOutputTask { .. })
            ));
            assert_eq!(
                fixture
                    .store
                    .events_after(&fixture.run.run_id, 0, 100)
                    .unwrap()
                    .len(),
                event_count
            );
            assert_eq!(
                fixture
                    .store
                    .workflow_snapshot(&fixture.run.run_id)
                    .unwrap()
                    .tasks[0]
                    .status,
                TaskStatus::Running
            );
            fixture.store.verify_integrity().unwrap();
        }

        let fixture = task_artifact_fixture(RunPurpose::Paper);
        let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "paper");
        fixture
            .store
            .commit_attempt(
                &fixture.permit,
                std::slice::from_ref(&artifact),
                TaskStatus::Succeeded,
                fixture.now,
            )
            .unwrap();
        assert_eq!(
            fixture
                .store
                .committed_task_outputs(&fixture.run.run_id, &fixture.permit.task_id)
                .unwrap(),
            vec![artifact]
        );
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn stale_permit_rejects_before_task_artifact_lifecycle() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let stale = fixture.permit.clone();
        fixture
            .store
            .recover_expired_tasks(fixture.now + Duration::seconds(31))
            .unwrap();
        let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "stale");

        assert!(matches!(
            fixture.store.write_task_artifact(
                &stale,
                &artifact,
                LifecycleEventType::ClaimCreated,
                fixture.now,
            ),
            Err(StoreError::StalePermit(_))
        ));
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn bootstrap_freeze_state_remains_outside_task_artifact_firewall() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let freeze = fixture
            .store
            .write_freeze_state(true, "lifecycle firewall test", fixture.now)
            .unwrap();

        assert_eq!(freeze.kind, ArtifactKind::FreezeState);
        assert_eq!(freeze.lifecycle, ArtifactLifecycle::Canonical);
        assert_eq!(fixture.store.artifact(&freeze.artifact_id).unwrap(), freeze);
        fixture.store.verify_integrity().unwrap();
    }

    fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        }
    }

    fn valid_execution_commitment(
        store: &V2Store,
        permit: &TaskWritePermit,
        session_key: &str,
        now: DateTime<Utc>,
    ) -> Artifact {
        let source = |kind, name: &'static [u8]| {
            let artifact = permit_artifact(
                store,
                permit,
                kind,
                &serde_json::json!({"fixture": String::from_utf8_lossy(name)}),
                vec![],
                ArtifactLifecycle::RunScoped,
                now,
            );
            store
                .write_task_artifact(
                    permit,
                    &artifact,
                    LifecycleEventType::FixtureSourceCreated,
                    now,
                )
                .unwrap();
            artifact_ref(&artifact)
        };
        let decision_context = source(ArtifactKind::DecisionContext, b"decision-context");
        let account_snapshot = source(ArtifactKind::NormalizedEvidence, b"account");
        let quote_snapshot = source(ArtifactKind::NormalizedEvidence, b"quote");
        let market_clock_snapshot = source(ArtifactKind::NormalizedEvidence, b"market-clock");

        let mut target = TargetPortfolio::zeroed();
        target.weights.insert(Asset::Qqq, WeightPpm(100_000));
        let mut plan_payload = ExecutionPlan {
            schema_version: V2_SCHEMA_VERSION,
            decision_context: decision_context.clone(),
            account_snapshot: account_snapshot.clone(),
            quote_snapshot: quote_snapshot.clone(),
            market_clock_snapshot: market_clock_snapshot.clone(),
            policy_hash: ContentHash::of_bytes(b"fixture-policy"),
            target: target.clone(),
            orders: vec![OrderIntent {
                asset: Asset::Qqq,
                side: OrderSide::Buy,
                notional: MoneyMicros::from_usd_cents(10_000),
                limit_price: MoneyMicros::from_usd_cents(5_000),
            }],
            gross_exposure_ppm: 100_000,
            net_exposure_ppm: 100_000,
            factor_exposure: FactorExposure::from_target(&target).unwrap(),
            turnover_ppm: 100_000,
            broker_session: session_key.to_owned(),
            created_at: now,
            plan_hash: ContentHash::of_bytes(b"pending"),
        };
        plan_payload.refresh_hash().unwrap();
        let plan_hash = plan_payload.plan_hash.clone();
        let plan = permit_artifact(
            store,
            permit,
            ArtifactKind::ExecutionPlan,
            &plan_payload,
            vec![
                decision_context.clone(),
                account_snapshot.clone(),
                quote_snapshot.clone(),
                market_clock_snapshot.clone(),
            ],
            ArtifactLifecycle::RunScoped,
            now,
        );
        store
            .write_task_artifact(permit, &plan, LifecycleEventType::ExecutionPlanCreated, now)
            .unwrap();
        let plan_ref = artifact_ref(&plan);
        let context = permit_artifact(
            store,
            permit,
            ArtifactKind::ExecutionContext,
            &ExecutionContext {
                schema_version: V2_SCHEMA_VERSION,
                run_id: permit.run_id.clone(),
                decision_context: decision_context.clone(),
                account_snapshot: Some(account_snapshot.clone()),
                quote_snapshot: Some(quote_snapshot.clone()),
                market_clock_snapshot: Some(market_clock_snapshot.clone()),
                execution_plan: Some(plan_ref.clone()),
                factor_exposure: Some(plan_payload.factor_exposure.clone()),
                turnover_ppm: Some(plan_payload.turnover_ppm),
                plan_hash: Some(plan_hash.clone()),
                broker_session: Some(session_key.to_owned()),
                frozen: false,
                created_at: now,
            },
            vec![
                decision_context,
                account_snapshot,
                quote_snapshot,
                market_clock_snapshot,
                plan_ref,
            ],
            ArtifactLifecycle::RunScoped,
            now,
        );
        store
            .write_task_artifact(
                permit,
                &context,
                LifecycleEventType::ExecutionContextCreated,
                now,
            )
            .unwrap();
        let context_ref = artifact_ref(&context);
        let verdict = permit_artifact(
            store,
            permit,
            ArtifactKind::ExecutionVerdict,
            &ExecutionVerdict::Accepted {
                execution_context: context_ref.clone(),
            },
            vec![context_ref.clone()],
            ArtifactLifecycle::RunScoped,
            now,
        );
        store
            .write_task_artifact(
                permit,
                &verdict,
                LifecycleEventType::ExecutionVerdictCreated,
                now,
            )
            .unwrap();
        permit_artifact(
            store,
            permit,
            ArtifactKind::ExecutionCommitment,
            &PaperCommitment {
                commitment_id: PaperCommitmentId::new(),
                execution_context: context_ref.clone(),
                plan_hash,
                broker_session: session_key.to_owned(),
                client_order_ids: std::collections::BTreeMap::from([(
                    Asset::Qqq,
                    "fixture-order".to_owned(),
                )]),
                created_at: now,
            },
            vec![artifact_ref(&verdict), context_ref],
            ArtifactLifecycle::Canonical,
            now,
        )
    }

    struct ExecutionCommitFixture {
        _root: tempfile::TempDir,
        store: V2Store,
        lease: DaemonLease,
        permit: TaskWritePermit,
        commitment: Artifact,
        now: DateTime<Utc>,
    }

    fn execution_commit_fixture() -> ExecutionCommitFixture {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease(
                "scheduler",
                "fixture-daemon",
                now,
                now + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let _reservation = store
            .reserve_session_slot(
                &lease,
                &SessionReservation {
                    session_key: "paper:fixture".to_owned(),
                    workflow: WorkflowCommit {
                        run: StoredRun {
                            run_id: RunId::new(),
                            purpose: RunPurpose::Paper,
                            topology_id: graph.topology_id.clone(),
                            graph_artifact_id: graph_artifact.artifact_id.clone(),
                            created_at: now,
                        },
                        graph: graph_artifact,
                        nodes: graph.nodes,
                    },
                    setup_artifacts: vec![],
                    reserved_at: now,
                },
            )
            .unwrap();
        let permit = store
            .claim_next_task("fixture-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let commitment = valid_execution_commitment(&store, &permit, "paper:fixture", now);
        ExecutionCommitFixture {
            _root: root,
            store,
            lease,
            permit,
            commitment,
            now,
        }
    }

    struct PolicyCommitFixture {
        _root: tempfile::TempDir,
        store: V2Store,
        run: StoredRun,
        permit: TaskWritePermit,
        subject: PolicySubject,
        outcome: Artifact,
        experience: Artifact,
        evaluation: Artifact,
        candidate_policy: Option<Artifact>,
        transition: PolicyTransition,
        seed_artifact_id: ArtifactId,
        now: DateTime<Utc>,
    }

    impl PolicyCommitFixture {
        fn memory() -> Self {
            Self::new(false)
        }

        fn topology() -> Self {
            Self::new(true)
        }

        fn new(with_candidate: bool) -> Self {
            let root = tempdir().unwrap();
            let store = V2Store::open(root.path()).unwrap();
            let now = Utc::now();

            let mut paper_graph = graph();
            paper_graph.topology_id = "policy-paper".to_owned();
            let seed = paper_graph.nodes[0].clone();
            let mut evaluation_node = seed.clone();
            evaluation_node.task_id = TaskId::new();
            evaluation_node.dependencies = vec![seed.task_id.clone()];
            evaluation_node.objective = "evaluate policy".to_owned();
            paper_graph.nodes = vec![seed, evaluation_node];
            paper_graph.validate().unwrap();
            let paper_graph_artifact = artifact(
                &store,
                ArtifactKind::WorkflowGraph,
                &serde_json::to_string(&paper_graph).unwrap(),
                None,
            );
            let paper_graph_ref = artifact_ref(&paper_graph_artifact);
            let run = StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: paper_graph.topology_id.clone(),
                graph_artifact_id: paper_graph_artifact.artifact_id.clone(),
                created_at: now,
            };
            store
                .commit_workflow(&WorkflowCommit {
                    run: run.clone(),
                    graph: paper_graph_artifact,
                    nodes: paper_graph.nodes,
                })
                .unwrap();

            let seed_permit = store
                .claim_next_task("policy-seed", now, Duration::seconds(30))
                .unwrap()
                .unwrap()
                .permit;
            let normalized = permit_artifact(
                &store,
                &seed_permit,
                ArtifactKind::NormalizedEvidence,
                &serde_json::json!({"normalized": true}),
                vec![],
                ArtifactLifecycle::RunScoped,
                now,
            );
            let decision = permit_artifact(
                &store,
                &seed_permit,
                ArtifactKind::Decision,
                &serde_json::json!({"decision": true}),
                vec![],
                ArtifactLifecycle::RunScoped,
                now,
            );
            let decision_context = permit_artifact(
                &store,
                &seed_permit,
                ArtifactKind::DecisionContext,
                &serde_json::json!({"context": true}),
                vec![],
                ArtifactLifecycle::RunScoped,
                now,
            );
            let execution_context = permit_artifact(
                &store,
                &seed_permit,
                ArtifactKind::ExecutionContext,
                &serde_json::json!({"execution": true}),
                vec![],
                ArtifactLifecycle::RunScoped,
                now,
            );
            let verdict_payload = ExecutionVerdict::NoOrder {
                no_order: akzio_domain::NoOrder {
                    execution_context: artifact_ref(&execution_context),
                    blockers: vec![akzio_domain::HardBlocker::Frozen],
                    created_at: now,
                },
            };
            let verdict = permit_artifact(
                &store,
                &seed_permit,
                ArtifactKind::ExecutionVerdict,
                &verdict_payload,
                vec![artifact_ref(&execution_context)],
                ArtifactLifecycle::RunScoped,
                now,
            );
            let outcome_id = akzio_domain::OutcomeId::new();
            let schedule_payload = OutcomeSchedule {
                schema_version: V2_SCHEMA_VERSION,
                outcome_id: outcome_id.clone(),
                decision: artifact_ref(&decision),
                decision_context: artifact_ref(&decision_context),
                execution_context: artifact_ref(&execution_context),
                execution: OutcomeExecutionLineage::NoOrder {
                    execution_verdict: artifact_ref(&verdict),
                },
                baseline_trading_day: now.date_naive(),
                created_at: now,
            };
            let schedule = permit_artifact(
                &store,
                &seed_permit,
                ArtifactKind::OutcomeSchedule,
                &schedule_payload,
                vec![
                    schedule_payload.decision.clone(),
                    schedule_payload.decision_context.clone(),
                    schedule_payload.execution_context.clone(),
                    artifact_ref(&verdict),
                ],
                ArtifactLifecycle::Canonical,
                now,
            );
            store
                .commit_attempt(
                    &seed_permit,
                    &[
                        normalized.clone(),
                        decision.clone(),
                        decision_context.clone(),
                        execution_context.clone(),
                        verdict.clone(),
                        schedule.clone(),
                    ],
                    TaskStatus::Succeeded,
                    now,
                )
                .unwrap();

            let permit = store
                .claim_next_task("policy-evaluation", now, Duration::seconds(30))
                .unwrap()
                .unwrap()
                .permit;

            let candidate_graph = if with_candidate {
                let mut candidate_graph = graph();
                candidate_graph.topology_id = "policy-shadow-candidate".to_owned();
                let candidate_graph_artifact = artifact(
                    &store,
                    ArtifactKind::WorkflowGraph,
                    &serde_json::to_string(&candidate_graph).unwrap(),
                    None,
                );
                let reference = artifact_ref(&candidate_graph_artifact);
                let candidate_run = StoredRun {
                    run_id: RunId::new(),
                    purpose: RunPurpose::Shadow,
                    topology_id: candidate_graph.topology_id.clone(),
                    graph_artifact_id: candidate_graph_artifact.artifact_id.clone(),
                    created_at: now,
                };
                store
                    .commit_workflow(&WorkflowCommit {
                        run: candidate_run,
                        graph: candidate_graph_artifact,
                        nodes: candidate_graph.nodes,
                    })
                    .unwrap();
                Some((reference, candidate_graph.topology_id))
            } else {
                None
            };
            let subject = candidate_graph.as_ref().map_or_else(
                || PolicySubject::Memory(akzio_domain::MemoryId::new()),
                |(_, topology_id)| {
                    PolicySubject::Topology(akzio_domain::TopologyId(topology_id.clone()))
                },
            );
            let from = subject.initial_state();
            let to = match subject {
                PolicySubject::Memory(_) => {
                    PolicyState::Memory(akzio_domain::MemoryLifecycle::Active)
                }
                PolicySubject::Topology(_) => {
                    PolicyState::Topology(akzio_domain::CandidatePolicyState::Canary10)
                }
                PolicySubject::Contract(_) => unreachable!(),
            };
            let outcome_payload = Outcome {
                schema_version: V2_SCHEMA_VERSION,
                outcome_id,
                schedule: artifact_ref(&schedule),
                market_evidence: vec![artifact_ref(&normalized)],
                windows: OutcomeHorizon::ALL
                    .into_iter()
                    .map(|horizon| akzio_domain::OutcomeWindow {
                        horizon,
                        observed_trading_day: now.date_naive()
                            + chrono::Days::new(u64::from(horizon.trading_days())),
                        portfolio_return_ppm: 1,
                        benchmark_return_ppm: 0,
                        transaction_cost_ppm: 0,
                        slippage_ppm: 0,
                        utility_ppm: 1,
                        calibration_ppm: 1_000_000,
                        evidence_completeness_ppm: 1_000_000,
                        risk_recall_ppm: 1_000_000,
                    })
                    .collect(),
                sealed_at: Some(now),
            };
            let outcome = permit_artifact(
                &store,
                &permit,
                ArtifactKind::Outcome,
                &outcome_payload,
                vec![artifact_ref(&schedule), artifact_ref(&normalized)],
                ArtifactLifecycle::Canonical,
                now,
            );
            let experience_payload = Experience {
                schema_version: V2_SCHEMA_VERSION,
                experience_id: akzio_domain::ExperienceId::new(),
                subject: subject.clone(),
                hypothesis_id: "fixture".to_owned(),
                decision: artifact_ref(&decision),
                decision_context: artifact_ref(&decision_context),
                execution_context: artifact_ref(&execution_context),
                policy_verdict: artifact_ref(&verdict),
                outcome: artifact_ref(&outcome),
                contract_hash: ContentHash::of_bytes(b"fixture-contract"),
                topology_id: match &subject {
                    PolicySubject::Topology(topology_id) => topology_id.clone(),
                    _ => akzio_domain::TopologyId("fixture-topology".to_owned()),
                },
                policy_state: from,
                created_at: now,
            };
            let experience = permit_artifact(
                &store,
                &permit,
                ArtifactKind::Experience,
                &experience_payload,
                vec![
                    experience_payload.decision.clone(),
                    experience_payload.decision_context.clone(),
                    experience_payload.execution_context.clone(),
                    experience_payload.policy_verdict.clone(),
                    experience_payload.outcome.clone(),
                ],
                ArtifactLifecycle::Canonical,
                now,
            );
            let evaluation_payload = Evaluation {
                schema_version: V2_SCHEMA_VERSION,
                evaluation_id: akzio_domain::EvaluationId::new(),
                outcome: artifact_ref(&outcome),
                experience: artifact_ref(&experience),
                marginal_utility_ppm: 1,
                token_cost: 1,
                latency_millis: 1,
                created_at: now,
            };
            let evaluation = permit_artifact(
                &store,
                &permit,
                ArtifactKind::Evaluation,
                &evaluation_payload,
                vec![artifact_ref(&outcome), artifact_ref(&experience)],
                ArtifactLifecycle::Canonical,
                now,
            );
            let candidate_policy = candidate_graph.map(|(candidate, _)| {
                let payload = CandidatePolicy {
                    schema_version: V2_SCHEMA_VERSION,
                    subject: subject.clone(),
                    baseline: paper_graph_ref,
                    candidate,
                    source_evaluation: artifact_ref(&evaluation),
                    created_at: now,
                };
                permit_artifact(
                    &store,
                    &permit,
                    ArtifactKind::CandidatePolicy,
                    &payload,
                    vec![
                        payload.baseline.clone(),
                        payload.candidate.clone(),
                        payload.source_evaluation.clone(),
                    ],
                    ArtifactLifecycle::Canonical,
                    now,
                )
            });
            let transition = PolicyTransition {
                schema_version: V2_SCHEMA_VERSION,
                transition_id: PolicyTransitionId::new(),
                subject: subject.clone(),
                from,
                to,
                evaluation: artifact_ref(&evaluation),
                created_at: now,
            };

            Self {
                _root: root,
                store,
                run,
                permit,
                subject,
                outcome,
                experience,
                evaluation,
                candidate_policy,
                transition,
                seed_artifact_id: decision.artifact_id,
                now,
            }
        }

        fn commit(&self, pair_snapshot: PolicyShadowPairSnapshot) -> PolicyEvaluationCommit {
            PolicyEvaluationCommit {
                permit: self.permit.clone(),
                outcome: self.outcome.clone(),
                experience: self.experience.clone(),
                evaluation: self.evaluation.clone(),
                candidate_policy: self.candidate_policy.clone(),
                subject: self.subject.clone(),
                from: self.transition.from,
                to: self.transition.to,
                pair_snapshot,
                transition: Some(self.transition.clone()),
                completed_at: self.now,
            }
        }

        fn insert_pair(
            &self,
            label: &str,
            horizon: OutcomeHorizon,
            completed_at: DateTime<Utc>,
        ) -> i64 {
            let pair_key = ContentHash::of_bytes(label.as_bytes());
            let mut connection = self.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let cursor = append_event(
                &transaction,
                &self.run.run_id,
                Some(&self.permit.task_id),
                Some(&self.permit.attempt_id),
                LifecycleEventType::ShadowPairCompleted,
                Some(&self.seed_artifact_id),
                completed_at,
            )
            .unwrap();
            transaction
                .execute(
                    r#"INSERT INTO rebuild_shadow_pairs
                       (pair_key, subject_id, subject_json, parent_decision_artifact_id,
                        execution_context_artifact_id, candidate_decision_artifact_id,
                        candidate_contract_hash, candidate_topology_id, horizon,
                        parent_outcome_artifact_id, candidate_outcome_artifact_id,
                        completed_at, pair_event_cursor)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
                    params![
                        pair_key.as_str(),
                        self.subject.subject_id(),
                        serde_json::to_string(&self.subject).unwrap(),
                        self.seed_artifact_id.0.as_str(),
                        self.seed_artifact_id.0.as_str(),
                        self.seed_artifact_id.0.as_str(),
                        ContentHash::of_bytes(b"fixture-candidate-contract").as_str(),
                        "fixture-candidate-topology",
                        enum_name(horizon),
                        self.seed_artifact_id.0.as_str(),
                        self.seed_artifact_id.0.as_str(),
                        completed_at.to_rfc3339(),
                        cursor,
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
            cursor
        }
    }

    #[test]
    fn workflow_commit_accepts_out_of_order_nodes_and_preserves_dependencies() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let mut graph = graph();
        let parent = graph.nodes[0].clone();
        let mut child = parent.clone();
        child.task_id = TaskId::new();
        child.objective = "dependent analysis".to_owned();
        child.dependencies = vec![parent.task_id.clone()];
        graph.nodes = vec![child, parent.clone()];
        graph.validate().unwrap();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let claimed = store
            .claim_next_task("worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(claimed.node.task_id, parent.task_id);
        store.verify_integrity().unwrap();
    }

    #[test]
    fn retry_and_cancellation_are_durable_and_fenced() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let mut graph = graph();
        graph.nodes[0].retry.max_attempts = 2;
        graph.nodes[0].retry.initial_backoff_ms = 0;
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let first = store
            .claim_next_task("worker-a", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .retry_task(&first.permit, Utc::now(), Utc::now())
                .unwrap(),
            RetryTaskResult::Requeued
        );
        let second = store
            .claim_next_task("worker-b", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_ne!(first.permit.attempt_id, second.permit.attempt_id);
        assert!(store
            .request_run_cancel(&run.run_id, "operator", Utc::now())
            .unwrap());
        assert!(store.run_cancel_requested(&run.run_id).unwrap());
        assert!(matches!(
            store.finish_task(&first.permit, TaskStatus::Cancelled, Utc::now()),
            Err(StoreError::StalePermit(_))
        ));
        store
            .finish_task(&second.permit, TaskStatus::Cancelled, Utc::now())
            .unwrap();
        let events = store.events_after(&run.run_id, 0, 100).unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "task.retry_scheduled"));
        assert!(events
            .iter()
            .any(|event| event.event_type == "run.cancel_requested"));
        store.verify_integrity().unwrap();
    }

    #[test]
    fn legacy_root_is_rejected_instead_of_migrated() {
        let root = tempdir().unwrap();
        fs::write(
            root.path().join(INCOMPATIBLE_DATABASE_FILE),
            b"incompatible",
        )
        .unwrap();
        assert!(matches!(
            V2Store::open(root.path()),
            Err(StoreError::IncompatibleStoreRoot(_))
        ));
    }

    #[test]
    fn workflow_commit_is_atomic_and_claim_yields_a_permit() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes.clone(),
            })
            .unwrap();
        let claimed = store
            .claim_next_task("worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(claimed.run_id, run.run_id);
        assert_eq!(claimed.node.task_id, graph.nodes[0].task_id);
        assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 2);
        store.verify_integrity().unwrap();
    }

    #[test]
    fn attempt_commit_is_atomic_with_outputs_and_terminal_event() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let claimed = store
            .claim_next_task("worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        let turn = artifact(
            &store,
            ArtifactKind::AgentTurn,
            "intermediate turn",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
        );
        store
            .write_task_artifact(
                &claimed.permit,
                &turn,
                LifecycleEventType::AgentTurn,
                Utc::now(),
            )
            .unwrap();
        assert!(matches!(
            store.committed_attempt_outputs(&claimed.permit.task_id, &claimed.permit.attempt_id),
            Err(StoreError::CommittedOutputAttempt { .. })
        ));
        let evidence = artifact(
            &store,
            ArtifactKind::NormalizedEvidence,
            "claim evidence",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
        );
        let output = artifact_with_refs(
            &store,
            ArtifactKind::Claim,
            "claim",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
            vec![artifact_ref(&evidence)],
        );

        store
            .commit_attempt(
                &claimed.permit,
                &[evidence.clone(), output.clone()],
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();

        assert_eq!(
            store
                .committed_attempt_outputs(&claimed.permit.task_id, &claimed.permit.attempt_id)
                .unwrap(),
            vec![evidence.clone(), output.clone()]
        );
        assert_eq!(
            store
                .committed_task_outputs(&run.run_id, &claimed.permit.task_id)
                .unwrap(),
            vec![evidence, output]
        );
        assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 6);
        assert!(store
            .claim_next_task("worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .is_none());
        store.verify_integrity().unwrap();
    }

    #[test]
    fn attempt_commit_resolves_same_batch_evidence_closure_before_persisting() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let claimed = store
            .claim_next_task("worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        let origin = Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        });
        let raw = artifact(&store, ArtifactKind::RawEvidence, "raw", origin.clone());
        let normalized = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store.put_bytes(b"normalized", "application/json").unwrap(),
            "fixture.normalized",
            ArtifactLifecycle::RunScoped,
            raw.provenance.clone(),
            origin.clone(),
            vec![ArtifactRef {
                artifact_id: raw.artifact_id.clone(),
                kind: ArtifactKind::RawEvidence,
            }],
            Utc::now(),
        )
        .unwrap();
        let missing = Artifact::new(
            ArtifactKind::NormalizedEvidence,
            store.put_bytes(b"missing", "application/json").unwrap(),
            "fixture.normalized",
            ArtifactLifecycle::RunScoped,
            raw.provenance.clone(),
            origin,
            vec![ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"missing raw")),
                kind: ArtifactKind::RawEvidence,
            }],
            Utc::now(),
        )
        .unwrap();

        assert!(matches!(
            store.commit_attempt(
                &claimed.permit,
                std::slice::from_ref(&missing),
                TaskStatus::Succeeded,
                Utc::now(),
            ),
            Err(StoreError::InvalidArtifactClosure(_))
        ));
        assert!(matches!(
            store.artifact(&missing.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));

        store
            .commit_attempt(
                &claimed.permit,
                &[normalized.clone(), raw.clone()],
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();
        assert_eq!(
            store
                .committed_task_outputs(&run.run_id, &claimed.permit.task_id)
                .unwrap(),
            vec![normalized, raw]
        );
        store.verify_integrity().unwrap();
    }

    #[test]
    fn attempt_commit_rolls_back_when_terminal_event_write_fails() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let claimed = store
            .claim_next_task("worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .unwrap();
        let evidence = artifact(
            &store,
            ArtifactKind::NormalizedEvidence,
            "claim evidence",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
        );
        let output = artifact_with_refs(
            &store,
            ArtifactKind::Claim,
            "claim",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
            vec![artifact_ref(&evidence)],
        );
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch(
                    "CREATE TRIGGER fail_terminal_event BEFORE INSERT ON rebuild_events
                     WHEN NEW.event_type = 'task.succeeded'
                     BEGIN SELECT RAISE(ABORT, 'injected terminal event failure'); END;",
                )
                .unwrap();
        }
        assert!(matches!(
            store.commit_attempt(
                &claimed.permit,
                &[evidence.clone(), output.clone()],
                TaskStatus::Succeeded,
                Utc::now()
            ),
            Err(StoreError::Sql(_))
        ));
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch("DROP TRIGGER fail_terminal_event;")
                .unwrap();
        }
        assert!(matches!(
            store.artifact(&output.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 2);
        store
            .commit_attempt(
                &claimed.permit,
                &[evidence, output],
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();
        store.verify_integrity().unwrap();
    }

    #[test]
    fn workflow_patch_rolls_back_proposal_graph_tasks_events_and_planner_completion() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let planner_contract = ContentHash::of_bytes(b"planner-contract");
        let planner = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("research.planner").unwrap(),
            contract_hash: Some(planner_contract.clone()),
            objective: "plan".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 90,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let planner_task_id = planner.task_id.clone();
        let evidence = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("gate.evidence").unwrap(),
            contract_hash: None,
            objective: "evidence gate".to_owned(),
            dependencies: vec![planner.task_id.clone()],
            input_artifacts: vec![],
            priority: 80,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let decision = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("gate.decision").unwrap(),
            contract_hash: None,
            objective: "decision gate".to_owned(),
            dependencies: vec![evidence.task_id.clone()],
            input_artifacts: vec![],
            priority: 70,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            nodes: vec![planner.clone(), evidence.clone(), decision.clone()],
        };
        graph.validate().unwrap();
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&graph).unwrap(),
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact.clone(),
                nodes: graph.nodes.clone(),
            })
            .unwrap();
        let claimed = store
            .claim_next_task("planner-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let evidence_need = artifact(
            &store,
            ArtifactKind::EvidenceNeed,
            "evidence need",
            Some(ArtifactOrigin {
                run_id: Some(run.run_id.clone()),
                task_id: Some(planner.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: claimed.permit.contract_hash.clone(),
            }),
        );
        let evidence_need_ref = ArtifactRef {
            artifact_id: evidence_need.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        };
        let planner_output = artifact(
            &store,
            ArtifactKind::WorkflowProposalDraft,
            "planner output",
            Some(ArtifactOrigin {
                run_id: Some(run.run_id.clone()),
                task_id: Some(planner.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: claimed.permit.contract_hash.clone(),
            }),
        );

        let proposal = WorkflowProposal {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            tasks: std::collections::BTreeMap::from([(
                "analyst".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    objective: "analyse".to_owned(),
                    depends_on: vec![],
                    priority: 60,
                    evidence_needs: vec![evidence_need_ref.clone()],
                },
            )]),
            stop_reason: None,
        };
        let proposal_artifact = Artifact::new(
            ArtifactKind::WorkflowProposal,
            store.put_json(&proposal).unwrap(),
            "agent.planner",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(planner_contract),
            },
            Some(ArtifactOrigin {
                run_id: Some(run.run_id.clone()),
                task_id: Some(planner.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: claimed.permit.contract_hash.clone(),
            }),
            vec![
                ArtifactRef {
                    artifact_id: planner_output.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowProposalDraft,
                },
                evidence_need_ref.clone(),
            ],
            now,
        )
        .unwrap();
        let added = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash: Some(ContentHash::of_bytes(b"analyst-contract")),
            objective: "analyse".to_owned(),
            dependencies: vec![evidence.task_id.clone()],
            input_artifacts: vec![evidence_need_ref.clone()],
            priority: 60,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let mut updated_evidence = evidence;
        updated_evidence.input_artifacts = vec![evidence_need_ref.clone()];
        let mut updated_decision = decision;
        updated_decision.dependencies = vec![added.task_id.clone()];
        let next_graph = WorkflowGraph {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            nodes: vec![
                planner,
                updated_evidence.clone(),
                added.clone(),
                updated_decision.clone(),
            ],
        };
        next_graph.validate().unwrap();
        let next_graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.put_json(&next_graph).unwrap(),
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![
                ArtifactRef {
                    artifact_id: graph_artifact.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowGraph,
                },
                ArtifactRef {
                    artifact_id: proposal_artifact.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowProposal,
                },
            ],
            now,
        )
        .unwrap();
        let patch = WorkflowPatchCommit {
            permit: claimed.permit.clone(),
            previous_graph_artifact_id: graph_artifact.artifact_id.clone(),
            planner_output: planner_output.clone(),
            evidence_needs: vec![evidence_need.clone()],
            proposal: proposal_artifact.clone(),
            next_graph: next_graph_artifact.clone(),
            added_nodes: vec![added.clone()],
            updated_nodes: vec![updated_evidence, updated_decision],
            completed_at: now,
        };

        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch(
                    "CREATE TRIGGER fail_workflow_patch_completion BEFORE INSERT ON rebuild_events
                     WHEN NEW.event_type = 'task.succeeded'
                     BEGIN SELECT RAISE(ABORT, 'injected workflow patch failure'); END;",
                )
                .unwrap();
        }
        assert!(matches!(
            store.commit_workflow_patch(&patch),
            Err(StoreError::Sql(_))
        ));
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch("DROP TRIGGER fail_workflow_patch_completion;")
                .unwrap();
            let revisions: u64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM rebuild_workflow_revisions WHERE run_id = ?1",
                    params![run.run_id.0],
                    |row| row.get(0),
                )
                .unwrap();
            let added_tasks: u64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM rebuild_tasks WHERE task_id = ?1",
                    params![added.task_id.0],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(revisions, 1);
            assert_eq!(added_tasks, 0);
        }
        assert!(matches!(
            store.artifact(&planner_output.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.artifact(&evidence_need.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.artifact(&proposal_artifact.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.artifact(&next_graph_artifact.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert_eq!(store.events_after(&run.run_id, 0, 100).unwrap().len(), 2);
        store.validate_task_permit(&claimed.permit).unwrap();

        store.commit_workflow_patch(&patch).unwrap();
        assert_eq!(
            store.artifact(&planner_output.artifact_id).unwrap(),
            planner_output
        );
        assert_eq!(
            store.artifact(&evidence_need.artifact_id).unwrap(),
            evidence_need
        );
        assert_eq!(
            store.artifact(&proposal_artifact.artifact_id).unwrap(),
            proposal_artifact
        );
        assert_eq!(
            store
                .committed_task_outputs(&run.run_id, &planner_task_id)
                .unwrap(),
            vec![proposal_artifact.clone()]
        );
        assert_eq!(
            store
                .committed_attempt_outputs(&planner_task_id, &claimed.permit.attempt_id)
                .unwrap(),
            vec![proposal_artifact.clone()]
        );
        let stored_graph = store.artifact(&next_graph_artifact.artifact_id).unwrap();
        assert_eq!(stored_graph.artifact_id, next_graph_artifact.artifact_id);
        let mut stored_refs = stored_graph.source_refs;
        let mut expected_refs = next_graph_artifact.source_refs;
        stored_refs.sort_by(|left, right| {
            left.artifact_id
                .cmp(&right.artifact_id)
                .then(left.kind.cmp(&right.kind))
        });
        expected_refs.sort_by(|left, right| {
            left.artifact_id
                .cmp(&right.artifact_id)
                .then(left.kind.cmp(&right.kind))
        });
        assert_eq!(stored_refs, expected_refs);
        assert!(matches!(
            store.validate_task_permit(&claimed.permit),
            Err(StoreError::StalePermit(_))
        ));
        let claimed_evidence = store
            .claim_next_task("evidence-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        assert_eq!(
            claimed_evidence.node.input_artifacts,
            vec![evidence_need_ref]
        );
        let initial_revision = store.workflow_revision(&run.run_id, 0).unwrap();
        let current_revision = store.workflow_revision(&run.run_id, 1).unwrap();
        assert_eq!(
            initial_revision.graph_artifact.artifact_id,
            graph_artifact.artifact_id
        );
        assert_eq!(
            current_revision.graph_artifact.artifact_id,
            next_graph_artifact.artifact_id
        );
        let snapshot = store.workflow_snapshot(&run.run_id).unwrap();
        assert_eq!(snapshot.status, WorkflowStatus::Running);
        assert_eq!(snapshot.revision, current_revision);
        assert_eq!(snapshot.tasks.len(), 4);
        assert_eq!(
            snapshot.event_cursor,
            store
                .events_after(&run.run_id, 0, 100)
                .unwrap()
                .last()
                .unwrap()
                .cursor
        );
        let evidence_snapshot = snapshot
            .tasks
            .iter()
            .find(|task| task.node.task_id == claimed_evidence.node.task_id)
            .unwrap();
        assert_eq!(evidence_snapshot.attempt_count, 1);
        assert_eq!(
            evidence_snapshot.active_attempt.as_ref().unwrap().permit,
            claimed_evidence.permit
        );
        store.verify_integrity().unwrap();
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute(
                    "DELETE FROM rebuild_artifact_refs WHERE artifact_id = ?1 AND source_kind = ?2",
                    params![
                        next_graph_artifact.artifact_id.0.as_str(),
                        enum_name(ArtifactKind::WorkflowProposal)
                    ],
                )
                .unwrap();
        }
        assert!(store.verify_integrity().is_err());
    }

    #[test]
    fn stale_permit_cannot_write_an_artifact() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: Utc::now(),
        };
        store
            .commit_workflow(&WorkflowCommit {
                run,
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let claimed = store
            .claim_next_task("worker", Utc::now(), Duration::milliseconds(-1))
            .unwrap()
            .unwrap();
        store.recover_expired_tasks(Utc::now()).unwrap();
        let evidence = artifact(
            &store,
            ArtifactKind::NormalizedEvidence,
            "claim evidence",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
        );
        let artifact = artifact_with_refs(
            &store,
            ArtifactKind::Claim,
            "claim",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
            vec![artifact_ref(&evidence)],
        );
        assert!(matches!(
            store.write_task_artifact(
                &claimed.permit,
                &artifact,
                LifecycleEventType::ClaimCreated,
                Utc::now()
            ),
            Err(StoreError::StalePermit(_))
        ));
    }

    #[test]
    fn bootstrapped_contract_must_not_carry_task_origin() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let artifact = artifact(&store, ArtifactKind::Contract, "contract", None);
        store.write_bootstrap_artifact(&artifact).unwrap();
        store.verify_integrity().unwrap();
    }

    #[test]
    fn execution_commitment_lineage_fails_closed() {
        let fixture = execution_commit_fixture();
        let mut commitment = fixture.commitment.clone();
        commitment.lifecycle = ArtifactLifecycle::RunScoped;
        assert!(fixture
            .store
            .commit_execution(
                &fixture.lease,
                &ExecutionCommit {
                    session_key: "paper:fixture".to_owned(),
                    permit: fixture.permit.clone(),
                    commitment,
                    committed_at: fixture.now,
                },
            )
            .is_err());

        let fixture = execution_commit_fixture();
        let mut commitment = fixture.commitment.clone();
        commitment
            .source_refs
            .retain(|source| source.kind != ArtifactKind::ExecutionVerdict);
        assert!(fixture
            .store
            .commit_execution(
                &fixture.lease,
                &ExecutionCommit {
                    session_key: "paper:fixture".to_owned(),
                    permit: fixture.permit.clone(),
                    commitment,
                    committed_at: fixture.now,
                },
            )
            .is_err());

        let fixture = execution_commit_fixture();
        let mut commitment = fixture.commitment.clone();
        let verdict = commitment
            .source_refs
            .iter()
            .find(|source| source.kind == ArtifactKind::ExecutionVerdict)
            .unwrap()
            .clone();
        commitment.source_refs.push(verdict);
        assert!(fixture
            .store
            .commit_execution(
                &fixture.lease,
                &ExecutionCommit {
                    session_key: "paper:fixture".to_owned(),
                    permit: fixture.permit.clone(),
                    commitment,
                    committed_at: fixture.now,
                },
            )
            .is_err());

        let fixture = execution_commit_fixture();
        let mut commitment = fixture.commitment.clone();
        let context = commitment
            .source_refs
            .iter()
            .find(|source| source.kind == ArtifactKind::ExecutionContext)
            .unwrap()
            .clone();
        let no_order = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ExecutionVerdict,
            &ExecutionVerdict::NoOrder {
                no_order: NoOrder {
                    execution_context: context.clone(),
                    blockers: vec![HardBlocker::Frozen],
                    created_at: fixture.now,
                },
            },
            vec![context.clone()],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &no_order,
                LifecycleEventType::ExecutionVerdictNoOrder,
                fixture.now,
            )
            .unwrap();
        commitment.source_refs = vec![artifact_ref(&no_order), context];
        assert!(fixture
            .store
            .commit_execution(
                &fixture.lease,
                &ExecutionCommit {
                    session_key: "paper:fixture".to_owned(),
                    permit: fixture.permit.clone(),
                    commitment,
                    committed_at: fixture.now,
                },
            )
            .is_err());

        let fixture = execution_commit_fixture();
        let mut commitment = fixture.commitment.clone();
        let context_index = commitment
            .source_refs
            .iter()
            .position(|source| source.kind == ArtifactKind::ExecutionContext)
            .unwrap();
        commitment.source_refs[context_index] = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"wrong-context")),
            kind: ArtifactKind::ExecutionContext,
        };
        assert!(fixture
            .store
            .commit_execution(
                &fixture.lease,
                &ExecutionCommit {
                    session_key: "paper:fixture".to_owned(),
                    permit: fixture.permit.clone(),
                    commitment,
                    committed_at: fixture.now,
                },
            )
            .is_err());

        let fixture = execution_commit_fixture();
        let context_ref = fixture
            .commitment
            .source_refs
            .iter()
            .find(|source| source.kind == ArtifactKind::ExecutionContext)
            .unwrap();
        let context = fixture.store.artifact(&context_ref.artifact_id).unwrap();
        let plan_ref = context
            .source_refs
            .iter()
            .find(|source| source.kind == ArtifactKind::ExecutionPlan)
            .unwrap();
        let wrong_plan = fixture
            .store
            .put_json(&serde_json::json!({
                "plan_hash": ContentHash::of_bytes(b"wrong-plan")
            }))
            .unwrap();
        fixture
            .store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE rebuild_artifacts SET blob_hash = ?1, media_type = ?2, bytes = ?3 WHERE artifact_id = ?4",
                params![
                    wrong_plan.hash.as_str(),
                    wrong_plan.media_type,
                    wrong_plan.bytes,
                    plan_ref.artifact_id.0.as_str(),
                ],
            )
            .unwrap();
        assert!(fixture
            .store
            .commit_execution(
                &fixture.lease,
                &ExecutionCommit {
                    session_key: "paper:fixture".to_owned(),
                    permit: fixture.permit.clone(),
                    commitment: fixture.commitment,
                    committed_at: fixture.now,
                },
            )
            .is_err());
    }

    #[test]
    fn stale_outcome_lease_rejects_artifact_write_without_partial_commit() {
        let fixture = execution_commit_fixture();
        let stale = fixture.now + Duration::seconds(31);
        let successor = fixture
            .store
            .acquire_daemon_lease(
                "scheduler",
                "successor",
                stale,
                stale + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let evidence = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::NormalizedEvidence,
            &serde_json::json!({"outcome": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            stale,
        );
        assert!(matches!(
            fixture.store.write_task_artifact_fenced(
                Some(&fixture.lease),
                &fixture.permit,
                &evidence,
                LifecycleEventType::OutcomeEvidence,
                stale,
            ),
            Err(StoreError::SchedulerFenced(_))
        ));
        assert!(matches!(
            fixture.store.artifact(&evidence.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        fixture
            .store
            .write_task_artifact_fenced(
                Some(&successor),
                &fixture.permit,
                &evidence,
                LifecycleEventType::OutcomeEvidence,
                stale,
            )
            .unwrap();
        assert_eq!(
            fixture.store.artifact(&evidence.artifact_id).unwrap().kind,
            ArtifactKind::NormalizedEvidence
        );
    }

    #[test]
    fn stale_outcome_lease_rejects_canonical_policy_evaluation() {
        let fixture = PolicyCommitFixture::memory();
        let lease_now = fixture.now;
        let lease = fixture
            .store
            .acquire_daemon_lease(
                "outcome-worker",
                "worker-a",
                lease_now,
                lease_now + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let stale = lease_now + Duration::seconds(31);
        fixture
            .store
            .acquire_daemon_lease(
                "outcome-worker",
                "worker-b",
                stale,
                stale + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        let commit = fixture.commit(
            fixture
                .store
                .policy_shadow_pair_snapshot(&fixture.subject)
                .unwrap(),
        );
        assert!(matches!(
            fixture
                .store
                .record_policy_evaluation_fenced(Some(&lease), &commit),
            Err(StoreError::SchedulerFenced(_))
        ));
        assert!(fixture
            .store
            .policy_head(&fixture.subject)
            .unwrap()
            .is_none());
        assert!(matches!(
            fixture.store.artifact(&commit.evaluation.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
    }

    #[test]
    fn outcome_schedule_worker_enqueue_is_idempotent_for_same_permit() {
        let fixture = PolicyCommitFixture::memory();
        let outcome_payload: Outcome = fixture
            .store
            .read_artifact_payload(&fixture.outcome)
            .unwrap();
        let stored_schedule = fixture
            .store
            .artifact(&outcome_payload.schedule.artifact_id)
            .unwrap();
        let mut payload: OutcomeSchedule = fixture
            .store
            .read_artifact_payload(&stored_schedule)
            .unwrap();
        payload.outcome_id = akzio_domain::OutcomeId::new();
        payload.created_at = fixture.now + Duration::seconds(1);
        let schedule = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::OutcomeSchedule,
            &payload,
            outcome_schedule_source_refs(&payload),
            ArtifactLifecycle::Canonical,
            payload.created_at,
        );

        fixture
            .store
            .commit_outcome_schedule_with_worker(&fixture.permit, &schedule, fixture.now)
            .unwrap();
        fixture
            .store
            .commit_outcome_schedule_with_worker(
                &fixture.permit,
                &schedule,
                fixture.now + Duration::seconds(1),
            )
            .unwrap();

        let worker_count = fixture
            .store
            .connection
            .lock()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM rebuild_tasks WHERE run_id = ?1 AND recipe_id = ?2",
                params![fixture.run.run_id.0, POST_TERMINAL_WORKER_RECIPE_ID],
                |row| row.get::<_, u64>(0),
            )
            .unwrap();
        assert_eq!(worker_count, 1);
        let enqueued_events = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == "outcome.worker.enqueued")
            .count();
        assert_eq!(enqueued_events, 1);
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn daemon_lease_validation_and_fenced_attempt_fail_closed() {
        let fixture = execution_commit_fixture();
        fixture
            .store
            .validate_daemon_lease(&fixture.lease, fixture.now)
            .unwrap();
        let successor_now = fixture.now + Duration::seconds(31);
        let successor = fixture
            .store
            .acquire_daemon_lease(
                "scheduler",
                "successor",
                successor_now,
                successor_now + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        assert!(matches!(
            fixture
                .store
                .validate_daemon_lease(&fixture.lease, successor_now),
            Err(StoreError::SchedulerFenced(_))
        ));
        fixture
            .store
            .validate_daemon_lease(&successor, successor_now)
            .unwrap();

        let receipt = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::OrderReceipt,
            &serde_json::json!({"receipt": true}),
            vec![],
            ArtifactLifecycle::Canonical,
            successor_now,
        );
        assert!(matches!(
            fixture.store.commit_fenced_attempt(
                &fixture.lease,
                &fixture.permit,
                std::slice::from_ref(&receipt),
                TaskStatus::Succeeded,
                successor_now,
            ),
            Err(StoreError::SchedulerFenced(_))
        ));
        assert!(matches!(
            fixture.store.artifact(&receipt.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        fixture.store.validate_task_permit(&fixture.permit).unwrap();

        fixture
            .store
            .commit_fenced_attempt(
                &successor,
                &fixture.permit,
                std::slice::from_ref(&receipt),
                TaskStatus::Succeeded,
                successor_now,
            )
            .unwrap();
        assert_eq!(
            fixture.store.artifact(&receipt.artifact_id).unwrap().kind,
            ArtifactKind::OrderReceipt
        );
    }

    #[test]
    fn doctor_rejects_corrupt_execution_lineage() {
        let fixture = execution_commit_fixture();
        let payload: PaperCommitment =
            serde_json::from_slice(&fixture.store.read_blob(&fixture.commitment.blob).unwrap())
                .unwrap();
        let context = fixture
            .commitment
            .source_refs
            .iter()
            .find(|source| source.kind == ArtifactKind::ExecutionContext)
            .unwrap()
            .clone();
        let invalid = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ExecutionCommitment,
            &payload,
            vec![context],
            ArtifactLifecycle::Canonical,
            fixture.now,
        );
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection.transaction().unwrap();
            insert_artifact(&transaction, &invalid).unwrap();
            transaction
                .execute(
                    "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3",
                    params![
                        invalid.artifact_id.0.as_str(),
                        fixture.now.to_rfc3339(),
                        "paper:fixture",
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
        }
        let error = fixture.store.verify_integrity().unwrap_err();
        assert!(
            matches!(
                &error,
                StoreError::Integrity(message)
                    if message.contains("commitment lineage is invalid")
            ),
            "{error}"
        );

        let fixture = execution_commit_fixture();
        let payload: PaperCommitment =
            serde_json::from_slice(&fixture.store.read_blob(&fixture.commitment.blob).unwrap())
                .unwrap();
        let invalid = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ExecutionCommitment,
            &payload,
            fixture.commitment.source_refs.clone(),
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection.transaction().unwrap();
            insert_artifact(&transaction, &invalid).unwrap();
            transaction
                .execute(
                    "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3",
                    params![
                        invalid.artifact_id.0.as_str(),
                        fixture.now.to_rfc3339(),
                        "paper:fixture",
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
        }
        let error = fixture.store.verify_integrity().unwrap_err();
        assert!(
            matches!(
                &error,
                StoreError::Integrity(message)
                    if message.contains("commitment lineage is invalid")
            ),
            "{error}"
        );
    }

    #[test]
    fn approved_paper_reservation_rejects_mismatched_proposal_and_keeps_store_atomic() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
            .unwrap()
            .unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        let workflow = WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        };
        let proposal_payload = WorkflowProposal {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: run.topology_id.clone(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    objective: "analyze".to_owned(),
                    depends_on: vec![],
                    priority: 50,
                    evidence_needs: vec![],
                },
            )]),
            stop_reason: Some("fixture".to_owned()),
        };
        let mut proposal = artifact(
            &store,
            ArtifactKind::WorkflowProposal,
            &serde_json::to_string(&proposal_payload).unwrap(),
            Some(ArtifactOrigin {
                run_id: Some(run.run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
        );
        proposal.producer = "runtime.paper_provisioning".to_owned();
        proposal.lifecycle = ArtifactLifecycle::RunScoped;
        let reservation = SessionReservation {
            session_key: "2026-08-12".to_owned(),
            workflow,
            setup_artifacts: vec![],
            reserved_at: now,
        };
        let mut wrong_proposal = proposal.clone();
        wrong_proposal.origin = Some(ArtifactOrigin {
            run_id: Some(RunId::new()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        });
        assert!(matches!(
            store.reserve_paper_session_with_proposal(&lease, &reservation, &wrong_proposal),
            Err(StoreError::InvalidSessionSlot(_))
        ));
        assert!(store.session_slot("2026-08-12").unwrap().is_none());
        assert!(matches!(
            store.run_purpose(&run.run_id),
            Err(StoreError::MissingRun(_))
        ));
        store.verify_integrity().unwrap();
    }

    #[test]
    fn approved_paper_reservation_rejects_source_closure_mismatch_atomically() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
            .unwrap()
            .unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        let workflow = WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        };
        let setup = artifact(
            &store,
            ArtifactKind::EvidenceNeed,
            "{}",
            Some(ArtifactOrigin {
                run_id: Some(run.run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
        );
        let proposal_payload = WorkflowProposal {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: run.topology_id.clone(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    objective: "analyze".to_owned(),
                    depends_on: vec![],
                    priority: 50,
                    evidence_needs: vec![],
                },
            )]),
            stop_reason: Some("fixture".to_owned()),
        };
        let mut proposal = artifact_with_refs(
            &store,
            ArtifactKind::WorkflowProposal,
            &serde_json::to_string(&proposal_payload).unwrap(),
            Some(ArtifactOrigin {
                run_id: Some(run.run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
        );
        proposal.producer = "runtime.paper_provisioning".to_owned();
        proposal.artifact_id = ArtifactId(proposal.expected_hash().unwrap());
        let reservation = SessionReservation {
            session_key: "2026-08-12-source-closure".to_owned(),
            workflow,
            setup_artifacts: vec![setup],
            reserved_at: now,
        };
        assert!(matches!(
            store.reserve_paper_session_with_proposal(&lease, &reservation, &proposal),
            Err(StoreError::InvalidWorkflowProposalArtifact)
        ));
        assert!(store
            .session_slot("2026-08-12-source-closure")
            .unwrap()
            .is_none());
        assert!(matches!(
            store.run_purpose(&run.run_id),
            Err(StoreError::MissingRun(_))
        ));
        store.verify_integrity().unwrap();
    }

    #[test]
    fn approved_paper_reservation_is_idempotent_for_duplicate_session() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
            .unwrap()
            .unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        let workflow = WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        };
        let proposal_payload = WorkflowProposal {
            schema_version: V2_SCHEMA_VERSION,
            topology_id: run.topology_id.clone(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    objective: "analyze".to_owned(),
                    depends_on: vec![],
                    priority: 50,
                    evidence_needs: vec![],
                },
            )]),
            stop_reason: Some("fixture".to_owned()),
        };
        let mut proposal = artifact(
            &store,
            ArtifactKind::WorkflowProposal,
            &serde_json::to_string(&proposal_payload).unwrap(),
            Some(ArtifactOrigin {
                run_id: Some(run.run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
        );
        proposal.producer = "runtime.paper_provisioning".to_owned();
        proposal.artifact_id = ArtifactId(proposal.expected_hash().unwrap());
        let reservation = SessionReservation {
            session_key: "2026-08-12".to_owned(),
            workflow,
            setup_artifacts: vec![],
            reserved_at: now,
        };
        let first = store
            .reserve_paper_session_with_proposal(&lease, &reservation, &proposal)
            .unwrap();
        let second = store
            .reserve_paper_session_with_proposal(&lease, &reservation, &proposal)
            .unwrap();
        assert!(first.newly_reserved);
        assert!(!second.newly_reserved);
        assert_eq!(
            first.slot.workflow.run.run_id,
            second.slot.workflow.run.run_id
        );
        let successor = store
            .acquire_daemon_lease(
                "scheduler",
                "daemon-b",
                now + Duration::seconds(31),
                now + Duration::seconds(61),
            )
            .unwrap()
            .unwrap();
        assert_eq!(successor.epoch, lease.epoch + 1);
        assert!(matches!(
            store.reserve_paper_session_with_proposal(&lease, &reservation, &proposal),
            Err(StoreError::SchedulerFenced(_))
        ));
        store.verify_integrity().unwrap();
    }

    #[test]
    fn session_slot_is_fenced_and_reuses_the_frozen_workflow() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let first_lease = store
            .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
            .unwrap()
            .unwrap();

        let first_graph = graph();
        let first_graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&first_graph).unwrap(),
            None,
        );
        let first_workflow = WorkflowCommit {
            run: StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: first_graph.topology_id.clone(),
                graph_artifact_id: first_graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: first_graph_artifact,
            nodes: first_graph.nodes,
        };
        let first = store
            .reserve_session_slot(
                &first_lease,
                &SessionReservation {
                    session_key: "paper:fixture-a".to_owned(),
                    workflow: first_workflow.clone(),
                    setup_artifacts: vec![],
                    reserved_at: now,
                },
            )
            .unwrap();
        assert!(first.newly_reserved);

        let mut replacement_graph = graph();
        replacement_graph.nodes[0].objective = "replacement plan".to_owned();
        let replacement_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&replacement_graph).unwrap(),
            None,
        );
        let replacement_workflow = WorkflowCommit {
            run: StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: replacement_graph.topology_id.clone(),
                graph_artifact_id: replacement_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: replacement_artifact,
            nodes: replacement_graph.nodes,
        };
        let duplicate = store
            .reserve_session_slot(
                &first_lease,
                &SessionReservation {
                    session_key: "paper:fixture-a".to_owned(),
                    workflow: replacement_workflow.clone(),
                    setup_artifacts: vec![],
                    reserved_at: now,
                },
            )
            .unwrap();
        assert!(!duplicate.newly_reserved);
        assert_eq!(
            duplicate.slot.workflow.run.run_id,
            first_workflow.run.run_id
        );
        assert_eq!(
            duplicate.slot.workflow.graph.artifact_id,
            first_workflow.graph.artifact_id
        );

        let claimed = store
            .claim_next_task("execution-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let commitment =
            valid_execution_commitment(&store, &claimed.permit, "paper:fixture-a", now);
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch(
                    "CREATE TRIGGER fail_execution_task_completion BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'task.succeeded' \
                     BEGIN SELECT RAISE(ABORT, 'injected execution completion event failure'); END;",
                )
                .unwrap();
        }
        assert!(matches!(
            store.commit_execution(
                &first_lease,
                &ExecutionCommit {
                    session_key: "paper:fixture-a".to_owned(),
                    permit: claimed.permit.clone(),
                    commitment: commitment.clone(),
                    committed_at: now,
                },
            ),
            Err(StoreError::Sql(_))
        ));
        assert_eq!(
            store
                .session_slot("paper:fixture-a")
                .unwrap()
                .unwrap()
                .commitment_artifact_id,
            None
        );
        assert!(matches!(
            store.artifact(&commitment.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert!(store
            .events_after(&claimed.permit.run_id, 0, 20)
            .unwrap()
            .iter()
            .all(|event| event.event_type != "execution.committed"));
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch("DROP TRIGGER fail_execution_task_completion;")
                .unwrap();
        }
        let committed = store
            .commit_execution(
                &first_lease,
                &ExecutionCommit {
                    session_key: "paper:fixture-a".to_owned(),
                    permit: claimed.permit.clone(),
                    commitment: commitment.clone(),
                    committed_at: now,
                },
            )
            .unwrap();
        assert!(committed.newly_committed);
        let outputs = store
            .committed_task_outputs(&claimed.permit.run_id, &claimed.permit.task_id)
            .unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].artifact_id, commitment.artifact_id);
        assert!(matches!(
            store.commit_execution(
                &first_lease,
                &ExecutionCommit {
                    session_key: "paper:fixture-a".to_owned(),
                    permit: claimed.permit.clone(),
                    commitment: commitment.clone(),
                    committed_at: now,
                },
            ),
            Err(StoreError::StalePermit(_))
        ));
        let events = store.events_after(&claimed.permit.run_id, 0, 20).unwrap();
        assert!(events.iter().any(|event| {
            event.event_type == "execution.committed"
                && event.artifact_id.as_ref() == Some(&commitment.artifact_id)
        }));
        assert!(events.iter().any(|event| {
            event.event_type == "task.succeeded"
                && event.task_id.as_ref() == Some(&claimed.permit.task_id)
                && event.attempt_id.as_ref() == Some(&claimed.permit.attempt_id)
                && event.artifact_id.as_ref() == Some(&commitment.artifact_id)
        }));
        assert_eq!(
            store
                .session_slot("paper:fixture-a")
                .unwrap()
                .unwrap()
                .commitment_artifact_id,
            Some(commitment.artifact_id.clone())
        );
        store.verify_integrity().unwrap();

        let successor_now = now + Duration::seconds(31);
        let successor = store
            .acquire_daemon_lease(
                "scheduler",
                "daemon-b",
                successor_now,
                successor_now + Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        assert_eq!(successor.epoch, first_lease.epoch + 1);
        assert!(matches!(
            store.commit_execution(
                &first_lease,
                &ExecutionCommit {
                    session_key: "paper:fixture-a".to_owned(),
                    permit: claimed.permit.clone(),
                    commitment,
                    committed_at: successor_now,
                },
            ),
            Err(StoreError::SchedulerFenced(_))
        ));
        assert!(matches!(
            store.reserve_session_slot(
                &first_lease,
                &SessionReservation {
                    session_key: "paper:fixture-b".to_owned(),
                    workflow: replacement_workflow,
                    setup_artifacts: vec![],
                    reserved_at: successor_now,
                },
            ),
            Err(StoreError::SchedulerFenced(_))
        ));
    }

    #[test]
    fn doctor_rejects_a_corrupt_session_slot() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
            .unwrap()
            .unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        store
            .reserve_session_slot(
                &lease,
                &SessionReservation {
                    session_key: "paper:fixture-corrupt".to_owned(),
                    workflow: WorkflowCommit {
                        run: StoredRun {
                            run_id: RunId::new(),
                            purpose: RunPurpose::Paper,
                            topology_id: graph.topology_id.clone(),
                            graph_artifact_id: graph_artifact.artifact_id.clone(),
                            created_at: now,
                        },
                        graph: graph_artifact,
                        nodes: graph.nodes,
                    },
                    setup_artifacts: vec![],
                    reserved_at: now,
                },
            )
            .unwrap();
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute(
                    "UPDATE rebuild_session_slots SET topology_id = 'corrupt' WHERE session_key = ?1",
                    params!["paper:fixture-corrupt"],
                )
                .unwrap();
        }
        assert!(matches!(
            store.verify_integrity(),
            Err(StoreError::Integrity(message)) if message.contains("topology mismatch")
        ));
    }

    #[test]
    fn policy_transition_is_atomic_with_learning_artifacts_and_terminal_event() {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let mut graph = graph();
        let seed = graph.nodes[0].clone();
        let mut evaluation_node = seed.clone();
        evaluation_node.task_id = TaskId::new();
        evaluation_node.dependencies = vec![seed.task_id.clone()];
        graph.nodes = vec![seed, evaluation_node];
        graph.validate().unwrap();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let seed_permit = store
            .claim_next_task("seed-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;

        let make_artifact = |permit: &TaskWritePermit,
                             kind: ArtifactKind,
                             payload: serde_json::Value,
                             source_refs: Vec<ArtifactRef>,
                             lifecycle: ArtifactLifecycle| {
            Artifact::new(
                kind,
                store.put_json(&payload).unwrap(),
                "fixture",
                lifecycle,
                ArtifactProvenance {
                    source_family: "fixture".to_owned(),
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
                source_refs,
                now,
            )
            .unwrap()
        };
        let reference = |artifact: &Artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        };
        let raw = make_artifact(
            &seed_permit,
            ArtifactKind::RawEvidence,
            serde_json::json!({"raw": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
        );
        let normalized = make_artifact(
            &seed_permit,
            ArtifactKind::NormalizedEvidence,
            serde_json::json!({"normalized": true}),
            vec![reference(&raw)],
            ArtifactLifecycle::RunScoped,
        );
        let execution_context = make_artifact(
            &seed_permit,
            ArtifactKind::ExecutionContext,
            serde_json::json!({"execution": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
        );
        let decision = make_artifact(
            &seed_permit,
            ArtifactKind::Decision,
            serde_json::json!({"decision": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
        );
        let decision_context = make_artifact(
            &seed_permit,
            ArtifactKind::DecisionContext,
            serde_json::json!({"context": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
        );
        let verdict_payload = ExecutionVerdict::NoOrder {
            no_order: akzio_domain::NoOrder {
                execution_context: reference(&execution_context),
                blockers: vec![akzio_domain::HardBlocker::Frozen],
                created_at: now,
            },
        };
        let verdict = make_artifact(
            &seed_permit,
            ArtifactKind::ExecutionVerdict,
            serde_json::to_value(&verdict_payload).unwrap(),
            vec![reference(&execution_context)],
            ArtifactLifecycle::RunScoped,
        );
        let outcome_id = akzio_domain::OutcomeId::new();
        let schedule_payload = OutcomeSchedule {
            schema_version: V2_SCHEMA_VERSION,
            outcome_id: outcome_id.clone(),
            decision: reference(&decision),
            decision_context: reference(&decision_context),
            execution_context: reference(&execution_context),
            execution: OutcomeExecutionLineage::NoOrder {
                execution_verdict: reference(&verdict),
            },
            baseline_trading_day: now.date_naive(),
            created_at: now,
        };
        let schedule = make_artifact(
            &seed_permit,
            ArtifactKind::OutcomeSchedule,
            serde_json::to_value(&schedule_payload).unwrap(),
            vec![
                schedule_payload.decision.clone(),
                schedule_payload.decision_context.clone(),
                schedule_payload.execution_context.clone(),
                reference(&verdict),
            ],
            ArtifactLifecycle::Canonical,
        );
        store
            .commit_attempt(
                &seed_permit,
                &[
                    raw,
                    normalized.clone(),
                    execution_context.clone(),
                    decision.clone(),
                    decision_context.clone(),
                    verdict.clone(),
                    schedule.clone(),
                ],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();

        let evaluation_permit = store
            .claim_next_task("evaluation-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let execution_ref = reference(&execution_context);
        let evidence_ref = reference(&normalized);
        let outcome_payload = Outcome {
            schema_version: V2_SCHEMA_VERSION,
            outcome_id,
            schedule: reference(&schedule),
            market_evidence: vec![evidence_ref.clone()],
            windows: [
                akzio_domain::OutcomeHorizon::T1,
                akzio_domain::OutcomeHorizon::T3,
                akzio_domain::OutcomeHorizon::T5,
            ]
            .into_iter()
            .map(|horizon| akzio_domain::OutcomeWindow {
                horizon,
                observed_trading_day: now.date_naive()
                    + chrono::Days::new(u64::from(horizon.trading_days())),
                portfolio_return_ppm: 1,
                benchmark_return_ppm: 0,
                transaction_cost_ppm: 0,
                slippage_ppm: 0,
                utility_ppm: 1,
                calibration_ppm: 1_000_000,
                evidence_completeness_ppm: 1_000_000,
                risk_recall_ppm: 1_000_000,
            })
            .collect(),
            sealed_at: Some(now),
        };
        let outcome = make_artifact(
            &evaluation_permit,
            ArtifactKind::Outcome,
            serde_json::to_value(&outcome_payload).unwrap(),
            vec![reference(&schedule), evidence_ref],
            ArtifactLifecycle::Canonical,
        );
        let outcome_ref = reference(&outcome);
        let subject = PolicySubject::Memory(akzio_domain::MemoryId::new());
        let experience_payload = Experience {
            schema_version: V2_SCHEMA_VERSION,
            experience_id: akzio_domain::ExperienceId::new(),
            subject: subject.clone(),
            hypothesis_id: "fixture".to_owned(),
            decision: reference(&decision),
            decision_context: reference(&decision_context),
            execution_context: execution_ref.clone(),
            policy_verdict: reference(&verdict),
            outcome: outcome_ref.clone(),
            contract_hash: ContentHash::of_bytes(b"fixture-contract"),
            topology_id: akzio_domain::TopologyId("fixture-topology".to_owned()),
            policy_state: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
            created_at: now,
        };
        let experience = make_artifact(
            &evaluation_permit,
            ArtifactKind::Experience,
            serde_json::to_value(&experience_payload).unwrap(),
            vec![
                experience_payload.decision.clone(),
                experience_payload.decision_context.clone(),
                experience_payload.execution_context.clone(),
                experience_payload.policy_verdict.clone(),
                experience_payload.outcome.clone(),
            ],
            ArtifactLifecycle::Canonical,
        );
        let experience_ref = reference(&experience);
        let evaluation_payload = Evaluation {
            schema_version: V2_SCHEMA_VERSION,
            evaluation_id: akzio_domain::EvaluationId::new(),
            outcome: outcome_ref.clone(),
            experience: experience_ref.clone(),
            marginal_utility_ppm: 1,
            token_cost: 1,
            latency_millis: 1,
            created_at: now,
        };
        let evaluation = make_artifact(
            &evaluation_permit,
            ArtifactKind::Evaluation,
            serde_json::to_value(&evaluation_payload).unwrap(),
            vec![outcome_ref, experience_ref],
            ArtifactLifecycle::Canonical,
        );
        let pair_snapshot = store.policy_shadow_pair_snapshot(&subject).unwrap();
        let commit = PolicyEvaluationCommit {
            permit: evaluation_permit,
            outcome: outcome.clone(),
            experience,
            evaluation: evaluation.clone(),
            candidate_policy: None,
            subject: subject.clone(),
            from: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
            to: PolicyState::Memory(akzio_domain::MemoryLifecycle::Active),
            pair_snapshot,
            transition: Some(PolicyTransition {
                schema_version: V2_SCHEMA_VERSION,
                transition_id: PolicyTransitionId::new(),
                subject: subject.clone(),
                from: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
                to: PolicyState::Memory(akzio_domain::MemoryLifecycle::Active),
                evaluation: reference(&evaluation),
                created_at: now,
            }),
            completed_at: now,
        };
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch(
                    "CREATE TRIGGER fail_policy_event BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'policy.transitioned' \
                     BEGIN SELECT RAISE(ABORT, 'injected policy event failure'); END;",
                )
                .unwrap();
        }
        let failed = store.record_policy_evaluation(&commit);
        assert!(
            matches!(&failed, Err(StoreError::Sql(_))),
            "unexpected policy transition result: {failed:?}"
        );
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch("DROP TRIGGER fail_policy_event;")
                .unwrap();
        }
        assert!(store.policy_head(&subject).unwrap().is_none());
        assert!(matches!(
            store.artifact(&outcome.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert!(store
            .events_after(&run.run_id, 0, 100)
            .unwrap()
            .iter()
            .all(|event| event.event_type != "policy.transitioned"));

        let recorded = store.record_policy_evaluation(&commit).unwrap();
        assert!(recorded.newly_recorded);
        assert!(recorded.policy_head.is_some());
        assert_eq!(store.policy_transitions(&subject).unwrap().len(), 1);
        store.verify_integrity().unwrap();

        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE rebuild_policy_consumption_heads \
                 SET consumed_pair_cursor = 999 WHERE subject_id = ?1",
                params![subject.subject_id()],
            )
            .unwrap();
        let corrupted = store.verify_integrity();
        assert!(
            matches!(&corrupted, Err(StoreError::Integrity(_))),
            "unexpected Doctor result after policy cursor corruption: {corrupted:?}"
        );
    }

    #[test]
    fn generic_learning_artifacts_require_specialized_atomic_apis() {
        let fixture = PolicyCommitFixture::memory();
        let candidate_policy = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::CandidatePolicy,
            &serde_json::json!({"candidate": true}),
            vec![],
            ArtifactLifecycle::Canonical,
            fixture.now,
        );

        for protected in [
            fixture.outcome.clone(),
            fixture.experience.clone(),
            fixture.evaluation.clone(),
            candidate_policy,
        ] {
            assert!(matches!(
                fixture.store.write_task_artifact(
                    &fixture.permit,
                    &protected,
                    LifecycleEventType::FixtureGenericWrite,
                    fixture.now,
                ),
                Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.atomic_commit_required"
                ))
            ));
            assert!(matches!(
                fixture.store.commit_attempt(
                    &fixture.permit,
                    &[protected],
                    TaskStatus::Succeeded,
                    fixture.now,
                ),
                Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.atomic_commit_required"
                ))
            ));
        }
    }

    #[test]
    fn old_v7_policy_evaluation_shape_is_rejected() {
        let root = tempdir().unwrap();
        let database = root.path().join(DATABASE_FILE);
        let connection = Connection::open(database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE rebuild_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO rebuild_metadata (key, value) VALUES ('schema_version', '7');
                 CREATE TABLE rebuild_policy_evaluations (
                    evaluation_artifact_id TEXT PRIMARY KEY,
                    subject_id TEXT NOT NULL,
                    outcome_artifact_id TEXT NOT NULL,
                    experience_artifact_id TEXT NOT NULL
                 );",
            )
            .unwrap();
        drop(connection);

        assert!(matches!(
            V2Store::open(root.path()),
            Err(StoreError::IncompatibleStoreRoot(path)) if path == root.path()
        ));
    }

    #[test]
    fn policy_snapshot_does_not_consume_pairs_completed_after_cutoff() {
        let fixture = PolicyCommitFixture::memory();
        let first_cursor =
            fixture.insert_pair("snapshot-before-cutoff", OutcomeHorizon::T1, fixture.now);
        let snapshot = fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap();
        assert_eq!(snapshot.through_cursor, first_cursor);
        assert_eq!(snapshot.counts_by_horizon, [1, 0, 0]);

        let second_cursor = fixture.insert_pair(
            "snapshot-after-cutoff",
            OutcomeHorizon::T3,
            fixture.now + Duration::seconds(1),
        );
        let recorded = fixture
            .store
            .record_policy_evaluation(&fixture.commit(snapshot))
            .unwrap();
        assert_eq!(recorded.consumed_pair_cursor, first_cursor);

        let remaining = fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap();
        assert_eq!(remaining.after_cursor, first_cursor);
        assert_eq!(remaining.through_cursor, second_cursor);
        assert_eq!(remaining.counts_by_horizon, [0, 1, 0]);
    }

    #[test]
    fn doctor_rejects_candidate_reverse_binding_corruption() {
        let fixture = PolicyCommitFixture::topology();
        let commit = fixture.commit(
            fixture
                .store
                .policy_shadow_pair_snapshot(&fixture.subject)
                .unwrap(),
        );
        fixture.store.record_policy_evaluation(&commit).unwrap();
        fixture.store.verify_integrity().unwrap();

        let original = commit.candidate_policy.as_ref().unwrap();
        let forged = Artifact::new(
            ArtifactKind::CandidatePolicy,
            original.blob.clone(),
            "fixture.policy.reverse-corruption",
            ArtifactLifecycle::Canonical,
            original.provenance.clone(),
            original.origin.clone(),
            original.source_refs.clone(),
            original.created_at + Duration::microseconds(1),
        )
        .unwrap();
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            insert_artifact(&transaction, &forged).unwrap();
            transaction
                .execute(
                    "UPDATE rebuild_policy_evaluations
                     SET candidate_policy_artifact_id = ?1
                     WHERE evaluation_artifact_id = ?2",
                    params![
                        forged.artifact_id.0.as_str(),
                        fixture.evaluation.artifact_id.0.as_str(),
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
        }

        match fixture.store.verify_integrity() {
            Err(StoreError::Integrity(_)) => {}
            other => panic!("unexpected Doctor result: {other:?}"),
        }
    }

    #[test]
    fn doctor_rejects_no_order_schedule_with_accepted_verdict() {
        let fixture = PolicyCommitFixture::memory();
        fixture.store.verify_integrity().unwrap();
        let schedule = fixture
            .store
            .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
            .unwrap()
            .unwrap();
        let execution_context = fixture
            .store
            .latest_artifact_by_kind(ArtifactKind::ExecutionContext)
            .unwrap()
            .unwrap();
        let accepted_verdict = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ExecutionVerdict,
            &ExecutionVerdict::Accepted {
                execution_context: artifact_ref(&execution_context),
            },
            vec![artifact_ref(&execution_context)],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        let mut payload: OutcomeSchedule =
            serde_json::from_slice(&fixture.store.read_blob(&schedule.blob).unwrap()).unwrap();
        payload.execution = OutcomeExecutionLineage::NoOrder {
            execution_verdict: artifact_ref(&accepted_verdict),
        };
        let forged_schedule = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::OutcomeSchedule,
            &payload,
            outcome_schedule_source_refs(&payload),
            ArtifactLifecycle::Canonical,
            fixture.now,
        );
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection.transaction().unwrap();
            insert_artifact(&transaction, &accepted_verdict).unwrap();
            insert_artifact(&transaction, &forged_schedule).unwrap();
            transaction.commit().unwrap();
        }

        assert!(matches!(
            fixture.store.verify_integrity(),
            Err(StoreError::Integrity(message)) if message.contains("execution lineage")
        ));
    }

    #[test]
    fn doctor_rejects_stale_policy_head() {
        let fixture = PolicyCommitFixture::memory();
        let commit = fixture.commit(
            fixture
                .store
                .policy_shadow_pair_snapshot(&fixture.subject)
                .unwrap(),
        );
        fixture.store.record_policy_evaluation(&commit).unwrap();
        fixture.store.verify_integrity().unwrap();

        let stale_transition = PolicyTransition {
            transition_id: PolicyTransitionId::new(),
            created_at: fixture.now + Duration::seconds(1),
            ..fixture.transition.clone()
        };
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let event_cursor = append_event(
                &transaction,
                &fixture.run.run_id,
                Some(&fixture.permit.task_id),
                Some(&fixture.permit.attempt_id),
                LifecycleEventType::PolicyTransitioned,
                Some(&fixture.evaluation.artifact_id),
                stale_transition.created_at,
            )
            .unwrap();
            transaction
                .execute(
                    r#"INSERT INTO rebuild_policy_transitions
                       (transition_id, subject_id, subject_json, from_state_json, to_state_json,
                        evaluation_artifact_id, run_id, revision, created_at, event_cursor)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
                    params![
                        stale_transition.transition_id.0,
                        fixture.subject.subject_id(),
                        serde_json::to_string(&fixture.subject).unwrap(),
                        serde_json::to_string(&stale_transition.from).unwrap(),
                        serde_json::to_string(&stale_transition.to).unwrap(),
                        fixture.evaluation.artifact_id.0.as_str(),
                        fixture.run.run_id.0,
                        2_u64,
                        stale_transition.created_at.to_rfc3339(),
                        event_cursor,
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
        }

        let corrupted = fixture.store.verify_integrity();
        assert!(matches!(
            &corrupted,
            Err(StoreError::Integrity(message)) if message.contains("stale")
        ));
    }

    #[test]
    fn paper_effect_events_require_complete_lineage_at_append_boundary() {
        let fixture = execution_commit_fixture();
        let event_types = [
            LifecycleEventType::ExecutionEffectIntent,
            LifecycleEventType::ExecutionEffectRecovered,
            LifecycleEventType::ExecutionEffectSettled,
        ];
        let cases = [
            (
                None,
                Some(&fixture.permit.attempt_id),
                Some(&fixture.commitment.artifact_id),
            ),
            (
                Some(&fixture.permit.task_id),
                None,
                Some(&fixture.commitment.artifact_id),
            ),
            (
                Some(&fixture.permit.task_id),
                Some(&fixture.permit.attempt_id),
                None,
            ),
        ];

        for lifecycle_type in event_types {
            for (task_id, attempt_id, artifact_id) in cases {
                let mut connection = fixture.store.connection.lock().unwrap();
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                assert!(matches!(
                    append_event(
                        &transaction,
                        &fixture.permit.run_id,
                        task_id,
                        attempt_id,
                        lifecycle_type,
                        artifact_id,
                        fixture.now,
                    ),
                    Err(StoreError::InvalidLifecycleEventShape { event_type: value })
                    if value == lifecycle_type.as_str()
                ));
            }
        }
    }

    #[test]
    fn lifecycle_event_shapes_accept_current_store_exceptions() {
        let fixture = execution_commit_fixture();
        let valid_cases = [
            (LifecycleEventType::WorkflowCreated, false, false, true),
            (LifecycleEventType::RunCancelRequested, false, false, false),
            (LifecycleEventType::OutcomeWorkerEnqueued, true, false, true),
            (LifecycleEventType::TaskCancelled, true, false, false),
            (LifecycleEventType::TaskStarted, true, true, false),
            (LifecycleEventType::TaskRetryScheduled, true, true, false),
            (LifecycleEventType::TaskSucceeded, true, true, true),
            (LifecycleEventType::ArtifactCommitted, true, true, true),
            (LifecycleEventType::ExecutionCommitted, true, true, true),
            (LifecycleEventType::PolicyEvaluated, true, true, true),
            (LifecycleEventType::ShadowPairCompleted, true, true, true),
        ];

        for (event_type, has_task_id, has_attempt_id, has_artifact_id) in valid_cases {
            assert!(
                validate_event_shape(event_type, has_task_id, has_attempt_id, has_artifact_id)
                    .is_ok(),
                "unexpectedly rejected {:?}",
                event_type
            );
        }

        let invalid_cases = [
            (LifecycleEventType::WorkflowCreated, true, false, true),
            (LifecycleEventType::RunCancelRequested, false, false, true),
            (LifecycleEventType::OutcomeWorkerEnqueued, true, true, true),
            (LifecycleEventType::TaskStarted, true, false, false),
            (LifecycleEventType::ArtifactCommitted, true, false, true),
        ];

        for (event_type, has_task_id, has_attempt_id, has_artifact_id) in invalid_cases {
            assert!(
                matches!(
                    validate_event_shape(event_type, has_task_id, has_attempt_id, has_artifact_id),
                    Err(StoreError::InvalidLifecycleEventShape { event_type: value })
                if value == event_type.as_str()
                ),
                "unexpectedly accepted {:?}",
                event_type
            );
        }

        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(matches!(
            append_event(
                &transaction,
                &fixture.permit.run_id,
                None,
                Some(&fixture.permit.attempt_id),
                LifecycleEventType::ArtifactCommitted,
                None,
                fixture.now,
            ),
            Err(StoreError::Domain(DomainError::AttemptOriginWithoutTask))
        ));
    }

    #[test]
    fn doctor_rejects_forged_paper_effect_event_shape() {
        let fixture = execution_commit_fixture();
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            transaction
                .execute(
                    r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, NULL, NULL, ?2, NULL, ?3)"#,
                    params![
                        fixture.permit.run_id.0,
                        LifecycleEventType::ExecutionEffectIntent.as_str(),
                        fixture.now.to_rfc3339(),
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
        }

        assert!(matches!(
            fixture.store.verify_integrity(),
            Err(StoreError::Integrity(message))
                if message.contains("invalid shape")
                    && message.contains("execution.effect.intent")
        ));
    }

    #[test]
    fn events_after_rejects_forged_paper_effect_event_shape() {
        let fixture = execution_commit_fixture();
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            transaction
                .execute(
                    r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, NULL, NULL, ?2, NULL, ?3)"#,
                    params![
                        fixture.permit.run_id.0,
                        LifecycleEventType::ExecutionEffectSettled.as_str(),
                        fixture.now.to_rfc3339(),
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
        }

        assert!(matches!(
            fixture.store.events_after(&fixture.permit.run_id, 0, 100),
            Err(StoreError::InvalidLifecycleEventShape { event_type })
                if event_type == LifecycleEventType::ExecutionEffectSettled.as_str()
        ));
    }

    fn insert_paper_effect_event(
        fixture: &ExecutionCommitFixture,
        effect: &ArtifactRef,
        event_type: LifecycleEventType,
    ) {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                   (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                params![
                    fixture.permit.run_id.0,
                    fixture.permit.task_id.0,
                    fixture.permit.attempt_id.0,
                    event_type.as_str(),
                    effect.artifact_id.0.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    #[test]
    fn paper_effect_history_requires_prior_intent_and_single_terminal() {
        let fixture = execution_commit_fixture();
        let effect = artifact_ref(&fixture.commitment);
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &fixture.commitment,
                LifecycleEventType::ExecutionCommitted,
                fixture.now,
            )
            .unwrap();
        insert_paper_effect_event(
            &fixture,
            &effect,
            LifecycleEventType::ExecutionEffectSettled,
        );
        assert!(matches!(
            fixture.store.events_after(&fixture.permit.run_id, 0, 100),
            Err(StoreError::Integrity(message))
                if message.contains("has no prior intent")
        ));

        let fixture = execution_commit_fixture();
        let effect = artifact_ref(&fixture.commitment);
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &fixture.commitment,
                LifecycleEventType::ExecutionCommitted,
                fixture.now,
            )
            .unwrap();
        insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
        insert_paper_effect_event(
            &fixture,
            &effect,
            LifecycleEventType::ExecutionEffectSettled,
        );
        insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
        assert!(matches!(
            fixture.store.verify_integrity(),
            Err(StoreError::Integrity(message))
                if message.contains("intent after terminal")
        ));

        let fixture = execution_commit_fixture();
        let effect = artifact_ref(&fixture.commitment);
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &fixture.commitment,
                LifecycleEventType::ExecutionCommitted,
                fixture.now,
            )
            .unwrap();
        insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
        insert_paper_effect_event(
            &fixture,
            &effect,
            LifecycleEventType::ExecutionEffectSettled,
        );
        insert_paper_effect_event(
            &fixture,
            &effect,
            LifecycleEventType::ExecutionEffectRecovered,
        );
        assert!(matches!(
            fixture.store.verify_integrity(),
            Err(StoreError::Integrity(message))
                if message.contains("duplicate terminal event")
        ));
    }

    #[test]
    fn tool_lifecycle_allows_completed_call_and_blocks_pending_success() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let call = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ToolCall,
            &serde_json::json!({"call_id": "fixture-call"}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &call,
                LifecycleEventType::ToolCalled,
                fixture.now,
            )
            .unwrap();
        let result = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ToolResult,
            &serde_json::json!({"call_id": "fixture-call", "ok": true}),
            vec![artifact_ref(&call)],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &result,
                LifecycleEventType::ToolCompleted,
                fixture.now,
            )
            .unwrap();
        let output = lifecycle_test_artifact(&fixture, ArtifactLifecycle::RunScoped, "output");
        fixture
            .store
            .commit_attempt(
                &fixture.permit,
                std::slice::from_ref(&output),
                TaskStatus::Succeeded,
                fixture.now,
            )
            .unwrap();
        fixture.store.verify_integrity().unwrap();

        let pending = task_artifact_fixture(RunPurpose::Debug);
        let pending_call = permit_artifact(
            &pending.store,
            &pending.permit,
            ArtifactKind::ToolCall,
            &serde_json::json!({"call_id": "pending-call"}),
            vec![],
            ArtifactLifecycle::RunScoped,
            pending.now,
        );
        pending
            .store
            .write_task_artifact(
                &pending.permit,
                &pending_call,
                LifecycleEventType::ToolCalled,
                pending.now,
            )
            .unwrap();
        let output =
            lifecycle_test_artifact(&pending, ArtifactLifecycle::RunScoped, "pending-output");
        assert!(matches!(
            pending.store.commit_attempt(
                &pending.permit,
                std::slice::from_ref(&output),
                TaskStatus::Succeeded,
                pending.now,
            ),
            Err(StoreError::Integrity(message)) if message.contains("pending tool calls")
        ));
        assert!(matches!(
            pending.store.artifact(&output.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert!(pending
            .store
            .events_after(&pending.run.run_id, 0, 100)
            .unwrap()
            .iter()
            .all(|event| event.event_type != LifecycleEventType::TaskSucceeded.as_str()));
    }

    #[test]
    fn tool_lifecycle_failure_can_close_pending_call() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let call = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ToolCall,
            &serde_json::json!({"call_id": "failed-call"}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &call,
                LifecycleEventType::ToolCalled,
                fixture.now,
            )
            .unwrap();
        fixture
            .store
            .finish_task(&fixture.permit, TaskStatus::Failed, fixture.now)
            .unwrap();
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn events_after_validates_effect_history_beyond_page() {
        let fixture = execution_commit_fixture();
        let effect = artifact_ref(&fixture.commitment);
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &fixture.commitment,
                LifecycleEventType::ExecutionCommitted,
                fixture.now,
            )
            .unwrap();
        insert_paper_effect_event(
            &fixture,
            &effect,
            LifecycleEventType::ExecutionEffectSettled,
        );
        assert!(fixture
            .store
            .events_after(&fixture.permit.run_id, i64::MAX, 1)
            .is_err());
    }

    #[test]
    fn events_after_rejects_tool_history_beyond_page() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            transaction
                .execute(
                    r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, ?2, ?3, ?4, NULL, ?5)"#,
                    params![
                        fixture.permit.run_id.0,
                        fixture.permit.task_id.0,
                        fixture.permit.attempt_id.0,
                        LifecycleEventType::ToolCalled.as_str(),
                        fixture.now.to_rfc3339(),
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
        }
        assert!(matches!(
            fixture.store.events_after(&fixture.run.run_id, i64::MAX, 1),
            Err(StoreError::Integrity(message)) if message.contains("has no artifact")
        ));
    }

    #[test]
    fn paper_effect_intent_is_idempotent_and_settlement_requires_intent() {
        let fixture = execution_commit_fixture();
        let effect = artifact_ref(&fixture.commitment);
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &fixture.commitment,
                LifecycleEventType::ExecutionCommitted,
                fixture.now,
            )
            .unwrap();

        assert!(matches!(
            fixture.store.commit_fenced_attempt_with_effect(
                &fixture.lease,
                &fixture.permit,
                std::slice::from_ref(&fixture.commitment),
                &effect,
                false,
                fixture.now,
            ),
            Err(StoreError::MissingPaperEffectIntent(_))
        ));

        assert!(!fixture
            .store
            .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now,)
            .unwrap());
        assert!(fixture
            .store
            .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now,)
            .unwrap());

        let intent_count = fixture
            .store
            .events_after(&fixture.permit.run_id, 0, 100)
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
                    && event.artifact_id.as_ref() == Some(&effect.artifact_id)
            })
            .count();
        assert_eq!(intent_count, 1);
    }

    #[test]
    fn paper_effect_settlement_rejects_non_paper_run() {
        let fixture = execution_commit_fixture();
        let effect = artifact_ref(&fixture.commitment);
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &fixture.commitment,
                LifecycleEventType::ExecutionCommitted,
                fixture.now,
            )
            .unwrap();
        fixture
            .store
            .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now)
            .unwrap();
        fixture
            .store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE rebuild_runs SET purpose = ?1 WHERE run_id = ?2",
                params![enum_name(RunPurpose::Debug), fixture.permit.run_id.0],
            )
            .unwrap();

        assert!(matches!(
            fixture.store.commit_fenced_attempt_with_effect(
                &fixture.lease,
                &fixture.permit,
                std::slice::from_ref(&fixture.commitment),
                &effect,
                false,
                fixture.now,
            ),
            Err(StoreError::NonCanonicalLearningPurpose(RunPurpose::Debug))
        ));

        let events = fixture
            .store
            .events_after(&fixture.permit.run_id, 0, 100)
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.artifact_id.as_ref() == Some(&effect.artifact_id))
                .filter(|event| {
                    matches!(
                        event.event_type.as_str(),
                        "execution.effect.intent"
                            | "execution.effect.settled"
                            | "execution.effect.recovered"
                    )
                })
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            vec!["execution.effect.intent"]
        );
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn paper_effect_settlement_rolls_back_and_can_retry_after_failure() {
        let fixture = execution_commit_fixture();
        let effect = artifact_ref(&fixture.commitment);
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &fixture.commitment,
                LifecycleEventType::ExecutionCommitted,
                fixture.now,
            )
            .unwrap();
        fixture
            .store
            .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now)
            .unwrap();
        {
            let connection = fixture.store.connection.lock().unwrap();
            connection
                .execute_batch(
                    "CREATE TRIGGER fail_paper_effect_settlement BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'execution.effect.settled' \
                     BEGIN SELECT RAISE(ABORT, 'injected settlement failure'); END;",
                )
                .unwrap();
        }

        assert!(matches!(
            fixture.store.commit_fenced_attempt_with_effect(
                &fixture.lease,
                &fixture.permit,
                std::slice::from_ref(&fixture.commitment),
                &effect,
                false,
                fixture.now,
            ),
            Err(StoreError::Sql(_))
        ));
        assert!(fixture
            .store
            .events_after(&fixture.permit.run_id, 0, 100)
            .unwrap()
            .iter()
            .all(|event| event.event_type != LifecycleEventType::ExecutionEffectSettled.as_str()));

        {
            let connection = fixture.store.connection.lock().unwrap();
            connection
                .execute_batch("DROP TRIGGER fail_paper_effect_settlement;")
                .unwrap();
        }
        fixture
            .store
            .commit_fenced_attempt_with_effect(
                &fixture.lease,
                &fixture.permit,
                std::slice::from_ref(&fixture.commitment),
                &effect,
                false,
                fixture.now,
            )
            .unwrap();
        assert!(matches!(
            fixture.store.commit_fenced_attempt_with_effect(
                &fixture.lease,
                &fixture.permit,
                std::slice::from_ref(&fixture.commitment),
                &effect,
                false,
                fixture.now,
            ),
            Err(StoreError::StalePermit(_)) | Err(StoreError::PaperEffectAlreadySettled(_))
        ));
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn metrics_are_empty_for_a_new_store() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let metrics = store.metrics(Utc::now()).unwrap();
        assert!(metrics.run_counts.is_empty());
        assert!(metrics.task_counts.is_empty());
        assert!(metrics.attempt_counts.is_empty());
        assert_eq!(metrics.event_count, 0);
        assert_eq!(metrics.active_daemon_leases, 0);
    }

    #[test]
    fn metrics_expose_failed_run_and_attempt_alerts() {
        let metrics = StoreMetrics {
            run_counts: BTreeMap::from([("failed".to_owned(), 2)]),
            task_counts: BTreeMap::new(),
            attempt_counts: BTreeMap::from([("failed".to_owned(), 1)]),
            event_count: 0,
            active_daemon_leases: 0,
        };
        let alerts = metrics.alerts();
        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[0].code, "failed_runs");
        assert_eq!(alerts[1].code, "failed_attempts");
    }

    #[test]
    fn backup_restore_round_trip_runs_store_doctor() {
        let source_directory = tempdir().unwrap();
        let store = V2Store::open(source_directory.path()).unwrap();
        let blob = store.put_bytes(b"backup-fixture", "text/plain").unwrap();

        let backup_parent = tempdir().unwrap();
        let backup_root = backup_parent.path().join("backup");
        let manifest = store.backup_to(&backup_root).unwrap();
        assert_eq!(manifest.blob_count, 1);
        assert_eq!(manifest.blob_bytes, blob.bytes);

        let restore_parent = tempdir().unwrap();
        let restore_root = restore_parent.path().join("restored");
        let restored = V2Store::restore_from(&backup_root, &restore_root).unwrap();
        let restored_blob = restored.read_blob(&blob).unwrap();
        assert_eq!(restored_blob, b"backup-fixture");
        restored.verify_integrity().unwrap();
    }

    fn task_artifact_fixture_with_retry(
        purpose: RunPurpose,
        max_attempts: u8,
    ) -> TaskArtifactFixture {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();
        let mut graph = graph();
        graph.nodes[0].retry.max_attempts = max_attempts;
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose,
            topology_id: graph.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        let permit = store
            .claim_next_task("lifecycle-worker", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        TaskArtifactFixture {
            _root: root,
            store,
            run,
            permit,
            now,
        }
    }

    fn agent_turn_artifact(fixture: &TaskArtifactFixture, label: &str) -> Artifact {
        permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::AgentTurn,
            &serde_json::json!({"label": label}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        )
    }

    #[test]
    fn agent_turn_started_is_durable_and_duplicate_write_rolls_back() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        fixture
            .store
            .append_task_event(
                &fixture.permit,
                LifecycleEventType::AgentTurnStarted,
                fixture.now,
            )
            .unwrap();

        let events = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap();
        assert_eq!(events.len(), 3);
        let started = events
            .iter()
            .find(|event| event.event_type == LifecycleEventType::AgentTurnStarted.as_str())
            .unwrap();
        assert_eq!(started.task_id, Some(fixture.permit.task_id.clone()));
        assert_eq!(started.attempt_id, Some(fixture.permit.attempt_id.clone()));
        assert!(started.artifact_id.is_none());

        assert!(matches!(
            fixture.store.append_task_event(
                &fixture.permit,
                LifecycleEventType::AgentTurnStarted,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));
        let after_duplicate = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap();
        assert_eq!(after_duplicate.len(), events.len());
        assert_eq!(
            after_duplicate
                .iter()
                .filter(|event| event.event_type == LifecycleEventType::AgentTurnStarted.as_str())
                .count(),
            1
        );
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn agent_turn_rejects_distinct_terminal_without_new_start() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        fixture
            .store
            .append_task_event(
                &fixture.permit,
                LifecycleEventType::AgentTurnStarted,
                fixture.now,
            )
            .unwrap();

        let completed = agent_turn_artifact(&fixture, "completed");
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &completed,
                LifecycleEventType::AgentTurnCompleted,
                fixture.now,
            )
            .unwrap();

        let failed = agent_turn_artifact(&fixture, "failed");
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &failed,
                LifecycleEventType::AgentTurnFailed,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));

        let events = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == LifecycleEventType::AgentTurnFailed.as_str())
                .count(),
            0
        );
        assert!(matches!(
            fixture.store.artifact(&failed.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn agent_turn_started_rejects_stale_epoch_without_writing() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let mut stale = fixture.permit.clone();
        stale.epoch += 1;
        let before = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len();

        assert!(matches!(
            fixture.store.append_task_event(
                &stale,
                LifecycleEventType::AgentTurnStarted,
                fixture.now,
            ),
            Err(StoreError::StalePermit(_))
        ));
        assert_eq!(
            fixture
                .store
                .events_after(&fixture.run.run_id, 0, 100)
                .unwrap()
                .len(),
            before
        );
        assert_eq!(
            fixture
                .store
                .workflow_snapshot(&fixture.run.run_id)
                .unwrap()
                .tasks[0]
                .status,
            TaskStatus::Running
        );
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn pending_agent_turn_blocks_success_until_completed() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        fixture
            .store
            .append_task_event(
                &fixture.permit,
                LifecycleEventType::AgentTurnStarted,
                fixture.now,
            )
            .unwrap();

        assert!(matches!(
            fixture
                .store
                .finish_task(&fixture.permit, TaskStatus::Succeeded, fixture.now),
            Err(StoreError::Integrity(_))
        ));
        assert_eq!(
            fixture
                .store
                .workflow_snapshot(&fixture.run.run_id)
                .unwrap()
                .tasks[0]
                .status,
            TaskStatus::Running
        );

        let turn = agent_turn_artifact(&fixture, "completed");
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &turn,
                LifecycleEventType::AgentTurnCompleted,
                fixture.now,
            )
            .unwrap();
        fixture
            .store
            .finish_task(&fixture.permit, TaskStatus::Succeeded, fixture.now)
            .unwrap();
        assert_eq!(
            fixture
                .store
                .workflow_snapshot(&fixture.run.run_id)
                .unwrap()
                .tasks[0]
                .status,
            TaskStatus::Succeeded
        );
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn retry_attempts_close_started_turns_and_preserve_attempt_order() {
        let fixture = task_artifact_fixture_with_retry(RunPurpose::Debug, 2);
        fixture
            .store
            .append_task_event(
                &fixture.permit,
                LifecycleEventType::AgentTurnStarted,
                fixture.now,
            )
            .unwrap();
        assert_eq!(
            fixture
                .store
                .retry_task(&fixture.permit, fixture.now, fixture.now)
                .unwrap(),
            RetryTaskResult::Requeued
        );

        let second = fixture
            .store
            .claim_next_task(
                "lifecycle-worker-2",
                fixture.now + Duration::seconds(1),
                Duration::seconds(30),
            )
            .unwrap()
            .unwrap();
        assert_ne!(fixture.permit.attempt_id, second.permit.attempt_id);
        fixture
            .store
            .append_task_event(
                &second.permit,
                LifecycleEventType::AgentTurnStarted,
                fixture.now + Duration::seconds(1),
            )
            .unwrap();
        let second_fixture = TaskArtifactFixture {
            _root: fixture._root,
            store: fixture.store,
            run: fixture.run,
            permit: second.permit,
            now: fixture.now + Duration::seconds(1),
        };
        let turn = agent_turn_artifact(&second_fixture, "retry-completed");
        second_fixture
            .store
            .write_task_artifact(
                &second_fixture.permit,
                &turn,
                LifecycleEventType::AgentTurnCompleted,
                second_fixture.now,
            )
            .unwrap();
        second_fixture
            .store
            .finish_task(
                &second_fixture.permit,
                TaskStatus::Succeeded,
                second_fixture.now,
            )
            .unwrap();

        let events = second_fixture
            .store
            .events_after(&second_fixture.run.run_id, 0, 100)
            .unwrap();
        let lifecycle: Vec<_> = events
            .iter()
            .filter(|event| {
                matches!(
                    event.lifecycle_kind().unwrap(),
                    LifecycleEventType::AgentTurnStarted
                        | LifecycleEventType::AgentTurnCompleted
                        | LifecycleEventType::TaskRetryScheduled
                        | LifecycleEventType::TaskSucceeded
                )
            })
            .collect();
        assert_eq!(
            lifecycle
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            vec![
                LifecycleEventType::AgentTurnStarted.as_str(),
                LifecycleEventType::TaskRetryScheduled.as_str(),
                LifecycleEventType::AgentTurnStarted.as_str(),
                LifecycleEventType::AgentTurnCompleted.as_str(),
                LifecycleEventType::TaskSucceeded.as_str(),
            ]
        );
        assert_eq!(
            lifecycle[0].attempt_id,
            Some(fixture.permit.attempt_id.clone())
        );
        assert_eq!(
            lifecycle[2].attempt_id,
            Some(second_fixture.permit.attempt_id.clone())
        );
        assert_eq!(
            lifecycle[3].attempt_id,
            Some(second_fixture.permit.attempt_id.clone())
        );
        second_fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn recovery_and_cancel_close_unfinished_agent_turns() {
        let recovered = task_artifact_fixture(RunPurpose::Debug);
        recovered
            .store
            .append_task_event(
                &recovered.permit,
                LifecycleEventType::AgentTurnStarted,
                recovered.now,
            )
            .unwrap();
        assert_eq!(
            recovered
                .store
                .recover_expired_tasks(recovered.now + Duration::seconds(31))
                .unwrap(),
            1
        );
        let recovery_events = recovered
            .store
            .events_after(&recovered.run.run_id, 0, 100)
            .unwrap();
        assert!(recovery_events.iter().any(|event| {
            matches!(
                event.lifecycle_kind().unwrap(),
                LifecycleEventType::TaskRecovered | LifecycleEventType::TaskRecoveryExhausted
            )
        }));
        recovered.store.verify_integrity().unwrap();

        let cancelled = task_artifact_fixture(RunPurpose::Debug);
        cancelled
            .store
            .append_task_event(
                &cancelled.permit,
                LifecycleEventType::AgentTurnStarted,
                cancelled.now,
            )
            .unwrap();
        cancelled
            .store
            .finish_task(&cancelled.permit, TaskStatus::Cancelled, cancelled.now)
            .unwrap();
        assert_eq!(
            cancelled
                .store
                .workflow_snapshot(&cancelled.run.run_id)
                .unwrap()
                .tasks[0]
                .status,
            TaskStatus::Cancelled
        );
        cancelled.store.verify_integrity().unwrap();
    }

    #[test]
    fn context_lifecycle_validator_rejects_wrong_kind_and_preserves_legacy_manifest() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let wrong = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::Decision,
            &serde_json::json!({"wrong": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        let before = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len();
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &wrong,
                LifecycleEventType::ContextManifestCreated,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));
        assert_eq!(
            fixture
                .store
                .events_after(&fixture.run.run_id, 0, 100)
                .unwrap()
                .len(),
            before
        );
        assert!(matches!(
            fixture.store.artifact(&wrong.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));

        let manifest = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ContextManifest,
            &serde_json::json!({"manifest": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &manifest,
                LifecycleEventType::ContextManifest,
                fixture.now,
            )
            .unwrap();
        fixture
            .store
            .commit_attempt(
                &fixture.permit,
                std::slice::from_ref(&manifest),
                TaskStatus::Succeeded,
                fixture.now,
            )
            .unwrap();
        let proof = fixture
            .store
            .current_succeeded_attempt(&fixture.run.run_id, &fixture.permit.task_id)
            .unwrap();
        assert_eq!(
            proof.context_manifest,
            Some(ArtifactRef {
                artifact_id: manifest.artifact_id,
                kind: ArtifactKind::ContextManifest,
            })
        );
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn gate_lifecycle_validator_enforces_event_kind_and_legacy_aliases() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let wrong = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::Decision,
            &serde_json::json!({"wrong": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        let before = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len();
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &wrong,
                LifecycleEventType::ExecutionPlanCreated,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));
        assert_eq!(
            fixture
                .store
                .events_after(&fixture.run.run_id, 0, 100)
                .unwrap()
                .len(),
            before
        );
        assert!(matches!(
            fixture.store.artifact(&wrong.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));

        let context = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ExecutionContext,
            &serde_json::json!({"context": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &context,
                LifecycleEventType::ExecutionContextCreatedLegacy,
                fixture.now,
            )
            .unwrap();

        let verdict = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ExecutionVerdict,
            &serde_json::json!({"verdict": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &verdict,
                LifecycleEventType::ExecutionVerdictCreatedLegacy,
                fixture.now,
            )
            .unwrap();
        fixture.store.verify_integrity().unwrap();
    }

    #[test]
    fn gate_lifecycle_validator_rejects_forged_origin() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let foreign_run = RunId::new();
        let forged = Artifact::new(
            ArtifactKind::ExecutionPlan,
            fixture
                .store
                .put_json(&serde_json::json!({"plan": true}))
                .unwrap(),
            "fixture.plan",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: None,
                retrieved_at: fixture.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: fixture.permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(foreign_run),
                task_id: Some(fixture.permit.task_id.clone()),
                attempt_id: Some(fixture.permit.attempt_id.clone()),
                contract_hash: fixture.permit.contract_hash.clone(),
            }),
            vec![],
            fixture.now,
        )
        .unwrap();
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            insert_artifact(&transaction, &forged).unwrap();
            transaction
                .execute(
                    r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    params![
                        fixture.run.run_id.0,
                        fixture.permit.task_id.0,
                        fixture.permit.attempt_id.0,
                        LifecycleEventType::ExecutionPlanCreated.as_str(),
                        forged.artifact_id.0.as_str(),
                        fixture.now.to_rfc3339(),
                    ],
                )
                .unwrap();
            transaction.commit().unwrap();
        }
        assert!(matches!(
            fixture.store.events_after(&fixture.run.run_id, 0, 100),
            Err(StoreError::Integrity(message))
                if message.contains("origin")
        ));
        assert!(matches!(
            fixture.store.verify_integrity(),
            Err(StoreError::Integrity(message))
                if message.contains("origin")
        ));
    }

    #[test]
    fn context_child_and_repair_lifecycle_validator_enforces_lineage_and_sources() {
        let fixture = task_artifact_fixture(RunPurpose::Debug);
        let parent = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ContextManifest,
            &serde_json::json!({"parent": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &parent,
                LifecycleEventType::ContextManifestCreated,
                fixture.now,
            )
            .unwrap();
        let parent_ref = ArtifactRef {
            artifact_id: parent.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        };

        let missing_parent = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ContextManifest,
            &serde_json::json!({"missing_parent": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &missing_parent,
                LifecycleEventType::ContextChildManifestCreated,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));
        assert!(matches!(
            fixture.store.artifact(&missing_parent.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));

        let foreign_run = RunId::new();
        let foreign_parent = Artifact::new(
            ArtifactKind::ContextManifest,
            fixture
                .store
                .put_json(&serde_json::json!({"foreign": true}))
                .unwrap(),
            "fixture.foreign",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: None,
                retrieved_at: fixture.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: fixture.permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(foreign_run),
                task_id: Some(fixture.permit.task_id.clone()),
                attempt_id: Some(fixture.permit.attempt_id.clone()),
                contract_hash: fixture.permit.contract_hash.clone(),
            }),
            vec![],
            fixture.now,
        )
        .unwrap();
        {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            insert_artifact(&transaction, &foreign_parent).unwrap();
            transaction.commit().unwrap();
        }
        let foreign_child = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ContextManifest,
            &serde_json::json!({"foreign_parent": true}),
            vec![ArtifactRef {
                artifact_id: foreign_parent.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &foreign_child,
                LifecycleEventType::ContextChildManifestCreated,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));
        assert!(matches!(
            fixture.store.artifact(&foreign_child.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));

        let child = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ContextManifest,
            &serde_json::json!({"child": true}),
            vec![parent_ref],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &child,
                LifecycleEventType::ContextChildManifestCreated,
                fixture.now,
            )
            .unwrap();
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &child,
                LifecycleEventType::ContextChildManifestCreated,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));

        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &parent,
                LifecycleEventType::ContextManifest,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));

        let empty_repair = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ContextRepair,
            &serde_json::json!({"empty": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &empty_repair,
                LifecycleEventType::ContextRepaired,
                fixture.now,
            ),
            Err(StoreError::Integrity(_))
        ));
        assert!(matches!(
            fixture.store.artifact(&empty_repair.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));

        let source = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::NormalizedEvidence,
            &serde_json::json!({"source": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &source,
                LifecycleEventType::Evidence,
                fixture.now,
            )
            .unwrap();
        let repair = permit_artifact(
            &fixture.store,
            &fixture.permit,
            ArtifactKind::ContextRepair,
            &serde_json::json!({"repair": true}),
            vec![ArtifactRef {
                artifact_id: source.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            }],
            ArtifactLifecycle::RunScoped,
            fixture.now,
        );
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &repair,
                LifecycleEventType::ContextRepaired,
                fixture.now,
            )
            .unwrap();
        fixture.store.verify_integrity().unwrap();
    }
}
