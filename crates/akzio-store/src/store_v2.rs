//! Store implementation for the source-incompatible Akzio v2 authority.
//!
//! `RebuildStore` deliberately uses a different database filename and metadata
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
    ExecutionContext, ExecutionPlan, ExecutionVerdict, Experience, FailureDisposition, LeaseId,
    OrderReceipt, OrderReceiptState, Outcome, OutcomeExecutionLineage, OutcomeHorizon,
    OutcomeSchedule, PaperCommitment, PaperReprice, PolicyState, PolicySubject, PolicyTransition,
    PolicyTransitionId, Reconciliation, RetryPolicy, RunId, RunPurpose, TaskId, TaskRecipeId,
    TaskStatus, TaskWritePermit, WorkflowGraph, WorkflowNode, WorkflowProposal, WorkflowStatus,
    REBUILD_SCHEMA_VERSION,
};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

const DATABASE_FILE: &str = "akzio.sqlite3";
const LEGACY_DATABASE_FILE: &str = "control.sqlite3";

#[derive(Debug, Error)]
pub enum RebuildStoreError {
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
}

pub type RebuildStoreResult<T> = Result<T, RebuildStoreError>;

#[derive(Debug, Clone)]
pub struct RebuildStore {
    root: Arc<PathBuf>,
    blobs: Arc<PathBuf>,
    connection: Arc<Mutex<Connection>>,
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
pub struct RebuildRun {
    pub run_id: RunId,
    pub purpose: RunPurpose,
    pub topology_id: String,
    pub graph_artifact_id: ArtifactId,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildTask {
    pub run_id: RunId,
    pub node: WorkflowNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowCommit {
    pub run: RebuildRun,
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
    pub run: RebuildRun,
    pub status: WorkflowStatus,
    pub finished_at: Option<DateTime<Utc>>,
    pub revision: WorkflowRevision,
    pub tasks: Vec<StoredTaskSnapshot>,
    pub event_cursor: i64,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedRebuildTask {
    pub run_id: RunId,
    pub node: WorkflowNode,
    pub permit: TaskWritePermit,
}

/// Result of atomically closing a failed attempt. The Store—not a handler—
/// decides whether the retry budget allows another attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryTaskResult {
    Requeued,
    Terminal(TaskStatus),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRebuildEvent {
    pub cursor: i64,
    pub run_id: RunId,
    pub task_id: Option<TaskId>,
    pub attempt_id: Option<akzio_domain::AttemptId>,
    pub event_type: String,
    pub artifact_id: Option<ArtifactId>,
    pub created_at: DateTime<Utc>,
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
    pub fn pair_key(&self) -> RebuildStoreResult<ContentHash> {
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

    fn validate(&self) -> RebuildStoreResult<()> {
        self.subject.validate()?;
        if self.candidate_topology_id.trim().is_empty() {
            return Err(RebuildStoreError::InvalidLearningCommit(
                "shadow_pair.identity",
            ));
        }
        match &self.subject {
            PolicySubject::Contract(contract_hash)
                if contract_hash != &self.candidate_contract_hash =>
            {
                return Err(RebuildStoreError::InvalidLearningCommit(
                    "shadow_pair.contract_subject",
                ));
            }
            PolicySubject::Topology(topology_id) if topology_id.0 != self.candidate_topology_id => {
                return Err(RebuildStoreError::InvalidLearningCommit(
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
            return Err(RebuildStoreError::InvalidLearningCommit(
                "shadow_pair.references",
            ));
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

impl RebuildStore {
    pub fn open(root: impl AsRef<Path>) -> RebuildStoreResult<Self> {
        let root = root.as_ref().to_path_buf();
        if root.join(LEGACY_DATABASE_FILE).exists() && !root.join(DATABASE_FILE).exists() {
            return Err(RebuildStoreError::IncompatibleStoreRoot(root));
        }
        fs::create_dir_all(root.join("blobs")).map_err(|source| RebuildStoreError::Io {
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

    pub fn put_bytes(
        &self,
        bytes: &[u8],
        media_type: impl Into<String>,
    ) -> RebuildStoreResult<BlobRef> {
        let media_type = media_type.into();
        if media_type.trim().is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
                field: "blob_ref.media_type",
            }));
        }
        let hash = ContentHash::of_bytes(bytes);
        let path = self.blob_path(&hash);
        if !path.exists() {
            let parent = path.parent().expect("content addressed blob has parent");
            fs::create_dir_all(parent).map_err(|source| RebuildStoreError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
            match OpenOptions::new().create_new(true).write(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(bytes)
                        .map_err(|source| RebuildStoreError::Io {
                            path: path.clone(),
                            source,
                        })?;
                    file.sync_all().map_err(|source| RebuildStoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => return Err(RebuildStoreError::Io { path, source }),
            }
        }
        Ok(BlobRef {
            hash,
            media_type,
            bytes: bytes.len() as u64,
        })
    }

    pub fn put_json<T: Serialize>(&self, value: &T) -> RebuildStoreResult<BlobRef> {
        self.put_bytes(&serde_json::to_vec(value)?, "application/json")
    }

    pub fn read_blob(&self, blob: &BlobRef) -> RebuildStoreResult<Vec<u8>> {
        let path = self.blob_path(&blob.hash);
        let bytes = fs::read(&path).map_err(|source| RebuildStoreError::Io {
            path: path.clone(),
            source,
        })?;
        if bytes.len() as u64 != blob.bytes || ContentHash::of_bytes(&bytes) != blob.hash {
            return Err(RebuildStoreError::MissingBlob(blob.hash.clone()));
        }
        Ok(bytes)
    }

    /// Writes a root artifact such as an installed Contract. Bootstrap is deliberately
    /// narrow: a task-origin artifact must use `write_task_artifact` instead.
    pub fn write_bootstrap_artifact(&self, artifact: &Artifact) -> RebuildStoreResult<()> {
        artifact.validate()?;
        if artifact.origin.is_some()
            || !matches!(
                artifact.kind,
                ArtifactKind::Contract | ArtifactKind::FreezeState
            )
        {
            return Err(RebuildStoreError::PermitOriginMismatch);
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
    pub fn active_contract(
        &self,
        purpose: &ContractPurpose,
    ) -> RebuildStoreResult<Option<StoredContract>> {
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
    ) -> RebuildStoreResult<Option<StoredContract>> {
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
    ) -> RebuildStoreResult<StoredContract> {
        contract.validate()?;
        let artifact = self.contract_artifact(contract, now)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) =
            self.stored_contract_with_connection(&transaction, &contract.contract_hash)?
        {
            if existing.contract != *contract || existing.activated_at.is_none() {
                return Err(RebuildStoreError::ContractActivationConflict(
                    contract.purpose.clone(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }
        assert_contract_identity_available(&transaction, contract)?;
        if contract_catalogue_head(&transaction, &contract.purpose)?.is_some() {
            return Err(RebuildStoreError::ContractActivationConflict(
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
            .ok_or_else(|| {
                RebuildStoreError::MissingContractInstallation(contract.contract_hash.clone())
            })
    }

    /// Persist a candidate relative to the current active Contract. This is an
    /// immutable install only: activation is coupled atomically to the
    /// candidate's canonical PolicyTransition in `record_policy_evaluation`.
    pub fn install_candidate_contract(
        &self,
        active_contract_hash: &ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> RebuildStoreResult<StoredContract> {
        candidate.validate()?;
        let artifact = self.contract_artifact(candidate, now)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = self
            .stored_contract_with_connection(&transaction, active_contract_hash)?
            .ok_or_else(|| {
                RebuildStoreError::MissingContractInstallation(active_contract_hash.clone())
            })?;
        if active.activated_at.is_none() || !candidate_is_bounded(&active.contract, candidate) {
            return Err(RebuildStoreError::ContractCapabilityExpansion {
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
            return Err(RebuildStoreError::ContractActivationConflict(
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
            .ok_or_else(|| {
                RebuildStoreError::MissingContractInstallation(candidate.contract_hash.clone())
            })
    }

    fn contract_artifact(
        &self,
        contract: &AgentContract,
        now: DateTime<Utc>,
    ) -> RebuildStoreResult<Artifact> {
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
    ) -> RebuildStoreResult<Option<StoredContract>> {
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
                return Err(RebuildStoreError::Integrity(format!(
                    "contract {contract_hash} has an invalid artifact"
                )));
            }
            let contract: AgentContract = self.read_artifact_payload(&artifact)?;
            contract.validate()?;
            if contract.contract_hash != *contract_hash {
                return Err(RebuildStoreError::Integrity(format!(
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
    ) -> RebuildStoreResult<()> {
        let PolicySubject::Contract(candidate_hash) = &commit.subject else {
            return Ok(());
        };
        let candidate = self
            .stored_contract_with_connection(transaction, candidate_hash)?
            .ok_or_else(|| {
                RebuildStoreError::MissingContractInstallation(candidate_hash.clone())
            })?;
        let Some(baseline_hash) = candidate.baseline_contract_hash.as_ref() else {
            return Err(RebuildStoreError::ContractActivationConflict(
                candidate.contract.purpose.clone(),
            ));
        };

        match (transition.from, transition.to) {
            (_, PolicyState::Contract(CandidatePolicyState::Active)) => {
                let candidate_policy_artifact = commit.candidate_policy.as_ref().ok_or(
                    RebuildStoreError::InvalidLearningCommit("contract_catalogue.candidate_policy"),
                )?;
                let candidate_policy: CandidatePolicy =
                    self.read_artifact_payload(candidate_policy_artifact)?;
                if candidate_policy.candidate.artifact_id != candidate.artifact.artifact_id
                    || candidate_policy.baseline.kind != ArtifactKind::Contract
                    || candidate_policy.subject != commit.subject
                {
                    return Err(RebuildStoreError::InvalidLearningCommit(
                        "contract_catalogue.candidate_policy_binding",
                    ));
                }
                let Some((current_hash, _)) =
                    contract_catalogue_head(transaction, &candidate.contract.purpose)?
                else {
                    return Err(RebuildStoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                };
                let current = self
                    .stored_contract_with_connection(transaction, &current_hash)?
                    .ok_or_else(|| {
                        RebuildStoreError::MissingContractInstallation(current_hash.clone())
                    })?;
                if current.contract.contract_hash != *baseline_hash
                    || candidate_policy.baseline.artifact_id != current.artifact.artifact_id
                    || !candidate_is_bounded(&current.contract, &candidate.contract)
                {
                    return Err(RebuildStoreError::ContractActivationConflict(
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
                    return Err(RebuildStoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                };
                if current_hash != *candidate_hash {
                    return Err(RebuildStoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let baseline = self
                    .stored_contract_with_connection(transaction, baseline_hash)?
                    .ok_or_else(|| {
                        RebuildStoreError::MissingContractInstallation(baseline_hash.clone())
                    })?;
                if baseline.contract.purpose != candidate.contract.purpose {
                    return Err(RebuildStoreError::ContractActivationConflict(
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
    pub fn commit_workflow(&self, commit: &WorkflowCommit) -> RebuildStoreResult<()> {
        if commit.graph.kind != ArtifactKind::WorkflowGraph
            || commit.graph.artifact_id != commit.run.graph_artifact_id
        {
            return Err(RebuildStoreError::InvalidWorkflowGraphArtifact);
        }
        commit.graph.validate()?;
        self.read_blob(&commit.graph.blob)?;
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&commit.graph.blob)?)?;
        graph.validate()?;
        if graph.nodes != commit.nodes || graph.topology_id != commit.run.topology_id {
            return Err(RebuildStoreError::WorkflowGraphMismatch);
        }

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_workflow_input_artifacts(&transaction, &commit.nodes)?;
        insert_artifact(&transaction, &commit.graph)?;
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
            return Err(RebuildStoreError::DuplicateRun(commit.run.run_id.clone()));
        }
        for node in &commit.nodes {
            insert_task_node(
                &transaction,
                &commit.run.run_id,
                node,
                commit.run.created_at,
            )?;
        }
        for node in &commit.nodes {
            insert_node_dependencies(&transaction, node)?;
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
            &transaction,
            &commit.run.run_id,
            None,
            None,
            "workflow.created",
            Some(&commit.graph.artifact_id),
            commit.run.created_at,
        )?;
        transaction.commit()?;
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
    ) -> RebuildStoreResult<Option<DaemonLease>> {
        if lease_name.trim().is_empty() || owner_id.trim().is_empty() || expires_at <= now {
            return Err(RebuildStoreError::InvalidDaemonLease(lease_name.to_owned()));
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
    ) -> RebuildStoreResult<bool> {
        if expires_at <= now {
            return Err(RebuildStoreError::InvalidDaemonLease(
                lease.lease_name.clone(),
            ));
        }
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "UPDATE rebuild_daemon_leases SET expires_at = ?1, heartbeat_at = ?2 WHERE lease_name = ?3 AND owner_id = ?4 AND epoch = ?5 AND expires_at > ?2",
            params![expires_at.to_rfc3339(), now.to_rfc3339(), lease.lease_name, lease.owner_id, lease.epoch],
        )?;
        Ok(changed == 1)
    }

    pub fn release_daemon_lease(&self, lease: &DaemonLease) -> RebuildStoreResult<bool> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let changed = connection.execute(
            "DELETE FROM rebuild_daemon_leases WHERE lease_name = ?1 AND owner_id = ?2 AND epoch = ?3",
            params![lease.lease_name, lease.owner_id, lease.epoch],
        )?;
        Ok(changed == 1)
    }

    pub fn daemon_lease(&self, lease_name: &str) -> RebuildStoreResult<Option<DaemonLease>> {
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
    ) -> RebuildStoreResult<()> {
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
    ) -> RebuildStoreResult<SessionSlotReservation> {
        if reservation.session_key.trim().is_empty()
            || reservation.workflow.run.purpose != RunPurpose::Paper
            || reservation.workflow.graph.kind != ArtifactKind::WorkflowGraph
            || reservation.workflow.graph.artifact_id != reservation.workflow.run.graph_artifact_id
        {
            return Err(RebuildStoreError::InvalidSessionSlot(
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
            return Err(RebuildStoreError::WorkflowGraphMismatch);
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
                insert_artifact(&transaction, &reservation.workflow.graph)?;
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
            .ok_or_else(|| RebuildStoreError::Integrity("session slot disappeared".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved,
        })
    }

    pub fn session_slot(&self, session_key: &str) -> RebuildStoreResult<Option<SessionSlot>> {
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
                    return Err(RebuildStoreError::InvalidSessionSlot(
                        session_key.to_owned(),
                    ));
                }
                let graph: WorkflowGraph =
                    serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
                graph.validate()?;
                if graph.topology_id != topology_id {
                    return Err(RebuildStoreError::WorkflowGraphMismatch);
                }
                Ok(SessionSlot {
                    session_key: session_key.to_owned(),
                    workflow: WorkflowCommit {
                        run: RebuildRun {
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
    pub fn commit_execution(
        &self,
        lease: &DaemonLease,
        commit: &ExecutionCommit,
    ) -> RebuildStoreResult<ExecutionCommitResult> {
        if commit.session_key.trim().is_empty()
            || commit.commitment.kind != ArtifactKind::ExecutionCommitment
        {
            return Err(RebuildStoreError::InvalidSessionSlot(
                commit.session_key.clone(),
            ));
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
            return Err(RebuildStoreError::InvalidSessionSlot(
                commit.session_key.clone(),
            ));
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
            return Err(RebuildStoreError::InvalidSessionSlot(
                commit.session_key.clone(),
            ));
        };
        if run_id != commit.permit.run_id.0 {
            return Err(RebuildStoreError::InvalidSessionSlot(
                commit.session_key.clone(),
            ));
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
                append_event(
                    &transaction,
                    &commit.permit.run_id,
                    Some(&commit.permit.task_id),
                    Some(&commit.permit.attempt_id),
                    "execution.commitment.recovered",
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
                    append_event(
                        &transaction,
                        &commit.permit.run_id,
                        Some(&commit.permit.task_id),
                        Some(&commit.permit.attempt_id),
                        "execution.commitment.recovered",
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
            return Err(RebuildStoreError::DuplicateExecutionCommitment(
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
        append_event(
            &transaction,
            &commit.permit.run_id,
            Some(&commit.permit.task_id),
            Some(&commit.permit.attempt_id),
            "execution.committed",
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
    ) -> RebuildStoreResult<()> {
        let invalid = || RebuildStoreError::InvalidSessionSlot(session_key.to_owned());
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
    ) -> RebuildStoreResult<Option<Artifact>> {
        if commitment.kind != ArtifactKind::ExecutionCommitment {
            return Err(RebuildStoreError::InvalidExecutionReprice);
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

    /// Atomically installs the single Rust-owned reprice intent for one
    /// commitment/asset lineage and terminally completes its task. The broker
    /// adapter may receive only the returned immutable intent afterwards.
    pub fn commit_reprice(
        &self,
        lease: &DaemonLease,
        commit: &RepriceCommit,
    ) -> RebuildStoreResult<RepriceCommitResult> {
        if commit.reprice.kind != ArtifactKind::ExecutionReprice {
            return Err(RebuildStoreError::InvalidExecutionReprice);
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
            return Err(RebuildStoreError::InvalidExecutionReprice);
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
            return Err(RebuildStoreError::InvalidExecutionReprice);
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
            return Err(RebuildStoreError::InvalidExecutionReprice);
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
            return Err(RebuildStoreError::InvalidExecutionReprice);
        }

        let slot = transaction
            .query_row(
                "SELECT run_id, commitment_artifact_id FROM rebuild_session_slots WHERE session_key = ?1",
                params![commitment.broker_session],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        let Some((run_id, commitment_artifact_id)) = slot else {
            return Err(RebuildStoreError::InvalidSessionSlot(
                commitment.broker_session.clone(),
            ));
        };
        if run_id != commit.permit.run_id.0
            || commitment_artifact_id.as_deref() != Some(payload.commitment.artifact_id.0.as_str())
        {
            return Err(RebuildStoreError::InvalidSessionSlot(
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
                        "execution.reprice.recovered",
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
            return Err(RebuildStoreError::DuplicateExecutionReprice(format!(
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
            "execution.reprice.committed",
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
    pub fn commit_workflow_patch(&self, commit: &WorkflowPatchCommit) -> RebuildStoreResult<()> {
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
            return Err(RebuildStoreError::InvalidWorkflowProposalArtifact);
        }
        if next_graph.kind != ArtifactKind::WorkflowGraph {
            return Err(RebuildStoreError::InvalidWorkflowGraphArtifact);
        }
        if planner_output.lifecycle != ArtifactLifecycle::RunScoped
            || evidence_needs
                .iter()
                .any(|artifact| artifact.lifecycle != ArtifactLifecycle::RunScoped)
            || proposal_artifact.lifecycle != ArtifactLifecycle::RunScoped
            || next_graph.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(RebuildStoreError::InvalidWorkflowProposalArtifact);
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
            return Err(RebuildStoreError::WorkflowGraphMismatch);
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
            return Err(RebuildStoreError::InvalidWorkflowProposalArtifact);
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
            return Err(RebuildStoreError::WorkflowGraphMismatch);
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
            "artifact.committed",
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
                "artifact.committed",
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
            return Err(RebuildStoreError::MissingRun(run_id.clone()));
        };
        if parse_enum::<RunPurpose>(&purpose)? == RunPurpose::Paper {
            return Err(RebuildStoreError::FrozenPaperWorkflow(run_id.clone()));
        }
        if current != previous_graph_artifact_id.0.as_str() {
            return Err(RebuildStoreError::StaleWorkflowGraph);
        }
        let previous_graph_artifact = read_artifact(&transaction, previous_graph_artifact_id)?;
        if previous_graph_artifact.kind != ArtifactKind::WorkflowGraph {
            return Err(RebuildStoreError::InvalidWorkflowGraphArtifact);
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
            return Err(RebuildStoreError::WorkflowGraphMismatch);
        }
        for previous in &previous_graph.nodes {
            let Some(next) = graph
                .nodes
                .iter()
                .find(|node| node.task_id == previous.task_id)
            else {
                return Err(RebuildStoreError::WorkflowGraphMismatch);
            };
            if next != previous {
                if !updated_ids.contains(&previous.task_id) {
                    return Err(RebuildStoreError::WorkflowGraphMismatch);
                }
                let mut permitted_update = previous.clone();
                permitted_update.dependencies = next.dependencies.clone();
                permitted_update.input_artifacts = next.input_artifacts.clone();
                if permitted_update != *next {
                    return Err(RebuildStoreError::WorkflowGraphMismatch);
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
            return Err(RebuildStoreError::WorkflowGraphMismatch);
        }
        insert_artifact(&transaction, proposal_artifact)?;
        let proposal_event_id = append_event(
            &transaction,
            run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            "artifact.committed",
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
                return Err(RebuildStoreError::TaskNotRunnable(node.task_id.clone()));
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
            "workflow.patched",
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
    ) -> RebuildStoreResult<bool> {
        if reason.trim().is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
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
            return Err(RebuildStoreError::MissingRun(run_id.clone()));
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
            "run.cancel_requested",
            None,
            now,
        )?;
        cancel_queued_tasks(&transaction, run_id, now)?;
        refresh_run_status(&transaction, run_id, now)?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn run_cancel_requested(&self, run_id: &RunId) -> RebuildStoreResult<bool> {
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
    ) -> RebuildStoreResult<RetryTaskResult> {
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
                "task.retry_scheduled",
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
            "task.retry_exhausted",
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
    ) -> RebuildStoreResult<Option<ClaimedRebuildTask>> {
        if worker_id.trim().is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
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
            return Err(RebuildStoreError::TaskNotRunnable(permit.task_id));
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
            "task.started",
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(Some(ClaimedRebuildTask {
            run_id,
            node,
            permit,
        }))
    }

    pub fn heartbeat_task(
        &self,
        permit: &TaskWritePermit,
        expires_at: DateTime<Utc>,
    ) -> RebuildStoreResult<()> {
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
            return Err(RebuildStoreError::StalePermit(permit.task_id.clone()));
        }
        Ok(())
    }

    /// Verifies that a handler still owns the active task attempt without
    /// creating an artifact or changing task state. External adapters use
    /// this immediately before side effects; final persistence rechecks the
    /// same permit in its own transaction.
    pub fn validate_task_permit(&self, permit: &TaskWritePermit) -> RebuildStoreResult<()> {
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
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
    ) -> RebuildStoreResult<()> {
        if !status.is_terminal() {
            return Err(RebuildStoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let connection = self.connection.lock().expect("store connection poisoned");
        let current = connection
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
            return Err(RebuildStoreError::StalePermit(permit.task_id.clone()));
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
            return Err(RebuildStoreError::StalePermit(permit.task_id.clone()));
        }
        Ok(())
    }

    pub fn write_task_artifact(
        &self,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        event_type: &str,
        now: DateTime<Utc>,
    ) -> RebuildStoreResult<()> {
        artifact.validate()?;
        reject_generic_learning_artifact(artifact)?;
        self.read_blob(&artifact.blob)?;
        if event_type.trim().is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
                field: "event_type",
            }));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
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
    ) -> RebuildStoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, status)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        commit_attempt_transaction(&transaction, permit, artifacts, status, now)?;
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
    ) -> RebuildStoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, status)?;
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        commit_attempt_transaction(&transaction, permit, artifacts, status, now)?;
        transaction.commit()?;
        Ok(())
    }

    fn validate_attempt_commit(
        &self,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
    ) -> RebuildStoreResult<()> {
        if !status.is_terminal() {
            return Err(RebuildStoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        if status == TaskStatus::Succeeded && artifacts.is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
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
    ) -> RebuildStoreResult<()> {
        if outcomes.is_empty() {
            return Err(RebuildStoreError::Domain(DomainError::EmptyField {
                field: "commit_outcomes.outcomes",
            }));
        }
        let payloads = outcomes
            .iter()
            .map(|artifact| {
                artifact.validate()?;
                self.read_blob(&artifact.blob)?;
                if artifact.kind != ArtifactKind::Outcome {
                    return Err(RebuildStoreError::InvalidLearningCommit(
                        "commit_outcomes.kind",
                    ));
                }
                let outcome: Outcome = self.read_artifact_payload(artifact)?;
                outcome.validate_sealed()?;
                Ok(outcome)
            })
            .collect::<RebuildStoreResult<Vec<_>>>()?;

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let purpose = run_purpose_from_connection(&transaction, &permit.run_id)?;
        let expected_lifecycle = match purpose {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            RunPurpose::Shadow => ArtifactLifecycle::RunScoped,
            _ => return Err(RebuildStoreError::NonCanonicalLearningPurpose(purpose)),
        };
        let allowed_schedule_purposes: &[RunPurpose] = match purpose {
            RunPurpose::Paper => &[RunPurpose::Paper],
            RunPurpose::Shadow => &[RunPurpose::Paper, RunPurpose::Shadow],
            _ => unreachable!("non-learning purpose rejected above"),
        };
        for (artifact, outcome) in outcomes.iter().zip(&payloads) {
            if artifact.lifecycle != expected_lifecycle {
                return Err(RebuildStoreError::InvalidLearningCommit(
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
                return Err(RebuildStoreError::InvalidLearningCommit(
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
                "artifact.committed",
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
    ) -> RebuildStoreResult<ShadowPairWriteResult> {
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
            return Err(RebuildStoreError::ShadowPairConflict(pair_key.to_string()));
        }

        let completion_cursor = append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            "shadow_pair.completed",
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
    ) -> RebuildStoreResult<PolicyEvaluationResult> {
        commit.subject.validate()?;
        if !commit.subject.accepts_state(commit.from) || !commit.subject.accepts_state(commit.to) {
            return Err(RebuildStoreError::InvalidLearningCommit(
                "policy_evaluation.subject_state",
            ));
        }
        let subject_id = commit.subject.subject_id();
        let subject_json = serde_json::to_string(&commit.subject)?;

        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        self.validate_policy_evaluation_commit_with_connection(&transaction, commit)?;

        if let Some(existing) =
            read_policy_evaluation(&transaction, &commit.evaluation.artifact_id)?
        {
            if !same_policy_evaluation(&existing, commit) {
                return Err(RebuildStoreError::PolicyEvaluationConflict(
                    commit.evaluation.artifact_id.to_string(),
                ));
            }
            if let Some(candidate_policy) = &commit.candidate_policy {
                let stored = read_artifact(&transaction, &candidate_policy.artifact_id)?;
                if stored != *candidate_policy {
                    return Err(RebuildStoreError::PolicyEvaluationConflict(
                        commit.evaluation.artifact_id.to_string(),
                    ));
                }
            }
            let consumption = read_policy_consumption_head(&transaction, &commit.subject)?
                .ok_or_else(|| {
                    RebuildStoreError::Integrity(format!(
                        "policy evaluation {} has no consumption head",
                        commit.evaluation.artifact_id
                    ))
                })?;
            if consumption.consumed_pair_cursor < existing.consumed_pair_cursor {
                return Err(RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::PolicyHeadMismatch(subject_id));
            }
            None if commit.subject.initial_state() != commit.from => {
                return Err(RebuildStoreError::PolicyHeadMismatch(subject_id));
            }
            _ => {}
        }
        match &commit.transition {
            Some(transition) => {
                if commit.from == commit.to || !is_allowed_policy_transition(commit.from, commit.to)
                {
                    return Err(RebuildStoreError::InvalidLearningCommit(
                        "policy_transition.path",
                    ));
                }
                if read_policy_transition(&transaction, &transition.transition_id)?.is_some() {
                    return Err(RebuildStoreError::PolicyTransitionConflict(
                        transition.transition_id.to_string(),
                    ));
                }
            }
            None if commit.from != commit.to => {
                return Err(RebuildStoreError::InvalidLearningCommit(
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
                "artifact.committed",
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
            "policy.evaluated",
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
                "policy.transitioned",
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
    ) -> RebuildStoreResult<()> {
        if !status.is_terminal() {
            return Err(RebuildStoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let mut connection = self.connection.lock().expect("store connection poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        finish_permitted_task(&transaction, permit, status, on_failure, None, now)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn recover_expired_tasks(&self, now: DateTime<Utc>) -> RebuildStoreResult<u64> {
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
                    "task.recovered",
                    None,
                    now,
                )?;
            } else {
                append_event(
                    &transaction,
                    run_id,
                    Some(task_id),
                    Some(attempt_id),
                    "task.recovery_exhausted",
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

    pub fn artifact(&self, artifact_id: &ArtifactId) -> RebuildStoreResult<Artifact> {
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
    ) -> RebuildStoreResult<Vec<Artifact>> {
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
            .ok_or_else(|| RebuildStoreError::CommittedOutputTask {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
            })?;
        read_committed_attempt_outputs(&connection, Some(run_id), task_id, &AttemptId(attempt_id))
    }

    /// Returns final artifacts for one exact succeeded task attempt. This is
    /// intentionally stricter than an event-log query so callers cannot feed
    /// an AgentTurn, ToolCall, or failed-attempt artifact into another task.
    pub fn committed_attempt_outputs(
        &self,
        task_id: &TaskId,
        attempt_id: &AttemptId,
    ) -> RebuildStoreResult<Vec<Artifact>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        read_committed_attempt_outputs(&connection, None, task_id, attempt_id)
    }

    pub fn artifacts_referencing(
        &self,
        source_artifact_id: &ArtifactId,
        kind: Option<ArtifactKind>,
    ) -> RebuildStoreResult<Vec<Artifact>> {
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
    pub fn latest_artifact_by_kind(
        &self,
        kind: ArtifactKind,
    ) -> RebuildStoreResult<Option<Artifact>> {
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
    pub fn run_purpose(&self, run_id: &RunId) -> RebuildStoreResult<RunPurpose> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let purpose = connection
            .query_row(
                "SELECT purpose FROM rebuild_runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| RebuildStoreError::MissingRun(run_id.clone()))?;
        parse_enum(&purpose)
    }

    pub fn workflow_revision(
        &self,
        run_id: &RunId,
        revision: u64,
    ) -> RebuildStoreResult<WorkflowRevision> {
        let connection = self.connection.lock().expect("store connection poisoned");
        self.workflow_revision_with_connection(&connection, run_id, revision)
    }

    fn workflow_revision_with_connection(
        &self,
        connection: &Connection,
        run_id: &RunId,
        revision: u64,
    ) -> RebuildStoreResult<WorkflowRevision> {
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
            .ok_or_else(|| RebuildStoreError::MissingWorkflowRevision {
                run_id: run_id.clone(),
                revision,
            })?;
        self.hydrate_workflow_revision(connection, row)
    }

    pub fn workflow_snapshot(&self, run_id: &RunId) -> RebuildStoreResult<WorkflowSnapshot> {
        let connection = self.connection.lock().expect("store connection poisoned");
        self.workflow_snapshot_with_connection(&connection, run_id)
    }

    fn workflow_snapshot_with_connection(
        &self,
        connection: &Connection,
        run_id: &RunId,
    ) -> RebuildStoreResult<WorkflowSnapshot> {
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
            .ok_or_else(|| RebuildStoreError::MissingRun(run_id.clone()))?;
        let (purpose, topology_id, graph_artifact_id, status, created_at, finished_at) = run_row;
        let run = RebuildRun {
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
                RebuildStoreError::Integrity(format!("run {run_id} has no workflow revision"))
            })?;
        let revision = self.hydrate_workflow_revision(connection, revision_row)?;
        if revision.graph_artifact.artifact_id != run.graph_artifact_id
            || revision.graph.topology_id != run.topology_id
        {
            return Err(RebuildStoreError::WorkflowGraphMismatch);
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
                return Err(RebuildStoreError::WorkflowGraphMismatch);
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
                            RebuildStoreError::Integrity(format!(
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
                        return Err(RebuildStoreError::Integrity(format!(
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
                    return Err(RebuildStoreError::Integrity(format!(
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
            .map(|task| (task.node.task_id.clone(), task.node.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        if graph_nodes != stored_nodes {
            return Err(RebuildStoreError::WorkflowGraphMismatch);
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
    ) -> RebuildStoreResult<WorkflowRevision> {
        let revision = u64::try_from(row.0).map_err(|_| {
            RebuildStoreError::Integrity(format!("invalid workflow revision {}", row.0))
        })?;
        let graph_artifact = read_artifact(connection, &ArtifactId(ContentHash::new(row.1)?))?;
        if graph_artifact.kind != ArtifactKind::WorkflowGraph {
            return Err(RebuildStoreError::InvalidWorkflowGraphArtifact);
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
    ) -> RebuildStoreResult<()> {
        let mut previous: Option<WorkflowRevision> = None;
        for revision_number in 0..=snapshot.revision.revision {
            let revision = self.workflow_revision_with_connection(
                connection,
                &snapshot.run.run_id,
                revision_number,
            )?;
            if revision.graph.topology_id != snapshot.run.topology_id {
                return Err(RebuildStoreError::WorkflowGraphMismatch);
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
                    return Err(RebuildStoreError::WorkflowGraphMismatch);
                }
            }
            previous = Some(revision);
        }
        if previous.as_ref() != Some(&snapshot.revision) {
            return Err(RebuildStoreError::WorkflowGraphMismatch);
        }
        Ok(())
    }

    /// Reads the current policy head without exposing mutable storage to
    /// callers. Previous policy versions remain in `rebuild_policy_transitions`.
    pub fn policy_head(&self, subject: &PolicySubject) -> RebuildStoreResult<Option<PolicyHead>> {
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
    ) -> RebuildStoreResult<PolicyShadowPairSnapshot> {
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
    ) -> RebuildStoreResult<Option<PolicySubject>> {
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
                return Err(RebuildStoreError::Integrity(format!(
                    "policy influence {artifact_id} has invalid kind"
                )));
            }
            let subject = parse_persisted_subject(&subject_id, &subject_json)?;
            if resolved.as_ref().is_some_and(|current| current != &subject) {
                return Err(RebuildStoreError::Integrity(format!(
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
    ) -> RebuildStoreResult<Vec<PolicyTransitionRecord>> {
        subject.validate()?;
        let connection = self.connection.lock().expect("store connection poisoned");
        read_policy_transitions(&connection, subject)
    }

    pub fn events_after(
        &self,
        run_id: &RunId,
        after: i64,
        limit: usize,
    ) -> RebuildStoreResult<Vec<StoredRebuildEvent>> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events WHERE run_id = ?1 AND event_id > ?2
               ORDER BY event_id ASC LIMIT ?3"#,
        )?;
        let rows = statement.query_map(params![run_id.0, after, limit as i64], |row| {
            Ok(StoredRebuildEvent {
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
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn verify_integrity(&self) -> RebuildStoreResult<()> {
        let connection = self.connection.lock().expect("store connection poisoned");
        let fk = connection
            .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
            .optional()?;
        if fk.is_some() {
            return Err(RebuildStoreError::Integrity(
                "foreign key check failed".to_owned(),
            ));
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
            return Err(RebuildStoreError::Integrity(
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
                return Err(RebuildStoreError::Integrity(format!(
                    "invalid daemon lease {lease_name}"
                )));
            }
            let expires_at = parse_time(&expires_at)?;
            if parse_time(&heartbeat_at)? > expires_at {
                return Err(RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(format!(
                    "invalid session slot {session_key}"
                )));
            }
            let graph_artifact_id = ArtifactId(ContentHash::new(graph_artifact_id)?);
            let graph_artifact = read_artifact(&connection, &graph_artifact_id)?;
            if graph_artifact.kind != ArtifactKind::WorkflowGraph {
                return Err(RebuildStoreError::Integrity(format!(
                    "session slot {session_key} graph kind is invalid"
                )));
            }
            let graph: WorkflowGraph =
                serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
            graph.validate()?;
            if graph.topology_id != topology_id {
                return Err(RebuildStoreError::Integrity(format!(
                    "session slot {session_key} graph topology mismatch"
                )));
            }
            parse_time(&run_created_at)?;
            parse_time(&reserved_at)?;
            match (commitment_artifact_id, committed_at) {
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => {
                    return Err(RebuildStoreError::Integrity(format!(
                        "session slot {session_key} has incomplete commitment state"
                    )));
                }
                (Some(commitment_artifact_id), Some(committed_at)) => {
                    let commitment_artifact_id =
                        ArtifactId(ContentHash::new(commitment_artifact_id)?);
                    let commitment_artifact = read_artifact(&connection, &commitment_artifact_id)?;
                    if commitment_artifact.kind != ArtifactKind::ExecutionCommitment {
                        return Err(RebuildStoreError::Integrity(format!(
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
                        RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(
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
                return Err(RebuildStoreError::Integrity(
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
                return Err(RebuildStoreError::Integrity(
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
                return Err(RebuildStoreError::Integrity(
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
                return Err(RebuildStoreError::Integrity(
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
                return Err(RebuildStoreError::Integrity(format!(
                    "policy head {subject_id} is invalid"
                )));
            }
            let state: PolicyState = serde_json::from_str(&state_json)?;
            let transition =
                read_policy_transition(&connection, &PolicyTransitionId(transition_id.clone()))?
                    .ok_or_else(|| {
                        RebuildStoreError::Integrity(format!(
                    "policy head {subject_id} references missing transition {transition_id}"
                ))
                    })?;
            if transition.transition.subject.subject_id() != subject_id
                || transition.revision != revision
                || transition.transition.to != state
                || transition.transition_cursor != transition_cursor
                || transition.transition.created_at != parse_time(&updated_at)?
            {
                return Err(RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(format!(
                    "policy head {subject_id} is stale"
                )));
            }
            let evaluation =
                read_artifact(&connection, &transition.transition.evaluation.artifact_id)?;
            if evaluation.kind != ArtifactKind::Evaluation
                || artifact_run_purpose(&connection, &evaluation)? != RunPurpose::Paper
            {
                return Err(RebuildStoreError::Integrity(format!(
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
            return Err(RebuildStoreError::Integrity(format!(
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
                RebuildStoreError::Integrity(format!("shadow pair {pair_key} disappeared"))
            })?;
            pair.completion.validate()?;
            if pair.completion.pair_key()? != pair_key {
                return Err(RebuildStoreError::Integrity(format!(
                    "shadow pair {pair_key} key mismatch"
                )));
            }
            self.assert_shadow_pair_sources_with_connection(&connection, &pair.completion)
                .map_err(|error| {
                    RebuildStoreError::Integrity(format!(
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
            return Err(RebuildStoreError::Integrity(format!(
                "task {task_id} has no run"
            )));
        }
        let run_ids = connection
            .prepare("SELECT run_id FROM rebuild_runs ORDER BY run_id")?
            .query_map([], |row| Ok(RunId(row.get(0)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        for run_id in run_ids {
            let snapshot = self.workflow_snapshot_with_connection(&connection, &run_id)?;
            self.verify_workflow_history(&connection, &snapshot)?;
        }
        self.verify_contract_catalogue_history(&connection)?;
        self.verify_policy_evaluation_history(&connection)?;
        self.verify_candidate_policy_history(&connection)?;
        Ok(())
    }

    fn verify_contract_catalogue_history(&self, connection: &Connection) -> RebuildStoreResult<()> {
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
                    RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(format!(
                    "contract installation {contract_hash} metadata disagrees with payload"
                )));
            }
            if let Some(baseline_hash) = &stored.baseline_contract_hash {
                let baseline_contract = self
                    .stored_contract_with_connection(connection, baseline_hash)?
                    .ok_or_else(|| {
                        RebuildStoreError::Integrity(format!(
                            "candidate contract {contract_hash} has missing baseline {baseline_hash}"
                        ))
                    })?;
                if !candidate_is_bounded(&baseline_contract.contract, &stored.contract) {
                    return Err(RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(format!(
                    "contract activation {activation_id} is not the next history entry for {purpose}"
                )));
            }
            let contract = contracts.get(&contract_hash).ok_or_else(|| {
                RebuildStoreError::Integrity(format!(
                    "contract activation {activation_id} references unknown contract {contract_hash}"
                ))
            })?;
            if contract.contract.purpose.as_str() != purpose {
                return Err(RebuildStoreError::Integrity(format!(
                    "contract activation {activation_id} purpose disagrees with its contract"
                )));
            }
            match (previous.as_ref(), transition_id) {
                (None, None) if contract.baseline_contract_hash.is_none() => {}
                (Some(previous_hash), Some(transition_id)) => {
                    let transition =
                        read_policy_transition(connection, &PolicyTransitionId(transition_id))?
                            .ok_or_else(|| {
                                RebuildStoreError::Integrity(format!(
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
                                    RebuildStoreError::Integrity(format!(
                                        "contract activation {activation_id} rollback has no baseline"
                                    ))
                                })?;
                    if !promoted && !rolled_back {
                        return Err(RebuildStoreError::Integrity(format!(
                            "contract activation {activation_id} is not a valid promotion or rollback"
                        )));
                    }
                }
                _ => {
                    return Err(RebuildStoreError::Integrity(format!(
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
            return Err(RebuildStoreError::Integrity(
                "contract catalogue head count disagrees with activation history".to_owned(),
            ));
        }
        for (purpose, contract_hash, activation_id) in heads {
            let contract_hash = ContentHash::new(contract_hash)?;
            if latest.get(&purpose) != Some(&(activation_id, contract_hash)) {
                return Err(RebuildStoreError::Integrity(format!(
                    "contract catalogue head for {purpose} is stale"
                )));
            }
        }
        Ok(())
    }

    fn verify_policy_evaluation_history(&self, connection: &Connection) -> RebuildStoreResult<()> {
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
                    RebuildStoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} disappeared"
                    ))
                })?;
            stored.subject.validate()?;
            if !stored.subject.accepts_state(stored.from)
                || !stored.subject.accepts_state(stored.to)
            {
                return Err(RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(format!(
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
                    return Err(RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} lineage is invalid"
                )));
            }

            match (&stored.subject, &stored.candidate_policy_artifact_id) {
                (PolicySubject::Memory(_), None) => {}
                (PolicySubject::Memory(_), Some(_)) => {
                    return Err(RebuildStoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} binds a memory candidate"
                    )));
                }
                (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                    return Err(RebuildStoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} has no candidate policy"
                    )));
                }
                (_, Some(candidate_policy_artifact_id)) => {
                    let candidate = read_artifact(connection, candidate_policy_artifact_id)?;
                    if candidate.kind != ArtifactKind::CandidatePolicy
                        || candidate.lifecycle != ArtifactLifecycle::Canonical
                        || artifact_run_purpose(connection, &candidate)? != RunPurpose::Paper
                    {
                        return Err(RebuildStoreError::Integrity(format!(
                            "policy evaluation {evaluation_artifact_id} has invalid candidate policy"
                        )));
                    }
                }
            }

            match &stored.transition_id {
                Some(transition_id) => {
                    let transition = read_policy_transition(connection, transition_id)?
                        .ok_or_else(|| {
                            RebuildStoreError::Integrity(format!(
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
                        return Err(RebuildStoreError::Integrity(format!(
                            "policy evaluation {evaluation_artifact_id} disagrees with transition {transition_id}"
                        )));
                    }
                }
                None if stored.from != stored.to => {
                    return Err(RebuildStoreError::Integrity(format!(
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
                    RebuildStoreError::Integrity(format!(
                        "policy evaluation {evaluation_artifact_id} has no durable event"
                    ))
                })?;
            if event.0 != stored.run_id.0
                || event.1 != "policy.evaluated"
                || event.2.as_deref() != Some(stored.evaluation_artifact_id.0.as_str())
                || parse_time(&event.3)? != stored.completed_at
            {
                return Err(RebuildStoreError::Integrity(format!(
                    "policy evaluation {evaluation_artifact_id} event is invalid"
                )));
            }

            if stored.consumed_pair_cursor < 0
                || (stored.consumed_pair_cursor != 0
                    && stored.consumed_pair_cursor >= stored.event_cursor)
            {
                return Err(RebuildStoreError::Integrity(format!(
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
                    return Err(RebuildStoreError::Integrity(format!(
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
                RebuildStoreError::Integrity(format!(
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
                        RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(format!(
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
            return Err(RebuildStoreError::Integrity(format!(
                "policy evaluation {evaluation_id} has no consumption head"
            )));
        }

        Ok(())
    }

    fn verify_candidate_policy_history(&self, connection: &Connection) -> RebuildStoreResult<()> {
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
                return Err(RebuildStoreError::Integrity(format!(
                    "candidate policy {artifact_id} is noncanonical"
                )));
            }
            assert_artifact_from_paper_with_connection(connection, &artifact).map_err(|error| {
                RebuildStoreError::Integrity(format!(
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
                return Err(RebuildStoreError::Integrity(format!(
                    "candidate policy {artifact_id} has invalid source closure"
                )));
            }
            let evaluation =
                read_policy_evaluation(connection, &policy.source_evaluation.artifact_id)?
                    .ok_or_else(|| {
                        RebuildStoreError::Integrity(format!(
                            "candidate policy {artifact_id} has no source evaluation"
                        ))
                    })?;
            if evaluation.subject != policy.subject
                || evaluation.completed_at != policy.created_at
                || evaluation.candidate_policy_artifact_id.as_ref() != Some(&artifact_id)
            {
                return Err(RebuildStoreError::Integrity(format!(
                    "candidate policy {artifact_id} disagrees with source evaluation"
                )));
            }
            self.validate_candidate_policy_sources(connection, &policy)
                .map_err(|error| {
                    RebuildStoreError::Integrity(format!(
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
    ) -> RebuildStoreResult<()> {
        for (artifact, kind) in [
            (&commit.outcome, ArtifactKind::Outcome),
            (&commit.experience, ArtifactKind::Experience),
            (&commit.evaluation, ArtifactKind::Evaluation),
        ] {
            artifact.validate()?;
            self.read_blob(&artifact.blob)?;
            if artifact.kind != kind || artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(RebuildStoreError::InvalidLearningCommit(
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
                return Err(RebuildStoreError::InvalidLearningCommit(
                    "candidate_policy.kind_or_lifecycle",
                ));
            }
        }
        let outcome: Outcome = self.read_artifact_payload(&commit.outcome)?;
        outcome.validate()?;
        if !outcome.is_sealed() {
            return Err(RebuildStoreError::UnsealedOutcome(
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
                return Err(RebuildStoreError::InvalidLearningCommit(
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
                return Err(RebuildStoreError::InvalidLearningCommit(
                    "candidate_policy.memory_subject",
                ));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                return Err(RebuildStoreError::InvalidLearningCommit(
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
                    return Err(RebuildStoreError::InvalidLearningCommit(
                        "candidate_policy.links",
                    ));
                }
                self.validate_candidate_policy_sources(connection, &candidate_policy)?;
            }
        }
        commit.subject.validate()?;
        if !commit.subject.accepts_state(commit.from) || !commit.subject.accepts_state(commit.to) {
            return Err(RebuildStoreError::InvalidLearningCommit(
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
            return Err(RebuildStoreError::InvalidLearningCommit(
                "learning_artifact.links",
            ));
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
            return Err(RebuildStoreError::InvalidLearningCommit(
                "learning_artifact.source_refs",
            ));
        }
        Ok(())
    }

    fn validate_candidate_policy_sources(
        &self,
        connection: &Connection,
        policy: &CandidatePolicy,
    ) -> RebuildStoreResult<()> {
        let baseline =
            read_required_artifact(connection, &policy.baseline, "candidate_policy.baseline")?;
        let candidate =
            read_required_artifact(connection, &policy.candidate, "candidate_policy.candidate")?;
        match &policy.subject {
            PolicySubject::Memory(_) => Err(RebuildStoreError::InvalidLearningCommit(
                "candidate_policy.memory_subject",
            )),
            PolicySubject::Contract(candidate_hash) => {
                if baseline.lifecycle != ArtifactLifecycle::Canonical
                    || candidate.lifecycle != ArtifactLifecycle::Canonical
                {
                    return Err(RebuildStoreError::InvalidLearningCommit(
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
                    return Err(RebuildStoreError::InvalidLearningCommit(
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
                    return Err(RebuildStoreError::InvalidLearningCommit(
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
    ) -> RebuildStoreResult<()> {
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
            return Err(RebuildStoreError::InvalidLearningCommit(
                "shadow_pair.schedule_binding",
            ));
        }
        Ok(())
    }

    fn read_artifact_payload<T: DeserializeOwned>(
        &self,
        artifact: &Artifact,
    ) -> RebuildStoreResult<T> {
        Ok(serde_json::from_slice(&self.read_blob(&artifact.blob)?)?)
    }

    fn read_outcome_schedule_with_connection(
        &self,
        connection: &Connection,
        outcome: &Outcome,
        allowed_purposes: &[RunPurpose],
    ) -> RebuildStoreResult<OutcomeSchedule> {
        if outcome.schedule.kind != ArtifactKind::OutcomeSchedule {
            return Err(RebuildStoreError::InvalidLearningCommit(
                "outcome.schedule_kind",
            ));
        }
        let schedule_artifact = read_artifact(connection, &outcome.schedule.artifact_id)?;
        if schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
            return Err(RebuildStoreError::InvalidLearningCommit(
                "outcome.schedule_artifact",
            ));
        }
        let schedule_purpose = artifact_run_purpose(connection, &schedule_artifact)?;
        let expected_lifecycle = match schedule_purpose {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            RunPurpose::Shadow => ArtifactLifecycle::RunScoped,
            _ => {
                return Err(RebuildStoreError::InvalidLearningCommit(
                    "outcome.schedule_artifact",
                ));
            }
        };
        if schedule_artifact.lifecycle != expected_lifecycle {
            return Err(RebuildStoreError::InvalidLearningCommit(
                "outcome.schedule_artifact",
            ));
        }
        assert_artifact_from_allowed_purposes(connection, &schedule_artifact, allowed_purposes)?;
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&self.read_blob(&schedule_artifact.blob)?)?;
        schedule.validate()?;
        if schedule.outcome_id != outcome.outcome_id {
            return Err(RebuildStoreError::InvalidLearningCommit(
                "outcome.schedule_identity",
            ));
        }

        let expected = outcome_schedule_source_refs(&schedule);
        if !has_exact_source_refs(&schedule_artifact, &expected) {
            return Err(RebuildStoreError::InvalidLearningCommit(
                "outcome_schedule.source_refs",
            ));
        }
        for reference in &expected {
            let artifact = read_artifact(connection, &reference.artifact_id)?;
            if artifact.kind != reference.kind {
                return Err(RebuildStoreError::InvalidLearningCommit(
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
    ) -> RebuildStoreResult<()> {
        let verdict_ref = match &schedule.execution {
            OutcomeExecutionLineage::NoOrder { execution_verdict } => execution_verdict,
            OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict, ..
            } => execution_verdict,
        };
        let verdict_artifact = read_artifact(connection, &verdict_ref.artifact_id)?;
        if verdict_artifact.kind != ArtifactKind::ExecutionVerdict {
            return Err(RebuildStoreError::InvalidLearningCommit(
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
                    return Err(RebuildStoreError::InvalidLearningCommit(
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
                    return Err(RebuildStoreError::InvalidLearningCommit(
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
                    return Err(RebuildStoreError::InvalidLearningCommit(
                        "outcome_schedule.commitment_lineage",
                    ));
                }

                let reconciliation_artifact =
                    read_artifact(connection, &reconciliation.artifact_id)?;
                if reconciliation_artifact.kind != ArtifactKind::Reconciliation {
                    return Err(RebuildStoreError::InvalidLearningCommit(
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
                    return Err(RebuildStoreError::InvalidLearningCommit(
                        "outcome_schedule.reconciliation_lineage",
                    ));
                }
            }
            _ => {
                return Err(RebuildStoreError::InvalidLearningCommit(
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

fn initialize(connection: &mut Connection, root: &Path) -> RebuildStoreResult<()> {
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
        return Err(RebuildStoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }
    if let Some(value) = version.as_deref() {
        if value != REBUILD_SCHEMA_VERSION.to_string() {
            return Err(RebuildStoreError::IncompatibleStoreRoot(PathBuf::from(
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
        return Err(RebuildStoreError::IncompatibleStoreRoot(root.to_path_buf()));
    }
    if version.is_none() {
        connection.execute(
            "INSERT INTO rebuild_metadata (key, value) VALUES ('schema_version', ?1)",
            params![REBUILD_SCHEMA_VERSION.to_string()],
        )?;
    }
    Ok(())
}

fn table_has_column(
    connection: &Connection,
    table: &str,
    required_column: &str,
) -> RebuildStoreResult<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns.iter().any(|column| column == required_column))
}

fn contract_catalogue_head(
    connection: &Connection,
    purpose: &ContractPurpose,
) -> RebuildStoreResult<Option<(ContentHash, i64)>> {
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
) -> RebuildStoreResult<()> {
    let existing = connection
        .query_row(
            "SELECT contract_hash FROM rebuild_contract_installations WHERE contract_id = ?1 AND contract_version = ?2",
            params![contract.contract_id.0.as_str(), i64::from(contract.version)],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if existing.is_some() {
        return Err(RebuildStoreError::DuplicateContractVersion {
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
) -> RebuildStoreResult<()> {
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
) -> RebuildStoreResult<i64> {
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
) -> RebuildStoreResult<()> {
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

fn insert_artifact(transaction: &Transaction<'_>, artifact: &Artifact) -> RebuildStoreResult<()> {
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
            return Err(RebuildStoreError::InvalidArtifactClosure(
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
            return Err(RebuildStoreError::Integrity(format!(
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
fn insert_artifact_batch(
    transaction: &Transaction<'_>,
    artifacts: &[Artifact],
) -> RebuildStoreResult<()> {
    let mut pending = BTreeMap::<ArtifactId, &Artifact>::new();
    for artifact in artifacts {
        artifact.validate()?;
        if let Some(existing) = pending.insert(artifact.artifact_id.clone(), artifact) {
            if existing != artifact {
                return Err(RebuildStoreError::Integrity(format!(
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
            return Err(RebuildStoreError::InvalidArtifactClosure(
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
) -> RebuildStoreResult<()> {
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
) -> RebuildStoreResult<()> {
    let artifact = read_artifact(transaction, &reference.artifact_id)?;
    if artifact.kind != reference.kind {
        return Err(RebuildStoreError::InvalidArtifactClosure(
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
) -> RebuildStoreResult<()> {
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
        return Err(RebuildStoreError::DuplicateTask(node.task_id.clone()));
    }
    Ok(())
}

fn insert_node_dependencies(
    transaction: &Transaction<'_>,
    node: &WorkflowNode,
) -> RebuildStoreResult<()> {
    for dependency in &node.dependencies {
        transaction.execute(
            "INSERT INTO rebuild_task_dependencies (task_id, depends_on_task_id) VALUES (?1, ?2)",
            params![node.task_id.0, dependency.0],
        )?;
    }
    Ok(())
}

fn task_dependencies(connection: &Connection, task_id: &TaskId) -> RebuildStoreResult<Vec<TaskId>> {
    let dependencies = connection
        .prepare(
            "SELECT depends_on_task_id FROM rebuild_task_dependencies \
             WHERE task_id = ?1 ORDER BY depends_on_task_id ASC",
        )?
        .query_map(params![task_id.0], |row| Ok(TaskId(row.get(0)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(dependencies)
}

fn assert_permit(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
) -> RebuildStoreResult<()> {
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
        return Err(RebuildStoreError::MissingTask(permit.task_id.clone()));
    };
    if run_id != permit.run_id.0
        || status != "running"
        || lease_id.as_deref() != Some(permit.lease_id.0.as_str())
        || epoch != permit.epoch
        || attempt_id.as_deref() != Some(permit.attempt_id.0.as_str())
        || contract_hash.as_deref().map(ContentHash::new).transpose()? != permit.contract_hash
    {
        return Err(RebuildStoreError::StalePermit(permit.task_id.clone()));
    }
    Ok(())
}

fn assert_daemon_lease(
    transaction: &Transaction<'_>,
    lease: &DaemonLease,
    now: DateTime<Utc>,
) -> RebuildStoreResult<()> {
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
        return Err(RebuildStoreError::SchedulerFenced(lease.lease_name.clone()));
    };
    if owner_id != lease.owner_id || epoch != lease.epoch || parse_time(&expires_at)? <= now {
        return Err(RebuildStoreError::SchedulerFenced(lease.lease_name.clone()));
    }
    Ok(())
}

fn assert_origin_matches(
    origin: Option<&ArtifactOrigin>,
    permit: &TaskWritePermit,
) -> RebuildStoreResult<()> {
    let Some(origin) = origin else {
        return Err(RebuildStoreError::PermitOriginMismatch);
    };
    if origin.run_id.as_ref() != Some(&permit.run_id)
        || origin.task_id.as_ref() != Some(&permit.task_id)
        || origin.attempt_id.as_ref() != Some(&permit.attempt_id)
        || origin.contract_hash != permit.contract_hash
    {
        return Err(RebuildStoreError::PermitOriginMismatch);
    }
    Ok(())
}

fn task_retry_policy(
    transaction: &Transaction<'_>,
    task_id: &TaskId,
) -> RebuildStoreResult<(RetryPolicy, FailureDisposition)> {
    let (retry_json, on_failure) = transaction
        .query_row(
            "SELECT retry_json, on_failure FROM rebuild_tasks WHERE task_id = ?1",
            params![task_id.0],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .ok_or_else(|| RebuildStoreError::MissingTask(task_id.clone()))?;
    Ok((serde_json::from_str(&retry_json)?, parse_enum(&on_failure)?))
}

fn commit_attempt_transaction(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    artifacts: &[Artifact],
    status: TaskStatus,
    now: DateTime<Utc>,
) -> RebuildStoreResult<()> {
    assert_permit(transaction, permit)?;
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
            "artifact.committed",
            Some(&artifact.artifact_id),
            now,
        )?;
        if status == TaskStatus::Succeeded {
            record_attempt_output(transaction, permit, &artifact.artifact_id, event_id)?;
        }
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
) -> RebuildStoreResult<TaskStatus> {
    let status =
        if requested_status == TaskStatus::Failed && on_failure == FailureDisposition::SkipTask {
            TaskStatus::Skipped
        } else {
            requested_status
        };
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
    append_event(
        transaction,
        &permit.run_id,
        Some(&permit.task_id),
        Some(&permit.attempt_id),
        match status {
            TaskStatus::Succeeded => "task.succeeded",
            TaskStatus::Failed => "task.failed",
            TaskStatus::Cancelled => "task.cancelled",
            TaskStatus::Skipped => "task.skipped",
            _ => unreachable!("terminal status checked above"),
        },
        terminal_artifact_id,
        now,
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
) -> RebuildStoreResult<()> {
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
                "task.cancelled",
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
) -> RebuildStoreResult<()> {
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
                    "task.cancelled",
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
    event_type: &str,
    artifact_id: Option<&ArtifactId>,
    created_at: DateTime<Utc>,
) -> RebuildStoreResult<i64> {
    transaction.execute(
        r#"INSERT INTO rebuild_events
           (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
        params![
            run_id.0,
            task_id.map(|id| id.0.as_str()),
            attempt_id.map(|id| id.0.as_str()),
            event_type,
            artifact_id.map(|id| id.0.as_str()),
            created_at.to_rfc3339(),
        ],
    )?;
    Ok(transaction.last_insert_rowid())
}

fn record_attempt_output(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    artifact_id: &ArtifactId,
    event_id: i64,
) -> RebuildStoreResult<()> {
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
) -> RebuildStoreResult<Vec<Artifact>> {
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
        return Err(RebuildStoreError::CommittedOutputAttempt {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        });
    };
    if attempt_task_id != task_id.0
        || attempt_status != "succeeded"
        || task_status != "succeeded"
        || expected_run_id.is_some_and(|run_id| attempt_run_id != run_id.0)
    {
        return Err(RebuildStoreError::CommittedOutputAttempt {
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
        return Err(RebuildStoreError::CommittedOutputAttempt {
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
) -> RebuildStoreResult<()> {
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

fn read_artifact(
    connection: &Connection,
    artifact_id: &ArtifactId,
) -> RebuildStoreResult<Artifact> {
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
        return Err(RebuildStoreError::MissingArtifact(artifact_id.clone()));
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
        schema_version: REBUILD_SCHEMA_VERSION,
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

fn parse_time(value: &str) -> RebuildStoreResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| RebuildStoreError::Integrity(format!("invalid time {value}: {error}")))
}

/// The indexed subject ID is derived from this typed JSON, never accepted as
/// an independent authority. A corrupt or hand-edited row must therefore
/// fail closed rather than silently changing a policy namespace.
fn parse_persisted_subject(
    subject_id: &str,
    subject_json: &str,
) -> RebuildStoreResult<PolicySubject> {
    let subject: PolicySubject = serde_json::from_str(subject_json)?;
    subject.validate()?;
    if subject.subject_id() != subject_id {
        return Err(RebuildStoreError::Integrity(format!(
            "policy subject JSON does not match indexed identity {subject_id}"
        )));
    }
    Ok(subject)
}

fn read_policy_evaluation(
    connection: &Connection,
    evaluation_artifact_id: &ArtifactId,
) -> RebuildStoreResult<Option<StoredPolicyEvaluation>> {
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
) -> RebuildStoreResult<Option<PolicyConsumptionHead>> {
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
        return Err(RebuildStoreError::Integrity(format!(
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

fn max_shadow_pair_cursor(
    connection: &Connection,
    subject: &PolicySubject,
) -> RebuildStoreResult<i64> {
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
) -> RebuildStoreResult<[u64; 3]> {
    if after_cursor < 0 || through_cursor < after_cursor {
        return Err(RebuildStoreError::InvalidLearningCommit(
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
) -> RebuildStoreResult<()> {
    let current_after = read_policy_consumption_head(connection, subject)?
        .map_or(0, |head| head.consumed_pair_cursor);
    if snapshot.after_cursor != current_after {
        return Err(RebuildStoreError::InvalidLearningCommit(
            "policy_evaluation.pair_snapshot_stale",
        ));
    }
    let current_max = max_shadow_pair_cursor(connection, subject)?;
    if snapshot.through_cursor < snapshot.after_cursor || snapshot.through_cursor > current_max {
        return Err(RebuildStoreError::InvalidLearningCommit(
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
            return Err(RebuildStoreError::InvalidLearningCommit(
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
        return Err(RebuildStoreError::InvalidLearningCommit(
            "policy_evaluation.pair_snapshot_counts",
        ));
    }
    Ok(())
}

fn reject_generic_learning_artifact(artifact: &Artifact) -> RebuildStoreResult<()> {
    if matches!(
        artifact.kind,
        ArtifactKind::Outcome
            | ArtifactKind::Experience
            | ArtifactKind::Evaluation
            | ArtifactKind::CandidatePolicy
    ) {
        return Err(RebuildStoreError::InvalidLearningCommit(
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
) -> RebuildStoreResult<Option<PolicyHead>> {
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
        return Err(RebuildStoreError::Integrity(format!(
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
) -> RebuildStoreResult<Option<PolicyTransitionRecord>> {
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
) -> RebuildStoreResult<Vec<PolicyTransitionRecord>> {
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
                return Err(RebuildStoreError::Integrity(format!(
                    "policy transition {transition_id} subject identity disagrees with key {subject_id}"
                )));
            }
            Ok(PolicyTransitionRecord {
                transition: PolicyTransition {
                    schema_version: REBUILD_SCHEMA_VERSION,
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
) -> RebuildStoreResult<Option<StoredShadowPair>> {
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

fn run_purpose_from_connection(
    connection: &Connection,
    run_id: &RunId,
) -> RebuildStoreResult<RunPurpose> {
    let purpose = connection
        .query_row(
            "SELECT purpose FROM rebuild_runs WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| RebuildStoreError::MissingRun(run_id.clone()))?;
    parse_enum(&purpose)
}

fn workflow_graph_run_purpose(
    connection: &Connection,
    artifact_id: &ArtifactId,
) -> RebuildStoreResult<RunPurpose> {
    let purpose = connection
        .query_row(
            "SELECT purpose FROM rebuild_runs WHERE graph_artifact_id = ?1",
            params![artifact_id.0.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| RebuildStoreError::MissingArtifact(artifact_id.clone()))?;
    parse_enum(&purpose)
}

fn artifact_run_purpose(
    connection: &Connection,
    artifact: &Artifact,
) -> RebuildStoreResult<RunPurpose> {
    let run_id = artifact
        .origin
        .as_ref()
        .and_then(|origin| origin.run_id.as_ref())
        .ok_or(RebuildStoreError::InvalidLearningCommit(
            "learning_artifact.origin",
        ))?;
    run_purpose_from_connection(connection, run_id)
}

fn assert_artifact_from_allowed_purposes(
    connection: &Connection,
    artifact: &Artifact,
    allowed_purposes: &[RunPurpose],
) -> RebuildStoreResult<()> {
    let purpose = artifact_run_purpose(connection, artifact)?;
    if allowed_purposes.contains(&purpose) {
        return Ok(());
    }
    if allowed_purposes == [RunPurpose::Paper] {
        return Err(RebuildStoreError::NonCanonicalLearningPurpose(purpose));
    }
    Err(RebuildStoreError::InvalidLearningCommit(
        "learning_artifact.run_purpose",
    ))
}

fn assert_artifact_from_paper_with_connection(
    connection: &Connection,
    artifact: &Artifact,
) -> RebuildStoreResult<()> {
    assert_artifact_from_allowed_purposes(connection, artifact, &[RunPurpose::Paper])
}

fn assert_paper_run(transaction: &Transaction<'_>, run_id: &RunId) -> RebuildStoreResult<()> {
    let purpose = run_purpose_from_connection(transaction, run_id)?;
    if purpose != RunPurpose::Paper {
        return Err(RebuildStoreError::NonCanonicalLearningPurpose(purpose));
    }
    Ok(())
}

fn read_required_artifact(
    connection: &Connection,
    reference: &ArtifactRef,
    error: &'static str,
) -> RebuildStoreResult<Artifact> {
    let artifact = read_artifact(connection, &reference.artifact_id)?;
    if artifact.kind != reference.kind {
        return Err(RebuildStoreError::InvalidLearningCommit(error));
    }
    Ok(artifact)
}

fn assert_canonical_paper_artifact(
    connection: &Connection,
    artifact: &Artifact,
) -> RebuildStoreResult<()> {
    if artifact.lifecycle != ArtifactLifecycle::Canonical {
        return Err(RebuildStoreError::InvalidLearningCommit(
            "shadow_pair.parent_lifecycle",
        ));
    }
    assert_artifact_from_paper_with_connection(connection, artifact)
}

fn assert_shadow_candidate_artifact(
    connection: &Connection,
    artifact: &Artifact,
) -> RebuildStoreResult<()> {
    match artifact_run_purpose(connection, artifact)? {
        RunPurpose::Paper => Ok(()),
        RunPurpose::Shadow if artifact.lifecycle != ArtifactLifecycle::Canonical => Ok(()),
        RunPurpose::Shadow => Err(RebuildStoreError::InvalidLearningCommit(
            "shadow_pair.candidate_shadow_canonical",
        )),
        _ => Err(RebuildStoreError::InvalidLearningCommit(
            "shadow_pair.candidate_purpose",
        )),
    }
}

fn assert_candidate_decision_binding(
    connection: &Connection,
    candidate_decision: &Artifact,
    completion: &ShadowPairCompletion,
) -> RebuildStoreResult<()> {
    let origin =
        candidate_decision
            .origin
            .as_ref()
            .ok_or(RebuildStoreError::InvalidLearningCommit(
                "shadow_pair.candidate_origin",
            ))?;
    if origin.contract_hash.as_ref() != Some(&completion.candidate_contract_hash) {
        return Err(RebuildStoreError::InvalidLearningCommit(
            "shadow_pair.candidate_contract",
        ));
    }
    let run_id = origin
        .run_id
        .as_ref()
        .ok_or(RebuildStoreError::InvalidLearningCommit(
            "shadow_pair.candidate_run",
        ))?;
    let topology_id = connection
        .query_row(
            "SELECT topology_id FROM rebuild_runs WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| RebuildStoreError::MissingRun(run_id.clone()))?;
    if topology_id != completion.candidate_topology_id {
        return Err(RebuildStoreError::InvalidLearningCommit(
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

fn parse_enum<T: for<'de> serde::Deserialize<'de>>(value: &str) -> RebuildStoreResult<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(RebuildStoreError::Json)
}

fn parse_task_status(value: &str) -> RebuildStoreResult<TaskStatus> {
    match value {
        "queued" => Ok(TaskStatus::Pending),
        "running" => Ok(TaskStatus::Running),
        "succeeded" => Ok(TaskStatus::Succeeded),
        "failed" => Ok(TaskStatus::Failed),
        "cancelled" => Ok(TaskStatus::Cancelled),
        "skipped" => Ok(TaskStatus::Skipped),
        other => Err(RebuildStoreError::Integrity(format!(
            "invalid task status {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use akzio_domain::{
        ArtifactLifecycle, ArtifactProvenance, Asset, ContextPolicy, ExecutionPlan, FactorExposure,
        FailureDisposition, HardBlocker, MoneyMicros, NoOrder, OrderIntent, OrderSide,
        OutputContract, PaperCommitment, PaperCommitmentId, RetryPolicy, TargetPortfolio,
        TaskBudget, TaskRecipeId, TerminationPolicy, ToolGrant, ToolKind, WeightPpm,
        WorkflowProposalTask,
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

    fn contract(store: &RebuildStore, version: u32) -> AgentContract {
        AgentContract::new(
            ContractId::new(),
            version,
            ContractPurpose::new("research.fixture").unwrap(),
            "fixture contract",
            store.put_bytes(b"fixture prompt", "text/plain").unwrap(),
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
        let store = RebuildStore::open(root.path()).unwrap();
        let now = Utc::now();
        let active = contract(&store, 1);
        store.install_active_contract(&active, now).unwrap();

        let mut duplicate = active.clone();
        duplicate.responsibility = "same identity, different contract".to_owned();
        duplicate.contract_hash = duplicate.expected_hash().unwrap();
        duplicate.validate().unwrap();
        assert!(matches!(
            store.install_active_contract(&duplicate, now),
            Err(RebuildStoreError::DuplicateContractVersion { .. })
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
            Err(RebuildStoreError::ContractCapabilityExpansion { .. })
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
            Err(RebuildStoreError::Integrity(_))
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
            let run = RebuildRun {
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
        let record_canary = |from, to, completed_at| -> RebuildStoreResult<()> {
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
                schema_version: REBUILD_SCHEMA_VERSION,
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
                schema_version: REBUILD_SCHEMA_VERSION,
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
        store: &RebuildStore,
        kind: ArtifactKind,
        value: &str,
        origin: Option<ArtifactOrigin>,
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
            vec![],
            Utc::now(),
        )
        .unwrap()
    }

    fn graph() -> WorkflowGraph {
        WorkflowGraph {
            schema_version: REBUILD_SCHEMA_VERSION,
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
        store: &RebuildStore,
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

    fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
        ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        }
    }

    fn valid_execution_commitment(
        store: &RebuildStore,
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
                .write_task_artifact(permit, &artifact, "fixture.source.created", now)
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            .write_task_artifact(permit, &plan, "execution.plan.created", now)
            .unwrap();
        let plan_ref = artifact_ref(&plan);
        let context = permit_artifact(
            store,
            permit,
            ArtifactKind::ExecutionContext,
            &ExecutionContext {
                schema_version: REBUILD_SCHEMA_VERSION,
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
            .write_task_artifact(permit, &context, "execution.context.created", now)
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
            .write_task_artifact(permit, &verdict, "execution.verdict.created", now)
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
        store: RebuildStore,
        lease: DaemonLease,
        permit: TaskWritePermit,
        commitment: Artifact,
        now: DateTime<Utc>,
    }

    fn execution_commit_fixture() -> ExecutionCommitFixture {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
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
        let reservation = store
            .reserve_session_slot(
                &lease,
                &SessionReservation {
                    session_key: "paper:fixture".to_owned(),
                    workflow: WorkflowCommit {
                        run: RebuildRun {
                            run_id: RunId::new(),
                            purpose: RunPurpose::Paper,
                            topology_id: graph.topology_id.clone(),
                            graph_artifact_id: graph_artifact.artifact_id.clone(),
                            created_at: now,
                        },
                        graph: graph_artifact,
                        nodes: graph.nodes,
                    },
                    reserved_at: now,
                },
            )
            .unwrap();
        store.commit_workflow(&reservation.slot.workflow).unwrap();
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
        store: RebuildStore,
        run: RebuildRun,
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
            let store = RebuildStore::open(root.path()).unwrap();
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
            let run = RebuildRun {
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
                schema_version: REBUILD_SCHEMA_VERSION,
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
                let candidate_run = RebuildRun {
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
                schema_version: REBUILD_SCHEMA_VERSION,
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
                schema_version: REBUILD_SCHEMA_VERSION,
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
                schema_version: REBUILD_SCHEMA_VERSION,
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
                    schema_version: REBUILD_SCHEMA_VERSION,
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
                schema_version: REBUILD_SCHEMA_VERSION,
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
                "shadow_pair.completed",
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
        let store = RebuildStore::open(root.path()).unwrap();
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
        let run = RebuildRun {
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
        let store = RebuildStore::open(root.path()).unwrap();
        let mut graph = graph();
        graph.nodes[0].retry.max_attempts = 2;
        graph.nodes[0].retry.initial_backoff_ms = 0;
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = RebuildRun {
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
            Err(RebuildStoreError::StalePermit(_))
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
        fs::write(root.path().join(LEGACY_DATABASE_FILE), b"legacy").unwrap();
        assert!(matches!(
            RebuildStore::open(root.path()),
            Err(RebuildStoreError::IncompatibleStoreRoot(_))
        ));
    }

    #[test]
    fn workflow_commit_is_atomic_and_claim_yields_a_permit() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = RebuildRun {
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
        let store = RebuildStore::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = RebuildRun {
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
            .write_task_artifact(&claimed.permit, &turn, "agent.turn", Utc::now())
            .unwrap();
        assert!(matches!(
            store.committed_attempt_outputs(&claimed.permit.task_id, &claimed.permit.attempt_id),
            Err(RebuildStoreError::CommittedOutputAttempt { .. })
        ));
        let output = artifact(
            &store,
            ArtifactKind::Claim,
            "claim",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
        );

        store
            .commit_attempt(
                &claimed.permit,
                std::slice::from_ref(&output),
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();

        assert_eq!(
            store
                .committed_attempt_outputs(&claimed.permit.task_id, &claimed.permit.attempt_id)
                .unwrap(),
            vec![output.clone()]
        );
        assert_eq!(
            store
                .committed_task_outputs(&run.run_id, &claimed.permit.task_id)
                .unwrap(),
            vec![output]
        );
        assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 5);
        assert!(store
            .claim_next_task("worker", Utc::now(), Duration::seconds(30))
            .unwrap()
            .is_none());
        store.verify_integrity().unwrap();
    }

    #[test]
    fn attempt_commit_resolves_same_batch_evidence_closure_before_persisting() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = RebuildRun {
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
            Err(RebuildStoreError::InvalidArtifactClosure(_))
        ));
        assert!(matches!(
            store.artifact(&missing.artifact_id),
            Err(RebuildStoreError::MissingArtifact(_))
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
        let store = RebuildStore::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = RebuildRun {
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
        let output = artifact(
            &store,
            ArtifactKind::Claim,
            "claim",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
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
                std::slice::from_ref(&output),
                TaskStatus::Succeeded,
                Utc::now()
            ),
            Err(RebuildStoreError::Sql(_))
        ));
        {
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch("DROP TRIGGER fail_terminal_event;")
                .unwrap();
        }
        assert!(matches!(
            store.artifact(&output.artifact_id),
            Err(RebuildStoreError::MissingArtifact(_))
        ));
        assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 2);
        store
            .commit_attempt(
                &claimed.permit,
                &[output],
                TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();
        store.verify_integrity().unwrap();
    }

    #[test]
    fn workflow_patch_rolls_back_proposal_graph_tasks_events_and_planner_completion() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
        let run = RebuildRun {
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            Err(RebuildStoreError::Sql(_))
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
            Err(RebuildStoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.artifact(&evidence_need.artifact_id),
            Err(RebuildStoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.artifact(&proposal_artifact.artifact_id),
            Err(RebuildStoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            store.artifact(&next_graph_artifact.artifact_id),
            Err(RebuildStoreError::MissingArtifact(_))
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
            Err(RebuildStoreError::StalePermit(_))
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
        let store = RebuildStore::open(root.path()).unwrap();
        let graph = graph();
        let graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&graph).unwrap(),
            None,
        );
        let run = RebuildRun {
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
        let artifact = artifact(
            &store,
            ArtifactKind::Claim,
            "claim",
            Some(ArtifactOrigin {
                run_id: Some(claimed.permit.run_id.clone()),
                task_id: Some(claimed.permit.task_id.clone()),
                attempt_id: Some(claimed.permit.attempt_id.clone()),
                contract_hash: None,
            }),
        );
        assert!(matches!(
            store.write_task_artifact(&claimed.permit, &artifact, "claim.created", Utc::now()),
            Err(RebuildStoreError::StalePermit(_))
        ));
    }

    #[test]
    fn bootstrapped_contract_must_not_carry_task_origin() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
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
                "execution.verdict.no_order",
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
            Err(RebuildStoreError::SchedulerFenced(_))
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
            Err(RebuildStoreError::SchedulerFenced(_))
        ));
        assert!(matches!(
            fixture.store.artifact(&receipt.artifact_id),
            Err(RebuildStoreError::MissingArtifact(_))
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
                RebuildStoreError::Integrity(message)
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
                RebuildStoreError::Integrity(message)
                    if message.contains("commitment lineage is invalid")
            ),
            "{error}"
        );
    }

    #[test]
    fn session_slot_is_fenced_and_reuses_the_frozen_workflow() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
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
            run: RebuildRun {
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
            run: RebuildRun {
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

        store.commit_workflow(&first.slot.workflow).unwrap();
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
            Err(RebuildStoreError::Sql(_))
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
            Err(RebuildStoreError::MissingArtifact(_))
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
            Err(RebuildStoreError::StalePermit(_))
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
            Err(RebuildStoreError::SchedulerFenced(_))
        ));
        assert!(matches!(
            store.reserve_session_slot(
                &first_lease,
                &SessionReservation {
                    session_key: "paper:fixture-b".to_owned(),
                    workflow: replacement_workflow,
                    reserved_at: successor_now,
                },
            ),
            Err(RebuildStoreError::SchedulerFenced(_))
        ));
    }

    #[test]
    fn doctor_rejects_a_corrupt_session_slot() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
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
                        run: RebuildRun {
                            run_id: RunId::new(),
                            purpose: RunPurpose::Paper,
                            topology_id: graph.topology_id.clone(),
                            graph_artifact_id: graph_artifact.artifact_id.clone(),
                            created_at: now,
                        },
                        graph: graph_artifact,
                        nodes: graph.nodes,
                    },
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
            Err(RebuildStoreError::Integrity(message)) if message.contains("topology mismatch")
        ));
    }

    #[test]
    fn policy_transition_is_atomic_with_learning_artifacts_and_terminal_event() {
        let root = tempdir().unwrap();
        let store = RebuildStore::open(root.path()).unwrap();
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
        let run = RebuildRun {
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
            schema_version: REBUILD_SCHEMA_VERSION,
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
                schema_version: REBUILD_SCHEMA_VERSION,
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
            matches!(&failed, Err(RebuildStoreError::Sql(_))),
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
            Err(RebuildStoreError::MissingArtifact(_))
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
            matches!(&corrupted, Err(RebuildStoreError::Integrity(_))),
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
                    "fixture.generic_write",
                    fixture.now,
                ),
                Err(RebuildStoreError::InvalidLearningCommit(
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
                Err(RebuildStoreError::InvalidLearningCommit(
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
            RebuildStore::open(root.path()),
            Err(RebuildStoreError::IncompatibleStoreRoot(path)) if path == root.path()
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
            Err(RebuildStoreError::Integrity(_)) => {}
            other => panic!("unexpected Doctor result: {other:?}"),
        }
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
                "policy.transitioned",
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
            Err(RebuildStoreError::Integrity(message)) if message.contains("stale")
        ));
    }
}
