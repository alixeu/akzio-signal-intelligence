//! Durable local control plane for Akzio v2.
//!
//! This crate is deliberately thin: it owns worker supervision and transport,
//! while the v2 Store and runtimes own durable state, contracts, context
//! grants, task attempts, and workflow transitions.

mod dispatch;
mod scheduler;
mod worker;

pub use scheduler::{
    AlpacaPaperSessionClock, BrokerSessionClock, PaperScheduler, PaperWorkflowSource,
    SchedulerError, StaticPaperWorkflowSource, SCHEDULER_LEASE_NAME,
};

pub use worker::{TaskHandler, WorkerPool, WorkerPoolConfig};

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, ContextPolicy, EvidenceNeed, ExecutionContext,
    ExecutionVerdict, FreezeState, MarketClockSnapshot, OutcomeExecutionLineage, QuoteSnapshot,
    RunId, RunPurpose, RuntimeTaskClass, TaskId, TaskStatus, WorkflowProposal, WorkflowStatus,
};
use akzio_execution::{
    paper::CommittedPaperBroker, DecisionGateError, DecisionGateInput, ExecutionGateError,
    ExecutionGateInput, PaperCommitmentError, PaperCommitmentInput, PaperDispatchError,
    PaperDispatchInput, V2DecisionRuntime, V2ExecutionRuntime, V2PaperCommitmentRuntime,
    V2PaperDispatchRuntime,
};
use akzio_ingest::{
    AcquiredEvidence, EvidenceRequest, EvidenceRuntime, EvidenceRuntimeError, EvidenceSource,
    FixtureEvidenceAdapter,
};
use akzio_learning::{OutcomeScheduleError, OutcomeScheduleInput, OutcomeSchedulingRuntime};
use akzio_model::{ModelClient, ModelError};
use akzio_research::v2::{
    ActiveResearchCatalogue, AgentRuntime, ModelClientAdapter, ResearchError,
};
use akzio_runtime::{RuntimeError, TaskCompletion, TaskRuntime, WorkflowRuntime};
use akzio_store::v2::{ClaimedAttempt, StoreError, StoredEvent, V2Store};
use async_stream::stream;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch};

const EVENT_PAGE_SIZE: usize = 256;
const PAPER_ACCOUNT_RESOURCE: &str = "paper.account";
const PAPER_QUOTES_RESOURCE: &str = "paper.quotes";
const PAPER_CLOCK_RESOURCE: &str = "paper.clock";

pub type FixtureEvidence = BTreeMap<EvidenceSource, BTreeMap<String, AcquiredEvidence>>;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Research(#[from] ResearchError),
    #[error(transparent)]
    Evidence(#[from] EvidenceRuntimeError),
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    #[error(transparent)]
    DecisionGate(#[from] DecisionGateError),
    #[error(transparent)]
    ExecutionGate(#[from] ExecutionGateError),
    #[error(transparent)]
    PaperCommitment(#[from] PaperCommitmentError),
    #[error(transparent)]
    PaperDispatch(#[from] PaperDispatchError),
    #[error(transparent)]
    OutcomeSchedule(#[from] OutcomeScheduleError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("invalid daemon input: {0}")]
    InvalidInput(String),
    #[error("{0}")]
    Unavailable(String),
    #[error("task class {0:?} is not safely wired in this daemon checkpoint")]
    UnsupportedTaskClass(RuntimeTaskClass),
    #[error("task {0} has no committed permitted context")]
    MissingTaskContext(TaskId),
    #[error("task {task_id} depends on non-succeeded task {dependency}")]
    UnfinishedDependency { task_id: TaskId, dependency: TaskId },
}

pub type Result<T> = std::result::Result<T, DaemonError>;

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub store_root: PathBuf,
    pub http_token: String,
    pub worker_count: usize,
    pub auto_paper: bool,
}

#[derive(Clone)]
pub struct Daemon {
    store: V2Store,
    workflow: WorkflowRuntime,
    task_runtime: TaskRuntime,
    agents: AgentRuntime,
    model: ModelClientAdapter,
    fixture_evidence: Arc<FixtureEvidence>,
    decision_runtime: V2DecisionRuntime,
    execution_runtime: V2ExecutionRuntime,
    paper_commitment_runtime: V2PaperCommitmentRuntime,
    paper_dispatch_runtime: V2PaperDispatchRuntime,
    outcome_scheduling_runtime: OutcomeSchedulingRuntime,
    paper_broker: Option<Arc<dyn CommittedPaperBroker>>,
    scheduler: PaperScheduler,
    http_token: String,
    auto_paper: bool,
    worker_pool: WorkerPoolConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonHealth {
    pub status: String,
    pub frozen: bool,
    pub scheduler_owner: Option<String>,
    pub scheduler_epoch: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSubmissionResponse {
    pub run_id: RunId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunCancellationResponse {
    pub run_id: RunId,
    pub cancelled_tasks: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRetryResponse {
    pub source_run_id: RunId,
    pub run_id: RunId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayReport {
    pub run_id: RunId,
    pub purpose: RunPurpose,
    pub status: WorkflowStatus,
    pub revision: u64,
    pub task_count: usize,
    pub terminal_task_count: usize,
    pub event_cursor: i64,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventView {
    pub cursor: i64,
    pub event_type: String,
    pub task_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    after: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SubmitRequest {
    purpose: RunPurpose,
}

#[derive(Debug, Deserialize)]
struct FreezeRequest {
    reason: String,
}

impl Daemon {
    /// Construct the local daemon with its production model adapter. Credentials
    /// remain in the environment and are never persisted by this crate.
    pub fn open(config: DaemonConfig) -> Result<Self> {
        Self::with_model(config, ModelClient::from_env()?)
    }

    /// Injecting a model keeps fixture and production dispatch on the same v2
    /// Runtime path. It deliberately installs no evidence adapter: a missing
    /// adapter fails evidence work closed.
    pub fn with_model(config: DaemonConfig, model: ModelClient) -> Result<Self> {
        Self::with_fixture_evidence(config, model, FixtureEvidence::new())
    }

    /// Install deterministic local fixture evidence for tests and replay only.
    /// This adapter has no HTTP or filesystem capability.
    pub fn with_fixture_evidence(
        config: DaemonConfig,
        model: ModelClient,
        fixture_evidence: FixtureEvidence,
    ) -> Result<Self> {
        let store = V2Store::open(&config.store_root)?;
        let active = ActiveResearchCatalogue::install(&store, Utc::now())?;
        let workflow = WorkflowRuntime::new(store.clone(), active.recipes);
        let agents = AgentRuntime::new(store.clone(), active.contracts, Duration::minutes(5));
        let decision_runtime = V2DecisionRuntime::new(store.clone(), Default::default())?;
        let execution_runtime =
            V2ExecutionRuntime::new(store.clone(), Default::default(), Default::default())?;
        let scheduler = PaperScheduler::new(
            store.clone(),
            workflow.clone(),
            format!("akzio-daemon-{}", RunId::new()),
        )?;

        Ok(Self {
            task_runtime: TaskRuntime::new(store.clone()),
            workflow,
            agents,
            model: ModelClientAdapter::new(model),
            fixture_evidence: Arc::new(fixture_evidence),
            decision_runtime,
            execution_runtime,
            paper_commitment_runtime: V2PaperCommitmentRuntime::new(store.clone()),
            paper_dispatch_runtime: V2PaperDispatchRuntime::new(store.clone()),
            outcome_scheduling_runtime: OutcomeSchedulingRuntime::new(store.clone()),
            paper_broker: None,
            scheduler,
            http_token: config.http_token,
            auto_paper: config.auto_paper,
            worker_pool: WorkerPoolConfig {
                worker_count: config.worker_count.max(1),
                ..WorkerPoolConfig::default()
            },
            store,
        })
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }

    /// Install a broker only through dependency injection. Construction of the
    /// daemon itself never reads credentials or performs network I/O.
    pub fn with_paper_broker(mut self, broker: Arc<dyn CommittedPaperBroker>) -> Self {
        self.paper_broker = Some(broker);
        self
    }

    /// Scheduler-only Paper entry point. HTTP and CLI never expose this;
    /// callers must supply the broker-authoritative session key and a Rust
    /// validated workflow proposal.
    pub fn reserve_paper_session(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        now: DateTime<Utc>,
    ) -> Result<akzio_store::v2::SessionSlotReservation> {
        Ok(self.scheduler.reserve_session(session_key, proposal, now)?)
    }

    pub fn reserve_paper_session_with_inputs(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> Result<akzio_store::v2::SessionSlotReservation> {
        Ok(self.scheduler.reserve_session_with_inputs(
            session_key,
            proposal,
            setup_artifacts,
            now,
        )?)
    }

    pub fn reserve_paper_session_with_inputs_for_run(
        &self,
        run_id: RunId,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> Result<akzio_store::v2::SessionSlotReservation> {
        Ok(self.scheduler.reserve_session_with_inputs_for_run(
            run_id,
            session_key,
            proposal,
            setup_artifacts,
            now,
        )?)
    }

    pub async fn serve_scheduler<C, P>(
        &self,
        clock: &C,
        source: &P,
        poll_interval: std::time::Duration,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>
    where
        C: BrokerSessionClock + ?Sized,
        P: PaperWorkflowSource + ?Sized,
    {
        if !self.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper scheduler requires auto_paper=true".to_owned(),
            ));
        }
        self.scheduler
            .serve(clock, source, poll_interval, shutdown)
            .await?;
        Ok(())
    }

    /// Runs the only automatic Paper entrypoint: a broker-authoritative clock,
    /// a Rust-validated workflow source, and the worker pool share shutdown.
    pub async fn serve_with_paper_scheduler<C, P>(
        &self,
        clock: &C,
        source: &P,
        poll_interval: std::time::Duration,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()>
    where
        C: BrokerSessionClock + ?Sized,
        P: PaperWorkflowSource + ?Sized,
    {
        if !self.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper scheduler requires auto_paper=true".to_owned(),
            ));
        }
        tokio::try_join!(
            self.serve_scheduler(clock, source, poll_interval, shutdown.clone()),
            self.serve_worker_pool(shutdown),
        )?;
        Ok(())
    }

    fn request_cancel(&self, run_id: &RunId, reason: &str) -> Result<u64> {
        Ok(u64::from(self.task_runtime.request_cancel(
            run_id,
            reason,
            Utc::now(),
        )?))
    }

    fn retry_run(&self, source_run_id: &RunId) -> Result<RunId> {
        match self.store.run_purpose(source_run_id)? {
            RunPurpose::Debug | RunPurpose::PaperDryRun => {}
            RunPurpose::Paper => {
                return Err(DaemonError::InvalidInput(
                    "Paper runs are scheduler-owned and cannot be retried by an operator"
                        .to_owned(),
                ));
            }
            RunPurpose::Replay | RunPurpose::Shadow => {
                return Err(DaemonError::InvalidInput(
                    "only Debug and Paper Dry Run runs may be retried by an operator".to_owned(),
                ));
            }
        }
        Ok(self.workflow.retry_run(source_run_id, Utc::now())?)
    }

    fn replay_report(&self, run_id: &RunId) -> Result<ReplayReport> {
        let snapshot = self.workflow.replay_run(run_id)?;
        Ok(ReplayReport {
            run_id: snapshot.run.run_id,
            purpose: snapshot.run.purpose,
            status: snapshot.status,
            revision: snapshot.revision.revision,
            task_count: snapshot.tasks.len(),
            terminal_task_count: snapshot
                .tasks
                .iter()
                .filter(|task| {
                    matches!(
                        task.status,
                        TaskStatus::Succeeded
                            | TaskStatus::Failed
                            | TaskStatus::Cancelled
                            | TaskStatus::Skipped
                    )
                })
                .count(),
            event_cursor: snapshot.event_cursor,
            cancel_requested: snapshot.cancel_requested,
        })
    }

    fn set_freeze(&self, frozen: bool, reason: String) -> Result<DaemonHealth> {
        self.store.write_freeze_state(frozen, reason, Utc::now())?;
        self.health()
    }

    /// Paper sessions are scheduler-owned and require a frozen session slot.
    /// The R5 daemon does not construct one directly, so this public submit
    /// surface rejects Paper before any workflow or broker side effect.
    pub fn submit_default(&self, purpose: RunPurpose) -> Result<RunId> {
        match purpose {
            RunPurpose::Debug | RunPurpose::PaperDryRun => {}
            RunPurpose::Paper => {
                return Err(DaemonError::InvalidInput(
                    "Paper runs are scheduler-owned and unavailable until the fenced scheduler is wired"
                        .to_owned(),
                ));
            }
            RunPurpose::Replay | RunPurpose::Shadow => {
                return Err(DaemonError::InvalidInput(
                    "Replay and Shadow runs must be created by their owning runtimes".to_owned(),
                ));
            }
        }

        let run_id = RunId::new();
        let graph = self.workflow.bootstrap(purpose, "active")?;
        self.workflow
            .submit(run_id.clone(), purpose, graph, Utc::now())?;
        Ok(run_id)
    }

    pub async fn run_one(&self, worker_id: &str) -> Result<bool> {
        let daemon = self.clone();
        Ok(self
            .task_runtime
            .run_one(worker_id, move |task| async move {
                daemon.execute_task(task).await
            })
            .await?)
    }

    /// Worker supervision contains no research, execution, or learning policy.
    pub async fn serve_workers(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        if self.auto_paper {
            return Err(DaemonError::InvalidInput(
                "auto_paper requires a broker session clock and Paper workflow source".to_owned(),
            ));
        }
        self.serve_worker_pool(shutdown).await
    }

    async fn serve_worker_pool(&self, shutdown: watch::Receiver<bool>) -> Result<()> {
        let daemon = self.clone();
        let handler: TaskHandler = Arc::new(move |task| {
            let daemon = daemon.clone();
            Box::pin(async move { daemon.execute_task(task).await })
        });
        WorkerPool::new(self.task_runtime.clone(), self.worker_pool.clone())
            .serve(handler, shutdown)
            .await?;
        Ok(())
    }

    pub fn health(&self) -> Result<DaemonHealth> {
        let lease = self
            .store
            .daemon_lease(SCHEDULER_LEASE_NAME)?
            .filter(|lease| lease.expires_at > Utc::now());
        let frozen = self
            .store
            .latest_artifact_by_kind(ArtifactKind::FreezeState)?
            .map(|artifact| {
                let state: FreezeState =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                state
                    .validate()
                    .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
                Ok::<_, DaemonError>(state.frozen)
            })
            .transpose()?
            .unwrap_or(false);
        Ok(DaemonHealth {
            status: if self.auto_paper {
                "paper_scheduler_fail_closed".to_owned()
            } else {
                "ok".to_owned()
            },
            frozen,
            scheduler_owner: lease.as_ref().map(|lease| lease.owner_id.clone()),
            scheduler_epoch: lease.map(|lease| lease.epoch),
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(http_health))
            .route("/runs/{run_id}/events", get(http_events))
            .route("/runs/{run_id}/replay", get(http_replay))
            .route("/runs", post(http_submit))
            .route("/runs/{run_id}/cancel", post(http_cancel))
            .route("/runs/{run_id}/retry", post(http_retry))
            .route("/control/freeze", post(http_freeze))
            .route("/control/unfreeze", post(http_unfreeze))
            .with_state(Arc::new(self.clone()))
    }

    pub async fn serve_http(
        &self,
        address: SocketAddr,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if !address.ip().is_loopback() {
            return Err(DaemonError::InvalidInput(
                "daemon HTTP control API must bind a loopback address".to_owned(),
            ));
        }
        let listener = TcpListener::bind(address).await?;
        axum::serve(listener, self.router())
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await
            .map_err(DaemonError::Io)
    }
    pub(crate) async fn execute_task(&self, task: ClaimedAttempt) -> TaskCompletion {
        match self.execute_task_inner(&task, Utc::now()).await {
            Ok(completion) => completion,
            Err(error) => {
                tracing::warn!(
                    run_id = %task.run_id,
                    task_id = %task.node.task_id,
                    recipe = %task.node.recipe_id,
                    error = %error,
                    "v2 daemon task failed closed"
                );
                TaskCompletion::Failed
            }
        }
    }

    async fn execute_task_inner(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let recipe = self.workflow.catalogue().recipe(&task.node.recipe_id)?;
        match recipe.task_class {
            RuntimeTaskClass::Agent => {
                let candidates = self.context_candidates(task)?;
                let output = self
                    .agents
                    .run(&task.permit, &task.node, candidates, &self.model, now)
                    .await?;
                if task.node.recipe_id.as_str() == "research.planner" {
                    let revision = self.workflow.recover(&task.run_id)?.revision;
                    self.workflow.apply_planner_output(
                        task,
                        &revision.graph_artifact,
                        &revision.graph,
                        &output,
                        now,
                    )?;
                    Ok(TaskCompletion::Committed)
                } else {
                    Ok(TaskCompletion::Succeeded(vec![output]))
                }
            }
            RuntimeTaskClass::Evidence => {
                Ok(TaskCompletion::Succeeded(self.acquire_evidence(task, now)?))
            }
            RuntimeTaskClass::DecisionGate => self.execute_decision_gate(task, now),
            RuntimeTaskClass::ExecutionGate => self.execute_execution_gate(task, now),
            RuntimeTaskClass::PaperCommit => self.execute_paper_commit(task, now),
            RuntimeTaskClass::Reconcile => self.execute_reconcile(task, now).await,
            RuntimeTaskClass::Evaluate => self.execute_evaluate(task, now),
        }
    }

    /// Build agent input strictly from its declared inputs and the Store's
    /// semantic committed-output query for declared, successful dependencies.
    /// This never scans a run's artifact set or exposes raw evidence;
    /// `AgentRuntime` then creates the task-bound ContextManifest and
    /// ReadGrant.
    fn execute_decision_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let proposal = self.terminal_input(task, ArtifactKind::DecisionProposal)?;
        self.decision_runtime.decide(&DecisionGateInput {
            permit: task.permit.clone(),
            proposal,
            now,
        })?;
        Ok(TaskCompletion::Committed)
    }

    fn execute_execution_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let decision_context = self.terminal_input(task, ArtifactKind::DecisionContext)?;
        let (account_snapshot, quote_snapshot, market_clock_snapshot) =
            self.execution_snapshot_inputs(task)?;
        // Snapshot acquisition is a separately governed Evidence path. Until a
        // provider returns typed, task-bound snapshots, the execution runtime
        // emits a durable NoOrder rather than guessing from arbitrary evidence.
        let output = self.execution_runtime.evaluate(&ExecutionGateInput {
            permit: task.permit.clone(),
            decision_context,
            account_snapshot,
            quote_snapshot,
            market_clock_snapshot,
            now,
        })?;
        self.execution_runtime.commit(&task.permit, &output, now)?;
        Ok(TaskCompletion::Committed)
    }

    fn execute_paper_commit(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let verdict = self.terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let ExecutionVerdict::Accepted { execution_context } = verdict_payload else {
            return Ok(TaskCompletion::NoOutput);
        };
        let context: ExecutionContext = self.read_artifact_payload(&execution_context)?;
        context
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let session_key = context.broker_session.ok_or_else(|| {
            DaemonError::InvalidInput("accepted execution verdict has no broker session".to_owned())
        })?;
        let lease = self.scheduler.active_lease(now)?;
        self.paper_commitment_runtime
            .commit(&PaperCommitmentInput {
                lease,
                permit: task.permit.clone(),
                verdict,
                session_key,
                now,
            })?;
        Ok(TaskCompletion::Committed)
    }

    async fn execute_reconcile(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(TaskCompletion::NoOutput);
        }
        let verdict = self.terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        if matches!(verdict_payload, ExecutionVerdict::NoOrder { .. }) {
            return Ok(TaskCompletion::NoOutput);
        }
        let commitment = self.terminal_input(task, ArtifactKind::ExecutionCommitment)?;
        let broker = self.paper_broker.as_ref().ok_or_else(|| {
            DaemonError::Unavailable(
                "Paper reconciliation requires an injected Alpaca Paper broker adapter".to_owned(),
            )
        })?;
        let lease = self.scheduler.active_lease(now)?;
        self.paper_dispatch_runtime
            .dispatch(
                broker.as_ref(),
                &PaperDispatchInput {
                    lease,
                    permit: task.permit.clone(),
                    commitment,
                    now,
                },
            )
            .await?;
        Ok(TaskCompletion::Committed)
    }

    fn execute_evaluate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(TaskCompletion::NoOutput);
        }
        let decision = self.terminal_input(task, ArtifactKind::Decision)?;
        let decision_context = self.terminal_input(task, ArtifactKind::DecisionContext)?;
        let execution_context = self.terminal_input(task, ArtifactKind::ExecutionContext)?;
        let verdict = self.terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let execution = match verdict_payload {
            ExecutionVerdict::NoOrder { .. } => OutcomeExecutionLineage::NoOrder {
                execution_verdict: verdict,
            },
            ExecutionVerdict::Accepted { .. } => OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict: verdict,
                commitment: self.terminal_input(task, ArtifactKind::ExecutionCommitment)?,
                reconciliation: self.terminal_input(task, ArtifactKind::Reconciliation)?,
            },
        };
        let baseline_trading_day = self.paper_baseline_day(&task.run_id)?;
        let output = self
            .outcome_scheduling_runtime
            .schedule(&OutcomeScheduleInput {
                permit: task.permit.clone(),
                decision,
                decision_context,
                execution_context,
                execution,
                baseline_trading_day,
                now,
            })?;
        self.outcome_scheduling_runtime
            .commit(&task.permit, &output, now)?;
        Ok(TaskCompletion::Committed)
    }

    fn paper_baseline_day(&self, run_id: &RunId) -> Result<NaiveDate> {
        let slot = self.store.session_slot_for_run(run_id)?.ok_or_else(|| {
            DaemonError::InvalidInput(format!("Paper run {run_id} has no session slot"))
        })?;
        NaiveDate::parse_from_str(&slot.session_key, "%Y-%m-%d").map_err(|_| {
            DaemonError::InvalidInput(format!(
                "Paper session key {} is not a broker trading date",
                slot.session_key
            ))
        })
    }

    fn terminal_input(&self, task: &ClaimedAttempt, kind: ArtifactKind) -> Result<ArtifactRef> {
        let mut matching = self
            .ancestor_outputs(task)?
            .into_iter()
            .filter(|artifact| artifact.kind == kind)
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: artifact.kind,
            })
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
        matching.dedup_by(|left, right| left.artifact_id == right.artifact_id);
        match matching.as_slice() {
            [reference] => Ok(reference.clone()),
            [] => Err(DaemonError::InvalidInput(format!(
                "terminal task {} has no {:?} input",
                task.node.task_id, kind
            ))),
            _ => Err(DaemonError::InvalidInput(format!(
                "terminal task {} has ambiguous {:?} inputs",
                task.node.task_id, kind
            ))),
        }
    }

    /// Execution snapshots are produced only by the evidence gate from the
    /// scheduler-reserved Alpaca resources below. This prevents arbitrary
    /// normalized evidence from being reinterpreted as broker state.
    fn execution_snapshot_inputs(
        &self,
        task: &ClaimedAttempt,
    ) -> Result<(
        Option<ArtifactRef>,
        Option<ArtifactRef>,
        Option<ArtifactRef>,
    )> {
        let mut account = None;
        let mut quotes = None;
        let mut clock = None;

        for artifact in self.ancestor_outputs(task)? {
            let target = match artifact.producer.as_str() {
                "execution.snapshot.account" => &mut account,
                "execution.snapshot.quotes" => &mut quotes,
                "execution.snapshot.clock" => &mut clock,
                _ => continue,
            };
            if artifact.kind != ArtifactKind::NormalizedEvidence
                || artifact.lifecycle != ArtifactLifecycle::Canonical
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&task.run_id)
                || !artifact
                    .source_refs
                    .iter()
                    .any(|source| source.kind == ArtifactKind::RawEvidence)
                || !artifact
                    .source_refs
                    .iter()
                    .any(|source| source.kind == ArtifactKind::NormalizedEvidence)
            {
                return Err(DaemonError::InvalidInput(format!(
                    "execution snapshot {} has invalid provenance",
                    artifact.artifact_id
                )));
            }
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            };
            if target.replace(reference).is_some() {
                return Err(DaemonError::InvalidInput(
                    "execution gate received duplicate governed snapshot".to_owned(),
                ));
            }
        }

        Ok((account, quotes, clock))
    }

    fn ancestor_outputs(&self, task: &ClaimedAttempt) -> Result<Vec<Artifact>> {
        let snapshot = self.workflow.recover(&task.run_id)?;
        let tasks = snapshot
            .tasks
            .into_iter()
            .map(|stored| (stored.node.task_id.clone(), stored))
            .collect::<BTreeMap<_, _>>();
        let mut pending = task.node.dependencies.clone();
        let mut visited = BTreeSet::new();
        let mut outputs = BTreeMap::<ArtifactId, Artifact>::new();
        while let Some(task_id) = pending.pop() {
            if !visited.insert(task_id.clone()) {
                continue;
            }
            let dependency = tasks.get(&task_id).ok_or_else(|| {
                DaemonError::InvalidInput(format!(
                    "terminal task {} references unknown dependency {task_id}",
                    task.node.task_id
                ))
            })?;
            if dependency.status != TaskStatus::Succeeded {
                return Err(DaemonError::UnfinishedDependency {
                    task_id: task.node.task_id.clone(),
                    dependency: task_id,
                });
            }
            for artifact in self
                .store
                .succeeded_task_outputs_or_empty(&task.run_id, &dependency.node.task_id)?
            {
                outputs.insert(artifact.artifact_id.clone(), artifact);
            }
            pending.extend(dependency.node.dependencies.iter().cloned());
        }
        Ok(outputs.into_values().collect())
    }

    fn read_artifact_payload<T: serde::de::DeserializeOwned>(
        &self,
        reference: &ArtifactRef,
    ) -> Result<T> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if artifact.kind != reference.kind {
            return Err(DaemonError::InvalidInput(format!(
                "artifact {} kind changed from {:?} to {:?}",
                reference.artifact_id, reference.kind, artifact.kind
            )));
        }
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }

    fn context_candidates(&self, task: &ClaimedAttempt) -> Result<Vec<ArtifactRef>> {
        let contract_hash = task.node.contract_hash.as_ref().ok_or_else(|| {
            DaemonError::InvalidInput(format!(
                "agent task {} has no contract hash",
                task.node.task_id
            ))
        })?;
        let policy = &self.agents.catalogue().get(contract_hash)?.contract.context;
        let mut candidates = BTreeMap::<ArtifactId, ArtifactRef>::new();

        for reference in &task.node.input_artifacts {
            self.admit_context_candidate(&mut candidates, policy, reference)?;
        }

        let dependencies = task
            .node
            .dependencies
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if !dependencies.is_empty() {
            let snapshot = self.store.workflow_snapshot(&task.run_id)?;
            for dependency in &dependencies {
                let dependency_task = snapshot
                    .tasks
                    .iter()
                    .find(|stored| stored.node.task_id == *dependency)
                    .ok_or_else(|| {
                        DaemonError::InvalidInput(format!(
                            "task {} references missing dependency {dependency}",
                            task.node.task_id
                        ))
                    })?;
                if dependency_task.status != TaskStatus::Succeeded {
                    return Err(DaemonError::UnfinishedDependency {
                        task_id: task.node.task_id.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }

            for dependency in dependencies {
                for artifact in self
                    .store
                    .committed_task_outputs(&task.run_id, &dependency)?
                {
                    self.admit_context_candidate(
                        &mut candidates,
                        policy,
                        &ArtifactRef {
                            artifact_id: artifact.artifact_id,
                            kind: artifact.kind,
                        },
                    )?;
                }
            }
        }

        if candidates.is_empty() && task.node.recipe_id.as_str() != "research.planner" {
            return Err(DaemonError::MissingTaskContext(task.node.task_id.clone()));
        }
        Ok(candidates.into_values().collect())
    }

    fn admit_context_candidate(
        &self,
        candidates: &mut BTreeMap<ArtifactId, ArtifactRef>,
        policy: &ContextPolicy,
        reference: &ArtifactRef,
    ) -> Result<()> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if artifact.kind == ArtifactKind::RawEvidence {
            return Ok(());
        }
        if policy.permitted_kinds.contains(&artifact.kind)
            && policy
                .permitted_source_families
                .contains(&artifact.provenance.source_family)
        {
            candidates.insert(
                artifact.artifact_id.clone(),
                ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                },
            );
        }
        Ok(())
    }

    fn acquire_evidence(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<Vec<akzio_domain::Artifact>> {
        let mut artifacts = BTreeMap::new();
        for need_reference in &task.node.input_artifacts {
            if need_reference.kind != ArtifactKind::EvidenceNeed {
                return Err(DaemonError::InvalidInput(format!(
                    "evidence task {} has non-EvidenceNeed input",
                    task.node.task_id
                )));
            }
            let need_artifact = self.store.artifact(&need_reference.artifact_id)?;
            let need: EvidenceNeed =
                serde_json::from_slice(&self.store.read_blob(&need_artifact.blob)?)?;
            need.validate()
                .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
            let source = evidence_source(&need.source_family)?;
            let max_age_secs = i64::try_from(need.max_age_secs).map_err(|_| {
                DaemonError::InvalidInput("EvidenceNeed max_age_secs exceeds i64".to_owned())
            })?;
            let responses = self.fixture_evidence.get(&source).ok_or_else(|| {
                DaemonError::Unavailable(format!(
                    "no local fixture adapter configured for evidence source {}",
                    source.as_str()
                ))
            })?;
            let adapter = FixtureEvidenceAdapter::new(
                source,
                responses
                    .iter()
                    .map(|(resource, evidence)| (resource.clone(), evidence.clone())),
            );
            let runtime = EvidenceRuntime::new(self.store.clone(), [source]);
            let bundle = runtime.acquire_and_normalize(
                &task.permit,
                need_reference,
                &EvidenceRequest {
                    source,
                    resource: need.resource.clone(),
                    max_age: Duration::seconds(max_age_secs),
                },
                &adapter,
                now,
            )?;
            if let Some(snapshot) = execution_snapshot_artifact(
                &self.store,
                task,
                &need_artifact,
                &need,
                &bundle.normalized,
                now,
            )? {
                artifacts.insert(snapshot.artifact_id.clone(), snapshot);
            }
            artifacts.insert(bundle.raw.artifact_id.clone(), bundle.raw);
            artifacts.insert(bundle.normalized.artifact_id.clone(), bundle.normalized);
        }
        if artifacts.is_empty() {
            return Err(DaemonError::InvalidInput(format!(
                "evidence task {} has no EvidenceNeed inputs",
                task.node.task_id
            )));
        }
        Ok(artifacts.into_values().collect())
    }
}

fn execution_snapshot_artifact(
    store: &V2Store,
    task: &ClaimedAttempt,
    need_artifact: &Artifact,
    need: &EvidenceNeed,
    normalized: &Artifact,
    now: DateTime<Utc>,
) -> Result<Option<Artifact>> {
    let resource = need.resource.as_str();
    if !matches!(
        resource,
        PAPER_ACCOUNT_RESOURCE | PAPER_QUOTES_RESOURCE | PAPER_CLOCK_RESOURCE
    ) {
        return Ok(None);
    }
    if store.run_purpose(&task.run_id)? != RunPurpose::Paper
        || need.source_family != "alpaca"
        || need_artifact.producer != "scheduler.paper_snapshot"
        || need_artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(&task.run_id)
    {
        return Err(DaemonError::InvalidInput(
            "Paper execution snapshots require scheduler-owned Alpaca evidence needs".to_owned(),
        ));
    }

    let envelope: akzio_ingest::NormalizedEvidencePayload =
        serde_json::from_slice(&store.read_blob(&normalized.blob)?)?;
    match resource {
        PAPER_ACCOUNT_RESOURCE => {
            let payload: AccountSnapshot = serde_json::from_value(envelope.value)?;
            payload
                .validate()
                .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
            seal_execution_snapshot(
                store,
                task,
                normalized,
                "execution.snapshot.account",
                &payload,
                now,
            )
            .map(Some)
        }
        PAPER_QUOTES_RESOURCE => {
            let payload: QuoteSnapshot = serde_json::from_value(envelope.value)?;
            payload
                .validate()
                .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
            seal_execution_snapshot(
                store,
                task,
                normalized,
                "execution.snapshot.quotes",
                &payload,
                now,
            )
            .map(Some)
        }
        PAPER_CLOCK_RESOURCE => {
            let payload: MarketClockSnapshot = serde_json::from_value(envelope.value)?;
            payload
                .validate()
                .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
            seal_execution_snapshot(
                store,
                task,
                normalized,
                "execution.snapshot.clock",
                &payload,
                now,
            )
            .map(Some)
        }
        _ => unreachable!("resource match was checked above"),
    }
}

fn seal_execution_snapshot<T: serde::Serialize>(
    store: &V2Store,
    task: &ClaimedAttempt,
    normalized: &Artifact,
    producer: &str,
    payload: &T,
    now: DateTime<Utc>,
) -> Result<Artifact> {
    Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store.put_json(payload)?,
        producer,
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: normalized.provenance.source_family.clone(),
            observed_at: normalized.provenance.observed_at,
            retrieved_at: now,
            source_uri: normalized.provenance.source_uri.clone(),
            confidence_ppm: normalized.provenance.confidence_ppm,
            producer_contract_hash: task.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(task.run_id.clone()),
            task_id: Some(task.node.task_id.clone()),
            attempt_id: Some(task.permit.attempt_id.clone()),
            contract_hash: task.permit.contract_hash.clone(),
        }),
        {
            let raw = normalized
                .source_refs
                .iter()
                .find(|source| source.kind == ArtifactKind::RawEvidence)
                .cloned()
                .ok_or_else(|| {
                    DaemonError::InvalidInput(
                        "governed normalized evidence has no RawEvidence source".to_owned(),
                    )
                })?;
            vec![
                raw,
                ArtifactRef {
                    artifact_id: normalized.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                },
            ]
        },
        now,
    )
    .map_err(|error| DaemonError::InvalidInput(error.to_string()))
}

fn evidence_source(source_family: &str) -> Result<EvidenceSource> {
    match source_family {
        "alpaca" => Ok(EvidenceSource::Alpaca),
        "sec_edgar" => Ok(EvidenceSource::SecEdgar),
        "fred" => Ok(EvidenceSource::Fred),
        "news_web" => Ok(EvidenceSource::NewsWeb),
        other => Err(DaemonError::InvalidInput(format!(
            "unsupported evidence source family {other}"
        ))),
    }
}

impl From<StoredEvent> for EventView {
    fn from(event: StoredEvent) -> Self {
        Self {
            cursor: event.cursor,
            event_type: event.event_type,
            task_id: event.task_id.map(|task_id| task_id.0),
            created_at: event.created_at.to_rfc3339(),
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

async fn http_health(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .health()
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_events(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    Query(query): Query<EventQuery>,
    headers: HeaderMap,
) -> std::result::Result<
    Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>>,
    StatusCode,
> {
    authorize(&daemon, &headers)?;
    let run_id = RunId(run_id);
    let mut cursor = query.after.unwrap_or(0);
    let stream = stream! {
        loop {
            match daemon.store.events_after(&run_id, cursor, EVENT_PAGE_SIZE) {
                Ok(events) if events.is_empty() => {
                    yield Ok(Event::default().comment("keepalive"));
                }
                Ok(events) => {
                    for event in events {
                        cursor = event.cursor;
                        match serde_json::to_string(&EventView::from(event)) {
                            Ok(data) => {
                                yield Ok(Event::default().id(cursor.to_string()).event("akzio").data(data));
                            }
                            Err(error) => {
                                yield Ok(Event::default().event("error").data(error.to_string()));
                            }
                        }
                    }
                }
                Err(error) => {
                    yield Ok(Event::default().event("error").data(error.to_string()));
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn http_replay(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<ReplayReport>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .replay_report(&RunId(run_id))
        .map(Json)
        .map_err(invalid_input_or_conflict)
}

async fn http_submit(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<SubmitRequest>,
) -> std::result::Result<Json<RunSubmissionResponse>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .submit_default(request.purpose)
        .map(|run_id| Json(RunSubmissionResponse { run_id }))
        .map_err(invalid_input_or_internal)
}

async fn http_cancel(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<RunCancellationResponse>, StatusCode> {
    authorize(&daemon, &headers)?;
    let run_id = RunId(run_id);
    daemon
        .request_cancel(&run_id, "operator cancellation request")
        .map(|cancelled_tasks| {
            Json(RunCancellationResponse {
                run_id,
                cancelled_tasks,
            })
        })
        .map_err(invalid_input_or_conflict)
}

async fn http_retry(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<RunRetryResponse>, StatusCode> {
    authorize(&daemon, &headers)?;
    let source_run_id = RunId(run_id);
    daemon
        .retry_run(&source_run_id)
        .map(|run_id| {
            Json(RunRetryResponse {
                source_run_id,
                run_id,
            })
        })
        .map_err(invalid_input_or_conflict)
}

fn invalid_input_or_internal(error: DaemonError) -> StatusCode {
    if matches!(error, DaemonError::InvalidInput(_)) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

fn invalid_input_or_conflict(error: DaemonError) -> StatusCode {
    if matches!(error, DaemonError::InvalidInput(_)) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::CONFLICT
    }
}

async fn http_freeze(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<FreezeRequest>,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .set_freeze(true, request.reason)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_unfreeze(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<FreezeRequest>,
) -> std::result::Result<Json<DaemonHealth>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .set_freeze(false, request.reason)
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn authorize(daemon: &Daemon, headers: &HeaderMap) -> std::result::Result<(), StatusCode> {
    headers
        .get("x-akzio-token")
        .and_then(|value| value.to_str().ok())
        .filter(|value| *value == daemon.http_token)
        .map(|_| ())
        .ok_or(StatusCode::UNAUTHORIZED)
}

pub fn fixture_model_client() -> ModelClient {
    let planner = serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "topology_id": "active",
        "tasks": {
            "analyst": {
                "recipe_id": "research.analyst",
                "objective": "Produce a fixture claim",
                "depends_on": [],
                "priority": 80,
                "evidence_needs": []
            }
        },
        "stop_reason": "fixture planner has no configured evidence adapter"
    });
    let forecasts = akzio_domain::Asset::EXECUTABLE
        .into_iter()
        .flat_map(|asset| {
            ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                serde_json::json!({
                    "asset": asset.symbol(),
                    "horizon": horizon,
                    "positive_return_probability_ppm": 500000,
                    "expected_return_ppm": 0,
                })
            })
        })
        .collect::<Vec<_>>();
    let response = |output: serde_json::Value| {
        serde_json::json!({
            "output_text": serde_json::to_string(&output).expect("static fixture JSON"),
        })
    };
    ModelClient::FixtureBySchema(BTreeMap::from([
        ("research.planner".to_owned(), response(planner)),
        (
            "research.analyst".to_owned(),
            response(serde_json::json!({
                "summary": "fixture claim",
                "confidence_ppm": 500000,
                "rationale": "fixture-only"
            })),
        ),
        (
            "research.critic".to_owned(),
            response(serde_json::json!({
                "summary": "fixture critique",
                "severity": "low",
                "blocker": false
            })),
        ),
        (
            "research.synthesizer".to_owned(),
            response(serde_json::json!({
                    "summary": "fixture decision draft",
                    "confidence_ppm": 500000,
                    "forecasts": forecasts,
                "claims": [],
                "critiques": [],
                "evidence": [],
                "material_conflicts": [],
                "hard_blockers": ["missing_evidence"],
                "soft_warnings": []
            })),
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };

    use super::*;
    use akzio_domain::{
        ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef, Asset, EvidenceNeed,
        MoneyMicros, OutcomeExecutionLineage, OutcomeSchedule, Quote, TaskRecipeId,
        WorkflowProposal, WorkflowProposalDraft, WorkflowProposalDraftTask, WorkflowProposalTask,
    };
    use akzio_execution::paper::{
        CommittedPaperBroker, PaperError, PaperExecution, PaperOrderReceipt,
    };
    use akzio_ingest::NormalizedEvidencePayload;
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use futures::StreamExt;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn config(root: PathBuf) -> DaemonConfig {
        DaemonConfig {
            store_root: root,
            http_token: "fixture-token".to_owned(),
            worker_count: 1,
            auto_paper: false,
        }
    }

    fn response(output: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "output_text": serde_json::to_string(&output).unwrap(),
        })
    }

    fn planner_with_alpaca_need() -> ModelClient {
        let draft = WorkflowProposalDraft {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                WorkflowProposalDraftTask {
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    objective: "Assess TQQQ fixture evidence".to_owned(),
                    depends_on: vec![],
                    priority: 80,
                    evidence_needs: vec![EvidenceNeed {
                        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
                        source_family: "alpaca".to_owned(),
                        resource: "bars:TQQQ:1d".to_owned(),
                        max_age_secs: 86_400,
                    }],
                },
            )]),
            stop_reason: Some("fixture".to_owned()),
        };
        ModelClient::FixtureBySchema(BTreeMap::from([
            (
                "research.planner".to_owned(),
                response(serde_json::to_value(draft).unwrap()),
            ),
            (
                "research.analyst".to_owned(),
                response(serde_json::json!({
                    "summary": "fixture claim",
                    "confidence_ppm": 500000,
                    "rationale": "normalized fixture evidence"
                })),
            ),
        ]))
    }

    fn paper_proposal() -> WorkflowProposal {
        WorkflowProposal {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "paper-fixture".to_owned(),
            tasks: BTreeMap::from([(
                "synthesizer".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.synthesizer").unwrap(),
                    objective: "Create a fixture Paper decision proposal".to_owned(),
                    depends_on: vec![],
                    priority: 100,
                    evidence_needs: vec![],
                },
            )]),
            stop_reason: Some("fixture Paper workflow".to_owned()),
        }
    }

    fn accepted_paper_decision(claim: ArtifactRef) -> serde_json::Value {
        let forecasts = Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                    serde_json::json!({
                        "asset": asset.symbol(),
                        "horizon": horizon,
                        "positive_return_probability_ppm": if asset == Asset::Qqq { 900000 } else { 500000 },
                        "expected_return_ppm": if asset == Asset::Qqq { 100000 } else { 0 },
                    })
                })
            })
            .collect::<Vec<_>>();
        response(serde_json::json!({
            "summary": "fixture accepted Paper decision",
            "confidence_ppm": 900000,
            "forecasts": forecasts,
            "claims": [claim],
            "critiques": [],
            "evidence": [],
            "material_conflicts": [],
            "hard_blockers": [],
            "soft_warnings": []
        }))
    }

    fn scheduler_snapshot_need(
        store: &V2Store,
        run_id: &RunId,
        resource: &str,
        now: DateTime<Utc>,
    ) -> Artifact {
        let need = EvidenceNeed {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            source_family: "alpaca".to_owned(),
            resource: resource.to_owned(),
            max_age_secs: 5,
        };
        Artifact::new(
            ArtifactKind::EvidenceNeed,
            store.put_json(&need).unwrap(),
            "scheduler.paper_snapshot",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.scheduler".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(ArtifactOrigin {
                run_id: Some(run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
            now,
        )
        .unwrap()
    }

    #[derive(Clone)]
    struct StaticSessionClock(Option<String>);

    impl BrokerSessionClock for StaticSessionClock {
        fn open_session_key<'a>(
            &'a self,
        ) -> Pin<
            Box<
                dyn Future<Output = std::result::Result<Option<String>, SchedulerError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move { Ok(self.0.clone()) })
        }
    }

    #[derive(Default)]
    struct FakePaperBroker {
        submissions: AtomicUsize,
    }

    impl CommittedPaperBroker for FakePaperBroker {
        fn execute_commitment<'a>(
            &'a self,
            commitment: &'a akzio_domain::PaperCommitment,
            plan: &'a akzio_execution::ExecutionPlan,
        ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
        {
            self.submissions.fetch_add(1, Ordering::SeqCst);
            let execution = PaperExecution {
                plan_hash: plan.plan_hash.clone(),
                orders: plan
                    .orders
                    .iter()
                    .map(|order| PaperOrderReceipt {
                        client_order_id: commitment.client_order_ids[&order.asset].clone(),
                        broker_order_id: format!("fixture-{}", order.asset.symbol()),
                        symbol: order.asset.symbol().to_owned(),
                        status: "filled".to_owned(),
                        requested_quantity_micros: 1_000_000,
                        filled_quantity_micros: 1_000_000,
                        remaining_quantity_micros: 0,
                        average_fill_price: Some(order.limit_price),
                        broker_updated_at: Utc::now(),
                        reason: None,
                        reused: false,
                        reprice_count: 0,
                    })
                    .collect(),
            };
            Box::pin(async move { Ok(execution) })
        }

        fn replace_commitment_once<'a>(
            &'a self,
            _commitment: &'a akzio_domain::PaperCommitment,
            _reprice: &'a akzio_domain::PaperReprice,
            _replacement: &'a akzio_execution::OrderIntent,
        ) -> Pin<
            Box<dyn Future<Output = akzio_execution::paper::Result<PaperOrderReceipt>> + Send + 'a>,
        > {
            Box::pin(async {
                Err(PaperError::InvalidCommitment(
                    "fixture has no reprice".to_owned(),
                ))
            })
        }

        fn reconcile_commitment<'a>(
            &'a self,
            _commitment: &'a akzio_domain::PaperCommitment,
            execution: &'a PaperExecution,
        ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
        {
            let execution = execution.clone();
            Box::pin(async move { Ok(execution) })
        }
    }

    #[tokio::test]
    async fn planner_task_runs_agent_runtime_and_commits_graph_patch() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

        assert!(daemon.run_one("fixture").await.unwrap());

        let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
        eprintln!("{snapshot:#?}");
        assert!(snapshot
            .revision
            .graph
            .nodes
            .iter()
            .any(|node| node.recipe_id.as_str() == "research.analyst"));
        assert!(daemon
            .store()
            .events_after(&run_id, 0, 64)
            .unwrap()
            .iter()
            .any(|event| event.event_type == "task.succeeded"));
        daemon.store().verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn evidence_gate_resolves_need_with_fixture_adapter_and_keeps_provenance() {
        let directory = tempdir().unwrap();
        let observed_at = Utc::now();
        let fixture_evidence = BTreeMap::from([(
            EvidenceSource::Alpaca,
            BTreeMap::from([(
                "bars:TQQQ:1d".to_owned(),
                AcquiredEvidence {
                    raw: br#"{\"bars\":[{\"close\":100}]}"#.to_vec(),
                    media_type: "application/json".to_owned(),
                    source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                    observed_at,
                    normalized: serde_json::json!({"close": 100}),
                },
            )]),
        )]);
        let daemon = Daemon::with_fixture_evidence(
            config(directory.path().to_path_buf()),
            planner_with_alpaca_need(),
            fixture_evidence,
        )
        .unwrap();
        let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

        for _ in 0..16 {
            if !daemon.run_one("fixture").await.unwrap() {
                break;
            }
        }
        let artifacts = daemon
            .store()
            .events_after(&run_id, 0, 256)
            .unwrap()
            .into_iter()
            .filter_map(|event| event.artifact_id)
            .filter_map(|artifact_id| daemon.store().artifact(&artifact_id).ok())
            .collect::<Vec<_>>();
        let normalized = artifacts
            .iter()
            .find(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence)
            .expect("evidence gate committed normalized fixture evidence");
        let payload: NormalizedEvidencePayload =
            serde_json::from_slice(&daemon.store().read_blob(&normalized.blob).unwrap()).unwrap();
        assert_eq!(payload.resource, "bars:TQQQ:1d");
        assert_eq!(payload.need.kind, ArtifactKind::EvidenceNeed);
        assert!(artifacts
            .iter()
            .any(|artifact| artifact.kind == ArtifactKind::Claim));

        let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
        let task_status = |recipe_id: &str| {
            snapshot
                .tasks
                .iter()
                .find(|task| task.node.recipe_id.as_str() == recipe_id)
                .map(|task| task.status)
        };
        assert_eq!(task_status("research.analyst"), Some(TaskStatus::Succeeded));
        assert_eq!(task_status("gate.decision"), Some(TaskStatus::Failed));
        daemon.store().verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn scheduler_owned_paper_run_forwards_no_order_and_schedules_outcome() {
        let directory = tempdir().unwrap();
        let fixture_evidence = BTreeMap::from([(
            EvidenceSource::Alpaca,
            BTreeMap::from([(
                "bars:TQQQ:1d".to_owned(),
                AcquiredEvidence {
                    raw: br#"{\"bars\":[{\"close\":100}]}"#.to_vec(),
                    media_type: "application/json".to_owned(),
                    source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                    observed_at: Utc::now(),
                    normalized: serde_json::json!({"close": 100}),
                },
            )]),
        )]);
        let daemon = Daemon::with_fixture_evidence(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
            fixture_evidence,
        )
        .unwrap();
        let now = Utc::now();
        let paper_run_id = RunId::new();
        let need = EvidenceNeed {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            source_family: "alpaca".to_owned(),
            resource: "bars:TQQQ:1d".to_owned(),
            max_age_secs: 86_400,
        };
        let need_artifact = Artifact::new(
            ArtifactKind::EvidenceNeed,
            daemon.store().put_json(&need).unwrap(),
            "scheduler.fixture",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.scheduler".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(ArtifactOrigin {
                run_id: Some(paper_run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
            now,
        )
        .unwrap();
        let mut proposal = paper_proposal();
        proposal
            .tasks
            .get_mut("synthesizer")
            .unwrap()
            .evidence_needs = vec![ArtifactRef {
            artifact_id: need_artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        }];
        let session_key = now.date_naive().to_string();
        let slot = daemon
            .reserve_paper_session_with_inputs_for_run(
                paper_run_id,
                &session_key,
                &proposal,
                &[need_artifact],
                now,
            )
            .unwrap();
        assert!(slot.newly_reserved);
        let run_id = slot.slot.workflow.run.run_id.clone();

        for _ in 0..32 {
            if !daemon.run_one("paper-fixture").await.unwrap() {
                break;
            }
        }

        let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
        assert!(
            snapshot
                .tasks
                .iter()
                .all(|task| task.status == TaskStatus::Succeeded),
            "statuses: {:?}",
            snapshot
                .tasks
                .iter()
                .map(|task| format!("{}={:?}", task.node.recipe_id, task.status))
                .collect::<Vec<_>>()
        );
        let schedule = daemon
            .store()
            .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
            .unwrap()
            .expect("Paper terminal chain must schedule future outcome");
        let payload: OutcomeSchedule =
            serde_json::from_slice(&daemon.store().read_blob(&schedule.blob).unwrap()).unwrap();
        assert_eq!(payload.baseline_trading_day, now.date_naive());
        assert!(matches!(
            payload.execution,
            OutcomeExecutionLineage::NoOrder { .. }
        ));
        assert!(daemon.store().session_slot(&session_key).unwrap().is_some());
        daemon.store().verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn paper_fixture_snapshots_reach_accepted_commit_reconcile_and_outcome_schedule() {
        let directory = tempdir().unwrap();
        let now = Utc::now();
        let session_key = now.date_naive().to_string();
        let account = AccountSnapshot {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            broker_session: session_key.clone(),
            observed_at: now,
            equity: MoneyMicros::from_usd_cents(1_000_000),
            buying_power: MoneyMicros::from_usd_cents(1_000_000),
            day_turnover: MoneyMicros::ZERO,
            active: true,
            trading_blocked: false,
            positions: BTreeMap::new(),
        };
        let quotes = QuoteSnapshot {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            broker_session: session_key.clone(),
            observed_at: now,
            quotes: BTreeMap::from([(
                Asset::Qqq,
                Quote {
                    bid: MoneyMicros::from_usd_cents(10_000),
                    ask: MoneyMicros::from_usd_cents(10_010),
                    observed_at: now,
                },
            )]),
        };
        let clock = MarketClockSnapshot {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            broker_session: session_key.clone(),
            is_open: true,
            observed_at: now,
        };
        let evidence = [
            (
                PAPER_ACCOUNT_RESOURCE,
                serde_json::to_value(&account).unwrap(),
            ),
            (
                PAPER_QUOTES_RESOURCE,
                serde_json::to_value(&quotes).unwrap(),
            ),
            (PAPER_CLOCK_RESOURCE, serde_json::to_value(&clock).unwrap()),
        ]
        .into_iter()
        .map(|(resource, normalized)| {
            (
                resource.to_owned(),
                AcquiredEvidence {
                    raw: serde_json::to_vec(&normalized).unwrap(),
                    media_type: "application/json".to_owned(),
                    source_uri: format!("fixture://alpaca/{resource}"),
                    observed_at: now,
                    normalized,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
        let responses = Arc::new(Mutex::new(VecDeque::from([response(serde_json::json!({
            "summary": "fixture accepted Paper claim",
            "confidence_ppm": 900000,
            "rationale": "governed Paper snapshots"
        }))])));
        let broker = Arc::new(FakePaperBroker::default());
        let daemon = Daemon::with_fixture_evidence(
            config(directory.path().to_path_buf()),
            ModelClient::FixtureSequence(responses.clone()),
            BTreeMap::from([(EvidenceSource::Alpaca, evidence)]),
        )
        .unwrap()
        .with_paper_broker(broker.clone());
        let paper_run_id = RunId::new();
        let setup_artifacts = [
            scheduler_snapshot_need(daemon.store(), &paper_run_id, PAPER_ACCOUNT_RESOURCE, now),
            scheduler_snapshot_need(daemon.store(), &paper_run_id, PAPER_QUOTES_RESOURCE, now),
            scheduler_snapshot_need(daemon.store(), &paper_run_id, PAPER_CLOCK_RESOURCE, now),
        ];
        let snapshot_refs = setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::EvidenceNeed,
            })
            .collect::<Vec<_>>();
        let mut proposal = paper_proposal();
        proposal.tasks.insert(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "Assess governed Paper snapshots".to_owned(),
                depends_on: vec![],
                priority: 90,
                evidence_needs: snapshot_refs,
            },
        );
        proposal.tasks.get_mut("synthesizer").unwrap().depends_on = vec!["analyst".to_owned()];
        let slot = daemon
            .reserve_paper_session_with_inputs_for_run(
                paper_run_id,
                &session_key,
                &proposal,
                &setup_artifacts,
                now,
            )
            .unwrap();
        let run_id = slot.slot.workflow.run.run_id.clone();

        let evidence_task = daemon
            .store()
            .claim_next_task("accepted-paper-evidence", now, Duration::seconds(30))
            .unwrap()
            .unwrap();
        let evidence_outputs = daemon
            .acquire_evidence(&evidence_task, now)
            .expect("fixture snapshots must be valid governed evidence");
        daemon
            .store()
            .commit_attempt(
                &evidence_task.permit,
                &evidence_outputs,
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();

        assert!(daemon.run_one("accepted-paper-analyst").await.unwrap());
        let analyst_task = daemon
            .store()
            .workflow_snapshot(&run_id)
            .unwrap()
            .tasks
            .into_iter()
            .find(|task| task.node.recipe_id.as_str() == "research.analyst")
            .expect("fixture workflow must contain analyst")
            .node
            .task_id;
        let claim = daemon
            .store()
            .committed_task_outputs(&run_id, &analyst_task)
            .unwrap()
            .into_iter()
            .find(|artifact| artifact.kind == ArtifactKind::Claim)
            .expect("analyst must emit a Claim");
        responses
            .lock()
            .unwrap()
            .push_back(accepted_paper_decision(ArtifactRef {
                artifact_id: claim.artifact_id,
                kind: ArtifactKind::Claim,
            }));
        assert!(daemon.run_one("accepted-paper-synthesizer").await.unwrap());

        for _ in 0..32 {
            if !daemon.run_one("accepted-paper-fixture").await.unwrap() {
                break;
            }
        }

        let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
        assert!(
            snapshot
                .tasks
                .iter()
                .all(|task| task.status == TaskStatus::Succeeded),
            "statuses: {:?}",
            snapshot
                .tasks
                .iter()
                .map(|task| format!("{}={:?}", task.node.recipe_id, task.status))
                .collect::<Vec<_>>()
        );
        assert_eq!(broker.submissions.load(Ordering::SeqCst), 1);
        let schedule = daemon
            .store()
            .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
            .unwrap()
            .expect("accepted fixture Paper chain must schedule an outcome");
        let payload: OutcomeSchedule =
            serde_json::from_slice(&daemon.store().read_blob(&schedule.blob).unwrap()).unwrap();
        assert!(matches!(
            payload.execution,
            OutcomeExecutionLineage::ReconciledPaper { .. }
        ));
        assert!(daemon
            .store()
            .artifacts_referencing(&schedule.artifact_id, None)
            .unwrap()
            .is_empty());
        assert!(daemon
            .store()
            .events_after(&run_id, 0, 256)
            .unwrap()
            .iter()
            .any(|event| event.event_type == "execution.committed"));
        daemon.store().verify_integrity().unwrap();
    }

    #[test]
    fn scheduler_fences_stale_daemon_and_reuses_frozen_session_workflow() {
        let directory = tempdir().unwrap();
        let first = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let second = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let now = Utc::now();
        let session_key = now.date_naive().to_string();
        let first_slot = first
            .reserve_paper_session(&session_key, &paper_proposal(), now)
            .unwrap();
        assert!(matches!(
            second.reserve_paper_session(&session_key, &paper_proposal(), now),
            Err(DaemonError::Scheduler(SchedulerError::NotLeader))
        ));

        let recovered = second
            .reserve_paper_session(&session_key, &paper_proposal(), now + Duration::seconds(31))
            .unwrap();
        assert!(!recovered.newly_reserved);
        assert_eq!(
            recovered.slot.workflow.run.run_id,
            first_slot.slot.workflow.run.run_id
        );
        assert!(matches!(
            first.reserve_paper_session(
                &session_key,
                &paper_proposal(),
                now + Duration::seconds(31),
            ),
            Err(DaemonError::Scheduler(SchedulerError::NotLeader))
        ));
        first.store().verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn auto_paper_requires_an_injected_scheduler_loop() {
        let directory = tempdir().unwrap();
        let mut daemon_config = config(directory.path().to_path_buf());
        daemon_config.auto_paper = true;
        let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
        let (_shutdown, receiver) = watch::channel(false);

        assert!(matches!(
            daemon.serve_workers(receiver).await,
            Err(DaemonError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn auto_paper_supervisor_reserves_an_open_broker_session() {
        let directory = tempdir().unwrap();
        let mut daemon_config = config(directory.path().to_path_buf());
        daemon_config.auto_paper = true;
        let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
        let session_key = Utc::now().date_naive().to_string();
        let clock = Arc::new(StaticSessionClock(Some(session_key.clone())));
        let source = Arc::new(StaticPaperWorkflowSource::new(paper_proposal()));
        let (shutdown, receiver) = watch::channel(false);
        let supervised = daemon.clone();
        let task = tokio::spawn(async move {
            supervised
                .serve_with_paper_scheduler(
                    clock.as_ref(),
                    source.as_ref(),
                    std::time::Duration::from_millis(1),
                    receiver,
                )
                .await
        });

        let mut reserved = None;
        for _ in 0..50 {
            reserved = daemon.store().session_slot(&session_key).unwrap();
            if reserved.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        shutdown.send(true).unwrap();
        assert!(task.await.unwrap().is_ok());
        assert!(reserved.is_some());
        daemon.store().verify_integrity().unwrap();
    }

    #[test]
    fn cancellation_and_freeze_are_durable_store_owned_transitions() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
        assert_eq!(
            daemon
                .request_cancel(&run_id, "fixture cancellation request")
                .unwrap(),
            1
        );
        assert!(daemon.store().run_cancel_requested(&run_id).unwrap());

        assert!(
            daemon
                .set_freeze(true, "fixture freeze".to_owned())
                .unwrap()
                .frozen
        );
        let reopened = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        assert!(reopened.health().unwrap().frozen);
        assert!(
            !reopened
                .set_freeze(false, "fixture operator unfreeze".to_owned())
                .unwrap()
                .frozen
        );
        reopened.store().verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn http_control_rejects_non_loopback_bind() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let (_shutdown, receiver) = watch::channel(false);
        assert!(matches!(
            daemon
                .serve_http("0.0.0.0:0".parse().unwrap(), receiver)
                .await,
            Err(DaemonError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn http_control_auth_cancel_retry_and_freeze_are_governed() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();

        let unauthorized = daemon
            .router()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = daemon
            .router()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .header("x-akzio-token", "fixture-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);

        let submitted = daemon
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs")
                    .header("x-akzio-token", "fixture-token")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"purpose":"debug"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submitted.status(), StatusCode::OK);
        let run_id = serde_json::from_slice::<RunSubmissionResponse>(
            &to_bytes(submitted.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap()
        .run_id;

        let cancelled = daemon
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/runs/{run_id}/cancel"))
                    .header("x-akzio-token", "fixture-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancelled.status(), StatusCode::OK);
        assert!(daemon.store().run_cancel_requested(&run_id).unwrap());

        let retried = daemon
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/runs/{run_id}/retry"))
                    .header("x-akzio-token", "fixture-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retried.status(), StatusCode::OK);
        let retry = serde_json::from_slice::<RunRetryResponse>(
            &to_bytes(retried.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(retry.source_run_id, run_id);
        let retry_run_id = retry.run_id;
        assert_eq!(
            daemon.store().run_purpose(&retry_run_id).unwrap(),
            RunPurpose::Debug
        );

        let frozen = daemon
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/control/freeze")
                    .header("x-akzio-token", "fixture-token")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"reason":"fixture freeze"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(frozen.status(), StatusCode::OK);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(frozen.into_body(), usize::MAX).await.unwrap(),
            )
            .unwrap()["frozen"]
                .as_bool(),
            Some(true)
        );

        let unfrozen = daemon
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/control/unfreeze")
                    .header("x-akzio-token", "fixture-token")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"reason":"fixture unfreeze"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unfrozen.status(), StatusCode::OK);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &to_bytes(unfrozen.into_body(), usize::MAX).await.unwrap(),
            )
            .unwrap()["frozen"]
                .as_bool(),
            Some(false)
        );
        daemon.store().verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn http_sse_resumes_from_the_requested_cursor() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
        daemon
            .request_cancel(&run_id, "fixture cancellation request")
            .unwrap();
        let events = daemon.store().events_after(&run_id, 0, 16).unwrap();
        assert!(events.len() >= 2);
        let after = events[0].cursor;
        let expected = &events[1];

        let response = daemon
            .router()
            .oneshot(
                Request::builder()
                    .uri(format!("/runs/{run_id}/events?after={after}"))
                    .header("x-akzio-token", "fixture-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let frame = String::from_utf8(frame.to_vec()).unwrap();
        assert!(frame.contains(&format!("id: {}", expected.cursor)));
        assert!(frame.contains(&expected.event_type));
    }

    #[tokio::test]
    async fn http_replay_reports_the_durable_snapshot() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

        let response = daemon
            .router()
            .oneshot(
                Request::builder()
                    .uri(format!("/runs/{run_id}/replay"))
                    .header("x-akzio-token", "fixture-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let report = serde_json::from_slice::<ReplayReport>(
            &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(report.run_id, run_id);
        assert_eq!(report.purpose, RunPurpose::Debug);
        assert!(report.task_count > 0);
        assert_eq!(report.revision, 0);
    }

    #[test]
    fn paper_submit_and_direct_retry_fail_closed() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        assert!(matches!(
            daemon.submit_default(RunPurpose::Paper),
            Err(DaemonError::InvalidInput(_))
        ));
        assert!(daemon.retry_run(&RunId::new()).is_err());
    }

    #[tokio::test]
    async fn retry_starts_a_fresh_terminal_nonpaper_run() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let source_run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
        daemon
            .request_cancel(&source_run_id, "fixture cancellation request")
            .unwrap();

        let run_id = daemon.retry_run(&source_run_id).unwrap();
        assert_ne!(run_id, source_run_id);
        assert_eq!(
            daemon.store().run_purpose(&run_id).unwrap(),
            RunPurpose::Debug
        );
        daemon.store().verify_integrity().unwrap();
    }

    #[test]
    fn direct_submit_allows_only_debug_and_paper_dry_run() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();

        for purpose in [RunPurpose::Paper, RunPurpose::Replay, RunPurpose::Shadow] {
            assert!(matches!(
                daemon.submit_default(purpose),
                Err(DaemonError::InvalidInput(_))
            ));
        }

        assert!(daemon.submit_default(RunPurpose::Debug).is_ok());
        assert!(daemon.submit_default(RunPurpose::PaperDryRun).is_ok());
    }

    #[test]
    fn operator_retry_rejects_paper_run() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();
        let now = Utc::now();
        let session_key = now.date_naive().to_string();
        let run_id = daemon
            .reserve_paper_session(&session_key, &paper_proposal(), now)
            .unwrap()
            .slot
            .workflow
            .run
            .run_id;

        assert!(matches!(
            daemon.retry_run(&run_id),
            Err(DaemonError::InvalidInput(message)) if message.contains("scheduler-owned")
        ));
    }

    #[tokio::test]
    async fn http_submit_rejects_replay_before_workflow_creation() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            config(directory.path().to_path_buf()),
            fixture_model_client(),
        )
        .unwrap();

        let response = daemon
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/runs")
                    .header("x-akzio-token", "fixture-token")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"purpose":"replay"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
