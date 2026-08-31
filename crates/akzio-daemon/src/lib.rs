//! Durable local control plane for Akzio v2.
//!
//! This crate is deliberately thin: it owns worker supervision and transport,
//! while the v2 Store and runtimes own durable state, contracts, context
//! grants, task attempts, and workflow transitions.

mod dispatch;
mod evidence;
mod http;
mod observer;
mod observer_analytics;
mod outcome;
mod scheduler;
mod worker;

pub use scheduler::{
    AlpacaPaperSessionClock, BrokerSessionClock, PaperScheduler, PaperWorkflowSource,
    SchedulerError, StaticPaperWorkflowSource, StorePaperWorkflowSource, SCHEDULER_LEASE_NAME,
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
    content_hash_json, AccountSnapshot, AgentContract, Artifact, ArtifactId, ArtifactKind,
    ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef, Asset, ContentHash,
    ContractPurpose, Decision, DecisionContext, DecisionHorizon, DomainError, EvidenceNeed,
    ExecutionContext, ExecutionVerdict, FreezeState, Lesson, LessonId, LessonLifecycle,
    LessonOrigin, LessonScope, LifecycleEventType, MemoryId, MoneyMicros, OrderReceipt, Outcome,
    OutcomeCostModel, OutcomeExecutionLineage, OutcomeHorizon, OutcomeId, OutcomeSchedule,
    PaperApprovalScope, PaperLaunchApproval, PolicySubject, QuoteSnapshot, Reconciliation,
    ReconciliationState, ResearchClaim, ResearchIntent, Retrospective, RetrospectiveDraft, RunId,
    RunPurpose, RuntimeIdentity, RuntimeManifest, RuntimeTaskClass, TargetPortfolio, TaskId,
    TaskStatus, TaskWritePermit, TopologyId, WorkflowGraph, WorkflowProposal, WorkflowStatus,
    V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_execution::{
    materialize_snapshot_artifact,
    paper::{AlpacaPaper, CommittedPaperBroker, PortfolioHistoryRange},
    DecisionGateError, DecisionGateInput, DecisionPolicy, ExecutionGateError, ExecutionGateInput,
    ExecutionPlan, ExecutionPolicy, PaperCommitmentError, PaperCommitmentInput, PaperDispatchError,
    PaperDispatchFailpoint, PaperDispatchInput, V2DecisionRuntime, V2ExecutionRuntime,
    V2PaperCommitmentRuntime, V2PaperDispatchRuntime,
};
pub use akzio_ingest::AlpacaMarketDataFeed;
use akzio_ingest::{
    common_bar_dates, decode_paper_account, decode_paper_clock, decode_paper_quotes,
    model_native_web_evidence_transport, parse_daily_bars, parse_money_micros, provider_money,
    AcquiredEvidence, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter, EvidenceBundle,
    EvidenceProvenance, EvidenceQuality, EvidenceRequest, EvidenceRuntime, EvidenceRuntimeError,
    EvidenceSource, FixtureEvidenceAdapter, FredDirectTransport, NormalizedEvidencePayload,
    PaperDecodeError, SecEdgarDirectTransport,
};
use akzio_learning::{
    horizon_observations, CanaryBundleComparison, CanaryCampaignRuntime, CanaryError,
    CanarySubjectComparison, CandidatePolicyInput, EvaluationError, EvaluationInput,
    EvaluationPolicy, EvaluationRuntime, OutcomeMaterializationInput, OutcomeScheduleError,
    OutcomeScheduleInput, OutcomeSchedulingRuntime, ShadowObservation,
};
use akzio_model::{ModelClient, ModelConfig, ModelError};
pub use akzio_research::v2::fixture_model_client;
use akzio_research::v2::{
    ActiveResearchCatalogue, AgentReasoningEvent, AgentRunBudget, AgentRuntime, ModelClientAdapter,
    ResearchError,
};
pub use akzio_research::{contract_component_hash, prompt_component_hash};
pub use akzio_runtime::topology_component_hash;
use akzio_runtime::v2::{
    should_run_structured_critique, RetryCause, RuntimeError, StoreExecutor, TaskCompletion,
    TaskRuntime, WorkflowRuntime,
};
use akzio_store::v2::{
    ClaimedAttempt, DaemonLease, LessonUsage, StoreAlert, StoreError, StoreMetrics, StoredEvent,
    StoredLesson, TrajectoryEntry, V2Store, WorkflowSnapshot,
};
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
use serde_json::Value;
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch},
};

const EVENT_PAGE_SIZE: usize = 256;
const PAPER_ACCOUNT_RESOURCE: &str = "paper.account";
const PAPER_POSITIONS_RESOURCE: &str = "paper.positions";
const PAPER_OPEN_ORDERS_RESOURCE: &str = "paper.open_orders";
const PAPER_QUOTES_RESOURCE: &str = "paper.quotes";
const PAPER_CLOCK_RESOURCE: &str = "paper.clock";
const OUTCOME_WORKER_LEASE_NAME: &str = "akzio.local.outcome_worker";

pub type FixtureEvidence = BTreeMap<EvidenceSource, BTreeMap<String, AcquiredEvidence>>;

struct CollectedOutcome {
    materialization: OutcomeMaterializationInput,
    evidence_artifacts: Vec<Artifact>,
}

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
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
    Canary(#[from] CanaryError),
    #[error(transparent)]
    Evaluation(#[from] EvaluationError),
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

impl From<PaperDecodeError> for DaemonError {
    fn from(error: PaperDecodeError) -> Self {
        match error {
            PaperDecodeError::Unavailable(message) => Self::Unavailable(message),
            PaperDecodeError::InvalidInput(message) => Self::InvalidInput(message),
            PaperDecodeError::Json(error) => Self::Json(error),
            PaperDecodeError::Domain(error) => Self::Domain(error),
        }
    }
}

pub type Result<T> = std::result::Result<T, DaemonError>;

#[derive(Debug, Clone)]
pub struct RuntimePolicyIdentity {
    pub decision_policy_hash: ContentHash,
    pub execution_policy_hash: ContentHash,
    pub evaluation_policy_hash: ContentHash,
    pub minimum_evidence_completeness_ppm: u32,
    pub minimum_risk_recall_ppm: u32,
    pub minimum_fresh_pairs_per_horizon: u64,
}

pub fn default_runtime_policy_identity() -> Result<RuntimePolicyIdentity> {
    let evaluation_policy = EvaluationPolicy::default();
    Ok(RuntimePolicyIdentity {
        decision_policy_hash: DecisionPolicy::default()
            .policy_hash()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?,
        execution_policy_hash: ExecutionPolicy::default()
            .policy_hash()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?,
        evaluation_policy_hash: content_hash_json(&serde_json::json!({
            "minimum_evidence_completeness_ppm": evaluation_policy.minimum_evidence_completeness_ppm,
            "minimum_risk_recall_ppm": evaluation_policy.minimum_risk_recall_ppm,
            "minimum_fresh_pairs_per_horizon": evaluation_policy.minimum_fresh_pairs_per_horizon,
        }))?,
        minimum_evidence_completeness_ppm: evaluation_policy.minimum_evidence_completeness_ppm,
        minimum_risk_recall_ppm: evaluation_policy.minimum_risk_recall_ppm,
        minimum_fresh_pairs_per_horizon: evaluation_policy.minimum_fresh_pairs_per_horizon,
    })
}

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub store_root: PathBuf,
    pub http_token: String,
    pub observer_token: Option<String>,
    pub worker_count: usize,
    pub auto_paper: bool,
    pub market_data_feed: Option<AlpacaMarketDataFeed>,
    pub outcome_cost_model: OutcomeCostModel,
    pub runtime_identity_hash: Option<ContentHash>,
}

#[derive(Clone)]
struct DaemonTransport {
    http_token: String,
    observer_token: Option<String>,
    worker_pool: WorkerPoolConfig,
}

#[derive(Clone)]
struct DaemonPaperState {
    paper_broker: Option<Arc<dyn CommittedPaperBroker>>,
    paper_observer: Option<AlpacaPaper>,
    scheduler: PaperScheduler,
    auto_paper: bool,
    runtime_identity_hash: Option<ContentHash>,
    outcome_cost_model: OutcomeCostModel,
}

#[derive(Clone)]
pub struct Daemon {
    store: V2Store,
    workflow: WorkflowRuntime,
    task_runtime: TaskRuntime,
    agents: AgentRuntime,
    model: ModelClientAdapter,
    stage_models: Arc<BTreeMap<String, ModelClientAdapter>>,
    reasoning_events: broadcast::Sender<AgentReasoningEvent>,
    fixture_evidence: Arc<FixtureEvidence>,
    fixture_mode: bool,
    production_evidence: Arc<BTreeMap<EvidenceSource, Arc<dyn AsyncEvidenceAdapter>>>,
    decision_runtime: V2DecisionRuntime,
    execution_runtime: V2ExecutionRuntime,
    paper_commitment_runtime: V2PaperCommitmentRuntime,
    paper_dispatch_runtime: V2PaperDispatchRuntime,
    outcome_scheduling_runtime: OutcomeSchedulingRuntime,
    transport: DaemonTransport,
    paper: DaemonPaperState,
}

impl Daemon {
    fn model_for(&self, purpose: &str) -> &ModelClientAdapter {
        self.stage_models.get(purpose).unwrap_or(&self.model)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonHealth {
    pub status: String,
    pub frozen: bool,
    pub scheduler_owner: Option<String>,
    pub scheduler_epoch: Option<u64>,
    pub metrics: StoreMetrics,
    pub alerts: Vec<StoreAlert>,
}

#[derive(Debug, Serialize)]
pub struct ObserverInvalidation {
    pub cursor: i64,
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
pub struct StoreWorkflowView {
    pub status: WorkflowStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreEventView {
    pub event_type: String,
    pub artifact_id: Option<ArtifactId>,
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
pub struct RetrospectiveView {
    pub artifact_id: ArtifactId,
    pub payload: Retrospective,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct PaperApprovalRequest {
    pub session_key: String,
    pub operator: String,
    pub reason: String,
    pub max_notional_usd_cents: i64,
    pub valid_hours: i64,
    pub identity: RuntimeIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperApprovalResponse {
    pub session_key: String,
    pub runtime_manifest_artifact_id: ArtifactId,
    pub runtime_manifest_hash: ContentHash,
    pub approval_artifact_id: ArtifactId,
    pub approval_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
}

mod orchestration;
fn retry_cause_for_daemon_error(error: &DaemonError) -> Option<RetryCause> {
    match error {
        DaemonError::Research(error) => error.retry_cause(),
        DaemonError::Evidence(EvidenceRuntimeError::Adapter(
            akzio_ingest::runtime::EvidenceAdapterError::Transport(_),
        )) => Some(RetryCause::Transport),
        _ => None,
    }
}

fn debug_fixture_evidence(
    source: EvidenceSource,
    resource: &str,
    now: DateTime<Utc>,
) -> AcquiredEvidence {
    let source_uri = format!("fixture://{}/{resource}", source.as_str());
    let normalized = match resource {
        PAPER_ACCOUNT_RESOURCE => serde_json::json!({
            "equity": "100000",
            "buying_power": "400000",
            "status": "ACTIVE",
            "trading_blocked": false,
        }),
        PAPER_POSITIONS_RESOURCE | PAPER_OPEN_ORDERS_RESOURCE => serde_json::json!([]),
        value if value.starts_with("paper.fills:") => serde_json::json!([]),
        PAPER_QUOTES_RESOURCE => serde_json::json!({
            "quotes": {
                "TQQQ": { "bp": 100.0, "ap": 100.1, "t": now.to_rfc3339() },
                "QQQ": { "bp": 100.0, "ap": 100.1, "t": now.to_rfc3339() },
                "SOXX": { "bp": 100.0, "ap": 100.1, "t": now.to_rfc3339() },
                "SOXL": { "bp": 100.0, "ap": 100.1, "t": now.to_rfc3339() },
            }
        }),
        PAPER_CLOCK_RESOURCE => serde_json::json!({
            "is_open": true,
            "timestamp": now.to_rfc3339(),
        }),
        value if value.starts_with("bars:") => serde_json::json!({
            "bars": [{
                "t": now.to_rfc3339(),
                "o": 100.0,
                "h": 101.0,
                "l": 99.0,
                "c": 100.5,
                "v": 1.0,
            }]
        }),
        value if value.starts_with("news:") => serde_json::json!({
            "answer": "fixture market news",
            "citations": [{ "uri": "https://www.reuters.com/", "published_at": now.to_rfc3339() }],
        }),
        value if value.starts_with("series:") => serde_json::json!({
            "observations": [{ "date": now.date_naive().to_string(), "value": "1.0" }]
        }),
        _ => serde_json::json!({ "resource": resource, "fixture": true }),
    };
    AcquiredEvidence {
        raw: serde_json::to_vec(&normalized).expect("static debug fixture JSON must serialize"),
        media_type: "application/json".to_owned(),
        source_uri: source_uri.clone(),
        observed_at: now,
        normalized,
        provenance: EvidenceProvenance {
            document_id: Some("akzio-debug-fixture".to_owned()),
            published_at: None,
            observed_at: now,
            revision: Some("debug-v1".to_owned()),
            source_uri: source_uri.clone(),
            dedupe_key: format!("akzio-debug-fixture:{resource}"),
            citations: Vec::new(),
        },
        quality: EvidenceQuality::default(),
    }
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

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
