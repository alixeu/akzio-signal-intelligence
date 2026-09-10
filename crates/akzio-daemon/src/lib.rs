//! Durable local control plane for Akzio.
//!
//! This crate is deliberately thin: it owns worker supervision and transport,
//! while the Store and runtimes own durable state, contracts, context
//! grants, task attempts, and workflow transitions.

mod application;
mod debug;
mod dispatch;
pub use debug::{DebugForkRequest, DebugPrepareRequest};
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
    content_hash_json, evidence_acquisition_mode, AccountSnapshot, AgentContract, Artifact,
    ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    Asset, CanaryPairedObservation, CanaryPairedOutcomeMetrics, CanaryPairedSubjectMetrics,
    ContentHash, ContractPurpose, Decision, DecisionContext, DecisionHorizon, DomainError,
    EvidenceAcquisitionMode, EvidenceNeed, ExecutionContext, ExecutionVerdict, ExperimentCondition,
    FreezeState, Lesson, LessonId, LessonLifecycle, LessonOrigin, LessonScope, LifecycleEventType,
    MemoryId, ModelQualificationGate, ModelQualificationReport, MoneyMicros, OrderReceipt, Outcome,
    OutcomeCostModel, OutcomeExecutionLineage, OutcomeHorizon, OutcomeId, OutcomeSchedule,
    PaperApprovalScope, PaperLaunchApproval, PolicySubject, QuoteSnapshot, Reconciliation,
    ReconciliationState, ResearchClaim, ResearchIntent, Retrospective, RetrospectiveDraft,
    RiskGroundTruthAssessment, RunId, RunPurpose, RuntimeIdentity, RuntimeManifest,
    RuntimeTaskClass, TargetPortfolio, TaskId, TaskStatus, TaskWritePermit, TopologyId,
    WorkflowGraph, WorkflowProposal, WorkflowStatus, DOMAIN_SCHEMA_VERSION,
};
use akzio_execution::{
    materialize_snapshot_artifact,
    paper::{AlpacaPaper, CommittedPaperBroker, PortfolioHistoryRange},
    DecisionGateError, DecisionGateInput, DecisionPolicy, DecisionRuntime, ExecutionGateError,
    ExecutionGateInput, ExecutionGatePolicy, ExecutionPlan, ExecutionPolicy, ExecutionRuntime,
    PaperCommitmentError, PaperCommitmentInput, PaperCommitmentRuntime, PaperDispatchError,
    PaperDispatchFailpoint, PaperDispatchInput, PaperDispatchRuntime,
    DEFAULT_PAPER_SETTLEMENT_ACTION_GRACE_SECS, DEFAULT_PAPER_SETTLEMENT_TIMEOUT_SECS,
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
    apply_risk_ground_truth_assessments, daily_observations, evaluate_canary_cohort,
    horizon_observations, CanaryCampaignRuntime, CanaryError, CandidatePolicyInput,
    EvaluationError, EvaluationInput, EvaluationPolicy, EvaluationRuntime,
    OutcomeMaterializationInput, OutcomeScheduleError, OutcomeScheduleInput,
    OutcomeSchedulingRuntime, ShadowObservation,
};
use akzio_model::{ModelCapabilityProbeSet, ModelClient, ModelConfig, ModelError};
pub use akzio_research::fixture_model_client;
pub use akzio_research::{contract_component_hash, prompt_component_hash};
use akzio_research::{
    ActiveResearchCatalogue, AgentReasoningEvent, AgentRunBudget, AgentRuntime, ModelClientAdapter,
    ResearchError,
};
pub use akzio_runtime::topology_component_hash;
use akzio_runtime::{
    should_run_structured_critique, RetryCause, RuntimeError, StoreExecutor, StoreMaintenanceKind,
    StoreMaintenanceState, TaskCompletion, TaskRuntime, WorkflowRuntime,
};
use akzio_store::{
    ClaimedAttempt, DaemonLease, LessonUsage, Store, StoreAlert, StoreError, StoreMetrics,
    StoredEvent, StoredLesson, TrajectoryEntry, WorkflowSnapshot,
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
    runtime_policy_identity(&DecisionPolicy::default())
}

pub fn runtime_policy_identity(decision_policy: &DecisionPolicy) -> Result<RuntimePolicyIdentity> {
    let evaluation_policy = EvaluationPolicy::default();
    let execution_policy = ExecutionPolicy::default();
    execution_policy
        .validate()
        .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
    let execution_gate_policy = ExecutionGatePolicy::default();
    execution_gate_policy
        .validate()
        .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
    Ok(RuntimePolicyIdentity {
        decision_policy_hash: decision_policy
            .policy_hash()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?,
        execution_policy_hash: content_hash_json(&serde_json::json!({
            "version": 1,
            "allocation_policy": execution_policy,
            "execution_gate_policy": execution_gate_policy,
            "paper_dispatch": {
                "settlement_timeout_secs": DEFAULT_PAPER_SETTLEMENT_TIMEOUT_SECS,
                "settlement_action_grace_secs": DEFAULT_PAPER_SETTLEMENT_ACTION_GRACE_SECS
            }
        }))?,
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
pub struct RuntimeGovernanceIdentity {
    pub component_hashes: BTreeMap<String, ContentHash>,
    pub bundle_hash: ContentHash,
}

pub fn default_runtime_governance_identity(
    cost_model: &OutcomeCostModel,
) -> Result<RuntimeGovernanceIdentity> {
    let policy = default_runtime_policy_identity()?;
    runtime_governance_identity(cost_model, &policy)
}

pub fn runtime_governance_identity(
    cost_model: &OutcomeCostModel,
    policy: &RuntimePolicyIdentity,
) -> Result<RuntimeGovernanceIdentity> {
    cost_model.validate()?;
    let contract_hash = contract_component_hash();
    let component_hashes = BTreeMap::from([
        (
            "instrument_evidence_registry".to_owned(),
            akzio_domain::instrument_evidence_registry_hash(),
        ),
        (
            "market_clock_observed_session_policy".to_owned(),
            content_hash_json(&serde_json::json!({
                "version": 1,
                "authority": "alpaca_paper_market_clock",
                "calendar_endpoint_observed": true,
                "session_slot_cardinality": "one_per_observed_broker_session_date",
                "daily_bar_availability": "new_york_provider_calendar_close_plus_20_minutes",
                "bar_policy_version": 3,
                "corporate_action_gate": "effective_date_within_selected_stage",
                "research_window": "latest_252_completed_adjusted_bars",
                "outcome_window": "first_five_common_completed_sessions_raw_prices"
            }))?,
        ),
        (
            "forecast_calibrator_risk_model".to_owned(),
            policy.decision_policy_hash.clone(),
        ),
        (
            "source_trust_injection_policy".to_owned(),
            content_hash_json(&serde_json::json!({
                "version": 1,
                "contract_component_hash": contract_hash,
                "external_context_trust": "untrusted_evidence",
                "prompt_injection_disposition": "quarantine",
                "canonical_claim_support": "claim_specific_primary_authority"
            }))?,
        ),
        (
            "cost_model".to_owned(),
            content_hash_json(&serde_json::json!({
                "version": 1,
                "transaction_cost_ppm": cost_model.transaction_cost_ppm,
                "slippage_ppm": cost_model.slippage_ppm,
                "metric_basis": "frozen_post_execution_exposure_v3",
                "signed_implementation_effect": "execution_midpoint_to_terminal_fill",
                "initial_valuation_bridge": "account_mark_to_execution_midpoint",
                "limit_shortfall": "diagnostic_only_not_deducted_twice",
                "realized_accounting": "unique_terminal_receipts_quantity_cash_v3",
                "evaluation_context_version": 1
            }))?,
        ),
        (
            "benchmark_definition".to_owned(),
            akzio_domain::outcome_benchmark_definition_bundle_hash()?,
        ),
        (
            "claim_verifier".to_owned(),
            content_hash_json(&serde_json::json!({
                "version": 1,
                "contract_component_hash": contract_hash,
                "critical_materiality_ppm": 500_000,
                "required_status": "supported",
                "requires_exactly_one_nonblocking_verification": true
            }))?,
        ),
        (
            "execution_policy_bundle".to_owned(),
            policy.execution_policy_hash.clone(),
        ),
        (
            "decision_validity_horizon_policy".to_owned(),
            policy.decision_policy_hash.clone(),
        ),
        (
            "lesson_mandate_policy".to_owned(),
            content_hash_json(&serde_json::json!({
                "version": 1,
                "lesson_governance": {
                    "requires_revalidation": true,
                    "usage_budgeted": true,
                    "quarantine_fail_closed": true
                },
                "mandate": ExecutionGatePolicy::default().mandate
            }))?,
        ),
        (
            "promotion_integrity_capability_retention".to_owned(),
            content_hash_json(&serde_json::json!({
                "version": 1,
                "hidden_evaluator_commitment": true,
                "sealed_trajectory_required": true,
                "non_compensable_safety_failures": true,
                "champion_challenger_rollback": true
            }))?,
        ),
        (
            "model_qualification".to_owned(),
            content_hash_json(&serde_json::json!({
                "version": 1,
                "covered_surfaces": [
                    "provider", "model_snapshot", "system_prompt", "role_prompt",
                    "tool_schema", "runtime_config", "context_strategy"
                ],
                "required_stages": ["replay", "adversarial", "execution_simulation", "shadow", "canary"],
                "paper_fail_closed": true
            }))?,
        ),
    ]);
    let bundle_hash = content_hash_json(&serde_json::json!(component_hashes))?;
    Ok(RuntimeGovernanceIdentity {
        component_hashes,
        bundle_hash,
    })
}

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub agent_budget: akzio_domain::AgentBudgetConfig,
    pub debug_control: Option<DebugCoreConfig>,
    pub outcome_processing: bool,
    pub store_root: PathBuf,
    pub http_token: String,
    pub worker_count: usize,
    pub auto_paper: bool,
    pub market_data_feed: Option<AlpacaMarketDataFeed>,
    pub outcome_cost_model: OutcomeCostModel,
    pub decision_policy: DecisionPolicy,
    pub runtime_identity_hash: Option<ContentHash>,
    pub historical_evaluation_condition: Option<ExperimentCondition>,
    pub model_knowledge_cutoff: Option<NaiveDate>,
}

#[derive(Clone)]
struct DaemonTransport {
    http_token: String,
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
    debug_control: Option<DebugCoreConfig>,
    outcome_processing: bool,
    store: Store,
    store_executor: StoreExecutor,
    workflow: WorkflowRuntime,
    task_runtime: TaskRuntime,
    agents: AgentRuntime,
    model: ModelClientAdapter,
    stage_models: Arc<BTreeMap<String, ModelClientAdapter>>,
    news_web_status: String,
    news_web_route: String,
    reasoning_events: broadcast::Sender<AgentReasoningEvent>,
    fixture_evidence: Arc<FixtureEvidence>,
    fixture_mode: bool,
    production_evidence: Arc<BTreeMap<EvidenceSource, Arc<dyn AsyncEvidenceAdapter>>>,
    decision_runtime: DecisionRuntime,
    execution_runtime: ExecutionRuntime,
    paper_commitment_runtime: PaperCommitmentRuntime,
    paper_dispatch_runtime: PaperDispatchRuntime,
    outcome_scheduling_runtime: OutcomeSchedulingRuntime,
    transport: DaemonTransport,
    paper: DaemonPaperState,
}

#[derive(Debug, Clone)]
pub struct DebugCoreConfig {
    pub code_revision: String,
    pub runtime_identity: ContentHash,
    pub decision_policy_status: String,
    pub decision_policy_input_hash: Option<ContentHash>,
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
    pub decision_policy_status: String,
    pub decision_policy_hash: ContentHash,
    pub decision_policy_input_hash: Option<ContentHash>,
    pub decision_capable: bool,
    #[serde(default)]
    pub news_web_status: String,
    #[serde(default)]
    pub news_web_route: String,
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

/// Shared CLI/HTTP input; its serialized bytes are retained as operator evidence.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LessonInput {
    pub lesson_id: Option<String>,
    pub title: String,
    pub statement: String,
    pub rationale: String,
    pub recommended_behavior: String,
    #[serde(default)]
    pub exclusions: Vec<String>,
    #[serde(default)]
    pub assets: Vec<String>,
    #[serde(default)]
    pub horizons: Vec<String>,
    #[serde(default)]
    pub regimes: Vec<String>,
    #[serde(default)]
    pub decision_stages: Vec<String>,
    #[serde(default)]
    pub supersedes: Vec<String>,
    #[serde(default)]
    pub conflicts_with: Vec<String>,
    #[serde(default = "default_lesson_confidence")]
    pub confidence_ppm: u32,
    pub authored_by: String,
}

fn default_lesson_confidence() -> u32 {
    500_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreEventView {
    pub event_type: String,
    pub artifact_id: Option<ArtifactId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayReport {
    pub run_id: RunId,
    #[serde(default)]
    pub lifecycle: Option<akzio_store::RunLifecycleHealth>,
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
    #[serde(default)]
    pub qualification: Option<ModelQualificationReport>,
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
    let source_uri = if source == EvidenceSource::Alpaca && resource.starts_with("bars:") {
        format!(
            "fixture://{}/{resource}?adjustment=all&feed=iex",
            source.as_str()
        )
    } else {
        format!("fixture://{}/{resource}", source.as_str())
    };
    let mut completed_bar_time = now - chrono::Duration::days(1);
    while matches!(
        chrono::Datelike::weekday(&completed_bar_time),
        chrono::Weekday::Sat | chrono::Weekday::Sun
    ) {
        completed_bar_time -= chrono::Duration::days(1);
    }
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
                "t": completed_bar_time.to_rfc3339(),
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
        value if value.starts_with("series:") => {
            let vintage = value
                .split(':')
                .nth(4)
                .and_then(|value| chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
                .unwrap_or_else(|| now.date_naive());
            serde_json::json!({
                "realtime_start": vintage.to_string(),
                "realtime_end": vintage.to_string(),
                "observations": [{ "date": vintage.to_string(), "value": "1.0" }]
            })
        }
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
            source_uri,
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
