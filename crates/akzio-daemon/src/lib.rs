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
    AccountSnapshot, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactOrigin,
    ArtifactProvenance, ArtifactRef, Asset, ContentHash, ContextPolicy, Decision, DecisionContext,
    DomainError, EvidenceNeed, ExecutionContext, ExecutionVerdict, FreezeState, LifecycleEventType,
    MarketClockSnapshot, MemoryId, MoneyMicros, OrderReceipt, OrderReceiptState,
    OutcomeExecutionLineage, OutcomeHorizon, OutcomeSchedule, PolicySubject, Quote, QuoteSnapshot,
    Reconciliation, ReconciliationState, ResearchClaim, Retrospective, RetrospectiveDraft, RunId,
    RunPurpose, RuntimeTaskClass, TargetPortfolio, TaskId, TaskStatus, TopologyId, WeightPpm,
    WorkflowProposal, WorkflowStatus,
};
use akzio_execution::{
    paper::{AlpacaPaper, CommittedPaperBroker, PortfolioHistoryRange},
    DecisionGateError, DecisionGateInput, ExecutionGateError, ExecutionGateInput, ExecutionPlan,
    OrderSide, PaperCommitmentError, PaperCommitmentInput, PaperDispatchError,
    PaperDispatchFailpoint, PaperDispatchInput, V2DecisionRuntime, V2ExecutionRuntime,
    V2PaperCommitmentRuntime, V2PaperDispatchRuntime,
};
pub use akzio_ingest::AlpacaMarketDataFeed;
use akzio_ingest::{
    AcquiredEvidence, AlpacaPaperEvidenceTransport, AsyncEvidenceAdapter, EvidenceProvenance,
    EvidenceQuality, EvidenceRequest, EvidenceRuntime, EvidenceRuntimeError, EvidenceSource,
    FixtureEvidenceAdapter, FredDirectTransport, ModelNativeWebEvidenceTransport,
    NormalizedEvidencePayload, SecEdgarDirectTransport,
};
use akzio_learning::{
    EvaluationError, EvaluationInput, EvaluationPolicy, EvaluationRuntime,
    GovernedHorizonObservation, OutcomeCostModel, OutcomeMaterializationInput,
    OutcomeScheduleError, OutcomeScheduleInput, OutcomeSchedulingRuntime,
};
use akzio_model::{ModelClient, ModelConfig, ModelError};
use akzio_research::v2::{
    ActiveResearchCatalogue, AgentReasoningEvent, AgentRuntime, ModelClientAdapter, ResearchError,
};
use akzio_runtime::{
    should_run_structured_critique, RetryCause, RuntimeError, TaskCompletion, TaskRuntime,
    WorkflowRuntime,
};
use akzio_store::v2::{
    ClaimedAttempt, DaemonLease, StoreAlert, StoreError, StoreMetrics, StoredEvent,
    TrajectoryEntry, V2Store, WorkflowSnapshot,
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
const OUTCOME_WORKER_RECIPE_ID: &str = "learning.outcome_worker";
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

pub type Result<T> = std::result::Result<T, DaemonError>;

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
pub struct Daemon {
    store: V2Store,
    workflow: WorkflowRuntime,
    task_runtime: TaskRuntime,
    agents: AgentRuntime,
    model: ModelClientAdapter,
    stage_models: Arc<BTreeMap<String, ModelClientAdapter>>,
    reasoning_events: broadcast::Sender<AgentReasoningEvent>,
    fixture_evidence: Arc<FixtureEvidence>,
    production_evidence: Arc<BTreeMap<EvidenceSource, Arc<dyn AsyncEvidenceAdapter>>>,
    decision_runtime: V2DecisionRuntime,
    execution_runtime: V2ExecutionRuntime,
    paper_commitment_runtime: V2PaperCommitmentRuntime,
    paper_dispatch_runtime: V2PaperDispatchRuntime,
    outcome_scheduling_runtime: OutcomeSchedulingRuntime,
    paper_broker: Option<Arc<dyn CommittedPaperBroker>>,
    paper_observer: Option<AlpacaPaper>,
    scheduler: PaperScheduler,
    http_token: String,
    observer_token: Option<String>,
    auto_paper: bool,
    runtime_identity_hash: Option<ContentHash>,
    outcome_cost_model: OutcomeCostModel,
    worker_pool: WorkerPoolConfig,
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

fn parse_daily_bars(
    value: &Value,
    observed_at: DateTime<Utc>,
) -> Result<BTreeMap<NaiveDate, MoneyMicros>> {
    let Some(items) = value.get("bars").and_then(Value::as_array) else {
        let close = value
            .get("close")
            .and_then(parse_money_micros)
            .ok_or_else(|| DaemonError::Unavailable("daily bars close is missing".to_owned()))?;
        return Ok(BTreeMap::from([(observed_at.date_naive(), close)]));
    };
    let mut bars = BTreeMap::new();
    for item in items {
        let close = item
            .get("c")
            .or_else(|| item.get("close"))
            .and_then(parse_money_micros)
            .ok_or_else(|| DaemonError::Unavailable("daily bar close is invalid".to_owned()))?;
        let date = item
            .get("t")
            .or_else(|| item.get("timestamp"))
            .and_then(Value::as_str)
            .and_then(|timestamp| {
                DateTime::parse_from_rfc3339(timestamp)
                    .map(|value| value.date_naive())
                    .or_else(|_| NaiveDate::parse_from_str(timestamp, "%Y-%m-%d").map_err(|_| ()))
                    .ok()
            })
            .unwrap_or_else(|| observed_at.date_naive());
        if bars.insert(date, close).is_some() {
            return Err(DaemonError::Unavailable(
                "daily bar date is duplicated".to_owned(),
            ));
        }
    }
    Ok(bars)
}

fn parse_money_micros(value: &Value) -> Option<MoneyMicros> {
    let raw = value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_number().map(ToString::to_string))?;
    let raw = raw.trim();
    let (negative, unsigned) = if let Some(value) = raw.strip_prefix('-') {
        (true, value)
    } else if let Some(value) = raw.strip_prefix('+') {
        (false, value)
    } else {
        (false, raw)
    };
    if unsigned.is_empty() || unsigned.contains(['e', 'E']) {
        return None;
    }
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if (!whole.is_empty() && !whole.chars().all(|character| character.is_ascii_digit()))
        || !fraction.chars().all(|character| character.is_ascii_digit())
    {
        return None;
    }
    let whole = if whole.is_empty() {
        0
    } else {
        whole.parse::<i64>().ok()?
    };
    let mut fraction = fraction.chars().take(6).collect::<String>();
    while fraction.len() < 6 {
        fraction.push('0');
    }
    let fraction = fraction.parse::<i64>().ok()?;
    let magnitude = whole.checked_mul(1_000_000)?.checked_add(fraction)?;
    Some(MoneyMicros(if negative {
        magnitude.checked_neg()?
    } else {
        magnitude
    }))
}

fn common_bar_dates(
    bars_by_asset: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    baseline: NaiveDate,
) -> Vec<NaiveDate> {
    let Some((_, first)) = bars_by_asset.iter().next() else {
        return Vec::new();
    };
    first
        .keys()
        .copied()
        .filter(|date| *date > baseline)
        .filter(|date| bars_by_asset.values().all(|bars| bars.get(date).is_some()))
        .collect()
}

fn debug_fixture_evidence(resource: &str, now: DateTime<Utc>) -> AcquiredEvidence {
    let source_uri = format!("fixture://alpaca/{resource}");
    AcquiredEvidence {
        raw: serde_json::to_vec(&serde_json::json!({
            "resource": resource,
            "bars": [{"asset": "TQQQ", "close": 100}],
        }))
        .expect("static debug fixture JSON must serialize"),
        media_type: "application/json".to_owned(),
        source_uri: source_uri.clone(),
        observed_at: now,
        normalized: serde_json::json!({
            "resource": resource,
            "bars": [{"asset": "TQQQ", "close": 100}],
        }),
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

fn paper_account_snapshot_artifact(
    store: &V2Store,
    task: &ClaimedAttempt,
    components: &BTreeMap<String, (Artifact, Artifact)>,
    now: DateTime<Utc>,
) -> Result<Artifact> {
    let broker_session = store
        .session_slot_for_run(&task.run_id)?
        .map(|slot| slot.session_key)
        .ok_or_else(|| {
            DaemonError::InvalidInput("Paper run has no scheduler session slot".to_owned())
        })?;
    let fills_resource = format!("paper.fills:{broker_session}");
    let expected = [
        PAPER_ACCOUNT_RESOURCE,
        PAPER_POSITIONS_RESOURCE,
        PAPER_OPEN_ORDERS_RESOURCE,
        fills_resource.as_str(),
    ];
    if components.len() != expected.len() {
        return Err(DaemonError::InvalidInput(
            "Paper account snapshot is missing broker truth components".to_owned(),
        ));
    }

    let mut envelopes = BTreeMap::new();
    let mut normalized_sources = Vec::new();
    for resource in expected {
        let (need_artifact, normalized) = components.get(resource).ok_or_else(|| {
            DaemonError::InvalidInput(format!("Paper account snapshot missing {resource}"))
        })?;
        let need: EvidenceNeed = serde_json::from_slice(&store.read_blob(&need_artifact.blob)?)?;
        if need.source_family != "alpaca"
            || need.resource != resource
            || need_artifact.producer != "scheduler.paper_snapshot"
            || need_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&task.run_id)
        {
            return Err(DaemonError::InvalidInput(
                "Paper account snapshot requires scheduler-owned Alpaca evidence".to_owned(),
            ));
        }
        let envelope: NormalizedEvidencePayload =
            serde_json::from_slice(&store.read_blob(&normalized.blob)?)?;
        envelopes.insert(resource.to_owned(), envelope);
        normalized_sources.push(normalized);
    }

    let observed_at = envelopes
        .values()
        .map(|envelope| envelope.observed_at)
        .max()
        .ok_or_else(|| DaemonError::InvalidInput("Paper account snapshot is empty".to_owned()))?;
    let account_value = &envelopes[PAPER_ACCOUNT_RESOURCE].value;
    let mut account = decode_paper_account(account_value, broker_session, observed_at)?;
    if account_value.get("schema_version").is_none() {
        account.positions.clear();
        account.external_positions.clear();
        for position in envelopes[PAPER_POSITIONS_RESOURCE]
            .value
            .as_array()
            .ok_or_else(|| {
                DaemonError::InvalidInput("Paper positions must be an array".to_owned())
            })?
        {
            let symbol = position
                .get("symbol")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    DaemonError::InvalidInput("Paper position symbol missing".to_owned())
                })?;
            let quantity_micros = position
                .get("qty")
                .and_then(parse_money_micros)
                .map(|quantity| quantity.0)
                .ok_or_else(|| {
                    DaemonError::InvalidInput("Paper position qty invalid".to_owned())
                })?;
            let market_value = provider_money(position, "market_value")?;
            match Asset::try_from(symbol) {
                Ok(asset) => {
                    account.positions.insert(
                        asset,
                        akzio_domain::Position {
                            quantity_micros,
                            market_value,
                        },
                    );
                }
                Err(_) => {
                    account.external_positions.insert(symbol.to_owned());
                }
            }
        }

        account.open_order_ids = envelopes[PAPER_OPEN_ORDERS_RESOURCE]
            .value
            .as_array()
            .ok_or_else(|| {
                DaemonError::InvalidInput("Paper open orders must be an array".to_owned())
            })?
            .iter()
            .map(|order| {
                order
                    .get("client_order_id")
                    .or_else(|| order.get("id"))
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| {
                        DaemonError::InvalidInput("Paper open order ID missing".to_owned())
                    })
            })
            .collect::<Result<BTreeSet<_>>>()?;

        let fills = envelopes[&fills_resource]
            .value
            .as_array()
            .ok_or_else(|| DaemonError::InvalidInput("Paper fills must be an array".to_owned()))?;
        if fills.len() >= 100 {
            return Err(DaemonError::InvalidInput(
                "Paper fills require pagination before execution".to_owned(),
            ));
        }
        let turnover = fills.iter().try_fold(0_i128, |sum, fill| {
            let quantity = fill
                .get("qty")
                .and_then(parse_money_micros)
                .map(|value| i128::from(value.0).abs())
                .ok_or_else(|| DaemonError::InvalidInput("Paper fill qty invalid".to_owned()))?;
            let price = i128::from(provider_money(fill, "price")?.0).abs();
            Ok::<_, DaemonError>(
                sum.saturating_add(quantity.saturating_mul(price).saturating_div(1_000_000)),
            )
        })?;
        account.day_turnover =
            MoneyMicros(i64::try_from(turnover).map_err(|_| {
                DaemonError::InvalidInput("Paper day turnover exceeds i64".to_owned())
            })?);
    }
    account
        .validate()
        .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;

    seal_execution_snapshot_sources(
        store,
        task,
        &normalized_sources,
        "execution.snapshot.account",
        &account,
        observed_at,
        None,
        now,
    )
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
    let broker_session = store
        .session_slot_for_run(&task.run_id)?
        .map(|slot| slot.session_key)
        .ok_or_else(|| {
            DaemonError::InvalidInput("Paper run has no scheduler session slot".to_owned())
        })?;
    match resource {
        PAPER_ACCOUNT_RESOURCE => {
            let payload = decode_paper_account(
                &envelope.value,
                broker_session.clone(),
                envelope.observed_at,
            )?;
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
            let payload = decode_paper_quotes(
                &envelope.value,
                broker_session.clone(),
                envelope.observed_at,
            )?;
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
            let payload =
                decode_paper_clock(&envelope.value, broker_session, envelope.observed_at)?;
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

fn decode_paper_account(
    value: &Value,
    broker_session: String,
    observed_at: DateTime<Utc>,
) -> Result<AccountSnapshot> {
    if value.get("schema_version").is_some() {
        return Ok(serde_json::from_value(value.clone())?);
    }
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| DaemonError::InvalidInput("Paper account status missing".to_owned()))?;
    Ok(AccountSnapshot {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        broker_session,
        observed_at,
        equity: provider_money(value, "equity")?,
        buying_power: provider_money(value, "buying_power")?,
        day_turnover: MoneyMicros::ZERO,
        active: status.eq_ignore_ascii_case("ACTIVE"),
        trading_blocked: value
            .get("trading_blocked")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                DaemonError::InvalidInput("Paper account trading_blocked missing".to_owned())
            })?,
        positions: BTreeMap::new(),
        external_positions: BTreeSet::new(),
        open_order_ids: BTreeSet::new(),
    })
}

fn decode_paper_quotes(
    value: &Value,
    broker_session: String,
    observed_at: DateTime<Utc>,
) -> Result<QuoteSnapshot> {
    if value.get("schema_version").is_some() {
        return Ok(serde_json::from_value(value.clone())?);
    }
    let quotes = value
        .get("quotes")
        .and_then(Value::as_object)
        .ok_or_else(|| DaemonError::InvalidInput("Paper quotes payload missing quotes".to_owned()))?
        .iter()
        .map(|(symbol, quote)| {
            let asset = Asset::try_from(symbol.as_str()).map_err(|_| {
                DaemonError::InvalidInput(format!(
                    "Paper quote asset outside v2 universe: {symbol}"
                ))
            })?;
            let quote_observed_at = quote
                .get("t")
                .map(|timestamp| provider_timestamp(timestamp, "quote.t"))
                .transpose()?
                .unwrap_or(observed_at);
            Ok((
                asset,
                Quote {
                    bid: provider_money(quote, "bp")?,
                    ask: provider_money(quote, "ap")?,
                    observed_at: quote_observed_at,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    if quotes.is_empty() {
        return Err(DaemonError::InvalidInput(
            "Paper quotes payload contains no executable assets".to_owned(),
        ));
    }
    Ok(QuoteSnapshot {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        broker_session,
        observed_at,
        quotes,
    })
}

fn decode_paper_clock(
    value: &Value,
    broker_session: String,
    observed_at: DateTime<Utc>,
) -> Result<MarketClockSnapshot> {
    if value.get("schema_version").is_some() {
        return Ok(serde_json::from_value(value.clone())?);
    }
    Ok(MarketClockSnapshot {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        broker_session,
        is_open: value
            .get("is_open")
            .and_then(Value::as_bool)
            .ok_or_else(|| DaemonError::InvalidInput("Paper clock is_open missing".to_owned()))?,
        observed_at: value
            .get("timestamp")
            .map(|timestamp| provider_timestamp(timestamp, "clock.timestamp"))
            .transpose()?
            .unwrap_or(observed_at),
    })
}

fn provider_money(value: &Value, field: &str) -> Result<MoneyMicros> {
    value
        .get(field)
        .and_then(parse_money_micros)
        .ok_or_else(|| DaemonError::InvalidInput(format!("Paper provider field {field} invalid")))
}

fn provider_timestamp(value: &Value, field: &str) -> Result<DateTime<Utc>> {
    let raw = value.as_str().ok_or_else(|| {
        DaemonError::InvalidInput(format!("Paper provider field {field} invalid"))
    })?;
    DateTime::parse_from_rfc3339(raw)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|error| {
            DaemonError::InvalidInput(format!("Paper provider field {field}: {error}"))
        })
}

fn seal_execution_snapshot<T: serde::Serialize>(
    store: &V2Store,
    task: &ClaimedAttempt,
    normalized: &Artifact,
    producer: &str,
    payload: &T,
    now: DateTime<Utc>,
) -> Result<Artifact> {
    seal_execution_snapshot_sources(
        store,
        task,
        &[normalized],
        producer,
        payload,
        normalized.provenance.observed_at.unwrap_or(now),
        normalized.provenance.source_uri.clone(),
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn seal_execution_snapshot_sources<T: serde::Serialize>(
    store: &V2Store,
    task: &ClaimedAttempt,
    normalized_sources: &[&Artifact],
    producer: &str,
    payload: &T,
    observed_at: DateTime<Utc>,
    source_uri: Option<String>,
    now: DateTime<Utc>,
) -> Result<Artifact> {
    let primary = normalized_sources.first().ok_or_else(|| {
        DaemonError::InvalidInput("execution snapshot has no normalized sources".to_owned())
    })?;
    let mut source_refs = Vec::new();
    for normalized in normalized_sources {
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
        source_refs.extend([
            raw,
            ArtifactRef {
                artifact_id: normalized.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            },
        ]);
    }
    source_refs.sort();
    source_refs.dedup();
    Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store.put_json(payload)?,
        producer,
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: primary.provenance.source_family.clone(),
            observed_at: Some(observed_at),
            retrieved_at: now,
            source_uri,
            confidence_ppm: primary.provenance.confidence_ppm,
            producer_contract_hash: task.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(task.run_id.clone()),
            task_id: Some(task.node.task_id.clone()),
            attempt_id: Some(task.permit.attempt_id.clone()),
            contract_hash: task.permit.contract_hash.clone(),
        }),
        source_refs,
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

fn fixture_claim_output() -> serde_json::Value {
    serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "topic": "fixture_market_regime",
        "statement": "The governed fixture evidence supports a neutral fixture claim.",
        "horizon": "t5",
        "stance": "neutral",
        "materiality_ppm": 500_000,
        "confidence_ppm": 500_000,
        "grounds": [{
            "evidence": {
                "artifact_id": akzio_model::FIXTURE_CONTEXT_EVIDENCE_ID,
                "kind": "normalized_evidence"
            },
            "support": "The selected governed fixture evidence is the stated support."
        }],
        "evidence_gaps": []
    })
}

fn fixture_critique_output() -> serde_json::Value {
    serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        "target": {
            "artifact_id": akzio_model::FIXTURE_CONTEXT_CLAIM_ID,
            "kind": "claim"
        },
        "topic": "fixture_market_regime",
        "severity": "low",
        "blocker": false,
        "rationale": "The fixture records an explicit evidence gap rather than inventing a rebuttal.",
        "grounds": [],
        "evidence_gaps": [{
            "topic": "fixture_depth",
            "rationale": "No additional governed detail was selected for the fixture critique."
        }]
    })
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
    let responses = |output: serde_json::Value| {
        let output = serde_json::json!({
            "result": output,
            "deliberation": {
                "selected_path": "fixture path",
                "alternatives": [],
                "alternative_match_ppm": [],
                "uncertainties": [],
                "uncertainty_weight_ppm": [],
                "basis_artifact_ids": [],
                "confidence_ppm": 1000000
            }
        });
        vec![
            serde_json::json!({
                "output": [{
                    "type": "message",
                    "content": [{"type": "output_text", "text": "fixture research memo"}]
                }]
            }),
            serde_json::json!({
                "output": [{
                    "type": "function_call",
                    "call_id": "fixture-submit",
                    "name": "submit_result",
                    "arguments": serde_json::to_string(&output).expect("static fixture JSON")
                }]
            }),
        ]
    };
    ModelClient::fixture_by_purpose(BTreeMap::from([
        ("research.planner".to_owned(), responses(planner)),
        (
            "research.analyst".to_owned(),
            responses(fixture_claim_output()),
        ),
        (
            "research.critic".to_owned(),
            responses(fixture_critique_output()),
        ),
        (
            "research.synthesizer".to_owned(),
            responses(serde_json::json!({
                "summary": "fixture decision draft",
                "confidence_ppm": 500000,
                "forecasts": forecasts,
                "claims": [{
                    "artifact_id": akzio_model::FIXTURE_CONTEXT_CLAIM_ID,
                    "kind": "claim"
                }],
                "critiques": [],
                "evidence": [{
                    "artifact_id": akzio_model::FIXTURE_CONTEXT_EVIDENCE_ID,
                    "kind": "normalized_evidence"
                }],
                "material_conflicts": [],
                "hard_blockers": [],
                "soft_warnings": []
            })),
        ),
    ]))
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
