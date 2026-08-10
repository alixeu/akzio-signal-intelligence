//! Durable local control plane for Akzio v2.
//!
//! Transport only submits work and streams the durable event log. The task
//! dispatcher owns business sequencing; `TaskRuntime` owns leases, retries,
//! heartbeats, and terminal task state.

mod dispatch;
mod worker;

pub use worker::{TaskHandler, WorkerPool, WorkerPoolConfig};

use std::{
    collections::BTreeMap, convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc,
    time::Duration as StdDuration,
};

use akzio_context::legacy::ContextError;
use akzio_domain::{DocumentKind, RunId, RunPurpose, WorkflowPlan};
use akzio_execution::{
    paper::{AlpacaPaper, PaperError},
    ExecutionRuntimeError,
};
use akzio_ingest::legacy::IngestError;
use akzio_learning::{LedgerError, TopologyLedger, TopologyState};
use akzio_model::{ModelClient, ModelError};
use akzio_research::legacy::{baseline_topology, bootstrap_workflow, ContractRegistry};
use akzio_runtime::legacy::{CompiledWorkflow, RuntimeError, TaskRuntime, WorkflowRuntime};
use akzio_store::legacy::{DaemonLease, StoreError, StoredEvent, V2Store};
use async_stream::stream;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
    Json, Router,
};
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, UnixListener},
    sync::watch,
};

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Context(#[from] ContextError),
    #[error(transparent)]
    Research(#[from] akzio_research::legacy::ResearchError),
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Execution(#[from] ExecutionRuntimeError),
    #[error(transparent)]
    Learning(#[from] LedgerError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("run {run_id} has no {kind:?} document")]
    MissingRunDocument { run_id: RunId, kind: DocumentKind },
    #[error("invalid sealed execution input: {0}")]
    InvalidInput(String),
}

impl DaemonError {
    fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Model(ModelError::Transport(_))
                | Self::Ingest(IngestError::Transport { .. })
                | Self::Execution(ExecutionRuntimeError::Paper(PaperError::Transport { .. }))
        )
    }
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
    contracts: ContractRegistry,
    model: ModelClient,
    http_token: String,
    instance_id: String,
    worker_pool: WorkerPoolConfig,
    auto_paper: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonHealth {
    pub status: String,
    pub scheduler_owner: Option<String>,
    pub scheduler_epoch: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum DaemonCommand {
    Health,
    Events { run_id: RunId, after: i64 },
    Submit { purpose: RunPurpose },
    Cancel { run_id: RunId },
    Retry { run_id: RunId },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonReply {
    Health {
        status: String,
        scheduler_owner: Option<String>,
        scheduler_epoch: Option<u64>,
    },
    Events {
        events: Vec<EventView>,
    },
    Submitted {
        run_id: RunId,
    },
    Cancelled {
        run_id: RunId,
        cancelled_tasks: u64,
    },
    Retried {
        source_run_id: RunId,
        run_id: RunId,
    },
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

const SCHEDULER_LEASE_NAME: &str = "akzio.local.scheduler";
const SCHEDULER_LEASE_SECONDS: i64 = 15;
const SCHEDULER_HEARTBEAT: StdDuration = StdDuration::from_secs(5);
const SCHEDULER_RETRY: StdDuration = StdDuration::from_millis(500);
const PAPER_SCHEDULE_POLL: StdDuration = StdDuration::from_secs(60);

impl Daemon {
    /// Construct the production daemon. Model credentials are read only here,
    /// never persisted in the Store.
    pub fn open(config: DaemonConfig) -> Result<Self> {
        Self::with_model(config, ModelClient::from_env()?)
    }

    /// Explicit injection keeps local fixture/debug tests on exactly the same
    /// dispatcher without giving a production daemon a fixture fallback.
    pub fn with_model(config: DaemonConfig, model: ModelClient) -> Result<Self> {
        let store = V2Store::open(config.store_root)?;
        let contracts = ContractRegistry::install(
            &akzio_context::legacy::ContextBroker::new(store.clone()),
            Utc::now(),
        )?;
        Ok(Self {
            workflow: WorkflowRuntime::new(store.clone()),
            task_runtime: TaskRuntime::new(store.clone()),
            contracts,
            store,
            model,
            http_token: config.http_token,
            auto_paper: config.auto_paper,
            instance_id: format!(
                "akzio-{}-{}",
                std::process::id(),
                Utc::now().timestamp_micros()
            ),
            worker_pool: WorkerPoolConfig {
                worker_count: config.worker_count.max(1),
                ..WorkerPoolConfig::default()
            },
        })
    }

    pub fn submit(
        &self,
        run_id: &RunId,
        purpose: RunPurpose,
        plan: WorkflowPlan,
    ) -> Result<CompiledWorkflow> {
        self.workflow
            .submit(run_id, purpose, plan, Utc::now())
            .map_err(Into::into)
    }

    fn default_plan(&self, run_id: &RunId, purpose: RunPurpose) -> Result<WorkflowPlan> {
        let baseline = baseline_topology();
        let broker = akzio_context::legacy::ContextBroker::new(self.store.clone());
        let topology = if purpose == RunPurpose::Paper {
            TopologyLedger::new(broker.clone()).topology_for_run(run_id, baseline.clone())?
        } else {
            baseline.clone()
        };
        Ok(bootstrap_workflow(
            purpose,
            topology,
            &self.contracts.installed(),
        ))
    }

    fn ensure_topology_for_plan(
        &self,
        run_id: &RunId,
        purpose: RunPurpose,
        plan: &WorkflowPlan,
        now: chrono::DateTime<Utc>,
    ) -> Result<()> {
        if purpose == RunPurpose::Paper {
            let state = if plan.topology_id == baseline_topology() {
                TopologyState::Active
            } else {
                TopologyState::Candidate
            };
            TopologyLedger::new(akzio_context::legacy::ContextBroker::new(
                self.store.clone(),
            ))
            .ensure_topology(run_id, plan.topology_id.clone(), state, now)?;
        }
        Ok(())
    }

    pub fn submit_default(&self, purpose: RunPurpose) -> Result<RunId> {
        if purpose == RunPurpose::Paper {
            return Err(DaemonError::InvalidInput(
                "Paper runs are scheduler-owned; start the daemon and wait for an open market session"
                    .to_owned(),
            ));
        }
        let run_id = RunId::new();
        let now = Utc::now();
        let plan = self.default_plan(&run_id, purpose)?;
        self.workflow.submit(&run_id, purpose, plan.clone(), now)?;
        self.ensure_topology_for_plan(&run_id, purpose, &plan, now)?;
        Ok(run_id)
    }

    fn schedule_paper_session(
        &self,
        lease: &DaemonLease,
        session_date: NaiveDate,
        now: chrono::DateTime<Utc>,
    ) -> Result<Option<RunId>> {
        if !self.auto_paper {
            return Ok(None);
        }
        let session_key = format!("paper:{}", session_date.format("%F"));
        let reservation = if let Some(slot) = self.store.paper_schedule_slot(&session_key)? {
            self.store.reserve_paper_schedule_slot(
                lease,
                &session_key,
                &slot.run_id,
                &slot.plan,
                now,
            )?
        } else {
            let proposed_run_id = RunId::new();
            let plan = self.default_plan(&proposed_run_id, RunPurpose::Paper)?;
            self.store.reserve_paper_schedule_slot(
                lease,
                &session_key,
                &proposed_run_id,
                &plan,
                now,
            )?
        };
        let slot = reservation.slot;
        if slot.submitted_at.is_some() {
            return Ok(Some(slot.run_id));
        }
        self.workflow
            .submit_or_recover(&slot.run_id, RunPurpose::Paper, slot.plan.clone(), now)?;
        self.ensure_topology_for_plan(&slot.run_id, RunPurpose::Paper, &slot.plan, now)?;
        if self
            .store
            .mark_paper_schedule_submitted(lease, &session_key, &slot.run_id, now)?
        {
            let plan_document_id = self.store.workflow_plan_document(&slot.run_id)?;
            let plan_document = self.store.read_document(&plan_document_id)?;
            self.store.append_event(&akzio_domain::EventEnvelope {
                schema_version: akzio_domain::V2_SCHEMA_VERSION,
                run_id: slot.run_id.clone(),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
                causation_id: Some(session_key),
                event_type: "scheduler.paper_submitted".to_owned(),
                payload_document_id: Some(plan_document_id),
                payload: Some(plan_document.blob),
                created_at: now,
            })?;
        }
        Ok(Some(slot.run_id))
    }

    async fn maybe_schedule_paper(
        &self,
        lease: &DaemonLease,
        now: chrono::DateTime<Utc>,
    ) -> Result<()> {
        if !self.auto_paper {
            return Ok(());
        }
        let paper = AlpacaPaper::from_env().map_err(ExecutionRuntimeError::Paper)?;
        let clock = paper
            .market_clock()
            .await
            .map_err(ExecutionRuntimeError::Paper)?;
        if clock.is_open {
            self.schedule_paper_session(lease, clock.session_date, now)?;
        }
        Ok(())
    }

    async fn poll_paper_schedule(
        &self,
        lease: &DaemonLease,
        now: chrono::DateTime<Utc>,
    ) -> Result<()> {
        match self.maybe_schedule_paper(lease, now).await {
            Ok(()) => Ok(()),
            Err(error) if error.is_retryable() => {
                tracing::warn!(error = %error, "paper schedule poll will retry");
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub async fn run_one(&self, worker_id: &str) -> Result<bool> {
        let daemon = self.clone();
        self.task_runtime
            .run_one_async(worker_id, move |task| {
                let daemon = daemon.clone();
                async move { daemon.execute_task(task).await }
            })
            .await
            .map_err(Into::into)
    }

    pub async fn serve_workers(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let now = Utc::now();
            let Some(lease) = self.store.acquire_daemon_lease(
                SCHEDULER_LEASE_NAME,
                &self.instance_id,
                now,
                now + chrono::Duration::seconds(SCHEDULER_LEASE_SECONDS),
            )?
            else {
                tokio::select! {
                    _ = tokio::time::sleep(SCHEDULER_RETRY) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                }
                continue;
            };
            self.store.recover_expired_tasks(now)?;
            self.poll_paper_schedule(&lease, now).await?;
            let (leader_shutdown_tx, leader_shutdown_rx) = watch::channel(false);
            let daemon = self.clone();
            let handler: TaskHandler = Arc::new(move |task| {
                let daemon = daemon.clone();
                Box::pin(async move { daemon.execute_task(task).await })
            });
            let pool = WorkerPool::new(self.task_runtime.clone(), self.worker_pool.clone());
            let mut workers =
                tokio::spawn(async move { pool.serve(handler, leader_shutdown_rx).await });
            let mut heartbeat = tokio::time::interval(SCHEDULER_HEARTBEAT);
            heartbeat.tick().await;
            let mut paper_schedule = tokio::time::interval(PAPER_SCHEDULE_POLL);
            paper_schedule.tick().await;
            let mut worker_finished = false;
            let mut lost_leadership = false;
            let mut worker_error = None;
            loop {
                tokio::select! {
                    result = &mut workers => {
                        worker_finished = true;
                        worker_error = Some(result);
                        break;
                    }
                    _ = heartbeat.tick() => {
                        let now = Utc::now();
                    if !self.store.heartbeat_daemon_lease(
                        &lease,
                        now,
                        now + chrono::Duration::seconds(SCHEDULER_LEASE_SECONDS),
                    )? {
                        lost_leadership = true;
                        break;
                    }
                    }
                    _ = paper_schedule.tick() => {
                        self.poll_paper_schedule(&lease, Utc::now()).await?;
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
            let _ = leader_shutdown_tx.send(true);
            if !worker_finished {
                let result = workers.await.map_err(|error| {
                    DaemonError::Runtime(RuntimeError::Handler(format!(
                        "leader worker pool join failed: {error}"
                    )))
                })?;
                result?;
            } else if let Some(result) = worker_error {
                result
                    .map_err(|error| {
                        DaemonError::Runtime(RuntimeError::Handler(format!(
                            "leader worker pool join failed: {error}"
                        )))
                    })?
                    .map_err(DaemonError::from)?;
            }
            let _ = self.store.release_daemon_lease(&lease)?;
            if *shutdown.borrow() {
                return Ok(());
            }
            if !lost_leadership {
                return Err(DaemonError::Runtime(RuntimeError::Handler(
                    "leader worker pool exited unexpectedly".to_owned(),
                )));
            }
        }
    }

    pub fn health(&self) -> Result<DaemonHealth> {
        let lease = self
            .store
            .daemon_lease(SCHEDULER_LEASE_NAME)?
            .filter(|lease| lease.expires_at > Utc::now());
        Ok(DaemonHealth {
            status: "ok".to_owned(),
            scheduler_owner: lease.as_ref().map(|lease| lease.owner_id.clone()),
            scheduler_epoch: lease.map(|lease| lease.epoch),
        })
    }

    pub fn handle(&self, command: DaemonCommand) -> Result<DaemonReply> {
        match command {
            DaemonCommand::Health => {
                let health = self.health()?;
                Ok(DaemonReply::Health {
                    status: health.status,
                    scheduler_owner: health.scheduler_owner,
                    scheduler_epoch: health.scheduler_epoch,
                })
            }
            DaemonCommand::Events { run_id, after } => Ok(DaemonReply::Events {
                events: self
                    .store
                    .events_after(&run_id, after, 256)?
                    .into_iter()
                    .map(EventView::from)
                    .collect(),
            }),
            DaemonCommand::Submit { purpose } => Ok(DaemonReply::Submitted {
                run_id: self.submit_default(purpose)?,
            }),
            DaemonCommand::Cancel { run_id } => {
                let cancelled_tasks = self.store.cancel_run(&run_id, Utc::now())?;
                let payload = self.store.put_bytes(
                    serde_json::to_string(&serde_json::json!({
                        "cancelled_tasks": cancelled_tasks,
                    }))?
                    .as_bytes(),
                    "application/json",
                )?;
                self.store.append_event(&akzio_domain::EventEnvelope {
                    schema_version: akzio_domain::V2_SCHEMA_VERSION,
                    run_id: run_id.clone(),
                    task_id: None,
                    attempt_id: None,
                    contract_hash: None,
                    causation_id: None,
                    event_type: "workflow.cancel_requested".to_owned(),
                    payload_document_id: None,
                    payload: Some(payload),
                    created_at: Utc::now(),
                })?;
                Ok(DaemonReply::Cancelled {
                    run_id,
                    cancelled_tasks,
                })
            }
            DaemonCommand::Retry { run_id } => {
                let purpose = self.store.run_purpose(&run_id)?;
                if purpose == RunPurpose::Paper {
                    return Err(DaemonError::InvalidInput(
                        "Paper retries are scheduler-owned; retrying a completed session would create a second execution path"
                            .to_owned(),
                    ));
                }
                let retry_run_id = self.submit_default(purpose)?;
                let payload = self.store.put_bytes(
                    serde_json::to_string(&serde_json::json!({"retry_of": run_id}))?.as_bytes(),
                    "application/json",
                )?;
                self.store.append_event(&akzio_domain::EventEnvelope {
                    schema_version: akzio_domain::V2_SCHEMA_VERSION,
                    run_id: retry_run_id.clone(),
                    task_id: None,
                    attempt_id: None,
                    contract_hash: None,
                    causation_id: Some(run_id.0.clone()),
                    event_type: "workflow.retried".to_owned(),
                    payload_document_id: None,
                    payload: Some(payload),
                    created_at: Utc::now(),
                })?;
                Ok(DaemonReply::Retried {
                    source_run_id: run_id,
                    run_id: retry_run_id,
                })
            }
        }
    }

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/health", get(http_health))
            .route("/runs/{run_id}/events", get(http_events))
            .route("/runs", post(http_submit))
            .route("/runs/{run_id}/cancel", post(http_cancel))
            .route("/runs/{run_id}/retry", post(http_retry))
            .with_state(self)
    }

    pub async fn serve_http(
        self: Arc<Self>,
        address: SocketAddr,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let listener = TcpListener::bind(address).await?;
        axum::serve(listener, self.router())
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await?;
        Ok(())
    }

    pub async fn serve_unix(
        self: Arc<Self>,
        path: PathBuf,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        let listener = UnixListener::bind(path)?;
        loop {
            let (stream, _) = tokio::select! {
                accepted = listener.accept() => accepted?,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            };
            let daemon = self.clone();
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut lines = BufReader::new(read).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let reply = serde_json::from_str::<DaemonCommand>(&line)
                        .map_err(DaemonError::from)
                        .and_then(|command| daemon.handle(command));
                    let payload = match reply {
                        Ok(reply) => serde_json::to_string(&reply),
                        Err(error) => serde_json::to_string(&serde_json::json!({
                            "error": error.to_string()
                        })),
                    };
                    let Ok(payload) = payload else { break };
                    if write
                        .write_all(format!("{payload}\n").as_bytes())
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
    }

    pub fn store(&self) -> &V2Store {
        &self.store
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

impl From<StoredEvent> for EventView {
    fn from(event: StoredEvent) -> Self {
        Self {
            cursor: event.cursor,
            event_type: event.envelope.event_type,
            task_id: event.envelope.task_id.map(|task| task.0),
            created_at: event.envelope.created_at.to_rfc3339(),
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
            match daemon.store.events_after(&run_id, cursor, 256) {
                Ok(events) if events.is_empty() => {
                    yield Ok(Event::default().comment("keepalive"));
                }
                Ok(events) => {
                    for event in events {
                        cursor = event.cursor;
                        let data = serde_json::to_string(&EventView::from(event))
                            .unwrap_or_else(|_| "{}".to_owned());
                        yield Ok(Event::default().id(cursor.to_string()).event("akzio").data(data));
                    }
                }
                Err(error) => yield Ok(Event::default().event("error").data(error.to_string())),
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn http_submit(
    State(daemon): State<Arc<Daemon>>,
    headers: HeaderMap,
    Json(request): Json<SubmitRequest>,
) -> std::result::Result<Json<DaemonReply>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .handle(DaemonCommand::Submit {
            purpose: request.purpose,
        })
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_cancel(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<DaemonReply>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .handle(DaemonCommand::Cancel {
            run_id: RunId(run_id),
        })
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn http_retry(
    State(daemon): State<Arc<Daemon>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Json<DaemonReply>, StatusCode> {
    authorize(&daemon, &headers)?;
    daemon
        .handle(DaemonCommand::Retry {
            run_id: RunId(run_id),
        })
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn authorize(daemon: &Daemon, headers: &HeaderMap) -> std::result::Result<(), StatusCode> {
    (headers
        .get("x-akzio-token")
        .and_then(|value| value.to_str().ok())
        == Some(daemon.http_token.as_str()))
    .then_some(())
    .ok_or(StatusCode::UNAUTHORIZED)
}

pub fn fixture_model_client() -> ModelClient {
    ModelClient::FixtureBySchema(BTreeMap::from([
        (
            "workflow_plan".to_owned(),
            serde_json::json!({
                "output_text": r#"{"summary":"fixture plan","tasks":[{"role":"investigator","question":"Assess current four-ETF evidence","priority":80},{"role":"challenger","question":"Find missing or contradictory evidence","priority":70}]}"#
            }),
        ),
        (
            "claims".to_owned(),
            serde_json::json!({
                "output_text": r#"{"summary":"fixture evidence","claims":[{"claim":"No actionable edge in fixture input","evidence_refs":[]}]}"#
            }),
        ),
        (
            "challenge".to_owned(),
            serde_json::json!({
                "output_text": r#"{"summary":"fixture challenge","verdict":"unresolved","arguments":[]}"#
            }),
        ),
        (
            "decision_draft".to_owned(),
            serde_json::json!({
                    "output_text": r#"{"summary":"fixture decision: stay in cash","targets":{"weights":{"TQQQ":0,"QQQ":0,"SOXX":0,"SOXL":0}},"confidence_ppm":500000,"forecasts":[{"trading_days":1,"positive_return_probability_ppm":500000,"expected_return_ppm":0},{"trading_days":3,"positive_return_probability_ppm":500000,"expected_return_ppm":0},{"trading_days":5,"positive_return_probability_ppm":500000,"expected_return_ppm":0}],"blockers":[],"claim_refs":[]}"#
            }),
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn daemon() -> (tempfile::TempDir, Daemon) {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            DaemonConfig {
                store_root: directory.path().to_path_buf(),
                http_token: "test-token".to_owned(),
                worker_count: 2,
                auto_paper: false,
            },
            fixture_model_client(),
        )
        .unwrap();
        (directory, daemon)
    }

    #[test]
    fn command_router_reads_the_durable_event_log() {
        let (_directory, daemon) = daemon();
        let run = RunId::new();
        daemon
            .store()
            .create_run(&run, RunPurpose::Debug, "test", Utc::now())
            .unwrap();
        daemon
            .store()
            .append_event(&akzio_domain::EventEnvelope {
                schema_version: akzio_domain::V2_SCHEMA_VERSION,
                run_id: run.clone(),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
                causation_id: None,
                event_type: "workflow.submitted".to_owned(),
                payload_document_id: None,
                payload: None,
                created_at: Utc::now(),
            })
            .unwrap();
        let DaemonReply::Events { events } = daemon
            .handle(DaemonCommand::Events {
                run_id: run,
                after: 0,
            })
            .unwrap()
        else {
            panic!("expected events");
        };
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn paper_dry_run_never_creates_canonical_topology_state() {
        let (_directory, daemon) = daemon();
        let topology_state_count = || {
            daemon
                .store()
                .documents_by_kind(DocumentKind::Evaluation)
                .unwrap()
                .into_iter()
                .filter(|document| document.producer == "learning.topology_state")
                .count()
        };

        let before = topology_state_count();
        let plan = daemon
            .default_plan(&RunId::new(), RunPurpose::PaperDryRun)
            .unwrap();
        assert_eq!(plan.topology_id, baseline_topology());
        assert_eq!(topology_state_count(), before);

        let run_id = daemon.submit_default(RunPurpose::PaperDryRun).unwrap();
        assert_eq!(topology_state_count(), before);
        assert!(daemon
            .store()
            .documents_for_run(&run_id)
            .unwrap()
            .into_iter()
            .all(|document| document.producer != "learning.topology_state"));
    }

    #[test]
    fn paper_schedule_recovers_a_reserved_slot_after_leader_takeover() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            DaemonConfig {
                store_root: directory.path().to_path_buf(),
                http_token: "test-token".to_owned(),
                worker_count: 2,
                auto_paper: true,
            },
            fixture_model_client(),
        )
        .unwrap();
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-06T14:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let first_lease = daemon
            .store
            .acquire_daemon_lease(
                SCHEDULER_LEASE_NAME,
                "leader-a",
                now,
                now + chrono::Duration::seconds(SCHEDULER_LEASE_SECONDS),
            )
            .unwrap()
            .unwrap();
        let run_id = RunId::new();
        let plan = daemon.default_plan(&run_id, RunPurpose::Paper).unwrap();
        let reservation = daemon
            .store
            .reserve_paper_schedule_slot(&first_lease, "paper:2026-08-06", &run_id, &plan, now)
            .unwrap();
        assert!(reservation.newly_reserved);
        assert!(!daemon.store.run_exists(&run_id).unwrap());

        let takeover_at = now + chrono::Duration::seconds(SCHEDULER_LEASE_SECONDS + 1);
        let replacement_lease = daemon
            .store
            .acquire_daemon_lease(
                SCHEDULER_LEASE_NAME,
                "leader-b",
                takeover_at,
                takeover_at + chrono::Duration::seconds(SCHEDULER_LEASE_SECONDS),
            )
            .unwrap()
            .unwrap();
        let scheduled = daemon
            .schedule_paper_session(
                &replacement_lease,
                chrono::NaiveDate::from_ymd_opt(2026, 8, 6).unwrap(),
                takeover_at,
            )
            .unwrap()
            .unwrap();
        assert_eq!(scheduled, run_id);
        assert_eq!(
            daemon.store.run_purpose(&run_id).unwrap(),
            RunPurpose::Paper
        );
        assert_eq!(daemon.store.workflow_plan(&run_id).unwrap(), plan);
        assert!(daemon
            .store
            .paper_schedule_slot("paper:2026-08-06")
            .unwrap()
            .unwrap()
            .submitted_at
            .is_some());
        assert_eq!(
            daemon
                .store
                .events_after(&run_id, 0, 32)
                .unwrap()
                .into_iter()
                .filter(|event| event.envelope.event_type == "scheduler.paper_submitted")
                .count(),
            1
        );
        assert_eq!(
            daemon
                .schedule_paper_session(
                    &replacement_lease,
                    chrono::NaiveDate::from_ymd_opt(2026, 8, 6).unwrap(),
                    takeover_at,
                )
                .unwrap(),
            Some(run_id.clone())
        );
        daemon.store.verify_integrity().unwrap();
    }

    #[test]
    fn direct_paper_submission_is_rejected() {
        let (_directory, daemon) = daemon();
        assert!(matches!(
            daemon.submit_default(RunPurpose::Paper),
            Err(DaemonError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn debug_run_uses_the_real_dispatcher_without_paper_orders() {
        let (_directory, daemon) = daemon();
        let run = daemon.submit_default(RunPurpose::Debug).unwrap();
        while daemon.run_one("fixture").await.unwrap() {}

        assert_eq!(
            daemon.store().run_status(&run).unwrap(),
            akzio_domain::WorkflowStatus::Completed
        );
        assert!(daemon.store().verify_integrity().is_ok());
        let documents = daemon.store().documents_for_run(&run).unwrap();
        assert!(documents
            .iter()
            .any(|document| document.kind == DocumentKind::Decision));
        assert!(documents
            .iter()
            .any(|document| document.kind == DocumentKind::ExecutionPlan));
        assert!(documents
            .iter()
            .all(|document| document.producer != "alpaca.paper"));
        assert_eq!(daemon.store.child_run(&run, "shadow").unwrap(), None);
    }

    #[tokio::test]
    async fn leader_supervisor_runs_workers_and_releases_its_lease() {
        let (_directory, daemon) = daemon();
        let daemon = Arc::new(daemon);
        let run = daemon.submit_default(RunPurpose::Debug).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let service = tokio::spawn(daemon.clone().serve_workers(shutdown_rx));
        for _ in 0..100 {
            if matches!(
                daemon.store().run_status(&run).unwrap(),
                akzio_domain::WorkflowStatus::Completed
                    | akzio_domain::WorkflowStatus::CompletedWithExecutionRejection
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            daemon.store().run_status(&run).unwrap(),
            akzio_domain::WorkflowStatus::Completed
        );
        assert!(daemon.health().unwrap().scheduler_owner.is_some());
        shutdown_tx.send(true).unwrap();
        service.await.unwrap().unwrap();
        assert!(daemon.health().unwrap().scheduler_owner.is_none());
        daemon.store().verify_integrity().unwrap();
    }
}
