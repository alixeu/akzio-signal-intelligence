//! Durable local control plane for Akzio v2.
//!
//! This crate is deliberately thin: it owns worker supervision and transport,
//! while the v2 Store and runtimes own durable state, contracts, context
//! grants, task attempts, and workflow transitions.

mod dispatch;
mod worker;

pub use worker::{TaskHandler, WorkerPool, WorkerPoolConfig};

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use akzio_domain::{
    ArtifactId, ArtifactKind, ArtifactRef, ContextPolicy, EvidenceNeed, RunId, RunPurpose,
    RuntimeTaskClass, TaskId, TaskStatus,
};
use akzio_ingest::{
    AcquiredEvidence, EvidenceRequest, EvidenceRuntime, EvidenceRuntimeError, EvidenceSource,
    FixtureEvidenceAdapter,
};
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
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, UnixListener},
    sync::watch,
};

const SCHEDULER_LEASE_NAME: &str = "akzio.local.scheduler";
const EVENT_PAGE_SIZE: usize = 256;

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
    http_token: String,
    auto_paper: bool,
    worker_pool: WorkerPoolConfig,
}

#[derive(Debug, Clone, Serialize)]
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
#[serde(tag = "reply", rename_all = "snake_case")]
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

        Ok(Self {
            task_runtime: TaskRuntime::new(store.clone()),
            workflow,
            agents,
            model: ModelClientAdapter::new(model),
            fixture_evidence: Arc::new(fixture_evidence),
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

    /// Paper sessions are scheduler-owned and require a frozen session slot.
    /// The R5 daemon does not construct one directly, so this public submit
    /// surface rejects Paper before any workflow or broker side effect.
    pub fn submit_default(&self, purpose: RunPurpose) -> Result<RunId> {
        if purpose == RunPurpose::Paper {
            return Err(DaemonError::InvalidInput(
                "Paper runs are scheduler-owned and unavailable until the fenced scheduler is wired"
                    .to_owned(),
            ));
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
        Ok(DaemonHealth {
            status: if self.auto_paper {
                "paper_scheduler_fail_closed".to_owned()
            } else {
                "ok".to_owned()
            },
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
                    .events_after(&run_id, after, EVENT_PAGE_SIZE)?
                    .into_iter()
                    .map(EventView::from)
                    .collect(),
            }),
            DaemonCommand::Submit { purpose } => Ok(DaemonReply::Submitted {
                run_id: self.submit_default(purpose)?,
            }),
            DaemonCommand::Cancel { .. } => Err(DaemonError::Unavailable(
                "v2 cancellation is not wired; fail closed rather than mutate task state outside TaskRuntime"
                    .to_owned(),
            )),
            DaemonCommand::Retry { .. } => Err(DaemonError::Unavailable(
                "v2 retry is owned by TaskRuntime retry policy; direct run retry is unavailable"
                    .to_owned(),
            )),
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(http_health))
            .route("/runs/{run_id}/events", get(http_events))
            .route("/runs", post(http_submit))
            .route("/runs/{run_id}/cancel", post(http_cancel))
            .route("/runs/{run_id}/retry", post(http_retry))
            .with_state(Arc::new(self.clone()))
    }

    pub async fn serve_http(
        &self,
        address: SocketAddr,
        shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let listener = TcpListener::bind(address).await?;
        axum::serve(listener, self.router())
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await
            .map_err(DaemonError::Io)
    }

    /// Transitional local Unix JSON transport retained until R8/R9. Its
    /// commands call the same v2 Daemon API as HTTP and never bypass it.
    pub async fn serve_unix(
        &self,
        path: PathBuf,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }
        let listener = UnixListener::bind(&path)?;

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let daemon = self.clone();
                    tokio::spawn(async move {
                        let (read, mut write) = stream.into_split();
                        let mut lines = BufReader::new(read).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            let payload = match serde_json::from_str::<DaemonCommand>(&line)
                                .map_err(DaemonError::from)
                                .and_then(|command| daemon.handle(command))
                            {
                                Ok(reply) => serde_json::to_string(&reply),
                                Err(error) => serde_json::to_string(&serde_json::json!({
                                    "error": error.to_string(),
                                })),
                            };
                            let Ok(payload) = payload else {
                                break;
                            };
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
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
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
            RuntimeTaskClass::DecisionGate
            | RuntimeTaskClass::ExecutionGate
            | RuntimeTaskClass::PaperCommit
            | RuntimeTaskClass::Reconcile
            | RuntimeTaskClass::Evaluate => {
                Err(DaemonError::UnsupportedTaskClass(recipe.task_class))
            }
        }
    }

    /// Build agent input strictly from its declared inputs and the Store's
    /// semantic committed-output query for declared, successful dependencies.
    /// This never scans a run's artifact set or exposes raw evidence;
    /// `AgentRuntime` then creates the task-bound ContextManifest and
    /// ReadGrant.
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
                    resource: need.resource,
                    max_age: Duration::seconds(max_age_secs),
                },
                &adapter,
                now,
            )?;
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
        .map_err(|_| StatusCode::CONFLICT)
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
        .map_err(|_| StatusCode::CONFLICT)
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
                "blockers": [],
                "asset_views": {}
            })),
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::{
        ArtifactKind, EvidenceNeed, TaskRecipeId, WorkflowProposalDraft, WorkflowProposalDraftTask,
    };
    use akzio_ingest::NormalizedEvidencePayload;
    use tempfile::tempdir;

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
        assert!(matches!(
            daemon.handle(DaemonCommand::Retry {
                run_id: RunId::new(),
            }),
            Err(DaemonError::Unavailable(_))
        ));
    }
}
