//! Fenced, scheduler-owned Paper session reservation.
//!
//! This module owns leadership and session timing only. Workflow compilation
//! remains in `akzio-runtime`; Paper commitment and broker I/O remain in
//! `akzio-execution`.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

use akzio_domain::{
    paper_session_evidence_needs, AgentContract, Artifact, ArtifactKind, ArtifactLifecycle,
    ArtifactOrigin, ArtifactProvenance, ArtifactRef, CanaryCampaignStatus,
    CanarySessionReservation, DomainError, PolicySubject, RunId, RunPurpose, RuntimeManifest,
    TopologyId, WorkflowProposal, STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID,
    V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_execution::paper::AlpacaPaper;
use akzio_ingest::AlpacaMarketDataFeed;
use akzio_runtime::v2::{RuntimeError, StoreExecutor, WorkflowRuntime};
use akzio_store::v2::{
    DaemonLease, SessionSlotReservation, StoreError, StoredCanarySession, V2Store,
};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use thiserror::Error;
use tokio::sync::watch;

pub const SCHEDULER_LEASE_NAME: &str = "akzio.local.scheduler";

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("invalid scheduler owner")]
    InvalidOwner,
    #[error("scheduler is not the active leader")]
    NotLeader,
    #[error("scheduler lease state poisoned")]
    LeasePoisoned,
    #[error("broker session key must be an ISO-8601 trading date: {0}")]
    InvalidSessionKey(String),
    #[error("broker session clock failed: {0}")]
    Clock(String),
    #[error("no durable Paper workflow proposal is available")]
    WorkflowUnavailable,
    #[error("workflow proposal is not owned by a Paper run")]
    WorkflowNotPaper,
}

pub type SchedulerResult<T> = std::result::Result<T, SchedulerError>;

/// Producer of the scheduler-minted, session-specific snapshot needs. Only
/// these may be carried in a stored proposal and re-minted per session.
const PAPER_SNAPSHOT_PRODUCER: &str = "scheduler.paper_snapshot";

/// A broker-authoritative clock returns an open session date; local wall-clock
/// dates and hand-maintained market calendars never create Paper slots.
pub trait BrokerSessionClock: Send + Sync {
    fn open_session_key<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<Option<String>>> + Send + 'a>>;

    fn paper_account_id<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<String>> + Send + 'a>>;
}

/// Production adapter over the already fail-closed Alpaca Paper client. Its
/// constructor is deliberately injected, so daemon construction does not read
/// credentials or perform network I/O.
pub struct AlpacaPaperSessionClock {
    paper: AlpacaPaper,
}

impl AlpacaPaperSessionClock {
    pub fn new(paper: AlpacaPaper) -> Self {
        Self { paper }
    }
}

impl BrokerSessionClock for AlpacaPaperSessionClock {
    fn open_session_key<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<Option<String>>> + Send + 'a>> {
        Box::pin(async move {
            let clock = self
                .paper
                .market_clock()
                .await
                .map_err(|error| SchedulerError::Clock(error.to_string()))?;
            Ok(clock.is_open.then(|| clock.session_date.to_string()))
        })
    }

    fn paper_account_id<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<String>> + Send + 'a>> {
        Box::pin(async move {
            self.paper
                .account()
                .await
                .map_err(|error| SchedulerError::Clock(error.to_string()))?
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| SchedulerError::Clock("broker account id missing".to_owned()))
        })
    }
}

/// Provides only a Rust-validated `WorkflowProposal`; it cannot create a Run
/// or bypass the scheduler lease.
pub trait PaperWorkflowSource: Send + Sync {
    fn proposal<'a>(
        &'a self,
        session_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<WorkflowProposal>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct StaticPaperWorkflowSource {
    proposal: WorkflowProposal,
}

/// Prefers a durable Paper `WorkflowProposal` from `V2Store`. A bounded,
/// Rust-compiled bootstrap may be supplied for the first scheduler session;
/// reservation persists that proposal atomically with the Paper run.
#[derive(Clone)]
pub struct StorePaperWorkflowSource {
    store: V2Store,
    store_executor: StoreExecutor,
    bootstrap: Option<(WorkflowRuntime, String)>,
}

impl StaticPaperWorkflowSource {
    pub fn new(proposal: WorkflowProposal) -> Self {
        Self { proposal }
    }
}

impl StorePaperWorkflowSource {
    pub fn new(store: V2Store) -> Self {
        Self {
            store_executor: StoreExecutor::new(store.clone()),
            store,
            bootstrap: None,
        }
    }

    pub fn with_store_executor(mut self, store_executor: StoreExecutor) -> Self {
        self.store_executor = store_executor;
        self
    }

    pub fn with_bootstrap(
        mut self,
        workflow: WorkflowRuntime,
        topology_id: impl Into<String>,
    ) -> Self {
        self.bootstrap = Some((workflow, topology_id.into()));
        self
    }

    /// A stored proposal that still references another Run's planner-lowered
    /// `EvidenceNeed` can never be reserved, because reservation rejects
    /// cross-run RunScoped evidence fail-closed. Such a proposal must be
    /// skipped during selection; otherwise it stays the newest durable Paper
    /// proposal and stalls every future session.
    ///
    /// Scheduler snapshots are excluded: reservation drops and re-mints those
    /// for the new session, so carrying them is harmless.
    pub(crate) fn references_foreign_run_scoped_need(
        &self,
        proposal: &WorkflowProposal,
    ) -> SchedulerResult<bool> {
        for task in proposal.tasks.values() {
            for reference in &task.evidence_needs {
                let artifact = self.store.artifact(&reference.artifact_id)?;
                if artifact.kind != ArtifactKind::EvidenceNeed
                    || artifact.lifecycle != ArtifactLifecycle::RunScoped
                    || artifact.producer == PAPER_SNAPSHOT_PRODUCER
                {
                    continue;
                }
                if artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    .is_some()
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn proposal_sync(&self) -> SchedulerResult<WorkflowProposal> {
        let candidate_subject = PolicySubject::Topology(TopologyId(
            STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID.to_owned(),
        ));
        let preferred_topology = self
            .store
            .policy_head(&candidate_subject)?
            .filter(|head| {
                head.state
                    == akzio_domain::PolicyState::Topology(
                        akzio_domain::CandidatePolicyState::Active,
                    )
            })
            .map(|_| STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID)
            .unwrap_or("active");
        let mut latest_paper = None;
        for artifact in self
            .store
            .recent_artifacts_by_kind(ArtifactKind::WorkflowProposal, 500)?
        {
            let Some(run_id) = artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
            else {
                continue;
            };
            if self.store.run_purpose(run_id)? == RunPurpose::Paper {
                let proposal: WorkflowProposal =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                if proposal.topology_id == preferred_topology {
                    if self.references_foreign_run_scoped_need(&proposal)? {
                        continue;
                    }
                    latest_paper = Some(proposal);
                    break;
                }
            }
        }

        let Some(proposal) = latest_paper else {
            return self
                .bootstrap
                .as_ref()
                .map(|(workflow, topology_id)| {
                    workflow.approved_paper_proposal(topology_id.clone())
                })
                .transpose()?
                .ok_or(SchedulerError::WorkflowUnavailable);
        };
        Ok(proposal)
    }
}

impl PaperWorkflowSource for StorePaperWorkflowSource {
    fn proposal<'a>(
        &'a self,
        _session_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<WorkflowProposal>> + Send + 'a>> {
        Box::pin(async move {
            let source = self.clone();
            self.store_executor
                .execute(move |_| source.proposal_sync())
                .await?
        })
    }
}

impl PaperWorkflowSource for StaticPaperWorkflowSource {
    fn proposal<'a>(
        &'a self,
        _session_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<WorkflowProposal>> + Send + 'a>> {
        Box::pin(async move { Ok(self.proposal.clone()) })
    }
}

#[derive(Clone)]
pub struct PaperScheduler {
    store: V2Store,
    store_executor: StoreExecutor,
    workflow: WorkflowRuntime,
    owner_id: String,
    lease_duration: Duration,
    lease: Arc<Mutex<Option<DaemonLease>>>,
    market_data_feed: Option<AlpacaMarketDataFeed>,
    runtime_identity_hash: Option<akzio_domain::ContentHash>,
}

#[path = "scheduler_parts/canary.rs"]
mod canary;
#[path = "scheduler_parts/lease.rs"]
mod lease;
#[path = "scheduler_parts/scheduler_core.rs"]
mod scheduler_core;
#[path = "scheduler_parts/scheduler_tick.rs"]
mod scheduler_tick;
#[path = "scheduler_parts/serve.rs"]
mod serve;
