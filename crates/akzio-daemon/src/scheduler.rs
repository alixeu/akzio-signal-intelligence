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

use akzio_domain::{Artifact, ArtifactKind, RunId, RunPurpose, WorkflowProposal};
use akzio_execution::paper::AlpacaPaper;
use akzio_runtime::{RuntimeError, WorkflowRuntime};
use akzio_store::v2::{DaemonLease, SessionSlotReservation, StoreError, V2Store};
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
    #[error("invalid scheduler owner")]
    InvalidOwner,
    #[error("scheduler is not the active leader")]
    NotLeader,
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

/// A broker-authoritative clock returns an open session date; local wall-clock
/// dates and hand-maintained market calendars never create Paper slots.
pub trait BrokerSessionClock: Send + Sync {
    fn open_session_key<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = SchedulerResult<Option<String>>> + Send + 'a>>;
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
}

/// Provides only a Rust-validated `WorkflowProposal`; it cannot create a Run
/// or bypass the scheduler lease.
pub trait PaperWorkflowSource: Send + Sync {
    fn proposal(&self, session_key: &str) -> SchedulerResult<WorkflowProposal>;
}

#[derive(Clone)]
pub struct StaticPaperWorkflowSource {
    proposal: WorkflowProposal,
}

/// Reads only a durable `WorkflowProposal` from `V2Store`; it never builds a
/// replacement plan after a session slot has been reserved.
pub struct StorePaperWorkflowSource {
    store: V2Store,
}

impl StaticPaperWorkflowSource {
    pub fn new(proposal: WorkflowProposal) -> Self {
        Self { proposal }
    }
}

impl StorePaperWorkflowSource {
    pub fn new(store: V2Store) -> Self {
        Self { store }
    }
}

impl PaperWorkflowSource for StorePaperWorkflowSource {
    fn proposal(&self, _session_key: &str) -> SchedulerResult<WorkflowProposal> {
        let artifact = self
            .store
            .latest_artifact_by_kind(ArtifactKind::WorkflowProposal)?
            .ok_or(SchedulerError::WorkflowUnavailable)?;
        let run_id = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            .ok_or(SchedulerError::WorkflowNotPaper)?;
        if self.store.run_purpose(run_id)? != RunPurpose::Paper {
            return Err(SchedulerError::WorkflowNotPaper);
        }
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }
}

impl PaperWorkflowSource for StaticPaperWorkflowSource {
    fn proposal(&self, _session_key: &str) -> SchedulerResult<WorkflowProposal> {
        Ok(self.proposal.clone())
    }
}

#[derive(Clone)]
pub struct PaperScheduler {
    store: V2Store,
    workflow: WorkflowRuntime,
    owner_id: String,
    lease_duration: Duration,
    lease: Arc<Mutex<Option<DaemonLease>>>,
}

impl PaperScheduler {
    pub fn new(
        store: V2Store,
        workflow: WorkflowRuntime,
        owner_id: String,
    ) -> SchedulerResult<Self> {
        if owner_id.trim().is_empty() {
            return Err(SchedulerError::InvalidOwner);
        }
        Ok(Self {
            store,
            workflow,
            owner_id,
            lease_duration: Duration::seconds(30),
            lease: Arc::new(Mutex::new(None)),
        })
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> SchedulerResult<Self> {
        if lease_duration <= Duration::zero() {
            return Err(SchedulerError::InvalidOwner);
        }
        self.lease_duration = lease_duration;
        Ok(self)
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub fn active_lease(&self, now: DateTime<Utc>) -> SchedulerResult<DaemonLease> {
        self.acquire_or_renew(now)
    }

    pub fn reserve_session(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
        self.reserve_session_with_inputs(session_key, proposal, &[], now)
    }

    pub fn reserve_session_with_inputs(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
        NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
            .map_err(|_| SchedulerError::InvalidSessionKey(session_key.to_owned()))?;
        let lease = self.acquire_or_renew(now)?;
        Ok(self.workflow.reserve_paper_session_with_inputs(
            &lease,
            session_key,
            proposal,
            setup_artifacts,
            now,
        )?)
    }

    pub fn reserve_session_with_inputs_for_run(
        &self,
        run_id: RunId,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
        NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
            .map_err(|_| SchedulerError::InvalidSessionKey(session_key.to_owned()))?;
        let lease = self.acquire_or_renew(now)?;
        Ok(self.workflow.reserve_paper_session_with_inputs_for_run(
            &lease,
            run_id,
            session_key,
            proposal,
            setup_artifacts,
            now,
        )?)
    }

    pub async fn tick<C, P>(
        &self,
        clock: &C,
        source: &P,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Option<SessionSlotReservation>>
    where
        C: BrokerSessionClock + ?Sized,
        P: PaperWorkflowSource + ?Sized,
    {
        let Some(session_key) = clock.open_session_key().await? else {
            return Ok(None);
        };
        let proposal = source.proposal(&session_key)?;
        self.reserve_session(&session_key, &proposal, now).map(Some)
    }

    pub async fn serve<C, P>(
        &self,
        clock: &C,
        source: &P,
        poll_interval: StdDuration,
        mut shutdown: watch::Receiver<bool>,
    ) -> SchedulerResult<()>
    where
        C: BrokerSessionClock + ?Sized,
        P: PaperWorkflowSource + ?Sized,
    {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            if let Err(error) = self.tick(clock, source, Utc::now()).await {
                tracing::warn!(error = %error, "Paper scheduler tick failed closed");
            }
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    fn acquire_or_renew(&self, now: DateTime<Utc>) -> SchedulerResult<DaemonLease> {
        let expires_at = now + self.lease_duration;
        let mut held = self.lease.lock().expect("scheduler lease state poisoned");
        if let Some(lease) = held.as_mut() {
            if self.store.heartbeat_daemon_lease(lease, now, expires_at)? {
                lease.expires_at = expires_at;
                return Ok(lease.clone());
            }
            *held = None;
        }

        let lease = self
            .store
            .acquire_daemon_lease(SCHEDULER_LEASE_NAME, &self.owner_id, now, expires_at)?
            .ok_or(SchedulerError::NotLeader)?;
        *held = Some(lease.clone());
        Ok(lease)
    }
}
