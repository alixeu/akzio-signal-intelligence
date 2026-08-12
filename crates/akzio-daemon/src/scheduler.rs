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
    Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef,
    DomainError, EvidenceNeed, RunId, RunPurpose, WorkflowProposal, V2_DOMAIN_SCHEMA_VERSION,
};
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
    #[error(transparent)]
    Domain(#[from] DomainError),
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

const PAPER_ACCOUNT_RESOURCE: &str = "paper.account";
const PAPER_QUOTES_RESOURCE: &str = "paper.quotes";
const PAPER_CLOCK_RESOURCE: &str = "paper.clock";

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

    pub fn reserve_approved_session(
        &self,
        run_id: RunId,
        session_key: &str,
        now: DateTime<Utc>,
    ) -> SchedulerResult<SessionSlotReservation> {
        NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
            .map_err(|_| SchedulerError::InvalidSessionKey(session_key.to_owned()))?;
        let lease = self.acquire_or_renew(now)?;
        let mut setup_artifacts = Vec::new();
        for resource in [
            PAPER_ACCOUNT_RESOURCE,
            PAPER_QUOTES_RESOURCE,
            PAPER_CLOCK_RESOURCE,
        ] {
            let need = EvidenceNeed {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                source_family: "alpaca".to_owned(),
                resource: resource.to_owned(),
                max_age_secs: 5,
            };
            need.validate()?;
            setup_artifacts.push(Artifact::new(
                ArtifactKind::EvidenceNeed,
                self.store.put_json(&need)?,
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
                Vec::new(),
                now,
            )?);
        }
        Ok(self.workflow.reserve_approved_paper_session(
            &lease,
            run_id,
            session_key,
            "paper.approved.v1",
            &setup_artifacts,
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
        if let Some(slot) = self.store.session_slot(&session_key)? {
            return Ok(Some(SessionSlotReservation {
                slot,
                newly_reserved: false,
            }));
        }
        let proposal = match source.proposal(&session_key) {
            Ok(proposal) => proposal,
            Err(SchedulerError::WorkflowUnavailable) => {
                return self
                    .reserve_approved_session(RunId::new(), &session_key, now)
                    .map(Some);
            }
            Err(error) => return Err(error),
        };
        let lease = self.acquire_or_renew(now)?;
        let run_id = RunId::new();
        let mut setup_artifacts = Vec::new();
        let mut proposal = proposal;

        for task in proposal.tasks.values_mut() {
            let mut retained = Vec::with_capacity(task.evidence_needs.len());
            for reference in task.evidence_needs.drain(..) {
                let artifact = self.store.artifact(&reference.artifact_id)?;
                if artifact.kind == ArtifactKind::EvidenceNeed
                    && artifact.lifecycle == ArtifactLifecycle::RunScoped
                {
                    let origin_run = artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref());
                    if artifact.producer == "scheduler.paper_snapshot" {
                        // Snapshot inputs are session-specific. A new slot must
                        // acquire fresh account/quotes/clock evidence below,
                        // never carry a prior Run's scheduler snapshot forward.
                        if origin_run.is_none() {
                            return Err(SchedulerError::WorkflowUnavailable);
                        }
                        continue;
                    }
                    if origin_run.is_some() {
                        return Err(SchedulerError::WorkflowUnavailable);
                    }
                }
                retained.push(reference);
            }
            task.evidence_needs = retained;
        }

        let snapshot_alias = proposal
            .tasks
            .iter()
            .find_map(|(alias, task)| {
                self.workflow
                    .catalogue()
                    .recipe(&task.recipe_id)
                    .ok()
                    .filter(|recipe| recipe.allowed_evidence_sources.contains("alpaca"))
                    .map(|_| alias.clone())
            })
            .ok_or(SchedulerError::WorkflowUnavailable)?;
        let first_task = proposal
            .tasks
            .get_mut(&snapshot_alias)
            .ok_or(SchedulerError::WorkflowUnavailable)?;
        for resource in [
            PAPER_ACCOUNT_RESOURCE,
            PAPER_QUOTES_RESOURCE,
            PAPER_CLOCK_RESOURCE,
        ] {
            let need = EvidenceNeed {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                source_family: "alpaca".to_owned(),
                resource: resource.to_owned(),
                max_age_secs: 5,
            };
            need.validate()?;
            let artifact = Artifact::new(
                ArtifactKind::EvidenceNeed,
                self.store.put_json(&need)?,
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
                Vec::new(),
                now,
            )?;
            first_task.evidence_needs.push(ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::EvidenceNeed,
            });
            setup_artifacts.push(artifact);
        }

        Ok(Some(
            self.workflow.reserve_paper_session_with_inputs_for_run(
                &lease,
                run_id,
                &session_key,
                &proposal,
                &setup_artifacts,
                now,
            )?,
        ))
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
