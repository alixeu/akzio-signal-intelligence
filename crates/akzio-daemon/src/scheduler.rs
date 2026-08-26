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
    AgentContract, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance,
    ArtifactRef, Asset, CanaryCampaignStatus, CanarySessionReservation, DomainError, EvidenceNeed,
    PolicySubject, RunId, RunPurpose, RuntimeManifest, TopologyId, WorkflowProposal,
    STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_execution::paper::AlpacaPaper;
use akzio_ingest::AlpacaMarketDataFeed;
use akzio_runtime::{RuntimeError, WorkflowRuntime};
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

const PAPER_ACCOUNT_RESOURCE: &str = "paper.account";
const PAPER_POSITIONS_RESOURCE: &str = "paper.positions";
const PAPER_OPEN_ORDERS_RESOURCE: &str = "paper.open_orders";
const PAPER_QUOTES_RESOURCE: &str = "paper.quotes";
const PAPER_CLOCK_RESOURCE: &str = "paper.clock";

pub(super) fn paper_snapshot_resources(session_key: &str) -> Vec<String> {
    let start = NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.checked_sub_signed(Duration::days(28)))
        .map(|date| date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| session_key.to_owned());
    let mut resources = vec![
        PAPER_ACCOUNT_RESOURCE.to_owned(),
        PAPER_POSITIONS_RESOURCE.to_owned(),
        PAPER_OPEN_ORDERS_RESOURCE.to_owned(),
        format!("paper.fills:{session_key}"),
        PAPER_QUOTES_RESOURCE.to_owned(),
        PAPER_CLOCK_RESOURCE.to_owned(),
    ];
    resources.extend(
        Asset::EXECUTABLE
            .into_iter()
            .map(|asset| format!("bars:{}:1d:{start}:32", asset.symbol())),
    );
    resources
}

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
    fn proposal(&self, session_key: &str) -> SchedulerResult<WorkflowProposal>;
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
            store,
            bootstrap: None,
        }
    }

    pub fn with_bootstrap(
        mut self,
        workflow: WorkflowRuntime,
        topology_id: impl Into<String>,
    ) -> Self {
        self.bootstrap = Some((workflow, topology_id.into()));
        self
    }
}

impl PaperWorkflowSource for StorePaperWorkflowSource {
    fn proposal(&self, _session_key: &str) -> SchedulerResult<WorkflowProposal> {
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
    market_data_feed: Option<AlpacaMarketDataFeed>,
    runtime_identity_hash: Option<akzio_domain::ContentHash>,
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
            market_data_feed: None,
            runtime_identity_hash: None,
        })
    }

    pub fn with_market_data_feed(mut self, market_data_feed: Option<AlpacaMarketDataFeed>) -> Self {
        self.market_data_feed = market_data_feed;
        self
    }

    pub fn with_runtime_identity_hash(
        mut self,
        runtime_identity_hash: Option<akzio_domain::ContentHash>,
    ) -> Self {
        self.runtime_identity_hash = runtime_identity_hash;
        self
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> SchedulerResult<Self> {
        if lease_duration <= Duration::zero() {
            return Err(SchedulerError::InvalidOwner);
        }
        self.lease_duration = lease_duration;
        Ok(self)
    }

    fn current_approval_binding(&self) -> SchedulerResult<Option<(Artifact, Artifact)>> {
        let Some(approval) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::PaperLaunchApproval)?
        else {
            return Ok(None);
        };
        let manifest_ref = approval
            .source_refs
            .first()
            .filter(|reference| reference.kind == ArtifactKind::RuntimeManifest)
            .ok_or(SchedulerError::WorkflowUnavailable)?;
        let manifest = self.store.artifact(&manifest_ref.artifact_id)?;
        Ok(Some((manifest, approval)))
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub fn active_lease(&self, now: DateTime<Utc>) -> SchedulerResult<DaemonLease> {
        self.acquire_or_renew(now)
    }

    pub fn reserve_canary_session(
        &self,
        reservation: &akzio_domain::CanarySessionReservation,
    ) -> SchedulerResult<StoredCanarySession> {
        let lease = self.acquire_or_renew(reservation.reserved_at)?;
        if lease.epoch != reservation.scheduler_epoch {
            return Err(SchedulerError::NotLeader);
        }
        Ok(self.store.reserve_canary_session(&lease, reservation)?)
    }

    fn paper_snapshot_artifacts(
        &self,
        run_id: &RunId,
        session_key: &str,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Vec<Artifact>> {
        paper_snapshot_resources(session_key)
            .into_iter()
            .map(|resource| {
                let need = EvidenceNeed {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    source_family: "alpaca".to_owned(),
                    resource,
                    max_age_secs: 5,
                };
                need.validate()?;
                Ok(Artifact::new(
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
                )?)
            })
            .collect()
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
        let setup_artifacts = self.paper_snapshot_artifacts(&run_id, session_key, now)?;
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
            eprintln!("Paper scheduler waiting: broker market is closed");
            return Ok(None);
        };
        if let Some(slot) = self.store.session_slot(&session_key)? {
            return Ok(Some(SessionSlotReservation {
                slot,
                newly_reserved: false,
            }));
        }
        if let Some(campaign) = self.store.active_canary_campaign()? {
            if campaign.status == CanaryCampaignStatus::Staged {
                return Ok(None);
            }
            if campaign.status.is_level() {
                return self.tick_canary(&campaign, &session_key, clock, now).await;
            }
        }
        let Some((runtime_manifest, approval)) = self.current_approval_binding()? else {
            eprintln!("Paper scheduler waiting: no current Paper approval binding");
            return Ok(None);
        };
        let manifest_payload: RuntimeManifest =
            serde_json::from_slice(&self.store.read_blob(&runtime_manifest.blob)?)?;
        if let Some(expected) = &self.runtime_identity_hash {
            if manifest_payload.runtime_identity_hash()? != *expected {
                eprintln!("Paper scheduler waiting: runtime identity does not match approval");
                return Ok(None);
            }
        }
        let account_id = clock.paper_account_id().await?;
        if manifest_payload.broker_account_id != account_id
            || self
                .market_data_feed
                .is_none_or(|feed| manifest_payload.market_data_feed != feed.as_str())
        {
            eprintln!("Paper scheduler waiting: broker account or market-data feed mismatch");
            return Ok(None);
        }
        let proposal = match source.proposal(&session_key) {
            Ok(proposal) => proposal,
            Err(SchedulerError::WorkflowUnavailable) => {
                eprintln!("Paper scheduler waiting: workflow proposal unavailable");
                return Ok(None);
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
        for resource in paper_snapshot_resources(&session_key) {
            let need = EvidenceNeed {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                source_family: "alpaca".to_owned(),
                resource,
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
            self.workflow
                .reserve_paper_session_with_inputs_for_run_approved(
                    &lease,
                    run_id,
                    &session_key,
                    &proposal,
                    &setup_artifacts,
                    &runtime_manifest,
                    &approval,
                    now,
                )?,
        ))
    }

    async fn tick_canary<C>(
        &self,
        campaign: &akzio_store::v2::CanaryCampaignHead,
        session_key: &str,
        clock: &C,
        now: DateTime<Utc>,
    ) -> SchedulerResult<Option<SessionSlotReservation>>
    where
        C: BrokerSessionClock + ?Sized,
    {
        if self
            .store
            .canary_session(&campaign.spec.campaign_id, campaign.status)?
            .is_some()
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }
        let Some((runtime_manifest, approval)) = self.current_approval_binding()? else {
            return Ok(None);
        };
        let manifest_payload: RuntimeManifest =
            serde_json::from_slice(&self.store.read_blob(&runtime_manifest.blob)?)?;
        if let Some(expected) = &self.runtime_identity_hash {
            if manifest_payload.runtime_identity_hash()? != *expected {
                return Ok(None);
            }
        }
        let account_id = clock.paper_account_id().await?;
        if manifest_payload.broker_account_id != account_id
            || self
                .market_data_feed
                .is_some_and(|feed| manifest_payload.market_data_feed != feed.as_str())
        {
            return Ok(None);
        }

        let candidate_artifact = self
            .store
            .artifact(&campaign.spec.candidate_contract.artifact_id)?;
        let candidate: AgentContract =
            serde_json::from_slice(&self.store.read_blob(&candidate_artifact.blob)?)?;
        candidate.validate()?;
        let candidate_installation = self
            .store
            .contract_installation(&candidate.contract_hash)?
            .ok_or(SchedulerError::WorkflowUnavailable)?;
        if candidate_artifact.kind != ArtifactKind::Contract
            || candidate_artifact.lifecycle != ArtifactLifecycle::Canonical
            || candidate.purpose.as_str() != "research.analyst"
            || candidate.contract_hash == campaign.spec.active_contract_hash
            || candidate_installation.activated_at.is_some()
            || candidate_installation.baseline_contract_hash.as_ref()
                != Some(&campaign.spec.active_contract_hash)
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }

        let candidate_topology_artifact = self
            .store
            .artifact(&campaign.spec.candidate_topology.artifact_id)?;
        let candidate_topology: akzio_domain::WorkflowGraph =
            serde_json::from_slice(&self.store.read_blob(&candidate_topology_artifact.blob)?)?;
        candidate_topology.validate()?;
        if candidate_topology_artifact.kind != ArtifactKind::WorkflowGraph
            || candidate_topology_artifact.lifecycle != ArtifactLifecycle::RunScoped
            || candidate_topology.topology_id != STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID
        {
            return Err(SchedulerError::WorkflowUnavailable);
        }

        let active_analyst = self
            .workflow
            .catalogue()
            .recipe(&akzio_domain::TaskRecipeId::new("research.analyst")?)?;
        if active_analyst.contract_hash.as_ref() != Some(&campaign.spec.active_contract_hash) {
            return Err(SchedulerError::WorkflowUnavailable);
        }

        let lease = self.acquire_or_renew(now)?;
        let parent_run_id = RunId::new();
        let setup_artifacts = self.paper_snapshot_artifacts(&parent_run_id, session_key, now)?;
        let mut parent_proposal = self.workflow.approved_paper_proposal("active")?;
        parent_proposal
            .tasks
            .get_mut("analyst")
            .ok_or(SchedulerError::WorkflowUnavailable)?
            .evidence_needs = setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::EvidenceNeed,
            })
            .collect();
        let (parent_reservation, parent_proposal_artifact) = self
            .workflow
            .prepare_approved_paper_session_with_inputs_for_run(
                parent_run_id.clone(),
                session_key,
                &parent_proposal,
                &setup_artifacts,
                now,
            )?;
        let snapshot_refs = parent_reservation
            .workflow
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == "research.analyst")
            .map(|node| node.input_artifacts.clone())
            .ok_or(SchedulerError::WorkflowUnavailable)?;

        let mut contract_proposal = parent_proposal.clone();
        contract_proposal
            .tasks
            .get_mut("analyst")
            .ok_or(SchedulerError::WorkflowUnavailable)?
            .evidence_needs = snapshot_refs.clone();
        let active_contract_hash = active_analyst
            .contract_hash
            .as_ref()
            .ok_or(SchedulerError::WorkflowUnavailable)?;

        let contract_shadow_run_id = RunId::new();
        let topology_shadow_run_id = RunId::new();
        let bundle_shadow_run_id = RunId::new();
        let contract_shadow = self.workflow.prepare_workflow_commit(
            contract_shadow_run_id.clone(),
            RunPurpose::Shadow,
            self.workflow
                .lower_shadow(&contract_proposal, Some(&candidate.contract_hash))?,
            now,
        )?;
        let topology_shadow = self.workflow.prepare_workflow_commit(
            topology_shadow_run_id.clone(),
            RunPurpose::Shadow,
            self.workflow.lower_shadow_from_graph(
                &candidate_topology,
                &snapshot_refs,
                Some(active_contract_hash),
            )?,
            now,
        )?;
        let bundle_shadow = self.workflow.prepare_workflow_commit(
            bundle_shadow_run_id.clone(),
            RunPurpose::Shadow,
            self.workflow.lower_shadow_from_graph(
                &candidate_topology,
                &snapshot_refs,
                Some(&candidate.contract_hash),
            )?,
            now,
        )?;

        let canary_reservation = CanarySessionReservation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            campaign_id: campaign.spec.campaign_id.clone(),
            level: campaign.status,
            session_key: session_key.to_owned(),
            parent_run_id,
            contract_shadow_run_id,
            topology_shadow_run_id,
            bundle_shadow_run_id,
            scheduler_epoch: lease.epoch,
            reserved_at: now,
        };
        let parent = self.store.reserve_canary_session_with_workflows(
            &lease,
            &parent_reservation,
            &parent_proposal_artifact,
            &runtime_manifest,
            &approval,
            &[contract_shadow, topology_shadow, bundle_shadow],
            &canary_reservation,
        )?;
        Ok(Some(parent))
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
        let mut held = self
            .lease
            .lock()
            .map_err(|_| SchedulerError::LeasePoisoned)?;
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
