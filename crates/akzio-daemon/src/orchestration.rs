use super::*;

impl Daemon {
    /// Construct the local daemon with its production model adapter. Model
    /// credentials stay in local configuration and are never persisted.
    pub fn open(config: DaemonConfig, model_config: ModelConfig) -> Result<Self> {
        let debug = model_config.debug;
        let auto_paper = config.auto_paper;
        let market_data_feed = config.market_data_feed;
        if auto_paper && market_data_feed.is_none() {
            return Err(DaemonError::InvalidInput(
                "auto_paper requires an explicit Alpaca market-data feed".to_owned(),
            ));
        }
        let model = ModelClient::from_config(&model_config)?;
        let stage_models = model_config
            .routes
            .iter()
            .map(|(purpose, route)| {
                let route_config = model_config.for_route(route);
                Ok((
                    purpose.clone(),
                    ModelClientAdapter::with_response_language(
                        ModelClient::from_config(&route_config)?,
                        debug,
                        route_config.response_language,
                    ),
                ))
            })
            .collect::<std::result::Result<BTreeMap<_, _>, ModelError>>()?;
        let mut daemon = Self::with_fixture_evidence_debug(
            config,
            model.clone(),
            FixtureEvidence::new(),
            debug,
            false,
        )?;
        daemon.model = ModelClientAdapter::with_response_language(
            model.clone(),
            debug,
            model_config.response_language,
        );
        daemon.stage_models = Arc::new(stage_models);
        let mut production_evidence: BTreeMap<EvidenceSource, Arc<dyn AsyncEvidenceAdapter>> =
            BTreeMap::new();
        if let Ok(alpaca) = AlpacaPaperEvidenceTransport::from_env(market_data_feed) {
            production_evidence.insert(EvidenceSource::Alpaca, Arc::new(alpaca));
        }
        if let Ok(sec) = SecEdgarDirectTransport::from_env() {
            production_evidence.insert(EvidenceSource::SecEdgar, Arc::new(sec));
        }
        if let Ok(fred) = FredDirectTransport::from_env() {
            production_evidence.insert(EvidenceSource::Fred, Arc::new(fred));
        }
        production_evidence.insert(
            EvidenceSource::NewsWeb,
            Arc::new(ModelNativeWebEvidenceTransport::for_source(
                model.clone(),
                EvidenceSource::NewsWeb,
            )),
        );
        let outcome_worker_enabled =
            auto_paper && production_evidence.contains_key(&EvidenceSource::Alpaca);
        daemon.production_evidence = Arc::new(production_evidence);
        daemon.outcome_scheduling_runtime = OutcomeSchedulingRuntime::new(daemon.store.clone())
            .with_worker_enabled(outcome_worker_enabled);
        Ok(daemon)
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
        Self::with_fixture_evidence_debug(config, model, fixture_evidence, false, true)
    }

    fn with_fixture_evidence_debug(
        config: DaemonConfig,
        model: ModelClient,
        fixture_evidence: FixtureEvidence,
        model_debug: bool,
        fixture_mode: bool,
    ) -> Result<Self> {
        let store = V2Store::open(&config.store_root)?;
        let active = ActiveResearchCatalogue::install(&store, Utc::now())?;
        let agent_catalogue = if fixture_mode {
            active.contracts.clone()
        } else {
            let candidate = active.install_analyst_freshness_candidate(&store, Utc::now())?;
            active.contracts.with_installed_candidate(candidate)?
        };
        let workflow =
            WorkflowRuntime::new(store.clone(), active.recipes).with_fixture_mode(fixture_mode);
        let (reasoning_events, _) = broadcast::channel(1_024);
        let agents = AgentRuntime::new(store.clone(), agent_catalogue, Duration::minutes(5))
            .with_reasoning_events(reasoning_events.clone());
        let decision_runtime = V2DecisionRuntime::new(store.clone(), Default::default())?;
        let execution_runtime =
            V2ExecutionRuntime::new(store.clone(), Default::default(), Default::default())?;
        let scheduler = PaperScheduler::new(
            store.clone(),
            workflow.clone(),
            format!("akzio-daemon-{}", RunId::new()),
        )?
        .with_market_data_feed(config.market_data_feed)
        .with_runtime_identity_hash(config.runtime_identity_hash.clone());

        Ok(Self {
            task_runtime: TaskRuntime::new(store.clone()),
            workflow,
            agents,
            model: ModelClientAdapter::with_debug(model, model_debug),
            stage_models: Arc::new(BTreeMap::new()),
            reasoning_events,
            fixture_evidence: Arc::new(fixture_evidence),
            fixture_mode,
            production_evidence: Arc::new(BTreeMap::new()),
            decision_runtime,
            execution_runtime,
            paper_commitment_runtime: V2PaperCommitmentRuntime::new(store.clone()),
            paper_dispatch_runtime: V2PaperDispatchRuntime::new(store.clone())
                .with_failpoint(PaperDispatchFailpoint::from_env()),
            outcome_scheduling_runtime: OutcomeSchedulingRuntime::new(store.clone()),
            paper: DaemonPaperState {
                paper_broker: None,
                paper_observer: None,
                scheduler,
                auto_paper: config.auto_paper,
                runtime_identity_hash: config.runtime_identity_hash,
                outcome_cost_model: config.outcome_cost_model,
            },
            transport: DaemonTransport {
                http_token: config.http_token,
                observer_token: config.observer_token,
                worker_pool: WorkerPoolConfig {
                    worker_count: config.worker_count.max(1),
                    ..WorkerPoolConfig::default()
                },
            },
            store,
        })
    }

    pub fn store(&self) -> &V2Store {
        &self.store
    }

    pub fn paper_workflow_source(&self) -> StorePaperWorkflowSource {
        StorePaperWorkflowSource::new(self.store.clone())
            .with_bootstrap(self.workflow.clone(), "active")
    }

    /// Install a broker only through dependency injection. Construction of the
    /// daemon itself never reads credentials or performs network I/O.
    pub fn with_paper_broker(mut self, broker: Arc<dyn CommittedPaperBroker>) -> Self {
        self.paper.paper_broker = Some(broker);
        self
    }

    pub fn with_paper_observer(mut self, paper: AlpacaPaper) -> Self {
        self.paper.paper_observer = Some(paper);
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
        Ok(self
            .paper
            .scheduler
            .reserve_session(session_key, proposal, now)?)
    }

    pub fn reserve_paper_session_with_inputs(
        &self,
        session_key: &str,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> Result<akzio_store::v2::SessionSlotReservation> {
        Ok(self.paper.scheduler.reserve_session_with_inputs(
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
        Ok(self.paper.scheduler.reserve_session_with_inputs_for_run(
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
        if !self.paper.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper scheduler requires auto_paper=true".to_owned(),
            ));
        }
        self.paper
            .scheduler
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
        if !self.paper.auto_paper {
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

    pub(super) fn request_cancel(&self, run_id: &RunId, reason: &str) -> Result<u64> {
        Ok(u64::from(self.task_runtime.request_cancel(
            run_id,
            reason,
            Utc::now(),
        )?))
    }

    pub(super) fn retry_run(&self, source_run_id: &RunId) -> Result<RunId> {
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

    pub(super) fn replay_report(&self, run_id: &RunId) -> Result<ReplayReport> {
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

    pub(super) fn retrospectives(&self, run_id: &RunId) -> Result<Vec<RetrospectiveView>> {
        let run_purpose = self.store.run_purpose(run_id)?;
        if !matches!(run_purpose, RunPurpose::Paper) {
            return Ok(Vec::new());
        }
        self.store
            .retrospectives(run_id)?
            .into_iter()
            .map(|artifact| {
                let payload: Retrospective =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                payload.validate().map_err(|error| {
                    DaemonError::InvalidInput(format!(
                        "invalid retrospective crossed query gate: {error}"
                    ))
                })?;
                Ok(RetrospectiveView {
                    artifact_id: artifact.artifact_id,
                    payload,
                })
            })
            .collect()
    }

    pub(super) fn trajectory(&self, run_id: &RunId) -> Result<Vec<TrajectoryEntry>> {
        Ok(self.store.trajectory(run_id)?)
    }

    pub(super) fn set_freeze(&self, frozen: bool, reason: String) -> Result<DaemonHealth> {
        self.store.write_freeze_state(frozen, reason, Utc::now())?;
        self.health()
    }

    pub async fn approve_paper(
        &self,
        request: PaperApprovalRequest,
    ) -> Result<PaperApprovalResponse> {
        request.identity.validate()?;
        if !self.paper.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper approval requires auto_paper=true".to_owned(),
            ));
        }
        let expected_identity_hash =
            self.paper.runtime_identity_hash.as_ref().ok_or_else(|| {
                DaemonError::InvalidInput(
                    "Paper approval requires the daemon runtime identity".to_owned(),
                )
            })?;
        if request.identity.identity_hash()? != *expected_identity_hash {
            return Err(DaemonError::InvalidInput(
                "Paper approval runtime identity does not match the running daemon".to_owned(),
            ));
        }
        let session = chrono::NaiveDate::parse_from_str(&request.session_key, "%Y-%m-%d")
            .map_err(|_| DaemonError::InvalidInput("session_key must be YYYY-MM-DD".to_owned()))?;
        if request.operator.trim().is_empty()
            || request.reason.trim().is_empty()
            || request.max_notional_usd_cents <= 0
            || request.valid_hours <= 0
            || request.valid_hours > 24 * 7
        {
            return Err(DaemonError::InvalidInput(
                "invalid Paper approval scope".to_owned(),
            ));
        }
        let execution_policy = ExecutionPolicy::default();
        let maximum_notional = MoneyMicros::from_usd_cents(request.max_notional_usd_cents);
        if maximum_notional.0 > execution_policy.max_new_notional.0 {
            return Err(DaemonError::InvalidInput(
                "approval max notional exceeds execution policy".to_owned(),
            ));
        }
        let paper = AlpacaPaper::from_env().map_err(|error| {
            DaemonError::Unavailable(format!("construct Paper broker for approval: {error}"))
        })?;
        let account = paper.account().await.map_err(|error| {
            DaemonError::Unavailable(format!("read Paper account for approval: {error}"))
        })?;
        let broker_account_id = account
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| DaemonError::InvalidInput("Paper account id missing".to_owned()))?
            .to_owned();
        let now = Utc::now();
        let identity = request.identity;
        let manifest_payload = RuntimeManifest {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            code_revision: identity.code_revision,
            cargo_lock_hash: identity.cargo_lock_hash,
            config_hash: identity.config_hash,
            provider_id: identity.provider_id,
            model_id: identity.model_id,
            prompt_hash: identity.prompt_hash,
            contract_hash: identity.contract_hash,
            topology_hash: identity.topology_hash,
            decision_policy_hash: identity.decision_policy_hash,
            execution_policy_hash: identity.execution_policy_hash,
            evaluation_policy_hash: identity.evaluation_policy_hash,
            market_data_feed: identity.market_data_feed,
            broker_account_id,
            maximum_notional,
            allowed_session_start: session,
            allowed_session_end: session,
            expires_at: now + Duration::hours(request.valid_hours),
            created_at: now,
        };
        let manifest_hash = manifest_payload.manifest_hash()?;
        let manifest = Artifact::new(
            ArtifactKind::RuntimeManifest,
            self.store.put_json(&manifest_payload)?,
            "runtime.manifest",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )?;
        let mut approval_payload = PaperLaunchApproval {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            operator_identity: request.operator,
            runtime_manifest: ArtifactRef {
                artifact_id: manifest.artifact_id.clone(),
                kind: ArtifactKind::RuntimeManifest,
            },
            runtime_manifest_hash: manifest_hash.clone(),
            scope: PaperApprovalScope::Canary,
            reason: request.reason,
            approved_at: now,
            expires_at: manifest_payload.expires_at,
            approval_hash: ContentHash::of_bytes(b"pending"),
        };
        approval_payload.approval_hash = approval_payload.unsigned_hash()?;
        let approval = Artifact::new(
            ArtifactKind::PaperLaunchApproval,
            self.store.put_json(&approval_payload)?,
            "operator.paper_approval",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![approval_payload.runtime_manifest.clone()],
            now,
        )?;
        self.store
            .write_paper_approval_binding(&manifest, &approval)?;
        Ok(PaperApprovalResponse {
            session_key: request.session_key,
            runtime_manifest_artifact_id: manifest.artifact_id,
            runtime_manifest_hash: manifest_hash,
            approval_artifact_id: approval.artifact_id,
            approval_hash: approval_payload.approval_hash,
            expires_at: approval_payload.expires_at,
        })
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
        if self.paper.auto_paper {
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
        WorkerPool::new(
            self.task_runtime.clone(),
            self.transport.worker_pool.clone(),
        )
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
        let metrics = self.store.metrics(Utc::now())?;
        Ok(DaemonHealth {
            status: if self.paper.auto_paper {
                "paper_scheduler_fail_closed".to_owned()
            } else {
                "ok".to_owned()
            },
            frozen,
            scheduler_owner: lease.as_ref().map(|lease| lease.owner_id.clone()),
            scheduler_epoch: lease.map(|lease| lease.epoch),
            alerts: metrics.alerts(),
            metrics,
        })
    }

    /// Readiness covers process configuration. Durable run/task failures stay
    /// visible in health and Observer data, but must not prevent inspection or
    /// recovery after a restart.
    pub fn ready(&self) -> Result<DaemonHealth> {
        let health = self.health()?;
        if self.paper.auto_paper && self.paper.paper_broker.is_none() {
            return Err(DaemonError::Unavailable(
                "Paper broker is not injected".to_owned(),
            ));
        }
        Ok(health)
    }

    pub fn canary_status(&self) -> Result<Option<akzio_store::v2::CanaryCampaignHead>> {
        Ok(self.store.active_canary_campaign()?)
    }

    pub fn stage_canary_campaign(
        &self,
        spec: akzio_domain::CanaryCampaignSpec,
    ) -> Result<akzio_store::v2::CanaryCampaignHead> {
        let now = Utc::now();
        let lease = self.paper.scheduler.active_lease(now)?;
        Ok(self.store.stage_canary_campaign(&lease, &spec, now)?)
    }

    pub fn resume_canary_campaign(
        &self,
        campaign_id: &ContentHash,
    ) -> Result<akzio_store::v2::CanaryCampaignHead> {
        let now = Utc::now();
        let _lease = self.paper.scheduler.active_lease(now)?;
        self.store
            .canary_campaign(campaign_id)?
            .ok_or_else(|| DaemonError::InvalidInput("canary campaign not found".to_owned()))
    }
}
