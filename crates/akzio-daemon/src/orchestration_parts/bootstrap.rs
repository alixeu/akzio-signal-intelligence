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
            model_native_web_evidence_transport(model.clone(), EvidenceSource::NewsWeb),
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

    #[cfg(test)]
    pub(crate) fn store(&self) -> &V2Store {
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
}
