// 文件导读：bootstrap 负责把配置/模型能力/adapter 注入 Daemon，并构造 WorkflowRuntime、
// AgentRuntime、Decision/Execution/Outcome runtime 和 PaperScheduler。生产构造不自动写
// Policy、不启动 worker 或 broker I/O；fixture 构造只提供离线 adapter，不能作为 real
// LLM/Paper/fill/Outcome 证据。
// Rust 机制：trait object `Arc<dyn AsyncEvidenceAdapter>`/`Arc<dyn ...Broker>` 做依赖注入；
// `BTreeMap` 保存 role route；builder 方法按所有权返回 `Self`，`with_*` 链式配置不复制 CAS。

use super::*;

impl Daemon {
    /// Construct the local daemon with its production model adapter. Model
    /// credentials stay in local configuration and are never persisted.
    pub fn open(
        config: DaemonConfig,
        model_config: ModelConfig,
        model_capabilities: ModelCapabilityProbeSet,
    ) -> Result<Self> {
        // 参数按值进入以供构造过程移动到多个 runtime；先验证 feed/capability，再构造模型
        // adapter，失败会在 Store worker/scheduler 启动前返回。
        let debug = model_config.debug || config.debug_control.is_some();
        let auto_paper = config.auto_paper;
        let market_data_feed = config.market_data_feed;
        if auto_paper && market_data_feed.is_none() {
            return Err(DaemonError::InvalidInput(
                "auto_paper requires an explicit Alpaca market-data feed".to_owned(),
            ));
        }
        model_capabilities.validate_for_config(&model_config)?;
        let model = ModelClient::from_config(&model_config)?;
        let stage_models = model_config
            .routes
            .iter()
            .map(|(purpose, route)| {
                let route_config = model_config.for_route(route);
                let capability_snapshot = model_capabilities
                    .routes
                    .get(purpose)
                    .cloned()
                    .ok_or_else(|| {
                        ModelError::CapabilityProbe(format!(
                            "missing capability snapshot for route {purpose}"
                        ))
                    })?;
                Ok((
                    purpose.clone(),
                    ModelClientAdapter::with_response_language(
                        ModelClient::from_config(&route_config)?,
                        debug,
                        route_config.response_language,
                    )
                    .with_capability_snapshot(capability_snapshot),
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
        // 每条显式 route 都使用匹配的能力快照；模型 client 的构造只配置传输，不在这里
        // 调用 provider，后续 AgentRuntime 任务才会驱动实际模型 Future。
        daemon.model = ModelClientAdapter::with_response_language(
            model.clone(),
            debug,
            model_config.response_language.clone(),
        )
        .with_capability_snapshot(model_capabilities.default.clone());
        daemon.stage_models = Arc::new(stage_models);
        let mut production_evidence: BTreeMap<EvidenceSource, Arc<dyn AsyncEvidenceAdapter>> =
            BTreeMap::new();
        // Adapter 构造从环境读取凭据/端点；真正的 Alpaca/FRED/SEC/News I/O 延后至采集时。
        if let Ok(alpaca) = AlpacaPaperEvidenceTransport::from_env(market_data_feed) {
            production_evidence.insert(EvidenceSource::Alpaca, Arc::new(alpaca));
        }
        if let Ok(sec) = SecEdgarDirectTransport::from_env() {
            production_evidence.insert(EvidenceSource::SecEdgar, Arc::new(sec));
        }
        if let Ok(fred) = FredDirectTransport::from_env() {
            production_evidence.insert(EvidenceSource::Fred, Arc::new(fred));
        }
        // Capability probes are diagnostics, not a permanent veto based on one
        // search result. Actual acquisitions validate hosted search responses.
        let capability = model_capabilities
            .routes
            .get("evidence.news_web")
            .unwrap_or(&model_capabilities.default);
        daemon.news_web_status = capability.native_web_status.as_str().to_owned();
        production_evidence.insert(
            EvidenceSource::NewsWeb,
            akzio_ingest::configured_news_evidence_transport(&model_config)
                .map_err(akzio_ingest::EvidenceRuntimeError::from)?,
        );
        daemon.news_web_route =
            "resource_router:official_direct+native_web_model_review".to_owned();
        let outcome_worker_enabled =
            daemon.outcome_processing && production_evidence.contains_key(&EvidenceSource::Alpaca);
        if auto_paper && !production_evidence.contains_key(&EvidenceSource::Alpaca) {
            return Err(DaemonError::InvalidInput(
                "auto_paper requires Alpaca Paper evidence adapter".to_owned(),
            ));
        }
        if auto_paper && !production_evidence.contains_key(&EvidenceSource::Fred) {
            return Err(DaemonError::InvalidInput(
                "auto_paper requires FRED_API_KEY".to_owned(),
            ));
        }
        daemon.production_evidence = Arc::new(production_evidence);
        // 只有配置启用 Outcome 且构造出 Alpaca adapter 才接线 Outcome worker；以下
        // ensure 仅检查/补齐持久化待办，不代表 adapter 已取得有效市场数据或跨日评估通过。
        // Store 中任务持久化，不依赖新 T0 Run 再次发现。
        daemon.outcome_processing = outcome_worker_enabled;
        daemon.task_runtime = daemon
            .task_runtime
            .with_outcome_processing(outcome_worker_enabled);
        if outcome_worker_enabled {
            daemon.store.ensure_pending_outcome_workers(Utc::now())?;
        }
        // Persist future work even while processing is disabled. Enabling the
        // adapter later must not require another T0 Run to discover old outcomes.
        daemon.outcome_scheduling_runtime =
            OutcomeSchedulingRuntime::new(daemon.store.clone()).with_worker_enabled(true);
        Ok(daemon)
    }

    /// Injecting a model keeps fixture and production dispatch on the same
    /// Runtime path. It deliberately installs no evidence adapter: a missing
    /// adapter fails evidence work closed.
    pub fn with_model(config: DaemonConfig, model: ModelClient) -> Result<Self> {
        // 此测试/fixture 构造路径没有生产 evidence adapter；缺失来源会由 EvidenceRuntime
        // 显式失败，而不是隐式退回真实网络。
        Self::with_fixture_evidence(config, model, FixtureEvidence::new())
    }

    /// Install deterministic local fixture evidence for tests and replay only.
    /// This adapter has no HTTP or filesystem capability.
    pub fn with_fixture_evidence(
        config: DaemonConfig,
        model: ModelClient,
        fixture_evidence: FixtureEvidence,
    ) -> Result<Self> {
        // 显式注入 fixture map，并将 fixture_mode 设为 true，使活动 catalogue 不安装
        // freshness candidate；该实例仍走相同 Workflow/TaskRuntime 入口。
        Self::with_fixture_evidence_debug(config, model, fixture_evidence, false, true)
    }

    fn with_fixture_evidence_debug(
        config: DaemonConfig,
        model: ModelClient,
        fixture_evidence: FixtureEvidence,
        model_debug: bool,
        fixture_mode: bool,
    ) -> Result<Self> {
        // 初始化顺序是 validate budget → 打开/标记 Store → 安装当前 catalogue → 构造
        // Workflow/Agent/runtime/scheduler；中间 Store 错误向上传播，绝不启动后台任务。
        config.agent_budget.validate()?;
        let store = Store::open(&config.store_root)?;
        if config.debug_control.is_some() && config.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Debug Core requires auto_paper=false".into(),
            ));
        }
        store.configure_debug_environment(config.debug_control.is_some())?;
        let active = ActiveResearchCatalogue::install(&store, Utc::now())?;
        let agent_catalogue = if fixture_mode {
            active.contracts.clone()
        } else {
            let candidate = active.install_analyst_freshness_candidate(&store, Utc::now())?;
            active.contracts.with_installed_candidate(candidate)?
        };
        let workflow = WorkflowRuntime::new(store.clone(), active.recipes)
            .with_agent_budgets(&config.agent_budget)?
            .with_research_settings(&config.research_settings)?;
        let store_executor = StoreExecutor::new(store.clone());
        let (reasoning_events, _) = broadcast::channel(1_024);
        if config.historical_evaluation_condition.is_some()
            != config.model_knowledge_cutoff.is_some()
        {
            return Err(DaemonError::InvalidInput(
                "historical projection requires both condition and model knowledge cutoff"
                    .to_owned(),
            ));
        }
        let mut agents = AgentRuntime::new(store.clone(), agent_catalogue, Duration::minutes(5))
            .with_store_executor(store_executor.clone())
            .with_reasoning_events(reasoning_events.clone());
        if let (Some(condition), Some(cutoff)) = (
            config.historical_evaluation_condition,
            config.model_knowledge_cutoff,
        ) {
            agents = agents.with_historical_projection(condition, cutoff);
        }
        let decision_runtime = DecisionRuntime::new(store.clone(), config.decision_policy.clone())?;
        let execution_runtime =
            ExecutionRuntime::new(store.clone(), Default::default(), Default::default())?;
        let fixture_capabilities = model.capability_snapshot();
        let scheduler = PaperScheduler::new(
            store.clone(),
            workflow.clone(),
            format!("akzio-daemon-{}", RunId::new()),
        )?
        .with_store_executor(store_executor.clone())
        .with_market_data_feed(config.market_data_feed)
        .with_runtime_identity_hash(config.runtime_identity_hash.clone());

        Ok(Self {
            debug_control: config.debug_control.clone(),
            outcome_processing: config.outcome_processing,
            store_executor: store_executor.clone(),
            task_runtime: TaskRuntime::new(store.clone())
                .with_store_executor(store_executor)
                .with_outcome_processing(config.outcome_processing)
                .with_debug_identity(
                    config
                        .debug_control
                        .as_ref()
                        .map(|d| d.runtime_identity.clone()),
                ),
            workflow,
            agents,
            model: ModelClientAdapter::with_debug(model, model_debug),
            stage_models: Arc::new(BTreeMap::new()),
            news_web_status: fixture_capabilities.native_web_status.as_str().to_owned(),
            news_web_route: "fixture".to_owned(),
            reasoning_events,
            fixture_evidence: Arc::new(fixture_evidence),
            fixture_mode,
            production_evidence: Arc::new(BTreeMap::new()),
            decision_runtime,
            execution_runtime,
            paper_commitment_runtime: PaperCommitmentRuntime::new(store.clone()),
            paper_dispatch_runtime: PaperDispatchRuntime::new(store.clone())
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
                worker_pool: WorkerPoolConfig {
                    worker_count: config.worker_count.max(1),
                    ..WorkerPoolConfig::default()
                },
            },
            store,
        })
    }

    pub fn paper_workflow_source(&self) -> StorePaperWorkflowSource {
        // 每次按同一 StoreExecutor 查找 durable Paper proposal；只有 Store 缺少可用 proposal
        // 时才允许用当前 Rust workflow 构建受控 bootstrap。
        StorePaperWorkflowSource::new(self.store.clone())
            .with_store_executor(self.store_executor.clone())
            .with_bootstrap(self.workflow.clone(), "active")
    }

    /// Install a broker only through dependency injection. Construction of the
    /// daemon itself never reads credentials or performs network I/O.
    pub fn with_paper_broker(mut self, broker: Arc<dyn CommittedPaperBroker>) -> Self {
        // 调用方转移一个共享 Broker trait object；本方法只注入依赖，不进行 HTTP I/O。
        self.paper.paper_broker = Some(broker);
        self
    }

    pub fn with_paper_observer(mut self, paper: AlpacaPaper) -> Self {
        // observer 持有 Paper API client，用于读取时钟/账户等受控状态；方法本身不调用接口。
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
    ) -> Result<akzio_store::SessionSlotReservation> {
        // 只委托 lease-fenced reservation；返回已有/新建 slot 都不领取 graph 中的 task。
        Ok(self
            .paper
            .scheduler
            .reserve_session(session_key, proposal, now)?)
    }
}
