use akzio_execution::DecisionPolicyArtifact;

fn configured_synthesizer_identity(model: &OpenAIResponsesConfig) -> Result<(String, ContentHash)> {
    // 解析 research.synthesizer 的有效路由，并把 provider、端点、模型及版本语义字段
    // 固化成哈希；这是身份比较输入，不会调用模型或改变 Store。
    let route = model.routes.get("research.synthesizer");
    let model_id = route
        .map(|route| route.model.clone())
        .unwrap_or_else(|| model.model.clone());
    let version_hash = content_hash_json(&serde_json::json!({
        "provider": model.provider_identity().as_str(),
        "base_url": model.base_url.trim_end_matches('/'),
        "model": model_id,
        "release_date": route.and_then(|route| route.release_date.as_ref()).or(model.release_date.as_ref()),
        "knowledge_cutoff": route.and_then(|route| route.knowledge_cutoff.as_ref()).or(model.knowledge_cutoff.as_ref()),
        "reasoning_effort": route.map(|route| route.reasoning_effort.as_str()).unwrap_or(model.reasoning_effort.as_str()),
        "response_language": route.and_then(|route| route.response_language.as_deref()).unwrap_or(model.response_language.as_str()),
    }))?;
    Ok((model_id, version_hash))
}

#[derive(Debug, Clone)]
struct LoadedDecisionPolicy {
    policy: DecisionPolicy,
    status: String,
    input_hash: Option<ContentHash>,
    contract_hash: Option<ContentHash>,
    artifact_id: Option<ArtifactId>,
    source: &'static str,
}

fn load_decision_policy_from_config(
    config: &Config,
) -> Result<LoadedDecisionPolicy> {
    // 配置只提供 Store Root；Policy 正文和 active head 仍由统一 Store 读取，
    // 不再从外部 JSON 或旧 decision_policy_path 导入。
    let store = Store::open(&config.daemon.store_root)?;
    load_decision_policy_from_store(config, &store)
}

fn load_decision_policy_from_store(
    config: &Config,
    store: &Store,
) -> Result<LoadedDecisionPolicy> {
    // 只读取 SQL active head 指向的不可变 CAS Artifact，并用 descriptor 与正文交叉校验；
    // 缺少 active head 时返回显式的默认/未配置投影，而不是把默认 Policy 当成已校准。
    if let Some(stored) = store.active_decision_policy()? {
        let bytes = store.read_blob(&stored.artifact.blob)?;
        let loaded = decode_loaded_decision_policy(config, &bytes, Some(&stored.descriptor))?;
        return Ok(LoadedDecisionPolicy {
            artifact_id: Some(stored.artifact.artifact_id),
            source: "store_active_head",
            ..loaded
        });
    }
    Ok(LoadedDecisionPolicy {
        policy: DecisionPolicy::default(),
        status: "store_active_head_missing".to_owned(),
        input_hash: None,
        contract_hash: None,
        artifact_id: None,
        source: "store_active_head_missing",
    })
}

fn decode_loaded_decision_policy(
    config: &Config,
    bytes: &[u8],
    descriptor: Option<&akzio_store::DecisionPolicyDescriptor>,
) -> Result<LoadedDecisionPolicy> {
    // 解码边界同时检查 CAS envelope、Policy provenance、provider/route 和当前配置模型；
    // 成功只返回内存中的 LoadedDecisionPolicy，激活仍必须走独立的 activate 路径。
    let artifact = DecisionPolicyArtifact::decode_strict(bytes)
        .context("decode frozen decision policy")?;
    if let Some(descriptor) = descriptor {
        if descriptor.envelope_hash != ContentHash::of_bytes(bytes)
            || descriptor.policy_hash != artifact.provenance.output_hash
            || artifact.provenance.provider_id.as_deref() != Some(&descriptor.provider_id)
            || artifact.provenance.model_route.as_deref() != Some(&descriptor.model_route)
            || artifact.provenance.contract_hash.as_ref() != Some(&descriptor.contract_hash)
        {
            bail!("active Store policy identity does not match its CAS envelope");
        }
    }
    let input_hash = ContentHash::of_bytes(bytes);
    let policy = artifact.policy;
    // Policy 自身的结构校验先于任何能力判断，避免用部分有效的校准字段进入运行时。
    policy
        .validate()
        .context("validate frozen decision policy")?;
    if let (Some(descriptor), Some(scope)) = (descriptor, &policy.active_forecast_calibration) {
        if descriptor.model_id != scope.model_id
            || descriptor.model_version_hash != scope.model_version_hash
        {
            bail!("active Store policy model identity does not match its CAS envelope");
        }
    }

    let provider_id = artifact
        .provenance
        .provider_id
        .as_deref()
        .context("frozen decision policy has no provider identity")?;
    if provider_id != OPENAI_RESPONSES_PROVIDER_ID {
        bail!("frozen decision policy provider identity does not match OpenAI Responses");
    }
    let model_route = artifact
        .provenance
        .model_route
        .as_deref()
        .context("frozen decision policy has no model route identity")?;
    if model_route != "research.synthesizer" {
        bail!("frozen decision policy must be scoped to research.synthesizer");
    }

    if let Some(scope) = &policy.active_forecast_calibration {
        // 带 forecast calibration 的 Policy 必须绑定当前配置的 Synthesizer 身份，
        // 否则历史模型的风险参数不能被当前模型复用。
        let model = config
            .model
            .as_ref()
            .context("active forecast calibration requires [model] configuration")?;
        let (model_id, model_version_hash) = configured_synthesizer_identity(model)?;
        if scope.model_id != model_id || scope.model_version_hash != model_version_hash {
            bail!(
                "frozen decision policy model/version does not match the configured research.synthesizer route"
            );
        }
    }
    let status = decision_policy_status(&policy);
    Ok(LoadedDecisionPolicy {
        policy,
        status: status.to_owned(),
        input_hash: Some(input_hash),
        contract_hash: artifact.provenance.contract_hash,
        artifact_id: None,
        source: "decoded",
    })
}

fn activate_decision_policy_artifact(
    store: &Store,
    artifact: &Artifact,
    now: DateTime<Utc>,
) -> Result<akzio_store::StoredDecisionPolicy> {
    // 将严格解码后的 Policy provenance 映射为 Store descriptor，再由 Store 原子地更新
    // active head；此处不会替 Policy 绕过 Contract 检查，调用方需先完成 Contract 比对。
    let bytes = store.read_blob(&artifact.blob)?;
    let envelope = DecisionPolicyArtifact::decode_strict(&bytes)
        .context("strictly decode DecisionPolicyArtifact before Store activation")?;
    if !envelope.policy.decision_capable() {
        bail!("refusing to activate a policy that is not decision-capable");
    }
    let scope = envelope.policy.active_forecast_calibration.as_ref()
        .context("decision-capable policy has no active forecast calibration")?;
    let provider_id = envelope.provenance.provider_id.clone()
        .context("decision policy provider identity missing")?;
    let model_route = envelope.provenance.model_route.clone()
        .context("decision policy model route missing")?;
    let contract_hash = envelope.provenance.contract_hash.clone()
        .context("decision policy Contract identity missing")?;
    let descriptor = akzio_store::DecisionPolicyDescriptor {
        policy_hash: envelope.provenance.output_hash.clone(),
        envelope_hash: artifact.blob.hash.clone(), provider_id,
        model_id: scope.model_id.clone(), model_version_hash: scope.model_version_hash.clone(),
        model_route, contract_hash,
    };
    Ok(store.activate_decision_policy(artifact, &descriptor, now)?)
}

fn decision_policy_audit(loaded: &LoadedDecisionPolicy) -> (String, Option<ContentHash>) {
    // 启动器使用该轻量投影记录 Policy 状态和输入哈希；它不改变 loaded Policy，也不做
    // active head 写入。
    (loaded.status.clone(), loaded.input_hash.clone())
}

fn runtime_identity_from_config(
    config: &Config,
    config_path: &Path,
    model_capabilities: &ModelCapabilityProbeSet,
) -> Result<RuntimeIdentity> {
    // 默认从 Store active head 加载 Policy，再把统一身份计算委托给带显式 Policy 的版本。
    let loaded = load_decision_policy_from_config(config)?;
    runtime_identity_from_config_with_policy(
        config,
        config_path,
        model_capabilities,
        &loaded.policy,
    )
}

fn runtime_identity_from_config_with_policy(
    config: &Config,
    config_path: &Path,
    model_capabilities: &ModelCapabilityProbeSet,
    decision_policy: &DecisionPolicy,
) -> Result<RuntimeIdentity> {
    // RuntimeIdentity 汇总源码、锁文件、配置、模型能力、Contract/Prompt/拓扑和治理哈希，
    // 供 Paper approval/Debug 启动绑定；它描述启动输入，不等于审批、Decision 或订单。
    let model = config
        .model
        .as_ref()
        .context("missing [model] configuration")?;
    let feed = config
        .execution
        .market_data_feed
        .context("Paper runtime requires execution.market_data_feed")?;
    let provider_id = model.provider_identity().as_str().to_owned();
    let policy_identity = runtime_policy_identity(decision_policy)?;
    // 每个角色的能力快照分别入 hash，再生成 bundle hash，避免只记录默认路由而遗漏覆盖。
    model_capabilities.validate_for_config(model)?;
    let mut model_capability_hashes = BTreeMap::from([(
        "default".to_owned(),
        content_hash_json(&serde_json::to_value(&model_capabilities.default)?)?,
    )]);
    for (purpose, snapshot) in &model_capabilities.routes {
        model_capability_hashes.insert(
            purpose.clone(),
            content_hash_json(&serde_json::to_value(snapshot)?)?,
        );
    }
    let model_capability_bundle_hash =
        content_hash_json(&serde_json::to_value(&model_capability_hashes)?)?;
    let cost_model = OutcomeCostModel {
        transaction_cost_ppm: config.execution.transaction_cost_ppm,
        slippage_ppm: config.execution.slippage_ppm,
    };
    let governance = runtime_governance_identity(&cost_model, &policy_identity)?;
    Ok(RuntimeIdentity {
        code_revision: source_revision()?,
        cargo_lock_hash: ContentHash::of_bytes(include_bytes!("../../../../Cargo.lock")),
        config_hash: content_hash_json(&serde_json::json!({
            "config_file_hash": redacted_config_hash(config_path)?,
            "daemon": {
                "http_addr": config.daemon.http_addr.to_string(),
                "worker_count": config.daemon.worker_count,
                "auto_paper": config.daemon.auto_paper,
                "manual_paper": config.daemon.manual_paper,
            },
            "execution": {
                "experiment_profile": config.execution.experiment_profile,
                "historical_evaluation_condition": config.execution.historical_evaluation_condition,
                "assets": config.execution.assets,
                "market_data_feed": config.execution.market_data_feed,
                "transaction_cost_ppm": config.execution.transaction_cost_ppm,
                "slippage_ppm": config.execution.slippage_ppm,
            },
            "model": {
                "provider": model.provider_identity().as_str(),
                "base_url": model.base_url,
                "model": model.model,
                "release_date": model.release_date,
                "knowledge_cutoff": model.knowledge_cutoff,
                "reasoning_effort": model.reasoning_effort,
                "response_language": model.response_language,
                "debug": model.debug,
                "routes": model.routes,
            },
        }))?,
        provider_id,
        model_id: model.model.clone(),
        prompt_hash: prompt_component_hash(),
        contract_hash: contract_component_hash(),
        topology_hash: topology_component_hash(),
        decision_policy_hash: policy_identity.decision_policy_hash,
        execution_policy_hash: policy_identity.execution_policy_hash,
        evaluation_policy_hash: policy_identity.evaluation_policy_hash,
        experiment_profile: config.execution.experiment_profile.as_str().to_owned(),
        rust_toolchain: env!("AKZIO_RUSTC_VERSION").to_owned(),
        model_release_date: model
            .release_date
            .as_deref()
            .map(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d"))
            .transpose()
            .context("parse model.release_date")?,
        model_knowledge_cutoff: model
            .knowledge_cutoff
            .as_deref()
            .map(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d"))
            .transpose()
            .context("parse model.knowledge_cutoff")?,
        historical_evaluation_condition: match config.execution.experiment_profile {
            ExperimentProfile::PaperResearch => Some(ExperimentCondition::PostCutoffForward),
            ExperimentProfile::HistoricalEval => config.execution.historical_evaluation_condition,
            ExperimentProfile::Fixture | ExperimentProfile::PaperEngineering => None,
        },
        model_routes_hash: Some(content_hash_json(&serde_json::json!({
            "default": {
                "model": model.model,
                "release_date": model.release_date,
                "knowledge_cutoff": model.knowledge_cutoff,
                "reasoning_effort": model.reasoning_effort,
                "response_language": model.response_language,
            },
            "routes": model.routes,
        }))?),
        model_capability_hashes,
        model_capability_bundle_hash,
        governance_component_hashes: governance.component_hashes,
        governance_bundle_hash: governance.bundle_hash,
        market_data_feed: feed.as_str().to_owned(),
    })
}

fn redacted_config_hash(config_path: &Path) -> Result<ContentHash> {
    // 配置身份保留行为字段，但先删除 credentials 和 model.api_key；哈希用于身份绑定，
    // 不把敏感值写入 RuntimeIdentity。
    let mut document = read_config_document(config_path)?;
    if let Some(root) = document.as_table_mut() {
        root.remove("credentials");
        if let Some(model) = root.get_mut("model").and_then(toml::Value::as_table_mut) {
            model.remove("api_key");
        }
    }
    Ok(ContentHash::of_bytes(
        toml::to_string(&document)
            .context("serialize redacted config TOML")?
            .as_bytes(),
    ))
}

fn source_revision() -> Result<String> {
    // 读取 build.rs 写入的编译期源码身份；缺少该环境变量时由编译阶段直接失败。
    Ok(env!("AKZIO_SOURCE_REVISION").to_owned())
}

fn read_config_file(path: &Path) -> Result<Config> {
    // 读取并反序列化完整配置；环境变量替换和运行时约束由调用方按入口需要执行。
    fs::read_to_string(path)
        .with_context(|| format!("read config {}", path.display()))
        .and_then(|text| toml::from_str::<Config>(&text).context("parse config TOML"))
}

fn read_config_document(path: &Path) -> Result<toml::Value> {
    // 保留 TOML 通用树以支持 Observatory 的局部编辑；这里不执行 Config 级业务校验。
    fs::read_to_string(path)
        .with_context(|| format!("read config {}", path.display()))
        .and_then(|text| toml::from_str::<toml::Value>(&text).context("parse config TOML"))
}

fn write_config_file(path: &Path, document: &toml::Value) -> Result<()> {
    // 将配置先写入同目录临时文件并设置最小权限，再 rename 到目标；这条路径只更新
    // 本地配置，不触碰 Store、Run 或 Paper 状态。
    let parent = path
        .parent()
        .context("Akzio configuration path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create Akzio configuration directory {}", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).with_context(|| {
            format!("secure Akzio configuration directory {}", parent.display())
        })?;
    }

    let temporary = path.with_extension("toml.tmp");
    let rendered = toml::to_string_pretty(document).context("serialize config TOML")?;
    fs::write(&temporary, rendered)
        .with_context(|| format!("write Akzio configuration {}", temporary.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("secure Akzio configuration {}", temporary.display()))?;
    }
    fs::rename(&temporary, path)
        .with_context(|| format!("install Akzio configuration {}", path.display()))?;
    Ok(())
}

fn toml_section_mut<'a>(
    document: &'a mut toml::Value,
    name: &str,
) -> Result<&'a mut toml::map::Map<String, toml::Value>> {
    // 获取或创建指定顶层 TOML table；若现有值不是 table，立即报错而不覆盖用户配置。
    let root = document
        .as_table_mut()
        .context("Akzio configuration root must be a TOML table")?;
    root.entry(name.to_owned())
        .or_insert_with(|| toml::Value::Table(toml::map::Map::new()))
        .as_table_mut()
        .with_context(|| format!("Akzio configuration [{name}] must be a TOML table"))
}

fn set_optional_toml_string(
    table: &mut toml::map::Map<String, toml::Value>,
    key: &str,
    value: Option<String>,
) {
    // 空白或空字符串按“未设置”处理并删除旧键；非空值才写回 TOML。
    if let Some(value) = value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        table.insert(key.to_owned(), toml::Value::String(value));
    } else {
        table.remove(key);
    }
}

fn validate_model_settings(model: &OpenAIResponsesConfig) -> Result<()> {
    // Observatory 编辑入口只校验模型基础字段、日期格式和受支持路由；执行 profile、
    // Paper 成本和资产约束仍由 load_config 负责，不能把此校验当作可运行性证明。
    if model.base_url.trim().is_empty()
        || model.model.trim().is_empty()
        || model.reasoning_effort.trim().is_empty()
        || model.response_language.trim().is_empty()
    {
        bail!("model base_url, model, reasoning_effort, and response_language must be non-empty");
    }
    for (field, date) in [
        ("release_date", model.release_date.as_deref()),
        ("knowledge_cutoff", model.knowledge_cutoff.as_deref()),
    ] {
        if let Some(date) = date {
            NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .with_context(|| format!("model.{field} must be YYYY-MM-DD, got {date}"))?;
        }
    }
    for (purpose, route) in &model.routes {
        if !matches!(
            purpose.as_str(),
            "research.analyst"
                | "research.critic"
                | "research.synthesizer"
                | "learning.outcome_worker"
                | "evidence.news_web"
        ) {
            bail!("unsupported model route {purpose}");
        }
        if route.model.trim().is_empty() || route.reasoning_effort.trim().is_empty() {
            bail!("model route {purpose} contains an empty value");
        }
        if route
            .response_language
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            bail!("model route {purpose} contains empty response_language");
        }
        for (field, date) in [
            ("release_date", route.release_date.as_deref()),
            ("knowledge_cutoff", route.knowledge_cutoff.as_deref()),
        ] {
            if let Some(date) = date {
                NaiveDate::parse_from_str(date, "%Y-%m-%d").with_context(|| {
                    format!("model route {purpose}.{field} must be YYYY-MM-DD, got {date}")
                })?;
            }
        }
    }
    Ok(())
}

const CANONICAL_RESEARCH_ROUTES: [&str; 3] = [
    "research.analyst",
    "research.critic",
    "research.synthesizer",
];

fn validate_canonical_model_identity(model: &OpenAIResponsesConfig) -> Result<()> {
    // PaperResearch/HistoricalEval 需要模型发布日期和知识截止日，以便构造可审计身份和
    // 历史时间语义；这里只检查字段存在，具体日期格式由 validate_model_settings 负责。
    if model.release_date.is_none() || model.knowledge_cutoff.is_none() {
        bail!("canonical research profiles require model.release_date and model.knowledge_cutoff");
    }
    Ok(())
}

fn validate_canonical_research_routes(profile: &str, model: &OpenAIResponsesConfig) -> Result<()> {
    // 正式研究拓扑要求 Analyst/Critic/Synthesizer 都有显式路由，并且每条路由能继承或
    // 覆盖 release_date/knowledge_cutoff；缺任一角色都在启动前失败。
    for purpose in CANONICAL_RESEARCH_ROUTES {
        let route = model
            .routes
            .get(purpose)
            .with_context(|| format!("{profile} requires an explicit {purpose} model route"))?;
        if route
            .release_date
            .as_ref()
            .or(model.release_date.as_ref())
            .is_none()
            || route
                .knowledge_cutoff
                .as_ref()
                .or(model.knowledge_cutoff.as_ref())
                .is_none()
        {
            bail!("{profile} route {purpose} requires effective release_date and knowledge_cutoff");
        }
    }
    Ok(())
}

fn initial_config_value(value: &str) -> String {
    // 初始化模板只展开形如 $ENV 的整值占位符；未设置的环境变量变为空字符串，
    // 其他普通字符串原样保留，不做任意模板求值。
    value
        .strip_prefix('$')
        .and_then(|name| std::env::var(name).ok())
        .unwrap_or_else(|| {
            if value.starts_with('$') {
                String::new()
            } else {
                value.to_owned()
            }
        })
}

fn apply_config_environment(config: &Config) {
    // 仅在进程环境尚未提供时注入 SEC user agent，避免配置文件覆盖调用方显式环境值。
    if let Some(value) = config.observatory.sec_user_agent.as_deref() {
        if std::env::var_os("SEC_USER_AGENT").is_none() && !value.is_empty() {
            std::env::set_var("SEC_USER_AGENT", value);
        }
    }
}

fn load_config(path: &Path) -> Result<Config> {
    // 这是常规 daemon/远端命令的完整入口：读取预算、模型和凭据，应用环境覆盖，
    // 再校验 loopback、四资产、成本、市场数据和 experiment profile；成功只返回可启动
    // 配置，不代表 daemon 已启动或 Paper 订单已获准。
    let mut config = read_config_file(path)?;
    config
        .agent
        .budget
        .validate()
        .context("invalid agent.budget configuration")?;
    config.agent.research.validate().context("invalid agent.research configuration")?;
    resolve_model_configuration(&mut config)?;
    for (name, value) in [("ALPACA_API_KEY", &config.credentials.alpaca_api_key),
        ("ALPACA_API_SECRET", &config.credentials.alpaca_api_secret)] {
        if !value.is_empty() { std::env::set_var(name, resolve_env_placeholder(value, name)?); }
    }
    if let Some(value) = &config.credentials.fred_api_key {
        if !value.is_empty() { std::env::set_var("FRED_API_KEY", resolve_env_placeholder(value, "FRED_API_KEY")?); }
    }
    if let Some(store_root) = std::env::var_os("AKZIO_STORE_ROOT") {
        // AKZIO_STORE_ROOT 是显式运行时覆盖，优先于文件中的路径，但仍受后续 Store
        // 生命周期和 Debug 隔离规则约束。
        config.daemon.store_root = PathBuf::from(store_root);
    }
    apply_config_environment(&config);
    if !config.daemon.http_addr.ip().is_loopback() {
        bail!("daemon.http_addr must be a loopback address");
    }
    if config.daemon.worker_count == Some(0) {
        bail!("daemon.worker_count must be greater than zero");
    }

    let expected = Asset::EXECUTABLE.into_iter().collect::<BTreeSet<_>>();
    let actual = config
        .execution
        .assets
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if actual != expected || config.execution.assets.len() != expected.len() {
        bail!("execution.assets must contain exactly TQQQ, QQQ, SOXX, SOXL");
    }
    OutcomeCostModel {
        transaction_cost_ppm: config.execution.transaction_cost_ppm,
        slippage_ppm: config.execution.slippage_ppm,
    }
    .validate()
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let auto_paper = config.daemon.auto_paper.unwrap_or(false) || config.daemon.manual_paper;
    let zero_cost =
        config.execution.transaction_cost_ppm == 0 && config.execution.slippage_ppm == 0;

    if auto_paper
        && config.execution.transaction_cost_ppm == 0
        && config.execution.slippage_ppm == 0
    {
        // Scheduler 必须有非零成本假设；零成本只允许显式 fixture 等不启动 Paper 的配置。
        bail!("Paper scheduler requires explicit transaction_cost_ppm or slippage_ppm");
    }
    if auto_paper && config.execution.market_data_feed.is_none() {
        bail!("Paper scheduler requires execution.market_data_feed");
    }
    if auto_paper
        && matches!(
            config.execution.experiment_profile,
            ExperimentProfile::Fixture | ExperimentProfile::HistoricalEval
        )
    {
        bail!("Paper scheduler requires a paper-engineering or paper-research experiment profile");
    }
    if matches!(
        config.execution.experiment_profile,
        ExperimentProfile::PaperEngineering
            | ExperimentProfile::PaperResearch
            | ExperimentProfile::HistoricalEval
    ) && zero_cost
    {
        bail!("non-fixture experiment profiles require nonzero cost assumptions");
    }
    if matches!(
        config.execution.experiment_profile,
        ExperimentProfile::PaperEngineering | ExperimentProfile::PaperResearch
    ) && config.execution.market_data_feed.is_none()
    {
        bail!("Paper experiment profiles require execution.market_data_feed");
    }
    if matches!(
        config.execution.experiment_profile,
        ExperimentProfile::PaperResearch | ExperimentProfile::HistoricalEval
    ) {
        if config.execution.market_data_feed != Some(AlpacaMarketDataFeed::Sip) {
            bail!("canonical research profiles require SIP market data");
        }
        let model = config
            .model
            .as_ref()
            .context("canonical research profiles require [model] configuration")?;
        validate_canonical_model_identity(model)?;
    }

    match config.execution.experiment_profile {
        ExperimentProfile::PaperResearch => {
            if config.execution.historical_evaluation_condition.is_some() {
                bail!(
                    "paper-research derives post_cutoff_forward automatically and does not accept execution.historical_evaluation_condition"
                );
            }
            let model = config
                .model
                .as_ref()
                .context("paper-research requires [model] configuration")?;
            validate_canonical_research_routes("paper-research", model)?;
        }
        ExperimentProfile::HistoricalEval => {
            let condition = config
                .execution
                .historical_evaluation_condition
                .context("historical-eval requires execution.historical_evaluation_condition")?;
            if condition == ExperimentCondition::PostCutoffForward {
                bail!(
                    "historical-eval requires bright, identifier_masked, calendar_masked, or fully_masked; post_cutoff_forward is reserved for paper-research"
                );
            }
            let model = config
                .model
                .as_ref()
                .context("historical-eval requires [model] configuration")?;
            validate_canonical_research_routes("historical-eval", model)?;
        }
        ExperimentProfile::Fixture | ExperimentProfile::PaperEngineering => {
            if config.execution.historical_evaluation_condition.is_some() {
                bail!(
                    "execution.historical_evaluation_condition is only valid for historical-eval"
                );
            }
        }
    }
    Ok(config)
}

#[cfg(test)]
mod agent_budget_config_tests {

    #[test]
    fn proposal_revision_toml_default_zero_and_invalid_values() {
        let absent: akzio_domain::AgentSettings = toml::from_str("").unwrap();
        assert_eq!(absent.research.max_proposal_revisions, 2);
        for limit in [0, 2, 5, 6] {
            let settings: akzio_domain::AgentSettings = toml::from_str(&format!("[research]\nmax_proposal_revisions = {limit}\n")).unwrap();
            assert_eq!(settings.research.validate().is_ok(), limit <= 5);
        }
        for value in ["-1", "1.5", "true", "256"] {
            assert!(toml::from_str::<akzio_domain::AgentSettings>(&format!("[research]\nmax_proposal_revisions = {value}\n")).is_err());
        }
    }

    #[test]
    fn budget_toml_defaults_role_override_and_million_input() {
        let absent: akzio_domain::AgentSettings = toml::from_str("").unwrap();
        for (purpose, output, timeout) in [
            ("research.analyst", 1_000_000, 180),
            ("research.critic", 1_000_000, 180),
            ("research.synthesizer", 1_000_000, 180),
            ("learning.outcome_worker", 4000, 180),
        ] {
            assert_eq!(
                absent.budget.resolve(purpose).unwrap(),
                akzio_domain::TaskBudget {
                    max_input_tokens: 1_000_000,
                    max_output_tokens: output,
                    max_tool_calls: akzio_domain::budget::ToolCallLimit::Unlimited,
                    max_wall_time_secs: timeout,
                }
            );
        }
        let root: super::Config = toml::from_str(
            r#"
[daemon]
store_root = ".akzio/budget-config-test"
http_addr = "127.0.0.1:17342"
[execution]
assets = ["TQQQ", "QQQ", "SOXX", "SOXL"]
[agent.budget.analyst]
max_input_tokens = 1000000
max_output_tokens = 1000000
"#,
        )
        .unwrap();
        root.agent.budget.validate().unwrap();
        assert_eq!(
            root.agent
                .budget
                .resolve("research.analyst")
                .unwrap()
                .max_input_tokens,
            1_000_000
        );
        assert_eq!(
            root.agent
                .budget
                .resolve("research.analyst")
                .unwrap()
                .max_output_tokens,
            1_000_000
        );
        let config: akzio_domain::AgentSettings = toml::from_str(
            r#"
[budget.default]
max_output_tokens = 7000
[budget.analyst]
max_input_tokens = 1000000
max_output_tokens = 12000
max_tool_calls = 8
timeout_seconds = 180
"#,
        )
        .unwrap();
        config.budget.validate().unwrap();
        let analyst = config.budget.resolve("research.analyst").unwrap();
        assert_eq!(
            analyst,
            akzio_domain::TaskBudget {
                max_input_tokens: 1_000_000,
                max_output_tokens: 12000,
                max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(8),
                max_wall_time_secs: 180,
            }
        );
        let critic = config.budget.resolve("research.critic").unwrap();
        assert_eq!(critic.max_input_tokens, 1_000_000);
        assert_eq!(critic.max_output_tokens, 7000);
        assert_eq!(
            critic.max_tool_calls,
            akzio_domain::budget::ToolCallLimit::Unlimited
        );
        assert_eq!(critic.max_wall_time_secs, 180);
    }

    #[test]
    fn role_can_select_unlimited_or_a_finite_tool_limit() {
        use akzio_domain::budget::ToolCallLimit;
        let settings: akzio_domain::AgentSettings = toml::from_str(
            r#"
[budget.default]
max_tool_calls = 4
[budget.analyst]
max_tool_calls = "unlimited"
[budget.synthesizer]
max_tool_calls = 0
"#,
        )
        .unwrap();
        settings.budget.validate().unwrap();
        assert_eq!(
            settings
                .budget
                .resolve("research.analyst")
                .unwrap()
                .max_tool_calls,
            ToolCallLimit::Unlimited
        );
        assert_eq!(
            settings
                .budget
                .resolve("research.critic")
                .unwrap()
                .max_tool_calls,
            ToolCallLimit::Limited(4)
        );
        assert_eq!(
            settings
                .budget
                .resolve("research.synthesizer")
                .unwrap()
                .max_tool_calls,
            ToolCallLimit::Limited(0)
        );
        let defaults: akzio_domain::AgentSettings = toml::from_str(
            r#"[budget.default]
max_input_tokens = 1000000
max_tool_calls = "unlimited"
"#,
        )
        .unwrap();
        assert_eq!(
            defaults.budget.resolved(),
            akzio_domain::AgentBudgetConfig::default().resolved()
        );
    }

    #[test]
    fn invalid_agent_budgets_fail_before_startup() {
        for entry in [
            "max_input_tokens = -1",
            "max_input_tokens = 4294967296",
            "max_tool_calls = 65536",
            "max_tool_calls = -1",
            "max_tool_calls = 'infinite'",
            "timeout_seconds = -1",
            "max_output_tokens = 1.5",
            "max_input_tokens = 'many'",
            "unknown_limit = 12",
        ] {
            assert!(
                toml::from_str::<akzio_domain::AgentSettings>(&format!(
                    "[budget.analyst]\n{entry}"
                ))
                .is_err(),
                "{entry}"
            );
        }
        for entry in [
            "max_input_tokens = 0",
            "max_output_tokens = 0",
            "max_output_tokens = 1000001",
            "timeout_seconds = 0",
        ] {
            let settings: akzio_domain::AgentSettings =
                toml::from_str(&format!("[budget.default]\n{entry}")).unwrap();
            assert!(settings.budget.validate().is_err(), "{entry}");
        }
        assert!(toml::from_str::<akzio_domain::AgentSettings>(
            "[budget.gpt_model]\nmax_input_tokens=1000"
        )
        .is_err());
    }
}

// Shared by daemon loading and real provider tests; execution configuration
// validation remains in load_config and is never weakened by model-only tests.
fn resolve_model_configuration(config: &mut Config) -> Result<()> {
    // 只解析模型相关的环境占位符和 AKZIO_* 覆盖，并检查路由字段非空/名称合法；
    // 该轻量入口供 preflight、校准身份和真实测试使用，完整执行配置仍须经过 load_config。
    if let Some(model) = config.model.as_mut() {
        model.base_url = resolve_env_placeholder(&model.base_url, "model.base_url")?;
        model.api_key = resolve_env_placeholder(&model.api_key, "model.api_key")?;
        if let Ok(value) = std::env::var("AKZIO_MODEL") {
            model.model = value;
        }
        if let Ok(value) = std::env::var("AKZIO_REASONING_EFFORT") {
            model.reasoning_effort = value;
        }
        if let Ok(value) = std::env::var("AKZIO_RESPONSE_LANGUAGE") {
            model.response_language = value;
        }
        if let Ok(value) = std::env::var("AKZIO_MODEL_ROUTES_JSON") {
            model.routes = serde_json::from_str(&value).context("parse AKZIO_MODEL_ROUTES_JSON")?;
        }
        if model.model.trim().is_empty()
            || model.reasoning_effort.trim().is_empty()
            || model.response_language.trim().is_empty()
        {
            bail!("model, reasoning_effort, and response_language must be non-empty");
        }
        for (purpose, route) in &model.routes {
            if !matches!(
                purpose.as_str(),
                "research.analyst"
                    | "research.critic"
                    | "research.synthesizer"
                    | "learning.outcome_worker"
                    | "evidence.news_web"
            ) {
                bail!("unsupported model route {purpose}");
            }
            if route.model.trim().is_empty() || route.reasoning_effort.trim().is_empty() {
                bail!("model route {purpose} contains an empty value");
            }
            if route
                .response_language
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
            {
                bail!("model route {purpose} contains an empty response_language");
            }
        }
    }
    Ok(())
}
