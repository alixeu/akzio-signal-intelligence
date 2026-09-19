use akzio_execution::DecisionPolicyArtifact;

fn configured_synthesizer_identity(model: &OpenAIResponsesConfig) -> Result<(String, ContentHash)> {
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
}

fn load_decision_policy_from_config(
    config: &Config,
    config_path: &Path,
) -> Result<LoadedDecisionPolicy> {
    let Some(policy_path) = config.execution.decision_policy_path.as_ref() else {
        return Ok(LoadedDecisionPolicy {
            policy: DecisionPolicy::default(),
            status: "unconfigured".to_owned(),
            input_hash: None,
        });
    };
    let resolved = if policy_path.is_absolute() {
        policy_path.clone()
    } else {
        config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(policy_path)
    };
    let bytes = fs::read(&resolved)
        .with_context(|| format!("read frozen decision policy {}", resolved.display()))?;
    let input_hash = ContentHash::of_bytes(&bytes);
    let artifact = DecisionPolicyArtifact::decode_strict(&bytes)
        .with_context(|| format!("decode frozen decision policy {}", resolved.display()))?;
    let policy = artifact.policy;
    policy
        .validate()
        .with_context(|| format!("validate frozen decision policy {}", resolved.display()))?;

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
    })
}

fn decision_policy_audit(loaded: &LoadedDecisionPolicy) -> (String, Option<ContentHash>) {
    (loaded.status.clone(), loaded.input_hash.clone())
}

fn runtime_identity_from_config(
    config: &Config,
    config_path: &Path,
    model_capabilities: &ModelCapabilityProbeSet,
) -> Result<RuntimeIdentity> {
    let loaded = load_decision_policy_from_config(config, config_path)?;
    runtime_identity_from_config_with_policy(config, config_path, model_capabilities, &loaded.policy)
}

fn runtime_identity_from_config_with_policy(
    config: &Config,
    config_path: &Path,
    model_capabilities: &ModelCapabilityProbeSet,
    decision_policy: &DecisionPolicy,
) -> Result<RuntimeIdentity> {
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
    Ok(env!("AKZIO_SOURCE_REVISION").to_owned())
}

fn read_config_file(path: &Path) -> Result<Config> {
    fs::read_to_string(path)
        .with_context(|| format!("read config {}", path.display()))
        .and_then(|text| toml::from_str::<Config>(&text).context("parse config TOML"))
}

fn read_config_document(path: &Path) -> Result<toml::Value> {
    fs::read_to_string(path)
        .with_context(|| format!("read config {}", path.display()))
        .and_then(|text| toml::from_str::<toml::Value>(&text).context("parse config TOML"))
}

fn write_config_file(path: &Path, document: &toml::Value) -> Result<()> {
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
            "research.planner"
                | "research.analyst"
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

const CANONICAL_RESEARCH_ROUTES: [&str; 4] = [
    "research.planner",
    "research.analyst",
    "research.critic",
    "research.synthesizer",
];

fn validate_canonical_model_identity(model: &OpenAIResponsesConfig) -> Result<()> {
    if model.release_date.is_none() || model.knowledge_cutoff.is_none() {
        bail!("canonical research profiles require model.release_date and model.knowledge_cutoff");
    }
    Ok(())
}

fn validate_canonical_research_routes(profile: &str, model: &OpenAIResponsesConfig) -> Result<()> {
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
    if let Some(value) = config.observatory.sec_user_agent.as_deref() {
        if std::env::var_os("SEC_USER_AGENT").is_none() && !value.is_empty() {
            std::env::set_var("SEC_USER_AGENT", value);
        }
    }
}

fn load_config(path: &Path) -> Result<Config> {
    let mut config = read_config_file(path)?;
    config.agent.budget.validate().context("invalid agent.budget configuration")?;
    resolve_model_configuration(&mut config)?;
    if let Some(store_root) = std::env::var_os("AKZIO_STORE_ROOT") {
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
    let auto_paper = config.daemon.auto_paper.unwrap_or(false);
    let zero_cost =
        config.execution.transaction_cost_ppm == 0 && config.execution.slippage_ppm == 0;

    if auto_paper
        && config.execution.transaction_cost_ppm == 0
        && config.execution.slippage_ppm == 0
    {
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
    fn budget_toml_defaults_role_override_and_million_input() {
        let absent: akzio_domain::AgentSettings = toml::from_str("").unwrap();
        for (purpose, output, timeout) in [
            ("research.planner", 2000, 120),
            ("research.analyst", 6000, 120),
            ("research.critic", 16000, 120),
            ("research.synthesizer", 5000, 120),
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
        assert_eq!(critic.max_wall_time_secs, 120);
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
                "research.planner"
                    | "research.analyst"
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
