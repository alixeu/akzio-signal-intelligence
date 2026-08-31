fn runtime_identity_from_config(config: &Config, config_path: &Path) -> Result<RuntimeIdentity> {
    let model = config
        .model
        .as_ref()
        .context("missing [model] configuration")?;
    let feed = config
        .execution
        .market_data_feed
        .context("Paper runtime requires execution.market_data_feed")?;
    let provider_id = model.provider_identity().as_str().to_owned();
    let policy_identity = default_runtime_policy_identity()?;
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
                "assets": config.execution.assets,
                "market_data_feed": config.execution.market_data_feed,
                "transaction_cost_ppm": config.execution.transaction_cost_ppm,
                "slippage_ppm": config.execution.slippage_ppm,
            },
            "model": {
                "provider": model.provider_identity().as_str(),
                "base_url": model.base_url,
                "model": model.model,
                "reasoning_effort": model.reasoning_effort,
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
            .context("serialize redacted v2 TOML")?
            .as_bytes(),
    ))
}

fn source_revision() -> Result<String> {
    Ok(env!("AKZIO_SOURCE_REVISION").to_owned())
}

fn read_config_file(path: &Path) -> Result<Config> {
    fs::read_to_string(path)
        .with_context(|| format!("read v2 config {}", path.display()))
        .and_then(|text| toml::from_str::<Config>(&text).context("parse v2 TOML"))
}

fn read_config_document(path: &Path) -> Result<toml::Value> {
    fs::read_to_string(path)
        .with_context(|| format!("read v2 config {}", path.display()))
        .and_then(|text| toml::from_str::<toml::Value>(&text).context("parse v2 TOML"))
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
    let rendered = toml::to_string_pretty(document).context("serialize v2 TOML")?;
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
    for (purpose, route) in &model.routes {
        if !matches!(
            purpose.as_str(),
            "research.planner"
                | "research.analyst"
                | "research.critic"
                | "research.synthesizer"
                | "learning.outcome_worker"
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
    if config.daemon.auto_paper.unwrap_or(false)
        && config.execution.transaction_cost_ppm == 0
        && config.execution.slippage_ppm == 0
    {
        bail!("Paper scheduler requires explicit transaction_cost_ppm or slippage_ppm");
    }
    if config.daemon.auto_paper.unwrap_or(false) && config.execution.market_data_feed.is_none() {
        bail!("Paper scheduler requires execution.market_data_feed");
    }
    Ok(config)
}
