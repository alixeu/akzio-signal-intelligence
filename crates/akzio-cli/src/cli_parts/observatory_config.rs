fn handle_observatory_config(config_path: &Path, command: &ObservatoryConfigCommand) -> Result<()> {
    match command {
        ObservatoryConfigCommand::Init {
            template,
            store_root,
        } => {
            if config_path.exists() {
                return print_json(&serde_json::json!({ "created": false }));
            }
            let template_config = read_config_file(template)?;
            let mut document = read_config_document(template)?;
            toml_section_mut(&mut document, "daemon")?.insert(
                "store_root".to_owned(),
                toml::Value::String(store_root.to_string_lossy().into_owned()),
            );
            if let Some(model) = template_config.model.as_ref() {
                let model_table = toml_section_mut(&mut document, "model")?;
                model_table.insert(
                    "provider".to_owned(),
                    toml::Value::String(model.provider_identity().as_str().to_owned()),
                );
                model_table.insert(
                    "base_url".to_owned(),
                    toml::Value::String(initial_config_value(&model.base_url)),
                );
                model_table.insert(
                    "api_key".to_owned(),
                    toml::Value::String(initial_config_value(&model.api_key)),
                );
            }
            let credentials = toml_section_mut(&mut document, "credentials")?;
            credentials.insert(
                "alpaca_api_key".to_owned(),
                toml::Value::String(std::env::var("ALPACA_API_KEY").unwrap_or_default()),
            );
            credentials.insert(
                "alpaca_api_secret".to_owned(),
                toml::Value::String(std::env::var("ALPACA_API_SECRET").unwrap_or_default()),
            );
            set_optional_toml_string(
                credentials,
                "fred_api_key",
                std::env::var("FRED_API_KEY").ok(),
            );
            set_optional_toml_string(
                toml_section_mut(&mut document, "observatory")?,
                "sec_user_agent",
                std::env::var("SEC_USER_AGENT").ok(),
            );
            write_config_file(config_path, &document)?;
            print_json(&serde_json::json!({ "created": true }))
        }
        ObservatoryConfigCommand::Get => {
            let config = read_config_file(config_path)?;
            print_json(&editable_observatory_configuration(&config)?)
        }
        ObservatoryConfigCommand::Set => {
            let mut payload = String::new();
            io::stdin()
                .read_to_string(&mut payload)
                .context("read Observatory configuration from stdin")?;
            let configuration: ObservatoryEditableConfiguration =
                serde_json::from_str(&payload).context("parse Observatory configuration JSON")?;
            update_observatory_configuration(config_path, configuration)?;
            print_json(&serde_json::json!({ "ok": true }))
        }
    }
}

fn editable_observatory_configuration(config: &Config) -> Result<ObservatoryEditableConfiguration> {
    let model = config
        .model
        .as_ref()
        .context("Observatory configuration requires [model]")?;
    Ok(ObservatoryEditableConfiguration {
        llm_base_url: model.base_url.clone(),
        llm_api_key: model.api_key.clone(),
        global_model: model.model.clone(),
        global_reasoning_effort: model.reasoning_effort.clone(),
        global_response_language: model.response_language.clone(),
        stage_models: model.routes.clone(),
        alpaca_api_key: config.credentials.alpaca_api_key.clone(),
        alpaca_api_secret: config.credentials.alpaca_api_secret.clone(),
        fred_api_key: config.credentials.fred_api_key.clone(),
        sec_user_agent: config.observatory.sec_user_agent.clone(),
    })
}

fn update_observatory_configuration(
    config_path: &Path,
    configuration: ObservatoryEditableConfiguration,
) -> Result<()> {
    let config = read_config_file(config_path)?;
    let current_model = config
        .model
        .as_ref()
        .context("Observatory configuration requires [model]")?;
    let model = OpenAIResponsesConfig {
        base_url: configuration.llm_base_url.trim().to_owned(),
        model: configuration.global_model.trim().to_owned(),
        api_key: configuration.llm_api_key,
        reasoning_effort: configuration.global_reasoning_effort.trim().to_owned(),
        response_language: configuration.global_response_language.trim().to_owned(),
        debug: current_model.debug,
        routes: configuration.stage_models,
    };
    validate_model_settings(&model)?;

    let mut document = read_config_document(config_path)?;
    let model_table = toml_section_mut(&mut document, "model")?;
    model_table.insert(
        "provider".to_owned(),
        toml::Value::String(model.provider_identity().as_str().to_owned()),
    );
    model_table.insert("base_url".to_owned(), toml::Value::String(model.base_url));
    model_table.insert("model".to_owned(), toml::Value::String(model.model));
    model_table.insert("api_key".to_owned(), toml::Value::String(model.api_key));
    model_table.insert(
        "reasoning_effort".to_owned(),
        toml::Value::String(model.reasoning_effort),
    );
    model_table.insert(
        "response_language".to_owned(),
        toml::Value::String(model.response_language),
    );
    model_table.insert(
        "routes".to_owned(),
        toml::Value::try_from(model.routes).context("serialize model routes")?,
    );

    let credentials = toml_section_mut(&mut document, "credentials")?;
    credentials.insert(
        "alpaca_api_key".to_owned(),
        toml::Value::String(configuration.alpaca_api_key),
    );
    credentials.insert(
        "alpaca_api_secret".to_owned(),
        toml::Value::String(configuration.alpaca_api_secret),
    );
    set_optional_toml_string(credentials, "fred_api_key", configuration.fred_api_key);
    set_optional_toml_string(
        toml_section_mut(&mut document, "observatory")?,
        "sec_user_agent",
        configuration.sec_user_agent,
    );
    write_config_file(config_path, &document)
}

#[allow(clippy::too_many_arguments)]
async fn approve_paper(
    config: &Config,
    config_path: &Path,
    session_key: &str,
    operator: &str,
    reason: &str,
    max_notional_usd_cents: i64,
    valid_hours: i64,
) -> Result<()> {
    let _session = chrono::NaiveDate::parse_from_str(session_key, "%Y-%m-%d")
        .context("session_key must be YYYY-MM-DD")?;
    if operator.trim().is_empty()
        || reason.trim().is_empty()
        || max_notional_usd_cents <= 0
        || valid_hours <= 0
        || valid_hours > 24 * 7
    {
        bail!("invalid Paper approval scope");
    }
    let identity = runtime_identity_from_config(config, config_path)?;
    print_json(
        &ControlApiClient::from_config(config)?
            .approve_paper(&PaperApprovalRequest {
                session_key: session_key.to_owned(),
                operator: operator.to_owned(),
                reason: reason.to_owned(),
                max_notional_usd_cents,
                valid_hours,
                identity,
            })
            .await?,
    )
}
