fn handle_observatory_config(config_path: &Path, command: &ObservatoryConfigCommand) -> Result<()> {
    // 配置入口只负责本地 TOML 的创建、读取和更新；它不启动 daemon、不创建 Run，
    // 也不把编辑后的模型字段视为已经通过 Paper/Decision Gate。
    match command {
        ObservatoryConfigCommand::Init {
            template,
            store_root,
        } => {
            // Init 只在目标不存在时从模板生成新配置；已存在的文件保持不变并返回 created=false。
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
            // 凭据来源是当前进程环境，写入前仍受配置文件权限保护；未提供的可选凭据保持缺省。
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
            // Get 返回当前可编辑投影，其中包含模型路由和连接配置；读取本身不做运行时探测。
            let config = read_config_file(config_path)?;
            print_json(&editable_observatory_configuration(&config)?)
        }
        ObservatoryConfigCommand::Set => {
            // Set 从 stdin 接收完整编辑对象，更新前由 update_observatory_configuration 做
            // provider/模型字段校验，解析或校验失败都不会写回文件。
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
    // 将 Config 映射为 UI 可编辑的扁平投影；release/cutoff 等当前模型身份字段不在该
    // 结构中修改，后续 Set 会从现有配置保留它们。
    let model = config
        .model
        .as_ref()
        .context("Observatory configuration requires [model]")?;
    Ok(ObservatoryEditableConfiguration {
        provider: model.provider_identity().as_str().to_owned(),
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
    // 只允许当前支持的 OpenAI Responses provider；新模型/路由替换后先做基础设置校验，
    // 再把模型、凭据和 SEC user-agent 一并写回，不能通过此入口激活 Policy 或批准 Paper。
    let provider = configuration.provider.trim();
    if provider != OPENAI_RESPONSES_PROVIDER_ID {
        bail!(
            "unsupported model provider {provider}; only {OPENAI_RESPONSES_PROVIDER_ID} is supported"
        );
    }

    let config = read_config_file(config_path)?;
    let current_model = config
        .model
        .as_ref()
        .context("Observatory configuration requires [model]")?;
    let model = OpenAIResponsesConfig {
        // 身份相关 release_date/knowledge_cutoff/debug 从现有配置继承，避免编辑界面
        // 意外清空历史校准所依赖的时间字段。
        base_url: configuration.llm_base_url.trim().to_owned(),
        model: configuration.global_model.trim().to_owned(),
        release_date: current_model.release_date.clone(),
        knowledge_cutoff: current_model.knowledge_cutoff.clone(),
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
    set_optional_toml_string(
        model_table,
        "release_date",
        model.release_date,
    );
    set_optional_toml_string(
        model_table,
        "knowledge_cutoff",
        model.knowledge_cutoff,
    );
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
    qualification_report: &Path,
) -> Result<()> {
    // 先校验交易 Session、operator、理由、金额和有效期，再探测当前模型能力并生成
    // RuntimeIdentity；最终请求只是在 daemon Store 中受理 approval，绝不直接下单。
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
    let model = config
        .model
        .as_ref()
        .context("missing [model] configuration for Paper approval")?;
    let model_capabilities = probe_configured_model_capabilities(model)
        .await
        .context("probe configured model capabilities before Paper approval")?;
    let identity = runtime_identity_from_config(config, config_path, &model_capabilities)?;
    let qualification: ModelQualificationReport = serde_json::from_slice(
        &fs::read(qualification_report).with_context(|| {
            format!(
                "read model qualification report {}",
                qualification_report.display()
            )
        })?,
    )
    .with_context(|| {
        format!(
            "decode model qualification report {}",
            qualification_report.display()
        )
    })?;
    // Qualification 报告必须是独立生成且结构完整的输入；读取成功或 API 受理都不等于
    // DecisionGate/ExecutionGate 已通过，后续 Paper scheduler 仍需执行全部检查。
    qualification.validate().context("validate model qualification report")?;
    print_json(
        &ControlApiClient::from_config(config)?
            .approve_paper(&PaperApprovalRequest {
                session_key: session_key.to_owned(),
                operator: operator.to_owned(),
                reason: reason.to_owned(),
                max_notional_usd_cents,
                valid_hours,
                identity,
                qualification: Some(qualification),
            })
            .await?,
    )
}
