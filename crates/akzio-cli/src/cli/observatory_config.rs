// 文件导读：配置初始化从模板创建本地 TOML。Paper 审批组装模型资格与运行身份，
// 再向 daemon 提交审批请求。

fn init_observatory_config(config_path: &Path, template: &Path, store_root: &Path) -> Result<()> {
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
