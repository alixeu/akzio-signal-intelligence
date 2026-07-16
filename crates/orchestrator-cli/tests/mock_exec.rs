use orchestrator_cli::exec::{self, ExecArgs, Mode};
use rusqlite::Connection;
use std::{fs, path::PathBuf};

#[tokio::test]
async fn mock_exec_writes_state_and_final_summary() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("run");
    let db_path = temp.path().join("orchestrator.sqlite");
    let result = exec::run(ExecArgs {
        date: Some("2026-06-15".to_string()),
        lang: "zh".to_string(),
        mode: Mode::Probability,
        window_days: None,
        phase1_agents: None,
        db_path: Some(db_path.clone()),
        run_dir: Some(run_dir.clone()),
        config: Some(config_path),
        model: Some("gpt-5.4".to_string()),
        reasoning_effort: Some("low".to_string()),
        max_debate_rounds: None,
        max_topics_per_side: None,
        technical_weight: None,
        news_weight: None,
        youtube_weight: None,
        reddit_weight: None,
        x_weight: None,
        from_phase: 1,
        to_phase: 3,
        tech_refresh_enabled: false,
        tech_refresh_intervals: "1d,3h,20min".to_string(),
        tech_refresh_save_bars: 120,
        tech_refresh_script_path: None,
        tech_refresh_timeout_sec: 900,
        tech_refresh_python_bin: None,
        jin10_refresh_enabled: false,
        jin10_refresh_lookback_hours: 24.0,
        jin10_refresh_script_path: None,
        jin10_refresh_timeout_sec: 120,
        mock: true,
        debug: false,
    })
    .await
    .unwrap();
    assert_eq!(result["long_probability"], 0.5);

    let state = &result["run_state"];
    assert_eq!(
        state["phase1_agents"],
        serde_json::json!([
            "analyst.technical",
            "analyst.news_macro",
            "analyst.youtube",
            "analyst.reddit",
            "analyst.x"
        ])
    );
    assert_role_metrics_ok(state);
    assert_phase_metrics_ok(state, 3);

    let conn = rusqlite::Connection::open(db_path).unwrap();
    let summary_comma_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM role_turn_summaries WHERE ticker LIKE '%,%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(summary_comma_rows, 0);
}

#[tokio::test]
async fn mock_exec_can_stop_after_phase1() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("phase1-only");
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut args = test_args(
        Some(db_path),
        Some(run_dir.clone()),
        Some(config_path),
        true,
    );
    args.to_phase = 1;

    let result = exec::run(args).await.unwrap();

    assert!(run_dir.join("state.json").exists());
    assert!(run_dir.join("final_summary.md").exists());

    let persisted_state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("state.json")).unwrap()).unwrap();
    let state = &result["run_state"];
    assert_eq!(&persisted_state, state);
    assert_eq!(state["phase_status"]["1"], "done");
    assert!(state["phase_status"].get("2").is_none());
    assert!(state["phase_status"].get("3").is_none());
    assert_contracts_ok(state);
    assert_phase_metrics_ok(state, 1);
}

#[tokio::test]
async fn mock_exec_phase7_writes_portfolio_allocation() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("phase7-run");
    let db_path = temp.path().join("phase7.sqlite");
    let mut args = test_args(
        Some(db_path.clone()),
        Some(run_dir.clone()),
        Some(config_path),
        true,
    );
    args.to_phase = 7;

    let result = exec::run(args).await.unwrap();

    assert_eq!(result["action"], "Hold");
    assert_eq!(result["final_trade_decision"]["rating"], "Hold");
    assert_eq!(result["portfolio_allocation"]["total_equity_exposure"], 0.0);
    assert_eq!(
        result["portfolio_allocation"]["weights"]["cash_hedge"]["weight"],
        1.0
    );
    assert_eq!(
        result["portfolio_allocation"]["allocation_method"],
        "fallback_cash"
    );
    assert!(result["portfolio_allocation"]["weights"]
        .get("QQQ")
        .is_none());
    assert!(result["portfolio_allocation"]["weights"]
        .get("SOXX")
        .is_none());
    assert!(result["portfolio_allocation"]["weights"]
        .get("VIX")
        .is_none());

    let state = &result["run_state"];
    // Default policy mode is selective: trader/portfolio may be derived while
    // risk still runs when probability is near-neutral (mock Hold @ 0.5).
    assert_eq!(state["phase_status"]["4"], "derived");
    assert_eq!(state["phase_status"]["5"], "done");
    assert_eq!(state["phase_status"]["6"], "derived");
    assert_eq!(state["phase_status"]["7"], "done");
    assert_eq!(state["trader_investment_plan"]["action"], "Hold");
    assert_eq!(
        state["risk_debate_state"]["history"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(state["portfolio_allocation"]["total_equity_exposure"], 0.0);
    assert_market_truth_ok(state);
    assert_contracts_ok(state);
    assert_role_metrics_ok(state);
    assert_phase_metrics_ok(state, 7);
    assert!(state["phase_status"].get("8").is_none());
    let conn = Connection::open(db_path).unwrap();
    let phase8_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM runs WHERE phase_count > 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(phase8_rows, 0);
}

#[tokio::test]
async fn mock_exec_phase8_writes_archive_predictions_and_system_metrics() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("phase8-run");
    let db_path = temp.path().join("phase8.sqlite");
    let mut args = test_args(
        Some(db_path.clone()),
        Some(run_dir.clone()),
        Some(config_path),
        true,
    );
    args.to_phase = 8;

    let result = exec::run(args).await.unwrap();

    let state = &result["run_state"];
    assert_eq!(state["phase_status"]["8"], "done");

    let conn = Connection::open(db_path).unwrap();
    let archive_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM runs WHERE phase_count > 0",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let prediction_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM predictions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(archive_count, 1);
    assert!(prediction_count >= 1);
}

#[tokio::test]
async fn selective_policy_derives_trader_runs_triggered_risk_and_allocates() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]["workflow"]["policy"]["mode"] =
        serde_json::Value::String("selective".to_string());
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
    let run_dir = temp.path().join("selective-run");
    let db_path = temp.path().join("selective.sqlite");
    let mut args = test_args(
        Some(db_path),
        Some(run_dir.clone()),
        Some(config_path),
        true,
    );
    args.to_phase = 7;

    let result = exec::run(args).await.unwrap();

    let state = &result["run_state"];
    assert_eq!(state["workflow_policy"]["mode"], "selective");
    assert_eq!(
        state["workflow_policy"]["skipped_phases"],
        serde_json::json!(["trader", "portfolio_review"])
    );
    assert_eq!(state["workflow_metrics"]["policy_mode"], "selective");
    assert_eq!(
        state["workflow_metrics"]["skipped_phases"],
        serde_json::json!(["trader", "portfolio_review"])
    );
    assert_eq!(state["trader_investment_plan"]["status"], "derived");
    assert_eq!(
        state["trader_investment_plan"]["method"],
        "conservative_research_plan_mapping"
    );
    assert_eq!(
        state["risk_debate_state"]["history"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(state["final_trade_decision"]["status"], "derived");
    assert_eq!(state["portfolio_allocation"]["total_equity_exposure"], 0.0);
    assert_eq!(
        state["portfolio_allocation"]["weights"]["cash_hedge"]["weight"],
        1.0
    );
    assert!(state["portfolio_allocation"]["weights"]
        .get("QQQ")
        .is_none());
    assert!(state["portfolio_allocation"]["weights"]
        .get("SOXX")
        .is_none());
    assert_eq!(
        result["long_probability"],
        state["research_plan"]["long_probability"]
    );
    assert_eq!(
        result["short_probability"],
        state["research_plan"]["short_probability"]
    );
    assert_eq!(result["portfolio_allocation"]["total_equity_exposure"], 0.0);
    assert_market_truth_ok(state);
    assert_contracts_ok(state);
    assert_role_metrics_ok(state);
    assert_phase_metrics_ok(state, 7);
    assert!(state["role_job_metrics"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item["role"] != "trader" && item["role"] != "portfolio.manager"));
}

#[tokio::test]
async fn legacy_policy_runs_all_optional_phases_and_allocates() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]["workflow"]["policy"]["mode"] =
        serde_json::Value::String("legacy".to_string());
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
    let run_dir = temp.path().join("legacy-run");
    let db_path = temp.path().join("legacy.sqlite");
    let mut args = test_args(
        Some(db_path),
        Some(run_dir.clone()),
        Some(config_path),
        true,
    );
    args.to_phase = 7;

    let result = exec::run(args).await.unwrap();

    let state = &result["run_state"];
    assert_eq!(state["workflow_policy"]["mode"], "legacy");
    assert_eq!(
        state["workflow_policy"]["skipped_phases"],
        serde_json::json!([])
    );
    assert_eq!(state["workflow_metrics"]["policy_mode"], "legacy");
    assert_eq!(
        state["trader_investment_plan"]["status"],
        serde_json::Value::Null
    );
    assert!(!state["risk_debate_state"]["history"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        state["final_trade_decision"]["status"],
        serde_json::Value::Null
    );
    assert_eq!(result["portfolio_allocation"]["total_equity_exposure"], 0.0);
    assert_contracts_ok(state);
}

#[tokio::test]
async fn mock_exec_uses_configured_shared_db_path_by_default() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("configured-db-run");
    let db_path = temp.path().join("configured-orchestrator.sqlite");
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]["db_path"] = serde_json::Value::String(db_path.display().to_string());
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();

    let result = exec::run(test_args(
        None,
        Some(run_dir.clone()),
        Some(config_path),
        true,
    ))
    .await
    .unwrap();

    assert_eq!(result["db_path"], db_path.display().to_string());
    assert!(db_path.exists());
    assert!(!run_dir.join("run.sqlite").exists());

    let state = &result["run_state"];
    assert_eq!(state["db_path"], db_path.display().to_string());

    let conn = Connection::open(db_path).unwrap();
    let run_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(run_rows, 1);
}

#[tokio::test]
async fn live_exec_requires_unknown_sqlite_context_when_strict() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let config_text = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config_text.replace("- technical", "- technical\n      - required-custom-source"),
    )
    .unwrap();
    let run_dir = temp.path().join("strict-run");
    let db_path = temp.path().join("strict.sqlite");

    let err = exec::run(test_args(
        Some(db_path),
        Some(run_dir),
        Some(config_path),
        false,
    ))
    .await
    .unwrap_err();

    assert!(
        err.to_string().contains("strict SQLite data source"),
        "{err:#}"
    );
}

#[tokio::test]
async fn openai_compatible_provider_can_use_configured_api_key() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]["llm"]["roles"] = openai_compatible_llm_roles_config();
    for role in config["orchestrator"]["llm"]["roles"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        role["api_key"] = serde_json::Value::String("configured-key".to_string());
    }
    let config_text = serde_yaml::to_string(&config).unwrap();
    fs::write(
        &config_path,
        config_text.replace("- technical", "- technical\n      - required-custom-source"),
    )
    .unwrap();
    let err = exec::run(test_args(
        Some(temp.path().join("configured-third-party-key.sqlite")),
        Some(temp.path().join("configured-third-party-key-run")),
        Some(config_path),
        false,
    ))
    .await
    .unwrap_err();

    assert!(
        err.to_string().contains("strict SQLite data source"),
        "{err:#}"
    );
}

#[tokio::test]
async fn explicit_partial_llm_roles_merge_with_builtin_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]["llm"]["roles"] = serde_json::json!({
        "analyst.technical": {
            "max_turns": 4,
            "think_tool": false,
            "tools": []
        }
    });
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();

    let output = exec::run(test_args(
        Some(temp.path().join("partial-roles.sqlite")),
        Some(temp.path().join("partial-roles-run")),
        Some(config_path),
        true,
    ))
    .await
    .unwrap();

    assert_eq!(
        output["phase1_agents"],
        serde_json::json!([
            "analyst.technical",
            "analyst.news_macro",
            "analyst.youtube",
            "analyst.reddit",
            "analyst.x"
        ])
    );
}

#[tokio::test]
async fn llm_roles_map_defaults_when_omitted() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]["llm"]
        .as_object_mut()
        .unwrap()
        .remove("roles");
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();

    let output = exec::run(test_args(
        Some(temp.path().join("default-roles.sqlite")),
        Some(temp.path().join("default-roles-run")),
        Some(config_path),
        true,
    ))
    .await
    .unwrap();

    assert_eq!(
        output["phase1_agents"],
        serde_json::json!([
            "analyst.technical",
            "analyst.news_macro",
            "analyst.youtube",
            "analyst.reddit",
            "analyst.x"
        ])
    );
}

#[tokio::test]
async fn llm_role_config_rejects_unknown_route() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]["llm"]["roles"] = complete_llm_roles_config();
    config["orchestrator"]["llm"]["roles"]["manager.research"]["route"] =
        serde_json::Value::String("legacy_route".to_string());
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();

    let err = exec::run(test_args(
        Some(temp.path().join("bad-provider.sqlite")),
        Some(temp.path().join("bad-provider-run")),
        Some(config_path),
        true,
    ))
    .await
    .unwrap_err();

    assert!(err.to_string().contains("invalid LLM config"));
}

#[tokio::test]
async fn mock_phase2_writes_initial_and_interaction_turns() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("phase2-run");
    let db_path = temp.path().join("phase2.sqlite");

    exec::run(test_args(
        Some(db_path.clone()),
        Some(run_dir),
        Some(config_path),
        true,
    ))
    .await
    .unwrap();

    let conn = Connection::open(db_path).unwrap();
    let rows: Vec<(String, String)> = {
        let mut stmt = conn
            .prepare(
                "SELECT role, summary_type
                 FROM role_turn_summaries
                 WHERE phase = 2
                 ORDER BY role, summary_type, id",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };

    assert!(rows.contains(&("mediator.topic".to_string(), "topic_final".to_string())));
    assert!(rows.contains(&(
        "researcher.bull.initial".to_string(),
        "bull_seed".to_string(),
    )));
    assert!(rows.contains(&(
        "researcher.bull.interaction".to_string(),
        "bull_packet".to_string(),
    )));
    assert!(rows.contains(&(
        "researcher.bear.initial".to_string(),
        "bear_seed".to_string(),
    )));
    assert!(rows.contains(&(
        "researcher.bear.interaction".to_string(),
        "bear_packet".to_string(),
    )));

    let controller_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM role_turn_summaries WHERE phase = 25 AND role = 'mediator.topic_controller'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(controller_count >= 2);
}

#[tokio::test]
async fn mock_exec_writes_reducer_turn_summaries() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("reducer-run");
    let db_path = temp.path().join("reducer.sqlite");

    exec::run(test_args(
        Some(db_path.clone()),
        Some(run_dir),
        Some(config_path),
        true,
    ))
    .await
    .unwrap();

    let conn = Connection::open(db_path).unwrap();
    let rows: Vec<(i64, String, String)> = {
        let mut stmt = conn
            .prepare(
                "SELECT phase, role, summary_json
                 FROM role_turn_summaries
                 WHERE role IN ('reducer.evidence', 'reducer.debate_final')
                 ORDER BY phase, role",
            )
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };

    assert_eq!(rows.len(), 6);
    assert!(rows.iter().any(|(phase, role, summary_json)| {
        *phase == 15 && role == "reducer.evidence" && summary_json.contains("reducer.evidence")
    }));
    assert!(rows.iter().any(|(phase, role, summary_json)| {
        *phase == 25
            && role == "reducer.debate_final"
            && summary_json.contains("reducer.debate_final")
    }));
    assert_eq!(
        rows.iter()
            .filter(|(_, role, _)| role == "reducer.evidence")
            .count(),
        3
    );
    assert_eq!(
        rows.iter()
            .filter(|(_, role, _)| role == "reducer.debate_final")
            .count(),
        3
    );

    let event_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM agent_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(event_rows, 0);
}

fn assert_market_truth_ok(state: &serde_json::Value) {
    assert_eq!(
        state
            .get("market_truth_violations")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
        serde_json::json!([])
    );
    let checks = state["market_truth_checks"].as_array().unwrap();
    assert_eq!(checks.len(), 6);
    assert!(checks.iter().all(|check| check["status"] == "ok"));
    assert_eq!(state["workflow_metrics"]["market_truth_check_count"], 6);
    assert_eq!(state["workflow_metrics"]["market_truth_violation_count"], 0);
}

fn assert_contracts_ok(state: &serde_json::Value) {
    assert_eq!(state["workflow_contracts"].as_array().unwrap().len(), 9);
    assert_eq!(state["contract_violations"], serde_json::json!([]));
    assert_downstream_contract_schemas_ok(state);
}

fn assert_downstream_contract_schemas_ok(state: &serde_json::Value) {
    let contracts = state["workflow_contracts"].as_array().unwrap();
    for (phase, name, state_field) in [
        (4, "TradeIntent", "trader_investment_plan"),
        (5, "RiskConstraints", "risk_debate_state"),
        (6, "FinalValidation", "final_trade_decision"),
        (7, "PortfolioAllocation", "portfolio_allocation"),
    ] {
        let contract = contracts
            .iter()
            .find(|item| item["phase"] == phase)
            .unwrap();
        assert_eq!(contract["name"], name);
        assert_eq!(contract["state_field"], state_field);
        assert!(contract["schema"].as_str().unwrap().contains("properties"));
    }
}

fn assert_role_metrics_ok(state: &serde_json::Value) {
    let jobs = state["role_job_metrics"].as_array().unwrap();
    assert!(!jobs.is_empty());
    assert_eq!(state["workflow_metrics"]["role_job_count"], jobs.len());
    assert_eq!(state["workflow_metrics"]["llm_call_count"], jobs.len());
    assert_eq!(state["workflow_metrics"]["timed_out_role_count"], 0);
    assert!(jobs.iter().all(|job| job["status"] == "ok"));
}

fn assert_phase_metrics_ok(state: &serde_json::Value, expected_count: usize) {
    let phases = state["phase_metrics"].as_array().unwrap();
    assert_eq!(phases.len(), expected_count);
    assert_eq!(state["workflow_metrics"]["phase_count"], expected_count);
    assert!(phases
        .iter()
        .all(|phase| phase["elapsed_ms"].as_u64().is_some()));
}

fn test_args(
    db_path: Option<PathBuf>,
    run_dir: Option<PathBuf>,
    config: Option<PathBuf>,
    mock: bool,
) -> ExecArgs {
    ExecArgs {
        date: Some("2026-06-15".to_string()),
        lang: "zh".to_string(),
        mode: Mode::Probability,
        window_days: None,
        phase1_agents: None,
        db_path,
        run_dir,
        config,
        model: Some("gpt-5.4".to_string()),
        reasoning_effort: Some("low".to_string()),
        max_debate_rounds: None,
        max_topics_per_side: None,
        technical_weight: None,
        news_weight: None,
        youtube_weight: None,
        reddit_weight: None,
        x_weight: None,
        from_phase: 1,
        to_phase: 3,
        tech_refresh_enabled: false,
        tech_refresh_intervals: "1d,3h,20min".to_string(),
        tech_refresh_save_bars: 120,
        tech_refresh_script_path: None,
        tech_refresh_timeout_sec: 900,
        tech_refresh_python_bin: None,
        jin10_refresh_enabled: false,
        jin10_refresh_lookback_hours: 24.0,
        jin10_refresh_script_path: None,
        jin10_refresh_timeout_sec: 120,
        mock,
        debug: false,
    }
}

fn write_test_config(root: &std::path::Path) -> PathBuf {
    let prompt_dir = root.join("prompts");
    fs::create_dir_all(&prompt_dir).unwrap();
    for name in [
        "analyst_technical.md",
        "analyst_news.md",
        "bull_initial.md",
        "bull_interaction.md",
        "bear_initial.md",
        "bear_interaction.md",
        "reducer_evidence.md",
        "reducer_debate_final.md",
        "topic_generation.md",
        "topic_controller.md",
        "manager.md",
        "trader.md",
        "risk_aggressive.md",
        "risk_conservative.md",
        "risk_neutral.md",
        "portfolio_manager.md",
        "allocation.md",
    ] {
        fs::write(prompt_dir.join(name), format!("{name} {{ticker}}")).unwrap();
    }
    let config_path = root.join("config.yaml");
    let config_text = format!(
        r#"
orchestrator:
  analysis_universe: [QQQ, SOXX, VIX]
  data_source:
    strict_sqlite: true
    required_contexts:
      - technical
  allocation:
    investable_assets: [QQQ, SOXX]
    regime_signal: VIX
    regime_thresholds: [15, 20, 30]
    regime_labels: [risk_on, normal, elevated, defensive]
    correlation_window_days: 60
    max_single_position: 0.70
    vol_indicator: STD20
  runtime:
    max_risk_rounds: 1
  prompts:
    analyst:
      technical: "{}"
      news_macro: "{}"
    phase2:
      topic_generation: "{}"
      bull_initial: "{}"
      bull_interaction: "{}"
      bear_initial: "{}"
      bear_interaction: "{}"
    mediator:
      topic: "{}"
      topic_controller: "{}"
    reducers:
      evidence: "{}"
      debate_final: "{}"
    manager:
      research: "{}"
    trader: "{}"
    risk:
      aggressive: "{}"
      conservative: "{}"
      neutral: "{}"
    portfolio:
      manager: "{}"
    allocation:
      manager: "{}"
"#,
        prompt_dir.join("analyst_technical.md").display(),
        prompt_dir.join("analyst_news.md").display(),
        prompt_dir.join("topic_generation.md").display(),
        prompt_dir.join("bull_initial.md").display(),
        prompt_dir.join("bull_interaction.md").display(),
        prompt_dir.join("bear_initial.md").display(),
        prompt_dir.join("bear_interaction.md").display(),
        prompt_dir.join("topic_generation.md").display(),
        prompt_dir.join("topic_controller.md").display(),
        prompt_dir.join("reducer_evidence.md").display(),
        prompt_dir.join("reducer_debate_final.md").display(),
        prompt_dir.join("manager.md").display(),
        prompt_dir.join("trader.md").display(),
        prompt_dir.join("risk_aggressive.md").display(),
        prompt_dir.join("risk_conservative.md").display(),
        prompt_dir.join("risk_neutral.md").display(),
        prompt_dir.join("portfolio_manager.md").display(),
        prompt_dir.join("allocation.md").display(),
    );
    let mut config: serde_json::Value = serde_yaml::from_str(&config_text).unwrap();
    config["orchestrator"]["llm"]["defaults"] = serde_json::json!({
        "route": "responses",
        "model": "gpt-5.4",
        "base_url": "https://llm.example.com/v1",
        "api_key": "test-key",
        "reasoning_effort": "low",
        "think_tool": false,
        "tools": []
    });
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
    config_path
}

fn complete_llm_roles_config() -> serde_json::Value {
    let mut roles = serde_json::Map::new();
    for role in [
        "analyst.technical",
        "analyst.news_macro",
        "analyst.youtube",
        "analyst.reddit",
        "analyst.x",
        "researcher.bull.initial",
        "researcher.bear.initial",
        "researcher.bull.interaction",
        "researcher.bear.interaction",
        "reducer.evidence",
        "mediator.topic_controller",
        "reducer.debate_final",
        "mediator.topic",
        "manager.research",
        "trader",
        "risk.aggressive",
        "risk.conservative",
        "risk.neutral",
        "portfolio.manager",
        "allocation.manager",
    ] {
        let is_phase1 = role.starts_with("analyst.");
        roles.insert(
            role.to_string(),
            serde_json::json!({
                "route": "responses",
                "model": "gpt-5.4",
                "base_url": "https://llm.example.com/v1",
                "api_key": "test-key",
                "max_turns": 4,
                "reasoning_effort": if is_phase1 { "low" } else { "medium" },
                "think_tool": !is_phase1,
                "tools": []
            }),
        );
    }
    serde_json::Value::Object(roles)
}

fn openai_compatible_llm_roles_config() -> serde_json::Value {
    let mut roles = serde_json::Map::new();
    for role in [
        "analyst.technical",
        "analyst.news_macro",
        "analyst.youtube",
        "analyst.reddit",
        "analyst.x",
        "researcher.bull.initial",
        "researcher.bear.initial",
        "researcher.bull.interaction",
        "researcher.bear.interaction",
        "reducer.evidence",
        "mediator.topic_controller",
        "reducer.debate_final",
        "mediator.topic",
        "manager.research",
        "trader",
        "risk.aggressive",
        "risk.conservative",
        "risk.neutral",
        "portfolio.manager",
        "allocation.manager",
    ] {
        let is_phase1 = role.starts_with("analyst.");
        roles.insert(
            role.to_string(),
            serde_json::json!({
                "route": "responses",
                "model": "gpt-5.4",
                "base_url": "https://llm.example.com/v1",
                "api_key": "test-key",
                "max_turns": 4,
                "reasoning_effort": null,
                "think_tool": !is_phase1,
                "tools": []
            }),
        );
    }
    serde_json::Value::Object(roles)
}
