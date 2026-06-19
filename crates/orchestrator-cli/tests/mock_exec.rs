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
        ticker: "QQQ,VIX,SOXX".to_string(),
        date: Some("2026-06-15".to_string()),
        lang: "zh".to_string(),
        mode: Mode::Probability,
        window_days: 150,
        phase1_agents: "technical,news,youtube,reddit,x".to_string(),
        db_path: Some(db_path.clone()),
        run_dir: Some(run_dir.clone()),
        config: Some(config_path),
        model: Some("gpt-5.4".to_string()),
        reasoning_effort: Some("low".to_string()),
        max_debate_rounds: 5,
        max_topics_per_side: 10,
        technical_weight: 40.0,
        news_weight: 35.0,
        youtube_weight: 8.0,
        reddit_weight: 9.0,
        x_weight: 8.0,
        cleanup_days: 0,
        cleanup_old_runs: false,
        from_phase: 1,
        to_phase: 3,
        tech_refresh_enabled: false,
        tech_refresh_intervals: "1d,2h,30min".to_string(),
        tech_refresh_save_bars: 120,
        tech_refresh_script_path: None,
        tech_refresh_timeout_sec: 900,
        tech_refresh_python_bin: None,
        jin10_refresh_enabled: false,
        jin10_refresh_lookback_hours: 24.0,
        jin10_refresh_script_path: None,
        jin10_refresh_timeout_sec: 120,
        wait_for_monitor_time: None,
        monitor_probability_threshold: None,
        monitor_reversal_threshold: None,
        monitor_email_enabled: None,
        mock: true,
    })
    .await
    .unwrap();
    assert_eq!(result["long_probability"], 0.5);
    assert!(run_dir.join("state.json").exists());
    assert!(run_dir.join("final_summary.md").exists());

    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("state.json")).unwrap()).unwrap();
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

    let conn = rusqlite::Connection::open(db_path).unwrap();
    let summary_comma_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM role_turn_summaries WHERE ticker LIKE '%,%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let tool_comma_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM turn_tool_calls WHERE ticker LIKE '%,%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(summary_comma_rows, 0);
    assert_eq!(tool_comma_rows, 0);
}

#[tokio::test]
async fn mock_exec_can_stop_after_phase1() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("phase1-only");
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut args = test_args(
        "QQQ,VIX,SOXX",
        Some(db_path),
        Some(run_dir.clone()),
        Some(config_path),
        true,
    );
    args.to_phase = 1;

    exec::run(args).await.unwrap();

    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("state.json")).unwrap()).unwrap();
    assert_eq!(state["phase_status"]["1"], "done");
    assert!(state["phase_status"].get("2").is_none());
    assert!(state["phase_status"].get("3").is_none());
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
        "QQQ,VIX,SOXX",
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

    let state: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("state.json")).unwrap()).unwrap();
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
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    for role in config["orchestrator"]["llm"]["roles"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        role["api_key"] = serde_json::Value::String("configured-key".to_string());
    }
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
    let run_dir = temp.path().join("strict-run");
    let db_path = temp.path().join("strict.sqlite");

    let err = exec::run(test_args(
        "QQQ",
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
        "QQQ",
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
async fn explicit_llm_roles_must_include_all_required_roles() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]["llm"]["roles"] = serde_json::json!({
        "analyst.technical": {
            "route": "responses",
            "model": "gpt-5.4",
            "base_url": "https://llm.example.com/v1",
            "api_key": "test-key",
            "max_turns": 4,
            "think_tool": false,
            "tools": []
        }
    });
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();

    let err = exec::run(test_args(
        "QQQ",
        Some(temp.path().join("missing-roles.sqlite")),
        Some(temp.path().join("missing-roles-run")),
        Some(config_path),
        true,
    ))
    .await
    .unwrap_err();

    assert!(err.to_string().contains("missing LLM config"));
}

#[tokio::test]
async fn llm_roles_map_is_required() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let mut config: serde_json::Value =
        serde_yaml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
    config["orchestrator"]
        .as_object_mut()
        .unwrap()
        .remove("llm");
    fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();

    let err = exec::run(test_args(
        "QQQ",
        Some(temp.path().join("missing-roles-map.sqlite")),
        Some(temp.path().join("missing-roles-map-run")),
        Some(config_path),
        true,
    ))
    .await
    .unwrap_err();

    assert!(err
        .to_string()
        .contains("orchestrator.llm.roles is required"));
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
        "QQQ",
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
        "QQQ",
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
        "analysis_initial".to_string(),
    )));
    assert!(rows.contains(&(
        "researcher.bull.interaction".to_string(),
        "interaction_research".to_string(),
    )));
    assert!(rows.contains(&(
        "researcher.bear.initial".to_string(),
        "analysis_initial".to_string(),
    )));
    assert!(rows.contains(&(
        "researcher.bear.interaction".to_string(),
        "interaction_research".to_string(),
    )));

    let controller_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM role_turn_summaries WHERE phase = 25 AND role = 'mediator.topic_controller'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(controller_count >= 4);
}

#[tokio::test]
async fn mock_exec_writes_reducer_turn_summaries() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = write_test_config(temp.path());
    let run_dir = temp.path().join("reducer-run");
    let db_path = temp.path().join("reducer.sqlite");

    exec::run(test_args(
        "QQQ",
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

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(phase, role, summary_json)| {
        *phase == 15 && role == "reducer.evidence" && summary_json.contains("reducer.evidence")
    }));
    assert!(rows.iter().any(|(phase, role, summary_json)| {
        *phase == 25
            && role == "reducer.debate_final"
            && summary_json.contains("reducer.debate_final")
    }));

    let turn_item_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM agent_turn_items", [], |row| {
            row.get(0)
        })
        .unwrap();
    let tool_call_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM turn_tool_calls", [], |row| row.get(0))
        .unwrap();
    assert_eq!(turn_item_rows, 0);
    assert_eq!(tool_call_rows, 0);
}

fn test_args(
    ticker: &str,
    db_path: Option<PathBuf>,
    run_dir: Option<PathBuf>,
    config: Option<PathBuf>,
    mock: bool,
) -> ExecArgs {
    ExecArgs {
        ticker: ticker.to_string(),
        date: Some("2026-06-15".to_string()),
        lang: "zh".to_string(),
        mode: Mode::Probability,
        window_days: 150,
        phase1_agents: "technical,news,youtube,reddit,x".to_string(),
        db_path,
        run_dir,
        config,
        model: Some("gpt-5.4".to_string()),
        reasoning_effort: Some("low".to_string()),
        max_debate_rounds: 1,
        max_topics_per_side: 10,
        technical_weight: 40.0,
        news_weight: 35.0,
        youtube_weight: 8.0,
        reddit_weight: 9.0,
        x_weight: 8.0,
        cleanup_days: 0,
        cleanup_old_runs: false,
        from_phase: 1,
        to_phase: 3,
        tech_refresh_enabled: false,
        tech_refresh_intervals: "1d,2h,30min".to_string(),
        tech_refresh_save_bars: 120,
        tech_refresh_script_path: None,
        tech_refresh_timeout_sec: 900,
        tech_refresh_python_bin: None,
        jin10_refresh_enabled: false,
        jin10_refresh_lookback_hours: 24.0,
        jin10_refresh_script_path: None,
        jin10_refresh_timeout_sec: 120,
        wait_for_monitor_time: None,
        monitor_probability_threshold: None,
        monitor_reversal_threshold: None,
        monitor_email_enabled: None,
        mock,
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
    ] {
        fs::write(prompt_dir.join(name), format!("{name} {{ticker}}")).unwrap();
    }
    let config_path = root.join("config.yaml");
    let config_text = format!(
        r#"
orchestrator:
  data_source:
    strict_sqlite: true
    required_contexts:
      - technical
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
    );
    let mut config: serde_json::Value = serde_yaml::from_str(&config_text).unwrap();
    config["orchestrator"]["llm"]["roles"] = complete_llm_roles_config();
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
