use orchestrator_cli::exec::{self, ExecArgs, Mode};
use std::{fs, path::Path};

#[tokio::test]
async fn mock_exec_writes_file_store_manifest_and_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let store_root = temp.path().join("store");

    let result = exec::run(test_args(temp.path(), store_root.clone(), 3))
        .await
        .unwrap();

    assert_eq!(result["long_probability"], 0.5);
    assert!(has_file_named(&store_root.join("runs"), "manifest.json"));
    assert!(has_directory_named(&store_root.join("runs"), "index"));
    assert_no_database_files(&store_root);

    let state = &result["run_state"];
    assert_eq!(state["phase_status"]["1"], "done");
    assert_eq!(state["phase_status"]["2"], "done");
    assert_eq!(state["phase_status"]["3"], "done");
    assert_eq!(
        state["phase1_agents"],
        serde_json::json!(["analyst.technical", "analyst.news_macro"])
    );
}

#[tokio::test]
async fn mock_exec_phase7_writes_file_store_allocation() {
    let temp = tempfile::tempdir().unwrap();
    let store_root = temp.path().join("store");

    let result = exec::run(test_args(temp.path(), store_root.clone(), 7))
        .await
        .unwrap();

    assert_eq!(result["action"], "Hold");
    assert_eq!(result["final_trade_decision"]["rating"], "Hold");
    assert_eq!(result["portfolio_allocation"]["total_equity_exposure"], 0.0);
    assert_eq!(
        result["portfolio_allocation"]["weights"]["cash_hedge"]["weight"],
        1.0
    );
    assert!(has_file_named(&store_root.join("runs"), "allocation.json"));
    assert_no_database_files(&store_root);

    let state = &result["run_state"];
    assert_eq!(state["phase_status"]["7"], "done");
    assert_eq!(state["phase4_authority"], "file_store");
    assert_eq!(state["phase6_authority"], "file_store");
}

#[tokio::test]
async fn mock_exec_phase8_archives_to_file_store() {
    let temp = tempfile::tempdir().unwrap();
    let store_root = temp.path().join("store");

    let result = exec::run(test_args(temp.path(), store_root.clone(), 8))
        .await
        .unwrap();

    assert_eq!(result["run_state"]["phase_status"]["8"], "done");
    assert!(has_file_named(&store_root.join("runs"), "manifest.json"));
    assert!(has_file_named(&store_root.join("runs"), "allocation.json"));
    assert_no_database_files(&store_root);
}

fn test_args(config_root: &Path, store_root: std::path::PathBuf, to_phase: i64) -> ExecArgs {
    ExecArgs {
        date: Some("2026-06-15".to_string()),
        lang: "zh".to_string(),
        mode: Mode::Probability,
        window_days: None,
        store_root: Some(store_root),
        config: Some(write_test_config(config_root)),
        model: Some("gpt-5.4".to_string()),
        reasoning_effort: Some("low".to_string()),
        max_debate_rounds: None,
        max_topics_per_side: None,
        from_phase: 1,
        to_phase,
        tech_refresh_enabled: false,
        jin10_refresh_lookback_hours: 24.0,
        mock: true,
        debug: false,
    }
}

fn write_test_config(root: &Path) -> std::path::PathBuf {
    let prompt_dir = root.join("prompts");
    fs::create_dir_all(&prompt_dir).unwrap();
    for name in [
        "analyst_technical.md",
        "analyst_news.md",
        "bull_initial.md",
        "bull_interaction.md",
        "bear_initial.md",
        "bear_interaction.md",
        "topic_controller.md",
        "manager.md",
        "trader.md",
        "risk_conservative.md",
        "portfolio_manager.md",
    ] {
        fs::write(prompt_dir.join(name), format!("{name} {{ticker}}")).unwrap();
    }
    let config_path = root.join("config.yaml");
    let config_text = format!(
        r#"
orchestrator:
  analysis_universe: [QQQ, SOXX, VIX]
  store:
    root: "{}"
  allocation:
    investable_assets: [QQQ, SOXX]
    regime_signal: VIX
    regime_thresholds: [15, 20, 30]
    regime_labels: [risk_on, normal, elevated, defensive]
    correlation_window_days: 60
    max_single_position: 0.70
    vol_indicator: STD20
  prompts:
    analyst:
      technical: "{}"
      news_macro: "{}"
    phase2:
      bull_initial: "{}"
      bull_interaction: "{}"
      bear_initial: "{}"
      bear_interaction: "{}"
    mediator:
      topic_controller: "{}"
    manager:
      research: "{}"
    trader: "{}"
    risk:
      conservative: "{}"
    portfolio:
      manager: "{}"
"#,
        root.join("store").display(),
        prompt_dir.join("analyst_technical.md").display(),
        prompt_dir.join("analyst_news.md").display(),
        prompt_dir.join("bull_initial.md").display(),
        prompt_dir.join("bull_interaction.md").display(),
        prompt_dir.join("bear_initial.md").display(),
        prompt_dir.join("bear_interaction.md").display(),
        prompt_dir.join("topic_controller.md").display(),
        prompt_dir.join("manager.md").display(),
        prompt_dir.join("trader.md").display(),
        prompt_dir.join("risk_conservative.md").display(),
        prompt_dir.join("portfolio_manager.md").display(),
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

fn has_file_named(root: &Path, name: &str) -> bool {
    walk(root)
        .iter()
        .any(|path| path.is_file() && path.file_name().is_some_and(|file| file == name))
}

fn has_directory_named(root: &Path, name: &str) -> bool {
    walk(root)
        .iter()
        .any(|path| path.is_dir() && path.file_name().is_some_and(|directory| directory == name))
}

fn assert_no_database_files(root: &Path) {
    let databases = walk(root)
        .into_iter()
        .filter(|path| path.is_file())
        .filter(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| matches!(extension, "sqlite" | "db"))
        })
        .collect::<Vec<_>>();
    assert!(
        databases.is_empty(),
        "unexpected database files: {databases:?}"
    );
}

fn walk(root: &Path) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return paths;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            paths.extend(walk(&path));
        }
        paths.push(path);
    }
    paths
}
