use orchestrator_cli::exec::{self, ExecArgs, Mode};
use orchestrator_store::{
    inspect_store, read_indexes, read_run_manifest, FileStore, FileStoreOptions, IndexKind,
    IndexQuery, RunLocation, RunStatus,
};
use std::{fs, path::Path};

#[tokio::test]
async fn mock_exec_writes_file_store_manifest_and_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let store_root = temp.path().join("store");

    let result = exec::run(test_args(temp.path(), store_root.clone(), 8))
        .await
        .unwrap();

    assert_eq!(
        result["run_state"]["research_plan"]["per_ticker"]["QQQ"]["long_probability"],
        0.5
    );
    assert!(has_file_named(&store_root.join("runs"), "manifest.json"));
    assert!(has_directory_named(&store_root.join("runs"), "index"));

    let state = &result["run_state"];
    assert_eq!(state["phase_status"]["1"], "done");
    assert_eq!(state["phase_status"]["2"], "done");
    assert_eq!(state["phase_status"]["3"], "done");
    assert!(state["analyst_reports"]["analyst.technical"].is_object());
    assert!(state["analyst_reports"]["analyst.news_macro"].is_object());
    assert_eq!(
        state["debate_state_artifact"]["final_reducer"]["authoritative_fields"]["controllers"]
            .as_object()
            .map(|items| items.len()),
        Some(0)
    );
    assert_eq!(result["store_compaction"]["eligible"], true);
    assert_eq!(result["store_compaction"]["applied"], true);
    assert!(result["store_compaction"]["index_archives"]
        .as_u64()
        .is_some_and(|count| count > 0));
    for removed in ["artifacts", "drafts", "inputs", "memory", "sessions"] {
        assert!(!has_directory_named(&store_root.join("runs"), removed));
    }
    assert!(!has_file_named(&store_root.join("runs"), "state.json"));
    assert!(!has_file_with_extension(&store_root.join("runs"), "lock"));
    let store = FileStore::open(&store_root, FileStoreOptions::default()).unwrap();
    let run = RunLocation::new("2026-06-15", result["run_id"].as_str().expect("run_id")).unwrap();
    assert!(!read_indexes(
        &store,
        Some(&run),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            limit: 100,
            ..Default::default()
        },
    )
    .unwrap()
    .indexes
    .is_empty());
    let doctor = inspect_store(&store);
    assert!(doctor.issues.is_empty(), "{:#?}", doctor.issues);
    let run_root = store_root.join(run.relative_root());
    for path in walk(&run_root).into_iter().filter(|path| path.is_file()) {
        let relative = path.strip_prefix(&run_root).unwrap();
        assert!(
            relative == Path::new("manifest.json")
                || (relative.starts_with("index")
                    && relative.extension().is_some_and(|value| value == "json")),
            "unexpected retained run file: {}",
            relative.display()
        );
    }
}

#[tokio::test]
async fn mock_exec_phase7_writes_file_store_allocation() {
    let temp = tempfile::tempdir().unwrap();
    let store_root = temp.path().join("store");

    let result = exec::run(test_args(temp.path(), store_root.clone(), 7))
        .await
        .unwrap();

    assert_eq!(
        result["run_state"]["trader_investment_plan"]["per_ticker"]["QQQ"]["action"],
        "Hold"
    );
    assert_eq!(
        result["run_state"]["final_trade_decision"]["per_asset"]["QQQ"]["rating"],
        "Hold"
    );
    assert_eq!(result["portfolio_allocation"]["total_equity_exposure"], 0.0);
    assert_eq!(
        result["portfolio_allocation"]["weights"]["cash_hedge"]["weight"],
        1.0
    );
    assert!(has_phase_index(&store_root, &result, 7));

    let state = &result["run_state"];
    assert_eq!(state["phase_status"]["7"], "done");
    assert_eq!(
        state["_completed_units"]["p4:trader:artifact:aggregate:none:0"]["profile"],
        "trade_intent"
    );
    assert_eq!(
        state["_completed_units"]["p6:portfolio.manager:artifact:aggregate:none:0"]["profile"],
        "portfolio_decision"
    );
}

#[tokio::test]
async fn partial_run_stays_running_and_retains_only_temporary_recovery_files() {
    let temp = tempfile::tempdir().unwrap();
    let store_root = temp.path().join("store");

    let result = exec::run(test_args(temp.path(), store_root.clone(), 7))
        .await
        .unwrap();

    let store = FileStore::open(&store_root, FileStoreOptions::default()).unwrap();
    let run = RunLocation::new("2026-06-15", result["run_id"].as_str().unwrap()).unwrap();
    let manifest = read_run_manifest(&store, &run).unwrap();
    assert_eq!(manifest.status, RunStatus::Running);
    assert!(store.exists(&run.state_relative()).unwrap());
    assert!(store
        .exists(&run.child_relative(Path::new("sessions")).unwrap())
        .unwrap());
    assert_eq!(result["store_compaction"]["eligible"], false);
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
    let store = FileStore::open(&store_root, FileStoreOptions::default()).unwrap();
    let run = RunLocation::new("2026-06-15", result["run_id"].as_str().unwrap()).unwrap();
    let final_index = read_indexes(
        &store,
        Some(&run),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(8),
            limit: 10,
            ..Default::default()
        },
    )
    .unwrap()
    .indexes
    .into_iter()
    .next()
    .expect("phase 8 final Index");
    assert_eq!(
        final_index.authoritative_fields["portfolio_allocation"]["total_equity_exposure"],
        0.0
    );
    assert!(
        !has_directory_named(&store_root.join("runs"), "decision"),
        "mock must not publish legacy Decision records"
    );
    assert!(
        !store_root.join("knowledge/evaluation").exists(),
        "mock must not publish canonical evaluation data"
    );
}

fn has_phase_index(store_root: &Path, result: &serde_json::Value, phase: u8) -> bool {
    let store = FileStore::open(store_root, FileStoreOptions::default()).unwrap();
    let run = RunLocation::new("2026-06-15", result["run_id"].as_str().unwrap()).unwrap();
    !read_indexes(
        &store,
        Some(&run),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(phase),
            limit: 10,
            ..Default::default()
        },
    )
    .unwrap()
    .indexes
    .is_empty()
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
        provider_contract: false,
        submit_orders: false,
        run_purpose: None,
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
        "think_tool": false
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

fn has_file_with_extension(root: &Path, extension: &str) -> bool {
    walk(root).iter().any(|path| {
        path.is_file()
            && path
                .extension()
                .is_some_and(|candidate| candidate == extension)
    })
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
