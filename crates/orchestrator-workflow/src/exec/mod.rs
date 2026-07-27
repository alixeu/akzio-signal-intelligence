use anyhow::{bail, Context, Result};
use chrono::{Local, NaiveDate, Utc};
use orchestrator_core::{
    config_int, config_str, config_strings, display_ticker, load_config, parse_tickers,
    project_path,
};
use orchestrator_store::{
    content_hash, read_run_manifest, write_learning_record, write_run_manifest, FileStore,
    FileStoreOptions, LearningKind, LearningRecord, RunLocation, RunManifest, RunManifestInit,
    RunStatus,
};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};

use crate::orchestration::{
    allocation::{compute_allocation_context, derive_guarded_allocation},
    config::RuntimeConfig,
    input_snapshot_runtime::{capture_phase1_file_store_inputs, phase1_input_sources},
    lifecycle::{run_id_for, set_phase_status, tickers_from_state},
    retrieval::inject_phase_summary_reflection,
    role_jobs::{prepare_role_job, record_role_job_metrics, run_role_jobs, RoleRun},
    summary_store::{planned_summary_units, write_deterministic_phase_summary},
};

mod args;
pub use args::*;

const STATE_SCHEMA_VERSION: u32 = 1;

pub async fn run(args: ExecArgs) -> Result<Value> {
    validate_args(&args)?;
    let current_date = args
        .date
        .clone()
        .unwrap_or_else(|| Local::now().date_naive().to_string());
    NaiveDate::parse_from_str(&current_date, "%Y-%m-%d")
        .with_context(|| format!("invalid --date value {current_date:?}"))?;
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| project_path("config/config.yaml"));
    let config = if args.config.is_some() {
        load_config(Some(&config_path))
            .with_context(|| format!("failed to load config from {}", config_path.display()))?
    } else {
        load_config(Some(&config_path)).unwrap_or_else(|_| json!({}))
    };
    let tickers =
        parse_tickers(config_strings(&config, "orchestrator.analysis_universe", &[]).join(","));
    if tickers.is_empty() {
        bail!("orchestrator.analysis_universe is required in config")
    }
    let runtime = RuntimeConfig::from_value(&config)?;
    let store_root = runtime.store.resolve_root(args.store_root.as_deref())?;
    let run_id = run_id_for(&tickers, &current_date);
    let location = RunLocation::new(current_date.clone(), run_id.clone())?;
    let store = FileStore::open(
        &store_root,
        FileStoreOptions {
            atomic_fsync: runtime.store.atomic_fsync,
            stale_temp_age: Some(Duration::from_secs(runtime.store.stale_temp_age_sec)),
        },
    )?;
    let mut manifest = prepare_manifest(&store, &location, &runtime, &config)?;

    let initial_state = json!({
        "schema_version": STATE_SCHEMA_VERSION,
        "run_id": run_id,
        "current_date": current_date,
        "ticker": display_ticker(&tickers),
        "tickers": tickers,
        "analysis_universe": tickers,
        "investable_assets": runtime.allocation.investable_assets,
        "store_root": store.root(),
        "config": config,
        "mode": args.mode.as_str(),
        "lang": if args.lang == "zh" { config_str(&config, "orchestrator.runtime.lang", "zh") } else { args.lang.clone() },
        "window_days": args.window_days.unwrap_or_else(|| config_int(&config, "orchestrator.runtime.window_days", 150)),
        "mock": args.mock,
        "debug": args.debug,
        "phase_status": {},
        "degraded": false,
    });
    let mut state = load_or_initialize_state(&store, &location, initial_state)?;

    if args.from_phase <= 0 && args.to_phase >= 0 && !phase_completed(&manifest, 0) {
        run_phase0(&mut state, &runtime)?;
        finish_phase(&store, &location, &mut manifest, &mut state, 0, "done")?;
    }
    if args.from_phase <= 1 && args.to_phase >= 1 && !phase_completed(&manifest, 1) {
        run_phase1(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        finish_phase(&store, &location, &mut manifest, &mut state, 1, "done")?;
        summarize(
            &store_root,
            &mut state,
            &runtime,
            1,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
    }
    if args.from_phase <= 2 && args.to_phase >= 2 && !phase_completed(&manifest, 2) {
        run_phase2(
            &store,
            &location,
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        finish_phase(&store, &location, &mut manifest, &mut state, 2, "done")?;
        summarize(
            &store_root,
            &mut state,
            &runtime,
            2,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
    }
    if args.from_phase <= 3 && args.to_phase >= 3 && !phase_completed(&manifest, 3) {
        run_phase3(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        finish_phase(&store, &location, &mut manifest, &mut state, 3, "done")?;
        summarize(
            &store_root,
            &mut state,
            &runtime,
            3,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
    }
    if args.from_phase <= 4 && args.to_phase >= 4 && !phase_completed(&manifest, 4) {
        run_phase4(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        finish_phase(&store, &location, &mut manifest, &mut state, 4, "done")?;
        summarize(
            &store_root,
            &mut state,
            &runtime,
            4,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
    }
    if args.from_phase <= 5 && args.to_phase >= 5 && !phase_completed(&manifest, 5) {
        run_phase5(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        finish_phase(&store, &location, &mut manifest, &mut state, 5, "done")?;
        summarize(
            &store_root,
            &mut state,
            &runtime,
            5,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
    }
    if args.from_phase <= 6 && args.to_phase >= 6 && !phase_completed(&manifest, 6) {
        run_phase6(
            &mut state,
            &runtime,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
        finish_phase(&store, &location, &mut manifest, &mut state, 6, "done")?;
        summarize(
            &store_root,
            &mut state,
            &runtime,
            6,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
    }
    if args.from_phase <= 7 && args.to_phase >= 7 && !phase_completed(&manifest, 7) {
        run_phase7(&store, &location, &mut state, &runtime)?;
        finish_phase(&store, &location, &mut manifest, &mut state, 7, "done")?;
        summarize(
            &store_root,
            &mut state,
            &runtime,
            7,
            args.model.as_deref(),
            args.reasoning_effort.as_deref(),
        )
        .await?;
    }
    if args.from_phase <= 8 && args.to_phase >= 8 && !phase_completed(&manifest, 8) {
        run_phase8(&store, &location, &mut state)?;
        finish_phase(&store, &location, &mut manifest, &mut state, 8, "done")?;
    }

    manifest.status = RunStatus::Completed;
    manifest.degraded = state["degraded"].as_bool().unwrap_or(false);
    manifest.completed_at = Some(Utc::now().to_rfc3339());
    write_run_manifest(&store, &location, manifest)?;
    seal_state(&mut state)?;
    store.write_json_value(&location.child_relative(Path::new("state.json"))?, &state)?;

    Ok(json!({
        "run_id": state["run_id"],
        "date": state["current_date"],
        "store_root": store.root(),
        "debate_mode": "file_store",
        "degraded": state["degraded"],
        "rating": state.pointer("/research_plan/rating").cloned().unwrap_or(Value::Null),
        "action": state.pointer("/trader_investment_plan/intent/action").cloned().unwrap_or(Value::Null),
        "portfolio_allocation": state.get("portfolio_allocation").cloned().unwrap_or(Value::Null),
        "run_state": state,
    }))
}

fn validate_args(args: &ExecArgs) -> Result<()> {
    if args.from_phase > args.to_phase || args.to_phase > 8 {
        bail!("phase range must satisfy 0 <= from_phase <= to_phase <= 8")
    }
    Ok(())
}

fn prepare_manifest(
    store: &FileStore,
    location: &RunLocation,
    runtime: &RuntimeConfig,
    config: &Value,
) -> Result<RunManifest> {
    if store.exists(&location.manifest_relative())? {
        let manifest = read_run_manifest(store, location)?;
        let current_config_hash = content_hash(config)?;
        if manifest.config_hash != current_config_hash {
            bail!(
                "run {} was created with config hash {}; current config hash is {}; start a new run instead of silently reusing artifacts",
                manifest.run_id,
                manifest.config_hash,
                current_config_hash
            );
        }
        return Ok(manifest);
    }
    let snapshot = runtime.authority_registry.snapshot();
    write_run_manifest(
        store,
        location,
        RunManifest::new(RunManifestInit {
            location: location.clone(),
            workflow_version: format!("orchestrator-workflow-v{}", env!("CARGO_PKG_VERSION")),
            prompt_versions: runtime.prompts.versions.clone(),
            git_sha: option_env!("GIT_SHA").unwrap_or("unavailable").to_owned(),
            config_hash: content_hash(config)?,
            authority_registry_hash: snapshot.content_hash,
            created_at: Utc::now().to_rfc3339(),
        })?,
    )
    .map_err(Into::into)
}

fn load_or_initialize_state(
    store: &FileStore,
    location: &RunLocation,
    initial_state: Value,
) -> Result<Value> {
    let relative = location.child_relative(Path::new("state.json"))?;
    if !store.exists(&relative)? {
        return Ok(initial_state);
    }
    let mut state = store.read_json_value(&relative)?;
    let version = state
        .get("schema_version")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .context("run state schema_version is required")?;
    if version > STATE_SCHEMA_VERSION {
        bail!("run state schema version {version} is newer than supported {STATE_SCHEMA_VERSION}");
    }
    if version < STATE_SCHEMA_VERSION {
        bail!(
            "run state schema version {version} requires an explicit migration to {STATE_SCHEMA_VERSION}"
        );
    }
    let stored_hash = state
        .get("content_hash")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("run state content_hash is required")?
        .to_owned();
    state["content_hash"] = Value::String(String::new());
    let expected_hash = content_hash(&state)?;
    if stored_hash != expected_hash {
        bail!(
            "run state content_hash mismatch at {}: expected {expected_hash}, found {stored_hash}",
            store.root().join(relative).display()
        );
    }
    state["content_hash"] = Value::String(stored_hash);
    for field in ["run_id", "current_date", "ticker", "tickers", "config"] {
        if state.get(field) != initial_state.get(field) {
            bail!("existing run state field {field} differs from requested run; start a new run");
        }
    }
    Ok(state)
}

fn finish_phase(
    store: &FileStore,
    location: &RunLocation,
    manifest: &mut RunManifest,
    state: &mut Value,
    phase: u8,
    status: &str,
) -> Result<()> {
    set_phase_status(state, i64::from(phase), status);
    manifest.current_phase = phase;
    manifest.phase_status.insert(
        phase.to_string(),
        if status == "done" {
            orchestrator_store::PhaseStatus::Completed
        } else {
            orchestrator_store::PhaseStatus::Failed
        },
    );
    write_run_manifest(store, location, manifest.clone())?;
    Ok(())
}

fn phase_completed(manifest: &RunManifest, phase: u8) -> bool {
    manifest.phase_status.get(&phase.to_string())
        == Some(&orchestrator_store::PhaseStatus::Completed)
}

async fn summarize(
    store_root: &Path,
    state: &mut Value,
    runtime: &RuntimeConfig,
    phase: i64,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    if state["mock"].as_bool().unwrap_or(false) {
        write_deterministic_phase_summary(
            store_root,
            state,
            phase,
            runtime.tool_managed.max_summary_units_per_phase,
        )?;
        return Ok(());
    }
    let (source_payload, units) = planned_summary_units(
        state,
        phase,
        runtime.tool_managed.max_summary_units_per_phase,
    )?;
    let mut completed = Vec::with_capacity(units.len());
    for unit in units {
        state["_summary_unit"] = serde_json::to_value(&unit)?;
        state["_summary_source_payload"] = source_payload.clone();
        let artifact = run_unit(
            state,
            runtime,
            "compressor.phase_summary",
            phase,
            "phase_summary",
            None,
            unit.topic_id.as_deref(),
            unit.ticker.as_deref(),
            model,
            reasoning,
        )
        .await?;
        completed.push(artifact);
    }
    state["phase_summary_live"][phase.to_string()] = Value::Array(completed);
    state.as_object_mut().map(|object| {
        object.remove("_summary_unit");
        object.remove("_summary_source_payload");
    });
    Ok(())
}

fn run_phase0(state: &mut Value, runtime: &RuntimeConfig) -> Result<()> {
    inject_phase_summary_reflection(state, runtime)?;
    state["phase0"] = json!({
        "status": "completed",
        "reflection": "no eligible historical task was planned for this run",
    });
    Ok(())
}

async fn run_phase1(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    if !state["mock"].as_bool().unwrap_or(false) {
        let tickers = tickers_from_state(state);
        let sources = phase1_input_sources(
            state["current_date"]
                .as_str()
                .context("missing current_date")?,
            true,
            true,
            &tickers,
        )?;
        let input = capture_phase1_file_store_inputs(state, runtime, &sources)?;
        state["file_store_input"] = json!({
            "store_root": input.store_root,
            "run_id": input.run_id,
            "current_date": input.current_date,
        });
    }
    let roles = ["analyst.technical", "analyst.news_macro"];
    let mut reports = serde_json::Map::new();
    for role in roles {
        for ticker in tickers_from_state(state) {
            let artifact = run_unit(
                state,
                runtime,
                role,
                1,
                "artifact",
                None,
                None,
                Some(&ticker),
                model,
                reasoning,
            )
            .await?;
            let entry = reports
                .entry(role.to_owned())
                .or_insert_with(|| json!({"role": role, "per_ticker": {}}));
            entry["per_ticker"][ticker] = artifact
                .get("per_ticker")
                .and_then(|items| items.get(&ticker))
                .cloned()
                .unwrap_or(artifact.clone());
        }
    }
    state["analyst_reports"] = Value::Object(reports);
    state["phase1_index"] = json!({"per_ticker": phase1_index(state), "authority": "file_store"});
    state["weighted_probability_base"] = weighted_probability_base(state);
    Ok(())
}

async fn run_phase2(
    store: &FileStore,
    location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let warmup = run_unit(
        state,
        runtime,
        "mediator.topic",
        2,
        "warmup",
        Some(0),
        None,
        None,
        model,
        reasoning,
    )
    .await?;
    record_phase2_session(state, "mediator.topic", "warmup", None, None, Some(0));
    state["phase2_warmup"] = warmup;
    let generated = run_unit(
        state,
        runtime,
        "mediator.topic",
        2,
        "topic_generation",
        None,
        None,
        None,
        model,
        reasoning,
    )
    .await?;
    record_phase2_session(
        state,
        "mediator.topic",
        "topic_generation",
        None,
        None,
        None,
    );
    let topics = generated
        .pointer("/payload/topics")
        .or_else(|| generated.get("topics"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    let topic_generation_session =
        runtime_session_for(state, "mediator.topic", "topic_generation", None, None);
    state["topic_generation_session_id"] = topic_generation_session["session_id"].clone();
    state["topic_generation_turn_id"] = topic_generation_session["turn_id"].clone();
    let actionable = topics.as_array().is_some_and(|items| !items.is_empty());
    state["topic_generation_artifact"] =
        json!({"artifact": generated, "topics": topics, "actionable": actionable});

    let mut controllers = serde_json::Map::new();
    for topic in topics.as_array().into_iter().flatten() {
        let topic_id = topic
            .get("topic_id")
            .and_then(Value::as_str)
            .context("Phase 2 topic generation returned a topic without topic_id")?
            .to_owned();
        state["topic_debate_states"][&topic_id] = json!({"topic": topic, "turns": []});
        for (role, kind, side) in [
            ("researcher.bull.initial", "bull_seed", "bull"),
            ("researcher.bear.initial", "bear_seed", "bear"),
        ] {
            let seed = run_unit(
                state,
                runtime,
                role,
                2,
                kind,
                Some(0),
                Some(&topic_id),
                None,
                model,
                reasoning,
            )
            .await?;
            record_phase2_session(state, role, kind, Some(&topic_id), Some(side), Some(0));
            state["topic_debate_states"][&topic_id]["turns"]
                .as_array_mut()
                .expect("topic turns initialized")
                .push(json!({"role":role, "artifact": seed}));
        }
        for (role, side) in [
            ("researcher.bull.interaction", "bull"),
            ("researcher.bear.interaction", "bear"),
        ] {
            let response = run_unit(
                state,
                runtime,
                role,
                2,
                "interaction",
                Some(1),
                Some(&topic_id),
                None,
                model,
                reasoning,
            )
            .await?;
            record_phase2_session(
                state,
                role,
                "interaction",
                Some(&topic_id),
                Some(side),
                Some(1),
            );
            state["topic_debate_states"][&topic_id]["turns"]
                .as_array_mut()
                .expect("topic turns initialized")
                .push(json!({"role":role, "artifact": response}));
        }
        let controller = run_unit(
            state,
            runtime,
            "mediator.topic_controller",
            2,
            "topic_control",
            Some(1),
            Some(&topic_id),
            None,
            model,
            reasoning,
        )
        .await?;
        state["topic_debate_states"][&topic_id]["controller_artifact"] = controller.clone();
        controllers.insert(topic_id, controller);
    }
    let reducer = json!({
        "schema_version": 1,
        "artifact_id": format!("phase2-final-reducer-{}", &content_hash(&Value::Object(controllers.clone()))?[7..31]),
        "run_id": state["run_id"],
        "phase": 2,
        "role": "rust.phase2_final_reducer",
        "controllers": controllers,
        "created_at": Utc::now().to_rfc3339(),
        "content_hash": "",
    });
    let mut reducer = reducer;
    reducer["content_hash"] = json!(content_hash(&reducer)?);
    store.write_json_value(
        &location.child_relative(Path::new("artifacts/phase2/final-reducer.json"))?,
        &reducer,
    )?;
    state["debate_state_artifact"] = json!({
        "status": "completed",
        "topic_briefs": state["topic_generation_artifact"]["topics"],
        "final_reducer": reducer,
        "authority": "file_store"
    });
    Ok(())
}

async fn run_phase3(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let artifact = run_unit(
        state,
        runtime,
        "manager.research",
        3,
        "artifact",
        None,
        None,
        None,
        model,
        reasoning,
    )
    .await?;
    state["research_plan"] = artifact;
    Ok(())
}

async fn run_phase4(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let mut per_ticker = serde_json::Map::new();
    for ticker in tickers_from_state(state) {
        let artifact = run_unit(
            state,
            runtime,
            "trader",
            4,
            "artifact",
            None,
            None,
            Some(&ticker),
            model,
            reasoning,
        )
        .await?;
        per_ticker.insert(ticker, artifact);
    }
    state["trader_investment_plan"] = json!({"per_ticker": per_ticker});
    Ok(())
}

async fn run_phase5(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let mut history = Vec::new();
    for role in ["risk.aggressive", "risk.neutral", "risk.conservative"] {
        for ticker in tickers_from_state(state) {
            history.push(
                run_unit(
                    state,
                    runtime,
                    role,
                    5,
                    "artifact",
                    None,
                    None,
                    Some(&ticker),
                    model,
                    reasoning,
                )
                .await?,
            );
        }
    }
    state["risk_debate_state"] = json!({"history": history, "authority": "file_store"});
    Ok(())
}

async fn run_phase6(
    state: &mut Value,
    runtime: &RuntimeConfig,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<()> {
    let assets = if runtime.allocation.investable_assets.is_empty() {
        tickers_from_state(state)
    } else {
        runtime.allocation.investable_assets.clone()
    };
    let mut per_asset = serde_json::Map::new();
    for ticker in assets {
        let artifact = run_unit(
            state,
            runtime,
            "portfolio.manager",
            6,
            "artifact",
            None,
            None,
            Some(&ticker),
            model,
            reasoning,
        )
        .await?;
        per_asset.insert(ticker, artifact);
    }
    state["final_trade_decision"] = json!({"per_asset": per_asset, "authority": "file_store"});
    Ok(())
}

fn run_phase7(
    store: &FileStore,
    location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
) -> Result<()> {
    let context = compute_allocation_context(state, &runtime.allocation)?;
    let allocation = derive_guarded_allocation(state, &context, &runtime.allocation)
        .unwrap_or_else(|error| {
            json!({
                "weights": {"cash_hedge": {"weight": 1.0, "rationale": error.to_string()}},
                "total_equity_exposure": 0.0,
                "allocation_method": "fallback_cash",
            })
        });
    state["allocation_context"] = context;
    state["portfolio_allocation"] = allocation.clone();
    store.write_json_value(
        &location.child_relative(Path::new("artifacts/phase7/allocation.json"))?,
        &json!({
            "schema_version": 1,
            "run_id": state["run_id"],
            "phase": 7,
            "role": "rust.allocation",
            "allocation": allocation,
            "created_at": Utc::now().to_rfc3339(),
        }),
    )?;
    Ok(())
}

fn run_phase8(store: &FileStore, location: &RunLocation, state: &mut Value) -> Result<()> {
    for ticker in tickers_from_state(state) {
        let record = LearningRecord {
            schema_version: orchestrator_store::LEARNING_RECORD_SCHEMA_VERSION,
            kind: LearningKind::Decision,
            run_id: state["run_id"].as_str().unwrap_or_default().to_owned(),
            ticker,
            source_run_id: None,
            payload: json!({"portfolio_allocation": state["portfolio_allocation"], "phase": 8}),
            created_at: Utc::now().to_rfc3339(),
            content_hash: String::new(),
        };
        write_learning_record(store, location, LearningKind::Decision, record)?;
    }
    state["phase8"] = json!({"status": "completed", "archive": "file_store"});
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_unit(
    state: &mut Value,
    runtime: &RuntimeConfig,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    ticker: Option<&str>,
    model: Option<&str>,
    reasoning: Option<&str>,
) -> Result<Value> {
    let mut scoped = state.clone();
    if let Some(ticker) = ticker {
        scoped["ticker"] = json!(ticker);
        scoped["tickers"] = json!([ticker]);
    }
    let prompt_path = runtime.prompts.path_for(role).cloned();
    let job = prepare_role_job(RoleRun {
        state: scoped,
        role,
        phase,
        kind,
        round,
        topic_id,
        mock: state["mock"].as_bool().unwrap_or(false),
        model_override: model,
        reasoning_effort_override: reasoning,
        config: runtime,
        prompt_path: prompt_path.as_deref(),
    })?;
    let result = run_role_jobs(vec![job], 1, runtime.workflow.agent_timeout_sec)
        .await
        .into_iter()
        .next()
        .context("ToolManaged role produced no result")?;
    record_role_job_metrics(state, &result);
    let artifact = result.artifact.with_context(|| {
        format!(
            "ToolManaged role {role} ended without terminal finalize: {}",
            result.error.unwrap_or_else(|| "unknown error".to_owned())
        )
    })?;
    let session_id = if result.session_id.is_empty() {
        format!(
            "{}:p{}:{}:{}:{}:{}",
            state["run_id"].as_str().unwrap_or_default(),
            phase,
            role,
            phase2_profile_name(role, kind),
            topic_id.unwrap_or("aggregate"),
            round.unwrap_or(0)
        )
    } else {
        result.session_id
    };
    let turn_id = if result.turn_id.is_empty() {
        "mock-finalize".to_owned()
    } else {
        result.turn_id
    };
    state["_runtime_sessions"][runtime_session_key(role, kind, topic_id, round)] =
        json!({"session_id": session_id, "turn_id": turn_id});
    Ok(artifact)
}

fn runtime_session_key(
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    round: Option<i64>,
) -> String {
    format!(
        "{role}:{kind}:{}:{}",
        topic_id.unwrap_or("aggregate"),
        round.unwrap_or(0)
    )
}

fn runtime_session_for(
    state: &Value,
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    round: Option<i64>,
) -> Value {
    state["_runtime_sessions"]
        .get(runtime_session_key(role, kind, topic_id, round))
        .cloned()
        .unwrap_or(Value::Null)
}

fn phase2_profile_name(role: &str, kind: &str) -> &'static str {
    match (role, kind) {
        ("mediator.topic", "warmup") => "researcher_warmup",
        ("mediator.topic", "topic_generation") => "topic_generation",
        ("researcher.bull.initial" | "researcher.bear.initial", _) => "debate_seed",
        ("researcher.bull.interaction" | "researcher.bear.interaction", _) => "debate_response",
        ("mediator.topic_controller", _) => "topic_control",
        ("manager.research", _) => "research_decision",
        ("trader", _) => "trade_intent",
        ("risk.aggressive" | "risk.neutral" | "risk.conservative", _) => "risk_review",
        ("portfolio.manager", _) => "portfolio_decision",
        _ => "analyst_report",
    }
}

fn record_phase2_session(
    state: &mut Value,
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    side: Option<&str>,
    round: Option<i64>,
) {
    let session = runtime_session_for(state, role, kind, topic_id, round);
    if kind == "warmup" {
        state["phase2_warmup"]["session_id"] = session["session_id"].clone();
        state["phase2_warmup"]["turn_id"] = session["turn_id"].clone();
        return;
    }
    let (Some(topic_id), Some(side)) = (topic_id, side) else {
        return;
    };
    state["phase2_file_store_sessions"][topic_id][side] = session;
}

fn phase1_index(state: &Value) -> serde_json::Map<String, Value> {
    tickers_from_state(state)
        .into_iter()
        .map(|ticker| {
            let roles = state["analyst_reports"]
                .as_object()
                .map(|reports| {
                    reports
                        .iter()
                        .filter_map(|(role, report)| {
                            report
                                .pointer(&format!("/per_ticker/{ticker}"))
                                .map(|value| json!({"role": role, "artifact": value}))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            (
                ticker,
                json!({"role_summaries": roles, "evidence_quality": "tool_managed"}),
            )
        })
        .collect()
}

fn weighted_probability_base(state: &Value) -> Value {
    let values = tickers_from_state(state)
        .into_iter()
        .map(|ticker| (ticker, json!({"long_probability": 0.5, "short_probability": 0.5, "source": "phase1_tool_managed"})))
        .collect::<serde_json::Map<_, _>>();
    Value::Object(values)
}

fn seal_state(state: &mut Value) -> Result<()> {
    state["content_hash"] = Value::String(String::new());
    state["content_hash"] = Value::String(content_hash(state)?);
    Ok(())
}
