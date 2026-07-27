use anyhow::{Context, Result};
use futures::{stream, StreamExt};
use orchestrator_core::{default_project_root, ArtifactAuthority, ToolManagedProfile};
use orchestrator_llm::{
    agent_loop::{ModelStreamResult, RetrievalPolicy, TokenUsage},
    llm_judge::JudgeConfig,
    mock_role_artifact, run_agent_loop_with_metrics, run_agent_steer_loop_with_metrics,
    tools::{ExternalToolConfig, FileStoreInputSnapshot},
    truncation::TruncationConfig,
    AgentLoopOutput, AgentSettings, OutputMode, RoleLlmSettings, SteerLoopInput,
};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};
use tokio::time;
use tracing::{debug, warn};

use super::config::{output_mode_for_role, prompt_version, RetrievalConfig, RuntimeConfig};
use super::degraded::role_artifact_or_degraded;
use super::domain_runtime::{file_store_domain_runtime, FileStoreDomainRuntimePlan};
use super::lifecycle::tickers_from_state;
use super::render::{direct_context_manifest, render_prompt_with_plugins};

pub(crate) struct RoleRun<'a> {
    pub state: Value,
    pub role: &'a str,
    pub phase: i64,
    pub kind: &'a str,
    pub round: Option<i64>,
    pub topic_id: Option<&'a str>,
    pub mock: bool,
    pub model_override: Option<&'a str>,
    pub reasoning_effort_override: Option<&'a str>,
    pub config: &'a RuntimeConfig,
    pub prompt_path: Option<&'a std::path::Path>,
}

pub(crate) struct SteerRoleRun<'a> {
    pub state: Value,
    pub role: &'a str,
    pub phase: i64,
    pub kind: &'a str,
    pub round: Option<i64>,
    pub topic_id: Option<&'a str>,
    pub mock: bool,
    pub model_override: Option<&'a str>,
    pub reasoning_effort_override: Option<&'a str>,
    pub config: &'a RuntimeConfig,
    pub prompt_path: Option<&'a std::path::Path>,
    pub session_id: String,
    pub turn_id: String,
    pub steer: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RoleJob {
    pub role: String,
    pub phase: i64,
    pub kind: String,
    pub round: Option<i64>,
    pub topic_id: Option<String>,
    pub mock: bool,
    pub debug: bool,
    pub prompt: String,
    pub prompt_path: Option<String>,
    pub debug_output_path: Option<PathBuf>,
    pub prompt_version: Option<String>,
    pub tickers: Vec<String>,
    pub output_mode: OutputMode,
    pub tool_managed_profile: Option<ToolManagedProfile>,
    pub domain_tool_runtime:
        Option<orchestrator_llm::tools::domain_tools::DomainToolRuntimeBinding>,
    pub llm: Option<RoleLlmSettings>,
    pub reasoning_effort_override: Option<String>,
    pub tools: ExternalToolConfig,
    pub web_search: orchestrator_llm::web_search::WebSearchConfig,
    pub truncation: TruncationConfig,
    pub judge: JudgeConfig,
    pub retrieval_policy: RetrievalPolicy,
    pub context_manifest: Value,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct RoleJobResult {
    pub role: String,
    pub phase: i64,
    pub kind: String,
    pub round: Option<i64>,
    pub topic_id: Option<String>,
    pub tickers: Vec<String>,
    pub prompt_version: Option<String>,
    pub model: String,
    pub turn_id: String,
    pub session_id: String,
    pub artifact: Option<Value>,
    pub error: Option<String>,
    pub timed_out: bool,
    pub elapsed_ms: u128,
    /// Time spent waiting on the LLM API (sum of model iterations).
    pub llm_ms: u128,
    /// Time spent running tools invoked by the LLM.
    pub tool_ms: u128,
    pub usage: TokenUsage,
    pub turn_count: u64,
    pub tool_call_count: u64,
}

impl RoleJobResult {
    /// Orchestration / idle wait: total - llm - tool.
    pub fn wait_ms(&self) -> u128 {
        self.elapsed_ms
            .saturating_sub(self.llm_ms.saturating_add(self.tool_ms))
    }
}

fn prompt_version_for_role(state: &Value, role: &str, kind: &str) -> Option<String> {
    let config = state.get("config")?;
    if role == "mediator.topic" && kind == "warmup" {
        return Some(prompt_version(config, "orchestrator.prompts.phase2.warmup"));
    }
    let prompt_key = match role {
        "reflector.historical" => "orchestrator.prompts.reflection.historical",
        "analyst.technical" => "orchestrator.prompts.analyst.technical",
        "analyst.news_macro" => "orchestrator.prompts.analyst.news_macro",
        "compressor.phase_summary" => "orchestrator.prompts.compressor.phase_summary",
        "mediator.topic" => "orchestrator.prompts.phase2.topic_generator",
        "researcher.bull.initial" => "orchestrator.prompts.phase2.bull_initial",
        "researcher.bull.interaction" => "orchestrator.prompts.phase2.bull_interaction",
        "researcher.bear.initial" => "orchestrator.prompts.phase2.bear_initial",
        "researcher.bear.interaction" => "orchestrator.prompts.phase2.bear_interaction",
        "mediator.topic_controller" => "orchestrator.prompts.mediator.topic_controller",
        "manager.research" => "orchestrator.prompts.manager.research",
        "trader" => "orchestrator.prompts.trader",
        "risk.aggressive" => "orchestrator.prompts.risk.aggressive",
        "risk.neutral" => "orchestrator.prompts.risk.neutral",
        "risk.conservative" => "orchestrator.prompts.risk.conservative",
        "portfolio.manager" => "orchestrator.prompts.portfolio.manager",
        _ => return None,
    };
    Some(prompt_version(config, prompt_key))
}

fn tool_managed_profile_for_role_kind(role: &str, kind: &str) -> Option<ToolManagedProfile> {
    match role {
        "reflector.historical" => Some(ToolManagedProfile::HistoricalReflection),
        "analyst.technical" | "analyst.news_macro" => Some(ToolManagedProfile::AnalystReport),
        "mediator.topic" if kind == "warmup" => Some(ToolManagedProfile::ResearcherWarmup),
        "mediator.topic" => Some(ToolManagedProfile::TopicGeneration),
        "researcher.bull.initial" | "researcher.bear.initial" => {
            Some(ToolManagedProfile::DebateSeed)
        }
        "researcher.bull.interaction" | "researcher.bear.interaction" => {
            Some(ToolManagedProfile::DebateResponse)
        }
        "mediator.topic_controller" => Some(ToolManagedProfile::TopicControl),
        "manager.research" => Some(ToolManagedProfile::ResearchDecision),
        "trader" => Some(ToolManagedProfile::TradeIntent),
        "risk.aggressive" | "risk.neutral" | "risk.conservative" => {
            Some(ToolManagedProfile::RiskReview)
        }
        "portfolio.manager" => Some(ToolManagedProfile::PortfolioDecision),
        "compressor.phase_summary" => Some(ToolManagedProfile::PhaseSummary),
        _ => None,
    }
}

/// Only structured evidence IDs may cross into a DomainTool scope.  Until the
/// FileStore Session adapter records an `evidence_read` event, live roles get
/// an empty set and a write that cites an invented ID fails closed.  Mock uses
/// deterministic IDs so it can exercise the same Draft/finalize path.
fn visible_domain_evidence_refs(
    state: &Value,
    role: &str,
    tickers: &[String],
    mock: bool,
) -> BTreeSet<String> {
    let mut visible = state
        .get("visible_evidence_refs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|reference| !reference.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    if mock {
        visible.extend(tickers.iter().map(|ticker| format!("mock:{role}:{ticker}")));
    }
    visible
}

pub(crate) fn prepare_role_job(input: RoleRun<'_>) -> Result<RoleJob> {
    let RoleRun {
        state,
        role,
        phase,
        kind,
        round,
        topic_id,
        mock,
        model_override,
        reasoning_effort_override,
        config,
        prompt_path,
    } = input;
    let debug_enabled = state.get("debug").and_then(Value::as_bool).unwrap_or(false);
    // LLM roles never receive account or order submission tools. Runtime-owned
    // Phase 7 execution uses a separate, fail-closed gate after allocation.
    let alpaca_live = false;
    let alpaca_market_data = role == "analyst.news_macro" && !mock && !debug_enabled;
    let tickers = tickers_from_state(&state);
    let tool_tickers = if role == "portfolio.manager" {
        state
            .get("investable_assets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect()
    } else {
        tickers.clone()
    };
    let prompt_version = prompt_version_for_role(&state, role, kind);
    let prompt = if mock {
        String::new()
    } else {
        render_prompt_with_plugins(
            &state,
            role,
            phase,
            kind,
            round,
            topic_id,
            prompt_path,
            Some(&config.component_plugins),
        )?
    };
    let llm = if mock {
        None
    } else {
        let mut llm = config
            .llm_roles
            .get(role)
            .or_else(|| {
                // Live phase_summary compressor reuses research-manager LLM defaults when not configured.
                if role == "compressor.phase_summary" {
                    config.llm_roles.get("manager.research")
                } else {
                    None
                }
            })
            .with_context(|| format!("missing LLM config for role {role:?}"))?
            .clone();
        if let Some(model) = model_override.filter(|value| !value.trim().is_empty()) {
            llm.model = model.to_string();
        }
        Some(llm)
    };
    debug!(
        role,
        phase,
        kind,
        round,
        topic_id,
        mock,
        debug = debug_enabled,
        prompt_path = prompt_path.map(|path| path.display().to_string()),
        prompt_version,
        prompt_chars = prompt.len(),
        "prepared role job"
    );
    let candidate_tool_managed_profile = tool_managed_profile_for_role_kind(role, kind);
    let (output_mode, tool_managed_profile, domain_tool_runtime, file_store_input) =
        if let Some(profile) = candidate_tool_managed_profile {
            match config.authority_registry.authority_for(role, profile)? {
                // A legacy role must not carry any ToolManaged profile into the
                // LLM settings: that would create an invalid mixed authority
                // contract even before its first tool call.
                ArtifactAuthority::Legacy => (output_mode_for_role(role), None, None, None),
                ArtifactAuthority::FileStore => {
                    let registration = config.authority_registry.registration(role, profile)?;
                    let store_root = state
                        .get("store_root")
                        .and_then(Value::as_str)
                        .context("store_root missing for migrated ToolManaged domain role")?;
                    let visible = visible_domain_evidence_refs(&state, role, &tickers, mock);
                    let binding = file_store_domain_runtime(
                        Path::new(store_root),
                        &state,
                        FileStoreDomainRuntimePlan {
                            role: role.to_owned(),
                            phase,
                            profile,
                            profile_version: registration.profile_version,
                            builder_version: registration.builder_version,
                            tickers: tickers.clone(),
                            visible_evidence_refs: visible,
                            topic_id: topic_id.map(ToOwned::to_owned),
                            side: phase2_side_for_role(role).map(ToOwned::to_owned),
                            round: round.and_then(|value| u32::try_from(value).ok()),
                            visible_claims: visible_phase2_claims(&state, topic_id),
                            fork: phase2_fork_reference(&state, role, topic_id),
                        },
                    )?;
                    let input = if profile == ToolManagedProfile::AnalystReport && !mock {
                        Some(file_store_input_from_state(&state)?)
                    } else {
                        None
                    };
                    (OutputMode::ToolManaged, Some(profile), Some(binding), input)
                }
            }
        } else {
            (output_mode_for_role(role), None, None, None)
        };

    Ok(RoleJob {
        role: role.to_string(),
        phase,
        kind: kind.to_string(),
        round,
        topic_id: topic_id.map(ToString::to_string),
        mock,
        debug: debug_enabled,
        prompt,
        prompt_path: prompt_path.map(|path| path.display().to_string()),
        debug_output_path: phase2_debug_output_path(phase, role, kind, topic_id),
        prompt_version,
        tickers: tickers.clone(),
        output_mode,
        tool_managed_profile,
        domain_tool_runtime,
        llm,
        reasoning_effort_override: reasoning_effort_override.map(ToString::to_string),
        tools: ExternalToolConfig {
            project_root: default_project_root(),
            db_path: state
                .get("db_path")
                .and_then(Value::as_str)
                .map(PathBuf::from),
            run_dir: state
                .get("run_dir")
                .and_then(Value::as_str)
                .map(PathBuf::from),
            run_id: state
                .get("run_id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            phase: Some(phase),
            allowed_reflection_task_ids: state
                .pointer("/phase0/tasks")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|task| task.get("task_id").and_then(Value::as_i64))
                .collect(),
            phase_summary_page_limit: config.retrieval.summary_page_limit,
            phase_summary_detail_page_limit: config.retrieval.detail_page_limit,
            tickers: tool_tickers,
            alpaca_live,
            alpaca_market_data,
            alpaca_api_key: if alpaca_live || alpaca_market_data {
                config.alpaca_api_key.clone()
            } else {
                None
            },
            alpaca_api_secret: if alpaca_live || alpaca_market_data {
                config.alpaca_api_secret.clone()
            } else {
                None
            },
            phase_summary_index: state.get("phase_summary_memory").map(|raw| {
                std::sync::Arc::new(orchestrator_sql::PhaseSummaryMemoryIndex::from_state_value(
                    raw,
                ))
            }),
            phase_summary_gate: state
                .get("run_id")
                .and_then(Value::as_str)
                .and_then(orchestrator_sql::phase_summary_gate),
            file_store_input,
        },
        web_search: config.web_search.get(role).cloned().unwrap_or_default(),
        truncation: config.truncation.clone(),
        judge: config.judge.clone(),
        retrieval_policy: retrieval_policy_for_role(role, kind, &config.retrieval),
        context_manifest: direct_context_manifest(&state, phase),
    })
}

fn phase2_side_for_role(role: &str) -> Option<&'static str> {
    if role.contains(".bull.") {
        Some("bull")
    } else if role.contains(".bear.") {
        Some("bear")
    } else {
        None
    }
}

fn visible_phase2_claims(state: &Value, topic_id: Option<&str>) -> BTreeSet<String> {
    let Some(topic_id) = topic_id else {
        return BTreeSet::new();
    };
    state
        .pointer(&format!("/topic_debate_states/{topic_id}/turns"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|turn| {
            turn.pointer("/artifact/payload/claims")
                .or_else(|| turn.pointer("/artifact/claims"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|claim| claim.get("claim_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect()
}

fn phase2_fork_reference(
    state: &Value,
    role: &str,
    topic_id: Option<&str>,
) -> Option<orchestrator_store::ForkReference> {
    let (session_id, turn_id) = if role == "mediator.topic_controller" {
        let _ = topic_id?;
        (
            state
                .get("topic_generation_session_id")?
                .as_str()?
                .to_owned(),
            state.get("topic_generation_turn_id")?.as_str()?.to_owned(),
        )
    } else if matches!(role, "researcher.bull.initial" | "researcher.bear.initial") {
        let _ = topic_id?;
        let warmup = state.get("phase2_warmup")?;
        (
            warmup.get("session_id")?.as_str()?.to_owned(),
            warmup.get("turn_id")?.as_str()?.to_owned(),
        )
    } else {
        return None;
    };
    Some(orchestrator_store::ForkReference {
        fork_from_session_id: session_id,
        fork_from_turn_id: turn_id,
    })
}

fn file_store_input_from_state(state: &Value) -> Result<FileStoreInputSnapshot> {
    let input = state
        .get("file_store_input")
        .and_then(Value::as_object)
        .context("migrated Phase 1 role requires a captured FileStore input manifest")?;
    let required = |field: &str| {
        input
            .get(field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .with_context(|| format!("file_store_input.{field} is required"))
    };
    Ok(FileStoreInputSnapshot {
        store_root: PathBuf::from(required("store_root")?),
        run_id: required("run_id")?,
        current_date: required("current_date")?,
    })
}

fn retrieval_policy_for_role(role: &str, kind: &str, config: &RetrievalConfig) -> RetrievalPolicy {
    let policy = |required_source_phases: &[i64],
                  required_detail_source_phases: &[i64],
                  minimum: usize,
                  maximum: usize| RetrievalPolicy {
        mandatory_summary_query: true,
        required_source_phases: required_source_phases.to_vec(),
        required_detail_source_phases: required_detail_source_phases.to_vec(),
        minimum_detail_expansions: minimum,
        maximum_detail_expansions: maximum,
        summary_page_limit: config.summary_page_limit,
        detail_page_limit: config.detail_page_limit,
        allow_empty_when_no_visible_summary: true,
        allowed_direct_contexts: vec![
            "rust_control_plane".to_string(),
            "current_phase_packet".to_string(),
            "current_task".to_string(),
        ],
    };
    match (role, kind) {
        ("reflector.historical", _) => policy(&[], &[], 1, config.reflection_max_details),
        ("mediator.topic", "warmup") => policy(&[1], &[], 0, 2),
        ("mediator.topic", _) => policy(&[1], &[], 0, config.phase2_max_details),
        ("researcher.bull.initial" | "researcher.bear.initial", _) => {
            policy(&[1], &[], 0, config.phase2_max_details)
        }
        ("researcher.bull.interaction" | "researcher.bear.interaction", _) => {
            policy(&[1], &[], 0, config.phase2_max_details)
        }
        ("mediator.topic_controller", _) => policy(&[1], &[], 0, config.phase2_max_details),
        ("manager.research", _) => policy(&[1, 2], &[], 1, config.phase3_max_details),
        ("trader", _) => policy(&[3], &[3], 1, config.phase4_max_details),
        ("risk.aggressive" | "risk.neutral" | "risk.conservative", _) => {
            policy(&[3, 4], &[3, 4], 2, config.phase5_max_details)
        }
        ("portfolio.manager", _) => policy(&[3, 4, 5], &[3, 4, 5], 3, config.phase6_max_details),
        _ => RetrievalPolicy::default(),
    }
}

fn phase2_debug_output_path(
    phase: i64,
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
) -> Option<PathBuf> {
    if phase != 2 {
        return None;
    }
    if role == "mediator.topic" {
        return Some(PathBuf::from(if kind == "warmup" {
            "outputs/debug/phase2/phase2-warmup-shared.json"
        } else {
            "outputs/debug/phase2/topic-generator.json"
        }));
    }
    let topic_id = topic_id?;
    let safe_topic_id: String = topic_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let topic_dir = if safe_topic_id.starts_with("topic-") || safe_topic_id.starts_with("topic_") {
        safe_topic_id
    } else {
        format!("topic-{safe_topic_id}")
    };
    let file = if role == "mediator.topic_controller" {
        "topic-controller.json"
    } else if role.contains(".bull.") {
        "debate-bull.json"
    } else if role.contains(".bear.") {
        "debate-bear.json"
    } else {
        return None;
    };
    Some(
        PathBuf::from("outputs/debug/phase2")
            .join(topic_dir)
            .join(file),
    )
}

pub(crate) async fn run_role_jobs(
    jobs: Vec<RoleJob>,
    parallelism: usize,
    timeout_sec: u64,
) -> Vec<RoleJobResult> {
    debug!(
        job_count = jobs.len(),
        parallelism = parallelism.max(1),
        timeout_sec,
        "running role jobs"
    );
    stream::iter(jobs)
        .map(|job| run_role_job_with_timeout(job, timeout_sec))
        .buffer_unordered(parallelism.max(1))
        .collect()
        .await
}

pub(crate) async fn run_single_role_job(
    input: RoleRun<'_>,
    timeout_sec: u64,
    config: &RuntimeConfig,
    state_for_degraded: &mut Value,
    conn: &rusqlite::Connection,
) -> Result<Value> {
    let result = run_single_role_job_result(input, timeout_sec, state_for_degraded, conn).await?;
    role_artifact_or_degraded(state_for_degraded, config, result)
}

/// Execute one role and preserve the raw result for a FileStore-authoritative
/// caller.  Such a caller must choose its typed degraded finalizer itself;
/// routing it through `role_artifact_or_degraded` would manufacture a legacy
/// JSON artifact after a terminal-tool failure and create a second authority.
pub(crate) async fn run_single_role_job_result(
    input: RoleRun<'_>,
    timeout_sec: u64,
    state_for_metrics: &mut Value,
    conn: &rusqlite::Connection,
) -> Result<RoleJobResult> {
    let job = prepare_role_job(input)?;
    let file_store_authoritative = job.tool_managed_profile.is_some();
    let result = run_role_job_with_timeout(job, timeout_sec).await;
    // This is currently a no-op, but keep it structurally impossible for a
    // migrated role to acquire an SQLite side effect when metrics return.
    if !file_store_authoritative {
        persist_prompt_metric(conn, &result);
    }
    record_role_job_metrics(state_for_metrics, &result);
    Ok(result)
}

pub(crate) async fn run_single_steer_role_job(
    input: SteerRoleRun<'_>,
    timeout_sec: u64,
    config: &RuntimeConfig,
    state_for_degraded: &mut Value,
    conn: &rusqlite::Connection,
) -> Result<Value> {
    let job = prepare_role_job(RoleRun {
        state: input.state,
        role: input.role,
        phase: input.phase,
        kind: input.kind,
        round: input.round,
        topic_id: input.topic_id,
        mock: input.mock,
        model_override: input.model_override,
        reasoning_effort_override: input.reasoning_effort_override,
        config: input.config,
        prompt_path: input.prompt_path,
    })?;
    let result = run_steer_role_job_with_timeout(
        job,
        input.session_id,
        input.turn_id,
        input.steer,
        timeout_sec,
    )
    .await;
    persist_prompt_metric(conn, &result);
    record_role_job_metrics(state_for_degraded, &result);
    role_artifact_or_degraded(state_for_degraded, config, result)
}

pub(crate) fn record_role_job_metrics(state: &mut Value, result: &RoleJobResult) {
    let status = if result.artifact.is_some() {
        "ok"
    } else {
        "degraded"
    };
    if !state.get("role_job_metrics").is_some_and(Value::is_array) {
        state["role_job_metrics"] = json!([]);
    }
    let wait_ms = result.wait_ms();
    if let Some(items) = state["role_job_metrics"].as_array_mut() {
        items.push(json!({
            "role": result.role,
            "phase": result.phase,
            "kind": result.kind,
            "round": result.round,
            "topic_id": result.topic_id,
            "prompt_version": result.prompt_version,
            "model": result.model,
            "timed_out": result.timed_out,
            "elapsed_ms": result.elapsed_ms,
            "llm_ms": result.llm_ms,
            "tool_ms": result.tool_ms,
            "wait_ms": wait_ms,
            "status": status,
            "input_tokens": result.usage.input_tokens,
            "output_tokens": result.usage.output_tokens,
            "cached_tokens": result.usage.cached_tokens,
            "reasoning_tokens": result.usage.reasoning_tokens,
            "total_tokens": result.usage.total_tokens,
            "non_cached_input_tokens": result.usage.non_cached_input_tokens(),
            "visible_output_tokens": result.usage.visible_output_tokens(),
            "turn_count": result.turn_count,
            "tool_call_count": result.tool_call_count
            ,"retrieval_audit": result.artifact.as_ref()
                .and_then(|artifact| artifact.get("retrieval_audit"))
                .cloned().unwrap_or(Value::Null)
        }));
    }
    refresh_role_job_metrics(state);
    if state.get("debug").and_then(Value::as_bool) == Some(true) {
        let root = default_project_root();
        // One role-level timing row: llm + tool + wait breakdown.
        orchestrator_llm::debug_log_time(
            &root,
            json!({
                "kind": "role_job",
                "name": result.role,
                "role": result.role,
                "phase": result.phase,
                "kind_job": result.kind,
                "round": result.round,
                "topic_id": result.topic_id,
                "model": result.model,
                "status": status,
                "timed_out": result.timed_out,
                "elapsed_ms": result.elapsed_ms,
                "llm_ms": result.llm_ms,
                "tool_ms": result.tool_ms,
                "wait_ms": wait_ms,
                "turn_count": result.turn_count,
                "tool_call_count": result.tool_call_count,
            }),
        );
        orchestrator_llm::debug_log_token(
            &root,
            json!({
                "kind": "role_job",
                "role": result.role,
                "phase": result.phase,
                "kind_job": result.kind,
                "round": result.round,
                "topic_id": result.topic_id,
                "model": result.model,
                "status": status,
                "timed_out": result.timed_out,
                "elapsed_ms": result.elapsed_ms,
                "llm_ms": result.llm_ms,
                "tool_ms": result.tool_ms,
                "wait_ms": wait_ms,
                "input_tokens": result.usage.input_tokens,
                "output_tokens": result.usage.output_tokens,
                "cached_tokens": result.usage.cached_tokens,
                "reasoning_tokens": result.usage.reasoning_tokens,
                "total_tokens": result.usage.total_tokens,
                "non_cached_input_tokens": result.usage.non_cached_input_tokens(),
                "visible_output_tokens": result.usage.visible_output_tokens(),
                "turn_count": result.turn_count,
                "tool_call_count": result.tool_call_count,
            }),
        );
    }
}

pub(crate) fn persist_prompt_metric(_conn: &rusqlite::Connection, _result: &RoleJobResult) {
    // ponytail: agent_events restructured — prompt metrics deferred
}

pub(crate) fn merge_role_job_metrics(state: &mut Value, metrics: &Value) {
    let Some(incoming) = metrics.as_array() else {
        return;
    };
    if incoming.is_empty() {
        return;
    }
    if !state.get("role_job_metrics").is_some_and(Value::is_array) {
        state["role_job_metrics"] = json!([]);
    }
    if let Some(items) = state["role_job_metrics"].as_array_mut() {
        items.extend(incoming.iter().cloned());
    }
    refresh_role_job_metrics(state);
}

fn debug_prompt_path_from_runtime_path(path: &str) -> Option<PathBuf> {
    let path = PathBuf::from(path);
    let project_root = default_project_root();
    path.strip_prefix(&project_root)
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            path.to_str().and_then(|value| {
                value
                    .find("prompts/")
                    .map(|index| PathBuf::from(&value[index..]))
            })
        })
}

fn refresh_role_job_metrics(state: &mut Value) {
    let jobs = state
        .get("role_job_metrics")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let total_elapsed_ms = jobs
        .iter()
        .filter_map(|job| job.get("elapsed_ms").and_then(Value::as_u64))
        .sum::<u64>();
    let timed_out_count = jobs
        .iter()
        .filter(|job| job.get("timed_out").and_then(Value::as_bool) == Some(true))
        .count();
    let sum = |field: &str| {
        jobs.iter()
            .filter_map(|job| job.get(field).and_then(Value::as_u64))
            .sum::<u64>()
    };
    let llm_request_count = sum("turn_count");
    let tool_call_count = sum("tool_call_count");

    if !state.get("workflow_metrics").is_some_and(Value::is_object) {
        state["workflow_metrics"] = json!({});
    }
    state["workflow_metrics"]["role_job_count"] = json!(jobs.len());
    state["workflow_metrics"]["llm_call_count"] = json!(llm_request_count);
    state["workflow_metrics"]["llm_request_count"] = json!(llm_request_count);
    state["workflow_metrics"]["tool_call_count"] = json!(tool_call_count);
    state["workflow_metrics"]["input_tokens"] = json!(sum("input_tokens"));
    state["workflow_metrics"]["output_tokens"] = json!(sum("output_tokens"));
    state["workflow_metrics"]["total_tokens"] = json!(sum("total_tokens"));
    state["workflow_metrics"]["total_role_elapsed_ms"] = json!(total_elapsed_ms);
    state["workflow_metrics"]["timed_out_role_count"] = json!(timed_out_count);
}

async fn run_steer_role_job_with_timeout(
    job: RoleJob,
    session_id: String,
    turn_id: String,
    steer: Option<String>,
    timeout_sec: u64,
) -> RoleJobResult {
    let role = job.role.clone();
    let phase = job.phase;
    let kind = job.kind.clone();
    let round = job.round;
    let topic_id = job.topic_id.clone();
    let tickers = job.tickers.clone();
    let prompt_version = job.prompt_version.clone();
    let started_at = Instant::now();
    debug!(
        role,
        phase, kind, round, topic_id, timeout_sec, "steer role job starting"
    );
    match time::timeout(
        Duration::from_secs(timeout_sec.max(1)),
        execute_steer_role_job(job, session_id, turn_id, steer),
    )
    .await
    {
        Ok(Ok(output)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            debug!(role, phase, kind, elapsed_ms, "steer role job completed");
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                prompt_version,
                model: output
                    .artifact
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                turn_id: output.turn_id,
                session_id: output.session_id,
                artifact: Some(output.artifact),
                error: None,
                timed_out: false,
                elapsed_ms,
                llm_ms: output.metrics.llm_ms,
                tool_ms: output.metrics.tool_ms,
                usage: output.metrics.usage,
                turn_count: output.metrics.turn_count,
                tool_call_count: output.metrics.tool_call_count,
            }
        }
        Ok(Err(error)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            warn!(role, phase, kind, elapsed_ms, error = %error, "steer role job failed");
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                prompt_version,
                model: String::new(),
                turn_id: String::new(),
                session_id: String::new(),
                artifact: None,
                error: Some(error.to_string()),
                timed_out: false,
                elapsed_ms,
                llm_ms: 0,
                tool_ms: 0,
                usage: TokenUsage::default(),
                turn_count: 0,
                tool_call_count: 0,
            }
        }
        Err(_) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            warn!(
                role,
                phase, kind, elapsed_ms, timeout_sec, "steer role job timed out"
            );
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                prompt_version,
                model: String::new(),
                turn_id: String::new(),
                session_id: String::new(),
                artifact: None,
                error: Some(format!("role execution timed out after {timeout_sec}s")),
                timed_out: true,
                elapsed_ms,
                llm_ms: 0,
                tool_ms: 0,
                usage: TokenUsage::default(),
                turn_count: 0,
                tool_call_count: 0,
            }
        }
    }
}

fn is_transient_role_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    // Permanent request/context errors must not burn role retries.
    // Do not treat bare "llm stream failed" wrappers as transient — that
    // previously retried context-window-full 400s after stream retries finished.
    if is_permanent_role_error_text(&text) {
        return false;
    }
    text.contains("503")
        || text.contains("502")
        || text.contains("429")
        || text.contains("bad_response_status_code")
        || text.contains("no healthy upstream")
        || text.contains("timeout")
        || text.contains("timed out")
        || text.contains("connection reset")
        || text.contains("transport error")
        || text.contains("error decoding response body")
        || text.contains("temporarily unavailable")
        || text.contains("upstream_error")
        || text.contains("upstream request failed")
}

fn is_permanent_role_error_text(text: &str) -> bool {
    text.contains("context window is full")
        || text.contains("reduce conversation history")
        || text.contains("invalid_request_error")
        || text.contains("请精简对话历史")
        || text.contains("context window")
        || text.contains("max_agent_loops")
        || (text.contains("400")
            && (text.contains("invalid_request")
                || text.contains("context")
                || text.contains("too large")
                || text.contains("token")))
}

fn role_retry_jitter_ms(role: &str, attempt: usize) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    role.hash(&mut hasher);
    attempt.hash(&mut hasher);
    hasher.finish() % 251
}

pub(crate) async fn run_role_job_with_timeout(job: RoleJob, timeout_sec: u64) -> RoleJobResult {
    let role = job.role.clone();
    let phase = job.phase;
    let kind = job.kind.clone();
    let round = job.round;
    let topic_id = job.topic_id.clone();
    let tickers = job.tickers.clone();
    let prompt_version = job.prompt_version.clone();
    let started_at = Instant::now();
    debug!(
        role,
        phase, kind, round, topic_id, timeout_sec, "role job starting"
    );

    // Live gateway 503s can exhaust stream-level retries; retry the whole role a
    // couple of times before surfacing a critical failure.
    const MAX_ROLE_ATTEMPTS: usize = 3;
    let mut attempt = 0usize;
    let result = loop {
        attempt += 1;
        match time::timeout(
            Duration::from_secs(timeout_sec.max(1)),
            execute_role_job(job.clone()),
        )
        .await
        {
            Ok(Ok(output)) => break Ok(output),
            Ok(Err(error)) => {
                // Use the full chain so permanent upstream messages (e.g. context
                // window full) are not masked by outer "LLM stream chunk failed".
                let message = format!("{error:#}");
                if attempt < MAX_ROLE_ATTEMPTS && is_transient_role_error(&message) {
                    let backoff_ms =
                        1_000u64 * attempt as u64 + role_retry_jitter_ms(&role, attempt);
                    warn!(
                        role = role.as_str(),
                        phase,
                        kind = kind.as_str(),
                        attempt,
                        backoff_ms,
                        error = %message,
                        "retrying transient role job failure"
                    );
                    time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                break Err((message, false));
            }
            Err(_) => {
                let message = format!("role execution timed out after {timeout_sec}s");
                if attempt < MAX_ROLE_ATTEMPTS {
                    let backoff_ms =
                        1_000u64 * attempt as u64 + role_retry_jitter_ms(&role, attempt);
                    warn!(
                        role = role.as_str(),
                        phase,
                        kind = kind.as_str(),
                        attempt,
                        backoff_ms,
                        error = %message,
                        "retrying timed-out role job"
                    );
                    time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                break Err((message, true));
            }
        }
    };

    match result {
        Ok(output) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            debug!(role, phase, kind, elapsed_ms, "role job completed");
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                prompt_version,
                model: output
                    .artifact
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                turn_id: output.turn_id,
                session_id: output.session_id,
                artifact: Some(output.artifact),
                error: None,
                timed_out: false,
                elapsed_ms,
                llm_ms: output.metrics.llm_ms,
                tool_ms: output.metrics.tool_ms,
                usage: output.metrics.usage,
                turn_count: output.metrics.turn_count,
                tool_call_count: output.metrics.tool_call_count,
            }
        }
        Err((message, timed_out)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            warn!(
                role,
                phase,
                kind,
                elapsed_ms,
                error = %message,
                timed_out,
                "role job failed"
            );
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                prompt_version,
                model: String::new(),
                turn_id: String::new(),
                session_id: String::new(),
                artifact: None,
                error: Some(message),
                timed_out,
                elapsed_ms,
                llm_ms: 0,
                tool_ms: 0,
                usage: TokenUsage::default(),
                turn_count: 0,
                tool_call_count: 0,
            }
        }
    }
}

async fn execute_role_job(job: RoleJob) -> Result<AgentLoopOutput> {
    if job.mock {
        if let Some(binding) = job.domain_tool_runtime.clone() {
            return mock_domain_tool_managed_output(job, binding);
        }
        debug!(
            role = job.role,
            phase = job.phase,
            kind = job.kind,
            "using mock artifact"
        );
        let mut artifact = mock_role_artifact(&job.role, &job.tickers);
        artifact["retrieval_audit"] = json!({
            "status": "not_applicable",
            "source": "mock_runtime",
            "summary_query_count": 0,
            "detail_call_count": 0
        });
        artifact["context_manifest"] = job.context_manifest;
        artifact["phase"] = Value::Number(job.phase.into());
        artifact["kind"] = Value::String(job.kind);
        if let Some(round) = job.round {
            artifact["round"] = Value::Number(round.into());
        }
        if let Some(topic_id) = job.topic_id {
            artifact["topic_id"] = Value::String(topic_id);
        }
        if let Some(path) = job.prompt_path {
            artifact["prompt_path"] = Value::String(path);
        }
        if let Some(version) = job.prompt_version {
            artifact["prompt_version"] = Value::String(version);
        }
        return Ok(AgentLoopOutput {
            artifact,
            terminal_tool_result: None,
            metrics: ModelStreamResult::default(),
            turn_id: String::new(),
            session_id: String::new(),
        });
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let debug_prompt_path = job
        .prompt_path
        .as_deref()
        .and_then(debug_prompt_path_from_runtime_path);
    let debug_round = job.round.and_then(|round| usize::try_from(round).ok());
    let settings = AgentSettings {
        role: job.role,
        phase: Some(job.phase),
        topic_id: job.topic_id,
        debug_prompt_path,
        debug_output_path: job.debug_output_path,
        debug_round,
        tickers: job.tickers,
        output_mode: job.output_mode,
        tool_managed_profile: job.tool_managed_profile,
        index_tool_runtime: None,
        domain_tool_runtime: job.domain_tool_runtime.clone(),
        llm,
        reasoning_effort_override: job.reasoning_effort_override,
        tools: Some(job.tools),
        web_search: job.web_search,
        truncation: job.truncation,
        judge: job.judge,
        debug: job.debug,
        retrieval_policy: job.retrieval_policy,
    };
    debug!(
        role = settings.role,
        model = settings.llm.model,
        prompt_chars = job.prompt.len(),
        "calling agent loop"
    );
    let mut output = run_agent_loop_with_metrics(&settings, &job.prompt).await?;
    output.artifact["context_manifest"] = job.context_manifest;
    Ok(output)
}

fn mock_domain_tool_managed_output(
    job: RoleJob,
    binding: orchestrator_llm::tools::domain_tools::DomainToolRuntimeBinding,
) -> Result<AgentLoopOutput> {
    use orchestrator_llm::agent_loop::ToolResultItem;
    use orchestrator_llm::tools::domain_tools::{
        ADD_AGREED_FACT, APPEND_ANALYST_EVIDENCE, CREATE_DEBATE_CLAIM, CREATE_PHASE2_TOPIC,
        FINALIZE_ANALYST_REPORT, FINALIZE_DEBATE_RESPONSE, FINALIZE_DEBATE_SEED,
        FINALIZE_RESEARCHER_WARMUP, FINALIZE_RESEARCH_DECISION, FINALIZE_TOPIC_CONTROL,
        FINALIZE_TOPIC_GENERATION, RESPOND_TO_DEBATE_CLAIM, SET_ANALYST_ASSESSMENT,
        SET_ANALYST_INVALIDATION, SET_DECISION_HINGE, SET_PHASE2_COMMON_GROUND,
        SET_RESEARCH_DECISION, SET_RESEARCH_SCENARIOS, SET_TOPIC_SOFT_CONTROL,
    };

    let profile = binding.scope().profile;
    let artifact = match profile {
        ToolManagedProfile::AnalystReport => {
            for ticker in &job.tickers {
                binding.execute(
                    SET_ANALYST_ASSESSMENT,
                    json!({
                        "ticker": ticker,
                        "direction": "neutral",
                        "confidence": 0.5,
                        "report": format!("Mock FileStore report for {ticker} from {}.", job.role),
                        "priced_in": "unclear",
                        "echo_chamber_risk": "low",
                        "crowded_consensus_risk": "low",
                    }),
                )?;
                binding.execute(
                    APPEND_ANALYST_EVIDENCE,
                    json!({
                        "ticker": ticker,
                        "evidence_ref": format!("mock:{}:{ticker}", job.role),
                        "evidence": {
                            "claim": format!("Mock evidence for {ticker}."),
                            "evidence_type": "fact",
                            "source": "mock runtime",
                            "timestamp": job.context_manifest.get("current_date").and_then(Value::as_str).unwrap_or("2026-01-01"),
                            "source_tier": "official",
                            "first_source": "mock runtime",
                            "is_derivative_repost": false,
                            "evidence_age": "0-2d",
                            "source_confidence": 0.9,
                        }
                    }),
                )?;
                binding.execute(
                    SET_ANALYST_INVALIDATION,
                    json!({
                        "ticker": ticker,
                        "validation_triggers": [format!("Mock invalidation for {ticker}.")],
                    }),
                )?;
            }
            binding.execute(FINALIZE_ANALYST_REPORT, json!({}))?
        }
        ToolManagedProfile::ResearchDecision => {
            for ticker in &job.tickers {
                binding.execute(
                    SET_RESEARCH_DECISION,
                    json!({
                        "ticker": ticker,
                        "rating": "Hold",
                        "long_probability": 0.5,
                        "short_probability": 0.5,
                        "confidence_basis": "evidence_balanced",
                        "hold_reason": "evidence_balanced",
                        "plan": format!("Mock FileStore research plan for {ticker}."),
                        "probability_rationale": "Mock evidence is balanced.",
                    }),
                )?;
                binding.execute(
                    SET_RESEARCH_SCENARIOS,
                    json!({
                        "ticker": ticker,
                        "bull": {"probability": 0.25, "drivers": ["mock upside"], "triggers": ["mock confirmation"], "confirmation": "mock"},
                        "base": {"probability": 0.50, "drivers": ["mock balance"], "triggers": ["mock confirmation"], "confirmation": "mock"},
                        "bear": {"probability": 0.25, "drivers": ["mock downside"], "triggers": ["mock confirmation"], "confirmation": "mock"},
                    }),
                )?;
            }
            binding.execute(FINALIZE_RESEARCH_DECISION, json!({}))?
        }
        ToolManagedProfile::ResearcherWarmup => {
            binding.execute(FINALIZE_RESEARCHER_WARMUP, json!({}))?
        }
        ToolManagedProfile::TopicGeneration => {
            binding.execute(
                SET_PHASE2_COMMON_GROUND,
                json!({"common_ground":"Mock Phase 2 common ground."}),
            )?;
            binding.execute(
                CREATE_PHASE2_TOPIC,
                json!({
                    "topic":"Mock decision hinge", "decision_hinge":"Mock confirmation",
                    "evidence_refs": ["mock:mediator.topic:QQQ"]
                }),
            )?;
            binding.execute(FINALIZE_TOPIC_GENERATION, json!({}))?
        }
        ToolManagedProfile::DebateSeed => {
            binding.execute(
                CREATE_DEBATE_CLAIM,
                json!({
                    "claim": format!("Mock {} seed claim.", job.role), "confidence":0.5,
                    "evidence_refs": [format!("mock:{}:QQQ", job.role)]
                }),
            )?;
            binding.execute(FINALIZE_DEBATE_SEED, json!({}))?
        }
        ToolManagedProfile::DebateResponse => {
            let claim_id = binding
                .scope()
                .visible_evidence_refs
                .iter()
                .find(|value| value.starts_with("claim-"))
                .cloned()
                .context("mock debate response requires a visible claim")?;
            binding.execute(
                RESPOND_TO_DEBATE_CLAIM,
                json!({
                    "reply_to_claim_id":claim_id, "response":"Mock counterpoint.",
                    "evidence_refs": [format!("mock:{}:QQQ", job.role)]
                }),
            )?;
            binding.execute(FINALIZE_DEBATE_RESPONSE, json!({}))?
        }
        ToolManagedProfile::TopicControl => {
            binding.execute(ADD_AGREED_FACT, json!({"value":"Mock topic fact."}))?;
            binding.execute(SET_DECISION_HINGE, json!({"value":"Mock topic hinge."}))?;
            binding.execute(SET_TOPIC_SOFT_CONTROL, json!({"should_continue":false}))?;
            binding.execute(FINALIZE_TOPIC_CONTROL, json!({}))?
        }
        _ => anyhow::bail!(
            "mock domain runtime profile {} is not wired",
            profile.as_str()
        ),
    };
    let artifact = artifact
        .get("artifact")
        .cloned()
        .context("mock domain finalizer did not return a canonical artifact")?;
    Ok(AgentLoopOutput {
        artifact: artifact.clone(),
        terminal_tool_result: Some(ToolResultItem {
            call_id: "mock-finalize".to_owned(),
            name: match profile {
                ToolManagedProfile::AnalystReport => FINALIZE_ANALYST_REPORT,
                ToolManagedProfile::ResearchDecision => FINALIZE_RESEARCH_DECISION,
                ToolManagedProfile::ResearcherWarmup => FINALIZE_RESEARCHER_WARMUP,
                ToolManagedProfile::TopicGeneration => FINALIZE_TOPIC_GENERATION,
                ToolManagedProfile::DebateSeed => FINALIZE_DEBATE_SEED,
                ToolManagedProfile::DebateResponse => FINALIZE_DEBATE_RESPONSE,
                ToolManagedProfile::TopicControl => FINALIZE_TOPIC_CONTROL,
                _ => unreachable!(),
            }
            .to_owned(),
            status: "completed".to_owned(),
            output: json!({"terminal": true, "artifact": artifact}),
            error: None,
        }),
        metrics: ModelStreamResult::default(),
        turn_id: String::new(),
        session_id: String::new(),
    })
}

async fn execute_steer_role_job(
    job: RoleJob,
    session_id: String,
    turn_id: String,
    steer: Option<String>,
) -> Result<AgentLoopOutput> {
    if job.mock {
        let mut artifact = mock_role_artifact(&job.role, &job.tickers);
        artifact["retrieval_audit"] = json!({
            "status": "not_applicable",
            "source": "mock_runtime",
            "summary_query_count": 0,
            "detail_call_count": 0
        });
        artifact["context_manifest"] = job.context_manifest;
        artifact["phase"] = Value::Number(job.phase.into());
        artifact["kind"] = Value::String(job.kind);
        if let Some(round) = job.round {
            artifact["round"] = Value::Number(round.into());
        }
        if let Some(topic_id) = job.topic_id {
            artifact["topic_id"] = Value::String(topic_id);
        }
        if let Some(path) = job.prompt_path {
            artifact["prompt_path"] = Value::String(path);
        }
        if let Some(version) = job.prompt_version {
            artifact["prompt_version"] = Value::String(version);
        }
        if let Some(steer) = steer {
            let steer_kind = serde_json::from_str::<Value>(&steer)
                .ok()
                .and_then(|value| value.get("kind").cloned())
                .unwrap_or_else(|| Value::String("unknown".to_string()));
            artifact["steer_ref"] = json!({
                "kind": steer_kind,
                "payload_omitted": true
            });
        }
        artifact["session_id"] = Value::String(session_id.clone());
        artifact["turn_id"] = Value::String(turn_id.clone());
        return Ok(AgentLoopOutput {
            artifact,
            terminal_tool_result: None,
            metrics: ModelStreamResult::default(),
            turn_id,
            session_id,
        });
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let debug_prompt_path = job
        .prompt_path
        .as_deref()
        .and_then(debug_prompt_path_from_runtime_path);
    let debug_round = job.round.and_then(|round| usize::try_from(round).ok());
    let settings = AgentSettings {
        role: job.role,
        phase: Some(job.phase),
        topic_id: job.topic_id,
        debug_prompt_path,
        debug_output_path: job.debug_output_path,
        debug_round,
        tickers: job.tickers,
        output_mode: job.output_mode,
        tool_managed_profile: job.tool_managed_profile,
        index_tool_runtime: None,
        domain_tool_runtime: job.domain_tool_runtime.clone(),
        llm,
        reasoning_effort_override: job.reasoning_effort_override,
        tools: Some(job.tools),
        web_search: job.web_search,
        truncation: job.truncation,
        judge: job.judge,
        debug: job.debug,
        retrieval_policy: job.retrieval_policy,
    };
    let mut output = run_agent_steer_loop_with_metrics(
        &settings,
        SteerLoopInput {
            session_id,
            turn_id,
            prompt: &job.prompt,
            steer,
        },
    )
    .await?;
    output.artifact["context_manifest"] = job.context_manifest;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(role: &str, timed_out: bool, elapsed_ms: u128) -> RoleJobResult {
        let llm_ms = elapsed_ms / 2;
        let tool_ms = elapsed_ms / 4;
        RoleJobResult {
            role: role.to_string(),
            phase: 3,
            kind: "artifact".to_string(),
            round: None,
            topic_id: None,
            tickers: vec!["QQQ".to_string()],
            prompt_version: Some("v1".to_string()),
            model: "test-model".to_string(),
            turn_id: "turn-1".to_string(),
            session_id: "session-1".to_string(),
            artifact: if timed_out { None } else { Some(json!({})) },
            error: timed_out.then(|| "timeout".to_string()),
            timed_out,
            elapsed_ms,
            llm_ms,
            tool_ms,
            usage: TokenUsage {
                input_tokens: 10,
                output_tokens: 4,
                cached_tokens: 2,
                reasoning_tokens: 0,
                total_tokens: 14,
            },
            turn_count: 1,
            tool_call_count: 3,
        }
    }

    #[test]
    fn wait_ms_is_total_minus_llm_and_tool() {
        let job = result("manager.research", false, 100);
        assert_eq!(job.llm_ms, 50);
        assert_eq!(job.tool_ms, 25);
        assert_eq!(job.wait_ms(), 25);
    }

    #[test]
    fn phase2_debug_paths_follow_the_checkpoint_and_topic_tree() {
        assert_eq!(
            phase2_debug_output_path(2, "mediator.topic", "warmup", None),
            Some(PathBuf::from(
                "outputs/debug/phase2/phase2-warmup-shared.json"
            ))
        );
        assert_eq!(
            phase2_debug_output_path(2, "mediator.topic", "topic_generation", None),
            Some(PathBuf::from("outputs/debug/phase2/topic-generator.json"))
        );
        for (role, file) in [
            ("researcher.bull.initial", "debate-bull.json"),
            ("researcher.bear.interaction", "debate-bear.json"),
            ("mediator.topic_controller", "topic-controller.json"),
        ] {
            assert_eq!(
                phase2_debug_output_path(2, role, "debate", Some("QQQ/risk")),
                Some(PathBuf::from("outputs/debug/phase2/topic-QQQ_risk").join(file))
            );
        }
        assert_eq!(
            phase2_debug_output_path(2, "researcher.bull.initial", "bull_seed", Some("topic_vix")),
            Some(PathBuf::from(
                "outputs/debug/phase2/topic_vix/debate-bull.json"
            ))
        );
    }

    #[test]
    fn phase2_json_debug_files_retain_structured_messages() {
        let temp = tempfile::tempdir().unwrap();
        let path =
            phase2_debug_output_path(2, "researcher.bull.initial", "bull_seed", Some("topic-a"))
                .unwrap();
        orchestrator_llm::append_debug_output_record(
            temp.path(),
            &path,
            "prompts/phase2/researcher/debate.md",
            json!({
                "kind": "stream",
                "req": {
                    "messages": [
                        {"role": "assistant", "content": "准备完毕"},
                        {"role": "user", "content": "BULL ROLE PROMPT\n\nSteer: topic-a"}
                    ]
                },
                "resp": {"status": "completed"}
            }),
        )
        .unwrap();

        let output: Value =
            serde_json::from_str(&std::fs::read_to_string(temp.path().join(path)).unwrap())
                .unwrap();
        assert_eq!(output["req"]["messages"][0]["content"], "准备完毕");
        assert_eq!(output["req"]["messages"][1]["role"], "user");
        assert!(output.get("records").is_none());
    }

    #[test]
    fn context_window_full_is_not_transient_role_error() {
        let message = "LLM stream chunk failed: InvalidStatusCodeWithMessage(400, \
            \"{\\\"error\\\":{\\\"message\\\":\\\"Context window is full — reduce conversation history\\\",\\\"type\\\":\\\"invalid_request_error\\\"}}\")";
        assert!(!is_transient_role_error(message));
        assert!(is_permanent_role_error_text(&message.to_ascii_lowercase()));
    }

    #[test]
    fn bare_stream_wrapper_is_not_transient_without_upstream_marker() {
        // Outer wrapper alone used to retry permanent 400s after chain was lost.
        assert!(!is_transient_role_error("LLM stream chunk failed"));
    }

    #[test]
    fn gateway_502_is_transient_role_error() {
        let message = "LLM stream chunk failed: InvalidStatusCodeWithMessage(502, \
            \"{\\\"error\\\":{\\\"message\\\":\\\"Upstream request failed\\\",\\\"type\\\":\\\"upstream_error\\\"}}\")";
        assert!(is_transient_role_error(message));
    }

    #[test]
    fn stream_transport_decode_error_is_transient_role_error() {
        assert!(is_transient_role_error(
            "Chat Completions stream chunk failed: stream failed: EventStream error: Transport error: error decoding response body"
        ));
    }

    #[test]
    fn records_role_job_metrics_and_aggregates() {
        let mut state = json!({});

        record_role_job_metrics(&mut state, &result("manager.research", false, 7));
        record_role_job_metrics(&mut state, &result("trader", true, 11));

        assert_eq!(state["role_job_metrics"].as_array().unwrap().len(), 2);
        assert_eq!(state["role_job_metrics"][0]["prompt_version"], "v1");
        assert_eq!(state["role_job_metrics"][0]["input_tokens"], 10);
        assert_eq!(state["role_job_metrics"][0]["output_tokens"], 4);
        assert_eq!(state["role_job_metrics"][0]["cached_tokens"], 2);
        assert_eq!(state["role_job_metrics"][0]["reasoning_tokens"], 0);
        assert_eq!(state["role_job_metrics"][0]["total_tokens"], 14);
        assert_eq!(state["role_job_metrics"][0]["non_cached_input_tokens"], 8);
        assert_eq!(state["role_job_metrics"][0]["visible_output_tokens"], 4);
        assert_eq!(state["role_job_metrics"][0]["model"], "test-model");
        assert_eq!(state["role_job_metrics"][0]["turn_count"], 1);
        assert_eq!(state["role_job_metrics"][0]["tool_call_count"], 3);
        assert_eq!(state["workflow_metrics"]["role_job_count"], 2);
        assert_eq!(state["workflow_metrics"]["llm_call_count"], 2);
        assert_eq!(state["workflow_metrics"]["tool_call_count"], 6);
        assert_eq!(state["workflow_metrics"]["input_tokens"], 20);
        assert_eq!(state["workflow_metrics"]["output_tokens"], 8);
        assert_eq!(state["workflow_metrics"]["total_tokens"], 28);
        assert_eq!(state["workflow_metrics"]["total_role_elapsed_ms"], 18);
        assert_eq!(state["workflow_metrics"]["timed_out_role_count"], 1);
    }

    #[test]
    fn merges_topic_local_role_job_metrics() {
        let mut state = json!({});
        let mut topic_state = json!({});
        record_role_job_metrics(
            &mut topic_state,
            &result("researcher.bull.initial", false, 5),
        );

        merge_role_job_metrics(&mut state, &topic_state["role_job_metrics"]);

        assert_eq!(state["role_job_metrics"].as_array().unwrap().len(), 1);
        assert_eq!(state["workflow_metrics"]["llm_call_count"], 1);
        assert_eq!(state["workflow_metrics"]["total_role_elapsed_ms"], 5);
    }
}
