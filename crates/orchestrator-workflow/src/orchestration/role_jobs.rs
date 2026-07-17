use anyhow::{Context, Result};
use futures::{stream, StreamExt};
use orchestrator_core::default_project_root;
use orchestrator_llm::{
    agent_loop::{ModelStreamResult, TokenUsage},
    llm_judge::JudgeConfig,
    mock_role_artifact, run_rig_agent_loop_with_metrics, run_rig_agent_steer_loop_with_metrics,
    tools::ExternalToolConfig,
    truncation::TruncationConfig,
    AgentLoopOutput, OutputMode, RigSettings, RoleLlmSettings, SteerLoopInput,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, warn};

use super::config::{output_mode_for_role, prompt_version, RuntimeConfig};
use super::degraded::role_artifact_or_degraded;
use super::render::render_prompt_with_plugins;
use super::state::tickers_from_state;

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
    pub prompt_version: Option<String>,
    pub tickers: Vec<String>,
    pub output_mode: OutputMode,
    pub llm: Option<RoleLlmSettings>,
    pub reasoning_effort_override: Option<String>,
    pub tools: ExternalToolConfig,
    pub web_search: orchestrator_llm::web_search::WebSearchConfig,
    pub truncation: TruncationConfig,
    pub judge: JudgeConfig,
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

fn prompt_version_for_role(state: &Value, role: &str) -> Option<String> {
    let config = state.get("config")?;
    let prompt_key = match role {
        "analyst.technical" => "orchestrator.prompts.analyst.technical",
        "analyst.news_macro" => "orchestrator.prompts.analyst.news_macro",
        "analyst.youtube" => "orchestrator.prompts.analyst.youtube",
        "analyst.reddit" => "orchestrator.prompts.analyst.reddit",
        "analyst.x" => "orchestrator.prompts.analyst.x",
        "researcher.bull.initial" => "orchestrator.prompts.phase2.bull_initial",
        "researcher.bull.interaction" => "orchestrator.prompts.phase2.bull_interaction",
        "researcher.bear.initial" => "orchestrator.prompts.phase2.bear_initial",
        "researcher.bear.interaction" => "orchestrator.prompts.phase2.bear_interaction",
        "mediator.topic" => "orchestrator.prompts.mediator.topic",
        "mediator.topic_controller" => "orchestrator.prompts.mediator.topic_controller",
        "manager.research" => "orchestrator.prompts.manager.research",
        "trader" => "orchestrator.prompts.trader",
        "risk.aggressive" => "orchestrator.prompts.risk.aggressive",
        "risk.conservative" => "orchestrator.prompts.risk.conservative",
        "risk.neutral" => "orchestrator.prompts.risk.neutral",
        "portfolio.manager" => "orchestrator.prompts.portfolio.manager",
        "allocation.manager" => "orchestrator.prompts.allocation.manager",
        _ => return None,
    };
    Some(prompt_version(config, prompt_key))
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
    let tickers = tickers_from_state(&state);
    let prompt_version = prompt_version_for_role(&state, role);
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
        prompt_version,
        tickers: tickers.clone(),
        output_mode: output_mode_for_role(role),
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
            tickers,
        },
        web_search: config.web_search.get(role).cloned().unwrap_or_default(),
        truncation: config.truncation.clone(),
        judge: config.judge.clone(),
    })
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
    let job = prepare_role_job(input)?;
    let result = run_role_job_with_timeout(job, timeout_sec).await;
    persist_prompt_metric(conn, &result);
    record_role_job_metrics(state_for_degraded, &result);
    role_artifact_or_degraded(state_for_degraded, config, result)
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

    if !state.get("workflow_metrics").is_some_and(Value::is_object) {
        state["workflow_metrics"] = json!({});
    }
    state["workflow_metrics"]["role_job_count"] = json!(jobs.len());
    state["workflow_metrics"]["llm_call_count"] = json!(jobs.len());
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
    text.contains("503")
        || text.contains("502")
        || text.contains("429")
        || text.contains("bad_response_status_code")
        || text.contains("no healthy upstream")
        || text.contains("llm stream chunk failed")
        || text.contains("llm stream failed")
        || text.contains("timeout")
        || text.contains("timed out")
        || text.contains("connection reset")
        || text.contains("temporarily unavailable")
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
                let message = error.to_string();
                if attempt < MAX_ROLE_ATTEMPTS && is_transient_role_error(&message) {
                    let backoff_ms = 1_000u64 * attempt as u64;
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
                    let backoff_ms = 1_000u64 * attempt as u64;
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
        debug!(
            role = job.role,
            phase = job.phase,
            kind = job.kind,
            "using mock artifact"
        );
        let mut artifact = mock_role_artifact(&job.role, &job.tickers);
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
            metrics: ModelStreamResult::default(),
            turn_id: String::new(),
            session_id: String::new(),
        });
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let settings = RigSettings {
        role: job.role,
        phase: Some(job.phase),
        topic_id: job.topic_id,
        tickers: job.tickers,
        output_mode: job.output_mode,
        llm,
        reasoning_effort_override: job.reasoning_effort_override,
        tools: Some(job.tools),
        web_search: job.web_search,
        truncation: job.truncation,
        judge: job.judge,
        debug: job.debug,
    };
    debug!(
        role = settings.role,
        model = settings.llm.model,
        prompt_chars = job.prompt.len(),
        "calling rig agent loop"
    );
    run_rig_agent_loop_with_metrics(&settings, &job.prompt).await
}

async fn execute_steer_role_job(
    job: RoleJob,
    session_id: String,
    turn_id: String,
    steer: Option<String>,
) -> Result<AgentLoopOutput> {
    if job.mock {
        let mut artifact = mock_role_artifact(&job.role, &job.tickers);
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
            metrics: ModelStreamResult::default(),
            turn_id,
            session_id,
        });
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let settings = RigSettings {
        role: job.role,
        phase: Some(job.phase),
        topic_id: job.topic_id,
        tickers: job.tickers,
        output_mode: job.output_mode,
        llm,
        reasoning_effort_override: job.reasoning_effort_override,
        tools: Some(job.tools),
        web_search: job.web_search,
        truncation: job.truncation,
        judge: job.judge,
        debug: job.debug,
    };
    run_rig_agent_steer_loop_with_metrics(
        &settings,
        SteerLoopInput {
            session_id,
            turn_id,
            prompt: &job.prompt,
            steer,
        },
    )
    .await
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
