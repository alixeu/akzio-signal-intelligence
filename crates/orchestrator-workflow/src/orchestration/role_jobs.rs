use anyhow::{Context, Result};
use futures::{stream, StreamExt};
use orchestrator_core::default_project_root;
use orchestrator_llm::{
    mock_role_artifact, run_rig_agent_loop, run_rig_agent_steer_loop, tools::ExternalToolConfig,
    OutputMode, RigSettings, RoleLlmSettings, SteerLoopInput,
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{debug, warn};

use super::config::{output_mode_for_role, RuntimeConfig};
use super::degraded::role_artifact_or_degraded;
use super::render::render_prompt;
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

#[derive(Debug)]
pub(crate) struct RoleJob {
    pub role: String,
    pub phase: i64,
    pub kind: String,
    pub round: Option<i64>,
    pub topic_id: Option<String>,
    pub mock: bool,
    pub prompt: String,
    pub prompt_path: Option<String>,
    pub tickers: Vec<String>,
    pub output_mode: OutputMode,
    pub llm: Option<RoleLlmSettings>,
    pub reasoning_effort_override: Option<String>,
    pub tools: ExternalToolConfig,
    pub web_search: orchestrator_llm::web_search::WebSearchConfig,
}

#[derive(Debug)]
pub(crate) struct RoleJobResult {
    pub role: String,
    pub phase: i64,
    pub kind: String,
    pub round: Option<i64>,
    pub topic_id: Option<String>,
    pub tickers: Vec<String>,
    pub artifact: Option<Value>,
    pub error: Option<String>,
    pub timed_out: bool,
    pub elapsed_ms: u128,
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
    let tickers = tickers_from_state(&state);
    let prompt = if mock {
        String::new()
    } else {
        render_prompt(&state, role, phase, kind, round, topic_id, prompt_path)?
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
        prompt_path = prompt_path.map(|path| path.display().to_string()),
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
        prompt,
        prompt_path: prompt_path.map(|path| path.display().to_string()),
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
) -> Result<Value> {
    let job = prepare_role_job(input)?;
    let result = run_role_job_with_timeout(job, timeout_sec).await;
    record_role_job_metrics(state_for_degraded, &result);
    role_artifact_or_degraded(state_for_degraded, config, result)
}

pub(crate) async fn run_single_steer_role_job(
    input: SteerRoleRun<'_>,
    timeout_sec: u64,
    config: &RuntimeConfig,
    state_for_degraded: &mut Value,
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
    record_role_job_metrics(state_for_degraded, &result);
    role_artifact_or_degraded(state_for_degraded, config, result)
}

pub(crate) fn record_role_job_metrics(state: &mut Value, result: &RoleJobResult) {
    if !state.get("role_job_metrics").is_some_and(Value::is_array) {
        state["role_job_metrics"] = json!([]);
    }
    if let Some(items) = state["role_job_metrics"].as_array_mut() {
        items.push(json!({
            "role": result.role,
            "phase": result.phase,
            "kind": result.kind,
            "round": result.round,
            "topic_id": result.topic_id,
            "timed_out": result.timed_out,
            "elapsed_ms": result.elapsed_ms,
            "status": if result.artifact.is_some() { "ok" } else { "degraded" }
        }));
    }
    refresh_role_job_metrics(state);
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
        Ok(Ok(artifact)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            debug!(role, phase, kind, elapsed_ms, "steer role job completed");
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                artifact: Some(artifact),
                error: None,
                timed_out: false,
                elapsed_ms,
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
                artifact: None,
                error: Some(error.to_string()),
                timed_out: false,
                elapsed_ms,
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
                artifact: None,
                error: Some(format!("role execution timed out after {timeout_sec}s")),
                timed_out: true,
                elapsed_ms,
            }
        }
    }
}

pub(crate) async fn run_role_job_with_timeout(job: RoleJob, timeout_sec: u64) -> RoleJobResult {
    let role = job.role.clone();
    let phase = job.phase;
    let kind = job.kind.clone();
    let round = job.round;
    let topic_id = job.topic_id.clone();
    let tickers = job.tickers.clone();
    let started_at = Instant::now();
    debug!(
        role,
        phase, kind, round, topic_id, timeout_sec, "role job starting"
    );
    match time::timeout(
        Duration::from_secs(timeout_sec.max(1)),
        execute_role_job(job),
    )
    .await
    {
        Ok(Ok(artifact)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            debug!(role, phase, kind, elapsed_ms, "role job completed");
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                artifact: Some(artifact),
                error: None,
                timed_out: false,
                elapsed_ms,
            }
        }
        Ok(Err(error)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            warn!(
                role,
                phase,
                kind,
                elapsed_ms,
                error = %error,
                "role job failed"
            );
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                artifact: None,
                error: Some(error.to_string()),
                timed_out: false,
                elapsed_ms,
            }
        }
        Err(_) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            warn!(
                role,
                phase, kind, elapsed_ms, timeout_sec, "role job timed out"
            );
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                tickers,
                artifact: None,
                error: Some(format!("role execution timed out after {timeout_sec}s")),
                timed_out: true,
                elapsed_ms,
            }
        }
    }
}

async fn execute_role_job(job: RoleJob) -> Result<Value> {
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
        return Ok(artifact);
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let settings = RigSettings {
        role: job.role,
        phase: Some(job.phase),
        tickers: job.tickers,
        output_mode: job.output_mode,
        llm,
        reasoning_effort_override: job.reasoning_effort_override,
        tools: Some(job.tools),
        web_search: job.web_search,
    };
    debug!(
        role = settings.role,
        model = settings.llm.model,
        prompt_chars = job.prompt.len(),
        "calling rig agent loop"
    );
    run_rig_agent_loop(&settings, &job.prompt).await
}

async fn execute_steer_role_job(
    job: RoleJob,
    session_id: String,
    turn_id: String,
    steer: Option<String>,
) -> Result<Value> {
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
        if let Some(steer) = steer {
            artifact["steer"] = Value::String(steer);
        }
        artifact["session_id"] = Value::String(session_id);
        artifact["turn_id"] = Value::String(turn_id);
        return Ok(artifact);
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let settings = RigSettings {
        role: job.role,
        phase: Some(job.phase),
        tickers: job.tickers,
        output_mode: job.output_mode,
        llm,
        reasoning_effort_override: job.reasoning_effort_override,
        tools: Some(job.tools),
        web_search: job.web_search,
    };
    run_rig_agent_steer_loop(
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
        RoleJobResult {
            role: role.to_string(),
            phase: 3,
            kind: "artifact".to_string(),
            round: None,
            topic_id: None,
            tickers: vec!["QQQ".to_string()],
            artifact: if timed_out { None } else { Some(json!({})) },
            error: timed_out.then(|| "timeout".to_string()),
            timed_out,
            elapsed_ms,
        }
    }

    #[test]
    fn records_role_job_metrics_and_aggregates() {
        let mut state = json!({});

        record_role_job_metrics(&mut state, &result("manager.research", false, 7));
        record_role_job_metrics(&mut state, &result("trader", true, 11));

        assert_eq!(state["role_job_metrics"].as_array().unwrap().len(), 2);
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
