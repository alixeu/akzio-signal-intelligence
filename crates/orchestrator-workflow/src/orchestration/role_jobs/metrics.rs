use orchestrator_core::default_project_root;
use serde_json::{json, Value};

use super::RoleJobResult;

pub(crate) fn record_role_job_metrics(state: &mut Value, result: &RoleJobResult) {
    let status = if result.artifact.is_some() {
        "ok"
    } else {
        "degraded"
    };
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
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
            "run_id": run_id,
            "session_id": result.session_id,
            "turn_id": result.turn_id,
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
            "tool_call_count": result.tool_call_count,
            "retrieval_audit": result
                .artifact
                .as_ref()
                .and_then(|artifact| artifact.get("retrieval_audit"))
                .cloned()
                .unwrap_or(Value::Null)
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
                "prompt_version": result.prompt_version,
                "run_id": run_id,
                "session_id": result.session_id,
                "turn_id": result.turn_id,
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
                "prompt_version": result.prompt_version,
                "run_id": run_id,
                "session_id": result.session_id,
                "turn_id": result.turn_id,
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

pub(crate) fn refresh_role_job_metrics(state: &mut Value) {
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
