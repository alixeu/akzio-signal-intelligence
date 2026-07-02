use anyhow::{self, Result};
use orchestrator_llm::mock_role_artifact;
use serde_json::{json, Value};
use tracing::warn;

use super::config::{is_critical_role, RuntimeConfig};
use super::role_jobs::RoleJobResult;
use super::state::tickers_from_state;

enum ConfidenceImpact {
    Minor,
    Moderate,
}

impl ConfidenceImpact {
    fn as_str(&self) -> &'static str {
        match self {
            ConfidenceImpact::Minor => "Minor",
            ConfidenceImpact::Moderate => "Moderate",
        }
    }
}

struct DegradedEntry {
    role: String,
    phase: i64,
    error: String,
    used_fallback: bool,
    confidence_impact: ConfidenceImpact,
}

impl DegradedEntry {
    fn into_value(self) -> Value {
        let DegradedEntry {
            role,
            phase,
            error,
            used_fallback,
            confidence_impact,
        } = self;

        json!({
            "role": role,
            "phase": phase,
            "error": error,
            "used_fallback": used_fallback,
            "confidence_impact": confidence_impact.as_str(),
        })
    }
}

pub(crate) fn degraded_fallback(role: &str, tickers: &[String], error: &anyhow::Error) -> Value {
    let mut artifact = mock_role_artifact(role, tickers);
    artifact["status"] = json!("degraded");
    artifact["degraded"] = json!(true);
    artifact["error"] = json!(error.to_string());
    artifact["probability_rationale"] = json!(format!("{role} fallback used: {error}"));
    artifact
}

fn push_degraded_entry(state: &mut Value, entry: DegradedEntry) {
    if state.get("degraded_report").is_none() {
        state["degraded_report"] = json!({"is_degraded": false, "roles": []});
    }

    if let Some(report_val) = state.get_mut("degraded_report") {
        if let Some(roles) = report_val.get_mut("roles") {
            if let Some(arr) = roles.as_array_mut() {
                arr.push(entry.into_value());
            }
        }
        report_val["is_degraded"] = json!(true);
    }

    state["degraded"] = json!(true);
}

pub(crate) fn record_degraded_role(state: &mut Value, result: &RoleJobResult, message: &str) {
    state["degraded"] = Value::Bool(true);
    if !state.get("degraded_roles").is_some_and(Value::is_array) {
        state["degraded_roles"] = json!([]);
    }
    if let Some(items) = state["degraded_roles"].as_array_mut() {
        items.push(json!({
            "role": result.role,
            "phase": result.phase,
            "kind": result.kind,
            "round": result.round,
            "topic_id": result.topic_id,
            "timed_out": result.timed_out,
            "elapsed_ms": result.elapsed_ms,
            "message": message
        }));
    }
    if !state.get("missing_sources").is_some_and(Value::is_array) {
        state["missing_sources"] = json!([]);
    }
    if let Some(items) = state["missing_sources"].as_array_mut() {
        items.push(Value::String(result.role.clone()));
    }

    push_degraded_entry(
        state,
        DegradedEntry {
            role: result.role.clone(),
            phase: result.phase,
            error: message.to_string(),
            used_fallback: true,
            confidence_impact: ConfidenceImpact::Moderate,
        },
    );
}

pub(crate) fn degraded_role_artifact(result: &RoleJobResult, message: &str) -> Value {
    let base = degraded_fallback(
        &result.role,
        &result.tickers,
        &anyhow::anyhow!("{}", message),
    );
    let per_ticker = result
        .tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "status": "missing",
                    "direction": "neutral",
                    "confidence": 0.0,
                    "report": format!("{} did not produce usable evidence: {message}", result.role),
                    "error": message
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let mut artifact = json!({
        "id": result.role,
        "role": result.role,
        "phase": result.phase,
        "kind": result.kind,
        "round": result.round,
        "topic_id": result.topic_id,
        "timed_out": result.timed_out,
        "elapsed_ms": result.elapsed_ms,
        "per_ticker": per_ticker
    });
    if let Some(obj) = artifact.as_object_mut() {
        for (k, v) in base.as_object().into_iter().flat_map(|o| o.iter()) {
            obj.insert(k.clone(), v.clone());
        }
    }
    artifact
}

pub(crate) fn role_artifact_or_degraded(
    state: &mut Value,
    config: &RuntimeConfig,
    result: RoleJobResult,
) -> Result<Value> {
    if let Some(artifact) = result.artifact {
        return Ok(artifact);
    }
    let message = result
        .error
        .clone()
        .unwrap_or_else(|| "role execution failed".to_string());
    if is_critical_role(config, &result.role) {
        anyhow::bail!(
            "critical role {} failed in phase {} kind {}: {}",
            result.role,
            result.phase,
            result.kind,
            message
        );
    }
    warn!(
        role = result.role,
        phase = result.phase,
        kind = result.kind,
        timed_out = result.timed_out,
        elapsed_ms = result.elapsed_ms,
        message,
        "role degraded"
    );
    record_degraded_role(state, &result, &message);
    Ok(degraded_role_artifact(&result, &message))
}

pub(crate) fn record_preflight_result(state: &mut Value, name: &str, result: Result<Value>) {
    if !state.get("preflight").is_some_and(Value::is_object) {
        state["preflight"] = json!({});
    }
    match result {
        Ok(mut value) => {
            if value.get("status").is_none() {
                value["status"] = Value::String("success".to_string());
            }
            state["preflight"][name] = value;
        }
        Err(error) => {
            push_degraded_entry(
                state,
                DegradedEntry {
                    role: name.to_string(),
                    phase: 1,
                    error: error.to_string(),
                    used_fallback: true,
                    confidence_impact: ConfidenceImpact::Minor,
                },
            );
            state["preflight"][name] = json!({
                "status": "error",
                "message": error.to_string()
            });
        }
    }
}

pub(crate) fn manager_research_fallback(state: &mut Value, error: anyhow::Error) -> Value {
    let tickers = tickers_from_state(state);
    let artifact = degraded_fallback("manager.research", &tickers, &error);
    push_degraded_entry(
        state,
        DegradedEntry {
            role: "manager.research".to_string(),
            phase: 3,
            error: error.to_string(),
            used_fallback: true,
            confidence_impact: ConfidenceImpact::Moderate,
        },
    );
    artifact
}
