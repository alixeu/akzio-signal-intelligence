use anyhow::{self, Result};
#[cfg(test)]
use orchestrator_llm::agent_loop::TokenUsage;
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

/// Honest degraded artifact for a failed role.
///
/// Deliberately does **not** call `mock_role_artifact`: mock payloads carry
/// synthetic `direction=neutral/confidence=0.5` (or research Hold@0.5) that look
/// like real evidence downstream. Degraded paths must emit unobserved / missing
/// markers with confidence 0 so weighted bases and policy treat them as
/// non-contributing.
pub(crate) fn degraded_fallback(role: &str, tickers: &[String], error: &anyhow::Error) -> Value {
    let error_text = error.to_string();
    if role == "manager.research" {
        return degraded_research_artifact(tickers, &error_text);
    }

    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "status": "missing",
                    "direction": "unobserved",
                    "confidence": 0.0,
                    "report": format!("{role} did not produce usable evidence: {error_text}"),
                    "data_gaps": [format!("{role} degraded: {error_text}")],
                    "error": error_text,
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();

    json!({
        "id": role,
        "role": role,
        "status": "degraded",
        "degraded": true,
        "usable": false,
        "error": error_text,
        "report": format!("{role} fallback used: {error_text}"),
        "probability_rationale": format!("{role} fallback used: {error_text}"),
        "per_ticker": per_ticker,
    })
}

fn degraded_research_artifact(tickers: &[String], error_text: &str) -> Value {
    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "status": "missing",
                    "rating": "Hold",
                    "long_probability": Value::Null,
                    "short_probability": Value::Null,
                    "confidence": 0.0,
                    "plan": format!("manager.research degraded for {ticker}: {error_text}"),
                    "probability_rationale": format!(
                        "manager.research fallback used; probabilities unavailable: {error_text}"
                    ),
                    "error": error_text,
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();

    json!({
        "id": "manager.research",
        "role": "manager.research",
        "status": "degraded",
        "degraded": true,
        "usable": false,
        "rating": "Hold",
        "long_probability": Value::Null,
        "short_probability": Value::Null,
        "confidence": 0.0,
        "plan": format!("manager.research degraded: {error_text}"),
        "probability_rationale": format!(
            "manager.research fallback used; probabilities unavailable: {error_text}"
        ),
        "error": error_text,
        "per_ticker": per_ticker,
    })
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
    // Build the honest missing payload first, then only copy non-conflicting
    // envelope metadata. Never let a mock `per_ticker` overwrite the missing
    // markers — that was the P0 bug that turned failed analysts into fake
    // neutral/0.5 votes.
    let mut artifact = degraded_fallback(
        &result.role,
        &result.tickers,
        &anyhow::anyhow!("{}", message),
    );
    if let Some(obj) = artifact.as_object_mut() {
        obj.insert("phase".to_string(), json!(result.phase));
        obj.insert("kind".to_string(), json!(result.kind));
        obj.insert("round".to_string(), json!(result.round));
        obj.insert("topic_id".to_string(), json!(result.topic_id));
        obj.insert("timed_out".to_string(), json!(result.timed_out));
        obj.insert("elapsed_ms".to_string(), json!(result.elapsed_ms));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn degraded_analyst_artifact_is_unobserved_not_mock_neutral() {
        let result = RoleJobResult {
            role: "analyst.youtube".to_string(),
            phase: 1,
            kind: "artifact".to_string(),
            round: None,
            topic_id: None,
            tickers: vec!["QQQ".to_string()],
            prompt_version: None,
            model: "test".to_string(),
            turn_id: "turn".to_string(),
            session_id: "session".to_string(),
            artifact: None,
            error: Some("timeout".to_string()),
            timed_out: false,
            elapsed_ms: 12,
            usage: TokenUsage::default(),
            turn_count: 0,
            tool_call_count: 0,
        };
        let artifact = degraded_role_artifact(&result, "timeout");
        assert_eq!(artifact["degraded"], json!(true));
        assert_eq!(artifact["usable"], json!(false));
        assert_eq!(
            artifact["per_ticker"]["QQQ"]["direction"],
            json!("unobserved")
        );
        assert_eq!(artifact["per_ticker"]["QQQ"]["confidence"], json!(0.0));
        assert_ne!(artifact["per_ticker"]["QQQ"]["confidence"], json!(0.5));
    }

    #[test]
    fn degraded_research_artifact_does_not_emit_fake_half_probabilities() {
        let artifact = degraded_fallback(
            "manager.research",
            &["QQQ".to_string()],
            &anyhow::anyhow!("llm failed"),
        );
        assert_eq!(artifact["degraded"], json!(true));
        assert_eq!(artifact["usable"], json!(false));
        assert!(artifact["long_probability"].is_null());
        assert!(artifact["short_probability"].is_null());
        assert_eq!(artifact["confidence"], json!(0.0));
    }
}
