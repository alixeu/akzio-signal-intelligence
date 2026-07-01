use anyhow::{Context, Result};
use orchestrator_core::replace_placeholders;
use serde_json::{json, Value};
use std::path::PathBuf;

use super::state::{tickers_from_state, topic_state};

pub(crate) fn mode_prompt_path(base: &std::path::Path, state: &Value) -> PathBuf {
    if state.get("mode").and_then(Value::as_str) != Some("monitor") {
        return base.to_path_buf();
    }
    let Some(stem) = base.file_stem().and_then(|value| value.to_str()) else {
        return base.to_path_buf();
    };
    let candidate = base.with_file_name(format!("{stem}_monitor.md"));
    if candidate.exists() {
        candidate
    } else {
        base.to_path_buf()
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn render_prompt(
    state: &Value,
    role: &str,
    phase: i64,
    kind: &str,
    round: Option<i64>,
    topic_id: Option<&str>,
    prompt_path: Option<&std::path::Path>,
) -> Result<String> {
    let tickers = tickers_from_state(state);
    let template = if let Some(path) = prompt_path {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read prompt template {}", path.display()))?
    } else {
        "Return only artifact JSON for role {role}, kind {kind}, phase {phase}, and tickers {tickers}. Include per_ticker for every ticker.".to_string()
    };
    let current_topic_state = topic_id
        .and_then(|id| topic_state(state, id))
        .unwrap_or(Value::Null);
    let current_topic = current_topic_state
        .get("topic")
        .cloned()
        .unwrap_or(Value::Null);
    let current_controller = current_topic_state
        .get("controller_artifact")
        .cloned()
        .unwrap_or(Value::Null);
    let blocked_repeats = current_controller
        .get("blocked_repeats")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let next_agenda = current_controller
        .get("next_agenda")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let values = json!({
        "run_id": state.get("run_id").and_then(Value::as_str).unwrap_or(""),
        "ticker": state.get("ticker").and_then(Value::as_str).unwrap_or(""),
        "tickers": tickers.join(","),
        "date": state.get("current_date").and_then(Value::as_str).unwrap_or(""),
        "lang": state.get("lang").and_then(Value::as_str).unwrap_or("zh"),
        "window_days": state.get("window_days").cloned().unwrap_or(Value::Null),
        "role": role,
        "phase": phase,
        "kind": kind,
        "round": round.unwrap_or_default(),
        "topic_id": topic_id.unwrap_or(""),
        "topic": serde_json::to_string_pretty(&current_topic)?,
        "blocked_repeats": serde_json::to_string_pretty(&blocked_repeats)?,
        "next_agenda": serde_json::to_string_pretty(&next_agenda)?,
        "workflow_pattern": "Workflow -> Stage/Sub-workflow -> Agent workers -> Reducer -> state artifact"
    });
    Ok(replace_placeholders(&template, &values))
}
