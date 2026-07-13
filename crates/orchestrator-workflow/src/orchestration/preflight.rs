use anyhow::{bail, Result};
use orchestrator_ingest::{jin10, technical};
use orchestrator_sql::import_jin10_payload;
use serde_json::{json, Value};
use std::path::PathBuf;

use super::config::RuntimeConfig;
use super::degraded::record_preflight_result;

pub(crate) fn enforce_preflight_policy(
    state: &mut Value,
    role: &str,
    #[allow(unused_variables)] config: &RuntimeConfig,
) -> Result<()> {
    let Some(tool) = preflight_tool_for_role_with_config(role, config) else {
        return Ok(());
    };
    let Some(status) = preflight_status(state, tool) else {
        return Ok(());
    };
    if status.get("status").and_then(Value::as_str) == Some("error") {
        let message = status
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("preflight failed")
            .to_string();
        if super::config::is_critical_role(config, role) {
            bail!("critical preflight {tool} for role {role} failed: {message}");
        }
        if !state.get("degraded_roles").is_some_and(Value::is_array) {
            state["degraded_roles"] = json!([]);
        }
        if let Some(items) = state["degraded_roles"].as_array_mut() {
            items.push(json!({
                "role": role,
                "phase": 1,
                "kind": "preflight",
                "tool": tool,
                "message": message
            }));
        }
    }
    Ok(())
}

fn preflight_tool_for_role_with_config(role: &str, config: &RuntimeConfig) -> Option<&'static str> {
    preflight_tool_from_registry(role, &config.agent_registry)
}

fn preflight_tool_from_registry(
    role: &str,
    registry: &orchestrator_core::role_registry::AgentRegistry,
) -> Option<&'static str> {
    match registry
        .get(role)
        .and_then(|def| def.preflight_tool.as_deref())
    {
        Some("run_technical_indicators") => Some("run_technical_indicators"),
        Some("fetch_jin10_flash") => Some("fetch_jin10_flash"),
        _ => None,
    }
}

pub(crate) async fn run_phase1_preflight(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    role: &str,
    #[allow(unused_variables)] config: &RuntimeConfig,
) -> Result<()> {
    match preflight_tool_for_role_with_config(role, config) {
        Some("run_technical_indicators") => run_technical_preflight(state).await,
        Some("fetch_jin10_flash") => run_jin10_preflight(conn, state).await,
        _ => Ok(()),
    }
}

pub(crate) async fn run_technical_preflight(state: &mut Value) -> Result<()> {
    let tool = "run_technical_indicators";
    if preflight_status(state, tool).is_some() {
        return Ok(());
    }
    if state
        .get("tech_refresh_enabled")
        .and_then(Value::as_bool)
        .is_some_and(|enabled| !enabled)
    {
        record_preflight_result(
            state,
            tool,
            Ok(json!({"status": "skipped", "reason": "tech_refresh_enabled=false"})),
        );
        return Ok(());
    }
    let db_path = state
        .get("db_path")
        .and_then(Value::as_str)
        .map(PathBuf::from);
    let result = technical::run(technical::TechnicalArgs {
        symbols: None,
        start: None,
        end: None,
        days: None,
        intervals: String::new(),
        db_path,
        model: None,
        timeout: None,
        sleep: None,
    })
    .await;
    record_preflight_result(state, tool, result);
    Ok(())
}

pub(crate) async fn run_jin10_preflight(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
) -> Result<()> {
    let tool = "fetch_jin10_flash";
    if preflight_status(state, tool).is_some() {
        return Ok(());
    }
    let lookback_hours = state
        .get("jin10_lookback_hours")
        .and_then(Value::as_f64)
        .unwrap_or(24.0);
    let result = jin10::run(jin10::Jin10Args {
        channel: None,
        vip: None,
        classify: None,
        lookback_hours: Some(lookback_hours),
        pages: None,
        sleep: None,
        timeout: None,
        output: String::new(),
        jsonl: String::new(),
        pretty: false,
    })
    .await
    .and_then(|payload| {
        let imported = import_jin10_payload(conn, &payload)?;
        Ok(json!({
            "status": "success",
            "imported_rows": imported,
            "payload": payload
        }))
    });
    record_preflight_result(state, tool, result);
    Ok(())
}

fn preflight_status<'a>(state: &'a Value, name: &str) -> Option<&'a Value> {
    state.get("preflight").and_then(|items| items.get(name))
}
