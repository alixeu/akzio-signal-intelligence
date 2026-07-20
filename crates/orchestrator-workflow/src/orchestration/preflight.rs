use anyhow::{bail, Result};
use orchestrator_ingest::{jin10, technical};
use serde_json::{json, Value};

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
        Some("read_technical_csv") | Some("run_technical_indicators") => Some("read_technical_csv"),
        Some("read_jin10_csv") | Some("fetch_jin10_flash") => Some("read_jin10_csv"),
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
        Some("read_technical_csv") => run_technical_csv_preflight(state).await,
        Some("read_jin10_csv") => run_jin10_preflight(conn, state).await,
        _ => Ok(()),
    }
}

pub(crate) async fn run_technical_csv_preflight(state: &mut Value) -> Result<()> {
    let tool = "read_technical_csv";
    if preflight_status(state, tool).is_some() {
        return Ok(());
    }
    if state
        .get("tech_refresh_enabled")
        .and_then(Value::as_bool)
        .is_some_and(|enabled| !enabled)
    {
        let csv_dir = orchestrator_core::default_technical_csv_dir();
        state["technical_csv_dir"] = json!(csv_dir.display().to_string());
        record_preflight_result(
            state,
            tool,
            Ok(json!({"status": "skipped", "reason": "tech_refresh_enabled=false"})),
        );
        return Ok(());
    }

    let symbols = state
        .get("analysis_universe")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|value| !value.is_empty());
    let end = state
        .get("current_date")
        .and_then(Value::as_str)
        .map(str::to_string);

    let result = technical::run(technical::TechnicalArgs {
        symbols,
        start: None,
        end,
        days: None,
        intervals: String::new(),
        timeout: None,
        sleep: None,
    })
    .await;

    let csv_dir = orchestrator_core::default_technical_csv_dir();
    if let Ok(value) = &result {
        if let Some(dir) = value.get("output_dir").and_then(|v| v.as_str()) {
            state["technical_csv_dir"] = json!(dir);
        } else {
            state["technical_csv_dir"] = json!(csv_dir.display().to_string());
        }
        if let Some(paths) = value.get("csv_paths") {
            state["technical_csv_paths"] = paths.clone();
        }
    } else {
        state["technical_csv_dir"] = json!(csv_dir.display().to_string());
    }
    record_preflight_result(state, tool, result);
    Ok(())
}

pub(crate) async fn run_jin10_preflight(
    _conn: &mut rusqlite::Connection,
    state: &mut Value,
) -> Result<()> {
    let tool = "read_jin10_csv";
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
        let items = payload
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let csv_rows: Vec<orchestrator_core::Jin10CsvRow> = items
            .iter()
            .filter_map(|item| {
                let time = item.get("time").and_then(Value::as_str)?;
                let content = item.get("content").and_then(Value::as_str)?;
                if time.is_empty() || content.is_empty() {
                    return None;
                }
                let id = orchestrator_sql::jin10_item_id(time, content);
                Some(orchestrator_core::Jin10CsvRow {
                    id,
                    time: time.to_string(),
                    content: content.to_string(),
                })
            })
            .collect();
        let date = state
            .get("current_date")
            .and_then(Value::as_str)
            .or_else(|| payload.get("fetched_at").and_then(Value::as_str))
            .unwrap_or("unknown")
            .to_string();
        let date_part = &date[..10.min(date.len())];
        let csv_dir = orchestrator_core::default_jin10_csv_dir();
        let csv_path = orchestrator_core::jin10_csv_path(&csv_dir, date_part);
        orchestrator_core::write_jin10_csv(&csv_path, &csv_rows)?;
        state["jin10_csv_path"] = json!(csv_path.display().to_string());
        Ok(json!({
            "status": "success",
            "csv_rows": csv_rows.len(),
            "csv_path": csv_path.display().to_string()
        }))
    });
    record_preflight_result(state, tool, result);
    Ok(())
}

fn preflight_status<'a>(state: &'a Value, name: &str) -> Option<&'a Value> {
    state.get("preflight").and_then(|items| items.get(name))
}
