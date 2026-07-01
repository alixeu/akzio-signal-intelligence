use anyhow::{bail, Result};
use orchestrator_ingest::{jin10, social, technical};
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
    let Some(tool) = preflight_tool_for_role(role) else {
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

pub(crate) fn preflight_tool_for_role(role: &str) -> Option<&'static str> {
    let registry = orchestrator_core::role_registry::AgentRegistry::builtin();
    match registry
        .get(role)
        .and_then(|def| def.preflight_tool.as_deref())
    {
        Some("run_technical_indicators") => Some("run_technical_indicators"),
        Some("fetch_jin10_flash") => Some("fetch_jin10_flash"),
        Some("fetch_social_youtube") => Some("fetch_social_youtube"),
        Some("fetch_social_reddit") => Some("fetch_social_reddit"),
        Some("fetch_social_x") => Some("fetch_social_x"),
        _ => None,
    }
}

pub(crate) async fn run_phase1_preflight(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    role: &str,
    #[allow(unused_variables)] config: &RuntimeConfig,
) -> Result<()> {
    let registry = orchestrator_core::role_registry::AgentRegistry::builtin();
    let tool = registry
        .get(role)
        .and_then(|def| def.preflight_tool.clone());
    match tool.as_deref() {
        Some("run_technical_indicators") => run_technical_preflight(state).await,
        Some("fetch_jin10_flash") => run_jin10_preflight(conn, state).await,
        Some("fetch_social_youtube") => {
            run_social_preflight(conn, state, social::Source::Youtube).await
        }
        Some("fetch_social_reddit") => {
            run_social_preflight(conn, state, social::Source::Reddit).await
        }
        Some("fetch_social_x") => run_social_preflight(conn, state, social::Source::X).await,
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
        api_key: None,
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
    let result = jin10::run(jin10::Jin10Args {
        channel: None,
        vip: None,
        classify: None,
        lookback_hours: Some(24.0),
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

pub(crate) async fn run_social_preflight(
    conn: &mut rusqlite::Connection,
    state: &mut Value,
    source: social::Source,
) -> Result<()> {
    let tool = match source {
        social::Source::Youtube => "fetch_social_youtube",
        social::Source::Reddit => "fetch_social_reddit",
        social::Source::X => "fetch_social_x",
    };
    if preflight_status(state, tool).is_some() {
        return Ok(());
    }
    let tickers = super::state::tickers_from_state(state);
    let result = social::run(social::SocialArgs {
        source,
        query: String::new(),
        tickers: tickers.clone(),
        days: if matches!(source, social::Source::Youtube) {
            3
        } else {
            30
        },
        depth: social::Depth::Balanced,
        limit: None,
        subreddits: Vec::new(),
        output: None,
    })
    .await
    .and_then(|payload| import_social_payload(conn, &tickers, &payload, source));
    record_preflight_result(state, tool, result);
    Ok(())
}

fn import_social_payload(
    conn: &mut rusqlite::Connection,
    tickers: &[String],
    payload: &Value,
    source: social::Source,
) -> Result<Value> {
    use orchestrator_sql::write_source_item;
    let mut imported = 0usize;
    for ticker in tickers {
        for item in social_payload_items(payload) {
            let input = social_source_item_input(ticker, item, source);
            write_source_item(conn, &input)?;
            imported += 1;
        }
    }
    Ok(json!({
        "status": "success",
        "imported_rows": imported,
        "source": format!("{:?}", source)
    }))
}

fn social_payload_items(payload: &Value) -> Vec<&Value> {
    payload
        .get("items")
        .and_then(Value::as_array)
        .map(|a| a.iter().collect())
        .unwrap_or_default()
}

fn social_source_item_input(
    ticker: &str,
    item: &Value,
    source: social::Source,
) -> orchestrator_sql::SourceItemInput {
    let source_name = match source {
        social::Source::Youtube => "youtube",
        social::Source::Reddit => "reddit",
        social::Source::X => "x",
    };
    orchestrator_sql::SourceItemInput {
        source: source_name.to_string(),
        ticker: ticker.to_string(),
        item_key: social_key(item, &["id", "url", "link"]),
        item_time: social_time(item, &["published_at", "created_utc", "timestamp"]),
        content: social_text(item, &["body", "description", "selftext", "snippet"]),
        item_json: item.clone(),
    }
}

fn social_key(item: &Value, keys: &[&str]) -> String {
    for key in keys {
        if let Some(value) = item.get(*key).and_then(Value::as_str) {
            return value.to_string();
        }
    }
    String::new()
}

fn social_time(item: &Value, keys: &[&str]) -> String {
    for key in keys {
        if let Some(value) = item.get(*key) {
            if let Some(s) = value.as_str() {
                return s.to_string();
            }
            if let Some(n) = value.as_f64() {
                return n.to_string();
            }
        }
    }
    String::new()
}

fn social_text(item: &Value, keys: &[&str]) -> String {
    social_key(item, keys)
}

fn preflight_status<'a>(state: &'a Value, name: &str) -> Option<&'a Value> {
    state.get("preflight").and_then(|items| items.get(name))
}
