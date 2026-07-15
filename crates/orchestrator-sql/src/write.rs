use crate::schema::AGGREGATE_TICKER;
use anyhow::{bail, Result};
use orchestrator_core::{display_ticker, parse_tickers};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Ticker,
    Aggregate,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Ticker => "ticker",
            Scope::Aggregate => "aggregate",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentMessageInput {
    pub run_id: String,
    pub phase: i64,
    pub role: String,
    pub ticker: String,
    pub tickers: Vec<String>,
    pub skill: String,
    pub kind: String,
    pub topic_id: Option<String>,
    pub round: Option<i64>,
    pub message_group_id: Option<String>,
    pub valid: bool,
    pub content: Value,
    pub last_md: String,
}

#[derive(Debug, Clone)]
pub struct RunRecordInput<'a> {
    pub run_id: &'a str,
    pub current_date: &'a str,
}

#[derive(Debug, Clone)]
pub struct SourceItemInput {
    pub source: String,
    pub item_key: String,
    pub ticker: String,
    pub item_time: i64,
    pub content: String,
    pub item_json: Value,
}

#[derive(Debug, Clone)]
pub struct RoleTurnSummaryInput {
    pub run_id: String,
    pub turn_id: String,
    pub role: String,
    pub phase: Option<i64>,
    pub ticker: String,
    pub item_time: i64,
    pub topic_id: Option<String>,
    pub debate_id: Option<String>,
    pub summary_type: String,
    pub summary: String,
    pub summary_json: Value,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct AgentTurnInput {
    pub turn_id: String,
    pub run_id: String,
    pub phase: Option<i64>,
    pub turn_number: i64,
    pub role: String,
    pub full_context_json: Value,
    pub summary: String,
}

pub fn safe_ticker_value(ticker: &str, scope: Scope) -> Result<(&str, Scope)> {
    if scope == Scope::Ticker && ticker.contains(',') {
        bail!("ticker-scoped SQL rows cannot use comma-joined ticker {ticker:?}");
    }
    Ok((ticker, scope))
}

pub fn new_message_group_id(
    run_id: &str,
    phase: i64,
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    round: Option<i64>,
) -> String {
    let seed = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}",
        run_id,
        phase,
        role,
        kind,
        topic_id.unwrap_or_default(),
        round.map(|n| n.to_string()).unwrap_or_default(),
        chrono::Utc::now().timestamp(),
        Uuid::new_v4()
    );
    let mut hasher = Sha256::new();
    hasher.update(seed.as_bytes());
    format!("{:x}", hasher.finalize())[..24].to_string()
}

pub fn clear_agent_loop_history(conn: &Connection, run_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM agent_events WHERE run_id = ?1",
        rusqlite::params![run_id],
    )?;
    Ok(())
}

pub fn write_run_record(conn: &mut Connection, input: &RunRecordInput) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT OR REPLACE INTO runs (run_id, current_date, created_at, status) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![
            input.run_id,
            input.current_date,
            chrono::Utc::now().timestamp(),
            "running"
        ],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn upsert_agent_turn(conn: &Connection, input: &AgentTurnInput) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        r#"
        INSERT INTO agent_events
            (turn_id, run_id, phase, turn_number, role, created_at, full_context_json, summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(turn_id) DO UPDATE SET
            run_id = excluded.run_id,
            phase = excluded.phase,
            role = excluded.role,
            full_context_json = excluded.full_context_json,
            summary = excluded.summary
        "#,
        params![
            input.turn_id,
            input.run_id,
            input.phase,
            input.turn_number,
            input.role,
            now,
            serde_json::to_string(&input.full_context_json)?,
            input.summary,
        ],
    )?;
    Ok(())
}

pub fn write_role_turn_summary(conn: &Connection, input: &RoleTurnSummaryInput) -> Result<()> {
    if input.ticker.contains(',') {
        bail!(
            "role_turn_summaries cannot use comma-joined ticker {:?}",
            input.ticker
        );
    }
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        r#"
        INSERT INTO role_turn_summaries
            (run_id, turn_id, role, phase, ticker, item_time, topic_id, debate_id,
             summary_type, summary, summary_json, confidence, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        params![
            input.run_id,
            input.turn_id,
            input.role,
            input.phase,
            input.ticker,
            input.item_time,
            input.topic_id,
            input.debate_id,
            input.summary_type,
            input.summary,
            serde_json::to_string(&input.summary_json)?,
            input.confidence,
            now
        ],
    )?;
    let _id = conn.last_insert_rowid();
    Ok(())
}

pub fn write_agent_message_scoped(
    conn: &mut Connection,
    input: &AgentMessageInput,
) -> Result<usize> {
    let tickers = if input.tickers.is_empty() {
        parse_tickers(&input.ticker)
    } else {
        input.tickers.clone()
    };
    let group_id = input.message_group_id.clone().unwrap_or_else(|| {
        new_message_group_id(
            &input.run_id,
            input.phase,
            &input.role,
            &input.kind,
            input.topic_id.as_deref(),
            input.round,
        )
    });

    let rows = ticker_payloads(
        &input.content,
        &tickers,
        &input.ticker,
        input.phase >= 2 || input.topic_id.is_some(),
    )?;
    let tx = conn.transaction()?;
    let mut written = 0;
    for (ticker, scope, payload) in rows {
        let (ticker, scope) = safe_ticker_value(&ticker, scope)?;
        let now = chrono::Utc::now().timestamp();
        let _ = scope;
        write_role_turn_summary(
            &tx,
            &RoleTurnSummaryInput {
                run_id: input.run_id.clone(),
                turn_id: group_id.clone(),
                role: input.role.clone(),
                phase: Some(input.phase),
                ticker: ticker.to_string(),
                item_time: now,
                topic_id: input.topic_id.clone(),
                debate_id: None,
                summary_type: input.kind.clone(),
                summary: summary_text(&payload, &input.last_md),
                summary_json: payload.clone(),
                confidence: if input.valid {
                    confidence_score(&payload)
                } else {
                    0.0
                },
            },
        )?;
        written += 1;
    }
    tx.commit()?;
    Ok(written)
}

pub fn write_source_item(conn: &mut Connection, input: &SourceItemInput) -> Result<usize> {
    let item_json = serde_json::to_string(&input.item_json)?;
    let mut hasher = Sha256::new();
    hasher.update(item_json.as_bytes());
    let content_hash = format!("{:x}", hasher.finalize());
    let imported_at = chrono::Utc::now().timestamp();
    write_dedicated_source_item(conn, input, &item_json, &content_hash, imported_at)
}

fn write_dedicated_source_item(
    conn: &Connection,
    input: &SourceItemInput,
    item_json: &str,
    content_hash: &str,
    imported_at: i64,
) -> Result<usize> {
    let title = input
        .item_json
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or_default();
    conn.execute(
        r#"
        INSERT OR REPLACE INTO external_items
            (source, item_key, ticker, item_time, title, content, metadata_json, content_hash, imported_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        params![
            input.source,
            input.item_key,
            input.ticker,
            input.item_time,
            title,
            input.content,
            item_json,
            content_hash,
            imported_at
        ],
    )
    .map_err(Into::into)
}

fn ticker_payloads(
    content: &Value,
    tickers: &[String],
    display_ticker_value: &str,
    fan_out_without_per_ticker: bool,
) -> Result<Vec<(String, Scope, Value)>> {
    let Some(object) = content.as_object() else {
        bail!("agent message content must be a JSON object");
    };
    if let Some(Value::Object(per_ticker)) = object.get("per_ticker") {
        let mut rows = Vec::new();
        for ticker in tickers {
            let payload = per_ticker
                .get(ticker)
                .cloned()
                .unwrap_or_else(|| content.clone());
            rows.push((ticker.clone(), Scope::Ticker, payload));
        }
        return Ok(rows);
    }

    if tickers.len() > 1 && fan_out_without_per_ticker {
        return Ok(tickers
            .iter()
            .map(|ticker| (ticker.clone(), Scope::Ticker, content.clone()))
            .collect());
    }

    if tickers.len() > 1 {
        return Ok(vec![(
            AGGREGATE_TICKER.to_string(),
            Scope::Aggregate,
            content.clone(),
        )]);
    }

    let ticker = tickers
        .first()
        .cloned()
        .unwrap_or_else(|| display_ticker(&parse_tickers(display_ticker_value)));
    if ticker.is_empty() {
        bail!("ticker is required for ticker-scoped write");
    }
    Ok(vec![(ticker, Scope::Ticker, content.clone())])
}

fn summary_text(payload: &Value, last_md: &str) -> String {
    if !last_md.trim().is_empty() {
        return last_md.trim().to_string();
    }
    for key in [
        "summary",
        "report",
        "probability_rationale",
        "plan",
        "status",
    ] {
        if let Some(value) = payload.get(key).and_then(Value::as_str) {
            if !value.trim().is_empty() {
                return value.trim().to_string();
            }
        }
    }
    payload.to_string()
}

fn confidence_score(payload: &Value) -> f64 {
    payload
        .get("confidence")
        .and_then(Value::as_f64)
        .unwrap_or(0.5)
        .clamp(0.0, 1.0)
}

pub fn split_artifact_per_ticker(content: &Value, tickers: &[String]) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    if let Some(per_ticker) = content.get("per_ticker").and_then(Value::as_object) {
        for ticker in tickers {
            if let Some(payload) = per_ticker.get(ticker) {
                out.insert(ticker.clone(), payload.clone());
            }
        }
    } else if tickers.len() == 1 {
        out.insert(tickers[0].clone(), content.clone());
    } else {
        out.insert(AGGREGATE_TICKER.to_string(), content.clone());
    }
    out
}

pub fn mock_research_artifact(tickers: &[String]) -> Value {
    let per_ticker = tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "rating": "Hold",
                    "long_probability": 0.5,
                    "short_probability": 0.5,
                    "plan": format!("Mock probability analysis for {ticker}."),
                    "probability_rationale": "Deterministic mock artifact generated by Rust orchestrator."
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "id": "manager.research",
        "role": "manager.research",
        "rating": "Hold",
        "long_probability": 0.5,
        "short_probability": 0.5,
        "plan": "Mock probability analysis.",
        "probability_rationale": "Deterministic mock artifact generated by Rust orchestrator.",
        "per_ticker": per_ticker
    })
}

pub fn update_run_status(
    conn: &mut Connection,
    run_id: &str,
    status: &str,
    error_message: Option<&str>,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        r#"
        UPDATE runs
        SET status = ?1,
            error_message = COALESCE(?2, error_message),
            completed_at = CASE WHEN ?1 IN ('completed','failed') THEN ?3 ELSE completed_at END
        WHERE run_id = ?4
        "#,
        params![status, error_message, now, run_id],
    )?;
    Ok(())
}

pub fn set_run_current_phase(conn: &mut Connection, run_id: &str, phase: i64) -> Result<()> {
    conn.execute(
        "UPDATE runs SET current_phase = ?1, status = 'running' WHERE run_id = ?2",
        params![phase, run_id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect;

    #[test]
    fn update_run_status_sets_completed_at() {
        let temp = tempfile::tempdir().unwrap();
        let mut conn = connect(temp.path().join("run.sqlite")).unwrap();
        write_run_record(
            &mut conn,
            &RunRecordInput {
                run_id: "run-1",
                current_date: "2026-01-01",
            },
        )
        .unwrap();
        update_run_status(&mut conn, "run-1", "completed", None).unwrap();
        let (status, completed_at): (String, Option<i64>) = conn
            .query_row(
                "SELECT status, completed_at FROM runs WHERE run_id = ?1",
                params!["run-1"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert!(completed_at.is_some());
    }
}
