use crate::schema::{canonical_json, ensure_run_exists, now_ms, payload_hash, AGGREGATE_TICKER};
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
        r#"
        INSERT INTO runs
            (run_id,current_date,created_at_ms,status,current_phase,error_message,completed_at_ms,
             run_dir,db_path,git_sha,config_hash,artifact_path,workflow_version,
             prompt_versions_json,degraded,phase_count,total_elapsed_ms)
        VALUES (?1,?2,?3,'running',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'{}',0,0,0)
        ON CONFLICT(run_id) DO UPDATE SET
            current_date = excluded.current_date,
            status = 'running',
            current_phase = NULL,
            error_message = NULL,
            completed_at_ms = NULL
        "#,
        rusqlite::params![input.run_id, input.current_date, now_ms()],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn upsert_agent_turn(conn: &Connection, input: &AgentTurnInput) -> Result<()> {
    ensure_run_exists(
        conn,
        &input.run_id,
        &chrono::Utc::now()
            .date_naive()
            .format("%Y-%m-%d")
            .to_string(),
    )?;
    let full_context = input
        .full_context_json
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("agent full_context_json must be an array"))?;
    let previous = crate::context::latest_role_history_items(
        conn,
        &input.run_id,
        &input.role,
        Some(&input.turn_id),
    )?;
    let debug_full = std::env::var("ORCHESTRATOR_SQL_DEBUG_FULL_CONTEXT")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let can_delta = !previous.is_empty()
        && full_context.len() >= previous.len()
        && full_context[..previous.len()] == previous;
    let checkpoint = debug_full || !can_delta || input.turn_number % 10 == 0;
    let checkpoint_json = checkpoint
        .then(|| canonical_json(&input.full_context_json))
        .transpose()?;
    let delta = if checkpoint {
        Value::Array(Vec::new())
    } else {
        Value::Array(full_context[previous.len()..].to_vec())
    };
    let delta_json = canonical_json(&delta)?;
    let context_hash = payload_hash(&input.full_context_json)?;
    conn.execute(
        r#"
        INSERT INTO agent_events
            (turn_id,run_id,phase,turn_number,role,created_at_ms,full_context_json,
             context_delta_json,context_hash,summary,model,input_tokens,output_tokens,
             cached_tokens,reasoning_tokens,total_tokens,non_cached_input_tokens,
             visible_output_tokens,cost_usd,context_warning,elapsed_ms)
        VALUES (?,?,?,?,?,?,?,?,?,?,NULL,0,0,0,0,0,0,0,0.0,0,0)
        ON CONFLICT(turn_id) DO UPDATE SET
            run_id = excluded.run_id,
            phase = excluded.phase,
            turn_number = excluded.turn_number,
            role = excluded.role,
            full_context_json = excluded.full_context_json,
            context_delta_json = excluded.context_delta_json,
            context_hash = excluded.context_hash,
            summary = excluded.summary,
            created_at_ms = excluded.created_at_ms
        "#,
        params![
            input.turn_id,
            input.run_id,
            input.phase,
            input.turn_number,
            input.role,
            now_ms(),
            checkpoint_json,
            delta_json,
            context_hash,
            truncate_summary(&input.summary),
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
    ensure_run_exists(
        conn,
        &input.run_id,
        &chrono::Utc::now()
            .date_naive()
            .format("%Y-%m-%d")
            .to_string(),
    )?;
    let summary_json = canonical_json(&input.summary_json)?;
    let hash = payload_hash(&input.summary_json)?;
    conn.execute(
        r#"
        INSERT INTO role_turn_summaries
            (run_id,turn_id,role,phase,ticker,item_time_ms,topic_id,debate_id,
             summary_type,summary,summary_json,payload_schema_version,payload_hash,
             confidence,created_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        "#,
        params![
            input.run_id,
            input.turn_id,
            input.role,
            input.phase,
            input.ticker,
            seconds_or_millis_to_millis(input.item_time),
            input.topic_id,
            input.debate_id,
            input.summary_type,
            truncate_summary(&input.summary),
            summary_json,
            hash,
            input.confidence.clamp(0.0, 1.0),
            now_ms()
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

fn ticker_payloads(
    content: &Value,
    tickers: &[String],
    display_ticker_value: &str,
    _fan_out_without_per_ticker: bool,
) -> Result<Vec<(String, Scope, Value)>> {
    let Some(object) = content.as_object() else {
        bail!("agent message content must be a JSON object");
    };
    if let Some(Value::Object(per_ticker)) = object.get("per_ticker") {
        let mut payloads = Vec::new();
        for ticker in tickers {
            let Some(payload) = per_ticker.get(ticker).cloned() else {
                return Ok(vec![(
                    AGGREGATE_TICKER.to_string(),
                    Scope::Aggregate,
                    content.clone(),
                )]);
            };
            payloads.push((ticker.clone(), payload));
        }
        let distinct_hashes = payloads
            .iter()
            .map(|(_, payload)| payload_hash(payload))
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        if payloads.len() > 1 && distinct_hashes.len() == 1 {
            return Ok(vec![(
                AGGREGATE_TICKER.to_string(),
                Scope::Aggregate,
                content.clone(),
            )]);
        }
        return Ok(payloads
            .into_iter()
            .map(|(ticker, payload)| (ticker, Scope::Ticker, payload))
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
        return truncate_summary(last_md.trim());
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
                return truncate_summary(value.trim());
            }
        }
    }
    truncate_summary(&payload.to_string())
}

fn truncate_summary(value: &str) -> String {
    value.chars().take(2048).collect()
}

fn seconds_or_millis_to_millis(value: i64) -> i64 {
    if value.abs() < 100_000_000_000 {
        value.saturating_mul(1000)
    } else {
        value
    }
}

fn confidence_score(payload: &Value) -> f64 {
    payload
        .get("confidence")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
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
                    "confidence_basis": "evidence_balanced",
                    "hold_reason": "evidence_balanced",
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
        "confidence_basis": "evidence_balanced",
        "hold_reason": "evidence_balanced",
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
    let now = now_ms();
    conn.execute(
        r#"
        UPDATE runs
        SET status = ?1,
            error_message = COALESCE(?2, error_message),
            completed_at_ms = CASE WHEN ?1 IN ('completed','failed') THEN ?3 ELSE completed_at_ms END
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
