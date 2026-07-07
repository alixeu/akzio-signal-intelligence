use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptMetricInput {
    pub run_id: String,
    pub turn_id: String,
    pub session_id: String,
    pub role: String,
    pub phase: Option<i64>,
    pub kind: String,
    pub round: Option<i64>,
    pub topic_id: Option<String>,
    pub prompt_version: String,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub total_tokens: u64,
    pub turn_count: u64,
    pub tool_call_count: u64,
    pub latency_ms: u64,
    pub validation_result: String,
    pub fallback_triggered: bool,
    pub error_message: String,
}

pub fn insert_prompt_metric(conn: &Connection, metric: &PromptMetricInput) -> Result<()> {
    let created_at = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO prompt_metrics (
            run_id, turn_id, session_id, role, phase, kind, round, topic_id,
            prompt_version, model, input_tokens, output_tokens, cached_tokens,
            total_tokens, turn_count, tool_call_count, latency_ms,
            validation_result, fallback_triggered, error_message, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
        rusqlite::params![
            metric.run_id,
            metric.turn_id,
            metric.session_id,
            metric.role,
            metric.phase,
            metric.kind,
            metric.round,
            metric.topic_id,
            metric.prompt_version,
            metric.model,
            u64_to_i64(metric.input_tokens),
            u64_to_i64(metric.output_tokens),
            u64_to_i64(metric.cached_tokens),
            u64_to_i64(metric.total_tokens),
            u64_to_i64(metric.turn_count),
            u64_to_i64(metric.tool_call_count),
            u64_to_i64(metric.latency_ms),
            metric.validation_result,
            metric.fallback_triggered as i64,
            metric.error_message,
            created_at,
        ],
    )
    .context("failed to insert prompt metric")?;
    Ok(())
}

pub fn query_metrics_by_run(conn: &Connection, run_id: &str) -> Result<Vec<Value>> {
    query_metrics(conn, Some(run_id), None)
}

pub fn query_metrics_by_run_and_role(
    conn: &Connection,
    run_id: Option<&str>,
    role: Option<&str>,
) -> Result<Vec<Value>> {
    query_metrics(conn, run_id, role)
}

pub fn query_summary(conn: &Connection) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT role,
                COUNT(*) AS invocations,
                COALESCE(SUM(input_tokens), 0) AS total_input,
                COALESCE(SUM(output_tokens), 0) AS total_output,
                COALESCE(SUM(cached_tokens), 0) AS total_cached,
                COALESCE(SUM(total_tokens), 0) AS total_tokens,
                COALESCE(AVG(latency_ms), 0) AS avg_latency,
                COALESCE(SUM(CASE WHEN validation_result = 'pass' THEN 1 ELSE 0 END), 0) AS pass_count,
                COALESCE(SUM(CASE WHEN fallback_triggered = 1 THEN 1 ELSE 0 END), 0) AS fallback_count
         FROM prompt_metrics
         GROUP BY role
         ORDER BY total_input DESC, role ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(json!({
            "role": row.get::<_, String>(0)?,
            "invocations": row.get::<_, i64>(1)?,
            "total_input": row.get::<_, i64>(2)?,
            "total_output": row.get::<_, i64>(3)?,
            "total_cached": row.get::<_, i64>(4)?,
            "total_tokens": row.get::<_, i64>(5)?,
            "avg_latency_ms": row.get::<_, f64>(6)?,
            "pass_count": row.get::<_, i64>(7)?,
            "fallback_count": row.get::<_, i64>(8)?,
        }))
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn query_metrics(
    conn: &Connection,
    run_id: Option<&str>,
    role: Option<&str>,
) -> Result<Vec<Value>> {
    let (sql, params): (&str, Vec<&str>) = match (run_id, role) {
        (Some(run_id), Some(role)) => (
            "SELECT id, run_id, turn_id, session_id, role, phase, kind, round, topic_id,
                    prompt_version, model, input_tokens, output_tokens, cached_tokens,
                    total_tokens, turn_count, tool_call_count, latency_ms,
                    validation_result, fallback_triggered, error_message, created_at
             FROM prompt_metrics WHERE run_id = ?1 AND role = ?2 ORDER BY id",
            vec![run_id, role],
        ),
        (Some(run_id), None) => (
            "SELECT id, run_id, turn_id, session_id, role, phase, kind, round, topic_id,
                    prompt_version, model, input_tokens, output_tokens, cached_tokens,
                    total_tokens, turn_count, tool_call_count, latency_ms,
                    validation_result, fallback_triggered, error_message, created_at
             FROM prompt_metrics WHERE run_id = ?1 ORDER BY id",
            vec![run_id],
        ),
        (None, Some(role)) => (
            "SELECT id, run_id, turn_id, session_id, role, phase, kind, round, topic_id,
                    prompt_version, model, input_tokens, output_tokens, cached_tokens,
                    total_tokens, turn_count, tool_call_count, latency_ms,
                    validation_result, fallback_triggered, error_message, created_at
             FROM prompt_metrics WHERE role = ?1 ORDER BY id",
            vec![role],
        ),
        (None, None) => (
            "SELECT id, run_id, turn_id, session_id, role, phase, kind, round, topic_id,
                    prompt_version, model, input_tokens, output_tokens, cached_tokens,
                    total_tokens, turn_count, tool_call_count, latency_ms,
                    validation_result, fallback_triggered, error_message, created_at
             FROM prompt_metrics ORDER BY id",
            vec![],
        ),
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), row_to_json)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn row_to_json(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(json!({
        "id": row.get::<_, i64>(0)?,
        "run_id": row.get::<_, String>(1)?,
        "turn_id": row.get::<_, String>(2)?,
        "session_id": row.get::<_, String>(3)?,
        "role": row.get::<_, String>(4)?,
        "phase": row.get::<_, Option<i64>>(5)?,
        "kind": row.get::<_, String>(6)?,
        "round": row.get::<_, Option<i64>>(7)?,
        "topic_id": row.get::<_, Option<String>>(8)?,
        "prompt_version": row.get::<_, String>(9)?,
        "model": row.get::<_, String>(10)?,
        "input_tokens": row.get::<_, i64>(11)?,
        "output_tokens": row.get::<_, i64>(12)?,
        "cached_tokens": row.get::<_, i64>(13)?,
        "total_tokens": row.get::<_, i64>(14)?,
        "turn_count": row.get::<_, i64>(15)?,
        "tool_call_count": row.get::<_, i64>(16)?,
        "latency_ms": row.get::<_, i64>(17)?,
        "validation_result": row.get::<_, String>(18)?,
        "fallback_triggered": row.get::<_, i64>(19)? != 0,
        "error_message": row.get::<_, String>(20)?,
        "created_at": row.get::<_, String>(21)?,
    }))
}

fn u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect;

    #[test]
    fn connect_creates_prompt_metrics_table() {
        let conn = Connection::open_in_memory().unwrap();
        crate::ensure_schema(&conn).unwrap();

        let columns = conn
            .prepare("PRAGMA table_info(prompt_metrics)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        assert!(columns.contains(&"run_id".to_string()));
        assert!(columns.contains(&"input_tokens".to_string()));
        assert!(columns.contains(&"fallback_triggered".to_string()));
    }

    #[test]
    fn insert_and_query_prompt_metrics_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let conn = connect(dir.path().join("metrics.sqlite")).unwrap();
        insert_prompt_metric(
            &conn,
            &PromptMetricInput {
                run_id: "run-1".to_string(),
                turn_id: "turn-1".to_string(),
                session_id: "session-1".to_string(),
                role: "analyst.technical".to_string(),
                phase: Some(1),
                kind: "artifact".to_string(),
                round: Some(2),
                topic_id: Some("topic-1".to_string()),
                prompt_version: "v2".to_string(),
                model: "test-model".to_string(),
                input_tokens: 10,
                output_tokens: 20,
                cached_tokens: 3,
                total_tokens: 30,
                turn_count: 2,
                tool_call_count: 1,
                latency_ms: 42,
                validation_result: "pass".to_string(),
                fallback_triggered: false,
                error_message: String::new(),
            },
        )
        .unwrap();

        let metrics = query_metrics_by_run(&conn, "run-1").unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0]["role"], "analyst.technical");
        assert_eq!(metrics[0]["input_tokens"], 10);
        assert_eq!(metrics[0]["output_tokens"], 20);
        assert_eq!(metrics[0]["cached_tokens"], 3);
        assert_eq!(metrics[0]["total_tokens"], 30);
        assert_eq!(metrics[0]["turn_count"], 2);
        assert_eq!(metrics[0]["tool_call_count"], 1);

        let summary = query_summary(&conn).unwrap();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0]["invocations"], 1);
        assert_eq!(summary[0]["total_input"], 10);
    }
}
