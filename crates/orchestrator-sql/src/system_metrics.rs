use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection};
use serde_json::Value;

use crate::ensure_schema;

#[derive(Debug, Clone)]
pub struct SystemMetricsCopyInput {
    pub run_id: String,
    pub workflow_version: String,
    pub reflection_version: String,
    pub agent_count: i64,
    pub prediction_date: String,
    pub ticker: String,
    pub market_regime_json: Value,
}

pub fn rewrite_system_metrics_from_prompt_metrics(
    conn: &Connection,
    input: &SystemMetricsCopyInput,
) -> Result<usize> {
    ensure_schema(conn)?;
    let now = Utc::now().to_rfc3339();
    let market_regime_json = serde_json::to_string(&input.market_regime_json)?;
    let updated = conn.execute(
        r#"
        UPDATE system_metrics
        SET workflow_version = ?,
            reflection_version = ?,
            agent_count = ?,
            prediction_date = ?,
            ticker = ?,
            market_regime_json = ?
        WHERE run_id = ?
          AND prompt_metric_id IS NOT NULL
        "#,
        params![
            input.workflow_version,
            input.reflection_version,
            input.agent_count,
            input.prediction_date,
            input.ticker,
            market_regime_json,
            input.run_id,
        ],
    )?;
    let inserted = conn.execute(
        r#"
        INSERT OR IGNORE INTO system_metrics
            (run_id, role, phase, model, prompt_version, prompt_tokens, completion_tokens,
             total_tokens, latency_ms, workflow_version, reflection_version, agent_count,
             prediction_date, ticker, market_regime_json, prompt_metric_id, created_at)
        SELECT pm.run_id, pm.role, pm.phase, pm.model, pm.prompt_version, pm.input_tokens,
               pm.output_tokens, pm.total_tokens, pm.latency_ms, ?, ?, ?, ?, ?, ?, pm.id, ?
        FROM prompt_metrics pm
        WHERE pm.run_id = ?
          AND NOT EXISTS (
              SELECT 1
              FROM system_metrics sm
              WHERE sm.prompt_metric_id = pm.id
          )
        "#,
        params![
            input.workflow_version,
            input.reflection_version,
            input.agent_count,
            input.prediction_date,
            input.ticker,
            market_regime_json,
            now,
            input.run_id,
        ],
    )?;
    Ok(updated + inserted)
}

pub fn system_metrics_count(conn: &Connection, run_id: &str) -> Result<i64> {
    ensure_schema(conn)?;
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM system_metrics WHERE run_id = ?",
        params![run_id],
        |row| row.get(0),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connect,
        metrics::{insert_prompt_metric, PromptMetricInput},
    };
    use serde_json::json;

    #[test]
    fn rewrites_system_metrics_from_prompt_metrics() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("system.sqlite")).unwrap();
        insert_prompt_metric(
            &conn,
            &PromptMetricInput {
                run_id: "run-1".to_string(),
                turn_id: "turn-1".to_string(),
                session_id: "s".to_string(),
                role: "manager.research".to_string(),
                phase: Some(3),
                kind: "artifact".to_string(),
                round: None,
                topic_id: None,
                prompt_version: "v1".to_string(),
                model: "m".to_string(),
                input_tokens: 10,
                output_tokens: 5,
                cached_tokens: 0,
                total_tokens: 15,
                turn_count: 1,
                tool_call_count: 0,
                latency_ms: 20,
                validation_result: "pass".to_string(),
                fallback_triggered: false,
                error_message: String::new(),
            },
        )
        .unwrap();
        let copied = rewrite_system_metrics_from_prompt_metrics(
            &conn,
            &SystemMetricsCopyInput {
                run_id: "run-1".to_string(),
                workflow_version: "v1".to_string(),
                reflection_version: "v1".to_string(),
                agent_count: 2,
                prediction_date: "2026-01-01".to_string(),
                ticker: "QQQ".to_string(),
                market_regime_json: json!({}),
            },
        )
        .unwrap();
        assert_eq!(copied, 1);
        assert_eq!(system_metrics_count(&conn, "run-1").unwrap(), 1);
    }
}
