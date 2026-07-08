use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use crate::ensure_schema;

#[derive(Debug, Clone)]
pub struct RunArchiveInput {
    pub run_id: String,
    pub ticker: String,
    pub tickers_json: Value,
    pub prediction_date: String,
    pub workflow_version: String,
    pub prompt_versions_json: Value,
    pub git_sha: String,
    pub config_hash: String,
    pub market_regime_json: Value,
    pub artifact_path: String,
    pub state_summary_json: Value,
    pub research_plan_json: Value,
    pub degraded: bool,
    pub phase_count: i64,
    pub total_elapsed_ms: i64,
}

pub fn upsert_run_archive(conn: &Connection, input: &RunArchiveInput) -> Result<()> {
    ensure_schema(conn)?;
    let now = Utc::now().to_rfc3339();
    conn.execute(
        r#"
        INSERT INTO run_archive
            (run_id, ticker, tickers_json, prediction_date, workflow_version, prompt_versions_json,
             git_sha, config_hash, market_regime_json, artifact_path, state_summary_json,
             research_plan_json, degraded, phase_count, total_elapsed_ms, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            ticker = excluded.ticker,
            tickers_json = excluded.tickers_json,
            prediction_date = excluded.prediction_date,
            workflow_version = excluded.workflow_version,
            prompt_versions_json = excluded.prompt_versions_json,
            git_sha = excluded.git_sha,
            config_hash = excluded.config_hash,
            market_regime_json = excluded.market_regime_json,
            artifact_path = excluded.artifact_path,
            state_summary_json = excluded.state_summary_json,
            research_plan_json = excluded.research_plan_json,
            degraded = excluded.degraded,
            phase_count = excluded.phase_count,
            total_elapsed_ms = excluded.total_elapsed_ms
        "#,
        params![
            input.run_id,
            input.ticker,
            serde_json::to_string(&input.tickers_json)?,
            input.prediction_date,
            input.workflow_version,
            serde_json::to_string(&input.prompt_versions_json)?,
            input.git_sha,
            input.config_hash,
            serde_json::to_string(&input.market_regime_json)?,
            input.artifact_path,
            serde_json::to_string(&input.state_summary_json)?,
            serde_json::to_string(&input.research_plan_json)?,
            input.degraded as i64,
            input.phase_count,
            input.total_elapsed_ms,
            now,
        ],
    )?;
    Ok(())
}

pub fn run_archive_by_id(conn: &Connection, run_id: &str) -> Result<Value> {
    ensure_schema(conn)?;
    let text = conn.query_row(
        r#"
        SELECT json_object(
            'run_id', run_id,
            'ticker', ticker,
            'tickers_json', json(tickers_json),
            'prediction_date', prediction_date,
            'workflow_version', workflow_version,
            'prompt_versions_json', json(prompt_versions_json),
            'market_regime_json', json(market_regime_json),
            'artifact_path', artifact_path,
            'state_summary_json', json(state_summary_json),
            'research_plan_json', json(research_plan_json),
            'degraded', degraded,
            'phase_count', phase_count,
            'total_elapsed_ms', total_elapsed_ms,
            'created_at', created_at
        )
        FROM run_archive WHERE run_id = ?
        "#,
        params![run_id],
        |row| row.get::<_, String>(0),
    )?;
    Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect;

    #[test]
    fn upserts_run_archive() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("archive.sqlite")).unwrap();
        upsert_run_archive(
            &conn,
            &RunArchiveInput {
                run_id: "run-1".to_string(),
                ticker: "QQQ".to_string(),
                tickers_json: json!(["QQQ"]),
                prediction_date: "2026-01-01".to_string(),
                workflow_version: "v1".to_string(),
                prompt_versions_json: json!({}),
                git_sha: String::new(),
                config_hash: String::new(),
                market_regime_json: json!({"volatility":"normal"}),
                artifact_path: "outputs/run".to_string(),
                state_summary_json: json!({"degraded":false}),
                research_plan_json: json!({"rating":"long"}),
                degraded: false,
                phase_count: 8,
                total_elapsed_ms: 10,
            },
        )
        .unwrap();
        let row = run_archive_by_id(&conn, "run-1").unwrap();
        assert_eq!(row["ticker"], "QQQ");
        assert_eq!(row["research_plan_json"]["rating"], "long");
    }
}
