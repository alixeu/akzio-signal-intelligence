use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection};
use serde_json::{json, Value};

use crate::ensure_schema;

#[derive(Debug, Clone)]
pub struct PredictionInput {
    pub run_id: String,
    pub ticker: String,
    pub prediction_date: String,
    pub long_probability: f64,
    pub short_probability: f64,
    pub rating: String,
    pub window_days: i64,
    pub market_regime_json: Value,
    pub agent_probabilities_json: Value,
    pub weighted_base_probability: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct ExpiredPrediction {
    pub id: i64,
    pub run_id: String,
    pub ticker: String,
    pub prediction_date: String,
    pub long_probability: f64,
    pub short_probability: f64,
    pub window_days: i64,
    pub market_regime_json: Value,
}

pub fn upsert_prediction(conn: &Connection, input: &PredictionInput) -> Result<i64> {
    ensure_schema(conn)?;
    let now = Utc::now().to_rfc3339();
    conn.execute(
        r#"
        INSERT INTO predictions
            (run_id, ticker, prediction_date, long_probability, short_probability, rating,
             window_days, market_regime_json, agent_probabilities_json, weighted_base_probability, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, ticker) DO UPDATE SET
            prediction_date = excluded.prediction_date,
            long_probability = excluded.long_probability,
            short_probability = excluded.short_probability,
            rating = excluded.rating,
            window_days = excluded.window_days,
            market_regime_json = excluded.market_regime_json,
            agent_probabilities_json = excluded.agent_probabilities_json,
            weighted_base_probability = excluded.weighted_base_probability
        "#,
        params![
            input.run_id,
            input.ticker,
            input.prediction_date,
            input.long_probability,
            input.short_probability,
            input.rating,
            input.window_days,
            serde_json::to_string(&input.market_regime_json)?,
            serde_json::to_string(&input.agent_probabilities_json)?,
            input.weighted_base_probability,
            now,
        ],
    )?;
    prediction_id(conn, &input.run_id, &input.ticker)
}

pub fn prediction_id(conn: &Connection, run_id: &str, ticker: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT id FROM predictions WHERE run_id = ? AND ticker = ?",
        params![run_id, ticker],
        |row| row.get(0),
    )?)
}

pub fn prediction_by_run_ticker(conn: &Connection, run_id: &str, ticker: &str) -> Result<Value> {
    ensure_schema(conn)?;
    let text = conn.query_row(
        r#"
        SELECT json_object(
            'id', id,
            'run_id', run_id,
            'ticker', ticker,
            'prediction_date', prediction_date,
            'long_probability', long_probability,
            'short_probability', short_probability,
            'rating', rating,
            'window_days', window_days,
            'market_regime_json', json(market_regime_json),
            'agent_probabilities_json', json(agent_probabilities_json),
            'weighted_base_probability', weighted_base_probability,
            'created_at', created_at
        )
        FROM predictions
        WHERE run_id = ? AND ticker = ?
        "#,
        params![run_id, ticker],
        |row| row.get::<_, String>(0),
    )?;
    Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({})))
}

pub fn expired_unscored_predictions(
    conn: &Connection,
    as_of: &str,
    limit: usize,
) -> Result<Vec<ExpiredPrediction>> {
    ensure_schema(conn)?;
    let mut stmt = conn.prepare(
        r#"
        SELECT p.id, p.run_id, p.ticker, p.prediction_date, p.long_probability,
               p.short_probability, p.window_days, p.market_regime_json
        FROM predictions p
        LEFT JOIN outcomes o ON o.prediction_id = p.id
        WHERE o.id IS NULL
          AND date(p.prediction_date, '+' || p.window_days || ' days') <= date(?)
        ORDER BY p.prediction_date ASC, p.id ASC
        LIMIT ?
        "#,
    )?;
    let rows = stmt.query_map(params![as_of, limit.max(1) as i64], |row| {
        let market_regime_json: String = row.get(7)?;
        Ok(ExpiredPrediction {
            id: row.get(0)?,
            run_id: row.get(1)?,
            ticker: row.get(2)?,
            prediction_date: row.get(3)?,
            long_probability: row.get(4)?,
            short_probability: row.get(5)?,
            window_days: row.get(6)?,
            market_regime_json: serde_json::from_str(&market_regime_json).unwrap_or(Value::Null),
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect;

    #[test]
    fn upserts_and_finds_expired_unscored_predictions() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("pred.sqlite")).unwrap();
        let id = upsert_prediction(
            &conn,
            &PredictionInput {
                run_id: "run-1".to_string(),
                ticker: "QQQ".to_string(),
                prediction_date: "2026-01-01".to_string(),
                long_probability: 0.6,
                short_probability: 0.4,
                rating: "long".to_string(),
                window_days: 5,
                market_regime_json: json!({"volatility":"normal"}),
                agent_probabilities_json: json!({}),
                weighted_base_probability: Some(0.55),
            },
        )
        .unwrap();
        assert!(id > 0);

        let value = prediction_by_run_ticker(&conn, "run-1", "QQQ").unwrap();
        assert_eq!(value["long_probability"], 0.6);

        let expired = expired_unscored_predictions(&conn, "2026-01-07", 10).unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].ticker, "QQQ");
    }
}
