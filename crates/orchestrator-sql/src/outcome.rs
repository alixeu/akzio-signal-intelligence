use anyhow::Result;
use orchestrator_core::{close_on_or_after, close_on_or_before};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct OutcomeInput {
    pub prediction_id: i64,
    pub run_id: String,
    pub ticker: String,
    pub prediction_date: String,
    pub outcome_date: String,
    pub window_days: i64,
    pub baseline_close: f64,
    pub outcome_close: f64,
    pub actual_return: f64,
    pub direction_correct: bool,
    pub probability_error: f64,
}

pub fn upsert_outcome(conn: &Connection, input: &OutcomeInput) -> Result<i64> {
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        r#"
        INSERT INTO outcomes
            (prediction_id, run_id, ticker, prediction_date, outcome_date, window_days,
             baseline_close, outcome_close, actual_return, direction_correct, probability_error,
             scored_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(prediction_id) DO UPDATE SET
            outcome_date = excluded.outcome_date,
            window_days = excluded.window_days,
            baseline_close = excluded.baseline_close,
            outcome_close = excluded.outcome_close,
            actual_return = excluded.actual_return,
            direction_correct = excluded.direction_correct,
            probability_error = excluded.probability_error,
            scored_at = excluded.scored_at
        "#,
        params![
            input.prediction_id,
            input.run_id,
            input.ticker,
            input.prediction_date,
            input.outcome_date,
            input.window_days,
            input.baseline_close,
            input.outcome_close,
            input.actual_return,
            input.direction_correct as i64,
            input.probability_error,
            now,
        ],
    )?;
    let outcome_id = conn.query_row(
        "SELECT id FROM outcomes WHERE prediction_id = ?",
        params![input.prediction_id],
        |row| row.get::<_, i64>(0),
    )?;

    Ok(outcome_id)
}

pub fn track_record(conn: &Connection, ticker: Option<&str>) -> Result<Value> {
    let (sql, params): (&str, Vec<String>) = if let Some(ticker) = ticker.filter(|v| !v.is_empty())
    {
        (
            "SELECT COUNT(*), COALESCE(AVG(direction_correct), 0), COALESCE(AVG(probability_error * probability_error), 0), COALESCE(AVG(probability_error), 0) FROM outcomes WHERE ticker = ?",
            vec![ticker.to_string()],
        )
    } else {
        (
            "SELECT COUNT(*), COALESCE(AVG(direction_correct), 0), COALESCE(AVG(probability_error * probability_error), 0), COALESCE(AVG(probability_error), 0) FROM outcomes",
            vec![],
        )
    };
    let row = conn.query_row(sql, rusqlite::params_from_iter(params), |row| {
        Ok(json!({
            "total_predictions": row.get::<_, i64>(0)?,
            "direction_accuracy": row.get::<_, f64>(1)?,
            "mean_brier_score": row.get::<_, f64>(2)?,
            "mean_probability_error": row.get::<_, f64>(3)?,
        }))
    })?;
    Ok(row)
}

pub fn latest_close_on_or_before(
    conn: &Connection,
    ticker: &str,
    date: &str,
    interval: &str,
) -> Result<Option<(String, f64)>> {
    let rows = crate::technical_store::load_technical_series(conn, ticker, interval)?;
    Ok(close_on_or_before(&rows, date))
}

pub fn earliest_close_on_or_after(
    conn: &Connection,
    ticker: &str,
    date: &str,
    interval: &str,
) -> Result<Option<(String, f64)>> {
    let rows = crate::technical_store::load_technical_series(conn, ticker, interval)?;
    Ok(close_on_or_after(&rows, date))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connect,
        prediction::{upsert_prediction, PredictionInput},
    };

    #[test]
    fn upserts_outcome_and_track_record() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("outcome.sqlite")).unwrap();
        let prediction_id = upsert_prediction(
            &conn,
            &PredictionInput {
                run_id: "run-1".to_string(),
                ticker: "QQQ".to_string(),
                prediction_date: "2026-01-01".to_string(),
                long_probability: 0.7,
                short_probability: 0.3,
                rating: "long".to_string(),
                window_days: 5,
                market_regime_json: json!({}),
                agent_probabilities_json: json!({}),
                weighted_base_probability: None,
            },
        )
        .unwrap();
        upsert_outcome(
            &conn,
            &OutcomeInput {
                prediction_id,
                run_id: "run-1".to_string(),
                ticker: "QQQ".to_string(),
                prediction_date: "2026-01-01".to_string(),
                outcome_date: "2026-01-06".to_string(),
                window_days: 5,
                baseline_close: 100.0,
                outcome_close: 105.0,
                actual_return: 0.05,
                direction_correct: true,
                probability_error: 0.3,
            },
        )
        .unwrap();
        let record = track_record(&conn, Some("QQQ")).unwrap();
        assert_eq!(record["total_predictions"], 1);
        assert_eq!(record["direction_accuracy"], 1.0);
    }
}
