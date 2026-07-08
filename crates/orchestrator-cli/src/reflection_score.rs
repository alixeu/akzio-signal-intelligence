use anyhow::{Context, Result};
use chrono::{Duration, NaiveDate};
use orchestrator_sql::{
    outcome::{
        earliest_close_on_or_after, latest_close_on_or_before, upsert_outcome, OutcomeInput,
    },
    prediction::expired_unscored_predictions,
};
use rusqlite::Connection;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct ScoreOptions {
    pub as_of: String,
    pub limit: usize,
    pub interval: String,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ScoreSummary {
    pub scored: usize,
    pub skipped: usize,
    pub errors: usize,
}

pub fn score_predictions(conn: &Connection, options: &ScoreOptions) -> Result<ScoreSummary> {
    let predictions = expired_unscored_predictions(conn, &options.as_of, options.limit)?;
    let mut summary = ScoreSummary::default();

    for prediction in predictions {
        let target_date = match add_days(&prediction.prediction_date, prediction.window_days) {
            Ok(date) => date,
            Err(_) => {
                summary.errors += 1;
                continue;
            }
        };
        let Some((_, baseline_close)) = latest_close_on_or_before(
            conn,
            &prediction.ticker,
            &prediction.prediction_date,
            &options.interval,
        )?
        else {
            summary.skipped += 1;
            continue;
        };
        let Some((outcome_date, outcome_close)) =
            earliest_close_on_or_after(conn, &prediction.ticker, &target_date, &options.interval)?
        else {
            summary.skipped += 1;
            continue;
        };

        let actual_return = (outcome_close - baseline_close) / baseline_close;
        let predicted_long = prediction.long_probability >= prediction.short_probability;
        let actual_long = actual_return >= 0.0;
        let probability_error = prediction.long_probability - if actual_long { 1.0 } else { 0.0 };

        upsert_outcome(
            conn,
            &OutcomeInput {
                prediction_id: prediction.id,
                run_id: prediction.run_id,
                ticker: prediction.ticker,
                prediction_date: prediction.prediction_date,
                outcome_date,
                window_days: prediction.window_days,
                baseline_close,
                outcome_close,
                actual_return,
                direction_correct: predicted_long == actual_long,
                probability_error,
                market_regime_json: prediction.market_regime_json,
            },
        )?;
        summary.scored += 1;
    }

    Ok(summary)
}

fn add_days(date: &str, days: i64) -> Result<String> {
    let date = date.get(..10).unwrap_or(date);
    let parsed = NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .with_context(|| format!("invalid prediction date {date}"))?;
    Ok((parsed + Duration::days(days)).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_sql::{
        connect,
        prediction::{upsert_prediction, PredictionInput},
    };
    use rusqlite::params;
    use serde_json::json;

    #[test]
    fn scores_upward_and_downward_predictions() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("score.sqlite")).unwrap();
        insert_prediction(&conn, "run-up", "QQQ", 0.7, 0.3);
        insert_prediction(&conn, "run-down", "SOXX", 0.2, 0.8);
        insert_close(&conn, "QQQ", "2026-01-01", 100.0);
        insert_close(&conn, "QQQ", "2026-01-06", 105.0);
        insert_close(&conn, "SOXX", "2026-01-01", 100.0);
        insert_close(&conn, "SOXX", "2026-01-06", 95.0);

        let summary = score_predictions(&conn, &options()).unwrap();
        assert_eq!(summary.scored, 2);
        assert_eq!(summary.skipped, 0);
        assert_eq!(summary.errors, 0);

        let accuracy: f64 = conn
            .query_row("SELECT AVG(direction_correct) FROM outcomes", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(accuracy, 1.0);
    }

    #[test]
    fn skips_predictions_without_required_close_rows() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("missing.sqlite")).unwrap();
        insert_prediction(&conn, "run-missing", "QQQ", 0.7, 0.3);
        insert_close(&conn, "QQQ", "2026-01-01", 100.0);

        let summary = score_predictions(&conn, &options()).unwrap();
        assert_eq!(summary.scored, 0);
        assert_eq!(summary.skipped, 1);
        assert_eq!(outcome_count(&conn), 0);
    }

    #[test]
    fn repeated_scoring_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("idem.sqlite")).unwrap();
        insert_prediction(&conn, "run-idem", "QQQ", 0.7, 0.3);
        insert_close(&conn, "QQQ", "2026-01-01", 100.0);
        insert_close(&conn, "QQQ", "2026-01-06", 105.0);

        assert_eq!(score_predictions(&conn, &options()).unwrap().scored, 1);
        assert_eq!(score_predictions(&conn, &options()).unwrap().scored, 0);
        assert_eq!(outcome_count(&conn), 1);
    }

    fn options() -> ScoreOptions {
        ScoreOptions {
            as_of: "2026-01-07".to_string(),
            limit: 100,
            interval: "1d".to_string(),
        }
    }

    fn insert_prediction(
        conn: &Connection,
        run_id: &str,
        ticker: &str,
        long_probability: f64,
        short_probability: f64,
    ) {
        upsert_prediction(
            conn,
            &PredictionInput {
                run_id: run_id.to_string(),
                ticker: ticker.to_string(),
                prediction_date: "2026-01-01".to_string(),
                long_probability,
                short_probability,
                rating: "test".to_string(),
                window_days: 5,
                market_regime_json: json!({}),
                agent_probabilities_json: json!({}),
                weighted_base_probability: None,
            },
        )
        .unwrap();
    }

    fn insert_close(conn: &Connection, ticker: &str, date: &str, value: f64) {
        conn.execute(
            r#"
            INSERT INTO technical_indicators
                (ticker, kline_time, indicator_name, indicator_value, model, interval, imported_at)
            VALUES (?, ?, 'Close', ?, 'test', '1d', '2026-01-01T00:00:00Z')
            "#,
            params![ticker, date, value],
        )
        .unwrap();
    }

    fn outcome_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM outcomes", [], |row| row.get(0))
            .unwrap()
    }
}
