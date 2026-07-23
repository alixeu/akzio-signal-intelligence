use anyhow::Result;
use orchestrator_sql::{score_mature_predictions, ReflectionScoreSummary, ReflectionThresholds};
use rusqlite::Connection;

#[derive(Debug, Clone)]
pub struct ScoreOptions {
    pub as_of: String,
    pub limit: usize,
    pub interval: String,
}

pub type ScoreSummary = ReflectionScoreSummary;

pub fn score_predictions(conn: &Connection, options: &ScoreOptions) -> Result<ScoreSummary> {
    score_mature_predictions(
        conn,
        &options.as_of,
        &options.interval,
        options.limit,
        ReflectionThresholds::default(),
        None,
        "v1",
        10,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{technical_csv_path, write_technical_csv, TechnicalCsvRow};
    use orchestrator_sql::{
        connect, import_technical_csv,
        prediction::{upsert_prediction, PredictionInput},
    };
    use serde_json::json;
    use std::collections::HashMap;
    #[test]
    fn scores_upward_and_downward_predictions() {
        let temp = tempfile::tempdir().unwrap();
        let mut conn = connect(temp.path().join("score.sqlite")).unwrap();
        insert_prediction(&conn, "run-up", "QQQ", 0.7, 0.3);
        insert_prediction(&conn, "run-down", "SOXX", 0.2, 0.8);
        insert_close(
            &mut conn,
            temp.path(),
            "QQQ",
            &[
                ("2026-01-01", 100.0),
                ("2026-01-02", 101.0),
                ("2026-01-05", 102.0),
                ("2026-01-06", 105.0),
            ],
        );
        insert_close(
            &mut conn,
            temp.path(),
            "SOXX",
            &[
                ("2026-01-01", 100.0),
                ("2026-01-02", 99.0),
                ("2026-01-05", 97.0),
                ("2026-01-06", 95.0),
            ],
        );

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
        let mut conn = connect(temp.path().join("missing.sqlite")).unwrap();
        insert_prediction(&conn, "run-missing", "QQQ", 0.7, 0.3);
        insert_close(&mut conn, temp.path(), "QQQ", &[("2026-01-01", 100.0)]);

        let summary = score_predictions(&conn, &options()).unwrap();
        assert_eq!(summary.scored, 0);
        assert_eq!(summary.skipped, 1);
        assert_eq!(outcome_count(&conn), 0);
    }

    #[test]
    fn repeated_scoring_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let mut conn = connect(temp.path().join("idem.sqlite")).unwrap();
        insert_prediction(&conn, "run-idem", "QQQ", 0.7, 0.3);
        insert_close(
            &mut conn,
            temp.path(),
            "QQQ",
            &[
                ("2026-01-01", 100.0),
                ("2026-01-02", 101.0),
                ("2026-01-05", 103.0),
                ("2026-01-06", 105.0),
            ],
        );

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
                window_days: 3,
                market_regime_json: json!({}),
                agent_probabilities_json: json!({}),
                weighted_base_probability: None,
            },
        )
        .unwrap();
    }

    fn insert_close(
        conn: &mut Connection,
        dir: &std::path::Path,
        ticker: &str,
        entries: &[(&str, f64)],
    ) {
        let rows: Vec<TechnicalCsvRow> = entries
            .iter()
            .map(|(date, close)| TechnicalCsvRow {
                date: date.to_string(),
                values: HashMap::from([("Close".to_string(), *close)]),
            })
            .collect();
        let path = technical_csv_path(dir, ticker, "1d").unwrap();
        write_technical_csv(&path, &rows).unwrap();
        import_technical_csv(conn, ticker, "1d", &path).unwrap();
    }

    fn outcome_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM outcomes", [], |row| row.get(0))
            .unwrap()
    }
}
