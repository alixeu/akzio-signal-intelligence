use anyhow::Result;
use orchestrator_sql::{
    candidate::{insert_candidate_experience, CandidateExperienceInput},
    ensure_schema,
};
use rusqlite::{params, Connection};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct DistillOptions {
    pub since: String,
    pub until: String,
    pub min_samples: usize,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct DistillSummary {
    pub groups: usize,
    pub generated: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone)]
struct OutcomeSample {
    run_id: String,
    ticker: String,
    prediction_date: String,
    direction_correct: bool,
    probability_error: f64,
    market_regime_json: Value,
}

pub fn distill_weekly(conn: &Connection, options: &DistillOptions) -> Result<DistillSummary> {
    ensure_schema(conn)?;
    let samples = outcome_samples(conn, &options.since, &options.until)?;
    let mut groups = grouped_samples(samples);
    let mut summary = DistillSummary {
        groups: groups.len(),
        generated: 0,
        skipped: 0,
    };

    for group in groups.iter_mut() {
        if group.samples.len() < options.min_samples.max(1) {
            summary.skipped += 1;
            continue;
        }
        let metrics = group_metrics(group);
        let direction_accuracy = metrics["direction_accuracy"].as_f64().unwrap_or_default();
        let experience_type = if direction_accuracy >= 0.65 {
            "calibration_strength"
        } else if direction_accuracy <= 0.45 {
            "calibration_correction"
        } else {
            summary.skipped += 1;
            continue;
        };
        let finding = if experience_type == "calibration_strength" {
            format!(
                "{} predictions were directionally reliable in this regime",
                group.ticker
            )
        } else {
            format!(
                "{} predictions were directionally unreliable in this regime",
                group.ticker
            )
        };
        let recommendation = if experience_type == "calibration_strength" {
            "Keep this regime pattern as a positive calibration prior when current evidence agrees."
        } else {
            "Discount similar future probabilities unless current evidence clearly improves the setup."
        };
        let run_ids = sample_run_ids(group);
        let evidence = evidence_rows(group, true);
        let counter_evidence = evidence_rows(group, false);
        insert_candidate_experience(
            conn,
            &CandidateExperienceInput {
                scope: "ticker".to_string(),
                scope_value: group.ticker.clone(),
                experience_type: experience_type.to_string(),
                market_regime_json: group.market_regime_json.clone(),
                finding,
                recommendation: recommendation.to_string(),
                evidence_json: evidence,
                counter_evidence_json: counter_evidence,
                metrics_json: metrics.clone(),
                sample_count: group.samples.len() as i64,
                sample_run_ids_json: json!(run_ids),
                confidence: confidence_from_accuracy(direction_accuracy, group.samples.len()),
                effect_size: (direction_accuracy - 0.5).abs(),
                distiller_version: "v1".to_string(),
                reflection_version: "v1".to_string(),
                source_window: format!("{}..{}", options.since, options.until),
            },
        )?;
        summary.generated += 1;
    }

    Ok(summary)
}

fn outcome_samples(conn: &Connection, since: &str, until: &str) -> Result<Vec<OutcomeSample>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT o.run_id, o.ticker, o.prediction_date, o.direction_correct, o.probability_error, p.market_regime_json
        FROM outcomes o
        JOIN predictions p ON p.id = o.prediction_id
        WHERE date(o.prediction_date) >= date(?) AND date(o.prediction_date) <= date(?)
        ORDER BY o.ticker ASC, p.market_regime_json ASC, o.prediction_date ASC, o.run_id ASC
        "#,
    )?;
    let rows = stmt.query_map(params![since, until], |row| {
        let market_regime: String = row.get(5)?;
        Ok(OutcomeSample {
            run_id: row.get(0)?,
            ticker: row.get(1)?,
            prediction_date: row.get(2)?,
            direction_correct: row.get::<_, i64>(3)? != 0,
            probability_error: row.get(4)?,
            market_regime_json: serde_json::from_str(&market_regime).unwrap_or(Value::Null),
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

#[derive(Debug, Clone)]
struct SampleGroup {
    ticker: String,
    market_regime_json: Value,
    samples: Vec<OutcomeSample>,
}

fn grouped_samples(samples: Vec<OutcomeSample>) -> Vec<SampleGroup> {
    let mut groups: Vec<SampleGroup> = Vec::new();
    for sample in samples {
        if let Some(group) = groups.iter_mut().find(|group| {
            group.ticker == sample.ticker && group.market_regime_json == sample.market_regime_json
        }) {
            group.samples.push(sample);
        } else {
            groups.push(SampleGroup {
                ticker: sample.ticker.clone(),
                market_regime_json: sample.market_regime_json.clone(),
                samples: vec![sample],
            });
        }
    }
    groups
}

fn group_metrics(group: &SampleGroup) -> Value {
    let sample_count = group.samples.len() as f64;
    let correct = group
        .samples
        .iter()
        .filter(|sample| sample.direction_correct)
        .count() as f64;
    let mean_probability_error = group
        .samples
        .iter()
        .map(|sample| sample.probability_error)
        .sum::<f64>()
        / sample_count;
    let mean_brier_score = group
        .samples
        .iter()
        .map(|sample| sample.probability_error * sample.probability_error)
        .sum::<f64>()
        / sample_count;
    json!({
        "sample_count": group.samples.len(),
        "direction_accuracy": correct / sample_count,
        "mean_probability_error": mean_probability_error,
        "mean_brier_score": mean_brier_score,
    })
}

fn confidence_from_accuracy(direction_accuracy: f64, sample_count: usize) -> f64 {
    let sample_factor = (sample_count as f64 / 10.0).min(1.0);
    (0.5 + (direction_accuracy - 0.5).abs() * sample_factor).clamp(0.0, 1.0)
}

fn sample_run_ids(group: &SampleGroup) -> Vec<String> {
    group
        .samples
        .iter()
        .map(|sample| sample.run_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn evidence_rows(group: &SampleGroup, correct: bool) -> Value {
    Value::Array(
        group
            .samples
            .iter()
            .filter(|sample| sample.direction_correct == correct)
            .take(5)
            .map(|sample| {
                json!({
                    "run_id": sample.run_id,
                    "prediction_date": sample.prediction_date,
                    "direction_correct": sample.direction_correct,
                    "probability_error": sample.probability_error,
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_sql::{
        connect,
        outcome::{upsert_outcome, OutcomeInput},
        prediction::{upsert_prediction, PredictionInput},
    };

    #[test]
    fn skips_groups_below_min_samples() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("distill-skip.sqlite")).unwrap();
        insert_outcome(&conn, "run-1", "QQQ", true, 0.1);

        let summary = distill_weekly(&conn, &options(2)).unwrap();
        assert_eq!(summary.generated, 0);
        assert_eq!(summary.skipped, 1);
    }

    #[test]
    fn generates_correction_candidate_for_low_accuracy() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("distill-low.sqlite")).unwrap();
        insert_outcome(&conn, "run-1", "QQQ", false, 0.8);
        insert_outcome(&conn, "run-2", "QQQ", false, -0.7);
        insert_outcome(&conn, "run-3", "QQQ", true, 0.2);

        let summary = distill_weekly(&conn, &options(3)).unwrap();
        assert_eq!(summary.generated, 1);
        assert_candidate(&conn, "calibration_correction", 3);
    }

    #[test]
    fn generates_strength_candidate_for_high_accuracy() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("distill-high.sqlite")).unwrap();
        insert_outcome(&conn, "run-1", "QQQ", true, 0.1);
        insert_outcome(&conn, "run-2", "QQQ", true, -0.1);
        insert_outcome(&conn, "run-3", "QQQ", false, 0.4);

        let summary = distill_weekly(&conn, &options(3)).unwrap();
        assert_eq!(summary.generated, 1);
        assert_candidate(&conn, "calibration_strength", 3);
    }

    fn options(min_samples: usize) -> DistillOptions {
        DistillOptions {
            since: "2026-01-01".to_string(),
            until: "2026-01-31".to_string(),
            min_samples,
        }
    }

    fn insert_outcome(
        conn: &Connection,
        run_id: &str,
        ticker: &str,
        direction_correct: bool,
        probability_error: f64,
    ) {
        let prediction_id = upsert_prediction(
            conn,
            &PredictionInput {
                run_id: run_id.to_string(),
                ticker: ticker.to_string(),
                prediction_date: "2026-01-05".to_string(),
                long_probability: 0.6,
                short_probability: 0.4,
                rating: "test".to_string(),
                window_days: 5,
                market_regime_json: json!({"volatility":"normal"}),
                agent_probabilities_json: json!({}),
                weighted_base_probability: None,
            },
        )
        .unwrap();
        upsert_outcome(
            conn,
            &OutcomeInput {
                prediction_id,
                run_id: run_id.to_string(),
                ticker: ticker.to_string(),
                prediction_date: "2026-01-05".to_string(),
                outcome_date: "2026-01-10".to_string(),
                window_days: 5,
                baseline_close: 100.0,
                outcome_close: 105.0,
                actual_return: 0.05,
                direction_correct,
                probability_error,
            },
        )
        .unwrap();
    }

    fn assert_candidate(conn: &Connection, experience_type: &str, sample_count: i64) {
        let row: (String, i64) = conn
            .query_row(
                "SELECT experience_type, sample_count FROM candidate_experiences",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(row.0, experience_type);
        assert_eq!(row.1, sample_count);
    }
}
