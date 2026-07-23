pub mod cli_config;
pub mod eval;
pub mod memory_promote;
pub mod reflection_score;
pub mod sql_cli;
pub mod weekly_distill;

pub use orchestrator_ingest::{jin10, technical};
pub use orchestrator_workflow::exec;
pub use orchestrator_workflow::report::report;
use serde_json::{json, Value};

pub async fn run_exec_with_learning(args: exec::ExecArgs) -> anyhow::Result<Value> {
    let should_learn = !args.mock && args.to_phase >= 8;
    let mut result = exec::run(args).await?;
    let learning = if !should_learn {
        json!({"status": "skipped", "reason": "mock_or_phase8_not_selected"})
    } else if result
        .pointer("/run_state/prior_memory/enabled")
        .and_then(Value::as_bool)
        != Some(true)
    {
        json!({"status": "disabled"})
    } else {
        match run_reflection_cycle(&result) {
            Ok(summary) => summary,
            Err(error) => {
                tracing::warn!(error = %error, "post-run reflection learning failed");
                json!({"status": "non_blocking_failed", "message": error.to_string()})
            }
        }
    };
    result["reflection_learning"] = learning.clone();
    result["run_state"]["reflection_learning"] = learning;
    persist_updated_state(&result);
    Ok(result)
}

fn run_reflection_cycle(result: &Value) -> anyhow::Result<Value> {
    use anyhow::Context;
    use chrono::{Duration, NaiveDate};

    let db_path = result
        .get("db_path")
        .and_then(Value::as_str)
        .context("reflection learning requires db_path")?;
    let as_of = result
        .get("date")
        .and_then(Value::as_str)
        .context("reflection learning requires date")?;
    let until = NaiveDate::parse_from_str(as_of, "%Y-%m-%d")
        .with_context(|| format!("invalid reflection date {as_of:?}"))?;
    let since = (until - Duration::days(30)).to_string();
    let conn = orchestrator_sql::connect(db_path)?;

    let scored = reflection_score::score_predictions(
        &conn,
        &reflection_score::ScoreOptions {
            as_of: as_of.to_string(),
            limit: 500,
            interval: "1d".to_string(),
        },
    )?;
    let distilled = weekly_distill::distill_weekly(
        &conn,
        &weekly_distill::DistillOptions {
            since: since.clone(),
            until: as_of.to_string(),
            min_samples: 5,
        },
    )?;
    let promote_mode = result
        .pointer("/run_state/prior_memory/promote_mode")
        .and_then(Value::as_str)
        .unwrap_or("auto");
    let promoted = memory_promote::promote_memories(
        &conn,
        &memory_promote::PromoteOptions {
            mode: memory_promote::PromoteMode::parse(promote_mode),
            min_quality: 0.0,
            min_samples: 3,
            min_confidence: 0.0,
        },
    )?;

    Ok(json!({
        "status": "completed",
        "source_window": format!("{since}..{as_of}"),
        "outcome_scoring": scored,
        "experience_distillation": distilled,
        "memory_promotion": promoted,
        "available_next_run": promoted.promoted > 0
    }))
}

fn persist_updated_state(result: &Value) {
    let Some(path) = result.get("state").and_then(Value::as_str) else {
        return;
    };
    let Some(state) = result.get("run_state") else {
        return;
    };
    let serialized = match serde_json::to_string_pretty(state) {
        Ok(serialized) => serialized,
        Err(error) => {
            tracing::warn!(path, error = %error, "failed to serialize reflection learning status");
            return;
        }
    };
    if let Err(error) = std::fs::write(path, serialized) {
        tracing::warn!(path, error = %error, "failed to persist reflection learning status");
    }
}

pub fn init_tracing() {
    init_tracing_with_debug(false);
}

pub fn init_tracing_with_debug(debug: bool) {
    let default_filter = if debug {
        "orchestrator_cli=debug,orchestrator_workflow=debug,orchestrator_llm=debug,orchestrator_sql=debug,info"
    } else {
        "info"
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_sql::{
        outcome::{upsert_outcome, OutcomeInput},
        prediction::{upsert_prediction, PredictionInput},
    };

    #[test]
    fn reflection_cycle_promotes_once_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("learning.sqlite");
        let conn = orchestrator_sql::connect(&db_path).unwrap();
        for day in 1..=10 {
            let run_id = format!("run-{day}");
            let prediction_date = format!("2026-01-{day:02}");
            let prediction_id = upsert_prediction(
                &conn,
                &PredictionInput {
                    run_id: run_id.clone(),
                    ticker: "QQQ".to_string(),
                    prediction_date: prediction_date.clone(),
                    long_probability: 0.8,
                    short_probability: 0.2,
                    rating: "Buy".to_string(),
                    window_days: 5,
                    market_regime_json: json!({"volatility":"normal"}),
                    agent_probabilities_json: json!({}),
                    weighted_base_probability: Some(0.8),
                },
            )
            .unwrap();
            upsert_outcome(
                &conn,
                &OutcomeInput {
                    prediction_id,
                    run_id,
                    ticker: "QQQ".to_string(),
                    prediction_date,
                    outcome_date: "2026-01-20".to_string(),
                    window_days: 5,
                    baseline_close: 100.0,
                    outcome_close: 105.0,
                    actual_return: 0.05,
                    direction_correct: true,
                    probability_error: -0.2,
                },
            )
            .unwrap();
        }
        let result = json!({
            "db_path": db_path,
            "date": "2026-01-31",
            "run_state": {"prior_memory": {"enabled": true, "promote_mode": "auto"}}
        });

        let first = run_reflection_cycle(&result).unwrap();
        assert_eq!(first["experience_distillation"]["generated"], 1);
        assert_eq!(first["memory_promotion"]["promoted"], 1);

        let second = run_reflection_cycle(&result).unwrap();
        assert_eq!(second["experience_distillation"]["generated"], 0);
        assert_eq!(second["memory_promotion"]["promoted"], 0);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM memory_items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
