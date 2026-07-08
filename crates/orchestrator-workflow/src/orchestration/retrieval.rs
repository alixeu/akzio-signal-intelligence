use anyhow::Result;
use orchestrator_core::MarketRegime;
use orchestrator_sql::{
    memory::{read_prior_memory, PriorMemoryQuery},
    outcome::track_record,
};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use crate::orchestration::config::RuntimeConfig;

pub(crate) fn inject_phase0_reflection(
    conn: &Connection,
    state: &mut Value,
    config: &RuntimeConfig,
) -> Result<()> {
    if !config.reflection.enabled {
        return Ok(());
    }

    let tickers = tickers_from_state(state);
    let market_regime = market_regime_from_state(state);
    let mut items_by_ticker = serde_json::Map::new();
    for ticker in &tickers {
        let result = read_prior_memory(
            conn,
            &PriorMemoryQuery {
                ticker: Some(ticker.clone()),
                market_regime: market_regime.clone(),
                budget: config.reflection.retrieval,
                include_body: false,
            },
        )?;
        items_by_ticker.insert(ticker.clone(), result);
    }

    state["prior_memory"] = json!({
        "enabled": true,
        "reflection_version": config.reflection.reflection_version,
        "budget": {
            "token_budget": config.reflection.retrieval.token_budget,
            "max_items": config.reflection.retrieval.max_items,
            "min_quality": config.reflection.retrieval.min_quality,
        },
        "market_regime": market_regime,
        "items_by_ticker": items_by_ticker,
    });
    state["track_record"] = json!({
        "aggregate": track_record(conn, None).unwrap_or_else(|_| empty_track_record()),
        "by_ticker": track_record_by_ticker(conn, &tickers),
    });
    state["agent_accuracy"] = agent_accuracy(conn).unwrap_or_else(|_| json!({}));
    Ok(())
}

fn tickers_from_state(state: &Value) -> Vec<String> {
    state
        .get("tickers")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .filter(|item| !item.trim().is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .or_else(|| {
            state
                .get("ticker")
                .and_then(Value::as_str)
                .map(|ticker| vec![ticker.to_string()])
        })
        .unwrap_or_default()
}

fn market_regime_from_state(state: &Value) -> MarketRegime {
    let volatility = state
        .get("allocation_context")
        .and_then(|value| value.get("vix"))
        .and_then(|value| value.get("regime"))
        .and_then(Value::as_str)
        .or_else(|| {
            state
                .get("portfolio_allocation")
                .and_then(|value| value.get("vix_regime"))
                .and_then(Value::as_str)
        })
        .unwrap_or_default()
        .to_string();
    MarketRegime {
        volatility,
        ..Default::default()
    }
}

fn track_record_by_ticker(conn: &Connection, tickers: &[String]) -> Value {
    Value::Object(
        tickers
            .iter()
            .map(|ticker| {
                let value =
                    track_record(conn, Some(ticker)).unwrap_or_else(|_| empty_track_record());
                (ticker.clone(), value)
            })
            .collect(),
    )
}

fn empty_track_record() -> Value {
    json!({
        "total_predictions": 0,
        "direction_accuracy": 0.0,
        "mean_brier_score": 0.0,
        "mean_probability_error": 0.0,
    })
}

fn agent_accuracy(conn: &Connection) -> Result<Value> {
    orchestrator_sql::ensure_schema(conn)?;
    let mut stmt = conn.prepare(
        r#"
        SELECT p.agent_probabilities_json, o.direction_correct, o.probability_error
        FROM outcomes o
        JOIN predictions p ON p.id = o.prediction_id
        ORDER BY o.scored_at DESC
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)? != 0,
            row.get::<_, f64>(2)?,
        ))
    })?;

    let mut stats: BTreeMap<String, RoleAccuracy> = BTreeMap::new();
    for row in rows {
        let (raw, direction_correct, probability_error) = row?;
        for role in roles_from_agent_probabilities(&raw) {
            stats
                .entry(role)
                .or_default()
                .record(direction_correct, probability_error);
        }
    }

    Ok(Value::Object(
        stats
            .into_iter()
            .map(|(role, stat)| (role, stat.value()))
            .collect(),
    ))
}

fn roles_from_agent_probabilities(raw: &str) -> Vec<String> {
    match serde_json::from_str::<Value>(raw).unwrap_or(Value::Null) {
        Value::Object(map) => map.keys().cloned().collect(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                item.get("role")
                    .or_else(|| item.get("agent"))
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect(),
        _ => Vec::new(),
    }
}

#[derive(Debug, Default)]
struct RoleAccuracy {
    total: usize,
    correct: usize,
    probability_error_sum: f64,
    brier_sum: f64,
}

impl RoleAccuracy {
    fn record(&mut self, direction_correct: bool, probability_error: f64) {
        self.total += 1;
        if direction_correct {
            self.correct += 1;
        }
        self.probability_error_sum += probability_error;
        self.brier_sum += probability_error * probability_error;
    }

    fn value(self) -> Value {
        if self.total == 0 {
            return json!({
                "total_predictions": 0,
                "direction_accuracy": 0.0,
                "mean_probability_error": 0.0,
                "mean_brier_score": 0.0,
            });
        }
        let total = self.total as f64;
        json!({
            "total_predictions": self.total,
            "direction_accuracy": self.correct as f64 / total,
            "mean_probability_error": self.probability_error_sum / total,
            "mean_brier_score": self.brier_sum / total,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::config::RuntimeConfig;
    use orchestrator_core::RetrievalBudget;
    use orchestrator_sql::{
        candidate::{insert_candidate_experience, pending_candidates, CandidateExperienceInput},
        connect,
        memory::{promote_candidate_to_memory, PromoteMemoryInput},
        outcome::{upsert_outcome, OutcomeInput},
        prediction::{upsert_prediction, PredictionInput},
    };
    use serde_json::json;

    #[test]
    fn injects_empty_structures_when_enabled_without_data() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("phase0-empty.sqlite")).unwrap();
        let mut state = json!({"ticker":"QQQ", "tickers":["QQQ"]});
        inject_phase0_reflection(
            &conn,
            &mut state,
            &test_runtime_config(true, RetrievalBudget::default()),
        )
        .unwrap();

        assert!(state.get("prior_memory").is_some());
        assert!(state.get("track_record").is_some());
        assert!(state.get("agent_accuracy").is_some());
        assert_eq!(
            state["prior_memory"]["items_by_ticker"]["QQQ"]["items"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn disabled_config_does_not_inject_reflection_state() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("phase0-disabled.sqlite")).unwrap();
        let mut state = json!({"ticker":"QQQ", "tickers":["QQQ"]});
        inject_phase0_reflection(
            &conn,
            &mut state,
            &test_runtime_config(false, RetrievalBudget::default()),
        )
        .unwrap();

        assert!(state.get("prior_memory").is_none());
        assert!(state.get("track_record").is_none());
        assert!(state.get("agent_accuracy").is_none());
    }

    #[test]
    fn retrieval_budget_caps_injected_prior_memory() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("phase0-budget.sqlite")).unwrap();
        seed_memory(&conn, "run-1", 0.9);
        seed_memory(&conn, "run-2", 0.8);
        let mut state = json!({"ticker":"QQQ", "tickers":["QQQ"]});
        inject_phase0_reflection(
            &conn,
            &mut state,
            &test_runtime_config(
                true,
                RetrievalBudget {
                    token_budget: 4000,
                    max_items: 1,
                    min_quality: 0.0,
                },
            ),
        )
        .unwrap();

        let items = state["prior_memory"]["items_by_ticker"]["QQQ"]["items"]
            .as_array()
            .unwrap();
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn injects_track_record_and_agent_accuracy() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("phase0-accuracy.sqlite")).unwrap();
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
                agent_probabilities_json: json!({"analyst.technical":{"long_probability":0.7}}),
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
                outcome_close: 110.0,
                actual_return: 0.1,
                direction_correct: true,
                probability_error: -0.3,
                market_regime_json: json!({}),
            },
        )
        .unwrap();
        let mut state = json!({"ticker":"QQQ", "tickers":["QQQ"]});
        inject_phase0_reflection(
            &conn,
            &mut state,
            &test_runtime_config(true, RetrievalBudget::default()),
        )
        .unwrap();

        assert_eq!(
            state["track_record"]["by_ticker"]["QQQ"]["total_predictions"],
            1
        );
        assert_eq!(
            state["agent_accuracy"]["analyst.technical"]["total_predictions"],
            1
        );
    }

    fn seed_memory(conn: &Connection, run_id: &str, quality_score: f64) {
        insert_candidate_experience(
            conn,
            &CandidateExperienceInput {
                scope: "ticker".to_string(),
                scope_value: "QQQ".to_string(),
                experience_type: "calibration".to_string(),
                market_regime_json: json!({}),
                finding: format!("pattern {run_id}"),
                recommendation: "adjust".to_string(),
                evidence_json: json!([]),
                counter_evidence_json: json!([]),
                metrics_json: json!({}),
                sample_count: 8,
                sample_run_ids_json: json!([run_id]),
                confidence: 0.8,
                effect_size: 0.2,
                distiller_version: "v1".to_string(),
                reflection_version: "v1".to_string(),
                source_window: run_id.to_string(),
            },
        )
        .unwrap();
        let candidate = pending_candidates(conn).unwrap().pop().unwrap();
        promote_candidate_to_memory(
            conn,
            &PromoteMemoryInput {
                candidate,
                quality_score,
                recent_success_rate: 0.8,
            },
        )
        .unwrap();
    }

    fn test_runtime_config(enabled: bool, retrieval: RetrievalBudget) -> RuntimeConfig {
        let roles = crate::orchestration::config::required_llm_roles()
            .iter()
            .map(|role| ((*role).to_string(), json!({})))
            .collect::<serde_json::Map<_, _>>();
        let mut config = RuntimeConfig::from_value(&json!({
            "orchestrator": {
                "llm": {
                    "defaults": {
                        "route": "responses",
                        "model": "gpt-5.4",
                        "base_url": "https://llm.example.com/v1",
                        "api_key": "test-key",
                        "max_turns": 3,
                        "reasoning_effort": "medium",
                        "native_web_search": true,
                        "think_tool": false,
                        "tools": []
                    },
                    "roles": roles
                }
            }
        }))
        .unwrap();
        config.reflection.enabled = enabled;
        config.reflection.retrieval = retrieval;
        config
    }
}
