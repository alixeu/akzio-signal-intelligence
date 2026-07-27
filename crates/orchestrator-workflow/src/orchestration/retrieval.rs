use anyhow::Result;
use orchestrator_core::MarketRegime;
use orchestrator_store::{
    experience_level, read_indexes, FileStore, FileStoreOptions, IndexKind, IndexQuery, IndexScope,
};
use serde_json::{json, Value};

use crate::orchestration::config::{AllocationConfig, RuntimeConfig};

pub(crate) fn inject_phase_summary_reflection(
    state: &mut Value,
    config: &RuntimeConfig,
) -> Result<()> {
    if !config.reflection.enabled {
        return Ok(());
    }

    let tickers = tickers_from_state(state);
    let market_regime = market_regime_from_state(state);
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("FileStore experience retrieval requires store_root"))?;
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let mut items_by_ticker = serde_json::Map::new();
    for ticker in &tickers {
        let page = read_indexes(
            &store,
            None,
            &IndexQuery {
                kind: Some(IndexKind::Experience),
                ticker: Some(ticker.clone()),
                limit: 100,
                ..Default::default()
            },
        )?;
        let mut remaining_chars = config.reflection.retrieval.token_budget.saturating_mul(4);
        let mut items = Vec::new();
        for index in page.indexes {
            if index.confidence < config.reflection.retrieval.min_quality
                || items.len() >= config.reflection.retrieval.max_items
                || index.summary.len() > remaining_chars
            {
                continue;
            }
            let scope = IndexScope {
                kind: index.kind,
                location: None,
                index_id: index.index_id.clone(),
                run_id: index.run_id.clone(),
                source_run_id: index.source_run_id.clone(),
                source_phase: index.source_phase,
                role: index.role.clone(),
                ticker: index.ticker.clone(),
                topic_id: index.topic_id.clone(),
                source_payload_hash: index.source_payload_hash.clone(),
                authoritative_fields: index.authoritative_fields.clone(),
                created_at: index.created_at.clone(),
            };
            let level = experience_level(&store, &scope)?;
            remaining_chars = remaining_chars.saturating_sub(index.summary.len());
            items.push(json!({
                "index_id": index.index_id,
                "pattern_key": index.pattern_key,
                "summary": index.summary,
                "confidence": index.confidence,
                "source_phase": index.source_phase,
                "applies_to_phases": index.applies_to_phases,
                "experience_level": level,
                "detail_count": index.detail_count,
            }));
        }
        items_by_ticker.insert(ticker.clone(), json!({"items": items}));
    }

    state["prior_experience"] = json!({
        "enabled": true,
        "budget": {
            "token_budget": config.reflection.retrieval.token_budget,
            "max_items": config.reflection.retrieval.max_items,
            "min_quality": config.reflection.retrieval.min_quality,
        },
        "market_regime": market_regime,
        "indexes_by_ticker": items_by_ticker,
    });
    // Decision/outcome records are per-run immutable files.  Aggregate score
    // cataloguing is intentionally not a second mutable authority; callers
    // can build it through Store Doctor when they need a report.
    state["track_record"] = json!({
        "aggregate": empty_track_record(),
        "by_ticker": tickers.into_iter().map(|ticker| (ticker, empty_track_record())).collect::<serde_json::Map<_, _>>(),
    });
    state["agent_accuracy"] = json!({});
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
    // Prefer regime already computed downstream (available after phase 3).
    if let Some(regime) = state
        .get("allocation_context")
        .and_then(|value| value.get("vix"))
        .and_then(|value| value.get("regime"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            state
                .get("portfolio_allocation")
                .and_then(|value| value.get("vix_regime"))
                .and_then(Value::as_str)
        })
        .filter(|value| !value.is_empty())
    {
        return MarketRegime {
            volatility: regime.to_string(),
            ..Default::default()
        };
    }

    MarketRegime::default()
}

fn empty_track_record() -> Value {
    json!({
        "total_predictions": 0,
        "direction_accuracy": 0.0,
        "mean_brier_score": 0.0,
        "mean_probability_error": 0.0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::config::RuntimeConfig;
    use orchestrator_core::RetrievalBudget;
    use orchestrator_store::{
        deterministic_experience_index_id, record_experience_case, FileStore, FileStoreOptions,
        IndexKind, IndexScope, RecordExperienceCaseInput,
    };
    use serde_json::json;

    #[test]
    fn injects_empty_structures_when_enabled_without_data() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = json!({
            "ticker":"QQQ",
            "tickers":["QQQ"],
            "store_root": temp.path(),
        });
        inject_phase_summary_reflection(
            &mut state,
            &test_runtime_config(true, RetrievalBudget::default()),
        )
        .unwrap();

        assert!(state.get("prior_experience").is_some());
        assert!(state.get("track_record").is_some());
        assert!(state.get("agent_accuracy").is_some());
        assert_eq!(
            state["prior_experience"]["indexes_by_ticker"]["QQQ"]["items"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn disabled_config_does_not_inject_reflection_state() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = json!({"ticker":"QQQ", "tickers":["QQQ"], "store_root": temp.path()});
        inject_phase_summary_reflection(
            &mut state,
            &test_runtime_config(false, RetrievalBudget::default()),
        )
        .unwrap();

        assert!(state.get("prior_experience").is_none());
        assert!(state.get("track_record").is_none());
        assert!(state.get("agent_accuracy").is_none());
    }

    #[test]
    fn retrieval_budget_caps_injected_prior_memory() {
        let temp = tempfile::tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        seed_experience(&store, "run-1", "pattern-one", 0.9);
        seed_experience(&store, "run-2", "pattern-two", 0.8);
        let mut state = json!({"ticker":"QQQ", "tickers":["QQQ"], "store_root": temp.path()});
        inject_phase_summary_reflection(
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

        let items = state["prior_experience"]["indexes_by_ticker"]["QQQ"]["items"]
            .as_array()
            .unwrap();
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn source_regime_is_reused_without_database_read() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = json!({
            "ticker":"QQQ",
            "tickers":["QQQ"],
            "store_root": temp.path(),
            "allocation_context":{"vix":{"regime":"defensive"}},
        });
        inject_phase_summary_reflection(
            &mut state,
            &test_runtime_config(true, RetrievalBudget::default()),
        )
        .unwrap();
        assert_eq!(
            state["prior_experience"]["market_regime"]["volatility"],
            "defensive"
        );
    }

    fn seed_experience(store: &FileStore, run_id: &str, pattern_key: &str, confidence: f64) {
        let scope = IndexScope {
            kind: IndexKind::Experience,
            location: None,
            index_id: deterministic_experience_index_id(pattern_key, Some("QQQ"), 0).unwrap(),
            run_id: "reflection-run".to_owned(),
            source_run_id: Some(run_id.to_owned()),
            source_phase: 0,
            role: "reflector.historical".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            source_payload_hash: format!("payload-{run_id}"),
            authoritative_fields: Default::default(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        };
        record_experience_case(
            store,
            RecordExperienceCaseInput {
                scope,
                pattern_key: pattern_key.to_owned(),
                summary: format!("summary {pattern_key}"),
                confidence,
                applies_to_phases: vec![1],
                detail: format!("case {run_id}"),
                source_refs: vec![],
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
