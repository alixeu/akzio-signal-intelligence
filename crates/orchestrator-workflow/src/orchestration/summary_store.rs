//! Deterministic FileStore writer for the fixed Phase Summary unit plan.
//!
//! Completed Index directories produced here are the only phase-summary
//! authority. Rust uses this deterministic writer for mock, derived, and
//! degraded units; the live tool runtime uses the same Index service.

use anyhow::{Context, Result};
use chrono::Utc;
use orchestrator_store::{
    append_index_detail, content_hash, create_index, finalize_index, AppendIndexDetailInput,
    CreateIndexInput, DetailSection, FileStore, FileStoreOptions, Index, IndexKind, IndexScope,
    RunLocation,
};
use serde_json::{json, Map, Value};
use std::path::Path;

use super::{
    lifecycle::tickers_from_state,
    summary_units::{SummaryUnit, SummaryUnitPlanRequest, SummaryUnitPlanner, SummaryUnitScope},
};

/// Completed deterministic Indexes for one source phase.  The caller may use
/// this only for diagnostics; the files are the authority.
#[derive(Debug, Clone)]
pub(crate) struct FileStoreSummaryResult {
    pub indexes: Vec<Index>,
}

/// The bounded, completed-artifact payload that may be summarized for one
/// phase. It is intentionally built from state projections only; no reader
/// may reach into mutable market data or a former database at this boundary.
pub(crate) fn phase_summary_source_payload(state: &Value, source_phase: i64) -> Result<Value> {
    let keys: &[&str] = match source_phase {
        1 => &[
            "phase1_index",
            "phase1_brief_md",
            "analyst_results",
            "analyst_conflicts",
            "weighted_probability_base",
        ],
        2 => &[
            "topic_generation_artifact",
            "debate_state_artifact",
            "debate_brief_md",
            "debate_turns",
        ],
        3 => &["research_plan"],
        4 => &["trader_investment_plan"],
        5 => &["risk_debate_state"],
        6 => &["final_trade_decision"],
        7 => &["allocation_result", "portfolio_allocation", "allocation"],
        _ => anyhow::bail!("unsupported phase_summary source phase {source_phase}"),
    };
    let artifacts = keys.iter().fold(Map::new(), |mut out, key| {
        if let Some(value) = state.get(*key).filter(|value| !value.is_null()) {
            out.insert((*key).to_string(), value.clone());
        }
        out
    });
    if artifacts.is_empty() {
        anyhow::bail!("phase_summary source phase {source_phase} has no completed artifacts");
    }
    Ok(json!({
        "source_phase": source_phase,
        "current_date": state.get("current_date").cloned().unwrap_or(Value::Null),
        "tickers": tickers_from_state(state),
        "artifacts": artifacts,
    }))
}

pub(crate) fn write_deterministic_phase_summary(
    store_root: &Path,
    state: &Value,
    source_phase: i64,
    max_units: usize,
) -> Result<FileStoreSummaryResult> {
    let source_phase_u8 =
        u8::try_from(source_phase).context("phase summary source phase must fit in a u8")?;
    let source_payload = phase_summary_source_payload(state, source_phase)?;
    let source_payload_hash = content_hash(&source_payload)?;
    let run_id = required_state_string(state, "run_id")?;
    let date = required_state_string(state, "current_date")?;
    let location = RunLocation::new(date, run_id.clone())?;
    let units = SummaryUnitPlanner::plan(SummaryUnitPlanRequest {
        run_id: run_id.clone(),
        source_payload_hash: source_payload_hash.clone(),
        max_units,
        scope: summary_scope(state, source_phase_u8)?,
    })?;
    let created_at = Utc::now().to_rfc3339();
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let mut indexes = Vec::with_capacity(units.len());
    for unit in &units {
        indexes.push(write_unit(
            &store,
            &location,
            &run_id,
            unit,
            &source_payload,
            &created_at,
        )?);
    }
    Ok(FileStoreSummaryResult { indexes })
}

fn write_unit(
    store: &FileStore,
    location: &RunLocation,
    run_id: &str,
    unit: &SummaryUnit,
    source_payload: &Value,
    created_at: &str,
) -> Result<Index> {
    let fallback = source_for_unit(source_payload, unit);
    let summary = format!(
        "Phase {} {} deterministic summary",
        unit.source_phase, unit.unit_key
    );
    let confidence = 0.0;
    let authoritative_fields = Map::from_iter([("source".to_owned(), fallback.clone())]);
    let scope = IndexScope {
        kind: IndexKind::PhaseSummary,
        location: Some(location.clone()),
        index_id: unit.index_id.clone(),
        run_id: run_id.to_owned(),
        source_run_id: None,
        source_phase: unit.source_phase,
        role: unit.role.clone(),
        ticker: unit.ticker.clone(),
        topic_id: unit.topic_id.clone(),
        source_payload_hash: unit.source_payload_hash.clone(),
        authoritative_fields,
        created_at: created_at.to_owned(),
    };
    create_index(
        store,
        CreateIndexInput {
            scope: scope.clone(),
            summary,
            confidence,
            pattern_key: None,
            applies_to_phases: applies_to_phases(unit.source_phase),
        },
    )?;
    append_index_detail(
        store,
        AppendIndexDetailInput {
            scope: scope.clone(),
            section: DetailSection::Analysis,
            detail: compact_detail(&fallback),
            source_refs: vec![format!(
                "artifact:phase{}:{}",
                unit.source_phase, unit.unit_key
            )],
        },
    )?;
    finalize_index(store, &scope).map_err(Into::into)
}

fn applies_to_phases(source_phase: u8) -> Vec<u8> {
    (source_phase < 7)
        .then_some(source_phase + 1)
        .into_iter()
        .collect()
}

fn detail_section(detail: &str) -> DetailSection {
    let lower = detail.to_ascii_lowercase();
    if lower.contains("conflict") {
        DetailSection::Conflict
    } else if lower.contains("risk") || lower.contains("stop") {
        DetailSection::Risk
    } else if lower.contains("hinge") {
        DetailSection::DecisionHinge
    } else if lower.contains("gap") || lower.contains("missing") {
        DetailSection::DataGap
    } else {
        DetailSection::Analysis
    }
}

fn compact_detail(value: &Value) -> String {
    let encoded =
        serde_json::to_string(value).unwrap_or_else(|_| json!({"unavailable": true}).to_string());
    const MAX_CHARS: usize = 6_000;
    if encoded.chars().count() <= MAX_CHARS {
        encoded
    } else {
        format!(
            "{}…",
            encoded.chars().take(MAX_CHARS - 1).collect::<String>()
        )
    }
}

fn source_for_unit(source_payload: &Value, unit: &SummaryUnit) -> Value {
    // A fallback Detail must remain independently intelligible and cannot
    // manufacture information.  It therefore stores the bounded, already
    // Rust-selected phase payload plus explicit unit identity.
    json!({
        "unit_key": unit.unit_key,
        "role": unit.role,
        "ticker": unit.ticker,
        "topic_id": unit.topic_id,
        "source_payload": source_payload,
    })
}

fn required_state_string(state: &Value, key: &str) -> Result<String> {
    state
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("state.{key} is required for FileStore phase summary"))
}

fn summary_scope(state: &Value, source_phase: u8) -> Result<SummaryUnitScope> {
    let tickers = tickers_from_state(state);
    Ok(match source_phase {
        1 => SummaryUnitScope::Phase1 {
            analyst_roles: non_empty_or(
                state
                    .pointer("/phase1_index/per_ticker")
                    .and_then(Value::as_object)
                    .into_iter()
                    .flat_map(|per_ticker| per_ticker.values())
                    .filter_map(|value| value.get("role_summaries").and_then(Value::as_array))
                    .flatten()
                    .filter_map(|summary| summary.get("role").and_then(Value::as_str))
                    .map(ToOwned::to_owned)
                    .collect(),
                vec![
                    "analyst.technical".to_owned(),
                    "analyst.news_macro".to_owned(),
                ],
            ),
            tickers,
        },
        2 => SummaryUnitScope::Phase2 {
            final_controller_topic_ids: state
                .pointer("/debate_state_artifact/topic_briefs")
                .and_then(Value::as_array)
                .or_else(|| {
                    state
                        .pointer("/topic_generation_artifact/topics")
                        .and_then(Value::as_array)
                })
                .into_iter()
                .flatten()
                .filter_map(|topic| topic.get("topic_id").and_then(Value::as_str))
                .map(ToOwned::to_owned)
                .collect(),
        },
        3 => SummaryUnitScope::Phase3 { tickers },
        4 => SummaryUnitScope::Phase4 { tickers },
        5 => SummaryUnitScope::Phase5 {
            risk_roles: non_empty_or(
                state
                    .pointer("/risk_debate_state/history")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|turn| turn.get("artifact").unwrap_or(turn))
                    .filter_map(|artifact| artifact.get("role").and_then(Value::as_str))
                    .map(ToOwned::to_owned)
                    .collect(),
                vec![
                    "risk.aggressive".to_owned(),
                    "risk.neutral".to_owned(),
                    "risk.conservative".to_owned(),
                ],
            ),
            tickers,
        },
        6 => SummaryUnitScope::Phase6 {
            investable_assets: state
                .get("investable_assets")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect(),
        },
        7 => SummaryUnitScope::Phase7,
        _ => anyhow::bail!("unsupported FileStore phase summary phase {source_phase}"),
    })
}

fn non_empty_or(mut values: Vec<String>, fallback: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    if values.is_empty() {
        fallback
    } else {
        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn deterministic_summary_writes_completed_fixed_units() {
        let temp = tempdir().unwrap();
        let state = json!({
            "run_id": "run-1",
            "current_date": "2026-07-27",
            "tickers": ["QQQ"],
            "investable_assets": ["QQQ"],
            "research_plan": {
                "summary": "Research decision",
                "per_ticker": {"QQQ": {"summary": "QQQ decision", "confidence": 0.7}}
            }
        });
        let result = write_deterministic_phase_summary(temp.path(), &state, 3, 32).unwrap();
        assert_eq!(result.indexes.len(), 1);
        assert_eq!(result.indexes[0].ticker.as_deref(), Some("QQQ"));
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let page = orchestrator_store::read_indexes(
            &store,
            Some(&RunLocation::new("2026-07-27", "run-1").unwrap()),
            &orchestrator_store::IndexQuery {
                limit: 10,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(page.indexes.len(), 1);
    }
}
