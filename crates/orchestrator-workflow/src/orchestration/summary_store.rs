//! Compile one completed phase-role response into its canonical Index.

use anyhow::{bail, Context, Result};
use chrono::Utc;
use orchestrator_store::{
    append_index_detail, content_hash, create_index, finalize_index, AppendIndexDetailInput,
    CreateIndexInput, DetailSection, FileStore, FileStoreOptions, Index, IndexKind, IndexScope,
    RunLocation,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::{collections::BTreeSet, path::Path};

use super::summary_units::derive_summary_index_id;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhaseIndexCandidate {
    pub(crate) summary: String,
    #[serde(deserialize_with = "deserialize_confidence")]
    pub(crate) confidence: f64,
    #[serde(default)]
    pub(crate) authoritative_fields: Map<String, Value>,
    #[serde(default, deserialize_with = "deserialize_details")]
    pub(crate) details: Vec<PhaseIndexCandidateDetail>,
    #[serde(default)]
    pub(crate) missing_fields: Vec<String>,
    #[serde(default)]
    pub(crate) ambiguities: Vec<String>,
}

/// Detail is Rust-owned: tolerate a model returning a string, null, or other
/// non-canonical shape here and replace it with the original response later.
fn deserialize_details<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<PhaseIndexCandidateDetail>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let Some(items) = value.as_array() else {
        return Ok(Vec::new());
    };
    Ok(items
        .iter()
        .filter_map(|item| serde_json::from_value(item.clone()).ok())
        .collect())
}

fn deserialize_confidence<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    Ok(value.as_f64().unwrap_or(0.0))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhaseIndexCandidateDetail {
    pub(crate) section: String,
    pub(crate) detail: String,
    #[serde(default)]
    pub(crate) source_refs: Vec<String>,
}

pub(crate) fn parse_phase_index_candidate(text: &str) -> Result<PhaseIndexCandidate> {
    let text = text.trim();
    let candidate = match serde_json::from_str(text) {
        Ok(candidate) => candidate,
        Err(_) => {
            let start = text
                .find('{')
                .context("Summary response contains no JSON object")?;
            let end = text
                .rfind('}')
                .context("Summary response contains no complete JSON object")?;
            serde_json::from_str(&text[start..=end])?
        }
    };
    validate_phase_index_candidate(&candidate)?;
    Ok(candidate)
}

fn validate_phase_index_candidate(candidate: &PhaseIndexCandidate) -> Result<()> {
    if candidate.summary.trim().is_empty()
        || !candidate.confidence.is_finite()
        || !(0.0..=1.0).contains(&candidate.confidence)
    {
        bail!("Phase Summary candidate requires non-empty summary and confidence in 0..=1")
    }
    if candidate.authoritative_fields.keys().any(|key| {
        matches!(
            key.as_str(),
            "run_id" | "index_id" | "source_phase" | "role"
        )
    }) {
        bail!("Phase Summary candidate cannot choose Rust-owned identity fields")
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_compiled_phase_index(
    store_root: &Path,
    state: &Value,
    phase: u8,
    role: &str,
    kind: &str,
    round: Option<i64>,
    ticker: Option<&str>,
    topic_id: Option<&str>,
    response_text: &str,
    mut candidate: PhaseIndexCandidate,
) -> Result<Index> {
    validate_phase_fields(phase, &candidate.authoritative_fields)?;
    let run_id = required_state_string(state, "run_id")?;
    let location = RunLocation::new(required_state_string(state, "current_date")?, &run_id)?;
    let unit_key = [
        format!("phase{phase}"),
        role.to_owned(),
        kind.to_owned(),
        ticker.unwrap_or("aggregate").to_owned(),
        topic_id.unwrap_or("none").to_owned(),
        round.unwrap_or_default().to_string(),
    ]
    .join(":");
    let source_payload_hash = content_hash(&json!({
        "phase": phase,
        "role": role,
        "kind": kind,
        "round": round,
        "ticker": ticker,
        "topic_id": topic_id,
        "response_text": response_text,
    }))?;
    let index_id = derive_summary_index_id(
        &run_id,
        phase,
        role,
        ticker,
        topic_id,
        &unit_key,
        &source_payload_hash,
    );
    candidate
        .authoritative_fields
        .insert("unit_key".to_owned(), Value::String(unit_key));
    candidate.authoritative_fields.insert(
        "missing_fields".to_owned(),
        serde_json::to_value(&candidate.missing_fields)?,
    );
    candidate.authoritative_fields.insert(
        "ambiguities".to_owned(),
        serde_json::to_value(&candidate.ambiguities)?,
    );
    let scope = IndexScope {
        kind: IndexKind::PhaseSummary,
        location: Some(location),
        index_id,
        run_id,
        source_run_id: None,
        source_phase: phase,
        role: role.to_owned(),
        ticker: ticker.map(ToOwned::to_owned),
        topic_id: topic_id.map(ToOwned::to_owned),
        source_payload_hash,
        authoritative_fields: candidate.authoritative_fields,
        created_at: Utc::now().to_rfc3339(),
    };
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    create_index(
        &store,
        CreateIndexInput {
            scope: scope.clone(),
            summary: candidate.summary,
            confidence: candidate.confidence,
            pattern_key: None,
            applies_to_phases: applies_to_phases(phase),
        },
    )?;
    if let Ok(index) = finalize_index(&store, &scope) {
        return Ok(index);
    }
    let source_refs = candidate
        .details
        .iter()
        .flat_map(|detail| detail.source_refs.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    append_index_detail(
        &store,
        AppendIndexDetailInput {
            scope: scope.clone(),
            section: detail_section_for_phase(phase),
            detail: response_text.trim().to_owned(),
            source_refs,
        },
    )?;
    for detail in candidate.details {
        if detail.detail.trim().is_empty() || detail.detail.trim() == response_text.trim() {
            continue;
        }
        append_index_detail(
            &store,
            AppendIndexDetailInput {
                scope: scope.clone(),
                section: DetailSection::parse(&detail.section).unwrap_or(DetailSection::Other),
                detail: detail.detail,
                source_refs: detail.source_refs,
            },
        )?;
    }
    finalize_index(&store, &scope).map_err(Into::into)
}

fn detail_section_for_phase(phase: u8) -> DetailSection {
    match phase {
        0 => DetailSection::HistoricalCase,
        4 | 6 | 7 | 8 => DetailSection::Execution,
        5 => DetailSection::Risk,
        _ => DetailSection::Analysis,
    }
}

fn validate_phase_fields(phase: u8, fields: &Map<String, Value>) -> Result<()> {
    let number_in_range = |key: &str| {
        fields
            .get(key)
            .and_then(Value::as_f64)
            .is_none_or(|value| (0.0..=1.0).contains(&value))
    };
    match phase {
        1 if !number_in_range("confidence") => bail!("Phase 1 confidence must be in 0..=1"),
        3 => {
            let decision = fields
                .get("decision")
                .and_then(Value::as_object)
                .context("Phase 3 Summary requires decision")?;
            let long = decision
                .get("long_probability")
                .and_then(Value::as_f64)
                .context("Phase 3 long_probability is required")?;
            let short = decision
                .get("short_probability")
                .and_then(Value::as_f64)
                .context("Phase 3 short_probability is required")?;
            if !(0.0..=1.0).contains(&long)
                || !(0.0..=1.0).contains(&short)
                || (long + short - 1.0).abs() > 0.001
            {
                bail!("Phase 3 long_probability + short_probability must equal 1")
            }
        }
        4 if !number_in_range("position_size_pct_max") => {
            bail!("Phase 4 position_size_pct_max must be in 0..=1")
        }
        5 => {
            for key in [
                "position_cap_pct",
                "max_drawdown_pct",
                "constraint_confidence",
            ] {
                if !number_in_range(key) {
                    bail!("Phase 5 {key} must be in 0..=1")
                }
            }
        }
        6 => {
            for key in ["max_target_weight", "max_weight_delta"] {
                if !number_in_range(key) {
                    bail!("Phase 6 {key} must be in 0..=1")
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn applies_to_phases(source_phase: u8) -> Vec<u8> {
    (source_phase < 7)
        .then_some(source_phase + 1)
        .into_iter()
        .collect()
}

fn required_state_string(state: &Value, key: &str) -> Result<String> {
    state
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("state.{key} is required for FileStore phase summary"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_parser_accepts_a_json_object_inside_prose() {
        let candidate = parse_phase_index_candidate(
            "result:\n{\"summary\":\"ok\",\"confidence\":0.5,\"authoritative_fields\":{}}",
        )
        .unwrap();
        assert_eq!(candidate.summary, "ok");
    }

    #[test]
    fn phase3_requires_balanced_probabilities() {
        let fields = serde_json::from_value(json!({
            "decision": {"long_probability": 0.7, "short_probability": 0.4}
        }))
        .unwrap();
        assert!(validate_phase_fields(3, &fields).is_err());
    }
}
