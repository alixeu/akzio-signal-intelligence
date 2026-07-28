//! Deterministic FileStore writer for the fixed Phase Summary unit plan.
//!
//! Completed Index directories produced here are the only phase-summary
//! authority. Rust uses this deterministic writer for mock, derived, and
//! degraded units; the live tool runtime uses the same Index service.

use anyhow::{Context, Result};
use chrono::Utc;
use orchestrator_store::{
    append_index_detail, content_hash, create_index, finalize_index, read_run_manifest,
    validate_content_hash_at, AppendIndexDetailInput, CreateIndexInput, DetailSection, FileStore,
    FileStoreOptions, Index, IndexKind, IndexScope, RunLocation,
};
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use super::summary_units::{
    SummaryUnit, SummaryUnitPlanRequest, SummaryUnitPlanner, SummaryUnitScope,
};

/// The bounded, completed-artifact payload that may be summarized for one
/// phase. The RunManifest is a reference-only catalog; each referenced,
/// finalized Artifact is read and checked here. Mutable workflow state only
/// supplies the run identity needed to locate that catalog.
pub(crate) fn finalized_phase_artifact_catalog(
    store_root: &Path,
    state: &Value,
    source_phase: i64,
) -> Result<Value> {
    let source_phase =
        u8::try_from(source_phase).context("phase summary source phase must fit in a u8")?;
    if !(1..=7).contains(&source_phase) {
        anyhow::bail!("unsupported phase_summary source phase {source_phase}");
    }
    let location = RunLocation::new(
        required_state_string(state, "current_date")?,
        required_state_string(state, "run_id")?,
    )?;
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let manifest = read_run_manifest(&store, &location)?;
    let mut artifacts = BTreeMap::new();
    for reference in manifest
        .artifacts
        .values()
        .filter(|reference| reference.phase == source_phase)
    {
        let relative = location.child_relative(&reference.relative_path())?;
        let artifact = store.read_json_value(&relative)?;
        validate_content_hash_at(&artifact, &store.root().join(&relative))?;
        validate_artifact_header(&artifact, reference)?;
        artifacts.insert(reference.artifact_id.clone(), artifact);
    }
    if artifacts.is_empty() {
        anyhow::bail!("phase_summary source phase {source_phase} has no finalized artifacts");
    }
    let tickers = artifact_tickers(artifacts.values());
    Ok(json!({
        "source_phase": source_phase,
        "current_date": manifest.current_date,
        "run_id": manifest.run_id,
        "tickers": tickers,
        "artifacts": artifacts,
    }))
}

pub(crate) fn write_deterministic_phase_summary(
    store_root: &Path,
    state: &Value,
    source_phase: i64,
    max_units: usize,
) -> Result<Vec<Index>> {
    let source_phase_u8 =
        u8::try_from(source_phase).context("phase summary source phase must fit in a u8")?;
    let source_payload = finalized_phase_artifact_catalog(store_root, state, source_phase)?;
    let source_payload_hash = content_hash(&source_payload)?;
    let run_id = required_state_string(state, "run_id")?;
    let date = required_state_string(state, "current_date")?;
    let location = RunLocation::new(date, run_id.clone())?;
    let units = SummaryUnitPlanner::plan(SummaryUnitPlanRequest {
        run_id: run_id.clone(),
        source_payload_hash: source_payload_hash.clone(),
        max_units,
        scope: summary_scope(&source_payload, source_phase_u8)?,
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
    Ok(indexes)
}

/// Plan the fixed, Rust-owned Summary Units for a completed source phase.
/// Both the live compressor and deterministic/mock writer use this exact
/// planner, so a model never chooses Index count or ownership.
pub(crate) fn planned_summary_units(
    store_root: &Path,
    state: &Value,
    source_phase: i64,
    max_units: usize,
) -> Result<(Value, Vec<SummaryUnit>)> {
    let source_phase_u8 =
        u8::try_from(source_phase).context("phase summary source phase must fit in a u8")?;
    let source_payload = finalized_phase_artifact_catalog(store_root, state, source_phase)?;
    let source_payload_hash = content_hash(&source_payload)?;
    let run_id = required_state_string(state, "run_id")?;
    let units = SummaryUnitPlanner::plan(SummaryUnitPlanRequest {
        run_id,
        source_payload_hash,
        max_units,
        scope: summary_scope(&source_payload, source_phase_u8)?,
    })?;
    Ok((source_payload, units))
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
    let authoritative_fields = Map::from_iter([
        ("source".to_owned(), fallback.clone()),
        ("unit_key".to_owned(), Value::String(unit.unit_key.clone())),
    ]);
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
    // A live Summary Agent may have completed this fixed unit immediately
    // before its enclosing phase failed.  `create_index` correctly returns
    // that canonical Index, but a deterministic fallback must not recreate a
    // draft directory merely to append another Detail.  Ask the same
    // finalizer first: success proves the completed directory is authoritative
    // and makes the fallback idempotent.
    if let Ok(completed) = finalize_index(store, &scope) {
        return Ok(completed);
    }
    append_index_detail(
        store,
        AppendIndexDetailInput {
            scope: scope.clone(),
            section: DetailSection::Analysis,
            detail: compact_detail(&fallback),
            source_refs: canonical_source_refs(&fallback),
        },
    )?;
    finalize_index(store, &scope).map_err(Into::into)
}

/// Detail references are authoritative IDs from the finalized payload, never
/// a reconstructed logical name, which cannot be resolved after a restart
/// and would create a second authority beside the canonical Artifact file.
fn canonical_source_refs(value: &Value) -> Vec<String> {
    let mut refs = BTreeSet::new();
    collect_canonical_source_refs(value, &mut refs);
    refs.into_iter().collect()
}

fn collect_canonical_source_refs(value: &Value, refs: &mut BTreeSet<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_canonical_source_refs(value, refs);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "artifact_id" | "index_id") {
                    if let Some(id) = value.as_str().filter(|id| !id.trim().is_empty()) {
                        refs.insert(id.to_owned());
                    }
                }
                collect_canonical_source_refs(value, refs);
            }
        }
        _ => {}
    }
}

fn applies_to_phases(source_phase: u8) -> Vec<u8> {
    (source_phase < 7)
        .then_some(source_phase + 1)
        .into_iter()
        .collect()
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
    // A fallback Detail must remain independently intelligible without
    // duplicating the phase catalog. Keep only the finalized artifacts that
    // belong to this Unit and their typed payloads; the Artifact files remain
    // the authority for every other field.
    let mut authoritative_fields = Map::new();
    let mut artifact_ids = Vec::new();
    for artifact in source_payload
        .get("artifacts")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|artifacts| artifacts.values())
        .filter(|artifact| artifact_matches_unit(artifact, unit))
    {
        let Some(artifact_id) = artifact.get("artifact_id").and_then(Value::as_str) else {
            continue;
        };
        artifact_ids.push(Value::String(artifact_id.to_owned()));
        authoritative_fields.insert(
            artifact_id.to_owned(),
            json!({
                "role": artifact.get("role"),
                "profile": artifact.get("profile"),
                "phase": artifact.get("phase"),
                "unit_key": artifact.get("unit_key"),
                "ticker": artifact.get("ticker"),
                "topic_id": artifact.get("topic_id"),
                "payload": artifact.get("payload"),
            }),
        );
    }
    json!({
        "unit_key": unit.unit_key,
        "role": unit.role,
        "ticker": unit.ticker,
        "topic_id": unit.topic_id,
        "finalized_artifact_ids": artifact_ids,
        "authoritative_fields": authoritative_fields,
        "degraded_reason": "deterministic phase summary; no model-generated summary was available",
    })
}

fn artifact_matches_unit(artifact: &Value, unit: &SummaryUnit) -> bool {
    let role_matches = artifact.get("role").and_then(Value::as_str) == Some(unit.role.as_str());
    let ticker_matches = unit
        .ticker
        .as_deref()
        .is_none_or(|ticker| artifact.get("ticker").and_then(Value::as_str) == Some(ticker));
    let topic_matches = unit
        .topic_id
        .as_deref()
        .is_none_or(|topic_id| artifact.get("topic_id").and_then(Value::as_str) == Some(topic_id));
    role_matches && ticker_matches && topic_matches
}

fn required_state_string(state: &Value, key: &str) -> Result<String> {
    state
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("state.{key} is required for FileStore phase summary"))
}

fn validate_artifact_header(
    artifact: &Value,
    reference: &orchestrator_store::FinalizedArtifactRef,
) -> Result<()> {
    let field = |name: &str| artifact.get(name).and_then(Value::as_str);
    if field("artifact_id") != Some(reference.artifact_id.as_str())
        || field("role") != Some(reference.role.as_str())
        || field("profile") != Some(reference.profile.as_str())
        || field("unit_key") != Some(reference.unit_key.as_str())
        || field("source_payload_hash") != Some(reference.source_payload_hash.as_str())
        || artifact.get("phase").and_then(Value::as_u64) != Some(u64::from(reference.phase))
    {
        anyhow::bail!(
            "finalized artifact {} does not match its RunManifest reference",
            reference.artifact_id
        );
    }
    Ok(())
}

fn artifact_tickers<'a>(artifacts: impl IntoIterator<Item = &'a Value>) -> Vec<String> {
    let mut tickers = BTreeSet::new();
    for artifact in artifacts {
        collect_artifact_tickers(artifact, &mut tickers);
    }
    tickers.into_iter().collect()
}

fn collect_artifact_tickers(value: &Value, tickers: &mut BTreeSet<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_artifact_tickers(value, tickers);
            }
        }
        Value::Object(object) => {
            if let Some(ticker) = object.get("ticker").and_then(Value::as_str) {
                if !ticker.trim().is_empty() {
                    tickers.insert(ticker.to_owned());
                }
            }
            for key in ["per_ticker", "per_asset"] {
                if let Some(entries) = object.get(key).and_then(Value::as_object) {
                    tickers.extend(entries.keys().cloned());
                }
            }
            for value in object.values() {
                collect_artifact_tickers(value, tickers);
            }
        }
        _ => {}
    }
}

fn artifact_roles(source_payload: &Value, profile: &str) -> Vec<String> {
    let mut roles = BTreeSet::new();
    for artifact in source_payload
        .get("artifacts")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|artifacts| artifacts.values())
    {
        if artifact.get("profile").and_then(Value::as_str) == Some(profile) {
            if let Some(role) = artifact.get("role").and_then(Value::as_str) {
                roles.insert(role.to_owned());
            }
        }
    }
    roles.into_iter().collect()
}

fn artifact_topic_ids(value: &Value) -> Vec<String> {
    let mut topics = BTreeSet::new();
    collect_topic_ids(value, &mut topics);
    topics.into_iter().collect()
}

fn collect_topic_ids(value: &Value, topics: &mut BTreeSet<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_topic_ids(value, topics);
            }
        }
        Value::Object(object) => {
            if let Some(topic_id) = object.get("topic_id").and_then(Value::as_str) {
                if !topic_id.trim().is_empty() {
                    topics.insert(topic_id.to_owned());
                }
            }
            for value in object.values() {
                collect_topic_ids(value, topics);
            }
        }
        _ => {}
    }
}

fn source_tickers(source_payload: &Value) -> Vec<String> {
    source_payload
        .get("tickers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

fn summary_scope(source_payload: &Value, source_phase: u8) -> Result<SummaryUnitScope> {
    let tickers = source_tickers(source_payload);
    Ok(match source_phase {
        1 => SummaryUnitScope::Phase1 {
            analyst_roles: artifact_roles(source_payload, "analyst_report"),
            tickers,
        },
        2 => SummaryUnitScope::Phase2 {
            final_controller_topic_ids: artifact_topic_ids(source_payload),
        },
        3 => SummaryUnitScope::Phase3 { tickers },
        4 => SummaryUnitScope::Phase4 { tickers },
        5 => SummaryUnitScope::Phase5 {
            risk_roles: artifact_roles(source_payload, "risk_review"),
            tickers,
        },
        6 => SummaryUnitScope::Phase6 {
            investable_assets: tickers,
        },
        7 => SummaryUnitScope::Phase7,
        _ => anyhow::bail!("unsupported FileStore phase summary phase {source_phase}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn deterministic_summary_writes_completed_fixed_units() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "run-1").unwrap();
        let source_hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let artifact_relative = Path::new("artifacts/phase3/QQQ.json");
        let mut artifact = json!({
            "schema_version": orchestrator_store::DOMAIN_ARTIFACT_SCHEMA_VERSION,
            "artifact_id": "artifact-research-qqq",
            "run_id": "run-1",
            "phase": 3,
            "role": "manager.research",
            "profile": "research_decision",
            "unit_key": "phase3:research-decision:ticker:QQQ",
            "source_payload_hash": source_hash,
            "ticker": "QQQ",
            "topic_id": null,
            "side": null,
            "stance": null,
            "round": null,
            "payload": {"decision": {"rating": "Hold"}, "decision_hinges": []},
            "evidence_refs": [],
            "created_at": "2026-07-27T00:00:00Z",
            "content_hash": "",
        });
        artifact["content_hash"] = Value::String(content_hash(&artifact).unwrap());
        store
            .write_json_value(
                &location.child_relative(artifact_relative).unwrap(),
                &artifact,
            )
            .unwrap();
        let mut manifest =
            orchestrator_store::RunManifest::new(orchestrator_store::RunManifestInit {
                location: location.clone(),
                workflow_version: "test".to_owned(),
                prompt_versions: Default::default(),
                git_sha: "test".to_owned(),
                config_hash: source_hash.to_owned(),
                role_profile_registry_hash: source_hash.to_owned(),
                created_at: "2026-07-27T00:00:00Z".to_owned(),
            })
            .unwrap();
        manifest
            .record_finalized_artifact(
                orchestrator_store::FinalizedArtifactRef::new(
                    "artifact-research-qqq",
                    artifact_relative,
                    3,
                    "manager.research",
                    "research_decision",
                    "phase3:research-decision:ticker:QQQ",
                    source_hash,
                    "2026-07-27T00:00:00Z",
                )
                .unwrap(),
            )
            .unwrap();
        orchestrator_store::write_run_manifest(&store, &location, manifest).unwrap();
        let state = json!({
            "run_id": "run-1",
            "current_date": "2026-07-27",
        });
        let source = finalized_phase_artifact_catalog(temp.path(), &state, 3).unwrap();
        let unit = SummaryUnit {
            source_phase: 3,
            role: "manager.research".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            unit_key: "phase3:research-decision:ticker:QQQ".to_owned(),
            source_payload_hash: source_hash.to_owned(),
            index_id: "idx-test".to_owned(),
        };
        let fallback = source_for_unit(&source, &unit);
        assert!(fallback.get("source_payload").is_none());
        assert_eq!(
            fallback["finalized_artifact_ids"],
            json!(["artifact-research-qqq"])
        );
        assert_eq!(
            fallback["authoritative_fields"]["artifact-research-qqq"]["payload"]["decision"]
                ["rating"],
            json!("Hold")
        );
        assert!(fallback["degraded_reason"].is_string());
        let mutated_state = json!({
            "run_id": "run-1",
            "current_date": "2026-07-27",
            "research_plan": {"per_ticker": {"NOT_AN_ARTIFACT": {"rating": "Buy"}}},
            "risk_debate_state": {"untrusted": true},
        });
        assert_eq!(
            source,
            finalized_phase_artifact_catalog(temp.path(), &mutated_state, 3).unwrap()
        );
        let result = write_deterministic_phase_summary(temp.path(), &state, 3, 32).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].ticker.as_deref(), Some("QQQ"));
        let recovered = write_deterministic_phase_summary(temp.path(), &state, 3, 32).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].index_id, result[0].index_id);
        let page = orchestrator_store::read_indexes(
            &store,
            Some(&location),
            &orchestrator_store::IndexQuery {
                limit: 10,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(page.indexes.len(), 1);
    }
}
