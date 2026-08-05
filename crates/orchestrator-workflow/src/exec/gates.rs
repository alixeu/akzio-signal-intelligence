use anyhow::{bail, Result};
use orchestrator_store::{
    read_indexes, FileStore, IndexKind, IndexQuery, RunLocation, RunManifest,
};
use serde_json::Value;

use super::{ExecArgs, RuntimeConfig};

pub(super) fn validate_phase_range(args: &ExecArgs) -> Result<()> {
    if args.from_phase > args.to_phase || args.to_phase > 8 {
        bail!("phase range must satisfy 0 <= from_phase <= to_phase <= 8")
    }
    Ok(())
}

pub(super) fn highest_completed_phase(manifest: &RunManifest) -> Option<u8> {
    manifest
        .phase_status
        .iter()
        .filter(|(_, status)| {
            matches!(
                status,
                orchestrator_store::PhaseStatus::Completed
                    | orchestrator_store::PhaseStatus::Degraded
            )
        })
        .filter_map(|(phase, _)| phase.parse::<u8>().ok())
        .max()
}

pub(super) fn phase_completed(manifest: &RunManifest, phase: u8) -> bool {
    matches!(
        manifest.phase_status.get(&phase.to_string()),
        Some(
            orchestrator_store::PhaseStatus::Completed | orchestrator_store::PhaseStatus::Degraded
        )
    )
}

pub(super) fn has_phase3_retrieval_audit(state: &Value) -> bool {
    state
        .get("role_job_metrics")
        .and_then(Value::as_array)
        .and_then(|metrics| {
            metrics.iter().rev().find(|metric| {
                metric.get("phase").and_then(Value::as_i64) == Some(3)
                    && metric.get("role").and_then(Value::as_str) == Some("manager.research")
                    && metric.get("kind").and_then(Value::as_str) == Some("artifact")
            })
        })
        .and_then(|metric| metric.get("retrieval_audit"))
        .is_some_and(|audit| retrieval_audit_covers_required_source_phases(Some(audit)))
}

pub(super) fn retrieval_audit_covers_required_source_phases(audit: Option<&Value>) -> bool {
    let Some(audit) = audit.and_then(Value::as_object) else {
        return false;
    };
    let visible_source_phases = audit
        .get("visible_source_phases")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    [1, 2].iter().all(|phase| {
        visible_source_phases
            .iter()
            .any(|value| value.as_i64() == Some(i64::from(*phase)))
    })
}

pub(super) fn phase1_summaries_visible_to_phase3(
    store: &FileStore,
    location: &RunLocation,
    runtime: &RuntimeConfig,
) -> Result<bool> {
    phase_summary_visible_to_phase(store, location, runtime, 1, 3, 2)
}

pub(super) fn phase_summary_visible_to_phase(
    store: &FileStore,
    location: &RunLocation,
    runtime: &RuntimeConfig,
    source_phase: u8,
    consumer_phase: u8,
    minimum_visible: usize,
) -> Result<bool> {
    let indexes = read_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(source_phase),
            limit: runtime.tool_managed.max_summary_units_per_phase,
            ..Default::default()
        },
    )?
    .indexes;
    Ok(indexes
        .iter()
        .filter(|index| index.applies_to_phases.contains(&consumer_phase))
        .count()
        >= minimum_visible)
}

pub(super) fn has_required_phase_summaries(
    store: &FileStore,
    location: &RunLocation,
    state: &Value,
    runtime: &RuntimeConfig,
    phase: u8,
) -> Result<bool> {
    let completed = read_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(phase),
            limit: runtime.tool_managed.max_summary_units_per_phase,
            ..Default::default()
        },
    )?
    .indexes;
    Ok(completed.len() >= required_phase_index_count(state, runtime, phase))
}

pub(super) fn required_phase_index_count(
    _state: &Value,
    _runtime: &RuntimeConfig,
    phase: u8,
) -> usize {
    match phase {
        1 => 2,
        // Phase 2 has exactly one cross-phase Index: the reducer compiled
        // after every topic's Controller closure. Warmup, topic generation,
        // and individual stree turns are run-local transient state.
        2 => 1,
        3 | 4 | 6 => 1,
        5 => 3,
        7 | 8 => 1,
        _ => 0,
    }
}
