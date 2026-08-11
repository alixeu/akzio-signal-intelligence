//! Deterministic evaluation for a frozen, offline evidence set.
//!
//! The evaluator consumes sealed fixture records only. It does not acquire
//! evidence, mutate policy state, or make a Paper decision.

use std::collections::BTreeSet;

use akzio_domain::{ContentHash, HardBlocker};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const PPM_ONE: u32 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenEvidenceRecord {
    pub case_id: String,
    pub model_version: String,
    pub prompt_hash: ContentHash,
    pub contract_hash: ContentHash,
    pub planner_schema_ok: bool,
    pub claim_schema_ok: bool,
    pub critique_schema_ok: bool,
    pub decision_proposal_schema_ok: bool,
    pub expected_evidence: u64,
    pub observed_evidence: u64,
    pub expected_blockers: BTreeSet<HardBlocker>,
    pub detected_blockers: BTreeSet<HardBlocker>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_micros: u64,
    pub latency_millis: u64,
}

impl FrozenEvidenceRecord {
    pub fn validate(&self) -> Result<(), FrozenEvidenceEvalError> {
        if self.case_id.trim().is_empty() || self.model_version.trim().is_empty() {
            return Err(FrozenEvidenceEvalError::InvalidRecord("identity"));
        }
        if self.observed_evidence > self.expected_evidence && self.expected_evidence != 0 {
            return Err(FrozenEvidenceEvalError::InvalidRecord("evidence_count"));
        }
        if !self.detected_blockers.is_subset(&self.expected_blockers) {
            return Err(FrozenEvidenceEvalError::InvalidRecord("blocker_set"));
        }
        Ok(())
    }

    pub fn schema_ok(&self) -> bool {
        self.planner_schema_ok
            && self.claim_schema_ok
            && self.critique_schema_ok
            && self.decision_proposal_schema_ok
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenEvidenceSet {
    pub set_id: String,
    pub records: Vec<FrozenEvidenceRecord>,
}

impl FrozenEvidenceSet {
    pub fn validate(&self) -> Result<(), FrozenEvidenceEvalError> {
        if self.set_id.trim().is_empty() || self.records.is_empty() {
            return Err(FrozenEvidenceEvalError::InvalidSet);
        }
        let mut ids = BTreeSet::new();
        for record in &self.records {
            record.validate()?;
            if !ids.insert(record.case_id.as_str()) {
                return Err(FrozenEvidenceEvalError::DuplicateCase(
                    record.case_id.clone(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenEvidenceMetrics {
    pub set_id: String,
    pub case_count: u64,
    pub schema_success_cases: u64,
    pub schema_success_rate_ppm: u32,
    pub evidence_completeness_ppm: u32,
    pub blocker_recall_ppm: u32,
    pub total_expected_blockers: u64,
    pub total_detected_blockers: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cost_micros: u64,
    pub average_latency_millis: u64,
    pub model_versions: BTreeSet<String>,
    pub prompt_hashes: BTreeSet<ContentHash>,
    pub contract_hashes: BTreeSet<ContentHash>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrozenEvidenceEvalError {
    #[error("frozen evidence set is empty or has no id")]
    InvalidSet,
    #[error("duplicate frozen evidence case {0}")]
    DuplicateCase(String),
    #[error("invalid frozen evidence record: {0}")]
    InvalidRecord(&'static str),
    #[error("frozen evidence metric overflow")]
    ArithmeticOverflow,
}

pub fn evaluate_frozen_evidence(
    set: &FrozenEvidenceSet,
) -> Result<FrozenEvidenceMetrics, FrozenEvidenceEvalError> {
    set.validate()?;

    let case_count = u64::try_from(set.records.len())
        .map_err(|_| FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let schema_success_cases = set
        .records
        .iter()
        .filter(|record| record.schema_ok())
        .count();
    let schema_success_cases = u64::try_from(schema_success_cases)
        .map_err(|_| FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let total_expected_evidence = set
        .records
        .iter()
        .try_fold(0_u64, |total, record| {
            total.checked_add(record.expected_evidence)
        })
        .ok_or(FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let total_observed_evidence = set
        .records
        .iter()
        .try_fold(0_u64, |total, record| {
            total.checked_add(record.observed_evidence)
        })
        .ok_or(FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let total_expected_blockers = set.records.iter().try_fold(0_u64, |total, record| {
        total.checked_add(record.expected_blockers.len() as u64)
    });
    let total_expected_blockers =
        total_expected_blockers.ok_or(FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let total_detected_blockers = set.records.iter().try_fold(0_u64, |total, record| {
        total.checked_add(record.detected_blockers.len() as u64)
    });
    let total_detected_blockers =
        total_detected_blockers.ok_or(FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let total_input_tokens = set
        .records
        .iter()
        .try_fold(0_u64, |total, record| {
            total.checked_add(record.input_tokens)
        })
        .ok_or(FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let total_output_tokens = set
        .records
        .iter()
        .try_fold(0_u64, |total, record| {
            total.checked_add(record.output_tokens)
        })
        .ok_or(FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let total_cost_micros = set
        .records
        .iter()
        .try_fold(0_u64, |total, record| total.checked_add(record.cost_micros))
        .ok_or(FrozenEvidenceEvalError::ArithmeticOverflow)?;
    let total_latency_millis = set
        .records
        .iter()
        .try_fold(0_u64, |total, record| {
            total.checked_add(record.latency_millis)
        })
        .ok_or(FrozenEvidenceEvalError::ArithmeticOverflow)?;

    let mut model_versions = BTreeSet::new();
    let mut prompt_hashes = BTreeSet::new();
    let mut contract_hashes = BTreeSet::new();
    for record in &set.records {
        model_versions.insert(record.model_version.clone());
        prompt_hashes.insert(record.prompt_hash.clone());
        contract_hashes.insert(record.contract_hash.clone());
    }

    Ok(FrozenEvidenceMetrics {
        set_id: set.set_id.clone(),
        case_count,
        schema_success_cases,
        schema_success_rate_ppm: ratio_ppm(schema_success_cases, case_count)?,
        evidence_completeness_ppm: ratio_ppm(total_observed_evidence, total_expected_evidence)?,
        blocker_recall_ppm: ratio_ppm(total_detected_blockers, total_expected_blockers)?,
        total_expected_blockers,
        total_detected_blockers,
        total_input_tokens,
        total_output_tokens,
        total_cost_micros,
        average_latency_millis: total_latency_millis / case_count,
        model_versions,
        prompt_hashes,
        contract_hashes,
    })
}

fn ratio_ppm(numerator: u64, denominator: u64) -> Result<u32, FrozenEvidenceEvalError> {
    if denominator == 0 {
        return Ok(PPM_ONE);
    }
    let scaled = u128::from(numerator.min(denominator)) * u128::from(PPM_ONE);
    u32::try_from(scaled / u128::from(denominator))
        .map_err(|_| FrozenEvidenceEvalError::ArithmeticOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(seed: &str) -> ContentHash {
        ContentHash::of_bytes(seed.as_bytes())
    }

    fn record(case_id: &str, schema_ok: bool) -> FrozenEvidenceRecord {
        FrozenEvidenceRecord {
            case_id: case_id.to_owned(),
            model_version: "fixture-model-v1".to_owned(),
            prompt_hash: hash("prompt"),
            contract_hash: hash("contract"),
            planner_schema_ok: schema_ok,
            claim_schema_ok: schema_ok,
            critique_schema_ok: schema_ok,
            decision_proposal_schema_ok: schema_ok,
            expected_evidence: 4,
            observed_evidence: 4,
            expected_blockers: BTreeSet::from([HardBlocker::MissingEvidence]),
            detected_blockers: BTreeSet::from([HardBlocker::MissingEvidence]),
            input_tokens: 10,
            output_tokens: 20,
            cost_micros: 3,
            latency_millis: 40,
        }
    }

    #[test]
    fn perfect_frozen_set_reports_complete_metrics() {
        let metrics = evaluate_frozen_evidence(&FrozenEvidenceSet {
            set_id: "fixture-perfect".to_owned(),
            records: vec![record("case-1", true), record("case-2", true)],
        })
        .unwrap();
        assert_eq!(metrics.schema_success_rate_ppm, PPM_ONE);
        assert_eq!(metrics.evidence_completeness_ppm, PPM_ONE);
        assert_eq!(metrics.blocker_recall_ppm, PPM_ONE);
        assert_eq!(metrics.total_input_tokens, 20);
        assert_eq!(metrics.average_latency_millis, 40);
    }

    #[test]
    fn schema_and_blocker_failures_are_visible() {
        let mut incomplete = record("case-2", false);
        incomplete.observed_evidence = 2;
        incomplete.detected_blockers.clear();
        let metrics = evaluate_frozen_evidence(&FrozenEvidenceSet {
            set_id: "fixture-mixed".to_owned(),
            records: vec![record("case-1", true), incomplete],
        })
        .unwrap();
        assert_eq!(metrics.schema_success_rate_ppm, 500_000);
        assert_eq!(metrics.evidence_completeness_ppm, 750_000);
        assert_eq!(metrics.blocker_recall_ppm, 500_000);
    }

    #[test]
    fn duplicate_cases_are_rejected() {
        let error = evaluate_frozen_evidence(&FrozenEvidenceSet {
            set_id: "fixture-duplicate".to_owned(),
            records: vec![record("case-1", true), record("case-1", true)],
        })
        .unwrap_err();
        assert!(matches!(
            error,
            FrozenEvidenceEvalError::DuplicateCase(case) if case == "case-1"
        ));
    }
}
