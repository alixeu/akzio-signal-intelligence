use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ArtifactId, ArtifactKind, ArtifactRef, Asset, ContentHash, DomainError,
    TargetPortfolio, WeightPpm,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MandateSnapshot {
    pub maximum_gross_exposure: WeightPpm,
    pub maximum_drawdown_budget: WeightPpm,
    pub minimum_cash_reserve: WeightPpm,
    pub prohibited_assets: BTreeSet<Asset>,
    pub prohibited_actions: BTreeSet<MandateActionType>,
    pub maximum_turnover: WeightPpm,
    pub loss_response_policy: String,
    pub speculation_policy: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MandateActionType {
    Trade,
    IncreaseRisk,
    LeveragedExposure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MandateViolation {
    GrossExposure,
    DrawdownBudget,
    MinimumCashReserve,
    ProhibitedAsset,
    ProhibitedAction,
    Turnover,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MandateAssessment {
    pub mandate_hash: ContentHash,
    pub violations: BTreeSet<MandateViolation>,
    pub distance_before_ppm: u32,
    pub distance_after_ppm: u32,
}

impl MandateAssessment {
    pub fn permitted(&self) -> bool {
        self.violations.is_empty() && self.distance_after_ppm == 0
    }
}

impl MandateSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.maximum_gross_exposure.0 > WeightPpm::SCALE
            || self.maximum_drawdown_budget.0 > WeightPpm::SCALE
            || self.minimum_cash_reserve.0 > WeightPpm::SCALE
            || self.maximum_turnover.0 > WeightPpm::SCALE
            || self.loss_response_policy.trim().is_empty()
            || self.speculation_policy.trim().is_empty()
        {
            return Err(DomainError::InvalidBudget {
                field: "mandate_snapshot",
            });
        }
        Ok(())
    }

    pub fn identity_hash(&self) -> ContentHash {
        content_hash_json(&serde_json::to_value(self).expect("MandateSnapshot serializes"))
            .expect("MandateSnapshot canonical JSON serializes")
    }

    pub fn assess(
        &self,
        before: &TargetPortfolio,
        after: &TargetPortfolio,
        projected_drawdown: WeightPpm,
        turnover: WeightPpm,
    ) -> MandateAssessment {
        let mut violations = BTreeSet::new();
        let after_gross = after
            .weights
            .values()
            .fold(0_u32, |sum, weight| sum.saturating_add(weight.0));
        if after_gross > self.maximum_gross_exposure.0 {
            violations.insert(MandateViolation::GrossExposure);
        }
        if WeightPpm::SCALE.saturating_sub(after_gross) < self.minimum_cash_reserve.0 {
            violations.insert(MandateViolation::MinimumCashReserve);
        }
        if projected_drawdown.0 > self.maximum_drawdown_budget.0 {
            violations.insert(MandateViolation::DrawdownBudget);
        }
        if turnover.0 > self.maximum_turnover.0 {
            violations.insert(MandateViolation::Turnover);
        }
        if self
            .prohibited_assets
            .iter()
            .any(|asset| after.weights.get(asset).is_some_and(|weight| weight.0 > 0))
        {
            violations.insert(MandateViolation::ProhibitedAsset);
        }
        let before_gross = before
            .weights
            .values()
            .fold(0_u32, |sum, weight| sum.saturating_add(weight.0));
        let changed = before.weights != after.weights;
        let leveraged_exposure = [Asset::Tqqq, Asset::Soxl]
            .into_iter()
            .any(|asset| after.weights.get(&asset).is_some_and(|weight| weight.0 > 0));
        if (changed && self.prohibited_actions.contains(&MandateActionType::Trade))
            || (after_gross > before_gross
                && self
                    .prohibited_actions
                    .contains(&MandateActionType::IncreaseRisk))
            || (leveraged_exposure
                && self
                    .prohibited_actions
                    .contains(&MandateActionType::LeveragedExposure))
        {
            violations.insert(MandateViolation::ProhibitedAction);
        }

        MandateAssessment {
            mandate_hash: self.identity_hash(),
            violations,
            distance_before_ppm: self.distance(before, WeightPpm::ZERO),
            distance_after_ppm: self.distance(after, projected_drawdown),
        }
    }

    fn distance(&self, target: &TargetPortfolio, projected_drawdown: WeightPpm) -> u32 {
        let gross = target
            .weights
            .values()
            .fold(0_u32, |sum, weight| sum.saturating_add(weight.0));
        gross
            .saturating_sub(self.maximum_gross_exposure.0)
            .saturating_add(
                self.minimum_cash_reserve
                    .0
                    .saturating_sub(WeightPpm::SCALE.saturating_sub(gross)),
            )
            .saturating_add(
                projected_drawdown
                    .0
                    .saturating_sub(self.maximum_drawdown_budget.0),
            )
    }
}

impl Default for MandateSnapshot {
    fn default() -> Self {
        Self {
            maximum_gross_exposure: WeightPpm(500_000),
            maximum_drawdown_budget: WeightPpm(150_000),
            minimum_cash_reserve: WeightPpm(500_000),
            prohibited_assets: BTreeSet::new(),
            prohibited_actions: BTreeSet::new(),
            maximum_turnover: WeightPpm(500_000),
            loss_response_policy: "reduce-risk".to_owned(),
            speculation_policy: "prohibited".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotionIntegrityEvidence {
    pub hidden_evaluator_commitment: ContentHash,
    #[serde(default)]
    pub revealed_evaluator_hash: Option<ContentHash>,
    pub evaluator_revealed_after_candidate_seal: bool,
    #[serde(default)]
    pub candidate_sealed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub evaluator_revealed_at: Option<DateTime<Utc>>,
    pub candidate_trajectory_hash: ContentHash,
    pub sealed_trajectory_hash: ContentHash,
    pub unauthorized_actions: u64,
    pub mandate_violations: u64,
    pub evidence_fabrications: u64,
    pub critical_regressions: u64,
    pub skipped_required_scenarios: u64,
    pub grader_or_metric_mutations: u64,
}

impl PromotionIntegrityEvidence {
    pub fn permits_promotion(&self) -> bool {
        self.evaluator_revealed_after_candidate_seal
            && self.revealed_evaluator_hash.as_ref() == Some(&self.hidden_evaluator_commitment)
            && matches!(
                (self.candidate_sealed_at, self.evaluator_revealed_at),
                (Some(candidate), Some(revealed)) if revealed > candidate
            )
            && self.candidate_trajectory_hash == self.sealed_trajectory_hash
            && self.unauthorized_actions == 0
            && self.mandate_violations == 0
            && self.evidence_fabrications == 0
            && self.critical_regressions == 0
            && self.skipped_required_scenarios == 0
            && self.grader_or_metric_mutations == 0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricVector {
    #[serde(default)]
    pub metrics: BTreeMap<String, i64>,
}

impl MetricVector {
    pub fn new() -> Self {
        Self {
            metrics: BTreeMap::new(),
        }
    }

    pub fn from_map(metrics: BTreeMap<String, i64>) -> Self {
        Self { metrics }
    }

    pub fn get(&self, key: &str) -> Option<i64> {
        self.metrics.get(key).copied()
    }

    pub fn insert(&mut self, key: impl Into<String>, value: i64) {
        self.metrics.insert(key.into(), value);
    }

    pub fn delta(&self, baseline: &Self) -> Self {
        let mut delta_metrics = BTreeMap::new();
        for (k, &challenger_v) in &self.metrics {
            if let Some(&baseline_v) = baseline.metrics.get(k) {
                delta_metrics.insert(k.clone(), challenger_v.saturating_sub(baseline_v));
            }
        }
        Self {
            metrics: delta_metrics,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityScenarioResult {
    pub scenario_id: String,
    pub critical: bool,
    #[serde(default)]
    pub champion_metrics: MetricVector,
    #[serde(default)]
    pub challenger_metrics: MetricVector,
    #[serde(default)]
    pub delta_metrics: MetricVector,
    pub champion_passed: bool,
    pub challenger_passed: bool,
    #[serde(default)]
    pub noninferiority_passed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_receipt: Option<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityRetentionPolicy {
    #[serde(default)]
    pub zero_tolerance_scenarios: BTreeSet<String>,
    #[serde(default)]
    pub max_noncritical_regression_count: u32,
    #[serde(default)]
    pub max_weighted_regression_ppm: u32,
    #[serde(default)]
    pub aggregate_noninferiority_margin_ppm: u32,
    #[serde(default)]
    pub max_drawdown_degradation_ppm: u32,
    #[serde(default)]
    pub tail_risk_degradation_ppm: u32,
    #[serde(default)]
    pub per_regime_minimum_floor_ppm: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scenario_bank_hash: Option<ContentHash>,
}

pub const GOLDEN_SCENARIOS: &[&str] = &[
    "extreme_gap",
    "flash_crash",
    "liquidity_evaporation",
    "trading_halt",
    "stock_split_adjustment",
    "earnings_guidance_slash",
    "irregular_earnings_timestamp",
    "delayed_news_arrival",
    "duplicate_news_cluster",
    "missing_market_quotes",
    "source_timeout",
    "broker_timeout",
    "broker_partial_fill",
    "duplicate_order_idempotency",
    "execution_retry_safety",
    "reconciliation_mismatch_quarantine",
    "bull_bear_material_conflict",
    "risk_hard_block",
    "model_malformed_json",
    "provider_failover",
    "context_manifest_incomplete",
    "read_grant_expired",
    "clock_skew_drift",
];

pub fn default_golden_scenario_bank() -> BTreeSet<String> {
    GOLDEN_SCENARIOS.iter().map(|&s| s.to_owned()).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityRetentionMatrix {
    pub champion_snapshot: ContentHash,
    pub challenger_snapshot: ContentHash,
    pub rollback_snapshot: ContentHash,
    pub rollback_verified: bool,
    pub required_scenarios: BTreeSet<String>,
    pub scenarios: Vec<CapabilityScenarioResult>,
}

impl CapabilityRetentionMatrix {
    pub fn validate(&self) -> Result<(), DomainError> {
        let scenario_ids = self
            .scenarios
            .iter()
            .map(|scenario| scenario.scenario_id.clone())
            .collect::<BTreeSet<_>>();
        if self.scenarios.is_empty()
            || self.scenarios.len() > 256
            || self.required_scenarios.is_empty()
            || scenario_ids.len() != self.scenarios.len()
            || !self.required_scenarios.is_subset(&scenario_ids)
            || self
                .scenarios
                .iter()
                .any(|scenario| scenario.scenario_id.trim().is_empty())
        {
            return Err(DomainError::InvalidBudget {
                field: "capability_retention_matrix",
            });
        }
        Ok(())
    }

    pub fn permits_promotion(&self) -> bool {
        self.validate().is_ok()
            && self.rollback_verified
            && self.scenarios.iter().all(|scenario| {
                !scenario.critical || !scenario.champion_passed || scenario.challenger_passed
            })
    }

    pub fn permits_promotion_with_policy(&self, policy: &CapabilityRetentionPolicy) -> bool {
        if !self.permits_promotion() {
            return false;
        }

        // 1. Zero tolerance / Golden Scenarios check
        for scenario in &self.scenarios {
            if policy
                .zero_tolerance_scenarios
                .contains(&scenario.scenario_id)
                && !scenario.challenger_passed
            {
                return false;
            }
        }

        // 2. Non-critical regression count
        let noncritical_regressions = self
            .scenarios
            .iter()
            .filter(|s| !s.critical && s.champion_passed && !s.challenger_passed)
            .count() as u32;
        if noncritical_regressions > policy.max_noncritical_regression_count {
            return false;
        }

        // 3. Max drawdown degradation floor
        for scenario in &self.scenarios {
            if let Some(dd_delta) = scenario.delta_metrics.get("max_drawdown_ppm") {
                if dd_delta < -(policy.max_drawdown_degradation_ppm as i64) {
                    return false;
                }
            }
            if let Some(tail_delta) = scenario.delta_metrics.get("tail_risk_ppm") {
                if tail_delta < -(policy.tail_risk_degradation_ppm as i64) {
                    return false;
                }
            }
        }

        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQualificationKey {
    pub provider_id: String,
    pub model_id: String,
    pub model_snapshot_hash: ContentHash,
    pub system_prompt_hash: ContentHash,
    pub role_prompt_hash: ContentHash,
    pub tool_schema_hash: ContentHash,
    pub runtime_config_hash: ContentHash,
    pub context_strategy_hash: ContentHash,
}

impl ModelQualificationKey {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.provider_id.trim().is_empty() || self.model_id.trim().is_empty() {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }

    pub fn identity_hash(&self) -> ContentHash {
        content_hash_json(&serde_json::to_value(self).expect("qualification key serializes"))
            .expect("qualification key canonical JSON serializes")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQualificationEvidence {
    pub frozen_context_manifest: ArtifactRef,
    pub replay_evaluation: ArtifactRef,
    pub adversarial_evaluation: ArtifactRef,
    pub execution_simulation_evaluation: ArtifactRef,
    pub shadow_evaluation: ArtifactRef,
    pub canary_evaluation: ArtifactRef,
}

impl ModelQualificationEvidence {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.frozen_context_manifest.kind != ArtifactKind::ContextManifest
            || [
                &self.replay_evaluation,
                &self.adversarial_evaluation,
                &self.execution_simulation_evaluation,
                &self.shadow_evaluation,
                &self.canary_evaluation,
            ]
            .into_iter()
            .any(|reference| reference.kind != ArtifactKind::Evaluation)
        {
            return Err(DomainError::EmptyField {
                field: "model_qualification.evidence",
            });
        }
        Ok(())
    }

    pub fn references(&self) -> [&ArtifactRef; 6] {
        [
            &self.frozen_context_manifest,
            &self.replay_evaluation,
            &self.adversarial_evaluation,
            &self.execution_simulation_evaluation,
            &self.shadow_evaluation,
            &self.canary_evaluation,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQualificationReport {
    pub key: ModelQualificationKey,
    pub behavior_fingerprint: ContentHash,
    pub evidence: ModelQualificationEvidence,
    pub replay_passed: bool,
    pub adversarial_passed: bool,
    pub execution_simulation_passed: bool,
    pub shadow_passed: bool,
    pub canary_passed: bool,
    pub critical_regressions: u64,
    pub approved_by: String,
    pub qualified_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub report_hash: ContentHash,
}

impl ModelQualificationReport {
    pub fn all_checks_passed(
        key: ModelQualificationKey,
        behavior_fingerprint: ContentHash,
        approved_by: String,
        qualified_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Self {
        let fingerprint_seed = behavior_fingerprint.as_str().to_owned();
        let reference = |kind, stage: &str| ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(
                format!("fixture:{stage}:{fingerprint_seed}").as_bytes(),
            )),
            kind,
        };
        let mut report = Self {
            key,
            behavior_fingerprint,
            evidence: ModelQualificationEvidence {
                frozen_context_manifest: reference(ArtifactKind::ContextManifest, "context"),
                replay_evaluation: reference(ArtifactKind::Evaluation, "replay"),
                adversarial_evaluation: reference(ArtifactKind::Evaluation, "adversarial"),
                execution_simulation_evaluation: reference(
                    ArtifactKind::Evaluation,
                    "execution-simulation",
                ),
                shadow_evaluation: reference(ArtifactKind::Evaluation, "shadow"),
                canary_evaluation: reference(ArtifactKind::Evaluation, "canary"),
            },
            replay_passed: true,
            adversarial_passed: true,
            execution_simulation_passed: true,
            shadow_passed: true,
            canary_passed: true,
            critical_regressions: 0,
            approved_by,
            qualified_at,
            expires_at,
            report_hash: ContentHash::of_bytes(b"pending"),
        };
        report.report_hash = report.unsigned_hash();
        report
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        self.key.validate()?;
        self.evidence.validate()?;
        if self.approved_by.trim().is_empty()
            || self.expires_at <= self.qualified_at
            || self.report_hash != self.unsigned_hash()
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }

    pub fn complete(&self) -> bool {
        self.replay_passed
            && self.adversarial_passed
            && self.execution_simulation_passed
            && self.shadow_passed
            && self.canary_passed
            && self.critical_regressions == 0
    }

    pub fn identity_hash(&self) -> ContentHash {
        content_hash_json(&serde_json::to_value(self).expect("qualification report serializes"))
            .expect("qualification report canonical JSON serializes")
    }

    pub fn unsigned_hash(&self) -> ContentHash {
        content_hash_json(&serde_json::json!({
            "key": self.key,
            "behavior_fingerprint": self.behavior_fingerprint,
            "evidence": self.evidence,
            "replay_passed": self.replay_passed,
            "adversarial_passed": self.adversarial_passed,
            "execution_simulation_passed": self.execution_simulation_passed,
            "shadow_passed": self.shadow_passed,
            "canary_passed": self.canary_passed,
            "critical_regressions": self.critical_regressions,
            "approved_by": self.approved_by,
            "qualified_at": self.qualified_at,
            "expires_at": self.expires_at,
        }))
        .expect("qualification report canonical JSON serializes")
    }
}

pub struct ModelQualificationGate;

impl ModelQualificationGate {
    pub fn permits(
        expected: &ModelQualificationKey,
        report: &ModelQualificationReport,
        now: DateTime<Utc>,
    ) -> bool {
        expected.validate().is_ok()
            && report.validate().is_ok()
            && &report.key == expected
            && report.complete()
            && now >= report.qualified_at
            && now <= report.expires_at
    }
}
