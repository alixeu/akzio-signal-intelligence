// 文件导读：提供 Mandate/Promotion 评估、指标向量、能力保留矩阵和模型资格报告。
// 这些纯领域计算用于把约束、回归和资格证据绑定到候选身份，不执行外部动作。
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ArtifactKind, ArtifactRef, Asset, ContentHash, DomainError, TargetPortfolio,
    WeightPpm,
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
    // 仅当没有违规且调整后的距离为零时允许该组合通过 Mandate。
    pub fn permitted(&self) -> bool {
        self.violations.is_empty() && self.distance_after_ppm == 0
    }
}

impl MandateSnapshot {
    // 校验所有 ppm 限制和两条策略文本的非空性。
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

    // 对 Mandate 快照的序列化值计算稳定身份哈希。
    pub fn identity_hash(&self) -> ContentHash {
        content_hash_json(&serde_json::to_value(self).expect("MandateSnapshot serializes"))
            .expect("MandateSnapshot canonical JSON serializes")
    }

    // 比较 before/after 组合的总敞口、现金、回撤、换手、禁用资产和禁用动作。
    pub fn assess(
        &self,
        before: &TargetPortfolio,
        after: &TargetPortfolio,
        projected_drawdown: WeightPpm,
        turnover: WeightPpm,
    ) -> MandateAssessment {
        let mut violations = BTreeSet::new();
        // fold 使用 saturating_add，任何异常权重都不会让审计计算回绕。
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

    // 用超出总敞口/现金/回撤上限的 ppm 之和衡量组合距离 Mandate 的偏差。
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
    // 提供保守的 50% 总敞口、50% 现金和 15% 回撤默认快照。
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
    // 检查 evaluator 先承诺后揭示、轨迹未变且所有安全/完整性计数为零。
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
    // 创建空的有序指标集合。
    pub fn new() -> Self {
        Self {
            metrics: BTreeMap::new(),
        }
    }

    // 从调用方提供的 BTreeMap 构造指标向量，不做额外归一化。
    pub fn from_map(metrics: BTreeMap<String, i64>) -> Self {
        Self { metrics }
    }

    // 查询指标并复制 i64 值，缺少 key 返回 None。
    pub fn get(&self, key: &str) -> Option<i64> {
        self.metrics.get(key).copied()
    }

    // 插入/覆盖一个指标值，key 转为拥有的 String。
    pub fn insert(&mut self, key: impl Into<String>, value: i64) {
        self.metrics.insert(key.into(), value);
    }

    // 只对双方共有的 key 计算 challenger-baseline，差值采用饱和减法。
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
    // 将编译期固定场景数组复制为有序集合，供资格矩阵复用。
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
    // 校验场景非空/唯一/必需子集及名称，并限制最多 256 个场景。
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

    // 基础矩阵合法、rollback 已验证且所有关键 champion 能力未回归时允许晋级。
    pub fn permits_promotion(&self) -> bool {
        self.validate().is_ok()
            && self.rollback_verified
            && self.scenarios.iter().all(|scenario| {
                !scenario.critical || !scenario.champion_passed || scenario.challenger_passed
            })
    }

    // 叠加零容忍、非关键回归数量以及回撤/尾部风险退化阈值。
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
        // filter 统计“champion 通过而 challenger 未通过”的非关键场景。
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
    // provider/model 文本必须存在；各类哈希由 RuntimeIdentity 产生并参与身份。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.provider_id.trim().is_empty() || self.model_id.trim().is_empty() {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }

    // 对完整资格键计算内容哈希。
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
    // 冻结 ContextManifest，后续五个资格阶段均必须引用 Evaluation Artifact。
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

    // 按固定顺序返回六个证据引用，供 provenance 闭包检查。
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
    // 校验资格键、证据 kind、审批人/有效期和不含 report_hash 的签名哈希。
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

    // 五个阶段全通过且没有 critical regression 才算完整资格报告。
    pub fn complete(&self) -> bool {
        self.replay_passed
            && self.adversarial_passed
            && self.execution_simulation_passed
            && self.shadow_passed
            && self.canary_passed
            && self.critical_regressions == 0
    }

    // 对完整报告序列化值计算包含 report_hash 的观察身份。
    pub fn identity_hash(&self) -> ContentHash {
        content_hash_json(&serde_json::to_value(self).expect("qualification report serializes"))
            .expect("qualification report canonical JSON serializes")
    }

    // 计算审批字段的无自引用哈希，作为 report_hash 的验证依据。
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
    // 同时检查期望键、报告、阶段完整性和当前时间窗口，返回是否可用于启动。
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
