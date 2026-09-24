// 文件导读：定义 Forecast、DecisionDraft/Context、研究分配、资格诊断和证据资格谓词。
// 这里把模型研究意图与 Rust 执行目标分开，并集中保留 blocker、风险和可审计 trace。
//! Typed decision inputs and risk findings.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    Asset, ContentHash, DecisionId, DomainError, EvidenceGroundRole, ResearchClaim, ResearchShard,
    RunId, TargetPortfolio, WeightPpm, DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardBlocker {
    UnsupportedUniverse,
    NoExecutableOrder,
    Frozen,
    MissingEvidence,
    UnverifiedClaim,
    InvalidProvenance,
    MaterialConflict,
    StaleQuote,
    MissingQuote,
    StaleAccount,
    MissingAccount,
    MarketClosed,
    FactorLimit,
    PairExposureLimit,
    TurnoverLimit,
    PlanHashMismatch,
    DuplicateCommitment,
    NonPaperEndpoint,
    NonCanonicalRun,
    RecoveryIncomplete,
    ExternalPosition,
    UnmanagedOpenOrder,
    StaleDecision,
    HorizonConflict,
    UnqualifiedRuntime,
    MandateViolation,
    CapacityLimit,
    ComplianceViolation,
    DependencyDegraded,
    InvalidQuote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoftWarning {
    LowConfidence,
    IncompleteEvidence,
    ElevatedTurnover,
    SlowModelResponse,
    StaleNoncriticalEvidence,
    CorrelatedConsensus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionValidity {
    pub evidence_cutoff: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub maximum_execution_delay_ms: u64,
    pub market_state_hash: ContentHash,
}

impl DecisionValidity {
    // 校验证据 cutoff、生成/失效顺序，以及有效窗口不超过最大执行延迟。
    pub fn validate(&self) -> Result<(), DomainError> {
        let allowed = i64::try_from(self.maximum_execution_delay_ms).map_err(|_| {
            DomainError::InvalidBudget {
                field: "decision_validity.maximum_execution_delay_ms",
            }
        })?;
        if self.evidence_cutoff > self.generated_at
            || self.generated_at >= self.valid_until
            || self
                .valid_until
                .signed_duration_since(self.generated_at)
                .num_milliseconds()
                > allowed
        {
            return Err(DomainError::InvalidBudget {
                field: "decision_validity.window",
            });
        }
        Ok(())
    }

    // 判断当前时刻同时位于 valid window 内且没有超过毫秒级执行延迟。
    pub fn is_valid_at(&self, now: DateTime<Utc>) -> bool {
        now >= self.generated_at
            && now <= self.valid_until
            && now
                .signed_duration_since(self.generated_at)
                .num_milliseconds()
                <= i64::try_from(self.maximum_execution_delay_ms).unwrap_or(i64::MAX)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionStageLatencies {
    /// Acquisition adapters do not persist a bounded start time yet.
    pub retrieval_latency_millis: Option<u64>,
    /// Complete provider latency for claim producers and the synthesizer.
    pub model_latency_millis: Option<u64>,
    /// Complete provider latency for critique producers.
    pub critic_latency_millis: Option<u64>,
    /// Wall-clock latency measured inside this DecisionGate invocation.
    pub decision_gate_latency_millis: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastThesis {
    pub thesis_valid_until: DateTime<Utc>,
    pub expected_holding_period_days: u8,
    pub exit_condition: String,
    pub invalidation_conditions: Vec<String>,
}

impl ForecastThesis {
    // 约束持有天数必须匹配 horizon，并要求退出条件和失效条件非空。
    pub fn validate(&self, horizon: DecisionHorizon) -> Result<(), DomainError> {
        if self.expected_holding_period_days != horizon.trading_days()
            || self.exit_condition.trim().is_empty()
            || self.invalidation_conditions.is_empty()
            || self
                .invalidation_conditions
                .iter()
                .any(|condition| condition.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "forecast.thesis",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HorizonSignalDirection {
    Bearish,
    Neutral,
    Bullish,
    Uncalibrated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HorizonSleeveAttribution {
    pub asset: Asset,
    pub horizon: DecisionHorizon,
    pub policy_weight_ppm: u32,
    pub direction: HorizonSignalDirection,
    pub calibrated_probability_ppm: Option<u32>,
    pub calibrated_expected_alpha_ppm: Option<i64>,
    pub thesis: ForecastThesis,
    pub included_in_target: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HorizonConflict {
    pub asset: Asset,
    pub shorter_horizon: DecisionHorizon,
    pub longer_horizon: DecisionHorizon,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HorizonDecisionTrace {
    pub sleeves: Vec<HorizonSleeveAttribution>,
    pub conflicts: Vec<HorizonConflict>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessQualityAssessment {
    pub event_grounding_ppm: Option<u32>,
    pub premise_support_ppm: Option<u32>,
    pub temporal_validity_ppm: Option<u32>,
    pub logic_validity_ppm: Option<u32>,
    pub mandate_consistency_ppm: Option<u32>,
    pub portfolio_consistency_ppm: Option<u32>,
    pub action_feasibility_ppm: Option<u32>,
}

impl ProcessQualityAssessment {
    // 检查所有已提供的 ppm 质量指标都不超过 100%。
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.event_grounding_ppm,
            self.premise_support_ppm,
            self.temporal_validity_ppm,
            self.logic_validity_ppm,
            self.mandate_consistency_ppm,
            self.portfolio_consistency_ppm,
            self.action_feasibility_ppm,
        ]
        .into_iter()
        .flatten()
        .any(|value| value > WeightPpm::SCALE)
        {
            return Err(DomainError::InvalidBudget {
                field: "investment_logic.quality",
            });
        }
        Ok(())
    }

    // 只有七个维度都已测量时才返回完整质量指标中的最低值。
    pub fn measured_floor(&self) -> Option<u32> {
        let metrics = [
            self.event_grounding_ppm,
            self.premise_support_ppm,
            self.temporal_validity_ppm,
            self.logic_validity_ppm,
            self.mandate_consistency_ppm,
            self.portfolio_consistency_ppm,
            self.action_feasibility_ppm,
        ];
        if metrics.iter().any(Option::is_none) {
            return None;
        }
        metrics.into_iter().flatten().min()
    }

    /// Minimum score across the evidence and reasoning dimensions available
    /// when the DecisionGate seals a decision. Execution-owned dimensions are
    /// intentionally excluded so process policy cannot create a circular
    /// dependency on a plan that has not been built yet.
    // 只聚合研究侧四个维度，避免在执行计划尚未产生时引入执行指标循环依赖。
    pub fn research_measured_floor(&self) -> Option<u32> {
        let metrics = [
            self.event_grounding_ppm,
            self.premise_support_ppm,
            self.temporal_validity_ppm,
            self.logic_validity_ppm,
        ];
        if metrics.iter().any(Option::is_none) {
            return None;
        }
        metrics.into_iter().flatten().min()
    }

    /// Finalize the assessment with values produced by the deterministic
    /// execution gate. Missing research measurements remain missing; callers
    /// must not manufacture an execution-final score from a partial trace.
    // 研究指标齐全后注入执行 Gate 的三个维度，并重新校验后返回 finalized 副本。
    pub fn execution_finalized(
        &self,
        mandate_consistency_ppm: u32,
        portfolio_consistency_ppm: u32,
        action_feasibility_ppm: u32,
    ) -> Option<Self> {
        self.research_measured_floor()?;
        let finalized = Self {
            event_grounding_ppm: self.event_grounding_ppm,
            premise_support_ppm: self.premise_support_ppm,
            temporal_validity_ppm: self.temporal_validity_ppm,
            logic_validity_ppm: self.logic_validity_ppm,
            mandate_consistency_ppm: Some(mandate_consistency_ppm),
            portfolio_consistency_ppm: Some(portfolio_consistency_ppm),
            action_feasibility_ppm: Some(action_feasibility_ppm),
        };
        finalized.validate().ok()?;
        Some(finalized)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvestmentLogicTrace {
    pub observed_events: Vec<ArtifactRef>,
    pub accepted_premises: Vec<ArtifactRef>,
    pub rejected_premises: Vec<ArtifactRef>,
    pub alternatives_considered: Vec<ArtifactRef>,
    pub selected_action_hash: ContentHash,
    pub invalidation_conditions: Vec<String>,
    pub quality: ProcessQualityAssessment,
    #[serde(default)]
    pub consensus_diversity: Option<crate::ConsensusDiversityAssessment>,
    #[serde(default)]
    pub stage_latencies: DecisionStageLatencies,
}

impl InvestmentLogicTrace {
    // 校验事件/Claim/Critique 引用 kind、失效条件和质量/共识子结构。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.observed_events.iter().any(|reference| {
            !matches!(
                reference.kind,
                ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
            )
        }) || self
            .accepted_premises
            .iter()
            .chain(self.rejected_premises.iter())
            .any(|reference| reference.kind != ArtifactKind::Claim)
            || self
                .alternatives_considered
                .iter()
                .any(|reference| reference.kind != ArtifactKind::Critique)
            || self.invalidation_conditions.is_empty()
            || self
                .invalidation_conditions
                .iter()
                .any(|condition| condition.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "investment_logic.trace",
            });
        }
        self.quality.validate()?;
        if let Some(consensus) = &self.consensus_diversity {
            consensus.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterialConflict {
    pub claim: ArtifactRef,
    pub critique: ArtifactRef,
    pub topic: String,
    pub rationale: String,
}

impl MaterialConflict {
    // 冲突必须绑定 Claim 与 Critique，并提供非空主题和理由。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.claim.kind != ArtifactKind::Claim || self.critique.kind != ArtifactKind::Critique {
            return Err(DomainError::EmptyField {
                field: "material_conflict.references",
            });
        }
        if self.topic.trim().is_empty() || self.rationale.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "material_conflict.description",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionContext {
    pub schema_version: u32,
    pub decision_id: DecisionId,
    pub run_id: RunId,
    pub claims: Vec<ArtifactRef>,
    pub critiques: Vec<ArtifactRef>,
    pub evidence: Vec<ArtifactRef>,
    pub policy_influences: Vec<ArtifactRef>,
    #[serde(default)]
    pub applied_learning_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub rejected_learning_refs: Vec<ArtifactRef>,
    pub material_conflicts: Vec<MaterialConflict>,
    pub hard_blockers: Vec<HardBlocker>,
    pub soft_warnings: Vec<SoftWarning>,
    /// Hash of the Rust-owned policy that converted model forecasts into the
    /// target portfolio. The model never supplies this value.
    pub decision_policy_hash: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behavior_bundle_hash: Option<ContentHash>,
    #[serde(default)]
    pub portfolio_risk: PortfolioRiskAssessment,
    pub target: TargetPortfolio,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub validity: Option<DecisionValidity>,
    #[serde(default)]
    pub horizon_trace: Option<HorizonDecisionTrace>,
    #[serde(default)]
    pub investment_logic: Option<InvestmentLogicTrace>,
    /// Per-asset eligibility diagnostics.  This is deliberately additive so
    /// older DecisionContext blobs remain readable while new decisions expose
    /// the independent evidence, claim, calibration, and risk checks.
    #[serde(default)]
    pub asset_eligibility: BTreeMap<Asset, AssetEligibility>,
    #[serde(default)]
    pub runtime_trace: Option<DecisionEvaluationTrace>,
    /// The model's research composition and Rust's research-layer review. This
    /// is intentionally separate from `target`, which is the execution-side
    /// portfolio and may remain zero or unassessed.
    #[serde(default)]
    pub research_plan: Option<ResearchPlanReview>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionEligibilityReason {
    MissingEvidence,
    UnverifiedClaim,
    MissingCalibration,
    InsufficientCalibrationSamples,
    RiskUnknown,
    RiskRejected,
    ConfidenceTooLow,
    HorizonConflict,
    NoDirectionalSignal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetEligibility {
    pub directional_evidence: bool,
    pub claim_verified: bool,
    pub calibration: bool,
    pub risk: bool,
    pub eligible: bool,
    #[serde(default)]
    pub reasons: Vec<DecisionEligibilityReason>,
}

impl AssetEligibility {
    // 要求 reasons 已排序去重；eligible 只能在四项资格布尔值全为 true 时成立。
    pub fn validate(&self) -> Result<(), DomainError> {
        let mut reasons = self.reasons.clone();
        reasons.sort();
        reasons.dedup();
        if reasons != self.reasons
            || (self.eligible && !self.reasons.is_empty())
            || (self.eligible
                && !(self.directional_evidence
                    && self.claim_verified
                    && self.calibration
                    && self.risk))
        {
            return Err(DomainError::InvalidBudget {
                field: "decision_context.asset_eligibility",
            });
        }
        Ok(())
    }
}

/// Rust-owned rule evaluation captured at the point a DecisionGate predicate
/// runs. This is an audit trace, not a second decision implementation and it
/// contains no model private reasoning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionRuleEvaluation {
    pub rule_id: String,
    pub rule_version: String,
    pub source_location: String,
    pub inputs: Value,
    pub operator: Option<String>,
    pub threshold: Option<Value>,
    pub evaluated: bool,
    pub result: String,
    pub reason_code: String,
    pub explanation: String,
    #[serde(default)]
    pub asset: Option<Asset>,
    #[serde(default)]
    pub horizon: Option<DecisionHorizon>,
    #[serde(default)]
    pub before: Option<Value>,
    #[serde(default)]
    pub after: Option<Value>,
    pub short_circuited: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DecisionEvaluationTrace {
    pub version: u32,
    pub first_zeroing_branch: Option<String>,
    #[serde(default)]
    pub asset_first_exclusion: BTreeMap<Asset, String>,
    pub rules: Vec<DecisionRuleEvaluation>,
}

/// Deterministic ex-ante risk certificate emitted with every DecisionContext.
/// An absent metric is explicitly unmeasured and cannot support an accepted,
/// non-zero target.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskAssessment {
    #[serde(default)]
    pub risk_model_hash: Option<ContentHash>,
    pub calibrated_assets: u8,
    pub covariance_sample_count: u32,
    #[serde(default)]
    pub ex_ante_volatility_ppm: Option<u32>,
    #[serde(default)]
    pub beta_ppm: Option<u32>,
    #[serde(default)]
    pub expected_shortfall_ppm: Option<u32>,
    #[serde(default)]
    pub gap_loss_ppm: Option<u32>,
    #[serde(default)]
    pub liquidity_binding_assets: Vec<Asset>,
    pub leveraged_holding_limit_days: u8,
}

impl PortfolioRiskAssessment {
    // 检查风险指标上限、可校准资产数、杠杆持有天数和流动性绑定资产去重。
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.ex_ante_volatility_ppm.unwrap_or_default(),
            self.beta_ppm.unwrap_or_default(),
            self.expected_shortfall_ppm.unwrap_or_default(),
            self.gap_loss_ppm.unwrap_or_default(),
        ]
        .into_iter()
        .any(|value| value > 3 * WeightPpm::SCALE)
            || self.calibrated_assets as usize > Asset::EXECUTABLE.len()
            || self.leveraged_holding_limit_days > 5
        {
            return Err(DomainError::InvalidBudget {
                field: "portfolio_risk_assessment",
            });
        }
        let mut bindings = self.liquidity_binding_assets.clone();
        bindings.sort();
        bindings.dedup();
        if bindings != self.liquidity_binding_assets {
            return Err(DomainError::InvalidBudget {
                field: "portfolio_risk_assessment.liquidity_binding_assets",
            });
        }
        Ok(())
    }
}

impl DecisionContext {
    // 没有硬 blocker 且没有 material conflict 才是接受状态；不等同于已执行。
    pub fn accepted(&self) -> bool {
        self.hard_blockers.is_empty() && self.material_conflicts.is_empty()
    }

    // 校验所有血缘 kind、学习引用互斥、目标 universe、风险/validity/trace 和非零目标风险证明。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "decision_context.schema_version",
            });
        }
        if self.decision_id.0.trim().is_empty() || self.run_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_context.identity",
            });
        }
        if self
            .claims
            .iter()
            .any(|reference| reference.kind != ArtifactKind::Claim)
            || self
                .critiques
                .iter()
                .any(|reference| reference.kind != ArtifactKind::Critique)
            || self.evidence.iter().any(|reference| {
                !matches!(
                    reference.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                )
            })
            || self.policy_influences.iter().any(|reference| {
                !matches!(
                    reference.kind,
                    ArtifactKind::Experience | ArtifactKind::CandidatePolicy
                )
            })
        {
            return Err(DomainError::EmptyField {
                field: "decision_context.references",
            });
        }
        for reference in self
            .applied_learning_refs
            .iter()
            .chain(self.rejected_learning_refs.iter())
        {
            if !matches!(
                reference.kind,
                ArtifactKind::Lesson | ArtifactKind::Experience | ArtifactKind::CandidatePolicy
            ) {
                return Err(DomainError::EmptyField {
                    field: "decision_context.learning_refs",
                });
            }
        }
        if self
            .applied_learning_refs
            .iter()
            .any(|reference| self.rejected_learning_refs.contains(reference))
        {
            return Err(DomainError::EmptyField {
                field: "decision_context.learning_refs_overlap",
            });
        }
        if self.claims.is_empty() && self.hard_blockers.is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_context.claims_or_blockers",
            });
        }
        for conflict in &self.material_conflicts {
            conflict.validate()?;
        }
        self.target.validate_universe()?;
        self.portfolio_risk.validate()?;
        if let Some(validity) = &self.validity {
            validity.validate()?;
            if validity.generated_at != self.created_at {
                return Err(DomainError::InvalidBudget {
                    field: "decision_context.validity",
                });
            }
        }
        if let Some(trace) = &self.investment_logic {
            trace.validate()?;
        }
        if let Some(research_plan) = &self.research_plan {
            research_plan.validate()?;
        }
        if self
            .asset_eligibility
            .keys()
            .any(|asset| !Asset::EXECUTABLE.contains(asset))
            || self
                .asset_eligibility
                .values()
                .any(|eligibility| eligibility.validate().is_err())
        {
            return Err(DomainError::InvalidBudget {
                field: "decision_context.asset_eligibility",
            });
        }
        if self.accepted()
            && self.target.weights.values().any(|weight| weight.0 > 0)
            && (self.portfolio_risk.risk_model_hash.is_none()
                || self.portfolio_risk.ex_ante_volatility_ppm.is_none()
                || self.portfolio_risk.beta_ppm.is_none()
                || self.portfolio_risk.expected_shortfall_ppm.is_none()
                || self.portfolio_risk.gap_loss_ppm.is_none())
        {
            return Err(DomainError::InvalidBudget {
                field: "decision_context.portfolio_risk",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionHorizon {
    T1,
    T3,
    T5,
}

impl DecisionHorizon {
    pub const ALL: [Self; 3] = [Self::T1, Self::T3, Self::T5];

    // 将 T1/T3/T5 转为其对应的交易日数量。
    pub const fn trading_days(self) -> u8 {
        match self {
            Self::T1 => 1,
            Self::T3 => 3,
            Self::T5 => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Forecast {
    pub asset: Asset,
    pub horizon: DecisionHorizon,
    pub positive_return_probability_ppm: u32,
    pub expected_return_ppm: i64,
    #[serde(default)]
    pub thesis: Option<ForecastThesis>,
}

impl Forecast {
    // 校验正收益概率上限，并在 thesis 存在时复用 horizon 持有期校验。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.positive_return_probability_ppm > 1_000_000 {
            return Err(DomainError::InvalidDecisionForecastProbability);
        }
        if let Some(thesis) = &self.thesis {
            thesis.validate(self.horizon)?;
        }
        Ok(())
    }

    // 中性 forecast 的判定是期望收益为 0 且正收益概率恰为 500000 ppm。
    pub fn is_neutral(&self) -> bool {
        self.expected_return_ppm == 0 && self.positive_return_probability_ppm == 500_000
    }

    /// Bind a directional forecast to a claim about the same direction.
    /// Expected return determines direction; probability breaks only a zero-return
    /// tie, since skewed distributions can have different mean and median signs.
    // 先用 expected_return 判断方向，零收益时再用概率与 500000 的比较打破平局。
    pub fn supported_by_stance(&self, stance: crate::ClaimStance) -> bool {
        let direction = self
            .expected_return_ppm
            .cmp(&0)
            .then_with(|| self.positive_return_probability_ppm.cmp(&500_000));
        matches!(
            (direction, stance),
            (std::cmp::Ordering::Greater, crate::ClaimStance::Bullish)
                | (std::cmp::Ordering::Less, crate::ClaimStance::Bearish)
        )
    }
}

/// One asset-level research intention emitted by the synthesizer. This is a
/// target-composition statement, not an order quantity or execution permit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchAssetAllocation {
    pub asset: Asset,
    pub target_weight_ppm: WeightPpm,
    #[serde(default)]
    pub supporting_horizons: Vec<DecisionHorizon>,
    #[serde(default)]
    pub evidence_refs: Vec<ArtifactRef>,
    pub rationale: String,
    #[serde(default)]
    pub abstention_reason: Option<String>,
}

impl ResearchAssetAllocation {
    // 校验资产/权重/理由、horizon 和 evidence 引用排序去重，以及非零/零行的互斥字段。
    pub fn validate(&self) -> Result<(), DomainError> {
        if !Asset::EXECUTABLE.contains(&self.asset)
            || self.target_weight_ppm.0 > WeightPpm::SCALE
            || self.rationale.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "research_allocation.asset",
            });
        }
        let mut horizons = self.supporting_horizons.clone();
        horizons.sort();
        horizons.dedup();
        if horizons != self.supporting_horizons {
            return Err(DomainError::InvalidBudget {
                field: "research_allocation.supporting_horizons",
            });
        }
        let mut evidence_refs = self.evidence_refs.clone();
        evidence_refs.sort();
        evidence_refs.dedup();
        if evidence_refs != self.evidence_refs
            || self.evidence_refs.iter().any(|reference| {
                !matches!(
                    reference.kind,
                    ArtifactKind::Claim
                        | ArtifactKind::Critique
                        | ArtifactKind::NormalizedEvidence
                        | ArtifactKind::SemanticDetail
                )
            })
        {
            return Err(DomainError::EmptyField {
                field: "research_allocation.evidence_refs",
            });
        }
        if self.target_weight_ppm.0 > 0 {
            if self.supporting_horizons.is_empty() || self.evidence_refs.is_empty() {
                return Err(DomainError::EmptyField {
                    field: "research_allocation.non_zero_support",
                });
            }
            if self.abstention_reason.is_some() {
                return Err(DomainError::EmptyField {
                    field: "research_allocation.non_zero_abstention",
                });
            }
        } else if self
            .abstention_reason
            .as_deref()
            .is_none_or(|reason| reason.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "research_allocation.zero_reason",
            });
        }
        Ok(())
    }
}

/// Complete model-proposed research composition. Cash is explicit so the wire
/// contract cannot confuse an unallocated residual with broker cash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchAllocationPlan {
    pub cash_weight_ppm: WeightPpm,
    pub allocations: Vec<ResearchAssetAllocation>,
}

impl ResearchAllocationPlan {
    // 校验四资产恰好一次、每行合法，并确保资产权重加现金严格等于 1_000_000 ppm。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.cash_weight_ppm.0 > WeightPpm::SCALE
            || self.allocations.len() != Asset::EXECUTABLE.len()
        {
            return Err(DomainError::InvalidTargetUniverse);
        }
        let mut assets = BTreeSet::new();
        let mut gross = 0_u32;
        for allocation in &self.allocations {
            allocation.validate()?;
            if !assets.insert(allocation.asset) {
                return Err(DomainError::InvalidTargetUniverse);
            }
            gross = gross.checked_add(allocation.target_weight_ppm.0).ok_or(
                DomainError::InvalidBudget {
                    field: "research_allocation.gross_weight",
                },
            )?;
        }
        if assets.len() != Asset::EXECUTABLE.len()
            || !Asset::EXECUTABLE
                .into_iter()
                .all(|asset| assets.contains(&asset))
            || gross.checked_add(self.cash_weight_ppm.0) != Some(WeightPpm::SCALE)
        {
            return Err(DomainError::InvalidBudget {
                field: "research_allocation.weights",
            });
        }
        Ok(())
    }

    // 查询指定资产的研究权重；找不到时返回零而不修改计划。
    pub fn weight(&self, asset: Asset) -> WeightPpm {
        self.allocations
            .iter()
            .find(|allocation| allocation.asset == asset)
            .map_or(WeightPpm::ZERO, |allocation| allocation.target_weight_ppm)
    }

    // 只要任一资产研究权重非零就返回 true，现金不计入该判断。
    pub fn has_non_zero_target(&self) -> bool {
        self.allocations
            .iter()
            .any(|allocation| allocation.target_weight_ppm.0 > 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchPlanStatus {
    QualifiedRecommendation,
    ExplicitCash,
    BlockedByResearch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchExecutionStatus {
    NotApplicable,
    Blocked,
    PendingExecutionGate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchPlanAdjustment {
    #[serde(default)]
    pub asset: Option<Asset>,
    pub from_weight_ppm: WeightPpm,
    pub to_weight_ppm: WeightPpm,
    pub reasons: Vec<String>,
}

impl ResearchPlanAdjustment {
    // 校验前后权重范围，并要求每次 Rust 调整都有至少一个非空原因。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.from_weight_ppm.0 > WeightPpm::SCALE
            || self.to_weight_ppm.0 > WeightPpm::SCALE
            || self.reasons.is_empty()
            || self.reasons.iter().any(|reason| reason.trim().is_empty())
        {
            return Err(DomainError::InvalidBudget {
                field: "research_allocation.adjustment",
            });
        }
        Ok(())
    }
}

/// Rust's auditable review of the model's research intention. The raw
/// proposal is retained, while `validated` is the research-layer composition
/// after evidence-scope and static weight checks. Execution readiness is a
/// separate state and is never inferred from a non-zero research target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchPlanReview {
    pub raw: ResearchAllocationPlan,
    pub validated: ResearchAllocationPlan,
    pub adjustments: Vec<ResearchPlanAdjustment>,
    pub status: ResearchPlanStatus,
    pub execution_status: ResearchExecutionStatus,
    #[serde(default)]
    pub execution_blockers: Vec<String>,
}

impl ResearchPlanReview {
    // 分别校验 raw/validated 计划和调整，再检查 status 与 validated 非零状态一致。
    pub fn validate(&self) -> Result<(), DomainError> {
        self.raw.validate()?;
        self.validated.validate()?;
        for adjustment in &self.adjustments {
            adjustment.validate()?;
        }
        if self
            .execution_blockers
            .iter()
            .any(|blocker| blocker.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "research_allocation.execution_blockers",
            });
        }
        if matches!(self.status, ResearchPlanStatus::QualifiedRecommendation)
            && !self.validated.has_non_zero_target()
        {
            return Err(DomainError::InvalidBudget {
                field: "research_allocation.status",
            });
        }
        if matches!(self.status, ResearchPlanStatus::ExplicitCash)
            && self.validated.has_non_zero_target()
        {
            return Err(DomainError::InvalidBudget {
                field: "research_allocation.status",
            });
        }
        Ok(())
    }
}

/// Schema-bounded model output. It can request a decision, but cannot embed a
/// grant, permit, endpoint, order, or free-form execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionDraft {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub numeric_basis: Vec<crate::NumericEstimateBasis>,
    pub summary: String,
    pub confidence_ppm: u32,
    pub forecasts: Vec<Forecast>,
    /// Required by new DecisionProposal contracts. Optional on the Rust type
    /// only so older fixture/test blobs remain decodable before the gate emits
    /// a structured missing-field error.
    #[serde(default)]
    pub research_allocation: Option<ResearchAllocationPlan>,
    pub claims: Vec<ArtifactRef>,
    pub critiques: Vec<ArtifactRef>,
    pub evidence: Vec<ArtifactRef>,
    pub material_conflicts: Vec<MaterialConflict>,
    pub hard_blockers: Vec<HardBlocker>,
    pub soft_warnings: Vec<SoftWarning>,
    #[serde(default)]
    pub applied_learning_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub rejected_learning_refs: Vec<ArtifactRef>,
}

/// Public vocabulary for the proposal emitted by the research synthesizer.
/// The wire shape remains `DecisionDraft`; Rust gates it before creating a
/// durable `DecisionContext`.
pub type DecisionProposal = DecisionDraft;

impl DecisionDraft {
    // 校验模型摘要、置信度、引用 kind、学习引用、冲突闭包、forecast 网格和可选研究分配。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.summary.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_draft.summary",
            });
        }
        if self.confidence_ppm > 1_000_000 {
            return Err(DomainError::InvalidDecisionConfidence);
        }
        if self
            .claims
            .iter()
            .any(|reference| reference.kind != ArtifactKind::Claim)
            || self
                .critiques
                .iter()
                .any(|reference| reference.kind != ArtifactKind::Critique)
            || self.evidence.iter().any(|reference| {
                !matches!(
                    reference.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                )
            })
        {
            return Err(DomainError::EmptyField {
                field: "decision_draft.references",
            });
        }
        if self.claims.is_empty() && self.hard_blockers.is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_draft.claims_or_blockers",
            });
        }
        for reference in self
            .applied_learning_refs
            .iter()
            .chain(self.rejected_learning_refs.iter())
        {
            if !matches!(
                reference.kind,
                ArtifactKind::Lesson | ArtifactKind::Experience | ArtifactKind::CandidatePolicy
            ) {
                return Err(DomainError::EmptyField {
                    field: "decision_draft.learning_refs",
                });
            }
        }
        if self
            .applied_learning_refs
            .iter()
            .any(|reference| self.rejected_learning_refs.contains(reference))
        {
            return Err(DomainError::EmptyField {
                field: "decision_draft.learning_refs_overlap",
            });
        }
        for conflict in &self.material_conflicts {
            conflict.validate()?;
            if !self.claims.contains(&conflict.claim)
                || !self.critiques.contains(&conflict.critique)
            {
                return Err(DomainError::EmptyField {
                    field: "decision_draft.material_conflicts",
                });
            }
        }
        // and_then 只有 forecast 网格通过后才继续校验可选 allocation，保持错误顺序稳定。
        validate_forecasts(&self.forecasts).and_then(|_| {
            self.research_allocation
                .as_ref()
                .map_or(Ok(()), ResearchAllocationPlan::validate)
        })
    }
}

/// First canonical contract with direction-bound forecasts and optional cash.
/// Older proposals must not be reinterpreted under this validation contract.
pub const DIRECTION_BOUND_RESEARCH_CONTRACT_VERSION: u32 = 47;

pub fn validate_decision_evidence_sufficiency(
    draft: &DecisionDraft,
    claims: &[ResearchClaim],
) -> Result<(), DomainError> {
    // 先识别方向 forecast 是否被 blocking gap 命中，再要求 price/macro directional ground。
    let has_gaps = claims.iter().any(|claim| {
        claim.evidence_gaps.iter().any(|gap| {
            draft.forecasts.iter().any(|forecast| {
                !forecast.is_neutral()
                    && gap.blocks_slot(forecast.asset, forecast.horizon, claim.horizon)
            })
        })
    });
    let has_incomplete_evidence = draft
        .soft_warnings
        .contains(&SoftWarning::IncompleteEvidence);

    if has_gaps && !has_incomplete_evidence {
        return Err(DomainError::InsufficientDecisionEvidence);
    }

    let has_non_neutral_forecast = draft
        .forecasts
        .iter()
        .any(|forecast| !forecast.is_neutral());
    if !has_non_neutral_forecast {
        return Ok(());
    }

    // 闭包按资产/期限收集方向 domain；News 是覆盖信号，不会替代最小 price/macro 条件。
    let covered = |asset: Asset, horizon: DecisionHorizon| {
        let domains = claims
            .iter()
            .filter(|claim| claim.horizon == horizon)
            .flat_map(|claim| claim.grounds.iter())
            .filter(|ground| {
                ground.role == EvidenceGroundRole::Directional && ground.assets.contains(&asset)
            })
            .filter_map(|ground| ground.domain)
            .collect::<std::collections::BTreeSet<_>>();
        // Price structure and macro are the minimum directional research
        // basis. News remains an explicit coverage signal and execution-side
        // risk input, but an unavailable NewsWeb adapter must not erase a
        // clearly scoped price/macro research recommendation.
        [ResearchShard::PriceMarketStructure, ResearchShard::Macro]
            .into_iter()
            .all(|domain| domains.contains(&domain))
    };

    if draft
        .forecasts
        .iter()
        .filter(|forecast| !forecast.is_neutral())
        .any(|forecast| {
            !covered(forecast.asset, forecast.horizon)
                || claims.iter().any(|claim| {
                    claim
                        .evidence_gaps
                        .iter()
                        .any(|gap| gap.blocks_slot(forecast.asset, forecast.horizon, claim.horizon))
                })
        })
    {
        return Err(DomainError::InsufficientDecisionEvidence);
    }

    Ok(())
}

/// Rust-bound decision. It carries no execution authority; the referenced
/// `DecisionContext` is the complete provenance and blocker surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub schema_version: u32,
    pub decision_context: ArtifactRef,
    pub summary: String,
    pub targets: TargetPortfolio,
    pub confidence_ppm: u32,
    pub forecasts: Vec<Forecast>,
    #[serde(default)]
    pub research_plan: Option<ResearchPlanReview>,
    pub created_at: DateTime<Utc>,
}

impl Decision {
    // 校验 DecisionContext 引用、摘要/置信度、完整 forecast/thesis 和研究计划。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "decision.schema_version",
            });
        }
        if self.decision_context.kind != ArtifactKind::DecisionContext
            || self.summary.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "decision.context_or_summary",
            });
        }
        if self.confidence_ppm > 1_000_000 {
            return Err(DomainError::InvalidDecisionConfidence);
        }
        validate_forecasts(&self.forecasts)?;
        if self
            .forecasts
            .iter()
            .any(|forecast| forecast.thesis.is_none())
        {
            return Err(DomainError::EmptyField {
                field: "decision_draft.forecast_thesis",
            });
        }
        if self.forecasts.iter().any(|forecast| {
            forecast
                .thesis
                .as_ref()
                .is_some_and(|thesis| thesis.thesis_valid_until < self.created_at)
        }) {
            return Err(DomainError::InvalidBudget {
                field: "decision.forecast_validity",
            });
        }
        self.targets.validate_universe().and_then(|_| {
            self.research_plan
                .as_ref()
                .map_or(Ok(()), ResearchPlanReview::validate)
        })
    }
}

fn validate_forecasts(forecasts: &[Forecast]) -> Result<(), DomainError> {
    // 用 (asset,horizon) 集合拒绝重复，并要求四资产覆盖三个固定 horizon 的完整网格。
    let mut coverage = std::collections::BTreeSet::new();
    for forecast in forecasts {
        forecast.validate()?;
        if !coverage.insert((forecast.asset, forecast.horizon)) {
            return Err(DomainError::InvalidDecisionForecastHorizons);
        }
    }
    if coverage.len()
        != Asset::EXECUTABLE.len()
            * [
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]
            .len()
        || Asset::EXECUTABLE.into_iter().any(|asset| {
            [
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]
            .into_iter()
            .any(|horizon| !coverage.contains(&(asset, horizon)))
        })
    {
        return Err(DomainError::InvalidDecisionForecastHorizons);
    }
    Ok(())
}

/// Research coverage policy v2: missing and unverified slots remain neutral.
/// Portfolio-level blockers are evaluated separately by the execution policy.
pub fn validate_legacy_verified_forecast_slots(
    draft: &DecisionDraft,
    claims: &[(ArtifactRef, ResearchClaim)],
    critiques: &[crate::ResearchCritique],
) -> Result<(), DomainError> {
    // 对每个非中性 slot 收集同 horizon、同 stance 且 Critique 支持的 Claim，再复用旧证据门槛。
    for forecast in draft.forecasts.iter().filter(|f| !f.is_neutral()) {
        let verified = claims
            .iter()
            .filter(|(reference, claim)| {
                claim.horizon == forecast.horizon
                    && forecast.supported_by_stance(claim.stance)
                    && critiques.iter().any(|critique| {
                        critique.target == *reference
                            && !critique.blocks_slot(
                                forecast.asset,
                                forecast.horizon,
                                claim.horizon,
                            )
                            && critique.verification_status
                                == crate::ClaimVerificationStatus::Supported
                    })
            })
            .map(|(_, claim)| claim.clone())
            .collect::<Vec<_>>();
        let mut scoped = draft.clone();
        scoped
            .forecasts
            .retain(|f| f.asset == forecast.asset && f.horizon == forecast.horizon);
        validate_decision_evidence_sufficiency(&scoped, &verified)?;
    }
    Ok(())
}

/// First contract binding each slot to one complete Claim and its own Critique.
pub const STRUCTURED_RESEARCH_CONTRACT_VERSION: u32 = 57;

/// The authoritative eligibility predicate shared by Context and both Rust gates.
/// Deliberation and additional Critic evidence never supply a missing Claim ground.
pub fn claim_slot_eligible(
    reference: &ArtifactRef,
    claim: &ResearchClaim,
    critiques: &[crate::ResearchCritique],
    asset: Asset,
    horizon: DecisionHorizon,
) -> bool {
    // 这是 Context、DecisionGate 共享的权威资格谓词：Claim 自身 ground、Critique 当前验证
    // 和 price/macro 同资产 scope 必须同时满足，Critic 不能补写缺失 Claim ground。
    if claim.horizon != horizon
        || claim.validate().is_err()
        || claim.stance == crate::ClaimStance::Neutral
        || claim
            .evidence_gaps
            .iter()
            .any(|g| g.blocks_slot(asset, horizon, claim.horizon))
    {
        return false;
    }
    critiques.iter().any(|critique| {
        critique.target == *reference
            && critique.validate().is_ok()
            && critique.verification_status == crate::ClaimVerificationStatus::Supported
            && !critique.blocks_slot(asset, horizon, claim.horizon)
            && [ResearchShard::PriceMarketStructure, ResearchShard::Macro]
                .into_iter()
                .all(|domain| {
                    claim.grounds.iter().any(|ground| {
                        ground.role == EvidenceGroundRole::Directional
                            && ground.domain == Some(domain)
                            && ground.assets.contains(&asset)
                            && critique.grounds.iter().any(|reviewed| {
                                reviewed.evidence == ground.evidence
                                    && reviewed.role == ground.role
                                    && reviewed.domain == ground.domain
                                    && reviewed.assets.contains(&asset)
                            })
                            && critique.supporting_refs.iter().any(|verified| {
                                verified.evidence == ground.evidence
                                    && verified.is_current_authoritative()
                            })
                    })
                })
    })
}

pub fn validate_verified_forecast_slots(
    draft: &DecisionDraft,
    claims: &[(ArtifactRef, ResearchClaim)],
    critiques: &[crate::ResearchCritique],
) -> Result<(), DomainError> {
    // 每个非中性 forecast 都必须找到 draft 已引用、stance 匹配且 claim_slot_eligible 的 Claim。
    for forecast in draft.forecasts.iter().filter(|f| !f.is_neutral()) {
        if !claims.iter().any(|(reference, claim)| {
            draft.claims.contains(reference)
                && forecast.supported_by_stance(claim.stance)
                && claim_slot_eligible(
                    reference,
                    claim,
                    critiques,
                    forecast.asset,
                    forecast.horizon,
                )
        }) {
            return Err(DomainError::InsufficientDecisionEvidence);
        }
    }
    Ok(())
}

pub fn validate_structured_allocation_eligibility(
    proposal: &DecisionDraft,
    claims: &[(ArtifactRef, ResearchClaim)],
    critiques: &[crate::ResearchCritique],
) -> Result<(), DomainError> {
    // 仅检查非零研究 allocation；它必须引用正收益 forecast、Bullish Claim 和对应证据。
    for allocation in proposal
        .research_allocation
        .iter()
        .flat_map(|plan| &plan.allocations)
        .filter(|row| row.target_weight_ppm.0 > 0)
    {
        let eligible = allocation.supporting_horizons.iter().any(|horizon| {
            proposal.forecasts.iter().any(|f| {
                f.asset == allocation.asset && f.horizon == *horizon && f.expected_return_ppm > 0
            }) && claims.iter().any(|(reference, claim)| {
                claim.stance == crate::ClaimStance::Bullish
                    && claim_slot_eligible(reference, claim, critiques, allocation.asset, *horizon)
                    && allocation.evidence_refs.iter().any(|cited| {
                        cited == reference
                            || claim.grounds.iter().any(|g| {
                                g.assets.contains(&allocation.asset) && g.evidence == *cited
                            })
                            || critiques.iter().enumerate().any(|(index, c)| {
                                c.target == *reference
                                    && proposal.critiques.get(index) == Some(cited)
                            })
                    })
            })
        });
        if !eligible {
            return Err(DomainError::InsufficientDecisionEvidence);
        }
    }
    Ok(())
}

/// Research coverage is independent of a completed four-asset price window.
/// All 12 slots need authoritative, non-blocking verification and the three
/// required directional evidence domains before a run can support learning.
pub fn research_coverage_is_complete(
    claims: &[(ArtifactRef, ResearchClaim)],
    critiques: &[crate::ResearchCritique],
) -> bool {
    // 逐四资产×三 horizon 检查无 blocking gap、Supported Critique 和 price/macro/news 三域。
    use crate::ClaimVerificationStatus;
    Asset::EXECUTABLE.into_iter().all(|asset| {
        DecisionHorizon::ALL.into_iter().all(|horizon| {
            if claims.iter().any(|(_, c)| {
                c.evidence_gaps
                    .iter()
                    .any(|g| g.blocks_slot(asset, horizon, c.horizon))
            }) {
                return false;
            }
            let domains = claims
                .iter()
                .filter(|(reference, claim)| {
                    claim.horizon == horizon
                        && critiques.iter().any(|v| {
                            &v.target == reference
                                && v.verification_status == ClaimVerificationStatus::Supported
                                && !v.blocks_slot(asset, horizon, claim.horizon)
                                && v.validate().is_ok()
                        })
                })
                .flat_map(|(_, claim)| &claim.grounds)
                .filter(|g| g.role == EvidenceGroundRole::Directional && g.assets.contains(&asset))
                .filter_map(|g| g.domain)
                .collect::<std::collections::BTreeSet<_>>();
            [
                ResearchShard::PriceMarketStructure,
                ResearchShard::Macro,
                ResearchShard::NewsEvent,
            ]
            .into_iter()
            .all(|d| domains.contains(&d))
        })
    })
}

#[cfg(test)]
mod research_allocation_tests {
    use super::*;
    use crate::ArtifactId;

    fn evidence_ref() -> ArtifactRef {
        // 生成稳定的 NormalizedEvidence 引用，供最小 allocation fixture 使用。
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"research-allocation-evidence")),
            kind: ArtifactKind::NormalizedEvidence,
        }
    }

    fn zero(asset: Asset) -> ResearchAssetAllocation {
        // 构造带显式 abstention_reason 的零权重资产行。
        ResearchAssetAllocation {
            asset,
            target_weight_ppm: WeightPpm::ZERO,
            supporting_horizons: Vec::new(),
            evidence_refs: Vec::new(),
            rationale: format!("No qualified research basis for {asset:?}."),
            abstention_reason: Some("explicit research abstention".to_owned()),
        }
    }

    #[test]
    // 研究分配必须显式给出现金，且每个零权重资产都要说明 abstention。
    fn research_allocation_requires_explicit_cash_and_zero_reason() {
        let plan = ResearchAllocationPlan {
            cash_weight_ppm: WeightPpm(900_000),
            allocations: vec![
                ResearchAssetAllocation {
                    asset: Asset::Tqqq,
                    target_weight_ppm: WeightPpm(100_000),
                    supporting_horizons: vec![DecisionHorizon::T1],
                    evidence_refs: vec![evidence_ref()],
                    rationale: "Price and macro support a bounded T1 research target.".to_owned(),
                    abstention_reason: None,
                },
                zero(Asset::Qqq),
                zero(Asset::Soxx),
                zero(Asset::Soxl),
            ],
        };
        assert!(plan.validate().is_ok());

        let mut no_reason = plan.clone();
        no_reason.allocations[1].abstention_reason = None;
        assert!(no_reason.validate().is_err());

        let mut wrong_cash = plan;
        wrong_cash.cash_weight_ppm = WeightPpm(899_999);
        assert!(wrong_cash.validate().is_err());
    }
}

#[cfg(test)]
mod scoped_blocker_tests {
    use super::*;
    use crate::{
        ArtifactId, ClaimStance, ClaimVerificationEvidence, ClaimVerificationStatus,
        CritiqueSeverity, EvidenceGap, EvidenceGapImpact, EvidenceGround, ResearchCritique,
        SourceAuthority, TemporalValidity,
    };
    use std::collections::BTreeSet;

    fn evidence_ref(label: &str) -> ArtifactRef {
        // 以 label 派生确定性 evidence 引用，便于测试跨资产错引。
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(label.as_bytes())),
            kind: ArtifactKind::NormalizedEvidence,
        }
    }

    fn directional_ground(label: &str, asset: Asset, domain: ResearchShard) -> EvidenceGround {
        // 构造指定资产/研究域的方向性 ground。
        EvidenceGround {
            evidence: evidence_ref(label),
            support: format!("authoritative support for {asset:?} {domain:?}"),
            role: EvidenceGroundRole::Directional,
            assets: BTreeSet::from([asset]),
            domain: Some(domain),
        }
    }

    fn scoped_fixture() -> (DecisionDraft, ResearchClaim, ResearchCritique) {
        // 建立一个 T1 双资产 fixture，其中 TQQQ 的方向缺口被明确限定，QQQ 保持可验证。
        let claim_ref = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"claim")),
            kind: ArtifactKind::Claim,
        };
        let mut grounds = Vec::new();
        for asset in [Asset::Tqqq, Asset::Qqq] {
            grounds.push(directional_ground(
                &format!("{asset:?}-price"),
                asset,
                ResearchShard::PriceMarketStructure,
            ));
            grounds.push(directional_ground(
                &format!("{asset:?}-macro"),
                asset,
                ResearchShard::Macro,
            ));
            grounds.push(directional_ground(
                &format!("{asset:?}-news"),
                asset,
                ResearchShard::NewsEvent,
            ));
        }
        let claim = ResearchClaim {
            schema_version: DOMAIN_SCHEMA_VERSION,
            topic: "T1 multi-asset claim".to_owned(),
            statement: "Both assets have independently grounded T1 evidence".to_owned(),
            horizon: DecisionHorizon::T1,
            stance: ClaimStance::Bullish,
            materiality_ppm: 600_000,
            confidence_ppm: 800_000,
            grounds: grounds.clone(),
            evidence_gaps: vec![EvidenceGap {
                supplemental_requests: Vec::new(),
                topic: "TQQQ news unavailable".to_owned(),
                rationale: "Only TQQQ lacks its T1 news domain".to_owned(),
                impact: EvidenceGapImpact::BlocksDirectionalForecast,
                assets: BTreeSet::from([Asset::Tqqq]),
                horizons: BTreeSet::from([DecisionHorizon::T1]),
                supplemental_needs: Vec::new(),
                retriable: false,
            }],
        };
        let critique = ResearchCritique {
            schema_version: DOMAIN_SCHEMA_VERSION,
            target: claim_ref.clone(),
            topic: "T1 multi-asset review".to_owned(),
            severity: CritiqueSeverity::Low,
            blocker: true,
            rationale: "The blocker is scoped to TQQQ only".to_owned(),
            grounds: grounds.clone(),
            evidence_gaps: claim.evidence_gaps.clone(),
            verification_status: ClaimVerificationStatus::Supported,
            supporting_refs: grounds
                .iter()
                .map(|ground| ClaimVerificationEvidence {
                    evidence: ground.evidence.clone(),
                    authority: SourceAuthority::Official,
                    temporal_validity: TemporalValidity::ValidAtDecisionCutoff,
                })
                .collect(),
            conflicting_refs: Vec::new(),
        };
        let forecast = Forecast {
            asset: Asset::Qqq,
            horizon: DecisionHorizon::T1,
            positive_return_probability_ppm: 700_000,
            expected_return_ppm: 10_000,
            thesis: Some(ForecastThesis {
                thesis_valid_until: Utc::now() + chrono::Duration::days(1),
                expected_holding_period_days: DecisionHorizon::T1.trading_days(),
                exit_condition: "test exit".to_owned(),
                invalidation_conditions: vec!["test invalidation".to_owned()],
            }),
        };
        let draft = DecisionDraft {
            numeric_basis: Vec::new(),
            summary: "scoped blocker regression".to_owned(),
            confidence_ppm: 800_000,
            forecasts: vec![forecast],
            research_allocation: None,
            claims: vec![claim_ref],
            critiques: vec![ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"critique")),
                kind: ArtifactKind::Critique,
            }],
            evidence: grounds.into_iter().map(|ground| ground.evidence).collect(),
            material_conflicts: Vec::new(),
            hard_blockers: Vec::new(),
            soft_warnings: Vec::new(),
            applied_learning_refs: Vec::new(),
            rejected_learning_refs: Vec::new(),
        };
        (draft, claim, critique)
    }

    #[test]
    // 非零 allocation 必须引用同资产的合格 Claim，不能用其他资产 ground 代替。
    fn nonzero_allocation_must_cite_its_eligible_asset_claim() {
        let (mut draft, claim, critique) = scoped_fixture();
        draft.research_allocation = Some(ResearchAllocationPlan {
            allocations: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| ResearchAssetAllocation {
                    asset,
                    target_weight_ppm: WeightPpm(if asset == Asset::Qqq { 100_000 } else { 0 }),
                    supporting_horizons: if asset == Asset::Qqq {
                        vec![DecisionHorizon::T1]
                    } else {
                        vec![]
                    },
                    evidence_refs: if asset == Asset::Qqq {
                        vec![draft.claims[0].clone()]
                    } else {
                        vec![]
                    },
                    rationale: "scoped allocation regression".into(),
                    abstention_reason: (asset != Asset::Qqq).then(|| "no allocation".into()),
                })
                .collect(),
            cash_weight_ppm: WeightPpm(900_000),
        });
        let claims = [(draft.claims[0].clone(), claim.clone())];
        assert!(validate_structured_allocation_eligibility(
            &draft,
            &claims,
            std::slice::from_ref(&critique)
        )
        .is_ok());
        let unrelated = claim
            .grounds
            .iter()
            .find(|g| g.assets.contains(&Asset::Tqqq))
            .unwrap()
            .evidence
            .clone();
        draft
            .research_allocation
            .as_mut()
            .unwrap()
            .allocations
            .iter_mut()
            .find(|a| a.asset == Asset::Qqq)
            .unwrap()
            .evidence_refs = vec![unrelated];
        assert!(validate_structured_allocation_eligibility(&draft, &claims, &[critique]).is_err());
    }

    #[test]
    // 无关 Claim 的未解决 gap 不应污染另一个已完整验证的 slot。
    fn unrelated_claim_gap_cannot_poison_a_verified_slot() {
        let (draft, claim, critique) = scoped_fixture();
        let mut unrelated = claim.clone();
        unrelated
            .grounds
            .retain(|g| g.assets.contains(&Asset::Tqqq));
        unrelated.evidence_gaps[0].assets.clear();
        let unrelated_ref = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"unrelated")),
            kind: ArtifactKind::Claim,
        };
        assert!(validate_verified_forecast_slots(
            &draft,
            &[(draft.claims[0].clone(), claim), (unrelated_ref, unrelated)],
            &[critique]
        )
        .is_ok());
    }

    #[test]
    // 新闻缺失作为 warning 时，仍保留已验证的价格/宏观方向资格。
    fn unavailable_news_warning_preserves_verified_price_and_macro_direction() {
        let (draft, mut claim, mut critique) = scoped_fixture();
        claim
            .grounds
            .retain(|g| g.domain != Some(ResearchShard::NewsEvent));
        claim.evidence_gaps[0].impact = EvidenceGapImpact::Warning;
        claim.evidence_gaps[0].assets = BTreeSet::from(Asset::EXECUTABLE);
        critique.grounds = claim.grounds.clone();
        critique.evidence_gaps = claim.evidence_gaps.clone();
        critique.blocker = false;
        critique
            .supporting_refs
            .retain(|r| claim.grounds.iter().any(|g| g.evidence == r.evidence));
        assert!(claim_slot_eligible(
            &draft.claims[0],
            &claim,
            std::slice::from_ref(&critique),
            Asset::Qqq,
            DecisionHorizon::T1
        ));
        validate_verified_forecast_slots(&draft, &[(draft.claims[0].clone(), claim)], &[critique])
            .unwrap();
    }

    #[test]
    // 资格只接受同资产/期限、正式 ground 和当前 Critique 验证，不接受跨 Claim 拼接。
    fn eligibility_uses_formal_grounds_and_matching_verified_scope_only() {
        let (draft, mut claim, mut critique) = scoped_fixture();
        let reference = &draft.claims[0];
        assert!(claim_slot_eligible(
            reference,
            &claim,
            &[critique.clone()],
            Asset::Qqq,
            DecisionHorizon::T1
        ));
        assert!(!claim_slot_eligible(
            reference,
            &claim,
            &[critique.clone()],
            Asset::Qqq,
            DecisionHorizon::T3
        ));
        assert!(!claim_slot_eligible(
            reference,
            &claim,
            &[critique.clone()],
            Asset::Soxl,
            DecisionHorizon::T1
        ));
        claim
            .grounds
            .retain(|g| g.domain != Some(ResearchShard::Macro));
        assert!(
            !claim_slot_eligible(
                reference,
                &claim,
                &[critique.clone()],
                Asset::Qqq,
                DecisionHorizon::T1
            ),
            "Critic macro cannot repair missing formal Claim macro"
        );
        let (_, claim, _) = scoped_fixture();
        critique.supporting_refs.retain(|r| {
            !claim
                .grounds
                .iter()
                .any(|g| g.evidence == r.evidence && g.domain == Some(ResearchShard::Macro))
        });
        assert!(
            !claim_slot_eligible(
                reference,
                &claim,
                &[critique],
                Asset::Qqq,
                DecisionHorizon::T1
            ),
            "price verification alone cannot verify macro"
        );
        let mut neutral = draft.clone();
        neutral.forecasts[0].expected_return_ppm = 0;
        neutral.forecasts[0].positive_return_probability_ppm = 500_000;
        assert!(validate_verified_forecast_slots(&neutral, &[], &[]).is_ok());
    }

    #[test]
    // 长度为 12 仍不能掩盖重复 asset/horizon，完整笛卡尔网格才合法。
    fn forecast_grid_rejects_duplicates_even_when_length_is_twelve() {
        let (draft, _, _) = scoped_fixture();
        let forecasts = Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                let template = draft.forecasts[0].clone();
                DecisionHorizon::ALL.into_iter().map(move |horizon| {
                    let mut forecast = template.clone();
                    forecast.asset = asset;
                    forecast.horizon = horizon;
                    forecast
                        .thesis
                        .as_mut()
                        .unwrap()
                        .expected_holding_period_days = horizon.trading_days();
                    forecast
                })
            })
            .collect::<Vec<_>>();
        assert!(validate_forecasts(&forecasts).is_ok());
        let mut duplicate = forecasts.clone();
        duplicate[11] = forecasts[0].clone();
        assert!(validate_forecasts(&duplicate).is_err());
        assert!(validate_forecasts(&forecasts[..11]).is_err());
    }

    #[test]
    // 一个 forecast 的 price 与 macro ground 必须来自同一个 Claim，不能跨 Claim 合并。
    fn forecast_must_not_join_price_and_macro_from_different_claims() {
        let (draft, mut price, mut critique) = scoped_fixture();
        price.evidence_gaps.clear();
        price
            .grounds
            .retain(|g| g.domain == Some(ResearchShard::PriceMarketStructure));
        critique.blocker = false;
        critique.evidence_gaps.clear();
        let mut macro_claim = price.clone();
        macro_claim.grounds = vec![directional_ground(
            "macro-only",
            Asset::Qqq,
            ResearchShard::Macro,
        )];
        let macro_ref = ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"macro-claim")),
            kind: ArtifactKind::Claim,
        };
        let mut macro_critique = critique.clone();
        macro_critique.target = macro_ref.clone();
        assert!(
            validate_verified_forecast_slots(
                &draft,
                &[(draft.claims[0].clone(), price), (macro_ref, macro_claim)],
                &[critique, macro_critique],
            )
            .is_err(),
            "a forecast must have price and macro in the same verified Claim"
        );
    }

    #[test]
    // 只限定 TQQQ 的 Critique blocker 不应阻断 QQQ slot；无 scope blocker 才是全局阻断。
    fn scoped_critique_blocker_does_not_block_another_asset_slot() {
        let (draft, claim, critique) = scoped_fixture();
        assert!(critique.blocks_slot(Asset::Tqqq, DecisionHorizon::T1, DecisionHorizon::T1));
        assert!(!critique.blocks_slot(Asset::Qqq, DecisionHorizon::T1, DecisionHorizon::T1));
        let mut global_critique = critique.clone();
        global_critique.evidence_gaps.clear();
        assert!(global_critique.blocks_slot(Asset::Qqq, DecisionHorizon::T1, DecisionHorizon::T1));

        let result = validate_verified_forecast_slots(
            &draft,
            &[(draft.claims[0].clone(), claim)],
            &[critique],
        );

        assert!(
            result.is_ok(),
            "a blocker scoped to TQQQ must not reject the fully grounded QQQ slot: {result:?}"
        );
    }

    #[test]
    // Claim stance 必须与正/负 forecast 方向一致，中性 Claim 不能提供方向支持。
    fn verified_forecast_rejects_opposite_or_neutral_claim_stance() {
        let (draft, mut claim, critique) = scoped_fixture();
        for stance in [ClaimStance::Bearish, ClaimStance::Neutral] {
            claim.stance = stance;
            assert!(
                validate_verified_forecast_slots(
                    &draft,
                    &[(draft.claims[0].clone(), claim.clone())],
                    std::slice::from_ref(&critique),
                )
                .is_err(),
                "a {stance:?} claim cannot support the positive QQQ:T1 forecast"
            );
        }
        let mut bearish_draft = draft.clone();
        bearish_draft.forecasts[0].expected_return_ppm = -10_000;
        bearish_draft.forecasts[0].positive_return_probability_ppm = 300_000;
        claim.stance = ClaimStance::Bearish;
        assert!(
            validate_verified_forecast_slots(
                &bearish_draft,
                &[(draft.claims[0].clone(), claim.clone())],
                std::slice::from_ref(&critique),
            )
            .is_ok(),
            "matching bearish research remains expressible"
        );
        claim.stance = ClaimStance::Bullish;
        assert!(validate_verified_forecast_slots(
            &bearish_draft,
            &[(draft.claims[0].clone(), claim)],
            &[critique],
        )
        .is_err());
    }
}
