//! Typed decision inputs and risk findings.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
    pub fn accepted(&self) -> bool {
        self.hard_blockers.is_empty() && self.material_conflicts.is_empty()
    }

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
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.positive_return_probability_ppm > 1_000_000 {
            return Err(DomainError::InvalidDecisionForecastProbability);
        }
        if let Some(thesis) = &self.thesis {
            thesis.validate(self.horizon)?;
        }
        Ok(())
    }

    pub fn is_neutral(&self) -> bool {
        self.expected_return_ppm == 0 && self.positive_return_probability_ppm == 500_000
    }
}

/// Schema-bounded model output. It can request a decision, but cannot embed a
/// grant, permit, endpoint, order, or free-form execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionDraft {
    pub summary: String,
    pub confidence_ppm: u32,
    pub forecasts: Vec<Forecast>,
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
        validate_forecasts(&self.forecasts)
    }
}

pub fn validate_decision_evidence_sufficiency(
    draft: &DecisionDraft,
    claims: &[ResearchClaim],
) -> Result<(), DomainError> {
    let has_gaps = claims.iter().any(|claim| !claim.evidence_gaps.is_empty());
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
        [
            ResearchShard::PriceMarketStructure,
            ResearchShard::Macro,
            ResearchShard::NewsEvent,
        ]
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
    pub created_at: DateTime<Utc>,
}

impl Decision {
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
        self.targets.validate_universe()
    }
}

fn validate_forecasts(forecasts: &[Forecast]) -> Result<(), DomainError> {
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
pub fn validate_verified_forecast_slots(
    draft: &DecisionDraft,
    claims: &[(ArtifactRef, ResearchClaim)],
    critiques: &[crate::ResearchCritique],
) -> Result<(), DomainError> {
    for forecast in draft.forecasts.iter().filter(|f| !f.is_neutral()) {
        let verified = claims
            .iter()
            .filter(|(reference, claim)| {
                claim.horizon == forecast.horizon
                    && critiques.iter().any(|critique| {
                        critique.target == *reference
                            && !critique.blocker
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

/// Research coverage is independent of a completed four-asset price window.
/// All 12 slots need authoritative, non-blocking verification and the three
/// required directional evidence domains before a run can support learning.
pub fn research_coverage_is_complete(
    claims: &[(ArtifactRef, ResearchClaim)],
    critiques: &[crate::ResearchCritique],
) -> bool {
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
                                && !v.blocker
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
