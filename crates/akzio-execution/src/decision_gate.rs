//! Typed DecisionGate.
//!
//! The model produces only a schema-bounded `DecisionDraft`. Rust reloads the
//! persisted manifest closure, binds the draft to the run, and atomically
//! commits the resulting `DecisionContext` and `Decision`.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    content_hash_json, manifest_input_hash, validate_decision_evidence_sufficiency,
    AgentEvidenceContribution, Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactRef,
    Asset, CandidatePolicy, ClaimVerificationStatus, ConsensusDiversityAssessment, ContentHash,
    ContextManifestPayload, ContextTrust, Decision, DecisionContext, DecisionDraft,
    DecisionHorizon, DecisionStageLatencies, DecisionValidity, DomainError, Experience, Forecast,
    HardBlocker, HorizonConflict, HorizonDecisionTrace, HorizonSignalDirection,
    HorizonSleeveAttribution, InvestmentLogicTrace, PolicySubject, PortfolioRiskAssessment,
    ProcessQualityAssessment, ResearchClaim, ResearchCritique, RunPurpose, SoftWarning,
    TargetPortfolio, TaskStatus, TaskWritePermit, WeightPpm, DOMAIN_SCHEMA_VERSION,
};
use akzio_store::{Store, StoreError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecisionGateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("decision proposal provenance is invalid")]
    InvalidProposalProvenance,
    #[error("decision proposal claim evidence is semantically insufficient")]
    InsufficientClaimEvidence,
    #[error("claim verification {0} does not close over a selected claim")]
    InvalidClaimVerification(ArtifactId),
    #[error(
        "decision proposal producer contract is not installed or predates evidence sufficiency"
    )]
    UnsupportedProposalContract,
    #[error("decision proposal must retain exactly one ContextManifest")]
    InvalidManifestReference,
    #[error("decision ContextManifest closure is invalid")]
    InvalidManifestClosure,
    #[error("decision proposal reference {0} is outside its ContextManifest")]
    ReferenceOutsideManifest(ArtifactId),
    #[error("policy influence {0} is not eligible")]
    InvalidPolicyInfluence(ArtifactId),
    #[error("learning artifact {0} was selected but not explicitly applied or rejected")]
    MissingLearningAttribution(ArtifactId),
}

pub type DecisionGateResult<T> = std::result::Result<T, DecisionGateError>;

const CRITICAL_CLAIM_MATERIALITY_PPM: u32 = 500_000;
const MAXIMUM_CONSENSUS_SOURCE_OVERLAP_PPM: u32 = 500_000;

#[derive(Debug, Clone)]
pub struct DecisionGateInput {
    pub permit: TaskWritePermit,
    pub proposal: ArtifactRef,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DecisionGateOutput {
    pub decision_context: Artifact,
    pub decision: Artifact,
}

/// Rust-owned conversion from schema-bounded forecasts to target exposure.
///
/// The synthesizer can only supply forecasts and confidence. This policy is
/// configured by Rust, hashed into every DecisionContext, and is the sole
/// authority that creates portfolio weights.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ForecastCalibrationScope {
    pub model_id: String,
    pub model_version_hash: ContentHash,
    pub regime: String,
}

impl ForecastCalibrationScope {
    fn validate(&self) -> Result<(), DomainError> {
        if self.model_id.trim().is_empty() || self.regime.trim().is_empty() {
            return Err(DomainError::InvalidBudget {
                field: "forecast_calibration.scope",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastCalibrationBin {
    pub raw_probability_min_ppm: u32,
    pub raw_probability_max_ppm: u32,
    pub sample_count: u32,
    pub calibrated_probability_ppm: u32,
    pub calibrated_expected_alpha_ppm: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenForecastCalibration {
    pub scope: ForecastCalibrationScope,
    pub asset: Asset,
    pub horizon: DecisionHorizon,
    pub sample_count: u32,
    pub mean_brier_score_ppm: u32,
    pub fit_dataset_hash: ContentHash,
    pub trained_through: DateTime<Utc>,
    pub frozen_at: DateTime<Utc>,
    pub bins: Vec<ForecastCalibrationBin>,
}

impl FrozenForecastCalibration {
    fn validate(&self) -> Result<(), DomainError> {
        self.scope.validate()?;
        if self.sample_count == 0
            || self.mean_brier_score_ppm > WeightPpm::SCALE
            || self.trained_through > self.frozen_at
            || self.bins.is_empty()
        {
            return Err(DomainError::InvalidBudget {
                field: "forecast_calibration",
            });
        }

        let mut next_lower = 0_u32;
        let mut total_samples = 0_u64;
        for bin in &self.bins {
            if bin.raw_probability_min_ppm != next_lower
                || bin.raw_probability_max_ppm < bin.raw_probability_min_ppm
                || bin.raw_probability_max_ppm > WeightPpm::SCALE
                || bin.calibrated_probability_ppm > WeightPpm::SCALE
                || bin.calibrated_expected_alpha_ppm.unsigned_abs() > u64::from(WeightPpm::SCALE)
            {
                return Err(DomainError::InvalidBudget {
                    field: "forecast_calibration.bins",
                });
            }
            total_samples = total_samples.saturating_add(u64::from(bin.sample_count));
            next_lower = bin.raw_probability_max_ppm.saturating_add(1);
        }
        if self.bins.last().map(|bin| bin.raw_probability_max_ppm) != Some(WeightPpm::SCALE)
            || total_samples != u64::from(self.sample_count)
        {
            return Err(DomainError::InvalidBudget {
                field: "forecast_calibration.bins",
            });
        }
        Ok(())
    }

    fn calibrated(&self, raw_probability_ppm: u32) -> Option<(u32, i64, u32)> {
        self.bins
            .iter()
            .find(|bin| {
                (bin.raw_probability_min_ppm..=bin.raw_probability_max_ppm)
                    .contains(&raw_probability_ppm)
            })
            .map(|bin| {
                (
                    bin.calibrated_probability_ppm,
                    bin.calibrated_expected_alpha_ppm,
                    bin.sample_count,
                )
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetRiskCalibration {
    pub sample_count: u32,
    pub brier_score_ppm: u32,
    pub annualized_volatility_ppm: u32,
    pub beta_ppm: u32,
    pub max_capital_weight: WeightPpm,
    pub liquidity_weight_cap: WeightPpm,
    pub expected_shortfall_ppm: u32,
    pub gap_loss_ppm: u32,
    pub daily_reset_decay_ppm: u32,
}

impl AssetRiskCalibration {
    fn validate(&self) -> Result<(), DomainError> {
        if self.brier_score_ppm > WeightPpm::SCALE
            || self.annualized_volatility_ppm == 0
            || self.annualized_volatility_ppm > 3 * WeightPpm::SCALE
            || self.beta_ppm == 0
            || self.beta_ppm > 3 * WeightPpm::SCALE
            || self.max_capital_weight.0 > WeightPpm::SCALE
            || self.liquidity_weight_cap.0 > self.max_capital_weight.0
            || self.expected_shortfall_ppm == 0
            || self.expected_shortfall_ppm > 3 * WeightPpm::SCALE
            || self.gap_loss_ppm == 0
            || self.gap_loss_ppm > 3 * WeightPpm::SCALE
            || self.daily_reset_decay_ppm > WeightPpm::SCALE
        {
            return Err(DomainError::InvalidBudget {
                field: "asset_risk_calibration",
            });
        }
        Ok(())
    }
}

/// Immutable, point-in-time portfolio-risk snapshot. Covariance entries use
/// ppm-squared units and must be symmetric for every calibrated asset pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioRiskModel {
    pub version: String,
    pub sample_count: u32,
    pub covariance_ppm_squared: BTreeMap<Asset, BTreeMap<Asset, i64>>,
    pub max_expected_shortfall_ppm: u32,
    pub max_gap_loss_ppm: u32,
    pub max_leveraged_holding_days: u8,
}

impl PortfolioRiskModel {
    fn validate(
        &self,
        calibrations: &BTreeMap<Asset, AssetRiskCalibration>,
    ) -> Result<(), DomainError> {
        if calibrations.is_empty() {
            return Ok(());
        }
        if self.version.trim().is_empty()
            || self.sample_count == 0
            || self.max_expected_shortfall_ppm == 0
            || self.max_expected_shortfall_ppm > 3 * WeightPpm::SCALE
            || self.max_gap_loss_ppm == 0
            || self.max_gap_loss_ppm > 3 * WeightPpm::SCALE
            || !(1..=5).contains(&self.max_leveraged_holding_days)
        {
            return Err(DomainError::InvalidBudget {
                field: "portfolio_risk_model",
            });
        }
        for (left, left_calibration) in calibrations {
            for (right, right_calibration) in calibrations {
                let covariance = self
                    .covariance_ppm_squared
                    .get(left)
                    .and_then(|row| row.get(right))
                    .copied()
                    .ok_or(DomainError::InvalidBudget {
                        field: "portfolio_risk_model.covariance",
                    })?;
                let reverse = self
                    .covariance_ppm_squared
                    .get(right)
                    .and_then(|row| row.get(left))
                    .copied()
                    .ok_or(DomainError::InvalidBudget {
                        field: "portfolio_risk_model.covariance",
                    })?;
                let maximum = i64::from(left_calibration.annualized_volatility_ppm)
                    .saturating_mul(i64::from(right_calibration.annualized_volatility_ppm));
                if covariance != reverse
                    || covariance.unsigned_abs() > maximum as u64
                    || (left == right && covariance <= 0)
                {
                    return Err(DomainError::InvalidBudget {
                        field: "portfolio_risk_model.covariance",
                    });
                }
            }
        }
        Ok(())
    }

    fn identity_hash(&self) -> Result<akzio_domain::ContentHash, DomainError> {
        let value = serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?;
        akzio_domain::content_hash_json(&value).map_err(|_| DomainError::InvalidContentHash)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPolicy {
    pub min_confidence_ppm: u32,
    pub max_gross_weight: WeightPpm,
    pub horizon_weights: BTreeMap<DecisionHorizon, WeightPpm>,
    pub maximum_execution_delay_ms: u64,
    pub minimum_process_quality_ppm: u32,
    pub min_probability_edge_ppm: u32,
    pub min_calibration_samples: u32,
    pub max_brier_score_ppm: u32,
    pub active_forecast_calibration: Option<ForecastCalibrationScope>,
    pub forecast_calibrations: Vec<FrozenForecastCalibration>,
    pub target_annualized_volatility_ppm: u32,
    pub max_portfolio_beta_ppm: u32,
    pub asset_calibrations: BTreeMap<Asset, AssetRiskCalibration>,
    pub portfolio_risk_model: PortfolioRiskModel,
}

impl DecisionPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.min_confidence_ppm > WeightPpm::SCALE
            || self.max_gross_weight.0 > WeightPpm::SCALE
            || self.maximum_execution_delay_ms == 0
            || self.minimum_process_quality_ppm > WeightPpm::SCALE
            || self.min_probability_edge_ppm > WeightPpm::SCALE / 2
            || self.min_calibration_samples == 0
            || self.max_brier_score_ppm > WeightPpm::SCALE
            || self.target_annualized_volatility_ppm == 0
            || self.target_annualized_volatility_ppm > WeightPpm::SCALE
            || self.max_portfolio_beta_ppm == 0
            || self.max_portfolio_beta_ppm > 3 * WeightPpm::SCALE
            || self.horizon_weights.len() != 3
            || [
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]
            .into_iter()
            .any(|horizon| !self.horizon_weights.contains_key(&horizon))
            || self
                .horizon_weights
                .values()
                .any(|weight| weight.0 > WeightPpm::SCALE)
            || self
                .horizon_weights
                .values()
                .try_fold(0_u32, |sum, weight| sum.checked_add(weight.0))
                != Some(WeightPpm::SCALE)
        {
            return Err(DomainError::InvalidBudget {
                field: "decision_policy",
            });
        }
        if let Some(scope) = &self.active_forecast_calibration {
            scope.validate()?;
        }
        let mut calibration_keys = BTreeSet::new();
        for calibration in &self.forecast_calibrations {
            calibration.validate()?;
            if !calibration_keys.insert((
                calibration.scope.clone(),
                calibration.asset,
                calibration.horizon,
            )) {
                return Err(DomainError::InvalidBudget {
                    field: "forecast_calibration.duplicate",
                });
            }
        }

        self.asset_calibrations
            .values()
            .try_for_each(AssetRiskCalibration::validate)?;
        for (asset, calibration) in &self.asset_calibrations {
            if !matches!(asset, Asset::Tqqq | Asset::Soxl) && calibration.daily_reset_decay_ppm != 0
            {
                return Err(DomainError::InvalidBudget {
                    field: "asset_risk_calibration.daily_reset_decay",
                });
            }
        }
        self.portfolio_risk_model
            .validate(&self.asset_calibrations)?;
        if !self.asset_calibrations.is_empty()
            && self.portfolio_risk_model.sample_count < self.min_calibration_samples
        {
            return Err(DomainError::InvalidBudget {
                field: "portfolio_risk_model.sample_count",
            });
        }
        Ok(())
    }

    pub fn policy_hash(&self) -> Result<akzio_domain::ContentHash, DomainError> {
        self.validate()?;
        content_hash_json(&serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?)
            .map_err(|_| DomainError::InvalidContentHash)
    }

    fn calibrated_forecast(
        &self,
        decision_at: DateTime<Utc>,
        forecast: &Forecast,
    ) -> Option<(u32, i64)> {
        let scope = self.active_forecast_calibration.as_ref()?;
        let calibration = self.forecast_calibrations.iter().find(|calibration| {
            &calibration.scope == scope
                && calibration.asset == forecast.asset
                && calibration.horizon == forecast.horizon
        })?;
        if calibration.frozen_at > decision_at
            || calibration.sample_count < self.min_calibration_samples
            || calibration.mean_brier_score_ppm > self.max_brier_score_ppm
        {
            return None;
        }
        let (probability_ppm, expected_alpha_ppm, bin_samples) =
            calibration.calibrated(forecast.positive_return_probability_ppm)?;
        (bin_samples >= self.min_calibration_samples)
            .then_some((probability_ppm, expected_alpha_ppm))
    }

    pub fn target_for(
        &self,
        decision_at: DateTime<Utc>,
        confidence_ppm: u32,
        forecasts: &[Forecast],
    ) -> Result<TargetPortfolio, DomainError> {
        self.target_with_risk(decision_at, confidence_ppm, forecasts)
            .map(|(target, _)| target)
    }

    pub fn target_with_risk(
        &self,
        decision_at: DateTime<Utc>,
        confidence_ppm: u32,
        forecasts: &[Forecast],
    ) -> Result<(TargetPortfolio, PortfolioRiskAssessment), DomainError> {
        self.validate()?;
        if confidence_ppm > WeightPpm::SCALE {
            return Err(DomainError::InvalidDecisionConfidence);
        }
        if confidence_ppm < self.min_confidence_ppm {
            return Ok((
                TargetPortfolio::zeroed(),
                PortfolioRiskAssessment::default(),
            ));
        }

        let horizon_trace = self.horizon_trace(decision_at, forecasts)?;
        let conflicted_assets = horizon_trace
            .conflicts
            .iter()
            .map(|conflict| conflict.asset)
            .collect::<BTreeSet<_>>();
        let mut eligible = BTreeMap::new();
        for asset in Asset::EXECUTABLE {
            if conflicted_assets.contains(&asset) {
                continue;
            }
            let Some(calibration) = self.asset_calibrations.get(&asset) else {
                continue;
            };
            if calibration.sample_count < self.min_calibration_samples
                || calibration.brier_score_ppm > self.max_brier_score_ppm
            {
                continue;
            }
            let mut probability_edge = 0_i128;
            let mut expected_alpha = 0_i128;
            let mut included_weight_ppm = 0_i128;
            let mut saw_forecast = false;
            let mut calibration_complete = true;
            for forecast in forecasts.iter().filter(|forecast| {
                forecast.asset == asset
                    && (!is_daily_reset(asset)
                        || horizon_days(forecast.horizon)
                            <= self.portfolio_risk_model.max_leveraged_holding_days)
            }) {
                saw_forecast = true;
                let Some((calibrated_probability, calibrated_expected_alpha)) =
                    self.calibrated_forecast(decision_at, forecast)
                else {
                    calibration_complete = false;
                    break;
                };
                let weight = i128::from(self.horizon_weights[&forecast.horizon].0);
                included_weight_ppm = included_weight_ppm.saturating_add(weight);
                probability_edge = probability_edge.saturating_add(
                    (i128::from(calibrated_probability) - i128::from(WeightPpm::SCALE / 2))
                        .saturating_mul(weight)
                        / i128::from(WeightPpm::SCALE),
                );
                expected_alpha = expected_alpha.saturating_add(
                    i128::from(calibrated_expected_alpha).saturating_mul(weight)
                        / i128::from(WeightPpm::SCALE),
                );
            }
            if included_weight_ppm > 0 {
                probability_edge = probability_edge.saturating_mul(i128::from(WeightPpm::SCALE))
                    / included_weight_ppm;
                expected_alpha = expected_alpha.saturating_mul(i128::from(WeightPpm::SCALE))
                    / included_weight_ppm;
            }
            if calibration_complete
                && saw_forecast
                && probability_edge >= i128::from(self.min_probability_edge_ppm)
                && expected_alpha > i128::from(calibration.daily_reset_decay_ppm)
            {
                let signal_strength_ppm = probability_edge
                    .saturating_mul(2)
                    .saturating_add(
                        expected_alpha
                            .saturating_sub(i128::from(calibration.daily_reset_decay_ppm)),
                    )
                    .clamp(0, i128::from(WeightPpm::SCALE))
                    as u32;
                eligible.insert(asset, (calibration, signal_strength_ppm));
            }
        }
        if eligible.is_empty() {
            return Ok((
                TargetPortfolio::zeroed(),
                PortfolioRiskAssessment::default(),
            ));
        }

        let mut target = TargetPortfolio::zeroed();
        let mut liquidity_binding_assets = Vec::new();
        let volatility_budget = u64::from(self.target_annualized_volatility_ppm);
        let beta_budget = u64::from(self.max_portfolio_beta_ppm);
        for (asset, (calibration, signal_strength_ppm)) in eligible {
            let volatility_cap = volatility_budget.saturating_mul(u64::from(WeightPpm::SCALE))
                / u64::from(calibration.annualized_volatility_ppm);
            let beta_cap = beta_budget.saturating_mul(u64::from(WeightPpm::SCALE))
                / u64::from(calibration.beta_ppm);
            let unconstrained = volatility_cap
                .min(beta_cap)
                .min(u64::from(calibration.max_capital_weight.0));
            let liquidity_capped = unconstrained.min(u64::from(calibration.liquidity_weight_cap.0));
            let weight = liquidity_capped.saturating_mul(u64::from(signal_strength_ppm))
                / u64::from(WeightPpm::SCALE);
            if liquidity_capped < unconstrained {
                liquidity_binding_assets.push(asset);
            }
            target.weights.insert(asset, WeightPpm(weight as u32));
        }
        let gross = gross_weight_ppm(&target)?;
        scale_target_to_limit(&mut target, gross, self.max_gross_weight.0)?;

        let first = self.assess_target(&target, liquidity_binding_assets.clone())?;
        let scale_numerator = [
            (
                self.target_annualized_volatility_ppm,
                first.ex_ante_volatility_ppm,
            ),
            (self.max_portfolio_beta_ppm, first.beta_ppm),
            (
                self.portfolio_risk_model.max_expected_shortfall_ppm,
                first.expected_shortfall_ppm,
            ),
            (
                self.portfolio_risk_model.max_gap_loss_ppm,
                first.gap_loss_ppm,
            ),
        ]
        .into_iter()
        .filter_map(|(limit, measured)| measured.map(|measured| (limit, measured)))
        .filter(|(limit, measured)| measured > limit)
        .map(|(limit, measured)| {
            u64::from(limit).saturating_mul(u64::from(WeightPpm::SCALE)) / u64::from(measured)
        })
        .min()
        .unwrap_or(u64::from(WeightPpm::SCALE));
        if scale_numerator < u64::from(WeightPpm::SCALE) {
            for weight in target.weights.values_mut() {
                weight.0 = (u64::from(weight.0).saturating_mul(scale_numerator)
                    / u64::from(WeightPpm::SCALE)) as u32;
            }
        }
        target.validate_universe()?;
        let assessment = self.assess_target(&target, liquidity_binding_assets)?;
        Ok((target, assessment))
    }

    pub fn horizon_trace(
        &self,
        decision_at: DateTime<Utc>,
        forecasts: &[Forecast],
    ) -> Result<HorizonDecisionTrace, DomainError> {
        self.validate()?;
        let mut sleeves = Vec::with_capacity(forecasts.len());
        for forecast in forecasts {
            forecast.validate()?;
            let thesis = forecast.thesis.clone().ok_or(DomainError::EmptyField {
                field: "decision_policy.forecast_thesis",
            })?;
            let calibrated = self.calibrated_forecast(decision_at, forecast);
            let direction = if forecast.is_neutral() {
                HorizonSignalDirection::Neutral
            } else {
                calibrated.map_or(HorizonSignalDirection::Uncalibrated, |(p, _)| {
                    let midpoint = WeightPpm::SCALE / 2;
                    if p >= midpoint.saturating_add(self.min_probability_edge_ppm) {
                        HorizonSignalDirection::Bullish
                    } else if p.saturating_add(self.min_probability_edge_ppm) <= midpoint {
                        HorizonSignalDirection::Bearish
                    } else {
                        HorizonSignalDirection::Neutral
                    }
                })
            };
            sleeves.push(HorizonSleeveAttribution {
                asset: forecast.asset,
                horizon: forecast.horizon,
                policy_weight_ppm: self.horizon_weights[&forecast.horizon].0,
                direction,
                calibrated_probability_ppm: calibrated.map(|value| value.0),
                calibrated_expected_alpha_ppm: calibrated.map(|value| value.1),
                thesis,
                included_in_target: calibrated.is_some(),
            });
        }

        let mut conflicts = Vec::new();
        for asset in Asset::EXECUTABLE {
            let asset_sleeves = sleeves
                .iter()
                .filter(|sleeve| sleeve.asset == asset)
                .collect::<Vec<_>>();
            for (index, left) in asset_sleeves.iter().enumerate() {
                for right in asset_sleeves.iter().skip(index + 1) {
                    if matches!(
                        (left.direction, right.direction),
                        (
                            HorizonSignalDirection::Bullish,
                            HorizonSignalDirection::Bearish
                        ) | (
                            HorizonSignalDirection::Bearish,
                            HorizonSignalDirection::Bullish
                        )
                    ) {
                        let (shorter_horizon, longer_horizon) =
                            if left.horizon.trading_days() < right.horizon.trading_days() {
                                (left.horizon, right.horizon)
                            } else {
                                (right.horizon, left.horizon)
                            };
                        conflicts.push(HorizonConflict {
                            asset,
                            shorter_horizon,
                            longer_horizon,
                        });
                    }
                }
            }
        }
        if !conflicts.is_empty() {
            let assets = conflicts
                .iter()
                .map(|conflict| conflict.asset)
                .collect::<BTreeSet<_>>();
            for sleeve in &mut sleeves {
                if assets.contains(&sleeve.asset) {
                    sleeve.included_in_target = false;
                }
            }
        }
        Ok(HorizonDecisionTrace { sleeves, conflicts })
    }

    fn assess_target(
        &self,
        target: &TargetPortfolio,
        mut liquidity_binding_assets: Vec<Asset>,
    ) -> Result<PortfolioRiskAssessment, DomainError> {
        let mut variance = 0_i128;
        for left in Asset::EXECUTABLE {
            let left_weight = i128::from(target.weights[&left].0);
            if left_weight == 0 {
                continue;
            }
            for right in Asset::EXECUTABLE {
                let right_weight = i128::from(target.weights[&right].0);
                if right_weight == 0 {
                    continue;
                }
                let covariance = self
                    .portfolio_risk_model
                    .covariance_ppm_squared
                    .get(&left)
                    .and_then(|row| row.get(&right))
                    .copied()
                    .ok_or(DomainError::InvalidBudget {
                        field: "portfolio_risk_model.covariance",
                    })?;
                variance = variance.saturating_add(
                    left_weight
                        .saturating_mul(right_weight)
                        .saturating_mul(i128::from(covariance))
                        / 1_000_000_000_000_i128,
                );
            }
        }
        if variance < 0 {
            return Err(DomainError::InvalidBudget {
                field: "portfolio_risk_model.variance",
            });
        }
        let aggregate = |selector: fn(&AssetRiskCalibration) -> u32| {
            Asset::EXECUTABLE
                .into_iter()
                .try_fold(0_u128, |sum, asset| {
                    let weight = u128::from(target.weights[&asset].0);
                    let calibration = self.asset_calibrations.get(&asset);
                    let value = if weight == 0 {
                        0
                    } else {
                        u128::from(selector(calibration.ok_or(DomainError::InvalidBudget {
                            field: "portfolio_risk_model.calibration",
                        })?))
                    };
                    Ok::<_, DomainError>(sum.saturating_add(weight.saturating_mul(value)))
                })
        };
        let scaled = |value: u128| {
            u32::try_from(value / u128::from(WeightPpm::SCALE)).map_err(|_| {
                DomainError::InvalidBudget {
                    field: "portfolio_risk_assessment",
                }
            })
        };
        liquidity_binding_assets.sort();
        liquidity_binding_assets.dedup();
        Ok(PortfolioRiskAssessment {
            risk_model_hash: Some(self.portfolio_risk_model.identity_hash()?),
            calibrated_assets: target
                .weights
                .values()
                .filter(|weight| weight.0 > 0)
                .count() as u8,
            covariance_sample_count: self.portfolio_risk_model.sample_count,
            ex_ante_volatility_ppm: Some(integer_sqrt(variance as u128) as u32),
            beta_ppm: Some(scaled(aggregate(|calibration| calibration.beta_ppm)?)?),
            expected_shortfall_ppm: Some(scaled(aggregate(|calibration| {
                calibration.expected_shortfall_ppm
            })?)?),
            gap_loss_ppm: Some(scaled(aggregate(|calibration| calibration.gap_loss_ppm)?)?),
            liquidity_binding_assets,
            leveraged_holding_limit_days: self.portfolio_risk_model.max_leveraged_holding_days,
        })
    }
}

impl Default for DecisionPolicy {
    fn default() -> Self {
        Self {
            min_confidence_ppm: 250_000,
            max_gross_weight: WeightPpm(500_000),
            horizon_weights: BTreeMap::from([
                (DecisionHorizon::T1, WeightPpm(333_333)),
                (DecisionHorizon::T3, WeightPpm(333_333)),
                (DecisionHorizon::T5, WeightPpm(333_334)),
            ]),
            maximum_execution_delay_ms: 5 * 60 * 1_000,
            minimum_process_quality_ppm: 900_000,
            min_probability_edge_ppm: 50_000,
            // No operational calibration sample threshold is inferred from a
            // paper or model output. The default remains fail-closed until a
            // validated policy snapshot installs an explicit threshold and
            // matching per-asset calibration records.
            min_calibration_samples: u32::MAX,
            max_brier_score_ppm: 250_000,
            active_forecast_calibration: None,
            forecast_calibrations: Vec::new(),
            target_annualized_volatility_ppm: 150_000,
            max_portfolio_beta_ppm: 500_000,
            asset_calibrations: BTreeMap::new(),
            portfolio_risk_model: PortfolioRiskModel {
                version: "audit-unapproved-v1".to_owned(),
                sample_count: 0,
                covariance_ppm_squared: BTreeMap::new(),
                max_expected_shortfall_ppm: 0,
                max_gap_loss_ppm: 0,
                max_leveraged_holding_days: 1,
            },
        }
    }
}

fn is_daily_reset(asset: Asset) -> bool {
    matches!(asset, Asset::Tqqq | Asset::Soxl)
}

fn horizon_days(horizon: DecisionHorizon) -> u8 {
    match horizon {
        DecisionHorizon::T1 => 1,
        DecisionHorizon::T3 => 3,
        DecisionHorizon::T5 => 5,
    }
}

fn gross_weight_ppm(target: &TargetPortfolio) -> Result<u32, DomainError> {
    target
        .weights
        .values()
        .try_fold(0_u32, |sum, weight| sum.checked_add(weight.0))
        .ok_or(DomainError::InvalidBudget {
            field: "decision_policy.target",
        })
}

fn scale_target_to_limit(
    target: &mut TargetPortfolio,
    measured: u32,
    limit: u32,
) -> Result<(), DomainError> {
    if measured <= limit || measured == 0 {
        return Ok(());
    }
    for weight in target.weights.values_mut() {
        weight.0 = u32::try_from(
            u64::from(weight.0).saturating_mul(u64::from(limit)) / u64::from(measured),
        )
        .map_err(|_| DomainError::InvalidBudget {
            field: "decision_policy.target",
        })?;
    }
    Ok(())
}

fn integer_sqrt(value: u128) -> u128 {
    if value < 2 {
        return value;
    }
    let mut lower = 1_u128;
    let mut upper = value.min(u128::from(u64::MAX));
    while lower <= upper {
        let middle = lower + (upper - lower) / 2;
        let quotient = value / middle;
        if middle == quotient {
            return middle;
        }
        if middle < quotient {
            lower = middle + 1;
        } else {
            upper = middle - 1;
        }
    }
    upper
}

#[derive(Debug, Clone)]
pub struct DecisionRuntime {
    store: Store,
    policy: DecisionPolicy,
}
include!("decision_gate/decide.rs");
include!("decision_gate/validate.rs");
include!("decision_gate/commit.rs");
include!("decision_gate/helpers.rs");
