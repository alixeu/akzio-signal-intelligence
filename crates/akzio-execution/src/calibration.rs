//! Offline, historical-only construction of the frozen DecisionPolicy.
//!
//! The live DecisionGate deliberately has no fitting path.  This module is the
//! explicit bootstrap boundary: callers provide predictions made before their
//! corresponding realized returns and an independent historical price panel.
//! Rust validates the temporal split, fits the forecast bins and risk model,
//! and emits a provenance-bearing policy document for operator review.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    content_hash_json, ArtifactKind, ArtifactRef, Asset, ContentHash, DecisionHorizon, DomainError,
    WeightPpm,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AssetRiskCalibration, DecisionPolicy, ForecastCalibrationBin, ForecastCalibrationScope,
    FrozenForecastCalibration, PortfolioRiskModel,
};

const PPM_ONE: i64 = 1_000_000;
const TRADING_DAYS_PER_YEAR: f64 = 252.0;

#[derive(Debug, Error)]
pub enum OfflineCalibrationError {
    #[error("offline calibration input is invalid: {0}")]
    InvalidInput(String),
    #[error("offline calibration has insufficient samples for {asset} {horizon:?}: {actual}, need {required}")]
    InsufficientSamples {
        asset: Asset,
        horizon: DecisionHorizon,
        actual: usize,
        required: u32,
    },
    #[error("offline risk model cannot be measured: {0}")]
    RiskUnavailable(String),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type OfflineCalibrationResult<T> = Result<T, OfflineCalibrationError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalForecastProvenance {
    pub provider_id: String,
    pub model_id: String,
    pub model_version_hash: ContentHash,
    pub route: String,
    pub contract_hash: ContentHash,
    pub source_decision: ArtifactRef,
    pub source_decision_context: ArtifactRef,
    pub forecast_cutoff: DateTime<Utc>,
}

impl HistoricalForecastProvenance {
    fn validate(&self) -> OfflineCalibrationResult<()> {
        if self.provider_id.trim().is_empty()
            || self.model_id.trim().is_empty()
            || self.route.trim().is_empty()
            || self.source_decision.kind != ArtifactKind::Decision
            || self.source_decision_context.kind != ArtifactKind::DecisionContext
        {
            return Err(OfflineCalibrationError::InvalidInput(
                "historical forecast provenance is incomplete".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalForecastSample {
    pub source_run_id: String,
    pub asset: Asset,
    pub horizon: DecisionHorizon,
    pub forecast_at: DateTime<Utc>,
    pub realized_at: DateTime<Utc>,
    pub raw_probability_ppm: u32,
    pub raw_expected_return_ppm: i64,
    pub realized_return_ppm: i64,
    pub provenance: HistoricalForecastProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPricePoint {
    pub observed_at: NaiveDate,
    pub close_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPriceSeries {
    pub asset: Asset,
    pub points: Vec<HistoricalPricePoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfflineRiskLimits {
    pub min_confidence_ppm: u32,
    pub max_gross_weight_ppm: u32,
    pub maximum_execution_delay_ms: u64,
    pub minimum_process_quality_ppm: u32,
    pub min_probability_edge_ppm: u32,
    pub max_brier_score_ppm: u32,
    pub target_annualized_volatility_ppm: u32,
    pub max_portfolio_beta_ppm: u32,
    pub max_expected_shortfall_ppm: u32,
    pub max_gap_loss_ppm: u32,
    pub max_capital_weight_ppm: u32,
    pub liquidity_weight_cap_ppm: u32,
    pub max_leveraged_holding_days: u8,
    pub daily_reset_decay_ppm: u32,
}

impl OfflineRiskLimits {
    pub fn validate(&self) -> OfflineCalibrationResult<()> {
        if self.max_capital_weight_ppm == 0
            || self.max_capital_weight_ppm > 1_000_000
            || self.liquidity_weight_cap_ppm == 0
            || self.liquidity_weight_cap_ppm > self.max_capital_weight_ppm
            || self.daily_reset_decay_ppm > 1_000_000
            || !(1..=3_000_000).contains(&self.max_expected_shortfall_ppm)
            || !(1..=3_000_000).contains(&self.max_gap_loss_ppm)
            || !(1..=5).contains(&self.max_leveraged_holding_days)
        {
            return Err(OfflineCalibrationError::InvalidInput(
                "invalid operator risk limits".into(),
            ));
        }
        let policy = DecisionPolicy {
            min_confidence_ppm: self.min_confidence_ppm,
            max_gross_weight: WeightPpm(self.max_gross_weight_ppm),
            maximum_execution_delay_ms: self.maximum_execution_delay_ms,
            minimum_process_quality_ppm: self.minimum_process_quality_ppm,
            min_probability_edge_ppm: self.min_probability_edge_ppm,
            max_brier_score_ppm: self.max_brier_score_ppm,
            target_annualized_volatility_ppm: self.target_annualized_volatility_ppm,
            max_portfolio_beta_ppm: self.max_portfolio_beta_ppm,
            portfolio_risk_model: PortfolioRiskModel {
                max_expected_shortfall_ppm: self.max_expected_shortfall_ppm,
                max_gap_loss_ppm: self.max_gap_loss_ppm,
                max_leveraged_holding_days: self.max_leveraged_holding_days,
                ..DecisionPolicy::default().portfolio_risk_model
            },
            ..DecisionPolicy::default()
        };
        policy.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfflineCalibrationInput {
    pub schema_version: u32,
    pub policy_version: String,
    pub algorithm_version: String,
    pub provider_id: String,
    pub model_id: String,
    pub model_version_hash: ContentHash,
    pub model_route: String,
    pub contract_hash: ContentHash,
    pub regime: String,
    pub training_start: DateTime<Utc>,
    pub training_end: DateTime<Utc>,
    pub min_samples: u32,
    pub source_runs: Vec<String>,
    pub forecasts: Vec<HistoricalForecastSample>,
    pub price_series: Vec<HistoricalPriceSeries>,
    pub risk_limits: OfflineRiskLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPolicyProvenance {
    pub policy_version: String,
    pub algorithm_version: String,
    pub created_at: DateTime<Utc>,
    pub training_start: DateTime<Utc>,
    pub training_end: DateTime<Utc>,
    pub assets: Vec<Asset>,
    pub horizons: Vec<DecisionHorizon>,
    pub sample_count: u64,
    pub source_runs: Vec<String>,
    pub input_hash: ContentHash,
    pub output_hash: ContentHash,
    pub risk_model_hash: ContentHash,
    /// Optional on read for pre-provenance policy envelopes; strict current
    /// policy loading requires all three identity fields.
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub model_route: Option<String>,
    #[serde(default)]
    pub contract_hash: Option<ContentHash>,
}

/// Frozen policy envelope persisted as an immutable SQL CAS Artifact.
/// Runtime identity reads only the canonical SQL Store active head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPolicyArtifact {
    pub schema_version: u32,
    pub provenance: DecisionPolicyProvenance,
    pub policy: DecisionPolicy,
}

impl DecisionPolicyArtifact {
    pub const SCHEMA_VERSION: u32 = 1;

    pub fn validate(&self) -> OfflineCalibrationResult<()> {
        if self.schema_version != Self::SCHEMA_VERSION
            || self.provenance.policy_version.trim().is_empty()
            || self.provenance.algorithm_version.trim().is_empty()
            || self.provenance.training_start > self.provenance.training_end
            || self.provenance.assets != Asset::EXECUTABLE
            || self.provenance.horizons != DecisionHorizon::ALL
            || self.provenance.sample_count == 0
            || self.provenance.source_runs.is_empty()
            || self
                .provenance
                .provider_id
                .as_deref()
                .is_none_or(str::is_empty)
            || self
                .provenance
                .model_route
                .as_deref()
                .is_none_or(str::is_empty)
            || self.provenance.contract_hash.is_none()
        {
            return Err(OfflineCalibrationError::InvalidInput(
                "policy provenance is incomplete".to_owned(),
            ));
        }
        self.policy.validate()?;
        let expected_risk_hash =
            content_hash_json(&serde_json::to_value(&self.policy.portfolio_risk_model)?)?;
        if expected_risk_hash != self.provenance.risk_model_hash {
            return Err(OfflineCalibrationError::InvalidInput(
                "risk model hash does not match policy".to_owned(),
            ));
        }
        let policy_value = serde_json::to_value(&self.policy)?;
        if content_hash_json(&policy_value)? != self.provenance.output_hash {
            return Err(OfflineCalibrationError::InvalidInput(
                "policy output hash does not match policy".to_owned(),
            ));
        }
        Ok(())
    }

    /// Load the complete provenance-bearing envelope from SQL CAS bytes.
    pub fn decode_strict(bytes: &[u8]) -> OfflineCalibrationResult<Self> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        if value.get("policy").is_none() {
            return Err(OfflineCalibrationError::InvalidInput(
                "frozen policy must use the provenance-bearing DecisionPolicyArtifact envelope"
                    .to_owned(),
            ));
        }
        let artifact: Self = serde_json::from_value(value)?;
        artifact.validate()?;
        Ok(artifact)
    }
}

pub fn build_offline_decision_policy(
    input: &OfflineCalibrationInput,
    frozen_at: DateTime<Utc>,
) -> OfflineCalibrationResult<DecisionPolicyArtifact> {
    validate_input(input, frozen_at)?;
    let input_hash = content_hash_json(&serde_json::to_value(input)?)?;
    let fit_dataset_hash = content_hash_json(&serde_json::to_value(&input.forecasts)?)?;
    let scope = ForecastCalibrationScope {
        model_id: input.model_id.clone(),
        model_version_hash: input.model_version_hash.clone(),
        regime: input.regime.clone(),
    };

    let mut forecast_calibrations = Vec::new();
    for horizon in DecisionHorizon::ALL {
        for asset in Asset::EXECUTABLE {
            forecast_calibrations.push(fit_forecast_calibration(
                input,
                asset,
                horizon,
                &scope,
                fit_dataset_hash.clone(),
                frozen_at,
            )?);
        }
    }

    let (asset_calibrations, portfolio_risk_model) = fit_risk_model(input)?;
    let limits = &input.risk_limits;
    let policy = DecisionPolicy {
        min_confidence_ppm: limits.min_confidence_ppm,
        max_gross_weight: WeightPpm(limits.max_gross_weight_ppm),
        maximum_execution_delay_ms: limits.maximum_execution_delay_ms,
        minimum_process_quality_ppm: limits.minimum_process_quality_ppm,
        min_probability_edge_ppm: limits.min_probability_edge_ppm,
        min_calibration_samples: input.min_samples,
        max_brier_score_ppm: limits.max_brier_score_ppm,
        active_forecast_calibration: Some(scope),
        forecast_calibrations,
        target_annualized_volatility_ppm: limits.target_annualized_volatility_ppm,
        max_portfolio_beta_ppm: limits.max_portfolio_beta_ppm,
        asset_calibrations,
        portfolio_risk_model: PortfolioRiskModel {
            max_expected_shortfall_ppm: limits.max_expected_shortfall_ppm,
            max_gap_loss_ppm: limits.max_gap_loss_ppm,
            max_leveraged_holding_days: limits.max_leveraged_holding_days,
            ..portfolio_risk_model
        },
        ..DecisionPolicy::default()
    };
    policy.validate()?;

    let output_hash = content_hash_json(&serde_json::to_value(&policy)?)?;
    let risk_model_hash = content_hash_json(&serde_json::to_value(&policy.portfolio_risk_model)?)?;
    let artifact = DecisionPolicyArtifact {
        schema_version: DecisionPolicyArtifact::SCHEMA_VERSION,
        provenance: DecisionPolicyProvenance {
            policy_version: input.policy_version.clone(),
            algorithm_version: input.algorithm_version.clone(),
            created_at: frozen_at,
            training_start: input.training_start,
            training_end: input.training_end,
            assets: Asset::EXECUTABLE.to_vec(),
            horizons: DecisionHorizon::ALL.to_vec(),
            sample_count: input.forecasts.len() as u64,
            source_runs: input.source_runs.clone(),
            input_hash,
            output_hash,
            risk_model_hash,
            provider_id: Some(input.provider_id.clone()),
            model_route: Some(input.model_route.clone()),
            contract_hash: Some(input.contract_hash.clone()),
        },
        policy,
    };
    artifact.validate()?;
    Ok(artifact)
}

fn validate_input(
    input: &OfflineCalibrationInput,
    frozen_at: DateTime<Utc>,
) -> OfflineCalibrationResult<()> {
    input.risk_limits.validate()?;
    if input.schema_version != 1
        || input.policy_version.trim().is_empty()
        || input.algorithm_version.trim().is_empty()
        || input.provider_id.trim().is_empty()
        || input.model_id.trim().is_empty()
        || input.model_route.trim().is_empty()
        || input.regime.trim().is_empty()
        || input.training_start > input.training_end
        || input.training_end > frozen_at
        || input.min_samples == 0
        || input.source_runs.is_empty()
        || input.risk_limits.max_capital_weight_ppm == 0
        || input.risk_limits.liquidity_weight_cap_ppm == 0
        || input.risk_limits.liquidity_weight_cap_ppm > input.risk_limits.max_capital_weight_ppm
        || input.risk_limits.max_leveraged_holding_days == 0
        || input.risk_limits.max_leveraged_holding_days > 5
    {
        return Err(OfflineCalibrationError::InvalidInput(
            "input metadata or risk limits are invalid".to_owned(),
        ));
    }
    let source_runs = input.source_runs.iter().collect::<BTreeSet<_>>();
    if source_runs.len() != input.source_runs.len() {
        return Err(OfflineCalibrationError::InvalidInput(
            "source_runs must be unique".to_owned(),
        ));
    }
    let price_assets = input
        .price_series
        .iter()
        .map(|series| series.asset)
        .collect::<BTreeSet<_>>();
    if price_assets.len() != input.price_series.len()
        || price_assets.len() != Asset::EXECUTABLE.len()
        || Asset::EXECUTABLE
            .into_iter()
            .any(|asset| !price_assets.contains(&asset))
    {
        return Err(OfflineCalibrationError::RiskUnavailable(
            "price series must contain exactly one series per executable asset".to_owned(),
        ));
    }
    for sample in &input.forecasts {
        sample.provenance.validate()?;
        if !source_runs.contains(&sample.source_run_id)
            || sample.raw_probability_ppm > 1_000_000
            || sample.forecast_at >= sample.realized_at
            || sample.realized_at > input.training_end
            || sample.forecast_at < input.training_start
            || sample.provenance.forecast_cutoff != sample.forecast_at
            || sample.provenance.provider_id != input.provider_id
            || sample.provenance.model_id != input.model_id
            || sample.provenance.model_version_hash != input.model_version_hash
            || sample.provenance.route != input.model_route
            || sample.provenance.contract_hash != input.contract_hash
            || sample.provenance.source_decision.kind != ArtifactKind::Decision
            || sample.provenance.source_decision_context.kind != ArtifactKind::DecisionContext
        {
            return Err(OfflineCalibrationError::InvalidInput(format!(
                "forecast sample has invalid provenance or time split for {} {:?}",
                sample.asset, sample.horizon
            )));
        }
    }
    for asset in Asset::EXECUTABLE {
        let series = input
            .price_series
            .iter()
            .find(|series| series.asset == asset)
            .ok_or_else(|| {
                OfflineCalibrationError::RiskUnavailable(format!("missing {asset} price series"))
            })?;
        if series.points.len() < input.min_samples as usize + 1
            || series.points.windows(2).any(|pair| {
                pair[0].observed_at >= pair[1].observed_at
                    || pair[0].close_micros <= 0
                    || pair[1].close_micros <= 0
            })
            || series.points.iter().any(|point| {
                point.observed_at < input.training_start.date_naive()
                    || point.observed_at > input.training_end.date_naive()
                    || point.close_micros <= 0
            })
        {
            return Err(OfflineCalibrationError::RiskUnavailable(format!(
                "{asset} price series is incomplete or outside the training window"
            )));
        }
    }
    Ok(())
}

fn fit_forecast_calibration(
    input: &OfflineCalibrationInput,
    asset: Asset,
    horizon: DecisionHorizon,
    scope: &ForecastCalibrationScope,
    fit_dataset_hash: ContentHash,
    frozen_at: DateTime<Utc>,
) -> OfflineCalibrationResult<FrozenForecastCalibration> {
    let samples = input
        .forecasts
        .iter()
        .filter(|sample| sample.asset == asset && sample.horizon == horizon)
        .collect::<Vec<_>>();
    if samples.len() < input.min_samples as usize {
        return Err(OfflineCalibrationError::InsufficientSamples {
            asset,
            horizon,
            actual: samples.len(),
            required: input.min_samples,
        });
    }
    let mut bins = Vec::with_capacity(10);
    for index in 0..10_u32 {
        let lower = index * 100_000;
        let upper = if index == 9 {
            1_000_000
        } else {
            (index + 1) * 100_000 - 1
        };
        let in_bin = samples
            .iter()
            .filter(|sample| (lower..=upper).contains(&sample.raw_probability_ppm));
        let mut count = 0_u32;
        let mut positives = 0_u32;
        let mut probability_sum = 0_u64;
        let mut alpha_sum = 0_i128;
        let mut brier_sum = 0_u128;
        for sample in in_bin {
            count += 1;
            positives += u32::from(sample.realized_return_ppm > 0);
            probability_sum += u64::from(sample.raw_probability_ppm);
            alpha_sum += i128::from(sample.realized_return_ppm);
            let outcome = if sample.realized_return_ppm > 0 {
                PPM_ONE
            } else {
                0
            };
            let difference = i128::from(sample.raw_probability_ppm) - i128::from(outcome);
            brier_sum += (difference * difference / i128::from(PPM_ONE)) as u128;
        }
        bins.push(ForecastCalibrationBin {
            raw_probability_min_ppm: lower,
            raw_probability_max_ppm: upper,
            sample_count: count,
            calibrated_probability_ppm: if count == 0 {
                500_000
            } else {
                positives
                    .saturating_mul(1_000_000)
                    .checked_div(count)
                    .unwrap_or_default()
            },
            calibrated_expected_alpha_ppm: if count == 0 {
                0
            } else {
                i64::try_from(alpha_sum / i128::from(count)).map_err(|_| {
                    OfflineCalibrationError::InvalidInput("expected alpha overflow".to_owned())
                })?
            },
        });
        let _ = (probability_sum, brier_sum);
    }
    let total_brier = samples.iter().fold(0_u128, |sum, sample| {
        let outcome = if sample.realized_return_ppm > 0 {
            PPM_ONE
        } else {
            0
        };
        let difference = i128::from(sample.raw_probability_ppm) - i128::from(outcome);
        sum.saturating_add((difference * difference / i128::from(PPM_ONE)) as u128)
    });
    Ok(FrozenForecastCalibration {
        scope: scope.clone(),
        asset,
        horizon,
        sample_count: samples.len() as u32,
        mean_brier_score_ppm: u32::try_from(total_brier / samples.len() as u128).map_err(|_| {
            OfflineCalibrationError::InvalidInput("Brier score overflow".to_owned())
        })?,
        fit_dataset_hash,
        trained_through: input.training_end,
        frozen_at,
        bins,
    })
}

fn fit_risk_model(
    input: &OfflineCalibrationInput,
) -> OfflineCalibrationResult<(BTreeMap<Asset, AssetRiskCalibration>, PortfolioRiskModel)> {
    let returns = common_returns(input)?;
    let benchmark = returns.get(&Asset::Qqq).ok_or_else(|| {
        OfflineCalibrationError::RiskUnavailable("QQQ benchmark is missing".to_owned())
    })?;
    let benchmark_variance = variance(benchmark);
    if benchmark_variance <= 0.0 {
        return Err(OfflineCalibrationError::RiskUnavailable(
            "QQQ variance is zero".to_owned(),
        ));
    }
    let mut calibrations = BTreeMap::new();
    for asset in Asset::EXECUTABLE {
        let values = returns.get(&asset).expect("validated common returns");
        // `common_returns` is already expressed in ppm.  Annualisation scales
        // the standard deviation by sqrt(252); multiplying by another ppm
        // factor here would manufacture a risk value a million times too high.
        let volatility = (variance(values).sqrt() * TRADING_DAYS_PER_YEAR.sqrt()).round();
        let covariance = covariance(values, benchmark) * TRADING_DAYS_PER_YEAR;
        let beta = (covariance / (benchmark_variance * TRADING_DAYS_PER_YEAR)).abs() * 1_000_000.0;
        let losses = values
            .iter()
            .copied()
            .filter(|value| *value < 0.0)
            .map(|value| -value)
            .collect::<Vec<_>>();
        if losses.is_empty() || volatility <= 0.0 || beta <= 0.0 {
            return Err(OfflineCalibrationError::RiskUnavailable(format!(
                "{asset} has no measurable volatility, beta, or loss tail"
            )));
        }
        let mut losses = losses;
        losses.sort_by(|left, right| right.total_cmp(left));
        let tail_count = ((values.len() as f64 * 0.05).ceil() as usize)
            .max(1)
            .min(losses.len());
        let expected_shortfall = losses.iter().take(tail_count).sum::<f64>() / tail_count as f64;
        let gap_loss = losses.first().copied().unwrap_or_default();
        let forecast_samples = input
            .forecasts
            .iter()
            .filter(|sample| sample.asset == asset)
            .collect::<Vec<_>>();
        let brier_sum = forecast_samples.iter().fold(0_u128, |sum, sample| {
            let outcome = if sample.realized_return_ppm > 0 {
                PPM_ONE
            } else {
                0
            };
            let difference = i128::from(sample.raw_probability_ppm) - i128::from(outcome);
            sum.saturating_add((difference * difference / i128::from(PPM_ONE)) as u128)
        });
        calibrations.insert(
            asset,
            AssetRiskCalibration {
                sample_count: forecast_samples.len() as u32,
                brier_score_ppm: u32::try_from(brier_sum / forecast_samples.len().max(1) as u128)
                    .map_err(|_| {
                    OfflineCalibrationError::InvalidInput("Brier score overflow".to_owned())
                })?,
                annualized_volatility_ppm: checked_risk_value(volatility, asset, "volatility")?,
                beta_ppm: checked_risk_value(beta, asset, "beta")?,
                max_capital_weight: WeightPpm(input.risk_limits.max_capital_weight_ppm),
                liquidity_weight_cap: WeightPpm(input.risk_limits.liquidity_weight_cap_ppm),
                expected_shortfall_ppm: checked_risk_value(
                    expected_shortfall,
                    asset,
                    "expected shortfall",
                )?,
                gap_loss_ppm: checked_risk_value(gap_loss, asset, "gap loss")?,
                daily_reset_decay_ppm: if matches!(asset, Asset::Tqqq | Asset::Soxl) {
                    input.risk_limits.daily_reset_decay_ppm
                } else {
                    0
                },
            },
        );
    }
    let mut covariance_ppm_squared = BTreeMap::new();
    for left in Asset::EXECUTABLE {
        let mut row = BTreeMap::new();
        for right in Asset::EXECUTABLE {
            let value = covariance(
                returns.get(&left).expect("validated common returns"),
                returns.get(&right).expect("validated common returns"),
            ) * TRADING_DAYS_PER_YEAR;
            let rounded = checked_covariance(value, left, right)?;
            let maximum = i64::from(calibrations[&left].annualized_volatility_ppm)
                .saturating_mul(i64::from(calibrations[&right].annualized_volatility_ppm));
            let bounded = if left == right {
                maximum
            } else {
                rounded.clamp(-maximum, maximum)
            };
            row.insert(right, bounded);
        }
        covariance_ppm_squared.insert(left, row);
    }
    Ok((
        calibrations,
        PortfolioRiskModel {
            version: "offline_historical_risk_v1".to_owned(),
            sample_count: returns.values().next().map_or(0, Vec::len) as u32,
            covariance_ppm_squared,
            max_expected_shortfall_ppm: input.risk_limits.max_expected_shortfall_ppm,
            max_gap_loss_ppm: input.risk_limits.max_gap_loss_ppm,
            max_leveraged_holding_days: input.risk_limits.max_leveraged_holding_days,
        },
    ))
}

fn common_returns(
    input: &OfflineCalibrationInput,
) -> OfflineCalibrationResult<BTreeMap<Asset, Vec<f64>>> {
    let mut by_date = BTreeMap::<NaiveDate, BTreeMap<Asset, i64>>::new();
    for series in &input.price_series {
        for point in &series.points {
            by_date
                .entry(point.observed_at)
                .or_default()
                .insert(series.asset, point.close_micros);
        }
    }
    let dates = by_date
        .iter()
        .filter(|(_, prices)| prices.len() == Asset::EXECUTABLE.len())
        .map(|(date, _)| *date)
        .collect::<Vec<_>>();
    let mut output = BTreeMap::new();
    for asset in Asset::EXECUTABLE {
        let prices = dates
            .iter()
            .map(|date| by_date[date][&asset] as f64)
            .collect::<Vec<_>>();
        let values = prices
            .windows(2)
            .map(|pair| (pair[1] - pair[0]) * 1_000_000.0 / pair[0])
            .collect::<Vec<_>>();
        if values.len() < input.min_samples as usize {
            return Err(OfflineCalibrationError::RiskUnavailable(format!(
                "common price panel has {} returns, need {}",
                values.len(),
                input.min_samples
            )));
        }
        output.insert(asset, values);
    }
    Ok(output)
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len().max(1) as f64
}

fn variance(values: &[f64]) -> f64 {
    let average = mean(values);
    values
        .iter()
        .map(|value| (value - average).powi(2))
        .sum::<f64>()
        / values.len().saturating_sub(1).max(1) as f64
}

fn covariance(left: &[f64], right: &[f64]) -> f64 {
    let left_mean = mean(left);
    let right_mean = mean(right);
    left.iter()
        .zip(right)
        .map(|(left, right)| (left - left_mean) * (right - right_mean))
        .sum::<f64>()
        / left.len().saturating_sub(1).max(1) as f64
}

fn checked_risk_value(value: f64, asset: Asset, label: &str) -> OfflineCalibrationResult<u32> {
    if !value.is_finite() || value <= 0.0 || value > 3_000_000.0 {
        return Err(OfflineCalibrationError::RiskUnavailable(format!(
            "{asset} {label} is outside the validated range"
        )));
    }
    Ok(value.round() as u32)
}

fn checked_covariance(value: f64, left: Asset, right: Asset) -> OfflineCalibrationResult<i64> {
    if !value.is_finite() || value.abs() > 9_000_000_000_000.0 {
        return Err(OfflineCalibrationError::RiskUnavailable(format!(
            "covariance {left}/{right} is outside the validated range"
        )));
    }
    Ok(value.round() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use akzio_domain::Forecast;

    fn fixture_input() -> OfflineCalibrationInput {
        let training_start = DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let training_end = DateTime::parse_from_rfc3339("2020-12-31T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut forecasts = Vec::new();
        for asset in Asset::EXECUTABLE {
            for horizon in DecisionHorizon::ALL {
                for index in 0..30 {
                    forecasts.push(HistoricalForecastSample {
                        source_run_id: "historical-run-1".to_owned(),
                        asset,
                        horizon,
                        forecast_at: training_start + chrono::Duration::days(100),
                        realized_at: training_start + chrono::Duration::days(101),
                        raw_probability_ppm: 700_000,
                        raw_expected_return_ppm: 10_000,
                        realized_return_ppm: if index == 0 { -2_000 } else { 10_000 },
                        provenance: HistoricalForecastProvenance {
                            provider_id: "fixture".to_owned(),
                            model_id: "gpt-test".to_owned(),
                            model_version_hash: ContentHash::of_bytes(b"gpt-test"),
                            route: "research.synthesizer".to_owned(),
                            contract_hash: ContentHash::of_bytes(b"test-contract"),
                            source_decision: ArtifactRef {
                                artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                                    b"historical-decision",
                                )),
                                kind: ArtifactKind::Decision,
                            },
                            source_decision_context: ArtifactRef {
                                artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(
                                    b"historical-decision-context",
                                )),
                                kind: ArtifactKind::DecisionContext,
                            },
                            forecast_cutoff: training_start + chrono::Duration::days(100),
                        },
                    });
                }
            }
        }
        let points = (0..61)
            .map(|index| HistoricalPricePoint {
                observed_at: training_start.date_naive() + chrono::Duration::days(index),
                close_micros: 1_000_000 + index * 1_000 + if index % 2 == 0 { 2_000 } else { 0 },
            })
            .collect::<Vec<_>>();
        OfflineCalibrationInput {
            schema_version: 1,
            policy_version: "test-policy".to_owned(),
            algorithm_version: "offline_historical_test_v1".to_owned(),
            provider_id: "fixture".to_owned(),
            model_id: "gpt-test".to_owned(),
            model_version_hash: ContentHash::of_bytes(b"gpt-test"),
            model_route: "research.synthesizer".to_owned(),
            contract_hash: ContentHash::of_bytes(b"test-contract"),
            regime: "all".to_owned(),
            training_start,
            training_end,
            min_samples: 30,
            source_runs: vec!["historical-run-1".to_owned()],
            forecasts,
            price_series: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| HistoricalPriceSeries {
                    asset,
                    points: points.clone(),
                })
                .collect(),
            risk_limits: OfflineRiskLimits {
                min_confidence_ppm: 250_000,
                max_gross_weight_ppm: 500_000,
                maximum_execution_delay_ms: 300_000,
                minimum_process_quality_ppm: 900_000,
                min_probability_edge_ppm: 50_000,
                max_brier_score_ppm: 250_000,
                target_annualized_volatility_ppm: 300_000,
                max_portfolio_beta_ppm: 2_000_000,
                max_expected_shortfall_ppm: 500_000,
                max_gap_loss_ppm: 500_000,
                max_capital_weight_ppm: 500_000,
                liquidity_weight_cap_ppm: 500_000,
                max_leveraged_holding_days: 1,
                daily_reset_decay_ppm: 0,
            },
        }
    }

    #[test]
    fn offline_policy_is_provenance_bound_and_can_produce_nonzero_target() {
        let input = fixture_input();
        let frozen_at = input.training_end + chrono::Duration::days(1);
        let artifact = build_offline_decision_policy(&input, frozen_at).unwrap();
        assert_eq!(artifact.provenance.sample_count, 360);
        assert_eq!(artifact.policy.asset_calibrations.len(), 4);
        artifact.validate().unwrap();
        let forecasts = Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                DecisionHorizon::ALL
                    .into_iter()
                    .map(move |horizon| Forecast {
                        asset,
                        horizon,
                        positive_return_probability_ppm: 700_000,
                        expected_return_ppm: 10_000,
                        thesis: Some(akzio_domain::ForecastThesis {
                            thesis_valid_until: frozen_at + chrono::Duration::days(10),
                            expected_holding_period_days: horizon.trading_days(),
                            exit_condition: "historical fixture exit".to_owned(),
                            invalidation_conditions: vec![
                                "historical fixture invalidation".to_owned()
                            ],
                        }),
                    })
            })
            .collect::<Vec<_>>();
        let (target, risk) = artifact
            .policy
            .target_with_risk(frozen_at, 900_000, &forecasts)
            .unwrap();
        assert!(target.weights.values().any(|weight| weight.0 > 0));
        assert!(risk.risk_model_hash.is_some());
        assert!(risk.covariance_sample_count >= 30);
    }

    #[test]
    fn insufficient_historical_forecasts_never_build_a_policy() {
        let mut input = fixture_input();
        input.forecasts.truncate(359);
        let error =
            build_offline_decision_policy(&input, input.training_end + chrono::Duration::days(1))
                .unwrap_err();
        assert!(matches!(
            error,
            OfflineCalibrationError::InsufficientSamples { .. }
        ));
    }

    #[test]
    fn strict_decode_rejects_bare_legacy_policy_without_provenance() {
        let bytes = serde_json::to_vec(&DecisionPolicy::default()).unwrap();
        assert!(DecisionPolicyArtifact::decode_strict(&bytes).is_err());
    }
}
