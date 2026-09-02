//! Deterministic search-bias evaluation over an immutable trial ledger.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use akzio_domain::{
    ArtifactKind, ArtifactRef, ExperimentCondition, ExperimentTrial, ExperimentTrialMetrics,
    ExperimentTrialStatus, MetricIdentity, SearchBiasAcceptancePolicy, SearchBiasCertificate,
    DOMAIN_SCHEMA_VERSION,
};
use chrono::{DateTime, Utc};
use thiserror::Error;

const MIN_STATISTICAL_SAMPLES: usize = 8;
const MIN_PBO_TRIALS: usize = 4;
const PBO_SLICES: usize = 10;
const BOOTSTRAP_RESAMPLES: usize = 1_024;
const PPM: f64 = 1_000_000.0;

#[derive(Debug, Error)]
pub enum SearchBiasEvaluationError {
    #[error(transparent)]
    Domain(#[from] akzio_domain::DomainError),
    #[error("trial ledger contains duplicate or mismatched trial references")]
    InvalidLedger,
    #[error("selected trial is not eligible for promotion")]
    InvalidSelectedTrial,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchBiasEvaluationResult {
    pub certificate: SearchBiasCertificate,
    pub comparable_trial_count: u64,
}

/// Computes DSR, CSCV-PBO, a moving-block bootstrap lower bound and a
/// Bonferroni family-wise error bound from the complete append-only ledger.
///
/// Failed, rejected and invalidated trials remain in `global_trial_count`.
/// Statistical metrics use only trials with comparable holdout slices. Missing
/// sample support produces `None`; the resulting certificate is valid but not
/// promotion-ready.
pub fn default_metric_identities() -> BTreeMap<String, MetricIdentity> {
    let mut map = BTreeMap::new();
    map.insert(
        "deflated_sharpe_ratio".to_string(),
        MetricIdentity {
            metric_name: "deflated_sharpe_ratio".to_string(),
            metric_version: "v2.0".to_string(),
            implementation_hash: akzio_domain::ContentHash::of_bytes(
                b"akzio_learning::experiment::dsr_v2",
            ),
            assumptions_hash: akzio_domain::ContentHash::of_bytes(
                b"bailey_de_prado_2014_dsr_approx",
            ),
        },
    );
    map.insert(
        "probability_of_backtest_overfitting".to_string(),
        MetricIdentity {
            metric_name: "probability_of_backtest_overfitting".to_string(),
            metric_version: "v2.0".to_string(),
            implementation_hash: akzio_domain::ContentHash::of_bytes(
                b"akzio_learning::experiment::cscv_pbo_v2",
            ),
            assumptions_hash: akzio_domain::ContentHash::of_bytes(
                b"bailey_borwein_de_prado_2015_cscv_10_slices",
            ),
        },
    );
    map.insert(
        "stationary_bootstrap".to_string(),
        MetricIdentity {
            metric_name: "stationary_bootstrap".to_string(),
            metric_version: "v2.0".to_string(),
            implementation_hash: akzio_domain::ContentHash::of_bytes(
                b"akzio_learning::experiment::politis_romano_1994_v2",
            ),
            assumptions_hash: akzio_domain::ContentHash::of_bytes(
                b"geometric_block_p_inv_sqrt_n_resamples_1024",
            ),
        },
    );
    map.insert(
        "moving_block_bootstrap".to_string(),
        MetricIdentity {
            metric_name: "moving_block_bootstrap".to_string(),
            metric_version: "v2.0".to_string(),
            implementation_hash: akzio_domain::ContentHash::of_bytes(
                b"akzio_learning::experiment::moving_block_v2",
            ),
            assumptions_hash: akzio_domain::ContentHash::of_bytes(
                b"fixed_block_sqrt_n_resamples_1024",
            ),
        },
    );
    map.insert(
        "family_wise_error_rate".to_string(),
        MetricIdentity {
            metric_name: "family_wise_error_rate".to_string(),
            metric_version: "v2.0".to_string(),
            implementation_hash: akzio_domain::ContentHash::of_bytes(
                b"akzio_learning::experiment::bonferroni_fwer_v2",
            ),
            assumptions_hash: akzio_domain::ContentHash::of_bytes(b"bonferroni_one_sided_z_bound"),
        },
    );
    map
}

pub fn build_search_bias_certificate(
    selected_reference: ArtifactRef,
    selected_trial: &ExperimentTrial,
    ledger: &[(ArtifactRef, ExperimentTrial)],
    created_at: DateTime<Utc>,
) -> Result<SearchBiasEvaluationResult, SearchBiasEvaluationError> {
    build_search_bias_certificate_with_policy(
        selected_reference,
        selected_trial,
        ledger,
        None,
        created_at,
    )
}

pub fn build_search_bias_certificate_with_policy(
    selected_reference: ArtifactRef,
    selected_trial: &ExperimentTrial,
    ledger: &[(ArtifactRef, ExperimentTrial)],
    acceptance_policy: Option<&SearchBiasAcceptancePolicy>,
    created_at: DateTime<Utc>,
) -> Result<SearchBiasEvaluationResult, SearchBiasEvaluationError> {
    selected_trial.validate()?;
    if selected_reference.kind != ArtifactKind::ExperimentTrial
        || selected_trial.status != ExperimentTrialStatus::Selected
        || selected_trial.holdout_access_count != 1
        || !selected_trial.is_contamination_controlled()
    {
        return Err(SearchBiasEvaluationError::InvalidSelectedTrial);
    }

    let mut trial_refs = Vec::with_capacity(ledger.len());
    let mut comparable = Vec::new();
    let mut selected_seen = false;
    let mut bright_pair: Option<(ArtifactRef, &ExperimentTrialMetrics)> = None;
    for (reference, trial) in ledger {
        trial.validate()?;
        if reference.kind != ArtifactKind::ExperimentTrial
            || trial.subject != selected_trial.subject
            || trial_refs.contains(reference)
        {
            return Err(SearchBiasEvaluationError::InvalidLedger);
        }
        selected_seen |=
            reference == &selected_reference && trial.trial_id == selected_trial.trial_id;
        trial_refs.push(reference.clone());
        if trial.holdout_dataset_id == selected_trial.holdout_dataset_id {
            if let Some(metrics) = &trial.metrics {
                comparable.push((reference, metrics));
                if selected_trial.condition == ExperimentCondition::FullyMasked
                    && trial.condition == ExperimentCondition::Bright
                    && trial.candidate_hash == selected_trial.candidate_hash
                    && trial.parameters_hash == selected_trial.parameters_hash
                    && trial.prompt_hash == selected_trial.prompt_hash
                    && trial.model_snapshot_hash == selected_trial.model_snapshot_hash
                    && trial.generation_dataset_id == selected_trial.generation_dataset_id
                    && trial.validation_dataset_id == selected_trial.validation_dataset_id
                {
                    if bright_pair.is_some() {
                        return Err(SearchBiasEvaluationError::InvalidLedger);
                    }
                    bright_pair = Some((reference.clone(), metrics));
                }
            }
        }
    }
    if !selected_seen || ledger.len() < 2 {
        return Err(SearchBiasEvaluationError::InvalidLedger);
    }
    trial_refs.sort();

    let selected_metrics = selected_trial
        .metrics
        .as_ref()
        .ok_or(SearchBiasEvaluationError::InvalidSelectedTrial)?;
    let global_trial_count = ledger.len() as u64;
    let deflated_sharpe_ratio_ppm = deflated_sharpe_ratio_ppm(selected_metrics, global_trial_count);
    let probability_of_backtest_overfitting_ppm = pbo_ppm(&comparable);
    let (bright_trial, bright_minus_masked_bootstrap_lower_bound_ppm, contamination_risk) =
        match selected_trial.condition {
            ExperimentCondition::FullyMasked => {
                let (bright_reference, bright_metrics) =
                    bright_pair.ok_or(SearchBiasEvaluationError::InvalidLedger)?;
                if bright_metrics.slice_returns_ppm.len()
                    != selected_metrics.slice_returns_ppm.len()
                {
                    return Err(SearchBiasEvaluationError::InvalidLedger);
                }
                let paired_differences = bright_metrics
                    .slice_returns_ppm
                    .iter()
                    .zip(&selected_metrics.slice_returns_ppm)
                    .map(|(bright, masked)| bright.saturating_sub(*masked))
                    .collect::<Vec<_>>();
                let paired_metrics = ExperimentTrialMetrics {
                    mean_return_ppm: paired_differences.iter().sum::<i64>()
                        / paired_differences.len() as i64,
                    slice_returns_ppm: paired_differences,
                    sharpe_ratio_ppm: None,
                };
                paired_metrics.validate()?;
                let lower_bound = stationary_bootstrap_lower_bound_ppm(
                    &paired_metrics,
                    selected_trial.trial_id.as_str(),
                )
                .ok_or(SearchBiasEvaluationError::InvalidLedger)?;
                (
                    Some(bright_reference),
                    Some(lower_bound),
                    Some(lower_bound > 0),
                )
            }
            ExperimentCondition::PostCutoffForward => (None, None, None),
            ExperimentCondition::Bright
            | ExperimentCondition::IdentifierMasked
            | ExperimentCondition::CalendarMasked => {
                return Err(SearchBiasEvaluationError::InvalidSelectedTrial);
            }
        };

    let stationary_bootstrap_lower_bound_ppm =
        stationary_bootstrap_lower_bound_ppm(selected_metrics, selected_trial.trial_id.as_str());
    let moving_block_bootstrap_lower_bound_ppm =
        moving_block_bootstrap_lower_bound_ppm(selected_metrics, selected_trial.trial_id.as_str());
    let family_wise_error_rate_ppm =
        family_wise_error_rate_ppm(selected_metrics, global_trial_count);

    let acceptance_policy_hash = match acceptance_policy {
        Some(policy) => {
            policy.validate()?;
            Some(policy.identity_hash()?)
        }
        None => None,
    };

    let certificate = SearchBiasCertificate {
        schema_version: DOMAIN_SCHEMA_VERSION,
        certificate_id: akzio_domain::ContentHash::of_bytes(b"pending-search-bias-certificate"),
        selected_trial: selected_reference,
        trial_refs,
        global_trial_count,
        deflated_sharpe_ratio_ppm,
        probability_of_backtest_overfitting_ppm,
        stationary_bootstrap_lower_bound_ppm,
        moving_block_bootstrap_lower_bound_ppm,
        family_wise_error_rate_ppm,
        selected_condition: selected_trial.condition,
        bright_trial,
        bright_minus_masked_bootstrap_lower_bound_ppm,
        contamination_risk,
        holdout_dataset_id: selected_trial.holdout_dataset_id.clone(),
        holdout_access_count: selected_trial.holdout_access_count,
        behavior_bundle_hash: selected_trial.behavior_bundle_hash.clone(),
        acceptance_policy_hash,
        metric_identities: default_metric_identities(),
        created_at,
    }
    .seal()?;

    Ok(SearchBiasEvaluationResult {
        certificate,
        comparable_trial_count: comparable.len() as u64,
    })
}

fn deflated_sharpe_ratio_ppm(
    metrics: &ExperimentTrialMetrics,
    global_trial_count: u64,
) -> Option<i64> {
    let returns = as_f64(&metrics.slice_returns_ppm);
    if returns.len() < MIN_STATISTICAL_SAMPLES || global_trial_count < 2 {
        return None;
    }
    let moments = moments(&returns)?;
    let sharpe = metrics
        .sharpe_ratio_ppm
        .map(|value| value as f64 / PPM)
        .unwrap_or(moments.mean / moments.standard_deviation);
    let trial_count = global_trial_count as f64;
    let euler_gamma = 0.577_215_664_901_532_9;
    let variance = 1.0 / (returns.len() as f64 - 1.0);
    let expected_maximum = variance.sqrt()
        * ((1.0 - euler_gamma) * inverse_normal_cdf(1.0 - 1.0 / trial_count)
            + euler_gamma * inverse_normal_cdf(1.0 - 1.0 / (trial_count * std::f64::consts::E)));
    let denominator = (1.0 - moments.skewness * sharpe
        + ((moments.kurtosis - 1.0) / 4.0) * sharpe * sharpe)
        .max(f64::EPSILON)
        .sqrt();
    let z = (sharpe - expected_maximum) * (returns.len() as f64 - 1.0).sqrt() / denominator;
    Some((normal_cdf(z) * PPM).round().clamp(0.0, PPM) as i64)
}

fn family_wise_error_rate_ppm(
    metrics: &ExperimentTrialMetrics,
    global_trial_count: u64,
) -> Option<u32> {
    let returns = as_f64(&metrics.slice_returns_ppm);
    if returns.len() < MIN_STATISTICAL_SAMPLES || global_trial_count < 2 {
        return None;
    }
    let moments = moments(&returns)?;
    let z = moments.mean / (moments.standard_deviation / (returns.len() as f64).sqrt());
    let one_sided_p = 1.0 - normal_cdf(z);
    Some(
        (one_sided_p * global_trial_count as f64 * PPM)
            .round()
            .clamp(0.0, PPM) as u32,
    )
}

/// Combinatorially symmetric cross-validation PBO over ten common slices.
fn pbo_ppm(comparable: &[(&ArtifactRef, &ExperimentTrialMetrics)]) -> Option<u32> {
    if comparable.len() < MIN_PBO_TRIALS {
        return None;
    }
    let strategies = comparable
        .iter()
        .map(|(_, metrics)| block_means(&metrics.slice_returns_ppm, PBO_SLICES))
        .collect::<Option<Vec<_>>>()?;
    let mut overfit = 0_u64;
    let mut splits = 0_u64;
    for mask in 0_u16..(1_u16 << PBO_SLICES) {
        if mask.count_ones() as usize != PBO_SLICES / 2 {
            continue;
        }
        let in_sample = strategies
            .iter()
            .map(|values| masked_mean(values, mask, true))
            .collect::<Vec<_>>();
        let selected = in_sample
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| left.total_cmp(right))?
            .0;
        let out_sample = strategies
            .iter()
            .map(|values| masked_mean(values, mask, false))
            .collect::<Vec<_>>();
        let selected_value = out_sample[selected];
        let rank = out_sample
            .iter()
            .filter(|value| value.total_cmp(&selected_value) != Ordering::Greater)
            .count();
        if rank * 2 <= out_sample.len() {
            overfit = overfit.saturating_add(1);
        }
        splits = splits.saturating_add(1);
    }
    (splits > 0).then(|| {
        u32::try_from(u128::from(overfit) * 1_000_000 / u128::from(splits)).unwrap_or(1_000_000)
    })
}

/// Genuine stationary bootstrap (Politis & Romano, 1994) with geometrically distributed
/// block lengths having expected mean block length sqrt(N). Resamples are strictly stationary.
pub fn stationary_bootstrap_lower_bound_ppm(
    metrics: &ExperimentTrialMetrics,
    seed_material: &str,
) -> Option<i64> {
    let values = &metrics.slice_returns_ppm;
    if values.len() < MIN_STATISTICAL_SAMPLES {
        return None;
    }
    let n = values.len();
    let mean_block = (n as f64).sqrt().max(2.0);
    let p = 1.0 / mean_block;
    let p_threshold = (p * (u64::MAX as f64)) as u64;

    let mut state = seed_material
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |state, byte| {
            state
                .wrapping_mul(0x100_0000_01b3)
                .wrapping_add(u64::from(byte))
        });
    let next_u64 = |state: &mut u64| -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state
    };

    let mut means = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let mut total = 0_i128;
        let mut curr_idx = (next_u64(&mut state) as usize) % n;
        total = total.saturating_add(i128::from(values[curr_idx]));
        for _ in 1..n {
            let rand_val = next_u64(&mut state);
            if rand_val < p_threshold {
                curr_idx = (next_u64(&mut state) as usize) % n;
            } else {
                curr_idx = (curr_idx + 1) % n;
            }
            total = total.saturating_add(i128::from(values[curr_idx]));
        }
        means.push(i64::try_from(total / n as i128).ok()?);
    }
    means.sort_unstable();
    means.get(BOOTSTRAP_RESAMPLES / 20).copied()
}

pub fn moving_block_bootstrap_lower_bound_ppm(
    metrics: &ExperimentTrialMetrics,
    seed_material: &str,
) -> Option<i64> {
    let values = &metrics.slice_returns_ppm;
    if values.len() < MIN_STATISTICAL_SAMPLES {
        return None;
    }
    let block = (values.len() as f64).sqrt().round().max(2.0) as usize;
    let mut state = seed_material
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |state, byte| {
            state
                .wrapping_mul(0x100_0000_01b3)
                .wrapping_add(u64::from(byte))
        });
    let mut means = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let mut total = 0_i128;
        let mut sampled = 0_usize;
        while sampled < values.len() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let start = (state as usize) % values.len();
            for offset in 0..block {
                if sampled == values.len() {
                    break;
                }
                total = total.saturating_add(i128::from(values[(start + offset) % values.len()]));
                sampled += 1;
            }
        }
        means.push(i64::try_from(total / values.len() as i128).ok()?);
    }
    means.sort_unstable();
    means.get(BOOTSTRAP_RESAMPLES / 20).copied()
}

fn block_means(values: &[i64], blocks: usize) -> Option<Vec<f64>> {
    if values.len() < blocks || blocks == 0 {
        return None;
    }
    let mut result = Vec::with_capacity(blocks);
    for block in 0..blocks {
        let start = block * values.len() / blocks;
        let end = (block + 1) * values.len() / blocks;
        if start == end {
            return None;
        }
        result.push(
            values[start..end]
                .iter()
                .map(|value| *value as f64)
                .sum::<f64>()
                / (end - start) as f64,
        );
    }
    Some(result)
}

fn masked_mean(values: &[f64], mask: u16, included: bool) -> f64 {
    let mut total = 0.0;
    let mut count = 0_u32;
    for (index, value) in values.iter().enumerate() {
        if ((mask >> index) & 1 == 1) == included {
            total += value;
            count += 1;
        }
    }
    total / f64::from(count)
}

fn as_f64(values: &[i64]) -> Vec<f64> {
    values.iter().map(|value| *value as f64).collect()
}

#[derive(Debug, Clone, Copy)]
struct Moments {
    mean: f64,
    standard_deviation: f64,
    skewness: f64,
    kurtosis: f64,
}

fn moments(values: &[f64]) -> Option<Moments> {
    if values.len() < 2 {
        return None;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let centered = values.iter().map(|value| value - mean).collect::<Vec<_>>();
    let second = centered.iter().map(|value| value.powi(2)).sum::<f64>() / (n - 1.0);
    if second <= f64::EPSILON {
        return None;
    }
    let standard_deviation = second.sqrt();
    let skewness =
        centered.iter().map(|value| value.powi(3)).sum::<f64>() / n / standard_deviation.powi(3);
    let kurtosis =
        centered.iter().map(|value| value.powi(4)).sum::<f64>() / n / standard_deviation.powi(4);
    Some(Moments {
        mean,
        standard_deviation,
        skewness,
        kurtosis,
    })
}

fn normal_cdf(value: f64) -> f64 {
    let sign = if value < 0.0 { -1.0 } else { 1.0 };
    let x = value.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let polynomial =
        (((((1.061_405_429 * t - 1.453_152_027) * t) + 1.421_413_741) * t - 0.284_496_736) * t
            + 0.254_829_592)
            * t;
    let erf = sign * (1.0 - polynomial * (-x * x).exp());
    0.5 * (1.0 + erf)
}

/// Acklam's rational approximation for the inverse normal CDF.
fn inverse_normal_cdf(probability: f64) -> f64 {
    let probability = probability.clamp(1.0e-12, 1.0 - 1.0e-12);
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    const LOW: f64 = 0.024_25;
    const HIGH: f64 = 1.0 - LOW;
    if probability < LOW {
        let q = (-2.0 * probability.ln()).sqrt();
        return (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0);
    }
    if probability > HIGH {
        let q = (-2.0 * (1.0 - probability).ln()).sqrt();
        return -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0);
    }
    let q = probability - 0.5;
    let r = q * q;
    (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
        / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
}
