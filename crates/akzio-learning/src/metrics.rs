use super::*;

/// Index the model's per-asset up-probabilities by horizon.
///
/// Deliberately returns the per-asset probabilities rather than a
/// portfolio-weighted scalar. `positive_return_probability_ppm` forecasts a
/// single asset's own direction, so the only event it can be scored against is
/// that asset's realized direction. A position-weighted average of marginal
/// probabilities is not the probability that the portfolio return is positive
/// (that needs the assets' joint distribution), and weighting by the target
/// portfolio would fold the allocation decision into the forecast-quality
/// measurement.
pub(super) fn index_forecasts(
    forecasts: &[Forecast],
) -> EvaluationRuntimeResult<BTreeMap<OutcomeHorizon, BTreeMap<Asset, u32>>> {
    let mut by_horizon = BTreeMap::<OutcomeHorizon, BTreeMap<Asset, u32>>::new();
    for forecast in forecasts {
        forecast.validate()?;
        let horizon = match forecast.horizon {
            DecisionHorizon::T1 => OutcomeHorizon::T1,
            DecisionHorizon::T3 => OutcomeHorizon::T3,
            DecisionHorizon::T5 => OutcomeHorizon::T5,
        };
        if by_horizon
            .entry(horizon)
            .or_default()
            .insert(forecast.asset, forecast.positive_return_probability_ppm)
            .is_some()
        {
            return Err(EvaluationError::InvalidMaterialization(
                "duplicate forecast horizon",
            ));
        }
    }
    if by_horizon.len() != OutcomeHorizon::ALL.len() {
        return Err(EvaluationError::InvalidMaterialization(
            "missing forecast horizon",
        ));
    }
    for by_asset in by_horizon.values() {
        if by_asset.len() != 1
            && (by_asset.len() != Asset::EXECUTABLE.len()
                || Asset::EXECUTABLE
                    .into_iter()
                    .any(|asset| !by_asset.contains_key(&asset)))
        {
            return Err(EvaluationError::InvalidMaterialization(
                "forecast asset coverage",
            ));
        }
    }
    Ok(by_horizon)
}

/// Brier score for one binary forecast, in ppm.
///
/// `(p - o)^2` where `o` is 1 when the asset actually rose. Lower is better and
/// the range is `[0, PPM_ONE]`.
fn asset_brier_ppm(probability_ppm: u32, realized_positive: bool) -> u32 {
    let outcome_ppm = if realized_positive {
        i64::from(PPM_ONE)
    } else {
        0
    };
    let difference = i64::from(probability_ppm) - outcome_ppm;
    let squared = i128::from(difference) * i128::from(difference) / i128::from(PPM_ONE);
    u32::try_from(squared).unwrap_or(PPM_ONE)
}

/// Macro-average the per-asset Brier scores for one horizon and convert to the
/// higher-is-better quality scale that `calibration_ppm` is consumed on.
///
/// The average is unweighted on purpose: weighting by target portfolio would
/// make forecast quality depend on the allocation decision. The polarity flip is
/// required because `CanaryPairedOutcomeMetrics::from_outcome_window` maps
/// `calibration_ppm` onto `confidence_ppm`, which the promotion policy gates
/// with `>= minimum_confidence_ppm`. Storing a raw Brier there would reward
/// worse forecasts.
pub(super) fn calibration_quality_ppm(
    probabilities_by_asset: &BTreeMap<Asset, u32>,
    baseline_prices: &BTreeMap<Asset, MoneyMicros>,
    future_prices: &BTreeMap<Asset, MoneyMicros>,
) -> EvaluationRuntimeResult<Option<u32>> {
    if probabilities_by_asset.is_empty() {
        return Ok(None);
    }
    let mut total_brier_ppm = 0_u128;
    for (asset, probability_ppm) in probabilities_by_asset {
        let realized = return_ppm(
            price(baseline_prices, *asset)?,
            price(future_prices, *asset)?,
        )?;
        total_brier_ppm += u128::from(asset_brier_ppm(*probability_ppm, realized > 0));
    }
    let mean_brier_ppm = u32::try_from(total_brier_ppm / probabilities_by_asset.len() as u128)
        .map_err(|_| EvaluationError::ArithmeticOverflow)?;
    Ok(Some(PPM_ONE.saturating_sub(mean_brier_ppm)))
}

pub(super) fn index_observations<'a>(
    schedule: &OutcomeSchedule,
    observations: &'a [GovernedHorizonObservation],
) -> EvaluationRuntimeResult<BTreeMap<OutcomeHorizon, &'a GovernedHorizonObservation>> {
    let mut indexed = BTreeMap::new();
    for observation in observations {
        if !observation
            .horizon
            .is_due_after(observation.completed_trading_sessions)
            || observation.observed_trading_day <= schedule.baseline_trading_day
        {
            return Err(EvaluationError::InvalidMaterialization(
                "horizon is not due",
            ));
        }
        validate_prices(&observation.future_prices)?;
        if indexed.insert(observation.horizon, observation).is_some() {
            return Err(EvaluationError::InvalidMaterialization(
                "duplicate observation horizon",
            ));
        }
    }
    if indexed.len() != OutcomeHorizon::ALL.len() {
        return Err(EvaluationError::InvalidMaterialization(
            "missing observation horizon",
        ));
    }
    Ok(indexed)
}

pub(super) fn validate_prices(
    prices: &BTreeMap<Asset, MoneyMicros>,
) -> EvaluationRuntimeResult<()> {
    if prices.len() != Asset::EXECUTABLE.len()
        || Asset::EXECUTABLE
            .into_iter()
            .any(|asset| prices.get(&asset).is_none_or(|price| price.0 <= 0))
    {
        return Err(EvaluationError::InvalidMaterialization(
            "price surface must contain positive prices for the exact universe",
        ));
    }
    Ok(())
}

pub(super) fn price(
    prices: &BTreeMap<Asset, MoneyMicros>,
    asset: Asset,
) -> EvaluationRuntimeResult<MoneyMicros> {
    prices
        .get(&asset)
        .copied()
        .ok_or(EvaluationError::InvalidMaterialization(
            "price surface is incomplete",
        ))
}

pub(super) fn return_ppm(
    baseline: MoneyMicros,
    future: MoneyMicros,
) -> EvaluationRuntimeResult<i64> {
    if baseline.0 <= 0 || future.0 <= 0 {
        return Err(EvaluationError::InvalidMaterialization(
            "prices must be positive",
        ));
    }
    i64::try_from(
        (i128::from(future.0) - i128::from(baseline.0)) * i128::from(PPM_ONE)
            / i128::from(baseline.0),
    )
    .map_err(|_| EvaluationError::ArithmeticOverflow)
}

pub(super) fn portfolio_return_ppm(
    target: &TargetPortfolio,
    baseline: &BTreeMap<Asset, MoneyMicros>,
    future: &BTreeMap<Asset, MoneyMicros>,
) -> EvaluationRuntimeResult<i64> {
    let weighted = target
        .weights
        .iter()
        .try_fold(0_i128, |sum, (asset, weight)| {
            let asset_return = return_ppm(price(baseline, *asset)?, price(future, *asset)?)?;
            sum.checked_add(i128::from(weight.0) * i128::from(asset_return))
                .ok_or(EvaluationError::ArithmeticOverflow)
        })?;
    i64::try_from(weighted / i128::from(PPM_ONE)).map_err(|_| EvaluationError::ArithmeticOverflow)
}

pub(super) fn bounded_ratio_ppm(expected: u64, observed: u64) -> u32 {
    if expected == 0 {
        return PPM_ONE;
    }
    let numerator = u128::from(observed.min(expected)) * u128::from(PPM_ONE);
    u32::try_from(numerator / u128::from(expected)).unwrap_or(PPM_ONE)
}

pub(super) fn execution_verdict(lineage: &OutcomeExecutionLineage) -> &ArtifactRef {
    match lineage {
        OutcomeExecutionLineage::NoOrder { execution_verdict }
        | OutcomeExecutionLineage::ReconciledPaper {
            execution_verdict, ..
        } => execution_verdict,
    }
}

pub(super) fn require_canonical_purpose(purpose: RunPurpose) -> EvaluationRuntimeResult<()> {
    if purpose.is_canonical_learning() {
        Ok(())
    } else {
        Err(EvaluationError::NonCanonicalPurpose(purpose))
    }
}

pub(super) fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

pub(super) fn stable_id(value: &serde_json::Value) -> EvaluationRuntimeResult<String> {
    Ok(content_hash_json(value)?.as_str().to_owned())
}

pub(super) fn marginal_utility(outcome: &Outcome) -> i64 {
    let total = outcome
        .windows
        .iter()
        .fold(0_i128, |sum, window| sum + i128::from(window.utility_ppm));
    let average = total / i128::try_from(outcome.windows.len()).unwrap_or(1);
    average.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Promote memory only after a fresh T+1/T+3/T+5 evaluation passes quality
/// gates; contract/topology promotion remains owned by their canary policy.
///
/// `risk_recall_measured` is a hard precondition for any forward transition.
/// When risk recall was never measured the subject holds its current state:
/// unmeasured evidence must not be readable as either a pass or a failure.
pub(super) fn next_state_with_fresh_pairs(
    current: PolicyState,
    target: Option<PolicyState>,
    degraded: bool,
    risk_recall_measured: bool,
    fresh_pairs_by_horizon: [u64; 3],
    minimum_fresh_pairs_per_horizon: u64,
) -> PolicyState {
    use CandidatePolicyState as Candidate;
    use MemoryLifecycle as Memory;

    let next = if degraded {
        match current {
            PolicyState::Memory(Memory::Contested) => PolicyState::Memory(Memory::Retired),
            PolicyState::Memory(Memory::Retired) => current,
            PolicyState::Memory(_) => PolicyState::Memory(Memory::Contested),
            PolicyState::Contract(Candidate::Candidate)
            | PolicyState::Topology(Candidate::Candidate) => current,
            PolicyState::Contract(_) => PolicyState::Contract(Candidate::Candidate),
            PolicyState::Topology(_) => PolicyState::Topology(Candidate::Candidate),
        }
    } else {
        target.unwrap_or(match current {
            PolicyState::Memory(Memory::Candidate) => PolicyState::Memory(Memory::Active),
            PolicyState::Memory(Memory::Active) => PolicyState::Memory(Memory::Proven),
            _ => current,
        })
    };

    if !is_forward_transition(current, next) {
        return next;
    }
    if !risk_recall_measured {
        return current;
    }
    if fresh_pairs_by_horizon
        .iter()
        .all(|&count| count >= minimum_fresh_pairs_per_horizon)
    {
        next
    } else {
        current
    }
}

fn is_forward_transition(from: PolicyState, to: PolicyState) -> bool {
    use CandidatePolicyState as Candidate;
    use MemoryLifecycle as Memory;

    matches!(
        (from, to),
        (
            PolicyState::Memory(Memory::Candidate),
            PolicyState::Memory(Memory::Active)
        ) | (
            PolicyState::Memory(Memory::Active),
            PolicyState::Memory(Memory::Proven)
        ) | (
            PolicyState::Memory(Memory::Contested),
            PolicyState::Memory(Memory::Active)
        ) | (
            PolicyState::Contract(Candidate::Candidate),
            PolicyState::Contract(Candidate::Canary10)
        ) | (
            PolicyState::Contract(Candidate::Canary10),
            PolicyState::Contract(Candidate::Canary25)
        ) | (
            PolicyState::Contract(Candidate::Canary25),
            PolicyState::Contract(Candidate::Canary50)
        ) | (
            PolicyState::Contract(Candidate::Canary50),
            PolicyState::Contract(Candidate::Active)
        ) | (
            PolicyState::Topology(Candidate::Candidate),
            PolicyState::Topology(Candidate::Canary10)
        ) | (
            PolicyState::Topology(Candidate::Canary10),
            PolicyState::Topology(Candidate::Canary25)
        ) | (
            PolicyState::Topology(Candidate::Canary25),
            PolicyState::Topology(Candidate::Canary50)
        ) | (
            PolicyState::Topology(Candidate::Canary50),
            PolicyState::Topology(Candidate::Active)
        )
    )
}
