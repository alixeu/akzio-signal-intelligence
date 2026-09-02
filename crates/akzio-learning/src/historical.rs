//! Deterministic, offline point-in-time historical evaluation.
//!
//! This module intentionally has no Store, model, market-data, or broker I/O.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{Asset, ContentHash, ExperimentTrialMetrics, MoneyMicros, TargetPortfolio};
use chrono::{DateTime, Datelike, NaiveDate, Utc, Weekday};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const HISTORICAL_EVALUATION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeSession {
    pub session_date: NaiveDate,
    pub open_at: DateTime<Utc>,
    pub close_at: DateTime<Utc>,
    pub early_close: bool,
    pub session_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedExchangeCalendar {
    pub schema_version: u32,
    pub exchange: String,
    pub calendar_version: ContentHash,
    pub sessions: Vec<ExchangeSession>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HistoricalEvaluationError {
    #[error("historical calendar metadata is invalid")]
    InvalidCalendarMetadata,
    #[error("exchange session falls on a weekend: {0}")]
    WeekendSession(NaiveDate),
    #[error("exchange session is duplicated: {0}")]
    DuplicateSession(NaiveDate),
    #[error("exchange sessions must be strictly increasing")]
    NonIncreasingSession,
    #[error("exchange session timestamps are invalid: {0}")]
    InvalidSessionTimes(NaiveDate),
    #[error("exchange session is missing: {0}")]
    MissingSession(NaiveDate),
    #[error("exchange session is not complete: {0}")]
    SessionIncomplete(NaiveDate),
    #[error("decision cutoff precedes session close: {0}")]
    CutoffBeforeSessionClose(NaiveDate),
    #[error("walk-forward configuration is invalid")]
    InvalidWalkForwardConfiguration,
    #[error("walk-forward calendar is too short")]
    InsufficientWalkForwardSessions,
    #[error("candidate has not been frozen")]
    CandidateNotFrozen,
    #[error("frozen holdout has already been opened")]
    HoldoutAlreadyOpened,
    #[error("frozen holdout is invalid")]
    InvalidFrozenHoldout,
    #[error("historical session inputs must be strictly increasing")]
    NonIncreasingBacktestSession,
    #[error("evidence became available after the decision cutoff: {0}")]
    FutureEvidence(NaiveDate),
    #[error("target portfolio is invalid")]
    InvalidTargetPortfolio,
    #[error("price is missing for {asset:?} on {session_date}")]
    MissingPrice {
        session_date: NaiveDate,
        asset: Asset,
    },
    #[error("price is invalid for {asset:?} on {session_date}")]
    InvalidPrice {
        session_date: NaiveDate,
        asset: Asset,
    },
    #[error("explicit execution cost cannot be negative")]
    NegativeExecutionCost,
    #[error("historical arithmetic overflowed")]
    ArithmeticOverflow,
    #[error("historical NAV must remain positive")]
    NonPositiveNav,
    #[error("experiment trial metrics are invalid")]
    InvalidExperimentTrialMetrics,
}

impl VersionedExchangeCalendar {
    pub fn validate(&self) -> Result<(), HistoricalEvaluationError> {
        if self.schema_version != HISTORICAL_EVALUATION_SCHEMA_VERSION
            || self.exchange.trim().is_empty()
            || self.sessions.is_empty()
        {
            return Err(HistoricalEvaluationError::InvalidCalendarMetadata);
        }

        let mut seen = BTreeSet::new();
        let mut previous = None;
        for session in &self.sessions {
            if matches!(session.session_date.weekday(), Weekday::Sat | Weekday::Sun) {
                return Err(HistoricalEvaluationError::WeekendSession(
                    session.session_date,
                ));
            }
            if !seen.insert(session.session_date) {
                return Err(HistoricalEvaluationError::DuplicateSession(
                    session.session_date,
                ));
            }
            if previous.is_some_and(|date| date >= session.session_date) {
                return Err(HistoricalEvaluationError::NonIncreasingSession);
            }
            if session.open_at >= session.close_at
                || session.open_at.date_naive() != session.session_date
                || session.close_at.date_naive() != session.session_date
            {
                return Err(HistoricalEvaluationError::InvalidSessionTimes(
                    session.session_date,
                ));
            }
            previous = Some(session.session_date);
        }
        Ok(())
    }

    pub fn completed_session_for_cutoff(
        &self,
        session_date: NaiveDate,
        decision_cutoff: DateTime<Utc>,
    ) -> Result<&ExchangeSession, HistoricalEvaluationError> {
        self.validate()?;
        let session = self
            .sessions
            .iter()
            .find(|session| session.session_date == session_date)
            .ok_or(HistoricalEvaluationError::MissingSession(session_date))?;
        if !session.session_complete {
            return Err(HistoricalEvaluationError::SessionIncomplete(session_date));
        }
        if decision_cutoff < session.close_at {
            return Err(HistoricalEvaluationError::CutoffBeforeSessionClose(
                session_date,
            ));
        }
        Ok(session)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalkForwardConfig {
    pub training_sessions: usize,
    pub purge_sessions: usize,
    pub validation_sessions: usize,
    pub embargo_sessions: usize,
    pub holdout_sessions: usize,
    pub step_sessions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalkForwardSplit {
    pub ordinal: u32,
    pub calendar_version: ContentHash,
    pub training: Vec<NaiveDate>,
    pub purged: Vec<NaiveDate>,
    pub validation: Vec<NaiveDate>,
    pub embargoed: Vec<NaiveDate>,
    pub holdout: Vec<NaiveDate>,
}

pub fn purged_walk_forward_splits(
    calendar: &VersionedExchangeCalendar,
    config: WalkForwardConfig,
) -> Result<Vec<WalkForwardSplit>, HistoricalEvaluationError> {
    calendar.validate()?;
    if config.training_sessions == 0
        || config.purge_sessions == 0
        || config.validation_sessions == 0
        || config.embargo_sessions == 0
        || config.holdout_sessions == 0
        || config.step_sessions == 0
    {
        return Err(HistoricalEvaluationError::InvalidWalkForwardConfiguration);
    }
    if let Some(session) = calendar
        .sessions
        .iter()
        .find(|session| !session.session_complete)
    {
        return Err(HistoricalEvaluationError::SessionIncomplete(
            session.session_date,
        ));
    }

    let window_len = config
        .training_sessions
        .checked_add(config.purge_sessions)
        .and_then(|value| value.checked_add(config.validation_sessions))
        .and_then(|value| value.checked_add(config.embargo_sessions))
        .and_then(|value| value.checked_add(config.holdout_sessions))
        .ok_or(HistoricalEvaluationError::InvalidWalkForwardConfiguration)?;
    let dates = calendar
        .sessions
        .iter()
        .map(|session| session.session_date)
        .collect::<Vec<_>>();
    if dates.len() < window_len {
        return Err(HistoricalEvaluationError::InsufficientWalkForwardSessions);
    }

    let mut splits = Vec::new();
    let mut start = 0_usize;
    while start
        .checked_add(window_len)
        .is_some_and(|end| end <= dates.len())
    {
        let training_end = start + config.training_sessions;
        let purge_end = training_end + config.purge_sessions;
        let validation_end = purge_end + config.validation_sessions;
        let embargo_end = validation_end + config.embargo_sessions;
        let holdout_end = embargo_end + config.holdout_sessions;
        splits.push(WalkForwardSplit {
            ordinal: u32::try_from(splits.len())
                .map_err(|_| HistoricalEvaluationError::ArithmeticOverflow)?,
            calendar_version: calendar.calendar_version.clone(),
            training: dates[start..training_end].to_vec(),
            purged: dates[training_end..purge_end].to_vec(),
            validation: dates[purge_end..validation_end].to_vec(),
            embargoed: dates[validation_end..embargo_end].to_vec(),
            holdout: dates[embargo_end..holdout_end].to_vec(),
        });
        start = start
            .checked_add(config.step_sessions)
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?;
    }
    Ok(splits)
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenHoldout {
    schema_version: u32,
    candidate_hash: ContentHash,
    candidate_frozen_at: DateTime<Utc>,
    calendar_version: ContentHash,
    sessions: Vec<NaiveDate>,
    opened_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenHoldoutView {
    pub candidate_hash: ContentHash,
    pub calendar_version: ContentHash,
    pub sessions: Vec<NaiveDate>,
    pub opened_at: DateTime<Utc>,
}

impl FrozenHoldout {
    pub fn new(
        candidate_hash: ContentHash,
        candidate_frozen_at: DateTime<Utc>,
        calendar_version: ContentHash,
        sessions: Vec<NaiveDate>,
    ) -> Result<Self, HistoricalEvaluationError> {
        let mut seen = BTreeSet::new();
        let strictly_increasing = sessions.iter().copied().all(|date| seen.insert(date))
            && sessions.windows(2).all(|pair| pair[0] < pair[1]);
        if sessions.is_empty() || !strictly_increasing {
            return Err(HistoricalEvaluationError::InvalidFrozenHoldout);
        }
        Ok(Self {
            schema_version: HISTORICAL_EVALUATION_SCHEMA_VERSION,
            candidate_hash,
            candidate_frozen_at,
            calendar_version,
            sessions,
            opened_at: None,
        })
    }

    pub fn open_once(
        &mut self,
        opened_at: DateTime<Utc>,
    ) -> Result<FrozenHoldoutView, HistoricalEvaluationError> {
        if opened_at < self.candidate_frozen_at {
            return Err(HistoricalEvaluationError::CandidateNotFrozen);
        }
        if self.opened_at.is_some() {
            return Err(HistoricalEvaluationError::HoldoutAlreadyOpened);
        }
        self.opened_at = Some(opened_at);
        Ok(FrozenHoldoutView {
            candidate_hash: self.candidate_hash.clone(),
            calendar_version: self.calendar_version.clone(),
            sessions: self.sessions.clone(),
            opened_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalSessionInput {
    pub session_date: NaiveDate,
    pub decision_cutoff: DateTime<Utc>,
    pub evidence_available_at: Vec<DateTime<Utc>>,
    pub target: TargetPortfolio,
    pub decision_prices: BTreeMap<Asset, MoneyMicros>,
    pub fill_prices: BTreeMap<Asset, MoneyMicros>,
    pub explicit_execution_cost: MoneyMicros,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalNavPoint {
    pub session_date: NaiveDate,
    pub nav_before_trade: MoneyMicros,
    pub nav_after_trade: MoneyMicros,
    pub gross_turnover: MoneyMicros,
    pub turnover_ppm: u64,
    pub explicit_execution_cost: MoneyMicros,
    pub implementation_shortfall: MoneyMicros,
    pub net_execution_cost: MoneyMicros,
    pub return_ppm: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalBacktestResult {
    pub schema_version: u32,
    pub calendar_version: ContentHash,
    pub initial_nav: MoneyMicros,
    pub final_nav: MoneyMicros,
    pub nav_path: Vec<HistoricalNavPoint>,
    pub total_turnover: MoneyMicros,
    pub total_explicit_execution_cost: MoneyMicros,
    pub total_implementation_shortfall: MoneyMicros,
    pub returns_ppm: Vec<i64>,
    pub experiment_trial_metrics: ExperimentTrialMetrics,
}

pub fn run_point_in_time_backtest(
    calendar: &VersionedExchangeCalendar,
    initial_nav: MoneyMicros,
    sessions: &[HistoricalSessionInput],
) -> Result<HistoricalBacktestResult, HistoricalEvaluationError> {
    calendar.validate()?;
    if initial_nav.0 <= 0 {
        return Err(HistoricalEvaluationError::NonPositiveNav);
    }

    let mut cash = initial_nav.0;
    let mut quantities = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| (asset, 0_i64))
        .collect::<BTreeMap<_, _>>();
    let mut previous_session = None;
    let mut previous_nav = initial_nav.0;
    let mut nav_path = Vec::with_capacity(sessions.len());
    let mut returns_ppm = Vec::with_capacity(sessions.len());
    let mut total_turnover = 0_i64;
    let mut total_explicit_cost = 0_i64;
    let mut total_shortfall = 0_i64;

    for input in sessions {
        if previous_session.is_some_and(|date| date >= input.session_date) {
            return Err(HistoricalEvaluationError::NonIncreasingBacktestSession);
        }
        calendar.completed_session_for_cutoff(input.session_date, input.decision_cutoff)?;
        if input
            .evidence_available_at
            .iter()
            .any(|available_at| *available_at > input.decision_cutoff)
        {
            return Err(HistoricalEvaluationError::FutureEvidence(
                input.session_date,
            ));
        }
        validate_target(&input.target)?;
        validate_prices(input)?;
        if input.explicit_execution_cost.0 < 0 {
            return Err(HistoricalEvaluationError::NegativeExecutionCost);
        }

        let mut nav_before_trade = cash;
        for asset in Asset::EXECUTABLE {
            nav_before_trade = checked_add(
                nav_before_trade,
                position_value(quantities[&asset], input.decision_prices[&asset])?,
            )?;
        }
        if nav_before_trade <= 0 {
            return Err(HistoricalEvaluationError::NonPositiveNav);
        }

        let mut session_turnover = 0_i64;
        let mut session_shortfall = 0_i64;
        for asset in Asset::EXECUTABLE {
            let decision_price = input.decision_prices[&asset];
            let fill_price = input.fill_prices[&asset];
            let desired_quantity = desired_quantity(
                nav_before_trade,
                input.target.weights[&asset].0,
                decision_price,
            )?;
            let trade_quantity = desired_quantity
                .checked_sub(quantities[&asset])
                .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?;
            let fill_notional = position_value(trade_quantity, fill_price)?;
            cash = cash
                .checked_sub(fill_notional)
                .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?;
            session_turnover = checked_add(
                session_turnover,
                absolute_position_value(trade_quantity, fill_price)?,
            )?;
            session_shortfall = checked_add(
                session_shortfall,
                execution_shortfall(trade_quantity, decision_price, fill_price)?,
            )?;
            quantities.insert(asset, desired_quantity);
        }
        cash = cash
            .checked_sub(input.explicit_execution_cost.0)
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?;

        let mut nav_after_trade = cash;
        for asset in Asset::EXECUTABLE {
            nav_after_trade = checked_add(
                nav_after_trade,
                position_value(quantities[&asset], input.decision_prices[&asset])?,
            )?;
        }
        if nav_after_trade <= 0 {
            return Err(HistoricalEvaluationError::NonPositiveNav);
        }

        let session_return = ratio_ppm(nav_after_trade - previous_nav, previous_nav)?;
        let turnover_ppm = u64::try_from(ratio_ppm(session_turnover, nav_before_trade)?)
            .map_err(|_| HistoricalEvaluationError::ArithmeticOverflow)?;
        let net_execution_cost = checked_add(session_shortfall, input.explicit_execution_cost.0)?;
        total_turnover = checked_add(total_turnover, session_turnover)?;
        total_explicit_cost = checked_add(total_explicit_cost, input.explicit_execution_cost.0)?;
        total_shortfall = checked_add(total_shortfall, session_shortfall)?;
        returns_ppm.push(session_return);
        nav_path.push(HistoricalNavPoint {
            session_date: input.session_date,
            nav_before_trade: MoneyMicros(nav_before_trade),
            nav_after_trade: MoneyMicros(nav_after_trade),
            gross_turnover: MoneyMicros(session_turnover),
            turnover_ppm,
            explicit_execution_cost: input.explicit_execution_cost,
            implementation_shortfall: MoneyMicros(session_shortfall),
            net_execution_cost: MoneyMicros(net_execution_cost),
            return_ppm: session_return,
        });
        previous_session = Some(input.session_date);
        previous_nav = nav_after_trade;
    }

    let experiment_trial_metrics = experiment_trial_metrics(&returns_ppm)?;
    Ok(HistoricalBacktestResult {
        schema_version: HISTORICAL_EVALUATION_SCHEMA_VERSION,
        calendar_version: calendar.calendar_version.clone(),
        initial_nav,
        final_nav: MoneyMicros(previous_nav),
        nav_path,
        total_turnover: MoneyMicros(total_turnover),
        total_explicit_execution_cost: MoneyMicros(total_explicit_cost),
        total_implementation_shortfall: MoneyMicros(total_shortfall),
        returns_ppm,
        experiment_trial_metrics,
    })
}

fn validate_target(target: &TargetPortfolio) -> Result<(), HistoricalEvaluationError> {
    target
        .validate_universe()
        .map_err(|_| HistoricalEvaluationError::InvalidTargetPortfolio)?;
    let gross = target.weights.values().try_fold(0_u64, |sum, weight| {
        if weight.0 > akzio_domain::WeightPpm::SCALE {
            return Err(HistoricalEvaluationError::InvalidTargetPortfolio);
        }
        sum.checked_add(u64::from(weight.0))
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)
    })?;
    if gross > u64::from(akzio_domain::WeightPpm::SCALE) {
        return Err(HistoricalEvaluationError::InvalidTargetPortfolio);
    }
    Ok(())
}

fn validate_prices(input: &HistoricalSessionInput) -> Result<(), HistoricalEvaluationError> {
    for asset in Asset::EXECUTABLE {
        for prices in [&input.decision_prices, &input.fill_prices] {
            let price = prices
                .get(&asset)
                .ok_or(HistoricalEvaluationError::MissingPrice {
                    session_date: input.session_date,
                    asset,
                })?;
            if price.0 <= 0 {
                return Err(HistoricalEvaluationError::InvalidPrice {
                    session_date: input.session_date,
                    asset,
                });
            }
        }
    }
    Ok(())
}

fn desired_quantity(
    nav_micros: i64,
    weight_ppm: u32,
    price: MoneyMicros,
) -> Result<i64, HistoricalEvaluationError> {
    narrow_i128(
        i128::from(nav_micros)
            .checked_mul(i128::from(weight_ppm))
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?
            / i128::from(price.0),
    )
}

fn position_value(
    quantity_micros: i64,
    price: MoneyMicros,
) -> Result<i64, HistoricalEvaluationError> {
    narrow_i128(
        i128::from(quantity_micros)
            .checked_mul(i128::from(price.0))
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?
            / i128::from(akzio_domain::WeightPpm::SCALE),
    )
}

fn absolute_position_value(
    quantity_micros: i64,
    price: MoneyMicros,
) -> Result<i64, HistoricalEvaluationError> {
    let product = i128::from(quantity_micros)
        .checked_mul(i128::from(price.0))
        .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?;
    narrow_i128(
        product
            .checked_abs()
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?
            / i128::from(akzio_domain::WeightPpm::SCALE),
    )
}

fn execution_shortfall(
    trade_quantity_micros: i64,
    decision_price: MoneyMicros,
    fill_price: MoneyMicros,
) -> Result<i64, HistoricalEvaluationError> {
    let price_difference = fill_price
        .0
        .checked_sub(decision_price.0)
        .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?;
    narrow_i128(
        i128::from(trade_quantity_micros)
            .checked_mul(i128::from(price_difference))
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?
            / i128::from(akzio_domain::WeightPpm::SCALE),
    )
}

fn ratio_ppm(numerator: i64, denominator: i64) -> Result<i64, HistoricalEvaluationError> {
    if denominator <= 0 {
        return Err(HistoricalEvaluationError::NonPositiveNav);
    }
    narrow_i128(
        i128::from(numerator)
            .checked_mul(i128::from(akzio_domain::WeightPpm::SCALE))
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?
            / i128::from(denominator),
    )
}

fn checked_add(left: i64, right: i64) -> Result<i64, HistoricalEvaluationError> {
    left.checked_add(right)
        .ok_or(HistoricalEvaluationError::ArithmeticOverflow)
}

fn narrow_i128(value: i128) -> Result<i64, HistoricalEvaluationError> {
    i64::try_from(value).map_err(|_| HistoricalEvaluationError::ArithmeticOverflow)
}

fn experiment_trial_metrics(
    returns_ppm: &[i64],
) -> Result<ExperimentTrialMetrics, HistoricalEvaluationError> {
    if returns_ppm.len() < 2 {
        return Err(HistoricalEvaluationError::InvalidExperimentTrialMetrics);
    }
    let return_sum = returns_ppm.iter().try_fold(0_i128, |sum, value| {
        sum.checked_add(i128::from(*value))
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)
    })?;
    let mean = narrow_i128(
        return_sum
            / i128::try_from(returns_ppm.len())
                .map_err(|_| HistoricalEvaluationError::ArithmeticOverflow)?,
    )?;
    let variance = returns_ppm.iter().try_fold(0_u128, |sum, value| {
        let difference = i128::from(*value) - i128::from(mean);
        let squared = difference
            .checked_mul(difference)
            .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?;
        sum.checked_add(
            u128::try_from(squared).map_err(|_| HistoricalEvaluationError::ArithmeticOverflow)?,
        )
        .ok_or(HistoricalEvaluationError::ArithmeticOverflow)
    })? / u128::try_from(returns_ppm.len())
        .map_err(|_| HistoricalEvaluationError::ArithmeticOverflow)?;
    let standard_deviation = integer_sqrt(variance);
    let sharpe_ratio_ppm = if standard_deviation == 0 {
        None
    } else {
        Some(narrow_i128(
            i128::from(mean)
                .checked_mul(i128::from(akzio_domain::WeightPpm::SCALE))
                .ok_or(HistoricalEvaluationError::ArithmeticOverflow)?
                / i128::try_from(standard_deviation)
                    .map_err(|_| HistoricalEvaluationError::ArithmeticOverflow)?,
        )?)
    };
    let metrics = ExperimentTrialMetrics {
        slice_returns_ppm: returns_ppm.to_vec(),
        mean_return_ppm: mean,
        sharpe_ratio_ppm,
    };
    metrics
        .validate()
        .map_err(|_| HistoricalEvaluationError::InvalidExperimentTrialMetrics)?;
    Ok(metrics)
}

fn integer_sqrt(value: u128) -> u128 {
    if value < 2 {
        return value;
    }
    let mut current = (value >> 1) + 1;
    let mut next = (current + value / current) >> 1;
    while next < current {
        current = next;
        next = (current + value / current) >> 1;
    }
    current
}
