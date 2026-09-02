use akzio_domain::DecisionClock;
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::runtime::{
    validate_daily_bar_payload, EvidenceRuntimeError, EvidenceRuntimeResult, EvidenceSource,
    GovernedResource,
};

pub const QUANT_FEATURE_FORMULA_VERSION: &str = "akzio.quant.daily.v1";
const PARTS_PER_MILLION: i128 = 1_000_000;
const ANNUAL_TRADING_DAYS: f64 = 252.0;

/// Point-in-time deterministic features computed only from one governed,
/// ascending, OHLCV-valid Alpaca daily-bar payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuantFeatureSnapshot {
    pub formula_version: String,
    pub source_resource: String,
    pub adjustment: String,
    pub feed: Option<String>,
    pub available_at: DateTime<Utc>,
    pub sample_start: NaiveDate,
    pub sample_end: NaiveDate,
    pub sample_count: u32,
    pub return_1d_ppm: Option<i64>,
    pub return_3d_ppm: Option<i64>,
    pub return_5d_ppm: Option<i64>,
    pub return_20d_ppm: Option<i64>,
    pub return_60d_ppm: Option<i64>,
    pub return_252d_ppm: Option<i64>,
    /// Annualized population standard deviation of daily log close returns.
    pub realized_volatility_20d_ppm: Option<u64>,
    /// Annualized population standard deviation of daily log close returns.
    pub realized_volatility_60d_ppm: Option<u64>,
    /// Arithmetic mean of the latest 14 true ranges, in price micros.
    pub atr_14d_price_micros: Option<u64>,
    /// Peak-to-trough drawdown over the complete current payload.
    pub maximum_drawdown_ppm: u32,
    /// Arithmetic mean of close * volume over the latest 20 bars.
    pub average_dollar_volume_20d_micros: Option<u64>,
    /// Latest open versus the previous close.
    pub latest_gap_ppm: Option<i64>,
}

impl QuantFeatureSnapshot {
    pub fn validate(&self) -> EvidenceRuntimeResult<()> {
        let count = usize::try_from(self.sample_count)
            .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
        let valid_feed = self
            .feed
            .as_deref()
            .is_none_or(|feed| matches!(feed, "iex" | "sip"));
        if self.formula_version != QUANT_FEATURE_FORMULA_VERSION
            || !matches!(
                GovernedResource::parse(EvidenceSource::Alpaca, &self.source_resource),
                Ok(GovernedResource::AlpacaBars { .. })
            )
            || self.adjustment != "all"
            || !valid_feed
            || count == 0
            || self.sample_start > self.sample_end
            || self.available_at.date_naive() != self.sample_end
            || self.maximum_drawdown_ppm > 1_000_000
            || !return_is_valid(self.return_1d_ppm, count >= 2)
            || !return_is_valid(self.return_3d_ppm, count >= 4)
            || !return_is_valid(self.return_5d_ppm, count >= 6)
            || !return_is_valid(self.return_20d_ppm, count >= 21)
            || !return_is_valid(self.return_60d_ppm, count >= 61)
            || !return_is_valid(self.return_252d_ppm, count >= 253)
            || self.realized_volatility_20d_ppm.is_some() != (count >= 21)
            || self.realized_volatility_60d_ppm.is_some() != (count >= 61)
            || self.atr_14d_price_micros.is_some() != (count >= 15)
            || self.atr_14d_price_micros.is_some_and(|value| value == 0)
            || self.average_dollar_volume_20d_micros.is_some() != (count >= 20)
            || self
                .average_dollar_volume_20d_micros
                .is_some_and(|value| value == 0)
            || !return_is_valid(self.latest_gap_ppm, count >= 2)
        {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct DailyBar {
    timestamp: DateTime<Utc>,
    open_micros: u64,
    high_micros: u64,
    low_micros: u64,
    close_micros: u64,
    volume_micros: u64,
}

pub(crate) fn build_quant_feature_snapshot(
    value: &Value,
    source_resource: &str,
    source_uri: &str,
    decision_clock: &DecisionClock,
) -> EvidenceRuntimeResult<QuantFeatureSnapshot> {
    if !matches!(
        GovernedResource::parse(EvidenceSource::Alpaca, source_resource),
        Ok(GovernedResource::AlpacaBars { .. })
    ) {
        return Err(EvidenceRuntimeError::InvalidRequest);
    }
    validate_daily_bar_payload(value)?;
    let bars = parse_bars(value)?;
    // Provider calendar metadata governs production daily bars. A UTC date
    // comparison is retained only for explicitly offline fixture payloads.
    let available_at = if value.get("session_closes").is_some() {
        value
            .get("content_available_at")
            .and_then(Value::as_str)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.with_timezone(&Utc))
            .ok_or(EvidenceRuntimeError::InvalidAcquisition)?
    } else if source_uri.starts_with("fixture://") {
        if bars
            .iter()
            .any(|bar| !decision_clock.contains_completed_daily_bar(bar.timestamp))
        {
            return Err(EvidenceRuntimeError::TemporalContamination);
        }
        bars.last()
            .ok_or(EvidenceRuntimeError::InvalidAcquisition)?
            .timestamp
    } else {
        return Err(EvidenceRuntimeError::InvalidAcquisition);
    };
    if available_at > decision_clock.decision_cutoff {
        return Err(EvidenceRuntimeError::TemporalContamination);
    }
    let (adjustment, feed) = source_metadata(source_uri)?;
    let first = bars
        .first()
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let last = bars
        .last()
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let sample_count =
        u32::try_from(bars.len()).map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
    let snapshot = QuantFeatureSnapshot {
        formula_version: QUANT_FEATURE_FORMULA_VERSION.to_owned(),
        source_resource: source_resource.to_owned(),
        adjustment,
        feed,
        available_at,
        sample_start: first.timestamp.date_naive(),
        sample_end: last.timestamp.date_naive(),
        sample_count,
        return_1d_ppm: trailing_return_ppm(&bars, 1)?,
        return_3d_ppm: trailing_return_ppm(&bars, 3)?,
        return_5d_ppm: trailing_return_ppm(&bars, 5)?,
        return_20d_ppm: trailing_return_ppm(&bars, 20)?,
        return_60d_ppm: trailing_return_ppm(&bars, 60)?,
        return_252d_ppm: trailing_return_ppm(&bars, 252)?,
        realized_volatility_20d_ppm: realized_volatility_ppm(&bars, 20)?,
        realized_volatility_60d_ppm: realized_volatility_ppm(&bars, 60)?,
        atr_14d_price_micros: atr_price_micros(&bars, 14)?,
        maximum_drawdown_ppm: maximum_drawdown_ppm(&bars)?,
        average_dollar_volume_20d_micros: average_dollar_volume_micros(&bars, 20)?,
        latest_gap_ppm: latest_gap_ppm(&bars)?,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn parse_bars(value: &Value) -> EvidenceRuntimeResult<Vec<DailyBar>> {
    value
        .get("bars")
        .and_then(Value::as_array)
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?
        .iter()
        .map(|bar| {
            let timestamp = bar
                .get("t")
                .or_else(|| bar.get("timestamp"))
                .and_then(Value::as_str)
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
            Ok(DailyBar {
                timestamp: DateTime::parse_from_rfc3339(timestamp)
                    .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?
                    .with_timezone(&Utc),
                open_micros: market_micros(bar.get("o"))?,
                high_micros: market_micros(bar.get("h"))?,
                low_micros: market_micros(bar.get("l"))?,
                close_micros: market_micros(bar.get("c"))?,
                volume_micros: market_micros(bar.get("v"))?,
            })
        })
        .collect()
}

fn market_micros(value: Option<&Value>) -> EvidenceRuntimeResult<u64> {
    let value = value.ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let number = value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let scaled = (number * PARTS_PER_MILLION as f64).round();
    if !(1.0..=u64::MAX as f64).contains(&scaled) {
        return Err(EvidenceRuntimeError::InvalidAcquisition);
    }
    Ok(scaled as u64)
}

fn source_metadata(source_uri: &str) -> EvidenceRuntimeResult<(String, Option<String>)> {
    let parsed = Url::parse(source_uri).map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
    let query = parsed
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    let adjustment = query
        .get("adjustment")
        .filter(|value| value.as_ref() == "all")
        .map(|value| value.to_string())
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let feed = query.get("feed").map(ToString::to_string);
    if feed
        .as_deref()
        .is_some_and(|feed| !matches!(feed, "iex" | "sip"))
    {
        return Err(EvidenceRuntimeError::InvalidAcquisition);
    }
    Ok((adjustment, feed))
}

fn trailing_return_ppm(bars: &[DailyBar], days: usize) -> EvidenceRuntimeResult<Option<i64>> {
    if bars.len() <= days {
        return Ok(None);
    }
    let latest = i128::from(bars[bars.len() - 1].close_micros);
    let previous = bars[bars.len() - 1 - days].close_micros;
    signed_ratio_ppm(latest - i128::from(previous), previous).map(Some)
}

fn realized_volatility_ppm(bars: &[DailyBar], days: usize) -> EvidenceRuntimeResult<Option<u64>> {
    if bars.len() <= days {
        return Ok(None);
    }
    let start = bars.len() - days - 1;
    let log_returns = bars[start..]
        .windows(2)
        .map(|pair| (pair[1].close_micros as f64 / pair[0].close_micros as f64).ln())
        .collect::<Vec<_>>();
    let mean = log_returns.iter().sum::<f64>() / days as f64;
    let variance = log_returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / days as f64;
    let annualized = variance.sqrt() * ANNUAL_TRADING_DAYS.sqrt();
    finite_nonnegative_ppm(annualized).map(Some)
}

fn atr_price_micros(bars: &[DailyBar], days: usize) -> EvidenceRuntimeResult<Option<u64>> {
    if bars.len() <= days {
        return Ok(None);
    }
    let start = bars.len() - days;
    let sum = bars[start..]
        .iter()
        .enumerate()
        .try_fold(0_u128, |sum, (offset, bar)| {
            let previous_close = bars[start + offset - 1].close_micros;
            let true_range = (bar.high_micros - bar.low_micros)
                .max(bar.high_micros.abs_diff(previous_close))
                .max(bar.low_micros.abs_diff(previous_close));
            sum.checked_add(u128::from(true_range))
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)
        })?;
    let rounded = (sum + (days as u128 / 2)) / days as u128;
    u64::try_from(rounded)
        .map(Some)
        .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)
}

fn maximum_drawdown_ppm(bars: &[DailyBar]) -> EvidenceRuntimeResult<u32> {
    let mut peak = bars
        .first()
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?
        .close_micros;
    let mut maximum = 0_u32;
    for bar in bars {
        peak = peak.max(bar.close_micros);
        let drawdown = unsigned_ratio_ppm(peak - bar.close_micros, peak)?;
        maximum = maximum.max(drawdown);
    }
    Ok(maximum)
}

fn average_dollar_volume_micros(
    bars: &[DailyBar],
    days: usize,
) -> EvidenceRuntimeResult<Option<u64>> {
    if bars.len() < days {
        return Ok(None);
    }
    let sum = bars[bars.len() - days..]
        .iter()
        .try_fold(0_u128, |sum, bar| {
            let dollar_volume_micros = u128::from(bar.close_micros)
                .checked_mul(u128::from(bar.volume_micros))
                .and_then(|value| value.checked_div(PARTS_PER_MILLION as u128))
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
            sum.checked_add(dollar_volume_micros)
                .ok_or(EvidenceRuntimeError::InvalidAcquisition)
        })?;
    let rounded = (sum + (days as u128 / 2)) / days as u128;
    u64::try_from(rounded)
        .map(Some)
        .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)
}

fn latest_gap_ppm(bars: &[DailyBar]) -> EvidenceRuntimeResult<Option<i64>> {
    if bars.len() < 2 {
        return Ok(None);
    }
    let latest = bars[bars.len() - 1];
    let previous_close = bars[bars.len() - 2].close_micros;
    signed_ratio_ppm(
        i128::from(latest.open_micros) - i128::from(previous_close),
        previous_close,
    )
    .map(Some)
}

fn signed_ratio_ppm(numerator: i128, denominator: u64) -> EvidenceRuntimeResult<i64> {
    let scaled = numerator
        .checked_mul(PARTS_PER_MILLION)
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let denominator = i128::from(denominator);
    let rounded = if scaled >= 0 {
        (scaled + denominator / 2) / denominator
    } else {
        -((-scaled + denominator / 2) / denominator)
    };
    i64::try_from(rounded).map_err(|_| EvidenceRuntimeError::InvalidAcquisition)
}

fn unsigned_ratio_ppm(numerator: u64, denominator: u64) -> EvidenceRuntimeResult<u32> {
    let scaled = u128::from(numerator)
        .checked_mul(PARTS_PER_MILLION as u128)
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let rounded = (scaled + u128::from(denominator) / 2) / u128::from(denominator);
    u32::try_from(rounded).map_err(|_| EvidenceRuntimeError::InvalidAcquisition)
}

fn finite_nonnegative_ppm(value: f64) -> EvidenceRuntimeResult<u64> {
    let scaled = (value * PARTS_PER_MILLION as f64).round();
    if !scaled.is_finite() || scaled < 0.0 || scaled > u64::MAX as f64 {
        return Err(EvidenceRuntimeError::InvalidAcquisition);
    }
    Ok(scaled as u64)
}

fn return_is_valid(value: Option<i64>, expected: bool) -> bool {
    value.is_some() == expected && value.is_none_or(|value| value > -1_000_000)
}
