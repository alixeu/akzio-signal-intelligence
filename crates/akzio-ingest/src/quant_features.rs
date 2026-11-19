use akzio_domain::DecisionClock;
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::runtime::{
    validate_daily_bar_payload, BarAdjustment, BarSessionCoverage, EvidenceRuntimeError,
    EvidenceRuntimeResult, EvidenceSource, GovernedResource,
};

// 文件导读：QuantFeatureSnapshot 只从一个已治理、升序、OHLCV 完整的 Alpaca 日线 payload
// 计算点时特征。build_quant_feature_snapshot 先绑定 bars resource、availability/cutoff、
// adjustment/feed，再用整数 ppm/micros 和有限浮点统计生成收益、波动、ATR、回撤、成交额
// 与 gap；feature snapshot 的版本和 sample window 会随 NormalizedEvidence 保存，不能被
// 后续模型自行重算或拿下载时间替代可用时间。

pub const QUANT_FEATURE_FORMULA_VERSION: &str = "akzio.quant.daily.v2";
const LEGACY_QUANT_FEATURE_FORMULA_VERSION: &str = "akzio.quant.daily.v1";
const CLOSES_FOR_252D_RETURN: usize = 253;
const PARTS_PER_MILLION: i128 = 1_000_000;
const ANNUAL_TRADING_DAYS: f64 = 252.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Return252dUnavailable {
    Requires253Closes,
    MissingExpectedSessions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyFeatureCoverage {
    pub expected_count: usize,
    pub consecutive_latest_count: usize,
    pub return_252d_unavailable: Option<Return252dUnavailable>,
}

impl DailyFeatureCoverage {
    fn return_252d_unavailable(&self) -> Option<Return252dUnavailable> {
        if self.expected_count < CLOSES_FOR_252D_RETURN {
            Some(Return252dUnavailable::Requires253Closes)
        } else if self.consecutive_latest_count < CLOSES_FOR_252D_RETURN {
            Some(Return252dUnavailable::MissingExpectedSessions)
        } else {
            None
        }
    }
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_coverage: Option<DailyFeatureCoverage>,
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
    /// Peak-to-trough drawdown when the full expected window is covered.
    pub maximum_drawdown_ppm: Option<u32>,
    /// Arithmetic mean of close * volume over the latest 20 bars.
    pub average_dollar_volume_20d_micros: Option<u64>,
    /// Latest open versus the previous close.
    pub latest_gap_ppm: Option<i64>,
}

impl QuantFeatureSnapshot {
    pub fn validate(&self) -> EvidenceRuntimeResult<()> {
        // 验证公式版本、资源类型、调整/feed、样本窗口和每个特征所需的最小 bars 数；缺少
        // 足够历史只保留 None，不用零值冒充有效统计。
        let count = usize::try_from(self.sample_count)
            .map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
        let valid_feed = self
            .feed
            .as_deref()
            .is_none_or(|feed| matches!(feed, "iex" | "sip"));
        let (consecutive_count, full_window) =
            match (&*self.formula_version, &self.session_coverage) {
                (LEGACY_QUANT_FEATURE_FORMULA_VERSION, None) => (count, true),
                (QUANT_FEATURE_FORMULA_VERSION, Some(coverage))
                    if coverage.consecutive_latest_count > 0
                        && coverage.consecutive_latest_count <= count
                        && count <= coverage.expected_count
                        && (count != coverage.expected_count
                            || coverage.consecutive_latest_count == count)
                        && coverage.return_252d_unavailable
                            == coverage.return_252d_unavailable() =>
                {
                    (
                        coverage.consecutive_latest_count,
                        count == coverage.expected_count,
                    )
                }
                _ => return Err(EvidenceRuntimeError::InvalidAcquisition),
            };
        if !matches!(
            GovernedResource::parse(EvidenceSource::Alpaca, &self.source_resource),
            Ok(GovernedResource::AlpacaBars { .. })
        ) || self.adjustment != "all"
            || !valid_feed
            || count == 0
            || self.sample_start > self.sample_end
            || (if self.session_coverage.is_some() {
                crate::market_session_day(self.available_at)
            } else {
                self.available_at.date_naive()
            }) != self.sample_end
            || self.maximum_drawdown_ppm.is_some() != full_window
            || self.maximum_drawdown_ppm.is_some_and(|v| v > 1_000_000)
            || !return_is_valid(self.return_1d_ppm, consecutive_count >= 2)
            || !return_is_valid(self.return_3d_ppm, consecutive_count >= 4)
            || !return_is_valid(self.return_5d_ppm, consecutive_count >= 6)
            || !return_is_valid(self.return_20d_ppm, consecutive_count >= 21)
            || !return_is_valid(self.return_60d_ppm, consecutive_count >= 61)
            || !return_is_valid(
                self.return_252d_ppm,
                consecutive_count >= CLOSES_FOR_252D_RETURN,
            )
            || self.realized_volatility_20d_ppm.is_some() != (consecutive_count >= 21)
            || self.realized_volatility_60d_ppm.is_some() != (consecutive_count >= 61)
            || self.atr_14d_price_micros.is_some() != (consecutive_count >= 15)
            || self.atr_14d_price_micros.is_some_and(|value| value == 0)
            || self.average_dollar_volume_20d_micros.is_some() != (consecutive_count >= 20)
            || self
                .average_dollar_volume_20d_micros
                .is_some_and(|value| value == 0)
            || !return_is_valid(self.latest_gap_ppm, consecutive_count >= 2)
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
    // 输入→资源/日线形状校验→按 production session_closes 或受控 fixture cutoff 确认
    // available_at→解析 bars→逐项计算特征→validate。任何未来数据、未知 query metadata
    // 或非 finite 数值都在返回前 fail closed。
    let GovernedResource::AlpacaBars {
        raw_prices: false,
        limit,
        ..
    } = GovernedResource::parse(EvidenceSource::Alpaca, source_resource)?
    else {
        return Err(EvidenceRuntimeError::InvalidRequest);
    };
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
    let session_coverage = if let Some(coverage) = BarSessionCoverage::read(value)? {
        if coverage.adjustment != BarAdjustment::All
            || coverage.cutoff != decision_clock.decision_cutoff
            || (!source_uri.starts_with("fixture://") && coverage.requested_limit != limit)
            || value["request_url"].as_str() != Some(source_uri)
            || coverage.feed.map(|feed| feed.as_str()) != feed.as_deref()
            || !bars
                .iter()
                .map(|bar| crate::market_session_day(bar.timestamp))
                .eq(coverage.observed_dates.iter().copied())
            || coverage
                .expected_closes
                .last_key_value()
                .map(|(date, _)| date)
                != coverage.observed_dates.last()
        {
            return Err(EvidenceRuntimeError::InvalidAcquisition);
        }
        let mut features = DailyFeatureCoverage {
            expected_count: coverage.expected_closes.len(),
            consecutive_latest_count: coverage
                .expected_closes
                .keys()
                .rev()
                .zip(coverage.observed_dates.iter().rev())
                .take_while(|(expected, actual)| expected == actual)
                .count(),
            return_252d_unavailable: None,
        };
        features.return_252d_unavailable = features.return_252d_unavailable();
        Some(features)
    } else if source_uri.starts_with("fixture://") {
        None
    } else {
        return Err(EvidenceRuntimeError::InvalidAcquisition);
    };
    let first = bars
        .first()
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let last = bars
        .last()
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let sample_count =
        u32::try_from(bars.len()).map_err(|_| EvidenceRuntimeError::InvalidAcquisition)?;
    let trailing = &bars[bars.len()
        - session_coverage
            .as_ref()
            .map_or(bars.len(), |coverage| coverage.consecutive_latest_count)..];
    let drawdown = session_coverage
        .as_ref()
        .is_none_or(|coverage| coverage.expected_count == bars.len())
        .then(|| maximum_drawdown_ppm(&bars))
        .transpose()?;
    let session_date = |bar: &DailyBar| {
        if session_coverage.is_some() {
            crate::market_session_day(bar.timestamp)
        } else {
            bar.timestamp.date_naive()
        }
    };
    let snapshot = QuantFeatureSnapshot {
        formula_version: if session_coverage.is_some() {
            QUANT_FEATURE_FORMULA_VERSION
        } else {
            LEGACY_QUANT_FEATURE_FORMULA_VERSION
        }
        .to_owned(),
        source_resource: source_resource.to_owned(),
        adjustment,
        feed,
        available_at,
        sample_start: session_date(first),
        sample_end: session_date(last),
        sample_count,
        session_coverage,
        return_1d_ppm: trailing_return_ppm(trailing, 1)?,
        return_3d_ppm: trailing_return_ppm(trailing, 3)?,
        return_5d_ppm: trailing_return_ppm(trailing, 5)?,
        return_20d_ppm: trailing_return_ppm(trailing, 20)?,
        return_60d_ppm: trailing_return_ppm(trailing, 60)?,
        return_252d_ppm: trailing_return_ppm(trailing, 252)?,
        realized_volatility_20d_ppm: realized_volatility_ppm(trailing, 20)?,
        realized_volatility_60d_ppm: realized_volatility_ppm(trailing, 60)?,
        atr_14d_price_micros: atr_price_micros(trailing, 14)?,
        maximum_drawdown_ppm: drawdown,
        average_dollar_volume_20d_micros: average_dollar_volume_micros(trailing, 20)?,
        latest_gap_ppm: latest_gap_ppm(trailing)?,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn parse_bars(value: &Value) -> EvidenceRuntimeResult<Vec<DailyBar>> {
    // 将 provider 字段映射为内部整数 DailyBar；外层质量门已检查顺序/价格关系，这里仍对
    // 每个字段和 timestamp 做 typed 解析。
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
    // 用有限正浮点乘一百万并四舍五入为 micros；解析失败或显式范围外返回 Err。
    // 转换仍经过 f64 精度，不能把它描述为逐位精确的十进制价格证明。
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
    // 从已治理 URL 读取 adjustment/feed，并要求 adjustment=all 和有限 feed，保证公式的
    // 输入口径与 provider 请求一致。
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
    // 以最新收盘相对 days 个 bars 前收盘计算有符号 ppm；历史不足时返回 None。
    if bars.len() <= days {
        return Ok(None);
    }
    let latest = i128::from(bars[bars.len() - 1].close_micros);
    let previous = bars[bars.len() - 1 - days].close_micros;
    signed_ratio_ppm(latest - i128::from(previous), previous).map(Some)
}

fn realized_volatility_ppm(bars: &[DailyBar], days: usize) -> EvidenceRuntimeResult<Option<u64>> {
    // 对最近 days+1 个收盘计算 log return 的总体方差并按 sqrt(252) 年化，非有限值交给
    // ppm 边界检查拒绝。
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
    // 用前一日 close 与当日 high/low 的最大距离构造 true range，再对最近窗口求整数均值。
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
    // 单次前向迭代维护历史 peak，计算 peak 到当前 close 的最大百分比回撤。
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
    // 对最近窗口计算 close×volume 的日均名义成交额；u128 中间值避免资产数量放大时溢出。
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
    // 比较最新 open 与前一日 close，保留跳空方向和 ppm 符号。
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
    // 统一执行带方向的整数比例和对称四舍五入，避免负数除法向零截断造成偏差。
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
    // 计算非负 ppm 比例并在转换为 u32 前检查乘法/结果范围。
    let scaled = u128::from(numerator)
        .checked_mul(PARTS_PER_MILLION as u128)
        .ok_or(EvidenceRuntimeError::InvalidAcquisition)?;
    let rounded = (scaled + u128::from(denominator) / 2) / u128::from(denominator);
    u32::try_from(rounded).map_err(|_| EvidenceRuntimeError::InvalidAcquisition)
}

fn finite_nonnegative_ppm(value: f64) -> EvidenceRuntimeResult<u64> {
    // 将非负有限浮点指标转换为 ppm；NaN、无穷和超范围不静默截断。
    let scaled = (value * PARTS_PER_MILLION as f64).round();
    if !scaled.is_finite() || scaled < 0.0 || scaled > u64::MAX as f64 {
        return Err(EvidenceRuntimeError::InvalidAcquisition);
    }
    Ok(scaled as u64)
}

fn return_is_valid(value: Option<i64>, expected: bool) -> bool {
    // 只允许与样本数量匹配的可选收益，并拒绝小于或等于 -100% 的值。
    value.is_some() == expected && value.is_none_or(|value| value > -1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Duration, Weekday};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn count_fixture(expected_count: u16, omit: Option<usize>) -> QuantFeatureSnapshot {
        let cutoff: DateTime<Utc> = "2026-01-10T06:00:00Z".parse().unwrap();
        let dates = (1..400)
            .map(|days| cutoff.date_naive() - Duration::days(days))
            .filter(|date| !matches!(date.weekday(), Weekday::Sat | Weekday::Sun))
            .take(usize::from(expected_count))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        let last_close = dates
            .last()
            .unwrap()
            .and_hms_opt(21, 0, 0)
            .unwrap()
            .and_utc();
        let expected_closes = dates
            .iter()
            .map(|date| (*date, date.and_hms_opt(21, 0, 0).unwrap().and_utc()))
            .collect::<BTreeMap<_, _>>();
        let mut observed_closes = expected_closes.clone();
        let missing_dates = omit.map_or_else(Vec::new, |index| {
            observed_closes.remove(&dates[index]);
            vec![dates[index]]
        });
        let bars = dates.iter().enumerate().filter(|(index, _)| Some(*index) != omit).map(|(index, date)| {
            let price = 100 + index;
            json!({"t":format!("{}T02:00:00Z", date.succ_opt().unwrap()), "o":price, "h":price + 1, "l":price - 1, "c":price, "v":1000})
        }).collect::<Vec<_>>();
        let source =
            format!("fixture://alpaca/bars?adjustment=all&feed=iex&limit={expected_count}");
        let value = json!({"bars":bars, "session_closes":observed_closes,
            "feed":"iex", "price_basis":"adjusted_research", "request_url":source,
            "content_available_at":last_close + Duration::minutes(20), "decision_cutoff":cutoff,
            "calendar_source":"fixture://alpaca/calendar",
            "bar_session_coverage":{
                "version":1, "expected_closes":expected_closes,
                "observed_dates":observed_closes.keys().collect::<Vec<_>>(), "missing_dates":missing_dates,
                "outside_window_dates":[], "feed":"iex", "adjustment":"all", "provider_revision":null,
                "calendar_source":"fixture://alpaca/calendar", "cutoff":cutoff, "requested_limit":expected_count,
                "provider_has_more":false
            }
        });
        build_quant_feature_snapshot(
            &value,
            "bars:QQQ:1d",
            &source,
            &DecisionClock {
                decision_cutoff: cutoff,
            },
        )
        .unwrap()
    }

    /// WHY THIS TEST EXISTS (Task #33): Exactly 252 closes cannot produce 252
    /// returns. At 253, use the first/last closes and never bridge a calendar gap.
    #[test]
    fn daily_feature_windows_count_consecutive_exchange_closes() {
        let capped = count_fixture(252, None);
        assert_eq!(capped.return_252d_ppm, None);
        assert_eq!(capped.sample_end.to_string(), "2026-01-09");
        assert_eq!(
            capped.session_coverage.unwrap().return_252d_unavailable,
            Some(Return252dUnavailable::Requires253Closes)
        );
        let full = count_fixture(253, None);
        assert_eq!(full.return_252d_ppm, Some(2_520_000));
        assert_eq!(full.session_coverage.unwrap().return_252d_unavailable, None);
        let gap = count_fixture(253, Some(250));
        assert_eq!(gap.return_252d_ppm, None);
        assert_eq!(gap.return_3d_ppm, None);
        assert_eq!(gap.return_1d_ppm, Some(2849));
        assert_eq!(gap.maximum_drawdown_ppm, None);
        assert_eq!(
            gap.session_coverage.unwrap().return_252d_unavailable,
            Some(Return252dUnavailable::MissingExpectedSessions)
        );
    }

    /// WHY THIS TEST EXISTS (Task #33): Existing daily v1 snapshots retain their
    /// serialized interpretation when exchange-calendar coverage was unavailable.
    #[test]
    fn legacy_daily_formula_deserializes_without_claiming_coverage() {
        let mut saved = serde_json::to_value(count_fixture(252, None)).unwrap();
        saved["formula_version"] = json!(LEGACY_QUANT_FEATURE_FORMULA_VERSION);
        saved.as_object_mut().unwrap().remove("session_coverage");
        let legacy: QuantFeatureSnapshot = serde_json::from_value(saved.clone()).unwrap();
        assert_eq!(legacy.session_coverage, None);
        assert!(legacy.validate().is_ok());
        assert_eq!(serde_json::to_value(legacy).unwrap(), saved);
    }
}
