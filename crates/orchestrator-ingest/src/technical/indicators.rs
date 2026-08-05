use std::collections::HashMap;

use super::Bar;

const EPS: f64 = 1e-12;
const PERIODS: [usize; 5] = [5, 10, 20, 30, 60];

#[derive(Debug, Clone)]
pub(super) struct FeatureRow {
    pub(super) date: String,
    pub(super) features: HashMap<String, Option<f64>>,
}

pub(super) fn feature_rows(bars: &[Bar]) -> Vec<FeatureRow> {
    let mut bars = bars.to_vec();
    bars.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.date.cmp(&b.date)));
    let mut out = Vec::new();
    let mut start = 0;
    while start < bars.len() {
        let symbol = bars[start].symbol.clone();
        let end = bars[start..]
            .iter()
            .position(|bar| bar.symbol != symbol)
            .map(|index| start + index)
            .unwrap_or(bars.len());
        out.extend(feature_rows_for_symbol(&bars[start..end]));
        start = end;
    }
    out
}

pub(super) fn feature_rows_for_symbol(bars: &[Bar]) -> Vec<FeatureRow> {
    let open = bars
        .iter()
        .map(|bar| adjusted_price(bar.open, bar.close, bar.adj_close))
        .collect::<Vec<_>>();
    let high = bars
        .iter()
        .map(|bar| adjusted_price(bar.high, bar.close, bar.adj_close))
        .collect::<Vec<_>>();
    let low = bars
        .iter()
        .map(|bar| adjusted_price(bar.low, bar.close, bar.adj_close))
        .collect::<Vec<_>>();
    let close = bars
        .iter()
        .map(|bar| bar.adj_close.or(bar.close))
        .collect::<Vec<_>>();
    let volume = bars.iter().map(|bar| bar.volume).collect::<Vec<_>>();
    let price_ratio = ratios(&close);
    let volume_ratio = ratios(&volume);
    let close_delta = deltas(&close);
    let volume_delta = deltas(&volume);
    let log_volume = volume
        .iter()
        .map(|value| value.map(|value| (value + 1.0).ln()))
        .collect::<Vec<_>>();
    let weighted_move = price_ratio
        .iter()
        .zip(volume.iter())
        .map(|(ratio, vol)| {
            (*ratio)
                .zip(*vol)
                .map(|(ratio, vol)| (ratio - 1.0).abs() * vol)
        })
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for i in 0..bars.len() {
        let mut features = HashMap::new();
        features.insert(
            "Return".to_string(),
            price_ratio[i].map(|value| value - 1.0),
        );
        features.insert("LogReturn".to_string(), price_ratio[i].map(f64::ln));
        features.insert("Close".to_string(), close[i]);
        features.insert(
            "Gap".to_string(),
            match (open[i], ref_value(&close, i, 1)) {
                (Some(open), Some(prev_close)) => Some(open / (prev_close + EPS) - 1.0),
                _ => None,
            },
        );
        features.insert(
            "Body".to_string(),
            match (open[i], close[i]) {
                (Some(open), Some(close)) => Some((close - open) / (open + EPS)),
                _ => None,
            },
        );
        features.insert(
            "UpperShadow".to_string(),
            match (open[i], close[i], high[i]) {
                (Some(open), Some(close), Some(high)) => {
                    Some((high - open.max(close)) / (open + EPS))
                }
                _ => None,
            },
        );
        features.insert(
            "LowerShadow".to_string(),
            match (open[i], close[i], low[i]) {
                (Some(open), Some(close), Some(low)) => {
                    Some((open.min(close) - low) / (open + EPS))
                }
                _ => None,
            },
        );
        for d in PERIODS {
            insert_period_features(
                &mut features,
                i,
                d,
                &close,
                &high,
                &low,
                &volume,
                &log_volume,
                &price_ratio,
                &volume_ratio,
                &close_delta,
                &volume_delta,
                &weighted_move,
            );
        }
        rows.push(FeatureRow {
            date: bars[i].date.clone(),
            features,
        });
    }
    rows
}

#[allow(clippy::too_many_arguments)]
fn insert_period_features(
    features: &mut HashMap<String, Option<f64>>,
    i: usize,
    d: usize,
    close: &[Option<f64>],
    high: &[Option<f64>],
    low: &[Option<f64>],
    volume: &[Option<f64>],
    log_volume: &[Option<f64>],
    price_ratio: &[Option<f64>],
    volume_ratio: &[Option<f64>],
    close_delta: &[Option<f64>],
    volume_delta: &[Option<f64>],
    weighted_move: &[Option<f64>],
) {
    let suffix = d.to_string();
    let c = close[i];
    features.insert(
        format!("ROC{suffix}"),
        ref_value(close, i, d)
            .filter(|reference| reference.abs() > EPS)
            .zip(c)
            .map(|(reference, current)| current / reference - 1.0),
    );
    features.insert(
        format!("MA{suffix}"),
        window(close, i, d)
            .and_then(mean)
            .zip(c)
            .map(|(m, c)| m / (c + EPS)),
    );
    features.insert(
        format!("STD{suffix}"),
        window(close, i, d)
            .and_then(stddev)
            .zip(c)
            .map(|(s, c)| s / (c + EPS)),
    );
    features.insert(
        format!("BETA{suffix}"),
        window(close, i, d)
            .and_then(slope)
            .zip(c)
            .map(|(s, c)| s / (c + EPS)),
    );
    features.insert(
        format!("RSQR{suffix}"),
        window(close, i, d).and_then(rsquare),
    );
    features.insert(
        format!("RESI{suffix}"),
        window(close, i, d)
            .and_then(resi)
            .zip(c)
            .map(|(r, c)| r / (c + EPS)),
    );
    features.insert(
        format!("MAX{suffix}"),
        window(high, i, d)
            .and_then(max_value)
            .zip(c)
            .map(|(m, c)| m / (c + EPS)),
    );
    features.insert(
        format!("MIN{suffix}"),
        window(low, i, d)
            .and_then(min_value)
            .zip(c)
            .map(|(m, c)| m / (c + EPS)),
    );
    features.insert(
        format!("QTLU{suffix}"),
        window(close, i, d)
            .map(|w| quantile(w, 0.8))
            .zip(c)
            .map(|(q, c)| q / (c + EPS)),
    );
    features.insert(
        format!("QTLD{suffix}"),
        window(close, i, d)
            .map(|w| quantile(w, 0.2))
            .zip(c)
            .map(|(q, c)| q / (c + EPS)),
    );
    features.insert(format!("RANK{suffix}"), window(close, i, d).map(rank_pct));
    let max_high = window(high, i, d).and_then(max_value);
    let min_low = window(low, i, d).and_then(min_value);
    features.insert(
        format!("RSV{suffix}"),
        c.zip(min_low)
            .zip(max_high)
            .map(|((c, lo), hi)| (c - lo) / (hi - lo + EPS)),
    );
    features.insert(
        format!("IMAX{suffix}"),
        window(high, i, d).map(idx_max).map(|v| v as f64 / d as f64),
    );
    features.insert(
        format!("IMIN{suffix}"),
        window(low, i, d).map(idx_min).map(|v| v as f64 / d as f64),
    );
    features.insert(
        format!("IMXD{suffix}"),
        window(high, i, d)
            .zip(window(low, i, d))
            .map(|(h, l)| (idx_max(h) as f64 - idx_min(l) as f64) / d as f64),
    );
    features.insert(
        format!("CORR{suffix}"),
        window2(close, log_volume, i, d).and_then(corr),
    );
    features.insert(
        format!("CORD{suffix}"),
        window2(price_ratio, volume_ratio, i, d).and_then(|items| {
            let transformed = items
                .iter()
                .map(|(price, volume)| (*price, volume.ln_1p()))
                .collect::<Vec<_>>();
            corr(transformed)
        }),
    );
    let up = close_delta
        .iter()
        .map(|v| v.map(|v| v > 0.0))
        .collect::<Vec<_>>();
    let down = close_delta
        .iter()
        .map(|v| v.map(|v| v < 0.0))
        .collect::<Vec<_>>();
    let cntp = bool_mean(&up, i, d);
    let cntn = bool_mean(&down, i, d);
    features.insert(format!("CNTP{suffix}"), cntp);
    features.insert(format!("CNTN{suffix}"), cntn);
    features.insert(format!("CNTD{suffix}"), cntp.zip(cntn).map(|(p, n)| p - n));
    features.insert(
        format!("SUMP{suffix}"),
        sum_positive_ratio(close_delta, i, d, true),
    );
    features.insert(
        format!("SUMN{suffix}"),
        sum_positive_ratio(close_delta, i, d, false),
    );
    features.insert(
        format!("SUMD{suffix}"),
        sum_positive_ratio(close_delta, i, d, true)
            .zip(sum_positive_ratio(close_delta, i, d, false))
            .map(|(p, n)| p - n),
    );
    features.insert(
        format!("VMA{suffix}"),
        window(volume, i, d)
            .and_then(mean)
            .zip(volume[i])
            .map(|(m, v)| m / (v + EPS)),
    );
    features.insert(
        format!("VSTD{suffix}"),
        window(volume, i, d)
            .and_then(stddev)
            .zip(volume[i])
            .map(|(s, v)| s / (v + EPS)),
    );
    features.insert(
        format!("WVMA{suffix}"),
        window(weighted_move, i, d)
            .and_then(|w| stddev(w.clone()).zip(mean(w)).map(|(s, m)| s / (m + EPS))),
    );
    features.insert(
        format!("VSUMP{suffix}"),
        sum_positive_ratio(volume_delta, i, d, true),
    );
    features.insert(
        format!("VSUMN{suffix}"),
        sum_positive_ratio(volume_delta, i, d, false),
    );
    features.insert(
        format!("VSUMD{suffix}"),
        sum_positive_ratio(volume_delta, i, d, true)
            .zip(sum_positive_ratio(volume_delta, i, d, false))
            .map(|(p, n)| p - n),
    );
}

pub(super) fn adjusted_price(
    value: Option<f64>,
    close: Option<f64>,
    adj_close: Option<f64>,
) -> Option<f64> {
    match (value, close, adj_close) {
        (Some(value), Some(close), Some(adj_close)) => Some(value * adj_close / (close + EPS)),
        (value, _, _) => value,
    }
}

fn ratios(values: &[Option<f64>]) -> Vec<Option<f64>> {
    values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            value
                .zip(ref_value(values, i, 1))
                .map(|(v, prev)| v / (prev + EPS))
        })
        .collect()
}

fn deltas(values: &[Option<f64>]) -> Vec<Option<f64>> {
    values
        .iter()
        .enumerate()
        .map(|(i, value)| value.zip(ref_value(values, i, 1)).map(|(v, prev)| v - prev))
        .collect()
}

fn ref_value(values: &[Option<f64>], i: usize, d: usize) -> Option<f64> {
    i.checked_sub(d)
        .and_then(|index| values.get(index).copied().flatten())
}

fn window(values: &[Option<f64>], i: usize, d: usize) -> Option<Vec<f64>> {
    if i + 1 < d {
        return None;
    }
    let start = i + 1 - d;
    values[start..=i].iter().copied().collect()
}

fn window2(a: &[Option<f64>], b: &[Option<f64>], i: usize, d: usize) -> Option<Vec<(f64, f64)>> {
    if i + 1 < d {
        return None;
    }
    let start = i + 1 - d;
    a[start..=i]
        .iter()
        .zip(&b[start..=i])
        .map(|(a, b)| (*a).zip(*b))
        .collect()
}

fn mean(values: Vec<f64>) -> Option<f64> {
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn stddev(values: Vec<f64>) -> Option<f64> {
    let avg = mean(values.clone())?;
    Some((values.iter().map(|v| (v - avg).powi(2)).sum::<f64>() / values.len() as f64).sqrt())
}

fn max_value(values: Vec<f64>) -> Option<f64> {
    values.into_iter().reduce(f64::max)
}

fn min_value(values: Vec<f64>) -> Option<f64> {
    values.into_iter().reduce(f64::min)
}

fn quantile(mut values: Vec<f64>, q: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() - 1) as f64 * q).round() as usize]
}

fn rank_pct(values: Vec<f64>) -> f64 {
    let last = *values.last().unwrap_or(&0.0);
    values.iter().filter(|value| **value <= last).count() as f64 / values.len() as f64
}

pub(super) fn slope(values: Vec<f64>) -> Option<f64> {
    let n = values.len() as f64;
    let x_mean = (n - 1.0) / 2.0;
    let y_mean = values.iter().sum::<f64>() / n;
    let numerator = values
        .iter()
        .enumerate()
        .map(|(i, y)| (i as f64 - x_mean) * (y - y_mean))
        .sum::<f64>();
    let denominator = (0..values.len())
        .map(|i| (i as f64 - x_mean).powi(2))
        .sum::<f64>();
    Some(numerator / (denominator + EPS))
}

pub(super) fn rsquare(values: Vec<f64>) -> Option<f64> {
    let s = slope(values.clone())?;
    let n = values.len() as f64;
    let x_mean = (n - 1.0) / 2.0;
    let y_mean = values.iter().sum::<f64>() / n;
    let intercept = y_mean - s * x_mean;
    let ss_tot = values.iter().map(|y| (y - y_mean).powi(2)).sum::<f64>();
    let ss_res = values
        .iter()
        .enumerate()
        .map(|(i, y)| (y - (intercept + s * i as f64)).powi(2))
        .sum::<f64>();
    Some(1.0 - ss_res / (ss_tot + EPS))
}

pub(super) fn resi(values: Vec<f64>) -> Option<f64> {
    let s = slope(values.clone())?;
    let n = values.len() as f64;
    let x_mean = (n - 1.0) / 2.0;
    let y_mean = values.iter().sum::<f64>() / n;
    let intercept = y_mean - s * x_mean;
    let i = values.len() - 1;
    Some(values[i] - (intercept + s * i as f64))
}

fn idx_max(values: Vec<f64>) -> usize {
    values
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|(i, _)| i)
        .unwrap_or(0)
}

fn idx_min(values: Vec<f64>) -> usize {
    values
        .iter()
        .enumerate()
        .min_by(|a, b| a.1.total_cmp(b.1))
        .map(|(i, _)| i)
        .unwrap_or(0)
}

fn corr(values: Vec<(f64, f64)>) -> Option<f64> {
    let n = values.len() as f64;
    let mean_x = values.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = values.iter().map(|(_, y)| y).sum::<f64>() / n;
    let cov = values
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let sx = values
        .iter()
        .map(|(x, _)| (x - mean_x).powi(2))
        .sum::<f64>();
    let sy = values
        .iter()
        .map(|(_, y)| (y - mean_y).powi(2))
        .sum::<f64>();
    Some(cov / ((sx * sy).sqrt() + EPS))
}

fn bool_mean(values: &[Option<bool>], i: usize, d: usize) -> Option<f64> {
    if i + 1 < d {
        return None;
    }
    let start = i + 1 - d;
    let mut sum = 0.0;
    for value in &values[start..=i] {
        sum += if (*value)? { 1.0 } else { 0.0 };
    }
    Some(sum / d as f64)
}

fn sum_positive_ratio(values: &[Option<f64>], i: usize, d: usize, positive: bool) -> Option<f64> {
    if i + 1 < d {
        return None;
    }
    let start = i + 1 - d;
    let mut selected = 0.0;
    let mut total = 0.0;
    for value in &values[start..=i] {
        let value = (*value)?;
        selected += if positive {
            value.max(0.0)
        } else {
            (-value).max(0.0)
        };
        total += value.abs();
    }
    Some(selected / (total + EPS))
}

pub(super) fn resample_bars(bars: Vec<Bar>, _interval: &str, chunk: usize) -> Vec<Bar> {
    let mut out = Vec::new();
    let mut bars = bars;
    bars.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.date.cmp(&b.date)));
    let mut day_start = 0;
    while day_start < bars.len() {
        let key = resample_day_key(&bars[day_start]);
        let day_end = bars[day_start..]
            .iter()
            .position(|bar| resample_day_key(bar) != key)
            .map(|index| day_start + index)
            .unwrap_or(bars.len());
        for group in bars[day_start..day_end].chunks(chunk) {
            if group.len() < chunk {
                continue;
            }
            let first = &group[0];
            let last = &group[group.len() - 1];
            out.push(Bar {
                symbol: first.symbol.clone(),
                // The interval is already part of the FileStore source key.
                // Appending it here made an otherwise RFC3339 timestamp invalid.
                date: last.date.clone(),
                open: first.open,
                high: group
                    .iter()
                    .map(|bar| bar.high)
                    .collect::<Option<Vec<_>>>()
                    .and_then(max_value),
                low: group
                    .iter()
                    .map(|bar| bar.low)
                    .collect::<Option<Vec<_>>>()
                    .and_then(min_value),
                close: last.close,
                volume: group
                    .iter()
                    .map(|bar| bar.volume)
                    .collect::<Option<Vec<_>>>()
                    .map(|v| v.iter().sum()),
                adj_close: last.adj_close,
                amount: None,
                turnover: None,
                vwap: None,
            });
        }
        day_start = day_end;
    }
    out
}

fn resample_day_key(bar: &Bar) -> (&str, &str) {
    (
        &bar.symbol,
        bar.date
            .split(['T', ' '])
            .next()
            .unwrap_or(bar.date.as_str()),
    )
}
