use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, Utc, Weekday};
use clap::Args;
use orchestrator_core::{
    config_int, config_str, config_strings, default_technical_csv_dir, parse_tickers,
    read_technical_csv, technical_csv_path, write_technical_csv, TechnicalCsvRow,
    DEFAULT_TECHNICAL_BARS,
};
use reqwest::header;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration as StdDuration};
use tokio::sync::Mutex;

const EPS: f64 = 1e-12;
const YAHOO_CHART_BASE_URL: &str = "https://query1.finance.yahoo.com/v8/finance/chart";
const YAHOO_CRUMB_URL: &str = "https://query1.finance.yahoo.com/v1/test/getcrumb";
const YAHOO_COOKIE_URL: &str = "https://fc.yahoo.com/";
const USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const PERIODS: [usize; 5] = [5, 10, 20, 30, 60];

#[derive(Debug, Clone)]
pub struct Bar {
    pub symbol: String,
    pub date: String,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume: Option<f64>,
    pub adj_close: Option<f64>,
    pub amount: Option<f64>,
    pub turnover: Option<f64>,
    pub vwap: Option<f64>,
}

#[derive(Clone)]
pub struct YahooDataSource {
    client: reqwest::Client,
    crumb: Arc<Mutex<Option<String>>>,
}

impl YahooDataSource {
    pub fn new(timeout_sec: f64) -> Result<Self> {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_static(USER_AGENT),
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(StdDuration::from_secs_f64(timeout_sec))
                .cookie_store(true)
                .default_headers(headers)
                .build()?,
            crumb: Arc::new(Mutex::new(None)),
        })
    }

    async fn ensure_crumb(&self) -> Result<String> {
        {
            let guard = self.crumb.lock().await;
            if let Some(ref crumb) = *guard {
                return Ok(crumb.clone());
            }
        }
        self.client
            .get(YAHOO_COOKIE_URL)
            .send()
            .await
            .context("failed to fetch Yahoo session cookie")?;
        let crumb = self
            .client
            .get(YAHOO_CRUMB_URL)
            .send()
            .await
            .context("failed to fetch Yahoo crumb")?
            .text()
            .await
            .context("failed to read Yahoo crumb body")?;
        let crumb = crumb.trim().to_string();
        if crumb.is_empty() || crumb.contains("Too Many Requests") {
            bail!("Yahoo crumb endpoint returned unusable value: {crumb:?}");
        }
        let mut guard = self.crumb.lock().await;
        *guard = Some(crumb.clone());
        Ok(crumb)
    }

    async fn fetch_daily_bars(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<Vec<Bar>> {
        self.fetch_bars(symbol, start, end, "1d").await
    }

    async fn fetch_bars(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        interval: &str,
    ) -> Result<Vec<Bar>> {
        let crumb = self.ensure_crumb().await?;
        let provider_symbol = provider_symbol(symbol);
        let mut url = reqwest::Url::parse(YAHOO_CHART_BASE_URL)?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid Yahoo chart base URL"))?
            .push(&provider_symbol);
        // Cap intraday range to Yahoo's rolling ≤60-day window relative to *now*,
        // not only the requested end date. Otherwise historical end-59 can fall
        // outside the live API window and return HTTP 422.
        let (start, end) = match interval {
            "1d" => (start, end),
            _ => {
                let today = Utc::now().date_naive();
                let end = end.min(today);
                let api_floor = today - Duration::days(59);
                let max_start = end - Duration::days(59);
                (start.max(max_start).max(api_floor), end)
            }
        };
        let response = self
            .client
            .get(url)
            .query(&[
                (
                    "period1",
                    start
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                        .and_utc()
                        .timestamp()
                        .to_string(),
                ),
                (
                    "period2",
                    (end + Duration::days(1))
                        .and_hms_opt(0, 0, 0)
                        .unwrap()
                        .and_utc()
                        .timestamp()
                        .to_string(),
                ),
                ("interval", interval.to_string()),
                ("events", "history".to_string()),
                ("includeAdjustedClose", "true".to_string()),
                ("crumb", crumb),
            ])
            .send()
            .await
            .with_context(|| format!("failed to fetch Yahoo chart data for {symbol}"))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let truncated: String = body.chars().take(500).collect();
            bail!("Yahoo chart HTTP {status} for {symbol}: {truncated}");
        }
        parse_yahoo_chart(symbol, response.json::<YahooChartResponse>().await?)
    }
}

fn provider_symbol(symbol: &str) -> String {
    match symbol {
        "VIX" => "^VIX".to_string(),
        other => other.to_string(),
    }
}

#[derive(Debug, Clone, Args, Default)]
pub struct TechnicalArgs {
    #[arg(long)]
    pub symbols: Option<String>,
    #[arg(long)]
    pub start: Option<String>,
    #[arg(long)]
    pub end: Option<String>,
    #[arg(long)]
    pub days: Option<i64>,
    #[arg(long, default_value = "")]
    pub intervals: String,
    #[arg(long)]
    pub timeout: Option<f64>,
    #[arg(long)]
    pub sleep: Option<f64>,
}

pub async fn run(args: TechnicalArgs) -> Result<Value> {
    let args = ResolvedTechnicalArgs::from_args(args)?;
    let source = YahooDataSource::new(args.timeout)?;
    let csv_dir = default_technical_csv_dir();
    let mut results = Vec::new();
    for symbol in &args.symbols {
        for interval in &args.intervals {
            if has_fresh_csv(&csv_dir, symbol, interval, args.start, args.end) {
                results.push(json!({
                    "symbol": symbol,
                    "interval": interval,
                    "bars": 0,
                    "feature_rows": 0,
                    "skipped": "existing_csv"
                }));
                continue;
            }
            if !results.is_empty() && args.sleep > 0.0 {
                tokio::time::sleep(StdDuration::from_secs_f64(args.sleep)).await;
            }
            let bars = match interval.as_str() {
                "1d" => source.fetch_daily_bars(symbol, args.start, args.end).await,
                "3h" => source
                    .fetch_bars(symbol, args.start, args.end, "1h")
                    .await
                    .map(|bars| resample_bars(bars, "3h", 3)),
                "20min" => source
                    .fetch_bars(symbol, args.start, args.end, "5m")
                    .await
                    .map(|bars| resample_bars(bars, "20min", 4)),
                other => Err(anyhow::anyhow!(
                    "unsupported interval {other:?}; use 1d, 3h, 20min"
                )),
            };
            let bars = match bars {
                Ok(bars) => bars,
                Err(error) => {
                    results.push(json!({
                        "symbol": symbol,
                        "interval": interval,
                        "bars": 0,
                        "feature_rows": 0,
                        "status": "error",
                        "error": error.to_string(),
                    }));
                    continue;
                }
            };
            let rows = feature_rows(interval, &bars);
            let mut rows = rows;
            let keep = DEFAULT_TECHNICAL_BARS;
            if rows.len() > keep {
                rows = rows.split_off(rows.len() - keep);
            }
            let csv_rows: Vec<TechnicalCsvRow> = rows
                .iter()
                .map(|row| TechnicalCsvRow {
                    date: row.date.clone(),
                    values: row
                        .features
                        .iter()
                        .filter_map(|(k, v)| v.filter(|v| v.is_finite()).map(|v| (k.clone(), v)))
                        .collect(),
                })
                .filter(|row| !row.values.is_empty())
                .collect();
            if let Some(csv_path) = technical_csv_path(&csv_dir, symbol, interval) {
                let _ = write_technical_csv(&csv_path, &csv_rows);
            }
            results.push(json!({
                "symbol": symbol,
                "interval": interval,
                "bars": bars.len(),
                "feature_rows": csv_rows.len(),
            }));
        }
    }
    Ok(json!({
        "status": "success",
        "source": "Yahoo",
        "start": args.start.to_string(),
        "end": args.end.to_string(),
        "output_dir": csv_dir.display().to_string(),
        "bars": DEFAULT_TECHNICAL_BARS,
        "symbols": args.symbols,
        "intervals": args.intervals,
        "results": results
    }))
}

#[derive(Debug, Clone)]
struct ResolvedTechnicalArgs {
    symbols: Vec<String>,
    start: NaiveDate,
    end: NaiveDate,
    intervals: Vec<String>,
    timeout: f64,
    sleep: f64,
}

impl ResolvedTechnicalArgs {
    fn from_args(args: TechnicalArgs) -> Result<Self> {
        let config = crate::config::load_default_config();
        let end = match args.end {
            Some(value) => NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                .with_context(|| format!("invalid --end date {value:?}"))?,
            None => Utc::now().date_naive(),
        };
        let days = args
            .days
            .unwrap_or_else(|| config_int(&config, "technical.days", 60));
        let start = match args.start {
            Some(value) => NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                .with_context(|| format!("invalid --start date {value:?}"))?,
            None => end - Duration::days(days),
        };
        let symbols = args
            .symbols
            .map(|s| parse_tickers(&s))
            .unwrap_or_else(|| config_strings(&config, "orchestrator.analysis_universe", &[]));
        if symbols.is_empty() {
            bail!("no symbols configured");
        }
        let intervals = if args.intervals.trim().is_empty() {
            config_str(&config, "technical.intervals", "1d,3h,20min")
        } else {
            args.intervals
        }
        .split(',')
        .map(|item| item.trim().to_lowercase())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();
        if intervals.is_empty() {
            bail!("no intervals configured");
        }
        Self {
            symbols,
            start,
            end,
            intervals,
            timeout: args.timeout.unwrap_or(20.0),
            sleep: args
                .sleep
                .unwrap_or_else(|| config_int(&config, "technical.sleep_sec", 1) as f64),
        }
        .validate()
    }

    fn validate(self) -> Result<Self> {
        Ok(self)
    }
}

#[derive(Debug, Clone)]
struct FeatureRow {
    #[allow(dead_code)]
    symbol: String,
    date: String,
    #[allow(dead_code)]
    interval: String,
    features: HashMap<String, Option<f64>>,
}

fn feature_rows(interval: &str, bars: &[Bar]) -> Vec<FeatureRow> {
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
        out.extend(feature_rows_for_symbol(interval, &bars[start..end]));
        start = end;
    }
    out
}

fn feature_rows_for_symbol(interval: &str, bars: &[Bar]) -> Vec<FeatureRow> {
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
            symbol: bars[i].symbol.clone(),
            date: bars[i].date.clone(),
            interval: interval.to_string(),
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
        ref_value(close, i, d).zip(c).map(|(r, c)| r / (c + EPS)),
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

fn adjusted_price(value: Option<f64>, close: Option<f64>, adj_close: Option<f64>) -> Option<f64> {
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

fn slope(values: Vec<f64>) -> Option<f64> {
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

fn rsquare(values: Vec<f64>) -> Option<f64> {
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

fn resi(values: Vec<f64>) -> Option<f64> {
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

fn resample_bars(bars: Vec<Bar>, interval: &str, chunk: usize) -> Vec<Bar> {
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
                date: format!("{}:{interval}", last.date),
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

fn has_fresh_csv(
    csv_dir: &std::path::Path,
    symbol: &str,
    interval: &str,
    _start: NaiveDate,
    end: NaiveDate,
) -> bool {
    let Some(path) = technical_csv_path(csv_dir, symbol, interval) else {
        return false;
    };
    let rows = match read_technical_csv(&path) {
        Ok(rows) => rows,
        Err(_) => return false,
    };
    let Some(last) = rows.last() else {
        return false;
    };
    let latest_day = last.date.get(..10).unwrap_or(last.date.as_str());
    let Ok(latest_date) = NaiveDate::parse_from_str(latest_day, "%Y-%m-%d") else {
        return false;
    };
    let required_end = match end.weekday() {
        Weekday::Sat => end - Duration::days(1),
        Weekday::Sun => end - Duration::days(2),
        _ => end,
    };
    latest_date >= required_end
}

#[derive(Debug, Deserialize)]
struct YahooChartResponse {
    chart: YahooChart,
}

#[derive(Debug, Deserialize)]
struct YahooChart {
    result: Option<Vec<YahooChartResult>>,
    error: Option<YahooChartError>,
}

#[derive(Debug, Deserialize)]
struct YahooChartError {
    code: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct YahooChartResult {
    timestamp: Option<Vec<i64>>,
    indicators: YahooIndicators,
}

#[derive(Debug, Deserialize)]
struct YahooIndicators {
    quote: Vec<YahooQuote>,
    adjclose: Option<Vec<YahooAdjClose>>,
}

#[derive(Debug, Deserialize)]
struct YahooQuote {
    open: Option<Vec<Option<f64>>>,
    high: Option<Vec<Option<f64>>>,
    low: Option<Vec<Option<f64>>>,
    close: Option<Vec<Option<f64>>>,
    volume: Option<Vec<Option<f64>>>,
}

#[derive(Debug, Deserialize)]
struct YahooAdjClose {
    adjclose: Option<Vec<Option<f64>>>,
}

fn parse_yahoo_chart(symbol: &str, response: YahooChartResponse) -> Result<Vec<Bar>> {
    if let Some(error) = response.chart.error {
        bail!(
            "Yahoo chart error for {symbol}: {} {}",
            error.code.unwrap_or_default(),
            error.description.unwrap_or_default()
        );
    }
    let mut results = response
        .chart
        .result
        .with_context(|| format!("Yahoo chart result missing for {symbol}"))?;
    let result = results
        .pop()
        .with_context(|| format!("Yahoo chart result empty for {symbol}"))?;
    let timestamps = result
        .timestamp
        .with_context(|| format!("Yahoo chart timestamps missing for {symbol}"))?;
    let quote = result
        .indicators
        .quote
        .into_iter()
        .next()
        .with_context(|| format!("Yahoo chart quote missing for {symbol}"))?;
    let adjclose = result
        .indicators
        .adjclose
        .and_then(|values| values.into_iter().next())
        .and_then(|item| item.adjclose)
        .unwrap_or_default();
    let mut bars = timestamps
        .into_iter()
        .enumerate()
        .map(|(index, timestamp)| Bar {
            symbol: symbol.to_string(),
            date: timestamp_to_date(timestamp),
            open: value_at(quote.open.as_deref(), index),
            high: value_at(quote.high.as_deref(), index),
            low: value_at(quote.low.as_deref(), index),
            close: value_at(quote.close.as_deref(), index),
            volume: value_at(quote.volume.as_deref(), index),
            adj_close: value_at(Some(&adjclose), index),
            amount: None,
            turnover: None,
            vwap: None,
        })
        .collect::<Vec<_>>();
    bars.sort_by(|a, b| a.date.cmp(&b.date));
    Ok(bars)
}

fn value_at(values: Option<&[Option<f64>]>, index: usize) -> Option<f64> {
    values?.get(index).copied().flatten()
}

fn timestamp_to_date(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|value| value.naive_utc().to_string())
        .unwrap_or_else(|| timestamp.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(i: usize, close: Option<f64>) -> Bar {
        Bar {
            symbol: "QQQ".to_string(),
            date: format!("2026-01-{i:02}"),
            open: close,
            high: close.map(|v| v + 1.0),
            low: close.map(|v| v - 1.0),
            close,
            volume: Some(100.0 + i as f64),
            adj_close: close,
            amount: None,
            turnover: None,
            vwap: None,
        }
    }

    #[test]
    fn adjusted_price_uses_adj_close_factor() {
        let value = adjusted_price(Some(10.0), Some(20.0), Some(30.0)).unwrap();
        assert!((value - 15.0).abs() < 1e-9);
        assert_eq!(adjusted_price(Some(10.0), Some(20.0), None), Some(10.0));
    }

    #[test]
    fn parses_yahoo_chart_values_oldest_first() {
        let response: YahooChartResponse = serde_json::from_value(json!({
            "chart": {
                "result": [{
                    "timestamp": [1782000000, 1781913600],
                    "indicators": {
                        "quote": [{
                            "open": [2.0, 1.0],
                            "high": [3.0, 2.0],
                            "low": [1.0, 0.5],
                            "close": [2.5, 1.5],
                            "volume": [20.0, null]
                        }],
                        "adjclose": [{"adjclose": [2.4, 1.4]}]
                    }
                }],
                "error": null
            }
        }))
        .unwrap();
        let bars = parse_yahoo_chart("QQQ", response).unwrap();
        assert!(bars[0].date < bars[1].date);
        assert_eq!(bars[0].volume, None);
        assert_eq!(bars[1].close, Some(2.5));
        assert_eq!(bars[1].adj_close, Some(2.4));
    }

    #[test]
    fn windows_emit_none_until_ready_or_when_null() {
        let bars = (1..=6)
            .map(|i| bar(i, if i == 3 { None } else { Some(i as f64) }))
            .collect::<Vec<_>>();
        let rows = feature_rows_for_symbol("1d", &bars);
        assert_eq!(rows[3].features["MA5"], None);
        assert!(rows[5].features["Return"].is_some());
    }

    #[test]
    fn regression_helpers_work_on_line() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((slope(values.clone()).unwrap() - 1.0).abs() < 1e-9);
        assert!((rsquare(values.clone()).unwrap() - 1.0).abs() < 1e-9);
        assert!(resi(values).unwrap().abs() < 1e-9);
    }

    #[test]
    fn resample_groups_intraday_bars() {
        let bars = (1..=6)
            .map(|i| Bar {
                date: format!("2026-01-01T{i:02}:00:00"),
                ..bar(i, Some(i as f64))
            })
            .collect();
        let sampled = resample_bars(bars, "3h", 3);
        assert_eq!(sampled.len(), 2);
        assert_eq!(sampled[0].open, Some(1.0));
        assert_eq!(sampled[0].close, Some(3.0));
        assert_eq!(sampled[0].volume, Some(306.0));
    }
}
