use anyhow::{bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, Utc, Weekday};
use clap::Args;
use futures::{stream, StreamExt};
use orchestrator_core::{
    config_bool, config_int, config_str, config_strings, default_technical_csv_dir, load_config,
    parse_tickers, project_path, read_technical_csv, render_technical_csv, technical_csv_path,
    TechnicalCsvRow, DEFAULT_TECHNICAL_BARS,
};
use reqwest::header;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{path::Path, sync::Arc, time::Duration as StdDuration};
use tokio::sync::Mutex;

mod indicators;

use indicators::{feature_rows, resample_bars};
const ALPACA_BARS_BASE_URL: &str = "https://data.alpaca.markets/v2/stocks";
const YAHOO_CHART_BASE_URL: &str = "https://query1.finance.yahoo.com/v8/finance/chart";
const YAHOO_CRUMB_URL: &str = "https://query1.finance.yahoo.com/v1/test/getcrumb";
const YAHOO_COOKIE_URL: &str = "https://fc.yahoo.com/";
const USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

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

#[derive(Clone)]
struct AlpacaDataSource {
    client: reqwest::Client,
    api_key: String,
    api_secret: String,
    feed: String,
}

impl AlpacaDataSource {
    fn new(timeout_sec: f64, api_key: String, api_secret: String, feed: String) -> Result<Self> {
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            bail!("Alpaca technical source requires ALPACA_API_KEY and ALPACA_API_SECRET");
        }
        if !matches!(feed.as_str(), "iex" | "sip" | "boats" | "otc") {
            bail!("unsupported Alpaca stock feed {feed:?}; use iex, sip, boats, or otc");
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(StdDuration::from_secs_f64(timeout_sec))
                .build()?,
            api_key,
            api_secret,
            feed,
        })
    }

    async fn fetch_bars(
        &self,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
        timeframe: &str,
    ) -> Result<Vec<Bar>> {
        let mut bars = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut query = vec![
                ("timeframe", timeframe.to_string()),
                ("start", format!("{start}T00:00:00Z")),
                ("end", format!("{}T00:00:00Z", end + Duration::days(1))),
                ("limit", "10000".to_string()),
                ("adjustment", "all".to_string()),
                ("feed", self.feed.clone()),
                ("sort", "asc".to_string()),
            ];
            if let Some(token) = page_token.as_ref() {
                query.push(("page_token", token.clone()));
            }
            let response = self
                .client
                .get(format!("{ALPACA_BARS_BASE_URL}/{symbol}/bars"))
                .header("APCA-API-KEY-ID", &self.api_key)
                .header("APCA-API-SECRET-KEY", &self.api_secret)
                .query(&query)
                .send()
                .await
                .with_context(|| format!("failed to fetch Alpaca bars for {symbol}"))?;
            let status = response.status();
            let text = response
                .text()
                .await
                .with_context(|| format!("failed to read Alpaca bars response for {symbol}"))?;
            if !status.is_success() {
                bail!(
                    "Alpaca bars HTTP {status} for {symbol}: {}",
                    text.chars().take(500).collect::<String>()
                );
            }
            let page: AlpacaBarsResponse = serde_json::from_str(&text)
                .with_context(|| format!("invalid Alpaca bars response for {symbol}"))?;
            bars.extend(parse_alpaca_bars(symbol, page.bars.unwrap_or_default()));
            page_token = page.next_page_token.filter(|token| !token.is_empty());
            if page_token.is_none() {
                break;
            }
        }
        Ok(bars)
    }
}

#[derive(Clone)]
struct TechnicalSources {
    default: String,
    alpaca: Option<AlpacaDataSource>,
    yahoo: YahooDataSource,
    yahoo_fallback_symbols: Vec<String>,
}

impl TechnicalSources {
    fn uses_yahoo(&self, symbol: &str) -> bool {
        self.default == "yahoo"
            || self
                .yahoo_fallback_symbols
                .iter()
                .any(|fallback| fallback.eq_ignore_ascii_case(symbol))
    }
}

#[derive(Debug, Clone, Args, Default)]
pub struct TechnicalArgs {
    #[arg(long)]
    pub source: Option<String>,
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
    /// Maximum number of independent ticker/interval downloads in flight.
    #[arg(long)]
    pub parallelism: Option<usize>,
}

pub async fn run(args: TechnicalArgs) -> Result<Value> {
    let args = ResolvedTechnicalArgs::from_args(args)?;
    let yahoo = YahooDataSource::new(args.timeout)?;
    let alpaca = (args.source == "alpaca").then(|| {
        AlpacaDataSource::new(
            args.timeout,
            args.alpaca_api_key.clone(),
            args.alpaca_api_secret.clone(),
            args.alpaca_feed.clone(),
        )
    });
    let alpaca = alpaca.transpose()?;
    let sources = TechnicalSources {
        default: args.source.clone(),
        alpaca,
        yahoo,
        yahoo_fallback_symbols: args.yahoo_fallback_symbols.clone(),
    };
    let csv_dir = default_technical_csv_dir();
    let mut results = Vec::new();
    let mut jobs = Vec::new();
    let mut order = 0usize;
    for symbol in &args.symbols {
        for interval in &args.intervals {
            let job_order = order;
            order += 1;
            if args.source == "yahoo"
                && has_fresh_csv(&csv_dir, symbol, interval, args.start, args.end)
            {
                results.push((
                    job_order,
                    json!({
                        "symbol": symbol,
                        "interval": interval,
                        "bars": 0,
                        "feature_rows": 0,
                        "skipped": "existing_csv"
                    }),
                ));
            } else {
                jobs.push((job_order, symbol.clone(), interval.clone()));
            }
        }
    }

    let mut failures = Vec::new();
    for (batch_index, batch) in jobs.chunks(args.parallelism).enumerate() {
        if batch_index > 0 && args.sleep > 0.0 {
            tokio::time::sleep(StdDuration::from_secs_f64(args.sleep)).await;
        }
        let completed = stream::iter(batch.iter().cloned().map(|(job_order, symbol, interval)| {
            let sources = sources.clone();
            let csv_dir = csv_dir.clone();
            async move {
                let result = download_technical_csv(
                    &sources, &csv_dir, &symbol, &interval, args.start, args.end,
                )
                .await;
                (job_order, symbol, interval, result)
            }
        }))
        .buffer_unordered(args.parallelism)
        .collect::<Vec<_>>()
        .await;
        for (job_order, symbol, interval, result) in completed {
            match result {
                Ok(result) => results.push((job_order, result)),
                Err(error) => {
                    if let Some((rows, csv_path)) =
                        cached_technical_rows(&csv_dir, &symbol, &interval)
                    {
                        results.push((
                            job_order,
                            json!({
                                    "symbol": symbol,
                                    "interval": interval,
                                    "source": "cached_csv",
                                "bars": rows,
                                "feature_rows": rows,
                                "status": "fallback",
                                "cache_path": csv_path.display().to_string(),
                                "error": error.to_string(),
                            }),
                        ));
                    } else {
                        failures.push(format!("{symbol}/{interval}: {error:#}"));
                        results.push((
                            job_order,
                            json!({
                                "symbol": symbol,
                                "interval": interval,
                                "bars": 0,
                                "feature_rows": 0,
                                "status": "error",
                                "error": error.to_string(),
                            }),
                        ));
                    }
                }
            }
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} technical refresh incomplete; refusing success with missing coverage: {}",
            args.source,
            failures.join("; ")
        );
    }
    results.sort_by_key(|(job_order, _)| *job_order);
    Ok(json!({
        "status": "success",
        "source": args.source,
        "alpaca_feed": (args.source == "alpaca").then_some(args.alpaca_feed),
        "extended_hours": (args.source == "alpaca").then_some(args.extended_hours),
        "yahoo_fallback_symbols": args.yahoo_fallback_symbols,
        "start": args.start.to_string(),
        "end": args.end.to_string(),
        "output_dir": csv_dir.display().to_string(),
        "bars": DEFAULT_TECHNICAL_BARS,
        "symbols": args.symbols,
        "intervals": args.intervals,
        "results": results.into_iter().map(|(_, result)| result).collect::<Vec<_>>(),
        "parallelism": args.parallelism
    }))
}

async fn download_technical_csv(
    sources: &TechnicalSources,
    csv_dir: &Path,
    symbol: &str,
    interval: &str,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<Value> {
    let (bars, provider) = if sources.uses_yahoo(symbol) {
        let bars = match interval {
            "1d" => sources.yahoo.fetch_daily_bars(symbol, start, end).await,
            "3h" => sources
                .yahoo
                .fetch_bars(symbol, start, end, "1h")
                .await
                .map(|bars| resample_bars(bars, "3h", 3)),
            "20min" => sources
                .yahoo
                .fetch_bars(symbol, start, end, "5m")
                .await
                .map(|bars| resample_bars(bars, "20min", 4)),
            other => Err(anyhow::anyhow!(
                "unsupported interval {other:?}; use 1d, 3h, 20min"
            )),
        }?;
        (bars, "yahoo")
    } else {
        let alpaca = sources
            .alpaca
            .as_ref()
            .context("Alpaca technical source is not configured")?;
        let timeframe = match interval {
            "1d" => "1Day",
            "3h" => "3Hour",
            "20min" => "20Min",
            other => {
                bail!("unsupported interval {other:?}; use 1d, 3h, 20min");
            }
        };
        (
            alpaca.fetch_bars(symbol, start, end, timeframe).await?,
            "alpaca",
        )
    };
    let bars_len = bars.len();
    let mut rows = feature_rows(&bars);
    if rows.len() > DEFAULT_TECHNICAL_BARS {
        rows = rows.split_off(rows.len() - DEFAULT_TECHNICAL_BARS);
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
    if csv_rows.is_empty() {
        bail!("{provider} returned no usable finite feature rows");
    }
    let csv_path = technical_csv_path(csv_dir, symbol, interval)
        .ok_or_else(|| anyhow::anyhow!("unsupported interval {interval:?}"))?;
    orchestrator_store::publish_bytes_atomic(&csv_path, render_technical_csv(&csv_rows).as_bytes())
        .with_context(|| {
            format!(
                "failed to persist {provider} technical data for {symbol}/{interval} to {}",
                csv_path.display()
            )
        })?;
    Ok(json!({
        "symbol": symbol,
        "interval": interval,
        "source": provider,
        "bars": bars_len,
        "feature_rows": csv_rows.len(),
    }))
}

#[derive(Debug, Clone)]
struct ResolvedTechnicalArgs {
    source: String,
    symbols: Vec<String>,
    start: NaiveDate,
    end: NaiveDate,
    intervals: Vec<String>,
    timeout: f64,
    sleep: f64,
    parallelism: usize,
    alpaca_api_key: String,
    alpaca_api_secret: String,
    alpaca_feed: String,
    extended_hours: bool,
    yahoo_fallback_symbols: Vec<String>,
}

impl ResolvedTechnicalArgs {
    fn from_args(args: TechnicalArgs) -> Result<Self> {
        let config = load_config(Some(&project_path("config/config.yaml")))
            .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));
        let source = args
            .source
            .unwrap_or_else(|| config_str(&config, "technical.source", "alpaca"))
            .trim()
            .to_ascii_lowercase();
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
        if args.parallelism == Some(0) {
            bail!("--parallelism must be at least 1");
        }
        Self {
            source,
            symbols,
            start,
            end,
            intervals,
            timeout: args.timeout.unwrap_or(20.0),
            sleep: args
                .sleep
                .unwrap_or_else(|| config_int(&config, "technical.sleep_sec", 1) as f64),
            parallelism: args.parallelism.unwrap_or_else(|| {
                config_int(&config, "technical.parallelism", 10).max(1) as usize
            }),
            alpaca_api_key: config_str(&config, "orchestrator.alpaca.api_key", ""),
            alpaca_api_secret: config_str(&config, "orchestrator.alpaca.api_secret", ""),
            alpaca_feed: config_str(&config, "technical.alpaca.feed", "iex")
                .trim()
                .to_ascii_lowercase(),
            extended_hours: config_bool(&config, "technical.alpaca.extended_hours", true),
            yahoo_fallback_symbols: config_strings(
                &config,
                "technical.yahoo_fallback_symbols",
                &["VIX"],
            ),
        }
        .validate()
    }

    fn validate(self) -> Result<Self> {
        if !matches!(self.source.as_str(), "alpaca" | "yahoo") {
            bail!(
                "unsupported technical source {:?}; use alpaca or yahoo",
                self.source
            );
        }
        if self.source == "alpaca" && !self.extended_hours {
            bail!("technical.alpaca.extended_hours must be true for this workflow");
        }
        Ok(self)
    }
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

fn cached_technical_rows(
    csv_dir: &std::path::Path,
    symbol: &str,
    interval: &str,
) -> Option<(usize, std::path::PathBuf)> {
    let path = technical_csv_path(csv_dir, symbol, interval)?;
    match read_technical_csv(&path) {
        Ok(rows) if !rows.is_empty() => Some((rows.len(), path)),
        Ok(_) => None,
        Err(_) => None,
    }
}

#[derive(Debug, Deserialize)]
struct AlpacaBarsResponse {
    bars: Option<Vec<AlpacaBar>>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AlpacaBar {
    t: String,
    o: Option<f64>,
    h: Option<f64>,
    l: Option<f64>,
    c: Option<f64>,
    v: Option<f64>,
    vw: Option<f64>,
}

fn parse_alpaca_bars(symbol: &str, bars: Vec<AlpacaBar>) -> Vec<Bar> {
    bars.into_iter()
        .map(|bar| Bar {
            symbol: symbol.to_string(),
            date: bar.t,
            open: bar.o,
            high: bar.h,
            low: bar.l,
            close: bar.c,
            volume: bar.v,
            adj_close: bar.c,
            amount: None,
            turnover: None,
            vwap: bar.vw,
        })
        .collect()
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
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| timestamp.to_string())
}

#[cfg(test)]
mod tests {
    use super::indicators::{adjusted_price, feature_rows_for_symbol, resi, rsquare, slope};
    use super::*;

    #[test]
    fn yahoo_timestamp_retains_its_utc_timezone() {
        assert_eq!(timestamp_to_date(0), "1970-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn unsupported_interval_is_an_error_not_top_level_success() {
        let error = run(TechnicalArgs {
            source: Some("yahoo".to_string()),
            symbols: Some("QQQ".to_string()),
            start: Some("2026-07-01".to_string()),
            end: Some("2026-07-02".to_string()),
            days: None,
            intervals: "unsupported".to_string(),
            timeout: Some(1.0),
            sleep: Some(0.0),
            parallelism: None,
        })
        .await
        .unwrap_err();

        assert!(error.to_string().contains("refusing success"));
        assert!(error.to_string().contains("unsupported interval"));
    }

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
    fn parses_alpaca_extended_hours_bars() {
        let response: AlpacaBarsResponse = serde_json::from_value(json!({
            "bars": [
                {"t": "2026-07-22T08:00:00Z", "o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 10.0, "vw": 1.2},
                {"t": "2026-07-22T21:00:00Z", "o": 1.5, "h": 2.5, "l": 1.0, "c": 2.0, "v": 20.0, "vw": 1.8}
            ],
            "next_page_token": null
        }))
        .unwrap();
        let bars = parse_alpaca_bars("QQQ", response.bars.unwrap());
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].date, "2026-07-22T08:00:00Z");
        assert_eq!(bars[1].date, "2026-07-22T21:00:00Z");
        assert_eq!(bars[1].vwap, Some(1.8));
    }

    #[test]
    fn windows_emit_none_until_ready_or_when_null() {
        let bars = (1..=6)
            .map(|i| bar(i, if i == 3 { None } else { Some(i as f64) }))
            .collect::<Vec<_>>();
        let rows = feature_rows_for_symbol(&bars);
        assert_eq!(rows[3].features["MA5"], None);
        assert!(rows[5].features["Return"].is_some());
    }

    #[test]
    fn roc_is_a_signed_period_return_and_rejects_zero_reference() {
        let rising = [100.0, 104.0, 108.0, 112.0, 116.0, 120.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| bar(index + 1, Some(close)))
            .collect::<Vec<_>>();
        let rising_rows = feature_rows_for_symbol(&rising);
        assert!((rising_rows[5].features["ROC5"].unwrap() - 0.2).abs() < 1e-9);

        let falling = [120.0, 116.0, 112.0, 108.0, 104.0, 100.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| bar(index + 1, Some(close)))
            .collect::<Vec<_>>();
        let falling_rows = feature_rows_for_symbol(&falling);
        assert!((falling_rows[5].features["ROC5"].unwrap() + 1.0 / 6.0).abs() < 1e-9);

        let zero_reference = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
            .into_iter()
            .enumerate()
            .map(|(index, close)| bar(index + 1, Some(close)))
            .collect::<Vec<_>>();
        assert_eq!(
            feature_rows_for_symbol(&zero_reference)[5].features["ROC5"],
            None
        );
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
        assert_eq!(sampled[0].date, "2026-01-01T03:00:00");
    }
}
