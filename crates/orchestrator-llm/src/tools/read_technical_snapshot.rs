use super::{api_tool_name, log_tool_result, ExternalToolConfig, ToolDefinition};
use anyhow::{bail, Context, Result};
use orchestrator_core::technical_csv::{storage_interval, TechnicalCsvRow};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub const NAME: &str = "read_technical_snapshot";
const DEFAULT_INTERVALS: [&str; 3] = ["daily", "3h", "20min"];

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Read compact, deterministic technical signals for one or more tickers from preflight-imported SQLite data. Returns structure, momentum, volatility, coverage and stable signal IDs; use read_technical_detail only when a returned signal needs raw-bar verification.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "tickers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "description": "Canonical ticker symbols to read."
                },
                "intervals": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["daily", "3h", "20min"]},
                    "description": "Optional subset of intervals; defaults to daily, 3h and 20min."
                }
            },
            "required": ["tickers"],
            "additionalProperties": false
        }),
    }
}

#[derive(Debug, Deserialize)]
struct Args {
    tickers: Vec<String>,
    #[serde(default)]
    intervals: Vec<String>,
}

pub fn execute(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let args: Args =
        serde_json::from_value(args).context("invalid read_technical_snapshot arguments")?;
    let tickers = canonical_tickers(args.tickers)?;
    let intervals = canonical_intervals(args.intervals)?;
    let db_path = config
        .db_path
        .as_ref()
        .context("read_technical_snapshot requires the run SQLite path")?;
    let conn = orchestrator_sql::connect(db_path)?;
    let snapshots = tickers
        .iter()
        .map(|ticker| {
            let intervals = intervals
                .iter()
                .map(|interval| {
                    let rows = orchestrator_sql::load_technical_series(&conn, ticker, interval)?;
                    Ok(snapshot_for(ticker, interval, &rows))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({"ticker": ticker, "intervals": intervals}))
        })
        .collect::<Result<Vec<_>>>()?;
    let result = json!({
        "source": "sqlite.technical_bars",
        "snapshots": snapshots,
        "raw_bars_available_via": "read_technical_detail"
    });
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}

fn canonical_tickers(tickers: Vec<String>) -> Result<Vec<String>> {
    let tickers = tickers
        .into_iter()
        .map(|ticker| ticker.trim().to_ascii_uppercase())
        .filter(|ticker| !ticker.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if tickers.is_empty() {
        bail!("read_technical_snapshot requires at least one ticker");
    }
    Ok(tickers)
}

fn canonical_intervals(intervals: Vec<String>) -> Result<Vec<String>> {
    let intervals = if intervals.is_empty() {
        DEFAULT_INTERVALS.iter().map(ToString::to_string).collect()
    } else {
        intervals
    };
    intervals
        .into_iter()
        .map(|interval| {
            storage_interval(&interval)
                .map(ToString::to_string)
                .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval:?}"))
        })
        .collect::<Result<BTreeSet<_>>>()
        .map(|intervals| intervals.into_iter().collect())
}

fn snapshot_for(ticker: &str, interval: &str, rows: &[TechnicalCsvRow]) -> Value {
    let closes = rows
        .iter()
        .filter_map(|row| {
            row.values
                .get("Close")
                .copied()
                .map(|close| (&row.date, close))
        })
        .collect::<Vec<_>>();
    if closes.len() < 2 {
        return json!({
            "interval": interval,
            "status": "data_gap",
            "data_gap": format!("no usable technical series for {ticker} @ {interval}"),
            "coverage": {"bars": rows.len()}
        });
    }

    let window = closes.len().min(20);
    let recent = &closes[closes.len() - window..];
    let (as_of, last_close) = recent[recent.len() - 1];
    let (_, first_close) = recent[0];
    let previous = &recent[..recent.len() - 1];
    let previous_high = previous
        .iter()
        .map(|(_, close)| *close)
        .fold(f64::NEG_INFINITY, f64::max);
    let previous_low = previous
        .iter()
        .map(|(_, close)| *close)
        .fold(f64::INFINITY, f64::min);
    let range_low = recent
        .iter()
        .map(|(_, close)| *close)
        .fold(f64::INFINITY, f64::min);
    let range_high = recent
        .iter()
        .map(|(_, close)| *close)
        .fold(f64::NEG_INFINITY, f64::max);
    let window_return = last_close / first_close - 1.0;
    let structure = if last_close > previous_high {
        "breakout"
    } else if last_close < previous_low {
        "breakdown"
    } else if window_return > 0.01 {
        "uptrend"
    } else if window_return < -0.01 {
        "downtrend"
    } else {
        "range"
    };
    let returns = recent
        .windows(2)
        .map(|pair| pair[1].1 / pair[0].1 - 1.0)
        .collect::<Vec<_>>();
    let volatility = standard_deviation(&returns);
    let range_position = if range_high > range_low {
        (last_close - range_low) / (range_high - range_low)
    } else {
        0.5
    };
    let evidence_rows = recent.iter().map(|(date, _)| *date).collect::<Vec<_>>();
    json!({
        "interval": interval,
        "status": "ok",
        "coverage": {
            "bars": rows.len(),
            "from": closes.first().map(|(date, _)| *date),
            "through": as_of,
            "window_bars": window
        },
        "signals": [
            {
                "signal_id": format!("{ticker}:{interval}:structure:{as_of}"),
                "kind": "structure",
                "label": structure,
                "as_of": as_of,
                "window_return": window_return,
                "range_position": range_position,
                "evidence_rows": evidence_rows
            },
            {
                "signal_id": format!("{ticker}:{interval}:volatility:{as_of}"),
                "kind": "volatility",
                "label": volatility_label(volatility),
                "as_of": as_of,
                "realized_volatility": volatility,
                "evidence_rows": recent.iter().rev().take(5).map(|(date, _)| *date).collect::<Vec<_>>()
            }
        ],
        "latest": {"date": as_of, "close": last_close}
    })
}

fn standard_deviation(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    (values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64)
        .sqrt()
}

fn volatility_label(value: f64) -> &'static str {
    if value >= 0.03 {
        "high"
    } else if value >= 0.015 {
        "elevated"
    } else {
        "normal"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{write_technical_csv, TechnicalCsvRow};
    use std::collections::HashMap;

    #[test]
    fn returns_compact_signals_for_multiple_intervals() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("run.sqlite");
        let csv_path = temp.path().join("qqq_day.csv");
        write_technical_csv(
            &csv_path,
            &[
                TechnicalCsvRow {
                    date: "2026-07-20".into(),
                    values: HashMap::from([("Close".into(), 100.0)]),
                },
                TechnicalCsvRow {
                    date: "2026-07-21".into(),
                    values: HashMap::from([("Close".into(), 104.0)]),
                },
            ],
        )
        .unwrap();
        let mut conn = orchestrator_sql::connect(&db_path).unwrap();
        orchestrator_sql::import_technical_csv(&mut conn, "QQQ", "daily", &csv_path).unwrap();
        drop(conn);
        let result = execute(
            json!({"tickers": ["QQQ"], "intervals": ["daily"]}),
            &ExternalToolConfig {
                db_path: Some(db_path),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            result["snapshots"][0]["intervals"][0]["signals"][0]["label"],
            "breakout"
        );
        assert!(result.to_string().contains("raw_bars_available_via"));
    }
}
