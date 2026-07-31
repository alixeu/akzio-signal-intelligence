use super::{api_tool_name, log_tool_result, ExternalToolConfig, ToolDefinition};
use anyhow::{bail, Context, Result};
use orchestrator_core::{
    technical_csv::{parse_technical_csv, storage_interval, TechnicalCsvRow},
    ToolId,
};
use orchestrator_store::InputSource;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub const NAME: &str = ToolId::ReadTechnicalSnapshot.as_str();
const DEFAULT_INTERVALS: [&str; 3] = ["daily", "3h", "20min"];

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Read compact, deterministic technical signals for one or more tickers from the Rust-owned run input. Returns structure, momentum, volatility, coverage and stable signal IDs; use read_technical_detail only when a returned signal needs raw-bar verification.".to_string(),
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
    let snapshot = config
        .file_store_input
        .as_ref()
        .context("read_technical_snapshot requires a sealed FileStore input snapshot")?;
    execute_file_store_snapshot(tickers, intervals, snapshot)
}

fn execute_file_store_snapshot(
    tickers: Vec<String>,
    intervals: Vec<String>,
    snapshot: &super::FileStoreInputSnapshot,
) -> Result<Value> {
    let snapshots = tickers
        .iter()
        .map(|ticker| {
            let intervals = intervals
                .iter()
                .map(|interval| {
                    let source = InputSource::technical(ticker, interval)?;
                    let payload = snapshot.read(&source)?;
                    let raw = std::str::from_utf8(&payload)
                        .context("snapshotted technical CSV is not valid UTF-8")?;
                    let rows = parse_technical_csv(raw)?;
                    Ok(snapshot_for(ticker, interval, &rows))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({"ticker": ticker, "intervals": intervals}))
        })
        .collect::<Result<Vec<_>>>()?;
    let result = json!({
        "source": "filestore.run_input.technical",
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
    use orchestrator_store::{capture_run_inputs, write_input_payload, FileStore, RunLocation};

    #[test]
    fn file_store_reader_rejects_direct_input_changed_after_run_binding() {
        let temp = tempfile::tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let source = InputSource::technical("QQQ", "daily").unwrap();
        let before = b"date,Close\n2026-07-20,100\n2026-07-21,104\n";
        write_input_payload(&store, source.clone(), before, "2026-07-27T00:00:00Z").unwrap();
        let location = RunLocation::new("2026-07-27", "run-input-test").unwrap();
        capture_run_inputs(
            &store,
            &location,
            std::slice::from_ref(&source),
            "2026-07-27T00:00:00Z",
        )
        .unwrap();

        // Direct data is not copied into the run. If it changes after the run
        // binds its hash, fail instead of mixing two market-data versions.
        write_input_payload(
            &store,
            source,
            b"date,Close\n2026-07-20,1\n2026-07-21,2\n",
            "2026-07-27T00:01:00Z",
        )
        .unwrap();
        let error = execute(
            json!({"tickers": ["QQQ"], "intervals": ["daily"]}),
            &ExternalToolConfig {
                file_store_input: Some(super::super::FileStoreInputSnapshot {
                    store_root: temp.path().to_path_buf(),
                    run_id: "run-input-test".to_owned(),
                    current_date: "2026-07-27".to_owned(),
                    storage_namespace: None,
                }),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("authoritative metadata"));
    }
}
