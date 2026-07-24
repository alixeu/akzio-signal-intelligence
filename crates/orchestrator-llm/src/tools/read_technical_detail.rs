use super::{api_tool_name, log_tool_result, ExternalToolConfig, ToolDefinition};
use anyhow::{bail, Context, Result};
use orchestrator_core::technical_csv::storage_interval;
use serde::Deserialize;
use serde_json::{json, Value};

pub const NAME: &str = "read_technical_detail";
const MAX_DETAIL_ROWS: usize = 120;

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Expand raw SQLite technical bars for a specific ticker/interval and bounded date range after a snapshot signal requires verification. This read-only tool returns only stored bars.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "interval": {"type": "string", "enum": ["daily", "3h", "20min"]},
                "start": {"type": "string", "description": "Optional inclusive bar timestamp."},
                "end": {"type": "string", "description": "Optional inclusive bar timestamp."},
                "signal_id": {"type": "string", "description": "Stable signal ID returned by read_technical_snapshot."}
            },
            "required": ["ticker", "interval"],
            "additionalProperties": false
        }),
    }
}

#[derive(Debug, Deserialize)]
struct Args {
    ticker: String,
    interval: String,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
    #[serde(default)]
    signal_id: Option<String>,
}

pub fn execute(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let args: Args =
        serde_json::from_value(args).context("invalid read_technical_detail arguments")?;
    if args.start.is_none() && args.end.is_none() && args.signal_id.is_none() {
        bail!("read_technical_detail requires signal_id or a bounded date range");
    }
    let ticker = args.ticker.trim().to_ascii_uppercase();
    let interval = storage_interval(&args.interval)
        .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {:?}", args.interval))?;
    let db_path = config
        .db_path
        .as_ref()
        .context("read_technical_detail requires the run SQLite path")?;
    let conn = orchestrator_sql::connect(db_path)?;
    let mut rows = orchestrator_sql::load_technical_range(
        &conn,
        &ticker,
        interval,
        args.start.as_deref(),
        args.end.as_deref(),
    )?;
    if rows.len() > MAX_DETAIL_ROWS {
        rows = rows.split_off(rows.len() - MAX_DETAIL_ROWS);
    }
    let data = rows
        .into_iter()
        .map(|row| {
            let mut value = serde_json::Map::new();
            value.insert("date".into(), json!(row.date));
            for (key, number) in row.values {
                value.insert(key, json!(number));
            }
            Value::Object(value)
        })
        .collect::<Vec<_>>();
    let result = if data.is_empty() {
        json!({"status": "data_gap", "ticker": ticker, "interval": interval, "data_gap": "no matching SQLite technical bars"})
    } else {
        json!({"status": "ok", "source": "sqlite.technical_bars", "ticker": ticker, "interval": interval, "signal_id": args.signal_id, "data": data})
    };
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}
