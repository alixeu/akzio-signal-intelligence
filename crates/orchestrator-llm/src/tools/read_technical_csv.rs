use anyhow::{Context, Result};
use rig_core::completion::ToolDefinition;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result};

pub const NAME: &str = "read_technical_csv";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When you need precomputed technical bars and indicator features (price structure, returns, momentum, volatility, volume, correlations) for a ticker and interval before forming a technical conclusion. Intervals: daily/1d, 3h, 20min. Do not invent readings if empty; do not use for news or social evidence.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "Ticker whose technical series is required, e.g. QQQ, TQQQ, VIX, SPY."
                },
                "symbol": {
                    "type": "string",
                    "description": "Alias for ticker when the model emits symbol instead."
                },
                "interval": {
                    "type": "string",
                    "description": "Bar interval for the situation: daily/1d, 3h, or 20min.",
                    "default": "daily"
                },
                "limit": {
                    "type": "integer",
                    "description": "Optional max recent bars when the full series is unnecessary."
                }
            },
            "required": ["ticker"],
            "additionalProperties": true
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Args {
    #[serde(alias = "symbol")]
    pub ticker: String,
    #[serde(default = "default_interval")]
    pub interval: String,
    #[serde(default)]
    pub limit: Option<usize>,
}

fn default_interval() -> String {
    "daily".to_string()
}

pub fn execute(args: Value) -> Result<Value> {
    let tool_args =
        serde_json::from_value::<Args>(args).context("invalid read_technical_csv arguments")?;
    let ticker = tool_args.ticker.trim().to_uppercase();
    let interval =
        orchestrator_core::technical_csv::storage_interval(&tool_args.interval).unwrap_or("daily");
    let csv_dir = orchestrator_core::technical_csv::default_technical_csv_dir();
    let rows = orchestrator_core::technical_csv::technical_csv_path(&csv_dir, &ticker, interval)
        .and_then(|p| orchestrator_core::technical_csv::read_technical_csv(&p).ok())
        .unwrap_or_default();
    let result = if rows.is_empty() {
        json!({"error": format!("no technical CSV data for {} @ {}", ticker, interval)})
    } else {
        let rows = match tool_args.limit {
            Some(limit) if limit < rows.len() => &rows[rows.len() - limit..],
            _ => &rows,
        };
        let entries: Vec<Value> = rows
            .iter()
            .map(|row| {
                let mut obj = serde_json::Map::new();
                obj.insert("date".to_string(), json!(row.date));
                for (key, val) in &row.values {
                    obj.insert(key.clone(), json!(val));
                }
                Value::Object(obj)
            })
            .collect();
        json!({"ticker": ticker, "interval": interval, "bars": rows.len(), "data": entries})
    };
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}
