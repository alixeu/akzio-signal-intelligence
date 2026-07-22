use super::ToolDefinition;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result};

pub const NAME: &str = "read_technical_csv";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Retrieve precomputed technical bars and indicator features for a ticker at a given interval. You MUST call this tool once per ticker per interval (daily, 3h, 20min) BEFORE forming any technical conclusion. Always fetch all assigned tickers × all three intervals. Do not invent readings; do not use for news or social evidence.".to_string(),
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
                    "description": "Bar interval: daily, 3h, or 20min. You must fetch all three intervals per ticker.",
                    "enum": ["daily", "3h", "20min"]
                }
            },
            "required": ["ticker", "interval"],
            "additionalProperties": false
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Args {
    #[serde(alias = "symbol")]
    pub ticker: String,
    #[serde(default = "default_interval")]
    pub interval: String,
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
