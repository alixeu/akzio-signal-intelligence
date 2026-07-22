use super::ToolDefinition;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result, ExternalToolConfig};

pub const NAME: &str = "read_technical_context";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Retrieve preflight-imported technical bars and indicator features from the run SQLite database. You MUST call this tool once per ticker per interval (daily, 3h, 20min) BEFORE forming any technical conclusion. Always fetch all assigned tickers × all three intervals. Do not invent readings; do not use for news evidence.".to_string(),
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

pub fn execute(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let tool_args =
        serde_json::from_value::<Args>(args).context("invalid read_technical_context arguments")?;
    let ticker = tool_args.ticker.trim().to_uppercase();
    let interval =
        orchestrator_core::technical_csv::storage_interval(&tool_args.interval).unwrap_or("daily");
    let db_path = config
        .db_path
        .as_ref()
        .context("read_technical_context requires the run SQLite path")?;
    let conn = orchestrator_sql::connect(db_path)?;
    let rows = orchestrator_sql::load_technical_series(&conn, &ticker, interval)?;
    let result = if rows.is_empty() {
        json!({"error": format!("no SQLite technical data for {} @ {}", ticker, interval)})
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
        json!({"source": "sqlite.technical_bars", "ticker": ticker, "interval": interval, "bars": rows.len(), "data": entries})
    };
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{write_technical_csv, TechnicalCsvRow};
    use std::collections::HashMap;

    #[test]
    fn reads_only_from_configured_sqlite_database() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("run.sqlite");
        let csv_path = temp.path().join("qqq_day.csv");
        write_technical_csv(
            &csv_path,
            &[TechnicalCsvRow {
                date: "2026-07-21".to_string(),
                values: HashMap::from([("Close".to_string(), 500.0)]),
            }],
        )
        .unwrap();
        let mut conn = orchestrator_sql::connect(&db_path).unwrap();
        orchestrator_sql::import_technical_csv(&mut conn, "QQQ", "daily", &csv_path).unwrap();
        drop(conn);
        let config = ExternalToolConfig {
            db_path: Some(db_path),
            ..Default::default()
        };

        let result = execute(json!({"ticker": "QQQ", "interval": "daily"}), &config).unwrap();

        assert_eq!(result["source"], "sqlite.technical_bars");
        assert_eq!(result["bars"], 1);
        assert_eq!(result["data"][0]["Close"], 500.0);
    }
}
