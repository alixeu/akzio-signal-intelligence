use anyhow::{Context, Result};
use rig_core::completion::ToolDefinition;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result, tool_connection, ExternalToolConfig};
use crate::agent_loop::ToolRuntimeTurnContext;
use orchestrator_ingest::jin10;

pub const NAME: &str = "fetch_jin10_flash";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When preflight Jin10 CSV/SQLite is missing or stale and the role is allowed to refresh the live Jin10 flash feed into storage. Prefer the precomputed CSV when it already covers the window.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "lookback_hours": {
                    "type": "number",
                    "description": "How many hours of flash items to pull when refreshing."
                },
                "pages": {
                    "type": "integer",
                    "description": "Optional page cap for the live fetch."
                },
                "classify": {
                    "type": "string",
                    "description": "Optional Jin10 classify filter string."
                }
            },
            "additionalProperties": true
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Args {
    #[serde(default)]
    pub lookback_hours: Option<f64>,
    #[serde(default)]
    pub pages: Option<usize>,
    #[serde(default)]
    pub classify: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
}

impl Args {
    fn to_ingest_args(&self) -> jin10::Jin10Args {
        jin10::Jin10Args {
            lookback_hours: self.lookback_hours,
            pages: self.pages,
            classify: self.classify.clone(),
            channel: None,
            vip: None,
            sleep: None,
            timeout: None,
            output: String::new(),
            jsonl: String::new(),
            pretty: false,
        }
    }
}

pub async fn execute(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    let tool_args =
        serde_json::from_value::<Args>(args).context("invalid fetch_jin10_flash arguments")?;
    let ingest_args = tool_args.to_ingest_args();
    let mut result = jin10::run(ingest_args)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut conn = tool_connection(config)?;
    let imported = orchestrator_sql::import_jin10_payload(&mut conn, &result)?;
    let jin10_context = orchestrator_sql::read_run_context(
        &mut conn,
        &orchestrator_sql::RunContextReadRequest {
            kind: "jin10".to_string(),
            run_id: turn_context.map(|context| context.run_id.clone()),
            ticker: config.tickers.first().cloned(),
            tickers: config.tickers.clone(),
            phase: None,
            role: turn_context.map(|context| context.role.clone()),
            topic_id: None,
            turn_id: turn_context.map(|context| context.turn_id.clone()),
            persist_context: false,
            token_budget: None,
        },
    )?;
    if let Some(object) = result.as_object_mut() {
        object.remove("items");
        object.insert("imported_rows".to_string(), json!(imported));
        object.insert("jin10_context".to_string(), jin10_context);
    }
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}
