use anyhow::{Context, Result};
use rig_core::completion::ToolDefinition;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result, tool_connection, ExternalToolConfig};
use crate::agent_loop::ToolRuntimeTurnContext;
use orchestrator_ingest::technical;

pub const NAME: &str = "run_technical_indicators";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When precomputed technical bars are missing for needed symbols/intervals and the role is allowed to recompute and import indicators. Prefer the preflight technical CSV when it already covers the analysis window.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "symbols": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Symbols that need indicator recompute/import."
                },
                "tickers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Alias for symbols."
                },
                "intervals": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Intervals to compute, e.g. daily, 3h, 20min."
                },
                "start": {
                    "type": "string",
                    "description": "Optional range start."
                },
                "end": {
                    "type": "string",
                    "description": "Optional range end."
                },
                "days": {
                    "type": "integer",
                    "description": "Optional lookback days when start/end are omitted."
                },
                "model": {
                    "type": "string",
                    "description": "Optional model/config tag for the compute job."
                }
            },
            "additionalProperties": true
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Args {
    #[serde(default, alias = "tickers")]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub intervals: Vec<String>,
    #[serde(default)]
    pub start: Option<String>,
    #[serde(default)]
    pub end: Option<String>,
    #[serde(default)]
    pub days: Option<i64>,
}

impl Args {
    fn to_ingest_args(&self) -> technical::TechnicalArgs {
        technical::TechnicalArgs {
            symbols: if self.symbols.is_empty() {
                None
            } else {
                Some(self.symbols.join(","))
            },
            intervals: self.intervals.join(","),
            start: self.start.clone(),
            end: self.end.clone(),
            days: self.days,
            timeout: None,
            sleep: None,
        }
    }
}

pub async fn execute(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    let tool_args = serde_json::from_value::<Args>(args)
        .context("invalid run_technical_indicators arguments")?;
    let ingest_args = tool_args.to_ingest_args();
    let mut result = technical::run(ingest_args)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let tickers = if tool_args.symbols.is_empty() {
        config.tickers.clone()
    } else {
        tool_args.symbols.clone()
    };
    let mut conn = tool_connection(config)?;
    let technical_context = orchestrator_sql::read_run_context(
        &mut conn,
        &orchestrator_sql::RunContextReadRequest {
            kind: "technical".to_string(),
            run_id: turn_context.map(|context| context.run_id.clone()),
            ticker: tickers.first().cloned(),
            tickers,
            phase: None,
            role: turn_context.map(|context| context.role.clone()),
            topic_id: None,
            turn_id: turn_context.map(|context| context.turn_id.clone()),
            persist_context: false,
            token_budget: None,
        },
    )?;
    if let Some(object) = result.as_object_mut() {
        object.insert("technical_context".to_string(), technical_context);
    }
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}
