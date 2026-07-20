use anyhow::{Context, Result};
use rig_core::completion::ToolDefinition;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result};

pub const NAME: &str = "read_jin10_csv";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When you need the preflight Jin10 flash feed (stable id/time/content) to shortlist high-relevance macro/news clues for the analysis window. Use before concluding from headlines. If empty, report a data gap rather than inventing items. After analysis, score used items via jin10_attention [{id, score 0.0-1.0}].".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Calendar date of the preflight CSV when not using the latest available, e.g. 2026-07-20."
                },
                "limit": {
                    "type": "integer",
                    "description": "Optional max items when only a shortlist is needed."
                }
            },
            "additionalProperties": true
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Args {
    #[serde(default)]
    pub date: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

pub fn execute(args: Value) -> Result<Value> {
    let tool_args =
        serde_json::from_value::<Args>(args).context("invalid read_jin10_csv arguments")?;
    let rows = if let Some(date) = &tool_args.date {
        orchestrator_core::load_jin10_csv(date)
    } else {
        orchestrator_core::load_jin10_csv_recent(3)
    };
    let result = if rows.is_empty() {
        json!({"error": "no jin10 CSV data available", "hint": "news data may not have been fetched yet"})
    } else {
        let items: Vec<Value> = match tool_args.limit {
            Some(limit) if limit < rows.len() => &rows[..limit],
            _ => &rows,
        }
        .iter()
        .map(|row| {
            json!({
                "id": row.id,
                "time": row.time,
                "content": row.content
            })
        })
        .collect();
        json!({
            "item_count": items.len(),
            "items": items,
            "attention_note": "After analysis, return jin10_attention: [{id, score}] with score 0.0-1.0 for items that influenced your analysis. Only scored items will be persisted."
        })
    };
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}
