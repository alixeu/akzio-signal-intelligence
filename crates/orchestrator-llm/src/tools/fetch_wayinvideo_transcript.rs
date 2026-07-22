use super::ToolDefinition;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result};
use orchestrator_ingest::wayinvideo;

pub const NAME: &str = "fetch_wayinvideo_transcript";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When a WayinVideo URL is the only path to a transcript and that transcript is required for the current analysis. Do not use for generic YouTube or social context.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "WayinVideo URL whose transcript is required."
                },
                "title": {
                    "type": "string",
                    "description": "Optional title hint for the task."
                },
                "published": {
                    "type": "string",
                    "description": "Optional publish time hint."
                },
                "task": {
                    "type": "string",
                    "description": "Optional Wayin task name."
                },
                "task_id": {
                    "type": "string",
                    "description": "Optional existing Wayin task id."
                }
            },
            "required": ["url"],
            "additionalProperties": true
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Args {
    pub url: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub published: Option<String>,
    #[serde(default)]
    pub task: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
}

pub async fn execute(args: Value) -> Result<Value> {
    let tool_args = serde_json::from_value::<Args>(args)
        .context("invalid fetch_wayinvideo_transcript arguments")?;
    let ingest_args = wayinvideo::WayinVideoArgs {
        url: tool_args.url,
        title: tool_args.title,
        published: tool_args.published,
        task: tool_args.task,
        task_id: tool_args.task_id,
        output: tool_args.output,
    };
    let result = wayinvideo::run(ingest_args)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"));
    log_tool_result(NAME, &result);
    result
}
