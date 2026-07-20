use anyhow::{Context, Result};
use rig_core::completion::ToolDefinition;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result};
use orchestrator_ingest::youtube;

pub const NAME: &str = "fetch_youtube_transcript";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When you must fetch YouTube transcripts for a configured channel or a specific URL because imported context has no usable captions for the target videos. Do not use when the run already imported Rhino/high-spread samples.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "all": {
                    "type": "boolean",
                    "description": "When true, fetch transcripts for the configured channel set."
                },
                "channel": {
                    "type": "string",
                    "description": "Configured channel key when targeting one channel."
                },
                "url": {
                    "type": "string",
                    "description": "Specific video URL when one video is required."
                },
                "max_videos": {
                    "type": "integer",
                    "description": "Optional max videos to fetch."
                }
            },
            "additionalProperties": true
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Args {
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub max_videos: Option<usize>,
    #[serde(default)]
    pub output: Option<String>,
}

pub async fn execute(args: Value) -> Result<Value> {
    let tool_args = serde_json::from_value::<Args>(args)
        .context("invalid fetch_youtube_transcript arguments")?;
    let ingest_args = youtube::YoutubeArgs {
        all: tool_args.all,
        channel: tool_args.channel,
        url: tool_args.url,
        max_videos: tool_args.max_videos,
        output: tool_args.output,
    };
    let result = youtube::run(ingest_args)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"));
    log_tool_result(NAME, &result);
    result
}
