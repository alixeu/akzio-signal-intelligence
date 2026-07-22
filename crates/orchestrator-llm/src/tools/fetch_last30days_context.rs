use super::ToolDefinition;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{api_tool_name, log_tool_result, ExternalToolConfig};
use orchestrator_ingest::social;

pub const NAME: &str = "fetch_last30days_context";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When last-30-days social/web context for a source (reddit, x, youtube) is required and not already present in the run's imported context. Prefer imported research_inputs when they already cover the source and window.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Social/web source for the situation, e.g. reddit, x, youtube."
                },
                "tickers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Tickers whose last-30-days context is required."
                },
                "query": {
                    "type": "string",
                    "description": "Optional focused query when the default ticker keywords are insufficient."
                }
            },
            "required": [],
            "additionalProperties": true
        }),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Args {
    #[serde(default)]
    pub ticker: Option<String>,
    #[serde(default)]
    pub tickers: Vec<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub days: Option<i64>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub depth: Option<String>,
}

impl Args {
    fn effective_tickers(&self) -> Vec<String> {
        if !self.tickers.is_empty() {
            return self.tickers.clone();
        }
        self.ticker.clone().into_iter().collect()
    }
}

pub async fn execute(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let tool_args = serde_json::from_value::<Args>(args)
        .context("invalid fetch_last30days_context arguments")?;
    let tickers = tool_args.effective_tickers();
    let tickers = if tickers.is_empty() {
        config.tickers.clone()
    } else {
        tickers
    };
    let source = normalize_source(tool_args.source.as_deref());
    let source_enum = match source {
        Some("reddit") => social::Source::Reddit,
        Some("x") | Some("twitter") => social::Source::X,
        Some("youtube") => social::Source::Youtube,
        _ => social::Source::Reddit,
    };
    let social_args = social::SocialArgs {
        source: source_enum,
        tickers,
        days: tool_args.days.unwrap_or(30),
        depth: match tool_args.depth.as_deref() {
            Some("quick") => social::Depth::Quick,
            Some("deep") => social::Depth::Deep,
            _ => social::Depth::Balanced,
        },
        limit: tool_args.limit,
        ..Default::default()
    };
    let result = social::run(social_args)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"));
    log_tool_result(NAME, &result);
    result
}

fn normalize_source(source: Option<&str>) -> Option<&str> {
    source.map(|value| match value.trim() {
        "twitter" | "x_twitter" | "x-twitter" => "x",
        other => other,
    })
}
