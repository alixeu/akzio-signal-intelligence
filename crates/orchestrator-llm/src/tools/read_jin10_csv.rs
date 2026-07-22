use super::ToolDefinition;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;

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
    pub date: Option<String>,
}

pub fn execute(args: Value) -> Result<Value> {
    let tool_args =
        serde_json::from_value::<Args>(args).context("invalid read_jin10_csv arguments")?;
    let csv_dir = orchestrator_core::default_jin10_csv_dir();
    let raw_csv = if let Some(date) = &tool_args.date {
        let path = orchestrator_core::jin10_csv_path(&csv_dir, date);
        fs::read_to_string(&path).ok()
    } else {
        load_recent_raw_csv(&csv_dir, 3)
    };
    let result = match raw_csv {
        Some(csv) if !csv.trim().is_empty() => {
            json!({
                "csv": csv,
                "attention_note": "After analysis, return jin10_attention: [{id, score}] with score 0.0-1.0 for items that influenced your analysis. Only scored items will be persisted."
            })
        }
        _ => {
            json!({"error": "no jin10 CSV data available", "hint": "news data may not have been fetched yet"})
        }
    };
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}

fn load_recent_raw_csv(csv_dir: &std::path::Path, max_files: usize) -> Option<String> {
    let entries = fs::read_dir(csv_dir).ok()?;
    let mut paths: Vec<std::path::PathBuf> = entries
        .flatten()
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".csv")))
        .map(|e| e.path())
        .collect();
    paths.sort();
    paths.reverse();
    paths.truncate(max_files);
    if paths.is_empty() {
        return None;
    }
    let mut combined = String::from("id,time,content\n");
    for path in paths {
        if let Ok(content) = fs::read_to_string(&path) {
            for line in content.lines().skip(1).filter(|l| !l.trim().is_empty()) {
                combined.push_str(line);
                combined.push('\n');
            }
        }
    }
    Some(combined)
}
