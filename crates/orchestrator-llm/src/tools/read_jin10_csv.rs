use super::ToolDefinition;
use super::{api_tool_name, log_tool_result, ExternalToolConfig};
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

pub const NAME: &str = "read_jin10_context";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Read the preflight Jin10 CSV feed (stable id/time/content) before it is admitted to the run SQLite database. Use it to shortlist high-relevance macro/news clues before concluding from headlines. If empty, report a data gap rather than inventing items. After analysis, score used items via jin10_attention [{id, score 0.0-1.0}]; only scored items are persisted to SQLite.".to_string(),
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

pub fn execute(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let tool_args =
        serde_json::from_value::<Args>(args).context("invalid read_jin10_context arguments")?;
    let csv_dir = config
        .project_root
        .join(orchestrator_core::DEFAULT_JIN10_CSV_DIR);
    let rows = if let Some(date) = tool_args.date.as_deref() {
        orchestrator_core::read_jin10_csv(&orchestrator_core::jin10_csv_path(&csv_dir, date))?
    } else {
        orchestrator_core::load_jin10_csv_recent_from_dir(&csv_dir, 3)
    };
    let items = rows
        .into_iter()
        .take(500)
        .map(|row| {
            json!({
                "id": row.id,
                "time": row.time,
                "time_raw": row.time,
                "content": row.content,
            })
        })
        .collect::<Vec<_>>();
    let result = if items.is_empty() {
        json!({"error": "no preflight Jin10 CSV data", "hint": "news preflight may not have completed"})
    } else {
        json!({
            "source": "csv.jin10",
            "items": items,
            "attention_note": "After analysis, return jin10_attention: [{id, score}] with score 0.0-1.0 for items that influenced your analysis. Only scored items will be persisted."
        })
    };
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_preflight_csv_without_a_database() {
        let temp = tempfile::tempdir().unwrap();
        let csv_dir = temp.path().join(orchestrator_core::DEFAULT_JIN10_CSV_DIR);
        let path = orchestrator_core::jin10_csv_path(&csv_dir, "2026-07-21");
        orchestrator_core::write_jin10_csv(
            &path,
            &[orchestrator_core::Jin10CsvRow {
                id: "event-1".to_string(),
                time: "2026-07-21 12:00:00".to_string(),
                content: "macro event".to_string(),
            }],
        )
        .unwrap();
        let config = ExternalToolConfig {
            project_root: temp.path().to_path_buf(),
            ..Default::default()
        };

        let result = execute(json!({}), &config).unwrap();

        assert_eq!(result["source"], "csv.jin10");
        assert_eq!(result["items"].as_array().unwrap().len(), 1);
        assert_eq!(result["items"][0]["content"], "macro event");
    }
}
