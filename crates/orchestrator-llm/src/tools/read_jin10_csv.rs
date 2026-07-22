use super::ToolDefinition;
use super::{api_tool_name, log_tool_result, ExternalToolConfig};
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

pub const NAME: &str = "read_jin10_context";

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Read the preflight Jin10 feed (stable id/time/content) from the run SQLite database. Use it to shortlist high-relevance macro/news clues before concluding from headlines. If empty, report a data gap rather than inventing items. After analysis, score used items via jin10_attention [{id, score 0.0-1.0}].".to_string(),
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
    let db_path = config
        .db_path
        .as_ref()
        .context("read_jin10_context requires the run SQLite path")?;
    let conn = orchestrator_sql::connect(db_path)?;
    let mut stmt = if tool_args.date.is_some() {
        conn.prepare(
            "SELECT content_json FROM jin10_items WHERE substr(json_extract(content_json, '$.time_raw'), 1, 10) = ?1 ORDER BY item_time DESC LIMIT 500",
        )?
    } else {
        conn.prepare("SELECT content_json FROM jin10_items ORDER BY item_time DESC LIMIT 500")?
    };
    let rows = if let Some(date) = tool_args.date.as_deref() {
        stmt.query_map([date], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    } else {
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let items = rows
        .into_iter()
        .map(|raw| serde_json::from_str::<Value>(&raw))
        .collect::<serde_json::Result<Vec<_>>>()?;
    let result = if items.is_empty() {
        json!({"error": "no Jin10 data in run SQLite", "hint": "news preflight may not have completed"})
    } else {
        json!({
            "source": "sqlite.jin10_items",
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
    fn reads_only_from_configured_sqlite_database() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("run.sqlite");
        let mut conn = orchestrator_sql::connect(&db_path).unwrap();
        orchestrator_sql::import_jin10_payload(
            &mut conn,
            &json!({"items": [{"time": "2026-07-21 12:00:00", "content": "macro event"}]}),
        )
        .unwrap();
        drop(conn);
        let config = ExternalToolConfig {
            db_path: Some(db_path),
            ..Default::default()
        };

        let result = execute(json!({}), &config).unwrap();

        assert_eq!(result["source"], "sqlite.jin10_items");
        assert_eq!(result["items"].as_array().unwrap().len(), 1);
        assert_eq!(result["items"][0]["content"], "macro event");
    }
}
