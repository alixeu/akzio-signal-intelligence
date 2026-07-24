use super::{api_tool_name, log_tool_result, ExternalToolConfig, ToolDefinition};
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

pub const NAME: &str = "read_jin10_candidates";
const DEFAULT_LIMIT: usize = 30;
const MAX_LIMIT: usize = 50;

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Read a bounded, deterministically ranked Jin10 candidate set with stable event IDs and timestamps. Candidates are leads to verify, not confirmed market facts.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "tickers": {"type": "array", "items": {"type": "string"}},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50}
            },
            "required": [],
            "additionalProperties": false
        }),
    }
}

#[derive(Debug, Deserialize)]
struct Args {
    #[serde(default)]
    tickers: Vec<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    DEFAULT_LIMIT
}

pub fn execute(args: Value, config: &ExternalToolConfig) -> Result<Value> {
    let args: Args =
        serde_json::from_value(args).context("invalid read_jin10_candidates arguments")?;
    let limit = args.limit.clamp(1, MAX_LIMIT);
    let tickers = if args.tickers.is_empty() {
        config.tickers.clone()
    } else {
        args.tickers
    };
    let rows = orchestrator_core::load_jin10_csv_recent_from_dir(
        &config
            .project_root
            .join(orchestrator_core::DEFAULT_JIN10_CSV_DIR),
        3,
    );
    let mut candidates = rows
        .into_iter()
        .map(|row| {
            let priority = candidate_priority(&row.content, &tickers);
            (priority, row)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| right.time.cmp(&left.time))
    });
    let events = candidates
        .into_iter()
        .take(limit)
        .map(|(priority, row)| {
            json!({
                "event_id": row.id,
                "event_time": row.time,
                "content": row.content,
                "runtime_priority": priority
            })
        })
        .collect::<Vec<_>>();
    let result = if events.is_empty() {
        json!({"status": "data_gap", "data_gap": "no preflight Jin10 candidate data"})
    } else {
        json!({"status": "ok", "source": "csv.jin10", "candidates": events})
    };
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}

fn candidate_priority(content: &str, tickers: &[String]) -> u8 {
    let lower = content.to_ascii_lowercase();
    let ticker_match = tickers
        .iter()
        .any(|ticker| lower.contains(&ticker.to_ascii_lowercase()));
    let macro_match = [
        "cpi",
        "inflation",
        "fomc",
        "federal reserve",
        "fed",
        "payroll",
        "jobs",
        "pce",
        "gdp",
        "treasury",
        "yield",
        "vix",
        "美联储",
        "通胀",
        "非农",
        "国债",
        "收益率",
    ]
    .iter()
    .any(|token| lower.contains(token));
    match (ticker_match, macro_match) {
        (true, true) => 3,
        (true, false) => 2,
        (false, true) => 1,
        (false, false) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_candidates_and_preserves_stable_event_ids() {
        let temp = tempfile::tempdir().unwrap();
        let csv_dir = temp.path().join(orchestrator_core::DEFAULT_JIN10_CSV_DIR);
        let path = orchestrator_core::jin10_csv_path(&csv_dir, "2026-07-21");
        orchestrator_core::write_jin10_csv(
            &path,
            &[orchestrator_core::Jin10CsvRow {
                id: "event-1".into(),
                time: "2026-07-21 12:00:00".into(),
                content: "Fed CPI update".into(),
            }],
        )
        .unwrap();
        let result = execute(
            json!({"tickers": ["QQQ"], "limit": 50}),
            &ExternalToolConfig {
                project_root: temp.path().to_path_buf(),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(result["candidates"][0]["event_id"], "event-1");
    }
}
