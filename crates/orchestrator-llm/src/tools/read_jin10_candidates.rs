use super::{api_tool_name, log_tool_result, ExternalToolConfig, ToolDefinition};
use anyhow::{Context, Result};
use orchestrator_core::ToolId;
use orchestrator_store::{InputSource, Jin10Format};
use serde::Deserialize;
use serde_json::{json, Value};

pub const NAME: &str = ToolId::ReadJin10Candidates.as_str();
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
    let rows = if let Some(rows) = read_snapshotted_rows(config)? {
        rows
    } else {
        orchestrator_core::load_jin10_csv_recent_from_dir(
            &config
                .project_root
                .join(orchestrator_core::DEFAULT_JIN10_CSV_DIR),
            3,
        )
    };
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
            let evidence_hash = orchestrator_store::content_hash(&json!({
                "event_id": row.id,
                "event_time": row.time,
                "content": row.content,
            }))
            .expect("Jin10 evidence identity contains only JSON-safe values");
            json!({
                "event_id": row.id,
                "evidence_id": format!(
                    "jin10-{}",
                    evidence_hash.strip_prefix("sha256:").unwrap_or(&evidence_hash)
                ),
                "event_time": row.time,
                "content": row.content,
                "runtime_priority": priority
            })
        })
        .collect::<Vec<_>>();
    let result = if events.is_empty() {
        json!({"status": "data_gap", "data_gap": "no preflight Jin10 candidate data"})
    } else {
        json!({
            "status": "ok",
            "source": if config.file_store_input.is_some() { "filestore.run_input.jin10" } else { "csv.jin10" },
            "candidates": events
        })
    };
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}

/// Return the Jin10 rows sealed for this run, when this role has a FileStore
/// input binding.  Callers that need a run-local provenance guarantee must not
/// fall back to the mutable project-root CSV when this returns `None`.
pub(crate) fn read_snapshotted_rows(
    config: &ExternalToolConfig,
) -> Result<Option<Vec<orchestrator_core::Jin10CsvRow>>> {
    let Some(snapshot) = &config.file_store_input else {
        return Ok(None);
    };
    let source = InputSource::jin10(snapshot.current_date.clone(), Jin10Format::Csv)?;
    let payload = snapshot.read(&source)?;
    let raw = std::str::from_utf8(&payload).context("snapshotted Jin10 CSV is not valid UTF-8")?;
    Ok(Some(orchestrator_core::parse_jin10_csv(raw)?))
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
    use orchestrator_store::{capture_run_inputs, write_input_payload, FileStore, RunLocation};

    #[test]
    fn bounds_candidates_and_preserves_stable_event_ids() {
        let temp = tempfile::tempdir().unwrap();
        let csv_dir = temp.path().join(orchestrator_core::DEFAULT_JIN10_CSV_DIR);
        let path = orchestrator_core::jin10_csv_path(&csv_dir, "2026-07-21");
        orchestrator_store::publish_bytes_atomic(
            &path,
            orchestrator_core::render_jin10_csv(&[orchestrator_core::Jin10CsvRow {
                id: "event-1".into(),
                time: "2026-07-21 12:00:00".into(),
                content: "Fed CPI update".into(),
            }])
            .as_bytes(),
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

    #[test]
    fn file_store_reader_reuses_jin10_run_copy_after_source_changes() {
        let temp = tempfile::tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let source = InputSource::jin10("2026-07-27", Jin10Format::Csv).unwrap();
        write_input_payload(
            &store,
            source.clone(),
            b"id,time,content\nevent-old,2026-07-27 09:00:00,Fed CPI update\n",
            "2026-07-27T00:00:00Z",
        )
        .unwrap();
        let location = RunLocation::new("2026-07-27", "run-jin10-test").unwrap();
        capture_run_inputs(
            &store,
            &location,
            std::slice::from_ref(&source),
            "2026-07-27T00:00:00Z",
        )
        .unwrap();
        write_input_payload(
            &store,
            source,
            b"id,time,content\nevent-new,2026-07-27 10:00:00,unrelated refresh\n",
            "2026-07-27T00:01:00Z",
        )
        .unwrap();

        let result = execute(
            json!({"tickers": ["QQQ"]}),
            &ExternalToolConfig {
                file_store_input: Some(super::super::FileStoreInputSnapshot {
                    store_root: temp.path().to_path_buf(),
                    run_id: "run-jin10-test".to_owned(),
                    current_date: "2026-07-27".to_owned(),
                    storage_namespace: None,
                }),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(result["candidates"][0]["event_id"], "event-old");
    }
}
