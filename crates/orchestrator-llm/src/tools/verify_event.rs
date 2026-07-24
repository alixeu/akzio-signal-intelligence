use super::{
    api_tool_name, log_tool_result, web_run, ExternalToolConfig, ToolDefinition, WebRunRuntime,
};
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

pub const NAME: &str = "verify_event";
const MAX_FIELDS: usize = 3;
const MAX_SOURCES: usize = 2;

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "Verify a material event through bounded, auditable searches. Accepts a stable event ID or observed claim plus the fields still missing; it composes focused source queries and returns the evidence candidates. Use only when the missing evidence could change a market conclusion.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "event_id": {"type": "string", "description": "Stable candidate event ID when available."},
                "observed_claim": {"type": "string", "description": "The event or market observation to verify."},
                "entities": {"type": "array", "items": {"type": "string"}},
                "event_time": {"type": "string"},
                "tickers": {"type": "array", "items": {"type": "string"}},
                "missing_fields": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["official_wording", "actual_vs_expected", "market_reaction"]},
                    "minItems": 1
                },
                "max_independent_sources": {"type": "integer", "minimum": 1, "maximum": 2}
            },
            "required": ["observed_claim", "missing_fields"],
            "additionalProperties": false
        }),
    }
}

#[derive(Debug, Deserialize)]
struct Args {
    #[serde(default)]
    event_id: Option<String>,
    observed_claim: String,
    #[serde(default)]
    entities: Vec<String>,
    #[serde(default)]
    event_time: Option<String>,
    #[serde(default)]
    tickers: Vec<String>,
    missing_fields: Vec<String>,
    #[serde(default = "default_max_sources")]
    max_independent_sources: usize,
}

fn default_max_sources() -> usize {
    MAX_SOURCES
}

pub async fn execute(
    args: Value,
    config: &ExternalToolConfig,
    web_runtime: Option<&WebRunRuntime>,
) -> Result<Value> {
    let args: Args = serde_json::from_value(args).context("invalid verify_event arguments")?;
    if args.observed_claim.trim().is_empty() {
        bail!("verify_event requires a non-empty observed_claim");
    }
    if args.missing_fields.is_empty() {
        bail!("verify_event requires at least one missing field");
    }
    let fields = args
        .missing_fields
        .iter()
        .take(MAX_FIELDS)
        .cloned()
        .collect::<Vec<_>>();
    if fields.iter().any(|field| {
        !matches!(
            field.as_str(),
            "official_wording" | "actual_vs_expected" | "market_reaction"
        )
    }) {
        bail!("verify_event received unsupported missing_fields");
    }
    let event = event_context(args.event_id.as_deref(), &args.observed_claim, config);
    let queries = fields
        .iter()
        .map(|field| {
            json!({
                "q": verification_query(field, &event, &args),
                "numResults": args.max_independent_sources.clamp(1, MAX_SOURCES)
            })
        })
        .collect::<Vec<_>>();
    let search = if let Some(runtime) = web_runtime {
        runtime
            .execute(json!({"search_query": queries, "response_length": "short"}))
            .await?
    } else {
        web_run::safe_error("Event verification search is disabled.")
    };
    let result = json!({
        "status": if search.get("status").and_then(Value::as_str) == Some("error") { "data_gap" } else { "ok" },
        "event": event,
        "missing_fields": fields,
        "search": search,
        "stop_condition": "stop after an authoritative answer and at most one independent cross-check"
    });
    log_tool_result(NAME, &Ok(result.clone()));
    Ok(result)
}

fn event_context(
    event_id: Option<&str>,
    observed_claim: &str,
    config: &ExternalToolConfig,
) -> Value {
    let event_id = event_id.map(str::trim).filter(|value| !value.is_empty());
    let candidate = event_id.and_then(|id| {
        orchestrator_core::load_jin10_csv_recent_from_dir(
            &config
                .project_root
                .join(orchestrator_core::DEFAULT_JIN10_CSV_DIR),
            3,
        )
        .into_iter()
        .find(|row| row.id == id)
    });
    json!({
        "event_id": event_id,
        "candidate_time": candidate.as_ref().map(|row| &row.time),
        "candidate_content": candidate.as_ref().map(|row| &row.content),
        "observed_claim": observed_claim
    })
}

fn verification_query(field: &str, event: &Value, args: &Args) -> String {
    let content = event
        .get("candidate_content")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(args.observed_claim.as_str());
    let entities = args.entities.join(" ");
    let time = args.event_time.as_deref().unwrap_or_default();
    let scope = format!("{content} {entities} {time}").trim().to_string();
    match field {
        "official_wording" => format!("{scope} official release statement"),
        "actual_vs_expected" => format!("{scope} actual versus expected consensus"),
        "market_reaction" => format!(
            "{scope} {} treasury yield dollar VIX market reaction",
            args.tickers.join(" ")
        ),
        _ => unreachable!("validated missing field"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_a_safe_gap_when_search_is_disabled() {
        let result = execute(
            json!({"observed_claim": "CPI surprise", "missing_fields": ["actual_vs_expected"]}),
            &ExternalToolConfig::default(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(result["status"], "data_gap");
        assert_eq!(result["missing_fields"][0], "actual_vs_expected");
    }
}
