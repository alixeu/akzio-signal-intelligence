pub mod fetch_jin10_flash;
pub mod fetch_last30days_context;
pub mod fetch_wayinvideo_transcript;
pub mod fetch_youtube_transcript;
pub mod read_jin10_csv;
pub mod read_run_context;
pub mod read_technical_csv;
pub mod run_technical_indicators;
pub mod think;
pub mod web_run;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;
use tracing::{debug, warn};

use crate::agent_loop::ToolRuntimeTurnContext;
pub use crate::web_search::{WebSearchConfig, WebSearchProvider};
pub use web_run::Runtime as WebRunRuntime;

pub const WEB_RUN_TOOL_NAME: &str = web_run::NAME;
pub const READ_RUN_CONTEXT_TOOL_NAME: &str = read_run_context::NAME;

#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExternalToolConfig {
    pub project_root: PathBuf,
    pub db_path: Option<PathBuf>,
    pub run_dir: Option<PathBuf>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub tickers: Vec<String>,
    #[serde(skip)]
    pub phase00_index: Option<std::sync::Arc<orchestrator_sql::Phase00MemoryIndex>>,
    #[serde(skip)]
    pub phase00_gate: Option<std::sync::Arc<orchestrator_sql::Phase00Gate>>,
}

impl Default for ExternalToolConfig {
    fn default() -> Self {
        Self {
            project_root: PathBuf::from("."),
            db_path: None,
            run_dir: None,
            run_id: None,
            tickers: Vec::new(),
            phase00_index: None,
            phase00_gate: None,
        }
    }
}

// --- Registry ---

struct ToolEntry {
    name: &'static str,
    definition: fn() -> ToolDefinition,
}

const REGISTRY: &[ToolEntry] = &[
    ToolEntry {
        name: think::NAME,
        definition: think::definition,
    },
    ToolEntry {
        name: read_run_context::NAME,
        definition: read_run_context::definition,
    },
    ToolEntry {
        name: web_run::NAME,
        definition: web_run::definition,
    },
    ToolEntry {
        name: fetch_jin10_flash::NAME,
        definition: fetch_jin10_flash::definition,
    },
    ToolEntry {
        name: fetch_youtube_transcript::NAME,
        definition: fetch_youtube_transcript::definition,
    },
    ToolEntry {
        name: fetch_wayinvideo_transcript::NAME,
        definition: fetch_wayinvideo_transcript::definition,
    },
    ToolEntry {
        name: run_technical_indicators::NAME,
        definition: run_technical_indicators::definition,
    },
    ToolEntry {
        name: read_technical_csv::NAME,
        definition: read_technical_csv::definition,
    },
    ToolEntry {
        name: read_jin10_csv::NAME,
        definition: read_jin10_csv::definition,
    },
    ToolEntry {
        name: fetch_last30days_context::NAME,
        definition: fetch_last30days_context::definition,
    },
];

pub fn tool_names() -> &'static [&'static str] {
    // Exclude think (always enabled via runtime, not listed in explicit names)
    // and web.run (conditionally added).
    &[
        read_run_context::NAME,
        fetch_jin10_flash::NAME,
        fetch_youtube_transcript::NAME,
        fetch_wayinvideo_transcript::NAME,
        run_technical_indicators::NAME,
        read_technical_csv::NAME,
        read_jin10_csv::NAME,
        fetch_last30days_context::NAME,
    ]
}

pub fn enabled_tool_names(web_run: Option<&WebSearchConfig>) -> Vec<&'static str> {
    let mut names = tool_names().to_vec();
    if web_run.is_some() {
        names.push(web_run::NAME);
    }
    names
}

pub fn tool_definition(name: &str) -> Option<ToolDefinition> {
    REGISTRY
        .iter()
        .find(|entry| entry.name == name)
        .map(|entry| (entry.definition)())
}

pub fn responses_tool_definitions(names: &[String]) -> Vec<async_openai::types::responses::Tool> {
    names
        .iter()
        .filter_map(|name| responses_tool_definition(name))
        .collect()
}

fn responses_tool_definition(name: &str) -> Option<async_openai::types::responses::Tool> {
    let core = tool_definition(name)?;
    Some(async_openai::types::responses::Tool::Function(
        async_openai::types::responses::FunctionToolArgs::default()
            .name(core.name)
            .description(core.description)
            .parameters(core.parameters)
            .strict(false)
            .build()
            .expect("FunctionTool build"),
    ))
}

pub fn chat_completions_tool_definitions(
    names: &[String],
) -> Vec<async_openai::types::chat::ChatCompletionTools> {
    names
        .iter()
        .filter_map(|name| chat_completions_tool_definition(name))
        .collect()
}

fn chat_completions_tool_definition(
    name: &str,
) -> Option<async_openai::types::chat::ChatCompletionTools> {
    let core = tool_definition(name)?;
    Some(async_openai::types::chat::ChatCompletionTools::Function(
        async_openai::types::chat::ChatCompletionTool {
            function: async_openai::types::chat::FunctionObject {
                name: core.name,
                description: Some(core.description),
                parameters: Some(core.parameters),
                strict: Some(false),
            },
        },
    ))
}

/// Build debug-friendly JSON array of tool definitions for the given names.
pub fn tool_definitions_json(names: &[String]) -> Vec<Value> {
    names
        .iter()
        .filter_map(|name| {
            let def = tool_definition(name)?;
            Some(json!({
                "type": "function",
                "function": {
                    "name": def.name,
                    "description": def.description,
                    "parameters": def.parameters,
                }
            }))
        })
        .collect()
}

/// OpenAI-compatible function names reject `.`; map internal names to API-safe form.
pub fn api_tool_name(name: &str) -> String {
    name.replace('.', "_")
}

/// Map a model-emitted function name back to the internal tool id.
pub fn resolve_tool_name(api_name: &str) -> String {
    match api_name {
        "web_run" => web_run::NAME.to_string(),
        other => other.to_string(),
    }
}

// --- Dispatch ---

pub async fn execute_named_tool(
    name: &str,
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
    web_run: Option<&WebRunRuntime>,
) -> Result<Value> {
    debug!(
        tool = name,
        has_turn_context = turn_context.is_some(),
        "named tool starting"
    );
    match name {
        read_run_context::NAME => {
            let result = read_run_context::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        web_run::NAME => {
            if let Some(web_run) = web_run {
                let result = web_run.execute(args).await;
                log_tool_result(name, &result);
                result
            } else {
                let result = Ok(web_run::safe_error("Web search is disabled."));
                log_tool_result(name, &result);
                result
            }
        }
        fetch_jin10_flash::NAME => fetch_jin10_flash::execute(args, config, turn_context).await,
        fetch_youtube_transcript::NAME => fetch_youtube_transcript::execute(args).await,
        fetch_wayinvideo_transcript::NAME => fetch_wayinvideo_transcript::execute(args).await,
        run_technical_indicators::NAME => {
            run_technical_indicators::execute(args, config, turn_context).await
        }
        read_technical_csv::NAME => read_technical_csv::execute(args),
        read_jin10_csv::NAME => read_jin10_csv::execute(args),
        fetch_last30days_context::NAME => fetch_last30days_context::execute(args, config).await,
        other => bail!("unknown tool name: {other}"),
    }
}

// --- Shared helpers ---

pub fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }
    let suffix = "\n[truncated]";
    let suffix_len = suffix.chars().count();
    if max_chars <= suffix_len {
        return value.chars().take(max_chars).collect();
    }
    let mut output = value
        .chars()
        .take(max_chars - suffix_len)
        .collect::<String>();
    output.push_str(suffix);
    output
}

pub(crate) fn log_tool_result(name: &str, result: &Result<Value>) {
    match result {
        Ok(value) => {
            let status = value
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("ok");
            debug!(
                tool = name,
                status,
                output_chars = value.to_string().len(),
                "named tool completed"
            );
        }
        Err(error) => warn!(tool = name, error = %error, "named tool failed"),
    }
}

fn tool_connection(config: &ExternalToolConfig) -> Result<rusqlite::Connection> {
    orchestrator_sql::connect(runtime_db_path(config)?)
}

fn runtime_db_path(config: &ExternalToolConfig) -> Result<PathBuf> {
    config
        .db_path
        .clone()
        .context("runtime tool requires ExternalToolConfig.db_path")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web_search::{MockWebPage, MockWebSearchProvider, WebSearchMode};
    use std::sync::Arc;

    fn external_config() -> ExternalToolConfig {
        ExternalToolConfig {
            project_root: PathBuf::from("."),
            db_path: None,
            run_dir: None,
            run_id: None,
            tickers: Vec::new(),
            phase00_index: None,
            phase00_gate: None,
        }
    }

    fn web_run_runtime<P>(config: WebSearchConfig, provider: P) -> WebRunRuntime
    where
        P: WebSearchProvider + 'static,
    {
        WebRunRuntime::new(config).with_provider(Arc::new(provider))
    }

    #[tokio::test]
    async fn read_run_context_uses_structured_sql_adapter() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("runtime.sqlite");
        let config = ExternalToolConfig {
            project_root: PathBuf::from("."),
            db_path: Some(db_path),
            run_dir: None,
            run_id: None,
            tickers: vec!["TQQQ".to_string()],
            phase00_index: None,
            phase00_gate: None,
        };

        let output = execute_named_tool(
            read_run_context::NAME,
            json!({"kind": "technical"}),
            &config,
            None,
            None,
        )
        .await
        .unwrap();

        let evidence = output.get("evidence").unwrap_or(&output);
        assert_eq!(evidence["query"], "get-technical-context");
        assert!(
            evidence.get("files").is_some()
                || evidence.get("daily").is_some()
                || evidence.get("source").is_some(),
            "unexpected technical payload: {evidence}"
        );
    }

    #[test]
    fn tool_definitions_map_web_run_api_name() {
        let names = [
            web_run::NAME.to_string(),
            read_run_context::NAME.to_string(),
        ];
        let defs: Vec<_> = names.iter().filter_map(|n| tool_definition(n)).collect();
        assert_eq!(defs.len(), 2);
        assert!(defs.iter().any(|tool| tool.name == "web_run"));
        assert!(defs.iter().any(|tool| tool.name == "read_run_context"));
        assert_eq!(resolve_tool_name("web_run"), web_run::NAME);
        assert_eq!(
            resolve_tool_name("read_run_context"),
            read_run_context::NAME
        );
    }

    #[tokio::test]
    async fn news_macro_defaults_empty_kind_to_jin10() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("runtime.sqlite");
        {
            let mut conn = orchestrator_sql::connect(&db_path).unwrap();
            orchestrator_sql::ensure_schema(&conn).unwrap();
            let imported = orchestrator_sql::import_jin10_payload(
                &mut conn,
                &json!({
                    "items": [{
                        "time": "2026-07-13 12:00:00",
                        "content": "macro headline for test"
                    }]
                }),
            )
            .unwrap();
            assert_eq!(imported, 1);
        }
        let config = ExternalToolConfig {
            project_root: PathBuf::from("."),
            db_path: Some(db_path),
            run_dir: None,
            run_id: Some("run-news".to_string()),
            tickers: vec!["QQQ".to_string()],
            phase00_index: None,
            phase00_gate: None,
        };
        let turn = ToolRuntimeTurnContext {
            run_id: "run-news".to_string(),
            session_id: "session-news".to_string(),
            turn_id: "turn-news".to_string(),
            role: "analyst.news_macro".to_string(),
            phase: None,
        };
        let output = execute_named_tool(
            read_run_context::NAME,
            json!({}),
            &config,
            Some(&turn),
            None,
        )
        .await
        .unwrap();
        assert_eq!(output["status"], "ok");
        assert_eq!(output["kind"], "jin10");
        assert_eq!(output["tickers"], json!(["QQQ"]));
        assert_eq!(output["evidence"]["query"], "get-jin10-context");
        assert_eq!(output["evidence"]["item_count"], 1);
        assert_eq!(
            output["evidence"]["items"][0]["content"],
            "macro headline for test"
        );
        assert!(
            output["evidence"]["items"][0]
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap()
                .len()
                == 32
        );
        assert_eq!(output["evidence"]["items"][0]["attention_score"], 0.0);
        assert!(output["evidence"]["items"][0].get("item").is_none());
    }

    #[tokio::test]
    async fn news_macro_stop_rereading_after_first_context() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("runtime.sqlite");
        {
            let mut conn = orchestrator_sql::connect(&db_path).unwrap();
            orchestrator_sql::ensure_schema(&conn).unwrap();
            orchestrator_sql::import_jin10_payload(
                &mut conn,
                &json!({"items":[{"time":"2026-07-13 12:00:00","content":"macro headline for test"}]}),
            )
            .unwrap();
            orchestrator_sql::upsert_agent_turn(
                &conn,
                &orchestrator_sql::AgentTurnInput {
                    turn_id: "turn-news".to_string(),
                    run_id: "run-news".to_string(),
                    phase: Some(1),
                    turn_number: 1,
                    role: "analyst.news_macro".to_string(),
                    full_context_json: json!([
                        {"event_type":"tool_result","role":"tool","content_text":"prior","content_json":{},"tool_call_id":"call-0","tool_name":"read_run_context"},
                        {"event_type":"tool_result","role":"tool","content_text":"prior","content_json":{},"tool_call_id":"call-1","tool_name":"read_run_context"}
                    ]),
                    summary: "test turn".to_string(),
                },
            )
            .unwrap();
        }
        let config = ExternalToolConfig {
            project_root: PathBuf::from("."),
            db_path: Some(db_path),
            run_dir: None,
            run_id: Some("run-news".to_string()),
            tickers: vec!["QQQ".to_string(), "SOXX".to_string()],
            phase00_index: None,
            phase00_gate: None,
        };
        let turn = ToolRuntimeTurnContext {
            run_id: "run-news".to_string(),
            session_id: "session-news".to_string(),
            turn_id: "turn-news".to_string(),
            role: "analyst.news_macro".to_string(),
            phase: None,
        };
        let output = execute_named_tool(
            read_run_context::NAME,
            json!({}),
            &config,
            Some(&turn),
            None,
        )
        .await
        .unwrap();
        assert_eq!(output["status"], "stop_rereading");
        assert_eq!(output["tickers"], json!(["QQQ", "SOXX"]));
        assert!(output["message"]
            .as_str()
            .unwrap()
            .contains("Emit one JSON object"));
    }

    #[test]
    fn run_technical_args_accept_tickers_alias() {
        let args: run_technical_indicators::Args =
            serde_json::from_value(json!({"tickers": ["QQQ", "SOXX"]})).unwrap();
        assert_eq!(args.symbols, vec!["QQQ".to_string(), "SOXX".to_string()]);
    }

    #[tokio::test]
    async fn run_technical_indicators_is_dispatchable() {
        let error = execute_named_tool(
            run_technical_indicators::NAME,
            json!({"days": "bad"}),
            &external_config(),
            None,
            None,
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("invalid run_technical_indicators arguments"));
    }

    #[tokio::test]
    async fn web_run_disabled_returns_safe_error() {
        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&WebRunRuntime::new(WebSearchConfig::default())),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert_eq!(output["content"], "Web search is disabled.");
    }

    #[tokio::test]
    async fn web_run_accepts_legacy_agent_search_shape() {
        let provider = MockWebSearchProvider::new(vec![MockWebPage {
            title: "TQQQ Reddit".to_string(),
            url: "https://www.reddit.com/r/TQQQ/comments/1".to_string(),
            content: "QQQ and VIX discussion for TQQQ.".to_string(),
        }]);
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({
                "search_query": "TQQQ site:reddit.com QQQ VIX",
                "include_domains": ["reddit.com"],
                "num_results": 10,
                "source": "exa",
                "response_length": "medium"
            }),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "success");
        assert!(output["content"].as_str().unwrap().contains("TQQQ Reddit"));
    }

    #[tokio::test]
    async fn web_run_rejects_too_many_search_queries() {
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({
                "search_query": [
                    {"q": "one"},
                    {"q": "two"},
                    {"q": "three"},
                    {"q": "four"},
                    {"q": "five"}
                ]
            }),
            &external_config(),
            None,
            Some(&WebRunRuntime::new(config)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("at most 4 queries"));
    }

    #[tokio::test]
    async fn web_run_rejects_overlong_queries() {
        let provider = MockWebSearchProvider::default();
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "x".repeat(513)}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("exceeds 512 chars"));
    }

    #[tokio::test]
    async fn web_run_formats_filters_and_truncates_search_results() {
        let provider = MockWebSearchProvider::new(vec![
            MockWebPage {
                title: "Allowed TQQQ".to_string(),
                url: "https://research.example.com/tqqq?token=secret#section".to_string(),
                content: "TQQQ volatility and liquidity signal with enough detail to truncate."
                    .to_string(),
            },
            MockWebPage {
                title: "Blocked TQQQ".to_string(),
                url: "https://blocked.example.com/tqqq".to_string(),
                content: "TQQQ blocked signal".to_string(),
            },
            MockWebPage {
                title: "Non HTTP TQQQ".to_string(),
                url: "ftp://research.example.com/tqqq".to_string(),
                content: "TQQQ non http signal".to_string(),
            },
        ]);
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            allowed_domains: vec!["example.com".to_string()],
            blocked_domains: vec!["blocked.example.com".to_string()],
            max_result_chars: 220,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ"}], "response_length": "short"}),
            &external_config(),
            None,
            Some(&web_run_runtime(config.clone(), provider.clone())),
        )
        .await
        .unwrap();
        let text = output["text"].as_str().unwrap();

        assert!(text.starts_with("Search results:\n[ref_id: search0]"));
        assert!(text.contains("Title: Allowed TQQQ"));
        assert!(text.contains("URL: https://research.example.com/tqqq"));
        assert!(!text.contains("token=secret"));
        assert!(!text.contains("Blocked TQQQ"));
        assert!(!text.contains("Non HTTP TQQQ"));
        assert_eq!(output["results"].as_array().unwrap().len(), 1);
        assert_eq!(
            output["results"][0]["url"],
            "https://research.example.com/tqqq"
        );

        let truncated_config = WebSearchConfig {
            max_result_chars: 80,
            ..config
        };
        let truncated = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(truncated_config, provider)),
        )
        .await
        .unwrap();
        let truncated_text = truncated["text"].as_str().unwrap();
        assert!(truncated_text.contains("[... middle truncated ...]"));
        assert!(truncated_text.chars().count() <= 80);
    }

    #[tokio::test]
    async fn web_run_rejects_query_domains_outside_policy() {
        let provider = MockWebSearchProvider::default();
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            allowed_domains: vec!["example.com".to_string()],
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ", "domains": ["not-example.com"]}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("domain is not allowed"));
    }

    #[derive(Debug)]
    struct FailingProvider;

    impl WebSearchProvider for FailingProvider {
        fn search<'a>(
            &'a self,
            _queries: Vec<crate::web_search::SearchQuery>,
            _options: crate::web_search::WebSearchOptions,
        ) -> crate::web_search::WebSearchFuture<'a, Vec<crate::web_search::SearchResult>> {
            Box::pin(async {
                bail!("provider rejected request with API key sk-secret-do-not-leak")
            })
        }
    }

    #[tokio::test]
    async fn web_run_sanitizes_provider_errors() {
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, FailingProvider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert_eq!(output["content"], "web.run provider failed.");
        assert!(!output.to_string().contains("sk-secret"));
    }
}
