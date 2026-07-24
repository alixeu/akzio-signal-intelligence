pub mod alpaca;
pub mod read_experience;
pub mod read_jin10_csv;
pub mod read_phase_summaries;
pub mod read_phase_summary_details;
pub mod read_reflection_source;
pub mod read_run_context;
pub mod read_technical_csv;
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
pub const READ_PHASE_SUMMARIES_TOOL_NAME: &str = read_phase_summaries::NAME;
pub const READ_PHASE_SUMMARY_DETAILS_TOOL_NAME: &str = read_phase_summary_details::NAME;
pub const READ_EXPERIENCE_TOOL_NAME: &str = read_experience::NAME;
pub const READ_REFLECTION_SOURCE_TOOL_NAME: &str = read_reflection_source::NAME;
// Internal compatibility only. This tool is intentionally absent from REGISTRY.
pub const READ_RUN_CONTEXT_TOOL_NAME: &str = read_run_context::NAME;
pub const ALPACA_GET_PORTFOLIO_TOOL_NAME: &str = alpaca::GET_PORTFOLIO_NAME;
pub const ALPACA_GET_HISTORY_TOOL_NAME: &str = alpaca::GET_HISTORY_NAME;
pub const ALPACA_GET_PRICE_TOOL_NAME: &str = alpaca::GET_PRICE_NAME;
pub const ALPACA_GET_NEWS_TOOL_NAME: &str = alpaca::GET_NEWS_NAME;
pub const ALPACA_SUBMIT_TRADE_TOOL_NAME: &str = alpaca::SUBMIT_TRADE_NAME;

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
    #[serde(default)]
    pub phase: Option<i64>,
    #[serde(default)]
    pub allowed_reflection_task_ids: Vec<i64>,
    pub tickers: Vec<String>,
    #[serde(default)]
    pub alpaca_live: bool,
    #[serde(default)]
    pub alpaca_market_data: bool,
    #[serde(skip)]
    pub alpaca_api_key: Option<String>,
    #[serde(skip)]
    pub alpaca_api_secret: Option<String>,
    #[serde(skip)]
    pub phase_summary_index: Option<std::sync::Arc<orchestrator_sql::PhaseSummaryMemoryIndex>>,
    #[serde(skip)]
    pub phase_summary_gate: Option<std::sync::Arc<orchestrator_sql::PhaseSummaryGate>>,
}

impl Default for ExternalToolConfig {
    fn default() -> Self {
        Self {
            project_root: PathBuf::from("."),
            db_path: None,
            run_dir: None,
            run_id: None,
            phase: None,
            allowed_reflection_task_ids: Vec::new(),
            tickers: Vec::new(),
            alpaca_live: false,
            alpaca_market_data: false,
            alpaca_api_key: None,
            alpaca_api_secret: None,
            phase_summary_index: None,
            phase_summary_gate: None,
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
        name: read_phase_summaries::NAME,
        definition: read_phase_summaries::definition,
    },
    ToolEntry {
        name: read_phase_summary_details::NAME,
        definition: read_phase_summary_details::definition,
    },
    ToolEntry {
        name: read_experience::NAME,
        definition: read_experience::definition,
    },
    ToolEntry {
        name: read_reflection_source::NAME,
        definition: read_reflection_source::definition,
    },
    ToolEntry {
        name: web_run::NAME,
        definition: web_run::definition,
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
        name: alpaca::GET_PORTFOLIO_NAME,
        definition: alpaca::get_portfolio_definition,
    },
    ToolEntry {
        name: alpaca::GET_HISTORY_NAME,
        definition: alpaca::get_history_definition,
    },
    ToolEntry {
        name: alpaca::GET_PRICE_NAME,
        definition: alpaca::get_price_definition,
    },
    ToolEntry {
        name: alpaca::GET_NEWS_NAME,
        definition: alpaca::get_news_definition,
    },
    ToolEntry {
        name: alpaca::SUBMIT_TRADE_NAME,
        definition: alpaca::submit_trade_definition,
    },
];

pub fn tool_names() -> &'static [&'static str] {
    // Exclude think (always enabled via runtime, not listed in explicit names)
    // and web.run (conditionally added).
    &[
        read_phase_summaries::NAME,
        read_phase_summary_details::NAME,
        read_experience::NAME,
        read_reflection_source::NAME,
        read_technical_csv::NAME,
        read_jin10_csv::NAME,
        alpaca::GET_NEWS_NAME,
    ]
}

pub fn enabled_tool_names(
    web_run: Option<&WebSearchConfig>,
    alpaca_live: bool,
    alpaca_market_data: bool,
) -> Vec<&'static str> {
    let mut names = tool_names()
        .iter()
        .copied()
        .filter(|name| *name != alpaca::GET_NEWS_NAME)
        .collect::<Vec<_>>();
    if web_run.is_some() {
        names.push(web_run::NAME);
    }
    if alpaca_live {
        names.extend([
            alpaca::GET_PORTFOLIO_NAME,
            alpaca::GET_HISTORY_NAME,
            alpaca::GET_PRICE_NAME,
            alpaca::SUBMIT_TRADE_NAME,
        ]);
    }
    if alpaca_market_data {
        names.push(alpaca::GET_NEWS_NAME);
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
        read_phase_summaries::NAME => {
            let result = read_phase_summaries::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        read_phase_summary_details::NAME => {
            let result = read_phase_summary_details::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        read_experience::NAME => {
            let result = read_experience::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        read_reflection_source::NAME => {
            let result = read_reflection_source::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
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
        read_technical_csv::NAME => read_technical_csv::execute(args, config),
        read_jin10_csv::NAME => read_jin10_csv::execute(args, config),
        alpaca::GET_PORTFOLIO_NAME => alpaca::get_portfolio(config).await,
        alpaca::GET_HISTORY_NAME => alpaca::get_history(config).await,
        alpaca::GET_PRICE_NAME => alpaca::get_price(args, config).await,
        alpaca::GET_NEWS_NAME => alpaca::get_news(args, config).await,
        alpaca::SUBMIT_TRADE_NAME => alpaca::submit_trade(args, config).await,
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
            phase: None,
            allowed_reflection_task_ids: Vec::new(),
            tickers: Vec::new(),
            alpaca_live: false,
            alpaca_market_data: false,
            alpaca_api_key: None,
            alpaca_api_secret: None,
            phase_summary_index: None,
            phase_summary_gate: None,
        }
    }

    fn web_run_runtime<P>(config: WebSearchConfig, provider: P) -> WebRunRuntime
    where
        P: WebSearchProvider + 'static,
    {
        WebRunRuntime::new(config).with_provider(Arc::new(provider))
    }

    #[tokio::test]
    async fn legacy_read_run_context_is_not_model_registered() {
        assert!(tool_definition(read_run_context::NAME).is_none());
        let error = execute_named_tool(
            read_run_context::NAME,
            json!({"kind": "technical"}),
            &external_config(),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("only supports kinds"));
    }

    #[test]
    fn tool_definitions_map_web_run_api_name() {
        let names = [
            web_run::NAME.to_string(),
            read_phase_summaries::NAME.to_string(),
            read_phase_summary_details::NAME.to_string(),
        ];
        let defs: Vec<_> = names.iter().filter_map(|n| tool_definition(n)).collect();
        assert_eq!(defs.len(), 3);
        assert!(defs.iter().any(|tool| tool.name == "web_run"));
        assert!(defs.iter().any(|tool| tool.name == "read_phase_summaries"));
        assert!(defs
            .iter()
            .any(|tool| tool.name == "read_phase_summary_details"));
        assert_eq!(resolve_tool_name("web_run"), web_run::NAME);
    }

    #[test]
    fn every_registered_tool_declares_required_as_an_array() {
        for entry in REGISTRY {
            let definition = (entry.definition)();
            assert!(
                definition
                    .parameters
                    .get("required")
                    .is_some_and(Value::is_array),
                "tool {} must provide a JSON Schema required array",
                entry.name
            );
        }
    }

    #[tokio::test]
    async fn phase_summary_tools_fail_closed_without_turn_context() {
        for (name, args) in [
            (read_phase_summaries::NAME, json!({})),
            (
                read_phase_summary_details::NAME,
                json!({"summary_id": "summary-1"}),
            ),
        ] {
            let error = execute_named_tool(name, args, &external_config(), None, None)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("turn context"));
        }
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
            title: "QQQ macro update".to_string(),
            url: "https://www.reuters.com/markets/example".to_string(),
            content: "QQQ and VIX macro context.".to_string(),
        }]);
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({
                "search_query": "QQQ VIX macro update",
                "include_domains": ["reuters.com"],
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
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("QQQ macro update"));
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
