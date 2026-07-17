use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};
use tracing::{debug, warn};

use crate::agent_loop::ToolRuntimeTurnContext;
use crate::truncation::{truncate_semantic, TruncationConfig};
use crate::web_search::{
    SearchQuery, SearchResult, WebSearchContextSize, WebSearchMode, WebSearchOptions,
};
pub use crate::web_search::{WebSearchConfig, WebSearchProvider};

use orchestrator_ingest::{jin10, social, technical, wayinvideo, youtube};

pub const WEB_RUN_TOOL_NAME: &str = "web.run";
const WEB_RUN_MAX_SEARCH_QUERIES: usize = 4;
const WEB_RUN_MAX_QUERY_CHARS: usize = 512;
pub const READ_RUN_CONTEXT_TOOL_NAME: &str = "read_run_context";
pub const FETCH_JIN10_FLASH_TOOL_NAME: &str = "fetch_jin10_flash";
pub const FETCH_YOUTUBE_TRANSCRIPT_TOOL_NAME: &str = "fetch_youtube_transcript";
pub const FETCH_WAYINVIDEO_TRANSCRIPT_TOOL_NAME: &str = "fetch_wayinvideo_transcript";
pub const RUN_TECHNICAL_INDICATORS_TOOL_NAME: &str = "run_technical_indicators";
pub const FETCH_LAST30DAYS_CONTEXT_TOOL_NAME: &str = "fetch_last30days_context";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExternalToolConfig {
    pub project_root: PathBuf,
    pub db_path: Option<PathBuf>,
    pub run_dir: Option<PathBuf>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub tickers: Vec<String>,
    /// In-run phase00 index snapshot (optional cache).
    #[serde(skip)]
    pub phase00_index: Option<std::sync::Arc<orchestrator_sql::Phase00MemoryIndex>>,
    /// Shared gate: wait for in-flight phase00 compress before serving index tools.
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

pub type SharedWebSearchProvider = Arc<dyn WebSearchProvider>;

#[derive(Clone)]
pub struct WebRunRuntime {
    config: WebSearchConfig,
    provider: Option<SharedWebSearchProvider>,
    truncation: TruncationConfig,
}

impl WebRunRuntime {
    pub fn new(config: WebSearchConfig) -> Self {
        Self {
            config,
            provider: None,
            truncation: TruncationConfig::default(),
        }
    }

    pub fn with_truncation(mut self, truncation: TruncationConfig) -> Self {
        self.truncation = truncation;
        self
    }

    pub fn with_provider(mut self, provider: SharedWebSearchProvider) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn config(&self) -> &WebSearchConfig {
        &self.config
    }

    pub async fn execute(&self, args: Value) -> Result<Value> {
        execute_web_run(
            args,
            &self.config,
            self.provider.as_deref(),
            &self.truncation,
        )
        .await
    }
}

#[derive(Debug, Clone)]
pub struct FetchJin10FlashTool;

impl FetchJin10FlashTool {
    pub const NAME: &'static str = FETCH_JIN10_FLASH_TOOL_NAME;
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchJin10FlashArgs {
    #[serde(default)]
    pub lookback_hours: Option<f64>,
    #[serde(default)]
    pub pages: Option<usize>,
    #[serde(default)]
    pub classify: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
}

impl FetchJin10FlashArgs {
    fn to_ingest_args(&self) -> jin10::Jin10Args {
        jin10::Jin10Args {
            lookback_hours: self.lookback_hours,
            pages: self.pages,
            classify: self.classify.clone(),
            channel: None,
            vip: None,
            sleep: None,
            timeout: None,
            output: String::new(),
            jsonl: String::new(),
            pretty: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchYoutubeTranscriptTool;

impl FetchYoutubeTranscriptTool {
    pub const NAME: &'static str = FETCH_YOUTUBE_TRANSCRIPT_TOOL_NAME;
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchYoutubeTranscriptArgs {
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

#[derive(Debug, Clone)]
pub struct FetchWayinVideoTranscriptTool;

impl FetchWayinVideoTranscriptTool {
    pub const NAME: &'static str = FETCH_WAYINVIDEO_TRANSCRIPT_TOOL_NAME;
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchWayinVideoTranscriptArgs {
    pub url: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub published: Option<String>,
    #[serde(default)]
    pub task: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReadRunContextTool;

impl ReadRunContextTool {
    pub const NAME: &'static str = READ_RUN_CONTEXT_TOOL_NAME;
}

#[derive(Debug, Clone)]
pub struct RunTechnicalIndicatorsTool;

impl RunTechnicalIndicatorsTool {
    pub const NAME: &'static str = RUN_TECHNICAL_INDICATORS_TOOL_NAME;
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunTechnicalIndicatorsArgs {
    /// Preferred field name. Models often send `tickers`, so accept that alias too.
    #[serde(default, alias = "tickers")]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub intervals: Vec<String>,
    #[serde(default)]
    pub start: Option<String>,
    #[serde(default)]
    pub end: Option<String>,
    #[serde(default)]
    pub days: Option<i64>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub db_path: Option<String>,
}

impl RunTechnicalIndicatorsArgs {
    fn to_ingest_args(&self, db_path: Option<PathBuf>) -> technical::TechnicalArgs {
        technical::TechnicalArgs {
            symbols: if self.symbols.is_empty() {
                None
            } else {
                Some(self.symbols.join(","))
            },
            intervals: self.intervals.join(","),
            start: self.start.clone(),
            end: self.end.clone(),
            days: self.days,
            db_path,
            model: self.model.clone(),
            timeout: None,
            sleep: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchLast30DaysContextTool;

impl FetchLast30DaysContextTool {
    pub const NAME: &'static str = FETCH_LAST30DAYS_CONTEXT_TOOL_NAME;
}

#[derive(Debug, Clone, Deserialize)]
pub struct FetchLast30DaysContextArgs {
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

impl FetchLast30DaysContextArgs {
    fn effective_tickers(&self) -> Vec<String> {
        if !self.tickers.is_empty() {
            return self.tickers.clone();
        }
        self.ticker.clone().into_iter().collect()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebRunInput {
    #[serde(default, deserialize_with = "deserialize_web_run_search_query")]
    pub search_query: Vec<WebRunSearchQueryInput>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub include_domains: Vec<String>,
    #[serde(default)]
    pub response_length: WebRunResponseLength,
    #[serde(default)]
    pub num_results: Option<usize>,
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebRunSearchQueryInput {
    pub q: String,
    #[serde(default)]
    pub recency: Option<u32>,
    #[serde(default)]
    pub domains: Vec<String>,
    #[serde(default, alias = "include_domains")]
    pub include_domains: Vec<String>,
    #[serde(default, alias = "numResults")]
    pub num_results: Option<usize>,
    #[serde(default, alias = "type")]
    pub search_type: Option<String>,
    #[serde(default)]
    pub livecrawl: Option<String>,
    #[serde(default, alias = "contextMaxCharacters")]
    pub context_max_characters: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebRunResponseLength {
    Short,
    #[default]
    Medium,
    Long,
}

pub fn web_run_tool_parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "search_query": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "q": {"type": "string"},
                        "recency": {"type": "integer"},
                        "domains": {"type": "array", "items": {"type": "string"}},
                        "numResults": {"type": "integer", "minimum": 1, "maximum": 20},
                        "type": {"type": "string", "enum": ["auto", "fast", "deep"]},
                        "livecrawl": {"type": "string", "enum": ["fallback", "preferred"]},
                        "contextMaxCharacters": {"type": "integer", "minimum": 1, "maximum": 50000}
                    },
                    "required": ["q"]
                }
            },
            "response_length": {"type": "string", "enum": ["short", "medium", "long"]}
        }
    })
}

async fn execute_web_run(
    args: Value,
    config: &WebSearchConfig,
    provider: Option<&dyn WebSearchProvider>,
    truncation: &TruncationConfig,
) -> Result<Value> {
    debug!(
        mode = ?config.mode,
        "web.run starting"
    );
    if config.mode == WebSearchMode::Disabled {
        debug!("web.run disabled");
        return Ok(safe_web_run_error("Web search is disabled."));
    }

    let args = match serde_json::from_value::<WebRunInput>(args) {
        Ok(args) => args,
        Err(_) => return Ok(safe_web_run_error("Invalid web.run arguments.")),
    };
    let args = normalize_web_run_input(args);
    if args.search_query.is_empty() {
        return Ok(safe_web_run_error("web.run requires search_query for v1."));
    }
    if args.search_query.len() > WEB_RUN_MAX_SEARCH_QUERIES {
        return Ok(safe_web_run_error(&format!(
            "web.run search_query supports at most {WEB_RUN_MAX_SEARCH_QUERIES} queries."
        )));
    }

    let Some(provider) = provider else {
        warn!("web.run provider missing");
        return Ok(safe_web_run_error("web.run provider is not configured."));
    };
    let response_length = args.response_length;
    let queries = match validated_web_search_queries(args) {
        Ok(queries) => queries,
        Err(error) => return Ok(safe_web_run_error(&error.to_string())),
    };
    if let Err(error) = validate_query_domain_policy(&queries, config) {
        return Ok(safe_web_run_error(&error.to_string()));
    }

    let search_queries = queries
        .iter()
        .map(|query| SearchQuery {
            q: query.query.clone(),
            recency: query.recency,
            domains: query.domains.clone(),
            num_results: query.num_results,
            search_type: query.search_type.clone(),
            livecrawl: query.livecrawl.clone(),
            context_max_characters: query.context_max_characters,
        })
        .collect::<Vec<_>>();
    debug!(
        query_count = search_queries.len(),
        response_length = ?response_length,
        "web.run provider search starting"
    );
    let options = WebSearchOptions {
        context_size: response_length_context_size(response_length),
        allowed_domains: config.allowed_domains.clone(),
        blocked_domains: config.blocked_domains.clone(),
        max_result_chars: config.max_result_chars,
        truncation: truncation.clone(),
    };
    let results = match provider.search(search_queries, options).await {
        Ok(results) => results,
        Err(error) => {
            warn!(error = %error, "web.run provider failed");
            return Ok(safe_web_run_error("web.run provider failed."));
        }
    };
    let results = filter_web_search_results(results, config);
    debug!(result_count = results.len(), "web.run completed");
    let text = truncate_semantic(
        &format_web_search_results(&results),
        config.max_result_chars,
        truncation,
    );
    Ok(json!({
        "status": "success",
        "tool": WEB_RUN_TOOL_NAME,
        "content": text,
        "text": text,
        "results": results_to_json(&results),
    }))
}

fn safe_web_run_error(message: &str) -> Value {
    json!({
        "status": "error",
        "tool": WEB_RUN_TOOL_NAME,
        "content": message,
        "text": message,
    })
}

fn response_length_context_size(response_length: WebRunResponseLength) -> WebSearchContextSize {
    match response_length {
        WebRunResponseLength::Short => WebSearchContextSize::Low,
        WebRunResponseLength::Medium => WebSearchContextSize::Medium,
        WebRunResponseLength::Long => WebSearchContextSize::High,
    }
}

fn normalize_last30days_source(source: Option<&str>) -> Option<&str> {
    source.map(|value| match value.trim() {
        "twitter" | "x_twitter" | "x-twitter" => "x",
        other => other,
    })
}

fn deserialize_web_run_search_query<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<WebRunSearchQueryInput>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(q) => Ok(vec![WebRunSearchQueryInput {
            q,
            recency: None,
            domains: Vec::new(),
            include_domains: Vec::new(),
            num_results: None,
            search_type: None,
            livecrawl: None,
            context_max_characters: None,
        }]),
        Value::Array(_) => serde_json::from_value(value).map_err(serde::de::Error::custom),
        Value::Null => Ok(Vec::new()),
        other => Err(serde::de::Error::custom(format!(
            "expected string or array for search_query, got {other}"
        ))),
    }
}

fn normalize_web_run_input(mut args: WebRunInput) -> WebRunInput {
    if args.search_query.is_empty() {
        if let Some(query) = args
            .query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            args.search_query.push(WebRunSearchQueryInput {
                q: query.to_string(),
                recency: None,
                domains: Vec::new(),
                include_domains: Vec::new(),
                num_results: args.num_results,
                search_type: None,
                livecrawl: None,
                context_max_characters: None,
            });
        }
    }
    if let Some(num_results) = args.num_results {
        for query in &mut args.search_query {
            if query.num_results.is_none() {
                query.num_results = Some(num_results);
            }
        }
    }
    if !args.include_domains.is_empty() {
        for query in &mut args.search_query {
            query.domains.extend(args.include_domains.clone());
        }
    }
    args
}

fn validated_web_search_queries(args: WebRunInput) -> Result<Vec<SearchQueryRequest>> {
    args.search_query
        .into_iter()
        .enumerate()
        .map(|(index, query)| {
            let q = query.q.trim();
            if q.is_empty() {
                bail!("web.run search_query[{index}].q is required");
            }
            if q.chars().count() > WEB_RUN_MAX_QUERY_CHARS {
                bail!("web.run search_query[{index}].q exceeds {WEB_RUN_MAX_QUERY_CHARS} chars");
            }
            let domains = query
                .domains
                .into_iter()
                .chain(query.include_domains)
                .filter_map(|domain| normalize_domain(&domain))
                .collect::<Vec<_>>();
            if let Some(num_results) = query.num_results {
                if !(1..=20).contains(&num_results) {
                    bail!("web.run search_query[{index}].numResults must be between 1 and 20");
                }
            }
            if let Some(search_type) = query.search_type.as_deref() {
                if !matches!(search_type, "auto" | "fast" | "deep") {
                    bail!("web.run search_query[{index}].type must be auto, fast, or deep");
                }
            }
            if let Some(livecrawl) = query.livecrawl.as_deref() {
                if !matches!(livecrawl, "fallback" | "preferred") {
                    bail!("web.run search_query[{index}].livecrawl must be fallback or preferred");
                }
            }
            if let Some(context_max_characters) = query.context_max_characters {
                if !(1..=50_000).contains(&context_max_characters) {
                    bail!(
                        "web.run search_query[{index}].contextMaxCharacters must be between 1 and 50000"
                    );
                }
            }
            Ok(SearchQueryRequest {
                query: q.to_string(),
                recency: query.recency,
                domains,
                response_length: args.response_length,
                num_results: query.num_results,
                search_type: query.search_type,
                livecrawl: query.livecrawl,
                context_max_characters: query.context_max_characters,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchQueryRequest {
    query: String,
    recency: Option<u32>,
    domains: Vec<String>,
    response_length: WebRunResponseLength,
    num_results: Option<usize>,
    search_type: Option<String>,
    livecrawl: Option<String>,
    context_max_characters: Option<usize>,
}

fn validate_query_domain_policy(
    queries: &[SearchQueryRequest],
    config: &WebSearchConfig,
) -> Result<()> {
    for query in queries {
        for domain in &query.domains {
            if domain_matches_any(domain, &config.blocked_domains) {
                bail!("web.run query domain is blocked: {domain}");
            }
            if !config.allowed_domains.is_empty()
                && !domain_matches_any(domain, &config.allowed_domains)
            {
                bail!("web.run query domain is not allowed: {domain}");
            }
        }
    }
    Ok(())
}

fn filter_web_search_results(
    results: Vec<SearchResult>,
    config: &WebSearchConfig,
) -> Vec<SearchResult> {
    results
        .into_iter()
        .filter_map(|result| {
            let host = http_url_host(&result.url)?;
            if domain_matches_any(&host, &config.blocked_domains) {
                return None;
            }
            if !config.allowed_domains.is_empty()
                && !domain_matches_any(&host, &config.allowed_domains)
            {
                return None;
            }
            Some(SearchResult {
                ref_id: result.ref_id,
                title: clean_text_field(&result.title),
                url: sanitize_http_url(&result.url),
                published_at: result.published_at.map(|value| clean_text_field(&value)),
                snippet: clean_text_field(&result.snippet),
                source: result.source.map(|value| clean_text_field(&value)),
            })
        })
        .collect()
}

fn format_web_search_results(results: &[SearchResult]) -> String {
    let mut output = String::from("Search results:\n");
    if results.is_empty() {
        output.push_str("No results after filtering.\n");
    } else {
        for (index, result) in results.iter().enumerate() {
            if index > 0 {
                output.push('\n');
            }
            output.push_str(&format!("[ref_id: search{index}]\n"));
            output.push_str(&format!("Title: {}\n", result.title));
            output.push_str(&format!("URL: {}\n", result.url));
            output.push_str(&format!(
                "Published: {}\n",
                result.published_at.as_deref().unwrap_or("unknown")
            ));
            output.push_str(&format!("Snippet: {}\n", result.snippet));
        }
    }
    output
}

fn results_to_json(results: &[SearchResult]) -> Value {
    Value::Array(
        results
            .iter()
            .enumerate()
            .map(|(index, result)| {
                json!({
                    "ref_id": if result.ref_id.is_empty() { format!("search{index}") } else { result.ref_id.clone() },
                    "title": result.title.clone(),
                    "url": result.url.clone(),
                    "published": result.published_at.clone(),
                    "snippet": result.snippet.clone(),
                })
            })
            .collect(),
    )
}

fn http_url_host(value: &str) -> Option<String> {
    let value = value.trim();
    let lower = value.to_ascii_lowercase();
    let rest = if lower.starts_with("https://") {
        &value[8..]
    } else if lower.starts_with("http://") {
        &value[7..]
    } else {
        return None;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority.rsplit('@').next().unwrap_or(authority);
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        rest.split(']').next().unwrap_or_default()
    } else {
        host_port.split(':').next().unwrap_or_default()
    };
    normalize_domain(host)
}

fn normalize_domain(value: &str) -> Option<String> {
    let value = value
        .trim()
        .trim_start_matches("*.")
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let value = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .unwrap_or(&value)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .trim_end_matches('.');
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn domain_matches_any(host: &str, domains: &[String]) -> bool {
    domains
        .iter()
        .filter_map(|domain| normalize_domain(domain))
        .any(|domain| domain_matches(host, &domain))
}

fn domain_matches(host: &str, domain: &str) -> bool {
    host == domain || host.ends_with(&format!(".{domain}"))
}

fn sanitize_http_url(value: &str) -> String {
    let value = value.trim();
    let end = value.find(['?', '#']).unwrap_or(value.len());
    value[..end].to_string()
}

fn clean_text_field(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

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

pub fn tool_names() -> &'static [&'static str] {
    &[
        READ_RUN_CONTEXT_TOOL_NAME,
        FetchJin10FlashTool::NAME,
        FetchYoutubeTranscriptTool::NAME,
        FetchWayinVideoTranscriptTool::NAME,
        RunTechnicalIndicatorsTool::NAME,
        FetchLast30DaysContextTool::NAME,
    ]
}

pub fn enabled_tool_names(web_run: Option<&WebSearchConfig>) -> Vec<&'static str> {
    let mut names = tool_names().to_vec();
    if web_run.is_some_and(|config| config.mode != WebSearchMode::Disabled) {
        names.push(WEB_RUN_TOOL_NAME);
    }
    names
}

/// Build rig-native tool definitions for the names available in this turn.
pub fn rig_tool_definitions(names: &[String]) -> Vec<rig_core::completion::ToolDefinition> {
    names
        .iter()
        .filter_map(|name| tool_definition(name))
        .collect()
}

/// OpenAI-compatible function names reject `.`; map internal names to API-safe form.
pub fn api_tool_name(name: &str) -> String {
    name.replace('.', "_")
}

/// Map a model-emitted function name back to the internal tool id.
pub fn resolve_tool_name(api_name: &str) -> String {
    match api_name {
        "web_run" => WEB_RUN_TOOL_NAME.to_string(),
        other => other.to_string(),
    }
}

pub fn tool_definition(name: &str) -> Option<rig_core::completion::ToolDefinition> {
    let (description, parameters) = match name {
        "think" => (
            "Record a short internal note before calling another tool or emitting the final artifact.",
            json!({
                "type": "object",
                "properties": {
                    "note": {"type": "string", "description": "Brief planning note"}
                },
                "additionalProperties": true
            }),
        ),
        READ_RUN_CONTEXT_TOOL_NAME => (
            "Read structured SQLite run context for the current role (technical, jin10, compose_context, research_inputs, etc.).",
            json!({
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "description": "Context kind. Analysts use technical/jin10; debate roles use compose_context then research_inputs."
                    },
                    "ticker": {"type": "string"},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "token_budget": {"type": "integer", "minimum": 256}
                },
                "additionalProperties": true
            }),
        ),
        WEB_RUN_TOOL_NAME => (
            "Live web search via the configured provider. Prefer focused queries with domains when possible.",
            json!({
                "type": "object",
                "properties": {
                    "search_query": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "q": {"type": "string"},
                                "domains": {"type": "array", "items": {"type": "string"}},
                                "numResults": {"type": "integer", "minimum": 1, "maximum": 20}
                            },
                            "required": ["q"]
                        }
                    },
                    "response_length": {"type": "string"}
                },
                "required": ["search_query"],
                "additionalProperties": true
            }),
        ),
        FETCH_JIN10_FLASH_TOOL_NAME => (
            "Fetch recent Jin10 flash news and import into SQLite.",
            json!({
                "type": "object",
                "properties": {
                    "lookback_hours": {"type": "number"},
                    "pages": {"type": "integer"},
                    "classify": {"type": "string"}
                },
                "additionalProperties": true
            }),
        ),
        FETCH_YOUTUBE_TRANSCRIPT_TOOL_NAME => (
            "Fetch YouTube video transcripts for configured channels or a specific URL.",
            json!({
                "type": "object",
                "properties": {
                    "all": {"type": "boolean"},
                    "channel": {"type": "string"},
                    "url": {"type": "string"},
                    "max_videos": {"type": "integer"}
                },
                "additionalProperties": true
            }),
        ),
        FETCH_WAYINVIDEO_TRANSCRIPT_TOOL_NAME => (
            "Fetch a WayinVideo transcript for a URL.",
            json!({
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                    "title": {"type": "string"},
                    "published": {"type": "string"},
                    "task": {"type": "string"},
                    "task_id": {"type": "string"}
                },
                "required": ["url"],
                "additionalProperties": true
            }),
        ),
        RUN_TECHNICAL_INDICATORS_TOOL_NAME => (
            "Run technical indicators for symbols/intervals and import bars into SQLite.",
            json!({
                "type": "object",
                "properties": {
                    "symbols": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "intervals": {"type": "array", "items": {"type": "string"}},
                    "start": {"type": "string"},
                    "end": {"type": "string"},
                    "days": {"type": "integer"},
                    "model": {"type": "string"}
                },
                "additionalProperties": true
            }),
        ),
        FETCH_LAST30DAYS_CONTEXT_TOOL_NAME => (
            "Fetch last-30-days social/web context for tickers/source.",
            json!({
                "type": "object",
                "properties": {
                    "source": {"type": "string", "description": "e.g. reddit, x, youtube"},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "query": {"type": "string"}
                },
                "additionalProperties": true
            }),
        ),
        _ => return None,
    };
    Some(rig_core::completion::ToolDefinition {
        name: api_tool_name(name),
        description: description.to_string(),
        parameters,
    })
}

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
        READ_RUN_CONTEXT_TOOL_NAME => {
            let result = execute_read_run_context(args, config, turn_context);
            log_named_tool_result(name, &result);
            result
        }
        WEB_RUN_TOOL_NAME => {
            if let Some(web_run) = web_run {
                let result = web_run.execute(args).await;
                log_named_tool_result(name, &result);
                result
            } else {
                let result = Ok(safe_web_run_error("Web search is disabled."));
                log_named_tool_result(name, &result);
                result
            }
        }
        FetchJin10FlashTool::NAME => {
            let tool_args = serde_json::from_value::<FetchJin10FlashArgs>(args)
                .context("invalid fetch_jin10_flash arguments")?;
            let ingest_args = tool_args.to_ingest_args();
            let mut result = jin10::run(ingest_args)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            // Persist flash items so later read_run_context(kind=jin10) works even
            // after conversation compaction drops the large fetch payload.
            let mut conn = tool_connection(config)?;
            let imported = orchestrator_sql::import_jin10_payload(&mut conn, &result)?;
            let jin10_context = orchestrator_sql::read_run_context(
                &mut conn,
                &orchestrator_sql::RunContextReadRequest {
                    kind: "jin10".to_string(),
                    run_id: turn_context.map(|context| context.run_id.clone()),
                    ticker: config.tickers.first().cloned(),
                    tickers: config.tickers.clone(),
                    phase: None,
                    role: turn_context.map(|context| context.role.clone()),
                    topic_id: None,
                    turn_id: turn_context.map(|context| context.turn_id.clone()),
                    persist_context: false,
                    token_budget: None,
                },
            )?;
            if let Some(object) = result.as_object_mut() {
                // Prefer the compact DB snapshot in the tool result so the model
                // can finish without another read_run_context round-trip.
                object.remove("items");
                object.insert("imported_rows".to_string(), json!(imported));
                object.insert("jin10_context".to_string(), jin10_context);
            }
            log_named_tool_result(name, &Ok(result.clone()));
            Ok(result)
        }
        FetchYoutubeTranscriptTool::NAME => {
            let tool_args = serde_json::from_value::<FetchYoutubeTranscriptArgs>(args)
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
            log_named_tool_result(name, &result);
            result
        }
        FetchWayinVideoTranscriptTool::NAME => {
            let tool_args = serde_json::from_value::<FetchWayinVideoTranscriptArgs>(args)
                .context("invalid fetch_wayinvideo_transcript arguments")?;
            let ingest_args = wayinvideo::WayinVideoArgs {
                url: tool_args.url,
                title: tool_args.title,
                published: tool_args.published,
                task: tool_args.task,
                task_id: tool_args.task_id,
                output: tool_args.output,
            };
            let result = wayinvideo::run(ingest_args)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"));
            log_named_tool_result(name, &result);
            result
        }
        RunTechnicalIndicatorsTool::NAME => {
            let tool_args = serde_json::from_value::<RunTechnicalIndicatorsArgs>(args)
                .context("invalid run_technical_indicators arguments")?;
            let db_path = config.db_path.clone();
            let ingest_args = tool_args.to_ingest_args(db_path);
            let mut result = technical::run(ingest_args)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            // When rows already exist, the ingest call only reports `skipped`.
            // Attach compact snapshots so the agent can finish without another
            // read_run_context round-trip.
            let tickers = if tool_args.symbols.is_empty() {
                config.tickers.clone()
            } else {
                tool_args.symbols.clone()
            };
            let mut conn = tool_connection(config)?;
            let technical_context = orchestrator_sql::read_run_context(
                &mut conn,
                &orchestrator_sql::RunContextReadRequest {
                    kind: "technical".to_string(),
                    run_id: turn_context.map(|context| context.run_id.clone()),
                    ticker: tickers.first().cloned(),
                    tickers,
                    phase: None,
                    role: turn_context.map(|context| context.role.clone()),
                    topic_id: None,
                    turn_id: turn_context.map(|context| context.turn_id.clone()),
                    persist_context: false,
                    token_budget: None,
                },
            )?;
            if let Some(object) = result.as_object_mut() {
                object.insert("technical_context".to_string(), technical_context);
            }
            log_named_tool_result(name, &Ok(result.clone()));
            Ok(result)
        }
        FetchLast30DaysContextTool::NAME => {
            let tool_args = serde_json::from_value::<FetchLast30DaysContextArgs>(args)
                .context("invalid fetch_last30days_context arguments")?;
            let tickers = tool_args.effective_tickers();
            let tickers = if tickers.is_empty() {
                config.tickers.clone()
            } else {
                tickers
            };
            let source = normalize_last30days_source(tool_args.source.as_deref());
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
            log_named_tool_result(name, &result);
            result
        }
        other => bail!("unknown tool name: {other}"),
    }
}

fn log_named_tool_result(name: &str, result: &Result<Value>) {
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

fn execute_read_run_context(
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
) -> Result<Value> {
    let mut request = serde_json::from_value::<orchestrator_sql::RunContextReadRequest>(args)
        .context("invalid read_run_context arguments")?;
    if request.run_id.is_none() {
        request.run_id = turn_context.map(|context| context.run_id.clone());
    }
    if request.role.is_none() {
        request.role = turn_context.map(|context| context.role.clone());
    }
    if request.phase.is_none() {
        request.phase = turn_context.and_then(|context| context.phase);
    }
    if request.tickers.is_empty() {
        request.tickers = config.tickers.clone();
    }
    let mut conn = tool_connection(config)?;
    let prior_reads = turn_context
        .map(|context| count_turn_tool_results(&conn, &context.turn_id, READ_RUN_CONTEXT_TOOL_NAME))
        .transpose()?
        .unwrap_or(0);
    if request.kind.trim().is_empty() {
        request.kind = match request.role.as_deref() {
            Some("analyst.technical") => "technical".to_string(),
            Some("analyst.news_macro") => "jin10".to_string(),
            // Phase-2+ roles expand Phase-00 compressor tables only (memory index).
            Some(role)
                if role.starts_with("researcher.")
                    || role.starts_with("mediator.")
                    || role.starts_with("manager.")
                    || role.starts_with("risk.")
                    || matches!(role, "trader" | "portfolio.manager" | "allocation.manager") =>
            {
                "phase_summaries".to_string()
            }
            _ => String::new(),
        };
    }
    // Phase-2+: only phase00 / attention; block raw market re-reads.
    if request.role.as_deref().is_some_and(is_phase2_plus_role) {
        let allowed = matches!(
            request.kind.as_str(),
            "phase_summaries"
                | "prior_phase_summaries"
                | "phase_summary_details"
                | "attention"
                | "attention_expand"
        );
        if !allowed {
            bail!(
                "role {:?} may only call read_run_context kinds \
                 phase_summaries|phase_summary_details|attention|attention_expand; got {:?}",
                request.role,
                request.kind
            );
        }
    }
    if request.kind == "compose_context" && request.token_budget.is_none() {
        request.token_budget = Some(4096);
    }

    // If phase00 compress is still running for prior phases, wait before serving index tools.
    maybe_wait_phase00_gate(config, &request);

    let evidence = if let Some(from_mem) = try_read_phase00_from_memory(config, &request) {
        from_mem
    } else {
        orchestrator_sql::read_run_context(&mut conn, &request)?
    };
    wrap_read_run_context_evidence(config, &request, prior_reads, evidence)
}

fn maybe_wait_phase00_gate(
    config: &ExternalToolConfig,
    request: &orchestrator_sql::RunContextReadRequest,
) {
    let needs_index = matches!(
        request.kind.as_str(),
        "phase_summaries"
            | "prior_phase_summaries"
            | "phase_summary_details"
            | "attention_expand"
    );
    if !needs_index {
        return;
    }
    let gate = config
        .phase00_gate
        .clone()
        .or_else(|| {
            config
                .run_id
                .as_deref()
                .and_then(orchestrator_sql::phase00_gate)
        });
    let Some(gate) = gate else {
        return;
    };
    // Wait for compress of phases strictly before current role phase (if known).
    let max_prior = request.phase.filter(|p| *p > 0).map(|p| p - 1);
    let ok = gate.wait_until_ready(max_prior, std::time::Duration::from_secs(600));
    let _ = ok; // on timeout, try_read still serves partial index
}

fn is_phase2_plus_role(role: &str) -> bool {
    role.starts_with("researcher.")
        || role.starts_with("mediator.")
        || role.starts_with("manager.")
        || role.starts_with("risk.")
        || matches!(role, "trader" | "portfolio.manager" | "allocation.manager")
}

fn try_read_phase00_from_memory(
    config: &ExternalToolConfig,
    request: &orchestrator_sql::RunContextReadRequest,
) -> Option<Value> {
    // Prefer live gate snapshot (post-wait); fall back to static snapshot.
    let owned;
    let index: &orchestrator_sql::Phase00MemoryIndex = if let Some(gate) = config
        .phase00_gate
        .as_ref()
        .cloned()
        .or_else(|| {
            config
                .run_id
                .as_deref()
                .and_then(orchestrator_sql::phase00_gate)
        })
    {
        owned = gate.snapshot();
        &owned
    } else if let Some(idx) = config.phase00_index.as_ref() {
        idx.as_ref()
    } else {
        return None;
    };

    match request.kind.as_str() {
        "phase_summaries" | "prior_phase_summaries" => {
            let max_source_phase = request.phase.filter(|p| *p > 0).map(|p| p - 1);
            Some(index.list_summaries(
                max_source_phase,
                request.ticker.as_deref().filter(|t| !t.is_empty()),
            ))
        }
        "phase_summary_details" => {
            let summary_id = request
                .topic_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .or_else(|| request.turn_id.as_deref().filter(|s| !s.is_empty()))
                .unwrap_or_default();
            if summary_id.is_empty() {
                return None;
            }
            Some(index.list_details(summary_id))
        }
        "attention_expand" => {
            let mut items = Vec::new();
            let mut any = false;
            for entry in &request.tickers {
                if let Some((kind, id)) = entry.split_once(':') {
                    let kind = kind.trim();
                    let id = id.trim();
                    if kind.is_empty() || id.is_empty() {
                        continue;
                    }
                    match kind {
                        "summary" => {
                            if let Some(v) = index.expand_summary(id) {
                                items.push(v);
                                any = true;
                            }
                        }
                        "detail" => {
                            if let Some(v) = index.expand_detail(id) {
                                items.push(v);
                                any = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
            if any {
                Some(json!({
                    "query": "attention_expand",
                    "item_count": items.len(),
                    "items": items,
                    "source": "phase00_memory"
                }))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn wrap_read_run_context_evidence(
    config: &ExternalToolConfig,
    request: &orchestrator_sql::RunContextReadRequest,
    prior_reads: i64,
    evidence: Value,
) -> Result<Value> {
    if request.role.as_deref().is_some_and(|role| {
        role.starts_with("analyst.")
            || role.starts_with("researcher.")
            || role.starts_with("mediator.")
            || role.starts_with("manager.")
            || role.starts_with("risk.")
            || matches!(role, "trader" | "portfolio.manager" | "allocation.manager")
    }) {
        let tickers = if request.tickers.is_empty() {
            config.tickers.clone()
        } else {
            request.tickers.clone()
        };
        let same_default_reread = prior_reads >= 1
            && matches!(
                request.kind.as_str(),
                "technical"
                    | "jin10"
                    | "compose_context"
                    | "research_inputs"
                    | "phase_summaries"
                    | "prior_phase_summaries"
            );
        let artifact_hint = if request
            .role
            .as_deref()
            .is_some_and(|role| role.starts_with("analyst."))
        {
            "Emit one JSON object with id/role for this analyst, status=completed, and per_ticker.<TICKER>.{direction,confidence,report}. direction must be bullish|bearish|neutral|mixed|unobserved; confidence must be a 0..1 number."
        } else {
            "Emit the final JSON artifact required by the role prompt now."
        };
        if same_default_reread {
            return Ok(json!({
                "status": "stop_rereading",
                "role": request.role,
                "tickers": tickers,
                "kind": request.kind,
                "message": format!(
                    "read_run_context already returned evidence in this turn. Do not call it again unless requesting a different kind. {artifact_hint}"
                ),
                "evidence": evidence,
            }));
        }
        return Ok(json!({
            "status": "ok",
            "role": request.role,
            "tickers": tickers,
            "kind": request.kind,
            "message": format!(
                "Evidence payload only. Tickers are listed in this object and the role prompt. {artifact_hint}"
            ),
            "evidence": evidence,
        }));
    }
    Ok(evidence)
}

fn count_turn_tool_results(
    conn: &rusqlite::Connection,
    turn_id: &str,
    tool_name: &str,
) -> Result<i64> {
    let full_context_json: String = match conn.query_row(
        "SELECT full_context_json FROM agent_events WHERE turn_id = ?",
        rusqlite::params![turn_id],
        |row| row.get(0),
    ) {
        Ok(json) => json,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let items: Vec<serde_json::Value> =
        serde_json::from_str(&full_context_json).unwrap_or_default();
    let count = items
        .iter()
        .filter(|item| {
            item.get("event_type").and_then(|v| v.as_str()) == Some("tool_result")
                && item.get("tool_name").and_then(|v| v.as_str()) == Some(tool_name)
        })
        .count() as i64;
    Ok(count)
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
    use crate::web_search::{MockWebPage, MockWebSearchProvider, WebSearchFuture, WebSearchMode};

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
            READ_RUN_CONTEXT_TOOL_NAME,
            json!({"kind": "technical"}),
            &config,
            None,
            None,
        )
        .await
        .unwrap();

        // Wrapped by execute_read_run_context when role is present; here no role so raw payload.
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
    fn rig_tool_definitions_map_web_run_api_name() {
        let defs = rig_tool_definitions(&[
            WEB_RUN_TOOL_NAME.to_string(),
            READ_RUN_CONTEXT_TOOL_NAME.to_string(),
        ]);
        assert_eq!(defs.len(), 2);
        assert!(defs.iter().any(|tool| tool.name == "web_run"));
        assert!(defs.iter().any(|tool| tool.name == "read_run_context"));
        assert_eq!(resolve_tool_name("web_run"), WEB_RUN_TOOL_NAME);
        assert_eq!(
            resolve_tool_name("read_run_context"),
            READ_RUN_CONTEXT_TOOL_NAME
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
            READ_RUN_CONTEXT_TOOL_NAME,
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
            READ_RUN_CONTEXT_TOOL_NAME,
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
        let args: RunTechnicalIndicatorsArgs =
            serde_json::from_value(json!({"tickers": ["QQQ", "SOXX"]})).unwrap();
        assert_eq!(args.symbols, vec!["QQQ".to_string(), "SOXX".to_string()]);
    }

    #[tokio::test]
    async fn run_technical_indicators_is_dispatchable() {
        let error = execute_named_tool(
            RunTechnicalIndicatorsTool::NAME,
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
            WEB_RUN_TOOL_NAME,
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
            WEB_RUN_TOOL_NAME,
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
            WEB_RUN_TOOL_NAME,
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
            WEB_RUN_TOOL_NAME,
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
            WEB_RUN_TOOL_NAME,
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
            WEB_RUN_TOOL_NAME,
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
            WEB_RUN_TOOL_NAME,
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
            _queries: Vec<SearchQuery>,
            _options: WebSearchOptions,
        ) -> WebSearchFuture<'a, Vec<SearchResult>> {
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
            WEB_RUN_TOOL_NAME,
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
