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
    #[serde(default)]
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
            let result = jin10::run(ingest_args)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"));
            log_named_tool_result(name, &result);
            result
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
            let result = technical::run(ingest_args)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"));
            log_named_tool_result(name, &result);
            result
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
    if request.kind.trim().is_empty() && request.role.as_deref() == Some("analyst.technical") {
        request.kind = "technical".to_string();
    }
    if request.tickers.is_empty() {
        request.tickers = config.tickers.clone();
    }
    let mut conn = tool_connection(config)?;
    orchestrator_sql::read_run_context(&mut conn, &request)
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

        assert_eq!(output["query"], "get-technical-context");
        assert!(output["daily"].as_array().unwrap().is_empty());
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
