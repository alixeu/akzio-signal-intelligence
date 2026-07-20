use anyhow::{bail, Result};
use rig_core::completion::ToolDefinition;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{debug, warn};

use super::api_tool_name;
use crate::truncation::TruncationConfig;
use crate::web_search::{
    SearchQuery, SearchResult, WebSearchConfig, WebSearchContextSize, WebSearchMode,
    WebSearchOptions, WebSearchProvider,
};

pub const NAME: &str = "web.run";
const MAX_SEARCH_QUERIES: usize = 4;
const MAX_QUERY_CHARS: usize = 512;

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(NAME),
        description: "When a thin headline or flash item is directionally material and you need actual-vs-expected numbers, cross-source confirmation, official wording, or market reaction (yields, FedWatch, VIX, USD, index). Prefer focused English queries and domain filters. Do not use for general browsing, to invent missing CSV/SQLite evidence, or when the imported feed already contains enough detail.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "search_query": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "q": {
                                "type": "string",
                                "description": "Focused search query for the material event to verify."
                            },
                            "domains": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Optional domain allow-list for higher-quality sources."
                            },
                            "numResults": {
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 20,
                                "description": "Optional max results for this query."
                            }
                        },
                        "required": ["q"]
                    }
                },
                "response_length": {
                    "type": "string",
                    "description": "Optional response length hint when the provider supports it."
                }
            },
            "required": ["search_query"],
            "additionalProperties": true
        }),
    }
}

pub type SharedWebSearchProvider = Arc<dyn WebSearchProvider>;

#[derive(Clone)]
pub struct Runtime {
    config: WebSearchConfig,
    provider: Option<SharedWebSearchProvider>,
    truncation: TruncationConfig,
}

impl Runtime {
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
        execute(
            args,
            &self.config,
            self.provider.as_deref(),
            &self.truncation,
        )
        .await
    }
}

pub async fn execute(
    args: Value,
    config: &WebSearchConfig,
    provider: Option<&dyn WebSearchProvider>,
    truncation: &TruncationConfig,
) -> Result<Value> {
    debug!(mode = ?config.mode, "web.run starting");
    if config.mode == WebSearchMode::Disabled {
        debug!("web.run disabled");
        return Ok(safe_error("Web search is disabled."));
    }

    let args = match serde_json::from_value::<Input>(args) {
        Ok(args) => args,
        Err(_) => return Ok(safe_error("Invalid web.run arguments.")),
    };
    let args = normalize_input(args);
    if args.search_query.is_empty() {
        return Ok(safe_error("web.run requires search_query for v1."));
    }
    if args.search_query.len() > MAX_SEARCH_QUERIES {
        return Ok(safe_error(&format!(
            "web.run search_query supports at most {MAX_SEARCH_QUERIES} queries."
        )));
    }

    let Some(provider) = provider else {
        warn!("web.run provider missing");
        return Ok(safe_error("web.run provider is not configured."));
    };
    let response_length = args.response_length;
    let queries = match validated_search_queries(args) {
        Ok(queries) => queries,
        Err(error) => return Ok(safe_error(&error.to_string())),
    };
    if let Err(error) = validate_query_domain_policy(&queries, config) {
        return Ok(safe_error(&error.to_string()));
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
            return Ok(safe_error("web.run provider failed."));
        }
    };
    let results = filter_results(results, config);
    debug!(result_count = results.len(), "web.run completed");
    let text = crate::truncation::truncate_semantic(
        &format_results(&results),
        config.max_result_chars,
        truncation,
    );
    Ok(json!({
        "status": "success",
        "tool": NAME,
        "content": text,
        "text": text,
        "results": results_to_json(&results),
    }))
}

pub fn safe_error(message: &str) -> Value {
    json!({
        "status": "error",
        "tool": NAME,
        "content": message,
        "text": message,
    })
}

// --- Input types ---

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Input {
    #[serde(default, deserialize_with = "deserialize_search_query")]
    pub search_query: Vec<SearchQueryInput>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub include_domains: Vec<String>,
    #[serde(default)]
    pub response_length: ResponseLength,
    #[serde(default)]
    pub num_results: Option<usize>,
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SearchQueryInput {
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
pub enum ResponseLength {
    Short,
    #[default]
    Medium,
    Long,
}

// --- Internal helpers ---

fn deserialize_search_query<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<SearchQueryInput>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(q) => Ok(vec![SearchQueryInput {
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

fn normalize_input(mut args: Input) -> Input {
    if args.search_query.is_empty() {
        if let Some(query) = args
            .query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            args.search_query.push(SearchQueryInput {
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchQueryRequest {
    query: String,
    recency: Option<u32>,
    domains: Vec<String>,
    response_length: ResponseLength,
    num_results: Option<usize>,
    search_type: Option<String>,
    livecrawl: Option<String>,
    context_max_characters: Option<usize>,
}

fn validated_search_queries(args: Input) -> Result<Vec<SearchQueryRequest>> {
    args.search_query
        .into_iter()
        .enumerate()
        .map(|(index, query)| {
            let q = query.q.trim();
            if q.is_empty() {
                bail!("web.run search_query[{index}].q is required");
            }
            if q.chars().count() > MAX_QUERY_CHARS {
                bail!("web.run search_query[{index}].q exceeds {MAX_QUERY_CHARS} chars");
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

fn response_length_context_size(response_length: ResponseLength) -> WebSearchContextSize {
    match response_length {
        ResponseLength::Short => WebSearchContextSize::Low,
        ResponseLength::Medium => WebSearchContextSize::Medium,
        ResponseLength::Long => WebSearchContextSize::High,
    }
}

fn filter_results(results: Vec<SearchResult>, config: &WebSearchConfig) -> Vec<SearchResult> {
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

fn format_results(results: &[SearchResult]) -> String {
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
