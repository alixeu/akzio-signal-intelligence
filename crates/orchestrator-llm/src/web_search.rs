use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{future::Future, pin::Pin, time::Duration};

const DEFAULT_MAX_RESULT_CHARS: usize = 12_000;
const DEFAULT_MAX_RESULTS: usize = 5;
const DEFAULT_EXA_MCP_URL: &str = "https://mcp.exa.ai/mcp";
const EXA_MCP_TIMEOUT_SECS: u64 = 25;
const EXA_MCP_MAX_BODY_BYTES: u64 = 256 * 1024;
const EXA_DEFAULT_NUM_RESULTS: usize = 8;
const EXA_MAX_NUM_RESULTS: usize = 20;
const EXA_DEFAULT_CONTEXT_MAX_CHARS: usize = 10_000;
const EXA_MAX_CONTEXT_MAX_CHARS: usize = 50_000;
const EXA_NO_RESULTS_MESSAGE: &str = "No search results found. Please try a different query.";

pub type WebSearchFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSearchMode {
    #[default]
    Disabled,
    Cached,
    Live,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSearchProviderKind {
    Exa,
    #[default]
    Mock,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSearchContextSize {
    Low,
    #[default]
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebSearchConfig {
    pub mode: WebSearchMode,
    pub provider: WebSearchProviderKind,
    #[serde(alias = "baseUrl", alias = "baseurl")]
    pub base_url: Option<String>,
    #[serde(alias = "apiKey")]
    pub api_key: Option<String>,
    #[serde(alias = "contextSize")]
    pub context_size: WebSearchContextSize,
    #[serde(alias = "allowedDomains")]
    pub allowed_domains: Vec<String>,
    #[serde(alias = "blockedDomains")]
    pub blocked_domains: Vec<String>,
    #[serde(alias = "maxResultChars")]
    pub max_result_chars: usize,
}

impl Default for WebSearchConfig {
    fn default() -> Self {
        Self {
            mode: WebSearchMode::Disabled,
            provider: WebSearchProviderKind::Mock,
            base_url: None,
            api_key: None,
            context_size: WebSearchContextSize::Medium,
            allowed_domains: Vec::new(),
            blocked_domains: Vec::new(),
            max_result_chars: DEFAULT_MAX_RESULT_CHARS,
        }
    }
}

impl WebSearchConfig {
    pub fn merge_override(&self, role_override: Option<&WebSearchConfigOverride>) -> Self {
        let Some(role_override) = role_override else {
            return self.clone();
        };

        Self {
            mode: role_override.mode.unwrap_or(self.mode),
            provider: role_override.provider.unwrap_or(self.provider),
            base_url: role_override
                .base_url
                .clone()
                .or_else(|| self.base_url.clone()),
            api_key: role_override
                .api_key
                .clone()
                .or_else(|| self.api_key.clone()),
            context_size: role_override.context_size.unwrap_or(self.context_size),
            allowed_domains: role_override
                .allowed_domains
                .clone()
                .unwrap_or_else(|| self.allowed_domains.clone()),
            blocked_domains: role_override
                .blocked_domains
                .clone()
                .unwrap_or_else(|| self.blocked_domains.clone()),
            max_result_chars: role_override
                .max_result_chars
                .unwrap_or(self.max_result_chars),
        }
    }
}

pub fn validate_web_search_runtime_config(config: &WebSearchConfig, role: &str) -> Result<()> {
    match config.mode {
        WebSearchMode::Disabled | WebSearchMode::Cached => Ok(()),
        WebSearchMode::Live => match config.provider {
            WebSearchProviderKind::Mock => Ok(()),
            WebSearchProviderKind::Exa => {
                validate_optional_http_base_url(config.base_url.as_deref(), role)
            }
        },
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebSearchConfigOverride {
    pub mode: Option<WebSearchMode>,
    pub provider: Option<WebSearchProviderKind>,
    #[serde(alias = "baseUrl", alias = "baseurl")]
    pub base_url: Option<String>,
    #[serde(alias = "apiKey")]
    pub api_key: Option<String>,
    #[serde(alias = "contextSize")]
    pub context_size: Option<WebSearchContextSize>,
    #[serde(alias = "allowedDomains")]
    pub allowed_domains: Option<Vec<String>>,
    #[serde(alias = "blockedDomains")]
    pub blocked_domains: Option<Vec<String>>,
    #[serde(alias = "maxResultChars")]
    pub max_result_chars: Option<usize>,
}

fn validate_optional_http_base_url(base_url: Option<&str>, role: &str) -> Result<()> {
    let Some(base_url) = base_url.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let parsed = reqwest::Url::parse(base_url)
        .with_context(|| format!("web_search base_url for role {role:?} is invalid"))?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        _ => bail!("web_search base_url for role {role:?} must use http or https"),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchQuery {
    pub q: String,
    pub recency: Option<u32>,
    pub domains: Vec<String>,
    #[serde(alias = "numResults")]
    pub num_results: Option<usize>,
    #[serde(alias = "type")]
    pub search_type: Option<String>,
    pub livecrawl: Option<String>,
    #[serde(alias = "contextMaxCharacters")]
    pub context_max_characters: Option<usize>,
}

impl SearchQuery {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            q: query.into(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchResult {
    pub ref_id: String,
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub published_at: Option<String>,
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WebSearchOptions {
    pub context_size: WebSearchContextSize,
    pub allowed_domains: Vec<String>,
    pub blocked_domains: Vec<String>,
    pub max_result_chars: usize,
}

impl Default for WebSearchOptions {
    fn default() -> Self {
        Self {
            context_size: WebSearchContextSize::Medium,
            allowed_domains: Vec::new(),
            blocked_domains: Vec::new(),
            max_result_chars: DEFAULT_MAX_RESULT_CHARS,
        }
    }
}

impl From<&WebSearchConfig> for WebSearchOptions {
    fn from(config: &WebSearchConfig) -> Self {
        Self {
            context_size: config.context_size,
            allowed_domains: config.allowed_domains.clone(),
            blocked_domains: config.blocked_domains.clone(),
            max_result_chars: config.max_result_chars,
        }
    }
}

pub trait WebSearchProvider: Send + Sync {
    fn search<'a>(
        &'a self,
        queries: Vec<SearchQuery>,
        options: WebSearchOptions,
    ) -> WebSearchFuture<'a, Vec<SearchResult>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockWebPage {
    pub title: String,
    pub url: String,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct MockWebSearchProvider {
    pages: Vec<MockWebPage>,
}

impl MockWebSearchProvider {
    pub fn new(pages: Vec<MockWebPage>) -> Self {
        Self { pages }
    }
}

impl Default for MockWebSearchProvider {
    fn default() -> Self {
        Self {
            pages: vec![
                MockWebPage {
                    title: "Mock TQQQ Market Signals".to_string(),
                    url: "https://mock.local/market/tqqq-signals".to_string(),
                    content: "TQQQ mock market signal summary with liquidity, volatility, and trend context.".to_string(),
                },
                MockWebPage {
                    title: "Mock Macro Calendar".to_string(),
                    url: "https://mock.local/macro/calendar".to_string(),
                    content: "Mock macro calendar covering CPI, FOMC, payrolls, and earnings catalysts.".to_string(),
                },
                MockWebPage {
                    title: "Mock Risk Dashboard".to_string(),
                    url: "https://mock.local/risk/dashboard".to_string(),
                    content: "Mock risk dashboard with drawdown, breadth, rates, and credit stress notes.".to_string(),
                },
            ],
        }
    }
}

impl WebSearchProvider for MockWebSearchProvider {
    fn search<'a>(
        &'a self,
        queries: Vec<SearchQuery>,
        options: WebSearchOptions,
    ) -> WebSearchFuture<'a, Vec<SearchResult>> {
        Box::pin(async move {
            let mut results = Vec::new();
            for query in queries {
                let query_text = query.q.trim();
                if query_text.is_empty() {
                    bail!("mock web search requires a non-empty query");
                }

                let query_terms = lowercase_terms(query_text);
                let mut query_results = self
                    .pages
                    .iter()
                    .filter(|page| {
                        domain_allowed(
                            &page.url,
                            &merged_allowed_domains(&options.allowed_domains, &query.domains),
                            &options.blocked_domains,
                        )
                    })
                    .filter_map(|page| {
                        let haystack = format!("{} {}", page.title, page.content).to_lowercase();
                        let score = match_score(&query_terms, &haystack);
                        (score > 0.0).then(|| {
                            (
                                score,
                                SearchResult {
                                    ref_id: String::new(),
                                    title: page.title.clone(),
                                    url: page.url.clone(),
                                    snippet: first_chars(&page.content, options.max_result_chars),
                                    published_at: None,
                                    source: Some("mock".to_string()),
                                },
                            )
                        })
                    })
                    .collect::<Vec<_>>();

                query_results.sort_by(|left, right| {
                    right
                        .0
                        .partial_cmp(&left.0)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| left.1.url.cmp(&right.1.url))
                });

                if query_results.is_empty()
                    && domain_allowed(
                        "https://mock.local/search/result",
                        &merged_allowed_domains(&options.allowed_domains, &query.domains),
                        &options.blocked_domains,
                    )
                {
                    query_results.push((
                        0.0,
                        SearchResult {
                            ref_id: String::new(),
                            title: format!("Mock result for {query_text}"),
                            url: "https://mock.local/search/result".to_string(),
                            snippet: first_chars(
                                &format!("Mock search result for {query_text}."),
                                options.max_result_chars,
                            ),
                            published_at: None,
                            source: Some("mock".to_string()),
                        },
                    ));
                }

                results.extend(
                    query_results
                        .into_iter()
                        .take(DEFAULT_MAX_RESULTS)
                        .map(|(_, result)| result),
                );
            }

            assign_ref_ids(&mut results);
            Ok(results)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ExaWebSearchProvider {
    client: reqwest::Client,
    endpoint: reqwest::Url,
}

impl ExaWebSearchProvider {
    pub fn from_config(config: &WebSearchConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: exa_base_url(config.base_url.as_deref()),
        }
    }

    fn endpoint_url(&self) -> reqwest::Url {
        build_exa_url_from_base(&self.endpoint)
    }
}

impl WebSearchProvider for ExaWebSearchProvider {
    fn search<'a>(
        &'a self,
        queries: Vec<SearchQuery>,
        options: WebSearchOptions,
    ) -> WebSearchFuture<'a, Vec<SearchResult>> {
        Box::pin(async move {
            let mut results = Vec::new();
            for query in queries {
                let query_text = query.q.trim();
                if query_text.is_empty() {
                    bail!("exa web search requires a non-empty query");
                }

                let request = build_exa_mcp_request(ExaMcpSearchArgs::from_query(&query, &options));
                let response = self
                    .client
                    .post(self.endpoint_url())
                    .header("Accept", "application/json, text/event-stream")
                    .header("Content-Type", "application/json")
                    .json(&request)
                    .timeout(Duration::from_secs(EXA_MCP_TIMEOUT_SECS))
                    .send()
                    .await
                    .context("exa web search provider request failed")?;
                if !response.status().is_success() {
                    bail!(
                        "exa web search provider returned HTTP {}",
                        response.status()
                    );
                }
                let body = read_limited_response_body(response, EXA_MCP_MAX_BODY_BYTES).await?;
                let text = std::str::from_utf8(&body)
                    .context("exa web search provider returned non-UTF-8 response")?;
                let snippet = parse_exa_mcp_response(text)?;
                let url =
                    first_http_url(&snippet).unwrap_or_else(|| DEFAULT_EXA_MCP_URL.to_string());
                results.push(SearchResult {
                    ref_id: String::new(),
                    title: format!("Exa search: {query_text}"),
                    url,
                    snippet: first_chars(&snippet, options.max_result_chars),
                    published_at: None,
                    source: Some("exa".to_string()),
                });
            }
            assign_ref_ids(&mut results);
            Ok(results)
        })
    }
}

async fn read_limited_response_body(
    mut response: reqwest::Response,
    max_bytes: u64,
) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("exa web search provider response read failed")?
    {
        if body.len() as u64 + chunk.len() as u64 > max_bytes {
            bail!("exa web search provider response exceeded size limit");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExaMcpSearchArgs {
    pub query: String,
    pub search_type: String,
    pub num_results: usize,
    pub livecrawl: String,
    pub context_max_characters: usize,
}

impl ExaMcpSearchArgs {
    fn from_query(query: &SearchQuery, options: &WebSearchOptions) -> Self {
        Self {
            query: query.q.trim().to_string(),
            search_type: exa_search_type(query.search_type.as_deref(), options.context_size)
                .to_string(),
            num_results: query
                .num_results
                .unwrap_or(EXA_DEFAULT_NUM_RESULTS)
                .clamp(1, EXA_MAX_NUM_RESULTS),
            livecrawl: exa_livecrawl(query.livecrawl.as_deref()).to_string(),
            context_max_characters: query
                .context_max_characters
                .unwrap_or(EXA_DEFAULT_CONTEXT_MAX_CHARS)
                .clamp(1, EXA_MAX_CONTEXT_MAX_CHARS),
        }
    }
}

pub fn build_exa_url() -> reqwest::Url {
    let base =
        reqwest::Url::parse(DEFAULT_EXA_MCP_URL).expect("default Exa MCP URL should be valid");
    build_exa_url_from_base(&base)
}

fn build_exa_url_from_base(base: &reqwest::Url) -> reqwest::Url {
    base.clone()
}

fn exa_base_url(configured: Option<&str>) -> reqwest::Url {
    let value = configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_EXA_MCP_URL);
    reqwest::Url::parse(value).unwrap_or_else(|_| {
        reqwest::Url::parse(DEFAULT_EXA_MCP_URL).expect("default Exa MCP URL should be valid")
    })
}

fn exa_search_type(configured: Option<&str>, context_size: WebSearchContextSize) -> &'static str {
    match configured.map(str::trim) {
        Some("auto") => return "auto",
        Some("fast") => return "fast",
        Some("deep") => return "deep",
        _ => {}
    }
    match context_size {
        WebSearchContextSize::Low => "fast",
        WebSearchContextSize::Medium => "auto",
        WebSearchContextSize::High => "deep",
    }
}

fn exa_livecrawl(configured: Option<&str>) -> &'static str {
    match configured.map(str::trim) {
        Some("preferred") => "preferred",
        _ => "fallback",
    }
}

pub fn build_exa_mcp_request(args: ExaMcpSearchArgs) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "web_search_exa",
            "arguments": {
                "query": args.query,
                "type": args.search_type,
                "numResults": args.num_results,
                "livecrawl": args.livecrawl,
                "contextMaxCharacters": args.context_max_characters,
            }
        }
    })
}

pub fn parse_exa_mcp_response(body: &str) -> Result<String> {
    let trimmed = body.trim();
    if !trimmed.is_empty() {
        if let Ok(payload) = serde_json::from_str::<Value>(trimmed) {
            if let Some(text) = exa_text_from_payload(&payload) {
                return Ok(text);
            }
        }
    }

    for line in body.lines() {
        let Some(payload) = line.trim_start().strip_prefix("data:") else {
            continue;
        };
        let payload = payload.trim();
        if payload.is_empty() || payload == "[DONE]" {
            continue;
        }
        let value: Value = serde_json::from_str(payload)
            .with_context(|| "exa web search provider returned invalid SSE JSON")?;
        if let Some(text) = exa_text_from_payload(&value) {
            return Ok(text);
        }
    }

    Ok(EXA_NO_RESULTS_MESSAGE.to_string())
}

fn exa_text_from_payload(payload: &Value) -> Option<String> {
    payload
        .get("result")?
        .get("content")?
        .as_array()?
        .iter()
        .find_map(|item| {
            let item_type = item.get("type").and_then(Value::as_str);
            let text = item.get("text").and_then(Value::as_str)?.trim();
            (item_type == Some("text") && !text.is_empty()).then(|| text.to_string())
        })
}

fn first_http_url(text: &str) -> Option<String> {
    text.split_whitespace()
        .filter_map(|word| {
            let start = word.find("http://").or_else(|| word.find("https://"))?;
            let url = word[start..].trim_matches(|character: char| {
                matches!(
                    character,
                    '"' | '\'' | ')' | ']' | '}' | ',' | '.' | ';' | ':' | '<' | '>'
                )
            });
            (!url.is_empty()).then(|| url.to_string())
        })
        .next()
}

fn merged_allowed_domains(global: &[String], query: &[String]) -> Vec<String> {
    if query.is_empty() {
        global.to_vec()
    } else if global.is_empty() {
        query.to_vec()
    } else {
        query
            .iter()
            .filter(|domain| {
                normalize_domain(domain).is_some_and(|candidate| {
                    global
                        .iter()
                        .any(|allowed| host_matches_domain(&candidate, allowed))
                })
            })
            .cloned()
            .collect()
    }
}

fn assign_ref_ids(results: &mut [SearchResult]) {
    for (index, result) in results.iter_mut().enumerate() {
        result.ref_id = format!("search{index}");
    }
}

fn lowercase_terms(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .map(|term| {
            term.trim_matches(|character: char| !character.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|term| !term.is_empty())
        .collect()
}

fn match_score(query_terms: &[String], haystack: &str) -> f32 {
    if query_terms.is_empty() {
        return 0.0;
    }

    let matched_terms = query_terms
        .iter()
        .filter(|term| haystack.contains(term.as_str()))
        .count();
    matched_terms as f32 / query_terms.len() as f32
}

fn domain_allowed(url: &str, allowed_domains: &[String], blocked_domains: &[String]) -> bool {
    let Some(host) = host_from_url(url) else {
        return false;
    };

    if blocked_domains
        .iter()
        .any(|domain| host_matches_domain(&host, domain))
    {
        return false;
    }

    allowed_domains.is_empty()
        || allowed_domains
            .iter()
            .any(|domain| host_matches_domain(&host, domain))
}

fn host_from_url(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let host = after_scheme.split('/').next()?.trim().to_lowercase();
    (!host.is_empty()).then_some(host)
}

fn host_matches_domain(host: &str, domain: &str) -> bool {
    let Some(domain) = normalize_domain(domain) else {
        return false;
    };
    !domain.is_empty() && (host == domain || host.ends_with(&format!(".{domain}")))
}

fn normalize_domain(value: &str) -> Option<String> {
    let value = value
        .trim()
        .trim_start_matches("*.")
        .trim_start_matches('.')
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
    (!value.is_empty()).then(|| value.to_string())
}

fn first_chars(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_to_disabled_mock_medium_context() {
        let config = WebSearchConfig::default();

        assert_eq!(config.mode, WebSearchMode::Disabled);
        assert_eq!(config.provider, WebSearchProviderKind::Mock);
        assert_eq!(config.base_url, None);
        assert_eq!(config.api_key, None);
        assert_eq!(config.context_size, WebSearchContextSize::Medium);
        assert!(config.allowed_domains.is_empty());
        assert!(config.blocked_domains.is_empty());
        assert_eq!(config.max_result_chars, 12_000);
    }

    #[test]
    fn deserializes_partial_config_with_defaults() {
        let config: WebSearchConfig = serde_json::from_str(r#"{"mode":"cached"}"#).unwrap();

        assert_eq!(config.mode, WebSearchMode::Cached);
        assert_eq!(config.provider, WebSearchProviderKind::Mock);
        assert_eq!(config.context_size, WebSearchContextSize::Medium);
        assert_eq!(config.max_result_chars, 12_000);
    }

    #[test]
    fn role_override_replaces_only_present_fields() {
        let global = WebSearchConfig {
            mode: WebSearchMode::Cached,
            provider: WebSearchProviderKind::Mock,
            base_url: Some("https://gateway.example.com/exa".to_string()),
            api_key: Some("global-key".to_string()),
            context_size: WebSearchContextSize::Low,
            allowed_domains: vec!["example.com".to_string()],
            blocked_domains: vec!["blocked.example".to_string()],
            max_result_chars: 4_000,
        };
        let role = WebSearchConfigOverride {
            mode: Some(WebSearchMode::Live),
            provider: Some(WebSearchProviderKind::Exa),
            api_key: Some("role-key".to_string()),
            allowed_domains: Some(vec!["role.example".to_string()]),
            max_result_chars: Some(8_000),
            ..WebSearchConfigOverride::default()
        };

        let merged = global.merge_override(Some(&role));

        assert_eq!(merged.mode, WebSearchMode::Live);
        assert_eq!(merged.provider, WebSearchProviderKind::Exa);
        assert_eq!(
            merged.base_url.as_deref(),
            Some("https://gateway.example.com/exa")
        );
        assert_eq!(merged.api_key.as_deref(), Some("role-key"));
        assert_eq!(merged.context_size, WebSearchContextSize::Low);
        assert_eq!(merged.allowed_domains, vec!["role.example"]);
        assert_eq!(merged.blocked_domains, vec!["blocked.example"]);
        assert_eq!(merged.max_result_chars, 8_000);
    }

    #[test]
    fn exa_provider_uses_default_mcp_url_without_api_key() {
        let url = build_exa_url();

        assert_eq!(url.as_str(), "https://mcp.exa.ai/mcp");
    }

    #[test]
    fn exa_mcp_request_uses_tools_call_shape() {
        let request = build_exa_mcp_request(ExaMcpSearchArgs {
            query: "TQQQ liquidity".to_string(),
            search_type: "auto".to_string(),
            num_results: 8,
            livecrawl: "fallback".to_string(),
            context_max_characters: 10_000,
        });

        assert_eq!(request["jsonrpc"], "2.0");
        assert_eq!(request["method"], "tools/call");
        assert_eq!(request["params"]["name"], "web_search_exa");
        assert_eq!(request["params"]["arguments"]["query"], "TQQQ liquidity");
        assert_eq!(request["params"]["arguments"]["type"], "auto");
        assert_eq!(request["params"]["arguments"]["numResults"], 8);
        assert_eq!(request["params"]["arguments"]["livecrawl"], "fallback");
        assert_eq!(
            request["params"]["arguments"]["contextMaxCharacters"],
            10_000
        );
    }

    #[test]
    fn parse_exa_mcp_response_reads_plain_json() {
        let body = r#"{"result":{"content":[{"type":"text","text":"Plain result"}]}}"#;

        assert_eq!(parse_exa_mcp_response(body).unwrap(), "Plain result");
    }

    #[test]
    fn parse_exa_mcp_response_reads_sse_data_line() {
        let body =
            "event: message\ndata: {\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"SSE result\"}]}}\n";

        assert_eq!(parse_exa_mcp_response(body).unwrap(), "SSE result");
    }

    #[test]
    fn parse_exa_mcp_response_returns_empty_result_message() {
        let body = r#"{"result":{"content":[]}}"#;

        assert_eq!(
            parse_exa_mcp_response(body).unwrap(),
            EXA_NO_RESULTS_MESSAGE
        );
    }

    #[test]
    fn parse_exa_mcp_response_rejects_invalid_sse_json() {
        let err = parse_exa_mcp_response("data: {not json").unwrap_err();

        assert!(format!("{err:#}").contains("invalid SSE JSON"));
    }

    #[tokio::test]
    async fn mock_provider_search_is_deterministic_and_applies_limits() {
        let provider = MockWebSearchProvider::new(vec![MockWebPage {
            title: "Alpha liquidity".to_string(),
            url: "https://example.com/a".to_string(),
            content: "Liquidity and volatility context".to_string(),
        }]);
        let options = WebSearchOptions {
            max_result_chars: 9,
            ..WebSearchOptions::default()
        };

        let results = provider
            .search(vec![SearchQuery::new("liquidity")], options)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Alpha liquidity");
        assert_eq!(results[0].url, "https://example.com/a");
        assert_eq!(results[0].snippet, "Liquidity");
    }

    #[tokio::test]
    async fn mock_provider_search_applies_domain_filters() {
        let provider = MockWebSearchProvider::new(vec![
            MockWebPage {
                title: "Allowed result".to_string(),
                url: "https://research.example.com/a".to_string(),
                content: "TQQQ signal".to_string(),
            },
            MockWebPage {
                title: "Blocked result".to_string(),
                url: "https://blocked.example.com/a".to_string(),
                content: "TQQQ signal".to_string(),
            },
        ]);
        let options = WebSearchOptions {
            allowed_domains: vec!["example.com".to_string()],
            blocked_domains: vec!["blocked.example.com".to_string()],
            ..WebSearchOptions::default()
        };

        let results = provider
            .search(vec![SearchQuery::new("TQQQ")], options)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Allowed result");
    }
}
