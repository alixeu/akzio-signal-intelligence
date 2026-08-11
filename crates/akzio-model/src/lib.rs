//! Stateless Responses API adapter.
//!
//! Akzio owns every durable turn, context manifest, and tool result. The
//! provider only receives the current, replayable turn and may request one of
//! the Rust-approved tools declared by an agent contract.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("model base URL is empty")]
    EmptyBaseUrl,
    #[error("model API key is empty")]
    EmptyApiKey,
    #[error("model name is empty")]
    EmptyModel,
    #[error("model reasoning effort is empty")]
    EmptyReasoningEffort,
    #[error("model response transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("model returned HTTP {status}: {body}")]
    Http { status: StatusCode, body: String },
    #[error("model response has neither output text nor a tool call")]
    MissingOutput,
    #[error("fixture response sequence is exhausted")]
    FixtureExhausted,
    #[error("native web tool is not configured")]
    NativeWebUnavailable,
    #[error("native web tool call is not allowed")]
    NativeWebToolNotAllowed,
    #[error("native web tool arguments are invalid")]
    NativeWebArgumentsInvalid,
    #[error("native web result has no verifiable citations")]
    NativeWebCitationsMissing,
    #[error("native web citation URI is not allowlisted")]
    NativeWebUnsafeCitation,
    #[error("native web result exceeds the configured limit")]
    NativeWebLimitExceeded,
}

pub type Result<T> = std::result::Result<T, ModelError>;

/// Test-fixture placeholder resolved from the current model request's governed context.
pub const FIXTURE_CONTEXT_EVIDENCE_ID: &str = "$fixture.context.first_evidence_id";
/// Test-fixture placeholder resolved from the current model request's governed context.
pub const FIXTURE_CONTEXT_CLAIM_ID: &str = "$fixture.context.first_claim_id";

fn default_reasoning_effort() -> String {
    "medium".to_owned()
}

/// Production model settings loaded from the local Akzio TOML configuration.
///
/// The API key is intentionally redacted from `Debug` output and never copied
/// into a durable AgentTurn trace.
#[derive(Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    #[serde(default = "default_reasoning_effort")]
    pub reasoning_effort: String,
    #[serde(default)]
    pub debug: bool,
}

impl std::fmt::Debug for ModelConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelConfig")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_key", &"<redacted>")
            .field("reasoning_effort", &self.reasoning_effort)
            .field("debug", &self.debug)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

pub const NATIVE_WEB_SEARCH_TOOL: &str = "web_search_preview";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeWebPolicy {
    pub tool_name: String,
    pub allowed_hosts: Vec<String>,
    pub max_query_chars: usize,
    pub max_results: usize,
    pub max_citations: usize,
}

impl Default for NativeWebPolicy {
    fn default() -> Self {
        Self {
            tool_name: NATIVE_WEB_SEARCH_TOOL.to_owned(),
            allowed_hosts: vec![
                "sec.gov".to_owned(),
                "fred.stlouisfed.org".to_owned(),
                "reuters.com".to_owned(),
                "apnews.com".to_owned(),
            ],
            max_query_chars: 2_000,
            max_results: 8,
            max_citations: 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeWebQuery {
    pub query: String,
    pub domains: Vec<String>,
    pub max_results: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeWebCitation {
    pub uri: String,
    pub title: Option<String>,
    pub excerpt: Option<String>,
    #[serde(default)]
    pub published_at: Option<String>,
    #[serde(default)]
    pub revision: Option<String>,
    #[serde(default)]
    pub document_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub instructions: String,
    pub input: String,
    pub schema_name: Option<String>,
    pub schema: Option<Value>,
    pub max_output_tokens: u32,
    pub tools: Vec<ModelToolDefinition>,
}

/// Provider-facing request/result pair retained only inside a RunScoped
/// AgentTurn when local model debugging is enabled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelCallTrace {
    pub request: Value,
    pub result: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelResponse {
    pub output_text: String,
    pub tool_calls: Vec<ModelToolCall>,
    pub raw: Value,
    /// Provider payload without authorization headers or credentials.
    pub request_body: Value,
}

impl NativeWebPolicy {
    pub fn tool_definition(&self) -> ModelToolDefinition {
        ModelToolDefinition {
            name: self.tool_name.clone(),
            description: "Rust-governed native web search; citations are mandatory".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string", "maxLength": self.max_query_chars},
                    "domains": {"type": "array", "items": {"type": "string"}},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": self.max_results}
                },
                "required": ["query"],
                "additionalProperties": false
            }),
            strict: true,
        }
    }

    pub fn validate_tool_calls(&self, calls: &[ModelToolCall]) -> Result<Vec<NativeWebQuery>> {
        let mut queries = Vec::with_capacity(calls.len());
        for call in calls {
            if call.name != self.tool_name {
                return Err(ModelError::NativeWebToolNotAllowed);
            }
            let object = call
                .arguments
                .as_object()
                .ok_or(ModelError::NativeWebArgumentsInvalid)?;
            let query = object
                .get("query")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or(ModelError::NativeWebArgumentsInvalid)?
                .trim()
                .to_owned();
            if query.chars().count() > self.max_query_chars {
                return Err(ModelError::NativeWebLimitExceeded);
            }
            let domains = match object.get("domains") {
                None => Vec::new(),
                Some(value) => value
                    .as_array()
                    .ok_or(ModelError::NativeWebArgumentsInvalid)?
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_owned)
                            .ok_or(ModelError::NativeWebArgumentsInvalid)
                    })
                    .collect::<Result<Vec<_>>>()?,
            };
            if domains
                .iter()
                .any(|domain| !self.allowed_hosts.iter().any(|allowed| domain == allowed))
            {
                return Err(ModelError::NativeWebToolNotAllowed);
            }
            let max_results = object
                .get("max_results")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(self.max_results);
            if max_results == 0 || max_results > self.max_results {
                return Err(ModelError::NativeWebLimitExceeded);
            }
            queries.push(NativeWebQuery {
                query,
                domains,
                max_results,
            });
        }
        Ok(queries)
    }

    pub fn extract_citations(&self, raw: &Value) -> Result<Vec<NativeWebCitation>> {
        let mut citations = Vec::new();
        collect_citations(raw, &mut citations, self.max_citations);
        citations.sort_by(|left, right| left.uri.cmp(&right.uri));
        citations.dedup_by(|left, right| left.uri == right.uri);
        if citations.is_empty() {
            return Err(ModelError::NativeWebCitationsMissing);
        }
        if citations.len() > self.max_citations {
            return Err(ModelError::NativeWebLimitExceeded);
        }
        for citation in &citations {
            let parsed = reqwest::Url::parse(&citation.uri)
                .map_err(|_| ModelError::NativeWebUnsafeCitation)?;
            if parsed.username() != ""
                || parsed.password().is_some()
                || parsed.query().is_some()
                || !self
                    .allowed_hosts
                    .iter()
                    .any(|host| parsed.host_str() == Some(host.as_str()))
            {
                return Err(ModelError::NativeWebUnsafeCitation);
            }
        }
        Ok(citations)
    }
}

fn collect_citations(value: &Value, output: &mut Vec<NativeWebCitation>, limit: usize) {
    if output.len() >= limit {
        return;
    }
    match value {
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_citations(value, output, limit)),
        Value::Object(object) => {
            let uri = object
                .get("url")
                .or_else(|| object.get("uri"))
                .and_then(Value::as_str);
            if let Some(uri) = uri.filter(|value| !value.trim().is_empty()) {
                output.push(NativeWebCitation {
                    uri: uri.to_owned(),
                    title: object
                        .get("title")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    excerpt: object
                        .get("quote")
                        .or_else(|| object.get("text"))
                        .or_else(|| object.get("excerpt"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    published_at: object
                        .get("published_at")
                        .or_else(|| object.get("publishedAt"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    revision: object
                        .get("revision")
                        .or_else(|| object.get("version"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    document_id: object
                        .get("document_id")
                        .or_else(|| object.get("documentId"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            }
            object
                .values()
                .for_each(|value| collect_citations(value, output, limit));
        }
        _ => {}
    }
}

#[derive(Clone)]
pub struct ResponsesClient {
    http: Client,
    base_url: String,
    api_key: String,
    model: String,
    reasoning_effort: String,
    timeout: std::time::Duration,
}

impl std::fmt::Debug for ResponsesClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResponsesClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .field("model", &self.model)
            .field("reasoning_effort", &self.reasoning_effort)
            .finish_non_exhaustive()
    }
}

impl ResponsesClient {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
        reasoning_effort: impl Into<String>,
    ) -> Result<Self> {
        let base_url = base_url.into().trim().trim_end_matches('/').to_owned();
        if base_url.is_empty() {
            return Err(ModelError::EmptyBaseUrl);
        }
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(ModelError::EmptyApiKey);
        }
        let model = model.into();
        if model.trim().is_empty() {
            return Err(ModelError::EmptyModel);
        }
        let reasoning_effort = reasoning_effort.into();
        if reasoning_effort.trim().is_empty() {
            return Err(ModelError::EmptyReasoningEffort);
        }
        Ok(Self {
            http: Client::new(),
            base_url,
            api_key,
            model,
            reasoning_effort,
            timeout: std::time::Duration::from_secs(30),
        })
    }

    pub fn request_body(&self, request: &ModelRequest) -> Value {
        responses_request_body(&self.model, &self.reasoning_effort, request)
    }

    pub async fn respond(&self, request: ModelRequest) -> Result<ModelResponse> {
        let body = self.request_body(&request);
        let response = self
            .http
            .post(format!("{}/responses", self.base_url))
            .bearer_auth(&self.api_key)
            .timeout(self.timeout)
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let raw = response.json::<Value>().await?;
        if !status.is_success() {
            return Err(ModelError::Http {
                status,
                body: raw.to_string(),
            });
        }
        response_from_raw(raw, body)
    }
}

fn responses_request_body(model: &str, reasoning_effort: &str, request: &ModelRequest) -> Value {
    let mut body = json!({
        "model": model,
        "instructions": request.instructions,
        "input": request.input,
        "max_output_tokens": request.max_output_tokens,
        "reasoning": {"effort": reasoning_effort},
        "store": false,
    });
    if let (Some(name), Some(schema)) = (&request.schema_name, &request.schema) {
        body["text"] = json!({
            "format": {
                "type": "json_schema",
                "name": name,
                "schema": schema,
                "strict": true,
            }
        });
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    if tool.name == NATIVE_WEB_SEARCH_TOOL {
                        json!({"type": NATIVE_WEB_SEARCH_TOOL})
                    } else {
                        json!({
                            "type": "function",
                            "name": tool.name,
                            "description": tool.description,
                            "parameters": tool.input_schema,
                            "strict": tool.strict,
                        })
                    }
                })
                .collect(),
        );
        body["tool_choice"] = json!("auto");
    }
    body
}

#[derive(Debug, Clone)]
pub enum ModelClient {
    Responses(ResponsesClient),
    Fixture(Value),
    FixtureBySchema(BTreeMap<String, Value>),
    FixtureSequence(Arc<Mutex<VecDeque<Value>>>),
}

impl ModelClient {
    pub fn fixture_sequence(values: impl IntoIterator<Item = Value>) -> Self {
        Self::FixtureSequence(Arc::new(Mutex::new(values.into_iter().collect())))
    }

    pub fn from_config(config: &ModelConfig) -> Result<Self> {
        Ok(Self::Responses(ResponsesClient::new(
            &config.base_url,
            &config.api_key,
            &config.model,
            &config.reasoning_effort,
        )?))
    }

    /// Exact provider payload used for an individual turn, excluding auth.
    pub fn request_body(&self, request: &ModelRequest) -> Value {
        match self {
            Self::Responses(client) => client.request_body(request),
            Self::Fixture(_) | Self::FixtureBySchema(_) | Self::FixtureSequence(_) => {
                responses_request_body("fixture", "none", request)
            }
        }
    }

    pub async fn respond(&self, request: ModelRequest) -> Result<ModelResponse> {
        match self {
            Self::Responses(client) => client.respond(request).await,
            Self::Fixture(raw) => response_from_raw(
                materialize_fixture(raw.clone(), &request),
                self.request_body(&request),
            ),
            Self::FixtureBySchema(outputs) => {
                let schema = request
                    .schema_name
                    .as_deref()
                    .ok_or(ModelError::MissingOutput)?;
                let raw = outputs.get(schema).ok_or(ModelError::MissingOutput)?;
                response_from_raw(
                    materialize_fixture(raw.clone(), &request),
                    self.request_body(&request),
                )
            }
            Self::FixtureSequence(values) => {
                let raw = values
                    .lock()
                    .expect("fixture response sequence poisoned")
                    .pop_front()
                    .ok_or(ModelError::FixtureExhausted)?;
                response_from_raw(
                    materialize_fixture(raw, &request),
                    self.request_body(&request),
                )
            }
        }
    }
}

fn materialize_fixture(mut raw: Value, request: &ModelRequest) -> Value {
    let evidence_id = fixture_context_artifact_id(&request.input, "normalized_evidence")
        .or_else(|| fixture_context_artifact_id(&request.input, "semantic_detail"));
    let claim_id = fixture_context_artifact_id(&request.input, "claim");
    if evidence_id.is_none() && claim_id.is_none() {
        return raw;
    }
    if let Some(Value::String(output_text)) = raw.get_mut("output_text") {
        if let Ok(mut output) = serde_json::from_str(output_text) {
            materialize_fixture_value(&mut output, evidence_id.as_deref(), claim_id.as_deref());
            if let Ok(text) = serde_json::to_string(&output) {
                *output_text = text;
            }
        }
    }
    materialize_fixture_value(&mut raw, evidence_id.as_deref(), claim_id.as_deref());
    raw
}

fn fixture_context_artifact_id(input: &str, kind: &str) -> Option<String> {
    serde_json::from_str::<Value>(input)
        .ok()?
        .get("context")?
        .as_array()?
        .iter()
        .find(|artifact| artifact.get("kind").and_then(Value::as_str) == Some(kind))?
        .get("artifact_id")?
        .as_str()
        .map(ToOwned::to_owned)
}

fn materialize_fixture_value(value: &mut Value, evidence_id: Option<&str>, claim_id: Option<&str>) {
    match value {
        Value::String(text) if text == FIXTURE_CONTEXT_EVIDENCE_ID => {
            if let Some(evidence_id) = evidence_id {
                *text = evidence_id.to_owned();
            }
        }
        Value::String(text) if text == FIXTURE_CONTEXT_CLAIM_ID => {
            if let Some(claim_id) = claim_id {
                *text = claim_id.to_owned();
            }
        }
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| materialize_fixture_value(value, evidence_id, claim_id)),
        Value::Object(values) => values
            .values_mut()
            .for_each(|value| materialize_fixture_value(value, evidence_id, claim_id)),
        _ => {}
    }
}

fn response_from_raw(raw: Value, request_body: Value) -> Result<ModelResponse> {
    let output_text = extract_output_text(&raw).unwrap_or_default();
    let tool_calls = extract_tool_calls(&raw);
    if output_text.is_empty() && tool_calls.is_empty() {
        return Err(ModelError::MissingOutput);
    }
    Ok(ModelResponse {
        output_text,
        tool_calls,
        raw,
        request_body,
    })
}

pub fn extract_output_text(response: &Value) -> Option<String> {
    response
        .get("output_text")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            response
                .get("output")
                .and_then(Value::as_array)
                .and_then(|items| {
                    items.iter().find_map(|item| {
                        item.get("content")
                            .and_then(Value::as_array)
                            .and_then(|content| {
                                content.iter().find_map(|part| {
                                    part.get("text")
                                        .or_else(|| part.get("output_text"))
                                        .and_then(Value::as_str)
                                        .map(ToOwned::to_owned)
                                })
                            })
                    })
                })
        })
}

pub fn extract_tool_calls(response: &Value) -> Vec<ModelToolCall> {
    let direct = response
        .get("tool_calls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten();
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .into_iter()
        .flatten();
    direct.chain(output).filter_map(parse_tool_call).collect()
}

fn parse_tool_call(value: &Value) -> Option<ModelToolCall> {
    let kind = value.get("type").and_then(Value::as_str);
    if kind.is_some_and(|kind| kind != "function_call" && kind != "tool_call") {
        return None;
    }
    let name = value.get("name")?.as_str()?.to_owned();
    let call_id = value
        .get("call_id")
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .unwrap_or(&name)
        .to_owned();
    let arguments = value
        .get("arguments")
        .or_else(|| value.get("input"))
        .map(|arguments| match arguments {
            Value::String(text) => {
                serde_json::from_str(text).unwrap_or_else(|_| json!({"raw": text}))
            }
            value => value.clone(),
        })
        .unwrap_or_else(|| json!({}));
    Some(ModelToolCall {
        call_id,
        name,
        arguments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ModelRequest {
        ModelRequest {
            instructions: "test".to_owned(),
            input: "{}".to_owned(),
            schema_name: Some("test".to_owned()),
            schema: Some(json!({"type": "object"})),
            max_output_tokens: 1,
            tools: vec![],
        }
    }

    #[test]
    fn response_request_body_marks_function_tools_strict() {
        let mut request = request();
        request.tools = vec![ModelToolDefinition {
            name: "read_artifact".to_owned(),
            description: "fixture".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {"artifact_id": {"type": "string"}},
                "required": ["artifact_id"],
                "additionalProperties": false,
            }),
            strict: true,
        }];

        let body = responses_request_body("fixture", "high", &request);
        assert_eq!(body["text"]["format"]["strict"], true);
        assert_eq!(body["tools"][0]["strict"], true);
        assert_eq!(body["reasoning"]["effort"], "high");
    }

    #[test]
    fn configured_client_redacts_its_api_key_from_debug_output() {
        let client = ResponsesClient::new("http://fixture", "secret", "fixture", "medium").unwrap();

        let rendered = format!("{client:?}");
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("secret"));
    }

    #[test]
    fn model_config_drives_reasoning_and_rejects_empty_credentials() {
        let config = ModelConfig {
            base_url: "http://fixture/v1".to_owned(),
            model: "fixture-model".to_owned(),
            api_key: "fixture-key".to_owned(),
            reasoning_effort: "low".to_owned(),
            debug: true,
        };
        let client = ModelClient::from_config(&config).unwrap();
        assert_eq!(
            client.request_body(&request())["reasoning"]["effort"],
            "low"
        );

        let mut missing_key = config;
        missing_key.api_key.clear();
        assert!(matches!(
            ModelClient::from_config(&missing_key),
            Err(ModelError::EmptyApiKey)
        ));
    }

    #[test]
    fn extracts_direct_responses_output_text() {
        assert_eq!(
            extract_output_text(&json!({"output_text": "hello"})),
            Some("hello".to_owned())
        );
    }

    #[test]
    fn extracts_nested_response_content() {
        assert_eq!(
            extract_output_text(&json!({
                "output": [{"content": [{"type": "output_text", "text": "hello"}]}]
            })),
            Some("hello".to_owned())
        );
    }

    #[test]
    fn extracts_responses_function_calls() {
        let calls = extract_tool_calls(&json!({
            "output": [{
                "type": "function_call",
                "call_id": "call-1",
                "name": "read_evidence",
                "arguments": "{\"document_id\":\"doc-1\"}"
            }]
        }));
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "read_evidence");
        assert_eq!(calls[0].arguments["document_id"], "doc-1");
    }

    #[tokio::test]
    async fn fixture_client_is_deterministic_without_network() {
        let client = ModelClient::Fixture(json!({"output_text": "{}"}));
        assert_eq!(client.respond(request()).await.unwrap().output_text, "{}");
    }

    #[tokio::test]
    async fn fixture_sequence_preserves_tool_turn_order() {
        let client = ModelClient::fixture_sequence([
            json!({"output": [{
                "type": "function_call",
                "call_id": "call-1",
                "name": "read_evidence",
                "arguments": "{\"document_id\":\"doc-1\"}"
            }]}),
            json!({"output_text": "{}"}),
        ]);
        assert_eq!(client.respond(request()).await.unwrap().tool_calls.len(), 1);
        assert_eq!(client.respond(request()).await.unwrap().output_text, "{}");
        assert!(matches!(
            client.respond(request()).await,
            Err(ModelError::FixtureExhausted)
        ));
    }

    #[test]
    fn native_web_contract_rejects_unallowlisted_query_and_uri() {
        let policy = NativeWebPolicy::default();
        let call = ModelToolCall {
            call_id: "call-1".to_owned(),
            name: policy.tool_name.clone(),
            arguments: json!({"query": "QQQ filing", "domains": ["example.com"]}),
        };
        assert!(matches!(
            policy.validate_tool_calls(&[call]),
            Err(ModelError::NativeWebToolNotAllowed)
        ));
        assert!(matches!(
            policy.extract_citations(&json!({"citations": [{"url": "https://example.com/a"}]})),
            Err(ModelError::NativeWebUnsafeCitation)
        ));
    }

    #[test]
    fn native_web_contract_requires_citations_and_bounds_results() {
        let policy = NativeWebPolicy::default();
        let call = ModelToolCall {
            call_id: "call-1".to_owned(),
            name: policy.tool_name.clone(),
            arguments: json!({"query": "QQQ filing", "max_results": 1}),
        };
        assert_eq!(
            policy.validate_tool_calls(&[call]).unwrap()[0].max_results,
            1
        );
        assert!(matches!(
            policy.extract_citations(&json!({"output": "no citations"})),
            Err(ModelError::NativeWebCitationsMissing)
        ));
        let body = responses_request_body(
            "fixture",
            "high",
            &ModelRequest {
                tools: vec![policy.tool_definition()],
                ..request()
            },
        );
        assert_eq!(body["tools"][0]["type"], NATIVE_WEB_SEARCH_TOOL);
    }
}
