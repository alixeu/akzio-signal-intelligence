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

#[derive(Clone)]
pub struct ResponsesClient {
    http: Client,
    base_url: String,
    api_key: String,
    model: String,
    reasoning_effort: String,
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
                    json!({
                        "type": "function",
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": tool.input_schema,
                        "strict": tool.strict,
                    })
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
}
