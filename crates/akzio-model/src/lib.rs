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

mod fixture;
mod native_web;
mod responses;
mod schema;

use fixture::*;
pub use native_web::{NativeWebCitation, NativeWebPolicy, NativeWebQuery};
pub use responses::ResponsesClient;
use responses::*;
use schema::*;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub instructions: String,
    pub input: String,
    pub schema_name: Option<String>,
    pub schema: Option<Value>,
    pub max_output_tokens: u32,
    pub tools: Vec<ModelToolDefinition>,
}

/// Adapter-declared capabilities for one model client.
///
/// This is descriptive metadata only: it is not a provider handshake and
/// never grants tools, context, or execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCapabilitySnapshot {
    pub provider_id: String,
    pub model_id: String,
    pub reasoning_effort: String,
    pub supports_structured_output: bool,
    pub supports_tool_calls: bool,
    pub native_web_tool: bool,
    #[serde(default)]
    pub streaming: Option<bool>,
    #[serde(default)]
    pub declared_context_limit: Option<u32>,
    #[serde(default)]
    pub declared_max_output_tokens: Option<u32>,
    pub source: String,
}

impl ModelCapabilitySnapshot {
    pub fn unknown() -> Self {
        Self {
            provider_id: "unknown".to_owned(),
            model_id: "unknown".to_owned(),
            reasoning_effort: "unknown".to_owned(),
            supports_structured_output: false,
            supports_tool_calls: false,
            native_web_tool: false,
            streaming: None,
            declared_context_limit: None,
            declared_max_output_tokens: None,
            source: "unknown".to_owned(),
        }
    }
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

    pub fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        match self {
            Self::Responses(client) => ModelCapabilitySnapshot {
                provider_id: "responses".to_owned(),
                model_id: client.model.clone(),
                reasoning_effort: client.reasoning_effort.clone(),
                supports_structured_output: true,
                supports_tool_calls: true,
                native_web_tool: true,
                streaming: Some(false),
                declared_context_limit: None,
                declared_max_output_tokens: None,
                source: "adapter_declared".to_owned(),
            },
            Self::Fixture(_) | Self::FixtureBySchema(_) | Self::FixtureSequence(_) => {
                ModelCapabilitySnapshot {
                    provider_id: "fixture".to_owned(),
                    model_id: "fixture".to_owned(),
                    reasoning_effort: "none".to_owned(),
                    supports_structured_output: true,
                    supports_tool_calls: true,
                    native_web_tool: true,
                    streaming: Some(false),
                    declared_context_limit: None,
                    declared_max_output_tokens: None,
                    source: "adapter_declared".to_owned(),
                }
            }
        }
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
    fn response_request_body_drops_provider_unsupported_object_bounds() {
        let mut request = request();
        request.schema = Some(json!({
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "object",
                    "minProperties": 1,
                    "maxProperties": 4,
                    "properties": {
                        "assets": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 4,
                            "uniqueItems": true,
                        },
                    },
                    "patternProperties": {},
                },
            },
        }));

        let body = responses_request_body("fixture", "high", &request);
        let tasks = &body["text"]["format"]["schema"]["properties"]["tasks"];
        assert!(tasks.get("minProperties").is_none());
        assert!(tasks.get("maxProperties").is_none());
        assert!(tasks.get("patternProperties").is_none());
        assert!(tasks["properties"]["assets"].get("minItems").is_none());
        assert!(tasks["properties"]["assets"].get("maxItems").is_none());
        assert!(tasks["properties"]["assets"].get("uniqueItems").is_none());
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
    fn capability_snapshot_is_stable_and_redacted() {
        let config = ModelConfig {
            base_url: "https://example.invalid/v1".to_owned(),
            model: "fixture-model".to_owned(),
            api_key: "secret-key".to_owned(),
            reasoning_effort: "high".to_owned(),
            debug: false,
        };
        let client = ModelClient::from_config(&config).unwrap();
        let snapshot = client.capability_snapshot();
        assert_eq!(snapshot.provider_id, "responses");
        assert_eq!(snapshot.model_id, "fixture-model");
        assert_eq!(snapshot.reasoning_effort, "high");
        assert_eq!(snapshot.source, "adapter_declared");

        let encoded = serde_json::to_string(&snapshot).unwrap();
        assert!(!encoded.contains("secret-key"));
        assert!(!encoded.contains("example.invalid"));
        assert_eq!(snapshot, client.capability_snapshot());
    }

    #[test]
    fn fixture_and_unknown_capability_snapshots_are_explicit() {
        let fixture = ModelClient::Fixture(json!({"output_text": "{}"}));
        let snapshot = fixture.capability_snapshot();
        assert_eq!(snapshot.provider_id, "fixture");
        assert_eq!(snapshot.model_id, "fixture");
        assert_eq!(snapshot.reasoning_effort, "none");
        assert!(snapshot.supports_tool_calls);

        let unknown = ModelCapabilitySnapshot::unknown();
        assert_eq!(unknown.provider_id, "unknown");
        assert!(!unknown.supports_tool_calls);
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
