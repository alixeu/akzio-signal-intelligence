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
    #[error("model response stream is invalid: {0}")]
    InvalidStream(String),
    #[error("model refused the request: {0}")]
    Refused(String),
    #[error("model response is incomplete: {0}")]
    Incomplete(String),
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

fn default_response_language() -> String {
    "简体中文".to_owned()
}

/// Production model settings loaded from the local Akzio TOML configuration.
///
/// The API key is intentionally redacted from `Debug` output and never copied
/// into a durable AgentTurn trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRouteConfig {
    pub model: String,
    pub reasoning_effort: String,
    #[serde(default)]
    pub response_language: Option<String>,
}

#[derive(Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    #[serde(default = "default_reasoning_effort")]
    pub reasoning_effort: String,
    #[serde(default = "default_response_language")]
    pub response_language: String,
    #[serde(default)]
    pub debug: bool,
    #[serde(default)]
    pub routes: BTreeMap<String, ModelRouteConfig>,
}

impl std::fmt::Debug for ModelConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelConfig")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_key", &"<redacted>")
            .field("reasoning_effort", &self.reasoning_effort)
            .field("response_language", &self.response_language)
            .field("routes", &self.routes)
            .field("debug", &self.debug)
            .finish()
    }
}

impl ModelConfig {
    pub fn for_route(&self, route: &ModelRouteConfig) -> Self {
        Self {
            base_url: self.base_url.clone(),
            model: route.model.clone(),
            api_key: self.api_key.clone(),
            reasoning_effort: route.reasoning_effort.clone(),
            response_language: route
                .response_language
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(&self.response_language)
                .to_owned(),
            debug: self.debug,
            routes: BTreeMap::new(),
        }
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
pub struct ModelToolOutput {
    pub call_id: String,
    pub output: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelContinuation {
    items: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fixture_input: Option<String>,
}

impl ModelContinuation {
    pub fn from_items(items: Vec<Value>) -> Self {
        Self {
            items,
            fixture_input: None,
        }
    }

    fn with_fixture_input(mut self, fixture_input: Option<String>) -> Self {
        self.fixture_input = fixture_input;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelToolChoice {
    None,
    Auto,
    RequiredFunction(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModelInput {
    Fresh {
        text: String,
    },
    Continue {
        continuation: ModelContinuation,
        tool_outputs: Vec<ModelToolOutput>,
        instruction: Option<String>,
    },
}

pub const NATIVE_WEB_SEARCH_TOOL: &str = "web_search_preview";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub instructions: String,
    pub input: ModelInput,
    pub max_output_tokens: u32,
    pub tools: Vec<ModelToolDefinition>,
    pub tool_choice: ModelToolChoice,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixture_key: Option<String>,
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
    pub supports_tool_calls: bool,
    pub supports_stateless_continuation: bool,
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
            supports_tool_calls: false,
            supports_stateless_continuation: false,
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
    pub continuation: ModelContinuation,
    pub raw: Value,
    /// Provider payload without authorization headers or credentials.
    pub request_body: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelStreamEvent {
    ReasoningStart,
    ReasoningDelta(String),
    ReasoningEnd,
}

#[derive(Debug, Clone)]
pub enum ModelClient {
    Responses(ResponsesClient),
    Fixture(Value),
    FixtureByPurpose(Arc<Mutex<BTreeMap<String, VecDeque<Value>>>>),
    FixtureSequence(Arc<Mutex<VecDeque<Value>>>),
}
include!("model_parts/client_setup.rs");
include!("model_parts/client_response.rs");
#[cfg(test)]
#[path = "model_parts/tests.rs"]
mod tests;
