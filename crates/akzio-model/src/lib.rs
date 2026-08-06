//! Stateless Responses API adapter.
//!
//! Akzio owns every durable turn, context manifest, and tool result. The
//! provider only receives the current, replayable turn and may request one of
//! the Rust-approved tools declared by an agent contract.

use std::{
    collections::{BTreeMap, VecDeque},
    env,
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
    #[error("model response transport failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("model returned HTTP {status}: {body}")]
    Http { status: StatusCode, body: String },
    #[error("model response has neither output text nor a tool call")]
    MissingOutput,
    #[error("fixture response sequence is exhausted")]
    FixtureExhausted,
    #[error("missing environment variable {0}")]
    MissingEnvironment(&'static str),
}

pub type Result<T> = std::result::Result<T, ModelError>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelResponse {
    pub output_text: String,
    pub tool_calls: Vec<ModelToolCall>,
    pub raw: Value,
}

#[derive(Debug, Clone)]
pub struct ResponsesClient {
    http: Client,
    base_url: String,
    api_key: String,
    model: String,
}

impl ResponsesClient {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        if base_url.is_empty() {
            return Err(ModelError::EmptyBaseUrl);
        }
        Ok(Self {
            http: Client::new(),
            base_url,
            api_key: api_key.into(),
            model: model.into(),
        })
    }

    pub async fn respond(&self, request: ModelRequest) -> Result<ModelResponse> {
        let ModelRequest {
            instructions,
            input,
            schema_name,
            schema,
            max_output_tokens,
            tools,
        } = request;
        let mut body = json!({
            "model": self.model,
            "instructions": instructions,
            "input": input,
            "max_output_tokens": max_output_tokens,
            "store": false,
        });
        if let (Some(name), Some(schema)) = (schema_name, schema) {
            body["text"] = json!({
                "format": {
                    "type": "json_schema",
                    "name": name,
                    "schema": schema,
                    "strict": true,
                }
            });
        }
        if !tools.is_empty() {
            body["tools"] = Value::Array(
                tools
                    .into_iter()
                    .map(|tool| {
                        json!({
                            "type": "function",
                            "name": tool.name,
                            "description": tool.description,
                            "parameters": tool.input_schema,
                        })
                    })
                    .collect(),
            );
            body["tool_choice"] = json!("auto");
        }
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
        response_from_raw(raw)
    }
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

    pub fn from_env() -> Result<Self> {
        let base_url = env::var("LLM_GATEWAY_BASE_URL")
            .map_err(|_| ModelError::MissingEnvironment("LLM_GATEWAY_BASE_URL"))?;
        let api_key = env::var("LLM_GATEWAY_API_KEY")
            .map_err(|_| ModelError::MissingEnvironment("LLM_GATEWAY_API_KEY"))?;
        let model =
            env::var("AKZIO_MODEL").map_err(|_| ModelError::MissingEnvironment("AKZIO_MODEL"))?;
        Ok(Self::Responses(ResponsesClient::new(
            base_url, api_key, model,
        )?))
    }

    pub async fn respond(&self, request: ModelRequest) -> Result<ModelResponse> {
        match self {
            Self::Responses(client) => client.respond(request).await,
            Self::Fixture(raw) => response_from_raw(raw.clone()),
            Self::FixtureBySchema(outputs) => {
                let schema = request
                    .schema_name
                    .as_deref()
                    .ok_or(ModelError::MissingOutput)?;
                let raw = outputs.get(schema).ok_or(ModelError::MissingOutput)?;
                response_from_raw(raw.clone())
            }
            Self::FixtureSequence(values) => {
                let raw = values
                    .lock()
                    .expect("fixture response sequence poisoned")
                    .pop_front()
                    .ok_or(ModelError::FixtureExhausted)?;
                response_from_raw(raw)
            }
        }
    }
}

fn response_from_raw(raw: Value) -> Result<ModelResponse> {
    let output_text = extract_output_text(&raw).unwrap_or_default();
    let tool_calls = extract_tool_calls(&raw);
    if output_text.is_empty() && tool_calls.is_empty() {
        return Err(ModelError::MissingOutput);
    }
    Ok(ModelResponse {
        output_text,
        tool_calls,
        raw,
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
