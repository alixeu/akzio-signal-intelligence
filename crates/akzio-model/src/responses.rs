//! OpenAI Responses provider adapter.

use super::*;

#[derive(Clone)]
pub struct ResponsesClient {
    http: Client,
    base_url: String,
    api_key: String,
    pub(super) model: String,
    pub(super) reasoning_effort: String,
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

pub(super) fn responses_request_body(
    model: &str,
    reasoning_effort: &str,
    request: &ModelRequest,
) -> Value {
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
                "name": provider_schema_name(name),
                "schema": provider_schema(schema),
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
                            "parameters": provider_schema(&tool.input_schema),
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

pub(super) fn response_from_raw(raw: Value, request_body: Value) -> Result<ModelResponse> {
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

pub(super) fn parse_tool_call(value: &Value) -> Option<ModelToolCall> {
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
