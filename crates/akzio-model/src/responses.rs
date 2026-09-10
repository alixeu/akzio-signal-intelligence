//! OpenAI Responses provider adapter.

use super::*;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
// Hosted reasoning responses can legitimately pause longer than 30 seconds
// between chunks. This remains a bounded transport timeout; the Agent
// Contract's 120s total wall-time and phase deadline still govern the call.
const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct OpenAIResponsesClient {
    http: Client,
    pub(super) base_url: String,
    api_key: String,
    pub(super) model: String,
    pub(super) reasoning_effort: String,
    stream_idle_timeout: Duration,
}

impl std::fmt::Debug for OpenAIResponsesClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAIResponsesClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .field("model", &self.model)
            .field("reasoning_effort", &self.reasoning_effort)
            .finish_non_exhaustive()
    }
}

impl OpenAIResponsesClient {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
        reasoning_effort: impl Into<String>,
    ) -> Result<Self> {
        Self::with_timeouts(
            base_url,
            api_key,
            model,
            reasoning_effort,
            DEFAULT_CONNECT_TIMEOUT,
            DEFAULT_STREAM_IDLE_TIMEOUT,
        )
    }

    fn with_timeouts(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
        reasoning_effort: impl Into<String>,
        connect_timeout: Duration,
        stream_idle_timeout: Duration,
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
            http: Client::builder()
                .connect_timeout(connect_timeout)
                .read_timeout(stream_idle_timeout)
                .build()?,
            base_url,
            api_key,
            model,
            reasoning_effort,
            stream_idle_timeout,
        })
    }

    pub fn request_body(&self, request: &ModelRequest) -> Value {
        openai_responses_request_body(&self.model, &self.reasoning_effort, request)
    }

    pub fn declared_capabilities(&self) -> OpenAIResponsesCapabilities {
        OpenAIResponsesCapabilities {
            supports_tool_calls: false,
            supports_stateless_continuation: false,
            reasoning_items: false,
            encrypted_continuation: false,
            native_web_tool: false,
            streaming: false,
            basis: ModelCapabilityBasis::Unknown,
            verified: false,
        }
    }

    pub async fn respond(&self, request: ModelRequest) -> Result<ModelResponse> {
        self.respond_with_events(request, |_| {}).await
    }

    pub async fn respond_with_events(
        &self,
        request: ModelRequest,
        mut on_event: impl FnMut(ModelStreamEvent),
    ) -> Result<ModelResponse> {
        let body = self.request_body(&request);
        let mut response = self
            .http
            .post(format!("{}/responses", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let provider_request_id = response
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        if !status.is_success() {
            return Err(ModelError::Http {
                status,
                body: response.text().await?,
            });
        }

        let mut pending = Vec::new();
        let mut data = Vec::new();
        let mut stream = ReasoningStream::default();
        loop {
            let chunk = match response.chunk().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(error) => {
                    end_reasoning(&mut stream, &mut on_event);
                    return Err(if error.is_timeout() {
                        ModelError::StreamIdleTimeout {
                            idle_timeout: self.stream_idle_timeout,
                        }
                    } else {
                        error.into()
                    });
                }
            };
            pending.extend_from_slice(&chunk);
            while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                let mut line = pending.drain(..=newline).collect::<Vec<_>>();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.is_empty() {
                    if let Err(error) = handle_sse_data(&data, &mut stream, &mut on_event) {
                        end_reasoning(&mut stream, &mut on_event);
                        return Err(error);
                    }
                    data.clear();
                } else if let Some(value) = line.strip_prefix(b"data:") {
                    if !data.is_empty() {
                        data.push(b'\n');
                    }
                    data.extend_from_slice(value.strip_prefix(b" ").unwrap_or(value));
                }
            }
        }
        if pending.last() == Some(&b'\r') {
            pending.pop();
        }
        if let Some(value) = pending.strip_prefix(b"data:") {
            if !data.is_empty() {
                data.push(b'\n');
            }
            data.extend_from_slice(value.strip_prefix(b" ").unwrap_or(value));
        }
        if let Err(error) = handle_sse_data(&data, &mut stream, &mut on_event) {
            end_reasoning(&mut stream, &mut on_event);
            return Err(error);
        }
        let raw = stream.response.ok_or_else(|| {
            ModelError::InvalidStream("missing response.completed event".to_owned())
        })?;
        openai_response_from_raw(raw, body).map(|mut result| {
            result.provider_request_id = provider_request_id;
            result
        })
    }
}

#[derive(Default)]
struct ReasoningStream {
    started: bool,
    ended: bool,
    response: Option<Value>,
}

fn handle_sse_data(
    data: &[u8],
    stream: &mut ReasoningStream,
    on_event: &mut impl FnMut(ModelStreamEvent),
) -> Result<()> {
    if data.is_empty() || data == b"[DONE]" {
        return Ok(());
    }
    let event: Value = serde_json::from_slice(data)
        .map_err(|error| ModelError::InvalidStream(error.to_string()))?;
    match event.get("type").and_then(Value::as_str) {
        Some("response.reasoning_summary_part.added") => start_reasoning(stream, on_event),
        Some("response.reasoning_summary_text.delta") => {
            start_reasoning(stream, on_event);
            if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                if !delta.is_empty() {
                    on_event(ModelStreamEvent::ReasoningDelta(delta.to_owned()));
                }
            }
        }
        Some("response.reasoning_summary_text.done") => end_reasoning(stream, on_event),
        Some("response.completed") | Some("response.incomplete") => {
            end_reasoning(stream, on_event);
            stream.response = event.get("response").cloned();
        }
        Some("response.failed") | Some("error") => {
            end_reasoning(stream, on_event);
            return Err(ModelError::InvalidStream(event.to_string()));
        }
        _ => {}
    }
    Ok(())
}

fn start_reasoning(stream: &mut ReasoningStream, on_event: &mut impl FnMut(ModelStreamEvent)) {
    if !stream.started {
        stream.started = true;
        on_event(ModelStreamEvent::ReasoningStart);
    }
}

fn end_reasoning(stream: &mut ReasoningStream, on_event: &mut impl FnMut(ModelStreamEvent)) {
    if stream.started && !stream.ended {
        stream.ended = true;
        on_event(ModelStreamEvent::ReasoningEnd);
    }
}

pub(super) fn openai_responses_request_body(
    model: &str,
    reasoning_effort: &str,
    request: &ModelRequest,
) -> Value {
    let input = match &request.input {
        ModelInput::Fresh { text } => Value::String(text.clone()),
        ModelInput::Continue {
            continuation,
            tool_outputs,
            instruction,
        } => {
            let mut items = continuation.items.clone();
            items.extend(tool_outputs.iter().map(|output| {
                json!({
                    "type": "function_call_output",
                    "call_id": output.call_id,
                    "output": serde_json::to_string(&output.output)
                        .expect("JSON tool output always serializes"),
                })
            }));
            if let Some(instruction) = instruction {
                items.push(json!({
                    "role": "user",
                    "content": instruction,
                }));
            }
            Value::Array(items)
        }
    };
    let mut body = json!({
        "model": model,
        "instructions": request.instructions,
        "input": input,
        "max_output_tokens": request.max_output_tokens,
        "reasoning": {"effort": request.reasoning_effort.as_deref().unwrap_or(reasoning_effort), "summary": "auto"},
        "include": ["reasoning.encrypted_content"],
        "store": false,
        "stream": true,
    });
    if request
        .tools
        .iter()
        .any(|tool| tool.name == NATIVE_WEB_SEARCH_TOOL)
    {
        body["include"]
            .as_array_mut()
            .expect("Responses include is an array")
            .push(json!("web_search_call.action.sources"));
    }
    if !request.tools.is_empty() {
        body["tools"] = Value::Array(
            request
                .tools
                .iter()
                .map(|tool| {
                    if tool.name == NATIVE_WEB_SEARCH_TOOL {
                        let mut native_web = json!({"type": NATIVE_WEB_SEARCH_TOOL});
                        if let Some(domains) = tool
                            .input_schema
                            .pointer("/properties/domains/items/enum")
                            .filter(|domains| domains.is_array())
                        {
                            native_web["filters"] = json!({
                                "allowed_domains": domains,
                            });
                        }
                        native_web
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
    }
    match &request.tool_choice {
        ModelToolChoice::None => {}
        ModelToolChoice::Auto => body["tool_choice"] = json!("auto"),
        ModelToolChoice::Required => body["tool_choice"] = json!("required"),
        ModelToolChoice::RequiredFunction(name) => {
            body["tool_choice"] = json!({"type": "function", "name": name});
        }
    }
    body
}

pub(super) fn openai_response_from_raw(raw: Value, request_body: Value) -> Result<ModelResponse> {
    if raw.get("status").and_then(Value::as_str) == Some("incomplete") {
        let reason = raw
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        return Err(ModelError::Incomplete(reason));
    }
    if let Some(refusal) = extract_refusal(&raw) {
        return Err(ModelError::Refused(refusal));
    }
    let output_text = extract_output_text(&raw).unwrap_or_default();
    let tool_calls = extract_tool_calls(&raw);
    if output_text.is_empty() && tool_calls.is_empty() {
        return Err(ModelError::MissingOutput);
    }
    let usage = normalize_usage(&raw);
    // store=false has no server-side conversation. Keep the actual input as
    // well as this output, including earlier tool results and the initial
    // authorized context. Returning only output silently forgets that context.
    let mut transcript = match request_body.get("input") {
        Some(Value::String(text)) => vec![json!({"role":"user","content":text})],
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    };
    transcript.extend(
        raw.get("output")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    );
    Ok(ModelResponse {
        provider_request_id: None,
        output_text,
        tool_calls,
        continuation: ModelContinuation::from_items(transcript),
        raw,
        usage,
        request_body,
    })
}

fn usage_value(raw: &Value, pointers: &[&str]) -> Option<u64> {
    pointers
        .iter()
        .find_map(|pointer| raw.pointer(pointer).and_then(Value::as_u64))
}

/// Normalize field names emitted by Responses-compatible providers without
/// claiming their broader transport or continuation semantics are identical.
fn normalize_usage(raw: &Value) -> ModelUsage {
    ModelUsage {
        input_tokens: usage_value(raw, &["/usage/input_tokens", "/usage/prompt_tokens"]),
        cached_input_tokens: usage_value(
            raw,
            &[
                "/usage/input_tokens_details/cached_tokens",
                "/usage/prompt_tokens_details/cached_tokens",
                "/usage/cache_read_input_tokens",
                "/usage/cached_input_tokens",
            ],
        ),
        output_tokens: usage_value(raw, &["/usage/output_tokens", "/usage/completion_tokens"]),
        reasoning_tokens: usage_value(
            raw,
            &[
                "/usage/output_tokens_details/reasoning_tokens",
                "/usage/completion_tokens_details/reasoning_tokens",
                "/usage/reasoning_tokens",
            ],
        ),
    }
}

fn extract_refusal(response: &Value) -> Option<String> {
    response
        .get("output")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(|item| item.get("content").and_then(Value::as_array))
        .flatten()
        .find(|part| part.get("type").and_then(Value::as_str) == Some("refusal"))
        .and_then(|part| part.get("refusal").and_then(Value::as_str))
        .map(ToOwned::to_owned)
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

#[cfg(test)]
mod transcript_regression {
    use super::*;

    #[test]
    fn stream_idle_timeout_is_bounded_but_allows_hosted_reasoning_pause() {
        assert_eq!(DEFAULT_STREAM_IDLE_TIMEOUT, Duration::from_secs(60));
        assert!(DEFAULT_STREAM_IDLE_TIMEOUT < Duration::from_secs(120));
    }

    #[test]
    fn stateless_continuation_retains_initial_context_and_prior_tool_results() {
        let output = json!({"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"memo"}]}]});
        let first =
            openai_response_from_raw(output.clone(), json!({"input":"authorized context A"}))
                .unwrap();
        assert_eq!(
            first.continuation.items()[0]["content"],
            "authorized context A"
        );
        let mut items = first.continuation.items().to_vec();
        items.push(json!({"type":"function_call_output","call_id":"call_A","output":"evidence A"}));
        let second = openai_response_from_raw(output, json!({"input":items})).unwrap();
        assert_eq!(
            second.continuation.items()[0]["content"],
            "authorized context A"
        );
        assert_eq!(second.continuation.items()[2]["output"], "evidence A");
        assert_eq!(second.continuation.items().len(), 4);
    }
}
