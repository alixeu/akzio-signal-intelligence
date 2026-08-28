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
            .timeout(self.timeout)
            .json(&body)
            .send()
            .await?;
        let status = response.status();
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
                    return Err(error.into());
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
        response_from_raw(raw, body)
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
        Some("response.completed") => {
            end_reasoning(stream, on_event);
            stream.response = event.get("response").cloned();
        }
        Some("response.incomplete") => {
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

pub(super) fn responses_request_body(
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
        "reasoning": {"effort": reasoning_effort, "summary": "auto"},
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
        ModelToolChoice::RequiredFunction(name) => {
            body["tool_choice"] = json!({"type": "function", "name": name});
        }
    }
    body
}

pub(super) fn response_from_raw(raw: Value, request_body: Value) -> Result<ModelResponse> {
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
    Ok(ModelResponse {
        output_text,
        tool_calls,
        continuation: ModelContinuation::from_items(
            raw.get("output")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
        ),
        raw,
        request_body,
    })
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
mod tests {
    use super::*;

    #[test]
    fn reasoning_summary_stream_maps_to_start_delta_end() {
        let mut stream = ReasoningStream::default();
        let mut events = Vec::new();
        for event in [
            json!({"type": "response.reasoning_summary_part.added"}),
            json!({"type": "response.reasoning_summary_text.delta", "delta": "First"}),
            json!({"type": "response.reasoning_summary_text.delta", "delta": " second"}),
            json!({"type": "response.reasoning_summary_text.done"}),
            json!({"type": "response.completed", "response": {"output_text": "{}"}}),
        ] {
            handle_sse_data(event.to_string().as_bytes(), &mut stream, &mut |event| {
                events.push(event)
            })
            .unwrap();
        }

        assert_eq!(
            events,
            vec![
                ModelStreamEvent::ReasoningStart,
                ModelStreamEvent::ReasoningDelta("First".to_owned()),
                ModelStreamEvent::ReasoningDelta(" second".to_owned()),
                ModelStreamEvent::ReasoningEnd,
            ]
        );
        assert_eq!(stream.response.unwrap()["output_text"], "{}");
    }

    #[test]
    fn failed_stream_closes_an_open_reasoning_sequence() {
        let mut stream = ReasoningStream::default();
        let mut events = Vec::new();
        handle_sse_data(
            br#"{"type":"response.reasoning_summary_text.delta","delta":"partial"}"#,
            &mut stream,
            &mut |event| events.push(event),
        )
        .unwrap();
        assert!(handle_sse_data(
            br#"{"type":"response.failed","response":{"error":"fixture"}}"#,
            &mut stream,
            &mut |event| events.push(event),
        )
        .is_err());
        assert_eq!(events.last(), Some(&ModelStreamEvent::ReasoningEnd));
    }
}
