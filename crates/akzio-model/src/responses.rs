//! OpenAI Responses provider adapter.

use super::*;

// Responses adapter 只负责受控 provider wire/stream 与 ModelResponse 转换；预算、Prompt、工具权限和业务 Gate 由上层拥有。
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
// Hosted reasoning responses can legitimately pause longer than 30 seconds
// between chunks. This remains a bounded transport timeout; the Agent
// Contract's 120s total wall-time and phase deadline still govern the call.
const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct OpenAIResponsesClient {
    // client 是可克隆的配置值；api_key 仅用于 bearer auth，Debug 实现会脱敏，stream timeout 只约束单次空闲。
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
        // new 使用 bounded 默认 timeout；实际累计墙钟仍由 Agent Contract/phase deadline 控制。
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
        // 在构造 HTTP client 前先规范化并拒绝空配置；这些错误属于本地配置错误，
        // 不会发起 provider I/O。
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
        // read_timeout 只限制单次流式空闲间隔；调用方的 Agent Contract/phase
        // deadline 仍负责整轮墙钟预算。
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
        // request_body 是纯 wire projection，保留调用方提供的 input/tools/tool_choice，不在 provider 层扩大授权。
        openai_responses_request_body(&self.model, &self.reasoning_effort, request)
    }

    pub async fn respond(&self, request: ModelRequest) -> Result<ModelResponse> {
        // 无事件调用复用同一 stream parser；空闭包表示调用方不订阅 reasoning 生命周期。
        self.respond_with_events(request, |_| {}).await
    }

    pub async fn respond_with_events(
        &self,
        request: ModelRequest,
        mut on_event: impl FnMut(ModelStreamEvent),
    ) -> Result<ModelResponse> {
        // 一次请求使用无状态 store=false 的 Responses 调用；函数只返回协议层
        // ModelResponse，持久化、Attempt 状态和业务 Gate 由上层负责。
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
            // 非 2xx 在读取完整错误 body 后结束本轮，不进入 SSE 解析或业务重试判断。
            return Err(ModelError::Http {
                status,
                body: response.text().await?,
            });
        }

        let mut pending = Vec::new();
        let mut data = Vec::new();
        let mut stream = ReasoningStream::default();
        // pending 保存尚未遇到换行的字节，data 聚合同一 SSE event 的连续 data 行；
        // provider 的 response.completed/incomplete 终态一旦校验并保存，就可停止等 EOF。
        'response_stream: loop {
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
                    // 空行提交一个完整 SSE event；[DONE] 只会被 handle_sse_data 忽略，
                    // 真正的 Responses 终态仍必须由 response.completed/incomplete 提供。
                    if let Err(error) = handle_sse_data(&data, &mut stream, &mut on_event) {
                        end_reasoning(&mut stream, &mut on_event);
                        return Err(error);
                    }
                    data.clear();
                    // A validated Responses terminal owns the result and its
                    // usage. HTTP EOF is not part of that protocol boundary:
                    // a gateway may keep the connection open or fail later.
                    if stream.response.is_some() {
                        break 'response_stream;
                    }
                } else if let Some(value) = line.strip_prefix(b"data:") {
                    if !data.is_empty() {
                        data.push(b'\n');
                    }
                    data.extend_from_slice(value.strip_prefix(b" ").unwrap_or(value));
                }
            }
        }
        if stream.response.is_none() {
            // EOF 前的残余字节也尝试作为最后一个 data event；若没有终态，最终统一
            // 报 missing response.completed，而不是把连接结束当作成功。
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
        }
        let raw = stream.response.ok_or_else(|| {
            ModelError::InvalidStream("missing response.completed event".to_owned())
        })?;
        // raw 已由 SSE 终态校验，下面再做 incomplete/refusal/output/usage 的统一协议
        // 解析，并把 provider request id 作为非敏感响应元数据附上。
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
    // SSE parser 通过 FnMut 回调向上层发送 reasoning 事件；终态 response 单独保存在 stream，避免把 delta 当成最终输出。
    // 解析一个已经按空行分隔的 SSE data；空 data/[DONE] 不是终态，未知事件
    // 暂时忽略，以便兼容 provider 增加非业务事件。
    if data.is_empty() || data == b"[DONE]" {
        return Ok(());
    }
    let event: Value = serde_json::from_slice(data)
        .map_err(|error| ModelError::InvalidStream(error.to_string()))?;
    match event.get("type").and_then(Value::as_str) {
        Some("response.reasoning_summary_part.added") => start_reasoning(stream, on_event),
        Some("response.reasoning_summary_text.delta") => {
            // delta 可能在 start 事件前到达，因此这里先确保只发出一次 ReasoningStart。
            start_reasoning(stream, on_event);
            if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                if !delta.is_empty() {
                    on_event(ModelStreamEvent::ReasoningDelta(delta.to_owned()));
                }
            }
        }
        Some("response.reasoning_summary_text.done") => end_reasoning(stream, on_event),
        Some("response.completed") | Some("response.incomplete") => {
            // 终态事件的类型必须与 response.status 一致；只看事件名会把失败/进行中的
            // body 错当成完整结果。
            let expected = if event["type"] == "response.completed" {
                "completed"
            } else {
                "incomplete"
            };
            let response = event
                .get("response")
                .filter(|response| response.get("status").and_then(Value::as_str) == Some(expected))
                .ok_or_else(|| {
                    ModelError::InvalidStream(format!(
                        "response.{expected} event has missing or mismatched response status"
                    ))
                })?;
            end_reasoning(stream, on_event);
            stream.response = Some(response.clone());
        }
        Some("response.failed") | Some("error") => {
            // provider 明确失败时不保存 response，调用方只能看到 InvalidStream。
            end_reasoning(stream, on_event);
            return Err(ModelError::InvalidStream(event.to_string()));
        }
        _ => {}
    }
    Ok(())
}

fn start_reasoning(stream: &mut ReasoningStream, on_event: &mut impl FnMut(ModelStreamEvent)) {
    // 多个 reasoning 事件共享一段生命周期，只向上层发出一个开始事件。
    if !stream.started {
        stream.started = true;
        on_event(ModelStreamEvent::ReasoningStart);
    }
}

fn end_reasoning(stream: &mut ReasoningStream, on_event: &mut impl FnMut(ModelStreamEvent)) {
    // 仅结束已经开始且尚未结束的 reasoning 段，避免错误路径重复发送结束事件。
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
    // 该函数只做请求表达式的确定性编译；Continue transcript 与 tool outputs 都由 Rust 显式拼接。
    // 将 ModelRequest 编译为单轮 Responses wire payload；这里仅序列化请求，不做
    // provider I/O，也不改变 Rust 持有的工具授权和预算。
    let input = match &request.input {
        ModelInput::Fresh { text } => Value::String(text.clone()),
        ModelInput::Continue {
            continuation,
            tool_outputs,
            instruction,
        } => {
            // 无状态续传沿用上一轮 transcript，随后追加 Rust 生成的 function_call_output
            // 和可选 instruction，避免 provider 端隐藏会话状态。
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
        // hosted web 的来源字段需要显式 include；普通 function tool 不走这个 provider
        // 专用分支。
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
                        // native web 使用 provider 原生 tool 类型；allowlist 从 Rust
                        // 生成的 Schema enum 映射到 filters，避免把任意域名交给 provider。
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
                        // 其他工具作为严格 function 定义发送，参数 Schema 先经过
                        // provider_schema 清理本地约束字段。
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
            // RequiredFunction 按调用方给出的函数名序列化；provider 返回后仍需由上层
            // 校验调用参数和业务提交边界。
            body["tool_choice"] = json!({"type": "function", "name": name});
        }
    }
    body
}

pub(super) fn openai_response_from_raw(raw: Value, request_body: Value) -> Result<ModelResponse> {
    // raw 到 ModelResponse 是协议验证边界：incomplete/refusal/空 output 先拒绝，再交给上层 Schema/业务校验。
    // 把 provider raw 规范化为 ModelResponse，并在协议边界拒绝 incomplete、refusal
    // 和空输出；这里不持久化，也不决定研究提交、Decision 或 Execution 状态。
    if raw.get("status").and_then(Value::as_str) == Some("incomplete") {
        let reason = raw
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        return Err(ModelError::Incomplete {
            reason,
            usage: normalize_usage(&raw),
        });
    }
    if let Some(refusal) = extract_refusal(&raw) {
        return Err(ModelError::Refused(refusal));
    }
    let output_text = extract_output_text(&raw).unwrap_or_default();
    let tool_calls = extract_tool_calls(&raw);
    if output_text.is_empty() && tool_calls.is_empty() {
        // 只有可见文本或工具调用至少有一个时，才可交给上层的 Schema/业务校验继续处理。
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
    // 按优先顺序读取 provider usage 别名；没有字段保持 None，交由上层按未知用量
    // 的预算规则处理，不能用零值冒充已计量。
    pointers
        .iter()
        .find_map(|pointer| raw.pointer(pointer).and_then(Value::as_u64))
}

/// Normalize field names emitted by Responses-compatible providers without
/// claiming their broader transport or continuation semantics are identical.
fn normalize_usage(raw: &Value) -> ModelUsage {
    // 兼容 Responses 及相近 provider 的字段命名，只归一化已报告的数字，不估算
    // 隐藏 reasoning 或缺失的输入/输出 token。
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
    // refusal 位于 output message 的 content 中；一旦找到就由协议层拒绝整轮，不能
    // 同时把其他文本或工具调用当作成功提交。
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
    // 优先读取顶层 output_text；缺失时回退到 output/content 中首个 text/output_text
    // 部分，保持 provider 表示差异对上层不可见。
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
    // 同时读取兼容 payload 的顶层 tool_calls 和 Responses output；parse_tool_call
    // 会过滤非 function/tool call 项并保留原出现顺序。
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
    // 将 function_call/tool_call 的 name、call_id 和 arguments 统一为内部类型；字符串
    // arguments 若不是 JSON 则保留为 raw 字段，真正 Schema 合法性留给上层判断。
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
    // 测试只验证 stream/continuation/usage 的协议边界，不调用真实 provider 或 Store。
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

    #[test]
    fn incomplete_response_retains_provider_usage_for_runtime_accounting() {
        let raw = json!({
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "usage": {
                "input_tokens": 123,
                "output_tokens": 4287,
                "output_tokens_details": {"reasoning_tokens": 900}
            }
        });

        let error = openai_response_from_raw(raw, json!({"input": "context"}))
            .expect_err("incomplete response must not be accepted as a complete turn");
        match error {
            ModelError::Incomplete { reason, usage } => {
                assert_eq!(reason, "max_output_tokens");
                assert_eq!(usage.input_tokens, Some(123));
                assert_eq!(usage.output_tokens, Some(4287));
                assert_eq!(usage.reasoning_tokens, Some(900));
            }
            other => panic!("expected incomplete response, got {other:?}"),
        }
    }
}
