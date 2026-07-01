use agent_loop::{
    AgentLoopConfig, ModelEventHandler, ModelStreamEvent, ProjectToolRuntime, RigLoopModel,
    ToolCallRequest, Turn,
};
use anyhow::{bail, Context, Result};
use futures::StreamExt;
use orchestrator_core::{
    default_project_root, extract_json_artifact, validate_research_artifact, ResearchArtifact,
};
use rig_core::{
    agent::AgentBuilder,
    client::CompletionClient,
    completion::{CompletionModel, Prompt},
    providers::openai::{self, responses_api},
    streaming::StreamedAssistantContent,
};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{fs, path::PathBuf, sync::Arc};
use uuid::Uuid;
use web_search::{
    validate_web_search_runtime_config, ExaWebSearchProvider, MockWebSearchProvider,
    WebSearchConfig, WebSearchMode, WebSearchProviderKind,
};

pub mod agent_loop;
pub mod tools;
pub mod web_search;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmRoute {
    Responses,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmTransport {
    #[default]
    Http,
    Ws,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    JsonArtifact,
    ResearchArtifact,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleLlmSettings {
    pub route: LlmRoute,
    pub model: String,
    #[serde(default)]
    pub preamble: Option<String>,
    #[serde(default)]
    pub max_turns: Option<usize>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub transport: LlmTransport,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub think_tool: bool,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub native_web_search: bool,
}

impl RoleLlmSettings {
    pub fn validate(&self, role: &str) -> Result<()> {
        if self.model.trim().is_empty() {
            bail!("LLM config for role {role:?} requires model");
        }
        if self.max_turns == Some(0) {
            bail!("LLM config for role {role:?} requires max_turns >= 1");
        }
        if self
            .base_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            bail!("LLM config for role {role:?} requires base_url for openai_compatible");
        }
        let has_api_key = self
            .api_key
            .as_deref()
            .map(str::trim)
            .is_some_and(|value| !value.is_empty());
        if !has_api_key {
            bail!("LLM config for role {role:?} requires api_key for openai_compatible");
        }
        for tool in &self.tools {
            validate_tool_name(tool)
                .with_context(|| format!("unknown tool name {tool:?} for role {role:?}"))?;
        }
        if let Some(effort) = &self.reasoning_effort {
            validate_reasoning_effort(effort)?;
        }
        Ok(())
    }

    pub fn effective_reasoning_effort<'a>(
        &'a self,
        override_effort: Option<&'a str>,
    ) -> Option<&'a str> {
        override_effort
            .filter(|value| !value.trim().is_empty())
            .or(self.reasoning_effort.as_deref())
    }

    pub fn effective_preamble(&self) -> Option<&str> {
        self.preamble
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }
}

#[derive(Debug, Clone)]
pub struct RigSettings {
    pub role: String,
    pub phase: Option<i64>,
    pub tickers: Vec<String>,
    pub output_mode: OutputMode,
    pub llm: RoleLlmSettings,
    pub reasoning_effort_override: Option<String>,
    pub tools: Option<tools::ExternalToolConfig>,
    pub web_search: WebSearchConfig,
}

pub async fn run_rig_agent_loop(settings: &RigSettings, prompt: &str) -> Result<Value> {
    settings.llm.validate(&settings.role)?;
    validate_fallback_web_search_runtime_config(settings)?;
    let conn = open_loop_connection(settings)?;
    let session_id = loop_session_id(settings);
    let turn_id = format!("turn-{}", Uuid::new_v4());
    let mut turn = Turn::new(
        turn_id,
        session_id,
        loop_run_id(settings),
        settings.role.clone(),
        prompt.to_string(),
    );
    turn.phase = settings.phase;
    turn.model_context = format!(
        "role={}, output_mode={:?}, tickers={}\navailable_tools={}",
        settings.role,
        settings.output_mode,
        settings.tickers.join(","),
        serde_json::to_string(&configured_tool_names(settings))?
    );
    let tool_config = settings.tools.clone().unwrap_or_else(default_tool_config);
    let mut tools = ProjectToolRuntime::new(tool_config);
    if let Some(web_run) = web_run_runtime_for_settings(settings) {
        tools = tools.with_web_run_runtime(web_run);
    }
    let mut model = RigLoopModel::new(settings.clone());
    agent_loop::run_turn(
        &conn,
        &mut turn,
        &mut model,
        &mut tools,
        AgentLoopConfig {
            max_agent_loops: settings.llm.max_turns,
            ..AgentLoopConfig::default()
        },
    )
    .await?;
    write_role_end_context(settings, &turn)?;
    let final_text = turn
        .emitted_items
        .iter()
        .rev()
        .find(|item| item.item_type == agent_loop::TurnItemType::AssistantMessage)
        .map(|item| {
            if !item.content_text.trim().is_empty() {
                item.content_text.clone()
            } else {
                item.content_json.to_string()
            }
        })
        .context("agent loop finished without assistant message")?;
    parse_final_output(settings, &final_text)
}

fn write_role_end_context(settings: &RigSettings, turn: &Turn) -> Result<()> {
    let Some(run_dir) = settings
        .tools
        .as_ref()
        .and_then(|tools| tools.run_dir.as_ref())
    else {
        return Ok(());
    };
    let phase = settings.phase.unwrap_or_default();
    let path = run_dir.join(format!("phase{phase:02}")).join(format!(
        "{}_end_context.jsonl",
        safe_path_part(&settings.role)
    ));
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut lines = Vec::new();
    for item in &turn.emitted_items {
        if let Some(value) = end_context_item(item) {
            lines.push(serde_json::to_string(&value)?);
        }
    }
    fs::write(path, lines.join("\n"))?;
    Ok(())
}

fn end_context_item(item: &agent_loop::TurnItem) -> Option<Value> {
    match item.item_type {
        agent_loop::TurnItemType::UserMessage => Some(json!({
            "role": "user",
            "content": item.content_text
        })),
        agent_loop::TurnItemType::AssistantMessage => Some(json!({
            "role": "assistant",
            "content": item.content_text
        })),
        agent_loop::TurnItemType::ToolCall => {
            let call = item.content_json.get("call")?;
            let arguments = call.get("arguments").cloned().unwrap_or(Value::Null);
            Some(json!({
                "role": "assistant",
                "tool_calls": [{
                    "id": call.get("call_id").and_then(Value::as_str).unwrap_or(&item.tool_call_id),
                    "type": "function",
                    "function": {
                        "name": call.get("name").and_then(Value::as_str).unwrap_or(&item.tool_name),
                        "arguments": serde_json::to_string(&arguments).unwrap_or_else(|_| "null".to_string())
                    }
                }]
            }))
        }
        agent_loop::TurnItemType::ToolResult => Some(json!({
            "role": "tool",
            "tool_call_id": item.tool_call_id,
            "content": item.content_text
        })),
        _ => None,
    }
}

fn safe_path_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn web_search_provider(config: &WebSearchConfig) -> Option<tools::SharedWebSearchProvider> {
    match config.mode {
        WebSearchMode::Disabled => None,
        WebSearchMode::Cached => Some(Arc::new(MockWebSearchProvider::default())),
        WebSearchMode::Live => match config.provider {
            WebSearchProviderKind::Mock => Some(Arc::new(MockWebSearchProvider::default())),
            WebSearchProviderKind::Exa => Some(Arc::new(ExaWebSearchProvider::from_config(config))),
        },
    }
}

fn web_run_runtime(config: &WebSearchConfig) -> Option<tools::WebRunRuntime> {
    if config.mode == WebSearchMode::Disabled {
        return None;
    }
    let runtime = tools::WebRunRuntime::new(config.clone());
    Some(if let Some(provider) = web_search_provider(config) {
        runtime.with_provider(provider)
    } else {
        runtime
    })
}

fn web_run_runtime_for_settings(settings: &RigSettings) -> Option<tools::WebRunRuntime> {
    if uses_web_run_fallback(settings) {
        web_run_runtime(&settings.web_search)
    } else {
        None
    }
}

fn uses_native_web_search(settings: &RigSettings) -> bool {
    settings.llm.native_web_search && settings.web_search.mode != WebSearchMode::Disabled
}

fn uses_web_run_fallback(settings: &RigSettings) -> bool {
    !uses_native_web_search(settings) && settings.web_search.mode != WebSearchMode::Disabled
}

fn validate_fallback_web_search_runtime_config(settings: &RigSettings) -> Result<()> {
    if uses_web_run_fallback(settings) {
        validate_web_search_runtime_config(&settings.web_search, &settings.role)
    } else {
        Ok(())
    }
}

async fn run_model_text_once(settings: &RigSettings, prompt: &str) -> Result<String> {
    let client = openai_compatible_responses_client(&settings.llm)?;
    let model = client.completion_model(&settings.llm.model);
    let builder = AgentBuilder::new(model).default_max_turns(1);
    let builder = apply_optional_preamble(builder, &settings.llm);
    let builder = if let Some(params) = additional_params(settings) {
        builder.additional_params(params)
    } else {
        builder
    };
    builder
        .build()
        .prompt(prompt)
        .await
        .context("OpenAI-compatible Responses ReAct prompt failed")
}

pub async fn run_model_event_stream(
    settings: &RigSettings,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    let client = openai_compatible_responses_client(&settings.llm)?;
    let model = client.completion_model(&settings.llm.model);
    match settings.llm.transport {
        LlmTransport::Http => stream_completion_model(settings, model, prompt, handler).await,
        LlmTransport::Ws => {
            stream_openai_compatible_responses_websocket(settings, model, prompt, handler).await
        }
    }
}

async fn stream_openai_compatible_responses_websocket(
    settings: &RigSettings,
    model: rig_core::providers::openai::responses_api::ResponsesCompletionModel,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    let mut builder = model.completion_request(prompt.to_string());
    if let Some(preamble) = settings.llm.effective_preamble() {
        builder = builder.preamble(preamble.to_string());
    }
    if let Some(params) = additional_params(settings) {
        builder = builder.additional_params(params);
    }
    let client = openai_compatible_responses_client(&settings.llm)?;
    let mut session = client
        .responses_websocket(settings.llm.model.clone())
        .await
        .context("OpenAI-compatible Responses websocket connection failed")?;
    let result =
        stream_openai_compatible_responses_websocket_events(&mut session, builder.build(), handler)
            .await
            .context("OpenAI-compatible Responses websocket completion failed");
    let close_result = session.close().await;
    if let Err(error) = close_result {
        return Err(anyhow::anyhow!(error))
            .context("OpenAI-compatible Responses websocket close failed");
    }
    result
}

async fn stream_openai_compatible_responses_websocket_events(
    session: &mut responses_api::websocket::ResponsesWebSocketSession,
    request: rig_core::completion::CompletionRequest,
    handler: &mut dyn ModelEventHandler,
) -> Result<()> {
    session.send(request).await?;
    loop {
        match session.next_event().await? {
            responses_api::websocket::ResponsesWebSocketEvent::Item(chunk) => match chunk.data {
                responses_api::streaming::ItemChunkKind::OutputTextDelta(delta)
                | responses_api::streaming::ItemChunkKind::RefusalDelta(delta) => {
                    if !delta.delta.is_empty() {
                        handler
                            .handle(ModelStreamEvent::AssistantTextDelta {
                                item_id: chunk
                                    .item_id
                                    .clone()
                                    .unwrap_or_else(|| format!("ws-text-{}", Uuid::new_v4())),
                                delta: delta.delta,
                            })
                            .await?;
                    }
                }
                responses_api::streaming::ItemChunkKind::ReasoningSummaryTextDelta(delta) => {
                    if !delta.delta.is_empty() {
                        handler
                            .handle(ModelStreamEvent::ReasoningSummaryDelta {
                                item_id: chunk
                                    .item_id
                                    .clone()
                                    .unwrap_or_else(|| "ws-reasoning".to_string()),
                                delta: delta.delta,
                            })
                            .await?;
                    }
                }
                responses_api::streaming::ItemChunkKind::OutputItemDone(output) => {
                    if let responses_api::Output::FunctionCall(function_call) = output.item {
                        handler
                            .handle(ModelStreamEvent::ToolCallCompleted {
                                tool_call: ToolCallRequest {
                                    call_id: function_call.call_id,
                                    name: function_call.name,
                                    arguments: function_call.arguments,
                                },
                            })
                            .await?;
                    }
                }
                _ => {}
            },
            responses_api::websocket::ResponsesWebSocketEvent::Response(chunk) => {
                match chunk.kind {
                    responses_api::streaming::ResponseChunkKind::ResponseCompleted => {
                        handler
                            .handle(ModelStreamEvent::ResponseCompleted {
                                end_turn: true,
                                raw: serde_json::to_value(chunk.response)
                                    .context("failed to serialize websocket response")?,
                            })
                            .await?;
                        return Ok(());
                    }
                    responses_api::streaming::ResponseChunkKind::ResponseFailed
                    | responses_api::streaming::ResponseChunkKind::ResponseIncomplete => {
                        response_status_result(chunk.response)?;
                    }
                    responses_api::streaming::ResponseChunkKind::ResponseCreated
                    | responses_api::streaming::ResponseChunkKind::ResponseInProgress => {}
                }
            }
            responses_api::websocket::ResponsesWebSocketEvent::Done(done) => {
                handler
                    .handle(ModelStreamEvent::ResponseCompleted {
                        end_turn: true,
                        raw: done.response,
                    })
                    .await?;
                return Ok(());
            }
            responses_api::websocket::ResponsesWebSocketEvent::Error(error) => {
                bail!("OpenAI-compatible Responses websocket error: {error}");
            }
        }
    }
}

fn response_status_result(response: responses_api::CompletionResponse) -> Result<()> {
    match response.status {
        responses_api::ResponseStatus::Completed => Ok(()),
        responses_api::ResponseStatus::Failed => {
            let message = response
                .error
                .map(|error| {
                    if error.code.is_empty() {
                        error.message
                    } else {
                        format!("{}: {}", error.code, error.message)
                    }
                })
                .unwrap_or_else(|| "OpenAI-compatible Responses websocket failed".to_string());
            bail!("{message}")
        }
        responses_api::ResponseStatus::Incomplete => {
            let reason = response
                .incomplete_details
                .map(|details| details.reason)
                .unwrap_or_else(|| "unknown reason".to_string());
            bail!("OpenAI-compatible Responses websocket incomplete: {reason}")
        }
        status => bail!("OpenAI-compatible Responses websocket ended with status {status:?}"),
    }
}

async fn stream_completion_model<M>(
    settings: &RigSettings,
    model: M,
    prompt: &str,
    handler: &mut dyn ModelEventHandler,
) -> Result<()>
where
    M: CompletionModel,
    M::StreamingResponse: Clone + Unpin,
{
    let mut builder = model.completion_request(prompt.to_string());
    if let Some(preamble) = settings.llm.effective_preamble() {
        builder = builder.preamble(preamble.to_string());
    }
    if let Some(params) = additional_params(settings) {
        builder = builder.additional_params(params);
    }
    let mut stream = builder.stream().await.context("LLM stream failed")?;
    let mut parser = RuntimeEventStreamParser::default();
    let mut fallback_text = String::new();
    while let Some(chunk) = stream.next().await {
        match chunk.context("LLM stream chunk failed")? {
            StreamedAssistantContent::Text(text) => {
                fallback_text.push_str(text.text());
                parser.push_text(text.text(), handler).await?;
            }
            StreamedAssistantContent::Reasoning(reasoning) => {
                let text = reasoning.display_text();
                if !text.trim().is_empty() {
                    let item_id = reasoning
                        .id
                        .clone()
                        .unwrap_or_else(|| format!("reasoning-{}", Uuid::new_v4()));
                    handler
                        .handle(ModelStreamEvent::ReasoningSummaryDelta {
                            item_id: item_id.clone(),
                            delta: text,
                        })
                        .await?;
                    handler
                        .handle(ModelStreamEvent::ReasoningSummaryCompleted { item_id })
                        .await?;
                }
            }
            StreamedAssistantContent::ReasoningDelta { id, reasoning } => {
                if !reasoning.trim().is_empty() {
                    handler
                        .handle(ModelStreamEvent::ReasoningSummaryDelta {
                            item_id: id.unwrap_or_else(|| "reasoning-stream".to_string()),
                            delta: reasoning,
                        })
                        .await?;
                }
            }
            StreamedAssistantContent::ToolCall { tool_call, .. } => {
                handler
                    .handle(ModelStreamEvent::ToolCallCompleted {
                        tool_call: ToolCallRequest {
                            call_id: tool_call.call_id.unwrap_or(tool_call.id),
                            name: tool_call.function.name,
                            arguments: tool_call.function.arguments,
                        },
                    })
                    .await?;
            }
            StreamedAssistantContent::ToolCallDelta { .. } => {}
            StreamedAssistantContent::Final(_) => {}
        }
    }
    parser.finish(handler, &fallback_text).await
}

fn apply_optional_preamble<M, P, ToolState>(
    builder: AgentBuilder<M, P, ToolState>,
    settings: &RoleLlmSettings,
) -> AgentBuilder<M, P, ToolState>
where
    M: CompletionModel,
    P: rig_core::agent::PromptHook<M>,
{
    if let Some(preamble) = settings.effective_preamble() {
        builder.preamble(preamble)
    } else {
        builder
    }
}

#[derive(Default)]
struct RuntimeEventStreamParser {
    buffer: String,
    parsed_any: bool,
}

impl RuntimeEventStreamParser {
    #[cfg(test)]
    async fn push_json_values(
        &mut self,
        text: &str,
        handler: &mut dyn ModelEventHandler,
    ) -> Result<bool> {
        let stream = serde_json::Deserializer::from_str(text).into_iter::<Value>();
        let mut emitted = false;
        for value in stream {
            let value = value?;
            let event = stream_event_from_value(value)?;
            self.record_event(&event);
            self.parsed_any = true;
            emitted = true;
            handler.handle(event).await?;
        }
        Ok(emitted)
    }

    async fn push_text(&mut self, text: &str, handler: &mut dyn ModelEventHandler) -> Result<()> {
        self.buffer.push_str(text);
        while let Some(index) = self.buffer.find('\n') {
            let line = self.buffer[..index].trim().to_string();
            self.buffer = self.buffer[index + 1..].to_string();
            if !line.is_empty() {
                self.emit_line(&line, handler).await?;
            }
        }
        Ok(())
    }

    async fn finish(
        &mut self,
        handler: &mut dyn ModelEventHandler,
        fallback_text: &str,
    ) -> Result<()> {
        let line = self.buffer.trim().to_string();
        if !line.is_empty() {
            let _ = self.emit_line(&line, handler).await;
        }
        if !self.parsed_any {
            let value = agent_loop::extract_json_value(fallback_text)?;
            for event in
                agent_loop::response_to_stream_events(agent_loop::parse_react_response(value)?)?
            {
                self.record_event(&event);
                handler.handle(event).await?;
            }
        }
        Ok(())
    }

    fn record_event(&mut self, _event: &ModelStreamEvent) {}

    async fn emit_line(&mut self, line: &str, handler: &mut dyn ModelEventHandler) -> Result<()> {
        let Some(start) = line.find('{').or_else(|| line.find('[')) else {
            return Ok(());
        };
        let json_line = &line[start..];
        let stream = serde_json::Deserializer::from_str(json_line).into_iter::<Value>();
        let mut emitted = false;
        for value in stream {
            let value = match value {
                Ok(value) => value,
                Err(_) if emitted => break,
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to parse streamed runtime event line: {line}")
                    });
                }
            };
            let event = stream_event_from_value(value)?;
            self.record_event(&event);
            self.parsed_any = true;
            emitted = true;
            handler.handle(event).await?;
        }
        Ok(())
    }
}

fn stream_event_from_value(value: Value) -> Result<ModelStreamEvent> {
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .context("streamed runtime event requires type")?;
    match event_type {
        "assistant_message_started" => Ok(ModelStreamEvent::AssistantMessageStarted {
            item_id: stream_item_id(&value)?,
        }),
        "assistant_text_delta" => Ok(ModelStreamEvent::AssistantTextDelta {
            item_id: stream_item_id(&value)?,
            delta: value
                .get("delta")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "assistant_message_completed" => Ok(ModelStreamEvent::AssistantMessageCompleted {
            item_id: stream_item_id(&value)?,
        }),
        "reasoning_summary_delta" => Ok(ModelStreamEvent::ReasoningSummaryDelta {
            item_id: stream_item_id(&value)?,
            delta: value
                .get("delta")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "reasoning_summary_completed" => Ok(ModelStreamEvent::ReasoningSummaryCompleted {
            item_id: stream_item_id(&value)?,
        }),
        "plan_update_completed" => Ok(ModelStreamEvent::PlanUpdateCompleted {
            item_id: stream_item_id(&value)?,
            content: value
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "tool_call_completed" => Ok(ModelStreamEvent::ToolCallCompleted {
            tool_call: serde_json::from_value(
                value
                    .get("tool_call")
                    .or_else(|| value.get("toolCall"))
                    .cloned()
                    .context("tool_call_completed requires tool_call")?,
            )?,
        }),
        "response_completed" => Ok(ModelStreamEvent::ResponseCompleted {
            end_turn: value
                .get("end_turn")
                .or_else(|| value.get("endTurn"))
                .and_then(Value::as_bool)
                .unwrap_or(true),
            raw: value,
        }),
        other => bail!("unsupported streamed runtime event type {other:?}"),
    }
}

fn stream_item_id(value: &Value) -> Result<String> {
    Ok(value
        .get("item_id")
        .or_else(|| value.get("itemId"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("item-{}", Uuid::new_v4())))
}

fn parse_final_output(settings: &RigSettings, text: &str) -> Result<Value> {
    match settings.output_mode {
        OutputMode::ResearchArtifact => {
            let value = extract_json_artifact(text)?;
            let artifact: ResearchArtifact =
                serde_json::from_value(value).context("failed to parse research artifact JSON")?;
            validate_research_artifact(&artifact, &settings.tickers)
                .map_err(|error| anyhow::anyhow!(error))?;
            serde_json::to_value(artifact).context("failed to serialize research artifact")
        }
        OutputMode::JsonArtifact => {
            if agent_loop::assistant_message_needs_follow_up(text) {
                bail!("agent ended with an action note instead of a JSON artifact");
            }
            parse_json_object_artifact(text).or_else(|_| Ok(text_fallback_artifact(settings, text)))
        }
    }
}

fn text_fallback_artifact(settings: &RigSettings, text: &str) -> Value {
    let per_ticker = settings
        .tickers
        .iter()
        .map(|ticker| {
            (
                ticker.clone(),
                json!({
                    "direction": "neutral",
                    "confidence": 0.0,
                    "report": text,
                    "data_gaps": ["model returned non-JSON artifact; raw text preserved"]
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "id": settings.role,
        "role": settings.role,
        "status": "completed",
        "report": text,
        "per_ticker": per_ticker
    })
}

fn parse_json_object_artifact(text: &str) -> Result<Value> {
    let value = extract_json_artifact(text)
        .or_else(|_| extract_artifact_from_event_text(text))
        .or_else(|_| extract_embedded_json_object(text))?;
    if !value.is_object() {
        bail!("JSON artifact must be an object");
    }
    Ok(value)
}

fn extract_artifact_from_event_text(text: &str) -> Result<Value> {
    let mut best = None;
    let stream = serde_json::Deserializer::from_str(text).into_iter::<Value>();
    for value in stream {
        let value = value?;
        if value.get("type").and_then(Value::as_str) != Some("assistant_text_delta") {
            continue;
        }
        let Some(delta) = value.get("delta").and_then(Value::as_str) else {
            continue;
        };
        if let Ok(value) =
            extract_json_artifact(delta).or_else(|_| extract_embedded_json_object(delta))
        {
            if value.is_object() {
                best = Some(value);
            }
        }
    }
    best.context("event text did not contain artifact delta")
}

fn extract_embedded_json_object(text: &str) -> Result<Value> {
    let mut depth = 0i64;
    let mut start = None;
    let mut in_string = false;
    let mut escape = false;
    let mut last = None;
    for (index, ch) in text.char_indices() {
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' => {
                if depth == 0 {
                    start = Some(index);
                }
                depth += 1;
            }
            '}' => {
                depth -= 1;
                if depth == 0 {
                    if let Some(start) = start {
                        last = Some((start, index + ch.len_utf8()));
                    }
                }
            }
            _ => {}
        }
    }
    let Some((start, end)) = last else {
        bail!("model response did not contain embedded JSON object");
    };
    serde_json::from_str(&text[start..end]).context("failed to parse embedded JSON object")
}

fn open_loop_connection(settings: &RigSettings) -> Result<Connection> {
    let db_path = settings
        .tools
        .as_ref()
        .and_then(|tools| tools.db_path.clone())
        .unwrap_or_else(|| default_project_root().join("outputs/agent_loop.sqlite"));
    orchestrator_sql::connect(db_path)
}

fn loop_session_id(settings: &RigSettings) -> String {
    let run_id = loop_run_id(settings);
    format!("{run_id}:{}", settings.role)
}

fn loop_run_id(settings: &RigSettings) -> String {
    settings
        .tools
        .as_ref()
        .and_then(|tools| {
            tools
                .run_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .or_else(|| {
                    tools
                        .run_dir
                        .as_ref()
                        .and_then(|path| path.file_name())
                        .and_then(|name| name.to_str())
                        .map(ToString::to_string)
                })
        })
        .unwrap_or_else(|| "agent-loop".to_string())
}

fn openai_compatible_api_key(settings: &RoleLlmSettings) -> Result<String> {
    if let Some(api_key) = settings
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(api_key.to_string());
    }
    bail!("api_key is required for OpenAI-compatible provider")
}

fn openai_compatible_base_url(settings: &RoleLlmSettings) -> Result<&str> {
    settings
        .base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("base_url is required for OpenAI-compatible provider")
}

fn openai_compatible_responses_client(settings: &RoleLlmSettings) -> Result<openai::Client> {
    let api_key = openai_compatible_api_key(settings)?;
    let base_url = openai_compatible_base_url(settings)?;
    openai::Client::builder()
        .api_key(&api_key)
        .base_url(base_url)
        .build()
        .context("failed to build OpenAI-compatible responses client")
}

pub fn additional_params(settings: &RigSettings) -> Option<Value> {
    let mut params = settings
        .llm
        .effective_reasoning_effort(settings.reasoning_effort_override.as_deref())
        .map(openai_responses_reasoning_params);
    if uses_native_web_search(settings) {
        params = Some(add_openai_responses_native_web_search(params));
    }
    params
}

pub fn openai_responses_reasoning_params(effort: &str) -> Value {
    json!({
        "reasoning": {
            "effort": effort.trim().to_ascii_lowercase()
        }
    })
}

fn add_openai_responses_native_web_search(params: Option<Value>) -> Value {
    let mut object = match params {
        Some(Value::Object(object)) => object,
        Some(other) => {
            let mut object = serde_json::Map::new();
            object.insert("value".to_string(), other);
            object
        }
        None => serde_json::Map::new(),
    };
    let mut tools = match object.remove("tools") {
        Some(Value::Array(tools)) => tools,
        _ => Vec::new(),
    };
    tools.push(json!({"type": "web_search"}));
    object.insert("tools".to_string(), Value::Array(tools));
    Value::Object(object)
}

fn configured_tool_names(settings: &RigSettings) -> Vec<&str> {
    let mut names = Vec::new();
    if settings.llm.think_tool {
        names.push("think");
    }
    names.extend(settings.llm.tools.iter().map(String::as_str));
    if uses_web_run_fallback(settings) {
        names.push(tools::WEB_RUN_TOOL_NAME);
    }
    names
}

fn validate_tool_name(name: &str) -> Result<()> {
    if tools::tool_names().contains(&name) {
        Ok(())
    } else {
        bail!("unknown tool name: {name}")
    }
}

fn validate_reasoning_effort(value: &str) -> Result<()> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" => Ok(()),
        other => bail!("unsupported reasoning_effort {other:?}"),
    }
}

fn default_tool_config() -> tools::ExternalToolConfig {
    tools::ExternalToolConfig {
        project_root: default_project_root(),
        db_path: std::env::var_os("ORCH_DB_PATH").map(PathBuf::from),
        run_dir: None,
        run_id: None,
        tickers: std::env::var("ORCH_TICKERS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
    }
}

pub fn mock_role_artifact(role: &str, tickers: &[String]) -> Value {
    match role {
        "manager.research" => orchestrator_sql::write::mock_research_artifact(tickers),
        _ => {
            let per_ticker = tickers
                .iter()
                .map(|ticker| {
                    (
                        ticker.clone(),
                        serde_json::json!({
                            "direction": "neutral",
                            "confidence": 0.5,
                            "report": format!("Mock report for {ticker} from {role}.")
                        }),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            serde_json::json!({
                "id": role,
                "role": role,
                "report": format!("Mock report from {role}."),
                "per_ticker": per_ticker
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        agent_loop, tools, LlmRoute, LlmTransport, OutputMode, RigSettings, RoleLlmSettings,
    };
    use crate::web_search::{WebSearchConfig, WebSearchMode};
    use crate::{ModelEventHandler, ModelStreamEvent, RuntimeEventStreamParser};
    use anyhow::Result;
    use serde_json::json;
    use std::{future::Future, pin::Pin};

    fn base_settings(route: LlmRoute) -> RigSettings {
        RigSettings {
            role: "manager.research".to_string(),
            phase: None,
            tickers: vec!["TQQQ".to_string()],
            output_mode: OutputMode::ResearchArtifact,
            llm: RoleLlmSettings {
                route,
                model: "gpt-5.4".to_string(),
                preamble: None,
                max_turns: Some(6),
                reasoning_effort: Some("low".to_string()),
                transport: Default::default(),
                base_url: None,
                api_key: None,
                think_tool: true,
                tools: Vec::new(),
                native_web_search: false,
            },
            reasoning_effort_override: None,
            tools: None,
            web_search: WebSearchConfig::default(),
        }
    }

    #[test]
    fn external_tool_names_are_registered() {
        assert_eq!(
            tools::tool_names(),
            vec![
                "read_run_context",
                "fetch_jin10_flash",
                "fetch_youtube_transcript",
                "fetch_wayinvideo_transcript",
                "run_technical_indicators",
                "fetch_last30days_context",
            ]
        );
    }

    #[test]
    fn route_deserializes_supported_values() {
        assert_eq!(
            serde_json::from_value::<LlmRoute>(json!("responses")).unwrap(),
            LlmRoute::Responses
        );
        assert!(serde_json::from_value::<LlmRoute>(json!("chat_completions")).is_err());
        assert!(serde_json::from_value::<LlmRoute>(json!("deepseek")).is_err());
    }

    #[test]
    fn ws_transport_is_allowed_for_responses() {
        let responses = RoleLlmSettings {
            transport: LlmTransport::Ws,
            base_url: Some("https://llm.example.com/v1".to_string()),
            api_key: Some("test-key".to_string()),
            ..base_settings(LlmRoute::Responses).llm
        };
        responses.validate("manager.research").unwrap();
    }

    #[test]
    fn end_context_item_matches_provider_message_shape() {
        let call = agent_loop::ToolCallRequest {
            call_id: "call_file_001".to_string(),
            name: "read_file".to_string(),
            arguments: json!({"path": "README.md"}),
        };
        let tool_result = agent_loop::ToolResultItem {
            call_id: "call_file_001".to_string(),
            name: "read_file".to_string(),
            status: "completed".to_string(),
            output: json!({"content": "# My Project"}),
            error: None,
        };

        assert_eq!(
            super::end_context_item(&agent_loop::TurnItem::user("帮我查看 README.md")).unwrap(),
            json!({"role": "user", "content": "帮我查看 README.md"})
        );
        assert_eq!(
            super::end_context_item(&agent_loop::TurnItem::tool_call(&call)).unwrap(),
            json!({
                "role": "assistant",
                "tool_calls": [{
                    "id": "call_file_001",
                    "type": "function",
                    "function": {
                        "name": "read_file",
                        "arguments": "{\"path\":\"README.md\"}"
                    }
                }]
            })
        );
        assert_eq!(
            super::end_context_item(&agent_loop::TurnItem::tool_result(&tool_result)).unwrap(),
            json!({"role": "tool", "tool_call_id": "call_file_001", "content": "# My Project"})
        );
        assert_eq!(
            super::safe_path_part("analyst.news_macro"),
            "analyst_news_macro"
        );
    }

    #[test]
    fn json_artifact_parser_requires_object() {
        assert_eq!(
            super::parse_json_object_artifact("{\"ok\":true}").unwrap(),
            json!({"ok": true})
        );
        let err = super::parse_json_object_artifact("[{\"ok\":true}]").unwrap_err();
        assert!(err.to_string().contains("must be an object"));
    }

    #[test]
    fn json_artifact_parser_reads_artifact_from_event_delta() {
        let text = "{\"type\":\"assistant_message_started\",\"item_id\":\"msg-1\"}\n{\"type\":\"assistant_text_delta\",\"item_id\":\"msg-1\",\"delta\":\"{\\\"id\\\":\\\"a\\\",\\\"status\\\":\\\"completed\\\"}\"}\n{\"type\":\"assistant_message_completed\",\"item_id\":\"msg-1\"}";

        assert_eq!(
            super::parse_json_object_artifact(text).unwrap(),
            json!({"id": "a", "status": "completed"})
        );
    }

    #[derive(Default)]
    struct CollectEvents {
        events: Vec<ModelStreamEvent>,
    }

    impl ModelEventHandler for CollectEvents {
        fn handle<'a>(
            &'a mut self,
            event: ModelStreamEvent,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
            self.events.push(event);
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn runtime_event_parser_accepts_adjacent_json_objects() {
        let mut parser = RuntimeEventStreamParser::default();
        let mut handler = CollectEvents::default();

        parser
            .push_text(
                "{\"type\":\"response_completed\",\"end_turn\":false}{\"type\":\"assistant_message_started\",\"item_id\":\"msg-2\"}\n",
                &mut handler,
            )
            .await
            .unwrap();

        assert_eq!(handler.events.len(), 2);
        assert!(matches!(
            handler.events[0],
            agent_loop::ModelStreamEvent::ResponseCompleted {
                end_turn: false,
                ..
            }
        ));
        assert!(matches!(
            handler.events[1],
            agent_loop::ModelStreamEvent::AssistantMessageStarted { .. }
        ));
    }

    #[tokio::test]
    async fn runtime_event_parser_accepts_pretty_json_object_stream() {
        let mut parser = RuntimeEventStreamParser::default();
        let mut handler = CollectEvents::default();

        let emitted = parser
            .push_json_values(
                "{\n  \"type\": \"assistant_message_started\",\n  \"item_id\": \"msg-1\"\n}\n{\n  \"type\": \"response_completed\",\n  \"end_turn\": false\n}",
                &mut handler,
            )
            .await
            .unwrap();

        assert!(emitted);
        assert_eq!(handler.events.len(), 2);
        assert!(matches!(
            handler.events[0],
            agent_loop::ModelStreamEvent::AssistantMessageStarted { .. }
        ));
        assert!(matches!(
            handler.events[1],
            agent_loop::ModelStreamEvent::ResponseCompleted {
                end_turn: false,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn runtime_event_parser_ignores_non_json_lines() {
        let mut parser = RuntimeEventStreamParser::default();
        let mut handler = CollectEvents::default();

        parser
            .push_text(
                "reasoning text before events\n{\"type\":\"response_completed\",\"end_turn\":true}\n",
                &mut handler,
            )
            .await
            .unwrap();

        assert_eq!(handler.events.len(), 1);
        assert!(matches!(
            handler.events[0],
            agent_loop::ModelStreamEvent::ResponseCompleted { .. }
        ));
    }

    #[tokio::test]
    async fn runtime_event_parser_accepts_embedded_json_event() {
        let mut parser = RuntimeEventStreamParser::default();
        let mut handler = CollectEvents::default();

        parser
            .push_text(
                "event shape: {\"type\":\"response_completed\",\"end_turn\":false}_completed.\n",
                &mut handler,
            )
            .await
            .unwrap();

        assert_eq!(handler.events.len(), 1);
        assert!(matches!(
            handler.events[0],
            agent_loop::ModelStreamEvent::ResponseCompleted {
                end_turn: false,
                ..
            }
        ));
    }

    #[test]
    fn role_llm_settings_rejects_unknown_tools() {
        let settings = RoleLlmSettings {
            route: LlmRoute::Responses,
            model: "gpt-5.4".to_string(),
            preamble: None,
            max_turns: Some(4),
            reasoning_effort: None,
            transport: Default::default(),
            base_url: Some("https://llm.example.com/v1".to_string()),
            api_key: Some("test-key".to_string()),
            think_tool: false,
            tools: vec!["missing_tool".to_string()],
            native_web_search: false,
        };
        let err = settings.validate("analyst.technical").unwrap_err();
        assert!(err.to_string().contains("unknown tool name"));
    }

    #[test]
    fn role_llm_settings_allows_missing_or_blank_preamble() {
        let value = json!({
            "route": "responses",
            "model": "gpt-5.4",
            "base_url": "https://llm.example.com/v1",
            "api_key": "test-key",
            "max_turns": 4,
            "tools": []
        });
        let settings: RoleLlmSettings = serde_json::from_value(value).unwrap();
        assert!(settings.effective_preamble().is_none());
        settings.validate("manager.research").unwrap();

        let settings = RoleLlmSettings {
            preamble: Some("   ".to_string()),
            ..settings
        };
        assert!(settings.effective_preamble().is_none());
        settings.validate("manager.research").unwrap();
    }

    #[test]
    fn openai_compatible_requires_base_url_and_api_key_for_responses() {
        let settings = RoleLlmSettings {
            route: LlmRoute::Responses,
            model: "third-party-model".to_string(),
            preamble: None,
            max_turns: Some(4),
            reasoning_effort: None,
            transport: Default::default(),
            base_url: Some("https://llm.example.com/v1".to_string()),
            api_key: Some("test-key".to_string()),
            think_tool: false,
            tools: Vec::new(),
            native_web_search: false,
        };
        settings.validate("manager.research").unwrap();

        let settings = RoleLlmSettings {
            base_url: None,
            ..settings
        };
        assert!(settings
            .validate("manager.research")
            .unwrap_err()
            .to_string()
            .contains("requires base_url"));

        let settings = RoleLlmSettings {
            base_url: Some("https://llm.example.com/v1".to_string()),
            api_key: None,
            ..settings
        };
        assert!(settings
            .validate("manager.research")
            .unwrap_err()
            .to_string()
            .contains("requires api_key"));

        let settings = RoleLlmSettings {
            api_key: Some("config-key".to_string()),
            ..settings
        };
        settings.validate("manager.research").unwrap();
    }

    #[test]
    fn responses_gets_reasoning_additional_params() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({"reasoning": {"effort": "low"}}))
        );

        settings.reasoning_effort_override = Some("HIGH".to_string());
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({"reasoning": {"effort": "high"}}))
        );
    }

    #[test]
    fn native_web_search_adds_provider_tool_to_additional_params() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.native_web_search = true;
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({"reasoning": {"effort": "low"}}))
        );

        settings.web_search.mode = WebSearchMode::Live;
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({
                "reasoning": {"effort": "low"},
                "tools": [{"type": "web_search"}]
            }))
        );

        settings.llm.reasoning_effort = None;
        settings.reasoning_effort_override = None;
        assert_eq!(
            super::additional_params(&settings),
            Some(json!({"tools": [{"type": "web_search"}]}))
        );
    }

    #[test]
    fn openai_compatible_responses_uses_responses_reasoning_and_native_web_search() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.native_web_search = true;
        settings.web_search.mode = WebSearchMode::Live;

        assert_eq!(
            super::additional_params(&settings),
            Some(json!({
                "reasoning": {"effort": "low"},
                "tools": [{"type": "web_search"}]
            }))
        );
        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));
    }

    #[test]
    fn think_tool_registration_is_role_controlled() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.tools = vec!["run_technical_indicators".to_string()];
        assert_eq!(
            super::configured_tool_names(&settings),
            vec!["think", "run_technical_indicators"]
        );

        settings.llm.think_tool = false;
        assert_eq!(
            super::configured_tool_names(&settings),
            vec!["run_technical_indicators"]
        );
    }

    #[test]
    fn web_run_tool_registration_follows_web_search_mode() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.think_tool = false;

        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));

        settings.web_search.mode = WebSearchMode::Cached;
        assert!(super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));

        settings.web_search.mode = WebSearchMode::Live;
        assert!(super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));
    }

    #[test]
    fn native_web_search_suppresses_web_run_fallback_tool() {
        let mut settings = base_settings(LlmRoute::Responses);
        settings.llm.base_url = Some("https://llm.example.com/v1".to_string());
        settings.llm.api_key = Some("test-key".to_string());
        settings.llm.think_tool = false;
        settings.llm.native_web_search = true;
        settings.web_search.mode = WebSearchMode::Live;

        assert!(!super::configured_tool_names(&settings).contains(&tools::WEB_RUN_TOOL_NAME));
        assert!(super::web_run_runtime_for_settings(&settings).is_none());
    }
}
