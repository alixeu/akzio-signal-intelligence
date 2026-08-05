//! Stand-alone provider capability checks.
//!
//! This path intentionally lives beside the workflow entry point.  It loads
//! the same resolved role configuration as a normal run, but does not create a
//! FileStore, read market inputs, or construct any workflow tool runtime.

use anyhow::{anyhow, bail, Context, Result};
use async_openai::{
    config::OpenAIConfig,
    traits::EventType,
    types::{chat as chat_types, responses as response_types},
    Client,
};
use chrono::Utc;
use futures::StreamExt;
use orchestrator_llm::{tools, LlmRoute};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use crate::orchestration::config::RuntimeConfig;

const ASYNC_OPENAI_VERSION: &str = "0.41.1";
const TARGET_TIMEOUT: Duration = Duration::from_secs(60);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(600);
const PROBE_FUNCTION: &str = "think";

#[derive(Clone, Debug)]
struct ProviderContractTarget {
    role: String,
    base_url: String,
    api_key: String,
    free_opencode: bool,
    model: String,
    route: LlmRoute,
    reasoning: bool,
    function_calling: bool,
    native_web_search: bool,
    json_object: bool,
    streaming: bool,
    max_completion_tokens: Option<u32>,
}

#[derive(Debug, Serialize)]
struct TargetReport {
    role: String,
    provider: &'static str,
    base_url_fingerprint: String,
    model: String,
    route: LlmRoute,
    reasoning: bool,
    function_calling: bool,
    native_web_search: bool,
    json_object: bool,
    streaming: bool,
    status: &'static str,
    events: Vec<String>,
    failure: Option<String>,
}

#[derive(Debug)]
struct StreamCapture {
    events: Vec<String>,
    terminal: Option<ResponseTerminal>,
    function_call: Option<response_types::FunctionToolCall>,
    source_urls: BTreeSet<String>,
    citation_urls: BTreeSet<String>,
}

#[derive(Debug)]
enum ResponseTerminal {
    Completed(response_types::Response),
    Failed(response_types::Response),
    Incomplete(response_types::Response),
}

#[derive(Debug)]
struct ChatCapture {
    events: Vec<String>,
    finish_reason: Option<chat_types::FinishReason>,
    tool_calls: BTreeMap<u32, ChatToolCall>,
}

#[derive(Debug, Default)]
struct ChatToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

pub async fn run(runtime: &RuntimeConfig) -> Result<Value> {
    tokio::time::timeout(TOTAL_TIMEOUT, run_with_budget(runtime))
        .await
        .map_err(|_| anyhow!("provider contract total budget exceeded"))?
}

async fn run_with_budget(runtime: &RuntimeConfig) -> Result<Value> {
    let started_at = Utc::now().to_rfc3339();
    let targets = targets(runtime);
    let mut reports = Vec::with_capacity(targets.len());

    for target in targets {
        let report = match tokio::time::timeout(TARGET_TIMEOUT, check_target(&target)).await {
            Ok(report) => report,
            Err(_) => target_report(&target, "failed", Vec::new(), Some("timeout".to_owned())),
        };
        reports.push(report);
    }

    let ok = reports
        .iter()
        .all(|report| matches!(report.status, "passed" | "skipped_non_standard"));
    Ok(json!({
        "schema_version": 1,
        "command": "provider_contract",
        "ok": ok,
        "started_at": started_at,
        "finished_at": Utc::now().to_rfc3339(),
        "async_openai_version": ASYNC_OPENAI_VERSION,
        "targets": reports,
    }))
}

fn targets(runtime: &RuntimeConfig) -> Vec<ProviderContractTarget> {
    let mut targets = Vec::new();
    for (role, settings) in &runtime.llm_roles {
        let function_calling = runtime
            .role_profile_registry
            .registrations()
            .filter(|registration| registration.role_id == *role)
            .any(|registration| !registration.tool_allowlist.is_empty());
        let target = ProviderContractTarget {
            role: role.clone(),
            base_url: settings.base_url.clone().unwrap_or_default(),
            api_key: settings.api_key.clone().unwrap_or_default(),
            free_opencode: settings.free_opencode,
            model: settings.effective_model().to_owned(),
            route: settings.effective_route(),
            reasoning: settings.reasoning_effort.is_some(),
            function_calling,
            native_web_search: settings.native_web_search,
            json_object: *role == "compressor.phase_summary",
            streaming: true,
            max_completion_tokens: settings.max_completion_tokens,
        };
        if !targets
            .iter()
            .any(|existing| same_target(existing, &target))
        {
            targets.push(target);
        }
    }
    targets
}

fn same_target(left: &ProviderContractTarget, right: &ProviderContractTarget) -> bool {
    left.base_url == right.base_url
        && left.api_key == right.api_key
        && left.free_opencode == right.free_opencode
        && left.model == right.model
        && left.route == right.route
        && left.reasoning == right.reasoning
        && left.function_calling == right.function_calling
        && left.native_web_search == right.native_web_search
        && left.json_object == right.json_object
        && left.streaming == right.streaming
}

async fn check_target(target: &ProviderContractTarget) -> TargetReport {
    if target.free_opencode {
        return target_report(
            target,
            "skipped_non_standard",
            Vec::new(),
            Some("free_opencode uses a separate non-standard provider path".to_owned()),
        );
    }

    let client = match client_for(target) {
        Ok(client) => client,
        Err(error) => {
            return target_report(
                target,
                "failed",
                Vec::new(),
                Some(sanitize_error(target, &error)),
            )
        }
    };

    let checked = match target.route {
        LlmRoute::Responses => check_responses(target, &client).await,
        LlmRoute::ChatCompletions => check_chat(target, &client).await,
    };
    match checked {
        Ok(events) => target_report(target, "passed", events, None),
        Err(error) => target_report(
            target,
            "failed",
            Vec::new(),
            Some(sanitize_error(target, &error)),
        ),
    }
}

fn client_for(target: &ProviderContractTarget) -> Result<Client<OpenAIConfig>> {
    if target.base_url.trim().is_empty() {
        bail!("provider base_url is empty")
    }
    if target.api_key.trim().is_empty() {
        bail!("provider api_key is empty")
    }
    let config = OpenAIConfig::new()
        .with_api_key(target.api_key.clone())
        .with_api_base(target.base_url.clone());
    Ok(Client::with_config(config))
}

async fn check_responses(
    target: &ProviderContractTarget,
    client: &Client<OpenAIConfig>,
) -> Result<Vec<String>> {
    let mut events = Vec::new();
    let basic = capture_responses(client, response_request(target, false, false, false)?).await?;
    events.extend(basic.events.clone());
    require_completed(&basic, "streaming")?;

    if target.reasoning {
        let reasoning =
            capture_responses(client, response_request(target, true, false, false)?).await?;
        events.extend(reasoning.events.clone());
        require_completed(&reasoning, "reasoning")?;
    }
    if target.function_calling {
        let function =
            capture_responses(client, response_request(target, false, true, false)?).await?;
        events.extend(function.events.clone());
        let call = function.function_call.clone().context(
            "protocol_violation: response.output_item.done did not contain a function call",
        )?;
        validate_function_call(&call)?;
        let follow_up = capture_responses(client, function_output_request(target, &call)?).await?;
        events.extend(follow_up.events.clone());
        require_completed(&follow_up, "function_call_output")?;
    }
    if target.native_web_search {
        let web = capture_responses(client, response_request(target, false, false, true)?).await?;
        events.extend(web.events.clone());
        require_completed(&web, "native_web_search")?;
        let intersection = web.source_urls.intersection(&web.citation_urls).next();
        if intersection.is_none() {
            bail!("protocol_violation: web search Sources and URL Citation had no common URL")
        }
    }
    if target.json_object {
        let json_response =
            capture_responses(client, response_request(target, false, false, false)?).await?;
        events.extend(json_response.events.clone());
        require_completed(&json_response, "json_object")?;
        let response = completed_response(&json_response)?;
        let text = response
            .output_text()
            .context("model_output_rejected: json_object response had no output text")?;
        if serde_json::from_str::<Value>(&text).is_err() {
            bail!("model_output_rejected: json_object output was not valid JSON")
        }
    }
    Ok(events)
}

fn response_request(
    target: &ProviderContractTarget,
    reasoning: bool,
    function_calling: bool,
    web_search: bool,
) -> Result<response_types::CreateResponse> {
    use response_types::*;
    let prompt = if web_search {
        "Search for the official OpenAI Responses API documentation and cite the source URL."
    } else if function_calling {
        "Call the think function with a short note, then stop."
    } else if target.json_object {
        "Return exactly a JSON object with the key contract and value passed."
    } else {
        "Reply with the single word contract-ok."
    };
    let mut request = CreateResponseArgs::default()
        .model(target.model.clone())
        .input(InputParam::Text(prompt.to_owned()))
        .instructions("Provider contract check. Do not include secrets or extra text.")
        .store(false)
        .stream(true)
        .max_output_tokens(target.max_completion_tokens.unwrap_or(256).min(512))
        .build()?;
    if reasoning {
        request.reasoning = Some(Reasoning {
            effort: Some(response_types::ReasoningEffort::Low),
            summary: None,
        });
    }
    if function_calling {
        let names = vec![PROBE_FUNCTION.to_owned()];
        let mut definitions = tools::responses_tool_definitions(&names);
        if definitions.is_empty() {
            bail!("provider contract function fixture is not registered")
        }
        request.tools = Some(std::mem::take(&mut definitions));
        request.tool_choice = Some(ToolChoiceParam::Function(ToolChoiceFunction {
            name: PROBE_FUNCTION.to_owned(),
        }));
    }
    if web_search {
        request.include = Some(vec![IncludeEnum::WebSearchCallActionSources]);
        request.tools = Some(vec![Tool::WebSearch(WebSearchTool::default())]);
    }
    if target.json_object {
        request.text = Some(ResponseTextParam {
            format: TextResponseFormatConfiguration::JsonObject,
            verbosity: None,
        });
    }
    Ok(request)
}

fn function_output_request(
    target: &ProviderContractTarget,
    call: &response_types::FunctionToolCall,
) -> Result<response_types::CreateResponse> {
    use response_types::*;
    Ok(CreateResponseArgs::default()
        .model(target.model.clone())
        .input(InputParam::Items(vec![
            InputItem::Item(Item::FunctionCall(call.clone())),
            InputItem::Item(Item::FunctionCallOutput(FunctionCallOutputItemParam {
                call_id: call.call_id.clone(),
                output: FunctionCallOutput::Text("{\"status\":\"completed\"}".to_owned()),
                id: None,
                status: Some(OutputStatus::Completed),
            })),
        ]))
        .instructions("Acknowledge the completed contract probe with one word.")
        .store(false)
        .stream(true)
        .max_output_tokens(64u32)
        .build()?)
}

async fn capture_responses(
    client: &Client<OpenAIConfig>,
    request: response_types::CreateResponse,
) -> Result<StreamCapture> {
    let mut stream = client.responses().create_stream(request).await?;
    let mut capture = StreamCapture {
        events: Vec::new(),
        terminal: None,
        function_call: None,
        source_urls: BTreeSet::new(),
        citation_urls: BTreeSet::new(),
    };
    let mut last_sequence = None;
    let mut saw_created = false;
    while let Some(item) = stream.next().await {
        let event = item.map_err(|error| {
            anyhow!("protocol_violation: typed Responses SSE event could not be decoded: {error}")
        })?;
        let raw = serde_json::to_value(&event)?;
        let sequence = raw.get("sequence_number").and_then(Value::as_u64);
        if let (Some(previous), Some(current)) = (last_sequence, sequence) {
            if current < previous {
                bail!("protocol_violation: Responses sequence_number moved backwards")
            }
        }
        last_sequence = sequence.or(last_sequence);
        if capture.terminal.is_some() {
            bail!("protocol_violation: Responses event arrived after terminal event")
        }
        if !saw_created {
            if !matches!(
                &event,
                response_types::ResponseStreamEvent::ResponseCreated(_)
            ) {
                bail!("protocol_violation: Responses stream did not begin with response.created")
            }
            saw_created = true;
        } else if matches!(
            &event,
            response_types::ResponseStreamEvent::ResponseCreated(_)
        ) {
            bail!("protocol_violation: Responses stream emitted duplicate response.created")
        }
        capture.events.push(event.event_type().to_owned());
        match event {
            response_types::ResponseStreamEvent::ResponseOutputItemDone(done) => {
                inspect_output_item(&mut capture, done.item)?;
            }
            response_types::ResponseStreamEvent::ResponseCompleted(done) => {
                capture.terminal = Some(ResponseTerminal::Completed(done.response));
            }
            response_types::ResponseStreamEvent::ResponseFailed(done) => {
                capture.terminal = Some(ResponseTerminal::Failed(done.response));
            }
            response_types::ResponseStreamEvent::ResponseIncomplete(done) => {
                capture.terminal = Some(ResponseTerminal::Incomplete(done.response));
            }
            _ => {}
        }
    }
    if capture.terminal.is_none() {
        bail!("protocol_violation: Responses stream ended without a terminal event")
    }
    Ok(capture)
}

fn inspect_output_item(
    capture: &mut StreamCapture,
    item: response_types::OutputItem,
) -> Result<()> {
    match item {
        response_types::OutputItem::FunctionCall(call) => {
            validate_function_call(&call)?;
            capture.function_call = Some(call);
        }
        response_types::OutputItem::WebSearchCall(call) => {
            if let Some(response_types::WebSearchToolCallAction::Search(search)) = call.action {
                for source in search.sources.unwrap_or_default() {
                    capture.source_urls.insert(normalize_url(&source.url)?);
                }
            }
        }
        response_types::OutputItem::Message(message) => {
            let raw = serde_json::to_value(message)?;
            collect_citations(&raw, &mut capture.citation_urls);
        }
        _ => {}
    }
    Ok(())
}

fn validate_function_call(call: &response_types::FunctionToolCall) -> Result<()> {
    if call.id.as_deref().is_none_or(str::is_empty)
        || call.call_id.trim().is_empty()
        || call.name.trim().is_empty()
    {
        bail!("protocol_violation: FunctionToolCall requires id, call_id, and name")
    }
    if call.status != Some(response_types::OutputStatus::Completed) {
        bail!("protocol_violation: FunctionToolCall status was not completed")
    }
    let arguments: Value = serde_json::from_str(&call.arguments)
        .context("protocol_violation: FunctionToolCall arguments were not JSON")?;
    if !arguments.is_object() {
        bail!("protocol_violation: FunctionToolCall arguments were not a JSON object")
    }
    Ok(())
}

fn require_completed(capture: &StreamCapture, capability: &str) -> Result<()> {
    match capture.terminal.as_ref() {
        Some(ResponseTerminal::Completed(response))
            if matches!(response.status, response_types::Status::Completed) =>
        {
            Ok(())
        }
        Some(ResponseTerminal::Failed(response)) => bail!(
            "response_failed ({capability}): {}",
            response
                .error
                .as_ref()
                .map(|error| format!("{}: {}", error.code, error.message))
                .unwrap_or_else(|| "provider returned response.failed".to_owned())
        ),
        Some(ResponseTerminal::Incomplete(response)) => bail!(
            "response_incomplete ({capability}): {}",
            response
                .incomplete_details
                .as_ref()
                .map(|details| details.reason.clone())
                .unwrap_or_else(|| "provider returned response.incomplete".to_owned())
        ),
        Some(ResponseTerminal::Completed(response)) => bail!(
            "protocol_violation: response.completed carried status {:?}",
            response.status
        ),
        None => bail!("protocol_violation: response had no terminal state"),
    }
}

fn completed_response(capture: &StreamCapture) -> Result<&response_types::Response> {
    match capture.terminal.as_ref() {
        Some(ResponseTerminal::Completed(response)) => Ok(response),
        _ => bail!("model_output_rejected: response did not complete"),
    }
}

async fn check_chat(
    target: &ProviderContractTarget,
    client: &Client<OpenAIConfig>,
) -> Result<Vec<String>> {
    let mut events = Vec::new();
    let basic = capture_chat(client, chat_request(target, false, false)?).await?;
    events.extend(basic.events.clone());
    if basic.finish_reason.is_none() {
        bail!("protocol_violation: Chat Completions stream had no finish_reason")
    }
    if target.function_calling {
        let function = capture_chat(client, chat_request(target, true, false)?).await?;
        events.extend(function.events.clone());
        let call = function
            .tool_calls
            .values()
            .next()
            .context("protocol_violation: Chat Completions returned no tool call")?;
        let id = call
            .id
            .as_deref()
            .context("protocol_violation: Chat tool call had no id")?;
        let name = call
            .name
            .as_deref()
            .context("protocol_violation: Chat tool call had no name")?;
        serde_json::from_str::<Value>(&call.arguments)
            .context("protocol_violation: Chat tool arguments were not JSON")?;
        let follow_up = capture_chat(
            client,
            chat_follow_up_request(target, id, name, &call.arguments)?,
        )
        .await?;
        events.extend(follow_up.events);
        if follow_up.finish_reason.is_none() {
            bail!("protocol_violation: Chat tool output response had no finish_reason")
        }
    }
    if target.json_object {
        let json_response = capture_chat(client, chat_request(target, false, true)?).await?;
        events.extend(json_response.events);
    }
    Ok(events)
}

fn chat_request(
    target: &ProviderContractTarget,
    function_calling: bool,
    json_object: bool,
) -> Result<chat_types::CreateChatCompletionRequest> {
    use chat_types::*;
    let message = ChatCompletionRequestMessage::User(
        ChatCompletionRequestUserMessageArgs::default()
            .content(ChatCompletionRequestUserMessageContent::Text(
                if json_object {
                    "Return exactly a JSON object with the key contract and value passed."
                        .to_owned()
                } else if function_calling {
                    "Call the think function with a short note, then stop.".to_owned()
                } else {
                    "Reply with the single word contract-ok.".to_owned()
                },
            ))
            .build()?,
    );
    let mut request = CreateChatCompletionRequestArgs::default()
        .model(target.model.clone())
        .messages(vec![message])
        .stream(true)
        .max_completion_tokens(target.max_completion_tokens.unwrap_or(256).min(512))
        .build()?;
    if let Some(effort) = target.reasoning.then_some(ReasoningEffort::Low) {
        request.reasoning_effort = Some(effort);
    }
    if function_calling {
        request.tools = Some(tools::chat_completions_tool_definitions(&[
            PROBE_FUNCTION.to_owned()
        ]));
        request.tool_choice = Some(ChatCompletionToolChoiceOption::Function(
            ChatCompletionNamedToolChoice {
                function: FunctionName {
                    name: PROBE_FUNCTION.to_owned(),
                },
            },
        ));
    }
    if json_object {
        request.response_format = Some(ResponseFormat::JsonObject);
    }
    Ok(request)
}

fn chat_follow_up_request(
    target: &ProviderContractTarget,
    id: &str,
    name: &str,
    arguments: &str,
) -> Result<chat_types::CreateChatCompletionRequest> {
    use chat_types::*;
    #[allow(deprecated)]
    let assistant =
        ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
            content: None,
            refusal: None,
            name: None,
            audio: None,
            tool_calls: Some(vec![ChatCompletionMessageToolCalls::Function(
                ChatCompletionMessageToolCall {
                    id: id.to_owned(),
                    function: FunctionCall {
                        name: name.to_owned(),
                        arguments: arguments.to_owned(),
                    },
                },
            )]),
            function_call: None,
        });
    let tool = ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
        content: ChatCompletionRequestToolMessageContent::Text(
            "{\"status\":\"completed\"}".to_owned(),
        ),
        tool_call_id: id.to_owned(),
    });
    Ok(CreateChatCompletionRequestArgs::default()
        .model(target.model.clone())
        .messages(vec![assistant, tool])
        .stream(true)
        .max_completion_tokens(64u32)
        .build()?)
}

async fn capture_chat(
    client: &Client<OpenAIConfig>,
    request: chat_types::CreateChatCompletionRequest,
) -> Result<ChatCapture> {
    let mut stream = client.chat().create_stream(request).await?;
    let mut capture = ChatCapture {
        events: Vec::new(),
        finish_reason: None,
        tool_calls: BTreeMap::new(),
    };
    while let Some(item) = stream.next().await {
        let chunk = item.map_err(|error| {
            anyhow!(
                "protocol_violation: typed Chat Completions SSE chunk could not be decoded: {error}"
            )
        })?;
        capture.events.push("chat.completion.chunk".to_owned());
        for choice in chunk.choices {
            if let Some(reason) = choice.finish_reason {
                capture.finish_reason = Some(reason);
            }
            for call in choice.delta.tool_calls.unwrap_or_default() {
                let pending = capture.tool_calls.entry(call.index).or_default();
                if let Some(id) = call.id {
                    pending.id = Some(id);
                }
                if let Some(function) = call.function {
                    if let Some(name) = function.name {
                        pending.name = Some(name);
                    }
                    if let Some(arguments) = function.arguments {
                        pending.arguments.push_str(&arguments);
                    }
                }
            }
        }
    }
    Ok(capture)
}

fn collect_citations(value: &Value, urls: &mut BTreeSet<String>) {
    match value {
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_citations(value, urls)),
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some("url_citation") {
                if let Some(url) = object.get("url").and_then(Value::as_str) {
                    if let Ok(url) = normalize_url(url) {
                        urls.insert(url);
                    }
                }
            }
            object
                .values()
                .for_each(|value| collect_citations(value, urls));
        }
        _ => {}
    }
}

fn normalize_url(value: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(value.trim())
        .with_context(|| format!("protocol_violation: invalid source URL type {:?}", value))?;
    url.set_fragment(None);
    let retained = url
        .query_pairs()
        .filter(|(key, _)| {
            let lower = key.to_ascii_lowercase();
            !lower.starts_with("utm_") && !matches!(lower.as_str(), "gclid" | "fbclid")
        })
        .map(|(key, value)| format!("{}={}", key, value))
        .collect::<Vec<_>>();
    if retained.is_empty() {
        url.set_query(None);
    } else {
        url.set_query(Some(&retained.join("&")));
    }
    if url.path() != "/" {
        let path = url.path().trim_end_matches('/').to_owned();
        url.set_path(if path.is_empty() { "/" } else { &path });
    }
    Ok(url.to_string())
}

fn target_report(
    target: &ProviderContractTarget,
    status: &'static str,
    events: Vec<String>,
    failure: Option<String>,
) -> TargetReport {
    TargetReport {
        role: target.role.clone(),
        provider: if target.free_opencode {
            "free_opencode"
        } else {
            "openai_compatible"
        },
        base_url_fingerprint: fingerprint(&target.base_url),
        model: target.model.clone(),
        route: target.route,
        reasoning: target.reasoning,
        function_calling: target.function_calling,
        native_web_search: target.native_web_search,
        json_object: target.json_object,
        streaming: target.streaming,
        status,
        events,
        failure,
    }
}

fn fingerprint(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("sha256:{:x}", hasher.finalize())[..23].to_owned()
}

fn sanitize_error(target: &ProviderContractTarget, error: &anyhow::Error) -> String {
    let mut text = format!("{error:#}");
    if !target.base_url.is_empty() {
        text = text.replace(&target.base_url, "<gateway>");
    }
    if !target.api_key.is_empty() {
        text = text.replace(&target.api_key, "<redacted>");
    }
    text.truncate(512);
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_sse_fixture_is_decoded_by_sdk_types() {
        let fixture = [
            json!({"type":"response.created","sequence_number":0,"response":{"id":"resp_1","object":"response","created_at":1,"status":"in_progress","model":"fixture","output":[]}}),
            json!({"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"think","arguments":"{}","status":"completed"}}),
            json!({"type":"response.completed","sequence_number":2,"response":{"id":"resp_1","object":"response","created_at":1,"completed_at":2,"status":"completed","model":"fixture","output":[]}}),
        ];
        let events = fixture
            .into_iter()
            .map(serde_json::from_value::<response_types::ResponseStreamEvent>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(events[0].event_type(), "response.created");
        let mut capture = StreamCapture {
            events: events
                .iter()
                .map(|event| event.event_type().to_owned())
                .collect(),
            terminal: None,
            function_call: None,
            source_urls: BTreeSet::new(),
            citation_urls: BTreeSet::new(),
        };
        for event in events {
            if let response_types::ResponseStreamEvent::ResponseOutputItemDone(done) = event {
                inspect_output_item(&mut capture, done.item).unwrap();
            }
        }
        let call = capture.function_call.unwrap();
        validate_function_call(&call).unwrap();
    }

    #[test]
    fn targets_are_deduplicated_by_capabilities_not_role_name() {
        let first = ProviderContractTarget {
            role: "a".to_owned(),
            base_url: "https://gateway/v1".to_owned(),
            api_key: "secret".to_owned(),
            free_opencode: false,
            model: "model".to_owned(),
            route: LlmRoute::Responses,
            reasoning: true,
            function_calling: true,
            native_web_search: false,
            json_object: false,
            streaming: true,
            max_completion_tokens: None,
        };
        let mut second = first.clone();
        second.role = "b".to_owned();
        assert!(same_target(&first, &second));
    }

    #[test]
    fn report_never_contains_raw_gateway_or_key() {
        let target = ProviderContractTarget {
            role: "role".to_owned(),
            base_url: "https://gateway.example/v1".to_owned(),
            api_key: "secret-key".to_owned(),
            free_opencode: false,
            model: "model".to_owned(),
            route: LlmRoute::Responses,
            reasoning: false,
            function_calling: false,
            native_web_search: false,
            json_object: false,
            streaming: true,
            max_completion_tokens: None,
        };
        let value = serde_json::to_string(&target_report(
            &target,
            "failed",
            Vec::new(),
            Some(sanitize_error(
                &target,
                &anyhow!("bad secret-key https://gateway.example/v1"),
            )),
        ))
        .unwrap();
        assert!(!value.contains("secret-key"));
        assert!(!value.contains("https://gateway.example/v1"));
        assert!(value.contains("sha256:"));
    }
}
