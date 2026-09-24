// 这些类型是 Rust-owned Agent/Model seam 的可持久化形状。Request 携带 Contract、
// Manifest、ReadGrant/Materialization identity 和当前阶段；Turn 只描述模型观测，
// 真正的 Artifact 提交、StageAcceptance 与业务完成仍在 AgentRuntime 中完成。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

pub const TERMINAL_SUBMISSION_TOOL: &str = "submit_result";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTurnPhase {
    Draft,
    Submit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTerminalDefinition {
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTerminalSubmission {
    pub call_id: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelRequest {
    pub contract_hash: akzio_domain::ContentHash,
    pub purpose: String,
    pub phase: AgentTurnPhase,
    pub prompt: String,
    pub objective: String,
    pub manifest_artifact_id: ArtifactId,
    #[serde(default)]
    pub read_grant_identity: Option<akzio_domain::ContentHash>,
    #[serde(default)]
    pub context_materialization_identity: Option<akzio_domain::ContentHash>,
    pub context: Vec<Value>,
    pub continuation: Option<ModelContinuation>,
    pub tool_outputs: Vec<ModelToolOutput>,
    pub continuation_instruction: Option<String>,
    pub max_output_tokens: u32,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    pub tools: Vec<AgentToolDefinition>,
    pub terminal: Option<AgentTerminalDefinition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTurnTelemetry {
    #[serde(default)]
    pub provider_request_id: Option<String>,
    #[serde(default)]
    pub response_id: Option<String>,
    #[serde(default)]
    pub requested_model: Option<String>,
    #[serde(default)]
    pub actual_model: Option<String>,
    pub latency_millis: u64,
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub cached_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub reasoning_tokens: Option<u64>,
}

#[derive(Debug, Clone)]
struct AgentTurnRuntimeSnapshot {
    resolved_budget: TaskBudget,
    budget_usage: Value,
    capability: ModelCapabilitySnapshot,
    capability_hash: akzio_domain::ContentHash,
    budget_policy: ModelBudgetPolicy,
    budget_policy_hash: akzio_domain::ContentHash,
    tool_set_hash: akzio_domain::ContentHash,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelTurn {
    pub assistant_text: Option<String>,
    pub tool_calls: Vec<AgentToolCall>,
    pub terminal_submission: Option<AgentTerminalSubmission>,
    pub continuation: ModelContinuation,
    pub telemetry: Option<AgentTurnTelemetry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_debug: Option<ModelCallTrace>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::enum_variant_names)]
pub enum AgentReasoningEvent {
    ReasoningStart {
        run_id: RunId,
        task_id: TaskId,
        attempt_id: AttemptId,
        purpose: String,
        turn: u16,
    },
    ReasoningDelta {
        run_id: RunId,
        task_id: TaskId,
        attempt_id: AttemptId,
        purpose: String,
        turn: u16,
        delta: String,
    },
    ReasoningEnd {
        run_id: RunId,
        task_id: TaskId,
        attempt_id: AttemptId,
        purpose: String,
        turn: u16,
    },
}

impl AgentReasoningEvent {
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::ReasoningStart { .. } => "reasoning-start",
            Self::ReasoningDelta { .. } => "reasoning-delta",
            Self::ReasoningEnd { .. } => "reasoning-end",
        }
    }

    pub fn run_id(&self) -> &RunId {
        match self {
            Self::ReasoningStart { run_id, .. }
            | Self::ReasoningDelta { run_id, .. }
            | Self::ReasoningEnd { run_id, .. } => run_id,
        }
    }
}

type ModelEventSink = Arc<dyn Fn(ModelStreamEvent) + Send + Sync>;

#[derive(Debug, Clone, PartialEq)]
struct ToolResult {
    value: Value,
    artifact: Artifact,
}

struct TurnRecord {
    permit: TaskWritePermit,
    contract: AgentContract,
    manifest: ContextManifest,
    turn: u16,
    attempt: u8,
    now: DateTime<Utc>,
}

/// Deliberately tiny seam. The production `akzio-model` adapter and fixture tests
/// both implement this; no execution/policy authority crosses it.
pub trait AgentModel: Send + Sync {
    // 默认 capability/预算是未知或空政策；生产调用必须由 Runtime 结合实际 probe
    // 和冻结预算再次校验，trait 本身不授予工具、订单或拓扑权限。
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        ModelCapabilitySnapshot::unknown()
    }

    fn response_language(&self) -> Option<&str> {
        None
    }

    fn budget_policy(&self) -> ModelBudgetPolicy {
        ModelBudgetPolicy::default()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>>;

    fn turn_with_events<'a>(
        &'a self,
        request: AgentModelRequest,
        _on_event: ModelEventSink,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        self.turn(request)
    }
}

#[derive(Debug, Clone)]
pub struct ModelClientAdapter {
    client: ModelClient,
    capability_snapshot: ModelCapabilitySnapshot,
    debug: bool,
    response_language: String,
    budget_policy: ModelBudgetPolicy,
}

impl ModelClientAdapter {
    pub fn new(client: ModelClient) -> Self {
        Self::with_debug(client, false)
    }

    pub fn with_debug(client: ModelClient, debug: bool) -> Self {
        Self::with_response_language(client, debug, "简体中文")
    }

    pub fn with_response_language(
        client: ModelClient,
        debug: bool,
        response_language: impl Into<String>,
    ) -> Self {
        // capability 在适配器创建时快照，之后作为每个 AgentTurn 的 provenance；
        // with_capability_snapshot 仅用于测试/已审计 probe 的明确替换。
        let capability_snapshot = client.capability_snapshot();
        Self {
            client,
            capability_snapshot,
            debug,
            response_language: response_language.into(),
            budget_policy: ModelBudgetPolicy::default(),
        }
    }

    pub fn with_budget_policy(mut self, budget_policy: ModelBudgetPolicy) -> Self {
        self.budget_policy = budget_policy;
        self
    }

    pub fn with_capability_snapshot(
        mut self,
        capability_snapshot: ModelCapabilitySnapshot,
    ) -> Self {
        self.capability_snapshot = capability_snapshot;
        self
    }
}

impl AgentModel for ModelClientAdapter {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        self.capability_snapshot.clone()
    }

    fn response_language(&self) -> Option<&str> {
        Some(&self.response_language)
    }

    fn budget_policy(&self) -> ModelBudgetPolicy {
        self.budget_policy.clone()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        self.turn_with_events(request, Arc::new(|_| {}))
    }

    fn turn_with_events<'a>(
        &'a self,
        request: AgentModelRequest,
        on_event: ModelEventSink,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            // Continue 请求只携带模型 continuation 与已持久化 ToolResult；Fresh 请求
            // 才发送 objective/context。terminal tool 被强制 required，确保 Submit
            // 结果不会因为普通文本返回而被误认为正式输出。
            let terminal_name = request
                .terminal
                .as_ref()
                .map(|_| TERMINAL_SUBMISSION_TOOL.to_owned());
            let input = match request.continuation {
                Some(continuation) => ModelInput::Continue {
                    continuation,
                    tool_outputs: request.tool_outputs,
                    instruction: request.continuation_instruction,
                },
                None => ModelInput::Fresh {
                    text: serde_json::to_string(&json!({
                        "objective": request.objective,
                        "context_manifest": request.manifest_artifact_id,
                        "context": request.context,
                    }))?,
                },
            };
            let mut tools = request
                .tools
                .into_iter()
                .map(|tool| ModelToolDefinition {
                    name: tool.name,
                    description: tool.description,
                    input_schema: tool.input_schema,
                    strict: tool.strict,
                })
                .collect::<Vec<_>>();
            if let Some(terminal) = request.terminal {
                tools.push(ModelToolDefinition {
                    name: TERMINAL_SUBMISSION_TOOL.to_owned(),
                    description: terminal.description,
                    input_schema: terminal.input_schema,
                    strict: true,
                });
            }
            let tool_choice = match terminal_name {
                Some(name) => ModelToolChoice::RequiredFunction(name),
                None if tools.is_empty() => ModelToolChoice::None,
                None => ModelToolChoice::Auto,
            };
            let request = ModelRequest {
                instructions: request.prompt,
                input,
                max_output_tokens: request.max_output_tokens,
                reasoning_effort: request.reasoning_effort,
                tools,
                tool_choice,
                fixture_key: Some(request.purpose),
            };
            let debug_request = self.debug.then(|| self.client.request_body(&request));
            let started = Instant::now();
            let response = self
                .client
                .respond_with_events(request, move |event| on_event(event))
                .await
                .map_err(|error| {
                    let trace = debug_request.map(|request| ModelCallTrace {
                        request,
                        result: model_error_result(&error),
                    });
                    model_client_error(error, trace)
                })?;
            let telemetry = AgentTurnTelemetry {
                provider_request_id: response.provider_request_id.clone(),
                response_id: response
                    .raw
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                requested_model: response
                    .request_body
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                actual_model: response
                    .raw
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                latency_millis: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                input_tokens: response.usage.input_tokens,
                cached_input_tokens: response.usage.cached_input_tokens,
                output_tokens: response.usage.output_tokens,
                reasoning_tokens: response.usage.reasoning_tokens,
            };
            let model_debug = self.debug.then(|| ModelCallTrace {
                request: response.request_body.clone(),
                result: response.raw.clone(),
            });
            // Only fixtures may use visible-text token estimates. A real
            // provider can consume hidden reasoning tokens, so missing totals
            // cannot establish compliance with the frozen Attempt budget.
            if matches!(&self.client, ModelClient::OpenAIResponses(_))
                && (response.usage.input_tokens.is_none() || response.usage.output_tokens.is_none())
            {
                return Err(ResearchError::ProviderUsageMissing {
                    usage: response.usage,
                    trace: model_debug.map(Box::new),
                });
            }
            let assistant_text = (!response.output_text.trim().is_empty())
                .then(|| response.output_text.trim().to_owned());
            let mut terminal_submission = None;
            let mut tool_calls = Vec::new();
            // 将 submit_result 与普通读取 ToolCall 分开；多个 terminal submission
            // 是歧义错误，不能任意挑一个提交以掩盖模型返回不确定性。
            for call in response.tool_calls {
                if call.name == TERMINAL_SUBMISSION_TOOL {
                    if terminal_submission.is_some() {
                        return Err(ResearchError::AmbiguousSubmission);
                    }
                    terminal_submission = Some(AgentTerminalSubmission {
                        call_id: call.call_id,
                        arguments: call.arguments,
                    });
                } else {
                    tool_calls.push(AgentToolCall {
                        call_id: call.call_id,
                        name: call.name,
                        arguments: call.arguments,
                    });
                }
            }
            Ok(AgentModelTurn {
                assistant_text,
                tool_calls,
                terminal_submission,
                continuation: response.continuation,
                telemetry: Some(telemetry),
                model_debug,
            })
        })
    }
}

#[cfg(test)]
mod provider_accounting_tests {
    use super::*;
    use std::io::{Read, Write};

    fn request() -> AgentModelRequest {
        AgentModelRequest {
            contract_hash: akzio_domain::ContentHash::of_bytes(b"usage-test"),
            purpose: RESEARCH_SYNTHESIZER_RECIPE_ID.into(),
            phase: AgentTurnPhase::Draft,
            prompt: "Offline protocol test".into(),
            objective: "Usage accounting".into(),
            manifest_artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(b"manifest")),
            read_grant_identity: None,
            context_materialization_identity: None,
            context: vec![],
            continuation: None,
            tool_outputs: vec![],
            continuation_instruction: None,
            max_output_tokens: 100,
            reasoning_effort: None,
            tools: vec![],
            terminal: None,
        }
    }

    #[test]
    fn recovered_completed_usage_keeps_inconsistent_and_output_limit_distinct() {
        let policy =
            akzio_domain::budget::default_agent_budget(RESEARCH_SYNTHESIZER_RECIPE_ID).unwrap();
        for (output, reasoning) in [(50, 51), (101, 10)] {
            let turn = AgentModelTurn {
                assistant_text: Some("Auditable memo".into()),
                tool_calls: vec![],
                terminal_submission: None,
                continuation: ModelContinuation::from_items(vec![]),
                telemetry: Some(AgentTurnTelemetry {
                    provider_request_id: None,
                    response_id: None,
                    requested_model: None,
                    actual_model: None,
                    latency_millis: 1,
                    input_tokens: Some(125),
                    cached_input_tokens: Some(25),
                    output_tokens: Some(output),
                    reasoning_tokens: Some(reasoning),
                }),
                model_debug: None,
            };
            let mut checkpoint = AgentRecoveryCheckpoint::fresh();
            checkpoint
                .usage
                .record(&request(), &turn, &ModelBudgetPolicy::default())
                .unwrap();
            assert_eq!(checkpoint.usage.output_tokens, output);
            assert_eq!(checkpoint.usage.input_tokens, 125);
            let result = AgentRunBudget::new(&policy, &RetryPolicy::none()).restore(&checkpoint);
            if reasoning > output {
                assert!(
                    matches!(result, Err(ResearchError::InvalidProviderUsage)),
                    "{result:?}"
                );
            } else {
                assert!(
                    matches!(
                        result,
                        Err(ResearchError::ProviderOutputLimitExceeded {
                            actual: 101,
                            maximum: 100
                        })
                    ),
                    "{result:?}"
                );
            }
        }
    }

    #[test]
    fn usage_failures_have_distinct_diagnostic_classes() {
        for (error, expected) in [
            (
                ResearchError::ProviderUsageUnknown,
                "provider_usage_unknown",
            ),
            (
                ResearchError::ProviderUsageMissing {
                    usage: ModelUsage::default(),
                    trace: None,
                },
                "provider_usage_missing",
            ),
            (
                ResearchError::InvalidProviderUsage,
                "provider_usage_inconsistent",
            ),
            (
                ResearchError::ProviderOutputLimitExceeded {
                    actual: 101,
                    maximum: 100,
                },
                "provider_output_limit_exceeded",
            ),
            (
                ResearchError::WallTimeExceeded { maximum_secs: 120 },
                "wall_time",
            ),
            (
                ResearchError::Model("offline transport failure".into()),
                "transport",
            ),
        ] {
            assert_eq!(model_error_class(&error), expected);
            assert_eq!(research_error_detail(&error)["kind"], expected);
        }
        assert!(ResearchError::ProviderUsageUnknown
            .to_string()
            .contains("unknown"));
        assert!(!ResearchError::ProviderUsageUnknown
            .to_string()
            .contains("exceeds"));
    }

    async fn local_http_error(timeout: bool) -> ModelError {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = timeout.then(|| {
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                // Hold the accepted connection until the client deadline cancels it.
                std::future::pending::<()>().await;
                drop(stream);
            })
        });
        let error = reqwest::Client::builder()
            .no_proxy()
            .timeout(StdDuration::from_millis(50))
            .build()
            .unwrap()
            .get(format!("http://{address}/responses"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(error.is_timeout(), timeout);
        if let Some(server) = server {
            server.abort();
            let _ = server.await;
        }
        ModelError::Transport(error)
    }

    #[tokio::test]
    async fn provider_timeouts_keep_type_with_and_without_debug_trace() {
        let retry = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 0,
            retry_transport: true,
            retry_rate_limited: false,
            retry_invalid_output: false,
        };
        for debug in [false, true] {
            for (source, expected_kind, expected_result) in [
                (
                    local_http_error(true).await,
                    Some("http_timeout"),
                    "http_timeout",
                ),
                (
                    ModelError::StreamIdleTimeout {
                        idle_timeout: StdDuration::from_secs(60),
                    },
                    Some("stream_idle_timeout"),
                    "stream_idle_timeout",
                ),
                (local_http_error(false).await, None, "transport"),
            ] {
                let result = model_error_result(&source);
                assert_eq!(result["error"], expected_result);
                let trace = debug.then(|| ModelCallTrace {
                    request: json!({"model": "offline-test"}),
                    result,
                });
                let error = model_client_error(source, trace.clone());
                let detail = research_error_detail(&error);
                if let Some(kind) = expected_kind {
                    assert_eq!(model_error_class(&error), "provider_timeout");
                    assert_eq!(detail["kind"], "provider_timeout");
                    assert_eq!(detail["timeout_kind"], kind);
                } else {
                    assert_eq!(model_error_class(&error), "transport");
                    assert_eq!(detail["kind"], "transport");
                }
                assert_eq!(model_debug_trace(&error), trace.as_ref());
                assert!(retryable_model_error(&error, &retry));
                assert!(!retryable_model_error(&error, &RetryPolicy::none()));
                assert_eq!(error.retry_cause(), None);
                // A transport deadline says nothing about Provider usage. The
                // same no-usage recovery branch must reject further spending.
                assert!(detail.get("usage").is_none());
                let mut checkpoint = AgentRecoveryCheckpoint::fresh();
                checkpoint
                    .usage
                    .record_failed(&request(), &ModelBudgetPolicy::default())
                    .unwrap();
                let policy =
                    akzio_domain::budget::default_agent_budget(RESEARCH_SYNTHESIZER_RECIPE_ID)
                        .unwrap();
                assert!(matches!(
                    AgentRunBudget::new(&policy, &RetryPolicy::none()).restore(&checkpoint),
                    Err(ResearchError::ProviderUsageUnknown)
                ));
            }
        }
        assert_eq!(
            model_error_class(&ResearchError::WallTimeExceeded { maximum_secs: 120 }),
            "wall_time"
        );
    }

    fn response() -> Value {
        json!({"id":"offline-response", "status":"completed", "model":"offline-test",
            "output":[{"type":"message", "role":"assistant", "content":[{"type":"output_text", "text":"Memo complete"}]}]})
    }

    async fn real_provider_turn(raw: Value) -> ResearchResult<AgentModelTurn> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 8192];
            loop {
                let size = stream.read(&mut chunk).unwrap();
                assert!(size > 0, "request ended before its declared body");
                request.extend_from_slice(&chunk[..size]);
                if let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                {
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let body_bytes = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= header_end + 4 + body_bytes {
                        break;
                    }
                }
            }
            let payload = format!(
                "data: {}\n\n",
                json!({"type":"response.completed", "response": raw})
            );
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", payload.len(), payload).unwrap();
        });
        let client = akzio_model::OpenAIResponsesClient::new(
            format!("http://{address}/v1"),
            "offline-test-key",
            "offline-test",
            "low",
        )
        .unwrap();
        let result = ModelClientAdapter::new(ModelClient::OpenAIResponses(client))
            .turn(request())
            .await;
        server.join().unwrap();
        result
    }

    #[test]
    fn missing_input_total_cannot_resume_from_failed_turn_usage() {
        // Use the same serialized error_detail usage and recovery accumulator
        // as recovery::record_agent_turn, rather than fabricating a recovery failure.
        let error = ResearchError::ProviderUsageMissing {
            usage: ModelUsage {
                input_tokens: None,
                cached_input_tokens: None,
                output_tokens: Some(50),
                reasoning_tokens: Some(10),
            },
            trace: None,
        };
        let usage: ModelUsage =
            serde_json::from_value(research_error_detail(&error)["usage"].clone()).unwrap();
        let mut checkpoint = AgentRecoveryCheckpoint::fresh();
        checkpoint
            .usage
            .record_failed_usage(&request(), &usage, &ModelBudgetPolicy::default())
            .unwrap();
        assert_eq!(checkpoint.usage.output_tokens, 50);
        assert_eq!(checkpoint.usage.reasoning_tokens, 10);
        assert!(matches!(
            checkpoint.usage.failure,
            Some(RecoveryUsageFailure::Missing(_))
        ));
        let policy =
            akzio_domain::budget::default_agent_budget(RESEARCH_SYNTHESIZER_RECIPE_ID).unwrap();
        let mut budget = AgentRunBudget::new(&policy, &RetryPolicy::none());
        assert!(matches!(
            budget.restore(&checkpoint),
            Err(ResearchError::ProviderUsageMissing { .. })
        ));
        let retry = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 0,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: true,
        };
        assert!(!retryable_model_error(&error, &retry));
    }

    #[tokio::test]
    async fn real_provider_missing_usage_is_rejected_but_fixture_remains_available() {
        let raw = response();
        let fixture = ModelClientAdapter::new(ModelClient::Fixture(raw.clone()));
        assert!(fixture.turn(request()).await.is_ok());
        for usage in [
            Value::Null,
            json!({"input_tokens": 125}),
            json!({"output_tokens": 50}),
        ] {
            let mut raw = response();
            raw["usage"] = usage;
            let error = real_provider_turn(raw)
                .await
                .expect_err("real provider requires both token totals");
            assert_eq!(model_error_class(&error), "provider_usage_missing");
            assert!(!retryable_model_error(&error, &RetryPolicy::none()));
            assert!(research_error_detail(&error).get("usage").is_some());
        }
        let mut complete = response();
        complete["usage"] = json!({"input_tokens": 125, "output_tokens": 50});
        let accepted = real_provider_turn(complete).await.unwrap();
        assert_eq!(accepted.telemetry.unwrap().output_tokens, Some(50));
    }
}
