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
            response_id: response.raw.get("id").and_then(Value::as_str).map(str::to_owned),
            requested_model: response.request_body.get("model").and_then(Value::as_str).map(str::to_owned),
            actual_model: response.raw.get("model").and_then(Value::as_str).map(str::to_owned),
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
            let assistant_text = (!response.output_text.trim().is_empty())
                .then(|| response.output_text.trim().to_owned());
            let mut terminal_submission = None;
            let mut tool_calls = Vec::new();
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
