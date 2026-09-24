const CAPABILITY_PROBE_TOOL: &str = "akzio_capability_probe";
const CAPABILITY_PROBE_COMPLETE_TOOL: &str = "akzio_capability_probe_complete";
const CAPABILITY_PROBE_SOURCE: &str = "runtime_function_tool_stateless_continuation_probe_v1";
const NATIVE_WEB_PROBE_SOURCE: &str = "native_web_required_search_probe_v2";

pub async fn probe_configured_model_capabilities(
    config: &OpenAIResponsesConfig,
) -> Result<ModelCapabilityProbeSet> {
    // 这是 async 的顺序探测：默认 route 完成后才逐个探测命名 route，避免把一个
    // route 的结果借给另一个 route；任一步 provider/校验错误都会经 ? 直接返回。
    // 默认 route 与每个 purpose route 都独立建 client、独立探测；只有全部快照
    // 与当前配置的模型和 reasoning_effort 对齐后，才返回可供上层使用的集合。
    let default = ModelClient::from_config(config)?
        .probe_capabilities()
        .await?;
    let mut routes = BTreeMap::new();
    for (purpose, route) in &config.routes {
        let route_config = config.for_route(route);
        routes.insert(
            purpose.clone(),
            ModelClient::from_config(&route_config)?
                .probe_capabilities()
                .await?,
        );
    }
    let probes = ModelCapabilityProbeSet { default, routes };
    probes.validate_for_config(config)?;
    Ok(probes)
}

impl ModelClient {
    pub async fn probe_capabilities(&self) -> Result<ModelCapabilitySnapshot> {
        // 真实 Responses client 需要实际 provider 往返；fixture 只返回离线声明，
        // 不把离线结果伪装成 runtime-negotiated handshake。
        match self {
            Self::OpenAIResponses(client) => probe_openai_responses_capabilities(client).await,
            Self::Fixture(_)
            | Self::FixtureByPurpose(_)
            | Self::FixtureByPurposePhase(_)
            | Self::FixtureSequence(_) => Ok(self.capability_snapshot()),
        }
    }
}

async fn probe_openai_responses_capabilities(
    client: &OpenAIResponsesClient,
) -> Result<ModelCapabilitySnapshot> {
    // 非审计入口复用同一探测流程，但丢弃仅供质量记录的调用摘要。
    probe_openai_responses_capabilities_audited(client)
        .await
        .map(|(snapshot, _)| snapshot)
}

/// Minimal provider-only audit; deliberately excludes headers and continuation payloads.
impl ModelClient {
    pub async fn probe_capabilities_audited(
        &self,
    ) -> Result<(ModelCapabilitySnapshot, Vec<Value>)> {
        // audited 结果包含 provider 请求/响应的脱敏摘要，因此只允许真实 provider
        // 产生；fixture 没有可证明的外部请求，直接拒绝而不是补造审计记录。
        match self {
            Self::OpenAIResponses(client) => {
                probe_openai_responses_capabilities_audited(client).await
            }
            _ => Err(ModelError::CapabilityProbe(
                "real provider required for audited preflight".into(),
            )),
        }
    }
}

async fn probe_openai_responses_capabilities_audited(
    client: &OpenAIResponsesClient,
) -> Result<(ModelCapabilitySnapshot, Vec<Value>)> {
    // 前两次 respond 的错误会短路整个 function/continuation 探测；native web 则在
    // 后面被折叠成状态快照，故 hosted web 失败不会抹掉已经通过的函数能力证据。
    // 两次 required function call 验证工具调用与无状态续传；随后另做一次 required
    // native web probe。所有请求都经过同一个无状态 Responses adapter。
    let started = std::time::Instant::now();
    let first = client
        .respond(capability_probe_request(
            CAPABILITY_PROBE_TOOL,
            ModelInput::Fresh {
                text: probe_prompts::CALL.to_owned(),
            },
        ))
        .await?;
    let call = first
        .tool_calls
        .iter()
        .find(|call| call.name == CAPABILITY_PROBE_TOOL)
        .ok_or_else(|| {
            ModelError::CapabilityProbe(
                "initial response did not return the required function call".to_owned(),
            )
        })?;
    // 第二次请求复用 first.continuation，并只取出 call_id 作为 tool output 的关联键；
    // provider transcript 本身不会被借用状态带入别的请求。
    let first_audit = capability_response_audit(&first, started.elapsed());
    let (reasoning_items, encrypted_continuation) =
        continuation_observations(first.continuation.items());

    let started = std::time::Instant::now();
    let second = client
        .respond(capability_probe_request(
            CAPABILITY_PROBE_COMPLETE_TOOL,
            ModelInput::Continue {
                continuation: first.continuation,
                tool_outputs: vec![ModelToolOutput {
                    call_id: call.call_id.clone(),
                    output: json!({"accepted": true}),
                }],
                instruction: Some(
                    "Continue from the supplied items and call the required completion function."
                        .to_owned(),
                ),
            },
        ))
        .await?;
    // function call 已返回并不等于续传可用；完成函数仍必须在第二个终态中出现。
    if !second
        .tool_calls
        .iter()
        .any(|call| call.name == CAPABILITY_PROBE_COMPLETE_TOOL)
    {
        return Err(ModelError::CapabilityProbe(
            "stateless continuation did not return the required completion function call"
                .to_owned(),
        ));
    }

    let audit = vec![
        first_audit,
        capability_response_audit(&second, started.elapsed()),
    ];
    // hosted web 能力是独立可失败项：失败原因进入状态和脱敏审计，但不抹掉已经
    // 验证通过的 function/continuation 能力。
    let (native_web_tool, native_web_tool_verified, native_web_status, native_web_audit) =
        probe_native_web_tool(client).await;
    let mut audit = audit;
    if let Some(native_web_audit) = native_web_audit {
        audit.push(native_web_audit);
    }
    Ok((
        ModelCapabilitySnapshot {
            provider_id: OPENAI_RESPONSES_PROVIDER_ID.to_owned(),
            model_id: client.model.clone(),
            reasoning_effort: client.reasoning_effort.clone(),
            supports_tool_calls: true,
            supports_stateless_continuation: true,
            native_web_tool,
            streaming: Some(true),
            declared_context_limit: None,
            declared_max_output_tokens: None,
            reasoning_items,
            encrypted_continuation,
            native_web_tool_verified,
            native_web_status,
            basis: ModelCapabilityBasis::RuntimeNegotiated,
            verified: true,
            source: format!("{CAPABILITY_PROBE_SOURCE}+{NATIVE_WEB_PROBE_SOURCE}"),
        },
        audit,
    ))
}

async fn probe_native_web_tool(
    client: &OpenAIResponsesClient,
) -> (bool, bool, NativeWebCapabilityStatus, Option<Value>) {
    // 这个函数刻意不返回 Result：web 探测的 transport、HTTP、解析和来源错误都转为
    // 可审计的 status；只有函数探测和无状态续传的硬失败才由上层 Result 传播。
    // 使用 Required 而非 Auto，确保“未调用”与“模型选择不搜索”可区分；验证同时
    // 要求 hosted action 和可提取、可 allowlist 校验的 citation。
    let policy = NativeWebPolicy::default();
    let request = ModelRequest {
        instructions: probe_prompts::WEB_GOVERNANCE
            .to_owned(),
        input: ModelInput::Fresh {
            text:
                probe_prompts::WEB_QUERY
                    .to_owned(),
        },
        max_output_tokens: 1000,
        reasoning_effort: None,
        tools: vec![policy.tool_definition()],
        // A capability probe must test the hosted tool itself. `auto` is
        // allowed to produce text without searching and therefore cannot
        // distinguish an unsupported tool from a model choice not to search.
        tool_choice: ModelToolChoice::Required,
        fixture_key: None,
    };
    match client.respond(request).await {
        Ok(response) => {
            // provider raw 只在这里做 hosted action/citation 验证；结果摘要不携带完整
            // response，避免 capability 审计扩散模型输出或凭据相关内容。
            let validation = policy
                .validate_provider_response(&response.raw)
                .and_then(|_| policy.extract_citations(&response.raw).map(|_| ()));
            let verified = validation.is_ok();
            let status = match &validation {
                Ok(()) => NativeWebCapabilityStatus::Verified,
                Err(error) => native_web_failure_status(Some(&response.raw), error),
            };
            (
                verified,
                verified,
                status,
                Some(json!({
                    "capability": "native_web",
                    "verified": verified,
                    "status": status,
                    "validation": validation.as_ref().err().map(safe_model_error),
                    "response_id": response.raw.get("id"),
                    "actual_model": response.raw.get("model"),
                    "usage": response.usage,
                    "web_search_calls": response.raw.get("output").and_then(Value::as_array).map(|items| items.iter().filter(|item| item.get("type").and_then(Value::as_str) == Some("web_search_call")).count()),
                })),
            )
        }
        Err(error) => {
            let status = native_web_failure_status(None, &error);
            (
                false,
                false,
                status,
                Some(json!({
                    "capability": "native_web",
                    "verified": false,
                    "status": status,
                    "error": safe_model_error(&error),
                })),
            )
        }
    }
}

fn native_web_failure_status(raw: Option<&Value>, error: &ModelError) -> NativeWebCapabilityStatus {
    // 若已有 raw response，优先按实际 hosted web_call 轨迹区分未调用、无来源和
    // 来源校验问题；没有 raw 时再按 HTTP/transport/adapter 错误分类。
    if let Some(raw) = raw {
        let calls = raw
            .get("output")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("web_search_call"))
            .collect::<Vec<_>>();
        if calls.is_empty() {
            return NativeWebCapabilityStatus::NotCalled;
        }
        let has_search_action = calls
            .iter()
            .any(|call| call.pointer("/action/type").and_then(Value::as_str) == Some("search"));
        if !has_search_action {
            return NativeWebCapabilityStatus::NoVerifiableSources;
        }
        let has_sources = calls.iter().any(|call| {
            call.pointer("/action/sources")
                .and_then(Value::as_array)
                .is_some_and(|sources| !sources.is_empty())
        });
        if !has_sources
            && matches!(
                error,
                ModelError::NativeWebArgumentsInvalid | ModelError::NativeWebCitationsMissing
            )
        {
            return NativeWebCapabilityStatus::NoVerifiableSources;
        }
    }

    match error {
        ModelError::Http { status, body } => match status.as_u16() {
            401 | 403 => NativeWebCapabilityStatus::AuthorizationDenied,
            408 | 500..=599 => NativeWebCapabilityStatus::TemporaryProviderError,
            429 => NativeWebCapabilityStatus::RateLimited,
            _ if provider_declares_unsupported_tool(body) => {
                NativeWebCapabilityStatus::ToolUnsupported
            }
            _ => NativeWebCapabilityStatus::ProviderRouteError,
        },
        ModelError::Transport(_) => NativeWebCapabilityStatus::TransportError,
        ModelError::StreamIdleTimeout { .. } => NativeWebCapabilityStatus::TemporaryProviderError,
        ModelError::NativeWebCitationsMissing => NativeWebCapabilityStatus::NoVerifiableSources,
        ModelError::NativeWebUnsafeCitation { .. } => {
            NativeWebCapabilityStatus::SourceValidationFailed
        }
        ModelError::NativeWebUnavailable => NativeWebCapabilityStatus::NoVerifiableSources,
        ModelError::NativeWebToolNotAllowed => NativeWebCapabilityStatus::ToolUnsupported,
        ModelError::NativeWebArgumentsInvalid | ModelError::NativeWebLimitExceeded => {
            NativeWebCapabilityStatus::InvalidResponse
        }
        _ => NativeWebCapabilityStatus::InvalidResponse,
    }
}

fn provider_declares_unsupported_tool(body: &str) -> bool {
    // provider 错误正文只用于识别“工具不支持”；无法解析或未命中固定词组时不猜测。
    let Ok(value) = serde_json::from_str::<Value>(body) else {
        return false;
    };
    let mut text = String::new();
    collect_error_text(&value, &mut text);
    let text = text.to_ascii_lowercase();
    [
        "unsupported tool",
        "tool is not supported",
        "tool not supported",
        "unknown tool",
        "web_search is not available",
        "web_search is not supported",
        "does not support web_search",
    ]
    .into_iter()
    .any(|needle| text.contains(needle))
}

fn collect_error_text(value: &Value, output: &mut String) {
    // 递归收集结构化错误里的字符串，供上层做有限、可审计的分类匹配。
    match value {
        Value::String(value) => {
            if !output.is_empty() {
                output.push(' ');
            }
            output.push_str(value);
        }
        Value::Array(values) => values
            .iter()
            .for_each(|value| collect_error_text(value, output)),
        Value::Object(values) => values
            .values()
            .for_each(|value| collect_error_text(value, output)),
        _ => {}
    }
}

fn safe_model_error(error: &ModelError) -> Value {
    // capability 审计只保留稳定错误类别和必要状态码，不复制 HTTP body、URL 细节或
    // 其他可能包含敏感内容的错误文本。
    match error {
        ModelError::Http { status, .. } => json!({
            "kind": "http",
            "status": status.as_u16(),
        }),
        ModelError::Transport(_) => json!({"kind": "transport"}),
        ModelError::StreamIdleTimeout { idle_timeout } => json!({
            "kind": "stream_idle_timeout",
            "timeout_ms": idle_timeout.as_millis(),
        }),
        ModelError::NativeWebCitationsMissing => json!({"kind": "no_verifiable_sources"}),
        ModelError::NativeWebUnsafeCitation { .. } => {
            json!({"kind": "source_validation_failed"})
        }
        ModelError::NativeWebUnavailable => json!({"kind": "no_verifiable_sources"}),
        ModelError::NativeWebToolNotAllowed => json!({"kind": "tool_unsupported"}),
        ModelError::NativeWebArgumentsInvalid | ModelError::NativeWebLimitExceeded => {
            json!({"kind": "invalid_response"})
        }
        ModelError::CapabilityProbe(_) => json!({"kind": "capability_probe"}),
        ModelError::Refused(_) => json!({"kind": "refused"}),
        ModelError::Incomplete { .. } => json!({"kind": "incomplete"}),
        ModelError::MissingOutput => json!({"kind": "missing_output"}),
        ModelError::FixtureExhausted => json!({"kind": "fixture_exhausted"}),
        ModelError::EmptyBaseUrl
        | ModelError::EmptyApiKey
        | ModelError::EmptyModel
        | ModelError::EmptyReasoningEffort => json!({"kind": "configuration"}),
        ModelError::InvalidStream(_) => json!({"kind": "invalid_stream"}),
    }
}

fn capability_probe_request(tool_name: &str, input: ModelInput) -> ModelRequest {
    // 构造最小 required function probe；输入只描述探测意图，不授予研究 Context 或
    // Decision/Execution 工具权限。
    ModelRequest {
        instructions: probe_prompts::FUNCTION_GOVERNANCE.to_owned(),
        input,
        max_output_tokens: 96,
        reasoning_effort: None,
        tools: vec![ModelToolDefinition {
            name: tool_name.to_owned(),
            description: "Return a minimal capability-probe acknowledgement.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }),
            strict: true,
        }],
        tool_choice: ModelToolChoice::RequiredFunction(tool_name.to_owned()),
        fixture_key: None,
    }
}

fn continuation_observations(items: &[Value]) -> (Option<bool>, Option<bool>) {
    // items 是上一轮响应返回的 transcript 借用；观察函数只读它，不修改或重新请求，
    // 因而无法凭空补出 reasoning/encrypted continuation 能力。
    // 从 provider 返回的 transcript 中观察 reasoning item；没有该 item 时保持未知，
    // 不把“未返回”误判成明确不支持。
    let reasoning = items
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"))
        .collect::<Vec<_>>();
    if reasoning.is_empty() {
        return (None, None);
    }
    (
        Some(true),
        Some(reasoning.iter().any(|item| {
            item.get("encrypted_content")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        })),
    )
}

fn capability_response_audit(response: &ModelResponse, elapsed: std::time::Duration) -> Value {
    // 记录请求/响应身份、usage、延迟和工具声明，故意不放入完整 raw response。
    json!({
        "provider": OPENAI_RESPONSES_PROVIDER_ID,
        "requested_model": response.request_body.get("model"),
        "actual_model": response.raw.get("model"),
        "provider_request_id": response.provider_request_id,
        "response_id": response.raw.get("id"),
        "usage": response.usage,
        "latency_millis": elapsed.as_millis(),
        "tools_offered": response.request_body.get("tools"),
        "tool_choice": response.request_body.get("tool_choice"),
        "tool_calls": response.tool_calls,
        "stream": response.request_body.get("stream"),
        "probe_version": CAPABILITY_PROBE_SOURCE,
    })
}

#[cfg(test)]
mod capability_audit_tests {
    use super::*;

    #[test]
    fn required_web_probe_serializes_as_required_tool_choice() {
        let request = ModelRequest {
            instructions: "probe".to_owned(),
            input: ModelInput::Fresh {
                text: "search".to_owned(),
            },
            max_output_tokens: 256,
            reasoning_effort: None,
            tools: vec![NativeWebPolicy::default().tool_definition()],
            tool_choice: ModelToolChoice::Required,
            fixture_key: None,
        };
        let body = openai_responses_request_body("gpt-test", "low", &request);
        assert_eq!(body["tool_choice"], "required");
        assert_eq!(body["tools"][0]["type"], NATIVE_WEB_SEARCH_TOOL);
        assert!(body["include"].as_array().is_some_and(|items| items
            .iter()
            .any(|item| { item == "web_search_call.action.sources" })));
        assert!(body["tools"][0]["filters"]["allowed_domains"]
            .as_array()
            .is_some_and(|domains| domains.iter().any(|domain| domain == "reuters.com")));
    }

    #[test]
    fn native_web_probe_classifies_no_call_no_sources_and_route_failures() {
        assert_eq!(
            native_web_failure_status(
                Some(&json!({"output": []})),
                &ModelError::NativeWebUnavailable,
            ),
            NativeWebCapabilityStatus::NotCalled
        );
        assert_eq!(
            native_web_failure_status(
                Some(&json!({
                    "output": [{"type":"web_search_call", "action":{"type":"search"}}]
                })),
                &ModelError::NativeWebArgumentsInvalid,
            ),
            NativeWebCapabilityStatus::NoVerifiableSources
        );
        assert_eq!(
            native_web_failure_status(
                None,
                &ModelError::Http {
                    status: reqwest::StatusCode::BAD_REQUEST,
                    body: json!({"error":{"message":"web_search is not supported by this model"}})
                        .to_string(),
                },
            ),
            NativeWebCapabilityStatus::ToolUnsupported
        );
        assert_eq!(
            native_web_failure_status(
                None,
                &ModelError::Http {
                    status: reqwest::StatusCode::BAD_GATEWAY,
                    body: "gateway failure".to_owned(),
                },
            ),
            NativeWebCapabilityStatus::TemporaryProviderError
        );
        assert_eq!(
            native_web_failure_status(
                None,
                &ModelError::Http {
                    status: reqwest::StatusCode::BAD_REQUEST,
                    body: json!({"error":{"code":"model_not_found"}}).to_string(),
                },
            ),
            NativeWebCapabilityStatus::ProviderRouteError
        );
    }

    #[test]
    fn audit_preserves_provider_ids_and_unknown_usage_without_raw_response() {
        let response = ModelResponse {
            provider_request_id: Some("provider-request".into()),
            output_text: String::new(),
            tool_calls: vec![],
            continuation: ModelContinuation::from_items(vec![]),
            raw: json!({"id":"provider-response", "model":"actual", "secret":"never export"}),
            usage: ModelUsage::default(),
            request_body: json!({"model":"requested", "stream":true}),
        };
        let audit = capability_response_audit(&response, std::time::Duration::from_millis(12));
        assert_eq!(audit["provider_request_id"], "provider-request");
        assert_eq!(audit["response_id"], "provider-response");
        assert_eq!(audit["actual_model"], "actual");
        assert!(audit["usage"]["input_tokens"].is_null());
        assert!(!audit.to_string().contains("never export"));
    }
}
