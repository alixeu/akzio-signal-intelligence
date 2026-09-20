impl ModelClient {
    pub fn fixture_sequence(values: impl IntoIterator<Item = Value>) -> Self {
        // 将输入顺序一次收集进共享 FIFO；并发响应通过 Mutex 串行消耗同一测试序列。
        Self::FixtureSequence(Arc::new(Mutex::new(values.into_iter().collect())))
    }

    pub fn fixture_by_purpose(values: BTreeMap<String, Vec<Value>>) -> Self {
        // 每个 purpose 拥有独立 FIFO，适合验证同一研究角色的重试/多次调用顺序。
        Self::FixtureByPurpose(Arc::new(Mutex::new(
            values
                .into_iter()
                .map(|(purpose, values)| (purpose, values.into_iter().collect()))
                .collect(),
        )))
    }

    pub fn fixture_by_purpose_phase(values: BTreeMap<String, [Value; 2]>) -> Self {
        // [0, 1] 固定表示 Draft 与 Submit；模板本身不可变，调用时只 clone 当前格。
        Self::FixtureByPurposePhase(Arc::new(values))
    }

    pub fn from_openai_responses_config(config: &OpenAIResponsesConfig) -> Result<Self> {
        // OpenAIResponsesClient::new 负责清理地址并校验凭据、模型和 reasoning；这里
        // 只把已经选择的 route 配置转换为 provider client。
        Ok(Self::OpenAIResponses(OpenAIResponsesClient::new(
            &config.base_url,
            &config.api_key,
            &config.model,
            &config.reasoning_effort,
        )?))
    }

    pub fn from_config(config: &ModelConfig) -> Result<Self> {
        // 保留旧别名入口，但实际仍只创建 OpenAI Responses client。
        Self::from_openai_responses_config(config)
    }

    pub fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        // 真实 client 在此处只报告“尚未探测”的端点身份；fixture 的静态快照只服务
        // 离线测试，不能作为真实 provider handshake 或生产能力授权。
        match self {
            Self::OpenAIResponses(client) => ModelCapabilitySnapshot {
                provider_id: OPENAI_RESPONSES_PROVIDER_ID.to_owned(),
                model_id: client.model.clone(),
                reasoning_effort: client.reasoning_effort.clone(),
                supports_tool_calls: false,
                supports_stateless_continuation: false,
                native_web_tool: false,
                streaming: None,
                declared_context_limit: None,
                declared_max_output_tokens: None,
                reasoning_items: None,
                encrypted_continuation: None,
                native_web_tool_verified: false,
                native_web_status: NativeWebCapabilityStatus::NotProbed,
                basis: ModelCapabilityBasis::Unknown,
                verified: false,
                source: "endpoint_unprobed".to_owned(),
            },
            Self::Fixture(_)
            | Self::FixtureByPurpose(_)
            | Self::FixtureByPurposePhase(_)
            | Self::FixtureSequence(_) => ModelCapabilitySnapshot {
                provider_id: "fixture".to_owned(),
                model_id: "fixture".to_owned(),
                reasoning_effort: "none".to_owned(),
                supports_tool_calls: true,
                supports_stateless_continuation: true,
                native_web_tool: true,
                streaming: Some(false),
                declared_context_limit: None,
                declared_max_output_tokens: None,
                reasoning_items: Some(true),
                encrypted_continuation: Some(true),
                native_web_tool_verified: true,
                native_web_status: NativeWebCapabilityStatus::Verified,
                basis: ModelCapabilityBasis::StaticDeclared,
                verified: true,
                source: "fixture_offline".to_owned(),
            },
        }
    }

    /// Exact provider payload used for an individual turn, excluding auth.
    pub fn request_body(&self, request: &ModelRequest) -> Value {
        // 调试/审计只拿到不含认证信息的单轮请求；fixture 也使用同一 Responses wire
        // 形状，便于比较输入、工具和 tool_choice，而不产生外部副作用。
        match self {
            Self::OpenAIResponses(client) => client.request_body(request),
            Self::Fixture(_)
            | Self::FixtureByPurpose(_)
            | Self::FixtureByPurposePhase(_)
            | Self::FixtureSequence(_) => openai_responses_request_body("fixture", "none", request),
        }
    }
}
