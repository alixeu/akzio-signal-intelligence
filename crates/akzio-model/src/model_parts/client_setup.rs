impl ModelClient {
    pub fn fixture_sequence(values: impl IntoIterator<Item = Value>) -> Self {
        Self::FixtureSequence(Arc::new(Mutex::new(values.into_iter().collect())))
    }

    pub fn fixture_by_purpose(values: BTreeMap<String, Vec<Value>>) -> Self {
        Self::FixtureByPurpose(Arc::new(Mutex::new(
            values
                .into_iter()
                .map(|(purpose, values)| (purpose, values.into_iter().collect()))
                .collect(),
        )))
    }

    pub fn from_openai_responses_config(config: &OpenAIResponsesConfig) -> Result<Self> {
        Ok(Self::OpenAIResponses(OpenAIResponsesClient::new(
            &config.base_url,
            &config.api_key,
            &config.model,
            &config.reasoning_effort,
        )?))
    }

    pub fn from_config(config: &ModelConfig) -> Result<Self> {
        Self::from_openai_responses_config(config)
    }

    pub fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        match self {
            Self::OpenAIResponses(client) => {
                let declared = client.declared_capabilities();
                ModelCapabilitySnapshot {
                    provider_id: OPENAI_RESPONSES_PROVIDER_ID.to_owned(),
                    model_id: client.model.clone(),
                    reasoning_effort: client.reasoning_effort.clone(),
                    supports_tool_calls: declared.supports_tool_calls,
                    supports_stateless_continuation: declared.supports_stateless_continuation,
                    native_web_tool: declared.native_web_tool,
                    streaming: Some(declared.streaming),
                    declared_context_limit: None,
                    declared_max_output_tokens: None,
                    source: if is_official_openai_base_url(&client.base_url) {
                        "openai_responses_static_declared_unverified".to_owned()
                    } else {
                        "custom_endpoint_unverified".to_owned()
                    },
                }
            }
            Self::Fixture(_) | Self::FixtureByPurpose(_) | Self::FixtureSequence(_) => {
                ModelCapabilitySnapshot {
                    provider_id: "fixture".to_owned(),
                    model_id: "fixture".to_owned(),
                    reasoning_effort: "none".to_owned(),
                    supports_tool_calls: true,
                    supports_stateless_continuation: true,
                    native_web_tool: true,
                    streaming: Some(false),
                    declared_context_limit: None,
                    declared_max_output_tokens: None,
                    source: "fixture_static_declared_unverified".to_owned(),
                }
            }
        }
    }

    pub fn openai_responses_capabilities(&self) -> Option<OpenAIResponsesCapabilities> {
        match self {
            Self::OpenAIResponses(client) => Some(client.declared_capabilities()),
            Self::Fixture(_) | Self::FixtureByPurpose(_) | Self::FixtureSequence(_) => None,
        }
    }

    /// Exact provider payload used for an individual turn, excluding auth.
    pub fn request_body(&self, request: &ModelRequest) -> Value {
        match self {
            Self::OpenAIResponses(client) => client.request_body(request),
            Self::Fixture(_) | Self::FixtureByPurpose(_) | Self::FixtureSequence(_) => {
                openai_responses_request_body("fixture", "none", request)
            }
        }
    }
}
