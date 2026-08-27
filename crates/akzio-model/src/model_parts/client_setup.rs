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

    pub fn from_config(config: &ModelConfig) -> Result<Self> {
        Ok(Self::Responses(ResponsesClient::new(
            &config.base_url,
            &config.api_key,
            &config.model,
            &config.reasoning_effort,
        )?))
    }

    pub fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        match self {
            Self::Responses(client) => ModelCapabilitySnapshot {
                provider_id: "responses".to_owned(),
                model_id: client.model.clone(),
                reasoning_effort: client.reasoning_effort.clone(),
                supports_tool_calls: true,
                supports_stateless_continuation: true,
                native_web_tool: true,
                streaming: Some(true),
                declared_context_limit: None,
                declared_max_output_tokens: None,
                source: "adapter_declared".to_owned(),
            },
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
                    source: "adapter_declared".to_owned(),
                }
            }
        }
    }

    /// Exact provider payload used for an individual turn, excluding auth.
    pub fn request_body(&self, request: &ModelRequest) -> Value {
        match self {
            Self::Responses(client) => client.request_body(request),
            Self::Fixture(_) | Self::FixtureByPurpose(_) | Self::FixtureSequence(_) => {
                responses_request_body("fixture", "none", request)
            }
        }
    }
}
