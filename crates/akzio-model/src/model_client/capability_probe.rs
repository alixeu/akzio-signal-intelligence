const CAPABILITY_PROBE_TOOL: &str = "akzio_capability_probe";
const CAPABILITY_PROBE_COMPLETE_TOOL: &str = "akzio_capability_probe_complete";
const CAPABILITY_PROBE_SOURCE: &str = "runtime_function_tool_stateless_continuation_probe_v1";

pub async fn probe_configured_model_capabilities(
    config: &OpenAIResponsesConfig,
) -> Result<ModelCapabilityProbeSet> {
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
        match self {
            Self::OpenAIResponses(client) => probe_openai_responses_capabilities(client).await,
            Self::Fixture(_) | Self::FixtureByPurpose(_) | Self::FixtureSequence(_) => {
                Ok(self.capability_snapshot())
            }
        }
    }
}

async fn probe_openai_responses_capabilities(
    client: &OpenAIResponsesClient,
) -> Result<ModelCapabilitySnapshot> {
    let first = client
        .respond(capability_probe_request(
            CAPABILITY_PROBE_TOOL,
            ModelInput::Fresh {
                text: "Call the required capability probe function once.".to_owned(),
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
    let (reasoning_items, encrypted_continuation) =
        continuation_observations(first.continuation.items());

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

    Ok(ModelCapabilitySnapshot {
        provider_id: OPENAI_RESPONSES_PROVIDER_ID.to_owned(),
        model_id: client.model.clone(),
        reasoning_effort: client.reasoning_effort.clone(),
        supports_tool_calls: true,
        supports_stateless_continuation: true,
        native_web_tool: false,
        streaming: Some(true),
        declared_context_limit: None,
        declared_max_output_tokens: None,
        reasoning_items,
        encrypted_continuation,
        native_web_tool_verified: false,
        basis: ModelCapabilityBasis::RuntimeNegotiated,
        verified: true,
        source: CAPABILITY_PROBE_SOURCE.to_owned(),
    })
}

fn capability_probe_request(tool_name: &str, input: ModelInput) -> ModelRequest {
    ModelRequest {
        instructions: "Perform only the required Akzio capability probe function call.".to_owned(),
        input,
        max_output_tokens: 96,
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

