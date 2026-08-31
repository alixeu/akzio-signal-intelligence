use super::*;

fn request() -> ModelRequest {
    ModelRequest {
        instructions: "test".to_owned(),
        input: ModelInput::Fresh {
            text: "{}".to_owned(),
        },
        max_output_tokens: 1,
        tools: vec![],
        tool_choice: ModelToolChoice::None,
        fixture_key: Some("test".to_owned()),
    }
}

#[test]
fn response_request_body_marks_function_tools_strict() {
    let mut request = request();
    request.tools = vec![ModelToolDefinition {
        name: "read_artifact".to_owned(),
        description: "fixture".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {"artifact_id": {"type": "string"}},
            "required": ["artifact_id"],
            "additionalProperties": false,
        }),
        strict: true,
    }];
    request.tool_choice = ModelToolChoice::Auto;

    let body = responses_request_body("fixture", "high", &request);
    assert_eq!(body["tools"][0]["strict"], true);
    assert_eq!(body["reasoning"]["effort"], "high");
    assert_eq!(body["reasoning"]["summary"], "auto");
    assert_eq!(body["include"][0], "reasoning.encrypted_content");
    assert_eq!(body["store"], false);
    assert_eq!(body["stream"], true);
}

#[test]
fn response_request_body_drops_provider_unsupported_object_bounds() {
    let mut request = request();
    request.tools = vec![ModelToolDefinition {
        name: "submit_result".to_owned(),
        description: "fixture".to_owned(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "object",
                    "minProperties": 1,
                    "maxProperties": 4,
                    "properties": {
                        "assets": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 4,
                            "uniqueItems": true,
                        },
                    },
                    "patternProperties": {},
                },
            },
        }),
        strict: true,
    }];
    request.tool_choice = ModelToolChoice::RequiredFunction("submit_result".to_owned());

    let body = responses_request_body("fixture", "high", &request);
    let tasks = &body["tools"][0]["parameters"]["properties"]["tasks"];
    assert!(tasks.get("minProperties").is_none());
    assert!(tasks.get("maxProperties").is_none());
    assert!(tasks.get("patternProperties").is_none());
    assert!(tasks["properties"]["assets"].get("minItems").is_none());
    assert!(tasks["properties"]["assets"].get("maxItems").is_none());
    assert!(tasks["properties"]["assets"].get("uniqueItems").is_none());
}

#[test]
fn continuation_replays_items_then_tool_outputs_then_instruction() {
    let mut request = request();
    request.input = ModelInput::Continue {
        continuation: ModelContinuation::from_items(vec![
            json!({"type": "reasoning", "encrypted_content": "opaque"}),
            json!({
                "type": "function_call",
                "call_id": "call-1",
                "name": "read_artifact",
                "arguments": "{}"
            }),
        ]),
        tool_outputs: vec![ModelToolOutput {
            call_id: "call-1".to_owned(),
            output: json!({"ok": true, "value": {"price": 100}}),
        }],
        instruction: Some("submit now".to_owned()),
    };
    request.tools = vec![ModelToolDefinition {
        name: "submit_result".to_owned(),
        description: "terminal".to_owned(),
        input_schema: json!({"type": "object"}),
        strict: true,
    }];
    request.tool_choice = ModelToolChoice::RequiredFunction("submit_result".to_owned());

    let body = responses_request_body("fixture", "high", &request);
    let input = body["input"].as_array().unwrap();
    assert_eq!(input[0]["type"], "reasoning");
    assert_eq!(input[1]["call_id"], "call-1");
    assert_eq!(input[2]["type"], "function_call_output");
    assert_eq!(input[2]["call_id"], "call-1");
    assert_eq!(input[3]["content"], "submit now");
    assert_eq!(body["tool_choice"]["name"], "submit_result");
}

#[test]
fn response_rejects_refusal_and_incomplete_status() {
    assert!(matches!(
        response_from_raw(
            json!({
                "output": [{
                    "type": "message",
                    "content": [{"type": "refusal", "refusal": "not allowed"}]
                }]
            }),
            json!({})
        ),
        Err(ModelError::Refused(message)) if message == "not allowed"
    ));
    assert!(matches!(
        response_from_raw(
            json!({
                "status": "incomplete",
                "incomplete_details": {"reason": "max_output_tokens"}
            }),
            json!({})
        ),
        Err(ModelError::Incomplete(reason)) if reason == "max_output_tokens"
    ));
}

#[test]
fn configured_client_redacts_its_api_key_from_debug_output() {
    let client = ResponsesClient::new("http://fixture", "secret", "fixture", "medium").unwrap();

    let rendered = format!("{client:?}");
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("secret"));
}

#[test]
fn model_config_drives_reasoning_and_rejects_empty_credentials() {
    let config = ModelConfig {
        base_url: "http://fixture/v1".to_owned(),
        model: "fixture-model".to_owned(),
        api_key: "fixture-key".to_owned(),
        reasoning_effort: "low".to_owned(),
        response_language: "English".to_owned(),
        debug: true,
        routes: BTreeMap::new(),
    };
    let client = ModelClient::from_config(&config).unwrap();
    assert_eq!(
        client.request_body(&request())["reasoning"]["effort"],
        "low"
    );

    let mut missing_key = config;
    missing_key.api_key.clear();
    assert!(matches!(
        ModelClient::from_config(&missing_key),
        Err(ModelError::EmptyApiKey)
    ));
}

#[test]
fn route_config_overrides_model_and_reasoning_only() {
    let config = ModelConfig {
        base_url: "http://fixture/v1".to_owned(),
        model: "global-model".to_owned(),
        api_key: "fixture-key".to_owned(),
        reasoning_effort: "low".to_owned(),
        response_language: "English".to_owned(),
        debug: true,
        routes: BTreeMap::from([(
            "research.critic".to_owned(),
            ModelRouteConfig {
                model: "critic-model".to_owned(),
                reasoning_effort: "high".to_owned(),
                response_language: Some("简体中文".to_owned()),
            },
        )]),
    };

    let routed = config.for_route(config.routes.get("research.critic").unwrap());
    assert_eq!(routed.model, "critic-model");
    assert_eq!(routed.reasoning_effort, "high");
    assert_eq!(routed.response_language, "简体中文");
    assert_eq!(routed.base_url, config.base_url);
    assert_eq!(routed.api_key, config.api_key);
    assert!(routed.routes.is_empty());
}

#[test]
fn capability_snapshot_is_stable_and_redacted() {
    let config = ModelConfig {
        base_url: "https://example.invalid/v1".to_owned(),
        model: "fixture-model".to_owned(),
        api_key: "secret-key".to_owned(),
        reasoning_effort: "high".to_owned(),
        response_language: "English".to_owned(),
        debug: false,
        routes: BTreeMap::new(),
    };
    let client = ModelClient::from_config(&config).unwrap();
    let snapshot = client.capability_snapshot();
    assert_eq!(snapshot.provider_id, "responses");
    assert_eq!(snapshot.model_id, "fixture-model");
    assert_eq!(snapshot.reasoning_effort, "high");
    assert_eq!(snapshot.source, "adapter_declared");

    let encoded = serde_json::to_string(&snapshot).unwrap();
    assert!(!encoded.contains("secret-key"));
    assert!(!encoded.contains("example.invalid"));
    assert_eq!(snapshot, client.capability_snapshot());
}

#[test]
fn fixture_and_unknown_capability_snapshots_are_explicit() {
    let fixture = ModelClient::Fixture(json!({"output_text": "{}"}));
    let snapshot = fixture.capability_snapshot();
    assert_eq!(snapshot.provider_id, "fixture");
    assert_eq!(snapshot.model_id, "fixture");
    assert_eq!(snapshot.reasoning_effort, "none");
    assert!(snapshot.supports_tool_calls);

    let unknown = ModelCapabilitySnapshot::unknown();
    assert_eq!(unknown.provider_id, "unknown");
    assert!(!unknown.supports_tool_calls);
}

#[test]
fn extracts_direct_responses_output_text() {
    assert_eq!(
        extract_output_text(&json!({"output_text": "hello"})),
        Some("hello".to_owned())
    );
}

#[test]
fn extracts_nested_response_content() {
    assert_eq!(
        extract_output_text(&json!({
            "output": [{"content": [{"type": "output_text", "text": "hello"}]}]
        })),
        Some("hello".to_owned())
    );
}

#[test]
fn extracts_responses_function_calls() {
    let calls = extract_tool_calls(&json!({
        "output": [{
            "type": "function_call",
            "call_id": "call-1",
            "name": "read_evidence",
            "arguments": "{\"document_id\":\"doc-1\"}"
        }]
    }));
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name, "read_evidence");
    assert_eq!(calls[0].arguments["document_id"], "doc-1");
}

#[tokio::test]
async fn fixture_client_is_deterministic_without_network() {
    let client = ModelClient::Fixture(json!({"output_text": "{}"}));
    assert_eq!(client.respond(request()).await.unwrap().output_text, "{}");
}

#[tokio::test]
async fn fixture_sequence_preserves_tool_turn_order() {
    let client = ModelClient::fixture_sequence([
        json!({"output": [{
            "type": "function_call",
            "call_id": "call-1",
            "name": "read_evidence",
            "arguments": "{\"document_id\":\"doc-1\"}"
        }]}),
        json!({"output_text": "{}"}),
    ]);
    assert_eq!(client.respond(request()).await.unwrap().tool_calls.len(), 1);
    assert_eq!(client.respond(request()).await.unwrap().output_text, "{}");
    assert!(matches!(
        client.respond(request()).await,
        Err(ModelError::FixtureExhausted)
    ));
}

#[test]
fn native_web_contract_rejects_unallowlisted_query_and_uri() {
    let policy = NativeWebPolicy::default();
    let call = ModelToolCall {
        call_id: "call-1".to_owned(),
        name: policy.tool_name.clone(),
        arguments: json!({"query": "QQQ filing", "domains": ["example.com"]}),
    };
    assert!(matches!(
        policy.validate_tool_calls(&[call]),
        Err(ModelError::NativeWebToolNotAllowed)
    ));
    assert!(matches!(
        policy.extract_citations(&json!({"citations": [{"url": "https://example.com/a"}]})),
        Err(ModelError::NativeWebUnsafeCitation { ref uri, .. }) if uri == "https://example.com/a"
    ));
    let query_uri = policy
        .extract_citations(&json!({
            "citations": [{"url": "https://reuters.com/article?utm_source=fixture"}]
        }))
        .unwrap();
    assert_eq!(
        query_uri[0].uri,
        "https://reuters.com/article?utm_source=fixture"
    );
}

#[test]
fn native_web_citation_deduplication_keeps_richer_metadata() {
    let policy = NativeWebPolicy::default();
    let citations = policy
        .extract_citations(&json!({
            "output": [{
                "type": "web_search_call",
                "status": "completed",
                "action": {
                    "type": "search",
                    "query": "QQQ filing",
                    "sources": [{"url": "https://reuters.com/article"}]
                }
            }],
            "citations": [{
                "url": "https://reuters.com/article",
                "title": "Source title",
                "text": "Exact source excerpt",
                "published_at": "2026-08-29T12:00:00Z"
            }]
        }))
        .unwrap();

    assert_eq!(citations.len(), 1);
    assert_eq!(citations[0].title.as_deref(), Some("Source title"));
    assert_eq!(
        citations[0].excerpt.as_deref(),
        Some("Exact source excerpt")
    );
    assert_eq!(
        citations[0].published_at.as_deref(),
        Some("2026-08-29T12:00:00Z")
    );
}

#[test]
fn native_web_contract_requires_citations_and_bounds_results() {
    let policy = NativeWebPolicy::default();
    let call = ModelToolCall {
        call_id: "call-1".to_owned(),
        name: policy.tool_name.clone(),
        arguments: json!({"query": "QQQ filing", "domains": ["reuters.com"], "max_results": 1}),
    };
    assert_eq!(
        policy.validate_tool_calls(&[call]).unwrap()[0].max_results,
        1
    );
    assert!(matches!(
        policy.extract_citations(&json!({"output": "no citations"})),
        Err(ModelError::NativeWebCitationsMissing)
    ));
    let missing_domains = ModelToolCall {
        call_id: "call-2".to_owned(),
        name: policy.tool_name.clone(),
        arguments: json!({"query": "QQQ filing", "max_results": 1}),
    };
    assert!(matches!(
        policy.validate_tool_calls(&[missing_domains]),
        Err(ModelError::NativeWebArgumentsInvalid)
    ));
    let body = responses_request_body(
        "fixture",
        "high",
        &ModelRequest {
            tools: vec![policy.tool_definition()],
            ..request()
        },
    );
    assert_eq!(body["tools"][0]["type"], NATIVE_WEB_SEARCH_TOOL);
    assert_eq!(body["tools"][0]["filters"]["allowed_domains"][0], "sec.gov");
    assert!(body["include"]
        .as_array()
        .unwrap()
        .contains(&json!("web_search_call.action.sources")));
}

#[test]
fn native_web_contract_validates_provider_search_actions() {
    let policy = NativeWebPolicy {
        max_query_chars: 8,
        max_results: 1,
        ..NativeWebPolicy::default()
    };
    let response = json!({
        "output": [{
            "type": "web_search_call",
            "status": "completed",
            "action": {
                "type": "search",
                "queries": ["QQQ news"],
                "sources": [{"url": "https://reuters.com/story"}]
            }
        }]
    });
    policy.validate_provider_response(&response).unwrap();

    let too_long = json!({
        "output": [{
            "type": "web_search_call",
            "status": "completed",
            "action": {
                "type": "search",
                "query": "QQQ news today",
                "sources": [{"url": "https://reuters.com/story"}]
            }
        }]
    });
    assert!(matches!(
        policy.validate_provider_response(&too_long),
        Err(ModelError::NativeWebLimitExceeded)
    ));

    let too_many_sources = json!({
        "output": [{
            "type": "web_search_call",
            "status": "completed",
            "action": {
                "type": "search",
                "query": "QQQ",
                "sources": [
                    {"url": "https://reuters.com/one"},
                    {"url": "https://reuters.com/two"}
                ]
            }
        }]
    });
    assert!(matches!(
        policy.validate_provider_response(&too_many_sources),
        Err(ModelError::NativeWebLimitExceeded)
    ));
    assert!(matches!(
        policy.validate_provider_response(&json!({"output": []})),
        Err(ModelError::NativeWebUnavailable)
    ));
}

#[test]
fn native_web_contract_rejects_citations_beyond_the_limit() {
    let policy = NativeWebPolicy {
        max_citations: 2,
        ..NativeWebPolicy::default()
    };
    let response = json!({
        "citations": [
            {"url": "https://reuters.com/one"},
            {"url": "https://reuters.com/two"},
            {"url": "https://reuters.com/three"}
        ]
    });
    assert!(matches!(
        policy.extract_citations(&response),
        Err(ModelError::NativeWebLimitExceeded)
    ));

    let unsafe_after_duplicates = json!({
        "citations": [
            {"url": "https://reuters.com/one"},
            {"url": "https://reuters.com/one"},
            {"url": "https://reuters.com/one"},
            {"url": "https://example.com/hidden"}
        ]
    });
    assert!(matches!(
        policy.extract_citations(&unsafe_after_duplicates),
        Err(ModelError::NativeWebUnsafeCitation { ref uri, .. })
            if uri == "https://example.com/hidden"
    ));
}
