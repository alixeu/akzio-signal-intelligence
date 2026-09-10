fn should_advertise_read_tools(purpose: RunPurpose) -> bool {
    purpose != RunPurpose::PaperDryRun
}

fn estimate_tokens<T: Serialize>(value: &T) -> ResearchResult<u32> {
    Ok(akzio_domain::estimate_json_tokens(value)?)
}

fn estimate_turn_output_tokens(turn: &AgentModelTurn) -> ResearchResult<u32> {
    let assistant = turn
        .assistant_text
        .as_deref()
        .map(|text| estimate_tokens(&text))
        .transpose()?
        .unwrap_or_default();
    let tools = (!turn.tool_calls.is_empty())
        .then(|| estimate_tokens(&turn.tool_calls))
        .transpose()?
        .unwrap_or_default();
    let submission = turn
        .terminal_submission
        .as_ref()
        .map(|submission| estimate_tokens(&submission.arguments))
        .transpose()?
        .unwrap_or_default();
    Ok(assistant.saturating_add(tools).saturating_add(submission))
}

fn model_request_hash(request: &AgentModelRequest) -> ResearchResult<akzio_domain::ContentHash> {
    Ok(akzio_domain::content_hash_json(&serde_json::to_value(
        request,
    )?)?)
}

fn capability_snapshot_hash(
    snapshot: &ModelCapabilitySnapshot,
) -> ResearchResult<akzio_domain::ContentHash> {
    Ok(akzio_domain::content_hash_json(&serde_json::to_value(
        snapshot,
    )?)?)
}

fn research_error_detail(error: &ResearchError) -> Value {
    match error {
        ResearchError::Model(message)
        | ResearchError::RateLimited(message)
        | ResearchError::InvalidOutput(message) => json!({
            "kind": model_error_class(error),
            "message": sanitize_provider_text(message),
        }),
        ResearchError::ModelDebug {
            error_class,
            message,
            ..
        } => json!({
            "kind": error_class,
            "message": sanitize_provider_text(message),
        }),
        _ => json!({ "kind": model_error_class(error) }),
    }
}

fn sanitize_provider_text(value: &str) -> String {
    let mut sanitized = value
        .replace("Authorization", "[redacted-header]")
        .replace("authorization", "[redacted-header]")
        .replace("api_key", "[redacted-key]")
        .replace("api-key", "[redacted-key]");
    if sanitized.chars().count() > 512 {
        sanitized = sanitized.chars().take(512).collect();
        sanitized.push_str("...");
    }
    sanitized
}

fn model_error_result(error: &ModelError) -> Value {
    match error {
        ModelError::Http { status, body } => json!({
            "status": status.as_u16(),
            "body": serde_json::from_str::<Value>(body)
                .unwrap_or_else(|_| Value::String(body.clone())),
        }),
        ModelError::Transport(error) => json!({
            "error": "transport",
            "message": sanitize_provider_text(&error.to_string()),
        }),
        ModelError::StreamIdleTimeout { idle_timeout } => json!({
            "error": "stream_idle_timeout",
            "idle_timeout_ms": idle_timeout.as_millis(),
        }),
        ModelError::InvalidStream(_) => json!({"error": "invalid_stream"}),
        ModelError::Refused(message) => json!({"error": "refused", "message": message}),
        ModelError::Incomplete(reason) => json!({"error": "incomplete", "reason": reason}),
        ModelError::MissingOutput => json!({"error": "missing_output"}),
        ModelError::CapabilityProbe(message) => {
            json!({"error": "capability_probe", "message": sanitize_provider_text(message)})
        }
        ModelError::FixtureExhausted => json!({"error": "fixture_exhausted"}),
        ModelError::NativeWebUnavailable
        | ModelError::NativeWebToolNotAllowed
        | ModelError::NativeWebArgumentsInvalid
        | ModelError::NativeWebCitationsMissing
        | ModelError::NativeWebUnsafeCitation { .. }
        | ModelError::NativeWebLimitExceeded => json!({"error": "native_web_contract"}),
        ModelError::EmptyBaseUrl
        | ModelError::EmptyApiKey
        | ModelError::EmptyModel
        | ModelError::EmptyReasoningEffort => json!({"error": "configuration"}),
    }
}

fn model_client_error(error: ModelError, trace: Option<ModelCallTrace>) -> ResearchError {
    let (error_class, message) = match error {
        ModelError::Transport(error) => ("transport", sanitize_provider_text(&error.to_string())),
        ModelError::StreamIdleTimeout { idle_timeout } => (
            "transport",
            format!("response stream idle for {idle_timeout:?}"),
        ),
        ModelError::Http { status, body } if status.as_u16() == 429 => (
            "rate_limited",
            format!("HTTP 429: {}", sanitize_provider_text(&body)),
        ),
        ModelError::Http { status, body } => (
            "transport",
            format!(
                "HTTP {}: {}",
                status.as_u16(),
                sanitize_provider_text(&body)
            ),
        ),
        ModelError::EmptyBaseUrl => ("configuration", "invalid base URL".to_owned()),
        ModelError::EmptyApiKey => ("configuration", "missing API key".to_owned()),
        ModelError::EmptyModel => ("configuration", "missing model name".to_owned()),
        ModelError::EmptyReasoningEffort => {
            ("configuration", "missing reasoning effort".to_owned())
        }
        ModelError::CapabilityProbe(message) => {
            ("configuration", sanitize_provider_text(&message))
        }
        ModelError::InvalidStream(_) => ("invalid_output", "invalid response stream".to_owned()),
        ModelError::Refused(message) => return ResearchError::ModelRefused(message),
        ModelError::Incomplete(reason) => ("invalid_output", format!("incomplete: {reason}")),
        ModelError::NativeWebUnavailable
        | ModelError::NativeWebToolNotAllowed
        | ModelError::NativeWebArgumentsInvalid
        | ModelError::NativeWebCitationsMissing
        | ModelError::NativeWebUnsafeCitation { .. }
        | ModelError::NativeWebLimitExceeded => (
            "native_web_contract",
            "native web contract rejected response".to_owned(),
        ),
        ModelError::MissingOutput => ("invalid_output", "missing model output".to_owned()),
        ModelError::FixtureExhausted => ("transport", "fixture sequence exhausted".to_owned()),
    };
    if let Some(trace) = trace {
        return ResearchError::ModelDebug {
            error_class,
            message,
            trace,
        };
    }
    match error_class {
        "rate_limited" => ResearchError::RateLimited(message),
        "invalid_output" => ResearchError::InvalidOutput(message),
        _ => ResearchError::Model(message),
    }
}

fn model_debug_trace(error: &ResearchError) -> Option<&ModelCallTrace> {
    match error {
        ResearchError::ModelDebug { trace, .. } => Some(trace),
        _ => None,
    }
}

fn logical_now(start: DateTime<Utc>, elapsed: StdDuration) -> DateTime<Utc> {
    start + Duration::from_std(elapsed).unwrap_or_else(|_| Duration::seconds(i64::MAX))
}

fn retryable_model_error(error: &ResearchError, retry: &akzio_domain::RetryPolicy) -> bool {
    match error {
        ResearchError::InvalidOutput(_) | ResearchError::MissingFinalOutput => {
            retry.retry_invalid_output
        }
        ResearchError::Model(_) => retry.retry_transport,
        ResearchError::RateLimited(_) => retry.retry_rate_limited,
        ResearchError::ModelDebug { error_class, .. } if *error_class == "invalid_output" => {
            retry.retry_invalid_output
        }
        ResearchError::ModelDebug { error_class, .. } if *error_class == "transport" => {
            retry.retry_transport
        }
        ResearchError::ModelDebug { error_class, .. } if *error_class == "rate_limited" => {
            retry.retry_rate_limited
        }
        _ => false,
    }
}

fn model_error_class(error: &ResearchError) -> &'static str {
    match error {
        ResearchError::Model(_) => "transport",
        ResearchError::RateLimited(_) => "rate_limited",
        ResearchError::ModelDebug { error_class, .. } => error_class,
        _ => "other",
    }
}

// A narrowing of the installed schema, bound to this immutable manifest. Full
// identities remain the wire and stored representation; no fuzzy ID repair.
fn bind_reference_schema(schema: &mut Value, refs: &[Value]) {
    match schema {
        Value::Object(object) => {
            if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
                let kinds = properties.get("kind").and_then(|k|k.get("enum")).and_then(Value::as_array).cloned();
                if let Some(id) = properties.get_mut("artifact_id") {
                    id["enum"] = Value::Array(refs.iter().filter(|r|kinds.as_ref().is_none_or(|k|k.contains(&r["kind"])))
                        .map(|r|r["artifact_id"].clone()).collect());
                    if kinds.is_some() { properties.remove("kind"); }
                }
                if let Some(basis) = properties.get_mut("basis_artifact_ids") {
                    basis["items"]["enum"] = Value::Array(refs.iter().map(|r|r["artifact_id"].clone()).collect());
                }
                if kinds.is_some() {
                    if let Some(required) = object.get_mut("required").and_then(Value::as_array_mut) {
                        required.retain(|v|v.as_str()!=Some("kind"));
                    }
                }
            }
            for (key,value) in object { if key!="enum" {bind_reference_schema(value, refs);} }
        }
        Value::Array(values) => for value in values { bind_reference_schema(value, refs); },
        _ => {}
    }
}

fn resolve_reference_kinds(value: &mut Value, refs: &[Value]) -> ResearchResult<()> {
    match value {
        Value::Object(object) => {
            if let Some(id)=object.get("artifact_id") {
                if !object.contains_key("kind") {
                    let reference=refs.iter().find(|r|&r["artifact_id"]==id).ok_or_else(||
                        ResearchError::InvalidOutput("reference ID is outside this attempt's Manifest".into()))?;
                    object.insert("kind".into(),reference["kind"].clone());
                }
            }
            for value in object.values_mut() {resolve_reference_kinds(value,refs)?;}
        }
        Value::Array(values) => for value in values {resolve_reference_kinds(value,refs)?;},
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod reference_binding_tests {
    use super::*;
    #[test]
    fn thesis_expiry_requires_timezone_timestamp_not_calendar_date() {
        let schema = decision_proposal_output_schema();
        let expiry = &schema["properties"]["forecasts"]["items"]["properties"]["thesis"]["properties"]["thesis_valid_until"];
        assert!(validate_schema_value(&json!("2026-09-14"), expiry, "$").is_err());
        assert!(validate_schema_value(&json!("2026-09-14T20:00:00Z"), expiry, "$").is_ok());
        assert!(validate_schema_value(&json!("2026-09-14T16:00:00-04:00"), expiry, "$").is_ok());
    }
    #[test]
    fn exact_reference_schema_is_manifest_bound_and_rejects_truncation() {
        let id = "a".repeat(64);
        let mut schema = json!({"type":"object","properties":{"artifact_id":{"type":"string"}},"required":["artifact_id"]});
        bind_reference_schema(&mut schema, &[json!({"artifact_id":id,"kind":"normalized_evidence"})]);

        assert!(validate_schema_value(&json!({"artifact_id":id}), &schema, "$").is_ok());
        assert!(validate_schema_value(&json!({"artifact_id":"a".repeat(63)}), &schema, "$").is_err());
        assert!(validate_schema_value(&json!({"artifact_id":"b".repeat(64)}), &schema, "$").is_err());
    }
}

#[cfg(test)]
mod cumulative_budget_tests {
    use super::*;
    #[test]
    fn over_budget_request_never_authorizes_a_provider_turn() {
        let policy = TaskBudget { max_input_tokens: 48000, max_output_tokens: 5000, max_wall_time_secs: 120, max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(2) };
        let mut budget = AgentRunBudget::new(&policy, &RetryPolicy::none());
        budget.record_input(30825).unwrap();
        assert!(matches!(budget.authorize_model_call(17236), Err(ResearchError::InputBudgetExceeded { actual: 48061, maximum: 48000 })));
        assert_eq!(budget.model_calls, 0);
        assert_eq!(budget.input_tokens, 30825);
    }

    #[test]
    fn failed_provider_turn_remains_charged_before_repair() {
        let policy = TaskBudget { max_input_tokens: 48000, max_output_tokens: 6000, max_wall_time_secs: 120, max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(4) };
        let mut budget = AgentRunBudget::new(&policy, &RetryPolicy::none());
        budget.record_turn(10000, 1000, None).unwrap();
        budget.record_failed_turn(14000).unwrap();
        assert_eq!(budget.input_tokens, 24000);
        budget.check_input(16000).unwrap();
        assert_eq!(budget.input_tokens, 24000);
        budget.record_turn(16000, 1000, None).unwrap();
        assert_eq!(budget.input_tokens, 40000);
        assert!(budget.check_input(8001).is_err());
    }

    #[test]
    fn reservation_is_not_a_charge_and_repair_overflow_is_rejected() {
        let policy=TaskBudget {max_input_tokens:48000,max_output_tokens:6000,max_wall_time_secs:120,max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(4)};
        let mut budget=AgentRunBudget::new(&policy,&RetryPolicy::none());
        budget.check_input(8117 * 2).unwrap();
        assert_eq!(budget.input_tokens,0);
        for observed in [10662,14112,16093] {budget.record_input(observed).unwrap();}
        assert_eq!(budget.input_tokens,40867);
        assert!(matches!(budget.check_input(15987),Err(ResearchError::InputBudgetExceeded{actual:56854,maximum:48000})));
        assert_eq!(budget.input_tokens,40867);
    }
}

#[cfg(test)]
mod wire_reference_tests {
    use super::*;
    #[test]
    fn wire_ids_are_kind_filtered_and_resolve_without_widening_authority() {
        let refs=vec![json!({"artifact_id":"a".repeat(64),"kind":"normalized_evidence"}),json!({"artifact_id":"b".repeat(64),"kind":"claim"})];
        let mut schema=artifact_ref_schema(&["normalized_evidence","semantic_detail"]);
        bind_reference_schema(&mut schema,&refs);
        assert!(validate_schema_value(&json!({"artifact_id":"b".repeat(64)}),&schema,"$").is_err());
        let mut value=json!({"artifact_id":"a".repeat(64)});
        assert!(validate_schema_value(&value,&schema,"$").is_ok());
        resolve_reference_kinds(&mut value,&refs).unwrap();
        assert_eq!(value,refs[0]);
        assert!(resolve_reference_kinds(&mut json!({"artifact_id":"a".repeat(63)}),&refs).is_err());
        // Explicit wrong kinds are never silently corrected; original validation rejects them.
        let mut wrong=json!({"artifact_id":"a".repeat(64),"kind":"claim"});
        resolve_reference_kinds(&mut wrong,&refs).unwrap();
        assert_eq!(wrong["kind"],"claim");
    }
}
