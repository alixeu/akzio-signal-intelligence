fn should_advertise_read_tools(
    purpose: RunPurpose,
    context_len: usize,
    max_tool_calls: u16,
) -> bool {
    context_len > 0
        && context_len <= usize::from(max_tool_calls)
        && purpose != RunPurpose::PaperDryRun
}

fn estimate_tokens<T: Serialize>(value: &T) -> ResearchResult<u32> {
    Ok(akzio_domain::estimate_json_tokens(value)?)
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
        ModelError::InvalidStream(_) => json!({"error": "invalid_stream"}),
        ModelError::Refused(message) => json!({"error": "refused", "message": message}),
        ModelError::Incomplete(reason) => json!({"error": "incomplete", "reason": reason}),
        ModelError::MissingOutput => json!({"error": "missing_output"}),
        ModelError::FixtureExhausted => json!({"error": "fixture_exhausted"}),
        ModelError::NativeWebUnavailable
        | ModelError::NativeWebToolNotAllowed
        | ModelError::NativeWebArgumentsInvalid
        | ModelError::NativeWebCitationsMissing
        | ModelError::NativeWebUnsafeCitation
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
        ModelError::InvalidStream(_) => ("invalid_output", "invalid response stream".to_owned()),
        ModelError::Refused(message) => return ResearchError::ModelRefused(message),
        ModelError::Incomplete(reason) => ("invalid_output", format!("incomplete: {reason}")),
        ModelError::NativeWebUnavailable
        | ModelError::NativeWebToolNotAllowed
        | ModelError::NativeWebArgumentsInvalid
        | ModelError::NativeWebCitationsMissing
        | ModelError::NativeWebUnsafeCitation
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
