use std::hash::{Hash, Hasher};

pub(super) fn is_transient_role_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    // Permanent request/context errors must not burn role retries.
    // Do not treat bare "llm stream failed" wrappers as transient — that
    // previously retried context-window-full 400s after stream retries finished.
    if is_permanent_role_error_text(&text) {
        return false;
    }
    text.contains("503")
        || text.contains("502")
        || text.contains("429")
        || text.contains("bad_response_status_code")
        || text.contains("no healthy upstream")
        || text.contains("timeout")
        || text.contains("timed out")
        || text.contains("connection reset")
        || text.contains("transport error")
        || text.contains("error decoding response body")
        || text.contains("temporarily unavailable")
        || text.contains("without a terminal finish_reason")
        || text.contains("internal_server_error")
        || text.contains("\"type\":\"server_error\"")
        || text.contains("upstream_error")
        || text.contains("upstream request failed")
}

fn is_permanent_role_error_text(text: &str) -> bool {
    text.contains("context window is full")
        || text.contains("reduce conversation history")
        || text.contains("invalid_request_error")
        || text.contains("请精简对话历史")
        || text.contains("context window")
        || text.contains("max_agent_loops")
        || (text.contains("400")
            && (text.contains("invalid_request")
                || text.contains("context")
                || text.contains("too large")
                || text.contains("token")))
}

fn role_retry_jitter_ms(role: &str, attempt: usize) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    role.hash(&mut hasher);
    attempt.hash(&mut hasher);
    hasher.finish() % 251
}

pub(super) fn backoff_ms(role: &str, attempt: usize) -> u64 {
    1_000u64 * attempt as u64 + role_retry_jitter_ms(role, attempt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_window_full_is_not_transient_role_error() {
        let message = "LLM stream chunk failed: InvalidStatusCodeWithMessage(400, \
            \"{\\\"error\\\":{\\\"message\\\":\\\"Context window is full — reduce conversation history\\\",\\\"type\\\":\\\"invalid_request_error\\\"}}\")";
        assert!(!is_transient_role_error(message));
        assert!(is_permanent_role_error_text(&message.to_ascii_lowercase()));
    }

    #[test]
    fn bare_stream_wrapper_is_not_transient_without_upstream_marker() {
        // Outer wrapper alone used to retry permanent 400s after chain was lost.
        assert!(!is_transient_role_error("LLM stream chunk failed"));
    }

    #[test]
    fn gateway_502_is_transient_role_error() {
        let message = "LLM stream chunk failed: InvalidStatusCodeWithMessage(502, \
            \"{\\\"error\\\":{\\\"message\\\":\\\"Upstream request failed\\\",\\\"type\\\":\\\"upstream_error\\\"}}\")";
        assert!(is_transient_role_error(message));
    }

    #[test]
    fn gateway_internal_server_error_is_transient_role_error() {
        let message = "Chat Completions stream chunk failed: failed to deserialize api response: \
            error:missing field `id` content:{\"error\":{\"message\":\"stream error\",\"type\":\"server_error\",\"code\":\"internal_server_error\"}}";
        assert!(is_transient_role_error(message));
    }

    #[test]
    fn stream_transport_decode_error_is_transient_role_error() {
        assert!(is_transient_role_error(
            "Chat Completions stream chunk failed: stream failed: EventStream error: Transport error: error decoding response body"
        ));
    }

    #[test]
    fn missing_chat_terminal_finish_is_transient_role_error() {
        assert!(is_transient_role_error(
            "Chat Completions stream ended without a terminal finish_reason after 1121 chunks"
        ));
    }
}
