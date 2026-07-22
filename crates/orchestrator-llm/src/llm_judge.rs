use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Default model for the judge call. Uses a fast, cheap model.
pub const DEFAULT_JUDGE_MODEL: &str = "gpt-4o-mini";

/// Classification prompt for the LLM judge.
/// Kept minimal to reduce token usage (~100 input tokens + ~5 output tokens).
pub const JUDGE_PROMPT_TEMPLATE: &str =
    include_str!("../../../prompts/system/judge.md");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JudgeConfig {
    pub enabled: bool,
    pub model: String,
    pub max_messages_per_turn: usize,
}

impl Default for JudgeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            model: DEFAULT_JUDGE_MODEL.to_string(),
            max_messages_per_turn: 3,
        }
    }
}

/// Classify an assistant message as "final" or "stall" using a lightweight LLM call.
/// Returns:
/// - Ok(true) if the message is a stall (needs follow-up)
/// - Ok(false) if the message is final (does not need follow-up)
/// - Err(...) if the LLM call fails (caller should fall back to default behavior)
pub async fn judge_message_status(
    message: &str,
    llm_gateway_base_url: &str,
    llm_gateway_api_key: &str,
    model: &str,
) -> Result<bool> {
    let prompt = JUDGE_PROMPT_TEMPLATE.replace("{message}", message);

    let client = reqwest::Client::new();
    let response = client
        .post(format!(
            "{}/chat/completions",
            llm_gateway_base_url.trim_end_matches('/')
        ))
        .header("Authorization", format!("Bearer {llm_gateway_api_key}"))
        .json(&json!({
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 10,
            "temperature": 0.0,
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;

    parse_judge_response(&response)
}

fn parse_judge_response(response: &Value) -> Result<bool> {
    let classification = response
        .get("choices")
        .and_then(|choices| choices.get(0))
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();

    match classification.as_str() {
        "final" => Ok(false),
        "stall" => Ok(true),
        _ => bail!("LLM judge returned unexpected classification: '{classification}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn judge_prompt_contains_message() {
        let prompt = JUDGE_PROMPT_TEMPLATE.replace("{message}", "test message here");
        assert!(prompt.contains("test message here"));
        assert!(prompt.contains("final"));
        assert!(prompt.contains("stall"));
    }

    #[test]
    fn parse_judge_response_reads_final_and_stall() {
        assert!(!parse_judge_response(&json!({
            "choices": [{"message": {"content": "final"}}]
        }))
        .unwrap());
        assert!(parse_judge_response(&json!({
            "choices": [{"message": {"content": " stall "}}]
        }))
        .unwrap());
    }

    #[test]
    fn parse_judge_response_rejects_unexpected_classification() {
        assert!(parse_judge_response(&json!({
            "choices": [{"message": {"content": "maybe"}}]
        }))
        .is_err());
    }

    #[tokio::test]
    async fn judge_message_status_reads_mock_http_final_response() {
        let base_url = spawn_one_response_server(json!({
            "choices": [{"message": {"content": "final"}}]
        }))
        .await;

        assert!(
            !judge_message_status("done", &base_url, "test-key", "test-model")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn judge_message_status_reads_mock_http_stall_response() {
        let base_url = spawn_one_response_server(json!({
            "choices": [{"message": {"content": "stall"}}]
        }))
        .await;

        assert!(
            judge_message_status("let me check", &base_url, "test-key", "test-model")
                .await
                .unwrap()
        );
    }

    async fn spawn_one_response_server(response_json: Value) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0; 4096];
            let _ = socket.read(&mut buffer).await.unwrap();
            let body = response_json.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://{addr}")
    }
}
