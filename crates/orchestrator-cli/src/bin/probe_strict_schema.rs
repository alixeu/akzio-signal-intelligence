//! One-shot probe: does the configured LLM gateway accept OpenAI Responses
//! structured output (`text.format` = `json_schema`, `strict: true`)?
//!
//! This answers audit item O4 (provider-side strict schema) without changing
//! the production request path. It sends TWO minimal requests to
//! `${LLM_GATEWAY_BASE_URL}/responses`:
//!
//! - a plain request (baseline — confirms creds/model/endpoint work)
//! - the same request plus a strict json_schema `text.format`
//!
//! and reports whether the strict request is accepted or rejected.
//!
//! Requires env: LLM_GATEWAY_API_KEY (and optionally LLM_GATEWAY_BASE_URL,
//! LLM_PROBE_MODEL). Nothing is persisted; output goes to stdout.
//!
//! Run: `cargo run -p orchestrator-cli --bin probe-strict-schema`

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let base_url = std::env::var("LLM_GATEWAY_BASE_URL")
        .unwrap_or_else(|_| "https://oneapi-comate.baidu-int.com/v1".to_string());
    let api_key = std::env::var("LLM_GATEWAY_API_KEY")
        .context("LLM_GATEWAY_API_KEY must be set to probe the gateway")?;
    if api_key.trim().is_empty() {
        bail!("LLM_GATEWAY_API_KEY is empty");
    }
    let model = std::env::var("LLM_PROBE_MODEL").unwrap_or_else(|_| "gpt-5.5".to_string());
    let endpoint = format!("{}/responses", base_url.trim_end_matches('/'));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;

    println!("Probing gateway strict-schema support");
    println!("  endpoint: {endpoint}");
    println!("  model:    {model}\n");

    // 1) Baseline plain request.
    let plain = json!({
        "model": model,
        "input": "Reply with the single word: ok",
    });
    let baseline = post(&client, &endpoint, &api_key, &plain).await;
    match &baseline {
        Ok((status, _)) if status.is_success() => {
            println!("[1/2] baseline plain request: OK ({status})");
        }
        Ok((status, body)) => {
            println!("[1/2] baseline plain request: FAILED ({status})");
            println!("      body: {}", truncate(body, 500));
            println!("\nGateway/creds/model do not work even for a plain request; fix that before judging strict-schema support.");
            return Ok(());
        }
        Err(e) => {
            println!("[1/2] baseline plain request: transport error: {e}");
            return Ok(());
        }
    }

    // 2) Strict json_schema request.
    let schema = json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "direction": {"type": "string", "enum": ["bullish", "bearish", "neutral"]},
            "confidence": {"type": "number"}
        },
        "required": ["direction", "confidence"]
    });
    let strict = json!({
        "model": model,
        "input": "Return a JSON object with direction and confidence for a neutral test.",
        "text": {
            "format": {
                "type": "json_schema",
                "name": "probe_schema",
                "strict": true,
                "schema": schema
            }
        }
    });
    match post(&client, &endpoint, &api_key, &strict).await {
        Ok((status, body)) if status.is_success() => {
            println!("[2/2] strict json_schema request: ACCEPTED ({status})");
            println!("\nVERDICT: gateway appears to SUPPORT strict structured output.");
            println!("Next step: wire `text.format` into additional_params() in orchestrator-llm and gate it behind a per-role flag.");
            if let Some(sample) = extract_output_text(&body) {
                println!("  sample output: {}", truncate(&sample, 200));
            }
        }
        Ok((status, body)) => {
            println!("[2/2] strict json_schema request: REJECTED ({status})");
            println!("      body: {}", truncate(&body, 800));
            println!("\nVERDICT: gateway does NOT accept this strict-schema shape.");
            println!("Keep the current approach (prompt-injected schema + runtime validation + observable degrade). Do not wire text.format.");
        }
        Err(e) => {
            println!("[2/2] strict json_schema request: transport error: {e}");
        }
    }

    Ok(())
}

async fn post(
    client: &reqwest::Client,
    endpoint: &str,
    api_key: &str,
    body: &Value,
) -> Result<(reqwest::StatusCode, String)> {
    let resp = client
        .post(endpoint)
        .bearer_auth(api_key)
        .json(body)
        .send()
        .await
        .context("request failed")?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    Ok((status, text))
}

fn extract_output_text(body: &str) -> Option<String> {
    let value: Value = serde_json::from_str(body).ok()?;
    // Responses API: output[].content[].text
    value
        .get("output")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("content"))
        .and_then(Value::as_array)
        .and_then(|c| c.first())
        .and_then(|c| c.get("text"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        text.to_string()
    } else {
        format!("{}…", &text[..max])
    }
}
