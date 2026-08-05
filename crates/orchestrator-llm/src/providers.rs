use anyhow::{bail, Context, Result};
use async_openai::{config::OpenAIConfig, Client as OpenAIClient};
use tracing::debug;
use uuid::Uuid;

use super::RoleLlmSettings;

/// Fixed endpoint, credentials, and model for the free opencode Zen gateway.
/// When a role sets `free_opencode`, the configured gateway base_url / api_key
/// and model are ignored in favor of these values (chat_completions only).
const FREE_OPENCODE_BASE_URL: &str = "https://opencode.ai/zen/v1";
const FREE_OPENCODE_API_KEY: &str = "public";
const FREE_OPENCODE_MODEL: &str = "deepseek-v4-flash-free";

pub(super) fn validate_configuration(settings: &RoleLlmSettings, role: &str) -> Result<()> {
    // free_opencode pins base_url / api_key to the opencode Zen gateway, so
    // the configured OpenAI-compatible endpoint credentials are not required.
    if !settings.free_opencode
        && settings
            .base_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
    {
        bail!("LLM config for role {role:?} requires base_url for openai_compatible");
    }
    let has_api_key = settings
        .api_key
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    if !settings.free_opencode && !has_api_key {
        bail!("LLM config for role {role:?} requires api_key for openai_compatible");
    }
    Ok(())
}

pub(super) fn effective_model(settings: &RoleLlmSettings) -> &str {
    if settings.free_opencode {
        FREE_OPENCODE_MODEL
    } else {
        settings.model.as_str()
    }
}

pub(super) fn openai_compatible_responses_client(
    settings: &RoleLlmSettings,
) -> Result<OpenAIClient<OpenAIConfig>> {
    if settings.free_opencode {
        return free_opencode_client();
    }
    let api_key = openai_compatible_api_key(settings)?;
    let base_url = openai_compatible_base_url(settings)?;
    debug!(
        base_url = %base_url,
        model = %settings.model,
        "creating OpenAI-compatible responses client"
    );
    let config = OpenAIConfig::new()
        .with_api_key(api_key)
        .with_api_base(base_url);
    Ok(OpenAIClient::with_config(config))
}

fn openai_compatible_api_key(settings: &RoleLlmSettings) -> Result<String> {
    if let Some(api_key) = settings
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(api_key.to_string());
    }
    bail!("api_key is required for OpenAI-compatible provider")
}

fn openai_compatible_base_url(settings: &RoleLlmSettings) -> Result<&str> {
    settings
        .base_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("base_url is required for OpenAI-compatible provider")
}

/// Build a client for the free opencode Zen gateway. Every documented header
/// must be present or the gateway rejects the request: Authorization comes from
/// the fixed public api key, Content-Type is added per JSON request by the
/// client, and the remaining opencode headers are attached here.
fn free_opencode_client() -> Result<OpenAIClient<OpenAIConfig>> {
    let session = format!("sess_{}", Uuid::new_v4().simple());
    let request_id = format!("msg_{}", Uuid::new_v4().simple());
    debug!(
        base_url = %FREE_OPENCODE_BASE_URL,
        model = %FREE_OPENCODE_MODEL,
        "creating free opencode Zen chat completions client"
    );
    let config = OpenAIConfig::new()
        .with_api_base(FREE_OPENCODE_BASE_URL)
        .with_api_key(FREE_OPENCODE_API_KEY)
        .with_header("x-opencode-project", "proj_akzio_signal")
        .and_then(|config| config.with_header("x-opencode-session", session.as_str()))
        .and_then(|config| config.with_header("x-opencode-request", request_id.as_str()))
        .and_then(|config| config.with_header("x-opencode-client", "cli"))
        .and_then(|config| config.with_header("Accept", "text/event-stream"))
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context("failed to set free opencode gateway headers")?;
    Ok(OpenAIClient::with_config(config))
}
