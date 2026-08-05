use anyhow::{bail, Context, Result};
use async_openai::{
    config::OpenAIConfig,
    middleware::{retry::OpenAIRetryLayer, ReqwestService},
    Client as OpenAIClient,
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};
use tracing::debug;
use uuid::Uuid;

use super::{LlmRoute, RoleLlmSettings};

/// Fixed endpoint, credentials, and model for the free opencode Zen gateway.
/// When a role sets `free_opencode`, the configured gateway base_url / api_key
/// and model are ignored in favor of these values (chat_completions only).
const FREE_OPENCODE_BASE_URL: &str = "https://opencode.ai/zen/v1";
const FREE_OPENCODE_API_KEY: &str = "public";
const FREE_OPENCODE_MODEL: &str = "deepseek-v4-flash-free";

/// Transport identity for a standard OpenAI-compatible client.  The model is
/// intentionally absent: it is a request field and does not change the HTTP
/// connection pool or retry policy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ProviderClientKey {
    pub base_url: String,
    pub auth_fingerprint: String,
    pub route: LlmRoute,
    pub compatibility: ProviderCompatibility,
    pub retry_policy: RetryPolicyKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum ProviderCompatibility {
    OpenAiStandard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum RetryPolicyKey {
    OpenAiRetries2,
}

type StandardClient = OpenAIClient<OpenAIConfig>;
type ClientRegistry = HashMap<ProviderClientKey, StandardClient>;

static STANDARD_CLIENTS: OnceLock<Mutex<ClientRegistry>> = OnceLock::new();

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
    let base_url = openai_compatible_base_url(settings)?.to_owned();
    let key = ProviderClientKey {
        base_url: base_url.clone(),
        auth_fingerprint: auth_fingerprint(&api_key),
        route: settings.effective_route(),
        compatibility: ProviderCompatibility::OpenAiStandard,
        retry_policy: RetryPolicyKey::OpenAiRetries2,
    };

    let registry = STANDARD_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut clients = registry
        .lock()
        .map_err(|_| anyhow::anyhow!("standard provider client registry poisoned"))?;
    if let Some(client) = clients.get(&key) {
        return Ok(client.clone());
    }

    debug!(
        base_url = %base_url,
        model = %settings.model,
        route = ?settings.effective_route(),
        "creating standard OpenAI-compatible provider client"
    );
    let config = OpenAIConfig::new()
        .with_api_key(api_key)
        .with_api_base(base_url.clone());

    // Installing a custom service replaces async-openai's default executor,
    // so this is the only HTTP retry layer for standard clients.  `new(2)`
    // means two additional attempts after the initial request (three HTTP
    // attempts total per SSE open).
    let service = tower::ServiceBuilder::new()
        .layer(OpenAIRetryLayer::new(2))
        .service(ReqwestService::new(reqwest::Client::new()));
    let client = OpenAIClient::with_config(config).with_http_service(service);
    clients.insert(key, client.clone());
    Ok(client)
}

fn auth_fingerprint(api_key: &str) -> String {
    let digest = Sha256::digest(api_key.as_bytes());
    format!("{digest:x}")
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
