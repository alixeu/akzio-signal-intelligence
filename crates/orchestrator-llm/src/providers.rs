use anyhow::{bail, Context, Result};
use async_openai::{
    config::OpenAIConfig,
    error::OpenAIError,
    middleware::{retry::OpenAIRetryLayer, HttpRequestFactory},
    Client as OpenAIClient,
};
use orchestrator_core::default_project_root;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context as TaskContext, Poll},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tower::Service;
use tracing::{debug, enabled, Level};
use uuid::Uuid;

use super::{LlmRoute, RoleLlmSettings};

/// Fixed endpoint, credentials, and model for the free opencode Zen gateway.
/// When a role sets `free_opencode`, the configured gateway base_url / api_key
/// and model are ignored in favor of these values (chat_completions only).
const FREE_OPENCODE_BASE_URL: &str = "https://opencode.ai/zen/v1";
const FREE_OPENCODE_API_KEY: &str = "public";
const FREE_OPENCODE_MODEL: &str = "deepseek-v4-flash-free";
const HTTP_TRACE_TARGET: &str = "orchestrator_llm::http";
const MAX_DEBUG_PAYLOAD_CHARS: usize = 24 * 1024;

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

/// The SDK's middleware boundary is the only place where the rebuilt request
/// and the untouched streaming response are both visible.  Keep this service
/// deliberately small: it logs a redacted request payload and response
/// metadata, then hands the response body back to async-openai unchanged.
#[derive(Clone)]
struct HttpDebugService {
    client: reqwest::Client,
    attempt_sequence: Arc<AtomicU64>,
}

impl HttpDebugService {
    fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            attempt_sequence: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Service<HttpRequestFactory> for HttpDebugService {
    type Response = reqwest::Response;
    type Error = OpenAIError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, factory: HttpRequestFactory) -> Self::Future {
        let client = self.client.clone();
        let attempt_sequence = self.attempt_sequence.clone();
        Box::pin(async move {
            let request = factory.build().await?;
            let attempt = attempt_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let started = Instant::now();
            let method = request.method().to_string();
            let url = safe_url(request.url());
            let request_fingerprint = request_fingerprint(&request);
            let http_debug = enabled!(target: HTTP_TRACE_TARGET, Level::DEBUG);
            let request_context = http_debug.then(|| debug_request_context(&request));
            let phase = request_context
                .as_ref()
                .map(|context| context.phase.clone())
                .unwrap_or_else(|| "phase-unknown".to_owned());
            let role = request_context
                .as_ref()
                .and_then(|context| context.role.clone());
            let profile = request_context
                .as_ref()
                .and_then(|context| context.profile.clone());
            let file_stem = debug_file_stem(role.as_deref(), profile.as_deref());
            let request_body = request_context
                .as_ref()
                .map(|context| context.body.as_str());

            if http_debug {
                debug!(
                    target: HTTP_TRACE_TARGET,
                    direction = "request",
                    attempt,
                    method = %method,
                    url = %url,
                    request_fingerprint = %request_fingerprint,
                    request_body = request_body.unwrap_or("<unavailable>"),
                    "async-openai HTTP request"
                );
                append_file_debug_record(
                    &phase,
                    &file_stem,
                    json!({
                        "kind": "http",
                        "phase_dir": &phase,
                        "file_stem": &file_stem,
                        "direction": "request",
                        "attempt": attempt,
                        "method": &method,
                        "url": &url,
                        "request_fingerprint": &request_fingerprint,
                        "role": role.as_deref(),
                        "request_body": request_body,
                    }),
                );
            }

            let result = client.execute(request).await.map_err(OpenAIError::Reqwest);
            match &result {
                Ok(response) => {
                    if http_debug {
                        debug!(
                            target: HTTP_TRACE_TARGET,
                            direction = "response",
                            attempt,
                            method = %method,
                            url = %url,
                            request_fingerprint = %request_fingerprint,
                            status = %response.status(),
                            content_type = response
                                .headers()
                                .get(reqwest::header::CONTENT_TYPE)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or("<missing>"),
                            content_length = response.content_length().unwrap_or_default(),
                            request_id = response_request_id(response).unwrap_or("<missing>"),
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "async-openai HTTP response headers"
                        );
                        append_file_debug_record(
                            &phase,
                            &file_stem,
                            json!({
                                "kind": "http",
                                "phase_dir": &phase,
                                "file_stem": &file_stem,
                                "direction": "response",
                                "attempt": attempt,
                                "method": &method,
                                "url": &url,
                                "request_fingerprint": &request_fingerprint,
                                "role": role.as_deref(),
                                "status": response.status().as_u16(),
                                "content_type": response
                                    .headers()
                                    .get(reqwest::header::CONTENT_TYPE)
                                    .and_then(|value| value.to_str().ok()),
                                "content_length": response.content_length(),
                                "request_id": response_request_id(response),
                                "elapsed_ms": started.elapsed().as_millis() as u64,
                            }),
                        );
                    }
                }
                Err(_) => {
                    debug!(
                        target: HTTP_TRACE_TARGET,
                        direction = "response",
                        attempt,
                        method = %method,
                        url = %url,
                        request_fingerprint = %request_fingerprint,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "async-openai HTTP transport failed"
                    );
                    append_file_debug_record(
                        &phase,
                        &file_stem,
                        json!({
                            "kind": "http",
                            "phase_dir": &phase,
                            "file_stem": &file_stem,
                            "direction": "response",
                            "attempt": attempt,
                            "method": &method,
                            "url": &url,
                            "request_fingerprint": &request_fingerprint,
                            "role": role.as_deref(),
                            "status": "transport_error",
                            "elapsed_ms": started.elapsed().as_millis() as u64,
                        }),
                    );
                }
            }
            result
        })
    }
}

fn http_debug_service() -> HttpDebugService {
    HttpDebugService::new(reqwest::Client::new())
}

fn client_with_config(config: OpenAIConfig) -> OpenAIClient<OpenAIConfig> {
    let service = tower::ServiceBuilder::new()
        .layer(OpenAIRetryLayer::new(2))
        .service(http_debug_service());
    OpenAIClient::with_config(config).with_http_service(service)
}

fn safe_url(url: &reqwest::Url) -> String {
    let host = url.host_str().unwrap_or("<no-host>");
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    format!("{}://{}{}{}", url.scheme(), host, port, url.path())
}

fn response_request_id(response: &reqwest::Response) -> Option<&str> {
    ["x-request-id", "request-id", "trace-id"]
        .into_iter()
        .find_map(|name| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
        })
}

fn request_fingerprint(request: &reqwest::Request) -> String {
    let mut digest = Sha256::new();
    digest.update(request.method().as_str().as_bytes());
    digest.update(request.url().path().as_bytes());
    if let Some(body) = request.body().and_then(reqwest::Body::as_bytes) {
        digest.update(body);
    }
    let digest = digest.finalize();
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct DebugRequestContext {
    body: String,
    phase: String,
    role: Option<String>,
    profile: Option<String>,
}

fn debug_request_context(request: &reqwest::Request) -> DebugRequestContext {
    let Some(bytes) = request.body().and_then(reqwest::Body::as_bytes) else {
        return DebugRequestContext {
            body: "<unavailable>".to_owned(),
            phase: "phase-unknown".to_owned(),
            role: None,
            profile: None,
        };
    };
    let mut value = serde_json::from_slice::<Value>(bytes)
        .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(bytes)}));
    let (phase, role, profile) = value
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(parse_prompt_cache_key)
        .unwrap_or_else(|| ("phase-unknown".to_owned(), None, None));
    redact_debug_value(&mut value);
    DebugRequestContext {
        body: truncate_debug_payload(
            serde_json::to_string(&value).unwrap_or_else(|_| "<unserializable>".to_owned()),
        ),
        phase,
        role,
        profile,
    }
}

fn parse_prompt_cache_key(value: &str) -> (String, Option<String>, Option<String>) {
    let Some(value) = value.strip_prefix("akzio:p") else {
        return ("phase-unknown".to_owned(), None, None);
    };
    let (phase_value, role_value) = value.split_once(':').unwrap_or((value, ""));
    let phase = phase_value
        .parse::<i64>()
        .ok()
        .filter(|phase| *phase >= 0)
        .map(|phase| format!("phase{phase}"))
        .unwrap_or_else(|| "phase-unknown".to_owned());
    let role = role_value
        .split_once(':')
        .map(|(role, _)| role)
        .unwrap_or(role_value)
        .trim();
    let role = (!role.is_empty()).then(|| role.to_owned());
    let profile = role_value
        .split_once(':')
        .map(|(_, profile)| profile)
        .map(str::trim)
        .filter(|profile| !profile.is_empty())
        .map(ToOwned::to_owned);
    (phase, role, profile)
}

pub(super) fn debug_typed_payload<T: serde::Serialize>(payload: &T) -> String {
    let mut value = serde_json::to_value(payload).unwrap_or_else(|_| json!("<unserializable>"));
    redact_debug_value(&mut value);
    truncate_debug_payload(
        serde_json::to_string(&value).unwrap_or_else(|_| "<unserializable>".to_owned()),
    )
}

fn redact_debug_value(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, child) in object.iter_mut() {
                if is_sensitive_key(key) {
                    *child = Value::String("[REDACTED]".to_owned());
                } else {
                    redact_debug_value(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_debug_value),
        _ => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "authorization"
            | "api_key"
            | "api-key"
            | "x-api-key"
            | "cookie"
            | "set-cookie"
            | "access_token"
            | "access-token"
            | "encrypted_content"
            | "secret"
            | "token"
    )
}

pub(super) fn truncate_debug_payload(payload: String) -> String {
    if payload.len() <= MAX_DEBUG_PAYLOAD_CHARS {
        return payload;
    }
    let mut truncated = payload
        .chars()
        .take(MAX_DEBUG_PAYLOAD_CHARS)
        .collect::<String>();
    truncated.push_str("...[truncated]");
    truncated
}

static DEBUG_FILE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(super) fn append_file_debug_record(phase: &str, file_stem: &str, record: Value) {
    if let Err(error) =
        append_file_debug_record_at(&default_project_root(), phase, file_stem, record)
    {
        tracing::warn!(
            target: HTTP_TRACE_TARGET,
            error = %error,
            "failed to persist async-openai debug record"
        );
    }
}

pub(super) fn debug_phase_directory(phase: Option<i64>) -> String {
    phase
        .filter(|phase| *phase >= 0)
        .map(|phase| format!("phase{phase}"))
        .unwrap_or_else(|| "phase-unknown".to_owned())
}

pub(super) fn debug_file_stem(role: Option<&str>, profile: Option<&str>) -> String {
    if role == Some("mediator.topic") {
        return match profile {
            Some("researcher_warmup") => "warmup".to_owned(),
            Some("topic_generation") => "topic_generator".to_owned(),
            _ => "topic".to_owned(),
        };
    }
    let role = role.unwrap_or("provider");
    let stem = role
        .split_once('.')
        .map(|(_, rest)| rest)
        .unwrap_or(role)
        .replace('.', "_");
    sanitize_debug_file_stem(&stem)
}

fn sanitize_debug_file_stem(stem: &str) -> String {
    let stem: String = stem
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if stem.is_empty() {
        "provider".to_owned()
    } else {
        stem
    }
}

fn append_file_debug_record_at(
    root: &Path,
    phase: &str,
    file_stem: &str,
    record: Value,
) -> Result<()> {
    let phase = if phase.starts_with("phase") {
        phase
    } else {
        "phase-unknown"
    };
    let path = root
        .join("outputs/debug")
        .join(phase)
        .join(format!("{}.jsonl", sanitize_debug_file_stem(file_stem)));
    let lock = DEBUG_FILE_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock
        .lock()
        .map_err(|_| anyhow::anyhow!("async-openai debug file lock was poisoned"))?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create debug directory {}", parent.display()))?;
    }
    let mut record = record;
    if let Some(object) = record.as_object_mut() {
        object
            .entry("ts_ms".to_owned())
            .or_insert_with(|| json!(debug_now_ms()));
    }
    let mut line = serde_json::to_string(&record)?;
    line.push('\n');
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open debug file {}", path.display()))?
        .write_all(line.as_bytes())
        .with_context(|| format!("failed to append debug file {}", path.display()))
}

fn debug_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

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
    let client = client_with_config(config);
    clients.insert(key, client.clone());
    Ok(client)
}

pub(super) fn openai_compatible_contract_client(
    base_url: &str,
    api_key: &str,
) -> Result<OpenAIClient<OpenAIConfig>> {
    if base_url.trim().is_empty() {
        bail!("provider base_url is empty");
    }
    if api_key.trim().is_empty() {
        bail!("provider api_key is empty");
    }
    let config = OpenAIConfig::new()
        .with_api_key(api_key.to_owned())
        .with_api_base(base_url.to_owned());
    Ok(client_with_config(config))
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
    Ok(client_with_config(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_payload_redacts_credentials_and_has_a_bound() {
        let payload = json!({
            "api_key": "secret",
            "messages": [{"role": "user", "content": "hello"}],
            "token": "also-secret"
        });
        let rendered = debug_typed_payload(&payload);
        assert!(!rendered.contains("secret"));
        assert!(rendered.contains("[REDACTED]"));
    }

    #[test]
    fn safe_url_drops_query_parameters() {
        let url = reqwest::Url::parse("https://gateway.example/v1/responses?api_key=secret")
            .expect("test URL should parse");
        assert_eq!(safe_url(&url), "https://gateway.example/v1/responses");
    }

    #[test]
    fn request_context_extracts_phase_and_role_without_leaking_tokens() {
        let request = reqwest::Client::new()
            .post("https://gateway.example/v1/responses")
            .json(&json!({
                "prompt_cache_key": "akzio:p2:researcher.bull:debate",
                "token": "secret",
            }))
            .build()
            .unwrap();

        let context = debug_request_context(&request);
        assert_eq!(context.phase, "phase2");
        assert_eq!(context.role.as_deref(), Some("researcher.bull"));
        assert_eq!(context.profile.as_deref(), Some("debate"));
        assert!(!context.body.contains("secret"));
        assert!(context.body.contains("[REDACTED]"));
    }

    #[test]
    fn file_debug_record_is_jsonl_under_the_phase_directory() {
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(debug_phase_directory(Some(3)), "phase3");
        assert_eq!(debug_phase_directory(None), "phase-unknown");
        assert_eq!(
            debug_file_stem(Some("analyst.news_macro"), Some("analyst_report")),
            "news_macro"
        );
        assert_eq!(
            debug_file_stem(Some("mediator.topic"), Some("researcher_warmup")),
            "warmup"
        );
        append_file_debug_record_at(
            temp.path(),
            "phase3",
            "news_macro",
            json!({"kind": "typed_provider_payload", "direction": "response"}),
        )
        .unwrap();

        let path = temp.path().join("outputs/debug/phase3/news_macro.jsonl");
        let lines = std::fs::read_to_string(path).unwrap();
        let records: Vec<Value> = lines
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["kind"], "typed_provider_payload");
        assert!(records[0]["ts_ms"].as_u64().is_some());
    }
}
