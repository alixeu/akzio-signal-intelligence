use anyhow::{bail, Context, Result};
use chrono::{NaiveDate, Utc};
use futures::{future::BoxFuture, stream, StreamExt};
use orchestrator_core::{
    config_get, default_project_root, md5_3, HistoricalReflectionArtifactV1, MarketRegime,
    ReflectionDisposition, ToolManagedProfile, HISTORICAL_REFLECTION_ARTIFACT_SCHEMA_VERSION,
};
use orchestrator_llm::{
    agent_loop::{
        FileStoreSessionRuntime, ModelStreamResult, RetrievalPolicy, SessionRuntimeSpec, TokenUsage,
    },
    run_agent_fork_loop_with_metrics, run_agent_loop_with_metrics,
    tools::{ExternalToolConfig, FileStoreInputSnapshot},
    truncation::TruncationConfig,
    AgentLoopOutput, AgentSettings, ForkLoopInput, RoleLlmSettings,
};
use serde_json::{json, Map, Value};
use std::time::{Duration, Instant};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::time;
use tracing::{debug, warn};

use super::config::{prompt_version, RetrievalConfig, RuntimeConfig};
use super::index_runtime::{file_store_index_tool_runtime, FileStoreIndexRuntimePlan};
use super::lifecycle::{run_location_from_state, tickers_from_state};
use super::render::{direct_context_manifest, render_prompt_with_plugins};
use crate::memory::{search_experiences, ExperienceSearchQuery};

mod metrics;
mod retry;
pub(crate) use self::metrics::{record_role_job_metrics, refresh_role_job_metrics};
use self::retry::{backoff_ms, is_transient_role_error};

pub(crate) struct RoleRun<'a> {
    pub state: Value,
    pub role: &'a str,
    pub phase: i64,
    pub kind: &'a str,
    pub round: Option<i64>,
    pub topic_id: Option<&'a str>,
    pub mock: bool,
    pub model_override: Option<&'a str>,
    pub reasoning_effort_override: Option<&'a str>,
    pub config: &'a RuntimeConfig,
    pub prompt_path: Option<&'a std::path::Path>,
}

#[derive(Debug, Clone)]
pub(crate) struct RoleJob {
    pub role: String,
    pub phase: i64,
    pub kind: String,
    pub round: Option<i64>,
    pub topic_id: Option<String>,
    pub mock: bool,
    pub debug: bool,
    pub prompt: String,
    pub prompt_path: Option<String>,
    pub debug_output_path: Option<PathBuf>,
    pub prompt_version: Option<String>,
    pub tickers: Vec<String>,
    pub tool_managed_profile: ToolManagedProfile,
    pub index_tool_runtime: Option<orchestrator_llm::tools::index_tools::IndexToolRuntimeBinding>,
    pub experience_retrieval:
        Option<orchestrator_llm::tools::experience_tools::ExperienceRetrievalBinding>,
    pub evidence_research:
        Option<orchestrator_llm::tools::research_evidence_gap::EvidenceResearchBinding>,
    pub session_runtime: FileStoreSessionRuntime,
    pub llm: Option<RoleLlmSettings>,
    pub reasoning_effort_override: Option<String>,
    pub tools: ExternalToolConfig,
    pub web_search: orchestrator_llm::web_search::WebSearchConfig,
    pub truncation: TruncationConfig,
    pub retrieval_policy: RetrievalPolicy,
    pub context_manifest: Value,
    /// Stable identity for one Phase 2 STree mailbox delivery. Retries of the
    /// same delivery reuse it, while later deliveries in the same role/round
    /// receive a new turn in the existing session.
    pub phase2_turn_key: Option<String>,
}

#[derive(Debug)]
pub(crate) struct RoleJobResult {
    pub role: String,
    pub phase: i64,
    pub kind: String,
    pub round: Option<i64>,
    pub topic_id: Option<String>,
    pub prompt_version: Option<String>,
    pub model: String,
    pub turn_id: String,
    pub session_id: String,
    pub artifact: Option<Value>,
    pub error: Option<String>,
    pub timed_out: bool,
    pub elapsed_ms: u128,
    /// Time spent waiting on the LLM API (sum of model iterations).
    pub llm_ms: u128,
    /// Time spent running tools invoked by the LLM.
    pub tool_ms: u128,
    pub usage: TokenUsage,
    pub turn_count: u64,
    pub tool_call_count: u64,
}

impl RoleJobResult {
    /// Orchestration / idle wait: total - llm - tool.
    pub fn wait_ms(&self) -> u128 {
        self.elapsed_ms
            .saturating_sub(self.llm_ms.saturating_add(self.tool_ms))
    }
}

fn prompt_version_for_role(state: &Value, role: &str, kind: &str) -> Option<String> {
    let config = state.get("config")?;
    if role == "mediator.topic" && kind == "warmup" {
        return Some(prompt_version(config, "orchestrator.prompts.phase2.warmup"));
    }
    if role == "compressor.phase_summary" {
        let phase = state
            .pointer("/_summary_source_payload/phase")
            .and_then(Value::as_i64)?;
        return Some(prompt_version(
            config,
            &format!("orchestrator.prompts.compressor.phase{phase}"),
        ));
    }
    let prompt_key = match role {
        "reflector.historical" => "orchestrator.prompts.reflection.historical",
        "analyst.technical" => "orchestrator.prompts.analyst.technical",
        "analyst.news_macro" => "orchestrator.prompts.analyst.news_macro",
        "mediator.topic" => "orchestrator.prompts.phase2.topic_generator",
        "researcher.bull" => "orchestrator.prompts.phase2.bull_interaction",
        "researcher.bear" => "orchestrator.prompts.phase2.bear_interaction",
        "mediator.topic_controller" => "orchestrator.prompts.mediator.topic_controller",
        "manager.research" => "orchestrator.prompts.manager.research",
        "trader" => "orchestrator.prompts.trader",
        "risk.aggressive" => "orchestrator.prompts.risk.aggressive",
        "risk.neutral" => "orchestrator.prompts.risk.neutral",
        "risk.conservative" => "orchestrator.prompts.risk.conservative",
        "portfolio.manager" => "orchestrator.prompts.portfolio.manager",
        _ => return None,
    };
    Some(prompt_version(config, prompt_key))
}

fn tool_managed_profile_for_role_kind(role: &str, kind: &str) -> Option<ToolManagedProfile> {
    match role {
        "reflector.historical" => Some(ToolManagedProfile::HistoricalReflection),
        "analyst.technical" | "analyst.news_macro" => Some(ToolManagedProfile::AnalystReport),
        "mediator.topic" if kind == "warmup" => Some(ToolManagedProfile::ResearcherWarmup),
        "mediator.topic" => Some(ToolManagedProfile::TopicGeneration),
        "researcher.bull" | "researcher.bear" => Some(ToolManagedProfile::DebateResponse),
        "mediator.topic_controller" => Some(ToolManagedProfile::TopicControl),
        "researcher.web_evidence" => Some(ToolManagedProfile::EvidenceResearch),
        "manager.research" => Some(ToolManagedProfile::ResearchDecision),
        "trader" => Some(ToolManagedProfile::TradeIntent),
        "risk.aggressive" | "risk.neutral" | "risk.conservative" => {
            Some(ToolManagedProfile::RiskReview)
        }
        "portfolio.manager" => Some(ToolManagedProfile::PortfolioDecision),
        "compressor.phase_summary" => Some(ToolManagedProfile::PhaseSummary),
        _ => None,
    }
}

pub(crate) fn prepare_role_job(input: RoleRun<'_>) -> Result<RoleJob> {
    let RoleRun {
        state,
        role,
        phase,
        kind,
        round,
        topic_id,
        mock,
        model_override,
        reasoning_effort_override,
        config,
        prompt_path,
    } = input;
    let debug_enabled = state.get("debug").and_then(Value::as_bool).unwrap_or(false);
    let phase2_turn_key = (phase == 2 && kind == "stree_turn")
        .then(|| {
            state
                .get("_phase2_stree_dispatch_key")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .flatten();
    let alpaca_market_data = role == "analyst.news_macro" && !mock && !debug_enabled;
    let tickers = tickers_from_state(&state);
    let tool_tickers = if role == "portfolio.manager" {
        state
            .get("investable_assets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect()
    } else {
        tickers.clone()
    };
    let prompt_version = prompt_version_for_role(&state, role, kind);
    let prompt = if mock {
        String::new()
    } else {
        render_prompt_with_plugins(
            &state,
            role,
            phase,
            kind,
            round,
            topic_id,
            prompt_path,
            Some(&config.component_plugins),
        )?
    };
    let mut llm = if mock {
        None
    } else {
        let mut llm = config
            .llm_roles
            .get(role)
            .or_else(|| {
                // Live phase_summary compressor reuses research-manager LLM defaults when not configured.
                if role == "compressor.phase_summary" {
                    config.llm_roles.get("manager.research")
                } else {
                    None
                }
            })
            .with_context(|| format!("missing LLM config for role {role:?}"))?
            .clone();
        if let Some(model) = model_override.filter(|value| !value.trim().is_empty()) {
            llm.model = model.to_string();
        }
        Some(llm)
    };
    debug!(
        role,
        phase,
        kind,
        round,
        topic_id,
        mock,
        debug = debug_enabled,
        prompt_path = prompt_path.map(|path| path.display().to_string()),
        prompt_version,
        prompt_chars = prompt.len(),
        "prepared role job"
    );
    let candidate_tool_managed_profile = tool_managed_profile_for_role_kind(role, kind);
    let (
        tool_managed_profile,
        index_tool_runtime,
        experience_retrieval,
        evidence_research,
        file_store_input,
        tool_allowlist,
    ) = {
        let profile = candidate_tool_managed_profile
            .with_context(|| format!("missing ToolManaged profile for role={role} kind={kind}"))?;
        let registration = config.role_profile_registry.registration(role, profile)?;
        let store_root = state
            .get("store_root")
            .and_then(Value::as_str)
            .context("store_root missing for migrated ToolManaged domain role")?;
        if profile == ToolManagedProfile::HistoricalReflection {
            let binding = file_store_historical_reflection_index_runtime(
                Path::new(store_root),
                &state,
                registration.profile_version,
                registration.builder_version,
            )?;
            (
                profile,
                Some(binding),
                None,
                None,
                None,
                registration
                    .tool_allowlist
                    .iter()
                    .filter(|tool| is_read_only_model_tool(tool.as_str()))
                    .cloned()
                    .collect(),
            )
        } else if profile == ToolManagedProfile::PhaseSummary {
            (profile, None, None, None, None, Vec::new())
        } else {
            let input = if profile == ToolManagedProfile::AnalystReport && !mock {
                Some(file_store_input_from_state(&state)?)
            } else {
                None
            };
            let index_reader = registration
                .allows_tool(orchestrator_llm::tools::index_tools::READ_INDEXES_NAME)
                .then(|| {
                    file_store_domain_index_read_runtime(
                        Path::new(store_root),
                        &state,
                        role,
                        phase,
                        profile,
                        &tickers,
                    )
                })
                .transpose()?;
            let experiences = registration
                .allows_tool(orchestrator_llm::tools::experience_tools::SEARCH_EXPERIENCES_NAME)
                .then(|| {
                    file_store_experience_retrieval(
                        Path::new(store_root),
                        &state,
                        role,
                        phase,
                        &tickers,
                        config.retrieval.reflection_max_details,
                    )
                })
                .transpose()?;
            let evidence_research = registration
                .allows_tool(orchestrator_llm::tools::research_evidence_gap::NAME)
                .then(|| {
                    phase2_evidence_research_binding(
                        &state,
                        role,
                        topic_id,
                        model_override,
                        reasoning_effort_override,
                        config,
                        &tool_tickers,
                    )
                })
                .transpose()?;
            (
                profile,
                index_reader,
                experiences,
                evidence_research,
                input,
                registration
                    .tool_allowlist
                    .iter()
                    .filter(|tool| is_read_only_model_tool(tool.as_str()))
                    .cloned()
                    .collect(),
            )
        }
    };
    if let Some(llm) = llm.as_mut() {
        // The built-in RoleProfileRegistry is the only profile allowlist.
        // YAML may select models and transport settings, but cannot add a
        // model-visible tool outside this typed Rust-owned contract.
        llm.tools = tool_allowlist
            .iter()
            .map(|tool| tool.as_str().to_owned())
            .collect();
    }
    // A migrated role may read only its Rust-projected FileStore indexes and
    // snapshots. Clearing every alternate persistence handle here turns a missing
    // FileStore projection into a hard tool error instead of an accidental
    // alternate persistence fallback.
    let phase2_fork = phase2_fork_reference(&state, role, topic_id, round);
    let phase2_context = phase2_context_payload(
        &state,
        role,
        topic_id,
        round,
        phase2_fork
            .as_ref()
            .map(|fork| fork.fork_from_turn_id.as_str()),
    );
    let session_runtime = file_store_session_runtime(
        &state,
        role,
        phase,
        topic_id,
        round,
        tool_managed_profile,
        phase2_fork,
    )?;

    Ok(RoleJob {
        role: role.to_string(),
        phase,
        kind: kind.to_string(),
        round,
        topic_id: topic_id.map(ToString::to_string),
        mock,
        debug: debug_enabled,
        prompt,
        prompt_path: prompt_path.map(|path| path.display().to_string()),
        debug_output_path: phase2_debug_output_path(phase, role, kind, topic_id, round),
        prompt_version,
        tickers: tickers.clone(),
        tool_managed_profile,
        index_tool_runtime,
        experience_retrieval,
        evidence_research,
        session_runtime,
        llm,
        reasoning_effort_override: reasoning_effort_override.map(ToString::to_string),
        tools: ExternalToolConfig {
            project_root: default_project_root(),
            run_id: state
                .get("run_id")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            phase: Some(phase),
            phase_summary_page_limit: config.retrieval.summary_page_limit,
            phase_summary_detail_page_limit: config.retrieval.detail_page_limit,
            tickers: tool_tickers,
            alpaca_market_data,
            alpaca_api_key: if alpaca_market_data {
                config.alpaca_api_key.clone()
            } else {
                None
            },
            alpaca_api_secret: if alpaca_market_data {
                config.alpaca_api_secret.clone()
            } else {
                None
            },
            file_store_input,
            file_store_reflection_source: file_store_reflection_source(
                &state,
                tool_managed_profile,
            ),
            phase2_context,
        },
        web_search: config.web_search.get(role).cloned().unwrap_or_default(),
        truncation: config.truncation.clone(),
        retrieval_policy: retrieval_policy_for_role(role, kind, &config.retrieval),
        context_manifest: direct_context_manifest(&state, phase),
        phase2_turn_key,
    })
}

fn is_read_only_model_tool(name: &str) -> bool {
    name == "think"
        || name == "web.run"
        || name == orchestrator_llm::tools::record_phase2_context::NAME
        || matches!(
            name,
            orchestrator_llm::tools::phase2_stree::SUBMIT_DEBATE_TURN
                | orchestrator_llm::tools::phase2_stree::ROUTE_DEBATE_TURN
                | orchestrator_llm::tools::phase2_stree::WAIT_FOR_DEBATE_TURN
                | orchestrator_llm::tools::phase2_stree::CLOSE_DEBATE
        )
        || name == "verify_event"
        || name == orchestrator_llm::tools::research_evidence_gap::NAME
        || name.starts_with("read_")
        || name.starts_with("search_")
        || name.starts_with("alpaca_get_")
}

#[derive(Clone)]
struct WorkflowEvidenceResearchService {
    store_root: PathBuf,
    current_date: String,
    run_id: String,
    storage_namespace: Option<String>,
    project_root: PathBuf,
    prompt_path: PathBuf,
    llm: RoleLlmSettings,
    web_search: orchestrator_llm::web_search::WebSearchConfig,
    truncation: TruncationConfig,
    tools: ExternalToolConfig,
    reasoning_effort_override: Option<String>,
    debug: bool,
}

impl orchestrator_llm::tools::research_evidence_gap::EvidenceResearchService
    for WorkflowEvidenceResearchService
{
    fn research(
        &self,
        request: orchestrator_llm::tools::research_evidence_gap::EvidenceResearchRequest,
        request_id: String,
        topic_id: Option<String>,
    ) -> BoxFuture<'static, Result<Value>> {
        let service = self.clone();
        Box::pin(
            async move { run_web_evidence_research(service, request, request_id, topic_id).await },
        )
    }
}

async fn run_web_evidence_research(
    service: WorkflowEvidenceResearchService,
    request: orchestrator_llm::tools::research_evidence_gap::EvidenceResearchRequest,
    request_id: String,
    topic_id: Option<String>,
) -> Result<Value> {
    let template = std::fs::read_to_string(&service.prompt_path).with_context(|| {
        format!(
            "failed to read Web evidence prompt {}",
            service.prompt_path.display()
        )
    })?;
    let request_json = serde_json::to_string_pretty(&json!({
        "request_id": request_id,
        "topic_id": topic_id,
        "request": request,
    }))?;
    let prompt =
        format!("{template}\n\n## Rust-owned evidence request\n\n```json\n{request_json}\n```");
    let store = orchestrator_store::FileStore::open(
        &service.store_root,
        orchestrator_store::FileStoreOptions::default(),
    )?;
    let scope_component = topic_id
        .as_deref()
        .map(|topic| format!("topic-{}", md5_3(topic)))
        .unwrap_or_else(|| "topic-generation".to_owned());
    let session_id = format!(
        "{}:p2:researcher.web_evidence:{}:{}",
        service.run_id, scope_component, request_id
    );
    let session_runtime = FileStoreSessionRuntime::create_or_load(
        store,
        SessionRuntimeSpec {
            run: orchestrator_store::RunLocation::with_storage_namespace(
                &service.current_date,
                &service.run_id,
                service.storage_namespace.clone(),
            )?,
            session_id,
            role: "researcher.web_evidence".to_owned(),
            phase: 2,
            profile: ToolManagedProfile::EvidenceResearch.as_str().to_owned(),
            fork: None,
            created_at: Utc::now().to_rfc3339(),
        },
    )?;
    let prompt_path = service
        .prompt_path
        .strip_prefix(&service.project_root)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("prompts/phase2/web_evidence_researcher.md"));
    let settings = AgentSettings {
        role: "researcher.web_evidence".to_owned(),
        phase: Some(2),
        topic_id,
        debug_prompt_path: Some(prompt_path),
        debug_output_path: Some(
            PathBuf::from("outputs/debug/phase2/evidence")
                .join(scope_component)
                .join(format!("{request_id}.json")),
        ),
        debug_round: None,
        debug_turn_id: None,
        tickers: request.tickers,
        tool_managed_profile: ToolManagedProfile::EvidenceResearch,
        session_runtime,
        index_tool_runtime: None,
        experience_retrieval: None,
        evidence_research: None,
        llm: service.llm,
        reasoning_effort_override: service.reasoning_effort_override,
        tools: Some(service.tools),
        web_search: service.web_search,
        truncation: service.truncation,
        debug: service.debug,
        retrieval_policy: RetrievalPolicy::default(),
    };
    let output = run_agent_loop_with_metrics(&settings, &prompt).await?;
    let response_text = output
        .artifact
        .get("response_text")
        .and_then(Value::as_str)
        .context("Web evidence researcher returned no final response text")?;
    normalize_web_evidence_packet(response_text, &request_id)
}

fn phase2_evidence_research_binding(
    state: &Value,
    role: &str,
    topic_id: Option<&str>,
    model_override: Option<&str>,
    reasoning_effort_override: Option<&str>,
    config: &RuntimeConfig,
    tickers: &[String],
) -> Result<orchestrator_llm::tools::research_evidence_gap::EvidenceResearchBinding> {
    use orchestrator_llm::tools::research_evidence_gap::{
        EvidenceResearchBinding, EvidenceResearchScope,
    };

    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("Phase 2 evidence research requires run_id")?;
    let current_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .context("Phase 2 evidence research requires current_date")?;
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .context("Phase 2 evidence research requires store_root")?;
    let (scope_key, max_calls) = if role == "mediator.topic" {
        (format!("{run_id}:phase2:topic-generation"), 2)
    } else {
        let topic_id = topic_id.context("Bull/Bear evidence research requires topic_id")?;
        (format!("{run_id}:phase2:topic:{topic_id}"), 2)
    };
    let mut llm = config
        .llm_roles
        .get("researcher.web_evidence")
        .context("missing LLM config for researcher.web_evidence")?
        .clone();
    if let Some(model) = model_override.filter(|value| !value.trim().is_empty()) {
        llm.model = model.to_owned();
    }
    let registration = config.role_profile_registry.registration(
        "researcher.web_evidence",
        ToolManagedProfile::EvidenceResearch,
    )?;
    llm.tools = registration
        .tool_allowlist
        .iter()
        .map(|tool| tool.as_str().to_owned())
        .collect();
    let project_root = default_project_root();
    let service = WorkflowEvidenceResearchService {
        store_root: PathBuf::from(store_root),
        current_date: current_date.to_owned(),
        run_id: run_id.to_owned(),
        storage_namespace: state
            .get("storage_namespace")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        project_root: project_root.clone(),
        prompt_path: config
            .prompts
            .path_for("researcher.web_evidence")
            .context("missing Web evidence researcher prompt")?
            .clone(),
        llm,
        web_search: config
            .web_search
            .get("researcher.web_evidence")
            .context("missing Web search config for researcher.web_evidence")?
            .clone(),
        truncation: config.truncation.clone(),
        tools: ExternalToolConfig {
            project_root,
            run_id: Some(run_id.to_owned()),
            phase: Some(2),
            phase_summary_page_limit: config.retrieval.summary_page_limit,
            phase_summary_detail_page_limit: config.retrieval.detail_page_limit,
            tickers: tickers.to_vec(),
            alpaca_market_data: false,
            alpaca_api_key: None,
            alpaca_api_secret: None,
            file_store_input: None,
            file_store_reflection_source: None,
            phase2_context: None,
        },
        reasoning_effort_override: reasoning_effort_override.map(ToOwned::to_owned),
        debug: state.get("debug").and_then(Value::as_bool).unwrap_or(false),
    };
    EvidenceResearchBinding::new(
        Arc::new(service),
        config.evidence_research_coordinator.clone(),
        EvidenceResearchScope {
            scope_key,
            role: role.to_owned(),
            topic_id: topic_id.map(ToOwned::to_owned),
            allowed_tickers: tickers
                .iter()
                .map(|ticker| ticker.trim().to_ascii_uppercase())
                .filter(|ticker| !ticker.is_empty())
                .collect(),
            max_calls,
        },
    )
}

fn normalize_web_evidence_packet(response_text: &str, request_id: &str) -> Result<Value> {
    let start = response_text
        .find('{')
        .context("Web evidence response must contain one JSON object")?;
    // The agent loop appends Rust-owned Web provenance after the model's JSON
    // response. Decode the first complete JSON value rather than extending the
    // model object through that attachment.
    let mut values =
        serde_json::Deserializer::from_str(&response_text[start..]).into_iter::<Value>();
    let value = values
        .next()
        .transpose()
        .context("Web evidence response is not valid JSON")?
        .context("Web evidence response must contain one JSON object")?;
    let object = value
        .as_object()
        .context("Web evidence response must be a JSON object")?;
    let verified_source_urls = verified_web_source_urls(response_text)?;
    let status = required_string(object, "status", 20)?;
    if !matches!(
        status.as_str(),
        "supported" | "refuted" | "mixed" | "not_found"
    ) {
        bail!("Web evidence status must be supported, refuted, mixed, or not_found");
    }
    let retrieved_at = Utc::now().to_rfc3339();
    let mut seen_ids = BTreeSet::new();
    let mut evidence_limit = 5usize;
    let mut evidence = normalize_web_evidence_items(
        object.get("evidence"),
        request_id,
        &retrieved_at,
        &mut seen_ids,
        &mut evidence_limit,
        verified_source_urls.as_ref(),
    )?;
    let mut counter_limit = 5usize;
    let mut counterevidence = normalize_web_evidence_items(
        object.get("counterevidence"),
        request_id,
        &retrieved_at,
        &mut seen_ids,
        &mut counter_limit,
        verified_source_urls.as_ref(),
    )?;
    if evidence.len() + counterevidence.len() > 5 {
        if evidence.is_empty() {
            counterevidence.truncate(5);
        } else if counterevidence.is_empty() {
            evidence.truncate(5);
        } else {
            evidence.truncate(4);
            counterevidence.truncate(5 - evidence.len());
        }
    }
    if status == "not_found" && (!evidence.is_empty() || !counterevidence.is_empty()) {
        bail!("Web evidence status not_found cannot include evidence");
    }
    if status != "not_found" && evidence.is_empty() && counterevidence.is_empty() {
        bail!("Web evidence status {status} requires at least one source");
    }
    Ok(json!({
        "status": status,
        "request_id": request_id,
        "evidence": evidence,
        "counterevidence": counterevidence,
        "unresolved_gaps": bounded_string_array(object.get("unresolved_gaps"), 5, 300),
        "search_queries": bounded_string_array(object.get("search_queries"), 5, 256),
        "source_count": evidence.len() + counterevidence.len(),
    }))
}

fn normalize_web_evidence_items(
    value: Option<&Value>,
    request_id: &str,
    retrieved_at: &str,
    seen_ids: &mut BTreeSet<String>,
    remaining: &mut usize,
    verified_source_urls: Option<&BTreeSet<String>>,
) -> Result<Vec<Value>> {
    let items = value.and_then(Value::as_array).cloned().unwrap_or_default();
    let mut normalized = Vec::new();
    for item in items {
        if *remaining == 0 {
            break;
        }
        let object = item
            .as_object()
            .context("Web evidence entries must be JSON objects")?;
        let claim = required_string(object, "claim", 500)?;
        let relation = required_string(object, "relation", 20)?;
        if !matches!(relation.as_str(), "supports" | "refutes" | "context") {
            bail!("Web evidence relation must be supports, refutes, or context");
        }
        let source_url = required_string(object, "source_url", 1_000)?;
        if !source_url.starts_with("https://") && !source_url.starts_with("http://") {
            bail!("Web evidence source_url must use http or https");
        }
        if verified_source_urls.is_some_and(|urls| !urls.contains(&source_url)) {
            bail!(
                "Web evidence source_url was not present in the Rust-verified Web search results"
            );
        }
        let publisher = required_string(object, "publisher", 200)?;
        let source_tier = required_string(object, "source_tier", 20)?;
        if !matches!(
            source_tier.as_str(),
            "primary" | "official" | "major_media" | "secondary"
        ) {
            bail!("Web evidence source_tier is invalid");
        }
        let evidence_hash = orchestrator_store::content_hash(&json!({
            "claim": claim,
            "relation": relation,
            "source_url": source_url,
            "published_at": object.get("published_at").cloned().unwrap_or(Value::Null),
        }))?;
        let evidence_id = format!(
            "web-{}",
            evidence_hash
                .strip_prefix("sha256:")
                .unwrap_or(&evidence_hash)
        );
        if !seen_ids.insert(evidence_id.clone()) {
            continue;
        }
        let published_at = match object.get("published_at") {
            None | Some(Value::Null) => Value::Null,
            Some(Value::String(value)) if value.chars().count() <= 100 => {
                Value::String(value.clone())
            }
            _ => bail!("Web evidence published_at must be a short string or null"),
        };
        normalized.push(json!({
            "evidence_id": evidence_id,
            "request_id": request_id,
            "claim": claim,
            "relation": relation,
            "source_url": source_url,
            "publisher": publisher,
            "published_at": published_at,
            "retrieved_at": retrieved_at,
            "source_tier": source_tier,
        }));
        *remaining -= 1;
    }
    Ok(normalized)
}

fn verified_web_source_urls(response_text: &str) -> Result<Option<BTreeSet<String>>> {
    let marker = orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER;
    let Some((_, registry_json)) = response_text.rsplit_once(marker) else {
        return Ok(None);
    };
    let results: Vec<Value> = serde_json::from_str(registry_json.trim())
        .context("Rust-verified Web search result attachment is malformed")?;
    Ok(Some(
        results
            .into_iter()
            .filter_map(|result| {
                result
                    .get("source_url")
                    .and_then(Value::as_str)
                    .filter(|url| url.starts_with("https://") || url.starts_with("http://"))
                    .map(ToOwned::to_owned)
            })
            .collect(),
    ))
}

fn required_string(object: &Map<String, Value>, field: &str, max_chars: usize) -> Result<String> {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("Web evidence response requires non-empty {field}"))?;
    if value.chars().count() > max_chars {
        bail!("Web evidence {field} exceeds {max_chars} characters");
    }
    Ok(value)
}

fn bounded_string_array(value: Option<&Value>, limit: usize, max_chars: usize) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.chars().count() <= max_chars)
        .take(limit)
        .map(ToOwned::to_owned)
        .collect()
}

/// One read-only Index/Detail binding for a business role. The unit scope,
/// allowed phases and ticker set are all Rust-owned; this intentionally does
/// not expose create/append/finalize to analysts, debaters, risk, or portfolio
/// roles.
fn file_store_domain_index_read_runtime(
    store_root: &Path,
    state: &Value,
    role: &str,
    phase: i64,
    profile: ToolManagedProfile,
    tickers: &[String],
) -> Result<orchestrator_llm::tools::index_tools::IndexToolRuntimeBinding> {
    use orchestrator_llm::tools::index_tools::{IndexKind, IndexOwnedScope, IndexReadVisibility};
    use orchestrator_store::{content_hash, FileStore, FileStoreOptions};

    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("domain Index reader requires run_id")?;
    let phase_u8 = u8::try_from(phase).context("domain Index reader phase must fit u8")?;
    let source_phases = phase_summary_source_phases(profile)?;
    let source_payload_hash = content_hash(&json!({
        "reader_role": role,
        "phase": phase,
        "profile": profile.as_str(),
        "source_phases": source_phases,
        "tickers": tickers,
        "run_id": run_id,
    }))?;
    let owned = IndexOwnedScope {
        run_id: run_id.to_owned(),
        source_run_id: None,
        source_phase: phase_u8,
        role: role.to_owned(),
        kind: IndexKind::PhaseSummary,
        ticker: None,
        topic_id: None,
        unit_key: format!("read-indexes:phase{phase}:{role}"),
        source_payload_hash,
        index_id: format!("read-only:phase{phase}:{role}"),
        authoritative_fields: Map::new(),
    };
    let visibility = IndexReadVisibility {
        kinds: BTreeSet::from([IndexKind::PhaseSummary]),
        tickers: tickers.iter().cloned().collect(),
        source_phases,
        applies_to_phases: BTreeSet::from([phase_u8]),
        max_page_size: 20,
        ..Default::default()
    };
    file_store_index_tool_runtime(
        FileStore::open(store_root, FileStoreOptions::default())?,
        owned,
        visibility,
        FileStoreIndexRuntimePlan::read_only(
            vec![run_location_from_state(state)?],
            Utc::now().to_rfc3339(),
        ),
    )
}

fn phase_summary_source_phases(profile: ToolManagedProfile) -> Result<BTreeSet<u8>> {
    let phases = match profile {
        ToolManagedProfile::ResearcherWarmup
        | ToolManagedProfile::TopicGeneration
        | ToolManagedProfile::DebateSeed
        | ToolManagedProfile::DebateResponse
        | ToolManagedProfile::TopicControl => &[1][..],
        ToolManagedProfile::ResearchDecision => &[1, 2],
        ToolManagedProfile::TradeIntent => &[3],
        ToolManagedProfile::RiskReview => &[3, 4],
        ToolManagedProfile::PortfolioDecision => &[3, 4, 5],
        ToolManagedProfile::HistoricalReflection
        | ToolManagedProfile::AnalystReport
        | ToolManagedProfile::EvidenceResearch
        | ToolManagedProfile::PhaseSummary => {
            bail!(
                "profile {} does not own a domain Phase Summary reader",
                profile.as_str()
            )
        }
    };
    Ok(phases.iter().copied().collect())
}

fn phase2_fork_reference(
    state: &Value,
    role: &str,
    topic_id: Option<&str>,
    _round: Option<i64>,
) -> Option<orchestrator_store::ForkReference> {
    let (session_id, turn_id) = if role == "mediator.topic_controller" {
        let topic_id = topic_id?;
        // The controller has one canonical topic session. Its first turn
        // forks Topic Generation; later turns reload that same session/turn
        // and only receive a stree user-message injection, never a sibling
        // controller fork.
        let _ = topic_id;
        (
            state
                .get("topic_generation_session_id")?
                .as_str()?
                .to_owned(),
            state.get("topic_generation_turn_id")?.as_str()?.to_owned(),
        )
    } else if matches!(role, "researcher.bull" | "researcher.bear") {
        let _ = topic_id?;
        let warmup = state.get("phase2_warmup")?;
        (
            warmup.get("session_id")?.as_str()?.to_owned(),
            warmup.get("turn_id")?.as_str()?.to_owned(),
        )
    } else {
        return None;
    };
    Some(orchestrator_store::ForkReference {
        fork_from_session_id: session_id,
        fork_from_turn_id: turn_id,
    })
}

fn phase2_context_payload(
    state: &Value,
    role: &str,
    topic_id: Option<&str>,
    round: Option<i64>,
    fork_from_turn_id: Option<&str>,
) -> Option<Value> {
    let context_kind = match role {
        "researcher.bull" | "researcher.bear" => "stree_debate",
        "mediator.topic_controller" => "topic_control",
        _ => return None,
    };
    let topic_id = topic_id?;
    let round_num = round?;
    let fork_from_turn_id = fork_from_turn_id?;
    let topic_state = state.pointer(&format!("/topic_debate_states/{topic_id}"));
    let mut context = json!({
        "kind": context_kind,
        "role": role,
        "topic_id": topic_id,
        "round": round_num,
        "round_num": round_num,
        "fork_from_turn_id": fork_from_turn_id,
        "include_prompt_on_fork": true,
    });
    if let Some(topic) = topic_state.and_then(|value| value.get("topic")) {
        context["topic"] = topic.clone();
    }
    context["debate_turns"] = topic_state
        .map(phase2_topic_context_turns)
        .unwrap_or_else(|| json!([]));
    if let Some(injection) = state.get("_phase2_stree_injection").and_then(Value::as_str) {
        context["stree_injection"] = json!(injection);
    }
    Some(context)
}

fn phase2_topic_context_turns(topic_state: &Value) -> Value {
    let mut turns = topic_state
        .get("turns")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    turns.extend(
        topic_state
            .get("controller_turns")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    );
    turns.sort_by_key(|turn| {
        let round = turn
            .get("round")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        let role_rank = match turn.get("role").and_then(Value::as_str) {
            Some("researcher.bull") => 0,
            Some("researcher.bear") => 1,
            Some("mediator.topic_controller") => 2,
            _ => 3,
        };
        (round, role_rank)
    });
    Value::Array(turns)
}

fn file_store_input_from_state(state: &Value) -> Result<FileStoreInputSnapshot> {
    let input = state
        .get("file_store_input")
        .and_then(Value::as_object)
        .context("migrated Phase 1 role requires a captured FileStore input manifest")?;
    let required = |field: &str| {
        input
            .get(field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .with_context(|| format!("file_store_input.{field} is required"))
    };
    Ok(FileStoreInputSnapshot {
        store_root: PathBuf::from(required("store_root")?),
        run_id: required("run_id")?,
        current_date: required("current_date")?,
        storage_namespace: input
            .get("storage_namespace")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

/// Construct the sole Agent Loop history authority for a migrated unit.  The
/// unit identity is Rust-owned and stable across a restart; the model has no
/// way to select a session, run, path, or fork parent.
fn file_store_session_runtime(
    state: &Value,
    role: &str,
    phase: i64,
    topic_id: Option<&str>,
    round: Option<i64>,
    profile: ToolManagedProfile,
    fork: Option<orchestrator_store::ForkReference>,
) -> Result<FileStoreSessionRuntime> {
    let store_root = state
        .get("store_root")
        .and_then(Value::as_str)
        .context("migrated role requires FileStore root")?;
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("migrated role requires run_id")?;
    let phase = u8::try_from(phase).context("session phase must fit in u8")?;
    let session_id = format!(
        "{}:p{}:{}:{}:{}:{}",
        run_id,
        phase,
        role,
        profile.as_str(),
        topic_id.unwrap_or("aggregate"),
        if phase == 2
            && matches!(
                role,
                "researcher.bull" | "researcher.bear" | "mediator.topic_controller"
            )
        {
            0
        } else {
            round.unwrap_or(0)
        }
    );
    let store = orchestrator_store::FileStore::open(
        store_root,
        orchestrator_store::FileStoreOptions::default(),
    )?;
    FileStoreSessionRuntime::create_or_load(
        store,
        SessionRuntimeSpec {
            run: run_location_from_state(state)?,
            session_id,
            role: role.to_owned(),
            phase,
            profile: profile.as_str().to_owned(),
            fork,
            created_at: Utc::now().to_rfc3339(),
        },
    )
}

/// Commit a Phase 0 Summary result. The model only writes prose; this Rust
/// boundary owns task completion and the optional Experience support case.
pub(crate) fn commit_historical_reflection(
    store_root: &Path,
    state: &Value,
    submission: orchestrator_llm::tools::historical_reflection::HistoricalReflectionSubmission,
) -> Result<Value> {
    use orchestrator_store::{
        find_run_location, read_all_indexes, FileSchemaKind, FileStore, FileStoreOptions,
        IndexArchive, IndexKind, IndexQuery, ReflectionTaskLedger,
    };

    let task_value = state
        .get("reflection_task")
        .and_then(Value::as_object)
        .context("HistoricalReflection terminal requires reflection_task")?;
    let task_id = task_value
        .get("task_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("reflection_task.task_id is required")?;
    let actor_run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("HistoricalReflection terminal requires current run_id")?;
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let ledger = ReflectionTaskLedger::new(store.clone());
    let task = ledger.read(task_id)?;
    let source_location =
        find_run_location(&store, &task.key.source_run_id)?.with_context(|| {
            format!(
                "HistoricalReflection source run {} is not available",
                task.key.source_run_id
            )
        })?;
    let source_indexes = read_all_indexes(
        &store,
        Some(&source_location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            ticker: Some(task.key.ticker.clone()),
            ..Default::default()
        },
    )?;
    let sources = source_indexes
        .into_iter()
        .map(|index| {
            let expanded_relative = source_location
                .relative_root()
                .join("index")
                .join(format!("phase{}", index.source_phase))
                .join(orchestrator_store::index_path_component(&index.index_id)?)
                .join("index.json");
            let (relative_path, content_hash) = if store.exists(&expanded_relative)? {
                (expanded_relative, index.content_hash.clone())
            } else {
                let archive_relative = IndexArchive::relative_path(
                    &source_location,
                    index.source_phase,
                    &index.index_id,
                )?;
                let archive: IndexArchive = store.read_versioned_json(
                    &archive_relative,
                    FileSchemaKind::Artifact("index_archive".to_owned()),
                )?;
                archive.validate_for_location(&source_location)?;
                (archive_relative, archive.content_hash)
            };
            Ok((
                index.index_id.clone(),
                (
                    index.source_phase,
                    index.role.clone(),
                    orchestrator_core::DocumentRef {
                        document_id: index.index_id,
                        relative_path: relative_path.to_string_lossy().to_string(),
                        content_hash,
                    },
                ),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    submission.validate()?;
    let service = FileStoreHistoricalReflectionTerminal {
        store,
        ledger,
        task,
        actor_run_id: actor_run_id.to_owned(),
        source_facts: sources,
    };
    orchestrator_llm::tools::historical_reflection::HistoricalReflectionTerminalService::finalize(
        &service, submission,
    )
}

struct FileStoreHistoricalReflectionTerminal {
    store: orchestrator_store::FileStore,
    ledger: orchestrator_store::ReflectionTaskLedger,
    task: orchestrator_store::ReflectionTaskV1,
    actor_run_id: String,
    source_facts: BTreeMap<String, (u8, String, orchestrator_core::DocumentRef)>,
}

impl orchestrator_llm::tools::historical_reflection::HistoricalReflectionTerminalService
    for FileStoreHistoricalReflectionTerminal
{
    fn finalize(
        &self,
        submission: orchestrator_llm::tools::historical_reflection::HistoricalReflectionSubmission,
    ) -> Result<Value> {
        if self.task.status != orchestrator_core::ReflectionTaskStatus::Claimed
            || self.task.claimed_by_run_id.as_deref() != Some(self.actor_run_id.as_str())
        {
            bail!("reflection task is no longer claimed by this run");
        }
        let source_refs = submission
            .source_refs
            .iter()
            .map(|reference| reference.trim().to_owned())
            .collect::<BTreeSet<_>>();
        if source_refs.len() != submission.source_refs.len()
            || source_refs
                .iter()
                .any(|reference| !self.source_facts.contains_key(reference))
        {
            bail!("reflection source_refs must be unique, Rust-visible Phase Summary Index IDs");
        }
        if let Some(root_cause_phase) = submission.root_cause_phase {
            let root_refs = source_refs
                .iter()
                .filter_map(|reference| self.source_facts.get(reference))
                .filter(|(phase, _, _)| *phase == root_cause_phase)
                .collect::<Vec<_>>();
            if root_refs.is_empty() {
                bail!("root_cause_phase requires a cited Phase Summary from that phase");
            }
            if let Some(pattern) = &submission.pattern_identity {
                if !root_refs
                    .iter()
                    .any(|(_, role, _)| role == &pattern.source_role)
                {
                    bail!("PatternIdentity.source_role must be evidenced at root_cause_phase");
                }
            }
        }
        let now = Utc::now().to_rfc3339();
        let source_documents = source_refs
            .iter()
            .map(|reference| self.source_facts[reference].2.clone())
            .collect::<Vec<_>>();
        let artifact_id = orchestrator_store::content_hash(&json!({
            "task_id": self.task.task_id,
            "submission": &submission,
        }))?;
        let artifact = HistoricalReflectionArtifactV1 {
            schema_version: HISTORICAL_REFLECTION_ARTIFACT_SCHEMA_VERSION,
            artifact_id,
            task_id: self.task.task_id.clone(),
            task_key: self.task.key.clone(),
            disposition: submission.disposition,
            outcome_ref: self.task.outcome_ref.clone(),
            source_refs: source_documents.clone(),
            summary: submission.summary.trim().to_owned(),
            detail: submission.detail.trim().to_owned(),
            root_cause_phase: submission.root_cause_phase,
            propagation_phases: submission.propagation_phases.clone(),
            pattern_identity: submission.pattern_identity.clone(),
            rule_revision: submission.rule_revision(),
            created_at: now.clone(),
            content_hash: String::new(),
        };
        let artifact_ref = self.ledger.write_artifact(artifact)?;
        if submission.disposition == ReflectionDisposition::Learned {
            let pattern = submission
                .pattern_identity
                .as_ref()
                .expect("validated Learned pattern");
            let pattern_key = orchestrator_store::content_hash(&serde_json::to_value(pattern)?)?;
            let scope = orchestrator_store::IndexScope {
                kind: orchestrator_store::IndexKind::Experience,
                location: None,
                index_id: orchestrator_store::deterministic_experience_index_id(
                    &pattern_key,
                    Some(&self.task.key.ticker),
                    pattern.root_cause_phase,
                )?,
                run_id: self.actor_run_id.clone(),
                source_run_id: Some(self.task.key.source_run_id.clone()),
                source_phase: pattern.root_cause_phase,
                role: "reflector.historical".to_owned(),
                ticker: Some(self.task.key.ticker.clone()),
                topic_id: None,
                source_payload_hash: self.task.key.outcome_content_hash.clone(),
                authoritative_fields: serde_json::Map::new(),
                created_at: now.clone(),
            };
            let input = orchestrator_store::RecordExperienceCaseInput {
                scope,
                pattern_key: pattern_key.clone(),
                summary: submission.summary.trim().to_owned(),
                confidence: submission.confidence.expect("validated Learned confidence"),
                applies_to_phases: vec![pattern.root_cause_phase],
                detail: submission.detail.trim().to_owned(),
                source_refs: source_refs.into_iter().collect(),
            };
            let experience_ledger = orchestrator_store::ExperienceLedger::new(self.store.clone());
            let event = orchestrator_store::ExperienceEventV1 {
                schema_version: orchestrator_store::EXPERIENCE_EVENT_SCHEMA_VERSION,
                sequence: 0,
                event_id: String::new(),
                pattern_id: pattern_key.clone(),
                pattern_identity: Some(pattern.clone()),
                rule_revision: submission.rule_revision(),
                operation: orchestrator_core::ExperienceOperation::AddSupport,
                source_run_id: Some(self.task.key.source_run_id.clone()),
                outcome_id: Some(self.task.key.outcome_id.clone()),
                source_refs: source_documents,
                policy_ref: Some(self.task.key.policy_ref.clone()),
                independent_date_cluster: None,
                independent_regime: Some(orchestrator_store::content_hash(&serde_json::to_value(
                    &pattern.regime,
                )?)?),
                utility_sample_micros: None,
                harmful_usage: None,
                created_at: now.clone(),
                content_hash: String::new(),
            };
            self.ledger.complete_learned_with(
                &self.task.task_id,
                &self.actor_run_id,
                artifact_ref.clone(),
                &now,
                || {
                    let outcome = orchestrator_store::record_experience_case(&self.store, input)?;
                    experience_ledger.append(event)?;
                    experience_ledger.rebuild_view(&pattern_key, &now)?;
                    Ok(outcome.disposition)
                },
            )?;
        } else {
            // A contested reflection can only demote an already-existing
            // Pattern. The model provides structured identity, but Rust
            // derives the key, verifies a prior support event, and writes the
            // provenance-bound contradiction itself. It never invokes the
            // positive legacy case finalizer on this path.
            if submission.disposition == ReflectionDisposition::Contested {
                if let Some(pattern) = submission.pattern_identity.as_ref() {
                    let pattern_key =
                        orchestrator_store::content_hash(&serde_json::to_value(pattern)?)?;
                    let experience_ledger =
                        orchestrator_store::ExperienceLedger::new(self.store.clone());
                    let supported =
                        experience_ledger
                            .read_events(&pattern_key)?
                            .iter()
                            .any(|event| {
                                event.operation
                                    == orchestrator_core::ExperienceOperation::AddSupport
                            });
                    if supported {
                        experience_ledger.append(orchestrator_store::ExperienceEventV1 {
                            schema_version: orchestrator_store::EXPERIENCE_EVENT_SCHEMA_VERSION,
                            sequence: 0,
                            event_id: String::new(),
                            pattern_id: pattern_key.clone(),
                            pattern_identity: Some(pattern.clone()),
                            rule_revision: None,
                            operation: orchestrator_core::ExperienceOperation::AddContradiction,
                            source_run_id: Some(self.task.key.source_run_id.clone()),
                            outcome_id: Some(self.task.key.outcome_id.clone()),
                            source_refs: source_documents.clone(),
                            policy_ref: Some(self.task.key.policy_ref.clone()),
                            independent_date_cluster: None,
                            independent_regime: Some(orchestrator_store::content_hash(
                                &serde_json::to_value(&pattern.regime)?,
                            )?),
                            utility_sample_micros: None,
                            harmful_usage: None,
                            created_at: now.clone(),
                            content_hash: String::new(),
                        })?;
                        experience_ledger.rebuild_view(&pattern_key, &now)?;
                    }
                }
            }
            self.ledger.complete(
                &self.task.task_id,
                &self.actor_run_id,
                submission.disposition,
                artifact_ref.clone(),
                None,
                &now,
            )?;
        }
        Ok(json!({"artifact_ref": artifact_ref, "disposition": submission.disposition}))
    }
}

fn file_store_experience_retrieval(
    store_root: &Path,
    state: &Value,
    role: &str,
    phase: i64,
    tickers: &[String],
    max_results: usize,
) -> Result<orchestrator_llm::tools::experience_tools::ExperienceRetrievalBinding> {
    let store = orchestrator_store::FileStore::open(
        store_root,
        orchestrator_store::FileStoreOptions::default(),
    )?;
    let location = run_location_from_state(state)?;
    let phase = u8::try_from(phase).context("Experience retrieval phase must fit u8")?;
    let query = ExperienceSearchQuery {
        phase,
        role: role.to_owned(),
        // Ticker selection is per search call, but Rust constrains it to the
        // role's actual asset universe. A multi-asset role cannot silently
        // blend a QQQ experience into an SOXX conclusion.
        ticker: None,
        horizon_trading_days: experience_horizon_from_state(state),
        regime: experience_regime_from_state(state),
        as_of_date: experience_as_of_date_from_state(state),
        lexical_query: String::new(),
        max_results: max_results.clamp(1, 20),
    };
    let allowed_tickers = if role == "manager.research" {
        state
            .get("investable_assets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>()
    } else {
        tickers.to_vec()
    };
    Ok(
        orchestrator_llm::tools::experience_tools::ExperienceRetrievalBinding::new_scoped(
            Arc::new(FileStoreExperienceRetrieval {
                ledger: orchestrator_store::ExperienceLedger::new(store.clone()),
                memory_usage: orchestrator_store::MemoryUsageLedger::new(store, location),
                max_case_events: max_results.clamp(1, 20),
                query,
            }),
            allowed_tickers,
            max_results,
        ),
    )
}

fn experience_horizon_from_state(state: &Value) -> Option<u32> {
    state
        .get("config")
        .and_then(|config| {
            config_get(
                config,
                "orchestrator.evaluation.prediction_horizon_trading_days",
            )
        })
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn experience_as_of_date_from_state(state: &Value) -> Option<NaiveDate> {
    state
        .get("current_date")
        .and_then(Value::as_str)
        .and_then(|value| value.trim().get(..10))
        .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())
}

fn experience_regime_from_state(state: &Value) -> MarketRegime {
    let volatility = state
        .pointer("/market_snapshot/vix")
        .filter(|vix| vix.get("status").and_then(Value::as_str) == Some("available"))
        .and_then(|vix| vix.get("regime"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_owned();
    // The current input has a Rust-derived VIX regime but no independently
    // materialized liquidity/rates/breadth labels. "unknown" deliberately
    // rejects lessons that claim those unobserved dimensions as prerequisites.
    MarketRegime {
        volatility,
        trend: "unknown".to_owned(),
        liquidity: "unknown".to_owned(),
        rates: "unknown".to_owned(),
        breadth: "unknown".to_owned(),
    }
}

struct FileStoreExperienceRetrieval {
    ledger: orchestrator_store::ExperienceLedger,
    memory_usage: orchestrator_store::MemoryUsageLedger,
    max_case_events: usize,
    query: ExperienceSearchQuery,
}

struct MemoryUsageInput {
    kind: orchestrator_core::MemoryUsageEventKind,
    lexical_query: Option<String>,
    retrieved_pattern_ids: Vec<String>,
    expanded_pattern_id: Option<String>,
    retrieval_stop_reason: Option<String>,
    application_disposition: Option<orchestrator_core::MemoryApplicationDisposition>,
    application_reason: Option<String>,
    ticker: Option<String>,
}

impl orchestrator_llm::tools::experience_tools::ExperienceRetrievalService
    for FileStoreExperienceRetrieval
{
    fn search(&self, lexical_query: &str, ticker: Option<&str>) -> Result<Value> {
        let mut query = self.query.clone();
        query.lexical_query = lexical_query.to_owned();
        query.ticker = ticker.map(ToOwned::to_owned);
        let result = search_experiences(&self.ledger, &query)?;
        let stop_reason = retrieval_stop_reason_name(result.stop_reason);
        let items = result
            .items
            .into_iter()
            .map(|item| {
                json!({
                    "pattern_id": item.pattern_id,
                    "score": item.score,
                    "state": item.view.state,
                    "support_count": item.view.support_count,
                    "contradiction_count": item.view.contradiction_count,
                    "utility_ema_micros": item.view.utility_ema_micros,
                    "harmful_usage_rate_ppm": item.view.harmful_usage_rate_ppm,
                    "applicability": {
                        "scope": item.scope.as_str(),
                        "ticker": item.ticker,
                        "horizon_trading_days": item.horizon_trading_days,
                        "regime": item.regime,
                        "as_of_eligible": true,
                        "recency_penalty": item.recency_penalty,
                    },
                })
            })
            .collect::<Vec<_>>();
        self.record_usage(MemoryUsageInput {
            kind: orchestrator_core::MemoryUsageEventKind::Search,
            lexical_query: Some(lexical_query.to_owned()),
            retrieved_pattern_ids: items
                .iter()
                .filter_map(|item| {
                    item.get("pattern_id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .collect(),
            expanded_pattern_id: None,
            retrieval_stop_reason: Some(stop_reason.to_owned()),
            application_disposition: None,
            application_reason: None,
            ticker: query.ticker.clone(),
        })?;
        Ok(json!({
            "items": items,
            "stop_reason": stop_reason,
            "query_context": {
                "ticker": query.ticker,
                "horizon_trading_days": query.horizon_trading_days,
                "regime": query.regime,
                "as_of_date": query.as_of_date.map(|date| date.to_string()),
                "authority": "rust_frozen_current_context_v1",
            },
        }))
    }

    fn read_cases(&self, pattern_id: &str, ticker: Option<&str>) -> Result<Value> {
        let events = self.ledger.read_events(pattern_id)?;
        self.record_usage(MemoryUsageInput {
            kind: orchestrator_core::MemoryUsageEventKind::Expand,
            lexical_query: None,
            retrieved_pattern_ids: Vec::new(),
            expanded_pattern_id: Some(pattern_id.to_owned()),
            retrieval_stop_reason: None,
            application_disposition: None,
            application_reason: None,
            ticker: ticker.map(ToOwned::to_owned),
        })?;
        Ok(json!({
            "pattern_id": pattern_id,
            "untrusted_historical_data": events.into_iter().rev().take(self.max_case_events).rev().map(|event| json!({
                "operation": event.operation,
                "source_run_id": event.source_run_id,
                "outcome_id": event.outcome_id,
                "pattern_identity": event.pattern_identity,
                "rule_revision": event.rule_revision,
                "source_refs": event.source_refs,
                "independent_date_cluster": event.independent_date_cluster,
                "independent_regime": event.independent_regime,
                "utility_sample_micros": event.utility_sample_micros,
                "harmful_usage": event.harmful_usage,
                "created_at": event.created_at,
            })).collect::<Vec<_>>(),
            "case_event_limit": self.max_case_events,
            "authority": "rust_bounded_untrusted_historical_data_v1",
        }))
    }

    fn record_application(
        &self,
        pattern_id: &str,
        ticker: Option<&str>,
        disposition: orchestrator_core::MemoryApplicationDisposition,
        reason: &str,
    ) -> Result<Value> {
        self.record_usage(MemoryUsageInput {
            kind: orchestrator_core::MemoryUsageEventKind::Application,
            lexical_query: None,
            retrieved_pattern_ids: Vec::new(),
            expanded_pattern_id: Some(pattern_id.to_owned()),
            retrieval_stop_reason: None,
            application_disposition: Some(disposition),
            application_reason: Some(reason.to_owned()),
            ticker: ticker.map(ToOwned::to_owned),
        })?;
        Ok(json!({
            "pattern_id": pattern_id,
            "disposition": disposition,
            "recorded_by": "rust_observed_tool_event",
        }))
    }
}

impl FileStoreExperienceRetrieval {
    fn record_usage(&self, input: MemoryUsageInput) -> Result<()> {
        self.memory_usage
            .append(orchestrator_core::MemoryUsageEventV1 {
                schema_version: orchestrator_core::MEMORY_USAGE_EVENT_SCHEMA_VERSION,
                sequence: 0,
                event_id: String::new(),
                kind: input.kind,
                role: self.query.role.clone(),
                phase: self.query.phase,
                ticker: input.ticker.clone(),
                unit_key: format!(
                    "memory:p{}:{}:{}",
                    self.query.phase,
                    self.query.role,
                    input.ticker.as_deref().unwrap_or("aggregate")
                ),
                lexical_query: input.lexical_query,
                retrieved_pattern_ids: input.retrieved_pattern_ids,
                expanded_pattern_id: input.expanded_pattern_id,
                retrieval_stop_reason: input.retrieval_stop_reason,
                application_disposition: input.application_disposition,
                application_reason: input.application_reason,
                created_at: Utc::now().to_rfc3339(),
                content_hash: String::new(),
            })?;
        Ok(())
    }
}

fn retrieval_stop_reason_name(reason: crate::memory::RetrievalStopReason) -> &'static str {
    match reason {
        crate::memory::RetrievalStopReason::Sufficient => "sufficient",
        crate::memory::RetrievalStopReason::NoMarginalGain => "no_marginal_gain",
        crate::memory::RetrievalStopReason::NoMatch => "no_match",
        crate::memory::RetrievalStopReason::ConflictUnresolved => "conflict_unresolved",
        crate::memory::RetrievalStopReason::BudgetExhausted => "budget_exhausted",
    }
}

/// Construct the Phase 0 evidence reader. The source run is found by its
/// manifest rather than a caller-provided path; absence is a hard error,
/// never a fallback to another summary storage path. The dedicated terminal
/// below is the sole writer.
fn file_store_historical_reflection_index_runtime(
    store_root: &Path,
    state: &Value,
    profile_version: u32,
    builder_version: u32,
) -> Result<orchestrator_llm::tools::index_tools::IndexToolRuntimeBinding> {
    use orchestrator_llm::tools::index_tools::{IndexKind, IndexOwnedScope, IndexReadVisibility};
    use orchestrator_store::{
        content_hash, find_run_location, read_all_indexes, FileStore, FileStoreOptions,
        IndexKind as StoreIndexKind, IndexQuery,
    };

    let task = state
        .get("reflection_task")
        .and_then(Value::as_object)
        .context("HistoricalReflection FileStore runtime requires reflection_task")?;
    let task_id = task
        .get("task_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("reflection_task.task_id is required")?;
    let source_run_id = task
        .get("source_run_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("reflection_task.source_run_id is required")?
        .to_owned();
    let ticker = task
        .get("ticker")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("reflection_task.ticker is required")?
        .to_owned();
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("run_id is required")?
        .to_owned();
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    let source_location = find_run_location(&store, &source_run_id)?.with_context(|| {
        format!("HistoricalReflection source run {source_run_id} is not available in FileStore")
    })?;
    let source_indexes = read_all_indexes(
        &store,
        Some(&source_location),
        &IndexQuery {
            kind: Some(StoreIndexKind::PhaseSummary),
            ticker: Some(ticker.clone()),
            ..Default::default()
        },
    )?;
    if source_indexes.is_empty() {
        bail!("HistoricalReflection source run has no ticker-scoped completed Phase Summary Index");
    }
    let source_index_ids = source_indexes
        .iter()
        .map(|index| index.index_id.clone())
        .collect::<BTreeSet<_>>();
    let source_phases = source_indexes
        .iter()
        .map(|index| index.source_phase)
        .collect::<BTreeSet<_>>();
    let source_payload_hash = content_hash(&json!({
        "task": task,
        "source_indexes": source_index_ids,
        "profile_version": profile_version,
        "builder_version": builder_version,
    }))?;
    let owned = IndexOwnedScope {
        run_id,
        source_run_id: Some(source_run_id),
        // This binding is now read-only. Root cause phase is selected only at
        // the dedicated terminal and must be proven by cited Phase Summaries;
        // the earliest available phase is not a root-cause proxy.
        source_phase: 0,
        role: "reflector.historical".to_owned(),
        kind: IndexKind::Experience,
        ticker: Some(ticker.clone()),
        topic_id: None,
        unit_key: format!("phase0:reflection-task:{task_id}"),
        source_payload_hash,
        // This placeholder is never persisted: `create_index` replaces it
        // with hash(kind, pattern_key, ticker, source_phase).
        index_id: format!("experience-pending-task-{task_id}"),
        authoritative_fields: Map::from_iter([(
            "reflection_task".to_owned(),
            Value::Object(task.clone()),
        )]),
    };
    file_store_index_tool_runtime(
        store,
        owned,
        IndexReadVisibility {
            kinds: BTreeSet::from([IndexKind::PhaseSummary]),
            tickers: BTreeSet::from([ticker]),
            source_phases,
            applies_to_phases: BTreeSet::from([1, 2, 3, 4, 5, 6]),
            roles: BTreeSet::new(),
            topic_ids: BTreeSet::new(),
            pattern_keys: BTreeSet::new(),
            source_refs: source_index_ids,
            evidence_ids: BTreeSet::new(),
            max_page_size: 20,
        },
        FileStoreIndexRuntimePlan::read_only(vec![source_location], Utc::now().to_rfc3339()),
    )
}

fn file_store_reflection_source(state: &Value, profile: ToolManagedProfile) -> Option<Value> {
    if profile != ToolManagedProfile::HistoricalReflection {
        return None;
    }
    let task = state.get("reflection_task")?.clone();
    Some(json!({
        "status": "available",
        "task": task,
        "decision": task.get("decision").cloned().unwrap_or(Value::Null),
        "outcome": task.get("outcome").cloned().unwrap_or(Value::Null),
        "source_run_metadata": {
            "source_run_id": task.get("source_run_id").cloned().unwrap_or(Value::Null),
            "data_complete": true,
            "source_policy": "task_allowlisted_historical_run_only"
        }
    }))
}

fn retrieval_policy_for_role(role: &str, kind: &str, config: &RetrievalConfig) -> RetrievalPolicy {
    let policy = |required_source_phases: &[i64],
                  required_detail_source_phases: &[i64],
                  minimum: usize,
                  maximum: usize| RetrievalPolicy {
        mandatory_summary_query: true,
        required_source_phases: required_source_phases.to_vec(),
        required_detail_source_phases: required_detail_source_phases.to_vec(),
        minimum_detail_expansions: minimum,
        maximum_detail_expansions: maximum,
        summary_page_limit: config.summary_page_limit,
        detail_page_limit: config.detail_page_limit,
        allow_empty_when_no_visible_summary: true,
        allowed_direct_contexts: vec![
            "rust_control_plane".to_string(),
            "current_phase_packet".to_string(),
            "current_task".to_string(),
        ],
    };
    match (role, kind) {
        ("reflector.historical", _) => policy(&[], &[], 1, config.reflection_max_details),
        ("mediator.topic", "warmup") => policy(&[1], &[], 0, 2),
        ("mediator.topic", _) => policy(&[1], &[1], 1, config.phase2_max_details),
        ("researcher.bull" | "researcher.bear", _) => {
            policy(&[1], &[1], 1, config.phase2_max_details)
        }
        ("researcher.bull.initial" | "researcher.bear.initial", _) => {
            policy(&[1], &[1], 1, config.phase2_max_details)
        }
        ("researcher.bull.interaction" | "researcher.bear.interaction", _) => {
            policy(&[1], &[1], 1, config.phase2_max_details)
        }
        ("mediator.topic_controller", _) => policy(&[1], &[], 0, config.phase2_max_details),
        // The Research Manager is the only role allowed to translate the
        // Phase 2 hinge into a probability adjustment.  Reading only the
        // Phase 1 baseline would make a successful Phase 3 completion look
        // valid while silently bypassing the debate.
        ("manager.research", _) => policy(&[1, 2], &[1, 2], 2, config.phase3_max_details),
        // Indexes carry the validated execution fields.  Detail expansion is
        // available for extra context but must not reject an otherwise valid
        // free-text response when the model has already read the Index.
        ("trader", _) => policy(&[3], &[], 0, config.phase4_max_details),
        ("risk.aggressive" | "risk.neutral" | "risk.conservative", _) => {
            policy(&[3, 4], &[], 0, config.phase5_max_details)
        }
        ("portfolio.manager", _) => policy(&[3, 4, 5], &[], 0, config.phase6_max_details),
        _ => RetrievalPolicy::default(),
    }
}

fn phase2_debug_output_path(
    phase: i64,
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
    _round: Option<i64>,
) -> Option<PathBuf> {
    if role == "compressor.phase_summary" {
        let file = if phase == 2 && kind == "phase2_extraction" {
            "phase2_extraction.json"
        } else {
            &format!("phase{phase}_summary.json")
        };
        return Some(
            PathBuf::from(format!("outputs/debug/phase{phase}"))
                .join(if phase == 2 && kind == "phase2_extraction" {
                    "extraction"
                } else {
                    "summary"
                })
                .join(file),
        );
    }
    if phase != 2 {
        return None;
    }
    if role == "mediator.topic" {
        return Some(PathBuf::from(if kind == "warmup" {
            "outputs/debug/phase2/phase2-warmup-shared.json"
        } else {
            "outputs/debug/phase2/topic-generator.json"
        }));
    }
    let topic_id = topic_id?;
    let safe_topic_id: String = topic_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let topic_dir = if safe_topic_id.starts_with("topic-") || safe_topic_id.starts_with("topic_") {
        safe_topic_id
    } else {
        format!("topic-{safe_topic_id}")
    };
    let file = if role == "mediator.topic_controller" {
        "topic-controller.json"
    } else if role == "researcher.bull" || role.contains(".bull.") {
        "debate-bull.json"
    } else if role == "researcher.bear" || role.contains(".bear.") {
        "debate-bear.json"
    } else {
        return None;
    };
    Some(
        PathBuf::from("outputs/debug/phase2")
            .join(topic_dir)
            .join(file),
    )
}

pub(crate) async fn run_role_jobs(
    jobs: Vec<RoleJob>,
    parallelism: usize,
    timeout_sec: u64,
) -> Vec<RoleJobResult> {
    debug!(
        job_count = jobs.len(),
        parallelism = parallelism.max(1),
        timeout_sec,
        "running role jobs"
    );
    stream::iter(jobs)
        .map(|job| run_role_job_with_timeout(job, timeout_sec))
        .buffer_unordered(parallelism.max(1))
        .collect()
        .await
}

fn debug_prompt_path_from_runtime_path(path: &str) -> Option<PathBuf> {
    let path = PathBuf::from(path);
    let project_root = default_project_root();
    path.strip_prefix(&project_root)
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            path.to_str().and_then(|value| {
                value
                    .find("prompts/")
                    .map(|index| PathBuf::from(&value[index..]))
            })
        })
}

pub(crate) async fn run_role_job_with_timeout(job: RoleJob, timeout_sec: u64) -> RoleJobResult {
    let role = job.role.clone();
    let phase = job.phase;
    let kind = job.kind.clone();
    let round = job.round;
    let topic_id = job.topic_id.clone();
    let prompt_version = job.prompt_version.clone();
    let started_at = Instant::now();
    debug!(
        role,
        phase, kind, round, topic_id, timeout_sec, "role job starting"
    );

    // Live gateway 503s can exhaust stream-level retries; retry the whole role a
    // couple of times before surfacing a critical failure.
    const MAX_ROLE_ATTEMPTS: usize = 3;
    let mut attempt = 0usize;
    let result = loop {
        attempt += 1;
        match time::timeout(
            Duration::from_secs(timeout_sec.max(1)),
            execute_role_job(job.clone()),
        )
        .await
        {
            Ok(Ok(output)) => break Ok(output),
            Ok(Err(error)) => {
                // Use the full chain so permanent upstream messages (e.g. context
                // window full) are not masked by outer "LLM stream chunk failed".
                let message = format!("{error:#}");
                if attempt < MAX_ROLE_ATTEMPTS && is_transient_role_error(&message) {
                    let backoff_ms = backoff_ms(&role, attempt);
                    warn!(
                        role = role.as_str(),
                        phase,
                        kind = kind.as_str(),
                        attempt,
                        backoff_ms,
                        error = %message,
                        "retrying transient role job failure"
                    );
                    time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                break Err((message, false));
            }
            Err(_) => {
                let message = format!("role execution timed out after {timeout_sec}s");
                if attempt < MAX_ROLE_ATTEMPTS {
                    let backoff_ms = backoff_ms(&role, attempt);
                    warn!(
                        role = role.as_str(),
                        phase,
                        kind = kind.as_str(),
                        attempt,
                        backoff_ms,
                        error = %message,
                        "retrying timed-out role job"
                    );
                    time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                break Err((message, true));
            }
        }
    };

    match result {
        Ok(output) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            debug!(role, phase, kind, elapsed_ms, "role job completed");
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                prompt_version,
                model: output
                    .artifact
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                turn_id: output.turn_id,
                session_id: output.session_id,
                artifact: Some(output.artifact),
                error: None,
                timed_out: false,
                elapsed_ms,
                llm_ms: output.metrics.llm_ms,
                tool_ms: output.metrics.tool_ms,
                usage: output.metrics.usage,
                turn_count: output.metrics.turn_count,
                tool_call_count: output.metrics.tool_call_count,
            }
        }
        Err((message, timed_out)) => {
            let elapsed_ms = started_at.elapsed().as_millis();
            warn!(
                role,
                phase,
                kind,
                elapsed_ms,
                error = %message,
                timed_out,
                "role job failed"
            );
            RoleJobResult {
                role,
                phase,
                kind,
                round,
                topic_id,
                prompt_version,
                model: String::new(),
                turn_id: String::new(),
                session_id: String::new(),
                artifact: None,
                error: Some(message),
                timed_out,
                elapsed_ms,
                llm_ms: 0,
                tool_ms: 0,
                usage: TokenUsage::default(),
                turn_count: 0,
                tool_call_count: 0,
            }
        }
    }
}

async fn execute_role_job(job: RoleJob) -> Result<AgentLoopOutput> {
    if job.mock {
        return Ok(mock_free_text_output(job));
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let debug_prompt_path = job
        .prompt_path
        .as_deref()
        .and_then(debug_prompt_path_from_runtime_path);
    let debug_round = job.round.and_then(|round| usize::try_from(round).ok());
    let phase2_context = job.tools.phase2_context.clone();
    let settings = AgentSettings {
        role: job.role,
        phase: Some(job.phase),
        topic_id: job.topic_id,
        debug_prompt_path,
        debug_output_path: job.debug_output_path,
        debug_round,
        debug_turn_id: None,
        tickers: job.tickers,
        tool_managed_profile: job.tool_managed_profile,
        index_tool_runtime: job.index_tool_runtime.clone(),
        experience_retrieval: job.experience_retrieval.clone(),
        evidence_research: job.evidence_research.clone(),
        session_runtime: job.session_runtime.clone(),
        llm,
        reasoning_effort_override: job.reasoning_effort_override,
        tools: Some(job.tools),
        web_search: job.web_search,
        truncation: job.truncation,
        debug: job.debug,
        retrieval_policy: job.retrieval_policy,
    };
    debug!(
        role = settings.role,
        model = settings.llm.model,
        prompt_chars = job.prompt.len(),
        "calling agent loop"
    );
    let mut output = if let Some(phase2_context) = phase2_context {
        let session_id = settings.session_runtime.manifest().session_id.clone();
        let turn_id = role_turn_id(&session_id, job.phase2_turn_key.as_deref());
        let fork_from_turn_id = phase2_context
            .get("fork_from_turn_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let include_prompt_on_fork = phase2_context
            .get("include_prompt_on_fork")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let injected_user_message = phase2_context
            .get("stree_injection")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        run_agent_fork_loop_with_metrics(
            &settings,
            ForkLoopInput {
                session_id,
                turn_id,
                prompt: &job.prompt,
                fork_from_turn_id,
                include_prompt_on_fork,
                injected_user_message,
            },
        )
        .await?
    } else {
        run_agent_loop_with_metrics(&settings, &job.prompt).await?
    };
    output.artifact["context_manifest"] = job.context_manifest;
    Ok(output)
}

fn role_turn_id(session_id: &str, phase2_turn_key: Option<&str>) -> String {
    let turn_identity = phase2_turn_key
        .map(|key| format!("{session_id}:{key}"))
        .unwrap_or_else(|| session_id.to_owned());
    format!("turn-{}", md5_3(&turn_identity))
}

fn mock_free_text_output(job: RoleJob) -> AgentLoopOutput {
    let response_text = format!(
        "Mock {} response for phase {} kind {} and tickers {}.",
        job.role,
        job.phase,
        job.kind,
        job.tickers.join(", ")
    );
    AgentLoopOutput {
        artifact: json!({
            "phase": job.phase,
            "role": job.role,
            "response_text": response_text,
        }),
        terminal_tool_result: None,
        metrics: ModelStreamResult::default(),
        turn_id: format!("turn-mock-{}-{}", job.phase, job.kind),
        session_id: format!("session-mock-{}-{}", job.phase, job.role),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(role: &str, timed_out: bool, elapsed_ms: u128) -> RoleJobResult {
        let llm_ms = elapsed_ms / 2;
        let tool_ms = elapsed_ms / 4;
        RoleJobResult {
            role: role.to_string(),
            phase: 3,
            kind: "artifact".to_string(),
            round: None,
            topic_id: None,
            prompt_version: Some("v1".to_string()),
            model: "test-model".to_string(),
            turn_id: "turn-1".to_string(),
            session_id: "session-1".to_string(),
            artifact: if timed_out { None } else { Some(json!({})) },
            error: timed_out.then(|| "timeout".to_string()),
            timed_out,
            elapsed_ms,
            llm_ms,
            tool_ms,
            usage: TokenUsage {
                input_tokens: 10,
                output_tokens: 4,
                cached_tokens: 2,
                reasoning_tokens: 0,
                total_tokens: 14,
            },
            turn_count: 1,
            tool_call_count: 3,
        }
    }

    #[test]
    fn wait_ms_is_total_minus_llm_and_tool() {
        let job = result("manager.research", false, 100);
        assert_eq!(job.llm_ms, 50);
        assert_eq!(job.tool_ms, 25);
        assert_eq!(job.wait_ms(), 25);
    }

    #[test]
    fn phase_summary_read_scope_is_profile_exact() {
        for (profile, expected) in [
            (ToolManagedProfile::TopicGeneration, vec![1]),
            (ToolManagedProfile::ResearcherWarmup, vec![1]),
            (ToolManagedProfile::DebateSeed, vec![1]),
            (ToolManagedProfile::DebateResponse, vec![1]),
            (ToolManagedProfile::TopicControl, vec![1]),
            (ToolManagedProfile::ResearchDecision, vec![1, 2]),
            (ToolManagedProfile::TradeIntent, vec![3]),
            (ToolManagedProfile::RiskReview, vec![3, 4]),
            (ToolManagedProfile::PortfolioDecision, vec![3, 4, 5]),
        ] {
            assert_eq!(
                phase_summary_source_phases(profile)
                    .unwrap()
                    .into_iter()
                    .collect::<Vec<_>>(),
                expected
            );
        }
        assert!(phase_summary_source_phases(ToolManagedProfile::AnalystReport).is_err());
        assert!(phase_summary_source_phases(ToolManagedProfile::EvidenceResearch).is_err());
        assert!(phase_summary_source_phases(ToolManagedProfile::PhaseSummary).is_err());
    }

    #[test]
    fn phase2_topic_and_debate_require_detail_before_completion() {
        let config = RetrievalConfig {
            summary_page_limit: 20,
            detail_page_limit: 20,
            phase2_max_details: 4,
            phase3_max_details: 6,
            phase4_max_details: 2,
            phase5_max_details: 4,
            phase6_max_details: 8,
            reflection_max_details: 8,
        };
        for (role, kind) in [
            ("mediator.topic", "topic_generation"),
            ("researcher.bull.initial", "bull_seed"),
            ("researcher.bear.interaction", "interaction"),
        ] {
            let policy = retrieval_policy_for_role(role, kind, &config);
            assert!(policy.mandatory_summary_query);
            assert_eq!(policy.required_source_phases, vec![1]);
            assert_eq!(policy.minimum_detail_expansions, 1);
            assert_eq!(policy.required_detail_source_phases, vec![1]);
        }
        assert_eq!(
            retrieval_policy_for_role("mediator.topic", "warmup", &config)
                .minimum_detail_expansions,
            0
        );
    }

    #[test]
    fn research_manager_requires_phase1_and_phase2_detail_before_completion() {
        let config = RetrievalConfig {
            summary_page_limit: 20,
            detail_page_limit: 20,
            phase2_max_details: 4,
            phase3_max_details: 6,
            phase4_max_details: 2,
            phase5_max_details: 4,
            phase6_max_details: 8,
            reflection_max_details: 8,
        };

        let policy = retrieval_policy_for_role("manager.research", "artifact", &config);

        assert!(policy.mandatory_summary_query);
        assert_eq!(policy.required_source_phases, vec![1, 2]);
        assert_eq!(policy.required_detail_source_phases, vec![1, 2]);
        assert_eq!(policy.minimum_detail_expansions, 2);
        assert_eq!(policy.maximum_detail_expansions, 6);
    }

    #[test]
    fn web_evidence_packet_gets_rust_owned_ids_and_source_cap() {
        let entries = (0..6)
            .map(|index| {
                json!({
                    "claim": format!("fact-{index}"),
                    "relation": "supports",
                    "source_url": format!("https://example.com/{index}"),
                    "publisher": "Example",
                    "published_at": Value::Null,
                    "source_tier": "official"
                })
            })
            .collect::<Vec<_>>();
        let response = json!({
            "status": "supported",
            "evidence": entries,
            "counterevidence": [],
            "unresolved_gaps": [],
            "search_queries": ["one", "two"]
        })
        .to_string();

        let packet = normalize_web_evidence_packet(&response, "web-abcdef").unwrap();
        let evidence = packet["evidence"].as_array().unwrap();
        assert_eq!(evidence.len(), 5);
        assert_eq!(packet["source_count"], 5);
        assert!(evidence.iter().all(|item| {
            item["evidence_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("web-") && id.len() == 68)
        }));
        assert!(evidence
            .iter()
            .all(|item| item["request_id"] == "web-abcdef"));
    }

    #[test]
    fn web_evidence_packet_rejects_non_http_sources() {
        let response = json!({
            "status": "supported",
            "evidence": [{
                "claim": "fact",
                "relation": "supports",
                "source_url": "file:///tmp/fake",
                "publisher": "Fake",
                "published_at": null,
                "source_tier": "secondary"
            }],
            "counterevidence": []
        })
        .to_string();
        assert!(normalize_web_evidence_packet(&response, "web-abcdef")
            .unwrap_err()
            .to_string()
            .contains("http"));
    }

    #[test]
    fn web_evidence_packet_reads_model_json_before_runtime_provenance() {
        let model_response = json!({
            "status": "supported",
            "evidence": [{
                "claim": "The filing is available.",
                "relation": "supports",
                "source_url": "https://www.sec.gov/example",
                "publisher": "SEC",
                "published_at": null,
                "source_tier": "primary"
            }],
            "counterevidence": []
        });
        let response = format!(
            "{}\n\n{}\n{}",
            model_response,
            orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER,
            json!([{
                "evidence_id": "web-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "source_url": "https://www.sec.gov/example",
                "title": "SEC filing"
            }])
        );

        let packet = normalize_web_evidence_packet(&response, "request-1").unwrap();
        assert_eq!(packet["source_count"], 1);
        assert_eq!(
            packet["evidence"][0]["source_url"],
            "https://www.sec.gov/example"
        );

        let unverified_response = format!(
            "{}\n\n{}\n{}",
            model_response,
            orchestrator_llm::tools::web_run::VERIFIED_RESULTS_MARKER,
            json!([{
                "evidence_id": "web-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "source_url": "https://unverified.example",
                "title": "different source"
            }])
        );
        assert!(
            normalize_web_evidence_packet(&unverified_response, "request-1")
                .unwrap_err()
                .to_string()
                .contains("Rust-verified Web search results")
        );
    }

    #[test]
    fn web_evidence_cap_keeps_counterevidence() {
        let evidence = (0..5)
            .map(|index| {
                json!({
                    "claim": format!("support-{index}"),
                    "relation": "supports",
                    "source_url": format!("https://example.com/support/{index}"),
                    "publisher": "Example",
                    "published_at": null,
                    "source_tier": "official"
                })
            })
            .collect::<Vec<_>>();
        let response = json!({
            "status": "mixed",
            "evidence": evidence,
            "counterevidence": [{
                "claim": "counter",
                "relation": "refutes",
                "source_url": "https://example.com/counter",
                "publisher": "Example",
                "published_at": null,
                "source_tier": "official"
            }]
        })
        .to_string();

        let packet = normalize_web_evidence_packet(&response, "web-abcdef").unwrap();
        assert_eq!(packet["evidence"].as_array().unwrap().len(), 4);
        assert_eq!(packet["counterevidence"].as_array().unwrap().len(), 1);
        assert_eq!(packet["source_count"], 5);
    }

    #[test]
    fn phase2_debug_paths_follow_the_checkpoint_and_topic_tree() {
        assert_eq!(
            phase2_debug_output_path(1, "compressor.phase_summary", "phase_summary", None, None),
            Some(PathBuf::from(
                "outputs/debug/phase1/summary/phase1_summary.json"
            ))
        );
        assert_eq!(
            phase2_debug_output_path(2, "mediator.topic", "warmup", None, Some(0)),
            Some(PathBuf::from(
                "outputs/debug/phase2/phase2-warmup-shared.json"
            ))
        );
        assert_eq!(
            phase2_debug_output_path(2, "mediator.topic", "topic_generation", None, None),
            Some(PathBuf::from("outputs/debug/phase2/topic-generator.json"))
        );
        assert_eq!(
            phase2_debug_output_path(
                2,
                "compressor.phase_summary",
                "phase2_extraction",
                None,
                None
            ),
            Some(PathBuf::from(
                "outputs/debug/phase2/extraction/phase2_extraction.json"
            ))
        );
        for (role, file) in [
            ("researcher.bull.initial", "debate-bull.json"),
            ("researcher.bear.interaction", "debate-bear.json"),
            ("mediator.topic_controller", "topic-controller.json"),
        ] {
            assert_eq!(
                phase2_debug_output_path(2, role, "debate", Some("QQQ/risk"), Some(1)),
                Some(PathBuf::from("outputs/debug/phase2/topic-QQQ_risk").join(file))
            );
        }
        assert_eq!(
            phase2_debug_output_path(
                2,
                "researcher.bull.interaction",
                "interaction",
                Some("QQQ/risk"),
                Some(2),
            ),
            phase2_debug_output_path(
                2,
                "researcher.bull.initial",
                "bull_seed",
                Some("QQQ/risk"),
                Some(0),
            )
        );
        assert_eq!(
            phase2_debug_output_path(
                2,
                "researcher.bull.initial",
                "bull_seed",
                Some("topic_vix"),
                Some(0),
            ),
            Some(PathBuf::from(
                "outputs/debug/phase2/topic_vix/debate-bull.json"
            ))
        );
    }

    #[test]
    fn phase2_context_records_topic_round_and_fork_parent() {
        let context = phase2_context_payload(
            &json!({
                "topic_debate_states": {
                    "topic-a": {
                        "topic": {"topic_id": "topic-a"},
                        "controller_artifact": {
                            "payload": {"next_steers": [{"steer_id": "steer-1"}]}
                        }
                    }
                }
            }),
            "researcher.bull",
            Some("topic-a"),
            Some(1),
            Some("warmup-turn"),
        )
        .unwrap();

        assert_eq!(context["kind"], "stree_debate");
        assert_eq!(context["topic_id"], "topic-a");
        assert_eq!(context["round"], 1);
        assert_eq!(context["round_num"], 1);
        assert_eq!(context["fork_from_turn_id"], "warmup-turn");
        assert_eq!(context["include_prompt_on_fork"], true);
        assert!(context.get("controller").is_none());
    }

    #[test]
    fn phase2_interaction_uses_shared_topic_tree_and_complete_prior_turns() {
        let state = json!({
            "phase2_warmup": {"session_id":"warmup-session", "turn_id":"warmup-turn"},
            "topic_debate_states": {
                "topic-a": {
                    "topic": {"topic_id": "topic-a"},
                    "turns": [
                        {
                            "role": "researcher.bull.initial",
                            "round": 0,
                            "artifact": {"payload": {"claims": [{"claim": "bull"}]}}
                        },
                        {
                            "role": "researcher.bear.initial",
                            "round": 0,
                            "artifact": {"payload": {"claims": [{"claim": "bear"}]}}
                        },
                        {
                            "role": "researcher.bull.interaction",
                            "round": 1,
                            "artifact": {"payload": {"replies": [{"reason": "bull reply"}]}}
                        }
                    ],
                    "controller_turns": [
                        {
                            "role": "mediator.topic_controller",
                            "round": 0,
                            "artifact": {"payload": {"next_steers": [{"steer_id": "s1"}]}}
                        }
                    ],
                    "controller_artifact": {
                        "payload": {"next_steers": [{"steer_id": "s1"}]}
                    }
                }
            }
        });

        for role in ["researcher.bull", "researcher.bear"] {
            let fork = phase2_fork_reference(&state, role, Some("topic-a"), Some(1))
                .expect("each canonical participant session forks from warmup once");
            assert_eq!(fork.fork_from_session_id, "warmup-session");
            assert_eq!(fork.fork_from_turn_id, "warmup-turn");

            let context = phase2_context_payload(
                &state,
                role,
                Some("topic-a"),
                Some(1),
                Some(&fork.fork_from_turn_id),
            )
            .unwrap();
            let turns = context["debate_turns"].as_array().unwrap();
            assert_eq!(
                turns
                    .iter()
                    .map(|turn| turn["role"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                vec![
                    "mediator.topic_controller",
                    "researcher.bull.initial",
                    "researcher.bear.initial",
                    "researcher.bull.interaction",
                ]
            );
            assert!(context.get("controller").is_none());
        }
    }

    #[test]
    fn phase2_controller_context_transfers_recorded_debate_turns() {
        let context = phase2_context_payload(
            &json!({
                "topic_debate_states": {
                    "topic-a": {
                        "topic": {"topic_id": "topic-a", "title": "Volatility regime"},
                        "turns": [
                            {"role": "researcher.bull", "artifact": {"summary": "bull"}},
                            {"role": "researcher.bear", "artifact": {"summary": "bear"}}
                        ],
                        "controller_turns": [
                            {"role": "mediator.topic_controller", "round": 0, "artifact": {"summary": "controller"}}
                        ]
                    }
                }
            }),
            "mediator.topic_controller",
            Some("topic-a"),
            Some(0),
            Some("topic-generator-turn"),
        )
        .unwrap();

        assert_eq!(context["kind"], "topic_control");
        assert_eq!(context["topic"]["title"], "Volatility regime");
        assert_eq!(context["debate_turns"].as_array().unwrap().len(), 3);
        assert_eq!(
            context["debate_turns"][2]["role"],
            "mediator.topic_controller"
        );
        assert!(context.get("controller").is_none());
    }

    #[test]
    fn phase2_json_debug_files_retain_structured_messages() {
        let temp = tempfile::tempdir().unwrap();
        let path = phase2_debug_output_path(
            2,
            "researcher.bull.initial",
            "bull_seed",
            Some("topic-a"),
            Some(0),
        )
        .unwrap();
        orchestrator_llm::append_debug_output_record(
            temp.path(),
            &path,
            "prompts/phase2/researcher/debate.md",
            json!({
                "kind": "stream",
                "req": {
                    "messages": [
                        {"role": "assistant", "content": "准备完毕"},
                        {"role": "user", "content": "BULL ROLE PROMPT\n\nSteer: topic-a"}
                    ]
                },
                "resp": {"status": "completed"}
            }),
        )
        .unwrap();

        let output: Value =
            serde_json::from_str(&std::fs::read_to_string(temp.path().join(path)).unwrap())
                .unwrap();
        assert_eq!(output["req"]["messages"][0]["content"], "准备完毕");
        assert_eq!(output["req"]["messages"][1]["role"], "user");
    }

    #[test]
    fn phase2_delivery_turn_keys_are_stable_for_retries_and_distinct_between_deliveries() {
        let first = role_turn_id("session", Some("delivery-a"));
        let retry = role_turn_id("session", Some("delivery-a"));
        let next = role_turn_id("session", Some("delivery-b"));

        assert_eq!(first, retry);
        assert_ne!(first, next);
    }

    #[test]
    fn records_role_job_metrics_and_aggregates() {
        let mut state = json!({"run_id":"run-test"});

        record_role_job_metrics(&mut state, &result("manager.research", false, 7));
        record_role_job_metrics(&mut state, &result("trader", true, 11));

        assert_eq!(state["role_job_metrics"].as_array().unwrap().len(), 2);
        assert_eq!(state["role_job_metrics"][0]["prompt_version"], "v1");
        assert_eq!(state["role_job_metrics"][0]["input_tokens"], 10);
        assert_eq!(state["role_job_metrics"][0]["output_tokens"], 4);
        assert_eq!(state["role_job_metrics"][0]["cached_tokens"], 2);
        assert_eq!(state["role_job_metrics"][0]["reasoning_tokens"], 0);
        assert_eq!(state["role_job_metrics"][0]["total_tokens"], 14);
        assert_eq!(state["role_job_metrics"][0]["non_cached_input_tokens"], 8);
        assert_eq!(state["role_job_metrics"][0]["visible_output_tokens"], 4);
        assert_eq!(state["role_job_metrics"][0]["model"], "test-model");
        assert_eq!(state["role_job_metrics"][0]["run_id"], "run-test");
        assert_eq!(state["role_job_metrics"][0]["session_id"], "session-1");
        assert_eq!(state["role_job_metrics"][0]["turn_id"], "turn-1");
        assert_eq!(state["role_job_metrics"][0]["turn_count"], 1);
        assert_eq!(state["role_job_metrics"][0]["tool_call_count"], 3);
        assert_eq!(state["workflow_metrics"]["role_job_count"], 2);
        assert_eq!(state["workflow_metrics"]["llm_call_count"], 2);
        assert_eq!(state["workflow_metrics"]["tool_call_count"], 6);
        assert_eq!(state["workflow_metrics"]["input_tokens"], 20);
        assert_eq!(state["workflow_metrics"]["output_tokens"], 8);
        assert_eq!(state["workflow_metrics"]["total_tokens"], 28);
        assert_eq!(state["workflow_metrics"]["total_role_elapsed_ms"], 18);
        assert_eq!(state["workflow_metrics"]["timed_out_role_count"], 1);
    }
}
