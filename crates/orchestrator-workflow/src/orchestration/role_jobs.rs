use anyhow::{bail, Context, Result};
use chrono::Utc;
use futures::{stream, StreamExt};
use orchestrator_core::{default_project_root, ToolManagedProfile};
use orchestrator_llm::{
    agent_loop::{
        FileStoreSessionRuntime, ModelStreamResult, RetrievalPolicy, SessionRuntimeSpec,
        TokenUsage, ToolResultItem, Turn,
    },
    run_agent_loop_with_metrics,
    tools::{ExternalToolConfig, FileStoreInputSnapshot},
    truncation::TruncationConfig,
    AgentLoopOutput, AgentSettings, RoleLlmSettings,
};
use serde_json::{json, Map, Value};
use std::time::{Duration, Instant};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};
use tokio::time;
use tracing::{debug, warn};

use super::config::{prompt_version, RetrievalConfig, RuntimeConfig};
use super::domain_runtime::{file_store_domain_runtime, FileStoreDomainRuntimePlan};
use super::index_runtime::{file_store_index_tool_runtime, FileStoreIndexRuntimePlan};
use super::lifecycle::tickers_from_state;
use super::render::{direct_context_manifest, render_prompt_with_plugins};

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
    pub domain_tool_runtime:
        Option<orchestrator_llm::tools::domain_tools::DomainToolRuntimeBinding>,
    pub session_runtime: FileStoreSessionRuntime,
    /// Per-role typed Draft/Index write-attempt budget from runtime config.
    pub max_write_calls: Option<usize>,
    pub llm: Option<RoleLlmSettings>,
    pub reasoning_effort_override: Option<String>,
    pub tools: ExternalToolConfig,
    pub web_search: orchestrator_llm::web_search::WebSearchConfig,
    pub truncation: TruncationConfig,
    pub retrieval_policy: RetrievalPolicy,
    pub context_manifest: Value,
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
    let prompt_key = match role {
        "reflector.historical" => "orchestrator.prompts.reflection.historical",
        "analyst.technical" => "orchestrator.prompts.analyst.technical",
        "analyst.news_macro" => "orchestrator.prompts.analyst.news_macro",
        "compressor.phase_summary" => "orchestrator.prompts.compressor.phase_summary",
        "mediator.topic" => "orchestrator.prompts.phase2.topic_generator",
        "researcher.bull.initial" => "orchestrator.prompts.phase2.bull_initial",
        "researcher.bull.interaction" => "orchestrator.prompts.phase2.bull_interaction",
        "researcher.bear.initial" => "orchestrator.prompts.phase2.bear_initial",
        "researcher.bear.interaction" => "orchestrator.prompts.phase2.bear_interaction",
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
        "researcher.bull.initial" | "researcher.bear.initial" => {
            Some(ToolManagedProfile::DebateSeed)
        }
        "researcher.bull.interaction" | "researcher.bear.interaction" => {
            Some(ToolManagedProfile::DebateResponse)
        }
        "mediator.topic_controller" => Some(ToolManagedProfile::TopicControl),
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

/// Only structured evidence IDs may cross into a DomainTool scope.  Until the
/// FileStore Session adapter records an `evidence_read` event, live roles get
/// an empty set and a write that cites an invented ID fails closed.  Mock uses
/// deterministic IDs so it can exercise the same Draft/finalize path.
fn visible_domain_evidence_refs(
    state: &Value,
    role: &str,
    tickers: &[String],
    mock: bool,
) -> BTreeSet<String> {
    let mut visible = state
        .get("visible_evidence_refs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|reference| !reference.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    if mock {
        visible.extend(tickers.iter().map(|ticker| format!("mock:{role}:{ticker}")));
        // Phase 2 aggregate units intentionally have no ticker scope, but
        // their typed tools still need a structured, Rust-owned mock evidence
        // reference to exercise the same finalize invariants as live runs.
        if tickers.is_empty() && phase2_side_for_role(role).is_some() || role == "mediator.topic" {
            visible.insert("mock:phase2:shared".to_owned());
        }
    }
    visible
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
        domain_tool_runtime,
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
        let visible = visible_domain_evidence_refs(&state, role, &tickers, mock);
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
                registration.tool_allowlist.clone(),
            )
        } else if profile == ToolManagedProfile::PhaseSummary {
            let binding = file_store_phase_summary_index_runtime(
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
                registration.tool_allowlist.clone(),
            )
        } else {
            let binding = file_store_domain_runtime(
                Path::new(store_root),
                &state,
                FileStoreDomainRuntimePlan {
                    role: role.to_owned(),
                    phase,
                    profile,
                    profile_version: registration.profile_version,
                    builder_version: registration.builder_version,
                    tickers: tickers.clone(),
                    visible_evidence_refs: visible,
                    topic_id: topic_id.map(ToOwned::to_owned),
                    side: phase2_side_for_role(role).map(ToOwned::to_owned),
                    round: round.and_then(|value| u32::try_from(value).ok()),
                    visible_claims: visible_phase2_claims(&state, topic_id),
                    fork: phase2_fork_reference(&state, role, topic_id),
                    trade_candidate_action: trade_candidate_action(&state, &tickers),
                    portfolio_rating: portfolio_rating(&state, &tickers),
                    portfolio_current_weight: portfolio_current_weight(&state, &tickers),
                },
            )?;
            let input = if profile == ToolManagedProfile::AnalystReport && !mock {
                Some(file_store_input_from_state(&state)?)
            } else {
                None
            };
            (
                profile,
                Some(file_store_domain_index_read_runtime(
                    Path::new(store_root),
                    &state,
                    role,
                    phase,
                    &tickers,
                )?),
                Some(binding),
                input,
                registration.tool_allowlist.clone(),
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
    let session_runtime = file_store_session_runtime(
        &state,
        role,
        phase,
        topic_id,
        round,
        tool_managed_profile,
        phase2_fork_reference(&state, role, topic_id),
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
        debug_output_path: phase2_debug_output_path(phase, role, kind, topic_id),
        prompt_version,
        tickers: tickers.clone(),
        tool_managed_profile,
        index_tool_runtime,
        domain_tool_runtime,
        session_runtime,
        max_write_calls: Some(config.tool_managed.max_write_calls_per_role),
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
        },
        web_search: config.web_search.get(role).cloned().unwrap_or_default(),
        truncation: config.truncation.clone(),
        retrieval_policy: retrieval_policy_for_role(role, kind, &config.retrieval),
        context_manifest: direct_context_manifest(&state, phase),
    })
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
    tickers: &[String],
) -> Result<orchestrator_llm::tools::index_tools::IndexToolRuntimeBinding> {
    use orchestrator_llm::tools::index_tools::{IndexKind, IndexOwnedScope, IndexReadVisibility};
    use orchestrator_store::{content_hash, FileStore, FileStoreOptions, RunLocation};

    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .context("domain Index reader requires run_id")?;
    let current_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .context("domain Index reader requires current_date")?;
    let phase_u8 = u8::try_from(phase).context("domain Index reader phase must fit u8")?;
    let source_payload_hash = content_hash(&json!({
        "reader_role": role,
        "phase": phase,
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
        kinds: BTreeSet::from([IndexKind::PhaseSummary, IndexKind::Experience]),
        tickers: tickers.iter().cloned().collect(),
        source_phases: (0..phase_u8).collect(),
        applies_to_phases: BTreeSet::from([phase_u8]),
        max_page_size: 20,
        ..Default::default()
    };
    file_store_index_tool_runtime(
        FileStore::open(store_root, FileStoreOptions::default())?,
        owned,
        visibility,
        FileStoreIndexRuntimePlan::read_only(
            vec![RunLocation::new(current_date, run_id)?],
            Utc::now().to_rfc3339(),
        ),
    )
}

/// These values are intentionally projected before constructing the domain
/// binding.  The model never receives either as a writable parameter.
fn trade_candidate_action(state: &Value, tickers: &[String]) -> Option<String> {
    let ticker = tickers.first()?;
    state
        .get("research_plan")
        .and_then(|plan| plan.get("per_ticker"))
        .and_then(|items| items.get(ticker))
        .and_then(|item| item.get("rating"))
        .or_else(|| {
            state
                .get("research_plan")
                .and_then(|plan| plan.get("rating"))
        })
        .and_then(Value::as_str)
        .map(|rating| match rating {
            "Buy" | "Overweight" => "Buy",
            "Sell" | "Underweight" => "Sell",
            _ => "Hold",
        })
        .map(ToOwned::to_owned)
}

fn portfolio_rating(state: &Value, tickers: &[String]) -> Option<String> {
    let ticker = tickers.first()?;
    state
        .get("research_plan")
        .and_then(|plan| plan.get("per_ticker"))
        .and_then(Value::as_object)
        .and_then(|items| items.get(ticker))
        .and_then(|item| item.get("rating"))
        .or_else(|| {
            state
                .get("research_plan")
                .and_then(|plan| plan.get("rating"))
        })
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn portfolio_current_weight(state: &Value, tickers: &[String]) -> Option<f64> {
    let ticker = tickers.first()?;
    state
        .get("account")
        .and_then(|account| account.get("positions"))
        .and_then(|positions| positions.get(ticker))
        .and_then(|position| position.get("weight"))
        .and_then(Value::as_f64)
        .or_else(|| {
            state
                .get("current_portfolio_weights")
                .and_then(Value::as_object)
                .and_then(|weights| weights.get(ticker))
                .and_then(Value::as_f64)
        })
        .or(Some(0.0))
}

fn phase2_side_for_role(role: &str) -> Option<&'static str> {
    if role.contains(".bull.") {
        Some("bull")
    } else if role.contains(".bear.") {
        Some("bear")
    } else {
        None
    }
}

fn visible_phase2_claims(state: &Value, topic_id: Option<&str>) -> BTreeSet<String> {
    let Some(topic_id) = topic_id else {
        return BTreeSet::new();
    };
    state
        .pointer(&format!("/topic_debate_states/{topic_id}/turns"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|turn| {
            turn.pointer("/artifact/payload/claims")
                .or_else(|| turn.pointer("/artifact/claims"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|claim| claim.get("claim_id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect()
}

fn phase2_fork_reference(
    state: &Value,
    role: &str,
    topic_id: Option<&str>,
) -> Option<orchestrator_store::ForkReference> {
    let (session_id, turn_id) = if role == "mediator.topic_controller" {
        let _ = topic_id?;
        (
            state
                .get("topic_generation_session_id")?
                .as_str()?
                .to_owned(),
            state.get("topic_generation_turn_id")?.as_str()?.to_owned(),
        )
    } else if matches!(role, "researcher.bull.initial" | "researcher.bear.initial") {
        let _ = topic_id?;
        let warmup = state.get("phase2_warmup")?;
        (
            warmup.get("session_id")?.as_str()?.to_owned(),
            warmup.get("turn_id")?.as_str()?.to_owned(),
        )
    } else if role == "researcher.bull.interaction" || role == "researcher.bear.interaction" {
        let topic_id = topic_id?;
        let side = if role.contains("bull") {
            "bull"
        } else {
            "bear"
        };
        let source = state.pointer(&format!("/phase2_file_store_sessions/{topic_id}/{side}"))?;
        (
            source.get("session_id")?.as_str()?.to_owned(),
            source.get("turn_id")?.as_str()?.to_owned(),
        )
    } else {
        return None;
    };
    Some(orchestrator_store::ForkReference {
        fork_from_session_id: session_id,
        fork_from_turn_id: turn_id,
    })
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
    let current_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .context("migrated role requires current_date")?;
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
        round.unwrap_or(0)
    );
    let store = orchestrator_store::FileStore::open(
        store_root,
        orchestrator_store::FileStoreOptions::default(),
    )?;
    FileStoreSessionRuntime::create_or_load(
        store,
        SessionRuntimeSpec {
            run: orchestrator_store::RunLocation::new(current_date, run_id)?,
            session_id,
            role: role.to_owned(),
            phase,
            profile: profile.as_str().to_owned(),
            fork,
            created_at: Utc::now().to_rfc3339(),
        },
    )
}

/// Construct the sole Phase 0 writer: a task-scoped Experience Index.  The
/// source run is found by its manifest rather than a caller-provided path;
/// absence is a hard error, never a fallback to another summary storage path.
fn file_store_historical_reflection_index_runtime(
    store_root: &Path,
    state: &Value,
    profile_version: u32,
    builder_version: u32,
) -> Result<orchestrator_llm::tools::index_tools::IndexToolRuntimeBinding> {
    use orchestrator_llm::tools::index_tools::{IndexKind, IndexOwnedScope, IndexReadVisibility};
    use orchestrator_store::{
        content_hash, find_run_location, read_indexes, FileStore, FileStoreOptions,
        IndexKind as StoreIndexKind, IndexQuery,
    };

    let task = state
        .get("reflection_task")
        .and_then(Value::as_object)
        .context("HistoricalReflection FileStore runtime requires reflection_task")?;
    let task_id = task
        .get("task_id")
        .and_then(Value::as_i64)
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
    let source_indexes = read_indexes(
        &store,
        Some(&source_location),
        &IndexQuery {
            kind: Some(StoreIndexKind::PhaseSummary),
            ticker: Some(ticker.clone()),
            limit: 100,
            ..Default::default()
        },
    )?
    .indexes;
    let source_phase = source_indexes
        .iter()
        .map(|index| index.source_phase)
        .min()
        .context(
            "HistoricalReflection source run has no ticker-scoped completed Phase Summary Index",
        )?;
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
        source_phase,
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
        FileStoreIndexRuntimePlan::for_experience(vec![source_location], Utc::now().to_rfc3339()),
    )
}

/// Construct the sole live Phase Summary writer for one Rust-planned unit.
/// The unit is copied into the role state by the executor and is never a tool
/// argument. The model can only supply prose, confidence and Detail sections.
fn file_store_phase_summary_index_runtime(
    store_root: &Path,
    state: &Value,
    _profile_version: u32,
    _builder_version: u32,
) -> Result<orchestrator_llm::tools::index_tools::IndexToolRuntimeBinding> {
    use orchestrator_llm::tools::index_tools::{IndexKind, IndexOwnedScope, IndexReadVisibility};
    use orchestrator_store::{FileStore, FileStoreOptions, RunLocation};

    let unit = state
        .get("_summary_unit")
        .and_then(Value::as_object)
        .context("PhaseSummary role requires Rust-planned _summary_unit")?;
    let required = |key: &str| -> Result<String> {
        unit.get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
            .with_context(|| format!("_summary_unit.{key} is required"))
    };
    let run_id = state
        .get("run_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("run_id is required for PhaseSummary runtime")?
        .to_owned();
    let current_date = state
        .get("current_date")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("current_date is required for PhaseSummary runtime")?
        .to_owned();
    let source_phase = unit
        .get("source_phase")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .context("_summary_unit.source_phase is required")?;
    let mut source_refs = canonical_source_refs(
        state
            .get("_summary_source_payload")
            .context("PhaseSummary role requires Rust-planned _summary_source_payload")?,
    );
    if source_refs.is_empty() {
        source_refs = state
            .get("_completed_units")
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|units| units.values())
            .filter(|artifact| {
                artifact.get("phase").and_then(Value::as_i64) == Some(i64::from(source_phase))
            })
            .flat_map(canonical_source_refs)
            .collect();
    }
    if source_refs.is_empty() {
        bail!("PhaseSummary source payload contains no canonical Artifact or Index ID")
    }
    let unit_key = required("unit_key")?;
    let source_payload_hash = required("source_payload_hash")?;
    let authoritative_fields = Map::from_iter([
        ("unit_key".to_owned(), Value::String(unit_key.clone())),
        (
            "source_payload".to_owned(),
            state
                .get("_summary_source_payload")
                .cloned()
                .context("PhaseSummary role requires Rust-planned _summary_source_payload")?,
        ),
    ]);
    let owned = IndexOwnedScope {
        run_id,
        source_run_id: None,
        source_phase,
        role: required("role")?,
        kind: IndexKind::PhaseSummary,
        ticker: unit
            .get("ticker")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        topic_id: unit
            .get("topic_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        unit_key,
        source_payload_hash,
        index_id: required("index_id")?,
        authoritative_fields,
    };
    let store = FileStore::open(store_root, FileStoreOptions::default())?;
    file_store_index_tool_runtime(
        store,
        owned,
        IndexReadVisibility {
            source_refs,
            applies_to_phases: (source_phase < 7)
                .then_some(source_phase + 1)
                .into_iter()
                .collect(),
            ..IndexReadVisibility::default().with_default_page_size(20)
        },
        FileStoreIndexRuntimePlan::for_phase_summary(
            RunLocation::new(current_date, state["run_id"].as_str().unwrap_or_default())?,
            Utc::now().to_rfc3339(),
        ),
    )?
    .with_writer_role("compressor.phase_summary")
}

fn canonical_source_refs(value: &Value) -> BTreeSet<String> {
    fn collect(value: &Value, refs: &mut BTreeSet<String>) {
        match value {
            Value::Array(values) => values.iter().for_each(|value| collect(value, refs)),
            Value::Object(values) => {
                for (key, value) in values {
                    if matches!(key.as_str(), "artifact_id" | "index_id") {
                        if let Some(id) = value.as_str().filter(|id| !id.trim().is_empty()) {
                            refs.insert(id.to_owned());
                        }
                    }
                    collect(value, refs);
                }
            }
            _ => {}
        }
    }
    let mut refs = BTreeSet::new();
    collect(value, &mut refs);
    refs
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
        ("mediator.topic", _) => policy(&[1], &[], 0, config.phase2_max_details),
        ("researcher.bull.initial" | "researcher.bear.initial", _) => {
            policy(&[1], &[], 0, config.phase2_max_details)
        }
        ("researcher.bull.interaction" | "researcher.bear.interaction", _) => {
            policy(&[1], &[], 0, config.phase2_max_details)
        }
        ("mediator.topic_controller", _) => policy(&[1], &[], 0, config.phase2_max_details),
        ("manager.research", _) => policy(&[1, 2], &[], 1, config.phase3_max_details),
        ("trader", _) => policy(&[3], &[3], 1, config.phase4_max_details),
        ("risk.aggressive" | "risk.neutral" | "risk.conservative", _) => {
            policy(&[3, 4], &[3, 4], 2, config.phase5_max_details)
        }
        ("portfolio.manager", _) => policy(&[3, 4, 5], &[3, 4, 5], 3, config.phase6_max_details),
        _ => RetrievalPolicy::default(),
    }
}

fn phase2_debug_output_path(
    phase: i64,
    role: &str,
    kind: &str,
    topic_id: Option<&str>,
) -> Option<PathBuf> {
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
    } else if role.contains(".bull.") {
        "debate-bull.json"
    } else if role.contains(".bear.") {
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

pub(crate) fn record_role_job_metrics(state: &mut Value, result: &RoleJobResult) {
    let status = if result.artifact.is_some() {
        "ok"
    } else {
        "degraded"
    };
    if !state.get("role_job_metrics").is_some_and(Value::is_array) {
        state["role_job_metrics"] = json!([]);
    }
    let wait_ms = result.wait_ms();
    if let Some(items) = state["role_job_metrics"].as_array_mut() {
        items.push(json!({
            "role": result.role,
            "phase": result.phase,
            "kind": result.kind,
            "round": result.round,
            "topic_id": result.topic_id,
            "prompt_version": result.prompt_version,
            "model": result.model,
            "timed_out": result.timed_out,
            "elapsed_ms": result.elapsed_ms,
            "llm_ms": result.llm_ms,
            "tool_ms": result.tool_ms,
            "wait_ms": wait_ms,
            "status": status,
            "input_tokens": result.usage.input_tokens,
            "output_tokens": result.usage.output_tokens,
            "cached_tokens": result.usage.cached_tokens,
            "reasoning_tokens": result.usage.reasoning_tokens,
            "total_tokens": result.usage.total_tokens,
            "non_cached_input_tokens": result.usage.non_cached_input_tokens(),
            "visible_output_tokens": result.usage.visible_output_tokens(),
            "turn_count": result.turn_count,
            "tool_call_count": result.tool_call_count
            ,"retrieval_audit": result.artifact.as_ref()
                .and_then(|artifact| artifact.get("retrieval_audit"))
                .cloned().unwrap_or(Value::Null)
        }));
    }
    refresh_role_job_metrics(state);
    if state.get("debug").and_then(Value::as_bool) == Some(true) {
        let root = default_project_root();
        // One role-level timing row: llm + tool + wait breakdown.
        orchestrator_llm::debug_log_time(
            &root,
            json!({
                "kind": "role_job",
                "name": result.role,
                "role": result.role,
                "phase": result.phase,
                "kind_job": result.kind,
                "round": result.round,
                "topic_id": result.topic_id,
                "model": result.model,
                "status": status,
                "timed_out": result.timed_out,
                "elapsed_ms": result.elapsed_ms,
                "llm_ms": result.llm_ms,
                "tool_ms": result.tool_ms,
                "wait_ms": wait_ms,
                "turn_count": result.turn_count,
                "tool_call_count": result.tool_call_count,
            }),
        );
        orchestrator_llm::debug_log_token(
            &root,
            json!({
                "kind": "role_job",
                "role": result.role,
                "phase": result.phase,
                "kind_job": result.kind,
                "round": result.round,
                "topic_id": result.topic_id,
                "model": result.model,
                "status": status,
                "timed_out": result.timed_out,
                "elapsed_ms": result.elapsed_ms,
                "llm_ms": result.llm_ms,
                "tool_ms": result.tool_ms,
                "wait_ms": wait_ms,
                "input_tokens": result.usage.input_tokens,
                "output_tokens": result.usage.output_tokens,
                "cached_tokens": result.usage.cached_tokens,
                "reasoning_tokens": result.usage.reasoning_tokens,
                "total_tokens": result.usage.total_tokens,
                "non_cached_input_tokens": result.usage.non_cached_input_tokens(),
                "visible_output_tokens": result.usage.visible_output_tokens(),
                "turn_count": result.turn_count,
                "tool_call_count": result.tool_call_count,
            }),
        );
    }
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

fn refresh_role_job_metrics(state: &mut Value) {
    let jobs = state
        .get("role_job_metrics")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let total_elapsed_ms = jobs
        .iter()
        .filter_map(|job| job.get("elapsed_ms").and_then(Value::as_u64))
        .sum::<u64>();
    let timed_out_count = jobs
        .iter()
        .filter(|job| job.get("timed_out").and_then(Value::as_bool) == Some(true))
        .count();
    let sum = |field: &str| {
        jobs.iter()
            .filter_map(|job| job.get(field).and_then(Value::as_u64))
            .sum::<u64>()
    };
    let llm_request_count = sum("turn_count");
    let tool_call_count = sum("tool_call_count");

    if !state.get("workflow_metrics").is_some_and(Value::is_object) {
        state["workflow_metrics"] = json!({});
    }
    state["workflow_metrics"]["role_job_count"] = json!(jobs.len());
    state["workflow_metrics"]["llm_call_count"] = json!(llm_request_count);
    state["workflow_metrics"]["llm_request_count"] = json!(llm_request_count);
    state["workflow_metrics"]["tool_call_count"] = json!(tool_call_count);
    state["workflow_metrics"]["input_tokens"] = json!(sum("input_tokens"));
    state["workflow_metrics"]["output_tokens"] = json!(sum("output_tokens"));
    state["workflow_metrics"]["total_tokens"] = json!(sum("total_tokens"));
    state["workflow_metrics"]["total_role_elapsed_ms"] = json!(total_elapsed_ms);
    state["workflow_metrics"]["timed_out_role_count"] = json!(timed_out_count);
}

fn is_transient_role_error(message: &str) -> bool {
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
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    role.hash(&mut hasher);
    attempt.hash(&mut hasher);
    hasher.finish() % 251
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
                    let backoff_ms =
                        1_000u64 * attempt as u64 + role_retry_jitter_ms(&role, attempt);
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
                    let backoff_ms =
                        1_000u64 * attempt as u64 + role_retry_jitter_ms(&role, attempt);
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
        if let Some(binding) = job.domain_tool_runtime.clone() {
            return mock_domain_tool_managed_output(job, binding);
        }
        if let Some(binding) = job.index_tool_runtime.clone() {
            return mock_index_tool_managed_output(job, binding);
        }
        bail!(
            "mock ToolManaged role {} has no typed Index or Domain runtime binding",
            job.role
        );
    }
    let llm = job
        .llm
        .with_context(|| format!("missing prepared LLM config for role {:?}", job.role))?;
    let debug_prompt_path = job
        .prompt_path
        .as_deref()
        .and_then(debug_prompt_path_from_runtime_path);
    let debug_round = job.round.and_then(|round| usize::try_from(round).ok());
    let settings = AgentSettings {
        role: job.role,
        phase: Some(job.phase),
        topic_id: job.topic_id,
        debug_prompt_path,
        debug_output_path: job.debug_output_path,
        debug_round,
        tickers: job.tickers,
        tool_managed_profile: job.tool_managed_profile,
        index_tool_runtime: job.index_tool_runtime.clone(),
        domain_tool_runtime: job.domain_tool_runtime.clone(),
        session_runtime: job.session_runtime.clone(),
        max_write_calls: job.max_write_calls,
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
    let mut output = run_agent_loop_with_metrics(&settings, &job.prompt).await?;
    output.artifact["context_manifest"] = job.context_manifest;
    Ok(output)
}

fn mock_index_tool_managed_output(
    job: RoleJob,
    binding: orchestrator_llm::tools::index_tools::IndexToolRuntimeBinding,
) -> Result<AgentLoopOutput> {
    use orchestrator_llm::{
        agent_loop::ToolRuntimeTurnContext,
        tools::index_tools::{APPEND_INDEX_DETAIL_NAME, CREATE_INDEX_NAME, FINALIZE_INDEX_NAME},
    };

    let task = job
        .tools
        .file_store_reflection_source
        .as_ref()
        .and_then(|source| source.get("task"))
        .cloned()
        .unwrap_or(Value::Null);
    let runtime = binding.build(ToolRuntimeTurnContext {
        run_id: job.tools.run_id.clone().unwrap_or_default(),
        phase: Some(job.phase),
        role: job.role.clone(),
        session_id: format!("{}:mock", job.role),
        turn_id: "mock-reflection-finalize".to_owned(),
    })?;
    runtime.execute(
        CREATE_INDEX_NAME,
        json!({
            "summary": "Verify historical evidence freshness before repeating this decision.",
            "confidence": 0.5,
            "pattern_key": "mock-historical-reflection",
            "applies_to_phases": [1, 2, 3]
        }),
    )?;
    runtime.execute(
        APPEND_INDEX_DETAIL_NAME,
        json!({
            "section": "historical_case",
            "detail": format!("Mock reflection for historical task {}.", task.get("task_id").and_then(Value::as_i64).unwrap_or_default()),
            "source_refs": []
        }),
    )?;
    let terminal = runtime.execute(FINALIZE_INDEX_NAME, json!({}))?;
    let artifact = terminal
        .get("artifact")
        .cloned()
        .context("mock Index finalizer did not return an artifact")?;
    let terminal_result = ToolResultItem {
        call_id: "mock-finalize-index".to_owned(),
        name: FINALIZE_INDEX_NAME.to_owned(),
        status: "completed".to_owned(),
        output: terminal,
        error: None,
    };
    let (session_id, turn_id) = persist_mock_terminal(&job, &terminal_result)?;
    Ok(AgentLoopOutput {
        artifact: artifact.clone(),
        terminal_tool_result: Some(terminal_result),
        metrics: ModelStreamResult::default(),
        turn_id,
        session_id,
    })
}

fn mock_domain_tool_managed_output(
    job: RoleJob,
    binding: orchestrator_llm::tools::domain_tools::DomainToolRuntimeBinding,
) -> Result<AgentLoopOutput> {
    use orchestrator_llm::tools::domain_tools::{
        ADD_AGREED_FACT, APPEND_ANALYST_EVIDENCE, APPEND_BINDING_RISK_CONTROL, CREATE_DEBATE_CLAIM,
        CREATE_PHASE2_TOPIC, FINALIZE_ANALYST_REPORT, FINALIZE_DEBATE_RESPONSE,
        FINALIZE_DEBATE_SEED, FINALIZE_PORTFOLIO_DECISION, FINALIZE_RESEARCHER_WARMUP,
        FINALIZE_RESEARCH_DECISION, FINALIZE_RISK_REVIEW, FINALIZE_TOPIC_CONTROL,
        FINALIZE_TOPIC_GENERATION, FINALIZE_TRADE_INTENT, RESPOND_TO_DEBATE_CLAIM,
        SET_ANALYST_ASSESSMENT, SET_ANALYST_INVALIDATION, SET_DECISION_HINGE,
        SET_PHASE2_COMMON_GROUND, SET_PORTFOLIO_ASSET_DECISION, SET_RESEARCH_DECISION,
        SET_RESEARCH_SCENARIOS, SET_RISK_ASSESSMENT, SET_RISK_CONSTRAINTS, SET_TOPIC_SOFT_CONTROL,
        SET_TRADE_INTENT,
    };

    let profile = binding.scope().profile;
    let artifact = match profile {
        ToolManagedProfile::AnalystReport => {
            for ticker in &job.tickers {
                binding.execute(
                    SET_ANALYST_ASSESSMENT,
                    json!({
                        "ticker": ticker,
                        "direction": "neutral",
                        "confidence": 0.5,
                        "report": format!("Mock FileStore report for {ticker} from {}.", job.role),
                        "priced_in": "unclear",
                        "echo_chamber_risk": "low",
                        "crowded_consensus_risk": "low",
                    }),
                )?;
                binding.execute(
                    APPEND_ANALYST_EVIDENCE,
                    json!({
                        "ticker": ticker,
                        "evidence_ref": format!("mock:{}:{ticker}", job.role),
                        "evidence": {
                            "claim": format!("Mock evidence for {ticker}."),
                            "evidence_type": "fact",
                            "source": "mock runtime",
                            "timestamp": job.context_manifest.get("current_date").and_then(Value::as_str).unwrap_or("2026-01-01"),
                            "source_tier": "official",
                            "first_source": "mock runtime",
                            "is_derivative_repost": false,
                            "evidence_age": "0-2d",
                            "source_confidence": 0.9,
                        }
                    }),
                )?;
                binding.execute(
                    SET_ANALYST_INVALIDATION,
                    json!({
                        "ticker": ticker,
                        "validation_triggers": [format!("Mock invalidation for {ticker}.")],
                    }),
                )?;
            }
            binding.execute(FINALIZE_ANALYST_REPORT, json!({}))?
        }
        ToolManagedProfile::ResearchDecision => {
            for ticker in &job.tickers {
                binding.execute(
                    SET_RESEARCH_DECISION,
                    json!({
                        "ticker": ticker,
                        "rating": "Hold",
                        "long_probability": 0.5,
                        "short_probability": 0.5,
                        "confidence_basis": "evidence_balanced",
                        "hold_reason": "evidence_balanced",
                        "plan": format!("Mock FileStore research plan for {ticker}."),
                        "probability_rationale": "Mock evidence is balanced.",
                    }),
                )?;
                binding.execute(
                    SET_RESEARCH_SCENARIOS,
                    json!({
                        "ticker": ticker,
                        "bull": {"probability": 0.25, "drivers": ["mock upside"], "triggers": ["mock confirmation"], "confirmation": "mock"},
                        "base": {"probability": 0.50, "drivers": ["mock balance"], "triggers": ["mock confirmation"], "confirmation": "mock"},
                        "bear": {"probability": 0.25, "drivers": ["mock downside"], "triggers": ["mock confirmation"], "confirmation": "mock"},
                    }),
                )?;
            }
            binding.execute(FINALIZE_RESEARCH_DECISION, json!({}))?
        }
        ToolManagedProfile::TradeIntent => {
            binding.execute(
                SET_TRADE_INTENT,
                json!({
                    "action":"Hold", "execution_decision":"hold",
                    "entry_price":null, "stop_loss":null, "position_size_pct_max":0.0,
                    "rationale":"Mock FileStore trader preserves the Rust-owned Hold candidate."
                }),
            )?;
            binding.execute(FINALIZE_TRADE_INTENT, json!({}))?
        }
        ToolManagedProfile::RiskReview => {
            binding.execute(
                SET_RISK_ASSESSMENT,
                json!({
                    "argument":"Mock risk assessment.",
                    "unique_risk_contribution":"Mock stance-specific constraint.",
                    "disagreement_with_prior":"none", "no_new_information":false
                }),
            )?;
            binding.execute(
                SET_RISK_CONSTRAINTS,
                json!({
                    "recommended_adjustment":"hold", "stop_type":"soft",
                    "max_drawdown_pct":0.10, "position_cap_pct":0.0,
                    "rebalance_trigger":"Mock rebalance trigger.",
                    "risk_off_trigger":"Mock risk-off trigger.",
                    "review_window":"daily", "cash_hedge_recommendation":"hold cash",
                    "constraint_confidence":0.5
                }),
            )?;
            binding.execute(FINALIZE_RISK_REVIEW, json!({}))?
        }
        ToolManagedProfile::PortfolioDecision => {
            binding.execute(
                SET_PORTFOLIO_ASSET_DECISION,
                json!({
                    "direction_constraint":"unchanged", "execution_status":"wait",
                    "max_target_weight":0.0, "max_weight_delta":0.0,
                    "execution_summary":"Mock portfolio decision waits.",
                    "investment_thesis":"Mock Phase 3 is neutral.", "target_price":null,
                    "horizon":"Mock horizon", "rationale":"Mock portfolio wait."
                }),
            )?;
            binding.execute(APPEND_BINDING_RISK_CONTROL, json!({
                "control":{"control":"Mock risk cap.","source_refs":[format!("mock:portfolio.manager:{}", job.tickers.first().context("mock portfolio ticker missing")?)]}
            }))?;
            binding.execute(FINALIZE_PORTFOLIO_DECISION, json!({}))?
        }
        ToolManagedProfile::ResearcherWarmup => {
            binding.execute(FINALIZE_RESEARCHER_WARMUP, json!({}))?
        }
        ToolManagedProfile::TopicGeneration => {
            binding.execute(
                SET_PHASE2_COMMON_GROUND,
                json!({"common_ground":"Mock Phase 2 common ground."}),
            )?;
            binding.execute(
                CREATE_PHASE2_TOPIC,
                json!({
                    "topic":"Mock decision hinge", "decision_hinge":"Mock confirmation",
                    "evidence_refs": ["mock:mediator.topic:QQQ"]
                }),
            )?;
            binding.execute(FINALIZE_TOPIC_GENERATION, json!({}))?
        }
        ToolManagedProfile::DebateSeed => {
            binding.execute(
                CREATE_DEBATE_CLAIM,
                json!({
                    "claim": format!("Mock {} seed claim.", job.role), "confidence":0.5,
                    "evidence_refs": [format!("mock:{}:QQQ", job.role)]
                }),
            )?;
            binding.execute(FINALIZE_DEBATE_SEED, json!({}))?
        }
        ToolManagedProfile::DebateResponse => {
            let claim_id = binding
                .scope()
                .visible_claims
                .iter()
                .next()
                .cloned()
                .context("mock debate response requires a visible claim")?;
            binding.execute(
                RESPOND_TO_DEBATE_CLAIM,
                json!({
                    "reply_to_claim_id":claim_id, "response":"Mock counterpoint.",
                    "evidence_refs": [format!("mock:{}:QQQ", job.role)]
                }),
            )?;
            binding.execute(FINALIZE_DEBATE_RESPONSE, json!({}))?
        }
        ToolManagedProfile::TopicControl => {
            binding.execute(ADD_AGREED_FACT, json!({"value":"Mock topic fact."}))?;
            binding.execute(SET_DECISION_HINGE, json!({"value":"Mock topic hinge."}))?;
            binding.execute(SET_TOPIC_SOFT_CONTROL, json!({"should_continue":false}))?;
            binding.execute(FINALIZE_TOPIC_CONTROL, json!({}))?
        }
        ToolManagedProfile::HistoricalReflection | ToolManagedProfile::PhaseSummary => {
            anyhow::bail!(
                "{} is an Index runtime profile, not a domain runtime profile",
                profile.as_str()
            )
        }
    };
    let artifact = artifact
        .get("artifact")
        .cloned()
        .context("mock domain finalizer did not return a canonical artifact")?;
    let terminal_result = ToolResultItem {
        call_id: "mock-finalize".to_owned(),
        name: match profile {
            ToolManagedProfile::AnalystReport => FINALIZE_ANALYST_REPORT,
            ToolManagedProfile::ResearchDecision => FINALIZE_RESEARCH_DECISION,
            ToolManagedProfile::TradeIntent => FINALIZE_TRADE_INTENT,
            ToolManagedProfile::RiskReview => FINALIZE_RISK_REVIEW,
            ToolManagedProfile::PortfolioDecision => FINALIZE_PORTFOLIO_DECISION,
            ToolManagedProfile::ResearcherWarmup => FINALIZE_RESEARCHER_WARMUP,
            ToolManagedProfile::TopicGeneration => FINALIZE_TOPIC_GENERATION,
            ToolManagedProfile::DebateSeed => FINALIZE_DEBATE_SEED,
            ToolManagedProfile::DebateResponse => FINALIZE_DEBATE_RESPONSE,
            ToolManagedProfile::TopicControl => FINALIZE_TOPIC_CONTROL,
            _ => unreachable!(),
        }
        .to_owned(),
        status: "completed".to_owned(),
        output: json!({"terminal": true, "artifact": artifact.clone()}),
        error: None,
    };
    let (session_id, turn_id) = persist_mock_terminal(&job, &terminal_result)?;
    Ok(AgentLoopOutput {
        artifact: artifact.clone(),
        terminal_tool_result: Some(terminal_result),
        metrics: ModelStreamResult::default(),
        turn_id,
        session_id,
    })
}

/// Mock finalizers use the same FileStore session record as live finalizers.
/// This prevents deterministic test artifacts from looking like unaudited
/// direct writes to Store Doctor or fork recovery.
fn persist_mock_terminal(job: &RoleJob, terminal: &ToolResultItem) -> Result<(String, String)> {
    let session = &job.session_runtime;
    let session_id = session.manifest().session_id.clone();
    let turn_id = format!(
        "mock-finalize:{}:{}:{}:{}",
        job.kind,
        job.tickers.join(","),
        job.topic_id.as_deref().unwrap_or("aggregate"),
        job.round.unwrap_or(0),
    );
    let mut turn = Turn::new(
        &turn_id,
        &session_id,
        job.tools.run_id.as_deref().unwrap_or_default(),
        &job.role,
        "",
    );
    turn.phase = Some(job.phase);
    turn.terminal_tool_result = Some(terminal.clone());
    session.append_terminal(&turn, terminal, Utc::now().to_rfc3339())?;
    Ok((session_id, turn_id))
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
    fn phase2_debug_paths_follow_the_checkpoint_and_topic_tree() {
        assert_eq!(
            phase2_debug_output_path(2, "mediator.topic", "warmup", None),
            Some(PathBuf::from(
                "outputs/debug/phase2/phase2-warmup-shared.json"
            ))
        );
        assert_eq!(
            phase2_debug_output_path(2, "mediator.topic", "topic_generation", None),
            Some(PathBuf::from("outputs/debug/phase2/topic-generator.json"))
        );
        for (role, file) in [
            ("researcher.bull.initial", "debate-bull.json"),
            ("researcher.bear.interaction", "debate-bear.json"),
            ("mediator.topic_controller", "topic-controller.json"),
        ] {
            assert_eq!(
                phase2_debug_output_path(2, role, "debate", Some("QQQ/risk")),
                Some(PathBuf::from("outputs/debug/phase2/topic-QQQ_risk").join(file))
            );
        }
        assert_eq!(
            phase2_debug_output_path(2, "researcher.bull.initial", "bull_seed", Some("topic_vix")),
            Some(PathBuf::from(
                "outputs/debug/phase2/topic_vix/debate-bull.json"
            ))
        );
    }

    #[test]
    fn phase2_json_debug_files_retain_structured_messages() {
        let temp = tempfile::tempdir().unwrap();
        let path =
            phase2_debug_output_path(2, "researcher.bull.initial", "bull_seed", Some("topic-a"))
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
        assert!(output.get("records").is_none());
    }

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
    fn stream_transport_decode_error_is_transient_role_error() {
        assert!(is_transient_role_error(
            "Chat Completions stream chunk failed: stream failed: EventStream error: Transport error: error decoding response body"
        ));
    }

    #[test]
    fn records_role_job_metrics_and_aggregates() {
        let mut state = json!({});

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
