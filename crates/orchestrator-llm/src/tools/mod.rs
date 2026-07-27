pub mod alpaca;
pub mod domain_tools;
pub mod index_tools;
pub mod read_experience;
pub mod read_jin10_candidates;
pub mod read_phase_summaries;
pub mod read_phase_summary_details;
pub mod read_reflection_source;
pub mod read_run_context;
pub mod read_technical_detail;
pub mod read_technical_snapshot;
pub mod think;
pub mod verify_event;
pub mod web_run;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;
use tracing::{debug, warn};

use crate::agent_loop::ToolRuntimeTurnContext;
use crate::tools::domain_tools::EvidenceReadRecord;
pub use crate::web_search::{WebSearchConfig, WebSearchProvider};
pub use web_run::Runtime as WebRunRuntime;

pub const WEB_RUN_TOOL_NAME: &str = web_run::NAME;
pub const READ_PHASE_SUMMARIES_TOOL_NAME: &str = read_phase_summaries::NAME;
pub const READ_PHASE_SUMMARY_DETAILS_TOOL_NAME: &str = read_phase_summary_details::NAME;
pub const READ_EXPERIENCE_TOOL_NAME: &str = read_experience::NAME;
pub const READ_REFLECTION_SOURCE_TOOL_NAME: &str = read_reflection_source::NAME;
pub const CREATE_INDEX_TOOL_NAME: &str = index_tools::CREATE_INDEX_NAME;
pub const APPEND_INDEX_DETAIL_TOOL_NAME: &str = index_tools::APPEND_INDEX_DETAIL_NAME;
pub const FINALIZE_INDEX_TOOL_NAME: &str = index_tools::FINALIZE_INDEX_NAME;
pub const READ_INDEXES_TOOL_NAME: &str = index_tools::READ_INDEXES_NAME;
pub const READ_INDEX_DETAILS_TOOL_NAME: &str = index_tools::READ_INDEX_DETAILS_NAME;
pub const SET_ANALYST_ASSESSMENT_TOOL_NAME: &str = domain_tools::SET_ANALYST_ASSESSMENT;
pub const APPEND_ANALYST_EVIDENCE_TOOL_NAME: &str = domain_tools::APPEND_ANALYST_EVIDENCE;
pub const APPEND_ANALYST_DATA_GAP_TOOL_NAME: &str = domain_tools::APPEND_ANALYST_DATA_GAP;
pub const SET_ANALYST_INVALIDATION_TOOL_NAME: &str = domain_tools::SET_ANALYST_INVALIDATION;
pub const FINALIZE_ANALYST_REPORT_TOOL_NAME: &str = domain_tools::FINALIZE_ANALYST_REPORT;
pub const SET_RESEARCH_DECISION_TOOL_NAME: &str = domain_tools::SET_RESEARCH_DECISION;
pub const SET_RESEARCH_SCENARIOS_TOOL_NAME: &str = domain_tools::SET_RESEARCH_SCENARIOS;
pub const APPEND_RESEARCH_HINGE_TOOL_NAME: &str = domain_tools::APPEND_RESEARCH_HINGE;
pub const FINALIZE_RESEARCH_DECISION_TOOL_NAME: &str = domain_tools::FINALIZE_RESEARCH_DECISION;
pub const SET_TRADE_INTENT_TOOL_NAME: &str = domain_tools::SET_TRADE_INTENT;
pub const APPEND_TRADE_BLOCKER_TOOL_NAME: &str = domain_tools::APPEND_TRADE_BLOCKER;
pub const FINALIZE_TRADE_INTENT_TOOL_NAME: &str = domain_tools::FINALIZE_TRADE_INTENT;
pub const SET_RISK_ASSESSMENT_TOOL_NAME: &str = domain_tools::SET_RISK_ASSESSMENT;
pub const SET_RISK_CONSTRAINTS_TOOL_NAME: &str = domain_tools::SET_RISK_CONSTRAINTS;
pub const FINALIZE_RISK_REVIEW_TOOL_NAME: &str = domain_tools::FINALIZE_RISK_REVIEW;
pub const SET_PORTFOLIO_ASSET_DECISION_TOOL_NAME: &str = domain_tools::SET_PORTFOLIO_ASSET_DECISION;
pub const APPEND_BINDING_RISK_CONTROL_TOOL_NAME: &str = domain_tools::APPEND_BINDING_RISK_CONTROL;
pub const FINALIZE_PORTFOLIO_DECISION_TOOL_NAME: &str = domain_tools::FINALIZE_PORTFOLIO_DECISION;
pub const SET_PHASE2_COMMON_GROUND_TOOL_NAME: &str = domain_tools::SET_PHASE2_COMMON_GROUND;
pub const CREATE_PHASE2_TOPIC_TOOL_NAME: &str = domain_tools::CREATE_PHASE2_TOPIC;
pub const FINALIZE_RESEARCHER_WARMUP_TOOL_NAME: &str = domain_tools::FINALIZE_RESEARCHER_WARMUP;
pub const FINALIZE_TOPIC_GENERATION_TOOL_NAME: &str = domain_tools::FINALIZE_TOPIC_GENERATION;
pub const CREATE_DEBATE_CLAIM_TOOL_NAME: &str = domain_tools::CREATE_DEBATE_CLAIM;
pub const FINALIZE_DEBATE_SEED_TOOL_NAME: &str = domain_tools::FINALIZE_DEBATE_SEED;
pub const RESPOND_TO_DEBATE_CLAIM_TOOL_NAME: &str = domain_tools::RESPOND_TO_DEBATE_CLAIM;
pub const FINALIZE_DEBATE_RESPONSE_TOOL_NAME: &str = domain_tools::FINALIZE_DEBATE_RESPONSE;
pub const SET_CLAIM_STATUS_TOOL_NAME: &str = domain_tools::SET_CLAIM_STATUS;
pub const ADD_AGREED_FACT_TOOL_NAME: &str = domain_tools::ADD_AGREED_FACT;
pub const SET_DECISION_HINGE_TOOL_NAME: &str = domain_tools::SET_DECISION_HINGE;
pub const ROUTE_DEBATE_STEER_TOOL_NAME: &str = domain_tools::ROUTE_DEBATE_STEER;
pub const SET_TOPIC_SOFT_CONTROL_TOOL_NAME: &str = domain_tools::SET_TOPIC_SOFT_CONTROL;
pub const FINALIZE_TOPIC_CONTROL_TOOL_NAME: &str = domain_tools::FINALIZE_TOPIC_CONTROL;
// Internal compatibility only. This tool is intentionally absent from REGISTRY.
pub const READ_RUN_CONTEXT_TOOL_NAME: &str = read_run_context::NAME;
pub const ALPACA_GET_PORTFOLIO_TOOL_NAME: &str = alpaca::GET_PORTFOLIO_NAME;
pub const ALPACA_GET_HISTORY_TOOL_NAME: &str = alpaca::GET_HISTORY_NAME;
pub const ALPACA_GET_PRICE_TOOL_NAME: &str = alpaca::GET_PRICE_NAME;
pub const ALPACA_GET_NEWS_TOOL_NAME: &str = alpaca::GET_NEWS_NAME;
pub const ALPACA_SUBMIT_TRADE_TOOL_NAME: &str = alpaca::SUBMIT_TRADE_NAME;

#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExternalToolConfig {
    pub project_root: PathBuf,
    pub db_path: Option<PathBuf>,
    pub run_dir: Option<PathBuf>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub phase: Option<i64>,
    #[serde(default)]
    pub allowed_reflection_task_ids: Vec<i64>,
    #[serde(default = "default_phase_summary_page_limit")]
    pub phase_summary_page_limit: usize,
    #[serde(default = "default_phase_summary_page_limit")]
    pub phase_summary_detail_page_limit: usize,
    pub tickers: Vec<String>,
    #[serde(default)]
    pub alpaca_live: bool,
    #[serde(default)]
    pub alpaca_market_data: bool,
    #[serde(skip)]
    pub alpaca_api_key: Option<String>,
    #[serde(skip)]
    pub alpaca_api_secret: Option<String>,
    #[serde(skip)]
    pub phase_summary_index: Option<std::sync::Arc<orchestrator_sql::PhaseSummaryMemoryIndex>>,
    #[serde(skip)]
    pub phase_summary_gate: Option<std::sync::Arc<orchestrator_sql::PhaseSummaryGate>>,
    /// Present only for an explicitly migrated FileStore unit.  The typed
    /// context is Rust-owned and lets read tools access the immutable input
    /// copies for this run without accepting a model-provided path.
    #[serde(skip)]
    pub file_store_input: Option<FileStoreInputSnapshot>,
    /// Rust-owned historical task bootstrap for the migrated Phase 0
    /// reflector.  When present, `read_reflection_source` must not open the
    /// legacy database.
    #[serde(skip)]
    pub file_store_reflection_source: Option<Value>,
}

/// Identity of immutable Technical/Jin10 inputs captured for one FileStore
/// run.  This is deliberately not a general filesystem capability: readers
/// derive every source path from `InputSource` and only read a hash-sealed
/// run-local copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStoreInputSnapshot {
    pub store_root: PathBuf,
    pub run_id: String,
    pub current_date: String,
}

impl FileStoreInputSnapshot {
    pub fn read(&self, source: &orchestrator_store::InputSource) -> Result<Vec<u8>> {
        let store = orchestrator_store::FileStore::open(
            &self.store_root,
            orchestrator_store::FileStoreOptions::default(),
        )?;
        let location =
            orchestrator_store::RunLocation::new(self.current_date.clone(), self.run_id.clone())?;
        orchestrator_store::read_snapshotted_input(&store, &location, source).map_err(Into::into)
    }
}

impl Default for ExternalToolConfig {
    fn default() -> Self {
        Self {
            project_root: PathBuf::from("."),
            db_path: None,
            run_dir: None,
            run_id: None,
            phase: None,
            allowed_reflection_task_ids: Vec::new(),
            phase_summary_page_limit: default_phase_summary_page_limit(),
            phase_summary_detail_page_limit: default_phase_summary_page_limit(),
            tickers: Vec::new(),
            alpaca_live: false,
            alpaca_market_data: false,
            alpaca_api_key: None,
            alpaca_api_secret: None,
            phase_summary_index: None,
            phase_summary_gate: None,
            file_store_input: None,
            file_store_reflection_source: None,
        }
    }
}

fn default_phase_summary_page_limit() -> usize {
    orchestrator_sql::DEFAULT_PHASE_SUMMARY_LIMIT
}

// --- Registry ---

struct ToolEntry {
    name: &'static str,
    definition: fn() -> ToolDefinition,
}

const REGISTRY: &[ToolEntry] = &[
    ToolEntry {
        name: domain_tools::SET_ANALYST_ASSESSMENT,
        definition: || {
            domain_tools::definition(domain_tools::SET_ANALYST_ASSESSMENT)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::APPEND_ANALYST_EVIDENCE,
        definition: || {
            domain_tools::definition(domain_tools::APPEND_ANALYST_EVIDENCE)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::APPEND_ANALYST_DATA_GAP,
        definition: || {
            domain_tools::definition(domain_tools::APPEND_ANALYST_DATA_GAP)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_ANALYST_INVALIDATION,
        definition: || {
            domain_tools::definition(domain_tools::SET_ANALYST_INVALIDATION)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_ANALYST_REPORT,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_ANALYST_REPORT)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_RESEARCH_DECISION,
        definition: || {
            domain_tools::definition(domain_tools::SET_RESEARCH_DECISION)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_RESEARCH_SCENARIOS,
        definition: || {
            domain_tools::definition(domain_tools::SET_RESEARCH_SCENARIOS)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::APPEND_RESEARCH_HINGE,
        definition: || {
            domain_tools::definition(domain_tools::APPEND_RESEARCH_HINGE)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_RESEARCH_DECISION,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_RESEARCH_DECISION)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_TRADE_INTENT,
        definition: || {
            domain_tools::definition(domain_tools::SET_TRADE_INTENT)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::APPEND_TRADE_BLOCKER,
        definition: || {
            domain_tools::definition(domain_tools::APPEND_TRADE_BLOCKER)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_TRADE_INTENT,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_TRADE_INTENT)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_RISK_ASSESSMENT,
        definition: || {
            domain_tools::definition(domain_tools::SET_RISK_ASSESSMENT)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_RISK_CONSTRAINTS,
        definition: || {
            domain_tools::definition(domain_tools::SET_RISK_CONSTRAINTS)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_RISK_REVIEW,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_RISK_REVIEW)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_PORTFOLIO_ASSET_DECISION,
        definition: || {
            domain_tools::definition(domain_tools::SET_PORTFOLIO_ASSET_DECISION)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::APPEND_BINDING_RISK_CONTROL,
        definition: || {
            domain_tools::definition(domain_tools::APPEND_BINDING_RISK_CONTROL)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_PORTFOLIO_DECISION,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_PORTFOLIO_DECISION)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_PHASE2_COMMON_GROUND,
        definition: || {
            domain_tools::definition(domain_tools::SET_PHASE2_COMMON_GROUND)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::CREATE_PHASE2_TOPIC,
        definition: || {
            domain_tools::definition(domain_tools::CREATE_PHASE2_TOPIC)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_RESEARCHER_WARMUP,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_RESEARCHER_WARMUP)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_TOPIC_GENERATION,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_TOPIC_GENERATION)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::CREATE_DEBATE_CLAIM,
        definition: || {
            domain_tools::definition(domain_tools::CREATE_DEBATE_CLAIM)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_DEBATE_SEED,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_DEBATE_SEED)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::RESPOND_TO_DEBATE_CLAIM,
        definition: || {
            domain_tools::definition(domain_tools::RESPOND_TO_DEBATE_CLAIM)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_DEBATE_RESPONSE,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_DEBATE_RESPONSE)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_CLAIM_STATUS,
        definition: || {
            domain_tools::definition(domain_tools::SET_CLAIM_STATUS)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::ADD_AGREED_FACT,
        definition: || {
            domain_tools::definition(domain_tools::ADD_AGREED_FACT).expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_DECISION_HINGE,
        definition: || {
            domain_tools::definition(domain_tools::SET_DECISION_HINGE)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::ROUTE_DEBATE_STEER,
        definition: || {
            domain_tools::definition(domain_tools::ROUTE_DEBATE_STEER)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::SET_TOPIC_SOFT_CONTROL,
        definition: || {
            domain_tools::definition(domain_tools::SET_TOPIC_SOFT_CONTROL)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: domain_tools::FINALIZE_TOPIC_CONTROL,
        definition: || {
            domain_tools::definition(domain_tools::FINALIZE_TOPIC_CONTROL)
                .expect("registered domain tool")
        },
    },
    ToolEntry {
        name: think::NAME,
        definition: think::definition,
    },
    ToolEntry {
        name: read_phase_summaries::NAME,
        definition: read_phase_summaries::definition,
    },
    ToolEntry {
        name: read_phase_summary_details::NAME,
        definition: read_phase_summary_details::definition,
    },
    ToolEntry {
        name: read_experience::NAME,
        definition: read_experience::definition,
    },
    ToolEntry {
        name: read_reflection_source::NAME,
        definition: read_reflection_source::definition,
    },
    // FileStore Index tools are discoverable by a migrated ToolManaged
    // profile, but are deliberately absent from `tool_names()` until the
    // workflow injects an IndexToolRuntimeContext. Legacy runtime paths must
    // never fall back to this store.
    ToolEntry {
        name: index_tools::CREATE_INDEX_NAME,
        definition: index_tools::create_index_definition,
    },
    ToolEntry {
        name: index_tools::APPEND_INDEX_DETAIL_NAME,
        definition: index_tools::append_index_detail_definition,
    },
    ToolEntry {
        name: index_tools::FINALIZE_INDEX_NAME,
        definition: index_tools::finalize_index_definition,
    },
    ToolEntry {
        name: index_tools::READ_INDEXES_NAME,
        definition: index_tools::read_indexes_definition,
    },
    ToolEntry {
        name: index_tools::READ_INDEX_DETAILS_NAME,
        definition: index_tools::read_index_details_definition,
    },
    ToolEntry {
        name: web_run::NAME,
        definition: web_run::definition,
    },
    ToolEntry {
        name: read_technical_snapshot::NAME,
        definition: read_technical_snapshot::definition,
    },
    ToolEntry {
        name: read_technical_detail::NAME,
        definition: read_technical_detail::definition,
    },
    ToolEntry {
        name: read_jin10_candidates::NAME,
        definition: read_jin10_candidates::definition,
    },
    ToolEntry {
        name: verify_event::NAME,
        definition: verify_event::definition,
    },
    ToolEntry {
        name: alpaca::GET_NEWS_NAME,
        definition: alpaca::get_news_definition,
    },
    ToolEntry {
        name: alpaca::SUBMIT_TRADE_NAME,
        definition: alpaca::submit_trade_definition,
    },
];

pub fn tool_names() -> &'static [&'static str] {
    // Exclude think (always enabled via runtime, not listed in explicit names)
    // and web.run (conditionally added).
    &[
        read_phase_summaries::NAME,
        read_phase_summary_details::NAME,
        read_experience::NAME,
        read_reflection_source::NAME,
        read_technical_snapshot::NAME,
        read_technical_detail::NAME,
        read_jin10_candidates::NAME,
        verify_event::NAME,
        alpaca::GET_NEWS_NAME,
    ]
}

pub fn enabled_tool_names(
    web_run: Option<&WebSearchConfig>,
    alpaca_live: bool,
    alpaca_market_data: bool,
) -> Vec<&'static str> {
    let mut names = tool_names()
        .iter()
        .copied()
        .filter(|name| *name != alpaca::GET_NEWS_NAME)
        .collect::<Vec<_>>();
    if web_run.is_some() {
        names.push(web_run::NAME);
    }
    let _ = alpaca_live;
    if alpaca_market_data {
        names.push(alpaca::GET_NEWS_NAME);
    }
    names
}

pub fn tool_definition(name: &str) -> Option<ToolDefinition> {
    REGISTRY
        .iter()
        .find(|entry| entry.name == name)
        .map(|entry| (entry.definition)())
}

pub fn responses_tool_definitions(names: &[String]) -> Vec<async_openai::types::responses::Tool> {
    names
        .iter()
        .filter_map(|name| responses_tool_definition(name))
        .collect()
}

fn responses_tool_definition(name: &str) -> Option<async_openai::types::responses::Tool> {
    let core = tool_definition(name)?;
    Some(async_openai::types::responses::Tool::Function(
        async_openai::types::responses::FunctionToolArgs::default()
            .name(core.name)
            .description(core.description)
            .parameters(core.parameters)
            .strict(false)
            .build()
            .expect("FunctionTool build"),
    ))
}

pub fn chat_completions_tool_definitions(
    names: &[String],
) -> Vec<async_openai::types::chat::ChatCompletionTools> {
    names
        .iter()
        .filter_map(|name| chat_completions_tool_definition(name))
        .collect()
}

fn chat_completions_tool_definition(
    name: &str,
) -> Option<async_openai::types::chat::ChatCompletionTools> {
    let core = tool_definition(name)?;
    Some(async_openai::types::chat::ChatCompletionTools::Function(
        async_openai::types::chat::ChatCompletionTool {
            function: async_openai::types::chat::FunctionObject {
                name: core.name,
                description: Some(core.description),
                parameters: Some(core.parameters),
                strict: Some(false),
            },
        },
    ))
}

/// Build debug-friendly JSON array of tool definitions for the given names.
pub fn tool_definitions_json(names: &[String]) -> Vec<Value> {
    names
        .iter()
        .filter_map(|name| {
            let def = tool_definition(name)?;
            Some(json!({
                "type": "function",
                "function": {
                    "name": def.name,
                    "description": def.description,
                    "parameters": def.parameters,
                }
            }))
        })
        .collect()
}

/// OpenAI-compatible function names reject `.`; map internal names to API-safe form.
pub fn api_tool_name(name: &str) -> String {
    name.replace('.', "_")
}

/// Map a model-emitted function name back to the internal tool id.
pub fn resolve_tool_name(api_name: &str) -> String {
    match api_name {
        "web_run" => web_run::NAME.to_string(),
        other => other.to_string(),
    }
}

// --- Dispatch ---

pub async fn execute_named_tool(
    name: &str,
    args: Value,
    config: &ExternalToolConfig,
    turn_context: Option<&ToolRuntimeTurnContext>,
    web_run: Option<&WebRunRuntime>,
) -> Result<Value> {
    debug!(
        tool = name,
        has_turn_context = turn_context.is_some(),
        "named tool starting"
    );
    match name {
        read_phase_summaries::NAME => {
            let result = read_phase_summaries::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        read_phase_summary_details::NAME => {
            let result = read_phase_summary_details::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        read_experience::NAME => {
            let result = read_experience::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        read_reflection_source::NAME => {
            let result = read_reflection_source::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        read_run_context::NAME => {
            let result = read_run_context::execute(args, config, turn_context);
            log_tool_result(name, &result);
            result
        }
        web_run::NAME => {
            if let Some(web_run) = web_run {
                let result = web_run.execute(args).await;
                log_tool_result(name, &result);
                result
            } else {
                let result = Ok(web_run::safe_error("Web search is disabled."));
                log_tool_result(name, &result);
                result
            }
        }
        read_technical_snapshot::NAME => read_technical_snapshot::execute(args, config),
        read_technical_detail::NAME => read_technical_detail::execute(args, config),
        read_jin10_candidates::NAME => read_jin10_candidates::execute(args, config),
        verify_event::NAME => verify_event::execute(args, config, web_run).await,
        alpaca::GET_PORTFOLIO_NAME => alpaca::get_portfolio(config).await,
        alpaca::GET_HISTORY_NAME => alpaca::get_history(config).await,
        alpaca::GET_PRICE_NAME => alpaca::get_price(args, config).await,
        alpaca::GET_NEWS_NAME => alpaca::get_news(args, config).await,
        alpaca::SUBMIT_TRADE_NAME => {
            bail!("alpaca_submit_trade is runtime-only and unavailable to LLM tool dispatch")
        }
        index_tools::CREATE_INDEX_NAME
        | index_tools::APPEND_INDEX_DETAIL_NAME
        | index_tools::FINALIZE_INDEX_NAME
        | index_tools::READ_INDEXES_NAME
        | index_tools::READ_INDEX_DETAILS_NAME => {
            bail!("{name} requires a FileStore IndexToolRuntimeContext and is unavailable to the legacy runtime")
        }
        other => bail!("unknown tool name: {other}"),
    }
}

/// Convert only successful, structured read-tool output into evidence
/// visibility records.  This intentionally never examines assistant text or
/// tool arguments: a model can cite an ID only after a Rust read tool actually
/// returned that ID in the same session.
pub fn evidence_reads_from_tool_output(
    tool_name: &str,
    output: &Value,
    turn: &ToolRuntimeTurnContext,
) -> Result<Vec<EvidenceReadRecord>> {
    if !is_evidence_read_tool(tool_name) {
        return Ok(Vec::new());
    }
    let source_phase = turn
        .phase
        .and_then(|phase| u8::try_from(phase).ok())
        .context("evidence read requires a u8 current phase")?;
    let default_kind = match tool_name {
        read_technical_snapshot::NAME | read_technical_detail::NAME => "technical_signal",
        read_jin10_candidates::NAME => "jin10_event",
        read_phase_summaries::NAME | index_tools::READ_INDEXES_NAME => "index",
        read_phase_summary_details::NAME | index_tools::READ_INDEX_DETAILS_NAME => "detail",
        read_experience::NAME => "experience",
        read_reflection_source::NAME => "reflection_source",
        verify_event::NAME => "verified_event",
        _ => return Ok(Vec::new()),
    };
    let mut records = std::collections::BTreeMap::new();
    collect_evidence_records(
        output,
        default_kind,
        &turn.run_id,
        source_phase,
        None,
        None,
        tool_name,
        turn,
        &mut records,
    )?;
    Ok(records.into_values().collect())
}

fn is_evidence_read_tool(name: &str) -> bool {
    matches!(
        name,
        read_technical_snapshot::NAME
            | read_technical_detail::NAME
            | read_jin10_candidates::NAME
            | read_phase_summaries::NAME
            | read_phase_summary_details::NAME
            | read_experience::NAME
            | read_reflection_source::NAME
            | verify_event::NAME
            | index_tools::READ_INDEXES_NAME
            | index_tools::READ_INDEX_DETAILS_NAME
    )
}

#[allow(clippy::too_many_arguments)]
fn collect_evidence_records(
    value: &Value,
    default_kind: &str,
    inherited_run_id: &str,
    inherited_phase: u8,
    inherited_ticker: Option<&str>,
    inherited_topic_id: Option<&str>,
    tool_name: &str,
    turn: &ToolRuntimeTurnContext,
    records: &mut std::collections::BTreeMap<String, EvidenceReadRecord>,
) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_evidence_records(
                    value,
                    default_kind,
                    inherited_run_id,
                    inherited_phase,
                    inherited_ticker,
                    inherited_topic_id,
                    tool_name,
                    turn,
                    records,
                )?;
            }
        }
        Value::Object(object) => {
            let source_run_id = object
                .get("source_run_id")
                .or_else(|| object.get("run_id"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(inherited_run_id);
            let source_phase = object
                .get("source_phase")
                .and_then(Value::as_u64)
                .map(u8::try_from)
                .transpose()
                .context("evidence read source_phase must fit in u8")?
                .unwrap_or(inherited_phase);
            let ticker = object
                .get("ticker")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .or(inherited_ticker);
            let topic_id = object
                .get("topic_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .or(inherited_topic_id);
            for field in [
                ("signal_id", "technical_signal"),
                ("event_id", "jin10_event"),
                ("summary_id", "index"),
                ("index_id", "index"),
                ("detail_id", "detail"),
                ("experience_id", "experience"),
                ("reflection_id", "reflection_source"),
            ] {
                if let Some(subject_id) = object
                    .get(field.0)
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                {
                    insert_evidence_record(
                        records,
                        EvidenceReadRecord {
                            tool_name: tool_name.to_owned(),
                            subject_kind: field.1.to_owned(),
                            subject_id: subject_id.to_owned(),
                            source_run_id: source_run_id.to_owned(),
                            source_phase,
                            ticker: ticker.map(ToOwned::to_owned),
                            topic_id: topic_id.map(ToOwned::to_owned),
                            turn_id: turn.turn_id.clone(),
                            session_id: turn.session_id.clone(),
                        },
                    )?;
                }
            }
            // Phase-summary SQL uses generic `id`; FileStore Index/Detail
            // readers use typed IDs.  Keep this fallback bounded to their
            // known reader tools rather than recursively accepting arbitrary
            // JSON as evidence.
            if matches!(
                tool_name,
                read_phase_summaries::NAME | read_phase_summary_details::NAME
            ) {
                if let Some(subject_id) = object
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                {
                    insert_evidence_record(
                        records,
                        EvidenceReadRecord {
                            tool_name: tool_name.to_owned(),
                            subject_kind: default_kind.to_owned(),
                            subject_id: subject_id.to_owned(),
                            source_run_id: source_run_id.to_owned(),
                            source_phase,
                            ticker: ticker.map(ToOwned::to_owned),
                            topic_id: topic_id.map(ToOwned::to_owned),
                            turn_id: turn.turn_id.clone(),
                            session_id: turn.session_id.clone(),
                        },
                    )?;
                }
            }
            for nested in object.values() {
                collect_evidence_records(
                    nested,
                    default_kind,
                    source_run_id,
                    source_phase,
                    ticker,
                    topic_id,
                    tool_name,
                    turn,
                    records,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn insert_evidence_record(
    records: &mut std::collections::BTreeMap<String, EvidenceReadRecord>,
    record: EvidenceReadRecord,
) -> Result<()> {
    record.validate()?;
    match records.get(&record.subject_id) {
        Some(existing)
            if existing.subject_kind != record.subject_kind
                || existing.source_run_id != record.source_run_id
                || existing.source_phase != record.source_phase
                || existing.ticker != record.ticker
                || existing.topic_id != record.topic_id =>
        {
            bail!(
                "read tool returned evidence ID `{}` with conflicting provenance",
                record.subject_id
            )
        }
        Some(_) => {}
        None => {
            records.insert(record.subject_id.clone(), record);
        }
    }
    Ok(())
}

// --- Shared helpers ---

pub fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }
    let suffix = "\n[truncated]";
    let suffix_len = suffix.chars().count();
    if max_chars <= suffix_len {
        return value.chars().take(max_chars).collect();
    }
    let mut output = value
        .chars()
        .take(max_chars - suffix_len)
        .collect::<String>();
    output.push_str(suffix);
    output
}

pub(crate) fn optional_string_arg<'a>(args: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match args.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.trim().is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.trim())),
        Some(_) => bail!("{field} must be a string"),
    }
}

pub(crate) fn pagination_args(args: &Value, maximum: usize) -> Result<(usize, usize)> {
    let maximum = maximum.clamp(1, 100);
    let limit = match args.get("limit") {
        None | Some(Value::Null) => orchestrator_sql::DEFAULT_PHASE_SUMMARY_LIMIT.min(maximum),
        Some(value) => value
            .as_u64()
            .filter(|value| *value > 0)
            .map(|value| value as usize)
            .context("limit must be a positive integer")?,
    };
    if limit > maximum {
        bail!("limit exceeds configured maximum {maximum}");
    }
    let offset = match args.get("cursor") {
        None | Some(Value::Null) => 0,
        Some(Value::String(value)) => value
            .parse::<usize>()
            .context("cursor must be a pagination token returned by the prior call")?,
        Some(_) => bail!("cursor must be a string or null"),
    };
    Ok((limit, offset))
}

pub(crate) fn log_tool_result(name: &str, result: &Result<Value>) {
    match result {
        Ok(value) => {
            let status = value
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("ok");
            debug!(
                tool = name,
                status,
                output_chars = value.to_string().len(),
                "named tool completed"
            );
        }
        Err(error) => warn!(tool = name, error = %error, "named tool failed"),
    }
}

fn tool_connection(config: &ExternalToolConfig) -> Result<rusqlite::Connection> {
    orchestrator_sql::connect(runtime_db_path(config)?)
}

fn runtime_db_path(config: &ExternalToolConfig) -> Result<PathBuf> {
    config
        .db_path
        .clone()
        .context("runtime tool requires ExternalToolConfig.db_path")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web_search::{MockWebPage, MockWebSearchProvider, WebSearchMode};
    use std::sync::Arc;

    fn external_config() -> ExternalToolConfig {
        ExternalToolConfig {
            project_root: PathBuf::from("."),
            db_path: None,
            run_dir: None,
            run_id: None,
            phase: None,
            allowed_reflection_task_ids: Vec::new(),
            phase_summary_page_limit: 20,
            phase_summary_detail_page_limit: 20,
            tickers: Vec::new(),
            alpaca_live: false,
            alpaca_market_data: false,
            alpaca_api_key: None,
            alpaca_api_secret: None,
            phase_summary_index: None,
            phase_summary_gate: None,
            file_store_input: None,
            file_store_reflection_source: None,
        }
    }

    fn web_run_runtime<P>(config: WebSearchConfig, provider: P) -> WebRunRuntime
    where
        P: WebSearchProvider + 'static,
    {
        WebRunRuntime::new(config).with_provider(Arc::new(provider))
    }

    #[tokio::test]
    async fn legacy_read_run_context_is_not_model_registered() {
        assert!(tool_definition(read_run_context::NAME).is_none());
        let error = execute_named_tool(
            read_run_context::NAME,
            json!({"kind": "technical"}),
            &external_config(),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("only supports kinds"));
    }

    #[test]
    fn tool_definitions_map_web_run_api_name() {
        let names = [
            web_run::NAME.to_string(),
            read_phase_summaries::NAME.to_string(),
            read_phase_summary_details::NAME.to_string(),
        ];
        let defs: Vec<_> = names.iter().filter_map(|n| tool_definition(n)).collect();
        assert_eq!(defs.len(), 3);
        assert!(defs.iter().any(|tool| tool.name == "web_run"));
        assert!(defs.iter().any(|tool| tool.name == "read_phase_summaries"));
        assert!(defs
            .iter()
            .any(|tool| tool.name == "read_phase_summary_details"));
        assert_eq!(resolve_tool_name("web_run"), web_run::NAME);
    }

    #[test]
    fn every_registered_tool_declares_required_as_an_array() {
        for entry in REGISTRY {
            let definition = (entry.definition)();
            assert!(
                definition
                    .parameters
                    .get("required")
                    .is_some_and(Value::is_array),
                "tool {} must provide a JSON Schema required array",
                entry.name
            );
        }
    }

    #[tokio::test]
    async fn phase_summary_tools_fail_closed_without_turn_context() {
        for (name, args) in [
            (read_phase_summaries::NAME, json!({})),
            (
                read_phase_summary_details::NAME,
                json!({"summary_id": "summary-1"}),
            ),
        ] {
            let error = execute_named_tool(name, args, &external_config(), None, None)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("turn context"));
        }
    }

    #[tokio::test]
    async fn phase0_summary_tools_reject_non_allowlisted_tasks_before_database_access() {
        let context = ToolRuntimeTurnContext {
            run_id: "current-run".to_string(),
            session_id: "session".to_string(),
            turn_id: "turn".to_string(),
            role: "reflector.historical".to_string(),
            phase: Some(0),
        };
        for (name, args) in [
            (read_phase_summaries::NAME, json!({"task_id": 99})),
            (
                read_phase_summary_details::NAME,
                json!({"task_id": 99, "summary_id": "summary-1"}),
            ),
        ] {
            let error = execute_named_tool(name, args, &external_config(), Some(&context), None)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("not allowlisted"));
        }
    }

    #[test]
    fn pagination_respects_configured_page_limit() {
        assert_eq!(pagination_args(&json!({}), 5).unwrap(), (5, 0));
        assert!(pagination_args(&json!({"limit": 6}), 5)
            .unwrap_err()
            .to_string()
            .contains("configured maximum 5"));
    }

    #[tokio::test]
    async fn web_run_disabled_returns_safe_error() {
        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&WebRunRuntime::new(WebSearchConfig::default())),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert_eq!(output["content"], "Web search is disabled.");
    }

    #[tokio::test]
    async fn web_run_accepts_legacy_agent_search_shape() {
        let provider = MockWebSearchProvider::new(vec![MockWebPage {
            title: "QQQ macro update".to_string(),
            url: "https://www.reuters.com/markets/example".to_string(),
            content: "QQQ and VIX macro context.".to_string(),
        }]);
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({
                "search_query": "QQQ VIX macro update",
                "include_domains": ["reuters.com"],
                "num_results": 10,
                "source": "exa",
                "response_length": "medium"
            }),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "success");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("QQQ macro update"));
    }

    #[tokio::test]
    async fn web_run_rejects_too_many_search_queries() {
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({
                "search_query": [
                    {"q": "one"},
                    {"q": "two"},
                    {"q": "three"},
                    {"q": "four"},
                    {"q": "five"}
                ]
            }),
            &external_config(),
            None,
            Some(&WebRunRuntime::new(config)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("at most 4 queries"));
    }

    #[tokio::test]
    async fn web_run_rejects_overlong_queries() {
        let provider = MockWebSearchProvider::default();
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "x".repeat(513)}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("exceeds 512 chars"));
    }

    #[tokio::test]
    async fn web_run_formats_filters_and_truncates_search_results() {
        let provider = MockWebSearchProvider::new(vec![
            MockWebPage {
                title: "Allowed TQQQ".to_string(),
                url: "https://research.example.com/tqqq?token=secret#section".to_string(),
                content: "TQQQ volatility and liquidity signal with enough detail to truncate."
                    .to_string(),
            },
            MockWebPage {
                title: "Blocked TQQQ".to_string(),
                url: "https://blocked.example.com/tqqq".to_string(),
                content: "TQQQ blocked signal".to_string(),
            },
            MockWebPage {
                title: "Non HTTP TQQQ".to_string(),
                url: "ftp://research.example.com/tqqq".to_string(),
                content: "TQQQ non http signal".to_string(),
            },
        ]);
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            allowed_domains: vec!["example.com".to_string()],
            blocked_domains: vec!["blocked.example.com".to_string()],
            max_result_chars: 220,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ"}], "response_length": "short"}),
            &external_config(),
            None,
            Some(&web_run_runtime(config.clone(), provider.clone())),
        )
        .await
        .unwrap();
        let text = output["text"].as_str().unwrap();

        assert!(text.starts_with("Search results:\n[ref_id: search0]"));
        assert!(text.contains("Title: Allowed TQQQ"));
        assert!(text.contains("URL: https://research.example.com/tqqq"));
        assert!(!text.contains("token=secret"));
        assert!(!text.contains("Blocked TQQQ"));
        assert!(!text.contains("Non HTTP TQQQ"));
        assert_eq!(output["results"].as_array().unwrap().len(), 1);
        assert_eq!(
            output["results"][0]["url"],
            "https://research.example.com/tqqq"
        );

        let truncated_config = WebSearchConfig {
            max_result_chars: 80,
            ..config
        };
        let truncated = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(truncated_config, provider)),
        )
        .await
        .unwrap();
        let truncated_text = truncated["text"].as_str().unwrap();
        assert!(truncated_text.contains("[... middle truncated ...]"));
        assert!(truncated_text.chars().count() <= 80);
    }

    #[tokio::test]
    async fn web_run_rejects_query_domains_outside_policy() {
        let provider = MockWebSearchProvider::default();
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            allowed_domains: vec!["example.com".to_string()],
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ", "domains": ["not-example.com"]}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, provider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert!(output["content"]
            .as_str()
            .unwrap()
            .contains("domain is not allowed"));
    }

    #[derive(Debug)]
    struct FailingProvider;

    impl WebSearchProvider for FailingProvider {
        fn search<'a>(
            &'a self,
            _queries: Vec<crate::web_search::SearchQuery>,
            _options: crate::web_search::WebSearchOptions,
        ) -> crate::web_search::WebSearchFuture<'a, Vec<crate::web_search::SearchResult>> {
            Box::pin(async {
                bail!("provider rejected request with API key sk-secret-do-not-leak")
            })
        }
    }

    #[tokio::test]
    async fn web_run_sanitizes_provider_errors() {
        let config = WebSearchConfig {
            mode: WebSearchMode::Cached,
            ..WebSearchConfig::default()
        };

        let output = execute_named_tool(
            web_run::NAME,
            json!({"search_query": [{"q": "TQQQ"}]}),
            &external_config(),
            None,
            Some(&web_run_runtime(config, FailingProvider)),
        )
        .await
        .unwrap();

        assert_eq!(output["status"], "error");
        assert_eq!(output["content"], "web.run provider failed.");
        assert!(!output.to_string().contains("sk-secret"));
    }
}
