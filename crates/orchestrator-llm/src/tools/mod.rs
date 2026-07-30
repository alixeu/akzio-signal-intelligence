pub mod alpaca;
pub mod experience_tools;
pub mod historical_reflection;
pub mod index_tools;
pub mod read_jin10_candidates;
pub mod read_reflection_source;
pub mod read_technical_detail;
pub mod read_technical_snapshot;
pub mod record_phase2_context;
pub mod research_evidence_gap;
pub mod think;
pub mod verify_event;
pub mod web_run;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::PathBuf;
use tracing::{debug, warn};

use crate::agent_loop::ToolRuntimeTurnContext;
pub use crate::web_search::{WebSearchConfig, WebSearchProvider};
pub use web_run::Runtime as WebRunRuntime;

#[derive(Debug, Clone)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExternalToolConfig {
    pub project_root: PathBuf,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub phase: Option<i64>,
    #[serde(default = "default_phase_summary_page_limit")]
    pub phase_summary_page_limit: usize,
    #[serde(default = "default_phase_summary_page_limit")]
    pub phase_summary_detail_page_limit: usize,
    pub tickers: Vec<String>,
    #[serde(default)]
    pub alpaca_market_data: bool,
    #[serde(skip)]
    pub alpaca_api_key: Option<String>,
    #[serde(skip)]
    pub alpaca_api_secret: Option<String>,
    /// Present only for an explicitly migrated FileStore unit.  The typed
    /// context is Rust-owned and lets read tools access stable, hash-bound
    /// input paths without accepting a model-provided path.
    #[serde(skip)]
    pub file_store_input: Option<FileStoreInputSnapshot>,
    /// Rust-owned historical task bootstrap for the migrated Phase 0
    /// reflector.  When present, `read_reflection_source` must not open the
    /// retired database implementation.
    #[serde(skip)]
    pub file_store_reflection_source: Option<Value>,
    /// Canonical Phase 2 in-memory context. Models cannot choose or modify
    /// these fields; the record tool only exposes this Rust-bound value.
    #[serde(skip)]
    pub phase2_context: Option<Value>,
}

/// Identity of Technical/Jin10 input hashes captured for one FileStore
/// run.  This is deliberately not a general filesystem capability: readers
/// derive every source path from `InputSource` and verify it against the
/// run-local hash manifest.
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
            run_id: None,
            phase: None,
            phase_summary_page_limit: default_phase_summary_page_limit(),
            phase_summary_detail_page_limit: default_phase_summary_page_limit(),
            tickers: Vec::new(),
            alpaca_market_data: false,
            alpaca_api_key: None,
            alpaca_api_secret: None,
            file_store_input: None,
            file_store_reflection_source: None,
            phase2_context: None,
        }
    }
}

fn default_phase_summary_page_limit() -> usize {
    20
}

// --- Registry ---

struct ToolEntry {
    name: &'static str,
    definition: fn() -> ToolDefinition,
}

const REGISTRY: &[ToolEntry] = &[
    ToolEntry {
        name: think::NAME,
        definition: think::definition,
    },
    ToolEntry {
        name: read_reflection_source::NAME,
        definition: read_reflection_source::definition,
    },
    ToolEntry {
        name: experience_tools::SEARCH_EXPERIENCES_NAME,
        definition: || {
            experience_tools::definition(experience_tools::SEARCH_EXPERIENCES_NAME)
                .expect("registered Experience tool")
        },
    },
    ToolEntry {
        name: experience_tools::READ_EXPERIENCE_CASES_NAME,
        definition: || {
            experience_tools::definition(experience_tools::READ_EXPERIENCE_CASES_NAME)
                .expect("registered Experience tool")
        },
    },
    ToolEntry {
        name: experience_tools::RECORD_MEMORY_APPLICATION_NAME,
        definition: || {
            experience_tools::definition(experience_tools::RECORD_MEMORY_APPLICATION_NAME)
                .expect("registered Experience tool")
        },
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
        name: record_phase2_context::NAME,
        definition: record_phase2_context::definition,
    },
    ToolEntry {
        name: research_evidence_gap::NAME,
        definition: research_evidence_gap::definition,
    },
    ToolEntry {
        name: alpaca::GET_NEWS_NAME,
        definition: alpaca::get_news_definition,
    },
];

pub fn tool_names() -> &'static [&'static str] {
    // Exclude think (always enabled via runtime, not listed in explicit names)
    // and web.run (conditionally added).
    &[
        read_reflection_source::NAME,
        read_technical_snapshot::NAME,
        read_technical_detail::NAME,
        read_jin10_candidates::NAME,
        verify_event::NAME,
        record_phase2_context::NAME,
        alpaca::GET_NEWS_NAME,
    ]
}

pub fn enabled_tool_names(
    web_run: Option<&WebSearchConfig>,
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

/// All model-visible Tool IDs registered by this crate.  Role ownership is
/// checked separately against the core RoleProfileRegistry so an orphaned
/// definition cannot silently survive a profile migration.
pub fn registered_tool_names() -> impl Iterator<Item = &'static str> {
    REGISTRY.iter().map(|entry| entry.name)
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
        read_reflection_source::NAME => {
            let result = read_reflection_source::execute(args, config, turn_context);
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
        record_phase2_context::NAME => record_phase2_context::execute(args, config, turn_context),
        research_evidence_gap::NAME => {
            bail!("{name} requires an EvidenceResearchBinding and is unavailable without that typed binding")
        }
        alpaca::GET_NEWS_NAME => alpaca::get_news(args, config).await,
        index_tools::READ_INDEXES_NAME | index_tools::READ_INDEX_DETAILS_NAME => {
            bail!("{name} requires a FileStore IndexToolRuntimeContext and is unavailable without that typed binding")
        }
        other => bail!("unknown tool name: {other}"),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::RoleProfileRegistry;
    use std::collections::BTreeSet;

    #[test]
    fn every_registered_tool_has_a_profile_owner_or_runtime_only_path() {
        let registry = RoleProfileRegistry::builtin();
        let owned = registry
            .registrations()
            .flat_map(|registration| registration.tool_allowlist.iter())
            .map(|tool| tool.as_str())
            .collect::<BTreeSet<_>>();
        for registration in registry.registrations() {
            for tool in &registration.tool_allowlist {
                assert!(
                    tool_definition(tool.as_str()).is_some(),
                    "{} is allowed for {} but has no registered definition",
                    tool.as_str(),
                    registration.role_id
                );
            }
        }
        for tool in registered_tool_names() {
            assert!(
                owned.contains(tool) || matches!(tool, think::NAME | web_run::NAME),
                "registered tool {tool} has neither a Role/Profile owner nor an explicit runtime-only path"
            );
        }
    }
}
