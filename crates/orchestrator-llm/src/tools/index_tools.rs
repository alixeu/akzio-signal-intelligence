//! Read-only Index/Detail tools for business roles.
//!
//! Models may list completed Indexes and expand their Details. Rust owns every
//! write after compiling a phase-specific Summary response.

use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use orchestrator_core::ToolId;
pub use orchestrator_store::{DetailSection, IndexKind};
use serde::Serialize;
use serde_json::{json, Map, Value};

use super::{api_tool_name, ToolDefinition};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const READ_INDEXES_NAME: &str = ToolId::ReadIndexes.as_str();
pub const READ_INDEX_DETAILS_NAME: &str = ToolId::ReadIndexDetails.as_str();

/// Immutable identity supplied by the Rust unit planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexOwnedScope {
    pub run_id: String,
    pub source_run_id: Option<String>,
    pub source_phase: u8,
    pub role: String,
    pub kind: IndexKind,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub authoritative_fields: Map<String, Value>,
    pub index_id: String,
}

impl IndexOwnedScope {
    pub fn validate(&self) -> Result<()> {
        if self.run_id.trim().is_empty()
            || self.role.trim().is_empty()
            || self.unit_key.trim().is_empty()
            || self.source_payload_hash.trim().is_empty()
            || self.index_id.trim().is_empty()
        {
            bail!("Index scope requires Rust-owned run, role, unit, source hash, and index id");
        }
        if self.source_phase > 8 {
            bail!("Index scope source_phase must be in 0..=8");
        }
        Ok(())
    }
}

/// Complete visibility boundary for model-selected read filters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexReadVisibility {
    pub kinds: BTreeSet<IndexKind>,
    pub tickers: BTreeSet<String>,
    pub source_phases: BTreeSet<u8>,
    pub applies_to_phases: BTreeSet<u8>,
    pub roles: BTreeSet<String>,
    pub topic_ids: BTreeSet<String>,
    pub pattern_keys: BTreeSet<String>,
    pub source_refs: BTreeSet<String>,
    pub evidence_ids: BTreeSet<String>,
    pub max_page_size: usize,
}

impl IndexReadVisibility {
    pub fn with_default_page_size(mut self, page_size: usize) -> Self {
        self.max_page_size = page_size.clamp(1, 100);
        self
    }
}

#[derive(Debug)]
pub struct IndexToolRuntimeContext {
    turn: ToolRuntimeTurnContext,
    owned: IndexOwnedScope,
    visibility: IndexReadVisibility,
    visible_index_ids: Mutex<BTreeSet<String>>,
}

impl IndexToolRuntimeContext {
    pub fn new(
        turn: ToolRuntimeTurnContext,
        owned: IndexOwnedScope,
        visibility: IndexReadVisibility,
    ) -> Result<Self> {
        owned.validate()?;
        if turn.run_id != owned.run_id || turn.role != owned.role {
            bail!("Index read context does not match the Rust-owned unit scope");
        }
        if turn.phase.is_none() {
            bail!("Index read context requires the current execution phase");
        }
        Ok(Self {
            turn,
            owned,
            visibility,
            visible_index_ids: Mutex::new(BTreeSet::new()),
        })
    }

    pub fn owned_scope(&self) -> &IndexOwnedScope {
        &self.owned
    }

    pub fn turn_context(&self) -> &ToolRuntimeTurnContext {
        &self.turn
    }

    pub fn record_visible_indexes(
        &self,
        index_ids: impl IntoIterator<Item = String>,
    ) -> Result<()> {
        let mut known = self
            .visible_index_ids
            .lock()
            .map_err(|_| anyhow::anyhow!("Index visibility lock poisoned"))?;
        for index_id in index_ids {
            let index_id = index_id.trim();
            if index_id.is_empty() {
                bail!("read service returned an empty index id");
            }
            known.insert(index_id.to_owned());
        }
        Ok(())
    }

    fn resolve_visible_index(&self, supplied: Option<&str>) -> Result<String> {
        let known = self
            .visible_index_ids
            .lock()
            .map_err(|_| anyhow::anyhow!("Index visibility lock poisoned"))?;
        match (supplied, known.len()) {
            (Some(index_id), _) if known.contains(index_id) => Ok(index_id.to_owned()),
            (Some(_), _) => bail!("index_id is not visible in this turn"),
            (None, 1) => Ok(known.iter().next().expect("single entry").clone()),
            (None, 0) => bail!("read_index_details requires a preceding read_indexes result"),
            (None, _) => bail!("index_id is required when multiple Indexes are visible"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ReadIndexesCommand {
    pub kind: Option<IndexKind>,
    pub ticker: Option<String>,
    pub source_phase: Option<u8>,
    pub applies_to_phase: Option<u8>,
    pub role: Option<String>,
    pub topic_id: Option<String>,
    pub pattern_key: Option<String>,
    pub limit: usize,
    pub cursor: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadIndexDetailsCommand {
    pub index_id: String,
    pub section: Option<DetailSection>,
    pub limit: usize,
    pub cursor: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum IndexToolCommand {
    ReadIndexes(ReadIndexesCommand),
    ReadDetails(ReadIndexDetailsCommand),
}

pub trait IndexToolService: Send + Sync {
    fn read_indexes(&self, command: ReadIndexesCommand) -> Result<IndexReadPage>;
    fn read_index_details(&self, command: ReadIndexDetailsCommand) -> Result<Value>;
}

impl<T> IndexToolService for Arc<T>
where
    T: IndexToolService + ?Sized,
{
    fn read_indexes(&self, command: ReadIndexesCommand) -> Result<IndexReadPage> {
        (**self).read_indexes(command)
    }

    fn read_index_details(&self, command: ReadIndexDetailsCommand) -> Result<Value> {
        (**self).read_index_details(command)
    }
}

#[derive(Clone)]
pub struct IndexToolRuntimeBinding {
    owned: IndexOwnedScope,
    visibility: IndexReadVisibility,
    service: Arc<dyn IndexToolService>,
}

impl fmt::Debug for IndexToolRuntimeBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexToolRuntimeBinding")
            .field("owned", &self.owned)
            .field("visibility", &self.visibility)
            .field("service", &"IndexToolService")
            .finish()
    }
}

impl IndexToolRuntimeBinding {
    pub fn new(
        owned: IndexOwnedScope,
        visibility: IndexReadVisibility,
        service: Arc<dyn IndexToolService>,
    ) -> Result<Self> {
        owned.validate()?;
        Ok(Self {
            owned,
            visibility,
            service,
        })
    }

    pub fn owned_scope(&self) -> &IndexOwnedScope {
        &self.owned
    }

    pub fn build(
        &self,
        turn: ToolRuntimeTurnContext,
    ) -> Result<IndexToolRuntime<Arc<dyn IndexToolService>>> {
        Ok(IndexToolRuntime::new(
            IndexToolRuntimeContext::new(turn, self.owned.clone(), self.visibility.clone())?,
            Arc::clone(&self.service),
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexReadPage {
    pub output: Value,
    pub index_ids: Vec<String>,
}

#[derive(Debug)]
pub struct IndexToolRuntime<S> {
    context: IndexToolRuntimeContext,
    service: S,
}

impl<S> IndexToolRuntime<S>
where
    S: IndexToolService,
{
    pub fn new(context: IndexToolRuntimeContext, service: S) -> Self {
        Self { context, service }
    }

    pub fn context(&self) -> &IndexToolRuntimeContext {
        &self.context
    }

    pub fn execute(&self, name: &str, args: Value) -> Result<Value> {
        match prepare_command(name, args, &self.context)? {
            IndexToolCommand::ReadIndexes(command) => {
                let page = self.service.read_indexes(command)?;
                self.context.record_visible_indexes(page.index_ids)?;
                Ok(page.output)
            }
            IndexToolCommand::ReadDetails(command) => self.service.read_index_details(command),
        }
    }
}

pub fn definition(name: &str) -> Option<ToolDefinition> {
    match name {
        READ_INDEXES_NAME => Some(read_indexes_definition()),
        READ_INDEX_DETAILS_NAME => Some(read_index_details_definition()),
        _ => None,
    }
}

pub fn read_indexes_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(READ_INDEXES_NAME),
        description: "List completed, visibility-authorized Index records. Omit singleton filters because Rust fills them from the role scope.".to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {
                "kind": {"type": "string", "enum": ["phase_summary", "experience"]},
                "ticker": {"type": "string"},
                "source_phase": {"type": "integer", "minimum": 0, "maximum": 8},
                "applies_to_phase": {"type": "integer", "minimum": 0, "maximum": 8},
                "role": {"type": "string"},
                "topic_id": {"type": "string"},
                "pattern_key": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                "cursor": {"type": ["string", "null"]}
            },
            "required": [],
            "additionalProperties": false
        }),
    }
}

pub fn read_index_details_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(READ_INDEX_DETAILS_NAME),
        description: "Expand Details from an Index returned by read_indexes. Omit index_id when exactly one Index is visible.".to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {
                "index_id": {"type": "string"},
                "section": {"type": "string", "enum": ["evidence", "counter_evidence", "conflict", "decision_hinge", "data_gap", "invalidation", "next_step", "analysis", "historical_case", "execution", "risk", "other"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                "cursor": {"type": ["string", "null"]}
            },
            "required": [],
            "additionalProperties": false
        }),
    }
}

pub fn prepare_command(
    name: &str,
    args: Value,
    context: &IndexToolRuntimeContext,
) -> Result<IndexToolCommand> {
    match name {
        READ_INDEXES_NAME => parse_read_indexes(args, context).map(IndexToolCommand::ReadIndexes),
        READ_INDEX_DETAILS_NAME => {
            parse_read_details(args, context).map(IndexToolCommand::ReadDetails)
        }
        _ => bail!("unknown Index tool name: {name}"),
    }
}

fn parse_read_indexes(
    args: Value,
    context: &IndexToolRuntimeContext,
) -> Result<ReadIndexesCommand> {
    let object = checked_object(
        &args,
        READ_INDEXES_NAME,
        &[
            "kind",
            "ticker",
            "source_phase",
            "applies_to_phase",
            "role",
            "topic_id",
            "pattern_key",
            "limit",
            "cursor",
        ],
    )?;
    let kind = optional_string(object, "kind")?
        .as_deref()
        .map(IndexKind::parse)
        .transpose()?;
    let kind = scoped_value_filter("kind", kind, &context.visibility.kinds)?;
    let ticker = scoped_filter(object, "ticker", &context.visibility.tickers)?;
    let source_phase = scoped_value_filter(
        "source_phase",
        optional_phase(object, "source_phase")?,
        &context.visibility.source_phases,
    )?;
    let applies_to_phase = scoped_value_filter(
        "applies_to_phase",
        optional_phase(object, "applies_to_phase")?,
        &context.visibility.applies_to_phases,
    )?;
    let role = scoped_filter(object, "role", &context.visibility.roles)?;
    let topic_id = scoped_filter(object, "topic_id", &context.visibility.topic_ids)?;
    let pattern_key = scoped_filter(object, "pattern_key", &context.visibility.pattern_keys)?;
    let (limit, cursor) = pagination_from_object(object, context.visibility.max_page_size)?;
    Ok(ReadIndexesCommand {
        kind,
        ticker,
        source_phase,
        applies_to_phase,
        role,
        topic_id,
        pattern_key,
        limit,
        cursor,
    })
}

fn parse_read_details(
    args: Value,
    context: &IndexToolRuntimeContext,
) -> Result<ReadIndexDetailsCommand> {
    let object = checked_object(
        &args,
        READ_INDEX_DETAILS_NAME,
        &["index_id", "section", "limit", "cursor"],
    )?;
    let index_id =
        context.resolve_visible_index(optional_string(object, "index_id")?.as_deref())?;
    let section = optional_string(object, "section")?
        .map(|value| DetailSection::parse(&value))
        .transpose()?;
    let (limit, cursor) = pagination_from_object(object, context.visibility.max_page_size)?;
    Ok(ReadIndexDetailsCommand {
        index_id,
        section,
        limit,
        cursor,
    })
}

fn checked_object<'a>(
    args: &'a Value,
    tool: &str,
    allowed: &[&str],
) -> Result<&'a serde_json::Map<String, Value>> {
    let object = args
        .as_object()
        .with_context(|| format!("{tool} arguments must be an object"))?;
    for field in object.keys() {
        if !allowed.contains(&field.as_str()) {
            bail!("{tool}.{field} is not an allowed parameter");
        }
    }
    Ok(object)
}

fn optional_string(object: &serde_json::Map<String, Value>, field: &str) -> Result<Option<String>> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.is_empty() {
                bail!("{field} must not be empty");
            }
            Ok(Some(value.to_owned()))
        }
        Some(_) => bail!("{field} must be a string"),
    }
}

fn optional_phase(object: &serde_json::Map<String, Value>, field: &str) -> Result<Option<u8>> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .filter(|value| *value <= 8)
            .map(|value| Some(value as u8))
            .with_context(|| format!("{field} must be a phase in 0..=8")),
    }
}

fn pagination_from_object(
    object: &serde_json::Map<String, Value>,
    maximum: usize,
) -> Result<(usize, usize)> {
    let maximum = maximum.clamp(1, 100);
    let limit = match object.get("limit") {
        None | Some(Value::Null) => maximum.min(20),
        Some(value) => value
            .as_u64()
            .filter(|value| *value > 0)
            .map(|value| value as usize)
            .context("limit must be a positive integer")?,
    };
    if limit > maximum {
        bail!("limit exceeds configured maximum {maximum}");
    }
    let cursor = match object.get("cursor") {
        None | Some(Value::Null) => 0,
        Some(Value::String(value)) => value
            .parse::<usize>()
            .context("cursor must be a token returned by the prior call")?,
        Some(_) => bail!("cursor must be a string or null"),
    };
    Ok((limit, cursor))
}

fn scoped_filter(
    object: &serde_json::Map<String, Value>,
    field: &str,
    allowed: &BTreeSet<String>,
) -> Result<Option<String>> {
    scoped_value_filter(field, optional_string(object, field)?, allowed)
}

fn scoped_value_filter<T>(field: &str, value: Option<T>, allowed: &BTreeSet<T>) -> Result<Option<T>>
where
    T: Clone + Ord + std::fmt::Debug,
{
    match (value, allowed.len()) {
        (Some(_), 0) => bail!("{field} is not selectable in this scope"),
        (Some(value), 1) if allowed.contains(&value) => Ok(Some(value)),
        (Some(_), 1) => bail!("{field} is Rust-owned for this singleton scope"),
        (Some(value), _) if allowed.contains(&value) => Ok(Some(value)),
        (Some(_), _) => bail!("{field} is not in this scope's allowlist"),
        (None, 1) => Ok(allowed.iter().next().cloned()),
        (None, _) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> IndexToolRuntimeContext {
        IndexToolRuntimeContext::new(
            ToolRuntimeTurnContext {
                run_id: "run-1".to_owned(),
                session_id: "session-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                role: "manager.research".to_owned(),
                phase: Some(3),
            },
            IndexOwnedScope {
                run_id: "run-1".to_owned(),
                source_run_id: None,
                source_phase: 3,
                role: "manager.research".to_owned(),
                kind: IndexKind::PhaseSummary,
                ticker: Some("QQQ".to_owned()),
                topic_id: None,
                unit_key: "phase3:QQQ".to_owned(),
                source_payload_hash: "source-hash".to_owned(),
                authoritative_fields: Default::default(),
                index_id: "idx-000001".to_owned(),
            },
            IndexReadVisibility {
                kinds: BTreeSet::from([IndexKind::PhaseSummary]),
                tickers: BTreeSet::from(["QQQ".to_owned()]),
                max_page_size: 20,
                ..Default::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn only_read_definitions_exist() {
        assert!(definition(READ_INDEXES_NAME).is_some());
        assert!(definition(READ_INDEX_DETAILS_NAME).is_some());
        assert!(definition("create_index").is_none());
        assert!(definition("finalize_index").is_none());
    }

    #[test]
    fn singleton_filters_accept_the_bound_value_and_reject_other_values() {
        assert!(prepare_command(READ_INDEXES_NAME, json!({"ticker":"QQQ"}), &context()).is_ok());
        let error =
            prepare_command(READ_INDEXES_NAME, json!({"ticker":"SOXX"}), &context()).unwrap_err();
        assert!(error.to_string().contains("Rust-owned"));
    }

    #[test]
    fn details_require_a_visible_index() {
        let error = prepare_command(READ_INDEX_DETAILS_NAME, json!({}), &context()).unwrap_err();
        assert!(error.to_string().contains("preceding read_indexes"));
    }
}
