//! Domain-only Index/Detail tool contracts for tool-managed agents.
//!
//! These tools deliberately produce typed commands rather than accepting a
//! store path, arbitrary JSON mutation, or a model-chosen ownership field.
//! The FileStore runtime supplies [`IndexToolRuntimeContext`] from a planned
//! unit and executes the resulting command against its Index/Detail service.

use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{api_tool_name, ToolDefinition};
use crate::agent_loop::ToolRuntimeTurnContext;

pub const CREATE_INDEX_NAME: &str = "create_index";
pub const APPEND_INDEX_DETAIL_NAME: &str = "append_index_detail";
pub const FINALIZE_INDEX_NAME: &str = "finalize_index";
pub const READ_INDEXES_NAME: &str = "read_indexes";
pub const READ_INDEX_DETAILS_NAME: &str = "read_index_details";

const MODEL_OWNED_FIELD_NAMES: &[&str] = &[
    "store_root",
    "path",
    "source_path",
    "run_id",
    "source_run_id",
    "phase",
    "source_phase",
    "role",
    "kind",
    "index_id",
    "detail_id",
    "created_at",
    "schema_version",
    "content_hash",
    "unit_key",
    "source_payload_hash",
    "session_id",
    "turn_id",
    "profile",
    "profile_version",
    "builder_version",
    "round",
    "side",
    "stance",
    "reflection_task",
    "candidate_action",
    "artifact_id",
    "ticker",
    "topic_id",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    PhaseSummary,
    Experience,
}

impl IndexKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PhaseSummary => "phase_summary",
            Self::Experience => "experience",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "phase_summary" => Ok(Self::PhaseSummary),
            "experience" => Ok(Self::Experience),
            _ => bail!("kind must be phase_summary or experience"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DetailSection {
    Evidence,
    CounterEvidence,
    Conflict,
    DecisionHinge,
    DataGap,
    Invalidation,
    NextStep,
    Analysis,
    HistoricalCase,
    Execution,
    Risk,
    Other,
}

impl DetailSection {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Evidence => "evidence",
            Self::CounterEvidence => "counter_evidence",
            Self::Conflict => "conflict",
            Self::DecisionHinge => "decision_hinge",
            Self::DataGap => "data_gap",
            Self::Invalidation => "invalidation",
            Self::NextStep => "next_step",
            Self::Analysis => "analysis",
            Self::HistoricalCase => "historical_case",
            Self::Execution => "execution",
            Self::Risk => "risk",
            Self::Other => "other",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "evidence" => Ok(Self::Evidence),
            "counter_evidence" => Ok(Self::CounterEvidence),
            "conflict" => Ok(Self::Conflict),
            "decision_hinge" => Ok(Self::DecisionHinge),
            "data_gap" => Ok(Self::DataGap),
            "invalidation" => Ok(Self::Invalidation),
            "next_step" => Ok(Self::NextStep),
            "analysis" => Ok(Self::Analysis),
            "historical_case" => Ok(Self::HistoricalCase),
            "execution" => Ok(Self::Execution),
            "risk" => Ok(Self::Risk),
            "other" => Ok(Self::Other),
            _ => bail!("section is not an allowed Index Detail section"),
        }
    }
}

/// Immutable fields determined by the Rust Summary Unit planner or the
/// historical-reflection task. None may be supplied by the model.
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
    /// The deterministic Index id assigned by Rust for this one Summary Unit,
    /// or by the Experience key rule. It is intentionally not a tool argument.
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
            bail!(
                "Index tool scope requires Rust-owned run, role, unit, source hash, and index id"
            );
        }
        if self.source_phase > 8 {
            bail!("Index tool scope source_phase must be in 0..=8");
        }
        Ok(())
    }
}

/// Read-only visibility calculated by Rust. Set membership is the complete
/// authority boundary for model-selected filters and evidence references.
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

    fn allowed_source_refs(&self) -> BTreeSet<String> {
        self.source_refs
            .union(&self.evidence_ids)
            .cloned()
            .collect()
    }
}

/// Runtime context is initialized by Rust from an active session/turn plus a
/// planned Index Unit. The model cannot construct it or change its fields.
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
            bail!("Index tool turn context does not match the Rust-owned unit scope");
        }
        if turn.phase.is_none() {
            bail!("Index tool runtime context requires the current execution phase");
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

    /// Called only after the unified read service returns completed Indexes.
    /// `read_index_details` accepts IDs from this list, never arbitrary IDs.
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
            (None, _) => bail!("index_id is required when multiple visible Indexes are available"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CreateIndexCommand {
    pub scope: IndexOwnedScope,
    pub summary: String,
    pub confidence: f64,
    pub pattern_key: Option<String>,
    pub applies_to_phases: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AppendIndexDetailCommand {
    pub scope: IndexOwnedScope,
    pub section: DetailSection,
    pub detail: String,
    pub source_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FinalizeIndexCommand {
    pub scope: IndexOwnedScope,
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum IndexToolCommand {
    Create(CreateIndexCommand),
    Append(AppendIndexDetailCommand),
    Finalize(FinalizeIndexCommand),
    ReadIndexes(ReadIndexesCommand),
    ReadDetails(ReadIndexDetailsCommand),
}

/// The only execution seam permitted for the five Index domain tools. A
/// FileStore adapter owns persistence; this trait intentionally has no method
/// for raw file, SQL, path, or arbitrary JSON writes.
pub trait IndexToolService: Send + Sync {
    fn create_index(&self, command: CreateIndexCommand) -> Result<Value>;
    fn append_index_detail(&self, command: AppendIndexDetailCommand) -> Result<Value>;
    fn finalize_index(&self, command: FinalizeIndexCommand) -> Result<Value>;
    fn read_indexes(&self, command: ReadIndexesCommand) -> Result<IndexReadPage>;
    fn read_index_details(&self, command: ReadIndexDetailsCommand) -> Result<Value>;
}

impl<T> IndexToolService for Arc<T>
where
    T: IndexToolService + ?Sized,
{
    fn create_index(&self, command: CreateIndexCommand) -> Result<Value> {
        (**self).create_index(command)
    }

    fn append_index_detail(&self, command: AppendIndexDetailCommand) -> Result<Value> {
        (**self).append_index_detail(command)
    }

    fn finalize_index(&self, command: FinalizeIndexCommand) -> Result<Value> {
        (**self).finalize_index(command)
    }

    fn read_indexes(&self, command: ReadIndexesCommand) -> Result<IndexReadPage> {
        (**self).read_indexes(command)
    }

    fn read_index_details(&self, command: ReadIndexDetailsCommand) -> Result<Value> {
        (**self).read_index_details(command)
    }
}

/// Immutable wiring supplied by the workflow for one migrated agent unit.
/// It is deliberately a typed domain seam: callers can provide an Index
/// service, but never a generic file/SQL/path mutation callback.
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

/// A completed, visibility-checked list response. The FileStore adapter must
/// return only Index IDs that it actually rendered in `output`.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexReadPage {
    pub output: Value,
    pub index_ids: Vec<String>,
}

/// Runtime adapter used only by migrated ToolManaged profiles. It converts
/// model arguments to ownership-safe commands, updates the per-turn visible
/// Index allowlist after reads, and makes `finalize_index` terminal.
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
            IndexToolCommand::Create(command) => self.service.create_index(command),
            IndexToolCommand::Append(command) => self.service.append_index_detail(command),
            IndexToolCommand::Finalize(command) => {
                let artifact = self.service.finalize_index(command)?;
                Ok(json!({
                    "status": "completed",
                    "terminal": true,
                    "artifact": artifact,
                }))
            }
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
        CREATE_INDEX_NAME => Some(create_index_definition()),
        APPEND_INDEX_DETAIL_NAME => Some(append_index_detail_definition()),
        FINALIZE_INDEX_NAME => Some(finalize_index_definition()),
        READ_INDEXES_NAME => Some(read_indexes_definition()),
        READ_INDEX_DETAILS_NAME => Some(read_index_details_definition()),
        _ => None,
    }
}

pub fn create_index_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(CREATE_INDEX_NAME),
        description: "Start the one Rust-planned Index for this unit. Ownership, source phase, ticker/topic scope, Index ID, timestamps, and authoritative fields are fixed by the runtime.".to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {
                "summary": {"type": "string", "minLength": 1},
                "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                "pattern_key": {"type": ["string", "null"], "minLength": 1},
                "applies_to_phases": {
                    "type": "array",
                    "items": {"type": "integer", "minimum": 0, "maximum": 8},
                    "uniqueItems": true
                }
            },
            "required": ["summary", "confidence", "applies_to_phases"],
            "additionalProperties": false
        }),
    }
}

pub fn append_index_detail_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(APPEND_INDEX_DETAIL_NAME),
        description: "Append one independently understandable Detail to this unit's Rust-owned Index. Source references must have been made visible by a permitted read.".to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {
                "section": {
                    "type": "string",
                    "enum": ["evidence", "counter_evidence", "conflict", "decision_hinge", "data_gap", "invalidation", "next_step", "analysis", "historical_case", "execution", "risk", "other"]
                },
                "detail": {"type": "string", "minLength": 1},
                "source_refs": {"type": "array", "items": {"type": "string"}, "uniqueItems": true}
            },
            "required": ["section", "detail", "source_refs"],
            "additionalProperties": false
        }),
    }
}

pub fn finalize_index_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(FINALIZE_INDEX_NAME),
        description: "Terminally validate and atomically finalize this unit's Index and Details. It ends the tool-managed agent loop on success.".to_owned(),
        parameters: json!({
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": false
        }),
    }
}

pub fn read_indexes_definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(READ_INDEXES_NAME),
        description: "List completed, visibility-authorized Index records. The runtime restricts every supplied filter to the current role's allowed scope.".to_owned(),
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
        description: "Expand completed Details from an Index returned by read_indexes. If exactly one Index is visible, omit index_id; otherwise select an ID from the returned allowlist.".to_owned(),
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

/// Parse a model tool call into a command whose ownership is fixed by
/// `context`. This is intentionally independent of any legacy SQL runtime.
pub fn prepare_command(
    name: &str,
    args: Value,
    context: &IndexToolRuntimeContext,
) -> Result<IndexToolCommand> {
    match name {
        CREATE_INDEX_NAME => parse_create(args, context).map(IndexToolCommand::Create),
        APPEND_INDEX_DETAIL_NAME => parse_append(args, context).map(IndexToolCommand::Append),
        FINALIZE_INDEX_NAME => parse_finalize(args, context).map(IndexToolCommand::Finalize),
        READ_INDEXES_NAME => parse_read_indexes(args, context).map(IndexToolCommand::ReadIndexes),
        READ_INDEX_DETAILS_NAME => {
            parse_read_details(args, context).map(IndexToolCommand::ReadDetails)
        }
        _ => bail!("unknown Index tool name: {name}"),
    }
}

fn parse_create(args: Value, context: &IndexToolRuntimeContext) -> Result<CreateIndexCommand> {
    let object = checked_object(
        &args,
        CREATE_INDEX_NAME,
        &["summary", "confidence", "pattern_key", "applies_to_phases"],
    )?;
    let summary = required_string(object, "summary")?;
    let confidence = object
        .get("confidence")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .context("create_index.confidence must be a finite number in 0..=1")?;
    let pattern_key = optional_string(object, "pattern_key")?;
    match (context.owned.kind, pattern_key.as_ref()) {
        (IndexKind::Experience, None) => bail!("experience Indexes require pattern_key"),
        (IndexKind::PhaseSummary, Some(_)) => {
            bail!("phase_summary Indexes must not provide pattern_key")
        }
        _ => {}
    }
    let applies_to_phases = phase_array(object, "applies_to_phases")?;
    ensure_subset(
        "applies_to_phases",
        &applies_to_phases,
        &context.visibility.applies_to_phases,
    )?;
    Ok(CreateIndexCommand {
        scope: context.owned.clone(),
        summary,
        confidence,
        pattern_key,
        applies_to_phases,
    })
}

fn parse_append(
    args: Value,
    context: &IndexToolRuntimeContext,
) -> Result<AppendIndexDetailCommand> {
    let object = checked_object(
        &args,
        APPEND_INDEX_DETAIL_NAME,
        &["section", "detail", "source_refs"],
    )?;
    let section = DetailSection::parse(required_string(object, "section")?.as_str())?;
    let detail = required_string(object, "detail")?;
    let source_refs = string_array(object, "source_refs")?;
    let allowed = context.visibility.allowed_source_refs();
    for source_ref in &source_refs {
        if !allowed.contains(source_ref) {
            bail!("source_ref {source_ref:?} is not visible in this Index tool scope");
        }
    }
    Ok(AppendIndexDetailCommand {
        scope: context.owned.clone(),
        section,
        detail,
        source_refs,
    })
}

fn parse_finalize(args: Value, context: &IndexToolRuntimeContext) -> Result<FinalizeIndexCommand> {
    checked_object(&args, FINALIZE_INDEX_NAME, &[])?;
    Ok(FinalizeIndexCommand {
        scope: context.owned.clone(),
    })
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
    let source_phase = optional_phase(object, "source_phase")?;
    let source_phase = scoped_value_filter(
        "source_phase",
        source_phase,
        &context.visibility.source_phases,
    )?;
    let applies_to_phase = optional_phase(object, "applies_to_phase")?;
    let applies_to_phase = scoped_value_filter(
        "applies_to_phase",
        applies_to_phase,
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
        // Read filters may select only from a runtime allowlist. All write
        // operations omit Rust-owned fields from `allowed`, so they reach the
        // explicit ownership error below instead of becoming mutable.
        if !allowed.contains(&field.as_str()) && MODEL_OWNED_FIELD_NAMES.contains(&field.as_str()) {
            bail!("{tool}.{field} is Rust-owned and must not be supplied by the model");
        }
        if !allowed.contains(&field.as_str()) {
            bail!("{tool}.{field} is not an allowed parameter");
        }
    }
    Ok(object)
}

fn required_string(object: &serde_json::Map<String, Value>, field: &str) -> Result<String> {
    optional_string(object, field)?.with_context(|| format!("{field} is required"))
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

fn string_array(object: &serde_json::Map<String, Value>, field: &str) -> Result<Vec<String>> {
    let values = object
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("{field} must be an array"))?;
    let mut unique = BTreeSet::new();
    let mut result = Vec::with_capacity(values.len());
    for value in values {
        let value = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .with_context(|| format!("{field} entries must be non-empty strings"))?;
        if !unique.insert(value.to_owned()) {
            bail!("{field} entries must be unique");
        }
        result.push(value.to_owned());
    }
    Ok(result)
}

fn phase_array(object: &serde_json::Map<String, Value>, field: &str) -> Result<Vec<u8>> {
    let values = object
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("{field} must be an array"))?;
    let mut unique = BTreeSet::new();
    for value in values {
        let phase = value
            .as_u64()
            .filter(|value| *value <= 8)
            .map(|value| value as u8)
            .with_context(|| format!("{field} entries must be phases 0..=8"))?;
        if !unique.insert(phase) {
            bail!("{field} entries must be unique");
        }
    }
    Ok(unique.into_iter().collect())
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
            .context("cursor must be a pagination token returned by the prior call")?,
        Some(_) => bail!("cursor must be a string or null"),
    };
    Ok((limit, cursor))
}

fn scoped_filter(
    object: &serde_json::Map<String, Value>,
    field: &str,
    allowed: &BTreeSet<String>,
) -> Result<Option<String>> {
    let value = optional_string(object, field)?;
    match (value, allowed.len()) {
        (Some(_), 0) => bail!("{field} is not selectable in this scope"),
        (Some(_), 1) => bail!("{field} is Rust-owned for this singleton scope"),
        (Some(value), _) if allowed.contains(&value) => Ok(Some(value)),
        (Some(_), _) => bail!("{field} is not in this scope's allowlist"),
        (None, _) => Ok(None),
    }
}

fn scoped_value_filter<T>(field: &str, value: Option<T>, allowed: &BTreeSet<T>) -> Result<Option<T>>
where
    T: Ord + std::fmt::Display,
{
    match (value, allowed.len()) {
        (Some(_), 0) => bail!("{field} is not selectable in this scope"),
        (Some(_), 1) => bail!("{field} is Rust-owned for this singleton scope"),
        (Some(value), _) if allowed.contains(&value) => Ok(Some(value)),
        (Some(_), _) => bail!("{field} is not in this scope's allowlist"),
        (None, _) => Ok(None),
    }
}

fn ensure_value_visible<T>(field: &str, value: T, allowed: &BTreeSet<T>) -> Result<()>
where
    T: Ord + std::fmt::Display,
{
    if !allowed.contains(&value) {
        bail!("{field}={value} is not in this scope's allowlist");
    }
    Ok(())
}

fn ensure_subset(field: &str, values: &[u8], allowed: &BTreeSet<u8>) -> Result<()> {
    for value in values {
        ensure_value_visible(field, *value, allowed)?;
    }
    Ok(())
}

impl std::fmt::Display for IndexKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> IndexOwnedScope {
        IndexOwnedScope {
            run_id: "run-1".to_owned(),
            source_run_id: None,
            source_phase: 2,
            role: "compressor.phase_summary".to_owned(),
            kind: IndexKind::PhaseSummary,
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            unit_key: "phase2-final".to_owned(),
            source_payload_hash: "source-hash".to_owned(),
            index_id: "index-1".to_owned(),
        }
    }

    fn context() -> IndexToolRuntimeContext {
        IndexToolRuntimeContext::new(
            ToolRuntimeTurnContext {
                run_id: "run-1".to_owned(),
                session_id: "session-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                role: "compressor.phase_summary".to_owned(),
                phase: Some(2),
            },
            scope(),
            IndexReadVisibility {
                kinds: BTreeSet::from([IndexKind::PhaseSummary, IndexKind::Experience]),
                tickers: BTreeSet::from(["QQQ".to_owned()]),
                source_phases: BTreeSet::from([1, 2]),
                applies_to_phases: BTreeSet::from([3, 4]),
                roles: BTreeSet::from([
                    "analyst.technical".to_owned(),
                    "analyst.news_macro".to_owned(),
                ]),
                topic_ids: BTreeSet::new(),
                pattern_keys: BTreeSet::from(["volatility-breakout".to_owned()]),
                source_refs: BTreeSet::from(["artifact:phase2".to_owned()]),
                evidence_ids: BTreeSet::from(["evidence:1".to_owned()]),
                max_page_size: 20,
            },
        )
        .unwrap()
    }

    #[test]
    fn every_definition_declares_required_array() {
        for name in [
            CREATE_INDEX_NAME,
            APPEND_INDEX_DETAIL_NAME,
            FINALIZE_INDEX_NAME,
            READ_INDEXES_NAME,
            READ_INDEX_DETAILS_NAME,
        ] {
            assert!(definition(name)
                .unwrap()
                .parameters
                .get("required")
                .is_some_and(Value::is_array));
        }
    }

    #[test]
    fn create_index_rejects_rust_owned_fields() {
        let error = prepare_command(
            CREATE_INDEX_NAME,
            json!({
                "summary": "topic conflict",
                "confidence": 0.7,
                "applies_to_phases": [3],
                "run_id": "model-run"
            }),
            &context(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("Rust-owned"));
    }

    #[test]
    fn create_index_derives_scope_and_rejects_pattern_key_for_summary() {
        let command = prepare_command(
            CREATE_INDEX_NAME,
            json!({"summary": "topic conflict", "confidence": 0.7, "applies_to_phases": [3]}),
            &context(),
        )
        .unwrap();
        let IndexToolCommand::Create(command) = command else {
            panic!("expected create command");
        };
        assert_eq!(command.scope.run_id, "run-1");
        assert_eq!(command.scope.index_id, "index-1");
        let error = prepare_command(
            CREATE_INDEX_NAME,
            json!({"summary": "topic conflict", "confidence": 0.7, "pattern_key": "x", "applies_to_phases": [3]}),
            &context(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("must not provide pattern_key"));
    }

    #[test]
    fn append_only_accepts_visible_source_refs_or_evidence_ids() {
        let command = prepare_command(
            APPEND_INDEX_DETAIL_NAME,
            json!({"section": "evidence", "detail": "source", "source_refs": ["evidence:1", "artifact:phase2"]}),
            &context(),
        )
        .unwrap();
        assert!(matches!(command, IndexToolCommand::Append(_)));
        let error = prepare_command(
            APPEND_INDEX_DETAIL_NAME,
            json!({"section": "evidence", "detail": "source", "source_refs": ["unread"]}),
            &context(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("not visible"));
    }

    #[test]
    fn singleton_ticker_and_unknown_filters_are_rejected() {
        let error =
            prepare_command(READ_INDEXES_NAME, json!({"ticker": "QQQ"}), &context()).unwrap_err();
        assert!(error.to_string().contains("singleton"));
        let error =
            prepare_command(READ_INDEXES_NAME, json!({"source_phase": 7}), &context()).unwrap_err();
        assert!(error.to_string().contains("allowlist"));
    }

    #[test]
    fn detail_read_uses_returned_index_allowlist() {
        let context = context();
        let error = prepare_command(READ_INDEX_DETAILS_NAME, json!({}), &context).unwrap_err();
        assert!(error.to_string().contains("preceding read_indexes"));
        context
            .record_visible_indexes(["index-visible".to_owned()])
            .unwrap();
        let command = prepare_command(READ_INDEX_DETAILS_NAME, json!({}), &context).unwrap();
        let IndexToolCommand::ReadDetails(command) = command else {
            panic!("expected read details");
        };
        assert_eq!(command.index_id, "index-visible");
        let error = prepare_command(
            READ_INDEX_DETAILS_NAME,
            json!({"index_id": "hidden-index"}),
            &context,
        )
        .unwrap_err();
        assert!(error.to_string().contains("not visible"));
    }

    #[test]
    fn context_refuses_turn_scope_mismatch() {
        let error = IndexToolRuntimeContext::new(
            ToolRuntimeTurnContext {
                run_id: "other-run".to_owned(),
                session_id: "session-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                role: "compressor.phase_summary".to_owned(),
                phase: Some(2),
            },
            scope(),
            IndexReadVisibility::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn historical_experience_keeps_historical_source_phase_rust_owned() {
        let mut historical = scope();
        historical.kind = IndexKind::Experience;
        historical.source_phase = 2;
        historical.role = "reflector.historical".to_owned();
        let context = IndexToolRuntimeContext::new(
            ToolRuntimeTurnContext {
                run_id: "run-1".to_owned(),
                session_id: "session-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                role: "reflector.historical".to_owned(),
                phase: Some(0),
            },
            historical,
            IndexReadVisibility {
                applies_to_phases: BTreeSet::from([3]),
                ..IndexReadVisibility::default()
            },
        )
        .unwrap();
        let command = prepare_command(
            CREATE_INDEX_NAME,
            json!({
                "summary": "Wait for confirmation after volatility breaks.",
                "confidence": 0.6,
                "pattern_key": "volatility-breakout",
                "applies_to_phases": [3]
            }),
            &context,
        )
        .unwrap();
        let IndexToolCommand::Create(command) = command else {
            panic!("expected create command");
        };
        assert_eq!(command.scope.source_phase, 2);
        assert_eq!(context.turn_context().phase, Some(0));
    }
}
