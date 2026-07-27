//! FileStore adapter for the five domain-only Index tools.
//!
//! This module is the sole workflow bridge from LLM Index commands to the
//! FileStore Index/Detail service. It carries a Rust-planned scope and never
//! accepts a model-selected path, run, or identifier.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use orchestrator_llm::tools::index_tools::{
    AppendIndexDetailCommand, CreateIndexCommand, DetailSection as ToolDetailSection,
    FinalizeIndexCommand, IndexKind as ToolIndexKind, IndexOwnedScope, IndexReadPage,
    IndexToolRuntimeBinding, IndexToolService, ReadIndexDetailsCommand, ReadIndexesCommand,
};
use orchestrator_store::{
    append_index_detail, create_index, finalize_index, read_index_details, read_indexes,
    AppendIndexDetailInput, CreateIndexInput, DetailQuery, DetailSection, FileStore, Index,
    IndexKind, IndexQuery, IndexScope, RunLocation,
};
use serde_json::{json, Map, Value};

/// Workflow-owned runtime plan for one Index tool unit. `created_at` is
/// captured before the loop begins and therefore remains stable across repair
/// turns; no model text participates in it.
#[derive(Debug, Clone)]
pub struct FileStoreIndexRuntimePlan {
    pub write_location: Option<RunLocation>,
    pub read_phase_summary_locations: Vec<RunLocation>,
    pub created_at: String,
    pub read_only: bool,
}

impl FileStoreIndexRuntimePlan {
    pub fn for_phase_summary(location: RunLocation, created_at: String) -> Self {
        Self {
            write_location: Some(location.clone()),
            read_phase_summary_locations: vec![location],
            created_at,
            read_only: false,
        }
    }

    pub fn for_experience(
        read_phase_summary_locations: Vec<RunLocation>,
        created_at: String,
    ) -> Self {
        Self {
            write_location: None,
            read_phase_summary_locations,
            created_at,
            read_only: false,
        }
    }

    /// Domain agents may consume completed knowledge, but never create a
    /// summary/experience Index. This uses the same reader implementation and
    /// visibility checks without granting a second writer authority.
    pub fn read_only(read_phase_summary_locations: Vec<RunLocation>, created_at: String) -> Self {
        Self {
            write_location: None,
            read_phase_summary_locations,
            created_at,
            read_only: true,
        }
    }

    fn validate_for(&self, owned: &IndexOwnedScope) -> Result<()> {
        if self.created_at.trim().is_empty() {
            bail!("FileStore Index runtime requires a Rust-owned created_at");
        }
        if self.read_only {
            if self.write_location.is_some() {
                bail!("read-only Index runtime must not carry a write location")
            }
            return Ok(());
        }
        match (owned.kind, self.write_location.is_some()) {
            (ToolIndexKind::PhaseSummary, true) | (ToolIndexKind::Experience, false) => Ok(()),
            (ToolIndexKind::PhaseSummary, false) => {
                bail!("phase_summary Index runtime requires its run location")
            }
            (ToolIndexKind::Experience, true) => {
                bail!("experience Index runtime must not carry a write run location")
            }
        }
    }
}

/// Construct the typed LLM runtime binding for a planned Index unit.
/// Legacy jobs must not call this helper; the LLM layer independently rejects
/// an Index binding unless `OutputMode::ToolManaged` is selected.
pub fn file_store_index_tool_runtime(
    store: FileStore,
    owned: IndexOwnedScope,
    visibility: orchestrator_llm::tools::index_tools::IndexReadVisibility,
    plan: FileStoreIndexRuntimePlan,
) -> Result<IndexToolRuntimeBinding> {
    plan.validate_for(&owned)?;
    let read_only = plan.read_only;
    let service = FileStoreIndexToolService {
        store,
        owned: owned.clone(),
        plan,
        read_scopes: Mutex::new(BTreeMap::new()),
    };
    let binding = IndexToolRuntimeBinding::new(owned, visibility, Arc::new(service))?;
    Ok(if read_only {
        binding.read_only()
    } else {
        binding
    })
}

#[derive(Debug)]
struct FileStoreIndexToolService {
    store: FileStore,
    owned: IndexOwnedScope,
    plan: FileStoreIndexRuntimePlan,
    /// Details can only expand Indexes returned by this service in the current
    /// runtime. The LLM layer adds a second per-turn visible-ID boundary.
    read_scopes: Mutex<BTreeMap<String, IndexScope>>,
}

impl FileStoreIndexToolService {
    fn write_scope(&self, owned: &IndexOwnedScope) -> Result<IndexScope> {
        // An Experience index ID is derived only after the model supplies a
        // pattern key to `create_index`.  The runtime context owns every
        // other field; accepting that one derived value is not a second
        // authority or a model-selected path.
        if owned != &self.owned
            && !(owned.kind == ToolIndexKind::Experience
                && self.owned.kind == ToolIndexKind::Experience
                && same_experience_scope_except_index(owned, &self.owned))
        {
            bail!("Index command scope differs from the Rust-planned unit")
        }
        self.plan.validate_for(owned)?;
        Ok(IndexScope {
            kind: store_kind(owned.kind),
            location: self.plan.write_location.clone(),
            index_id: owned.index_id.clone(),
            run_id: owned.run_id.clone(),
            source_run_id: owned.source_run_id.clone(),
            source_phase: owned.source_phase,
            role: owned.role.clone(),
            ticker: owned.ticker.clone(),
            topic_id: owned.topic_id.clone(),
            source_payload_hash: owned.source_payload_hash.clone(),
            authoritative_fields: Map::from_iter([(
                "unit_key".to_owned(),
                Value::String(owned.unit_key.clone()),
            )]),
            created_at: self.plan.created_at.clone(),
        })
    }

    fn scope_from_index(&self, index: &Index, location: Option<RunLocation>) -> IndexScope {
        IndexScope {
            kind: index.kind,
            location,
            index_id: index.index_id.clone(),
            run_id: index.run_id.clone(),
            source_run_id: index.source_run_id.clone(),
            source_phase: index.source_phase,
            role: index.role.clone(),
            ticker: index.ticker.clone(),
            topic_id: index.topic_id.clone(),
            source_payload_hash: index.source_payload_hash.clone(),
            authoritative_fields: index.authoritative_fields.clone(),
            created_at: index.created_at.clone(),
        }
    }

    fn remember_indexes(&self, indexes: &[(Index, Option<RunLocation>)]) -> Result<()> {
        let mut known = self
            .read_scopes
            .lock()
            .map_err(|_| anyhow::anyhow!("FileStore Index read scope lock poisoned"))?;
        for (index, location) in indexes {
            known.insert(
                index.index_id.clone(),
                self.scope_from_index(index, location.clone()),
            );
        }
        Ok(())
    }
}

fn same_experience_scope_except_index(left: &IndexOwnedScope, right: &IndexOwnedScope) -> bool {
    left.run_id == right.run_id
        && left.source_run_id == right.source_run_id
        && left.source_phase == right.source_phase
        && left.role == right.role
        && left.kind == right.kind
        && left.ticker == right.ticker
        && left.topic_id == right.topic_id
        && left.unit_key == right.unit_key
        && left.source_payload_hash == right.source_payload_hash
}

impl IndexToolService for FileStoreIndexToolService {
    fn create_index(&self, command: CreateIndexCommand) -> Result<Value> {
        let index = create_index(
            &self.store,
            CreateIndexInput {
                scope: self.write_scope(&command.scope)?,
                summary: command.summary,
                confidence: command.confidence,
                pattern_key: command.pattern_key,
                applies_to_phases: command.applies_to_phases,
            },
        )?;
        Ok(json!({
            "status": "draft",
            "index_id": index.index_id,
        }))
    }

    fn append_index_detail(&self, command: AppendIndexDetailCommand) -> Result<Value> {
        let detail = append_index_detail(
            &self.store,
            AppendIndexDetailInput {
                scope: self.write_scope(&command.scope)?,
                section: store_section(command.section),
                detail: command.detail,
                source_refs: command.source_refs,
            },
        )?;
        Ok(json!({
            "status": "draft",
            "index_id": detail.index_id,
            "detail_id": detail.detail_id,
        }))
    }

    fn finalize_index(&self, command: FinalizeIndexCommand) -> Result<Value> {
        Ok(serde_json::to_value(finalize_index(
            &self.store,
            &self.write_scope(&command.scope)?,
        )?)?)
    }

    fn read_indexes(&self, command: ReadIndexesCommand) -> Result<IndexReadPage> {
        let query = IndexQuery {
            kind: command.kind.map(store_kind),
            ticker: command.ticker,
            source_phase: command.source_phase,
            applies_to_phase: command.applies_to_phase,
            role: command.role,
            topic_id: command.topic_id,
            pattern_key: command.pattern_key,
            limit: command.limit,
            cursor: command.cursor,
        };
        let mut found = Vec::<(Index, Option<RunLocation>)>::new();
        let include_phase_summaries = !matches!(query.kind, Some(IndexKind::Experience));
        if include_phase_summaries {
            for location in &self.plan.read_phase_summary_locations {
                let mut per_run = query.clone();
                per_run.kind = Some(IndexKind::PhaseSummary);
                // Aggregate first, then apply the one model-visible cursor
                // across every allowed source. Per-run pagination would make
                // a cursor unstable when more than one source run is visible.
                per_run.limit = 100;
                per_run.cursor = 0;
                for index in read_indexes(&self.store, Some(location), &per_run)?.indexes {
                    found.push((index, Some(location.clone())));
                }
            }
        }
        if !matches!(query.kind, Some(IndexKind::PhaseSummary)) {
            let mut experiences = query;
            experiences.kind = Some(IndexKind::Experience);
            experiences.limit = 100;
            experiences.cursor = 0;
            for index in read_indexes(&self.store, None, &experiences)?.indexes {
                found.push((index, None));
            }
        }
        found.sort_by(|left, right| left.0.index_id.cmp(&right.0.index_id));
        found.dedup_by(|left, right| left.0.index_id == right.0.index_id);
        let start = command.cursor.min(found.len());
        let end = (start + command.limit.clamp(1, 100)).min(found.len());
        let page = &found[start..end];
        self.remember_indexes(page)?;
        let index_ids = page
            .iter()
            .map(|(index, _)| index.index_id.clone())
            .collect();
        let indexes = page
            .iter()
            .map(|(index, _)| index.clone())
            .collect::<Vec<_>>();
        Ok(IndexReadPage {
            output: json!({
                "indexes": indexes,
                "next_cursor": (end < found.len()).then(|| end.to_string()),
            }),
            index_ids,
        })
    }

    fn read_index_details(&self, command: ReadIndexDetailsCommand) -> Result<Value> {
        let scope = self
            .read_scopes
            .lock()
            .map_err(|_| anyhow::anyhow!("FileStore Index read scope lock poisoned"))?
            .get(&command.index_id)
            .cloned()
            .with_context(|| "Index details require an Index returned by this FileStore runtime")?;
        let page = read_index_details(
            &self.store,
            &scope,
            &DetailQuery {
                section: command.section.map(store_section),
                limit: command.limit,
                cursor: command.cursor,
            },
        )?;
        Ok(json!({
            "index_id": command.index_id,
            "details": page.details,
            "next_cursor": page.next_cursor,
        }))
    }
}

fn store_kind(kind: ToolIndexKind) -> IndexKind {
    match kind {
        ToolIndexKind::PhaseSummary => IndexKind::PhaseSummary,
        ToolIndexKind::Experience => IndexKind::Experience,
    }
}

fn store_section(section: ToolDetailSection) -> DetailSection {
    match section {
        ToolDetailSection::Evidence => DetailSection::Evidence,
        ToolDetailSection::CounterEvidence => DetailSection::CounterEvidence,
        ToolDetailSection::Conflict => DetailSection::Conflict,
        ToolDetailSection::DecisionHinge => DetailSection::DecisionHinge,
        ToolDetailSection::DataGap => DetailSection::DataGap,
        ToolDetailSection::Invalidation => DetailSection::Invalidation,
        ToolDetailSection::NextStep => DetailSection::NextStep,
        ToolDetailSection::Analysis => DetailSection::Analysis,
        ToolDetailSection::HistoricalCase => DetailSection::HistoricalCase,
        ToolDetailSection::Execution => DetailSection::Execution,
        ToolDetailSection::Risk => DetailSection::Risk,
        ToolDetailSection::Other => DetailSection::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_llm::{
        agent_loop::ToolRuntimeTurnContext, tools::index_tools::IndexReadVisibility,
    };
    use orchestrator_store::FileStoreOptions;
    use tempfile::tempdir;

    fn owned() -> IndexOwnedScope {
        IndexOwnedScope {
            run_id: "run-1".to_owned(),
            source_run_id: None,
            source_phase: 1,
            role: "compressor.phase_summary".to_owned(),
            kind: ToolIndexKind::PhaseSummary,
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            unit_key: "phase1:technical:QQQ".to_owned(),
            source_payload_hash: "sha256:source".to_owned(),
            index_id: "index-1".to_owned(),
        }
    }

    fn binding(store: FileStore) -> IndexToolRuntimeBinding {
        file_store_index_tool_runtime(
            store,
            owned(),
            IndexReadVisibility {
                applies_to_phases: [2].into_iter().collect(),
                source_refs: ["artifact:phase1:QQQ".to_owned()].into_iter().collect(),
                max_page_size: 20,
                ..Default::default()
            },
            FileStoreIndexRuntimePlan::for_phase_summary(
                RunLocation::new("2026-07-27", "run-1").unwrap(),
                "2026-07-27T00:00:00Z".to_owned(),
            ),
        )
        .unwrap()
    }

    fn turn() -> ToolRuntimeTurnContext {
        ToolRuntimeTurnContext {
            run_id: "run-1".to_owned(),
            session_id: "session-1".to_owned(),
            turn_id: "turn-1".to_owned(),
            role: "compressor.phase_summary".to_owned(),
            phase: Some(1),
        }
    }

    #[test]
    fn finalize_is_terminal_and_persists_the_canonical_index() {
        let temp = tempdir().unwrap();
        let runtime = binding(FileStore::open(temp.path(), FileStoreOptions::default()).unwrap())
            .build(turn())
            .unwrap();
        runtime
            .execute(
                "create_index",
                json!({
                    "summary": "Phase 1 technical view",
                    "confidence": 0.7,
                    "applies_to_phases": [2]
                }),
            )
            .unwrap();
        runtime
            .execute(
                "append_index_detail",
                json!({
                    "section": "evidence",
                    "detail": "Trend is above the daily moving average.",
                    "source_refs": ["artifact:phase1:QQQ"]
                }),
            )
            .unwrap();
        let terminal = runtime.execute("finalize_index", json!({})).unwrap();
        assert_eq!(terminal["terminal"], true);
        assert_eq!(terminal["artifact"]["index_id"], "index-1");
    }
}
