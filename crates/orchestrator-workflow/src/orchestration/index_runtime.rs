//! Read-only FileStore adapter for model-visible Index tools.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use orchestrator_llm::tools::index_tools::{
    DetailSection as ToolDetailSection, IndexKind as ToolIndexKind, IndexOwnedScope, IndexReadPage,
    IndexToolRuntimeBinding, IndexToolService, ReadIndexDetailsCommand, ReadIndexesCommand,
};
use orchestrator_store::{
    read_all_indexes, read_index_details, DetailQuery, DetailSection, FileStore, Index, IndexKind,
    IndexQuery, IndexScope, RunLocation,
};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct FileStoreIndexRuntimePlan {
    pub read_phase_summary_locations: Vec<RunLocation>,
    pub created_at: String,
}

impl FileStoreIndexRuntimePlan {
    pub fn read_only(read_phase_summary_locations: Vec<RunLocation>, created_at: String) -> Self {
        Self {
            read_phase_summary_locations,
            created_at,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.created_at.trim().is_empty() {
            bail!("FileStore Index read runtime requires a Rust-owned created_at");
        }
        Ok(())
    }
}

pub fn file_store_index_tool_runtime(
    store: FileStore,
    owned: IndexOwnedScope,
    visibility: orchestrator_llm::tools::index_tools::IndexReadVisibility,
    plan: FileStoreIndexRuntimePlan,
) -> Result<IndexToolRuntimeBinding> {
    plan.validate()?;
    let service = FileStoreIndexToolService {
        store,
        plan,
        read_scopes: Mutex::new(BTreeMap::new()),
    };
    IndexToolRuntimeBinding::new(owned, visibility, Arc::new(service))
}

#[derive(Debug)]
struct FileStoreIndexToolService {
    store: FileStore,
    plan: FileStoreIndexRuntimePlan,
    /// Details may only expand Indexes returned by this runtime.
    read_scopes: Mutex<BTreeMap<String, IndexScope>>,
}

impl FileStoreIndexToolService {
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

impl IndexToolService for FileStoreIndexToolService {
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
        if !matches!(query.kind, Some(IndexKind::Experience)) {
            for location in &self.plan.read_phase_summary_locations {
                let mut per_run = query.clone();
                per_run.kind = Some(IndexKind::PhaseSummary);
                per_run.limit = 100;
                per_run.cursor = 0;
                for index in read_all_indexes(&self.store, Some(location), &per_run)? {
                    found.push((index, Some(location.clone())));
                }
            }
        }
        if !matches!(query.kind, Some(IndexKind::PhaseSummary)) {
            let mut experiences = query;
            experiences.kind = Some(IndexKind::Experience);
            experiences.limit = 100;
            experiences.cursor = 0;
            for index in read_all_indexes(&self.store, None, &experiences)? {
                found.push((index, None));
            }
        }
        found.sort_by(|left, right| left.0.index_id.cmp(&right.0.index_id));
        found.dedup_by(|left, right| left.0.index_id == right.0.index_id);
        let start = command.cursor.min(found.len());
        let end = (start + command.limit.clamp(1, 100)).min(found.len());
        let page = &found[start..end];
        self.remember_indexes(page)?;
        Ok(IndexReadPage {
            output: json!({
                "indexes": page.iter().map(|(index, _)| index).collect::<Vec<_>>(),
                "next_cursor": (end < found.len()).then(|| end.to_string()),
            }),
            index_ids: page
                .iter()
                .map(|(index, _)| index.index_id.clone())
                .collect(),
        })
    }

    fn read_index_details(&self, command: ReadIndexDetailsCommand) -> Result<Value> {
        let scope = self
            .read_scopes
            .lock()
            .map_err(|_| anyhow::anyhow!("FileStore Index read scope lock poisoned"))?
            .get(&command.index_id)
            .cloned()
            .with_context(|| "Index details require an Index returned by this runtime")?;
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
            "source_run_id": scope.source_run_id.as_deref().unwrap_or(&scope.run_id),
            "source_phase": scope.source_phase,
            "ticker": scope.ticker,
            "topic_id": scope.topic_id,
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
        agent_loop::ToolRuntimeTurnContext,
        tools::index_tools::{IndexReadVisibility, READ_INDEXES_NAME},
    };
    use orchestrator_store::{FileStoreOptions, RunLocation};
    use tempfile::tempdir;

    #[test]
    fn binding_is_read_only_and_returns_an_empty_page() {
        let temp = tempdir().unwrap();
        let location = RunLocation::new("2026-07-29", "run-1").unwrap();
        let binding = file_store_index_tool_runtime(
            FileStore::open(temp.path(), FileStoreOptions::default()).unwrap(),
            IndexOwnedScope {
                run_id: "run-1".to_owned(),
                source_run_id: None,
                source_phase: 2,
                role: "mediator.topic".to_owned(),
                kind: ToolIndexKind::PhaseSummary,
                ticker: Some("QQQ".to_owned()),
                topic_id: None,
                unit_key: "phase2:QQQ".to_owned(),
                source_payload_hash: "hash".to_owned(),
                authoritative_fields: Default::default(),
                index_id: "idx-000001".to_owned(),
            },
            IndexReadVisibility {
                max_page_size: 20,
                ..Default::default()
            },
            FileStoreIndexRuntimePlan::read_only(vec![location], "2026-07-29T00:00:00Z".to_owned()),
        )
        .unwrap();
        let runtime = binding
            .build(ToolRuntimeTurnContext {
                run_id: "run-1".to_owned(),
                session_id: "session-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                role: "mediator.topic".to_owned(),
                phase: Some(2),
            })
            .unwrap();
        let output = runtime.execute(READ_INDEXES_NAME, json!({})).unwrap();
        assert_eq!(output["indexes"], json!([]));
    }
}
