//! Unified, finalized-only Index/Detail persistence for phase summaries and
//! cross-run experience.  A model never supplies a path or an identifier:
//! callers construct an [`IndexScope`] from Rust-owned unit planning.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    content_hash_bytes, rename_dir_atomic, ContentHashDocument, FileStore, Result, RunLocation,
    SafeSlug, StoreError, Versioned,
};

pub const INDEX_SCHEMA_VERSION: u32 = 1;
pub const INDEX_DETAIL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    PhaseSummary,
    Experience,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Index {
    pub schema_version: u32,
    pub index_id: String,
    pub kind: IndexKind,
    pub run_id: String,
    pub source_run_id: Option<String>,
    pub source_phase: u8,
    pub role: String,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub pattern_key: Option<String>,
    pub applies_to_phases: Vec<u8>,
    pub summary: String,
    pub confidence: f64,
    pub authoritative_fields: Map<String, Value>,
    pub detail_count: usize,
    pub source_payload_hash: String,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexDetail {
    pub schema_version: u32,
    pub detail_id: String,
    pub index_id: String,
    pub section: DetailSection,
    pub detail: String,
    pub source_run_id: String,
    pub source_refs: Vec<String>,
    pub sort_order: usize,
    pub created_at: String,
    pub content_hash: String,
}

impl Versioned for Index {
    const SCHEMA_VERSION: u32 = INDEX_SCHEMA_VERSION;
}
impl Versioned for IndexDetail {
    const SCHEMA_VERSION: u32 = INDEX_DETAIL_SCHEMA_VERSION;
}
impl ContentHashDocument for Index {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}
impl ContentHashDocument for IndexDetail {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexScope {
    pub kind: IndexKind,
    pub location: Option<RunLocation>,
    pub index_id: String,
    pub run_id: String,
    pub source_run_id: Option<String>,
    pub source_phase: u8,
    pub role: String,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub source_payload_hash: String,
    pub authoritative_fields: Map<String, Value>,
    pub created_at: String,
}

impl IndexScope {
    pub fn validate(&self) -> Result<()> {
        if self.index_id.trim().is_empty()
            || self.run_id.trim().is_empty()
            || self.role.trim().is_empty()
            || self.source_payload_hash.trim().is_empty()
            || self.created_at.trim().is_empty()
            || self.source_phase > 8
        {
            return Err(StoreError::InvalidDocument {
                kind: "index scope",
                message: "missing required field or invalid phase".to_owned(),
            });
        }
        match self.kind {
            IndexKind::PhaseSummary if self.location.is_none() => {
                Err(StoreError::InvalidDocument {
                    kind: "index scope",
                    message: "phase_summary requires a run location".to_owned(),
                })
            }
            IndexKind::Experience if self.location.is_some() => Err(StoreError::InvalidDocument {
                kind: "index scope",
                message: "experience must not be stored under a run".to_owned(),
            }),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexInput {
    pub scope: IndexScope,
    pub summary: String,
    pub confidence: f64,
    pub pattern_key: Option<String>,
    pub applies_to_phases: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppendIndexDetailInput {
    pub scope: IndexScope,
    pub section: DetailSection,
    pub detail: String,
    pub source_refs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexQuery {
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DetailQuery {
    pub section: Option<DetailSection>,
    pub limit: usize,
    pub cursor: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexPage {
    pub indexes: Vec<Index>,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct DetailPage {
    pub details: Vec<IndexDetail>,
    pub next_cursor: Option<String>,
}

pub fn deterministic_experience_index_id(
    pattern_key: &str,
    ticker: Option<&str>,
    source_phase: u8,
) -> Result<String> {
    if pattern_key.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "experience index",
            message: "pattern_key is required".to_owned(),
        });
    }
    Ok(content_hash_bytes(
        format!(
            "experience\x1f{pattern_key}\x1f{}\x1f{source_phase}",
            ticker.unwrap_or("")
        )
        .as_bytes(),
    ))
}

fn final_dir(scope: &IndexScope) -> Result<PathBuf> {
    scope.validate()?;
    let slug = SafeSlug::new("index", &scope.index_id)?;
    Ok(match (&scope.kind, &scope.location) {
        (IndexKind::PhaseSummary, Some(location)) => {
            location.relative_root().join("index").join(slug.as_str())
        }
        (IndexKind::Experience, None) => PathBuf::from("knowledge/experience").join(slug.as_str()),
        _ => unreachable!("validated scope"),
    })
}
fn draft_dir(scope: &IndexScope) -> Result<PathBuf> {
    Ok(final_dir(scope)?.with_file_name(format!(
        ".index-draft-{}",
        SafeSlug::new("index", &scope.index_id)?.as_str()
    )))
}
fn index_path(dir: &Path) -> PathBuf {
    dir.join("index.json")
}
fn detail_path(dir: &Path, detail_id: &str) -> Result<PathBuf> {
    Ok(dir.join("details").join(format!(
        "{}.json",
        SafeSlug::new("detail", detail_id)?.as_str()
    )))
}

pub fn create_index(store: &FileStore, input: CreateIndexInput) -> Result<Index> {
    input.scope.validate()?;
    if input.summary.trim().is_empty()
        || !input.confidence.is_finite()
        || !(0.0..=1.0).contains(&input.confidence)
    {
        return Err(StoreError::InvalidDocument {
            kind: "index",
            message: "summary/confidence is invalid".to_owned(),
        });
    }
    if input.applies_to_phases.iter().any(|phase| *phase > 8)
        || input
            .applies_to_phases
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            != input.applies_to_phases.len()
    {
        return Err(StoreError::InvalidDocument {
            kind: "index",
            message: "applies_to_phases must be unique phases 0..=8".to_owned(),
        });
    }
    if matches!(input.scope.kind, IndexKind::Experience) != input.pattern_key.is_some() {
        return Err(StoreError::InvalidDocument {
            kind: "index",
            message: "experience requires pattern_key; phase_summary forbids it".to_owned(),
        });
    }
    let directory = draft_dir(&input.scope)?;
    if store.exists(&index_path(&directory))? {
        let existing: Index =
            store.read_versioned_json(&index_path(&directory), crate::FileSchemaKind::Index)?;
        if existing.index_id == input.scope.index_id {
            return Ok(existing);
        }
    }
    let document = Index {
        schema_version: INDEX_SCHEMA_VERSION,
        index_id: input.scope.index_id,
        kind: input.scope.kind,
        run_id: input.scope.run_id,
        source_run_id: input.scope.source_run_id,
        source_phase: input.scope.source_phase,
        role: input.scope.role,
        ticker: input.scope.ticker,
        topic_id: input.scope.topic_id,
        pattern_key: input.pattern_key,
        applies_to_phases: input.applies_to_phases,
        summary: input.summary,
        confidence: input.confidence,
        authoritative_fields: input.scope.authoritative_fields,
        detail_count: 0,
        source_payload_hash: input.scope.source_payload_hash,
        created_at: input.scope.created_at,
        content_hash: String::new(),
    };
    store.write_authoritative_json(&index_path(&directory), document)
}

pub fn append_index_detail(
    store: &FileStore,
    input: AppendIndexDetailInput,
) -> Result<IndexDetail> {
    let directory = draft_dir(&input.scope)?;
    let index: Index =
        store.read_versioned_json(&index_path(&directory), crate::FileSchemaKind::Index)?;
    if index.index_id != input.scope.index_id || input.detail.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "index detail",
            message: "draft missing or detail is empty".to_owned(),
        });
    }
    let source_refs = input
        .source_refs
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let detail_id = content_hash_bytes(
        format!(
            "{}\x1f{}\x1f{}\x1f{}",
            index.index_id,
            input.section as u8,
            input.detail.trim(),
            source_refs.join("\x1e")
        )
        .as_bytes(),
    );
    let path = detail_path(&directory, &detail_id)?;
    if store.exists(&path)? {
        return store.read_versioned_json(&path, crate::FileSchemaKind::Detail);
    }
    store.write_authoritative_json(
        &path,
        IndexDetail {
            schema_version: INDEX_DETAIL_SCHEMA_VERSION,
            detail_id,
            index_id: index.index_id,
            section: input.section,
            detail: input.detail,
            source_run_id: input.scope.source_run_id.unwrap_or(input.scope.run_id),
            source_refs,
            sort_order: index.detail_count + 1,
            created_at: input.scope.created_at,
            content_hash: String::new(),
        },
    )
}

pub fn finalize_index(store: &FileStore, scope: &IndexScope) -> Result<Index> {
    let draft = draft_dir(scope)?;
    let final_dir = final_dir(scope)?;
    if store.exists(&index_path(&final_dir))? {
        return store.read_versioned_json(&index_path(&final_dir), crate::FileSchemaKind::Index);
    }
    let mut index: Index =
        store.read_versioned_json(&index_path(&draft), crate::FileSchemaKind::Index)?;
    let details_dir = draft.join("details");
    let count = if store.exists(&details_dir)? {
        fs::read_dir(store.root().join(&details_dir))
            .map_err(|source| StoreError::Io {
                path: store.root().join(&details_dir),
                source,
            })?
            .count()
    } else {
        0
    };
    if count == 0 {
        return Err(StoreError::InvalidDocument {
            kind: "index",
            message: "finalize requires at least one Detail".to_owned(),
        });
    }
    index.detail_count = count;
    store.write_authoritative_json(&index_path(&draft), index.clone())?;
    rename_dir_atomic(store.root(), &draft, &final_dir)?;
    store.read_versioned_json(&index_path(&final_dir), crate::FileSchemaKind::Index)
}

pub fn read_indexes(
    store: &FileStore,
    run: Option<&RunLocation>,
    query: &IndexQuery,
) -> Result<IndexPage> {
    let base = match query.kind {
        Some(IndexKind::Experience) => PathBuf::from("knowledge/experience"),
        _ => run
            .ok_or_else(|| StoreError::InvalidDocument {
                kind: "index query",
                message: "run location is required for phase summaries".to_owned(),
            })?
            .relative_root()
            .join("index"),
    };
    let absolute = store.root().join(&base);
    if !absolute.exists() {
        return Ok(IndexPage {
            indexes: Vec::new(),
            next_cursor: None,
        });
    }
    let mut found = Vec::new();
    for entry in fs::read_dir(&absolute).map_err(|source| StoreError::Io {
        path: absolute.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| StoreError::Io {
            path: absolute.clone(),
            source,
        })?;
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        if !entry
            .file_type()
            .map_err(|source| StoreError::Io {
                path: entry.path(),
                source,
            })?
            .is_dir()
        {
            continue;
        }
        let relative = base.join(entry.file_name()).join("index.json");
        let index: Index = match store.read_versioned_json(&relative, crate::FileSchemaKind::Index)
        {
            Ok(value) => value,
            Err(_) => continue,
        };
        if matches_query(&index, query) {
            found.push(index);
        }
    }
    found.sort_by(|a, b| a.index_id.cmp(&b.index_id));
    let start = query.cursor.min(found.len());
    let limit = query.limit.clamp(1, 100);
    let end = (start + limit).min(found.len());
    Ok(IndexPage {
        indexes: found[start..end].to_vec(),
        next_cursor: (end < found.len()).then(|| end.to_string()),
    })
}

fn matches_query(index: &Index, q: &IndexQuery) -> bool {
    q.kind.is_none_or(|v| v == index.kind)
        && q.ticker
            .as_ref()
            .is_none_or(|v| index.ticker.as_ref() == Some(v))
        && q.source_phase.is_none_or(|v| v == index.source_phase)
        && q.applies_to_phase
            .is_none_or(|v| index.applies_to_phases.contains(&v))
        && q.role.as_ref().is_none_or(|v| &index.role == v)
        && q.topic_id
            .as_ref()
            .is_none_or(|v| index.topic_id.as_ref() == Some(v))
        && q.pattern_key
            .as_ref()
            .is_none_or(|v| index.pattern_key.as_ref() == Some(v))
}

pub fn read_index_details(
    store: &FileStore,
    scope: &IndexScope,
    query: &DetailQuery,
) -> Result<DetailPage> {
    let dir = final_dir(scope)?;
    let details_dir = dir.join("details");
    let absolute = store.root().join(&details_dir);
    if !absolute.exists() {
        return Ok(DetailPage {
            details: Vec::new(),
            next_cursor: None,
        });
    }
    let mut details = Vec::new();
    for entry in fs::read_dir(&absolute).map_err(|source| StoreError::Io {
        path: absolute.clone(),
        source,
    })? {
        let entry = entry.map_err(|source| StoreError::Io {
            path: absolute.clone(),
            source,
        })?;
        let name = entry.file_name();
        let relative = details_dir.join(name);
        let detail: IndexDetail =
            store.read_versioned_json(&relative, crate::FileSchemaKind::Detail)?;
        if detail.index_id == scope.index_id && query.section.is_none_or(|s| s == detail.section) {
            details.push(detail)
        }
    }
    details.sort_by_key(|d| d.sort_order);
    let start = query.cursor.min(details.len());
    let limit = query.limit.clamp(1, 100);
    let end = (start + limit).min(details.len());
    Ok(DetailPage {
        details: details[start..end].to_vec(),
        next_cursor: (end < details.len()).then(|| end.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    fn scope(location: RunLocation) -> IndexScope {
        IndexScope {
            kind: IndexKind::PhaseSummary,
            location: Some(location),
            index_id: "id/one".to_owned(),
            run_id: "run".to_owned(),
            source_run_id: None,
            source_phase: 2,
            role: "compressor.phase_summary".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            source_payload_hash: "source".to_owned(),
            authoritative_fields: Map::new(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        }
    }
    #[test]
    fn draft_finalizes_as_completed_index() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let scope = scope(RunLocation::new("2026-07-27", "run").unwrap());
        create_index(
            &store,
            CreateIndexInput {
                scope: scope.clone(),
                summary: "summary".to_owned(),
                confidence: 0.7,
                pattern_key: None,
                applies_to_phases: vec![3],
            },
        )
        .unwrap();
        append_index_detail(
            &store,
            AppendIndexDetailInput {
                scope: scope.clone(),
                section: DetailSection::Evidence,
                detail: "evidence".to_owned(),
                source_refs: vec!["a".to_owned()],
            },
        )
        .unwrap();
        assert_eq!(finalize_index(&store, &scope).unwrap().detail_count, 1);
        assert_eq!(
            read_indexes(
                &store,
                scope.location.as_ref(),
                &IndexQuery {
                    limit: 20,
                    ..Default::default()
                }
            )
            .unwrap()
            .indexes
            .len(),
            1
        );
    }
    #[test]
    fn incomplete_draft_is_not_visible() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let scope = scope(RunLocation::new("2026-07-27", "run").unwrap());
        create_index(
            &store,
            CreateIndexInput {
                scope: scope.clone(),
                summary: "summary".to_owned(),
                confidence: 0.7,
                pattern_key: None,
                applies_to_phases: vec![3],
            },
        )
        .unwrap();
        assert!(read_indexes(
            &store,
            scope.location.as_ref(),
            &IndexQuery {
                limit: 20,
                ..Default::default()
            }
        )
        .unwrap()
        .indexes
        .is_empty());
    }
}
