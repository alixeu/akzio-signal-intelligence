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

impl IndexKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PhaseSummary => "phase_summary",
            Self::Experience => "experience",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "phase_summary" => Ok(Self::PhaseSummary),
            "experience" => Ok(Self::Experience),
            _ => Err(StoreError::InvalidDocument {
                kind: "index kind",
                message: "kind must be phase_summary or experience".to_owned(),
            }),
        }
    }
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

    pub fn parse(value: &str) -> Result<Self> {
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
            _ => Err(StoreError::InvalidDocument {
                kind: "index detail section",
                message: "section is not an allowed Index Detail section".to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// One outcome-backed historical case to retain under a reusable Experience
/// Index.  This is deliberately a specialized service rather than an escape
/// hatch for mutating arbitrary completed Indexes: only `historical_case`
/// Details may be appended, and the historical source run is the idempotency
/// boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordExperienceCaseInput {
    /// The current reflection run owns `run_id`; `source_run_id` is the one
    /// immutable historical run whose outcome was analyzed.
    pub scope: IndexScope,
    pub pattern_key: String,
    pub summary: String,
    pub confidence: f64,
    pub applies_to_phases: Vec<u8>,
    pub detail: String,
    pub source_refs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExperienceCaseDisposition {
    Created,
    Appended,
    DuplicateSourceRun,
}

/// Derived at read/write time from unique historical source runs.  It is not
/// stored in `index.json`, so no candidate/promotion/version state exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperienceLevel {
    RecentEpisode,
    RepeatedWarning,
    ActivePolicy,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordExperienceCaseOutcome {
    pub index: Index,
    pub detail: IndexDetail,
    pub disposition: ExperienceCaseDisposition,
    pub level: ExperienceLevel,
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
            "kind=experience\x1fpattern_key={pattern_key}\x1fticker={}\x1fsource_phase={source_phase}",
            ticker.unwrap_or("")
        )
        .as_bytes(),
    ))
}

fn validate_experience_identity(
    scope: &IndexScope,
    pattern_key: &str,
    require_source_run: bool,
) -> Result<()> {
    scope.validate()?;
    if scope.kind != IndexKind::Experience {
        return Err(StoreError::InvalidDocument {
            kind: "experience index",
            message: "experience service requires kind=experience".to_owned(),
        });
    }
    if require_source_run
        && scope
            .source_run_id
            .as_deref()
            .is_none_or(|source_run_id| source_run_id.trim().is_empty())
    {
        return Err(StoreError::InvalidDocument {
            kind: "experience index",
            message: "experience requires a historical source_run_id".to_owned(),
        });
    }
    let expected = deterministic_experience_index_id(
        pattern_key,
        scope.ticker.as_deref(),
        scope.source_phase,
    )?;
    if scope.index_id != expected {
        return Err(StoreError::InvalidDocument {
            kind: "experience index",
            message: "index_id must be hash(kind, pattern_key, ticker, source_phase)".to_owned(),
        });
    }
    Ok(())
}

fn normalized_source_refs(source_refs: Vec<String>) -> Result<Vec<String>> {
    let refs = source_refs
        .into_iter()
        .map(|source_ref| source_ref.trim().to_owned())
        .collect::<BTreeSet<_>>();
    if refs.iter().any(|source_ref| source_ref.is_empty()) {
        return Err(StoreError::InvalidDocument {
            kind: "index detail",
            message: "source_refs cannot include empty values".to_owned(),
        });
    }
    Ok(refs.into_iter().collect())
}

fn experience_lock_path(scope: &IndexScope) -> Result<PathBuf> {
    Ok(PathBuf::from("knowledge/experience").join(format!(
        ".experience-{}.lock",
        SafeSlug::new("index", &scope.index_id)?.as_str()
    )))
}

fn validate_existing_experience(
    existing: &Index,
    scope: &IndexScope,
    pattern_key: &str,
) -> Result<()> {
    if existing.kind != IndexKind::Experience
        || existing.index_id != scope.index_id
        || existing.pattern_key.as_deref() != Some(pattern_key)
        || existing.ticker != scope.ticker
        || existing.source_phase != scope.source_phase
    {
        return Err(StoreError::InvalidDocument {
            kind: "experience index",
            message: "existing Index conflicts with the requested experience identity".to_owned(),
        });
    }
    Ok(())
}

fn experience_case_detail_id(
    index_id: &str,
    source_run_id: &str,
    detail: &str,
    source_refs: &[String],
) -> String {
    content_hash_bytes(
        format!(
            "{index_id}\x1fsection=historical_case\x1fsource_run_id={source_run_id}\x1fdetail={detail}\x1fsource_refs={}",
            source_refs.join("\x1e")
        )
        .as_bytes(),
    )
}

fn read_all_completed_details(store: &FileStore, scope: &IndexScope) -> Result<Vec<IndexDetail>> {
    let mut cursor = 0;
    let mut details = Vec::new();
    loop {
        let page = read_index_details(
            store,
            scope,
            &DetailQuery {
                limit: 100,
                cursor,
                ..Default::default()
            },
        )?;
        details.extend(page.details);
        let Some(next) = page.next_cursor else {
            return Ok(details);
        };
        cursor = next
            .parse::<usize>()
            .map_err(|_| StoreError::InvalidDocument {
                kind: "index detail",
                message: "generated detail pagination cursor is invalid".to_owned(),
            })?;
    }
}

fn experience_level_from_details(details: &[IndexDetail]) -> ExperienceLevel {
    let distinct_source_runs = details
        .iter()
        .filter(|detail| detail.section == DetailSection::HistoricalCase)
        .map(|detail| detail.source_run_id.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    match distinct_source_runs {
        0 => ExperienceLevel::RecentEpisode,
        1 => ExperienceLevel::RecentEpisode,
        2 => ExperienceLevel::RepeatedWarning,
        _ => ExperienceLevel::ActivePolicy,
    }
}

/// Return the derived experience level without persisting any promotion or
/// version state.  Only completed Indexes are readable here.
pub fn experience_level(store: &FileStore, scope: &IndexScope) -> Result<ExperienceLevel> {
    let index: Index = store.read_versioned_json(
        &index_path(&final_dir(scope)?),
        crate::FileSchemaKind::Index,
    )?;
    let pattern_key = index
        .pattern_key
        .as_deref()
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "experience index",
            message: "completed experience Index is missing pattern_key".to_owned(),
        })?;
    validate_experience_identity(scope, pattern_key, false)?;
    Ok(experience_level_from_details(&read_all_completed_details(
        store, scope,
    )?))
}

/// Create an Experience Index for its first historical source run, or append
/// one new `historical_case` Detail for a different source run.  A source run
/// is recorded at most once: retrying a reflection returns that original
/// Detail, never overwrites it, and never manufactures another version.
pub fn record_experience_case(
    store: &FileStore,
    input: RecordExperienceCaseInput,
) -> Result<RecordExperienceCaseOutcome> {
    validate_experience_identity(&input.scope, &input.pattern_key, true)?;
    if input.summary.trim().is_empty()
        || input.detail.trim().is_empty()
        || !input.confidence.is_finite()
        || !(0.0..=1.0).contains(&input.confidence)
    {
        return Err(StoreError::InvalidDocument {
            kind: "experience index",
            message: "summary, detail, or confidence is invalid".to_owned(),
        });
    }
    let lock = experience_lock_path(&input.scope)?;
    store.with_exclusive_lock(&lock, || {
        let final_directory = final_dir(&input.scope)?;
        let final_index_path = index_path(&final_directory);
        let source_run_id = input
            .scope
            .source_run_id
            .as_deref()
            .expect("validated historical source run")
            .trim();
        if !store.exists(&final_index_path)? {
            create_index(
                store,
                CreateIndexInput {
                    scope: input.scope.clone(),
                    summary: input.summary.clone(),
                    confidence: input.confidence,
                    pattern_key: Some(input.pattern_key.clone()),
                    applies_to_phases: input.applies_to_phases.clone(),
                },
            )?;
            let detail = append_index_detail(
                store,
                AppendIndexDetailInput {
                    scope: input.scope.clone(),
                    section: DetailSection::HistoricalCase,
                    detail: input.detail.trim().to_owned(),
                    source_refs: normalized_source_refs(input.source_refs.clone())?,
                },
            )?;
            let index = finalize_index(store, &input.scope)?;
            return Ok(RecordExperienceCaseOutcome {
                index,
                detail,
                disposition: ExperienceCaseDisposition::Created,
                level: ExperienceLevel::RecentEpisode,
            });
        }

        let mut index: Index =
            store.read_versioned_json(&final_index_path, crate::FileSchemaKind::Index)?;
        validate_existing_experience(&index, &input.scope, &input.pattern_key)?;
        let details = read_all_completed_details(store, &input.scope)?;
        if let Some(detail) = details.iter().find(|detail| {
            detail.section == DetailSection::HistoricalCase && detail.source_run_id == source_run_id
        }) {
            if index.detail_count != details.len() {
                index.detail_count = details.len();
                index = store.write_authoritative_json(&final_index_path, index)?;
            }
            return Ok(RecordExperienceCaseOutcome {
                index,
                detail: detail.clone(),
                disposition: ExperienceCaseDisposition::DuplicateSourceRun,
                level: experience_level_from_details(&details),
            });
        }

        let source_refs = normalized_source_refs(input.source_refs.clone())?;
        let detail_text = input.detail.trim().to_owned();
        let detail = store.write_authoritative_json(
            &detail_path(
                &final_directory,
                &experience_case_detail_id(
                    &index.index_id,
                    source_run_id,
                    &detail_text,
                    &source_refs,
                ),
            )?,
            IndexDetail {
                schema_version: INDEX_DETAIL_SCHEMA_VERSION,
                detail_id: experience_case_detail_id(
                    &index.index_id,
                    source_run_id,
                    &detail_text,
                    &source_refs,
                ),
                index_id: index.index_id.clone(),
                section: DetailSection::HistoricalCase,
                detail: detail_text,
                source_run_id: source_run_id.to_owned(),
                source_refs,
                sort_order: details
                    .iter()
                    .map(|detail| detail.sort_order)
                    .max()
                    .unwrap_or(0)
                    + 1,
                created_at: input.scope.created_at.clone(),
                content_hash: String::new(),
            },
        )?;
        let all_details = read_all_completed_details(store, &input.scope)?;
        index.detail_count = all_details.len();
        let index = store.write_authoritative_json(&final_index_path, index)?;
        Ok(RecordExperienceCaseOutcome {
            index,
            detail,
            disposition: ExperienceCaseDisposition::Appended,
            level: experience_level_from_details(&all_details),
        })
    })
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
    if let Some(pattern_key) = input.pattern_key.as_deref() {
        validate_experience_identity(&input.scope, pattern_key, true)?;
    }
    // A finalized Index is authoritative over any stale Draft left behind by
    // a prior interrupted attempt.  Repeated create/finalize calls must
    // recover the canonical result rather than reopen or mutate that Draft.
    let final_directory = final_dir(&input.scope)?;
    let final_index_path = index_path(&final_directory);
    if store.exists(&final_index_path)? {
        let completed: Index =
            store.read_versioned_json(&final_index_path, crate::FileSchemaKind::Index)?;
        if completed.index_id != input.scope.index_id
            || completed.source_payload_hash != input.scope.source_payload_hash
        {
            return Err(StoreError::InvalidDocument {
                kind: "index",
                message: "completed Index identity differs from the requested scope".to_owned(),
            });
        }
        return Ok(completed);
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
    let lock = directory.join("details.lock");
    store.with_exclusive_lock(&lock, || {
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
        let source_run_id = input
            .scope
            .source_run_id
            .clone()
            .unwrap_or_else(|| input.scope.run_id.clone());
        let detail_id = content_hash_bytes(
            format!(
                "{}\x1f{}\x1f{}\x1f{}\x1f{}",
                index.index_id,
                input.section as u8,
                source_run_id,
                input.detail.trim(),
                source_refs.join("\x1e")
            )
            .as_bytes(),
        );
        let path = detail_path(&directory, &detail_id)?;
        if store.exists(&path)? {
            return store.read_versioned_json(&path, crate::FileSchemaKind::Detail);
        }
        let details_dir = directory.join("details");
        let sort_order = count_details(store, &details_dir)? + 1;
        store.write_authoritative_json(
            &path,
            IndexDetail {
                schema_version: INDEX_DETAIL_SCHEMA_VERSION,
                detail_id,
                index_id: index.index_id,
                section: input.section,
                detail: input.detail,
                source_run_id,
                source_refs,
                sort_order,
                created_at: input.scope.created_at,
                content_hash: String::new(),
            },
        )
    })
}

pub fn finalize_index(store: &FileStore, scope: &IndexScope) -> Result<Index> {
    let draft = draft_dir(scope)?;
    let final_dir = final_dir(scope)?;
    if store.exists(&index_path(&final_dir))? {
        let completed =
            store.read_versioned_json(&index_path(&final_dir), crate::FileSchemaKind::Index)?;
        validate_detail_layout(store, &final_dir, &completed)?;
        return Ok(completed);
    }
    let mut index: Index =
        store.read_versioned_json(&index_path(&draft), crate::FileSchemaKind::Index)?;
    validate_detail_layout(store, &draft, &index)?;
    let count = count_details(store, &draft.join("details"))?;
    index.detail_count = count;
    store.write_authoritative_json(&index_path(&draft), index.clone())?;
    rename_dir_atomic(store.root(), &draft, &final_dir)?;
    store.read_versioned_json(&index_path(&final_dir), crate::FileSchemaKind::Index)
}

fn validate_detail_layout(store: &FileStore, directory: &Path, index: &Index) -> Result<()> {
    let details_dir = directory.join("details");
    let count = count_details(store, &details_dir)?;
    if count == 0 {
        return Err(StoreError::InvalidDocument {
            kind: "index",
            message: "finalize requires at least one Detail".to_owned(),
        });
    }
    if index.detail_count != 0 && index.detail_count != count {
        return Err(StoreError::InvalidDocument {
            kind: "index",
            message: "Index detail_count does not match Detail files".to_owned(),
        });
    }
    let mut sort_orders = fs::read_dir(store.root().join(&details_dir))
        .map_err(|source| StoreError::Io {
            path: store.root().join(&details_dir),
            source,
        })?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json")
        })
        .map(|entry| {
            let relative = details_dir.join(entry.file_name());
            let detail: IndexDetail =
                store.read_versioned_json(&relative, crate::FileSchemaKind::Detail)?;
            if detail.index_id != index.index_id {
                return Err(StoreError::InvalidDocument {
                    kind: "index detail",
                    message: "detail index_id does not match its Index".to_owned(),
                });
            }
            Ok(detail.sort_order)
        })
        .collect::<Result<Vec<_>>>()?;
    sort_orders.sort_unstable();
    if sort_orders != (1..=count).collect::<Vec<_>>() {
        return Err(StoreError::InvalidDocument {
            kind: "index",
            message: "Detail sort_order values must be contiguous and unique".to_owned(),
        });
    }
    Ok(())
}

fn count_details(store: &FileStore, details_dir: &Path) -> Result<usize> {
    if !store.exists(details_dir)? {
        return Ok(0);
    }
    let count = fs::read_dir(store.root().join(details_dir))
        .map_err(|source| StoreError::Io {
            path: store.root().join(details_dir),
            source,
        })?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json")
        })
        .count();
    Ok(count)
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
    use serde_json::json;
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

    #[test]
    fn create_after_finalize_returns_the_canonical_index() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let scope = scope(RunLocation::new("2026-07-27", "run").unwrap());
        create_index(
            &store,
            CreateIndexInput {
                scope: scope.clone(),
                summary: "canonical summary".to_owned(),
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
                detail: "canonical evidence".to_owned(),
                source_refs: vec!["a".to_owned()],
            },
        )
        .unwrap();
        let completed = finalize_index(&store, &scope).unwrap();
        let recovered = create_index(
            &store,
            CreateIndexInput {
                scope,
                summary: "must not replace canonical summary".to_owned(),
                confidence: 0.1,
                pattern_key: None,
                applies_to_phases: vec![3],
            },
        )
        .unwrap();
        assert_eq!(recovered.index_id, completed.index_id);
        assert_eq!(recovered.summary, "canonical summary");
        assert_eq!(recovered.detail_count, 1);
    }

    #[test]
    fn detail_append_assigns_contiguous_sort_order_before_finalize() {
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
        for detail in ["first", "second", "third"] {
            append_index_detail(
                &store,
                AppendIndexDetailInput {
                    scope: scope.clone(),
                    section: DetailSection::Evidence,
                    detail: detail.to_owned(),
                    source_refs: vec![detail.to_owned()],
                },
            )
            .unwrap();
        }
        assert_eq!(finalize_index(&store, &scope).unwrap().detail_count, 3);
        assert_eq!(
            read_index_details(
                &store,
                &scope,
                &DetailQuery {
                    limit: 3,
                    ..Default::default()
                },
            )
            .unwrap()
            .details
            .iter()
            .map(|detail| detail.sort_order)
            .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    fn experience_scope(source_run_id: &str) -> IndexScope {
        let pattern_key = "volatility-breakout";
        IndexScope {
            kind: IndexKind::Experience,
            location: None,
            index_id: deterministic_experience_index_id(pattern_key, Some("QQQ"), 2).unwrap(),
            run_id: "reflection-run".to_owned(),
            source_run_id: Some(source_run_id.to_owned()),
            source_phase: 2,
            role: "historical_reflector".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            source_payload_hash: "outcome-source".to_owned(),
            authoritative_fields: Map::new(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        }
    }

    fn experience_input(source_run_id: &str, detail: &str) -> RecordExperienceCaseInput {
        RecordExperienceCaseInput {
            scope: experience_scope(source_run_id),
            pattern_key: "volatility-breakout".to_owned(),
            summary: "Reduce size when volatility invalidates the breakout.".to_owned(),
            confidence: 0.8,
            applies_to_phases: vec![3, 6],
            detail: detail.to_owned(),
            source_refs: vec![format!("outcome:{source_run_id}")],
        }
    }

    #[test]
    fn experience_appends_distinct_source_runs_and_derives_level() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let first =
            record_experience_case(&store, experience_input("source-1", "case one")).unwrap();
        assert_eq!(first.disposition, ExperienceCaseDisposition::Created);
        assert_eq!(first.level, ExperienceLevel::RecentEpisode);

        let second =
            record_experience_case(&store, experience_input("source-2", "case two")).unwrap();
        assert_eq!(second.disposition, ExperienceCaseDisposition::Appended);
        assert_eq!(second.level, ExperienceLevel::RepeatedWarning);
        assert_eq!(first.index.index_id, second.index.index_id);

        let third =
            record_experience_case(&store, experience_input("source-3", "case three")).unwrap();
        assert_eq!(third.level, ExperienceLevel::ActivePolicy);
        let completed = read_index_details(
            &store,
            &experience_scope("source-3"),
            &DetailQuery {
                limit: 20,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(completed.details.len(), 3);
        assert_eq!(
            experience_level(&store, &experience_scope("source-3")).unwrap(),
            ExperienceLevel::ActivePolicy
        );
        let index_json: Value = store
            .read_json(&index_path(
                &final_dir(&experience_scope("source-3")).unwrap(),
            ))
            .unwrap();
        assert!(index_json.get("experience_level").is_none());
    }

    #[test]
    fn experience_same_source_run_is_idempotent_without_overwrite() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let original =
            record_experience_case(&store, experience_input("source-1", "original case")).unwrap();
        let duplicate = record_experience_case(
            &store,
            experience_input("source-1", "new wording must not overwrite"),
        )
        .unwrap();
        assert_eq!(
            duplicate.disposition,
            ExperienceCaseDisposition::DuplicateSourceRun
        );
        assert_eq!(duplicate.detail.detail, "original case");
        assert_eq!(duplicate.detail.detail_id, original.detail.detail_id);
        assert_eq!(duplicate.index.detail_count, 1);
    }

    #[test]
    fn experience_rejects_non_deterministic_id_and_strict_unknown_fields() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let mut invalid = experience_input("source-1", "case");
        invalid.scope.index_id = "model-selected".to_owned();
        assert!(record_experience_case(&store, invalid).is_err());

        let output = record_experience_case(&store, experience_input("source-1", "case")).unwrap();
        let path = index_path(&final_dir(&experience_scope("source-1")).unwrap());
        let mut raw: Value = store.read_json(&path).unwrap();
        raw["unexpected"] = json!(true);
        store
            .write_json_value(&path, &crate::set_content_hash(&raw).unwrap())
            .unwrap();
        assert!(store
            .read_versioned_json::<Index>(&path, crate::FileSchemaKind::Index)
            .is_err());
        assert_eq!(output.level, ExperienceLevel::RecentEpisode);
    }
}
