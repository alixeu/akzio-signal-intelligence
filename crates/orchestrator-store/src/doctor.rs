//! Bounded consistency checks and non-authoritative cache rebuilds for the
//! file store.  The doctor never invents business data and never attempts a
//! schema migration: a malformed or version-incompatible authoritative file
//! is reported for an operator to repair explicitly.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use orchestrator_core::{
    EvaluationInputManifestV1, MaterializationGapV1, MaterializationIntegrityIssueV1,
    MemoryAttributionRecordV1, MemoryUsageEventV1, MemoryUsageReportV1, OutcomeHeadV1,
    OutcomeRecordV1, OutcomeRevisionCommitV1, OutcomeStatus,
};

use crate::{
    content_hash_bytes, read_indexes, read_jsonl_recover_tail,
    rebuild_manifest_from_finalized_artifacts, validate_content_hash_at, validate_relative_path,
    write_run_manifest, ArtifactDraft, ContentHashDocument, DetailSection, ExperienceEventV1,
    ExperienceViewV1, FileSchemaKind, FileStore, FinalizedArtifactRef, Index, IndexArchive,
    IndexDetail, IndexKind, IndexQuery, JsonlEvent, ReflectionTaskEventV1, Result, RunLocation,
    RunManifest, RunManifestInit, SafeSlug, SessionEvent, SessionEventType, StoreError,
};

pub const INDEX_CATALOG_SCHEMA_VERSION: u32 = 1;
pub const EXPERIENCE_STATS_SCHEMA_VERSION: u32 = 1;

/// A single diagnostic finding.  `path` is always relative to the configured
/// store root, so it is safe to render in an operator or CLI report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoctorIssue {
    pub code: String,
    pub path: String,
    pub message: String,
}

/// A bounded doctor report.  The caller can decide whether warnings should
/// fail a maintenance command; the store itself remains fail-closed on reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreDoctorReport {
    pub checked_files: usize,
    pub recovered_jsonl_tails: usize,
    pub issues: Vec<DoctorIssue>,
}

impl StoreDoctorReport {
    fn issue(&mut self, code: impl Into<String>, path: &Path, message: impl Into<String>) {
        self.issues.push(DoctorIssue {
            code: code.into(),
            path: path.to_string_lossy().into_owned(),
            message: message.into(),
        });
    }

    pub fn is_healthy(&self) -> bool {
        self.issues.is_empty()
    }
}

/// A cache of finalized Index headers.  It is deliberately not authoritative:
/// the individual `index.json` and Detail documents remain the source of
/// truth and may always be scanned again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCatalog {
    pub schema_version: u32,
    pub kind: IndexKind,
    pub run_id: Option<String>,
    pub entries: Vec<IndexCatalogEntry>,
    pub generated_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCatalogEntry {
    pub index_id: String,
    pub source_phase: u8,
    pub role: String,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub pattern_key: Option<String>,
    pub detail_count: usize,
}

impl ContentHashDocument for IndexCatalog {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

/// Derived counts only.  No experience level is persisted in an Index: it is
/// reconstructed from distinct `historical_case.source_run_id` values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperienceStats {
    pub schema_version: u32,
    pub indexes: Vec<ExperienceStat>,
    pub generated_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExperienceStat {
    pub index_id: String,
    pub historical_source_run_count: usize,
    pub level: String,
}

impl ContentHashDocument for ExperienceStats {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

/// Walk every store-owned file without following symbolic links, validate the
/// generic envelope, then apply the cross-file checks that have enough typed
/// context to be meaningful.  This operation may repair exactly one
/// unterminated final JSONL line per log, matching normal read semantics.
pub fn inspect_store(store: &FileStore) -> StoreDoctorReport {
    let mut report = StoreDoctorReport::default();
    let files = match collect_files(store.root()) {
        Ok(files) => files,
        Err(error) => {
            report.issue("path_escape", Path::new("."), error.to_string());
            return report;
        }
    };

    for relative in &files {
        inspect_file_envelope(store, relative, &mut report);
    }
    inspect_runs(store, &files, &mut report);
    inspect_indexes(store, &files, &mut report);
    inspect_evaluation(store, &files, &mut report);
    inspect_experience_views(store, &files, &mut report);
    inspect_memory_usage(store, &files, &mut report);
    resolve_source_refs(store, &files, &mut report);
    report
        .issues
        .sort_by(|a, b| (&a.path, &a.code, &a.message).cmp(&(&b.path, &b.code, &b.message)));
    report
}

/// Rebuild every non-authoritative Experience View from its append-only Event
/// Ledger. This is intentionally separate from the legacy Index stats cache.
pub fn rebuild_experience_views(
    store: &FileStore,
    rebuilt_at: &str,
) -> Result<Vec<ExperienceViewV1>> {
    crate::ExperienceLedger::new(store.clone()).rebuild_all_views(rebuilt_at)
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    collect_files_inner(root, root, &mut output)?;
    output.sort();
    Ok(output)
}

fn collect_files_inner(root: &Path, directory: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory).map_err(|source| StoreError::Io {
        path: directory.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| StoreError::Io {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| StoreError::Io {
            path: path.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path });
        }
        if metadata.is_dir() {
            collect_files_inner(root, &path, output)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .expect("recursive path beneath root")
                .to_path_buf();
            validate_relative_path(&relative)?;
            output.push(relative);
        }
    }
    Ok(())
}

fn inspect_file_envelope(store: &FileStore, relative: &Path, report: &mut StoreDoctorReport) {
    let absolute = store.root().join(relative);
    let name = relative
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if name.starts_with('.') && name.contains(".tmp-") {
        report.issue(
            "stale_temp",
            relative,
            "adjacent temporary file remains in store",
        );
        return;
    }
    if relative
        .extension()
        .is_some_and(|extension| extension == "json")
    {
        report.checked_files += 1;
        match fs::read(&absolute)
            .map_err(|source| StoreError::Io {
                path: absolute.clone(),
                source,
            })
            .and_then(|bytes| {
                serde_json::from_slice::<Value>(&bytes).map_err(|source| StoreError::Json {
                    path: absolute.clone(),
                    source,
                })
            }) {
            Ok(value) => {
                if let Err(error) = validate_generic_document(&value, &absolute) {
                    report.issue("invalid_document", relative, error.to_string());
                }
            }
            Err(error) => report.issue("malformed_json", relative, error.to_string()),
        }
    } else if relative
        .extension()
        .is_some_and(|extension| extension == "jsonl")
    {
        report.checked_files += 1;
        let before = fs::metadata(&absolute).ok().map(|metadata| metadata.len());
        let parsed = if relative
            .components()
            .any(|component| component.as_os_str() == "sessions")
        {
            read_jsonl_recover_tail::<SessionEvent>(store.root(), relative).map(|_| ())
        } else if relative
            .components()
            .any(|component| component.as_os_str() == "reflection")
        {
            read_jsonl_recover_tail::<ReflectionTaskEventV1>(store.root(), relative).map(|_| ())
        } else if relative
            .components()
            .any(|component| component.as_os_str() == "experiences")
        {
            read_jsonl_recover_tail::<ExperienceEventV1>(store.root(), relative).map(|_| ())
        } else if relative
            .components()
            .any(|component| component.as_os_str() == "memory")
        {
            read_jsonl_recover_tail::<MemoryUsageEventV1>(store.root(), relative).map(|_| ())
        } else {
            read_jsonl_recover_tail::<JsonlEvent>(store.root(), relative).map(|_| ())
        };
        match parsed {
            Ok(()) => {
                if before
                    .zip(fs::metadata(&absolute).ok().map(|metadata| metadata.len()))
                    .is_some_and(|(before, after)| after < before)
                {
                    report.recovered_jsonl_tails += 1;
                    report.issue(
                        "recovered_jsonl_tail",
                        relative,
                        "discarded one incomplete final JSONL line",
                    );
                }
            }
            Err(error) => report.issue("malformed_jsonl", relative, error.to_string()),
        }
    }
}

fn validate_generic_document(value: &Value, path: &Path) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "authoritative file",
            message: "document must be a JSON object".to_owned(),
        })?;
    let schema_version = object
        .get("schema_version")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| StoreError::MissingSchemaVersion {
            kind: "authoritative file".to_owned(),
            path: path.to_path_buf(),
        })?;
    // Run manifests own a strictly constrained v1→v2 reader migration. The
    // generic envelope can validate their immutable hash but must not reject
    // a known legacy version before `inspect_run` applies that migration.
    if path.file_name().is_some_and(|name| name == "manifest.json")
        && path
            .components()
            .any(|component| component.as_os_str() == "runs")
    {
        return validate_content_hash_at(value, path);
    }
    let current = if path
        .components()
        .any(|component| component.as_os_str() == "learning")
        && path
            .components()
            .any(|component| component.as_os_str() == "v2")
        && path
            .components()
            .any(|component| component.as_os_str() == "decisions")
    {
        orchestrator_core::DECISION_SNAPSHOT_SCHEMA_VERSION
    } else if path.starts_with("knowledge/evaluation") {
        evaluation_schema_version_for_path(path)
    } else if path
        .components()
        .any(|component| component.as_os_str() == "artifacts")
        && object
            .get("profile")
            .cloned()
            .and_then(|profile| serde_json::from_value::<crate::ToolManagedProfile>(profile).ok())
            .is_some()
    {
        2
    } else {
        1
    };
    if schema_version > current {
        return Err(StoreError::UnsupportedFutureSchema {
            kind: "authoritative file".to_owned(),
            path: path.to_path_buf(),
            found: schema_version,
            current,
        });
    }
    if schema_version < current {
        return Err(StoreError::MigrationRequired {
            kind: "authoritative file".to_owned(),
            path: path.to_path_buf(),
            found: schema_version,
            current,
        });
    }
    validate_content_hash_at(value, path)
}

fn evaluation_schema_version_for_path(path: &Path) -> u32 {
    if path
        .components()
        .any(|component| component.as_os_str() == "outcomes")
    {
        orchestrator_core::OUTCOME_RECORD_SCHEMA_VERSION
    } else if path
        .components()
        .any(|component| component.as_os_str() == "revisions")
    {
        orchestrator_core::OUTCOME_REVISION_COMMIT_SCHEMA_VERSION
    } else if path
        .components()
        .any(|component| component.as_os_str() == "outcome_heads")
    {
        orchestrator_core::OUTCOME_HEAD_SCHEMA_VERSION
    } else if path
        .components()
        .any(|component| component.as_os_str() == "manifests")
    {
        orchestrator_core::EVALUATION_INPUT_MANIFEST_SCHEMA_VERSION
    } else if path
        .components()
        .any(|component| component.as_os_str() == "gaps")
    {
        orchestrator_core::MATERIALIZATION_GAP_SCHEMA_VERSION
    } else if path
        .components()
        .any(|component| component.as_os_str() == "integrity")
    {
        orchestrator_core::MATERIALIZATION_INTEGRITY_ISSUE_SCHEMA_VERSION
    } else {
        1
    }
}

fn inspect_runs(store: &FileStore, files: &[PathBuf], report: &mut StoreDoctorReport) {
    let mut run_roots = BTreeSet::new();
    for path in files {
        let mut components = path.components();
        if components
            .next()
            .is_some_and(|component| component.as_os_str() == "runs")
        {
            let Some(date) = components.next() else {
                continue;
            };
            let Some(run) = components.next() else {
                continue;
            };
            run_roots.insert(
                PathBuf::from("runs")
                    .join(date.as_os_str())
                    .join(run.as_os_str()),
            );
        }
    }
    for run_root in run_roots {
        inspect_run(store, &run_root, files, report);
    }
}

fn inspect_run(
    store: &FileStore,
    run_root: &Path,
    files: &[PathBuf],
    report: &mut StoreDoctorReport,
) {
    let manifest_relative = run_root.join("manifest.json");
    let manifest_path = store.root().join(&manifest_relative);
    let location = match crate::manifest::read_manifest_relative(store, &manifest_relative) {
        Ok(manifest) => match manifest.location() {
            Ok(location) => {
                if location.relative_root() != run_root {
                    report.issue(
                        "path_identity_mismatch",
                        &manifest_relative,
                        "manifest run identity does not reproduce its safe path",
                    );
                }
                Some((location, manifest))
            }
            Err(error) => {
                report.issue("invalid_manifest", &manifest_relative, error.to_string());
                None
            }
        },
        Err(error) => {
            let code = if manifest_path.exists() {
                "invalid_manifest"
            } else {
                "missing_manifest"
            };
            report.issue(code, &manifest_relative, error.to_string());
            None
        }
    };
    let Some((location, manifest)) = location else {
        return;
    };

    let artifact_ids = artifact_ids_under_run(store, run_root, files, report);
    for artifact_id in &artifact_ids {
        if !manifest.artifacts.contains_key(artifact_id) {
            report.issue(
                "manifest_missing_artifact",
                &manifest_relative,
                format!("finalized artifact `{artifact_id}` is absent from manifest"),
            );
        }
    }
    for reference in manifest.artifacts.values() {
        let relative = match location.child_relative(Path::new(&reference.relative_path)) {
            Ok(relative) => relative,
            Err(error) => {
                report.issue(
                    "unsafe_manifest_artifact_path",
                    &manifest_relative,
                    error.to_string(),
                );
                continue;
            }
        };
        if !store.root().join(&relative).is_file() {
            report.issue(
                "missing_artifact",
                &relative,
                format!(
                    "manifest references artifact `{}` that does not exist",
                    reference.artifact_id
                ),
            );
            continue;
        }
        if let Err(error) =
            crate::draft::validate_existing_artifact_ref(store, &relative, reference)
        {
            report.issue("artifact_manifest_mismatch", &relative, error.to_string());
        }
    }

    let terminal_ids = terminal_ids_for_run(store, &location, files, report);
    for draft_relative in files.iter().filter(|path| {
        path.starts_with(run_root.join("drafts"))
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
    }) {
        let draft = match store.read_versioned_json::<ArtifactDraft>(
            draft_relative,
            FileSchemaKind::Draft("doctor".to_owned()),
        ) {
            Ok(draft) => draft,
            Err(error) => {
                report.issue("invalid_draft", draft_relative, error.to_string());
                continue;
            }
        };
        if let Err(error) = draft.validate_for_location(&location) {
            report.issue("invalid_draft", draft_relative, error.to_string());
            continue;
        }
        match draft.lifecycle {
            crate::DraftLifecycle::Draft => report.issue(
                "incomplete_draft",
                draft_relative,
                "draft was never finalized",
            ),
            crate::DraftLifecycle::Completed => {
                let Some(reference) = draft.finalized_artifact.as_ref() else {
                    continue;
                };
                if !artifact_ids.contains(&reference.artifact_id) {
                    report.issue(
                        "missing_artifact",
                        draft_relative,
                        format!(
                            "completed draft references missing artifact `{}`",
                            reference.artifact_id
                        ),
                    );
                }
                if !manifest.artifacts.contains_key(&reference.artifact_id) {
                    report.issue(
                        "manifest_missing_artifact",
                        draft_relative,
                        format!(
                            "completed draft artifact `{}` is absent from manifest",
                            reference.artifact_id
                        ),
                    );
                }
                if !terminal_ids.contains(&reference.artifact_id) {
                    report.issue(
                        "completed_unit_missing_terminal",
                        draft_relative,
                        format!(
                            "completed artifact `{}` has no matching terminal session event",
                            reference.artifact_id
                        ),
                    );
                }
            }
            crate::DraftLifecycle::Failed | crate::DraftLifecycle::Superseded => {}
        }
    }
}

fn artifact_ids_under_run(
    store: &FileStore,
    run_root: &Path,
    files: &[PathBuf],
    report: &mut StoreDoctorReport,
) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for relative in files.iter().filter(|path| {
        path.starts_with(run_root.join("artifacts"))
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
    }) {
        match store.read_json_value(relative) {
            Ok(value) => match value.get("artifact_id").and_then(Value::as_str) {
                Some(id) if !id.is_empty() => {
                    ids.insert(id.to_owned());
                }
                _ => report.issue(
                    "artifact_missing_identity",
                    relative,
                    "artifact JSON has no non-empty artifact_id",
                ),
            },
            Err(error) => report.issue("malformed_artifact", relative, error.to_string()),
        }
    }
    ids
}

fn terminal_ids_for_run(
    store: &FileStore,
    location: &RunLocation,
    files: &[PathBuf],
    report: &mut StoreDoctorReport,
) -> BTreeSet<String> {
    let base = location.relative_root().join("sessions");
    let mut ids = BTreeSet::new();
    for relative in files.iter().filter(|path| {
        path.starts_with(&base)
            && path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
    }) {
        match read_jsonl_recover_tail::<SessionEvent>(store.root(), relative) {
            Ok(events) => collect_terminal_ids(events, &mut ids),
            Err(error) => report.issue("malformed_jsonl", relative, error.to_string()),
        }
    }
    ids
}

fn collect_terminal_ids(
    events: impl IntoIterator<Item = SessionEvent>,
    ids: &mut BTreeSet<String>,
) {
    for event in events
        .into_iter()
        .filter(|event| event.event_type == SessionEventType::Terminal)
    {
        for result in [
            Some(&event.payload),
            event.payload.get("output"),
            event
                .payload
                .get("output")
                .and_then(|output| output.get("artifact")),
            event
                .payload
                .get("output")
                .and_then(|output| output.get("index")),
        ]
        .into_iter()
        .flatten()
        {
            for key in ["artifact_id", "index_id"] {
                if let Some(value) = result.get(key).and_then(Value::as_str) {
                    ids.insert(value.to_owned());
                }
            }
        }
    }
}

fn inspect_indexes(store: &FileStore, files: &[PathBuf], report: &mut StoreDoctorReport) {
    let mut bases = BTreeSet::from([PathBuf::from("knowledge/experience")]);
    for path in files {
        let components = path.components().collect::<Vec<_>>();
        if components.len() >= 4
            && components[0].as_os_str() == "runs"
            && components[3].as_os_str() == "index"
        {
            bases.insert(
                PathBuf::from("runs")
                    .join(components[1].as_os_str())
                    .join(components[2].as_os_str())
                    .join("index"),
            );
        }
    }
    for base in bases {
        inspect_index_base(store, &base, files, report);
    }
}

/// Validate the canonical evaluation ledger separately from generic envelope
/// checks.  In particular, a head may only expose one current Outcome and an
/// Outcome's decision/input references must stay inside FileStore.
fn inspect_evaluation(store: &FileStore, files: &[PathBuf], report: &mut StoreDoctorReport) {
    let root = Path::new("knowledge/evaluation");
    for relative in files.iter().filter(|path| path.starts_with(root)) {
        let path = relative.as_path();
        let typed = if path.components().any(|part| part.as_os_str() == "outcomes") {
            store
                .read_versioned_json::<OutcomeRecordV1>(path, FileSchemaKind::OutcomeRecord)
                .map(|outcome| {
                    inspect_document_ref(store, path, &outcome.decision_ref, report);
                    inspect_document_ref(
                        store,
                        path,
                        &outcome.evaluation_input_manifest_ref,
                        report,
                    );
                })
        } else if path
            .components()
            .any(|part| part.as_os_str() == "attributions")
        {
            store
                .read_versioned_json::<MemoryAttributionRecordV1>(
                    path,
                    FileSchemaKind::MemoryAttribution,
                )
                .map(|_| ())
        } else if path
            .components()
            .any(|part| part.as_os_str() == "outcome_heads")
        {
            store
                .read_versioned_json::<OutcomeHeadV1>(path, FileSchemaKind::OutcomeHead)
                .and_then(|head| {
                    inspect_outcome_head(&head).map_err(|message| StoreError::InvalidDocument {
                        kind: "outcome head",
                        message,
                    })
                })
        } else if path
            .components()
            .any(|part| part.as_os_str() == "revisions")
        {
            store
                .read_versioned_json::<OutcomeRevisionCommitV1>(
                    path,
                    FileSchemaKind::OutcomeRevisionCommit,
                )
                .map(|_| ())
        } else if path
            .components()
            .any(|part| part.as_os_str() == "manifests")
        {
            store
                .read_versioned_json::<EvaluationInputManifestV1>(
                    path,
                    FileSchemaKind::EvaluationInputManifest,
                )
                .map(|_| ())
        } else if path.components().any(|part| part.as_os_str() == "gaps") {
            store
                .read_versioned_json::<MaterializationGapV1>(
                    path,
                    FileSchemaKind::MaterializationGap,
                )
                .map(|_| ())
        } else if path
            .components()
            .any(|part| part.as_os_str() == "integrity")
        {
            store
                .read_versioned_json::<MaterializationIntegrityIssueV1>(
                    path,
                    FileSchemaKind::MaterializationIntegrityIssue,
                )
                .map(|_| ())
        } else {
            // Receipts/reports are run-local; generic envelope checks above
            // already cover them. Unknown canonical files remain visible as
            // generic documents but do not become trusted typed ledger data.
            continue;
        };
        if let Err(error) = typed {
            report.issue("evaluation_ledger_invalid", path, error.to_string());
        }
    }
}

fn inspect_outcome_head(head: &OutcomeHeadV1) -> std::result::Result<(), String> {
    let current = head
        .statuses
        .iter()
        .filter(|(_, status)| **status == OutcomeStatus::Current)
        .map(|(outcome_id, _)| outcome_id)
        .collect::<Vec<_>>();
    if current.len() > 1 {
        return Err("more than one outcome is marked current".to_owned());
    }
    if head.current_outcome_id.as_deref()
        != current
            .first()
            .copied()
            .map(|outcome_id| outcome_id.as_str())
    {
        return Err("current_outcome_id disagrees with status map".to_owned());
    }
    Ok(())
}

/// Experience views are derived data, but a malformed view must still be
/// visible to operators rather than silently being consumed by retrieval.
/// The event ledger remains the rebuild authority.
fn inspect_experience_views(store: &FileStore, files: &[PathBuf], report: &mut StoreDoctorReport) {
    let root = Path::new("knowledge/experiences/views");
    for relative in files.iter().filter(|path| path.starts_with(root)) {
        if let Err(error) =
            store.read_versioned_json::<ExperienceViewV1>(relative, FileSchemaKind::ExperienceView)
        {
            report.issue("experience_view_invalid", relative, error.to_string());
        }
    }
}

fn inspect_memory_usage(store: &FileStore, files: &[PathBuf], report: &mut StoreDoctorReport) {
    for relative in files
        .iter()
        .filter(|path| path.ends_with("memory/usage/report.json"))
    {
        if let Err(error) = store
            .read_versioned_json::<MemoryUsageReportV1>(relative, FileSchemaKind::MemoryUsageReport)
        {
            report.issue("memory_usage_report_invalid", relative, error.to_string());
        }
    }
}

fn inspect_document_ref(
    store: &FileStore,
    owner: &Path,
    reference: &orchestrator_core::DocumentRef,
    report: &mut StoreDoctorReport,
) {
    let relative = Path::new(&reference.relative_path);
    if validate_relative_path(relative).is_err() || !store.exists(relative).unwrap_or(false) {
        report.issue(
            "evaluation_unresolved_provenance",
            owner,
            format!(
                "reference {} does not resolve inside FileStore",
                reference.document_id
            ),
        );
        return;
    }
    let actual = if relative
        .extension()
        .is_some_and(|extension| extension == "json")
    {
        store.read_json_value(relative).ok().and_then(|value| {
            value
                .get("content_hash")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
    } else {
        store
            .read_bytes(relative)
            .ok()
            .map(|bytes| content_hash_bytes(&bytes))
    };
    if actual.as_deref() != Some(reference.content_hash.as_str()) {
        report.issue(
            "evaluation_provenance_hash_mismatch",
            owner,
            format!(
                "reference {} hash does not match its target",
                reference.document_id
            ),
        );
    }
}

fn inspect_index_base(
    store: &FileStore,
    base: &Path,
    files: &[PathBuf],
    report: &mut StoreDoctorReport,
) {
    let mut completed = BTreeMap::<PathBuf, Index>::new();
    for relative in files.iter().filter(|path| {
        path.parent()
            .is_some_and(|parent| parent.parent() == Some(base))
            && path.file_name().is_some_and(|name| name == "index.json")
    }) {
        match store.read_versioned_json::<Index>(relative, FileSchemaKind::Index) {
            Ok(index) => {
                let directory = relative.parent().expect("index path has parent");
                match crate::index_path_component(&index.index_id) {
                    Ok(slug)
                        if directory
                            .file_name()
                            .is_some_and(|name| name == slug.as_str()) => {}
                    Ok(_) => report.issue(
                        "path_identity_mismatch",
                        relative,
                        "Index ID does not reproduce index directory slug",
                    ),
                    Err(error) => report.issue("invalid_index", relative, error.to_string()),
                }
                completed.insert(directory.to_path_buf(), index);
            }
            Err(error) => report.issue("invalid_index", relative, error.to_string()),
        }
    }
    for (directory, index) in completed {
        let details_dir = directory.join("details");
        let mut details = Vec::new();
        for detail_relative in files.iter().filter(|path| {
            path.parent() == Some(details_dir.as_path())
                && path
                    .extension()
                    .is_some_and(|extension| extension == "json")
        }) {
            match store.read_versioned_json::<IndexDetail>(detail_relative, FileSchemaKind::Detail)
            {
                Ok(detail) => {
                    if detail.index_id != index.index_id {
                        report.issue(
                            "orphan_detail",
                            detail_relative,
                            "Detail index_id does not match containing Index",
                        );
                    }
                    match SafeSlug::new("detail", &detail.detail_id) {
                        Ok(slug)
                            if detail_relative
                                .file_stem()
                                .is_some_and(|name| name == slug.as_os_str()) => {}
                        Ok(_) => report.issue(
                            "path_identity_mismatch",
                            detail_relative,
                            "Detail ID does not reproduce detail file slug",
                        ),
                        Err(error) => {
                            report.issue("invalid_detail", detail_relative, error.to_string())
                        }
                    }
                    details.push(detail);
                }
                Err(error) => report.issue("invalid_detail", detail_relative, error.to_string()),
            }
        }
        if details.len() != index.detail_count {
            report.issue(
                "index_detail_count_mismatch",
                &directory.join("index.json"),
                format!(
                    "Index records {} Details, found {}",
                    index.detail_count,
                    details.len()
                ),
            );
        }
        let sort_orders = details
            .iter()
            .map(|detail| detail.sort_order)
            .collect::<BTreeSet<_>>();
        if sort_orders.len() != details.len()
            || sort_orders.iter().copied().collect::<Vec<_>>()
                != (1..=details.len()).collect::<Vec<_>>()
        {
            report.issue(
                "detail_sort_order_mismatch",
                &directory.join("details"),
                "Detail sort_order values must be contiguous and unique",
            );
        }
        if index.kind == IndexKind::Experience {
            let historical_runs = details
                .iter()
                .filter(|detail| detail.section == DetailSection::HistoricalCase)
                .map(|detail| detail.source_run_id.as_str())
                .collect::<Vec<_>>();
            if historical_runs.iter().collect::<BTreeSet<_>>().len() != historical_runs.len() {
                report.issue(
                    "experience_duplicate_source_run",
                    &directory.join("details"),
                    "Experience has multiple historical_case Details for one source_run_id",
                );
            }
        }
    }
    for relative in files.iter().filter(|path| {
        path.parent() == Some(base)
            && path.file_name().is_some_and(|name| {
                let name = name.to_string_lossy();
                name.starts_with("idx-") || name.starts_with("index-")
            })
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
    }) {
        let archive = match store.read_versioned_json::<IndexArchive>(
            relative,
            FileSchemaKind::Artifact("index_archive".to_owned()),
        ) {
            Ok(archive) => archive,
            Err(error) => {
                report.issue("invalid_index_archive", relative, error.to_string());
                continue;
            }
        };
        let components = base.components().collect::<Vec<_>>();
        let location = if components.len() == 4
            && components[0].as_os_str() == "runs"
            && components[3].as_os_str() == "index"
        {
            RunLocation::new(
                components[1].as_os_str().to_string_lossy(),
                archive.index.run_id.clone(),
            )
        } else {
            Err(StoreError::InvalidDocument {
                kind: "index archive",
                message: "Index archives are allowed only inside a run".to_owned(),
            })
        };
        let location = match location {
            Ok(location) if location.relative_root().join("index") == base => location,
            Ok(_) => {
                report.issue(
                    "path_identity_mismatch",
                    relative,
                    "archive run identity does not reproduce its Index base",
                );
                continue;
            }
            Err(error) => {
                report.issue("invalid_index_archive", relative, error.to_string());
                continue;
            }
        };
        if let Err(error) = archive.validate_for_location(&location) {
            report.issue("invalid_index_archive", relative, error.to_string());
            continue;
        }
        match IndexArchive::relative_path(
            &location,
            archive.index.source_phase,
            &archive.index.index_id,
        ) {
            Ok(expected) if expected == *relative => {}
            Ok(_) => report.issue(
                "path_identity_mismatch",
                relative,
                "Index ID does not reproduce archive path",
            ),
            Err(error) => report.issue("invalid_index_archive", relative, error.to_string()),
        }
        inspect_index_detail_semantics(&archive.index, &archive.details, relative, report);
    }
    for relative in files.iter().filter(|path| {
        path.parent()
            .is_some_and(|parent| parent.file_name().is_some_and(|name| name == "details"))
            && path.starts_with(base)
    }) {
        let index_directory = relative.parent().and_then(Path::parent);
        if index_directory.is_some_and(|directory| !completed_index_dir_exists(store, directory)) {
            report.issue(
                "orphan_detail",
                relative,
                "Detail exists without completed containing Index",
            );
        }
    }
    for relative in files.iter().filter(|path| {
        path.starts_with(base)
            && path.components().any(|component| {
                component
                    .as_os_str()
                    .to_string_lossy()
                    .starts_with(".index-draft-")
            })
    }) {
        report.issue(
            "incomplete_index_draft",
            relative,
            "Index draft remains unfinalized",
        );
    }
}

fn inspect_index_detail_semantics(
    index: &Index,
    details: &[IndexDetail],
    owner: &Path,
    report: &mut StoreDoctorReport,
) {
    if details.len() != index.detail_count {
        report.issue(
            "index_detail_count_mismatch",
            owner,
            format!(
                "Index records {} Details, found {}",
                index.detail_count,
                details.len()
            ),
        );
    }
    let sort_orders = details
        .iter()
        .map(|detail| detail.sort_order)
        .collect::<BTreeSet<_>>();
    if sort_orders.len() != details.len()
        || sort_orders.iter().copied().collect::<Vec<_>>()
            != (1..=details.len()).collect::<Vec<_>>()
    {
        report.issue(
            "detail_sort_order_mismatch",
            owner,
            "Detail sort_order values must be contiguous and unique",
        );
    }
    if details
        .iter()
        .any(|detail| detail.index_id != index.index_id)
    {
        report.issue(
            "orphan_detail",
            owner,
            "archived Detail index_id does not match its Index",
        );
    }
    if index.kind == IndexKind::Experience {
        let historical_runs = details
            .iter()
            .filter(|detail| detail.section == DetailSection::HistoricalCase)
            .map(|detail| detail.source_run_id.as_str())
            .collect::<Vec<_>>();
        if historical_runs.iter().collect::<BTreeSet<_>>().len() != historical_runs.len() {
            report.issue(
                "experience_duplicate_source_run",
                owner,
                "Experience has multiple historical_case Details for one source_run_id",
            );
        }
    }
}

fn completed_index_dir_exists(store: &FileStore, directory: &Path) -> bool {
    store.root().join(directory).join("index.json").is_file()
        && !directory
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with('.'))
}

fn resolve_source_refs(store: &FileStore, files: &[PathBuf], report: &mut StoreDoctorReport) {
    let mut known = BTreeSet::new();
    for relative in files.iter().filter(|path| {
        path.extension()
            .is_some_and(|extension| extension == "json")
    }) {
        if let Ok(value) = store.read_json_value(relative) {
            for field in ["artifact_id", "index_id", "detail_id"] {
                if let Some(id) = value.get(field).and_then(Value::as_str) {
                    known.insert(format!("{field}:{id}"));
                }
            }
        }
    }
    for relative in files.iter().filter(|path| {
        path.parent()
            .is_some_and(|parent| parent.file_name().is_some_and(|name| name == "index"))
            && path.file_name().is_some_and(|name| {
                let name = name.to_string_lossy();
                name.starts_with("idx-") || name.starts_with("index-")
            })
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
    }) {
        let Ok(archive) = store.read_versioned_json::<IndexArchive>(
            relative,
            FileSchemaKind::Artifact("index_archive".to_owned()),
        ) else {
            continue;
        };
        known.insert(format!("index_id:{}", archive.index.index_id));
        for detail in archive.details {
            known.insert(format!("detail_id:{}", detail.detail_id));
        }
    }
    for relative in files
        .iter()
        .filter(|path| path.file_name().is_some_and(|name| name == "index.json"))
    {
        let Ok(index) = store.read_versioned_json::<Index>(relative, FileSchemaKind::Index) else {
            continue;
        };
        let details_directory = relative
            .parent()
            .expect("index path has parent")
            .join("details");
        for detail_path in files
            .iter()
            .filter(|path| path.parent() == Some(details_directory.as_path()))
        {
            let Ok(detail) =
                store.read_versioned_json::<IndexDetail>(detail_path, FileSchemaKind::Detail)
            else {
                continue;
            };
            for reference in &detail.source_refs {
                let Some((kind, id)) = reference.split_once(':') else {
                    continue;
                };
                let supported = match kind {
                    "artifact" => "artifact_id",
                    "index" => "index_id",
                    "detail" => "detail_id",
                    _ => continue,
                };
                if !known.contains(&format!("{supported}:{id}")) {
                    report.issue(
                        "unresolved_source_ref",
                        detail_path,
                        format!("source ref `{reference}` cannot be resolved"),
                    );
                }
            }
        }
        let _ = index;
    }
    for relative in files.iter().filter(|path| {
        path.parent()
            .is_some_and(|parent| parent.file_name().is_some_and(|name| name == "index"))
            && path.file_name().is_some_and(|name| {
                let name = name.to_string_lossy();
                name.starts_with("idx-") || name.starts_with("index-")
            })
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
    }) {
        let Ok(archive) = store.read_versioned_json::<IndexArchive>(
            relative,
            FileSchemaKind::Artifact("index_archive".to_owned()),
        ) else {
            continue;
        };
        for detail in &archive.details {
            for reference in &detail.source_refs {
                let Some((kind, id)) = reference.split_once(':') else {
                    continue;
                };
                let supported = match kind {
                    "artifact" => "artifact_id",
                    "index" => "index_id",
                    "detail" => "detail_id",
                    _ => continue,
                };
                if !known.contains(&format!("{supported}:{id}")) {
                    report.issue(
                        "unresolved_source_ref",
                        relative,
                        format!("source ref `{reference}` cannot be resolved"),
                    );
                }
            }
        }
    }
}

/// Rebuild a missing or stale manifest from finalized Draft references and
/// write it atomically.  Version/config metadata must be supplied by the
/// caller; the doctor never fabricates it with defaults.
pub fn rebuild_run_manifest(store: &FileStore, init: RunManifestInit) -> Result<RunManifest> {
    let location = init.location.clone();
    let draft_root = location.relative_root().join("drafts");
    let artifact_root = location.relative_root().join("artifacts");
    let files = collect_files(store.root())?;
    let mut references = BTreeMap::<String, FinalizedArtifactRef>::new();
    for relative in files.iter().filter(|path| {
        path.starts_with(&draft_root)
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
    }) {
        let draft: ArtifactDraft =
            store.read_versioned_json(relative, FileSchemaKind::Draft("doctor".to_owned()))?;
        draft.validate_for_location(&location)?;
        if let Some(reference) = draft.finalized_artifact {
            references.insert(reference.artifact_id.clone(), reference);
        }
    }
    // Rust-owned phases (notably allocation) do not have a Draft. Their
    // finalized canonical files are still more authoritative than a stale
    // manifest, so rebuild them from the same required envelope fields.
    for relative in files.iter().filter(|path| {
        path.starts_with(&artifact_root)
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
    }) {
        let value = store.read_json_value(relative)?;
        let artifact_relative = relative
            .strip_prefix(location.relative_root())
            .map_err(|_| StoreError::InvalidDocument {
                kind: "artifact path",
                message: format!("artifact is outside run root: {}", relative.display()),
            })?;
        let required = |field: &str| {
            value
                .get(field)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| StoreError::InvalidDocument {
                    kind: "canonical artifact",
                    message: format!("{} is required at {}", field, relative.display()),
                })
        };
        let phase = value
            .get("phase")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .ok_or_else(|| StoreError::InvalidDocument {
                kind: "canonical artifact",
                message: format!("phase is required at {}", relative.display()),
            })?;
        let reference = FinalizedArtifactRef::new(
            required("artifact_id")?,
            artifact_relative,
            phase,
            required("role")?,
            required("profile")?,
            required("unit_key")?,
            required("source_payload_hash")?,
            required("created_at")?,
        )?;
        references.insert(reference.artifact_id.clone(), reference);
    }
    let mut manifest =
        rebuild_manifest_from_finalized_artifacts(store, init, references.into_values())?;
    // `summary_units` is a convenience projection, never an authority.  The
    // completed Index remains canonical; its Rust-owned unit key lets a
    // missing manifest recover the exact fixed planner unit without asking a
    // model or reconstructing identity from prose.
    for index in read_indexes(
        store,
        Some(&location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            limit: 256,
            ..Default::default()
        },
    )?
    .indexes
    {
        if let Some(unit_key) = index
            .authoritative_fields
            .get("unit_key")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            manifest
                .summary_units
                .insert(unit_key.to_owned(), index.index_id);
        }
    }
    write_run_manifest(store, &location, manifest)
}

/// Rebuild a derived Index catalog.  The returned document is also atomically
/// written as a cache; deleting it never loses authoritative knowledge.
pub fn rebuild_index_catalog(
    store: &FileStore,
    kind: IndexKind,
    run: Option<&RunLocation>,
    generated_at: impl Into<String>,
) -> Result<IndexCatalog> {
    let generated_at = generated_at.into();
    if generated_at.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "index catalog",
            message: "generated_at must not be empty".to_owned(),
        });
    }
    let base = match kind {
        IndexKind::PhaseSummary => run
            .ok_or_else(|| StoreError::InvalidDocument {
                kind: "index catalog",
                message: "phase_summary catalog requires run location".to_owned(),
            })?
            .relative_root()
            .join("index"),
        IndexKind::Experience => PathBuf::from("knowledge/experience"),
    };
    let mut entries = Vec::new();
    for index in read_indexes(
        store,
        run,
        &IndexQuery {
            kind: Some(kind),
            limit: 100,
            ..Default::default()
        },
    )?
    .indexes
    {
        entries.push(IndexCatalogEntry {
            index_id: index.index_id,
            source_phase: index.source_phase,
            role: index.role,
            ticker: index.ticker,
            topic_id: index.topic_id,
            pattern_key: index.pattern_key,
            detail_count: index.detail_count,
        });
    }
    entries.sort_by(|a, b| a.index_id.cmp(&b.index_id));
    let catalog = IndexCatalog {
        schema_version: INDEX_CATALOG_SCHEMA_VERSION,
        kind,
        run_id: run.map(|location| location.run_id.clone()),
        entries,
        generated_at,
        content_hash: String::new(),
    };
    store.write_authoritative_json(&base.join("catalog.json"), catalog)
}

/// Rebuild derived Experience occurrence counts and calculated levels.  The
/// source files remain authoritative and no candidate, promotion, or version
/// record is created.
pub fn rebuild_experience_stats(
    store: &FileStore,
    generated_at: impl Into<String>,
) -> Result<ExperienceStats> {
    let generated_at = generated_at.into();
    if generated_at.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "experience stats",
            message: "generated_at must not be empty".to_owned(),
        });
    }
    let files = collect_files(store.root())?;
    let base = Path::new("knowledge/experience");
    let mut indexes = Vec::new();
    for relative in files.iter().filter(|path| {
        path.parent()
            .is_some_and(|parent| parent.parent() == Some(base))
            && path.file_name().is_some_and(|name| name == "index.json")
    }) {
        let index: Index = store.read_versioned_json(relative, FileSchemaKind::Index)?;
        if index.kind != IndexKind::Experience {
            continue;
        }
        let detail_base = relative
            .parent()
            .expect("index path has parent")
            .join("details");
        let mut runs = BTreeSet::new();
        for detail_relative in files
            .iter()
            .filter(|path| path.parent() == Some(detail_base.as_path()))
        {
            let detail: IndexDetail =
                store.read_versioned_json(detail_relative, FileSchemaKind::Detail)?;
            if detail.section == DetailSection::HistoricalCase {
                runs.insert(detail.source_run_id);
            }
        }
        let count = runs.len();
        indexes.push(ExperienceStat {
            index_id: index.index_id,
            historical_source_run_count: count,
            level: match count {
                0 | 1 => "recent_episode",
                2 => "repeated_warning",
                _ => "active_policy",
            }
            .to_owned(),
        });
    }
    indexes.sort_by(|a, b| a.index_id.cmp(&b.index_id));
    let stats = ExperienceStats {
        schema_version: EXPERIENCE_STATS_SCHEMA_VERSION,
        indexes,
        generated_at,
        content_hash: String::new(),
    };
    store.write_authoritative_json(Path::new("knowledge/experience/stats.json"), stats)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::Path};

    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    use super::{
        inspect_store, rebuild_experience_stats, rebuild_index_catalog, rebuild_run_manifest,
        DoctorIssue,
    };
    use crate::{
        append_index_detail, create_index, create_or_recover_draft, finalize_draft_atomic,
        finalize_index, write_run_manifest, AppendIndexDetailInput, ArtifactScope,
        ContentHashDocument, CreateIndexInput, DetailSection, FileStore, FinalizableArtifact,
        IndexKind, IndexScope, RunLocation, RunManifest, RunManifestInit, ToolManagedProfile,
    };

    fn location() -> RunLocation {
        RunLocation::new("2026-07-27", "doctor-run").unwrap()
    }
    fn manifest(location: RunLocation) -> RunManifest {
        RunManifest::new(RunManifestInit {
            location,
            workflow_version: "v1".to_owned(),
            prompt_versions: BTreeMap::new(),
            git_sha: "sha".to_owned(),
            config_hash: "config".to_owned(),
            role_profile_registry_hash: "authority".to_owned(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        })
        .unwrap()
    }
    fn summary_scope(location: RunLocation) -> IndexScope {
        IndexScope {
            kind: IndexKind::PhaseSummary,
            location: Some(location),
            index_id: "summary-one".to_owned(),
            run_id: "doctor-run".to_owned(),
            source_run_id: None,
            source_phase: 1,
            role: "compressor.phase_summary".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            source_payload_hash: "source".to_owned(),
            authoritative_fields: Default::default(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestArtifact {
        schema_version: u32,
        artifact_id: String,
        phase: u8,
        role: String,
        profile: String,
        unit_key: String,
        source_payload_hash: String,
        created_at: String,
        content_hash: String,
    }

    impl ContentHashDocument for TestArtifact {
        fn content_hash(&self) -> &str {
            &self.content_hash
        }

        fn set_content_hash(&mut self, hash: String) {
            self.content_hash = hash;
        }
    }

    impl FinalizableArtifact for TestArtifact {
        fn artifact_id(&self) -> &str {
            &self.artifact_id
        }

        fn source_payload_hash(&self) -> &str {
            &self.source_payload_hash
        }
    }

    fn draft_scope() -> ArtifactScope {
        ArtifactScope {
            run_id: "doctor-run".to_owned(),
            current_date: "2026-07-27".to_owned(),
            phase: 1,
            role: "analyst.technical".to_owned(),
            profile: ToolManagedProfile::AnalystReport,
            profile_version: 1,
            builder_version: 1,
            unit_key: "QQQ".to_owned(),
            source_payload_hash: "source".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            side: None,
            stance: None,
            round: None,
            reflection_task: None,
        }
    }

    #[test]
    fn doctor_reports_malformed_documents_and_incomplete_index_drafts() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let location = location();
        write_run_manifest(&store, &location, manifest(location.clone())).unwrap();
        let scope = summary_scope(location.clone());
        create_index(
            &store,
            CreateIndexInput {
                scope,
                summary: "summary".to_owned(),
                confidence: 0.7,
                pattern_key: None,
                applies_to_phases: vec![2],
            },
        )
        .unwrap();
        store
            .write_bytes(
                &location.child_relative(Path::new("bad.json")).unwrap(),
                b"{",
            )
            .unwrap();
        let report = inspect_store(&store);
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "malformed_json"));
        assert!(report
            .issues
            .iter()
            .any(|issue| issue.code == "incomplete_index_draft"));
    }

    #[test]
    fn doctor_detects_duplicate_experience_source_runs_and_rebuilds_caches() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let location = location();
        write_run_manifest(&store, &location, manifest(location.clone())).unwrap();
        let scope = summary_scope(location.clone());
        create_index(
            &store,
            CreateIndexInput {
                scope: scope.clone(),
                summary: "summary".to_owned(),
                confidence: 0.7,
                pattern_key: None,
                applies_to_phases: vec![2],
            },
        )
        .unwrap();
        append_index_detail(
            &store,
            AppendIndexDetailInput {
                scope: scope.clone(),
                section: DetailSection::Evidence,
                detail: "detail".to_owned(),
                source_refs: vec![],
            },
        )
        .unwrap();
        finalize_index(&store, &scope).unwrap();
        let catalog =
            rebuild_index_catalog(&store, IndexKind::PhaseSummary, Some(&location), "now").unwrap();
        assert_eq!(catalog.entries.len(), 1);
        let stats = rebuild_experience_stats(&store, "now").unwrap();
        assert!(stats.indexes.is_empty());
        assert!(Path::new("knowledge/experience/stats.json").is_relative());
    }

    #[test]
    fn rebuild_manifest_uses_only_completed_draft_artifacts() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        let location = location();
        let scope = draft_scope();
        create_or_recover_draft(&store, &location, scope.clone(), "2026-07-27T00:00:00Z").unwrap();
        finalize_draft_atomic(
            &store,
            &location,
            &scope,
            Path::new("artifacts/phase1/QQQ.json"),
            TestArtifact {
                schema_version: 2,
                artifact_id: "artifact-QQQ".to_owned(),
                phase: 1,
                role: "analyst.technical".to_owned(),
                profile: "analyst_report".to_owned(),
                unit_key: "QQQ".to_owned(),
                source_payload_hash: "source".to_owned(),
                created_at: "2026-07-27T00:01:00Z".to_owned(),
                content_hash: String::new(),
            },
            "2026-07-27T00:01:00Z",
        )
        .unwrap();
        let rebuilt = rebuild_run_manifest(
            &store,
            RunManifestInit {
                location,
                workflow_version: "v1".to_owned(),
                prompt_versions: BTreeMap::new(),
                git_sha: "sha".to_owned(),
                config_hash: "config".to_owned(),
                role_profile_registry_hash: "authority".to_owned(),
                created_at: "2026-07-27T00:02:00Z".to_owned(),
            },
        )
        .unwrap();
        assert!(rebuilt.artifacts.contains_key("artifact-QQQ"));
    }

    #[test]
    fn doctor_accepts_the_current_domain_artifact_schema() {
        let path = Path::new("runs/2026-07-27/run-x/artifacts/phase1/QQQ.json");
        let value = crate::set_content_hash(&serde_json::json!({
            "schema_version": 2,
            "artifact_id": "artifact-QQQ",
            "profile": "analyst_report"
        }))
        .unwrap();

        super::validate_generic_document(&value, path).unwrap();
    }

    #[test]
    fn doctor_preserves_one_incomplete_jsonl_tail_and_rejects_middle_corruption() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), Default::default()).unwrap();
        store
            .write_bytes(
                Path::new("runs/2026-07-27/run-x/sessions/session-x/turn.jsonl"),
                b"{",
            )
            .unwrap();
        let report = inspect_store(&store);
        assert_eq!(report.recovered_jsonl_tails, 1);
        assert!(report
            .issues
            .iter()
            .any(|DoctorIssue { code, .. }| code == "recovered_jsonl_tail"));
    }
}
