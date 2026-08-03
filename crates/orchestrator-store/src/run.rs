use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    error::io_error, paths::resolve_existing, read_run_manifest, FileSchemaKind, FileStore, Index,
    IndexArchive, IndexDetail, PhaseStatus, Result, RunLocation, RunStatus, StoreError,
};

#[derive(Debug, Clone)]
pub struct RunStore {
    store: FileStore,
    location: RunLocation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunCompactionMode {
    DryRun,
    Apply,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCompactionReport {
    pub run_id: String,
    pub eligible: bool,
    pub applied: bool,
    pub skipped_reason: Option<String>,
    pub candidate_files: usize,
    pub removed_files: usize,
    pub removed_directories: usize,
    pub reclaimable_bytes: u64,
    pub index_archives: usize,
    pub candidate_paths: Vec<String>,
}

impl RunStore {
    pub fn new(store: FileStore, location: RunLocation) -> Self {
        Self { store, location }
    }

    /// Pack finalized run-local Indexes for ordinary healthy runs. Debug and
    /// degraded runs are never compacted so their state, sessions, stree
    /// checkpoints, and input snapshots remain available for diagnosis and
    /// recovery. Partial and failed runs are untouched.
    pub fn compact_completed_run(&self, mode: RunCompactionMode) -> Result<RunCompactionReport> {
        let manifest = read_run_manifest(&self.store, &self.location)?;
        let skipped_reason = if manifest.status != RunStatus::Completed {
            Some(format!("run status is {:?}", manifest.status))
        } else if manifest.phase_status.get("8") != Some(&PhaseStatus::Completed) {
            Some("phase 8 is not completed".to_owned())
        } else if self.location.storage_namespace() == Some("debug") {
            Some("debug namespace is never compacted".to_owned())
        } else if manifest.degraded {
            Some("degraded run is never compacted".to_owned())
        } else {
            None
        };
        if let Some(reason) = skipped_reason {
            return Ok(RunCompactionReport {
                run_id: self.location.run_id.clone(),
                eligible: false,
                applied: false,
                skipped_reason: Some(reason),
                candidate_files: 0,
                removed_files: 0,
                removed_directories: 0,
                reclaimable_bytes: 0,
                index_archives: 0,
                candidate_paths: Vec::new(),
            });
        }

        let run_relative = self.location.relative_root();
        let run_root = resolve_existing(self.store.root(), &run_relative)?;
        let index_archives = self.prepare_index_archives(mode)?;
        let mut candidates = BTreeMap::new();
        collect_candidates(
            self.store.root(),
            &run_root,
            &self.location.manifest_relative(),
            &index_archives,
            &mut candidates,
        )?;
        let candidate_paths = candidates
            .keys()
            .map(|path| path.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let reclaimable_bytes = candidates.values().copied().sum();
        let candidate_files = candidates.len();
        if mode == RunCompactionMode::DryRun {
            return Ok(RunCompactionReport {
                run_id: self.location.run_id.clone(),
                eligible: true,
                applied: false,
                skipped_reason: None,
                candidate_files,
                removed_files: 0,
                removed_directories: 0,
                reclaimable_bytes,
                index_archives: index_archives.len(),
                candidate_paths,
            });
        }

        for relative in candidates.keys() {
            let path = resolve_existing(self.store.root(), relative)?;
            let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
            if metadata.file_type().is_symlink() {
                return Err(StoreError::SymlinkPath { path });
            }
            if !metadata.is_file() {
                return Err(StoreError::InvalidDocument {
                    kind: "run compaction",
                    message: format!("candidate is not a regular file: {}", path.display()),
                });
            }
            fs::remove_file(&path).map_err(|source| io_error(&path, source))?;
        }
        let mut compacted_manifest = manifest;
        compacted_manifest.artifacts.clear();
        crate::write_run_manifest(&self.store, &self.location, compacted_manifest)?;
        let removed_directories = remove_empty_directories(&run_root, &run_root)?;

        Ok(RunCompactionReport {
            run_id: self.location.run_id.clone(),
            eligible: true,
            applied: true,
            skipped_reason: None,
            candidate_files,
            removed_files: candidate_files,
            removed_directories,
            reclaimable_bytes,
            index_archives: index_archives.len(),
            candidate_paths,
        })
    }

    fn prepare_index_archives(&self, mode: RunCompactionMode) -> Result<BTreeSet<PathBuf>> {
        let base_relative = self.location.relative_root().join("index");
        if !self.store.exists(&base_relative)? {
            return Ok(BTreeSet::new());
        }
        let base = resolve_existing(self.store.root(), &base_relative)?;
        let mut index_directories = Vec::new();
        let mut archives = BTreeSet::new();
        discover_run_indexes(
            &self.store,
            &self.location,
            &base,
            &mut index_directories,
            &mut archives,
        )?;
        index_directories.sort();

        for directory in index_directories {
            let directory_relative = directory
                .strip_prefix(self.store.root())
                .expect("Index directory is beneath store root")
                .to_path_buf();
            let index: Index = self.store.read_versioned_json(
                &directory_relative.join("index.json"),
                FileSchemaKind::Index,
            )?;
            let archive_relative =
                IndexArchive::relative_path(&self.location, index.source_phase, &index.index_id)?;
            let archive = if self.store.exists(&archive_relative)? {
                let archive: IndexArchive = self.store.read_versioned_json(
                    &archive_relative,
                    FileSchemaKind::Artifact("index_archive".to_owned()),
                )?;
                archive.validate_for_location(&self.location)?;
                archive
            } else {
                let details_relative = directory_relative.join("details");
                let details_root = resolve_existing(self.store.root(), &details_relative)?;
                let mut detail_entries = fs::read_dir(&details_root)
                    .map_err(|source| io_error(&details_root, source))?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|source| io_error(&details_root, source))?;
                detail_entries.sort_by_key(|entry| entry.file_name());
                let mut details = Vec::new();
                for entry in detail_entries {
                    let path = entry.path();
                    let metadata =
                        fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
                    if metadata.file_type().is_symlink() {
                        return Err(StoreError::SymlinkPath { path });
                    }
                    if !metadata.is_file()
                        || path.extension().and_then(|value| value.to_str()) != Some("json")
                    {
                        continue;
                    }
                    let relative = path
                        .strip_prefix(self.store.root())
                        .expect("Index Detail is beneath store root");
                    details.push(
                        self.store
                            .read_versioned_json::<IndexDetail>(relative, FileSchemaKind::Detail)?,
                    );
                }
                let archive = IndexArchive::from_index(&self.location, index, details)?;
                if mode == RunCompactionMode::Apply {
                    self.store
                        .write_authoritative_json(&archive_relative, archive.clone())?;
                }
                archive
            };
            if archive.index.detail_count != archive.details.len() {
                return Err(StoreError::InvalidDocument {
                    kind: "index archive",
                    message: "archive Detail count changed during compaction".to_owned(),
                });
            }
            archives.insert(self.store.root().join(archive_relative));
        }
        Ok(archives)
    }
}

fn discover_run_indexes(
    store: &FileStore,
    location: &RunLocation,
    directory: &Path,
    index_directories: &mut Vec<PathBuf>,
    archives: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    for entry in fs::read_dir(directory).map_err(|source| io_error(directory, source))? {
        let entry = entry.map_err(|source| io_error(directory, source))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path });
        }
        if metadata.is_dir() {
            if entry.file_name().to_string_lossy().starts_with('.') {
                return Err(StoreError::InvalidDocument {
                    kind: "index archive",
                    message: format!("unfinished Index draft remains at {}", path.display()),
                });
            }
            if path.join("index.json").is_file() {
                index_directories.push(path);
            } else {
                discover_run_indexes(store, location, &path, index_directories, archives)?;
            }
            continue;
        }
        if !metadata.is_file() || path.extension().and_then(|value| value.to_str()) != Some("json")
        {
            continue;
        }
        let relative = path
            .strip_prefix(store.root())
            .expect("Index archive is beneath store root");
        let archive: IndexArchive = store.read_versioned_json(
            relative,
            FileSchemaKind::Artifact("index_archive".to_owned()),
        )?;
        archive.validate_for_location(location)?;
        if IndexArchive::relative_path(
            location,
            archive.index.source_phase,
            &archive.index.index_id,
        )? != relative
        {
            return Err(StoreError::InvalidDocument {
                kind: "index archive",
                message: format!("archive path is not canonical: {}", path.display()),
            });
        }
        archives.insert(path);
    }
    Ok(())
}

fn collect_candidates(
    store_root: &Path,
    directory: &Path,
    manifest_relative: &Path,
    index_archives: &BTreeSet<PathBuf>,
    output: &mut BTreeMap<PathBuf, u64>,
) -> Result<()> {
    let entries = fs::read_dir(directory).map_err(|source| io_error(directory, source))?;
    for entry in entries {
        let entry = entry.map_err(|source| io_error(directory, source))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path });
        }
        if metadata.is_dir() {
            collect_candidates(store_root, &path, manifest_relative, index_archives, output)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(StoreError::InvalidDocument {
                kind: "run compaction",
                message: format!("unsupported filesystem entry: {}", path.display()),
            });
        }
        let relative = path
            .strip_prefix(store_root)
            .expect("candidate is beneath store root")
            .to_path_buf();
        if relative != manifest_relative && !index_archives.contains(&path) {
            output.insert(relative, metadata.len());
        }
    }
    Ok(())
}

fn remove_empty_directories(run_root: &Path, directory: &Path) -> Result<usize> {
    let mut removed = 0;
    let entries = fs::read_dir(directory)
        .map_err(|source| io_error(directory, source))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|source| io_error(directory, source))?;
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path });
        }
        if metadata.is_dir() {
            removed += remove_empty_directories(run_root, &path)?;
        }
    }
    if directory != run_root
        && fs::read_dir(directory)
            .map_err(|source| io_error(directory, source))?
            .next()
            .is_none()
    {
        fs::remove_dir(directory).map_err(|source| io_error(directory, source))?;
        removed += 1;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use super::{RunCompactionMode, RunStore};
    use crate::{
        append_index_detail, append_session_event, create_index, create_or_recover_draft,
        finalize_index, read_index_details, read_indexes, set_content_hash, write_run_manifest,
        write_session_manifest, AppendIndexDetailInput, ArtifactScope, CreateIndexInput,
        DetailQuery, DetailSection, FileStore, FileStoreOptions, IndexKind, IndexQuery, IndexScope,
        PhaseStatus, RunLocation, RunManifest, RunManifestInit, RunStatus, SessionEventInput,
        SessionEventType, SessionLocation, SessionManifest, ToolManagedProfile,
    };

    fn location() -> RunLocation {
        RunLocation::new("2026-07-29", "run-one").unwrap()
    }

    fn manifest(location: RunLocation, status: RunStatus, degraded: bool) -> RunManifest {
        let mut manifest = RunManifest::new(RunManifestInit {
            location,
            workflow_version: "test".to_owned(),
            prompt_versions: Default::default(),
            git_sha: "test".to_owned(),
            config_hash: "sha256:config".to_owned(),
            role_profile_registry_hash: "sha256:registry".to_owned(),
            created_at: "2026-07-29T00:00:00Z".to_owned(),
        })
        .unwrap();
        manifest.status = status;
        manifest.degraded = degraded;
        if status == RunStatus::Completed {
            manifest
                .phase_status
                .insert("8".to_owned(), PhaseStatus::Completed);
        }
        manifest
    }

    fn scope(location: &RunLocation) -> ArtifactScope {
        ArtifactScope {
            run_id: location.run_id.clone(),
            current_date: location.current_date.clone(),
            phase: 2,
            role: "researcher.warmup".to_owned(),
            profile: ToolManagedProfile::ResearcherWarmup,
            profile_version: 1,
            builder_version: 1,
            unit_key: "warmup".to_owned(),
            source_payload_hash: "sha256:source".to_owned(),
            ticker: None,
            topic_id: None,
            side: None,
            stance: None,
            round: None,
            reflection_task: None,
        }
    }

    #[test]
    fn completed_run_keeps_only_manifest_and_indexes() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let mut draft =
            create_or_recover_draft(&store, &location, scope(&location), "2026-07-29T00:00:00Z")
                .unwrap();
        draft.lifecycle = crate::DraftLifecycle::Completed;
        if let crate::ArtifactDraftState::ResearcherWarmup(warmup) = &mut draft.state {
            warmup.finalized = true;
        }
        store
            .write_authoritative_json(
                &crate::draft_relative(&location, &draft.scope).unwrap(),
                draft,
            )
            .unwrap();
        let session_location = SessionLocation::new(location.clone(), "session-one").unwrap();
        let session = write_session_manifest(
            &store,
            &session_location,
            SessionManifest::new(
                &session_location,
                "researcher.warmup",
                2,
                "researcher_warmup",
                None,
                "2026-07-29T00:00:00Z",
            )
            .unwrap(),
        )
        .unwrap();
        append_session_event(
            &store,
            &session_location,
            &session,
            SessionEventInput {
                event_type: SessionEventType::Terminal,
                turn_id: "turn-one".to_owned(),
                payload: serde_json::json!({"status": "completed"}),
                created_at: "2026-07-29T00:01:00Z".to_owned(),
            },
        )
        .unwrap();
        write_run_manifest(
            &store,
            &location,
            manifest(location.clone(), RunStatus::Completed, false),
        )
        .unwrap();

        let run = RunStore::new(store.clone(), location.clone());
        let dry_run = run
            .compact_completed_run(RunCompactionMode::DryRun)
            .unwrap();
        assert!(dry_run.eligible);
        assert!(!dry_run.applied);
        assert_eq!(dry_run.candidate_files, 3);

        let applied = run.compact_completed_run(RunCompactionMode::Apply).unwrap();
        assert!(applied.applied);
        assert_eq!(applied.removed_files, 3);
        assert!(!store.exists(&session_location.relative_dir()).unwrap());
        assert!(!store
            .exists(&location.child_relative(Path::new("drafts")).unwrap())
            .unwrap());
        assert!(store.exists(&location.manifest_relative()).unwrap());
    }

    #[test]
    fn completed_degraded_run_is_not_compacted() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let state = location.state_relative();
        store.write_bytes(&state, b"{}").unwrap();
        let session_file = location
            .child_relative(Path::new("sessions/session-one/turn.jsonl"))
            .unwrap();
        store.write_bytes(&session_file, b"{}").unwrap();
        let input_manifest = location
            .child_relative(Path::new("inputs/manifest.json"))
            .unwrap();
        store.write_bytes(&input_manifest, b"{}").unwrap();
        write_run_manifest(
            &store,
            &location,
            manifest(location.clone(), RunStatus::Completed, true),
        )
        .unwrap();

        let run = RunStore::new(store.clone(), location.clone());
        let report = run.compact_completed_run(RunCompactionMode::Apply).unwrap();
        assert!(!report.eligible);
        assert!(!report.applied);
        assert_eq!(
            report.skipped_reason.as_deref(),
            Some("degraded run is never compacted")
        );
        assert!(store.exists(&state).unwrap());
        assert!(store.exists(&session_file).unwrap());
        assert!(store.exists(&input_manifest).unwrap());
    }

    #[test]
    fn partial_run_retains_recovery_files() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let mut partial = manifest(location.clone(), RunStatus::Completed, false);
        partial.phase_status.remove("8");
        partial
            .phase_status
            .insert("3".to_owned(), PhaseStatus::Completed);
        let session_file = location
            .child_relative(Path::new("sessions/session-one/turn.jsonl"))
            .unwrap();
        store.write_bytes(&session_file, b"").unwrap();
        write_run_manifest(&store, &location, partial).unwrap();

        let run = RunStore::new(store.clone(), location);
        let report = run.compact_completed_run(RunCompactionMode::Apply).unwrap();
        assert!(!report.eligible);
        assert_eq!(
            report.skipped_reason.as_deref(),
            Some("phase 8 is not completed")
        );
        assert!(store.exists(&session_file).unwrap());
    }

    #[test]
    fn completed_normal_namespace_is_compacted() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let state = set_content_hash(&serde_json::json!({"debug": true})).unwrap();
        store
            .write_json_value(&location.state_relative(), &state)
            .unwrap();
        write_run_manifest(
            &store,
            &location,
            manifest(location.clone(), RunStatus::Completed, false),
        )
        .unwrap();

        let run = RunStore::new(store.clone(), location.clone());
        let report = run.compact_completed_run(RunCompactionMode::Apply).unwrap();

        assert!(report.eligible);
        assert!(report.applied);
        assert!(!store.exists(&location.state_relative()).unwrap());
    }

    #[test]
    fn completed_debug_namespace_retains_all_recovery_files() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::debug("2026-07-29", "debug-run").unwrap();
        let state = set_content_hash(&serde_json::json!({"debug": true})).unwrap();
        store
            .write_json_value(&location.state_relative(), &state)
            .unwrap();
        let session_file = location
            .child_relative(Path::new("sessions/session-one/turn.jsonl"))
            .unwrap();
        store.write_bytes(&session_file, b"{}").unwrap();
        let stree_file = location
            .child_relative(Path::new("drafts/topic-one/stree.json"))
            .unwrap();
        store.write_bytes(&stree_file, b"{}").unwrap();
        let input_manifest = location
            .child_relative(Path::new("inputs/manifest.json"))
            .unwrap();
        store.write_bytes(&input_manifest, b"{}").unwrap();
        write_run_manifest(
            &store,
            &location,
            manifest(location.clone(), RunStatus::Completed, false),
        )
        .unwrap();

        let report = RunStore::new(store.clone(), location.clone())
            .compact_completed_run(RunCompactionMode::Apply)
            .unwrap();

        assert!(!report.eligible);
        assert!(!report.applied);
        assert_eq!(
            report.skipped_reason.as_deref(),
            Some("debug namespace is never compacted")
        );
        assert!(store.exists(&location.state_relative()).unwrap());
        assert!(store.exists(&session_file).unwrap());
        assert!(store.exists(&stree_file).unwrap());
        assert!(store.exists(&input_manifest).unwrap());
    }

    #[test]
    fn completed_run_compacts_indexes_without_changing_queries() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let scope = IndexScope {
            kind: IndexKind::PhaseSummary,
            location: Some(location.clone()),
            index_id: "summary-one".to_owned(),
            run_id: location.run_id.clone(),
            source_run_id: None,
            source_phase: 1,
            role: "compressor.phase_summary".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            source_payload_hash: "sha256:source".to_owned(),
            authoritative_fields: Default::default(),
            created_at: "2026-07-29T00:00:00Z".to_owned(),
        };
        create_index(
            &store,
            CreateIndexInput {
                scope: scope.clone(),
                summary: "summary".to_owned(),
                confidence: 0.8,
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
                source_refs: Vec::new(),
            },
        )
        .unwrap();
        finalize_index(&store, &scope).unwrap();
        write_run_manifest(
            &store,
            &location,
            manifest(location.clone(), RunStatus::Completed, false),
        )
        .unwrap();

        let run = RunStore::new(store.clone(), location.clone());
        let report = run.compact_completed_run(RunCompactionMode::Apply).unwrap();
        assert_eq!(report.index_archives, 1);
        assert_eq!(
            read_indexes(
                &store,
                Some(&location),
                &IndexQuery {
                    kind: Some(IndexKind::PhaseSummary),
                    limit: 10,
                    ..Default::default()
                },
            )
            .unwrap()
            .indexes
            .len(),
            1
        );
        assert_eq!(
            read_index_details(
                &store,
                &scope,
                &DetailQuery {
                    limit: 10,
                    ..Default::default()
                },
            )
            .unwrap()
            .details
            .len(),
            1
        );

        let second = run.compact_completed_run(RunCompactionMode::Apply).unwrap();
        assert_eq!(second.index_archives, 1);
        assert_eq!(second.removed_files, 0);
        assert_eq!(
            read_indexes(
                &store,
                Some(&location),
                &IndexQuery {
                    kind: Some(IndexKind::PhaseSummary),
                    limit: 10,
                    ..Default::default()
                },
            )
            .unwrap()
            .indexes
            .len(),
            1
        );
    }

    #[test]
    fn completed_run_removes_active_runtime_draft() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let draft =
            create_or_recover_draft(&store, &location, scope(&location), "2026-07-29T00:00:00Z")
                .unwrap();
        let draft_relative = crate::draft_relative(&location, &draft.scope).unwrap();
        write_run_manifest(
            &store,
            &location,
            manifest(location.clone(), RunStatus::Completed, false),
        )
        .unwrap();

        let run = RunStore::new(store.clone(), location);
        let report = run.compact_completed_run(RunCompactionMode::Apply).unwrap();
        assert!(report.applied);
        assert!(!store.exists(&draft_relative).unwrap());
    }
}
