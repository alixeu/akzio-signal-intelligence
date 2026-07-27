use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    validate_relative_path, ContentHashDocument, FileSchemaKind, FileStore, Result, SafeSlug,
    StoreError, Versioned,
};

pub const RUN_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Stable store location for one workflow run. The human workflow date stays
/// readable; the untrusted/generated run ID is encoded before it becomes a
/// path component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLocation {
    pub current_date: String,
    pub run_id: String,
    run_slug: SafeSlug,
}

impl RunLocation {
    pub fn new(current_date: impl Into<String>, run_id: impl Into<String>) -> Result<Self> {
        let current_date = current_date.into();
        let run_id = run_id.into();
        if !is_workflow_date(&current_date) {
            return Err(StoreError::InvalidDocument {
                kind: "run location",
                message: format!("current_date must be YYYY-MM-DD, found `{current_date}`"),
            });
        }
        if run_id.trim().is_empty() {
            return Err(StoreError::InvalidDocument {
                kind: "run location",
                message: "run_id must not be empty".to_owned(),
            });
        }
        Ok(Self {
            run_slug: SafeSlug::new("run", &run_id)?,
            current_date,
            run_id,
        })
    }

    pub fn relative_root(&self) -> PathBuf {
        PathBuf::from("runs")
            .join(&self.current_date)
            .join(self.run_slug.as_str())
    }

    pub fn manifest_relative(&self) -> PathBuf {
        self.relative_root().join("manifest.json")
    }

    pub fn state_relative(&self) -> PathBuf {
        self.relative_root().join("state.json")
    }

    pub fn run_slug(&self) -> &SafeSlug {
        &self.run_slug
    }

    pub fn child_relative(&self, child: &Path) -> Result<PathBuf> {
        validate_relative_path(child)?;
        Ok(self.relative_root().join(child))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestError {
    pub phase: Option<u8>,
    pub code: String,
    pub message: String,
    pub created_at: String,
}

/// Reference-only manifest entry. The Artifact file is more authoritative
/// than this projection during recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizedArtifactRef {
    pub artifact_id: String,
    pub relative_path: String,
    pub phase: u8,
    pub role: String,
    pub profile: String,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub created_at: String,
}

impl FinalizedArtifactRef {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        artifact_id: impl Into<String>,
        relative_path: &Path,
        phase: u8,
        role: impl Into<String>,
        profile: impl Into<String>,
        unit_key: impl Into<String>,
        source_payload_hash: impl Into<String>,
        created_at: impl Into<String>,
    ) -> Result<Self> {
        validate_relative_path(relative_path)?;
        if !relative_path.starts_with("artifacts") {
            return Err(StoreError::InvalidDocument {
                kind: "finalized artifact reference",
                message: "artifact path must be beneath artifacts/".to_owned(),
            });
        }
        let relative_path = relative_path
            .to_str()
            .ok_or_else(|| StoreError::InvalidDocument {
                kind: "finalized artifact reference",
                message: "artifact path must be valid UTF-8".to_owned(),
            })?
            .to_owned();
        let reference = Self {
            artifact_id: artifact_id.into(),
            relative_path,
            phase,
            role: role.into(),
            profile: profile.into(),
            unit_key: unit_key.into(),
            source_payload_hash: source_payload_hash.into(),
            created_at: created_at.into(),
        };
        reference.validate()?;
        Ok(reference)
    }

    pub fn relative_path(&self) -> PathBuf {
        PathBuf::from(&self.relative_path)
    }

    /// Idempotent finalization deliberately ignores the observation timestamp:
    /// the canonical Artifact identity is its scope and immutable path.
    pub fn same_artifact_identity(&self, other: &Self) -> bool {
        self.artifact_id == other.artifact_id
            && self.relative_path == other.relative_path
            && self.phase == other.phase
            && self.role == other.role
            && self.profile == other.profile
            && self.unit_key == other.unit_key
            && self.source_payload_hash == other.source_payload_hash
    }

    pub fn validate(&self) -> Result<()> {
        if self.artifact_id.is_empty()
            || self.role.is_empty()
            || self.profile.is_empty()
            || self.unit_key.is_empty()
            || self.source_payload_hash.is_empty()
            || self.created_at.is_empty()
        {
            return Err(StoreError::InvalidDocument {
                kind: "finalized artifact reference",
                message: "artifact reference contains an empty required field".to_owned(),
            });
        }
        validate_relative_path(Path::new(&self.relative_path))?;
        if !Path::new(&self.relative_path).starts_with("artifacts") {
            return Err(StoreError::InvalidDocument {
                kind: "finalized artifact reference",
                message: "artifact path must be beneath artifacts/".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunManifestInit {
    pub location: RunLocation,
    pub workflow_version: String,
    pub prompt_versions: BTreeMap<String, String>,
    pub git_sha: String,
    pub config_hash: String,
    pub authority_registry_hash: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunManifest {
    pub schema_version: u32,
    pub run_id: String,
    pub current_date: String,
    pub status: RunStatus,
    pub current_phase: u8,
    pub phase_status: BTreeMap<String, PhaseStatus>,
    pub workflow_version: String,
    pub prompt_versions: BTreeMap<String, String>,
    pub git_sha: String,
    pub config_hash: String,
    pub authority_registry_hash: String,
    pub degraded: bool,
    pub errors: Vec<ManifestError>,
    pub artifacts: BTreeMap<String, FinalizedArtifactRef>,
    pub summary_units: BTreeMap<String, String>,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub content_hash: String,
}

impl RunManifest {
    pub fn new(init: RunManifestInit) -> Result<Self> {
        for (name, value) in [
            ("workflow_version", &init.workflow_version),
            ("git_sha", &init.git_sha),
            ("config_hash", &init.config_hash),
            ("authority_registry_hash", &init.authority_registry_hash),
            ("created_at", &init.created_at),
        ] {
            if value.is_empty() {
                return Err(StoreError::InvalidDocument {
                    kind: "run manifest",
                    message: format!("{name} must not be empty"),
                });
            }
        }
        Ok(Self {
            schema_version: RUN_MANIFEST_SCHEMA_VERSION,
            run_id: init.location.run_id,
            current_date: init.location.current_date,
            status: RunStatus::Running,
            current_phase: 0,
            phase_status: BTreeMap::new(),
            workflow_version: init.workflow_version,
            prompt_versions: init.prompt_versions,
            git_sha: init.git_sha,
            config_hash: init.config_hash,
            authority_registry_hash: init.authority_registry_hash,
            degraded: false,
            errors: Vec::new(),
            artifacts: BTreeMap::new(),
            summary_units: BTreeMap::new(),
            created_at: init.created_at,
            completed_at: None,
            content_hash: String::new(),
        })
    }

    pub fn location(&self) -> Result<RunLocation> {
        RunLocation::new(self.current_date.clone(), self.run_id.clone())
    }

    pub fn record_finalized_artifact(&mut self, artifact: FinalizedArtifactRef) -> Result<()> {
        artifact.validate()?;
        if let Some(existing) = self.artifacts.get(&artifact.artifact_id) {
            if existing != &artifact {
                return Err(StoreError::InvalidDocument {
                    kind: "run manifest",
                    message: format!(
                        "artifact ID `{}` points to conflicting files",
                        artifact.artifact_id
                    ),
                });
            }
            return Ok(());
        }
        self.current_phase = self.current_phase.max(artifact.phase);
        self.phase_status
            .entry(artifact.phase.to_string())
            .or_insert(PhaseStatus::Completed);
        self.artifacts
            .insert(artifact.artifact_id.clone(), artifact);
        Ok(())
    }

    pub fn validate_for_location(&self, location: &RunLocation) -> Result<()> {
        if self.schema_version != RUN_MANIFEST_SCHEMA_VERSION {
            return Err(StoreError::InvalidDocument {
                kind: "run manifest",
                message: "schema_version differs from current typed manifest".to_owned(),
            });
        }
        if self.run_id != location.run_id || self.current_date != location.current_date {
            return Err(StoreError::InvalidDocument {
                kind: "run manifest",
                message: "manifest run identity differs from requested store location".to_owned(),
            });
        }
        for artifact in self.artifacts.values() {
            artifact.validate()?;
        }
        Ok(())
    }
}

impl Versioned for RunManifest {
    const SCHEMA_VERSION: u32 = RUN_MANIFEST_SCHEMA_VERSION;
}

impl ContentHashDocument for RunManifest {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

pub fn write_run_manifest(
    store: &FileStore,
    location: &RunLocation,
    manifest: RunManifest,
) -> Result<RunManifest> {
    manifest.validate_for_location(location)?;
    store.write_authoritative_json(&location.manifest_relative(), manifest)
}

pub fn read_run_manifest(store: &FileStore, location: &RunLocation) -> Result<RunManifest> {
    let manifest = store.read_versioned_json::<RunManifest>(
        &location.manifest_relative(),
        FileSchemaKind::RunManifest,
    )?;
    manifest.validate_for_location(location)?;
    Ok(manifest)
}

/// Locate a FileStore run by its authoritative `run_id`. Run IDs are never
/// path components, so callers must not reconstruct a slug from untrusted
/// input. Duplicate IDs indicate corruption and fail closed.
pub fn find_run_location(store: &FileStore, run_id: &str) -> Result<Option<RunLocation>> {
    if run_id.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "run location",
            message: "run_id must not be empty".to_owned(),
        });
    }
    let runs = store.root().join("runs");
    if !runs.exists() {
        return Ok(None);
    }
    let mut found = None;
    for date in std::fs::read_dir(&runs).map_err(|source| StoreError::Io {
        path: runs.clone(),
        source,
    })? {
        let date = date.map_err(|source| StoreError::Io {
            path: runs.clone(),
            source,
        })?;
        let date_path = date.path();
        let metadata = std::fs::symlink_metadata(&date_path).map_err(|source| StoreError::Io {
            path: date_path.clone(),
            source,
        })?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path: date_path });
        }
        if !metadata.is_dir() || !is_workflow_date(&date.file_name().to_string_lossy()) {
            continue;
        }
        for entry in std::fs::read_dir(&date_path).map_err(|source| StoreError::Io {
            path: date_path.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| StoreError::Io {
                path: date_path.clone(),
                source,
            })?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(|source| StoreError::Io {
                path: path.clone(),
                source,
            })?;
            if metadata.file_type().is_symlink() {
                return Err(StoreError::SymlinkPath { path });
            }
            if !metadata.is_dir() {
                continue;
            }
            let relative = PathBuf::from("runs")
                .join(date.file_name())
                .join(entry.file_name())
                .join("manifest.json");
            if !store.exists(&relative)? {
                continue;
            }
            let manifest: RunManifest =
                store.read_versioned_json(&relative, FileSchemaKind::RunManifest)?;
            if manifest.run_id != run_id {
                continue;
            }
            let location = RunLocation::new(manifest.current_date, manifest.run_id)?;
            if found.replace(location).is_some() {
                return Err(StoreError::InvalidDocument {
                    kind: "run location",
                    message: format!("duplicate FileStore manifests for run_id {run_id}"),
                });
            }
        }
    }
    Ok(found)
}

fn is_workflow_date(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::{
        find_run_location, read_run_manifest, write_run_manifest, FinalizedArtifactRef,
        RunLocation, RunManifest, RunManifestInit,
    };
    use crate::{FileStore, FileStoreOptions};

    fn location() -> RunLocation {
        RunLocation::new("2026-07-27", "run/with special characters").unwrap()
    }

    fn manifest(location: RunLocation) -> RunManifest {
        RunManifest::new(RunManifestInit {
            location,
            workflow_version: "workflow-v2".to_owned(),
            prompt_versions: BTreeMap::new(),
            git_sha: "deadbeef".to_owned(),
            config_hash: "sha256:config".to_owned(),
            authority_registry_hash: "sha256:authority".to_owned(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        })
        .unwrap()
    }

    #[test]
    fn manifest_uses_safe_run_directory_and_verifies_content_hash() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let written = write_run_manifest(&store, &location, manifest(location.clone())).unwrap();
        assert!(location
            .manifest_relative()
            .to_string_lossy()
            .contains("run-run-with-special-"));
        assert!(!written.content_hash.is_empty());
        assert_eq!(read_run_manifest(&store, &location).unwrap(), written);
    }

    #[test]
    fn finds_historical_run_by_manifest_identity_not_path_component() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        write_run_manifest(&store, &location, manifest(location.clone())).unwrap();
        assert_eq!(
            find_run_location(&store, "run/with special characters").unwrap(),
            Some(location)
        );
        assert_eq!(find_run_location(&store, "missing").unwrap(), None);
    }

    #[test]
    fn manifest_records_only_finalized_artifact_references() {
        let location = location();
        let mut manifest = manifest(location);
        let artifact = FinalizedArtifactRef::new(
            "artifact-1",
            std::path::Path::new("artifacts/phase1/technical.json"),
            1,
            "analyst.technical",
            "analyst_report",
            "technical/QQQ",
            "sha256:source",
            "2026-07-27T00:01:00Z",
        )
        .unwrap();
        manifest
            .record_finalized_artifact(artifact.clone())
            .unwrap();
        manifest.record_finalized_artifact(artifact).unwrap();
        assert_eq!(manifest.artifacts.len(), 1);
        assert_eq!(manifest.current_phase, 1);
    }

    #[test]
    fn malformed_workflow_date_is_rejected_before_path_construction() {
        assert!(RunLocation::new("2026/07/27", "run").is_err());
    }
}
