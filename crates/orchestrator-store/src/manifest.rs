use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    seal_content_hash, validate_content_hash_at, validate_relative_path, ContentHashDocument,
    FileStore, Result, SafeSlug, StoreError, Versioned,
};

pub const RUN_MANIFEST_SCHEMA_VERSION: u32 = 2;

/// Stable store location for one workflow run. Normal runs use their human
/// workflow date as the partition; the debug namespace deliberately uses one
/// date-independent partition so an operator can resume the same diagnostic
/// run across calendar days.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLocation {
    pub current_date: String,
    pub run_id: String,
    run_slug: String,
    storage_namespace: Option<String>,
}

impl RunLocation {
    pub fn new(current_date: impl Into<String>, run_id: impl Into<String>) -> Result<Self> {
        Self::with_storage_namespace(current_date, run_id, None)
    }

    pub fn debug(current_date: impl Into<String>, run_id: impl Into<String>) -> Result<Self> {
        Self::with_storage_namespace(current_date, run_id, Some("debug".to_owned()))
    }

    pub fn with_storage_namespace(
        current_date: impl Into<String>,
        run_id: impl Into<String>,
        storage_namespace: Option<String>,
    ) -> Result<Self> {
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
        if storage_namespace
            .as_deref()
            .is_some_and(|namespace| namespace != "debug")
        {
            return Err(StoreError::InvalidDocument {
                kind: "run location",
                message: "storage_namespace must be `debug` when present".to_owned(),
            });
        }
        let run_slug = if run_id.len() <= 99
            && run_id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            run_id.clone()
        } else {
            SafeSlug::new("run", &run_id)?.as_str().to_owned()
        };
        Ok(Self {
            run_slug,
            current_date,
            run_id,
            storage_namespace,
        })
    }

    pub fn relative_root(&self) -> PathBuf {
        PathBuf::from("runs")
            .join(
                self.storage_namespace
                    .as_deref()
                    .unwrap_or(&self.current_date),
            )
            .join(self.run_slug.as_str())
    }

    pub fn manifest_relative(&self) -> PathBuf {
        self.relative_root().join("manifest.json")
    }

    pub fn state_relative(&self) -> PathBuf {
        self.relative_root().join("state.json")
    }

    pub fn run_slug(&self) -> &String {
        &self.run_slug
    }

    pub fn storage_namespace(&self) -> Option<&str> {
        self.storage_namespace.as_deref()
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
    pub role_profile_registry_hash: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunManifest {
    pub schema_version: u32,
    pub run_id: String,
    pub current_date: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_namespace: Option<String>,
    pub status: RunStatus,
    pub current_phase: u8,
    pub phase_status: BTreeMap<String, PhaseStatus>,
    pub workflow_version: String,
    pub prompt_versions: BTreeMap<String, String>,
    pub git_sha: String,
    pub config_hash: String,
    pub role_profile_registry_hash: String,
    pub degraded: bool,
    pub errors: Vec<ManifestError>,
    pub artifacts: BTreeMap<String, FinalizedArtifactRef>,
    pub summary_units: BTreeMap<String, String>,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub content_hash: String,
}

/// The first FileStore manifest used `authority_registry_hash`.  Schema v2
/// renames it to explain that it pins the ToolManaged profile registry. The
/// reader accepts only this known, hash-validated v1 shape and rewrites it on
/// the next normal authoritative manifest write; no generic best-effort
/// migration is permitted.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct LegacyRunManifestV1 {
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
    #[serde(default)]
    pub role_profile_registry_hash: String,
    #[serde(default)]
    pub authority_registry_hash: String,
    pub degraded: bool,
    pub errors: Vec<ManifestError>,
    pub artifacts: BTreeMap<String, FinalizedArtifactRef>,
    pub summary_units: BTreeMap<String, String>,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub content_hash: String,
}

impl TryFrom<LegacyRunManifestV1> for RunManifest {
    type Error = StoreError;

    fn try_from(legacy: LegacyRunManifestV1) -> Result<Self> {
        if legacy.schema_version != 1 {
            return Err(StoreError::InvalidDocument {
                kind: "run manifest",
                message: "legacy manifest schema_version is invalid".to_owned(),
            });
        }
        let role_profile_registry_hash = if !legacy.role_profile_registry_hash.is_empty() {
            legacy.role_profile_registry_hash
        } else {
            legacy.authority_registry_hash
        };
        if role_profile_registry_hash.is_empty() {
            return Err(StoreError::InvalidDocument {
                kind: "run manifest",
                message: "legacy manifest has no authority registry hash".to_owned(),
            });
        }
        seal_content_hash(RunManifest {
            schema_version: RUN_MANIFEST_SCHEMA_VERSION,
            run_id: legacy.run_id,
            current_date: legacy.current_date,
            storage_namespace: None,
            status: legacy.status,
            current_phase: legacy.current_phase,
            phase_status: legacy.phase_status,
            workflow_version: legacy.workflow_version,
            prompt_versions: legacy.prompt_versions,
            git_sha: legacy.git_sha,
            config_hash: legacy.config_hash,
            role_profile_registry_hash,
            degraded: legacy.degraded,
            errors: legacy.errors,
            artifacts: legacy.artifacts,
            summary_units: legacy.summary_units,
            created_at: legacy.created_at,
            completed_at: legacy.completed_at,
            content_hash: String::new(),
        })
    }
}

impl RunManifest {
    pub fn new(init: RunManifestInit) -> Result<Self> {
        for (name, value) in [
            ("workflow_version", &init.workflow_version),
            ("git_sha", &init.git_sha),
            ("config_hash", &init.config_hash),
            (
                "role_profile_registry_hash",
                &init.role_profile_registry_hash,
            ),
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
            storage_namespace: init.location.storage_namespace.clone(),
            status: RunStatus::Running,
            current_phase: 0,
            phase_status: BTreeMap::new(),
            workflow_version: init.workflow_version,
            prompt_versions: init.prompt_versions,
            git_sha: init.git_sha,
            config_hash: init.config_hash,
            role_profile_registry_hash: init.role_profile_registry_hash,
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
        RunLocation::with_storage_namespace(
            self.current_date.clone(),
            self.run_id.clone(),
            self.storage_namespace.clone(),
        )
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
        let same_debug_namespace = self.storage_namespace.as_deref() == Some("debug")
            && location.storage_namespace() == Some("debug");
        if self.run_id != location.run_id
            || (!same_debug_namespace && self.current_date != location.current_date)
            || self.storage_namespace.as_deref() != location.storage_namespace()
        {
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
    let manifest = read_manifest_relative(store, &location.manifest_relative())?;
    manifest.validate_for_location(location)?;
    Ok(manifest)
}

pub(crate) fn read_manifest_relative(store: &FileStore, relative: &Path) -> Result<RunManifest> {
    let value: Value = store.read_json_value(relative)?;
    validate_content_hash_at(&value, &store.root().join(relative))?;
    let schema_version = value
        .get("schema_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "run manifest",
            message: "schema_version is required".to_owned(),
        })?;
    match u32::try_from(schema_version).ok() {
        Some(RUN_MANIFEST_SCHEMA_VERSION) => {
            serde_json::from_value(value).map_err(|source| StoreError::Json {
                path: store.root().join(relative),
                source,
            })
        }
        Some(1) => serde_json::from_value::<LegacyRunManifestV1>(value)
            .map_err(|source| StoreError::Json {
                path: store.root().join(relative),
                source,
            })?
            .try_into(),
        _ => Err(StoreError::InvalidDocument {
            kind: "run manifest",
            message: "schema_version is unsupported".to_owned(),
        }),
    }
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
        if !metadata.is_dir() || !is_run_partition(&date.file_name().to_string_lossy()) {
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
            let manifest = read_manifest_relative(store, &relative)?;
            if manifest.run_id != run_id {
                continue;
            }
            let location = manifest.location()?;
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

/// Return every valid FileStore run location in deterministic chronological
/// order. Callers still read each manifest/record explicitly; this is only a
/// safe discovery primitive for Rust-owned recovery and reflection planning.
pub fn list_run_locations(store: &FileStore) -> Result<Vec<RunLocation>> {
    let runs = store.root().join("runs");
    if !runs.exists() {
        return Ok(Vec::new());
    }
    let mut locations = Vec::new();
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
        if !metadata.is_dir() || !is_run_partition(&date.file_name().to_string_lossy()) {
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
            let manifest = read_manifest_relative(store, &relative)?;
            locations.push(manifest.location()?);
        }
    }
    locations.sort_by(|left, right| {
        left.current_date
            .cmp(&right.current_date)
            .then(left.run_id.cmp(&right.run_id))
    });
    locations.dedup();
    Ok(locations)
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

fn is_run_partition(value: &str) -> bool {
    value == "debug" || is_workflow_date(value)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf};

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
            role_profile_registry_hash: "sha256:authority".to_owned(),
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
    fn generated_run_id_is_the_directory_name() {
        let location = RunLocation::new("2026-07-27", "qqq-soxx-vix-a1b2c3").unwrap();
        assert_eq!(
            location.relative_root(),
            PathBuf::from("runs/2026-07-27/qqq-soxx-vix-a1b2c3")
        );
    }

    #[test]
    fn debug_location_uses_a_date_independent_partition() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::debug("2026-07-31", "qqq-soxx-vix-debug").unwrap();
        assert_eq!(location.current_date, "2026-07-31");
        assert_eq!(location.storage_namespace(), Some("debug"));
        assert_eq!(
            location.relative_root(),
            PathBuf::from("runs/debug/qqq-soxx-vix-debug")
        );
        assert_eq!(manifest(location.clone()).location().unwrap(), location);
        write_run_manifest(&store, &location, manifest(location.clone())).unwrap();
        let resumed = RunLocation::debug("2026-08-01", "qqq-soxx-vix-debug").unwrap();
        assert_eq!(
            read_run_manifest(&store, &resumed).unwrap().run_id,
            resumed.run_id
        );
    }

    #[test]
    fn legacy_authority_registry_hash_is_read_and_normalized_to_schema_v2() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let current = manifest(location.clone());
        let mut value = serde_json::to_value(current).unwrap();
        value["schema_version"] = serde_json::json!(1);
        let object = value.as_object_mut().unwrap();
        let registry_hash = object
            .remove("role_profile_registry_hash")
            .expect("v2 fixture has registry hash");
        object.insert("authority_registry_hash".to_owned(), registry_hash);
        let value = crate::set_content_hash(&value).unwrap();
        store
            .write_json_value(&location.manifest_relative(), &value)
            .unwrap();

        let read = read_run_manifest(&store, &location).unwrap();
        assert_eq!(read.schema_version, super::RUN_MANIFEST_SCHEMA_VERSION);
        assert_eq!(read.role_profile_registry_hash, "sha256:authority");
        assert!(!read.content_hash.is_empty());
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
