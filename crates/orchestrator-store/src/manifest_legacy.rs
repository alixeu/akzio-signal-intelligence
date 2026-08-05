use std::{collections::BTreeMap, path::Path};

use serde::Deserialize;
use serde_json::Value;

use super::{FinalizedArtifactRef, ManifestError, PhaseStatus, RunManifest, RunStatus};
use crate::{seal_content_hash, Result, StoreError};

/// The first FileStore manifest used `authority_registry_hash`. Schema v2
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
    #[serde(default)]
    pub prompt_content_hash: String,
    #[serde(default)]
    pub source_surface_hash: String,
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
            schema_version: super::RUN_MANIFEST_SCHEMA_VERSION,
            run_id: legacy.run_id,
            current_date: legacy.current_date,
            storage_namespace: None,
            status: legacy.status,
            current_phase: legacy.current_phase,
            phase_status: legacy.phase_status,
            workflow_version: legacy.workflow_version,
            prompt_versions: legacy.prompt_versions,
            prompt_content_hash: legacy.prompt_content_hash,
            source_surface_hash: legacy.source_surface_hash,
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

pub(super) fn read(value: Value, path: &Path) -> Result<RunManifest> {
    serde_json::from_value::<LegacyRunManifestV1>(value)
        .map_err(|source| StoreError::Json {
            path: path.to_owned(),
            source,
        })?
        .try_into()
}
