//! Pure file enumeration and envelope/schema checks used by Store Doctor.
//!
//! This module deliberately reports findings through the parent Doctor report
//! and performs no repair, compaction, or other writes.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

use orchestrator_core::MemoryUsageEventV1;

use super::StoreDoctorReport;
use crate::{
    read_jsonl_strict, validate_content_hash_at, validate_relative_path, ExperienceEventV1,
    FileStore, JsonlEvent, ReflectionTaskEventV1, Result, SessionEvent, StoreError,
};

pub(super) fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
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
            if path == root.join(".locks") {
                continue;
            }
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

pub(super) fn inspect_file_envelope(
    store: &FileStore,
    relative: &Path,
    report: &mut StoreDoctorReport,
) {
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
        let parsed = if relative
            .components()
            .any(|component| component.as_os_str() == "sessions")
        {
            read_jsonl_strict::<SessionEvent>(store.root(), relative).map(|_| ())
        } else if relative
            .components()
            .any(|component| component.as_os_str() == "reflection")
        {
            read_jsonl_strict::<ReflectionTaskEventV1>(store.root(), relative).map(|_| ())
        } else if relative
            .components()
            .any(|component| component.as_os_str() == "experiences")
        {
            read_jsonl_strict::<ExperienceEventV1>(store.root(), relative).map(|_| ())
        } else if relative
            .components()
            .any(|component| component.as_os_str() == "memory")
        {
            read_jsonl_strict::<MemoryUsageEventV1>(store.root(), relative).map(|_| ())
        } else {
            read_jsonl_strict::<JsonlEvent>(store.root(), relative).map(|_| ())
        };
        if let Err(error) = parsed {
            report.issue("malformed_jsonl", relative, error.to_string());
        }
    }
}

pub(super) fn validate_generic_document(value: &Value, path: &Path) -> Result<()> {
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
