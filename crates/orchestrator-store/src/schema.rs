use std::{fmt, path::Path};

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{Result, StoreError};

/// Identifies the on-disk schema family in diagnostics and migration tooling.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FileSchemaKind {
    RunManifest,
    RunState,
    Artifact(String),
    Draft(String),
    SessionManifest,
    SessionEvent,
    Index,
    Detail,
    InputSnapshotManifest,
    DataFileMetadata,
    Allocation,
    DecisionSnapshot,
    OutcomeRecord,
    OutcomeRevisionCommit,
    OutcomeHead,
    OutcomeWriteReceipt,
    MaterializationGap,
    MaterializationIntegrityIssue,
    MaterializationBatchReport,
    EvaluationInputManifest,
    ReflectionTask,
    HistoricalReflectionArtifact,
    ExperienceView,
    MemoryUsageReport,
    MemoryAttribution,
}

impl fmt::Display for FileSchemaKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RunManifest => f.write_str("run manifest"),
            Self::RunState => f.write_str("run state"),
            Self::Artifact(profile) => write!(f, "artifact:{profile}"),
            Self::Draft(profile) => write!(f, "draft:{profile}"),
            Self::SessionManifest => f.write_str("session manifest"),
            Self::SessionEvent => f.write_str("session event"),
            Self::Index => f.write_str("index"),
            Self::Detail => f.write_str("detail"),
            Self::InputSnapshotManifest => f.write_str("input snapshot manifest"),
            Self::DataFileMetadata => f.write_str("data file metadata"),
            Self::Allocation => f.write_str("allocation"),
            Self::DecisionSnapshot => f.write_str("decision snapshot"),
            Self::OutcomeRecord => f.write_str("outcome record"),
            Self::OutcomeRevisionCommit => f.write_str("outcome revision commit"),
            Self::OutcomeHead => f.write_str("outcome head"),
            Self::OutcomeWriteReceipt => f.write_str("outcome write receipt"),
            Self::MaterializationGap => f.write_str("materialization gap"),
            Self::MaterializationIntegrityIssue => f.write_str("materialization integrity issue"),
            Self::MaterializationBatchReport => f.write_str("materialization batch report"),
            Self::EvaluationInputManifest => f.write_str("evaluation input manifest"),
            Self::ReflectionTask => f.write_str("reflection task"),
            Self::HistoricalReflectionArtifact => f.write_str("historical reflection artifact"),
            Self::ExperienceView => f.write_str("experience view"),
            Self::MemoryUsageReport => f.write_str("memory usage report"),
            Self::MemoryAttribution => f.write_str("memory attribution"),
        }
    }
}

/// Current-version models opt into strict file-version validation.
pub trait Versioned {
    const SCHEMA_VERSION: u32;
}

pub(crate) fn deserialize_current<T: DeserializeOwned>(value: Value, path: &Path) -> Result<T> {
    serde_json::from_value(value).map_err(|source| StoreError::Json {
        path: path.to_path_buf(),
        source,
    })
}

pub(crate) fn validate_schema_version(
    value: &Value,
    path: &Path,
    kind: &FileSchemaKind,
    current: u32,
) -> Result<()> {
    let raw_version =
        value
            .get("schema_version")
            .ok_or_else(|| StoreError::MissingSchemaVersion {
                kind: kind.to_string(),
                path: path.to_path_buf(),
            })?;
    let found = raw_version
        .as_u64()
        .and_then(|version| u32::try_from(version).ok())
        .ok_or_else(|| StoreError::InvalidSchemaVersion {
            kind: kind.to_string(),
            path: path.to_path_buf(),
        })?;
    if found == 0 {
        return Err(StoreError::InvalidSchemaVersion {
            kind: kind.to_string(),
            path: path.to_path_buf(),
        });
    }
    if found > current {
        return Err(StoreError::UnsupportedFutureSchema {
            kind: kind.to_string(),
            path: path.to_path_buf(),
            found,
            current,
        });
    }
    if found < current {
        return Err(StoreError::MigrationRequired {
            kind: kind.to_string(),
            path: path.to_path_buf(),
            found,
            current,
        });
    }
    Ok(())
}
