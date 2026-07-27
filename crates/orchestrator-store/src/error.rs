use std::{io, path::PathBuf};

use thiserror::Error;

pub type Result<T, E = StoreError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("store I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("unsafe relative path `{path}`: {reason}")]
    UnsafeRelativePath { path: PathBuf, reason: &'static str },

    #[error("store path contains a symbolic link: {path}")]
    SymlinkPath { path: PathBuf },

    #[error("safe slug kind `{kind}` is invalid: {reason}")]
    InvalidSlugKind { kind: String, reason: &'static str },

    #[error("safe slug `{slug}` does not match original value for kind `{kind}`")]
    SlugMismatch { kind: String, slug: String },

    #[error("JSON parse failed at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    #[error("JSON serialize failed: {source}")]
    JsonSerialize {
        #[source]
        source: serde_json::Error,
    },

    #[error("{kind} file {path} is missing required schema_version")]
    MissingSchemaVersion { kind: String, path: PathBuf },

    #[error("{kind} file {path} has invalid schema_version")]
    InvalidSchemaVersion { kind: String, path: PathBuf },

    #[error(
        "{kind} file {path} uses unsupported future schema version {found}; this runtime supports {current}"
    )]
    UnsupportedFutureSchema {
        kind: String,
        path: PathBuf,
        found: u32,
        current: u32,
    },

    #[error(
        "{kind} file {path} uses old schema version {found}; explicit migration to {current} is required"
    )]
    MigrationRequired {
        kind: String,
        path: PathBuf,
        found: u32,
        current: u32,
    },

    #[error("content hash is missing from {path}")]
    MissingContentHash { path: PathBuf },

    #[error("content hash mismatch at {path}: expected {expected}, found {found}")]
    ContentHashMismatch {
        path: PathBuf,
        expected: String,
        found: String,
    },

    #[error("JSON document at {path} must be an object to carry content_hash")]
    ContentHashRequiresObject { path: PathBuf },

    #[error("JSONL record at {path} has invalid sequence: expected {expected}, found {found}")]
    JsonlSequence {
        path: PathBuf,
        expected: u64,
        found: u64,
    },

    #[error("JSONL record at {path} has unsupported future schema version {found}; current is {current}")]
    JsonlFutureSchema {
        path: PathBuf,
        found: u32,
        current: u32,
    },

    #[error("JSONL record at {path} requires explicit migration from schema version {found} to {current}")]
    JsonlMigrationRequired {
        path: PathBuf,
        found: u32,
        current: u32,
    },

    #[error("JSONL record at {path} has invalid content hash: {message}")]
    JsonlHash { path: PathBuf, message: String },

    #[error("atomic directory target already exists: {path}")]
    DestinationExists { path: PathBuf },

    #[error("expected directory at {path}")]
    ExpectedDirectory { path: PathBuf },

    #[error("invalid {kind}: {message}")]
    InvalidDocument { kind: &'static str, message: String },

    #[error("invalid draft lifecycle transition from `{from}` to `{to}`")]
    InvalidDraftTransition { from: String, to: String },

    #[error("finalized artifact at {path} does not match draft scope: {message}")]
    FinalizedArtifactMismatch { path: PathBuf, message: String },
}

pub(crate) fn io_error(path: impl Into<PathBuf>, source: io::Error) -> StoreError {
    StoreError::Io {
        path: path.into(),
        source,
    }
}
