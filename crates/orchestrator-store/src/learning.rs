//! File-backed learning records.
//!
//! These are deliberately independent records instead of a mutable task queue
//! or a memory/version table.  A completed file is the completion marker.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ContentHashDocument, FileSchemaKind, FileStore, Result, RunLocation, SafeSlug, StoreError,
    Versioned,
};

pub const LEARNING_RECORD_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LearningKind {
    Decision,
    Outcome,
    Reflection,
}

impl LearningKind {
    fn directory(self) -> &'static str {
        match self {
            Self::Decision => "decisions",
            Self::Outcome => "outcomes",
            Self::Reflection => "reflections",
        }
    }

    fn schema_kind(self) -> FileSchemaKind {
        match self {
            Self::Decision => FileSchemaKind::Decision,
            Self::Outcome => FileSchemaKind::Outcome,
            Self::Reflection => FileSchemaKind::Reflection,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LearningRecord {
    pub schema_version: u32,
    pub kind: LearningKind,
    pub run_id: String,
    pub ticker: String,
    /// Immutable source run for an outcome/reflection.  A current decision
    /// leaves this unset rather than inventing a relation.
    pub source_run_id: Option<String>,
    pub payload: Value,
    pub created_at: String,
    pub content_hash: String,
}

impl Versioned for LearningRecord {
    const SCHEMA_VERSION: u32 = LEARNING_RECORD_SCHEMA_VERSION;
}

impl ContentHashDocument for LearningRecord {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

impl LearningRecord {
    pub fn validate(&self, location: &RunLocation, expected: LearningKind) -> Result<()> {
        if self.schema_version != LEARNING_RECORD_SCHEMA_VERSION
            || self.kind != expected
            || self.run_id != location.run_id
            || self.ticker.trim().is_empty()
            || self.created_at.trim().is_empty()
            || !self.payload.is_object()
        {
            return Err(StoreError::InvalidDocument {
                kind: "learning record",
                message: "kind, run identity, ticker, payload, or schema is invalid".to_owned(),
            });
        }
        Ok(())
    }
}

pub fn learning_record_relative(
    location: &RunLocation,
    kind: LearningKind,
    ticker: &str,
) -> Result<PathBuf> {
    if ticker.trim().is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "learning record",
            message: "ticker must not be empty".to_owned(),
        });
    }
    location.child_relative(
        &PathBuf::from("learning")
            .join(kind.directory())
            .join(format!(
                "{}.json",
                SafeSlug::new("ticker", ticker)?.as_str()
            )),
    )
}

pub fn write_learning_record(
    store: &FileStore,
    location: &RunLocation,
    kind: LearningKind,
    record: LearningRecord,
) -> Result<LearningRecord> {
    record.validate(location, kind)?;
    store.write_authoritative_json(
        &learning_record_relative(location, kind, &record.ticker)?,
        record,
    )
}

pub fn read_learning_record(
    store: &FileStore,
    location: &RunLocation,
    kind: LearningKind,
    ticker: &str,
) -> Result<LearningRecord> {
    let record: LearningRecord = store.read_versioned_json(
        &learning_record_relative(location, kind, ticker)?,
        kind.schema_kind(),
    )?;
    record.validate(location, kind)?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::{FileStore, FileStoreOptions};

    #[test]
    fn completed_learning_file_is_typed_hash_sealed_and_ticker_safe() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "run/with special id").unwrap();
        let record = LearningRecord {
            schema_version: LEARNING_RECORD_SCHEMA_VERSION,
            kind: LearningKind::Reflection,
            run_id: location.run_id.clone(),
            ticker: "QQQ/../unsafe".to_owned(),
            source_run_id: Some("prior-run".to_owned()),
            payload: json!({"classification":"correct_logic_bad_timing"}),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
            content_hash: String::new(),
        };
        let written =
            write_learning_record(&store, &location, LearningKind::Reflection, record).unwrap();
        assert!(!written.content_hash.is_empty());
        assert!(
            learning_record_relative(&location, LearningKind::Reflection, &written.ticker)
                .unwrap()
                .to_string_lossy()
                .contains("ticker-qqq-unsafe-")
        );
        assert_eq!(
            read_learning_record(&store, &location, LearningKind::Reflection, &written.ticker)
                .unwrap(),
            written
        );
    }
}
