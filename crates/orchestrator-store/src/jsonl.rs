use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::{
    atomic::sync_directory, canonical_json_bytes, content_hash, error::io_error,
    paths::resolve_for_write, Result, StoreError,
};

pub const SESSION_EVENT_SCHEMA_VERSION: u32 = 1;

pub trait JsonlRecord: Serialize + DeserializeOwned {
    const SCHEMA_VERSION: u32;

    fn schema_version(&self) -> u32;
    fn sequence(&self) -> u64;
    fn validate_record(&self) -> std::result::Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonlEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub event_type: String,
    pub turn_id: String,
    pub role: String,
    pub phase: u8,
    pub payload: Value,
    pub created_at: String,
    pub content_hash: String,
}

impl JsonlEvent {
    pub fn new(
        sequence: u64,
        event_type: impl Into<String>,
        turn_id: impl Into<String>,
        role: impl Into<String>,
        phase: u8,
        payload: Value,
        created_at: impl Into<String>,
    ) -> Result<Self> {
        let mut event = Self {
            schema_version: SESSION_EVENT_SCHEMA_VERSION,
            sequence,
            event_type: event_type.into(),
            turn_id: turn_id.into(),
            role: role.into(),
            phase,
            payload,
            created_at: created_at.into(),
            content_hash: String::new(),
        };
        event.content_hash = content_hash(
            &serde_json::to_value(&event).map_err(|source| StoreError::JsonSerialize { source })?,
        )?;
        Ok(event)
    }
}

impl JsonlRecord for JsonlEvent {
    const SCHEMA_VERSION: u32 = SESSION_EVENT_SCHEMA_VERSION;

    fn schema_version(&self) -> u32 {
        self.schema_version
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn validate_record(&self) -> std::result::Result<(), String> {
        if self.sequence == 0 {
            return Err("sequence must start at 1".to_owned());
        }
        let value = serde_json::to_value(self).map_err(|error| error.to_string())?;
        let expected = content_hash(&value).map_err(|error| error.to_string())?;
        if self.content_hash != expected {
            return Err(format!(
                "expected content hash {expected}, found {}",
                self.content_hash
            ));
        }
        Ok(())
    }
}

/// Append exactly one checked record. Existing records are validated first,
/// which makes a corrupt middle line a hard failure instead of silently
/// skipping history.
pub fn append_jsonl<R: JsonlRecord>(root: &Path, relative: &Path, record: &R) -> Result<()> {
    let _lock = crate::lock::lock_exclusive(root, relative)?;
    let path = resolve_for_write(root, relative)?;
    let parent = path
        .parent()
        .expect("resolved JSONL path has a parent")
        .to_path_buf();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| io_error(&path, source))?;
    let records = read_jsonl_from_locked_file::<R>(&mut file, &path, true)?;
    append_record(file, &path, &parent, &records, record)
}

/// Read, decide, and append one JSONL record while holding the file's
/// path-scoped exclusive lock.  This is the primitive for idempotent ledgers
/// whose record identity and sequence depend on their current history.
/// Returning `None` performs no write and leaves the validated history intact.
pub(crate) fn append_jsonl_transaction<R: JsonlRecord>(
    root: &Path,
    relative: &Path,
    build: impl FnOnce(&[R]) -> Result<Option<R>>,
) -> Result<Option<R>> {
    let _lock = crate::lock::lock_exclusive(root, relative)?;
    let path = resolve_for_write(root, relative)?;
    let parent = path
        .parent()
        .expect("resolved JSONL path has a parent")
        .to_path_buf();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| io_error(&path, source))?;
    let records = read_jsonl_from_locked_file::<R>(&mut file, &path, true)?;
    let Some(record) = build(&records)? else {
        return Ok(None);
    };
    append_record(file, &path, &parent, &records, &record)?;
    Ok(Some(record))
}

fn append_record<R: JsonlRecord>(
    mut file: File,
    path: &Path,
    parent: &Path,
    records: &[R],
    record: &R,
) -> Result<()> {
    let expected = records.last().map_or(1, |last| last.sequence() + 1);
    if record.sequence() != expected {
        return Err(StoreError::JsonlSequence {
            path: path.to_path_buf(),
            expected,
            found: record.sequence(),
        });
    }
    validate_record(record, path)?;

    let value =
        serde_json::to_value(record).map_err(|source| StoreError::JsonSerialize { source })?;
    let mut bytes = canonical_json_bytes(&value)?;
    bytes.push(b'\n');
    file.seek(SeekFrom::End(0))
        .map_err(|source| io_error(path, source))?;
    file.write_all(&bytes)
        .map_err(|source| io_error(path, source))?;
    file.flush().map_err(|source| io_error(path, source))?;
    file.sync_all().map_err(|source| io_error(path, source))?;
    drop(file);
    sync_directory(parent)
}

/// Read JSONL after repairing at most one unterminated final line. Any
/// newline-terminated malformed line, invalid hash, or sequence discontinuity
/// is a hard failure.
pub fn read_jsonl_recover_tail<R: JsonlRecord>(root: &Path, relative: &Path) -> Result<Vec<R>> {
    let _lock = crate::lock::lock_exclusive(root, relative)?;
    let path = resolve_for_write(root, relative)?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|source| io_error(&path, source))?;
    let records = read_jsonl_from_locked_file::<R>(&mut file, &path, true)?;
    drop(file);
    Ok(records)
}

/// Read JSONL without mutating the file. Unlike [`read_jsonl_recover_tail`],
/// an unterminated final record is reported as corruption instead of being
/// truncated. Use this for inspection and validation paths.
pub fn read_jsonl_strict<R: JsonlRecord>(root: &Path, relative: &Path) -> Result<Vec<R>> {
    let _lock = crate::lock::lock_shared(root, relative)?;
    let path = resolve_for_write(root, relative)?;
    let mut file = File::open(&path).map_err(|source| io_error(&path, source))?;
    read_jsonl_from_locked_file::<R>(&mut file, &path, false)
}

fn read_jsonl_from_locked_file<R: JsonlRecord>(
    file: &mut File,
    path: &Path,
    repair_tail: bool,
) -> Result<Vec<R>> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| io_error(path, source))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| io_error(path, source))?;

    let complete_len = match bytes.iter().rposition(|byte| *byte == b'\n') {
        Some(position) => position + 1,
        None if bytes.is_empty() => 0,
        None => 0,
    };
    if complete_len < bytes.len() {
        if !repair_tail {
            return Err(StoreError::JsonlHash {
                path: path.to_path_buf(),
                message: "unterminated final JSONL record".to_owned(),
            });
        }
        file.set_len(complete_len as u64)
            .map_err(|source| io_error(path, source))?;
        file.sync_all().map_err(|source| io_error(path, source))?;
        if let Some(parent) = path.parent() {
            sync_directory(parent)?;
        }
        bytes.truncate(complete_len);
    }

    let mut records = Vec::new();
    for (expected, line) in (1..).zip(bytes.split_inclusive(|byte| *byte == b'\n')) {
        let line = &line[..line.len() - 1];
        if line.is_empty() {
            return Err(StoreError::JsonlHash {
                path: path.to_path_buf(),
                message: "empty newline-terminated record".to_owned(),
            });
        }
        let record = serde_json::from_slice::<R>(line).map_err(|source| StoreError::Json {
            path: path.to_path_buf(),
            source,
        })?;
        if record.sequence() != expected {
            return Err(StoreError::JsonlSequence {
                path: path.to_path_buf(),
                expected,
                found: record.sequence(),
            });
        }
        validate_record(&record, path)?;
        records.push(record);
    }
    Ok(records)
}

fn validate_record<R: JsonlRecord>(record: &R, path: &Path) -> Result<()> {
    let found = record.schema_version();
    if found > R::SCHEMA_VERSION {
        return Err(StoreError::JsonlFutureSchema {
            path: path.to_path_buf(),
            found,
            current: R::SCHEMA_VERSION,
        });
    }
    if found < R::SCHEMA_VERSION {
        return Err(StoreError::JsonlMigrationRequired {
            path: path.to_path_buf(),
            found,
            current: R::SCHEMA_VERSION,
        });
    }
    record
        .validate_record()
        .map_err(|message| StoreError::JsonlHash {
            path: path.to_path_buf(),
            message,
        })
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use serde_json::json;
    use tempfile::tempdir;

    use super::{append_jsonl, read_jsonl_recover_tail, read_jsonl_strict, JsonlEvent};
    use crate::StoreError;

    fn event(sequence: u64) -> JsonlEvent {
        JsonlEvent::new(
            sequence,
            "tool_result",
            "turn-1",
            "analyst.technical",
            1,
            json!({"id": sequence}),
            "2026-07-27T00:00:00Z",
        )
        .unwrap()
    }

    #[test]
    fn append_and_read_validate_sequence_and_hash() {
        let directory = tempdir().unwrap();
        let path = Path::new("session/turn.jsonl");
        append_jsonl(directory.path(), path, &event(1)).unwrap();
        append_jsonl(directory.path(), path, &event(2)).unwrap();
        assert_eq!(
            read_jsonl_recover_tail::<JsonlEvent>(directory.path(), path)
                .unwrap()
                .len(),
            2
        );
        assert!(append_jsonl(directory.path(), path, &event(4)).is_err());
    }

    #[test]
    fn recovery_discards_only_unterminated_tail() {
        let directory = tempdir().unwrap();
        let path = Path::new("session/turn.jsonl");
        append_jsonl(directory.path(), path, &event(1)).unwrap();
        let absolute = directory.path().join(path);
        fs::write(
            &absolute,
            [fs::read(&absolute).unwrap(), b"{partial".to_vec()].concat(),
        )
        .unwrap();

        let events = read_jsonl_recover_tail::<JsonlEvent>(directory.path(), path).unwrap();
        assert_eq!(events.len(), 1);
        assert!(fs::read(&absolute).unwrap().ends_with(b"\n"));
    }

    #[test]
    fn strict_read_reports_an_unterminated_tail_without_truncating_it() {
        let directory = tempdir().unwrap();
        let path = Path::new("session/turn.jsonl");
        append_jsonl(directory.path(), path, &event(1)).unwrap();
        let absolute = directory.path().join(path);
        let before = [fs::read(&absolute).unwrap(), b"{partial".to_vec()].concat();
        fs::write(&absolute, &before).unwrap();

        assert!(read_jsonl_strict::<JsonlEvent>(directory.path(), path).is_err());
        assert_eq!(fs::read(&absolute).unwrap(), before);
    }

    #[test]
    fn malformed_middle_line_is_a_hard_failure() {
        let directory = tempdir().unwrap();
        let path = Path::new("session/turn.jsonl");
        append_jsonl(directory.path(), path, &event(1)).unwrap();
        let absolute = directory.path().join(path);
        fs::write(
            &absolute,
            [fs::read(&absolute).unwrap(), b"{bad}\n".to_vec()].concat(),
        )
        .unwrap();
        assert!(read_jsonl_recover_tail::<JsonlEvent>(directory.path(), path).is_err());
    }

    #[test]
    fn tampered_hash_is_a_hard_failure() {
        let directory = tempdir().unwrap();
        let path = Path::new("session/turn.jsonl");
        append_jsonl(directory.path(), path, &event(1)).unwrap();
        let absolute = directory.path().join(path);
        let changed = fs::read_to_string(&absolute)
            .unwrap()
            .replace("tool_result", "tool_result_changed");
        fs::write(&absolute, changed).unwrap();
        assert!(read_jsonl_recover_tail::<JsonlEvent>(directory.path(), path).is_err());
    }

    #[test]
    fn event_schema_versions_never_default_or_downgrade() {
        let directory = tempdir().unwrap();
        let path = Path::new("session/turn.jsonl");
        let mut future = event(1);
        future.schema_version = 2;
        assert!(matches!(
            append_jsonl(directory.path(), path, &future),
            Err(StoreError::JsonlFutureSchema {
                found: 2,
                current: 1,
                ..
            })
        ));

        let mut old = event(1);
        old.schema_version = 0;
        assert!(matches!(
            append_jsonl(directory.path(), path, &old),
            Err(StoreError::JsonlMigrationRequired {
                found: 0,
                current: 1,
                ..
            })
        ));
    }
}
