use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use serde::Serialize;

use crate::{
    canonical_json_bytes,
    error::io_error,
    paths::{resolve_existing, resolve_for_write},
    Result, StoreError,
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtomicWriteOptions {
    pub fsync: bool,
}

impl Default for AtomicWriteOptions {
    fn default() -> Self {
        Self { fsync: true }
    }
}

pub fn write_bytes_atomic(root: &Path, relative: &Path, bytes: &[u8]) -> Result<()> {
    write_bytes_atomic_with_options(root, relative, bytes, AtomicWriteOptions::default())
}

/// Atomically publish one explicit output path through the same FileStore
/// implementation used by run artifacts. The caller owns path selection;
/// this helper never accepts a directory traversal component as the filename.
pub fn publish_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| StoreError::InvalidDocument {
        kind: "atomic publication path",
        message: "path must have a parent directory".to_owned(),
    })?;
    let file_name = path
        .file_name()
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "atomic publication path",
            message: "path must name a file".to_owned(),
        })?;
    fs::create_dir_all(parent).map_err(|source| io_error(parent, source))?;
    write_bytes_atomic(parent, Path::new(file_name), bytes)
}

/// Serialize canonical JSON and atomically replace its target file.
pub fn write_json_atomic<T: Serialize>(root: &Path, relative: &Path, value: &T) -> Result<()> {
    let value =
        serde_json::to_value(value).map_err(|source| StoreError::JsonSerialize { source })?;
    let bytes = canonical_json_bytes(&value)?;
    write_bytes_atomic(root, relative, &bytes)
}

pub fn write_bytes_atomic_with_options(
    root: &Path,
    relative: &Path,
    bytes: &[u8],
    options: AtomicWriteOptions,
) -> Result<()> {
    let target = resolve_for_write(root, relative)?;
    let parent = target.parent().expect("resolved file always has a parent");
    let (temp, mut file) = create_adjacent_temp_file(&target)?;

    let result = (|| -> Result<()> {
        file.write_all(bytes)
            .map_err(|source| io_error(&temp, source))?;
        file.flush().map_err(|source| io_error(&temp, source))?;
        if options.fsync {
            file.sync_all().map_err(|source| io_error(&temp, source))?;
        }
        drop(file);

        fs::rename(&temp, &target).map_err(|source| io_error(&target, source))?;
        if options.fsync {
            sync_directory(parent)?;
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Atomically move a complete, already-written directory within one store root.
/// The destination must not yet exist; replacement is deliberately forbidden.
pub fn rename_dir_atomic(
    root: &Path,
    source_relative: &Path,
    target_relative: &Path,
) -> Result<()> {
    let source = resolve_existing(root, source_relative)?;
    let target = resolve_for_write(root, target_relative)?;
    if !source.is_dir() {
        return Err(StoreError::ExpectedDirectory { path: source });
    }
    if target.exists() {
        return Err(StoreError::DestinationExists { path: target });
    }
    fs::rename(&source, &target).map_err(|source_error| io_error(&target, source_error))?;
    sync_directory(target.parent().expect("resolved target has parent"))?;
    Ok(())
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| io_error(path, source))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

pub(crate) fn adjacent_temp_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("store");
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    target.with_file_name(format!(".{file_name}.tmp-{}-{counter}", std::process::id()))
}

fn create_adjacent_temp_file(target: &Path) -> Result<(PathBuf, File)> {
    for _ in 0..32 {
        let temp = adjacent_temp_path(target);
        match OpenOptions::new().write(true).create_new(true).open(&temp) {
            Ok(file) => return Ok((temp, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(io_error(&temp, source)),
        }
    }
    let temp = adjacent_temp_path(target);
    Err(io_error(
        temp,
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique adjacent temporary file",
        ),
    ))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use tempfile::tempdir;

    use super::{rename_dir_atomic, write_bytes_atomic, write_json_atomic, AtomicWriteOptions};

    #[test]
    fn atomic_write_replaces_contents_without_temp_left_behind() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        write_bytes_atomic(root, Path::new("nested/data.json"), b"first").unwrap();
        write_bytes_atomic(root, Path::new("nested/data.json"), b"second").unwrap();
        assert_eq!(fs::read(root.join("nested/data.json")).unwrap(), b"second");
        assert_eq!(fs::read_dir(root.join("nested")).unwrap().count(), 1);
    }

    #[test]
    fn atomic_write_can_disable_fsync_for_test_or_ephemeral_data() {
        let directory = tempdir().unwrap();
        write_bytes_atomic(directory.path(), Path::new("data.json"), b"ok").unwrap();
        super::write_bytes_atomic_with_options(
            directory.path(),
            Path::new("data.json"),
            b"still-ok",
            AtomicWriteOptions { fsync: false },
        )
        .unwrap();
        assert_eq!(
            fs::read(directory.path().join("data.json")).unwrap(),
            b"still-ok"
        );
    }

    #[test]
    fn atomic_json_write_uses_canonical_key_order() {
        let directory = tempdir().unwrap();
        write_json_atomic(
            directory.path(),
            Path::new("data.json"),
            &serde_json::json!({"z": 1, "a": {"b": 2, "a": 1}}),
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(directory.path().join("data.json")).unwrap(),
            r#"{"a":{"a":1,"b":2},"z":1}"#
        );
    }

    #[test]
    fn atomic_directory_rename_refuses_replacement() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        fs::create_dir_all(root.join("staging/index")).unwrap();
        fs::write(root.join("staging/index/index.json"), b"{}").unwrap();
        rename_dir_atomic(root, Path::new("staging/index"), Path::new("final/index")).unwrap();
        assert!(root.join("final/index/index.json").exists());

        fs::create_dir_all(root.join("staging/other")).unwrap();
        assert!(
            rename_dir_atomic(root, Path::new("staging/other"), Path::new("final/index")).is_err()
        );
    }

    #[test]
    fn independent_units_can_write_in_parallel() {
        let directory = tempdir().unwrap();
        std::thread::scope(|scope| {
            for unit in 0..16 {
                let root = directory.path();
                scope.spawn(move || {
                    write_bytes_atomic(
                        root,
                        Path::new(&format!("artifacts/unit-{unit}.json")),
                        format!("{{\"unit\":{unit}}}").as_bytes(),
                    )
                    .unwrap();
                });
            }
        });
        assert_eq!(
            fs::read_dir(directory.path().join("artifacts"))
                .unwrap()
                .count(),
            16
        );
    }

    #[cfg(unix)]
    #[test]
    fn write_rejects_symlinked_store_component() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let outside = tempdir().unwrap();
        symlink(outside.path(), directory.path().join("escape")).unwrap();
        assert!(write_bytes_atomic(
            directory.path(),
            Path::new("escape/data.json"),
            b"must-not-write",
        )
        .is_err());
        assert!(!outside.path().join("data.json").exists());
    }
}
