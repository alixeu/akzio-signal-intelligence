use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::{
    append_jsonl, atomic::write_bytes_atomic_with_options, canonical_json_bytes, error::io_error,
    jsonl::JsonlRecord, paths::resolve_existing, schema::deserialize_current,
    schema::validate_schema_version, validate_content_hash_at, AtomicWriteOptions,
    ContentHashDocument, FileSchemaKind, Result, StoreError, Versioned,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileStoreOptions {
    pub atomic_fsync: bool,
    /// `None` is useful only for tests or an explicit maintenance mode.
    /// Production startup supplies the configured stale-temp age.
    pub stale_temp_age: Option<Duration>,
}

impl Default for FileStoreOptions {
    fn default() -> Self {
        Self {
            atomic_fsync: true,
            stale_temp_age: Some(Duration::from_secs(3600)),
        }
    }
}

/// The sole persistence implementation used by the new orchestration path.
#[derive(Debug, Clone)]
pub struct FileStore {
    root: PathBuf,
    options: FileStoreOptions,
}

/// Read a JSON document beneath a previously initialized store root. Malformed
/// JSON remains an error and is never interpreted as an empty object.
pub fn read_json<T: DeserializeOwned>(root: &Path, relative: &Path) -> Result<T> {
    let path = resolve_existing(root, relative)?;
    let bytes = fs::read(&path).map_err(|source| io_error(&path, source))?;
    serde_json::from_slice(&bytes).map_err(|source| StoreError::Json { path, source })
}

/// Read arbitrary bytes beneath a previously initialized store root while
/// applying the same safe-relative and symlink checks as JSON readers.
pub fn read_bytes(root: &Path, relative: &Path) -> Result<Vec<u8>> {
    let path = resolve_existing(root, relative)?;
    fs::read(&path).map_err(|source| io_error(&path, source))
}

impl FileStore {
    pub fn open(root: impl AsRef<Path>, options: FileStoreOptions) -> Result<Self> {
        let requested = root.as_ref();
        if requested.as_os_str().is_empty() {
            return Err(StoreError::UnsafeRelativePath {
                path: requested.to_path_buf(),
                reason: "store root is empty",
            });
        }
        ensure_existing_ancestors_not_symlinks(requested)?;
        match fs::symlink_metadata(requested) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StoreError::SymlinkPath {
                    path: requested.to_path_buf(),
                });
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(StoreError::ExpectedDirectory {
                    path: requested.to_path_buf(),
                });
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(requested).map_err(|source| io_error(requested, source))?;
            }
            Err(source) => return Err(io_error(requested, source)),
        }
        let root = fs::canonicalize(requested).map_err(|source| io_error(requested, source))?;
        let store = Self { root, options };
        if let Some(age) = store.options.stale_temp_age {
            store.cleanup_stale_temps(age)?;
        }
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn write_json<T: Serialize>(&self, relative: &Path, value: &T) -> Result<()> {
        let value =
            serde_json::to_value(value).map_err(|source| StoreError::JsonSerialize { source })?;
        self.write_json_value(relative, &value)
    }

    pub fn write_bytes(&self, relative: &Path, bytes: &[u8]) -> Result<()> {
        write_bytes_atomic_with_options(
            &self.root,
            relative,
            bytes,
            AtomicWriteOptions {
                fsync: self.options.atomic_fsync,
            },
        )
    }

    pub fn write_json_value(&self, relative: &Path, value: &Value) -> Result<()> {
        let bytes = canonical_json_bytes(value)?;
        write_bytes_atomic_with_options(
            &self.root,
            relative,
            &bytes,
            AtomicWriteOptions {
                fsync: self.options.atomic_fsync,
            },
        )
    }

    /// Seal a typed authoritative document and atomically persist it. The
    /// returned value is the exact sealed document written to disk.
    pub fn write_authoritative_json<T: ContentHashDocument>(
        &self,
        relative: &Path,
        document: T,
    ) -> Result<T> {
        let document = crate::seal_content_hash(document)?;
        self.write_json(relative, &document)?;
        Ok(document)
    }

    pub fn read_json<T: DeserializeOwned>(&self, relative: &Path) -> Result<T> {
        read_json(&self.root, relative)
    }

    pub fn read_bytes(&self, relative: &Path) -> Result<Vec<u8>> {
        read_bytes(&self.root, relative)
    }

    pub fn read_json_value(&self, relative: &Path) -> Result<Value> {
        self.read_json(relative)
    }

    pub fn read_versioned_json<T: DeserializeOwned + Versioned>(
        &self,
        relative: &Path,
        kind: FileSchemaKind,
    ) -> Result<T> {
        let path = resolve_existing(&self.root, relative)?;
        let bytes = fs::read(&path).map_err(|source| io_error(&path, source))?;
        let value = serde_json::from_slice(&bytes).map_err(|source| StoreError::Json {
            path: path.clone(),
            source,
        })?;
        validate_schema_version(&value, &path, &kind, T::SCHEMA_VERSION)?;
        validate_content_hash_at(&value, &path)?;
        deserialize_current(value, &path)
    }

    pub fn append_jsonl<R: JsonlRecord>(&self, relative: &Path, record: &R) -> Result<()> {
        append_jsonl(&self.root, relative, record)
    }

    pub fn exists(&self, relative: &Path) -> Result<bool> {
        match resolve_existing(&self.root, relative) {
            Ok(path) => Ok(path.exists()),
            Err(StoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Remove stale files created by this crate's adjacent-temp naming scheme.
    /// Callers decide when startup cleanup is safe; this method never follows
    /// symbolic links.
    pub fn cleanup_stale_temps(&self, age: Duration) -> Result<Vec<PathBuf>> {
        let cutoff = SystemTime::now()
            .checked_sub(age)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mut removed = Vec::new();
        cleanup_directory(&self.root, &self.root, cutoff, &mut removed)?;
        Ok(removed)
    }
}

fn ensure_existing_ancestors_not_symlinks(path: &Path) -> Result<()> {
    // Inspect only the requested suffix. System roots can legitimately include
    // aliases such as macOS `/var`; they are outside the configured store
    // boundary. The first existing requested component is enough to reject a
    // root being introduced through a user-controlled symlink.
    for ancestor in path.ancestors() {
        if ancestor.as_os_str().is_empty() {
            break;
        }
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StoreError::SymlinkPath {
                    path: ancestor.to_path_buf(),
                });
            }
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(io_error(ancestor, source)),
        }
    }
    Ok(())
}

fn cleanup_directory(
    root: &Path,
    directory: &Path,
    cutoff: SystemTime,
    removed: &mut Vec<PathBuf>,
) -> Result<()> {
    for entry in fs::read_dir(directory).map_err(|source| io_error(directory, source))? {
        let entry = entry.map_err(|source| io_error(directory, source))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path });
        }
        if metadata.is_dir() {
            cleanup_directory(root, &path, cutoff, removed)?;
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let stale_temp = name.starts_with('.') && name.contains(".tmp-");
        let modified = metadata
            .modified()
            .map_err(|source| io_error(&path, source))?;
        if stale_temp && modified <= cutoff {
            fs::remove_file(&path).map_err(|source| io_error(&path, source))?;
            let relative = path
                .strip_prefix(root)
                .expect("recursion only walks within root")
                .to_path_buf();
            removed.push(relative);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, time::Duration};

    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    use super::{FileStore, FileStoreOptions};
    use crate::{FileSchemaKind, StoreError, Versioned};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct CurrentV2 {
        schema_version: u32,
        required: String,
        content_hash: String,
    }

    impl Versioned for CurrentV2 {
        const SCHEMA_VERSION: u32 = 2;
    }

    fn hashed(value: serde_json::Value) -> serde_json::Value {
        crate::set_content_hash(&value).unwrap()
    }

    #[test]
    fn versioned_reader_requires_explicit_migration_or_rejects_future() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let file = Path::new("record.json");

        store
            .write_json(
                file,
                &hashed(serde_json::json!({"schema_version": 1, "required": "old"})),
            )
            .unwrap();
        assert!(matches!(
            store.read_versioned_json::<CurrentV2>(file, FileSchemaKind::Index),
            Err(StoreError::MigrationRequired {
                found: 1,
                current: 2,
                ..
            })
        ));

        store
            .write_json(
                file,
                &hashed(serde_json::json!({"schema_version": 3, "required": "new"})),
            )
            .unwrap();
        assert!(matches!(
            store.read_versioned_json::<CurrentV2>(file, FileSchemaKind::Index),
            Err(StoreError::UnsupportedFutureSchema {
                found: 3,
                current: 2,
                ..
            })
        ));
    }

    #[test]
    fn current_schema_missing_required_field_is_not_defaulted() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let file = Path::new("record.json");
        store
            .write_json(file, &hashed(serde_json::json!({"schema_version": 2})))
            .unwrap();
        assert!(matches!(
            store.read_versioned_json::<CurrentV2>(file, FileSchemaKind::Index),
            Err(StoreError::Json { .. })
        ));
    }

    #[test]
    fn invalid_or_missing_schema_versions_are_distinguished() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let file = Path::new("record.json");

        store
            .write_json(file, &serde_json::json!({"required": "missing"}))
            .unwrap();
        assert!(matches!(
            store.read_versioned_json::<CurrentV2>(file, FileSchemaKind::Index),
            Err(StoreError::MissingSchemaVersion { .. })
        ));

        store
            .write_json(
                file,
                &serde_json::json!({"schema_version": "two", "required": "invalid"}),
            )
            .unwrap();
        assert!(matches!(
            store.read_versioned_json::<CurrentV2>(file, FileSchemaKind::Index),
            Err(StoreError::InvalidSchemaVersion { .. })
        ));
    }

    #[test]
    fn versioned_reader_requires_a_valid_content_hash() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let file = Path::new("record.json");
        store
            .write_json(
                file,
                &serde_json::json!({"schema_version": 2, "required": "missing hash"}),
            )
            .unwrap();
        assert!(matches!(
            store.read_versioned_json::<CurrentV2>(file, FileSchemaKind::Index),
            Err(StoreError::MissingContentHash { .. })
        ));
    }

    #[test]
    fn malformed_json_does_not_become_an_empty_object() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        fs::write(directory.path().join("bad.json"), b"{").unwrap();
        assert!(matches!(
            store.read_json_value(Path::new("bad.json")),
            Err(StoreError::Json { .. })
        ));
    }

    #[test]
    fn cleanup_removes_only_our_stale_temp_files() {
        let directory = tempdir().unwrap();
        let stale = directory.path().join(".record.json.tmp-1-1");
        let keep = directory.path().join("record.json");
        fs::write(&stale, b"temp").unwrap();
        fs::write(&keep, b"real").unwrap();
        let store = FileStore::open(
            directory.path(),
            FileStoreOptions {
                atomic_fsync: true,
                stale_temp_age: Some(Duration::ZERO),
            },
        )
        .unwrap();
        assert!(store
            .cleanup_stale_temps(Duration::ZERO)
            .unwrap()
            .is_empty());
        assert!(!stale.exists());
        assert!(keep.exists());
    }

    #[cfg(unix)]
    #[test]
    fn store_root_cannot_be_created_through_a_symlinked_ancestor() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let link = directory.path().join("link");
        symlink(outside.path(), &link).unwrap();
        assert!(matches!(
            FileStore::open(link.join("store"), FileStoreOptions::default()),
            Err(StoreError::SymlinkPath { .. })
        ));
    }
}
