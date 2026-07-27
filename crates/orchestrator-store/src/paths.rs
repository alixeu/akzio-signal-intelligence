use std::{
    ffi::OsStr,
    fs,
    path::{Component, Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::{error::io_error, Result, StoreError};

// Keep every encoded component strictly below the 100-byte store-path limit.
const MAX_COMPONENT_BYTES: usize = 99;
const MAX_KIND_BYTES: usize = 16;
const MAX_READABLE_PREFIX_BYTES: usize = 24;

/// A deterministic, single-component path name derived from an arbitrary
/// original identifier.  The original identifier must remain in the stored
/// JSON envelope; it is intentionally not recoverable from the slug alone.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SafeSlug(String);

impl SafeSlug {
    pub fn new(kind: &str, original: &str) -> Result<Self> {
        validate_kind(kind)?;

        let digest = Sha256::digest(original.as_bytes());
        let hash = hex_digest(&digest);
        let max_prefix =
            (MAX_COMPONENT_BYTES - kind.len() - hash.len() - 2).min(MAX_READABLE_PREFIX_BYTES);
        let readable = readable_prefix(original, max_prefix);
        Ok(Self(format!("{kind}-{readable}-{hash}")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_os_str(&self) -> &OsStr {
        OsStr::new(&self.0)
    }

    pub fn verify(&self, kind: &str, original: &str) -> Result<()> {
        let expected = Self::new(kind, original)?;
        if self == &expected {
            Ok(())
        } else {
            Err(StoreError::SlugMismatch {
                kind: kind.to_owned(),
                slug: self.0.clone(),
            })
        }
    }
}

impl AsRef<str> for SafeSlug {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for SafeSlug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Reject a path that could escape the store root.
pub fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Err(StoreError::UnsafeRelativePath {
            path: path.to_path_buf(),
            reason: "path is empty",
        });
    }
    if path.is_absolute() {
        return Err(StoreError::UnsafeRelativePath {
            path: path.to_path_buf(),
            reason: "path is absolute",
        });
    }
    for component in path.components() {
        match component {
            Component::Normal(value) if !value.is_empty() => {}
            Component::CurDir => {
                return Err(StoreError::UnsafeRelativePath {
                    path: path.to_path_buf(),
                    reason: "current-directory components are not allowed",
                });
            }
            Component::ParentDir => {
                return Err(StoreError::UnsafeRelativePath {
                    path: path.to_path_buf(),
                    reason: "parent-directory components are not allowed",
                });
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(StoreError::UnsafeRelativePath {
                    path: path.to_path_buf(),
                    reason: "root or platform prefix components are not allowed",
                });
            }
            _ => {
                return Err(StoreError::UnsafeRelativePath {
                    path: path.to_path_buf(),
                    reason: "empty path components are not allowed",
                });
            }
        }
    }
    Ok(())
}

/// Resolve a target underneath an already-initialized root, creating missing
/// parent directories only after every existing component has been checked for
/// symlinks.
pub(crate) fn resolve_for_write(root: &Path, relative: &Path) -> Result<PathBuf> {
    validate_relative_path(relative)?;
    ensure_not_symlink(root)?;

    let parent = relative
        .parent()
        .ok_or_else(|| StoreError::UnsafeRelativePath {
            path: relative.to_path_buf(),
            reason: "path has no parent",
        })?;
    let mut cursor = root.to_path_buf();
    for component in parent.components() {
        let Component::Normal(name) = component else {
            unreachable!("validate_relative_path already validated components");
        };
        cursor.push(name);
        match fs::symlink_metadata(&cursor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StoreError::SymlinkPath { path: cursor });
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(StoreError::ExpectedDirectory { path: cursor });
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::create_dir(&cursor) {
                    Ok(()) => {}
                    Err(create_error)
                        if create_error.kind() == std::io::ErrorKind::AlreadyExists =>
                    {
                        let metadata = fs::symlink_metadata(&cursor)
                            .map_err(|source| io_error(&cursor, source))?;
                        if metadata.file_type().is_symlink() {
                            return Err(StoreError::SymlinkPath { path: cursor });
                        }
                        if !metadata.is_dir() {
                            return Err(StoreError::ExpectedDirectory { path: cursor });
                        }
                    }
                    Err(source) => return Err(io_error(&cursor, source)),
                }
            }
            Err(source) => return Err(io_error(&cursor, source)),
        }
    }

    let target = root.join(relative);
    if let Ok(metadata) = fs::symlink_metadata(&target) {
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path: target });
        }
    }
    Ok(target)
}

pub(crate) fn resolve_existing(root: &Path, relative: &Path) -> Result<PathBuf> {
    validate_relative_path(relative)?;
    ensure_not_symlink(root)?;
    let mut cursor = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            unreachable!("validate_relative_path already validated components");
        };
        cursor.push(name);
        ensure_not_symlink(&cursor)?;
    }
    Ok(cursor)
}

pub(crate) fn ensure_not_symlink(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|source| io_error(path, source))?;
    if metadata.file_type().is_symlink() {
        return Err(StoreError::SymlinkPath {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn validate_kind(kind: &str) -> Result<()> {
    if kind.is_empty() {
        return Err(StoreError::InvalidSlugKind {
            kind: kind.to_owned(),
            reason: "kind is empty",
        });
    }
    if kind.len() > MAX_KIND_BYTES {
        return Err(StoreError::InvalidSlugKind {
            kind: kind.to_owned(),
            reason: "kind exceeds 16 bytes",
        });
    }
    if !kind
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(StoreError::InvalidSlugKind {
            kind: kind.to_owned(),
            reason: "kind must contain only lowercase ASCII letters, digits, or hyphens",
        });
    }
    Ok(())
}

fn readable_prefix(original: &str, max_bytes: usize) -> String {
    let mut output = String::new();
    let mut previous_hyphen = false;
    for character in original.chars() {
        let normalized = if character.is_ascii_alphanumeric() {
            Some(character.to_ascii_lowercase())
        } else {
            None
        };
        match normalized {
            Some(character) => {
                if output.len() + character.len_utf8() > max_bytes {
                    break;
                }
                output.push(character);
                previous_hyphen = false;
            }
            None if !output.is_empty() && !previous_hyphen => {
                if output.len() + 1 > max_bytes {
                    break;
                }
                output.push('-');
                previous_hyphen = true;
            }
            None => {}
        }
    }
    let trimmed = output.trim_matches('-');
    if trimmed.is_empty() {
        "item".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{validate_relative_path, SafeSlug};

    #[test]
    fn safe_slug_is_case_sensitive_in_hash_but_path_safe() {
        let upper = SafeSlug::new("ticker", "QQQ").unwrap();
        let lower = SafeSlug::new("ticker", "qqq").unwrap();
        assert_ne!(upper, lower);
        assert!(upper.as_str().starts_with("ticker-qqq-"));
        assert!(upper.as_str().len() < 100);
    }

    #[test]
    fn safe_slug_handles_special_and_long_input() {
        let slug = SafeSlug::new("topic", "风险/主题: 🚀 %%%").unwrap();
        assert!(slug.as_str().starts_with("topic-item-"));
        assert!(slug
            .as_str()
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));

        let long = SafeSlug::new("role", &"x".repeat(4_000)).unwrap();
        assert!(long.as_str().len() < 100);
    }

    #[test]
    fn slug_verification_rejects_substituted_original_value() {
        let slug = SafeSlug::new("topic", "earnings").unwrap();
        assert!(slug.verify("topic", "macro").is_err());
    }

    #[test]
    fn unsafe_relative_paths_are_rejected() {
        for path in [
            "",
            "../escape",
            "/absolute",
            "./relative",
            "dir/../../escape",
        ] {
            assert!(validate_relative_path(Path::new(path)).is_err(), "{path}");
        }
        assert!(validate_relative_path(Path::new("runs/date/file.json")).is_ok());
    }
}
