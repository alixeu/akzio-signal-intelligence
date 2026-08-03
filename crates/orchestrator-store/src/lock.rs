//! Advisory, path-scoped FileStore locks.
//!
//! Atomic replacement protects a single write from torn bytes, but it does
//! not serialize read-modify-write transitions.  Lock files live beneath the
//! store root (outside any run root) so a run compaction can never unlink a
//! lock currently protecting that run's data.

use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use sha2::{Digest, Sha256};

use crate::{
    error::io_error,
    paths::{resolve_for_write, validate_relative_path},
    Result,
};

const LOCK_DIRECTORY: &str = ".locks";

/// A held advisory FileStore lock.  Dropping it releases the OS lock while
/// retaining its sidecar file for stable future lock acquisition.
#[derive(Debug)]
pub struct FileStoreLock {
    file: std::fs::File,
}

impl Drop for FileStoreLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

pub(crate) fn lock_exclusive(root: &Path, protected_relative: &Path) -> Result<FileStoreLock> {
    let path = lock_path(root, protected_relative)?;
    let file = open_lock_file(&path)?;
    file.lock_exclusive()
        .map_err(|source| io_error(&path, source))?;
    Ok(FileStoreLock { file })
}

pub(crate) fn lock_shared(root: &Path, protected_relative: &Path) -> Result<FileStoreLock> {
    let path = lock_path(root, protected_relative)?;
    let file = open_lock_file(&path)?;
    file.lock_shared()
        .map_err(|source| io_error(&path, source))?;
    Ok(FileStoreLock { file })
}

fn open_lock_file(path: &Path) -> Result<std::fs::File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|source| io_error(path, source))
}

fn lock_path(root: &Path, protected_relative: &Path) -> Result<PathBuf> {
    validate_relative_path(protected_relative)?;
    let digest = Sha256::digest(protected_relative.as_os_str().as_encoded_bytes());
    let relative = PathBuf::from(LOCK_DIRECTORY).join(format!("{:x}.lock", digest));
    resolve_for_write(root, &relative)
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        sync::{mpsc, Arc, Barrier},
        time::Duration,
    };

    use tempfile::tempdir;

    use super::lock_exclusive;

    #[test]
    fn exclusive_locks_serialize_independent_callers() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        let held = lock_exclusive(root, Path::new("runs/run/manifest.json")).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let (sender, receiver) = mpsc::channel();
        std::thread::scope(|scope| {
            let worker_barrier = Arc::clone(&barrier);
            scope.spawn(move || {
                worker_barrier.wait();
                let _second = lock_exclusive(root, Path::new("runs/run/manifest.json")).unwrap();
                sender.send(()).unwrap();
            });
            barrier.wait();
            assert!(receiver.recv_timeout(Duration::from_millis(40)).is_err());
            drop(held);
            receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        });
    }
}
