use std::{env, path::PathBuf};

pub fn default_project_root() -> PathBuf {
    if let Ok(value) = env::var("CODEX_PROJECT_ROOT") {
        if !value.trim().is_empty() {
            return PathBuf::from(value);
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn project_path(path: impl AsRef<std::path::Path>) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        default_project_root().join(path)
    }
}
