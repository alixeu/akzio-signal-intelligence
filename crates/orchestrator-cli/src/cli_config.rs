use anyhow::Result;
use orchestrator_core::{load_config, project_path};
use serde_json::Value;
use std::path::{Path, PathBuf};

pub fn load_default_config() -> Result<Value> {
    load_config(Some(&project_path("config/config.yaml")))
}

pub fn project_path_from_config(value: impl AsRef<Path>) -> PathBuf {
    let path = value.as_ref();
    if path.as_os_str().is_empty() {
        PathBuf::new()
    } else {
        project_path(path)
    }
}
