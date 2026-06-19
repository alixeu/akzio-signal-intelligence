use anyhow::Result;
use orchestrator_core::{config_get, load_config, project_path};
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

pub fn shared_db_path_from_config(config: &Value) -> PathBuf {
    for key in ["orchestrator.db_path", "orchestrator.runtime.db_path"] {
        if let Some(value) = config_get(config, key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return project_path_from_config(value);
        }
    }
    project_path("outputs/orchestrator.sqlite")
}
