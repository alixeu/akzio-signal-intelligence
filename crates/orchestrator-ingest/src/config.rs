use orchestrator_core::{config_get, load_config, project_path};
use serde_json::Value;
use std::path::PathBuf;

pub fn load_default_config() -> Value {
    load_config(Some(&project_path("config/config.yaml"))).unwrap_or_else(|_| serde_json::json!({}))
}

pub fn shared_db_path_from_config(config: &Value) -> PathBuf {
    for key in ["orchestrator.db_path", "orchestrator.runtime.db_path"] {
        if let Some(value) = config_get(config, key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return project_path(value);
        }
    }
    project_path("outputs/orchestrator.sqlite")
}
