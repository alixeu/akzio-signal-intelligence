use orchestrator_core::{load_config, project_path};
use serde_json::Value;

pub fn load_default_config() -> Value {
    load_config(Some(&project_path("config/config.yaml"))).unwrap_or_else(|_| serde_json::json!({}))
}
