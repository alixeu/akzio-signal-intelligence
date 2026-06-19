use anyhow::{Context, Result};
use serde_json::{Map, Value};
use std::path::Path;

pub fn deep_merge(base: &mut Value, extra: Value) {
    match (base, extra) {
        (Value::Object(base_map), Value::Object(extra_map)) => {
            for (key, value) in extra_map {
                match base_map.get_mut(&key) {
                    Some(existing) if existing.is_object() && value.is_object() => {
                        deep_merge(existing, value);
                    }
                    _ => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (slot, value) => *slot = value,
    }
}

pub fn load_config(path: Option<&Path>) -> Result<Value> {
    let Some(path) = path else {
        return Ok(Value::Object(Map::new()));
    };
    if !path.exists() {
        return Ok(Value::Object(Map::new()));
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let yaml: serde_yaml::Value = serde_yaml::from_str(&text)
        .with_context(|| format!("failed to parse YAML config {}", path.display()))?;
    serde_json::to_value(yaml).context("failed to convert YAML config to JSON value")
}

pub fn config_get<'a>(config: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = config;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

pub fn config_str(config: &Value, path: &str, default: &str) -> String {
    match config_get(config, path) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(value) if !value.is_null() => value.to_string(),
        _ => default.to_string(),
    }
}

pub fn config_strings(config: &Value, path: &str, default: &[&str]) -> Vec<String> {
    match config_get(config, path) {
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(|value| match value {
                Value::String(text) if !text.trim().is_empty() => Some(text.to_string()),
                Value::Number(number) => Some(number.to_string()),
                Value::Bool(value) => Some(value.to_string()),
                _ => None,
            })
            .collect(),
        Some(Value::String(text)) => text
            .split(',')
            .filter_map(|item| {
                let item = item.trim();
                (!item.is_empty()).then(|| item.to_string())
            })
            .collect(),
        _ => default.iter().map(|item| item.to_string()).collect(),
    }
}

pub fn config_int(config: &Value, path: &str, default: i64) -> i64 {
    config_get(config, path)
        .and_then(|value| match value {
            Value::Number(number) => number.as_i64(),
            Value::String(text) => text.parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

pub fn config_float(config: &Value, path: &str, default: f64) -> f64 {
    config_get(config, path)
        .and_then(|value| match value {
            Value::Number(number) => number.as_f64(),
            Value::String(text) => text.parse::<f64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

pub fn config_bool(config: &Value, path: &str, default: bool) -> bool {
    config_get(config, path)
        .and_then(|value| match value {
            Value::Bool(value) => Some(*value),
            Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "y" | "on" => Some(true),
                "0" | "false" | "no" | "n" | "off" => Some(false),
                _ => None,
            },
            _ => None,
        })
        .unwrap_or(default)
}
