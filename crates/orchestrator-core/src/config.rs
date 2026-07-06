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
    let mut value =
        serde_json::to_value(yaml).context("failed to convert YAML config to JSON value")?;
    expand_env_placeholders(&mut value);
    Ok(value)
}

/// Recursively expand `${VAR}` and `${VAR:-default}` placeholders in string
/// config values using process environment variables. This keeps secrets
/// (API keys, SMTP passwords, provider tokens) out of the committed config
/// file: the YAML references an env var, the value is injected at load time.
/// An unset variable with no default expands to an empty string.
pub fn expand_env_placeholders(value: &mut Value) {
    match value {
        Value::String(text) => {
            if text.contains("${") {
                *text = expand_env_str(text);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(expand_env_placeholders),
        Value::Object(map) => map.values_mut().for_each(expand_env_placeholders),
        _ => {}
    }
}

fn expand_env_str(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        if let Some(end) = after.find('}') {
            let token = &after[..end];
            let (name, default) = match token.split_once(":-") {
                Some((name, default)) => (name.trim(), Some(default)),
                None => (token.trim(), None),
            };
            let resolved = std::env::var(name)
                .ok()
                .or_else(|| default.map(ToString::to_string))
                .unwrap_or_default();
            out.push_str(&resolved);
            rest = &after[end + 1..];
        } else {
            // No closing brace; emit the remainder verbatim.
            out.push_str(&rest[start..]);
            rest = "";
        }
    }
    out.push_str(rest);
    out
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn expands_plain_var() {
        std::env::set_var("AKZIO_TEST_KEY", "secret-123");
        let mut value = json!({"api_key": "${AKZIO_TEST_KEY}"});
        expand_env_placeholders(&mut value);
        assert_eq!(value["api_key"], json!("secret-123"));
        std::env::remove_var("AKZIO_TEST_KEY");
    }

    #[test]
    fn uses_default_when_unset() {
        std::env::remove_var("AKZIO_TEST_MISSING");
        let mut value = json!({"url": "${AKZIO_TEST_MISSING:-https://fallback}"});
        expand_env_placeholders(&mut value);
        assert_eq!(value["url"], json!("https://fallback"));
    }

    #[test]
    fn unset_without_default_becomes_empty() {
        std::env::remove_var("AKZIO_TEST_MISSING2");
        let mut value = json!({"api_key": "${AKZIO_TEST_MISSING2}"});
        expand_env_placeholders(&mut value);
        assert_eq!(value["api_key"], json!(""));
    }

    #[test]
    fn expands_nested_and_arrays() {
        std::env::set_var("AKZIO_TEST_NESTED", "v");
        let mut value = json!({
            "a": {"b": "${AKZIO_TEST_NESTED}"},
            "list": ["${AKZIO_TEST_NESTED}", "literal"]
        });
        expand_env_placeholders(&mut value);
        assert_eq!(value["a"]["b"], json!("v"));
        assert_eq!(value["list"][0], json!("v"));
        assert_eq!(value["list"][1], json!("literal"));
        std::env::remove_var("AKZIO_TEST_NESTED");
    }

    #[test]
    fn leaves_plain_strings_untouched() {
        let mut value = json!({"model": "gpt-5.5"});
        expand_env_placeholders(&mut value);
        assert_eq!(value["model"], json!("gpt-5.5"));
    }
}
