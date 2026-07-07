use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

const HARD_TRUNCATION_SUFFIX: &str = "\n[truncated]";
const TEXT_TRUNCATION_SEPARATOR: &str = "\n[... middle truncated ...]\n";

/// Content format detected for truncation strategy selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentFormat {
    Json,
    Text,
    Markdown,
}

/// Truncation strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncationStrategy {
    /// Hard character cutoff (legacy behavior).
    Hard,
    /// Format-aware truncation with head+tail and JSON boundary preservation.
    Semantic,
}

/// Runtime truncation configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TruncationConfig {
    pub tool_result_chars: usize,
    pub context_fragment_chars: usize,
    pub strategy: TruncationStrategy,
    pub json: JsonTruncationConfig,
    pub text: TextTruncationConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct JsonTruncationConfig {
    /// Fields to always preserve even when truncating (top-level keys).
    pub preserve_fields: Vec<String>,
    /// Maximum array elements to keep when truncating arrays.
    pub max_array_elements: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TextTruncationConfig {
    /// Ratio of budget for head. Tail receives the remaining normalized ratio.
    pub head_ratio: f64,
    /// Ratio of budget for tail.
    pub tail_ratio: f64,
}

fn default_preserve_fields() -> Vec<String> {
    vec![
        "status".to_string(),
        "error".to_string(),
        "summary".to_string(),
        "artifact_type".to_string(),
        "role".to_string(),
        "id".to_string(),
    ]
}

impl Default for TruncationConfig {
    fn default() -> Self {
        Self {
            tool_result_chars: 8_000,
            context_fragment_chars: 12_000,
            strategy: TruncationStrategy::Semantic,
            json: JsonTruncationConfig::default(),
            text: TextTruncationConfig::default(),
        }
    }
}

impl Default for JsonTruncationConfig {
    fn default() -> Self {
        Self {
            preserve_fields: default_preserve_fields(),
            max_array_elements: 50,
        }
    }
}

impl Default for TextTruncationConfig {
    fn default() -> Self {
        Self {
            head_ratio: 0.6,
            tail_ratio: 0.4,
        }
    }
}

/// Detect content format from the first non-whitespace characters.
pub fn detect_format(content: &str) -> ContentFormat {
    let trimmed = content.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        ContentFormat::Json
    } else if trimmed.starts_with('#') || trimmed.contains("```") {
        ContentFormat::Markdown
    } else {
        ContentFormat::Text
    }
}

/// Semantic truncation: format-aware, preserves JSON validity for valid JSON inputs.
pub fn truncate_semantic(content: &str, max_chars: usize, config: &TruncationConfig) -> String {
    if content.chars().count() <= max_chars {
        return content.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }

    match config.strategy {
        TruncationStrategy::Hard => truncate_hard(content, max_chars),
        TruncationStrategy::Semantic => match detect_format(content) {
            ContentFormat::Json => truncate_json(content, max_chars, config),
            ContentFormat::Text | ContentFormat::Markdown => {
                truncate_text_head_tail(content, max_chars, &config.text)
            }
        },
    }
}

/// Legacy hard truncation. This intentionally matches `tools::truncate_chars`.
pub fn truncate_hard(content: &str, max_chars: usize) -> String {
    if content.chars().count() <= max_chars {
        return content.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }
    let suffix_len = HARD_TRUNCATION_SUFFIX.chars().count();
    if max_chars <= suffix_len {
        return content.chars().take(max_chars).collect();
    }
    let mut output = content
        .chars()
        .take(max_chars - suffix_len)
        .collect::<String>();
    output.push_str(HARD_TRUNCATION_SUFFIX);
    output
}

fn truncate_json(content: &str, max_chars: usize, config: &TruncationConfig) -> String {
    let Ok(original) = serde_json::from_str::<Value>(content.trim()) else {
        return truncate_text_head_tail(content, max_chars, &config.text);
    };

    let preserve = &config.json.preserve_fields;
    let mut value = original.clone();
    let mut max_array_elements = config.json.max_array_elements;
    let mut max_string_chars = max_chars.saturating_div(4).max(32);

    for _ in 0..12 {
        reduce_json_value(
            &mut value,
            max_array_elements,
            preserve,
            max_string_chars,
            false,
        );
        if let Some(serialized) = serialize_json_within_limit(&value, max_chars) {
            return serialized;
        }

        let next_array_elements = max_array_elements.saturating_div(2);
        let next_string_chars = max_string_chars.saturating_div(2).max(16);
        if next_array_elements == max_array_elements && next_string_chars == max_string_chars {
            break;
        }
        max_array_elements = next_array_elements;
        max_string_chars = next_string_chars;
    }

    let mut pruned = value.clone();
    prune_non_preserved_fields(&mut pruned, preserve);
    if let Some(serialized) = serialize_json_within_limit(&pruned, max_chars) {
        return serialized;
    }

    compact_preserved_json(&original, max_chars, preserve)
}

fn serialize_json_within_limit(value: &Value, max_chars: usize) -> Option<String> {
    if let Ok(pretty) = serde_json::to_string_pretty(value) {
        if pretty.chars().count() <= max_chars {
            return Some(pretty);
        }
    }
    let compact = serde_json::to_string(value).ok()?;
    (compact.chars().count() <= max_chars).then_some(compact)
}

fn reduce_json_value(
    value: &mut Value,
    max_array_elements: usize,
    preserve_fields: &[String],
    max_string_chars: usize,
    field_preserved: bool,
) -> bool {
    match value {
        Value::Array(items) => {
            let mut reduced = false;
            for item in items.iter_mut() {
                reduced |= reduce_json_value(
                    item,
                    max_array_elements,
                    preserve_fields,
                    max_string_chars,
                    field_preserved,
                );
            }
            if items.len() > max_array_elements {
                items.truncate(max_array_elements);
                reduced = true;
            }
            reduced
        }
        Value::Object(object) => {
            let mut reduced = false;
            for (key, nested) in object.iter_mut() {
                let nested_preserved = preserve_fields.iter().any(|field| field == key);
                reduced |= reduce_json_value(
                    nested,
                    max_array_elements,
                    preserve_fields,
                    max_string_chars,
                    nested_preserved,
                );
            }
            reduced
        }
        Value::String(text) if !field_preserved && text.chars().count() > max_string_chars => {
            *text = truncate_string_with_suffix(text, max_string_chars);
            true
        }
        _ => false,
    }
}

fn prune_non_preserved_fields(value: &mut Value, preserve_fields: &[String]) -> bool {
    match value {
        Value::Object(object) => {
            let before = object.len();
            object.retain(|key, nested| {
                preserve_fields.iter().any(|field| field == key)
                    || nested.is_array()
                    || nested.is_object()
            });
            let mut pruned = object.len() != before;
            for nested in object.values_mut() {
                pruned |= prune_non_preserved_fields(nested, preserve_fields);
            }
            pruned
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                prune_non_preserved_fields(item, preserve_fields);
            }
            false
        }
        _ => false,
    }
}

fn compact_preserved_json(
    original: &Value,
    max_chars: usize,
    preserve_fields: &[String],
) -> String {
    let mut object = Map::new();
    object.insert("_truncated".to_string(), json!(true));
    object.insert(
        "_note".to_string(),
        json!("content exceeded truncation budget; preserved configured fields"),
    );

    if let Value::Object(original_object) = original {
        for field in preserve_fields {
            if let Some(value) = original_object.get(field) {
                object.insert(field.clone(), value.clone());
            }
        }
    }

    let mut value = Value::Object(object);
    if let Some(serialized) = serialize_json_within_limit(&value, max_chars) {
        return serialized;
    }

    let mut preserved_string_budget = max_chars.saturating_div(4).max(8);
    for _ in 0..12 {
        truncate_all_strings(&mut value, preserved_string_budget);
        if let Some(serialized) = serialize_json_within_limit(&value, max_chars) {
            return serialized;
        }
        let next_budget = preserved_string_budget.saturating_div(2).max(1);
        if next_budget == preserved_string_budget {
            break;
        }
        preserved_string_budget = next_budget;
    }

    let minimal = json!({"_truncated": true});
    serialize_json_within_limit(&minimal, max_chars)
        .unwrap_or_else(|| truncate_hard("{}", max_chars))
}

fn truncate_all_strings(value: &mut Value, max_string_chars: usize) {
    match value {
        Value::String(text) if text.chars().count() > max_string_chars => {
            *text = truncate_string_with_suffix(text, max_string_chars);
        }
        Value::Array(items) => {
            for item in items {
                truncate_all_strings(item, max_string_chars);
            }
        }
        Value::Object(object) => {
            for nested in object.values_mut() {
                truncate_all_strings(nested, max_string_chars);
            }
        }
        _ => {}
    }
}

fn truncate_string_with_suffix(text: &str, max_chars: usize) -> String {
    let suffix = "...[truncated]";
    let suffix_len = suffix.chars().count();
    if max_chars == 0 {
        return String::new();
    }
    if max_chars <= suffix_len {
        return text.chars().take(max_chars).collect();
    }
    let mut output = text
        .chars()
        .take(max_chars - suffix_len)
        .collect::<String>();
    output.push_str(suffix);
    output
}

fn truncate_text_head_tail(
    content: &str,
    max_chars: usize,
    config: &TextTruncationConfig,
) -> String {
    let total_chars = content.chars().count();
    if total_chars <= max_chars {
        return content.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }

    let separator_len = TEXT_TRUNCATION_SEPARATOR.chars().count();
    if max_chars <= separator_len {
        return content.chars().take(max_chars).collect();
    }

    let available = max_chars - separator_len;
    let head_ratio = if config.head_ratio.is_finite() && config.head_ratio >= 0.0 {
        config.head_ratio
    } else {
        TextTruncationConfig::default().head_ratio
    };
    let tail_ratio = if config.tail_ratio.is_finite() && config.tail_ratio >= 0.0 {
        config.tail_ratio
    } else {
        TextTruncationConfig::default().tail_ratio
    };
    let ratio_total = head_ratio + tail_ratio;
    let head_fraction = if ratio_total > 0.0 {
        head_ratio / ratio_total
    } else {
        TextTruncationConfig::default().head_ratio
    };
    let head_chars = ((available as f64) * head_fraction).floor() as usize;
    let tail_chars = available.saturating_sub(head_chars);

    let head = content.chars().take(head_chars).collect::<String>();
    let tail = content
        .chars()
        .skip(total_chars.saturating_sub(tail_chars))
        .collect::<String>();
    format!("{head}{TEXT_TRUNCATION_SEPARATOR}{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> TruncationConfig {
        TruncationConfig::default()
    }

    #[test]
    fn short_content_not_truncated() {
        let result = truncate_semantic("short text", 100, &default_config());
        assert_eq!(result, "short text");
    }

    #[test]
    fn zero_max_chars_returns_empty_string() {
        let result = truncate_semantic("not empty", 0, &default_config());
        assert_eq!(result, "");
    }

    #[test]
    fn content_exactly_at_limit_is_not_truncated() {
        let result = truncate_semantic("12345", 5, &default_config());
        assert_eq!(result, "12345");
    }

    #[test]
    fn json_truncation_preserves_validity() {
        let large_json = serde_json::json!({
            "status": "completed",
            "results": (0..100).map(|i| format!("result item {i}")).collect::<Vec<_>>(),
            "summary": "important summary at end"
        })
        .to_string();
        let result = truncate_semantic(&large_json, 500, &default_config());
        let parsed: Value = serde_json::from_str(&result)
            .unwrap_or_else(|error| panic!("truncated JSON is not valid: {error}\n{result}"));
        assert_eq!(parsed["status"], json!("completed"));
        assert_eq!(parsed["summary"], json!("important summary at end"));
        assert!(result.chars().count() <= 500);
    }

    #[test]
    fn json_array_truncated_at_boundary() {
        let large_json = serde_json::json!({
            "items": (0..200).map(|i| json!({"id": i, "name": format!("item {i}")})).collect::<Vec<_>>()
        })
        .to_string();
        let result = truncate_semantic(&large_json, 1000, &default_config());
        let parsed: Value = serde_json::from_str(&result).expect("should be valid JSON");
        let items = parsed.get("items").and_then(Value::as_array).unwrap();
        assert!(
            items.len() <= 50,
            "array should be truncated to max_array_elements"
        );
        assert!(result.chars().count() <= 1000);
    }

    #[test]
    fn text_head_tail_preservation() {
        let content = "HEAD ".to_string() + &"x".repeat(1000) + " TAIL";
        let result = truncate_semantic(&content, 200, &default_config());
        assert!(result.starts_with("HEAD "), "head should be preserved");
        assert!(
            result.ends_with(" TAIL") || result.contains("TAIL"),
            "tail should be preserved"
        );
        assert!(
            result.contains("[... middle truncated ...]"),
            "separator should be present"
        );
        assert!(result.chars().count() <= 200);
    }

    #[test]
    fn hard_strategy_matches_legacy_behavior() {
        let mut config = default_config();
        config.strategy = TruncationStrategy::Hard;
        let content = "x".repeat(100);
        let result = truncate_semantic(&content, 50, &config);
        assert_eq!(result, truncate_hard(&content, 50));
        assert!(result.ends_with("[truncated]"));
        assert_eq!(result.chars().count(), 50);
    }

    #[test]
    fn invalid_json_falls_back_to_text_truncation() {
        let content = "{ this is not valid json but it is long ".to_string() + &"x".repeat(1000);
        let result = truncate_semantic(&content, 200, &default_config());
        assert!(result.contains("[... middle truncated ...]"));
        assert!(result.chars().count() <= 200);
    }

    #[test]
    fn markdown_detected_as_markdown() {
        assert_eq!(detect_format("# Heading"), ContentFormat::Markdown);
        assert_eq!(detect_format("```json\n{}\n```"), ContentFormat::Markdown);
    }

    #[test]
    fn json_detected_correctly() {
        assert_eq!(detect_format("{\"key\": \"value\"}"), ContentFormat::Json);
        assert_eq!(detect_format("[1, 2, 3]"), ContentFormat::Json);
        assert_eq!(detect_format("  \n  {\"key\": 1}"), ContentFormat::Json);
    }

    #[test]
    fn preserve_fields_kept_during_truncation() {
        let large_json = serde_json::json!({
            "status": "completed",
            "role": "analyst.technical",
            "long_field": "x".repeat(5000),
            "another_long": "y".repeat(5000)
        })
        .to_string();
        let result = truncate_semantic(&large_json, 500, &default_config());
        let parsed: Value = serde_json::from_str(&result).expect("valid JSON");
        assert_eq!(parsed["status"], json!("completed"));
        assert_eq!(parsed["role"], json!("analyst.technical"));
        assert!(result.chars().count() <= 500);
    }

    #[test]
    fn custom_config_deserializes_with_defaults() {
        let config: TruncationConfig = serde_json::from_value(json!({
            "tool_result_chars": 100,
            "strategy": "hard",
            "json": {"max_array_elements": 3}
        }))
        .unwrap();
        assert_eq!(config.tool_result_chars, 100);
        assert_eq!(config.context_fragment_chars, 12_000);
        assert_eq!(config.strategy, TruncationStrategy::Hard);
        assert_eq!(config.json.max_array_elements, 3);
        assert!(config.json.preserve_fields.contains(&"status".to_string()));
        assert_eq!(config.text.head_ratio, 0.6);
    }
}
