use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

const HARD_TRUNCATION_SUFFIX: &str = "\n[truncated]";
const TEXT_TRUNCATION_SEPARATOR: &str = "\n[... middle truncated ...]\n";
const MAX_REFERENCE_ARRAY_ITEMS: usize = 20;

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
    /// Additional fields to preserve even when truncating. Stable identity and
    /// evidence-reference fields are always preserved recursively by Rust,
    /// regardless of this configurable list.
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

/// Values in these fields are not prose: later tools need them verbatim to
/// address a stored object. Truncating an `index_id` into an ellipsis makes a
/// valid prior `read_indexes` result impossible to consume with
/// `read_index_details`, which in turn converts a retrieval requirement into
/// a futile tool loop.
fn is_stable_reference_field(field: &str) -> bool {
    matches!(
        field,
        "id" | "index_id"
            | "detail_id"
            | "content_hash"
            | "run_id"
            | "source_run_id"
            | "session_id"
            | "turn_id"
            | "topic_id"
            | "claim_id"
            | "hinge_id"
            | "evidence_id"
            | "event_cluster_id"
            | "decision_id"
            | "outcome_id"
            | "reflection_id"
            | "pattern_id"
            | "call_id"
            | "output_item_id"
            | "parent_index_id"
    )
}

/// These are bounded collections of stable references rather than free-form
/// prose. Keeping the first deterministic page preserves evidence lineage
/// while the enclosing JSON truncation still has a hard size bound.
fn is_reference_collection_field(field: &str) -> bool {
    matches!(
        field,
        "source_refs" | "evidence_refs" | "evidence_ids" | "event_cluster_ids"
    )
}

/// Small metadata needed to interpret a stable reference in a later call.
fn is_reference_context_field(field: &str) -> bool {
    matches!(
        field,
        "source_phase"
            | "role"
            | "kind"
            | "ticker"
            | "section"
            | "next_cursor"
            | "has_more"
            | "truncated"
    )
}

fn field_must_survive_truncation(field: &str, preserve_fields: &[String]) -> bool {
    preserve_fields.iter().any(|configured| configured == field)
        || is_stable_reference_field(field)
        || is_reference_collection_field(field)
        || is_reference_context_field(field)
}

impl Default for TruncationConfig {
    fn default() -> Self {
        Self {
            // Bound tool payloads before they enter turn history. The former
            // 200k default duplicated large SQL/CSV results in prompts and DB.
            tool_result_chars: 16_000,
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

fn is_within_char_budget(content: &str, max_chars: usize) -> bool {
    content.chars().count() <= max_chars
}

/// Semantic truncation: format-aware, preserves JSON validity for valid JSON inputs.
pub fn truncate_semantic(content: &str, max_chars: usize, config: &TruncationConfig) -> String {
    if is_within_char_budget(content, max_chars) {
        return content.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }

    truncate_semantic_over_budget(content, max_chars, config)
}

fn truncate_semantic_over_budget(
    content: &str,
    max_chars: usize,
    config: &TruncationConfig,
) -> String {
    match config.strategy {
        TruncationStrategy::Hard => truncate_hard(content, max_chars),
        TruncationStrategy::Semantic => truncate_semantic_by_format(content, max_chars, config),
    }
}

fn truncate_semantic_by_format(
    content: &str,
    max_chars: usize,
    config: &TruncationConfig,
) -> String {
    match detect_format(content) {
        ContentFormat::Json => truncate_json(content, max_chars, config),
        ContentFormat::Text | ContentFormat::Markdown => {
            truncate_text_head_tail(content, max_chars, &config.text)
        }
    }
}

/// Legacy hard truncation. This intentionally matches `tools::truncate_chars`.
pub fn truncate_hard(content: &str, max_chars: usize) -> String {
    if is_within_char_budget(content, max_chars) {
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
    let Some(original) = parse_json_content(content) else {
        return truncate_text_head_tail(content, max_chars, &config.text);
    };

    let preserve = &config.json.preserve_fields;
    let mut value = original.clone();

    if let Some(serialized) = reduce_json_until_within_limit(
        &mut value,
        max_chars,
        preserve,
        JsonReductionBudget::from_limit(max_chars, config.json.max_array_elements),
    ) {
        return serialized;
    }

    fallback_json_truncation(&original, &value, max_chars, preserve)
}

fn parse_json_content(content: &str) -> Option<Value> {
    serde_json::from_str(content.trim()).ok()
}

#[derive(Debug, Clone, Copy)]
struct JsonReductionBudget {
    max_array_elements: usize,
    max_string_chars: usize,
}

impl JsonReductionBudget {
    fn from_limit(max_chars: usize, max_array_elements: usize) -> Self {
        Self {
            max_array_elements,
            max_string_chars: max_chars.saturating_div(4).max(32),
        }
    }

    /// Keep at least one array element and a small string budget while shrinking.
    fn shrink(&mut self) -> bool {
        let next_array_elements = self.max_array_elements.saturating_div(2).max(1);
        let next_string_chars = self.max_string_chars.saturating_div(2).max(16);
        if next_array_elements == self.max_array_elements
            && next_string_chars == self.max_string_chars
        {
            return false;
        }
        self.max_array_elements = next_array_elements;
        self.max_string_chars = next_string_chars;
        true
    }
}

fn reduce_json_until_within_limit(
    value: &mut Value,
    max_chars: usize,
    preserve_fields: &[String],
    mut budget: JsonReductionBudget,
) -> Option<String> {
    for _ in 0..12 {
        reduce_json_value(
            value,
            budget.max_array_elements,
            preserve_fields,
            budget.max_string_chars,
            false,
        );
        if let Some(serialized) = serialize_json_within_limit(value, max_chars) {
            return Some(serialized);
        }

        if !budget.shrink() {
            break;
        }
    }

    None
}

fn fallback_json_truncation(
    original: &Value,
    reduced: &Value,
    max_chars: usize,
    preserve_fields: &[String],
) -> String {
    let mut pruned = reduced.clone();
    prune_non_preserved_fields(&mut pruned, preserve_fields);
    if let Some(serialized) = serialize_json_within_limit(&pruned, max_chars) {
        return serialized;
    }

    compact_preserved_json(original, max_chars, preserve_fields)
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
                let nested_preserved = field_must_survive_truncation(key, preserve_fields);
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
                field_must_survive_truncation(key, preserve_fields)
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

    if let Some(Value::Object(reference_tree)) = compact_reference_tree(original) {
        for (key, reference) in reference_tree {
            object.entry(key).or_insert(reference);
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

/// Build a shape-preserving projection of the fields that are required to
/// address or verify a later ToolManaged read. This is used only as the final
/// JSON fallback; normal truncation retains richer content first.
fn compact_reference_tree(value: &Value) -> Option<Value> {
    match value {
        Value::Array(items) => {
            let projected = items
                .iter()
                .filter_map(compact_reference_tree)
                .take(MAX_REFERENCE_ARRAY_ITEMS)
                .collect::<Vec<_>>();
            (!projected.is_empty()).then_some(Value::Array(projected))
        }
        Value::Object(object) => {
            let mut projected = Map::new();
            for (key, nested) in object {
                if is_stable_reference_field(key) || is_reference_context_field(key) {
                    if nested.is_string() || nested.is_number() || nested.is_boolean() {
                        projected.insert(key.clone(), nested.clone());
                    }
                    continue;
                }
                if is_reference_collection_field(key) {
                    if let Value::Array(items) = nested {
                        let items = items
                            .iter()
                            .filter(|item| item.is_string())
                            .take(MAX_REFERENCE_ARRAY_ITEMS)
                            .cloned()
                            .collect::<Vec<_>>();
                        if !items.is_empty() {
                            projected.insert(key.clone(), Value::Array(items));
                        }
                    }
                    continue;
                }
                if let Some(child) = compact_reference_tree(nested) {
                    projected.insert(key.clone(), child);
                }
            }
            (!projected.is_empty()).then_some(Value::Object(projected))
        }
        _ => None,
    }
}

fn truncate_all_strings(value: &mut Value, max_string_chars: usize) {
    truncate_all_strings_at(value, max_string_chars, false);
}

fn truncate_all_strings_at(value: &mut Value, max_string_chars: usize, preserve_string: bool) {
    match value {
        Value::String(text) if !preserve_string && text.chars().count() > max_string_chars => {
            *text = truncate_string_with_suffix(text, max_string_chars);
        }
        Value::Array(items) => {
            for item in items {
                truncate_all_strings_at(item, max_string_chars, preserve_string);
            }
        }
        Value::Object(object) => {
            for (key, nested) in object {
                truncate_all_strings_at(
                    nested,
                    max_string_chars,
                    is_stable_reference_field(key) || is_reference_collection_field(key),
                );
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
    if is_within_char_budget(content, max_chars) {
        return content.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }

    let Some((head_chars, tail_chars)) = text_split_budget(max_chars, config) else {
        return content.chars().take(max_chars).collect();
    };

    let head = content.chars().take(head_chars).collect::<String>();
    let tail = content
        .chars()
        .skip(total_chars.saturating_sub(tail_chars))
        .collect::<String>();
    format!("{head}{TEXT_TRUNCATION_SEPARATOR}{tail}")
}

fn text_split_budget(max_chars: usize, config: &TextTruncationConfig) -> Option<(usize, usize)> {
    let separator_len = TEXT_TRUNCATION_SEPARATOR.chars().count();
    if max_chars <= separator_len {
        return None;
    }

    let available = max_chars - separator_len;
    let defaults = TextTruncationConfig::default();
    let head_ratio = normalized_ratio(config.head_ratio, defaults.head_ratio);
    let tail_ratio = normalized_ratio(config.tail_ratio, defaults.tail_ratio);
    let ratio_total = head_ratio + tail_ratio;
    let head_fraction = if ratio_total > 0.0 {
        head_ratio / ratio_total
    } else {
        defaults.head_ratio
    };
    let head_chars = ((available as f64) * head_fraction).floor() as usize;
    let tail_chars = available.saturating_sub(head_chars);
    Some((head_chars, tail_chars))
}

fn normalized_ratio(value: f64, default: f64) -> f64 {
    if value.is_finite() && value >= 0.0 {
        value
    } else {
        default
    }
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
    fn keeps_at_least_one_array_element_while_shrinking_large_json() {
        let mut items = Vec::new();
        for i in 0..80 {
            items.push(json!({
                "ticker": "QQQ",
                "kline_time": format!("2026-07-07T{i:02}:00:00Z"),
                "indicators": {"Close": 500.0 + i as f64, "Return": 0.01}
            }));
        }
        let content = serde_json::to_string(&json!({
            "query": "get-technical-context",
            "daily": items
        }))
        .unwrap();
        let truncated = truncate_semantic(&content, 800, &TruncationConfig::default());
        let parsed: Value = serde_json::from_str(&truncated).unwrap();
        let daily = parsed.get("daily").and_then(Value::as_array).unwrap();
        assert!(
            !daily.is_empty(),
            "truncation should retain at least one daily snapshot, got {truncated}"
        );
    }

    #[test]
    fn json_truncation_keeps_nested_index_id_exact_for_follow_up_detail_reads() {
        let index_id = "idx-70bef6dde5ca4652712360d7874e8497ca63216faeb731c78b60a81a3e724f01";
        let content = serde_json::to_string(&json!({
            "indexes": [{
                "index_id": index_id,
                "source_phase": 2,
                "role": "mediator.topic_controller",
                "summary": "x".repeat(20_000)
            }],
            "diagnostics": "y".repeat(20_000)
        }))
        .unwrap();

        let truncated = truncate_semantic(&content, 500, &TruncationConfig::default());
        let parsed: Value = serde_json::from_str(&truncated).unwrap();

        assert_eq!(
            parsed
                .pointer("/indexes/0/index_id")
                .and_then(Value::as_str),
            Some(index_id),
            "a model must retain the exact visible Index ID in order to call read_index_details"
        );
        assert_eq!(
            parsed
                .pointer("/indexes/0/source_phase")
                .and_then(Value::as_u64),
            Some(2),
            "retrieval audit needs the source phase paired with the visible Index ID"
        );
        assert!(truncated.chars().count() <= 500);
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
