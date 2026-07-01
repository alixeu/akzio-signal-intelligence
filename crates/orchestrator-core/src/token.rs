use serde_json::Value;

pub const MAX_PROMPT_TOKENS: usize = 12_000;
pub const MAX_COMPLETION_TOKENS: usize = 4_096;

/// Estimate token count for a string.
/// Heuristic: ~4 ASCII chars per token, ~1.5 CJK chars per token.
pub fn estimate_tokens(text: &str) -> usize {
    let cjk_count = text
        .chars()
        .filter(|c| {
            ('\u{4E00}'..='\u{9FFF}').contains(c)
                || ('\u{3400}'..='\u{4DBF}').contains(c)
                || ('\u{F900}'..='\u{FAFF}').contains(c)
                || ('\u{3000}'..='\u{303F}').contains(c)
                || ('\u{FF00}'..='\u{FFEF}').contains(c)
        })
        .count();
    let ascii_count = text.chars().count() - cjk_count;
    (cjk_count as f64 / 1.5).ceil() as usize + (ascii_count as f64 / 4.0).ceil() as usize
}

/// Estimate tokens for a JSON value (stringifies it first)
pub fn estimate_json_tokens(value: &Value) -> usize {
    let s = value.to_string();
    estimate_tokens(&s)
}

/// Estimate tokens for a single turn item based on its parts
pub fn estimate_turn_item_tokens(
    item_type: &str,
    role: &str,
    content_text: &str,
    content_json: &Value,
) -> usize {
    estimate_tokens(item_type)
        + estimate_tokens(role)
        + estimate_tokens(content_text)
        + estimate_json_tokens(content_json)
        + 8
}
