use serde_json::Value;

pub const MAX_PROMPT_TOKENS: usize = 120_000;

/// Per-model pricing in USD per million tokens.
#[derive(Debug, Clone, Copy)]
pub struct ModelPricing {
    pub input_per_mtok: f64,
    pub cached_input_per_mtok: f64,
    pub output_per_mtok: f64,
    pub context_window: u64,
}

const fn model_pricing(
    input_per_mtok: f64,
    cached_input_per_mtok: f64,
    output_per_mtok: f64,
    context_window: u64,
) -> ModelPricing {
    ModelPricing {
        input_per_mtok,
        cached_input_per_mtok,
        output_per_mtok,
        context_window,
    }
}

const O3_PRICING: ModelPricing = model_pricing(2.0, 0.50, 8.0, 200_000);
const O4_PRICING: ModelPricing = model_pricing(10.0, 2.50, 40.0, 200_000);
const GPT_41_PRICING: ModelPricing = model_pricing(2.0, 0.50, 8.0, 1_000_000);
const GPT_41_MINI_PRICING: ModelPricing = model_pricing(0.40, 0.10, 1.60, 1_000_000);
const GPT_4O_MINI_PRICING: ModelPricing = model_pricing(0.15, 0.075, 0.60, 128_000);
const GPT_4O_PRICING: ModelPricing = model_pricing(2.50, 1.25, 10.0, 128_000);

/// Compute cost in USD from token counts and pricing.
/// Uses `non_cached_input_tokens` and `cached_tokens` separately to avoid
/// double-counting the cached portion.
pub fn cost_usd(
    non_cached_input_tokens: u64,
    cached_tokens: u64,
    output_tokens: u64,
    pricing: &ModelPricing,
) -> f64 {
    (non_cached_input_tokens as f64 * pricing.input_per_mtok
        + cached_tokens as f64 * pricing.cached_input_per_mtok
        + output_tokens as f64 * pricing.output_per_mtok)
        / 1_000_000.0
}

/// Look up pricing for a model name. Falls back to gpt-4.1 pricing for
/// unknown models so cost is never silently zero.
pub fn pricing_for_model(model: &str) -> ModelPricing {
    let m = model.to_ascii_lowercase();
    if m.starts_with("o3") || m.starts_with("o4-mini") {
        O3_PRICING
    } else if m.starts_with("o4") {
        O4_PRICING
    } else if m.starts_with("gpt-5") {
        GPT_41_PRICING
    } else if m.starts_with("gpt-4.1-mini") || m.starts_with("gpt-4.1-nano") {
        GPT_41_MINI_PRICING
    } else if m.starts_with("gpt-4.1") {
        GPT_41_PRICING
    } else if m.starts_with("gpt-4o-mini") {
        GPT_4O_MINI_PRICING
    } else if m.starts_with("gpt-4o") {
        GPT_4O_PRICING
    } else {
        // Fallback: gpt-4.1 pricing
        GPT_41_PRICING
    }
}

/// Estimate token count for a string.
/// Heuristic: ~4 ASCII chars per token, ~1.5 CJK chars per token.
pub fn estimate_tokens(text: &str) -> usize {
    let (cjk_count, char_count) = text.chars().fold((0, 0), |(cjk, total), character| {
        (cjk + is_cjk_or_fullwidth(character) as usize, total + 1)
    });
    let ascii_count = char_count - cjk_count;
    (cjk_count as f64 / 1.5).ceil() as usize + (ascii_count as f64 / 4.0).ceil() as usize
}

fn is_cjk_or_fullwidth(character: char) -> bool {
    ('\u{4E00}'..='\u{9FFF}').contains(&character)
        || ('\u{3400}'..='\u{4DBF}').contains(&character)
        || ('\u{F900}'..='\u{FAFF}').contains(&character)
        || ('\u{3000}'..='\u{303F}').contains(&character)
        || ('\u{FF00}'..='\u{FFEF}').contains(&character)
}

/// Estimate tokens for a JSON value (stringifies it first)
pub fn estimate_json_tokens(value: &Value) -> usize {
    estimate_tokens(&value.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_usd_splits_cached_and_non_cached() {
        let pricing = pricing_for_model("gpt-4.1");
        // 4000 non-cached input @ $2/M + 8000 cached @ $0.50/M + 1500 output @ $8/M
        let cost = cost_usd(4000, 8000, 1500, &pricing);
        let expected = (4000.0 * 2.0 + 8000.0 * 0.50 + 1500.0 * 8.0) / 1_000_000.0;
        assert!((cost - expected).abs() < 1e-10);
    }

    #[test]
    fn pricing_for_model_returns_known_models() {
        let p = pricing_for_model("gpt-5.4");
        assert_eq!(p.context_window, 1_000_000);

        let p = pricing_for_model("o3");
        assert_eq!(p.context_window, 200_000);

        let p = pricing_for_model("gpt-4o-mini");
        assert!(p.input_per_mtok < 1.0);
    }

    #[test]
    fn pricing_for_unknown_model_uses_fallback() {
        let p = pricing_for_model("unknown-model-v99");
        assert!(p.input_per_mtok > 0.0);
        assert!(p.context_window > 0);
    }
}
