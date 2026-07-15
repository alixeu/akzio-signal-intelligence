use serde_json::Value;

pub const MAX_PROMPT_TOKENS: usize = 12_000;
pub const MAX_COMPLETION_TOKENS: usize = 4_096;

/// Per-model pricing in USD per million tokens.
#[derive(Debug, Clone, Copy)]
pub struct ModelPricing {
    pub input_per_mtok: f64,
    pub cached_input_per_mtok: f64,
    pub output_per_mtok: f64,
    pub context_window: u64,
}

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
        ModelPricing {
            input_per_mtok: 2.0,
            cached_input_per_mtok: 0.50,
            output_per_mtok: 8.0,
            context_window: 200_000,
        }
    } else if m.starts_with("o4") {
        ModelPricing {
            input_per_mtok: 10.0,
            cached_input_per_mtok: 2.50,
            output_per_mtok: 40.0,
            context_window: 200_000,
        }
    } else if m.starts_with("gpt-5") {
        ModelPricing {
            input_per_mtok: 2.0,
            cached_input_per_mtok: 0.50,
            output_per_mtok: 8.0,
            context_window: 1_000_000,
        }
    } else if m.starts_with("gpt-4.1-mini") || m.starts_with("gpt-4.1-nano") {
        ModelPricing {
            input_per_mtok: 0.40,
            cached_input_per_mtok: 0.10,
            output_per_mtok: 1.60,
            context_window: 1_000_000,
        }
    } else if m.starts_with("gpt-4.1") {
        ModelPricing {
            input_per_mtok: 2.0,
            cached_input_per_mtok: 0.50,
            output_per_mtok: 8.0,
            context_window: 1_000_000,
        }
    } else if m.starts_with("gpt-4o-mini") {
        ModelPricing {
            input_per_mtok: 0.15,
            cached_input_per_mtok: 0.075,
            output_per_mtok: 0.60,
            context_window: 128_000,
        }
    } else if m.starts_with("gpt-4o") {
        ModelPricing {
            input_per_mtok: 2.50,
            cached_input_per_mtok: 1.25,
            output_per_mtok: 10.0,
            context_window: 128_000,
        }
    } else {
        // Fallback: gpt-4.1 pricing
        ModelPricing {
            input_per_mtok: 2.0,
            cached_input_per_mtok: 0.50,
            output_per_mtok: 8.0,
            context_window: 1_000_000,
        }
    }
}

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
