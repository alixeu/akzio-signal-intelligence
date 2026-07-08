use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Ticker,
    Sector,
    Theme,
    Macro,
    MarketRegime,
    Strategy,
    Agent,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ticker => "ticker",
            Self::Sector => "sector",
            Self::Theme => "theme",
            Self::Macro => "macro",
            Self::MarketRegime => "market_regime",
            Self::Strategy => "strategy",
            Self::Agent => "agent",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketRegime {
    pub volatility: String,
    #[serde(default)]
    pub trend: String,
    #[serde(default)]
    pub liquidity: String,
    #[serde(default)]
    pub rates: String,
    #[serde(default)]
    pub breadth: String,
}

impl MarketRegime {
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        regime_dimension_matches(&self.volatility, &other.volatility)
            && regime_dimension_matches(&self.trend, &other.trend)
            && regime_dimension_matches(&self.liquidity, &other.liquidity)
            && regime_dimension_matches(&self.rates, &other.rates)
            && regime_dimension_matches(&self.breadth, &other.breadth)
    }
}

fn regime_dimension_matches(left: &str, right: &str) -> bool {
    left.trim().is_empty() || right.trim().is_empty() || left == right
}

pub trait QualityScorer: Send + Sync {
    fn score(&self, input: &MemoryQualityInput) -> f64;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MemoryQualityInput {
    pub confidence: f64,
    pub sample_count: usize,
    pub recent_success_rate: f64,
    pub days_since_observed: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultQualityScorer;

impl QualityScorer for DefaultQualityScorer {
    fn score(&self, input: &MemoryQualityInput) -> f64 {
        let sample_ratio = (input.sample_count as f64 / 10.0).min(1.0);
        let recency = (1.0 - (input.days_since_observed / 300.0).min(0.9)).max(0.1);
        input.confidence * sample_ratio * input.recent_success_rate * recency
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetrievalBudget {
    pub token_budget: usize,
    pub max_items: usize,
    pub min_quality: f64,
}

impl Default for RetrievalBudget {
    fn default() -> Self {
        Self {
            token_budget: 4000,
            max_items: 20,
            min_quality: 0.6,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_quality_scorer_uses_confidence_sample_success_and_recency() {
        let scorer = DefaultQualityScorer;
        let score = scorer.score(&MemoryQualityInput {
            confidence: 0.8,
            sample_count: 5,
            recent_success_rate: 0.75,
            days_since_observed: 30.0,
        });

        assert!((score - 0.27).abs() < 1e-9);
    }

    #[test]
    fn retrieval_budget_defaults_are_conservative() {
        let budget = RetrievalBudget::default();
        assert_eq!(budget.token_budget, 4000);
        assert_eq!(budget.max_items, 20);
        assert_eq!(budget.min_quality, 0.6);
    }

    #[test]
    fn empty_regime_dimensions_are_wildcards() {
        let memory = MarketRegime {
            volatility: "elevated".to_string(),
            ..Default::default()
        };
        let current = MarketRegime {
            volatility: "elevated".to_string(),
            trend: "bull".to_string(),
            ..Default::default()
        };
        assert!(memory.is_compatible_with(&current));
    }
}
