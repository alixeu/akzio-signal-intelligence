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

#[cfg(test)]
mod tests {
    use super::*;

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
