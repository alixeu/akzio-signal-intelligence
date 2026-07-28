use serde::{Deserialize, Serialize};

pub const REFLECTION_TASK_SCHEMA_VERSION: u32 = 1;
pub const HISTORICAL_REFLECTION_ARTIFACT_SCHEMA_VERSION: u32 = 2;

/// Rust-derived identity for one immutable Outcome reflection attempt. The
/// model never receives authority to choose or alter these fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReflectionTaskKeyV1 {
    pub source_run_id: String,
    pub ticker: String,
    pub outcome_id: String,
    pub outcome_content_hash: String,
    pub policy_ref: super::evaluation::PolicyRef,
    pub profile_version: u32,
    pub builder_version: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReflectionTaskStatus {
    Pending,
    Claimed,
    Completed,
    NoReusableMemory,
    Duplicate,
    Deferred,
    Contested,
    Superseded,
    FailedRetryable,
    FailedPermanent,
}

/// The only terminal dispositions exposed to the historical reflector model.
/// Duplicate is deliberately absent: Store idempotency determines it after a
/// Learned submission reaches `record_experience_case`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReflectionDisposition {
    Learned,
    NoReusableMemory,
    Deferred,
    Contested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperienceOperation {
    AddSupport,
    AddContradiction,
    Supersede,
    Suspend,
    Reinstate,
    Retire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperienceState {
    Candidate,
    RepeatedWarning,
    Active,
    Contested,
    Suspended,
    Retired,
}

/// Deterministic lifecycle thresholds for rebuilding an Experience View.
/// The Event Ledger is independent of this policy; changing the policy only
/// requires rebuilding derived views with a new PolicyRef.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperienceLifecyclePolicyV1 {
    pub policy_ref: super::evaluation::PolicyRef,
    pub repeated_warning_min_support: u32,
    pub active_min_support: u32,
    pub active_min_date_clusters: u32,
    pub active_min_regime_clusters: u32,
    pub active_min_utility_ema_micros: i64,
    pub active_max_harmful_usage_rate_ppm: u32,
}

/// Versioned, deterministic limits for the Historical Reflection scheduler.
/// The default 6/2/2 split is a deployable starting point, not an invariant:
/// all task identity and scheduling receipts carry `policy_ref`, so changing a
/// quota or retry budget is auditable rather than an implicit behavior drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryPolicyV1 {
    pub policy_ref: super::evaluation::PolicyRef,
    pub reflection_total_quota: u32,
    pub reflection_new_outcome_quota: u32,
    pub reflection_retry_quota: u32,
    pub reflection_backlog_quota: u32,
    pub reflection_max_attempts: u32,
}

impl Default for MemoryPolicyV1 {
    fn default() -> Self {
        Self {
            policy_ref: super::evaluation::PolicyRef {
                policy_id: "memory.policy".to_owned(),
                version: 1,
                content_hash: "sha256:memory-policy-v1".to_owned(),
            },
            reflection_total_quota: 10,
            reflection_new_outcome_quota: 6,
            reflection_retry_quota: 2,
            reflection_backlog_quota: 2,
            reflection_max_attempts: 3,
        }
    }
}

impl MemoryPolicyV1 {
    pub fn is_valid(&self) -> bool {
        self.policy_ref.version > 0
            && !self.policy_ref.content_hash.trim().is_empty()
            && self.reflection_total_quota > 0
            && self.reflection_max_attempts > 0
            && self
                .reflection_new_outcome_quota
                .saturating_add(self.reflection_retry_quota)
                .saturating_add(self.reflection_backlog_quota)
                <= self.reflection_total_quota
    }
}

impl Default for ExperienceLifecyclePolicyV1 {
    fn default() -> Self {
        Self {
            policy_ref: super::evaluation::PolicyRef {
                policy_id: "experience.lifecycle".to_owned(),
                version: 1,
                content_hash: "sha256:experience-lifecycle-v1".to_owned(),
            },
            repeated_warning_min_support: 2,
            active_min_support: 3,
            active_min_date_clusters: 2,
            active_min_regime_clusters: 1,
            active_min_utility_ema_micros: 0,
            active_max_harmful_usage_rate_ppm: 250_000,
        }
    }
}

/// Stable semantic identity for an Experience. It intentionally excludes the
/// natural-language wording of a rule, so a rewrite of the same lesson does
/// not manufacture a new Pattern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatternIdentityV1 {
    pub root_cause_phase: u8,
    pub source_role: String,
    pub scope: Scope,
    pub ticker: Option<String>,
    pub horizon_trading_days: Option<u32>,
    pub regime: MarketRegime,
    pub signal_family: SignalFamily,
    pub action_kind: PatternActionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalFamily {
    Technical,
    Macro,
    Fundamental,
    Sentiment,
    CrossAsset,
    Risk,
    Execution,
    Process,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatternActionKind {
    Enter,
    Exit,
    Hold,
    Size,
    Hedge,
    Rebalance,
    Research,
    RiskControl,
    Execute,
}

/// Versioned wording attached to one stable Pattern identity. The wording is
/// evidence, never the primary identity key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleRevisionV1 {
    pub revision: u32,
    pub rule: String,
    pub trigger_conditions: Vec<String>,
    pub invalidation_conditions: Vec<String>,
}

/// Immutable auditable terminal record for every Historical Reflection task,
/// including terminals that intentionally create no reusable Experience.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalReflectionArtifactV1 {
    pub schema_version: u32,
    pub artifact_id: String,
    pub task_id: String,
    pub task_key: ReflectionTaskKeyV1,
    pub disposition: ReflectionDisposition,
    pub outcome_ref: super::evaluation::DocumentRef,
    pub source_refs: Vec<super::evaluation::DocumentRef>,
    pub summary: String,
    pub detail: String,
    pub root_cause_phase: Option<u8>,
    pub propagation_phases: Vec<u8>,
    pub pattern_identity: Option<PatternIdentityV1>,
    pub rule_revision: Option<RuleRevisionV1>,
    pub created_at: String,
    pub content_hash: String,
}

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
