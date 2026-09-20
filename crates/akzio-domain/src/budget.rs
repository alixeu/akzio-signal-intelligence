//! Agent resource governance, independent of provider context-window capacity.
use crate::{DomainError, TaskBudget};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentSettings {
    pub research: crate::ResearchSettings,
    pub budget: AgentBudgetConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentBudgetConfig {
    pub default: BudgetOverride,
    pub analyst: BudgetOverride,
    pub critic: BudgetOverride,
    pub synthesizer: BudgetOverride,
    pub outcome_worker: BudgetOverride,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BudgetOverride {
    pub max_input_tokens: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub max_tool_calls: Option<ToolCallLimit>,
    pub timeout_seconds: Option<u32>,
}

impl BudgetOverride {
    fn validate(&self) -> Result<(), DomainError> {
        for (field, value) in [
            ("agent.budget.max_input_tokens", self.max_input_tokens),
            ("agent.budget.max_output_tokens", self.max_output_tokens),
            ("agent.budget.timeout_seconds", self.timeout_seconds),
        ] {
            if value == Some(0) {
                return Err(DomainError::InvalidBudget { field });
            }
        }
        if self
            .max_output_tokens
            .is_some_and(|value| value > MAX_AGENT_OUTPUT_TOKENS)
        {
            return Err(DomainError::InvalidBudget {
                field: "agent.budget.max_output_tokens",
            });
        }
        // Zero read tools is a valid governance policy; submit_result is separate.
        Ok(())
    }
    fn apply(&self, budget: &mut TaskBudget) {
        if let Some(v) = self.max_input_tokens {
            budget.max_input_tokens = v;
        }
        if let Some(v) = self.max_output_tokens {
            budget.max_output_tokens = v;
        }
        if let Some(v) = self.max_tool_calls {
            budget.max_tool_calls = v;
        }
        if let Some(v) = self.timeout_seconds {
            budget.max_wall_time_secs = v;
        }
    }
}

pub const AGENT_ROLES: [(&str, &str); 4] = [
    ("analyst", "research.analyst"),
    ("critic", "research.critic"),
    ("synthesizer", "research.synthesizer"),
    ("outcome_worker", "learning.outcome_worker"),
];

/// Application output ceiling for newly configured tasks. This is neither an
/// input-token budget nor a provider context-window declaration.
pub const MAX_AGENT_OUTPUT_TOKENS: u32 = 1_000_000;
pub const OUTPUT_BUDGET_RESEARCH_CONTRACT_VERSION: u32 = 49;

/// Immutable legacy Contract defaults; preserve existing hashes and ContextPolicy.
pub fn legacy_contract_budget(purpose: &str) -> Option<TaskBudget> {
    let (input, output, tools, timeout) = match purpose {
        "research.planner" => (12_000, 2_000, 4, 120),
        "research.analyst" => (48_000, 6_000, 4, 120),
        "research.critic" => (48_000, 4_000, 4, 120),
        "research.synthesizer" => (48_000, 5_000, 2, 120),
        "learning.outcome_worker" => (12_000, 4_000, 2, 180),
        _ => return None,
    };
    Some(TaskBudget {
        max_input_tokens: input,
        max_output_tokens: output,
        max_tool_calls: ToolCallLimit::Limited(tools),
        max_wall_time_secs: timeout,
    })
}

/// Contract 49 changes only the research output dimension. Historical Contract
/// objects keep their original serialized budgets; ContextPolicy is separate.
pub fn versioned_contract_budget(purpose: &str) -> Option<TaskBudget> {
    let purpose = if purpose == "research.proposal_reviewer" {
        "research.critic"
    } else {
        purpose
    };
    let (input, output, tools, timeout) = match purpose {
        "research.analyst" | "research.critic" => (48_000, MAX_AGENT_OUTPUT_TOKENS, 4, 120),
        "research.synthesizer" => (48_000, MAX_AGENT_OUTPUT_TOKENS, 2, 120),
        "learning.outcome_worker" => (12_000, 4_000, 2, 180),
        _ => return None,
    };
    Some(TaskBudget {
        max_input_tokens: input,
        max_output_tokens: output,
        max_tool_calls: ToolCallLimit::Limited(tools),
        max_wall_time_secs: timeout,
    })
}

/// Defaults for newly created Runs, independent of immutable Contract defaults.
pub fn default_agent_budget(purpose: &str) -> Option<TaskBudget> {
    let mut budget = versioned_contract_budget(purpose)?;
    budget.max_input_tokens = 1_000_000;
    budget.max_tool_calls = ToolCallLimit::Unlimited;
    // Real structured research exhausted 120s both while waiting for Critic
    // and while repairing an Analyst submission. Give new research Runs one
    // bounded 180s Attempt; existing serialized budgets remain unchanged.
    if matches!(
        purpose,
        "research.analyst" | "research.critic" | "research.synthesizer"
    ) {
        budget.max_wall_time_secs = 180;
    }
    Some(budget)
}

impl AgentBudgetConfig {
    pub fn validate(&self) -> Result<(), DomainError> {
        for value in [
            &self.default,
            &self.analyst,
            &self.critic,
            &self.synthesizer,
            &self.outcome_worker,
        ] {
            value.validate()?;
        }
        Ok(())
    }
    pub fn resolve(&self, purpose: &str) -> Option<TaskBudget> {
        let role = match purpose {
            "research.analyst" => &self.analyst,
            "research.critic" | "research.proposal_reviewer" => &self.critic,
            "research.synthesizer" => &self.synthesizer,
            "learning.outcome_worker" => &self.outcome_worker,
            _ => return None,
        };
        let mut budget = default_agent_budget(if purpose == "research.proposal_reviewer" {
            "research.critic"
        } else {
            purpose
        })?;
        self.default.apply(&mut budget);
        role.apply(&mut budget);
        Some(budget)
    }
    pub fn resolved(&self) -> BTreeMap<String, TaskBudget> {
        AGENT_ROLES
            .into_iter()
            .chain([("proposal_reviewer", "research.proposal_reviewer")])
            .map(|(_, purpose)| {
                (
                    purpose.to_owned(),
                    self.resolve(purpose).expect("registered role"),
                )
            })
            .collect()
    }
}

/// Finite values retain their legacy numeric wire representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ToolCallLimit {
    Limited(u16),
    Unlimited,
}

impl ToolCallLimit {
    pub fn finite(self) -> Option<u16> {
        match self {
            Self::Limited(value) => Some(value),
            Self::Unlimited => None,
        }
    }
    pub fn allows(self, used: u64) -> bool {
        self.finite().is_none_or(|limit| used <= u64::from(limit))
    }
    pub fn remaining(self, used: u64) -> Option<u64> {
        self.finite()
            .map(|limit| u64::from(limit).saturating_sub(used))
    }
}

impl Serialize for ToolCallLimit {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Limited(value) => serializer.serialize_u16(*value),
            Self::Unlimited => serializer.serialize_str("unlimited"),
        }
    }
}

impl<'de> Deserialize<'de> for ToolCallLimit {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Limited(u16),
            Named(String),
        }
        match Wire::deserialize(deserializer)? {
            Wire::Limited(value) => Ok(Self::Limited(value)),
            Wire::Named(value) if value == "unlimited" => Ok(Self::Unlimited),
            Wire::Named(_) => Err(serde::de::Error::custom(
                "max_tool_calls must be an integer 0..=65535 or 'unlimited'",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_output_defaults_allow_a_million_with_role_appropriate_timeouts() {
        for (_, purpose) in AGENT_ROLES {
            let legacy = legacy_contract_budget(purpose).unwrap();
            let budget = default_agent_budget(purpose).unwrap();
            assert_eq!(budget.max_input_tokens, 1_000_000);
            assert_eq!(budget.max_tool_calls, ToolCallLimit::Unlimited);
            assert_eq!(
                budget.max_wall_time_secs,
                if matches!(
                    purpose,
                    "research.analyst" | "research.critic" | "research.synthesizer"
                ) {
                    180
                } else {
                    legacy.max_wall_time_secs
                }
            );
            assert_eq!(
                budget.max_output_tokens,
                if purpose.starts_with("research.") {
                    1_000_000
                } else {
                    4_000
                }
            );
        }
    }

    #[test]
    fn output_override_rejects_more_than_the_application_ceiling() {
        for limit in [1, 1_000_000] {
            let mut settings = AgentBudgetConfig::default();
            settings.default.max_output_tokens = Some(limit);
            assert!(settings.validate().is_ok());
        }
        for limit in [0, 1_000_001, u32::MAX] {
            let mut settings = AgentBudgetConfig::default();
            settings.critic.max_output_tokens = Some(limit);
            assert!(settings.validate().is_err(), "output {limit}");
        }
    }

    #[test]
    fn versioned_contract_changes_only_research_output() {
        for (_, purpose) in AGENT_ROLES {
            let mut expected = legacy_contract_budget(purpose).unwrap();
            if purpose.starts_with("research.") {
                expected.max_output_tokens = 1_000_000;
            }
            assert_eq!(versioned_contract_budget(purpose).unwrap(), expected);
        }
    }

    #[test]
    fn new_critic_budget_is_distinct_from_legacy_contract_budget() {
        assert_eq!(
            legacy_contract_budget("research.critic")
                .expect("registered Critic role")
                .max_output_tokens,
            4_000
        );
        assert_eq!(
            default_agent_budget("research.critic")
                .expect("registered Critic role")
                .max_output_tokens,
            1_000_000
        );

        let mut settings = AgentBudgetConfig::default();
        settings.critic.max_output_tokens = Some(16_000);
        assert_eq!(
            settings.resolve("research.proposal_reviewer"),
            settings.resolve("research.critic")
        );
        assert_eq!(
            settings
                .resolve("research.critic")
                .expect("registered Critic role")
                .max_output_tokens,
            16_000
        );
    }

    #[test]
    fn legacy_budget_wire_format_is_unchanged() {
        let raw = r#"{"max_input_tokens":48000,"max_output_tokens":6000,"max_wall_time_secs":120,"max_tool_calls":4}"#;
        let budget: TaskBudget = serde_json::from_str(raw).unwrap();
        assert_eq!(budget, legacy_contract_budget("research.analyst").unwrap());
        assert_eq!(serde_json::to_string(&budget).unwrap(), raw);
        assert!(ToolCallLimit::Unlimited > ToolCallLimit::Limited(u16::MAX));
    }
}
