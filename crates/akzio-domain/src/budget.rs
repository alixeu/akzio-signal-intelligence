//! Agent resource governance, independent of provider context-window capacity.
use crate::{DomainError, TaskBudget};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentSettings {
    pub budget: AgentBudgetConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentBudgetConfig {
    pub default: BudgetOverride,
    pub planner: BudgetOverride,
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

pub const AGENT_ROLES: [(&str, &str); 5] = [
    ("planner", "research.planner"),
    ("analyst", "research.analyst"),
    ("critic", "research.critic"),
    ("synthesizer", "research.synthesizer"),
    ("outcome_worker", "learning.outcome_worker"),
];

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

/// Defaults for newly created Runs, independent of immutable Contract defaults.
pub fn default_agent_budget(purpose: &str) -> Option<TaskBudget> {
    let mut budget = legacy_contract_budget(purpose)?;
    budget.max_input_tokens = 1_000_000;
    budget.max_tool_calls = ToolCallLimit::Unlimited;
    Some(budget)
}

impl AgentBudgetConfig {
    pub fn validate(&self) -> Result<(), DomainError> {
        for value in [
            &self.default,
            &self.planner,
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
            "research.planner" => &self.planner,
            "research.analyst" => &self.analyst,
            "research.critic" => &self.critic,
            "research.synthesizer" => &self.synthesizer,
            "learning.outcome_worker" => &self.outcome_worker,
            _ => return None,
        };
        let mut budget = default_agent_budget(purpose)?;
        self.default.apply(&mut budget);
        role.apply(&mut budget);
        Some(budget)
    }
    pub fn resolved(&self) -> BTreeMap<String, TaskBudget> {
        AGENT_ROLES
            .into_iter()
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
    fn legacy_budget_wire_format_is_unchanged() {
        let raw = r#"{"max_input_tokens":48000,"max_output_tokens":6000,"max_wall_time_secs":120,"max_tool_calls":4}"#;
        let budget: TaskBudget = serde_json::from_str(raw).unwrap();
        assert_eq!(budget, legacy_contract_budget("research.analyst").unwrap());
        assert_eq!(serde_json::to_string(&budget).unwrap(), raw);
        assert!(ToolCallLimit::Unlimited > ToolCallLimit::Limited(u16::MAX));
    }
}
