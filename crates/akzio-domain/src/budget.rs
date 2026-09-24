// 文件导读：定义 Agent 任务的输入/输出、工具次数和墙钟预算，以及历史 Contract
// 默认值、新 Run 默认值和配置覆盖的分层解析规则。
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
    // 拒绝显式 0 和超过应用输出上限的值；工具数为 0 仍代表合法的无读工具策略。
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
    // 只覆盖配置中显式提供的字段，未提供字段保留传入 TaskBudget。
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
// 按旧 purpose 返回冻结的历史预算；未注册 purpose 返回 None。
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
// 将 ProposalReviewer 归并到 Critic 角色，并只把研究输出改为新的应用上限。
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
// 按角色返回新 Run 的预算，并把研究任务墙钟提升到有界的 180 秒。
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
    // 逐项验证全局和各角色覆盖，不修改配置本身。
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
    // 将 purpose 映射到角色覆盖，先应用全局覆盖，再应用角色覆盖。
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
    // 为注册的四类 Agent 和额外 ProposalReviewer 生成完整的确定性预算表。
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
    // 有限上限返回数值；Unlimited 不暴露人为伪造的有限值。
    pub fn finite(self) -> Option<u16> {
        match self {
            Self::Limited(value) => Some(value),
            Self::Unlimited => None,
        }
    }
    // used 不超过有限上限时允许继续；无限上限总是允许。
    pub fn allows(self, used: u64) -> bool {
        self.finite().is_none_or(|limit| used <= u64::from(limit))
    }
    // 计算剩余次数；无限上限返回 None 表示没有可枚举的剩余额度。
    pub fn remaining(self, used: u64) -> Option<u64> {
        self.finite()
            .map(|limit| u64::from(limit).saturating_sub(used))
    }
}

impl Serialize for ToolCallLimit {
    // 保持旧 wire 格式：有限值是整数，无限值是字符串 unlimited。
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Limited(value) => serializer.serialize_u16(*value),
            Self::Unlimited => serializer.serialize_str("unlimited"),
        }
    }
}

impl<'de> Deserialize<'de> for ToolCallLimit {
    // 通过无标签枚举同时接受历史整数和明确的 unlimited 字符串，拒绝其他文本。
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
    // 新研究预算提升输出和墙钟，但 Outcome 仍保留独立的 4k 输出和原超时。
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
    // 覆盖值允许达到上限，0 或超过上限必须失败。
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
    // Contract 版本升级只改变研究输出预算，其他历史维度保持不变。
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
    // ProposalReviewer 与 Critic 共享角色预算覆盖，但都不回写旧 Contract 默认值。
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
    // TaskBudget 的 JSON 整数/字符串编码保持与历史序列化完全兼容。
    fn legacy_budget_wire_format_is_unchanged() {
        let raw = r#"{"max_input_tokens":48000,"max_output_tokens":6000,"max_wall_time_secs":120,"max_tool_calls":4}"#;
        let budget: TaskBudget = serde_json::from_str(raw).unwrap();
        assert_eq!(budget, legacy_contract_budget("research.analyst").unwrap());
        assert_eq!(serde_json::to_string(&budget).unwrap(), raw);
        assert!(ToolCallLimit::Unlimited > ToolCallLimit::Limited(u16::MAX));
    }
}
