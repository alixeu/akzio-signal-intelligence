//! The sole terminal writer exposed to the Historical Reflection profile.
//!
//! It deliberately accepts an analysis submission, not paths, IDs, timestamps
//! or an Experience operation. The workflow service owns task completion,
//! immutable Artifact persistence and `record_experience_case` idempotency.

use std::{fmt, sync::Arc};

use anyhow::{bail, Context, Result};
use orchestrator_core::{PatternIdentityV1, ReflectionDisposition, RuleRevisionV1, ToolId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{api_tool_name, ToolDefinition};

pub const FINALIZE_HISTORICAL_REFLECTION_NAME: &str = ToolId::FinalizeHistoricalReflection.as_str();

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LearnedRuleSubmission {
    pub rule: String,
    pub trigger_conditions: Vec<String>,
    pub invalidation_conditions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalReflectionSubmission {
    pub disposition: ReflectionDisposition,
    pub summary: String,
    pub detail: String,
    pub confidence: Option<f64>,
    pub root_cause_phase: Option<u8>,
    pub propagation_phases: Vec<u8>,
    pub source_refs: Vec<String>,
    pub pattern_identity: Option<PatternIdentityV1>,
    pub learned_rule: Option<LearnedRuleSubmission>,
}

impl HistoricalReflectionSubmission {
    pub fn validate(&self) -> Result<()> {
        if self.summary.trim().is_empty()
            || self.detail.trim().is_empty()
            || self.source_refs.is_empty()
            || self
                .source_refs
                .iter()
                .any(|reference| reference.trim().is_empty())
            || self
                .root_cause_phase
                .is_some_and(|phase| phase == 0 || phase > 8)
            || self
                .propagation_phases
                .iter()
                .any(|phase| *phase == 0 || *phase > 8)
        {
            bail!("reflection submission has empty text, source references, or invalid phases");
        }
        let learned = self.disposition == ReflectionDisposition::Learned;
        let contested = self.disposition == ReflectionDisposition::Contested;
        if learned != (self.pattern_identity.is_some() && self.learned_rule.is_some()) {
            bail!("only Learned requires both pattern_identity and learned_rule");
        }
        if !learned
            && !contested
            && (self.pattern_identity.is_some() || self.learned_rule.is_some())
        {
            bail!("only Learned or Contested may identify a Pattern");
        }
        if contested && self.learned_rule.is_some() {
            bail!("Contested may match an existing Pattern but may not submit a RuleRevision");
        }
        if learned != self.confidence.is_some()
            || self
                .confidence
                .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
        {
            bail!("only Learned requires a finite confidence in 0..=1");
        }
        if let Some(pattern) = &self.pattern_identity {
            if self.root_cause_phase != Some(pattern.root_cause_phase)
                || pattern.source_role.trim().is_empty()
            {
                bail!("PatternIdentity must agree with root_cause_phase and name a source role");
            }
        }
        if let Some(rule) = &self.learned_rule {
            if rule.rule.trim().is_empty()
                || rule
                    .trigger_conditions
                    .iter()
                    .any(|item| item.trim().is_empty())
                || rule
                    .invalidation_conditions
                    .iter()
                    .any(|item| item.trim().is_empty())
            {
                bail!("learned_rule contains empty fields");
            }
        }
        Ok(())
    }

    pub fn rule_revision(&self) -> Option<RuleRevisionV1> {
        self.learned_rule.as_ref().map(|rule| RuleRevisionV1 {
            revision: 1,
            rule: rule.rule.trim().to_owned(),
            trigger_conditions: rule.trigger_conditions.clone(),
            invalidation_conditions: rule.invalidation_conditions.clone(),
        })
    }
}

pub trait HistoricalReflectionTerminalService: Send + Sync {
    fn finalize(&self, submission: HistoricalReflectionSubmission) -> Result<Value>;
}

#[derive(Clone)]
pub struct HistoricalReflectionTerminalBinding {
    service: Arc<dyn HistoricalReflectionTerminalService>,
}

impl fmt::Debug for HistoricalReflectionTerminalBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoricalReflectionTerminalBinding")
            .field("service", &"HistoricalReflectionTerminalService")
            .finish()
    }
}

impl HistoricalReflectionTerminalBinding {
    pub fn new(service: Arc<dyn HistoricalReflectionTerminalService>) -> Self {
        Self { service }
    }

    pub fn execute(&self, arguments: Value) -> Result<Value> {
        let submission: HistoricalReflectionSubmission = serde_json::from_value(arguments)
            .context("finalize_historical_reflection arguments are invalid")?;
        submission.validate()?;
        self.service
            .finalize(submission)
            .map(|artifact| json!({"status":"completed", "terminal":true, "artifact":artifact}))
    }
}

pub fn definition() -> ToolDefinition {
    ToolDefinition {
        name: api_tool_name(FINALIZE_HISTORICAL_REFLECTION_NAME),
        description: "Terminally submit the audited Historical Reflection. Duplicate is determined by Rust and is not a model disposition. Learned is the only disposition eligible to add a support case.".to_owned(),
        parameters: json!({
            "type":"object",
            "properties":{
                "disposition":{"type":"string","enum":["learned","no_reusable_memory","deferred","contested"]},
                "summary":{"type":"string","minLength":1},
                "detail":{"type":"string","minLength":1},
                "confidence":{"type":["number","null"],"minimum":0.0,"maximum":1.0},
                "root_cause_phase":{"type":["integer","null"],"minimum":1,"maximum":8},
                "propagation_phases":{"type":"array","items":{"type":"integer","minimum":1,"maximum":8}},
                "source_refs":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},
                "pattern_identity":{"type":["object","null"]},
                "learned_rule":{"type":["object","null"]}
            },
            "required":["disposition","summary","detail","confidence","root_cause_phase","propagation_phases","source_refs","pattern_identity","learned_rule"],
            "additionalProperties":false
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_is_not_a_model_disposition() {
        let submission = serde_json::json!({
            "disposition":"duplicate",
            "summary":"x",
            "detail":"x",
            "confidence":null,
            "root_cause_phase":null,
            "propagation_phases":[],
            "source_refs":["summary"],
            "pattern_identity":null,
            "learned_rule":null
        });
        assert!(serde_json::from_value::<HistoricalReflectionSubmission>(submission).is_err());
    }

    #[test]
    fn learned_requires_structured_pattern_and_rule() {
        let submission: HistoricalReflectionSubmission = serde_json::from_value(json!({
            "disposition":"learned",
            "summary":"x",
            "detail":"x",
            "confidence":0.6,
            "root_cause_phase":2,
            "propagation_phases":[],
            "source_refs":["summary"],
            "pattern_identity":null,
            "learned_rule":null
        }))
        .unwrap();
        assert!(submission.validate().is_err());
    }

    #[test]
    fn contested_may_match_but_cannot_create_a_rule_revision() {
        let submission: HistoricalReflectionSubmission = serde_json::from_value(json!({
            "disposition":"contested",
            "summary":"counter evidence exists",
            "detail":"the cited summary contradicts the reusable condition",
            "confidence":null,
            "root_cause_phase":2,
            "propagation_phases":[],
            "source_refs":["summary"],
            "pattern_identity":{
                "root_cause_phase":2,
                "source_role":"manager.research",
                "scope":"ticker",
                "ticker":"QQQ",
                "horizon_trading_days":3,
                "regime":{"volatility":"","trend":"","liquidity":"","rates":"","breadth":""},
                "signal_family":"technical",
                "action_kind":"hold"
            },
            "learned_rule":null
        }))
        .unwrap();
        assert!(submission.validate().is_ok());
    }
}
