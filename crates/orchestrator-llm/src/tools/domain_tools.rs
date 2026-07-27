//! Domain-only Phase 1 / Phase 3 ToolManaged contracts.
//!
//! The model can supply analysis values only.  Scope, paths, identity,
//! timestamps, and final Artifact construction remain in the FileStore
//! service supplied by the workflow.

use std::{collections::BTreeSet, fmt, sync::Arc};

use anyhow::{bail, Context, Result};
use orchestrator_core::artifact::Scenario;
use orchestrator_core::{EvidenceItem, ToolManagedProfile};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{api_tool_name, ToolDefinition};

pub const SET_ANALYST_ASSESSMENT: &str = "set_analyst_assessment";
pub const APPEND_ANALYST_EVIDENCE: &str = "append_analyst_evidence";
pub const APPEND_ANALYST_DATA_GAP: &str = "append_analyst_data_gap";
pub const SET_ANALYST_INVALIDATION: &str = "set_analyst_invalidation";
pub const FINALIZE_ANALYST_REPORT: &str = "finalize_analyst_report";
pub const SET_RESEARCH_DECISION: &str = "set_research_decision";
pub const SET_RESEARCH_SCENARIOS: &str = "set_research_scenarios";
pub const APPEND_RESEARCH_HINGE: &str = "append_research_hinge";
pub const FINALIZE_RESEARCH_DECISION: &str = "finalize_research_decision";

const RUST_OWNED_FIELDS: &[&str] = &[
    "store_root",
    "path",
    "source_path",
    "run_id",
    "phase",
    "role",
    "kind",
    "round",
    "session_id",
    "turn_id",
    "ticker_scope",
    "topic_id",
    "artifact_id",
    "created_at",
    "schema_version",
    "content_hash",
    "source_payload_hash",
    "profile",
    "profile_version",
    "builder_version",
    "unit_key",
    "candidate_action",
    "side",
    "stance",
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystAssessmentCommand {
    pub ticker: String,
    pub direction: String,
    pub confidence: f64,
    pub report: String,
    pub priced_in: String,
    pub echo_chamber_risk: String,
    pub crowded_consensus_risk: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystEvidenceCommand {
    pub ticker: String,
    pub evidence: EvidenceItem,
    pub evidence_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystDataGapCommand {
    pub ticker: String,
    pub data_gap: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystInvalidationCommand {
    pub ticker: String,
    pub validation_triggers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchDecisionCommand {
    pub ticker: String,
    pub rating: String,
    pub long_probability: f64,
    pub short_probability: f64,
    pub confidence_basis: String,
    pub hold_reason: Option<String>,
    pub plan: String,
    pub probability_rationale: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchScenariosCommand {
    pub ticker: String,
    pub bull: Scenario,
    pub base: Scenario,
    pub bear: Scenario,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchHingeCommand {
    pub ticker: String,
    pub hinge: String,
    pub evidence_ref: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DomainToolCommand {
    SetAnalystAssessment(AnalystAssessmentCommand),
    AppendAnalystEvidence(AnalystEvidenceCommand),
    AppendAnalystDataGap(AnalystDataGapCommand),
    SetAnalystInvalidation(AnalystInvalidationCommand),
    FinalizeAnalyst,
    SetResearchDecision(ResearchDecisionCommand),
    SetResearchScenarios(ResearchScenariosCommand),
    AppendResearchHinge(ResearchHingeCommand),
    FinalizeResearch,
}

/// Typed bridge implemented only by a FileStore phase runtime.  It exposes no
/// database, filesystem, JSON-path, or arbitrary-field operation.
pub trait DomainToolService: Send + Sync {
    fn set_analyst_assessment(&self, command: AnalystAssessmentCommand) -> Result<Value>;
    fn append_analyst_evidence(&self, command: AnalystEvidenceCommand) -> Result<Value>;
    fn append_analyst_data_gap(&self, command: AnalystDataGapCommand) -> Result<Value>;
    fn set_analyst_invalidation(&self, command: AnalystInvalidationCommand) -> Result<Value>;
    fn finalize_analyst_report(&self) -> Result<Value>;
    fn set_research_decision(&self, command: ResearchDecisionCommand) -> Result<Value>;
    fn set_research_scenarios(&self, command: ResearchScenariosCommand) -> Result<Value>;
    fn append_research_hinge(&self, command: ResearchHingeCommand) -> Result<Value>;
    fn finalize_research_decision(&self) -> Result<Value>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainToolScope {
    pub profile: ToolManagedProfile,
    pub tickers: BTreeSet<String>,
    pub visible_evidence_refs: BTreeSet<String>,
}

impl DomainToolScope {
    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.profile,
            ToolManagedProfile::AnalystReport | ToolManagedProfile::ResearchDecision
        ) {
            bail!("domain runtime only supports analyst_report or research_decision")
        }
        if self.tickers.is_empty() {
            bail!("domain runtime requires a Rust-owned ticker allowlist")
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct DomainToolRuntimeBinding {
    scope: DomainToolScope,
    service: Arc<dyn DomainToolService>,
}
impl fmt::Debug for DomainToolRuntimeBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DomainToolRuntimeBinding")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}
impl DomainToolRuntimeBinding {
    pub fn new(scope: DomainToolScope, service: Arc<dyn DomainToolService>) -> Result<Self> {
        scope.validate()?;
        Ok(Self { scope, service })
    }
    pub fn scope(&self) -> &DomainToolScope {
        &self.scope
    }
    pub fn execute(&self, name: &str, arguments: Value) -> Result<Value> {
        match prepare_command(name, arguments, &self.scope)? {
            DomainToolCommand::SetAnalystAssessment(command) => {
                self.service.set_analyst_assessment(command)
            }
            DomainToolCommand::AppendAnalystEvidence(command) => {
                self.service.append_analyst_evidence(command)
            }
            DomainToolCommand::AppendAnalystDataGap(command) => {
                self.service.append_analyst_data_gap(command)
            }
            DomainToolCommand::SetAnalystInvalidation(command) => {
                self.service.set_analyst_invalidation(command)
            }
            DomainToolCommand::FinalizeAnalyst => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_analyst_report()?}),
            ),
            DomainToolCommand::SetResearchDecision(command) => {
                self.service.set_research_decision(command)
            }
            DomainToolCommand::SetResearchScenarios(command) => {
                self.service.set_research_scenarios(command)
            }
            DomainToolCommand::AppendResearchHinge(command) => {
                self.service.append_research_hinge(command)
            }
            DomainToolCommand::FinalizeResearch => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_research_decision()?}),
            ),
        }
    }
}

pub fn is_domain_tool(name: &str) -> bool {
    matches!(
        name,
        SET_ANALYST_ASSESSMENT
            | APPEND_ANALYST_EVIDENCE
            | APPEND_ANALYST_DATA_GAP
            | SET_ANALYST_INVALIDATION
            | FINALIZE_ANALYST_REPORT
            | SET_RESEARCH_DECISION
            | SET_RESEARCH_SCENARIOS
            | APPEND_RESEARCH_HINGE
            | FINALIZE_RESEARCH_DECISION
    )
}

pub fn definition(name: &str) -> Option<ToolDefinition> {
    let (description, properties, required) = match name {
        SET_ANALYST_ASSESSMENT => (
            "Set the assessment for one runtime-authorized ticker.",
            json!({"ticker":{"type":"string"},"direction":{"type":"string","enum":["bullish","bearish","neutral","mixed","unobserved"]},"confidence":{"type":"number","minimum":0.0,"maximum":1.0},"report":{"type":"string","minLength":1},"priced_in":{"type":"string","enum":["already_priced","under_priced","unclear"]},"echo_chamber_risk":{"type":"string","enum":["low","medium","high"]},"crowded_consensus_risk":{"type":"string","enum":["low","medium","high"]}}),
            json!([
                "ticker",
                "direction",
                "confidence",
                "report",
                "priced_in",
                "echo_chamber_risk",
                "crowded_consensus_risk"
            ]),
        ),
        APPEND_ANALYST_EVIDENCE => (
            "Append one source-backed evidence item for an assessed ticker.",
            json!({"ticker":{"type":"string"},"evidence":{"type":"object"},"evidence_ref":{"type":"string","minLength":1}}),
            json!(["ticker", "evidence", "evidence_ref"]),
        ),
        APPEND_ANALYST_DATA_GAP => (
            "Append one data gap for an assessed ticker.",
            json!({"ticker":{"type":"string"},"data_gap":{"type":"string","minLength":1}}),
            json!(["ticker", "data_gap"]),
        ),
        SET_ANALYST_INVALIDATION => (
            "Set observable invalidation triggers for an assessed ticker.",
            json!({"ticker":{"type":"string"},"validation_triggers":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}),
            json!(["ticker", "validation_triggers"]),
        ),
        FINALIZE_ANALYST_REPORT => (
            "Terminally validate and atomically finalize the planned analyst artifact.",
            json!({}),
            json!([]),
        ),
        SET_RESEARCH_DECISION => (
            "Set one runtime-authorized ticker decision and probabilities.",
            json!({"ticker":{"type":"string"},"rating":{"type":"string"},"long_probability":{"type":"number","minimum":0.0,"maximum":1.0},"short_probability":{"type":"number","minimum":0.0,"maximum":1.0},"confidence_basis":{"type":"string"},"hold_reason":{"type":["string","null"]},"plan":{"type":"string","minLength":1},"probability_rationale":{"type":"string","minLength":1}}),
            json!([
                "ticker",
                "rating",
                "long_probability",
                "short_probability",
                "confidence_basis",
                "hold_reason",
                "plan",
                "probability_rationale"
            ]),
        ),
        SET_RESEARCH_SCENARIOS => (
            "Set bull/base/bear scenarios for a decision already set for this ticker.",
            json!({"ticker":{"type":"string"},"bull":{"type":"object"},"base":{"type":"object"},"bear":{"type":"object"}}),
            json!(["ticker", "bull", "base", "bear"]),
        ),
        APPEND_RESEARCH_HINGE => (
            "Append a decision hinge backed by a visible evidence reference.",
            json!({"ticker":{"type":"string"},"hinge":{"type":"string","minLength":1},"evidence_ref":{"type":"string","minLength":1}}),
            json!(["ticker", "hinge", "evidence_ref"]),
        ),
        FINALIZE_RESEARCH_DECISION => (
            "Terminally validate and atomically finalize the planned research artifact.",
            json!({}),
            json!([]),
        ),
        _ => return None,
    };
    Some(ToolDefinition {
        name: api_tool_name(name),
        description: description.to_owned(),
        parameters: json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
    })
}

pub fn prepare_command(
    name: &str,
    arguments: Value,
    scope: &DomainToolScope,
) -> Result<DomainToolCommand> {
    scope.validate()?;
    let expected = match scope.profile {
        ToolManagedProfile::AnalystReport => matches!(
            name,
            SET_ANALYST_ASSESSMENT
                | APPEND_ANALYST_EVIDENCE
                | APPEND_ANALYST_DATA_GAP
                | SET_ANALYST_INVALIDATION
                | FINALIZE_ANALYST_REPORT
        ),
        ToolManagedProfile::ResearchDecision => matches!(
            name,
            SET_RESEARCH_DECISION
                | SET_RESEARCH_SCENARIOS
                | APPEND_RESEARCH_HINGE
                | FINALIZE_RESEARCH_DECISION
        ),
        _ => false,
    };
    if !expected {
        bail!("{name} is not available to this ToolManaged profile")
    }
    let object = arguments
        .as_object()
        .context("domain tool arguments must be an object")?;
    for key in object.keys() {
        if RUST_OWNED_FIELDS.contains(&key.as_str()) {
            bail!("{name}.{key} is Rust-owned and must not be supplied by the model")
        }
    }
    let command = match name {
        SET_ANALYST_ASSESSMENT => DomainToolCommand::SetAnalystAssessment(parse(arguments)?),
        APPEND_ANALYST_EVIDENCE => {
            let command: AnalystEvidenceCommand = parse(arguments)?;
            require_visible(scope, &command.evidence_ref)?;
            DomainToolCommand::AppendAnalystEvidence(command)
        }
        APPEND_ANALYST_DATA_GAP => DomainToolCommand::AppendAnalystDataGap(parse(arguments)?),
        SET_ANALYST_INVALIDATION => DomainToolCommand::SetAnalystInvalidation(parse(arguments)?),
        FINALIZE_ANALYST_REPORT => {
            empty(object, name)?;
            DomainToolCommand::FinalizeAnalyst
        }
        SET_RESEARCH_DECISION => DomainToolCommand::SetResearchDecision(parse(arguments)?),
        SET_RESEARCH_SCENARIOS => DomainToolCommand::SetResearchScenarios(parse(arguments)?),
        APPEND_RESEARCH_HINGE => {
            let command: ResearchHingeCommand = parse(arguments)?;
            require_visible(scope, &command.evidence_ref)?;
            DomainToolCommand::AppendResearchHinge(command)
        }
        FINALIZE_RESEARCH_DECISION => {
            empty(object, name)?;
            DomainToolCommand::FinalizeResearch
        }
        _ => bail!("unknown domain tool {name}"),
    };
    match &command {
        DomainToolCommand::SetAnalystAssessment(c) => ticker(scope, &c.ticker)?,
        DomainToolCommand::AppendAnalystEvidence(c) => ticker(scope, &c.ticker)?,
        DomainToolCommand::AppendAnalystDataGap(c) => ticker(scope, &c.ticker)?,
        DomainToolCommand::SetAnalystInvalidation(c) => ticker(scope, &c.ticker)?,
        DomainToolCommand::SetResearchDecision(c) => ticker(scope, &c.ticker)?,
        DomainToolCommand::SetResearchScenarios(c) => ticker(scope, &c.ticker)?,
        DomainToolCommand::AppendResearchHinge(c) => ticker(scope, &c.ticker)?,
        _ => {}
    }
    Ok(command)
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(Into::into)
}
fn ticker(scope: &DomainToolScope, value: &str) -> Result<()> {
    if !scope.tickers.contains(value) {
        bail!("ticker is not in the Rust-owned allowlist")
    }
    Ok(())
}
fn require_visible(scope: &DomainToolScope, reference: &str) -> Result<()> {
    if !scope.visible_evidence_refs.contains(reference) {
        bail!("evidence_ref is not visible in this turn")
    }
    Ok(())
}
fn empty(object: &serde_json::Map<String, Value>, tool: &str) -> Result<()> {
    if object.is_empty() {
        Ok(())
    } else {
        bail!("{tool} accepts no model-owned fields")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyst_scope() -> DomainToolScope {
        DomainToolScope {
            profile: ToolManagedProfile::AnalystReport,
            tickers: ["QQQ".to_owned()].into_iter().collect(),
            visible_evidence_refs: ["evidence:technical:QQQ".to_owned()].into_iter().collect(),
        }
    }

    #[test]
    fn analyst_evidence_requires_a_visible_reference() {
        let arguments = json!({
            "ticker":"QQQ", "evidence_ref":"not-visible",
            "evidence": {
                "claim":"support held", "evidence_type":"fact", "source":"technical",
                "timestamp":"2026-07-27", "source_tier":"official", "first_source":"technical",
                "is_derivative_repost":false, "evidence_age":"0-2d", "source_confidence":0.9
            }
        });
        assert!(prepare_command(APPEND_ANALYST_EVIDENCE, arguments, &analyst_scope()).is_err());
    }

    #[test]
    fn terminal_tool_is_profile_bound_and_has_no_arguments() {
        assert!(matches!(
            prepare_command(FINALIZE_ANALYST_REPORT, json!({}), &analyst_scope()).unwrap(),
            DomainToolCommand::FinalizeAnalyst
        ));
        assert!(prepare_command(FINALIZE_RESEARCH_DECISION, json!({}), &analyst_scope()).is_err());
        assert!(prepare_command(
            FINALIZE_ANALYST_REPORT,
            json!({"run_id":"bad"}),
            &analyst_scope()
        )
        .is_err());
    }
}
