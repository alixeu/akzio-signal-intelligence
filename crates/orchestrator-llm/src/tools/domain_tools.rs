//! Domain-only Phase 1 / Phase 3 ToolManaged contracts.
//!
//! The model can supply analysis values only.  Scope, paths, identity,
//! timestamps, and final Artifact construction remain in the FileStore
//! service supplied by the workflow.

use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Context, Result};
use orchestrator_core::artifact::{BindingRiskControl, Scenario, StopType};
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
pub const SET_TRADE_INTENT: &str = "set_trade_intent";
pub const APPEND_TRADE_BLOCKER: &str = "append_trade_blocker";
pub const FINALIZE_TRADE_INTENT: &str = "finalize_trade_intent";
pub const SET_RISK_ASSESSMENT: &str = "set_risk_assessment";
pub const SET_RISK_CONSTRAINTS: &str = "set_risk_constraints";
pub const FINALIZE_RISK_REVIEW: &str = "finalize_risk_review";
pub const SET_PORTFOLIO_ASSET_DECISION: &str = "set_portfolio_asset_decision";
pub const APPEND_BINDING_RISK_CONTROL: &str = "append_binding_risk_control";
pub const FINALIZE_PORTFOLIO_DECISION: &str = "finalize_portfolio_decision";
pub const SET_PHASE2_COMMON_GROUND: &str = "set_phase2_common_ground";
pub const CREATE_PHASE2_TOPIC: &str = "create_phase2_topic";
pub const FINALIZE_RESEARCHER_WARMUP: &str = "finalize_researcher_warmup";
pub const FINALIZE_TOPIC_GENERATION: &str = "finalize_topic_generation";
pub const CREATE_DEBATE_CLAIM: &str = "create_debate_claim";
pub const FINALIZE_DEBATE_SEED: &str = "finalize_debate_seed";
pub const RESPOND_TO_DEBATE_CLAIM: &str = "respond_to_debate_claim";
pub const FINALIZE_DEBATE_RESPONSE: &str = "finalize_debate_response";
pub const SET_CLAIM_STATUS: &str = "set_claim_status";
pub const ADD_AGREED_FACT: &str = "add_agreed_fact";
pub const SET_DECISION_HINGE: &str = "set_decision_hinge";
pub const ROUTE_DEBATE_STEER: &str = "route_debate_steer";
pub const SET_TOPIC_SOFT_CONTROL: &str = "set_topic_soft_control";
pub const FINALIZE_TOPIC_CONTROL: &str = "finalize_topic_control";

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeIntentCommand {
    pub action: String,
    pub execution_decision: String,
    pub entry_price: Option<String>,
    pub stop_loss: Option<String>,
    pub position_size_pct_max: f64,
    pub rationale: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeBlockerCommand {
    pub blocker: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskAssessmentCommand {
    pub argument: String,
    pub unique_risk_contribution: String,
    pub disagreement_with_prior: String,
    pub no_new_information: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskConstraintsCommand {
    pub recommended_adjustment: String,
    pub stop_type: StopType,
    pub max_drawdown_pct: f64,
    pub position_cap_pct: f64,
    pub rebalance_trigger: String,
    pub risk_off_trigger: String,
    pub review_window: String,
    pub cash_hedge_recommendation: String,
    pub constraint_confidence: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioAssetDecisionCommand {
    pub direction_constraint: String,
    pub execution_status: String,
    pub max_target_weight: f64,
    pub max_weight_delta: f64,
    pub execution_summary: String,
    pub investment_thesis: String,
    pub target_price: Option<String>,
    pub horizon: String,
    pub rationale: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingRiskControlCommand {
    pub control: BindingRiskControl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Phase2CommonGroundCommand {
    pub common_ground: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Phase2TopicCommand {
    pub topic: String,
    pub decision_hinge: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DebateClaimCommand {
    pub claim: String,
    pub confidence: f64,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebateResponseCommand {
    pub reply_to_claim_id: String,
    pub response: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimStatusCommand {
    pub claim_id: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextCommand {
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebateSteerCommand {
    pub target: String,
    pub instruction: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSoftControlCommand {
    pub should_continue: bool,
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
    SetTradeIntent(TradeIntentCommand),
    AppendTradeBlocker(TradeBlockerCommand),
    FinalizeTrade,
    SetRiskAssessment(RiskAssessmentCommand),
    SetRiskConstraints(RiskConstraintsCommand),
    FinalizeRisk,
    SetPortfolioAssetDecision(PortfolioAssetDecisionCommand),
    AppendBindingRiskControl(BindingRiskControlCommand),
    FinalizePortfolio,
    SetPhase2CommonGround(Phase2CommonGroundCommand),
    CreatePhase2Topic(Phase2TopicCommand),
    FinalizeResearcherWarmup,
    FinalizeTopicGeneration,
    CreateDebateClaim(DebateClaimCommand),
    FinalizeDebateSeed,
    RespondToDebateClaim(DebateResponseCommand),
    FinalizeDebateResponse,
    SetClaimStatus(ClaimStatusCommand),
    AddAgreedFact(TextCommand),
    SetDecisionHinge(TextCommand),
    RouteDebateSteer(DebateSteerCommand),
    SetTopicSoftControl(TopicSoftControlCommand),
    FinalizeTopicControl,
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
    fn set_trade_intent(&self, command: TradeIntentCommand) -> Result<Value>;
    fn append_trade_blocker(&self, command: TradeBlockerCommand) -> Result<Value>;
    fn finalize_trade_intent(&self) -> Result<Value>;
    fn set_risk_assessment(&self, command: RiskAssessmentCommand) -> Result<Value>;
    fn set_risk_constraints(&self, command: RiskConstraintsCommand) -> Result<Value>;
    fn finalize_risk_review(&self) -> Result<Value>;
    fn set_portfolio_asset_decision(&self, command: PortfolioAssetDecisionCommand)
        -> Result<Value>;
    fn append_binding_risk_control(&self, command: BindingRiskControlCommand) -> Result<Value>;
    fn finalize_portfolio_decision(&self) -> Result<Value>;
    fn set_phase2_common_ground(&self, _: Phase2CommonGroundCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn create_phase2_topic(&self, _: Phase2TopicCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn finalize_researcher_warmup(&self) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn finalize_topic_generation(&self) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn create_debate_claim(&self, _: DebateClaimCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn finalize_debate_seed(&self) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn respond_to_debate_claim(&self, _: DebateResponseCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn finalize_debate_response(&self) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn set_claim_status(&self, _: ClaimStatusCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn add_agreed_fact(&self, _: TextCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn set_decision_hinge(&self, _: TextCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn route_debate_steer(&self, _: DebateSteerCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn set_topic_soft_control(&self, _: TopicSoftControlCommand) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
    fn finalize_topic_control(&self) -> Result<Value> {
        bail!("Phase 2 domain runtime is not wired")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainToolScope {
    pub profile: ToolManagedProfile,
    pub tickers: BTreeSet<String>,
    pub visible_evidence_refs: BTreeSet<String>,
    /// Rust-derived Phase 2 claim IDs visible to this turn.  This is an
    /// allowlist, never a model-provided routing field.
    pub visible_claims: BTreeSet<String>,
}

/// A provenance-bearing read that the Rust tool runtime, rather than the
/// assistant, observed.  Domain writers may cite only `subject_id`s recorded
/// through this interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceReadRecord {
    pub tool_name: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub source_run_id: String,
    pub source_phase: u8,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub turn_id: String,
    pub session_id: String,
}

impl EvidenceReadRecord {
    pub fn validate(&self) -> Result<()> {
        if self.tool_name.trim().is_empty()
            || self.subject_kind.trim().is_empty()
            || self.subject_id.trim().is_empty()
            || self.source_run_id.trim().is_empty()
            || self.turn_id.trim().is_empty()
            || self.session_id.trim().is_empty()
        {
            bail!("evidence_read record has an empty required field")
        }
        Ok(())
    }
}

/// Dynamic evidence visibility for one ToolManaged runtime.  Implementations
/// may persist reads in the FileStore session log and inherit a fork's
/// immutable reads.  It intentionally exposes no model-controlled mutation.
pub trait EvidenceVisibility: Send + Sync {
    fn set_turn_context(&self, _context: &crate::agent_loop::ToolRuntimeTurnContext) -> Result<()> {
        Ok(())
    }
    fn record_evidence_read(&self, read: EvidenceReadRecord) -> Result<()>;
    fn contains(&self, reference: &str) -> Result<bool>;
}

/// Small in-memory implementation used by direct bindings and tests.  The
/// production workflow supplies a FileStore-backed implementation.
#[derive(Debug, Default)]
pub struct InMemoryEvidenceVisibility {
    references: Mutex<BTreeSet<String>>,
}

impl InMemoryEvidenceVisibility {
    pub fn seeded(references: impl IntoIterator<Item = String>) -> Self {
        Self {
            references: Mutex::new(references.into_iter().collect()),
        }
    }
}

impl EvidenceVisibility for InMemoryEvidenceVisibility {
    fn record_evidence_read(&self, read: EvidenceReadRecord) -> Result<()> {
        read.validate()?;
        self.references
            .lock()
            .map_err(|_| anyhow::anyhow!("evidence visibility lock poisoned"))?
            .insert(read.subject_id);
        Ok(())
    }

    fn contains(&self, reference: &str) -> Result<bool> {
        Ok(self
            .references
            .lock()
            .map_err(|_| anyhow::anyhow!("evidence visibility lock poisoned"))?
            .contains(reference))
    }
}

impl DomainToolScope {
    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.profile,
            ToolManagedProfile::AnalystReport
                | ToolManagedProfile::ResearcherWarmup
                | ToolManagedProfile::TopicGeneration
                | ToolManagedProfile::DebateSeed
                | ToolManagedProfile::DebateResponse
                | ToolManagedProfile::TopicControl
                | ToolManagedProfile::ResearchDecision
                | ToolManagedProfile::TradeIntent
                | ToolManagedProfile::RiskReview
                | ToolManagedProfile::PortfolioDecision
        ) {
            bail!("domain runtime only supports analyst_report or research_decision")
        }
        if self.tickers.is_empty()
            && !matches!(
                self.profile,
                ToolManagedProfile::ResearcherWarmup
                    | ToolManagedProfile::TopicGeneration
                    | ToolManagedProfile::DebateSeed
                    | ToolManagedProfile::DebateResponse
                    | ToolManagedProfile::TopicControl
            )
        {
            bail!("domain runtime requires a Rust-owned ticker allowlist")
        }
        if matches!(
            self.profile,
            ToolManagedProfile::TradeIntent
                | ToolManagedProfile::RiskReview
                | ToolManagedProfile::PortfolioDecision
        ) && self.tickers.len() != 1
        {
            bail!("this profile requires one Rust-owned ticker scope")
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct DomainToolRuntimeBinding {
    scope: DomainToolScope,
    service: Arc<dyn DomainToolService>,
    evidence_visibility: Arc<dyn EvidenceVisibility>,
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
        let visibility = Arc::new(InMemoryEvidenceVisibility::seeded(
            scope.visible_evidence_refs.iter().cloned(),
        ));
        Self::new_with_evidence_visibility(scope, service, visibility)
    }

    pub fn new_with_evidence_visibility(
        scope: DomainToolScope,
        service: Arc<dyn DomainToolService>,
        evidence_visibility: Arc<dyn EvidenceVisibility>,
    ) -> Result<Self> {
        scope.validate()?;
        Ok(Self {
            scope,
            service,
            evidence_visibility,
        })
    }
    pub fn scope(&self) -> &DomainToolScope {
        &self.scope
    }

    /// Called solely by the tool runtime after a successful structured read.
    /// Assistant text, tool arguments, and failed reads never reach this
    /// method.
    pub fn record_evidence_read(&self, read: EvidenceReadRecord) -> Result<()> {
        self.evidence_visibility.record_evidence_read(read)
    }

    pub fn set_turn_context(
        &self,
        context: &crate::agent_loop::ToolRuntimeTurnContext,
    ) -> Result<()> {
        self.evidence_visibility.set_turn_context(context)
    }

    pub fn execute(&self, name: &str, arguments: Value) -> Result<Value> {
        match prepare_command_with_visibility(name, arguments, &self.scope, |reference| {
            if self.scope.visible_evidence_refs.contains(reference) {
                Ok(true)
            } else {
                self.evidence_visibility.contains(reference)
            }
        })? {
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
            DomainToolCommand::SetTradeIntent(command) => self.service.set_trade_intent(command),
            DomainToolCommand::AppendTradeBlocker(command) => {
                self.service.append_trade_blocker(command)
            }
            DomainToolCommand::FinalizeTrade => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_trade_intent()?}),
            ),
            DomainToolCommand::SetRiskAssessment(command) => {
                self.service.set_risk_assessment(command)
            }
            DomainToolCommand::SetRiskConstraints(command) => {
                self.service.set_risk_constraints(command)
            }
            DomainToolCommand::FinalizeRisk => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_risk_review()?}),
            ),
            DomainToolCommand::SetPortfolioAssetDecision(command) => {
                self.service.set_portfolio_asset_decision(command)
            }
            DomainToolCommand::AppendBindingRiskControl(command) => {
                self.service.append_binding_risk_control(command)
            }
            DomainToolCommand::FinalizePortfolio => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_portfolio_decision()?}),
            ),
            DomainToolCommand::SetPhase2CommonGround(command) => {
                self.service.set_phase2_common_ground(command)
            }
            DomainToolCommand::CreatePhase2Topic(command) => {
                self.service.create_phase2_topic(command)
            }
            DomainToolCommand::FinalizeResearcherWarmup => Ok(json!({
                "status":"completed",
                "terminal":true,
                "warmup":self.service.finalize_researcher_warmup()?,
                "artifact":{"status":"ready","response":"准备完毕","kind":"researcher_warmup"}
            })),
            DomainToolCommand::FinalizeTopicGeneration => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_topic_generation()?}),
            ),
            DomainToolCommand::CreateDebateClaim(command) => {
                self.service.create_debate_claim(command)
            }
            DomainToolCommand::FinalizeDebateSeed => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_debate_seed()?}),
            ),
            DomainToolCommand::RespondToDebateClaim(command) => {
                self.service.respond_to_debate_claim(command)
            }
            DomainToolCommand::FinalizeDebateResponse => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_debate_response()?}),
            ),
            DomainToolCommand::SetClaimStatus(command) => self.service.set_claim_status(command),
            DomainToolCommand::AddAgreedFact(command) => self.service.add_agreed_fact(command),
            DomainToolCommand::SetDecisionHinge(command) => {
                self.service.set_decision_hinge(command)
            }
            DomainToolCommand::RouteDebateSteer(command) => {
                self.service.route_debate_steer(command)
            }
            DomainToolCommand::SetTopicSoftControl(command) => {
                self.service.set_topic_soft_control(command)
            }
            DomainToolCommand::FinalizeTopicControl => Ok(
                json!({"status":"completed","terminal":true,"artifact":self.service.finalize_topic_control()?}),
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
            | SET_TRADE_INTENT
            | APPEND_TRADE_BLOCKER
            | FINALIZE_TRADE_INTENT
            | SET_RISK_ASSESSMENT
            | SET_RISK_CONSTRAINTS
            | FINALIZE_RISK_REVIEW
            | SET_PORTFOLIO_ASSET_DECISION
            | APPEND_BINDING_RISK_CONTROL
            | FINALIZE_PORTFOLIO_DECISION
            | SET_PHASE2_COMMON_GROUND
            | CREATE_PHASE2_TOPIC
            | FINALIZE_RESEARCHER_WARMUP
            | FINALIZE_TOPIC_GENERATION
            | CREATE_DEBATE_CLAIM
            | FINALIZE_DEBATE_SEED
            | RESPOND_TO_DEBATE_CLAIM
            | FINALIZE_DEBATE_RESPONSE
            | SET_CLAIM_STATUS
            | ADD_AGREED_FACT
            | SET_DECISION_HINGE
            | ROUTE_DEBATE_STEER
            | SET_TOPIC_SOFT_CONTROL
            | FINALIZE_TOPIC_CONTROL
    )
}

/// The exact domain tools exposed for one ToolManaged profile.  The caller
/// supplies the profile from its Rust-owned authority binding; model text
/// never selects this allowlist.
pub fn tool_names_for_profile(profile: ToolManagedProfile) -> &'static [&'static str] {
    match profile {
        ToolManagedProfile::AnalystReport => &[
            SET_ANALYST_ASSESSMENT,
            APPEND_ANALYST_EVIDENCE,
            APPEND_ANALYST_DATA_GAP,
            SET_ANALYST_INVALIDATION,
            FINALIZE_ANALYST_REPORT,
        ],
        ToolManagedProfile::ResearchDecision => &[
            SET_RESEARCH_DECISION,
            SET_RESEARCH_SCENARIOS,
            APPEND_RESEARCH_HINGE,
            FINALIZE_RESEARCH_DECISION,
        ],
        ToolManagedProfile::TradeIntent => &[
            SET_TRADE_INTENT,
            APPEND_TRADE_BLOCKER,
            FINALIZE_TRADE_INTENT,
        ],
        ToolManagedProfile::RiskReview => &[
            SET_RISK_ASSESSMENT,
            SET_RISK_CONSTRAINTS,
            FINALIZE_RISK_REVIEW,
        ],
        ToolManagedProfile::PortfolioDecision => &[
            SET_PORTFOLIO_ASSET_DECISION,
            APPEND_BINDING_RISK_CONTROL,
            FINALIZE_PORTFOLIO_DECISION,
        ],
        ToolManagedProfile::ResearcherWarmup => &[FINALIZE_RESEARCHER_WARMUP],
        ToolManagedProfile::TopicGeneration => &[
            SET_PHASE2_COMMON_GROUND,
            CREATE_PHASE2_TOPIC,
            FINALIZE_TOPIC_GENERATION,
        ],
        ToolManagedProfile::DebateSeed => &[CREATE_DEBATE_CLAIM, FINALIZE_DEBATE_SEED],
        ToolManagedProfile::DebateResponse => &[RESPOND_TO_DEBATE_CLAIM, FINALIZE_DEBATE_RESPONSE],
        ToolManagedProfile::TopicControl => &[
            SET_CLAIM_STATUS,
            ADD_AGREED_FACT,
            SET_DECISION_HINGE,
            ROUTE_DEBATE_STEER,
            SET_TOPIC_SOFT_CONTROL,
            FINALIZE_TOPIC_CONTROL,
        ],
        _ => &[],
    }
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
            "Append one Canonical Contract v2 source-backed evidence item for an assessed ticker. `evidence` MUST be a JSON object matching this schema, never a serialized JSON string. evidence_type is required; do not use a legacy `type` field.",
            json!({
                "ticker":{"type":"string"},
                "evidence":{
                    "type":"object",
                    "properties":{
                        "claim":{"type":"string","minLength":1},
                        "evidence_type":{"type":"string","enum":["fact","opinion","inference"]},
                        "source":{"type":"string","minLength":1},
                        "timestamp":{"type":"string","minLength":1},
                        "source_tier":{"type":"string","enum":["official","major_media","professional_research","longform_analysis","unknown"]},
                        "first_source":{"type":"string"},
                        "is_derivative_repost":{"type":"boolean"},
                        "evidence_age":{"type":"string"},
                        "source_confidence":{"type":"number","minimum":0.0,"maximum":1.0}
                    },
                    "required":["claim","evidence_type","source","timestamp","source_tier","source_confidence"],
                    "additionalProperties":false
                },
                "evidence_ref":{"type":"string","minLength":1}
            }),
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
        SET_TRADE_INTENT => (
            "Set the Phase 4 intent for the single Rust-scoped ticker. Candidate action is derived by Rust and cannot be supplied.",
            json!({"action":{"type":"string","enum":["Buy","Sell","Hold"]},"execution_decision":{"type":"string","enum":["execute_candidate","hold"]},"entry_price":{"type":["string","null"]},"stop_loss":{"type":["string","null"]},"position_size_pct_max":{"type":"number","minimum":0.0,"maximum":1.0},"rationale":{"type":"string","minLength":1}}),
            json!(["action", "execution_decision", "entry_price", "stop_loss", "position_size_pct_max", "rationale"]),
        ),
        APPEND_TRADE_BLOCKER => (
            "Append one concrete blocker for the scoped trade intent.",
            json!({"blocker":{"type":"string","minLength":1}}),
            json!(["blocker"]),
        ),
        FINALIZE_TRADE_INTENT => (
            "Terminally validate candidate-action and numeric-cap semantics, then atomically finalize the scoped trade intent.",
            json!({}),
            json!([]),
        ),
        SET_RISK_ASSESSMENT => (
            "Set the Phase 5 assessment for the Rust-scoped ticker and role stance.",
            json!({"argument":{"type":"string"},"unique_risk_contribution":{"type":"string"},"disagreement_with_prior":{"type":"string"},"no_new_information":{"type":"boolean"}}),
            json!(["argument", "unique_risk_contribution", "disagreement_with_prior", "no_new_information"]),
        ),
        SET_RISK_CONSTRAINTS => (
            "Set numeric risk constraints. Stance is Rust-owned; stop_type is one of hard, soft, none.",
            json!({"recommended_adjustment":{"type":"string"},"stop_type":{"type":"string","enum":["hard","soft","none"]},"max_drawdown_pct":{"type":"number","minimum":0.0,"maximum":1.0},"position_cap_pct":{"type":"number","minimum":0.0,"maximum":1.0},"rebalance_trigger":{"type":"string"},"risk_off_trigger":{"type":"string"},"review_window":{"type":"string"},"cash_hedge_recommendation":{"type":"string"},"constraint_confidence":{"type":"number","minimum":0.0,"maximum":1.0}}),
            json!(["recommended_adjustment", "stop_type", "max_drawdown_pct", "position_cap_pct", "rebalance_trigger", "risk_off_trigger", "review_window", "cash_hedge_recommendation", "constraint_confidence"]),
        ),
        FINALIZE_RISK_REVIEW => (
            "Terminally validate and atomically finalize the scoped risk review.",
            json!({}),
            json!([]),
        ),
        SET_PORTFOLIO_ASSET_DECISION => (
            "Set the Phase 6 decision for the single Rust-scoped asset. Ticker, rating, and current weight are Rust-owned.",
            json!({"direction_constraint":{"type":"string","enum":["increase_only","decrease_only","unchanged"]},"execution_status":{"type":"string","enum":["execute","wait","downgrade"]},"max_target_weight":{"type":"number","minimum":0.0,"maximum":1.0},"max_weight_delta":{"type":"number","minimum":0.0,"maximum":1.0},"execution_summary":{"type":"string","minLength":1},"investment_thesis":{"type":"string"},"target_price":{"type":["string","null"]},"horizon":{"type":"string"},"rationale":{"type":"string"}}),
            json!(["direction_constraint", "execution_status", "max_target_weight", "max_weight_delta", "execution_summary", "investment_thesis", "target_price", "horizon", "rationale"]),
        ),
        APPEND_BINDING_RISK_CONTROL => (
            "Append one traceable Phase 5 binding control. Every source_ref must have been read in this session.",
            json!({"control":{"type":"object","properties":{"control":{"type":"string","minLength":1},"source_refs":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}},"required":["control","source_refs"],"additionalProperties":false}}),
            json!(["control"]),
        ),
        FINALIZE_PORTFOLIO_DECISION => (
            "Terminally validate direction/wait/downgrade constraints and atomically finalize the scoped portfolio decision.",
            json!({}),
            json!([]),
        ),
        SET_PHASE2_COMMON_GROUND => (
            "Set the shared Phase 2 common ground. Topic identity and scope stay Rust-owned.",
            json!({"common_ground":{"type":"string","minLength":1}}),
            json!(["common_ground"]),
        ),
        CREATE_PHASE2_TOPIC => (
            "Create one bounded debate topic. Rust derives the topic ID; every evidence reference must already be visible.",
            json!({"topic":{"type":"string","minLength":1},"decision_hinge":{"type":"string","minLength":1},"evidence_refs":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}),
            json!(["topic", "decision_hinge", "evidence_refs"]),
        ),
        FINALIZE_RESEARCHER_WARMUP => (
            "Terminally finish warmup after the required structured evidence reads. It writes no business artifact.",
            json!({}),
            json!([]),
        ),
        FINALIZE_TOPIC_GENERATION => (
            "Terminally validate and atomically finalize the planned topic generation artifact.",
            json!({}),
            json!([]),
        ),
        CREATE_DEBATE_CLAIM => (
            "Create one claim for the Rust-bound topic and side. Rust derives claim ID; cite visible evidence only.",
            json!({"claim":{"type":"string","minLength":1},"confidence":{"type":"number","minimum":0.0,"maximum":1.0},"evidence_refs":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}),
            json!(["claim", "confidence", "evidence_refs"]),
        ),
        FINALIZE_DEBATE_SEED => (
            "Terminally validate and atomically finalize the bound debate seed.",
            json!({}),
            json!([]),
        ),
        RESPOND_TO_DEBATE_CLAIM => (
            "Respond to one visible claim in the Rust-bound topic. The claim ID must be inherited or read, and evidence must be visible.",
            json!({"reply_to_claim_id":{"type":"string","minLength":1},"response":{"type":"string","minLength":1},"evidence_refs":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}}),
            json!(["reply_to_claim_id", "response", "evidence_refs"]),
        ),
        FINALIZE_DEBATE_RESPONSE => (
            "Terminally validate and atomically finalize the bound debate response.",
            json!({}),
            json!([]),
        ),
        SET_CLAIM_STATUS => (
            "Set the disposition of one visible claim for this topic controller.",
            json!({"claim_id":{"type":"string","minLength":1},"status":{"type":"string","enum":["accepted","rejected","unresolved","blocked"]}}),
            json!(["claim_id", "status"]),
        ),
        ADD_AGREED_FACT => (
            "Append one concise agreed fact to the bound topic controller draft.",
            json!({"value":{"type":"string","minLength":1}}),
            json!(["value"]),
        ),
        SET_DECISION_HINGE => (
            "Append one decision hinge to the bound topic controller draft.",
            json!({"value":{"type":"string","minLength":1}}),
            json!(["value"]),
        ),
        ROUTE_DEBATE_STEER => (
            "Route one concise Rust-bound follow-up steer to bull or bear.",
            json!({"target":{"type":"string","enum":["bull","bear"]},"instruction":{"type":"string","minLength":1}}),
            json!(["target", "instruction"]),
        ),
        SET_TOPIC_SOFT_CONTROL => (
            "Set whether this topic should continue. This does not bypass the Rust maximum-round control.",
            json!({"should_continue":{"type":"boolean"}}),
            json!(["should_continue"]),
        ),
        FINALIZE_TOPIC_CONTROL => (
            "Terminally validate and atomically finalize the bound topic controller artifact.",
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
    prepare_command_with_visibility(name, arguments, scope, |reference| {
        Ok(scope.visible_evidence_refs.contains(reference))
    })
}

fn prepare_command_with_visibility<F>(
    name: &str,
    arguments: Value,
    scope: &DomainToolScope,
    mut is_visible: F,
) -> Result<DomainToolCommand>
where
    F: FnMut(&str) -> Result<bool>,
{
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
        ToolManagedProfile::TradeIntent => matches!(
            name,
            SET_TRADE_INTENT | APPEND_TRADE_BLOCKER | FINALIZE_TRADE_INTENT
        ),
        ToolManagedProfile::RiskReview => matches!(
            name,
            SET_RISK_ASSESSMENT | SET_RISK_CONSTRAINTS | FINALIZE_RISK_REVIEW
        ),
        ToolManagedProfile::PortfolioDecision => matches!(
            name,
            SET_PORTFOLIO_ASSET_DECISION
                | APPEND_BINDING_RISK_CONTROL
                | FINALIZE_PORTFOLIO_DECISION
        ),
        ToolManagedProfile::ResearcherWarmup => name == FINALIZE_RESEARCHER_WARMUP,
        ToolManagedProfile::TopicGeneration => matches!(
            name,
            SET_PHASE2_COMMON_GROUND | CREATE_PHASE2_TOPIC | FINALIZE_TOPIC_GENERATION
        ),
        ToolManagedProfile::DebateSeed => {
            matches!(name, CREATE_DEBATE_CLAIM | FINALIZE_DEBATE_SEED)
        }
        ToolManagedProfile::DebateResponse => {
            matches!(name, RESPOND_TO_DEBATE_CLAIM | FINALIZE_DEBATE_RESPONSE)
        }
        ToolManagedProfile::TopicControl => matches!(
            name,
            SET_CLAIM_STATUS
                | ADD_AGREED_FACT
                | SET_DECISION_HINGE
                | ROUTE_DEBATE_STEER
                | SET_TOPIC_SOFT_CONTROL
                | FINALIZE_TOPIC_CONTROL
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
        if !model_owned_fields(name).contains(&key.as_str()) {
            bail!("{name}.{key} is not a declared model-owned field")
        }
    }
    let command = match name {
        SET_ANALYST_ASSESSMENT => DomainToolCommand::SetAnalystAssessment(parse(arguments)?),
        APPEND_ANALYST_EVIDENCE => {
            let command: AnalystEvidenceCommand = parse(arguments)?;
            require_visible(&command.evidence_ref, &mut is_visible)?;
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
            require_visible(&command.evidence_ref, &mut is_visible)?;
            DomainToolCommand::AppendResearchHinge(command)
        }
        FINALIZE_RESEARCH_DECISION => {
            empty(object, name)?;
            DomainToolCommand::FinalizeResearch
        }
        SET_TRADE_INTENT => DomainToolCommand::SetTradeIntent(parse(arguments)?),
        APPEND_TRADE_BLOCKER => DomainToolCommand::AppendTradeBlocker(parse(arguments)?),
        FINALIZE_TRADE_INTENT => {
            empty(object, name)?;
            DomainToolCommand::FinalizeTrade
        }
        SET_RISK_ASSESSMENT => DomainToolCommand::SetRiskAssessment(parse(arguments)?),
        SET_RISK_CONSTRAINTS => DomainToolCommand::SetRiskConstraints(parse(arguments)?),
        FINALIZE_RISK_REVIEW => {
            empty(object, name)?;
            DomainToolCommand::FinalizeRisk
        }
        SET_PORTFOLIO_ASSET_DECISION => {
            DomainToolCommand::SetPortfolioAssetDecision(parse(arguments)?)
        }
        APPEND_BINDING_RISK_CONTROL => {
            let command: BindingRiskControlCommand = parse(arguments)?;
            for reference in &command.control.source_refs {
                require_visible(reference, &mut is_visible)?;
            }
            DomainToolCommand::AppendBindingRiskControl(command)
        }
        FINALIZE_PORTFOLIO_DECISION => {
            empty(object, name)?;
            DomainToolCommand::FinalizePortfolio
        }
        SET_PHASE2_COMMON_GROUND => DomainToolCommand::SetPhase2CommonGround(parse(arguments)?),
        CREATE_PHASE2_TOPIC => {
            let command: Phase2TopicCommand = parse(arguments)?;
            for reference in &command.evidence_refs {
                require_visible(reference, &mut is_visible)?;
            }
            DomainToolCommand::CreatePhase2Topic(command)
        }
        FINALIZE_RESEARCHER_WARMUP => {
            empty(object, name)?;
            DomainToolCommand::FinalizeResearcherWarmup
        }
        FINALIZE_TOPIC_GENERATION => {
            empty(object, name)?;
            DomainToolCommand::FinalizeTopicGeneration
        }
        CREATE_DEBATE_CLAIM => {
            let command: DebateClaimCommand = parse(arguments)?;
            for reference in &command.evidence_refs {
                require_visible(reference, &mut is_visible)?;
            }
            DomainToolCommand::CreateDebateClaim(command)
        }
        FINALIZE_DEBATE_SEED => {
            empty(object, name)?;
            DomainToolCommand::FinalizeDebateSeed
        }
        RESPOND_TO_DEBATE_CLAIM => {
            let command: DebateResponseCommand = parse(arguments)?;
            for reference in &command.evidence_refs {
                require_visible(reference, &mut is_visible)?;
            }
            DomainToolCommand::RespondToDebateClaim(command)
        }
        FINALIZE_DEBATE_RESPONSE => {
            empty(object, name)?;
            DomainToolCommand::FinalizeDebateResponse
        }
        SET_CLAIM_STATUS => DomainToolCommand::SetClaimStatus(parse(arguments)?),
        ADD_AGREED_FACT => DomainToolCommand::AddAgreedFact(parse(arguments)?),
        SET_DECISION_HINGE => DomainToolCommand::SetDecisionHinge(parse(arguments)?),
        ROUTE_DEBATE_STEER => DomainToolCommand::RouteDebateSteer(parse(arguments)?),
        SET_TOPIC_SOFT_CONTROL => DomainToolCommand::SetTopicSoftControl(parse(arguments)?),
        FINALIZE_TOPIC_CONTROL => {
            empty(object, name)?;
            DomainToolCommand::FinalizeTopicControl
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

fn model_owned_fields(name: &str) -> &'static [&'static str] {
    match name {
        SET_ANALYST_ASSESSMENT => &[
            "ticker",
            "direction",
            "confidence",
            "report",
            "priced_in",
            "echo_chamber_risk",
            "crowded_consensus_risk",
        ],
        APPEND_ANALYST_EVIDENCE => &["ticker", "evidence", "evidence_ref"],
        APPEND_ANALYST_DATA_GAP => &["ticker", "data_gap"],
        SET_ANALYST_INVALIDATION => &["ticker", "validation_triggers"],
        FINALIZE_ANALYST_REPORT
        | FINALIZE_RESEARCH_DECISION
        | FINALIZE_TRADE_INTENT
        | FINALIZE_RISK_REVIEW
        | FINALIZE_PORTFOLIO_DECISION
        | FINALIZE_RESEARCHER_WARMUP
        | FINALIZE_TOPIC_GENERATION
        | FINALIZE_DEBATE_SEED
        | FINALIZE_DEBATE_RESPONSE
        | FINALIZE_TOPIC_CONTROL => &[],
        SET_RESEARCH_DECISION => &[
            "ticker",
            "rating",
            "long_probability",
            "short_probability",
            "confidence_basis",
            "hold_reason",
            "plan",
            "probability_rationale",
        ],
        SET_RESEARCH_SCENARIOS => &["ticker", "bull", "base", "bear"],
        APPEND_RESEARCH_HINGE => &["ticker", "hinge", "evidence_ref"],
        SET_TRADE_INTENT => &[
            "action",
            "execution_decision",
            "entry_price",
            "stop_loss",
            "position_size_pct_max",
            "rationale",
        ],
        APPEND_TRADE_BLOCKER => &["blocker"],
        SET_RISK_ASSESSMENT => &[
            "argument",
            "unique_risk_contribution",
            "disagreement_with_prior",
            "no_new_information",
        ],
        SET_RISK_CONSTRAINTS => &[
            "recommended_adjustment",
            "stop_type",
            "max_drawdown_pct",
            "position_cap_pct",
            "rebalance_trigger",
            "risk_off_trigger",
            "review_window",
            "cash_hedge_recommendation",
            "constraint_confidence",
        ],
        SET_PORTFOLIO_ASSET_DECISION => &[
            "direction_constraint",
            "execution_status",
            "max_target_weight",
            "max_weight_delta",
            "execution_summary",
            "investment_thesis",
            "target_price",
            "horizon",
            "rationale",
        ],
        APPEND_BINDING_RISK_CONTROL => &["control"],
        SET_PHASE2_COMMON_GROUND => &["common_ground"],
        CREATE_PHASE2_TOPIC => &["topic", "decision_hinge", "evidence_refs"],
        CREATE_DEBATE_CLAIM => &["claim", "confidence", "evidence_refs"],
        RESPOND_TO_DEBATE_CLAIM => &["reply_to_claim_id", "response", "evidence_refs"],
        SET_CLAIM_STATUS => &["claim_id", "status"],
        ADD_AGREED_FACT | SET_DECISION_HINGE => &["value"],
        ROUTE_DEBATE_STEER => &["target", "instruction"],
        SET_TOPIC_SOFT_CONTROL => &["should_continue"],
        _ => &[],
    }
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
fn require_visible<F>(reference: &str, is_visible: &mut F) -> Result<()>
where
    F: FnMut(&str) -> Result<bool>,
{
    if !is_visible(reference)? {
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
            visible_claims: Default::default(),
        }
    }

    fn scoped_profile(profile: ToolManagedProfile) -> DomainToolScope {
        DomainToolScope {
            profile,
            tickers: ["QQQ".to_owned()].into_iter().collect(),
            visible_evidence_refs: ["phase5:neutral:QQQ".to_owned()].into_iter().collect(),
            visible_claims: Default::default(),
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
    fn analyst_evidence_schema_exposes_only_canonical_v2_fields() {
        let definition = definition(APPEND_ANALYST_EVIDENCE).unwrap();
        let evidence = &definition.parameters["properties"]["evidence"];
        assert_eq!(
            evidence["properties"]["evidence_type"]["enum"],
            json!(["fact", "opinion", "inference"])
        );
        assert_eq!(evidence["additionalProperties"], json!(false));
        assert!(evidence["properties"].get("type").is_none());
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

    #[test]
    fn trade_contract_rejects_rust_owned_candidate_and_ticker() {
        let scope = scoped_profile(ToolManagedProfile::TradeIntent);
        assert!(prepare_command(
            SET_TRADE_INTENT,
            json!({
                "action":"Buy", "execution_decision":"execute_candidate",
                "entry_price":null, "stop_loss":null, "position_size_pct_max":0.2,
                "rationale":"confirmed", "candidate_action":"Buy"
            }),
            &scope,
        )
        .is_err());
        assert!(prepare_command(
            SET_TRADE_INTENT,
            json!({
                "action":"Buy", "execution_decision":"execute_candidate",
                "entry_price":null, "stop_loss":null, "position_size_pct_max":0.2,
                "rationale":"confirmed", "ticker":"QQQ"
            }),
            &scope,
        )
        .is_err());
    }

    #[test]
    fn risk_contract_only_allows_v2_stop_types() {
        let scope = scoped_profile(ToolManagedProfile::RiskReview);
        assert!(prepare_command(
            SET_RISK_CONSTRAINTS,
            json!({
                "recommended_adjustment":"cap", "stop_type":"event_based",
                "max_drawdown_pct":0.05, "position_cap_pct":0.2,
                "rebalance_trigger":"weekly", "risk_off_trigger":"breakdown",
                "review_window":"one day", "cash_hedge_recommendation":"cash",
                "constraint_confidence":0.6
            }),
            &scope,
        )
        .is_err());
        assert!(matches!(
            prepare_command(
                SET_RISK_CONSTRAINTS,
                json!({
                    "recommended_adjustment":"cap", "stop_type":"hard",
                    "max_drawdown_pct":0.05, "position_cap_pct":0.2,
                    "rebalance_trigger":"weekly", "risk_off_trigger":"breakdown",
                    "review_window":"one day", "cash_hedge_recommendation":"cash",
                    "constraint_confidence":0.6
                }),
                &scope,
            )
            .unwrap(),
            DomainToolCommand::SetRiskConstraints(_)
        ));
    }

    #[test]
    fn portfolio_binding_controls_require_visible_sources() {
        let scope = scoped_profile(ToolManagedProfile::PortfolioDecision);
        assert!(prepare_command(
            APPEND_BINDING_RISK_CONTROL,
            json!({"control":{"control":"cap exposure","source_refs":["not-visible"]}}),
            &scope,
        )
        .is_err());
        assert!(matches!(
            prepare_command(
                APPEND_BINDING_RISK_CONTROL,
                json!({"control":{"control":"cap exposure","source_refs":["phase5:neutral:QQQ"]}}),
                &scope,
            )
            .unwrap(),
            DomainToolCommand::AppendBindingRiskControl(_)
        ));
    }
}
