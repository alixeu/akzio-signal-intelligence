//! Typed commands that a ToolManaged role may issue to its Rust-owned domain
//! service.  These carry model-provided analysis only; scope and persistence
//! identity remain owned by the runtime.

use serde::{Deserialize, Serialize};

use crate::artifact::Scenario;
use crate::{BindingRiskControl, EvidenceItem, StopType};

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
pub enum DomainCommand {
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

impl DomainCommand {
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::FinalizeAnalyst
                | Self::FinalizeResearch
                | Self::FinalizeTrade
                | Self::FinalizeRisk
                | Self::FinalizePortfolio
                | Self::FinalizeResearcherWarmup
                | Self::FinalizeTopicGeneration
                | Self::FinalizeDebateSeed
                | Self::FinalizeDebateResponse
                | Self::FinalizeTopicControl
        )
    }

    pub const fn is_researcher_warmup(&self) -> bool {
        matches!(self, Self::FinalizeResearcherWarmup)
    }
}
