//! Typed Draft builders for the first migrated business profiles.
//!
//! These functions are intentionally small and domain-specific.  They are the
//! only mutable route for AnalystReport and ResearchDecision Drafts; callers
//! cannot select a path or apply arbitrary JSON patches.

use std::{collections::BTreeMap, path::PathBuf};

use orchestrator_core::artifact::{Scenario, Scenarios};
use orchestrator_core::{
    validate_analyst_ticker_artifact, validate_final_validation, validate_research_artifact,
    validate_risk_constraints, validate_trade_intent, AnalystTickerArtifact,
    AssetExecutionConstraint, BindingRiskControl, EvidenceItem, FinalValidation, RiskConstraints,
    StopType, TradeIntent,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    content_hash, create_or_recover_draft, draft::mutate_draft, draft::read_draft,
    finalize_draft_atomic, AnalystAssessmentDraft, ArtifactDraftState, ArtifactScope,
    ContentHashDocument, DraftAppendOutcome, DraftProfile, FileStore, FinalizableArtifact,
    FinalizeDraftOutcome, PortfolioAssetDecisionDraft, ResearchDecisionDraftEntry, Result,
    RiskAssessmentDraft, RiskConstraintDraft, RunLocation, SafeSlug, StoreError,
    TradeIntentDraftEntry,
};

pub const DOMAIN_ARTIFACT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystAssessmentInput {
    pub ticker: String,
    pub direction: String,
    pub confidence: f64,
    pub report: String,
    pub priced_in: String,
    pub echo_chamber_risk: String,
    pub crowded_consensus_risk: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystEvidenceInput {
    pub ticker: String,
    pub evidence: EvidenceItem,
    pub evidence_ref: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchDecisionInput {
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
pub struct ResearchScenarioInput {
    pub ticker: String,
    pub bull: Scenario,
    pub base: Scenario,
    pub bear: Scenario,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeIntentInput {
    pub action: String,
    pub execution_decision: String,
    pub entry_price: Option<String>,
    pub stop_loss: Option<String>,
    pub position_size_pct_max: f64,
    pub rationale: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskAssessmentInput {
    pub argument: String,
    pub unique_risk_contribution: String,
    pub disagreement_with_prior: String,
    pub no_new_information: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskConstraintsInput {
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
pub struct PortfolioAssetDecisionInput {
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

/// Rust-owned values projected into a Phase 4 canonical artifact.  The model
/// is intentionally unable to choose a candidate action or ticker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeIntentFinalizePolicy {
    pub candidate_action: String,
}

/// Rust-owned runtime inputs for Phase 6. They come from the portfolio
/// snapshot and Phase 3 decision, never a model tool call.
#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioDecisionFinalizePolicy {
    pub ticker: String,
    pub rating: String,
    pub current_weight: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystArtifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub run_id: String,
    pub phase: u8,
    pub role: String,
    pub profile: String,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub id: String,
    pub per_ticker: BTreeMap<String, AnalystTickerArtifact>,
    pub evidence_refs: Vec<String>,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchArtifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub run_id: String,
    pub phase: u8,
    pub role: String,
    pub profile: String,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub id: String,
    pub rating: String,
    pub long_probability: f64,
    pub short_probability: f64,
    pub confidence_basis: String,
    pub hold_reason: Option<String>,
    pub plan: String,
    pub probability_rationale: String,
    pub scenarios: Option<Scenarios>,
    pub per_ticker: BTreeMap<String, Value>,
    pub decision_hinges: BTreeMap<String, Vec<String>>,
    pub evidence_refs: Vec<String>,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeIntentArtifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub run_id: String,
    pub phase: u8,
    pub role: String,
    pub ticker: String,
    pub profile: String,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub intent: TradeIntent,
    pub evidence_refs: Vec<String>,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskReviewArtifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub run_id: String,
    pub phase: u8,
    pub role: String,
    pub ticker: String,
    pub profile: String,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub constraints: RiskConstraints,
    pub evidence_refs: Vec<String>,
    pub created_at: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioDecisionArtifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub run_id: String,
    pub phase: u8,
    pub role: String,
    pub ticker: String,
    pub profile: String,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub decision: FinalValidation,
    pub evidence_refs: Vec<String>,
    pub created_at: String,
    pub content_hash: String,
}

impl ContentHashDocument for AnalystArtifact {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

impl ContentHashDocument for ResearchArtifact {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }
    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

macro_rules! content_hash_document {
    ($type:ty) => {
        impl ContentHashDocument for $type {
            fn content_hash(&self) -> &str {
                &self.content_hash
            }
            fn set_content_hash(&mut self, hash: String) {
                self.content_hash = hash;
            }
        }
    };
}
content_hash_document!(TradeIntentArtifact);
content_hash_document!(RiskReviewArtifact);
content_hash_document!(PortfolioDecisionArtifact);

impl FinalizableArtifact for AnalystArtifact {
    fn artifact_id(&self) -> &str {
        &self.artifact_id
    }
    fn source_payload_hash(&self) -> &str {
        &self.source_payload_hash
    }
}

impl FinalizableArtifact for ResearchArtifact {
    fn artifact_id(&self) -> &str {
        &self.artifact_id
    }
    fn source_payload_hash(&self) -> &str {
        &self.source_payload_hash
    }
}

macro_rules! finalizable_artifact {
    ($type:ty) => {
        impl FinalizableArtifact for $type {
            fn artifact_id(&self) -> &str {
                &self.artifact_id
            }
            fn source_payload_hash(&self) -> &str {
                &self.source_payload_hash
            }
        }
    };
}
finalizable_artifact!(TradeIntentArtifact);
finalizable_artifact!(RiskReviewArtifact);
finalizable_artifact!(PortfolioDecisionArtifact);

#[derive(Debug, Clone, PartialEq)]
pub enum DomainFinalizeOutcome {
    Analyst(Box<FinalizeDraftOutcome<AnalystArtifact>>),
    Research(Box<FinalizeDraftOutcome<ResearchArtifact>>),
    Trade(Box<FinalizeDraftOutcome<TradeIntentArtifact>>),
    Risk(Box<FinalizeDraftOutcome<RiskReviewArtifact>>),
    Portfolio(Box<FinalizeDraftOutcome<PortfolioDecisionArtifact>>),
}

pub fn set_analyst_assessment(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    input: AnalystAssessmentInput,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::AnalystReport)?;
    require_ticker(scope, &input.ticker)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "set_analyst_assessment",
        &input,
        input.ticker.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::AnalystReport(draft) = state else {
                return profile_state_error("analyst_report");
            };
            draft.assessments.insert(
                input.ticker.clone(),
                AnalystAssessmentDraft::empty(
                    input.direction.clone(),
                    input.confidence,
                    input.report.clone(),
                    input.priced_in.clone(),
                    input.echo_chamber_risk.clone(),
                    input.crowded_consensus_risk.clone(),
                ),
            );
            Ok(())
        },
    )
}

pub fn append_analyst_evidence(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    input: AnalystEvidenceInput,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::AnalystReport)?;
    require_ticker(scope, &input.ticker)?;
    require_non_empty("evidence_ref", &input.evidence_ref)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "append_analyst_evidence",
        &input,
        input.evidence_ref.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::AnalystReport(draft) = state else {
                return profile_state_error("analyst_report");
            };
            let assessment = draft.assessments.get_mut(&input.ticker).ok_or_else(|| {
                StoreError::InvalidDocument {
                    kind: "analyst draft",
                    message: "set_analyst_assessment must precede evidence".to_owned(),
                }
            })?;
            if !assessment.key_evidence.contains(&input.evidence) {
                assessment.key_evidence.push(input.evidence.clone());
            }
            draft
                .metadata
                .evidence_refs
                .insert(input.evidence_ref.clone());
            Ok(())
        },
    )
}

pub fn append_analyst_data_gap(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    ticker: String,
    data_gap: String,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::AnalystReport)?;
    require_ticker(scope, &ticker)?;
    require_non_empty("data_gap", &data_gap)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "append_analyst_data_gap",
        &(ticker.clone(), data_gap.clone()),
        ticker.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::AnalystReport(draft) = state else {
                return profile_state_error("analyst_report");
            };
            let assessment =
                draft
                    .assessments
                    .get_mut(&ticker)
                    .ok_or_else(|| StoreError::InvalidDocument {
                        kind: "analyst draft",
                        message: "set_analyst_assessment must precede data gaps".to_owned(),
                    })?;
            if !assessment.data_gaps.contains(&data_gap) {
                assessment.data_gaps.push(data_gap.clone());
            }
            Ok(())
        },
    )
}

pub fn set_analyst_invalidation(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    ticker: String,
    validation_triggers: Vec<String>,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::AnalystReport)?;
    require_ticker(scope, &ticker)?;
    if validation_triggers.is_empty()
        || validation_triggers
            .iter()
            .any(|value| value.trim().is_empty())
    {
        return Err(StoreError::InvalidDocument {
            kind: "analyst invalidation",
            message: "at least one non-empty validation trigger is required".to_owned(),
        });
    }
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "set_analyst_invalidation",
        &(ticker.clone(), validation_triggers.clone()),
        ticker.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::AnalystReport(draft) = state else {
                return profile_state_error("analyst_report");
            };
            let assessment =
                draft
                    .assessments
                    .get_mut(&ticker)
                    .ok_or_else(|| StoreError::InvalidDocument {
                        kind: "analyst draft",
                        message: "set_analyst_assessment must precede invalidation".to_owned(),
                    })?;
            assessment.validation_triggers = validation_triggers.clone();
            Ok(())
        },
    )
}

pub fn set_research_decision(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    input: ResearchDecisionInput,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::ResearchDecision)?;
    require_ticker(scope, &input.ticker)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "set_research_decision",
        &input,
        input.ticker.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::ResearchDecision(draft) = state else {
                return profile_state_error("research_decision");
            };
            draft.decisions.insert(
                input.ticker.clone(),
                ResearchDecisionDraftEntry::new(
                    input.rating.clone(),
                    input.long_probability,
                    input.short_probability,
                    input.confidence_basis.clone(),
                    input.hold_reason.clone(),
                    input.plan.clone(),
                    input.probability_rationale.clone(),
                ),
            );
            Ok(())
        },
    )
}

pub fn set_research_scenarios(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    input: ResearchScenarioInput,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::ResearchDecision)?;
    require_ticker(scope, &input.ticker)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "set_research_scenarios",
        &input,
        input.ticker.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::ResearchDecision(draft) = state else {
                return profile_state_error("research_decision");
            };
            let decision = draft.decisions.get_mut(&input.ticker).ok_or_else(|| {
                StoreError::InvalidDocument {
                    kind: "research draft",
                    message: "set_research_decision must precede scenarios".to_owned(),
                }
            })?;
            decision.set_scenarios(input.bull.clone(), input.base.clone(), input.bear.clone());
            Ok(())
        },
    )
}

pub fn append_research_hinge(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    ticker: String,
    hinge: String,
    evidence_ref: String,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::ResearchDecision)?;
    require_ticker(scope, &ticker)?;
    require_non_empty("hinge", &hinge)?;
    require_non_empty("evidence_ref", &evidence_ref)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "append_research_hinge",
        &(ticker.clone(), hinge.clone(), evidence_ref.clone()),
        evidence_ref.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::ResearchDecision(draft) = state else {
                return profile_state_error("research_decision");
            };
            let decision =
                draft
                    .decisions
                    .get_mut(&ticker)
                    .ok_or_else(|| StoreError::InvalidDocument {
                        kind: "research draft",
                        message: "set_research_decision must precede hinges".to_owned(),
                    })?;
            if !decision.decision_hinges.contains(&hinge) {
                decision.decision_hinges.push(hinge.clone());
            }
            draft.metadata.evidence_refs.insert(evidence_ref.clone());
            Ok(())
        },
    )
}

pub fn finalize_analyst_report(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    expected_tickers: &[String],
    created_at: &str,
) -> Result<DomainFinalizeOutcome> {
    require_profile(scope, DraftProfile::AnalystReport)?;
    let draft = read_draft(
        store,
        location,
        &crate::draft_relative(location, scope)?,
        DraftProfile::AnalystReport,
    )?;
    let ArtifactDraftState::AnalystReport(state) = draft.state else {
        return Err(profile_state_error("analyst_report").unwrap_err());
    };
    ensure_exact_tickers(&state.assessments, expected_tickers, "analyst report")?;
    let mut per_ticker = BTreeMap::new();
    for (ticker, draft) in state.assessments {
        let artifact = AnalystTickerArtifact {
            direction: draft.direction,
            confidence: draft.confidence,
            report: draft.report,
            key_evidence: draft.key_evidence,
            priced_in: draft.priced_in,
            echo_chamber_risk: draft.echo_chamber_risk,
            crowded_consensus_risk: draft.crowded_consensus_risk,
            validation_triggers: draft.validation_triggers,
            data_gaps: draft.data_gaps,
        };
        validate_analyst_ticker_artifact(&artifact).map_err(|message| {
            StoreError::InvalidDocument {
                kind: "analyst finalizer",
                message: format!("{ticker}: {message}"),
            }
        })?;
        per_ticker.insert(ticker, artifact);
    }
    let artifact = AnalystArtifact {
        schema_version: DOMAIN_ARTIFACT_SCHEMA_VERSION,
        artifact_id: artifact_id(scope, "analyst")?,
        run_id: scope.run_id.clone(),
        phase: scope.phase,
        role: scope.role.clone(),
        profile: scope.profile.as_str().to_owned(),
        unit_key: scope.unit_key.clone(),
        source_payload_hash: scope.source_payload_hash.clone(),
        id: scope.role.clone(),
        per_ticker,
        evidence_refs: state.metadata.evidence_refs.into_iter().collect(),
        created_at: created_at.to_owned(),
        content_hash: String::new(),
    };
    let relative = PathBuf::from("artifacts").join("phase1").join(format!(
        "{}.json",
        SafeSlug::new("role", &scope.role)?.as_str()
    ));
    Ok(DomainFinalizeOutcome::Analyst(Box::new(
        finalize_draft_atomic(store, location, scope, &relative, artifact, created_at)?,
    )))
}

pub fn finalize_research_decision(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    expected_tickers: &[String],
    created_at: &str,
) -> Result<DomainFinalizeOutcome> {
    require_profile(scope, DraftProfile::ResearchDecision)?;
    let draft = read_draft(
        store,
        location,
        &crate::draft_relative(location, scope)?,
        DraftProfile::ResearchDecision,
    )?;
    let ArtifactDraftState::ResearchDecision(state) = draft.state else {
        return Err(profile_state_error("research_decision").unwrap_err());
    };
    ensure_exact_tickers(&state.decisions, expected_tickers, "research decision")?;
    let mut per_ticker = BTreeMap::new();
    let mut hinges = BTreeMap::new();
    for (ticker, entry) in &state.decisions {
        let per = research_value(entry)?;
        let core: orchestrator_core::ResearchArtifact = serde_json::from_value(per.clone())
            .map_err(|source| StoreError::JsonSerialize { source })?;
        validate_research_artifact(&core, &[]).map_err(|error| StoreError::InvalidDocument {
            kind: "research finalizer",
            message: format!("{ticker}: {error}"),
        })?;
        per_ticker.insert(ticker.clone(), per);
        hinges.insert(ticker.clone(), entry.decision_hinges.clone());
    }
    let first = state
        .decisions
        .values()
        .next()
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "research finalizer",
            message: "at least one ticker is required".to_owned(),
        })?;
    let artifact = ResearchArtifact {
        schema_version: DOMAIN_ARTIFACT_SCHEMA_VERSION,
        artifact_id: artifact_id(scope, "research")?,
        run_id: scope.run_id.clone(),
        phase: scope.phase,
        role: scope.role.clone(),
        profile: scope.profile.as_str().to_owned(),
        unit_key: scope.unit_key.clone(),
        source_payload_hash: scope.source_payload_hash.clone(),
        id: scope.role.clone(),
        rating: first.rating.clone(),
        long_probability: first.long_probability,
        short_probability: first.short_probability,
        confidence_basis: first.confidence_basis.clone(),
        hold_reason: first.hold_reason.clone(),
        plan: first.plan.clone(),
        probability_rationale: first.probability_rationale.clone(),
        scenarios: first.scenarios.clone(),
        per_ticker,
        decision_hinges: hinges,
        evidence_refs: state.metadata.evidence_refs.into_iter().collect(),
        created_at: created_at.to_owned(),
        content_hash: String::new(),
    };
    let relative = PathBuf::from("artifacts")
        .join("phase3")
        .join("research-decision.json");
    Ok(DomainFinalizeOutcome::Research(Box::new(
        finalize_draft_atomic(store, location, scope, &relative, artifact, created_at)?,
    )))
}

pub fn set_trade_intent(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    input: TradeIntentInput,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::TradeIntent)?;
    scoped_ticker(scope)?;
    require_non_empty("action", &input.action)?;
    require_non_empty("execution_decision", &input.execution_decision)?;
    require_non_empty("rationale", &input.rationale)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "set_trade_intent",
        &input,
        "trade-intent".to_owned(),
        created_at,
        |state| {
            let ArtifactDraftState::TradeIntent(draft) = state else {
                return profile_state_error("trade_intent");
            };
            draft.intent = Some(TradeIntentDraftEntry {
                action: input.action.clone(),
                execution_decision: input.execution_decision.clone(),
                entry_price: input.entry_price.clone(),
                stop_loss: input.stop_loss.clone(),
                position_size_pct_max: input.position_size_pct_max,
                rationale: input.rationale.clone(),
            });
            Ok(())
        },
    )
}

pub fn append_trade_blocker(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    blocker: String,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::TradeIntent)?;
    scoped_ticker(scope)?;
    require_non_empty("blocker", &blocker)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "append_trade_blocker",
        &blocker,
        blocker.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::TradeIntent(draft) = state else {
                return profile_state_error("trade_intent");
            };
            if !draft.blockers.contains(&blocker) {
                draft.blockers.push(blocker.clone());
            }
            Ok(())
        },
    )
}

pub fn finalize_trade_intent(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    policy: &TradeIntentFinalizePolicy,
    created_at: &str,
) -> Result<DomainFinalizeOutcome> {
    require_profile(scope, DraftProfile::TradeIntent)?;
    let ticker = scoped_ticker(scope)?;
    let candidate_action = normalize_action("candidate_action", &policy.candidate_action)?;
    let draft = read_draft(
        store,
        location,
        &crate::draft_relative(location, scope)?,
        DraftProfile::TradeIntent,
    )?;
    let ArtifactDraftState::TradeIntent(state) = draft.state else {
        return Err(profile_state_error("trade_intent").unwrap_err());
    };
    let entry = state.intent.ok_or_else(|| StoreError::InvalidDocument {
        kind: "trade finalizer",
        message: "set_trade_intent is required before finalize".to_owned(),
    })?;
    let action = normalize_action("action", &entry.action)?;
    if entry.execution_decision == "execute_candidate" && action != candidate_action {
        return Err(StoreError::InvalidDocument {
            kind: "trade finalizer",
            message: "execute_candidate may not reverse the Rust-owned candidate action".to_owned(),
        });
    }
    if entry.execution_decision == "hold" && action != "Hold" {
        return Err(StoreError::InvalidDocument {
            kind: "trade finalizer",
            message: "hold execution_decision requires action=Hold".to_owned(),
        });
    }
    if candidate_action == "Hold" && (action != "Hold" || entry.execution_decision != "hold") {
        return Err(StoreError::InvalidDocument {
            kind: "trade finalizer",
            message: "a Rust-owned Hold candidate cannot be converted into a trade".to_owned(),
        });
    }
    let intent = TradeIntent {
        action,
        candidate_action,
        execution_decision: entry.execution_decision,
        entry_price: entry.entry_price,
        stop_loss: entry.stop_loss,
        position_size_pct_max: entry.position_size_pct_max,
        blockers: state.blockers,
        rationale: entry.rationale,
    };
    validate_trade_intent(&intent).map_err(|message| StoreError::InvalidDocument {
        kind: "trade finalizer",
        message,
    })?;
    let artifact = TradeIntentArtifact {
        schema_version: DOMAIN_ARTIFACT_SCHEMA_VERSION,
        artifact_id: artifact_id(scope, "trade-intent")?,
        run_id: scope.run_id.clone(),
        phase: scope.phase,
        role: scope.role.clone(),
        ticker: ticker.clone(),
        profile: scope.profile.as_str().to_owned(),
        unit_key: scope.unit_key.clone(),
        source_payload_hash: scope.source_payload_hash.clone(),
        intent,
        evidence_refs: state.metadata.evidence_refs.into_iter().collect(),
        created_at: created_at.to_owned(),
        content_hash: String::new(),
    };
    let relative = PathBuf::from("artifacts").join("phase4").join(format!(
        "{}.json",
        SafeSlug::new("ticker", &ticker)?.as_str()
    ));
    Ok(DomainFinalizeOutcome::Trade(Box::new(
        finalize_draft_atomic(store, location, scope, &relative, artifact, created_at)?,
    )))
}

pub fn set_risk_assessment(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    input: RiskAssessmentInput,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::RiskReview)?;
    scoped_ticker(scope)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "set_risk_assessment",
        &input,
        "risk-assessment".to_owned(),
        created_at,
        |state| {
            let ArtifactDraftState::RiskReview(draft) = state else {
                return profile_state_error("risk_review");
            };
            draft.assessment = Some(RiskAssessmentDraft {
                argument: input.argument.clone(),
                unique_risk_contribution: input.unique_risk_contribution.clone(),
                disagreement_with_prior: input.disagreement_with_prior.clone(),
                no_new_information: input.no_new_information,
            });
            Ok(())
        },
    )
}

pub fn set_risk_constraints(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    input: RiskConstraintsInput,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::RiskReview)?;
    scoped_ticker(scope)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "set_risk_constraints",
        &input,
        "risk-constraints".to_owned(),
        created_at,
        |state| {
            let ArtifactDraftState::RiskReview(draft) = state else {
                return profile_state_error("risk_review");
            };
            draft.constraints = Some(RiskConstraintDraft {
                recommended_adjustment: input.recommended_adjustment.clone(),
                stop_type: input.stop_type,
                max_drawdown_pct: input.max_drawdown_pct,
                position_cap_pct: input.position_cap_pct,
                rebalance_trigger: input.rebalance_trigger.clone(),
                risk_off_trigger: input.risk_off_trigger.clone(),
                review_window: input.review_window.clone(),
                cash_hedge_recommendation: input.cash_hedge_recommendation.clone(),
                constraint_confidence: input.constraint_confidence,
            });
            Ok(())
        },
    )
}

pub fn finalize_risk_review(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    created_at: &str,
) -> Result<DomainFinalizeOutcome> {
    require_profile(scope, DraftProfile::RiskReview)?;
    let ticker = scoped_ticker(scope)?;
    let stance = scope
        .stance
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "risk finalizer",
            message: "Rust-owned stance is required in ArtifactScope".to_owned(),
        })?;
    let draft = read_draft(
        store,
        location,
        &crate::draft_relative(location, scope)?,
        DraftProfile::RiskReview,
    )?;
    let ArtifactDraftState::RiskReview(state) = draft.state else {
        return Err(profile_state_error("risk_review").unwrap_err());
    };
    let assessment = state
        .assessment
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "risk finalizer",
            message: "set_risk_assessment is required before finalize".to_owned(),
        })?;
    let constraints = state
        .constraints
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "risk finalizer",
            message: "set_risk_constraints is required before finalize".to_owned(),
        })?;
    let constraints = RiskConstraints {
        stance: stance.to_owned(),
        argument: assessment.argument,
        unique_risk_contribution: assessment.unique_risk_contribution,
        disagreement_with_prior: assessment.disagreement_with_prior,
        no_new_information: assessment.no_new_information,
        recommended_adjustment: constraints.recommended_adjustment,
        stop_type: constraints.stop_type,
        max_drawdown_pct: constraints.max_drawdown_pct,
        position_cap_pct: constraints.position_cap_pct,
        rebalance_trigger: constraints.rebalance_trigger,
        risk_off_trigger: constraints.risk_off_trigger,
        review_window: constraints.review_window,
        cash_hedge_recommendation: constraints.cash_hedge_recommendation,
        constraint_confidence: constraints.constraint_confidence,
    };
    validate_risk_constraints(&constraints).map_err(|message| StoreError::InvalidDocument {
        kind: "risk finalizer",
        message,
    })?;
    let artifact = RiskReviewArtifact {
        schema_version: DOMAIN_ARTIFACT_SCHEMA_VERSION,
        artifact_id: artifact_id(scope, "risk-review")?,
        run_id: scope.run_id.clone(),
        phase: scope.phase,
        role: scope.role.clone(),
        ticker: ticker.clone(),
        profile: scope.profile.as_str().to_owned(),
        unit_key: scope.unit_key.clone(),
        source_payload_hash: scope.source_payload_hash.clone(),
        constraints,
        evidence_refs: state.metadata.evidence_refs.into_iter().collect(),
        created_at: created_at.to_owned(),
        content_hash: String::new(),
    };
    let relative = PathBuf::from("artifacts")
        .join("phase5")
        .join(SafeSlug::new("role", &scope.role)?.as_str())
        .join(format!(
            "{}.json",
            SafeSlug::new("ticker", &ticker)?.as_str()
        ));
    Ok(DomainFinalizeOutcome::Risk(Box::new(
        finalize_draft_atomic(store, location, scope, &relative, artifact, created_at)?,
    )))
}

pub fn set_portfolio_asset_decision(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    input: PortfolioAssetDecisionInput,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::PortfolioDecision)?;
    scoped_ticker(scope)?;
    require_non_empty("execution_summary", &input.execution_summary)?;
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "set_portfolio_asset_decision",
        &input,
        "portfolio-asset-decision".to_owned(),
        created_at,
        |state| {
            let ArtifactDraftState::PortfolioDecision(draft) = state else {
                return profile_state_error("portfolio_decision");
            };
            draft.decision = Some(PortfolioAssetDecisionDraft {
                direction_constraint: input.direction_constraint.clone(),
                execution_status: input.execution_status.clone(),
                max_target_weight: input.max_target_weight,
                max_weight_delta: input.max_weight_delta,
                execution_summary: input.execution_summary.clone(),
                investment_thesis: input.investment_thesis.clone(),
                target_price: input.target_price.clone(),
                horizon: input.horizon.clone(),
                rationale: input.rationale.clone(),
            });
            Ok(())
        },
    )
}

pub fn append_binding_risk_control(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    control: BindingRiskControl,
    created_at: &str,
) -> Result<DraftAppendOutcome> {
    require_profile(scope, DraftProfile::PortfolioDecision)?;
    scoped_ticker(scope)?;
    if control.control.trim().is_empty()
        || control.source_refs.is_empty()
        || control
            .source_refs
            .iter()
            .any(|reference| reference.trim().is_empty())
    {
        return Err(StoreError::InvalidDocument {
            kind: "binding risk control",
            message: "control and at least one non-empty source_ref are required".to_owned(),
        });
    }
    create_or_recover_draft(store, location, scope.clone(), created_at)?;
    mutate_draft(
        store,
        location,
        scope,
        "append_binding_risk_control",
        &control,
        control.control.clone(),
        created_at,
        |state| {
            let ArtifactDraftState::PortfolioDecision(draft) = state else {
                return profile_state_error("portfolio_decision");
            };
            if !draft.binding_risk_controls.contains(&control) {
                draft.binding_risk_controls.push(control.clone());
            }
            for reference in &control.source_refs {
                draft.metadata.evidence_refs.insert(reference.clone());
            }
            Ok(())
        },
    )
}

pub fn finalize_portfolio_decision(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    policy: &PortfolioDecisionFinalizePolicy,
    created_at: &str,
) -> Result<DomainFinalizeOutcome> {
    require_profile(scope, DraftProfile::PortfolioDecision)?;
    let ticker = scoped_ticker(scope)?;
    if policy.ticker != ticker {
        return Err(StoreError::InvalidDocument {
            kind: "portfolio finalizer",
            message: "runtime policy ticker differs from Rust-owned ArtifactScope".to_owned(),
        });
    }
    let draft = read_draft(
        store,
        location,
        &crate::draft_relative(location, scope)?,
        DraftProfile::PortfolioDecision,
    )?;
    let ArtifactDraftState::PortfolioDecision(state) = draft.state else {
        return Err(profile_state_error("portfolio_decision").unwrap_err());
    };
    let decision = state.decision.ok_or_else(|| StoreError::InvalidDocument {
        kind: "portfolio finalizer",
        message: "set_portfolio_asset_decision is required before finalize".to_owned(),
    })?;
    if state.binding_risk_controls.is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "portfolio finalizer",
            message: "at least one traceable binding risk control is required".to_owned(),
        });
    }
    validate_portfolio_semantics(&decision, policy.current_weight)?;
    let mut per_asset = BTreeMap::new();
    per_asset.insert(
        ticker.clone(),
        AssetExecutionConstraint {
            direction_constraint: decision.direction_constraint,
            execution_status: decision.execution_status.clone(),
            current_weight: policy.current_weight,
            max_target_weight: decision.max_target_weight,
            max_weight_delta: decision.max_weight_delta,
            binding_risk_controls: state.binding_risk_controls,
        },
    );
    let validation = FinalValidation {
        rating: policy.rating.clone(),
        execution_status: decision.execution_status,
        execution_summary: decision.execution_summary,
        investment_thesis: decision.investment_thesis,
        target_price: decision.target_price,
        horizon: decision.horizon,
        risk_controls: Vec::new(),
        rationale: decision.rationale,
        per_asset,
        extra: Default::default(),
    };
    validate_final_validation(&validation).map_err(|message| StoreError::InvalidDocument {
        kind: "portfolio finalizer",
        message,
    })?;
    let artifact = PortfolioDecisionArtifact {
        schema_version: DOMAIN_ARTIFACT_SCHEMA_VERSION,
        artifact_id: artifact_id(scope, "portfolio-decision")?,
        run_id: scope.run_id.clone(),
        phase: scope.phase,
        role: scope.role.clone(),
        ticker: ticker.clone(),
        profile: scope.profile.as_str().to_owned(),
        unit_key: scope.unit_key.clone(),
        source_payload_hash: scope.source_payload_hash.clone(),
        decision: validation,
        evidence_refs: state.metadata.evidence_refs.into_iter().collect(),
        created_at: created_at.to_owned(),
        content_hash: String::new(),
    };
    let relative = PathBuf::from("artifacts").join("phase6").join(format!(
        "{}.json",
        SafeSlug::new("ticker", &ticker)?.as_str()
    ));
    Ok(DomainFinalizeOutcome::Portfolio(Box::new(
        finalize_draft_atomic(store, location, scope, &relative, artifact, created_at)?,
    )))
}

fn research_value(entry: &ResearchDecisionDraftEntry) -> Result<Value> {
    serde_json::to_value(json!({
        "rating": entry.rating, "long_probability": entry.long_probability, "short_probability": entry.short_probability,
        "confidence_basis": entry.confidence_basis, "hold_reason": entry.hold_reason, "plan": entry.plan,
        "probability_rationale": entry.probability_rationale, "scenarios": entry.scenarios, "per_ticker": {}
    })).map_err(|source| StoreError::JsonSerialize { source })
}

fn artifact_id(scope: &ArtifactScope, kind: &str) -> Result<String> {
    Ok(format!(
        "artifact-{}",
        &content_hash(&serde_json::json!({"scope": scope, "kind": kind}))?[..32]
    ))
}
fn require_profile(scope: &ArtifactScope, profile: DraftProfile) -> Result<()> {
    if scope.profile == profile {
        Ok(())
    } else {
        Err(StoreError::InvalidDocument {
            kind: "domain draft",
            message: format!("expected {} profile", profile.as_str()),
        })
    }
}
fn require_ticker(scope: &ArtifactScope, ticker: &str) -> Result<()> {
    require_non_empty("ticker", ticker)?;
    if let Some(scoped) = &scope.ticker {
        if scoped != ticker {
            return Err(StoreError::InvalidDocument {
                kind: "domain draft",
                message: "ticker is outside the Rust-owned scope".to_owned(),
            });
        }
    }
    Ok(())
}
fn scoped_ticker(scope: &ArtifactScope) -> Result<String> {
    scope
        .ticker
        .as_deref()
        .filter(|ticker| !ticker.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| StoreError::InvalidDocument {
            kind: "domain draft",
            message: "a single Rust-owned ticker scope is required".to_owned(),
        })
}
fn normalize_action(field: &'static str, action: &str) -> Result<String> {
    if matches!(action, "Buy" | "Sell" | "Hold") {
        Ok(action.to_owned())
    } else {
        Err(StoreError::InvalidDocument {
            kind: "trade finalizer",
            message: format!("{field} must be Buy, Sell, or Hold"),
        })
    }
}
fn validate_portfolio_semantics(
    decision: &PortfolioAssetDecisionDraft,
    current_weight: f64,
) -> Result<()> {
    if !current_weight.is_finite() || !(0.0..=1.0).contains(&current_weight) {
        return Err(StoreError::InvalidDocument {
            kind: "portfolio finalizer",
            message: "Rust-owned current_weight must be finite and in [0.0, 1.0]".to_owned(),
        });
    }
    if decision.execution_status == "wait"
        && (decision.direction_constraint != "unchanged"
            || (decision.max_target_weight - current_weight).abs() > f64::EPSILON
            || decision.max_weight_delta > f64::EPSILON)
    {
        return Err(StoreError::InvalidDocument {
            kind: "portfolio finalizer",
            message: "wait requires unchanged direction, current target, and zero delta".to_owned(),
        });
    }
    if decision.execution_status == "downgrade"
        && (decision.direction_constraint == "increase_only"
            || decision.max_target_weight > current_weight + f64::EPSILON)
    {
        return Err(StoreError::InvalidDocument {
            kind: "portfolio finalizer",
            message: "downgrade cannot increase the target weight".to_owned(),
        });
    }
    if decision.direction_constraint == "unchanged"
        && decision.max_target_weight > current_weight + f64::EPSILON
    {
        return Err(StoreError::InvalidDocument {
            kind: "portfolio finalizer",
            message: "unchanged direction cannot increase the target weight".to_owned(),
        });
    }
    Ok(())
}
fn require_non_empty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(StoreError::InvalidDocument {
            kind: "domain tool input",
            message: format!("{field} must not be empty"),
        })
    } else {
        Ok(())
    }
}
fn profile_state_error(profile: &'static str) -> Result<()> {
    Err(StoreError::InvalidDocument {
        kind: "domain draft",
        message: format!("draft state is not {profile}"),
    })
}
fn ensure_exact_tickers<T>(
    values: &BTreeMap<String, T>,
    expected: &[String],
    kind: &'static str,
) -> Result<()> {
    let expected = expected.iter().collect::<std::collections::BTreeSet<_>>();
    let actual = values.keys().collect::<std::collections::BTreeSet<_>>();
    if actual != expected {
        Err(StoreError::InvalidDocument {
            kind,
            message: format!("ticker coverage mismatch: expected {expected:?}, found {actual:?}"),
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileStoreOptions, RunLocation};
    use tempfile::tempdir;

    fn scope(profile: DraftProfile, phase: u8, role: &str) -> ArtifactScope {
        ArtifactScope {
            run_id: "r1".into(),
            current_date: "2026-07-27".into(),
            phase,
            role: role.into(),
            profile,
            profile_version: 1,
            builder_version: 1,
            unit_key: "unit".into(),
            source_payload_hash: "source".into(),
            ticker: None,
            topic_id: None,
            side: None,
            stance: None,
            round: None,
            reflection_task: None,
        }
    }
    fn evidence() -> EvidenceItem {
        EvidenceItem {
            claim: "QQQ held support".into(),
            evidence_type: orchestrator_core::EvidenceType::Fact,
            source: "technical snapshot".into(),
            timestamp: "2026-07-27".into(),
            source_tier: "official".into(),
            first_source: "source".into(),
            is_derivative_repost: false,
            evidence_age: "0-2d".into(),
            source_confidence: 0.9,
        }
    }

    fn single_ticker_scope(
        profile: DraftProfile,
        phase: u8,
        role: &str,
        stance: Option<&str>,
    ) -> ArtifactScope {
        let mut scope = scope(profile, phase, role);
        scope.ticker = Some("QQQ".into());
        scope.stance = stance.map(str::to_owned);
        scope
    }

    #[test]
    fn analyst_finalizer_writes_canonical_artifact_and_recovers() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "r1").unwrap();
        let scope = scope(DraftProfile::AnalystReport, 1, "analyst.technical");
        let now = "2026-07-27T00:00:00Z";
        set_analyst_assessment(
            &store,
            &location,
            &scope,
            AnalystAssessmentInput {
                ticker: "QQQ".into(),
                direction: "bullish".into(),
                confidence: 0.7,
                report: "report".into(),
                priced_in: "unclear".into(),
                echo_chamber_risk: "low".into(),
                crowded_consensus_risk: "low".into(),
            },
            now,
        )
        .unwrap();
        append_analyst_evidence(
            &store,
            &location,
            &scope,
            AnalystEvidenceInput {
                ticker: "QQQ".into(),
                evidence: evidence(),
                evidence_ref: "e1".into(),
            },
            now,
        )
        .unwrap();
        set_analyst_invalidation(
            &store,
            &location,
            &scope,
            "QQQ".into(),
            vec!["lose support".into()],
            now,
        )
        .unwrap();
        assert!(matches!(
            finalize_analyst_report(&store, &location, &scope, &["QQQ".into()], now).unwrap(),
            DomainFinalizeOutcome::Analyst(outcome) if matches!(*outcome, FinalizeDraftOutcome::Completed { .. })
        ));
        assert!(matches!(
            finalize_analyst_report(&store, &location, &scope, &["QQQ".into()], now).unwrap(),
            DomainFinalizeOutcome::Analyst(outcome) if matches!(*outcome, FinalizeDraftOutcome::Recovered { .. })
        ));
    }

    #[test]
    fn trade_finalizer_derives_candidate_action_and_rejects_reversal() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "r1").unwrap();
        let scope = single_ticker_scope(DraftProfile::TradeIntent, 4, "trader", None);
        let now = "2026-07-27T00:00:00Z";
        set_trade_intent(
            &store,
            &location,
            &scope,
            TradeIntentInput {
                action: "Buy".into(),
                execution_decision: "execute_candidate".into(),
                entry_price: Some("500".into()),
                stop_loss: Some("490".into()),
                position_size_pct_max: 0.2,
                rationale: "trend confirmation".into(),
            },
            now,
        )
        .unwrap();
        append_trade_blocker(&store, &location, &scope, "wait for open".into(), now).unwrap();
        let outcome = finalize_trade_intent(
            &store,
            &location,
            &scope,
            &TradeIntentFinalizePolicy {
                candidate_action: "Buy".into(),
            },
            now,
        )
        .unwrap();
        assert!(matches!(outcome, DomainFinalizeOutcome::Trade(_)));

        let reverse_scope = single_ticker_scope(DraftProfile::TradeIntent, 4, "trader", None);
        let mut reverse_scope = reverse_scope;
        reverse_scope.unit_key = "reverse".into();
        set_trade_intent(
            &store,
            &location,
            &reverse_scope,
            TradeIntentInput {
                action: "Sell".into(),
                execution_decision: "execute_candidate".into(),
                entry_price: None,
                stop_loss: None,
                position_size_pct_max: 0.2,
                rationale: "incorrect reversal".into(),
            },
            now,
        )
        .unwrap();
        assert!(finalize_trade_intent(
            &store,
            &location,
            &reverse_scope,
            &TradeIntentFinalizePolicy {
                candidate_action: "Buy".into(),
            },
            now,
        )
        .is_err());
    }

    #[test]
    fn risk_finalizer_owns_stance_and_enforces_stop_type() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "r1").unwrap();
        let scope =
            single_ticker_scope(DraftProfile::RiskReview, 5, "risk.neutral", Some("neutral"));
        let now = "2026-07-27T00:00:00Z";
        set_risk_assessment(
            &store,
            &location,
            &scope,
            RiskAssessmentInput {
                argument: "volatility is elevated".into(),
                unique_risk_contribution: "gap risk".into(),
                disagreement_with_prior: "none".into(),
                no_new_information: false,
            },
            now,
        )
        .unwrap();
        set_risk_constraints(
            &store,
            &location,
            &scope,
            RiskConstraintsInput {
                recommended_adjustment: "cap exposure".into(),
                stop_type: StopType::Hard,
                max_drawdown_pct: 0.05,
                position_cap_pct: 0.2,
                rebalance_trigger: "weekly".into(),
                risk_off_trigger: "support fails".into(),
                review_window: "one day".into(),
                cash_hedge_recommendation: "hold cash".into(),
                constraint_confidence: 0.7,
            },
            now,
        )
        .unwrap();
        let outcome = finalize_risk_review(&store, &location, &scope, now).unwrap();
        let DomainFinalizeOutcome::Risk(outcome) = outcome else {
            panic!("expected risk outcome");
        };
        let FinalizeDraftOutcome::Completed { artifact, .. } = *outcome else {
            panic!("expected new artifact");
        };
        assert_eq!(artifact.constraints.stance, "neutral");
        assert_eq!(artifact.constraints.stop_type, StopType::Hard);
    }

    #[test]
    fn portfolio_finalizer_requires_traceable_control_and_wait_semantics() {
        let temp = tempdir().unwrap();
        let store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let location = RunLocation::new("2026-07-27", "r1").unwrap();
        let scope = single_ticker_scope(
            DraftProfile::PortfolioDecision,
            6,
            "portfolio.manager",
            None,
        );
        let now = "2026-07-27T00:00:00Z";
        set_portfolio_asset_decision(
            &store,
            &location,
            &scope,
            PortfolioAssetDecisionInput {
                direction_constraint: "unchanged".into(),
                execution_status: "wait".into(),
                max_target_weight: 0.25,
                max_weight_delta: 0.0,
                execution_summary: "wait for confirmation".into(),
                investment_thesis: "not yet invalidated".into(),
                target_price: None,
                horizon: "one week".into(),
                rationale: "risk gate remains active".into(),
            },
            now,
        )
        .unwrap();
        assert!(finalize_portfolio_decision(
            &store,
            &location,
            &scope,
            &PortfolioDecisionFinalizePolicy {
                ticker: "QQQ".into(),
                rating: "Hold".into(),
                current_weight: 0.25,
            },
            now,
        )
        .is_err());
        append_binding_risk_control(
            &store,
            &location,
            &scope,
            BindingRiskControl {
                control: "Do not add before support holds".into(),
                source_refs: vec!["phase5:neutral:QQQ".into()],
            },
            now,
        )
        .unwrap();
        assert!(matches!(
            finalize_portfolio_decision(
                &store,
                &location,
                &scope,
                &PortfolioDecisionFinalizePolicy {
                    ticker: "QQQ".into(),
                    rating: "Hold".into(),
                    current_weight: 0.25,
                },
                now,
            )
            .unwrap(),
            DomainFinalizeOutcome::Portfolio(_)
        ));
    }
}
