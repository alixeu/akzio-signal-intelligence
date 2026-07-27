use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use orchestrator_core::artifact::{BindingRiskControl, Scenario, Scenarios, StopType};
use orchestrator_core::EvidenceItem;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    canonical_json_bytes, content_hash, content_hash_bytes, error::io_error,
    validate_content_hash_at, validate_relative_path, ContentHashDocument, FileSchemaKind,
    FileStore, FinalizedArtifactRef, Result, RunLocation, SafeSlug, StoreError, Versioned,
};

pub const DRAFT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DraftProfile {
    HistoricalReflection,
    AnalystReport,
    TopicGeneration,
    ResearcherWarmup,
    DebateSeed,
    DebateResponse,
    TopicControl,
    ResearchDecision,
    TradeIntent,
    RiskReview,
    PortfolioDecision,
    PhaseSummary,
}

impl DraftProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HistoricalReflection => "historical_reflection",
            Self::AnalystReport => "analyst_report",
            Self::TopicGeneration => "topic_generation",
            Self::ResearcherWarmup => "researcher_warmup",
            Self::DebateSeed => "debate_seed",
            Self::DebateResponse => "debate_response",
            Self::TopicControl => "topic_control",
            Self::ResearchDecision => "research_decision",
            Self::TradeIntent => "trade_intent",
            Self::RiskReview => "risk_review",
            Self::PortfolioDecision => "portfolio_decision",
            Self::PhaseSummary => "phase_summary",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactScope {
    pub run_id: String,
    pub current_date: String,
    pub phase: u8,
    pub role: String,
    pub profile: DraftProfile,
    pub profile_version: u32,
    pub builder_version: u32,
    pub unit_key: String,
    pub source_payload_hash: String,
    pub ticker: Option<String>,
    pub topic_id: Option<String>,
    pub side: Option<String>,
    pub stance: Option<String>,
    pub round: Option<u32>,
    pub reflection_task: Option<String>,
}

impl ArtifactScope {
    pub fn validate_for_location(&self, location: &RunLocation) -> Result<()> {
        if self.run_id != location.run_id || self.current_date != location.current_date {
            return Err(StoreError::InvalidDocument {
                kind: "artifact scope",
                message: "run identity differs from draft store location".to_owned(),
            });
        }
        if self.role.is_empty()
            || self.unit_key.is_empty()
            || self.source_payload_hash.is_empty()
            || self.profile_version == 0
            || self.builder_version == 0
        {
            return Err(StoreError::InvalidDocument {
                kind: "artifact scope",
                message: "required scope fields must be present and versions must be non-zero"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DraftIdempotencyKey(String);

impl DraftIdempotencyKey {
    pub fn from_scope(scope: &ArtifactScope) -> Result<Self> {
        let value =
            serde_json::to_value(scope).map_err(|source| StoreError::JsonSerialize { source })?;
        Ok(Self(content_hash(&value)?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DraftIdempotencyKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DraftLifecycle {
    Draft,
    Completed,
    Failed,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProfileDraftMetadata {
    pub evidence_refs: BTreeSet<String>,
}

macro_rules! typed_profile_draft {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
        pub struct $name {
            pub metadata: ProfileDraftMetadata,
        }
    };
}

typed_profile_draft!(HistoricalReflectionDraft);
/// Explicit mutable state for Phase 1.  The model can only change this through
/// the analyst domain tools; there is deliberately no `serde_json::Value`
/// scratch pad or JSON-path mutation API.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct AnalystReportDraft {
    pub metadata: ProfileDraftMetadata,
    pub assessments: BTreeMap<String, AnalystAssessmentDraft>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystAssessmentDraft {
    pub direction: String,
    pub confidence: f64,
    pub report: String,
    pub priced_in: String,
    pub echo_chamber_risk: String,
    pub crowded_consensus_risk: String,
    pub key_evidence: Vec<EvidenceItem>,
    pub data_gaps: Vec<String>,
    pub validation_triggers: Vec<String>,
}

impl AnalystAssessmentDraft {
    pub fn empty(
        direction: String,
        confidence: f64,
        report: String,
        priced_in: String,
        echo_chamber_risk: String,
        crowded_consensus_risk: String,
    ) -> Self {
        Self {
            direction,
            confidence,
            report,
            priced_in,
            echo_chamber_risk,
            crowded_consensus_risk,
            key_evidence: Vec::new(),
            data_gaps: Vec::new(),
            validation_triggers: Vec::new(),
        }
    }
}

/// Rust-owned topic identity and fields captured by `create_phase2_topic`.
/// The model may describe a topic but never supplies this identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Phase2TopicDraft {
    pub topic_id: String,
    pub topic: String,
    pub decision_hinge: String,
    pub evidence_refs: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TopicGenerationDraft {
    pub metadata: ProfileDraftMetadata,
    pub common_ground: Option<String>,
    pub topics: BTreeMap<String, Phase2TopicDraft>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResearcherWarmupDraft {
    pub metadata: ProfileDraftMetadata,
    /// A warm-up only records that its terminal preconditions were met. It
    /// deliberately has no claim or business artifact payload.
    pub finalized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebateClaimDraft {
    pub claim_id: String,
    pub claim: String,
    pub confidence_bps: u16,
    pub evidence_refs: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DebateSeedDraft {
    pub metadata: ProfileDraftMetadata,
    pub claims: BTreeMap<String, DebateClaimDraft>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebateResponseDraftEntry {
    pub response_id: String,
    pub reply_to_claim_id: String,
    pub response: String,
    pub evidence_refs: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DebateResponseDraft {
    pub metadata: ProfileDraftMetadata,
    pub responses: BTreeMap<String, DebateResponseDraftEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TopicControlDraft {
    pub metadata: ProfileDraftMetadata,
    pub claim_statuses: BTreeMap<String, String>,
    pub agreed_facts: Vec<String>,
    pub decision_hinges: Vec<String>,
    pub routes: BTreeMap<String, String>,
    pub should_continue: Option<bool>,
}
/// Explicit mutable state for Phase 3.  A decision is stored per planned
/// ticker so the same finalizer can enforce full ticker coverage.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ResearchDecisionDraft {
    pub metadata: ProfileDraftMetadata,
    pub decisions: BTreeMap<String, ResearchDecisionDraftEntry>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchDecisionDraftEntry {
    pub rating: String,
    pub long_probability: f64,
    pub short_probability: f64,
    pub confidence_basis: String,
    pub hold_reason: Option<String>,
    pub plan: String,
    pub probability_rationale: String,
    pub scenarios: Option<Scenarios>,
    pub decision_hinges: Vec<String>,
}

impl ResearchDecisionDraftEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rating: String,
        long_probability: f64,
        short_probability: f64,
        confidence_basis: String,
        hold_reason: Option<String>,
        plan: String,
        probability_rationale: String,
    ) -> Self {
        Self {
            rating,
            long_probability,
            short_probability,
            confidence_basis,
            hold_reason,
            plan,
            probability_rationale,
            scenarios: None,
            decision_hinges: Vec::new(),
        }
    }

    pub fn set_scenarios(&mut self, bull: Scenario, base: Scenario, bear: Scenario) {
        self.scenarios = Some(Scenarios { bull, base, bear });
    }
}
/// Mutable fields accepted from the Phase 4 Trader. `candidate_action` is
/// deliberately absent: it is derived by Rust from the finalized Phase 3
/// decision when the canonical artifact is built.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct TradeIntentDraft {
    pub metadata: ProfileDraftMetadata,
    pub intent: Option<TradeIntentDraftEntry>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeIntentDraftEntry {
    pub action: String,
    pub execution_decision: String,
    pub entry_price: Option<String>,
    pub stop_loss: Option<String>,
    pub position_size_pct_max: f64,
    pub rationale: String,
}

/// Phase 5 assessment fields, independent from the binding constraint values.
/// The role's stance is Rust-owned by `ArtifactScope` and cannot appear here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskAssessmentDraft {
    pub argument: String,
    pub unique_risk_contribution: String,
    pub disagreement_with_prior: String,
    pub no_new_information: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskConstraintDraft {
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

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RiskReviewDraft {
    pub metadata: ProfileDraftMetadata,
    pub assessment: Option<RiskAssessmentDraft>,
    pub constraints: Option<RiskConstraintDraft>,
}

/// One per-asset Phase 6 decision. Current weight and rating are excluded
/// because they are authoritative runtime inputs, not model selections.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioAssetDecisionDraft {
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

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct PortfolioDecisionDraft {
    pub metadata: ProfileDraftMetadata,
    pub decision: Option<PortfolioAssetDecisionDraft>,
    pub binding_risk_controls: Vec<BindingRiskControl>,
}
typed_profile_draft!(PhaseSummaryDraft);

/// Typed profile state prevents a tool runtime from performing arbitrary JSON
/// path mutation. Each profile grows explicit fields as its domain tools are
/// migrated, while lifecycle ownership remains shared here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactDraftState {
    HistoricalReflection(HistoricalReflectionDraft),
    AnalystReport(AnalystReportDraft),
    TopicGeneration(TopicGenerationDraft),
    ResearcherWarmup(ResearcherWarmupDraft),
    DebateSeed(DebateSeedDraft),
    DebateResponse(DebateResponseDraft),
    TopicControl(TopicControlDraft),
    ResearchDecision(ResearchDecisionDraft),
    TradeIntent(TradeIntentDraft),
    RiskReview(RiskReviewDraft),
    PortfolioDecision(PortfolioDecisionDraft),
    PhaseSummary(PhaseSummaryDraft),
}

impl ArtifactDraftState {
    pub fn for_profile(profile: DraftProfile) -> Self {
        match profile {
            DraftProfile::HistoricalReflection => Self::HistoricalReflection(Default::default()),
            DraftProfile::AnalystReport => Self::AnalystReport(Default::default()),
            DraftProfile::TopicGeneration => Self::TopicGeneration(Default::default()),
            DraftProfile::ResearcherWarmup => Self::ResearcherWarmup(Default::default()),
            DraftProfile::DebateSeed => Self::DebateSeed(Default::default()),
            DraftProfile::DebateResponse => Self::DebateResponse(Default::default()),
            DraftProfile::TopicControl => Self::TopicControl(Default::default()),
            DraftProfile::ResearchDecision => Self::ResearchDecision(Default::default()),
            DraftProfile::TradeIntent => Self::TradeIntent(Default::default()),
            DraftProfile::RiskReview => Self::RiskReview(Default::default()),
            DraftProfile::PortfolioDecision => Self::PortfolioDecision(Default::default()),
            DraftProfile::PhaseSummary => Self::PhaseSummary(Default::default()),
        }
    }

    pub fn profile(&self) -> DraftProfile {
        match self {
            Self::HistoricalReflection(_) => DraftProfile::HistoricalReflection,
            Self::AnalystReport(_) => DraftProfile::AnalystReport,
            Self::TopicGeneration(_) => DraftProfile::TopicGeneration,
            Self::ResearcherWarmup(_) => DraftProfile::ResearcherWarmup,
            Self::DebateSeed(_) => DraftProfile::DebateSeed,
            Self::DebateResponse(_) => DraftProfile::DebateResponse,
            Self::TopicControl(_) => DraftProfile::TopicControl,
            Self::ResearchDecision(_) => DraftProfile::ResearchDecision,
            Self::TradeIntent(_) => DraftProfile::TradeIntent,
            Self::RiskReview(_) => DraftProfile::RiskReview,
            Self::PortfolioDecision(_) => DraftProfile::PortfolioDecision,
            Self::PhaseSummary(_) => DraftProfile::PhaseSummary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DraftWriteReceipt {
    pub normalized_parameters_hash: String,
    pub tool_name: String,
    pub result_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DraftFailure {
    pub code: String,
    pub message: String,
    pub failed_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactDraft {
    pub schema_version: u32,
    pub draft_id: String,
    pub scope: ArtifactScope,
    pub lifecycle: DraftLifecycle,
    pub state: ArtifactDraftState,
    pub revision: u64,
    pub write_receipts: BTreeMap<String, DraftWriteReceipt>,
    pub pending_artifact: Option<FinalizedArtifactRef>,
    pub finalized_artifact: Option<FinalizedArtifactRef>,
    pub failure: Option<DraftFailure>,
    pub superseded_by: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub content_hash: String,
}

impl ArtifactDraft {
    pub fn new(scope: ArtifactScope, created_at: impl Into<String>) -> Result<Self> {
        let created_at = created_at.into();
        if created_at.is_empty() {
            return Err(StoreError::InvalidDocument {
                kind: "artifact draft",
                message: "created_at must not be empty".to_owned(),
            });
        }
        let draft_id = DraftIdempotencyKey::from_scope(&scope)?.to_string();
        Ok(Self {
            schema_version: DRAFT_SCHEMA_VERSION,
            draft_id,
            state: ArtifactDraftState::for_profile(scope.profile),
            scope,
            lifecycle: DraftLifecycle::Draft,
            revision: 0,
            write_receipts: BTreeMap::new(),
            pending_artifact: None,
            finalized_artifact: None,
            failure: None,
            superseded_by: None,
            updated_at: created_at.clone(),
            created_at,
            content_hash: String::new(),
        })
    }

    pub fn idempotency_key(&self) -> Result<DraftIdempotencyKey> {
        DraftIdempotencyKey::from_scope(&self.scope)
    }

    pub fn validate_for_location(&self, location: &RunLocation) -> Result<()> {
        self.scope.validate_for_location(location)?;
        if self.schema_version != DRAFT_SCHEMA_VERSION
            || self.draft_id != self.idempotency_key()?.as_str()
            || self.state.profile() != self.scope.profile
        {
            return Err(StoreError::InvalidDocument {
                kind: "artifact draft",
                message: "draft identity, schema, or profile state is inconsistent".to_owned(),
            });
        }
        match self.lifecycle {
            DraftLifecycle::Draft => {
                if self.finalized_artifact.is_some()
                    || self.failure.is_some()
                    || self.superseded_by.is_some()
                {
                    return Err(StoreError::InvalidDocument {
                        kind: "artifact draft",
                        message: "draft lifecycle has terminal fields".to_owned(),
                    });
                }
            }
            DraftLifecycle::Completed => {
                if self.finalized_artifact.is_none()
                    || self.pending_artifact.is_some()
                    || self.failure.is_some()
                    || self.superseded_by.is_some()
                {
                    return Err(StoreError::InvalidDocument {
                        kind: "artifact draft",
                        message: "completed lifecycle requires exactly a finalized artifact"
                            .to_owned(),
                    });
                }
            }
            DraftLifecycle::Failed => {
                if self.failure.is_none()
                    || self.finalized_artifact.is_some()
                    || self.pending_artifact.is_some()
                {
                    return Err(StoreError::InvalidDocument {
                        kind: "artifact draft",
                        message: "failed lifecycle requires a failure and no artifact".to_owned(),
                    });
                }
            }
            DraftLifecycle::Superseded => {
                if self.superseded_by.as_deref().unwrap_or_default().is_empty()
                    || self.pending_artifact.is_some()
                {
                    return Err(StoreError::InvalidDocument {
                        kind: "artifact draft",
                        message: "superseded lifecycle requires successor and no pending artifact"
                            .to_owned(),
                    });
                }
            }
        }
        if let Some(reference) = &self.pending_artifact {
            reference.validate()?;
        }
        if let Some(reference) = &self.finalized_artifact {
            reference.validate()?;
        }
        Ok(())
    }
}

impl Versioned for ArtifactDraft {
    const SCHEMA_VERSION: u32 = DRAFT_SCHEMA_VERSION;
}

impl ContentHashDocument for ArtifactDraft {
    fn content_hash(&self) -> &str {
        &self.content_hash
    }

    fn set_content_hash(&mut self, hash: String) {
        self.content_hash = hash;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DraftAppendOutcome {
    Appended {
        draft: ArtifactDraft,
        receipt: DraftWriteReceipt,
    },
    AlreadyApplied {
        draft: ArtifactDraft,
        receipt: DraftWriteReceipt,
    },
}

pub trait FinalizableArtifact: ContentHashDocument {
    fn artifact_id(&self) -> &str;
    fn source_payload_hash(&self) -> &str;
}

#[derive(Debug, Clone, PartialEq)]
pub enum FinalizeDraftOutcome<T> {
    Completed {
        draft: ArtifactDraft,
        artifact: T,
    },
    Recovered {
        draft: ArtifactDraft,
        artifact: FinalizedArtifactRef,
    },
}

pub fn draft_relative(location: &RunLocation, scope: &ArtifactScope) -> Result<PathBuf> {
    scope.validate_for_location(location)?;
    let key = DraftIdempotencyKey::from_scope(scope)?;
    Ok(draft_unit_relative(location, scope)?
        .join(format!("{}.json", SafeSlug::new("draft", key.as_str())?)))
}

pub fn create_or_recover_draft(
    store: &FileStore,
    location: &RunLocation,
    scope: ArtifactScope,
    created_at: impl Into<String>,
) -> Result<ArtifactDraft> {
    scope.validate_for_location(location)?;
    let created_at = created_at.into();
    let relative = draft_relative(location, &scope)?;
    let lock_relative = draft_unit_relative(location, &scope)?.join("lifecycle.lock");
    store.with_exclusive_lock(&lock_relative, || {
        if store.exists(&relative)? {
            let draft = read_draft(store, location, &relative, scope.profile)?;
            if draft.scope != scope {
                return Err(StoreError::InvalidDocument {
                    kind: "artifact draft",
                    message: "idempotency-key collision with a different scope".to_owned(),
                });
            }
            return Ok(draft);
        }

        let successor_id = DraftIdempotencyKey::from_scope(&scope)?.to_string();
        for mut prior in read_unit_drafts(store, location, &scope)? {
            if prior.lifecycle != DraftLifecycle::Superseded
                && prior.scope.source_payload_hash != scope.source_payload_hash
            {
                prior.lifecycle = DraftLifecycle::Superseded;
                prior.pending_artifact = None;
                prior.superseded_by = Some(successor_id.clone());
                prior.updated_at = created_at.clone();
                prior.revision += 1;
                let prior_relative = draft_relative(location, &prior.scope)?;
                write_draft(store, location, &prior_relative, prior)?;
            }
        }

        let draft = ArtifactDraft::new(scope, created_at)?;
        write_draft(store, location, &relative, draft)
    })
}

pub fn append_draft_receipt<T: Serialize>(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    tool_name: impl Into<String>,
    normalized_parameters: &T,
    result_id: impl Into<String>,
    created_at: impl Into<String>,
) -> Result<DraftAppendOutcome> {
    let tool_name = tool_name.into();
    let result_id = result_id.into();
    let created_at = created_at.into();
    if tool_name.is_empty() || result_id.is_empty() || created_at.is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "draft write receipt",
            message: "tool_name, result_id, and created_at must not be empty".to_owned(),
        });
    }
    let parameters = serde_json::to_value(normalized_parameters)
        .map_err(|source| StoreError::JsonSerialize { source })?;
    let key = receipt_hash(&tool_name, &parameters)?;
    let relative = draft_relative(location, scope)?;
    let lock_relative = draft_unit_relative(location, scope)?.join("lifecycle.lock");
    store.with_exclusive_lock(&lock_relative, || {
        let mut draft = read_draft(store, location, &relative, scope.profile)?;
        ensure_draft_lifecycle(&draft, DraftLifecycle::Draft)?;
        if let Some(existing) = draft.write_receipts.get(&key).cloned() {
            return Ok(DraftAppendOutcome::AlreadyApplied {
                draft,
                receipt: existing,
            });
        }
        let receipt = DraftWriteReceipt {
            normalized_parameters_hash: key.clone(),
            tool_name,
            result_id,
            created_at: created_at.clone(),
        };
        draft.write_receipts.insert(key, receipt.clone());
        draft.revision += 1;
        draft.updated_at = created_at;
        let draft = write_draft(store, location, &relative, draft)?;
        Ok(DraftAppendOutcome::Appended { draft, receipt })
    })
}

/// Atomically apply one explicit domain mutation and its idempotency receipt.
/// Keeping the state change and receipt beneath the same unit lock prevents a
/// crash from recording a successful tool call without its typed Draft change.
#[allow(clippy::too_many_arguments)] // the lifecycle boundary is explicit at every call site
pub(crate) fn mutate_draft<T: Serialize, F>(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    tool_name: impl Into<String>,
    normalized_parameters: &T,
    result_id: impl Into<String>,
    created_at: impl Into<String>,
    mutate: F,
) -> Result<DraftAppendOutcome>
where
    F: FnOnce(&mut ArtifactDraftState) -> Result<()>,
{
    let tool_name = tool_name.into();
    let result_id = result_id.into();
    let created_at = created_at.into();
    if tool_name.is_empty() || result_id.is_empty() || created_at.is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "draft write receipt",
            message: "tool_name, result_id, and created_at must not be empty".to_owned(),
        });
    }
    let parameters = serde_json::to_value(normalized_parameters)
        .map_err(|source| StoreError::JsonSerialize { source })?;
    let key = receipt_hash(&tool_name, &parameters)?;
    let relative = draft_relative(location, scope)?;
    let lock_relative = draft_unit_relative(location, scope)?.join("lifecycle.lock");
    store.with_exclusive_lock(&lock_relative, || {
        let mut draft = read_draft(store, location, &relative, scope.profile)?;
        ensure_draft_lifecycle(&draft, DraftLifecycle::Draft)?;
        if let Some(existing) = draft.write_receipts.get(&key).cloned() {
            return Ok(DraftAppendOutcome::AlreadyApplied {
                draft,
                receipt: existing,
            });
        }
        mutate(&mut draft.state)?;
        let receipt = DraftWriteReceipt {
            normalized_parameters_hash: key.clone(),
            tool_name,
            result_id,
            created_at: created_at.clone(),
        };
        draft.write_receipts.insert(key, receipt.clone());
        draft.revision += 1;
        draft.updated_at = created_at;
        let draft = write_draft(store, location, &relative, draft)?;
        Ok(DraftAppendOutcome::Appended { draft, receipt })
    })
}

/// Apply one explicit typed domain command to a live draft.  This is the
/// mutation seam used by domain services; it intentionally accepts an
/// `ArtifactDraftState`, never a JSON path or arbitrary document value.
///
/// The normalized command hash is recorded in the same lock-protected write
/// as the state change, so a provider retry cannot append a duplicate claim,
/// response, or controller instruction.
#[allow(clippy::too_many_arguments)] // profile commands need full Rust-owned lifecycle scope
pub fn apply_typed_draft_command<T: Serialize>(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    tool_name: impl Into<String>,
    normalized_parameters: &T,
    result_id: impl Into<String>,
    created_at: impl Into<String>,
    mutate: impl FnOnce(&mut ArtifactDraftState) -> Result<()>,
) -> Result<DraftAppendOutcome> {
    let tool_name = tool_name.into();
    let result_id = result_id.into();
    let created_at = created_at.into();
    if tool_name.is_empty() || result_id.is_empty() || created_at.is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "draft command",
            message: "tool_name, result_id, and created_at must not be empty".to_owned(),
        });
    }
    let parameters = serde_json::to_value(normalized_parameters)
        .map_err(|source| StoreError::JsonSerialize { source })?;
    let key = receipt_hash(&tool_name, &parameters)?;
    let relative = draft_relative(location, scope)?;
    let lock_relative = draft_unit_relative(location, scope)?.join("lifecycle.lock");
    store.with_exclusive_lock(&lock_relative, || {
        let mut draft = read_draft(store, location, &relative, scope.profile)?;
        ensure_draft_lifecycle(&draft, DraftLifecycle::Draft)?;
        if let Some(existing) = draft.write_receipts.get(&key).cloned() {
            return Ok(DraftAppendOutcome::AlreadyApplied {
                draft,
                receipt: existing,
            });
        }
        mutate(&mut draft.state)?;
        let receipt = DraftWriteReceipt {
            normalized_parameters_hash: key.clone(),
            tool_name,
            result_id,
            created_at: created_at.clone(),
        };
        draft.write_receipts.insert(key, receipt.clone());
        draft.revision += 1;
        draft.updated_at = created_at;
        let draft = write_draft(store, location, &relative, draft)?;
        Ok(DraftAppendOutcome::Appended { draft, receipt })
    })
}

/// Read the exact typed draft for a known scope.  Domain finalizers use this
/// after acquiring their lifecycle lock through `finalize_draft_atomic`.
pub fn read_draft_for_scope(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
) -> Result<ArtifactDraft> {
    let relative = draft_relative(location, scope)?;
    read_draft(store, location, &relative, scope.profile)
}

pub fn fail_draft(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    failure: DraftFailure,
) -> Result<ArtifactDraft> {
    if failure.code.is_empty() || failure.message.is_empty() || failure.failed_at.is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "draft failure",
            message: "failure code, message, and timestamp are required".to_owned(),
        });
    }
    let relative = draft_relative(location, scope)?;
    let lock_relative = draft_unit_relative(location, scope)?.join("lifecycle.lock");
    store.with_exclusive_lock(&lock_relative, || {
        let mut draft = read_draft(store, location, &relative, scope.profile)?;
        ensure_draft_lifecycle(&draft, DraftLifecycle::Draft)?;
        draft.lifecycle = DraftLifecycle::Failed;
        draft.failure = Some(failure.clone());
        draft.updated_at = failure.failed_at;
        draft.revision += 1;
        write_draft(store, location, &relative, draft)
    })
}

pub fn finalize_draft_atomic<T: FinalizableArtifact>(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
    artifact_relative: &Path,
    artifact: T,
    created_at: impl Into<String>,
) -> Result<FinalizeDraftOutcome<T>> {
    validate_relative_path(artifact_relative)?;
    let created_at = created_at.into();
    if created_at.is_empty() {
        return Err(StoreError::InvalidDocument {
            kind: "draft finalize",
            message: "created_at must not be empty".to_owned(),
        });
    }
    if artifact.source_payload_hash() != scope.source_payload_hash {
        return Err(StoreError::InvalidDocument {
            kind: "finalized artifact",
            message: "artifact source_payload_hash differs from draft scope".to_owned(),
        });
    }
    let artifact_ref = FinalizedArtifactRef::new(
        artifact.artifact_id(),
        artifact_relative,
        scope.phase,
        scope.role.clone(),
        scope.profile.as_str(),
        scope.unit_key.clone(),
        scope.source_payload_hash.clone(),
        created_at.clone(),
    )?;
    let draft_relative = draft_relative(location, scope)?;
    let artifact_store_relative = location.child_relative(artifact_relative)?;
    let lock_relative = draft_unit_relative(location, scope)?.join("lifecycle.lock");
    store.with_exclusive_lock(&lock_relative, || {
        let mut draft = read_draft(store, location, &draft_relative, scope.profile)?;
        match draft.lifecycle {
            DraftLifecycle::Completed => {
                let finalized = draft
                    .finalized_artifact
                    .clone()
                    .expect("validated completed draft");
                if !finalized.same_artifact_identity(&artifact_ref) {
                    return Err(StoreError::InvalidDocument {
                        kind: "draft finalize",
                        message: "completed draft was finalized with a different artifact"
                            .to_owned(),
                    });
                }
                return Ok(FinalizeDraftOutcome::Recovered {
                    draft,
                    artifact: finalized,
                });
            }
            DraftLifecycle::Draft => {}
            _ => {
                return Err(invalid_transition(
                    draft.lifecycle,
                    DraftLifecycle::Completed,
                ))
            }
        }

        draft.pending_artifact = Some(artifact_ref.clone());
        draft.updated_at = created_at.clone();
        draft.revision += 1;
        draft = write_draft(store, location, &draft_relative, draft)?;

        let sealed_artifact = if store.exists(&artifact_store_relative)? {
            validate_existing_artifact_ref(store, &artifact_store_relative, &artifact_ref)?;
            None
        } else {
            Some(store.write_authoritative_json(&artifact_store_relative, artifact)?)
        };

        draft.lifecycle = DraftLifecycle::Completed;
        draft.pending_artifact = None;
        draft.finalized_artifact = Some(artifact_ref.clone());
        draft.updated_at = created_at;
        draft.revision += 1;
        let draft = write_draft(store, location, &draft_relative, draft)?;
        match sealed_artifact {
            Some(artifact) => Ok(FinalizeDraftOutcome::Completed { draft, artifact }),
            None => Ok(FinalizeDraftOutcome::Recovered {
                draft,
                artifact: artifact_ref,
            }),
        }
    })
}

pub(crate) fn validate_existing_artifact_ref(
    store: &FileStore,
    relative: &Path,
    expected: &FinalizedArtifactRef,
) -> Result<()> {
    let value = store.read_json_value(relative)?;
    let absolute = store.root().join(relative);
    validate_content_hash_at(&value, &absolute)?;
    let object = value
        .as_object()
        .ok_or_else(|| StoreError::FinalizedArtifactMismatch {
            path: absolute.clone(),
            message: "artifact is not a JSON object".to_owned(),
        })?;
    let artifact_id = object.get("artifact_id").and_then(Value::as_str);
    let source_hash = object.get("source_payload_hash").and_then(Value::as_str);
    let phase = object.get("phase").and_then(Value::as_u64);
    let role = object.get("role").and_then(Value::as_str);
    let profile = object.get("profile").and_then(Value::as_str);
    let unit_key = object.get("unit_key").and_then(Value::as_str);
    if artifact_id != Some(expected.artifact_id.as_str())
        || source_hash != Some(expected.source_payload_hash.as_str())
        || phase != Some(u64::from(expected.phase))
        || role != Some(expected.role.as_str())
        || profile != Some(expected.profile.as_str())
        || unit_key != Some(expected.unit_key.as_str())
    {
        return Err(StoreError::FinalizedArtifactMismatch {
            path: absolute,
            message: "artifact header differs from pending draft scope".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn read_draft(
    store: &FileStore,
    location: &RunLocation,
    relative: &Path,
    profile: DraftProfile,
) -> Result<ArtifactDraft> {
    let draft = store.read_versioned_json::<ArtifactDraft>(
        relative,
        FileSchemaKind::Draft(profile.as_str().to_owned()),
    )?;
    draft.validate_for_location(location)?;
    if draft.scope.profile != profile {
        return Err(StoreError::InvalidDocument {
            kind: "artifact draft",
            message: "draft path profile differs from typed scope profile".to_owned(),
        });
    }
    Ok(draft)
}

pub(crate) fn write_draft(
    store: &FileStore,
    location: &RunLocation,
    relative: &Path,
    draft: ArtifactDraft,
) -> Result<ArtifactDraft> {
    draft.validate_for_location(location)?;
    store.write_authoritative_json(relative, draft)
}

pub(crate) fn draft_unit_relative(
    location: &RunLocation,
    scope: &ArtifactScope,
) -> Result<PathBuf> {
    scope.validate_for_location(location)?;
    Ok(location
        .relative_root()
        .join("drafts")
        .join(SafeSlug::new("profile", scope.profile.as_str())?.as_str())
        .join(SafeSlug::new("unit", &scope.unit_key)?.as_str()))
}

fn read_unit_drafts(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
) -> Result<Vec<ArtifactDraft>> {
    let unit_relative = draft_unit_relative(location, scope)?;
    let absolute = store.root().join(&unit_relative);
    if !absolute.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(&absolute).map_err(|source| io_error(&absolute, source))? {
        let entry = entry.map_err(|source| io_error(&absolute, source))?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::SymlinkPath { path });
        }
        if metadata.is_file()
            && path
                .extension()
                .is_some_and(|extension| extension == "json")
        {
            paths.push(unit_relative.join(entry.file_name()));
        }
    }
    paths.sort();
    paths
        .into_iter()
        .map(|relative| read_draft(store, location, &relative, scope.profile))
        .collect()
}

fn ensure_draft_lifecycle(draft: &ArtifactDraft, expected: DraftLifecycle) -> Result<()> {
    if draft.lifecycle == expected {
        Ok(())
    } else {
        Err(invalid_transition(draft.lifecycle, expected))
    }
}

fn invalid_transition(from: DraftLifecycle, to: DraftLifecycle) -> StoreError {
    StoreError::InvalidDraftTransition {
        from: lifecycle_name(from).to_owned(),
        to: lifecycle_name(to).to_owned(),
    }
}

fn lifecycle_name(value: DraftLifecycle) -> &'static str {
    match value {
        DraftLifecycle::Draft => "draft",
        DraftLifecycle::Completed => "completed",
        DraftLifecycle::Failed => "failed",
        DraftLifecycle::Superseded => "superseded",
    }
}

fn receipt_hash(tool_name: &str, parameters: &Value) -> Result<String> {
    #[derive(Serialize)]
    struct ReceiptHashInput<'a> {
        tool_name: &'a str,
        parameters: &'a Value,
    }
    let value = serde_json::to_value(ReceiptHashInput {
        tool_name,
        parameters,
    })
    .map_err(|source| StoreError::JsonSerialize { source })?;
    Ok(content_hash_bytes(&canonical_json_bytes(&value)?))
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    use super::{
        append_draft_receipt, create_or_recover_draft, draft_relative, fail_draft,
        finalize_draft_atomic, ArtifactDraft, ArtifactScope, DraftAppendOutcome, DraftFailure,
        DraftLifecycle, DraftProfile, FinalizableArtifact, FinalizeDraftOutcome,
    };
    use crate::{ContentHashDocument, FileSchemaKind, FileStore, FileStoreOptions, RunLocation};

    fn location() -> RunLocation {
        RunLocation::new("2026-07-27", "run-one").unwrap()
    }

    fn scope(source_payload_hash: &str) -> ArtifactScope {
        ArtifactScope {
            run_id: "run-one".to_owned(),
            current_date: "2026-07-27".to_owned(),
            phase: 3,
            role: "manager.research".to_owned(),
            profile: DraftProfile::ResearchDecision,
            profile_version: 1,
            builder_version: 1,
            unit_key: "QQQ".to_owned(),
            source_payload_hash: source_payload_hash.to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            side: None,
            stance: None,
            round: None,
            reflection_task: None,
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestArtifact {
        schema_version: u32,
        artifact_id: String,
        phase: u8,
        role: String,
        profile: String,
        unit_key: String,
        source_payload_hash: String,
        value: String,
        content_hash: String,
    }

    impl ContentHashDocument for TestArtifact {
        fn content_hash(&self) -> &str {
            &self.content_hash
        }

        fn set_content_hash(&mut self, hash: String) {
            self.content_hash = hash;
        }
    }

    impl FinalizableArtifact for TestArtifact {
        fn artifact_id(&self) -> &str {
            &self.artifact_id
        }

        fn source_payload_hash(&self) -> &str {
            &self.source_payload_hash
        }
    }

    fn artifact(source_payload_hash: &str) -> TestArtifact {
        TestArtifact {
            schema_version: 2,
            artifact_id: "artifact-qqq".to_owned(),
            phase: 3,
            role: "manager.research".to_owned(),
            profile: "research_decision".to_owned(),
            unit_key: "QQQ".to_owned(),
            source_payload_hash: source_payload_hash.to_owned(),
            value: "decision".to_owned(),
            content_hash: String::new(),
        }
    }

    #[test]
    fn duplicate_append_is_idempotent_and_source_change_supersedes_old_draft() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let first_scope = scope("sha256:one");
        let first = create_or_recover_draft(
            &store,
            &location,
            first_scope.clone(),
            "2026-07-27T00:00:00Z",
        )
        .unwrap();
        let appended = append_draft_receipt(
            &store,
            &location,
            &first_scope,
            "set_research_decision",
            &("long", 0.7f64),
            "receipt-1",
            "2026-07-27T00:00:01Z",
        )
        .unwrap();
        assert!(matches!(appended, DraftAppendOutcome::Appended { .. }));
        let duplicate = append_draft_receipt(
            &store,
            &location,
            &first_scope,
            "set_research_decision",
            &("long", 0.7f64),
            "receipt-ignored",
            "2026-07-27T00:00:02Z",
        )
        .unwrap();
        assert!(matches!(
            duplicate,
            DraftAppendOutcome::AlreadyApplied { .. }
        ));

        let second_scope = scope("sha256:two");
        let second =
            create_or_recover_draft(&store, &location, second_scope, "2026-07-27T00:01:00Z")
                .unwrap();
        assert_ne!(first.draft_id, second.draft_id);
        let superseded = store
            .read_versioned_json::<ArtifactDraft>(
                &draft_relative(&location, &first_scope).unwrap(),
                FileSchemaKind::Draft("research_decision".to_owned()),
            )
            .unwrap();
        assert_eq!(superseded.lifecycle, DraftLifecycle::Superseded);
    }

    #[test]
    fn finalize_commits_artifact_then_restores_completed_draft_idempotently() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let scope = scope("sha256:source");
        create_or_recover_draft(&store, &location, scope.clone(), "2026-07-27T00:00:00Z").unwrap();
        let first = finalize_draft_atomic(
            &store,
            &location,
            &scope,
            std::path::Path::new("artifacts/phase3/qqq.json"),
            artifact("sha256:source"),
            "2026-07-27T00:03:00Z",
        )
        .unwrap();
        assert!(matches!(first, FinalizeDraftOutcome::Completed { .. }));
        let second = finalize_draft_atomic(
            &store,
            &location,
            &scope,
            std::path::Path::new("artifacts/phase3/qqq.json"),
            artifact("sha256:source"),
            "2026-07-27T00:02:00Z",
        )
        .unwrap();
        assert!(matches!(second, FinalizeDraftOutcome::Recovered { .. }));
    }

    #[test]
    fn failed_drafts_are_terminal_and_cannot_accept_more_writes() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let scope = scope("sha256:source");
        create_or_recover_draft(&store, &location, scope.clone(), "2026-07-27T00:00:00Z").unwrap();
        let failed = fail_draft(
            &store,
            &location,
            &scope,
            DraftFailure {
                code: "finalize_failed".to_owned(),
                message: "missing evidence".to_owned(),
                failed_at: "2026-07-27T00:03:00Z".to_owned(),
            },
        )
        .unwrap();
        assert_eq!(failed.lifecycle, DraftLifecycle::Failed);
        assert!(append_draft_receipt(
            &store,
            &location,
            &scope,
            "set_research_decision",
            &"hold",
            "receipt",
            "2026-07-27T00:04:00Z",
        )
        .is_err());
    }
}
