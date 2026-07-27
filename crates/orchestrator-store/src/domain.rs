//! Typed Draft builders for the first migrated business profiles.
//!
//! These functions are intentionally small and domain-specific.  They are the
//! only mutable route for AnalystReport and ResearchDecision Drafts; callers
//! cannot select a path or apply arbitrary JSON patches.

use std::{collections::BTreeMap, path::PathBuf};

use orchestrator_core::artifact::{Scenario, Scenarios};
use orchestrator_core::{
    validate_analyst_ticker_artifact, validate_research_artifact, AnalystTickerArtifact,
    EvidenceItem,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    content_hash, create_or_recover_draft, draft::mutate_draft, draft::read_draft,
    finalize_draft_atomic, AnalystAssessmentDraft, ArtifactDraftState, ArtifactScope,
    ContentHashDocument, DraftAppendOutcome, DraftProfile, FileStore, FinalizableArtifact,
    FinalizeDraftOutcome, ResearchDecisionDraftEntry, Result, RunLocation, SafeSlug, StoreError,
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

#[derive(Debug, Clone, PartialEq)]
pub enum DomainFinalizeOutcome {
    Analyst(Box<FinalizeDraftOutcome<AnalystArtifact>>),
    Research(Box<FinalizeDraftOutcome<ResearchArtifact>>),
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
}
