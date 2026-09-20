//! Bounded research refinement and immutable final-proposal review.
use crate::{ArtifactKind, ArtifactRef, Asset, ContentHash, DecisionHorizon, DomainError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const REVIEWED_RESEARCH_CONTRACT_VERSION: u32 = 67;
pub const STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION: u32 = 69;
#[cfg(test)]
#[path = "research_review_tests.rs"]
mod quality_tests;
pub const RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID: &str = "research.proposal_reviewer";
pub const RESEARCH_SUPPLEMENT_RECIPE_ID: &str = "research.supplement";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResearchSettings {
    /// Revisions after the initial proposal; each revision gets its own review.
    pub max_proposal_revisions: u8,
}
impl Default for ResearchSettings {
    fn default() -> Self {
        Self {
            max_proposal_revisions: 2,
        }
    }
}
impl ResearchSettings {
    pub fn validate(&self) -> Result<(), DomainError> {
        // Full Paper graph: evidence + 12 horizon tasks + supplement +
        // proposal/review pairs + five terminal gates = 21 + 2 * revisions.
        if 21usize + 2 * usize::from(self.max_proposal_revisions) > 32 {
            return Err(DomainError::InvalidBudget {
                field: "agent.research.max_proposal_revisions exceeds 32 workflow nodes",
            });
        }
        Ok(())
    }
}

pub fn proposal_review_keys() -> BTreeSet<String> {
    let mut keys = BTreeSet::from(["allocation.cash".to_owned()]);
    for asset in Asset::EXECUTABLE {
        keys.insert(format!("allocation.{}", asset.symbol()));
        for horizon in ["t1", "t3", "t5"] {
            keys.insert(format!("forecast.{}.{horizon}", asset.symbol()));
        }
    }
    keys
}

/// An explanation of a raw estimate, never an empirical calibration claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumericEstimateBasis {
    pub scope: String,
    pub inputs: Vec<ArtifactRef>,
    pub units: String,
    pub method: String,
    pub assumptions: String,
    pub uncertainty: String,
}
pub fn validate_numeric_bases(bases: &[NumericEstimateBasis]) -> Result<(), DomainError> {
    let scopes = bases
        .iter()
        .map(|b| b.scope.clone())
        .collect::<BTreeSet<_>>();
    if scopes != proposal_review_keys()
        || scopes.len() != bases.len()
        || bases.iter().any(|b| {
            b.inputs.is_empty()
                || [&b.units, &b.method, &b.assumptions, &b.uncertainty]
                    .into_iter()
                    .any(|s| s.trim().is_empty())
                || b.inputs.iter().any(|r| {
                    !matches!(
                        r.kind,
                        ArtifactKind::Claim
                            | ArtifactKind::Critique
                            | ArtifactKind::NormalizedEvidence
                            | ArtifactKind::SemanticDetail
                    )
                })
        })
    {
        return Err(DomainError::EmptyField { field: "proposal.numeric_basis requires all 12 forecasts and 5 allocations with input refs, units, method, assumptions and uncertainty" });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalAssessment {
    pub scope: String,
    pub accepted: bool,
    pub rationale: String,
    pub evidence_refs: Vec<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<ProposalIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProposalIssueCategory {
    SupportMissing,
    SourceQualification,
    ConflictingSupport,
    TemporalMismatch,
    UnitError,
    EstimateBasisMismatch,
    AllocationInconsistency,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposalIssue {
    pub category: ProposalIssueCategory,
    /// Semantic field path, independent of array ordering.
    pub field_path: String,
    pub correction_criterion: String,
    pub evidence_refs: Vec<ArtifactRef>,
}

impl ProposalIssue {
    pub fn stable_id(&self, scope: &str) -> String {
        // Typed category + field identity; prose changes never reset progress.
        ContentHash::of_bytes(
            serde_json::to_string(&(scope, &self.category, &self.field_path))
                .expect("serializable issue identity")
                .as_bytes(),
        )
        .to_string()
    }
}
/// Identity fields are filled by Rust after validating the model's assessments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProposalReview {
    pub proposal: ArtifactRef,
    pub proposal_hash: ContentHash,
    pub manifest: ArtifactRef,
    pub contract_hash: ContentHash,
    pub assessments: Vec<ProposalAssessment>,
}
impl ProposalReview {
    pub fn validate_for_contract(&self, version: u32) -> Result<(), DomainError> {
        self.validate()?;
        if version < STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION {
            return Ok(());
        }
        for assessment in &self.assessments {
            let mut ids = BTreeSet::new();
            if assessment.issues.len() > 3
                || assessment.accepted != assessment.issues.is_empty()
                || assessment.issues.iter().any(|issue| {
                    let path_matches = issue.field_path == assessment.scope
                        || issue
                            .field_path
                            .starts_with(&format!("{}.", assessment.scope))
                        || issue
                            .field_path
                            .starts_with(&format!("numeric_basis.{}.", assessment.scope));
                    !path_matches
                        || issue.correction_criterion.trim().is_empty()
                        || !ids.insert(issue.stable_id(&assessment.scope))
                        || issue.evidence_refs.iter().any(|r| {
                            !matches!(
                                r.kind,
                                ArtifactKind::Claim
                                    | ArtifactKind::Critique
                                    | ArtifactKind::NormalizedEvidence
                                    | ArtifactKind::SemanticDetail
                            )
                        })
                })
            {
                return Err(DomainError::EmptyField {field:"proposal_review structured issues require a rejected scope, matching field, unique ID and correction criterion"});
            }
        }
        Ok(())
    }
    pub fn validate(&self) -> Result<(), DomainError> {
        let keys = self
            .assessments
            .iter()
            .map(|a| a.scope.clone())
            .collect::<BTreeSet<_>>();
        if self.proposal.kind != ArtifactKind::DecisionProposal
            || self.manifest.kind != ArtifactKind::ContextManifest
            || keys != proposal_review_keys()
            || keys.len() != self.assessments.len()
            || self
                .assessments
                .iter()
                .any(|a| a.rationale.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "proposal_review complete scope and binding",
            });
        }
        Ok(())
    }
    pub fn accepted(&self) -> bool {
        self.validate().is_ok() && self.assessments.iter().all(|a| a.accepted)
    }
    pub fn authorizes(&self, proposal: &ArtifactRef, hash: &ContentHash) -> bool {
        self.accepted() && &self.proposal == proposal && &self.proposal_hash == hash
    }
}

/// Scope equality compares typed values, never fuzzy prose or array position.
pub fn proposal_scope_value(proposal: &crate::DecisionDraft, scope: &str) -> serde_json::Value {
    let parts = scope.split('.').collect::<Vec<_>>();
    let value = match parts.as_slice() {
        ["forecast", asset, horizon] => proposal
            .forecasts
            .iter()
            .find(|f| {
                f.asset.symbol() == *asset
                    && serde_json::to_value(f.horizon)
                        .ok()
                        .as_ref()
                        .and_then(serde_json::Value::as_str)
                        == Some(*horizon)
            })
            .map(|f| serde_json::to_value(f).expect("forecast serialization")),
        ["allocation", "cash"] => proposal
            .research_allocation
            .as_ref()
            .map(|a| serde_json::json!(a.cash_weight_ppm)),
        ["allocation", asset] => proposal
            .research_allocation
            .as_ref()
            .and_then(|a| a.allocations.iter().find(|a| a.asset.symbol() == *asset))
            .map(|a| serde_json::to_value(a).expect("allocation serialization")),
        _ => None,
    };
    serde_json::json!({"value":value,"basis":proposal.numeric_basis.iter().find(|b| b.scope == scope)})
}

pub fn validate_proposal_revision(
    previous: &crate::DecisionDraft,
    next: &crate::DecisionDraft,
    review: &ProposalReview,
) -> Result<(), DomainError> {
    // Portfolio weights share a sum and may depend on any repaired forecast.
    // Unrelated accepted forecasts (including their basis) remain frozen.
    let model_owned = |proposal: &crate::DecisionDraft, scope: &str| {
        let mut value = proposal_scope_value(proposal, scope);
        if let Some(thesis) = value
            .pointer_mut("/value/thesis")
            .and_then(serde_json::Value::as_object_mut)
        {
            // These two fields are rebound from the governed calendar by Rust.
            thesis.remove("thesis_valid_until");
            thesis.remove("expected_holding_period_days");
        }
        value
    };
    if review.assessments.iter().any(|a| {
        a.accepted
            && a.scope.starts_with("forecast.")
            && model_owned(previous, &a.scope) != model_owned(next, &a.scope)
    }) {
        return Err(DomainError::EmptyField {
            field: "proposal revision changed an accepted forecast or its numeric basis",
        });
    }
    Ok(())
}

pub fn proposal_revision_stagnated(
    previous: &crate::DecisionDraft,
    previous_review: &ProposalReview,
    current: &crate::DecisionDraft,
    current_review: &ProposalReview,
) -> bool {
    if previous_review
        .validate_for_contract(STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION)
        .is_err()
        || current_review
            .validate_for_contract(STRUCTURED_REVIEW_ISSUES_CONTRACT_VERSION)
            .is_err()
        || previous_review.accepted()
        || current_review.accepted()
    {
        return false;
    }
    let fingerprint = |review: &ProposalReview, proposal: &crate::DecisionDraft| {
        let mut entries = review.assessments.iter().filter(|a| !a.accepted).map(|a| {
            let mut issues = a.issues.iter().map(|i| {
                let mut refs = i.evidence_refs.clone(); refs.sort(); refs.dedup();
                (i.stable_id(&a.scope),refs)
            }).collect::<Vec<_>>(); issues.sort();
            let mut refs = a.evidence_refs.clone(); refs.sort(); refs.dedup();
            (a.scope.clone(),serde_json::json!({"issues":issues,"refs":refs,"content":proposal_scope_value(proposal,&a.scope)}).to_string())
        }).collect::<Vec<_>>();
        entries.sort();
        entries
    };
    fingerprint(previous_review, previous) == fingerprint(current_review, current)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupplementalKind {
    News,
    Price,
    Macro,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupplementalIntent {
    pub kind: SupplementalKind,
    pub assets: Vec<Asset>,
    pub series: Vec<String>,
    pub query: String,
}
impl SupplementalIntent {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.query.trim().is_empty()
            || self.assets.is_empty()
            || self.assets.iter().collect::<BTreeSet<_>>().len() != self.assets.len()
            || self.series.iter().collect::<BTreeSet<_>>().len() != self.series.len()
            || match self.kind {
                SupplementalKind::Macro => {
                    self.series.is_empty()
                        || self.series.iter().any(|s| {
                            !matches!(s.as_str(), "DFF" | "DFII10" | "VIXCLS" | "DGS2" | "DGS10")
                        })
                }
                _ => !self.series.is_empty(),
            }
        {
            return Err(DomainError::EmptyField {
                field:
                    "supplemental intent: unique assets, supported kind/series and query required",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupplementalDisposition {
    pub requester: ArtifactRef,
    pub horizon: DecisionHorizon,
    pub gap_index: usize,
    pub request_index: usize,
    pub resource: Option<String>,
    pub status: String,
    pub reason: String,
    pub evidence: Vec<ArtifactRef>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupplementalRound {
    pub dispositions: Vec<SupplementalDisposition>,
    pub affected_horizons: BTreeSet<DecisionHorizon>,
    pub evidence: Vec<ArtifactRef>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn revision_budget_is_bounded_and_zero_is_legal() {
        assert_eq!(ResearchSettings::default().max_proposal_revisions, 2);
        for n in 0..=5 {
            assert!(ResearchSettings {
                max_proposal_revisions: n
            }
            .validate()
            .is_ok());
        }
        assert!(ResearchSettings {
            max_proposal_revisions: 6
        }
        .validate()
        .is_err());
        assert_eq!(proposal_review_keys().len(), 17);
    }
    #[test]
    fn supplemental_intent_rejects_duplicate_assets_and_unsupported_series() {
        let mut intent = SupplementalIntent {
            kind: SupplementalKind::News,
            assets: Asset::EXECUTABLE.to_vec(),
            series: vec![],
            query: "news".into(),
        };
        assert!(intent.validate().is_ok());
        intent.assets.push(Asset::Qqq);
        assert!(intent.validate().is_err());
        intent.assets = vec![Asset::Qqq];
        intent.kind = SupplementalKind::Macro;
        intent.series = vec!["unknown".into()];
        assert!(intent.validate().is_err());
    }
}
