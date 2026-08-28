//! Typed decision inputs and risk findings.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    Asset, ContentHash, DecisionId, DomainError, EvidenceGapImpact, EvidenceGroundRole,
    ResearchClaim, ResearchShard, RunId, TargetPortfolio, V2_DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardBlocker {
    UnsupportedUniverse,
    NoExecutableOrder,
    Frozen,
    MissingEvidence,
    InvalidProvenance,
    MaterialConflict,
    StaleQuote,
    MissingQuote,
    StaleAccount,
    MissingAccount,
    MarketClosed,
    FactorLimit,
    PairExposureLimit,
    TurnoverLimit,
    PlanHashMismatch,
    DuplicateCommitment,
    NonPaperEndpoint,
    NonCanonicalRun,
    RecoveryIncomplete,
    ExternalPosition,
    UnmanagedOpenOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SoftWarning {
    LowConfidence,
    IncompleteEvidence,
    ElevatedTurnover,
    SlowModelResponse,
    StaleNoncriticalEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterialConflict {
    pub claim: ArtifactRef,
    pub critique: ArtifactRef,
    pub topic: String,
    pub rationale: String,
}

impl MaterialConflict {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.claim.kind != ArtifactKind::Claim || self.critique.kind != ArtifactKind::Critique {
            return Err(DomainError::EmptyField {
                field: "material_conflict.references",
            });
        }
        if self.topic.trim().is_empty() || self.rationale.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "material_conflict.description",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionContext {
    pub schema_version: u32,
    pub decision_id: DecisionId,
    pub run_id: RunId,
    pub claims: Vec<ArtifactRef>,
    pub critiques: Vec<ArtifactRef>,
    pub evidence: Vec<ArtifactRef>,
    pub policy_influences: Vec<ArtifactRef>,
    #[serde(default)]
    pub applied_learning_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub rejected_learning_refs: Vec<ArtifactRef>,
    pub material_conflicts: Vec<MaterialConflict>,
    pub hard_blockers: Vec<HardBlocker>,
    pub soft_warnings: Vec<SoftWarning>,
    /// Hash of the Rust-owned policy that converted model forecasts into the
    /// target portfolio. The model never supplies this value.
    pub decision_policy_hash: ContentHash,
    pub target: TargetPortfolio,
    pub created_at: DateTime<Utc>,
}

impl DecisionContext {
    pub fn accepted(&self) -> bool {
        self.hard_blockers.is_empty() && self.material_conflicts.is_empty()
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "decision_context.schema_version",
            });
        }
        if self.decision_id.0.trim().is_empty() || self.run_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_context.identity",
            });
        }
        if self
            .claims
            .iter()
            .any(|reference| reference.kind != ArtifactKind::Claim)
            || self
                .critiques
                .iter()
                .any(|reference| reference.kind != ArtifactKind::Critique)
            || self.evidence.iter().any(|reference| {
                !matches!(
                    reference.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                )
            })
            || self.policy_influences.iter().any(|reference| {
                !matches!(
                    reference.kind,
                    ArtifactKind::Experience | ArtifactKind::CandidatePolicy
                )
            })
        {
            return Err(DomainError::EmptyField {
                field: "decision_context.references",
            });
        }
        for reference in self
            .applied_learning_refs
            .iter()
            .chain(self.rejected_learning_refs.iter())
        {
            if !matches!(
                reference.kind,
                ArtifactKind::Lesson | ArtifactKind::Experience | ArtifactKind::CandidatePolicy
            ) {
                return Err(DomainError::EmptyField {
                    field: "decision_context.learning_refs",
                });
            }
        }
        if self
            .applied_learning_refs
            .iter()
            .any(|reference| self.rejected_learning_refs.contains(reference))
        {
            return Err(DomainError::EmptyField {
                field: "decision_context.learning_refs_overlap",
            });
        }
        if self.claims.is_empty() && self.hard_blockers.is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_context.claims_or_blockers",
            });
        }
        for conflict in &self.material_conflicts {
            conflict.validate()?;
        }
        self.target.validate_universe()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionHorizon {
    T1,
    T3,
    T5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Forecast {
    pub asset: Asset,
    pub horizon: DecisionHorizon,
    pub positive_return_probability_ppm: u32,
    pub expected_return_ppm: i64,
}

impl Forecast {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.positive_return_probability_ppm > 1_000_000 {
            return Err(DomainError::InvalidDecisionForecastProbability);
        }
        Ok(())
    }

    pub fn is_neutral(&self) -> bool {
        self.expected_return_ppm == 0 && self.positive_return_probability_ppm == 500_000
    }
}

/// Schema-bounded model output. It can request a decision, but cannot embed a
/// grant, permit, endpoint, order, or free-form execution authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionDraft {
    pub summary: String,
    pub confidence_ppm: u32,
    pub forecasts: Vec<Forecast>,
    pub claims: Vec<ArtifactRef>,
    pub critiques: Vec<ArtifactRef>,
    pub evidence: Vec<ArtifactRef>,
    pub material_conflicts: Vec<MaterialConflict>,
    pub hard_blockers: Vec<HardBlocker>,
    pub soft_warnings: Vec<SoftWarning>,
    #[serde(default)]
    pub applied_learning_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub rejected_learning_refs: Vec<ArtifactRef>,
}

/// Public vocabulary for the proposal emitted by the research synthesizer.
/// The wire shape remains `DecisionDraft`; Rust gates it before creating a
/// durable `DecisionContext`.
pub type DecisionProposal = DecisionDraft;

impl DecisionDraft {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.summary.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_draft.summary",
            });
        }
        if self.confidence_ppm > 1_000_000 {
            return Err(DomainError::InvalidDecisionConfidence);
        }
        if self
            .claims
            .iter()
            .any(|reference| reference.kind != ArtifactKind::Claim)
            || self
                .critiques
                .iter()
                .any(|reference| reference.kind != ArtifactKind::Critique)
            || self.evidence.iter().any(|reference| {
                !matches!(
                    reference.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                )
            })
        {
            return Err(DomainError::EmptyField {
                field: "decision_draft.references",
            });
        }
        if self.claims.is_empty() && self.hard_blockers.is_empty() {
            return Err(DomainError::EmptyField {
                field: "decision_draft.claims_or_blockers",
            });
        }
        for reference in self
            .applied_learning_refs
            .iter()
            .chain(self.rejected_learning_refs.iter())
        {
            if !matches!(
                reference.kind,
                ArtifactKind::Lesson | ArtifactKind::Experience | ArtifactKind::CandidatePolicy
            ) {
                return Err(DomainError::EmptyField {
                    field: "decision_draft.learning_refs",
                });
            }
        }
        if self
            .applied_learning_refs
            .iter()
            .any(|reference| self.rejected_learning_refs.contains(reference))
        {
            return Err(DomainError::EmptyField {
                field: "decision_draft.learning_refs_overlap",
            });
        }
        for conflict in &self.material_conflicts {
            conflict.validate()?;
            if !self.claims.contains(&conflict.claim)
                || !self.critiques.contains(&conflict.critique)
            {
                return Err(DomainError::EmptyField {
                    field: "decision_draft.material_conflicts",
                });
            }
        }
        validate_forecasts(&self.forecasts)
    }
}

pub fn validate_decision_evidence_sufficiency(
    draft: &DecisionDraft,
    claims: &[ResearchClaim],
) -> Result<(), DomainError> {
    let has_gaps = claims.iter().any(|claim| !claim.evidence_gaps.is_empty());
    let has_blocking_gap = claims.iter().any(|claim| {
        claim
            .evidence_gaps
            .iter()
            .any(|gap| gap.impact == EvidenceGapImpact::BlocksDirectionalForecast)
    });
    let has_missing_evidence = draft.hard_blockers.contains(&HardBlocker::MissingEvidence);
    let has_incomplete_evidence = draft
        .soft_warnings
        .contains(&SoftWarning::IncompleteEvidence);

    if has_gaps && !has_incomplete_evidence {
        return Err(DomainError::InsufficientDecisionEvidence);
    }

    let has_non_neutral_forecast = draft
        .forecasts
        .iter()
        .any(|forecast| !forecast.is_neutral());
    if has_blocking_gap || has_missing_evidence {
        if !has_missing_evidence || has_non_neutral_forecast {
            return Err(DomainError::InsufficientDecisionEvidence);
        }
        return Ok(());
    }

    if !has_non_neutral_forecast {
        return Ok(());
    }

    let covered = |asset: Asset, horizon: DecisionHorizon| {
        let domains = claims
            .iter()
            .filter(|claim| claim.horizon == horizon)
            .flat_map(|claim| claim.grounds.iter())
            .filter(|ground| {
                ground.role == EvidenceGroundRole::Directional && ground.assets.contains(&asset)
            })
            .filter_map(|ground| ground.domain)
            .collect::<std::collections::BTreeSet<_>>();
        [
            ResearchShard::PriceMarketStructure,
            ResearchShard::Macro,
            ResearchShard::NewsEvent,
        ]
        .into_iter()
        .all(|domain| domains.contains(&domain))
    };

    if draft
        .forecasts
        .iter()
        .filter(|forecast| !forecast.is_neutral())
        .any(|forecast| !covered(forecast.asset, forecast.horizon))
    {
        return Err(DomainError::InsufficientDecisionEvidence);
    }

    Ok(())
}

/// Rust-bound decision. It carries no execution authority; the referenced
/// `DecisionContext` is the complete provenance and blocker surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub schema_version: u32,
    pub decision_context: ArtifactRef,
    pub summary: String,
    pub targets: TargetPortfolio,
    pub confidence_ppm: u32,
    pub forecasts: Vec<Forecast>,
    pub created_at: DateTime<Utc>,
}

impl Decision {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "decision.schema_version",
            });
        }
        if self.decision_context.kind != ArtifactKind::DecisionContext
            || self.summary.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "decision.context_or_summary",
            });
        }
        if self.confidence_ppm > 1_000_000 {
            return Err(DomainError::InvalidDecisionConfidence);
        }
        validate_forecasts(&self.forecasts)?;
        self.targets.validate_universe()
    }
}

fn validate_forecasts(forecasts: &[Forecast]) -> Result<(), DomainError> {
    let mut coverage = std::collections::BTreeSet::new();
    for forecast in forecasts {
        forecast.validate()?;
        if !coverage.insert((forecast.asset, forecast.horizon)) {
            return Err(DomainError::InvalidDecisionForecastHorizons);
        }
    }
    if coverage.len()
        != Asset::EXECUTABLE.len()
            * [
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]
            .len()
        || Asset::EXECUTABLE.into_iter().any(|asset| {
            [
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]
            .into_iter()
            .any(|horizon| !coverage.contains(&(asset, horizon)))
        })
    {
        return Err(DomainError::InvalidDecisionForecastHorizons);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{
        validate_decision_evidence_sufficiency, Decision, DecisionContext, DecisionDraft,
        DecisionHorizon, Forecast, HardBlocker, MaterialConflict, ResearchShard, SoftWarning,
    };
    use crate::{
        artifact::{ArtifactId, ArtifactKind, ArtifactRef},
        Asset, ClaimStance, ContentHash, DecisionId, DomainError, EvidenceGap, EvidenceGapImpact,
        EvidenceGround, EvidenceGroundRole, ResearchClaim, RunId, TargetPortfolio,
    };

    fn reference(kind: ArtifactKind, value: &[u8]) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(value)),
            kind,
        }
    }

    fn valid_forecasts() -> Vec<Forecast> {
        Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                [
                    DecisionHorizon::T1,
                    DecisionHorizon::T3,
                    DecisionHorizon::T5,
                ]
                .into_iter()
                .map(move |horizon| Forecast {
                    asset,
                    horizon,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 1,
                })
            })
            .collect()
    }

    #[test]
    fn material_conflict_must_link_claim_and_critique() {
        let conflict = MaterialConflict {
            claim: reference(ArtifactKind::Critique, b"claim"),
            critique: reference(ArtifactKind::Critique, b"critique"),
            topic: "inflation".to_owned(),
            rationale: "sources disagree".to_owned(),
        };

        assert!(conflict.validate().is_err());
    }

    #[test]
    fn hard_blocked_context_is_not_accepted() {
        let context = DecisionContext {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            decision_id: DecisionId::new(),
            run_id: RunId::new(),
            claims: vec![],
            critiques: vec![],
            evidence: vec![],
            policy_influences: vec![],
            applied_learning_refs: vec![],
            rejected_learning_refs: vec![],
            material_conflicts: vec![],
            hard_blockers: vec![HardBlocker::Frozen],
            soft_warnings: vec![],
            decision_policy_hash: ContentHash::of_bytes(b"fixture-policy"),
            target: TargetPortfolio::zeroed(),
            created_at: Utc::now(),
        };

        context.validate().unwrap();
        assert!(!context.accepted());
    }

    #[test]
    fn decision_context_accepts_only_learning_policy_influences() {
        let mut context = DecisionContext {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            decision_id: DecisionId::new(),
            run_id: RunId::new(),
            claims: vec![],
            critiques: vec![],
            evidence: vec![],
            policy_influences: vec![reference(ArtifactKind::Experience, b"experience")],
            applied_learning_refs: vec![],
            rejected_learning_refs: vec![],
            material_conflicts: vec![],
            hard_blockers: vec![HardBlocker::Frozen],
            soft_warnings: vec![],
            decision_policy_hash: ContentHash::of_bytes(b"fixture-policy"),
            target: TargetPortfolio::zeroed(),
            created_at: Utc::now(),
        };
        context.validate().unwrap();

        context.policy_influences = vec![reference(ArtifactKind::Claim, b"claim")];
        assert!(context.validate().is_err());
    }

    #[test]
    fn decision_draft_requires_each_typed_horizon_once() {
        let draft = DecisionDraft {
            summary: "evidence-backed draft".to_owned(),
            confidence_ppm: 500_000,
            forecasts: vec![
                Forecast {
                    asset: Asset::Tqqq,
                    horizon: DecisionHorizon::T1,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 1,
                },
                Forecast {
                    asset: Asset::Tqqq,
                    horizon: DecisionHorizon::T3,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 1,
                },
                Forecast {
                    asset: Asset::Tqqq,
                    horizon: DecisionHorizon::T3,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm: 1,
                },
            ],
            claims: vec![reference(ArtifactKind::Claim, b"claim")],
            critiques: vec![],
            evidence: vec![reference(ArtifactKind::NormalizedEvidence, b"evidence")],
            applied_learning_refs: vec![],
            rejected_learning_refs: vec![],
            material_conflicts: vec![],
            hard_blockers: vec![],
            soft_warnings: vec![],
        };

        assert_eq!(
            draft.validate(),
            Err(crate::DomainError::InvalidDecisionForecastHorizons)
        );
    }

    #[test]
    fn decision_binds_typed_context_and_three_horizons() {
        let decision = Decision {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            decision_context: reference(ArtifactKind::DecisionContext, b"context"),
            summary: "accepted decision".to_owned(),
            targets: TargetPortfolio::zeroed(),
            confidence_ppm: 500_000,
            forecasts: valid_forecasts(),
            created_at: Utc::now(),
        };

        decision.validate().unwrap();
    }

    fn forecast_set(expected_return_ppm: i64) -> Vec<Forecast> {
        Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                [
                    DecisionHorizon::T1,
                    DecisionHorizon::T3,
                    DecisionHorizon::T5,
                ]
                .into_iter()
                .map(move |horizon| Forecast {
                    asset,
                    horizon,
                    positive_return_probability_ppm: 500_000,
                    expected_return_ppm,
                })
            })
            .collect()
    }

    fn claim_for(asset: Asset, horizon: DecisionHorizon) -> ResearchClaim {
        ResearchClaim {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            topic: "direction".to_owned(),
            statement: "bounded directional evidence".to_owned(),
            horizon,
            stance: ClaimStance::Neutral,
            materiality_ppm: 500_000,
            confidence_ppm: 500_000,
            grounds: [
                (
                    b"directional-price".as_slice(),
                    ResearchShard::PriceMarketStructure,
                ),
                (b"directional-macro".as_slice(), ResearchShard::Macro),
                (b"directional-news".as_slice(), ResearchShard::NewsEvent),
            ]
            .into_iter()
            .map(|(value, domain)| EvidenceGround {
                evidence: reference(ArtifactKind::NormalizedEvidence, value),
                support: "scoped directional evidence".to_owned(),
                role: EvidenceGroundRole::Directional,
                assets: std::collections::BTreeSet::from([asset]),
                domain: Some(domain),
            })
            .collect(),
            evidence_gaps: vec![],
        }
    }

    fn draft_with(
        forecasts: Vec<Forecast>,
        hard_blockers: Vec<HardBlocker>,
        soft_warnings: Vec<SoftWarning>,
    ) -> DecisionDraft {
        DecisionDraft {
            summary: "bounded decision".to_owned(),
            confidence_ppm: 500_000,
            forecasts,
            claims: vec![reference(ArtifactKind::Claim, b"claim")],
            critiques: vec![],
            evidence: vec![reference(ArtifactKind::NormalizedEvidence, b"directional")],
            material_conflicts: vec![],
            hard_blockers,
            soft_warnings,
            applied_learning_refs: vec![],
            rejected_learning_refs: vec![],
        }
    }

    #[test]
    fn blocking_gap_requires_missing_evidence_and_neutral_forecasts() {
        let mut claim = claim_for(Asset::Tqqq, DecisionHorizon::T1);
        claim.evidence_gaps = vec![EvidenceGap {
            topic: "direction".to_owned(),
            rationale: "missing directional inputs".to_owned(),
            impact: EvidenceGapImpact::BlocksDirectionalForecast,
            supplemental_needs: vec![],
        }];

        let non_neutral = draft_with(
            forecast_set(1),
            vec![],
            vec![SoftWarning::IncompleteEvidence],
        );
        assert_eq!(
            validate_decision_evidence_sufficiency(&non_neutral, &[claim.clone()]),
            Err(DomainError::InsufficientDecisionEvidence)
        );

        let blocked = draft_with(
            forecast_set(0),
            vec![HardBlocker::MissingEvidence],
            vec![SoftWarning::IncompleteEvidence],
        );
        assert!(validate_decision_evidence_sufficiency(&blocked, &[claim]).is_ok());
    }

    #[test]
    fn non_neutral_forecasts_require_same_horizon_asset_coverage() {
        let claims = Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                [
                    DecisionHorizon::T1,
                    DecisionHorizon::T3,
                    DecisionHorizon::T5,
                ]
                .into_iter()
                .map(move |horizon| claim_for(asset, horizon))
            })
            .collect::<Vec<_>>();
        let draft = draft_with(forecast_set(1), vec![], vec![]);
        assert!(validate_decision_evidence_sufficiency(&draft, &claims).is_ok());

        let incomplete = &claims[..claims.len() - 1];
        assert_eq!(
            validate_decision_evidence_sufficiency(&draft, incomplete),
            Err(DomainError::InsufficientDecisionEvidence)
        );
    }

    #[test]
    fn neutral_forecasts_without_coverage_do_not_block_non_neutral_evidence() {
        let mut forecasts = forecast_set(0);
        forecasts
            .iter_mut()
            .find(|forecast| {
                forecast.asset == Asset::Tqqq && forecast.horizon == DecisionHorizon::T1
            })
            .expect("forecast set includes TQQQ/t1")
            .expected_return_ppm = 1;
        let draft = draft_with(forecasts, vec![], vec![]);
        let claim = claim_for(Asset::Tqqq, DecisionHorizon::T1);

        assert!(validate_decision_evidence_sufficiency(&draft, &[claim]).is_ok());
    }

    #[test]
    fn any_claim_gap_requires_incomplete_evidence_warning() {
        let mut claim = claim_for(Asset::Tqqq, DecisionHorizon::T1);
        claim.evidence_gaps = vec![EvidenceGap {
            topic: "governance".to_owned(),
            rationale: "risk rules are not in context".to_owned(),
            impact: EvidenceGapImpact::Warning,
            supplemental_needs: vec![],
        }];
        let draft = draft_with(forecast_set(0), vec![], vec![]);
        assert_eq!(
            validate_decision_evidence_sufficiency(&draft, &[claim]),
            Err(DomainError::InsufficientDecisionEvidence)
        );
    }
}
