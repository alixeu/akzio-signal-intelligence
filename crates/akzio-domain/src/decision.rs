//! Typed decision inputs and risk findings.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    Asset, ContentHash, DecisionId, DomainError, RunId, TargetPortfolio, V2_DOMAIN_SCHEMA_VERSION,
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
}

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
        Decision, DecisionContext, DecisionDraft, DecisionHorizon, Forecast, HardBlocker,
        MaterialConflict,
    };
    use crate::{
        artifact::{ArtifactId, ArtifactKind, ArtifactRef},
        Asset, ContentHash, DecisionId, RunId, TargetPortfolio,
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
}
