//! Typed, evidence-bound research artifacts.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactKind, ArtifactRef, Asset, DecisionHorizon, DomainError, EvidenceNeed,
    V2_DOMAIN_SCHEMA_VERSION,
};

pub const MAX_EVIDENCE_GAPS: usize = 2;
pub const STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID: &str = "candidate.structured_critique";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStance {
    Bullish,
    Bearish,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CritiqueSeverity {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceGroundRole {
    #[default]
    Descriptive,
    Directional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceGapImpact {
    #[default]
    Warning,
    BlocksDirectionalForecast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionDisposition {
    Accepted,
    Rebutted,
    Unresolved,
}

/// Rust-owned Planner research lanes. A Planner may select a bounded subset
/// of these lanes, but it cannot invent a new source family or lane at run
/// time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchShard {
    PriceMarketStructure,
    Macro,
    FundamentalsSemiconductor,
    NewsEvent,
}

/// Rust-owned research request. It is lowered to an `EvidenceNeed` before a
/// workflow is installed; models may propose values but cannot widen them at
/// execution time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchIntent {
    pub schema_version: u32,
    pub source_family: String,
    pub resource: String,
    pub query: String,
    pub assets: BTreeSet<Asset>,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub max_age_secs: u64,
    pub max_results: u16,
}

impl ResearchIntent {
    pub fn shard(&self) -> ResearchShard {
        match self.source_family.as_str() {
            "alpaca" => ResearchShard::PriceMarketStructure,
            "fred" => ResearchShard::Macro,
            "sec_edgar" => ResearchShard::FundamentalsSemiconductor,
            _ => ResearchShard::NewsEvent,
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.source_family.trim().is_empty()
            || self.resource.trim().is_empty()
            || self.query.trim().is_empty()
            || self.resource.chars().count() > 2_048
            || self.query.chars().count() > 2_000
            || !(1..=86_400 * 7).contains(&self.max_age_secs)
            || !(1..=32).contains(&self.max_results)
        {
            return Err(DomainError::EmptyField {
                field: "research.intent",
            });
        }
        if !matches!(
            self.source_family.as_str(),
            "alpaca" | "sec_edgar" | "fred" | "news_web"
        ) {
            return Err(DomainError::EvidenceSourceNotAllowed(
                self.source_family.clone(),
            ));
        }
        if let (Some(start), Some(end)) = (self.window_start, self.window_end) {
            if end < start || end.signed_duration_since(start) > Duration::days(366) {
                return Err(DomainError::InvalidBudget {
                    field: "research.intent.window",
                });
            }
        }
        Ok(())
    }

    pub fn evidence_need(&self) -> Result<EvidenceNeed, DomainError> {
        self.validate()?;
        Ok(EvidenceNeed {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            source_family: self.source_family.clone(),
            resource: self.resource.clone(),
            max_age_secs: self.max_age_secs,
        })
    }
}

/// A concrete statement of support attached to one governed evidence artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceGround {
    pub evidence: ArtifactRef,
    pub support: String,
    #[serde(default)]
    pub role: EvidenceGroundRole,
    #[serde(default)]
    pub assets: BTreeSet<Asset>,
    #[serde(default)]
    pub domain: Option<ResearchShard>,
}

impl EvidenceGround {
    pub fn validate(&self) -> Result<(), DomainError> {
        if !matches!(
            self.evidence.kind,
            ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
        ) || self.support.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "research.ground",
            });
        }
        if self.role == EvidenceGroundRole::Directional && self.assets.is_empty() {
            return Err(DomainError::InvalidEvidenceGroundScope);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceGap {
    pub topic: String,
    pub rationale: String,
    #[serde(default)]
    pub impact: EvidenceGapImpact,
    #[serde(default)]
    pub supplemental_needs: Vec<ResearchIntent>,
}

impl EvidenceGap {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.topic.trim().is_empty() || self.rationale.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "research.evidence_gap",
            });
        }
        if self.supplemental_needs.len() > 8 {
            return Err(DomainError::InvalidBudget {
                field: "research.evidence_gap.supplemental_needs",
            });
        }
        for need in &self.supplemental_needs {
            need.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchClaim {
    pub schema_version: u32,
    pub topic: String,
    pub statement: String,
    pub horizon: DecisionHorizon,
    pub stance: ClaimStance,
    pub materiality_ppm: u32,
    pub confidence_ppm: u32,
    pub grounds: Vec<EvidenceGround>,
    pub evidence_gaps: Vec<EvidenceGap>,
}

impl ResearchClaim {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_research_identity(self.schema_version, &self.topic, &self.statement)?;
        validate_ppm(self.materiality_ppm, "research.claim.materiality_ppm")?;
        validate_ppm(self.confidence_ppm, "research.claim.confidence_ppm")?;
        validate_grounds(&self.grounds)?;
        validate_gaps(&self.evidence_gaps)
    }

    pub fn source_refs(&self) -> Vec<ArtifactRef> {
        ground_refs(&self.grounds)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchCritique {
    pub schema_version: u32,
    pub target: ArtifactRef,
    pub topic: String,
    pub severity: CritiqueSeverity,
    pub blocker: bool,
    pub rationale: String,
    pub grounds: Vec<EvidenceGround>,
    pub evidence_gaps: Vec<EvidenceGap>,
}

impl ResearchCritique {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.target.kind != ArtifactKind::Claim
            || self.topic.trim().is_empty()
            || self.rationale.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "research.critique",
            });
        }
        if self.grounds.is_empty() && self.evidence_gaps.is_empty() {
            return Err(DomainError::EmptyField {
                field: "research.critique.grounds_or_gaps",
            });
        }
        if !self.grounds.is_empty() {
            validate_grounds(&self.grounds)?;
        }
        validate_gaps(&self.evidence_gaps)
    }

    pub fn source_refs(&self) -> Vec<ArtifactRef> {
        let mut refs = BTreeSet::from([self.target.clone()]);
        refs.extend(ground_refs(&self.grounds));
        refs.into_iter().collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchResolution {
    pub schema_version: u32,
    pub claim: ArtifactRef,
    pub critique: ArtifactRef,
    pub disposition: ResolutionDisposition,
    pub rationale: String,
    pub grounds: Vec<EvidenceGround>,
    pub remaining_gaps: Vec<EvidenceGap>,
}

impl ResearchResolution {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.claim.kind != ArtifactKind::Claim
            || self.critique.kind != ArtifactKind::Critique
            || self.rationale.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "research.resolution",
            });
        }
        validate_grounds(&self.grounds)?;
        validate_gaps(&self.remaining_gaps)
    }

    pub fn source_refs(&self) -> Vec<ArtifactRef> {
        let mut refs = BTreeSet::from([self.claim.clone(), self.critique.clone()]);
        refs.extend(ground_refs(&self.grounds));
        refs.into_iter().collect()
    }
}

fn validate_research_identity(
    schema_version: u32,
    topic: &str,
    statement: &str,
) -> Result<(), DomainError> {
    if schema_version != V2_DOMAIN_SCHEMA_VERSION
        || topic.trim().is_empty()
        || statement.trim().is_empty()
    {
        return Err(DomainError::EmptyField {
            field: "research.claim",
        });
    }
    Ok(())
}

fn validate_ppm(value: u32, field: &'static str) -> Result<(), DomainError> {
    if value > 1_000_000 {
        return Err(DomainError::InvalidBudget { field });
    }
    Ok(())
}

fn validate_grounds(grounds: &[EvidenceGround]) -> Result<(), DomainError> {
    if grounds.is_empty() {
        return Err(DomainError::EmptyField {
            field: "research.grounds",
        });
    }
    let mut evidence = BTreeSet::new();
    for ground in grounds {
        ground.validate()?;
        if !evidence.insert(ground.evidence.clone()) {
            return Err(DomainError::EmptyField {
                field: "research.grounds",
            });
        }
    }
    Ok(())
}

fn validate_gaps(gaps: &[EvidenceGap]) -> Result<(), DomainError> {
    if gaps.len() > MAX_EVIDENCE_GAPS {
        return Err(DomainError::InvalidBudget {
            field: "research.evidence_gaps",
        });
    }
    for gap in gaps {
        gap.validate()?;
    }
    Ok(())
}

fn ground_refs(grounds: &[EvidenceGround]) -> Vec<ArtifactRef> {
    grounds
        .iter()
        .map(|ground| ground.evidence.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArtifactId, ContentHash};

    fn reference(kind: ArtifactKind, value: &[u8]) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(value)),
            kind,
        }
    }

    fn ground() -> EvidenceGround {
        EvidenceGround {
            evidence: reference(ArtifactKind::NormalizedEvidence, b"evidence"),
            role: EvidenceGroundRole::Descriptive,
            assets: BTreeSet::new(),
            domain: None,
            support: "reported price and date support the claim".to_owned(),
        }
    }

    #[test]
    fn claim_requires_governed_evidence_and_bounded_ppm() {
        let claim = ResearchClaim {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topic: "market_regime".to_owned(),
            statement: "Trend remains positive at the stated horizon.".to_owned(),
            horizon: DecisionHorizon::T5,
            stance: ClaimStance::Bullish,
            materiality_ppm: 800_000,
            confidence_ppm: 700_000,
            grounds: vec![ground()],
            evidence_gaps: vec![],
        };
        claim.validate().unwrap();
        assert_eq!(claim.source_refs(), vec![ground().evidence]);

        let mut invalid = claim;
        invalid.grounds[0].evidence.kind = ArtifactKind::Claim;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn critique_and_resolution_close_over_claim_and_evidence() {
        let critique = ResearchCritique {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            target: reference(ArtifactKind::Claim, b"claim"),
            topic: "market_regime".to_owned(),
            severity: CritiqueSeverity::High,
            blocker: true,
            rationale: "The cited series is stale for the requested horizon.".to_owned(),
            grounds: vec![ground()],
            evidence_gaps: vec![],
        };
        critique.validate().unwrap();

        let resolution = ResearchResolution {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            claim: critique.target,
            critique: reference(ArtifactKind::Critique, b"critique"),
            disposition: ResolutionDisposition::Unresolved,
            rationale: "Fresh evidence is required before the conflict can close.".to_owned(),
            grounds: vec![ground()],
            remaining_gaps: vec![EvidenceGap {
                topic: "freshness".to_owned(),
                rationale: "No current session observation is available.".to_owned(),
                impact: EvidenceGapImpact::Warning,
                supplemental_needs: vec![],
            }],
        };
        resolution.validate().unwrap();
        assert!(resolution
            .source_refs()
            .iter()
            .any(|reference| reference.kind == ArtifactKind::Critique));
    }

    #[test]
    fn research_intent_is_bounded_before_lowering_to_evidence_need() {
        let intent = ResearchIntent {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source_family: "fred".to_owned(),
            resource: "series:DFII10".to_owned(),
            query: "real yield regime".to_owned(),
            assets: BTreeSet::new(),
            window_start: None,
            window_end: None,
            max_age_secs: 86_400,
            max_results: 4,
        };
        let need = intent.evidence_need().unwrap();
        assert_eq!(need.source_family, "fred");
        assert_eq!(need.resource, "series:DFII10");

        let mut invalid = intent;
        invalid.source_family = "arbitrary_http".to_owned();
        assert!(invalid.validate().is_err());
    }
}
