//! Typed, evidence-bound research artifacts.

use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactKind, ArtifactRef, Asset, DecisionHorizon, DomainError, EvidenceNeed,
    DOMAIN_SCHEMA_VERSION,
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
pub enum ClaimVerificationStatus {
    Supported,
    Contradicted,
    #[default]
    NotEnoughInformation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SourceAuthority {
    Primary,
    Official,
    EstablishedSecondary,
    #[default]
    Unrated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TemporalValidity {
    ValidAtDecisionCutoff,
    Stale,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimVerificationEvidence {
    pub evidence: ArtifactRef,
    #[serde(default)]
    pub authority: SourceAuthority,
    #[serde(default)]
    pub temporal_validity: TemporalValidity,
}

impl ClaimVerificationEvidence {
    pub fn validate(&self) -> Result<(), DomainError> {
        if !matches!(
            self.evidence.kind,
            ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
        ) {
            return Err(DomainError::EmptyField {
                field: "research.claim_verification.evidence",
            });
        }
        Ok(())
    }

    pub fn is_current_authoritative(&self) -> bool {
        self.authority != SourceAuthority::Unrated
            && self.temporal_validity == TemporalValidity::ValidAtDecisionCutoff
    }
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
        if self.schema_version != DOMAIN_SCHEMA_VERSION
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
            schema_version: crate::DOMAIN_SCHEMA_VERSION,
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
    /// Empty assets means all assets; empty horizons inherits the Claim horizon.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub assets: BTreeSet<Asset>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub horizons: BTreeSet<DecisionHorizon>,
    #[serde(default)]
    pub supplemental_needs: Vec<ResearchIntent>,
}

impl EvidenceGap {
    pub fn blocks_slot(
        &self,
        asset: Asset,
        horizon: DecisionHorizon,
        claim_horizon: DecisionHorizon,
    ) -> bool {
        self.impact == EvidenceGapImpact::BlocksDirectionalForecast
            && (self.assets.is_empty() || self.assets.contains(&asset))
            && if self.horizons.is_empty() {
                horizon == claim_horizon
            } else {
                self.horizons.contains(&horizon)
            }
    }
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
    #[serde(default)]
    pub verification_status: ClaimVerificationStatus,
    #[serde(default)]
    pub supporting_refs: Vec<ClaimVerificationEvidence>,
    #[serde(default)]
    pub conflicting_refs: Vec<ClaimVerificationEvidence>,
}

impl ResearchCritique {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
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
        validate_gaps(&self.evidence_gaps)?;
        validate_verification_refs(&self.supporting_refs)?;
        validate_verification_refs(&self.conflicting_refs)?;
        let ground_refs = self
            .grounds
            .iter()
            .map(|ground| &ground.evidence)
            .collect::<BTreeSet<_>>();
        if self
            .supporting_refs
            .iter()
            .chain(self.conflicting_refs.iter())
            .any(|reference| !ground_refs.contains(&reference.evidence))
        {
            return Err(DomainError::EmptyField {
                field: "research.claim_verification.ground_closure",
            });
        }
        match self.verification_status {
            ClaimVerificationStatus::Supported
                if self.supporting_refs.is_empty()
                    || !self.conflicting_refs.is_empty()
                    || self
                        .supporting_refs
                        .iter()
                        .any(|reference| !reference.is_current_authoritative()) =>
            {
                Err(DomainError::EmptyField {
                    field: "research.claim_verification.supported",
                })
            }
            ClaimVerificationStatus::Contradicted if self.conflicting_refs.is_empty() => {
                Err(DomainError::EmptyField {
                    field: "research.claim_verification.contradicted",
                })
            }
            _ => Ok(()),
        }
    }

    pub fn source_refs(&self) -> Vec<ArtifactRef> {
        let mut refs = BTreeSet::from([self.target.clone()]);
        refs.extend(ground_refs(&self.grounds));
        refs.extend(
            self.supporting_refs
                .iter()
                .chain(self.conflicting_refs.iter())
                .map(|reference| reference.evidence.clone()),
        );
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
        if self.schema_version != DOMAIN_SCHEMA_VERSION
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
    if schema_version != DOMAIN_SCHEMA_VERSION
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

fn validate_verification_refs(references: &[ClaimVerificationEvidence]) -> Result<(), DomainError> {
    let mut seen = BTreeSet::new();
    for reference in references {
        reference.validate()?;
        if !seen.insert(reference.evidence.clone()) {
            return Err(DomainError::EmptyField {
                field: "research.claim_verification.references",
            });
        }
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
