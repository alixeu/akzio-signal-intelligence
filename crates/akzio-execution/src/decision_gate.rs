//! Typed v2 DecisionGate.
//!
//! The model produces only a schema-bounded `DecisionDraft`. Rust reloads the
//! persisted manifest closure, binds the draft to the run, and atomically
//! commits the resulting `DecisionContext` and `Decision`.

use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    content_hash_json, validate_decision_evidence_sufficiency, Artifact, ArtifactId, ArtifactKind,
    ArtifactLifecycle, ArtifactRef, Asset, CandidatePolicy, ContextManifestPayload, Decision,
    DecisionContext, DecisionDraft, DecisionHorizon, DomainError, Experience, Forecast,
    HardBlocker, PolicySubject, ResearchClaim, RunPurpose, TargetPortfolio, TaskStatus,
    TaskWritePermit, WeightPpm, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoreError, V2Store};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(test)]
use akzio_domain::{ArtifactOrigin, ArtifactProvenance};

#[derive(Debug, Error)]
pub enum DecisionGateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("decision proposal provenance is invalid")]
    InvalidProposalProvenance,
    #[error("decision proposal claim evidence is semantically insufficient")]
    InsufficientClaimEvidence,
    #[error(
        "decision proposal producer contract is not installed or predates evidence sufficiency"
    )]
    UnsupportedProposalContract,
    #[error("decision proposal must retain exactly one ContextManifest")]
    InvalidManifestReference,
    #[error("decision ContextManifest closure is invalid")]
    InvalidManifestClosure,
    #[error("decision proposal reference {0} is outside its ContextManifest")]
    ReferenceOutsideManifest(ArtifactId),
    #[error("policy influence {0} is not eligible")]
    InvalidPolicyInfluence(ArtifactId),
    #[error("learning artifact {0} was selected but not explicitly applied or rejected")]
    MissingLearningAttribution(ArtifactId),
}

pub type DecisionGateResult<T> = std::result::Result<T, DecisionGateError>;

#[derive(Debug, Clone)]
pub struct DecisionGateInput {
    pub permit: TaskWritePermit,
    pub proposal: ArtifactRef,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct DecisionGateOutput {
    pub decision_context: Artifact,
    pub decision: Artifact,
}

/// Rust-owned conversion from schema-bounded forecasts to target exposure.
///
/// The synthesizer can only supply forecasts and confidence. This policy is
/// configured by Rust, hashed into every DecisionContext, and is the sole
/// authority that creates portfolio weights.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPolicy {
    pub min_confidence_ppm: u32,
    pub max_gross_weight: WeightPpm,
    pub horizon_weights: BTreeMap<DecisionHorizon, WeightPpm>,
}

impl DecisionPolicy {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.min_confidence_ppm > WeightPpm::SCALE
            || self.max_gross_weight.0 > WeightPpm::SCALE
            || self.horizon_weights.len() != 3
            || [
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]
            .into_iter()
            .any(|horizon| !self.horizon_weights.contains_key(&horizon))
            || self
                .horizon_weights
                .values()
                .any(|weight| weight.0 > WeightPpm::SCALE)
            || self
                .horizon_weights
                .values()
                .try_fold(0_u32, |sum, weight| sum.checked_add(weight.0))
                != Some(WeightPpm::SCALE)
        {
            return Err(DomainError::InvalidBudget {
                field: "decision_policy",
            });
        }
        Ok(())
    }

    pub fn policy_hash(&self) -> Result<akzio_domain::ContentHash, DomainError> {
        self.validate()?;
        content_hash_json(&serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?)
            .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn target_for(
        &self,
        confidence_ppm: u32,
        forecasts: &[Forecast],
    ) -> Result<TargetPortfolio, DomainError> {
        self.validate()?;
        if confidence_ppm > WeightPpm::SCALE {
            return Err(DomainError::InvalidDecisionConfidence);
        }
        if confidence_ppm < self.min_confidence_ppm {
            return Ok(TargetPortfolio::zeroed());
        }

        let mut scores = BTreeMap::new();
        for asset in Asset::EXECUTABLE {
            let score = forecasts
                .iter()
                .filter(|forecast| forecast.asset == asset)
                .try_fold(0_i128, |total, forecast| {
                    let weight = i128::from(self.horizon_weights[&forecast.horizon].0);
                    let probability_signal = i128::from(forecast.positive_return_probability_ppm)
                        .saturating_mul(2)
                        .saturating_sub(i128::from(WeightPpm::SCALE));
                    let return_signal = i128::from(forecast.expected_return_ppm)
                        .clamp(-i128::from(WeightPpm::SCALE), i128::from(WeightPpm::SCALE));
                    Ok::<_, DomainError>(
                        total.saturating_add(
                            probability_signal
                                .saturating_add(return_signal)
                                .saturating_mul(weight)
                                / i128::from(WeightPpm::SCALE),
                        ),
                    )
                })?;
            scores.insert(asset, score.max(0));
        }

        let total_score = scores.values().copied().sum::<i128>();
        let strongest_signal = scores.values().copied().max().unwrap_or_default();
        if total_score == 0 || strongest_signal == 0 {
            return Ok(TargetPortfolio::zeroed());
        }

        let confidence_scale = if self.min_confidence_ppm == WeightPpm::SCALE {
            WeightPpm::SCALE
        } else {
            (u64::from(confidence_ppm - self.min_confidence_ppm) * u64::from(WeightPpm::SCALE)
                / u64::from(WeightPpm::SCALE - self.min_confidence_ppm)) as u32
        };
        let signal_scale = strongest_signal.min(i128::from(WeightPpm::SCALE));
        let gross = i128::from(self.max_gross_weight.0)
            .saturating_mul(i128::from(confidence_scale))
            .saturating_mul(signal_scale)
            / i128::from(WeightPpm::SCALE)
            / i128::from(WeightPpm::SCALE);
        let mut target = TargetPortfolio::zeroed();
        for asset in Asset::EXECUTABLE {
            let weight = gross.saturating_mul(scores[&asset]) / total_score;
            target.weights.insert(
                asset,
                WeightPpm(
                    u32::try_from(weight).map_err(|_| DomainError::InvalidBudget {
                        field: "decision_policy.target",
                    })?,
                ),
            );
        }
        target.validate_universe()?;
        Ok(target)
    }
}

impl Default for DecisionPolicy {
    fn default() -> Self {
        Self {
            min_confidence_ppm: 250_000,
            max_gross_weight: WeightPpm(500_000),
            horizon_weights: BTreeMap::from([
                (DecisionHorizon::T1, WeightPpm(333_333)),
                (DecisionHorizon::T3, WeightPpm(333_333)),
                (DecisionHorizon::T5, WeightPpm(333_334)),
            ]),
        }
    }
}

#[derive(Debug, Clone)]
pub struct V2DecisionRuntime {
    store: V2Store,
    policy: DecisionPolicy,
}
include!("decision_gate_parts/decide.rs");
include!("decision_gate_parts/validate.rs");
include!("decision_gate_parts/commit.rs");
include!("decision_gate_parts/helpers.rs");
#[cfg(test)]
#[path = "decision_gate/tests.rs"]
mod tests;
