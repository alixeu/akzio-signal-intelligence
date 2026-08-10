//! Outcome-backed learning vocabulary.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactKind, ArtifactRef},
    ids::{EvaluationId, ExperienceId, OutcomeId, PolicyTransitionId},
    ContentHash, DomainError, TopologyId, V2_DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeHorizon {
    T1,
    T3,
    T5,
}

impl OutcomeHorizon {
    pub const fn trading_days(self) -> u8 {
        match self {
            Self::T1 => 1,
            Self::T3 => 3,
            Self::T5 => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeWindow {
    pub horizon: OutcomeHorizon,
    pub portfolio_return_ppm: i64,
    pub benchmark_return_ppm: i64,
    pub utility_ppm: i64,
    pub calibration_ppm: u32,
    pub evidence_completeness_ppm: u32,
    pub risk_recall_ppm: u32,
}

impl OutcomeWindow {
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.calibration_ppm,
            self.evidence_completeness_ppm,
            self.risk_recall_ppm,
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "outcome_window.ppm",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub execution_context: ArtifactRef,
    pub market_evidence: Vec<ArtifactRef>,
    pub windows: Vec<OutcomeWindow>,
    pub sealed_at: Option<DateTime<Utc>>,
}

impl Outcome {
    pub fn is_sealed(&self) -> bool {
        self.sealed_at.is_some()
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.outcome_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "outcome.identity",
            });
        }
        if self.execution_context.kind != ArtifactKind::ExecutionContext
            || self.market_evidence.is_empty()
            || self.market_evidence.iter().any(|evidence| {
                !matches!(
                    evidence.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                )
            })
        {
            return Err(DomainError::EmptyField {
                field: "outcome.references",
            });
        }
        let mut expected = [false; 3];
        for window in &self.windows {
            window.validate()?;
            let index = match window.horizon {
                OutcomeHorizon::T1 => 0,
                OutcomeHorizon::T3 => 1,
                OutcomeHorizon::T5 => 2,
            };
            if expected[index] {
                return Err(DomainError::InvalidBudget {
                    field: "outcome.windows",
                });
            }
            expected[index] = true;
        }
        if !expected.into_iter().all(|present| present) {
            return Err(DomainError::InvalidBudget {
                field: "outcome.windows",
            });
        }
        Ok(())
    }

    pub fn validate_sealed(&self) -> Result<(), DomainError> {
        self.validate()?;
        self.sealed_at.ok_or(DomainError::EmptyField {
            field: "outcome.sealed_at",
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryLifecycle {
    Candidate,
    Active,
    Proven,
    Contested,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidatePolicyState {
    Candidate,
    Canary10,
    Canary25,
    Canary50,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "state")]
pub enum PolicyState {
    Memory(MemoryLifecycle),
    Contract(CandidatePolicyState),
    Topology(CandidatePolicyState),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Experience {
    pub schema_version: u32,
    pub experience_id: ExperienceId,
    pub hypothesis_id: String,
    pub decision: ArtifactRef,
    pub decision_context: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub policy_verdict: ArtifactRef,
    pub outcome: ArtifactRef,
    pub contract_hash: ContentHash,
    pub topology_id: TopologyId,
    pub lifecycle: MemoryLifecycle,
    pub created_at: DateTime<Utc>,
}

impl Experience {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.experience_id.0.trim().is_empty()
            || self.hypothesis_id.trim().is_empty()
            || self.topology_id.0.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "experience.identity",
            });
        }
        if self.decision.kind != ArtifactKind::Decision
            || self.decision_context.kind != ArtifactKind::DecisionContext
            || self.execution_context.kind != ArtifactKind::ExecutionContext
            || self.policy_verdict.kind != ArtifactKind::ExecutionVerdict
            || self.outcome.kind != ArtifactKind::Outcome
        {
            return Err(DomainError::EmptyField {
                field: "experience.references",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evaluation {
    pub schema_version: u32,
    pub evaluation_id: EvaluationId,
    pub outcome: ArtifactRef,
    pub experience: ArtifactRef,
    pub marginal_utility_ppm: i64,
    pub token_cost: u64,
    pub latency_millis: u64,
    pub created_at: DateTime<Utc>,
}

impl Evaluation {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.evaluation_id.0.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "evaluation.identity",
            });
        }
        if self.outcome.kind != ArtifactKind::Outcome
            || self.experience.kind != ArtifactKind::Experience
        {
            return Err(DomainError::EmptyField {
                field: "evaluation.references",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyTransition {
    pub schema_version: u32,
    pub transition_id: PolicyTransitionId,
    pub subject_id: String,
    pub from: PolicyState,
    pub to: PolicyState,
    pub evaluation: ArtifactRef,
    pub created_at: DateTime<Utc>,
}

impl PolicyTransition {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.transition_id.0.trim().is_empty()
            || self.subject_id.trim().is_empty()
            || self.from == self.to
            || self.evaluation.kind != ArtifactKind::Evaluation
        {
            return Err(DomainError::EmptyField {
                field: "policy_transition",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::{Outcome, OutcomeHorizon, OutcomeWindow};
    use crate::{
        artifact::{ArtifactId, ArtifactKind, ArtifactRef},
        ids::OutcomeId,
        ContentHash,
    };

    fn reference(kind: ArtifactKind, value: &[u8]) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(value)),
            kind,
        }
    }

    fn window(horizon: OutcomeHorizon) -> OutcomeWindow {
        OutcomeWindow {
            horizon,
            portfolio_return_ppm: 1,
            benchmark_return_ppm: 0,
            utility_ppm: 1,
            calibration_ppm: 1,
            evidence_completeness_ppm: 1_000_000,
            risk_recall_ppm: 1_000_000,
        }
    }

    #[test]
    fn learning_requires_a_sealed_complete_outcome() {
        let outcome = Outcome {
            schema_version: crate::V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: OutcomeId::new(),
            execution_context: reference(ArtifactKind::ExecutionContext, b"execution"),
            market_evidence: vec![reference(ArtifactKind::NormalizedEvidence, b"market")],
            windows: vec![
                window(OutcomeHorizon::T1),
                window(OutcomeHorizon::T3),
                window(OutcomeHorizon::T5),
            ],
            sealed_at: None,
        };

        outcome.validate().unwrap();
        assert!(outcome.validate_sealed().is_err());

        let sealed = Outcome {
            sealed_at: Some(Utc::now()),
            ..outcome
        };
        sealed.validate_sealed().unwrap();
    }
}
