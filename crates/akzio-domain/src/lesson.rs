//! Reusable learning statements, kept separate from outcome-backed experiences.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactKind, ArtifactRef, Asset, ContentHash, DecisionHorizon, DomainError, LessonId,
    V2_DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LessonOrigin {
    Operator,
    OutcomeDerived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LessonLifecycle {
    Draft,
    Active,
    Contested,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LessonScope {
    #[serde(default)]
    pub assets: BTreeSet<Asset>,
    #[serde(default)]
    pub horizons: BTreeSet<DecisionHorizon>,
    #[serde(default)]
    pub regimes: BTreeSet<String>,
    #[serde(default)]
    pub decision_stages: BTreeSet<String>,
}

impl LessonScope {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.regimes.iter().any(|value| value.trim().is_empty())
            || self
                .decision_stages
                .iter()
                .any(|value| value.trim().is_empty())
        {
            return Err(DomainError::EmptyField {
                field: "lesson.scope",
            });
        }
        if self.regimes.len() > 16 || self.decision_stages.len() > 8 {
            return Err(DomainError::InvalidBudget {
                field: "lesson.scope",
            });
        }
        Ok(())
    }

    pub fn matches(
        &self,
        assets: &BTreeSet<Asset>,
        horizons: &BTreeSet<DecisionHorizon>,
        regimes: &BTreeSet<String>,
        decision_stages: &BTreeSet<String>,
    ) -> bool {
        (self.assets.is_empty()
            || assets.is_empty()
            || self.assets.iter().any(|asset| assets.contains(asset)))
            && (self.horizons.is_empty()
                || horizons.is_empty()
                || self
                    .horizons
                    .iter()
                    .any(|horizon| horizons.contains(horizon)))
            && (self.regimes.is_empty()
                || regimes.is_empty()
                || self.regimes.iter().any(|regime| regimes.contains(regime)))
            && (self.decision_stages.is_empty()
                || decision_stages.is_empty()
                || self
                    .decision_stages
                    .iter()
                    .any(|stage| decision_stages.contains(stage)))
    }
}

/// A reusable, auditable statement. It is not a Paper experience and cannot
/// grant execution authority; it becomes eligible for model context only
/// through the ContextManifest and its lifecycle/policy gates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lesson {
    pub schema_version: u32,
    pub lesson_id: LessonId,
    pub origin: LessonOrigin,
    pub lifecycle: LessonLifecycle,
    pub title: String,
    pub statement: String,
    pub rationale: String,
    pub recommended_behavior: String,
    #[serde(default)]
    pub exclusions: Vec<String>,
    pub scope: LessonScope,
    pub source_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub supersedes: Vec<ArtifactRef>,
    #[serde(default)]
    pub conflicts_with: Vec<ArtifactRef>,
    pub confidence_ppm: u32,
    pub authored_by: Option<String>,
    pub approved_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Lesson {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.lesson_id.0.trim().is_empty()
            || self.title.trim().is_empty()
            || self.statement.trim().is_empty()
            || self.rationale.trim().is_empty()
            || self.recommended_behavior.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "lesson.identity_or_content",
            });
        }
        if self.confidence_ppm > 1_000_000
            || self.source_refs.is_empty()
            || self.source_refs.len() > 16
            || self.exclusions.len() > 16
            || self.supersedes.len() > 8
            || self.conflicts_with.len() > 8
            || self.exclusions.iter().any(|value| value.trim().is_empty())
        {
            return Err(DomainError::InvalidBudget {
                field: "lesson.bounds",
            });
        }
        self.scope.validate()?;
        if self
            .source_refs
            .iter()
            .any(|reference| reference.kind == ArtifactKind::Lesson)
        {
            return Err(DomainError::EmptyField {
                field: "lesson.source_refs",
            });
        }
        for reference in self.supersedes.iter().chain(self.conflicts_with.iter()) {
            if reference.kind != ArtifactKind::Lesson {
                return Err(DomainError::EmptyField {
                    field: "lesson.related_refs",
                });
            }
        }
        match self.origin {
            LessonOrigin::Operator if self.authored_by.as_deref().is_none_or(str::is_empty) => {
                return Err(DomainError::EmptyField {
                    field: "lesson.authored_by",
                });
            }
            LessonOrigin::OutcomeDerived if self.source_refs.is_empty() => {
                return Err(DomainError::EmptyField {
                    field: "lesson.source_refs",
                });
            }
            _ => {}
        }
        if matches!(
            self.lifecycle,
            LessonLifecycle::Active | LessonLifecycle::Contested
        ) && self.approved_by.as_deref().is_none_or(str::is_empty)
        {
            return Err(DomainError::EmptyField {
                field: "lesson.approved_by",
            });
        }
        Ok(())
    }
}

/// Whether the model applied or rejected a Lesson for one decision.
///
/// Sourced from `DecisionContext::applied_learning_refs` /
/// `rejected_learning_refs`, which the decision gate already forces the model to
/// populate for every Lesson in its manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LessonAttribution {
    Applied,
    Rejected,
}

impl LessonAttribution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Rejected => "rejected",
        }
    }
}

/// One immutable, purely **observational** record linking a Lesson revision to
/// the sealed outcome of a decision that cited it.
///
/// This is an evidence log, not a causal claim. A negative `utility_ppm` after a
/// Lesson was applied does not show the Lesson caused it: market regime, the
/// other injected Lessons, the contract, the topology, the target weights,
/// execution cost and evidence quality all move the same number, and the outcome
/// after a *rejected* Lesson is not a counterfactual either. Establishing effect
/// requires a paired on/off comparison against the same outcome, so nothing here
/// may drive an automatic contest or retire on its own.
///
/// Keyed on the stable `lesson_id`, never on `lesson_artifact`: every lifecycle
/// transition writes a fresh Lesson artifact, so keying on the artifact would
/// orphan all prior evidence from the current head. `lesson_artifact` is retained
/// alongside it to record which revision was actually injected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LessonEvidence {
    pub schema_version: u32,
    pub lesson_id: LessonId,
    /// The exact Lesson revision present in the decision's context manifest.
    pub lesson_artifact: ArtifactRef,
    pub decision_context: ArtifactRef,
    pub outcome: ArtifactRef,
    pub attribution: LessonAttribution,
    /// Sealed `utility_ppm` for T+1/T+3/T+5, in `OutcomeHorizon::ALL` order.
    pub utility_ppm_by_horizon: [i64; 3],
    /// Sealed calibration quality for the same horizons; `None` where the window
    /// carried no forecast to score.
    pub calibration_ppm_by_horizon: [Option<u32>; 3],
    pub recorded_at: DateTime<Utc>,
}

impl LessonEvidence {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.lesson_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "lesson_evidence.identity",
            });
        }
        if self.lesson_artifact.kind != ArtifactKind::Lesson
            || self.decision_context.kind != ArtifactKind::DecisionContext
            || self.outcome.kind != ArtifactKind::Outcome
        {
            return Err(DomainError::EmptyField {
                field: "lesson_evidence.references",
            });
        }
        if self
            .calibration_ppm_by_horizon
            .iter()
            .flatten()
            .any(|value| *value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "lesson_evidence.calibration_ppm",
            });
        }
        Ok(())
    }

    /// Idempotency identity. Deliberately excludes `recorded_at` so that
    /// reprocessing the same (lesson, decision, outcome) triple is a no-op rather
    /// than a duplicate row.
    pub fn idempotency_key(&self) -> (String, String, String) {
        (
            self.lesson_id.0.clone(),
            self.decision_context.artifact_id.0.as_str().to_owned(),
            self.outcome.artifact_id.0.as_str().to_owned(),
        )
    }

    pub fn identity_hash(&self) -> Result<ContentHash, serde_json::Error> {
        let (lesson_id, decision_context, outcome) = self.idempotency_key();
        crate::content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "lesson_id": lesson_id,
            "decision_context": decision_context,
            "outcome": outcome,
        }))
    }

    /// True when two records describe the same observation.
    ///
    /// Excludes `recorded_at` for the same reason `idempotency_key` does: it is
    /// bookkeeping, not observation. Reprocessing one sealed decision on a later
    /// day yields the same evidence with a later timestamp, so comparing it must
    /// not report a conflict. Every field that carries meaning is compared, so a
    /// changed attribution or utility is still rejected as tampering.
    pub fn describes_same_observation(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.lesson_id == other.lesson_id
            && self.lesson_artifact == other.lesson_artifact
            && self.decision_context == other.decision_context
            && self.outcome == other.outcome
            && self.attribution == other.attribution
            && self.utility_ppm_by_horizon == other.utility_ppm_by_horizon
            && self.calibration_ppm_by_horizon == other.calibration_ppm_by_horizon
    }

    /// True when at least one horizon closed with positive utility. Descriptive
    /// only; see the type-level note on causality.
    pub fn any_horizon_positive_utility(&self) -> bool {
        self.utility_ppm_by_horizon.iter().any(|value| *value > 0)
    }
}

/// Observational rollup over a Lesson's evidence ledger.
///
/// Every field is a co-occurrence count, not an effect estimate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LessonEvidenceSummary {
    pub applied_count: u64,
    pub rejected_count: u64,
    /// Of `applied_count`, how many closed with positive utility on any horizon.
    pub applied_with_positive_utility: u64,
    /// Of `rejected_count`, how many closed with positive utility on any horizon.
    /// Not a counterfactual for the applied arm.
    pub rejected_with_positive_utility: u64,
    /// Mean calibration quality across every scored horizon of every record.
    pub mean_calibration_ppm: Option<u32>,
    /// Always true. Present so callers cannot silently treat this as causal.
    pub observational: bool,
}

impl LessonEvidenceSummary {
    pub fn from_records(records: &[LessonEvidence]) -> Self {
        let mut summary = Self {
            observational: true,
            ..Self::default()
        };
        let mut calibration_total = 0_u64;
        let mut calibration_count = 0_u64;
        for record in records {
            let positive = record.any_horizon_positive_utility();
            match record.attribution {
                LessonAttribution::Applied => {
                    summary.applied_count += 1;
                    summary.applied_with_positive_utility += u64::from(positive);
                }
                LessonAttribution::Rejected => {
                    summary.rejected_count += 1;
                    summary.rejected_with_positive_utility += u64::from(positive);
                }
            }
            for value in record.calibration_ppm_by_horizon.iter().flatten() {
                calibration_total += u64::from(*value);
                calibration_count += 1;
            }
        }
        summary.mean_calibration_ppm = calibration_total
            .checked_div(calibration_count)
            .and_then(|mean| u32::try_from(mean).ok());
        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArtifactId, ContentHash};

    fn reference(kind: ArtifactKind) -> ArtifactRef {
        ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(format!("{kind:?}").as_bytes())),
            kind,
        }
    }

    fn lesson() -> Lesson {
        Lesson {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            lesson_id: LessonId::new(),
            origin: LessonOrigin::Operator,
            lifecycle: LessonLifecycle::Draft,
            title: "Opening volatility".to_owned(),
            statement: "High opening volatility weakens the signal.".to_owned(),
            rationale: "The first quote window is noisy.".to_owned(),
            recommended_behavior: "Require stronger evidence before acting.".to_owned(),
            exclusions: vec!["Do not apply after the first stable window.".to_owned()],
            scope: LessonScope {
                assets: BTreeSet::from([Asset::Tqqq]),
                horizons: BTreeSet::from([DecisionHorizon::T1]),
                ..LessonScope::default()
            },
            source_refs: vec![reference(ArtifactKind::SemanticDetail)],
            supersedes: vec![],
            conflicts_with: vec![],
            confidence_ppm: 700_000,
            authored_by: Some("operator:test".to_owned()),
            approved_by: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn operator_draft_validates_without_approval() {
        lesson().validate().unwrap();
    }

    #[test]
    fn active_lesson_requires_approval() {
        let mut value = lesson();
        value.lifecycle = LessonLifecycle::Active;
        assert!(value.validate().is_err());
        value.approved_by = Some("operator:reviewer".to_owned());
        value.validate().unwrap();
    }

    #[test]
    fn lesson_relationships_are_typed() {
        let mut value = lesson();
        value.supersedes = vec![reference(ArtifactKind::Claim)];
        assert!(value.validate().is_err());
    }

    #[test]
    fn scope_matching_filters_known_assets_and_horizons() {
        let scope = LessonScope {
            assets: BTreeSet::from([Asset::Tqqq]),
            horizons: BTreeSet::from([DecisionHorizon::T1]),
            ..LessonScope::default()
        };
        assert!(scope.matches(
            &BTreeSet::from([Asset::Tqqq]),
            &BTreeSet::from([DecisionHorizon::T1]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        ));
        assert!(!scope.matches(
            &BTreeSet::from([Asset::Qqq]),
            &BTreeSet::from([DecisionHorizon::T1]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        ));
    }
}
