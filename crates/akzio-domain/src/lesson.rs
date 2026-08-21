//! Reusable learning statements, kept separate from outcome-backed experiences.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactKind, ArtifactRef, Asset, DecisionHorizon, DomainError, LessonId,
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
