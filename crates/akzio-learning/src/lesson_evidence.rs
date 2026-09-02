//! Derives the observational Lesson evidence ledger from sealed Paper outcomes.
//!
//! This module only *describes* what happened: which Lesson revision a decision
//! cited, and how that decision's sealed outcome closed. It makes no causal
//! claim, and nothing here may drive an automatic contest or retire. See
//! `LessonEvidence` in `akzio-domain` for why: utility after a Lesson was applied
//! moves with market regime, the other injected Lessons, the contract, the
//! topology, the target weights, execution cost and evidence quality, and the
//! outcome after a *rejected* Lesson is not a counterfactual. Attributing effect
//! needs a paired on/off comparison against the same outcome.

use chrono::{DateTime, Utc};

use akzio_domain::{
    ArtifactKind, ArtifactRef, DecisionContext, Lesson, LessonAttribution, LessonEvidence, Outcome,
    OutcomeHorizon, V2_DOMAIN_SCHEMA_VERSION,
};

use crate::{EvaluationError, EvaluationRuntime, EvaluationRuntimeResult};

impl EvaluationRuntime {
    /// Build one record per Lesson the decision took a position on.
    ///
    /// The decision gate already forces the model to place every Lesson in its
    /// manifest into `applied_learning_refs` or `rejected_learning_refs`
    /// (`MissingLearningAttribution`), so both arms are observable rather than
    /// only the applied one. Experience and CandidatePolicy references are
    /// skipped: they are outcome-backed artifacts with their own lineage, not
    /// Lessons.
    pub fn lesson_evidence_from_decision(
        &self,
        decision_context: &ArtifactRef,
        context: &DecisionContext,
        outcome_artifact: &ArtifactRef,
        outcome: &Outcome,
        recorded_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<Vec<LessonEvidence>> {
        context.validate()?;
        // Only a sealed outcome carries all three windows, and canonical
        // learning is defined on sealed outcomes alone.
        outcome.validate_sealed()?;
        if decision_context.kind != ArtifactKind::DecisionContext
            || outcome_artifact.kind != ArtifactKind::Outcome
        {
            return Err(EvaluationError::InvalidMaterialization(
                "lesson evidence reference kind",
            ));
        }

        let utility_ppm_by_horizon = Self::by_horizon(outcome, |window| window.utility_ppm, 0);
        let calibration_ppm_by_horizon =
            Self::by_horizon(outcome, |window| window.calibration_ppm, None);

        let mut records = Vec::new();
        for (attribution, references) in [
            (LessonAttribution::Applied, &context.applied_learning_refs),
            (LessonAttribution::Rejected, &context.rejected_learning_refs),
        ] {
            for reference in references
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::Lesson)
            {
                let lesson = self.read_lesson(reference)?;
                let record = LessonEvidence {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    lesson_id: lesson.lesson_id,
                    lesson_artifact: reference.clone(),
                    decision_context: decision_context.clone(),
                    outcome: outcome_artifact.clone(),
                    attribution,
                    utility_ppm_by_horizon,
                    calibration_ppm_by_horizon,
                    recorded_at,
                };
                record.validate()?;
                records.push(record);
            }
        }
        Ok(records)
    }

    fn by_horizon<T: Copy>(
        outcome: &Outcome,
        project: impl Fn(&akzio_domain::OutcomeWindow) -> T,
        fallback: T,
    ) -> [T; 3] {
        let mut values = [fallback; 3];
        for (index, horizon) in OutcomeHorizon::ALL.into_iter().enumerate() {
            if let Some(window) = outcome
                .windows
                .iter()
                .find(|window| window.horizon == horizon)
            {
                values[index] = project(window);
            }
        }
        values
    }

    fn read_lesson(&self, reference: &ArtifactRef) -> EvaluationRuntimeResult<Lesson> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if artifact.kind != ArtifactKind::Lesson {
            return Err(EvaluationError::InvalidMaterialization(
                "lesson evidence reference kind",
            ));
        }
        let lesson: Lesson = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
        lesson.validate()?;
        Ok(lesson)
    }
}
