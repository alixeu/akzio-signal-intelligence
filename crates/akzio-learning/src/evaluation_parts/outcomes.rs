impl EvaluationRuntime {
    fn materialize_retrospective_lessons(
        &self,
        retrospective_artifact: &Artifact,
        retrospective: &Retrospective,
        created_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<()> {
        for (index, candidate) in retrospective.lesson_candidates.iter().enumerate() {
            let statement = candidate.trim();
            if statement.is_empty() {
                continue;
            }
            let lesson = Lesson {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                lesson_id: LessonId(stable_id(&serde_json::json!({
                    "retrospective": retrospective_artifact.artifact_id,
                    "index": index,
                    "statement": statement,
                }))?),
                origin: LessonOrigin::OutcomeDerived,
                lifecycle: LessonLifecycle::Draft,
                title: format!("Outcome lesson {}", index + 1),
                statement: statement.to_owned(),
                rationale: retrospective.summary.clone(),
                recommended_behavior: "Treat as a hypothesis until a reviewer approves it and Paper outcomes support it.".to_owned(),
                exclusions: retrospective.diagnostic_gaps.clone(),
                scope: LessonScope::default(),
                source_refs: vec![reference(retrospective_artifact)],
                supersedes: Vec::new(),
                conflicts_with: Vec::new(),
                confidence_ppm: 500_000,
                authored_by: None,
                approved_by: None,
                created_at,
                updated_at: created_at,
            };
            self.store
                .write_lesson(&lesson, retrospective_artifact, created_at)?;
        }
        Ok(())
    }

    fn require_paper(&self, run_id: &akzio_domain::RunId) -> EvaluationRuntimeResult<()> {
        require_canonical_purpose(self.store.run_purpose(run_id)?)
    }

    /// Seal an outcome and a Rust-only retrospective without creating any
    /// Experience, Evaluation, or policy influence.
    pub fn seal_outcome_with_rust_retrospective_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        materialization: OutcomeMaterializationInput,
        diagnostic_gap: &str,
        now: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<(Artifact, Artifact)> {
        self.seal_outcome_with_retrospective_fenced(
            lease,
            permit,
            materialization,
            None,
            diagnostic_gap,
            now,
        )
    }

    pub fn seal_outcome_with_retrospective_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        materialization: OutcomeMaterializationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
        diagnostic_gap: &str,
        now: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<(Artifact, Artifact)> {
        self.require_paper(&permit.run_id)?;
        let outcome = materialize_outcome(&materialization)?;
        outcome.validate_sealed()?;
        let origin = permit.artifact_origin();
        let provenance = crate::trusted_learning_provenance(permit, now);
        let outcome_artifact = if let Some(existing) = self
            .store
            .outcome_for(&permit.run_id, &outcome.outcome_id)?
        {
            existing
        } else {
            let sources = std::iter::once(materialization.schedule_artifact.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .collect();
            self.artifact(
                ArtifactKind::Outcome,
                &outcome,
                sources,
                &origin,
                &provenance,
                now,
            )?
        };
        let outcome_ref = reference(&outcome_artifact);
        let mut retrospective_source_refs = vec![outcome_ref.clone()];
        retrospective_source_refs.extend(
            self.store
                .retrospectives(&permit.run_id)?
                .into_iter()
                .map(|artifact| reference(&artifact)),
        );
        retrospective_source_refs.sort();
        retrospective_source_refs.dedup();
        let mut retrospective = Retrospective {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: outcome.outcome_id.clone(),
            horizon: OutcomeHorizon::T5,
            status: RetrospectiveStatus::ModelUnavailable,
            summary: "Rust-sealed retrospective; governed model unavailable".to_owned(),
            findings: Vec::new(),
            counterfactuals: Vec::new(),
            lesson_candidates: Vec::new(),
            diagnostic_gaps: vec![diagnostic_gap.to_owned()],
            source_refs: retrospective_source_refs,
            outcome: outcome_ref,
            created_at: now,
            sealed_at: Some(now),
        };
        if let Some(draft) = retrospective_draft {
            if draft.outcome_id != outcome.outcome_id || draft.horizon != OutcomeHorizon::T5 {
                return Err(EvaluationError::InvalidMaterialization(
                    "retrospective draft identity",
                ));
            }
            retrospective.status = RetrospectiveStatus::Complete;
            retrospective.summary = draft.summary.clone();
            retrospective.findings = draft.findings.clone();
            retrospective.counterfactuals = draft.counterfactuals.clone();
            retrospective.lesson_candidates = draft.lesson_candidates.clone();
            retrospective.diagnostic_gaps = draft.diagnostic_gaps.clone();
            retrospective.source_refs.extend(draft.source_refs.clone());
            retrospective.source_refs.extend(
                draft
                    .findings
                    .iter()
                    .flat_map(|finding| finding.artifact_refs.iter().cloned()),
            );
            retrospective.source_refs.sort();
            retrospective.source_refs.dedup();
        }
        retrospective.validate()?;
        let retrospective_artifact = if let Some(existing) =
            self.store
                .retrospective_for(&permit.run_id, &outcome.outcome_id, OutcomeHorizon::T5)?
        {
            existing
        } else {
            self.artifact(
                ArtifactKind::Retrospective,
                &retrospective,
                retrospective.source_refs.clone(),
                &origin,
                &provenance,
                now,
            )?
        };
        self.store.commit_outcome_retrospective_fenced(
            lease,
            permit,
            &outcome_artifact,
            &retrospective_artifact,
            now,
        )?;
        Ok((outcome_artifact, retrospective_artifact))
    }
}
