impl EvaluationRuntime {
    fn materialize_retrospective_lessons(
        &self,
        retrospective_artifact: &Artifact,
        retrospective: &Retrospective,
        created_at: DateTime<Utc>,
    ) -> EvaluationRuntimeResult<()> {
        // 只有已提交的 T+5 retrospective lesson_proposals 才进入这里；每条 proposal
        // 先验收并写成 Draft + outcome_quarantined，绝不会因为一次 Outcome 自动变成 Active。
        for (index, candidate) in retrospective.lesson_proposals.iter().enumerate() {
            candidate.validate()?;
            let statement = candidate.statement.trim();
            if statement.is_empty() {
                continue;
            }
            let lesson = Lesson {
                schema_version: DOMAIN_SCHEMA_VERSION,
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
                recommended_behavior: candidate.recommended_behavior.clone(),
                exclusions: candidate.exclusions.clone(),
                scope: LessonScope {
                    assets: candidate.assets.clone(),
                    horizons: candidate.horizons.clone(),
                    ..LessonScope::default()
                },
                source_refs: std::iter::once(reference(retrospective_artifact))
                    .chain(candidate.evidence_refs.iter().cloned())
                    .collect(),
                supersedes: Vec::new(),
                conflicts_with: Vec::new(),
                confidence_ppm: 500_000,
                authored_by: None,
                approved_by: None,
                created_at,
                updated_at: created_at,
                governance: Some(akzio_domain::LessonGovernance::outcome_quarantined(
                    created_at,
                )),
            };
            // source_refs 同时保留 retrospective 和 proposal evidence，供后续人工治理/召回
            // 审计；空 statement 被跳过，因此“有 proposal”不等于“写入一条 Lesson”。
            self.store
                .write_lesson(&lesson, retrospective_artifact, created_at)?;
        }
        Ok(())
    }

    fn require_paper(&self, run_id: &akzio_domain::RunId) -> EvaluationRuntimeResult<()> {
        // Outcome/Learning 的 canonical 写入只允许 Paper；隔离 Debug 或 PositionPlan 在
        // 这里 fail closed，不会靠调用方传入的 payload 伪装成正式样本。
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
        // Rust-only 路径只封存数值 Outcome 和“模型不可用”的 T5 retrospective；它不创建
        // Experience/Evaluation，也不推进 Policy，便于把部分完成与学习资格分开记录。
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
        // 有 draft 时只接受同一 outcome_id/T5 的受治理叙事，并把 draft 的来源和已提交
        // draft Artifact 引用并入 source_refs；没有 draft 仍可形成可审计的 ModelUnavailable。
        self.seal_outcome_for_evaluation_fenced(
            lease,
            permit,
            materialization,
            retrospective_draft,
            diagnostic_gap,
            now,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn seal_outcome_for_evaluation_fenced(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        materialization: OutcomeMaterializationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
        diagnostic_gap: &str,
        now: DateTime<Utc>,
        complete_task: bool,
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
            let sources = std::iter::once(materialization.schedule_artifact)
                .chain(outcome.market_evidence.iter().cloned())
                .chain(outcome.risk_ground_truth_refs())
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
            schema_version: DOMAIN_SCHEMA_VERSION,
            outcome_id: outcome.outcome_id.clone(),
            horizon: OutcomeHorizon::T5,
            status: RetrospectiveStatus::ModelUnavailable,
            summary: "Rust-sealed retrospective; governed model unavailable".to_owned(),
            findings: Vec::new(),
            counterfactuals: Vec::new(),
            lesson_candidates: Vec::new(),
            lesson_proposals: Vec::new(),
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
            retrospective.lesson_proposals = draft.lesson_proposals.clone();
            retrospective.diagnostic_gaps = draft.diagnostic_gaps.clone();
            retrospective.source_refs.extend(draft.source_refs.clone());
            retrospective
                .source_refs
                .extend(self.committed_draft_refs(permit, draft)?);
            retrospective.source_refs.extend(
                draft
                    .lesson_proposals
                    .iter()
                    .flat_map(|p| p.evidence_refs.iter().cloned()),
            );
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
        // 该 Store 入口在同一带 lease 的事务中写 canonical Outcome 与 T5 retrospective；
        // complete_task=false 可保留可重试的已密封进度，true 才同时收束当前 task。
        self.store.write_outcome_retrospective_fenced(
            lease,
            permit,
            &outcome_artifact,
            &retrospective_artifact,
            now,
            complete_task,
        )?;
        Ok((outcome_artifact, retrospective_artifact))
    }
}
