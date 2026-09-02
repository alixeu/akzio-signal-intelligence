impl EvaluationRuntime {
    fn evaluate_with_retrospective_at_state(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
        target_state: Option<PolicyState>,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        self.require_paper(&input.permit.run_id)?;
        if input.hypothesis_id.trim().is_empty() {
            return Err(EvaluationError::EmptyHypothesis);
        }
        match (&input.subject, &input.candidate_policy) {
            (PolicySubject::Memory(_), None)
            | (PolicySubject::Contract(_), Some(_))
            | (PolicySubject::Topology(_), Some(_)) => {}
            (PolicySubject::Memory(_), Some(_)) => {
                return Err(EvaluationError::InvalidCandidatePolicy("memory_subject"));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                return Err(EvaluationError::InvalidCandidatePolicy("missing_candidate"));
            }
        }
        let outcome = materialize_outcome(&input.materialization)?;
        outcome.validate_sealed()?;

        let previous_head = self.store.policy_head(&input.subject)?;
        let current = previous_head
            .as_ref()
            .map(|head| head.state)
            .unwrap_or_else(|| input.subject.initial_state());
        if !input.subject.accepts_state(current) {
            return Err(EvaluationError::SubjectStateMismatch);
        }

        let created_at = outcome
            .sealed_at
            .expect("materialized outcome always has sealed_at");
        let pair_snapshot = self.store.policy_shadow_pair_snapshot(&input.subject)?;
        let fresh_pairs_by_horizon = pair_snapshot.counts_by_horizon;
        let degraded = self.policy.outcome_is_degraded(&outcome);
        let risk_recall_measured = self.policy.risk_recall_is_measured(&outcome);

        let origin = input.permit.artifact_origin();
        let provenance = crate::trusted_learning_provenance(&input.permit, created_at);

        let outcome_artifact = if let Some(existing) = self
            .store
            .outcome_for(&input.permit.run_id, &outcome.outcome_id)?
        {
            existing
        } else {
            let outcome_sources = std::iter::once(input.materialization.schedule_artifact.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .collect();
            self.artifact(
                ArtifactKind::Outcome,
                &outcome,
                outcome_sources,
                &origin,
                &provenance,
                created_at,
            )?
        };
        let outcome_ref = reference(&outcome_artifact);
        let retrospective_artifact = if let Some(existing) = self.store.retrospective_for(
            &input.permit.run_id,
            &outcome.outcome_id,
            OutcomeHorizon::T5,
        )? {
            existing
        } else {
            let mut retrospective = Retrospective {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                outcome_id: outcome.outcome_id.clone(),
                horizon: OutcomeHorizon::T5,
                status: RetrospectiveStatus::Complete,
                summary:
                    "Rust-sealed outcome retrospective; model narrative unavailable in this commit"
                        .to_owned(),
                findings: Vec::new(),
                counterfactuals: Vec::new(),
                lesson_candidates: Vec::new(),
                diagnostic_gaps: vec![
                    "governed retrospective model narrative not installed".to_owned()
                ],
                source_refs: vec![outcome_ref.clone()],
                outcome: outcome_ref.clone(),
                created_at,
                sealed_at: Some(created_at),
            };
            if let Some(draft) = retrospective_draft {
                if draft.outcome_id != outcome.outcome_id || draft.horizon != OutcomeHorizon::T5 {
                    return Err(EvaluationError::InvalidMaterialization(
                        "retrospective draft identity",
                    ));
                }
                retrospective.summary = draft.summary.clone();
                retrospective.findings = draft.findings.clone();
                retrospective.counterfactuals = draft.counterfactuals.clone();
                retrospective.lesson_candidates = draft.lesson_candidates.clone();
                retrospective.diagnostic_gaps = draft.diagnostic_gaps.clone();
                retrospective.source_refs = draft.source_refs.clone();
                retrospective.source_refs.extend(
                    draft
                        .findings
                        .iter()
                        .flat_map(|finding| finding.artifact_refs.iter().cloned()),
                );
                retrospective.source_refs.push(outcome_ref.clone());
                retrospective.source_refs.sort();
                retrospective.source_refs.dedup();
            }
            for prior in self.store.retrospectives(&input.permit.run_id)? {
                retrospective.source_refs.push(reference(&prior));
            }
            retrospective.source_refs.sort();
            retrospective.source_refs.dedup();
            retrospective.validate()?;
            self.artifact(
                ArtifactKind::Retrospective,
                &retrospective,
                retrospective.source_refs.clone(),
                &origin,
                &provenance,
                created_at,
            )?
        };
        let retrospective_ref = reference(&retrospective_artifact);
        let schedule = &input.materialization.schedule;
        let policy_verdict = execution_verdict(&schedule.execution).clone();
        let experience = Experience {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            experience_id: ExperienceId(stable_id(&serde_json::json!({
                "subject": &input.subject,
                "hypothesis_id": &input.hypothesis_id,
                "decision": &schedule.decision,
                "outcome": &outcome_ref,
                "contract_hash": &input.contract_hash,
                "topology_id": &input.topology_id,
            }))?),
            subject: input.subject.clone(),
            hypothesis_id: input.hypothesis_id.clone(),
            decision: schedule.decision.clone(),
            decision_context: schedule.decision_context.clone(),
            execution_context: schedule.execution_context.clone(),
            policy_verdict,
            outcome: outcome_ref.clone(),
            contract_hash: input.contract_hash.clone(),
            topology_id: input.topology_id.clone(),
            policy_state: current,
            created_at,
        };
        experience.validate()?;
        let experience_artifact = self.artifact(
            ArtifactKind::Experience,
            &experience,
            vec![
                experience.decision.clone(),
                experience.decision_context.clone(),
                experience.execution_context.clone(),
                experience.policy_verdict.clone(),
                experience.outcome.clone(),
                retrospective_ref.clone(),
            ],
            &origin,
            &provenance,
            created_at,
        )?;
        let experience_ref = reference(&experience_artifact);
        let evaluation = Evaluation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            evaluation_id: EvaluationId(stable_id(&serde_json::json!({
                "subject": &input.subject,
                "outcome": &outcome_ref,
                "experience": &experience_ref,
                "candidate_policy": &input.candidate_policy,
                "token_cost": input.token_cost,
                "latency_millis": input.latency_millis,
            }))?),
            outcome: outcome_ref.clone(),
            experience: experience_ref.clone(),
            marginal_utility_ppm: marginal_utility(&outcome),
            token_cost: input.token_cost,
            latency_millis: input.latency_millis,
            created_at,
        };
        let evaluation_artifact = self.artifact(
            ArtifactKind::Evaluation,
            &evaluation,
            vec![
                evaluation.outcome.clone(),
                evaluation.experience.clone(),
                retrospective_ref.clone(),
            ],
            &origin,
            &provenance,
            created_at,
        )?;
        let evaluation_ref = reference(&evaluation_artifact);
        let candidate_policy_artifact = input
            .candidate_policy
            .as_ref()
            .map(|candidate| {
                let policy = CandidatePolicy {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    subject: input.subject.clone(),
                    baseline: candidate.baseline.clone(),
                    candidate: candidate.candidate.clone(),
                    source_evaluation: evaluation_ref.clone(),
                    created_at,
                };
                policy.validate()?;
                self.artifact(
                    ArtifactKind::CandidatePolicy,
                    &policy,
                    vec![
                        policy.baseline.clone(),
                        policy.candidate.clone(),
                        policy.source_evaluation.clone(),
                    ],
                    &origin,
                    &provenance,
                    created_at,
                )
            })
            .transpose()?;
        let candidate_policy_ref = candidate_policy_artifact.as_ref().map(reference);
        let next = next_state_with_fresh_pairs(
            current,
            target_state,
            degraded,
            risk_recall_measured,
            fresh_pairs_by_horizon,
            self.policy.minimum_fresh_pairs_per_horizon,
        );
        if !input.subject.accepts_state(next) {
            return Err(EvaluationError::SubjectStateMismatch);
        }

        let transition = if next == current {
            None
        } else {
            Some(PolicyTransition {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                transition_id: PolicyTransitionId(stable_id(&serde_json::json!({
                    "subject": &input.subject,
                    "from": current,
                    "to": next,
                    "evaluation": &evaluation_ref,
                }))?),
                subject: input.subject.clone(),
                from: current,
                to: next,
                evaluation: evaluation_ref.clone(),
                created_at,
            })
        };
        let retrospective_for_lessons = retrospective_artifact.clone();
        let retrospective_payload: Retrospective =
            serde_json::from_slice(&self.store.read_blob(&retrospective_for_lessons.blob)?)?;
        let policy_head = self
            .store
            .record_policy_evaluation_fenced(
                lease,
                &PolicyEvaluationCommit {
                    permit: input.permit,
                    outcome: outcome_artifact,
                    final_retrospective: retrospective_artifact,
                    experience: experience_artifact,
                    evaluation: evaluation_artifact,
                    candidate_policy: candidate_policy_artifact,
                    subject: input.subject,
                    from: current,
                    to: next,
                    pair_snapshot,
                    transition,
                    completed_at: created_at,
                },
            )?
            .policy_head;
        self.materialize_retrospective_lessons(
            &retrospective_for_lessons,
            &retrospective_payload,
            created_at,
        )?;

        Ok(EvaluationResult {
            outcome: outcome_ref,
            experience: experience_ref,
            evaluation: evaluation_ref,
            candidate_policy: candidate_policy_ref,
            policy_head,
            fresh_pairs_by_horizon,
        })
    }
}
