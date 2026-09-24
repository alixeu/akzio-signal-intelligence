impl EvaluationRuntime {
    fn evaluate_with_retrospective(
        &self,
        lease: Option<&DaemonLease>,
        input: EvaluationInput,
        retrospective_draft: Option<&RetrospectiveDraft>,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        // 入口先锁定 Paper purpose，再由 Rust 从原始观察密封 Outcome；已存在同一
        // outcome_id 的 Artifact 会复用，避免重试产生第二份数值事实。
        self.require_paper(&input.permit.run_id)?;
        let outcome = materialize_outcome(&input.materialization)?;
        let now = Utc::now();
        let artifact = if let Some(existing) = self
            .store
            .outcome_for(&input.permit.run_id, &outcome.outcome_id)?
        {
            existing
        } else {
            self.artifact(
                ArtifactKind::Outcome,
                &outcome,
                std::iter::once(input.materialization.schedule_artifact.clone())
                    .chain(outcome.market_evidence.iter().cloned())
                    .chain(outcome.risk_ground_truth_refs())
                    .collect(),
                &input.permit.artifact_origin(),
                &crate::trusted_learning_provenance(&input.permit, now),
                now,
            )?
        };
        let sealed = SealedEvaluationInput {
            complete_task: true,
            permit: input.permit,
            subject: input.subject,
            hypothesis_id: input.hypothesis_id,
            outcome: reference(&artifact),
            contract_hash: input.contract_hash,
            topology_id: input.topology_id,
            candidate_policy: input.candidate_policy,
            token_cost: input.token_cost,
            latency_millis: input.latency_millis,
        };
        self.evaluate_frozen(lease, sealed, artifact, retrospective_draft, None)
    }

    /// Uses the same eligibility, pair-consumption and policy transaction as T5.
    pub fn evaluate_sealed_with_retrospective(
        &self,
        lease: Option<&DaemonLease>,
        input: SealedEvaluationInput,
        draft: &RetrospectiveDraft,
        target_state: Option<PolicyState>,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        let artifact = self.store.artifact(&input.outcome.artifact_id)?;
        self.evaluate_frozen(lease, input, artifact, Some(draft), target_state)
    }

    fn evaluate_frozen(
        &self,
        lease: Option<&DaemonLease>,
        input: SealedEvaluationInput,
        outcome_artifact: Artifact,
        retrospective_draft: Option<&RetrospectiveDraft>,
        target_state: Option<PolicyState>,
    ) -> EvaluationRuntimeResult<EvaluationResult> {
        // 这里重新从 CAS 读取 OutcomeSchedule、Outcome 和 retrospective draft，并逐项
        // 校验 Run、T+5、subject 与候选 Policy 绑定；传入一个“看起来完成”的引用不足以越过这些 Gate。
        if input.outcome.kind != ArtifactKind::Outcome
            || outcome_artifact.kind != ArtifactKind::Outcome
            || outcome_artifact
                .origin
                .as_ref()
                .and_then(|o| o.run_id.as_ref())
                != Some(&input.permit.run_id)
        {
            return Err(EvaluationError::InvalidMaterialization(
                "sealed outcome binding",
            ));
        }
        let outcome: Outcome =
            serde_json::from_slice(&self.store.read_blob(&outcome_artifact.blob)?)?;
        let schedule_artifact = self.store.artifact(&outcome.schedule.artifact_id)?;
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&self.store.read_blob(&schedule_artifact.blob)?)?;
        schedule.validate()?;
        self.require_paper(&input.permit.run_id)?;
        let draft = retrospective_draft.ok_or(EvaluationError::InvalidMaterialization(
            "valid governed retrospective required for learning",
        ))?;
        draft.validate()?;

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
        outcome.validate_sealed()?;

        if draft.outcome_id != outcome.outcome_id || draft.horizon != OutcomeHorizon::T5 {
            return Err(EvaluationError::InvalidMaterialization(
                "retrospective draft identity",
            ));
        }
        if let Some(existing) = self
            .store
            .policy_evaluation_for_outcome(&input.subject, &input.outcome)?
        {
            // 相同 subject/outcome 的重复请求走幂等读取路径；若要求 CandidatePolicy，
            // 还必须找到与原 Evaluation 完全绑定的已记录 Artifact，否则报错而不补造。
            let evaluation: Evaluation =
                serde_json::from_slice(&self.store.read_blob(&existing.blob)?)?;
            let mut candidate_policy = None;
            if input.candidate_policy.is_some() {
                for artifact in self
                    .store
                    .run_artifacts_by_kind(&input.permit.run_id, ArtifactKind::CandidatePolicy)?
                {
                    let policy: CandidatePolicy =
                        serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                    if policy.source_evaluation == reference(&existing)
                        && policy.subject == input.subject
                    {
                        candidate_policy = Some(reference(&artifact));
                        break;
                    }
                }
                if candidate_policy.is_none() {
                    return Err(EvaluationError::InvalidCandidatePolicy(
                        "recorded candidate missing",
                    ));
                }
            }
            return Ok(EvaluationResult {
                outcome: evaluation.outcome,
                experience: evaluation.experience,
                evaluation: reference(&existing),
                candidate_policy,
                policy_head: self.store.policy_head(&input.subject)?,
                fresh_pairs_by_horizon: self
                    .store
                    .policy_shadow_pair_snapshot(&input.subject)?
                    .counts_by_horizon,
            });
        }

        let previous_head = self.store.policy_head(&input.subject)?;
        let current = previous_head
            .as_ref()
            .map(|head| head.state)
            .unwrap_or_else(|| input.subject.initial_state());
        if !input.subject.accepts_state(current) {
            return Err(EvaluationError::SubjectStateMismatch);
        }

        let created_at = Utc::now();
        let pair_snapshot = self.store.policy_shadow_pair_snapshot(&input.subject)?;
        let fresh_pairs_by_horizon = pair_snapshot.counts_by_horizon;
        let degraded = self.policy.outcome_is_degraded(&outcome);
        let context_artifact = self
            .store
            .artifact(&schedule.decision_context.artifact_id)?;
        let context: akzio_domain::DecisionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        context.validate()?;
        let claims = context
            .claims
            .iter()
            .map(|r| {
                let artifact = self.store.artifact(&r.artifact_id)?;
                let claim: akzio_domain::ResearchClaim =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                claim.validate()?;
                Ok((r.clone(), claim))
            })
            .collect::<EvaluationRuntimeResult<Vec<_>>>()?;
        let critiques = context
            .critiques
            .iter()
            .map(|r| {
                let artifact = self.store.artifact(&r.artifact_id)?;
                let critique: akzio_domain::ResearchCritique =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                critique.validate()?;
                Ok(critique)
            })
            .collect::<EvaluationRuntimeResult<Vec<_>>>()?;
        let research_sufficient = akzio_domain::research_coverage_is_complete(&claims, &critiques)
            && context.hard_blockers.is_empty()
            && !context
                .soft_warnings
                .contains(&akzio_domain::SoftWarning::IncompleteEvidence);
        let quality_metrics_measured = self.policy.risk_recall_is_measured(&outcome)
            && self.policy.evidence_completeness_is_measured(&outcome)
            && research_sufficient;
        // learning_eligible 同时需要风险真值、证据完整度和研究覆盖；缺一项只会让
        // Experience 记录为不可学习，Outcome/Experience 本身仍可作为审计事实保留。
        let producer_contract_hashes = context
            .claims
            .iter()
            .chain(context.critiques.iter())
            .map(|r| self.store.artifact(&r.artifact_id))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|a| a.provenance.producer_contract_hash)
            .chain(std::iter::once(input.contract_hash.clone()))
            .collect();
        let producing_workflow = self.store.workflow_snapshot(&input.permit.run_id)?;
        let evaluation_context = akzio_domain::ExperienceEvaluationContext {
            version: 1,
            producer_run_id: input.permit.run_id.clone(),
            producer_contract_hashes,
            producer_workflow: reference(&producing_workflow.revision.graph_artifact),
            producer_workflow_revision: producing_workflow.revision.revision,
            evaluator_contract_hash: input.permit.contract_hash.clone(),
            metric_basis: outcome.metric_basis,
            market_window_complete: self.policy.evidence_completeness_is_measured(&outcome),
            research_sufficient,
            retrospective_valid: true,
            risk_ground_truth_measured: self.policy.risk_recall_is_measured(&outcome),
            learning_eligible: quality_metrics_measured && !degraded,
        };

        let origin = input.permit.artifact_origin();
        let provenance = crate::trusted_learning_provenance(&input.permit, created_at);

        let outcome_ref = reference(&outcome_artifact);
        let existing_retrospective = self.store.retrospective_for(
            &input.permit.run_id,
            &outcome.outcome_id,
            OutcomeHorizon::T5,
        )?;
        let complete_retrospective = existing_retrospective
            .as_ref()
            .map(|artifact| {
                let payload: Retrospective =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                Ok::<_, EvaluationError>(payload.status == RetrospectiveStatus::Complete)
            })
            .transpose()?
            .unwrap_or(false);
        let retrospective_artifact = if complete_retrospective {
            existing_retrospective.expect("complete retrospective")
        } else {
            let mut retrospective = Retrospective {
                schema_version: DOMAIN_SCHEMA_VERSION,
                outcome_id: outcome.outcome_id.clone(),
                horizon: OutcomeHorizon::T5,
                status: RetrospectiveStatus::Complete,
                summary:
                    "Rust-sealed outcome retrospective; model narrative unavailable in this commit"
                        .to_owned(),
                findings: Vec::new(),
                counterfactuals: Vec::new(),
                lesson_candidates: Vec::new(),
                lesson_proposals: Vec::new(),
                diagnostic_gaps: vec![
                    "governed retrospective model narrative not installed".to_owned(),
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
                retrospective.lesson_proposals = draft.lesson_proposals.clone();
                retrospective.diagnostic_gaps = draft.diagnostic_gaps.clone();
                retrospective.source_refs = draft.source_refs.clone();
                retrospective
                    .source_refs
                    .extend(self.committed_draft_refs(&input.permit, draft)?);
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
        let policy_verdict = execution_verdict(&schedule.execution).clone();
        let experience = Experience {
            schema_version: DOMAIN_SCHEMA_VERSION,
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
            evaluation_context: Some(evaluation_context),
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
                experience
                    .evaluation_context
                    .as_ref()
                    .expect("new experience context")
                    .producer_workflow
                    .clone(),
            ],
            &origin,
            &provenance,
            created_at,
        )?;
        let experience_ref = reference(&experience_artifact);
        let evaluation = Evaluation {
            schema_version: DOMAIN_SCHEMA_VERSION,
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
                retrospective_ref,
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
                    schema_version: DOMAIN_SCHEMA_VERSION,
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
            quality_metrics_measured,
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
                schema_version: DOMAIN_SCHEMA_VERSION,
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
        let lesson_evidence = self.lesson_evidence_for_outcome(
            &schedule.decision_context,
            &outcome_ref,
            &outcome,
            created_at,
        )?;
        let policy_head = self
            .store
            .record_policy_evaluation_fenced(
                lease,
                &PolicyEvaluationCommit {
                    complete_task: input.complete_task,
                    permit: input.permit,
                    outcome: outcome_artifact,
                    final_retrospective: retrospective_artifact,
                    experience: experience_artifact,
                    evaluation: evaluation_artifact,
                    candidate_policy: candidate_policy_artifact,
                    lesson_evidence,
                    subject: input.subject,
                    from: current,
                    to: next,
                    pair_snapshot,
                    transition,
                    completed_at: created_at,
                },
            )?
            .policy_head;
        // Store 在同一 SQLite Immediate 事务中校验 lease/permit、写入 Outcome、T5
        // retrospective、Experience、Evaluation、可选 CandidatePolicy、Lesson evidence
        // 和 policy cursor/transition；事务失败时不把内存里已构造的对象当成已提交。
        // 随后的 LessonProposal 写入是逐条的后续 Store 操作：若其中一条失败，前一事务
        // 已提交的 canonical Evaluation/Policy 不回滚，已成功写入的 Lesson 也保留并可审计。
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
