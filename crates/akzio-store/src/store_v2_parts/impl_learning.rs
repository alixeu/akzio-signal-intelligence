impl V2Store {
    fn verify_candidate_policy_history(&self, connection: &Connection) -> StoreResult<()> {
        let artifact_ids = connection
            .prepare(
                "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY artifact_id",
            )?
            .query_map(params![enum_name(ArtifactKind::CandidatePolicy)], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for value in artifact_ids {
            let artifact_id = ArtifactId(ContentHash::new(value)?);
            let artifact = read_artifact(connection, &artifact_id)?;
            if artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} is noncanonical"
                )));
            }
            assert_artifact_from_paper_with_connection(connection, &artifact).map_err(|error| {
                StoreError::Integrity(format!(
                    "candidate policy {artifact_id} has invalid origin: {error}"
                ))
            })?;
            let policy: CandidatePolicy = self.read_artifact_payload(&artifact)?;
            policy.validate()?;
            if !has_exact_source_refs(
                &artifact,
                &[
                    policy.baseline.clone(),
                    policy.candidate.clone(),
                    policy.source_evaluation.clone(),
                ],
            ) {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} has invalid source closure"
                )));
            }
            let evaluation =
                read_policy_evaluation(connection, &policy.source_evaluation.artifact_id)?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                            "candidate policy {artifact_id} has no source evaluation"
                        ))
                    })?;
            if evaluation.subject != policy.subject
                || evaluation.completed_at != policy.created_at
                || evaluation.candidate_policy_artifact_id.as_ref() != Some(&artifact_id)
            {
                return Err(StoreError::Integrity(format!(
                    "candidate policy {artifact_id} disagrees with source evaluation"
                )));
            }
            self.validate_candidate_policy_sources(connection, &policy)
                .map_err(|error| {
                    StoreError::Integrity(format!(
                        "candidate policy {artifact_id} has invalid binding: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    fn validate_policy_evaluation_commit_with_connection(
        &self,
        connection: &Connection,
        commit: &PolicyEvaluationCommit,
    ) -> StoreResult<()> {
        for (artifact, kind) in [
            (&commit.outcome, ArtifactKind::Outcome),
            (&commit.final_retrospective, ArtifactKind::Retrospective),
            (&commit.experience, ArtifactKind::Experience),
            (&commit.evaluation, ArtifactKind::Evaluation),
        ] {
            artifact.validate()?;
            self.read_blob(&artifact.blob)?;
            if artifact.kind != kind || artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.kind_or_lifecycle",
                ));
            }
        }
        if let Some(candidate_policy) = &commit.candidate_policy {
            candidate_policy.validate()?;
            self.read_blob(&candidate_policy.blob)?;
            if candidate_policy.kind != ArtifactKind::CandidatePolicy
                || candidate_policy.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.kind_or_lifecycle",
                ));
            }
        }
        let outcome: Outcome = self.read_artifact_payload(&commit.outcome)?;
        outcome.validate()?;
        if !outcome.is_sealed() {
            return Err(StoreError::UnsealedOutcome(
                commit.outcome.artifact_id.clone(),
            ));
        }
        let schedule =
            self.read_outcome_schedule_with_connection(connection, &outcome, &[RunPurpose::Paper])?;
        let final_retrospective: akzio_domain::Retrospective =
            self.read_artifact_payload(&commit.final_retrospective)?;
        final_retrospective.validate()?;
        if final_retrospective.horizon != OutcomeHorizon::T5
            || final_retrospective.status != akzio_domain::RetrospectiveStatus::Complete
            || final_retrospective.outcome.artifact_id != commit.outcome.artifact_id
            || final_retrospective.outcome.kind != ArtifactKind::Outcome
        {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.final_retrospective",
            ));
        }
        let experience: Experience = self.read_artifact_payload(&commit.experience)?;
        experience.validate()?;
        let evaluation: Evaluation = self.read_artifact_payload(&commit.evaluation)?;
        evaluation.validate()?;

        for reference in std::iter::once(&outcome.schedule)
            .chain(outcome.market_evidence.iter())
            .chain([
                &experience.decision,
                &experience.decision_context,
                &experience.execution_context,
                &experience.policy_verdict,
            ])
        {
            let source = read_artifact(connection, &reference.artifact_id)?;
            if source.kind != reference.kind {
                return Err(StoreError::InvalidLearningCommit(
                    "learning_artifact.source_kind",
                ));
            }
            assert_artifact_from_paper_with_connection(connection, &source)?;
        }

        let outcome_ref = ArtifactRef {
            artifact_id: commit.outcome.artifact_id.clone(),
            kind: ArtifactKind::Outcome,
        };
        let experience_ref = ArtifactRef {
            artifact_id: commit.experience.artifact_id.clone(),
            kind: ArtifactKind::Experience,
        };
        let evaluation_ref = ArtifactRef {
            artifact_id: commit.evaluation.artifact_id.clone(),
            kind: ArtifactKind::Evaluation,
        };
        let retrospective_ref = ArtifactRef {
            artifact_id: commit.final_retrospective.artifact_id.clone(),
            kind: ArtifactKind::Retrospective,
        };
        if !commit
            .final_retrospective
            .source_refs
            .contains(&outcome_ref)
        {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.final_retrospective_source_refs",
            ));
        }
        match (&commit.subject, &commit.candidate_policy) {
            (PolicySubject::Memory(_), None) => {}
            (PolicySubject::Memory(_), Some(_)) => {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.memory_subject",
                ));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), None) => {
                return Err(StoreError::InvalidLearningCommit(
                    "candidate_policy.missing",
                ));
            }
            (PolicySubject::Contract(_) | PolicySubject::Topology(_), Some(artifact)) => {
                let candidate_policy: CandidatePolicy = self.read_artifact_payload(artifact)?;
                candidate_policy.validate()?;
                if candidate_policy.subject != commit.subject
                    || candidate_policy.source_evaluation != evaluation_ref
                    || candidate_policy.created_at != commit.completed_at
                    || !has_exact_source_refs(
                        artifact,
                        &[
                            candidate_policy.baseline.clone(),
                            candidate_policy.candidate.clone(),
                            candidate_policy.source_evaluation.clone(),
                        ],
                    )
                {
                    return Err(StoreError::InvalidLearningCommit("candidate_policy.links"));
                }
                self.validate_candidate_policy_sources(connection, &candidate_policy)?;
            }
        }
        commit.subject.validate()?;
        if !commit.subject.accepts_state(commit.from) || !commit.subject.accepts_state(commit.to) {
            return Err(StoreError::InvalidLearningCommit(
                "policy_evaluation.subject_state",
            ));
        }
        let transition_matches = match &commit.transition {
            Some(transition) => {
                transition.validate()?;
                transition.subject == commit.subject
                    && transition.from == commit.from
                    && transition.to == commit.to
                    && transition.evaluation == evaluation_ref
                    && transition.created_at == commit.completed_at
            }
            None => commit.from == commit.to,
        };
        if experience.outcome != outcome_ref
            || evaluation.outcome != outcome_ref
            || evaluation.experience != experience_ref
            || !transition_matches
            || experience.subject != commit.subject
            || experience.policy_state != commit.from
            || experience.decision != schedule.decision
            || experience.decision_context != schedule.decision_context
            || experience.execution_context != schedule.execution_context
        {
            return Err(StoreError::InvalidLearningCommit("learning_artifact.links"));
        }
        if !has_exact_source_refs(
            &commit.outcome,
            &std::iter::once(outcome.schedule.clone())
                .chain(outcome.market_evidence.iter().cloned())
                .collect::<Vec<_>>(),
        ) || !has_exact_source_refs(
            &commit.experience,
            &[
                experience.decision.clone(),
                experience.decision_context.clone(),
                experience.execution_context.clone(),
                experience.policy_verdict.clone(),
                experience.outcome.clone(),
                retrospective_ref.clone(),
            ],
        ) || !has_exact_source_refs(
            &commit.evaluation,
            &[
                evaluation.outcome.clone(),
                evaluation.experience.clone(),
                retrospective_ref,
            ],
        ) {
            return Err(StoreError::InvalidLearningCommit(
                "learning_artifact.source_refs",
            ));
        }
        Ok(())
    }

    fn validate_candidate_policy_sources(
        &self,
        connection: &Connection,
        policy: &CandidatePolicy,
    ) -> StoreResult<()> {
        let baseline =
            read_required_artifact(connection, &policy.baseline, "candidate_policy.baseline")?;
        let candidate =
            read_required_artifact(connection, &policy.candidate, "candidate_policy.candidate")?;
        match &policy.subject {
            PolicySubject::Memory(_) => Err(StoreError::InvalidLearningCommit(
                "candidate_policy.memory_subject",
            )),
            PolicySubject::Contract(candidate_hash) => {
                if baseline.lifecycle != ArtifactLifecycle::Canonical
                    || candidate.lifecycle != ArtifactLifecycle::Canonical
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.contract_lifecycle",
                    ));
                }
                let baseline_contract: AgentContract = self.read_artifact_payload(&baseline)?;
                let candidate_contract: AgentContract = self.read_artifact_payload(&candidate)?;
                baseline_contract.validate()?;
                candidate_contract.validate()?;
                if &candidate_contract.contract_hash != candidate_hash
                    || !baseline_contract.permits_candidate(&candidate_contract)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.contract_binding",
                    ));
                }
                Ok(())
            }
            PolicySubject::Topology(topology_id) => {
                let baseline_graph: WorkflowGraph = self.read_artifact_payload(&baseline)?;
                let candidate_graph: WorkflowGraph = self.read_artifact_payload(&candidate)?;
                baseline_graph.validate()?;
                candidate_graph.validate()?;
                if candidate_graph.topology_id != topology_id.0
                    || workflow_graph_run_purpose(connection, &baseline.artifact_id)?
                        != RunPurpose::Paper
                    || workflow_graph_run_purpose(connection, &candidate.artifact_id)?
                        != RunPurpose::Shadow
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "candidate_policy.topology_binding",
                    ));
                }
                Ok(())
            }
        }
    }

    fn assert_shadow_pair_sources_with_connection(
        &self,
        connection: &Connection,
        completion: &ShadowPairCompletion,
    ) -> StoreResult<()> {
        let parent_decision = read_required_artifact(
            connection,
            &completion.parent_decision,
            "shadow_pair.parent_decision",
        )?;
        let execution_context = read_required_artifact(
            connection,
            &completion.execution_context,
            "shadow_pair.execution_context",
        )?;
        let candidate_decision = read_required_artifact(
            connection,
            &completion.candidate_decision,
            "shadow_pair.candidate_decision",
        )?;
        let parent_outcome_artifact = read_required_artifact(
            connection,
            &completion.parent_outcome,
            "shadow_pair.parent_outcome",
        )?;
        let candidate_outcome_artifact = read_required_artifact(
            connection,
            &completion.candidate_outcome,
            "shadow_pair.candidate_outcome",
        )?;

        assert_canonical_paper_artifact(connection, &parent_decision)?;
        assert_artifact_from_paper_with_connection(connection, &execution_context)?;
        assert_canonical_paper_artifact(connection, &parent_outcome_artifact)?;
        assert_shadow_candidate_artifact(connection, &candidate_decision)?;
        assert_shadow_candidate_artifact(connection, &candidate_outcome_artifact)?;
        assert_candidate_decision_binding(connection, &candidate_decision, completion)?;

        let parent_outcome: Outcome =
            serde_json::from_slice(&self.read_blob(&parent_outcome_artifact.blob)?)?;
        let candidate_outcome: Outcome =
            serde_json::from_slice(&self.read_blob(&candidate_outcome_artifact.blob)?)?;
        parent_outcome.validate_sealed()?;
        candidate_outcome.validate_sealed()?;

        let parent_schedule = self.read_outcome_schedule_with_connection(
            connection,
            &parent_outcome,
            &[RunPurpose::Paper],
        )?;
        let candidate_schedule = self.read_outcome_schedule_with_connection(
            connection,
            &candidate_outcome,
            &[RunPurpose::Paper, RunPurpose::Shadow],
        )?;
        if parent_schedule.decision != completion.parent_decision
            || candidate_schedule.decision != completion.candidate_decision
            || parent_schedule.execution_context != completion.execution_context
            || candidate_schedule.execution_context != completion.execution_context
        {
            return Err(StoreError::InvalidLearningCommit(
                "shadow_pair.schedule_binding",
            ));
        }
        Ok(())
    }

    fn read_artifact_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> StoreResult<T> {
        Ok(serde_json::from_slice(&self.read_blob(&artifact.blob)?)?)
    }

    fn read_outcome_schedule_with_connection(
        &self,
        connection: &Connection,
        outcome: &Outcome,
        allowed_purposes: &[RunPurpose],
    ) -> StoreResult<OutcomeSchedule> {
        if outcome.schedule.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit("outcome.schedule_kind"));
        }
        let schedule_artifact = read_artifact(connection, &outcome.schedule.artifact_id)?;
        if schedule_artifact.kind != ArtifactKind::OutcomeSchedule {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_artifact",
            ));
        }
        let schedule_purpose = artifact_run_purpose(connection, &schedule_artifact)?;
        let expected_lifecycle = match schedule_purpose {
            RunPurpose::Paper => ArtifactLifecycle::Canonical,
            RunPurpose::Shadow => ArtifactLifecycle::RunScoped,
            _ => {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome.schedule_artifact",
                ));
            }
        };
        if schedule_artifact.lifecycle != expected_lifecycle {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_artifact",
            ));
        }
        assert_artifact_from_allowed_purposes(connection, &schedule_artifact, allowed_purposes)?;
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&self.read_blob(&schedule_artifact.blob)?)?;
        schedule.validate()?;
        if schedule.outcome_id != outcome.outcome_id {
            return Err(StoreError::InvalidLearningCommit(
                "outcome.schedule_identity",
            ));
        }

        let expected = outcome_schedule_source_refs(&schedule);
        if !has_exact_source_refs(&schedule_artifact, &expected) {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_schedule.source_refs",
            ));
        }
        for reference in &expected {
            let artifact = read_artifact(connection, &reference.artifact_id)?;
            if artifact.kind != reference.kind {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome_schedule.source_kind",
                ));
            }
            assert_artifact_from_allowed_purposes(connection, &artifact, allowed_purposes)?;
        }
        self.validate_outcome_schedule_execution_lineage(connection, &schedule, allowed_purposes)?;
        Ok(schedule)
    }

    fn validate_outcome_schedule_execution_lineage(
        &self,
        connection: &Connection,
        schedule: &OutcomeSchedule,
        allowed_purposes: &[RunPurpose],
    ) -> StoreResult<()> {
        let verdict_ref = match &schedule.execution {
            OutcomeExecutionLineage::NoOrder { execution_verdict } => execution_verdict,
            OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict, ..
            } => execution_verdict,
        };
        let verdict_artifact = read_artifact(connection, &verdict_ref.artifact_id)?;
        if verdict_artifact.kind != ArtifactKind::ExecutionVerdict {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_schedule.execution_verdict_kind",
            ));
        }
        assert_artifact_from_allowed_purposes(connection, &verdict_artifact, allowed_purposes)?;
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&self.read_blob(&verdict_artifact.blob)?)?;
        verdict.validate()?;

        match (&schedule.execution, verdict) {
            (
                OutcomeExecutionLineage::NoOrder { execution_verdict },
                ExecutionVerdict::NoOrder { no_order },
            ) if execution_verdict == verdict_ref
                && no_order.execution_context == schedule.execution_context =>
            {
                if !verdict_artifact
                    .source_refs
                    .iter()
                    .any(|reference| reference == &schedule.execution_context)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.no_order_context",
                    ));
                }
            }
            (
                OutcomeExecutionLineage::ReconciledPaper {
                    execution_verdict,
                    commitment,
                    reconciliation,
                },
                ExecutionVerdict::Accepted { execution_context },
            ) if execution_verdict == verdict_ref
                && execution_context == schedule.execution_context =>
            {
                let commitment_artifact = read_artifact(connection, &commitment.artifact_id)?;
                if commitment_artifact.kind != ArtifactKind::ExecutionCommitment {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.commitment_kind",
                    ));
                }
                assert_artifact_from_allowed_purposes(
                    connection,
                    &commitment_artifact,
                    allowed_purposes,
                )?;
                let commitment_payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&commitment_artifact.blob)?)?;
                commitment_payload.validate()?;
                if commitment_payload.execution_context != schedule.execution_context
                    || !commitment_artifact
                        .source_refs
                        .iter()
                        .any(|reference| reference == execution_verdict)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.commitment_lineage",
                    ));
                }

                let reconciliation_artifact =
                    read_artifact(connection, &reconciliation.artifact_id)?;
                if reconciliation_artifact.kind != ArtifactKind::Reconciliation {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.reconciliation_kind",
                    ));
                }
                assert_artifact_from_allowed_purposes(
                    connection,
                    &reconciliation_artifact,
                    allowed_purposes,
                )?;
                let reconciliation_payload: Reconciliation =
                    serde_json::from_slice(&self.read_blob(&reconciliation_artifact.blob)?)?;
                reconciliation_payload.validate()?;
                if reconciliation_payload.commitment != *commitment
                    || !reconciliation_artifact
                        .source_refs
                        .iter()
                        .any(|reference| reference == commitment)
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "outcome_schedule.reconciliation_lineage",
                    ));
                }
            }
            _ => {
                return Err(StoreError::InvalidLearningCommit(
                    "outcome_schedule.execution_lineage",
                ));
            }
        }
        Ok(())
    }
}
