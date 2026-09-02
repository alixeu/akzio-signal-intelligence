#[test]
fn contract_policy_transitions_activate_and_rollback_catalogue_history() {
    let fixture = PolicyCommitFixture::memory();
    let active = contract(&fixture.store, 1);
    let active_installation = fixture
        .store
        .install_active_contract(&active, fixture.now)
        .unwrap();
    let mut candidate = active.clone();
    candidate.version = 2;
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();
    let candidate_installation = fixture
        .store
        .install_candidate_contract(&active.contract_hash, &candidate, fixture.now)
        .unwrap();
    let subject = PolicySubject::Contract(candidate.contract_hash.clone());
    let fresh_permit = |label: &str, now: DateTime<Utc>| {
        let mut workflow = graph();
        workflow.topology_id = format!("contract-policy-{label}");
        let graph_artifact = artifact(
            &fixture.store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&workflow).unwrap(),
            None,
        );
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: workflow.topology_id.clone(),
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        fixture
            .store
            .commit_workflow(&WorkflowCommit {
                run,
                graph: graph_artifact,
                nodes: workflow.nodes,
            })
            .unwrap();
        fixture
            .store
            .claim_next_task(
                &format!("contract-policy-{label}"),
                now,
                Duration::seconds(30),
            )
            .unwrap()
            .unwrap()
            .permit
    };
    let fresh_outcome = |permit: &TaskWritePermit, now: DateTime<Utc>| {
        let mut provenance = fixture.outcome.provenance.clone();
        provenance.producer_contract_hash = permit.contract_hash.clone();
        Artifact::new(
            ArtifactKind::Outcome,
            fixture.outcome.blob.clone(),
            fixture.outcome.producer.clone(),
            ArtifactLifecycle::Canonical,
            provenance,
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            fixture.outcome.source_refs.clone(),
            now,
        )
        .unwrap()
    };
    let promotion_permit = fresh_permit("promotion", fixture.now);
    let promotion_outcome = fresh_outcome(&promotion_permit, fixture.now);
    let promotion_retrospective = retrospective_artifact(
        &fixture.store,
        &promotion_permit,
        &promotion_outcome,
        fixture.now,
    );
    let promotion_retrospective_ref = artifact_ref(&promotion_retrospective);

    let mut promoted_experience: Experience = fixture
        .store
        .read_artifact_payload(&fixture.experience)
        .unwrap();
    promoted_experience.experience_id = akzio_domain::ExperienceId::new();
    promoted_experience.subject = subject.clone();
    promoted_experience.contract_hash = candidate.contract_hash.clone();
    promoted_experience.outcome = artifact_ref(&promotion_outcome);
    promoted_experience.policy_state =
        PolicyState::Contract(akzio_domain::CandidatePolicyState::Canary50);
    promoted_experience.validate().unwrap();
    let promoted_experience_artifact = permit_artifact(
        &fixture.store,
        &promotion_permit,
        ArtifactKind::Experience,
        &promoted_experience,
        vec![
            promoted_experience.decision.clone(),
            promoted_experience.decision_context.clone(),
            promoted_experience.execution_context.clone(),
            promoted_experience.policy_verdict.clone(),
            promoted_experience.outcome.clone(),
            promotion_retrospective_ref.clone(),
        ],
        ArtifactLifecycle::Canonical,
        fixture.now,
    );
    let mut promoted_evaluation: Evaluation = fixture
        .store
        .read_artifact_payload(&fixture.evaluation)
        .unwrap();
    promoted_evaluation.evaluation_id = akzio_domain::EvaluationId::new();
    promoted_evaluation.outcome = artifact_ref(&promotion_outcome);
    promoted_evaluation.experience = artifact_ref(&promoted_experience_artifact);
    promoted_evaluation.validate().unwrap();
    let promoted_evaluation_artifact = permit_artifact(
        &fixture.store,
        &promotion_permit,
        ArtifactKind::Evaluation,
        &promoted_evaluation,
        vec![
            promoted_evaluation.outcome.clone(),
            promoted_evaluation.experience.clone(),
            promotion_retrospective_ref,
        ],
        ArtifactLifecycle::Canonical,
        fixture.now,
    );
    let candidate_policy = CandidatePolicy {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        subject: subject.clone(),
        baseline: artifact_ref(&active_installation.artifact),
        candidate: artifact_ref(&candidate_installation.artifact),
        source_evaluation: artifact_ref(&promoted_evaluation_artifact),
        created_at: fixture.now,
    };
    candidate_policy.validate().unwrap();
    let candidate_policy_artifact = permit_artifact(
        &fixture.store,
        &promotion_permit,
        ArtifactKind::CandidatePolicy,
        &candidate_policy,
        vec![
            candidate_policy.baseline.clone(),
            candidate_policy.candidate.clone(),
            candidate_policy.source_evaluation.clone(),
        ],
        ArtifactLifecycle::Canonical,
        fixture.now,
    );
    let record_canary = |from, to, completed_at| -> StoreResult<()> {
        let permit = fresh_permit(&format!("canary-{from:?}-{to:?}"), completed_at);
        let outcome = fresh_outcome(&permit, completed_at);
        let retrospective = retrospective_artifact(&fixture.store, &permit, &outcome, completed_at);
        let retrospective_ref = artifact_ref(&retrospective);
        let mut experience: Experience =
            fixture.store.read_artifact_payload(&fixture.experience)?;
        experience.experience_id = akzio_domain::ExperienceId::new();
        experience.subject = subject.clone();
        experience.contract_hash = candidate.contract_hash.clone();
        experience.outcome = artifact_ref(&outcome);
        experience.policy_state = PolicyState::Contract(from);
        experience.created_at = completed_at;
        experience.validate()?;
        let experience_artifact = permit_artifact(
            &fixture.store,
            &permit,
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
            ArtifactLifecycle::Canonical,
            completed_at,
        );
        let mut evaluation: Evaluation =
            fixture.store.read_artifact_payload(&fixture.evaluation)?;
        evaluation.evaluation_id = akzio_domain::EvaluationId::new();
        evaluation.outcome = artifact_ref(&outcome);
        evaluation.experience = artifact_ref(&experience_artifact);
        evaluation.created_at = completed_at;
        evaluation.validate()?;
        let evaluation_artifact = permit_artifact(
            &fixture.store,
            &permit,
            ArtifactKind::Evaluation,
            &evaluation,
            vec![
                evaluation.outcome.clone(),
                evaluation.experience.clone(),
                retrospective_ref,
            ],
            ArtifactLifecycle::Canonical,
            completed_at,
        );
        let candidate_policy = CandidatePolicy {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            subject: subject.clone(),
            baseline: artifact_ref(&active_installation.artifact),
            candidate: artifact_ref(&candidate_installation.artifact),
            source_evaluation: artifact_ref(&evaluation_artifact),
            created_at: completed_at,
        };
        candidate_policy.validate()?;
        let candidate_policy_artifact = permit_artifact(
            &fixture.store,
            &permit,
            ArtifactKind::CandidatePolicy,
            &candidate_policy,
            vec![
                candidate_policy.baseline.clone(),
                candidate_policy.candidate.clone(),
                candidate_policy.source_evaluation.clone(),
            ],
            ArtifactLifecycle::Canonical,
            completed_at,
        );
        let transition = PolicyTransition {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from: PolicyState::Contract(from),
            to: PolicyState::Contract(to),
            evaluation: artifact_ref(&evaluation_artifact),
            created_at: completed_at,
        };
        fixture
            .store
            .record_policy_evaluation(&PolicyEvaluationCommit {
                permit,
                final_retrospective: retrospective,
                outcome,
                experience: experience_artifact,
                evaluation: evaluation_artifact,
                candidate_policy: Some(candidate_policy_artifact),
                subject: subject.clone(),
                from: transition.from,
                to: transition.to,
                pair_snapshot: fixture.store.policy_shadow_pair_snapshot(&subject)?,
                transition: Some(transition),
                completed_at,
            })?;
        Ok(())
    };
    record_canary(
        akzio_domain::CandidatePolicyState::Candidate,
        akzio_domain::CandidatePolicyState::Canary10,
        fixture.now,
    )
    .unwrap();
    record_canary(
        akzio_domain::CandidatePolicyState::Canary10,
        akzio_domain::CandidatePolicyState::Canary25,
        fixture.now + Duration::microseconds(1),
    )
    .unwrap();
    record_canary(
        akzio_domain::CandidatePolicyState::Canary25,
        akzio_domain::CandidatePolicyState::Canary50,
        fixture.now + Duration::microseconds(2),
    )
    .unwrap();
    let promote_transition = PolicyTransition {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        transition_id: PolicyTransitionId::new(),
        subject: subject.clone(),
        from: PolicyState::Contract(akzio_domain::CandidatePolicyState::Canary50),
        to: PolicyState::Contract(akzio_domain::CandidatePolicyState::Active),
        evaluation: artifact_ref(&promoted_evaluation_artifact),
        created_at: fixture.now,
    };
    let promoted_outcome_payload: Outcome = fixture
        .store
        .read_artifact_payload(&promotion_outcome)
        .unwrap();
    let schedule_artifact = fixture
        .store
        .artifact(&promoted_outcome_payload.schedule.artifact_id)
        .unwrap();
    let schedule_payload: OutcomeSchedule = fixture
        .store
        .read_artifact_payload(&schedule_artifact)
        .unwrap();
    assert_eq!(
        promoted_experience.outcome,
        artifact_ref(&promotion_outcome)
    );
    assert_eq!(
        promoted_evaluation.outcome,
        artifact_ref(&promotion_outcome)
    );
    assert_eq!(
        promoted_evaluation.experience,
        artifact_ref(&promoted_experience_artifact)
    );
    assert_eq!(promoted_experience.decision, schedule_payload.decision);
    assert_eq!(
        promoted_experience.decision_context,
        schedule_payload.decision_context
    );
    assert_eq!(
        promoted_experience.execution_context,
        schedule_payload.execution_context
    );
    fixture
        .store
        .record_policy_evaluation(&PolicyEvaluationCommit {
            permit: promotion_permit,
            final_retrospective: promotion_retrospective,
            outcome: promotion_outcome,
            experience: promoted_experience_artifact,
            evaluation: promoted_evaluation_artifact,
            candidate_policy: Some(candidate_policy_artifact),
            subject: subject.clone(),
            from: promote_transition.from,
            to: promote_transition.to,
            pair_snapshot: fixture.store.policy_shadow_pair_snapshot(&subject).unwrap(),
            transition: Some(promote_transition),
            completed_at: fixture.now,
        })
        .unwrap();
    assert_eq!(
        fixture
            .store
            .active_contract(&active.purpose)
            .unwrap()
            .unwrap()
            .contract
            .contract_hash,
        candidate.contract_hash
    );

    let rollback_at = fixture.now + Duration::microseconds(4);
    let rollback_permit = fresh_permit("rollback", rollback_at);
    let rollback_outcome = fresh_outcome(&rollback_permit, rollback_at);
    let rollback_retrospective = retrospective_artifact(
        &fixture.store,
        &rollback_permit,
        &rollback_outcome,
        rollback_at,
    );
    let rollback_retrospective_ref = artifact_ref(&rollback_retrospective);
    let mut rollback_experience: Experience = fixture
        .store
        .read_artifact_payload(&fixture.experience)
        .unwrap();
    rollback_experience.experience_id = akzio_domain::ExperienceId::new();
    rollback_experience.subject = subject.clone();
    rollback_experience.contract_hash = candidate.contract_hash.clone();
    rollback_experience.outcome = artifact_ref(&rollback_outcome);
    rollback_experience.policy_state =
        PolicyState::Contract(akzio_domain::CandidatePolicyState::Active);
    rollback_experience.validate().unwrap();
    let rollback_experience_artifact = permit_artifact(
        &fixture.store,
        &rollback_permit,
        ArtifactKind::Experience,
        &rollback_experience,
        vec![
            rollback_experience.decision.clone(),
            rollback_experience.decision_context.clone(),
            rollback_experience.execution_context.clone(),
            rollback_experience.policy_verdict.clone(),
            rollback_experience.outcome.clone(),
            rollback_retrospective_ref.clone(),
        ],
        ArtifactLifecycle::Canonical,
        fixture.now + Duration::microseconds(1),
    );
    let mut rollback_evaluation: Evaluation = fixture
        .store
        .read_artifact_payload(&fixture.evaluation)
        .unwrap();
    rollback_evaluation.evaluation_id = akzio_domain::EvaluationId::new();
    rollback_evaluation.outcome = artifact_ref(&rollback_outcome);
    rollback_evaluation.experience = artifact_ref(&rollback_experience_artifact);
    rollback_evaluation.created_at = rollback_at;
    rollback_evaluation.validate().unwrap();
    let rollback_evaluation_artifact = permit_artifact(
        &fixture.store,
        &rollback_permit,
        ArtifactKind::Evaluation,
        &rollback_evaluation,
        vec![
            rollback_evaluation.outcome.clone(),
            rollback_evaluation.experience.clone(),
            rollback_retrospective_ref,
        ],
        ArtifactLifecycle::Canonical,
        rollback_at,
    );
    let rollback_candidate_policy = CandidatePolicy {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        subject: subject.clone(),
        baseline: artifact_ref(&active_installation.artifact),
        candidate: artifact_ref(&candidate_installation.artifact),
        source_evaluation: artifact_ref(&rollback_evaluation_artifact),
        created_at: rollback_at,
    };
    rollback_candidate_policy.validate().unwrap();
    let rollback_candidate_policy_artifact = permit_artifact(
        &fixture.store,
        &rollback_permit,
        ArtifactKind::CandidatePolicy,
        &rollback_candidate_policy,
        vec![
            rollback_candidate_policy.baseline.clone(),
            rollback_candidate_policy.candidate.clone(),
            rollback_candidate_policy.source_evaluation.clone(),
        ],
        ArtifactLifecycle::Canonical,
        rollback_at,
    );
    let rollback_transition = PolicyTransition {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        transition_id: PolicyTransitionId::new(),
        subject: subject.clone(),
        from: PolicyState::Contract(akzio_domain::CandidatePolicyState::Active),
        to: PolicyState::Contract(akzio_domain::CandidatePolicyState::Candidate),
        evaluation: artifact_ref(&rollback_evaluation_artifact),
        created_at: rollback_at,
    };
    fixture
        .store
        .record_policy_evaluation(&PolicyEvaluationCommit {
            permit: rollback_permit,
            final_retrospective: rollback_retrospective,
            outcome: rollback_outcome,
            experience: rollback_experience_artifact,
            evaluation: rollback_evaluation_artifact,
            candidate_policy: Some(rollback_candidate_policy_artifact),
            subject: subject.clone(),
            from: rollback_transition.from,
            to: rollback_transition.to,
            pair_snapshot: fixture.store.policy_shadow_pair_snapshot(&subject).unwrap(),
            transition: Some(rollback_transition),
            completed_at: rollback_at,
        })
        .unwrap();
    assert_eq!(
        fixture
            .store
            .active_contract(&active.purpose)
            .unwrap()
            .unwrap()
            .contract
            .contract_hash,
        active.contract_hash
    );
    assert_eq!(fixture.store.policy_transitions(&subject).unwrap().len(), 5);
    fixture.store.verify_integrity().unwrap();
}
