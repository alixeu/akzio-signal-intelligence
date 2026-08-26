use akzio_domain::{
    ArtifactLifecycle, ArtifactProvenance, Asset, ContextPolicy, ExecutionPlan, FactorExposure,
    FailureDisposition, HardBlocker, MoneyMicros, NoOrder, OrderIntent, OrderSide, OutputContract,
    PaperApprovalScope, PaperCommitment, PaperCommitmentId, PromptBundle, RetryPolicy,
    RuntimeManifest, TargetPortfolio, TaskBudget, TaskRecipeId, TerminationPolicy, ToolGrant,
    ToolKind, ToolSpec, WeightPpm, WorkflowProposalTask,
};
use chrono::NaiveDate;
use tempfile::tempdir;

use super::*;

fn budget() -> TaskBudget {
    TaskBudget {
        max_input_tokens: 32,
        max_output_tokens: 16,
        max_wall_time_secs: 10,
        max_tool_calls: 1,
    }
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 1,
        initial_backoff_ms: 1,
        retry_transport: true,
        retry_rate_limited: true,
        retry_invalid_output: false,
    }
}

#[test]
fn poisoned_connection_returns_integrity_error() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let connection = store.connection.clone();
    assert!(std::thread::spawn(move || {
        let _guard = connection.lock().unwrap();
        panic!("poison fixture connection");
    })
    .join()
    .is_err());

    assert!(matches!(
        store.metrics(Utc::now()),
        Err(StoreError::Integrity(message)) if message == "store connection poisoned"
    ));
}

fn contract(store: &V2Store, version: u32) -> AgentContract {
    AgentContract::new(
            ContractId::new(),
        version,
        ContractPurpose::new("research.fixture").unwrap(),
        "fixture contract",
        PromptBundle {
            version: 1,
            governance: store.put_bytes(b"fixture governance", "text/plain").unwrap(),
            role: store.put_bytes(b"fixture prompt", "text/plain").unwrap(),
        },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["fixture".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 4096,
                max_tokens: 1024,
                allow_raw_reread: false,
            },
        vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["fixture".to_owned()],
        }],
        vec![ToolSpec {
            name: "read_artifact".to_owned(),
            description: "read fixture artifact".to_owned(),
            kind: ToolKind::ReadEvidence,
            input_schema: store.put_bytes(b"fixture tool schema", "application/json").unwrap(),
            strict: true,
        }],
        OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: store
                    .put_bytes(
                        br#"{"type":"object","properties":{"summary":{"type":"string"}},"required":["summary"],"additionalProperties":false}"#,
                        "application/json",
                    )
                    .unwrap(),
            },
            budget(),
            retry(),
            TerminationPolicy::leaf(),
            FailureDisposition::FailRun,
        )
        .unwrap()
}

#[test]
fn contract_catalogue_rejects_duplicate_or_expanded_installations_and_doctor_corruption() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let active = contract(&store, 1);
    store.install_active_contract(&active, now).unwrap();

    let mut duplicate = active.clone();
    duplicate.responsibility = "same identity, different contract".to_owned();
    duplicate.contract_hash = duplicate.expected_hash().unwrap();
    duplicate.validate().unwrap();
    assert!(matches!(
        store.install_active_contract(&duplicate, now),
        Err(StoreError::DuplicateContractVersion { .. })
    ));

    let mut expanded = active.clone();
    expanded.version = 2;
    expanded
        .context
        .permitted_source_families
        .insert("unapproved".to_owned());
    expanded.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
        context: expanded.context.clone(),
        tool_grants: expanded.tool_grants.clone(),
    };
    expanded.contract_hash = expanded.expected_hash().unwrap();
    expanded.validate().unwrap();
    assert!(matches!(
        store.install_candidate_contract(&active.contract_hash, &expanded, now),
        Err(StoreError::ContractCapabilityExpansion { .. })
    ));

    let mut candidate = active.clone();
    candidate.version = 2;
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();
    let stored_candidate = store
        .install_candidate_contract(&active.contract_hash, &candidate, now)
        .unwrap();
    assert_eq!(stored_candidate.contract, candidate);
    assert_eq!(
        store
            .active_contract(&active.purpose)
            .unwrap()
            .unwrap()
            .contract
            .contract_hash,
        active.contract_hash
    );
    store.verify_integrity().unwrap();

    store
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE rebuild_contract_installations \
                 SET contract_id = ?1 WHERE contract_hash = ?2",
            params!["forged-contract-id", active.contract_hash.as_str()],
        )
        .unwrap();
    assert!(matches!(
        store.verify_integrity(),
        Err(StoreError::Integrity(_))
    ));
}

#[test]
fn observatory_configuration_round_trips_and_clears() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let configuration = serde_json::json!({
        "llm_api_key": "fixture-llm-key",
        "alpaca_api_secret": "fixture-alpaca-secret",
        "model": "fixture-model"
    });

    assert_eq!(
        store
            .observatory_configuration::<serde_json::Value>()
            .unwrap(),
        None
    );
    store.set_observatory_configuration(&configuration).unwrap();
    assert_eq!(
        store
            .observatory_configuration::<serde_json::Value>()
            .unwrap(),
        Some(configuration)
    );
    assert!(store.clear_observatory_configuration().unwrap());
    assert!(!store.clear_observatory_configuration().unwrap());
    assert_eq!(
        store
            .observatory_configuration::<serde_json::Value>()
            .unwrap(),
        None
    );
}

#[test]
fn canonical_contract_upgrade_is_monotonic_bounded_and_preserves_history() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let active = contract(&store, 1);
    store.install_active_contract(&active, now).unwrap();

    let mut upgraded = active.clone();
    upgraded.version = 2;
    upgraded.responsibility = "bounded canonical runtime upgrade".to_owned();
    upgraded.contract_hash = upgraded.expected_hash().unwrap();
    upgraded.validate().unwrap();
    let stored = store
        .install_canonical_contract_upgrade(
            &active.contract_hash,
            &upgraded,
            now + Duration::seconds(1),
        )
        .unwrap();

    assert_eq!(stored.contract, upgraded);
    assert_eq!(
        stored.baseline_contract_hash,
        Some(active.contract_hash.clone())
    );
    assert!(stored.activated_at.is_some());
    assert_eq!(
        store
            .active_contract(&active.purpose)
            .unwrap()
            .unwrap()
            .contract
            .contract_hash,
        upgraded.contract_hash
    );
    assert!(store
        .contract_installation(&active.contract_hash)
        .unwrap()
        .is_some());

    let mut expanded = upgraded.clone();
    expanded.version = 3;
    expanded
        .context
        .permitted_source_families
        .insert("unapproved".to_owned());
    expanded.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
        context: expanded.context.clone(),
        tool_grants: expanded.tool_grants.clone(),
    };
    expanded.contract_hash = expanded.expected_hash().unwrap();
    expanded.validate().unwrap();
    assert!(matches!(
        store.install_canonical_contract_upgrade(
            &upgraded.contract_hash,
            &expanded,
            now + Duration::seconds(2),
        ),
        Err(StoreError::ContractCapabilityExpansion { .. })
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn canonical_contract_upgrade_rejects_nonterminal_tasks_and_lists_blockers() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let active = contract(&store, 1);
    store.install_active_contract(&active, now).unwrap();

    let mut graph = graph();
    graph.nodes[0].contract_hash = Some(active.contract_hash.clone());
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes.clone(),
        })
        .unwrap();

    let mut upgraded = active.clone();
    upgraded.version = 2;
    upgraded.responsibility = "blocked upgrade".to_owned();
    upgraded.contract_hash = upgraded.expected_hash().unwrap();
    let error = store
        .install_canonical_contract_upgrade(
            &active.contract_hash,
            &upgraded,
            now + Duration::seconds(1),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::ContractUpgradeBlocked { active: hash, blockers }
            if hash == active.contract_hash
                && blockers.contains(run.run_id.0.as_str())
                && blockers.contains(graph.nodes[0].task_id.0.as_str())
    ));
}

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
                permit: permit.clone(),
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
            permit: promotion_permit.clone(),
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
            permit: rollback_permit.clone(),
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

fn artifact(
    store: &V2Store,
    kind: ArtifactKind,
    value: &str,
    origin: Option<ArtifactOrigin>,
) -> Artifact {
    artifact_with_refs(store, kind, value, origin, vec![])
}

fn artifact_with_refs(
    store: &V2Store,
    kind: ArtifactKind,
    value: &str,
    origin: Option<ArtifactOrigin>,
    source_refs: Vec<ArtifactRef>,
) -> Artifact {
    let producer_contract_hash = origin
        .as_ref()
        .and_then(|origin| origin.contract_hash.clone());
    Artifact::new(
        kind,
        store
            .put_bytes(value.as_bytes(), "application/json")
            .unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: Utc::now(),
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash,
        },
        origin,
        source_refs,
        Utc::now(),
    )
    .unwrap()
}

fn graph() -> WorkflowGraph {
    WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        nodes: vec![WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash: None,
            objective: "analyze".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 50,
            budget: budget(),
            retry: retry(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        }],
    }
}

fn permit_artifact<T: Serialize>(
    store: &V2Store,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    payload: &T,
    source_refs: Vec<ArtifactRef>,
    lifecycle: ArtifactLifecycle,
    now: DateTime<Utc>,
) -> Artifact {
    Artifact::new(
        kind,
        store.put_json(payload).unwrap(),
        "fixture.policy",
        lifecycle,
        ArtifactProvenance {
            source_family: "fixture.policy".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(permit.run_id.clone()),
            task_id: Some(permit.task_id.clone()),
            attempt_id: Some(permit.attempt_id.clone()),
            contract_hash: permit.contract_hash.clone(),
        }),
        source_refs,
        now,
    )
    .unwrap()
}

struct TaskArtifactFixture {
    _root: tempfile::TempDir,
    store: V2Store,
    run: StoredRun,
    permit: TaskWritePermit,
    now: DateTime<Utc>,
}

fn task_artifact_fixture(purpose: RunPurpose) -> TaskArtifactFixture {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let permit = store
        .claim_next_task("lifecycle-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    TaskArtifactFixture {
        _root: root,
        store,
        run,
        permit,
        now,
    }
}

fn lifecycle_test_artifact(
    fixture: &TaskArtifactFixture,
    lifecycle: ArtifactLifecycle,
    label: &str,
) -> Artifact {
    permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::Decision,
        &serde_json::json!({"label": label}),
        vec![],
        lifecycle,
        fixture.now,
    )
}

#[test]
fn task_artifact_lifecycle_matrix_is_enforced_without_partial_writes() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::PaperDryRun,
        RunPurpose::Replay,
        RunPurpose::Shadow,
    ] {
        for lifecycle in [ArtifactLifecycle::Ephemeral, ArtifactLifecycle::Canonical] {
            let fixture = task_artifact_fixture(purpose);
            let artifact = lifecycle_test_artifact(&fixture, lifecycle, "rejected");
            let event_count = fixture
                .store
                .events_after(&fixture.run.run_id, 0, 100)
                .unwrap()
                .len();

            assert!(matches!(
                fixture.store.write_task_artifact(
                    &fixture.permit,
                    &artifact,
                    LifecycleEventType::ClaimCreated,
                    fixture.now,
                ),
                Err(StoreError::InvalidTaskArtifactLifecycle { purpose: actual, lifecycle: rejected })
                    if actual == purpose && rejected == lifecycle
            ));
            assert!(matches!(
                fixture.store.artifact(&artifact.artifact_id),
                Err(StoreError::MissingArtifact(_))
            ));
            assert_eq!(
                fixture
                    .store
                    .events_after(&fixture.run.run_id, 0, 100)
                    .unwrap()
                    .len(),
                event_count
            );
            fixture.store.verify_integrity().unwrap();
        }
    }

    for purpose in [
        RunPurpose::Debug,
        RunPurpose::Paper,
        RunPurpose::PaperDryRun,
        RunPurpose::Replay,
        RunPurpose::Shadow,
    ] {
        let fixture = task_artifact_fixture(purpose);
        let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::RunScoped, "accepted");
        fixture
            .store
            .write_task_artifact(
                &fixture.permit,
                &artifact,
                LifecycleEventType::ClaimCreated,
                fixture.now,
            )
            .unwrap();
        assert_eq!(
            fixture.store.artifact(&artifact.artifact_id).unwrap(),
            artifact
        );
        fixture.store.verify_integrity().unwrap();
    }

    let fixture = task_artifact_fixture(RunPurpose::Paper);
    let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "paper");
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &artifact,
            LifecycleEventType::ClaimCreated,
            fixture.now,
        )
        .unwrap();
    assert_eq!(
        fixture.store.artifact(&artifact.artifact_id).unwrap(),
        artifact
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn attempt_commit_lifecycle_rejection_is_atomic_and_paper_canonical_is_allowed() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::PaperDryRun,
        RunPurpose::Replay,
        RunPurpose::Shadow,
    ] {
        let fixture = task_artifact_fixture(purpose);
        let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "rejected");
        let event_count = fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len();

        assert!(matches!(
            fixture.store.commit_attempt(
                &fixture.permit,
                std::slice::from_ref(&artifact),
                TaskStatus::Succeeded,
                fixture.now,
            ),
            Err(StoreError::InvalidTaskArtifactLifecycle { purpose: actual, lifecycle: ArtifactLifecycle::Canonical })
                if actual == purpose
        ));
        assert!(matches!(
            fixture.store.artifact(&artifact.artifact_id),
            Err(StoreError::MissingArtifact(_))
        ));
        assert!(matches!(
            fixture
                .store
                .committed_task_outputs(&fixture.run.run_id, &fixture.permit.task_id),
            Err(StoreError::CommittedOutputTask { .. })
        ));
        assert_eq!(
            fixture
                .store
                .events_after(&fixture.run.run_id, 0, 100)
                .unwrap()
                .len(),
            event_count
        );
        assert_eq!(
            fixture
                .store
                .workflow_snapshot(&fixture.run.run_id)
                .unwrap()
                .tasks[0]
                .status,
            TaskStatus::Running
        );
        fixture.store.verify_integrity().unwrap();
    }

    let fixture = task_artifact_fixture(RunPurpose::Paper);
    let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "paper");
    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&artifact),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .committed_task_outputs(&fixture.run.run_id, &fixture.permit.task_id)
            .unwrap(),
        vec![artifact]
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn stale_permit_rejects_before_task_artifact_lifecycle() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let stale = fixture.permit.clone();
    fixture
        .store
        .recover_expired_tasks(fixture.now + Duration::seconds(31))
        .unwrap();
    let artifact = lifecycle_test_artifact(&fixture, ArtifactLifecycle::Canonical, "stale");

    assert!(matches!(
        fixture.store.write_task_artifact(
            &stale,
            &artifact,
            LifecycleEventType::ClaimCreated,
            fixture.now,
        ),
        Err(StoreError::StalePermit(_))
    ));
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn bootstrap_freeze_state_remains_outside_task_artifact_firewall() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let freeze = fixture
        .store
        .write_freeze_state(true, "lifecycle firewall test", fixture.now)
        .unwrap();

    assert_eq!(freeze.kind, ArtifactKind::FreezeState);
    assert_eq!(freeze.lifecycle, ArtifactLifecycle::Canonical);
    assert_eq!(fixture.store.artifact(&freeze.artifact_id).unwrap(), freeze);
    fixture.store.verify_integrity().unwrap();
}

fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn reserve_approved_test_session(
    store: &V2Store,
    lease: &DaemonLease,
    reservation: &SessionReservation,
) -> SessionSlotReservation {
    reserve_approved_test_session_with_limits(
        store,
        lease,
        reservation,
        MoneyMicros::from_usd_cents(100_000),
        reservation.reserved_at + Duration::hours(8),
    )
}

fn reserve_approved_test_session_with_limits(
    store: &V2Store,
    lease: &DaemonLease,
    reservation: &SessionReservation,
    maximum_notional: MoneyMicros,
    expires_at: DateTime<Utc>,
) -> SessionSlotReservation {
    let now = reservation.reserved_at;
    let run = &reservation.workflow.run;
    let provenance = ArtifactProvenance {
        source_family: "fixture.paper_approval".to_owned(),
        observed_at: None,
        retrieved_at: now,
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: None,
    };
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: run.topology_id.clone(),
        tasks: BTreeMap::new(),
        stop_reason: Some("fixture approved Paper session".to_owned()),
    };
    let proposal = Artifact::new(
        ArtifactKind::WorkflowProposal,
        store.put_json(&proposal_payload).unwrap(),
        "runtime.paper_provisioning",
        ArtifactLifecycle::RunScoped,
        provenance.clone(),
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    let session = NaiveDate::parse_from_str(&reservation.session_key, "%Y-%m-%d").unwrap();
    let manifest_payload = RuntimeManifest {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        code_revision: "fixture-revision".to_owned(),
        cargo_lock_hash: ContentHash::of_bytes(b"fixture-cargo"),
        config_hash: ContentHash::of_bytes(b"fixture-config"),
        provider_id: "fixture-provider".to_owned(),
        model_id: "fixture-model".to_owned(),
        prompt_hash: ContentHash::of_bytes(b"fixture-prompt"),
        contract_hash: ContentHash::of_bytes(b"fixture-contract"),
        topology_hash: ContentHash::of_bytes(b"fixture-topology"),
        decision_policy_hash: ContentHash::of_bytes(b"fixture-decision"),
        execution_policy_hash: ContentHash::of_bytes(b"fixture-execution"),
        evaluation_policy_hash: ContentHash::of_bytes(b"fixture-evaluation"),
        market_data_feed: "iex".to_owned(),
        broker_account_id: "fixture-account".to_owned(),
        maximum_notional,
        allowed_session_start: session,
        allowed_session_end: session,
        expires_at,
        created_at: now,
    };
    let manifest_hash = manifest_payload.manifest_hash().unwrap();
    let manifest = Artifact::new(
        ArtifactKind::RuntimeManifest,
        store.put_json(&manifest_payload).unwrap(),
        "runtime.manifest",
        ArtifactLifecycle::Canonical,
        provenance.clone(),
        None,
        vec![],
        now,
    )
    .unwrap();
    let mut approval_payload = PaperLaunchApproval {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        operator_identity: "fixture-operator".to_owned(),
        runtime_manifest: artifact_ref(&manifest),
        runtime_manifest_hash: manifest_hash,
        scope: PaperApprovalScope::Canary,
        reason: "fixture approval".to_owned(),
        approved_at: now,
        expires_at: manifest_payload.expires_at,
        approval_hash: ContentHash::of_bytes(b"pending"),
    };
    approval_payload.approval_hash = approval_payload.unsigned_hash().unwrap();
    let approval = Artifact::new(
        ArtifactKind::PaperLaunchApproval,
        store.put_json(&approval_payload).unwrap(),
        "operator.paper_approval",
        ArtifactLifecycle::Canonical,
        provenance,
        None,
        vec![approval_payload.runtime_manifest.clone()],
        now,
    )
    .unwrap();
    store
        .reserve_paper_session_with_approval(lease, reservation, &proposal, &manifest, &approval)
        .unwrap()
}

fn valid_execution_commitment(
    store: &V2Store,
    permit: &TaskWritePermit,
    session_key: &str,
    now: DateTime<Utc>,
) -> Artifact {
    let source = |kind, name: &'static [u8]| {
        let artifact = permit_artifact(
            store,
            permit,
            kind,
            &serde_json::json!({"fixture": String::from_utf8_lossy(name)}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        store
            .write_task_artifact(
                permit,
                &artifact,
                LifecycleEventType::FixtureSourceCreated,
                now,
            )
            .unwrap();
        artifact_ref(&artifact)
    };
    let decision_context = source(ArtifactKind::DecisionContext, b"decision-context");
    let account_snapshot = source(ArtifactKind::NormalizedEvidence, b"account");
    let quote_snapshot = source(ArtifactKind::NormalizedEvidence, b"quote");
    let market_clock_snapshot = source(ArtifactKind::NormalizedEvidence, b"market-clock");

    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Qqq, WeightPpm(100_000));
    let mut plan_payload = ExecutionPlan {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        decision_context: decision_context.clone(),
        account_snapshot: account_snapshot.clone(),
        quote_snapshot: quote_snapshot.clone(),
        market_clock_snapshot: market_clock_snapshot.clone(),
        policy_hash: ContentHash::of_bytes(b"fixture-policy"),
        maximum_total_notional: MoneyMicros::from_usd_cents(100_000),
        target: target.clone(),
        orders: vec![OrderIntent {
            asset: Asset::Qqq,
            side: OrderSide::Buy,
            notional: MoneyMicros::from_usd_cents(10_000),
            limit_price: MoneyMicros::from_usd_cents(5_000),
        }],
        gross_exposure_ppm: 100_000,
        net_exposure_ppm: 100_000,
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        turnover_ppm: 100_000,
        broker_session: session_key.to_owned(),
        created_at: now,
        plan_hash: ContentHash::of_bytes(b"pending"),
    };
    plan_payload.refresh_hash().unwrap();
    let plan_hash = plan_payload.plan_hash.clone();
    let plan = permit_artifact(
        store,
        permit,
        ArtifactKind::ExecutionPlan,
        &plan_payload,
        vec![
            decision_context.clone(),
            account_snapshot.clone(),
            quote_snapshot.clone(),
            market_clock_snapshot.clone(),
        ],
        ArtifactLifecycle::RunScoped,
        now,
    );
    store
        .write_task_artifact(permit, &plan, LifecycleEventType::ExecutionPlanCreated, now)
        .unwrap();
    let plan_ref = artifact_ref(&plan);
    let context = permit_artifact(
        store,
        permit,
        ArtifactKind::ExecutionContext,
        &ExecutionContext {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            run_id: permit.run_id.clone(),
            decision_context: decision_context.clone(),
            account_snapshot: Some(account_snapshot.clone()),
            quote_snapshot: Some(quote_snapshot.clone()),
            market_clock_snapshot: Some(market_clock_snapshot.clone()),
            execution_plan: Some(plan_ref.clone()),
            factor_exposure: Some(plan_payload.factor_exposure.clone()),
            turnover_ppm: Some(plan_payload.turnover_ppm),
            plan_hash: Some(plan_hash.clone()),
            broker_session: Some(session_key.to_owned()),
            frozen: false,
            created_at: now,
        },
        vec![
            decision_context,
            account_snapshot,
            quote_snapshot,
            market_clock_snapshot,
            plan_ref,
        ],
        ArtifactLifecycle::RunScoped,
        now,
    );
    store
        .write_task_artifact(
            permit,
            &context,
            LifecycleEventType::ExecutionContextCreated,
            now,
        )
        .unwrap();
    let context_ref = artifact_ref(&context);
    let verdict = permit_artifact(
        store,
        permit,
        ArtifactKind::ExecutionVerdict,
        &ExecutionVerdict::Accepted {
            execution_context: context_ref.clone(),
        },
        vec![context_ref.clone()],
        ArtifactLifecycle::RunScoped,
        now,
    );
    store
        .write_task_artifact(
            permit,
            &verdict,
            LifecycleEventType::ExecutionVerdictCreated,
            now,
        )
        .unwrap();
    permit_artifact(
        store,
        permit,
        ArtifactKind::ExecutionCommitment,
        &PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: context_ref.clone(),
            plan_hash,
            broker_session: session_key.to_owned(),
            client_order_ids: std::collections::BTreeMap::from([(
                Asset::Qqq,
                "fixture-order".to_owned(),
            )]),
            created_at: now,
        },
        vec![artifact_ref(&verdict), context_ref],
        ArtifactLifecycle::Canonical,
        now,
    )
}

struct ExecutionCommitFixture {
    _root: tempfile::TempDir,
    store: V2Store,
    lease: DaemonLease,
    permit: TaskWritePermit,
    commitment: Artifact,
    now: DateTime<Utc>,
}

fn execution_commit_fixture() -> ExecutionCommitFixture {
    execution_commit_fixture_with_approval(None)
}

fn approved_execution_commit_fixture(
    maximum_notional: MoneyMicros,
    valid_for: Duration,
) -> ExecutionCommitFixture {
    execution_commit_fixture_with_approval(Some((maximum_notional, valid_for)))
}

fn execution_commit_fixture_with_approval(
    approval: Option<(MoneyMicros, Duration)>,
) -> ExecutionCommitFixture {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease(
            "scheduler",
            "fixture-daemon",
            now,
            now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let session_key = if approval.is_some() {
        "2026-08-25"
    } else {
        "paper:fixture"
    };
    let reservation = SessionReservation {
        session_key: session_key.to_owned(),
        workflow: WorkflowCommit {
            run: StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        },
        setup_artifacts: vec![],
        reserved_at: now,
    };
    if let Some((maximum_notional, valid_for)) = approval {
        reserve_approved_test_session_with_limits(
            &store,
            &lease,
            &reservation,
            maximum_notional,
            now + valid_for,
        );
    } else {
        store.reserve_session_slot(&lease, &reservation).unwrap();
    }
    let permit = store
        .claim_next_task("fixture-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let commitment = valid_execution_commitment(&store, &permit, session_key, now);
    ExecutionCommitFixture {
        _root: root,
        store,
        lease,
        permit,
        commitment,
        now,
    }
}

struct PolicyCommitFixture {
    _root: tempfile::TempDir,
    store: V2Store,
    run: StoredRun,
    permit: TaskWritePermit,
    subject: PolicySubject,
    outcome: Artifact,
    final_retrospective: Artifact,
    experience: Artifact,
    evaluation: Artifact,
    candidate_policy: Option<Artifact>,
    transition: PolicyTransition,
    seed_artifact_id: ArtifactId,
    now: DateTime<Utc>,
}

impl PolicyCommitFixture {
    fn memory() -> Self {
        Self::new(false)
    }

    fn topology() -> Self {
        Self::new(true)
    }

    fn new(with_candidate: bool) -> Self {
        let root = tempdir().unwrap();
        let store = V2Store::open(root.path()).unwrap();
        let now = Utc::now();

        let mut paper_graph = graph();
        paper_graph.topology_id = "policy-paper".to_owned();
        let seed = paper_graph.nodes[0].clone();
        let mut evaluation_node = seed.clone();
        evaluation_node.task_id = TaskId::new();
        evaluation_node.dependencies = vec![seed.task_id.clone()];
        evaluation_node.objective = "evaluate policy".to_owned();
        paper_graph.nodes = vec![seed, evaluation_node];
        paper_graph.validate().unwrap();
        let paper_graph_artifact = artifact(
            &store,
            ArtifactKind::WorkflowGraph,
            &serde_json::to_string(&paper_graph).unwrap(),
            None,
        );
        let paper_graph_ref = artifact_ref(&paper_graph_artifact);
        let run = StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: paper_graph.topology_id.clone(),
            graph_artifact_id: paper_graph_artifact.artifact_id.clone(),
            created_at: now,
        };
        store
            .commit_workflow(&WorkflowCommit {
                run: run.clone(),
                graph: paper_graph_artifact,
                nodes: paper_graph.nodes,
            })
            .unwrap();

        let seed_permit = store
            .claim_next_task("policy-seed", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;
        let normalized = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::NormalizedEvidence,
            &serde_json::json!({"normalized": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let decision = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::Decision,
            &serde_json::json!({"decision": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let decision_context = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::DecisionContext,
            &serde_json::json!({"context": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let execution_context = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::ExecutionContext,
            &serde_json::json!({"execution": true}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let verdict_payload = ExecutionVerdict::NoOrder {
            no_order: akzio_domain::NoOrder {
                execution_context: artifact_ref(&execution_context),
                blockers: vec![akzio_domain::HardBlocker::Frozen],
                created_at: now,
            },
        };
        let verdict = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::ExecutionVerdict,
            &verdict_payload,
            vec![artifact_ref(&execution_context)],
            ArtifactLifecycle::RunScoped,
            now,
        );
        let outcome_id = akzio_domain::OutcomeId::new();
        let schedule_payload = OutcomeSchedule {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id: outcome_id.clone(),
            decision: artifact_ref(&decision),
            decision_context: artifact_ref(&decision_context),
            execution_context: artifact_ref(&execution_context),
            execution: OutcomeExecutionLineage::NoOrder {
                execution_verdict: artifact_ref(&verdict),
            },
            baseline_trading_day: now.date_naive(),
            created_at: now,
        };
        let schedule = permit_artifact(
            &store,
            &seed_permit,
            ArtifactKind::OutcomeSchedule,
            &schedule_payload,
            vec![
                schedule_payload.decision.clone(),
                schedule_payload.decision_context.clone(),
                schedule_payload.execution_context.clone(),
                artifact_ref(&verdict),
            ],
            ArtifactLifecycle::Canonical,
            now,
        );
        store
            .commit_attempt(
                &seed_permit,
                &[
                    normalized.clone(),
                    decision.clone(),
                    decision_context.clone(),
                    execution_context.clone(),
                    verdict.clone(),
                    schedule.clone(),
                ],
                TaskStatus::Succeeded,
                now,
            )
            .unwrap();

        let permit = store
            .claim_next_task("policy-evaluation", now, Duration::seconds(30))
            .unwrap()
            .unwrap()
            .permit;

        let candidate_graph = if with_candidate {
            let mut candidate_graph = graph();
            candidate_graph.topology_id = "policy-shadow-candidate".to_owned();
            let candidate_graph_artifact = artifact(
                &store,
                ArtifactKind::WorkflowGraph,
                &serde_json::to_string(&candidate_graph).unwrap(),
                None,
            );
            let reference = artifact_ref(&candidate_graph_artifact);
            let candidate_run = StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Shadow,
                topology_id: candidate_graph.topology_id.clone(),
                graph_artifact_id: candidate_graph_artifact.artifact_id.clone(),
                created_at: now,
            };
            store
                .commit_workflow(&WorkflowCommit {
                    run: candidate_run,
                    graph: candidate_graph_artifact,
                    nodes: candidate_graph.nodes,
                })
                .unwrap();
            Some((reference, candidate_graph.topology_id))
        } else {
            None
        };
        let subject = candidate_graph.as_ref().map_or_else(
            || PolicySubject::Memory(akzio_domain::MemoryId::new()),
            |(_, topology_id)| {
                PolicySubject::Topology(akzio_domain::TopologyId(topology_id.clone()))
            },
        );
        let from = subject.initial_state();
        let to = match subject {
            PolicySubject::Memory(_) => PolicyState::Memory(akzio_domain::MemoryLifecycle::Active),
            PolicySubject::Topology(_) => {
                PolicyState::Topology(akzio_domain::CandidatePolicyState::Canary10)
            }
            PolicySubject::Contract(_) => unreachable!(),
        };
        let outcome_payload = Outcome {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            outcome_id,
            schedule: artifact_ref(&schedule),
            market_evidence: vec![artifact_ref(&normalized)],
            windows: OutcomeHorizon::ALL
                .into_iter()
                .map(|horizon| akzio_domain::OutcomeWindow {
                    horizon,
                    observed_trading_day: now.date_naive()
                        + chrono::Days::new(u64::from(horizon.trading_days())),
                    portfolio_return_ppm: 1,
                    benchmark_return_ppm: 0,
                    transaction_cost_ppm: 0,
                    slippage_ppm: 0,
                    utility_ppm: 1,
                    calibration_ppm: Some(1_000_000),
                    evidence_completeness_ppm: 1_000_000,
                    risk_recall_ppm: Some(1_000_000),
                })
                .collect(),
            sealed_at: Some(now),
        };
        let outcome = permit_artifact(
            &store,
            &permit,
            ArtifactKind::Outcome,
            &outcome_payload,
            vec![artifact_ref(&schedule), artifact_ref(&normalized)],
            ArtifactLifecycle::Canonical,
            now,
        );
        let final_retrospective = retrospective_artifact(&store, &permit, &outcome, now);
        let retrospective_ref = artifact_ref(&final_retrospective);
        let experience_payload = Experience {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            experience_id: akzio_domain::ExperienceId::new(),
            subject: subject.clone(),
            hypothesis_id: "fixture".to_owned(),
            decision: artifact_ref(&decision),
            decision_context: artifact_ref(&decision_context),
            execution_context: artifact_ref(&execution_context),
            policy_verdict: artifact_ref(&verdict),
            outcome: artifact_ref(&outcome),
            contract_hash: ContentHash::of_bytes(b"fixture-contract"),
            topology_id: match &subject {
                PolicySubject::Topology(topology_id) => topology_id.clone(),
                _ => akzio_domain::TopologyId("fixture-topology".to_owned()),
            },
            policy_state: from,
            created_at: now,
        };
        let experience = permit_artifact(
            &store,
            &permit,
            ArtifactKind::Experience,
            &experience_payload,
            vec![
                experience_payload.decision.clone(),
                experience_payload.decision_context.clone(),
                experience_payload.execution_context.clone(),
                experience_payload.policy_verdict.clone(),
                experience_payload.outcome.clone(),
                retrospective_ref.clone(),
            ],
            ArtifactLifecycle::Canonical,
            now,
        );
        let evaluation_payload = Evaluation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            evaluation_id: akzio_domain::EvaluationId::new(),
            outcome: artifact_ref(&outcome),
            experience: artifact_ref(&experience),
            marginal_utility_ppm: 1,
            token_cost: Some(1),
            latency_millis: Some(1),
            created_at: now,
        };
        let evaluation = permit_artifact(
            &store,
            &permit,
            ArtifactKind::Evaluation,
            &evaluation_payload,
            vec![
                artifact_ref(&outcome),
                artifact_ref(&experience),
                retrospective_ref,
            ],
            ArtifactLifecycle::Canonical,
            now,
        );
        let candidate_policy = candidate_graph.map(|(candidate, _)| {
            let payload = CandidatePolicy {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                subject: subject.clone(),
                baseline: paper_graph_ref,
                candidate,
                source_evaluation: artifact_ref(&evaluation),
                created_at: now,
            };
            permit_artifact(
                &store,
                &permit,
                ArtifactKind::CandidatePolicy,
                &payload,
                vec![
                    payload.baseline.clone(),
                    payload.candidate.clone(),
                    payload.source_evaluation.clone(),
                ],
                ArtifactLifecycle::Canonical,
                now,
            )
        });
        let transition = PolicyTransition {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from,
            to,
            evaluation: artifact_ref(&evaluation),
            created_at: now,
        };

        Self {
            _root: root,
            store,
            run,
            permit,
            subject,
            outcome,
            final_retrospective,
            experience,
            evaluation,
            candidate_policy,
            transition,
            seed_artifact_id: decision.artifact_id,
            now,
        }
    }

    fn commit(&self, pair_snapshot: PolicyShadowPairSnapshot) -> PolicyEvaluationCommit {
        PolicyEvaluationCommit {
            permit: self.permit.clone(),
            outcome: self.outcome.clone(),
            final_retrospective: self.final_retrospective.clone(),
            experience: self.experience.clone(),
            evaluation: self.evaluation.clone(),
            candidate_policy: self.candidate_policy.clone(),
            subject: self.subject.clone(),
            from: self.transition.from,
            to: self.transition.to,
            pair_snapshot,
            transition: Some(self.transition.clone()),
            completed_at: self.now,
        }
    }

    fn insert_pair(
        &self,
        label: &str,
        horizon: OutcomeHorizon,
        completed_at: DateTime<Utc>,
    ) -> i64 {
        let pair_key = ContentHash::of_bytes(label.as_bytes());
        let mut connection = self.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let cursor = append_event(
            &transaction,
            &self.run.run_id,
            Some(&self.permit.task_id),
            Some(&self.permit.attempt_id),
            LifecycleEventType::ShadowPairCompleted,
            Some(&self.seed_artifact_id),
            completed_at,
        )
        .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_shadow_pairs
                       (pair_key, subject_id, subject_json, parent_decision_artifact_id,
                        execution_context_artifact_id, candidate_decision_artifact_id,
                        candidate_contract_hash, candidate_topology_id, horizon,
                        parent_outcome_artifact_id, candidate_outcome_artifact_id,
                        completed_at, pair_event_cursor)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"#,
                params![
                    pair_key.as_str(),
                    self.subject.subject_id(),
                    serde_json::to_string(&self.subject).unwrap(),
                    self.seed_artifact_id.0.as_str(),
                    self.seed_artifact_id.0.as_str(),
                    self.seed_artifact_id.0.as_str(),
                    ContentHash::of_bytes(b"fixture-candidate-contract").as_str(),
                    "fixture-candidate-topology",
                    enum_name(horizon),
                    self.seed_artifact_id.0.as_str(),
                    self.seed_artifact_id.0.as_str(),
                    completed_at.to_rfc3339(),
                    cursor,
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
        cursor
    }
}

#[test]
fn workflow_commit_accepts_out_of_order_nodes_and_preserves_dependencies() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut graph = graph();
    let parent = graph.nodes[0].clone();
    let mut child = parent.clone();
    child.task_id = TaskId::new();
    child.objective = "dependent analysis".to_owned();
    child.dependencies = vec![parent.task_id.clone()];
    graph.nodes = vec![child, parent.clone()];
    graph.validate().unwrap();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.node.task_id, parent.task_id);
    store.verify_integrity().unwrap();
}

#[test]
fn retry_and_cancellation_are_durable_and_fenced() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut graph = graph();
    graph.nodes[0].retry.max_attempts = 2;
    graph.nodes[0].retry.initial_backoff_ms = 0;
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let first = store
        .claim_next_task("worker-a", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .retry_task(&first.permit, Utc::now(), Utc::now())
            .unwrap(),
        RetryTaskResult::Requeued
    );
    let second = store
        .claim_next_task("worker-b", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_ne!(first.permit.attempt_id, second.permit.attempt_id);
    assert!(store
        .request_run_cancel(&run.run_id, "operator", Utc::now())
        .unwrap());
    assert!(store.run_cancel_requested(&run.run_id).unwrap());
    assert!(matches!(
        store.finish_task(&first.permit, TaskStatus::Cancelled, Utc::now()),
        Err(StoreError::StalePermit(_))
    ));
    store
        .finish_task(&second.permit, TaskStatus::Cancelled, Utc::now())
        .unwrap();
    let events = store.events_after(&run.run_id, 0, 100).unwrap();
    assert!(events
        .iter()
        .any(|event| event.event_type == "task.retry_scheduled"));
    assert!(events
        .iter()
        .any(|event| event.event_type == "run.cancel_requested"));
    store.verify_integrity().unwrap();
}

#[test]
fn workflow_snapshot_ignores_dependency_ordering() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut graph = graph();
    let mut first = graph.nodes.remove(0);
    first.task_id = TaskId("task-b".to_owned());
    let mut second = first.clone();
    second.task_id = TaskId("task-a".to_owned());
    second.recipe_id = TaskRecipeId::new("research.synthesizer").unwrap();
    let mut child = first.clone();
    child.task_id = TaskId("task-c".to_owned());
    child.recipe_id = TaskRecipeId::new("gate.decision").unwrap();
    child.dependencies = vec![first.task_id.clone(), second.task_id.clone()];
    graph.nodes = vec![first.clone(), second.clone(), child.clone()];
    graph.validate().unwrap();

    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();

    let snapshot = store.workflow_snapshot(&run.run_id).unwrap();
    let stored_child = snapshot
        .tasks
        .iter()
        .find(|task| task.node.task_id == child.task_id)
        .unwrap();
    assert_eq!(
        stored_child.node.dependencies,
        vec![second.task_id, first.task_id]
    );
}

#[test]
fn workflow_commit_is_atomic_and_claim_yields_a_permit() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes.clone(),
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run_id, run.run_id);
    assert_eq!(claimed.node.task_id, graph.nodes[0].task_id);
    assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 2);
    store.verify_integrity().unwrap();
}

#[test]
fn attempt_commit_is_atomic_with_outputs_and_terminal_event() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let turn = artifact(
        &store,
        ArtifactKind::AgentTurn,
        "intermediate turn",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
    );
    store
        .write_task_artifact(
            &claimed.permit,
            &turn,
            LifecycleEventType::AgentTurn,
            Utc::now(),
        )
        .unwrap();
    assert!(matches!(
        store.committed_attempt_outputs(&claimed.permit.task_id, &claimed.permit.attempt_id),
        Err(StoreError::CommittedOutputAttempt { .. })
    ));
    let evidence = artifact(
        &store,
        ArtifactKind::NormalizedEvidence,
        "claim evidence",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
    );
    let output = artifact_with_refs(
        &store,
        ArtifactKind::Claim,
        "claim",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
        vec![artifact_ref(&evidence)],
    );

    store
        .commit_attempt(
            &claimed.permit,
            &[evidence.clone(), output.clone()],
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();

    assert_eq!(
        store
            .committed_attempt_outputs(&claimed.permit.task_id, &claimed.permit.attempt_id)
            .unwrap(),
        vec![evidence.clone(), output.clone()]
    );
    assert_eq!(
        store
            .committed_task_outputs(&run.run_id, &claimed.permit.task_id)
            .unwrap(),
        vec![evidence, output]
    );
    assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 6);
    assert!(store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .is_none());
    store.verify_integrity().unwrap();
}

#[test]
fn attempt_commit_resolves_same_batch_evidence_closure_before_persisting() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let origin = Some(ArtifactOrigin {
        run_id: Some(claimed.permit.run_id.clone()),
        task_id: Some(claimed.permit.task_id.clone()),
        attempt_id: Some(claimed.permit.attempt_id.clone()),
        contract_hash: None,
    });
    let raw = artifact(&store, ArtifactKind::RawEvidence, "raw", origin.clone());
    let normalized = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store.put_bytes(b"normalized", "application/json").unwrap(),
        "fixture.normalized",
        ArtifactLifecycle::RunScoped,
        raw.provenance.clone(),
        origin.clone(),
        vec![ArtifactRef {
            artifact_id: raw.artifact_id.clone(),
            kind: ArtifactKind::RawEvidence,
        }],
        Utc::now(),
    )
    .unwrap();
    let missing = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store.put_bytes(b"missing", "application/json").unwrap(),
        "fixture.normalized",
        ArtifactLifecycle::RunScoped,
        raw.provenance.clone(),
        origin,
        vec![ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"missing raw")),
            kind: ArtifactKind::RawEvidence,
        }],
        Utc::now(),
    )
    .unwrap();

    assert!(matches!(
        store.commit_attempt(
            &claimed.permit,
            std::slice::from_ref(&missing),
            TaskStatus::Succeeded,
            Utc::now(),
        ),
        Err(StoreError::InvalidArtifactClosure(_))
    ));
    assert!(matches!(
        store.artifact(&missing.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    store
        .commit_attempt(
            &claimed.permit,
            &[normalized.clone(), raw.clone()],
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();
    assert_eq!(
        store
            .committed_task_outputs(&run.run_id, &claimed.permit.task_id)
            .unwrap(),
        vec![normalized, raw]
    );
    store.verify_integrity().unwrap();
}

#[test]
fn attempt_commit_rolls_back_when_terminal_event_write_fails() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let evidence = artifact(
        &store,
        ArtifactKind::NormalizedEvidence,
        "claim evidence",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
    );
    let output = artifact_with_refs(
        &store,
        ArtifactKind::Claim,
        "claim",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
        vec![artifact_ref(&evidence)],
    );
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_terminal_event BEFORE INSERT ON rebuild_events
                     WHEN NEW.event_type = 'task.succeeded'
                     BEGIN SELECT RAISE(ABORT, 'injected terminal event failure'); END;",
            )
            .unwrap();
    }
    assert!(matches!(
        store.commit_attempt(
            &claimed.permit,
            &[evidence.clone(), output.clone()],
            TaskStatus::Succeeded,
            Utc::now()
        ),
        Err(StoreError::Sql(_))
    ));
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_terminal_event;")
            .unwrap();
    }
    assert!(matches!(
        store.artifact(&output.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert_eq!(store.events_after(&run.run_id, 0, 10).unwrap().len(), 2);
    store
        .commit_attempt(
            &claimed.permit,
            &[evidence, output],
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();
    store.verify_integrity().unwrap();
}

#[test]
fn workflow_patch_rolls_back_proposal_graph_tasks_events_and_planner_completion() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let planner_contract = ContentHash::of_bytes(b"planner-contract");
    let planner = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("research.planner").unwrap(),
        contract_hash: Some(planner_contract.clone()),
        objective: "plan".to_owned(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 90,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let planner_task_id = planner.task_id.clone();
    let evidence = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("gate.evidence").unwrap(),
        contract_hash: None,
        objective: "evidence gate".to_owned(),
        dependencies: vec![planner.task_id.clone()],
        input_artifacts: vec![],
        priority: 80,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let decision = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("gate.decision").unwrap(),
        contract_hash: None,
        objective: "decision gate".to_owned(),
        dependencies: vec![evidence.task_id.clone()],
        input_artifacts: vec![],
        priority: 70,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        nodes: vec![planner.clone(), evidence.clone(), decision.clone()],
    };
    graph.validate().unwrap();
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&graph).unwrap(),
        "runtime.workflow",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.runtime".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![],
        now,
    )
    .unwrap();
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact.clone(),
            nodes: graph.nodes.clone(),
        })
        .unwrap();
    let claimed = store
        .claim_next_task("planner-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let evidence_need = artifact(
        &store,
        ArtifactKind::EvidenceNeed,
        "evidence need",
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: Some(planner.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
    );
    let evidence_need_ref = ArtifactRef {
        artifact_id: evidence_need.artifact_id.clone(),
        kind: ArtifactKind::EvidenceNeed,
    };
    let planner_output = artifact(
        &store,
        ArtifactKind::WorkflowProposalDraft,
        "planner output",
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: Some(planner.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
    );

    let proposal = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: std::collections::BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyse".to_owned(),
                depends_on: vec![],
                priority: 60,
                evidence_needs: vec![evidence_need_ref.clone()],
            },
        )]),
        stop_reason: None,
    };
    let proposal_artifact = Artifact::new(
        ArtifactKind::WorkflowProposal,
        store.put_json(&proposal).unwrap(),
        "agent.planner",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.agent".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: Some(planner_contract),
        },
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: Some(planner.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
        vec![
            ArtifactRef {
                artifact_id: planner_output.artifact_id.clone(),
                kind: ArtifactKind::WorkflowProposalDraft,
            },
            evidence_need_ref.clone(),
        ],
        now,
    )
    .unwrap();
    let added = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
        contract_hash: Some(ContentHash::of_bytes(b"analyst-contract")),
        objective: "analyse".to_owned(),
        dependencies: vec![evidence.task_id.clone()],
        input_artifacts: vec![evidence_need_ref.clone()],
        priority: 60,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let mut updated_evidence = evidence;
    updated_evidence.input_artifacts = vec![evidence_need_ref.clone()];
    let mut updated_decision = decision;
    updated_decision.dependencies = vec![added.task_id.clone()];
    let next_graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        nodes: vec![
            planner,
            updated_evidence.clone(),
            added.clone(),
            updated_decision.clone(),
        ],
    };
    next_graph.validate().unwrap();
    let next_graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&next_graph).unwrap(),
        "runtime.workflow",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.runtime".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![
            ArtifactRef {
                artifact_id: graph_artifact.artifact_id.clone(),
                kind: ArtifactKind::WorkflowGraph,
            },
            ArtifactRef {
                artifact_id: proposal_artifact.artifact_id.clone(),
                kind: ArtifactKind::WorkflowProposal,
            },
        ],
        now,
    )
    .unwrap();
    let patch = WorkflowPatchCommit {
        permit: claimed.permit.clone(),
        previous_graph_artifact_id: graph_artifact.artifact_id.clone(),
        planner_output: planner_output.clone(),
        evidence_needs: vec![evidence_need.clone()],
        proposal: proposal_artifact.clone(),
        next_graph: next_graph_artifact.clone(),
        added_nodes: vec![added.clone()],
        updated_nodes: vec![updated_evidence, updated_decision],
        completed_at: now,
    };

    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_workflow_patch_completion BEFORE INSERT ON rebuild_events
                     WHEN NEW.event_type = 'task.succeeded'
                     BEGIN SELECT RAISE(ABORT, 'injected workflow patch failure'); END;",
            )
            .unwrap();
    }
    assert!(matches!(
        store.commit_workflow_patch(&patch),
        Err(StoreError::Sql(_))
    ));
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_workflow_patch_completion;")
            .unwrap();
        let revisions: u64 = connection
            .query_row(
                "SELECT COUNT(*) FROM rebuild_workflow_revisions WHERE run_id = ?1",
                params![run.run_id.0],
                |row| row.get(0),
            )
            .unwrap();
        let added_tasks: u64 = connection
            .query_row(
                "SELECT COUNT(*) FROM rebuild_tasks WHERE task_id = ?1",
                params![added.task_id.0],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revisions, 1);
        assert_eq!(added_tasks, 0);
    }
    assert!(matches!(
        store.artifact(&planner_output.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(matches!(
        store.artifact(&evidence_need.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(matches!(
        store.artifact(&proposal_artifact.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(matches!(
        store.artifact(&next_graph_artifact.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert_eq!(store.events_after(&run.run_id, 0, 100).unwrap().len(), 2);
    store.validate_task_permit(&claimed.permit).unwrap();

    store.commit_workflow_patch(&patch).unwrap();
    assert_eq!(
        store.artifact(&planner_output.artifact_id).unwrap(),
        planner_output
    );
    assert_eq!(
        store.artifact(&evidence_need.artifact_id).unwrap(),
        evidence_need
    );
    assert_eq!(
        store.artifact(&proposal_artifact.artifact_id).unwrap(),
        proposal_artifact
    );
    assert_eq!(
        store
            .committed_task_outputs(&run.run_id, &planner_task_id)
            .unwrap(),
        vec![proposal_artifact.clone()]
    );
    assert_eq!(
        store
            .committed_attempt_outputs(&planner_task_id, &claimed.permit.attempt_id)
            .unwrap(),
        vec![proposal_artifact.clone()]
    );
    let stored_graph = store.artifact(&next_graph_artifact.artifact_id).unwrap();
    assert_eq!(stored_graph.artifact_id, next_graph_artifact.artifact_id);
    let mut stored_refs = stored_graph.source_refs;
    let mut expected_refs = next_graph_artifact.source_refs;
    stored_refs.sort_by(|left, right| {
        left.artifact_id
            .cmp(&right.artifact_id)
            .then(left.kind.cmp(&right.kind))
    });
    expected_refs.sort_by(|left, right| {
        left.artifact_id
            .cmp(&right.artifact_id)
            .then(left.kind.cmp(&right.kind))
    });
    assert_eq!(stored_refs, expected_refs);
    assert!(matches!(
        store.validate_task_permit(&claimed.permit),
        Err(StoreError::StalePermit(_))
    ));
    let claimed_evidence = store
        .claim_next_task("evidence-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(
        claimed_evidence.node.input_artifacts,
        vec![evidence_need_ref]
    );
    let initial_revision = store.workflow_revision(&run.run_id, 0).unwrap();
    let current_revision = store.workflow_revision(&run.run_id, 1).unwrap();
    assert_eq!(
        initial_revision.graph_artifact.artifact_id,
        graph_artifact.artifact_id
    );
    assert_eq!(
        current_revision.graph_artifact.artifact_id,
        next_graph_artifact.artifact_id
    );
    let snapshot = store.workflow_snapshot(&run.run_id).unwrap();
    assert_eq!(snapshot.status, WorkflowStatus::Running);
    assert_eq!(snapshot.revision, current_revision);
    assert_eq!(snapshot.tasks.len(), 4);
    assert_eq!(
        snapshot.event_cursor,
        store
            .events_after(&run.run_id, 0, 100)
            .unwrap()
            .last()
            .unwrap()
            .cursor
    );
    let evidence_snapshot = snapshot
        .tasks
        .iter()
        .find(|task| task.node.task_id == claimed_evidence.node.task_id)
        .unwrap();
    assert_eq!(evidence_snapshot.attempt_count, 1);
    assert_eq!(
        evidence_snapshot.active_attempt.as_ref().unwrap().permit,
        claimed_evidence.permit
    );
    store.verify_integrity().unwrap();
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute(
                "DELETE FROM rebuild_artifact_refs WHERE artifact_id = ?1 AND source_kind = ?2",
                params![
                    next_graph_artifact.artifact_id.0.as_str(),
                    enum_name(ArtifactKind::WorkflowProposal)
                ],
            )
            .unwrap();
    }
    assert!(store.verify_integrity().is_err());
}

#[test]
fn stale_permit_cannot_write_an_artifact() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run,
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("worker", Utc::now(), Duration::milliseconds(-1))
        .unwrap()
        .unwrap();
    store.recover_expired_tasks(Utc::now()).unwrap();
    let evidence = artifact(
        &store,
        ArtifactKind::NormalizedEvidence,
        "claim evidence",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
    );
    let artifact = artifact_with_refs(
        &store,
        ArtifactKind::Claim,
        "claim",
        Some(ArtifactOrigin {
            run_id: Some(claimed.permit.run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: None,
        }),
        vec![artifact_ref(&evidence)],
    );
    assert!(matches!(
        store.write_task_artifact(
            &claimed.permit,
            &artifact,
            LifecycleEventType::ClaimCreated,
            Utc::now()
        ),
        Err(StoreError::StalePermit(_))
    ));
}

#[test]
fn bootstrapped_contract_must_not_carry_task_origin() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let artifact = store
        .contract_artifact(&contract(&store, 1), Utc::now())
        .unwrap();
    store.write_bootstrap_artifact(&artifact).unwrap();
    store.verify_integrity().unwrap();
}

#[test]
fn execution_commitment_requires_a_consumed_paper_approval() {
    let fixture = execution_commit_fixture();

    assert!(matches!(
        fixture.store.commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit,
                commitment: fixture.commitment,
                committed_at: fixture.now,
            },
        ),
        Err(StoreError::InvalidSessionSlot(_))
    ));
}

#[test]
fn execution_commitment_rejects_approval_notional_overrun() {
    let fixture =
        approved_execution_commit_fixture(MoneyMicros::from_usd_cents(1), Duration::hours(8));

    assert!(matches!(
        fixture.store.commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "2026-08-25".to_owned(),
                permit: fixture.permit,
                commitment: fixture.commitment,
                committed_at: fixture.now,
            },
        ),
        Err(StoreError::InvalidSessionSlot(_))
    ));
}

#[test]
fn execution_commitment_rejects_expired_approval() {
    let fixture = approved_execution_commit_fixture(
        MoneyMicros::from_usd_cents(100_000),
        Duration::seconds(1),
    );

    assert!(matches!(
        fixture.store.commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "2026-08-25".to_owned(),
                permit: fixture.permit,
                commitment: fixture.commitment,
                committed_at: fixture.now + Duration::seconds(2),
            },
        ),
        Err(StoreError::InvalidSessionSlot(_))
    ));
}

#[test]
fn execution_commitment_lineage_fails_closed() {
    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    commitment.lifecycle = ArtifactLifecycle::RunScoped;
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    commitment
        .source_refs
        .retain(|source| source.kind != ArtifactKind::ExecutionVerdict);
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    let verdict = commitment
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionVerdict)
        .unwrap()
        .clone();
    commitment.source_refs.push(verdict);
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    let context = commitment
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionContext)
        .unwrap()
        .clone();
    let no_order = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionVerdict,
        &ExecutionVerdict::NoOrder {
            no_order: NoOrder {
                execution_context: context.clone(),
                blockers: vec![HardBlocker::Frozen],
                created_at: fixture.now,
            },
        },
        vec![context.clone()],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &no_order,
            LifecycleEventType::ExecutionVerdictNoOrder,
            fixture.now,
        )
        .unwrap();
    commitment.source_refs = vec![artifact_ref(&no_order), context];
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let mut commitment = fixture.commitment.clone();
    let context_index = commitment
        .source_refs
        .iter()
        .position(|source| source.kind == ArtifactKind::ExecutionContext)
        .unwrap();
    commitment.source_refs[context_index] = ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(b"wrong-context")),
        kind: ArtifactKind::ExecutionContext,
    };
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());

    let fixture = execution_commit_fixture();
    let context_ref = fixture
        .commitment
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionContext)
        .unwrap();
    let context = fixture.store.artifact(&context_ref.artifact_id).unwrap();
    let plan_ref = context
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionPlan)
        .unwrap();
    let wrong_plan = fixture
        .store
        .put_json(&serde_json::json!({
            "plan_hash": ContentHash::of_bytes(b"wrong-plan")
        }))
        .unwrap();
    fixture
            .store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE rebuild_artifacts SET blob_hash = ?1, media_type = ?2, bytes = ?3 WHERE artifact_id = ?4",
                params![
                    wrong_plan.hash.as_str(),
                    wrong_plan.media_type,
                    wrong_plan.bytes,
                    plan_ref.artifact_id.0.as_str(),
                ],
            )
            .unwrap();
    assert!(fixture
        .store
        .commit_execution(
            &fixture.lease,
            &ExecutionCommit {
                session_key: "paper:fixture".to_owned(),
                permit: fixture.permit.clone(),
                commitment: fixture.commitment,
                committed_at: fixture.now,
            },
        )
        .is_err());
}

#[test]
fn stale_outcome_lease_rejects_artifact_write_without_partial_commit() {
    let fixture = execution_commit_fixture();
    let stale = fixture.now + Duration::seconds(31);
    let successor = fixture
        .store
        .acquire_daemon_lease(
            "scheduler",
            "successor",
            stale,
            stale + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let evidence = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::NormalizedEvidence,
        &serde_json::json!({"outcome": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        stale,
    );
    assert!(matches!(
        fixture.store.write_task_artifact_fenced(
            Some(&fixture.lease),
            &fixture.permit,
            &evidence,
            LifecycleEventType::OutcomeEvidence,
            stale,
        ),
        Err(StoreError::SchedulerFenced(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&evidence.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    fixture
        .store
        .write_task_artifact_fenced(
            Some(&successor),
            &fixture.permit,
            &evidence,
            LifecycleEventType::OutcomeEvidence,
            stale,
        )
        .unwrap();
    assert_eq!(
        fixture.store.artifact(&evidence.artifact_id).unwrap().kind,
        ArtifactKind::NormalizedEvidence
    );
}

#[test]
fn stale_outcome_lease_rejects_canonical_policy_evaluation() {
    let fixture = PolicyCommitFixture::memory();
    let lease_now = fixture.now;
    let lease = fixture
        .store
        .acquire_daemon_lease(
            "outcome-worker",
            "worker-a",
            lease_now,
            lease_now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let stale = lease_now + Duration::seconds(31);
    fixture
        .store
        .acquire_daemon_lease(
            "outcome-worker",
            "worker-b",
            stale,
            stale + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let commit = fixture.commit(
        fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap(),
    );
    assert!(matches!(
        fixture
            .store
            .record_policy_evaluation_fenced(Some(&lease), &commit),
        Err(StoreError::SchedulerFenced(_))
    ));
    assert!(fixture
        .store
        .policy_head(&fixture.subject)
        .unwrap()
        .is_none());
    assert!(matches!(
        fixture.store.artifact(&commit.evaluation.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
}

#[test]
fn outcome_schedule_worker_enqueue_is_idempotent_for_same_permit() {
    let fixture = PolicyCommitFixture::memory();
    let outcome_payload: Outcome = fixture
        .store
        .read_artifact_payload(&fixture.outcome)
        .unwrap();
    let stored_schedule = fixture
        .store
        .artifact(&outcome_payload.schedule.artifact_id)
        .unwrap();
    let mut payload: OutcomeSchedule = fixture
        .store
        .read_artifact_payload(&stored_schedule)
        .unwrap();
    payload.outcome_id = akzio_domain::OutcomeId::new();
    payload.created_at = fixture.now + Duration::seconds(1);
    let schedule = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::OutcomeSchedule,
        &payload,
        outcome_schedule_source_refs(&payload),
        ArtifactLifecycle::Canonical,
        payload.created_at,
    );

    fixture
        .store
        .commit_outcome_schedule_with_worker(&fixture.permit, &schedule, fixture.now)
        .unwrap();
    fixture
        .store
        .commit_outcome_schedule_with_worker(
            &fixture.permit,
            &schedule,
            fixture.now + Duration::seconds(1),
        )
        .unwrap();

    let worker_count = fixture
        .store
        .connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM rebuild_tasks WHERE run_id = ?1 AND recipe_id = ?2",
            params![fixture.run.run_id.0, POST_TERMINAL_WORKER_RECIPE_ID],
            |row| row.get::<_, u64>(0),
        )
        .unwrap();
    assert_eq!(worker_count, 1);
    let enqueued_events = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == "outcome.worker.enqueued")
        .count();
    assert_eq!(enqueued_events, 1);
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn outcome_worker_defers_without_consuming_retry_or_failing_completed_run() {
    let fixture = PolicyCommitFixture::memory();
    let outcome_payload: Outcome = fixture
        .store
        .read_artifact_payload(&fixture.outcome)
        .unwrap();
    let stored_schedule = fixture
        .store
        .artifact(&outcome_payload.schedule.artifact_id)
        .unwrap();
    let mut payload: OutcomeSchedule = fixture
        .store
        .read_artifact_payload(&stored_schedule)
        .unwrap();
    payload.outcome_id = akzio_domain::OutcomeId::new();
    payload.created_at = fixture.now + Duration::seconds(1);
    let schedule = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::OutcomeSchedule,
        &payload,
        outcome_schedule_source_refs(&payload),
        ArtifactLifecycle::Canonical,
        payload.created_at,
    );

    fixture
        .store
        .commit_outcome_schedule_with_worker(&fixture.permit, &schedule, fixture.now)
        .unwrap();
    let completed = fixture
        .store
        .workflow_snapshot(&fixture.run.run_id)
        .unwrap();
    assert_eq!(completed.status, WorkflowStatus::Completed);
    let worker = fixture
        .store
        .claim_next_task("outcome-worker", fixture.now, Duration::seconds(30))
        .unwrap()
        .expect("completed Paper run must keep its outcome worker claimable");
    assert_eq!(
        worker.node.recipe_id.as_str(),
        POST_TERMINAL_WORKER_RECIPE_ID
    );

    let ready_at = fixture.now + Duration::days(1);
    fixture
        .store
        .defer_task(&worker.permit, ready_at, fixture.now)
        .unwrap();
    let deferred = fixture
        .store
        .workflow_snapshot(&fixture.run.run_id)
        .unwrap();
    assert_eq!(deferred.status, WorkflowStatus::Completed);
    assert!(fixture
        .store
        .claim_next_task(
            "too-early-outcome-worker",
            ready_at - Duration::seconds(1),
            Duration::seconds(30),
        )
        .unwrap()
        .is_none());
    assert!(fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| event.event_type == LifecycleEventType::TaskDeferred.as_str()));
    assert!(!fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| {
            matches!(
                event.lifecycle_kind().unwrap(),
                LifecycleEventType::TaskRetryScheduled | LifecycleEventType::TaskRetryExhausted
            )
        }));

    let resumed = fixture
        .store
        .claim_next_task("resumed-outcome-worker", ready_at, Duration::seconds(30))
        .unwrap()
        .expect("deferred outcome worker must reactivate when due");
    fixture
        .store
        .finish_task(&resumed.permit, TaskStatus::Failed, ready_at)
        .unwrap();
    assert_eq!(
        fixture
            .store
            .workflow_snapshot(&fixture.run.run_id)
            .unwrap()
            .status,
        WorkflowStatus::Completed
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn daemon_lease_validation_and_fenced_attempt_fail_closed() {
    let fixture = execution_commit_fixture();
    fixture
        .store
        .validate_daemon_lease(&fixture.lease, fixture.now)
        .unwrap();
    let successor_now = fixture.now + Duration::seconds(31);
    let successor = fixture
        .store
        .acquire_daemon_lease(
            "scheduler",
            "successor",
            successor_now,
            successor_now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .validate_daemon_lease(&fixture.lease, successor_now),
        Err(StoreError::SchedulerFenced(_))
    ));
    fixture
        .store
        .validate_daemon_lease(&successor, successor_now)
        .unwrap();

    let receipt = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::OrderReceipt,
        &serde_json::json!({"receipt": true}),
        vec![],
        ArtifactLifecycle::Canonical,
        successor_now,
    );
    assert!(matches!(
        fixture.store.commit_fenced_attempt(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&receipt),
            TaskStatus::Succeeded,
            successor_now,
        ),
        Err(StoreError::SchedulerFenced(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&receipt.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    fixture.store.validate_task_permit(&fixture.permit).unwrap();

    fixture
        .store
        .commit_fenced_attempt(
            &successor,
            &fixture.permit,
            std::slice::from_ref(&receipt),
            TaskStatus::Succeeded,
            successor_now,
        )
        .unwrap();
    assert_eq!(
        fixture.store.artifact(&receipt.artifact_id).unwrap().kind,
        ArtifactKind::OrderReceipt
    );
}

#[test]
fn doctor_rejects_corrupt_execution_lineage() {
    let fixture = execution_commit_fixture();
    let payload: PaperCommitment =
        serde_json::from_slice(&fixture.store.read_blob(&fixture.commitment.blob).unwrap())
            .unwrap();
    let context = fixture
        .commitment
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ExecutionContext)
        .unwrap()
        .clone();
    let invalid = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionCommitment,
        &payload,
        vec![context],
        ArtifactLifecycle::Canonical,
        fixture.now,
    );
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection.transaction().unwrap();
        insert_artifact(&transaction, &invalid).unwrap();
        transaction
                .execute(
                    "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3",
                    params![
                        invalid.artifact_id.0.as_str(),
                        fixture.now.to_rfc3339(),
                        "paper:fixture",
                    ],
                )
                .unwrap();
        transaction.commit().unwrap();
    }
    let error = fixture.store.verify_integrity().unwrap_err();
    assert!(
        matches!(
            &error,
            StoreError::Integrity(message)
                if message.contains("commitment lineage is invalid")
        ),
        "{error}"
    );

    let fixture = execution_commit_fixture();
    let payload: PaperCommitment =
        serde_json::from_slice(&fixture.store.read_blob(&fixture.commitment.blob).unwrap())
            .unwrap();
    let invalid = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionCommitment,
        &payload,
        fixture.commitment.source_refs.clone(),
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection.transaction().unwrap();
        insert_artifact(&transaction, &invalid).unwrap();
        transaction
                .execute(
                    "UPDATE rebuild_session_slots SET commitment_artifact_id = ?1, committed_at = ?2 WHERE session_key = ?3",
                    params![
                        invalid.artifact_id.0.as_str(),
                        fixture.now.to_rfc3339(),
                        "paper:fixture",
                    ],
                )
                .unwrap();
        transaction.commit().unwrap();
    }
    let error = fixture.store.verify_integrity().unwrap_err();
    assert!(
        matches!(
            &error,
            StoreError::Integrity(message)
                if message.contains("commitment lineage is invalid")
        ),
        "{error}"
    );
}

#[test]
fn approved_paper_reservation_rejects_mismatched_proposal_and_keeps_store_atomic() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    let workflow = WorkflowCommit {
        run: run.clone(),
        graph: graph_artifact,
        nodes: graph.nodes,
    };
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: run.topology_id.clone(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyze".to_owned(),
                depends_on: vec![],
                priority: 50,
                evidence_needs: vec![],
            },
        )]),
        stop_reason: Some("fixture".to_owned()),
    };
    let mut proposal = artifact(
        &store,
        ArtifactKind::WorkflowProposal,
        &serde_json::to_string(&proposal_payload).unwrap(),
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
    );
    proposal.producer = "runtime.paper_provisioning".to_owned();
    proposal.lifecycle = ArtifactLifecycle::RunScoped;
    let reservation = SessionReservation {
        session_key: "2026-08-12".to_owned(),
        workflow,
        setup_artifacts: vec![],
        reserved_at: now,
    };
    let mut wrong_proposal = proposal.clone();
    wrong_proposal.origin = Some(ArtifactOrigin {
        run_id: Some(RunId::new()),
        task_id: None,
        attempt_id: None,
        contract_hash: None,
    });
    assert!(matches!(
        store.reserve_paper_session_with_proposal(&lease, &reservation, &wrong_proposal),
        Err(StoreError::InvalidSessionSlot(_))
    ));
    assert!(store.session_slot("2026-08-12").unwrap().is_none());
    assert!(matches!(
        store.run_purpose(&run.run_id),
        Err(StoreError::MissingRun(_))
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn approved_paper_reservation_rejects_source_closure_mismatch_atomically() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    let workflow = WorkflowCommit {
        run: run.clone(),
        graph: graph_artifact,
        nodes: graph.nodes,
    };
    let setup = artifact(
        &store,
        ArtifactKind::EvidenceNeed,
        "{}",
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
    );
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: run.topology_id.clone(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyze".to_owned(),
                depends_on: vec![],
                priority: 50,
                evidence_needs: vec![],
            },
        )]),
        stop_reason: Some("fixture".to_owned()),
    };
    let mut proposal = artifact_with_refs(
        &store,
        ArtifactKind::WorkflowProposal,
        &serde_json::to_string(&proposal_payload).unwrap(),
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
    );
    proposal.producer = "runtime.paper_provisioning".to_owned();
    proposal.artifact_id = ArtifactId(proposal.expected_hash().unwrap());
    let reservation = SessionReservation {
        session_key: "2026-08-12-source-closure".to_owned(),
        workflow,
        setup_artifacts: vec![setup],
        reserved_at: now,
    };
    assert!(matches!(
        store.reserve_paper_session_with_proposal(&lease, &reservation, &proposal),
        Err(StoreError::InvalidWorkflowProposalArtifact)
    ));
    assert!(store
        .session_slot("2026-08-12-source-closure")
        .unwrap()
        .is_none());
    assert!(matches!(
        store.run_purpose(&run.run_id),
        Err(StoreError::MissingRun(_))
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn approved_paper_reservation_is_idempotent_for_duplicate_session() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    let workflow = WorkflowCommit {
        run: run.clone(),
        graph: graph_artifact,
        nodes: graph.nodes,
    };
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: run.topology_id.clone(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyze".to_owned(),
                depends_on: vec![],
                priority: 50,
                evidence_needs: vec![],
            },
        )]),
        stop_reason: Some("fixture".to_owned()),
    };
    let mut proposal = artifact(
        &store,
        ArtifactKind::WorkflowProposal,
        &serde_json::to_string(&proposal_payload).unwrap(),
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
    );
    proposal.producer = "runtime.paper_provisioning".to_owned();
    proposal.artifact_id = ArtifactId(proposal.expected_hash().unwrap());
    let reservation = SessionReservation {
        session_key: "2026-08-12".to_owned(),
        workflow,
        setup_artifacts: vec![],
        reserved_at: now,
    };
    let first = store
        .reserve_paper_session_with_proposal(&lease, &reservation, &proposal)
        .unwrap();
    let second = store
        .reserve_paper_session_with_proposal(&lease, &reservation, &proposal)
        .unwrap();
    assert!(first.newly_reserved);
    assert!(!second.newly_reserved);
    assert_eq!(
        first.slot.workflow.run.run_id,
        second.slot.workflow.run.run_id
    );
    let successor = store
        .acquire_daemon_lease(
            "scheduler",
            "daemon-b",
            now + Duration::seconds(31),
            now + Duration::seconds(61),
        )
        .unwrap()
        .unwrap();
    assert_eq!(successor.epoch, lease.epoch + 1);
    assert!(matches!(
        store.reserve_paper_session_with_proposal(&lease, &reservation, &proposal),
        Err(StoreError::SchedulerFenced(_))
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn session_slot_is_fenced_and_reuses_the_frozen_workflow() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let session_key = "2026-08-25";
    let first_lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();

    let first_graph = graph();
    let first_graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&first_graph).unwrap(),
        None,
    );
    let first_workflow = WorkflowCommit {
        run: StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: first_graph.topology_id.clone(),
            graph_artifact_id: first_graph_artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: first_graph_artifact,
        nodes: first_graph.nodes,
    };
    let first = reserve_approved_test_session(
        &store,
        &first_lease,
        &SessionReservation {
            session_key: session_key.to_owned(),
            workflow: first_workflow.clone(),
            setup_artifacts: vec![],
            reserved_at: now,
        },
    );
    assert!(first.newly_reserved);

    let mut replacement_graph = graph();
    replacement_graph.nodes[0].objective = "replacement plan".to_owned();
    let replacement_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&replacement_graph).unwrap(),
        None,
    );
    let replacement_workflow = WorkflowCommit {
        run: StoredRun {
            run_id: RunId::new(),
            purpose: RunPurpose::Paper,
            topology_id: replacement_graph.topology_id.clone(),
            graph_artifact_id: replacement_artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: replacement_artifact,
        nodes: replacement_graph.nodes,
    };
    let duplicate = store
        .reserve_session_slot(
            &first_lease,
            &SessionReservation {
                session_key: session_key.to_owned(),
                workflow: replacement_workflow.clone(),
                setup_artifacts: vec![],
                reserved_at: now,
            },
        )
        .unwrap();
    assert!(!duplicate.newly_reserved);
    assert_eq!(
        duplicate.slot.workflow.run.run_id,
        first_workflow.run.run_id
    );
    assert_eq!(
        duplicate.slot.workflow.graph.artifact_id,
        first_workflow.graph.artifact_id
    );

    let claimed = store
        .claim_next_task("execution-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let commitment = valid_execution_commitment(&store, &claimed.permit, session_key, now);
    {
        let connection = store.connection.lock().unwrap();
        connection
                .execute_batch(
                    "CREATE TRIGGER fail_execution_task_completion BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'task.succeeded' \
                     BEGIN SELECT RAISE(ABORT, 'injected execution completion event failure'); END;",
                )
                .unwrap();
    }
    assert!(matches!(
        store.commit_execution(
            &first_lease,
            &ExecutionCommit {
                session_key: session_key.to_owned(),
                permit: claimed.permit.clone(),
                commitment: commitment.clone(),
                committed_at: now,
            },
        ),
        Err(StoreError::Sql(_))
    ));
    assert_eq!(
        store
            .session_slot(session_key)
            .unwrap()
            .unwrap()
            .commitment_artifact_id,
        None
    );
    assert!(matches!(
        store.artifact(&commitment.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(store
        .events_after(&claimed.permit.run_id, 0, 20)
        .unwrap()
        .iter()
        .all(|event| event.event_type != "execution.committed"));
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_execution_task_completion;")
            .unwrap();
    }
    let committed = store
        .commit_execution(
            &first_lease,
            &ExecutionCommit {
                session_key: session_key.to_owned(),
                permit: claimed.permit.clone(),
                commitment: commitment.clone(),
                committed_at: now,
            },
        )
        .unwrap();
    assert!(committed.newly_committed);
    let outputs = store
        .committed_task_outputs(&claimed.permit.run_id, &claimed.permit.task_id)
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].artifact_id, commitment.artifact_id);
    assert!(matches!(
        store.commit_execution(
            &first_lease,
            &ExecutionCommit {
                session_key: session_key.to_owned(),
                permit: claimed.permit.clone(),
                commitment: commitment.clone(),
                committed_at: now,
            },
        ),
        Err(StoreError::StalePermit(_))
    ));
    let events = store.events_after(&claimed.permit.run_id, 0, 20).unwrap();
    assert!(events.iter().any(|event| {
        event.event_type == "execution.committed"
            && event.artifact_id.as_ref() == Some(&commitment.artifact_id)
    }));
    assert!(events.iter().any(|event| {
        event.event_type == "task.succeeded"
            && event.task_id.as_ref() == Some(&claimed.permit.task_id)
            && event.attempt_id.as_ref() == Some(&claimed.permit.attempt_id)
            && event.artifact_id.as_ref() == Some(&commitment.artifact_id)
    }));
    assert_eq!(
        store
            .session_slot(session_key)
            .unwrap()
            .unwrap()
            .commitment_artifact_id,
        Some(commitment.artifact_id.clone())
    );
    store.verify_integrity().unwrap();

    let successor_now = now + Duration::seconds(31);
    let successor = store
        .acquire_daemon_lease(
            "scheduler",
            "daemon-b",
            successor_now,
            successor_now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    assert_eq!(successor.epoch, first_lease.epoch + 1);
    assert!(matches!(
        store.commit_execution(
            &first_lease,
            &ExecutionCommit {
                session_key: session_key.to_owned(),
                permit: claimed.permit.clone(),
                commitment,
                committed_at: successor_now,
            },
        ),
        Err(StoreError::SchedulerFenced(_))
    ));
    assert!(matches!(
        store.reserve_session_slot(
            &first_lease,
            &SessionReservation {
                session_key: "paper:fixture-b".to_owned(),
                workflow: replacement_workflow,
                setup_artifacts: vec![],
                reserved_at: successor_now,
            },
        ),
        Err(StoreError::SchedulerFenced(_))
    ));
}

#[test]
fn doctor_rejects_a_corrupt_session_slot() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease("scheduler", "daemon-a", now, now + Duration::seconds(30))
        .unwrap()
        .unwrap();
    let graph = graph();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    store
        .reserve_session_slot(
            &lease,
            &SessionReservation {
                session_key: "paper:fixture-corrupt".to_owned(),
                workflow: WorkflowCommit {
                    run: StoredRun {
                        run_id: RunId::new(),
                        purpose: RunPurpose::Paper,
                        topology_id: graph.topology_id.clone(),
                        graph_artifact_id: graph_artifact.artifact_id.clone(),
                        created_at: now,
                    },
                    graph: graph_artifact,
                    nodes: graph.nodes,
                },
                setup_artifacts: vec![],
                reserved_at: now,
            },
        )
        .unwrap();
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute(
                "UPDATE rebuild_session_slots SET topology_id = 'corrupt' WHERE session_key = ?1",
                params!["paper:fixture-corrupt"],
            )
            .unwrap();
    }
    assert!(matches!(
        store.verify_integrity(),
        Err(StoreError::Integrity(message)) if message.contains("topology mismatch")
    ));
}

#[test]
fn policy_transition_is_atomic_with_learning_artifacts_and_terminal_event() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let mut graph = graph();
    let seed = graph.nodes[0].clone();
    let mut evaluation_node = seed.clone();
    evaluation_node.task_id = TaskId::new();
    evaluation_node.dependencies = vec![seed.task_id.clone()];
    graph.nodes = vec![seed, evaluation_node];
    graph.validate().unwrap();
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let seed_permit = store
        .claim_next_task("seed-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;

    let make_artifact = |permit: &TaskWritePermit,
                         kind: ArtifactKind,
                         payload: serde_json::Value,
                         source_refs: Vec<ArtifactRef>,
                         lifecycle: ArtifactLifecycle| {
        Artifact::new(
            kind,
            store.put_json(&payload).unwrap(),
            "fixture",
            lifecycle,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            source_refs,
            now,
        )
        .unwrap()
    };
    let reference = |artifact: &Artifact| ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    };
    let raw = make_artifact(
        &seed_permit,
        ArtifactKind::RawEvidence,
        serde_json::json!({"raw": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
    );
    let normalized = make_artifact(
        &seed_permit,
        ArtifactKind::NormalizedEvidence,
        serde_json::json!({"normalized": true}),
        vec![reference(&raw)],
        ArtifactLifecycle::RunScoped,
    );
    let execution_context = make_artifact(
        &seed_permit,
        ArtifactKind::ExecutionContext,
        serde_json::json!({"execution": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
    );
    let decision = make_artifact(
        &seed_permit,
        ArtifactKind::Decision,
        serde_json::json!({"decision": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
    );
    let decision_context = make_artifact(
        &seed_permit,
        ArtifactKind::DecisionContext,
        serde_json::json!({"context": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
    );
    let verdict_payload = ExecutionVerdict::NoOrder {
        no_order: akzio_domain::NoOrder {
            execution_context: reference(&execution_context),
            blockers: vec![akzio_domain::HardBlocker::Frozen],
            created_at: now,
        },
    };
    let verdict = make_artifact(
        &seed_permit,
        ArtifactKind::ExecutionVerdict,
        serde_json::to_value(&verdict_payload).unwrap(),
        vec![reference(&execution_context)],
        ArtifactLifecycle::RunScoped,
    );
    let outcome_id = akzio_domain::OutcomeId::new();
    let schedule_payload = OutcomeSchedule {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id: outcome_id.clone(),
        decision: reference(&decision),
        decision_context: reference(&decision_context),
        execution_context: reference(&execution_context),
        execution: OutcomeExecutionLineage::NoOrder {
            execution_verdict: reference(&verdict),
        },
        baseline_trading_day: now.date_naive(),
        created_at: now,
    };
    let schedule = make_artifact(
        &seed_permit,
        ArtifactKind::OutcomeSchedule,
        serde_json::to_value(&schedule_payload).unwrap(),
        vec![
            schedule_payload.decision.clone(),
            schedule_payload.decision_context.clone(),
            schedule_payload.execution_context.clone(),
            reference(&verdict),
        ],
        ArtifactLifecycle::Canonical,
    );
    store
        .commit_attempt(
            &seed_permit,
            &[
                raw,
                normalized.clone(),
                execution_context.clone(),
                decision.clone(),
                decision_context.clone(),
                verdict.clone(),
                schedule.clone(),
            ],
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();

    let evaluation_permit = store
        .claim_next_task("evaluation-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let execution_ref = reference(&execution_context);
    let evidence_ref = reference(&normalized);
    let outcome_payload = Outcome {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        outcome_id,
        schedule: reference(&schedule),
        market_evidence: vec![evidence_ref.clone()],
        windows: [
            akzio_domain::OutcomeHorizon::T1,
            akzio_domain::OutcomeHorizon::T3,
            akzio_domain::OutcomeHorizon::T5,
        ]
        .into_iter()
        .map(|horizon| akzio_domain::OutcomeWindow {
            horizon,
            observed_trading_day: now.date_naive()
                + chrono::Days::new(u64::from(horizon.trading_days())),
            portfolio_return_ppm: 1,
            benchmark_return_ppm: 0,
            transaction_cost_ppm: 0,
            slippage_ppm: 0,
            utility_ppm: 1,
            calibration_ppm: Some(1_000_000),
            evidence_completeness_ppm: 1_000_000,
            risk_recall_ppm: Some(1_000_000),
        })
        .collect(),
        sealed_at: Some(now),
    };
    let outcome = make_artifact(
        &evaluation_permit,
        ArtifactKind::Outcome,
        serde_json::to_value(&outcome_payload).unwrap(),
        vec![reference(&schedule), evidence_ref],
        ArtifactLifecycle::Canonical,
    );
    let outcome_ref = reference(&outcome);
    let final_retrospective = retrospective_artifact(&store, &evaluation_permit, &outcome, now);
    let retrospective_ref = reference(&final_retrospective);
    let subject = PolicySubject::Memory(akzio_domain::MemoryId::new());
    let experience_payload = Experience {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        experience_id: akzio_domain::ExperienceId::new(),
        subject: subject.clone(),
        hypothesis_id: "fixture".to_owned(),
        decision: reference(&decision),
        decision_context: reference(&decision_context),
        execution_context: execution_ref.clone(),
        policy_verdict: reference(&verdict),
        outcome: outcome_ref.clone(),
        contract_hash: ContentHash::of_bytes(b"fixture-contract"),
        topology_id: akzio_domain::TopologyId("fixture-topology".to_owned()),
        policy_state: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
        created_at: now,
    };
    let experience = make_artifact(
        &evaluation_permit,
        ArtifactKind::Experience,
        serde_json::to_value(&experience_payload).unwrap(),
        vec![
            experience_payload.decision.clone(),
            experience_payload.decision_context.clone(),
            experience_payload.execution_context.clone(),
            experience_payload.policy_verdict.clone(),
            experience_payload.outcome.clone(),
            retrospective_ref.clone(),
        ],
        ArtifactLifecycle::Canonical,
    );
    let experience_ref = reference(&experience);
    let evaluation_payload = Evaluation {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        evaluation_id: akzio_domain::EvaluationId::new(),
        outcome: outcome_ref.clone(),
        experience: experience_ref.clone(),
        marginal_utility_ppm: 1,
        token_cost: Some(1),
        latency_millis: Some(1),
        created_at: now,
    };
    let evaluation = make_artifact(
        &evaluation_permit,
        ArtifactKind::Evaluation,
        serde_json::to_value(&evaluation_payload).unwrap(),
        vec![outcome_ref, experience_ref, retrospective_ref],
        ArtifactLifecycle::Canonical,
    );
    let pair_snapshot = store.policy_shadow_pair_snapshot(&subject).unwrap();
    let commit = PolicyEvaluationCommit {
        permit: evaluation_permit,
        outcome: outcome.clone(),
        final_retrospective,
        experience,
        evaluation: evaluation.clone(),
        candidate_policy: None,
        subject: subject.clone(),
        from: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
        to: PolicyState::Memory(akzio_domain::MemoryLifecycle::Active),
        pair_snapshot,
        transition: Some(PolicyTransition {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            transition_id: PolicyTransitionId::new(),
            subject: subject.clone(),
            from: PolicyState::Memory(akzio_domain::MemoryLifecycle::Candidate),
            to: PolicyState::Memory(akzio_domain::MemoryLifecycle::Active),
            evaluation: reference(&evaluation),
            created_at: now,
        }),
        completed_at: now,
    };
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_policy_event BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'policy.transitioned' \
                     BEGIN SELECT RAISE(ABORT, 'injected policy event failure'); END;",
            )
            .unwrap();
    }
    let failed = store.record_policy_evaluation(&commit);
    assert!(
        matches!(&failed, Err(StoreError::Sql(_))),
        "unexpected policy transition result: {failed:?}"
    );
    {
        let connection = store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_policy_event;")
            .unwrap();
    }
    assert!(store.policy_head(&subject).unwrap().is_none());
    assert!(matches!(
        store.artifact(&outcome.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(store
        .events_after(&run.run_id, 0, 100)
        .unwrap()
        .iter()
        .all(|event| event.event_type != "policy.transitioned"));

    let recorded = store.record_policy_evaluation(&commit).unwrap();
    assert!(recorded.newly_recorded);
    assert!(recorded.policy_head.is_some());
    assert_eq!(store.policy_transitions(&subject).unwrap().len(), 1);
    store.verify_integrity().unwrap();

    store
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE rebuild_policy_consumption_heads \
                 SET consumed_pair_cursor = 999 WHERE subject_id = ?1",
            params![subject.subject_id()],
        )
        .unwrap();
    let corrupted = store.verify_integrity();
    assert!(
        matches!(&corrupted, Err(StoreError::Integrity(_))),
        "unexpected Doctor result after policy cursor corruption: {corrupted:?}"
    );
}

#[test]
fn generic_learning_artifacts_require_specialized_atomic_apis() {
    let fixture = PolicyCommitFixture::memory();
    let candidate_policy = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::CandidatePolicy,
        &serde_json::json!({"candidate": true}),
        vec![],
        ArtifactLifecycle::Canonical,
        fixture.now,
    );

    for protected in [
        fixture.outcome.clone(),
        fixture.experience.clone(),
        fixture.evaluation.clone(),
        candidate_policy,
    ] {
        assert!(matches!(
            fixture.store.write_task_artifact(
                &fixture.permit,
                &protected,
                LifecycleEventType::FixtureGenericWrite,
                fixture.now,
            ),
            Err(StoreError::InvalidLearningCommit(
                "learning_artifact.atomic_commit_required"
            ))
        ));
        assert!(matches!(
            fixture.store.commit_attempt(
                &fixture.permit,
                &[protected],
                TaskStatus::Succeeded,
                fixture.now,
            ),
            Err(StoreError::InvalidLearningCommit(
                "learning_artifact.atomic_commit_required"
            ))
        ));
    }
}

#[test]
fn old_v7_policy_evaluation_shape_is_rejected() {
    let root = tempdir().unwrap();
    let database = root.path().join(DATABASE_FILE);
    let connection = Connection::open(database).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE rebuild_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO rebuild_metadata (key, value) VALUES ('schema_version', '7');
                 CREATE TABLE rebuild_policy_evaluations (
                    evaluation_artifact_id TEXT PRIMARY KEY,
                    subject_id TEXT NOT NULL,
                    outcome_artifact_id TEXT NOT NULL,
                    experience_artifact_id TEXT NOT NULL
                 );",
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        V2Store::open(root.path()),
        Err(StoreError::IncompatibleStoreRoot(path)) if path == root.path()
    ));
}

#[test]
fn policy_snapshot_does_not_consume_pairs_completed_after_cutoff() {
    let fixture = PolicyCommitFixture::memory();
    let first_cursor =
        fixture.insert_pair("snapshot-before-cutoff", OutcomeHorizon::T1, fixture.now);
    let snapshot = fixture
        .store
        .policy_shadow_pair_snapshot(&fixture.subject)
        .unwrap();
    assert_eq!(snapshot.through_cursor, first_cursor);
    assert_eq!(snapshot.counts_by_horizon, [1, 0, 0]);

    let second_cursor = fixture.insert_pair(
        "snapshot-after-cutoff",
        OutcomeHorizon::T3,
        fixture.now + Duration::seconds(1),
    );
    let recorded = fixture
        .store
        .record_policy_evaluation(&fixture.commit(snapshot))
        .unwrap();
    assert_eq!(recorded.consumed_pair_cursor, first_cursor);

    let remaining = fixture
        .store
        .policy_shadow_pair_snapshot(&fixture.subject)
        .unwrap();
    assert_eq!(remaining.after_cursor, first_cursor);
    assert_eq!(remaining.through_cursor, second_cursor);
    assert_eq!(remaining.counts_by_horizon, [0, 1, 0]);
}

#[test]
fn doctor_rejects_candidate_reverse_binding_corruption() {
    let fixture = PolicyCommitFixture::topology();
    let commit = fixture.commit(
        fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap(),
    );
    fixture.store.record_policy_evaluation(&commit).unwrap();
    fixture.store.verify_integrity().unwrap();

    let original = commit.candidate_policy.as_ref().unwrap();
    let forged = Artifact::new(
        ArtifactKind::CandidatePolicy,
        original.blob.clone(),
        "fixture.policy.reverse-corruption",
        ArtifactLifecycle::Canonical,
        original.provenance.clone(),
        original.origin.clone(),
        original.source_refs.clone(),
        original.created_at + Duration::microseconds(1),
    )
    .unwrap();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_artifact(&transaction, &forged).unwrap();
        transaction
            .execute(
                "UPDATE rebuild_policy_evaluations
                     SET candidate_policy_artifact_id = ?1
                     WHERE evaluation_artifact_id = ?2",
                params![
                    forged.artifact_id.0.as_str(),
                    fixture.evaluation.artifact_id.0.as_str(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    match fixture.store.verify_integrity() {
        Err(StoreError::Integrity(_)) => {}
        other => panic!("unexpected Doctor result: {other:?}"),
    }
}

#[test]
fn doctor_rejects_no_order_schedule_with_accepted_verdict() {
    let fixture = PolicyCommitFixture::memory();
    fixture.store.verify_integrity().unwrap();
    let schedule = fixture
        .store
        .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
        .unwrap()
        .unwrap();
    let execution_context = fixture
        .store
        .latest_artifact_by_kind(ArtifactKind::ExecutionContext)
        .unwrap()
        .unwrap();
    let accepted_verdict = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionVerdict,
        &ExecutionVerdict::Accepted {
            execution_context: artifact_ref(&execution_context),
        },
        vec![artifact_ref(&execution_context)],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    let mut payload: OutcomeSchedule =
        serde_json::from_slice(&fixture.store.read_blob(&schedule.blob).unwrap()).unwrap();
    payload.execution = OutcomeExecutionLineage::NoOrder {
        execution_verdict: artifact_ref(&accepted_verdict),
    };
    let forged_schedule = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::OutcomeSchedule,
        &payload,
        outcome_schedule_source_refs(&payload),
        ArtifactLifecycle::Canonical,
        fixture.now,
    );
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection.transaction().unwrap();
        insert_artifact(&transaction, &accepted_verdict).unwrap();
        insert_artifact(&transaction, &forged_schedule).unwrap();
        transaction.commit().unwrap();
    }

    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message)) if message.contains("execution lineage")
    ));
}

#[test]
fn doctor_rejects_stale_policy_head() {
    let fixture = PolicyCommitFixture::memory();
    let commit = fixture.commit(
        fixture
            .store
            .policy_shadow_pair_snapshot(&fixture.subject)
            .unwrap(),
    );
    fixture.store.record_policy_evaluation(&commit).unwrap();
    fixture.store.verify_integrity().unwrap();

    let stale_transition = PolicyTransition {
        transition_id: PolicyTransitionId::new(),
        created_at: fixture.now + Duration::seconds(1),
        ..fixture.transition.clone()
    };
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let event_cursor = append_event(
            &transaction,
            &fixture.run.run_id,
            Some(&fixture.permit.task_id),
            Some(&fixture.permit.attempt_id),
            LifecycleEventType::PolicyTransitioned,
            Some(&fixture.evaluation.artifact_id),
            stale_transition.created_at,
        )
        .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_policy_transitions
                       (transition_id, subject_id, subject_json, from_state_json, to_state_json,
                        evaluation_artifact_id, run_id, revision, created_at, event_cursor)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
                params![
                    stale_transition.transition_id.0,
                    fixture.subject.subject_id(),
                    serde_json::to_string(&fixture.subject).unwrap(),
                    serde_json::to_string(&stale_transition.from).unwrap(),
                    serde_json::to_string(&stale_transition.to).unwrap(),
                    fixture.evaluation.artifact_id.0.as_str(),
                    fixture.run.run_id.0,
                    2_u64,
                    stale_transition.created_at.to_rfc3339(),
                    event_cursor,
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    let corrupted = fixture.store.verify_integrity();
    assert!(matches!(
        &corrupted,
        Err(StoreError::Integrity(message)) if message.contains("stale")
    ));
}

#[test]
fn paper_effect_events_require_complete_lineage_at_append_boundary() {
    let fixture = execution_commit_fixture();
    let event_types = [
        LifecycleEventType::ExecutionEffectIntent,
        LifecycleEventType::ExecutionEffectRecovered,
        LifecycleEventType::ExecutionEffectSettled,
    ];
    let cases = [
        (
            None,
            Some(&fixture.permit.attempt_id),
            Some(&fixture.commitment.artifact_id),
        ),
        (
            Some(&fixture.permit.task_id),
            None,
            Some(&fixture.commitment.artifact_id),
        ),
        (
            Some(&fixture.permit.task_id),
            Some(&fixture.permit.attempt_id),
            None,
        ),
    ];

    for lifecycle_type in event_types {
        for (task_id, attempt_id, artifact_id) in cases {
            let mut connection = fixture.store.connection.lock().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            assert!(matches!(
                append_event(
                    &transaction,
                    &fixture.permit.run_id,
                    task_id,
                    attempt_id,
                    lifecycle_type,
                    artifact_id,
                    fixture.now,
                ),
                Err(StoreError::InvalidLifecycleEventShape { event_type: value })
                if value == lifecycle_type.as_str()
            ));
        }
    }
}

#[test]
fn trajectory_redacts_provider_and_tool_payloads() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();
    let turn = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::AgentTurn,
        &serde_json::json!({
            "turn": 1,
            "attempt": 1,
            "contract_hash": "contract",
            "request_hash": "request-hash",
            "capability_snapshot": {
                "provider_id": "fixture-provider",
                "model_id": "fixture-model",
                "reasoning_effort": "high",
                "supports_tool_calls": true,
                "supports_stateless_continuation": true,
                "native_web_tool": false,
                "source": "fixture"
                },
                "capability_snapshot_hash": "capability-hash",
                "tool_set_hash": "tool-hash",
            "request": {"phase": "draft", "secret": "provider-request"},
            "response": {
                "assistant_text": "bounded fixture research memo",
                "telemetry": {
                    "latency_millis": 321,
                    "input_tokens": 123,
                    "output_tokens": 45
                },
                "secret": "provider-result"
            }
        }),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &turn,
            LifecycleEventType::AgentTurnCompleted,
            fixture.now,
        )
        .unwrap();

    let call = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolCall,
        &serde_json::json!({
            "call": {
                "call_id": "call-1",
                "name": "read_artifact",
                "arguments": {"secret": "tool-arguments"}
            }
        }),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &call,
            LifecycleEventType::ToolCalled,
            fixture.now,
        )
        .unwrap();
    let result = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolResult,
        &serde_json::json!({
            "call_id": "call-1",
            "name": "read_artifact",
            "ok": true,
            "value": {"secret": "tool-result"}
        }),
        vec![artifact_ref(&call)],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &result,
            LifecycleEventType::ToolCompleted,
            fixture.now,
        )
        .unwrap();

    let note = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::DeliberationNote,
        &serde_json::json!({
            "selected_path": "use the governed evidence",
            "alternatives": ["defer"],
            "uncertainties": ["fixture uncertainty"],
            "basis_artifact_ids": [],
            "confidence_ppm": 750_000
        }),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &note,
            LifecycleEventType::DeliberationNoteCreated,
            fixture.now,
        )
        .unwrap();
    let output = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::DecisionProposal,
        &serde_json::json!({"statement": "fixture output"}),
        vec![artifact_ref(&note)],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();

    let entries = fixture.store.trajectory(&fixture.run.run_id).unwrap();
    let recent = fixture
        .store
        .recent_trajectory(&fixture.run.run_id, 2)
        .unwrap();
    assert!(recent.len() <= 2);
    assert_eq!(
        recent,
        entries[entries.len().saturating_sub(recent.len())..]
    );
    let recent_outputs = fixture
        .store
        .recent_artifacts_by_kind(ArtifactKind::DecisionProposal, 1)
        .unwrap();
    assert_eq!(
        recent_outputs.first().map(|artifact| &artifact.artifact_id),
        Some(&output.artifact_id)
    );
    assert!(entries
        .windows(2)
        .all(|pair| pair[0].cursor < pair[1].cursor));
    let model = entries
        .iter()
        .find(|entry| entry.artifact_kind == Some(ArtifactKind::AgentTurn))
        .expect("model trajectory entry");
    assert_eq!(
        model
            .model
            .as_ref()
            .and_then(|value| value.model_id.as_deref()),
        Some("fixture-model")
    );
    assert_eq!(model.latency_millis, Some(321));
    assert_eq!(model.input_tokens, Some(123));
    assert_eq!(model.output_tokens, Some(45));
    assert_eq!(model.phase.as_deref(), Some("draft"));
    assert_eq!(
        model.assistant_text.as_deref(),
        Some("bounded fixture research memo")
    );
    let model_json = serde_json::to_string(model).unwrap();
    assert!(!model_json.contains("provider-request"));
    assert!(!model_json.contains("provider-result"));

    let tool = entries
        .iter()
        .find(|entry| entry.tool.is_some())
        .expect("tool trajectory entry");
    assert_eq!(
        tool.tool
            .as_ref()
            .and_then(|value| value.call_id.as_deref()),
        Some("call-1")
    );
    let tool_json = serde_json::to_string(tool).unwrap();
    assert!(!tool_json.contains("tool-arguments"));
    assert!(!tool_json.contains("tool-result"));

    assert!(entries.iter().any(|entry| {
        entry
            .deliberation
            .as_ref()
            .is_some_and(|value| value.selected_path == "use the governed evidence")
    }));
    let output_entry = entries
        .iter()
        .find(|entry| entry.artifact_id.as_ref() == Some(&output.artifact_id))
        .expect("output trajectory entry");
    assert!(output_entry
        .output_refs
        .iter()
        .any(|reference| reference.artifact_id == note.artifact_id));
}

#[test]
fn trajectory_handles_opaque_agent_turn_payload() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();
    let turn = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::AgentTurn,
        &serde_json::json!("opaque fixture turn"),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &turn,
            LifecycleEventType::AgentTurnCompleted,
            fixture.now,
        )
        .unwrap();

    let entries = fixture.store.trajectory(&fixture.run.run_id).unwrap();
    let entry = entries
        .iter()
        .find(|entry| entry.artifact_id.as_ref() == Some(&turn.artifact_id))
        .expect("opaque agent turn trajectory entry");
    assert_eq!(entry.artifact_kind, Some(ArtifactKind::AgentTurn));
    assert!(entry.model.is_none());
}

#[test]
fn lifecycle_event_shapes_accept_current_store_exceptions() {
    let fixture = execution_commit_fixture();
    let valid_cases = [
        (LifecycleEventType::WorkflowCreated, false, false, true),
        (LifecycleEventType::RunCancelRequested, false, false, false),
        (LifecycleEventType::OutcomeWorkerEnqueued, true, false, true),
        (LifecycleEventType::TaskCancelled, true, false, false),
        (LifecycleEventType::TaskStarted, true, true, false),
        (LifecycleEventType::TaskRetryScheduled, true, true, false),
        (LifecycleEventType::TaskSucceeded, true, true, true),
        (LifecycleEventType::ArtifactCommitted, true, true, true),
        (LifecycleEventType::ExecutionCommitted, true, true, true),
        (LifecycleEventType::PolicyEvaluated, true, true, true),
        (LifecycleEventType::ShadowPairCompleted, true, true, true),
    ];

    for (event_type, has_task_id, has_attempt_id, has_artifact_id) in valid_cases {
        assert!(
            validate_event_shape(event_type, has_task_id, has_attempt_id, has_artifact_id).is_ok(),
            "unexpectedly rejected {:?}",
            event_type
        );
    }

    let invalid_cases = [
        (LifecycleEventType::WorkflowCreated, true, false, true),
        (LifecycleEventType::RunCancelRequested, false, false, true),
        (LifecycleEventType::OutcomeWorkerEnqueued, true, true, true),
        (LifecycleEventType::TaskStarted, true, false, false),
        (LifecycleEventType::ArtifactCommitted, true, false, true),
    ];

    for (event_type, has_task_id, has_attempt_id, has_artifact_id) in invalid_cases {
        assert!(
            matches!(
                validate_event_shape(event_type, has_task_id, has_attempt_id, has_artifact_id),
                Err(StoreError::InvalidLifecycleEventShape { event_type: value })
            if value == event_type.as_str()
            ),
            "unexpectedly accepted {:?}",
            event_type
        );
    }

    let mut connection = fixture.store.connection.lock().unwrap();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        append_event(
            &transaction,
            &fixture.permit.run_id,
            None,
            Some(&fixture.permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            None,
            fixture.now,
        ),
        Err(StoreError::Domain(DomainError::AttemptOriginWithoutTask))
    ));
}

#[test]
fn doctor_rejects_forged_paper_effect_event_shape() {
    let fixture = execution_commit_fixture();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, NULL, NULL, ?2, NULL, ?3)"#,
                params![
                    fixture.permit.run_id.0,
                    LifecycleEventType::ExecutionEffectIntent.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("invalid shape")
                && message.contains("execution.effect.intent")
    ));
}

#[test]
fn events_after_rejects_forged_paper_effect_event_shape() {
    let fixture = execution_commit_fixture();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, NULL, NULL, ?2, NULL, ?3)"#,
                params![
                    fixture.permit.run_id.0,
                    LifecycleEventType::ExecutionEffectSettled.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    assert!(matches!(
        fixture.store.events_after(&fixture.permit.run_id, 0, 100),
        Err(StoreError::InvalidLifecycleEventShape { event_type })
            if event_type == LifecycleEventType::ExecutionEffectSettled.as_str()
    ));
}

fn insert_paper_effect_event(
    fixture: &ExecutionCommitFixture,
    effect: &ArtifactRef,
    event_type: LifecycleEventType,
) {
    let mut connection = fixture.store.connection.lock().unwrap();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    transaction
        .execute(
            r#"INSERT INTO rebuild_events
                   (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
            params![
                fixture.permit.run_id.0,
                fixture.permit.task_id.0,
                fixture.permit.attempt_id.0,
                event_type.as_str(),
                effect.artifact_id.0.as_str(),
                fixture.now.to_rfc3339(),
            ],
        )
        .unwrap();
    transaction.commit().unwrap();
}

#[test]
fn paper_effect_history_requires_prior_intent_and_single_terminal() {
    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectSettled,
    );
    assert!(matches!(
        fixture.store.events_after(&fixture.permit.run_id, 0, 100),
        Err(StoreError::Integrity(message))
            if message.contains("has no prior intent")
    ));

    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectSettled,
    );
    insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("intent after terminal")
    ));

    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    insert_paper_effect_event(&fixture, &effect, LifecycleEventType::ExecutionEffectIntent);
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectSettled,
    );
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectRecovered,
    );
    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("duplicate terminal event")
    ));
}

#[test]
fn tool_lifecycle_allows_completed_call_and_blocks_pending_success() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let call = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolCall,
        &serde_json::json!({"call_id": "fixture-call"}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &call,
            LifecycleEventType::ToolCalled,
            fixture.now,
        )
        .unwrap();
    let result = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolResult,
        &serde_json::json!({"call_id": "fixture-call", "ok": true}),
        vec![artifact_ref(&call)],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &result,
            LifecycleEventType::ToolCompleted,
            fixture.now,
        )
        .unwrap();
    let output = lifecycle_test_artifact(&fixture, ArtifactLifecycle::RunScoped, "output");
    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();
    fixture.store.verify_integrity().unwrap();

    let pending = task_artifact_fixture(RunPurpose::Debug);
    let pending_call = permit_artifact(
        &pending.store,
        &pending.permit,
        ArtifactKind::ToolCall,
        &serde_json::json!({"call_id": "pending-call"}),
        vec![],
        ArtifactLifecycle::RunScoped,
        pending.now,
    );
    pending
        .store
        .write_task_artifact(
            &pending.permit,
            &pending_call,
            LifecycleEventType::ToolCalled,
            pending.now,
        )
        .unwrap();
    let output = lifecycle_test_artifact(&pending, ArtifactLifecycle::RunScoped, "pending-output");
    assert!(matches!(
        pending.store.commit_attempt(
            &pending.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            pending.now,
        ),
        Err(StoreError::Integrity(message)) if message.contains("pending tool calls")
    ));
    assert!(matches!(
        pending.store.artifact(&output.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    assert!(pending
        .store
        .events_after(&pending.run.run_id, 0, 100)
        .unwrap()
        .iter()
        .all(|event| event.event_type != LifecycleEventType::TaskSucceeded.as_str()));
}

#[test]
fn tool_lifecycle_failure_can_close_pending_call() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let call = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ToolCall,
        &serde_json::json!({"call_id": "failed-call"}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &call,
            LifecycleEventType::ToolCalled,
            fixture.now,
        )
        .unwrap();
    fixture
        .store
        .finish_task(&fixture.permit, TaskStatus::Failed, fixture.now)
        .unwrap();
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn events_after_validates_effect_history_beyond_page() {
    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    insert_paper_effect_event(
        &fixture,
        &effect,
        LifecycleEventType::ExecutionEffectSettled,
    );
    assert!(fixture
        .store
        .events_after(&fixture.permit.run_id, i64::MAX, 1)
        .is_err());
}

#[test]
fn events_after_rejects_tool_history_beyond_page() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, ?2, ?3, ?4, NULL, ?5)"#,
                params![
                    fixture.permit.run_id.0,
                    fixture.permit.task_id.0,
                    fixture.permit.attempt_id.0,
                    LifecycleEventType::ToolCalled.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    assert!(matches!(
        fixture.store.events_after(&fixture.run.run_id, i64::MAX, 1),
        Err(StoreError::Integrity(message)) if message.contains("has no artifact")
    ));
}

#[test]
fn paper_effect_intent_is_idempotent_and_settlement_requires_intent() {
    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();

    assert!(matches!(
        fixture.store.commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        ),
        Err(StoreError::MissingPaperEffectIntent(_))
    ));

    assert!(!fixture
        .store
        .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now,)
        .unwrap());
    assert!(fixture
        .store
        .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now,)
        .unwrap());

    let intent_count = fixture
        .store
        .events_after(&fixture.permit.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.event_type == LifecycleEventType::ExecutionEffectIntent.as_str()
                && event.artifact_id.as_ref() == Some(&effect.artifact_id)
        })
        .count();
    assert_eq!(intent_count, 1);
}

#[test]
fn paper_effect_settlement_rejects_non_paper_run() {
    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    fixture
        .store
        .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now)
        .unwrap();
    fixture
        .store
        .connection
        .lock()
        .unwrap()
        .execute(
            "UPDATE rebuild_runs SET purpose = ?1 WHERE run_id = ?2",
            params![enum_name(RunPurpose::Debug), fixture.permit.run_id.0],
        )
        .unwrap();

    assert!(matches!(
        fixture.store.commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        ),
        Err(StoreError::NonCanonicalLearningPurpose(RunPurpose::Debug))
    ));

    let events = fixture
        .store
        .events_after(&fixture.permit.run_id, 0, 100)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.artifact_id.as_ref() == Some(&effect.artifact_id))
            .filter(|event| {
                matches!(
                    event.event_type.as_str(),
                    "execution.effect.intent"
                        | "execution.effect.settled"
                        | "execution.effect.recovered"
                )
            })
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["execution.effect.intent"]
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn paper_effect_settlement_rolls_back_and_can_retry_after_failure() {
    let fixture = execution_commit_fixture();
    let effect = artifact_ref(&fixture.commitment);
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &fixture.commitment,
            LifecycleEventType::ExecutionCommitted,
            fixture.now,
        )
        .unwrap();
    fixture
        .store
        .record_paper_effect_intent(&fixture.lease, &fixture.permit, &effect, fixture.now)
        .unwrap();
    {
        let connection = fixture.store.connection.lock().unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER fail_paper_effect_settlement BEFORE INSERT ON rebuild_events \
                     WHEN NEW.event_type = 'execution.effect.settled' \
                     BEGIN SELECT RAISE(ABORT, 'injected settlement failure'); END;",
            )
            .unwrap();
    }

    assert!(matches!(
        fixture.store.commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        ),
        Err(StoreError::Sql(_))
    ));
    assert!(fixture
        .store
        .events_after(&fixture.permit.run_id, 0, 100)
        .unwrap()
        .iter()
        .all(|event| event.event_type != LifecycleEventType::ExecutionEffectSettled.as_str()));

    {
        let connection = fixture.store.connection.lock().unwrap();
        connection
            .execute_batch("DROP TRIGGER fail_paper_effect_settlement;")
            .unwrap();
    }
    fixture
        .store
        .commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        )
        .unwrap();
    assert!(matches!(
        fixture.store.commit_fenced_attempt_with_effect(
            &fixture.lease,
            &fixture.permit,
            std::slice::from_ref(&fixture.commitment),
            &effect,
            false,
            fixture.now,
        ),
        Err(StoreError::StalePermit(_)) | Err(StoreError::PaperEffectAlreadySettled(_))
    ));
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn metrics_are_empty_for_a_new_store() {
    let directory = tempdir().unwrap();
    let store = V2Store::open(directory.path()).unwrap();
    let metrics = store.metrics(Utc::now()).unwrap();
    assert!(metrics.run_counts.is_empty());
    assert!(metrics.task_counts.is_empty());
    assert!(metrics.attempt_counts.is_empty());
    assert_eq!(metrics.event_count, 0);
    assert_eq!(metrics.active_daemon_leases, 0);
}

#[test]
fn metrics_expose_failed_run_and_attempt_alerts() {
    let metrics = StoreMetrics {
        run_counts: BTreeMap::from([("failed".to_owned(), 2)]),
        task_counts: BTreeMap::new(),
        attempt_counts: BTreeMap::from([("failed".to_owned(), 1)]),
        event_count: 0,
        active_daemon_leases: 0,
    };
    let alerts = metrics.alerts();
    assert_eq!(alerts.len(), 2);
    assert_eq!(alerts[0].code, "failed_runs");
    assert_eq!(alerts[1].code, "failed_attempts");
}

#[test]
fn backup_restore_round_trip_runs_store_doctor() {
    let source_directory = tempdir().unwrap();
    let store = V2Store::open(source_directory.path()).unwrap();
    let blob = store.put_bytes(b"backup-fixture", "text/plain").unwrap();

    let backup_parent = tempdir().unwrap();
    let backup_root = backup_parent.path().join("backup");
    let manifest = store.backup_to(&backup_root).unwrap();
    assert_eq!(manifest.blob_count, 1);
    assert_eq!(manifest.blob_bytes, blob.bytes);

    let restore_parent = tempdir().unwrap();
    let restore_root = restore_parent.path().join("restored");
    let restored = V2Store::restore_from(&backup_root, &restore_root).unwrap();
    let restored_blob = restored.read_blob(&blob).unwrap();
    assert_eq!(restored_blob, b"backup-fixture");
    restored.verify_integrity().unwrap();
}

#[test]
fn open_existing_does_not_create_a_missing_store_root() {
    let parent = tempdir().unwrap();
    let root = parent.path().join("missing");
    assert!(matches!(
        V2Store::open_existing(&root),
        Err(StoreError::Io { .. })
    ));
    assert!(!root.exists());

    let initialized = parent.path().join("initialized");
    V2Store::open(&initialized).unwrap();
    let existing = V2Store::open_existing(&initialized).unwrap();
    assert!(existing.metrics(Utc::now()).unwrap().run_counts.is_empty());
}

#[test]
fn export_run_writes_manifest_and_non_model_payloads() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let export_parent = tempdir().unwrap();
    let target = export_parent.path().join("run-export");

    let manifest = fixture
        .store
        .export_run(&fixture.run.run_id, &target, false)
        .unwrap();

    assert_eq!(manifest.workflow.run.run_id, fixture.run.run_id);
    assert!(!manifest.include_raw_model);
    assert!(target.join(EXPORT_DATABASE_FILE).is_file());
    assert!(!target.join("manifest.json").exists());
    assert!(!target.join("artifacts").exists());
    assert!(manifest
        .artifacts
        .iter()
        .any(|entry| entry.payload_file.is_some()));
    let export = Connection::open(target.join(EXPORT_DATABASE_FILE)).unwrap();
    let stored_manifest = export
        .query_row(
            "SELECT value FROM export_metadata WHERE key = 'manifest'",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stored_manifest).unwrap()["workflow"]["run"]
            ["run_id"],
        serde_json::to_value(&fixture.run.run_id).unwrap()
    );
    assert!(export
        .query_row("SELECT COUNT(*) > 0 FROM rebuild_blobs", [], |row| {
            row.get::<_, bool>(0)
        })
        .unwrap());

    let existing = export_parent.path().join("existing");
    std::fs::create_dir(&existing).unwrap();
    assert!(matches!(
        fixture
            .store
            .export_run(&fixture.run.run_id, &existing, false),
        Err(StoreError::BackupTargetExists(_))
    ));
}

#[test]
fn export_run_rejects_raw_model_payloads_for_non_debug_runs() {
    let fixture = task_artifact_fixture(RunPurpose::PaperDryRun);
    let export_parent = tempdir().unwrap();
    let target = export_parent.path().join("run-export");

    assert!(matches!(
        fixture.store.export_run(&fixture.run.run_id, &target, true),
        Err(StoreError::RawModelExportNotAllowed(
            RunPurpose::PaperDryRun
        ))
    ));
}

fn task_artifact_fixture_with_retry(purpose: RunPurpose, max_attempts: u8) -> TaskArtifactFixture {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let mut graph = graph();
    graph.nodes[0].retry.max_attempts = max_attempts;
    let graph_artifact = artifact(
        &store,
        ArtifactKind::WorkflowGraph,
        &serde_json::to_string(&graph).unwrap(),
        None,
    );
    let run = StoredRun {
        run_id: RunId::new(),
        purpose,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run: run.clone(),
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let permit = store
        .claim_next_task("lifecycle-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    TaskArtifactFixture {
        _root: root,
        store,
        run,
        permit,
        now,
    }
}

fn agent_turn_artifact(fixture: &TaskArtifactFixture, label: &str) -> Artifact {
    permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::AgentTurn,
        &serde_json::json!({"label": label}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    )
}

#[test]
fn agent_turn_started_is_durable_and_duplicate_write_rolls_back() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();

    let events = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap();
    assert_eq!(events.len(), 3);
    let started = events
        .iter()
        .find(|event| event.event_type == LifecycleEventType::AgentTurnStarted.as_str())
        .unwrap();
    assert_eq!(started.task_id, Some(fixture.permit.task_id.clone()));
    assert_eq!(started.attempt_id, Some(fixture.permit.attempt_id.clone()));
    assert!(started.artifact_id.is_none());

    assert!(matches!(
        fixture.store.append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    let after_duplicate = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap();
    assert_eq!(after_duplicate.len(), events.len());
    assert_eq!(
        after_duplicate
            .iter()
            .filter(|event| event.event_type == LifecycleEventType::AgentTurnStarted.as_str())
            .count(),
        1
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn agent_turn_rejects_distinct_terminal_without_new_start() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();

    let completed = agent_turn_artifact(&fixture, "completed");
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &completed,
            LifecycleEventType::AgentTurnCompleted,
            fixture.now,
        )
        .unwrap();

    let failed = agent_turn_artifact(&fixture, "failed");
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &failed,
            LifecycleEventType::AgentTurnFailed,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));

    let events = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == LifecycleEventType::AgentTurnFailed.as_str())
            .count(),
        0
    );
    assert!(matches!(
        fixture.store.artifact(&failed.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn agent_turn_started_rejects_stale_epoch_without_writing() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let mut stale = fixture.permit.clone();
    stale.epoch += 1;
    let before = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .len();

    assert!(matches!(
        fixture
            .store
            .append_task_event(&stale, LifecycleEventType::AgentTurnStarted, fixture.now,),
        Err(StoreError::StalePermit(_))
    ));
    assert_eq!(
        fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len(),
        before
    );
    assert_eq!(
        fixture
            .store
            .workflow_snapshot(&fixture.run.run_id)
            .unwrap()
            .tasks[0]
            .status,
        TaskStatus::Running
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn pending_agent_turn_blocks_success_until_completed() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();

    assert!(matches!(
        fixture
            .store
            .finish_task(&fixture.permit, TaskStatus::Succeeded, fixture.now),
        Err(StoreError::Integrity(_))
    ));
    assert_eq!(
        fixture
            .store
            .workflow_snapshot(&fixture.run.run_id)
            .unwrap()
            .tasks[0]
            .status,
        TaskStatus::Running
    );

    let turn = agent_turn_artifact(&fixture, "completed");
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &turn,
            LifecycleEventType::AgentTurnCompleted,
            fixture.now,
        )
        .unwrap();
    fixture
        .store
        .finish_task(&fixture.permit, TaskStatus::Succeeded, fixture.now)
        .unwrap();
    assert_eq!(
        fixture
            .store
            .workflow_snapshot(&fixture.run.run_id)
            .unwrap()
            .tasks[0]
            .status,
        TaskStatus::Succeeded
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn retry_attempts_close_started_turns_and_preserve_attempt_order() {
    let fixture = task_artifact_fixture_with_retry(RunPurpose::Debug, 2);
    fixture
        .store
        .append_task_event(
            &fixture.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now,
        )
        .unwrap();
    assert_eq!(
        fixture
            .store
            .retry_task(&fixture.permit, fixture.now, fixture.now)
            .unwrap(),
        RetryTaskResult::Requeued
    );

    let second = fixture
        .store
        .claim_next_task(
            "lifecycle-worker-2",
            fixture.now + Duration::seconds(1),
            Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    assert_ne!(fixture.permit.attempt_id, second.permit.attempt_id);
    fixture
        .store
        .append_task_event(
            &second.permit,
            LifecycleEventType::AgentTurnStarted,
            fixture.now + Duration::seconds(1),
        )
        .unwrap();
    let second_fixture = TaskArtifactFixture {
        _root: fixture._root,
        store: fixture.store,
        run: fixture.run,
        permit: second.permit,
        now: fixture.now + Duration::seconds(1),
    };
    let turn = agent_turn_artifact(&second_fixture, "retry-completed");
    second_fixture
        .store
        .write_task_artifact(
            &second_fixture.permit,
            &turn,
            LifecycleEventType::AgentTurnCompleted,
            second_fixture.now,
        )
        .unwrap();
    second_fixture
        .store
        .finish_task(
            &second_fixture.permit,
            TaskStatus::Succeeded,
            second_fixture.now,
        )
        .unwrap();

    let events = second_fixture
        .store
        .events_after(&second_fixture.run.run_id, 0, 100)
        .unwrap();
    let lifecycle: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event.lifecycle_kind().unwrap(),
                LifecycleEventType::AgentTurnStarted
                    | LifecycleEventType::AgentTurnCompleted
                    | LifecycleEventType::TaskRetryScheduled
                    | LifecycleEventType::TaskSucceeded
            )
        })
        .collect();
    assert_eq!(
        lifecycle
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            LifecycleEventType::AgentTurnStarted.as_str(),
            LifecycleEventType::TaskRetryScheduled.as_str(),
            LifecycleEventType::AgentTurnStarted.as_str(),
            LifecycleEventType::AgentTurnCompleted.as_str(),
            LifecycleEventType::TaskSucceeded.as_str(),
        ]
    );
    assert_eq!(
        lifecycle[0].attempt_id,
        Some(fixture.permit.attempt_id.clone())
    );
    assert_eq!(
        lifecycle[2].attempt_id,
        Some(second_fixture.permit.attempt_id.clone())
    );
    assert_eq!(
        lifecycle[3].attempt_id,
        Some(second_fixture.permit.attempt_id.clone())
    );
    second_fixture.store.verify_integrity().unwrap();
}

#[test]
fn recovery_and_cancel_close_unfinished_agent_turns() {
    let recovered = task_artifact_fixture(RunPurpose::Debug);
    recovered
        .store
        .append_task_event(
            &recovered.permit,
            LifecycleEventType::AgentTurnStarted,
            recovered.now,
        )
        .unwrap();
    assert_eq!(
        recovered
            .store
            .recover_expired_tasks(recovered.now + Duration::seconds(31))
            .unwrap(),
        1
    );
    let recovery_events = recovered
        .store
        .events_after(&recovered.run.run_id, 0, 100)
        .unwrap();
    assert!(recovery_events.iter().any(|event| {
        matches!(
            event.lifecycle_kind().unwrap(),
            LifecycleEventType::TaskRecovered | LifecycleEventType::TaskRecoveryExhausted
        )
    }));
    recovered.store.verify_integrity().unwrap();

    let cancelled = task_artifact_fixture(RunPurpose::Debug);
    cancelled
        .store
        .append_task_event(
            &cancelled.permit,
            LifecycleEventType::AgentTurnStarted,
            cancelled.now,
        )
        .unwrap();
    cancelled
        .store
        .finish_task(&cancelled.permit, TaskStatus::Cancelled, cancelled.now)
        .unwrap();
    assert_eq!(
        cancelled
            .store
            .workflow_snapshot(&cancelled.run.run_id)
            .unwrap()
            .tasks[0]
            .status,
        TaskStatus::Cancelled
    );
    cancelled.store.verify_integrity().unwrap();
}

#[test]
fn context_lifecycle_validator_rejects_wrong_kind_and_preserves_legacy_manifest() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let wrong = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::Decision,
        &serde_json::json!({"wrong": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    let before = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .len();
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &wrong,
            LifecycleEventType::ContextManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert_eq!(
        fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len(),
        before
    );
    assert!(matches!(
        fixture.store.artifact(&wrong.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let manifest = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"manifest": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &manifest,
            LifecycleEventType::ContextManifestCreated,
            fixture.now,
        )
        .unwrap();
    fixture
        .store
        .commit_attempt(
            &fixture.permit,
            std::slice::from_ref(&manifest),
            TaskStatus::Succeeded,
            fixture.now,
        )
        .unwrap();
    let proof = fixture
        .store
        .current_succeeded_attempt(&fixture.run.run_id, &fixture.permit.task_id)
        .unwrap();
    assert_eq!(
        proof.context_manifest,
        Some(ArtifactRef {
            artifact_id: manifest.artifact_id,
            kind: ArtifactKind::ContextManifest,
        })
    );
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn gate_lifecycle_validator_enforces_event_kind_and_legacy_aliases() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let wrong = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::Decision,
        &serde_json::json!({"wrong": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    let before = fixture
        .store
        .events_after(&fixture.run.run_id, 0, 100)
        .unwrap()
        .len();
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &wrong,
            LifecycleEventType::ExecutionPlanCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert_eq!(
        fixture
            .store
            .events_after(&fixture.run.run_id, 0, 100)
            .unwrap()
            .len(),
        before
    );
    assert!(matches!(
        fixture.store.artifact(&wrong.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let context = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionContext,
        &serde_json::json!({"context": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &context,
            LifecycleEventType::ExecutionContextCreated,
            fixture.now,
        )
        .unwrap();

    let verdict = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ExecutionVerdict,
        &serde_json::json!({"verdict": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &verdict,
            LifecycleEventType::ExecutionVerdictCreated,
            fixture.now,
        )
        .unwrap();
    fixture.store.verify_integrity().unwrap();
}

#[test]
fn gate_lifecycle_validator_rejects_forged_origin() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let foreign_run = RunId::new();
    let forged = Artifact::new(
        ArtifactKind::ExecutionPlan,
        fixture
            .store
            .put_json(&serde_json::json!({"plan": true}))
            .unwrap(),
        "fixture.plan",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: fixture.now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: fixture.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(foreign_run),
            task_id: Some(fixture.permit.task_id.clone()),
            attempt_id: Some(fixture.permit.attempt_id.clone()),
            contract_hash: fixture.permit.contract_hash.clone(),
        }),
        vec![],
        fixture.now,
    )
    .unwrap();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_artifact(&transaction, &forged).unwrap();
        transaction
            .execute(
                r#"INSERT INTO rebuild_events
                       (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                params![
                    fixture.run.run_id.0,
                    fixture.permit.task_id.0,
                    fixture.permit.attempt_id.0,
                    LifecycleEventType::ExecutionPlanCreated.as_str(),
                    forged.artifact_id.0.as_str(),
                    fixture.now.to_rfc3339(),
                ],
            )
            .unwrap();
        transaction.commit().unwrap();
    }
    assert!(matches!(
        fixture.store.events_after(&fixture.run.run_id, 0, 100),
        Err(StoreError::Integrity(message))
            if message.contains("origin")
    ));
    assert!(matches!(
        fixture.store.verify_integrity(),
        Err(StoreError::Integrity(message))
            if message.contains("origin")
    ));
}

#[test]
fn context_child_and_repair_lifecycle_validator_enforces_lineage_and_sources() {
    let fixture = task_artifact_fixture(RunPurpose::Debug);
    let parent = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"parent": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &parent,
            LifecycleEventType::ContextManifestCreated,
            fixture.now,
        )
        .unwrap();
    let parent_ref = ArtifactRef {
        artifact_id: parent.artifact_id.clone(),
        kind: ArtifactKind::ContextManifest,
    };

    let missing_parent = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"missing_parent": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &missing_parent,
            LifecycleEventType::ContextChildManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&missing_parent.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let foreign_run = RunId::new();
    let foreign_parent = Artifact::new(
        ArtifactKind::ContextManifest,
        fixture
            .store
            .put_json(&serde_json::json!({"foreign": true}))
            .unwrap(),
        "fixture.foreign",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: fixture.now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: fixture.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(foreign_run),
            task_id: Some(fixture.permit.task_id.clone()),
            attempt_id: Some(fixture.permit.attempt_id.clone()),
            contract_hash: fixture.permit.contract_hash.clone(),
        }),
        vec![],
        fixture.now,
    )
    .unwrap();
    {
        let mut connection = fixture.store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_artifact(&transaction, &foreign_parent).unwrap();
        transaction.commit().unwrap();
    }
    let foreign_child = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"foreign_parent": true}),
        vec![ArtifactRef {
            artifact_id: foreign_parent.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        }],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &foreign_child,
            LifecycleEventType::ContextChildManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&foreign_child.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let child = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextManifest,
        &serde_json::json!({"child": true}),
        vec![parent_ref],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &child,
            LifecycleEventType::ContextChildManifestCreated,
            fixture.now,
        )
        .unwrap();
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &child,
            LifecycleEventType::ContextChildManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));

    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &parent,
            LifecycleEventType::ContextManifestCreated,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));

    let empty_repair = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextRepair,
        &serde_json::json!({"empty": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    assert!(matches!(
        fixture.store.write_task_artifact(
            &fixture.permit,
            &empty_repair,
            LifecycleEventType::ContextRepaired,
            fixture.now,
        ),
        Err(StoreError::Integrity(_))
    ));
    assert!(matches!(
        fixture.store.artifact(&empty_repair.artifact_id),
        Err(StoreError::MissingArtifact(_))
    ));

    let source = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::NormalizedEvidence,
        &serde_json::json!({"source": true}),
        vec![],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &source,
            LifecycleEventType::Evidence,
            fixture.now,
        )
        .unwrap();
    let repair = permit_artifact(
        &fixture.store,
        &fixture.permit,
        ArtifactKind::ContextRepair,
        &serde_json::json!({"repair": true}),
        vec![ArtifactRef {
            artifact_id: source.artifact_id,
            kind: ArtifactKind::NormalizedEvidence,
        }],
        ArtifactLifecycle::RunScoped,
        fixture.now,
    );
    fixture
        .store
        .write_task_artifact(
            &fixture.permit,
            &repair,
            LifecycleEventType::ContextRepaired,
            fixture.now,
        )
        .unwrap();
    fixture.store.verify_integrity().unwrap();
}
