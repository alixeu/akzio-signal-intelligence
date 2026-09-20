use akzio_domain::*;
use akzio_store::*;
use chrono::{Duration, Utc};
use std::sync::{Arc, Barrier};

#[test]
fn research_quality_budget_is_durable_and_never_refunds_started_calls() {
    let c = Case::new();
    c.control(DebugAction::Step, Some(0));
    let task = c.claim().unwrap();
    for index in 0..40 {
        assert_eq!(
            c.store
                .reserve_research_quality_call(
                    &task.permit,
                    &serde_json::json!({"synthetic":true,"index":index})
                )
                .unwrap(),
            index + 1
        );
    }
    let reopened = Store::open(c.store.root()).unwrap();
    assert!(reopened
        .reserve_research_quality_call(&task.permit, &serde_json::json!({"resumed":true}))
        .is_err());
    assert_eq!(
        c.store
            .research_quality_records("research.quality.call.started")
            .unwrap()
            .len(),
        40
    );
}

struct Case {
    store: Store,
    run: RunId,
    tasks: Vec<TaskId>,
    identity: ContentHash,
}
impl Case {
    fn new() -> Self {
        Self::with_purpose(RunPurpose::Debug)
    }
    fn with_purpose(purpose: RunPurpose) -> Self {
        let run = RunId::new();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug-control-tests")
            .join(&run.0);
        let store = Store::open(root).unwrap();
        let env = if purpose == RunPurpose::Debug {
            store.configure_debug_environment(true).unwrap().unwrap()
        } else {
            String::new()
        };
        let now = Utc::now();
        let tasks = (0..3).map(|_| TaskId::new()).collect::<Vec<_>>();
        let nodes = tasks
            .iter()
            .enumerate()
            .map(|(i, id)| WorkflowNode {
                spec: None,
                task_id: id.clone(),
                recipe_id: TaskRecipeId::new("test.stage").unwrap(),
                contract_hash: None,
                objective: format!("stage {i} [research_horizon=t1]"),
                dependencies: if i == 2 {
                    vec![tasks[0].clone()]
                } else {
                    vec![]
                },
                input_artifacts: vec![],
                priority: 50,
                budget: TaskBudget {
                    max_input_tokens: 1000,
                    max_output_tokens: 100,
                    max_wall_time_secs: 30,
                    max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(2),
                },
                retry: RetryPolicy {
                    max_attempts: 3,
                    initial_backoff_ms: 1,
                    retry_transport: true,
                    retry_rate_limited: true,
                    retry_invalid_output: true,
                },
                on_failure: FailureDisposition::FailTask,
                parent_task_id: None,
            })
            .collect::<Vec<_>>();
        let graph = WorkflowGraph {
            definition_version: None,
            agent_budgets: Default::default(),
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id: "controller-test".into(),
            nodes: nodes.clone(),
        };
        let artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.stage_json(&graph).unwrap(),
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(ArtifactOrigin {
                run_id: Some(run.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
            now,
        )
        .unwrap();
        let workflow = WorkflowCommit {
            run: StoredRun {
                run_id: run.clone(),
                purpose,
                topology_id: graph.topology_id,
                graph_artifact_id: artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: artifact,
            nodes,
        };
        let identity = ContentHash::of_bytes(b"test-runtime");
        if purpose == RunPurpose::Debug {
            store
                .commit_debug_experiment(
                    &workflow,
                    &[],
                    &DebugSessionIdentity {
                        version: 1,
                        debug_session_id: format!("debug-{run}"),
                        store_identity: env,
                        run_id: run.clone(),
                        run_purpose: RunPurpose::Debug,
                        llm_mode: DebugLlmMode::Fixture,
                        broker_write_policy: DebugBrokerPolicy::Forbidden,
                        learning_scope: DebugLearningScope::Isolated,
                        code_revision: "test".into(),
                        runtime_identity: identity.clone(),
                        decision_policy_status: "unconfigured".into(),
                        decision_policy_input_hash: None,
                        decision_policy_artifact: None,
                        contract_hashes: vec![],
                        dataset: vec![],
                        parent_run_id: None,
                        parent_task_id: None,
                        parent_artifacts: vec![],
                        reason: None,
                        created_at: now,
                    },
                )
                .unwrap();
        } else {
            store.commit_workflow(&workflow).unwrap();
        }
        Self {
            store,
            run,
            tasks,
            identity,
        }
    }
    fn control(&self, action: DebugAction, task: Option<usize>) -> DebugSession {
        let revision = self
            .store
            .debug_session(&self.run)
            .unwrap()
            .unwrap()
            .revision;
        self.store
            .debug_control(
                &self.run,
                &DebugControlRequest {
                    action,
                    expected_revision: revision,
                    task_id: task.map(|i| self.tasks[i].clone()),
                },
                &self.identity,
                Utc::now(),
            )
            .unwrap()
    }
    fn claim(&self) -> Option<ClaimedAttempt> {
        self.store
            .claim_next_task_for_workload_with_identity(
                "test-worker",
                Utc::now(),
                Duration::seconds(30),
                TaskWorkload::Any,
                Some(&self.identity),
            )
            .unwrap()
    }
    fn inspect(&self) -> DebugRunView {
        self.store
            .debug_inspect(&self.run, None, None, Utc::now())
            .unwrap()
    }
}

#[test]
fn t01_pause_prevents_new_claims_and_drains_without_cancelling() {
    let c = Case::new();
    c.control(DebugAction::Resume, None);
    let a = c.claim().unwrap();
    assert_eq!(
        c.control(DebugAction::Pause, None).status,
        DebugStatus::PauseRequested
    );
    assert!(c.claim().is_none());
    c.store.validate_task_permit(&a.permit).unwrap();
    c.store
        .finish_task(&a.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
    assert_eq!(c.inspect().session.status, DebugStatus::Paused);
    assert!(c.claim().is_none());
}

#[test]
fn expired_task_permit_cannot_write_or_heartbeat_before_recovery() {
    let c = Case::new();
    c.control(DebugAction::Resume, None);
    let claim = c
        .store
        .claim_next_task_for_workload_with_identity(
            "expiry-worker",
            Utc::now(),
            Duration::milliseconds(1),
            TaskWorkload::Any,
            Some(&c.identity),
        )
        .unwrap()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(matches!(
        c.store.validate_task_permit(&claim.permit),
        Err(StoreError::StalePermit(_))
    ));
    assert!(matches!(
        c.store
            .finish_task(&claim.permit, TaskStatus::Succeeded, Utc::now()),
        Err(StoreError::StalePermit(_))
    ));
    assert!(matches!(
        c.store
            .heartbeat_task(&claim.permit, Utc::now() + Duration::seconds(30)),
        Err(StoreError::StalePermit(_))
    ));
    assert_eq!(c.store.recover_expired_tasks(Utc::now()).unwrap(), 1);
    let replacement = c.claim().unwrap();
    c.store.validate_task_permit(&replacement.permit).unwrap();
    c.store
        .finish_task(&replacement.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
}

#[test]
fn heartbeat_checks_complete_identity_and_cannot_shorten_existing_lease() {
    let c = Case::new();
    c.control(DebugAction::Resume, None);
    let claim = c.claim().unwrap();
    let mut wrong_run = claim.permit.clone();
    wrong_run.run_id = RunId::new();
    assert!(matches!(
        c.store
            .heartbeat_task(&wrong_run, Utc::now() + Duration::seconds(30)),
        Err(StoreError::StalePermit(_))
    ));
    let mut wrong_contract = claim.permit.clone();
    wrong_contract.contract_hash = Some(ContentHash::of_bytes(b"wrong contract"));
    assert!(matches!(
        c.store
            .heartbeat_task(&wrong_contract, Utc::now() + Duration::seconds(30)),
        Err(StoreError::StalePermit(_))
    ));
    let now = Utc::now();
    c.store
        .heartbeat_task(&claim.permit, now + Duration::seconds(60))
        .unwrap();
    c.store
        .heartbeat_task(&claim.permit, now + Duration::seconds(20))
        .unwrap();
    assert_eq!(
        c.store
            .recover_expired_tasks(now + Duration::seconds(40))
            .unwrap(),
        0
    );
    c.store
        .finish_task(&claim.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
}

#[test]
fn t02_exact_step_never_releases_sibling_or_downstream() {
    let c = Case::new();
    c.control(DebugAction::Step, Some(0));
    let a = c.claim().unwrap();
    assert_eq!(a.node.task_id, c.tasks[0]);
    assert!(c.claim().is_none());
    c.store
        .finish_task(&a.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
    let view = c.inspect();
    assert_eq!(view.session.status, DebugStatus::Paused);
    assert_eq!(view.nodes.iter().filter(|n| n.step_eligible).count(), 2);
    assert_eq!(
        view.nodes.iter().map(|n| n.attempts.len()).sum::<usize>(),
        1
    );
    assert!(c.claim().is_none());
}

#[test]
fn t03_eight_connections_atomically_consume_one_permission() {
    let c = Case::new();
    c.control(DebugAction::Step, Some(0));
    let stores = (0..8)
        .map(|_| Store::open(c.store.root()).unwrap())
        .collect::<Vec<_>>();
    let barrier = Arc::new(Barrier::new(8));
    let threads = stores
        .into_iter()
        .enumerate()
        .map(|(i, store)| {
            let b = barrier.clone();
            let identity = c.identity.clone();
            std::thread::spawn(move || {
                b.wait();
                store
                    .claim_next_task_for_workload_with_identity(
                        &format!("worker-{i}"),
                        Utc::now(),
                        Duration::seconds(30),
                        TaskWorkload::Any,
                        Some(&identity),
                    )
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        threads
            .into_iter()
            .filter_map(|t| t.join().unwrap())
            .count(),
        1
    );
}

#[test]
fn t04_duplicate_step_cas_and_late_replay_conflict() {
    let c = Case::new();
    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let store = Store::open(c.store.root()).unwrap();
            let run = c.run.clone();
            let task = c.tasks[0].clone();
            let identity = c.identity.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                store.debug_control(
                    &run,
                    &DebugControlRequest {
                        action: DebugAction::Step,
                        expected_revision: 0,
                        task_id: Some(task),
                    },
                    &identity,
                    Utc::now(),
                )
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        handles
            .into_iter()
            .map(|h| h.join().unwrap().is_ok() as usize)
            .sum::<usize>(),
        1
    );
    let a = c.claim().unwrap();
    c.store
        .finish_task(&a.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
    assert!(c
        .store
        .debug_control(
            &c.run,
            &DebugControlRequest {
                action: DebugAction::Step,
                expected_revision: 0,
                task_id: Some(c.tasks[0].clone())
            },
            &c.identity,
            Utc::now()
        )
        .unwrap_err()
        .to_string()
        .contains("revision_conflict"));
}

#[test]
fn t05_unsatisfied_dependency_is_blocked() {
    let c = Case::new();
    let err = c
        .store
        .debug_control(
            &c.run,
            &DebugControlRequest {
                action: DebugAction::Step,
                expected_revision: 0,
                task_id: Some(c.tasks[2].clone()),
            },
            &c.identity,
            Utc::now(),
        )
        .unwrap_err();
    assert!(err.to_string().contains("dependencies_not_satisfied"));
    assert!(c.claim().is_none());
}

#[test]
fn t06_pause_and_consumed_step_survive_restart_and_recovery() {
    let c = Case::new();
    let reopened = Store::open(c.store.root()).unwrap();
    assert_eq!(
        reopened.debug_session(&c.run).unwrap().unwrap().status,
        DebugStatus::Paused
    );
    c.control(DebugAction::Step, Some(0));
    let a = c.claim().unwrap();
    assert_eq!(
        reopened
            .recover_expired_tasks(Utc::now() + Duration::seconds(60))
            .unwrap(),
        1
    );
    let view = c.inspect();
    assert_eq!(view.session.status, DebugStatus::Paused);
    assert_eq!(
        view.nodes
            .iter()
            .find(|n| n.task.node.task_id == a.node.task_id)
            .unwrap()
            .attempts[0]
            .status,
        "abandoned"
    );
    assert!(c.claim().is_none());
}

#[test]
fn t07_resume_keeps_success_and_requires_matching_runtime() {
    let c = Case::new();
    c.control(DebugAction::Step, Some(0));
    let a = c.claim().unwrap();
    c.store
        .finish_task(&a.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
    c.control(DebugAction::Resume, None);
    assert!(c
        .store
        .claim_next_task_for_workload(
            "unidentified-worker",
            Utc::now(),
            Duration::seconds(30),
            TaskWorkload::Any
        )
        .unwrap()
        .is_none());
    assert!(c
        .store
        .claim_next_task_for_workload_with_identity(
            "changed-runtime",
            Utc::now(),
            Duration::seconds(30),
            TaskWorkload::Any,
            Some(&ContentHash::of_bytes(b"other"))
        )
        .unwrap()
        .is_none());
    for _ in 0..2 {
        let next = c.claim().unwrap();
        assert_ne!(next.node.task_id, a.node.task_id);
        c.store
            .finish_task(&next.permit, TaskStatus::Succeeded, Utc::now())
            .unwrap();
    }
    assert!(c.claim().is_none());
    assert_eq!(c.inspect().session.status, DebugStatus::Completed);
}

#[test]
fn t08_retry_preserves_attempt_history_and_limits() {
    let c = Case::new();
    c.control(DebugAction::Step, Some(0));
    let first = c.claim().unwrap();
    c.store
        .retry_task(&first.permit, Utc::now(), Utc::now())
        .unwrap();
    assert_eq!(c.inspect().session.status, DebugStatus::Paused);
    c.control(DebugAction::RetryNode, Some(0));
    let second = c.claim().unwrap();
    assert_ne!(first.permit.attempt_id, second.permit.attempt_id);
    c.store
        .finish_task(&second.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
    let view = c.inspect();
    let node = view
        .nodes
        .iter()
        .find(|n| n.task.node.task_id == first.node.task_id)
        .unwrap();
    assert_eq!(node.attempts.len(), 2);
    assert_eq!(node.attempts[0].status, "retried");
    assert_eq!(node.attempts[1].status, "succeeded");
    assert!(!node.retry_eligible);
    assert_eq!(node.task.node.budget.max_input_tokens, 1000);
    // Doctor must read AttemptRelation payloads through its already-held
    // connection rather than attempting a nested Store connection acquisition.
    c.store.verify_integrity().unwrap();
}

#[test]
fn t10_default_policy_blocks_at_store_authority() {
    let c = Case::new();
    assert!(matches!(
        c.store.assert_debug_broker_write(&c.run),
        Err(StoreError::DebugBrokerWriteForbidden)
    ));
    assert!(matches!(
        c.store.assert_debug_broker_write(&RunId::new()),
        Err(StoreError::DebugBrokerWriteForbidden)
    ));
    c.control(DebugAction::Step, Some(0));
    let a = c.claim().unwrap();
    assert!(c
        .store
        .block_debug_broker_task(&a.permit, &[], Utc::now())
        .unwrap());
    c.store
        .defer_task(&a.permit, Utc::now() + Duration::seconds(10), Utc::now())
        .unwrap();
    let view = c.inspect();
    assert_eq!(view.session.status, DebugStatus::Paused);
    assert!(view
        .acceptance
        .iter()
        .any(|a| a.business_result == "Accepted" && a.test_result == AcceptanceResult::Blocked));
}

#[test]
fn t13_isolation_cannot_be_reopened_as_production() {
    let c = Case::new();
    assert!(c.store.configure_debug_environment(false).is_err());
    assert!(c.store.debug_learning_isolated(&c.run).unwrap());
    let now = Utc::now();
    let source = Artifact::new(
        ArtifactKind::SemanticDetail,
        c.store
            .stage_json(&serde_json::json!({"operator":"test"}))
            .unwrap(),
        "test.operator",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".into(),
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
    let lesson = Lesson {
        schema_version: DOMAIN_SCHEMA_VERSION,
        lesson_id: LessonId("test-lesson".into()),
        origin: LessonOrigin::Operator,
        lifecycle: LessonLifecycle::Active,
        title: "test".into(),
        statement: "test".into(),
        rationale: "test".into(),
        recommended_behavior: "test".into(),
        exclusions: vec![],
        scope: LessonScope {
            assets: Default::default(),
            horizons: Default::default(),
            regimes: Default::default(),
            decision_stages: Default::default(),
        },
        source_refs: vec![ArtifactRef {
            artifact_id: source.artifact_id.clone(),
            kind: source.kind,
        }],
        supersedes: vec![],
        conflicts_with: vec![],
        confidence_ppm: 500_000,
        authored_by: Some("test".into()),
        approved_by: Some("test".into()),
        created_at: now,
        updated_at: now,
        governance: Some(LessonGovernance::operator_reviewed(now)),
    };
    lesson.validate().unwrap();
    assert!(c
        .store
        .write_lesson(&lesson, &source, now)
        .unwrap_err()
        .to_string()
        .contains("debug_learning_isolated"));
    c.control(DebugAction::Step, Some(0));
    let task = c.claim().unwrap();
    let commit = PolicyEvaluationCommit {
        complete_task: true,
        permit: task.permit,
        outcome: source.clone(),
        final_retrospective: source.clone(),
        experience: source.clone(),
        evaluation: source,
        candidate_policy: None,
        lesson_evidence: vec![],
        subject: PolicySubject::Contract(c.identity.clone()),
        from: PolicyState::Contract(CandidatePolicyState::Candidate),
        to: PolicyState::Contract(CandidatePolicyState::Candidate),
        pair_snapshot: PolicyShadowPairSnapshot {
            after_cursor: 0,
            through_cursor: 0,
            counts_by_horizon: [0; 3],
        },
        transition: None,
        completed_at: now,
    };
    assert!(c
        .store
        .record_policy_evaluation_fenced(None, &commit)
        .unwrap_err()
        .to_string()
        .contains("isolated_learning_cannot_enter_canonical_policy"));
}

#[test]
fn t12_old_completed_paper_schedule_is_discovered_without_new_t0() {
    // A Store scheduling fixture, not a claim that evidence/gates/LLM passed.
    let c = Case::with_purpose(RunPurpose::Paper);
    let first = c.claim().unwrap();
    let now = Utc::now();
    let mut inputs = Vec::new();
    for kind in [
        ArtifactKind::Decision,
        ArtifactKind::DecisionContext,
        ArtifactKind::ExecutionContext,
        ArtifactKind::ExecutionVerdict,
    ] {
        inputs.push(
            Artifact::new(
                kind,
                c.store
                    .stage_json(&serde_json::json!({"fixture":"scheduler-only"}))
                    .unwrap(),
                "test.scheduler",
                ArtifactLifecycle::Canonical,
                ArtifactProvenance {
                    source_family: "akzio.fixture".into(),
                    observed_at: None,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: None,
                },
                Some(first.permit.artifact_origin()),
                vec![],
                now,
            )
            .unwrap(),
        );
    }
    let refs = inputs
        .iter()
        .map(|a| ArtifactRef {
            artifact_id: a.artifact_id.clone(),
            kind: a.kind,
        })
        .collect::<Vec<_>>();
    let schedule = OutcomeSchedule {
        schema_version: DOMAIN_SCHEMA_VERSION,
        outcome_id: OutcomeId::new(),
        decision: refs[0].clone(),
        decision_context: refs[1].clone(),
        execution_context: refs[2].clone(),
        execution: OutcomeExecutionLineage::NoOrder {
            execution_verdict: refs[3].clone(),
        },
        baseline_trading_day: now.date_naive(),
        created_at: now,
    };
    schedule.validate().unwrap();
    inputs.push(
        Artifact::new(
            ArtifactKind::OutcomeSchedule,
            c.store.stage_json(&schedule).unwrap(),
            "learning.outcome_schedule",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.learning".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(first.permit.artifact_origin()),
            refs,
            now,
        )
        .unwrap(),
    );
    c.store
        .commit_attempt(&first.permit, &inputs, TaskStatus::Succeeded, now)
        .unwrap();
    for _ in 0..2 {
        let a = c.claim().unwrap();
        c.store
            .finish_task(&a.permit, TaskStatus::Succeeded, Utc::now())
            .unwrap();
    }
    assert_eq!(
        c.store.workflow_snapshot(&c.run).unwrap().status,
        WorkflowStatus::Completed
    );
    assert_eq!(
        c.store.ensure_pending_outcome_workers(Utc::now()).unwrap(),
        1
    );
    assert_eq!(
        c.store.ensure_pending_outcome_workers(Utc::now()).unwrap(),
        0
    );
    let outcome = c
        .store
        .claim_next_task_for_workload(
            "outcome-only",
            Utc::now(),
            Duration::seconds(30),
            TaskWorkload::Outcome,
        )
        .unwrap()
        .unwrap();
    assert_eq!(outcome.run_id, c.run);
    assert_eq!(outcome.node.recipe_id.as_str(), "learning.outcome_worker");
    assert_eq!(
        c.store.workflow_snapshot(&c.run).unwrap().status,
        WorkflowStatus::Completed
    );
    assert_eq!(
        c.store
            .current_succeeded_attempt(&c.run, &first.node.task_id)
            .unwrap()
            .outputs
            .len(),
        inputs.len()
    );
}

#[test]
fn t14_readonly_inspect_and_acceptance_keep_business_and_test_separate() {
    let c = Case::new();
    c.control(DebugAction::Step, Some(0));
    let a = c.claim().unwrap();
    c.store
        .finish_task(&a.permit, TaskStatus::Succeeded, Utc::now())
        .unwrap();
    let acceptance = StageAcceptance {
        version: 1,
        run_id: c.run.clone(),
        task_id: a.node.task_id.clone(),
        attempt_id: a.permit.attempt_id.clone(),
        stage: "ExecutionGate".into(),
        business_result: "Rejected".into(),
        test_result: AcceptanceResult::Pass,
        checks: vec![AcceptanceCheck {
            check_id: "expired_approval".into(),
            category: AcceptanceCategory::Authorization,
            expected: "Rejected".into(),
            actual: "Rejected".into(),
            result: AcceptanceResult::Pass,
            evidence_refs: vec![],
            message: "expired approval correctly refused".into(),
        }],
        created_at: Utc::now(),
    };
    c.store.record_stage_acceptance(&acceptance).unwrap();
    c.store.record_stage_acceptance(&acceptance).unwrap();
    let readonly = Store::open_existing(c.store.root()).unwrap();
    let before = c.inspect();
    let inspected = readonly
        .debug_inspect(&c.run, None, None, Utc::now())
        .unwrap();
    assert_eq!(before.events, inspected.events);
    assert_eq!(before.session, inspected.session);
    assert_eq!(
        inspected
            .acceptance
            .iter()
            .filter(|a| a.stage == "ExecutionGate")
            .count(),
        1
    );
    assert_eq!(
        inspected.acceptance.last().unwrap().test_result,
        AcceptanceResult::Pass
    );
}

#[test]
fn t15_debug_bundle_is_read_only_and_raw_access_follows_isolation() {
    let debug = Case::new();
    debug.control(DebugAction::Resume, None);
    let attempt = debug.claim().unwrap();
    let raw = concat!(
        "{\"provider_result\":{\"id\":\"discovery\",\"output\":[{\"type\":\"web_search_call\",\"id\":\"search-1\",\"status\":\"completed\",\"action\":{\"sources\":[{\"url\":\"https://example.com/news\"}]}}]}}\n",
        "{\"audit\":{\"request\":{\"api_key\":\"private-fixture-value\"},\"response\":{\"id\":\"review\",\"output\":[{\"type\":\"web_search_call\",\"id\":\"review-1\",\"action\":{\"sources\":[{\"url\":\"https://example.com/news\"}]}}]}}}\n"
    );
    let now = Utc::now();
    let evidence = Artifact::new(
        ArtifactKind::RawEvidence,
        debug
            .store
            .stage_bytes(raw.as_bytes(), "application/x-ndjson")
            .unwrap(),
        "evidence.raw",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "news_web".into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(attempt.permit.artifact_origin()),
        vec![],
        now,
    )
    .unwrap();
    debug
        .store
        .write_task_artifact(
            &attempt.permit,
            &evidence,
            LifecycleEventType::ArtifactCommitted,
            now,
        )
        .unwrap();
    let target = debug
        .store
        .root()
        .parent()
        .unwrap()
        .join(format!("bundle-{}", debug.run.0));
    let before_cursor = debug.store.event_cursor().unwrap();
    let manifest = debug
        .store
        .export_debug_bundle(&debug.run, &target)
        .unwrap();
    assert_eq!(manifest.run_id, debug.run);
    assert_eq!(manifest.snapshot_cursor, before_cursor);
    assert!(target.join("SUMMARY.md").is_file());
    assert!(target.join("checksums.sha256").is_file());
    assert_eq!(debug.store.event_cursor().unwrap(), before_cursor);
    let exported =
        std::fs::read_to_string(target.join(format!("artifacts/{}.json", evidence.artifact_id)))
            .unwrap();
    assert!(!exported.contains("private-fixture-value"));
    let payload: serde_json::Value = serde_json::from_str(&exported).unwrap();
    assert_eq!(payload["export_encoding"], "ndjson");
    assert_eq!(payload["records"].as_array().unwrap().len(), 2);
    let status: serde_json::Value =
        serde_json::from_slice(&std::fs::read(target.join("evidence_status.json")).unwrap())
            .unwrap();
    assert_eq!(status["web_search_calls"], 2);
    assert_eq!(status["web_search_audit"]["observed_call_count"], 2);
    assert_eq!(manifest.integrity.uncaptured_payloads, 0);

    let paper = Case::with_purpose(RunPurpose::Paper);
    let legacy_target = paper
        .store
        .root()
        .parent()
        .unwrap()
        .join(format!("legacy-raw-{}", paper.run.0));
    let error = paper
        .store
        .export_run(&paper.run, &legacy_target, true)
        .unwrap_err();
    assert!(matches!(
        error,
        StoreError::RawModelExportNotAllowed(RunPurpose::Paper)
    ));
}

fn completed_draft_with_abandoned_submit() -> (Case, i64) {
    let case = Case::new();
    case.control(DebugAction::Resume, None);
    let claimed = case.claim().unwrap();
    let now = Utc::now();
    case.store
        .append_task_event(&claimed.permit, LifecycleEventType::AgentTurnStarted, now)
        .unwrap();
    let turn = Artifact::new(
        ArtifactKind::AgentTurn,
        case.store
            .stage_json(&serde_json::json!({
                "turn": 0,
                "call_id": "draft-call",
                "request": {"phase": "draft"},
                "response": {"telemetry": {
                    "input_tokens": 17,
                    "output_tokens": 5,
                    "latency_millis": 3
                }}
            }))
            .unwrap(),
        "research.agent.turn",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "model".into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: claimed.permit.contract_hash.clone(),
        },
        Some(claimed.permit.artifact_origin()),
        vec![],
        now,
    )
    .unwrap();
    case.store
        .write_task_artifact(
            &claimed.permit,
            &turn,
            LifecycleEventType::AgentTurnCompleted,
            now,
        )
        .unwrap();
    case.store
        .append_task_event(&claimed.permit, LifecycleEventType::AgentTurnStarted, now)
        .unwrap();
    let submit_cursor = case.store.event_cursor().unwrap();
    // Older stores can contain the legacy alias for a completed artifact.
    // It must neither double-charge Draft nor close the later Submit start.
    case.store
        .write_task_artifact(&claimed.permit, &turn, LifecycleEventType::AgentTurn, now)
        .unwrap();
    case.store
        .retry_task(&claimed.permit, now + Duration::seconds(1), now)
        .unwrap();
    (case, submit_cursor)
}

#[test]
fn bundle_does_not_pair_completed_draft_with_abandoned_submit() {
    let (case, submit_cursor) = completed_draft_with_abandoned_submit();
    let before = case.store.event_cursor().unwrap();
    let target = case
        .store
        .root()
        .parent()
        .unwrap()
        .join(format!("calls-{}", case.run));
    let manifest = case.store.export_debug_bundle(&case.run, &target).unwrap();
    assert_eq!(manifest.integrity.unknown_after_crash_calls, 1);
    let calls = std::fs::read_to_string(target.join("llm_calls.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    let unknown = calls
        .iter()
        .find(|call| call["status"] == "unknown_after_crash")
        .unwrap();
    assert_eq!(unknown["event_cursor"], submit_cursor);
    assert!(
        unknown["call_id"].is_null(),
        "no durable call ID was recorded"
    );
    assert!(
        unknown["turn_id"].is_null(),
        "do not infer the missing Submit identity"
    );
    assert_eq!(case.store.event_cursor().unwrap(), before);
}

#[test]
fn usage_counts_abandoned_submit_as_unknown_after_completed_draft() {
    let (case, _) = completed_draft_with_abandoned_submit();
    let before = case.store.event_cursor().unwrap();
    let usage = case.store.run_model_usage(&case.run).unwrap();
    assert_eq!(usage.turns, 2, "the second dispatch is a separate call");
    assert_eq!(usage.turns_missing_usage, 1);
    assert_eq!(
        usage.input_tokens, 17,
        "preserve known provider usage exactly once"
    );
    assert_eq!(usage.output_tokens, 5);
    assert_eq!(case.store.event_cursor().unwrap(), before);
}

/// A broker-session slot is the single exclusive record of one Paper run.
/// `rebuild_session_slots.run_id` carries no foreign key and no uniqueness
/// constraint, so the Store asserts both inside the reserving transaction.
mod session_slot_integrity {
    use super::*;

    fn paper_reservation(
        store: &Store,
        session_key: &str,
        now: chrono::DateTime<Utc>,
    ) -> SessionReservation {
        let run = RunId::new();
        let task = TaskId::new();
        let nodes = vec![WorkflowNode {
            spec: None,
            task_id: task,
            recipe_id: TaskRecipeId::new("test.stage").unwrap(),
            contract_hash: None,
            objective: "stage 0 [research_horizon=t1]".into(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 50,
            budget: TaskBudget {
                max_input_tokens: 1000,
                max_output_tokens: 100,
                max_wall_time_secs: 30,
                max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(2),
            },
            retry: RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
                retry_transport: false,
                retry_rate_limited: false,
                retry_invalid_output: false,
            },
            on_failure: FailureDisposition::FailTask,
            parent_task_id: None,
        }];
        let graph = WorkflowGraph {
            definition_version: None,
            agent_budgets: Default::default(),
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id: "paper-slot-test".into(),
            nodes: nodes.clone(),
        };
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.stage_json(&graph).unwrap(),
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(ArtifactOrigin {
                run_id: Some(run.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
            now,
        )
        .unwrap();
        SessionReservation {
            session_key: session_key.to_owned(),
            workflow: WorkflowCommit {
                run: StoredRun {
                    run_id: run,
                    purpose: RunPurpose::Paper,
                    topology_id: graph.topology_id.clone(),
                    graph_artifact_id: graph_artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: graph_artifact,
                nodes,
            },
            setup_artifacts: vec![],
            reserved_at: now,
        }
    }

    fn store_with_lease(label: &str) -> (Store, DaemonLease, chrono::DateTime<Utc>) {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/session-slot-tests")
            .join(format!("{label}-{}", RunId::new().0));
        let store = Store::open(root).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease("scheduler", "owner", now, now + Duration::minutes(5))
            .unwrap()
            .unwrap();
        (store, lease, now)
    }

    /// One run may never own two slots. `Store::session_slot_for_run` resolves at
    /// most one row, so a second slot would make that lookup pick arbitrarily.
    #[test]
    fn a_run_cannot_own_a_second_session_slot() {
        let (store, lease, now) = store_with_lease("one-slot-per-run");
        let first = paper_reservation(&store, "2026-09-21", now);
        store.reserve_session_slot(&lease, &first).unwrap();

        // Same run, different trading session. `commit_workflow_transaction`
        // rejects the duplicate Run row first, so the slot guard is the second
        // line of defence; either way no second slot may reach the table.
        let mut second = paper_reservation(&store, "2026-09-22", now);
        second.workflow.run.run_id = first.workflow.run.run_id.clone();
        assert!(store.reserve_session_slot(&lease, &second).is_err());

        assert_eq!(
            store
                .session_slot_for_run(&first.workflow.run.run_id)
                .unwrap()
                .unwrap()
                .session_key,
            "2026-09-21"
        );
        store.verify_integrity().unwrap();
    }

    /// Re-reserving the identical session stays idempotent: the original graph
    /// is returned and the caller's replacement is not recorded.
    #[test]
    fn reserving_the_same_session_twice_is_idempotent() {
        let (store, lease, now) = store_with_lease("idempotent-slot");
        let reservation = paper_reservation(&store, "2026-09-21", now);
        let first = store.reserve_session_slot(&lease, &reservation).unwrap();
        assert!(first.newly_reserved);

        let again = store.reserve_session_slot(&lease, &reservation).unwrap();
        assert!(!again.newly_reserved);
        assert_eq!(
            again.slot.workflow.run.run_id,
            first.slot.workflow.run.run_id
        );
        store.verify_integrity().unwrap();
    }

    /// A reserved slot is discoverable by its run, and the Doctor accepts the
    /// run/slot pairing it just wrote.
    #[test]
    fn reserved_slot_resolves_by_run_and_passes_doctor() {
        let (store, lease, now) = store_with_lease("slot-by-run");
        let reservation = paper_reservation(&store, "2026-09-21", now);
        let run_id = reservation.workflow.run.run_id.clone();
        store.reserve_session_slot(&lease, &reservation).unwrap();

        let slot = store.session_slot_for_run(&run_id).unwrap().unwrap();
        assert_eq!(slot.session_key, "2026-09-21");
        store.verify_integrity().unwrap();
    }
}

#[path = "support/runtime_control.rs"]
mod runtime_control;
