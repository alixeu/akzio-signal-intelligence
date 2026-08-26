use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    ArtifactOrigin, ClaimStance, ContentHash, ContractPurpose, DecisionHorizon, EvidenceGap,
    EvidenceNeed, FailureDisposition, LifecycleEventType, ResearchClaim, RetryPolicy, TaskBudget,
    WorkflowProposalDraft, WorkflowProposalDraftTask, WorkflowProposalTask,
};
use tempfile::tempdir;

use super::*;

fn budget() -> TaskBudget {
    TaskBudget {
        max_input_tokens: 100,
        max_output_tokens: 50,
        max_wall_time_secs: 30,
        max_tool_calls: 2,
    }
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 2,
        initial_backoff_ms: 0,
        retry_transport: true,
        retry_rate_limited: true,
        retry_invalid_output: false,
    }
}

fn claim(
    stance: ClaimStance,
    materiality_ppm: u32,
    confidence_ppm: u32,
    has_gap: bool,
) -> ResearchClaim {
    ResearchClaim {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topic: "TQQQ regime".to_owned(),
        statement: "fixture claim".to_owned(),
        horizon: DecisionHorizon::T5,
        stance,
        materiality_ppm,
        confidence_ppm,
        grounds: vec![],
        evidence_gaps: has_gap
            .then(|| EvidenceGap {
                topic: "fixture gap".to_owned(),
                rationale: "fixture uncertainty".to_owned(),
            })
            .into_iter()
            .collect(),
    }
}

fn recipe(id: &str, class: RuntimeTaskClass, agent: bool) -> TaskRecipe {
    TaskRecipe {
        recipe_id: TaskRecipeId::new(id).unwrap(),
        purpose: ContractPurpose::new(id).unwrap(),
        contract_hash: agent.then(|| ContentHash::of_bytes(id.as_bytes())),
        task_class: class,
        allowed_evidence_sources: if agent {
            BTreeSet::from(["alpaca".to_owned()])
        } else {
            BTreeSet::new()
        },
        max_children: 8,
        max_depth: 2,
        priority_ceiling: 100,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
    }
}

fn catalogue() -> RecipeCatalogue {
    let mut analyst = recipe("research.analyst", RuntimeTaskClass::Agent, true);
    analyst.max_children = 1;
    analyst.max_depth = 1;
    RecipeCatalogue::new(
        [
            recipe("research.planner", RuntimeTaskClass::Agent, true),
            analyst,
            recipe("research.critic", RuntimeTaskClass::Agent, true),
            recipe("research.synthesizer", RuntimeTaskClass::Agent, true),
            recipe("gate.evidence", RuntimeTaskClass::Evidence, false),
            recipe("gate.decision", RuntimeTaskClass::DecisionGate, false),
            recipe("gate.execution", RuntimeTaskClass::ExecutionGate, false),
            recipe("gate.paper", RuntimeTaskClass::PaperCommit, false),
            recipe("gate.reconcile", RuntimeTaskClass::Reconcile, false),
            recipe("gate.evaluate", RuntimeTaskClass::Evaluate, false),
            recipe("learning.outcome_worker", RuntimeTaskClass::Evaluate, false),
        ],
        TaskRecipeId::new("research.planner").unwrap(),
        TerminalRecipeSet {
            evidence_gate: TaskRecipeId::new("gate.evidence").unwrap(),
            decision_gate: TaskRecipeId::new("gate.decision").unwrap(),
            execution_gate: TaskRecipeId::new("gate.execution").unwrap(),
            paper_commit: TaskRecipeId::new("gate.paper").unwrap(),
            reconcile: TaskRecipeId::new("gate.reconcile").unwrap(),
            evaluate: TaskRecipeId::new("gate.evaluate").unwrap(),
        },
        32,
    )
    .unwrap()
}

fn proposal() -> WorkflowProposal {
    WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: BTreeMap::from([
            (
                "analyst".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    objective: "analyse evidence".to_owned(),
                    depends_on: vec![],
                    priority: 80,
                    evidence_needs: vec![],
                },
            ),
            (
                "critic".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.critic").unwrap(),
                    objective: "challenge claim".to_owned(),
                    depends_on: vec!["analyst".to_owned()],
                    priority: 70,
                    evidence_needs: vec![],
                },
            ),
        ]),
        stop_reason: None,
    }
}

fn planner_output_artifact(
    store: &V2Store,
    planner: &ClaimedAttempt,
    now: DateTime<Utc>,
) -> Artifact {
    let draft = WorkflowProposalDraft {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalDraftTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyse evidence".to_owned(),
                depends_on: vec![],
                priority: 80,
                evidence_needs: vec![EvidenceNeed {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    source_family: "alpaca".to_owned(),
                    resource: "bars:TQQQ:1d".to_owned(),
                    max_age_secs: 86_400,
                }],
                research_intents: vec![],
            },
        )]),
        stop_reason: None,
    };
    Artifact::new(
        ArtifactKind::WorkflowProposalDraft,
        store.put_json(&draft).unwrap(),
        "agent.planner",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.agent".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: planner.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(planner.run_id.clone()),
            task_id: Some(planner.node.task_id.clone()),
            attempt_id: Some(planner.permit.attempt_id.clone()),
            contract_hash: planner.permit.contract_hash.clone(),
        }),
        vec![],
        now,
    )
    .unwrap()
}

fn task_artifact(store: &V2Store, task: &ClaimedAttempt, now: DateTime<Utc>) -> Artifact {
    let blob = store
        .put_bytes(b"task output", "text/plain; charset=utf-8")
        .unwrap();
    Artifact::new(
        ArtifactKind::AgentTurn,
        blob,
        "runtime.fixture",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: task.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(task.run_id.clone()),
            task_id: Some(task.node.task_id.clone()),
            attempt_id: Some(task.permit.attempt_id.clone()),
            contract_hash: task.permit.contract_hash.clone(),
        }),
        vec![],
        now,
    )
    .unwrap()
}

#[test]
fn planner_graph_gets_non_bypassable_terminal_gates() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let graph = runtime.bootstrap(RunPurpose::Debug, "active").unwrap();
    assert_eq!(graph.nodes.len(), 6);
    assert!(graph
        .nodes
        .iter()
        .any(|node| node.recipe_id.as_str() == "gate.evidence"));
    assert!(graph
        .nodes
        .iter()
        .any(|node| node.recipe_id.as_str() == "gate.decision"));
    assert!(!graph
        .nodes
        .iter()
        .any(|node| node.recipe_id.as_str() == "gate.paper"));
    graph.validate().unwrap();
}

#[test]
fn structured_critique_requires_material_uncertainty_or_opposed_stances() {
    let clean = claim(ClaimStance::Neutral, 499_999, 500_001, false);
    assert!(!should_run_structured_critique(&[clean]));

    let gap = claim(ClaimStance::Neutral, 500_000, 900_000, true);
    assert!(should_run_structured_critique(&[gap]));

    let low_confidence = claim(ClaimStance::Neutral, 500_000, 500_000, false);
    assert!(should_run_structured_critique(&[low_confidence]));

    let bullish = claim(ClaimStance::Bullish, 1, 1_000_000, false);
    let bearish = claim(ClaimStance::Bearish, 1, 1_000_000, false);
    assert!(should_run_structured_critique(&[bullish, bearish]));
}

#[test]
fn approved_candidate_topology_adds_one_structured_critic() {
    let root = tempdir().unwrap();
    let mut recipes = catalogue();
    recipes
        .recipes
        .get_mut(&TaskRecipeId::new(ANALYST_RECIPE_ID).unwrap())
        .unwrap()
        .max_depth = 2;
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), recipes);

    let proposal = runtime
        .approved_paper_proposal(STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID)
        .unwrap();

    assert_eq!(proposal.tasks.len(), 3);
    assert_eq!(
        proposal.tasks["structured_critic"].depends_on,
        vec!["analyst".to_owned()]
    );
    assert_eq!(
        proposal.tasks["synthesizer"].depends_on,
        vec!["structured_critic".to_owned()]
    );
}

#[test]
fn planner_cannot_schedule_critic_directly() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let mut draft = WorkflowProposalDraft {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID.to_owned(),
        tasks: BTreeMap::from([
            (
                "analyst".to_owned(),
                WorkflowProposalDraftTask {
                    recipe_id: TaskRecipeId::new(ANALYST_RECIPE_ID).unwrap(),
                    objective: "analyse evidence".to_owned(),
                    depends_on: vec![],
                    priority: 80,
                    evidence_needs: vec![],
                    research_intents: vec![],
                },
            ),
            (
                "critic".to_owned(),
                WorkflowProposalDraftTask {
                    recipe_id: TaskRecipeId::new(CRITIC_RECIPE_ID).unwrap(),
                    objective: "challenge claim".to_owned(),
                    depends_on: vec!["analyst".to_owned()],
                    priority: 70,
                    evidence_needs: vec![],
                    research_intents: vec![],
                },
            ),
        ]),
        stop_reason: None,
    };
    assert!(matches!(
        runtime.insert_structured_critic(&mut draft),
        Err(RuntimeError::PlannerSchedulesCritic)
    ));
}

#[test]
fn structured_critique_is_reserved_for_the_candidate_topology() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let analyst = WorkflowProposalDraftTask {
        recipe_id: TaskRecipeId::new(ANALYST_RECIPE_ID).unwrap(),
        objective: "analyse evidence".to_owned(),
        depends_on: vec![],
        priority: 80,
        evidence_needs: vec![],
        research_intents: vec![],
    };
    let mut active = WorkflowProposalDraft {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: BTreeMap::from([("analyst".to_owned(), analyst.clone())]),
        stop_reason: None,
    };
    runtime.insert_structured_critic(&mut active).unwrap();
    assert_eq!(active.tasks.len(), 1);

    let mut candidate = WorkflowProposalDraft {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID.to_owned(),
        tasks: BTreeMap::from([("analyst".to_owned(), analyst)]),
        stop_reason: None,
    };
    runtime.insert_structured_critic(&mut candidate).unwrap();
    let critic = candidate
        .tasks
        .values()
        .find(|task| task.recipe_id.as_str() == CRITIC_RECIPE_ID)
        .unwrap();
    assert_eq!(critic.depends_on, vec!["analyst"]);
}

#[test]
fn planner_cannot_schedule_a_terminal_gate() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let mut proposal = proposal();
    proposal.tasks.insert(
        "escape".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("gate.execution").unwrap(),
            objective: "bypass".to_owned(),
            depends_on: vec![],
            priority: 100,
            evidence_needs: vec![],
        },
    );
    assert!(matches!(
        runtime.lower(RunPurpose::Debug, &proposal),
        Err(RuntimeError::TerminalRecipeInProposal(_))
    ));
}

#[test]
fn proposal_lowering_enforces_recipe_fanout_and_depth_limits() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());

    let mut fanout = proposal();
    fanout.tasks.insert(
        "parallel".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.critic").unwrap(),
            objective: "parallel review".to_owned(),
            depends_on: vec!["analyst".to_owned()],
            priority: 60,
            evidence_needs: vec![],
        },
    );
    assert!(matches!(
        runtime.lower(RunPurpose::Debug, &fanout),
        Err(RuntimeError::WorkflowFanoutLimit { .. })
    ));

    let mut depth = proposal();
    depth.tasks.insert(
        "grandchild".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.critic").unwrap(),
            objective: "deeper review".to_owned(),
            depends_on: vec!["critic".to_owned()],
            priority: 60,
            evidence_needs: vec![],
        },
    );
    assert!(matches!(
        runtime.lower(RunPurpose::Debug, &depth),
        Err(RuntimeError::WorkflowDepthLimit { .. })
    ));
}

#[test]
fn proposal_rejects_cycles_unknown_recipes_and_priority_escalation() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());

    let mut cyclic = proposal();
    cyclic.tasks.get_mut("analyst").unwrap().depends_on = vec!["critic".to_owned()];
    assert!(matches!(
        runtime.lower(RunPurpose::Debug, &cyclic),
        Err(RuntimeError::Domain(DomainError::CyclicPlan))
    ));

    let mut unknown = proposal();
    unknown.tasks.get_mut("analyst").unwrap().recipe_id =
        TaskRecipeId::new("research.uninstalled").unwrap();
    assert!(matches!(
        runtime.lower(RunPurpose::Debug, &unknown),
        Err(RuntimeError::Domain(DomainError::EmptyField {
            field: "workflow_proposal.recipe"
        }))
    ));

    let mut escalated = proposal();
    escalated.tasks.get_mut("analyst").unwrap().priority = 101;
    assert!(matches!(
        runtime.lower(RunPurpose::Debug, &escalated),
        Err(RuntimeError::Domain(DomainError::InvalidBudget { .. }))
    ));
}

#[test]
fn evidence_gate_aggregates_unique_evidence_needs_and_rejects_other_kinds() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let need = ArtifactRef {
        artifact_id: akzio_domain::ArtifactId(ContentHash::of_bytes(b"evidence-need")),
        kind: ArtifactKind::EvidenceNeed,
    };
    let mut proposed = proposal();
    proposed.tasks.get_mut("analyst").unwrap().evidence_needs = vec![need.clone()];
    proposed.tasks.get_mut("critic").unwrap().evidence_needs = vec![need.clone()];

    let graph = runtime.lower(RunPurpose::Debug, &proposed).unwrap();
    let evidence_gate = graph
        .nodes
        .iter()
        .find(|node| node.recipe_id.as_str() == "gate.evidence")
        .unwrap();
    assert_eq!(evidence_gate.input_artifacts, vec![need]);
    let critic = graph
        .nodes
        .iter()
        .find(|node| node.recipe_id.as_str() == "research.critic")
        .unwrap();
    assert!(critic.dependencies.contains(&evidence_gate.task_id));
    assert!(critic.parent_task_id.is_none());

    proposed.tasks.get_mut("analyst").unwrap().evidence_needs[0].kind = ArtifactKind::Claim;
    assert!(matches!(
        runtime.lower(RunPurpose::Debug, &proposed),
        Err(RuntimeError::Domain(DomainError::EmptyField {
            field: "workflow_proposal.evidence_needs"
        }))
    ));
}

#[test]
fn independent_research_nodes_are_claimable_in_parallel() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let runtime = WorkflowRuntime::new(store.clone(), catalogue());
    let mut parallel = proposal();
    parallel.tasks.insert(
        "parallel".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.critic").unwrap(),
            objective: "independent review".to_owned(),
            depends_on: vec![],
            priority: 60,
            evidence_needs: vec![],
        },
    );
    let graph = runtime.lower(RunPurpose::Debug, &parallel).unwrap();
    let run_id = RunId::new();
    runtime
        .submit(run_id, RunPurpose::Debug, graph, Utc::now())
        .unwrap();

    let evidence = store
        .claim_next_task("evidence-worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(evidence.node.recipe_id.as_str(), "gate.evidence");
    let evidence_output = task_artifact(&store, &evidence, Utc::now());
    store
        .commit_attempt(
            &evidence.permit,
            &[evidence_output],
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();

    let first = store
        .claim_next_task("worker-a", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let second = store
        .claim_next_task("worker-b", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_ne!(first.node.task_id, second.node.task_id);
    assert_eq!(first.node.objective, "analyse evidence");
    assert_eq!(second.node.objective, "independent review");
}

#[test]
fn dynamic_patch_extends_research_without_replacing_terminal_chain() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let runtime = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = runtime.bootstrap(RunPurpose::Debug, "active").unwrap();
    let run_id = RunId::new();
    let first = runtime
        .submit(run_id.clone(), RunPurpose::Debug, graph.clone(), Utc::now())
        .unwrap();
    let planner = store
        .claim_next_task("planner-worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(planner.node.recipe_id.as_str(), "research.planner");
    let planner_output = planner_output_artifact(&store, &planner, Utc::now());
    let second = runtime
        .apply_planner_output(&planner, &first, &graph, &planner_output, Utc::now())
        .unwrap();
    let patched: WorkflowGraph =
        serde_json::from_slice(&store.read_blob(&second.blob).unwrap()).unwrap();
    assert_eq!(
        store.artifact(&planner_output.artifact_id).unwrap(),
        planner_output
    );
    assert!(second.source_refs.contains(&ArtifactRef {
        artifact_id: first.artifact_id.clone(),
        kind: ArtifactKind::WorkflowGraph,
    }));
    let proposal_ref = second
        .source_refs
        .iter()
        .find(|reference| reference.kind == ArtifactKind::WorkflowProposal)
        .unwrap();
    let stored_proposal = store.artifact(&proposal_ref.artifact_id).unwrap();
    assert!(stored_proposal.source_refs.iter().any(|reference| {
        reference.artifact_id == planner_output.artifact_id
            && reference.kind == ArtifactKind::WorkflowProposalDraft
    }));
    assert!(matches!(
        store.validate_task_permit(&planner.permit),
        Err(StoreError::StalePermit(_))
    ));
    let recovered = runtime.recover(&run_id).unwrap();
    assert_eq!(recovered.revision.revision, 1);
    assert_eq!(recovered.revision.graph, patched);
    assert_eq!(runtime.replay_run(&run_id).unwrap(), recovered);
    assert_eq!(runtime.replay_revision(&run_id, 0).unwrap().graph, graph);
    assert_eq!(
        recovered
            .tasks
            .iter()
            .map(|task| task.node.task_id.clone())
            .collect::<BTreeSet<_>>(),
        patched
            .nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<BTreeSet<_>>()
    );
    let evidence = store
        .claim_next_task("evidence-worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(evidence.node.recipe_id.as_str(), "gate.evidence");
    assert!(store
        .events_after(&run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| {
            event.event_type == "workflow.patched"
                && event.artifact_id.as_ref() == Some(&second.artifact_id)
        }));
    for recipe in [
        "gate.evidence",
        "gate.decision",
        "gate.execution",
        "gate.reconcile",
        "gate.evaluate",
    ] {
        let before = graph
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == recipe)
            .unwrap();
        let after = patched
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == recipe)
            .unwrap();
        assert_eq!(before.task_id, after.task_id);
    }
    let decision = patched
        .nodes
        .iter()
        .find(|node| node.recipe_id.as_str() == "gate.decision")
        .unwrap();
    assert_eq!(decision.dependencies.len(), 1);
    assert!(matches!(
        runtime.bootstrap(RunPurpose::Paper, "active"),
        Err(RuntimeError::PaperWorkflowRequiresPrecompiledProposal)
    ));
}

#[test]
fn replay_rejects_unknown_durable_event_types() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let run_id = RunId::new();
    let event = StoredEvent {
        cursor: 1,
        run_id: run_id.clone(),
        task_id: None,
        attempt_id: None,
        event_type: "unknown.replay.event".to_owned(),
        artifact_id: None,
        created_at: Utc::now(),
    };

    assert!(matches!(
        runtime.reduce_event(&run_id, &mut ReplayedWorkflow::default(), &event),
        Err(RuntimeError::ReplayDiverged { .. })
    ));
}

#[test]
fn replay_accepts_task_artifact_trace_events_with_matching_origin() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let runtime = WorkflowRuntime::new(store.clone(), catalogue());
    let run_id = RunId::new();
    let now = Utc::now();
    runtime
        .submit(
            run_id.clone(),
            RunPurpose::Debug,
            runtime.bootstrap(RunPurpose::Debug, "active").unwrap(),
            now,
        )
        .unwrap();
    let claimed = store
        .claim_next_task("trace-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    store
        .append_task_event(&claimed.permit, LifecycleEventType::AgentTurnStarted, now)
        .unwrap();
    let artifact = task_artifact(&store, &claimed, now);
    store
        .write_task_artifact(
            &claimed.permit,
            &artifact,
            LifecycleEventType::AgentTurnCompleted,
            now,
        )
        .unwrap();

    assert_eq!(
        runtime.replay_run(&run_id).unwrap(),
        store.workflow_snapshot(&run_id).unwrap()
    );
}

#[test]
fn replay_rejects_snapshot_task_divergence() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let runtime = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    runtime
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let replay = runtime.reduce_history(&run_id).unwrap();
    let mut forged = store.workflow_snapshot(&run_id).unwrap();
    forged.tasks[0].status = TaskStatus::Succeeded;

    assert!(matches!(
        runtime.validate_replay_snapshot(&run_id, &replay, &forged),
        Err(RuntimeError::ReplayDiverged { .. })
    ));
}

#[test]
fn planner_proposal_cannot_be_replayed_after_atomic_commit() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let workflow = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = workflow.bootstrap(RunPurpose::Debug, "active").unwrap();
    let run_id = RunId::new();
    let first = workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph.clone(), Utc::now())
        .unwrap();
    let planner = store
        .claim_next_task("planner-worker", Utc::now(), Duration::seconds(30))
        .unwrap()
        .unwrap();
    let planner_output = planner_output_artifact(&store, &planner, Utc::now());
    let second = workflow
        .apply_planner_output(&planner, &first, &graph, &planner_output, Utc::now())
        .unwrap();
    let patched: WorkflowGraph =
        serde_json::from_slice(&store.read_blob(&second.blob).unwrap()).unwrap();
    let events_before = store.events_after(&run_id, 0, 100).unwrap();

    assert!(matches!(
        workflow.apply_planner_output(&planner, &second, &patched, &planner_output, Utc::now(),),
        Err(RuntimeError::Store(StoreError::StalePermit(_)))
    ));
    assert_eq!(store.events_after(&run_id, 0, 100).unwrap(), events_before);
}

#[tokio::test]
async fn task_runtime_accepts_only_store_verified_committed_attempts() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let workflow = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = workflow.bootstrap(RunPurpose::Debug, "active").unwrap();
    let run_id = RunId::new();
    let first = workflow
        .submit(run_id, RunPurpose::Debug, graph.clone(), Utc::now())
        .unwrap();
    let tasks = TaskRuntime::new(store.clone());
    let handler_store = store.clone();
    let handler_workflow = workflow.clone();
    assert!(tasks
        .run_one("planner-worker", move |planner| {
            let planner_output = planner_output_artifact(&handler_store, &planner, Utc::now());
            handler_workflow
                .apply_planner_output(&planner, &first, &graph, &planner_output, Utc::now())
                .unwrap();
            async { TaskCompletion::Committed }
        })
        .await
        .unwrap());

    assert!(matches!(
        tasks
            .run_one("untrusted-worker", |_| async { TaskCompletion::Committed })
            .await,
        Err(RuntimeError::Store(StoreError::StalePermit(_)))
    ));
}

#[tokio::test]
async fn task_runtime_retries_then_commits_outputs_with_a_new_attempt() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let workflow = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let tasks = TaskRuntime::new(store.clone())
        .with_lease_duration(Duration::seconds(3))
        .unwrap();
    assert!(tasks
        .run_one("worker", |_| async {
            TaskCompletion::Retry(RetryCause::Transport)
        })
        .await
        .unwrap());
    assert!(tasks
        .run_one("worker", move |task| {
            let artifact = task_artifact(&store, &task, Utc::now());
            async move { TaskCompletion::Succeeded(vec![artifact]) }
        })
        .await
        .unwrap());
    let events = tasks.store().events_after(&run_id, 0, 100).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "task.retry_scheduled")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "task.started")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == "task.succeeded")
            .count(),
        1
    );
    let first_page = tasks.store().events_after(&run_id, 0, 2).unwrap();
    let cursor = first_page.last().unwrap().cursor;
    let mut replay = first_page;
    replay.extend(tasks.store().events_after(&run_id, cursor, 100).unwrap());
    assert_eq!(replay, events);
    assert_eq!(
        workflow.replay_run(&run_id).unwrap(),
        tasks.store().workflow_snapshot(&run_id).unwrap()
    );
}

#[tokio::test]
async fn task_runtime_replays_exhausted_retry_as_terminal_failure() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut recipes = catalogue();
    recipes
        .recipes
        .values_mut()
        .for_each(|recipe| recipe.retry.max_attempts = 1);
    let workflow = WorkflowRuntime::new(store.clone(), recipes);
    let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let tasks = TaskRuntime::new(store);

    assert!(tasks
        .run_one("worker", |_| async {
            TaskCompletion::Retry(RetryCause::Transport)
        })
        .await
        .unwrap());

    let events = tasks.store().events_after(&run_id, 0, 100).unwrap();
    let exhausted = events
        .iter()
        .find(|event| event.event_type == "task.retry_exhausted")
        .unwrap();
    let task_id = exhausted.task_id.as_ref().unwrap();
    let attempt_id = exhausted.attempt_id.as_ref().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.task_id.as_ref() == Some(task_id))
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["task.started", "task.retry_exhausted", "task.failed"]
    );
    assert_eq!(
        events
            .iter()
            .find(|event| {
                event.task_id.as_ref() == Some(task_id) && event.event_type == "task.failed"
            })
            .unwrap()
            .attempt_id
            .as_ref(),
        Some(attempt_id)
    );
    let snapshot = tasks.store().workflow_snapshot(&run_id).unwrap();
    let failed = snapshot
        .tasks
        .iter()
        .find(|task| &task.node.task_id == task_id)
        .unwrap();
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.attempt_count, 1);
    assert_eq!(workflow.replay_run(&run_id).unwrap(), snapshot);
}

#[tokio::test]
async fn task_runtime_recovers_expired_attempt_and_honors_cancel_requests() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let workflow = WorkflowRuntime::new(store.clone(), catalogue());
    let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let abandoned = store
        .claim_next_task("crashed-worker", Utc::now(), Duration::milliseconds(-1))
        .unwrap()
        .unwrap();
    let abandoned_task_id = abandoned.node.task_id.clone();
    let before_recovery = workflow.recover(&run_id).unwrap();
    assert_eq!(
        before_recovery
            .tasks
            .iter()
            .find(|task| task.node.task_id == abandoned_task_id)
            .unwrap()
            .active_attempt
            .as_ref()
            .unwrap()
            .permit,
        abandoned.permit
    );
    let tasks = TaskRuntime::new(store.clone())
        .with_lease_duration(Duration::seconds(3))
        .unwrap();
    let old_permit = abandoned.permit.clone();
    let old_attempt_id = old_permit.attempt_id.clone();
    let old_epoch = old_permit.epoch;
    assert!(tasks
        .run_one("recovery-worker", move |task| {
            assert_ne!(task.permit.attempt_id, old_attempt_id);
            assert!(task.permit.epoch > old_epoch);
            let artifact = task_artifact(&store, &task, Utc::now());
            async move { TaskCompletion::Succeeded(vec![artifact]) }
        })
        .await
        .unwrap());
    let after_recovery = workflow.recover(&run_id).unwrap();
    assert_eq!(after_recovery.revision, before_recovery.revision);
    assert_eq!(
        after_recovery
            .tasks
            .iter()
            .map(|task| task.node.task_id.clone())
            .collect::<BTreeSet<_>>(),
        before_recovery
            .tasks
            .iter()
            .map(|task| task.node.task_id.clone())
            .collect::<BTreeSet<_>>()
    );
    let recovered_task = after_recovery
        .tasks
        .iter()
        .find(|task| task.node.task_id == abandoned_task_id)
        .unwrap();
    assert_eq!(recovered_task.status, TaskStatus::Succeeded);
    assert_eq!(recovered_task.attempt_count, 2);
    assert!(recovered_task.active_attempt.is_none());
    assert_eq!(workflow.replay_run(&run_id).unwrap(), after_recovery);
    assert!(matches!(
        tasks
            .store()
            .finish_task(&old_permit, TaskStatus::Skipped, Utc::now()),
        Err(StoreError::StalePermit(_))
    ));
    assert!(tasks
        .store()
        .request_run_cancel(&run_id, "operator", Utc::now())
        .unwrap());
    assert!(!tasks
        .store()
        .request_run_cancel(&run_id, "operator", Utc::now())
        .unwrap());
    assert!(!tasks
        .run_one("cancelled-worker", |_| async {
            panic!("cancelled run must not dispatch")
        })
        .await
        .unwrap());
    let events = tasks.store().events_after(&run_id, 0, 100).unwrap();
    assert!(events
        .iter()
        .any(|event| event.event_type == "task.recovered"));
    assert!(events
        .iter()
        .any(|event| event.event_type == "run.cancel_requested"));
    assert_eq!(
        workflow.replay_run(&run_id).unwrap(),
        tasks.store().workflow_snapshot(&run_id).unwrap()
    );
}

#[test]
fn task_runtime_replays_exhausted_recovery_as_terminal_failure() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut recipes = catalogue();
    recipes
        .recipes
        .values_mut()
        .for_each(|recipe| recipe.retry.max_attempts = 1);
    let workflow = WorkflowRuntime::new(store.clone(), recipes);
    let graph = workflow.lower(RunPurpose::Debug, &proposal()).unwrap();
    let run_id = RunId::new();
    workflow
        .submit(run_id.clone(), RunPurpose::Debug, graph, Utc::now())
        .unwrap();
    let abandoned = store
        .claim_next_task("crashed-worker", Utc::now(), Duration::milliseconds(-1))
        .unwrap()
        .unwrap();
    store.recover_expired_tasks(Utc::now()).unwrap();

    let events = store.events_after(&run_id, 0, 100).unwrap();
    let task_events = events
        .iter()
        .filter(|event| event.task_id.as_ref() == Some(&abandoned.node.task_id))
        .collect::<Vec<_>>();
    assert_eq!(
        task_events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["task.started", "task.recovery_exhausted", "task.failed"]
    );
    assert_eq!(
        task_events[1].attempt_id.as_ref(),
        Some(&abandoned.permit.attempt_id)
    );
    assert_eq!(
        task_events[2].attempt_id.as_ref(),
        Some(&abandoned.permit.attempt_id)
    );
    let snapshot = store.workflow_snapshot(&run_id).unwrap();
    let failed = snapshot
        .tasks
        .iter()
        .find(|task| task.node.task_id == abandoned.node.task_id)
        .unwrap();
    assert_eq!(failed.status, TaskStatus::Failed);
    assert_eq!(failed.attempt_count, 1);
    assert_eq!(workflow.replay_run(&run_id).unwrap(), snapshot);
}

#[test]
fn submit_rejects_graphs_that_bypass_or_mutate_rust_terminal_gates() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let mut missing_gate = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
    missing_gate
        .nodes
        .retain(|node| node.recipe_id.as_str() != "gate.evaluate");
    missing_gate.validate().unwrap();
    assert!(matches!(
        runtime.submit(RunId::new(), RunPurpose::Debug, missing_gate, Utc::now()),
        Err(RuntimeError::MissingTerminalGate(_))
    ));

    let mut altered_gate = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
    altered_gate
        .nodes
        .iter_mut()
        .find(|node| node.recipe_id.as_str() == "gate.execution")
        .unwrap()
        .dependencies
        .clear();
    altered_gate.validate().unwrap();
    assert!(matches!(
        runtime.submit(RunId::new(), RunPurpose::Debug, altered_gate, Utc::now()),
        Err(RuntimeError::InvalidTerminalDependencies(_))
    ));
}

#[test]
fn submit_rejects_nodes_that_diverge_from_the_installed_recipe() {
    let root = tempdir().unwrap();
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), catalogue());
    let mut graph = runtime.lower(RunPurpose::Debug, &proposal()).unwrap();
    graph
        .nodes
        .iter_mut()
        .find(|node| node.recipe_id.as_str() == "research.analyst")
        .unwrap()
        .budget
        .max_output_tokens = 49;
    graph.validate().unwrap();
    assert!(matches!(
        runtime.submit(RunId::new(), RunPurpose::Debug, graph, Utc::now()),
        Err(RuntimeError::NodeRecipeMismatch(_))
    ));
}
