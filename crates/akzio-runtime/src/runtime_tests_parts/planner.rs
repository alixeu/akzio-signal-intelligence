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
fn shadow_graph_rebinds_candidate_topology_and_session_inputs() {
    let root = tempdir().unwrap();
    let mut recipes = catalogue();
    recipes
        .recipes
        .get_mut(&TaskRecipeId::new(ANALYST_RECIPE_ID).unwrap())
        .unwrap()
        .max_depth = 2;
    let runtime = WorkflowRuntime::new(V2Store::open(root.path()).unwrap(), recipes);
    let candidate = runtime
        .lower_shadow(
            &runtime
                .approved_paper_proposal(STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID)
                .unwrap(),
            None,
        )
        .unwrap();
    let evidence_inputs = vec![ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(b"evidence-input")),
        kind: ArtifactKind::EvidenceNeed,
    }];
    let rebound = runtime
        .lower_shadow_from_graph(&candidate, &evidence_inputs, None)
        .unwrap();

    assert_eq!(rebound.topology_id, candidate.topology_id);
    assert!(rebound
        .nodes
        .iter()
        .zip(candidate.nodes.iter())
        .all(|(rebound, candidate)| rebound.task_id != candidate.task_id));
    assert!(rebound
        .nodes
        .iter()
        .filter(|node| {
            node.recipe_id.as_str() == ANALYST_RECIPE_ID
                || node.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID
                || node.recipe_id.as_str() == CRITIC_RECIPE_ID
                || node.recipe_id.as_str() == EVIDENCE_GATE_RECIPE_ID
        })
        .all(|node| node.input_artifacts == evidence_inputs));
    assert_eq!(
        rebound
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == ANALYST_RECIPE_ID)
            .unwrap()
            .contract_hash,
        candidate
            .nodes
            .iter()
            .find(|node| node.recipe_id.as_str() == ANALYST_RECIPE_ID)
            .unwrap()
            .contract_hash,
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
