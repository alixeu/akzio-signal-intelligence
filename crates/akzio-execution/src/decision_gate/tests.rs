use akzio_domain::{
    ArtifactLifecycle, Asset, ContentHash, ContextSelection, DecisionHorizon, FailureDisposition,
    Forecast, LifecycleEventType, RetryPolicy, RunId, TaskBudget, TaskId, TaskRecipeId, WeightPpm,
    WorkflowGraph, WorkflowNode,
};
use akzio_store::v2::{StoredRun, WorkflowCommit};
use chrono::Duration;
use tempfile::tempdir;

use super::*;

#[derive(Clone, Copy)]
enum DraftMode {
    Accepted,
    Blocked,
    ForgedReference,
}

struct GateCase {
    permit: TaskWritePermit,
    proposal: ArtifactRef,
}

fn budget() -> TaskBudget {
    TaskBudget {
        max_input_tokens: 128,
        max_output_tokens: 128,
        max_wall_time_secs: 30,
        max_tool_calls: 1,
    }
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 1,
        initial_backoff_ms: 1,
        retry_transport: false,
        retry_rate_limited: false,
        retry_invalid_output: false,
    }
}

fn node(
    recipe: &str,
    dependencies: Vec<TaskId>,
    contract_hash: Option<ContentHash>,
    priority: u8,
) -> WorkflowNode {
    WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new(recipe).unwrap(),
        contract_hash,
        objective: recipe.to_owned(),
        dependencies,
        input_artifacts: vec![],
        priority,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    }
}

fn provenance(
    source_family: &str,
    contract_hash: Option<ContentHash>,
    now: DateTime<Utc>,
) -> ArtifactProvenance {
    ArtifactProvenance {
        source_family: source_family.to_owned(),
        observed_at: Some(now),
        retrieved_at: now,
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: contract_hash,
    }
}

fn origin(permit: &TaskWritePermit) -> ArtifactOrigin {
    ArtifactOrigin {
        run_id: Some(permit.run_id.clone()),
        task_id: Some(permit.task_id.clone()),
        attempt_id: Some(permit.attempt_id.clone()),
        contract_hash: permit.contract_hash.clone(),
    }
}

fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn decision_policy() -> DecisionPolicy {
    DecisionPolicy {
        min_confidence_ppm: 250_000,
        max_gross_weight: WeightPpm(500_000),
        horizon_weights: std::collections::BTreeMap::from([
            (DecisionHorizon::T1, WeightPpm(333_333)),
            (DecisionHorizon::T3, WeightPpm(333_333)),
            (DecisionHorizon::T5, WeightPpm(333_334)),
        ]),
    }
}

fn forecasts() -> Vec<Forecast> {
    Asset::EXECUTABLE
        .into_iter()
        .flat_map(|asset| {
            [
                DecisionHorizon::T1,
                DecisionHorizon::T3,
                DecisionHorizon::T5,
            ]
            .into_iter()
            .map(move |horizon| Forecast {
                asset,
                horizon,
                positive_return_probability_ppm: if asset == Asset::Tqqq {
                    800_000
                } else {
                    400_000
                },
                expected_return_ppm: if asset == Asset::Tqqq {
                    100_000
                } else {
                    -100_000
                },
            })
        })
        .collect()
}

#[test]
fn rust_owned_policy_derives_target_from_forecasts_without_model_weights() {
    let policy = decision_policy();
    let target = policy.target_for(500_000, &forecasts()).unwrap();

    assert!(target.weights[&Asset::Tqqq].0 > 0);
    assert_eq!(target.weights[&Asset::Qqq], WeightPpm::ZERO);
    assert_eq!(target.weights[&Asset::Soxx], WeightPpm::ZERO);
    assert_eq!(target.weights[&Asset::Soxl], WeightPpm::ZERO);
    assert_ne!(
        policy.policy_hash().unwrap(),
        ContentHash::of_bytes(b"other-policy")
    );
}

fn draft(claim: &ArtifactRef, mode: DraftMode) -> DecisionDraft {
    let claims = match mode {
        DraftMode::ForgedReference => vec![ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(b"forged-claim")),
            kind: ArtifactKind::Claim,
        }],
        _ => vec![claim.clone()],
    };
    DecisionDraft {
        summary: "fixture decision".to_owned(),
        confidence_ppm: 500_000,
        forecasts: forecasts(),
        claims,
        critiques: vec![],
        evidence: vec![],
        applied_learning_refs: vec![],
        rejected_learning_refs: vec![],
        material_conflicts: vec![],
        hard_blockers: matches!(mode, DraftMode::Blocked)
            .then_some(HardBlocker::MissingEvidence)
            .into_iter()
            .collect(),
        soft_warnings: vec![],
    }
}

fn seed_case(
    store: &V2Store,
    purpose: RunPurpose,
    mode: DraftMode,
    manifest_contract_matches: bool,
    include_manifest_ref: bool,
    now: DateTime<Utc>,
) -> GateCase {
    let contract_hash = ContentHash::of_bytes(b"synthesizer-contract");
    let source = node("fixture.source", vec![], None, 100);
    let synthesizer = node(
        "research.synthesizer",
        vec![source.task_id.clone()],
        Some(contract_hash.clone()),
        90,
    );
    let gate = node("gate.decision", vec![synthesizer.task_id.clone()], None, 80);
    let graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: format!("decision-gate-{}", RunId::new()),
        nodes: vec![source, synthesizer, gate],
    };
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&graph).unwrap(),
        "fixture.workflow",
        ArtifactLifecycle::RunScoped,
        provenance("fixture.workflow", None, now),
        None,
        vec![],
        now,
    )
    .unwrap();
    let run = StoredRun {
        run_id: RunId::new(),
        purpose,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: now,
    };
    store
        .commit_workflow(&WorkflowCommit {
            run,
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();

    let source_permit = store
        .claim_next_task("source", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let evidence = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store
            .put_json(&serde_json::json!({"evidence": "fixture"}))
            .unwrap(),
        "fixture.evidence",
        ArtifactLifecycle::RunScoped,
        provenance("fixture.evidence", None, now),
        Some(origin(&source_permit)),
        vec![],
        now,
    )
    .unwrap();
    let claim = Artifact::new(
        ArtifactKind::Claim,
        store
            .put_json(&serde_json::json!({"claim": "fixture"}))
            .unwrap(),
        "fixture.claim",
        ArtifactLifecycle::RunScoped,
        provenance("akzio.agent", None, now),
        Some(origin(&source_permit)),
        vec![reference(&evidence)],
        now,
    )
    .unwrap();
    store
        .commit_attempt(
            &source_permit,
            &[evidence, claim.clone()],
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();

    let synth_permit = store
        .claim_next_task("synthesizer", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let claim_ref = reference(&claim);
    let selection = ContextSelection {
        artifact: claim_ref.clone(),
        reason: "claim".to_owned(),
        estimated_tokens: estimate_tokens(claim.blob.bytes),
    };
    let manifest_contract = if manifest_contract_matches {
        contract_hash.clone()
    } else {
        ContentHash::of_bytes(b"forged-contract")
    };
    let manifest_payload = ContextManifestPayload {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        contract_hash: manifest_contract,
        selections: vec![selection],
        total_bytes: claim.blob.bytes,
        estimated_tokens: estimate_tokens(claim.blob.bytes),
        input_hash: manifest_input_hash(&[ContextSelection {
            artifact: claim_ref.clone(),
            reason: "claim".to_owned(),
            estimated_tokens: estimate_tokens(claim.blob.bytes),
        }])
        .unwrap(),
    };
    let manifest = Artifact::new(
        ArtifactKind::ContextManifest,
        store.put_json(&manifest_payload).unwrap(),
        "context.research.synthesizer",
        ArtifactLifecycle::RunScoped,
        provenance("akzio.context", Some(contract_hash.clone()), now),
        Some(origin(&synth_permit)),
        vec![claim_ref.clone()],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &synth_permit,
            &manifest,
            LifecycleEventType::ContextManifestCreated,
            now,
        )
        .unwrap();

    let proposal = Artifact::new(
        ArtifactKind::DecisionProposal,
        store.put_json(&draft(&claim_ref, mode)).unwrap(),
        "agent.research.synthesizer",
        ArtifactLifecycle::RunScoped,
        provenance("akzio.agent", Some(contract_hash), now),
        Some(origin(&synth_permit)),
        include_manifest_ref
            .then(|| reference(&manifest))
            .into_iter()
            .collect(),
        now,
    )
    .unwrap();
    store
        .commit_attempt(
            &synth_permit,
            std::slice::from_ref(&proposal),
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();
    let permit = store
        .claim_next_task("decision-gate", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    GateCase {
        permit,
        proposal: reference(&proposal),
    }
}

#[test]
fn accepted_paper_proposal_commits_canonical_context_and_decision() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let case = seed_case(
        &store,
        RunPurpose::Paper,
        DraftMode::Accepted,
        true,
        true,
        now,
    );
    let output = V2DecisionRuntime::new(store.clone(), decision_policy())
        .unwrap()
        .decide(&DecisionGateInput {
            permit: case.permit.clone(),
            proposal: case.proposal,
            now,
        })
        .unwrap();

    assert_eq!(
        output.decision_context.lifecycle,
        ArtifactLifecycle::Canonical
    );
    assert_eq!(output.decision.lifecycle, ArtifactLifecycle::Canonical);
    let context: DecisionContext =
        serde_json::from_slice(&store.read_blob(&output.decision_context.blob).unwrap()).unwrap();
    assert!(context.accepted());
    assert_eq!(
        context.decision_policy_hash,
        decision_policy().policy_hash().unwrap()
    );
    assert!(context.target.weights[&Asset::Tqqq].0 > 0);
    let decision: Decision =
        serde_json::from_slice(&store.read_blob(&output.decision.blob).unwrap()).unwrap();
    assert_eq!(
        decision.decision_context,
        reference(&output.decision_context)
    );
    store
        .verify_attempt_terminal(&case.permit, TaskStatus::Succeeded)
        .unwrap();
}

#[test]
fn model_blocker_is_preserved_and_cannot_create_acceptance() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let case = seed_case(
        &store,
        RunPurpose::Paper,
        DraftMode::Blocked,
        true,
        true,
        now,
    );
    let output = V2DecisionRuntime::new(store.clone(), decision_policy())
        .unwrap()
        .decide(&DecisionGateInput {
            permit: case.permit,
            proposal: case.proposal,
            now,
        })
        .unwrap();
    let context: DecisionContext =
        serde_json::from_slice(&store.read_blob(&output.decision_context.blob).unwrap()).unwrap();

    assert!(!context.accepted());
    assert_eq!(context.hard_blockers, vec![HardBlocker::MissingEvidence]);
}

#[test]
fn forged_reference_manifest_contract_and_run_are_rejected() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let runtime = V2DecisionRuntime::new(store.clone(), decision_policy()).unwrap();

    let forged = seed_case(
        &store,
        RunPurpose::Paper,
        DraftMode::ForgedReference,
        true,
        true,
        now,
    );
    assert!(matches!(
        runtime.decide(&DecisionGateInput {
            permit: forged.permit,
            proposal: forged.proposal,
            now,
        }),
        Err(DecisionGateError::ReferenceOutsideManifest(_))
    ));

    let no_manifest = seed_case(
        &store,
        RunPurpose::Paper,
        DraftMode::Accepted,
        true,
        false,
        now,
    );
    assert!(matches!(
        runtime.decide(&DecisionGateInput {
            permit: no_manifest.permit,
            proposal: no_manifest.proposal,
            now,
        }),
        Err(DecisionGateError::InvalidManifestReference)
    ));

    let bad_contract = seed_case(
        &store,
        RunPurpose::Paper,
        DraftMode::Accepted,
        false,
        true,
        now,
    );
    assert!(matches!(
        runtime.decide(&DecisionGateInput {
            permit: bad_contract.permit,
            proposal: bad_contract.proposal,
            now,
        }),
        Err(DecisionGateError::InvalidManifestClosure)
    ));

    let source_run = seed_case(
        &store,
        RunPurpose::Paper,
        DraftMode::Accepted,
        true,
        true,
        now,
    );
    let target_run = seed_case(
        &store,
        RunPurpose::Paper,
        DraftMode::Accepted,
        true,
        true,
        now,
    );
    assert!(matches!(
        runtime.decide(&DecisionGateInput {
            permit: target_run.permit,
            proposal: source_run.proposal,
            now,
        }),
        Err(DecisionGateError::InvalidProposalProvenance)
    ));
}

#[test]
fn nonpaper_decisions_remain_run_scoped() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let case = seed_case(
        &store,
        RunPurpose::Debug,
        DraftMode::Accepted,
        true,
        true,
        now,
    );
    let output = V2DecisionRuntime::new(store, decision_policy())
        .unwrap()
        .decide(&DecisionGateInput {
            permit: case.permit,
            proposal: case.proposal,
            now,
        })
        .unwrap();

    assert_eq!(
        output.decision_context.lifecycle,
        ArtifactLifecycle::RunScoped
    );
    assert_eq!(output.decision.lifecycle, ArtifactLifecycle::RunScoped);
}

#[test]
fn selected_learning_requires_explicit_attribution() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let runtime = V2DecisionRuntime::new(store, decision_policy()).unwrap();
    let claim = ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(b"claim")),
        kind: ArtifactKind::Claim,
    };
    let lesson = ArtifactRef {
        artifact_id: ArtifactId(ContentHash::of_bytes(b"lesson")),
        kind: ArtifactKind::Lesson,
    };
    let selected = BTreeSet::from([claim.clone(), lesson.clone()]);
    let draft_without_attribution = draft(&claim, DraftMode::Accepted);

    assert!(matches!(
        runtime.validate_draft_closure(&draft_without_attribution, &selected),
        Err(DecisionGateError::MissingLearningAttribution(artifact_id))
            if artifact_id == lesson.artifact_id
    ));

    let mut draft_with_rejection = draft_without_attribution;
    draft_with_rejection.rejected_learning_refs.push(lesson);
    runtime
        .validate_draft_closure(&draft_with_rejection, &selected)
        .unwrap();
}
