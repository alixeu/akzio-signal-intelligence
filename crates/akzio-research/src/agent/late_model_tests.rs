use super::*;
use akzio_domain::{TaskStatus, WorkflowGraph};
use akzio_store::{StoredRun, TaskWorkload, WorkflowCommit};
use std::sync::atomic::{AtomicUsize, Ordering};

struct ModelAtDeadline {
    calls: AtomicUsize,
    missing_input: bool,
    interrupted: bool,
    entered: tokio::sync::Notify,
    request: std::sync::Mutex<Option<AgentModelRequest>>,
}

impl AgentModel for ModelAtDeadline {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        ModelClient::Fixture(json!({})).capability_snapshot()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            assert_eq!(request.phase, AgentTurnPhase::Draft);
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.request.lock().unwrap() = Some(request);
            if self.interrupted {
                self.entered.notify_one();
                return std::future::pending().await;
            }
            if self.missing_input {
                return Err(ResearchError::ProviderUsageMissing {
                    usage: ModelUsage {
                        input_tokens: None,
                        cached_input_tokens: None,
                        output_tokens: Some(50),
                        reasoning_tokens: Some(10),
                    },
                    trace: None,
                });
            }
            // A final response parse or executor stall need not yield to Tokio.
            // timeout cannot preempt this poll; run_inner must reject the late
            // completed result itself while retaining its reported usage.
            std::thread::sleep(StdDuration::from_millis(2200));
            Ok(AgentModelTurn {
                assistant_text: Some("Late offline memo; must not authorize Submit".into()),
                tool_calls: vec![],
                terminal_submission: None,
                continuation: ModelContinuation::from_items(vec![json!({
                    "role": "assistant", "content": "Late offline memo"
                })]),
                telemetry: Some(AgentTurnTelemetry {
                    provider_request_id: Some("offline-late-call".into()),
                    response_id: Some("offline-late-response".into()),
                    requested_model: Some("fixture".into()),
                    actual_model: Some("fixture".into()),
                    latency_millis: 2200,
                    input_tokens: Some(125),
                    cached_input_tokens: Some(25),
                    output_tokens: Some(50),
                    reasoning_tokens: Some(10),
                }),
                model_debug: None,
            })
        })
    }
}

#[derive(Clone, Copy)]
enum AttemptTransition {
    Recover,
    Retry,
    RetryBeforeProvider,
}

pub(super) fn isolated_outcome_attempt(
    wall_secs: u32,
    claimed_at: DateTime<Utc>,
) -> (Store, AgentRuntime, akzio_store::ClaimedAttempt) {
    isolated_agent_attempt(
        LEARNING_OUTCOME_WORKER_RECIPE_ID,
        RunPurpose::Paper,
        wall_secs,
        claimed_at,
    )
}

pub(super) fn isolated_agent_attempt(
    role: &str,
    purpose: RunPurpose,
    wall_secs: u32,
    claimed_at: DateTime<Utc>,
) -> (Store, AgentRuntime, akzio_store::ClaimedAttempt) {
    isolated_agent_chain(&[role], purpose, wall_secs, claimed_at)
}

pub(super) fn isolated_agent_chain(
    roles: &[&str],
    purpose: RunPurpose,
    wall_secs: u32,
    claimed_at: DateTime<Utc>,
) -> (Store, AgentRuntime, akzio_store::ClaimedAttempt) {
    let role = roles[0];
    let run_id = RunId::new();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.akzio/llm-chain-20260920/tests/late-model")
        .join(&run_id.0);
    let store = Store::open(&root).unwrap();
    let now = claimed_at;
    let catalogue = ActiveResearchCatalogue::install(&store, now)
        .unwrap()
        .contracts;
    let contract = catalogue
        .contracts()
        .find(|installed| installed.contract.purpose.as_str() == role)
        .unwrap()
        .contract
        .clone();
    // A lower frozen budget for this isolated task, never a production route or
    // Contract edit. The production Outcome protocol and Contract stay intact.
    let mut task_budget = contract.budget.clone();
    task_budget.max_wall_time_secs = wall_secs;
    let node = WorkflowNode {
        spec: None,
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new(role).unwrap(),
        contract_hash: Some(contract.contract_hash.clone()),
        objective: "Offline deadline accounting".into(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget: task_budget,
        retry: contract.retry.clone(),
        on_failure: contract.on_failure,
        parent_task_id: None,
    };
    let mut nodes = vec![node.clone()];
    for role in &roles[1..] {
        let contract = &catalogue
            .contracts()
            .find(|c| c.contract.purpose.as_str() == *role)
            .unwrap()
            .contract;
        let mut next = node.clone();
        next.task_id = TaskId::new();
        next.recipe_id = TaskRecipeId::new(*role).unwrap();
        next.contract_hash = Some(contract.contract_hash.clone());
        next.budget = contract.budget.clone();
        next.budget.max_wall_time_secs = wall_secs;
        next.retry = contract.retry.clone();
        next.on_failure = contract.on_failure;
        next.dependencies = vec![nodes.last().unwrap().task_id.clone()];
        nodes.push(next);
    }
    let graph = WorkflowGraph {
        definition_version: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        topology_id: "late-model-test".into(),
        nodes: nodes.clone(),
        agent_budgets: Default::default(),
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
        Some(akzio_domain::ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    store
        .commit_workflow(&WorkflowCommit {
            run: StoredRun {
                run_id: run_id.clone(),
                purpose,
                topology_id: graph.topology_id,
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes,
        })
        .unwrap();
    let mut attempt = store
        .claim_next_task_for_workload(
            "late-model-test",
            now,
            Duration::minutes(5),
            TaskWorkload::Any,
        )
        .unwrap()
        .unwrap();
    // Explicit accounting-only documents satisfy the real Outcome context contract.
    // They are not business validation fixtures and never become a Decision or order.
    for kind in if role == LEARNING_OUTCOME_WORKER_RECIPE_ID {
        vec![
            ArtifactKind::Decision,
            ArtifactKind::DecisionContext,
            ArtifactKind::ExecutionContext,
            ArtifactKind::OutcomeSchedule,
            ArtifactKind::SemanticDetail,
        ]
    } else {
        vec![ArtifactKind::NormalizedEvidence]
    } {
        let artifact = Artifact::new(kind, store.stage_json(&json!({"resource":"quote:QQQ","accounting_fixture_only":true,"type":"outcome_stage_context","claims":[],"critiques":[]})).unwrap(),
            match kind { ArtifactKind::NormalizedEvidence=>"evidence.normalize",ArtifactKind::Decision=>"decision.bound",ArtifactKind::DecisionContext=>"decision.context",ArtifactKind::ExecutionContext=>"execution.context",ArtifactKind::OutcomeSchedule=>"learning.outcome_schedule",_=>"learning.outcome_stage" }, ArtifactLifecycle::RunScoped,
            ArtifactProvenance { source_family:if kind == ArtifactKind::NormalizedEvidence { "alpaca".into() } else if matches!(kind,ArtifactKind::OutcomeSchedule|ArtifactKind::SemanticDetail) {"akzio-learning".into()} else {"akzio.execution".into()}, observed_at:None, retrieved_at:now,source_uri:None,confidence_ppm:0,producer_contract_hash:None },
            Some(attempt.permit.artifact_origin()), vec![], now).unwrap();
        store
            .write_task_artifact(
                &attempt.permit,
                &artifact,
                LifecycleEventType::ArtifactCommitted,
                now,
            )
            .unwrap();
        attempt.node.input_artifacts.push(ArtifactRef {
            artifact_id: artifact.artifact_id,
            kind,
        });
    }
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(10));
    (store, runtime, attempt)
}

async fn failed_draft_checkpoint(
    missing_input: bool,
    interrupted: bool,
    transition: AttemptTransition,
) -> (AgentRecoveryCheckpoint, WorkflowNode) {
    let now = Utc::now();
    let (store, runtime, attempt) = isolated_outcome_attempt(2, now);
    let node = attempt.node.clone();
    let run_id = attempt.run_id.clone();
    if matches!(transition, AttemptTransition::RetryBeforeProvider) {
        store.retry_task(&attempt.permit, now, now).unwrap();
        let replacement = store
            .claim_next_task_for_workload(
                "retry-before-provider",
                now,
                Duration::minutes(5),
                TaskWorkload::Any,
            )
            .unwrap()
            .unwrap();
        let hash = node.contract_hash.clone().unwrap();
        let guard = AgentRecoveryGuard {
            initial_phase: AgentTurnPhase::Draft,
            deliberation_repair_tool_set_hash: None,
            contract_hash: hash.clone(),
            context_manifest: akzio_domain::ContextManifestPayload {
                schema_version: DOMAIN_SCHEMA_VERSION,
                contract_hash: hash.clone(),
                selections: vec![],
                quarantined: vec![],
                total_bytes: 0,
                projected_bytes: Some(0),
                estimated_tokens: 0,
                input_hash: hash.clone(),
            },
            read_grant_identity: hash.clone(),
            context_materialization_identity: hash.clone(),
            capability_snapshot_hash: hash.clone(),
            budget_policy_hash: hash.clone(),
            draft_tool_set_hash: hash.clone(),
            submit_tool_set_hash: hash,
        };
        let checkpoint = agent_recovery_checkpoint(&store, &replacement.permit, &guard).unwrap();
        return (checkpoint, node);
    }
    let model = ModelAtDeadline {
        calls: AtomicUsize::new(0),
        missing_input,
        interrupted,
        entered: tokio::sync::Notify::new(),
        request: std::sync::Mutex::new(None),
    };
    let mut budget = AgentRunBudget::new(&node.budget, &node.retry);
    let result = if interrupted {
        let mut running = Box::pin(runtime.run_with_budget(
            &attempt.permit,
            &node,
            node.input_artifacts.clone(),
            &model,
            now,
            &mut budget,
        ));
        tokio::select! {
            _ = model.entered.notified() => None,
            result = &mut running => Some(result),
        }
        // Drop the in-flight provider future after the durable Started event.
    } else {
        Some(
            runtime
                .run_with_budget(
                    &attempt.permit,
                    &node,
                    node.input_artifacts.clone(),
                    &model,
                    now,
                    &mut budget,
                )
                .await,
        )
    };
    if interrupted {
        assert!(result.is_none());
    } else if missing_input {
        assert!(
            matches!(
                result,
                Some(Err(ResearchError::ProviderUsageMissing { .. }))
            ),
            "{result:?}"
        );
    } else {
        assert!(
            matches!(result, Some(Err(ResearchError::WallTimeExceeded { .. }))),
            "{result:?}"
        );
        assert_eq!(budget.input_tokens, 125);
        assert_eq!(budget.cached_input_tokens, 25);
    }
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    if !interrupted {
        assert_eq!(budget.output_tokens, 50);
        assert_eq!(budget.reasoning_tokens, 10);
    }
    let events = store
        .attempt_events(&run_id, &node.task_id, &attempt.permit.attempt_id)
        .unwrap();
    assert!(!events
        .iter()
        .any(|event| event.lifecycle_kind().unwrap() == LifecycleEventType::AgentTurnCompleted));
    let failed = events
        .iter()
        .filter(|event| event.lifecycle_kind().unwrap() == LifecycleEventType::AgentTurnFailed)
        .collect::<Vec<_>>();
    assert_eq!(failed.len(), usize::from(!interrupted));
    if !interrupted {
        let artifact = store
            .artifact(failed[0].artifact_id.as_ref().unwrap())
            .unwrap();
        let trace: Value =
            serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
        assert_eq!(trace["lifecycle"]["status"], "failed");
        assert_eq!(trace["error_detail"]["usage"]["output_tokens"], 50);
        assert!(
            trace.get("response").is_none(),
            "failed memo must not enter the accepted response slot"
        );
        if !missing_input {
            assert_eq!(
                trace["error_detail"]["response"]["assistant_text"],
                "Late offline memo; must not authorize Submit"
            );
        }
    }
    let request = model.request.lock().unwrap().clone().unwrap();
    let manifest = store.artifact(&request.manifest_artifact_id).unwrap();
    let guard = AgentRecoveryGuard {
        initial_phase: AgentTurnPhase::Draft,
        deliberation_repair_tool_set_hash: None,
        contract_hash: node.contract_hash.clone().unwrap(),
        context_manifest: serde_json::from_slice(&store.read_blob(&manifest.blob).unwrap())
            .unwrap(),
        read_grant_identity: request.read_grant_identity.clone().unwrap(),
        context_materialization_identity: request.context_materialization_identity.clone().unwrap(),
        capability_snapshot_hash: capability_snapshot_hash(&model.capability_snapshot()).unwrap(),
        budget_policy_hash: budget_policy_hash(&model.budget_policy()).unwrap(),
        draft_tool_set_hash: tool_set_hash(&request).unwrap(),
        submit_tool_set_hash: akzio_domain::ContentHash::of_bytes(b"unused-submit-tools"),
    };
    let recovered_at = if matches!(transition, AttemptTransition::Retry) {
        let retry_at = Utc::now();
        store
            .retry_task(&attempt.permit, retry_at, retry_at)
            .unwrap();
        retry_at
    } else {
        let recovered_at = now + Duration::minutes(6);
        assert_eq!(store.recover_expired_tasks(recovered_at).unwrap(), 1);
        recovered_at
    };
    let replacement = store
        .claim_next_task_for_workload(
            "recovered-late-model-test",
            recovered_at,
            Duration::minutes(5),
            TaskWorkload::Any,
        )
        .unwrap()
        .unwrap();
    assert_eq!(replacement.node.task_id, node.task_id);
    assert_eq!(
        store
            .attempt_relation(&replacement.permit.attempt_id)
            .unwrap()
            .unwrap()
            .relation,
        if matches!(transition, AttemptTransition::Retry) {
            akzio_domain::AttemptRelationKind::Retry
        } else {
            akzio_domain::AttemptRelationKind::Recovery
        }
    );
    assert!(replacement.permit.epoch > attempt.permit.epoch);
    assert_eq!(
        store.workflow_snapshot(&run_id).unwrap().tasks[0].status,
        TaskStatus::Running
    );
    // Reload actual persisted events through the real recovery selector from a
    // separate read-only connection, including the Store-created Recovery link.
    let reopened = Store::open_existing(store.root()).unwrap();
    let checkpoint = agent_recovery_checkpoint(&reopened, &replacement.permit, &guard).unwrap();
    let mut incompatible_guard = guard.clone();
    incompatible_guard.draft_tool_set_hash =
        akzio_domain::ContentHash::of_bytes(b"incompatible-current-tools");
    let incompatible =
        agent_recovery_checkpoint(&reopened, &replacement.permit, &incompatible_guard).unwrap();
    assert!(matches!(
        incompatible.source,
        AgentRecoverySource::Recovered(_)
    ));
    assert_eq!(incompatible.provider_calls, 1);
    assert!(
        matches!(
            AgentRunBudget::new(&node.budget, &node.retry).restore(&incompatible),
            Err(ResearchError::ProviderUsageUnknown)
        ),
        "a changed guard must not reset previously consumed provider budget"
    );
    (checkpoint, node)
}

#[tokio::test]
async fn late_completed_turn_is_charged_failed_and_recovered_without_accepting_memo() {
    let (checkpoint, node) =
        failed_draft_checkpoint(false, false, AttemptTransition::Recover).await;
    assert!(
        matches!(checkpoint.source, AgentRecoverySource::Recovered(_)),
        "known late-call usage must not disappear into FreshRestart"
    );
    assert_eq!(checkpoint.phase, AgentTurnPhase::Draft);
    assert!(checkpoint.continuation.is_none());
    assert_eq!(checkpoint.provider_calls, 1);
    let mut restored = AgentRunBudget::new(&node.budget, &node.retry);
    restored.restore(&checkpoint).unwrap();
    assert_eq!(restored.input_tokens, 125);
    assert_eq!(restored.output_tokens, 50);
    assert_eq!(restored.reasoning_tokens, 10);
}

#[tokio::test]
async fn missing_usage_before_first_memo_cannot_fall_back_to_fresh_budget() {
    let (checkpoint, node) = failed_draft_checkpoint(true, false, AttemptTransition::Recover).await;
    assert!(
        matches!(checkpoint.source, AgentRecoverySource::Recovered(_)),
        "unknown provider usage must not disappear into FreshRestart"
    );
    assert_eq!(checkpoint.phase, AgentTurnPhase::Draft);
    assert!(checkpoint.continuation.is_none());
    let mut restored = AgentRunBudget::new(&node.budget, &node.retry);
    assert!(matches!(
        restored.restore(&checkpoint),
        Err(ResearchError::ProviderUsageMissing { .. })
    ));
}

#[tokio::test]
async fn interrupted_provider_start_cannot_resume_with_zero_usage() {
    let (checkpoint, node) = failed_draft_checkpoint(false, true, AttemptTransition::Recover).await;
    assert!(
        matches!(checkpoint.source, AgentRecoverySource::Recovered(_)),
        "unclosed provider call must not disappear into FreshRestart"
    );
    assert_eq!(checkpoint.provider_calls, 1);
    assert_eq!(checkpoint.phase, AgentTurnPhase::Draft);
    assert!(checkpoint.continuation.is_none());
    let mut restored = AgentRunBudget::new(&node.budget, &node.retry);
    assert!(matches!(
        restored.restore(&checkpoint),
        Err(ResearchError::ProviderUsageUnknown)
    ));
}

#[tokio::test]
async fn task_retry_preserves_known_provider_usage() {
    let (checkpoint, node) = failed_draft_checkpoint(false, false, AttemptTransition::Retry).await;
    assert!(matches!(
        checkpoint.source,
        AgentRecoverySource::Recovered(_)
    ));
    assert_eq!(checkpoint.provider_calls, 1);
    let mut restored = AgentRunBudget::new(&node.budget, &node.retry);
    restored.restore(&checkpoint).unwrap();
    assert_eq!(restored.input_tokens, 125);
    assert_eq!(restored.output_tokens, 50);
}

#[tokio::test]
async fn task_retry_cannot_erase_unfinished_provider_usage() {
    let (checkpoint, node) = failed_draft_checkpoint(false, true, AttemptTransition::Retry).await;
    assert!(matches!(
        checkpoint.source,
        AgentRecoverySource::Recovered(_)
    ));
    assert_eq!(checkpoint.provider_calls, 1);
    assert!(matches!(
        AgentRunBudget::new(&node.budget, &node.retry).restore(&checkpoint),
        Err(ResearchError::ProviderUsageUnknown)
    ));
}

#[tokio::test]
async fn task_retry_before_provider_can_start_with_fresh_budget() {
    let (checkpoint, node) =
        failed_draft_checkpoint(false, false, AttemptTransition::RetryBeforeProvider).await;
    assert_eq!(checkpoint.source, AgentRecoverySource::FreshRestart);
    assert_eq!(checkpoint.provider_calls, 0);
    let mut restored = AgentRunBudget::new(&node.budget, &node.retry);
    restored.restore(&checkpoint).unwrap();
    assert_eq!(restored.input_tokens, 0);
    assert_eq!(restored.output_tokens, 0);
}

#[tokio::test]
async fn task_retry_preserves_missing_usage_classification() {
    let (checkpoint, node) = failed_draft_checkpoint(true, false, AttemptTransition::Retry).await;
    let result = AgentRunBudget::new(&node.budget, &node.retry).restore(&checkpoint);
    match result {
        Err(ResearchError::ProviderUsageMissing { usage, .. }) => {
            assert_eq!(usage.input_tokens, None);
            assert_eq!(usage.output_tokens, Some(50));
        }
        other => panic!(
            "missing reported totals must not be described as contradictory totals: {other:?}"
        ),
    }
}

pub(super) fn outcome_accounting_fixture() -> ModelClient {
    let payload = json!({
        "result": {"schema_version":DOMAIN_SCHEMA_VERSION,"outcome_id":"offline-accounting", "horizon":"t1", "summary":"Offline accounting probe", "findings":[], "counterfactuals":[], "lesson_candidates":[], "lesson_proposals":[], "diagnostic_gaps":[], "source_refs":[], "created_at":"2026-09-22T00:00:00Z"},
        "deliberation":{"selected_path":"Offline accounting probe", "alternatives":[], "alternative_match_ppm":[], "uncertainties":[], "uncertainty_weight_ppm":[], "basis_artifact_ids":[], "confidence_ppm":1000000}
    });
    ModelClient::fixture_by_purpose_phase(BTreeMap::from([(
        LEARNING_OUTCOME_WORKER_RECIPE_ID.into(),
        [
            json!({"output":[{"type":"message","content":[{"type":"output_text","text":"fixture outcome memo"}]}]}),
            json!({"output":[{"type":"function_call","call_id":"fixture-submit","name":"submit_result","arguments":serde_json::to_string(&payload).unwrap()}]}),
        ],
    )]))
}
