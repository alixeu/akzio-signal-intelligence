//! Offline golden checks at the AgentRuntime model-dispatch boundary.
use super::*;
use std::sync::Mutex;

struct RecordingModel {
    fixture: ModelClientAdapter,
    requests: Mutex<Vec<AgentModelRequest>>,
}

#[tokio::test]
async fn changed_execution_scope_is_rejected_before_model_dispatch() {
    let now = Utc::now();
    let (_store, runtime, attempt) = super::late_model_tests::isolated_agent_attempt(
        RESEARCH_ANALYST_RECIPE_ID,
        RunPurpose::PositionPlan,
        30,
        now,
    );
    let mut changed = attempt.node.clone();
    changed.spec = Some(akzio_domain::NodeSpec {
        key: "analyst_t5".into(),
        horizon: Some(akzio_domain::DecisionHorizon::T5),
        research_round: Some(0),
        proposal_revision: None,
    });
    let model = RecordingModel {
        fixture: ModelClientAdapter::new(crate::fixture_model_client()),
        requests: Mutex::new(Vec::new()),
    };
    let result = runtime
        .run(
            &attempt.permit,
            &changed,
            changed.input_artifacts.clone(),
            &model,
            now,
        )
        .await;
    assert!(matches!(result, Err(ResearchError::NodePolicyMismatch)));
    assert!(model.requests.lock().unwrap().is_empty());
}

impl AgentModel for RecordingModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        self.fixture.capability_snapshot()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request.clone());
            self.fixture.turn(request).await
        })
    }
}

#[tokio::test]
async fn dispatched_draft_and_submit_preserve_prompt_context_tools_and_schema() {
    let now = Utc::now();
    let (_store, runtime, attempt) = super::late_model_tests::isolated_outcome_attempt(30, now);
    let model = RecordingModel {
        fixture: ModelClientAdapter::new(ModelClient::fixture_by_purpose_phase(BTreeMap::from([
            (
                LEARNING_OUTCOME_WORKER_RECIPE_ID.into(),
                [
                    json!({"output":[{"type":"message","content":[{"type":"output_text","text":"fixture outcome memo"}]}]}),
                    json!({"output":[]}),
                ],
            ),
        ]))),
        requests: Mutex::new(Vec::new()),
    };
    runtime
        .run(
            &attempt.permit,
            &attempt.node,
            attempt.node.input_artifacts.clone(),
            &model,
            now,
        )
        .await
        .unwrap_err();
    let requests = model.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].phase, AgentTurnPhase::Draft);
    assert_eq!(requests[1].phase, AgentTurnPhase::Submit);
    assert!(requests[0].terminal.is_none());
    assert!(requests[1].tools.is_empty());
    assert!(requests[1].terminal.is_some());
    assert!(requests[0].continuation.is_none());
    assert!(requests[1].continuation.is_some());
    for request in requests.iter() {
        let contract = runtime
            .contract(attempt.node.contract_hash.as_ref().unwrap())
            .unwrap();
        let role = String::from_utf8(
            runtime
                .store
                .read_blob(&contract.contract.prompt.role)
                .unwrap(),
        )
        .unwrap();
        assert!(request
            .prompt
            .starts_with(&format!("{SHARED_GOVERNANCE_PROMPT}\n\n{role}\n\n")));
        assert!(request.prompt.contains("Draft 和 Submit 共享该预算"));
        assert!(!request.prompt.contains("单次结构化研究协议"));
    }
}

#[tokio::test]
async fn paper_position_plan_and_shadow_use_the_same_submit_only_research_protocol() {
    for purpose in [
        RunPurpose::Paper,
        RunPurpose::PositionPlan,
        RunPurpose::Shadow,
    ] {
        let now = Utc::now();
        let (_store, runtime, attempt) = super::late_model_tests::isolated_agent_attempt(
            RESEARCH_ANALYST_RECIPE_ID,
            purpose,
            30,
            now,
        );
        let model = RecordingModel {
            fixture: ModelClientAdapter::new(crate::fixture_model_client()),
            requests: Mutex::new(Vec::new()),
        };
        let artifact = runtime
            .run(
                &attempt.permit,
                &attempt.node,
                attempt.node.input_artifacts.clone(),
                &model,
                now,
            )
            .await
            .unwrap();
        assert_eq!(artifact.kind, ArtifactKind::Claim);
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "{purpose:?}");
        assert_eq!(requests[0].phase, AgentTurnPhase::Submit);
        assert!(requests[0].tools.is_empty());
        assert!(requests[0].continuation.is_none());
        assert!(!requests[0].prompt.contains("Draft"));
        assert!(requests[0].terminal.is_some());
    }
}

#[tokio::test]
async fn shadow_critic_and_synthesizer_dispatch_submit_without_draft_or_reads() {
    for role in [RESEARCH_CRITIC_RECIPE_ID, RESEARCH_SYNTHESIZER_RECIPE_ID] {
        let now = Utc::now();
        let (store, runtime, analyst) = super::late_model_tests::isolated_agent_chain(
            &[RESEARCH_ANALYST_RECIPE_ID, role],
            RunPurpose::Shadow,
            30,
            now,
        );
        let mut inputs = analyst.node.input_artifacts.clone();
        let claim = runtime
            .run(
                &analyst.permit,
                &analyst.node,
                inputs.clone(),
                &ModelClientAdapter::new(crate::fixture_model_client()),
                now,
            )
            .await
            .unwrap();
        store
            .write_task_artifact(
                &analyst.permit,
                &claim,
                LifecycleEventType::ArtifactCommitted,
                now,
            )
            .unwrap();
        store
            .finish_task(&analyst.permit, akzio_domain::TaskStatus::Succeeded, now)
            .unwrap();
        inputs.push(ArtifactRef {
            artifact_id: claim.artifact_id,
            kind: claim.kind,
        });
        let mut attempt = store
            .claim_next_task_for_workload(
                "shadow-protocol-test",
                now,
                Duration::minutes(5),
                akzio_store::TaskWorkload::Any,
            )
            .unwrap()
            .unwrap();
        attempt.node.input_artifacts = inputs;
        // Stop at the dispatch boundary. Full output validation is exercised by
        // the formal Paper and PositionPlan fixtures; this checks Shadow routing.
        let model = RecordingModel {
            fixture: ModelClientAdapter::new(ModelClient::Fixture(json!({"output": []}))),
            requests: Mutex::new(Vec::new()),
        };
        let error = runtime
            .run(
                &attempt.permit,
                &attempt.node,
                attempt.node.input_artifacts.clone(),
                &model,
                now,
            )
            .await
            .unwrap_err();
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "{role}: {error:?}");
        assert_eq!(requests[0].phase, AgentTurnPhase::Submit);
        assert!(requests[0].tools.is_empty());
        assert!(requests[0].continuation.is_none());
        assert!(!requests[0].prompt.contains("Draft"));
        assert!(requests[0].terminal.is_some());
    }
}
