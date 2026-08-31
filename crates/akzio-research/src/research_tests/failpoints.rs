#[derive(Debug)]
struct FailpointSubmissionModel {
    calls: AtomicU8,
    evidence_id: ArtifactId,
}

impl AgentModel for FailpointSubmissionModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        let evidence_id = self.evidence_id.clone();
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if request.phase == AgentTurnPhase::Draft {
                return Ok(draft_turn("failpoint recovery memo"));
            }
            Ok(submission_turn(json!({
                "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                "topic": "failpoint",
                "statement": "Durable recovery preserves the authorized evidence claim.",
                "horizon": "t1",
                "stance": "neutral",
                "materiality_ppm": 500_000,
                "confidence_ppm": 500_000,
                "grounds": [{
                    "evidence": {
                        "artifact_id": evidence_id.0.as_str(),
                        "kind": "normalized_evidence"
                    },
                    "support": "deterministic failpoint fixture evidence",
                    "role": "descriptive",
                    "assets": [],
                    "domain": null
                }],
                "evidence_gaps": []
            })))
        })
    }
}

#[tokio::test]
async fn failpoint_before_provider_request_has_zero_provider_calls() {
    let fixture = fixture_with(|contract| contract.retry.max_attempts = 3);
    let model = FailpointSubmissionModel {
        calls: AtomicU8::new(0),
        evidence_id: fixture.evidence.artifact_id.clone(),
    };
    let runtime = AgentRuntime::new(
        fixture.store,
        fixture.catalogue,
        Duration::minutes(5),
    )
    .with_failpoint(AgentFailpoint::BeforeProviderRequest);

    let result = runtime
        .run(
            &fixture.claimed.permit,
            &fixture.claimed.node,
            [ArtifactRef {
                artifact_id: fixture.evidence.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &model,
            Utc::now(),
        )
        .await;

    assert!(matches!(result, Err(ResearchError::InjectedFailpoint(_))));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}

async fn recover_submission_failpoint(point: AgentFailpoint) -> (u8, Artifact) {
    let fixture = fixture_with(|contract| contract.retry.max_attempts = 3);
    let now = Utc::now();
    let model = FailpointSubmissionModel {
        calls: AtomicU8::new(0),
        evidence_id: fixture.evidence.artifact_id.clone(),
    };
    let candidates = [ArtifactRef {
        artifact_id: fixture.evidence.artifact_id.clone(),
        kind: ArtifactKind::NormalizedEvidence,
    }];
    let first = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    )
    .with_failpoint(point)
    .run(
        &fixture.claimed.permit,
        &fixture.claimed.node,
        candidates.clone(),
        &model,
        now,
    )
    .await;
    assert!(matches!(first, Err(ResearchError::InjectedFailpoint(_))));

    let recovered = recover_attempt(&fixture, now + Duration::hours(1), "failpoint-recovery");
    let output = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    )
    .run(
        &recovered.permit,
        &recovered.node,
        candidates,
        &model,
        now + Duration::hours(1),
    )
    .await
    .unwrap();
    (model.calls.load(Ordering::SeqCst), output)
}

#[tokio::test]
async fn provider_and_terminal_failpoint_matrix_has_explicit_replay_semantics() {
    for (point, expected_calls) in [
        (AgentFailpoint::BeforeProviderRequest, 2),
        (
            AgentFailpoint::AfterProviderResponseBeforeTurnPersist,
            3,
        ),
        (AgentFailpoint::AfterAgentTurnPersist, 2),
        (AgentFailpoint::BeforeFinalSubmission, 4),
        (AgentFailpoint::AfterFinalSubmission, 4),
    ] {
        let (calls, output) = recover_submission_failpoint(point).await;
        assert_eq!(output.kind, ArtifactKind::Claim, "{point:?}");
        assert_eq!(calls, expected_calls, "{point:?}");
    }
}

#[derive(Debug)]
struct FailpointToolModel {
    calls: AtomicU8,
    evidence_id: ArtifactId,
}

impl AgentModel for FailpointToolModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        let evidence_id = self.evidence_id.clone();
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if request.phase == AgentTurnPhase::Submit {
                return Ok(submission_turn(json!({
                    "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                    "topic": "tool_failpoint",
                    "statement": "Recovered tool evidence supports the neutral claim.",
                    "horizon": "t1",
                    "stance": "neutral",
                    "materiality_ppm": 500_000,
                    "confidence_ppm": 500_000,
                    "grounds": [{
                        "evidence": {
                            "artifact_id": evidence_id.0.as_str(),
                            "kind": "normalized_evidence"
                        },
                        "support": "deterministic recovered tool evidence",
                        "role": "descriptive",
                        "assets": [],
                        "domain": null
                    }],
                    "evidence_gaps": []
                })));
            }
            if request.tool_outputs.is_empty() {
                return Ok(tool_turn(AgentToolCall {
                    call_id: "failpoint-read".to_owned(),
                    name: "read_document".to_owned(),
                    arguments: json!({"artifact_id": evidence_id.0.as_str()}),
                }));
            }
            Ok(draft_turn("tool result recovered into durable memo"))
        })
    }
}

async fn recover_tool_failpoint(point: AgentFailpoint) -> (u8, usize, Artifact) {
    let fixture = fixture_with(|contract| {
        contract.retry.max_attempts = 3;
        contract.budget.max_output_tokens = 512;
    });
    let now = Utc::now();
    let model = FailpointToolModel {
        calls: AtomicU8::new(0),
        evidence_id: fixture.evidence.artifact_id.clone(),
    };
    let candidates = [ArtifactRef {
        artifact_id: fixture.evidence.artifact_id.clone(),
        kind: ArtifactKind::NormalizedEvidence,
    }];
    let first = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    )
    .with_failpoint(point)
    .run(
        &fixture.claimed.permit,
        &fixture.claimed.node,
        candidates.clone(),
        &model,
        now,
    )
    .await;
    assert!(matches!(first, Err(ResearchError::InjectedFailpoint(_))));

    let recovered = recover_attempt(&fixture, now + Duration::hours(1), "tool-failpoint-recovery");
    let output = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    )
    .run(
        &recovered.permit,
        &recovered.node,
        candidates,
        &model,
        now + Duration::hours(1),
    )
    .await
    .unwrap();
    let tool_calls = fixture
        .store
        .events_after(&fixture.claimed.run_id, 0, 1_000)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == LifecycleEventType::ToolCalled.as_str())
        .count();
    (model.calls.load(Ordering::SeqCst), tool_calls, output)
}

#[tokio::test]
async fn tool_failpoint_matrix_distinguishes_missing_and_durable_results() {
    for (point, expected_provider_calls, expected_tool_calls) in [
        (AgentFailpoint::BeforeToolCallPersist, 4, 1),
        (AgentFailpoint::AfterToolCallPersist, 4, 2),
        (AgentFailpoint::BeforeToolResultPersist, 4, 2),
        (AgentFailpoint::AfterToolResultPersist, 3, 1),
    ] {
        let (provider_calls, tool_calls, output) = recover_tool_failpoint(point).await;
        assert_eq!(output.kind, ArtifactKind::Claim, "{point:?}");
        assert_eq!(provider_calls, expected_provider_calls, "{point:?}");
        assert_eq!(tool_calls, expected_tool_calls, "{point:?}");
    }
}
