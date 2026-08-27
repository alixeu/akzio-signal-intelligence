#[tokio::test]
async fn invalid_terminal_submission_repairs_once_without_tool_side_effects() {
    let fixture = fixture_with(|_| {});
    let model = RepairSubmissionModel {
        evidence_id: fixture.evidence.artifact_id.clone(),
        calls: AtomicU8::new(0),
    };
    let runtime = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue,
        Duration::minutes(5),
    );
    let output = runtime
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
        .await
        .unwrap();

    assert_eq!(output.kind, ArtifactKind::Claim);
    assert_eq!(model.calls.load(Ordering::SeqCst), 3);
    assert!(!fixture
        .store
        .events_after(&fixture.claimed.run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| event.event_type == LifecycleEventType::ToolCalled.as_str()));
}

#[derive(Debug)]
struct ToolCountModel {
    tool_count: Arc<AtomicU8>,
    evidence_id: ArtifactId,
}

impl AgentModel for ToolCountModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        let tool_count = Arc::clone(&self.tool_count);
        let evidence_id = self.evidence_id.clone();
        Box::pin(async move {
            if request.phase == AgentTurnPhase::Draft {
                tool_count.store(request.tools.len() as u8, Ordering::SeqCst);
                return Ok(draft_turn("debug fixture research memo"));
            }
            Ok(submission_turn(json!({
                "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                "topic": "debug",
                "statement": "Debug context is sufficient without a model tool call.",
                "horizon": "t1",
                "stance": "neutral",
                "materiality_ppm": 100_000,
                "confidence_ppm": 900_000,
                "grounds": [{
                    "evidence": {
                        "artifact_id": evidence_id.0.as_str(),
                        "kind": "normalized_evidence"
                    },
                    "support": "The debug fixture is already present in the authorized context."
                }],
                "evidence_gaps": []
            })))
        })
    }
}

#[tokio::test]
async fn debug_run_advertises_read_artifact_tool() {
    let fixture = fixture_with(|_| {});
    let tool_count = Arc::new(AtomicU8::new(u8::MAX));
    let model = ToolCountModel {
        tool_count: Arc::clone(&tool_count),
        evidence_id: fixture.evidence.artifact_id.clone(),
    };
    let runtime = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue,
        Duration::minutes(5),
    );

    let output = runtime
        .run(
            &fixture.claimed.permit,
            &fixture.claimed.node,
            [ArtifactRef {
                artifact_id: fixture.evidence.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &model,
            Utc::now(),
        )
        .await
        .unwrap();

    assert_eq!(output.kind, ArtifactKind::Claim);
    assert_eq!(tool_count.load(Ordering::SeqCst), 1);
}

#[derive(Debug)]
struct DelayedToolModel {
    evidence_id: ArtifactId,
}

impl AgentModel for DelayedToolModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(&'a self, _: AgentModelRequest) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        let evidence_id = self.evidence_id.clone();
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            Ok(tool_turn(AgentToolCall {
                call_id: "fixture-expired-grant".to_owned(),
                name: "read_artifact".to_owned(),
                arguments: json!({"artifact_id": evidence_id.0.as_str()}),
            }))
        })
    }
}

#[derive(Debug)]
struct SlowOutputModel;

impl AgentModel for SlowOutputModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(&'a self, _: AgentModelRequest) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
            Ok(draft_turn("too late"))
        })
    }
}
