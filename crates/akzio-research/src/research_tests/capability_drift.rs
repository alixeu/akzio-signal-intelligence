#[tokio::test]
async fn capability_drift_stops_retry_before_the_second_model_call() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let model = CapabilityDriftModel {
        snapshots: AtomicU8::new(0),
        calls: AtomicU8::new(0),
    };

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::CapabilityMismatch {
                capability: "stateless_continuation",
            provider_id,
            model_id,
        }) if provider_id == "fixture" && model_id == "fixture-drifted"
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.snapshots.load(Ordering::SeqCst), 2);

    let traces: Vec<Value> = store
        .events_after(&claimed.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.event_type == "agent.turn_retryable_failed"
                || event.event_type == "agent.turn_failed"
        })
        .filter_map(|event| event.artifact_id)
        .map(|artifact_id| {
            let artifact = store.artifact(&artifact_id).unwrap();
            serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap()
        })
        .collect();
    assert_eq!(traces.len(), 2);
    assert_eq!(traces[0]["capability_snapshot"]["model_id"], "fixture");
    assert_eq!(
        traces[1]["capability_snapshot"]["model_id"],
        "fixture-drifted"
    );
    assert_ne!(
        traces[0]["capability_snapshot_hash"],
        traces[1]["capability_snapshot_hash"]
    );
    store.verify_integrity().unwrap();
}

#[derive(Debug)]
struct ToolThenOutputModel {
    evidence_id: ArtifactId,
    calls: AtomicU8,
}

impl AgentModel for ToolThenOutputModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => Err(ResearchError::ModelDebug {
                    error_class: "transport",
                    message: "transient fixture failure".to_owned(),
                    trace: ModelCallTrace {
                        request: json!({"fixture": "failed-provider-request"}),
                        result: json!({"error": "fixture-transport"}),
                    },
                }),
                1 => {
                    assert_eq!(request.phase, AgentTurnPhase::Draft);
                    assert!(request.tool_outputs.is_empty());
                    let mut turn = tool_turn(AgentToolCall {
                        call_id: "fixture-read-evidence".to_owned(),
                        name: "read_artifact".to_owned(),
                        arguments: json!({"artifact_id": self.evidence_id.0.as_str()}),
                    });
                    turn.model_debug = Some(ModelCallTrace {
                        request: json!({"fixture": "provider-request"}),
                        result: json!({"fixture": "provider-result"}),
                    });
                    Ok(turn)
                }
                2 => {
                    assert_eq!(request.phase, AgentTurnPhase::Draft);
                    assert_eq!(request.tool_outputs.len(), 1);
                    assert_eq!(
                        request.tool_outputs[0].output["value"],
                        json!({"price": 100})
                    );
                    Ok(draft_turn("fixture evidence supports the claim"))
                }
                3 => {
                    assert_eq!(request.phase, AgentTurnPhase::Submit);
                    Ok(submission_turn(json!({
                        "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                                        "topic": "market_regime",
                                        "statement": "The selected price evidence supports the stated regime claim.",
                                        "horizon": "t5",
                                        "stance": "bullish",
                                        "materiality_ppm": 800_000,
                                        "confidence_ppm": 700_000,
                                        "grounds": [{
                                            "evidence": {
                                                "artifact_id": self.evidence_id.0.as_str(),
                                                "kind": "normalized_evidence"
                                            },
                                            "support": "The governed evidence supplied the price used in this claim."
                        }],
                        "evidence_gaps": []
                    })))
                }
                _ => panic!("runtime requested an unexpected extra model turn"),
            }
        })
    }
}

#[derive(Debug)]
struct FixedModel(AgentModelTurn);

impl AgentModel for FixedModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            if request.phase == AgentTurnPhase::Draft
                && self.0.terminal_submission.is_some()
                && self.0.tool_calls.is_empty()
            {
                Ok(draft_turn("fixed fixture research memo"))
            } else {
                Ok(self.0.clone())
            }
        })
    }
}

#[derive(Debug)]
struct RepairSubmissionModel {
    evidence_id: ArtifactId,
    calls: AtomicU8,
}

impl AgentModel for RepairSubmissionModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        Box::pin(async move {
            match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    assert_eq!(request.phase, AgentTurnPhase::Draft);
                    Ok(draft_turn("fixture memo before repaired submission"))
                }
                1 => {
                    assert_eq!(request.phase, AgentTurnPhase::Submit);
                    Ok(submission_turn(json!({"summary": 42})))
                }
                2 => {
                    assert_eq!(request.phase, AgentTurnPhase::Submit);
                    assert_eq!(request.tool_outputs.len(), 1);
                    assert_eq!(
                        request.tool_outputs[0].output["error"],
                        "invalid_submission"
                    );
                    Ok(submission_turn(json!({
                        "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                        "topic": "repaired",
                        "statement": "The repaired submission uses governed evidence.",
                        "horizon": "t1",
                        "stance": "neutral",
                        "materiality_ppm": 100_000,
                        "confidence_ppm": 900_000,
                        "grounds": [{
                            "evidence": {
                                "artifact_id": self.evidence_id.0.as_str(),
                                "kind": "normalized_evidence"
                            },
                            "support": "Governed fixture evidence."
                        }],
                        "evidence_gaps": []
                    })))
                }
                _ => panic!("unexpected repair fixture turn"),
            }
        })
    }
}
