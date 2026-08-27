#[test]
fn capability_preflight_is_fail_closed_for_unknown_and_missing_features() {
    let structured_request = capability_request(json!({"type": "object"}), vec![]);
    assert!(matches!(
        validate_model_capabilities(&ModelCapabilitySnapshot::unknown(), &structured_request),
        Err(ResearchError::CapabilityMismatch {
            capability: "stateless_continuation",
            provider_id,
            model_id,
        }) if provider_id == "unknown" && model_id == "unknown"
    ));

    let mut no_continuation = fixture_capabilities();
    no_continuation.supports_stateless_continuation = false;
    assert!(matches!(
        validate_model_capabilities(&no_continuation, &structured_request),
        Err(ResearchError::CapabilityMismatch {
            capability: "stateless_continuation",
            ..
        })
    ));

    let tool_request = capability_request(Value::Null, vec![capability_tool()]);
    let mut no_tools = fixture_capabilities();
    no_tools.supports_tool_calls = false;
    assert!(matches!(
        validate_model_capabilities(&no_tools, &tool_request),
        Err(ResearchError::CapabilityMismatch {
            capability: "tool_calls",
            ..
        })
    ));
}

#[derive(Debug)]
struct CapabilityProbeModel {
    snapshot: ModelCapabilitySnapshot,
    calls: AtomicU8,
}

impl AgentModel for CapabilityProbeModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        self.snapshot.clone()
    }

    fn turn<'a>(&'a self, _: AgentModelRequest) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(ResearchError::Model(
                "capability preflight bypassed".to_owned(),
            ))
        })
    }
}

#[tokio::test]
async fn capability_mismatch_is_durable_and_makes_zero_model_calls() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let model = CapabilityProbeModel {
        snapshot: ModelCapabilitySnapshot::unknown(),
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
            ..
        })
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);

    let failure_id = store
        .events_after(&claimed.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .find(|event| event.event_type == "agent.turn_failed")
        .and_then(|event| event.artifact_id)
        .expect("capability mismatch is durable");
    assert!(!store
        .events_after(&claimed.run_id, 0, 100)
        .unwrap()
        .iter()
        .any(|event| event.event_type == LifecycleEventType::AgentTurnStarted.as_str()));
    let failure = store.artifact(&failure_id).unwrap();
    let trace: Value = serde_json::from_slice(&store.read_blob(&failure.blob).unwrap()).unwrap();
    assert_eq!(trace["error_class"], "capability_mismatch");
    assert_eq!(trace["capability_snapshot"]["provider_id"], "unknown");
    store.verify_integrity().unwrap();
}

#[derive(Debug)]
struct CapabilityDriftModel {
    snapshots: AtomicU8,
    calls: AtomicU8,
}

impl AgentModel for CapabilityDriftModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        if self.snapshots.fetch_add(1, Ordering::SeqCst) == 0 {
            fixture_capabilities()
        } else {
            let mut snapshot = fixture_capabilities();
            snapshot.model_id = "fixture-drifted".to_owned();
            snapshot.supports_stateless_continuation = false;
            snapshot
        }
    }

    fn turn<'a>(&'a self, _: AgentModelRequest) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(ResearchError::ModelDebug {
                error_class: "transport",
                message: "retry fixture failure".to_owned(),
                trace: ModelCallTrace {
                    request: json!({"fixture": "retry-request"}),
                    result: json!({"error": "retry-transport"}),
                },
            })
        })
    }
}
