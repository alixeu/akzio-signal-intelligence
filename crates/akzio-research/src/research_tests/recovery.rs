fn recovery_manifest(
    fixture: &Fixture,
    permit: &TaskWritePermit,
    now: DateTime<Utc>,
) -> ContextManifest {
    let contract_hash = permit.contract_hash.as_ref().unwrap();
    let contract = &fixture.catalogue.get(contract_hash).unwrap().contract;
    ContextBroker::new(fixture.store.clone())
        .assemble(
            permit,
            contract,
            [ArtifactRef {
                artifact_id: fixture.evidence.artifact_id.clone(),
                kind: fixture.evidence.kind,
            }],
            now,
            Duration::minutes(5),
        )
        .unwrap()
}

fn recovery_request(
    fixture: &Fixture,
    manifest: &ContextManifest,
    phase: AgentTurnPhase,
    continuation: Option<ModelContinuation>,
    tool_outputs: Vec<ModelToolOutput>,
) -> AgentModelRequest {
    let contract = &fixture
        .catalogue
        .get(&manifest.payload.contract_hash)
        .unwrap()
        .contract;
    AgentModelRequest {
        contract_hash: contract.contract_hash.clone(),
        purpose: contract.purpose.as_str().to_owned(),
        phase,
        prompt: "recovery fixture".to_owned(),
        objective: "recover without replaying durable work".to_owned(),
        manifest_artifact_id: manifest.artifact.artifact_id.clone(),
        context: vec![],
        continuation,
        tool_outputs,
        continuation_instruction: None,
        max_output_tokens: contract.budget.max_output_tokens,
        tools: model_tool_definitions(&ContextBroker::new(fixture.store.clone()), contract).unwrap(),
        terminal: None,
    }
}

fn recovery_guard(
    manifest: &ContextManifest,
    request: &AgentModelRequest,
) -> AgentRecoveryGuard {
    AgentRecoveryGuard {
        contract_hash: request.contract_hash.clone(),
        context_manifest: manifest.payload.clone(),
        capability_snapshot_hash: capability_snapshot_hash(&fixture_capabilities()).unwrap(),
        draft_tool_set_hash: tool_set_hash(request).unwrap(),
        submit_tool_set_hash: tool_set_hash(request).unwrap(),
    }
}

fn write_recovery_turn(
    fixture: &Fixture,
    permit: &TaskWritePermit,
    manifest: &ContextManifest,
    turn: u16,
    request: &AgentModelRequest,
    response: &AgentModelTurn,
    now: DateTime<Utc>,
) -> Artifact {
    write_recovery_turn_with_hash(
        fixture,
        permit,
        manifest,
        turn,
        request,
        response,
        model_request_hash(request).unwrap(),
        now,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_recovery_turn_with_hash(
    fixture: &Fixture,
    permit: &TaskWritePermit,
    manifest: &ContextManifest,
    turn: u16,
    request: &AgentModelRequest,
    response: &AgentModelTurn,
    request_hash: akzio_domain::ContentHash,
    now: DateTime<Utc>,
) -> Artifact {
    fixture
        .store
        .append_task_event(permit, LifecycleEventType::AgentTurnStarted, now)
        .unwrap();
    let capabilities = fixture_capabilities();
    let artifact = Artifact::new(
        ArtifactKind::AgentTurn,
        fixture
            .store
            .put_json(&json!({
                "turn": turn,
                "attempt": 1,
                "contract_hash": request.contract_hash,
                "context_manifest": manifest.artifact.artifact_id,
                "request_hash": request_hash,
                "capability_snapshot": capabilities,
                "capability_snapshot_hash": capability_snapshot_hash(&capabilities).unwrap(),
                "tool_set_hash": tool_set_hash(request).unwrap(),
                "request": request,
                "response": response,
            }))
            .unwrap(),
        "agent.turn.fixture",
        ArtifactLifecycle::RunScoped,
        provenance(),
        Some(permit.artifact_origin()),
        vec![ArtifactRef {
            artifact_id: manifest.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        }],
        now,
    )
    .unwrap();
    fixture
        .store
        .write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::AgentTurnCompleted,
            now,
        )
        .unwrap();
    artifact
}

fn write_unfinished_tool_call(
    fixture: &Fixture,
    permit: &TaskWritePermit,
    manifest: &ContextManifest,
    request_hash: &akzio_domain::ContentHash,
    call: &AgentToolCall,
    now: DateTime<Utc>,
) {
    let artifact = Artifact::new(
        ArtifactKind::ToolCall,
        fixture
            .store
            .put_json(&json!({"request_hash": request_hash, "call": call}))
            .unwrap(),
        "agent.tool.fixture",
        ArtifactLifecycle::RunScoped,
        provenance(),
        Some(permit.artifact_origin()),
        vec![ArtifactRef {
            artifact_id: manifest.artifact.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        }],
        now,
    )
    .unwrap();
    fixture
        .store
        .write_task_artifact(permit, &artifact, LifecycleEventType::ToolCalled, now)
        .unwrap();
}

fn recover_attempt(
    fixture: &Fixture,
    when: DateTime<Utc>,
    worker: &str,
) -> akzio_store::v2::ClaimedAttempt {
    assert_eq!(fixture.store.recover_expired_tasks(when).unwrap(), 1);
    fixture
        .store
        .claim_next_task(worker, when + Duration::seconds(1), Duration::seconds(60))
        .unwrap()
        .unwrap()
}

fn recovery_budget(
    max_input_tokens: u32,
    max_output_tokens: u32,
    max_tool_calls: u16,
) -> AgentRunBudget {
    AgentRunBudget::new(
        &TaskBudget {
            max_input_tokens,
            max_output_tokens,
            max_wall_time_secs: 60,
            max_tool_calls,
        },
        &RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            retry_transport: false,
            retry_rate_limited: false,
            retry_invalid_output: false,
        },
    )
}

#[test]
fn recovery_checkpoint_promotes_durable_draft_memo_to_submit() {
    let fixture = fixture_with(|_| {});
    let now = Utc::now();
    let parent_manifest = recovery_manifest(&fixture, &fixture.claimed.permit, now);
    let request = recovery_request(
        &fixture,
        &parent_manifest,
        AgentTurnPhase::Draft,
        None,
        vec![],
    );
    let mut response = draft_turn("durable memo");
    response.telemetry = Some(AgentTurnTelemetry {
        latency_millis: 17,
        input_tokens: Some(23),
        output_tokens: Some(11),
    });
    write_recovery_turn(
        &fixture,
        &fixture.claimed.permit,
        &parent_manifest,
        0,
        &request,
        &response,
        now,
    );

    let child = recover_attempt(&fixture, now + Duration::hours(1), "recovery-memo");
    let child_manifest = recovery_manifest(&fixture, &child.permit, now + Duration::hours(1));
    let checkpoint = agent_recovery_checkpoint(
        &fixture.store,
        &child.permit,
        &recovery_guard(&child_manifest, &request),
    )
    .unwrap();

    assert!(checkpoint.is_recovered());
    assert_eq!(checkpoint.phase, AgentTurnPhase::Submit);
    assert_eq!(checkpoint.next_model_turn, 1);
    assert_eq!(checkpoint.continuation, Some(response.continuation));
    assert_eq!((checkpoint.provider_calls, checkpoint.tool_calls), (1, 0));
    assert_eq!(checkpoint.usage.input_tokens, 23);
    assert_eq!(checkpoint.usage.output_tokens, 11);
    assert_eq!(checkpoint.usage.latency_millis, 17);
    assert_eq!(checkpoint.trace_refs.len(), 1);

    let mut budget = recovery_budget(23, 11, 0);
    budget.restore(&checkpoint).unwrap();
    assert_eq!(budget.model_calls, 1);
    assert_eq!(budget.tool_calls, 0);
    assert_eq!(budget.input_tokens, 23);
    assert_eq!(budget.output_tokens, 11);
}

#[test]
fn recovery_checkpoint_estimates_missing_usage_fields() {
    let fixture = fixture_with(|_| {});
    let now = Utc::now();
    let manifest = recovery_manifest(&fixture, &fixture.claimed.permit, now);
    let request = recovery_request(
        &fixture,
        &manifest,
        AgentTurnPhase::Draft,
        None,
        vec![],
    );
    let mut response = draft_turn("estimate durable usage");
    response.telemetry = Some(AgentTurnTelemetry {
        latency_millis: 19,
        input_tokens: None,
        output_tokens: None,
    });
    let expected_input = estimate_tokens(&request).unwrap();
    let expected_output = estimate_turn_output_tokens(&response).unwrap();
    write_recovery_turn(
        &fixture,
        &fixture.claimed.permit,
        &manifest,
        0,
        &request,
        &response,
        now,
    );

    let child = recover_attempt(&fixture, now + Duration::hours(1), "recovery-estimate");
    let child_manifest = recovery_manifest(&fixture, &child.permit, now + Duration::hours(1));
    let checkpoint = agent_recovery_checkpoint(
        &fixture.store,
        &child.permit,
        &recovery_guard(&child_manifest, &request),
    )
    .unwrap();

    assert_eq!(checkpoint.usage.input_tokens, u64::from(expected_input));
    assert_eq!(checkpoint.usage.output_tokens, u64::from(expected_output));
    assert_eq!(checkpoint.usage.latency_millis, 19);

    let mut budget = recovery_budget(expected_input, expected_output, 0);
    budget.restore(&checkpoint).unwrap();
    assert_eq!(budget.input_tokens, expected_input);
    assert_eq!(budget.output_tokens, expected_output);
}

#[test]
fn recovery_budget_rejects_historical_overages_without_partial_restore() {
    let checkpoint = AgentRecoveryCheckpoint::fresh();

    let mut provider = checkpoint.clone();
    provider.provider_calls = 6;
    let mut budget = recovery_budget(10, 10, 2);
    assert!(matches!(
        budget.restore(&provider),
        Err(ResearchError::ModelCallBudgetExceeded)
    ));

    let mut tools = checkpoint.clone();
    tools.tool_calls = 3;
    assert!(matches!(
        budget.restore(&tools),
        Err(ResearchError::ToolBudgetExceeded)
    ));

    let mut input = checkpoint.clone();
    input.usage.input_tokens = 11;
    assert!(matches!(
        budget.restore(&input),
        Err(ResearchError::InputBudgetExceeded {
            actual: 11,
            maximum: 10,
        })
    ));

    let mut output = checkpoint;
    output.usage.output_tokens = 11;
    assert!(matches!(
        budget.restore(&output),
        Err(ResearchError::OutputBudgetExceeded {
            actual: 11,
            maximum: 10,
        })
    ));
    assert_eq!(
        (
            budget.model_calls,
            budget.tool_calls,
            budget.input_tokens,
            budget.output_tokens,
        ),
        (0, 0, 0, 0),
    );
}

#[test]
fn recovery_budget_rejects_counter_conversion_overflow() {
    let mut checkpoint = AgentRecoveryCheckpoint::fresh();
    checkpoint.tool_calls = u32::from(u16::MAX) + 1;
    let mut budget = recovery_budget(u32::MAX, u32::MAX, u16::MAX);
    assert!(matches!(
        budget.restore(&checkpoint),
        Err(ResearchError::ToolBudgetExceeded)
    ));

    checkpoint.tool_calls = 0;
    checkpoint.usage.input_tokens = u64::MAX;
    assert!(matches!(
        budget.restore(&checkpoint),
        Err(ResearchError::InputBudgetExceeded {
            actual: u32::MAX,
            maximum: u32::MAX,
        })
    ));

    checkpoint.usage.input_tokens = 0;
    checkpoint.usage.output_tokens = u64::MAX;
    assert!(matches!(
        budget.restore(&checkpoint),
        Err(ResearchError::OutputBudgetExceeded {
            actual: u32::MAX,
            maximum: u32::MAX,
        })
    ));
}

#[test]
fn recovery_checkpoint_reuses_complete_tool_results_in_call_order() {
    let fixture = fixture_with(|_| {});
    let now = Utc::now();
    let manifest = recovery_manifest(&fixture, &fixture.claimed.permit, now);
    let request = recovery_request(
        &fixture,
        &manifest,
        AgentTurnPhase::Draft,
        None,
        vec![],
    );
    let call = AgentToolCall {
        call_id: "recover-read".to_owned(),
        name: "read_artifact".to_owned(),
        arguments: json!({"artifact_id": fixture.evidence.artifact_id}),
    };
    let response = tool_turn(call.clone());
    write_recovery_turn(
        &fixture,
        &fixture.claimed.permit,
        &manifest,
        0,
        &request,
        &response,
        now,
    );
    let contract = &fixture
        .catalogue
        .get(&request.contract_hash)
        .unwrap()
        .contract;
    AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    )
    .execute_tool(
        &fixture.claimed.permit,
        contract,
        &manifest.grant,
        &call,
        &model_request_hash(&request).unwrap(),
        now,
    )
    .unwrap();

    let child = recover_attempt(&fixture, now + Duration::hours(1), "recovery-tool");
    let child_manifest = recovery_manifest(&fixture, &child.permit, now + Duration::hours(1));
    let checkpoint = agent_recovery_checkpoint(
        &fixture.store,
        &child.permit,
        &recovery_guard(&child_manifest, &request),
    )
    .unwrap();

    assert!(checkpoint.is_recovered());
    assert_eq!(checkpoint.phase, AgentTurnPhase::Draft);
    assert_eq!(checkpoint.next_model_turn, 1);
    assert_eq!(checkpoint.pending_tool_outputs.len(), 1);
    assert_eq!(checkpoint.pending_tool_outputs[0].call_id, call.call_id);
    assert_eq!((checkpoint.provider_calls, checkpoint.tool_calls), (1, 1));
    assert_eq!(checkpoint.trace_refs.len(), 2);
}

#[test]
fn recovery_checkpoint_stays_fresh_when_tool_result_is_missing() {
    let fixture = fixture_with(|_| {});
    let now = Utc::now();
    let manifest = recovery_manifest(&fixture, &fixture.claimed.permit, now);
    let request = recovery_request(
        &fixture,
        &manifest,
        AgentTurnPhase::Draft,
        None,
        vec![],
    );
    let call = AgentToolCall {
        call_id: "unfinished-read".to_owned(),
        name: "read_artifact".to_owned(),
        arguments: json!({"artifact_id": fixture.evidence.artifact_id}),
    };
    write_recovery_turn(
        &fixture,
        &fixture.claimed.permit,
        &manifest,
        0,
        &request,
        &tool_turn(call.clone()),
        now,
    );
    write_unfinished_tool_call(
        &fixture,
        &fixture.claimed.permit,
        &manifest,
        &model_request_hash(&request).unwrap(),
        &call,
        now,
    );
    let child = recover_attempt(&fixture, now + Duration::hours(1), "recovery-missing");
    let child_manifest = recovery_manifest(&fixture, &child.permit, now + Duration::hours(1));

    let checkpoint = agent_recovery_checkpoint(
        &fixture.store,
        &child.permit,
        &recovery_guard(&child_manifest, &request),
    )
    .unwrap();
    assert!(!checkpoint.is_recovered());
}

#[test]
fn recovery_checkpoint_rejects_hash_and_context_drift() {
    let fixture = fixture_with(|_| {});
    let now = Utc::now();
    let manifest = recovery_manifest(&fixture, &fixture.claimed.permit, now);
    let request = recovery_request(
        &fixture,
        &manifest,
        AgentTurnPhase::Draft,
        None,
        vec![],
    );
    write_recovery_turn(
        &fixture,
        &fixture.claimed.permit,
        &manifest,
        0,
        &request,
        &draft_turn("memo"),
        now,
    );
    let child = recover_attempt(&fixture, now + Duration::hours(1), "recovery-drift");
    let child_manifest = recovery_manifest(&fixture, &child.permit, now + Duration::hours(1));

    let mut hash_drift = recovery_guard(&child_manifest, &request);
    hash_drift.capability_snapshot_hash = akzio_domain::ContentHash::of_bytes(b"drift");
    assert!(!agent_recovery_checkpoint(&fixture.store, &child.permit, &hash_drift)
        .unwrap()
        .is_recovered());

    let mut context_drift = recovery_guard(&child_manifest, &request);
    context_drift.context_manifest.estimated_tokens = context_drift
        .context_manifest
        .estimated_tokens
        .saturating_add(1);
    assert!(!agent_recovery_checkpoint(&fixture.store, &child.permit, &context_drift)
        .unwrap()
        .is_recovered());
}

#[test]
fn retry_is_fresh_while_multilevel_recovery_folds_oldest_first() {
    let retry_fixture = fixture_with(|_| {});
    let now = Utc::now();
    let retry_manifest = recovery_manifest(&retry_fixture, &retry_fixture.claimed.permit, now);
    let retry_request = recovery_request(
        &retry_fixture,
        &retry_manifest,
        AgentTurnPhase::Draft,
        None,
        vec![],
    );
    write_recovery_turn(
        &retry_fixture,
        &retry_fixture.claimed.permit,
        &retry_manifest,
        0,
        &retry_request,
        &draft_turn("memo"),
        now,
    );
    retry_fixture
        .store
        .retry_task(&retry_fixture.claimed.permit, now, now)
        .unwrap();
    let retry = retry_fixture
        .store
        .claim_next_task("retry", now + Duration::seconds(1), Duration::seconds(60))
        .unwrap()
        .unwrap();
    let retry_child_manifest = recovery_manifest(&retry_fixture, &retry.permit, now);
    assert!(!agent_recovery_checkpoint(
        &retry_fixture.store,
        &retry.permit,
        &recovery_guard(&retry_child_manifest, &retry_request),
    )
    .unwrap()
    .is_recovered());

    let recovery_fixture = fixture_with(|contract| contract.retry.max_attempts = 3);
    let first_manifest = recovery_manifest(&recovery_fixture, &recovery_fixture.claimed.permit, now);
    let first_request = recovery_request(
        &recovery_fixture,
        &first_manifest,
        AgentTurnPhase::Draft,
        None,
        vec![],
    );
    write_recovery_turn(
        &recovery_fixture,
        &recovery_fixture.claimed.permit,
        &first_manifest,
        0,
        &first_request,
        &draft_turn("memo"),
        now,
    );
    let second = recover_attempt(&recovery_fixture, now + Duration::hours(1), "recovery-2");
    let third = recover_attempt(&recovery_fixture, now + Duration::hours(2), "recovery-3");
    let third_manifest = recovery_manifest(&recovery_fixture, &third.permit, now);
    let checkpoint = agent_recovery_checkpoint(
        &recovery_fixture.store,
        &third.permit,
        &recovery_guard(&third_manifest, &first_request),
    )
    .unwrap();
    assert!(checkpoint.is_recovered());
    assert_eq!(
        checkpoint.source,
        AgentRecoverySource::Recovered(vec![
            recovery_fixture.claimed.permit.attempt_id,
            second.permit.attempt_id,
        ])
    );
}

#[derive(Debug)]
struct RecoveredSubmitModel {
    evidence_id: ArtifactId,
    calls: AtomicU8,
}

impl AgentModel for RecoveredSubmitModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        let evidence_id = self.evidence_id.clone();
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            assert_eq!(call, 0, "durable Draft turn was replayed");
            assert_eq!(request.phase, AgentTurnPhase::Submit);
            assert!(request.context.is_empty());
            assert!(request.continuation.is_some());
            assert!(request.tool_outputs.is_empty());

            Ok(submission_turn(json!({
                "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                "topic": "recovery",
                "statement": "The durable Draft memo resumed directly at Submit.",
                "horizon": "t1",
                "stance": "neutral",
                "materiality_ppm": 100_000,
                "confidence_ppm": 900_000,
                "grounds": [{
                    "evidence": {
                        "artifact_id": evidence_id.0.as_str(),
                        "kind": "normalized_evidence"
                    },
                    "support": "The governed evidence remains available through the new grant.",
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
async fn agent_runtime_resumes_durable_draft_without_replaying_provider_turn() {
    let fixture = fixture_with(|_| {});
    let now = Utc::now();
    let parent_manifest = recovery_manifest(&fixture, &fixture.claimed.permit, now);
    let parent_request = recovery_request(
        &fixture,
        &parent_manifest,
        AgentTurnPhase::Draft,
        None,
        vec![],
    );
    write_recovery_turn(
        &fixture,
        &fixture.claimed.permit,
        &parent_manifest,
        0,
        &parent_request,
        &draft_turn("durable recovery memo"),
        now,
    );

    let child = recover_attempt(&fixture, now + Duration::hours(1), "runtime-recovery");
    let model = RecoveredSubmitModel {
        evidence_id: fixture.evidence.artifact_id.clone(),
        calls: AtomicU8::new(0),
    };
    let runtime = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    );
    let output = runtime
        .run(
            &child.permit,
            &child.node,
            [ArtifactRef {
                artifact_id: fixture.evidence.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &model,
            now + Duration::hours(1),
        )
        .await
        .unwrap();

    assert_eq!(output.kind, ArtifactKind::Claim);
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    let child_turns = fixture
        .store
        .attempt_events(&child.run_id, &child.node.task_id, &child.permit.attempt_id)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == LifecycleEventType::AgentTurnCompleted.as_str())
        .count();
    assert_eq!(child_turns, 1);
    fixture.store.verify_integrity().unwrap();
}

#[derive(Debug)]
struct RecoveredToolModel {
    evidence_id: ArtifactId,
    calls: AtomicU8,
}

impl AgentModel for RecoveredToolModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        request: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        let evidence_id = self.evidence_id.clone();
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            match call {
                0 => {
                    assert_eq!(request.phase, AgentTurnPhase::Draft);
                    assert_eq!(request.tool_outputs.len(), 1);
                    assert_eq!(request.tool_outputs[0].call_id, "recover-read");
                    Ok(draft_turn("durable tool result reused"))
                }
                1 => {
                    assert_eq!(request.phase, AgentTurnPhase::Submit);
                    Ok(submission_turn(json!({
                        "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                        "topic": "recovery tool",
                        "statement": "The durable tool result resumed without another read.",
                        "horizon": "t1",
                        "stance": "neutral",
                        "materiality_ppm": 100_000,
                        "confidence_ppm": 900_000,
                        "grounds": [{
                            "evidence": {
                                "artifact_id": evidence_id.0.as_str(),
                                "kind": "normalized_evidence"
                            },
                            "support": "The prior durable read remains in the recovery trace.",
                            "role": "descriptive",
                            "assets": [],
                            "domain": null
                        }],
                        "evidence_gaps": []
                    })))
                }
                _ => panic!("unexpected replayed provider turn"),
            }
        })
    }
}

#[tokio::test]
async fn agent_runtime_reuses_durable_tool_result_without_reexecuting_tool() {
    let fixture = fixture_with(|contract| contract.budget.max_output_tokens = 512);
    let now = Utc::now();
    let manifest = recovery_manifest(&fixture, &fixture.claimed.permit, now);
    let request = recovery_request(&fixture, &manifest, AgentTurnPhase::Draft, None, vec![]);
    let call = AgentToolCall {
        call_id: "recover-read".to_owned(),
        name: "read_artifact".to_owned(),
        arguments: json!({"artifact_id": fixture.evidence.artifact_id}),
    };
    write_recovery_turn(
        &fixture,
        &fixture.claimed.permit,
        &manifest,
        0,
        &request,
        &tool_turn(call.clone()),
        now,
    );
    let contract = &fixture
        .catalogue
        .get(&request.contract_hash)
        .unwrap()
        .contract;
    AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    )
    .execute_tool(
        &fixture.claimed.permit,
        contract,
        &manifest.grant,
        &call,
        &model_request_hash(&request).unwrap(),
        now,
    )
    .unwrap();

    let child = recover_attempt(&fixture, now + Duration::hours(1), "runtime-tool-recovery");
    let model = RecoveredToolModel {
        evidence_id: fixture.evidence.artifact_id.clone(),
        calls: AtomicU8::new(0),
    };
    let output = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    )
    .run(
        &child.permit,
        &child.node,
        [ArtifactRef {
            artifact_id: fixture.evidence.artifact_id.clone(),
            kind: ArtifactKind::NormalizedEvidence,
        }],
        &model,
        now + Duration::hours(1),
    )
    .await
    .unwrap();

    assert_eq!(output.kind, ArtifactKind::Claim);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let child_tool_calls = fixture
        .store
        .attempt_events(&child.run_id, &child.node.task_id, &child.permit.attempt_id)
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == LifecycleEventType::ToolCalled.as_str())
        .count();
    assert_eq!(child_tool_calls, 0);
    fixture.store.verify_integrity().unwrap();
}

#[derive(Debug)]
struct NoRecoveryTurnModel {
    calls: AtomicU8,
}

impl AgentModel for NoRecoveryTurnModel {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        fixture_capabilities()
    }

    fn turn<'a>(
        &'a self,
        _: AgentModelRequest,
    ) -> BoxFuture<'a, ResearchResult<AgentModelTurn>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { panic!("exhausted recovered budget reached the provider") })
    }
}

#[tokio::test]
async fn agent_runtime_restores_budget_before_resuming_provider() {
    let fixture = fixture_with(|_| {});
    let now = Utc::now();
    let manifest = recovery_manifest(&fixture, &fixture.claimed.permit, now);
    let request = recovery_request(&fixture, &manifest, AgentTurnPhase::Draft, None, vec![]);
    let mut response = draft_turn("budget exhausted");
    response.telemetry = Some(AgentTurnTelemetry {
        latency_millis: 1,
        input_tokens: Some(1),
        output_tokens: Some(128),
    });
    write_recovery_turn(
        &fixture,
        &fixture.claimed.permit,
        &manifest,
        0,
        &request,
        &response,
        now,
    );

    let child = recover_attempt(&fixture, now + Duration::hours(1), "runtime-budget-recovery");
    let model = NoRecoveryTurnModel {
        calls: AtomicU8::new(0),
    };
    let result = AgentRuntime::new(
        fixture.store.clone(),
        fixture.catalogue.clone(),
        Duration::minutes(5),
    )
    .run(
        &child.permit,
        &child.node,
        [ArtifactRef {
            artifact_id: fixture.evidence.artifact_id.clone(),
            kind: ArtifactKind::NormalizedEvidence,
        }],
        &model,
        now + Duration::hours(1),
    )
    .await;

    assert!(matches!(
        result,
        Err(ResearchError::OutputBudgetExceeded {
            actual: 129,
            maximum: 128
        })
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}
