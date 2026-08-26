use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use akzio_domain::{
    ArtifactLifecycle, ContextPolicy, ContractId, ContractPurpose, FailureDisposition,
    OutputContract, PromptBundle, RetryPolicy, TaskBudget, TaskRecipeId, TaskStatus,
    TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowGraph, WorkflowNode,
    V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoredRun, WorkflowCommit};
use tempfile::tempdir;

use super::*;
use akzio_runtime::v2::{
    DECISION_GATE_RECIPE_ID, EVALUATE_RECIPE_ID, EVIDENCE_GATE_RECIPE_ID, EXECUTION_GATE_RECIPE_ID,
    PAPER_COMMIT_RECIPE_ID, RECONCILE_RECIPE_ID,
};

#[test]
fn deliberation_scores_are_additive_for_old_records_and_validated_for_new_runs() {
    let old: AgentOutputEnvelope = serde_json::from_value(json!({
        "result": {},
        "deliberation": {
            "selected_path": "hold",
            "alternatives": ["defer"],
            "uncertainties": ["market gap"],
            "basis_artifact_ids": [],
            "confidence_ppm": 750000
        }
    }))
    .unwrap();
    assert!(old.deliberation.alternative_match_ppm.is_empty());
    assert!(old.deliberation.uncertainty_weight_ppm.is_empty());
    old.deliberation.validate().unwrap();

    let mut legacy_oversized = old.deliberation.clone();
    legacy_oversized.uncertainties = vec![
        "market gap".to_owned(),
        "macro release".to_owned(),
        "liquidity".to_owned(),
        "overnight move".to_owned(),
    ];
    legacy_oversized.validate().unwrap();

    let mut unscored_new_output = old.deliberation.clone();
    unscored_new_output.assessment_source = Some("model_assessed".to_owned());
    assert!(unscored_new_output.validate_model_assessment().is_err());
    legacy_oversized.assessment_source = Some("model_assessed".to_owned());
    legacy_oversized.validate().unwrap();
    assert!(legacy_oversized.validate_model_assessment().is_err());

    let scored: AgentOutputEnvelope = serde_json::from_value(json!({
        "result": {},
        "deliberation": {
            "selected_path": "hold",
            "alternatives": ["defer"],
            "alternative_match_ppm": [400000],
            "uncertainties": ["market gap"],
            "uncertainty_weight_ppm": [250000],
            "assessment_source": "model_assessed",
            "basis_artifact_ids": [],
            "confidence_ppm": 750000
        }
    }))
    .unwrap();
    scored.deliberation.validate_model_assessment().unwrap();

    let mut invalid = scored.deliberation;
    invalid.uncertainty_weight_ppm = vec![249999];
    assert!(invalid.validate().is_err());
}

#[test]
fn missing_model_output_keeps_invalid_output_classification() {
    assert!(matches!(
        model_client_error(ModelError::MissingOutput, None),
        ResearchError::InvalidOutput(_)
    ));
}

#[test]
fn only_invalid_output_errors_request_task_retry() {
    let invalid = [
        ResearchError::InvalidOutput("invalid JSON".to_owned()),
        ResearchError::MissingFinalOutput,
        ResearchError::ModelDebug {
            error_class: "invalid_output",
            message: "missing field".to_owned(),
            trace: ModelCallTrace {
                request: json!({}),
                result: json!({}),
            },
        },
    ];

    assert!(invalid
        .iter()
        .all(|error| error.retry_cause() == Some(RetryCause::InvalidOutput)));
    assert_eq!(
        ResearchError::Model("transport".to_owned()).retry_cause(),
        None
    );
    assert_eq!(
        ResearchError::ToolNotGranted("read_artifact".to_owned()).retry_cause(),
        None
    );
}

#[test]
fn read_tool_is_hidden_when_selected_context_exceeds_tool_budget() {
    assert!(should_advertise_read_tools(RunPurpose::Paper, 4, 4));
    assert!(!should_advertise_read_tools(RunPurpose::Paper, 10, 4));
    assert!(should_advertise_read_tools(RunPurpose::Debug, 1, 4));
}

#[test]
fn invalid_output_retry_respects_contract_policy() {
    let invalid = [
        ResearchError::InvalidOutput("invalid JSON".to_owned()),
        ResearchError::MissingFinalOutput,
        ResearchError::ModelDebug {
            error_class: "invalid_output",
            message: "missing field".to_owned(),
            trace: ModelCallTrace {
                request: json!({}),
                result: json!({}),
            },
        },
    ];
    let retry = RetryPolicy {
        max_attempts: 2,
        initial_backoff_ms: 0,
        retry_transport: false,
        retry_rate_limited: false,
        retry_invalid_output: true,
    };
    assert!(invalid
        .iter()
        .all(|error| retryable_model_error(error, &retry)));

    let no_retry = RetryPolicy {
        retry_invalid_output: false,
        ..retry
    };
    assert!(invalid
        .iter()
        .all(|error| !retryable_model_error(error, &no_retry)));
    assert!(!retryable_model_error(
        &ResearchError::Model("transport".to_owned()),
        &retry,
    ));
}

#[test]
fn planner_prompt_states_research_intent_bounds() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let planner = canonical_active_contracts(&store)
        .unwrap()
        .into_iter()
        .find(|contract| contract.purpose.as_str() == PLANNER_RECIPE_ID)
        .unwrap();
    let prompt = String::from_utf8(store.read_blob(&planner.prompt.role).unwrap()).unwrap();
    assert!(prompt.contains("max_results 1-32"));
    assert!(prompt.contains("priority 0-100"));
    assert!(prompt.contains("max_age_secs 1-604800"));
    assert!(prompt.contains("window_start and window_end must be null or RFC3339 timestamps"));
}

#[test]
fn analyst_prompt_requires_exact_context_artifact_refs() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let analyst = canonical_active_contracts(&store)
        .unwrap()
        .into_iter()
        .find(|contract| contract.purpose.as_str() == RESEARCH_ANALYST_RECIPE_ID)
        .unwrap();
    let prompt = String::from_utf8(store.read_blob(&analyst.prompt.role).unwrap()).unwrap();

    assert!(prompt.contains("exact 64-character artifact_id"));
    assert!(prompt.contains("Never use the ContextManifest ID"));
    assert!(prompt.contains("Include at least one ground"));
}

#[test]
fn analyst_freshness_candidate_is_inactive_and_capability_bounded() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let active = ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
    let baseline = active
        .contracts
        .contracts()
        .find(|installed| installed.contract.purpose.as_str() == RESEARCH_ANALYST_RECIPE_ID)
        .unwrap()
        .contract
        .clone();

    let candidate = active
        .install_analyst_freshness_candidate(&store, Utc::now())
        .unwrap();

    assert_eq!(candidate.contract.version, 5);
    assert!(baseline.permits_candidate(&candidate.contract));
    assert!(store
        .active_contract(&candidate.contract.purpose)
        .unwrap()
        .is_some_and(|stored| stored.contract.contract_hash == baseline.contract_hash));
    assert!(store
        .contract_installation(&candidate.contract.contract_hash)
        .unwrap()
        .is_some_and(|stored| stored.activated_at.is_none()));
    assert!(active
        .contracts
        .with_installed_candidate(candidate.clone())
        .unwrap()
        .get(&candidate.contract.contract_hash)
        .is_ok());
    store.verify_integrity().unwrap();
}

#[test]
fn planner_schema_rejects_natural_language_windows() {
    let schema = planner_draft_output_schema();
    let window = &schema["properties"]["tasks"]["additionalProperties"]["properties"]
        ["research_intents"]["items"]["properties"]["window_start"];

    assert!(validate_schema_value(&json!("latest"), window, "$.window_start").is_err());
    assert!(validate_schema_value(&Value::Null, window, "$.window_start").is_ok());
    assert!(
        validate_schema_value(&json!("2026-08-15T00:00:00Z"), window, "$.window_start",).is_ok()
    );
}

fn fixture_capabilities() -> ModelCapabilitySnapshot {
    ModelCapabilitySnapshot {
        provider_id: "fixture".to_owned(),
        model_id: "fixture".to_owned(),
        reasoning_effort: "none".to_owned(),
        supports_tool_calls: true,
        supports_stateless_continuation: true,
        native_web_tool: false,
        streaming: Some(false),
        declared_context_limit: None,
        declared_max_output_tokens: None,
        source: "test_declared".to_owned(),
    }
}

fn fixture_continuation(label: &str) -> ModelContinuation {
    ModelContinuation::from_items(vec![json!({
        "type": "message",
        "content": [{"type": "output_text", "text": label}],
    })])
}

fn draft_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn {
        assistant_text: Some(text.to_owned()),
        tool_calls: vec![],
        terminal_submission: None,
        continuation: fixture_continuation(text),
        telemetry: None,
        model_debug: None,
    }
}

fn tool_turn(call: AgentToolCall) -> AgentModelTurn {
    AgentModelTurn {
        assistant_text: None,
        tool_calls: vec![call],
        terminal_submission: None,
        continuation: fixture_continuation("tool call"),
        telemetry: None,
        model_debug: None,
    }
}

fn submission_turn(output: Value) -> AgentModelTurn {
    AgentModelTurn {
        assistant_text: None,
        tool_calls: vec![],
        terminal_submission: Some(AgentTerminalSubmission {
            call_id: "fixture-submit".to_owned(),
            arguments: output,
        }),
        continuation: fixture_continuation("submission"),
        telemetry: None,
        model_debug: None,
    }
}

fn capability_request(output_schema: Value, tools: Vec<AgentToolDefinition>) -> AgentModelRequest {
    AgentModelRequest {
        contract_hash: akzio_domain::ContentHash::of_bytes(b"capability-contract"),
        purpose: "research.analyst".to_owned(),
        prompt: "capability test".to_owned(),
        objective: "capability test".to_owned(),
        manifest_artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(
            b"capability-manifest",
        )),
        context: vec![],
        phase: AgentTurnPhase::Submit,
        continuation: Some(ModelContinuation::from_items(vec![])),
        tool_outputs: vec![],
        continuation_instruction: Some("submit".to_owned()),
        max_output_tokens: 32,
        tools,
        terminal: Some(AgentTerminalDefinition {
            description: "submit".to_owned(),
            input_schema: output_schema,
        }),
    }
}

fn capability_tool() -> AgentToolDefinition {
    AgentToolDefinition {
        name: "read_artifact".to_owned(),
        description: "capability test".to_owned(),
        input_schema: json!({"type": "object"}),
        strict: true,
    }
}

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

fn contract(store: &V2Store) -> AgentContract {
    AgentContract::new(
        ContractId::new(),
        1,
        ContractPurpose::new("research.analyst").unwrap(),
        "produce a claim",
        PromptBundle {
            version: 1,
            governance: store.put_bytes(b"governance", "text/plain").unwrap(),
            role: store.put_bytes(b"prompt", "text/plain").unwrap(),
        },
        ContextPolicy {
            permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
            permitted_source_families: BTreeSet::from(["market".to_owned()]),
            min_artifacts: 1,
            max_artifacts: 4,
            max_bytes: 4096,
            max_tokens: 1024,
            allow_raw_reread: false,
        },
        vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["market".to_owned()],
        }],
        vec![ToolSpec {
            name: "read_artifact".to_owned(),
            description: "read granted artifact".to_owned(),
            kind: ToolKind::ReadEvidence,
            input_schema: store.put_json(&artifact_id_tool_input_schema()).unwrap(),
            strict: true,
        }],
        OutputContract {
            artifact_kind: ArtifactKind::Claim,
            schema: store.put_json(&claim_output_schema()).unwrap(),
        },
        TaskBudget {
            max_input_tokens: 1024,
            max_output_tokens: 128,
            max_wall_time_secs: 30,
            max_tool_calls: 2,
        },
        RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        },
        TerminationPolicy::leaf(),
        FailureDisposition::FailRun,
    )
    .unwrap()
}

fn provenance() -> ArtifactProvenance {
    ArtifactProvenance {
        source_family: "market".to_owned(),
        observed_at: None,
        retrieved_at: Utc::now(),
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: None,
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    store: V2Store,
    catalogue: ContractCatalogue,
    claimed: akzio_store::v2::ClaimedAttempt,
    evidence: Artifact,
}

fn fixture_with(configure: impl FnOnce(&mut AgentContract)) -> Fixture {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contract = contract(&store);
    configure(&mut contract);
    contract.candidate_capability_ceiling.context = contract.context.clone();
    contract.candidate_capability_ceiling.tool_grants = contract.tool_grants.clone();
    contract.contract_hash = contract.expected_hash().unwrap();
    contract.validate().unwrap();
    let catalogue = ContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
    let node = WorkflowNode {
        task_id: akzio_domain::TaskId::new(),
        recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
        contract_hash: Some(contract.contract_hash.clone()),
        objective: "claim".to_owned(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget: contract.budget.clone(),
        retry: contract.retry.clone(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "test".to_owned(),
        nodes: vec![node.clone()],
    };
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&graph).unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        provenance(),
        None,
        vec![],
        Utc::now(),
    )
    .unwrap();
    let run = StoredRun {
        run_id: akzio_domain::RunId::new(),
        purpose: akzio_domain::RunPurpose::Debug,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run,
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("fixture", Utc::now(), Duration::seconds(60))
        .unwrap()
        .unwrap();
    let evidence = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store
            .put_bytes(br#"{"price":100}"#, "application/json")
            .unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        provenance(),
        Some(claimed.permit.artifact_origin()),
        vec![],
        Utc::now(),
    )
    .unwrap();
    store
        .write_task_artifact(
            &claimed.permit,
            &evidence,
            LifecycleEventType::EvidenceNormalized,
            Utc::now(),
        )
        .unwrap();
    Fixture {
        _root: root,
        store,
        catalogue,
        claimed,
        evidence,
    }
}

#[test]
fn planner_draft_schema_is_closed_and_governed() {
    let schema = planner_draft_output_schema();
    let valid = serde_json::json!({
        "schema_version": V2_DOMAIN_SCHEMA_VERSION,
        "topology_id": "active",
        "tasks": {
            "analyst": {
                "recipe_id": "research.analyst",
                "objective": "analyse governed TQQQ evidence",
                "depends_on": [],
                "priority": 50,
                "evidence_needs": [{
                    "schema_version": V2_DOMAIN_SCHEMA_VERSION,
                    "source_family": "alpaca",
                    "resource": "bars:TQQQ:1d",
                    "max_age_secs": 86400
                }]
            }
        }
    });
    validate_schema_value(&valid, &schema, "$").unwrap();

    let mut invalid_version = valid.clone();
    invalid_version["schema_version"] = serde_json::json!(V2_DOMAIN_SCHEMA_VERSION + 1);
    assert!(validate_schema_value(&invalid_version, &schema, "$").is_err());

    let mut invalid_recipe = valid.clone();
    invalid_recipe["tasks"]["analyst"]["recipe_id"] = serde_json::json!("gate.paper");
    assert!(validate_schema_value(&invalid_recipe, &schema, "$").is_err());

    let mut invalid_source = valid.clone();
    invalid_source["tasks"]["analyst"]["evidence_needs"][0]["source_family"] =
        serde_json::json!("uninstalled-web");
    assert!(validate_schema_value(&invalid_source, &schema, "$").is_err());

    let mut invalid_priority = valid.clone();
    invalid_priority["tasks"]["analyst"]["priority"] = serde_json::json!(101);
    assert!(validate_schema_value(&invalid_priority, &schema, "$").is_err());

    let mut artifact_ref = valid.clone();
    artifact_ref["tasks"]["analyst"]["artifact_id"] = serde_json::json!("sha256:forged");
    assert!(validate_schema_value(&artifact_ref, &schema, "$").is_err());

    let mut tool_or_role = valid;
    tool_or_role["tasks"]["analyst"]["tool"] = serde_json::json!("fetch_web");
    assert!(validate_schema_value(&tool_or_role, &schema, "$").is_err());
}

#[test]
fn active_catalogue_installs_canonical_contracts_and_bounded_recipes() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let active = ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
    let expected = [
        (PLANNER_RECIPE_ID, ArtifactKind::WorkflowProposalDraft),
        ("research.analyst", ArtifactKind::Claim),
        ("research.critic", ArtifactKind::Critique),
        ("research.synthesizer", ArtifactKind::DecisionProposal),
        (
            LEARNING_OUTCOME_WORKER_RECIPE_ID,
            ArtifactKind::RetrospectiveDraft,
        ),
    ];

    assert_eq!(active.contracts.contracts().count(), expected.len());
    for (purpose, output_kind) in expected {
        let installed = active
            .contracts
            .contracts()
            .find(|installed| installed.contract.purpose.as_str() == purpose)
            .unwrap();
        assert_eq!(installed.contract.output.artifact_kind, output_kind);
        assert_eq!(
            installed.contract.context.min_artifacts,
            if purpose == PLANNER_RECIPE_ID { 0 } else { 1 }
        );
        assert_eq!(
            installed.contract.termination.require_evidence,
            purpose != PLANNER_RECIPE_ID
        );
        let recipe = active
            .recipes
            .recipe(&TaskRecipeId::new(purpose).unwrap())
            .unwrap();
        assert_eq!(
            recipe.contract_hash.as_ref(),
            Some(&installed.contract.contract_hash)
        );
        assert_eq!(recipe.budget, installed.contract.budget);
        assert_eq!(recipe.retry, installed.contract.retry);
        assert_eq!(recipe.on_failure, installed.contract.on_failure);
        assert_eq!(
            recipe.max_children,
            installed.contract.termination.max_child_tasks
        );
        assert_eq!(recipe.max_depth, installed.contract.termination.max_depth);
        assert_eq!(
            recipe.allowed_evidence_sources,
            recipe_evidence_sources(&installed.contract)
        );
    }

    for (recipe_id, task_class) in [
        (EVIDENCE_GATE_RECIPE_ID, RuntimeTaskClass::Evidence),
        (DECISION_GATE_RECIPE_ID, RuntimeTaskClass::DecisionGate),
        (EXECUTION_GATE_RECIPE_ID, RuntimeTaskClass::ExecutionGate),
        (PAPER_COMMIT_RECIPE_ID, RuntimeTaskClass::PaperCommit),
        (RECONCILE_RECIPE_ID, RuntimeTaskClass::Reconcile),
        (EVALUATE_RECIPE_ID, RuntimeTaskClass::Evaluate),
    ] {
        let recipe = active
            .recipes
            .recipe(&TaskRecipeId::new(recipe_id).unwrap())
            .unwrap();
        assert_eq!(recipe.task_class, task_class);
        assert_eq!(recipe.contract_hash, None);
        assert!(recipe.allowed_evidence_sources.is_empty());
        if task_class == RuntimeTaskClass::Evidence {
            assert_eq!(recipe.retry.max_attempts, 5);
            assert_eq!(recipe.retry.initial_backoff_ms, 1_000);
            assert!(recipe.retry.retry_transport);
            assert!(recipe.retry.retry_rate_limited);
            assert!(!recipe.retry.retry_invalid_output);
        } else if task_class == RuntimeTaskClass::ExecutionGate {
            assert_eq!(recipe.retry.max_attempts, 2);
            assert_eq!(recipe.retry.initial_backoff_ms, 1_000);
            assert!(recipe.retry.retry_transport);
            assert!(recipe.retry.retry_rate_limited);
            assert!(!recipe.retry.retry_invalid_output);
            assert_eq!(recipe.budget.max_wall_time_secs, 90);
        } else {
            assert_eq!(recipe.retry, RetryPolicy::none());
            assert_eq!(recipe.budget.max_wall_time_secs, 30);
        }
    }
    let worker_recipe = active
        .recipes
        .recipe(&TaskRecipeId::new(LEARNING_OUTCOME_WORKER_RECIPE_ID).unwrap())
        .unwrap();
    assert_eq!(worker_recipe.task_class, RuntimeTaskClass::Evaluate);
    assert!(worker_recipe.contract_hash.is_some());
    store.verify_integrity().unwrap();
}

#[test]
fn active_catalogue_restores_store_owned_heads_after_restart() {
    let root = tempdir().unwrap();
    let now = Utc::now();
    let store = V2Store::open(root.path()).unwrap();
    let first = ActiveResearchCatalogue::install(&store, now).unwrap();
    let expected = first
        .contracts
        .contracts()
        .map(|installed| {
            (
                installed.contract.purpose.as_str().to_owned(),
                installed.contract.contract_hash.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    drop(first);
    drop(store);

    let reopened = V2Store::open(root.path()).unwrap();
    let restored = ActiveResearchCatalogue::install(&reopened, now + Duration::seconds(1)).unwrap();
    let actual = restored
        .contracts
        .contracts()
        .map(|installed| {
            (
                installed.contract.purpose.as_str().to_owned(),
                installed.contract.contract_hash.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(actual, expected);
    reopened.verify_integrity().unwrap();
}

#[test]
fn active_catalogue_upgrades_an_older_bounded_canonical_version() {
    let root = tempdir().unwrap();
    let now = Utc::now();
    let store = V2Store::open(root.path()).unwrap();
    let mut older = canonical_active_contracts(&store).unwrap();
    for contract in &mut older {
        contract.version = ACTIVE_CONTRACT_VERSION - 1;
        contract.prompt.version = ACTIVE_PROMPT_BUNDLE_VERSION - 1;
        contract.contract_hash = contract.expected_hash().unwrap();
        contract.validate().unwrap();
        store.install_active_contract(contract, now).unwrap();
    }

    let upgraded = ActiveResearchCatalogue::install(&store, now + Duration::seconds(1)).unwrap();
    for installed in upgraded.contracts.contracts() {
        assert_eq!(installed.contract.version, ACTIVE_CONTRACT_VERSION);
        assert_eq!(
            installed.contract.prompt.version,
            ACTIVE_PROMPT_BUNDLE_VERSION
        );
        assert_eq!(
            store
                .active_contract(&installed.contract.purpose)
                .unwrap()
                .unwrap()
                .contract
                .contract_hash,
            installed.contract.contract_hash
        );
    }
    store.verify_integrity().unwrap();
}

#[test]
fn candidate_install_is_durable_bounded_and_non_executable() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let now = Utc::now();
    let active = ActiveResearchCatalogue::install(&store, now).unwrap();
    let baseline = active
        .contracts
        .contracts()
        .find(|installed| installed.contract.purpose.as_str() == PLANNER_RECIPE_ID)
        .unwrap()
        .contract
        .clone();
    let mut candidate = baseline.clone();
    candidate.version += 1;
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();

    let installed = active
        .install_candidate(&store, &baseline.contract_hash, &candidate, now)
        .unwrap();
    assert_eq!(installed.contract, candidate);
    assert_eq!(
        store
            .active_contract(&baseline.purpose)
            .unwrap()
            .unwrap()
            .contract
            .contract_hash,
        baseline.contract_hash
    );
    assert_eq!(
        store
            .contract_installation(&candidate.contract_hash)
            .unwrap()
            .unwrap()
            .baseline_contract_hash,
        Some(baseline.contract_hash.clone())
    );
    assert!(active.contracts.get(&candidate.contract_hash).is_err());

    let mut expanded = candidate;
    expanded.version += 1;
    expanded
        .context
        .permitted_source_families
        .insert("unapproved_source".to_owned());
    expanded.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
        context: expanded.context.clone(),
        tool_grants: expanded.tool_grants.clone(),
    };
    expanded.contract_hash = expanded.expected_hash().unwrap();
    expanded.validate().unwrap();
    assert!(matches!(
        active.install_candidate(&store, &baseline.contract_hash, &expanded, now),
        Err(ResearchError::CandidateCapabilityExpansion { .. })
    ));
    store.verify_integrity().unwrap();
}

#[test]
fn decision_proposal_schema_matches_typed_decision_draft() {
    let schema = decision_proposal_output_schema();
    let forecasts = akzio_domain::Asset::EXECUTABLE
        .into_iter()
        .flat_map(|asset| {
            ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                json!({
                    "asset": asset.symbol(),
                    "horizon": horizon,
                    "positive_return_probability_ppm": 500000,
                    "expected_return_ppm": 0,
                })
            })
        })
        .collect::<Vec<_>>();
    let valid = json!({
        "summary": "blocked fixture decision",
        "confidence_ppm": 500000,
        "forecasts": forecasts,
        "claims": [],
        "critiques": [],
        "evidence": [],
        "material_conflicts": [],
        "hard_blockers": ["missing_evidence"],
        "soft_warnings": []
    });

    validate_schema_value(&valid, &schema, "$").unwrap();
    serde_json::from_value::<akzio_domain::DecisionDraft>(valid)
        .unwrap()
        .validate()
        .unwrap();
    for invalid in [
        json!({
            "summary": "invalid",
            "confidence_ppm": 500000,
            "blockers": ["anything"],
            "asset_views": {}
        }),
        json!({
            "summary": "extra field",
            "targets": {
                "weights": { "TQQQ": 0, "QQQ": 0, "SOXX": 0, "SOXL": 0 }
            },
            "confidence_ppm": 500000,
            "forecasts": [],
            "claims": [],
            "critiques": [],
            "evidence": [],
            "material_conflicts": [],
            "hard_blockers": ["missing_evidence"],
            "soft_warnings": [],
            "authority": "paper"
        }),
    ] {
        assert!(validate_schema_value(&invalid, &schema, "$").is_err());
    }
}

#[test]
fn schema_validator_accepts_nullable_union_types() {
    let schema = json!({"type": ["string", "null"]});

    validate_schema_value(&Value::Null, &schema, "$").unwrap();
    validate_schema_value(&json!("fixture"), &schema, "$").unwrap();
    assert!(validate_schema_value(&json!(42), &schema, "$").is_err());
}

#[test]
fn artifact_reference_schema_enforces_sha256_pattern() {
    let schema = artifact_ref_schema(&["claim"]);
    let valid = json!({
        "artifact_id": "a".repeat(64),
        "kind": "claim",
    });
    validate_schema_value(&valid, &schema, "$").unwrap();

    let invalid = json!({
        "artifact_id": "not-a-content-hash",
        "kind": "claim",
    });
    assert!(validate_schema_value(&invalid, &schema, "$").is_err());
}

#[test]
fn active_catalogue_rejects_planner_that_does_not_output_a_draft() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contracts = canonical_active_contracts(&store).unwrap();
    let planner = contracts
        .iter_mut()
        .find(|contract| contract.purpose.as_str() == PLANNER_RECIPE_ID)
        .unwrap();
    planner.output.artifact_kind = ArtifactKind::WorkflowProposal;
    planner.contract_hash = planner.expected_hash().unwrap();
    planner.validate().unwrap();
    let catalogue = ContractCatalogue::install(&store, contracts, Utc::now()).unwrap();

    assert!(matches!(
        catalogue.active_recipe_catalogue(&store),
        Err(ResearchError::ActiveContractOutputMismatch {
            purpose,
            expected: ArtifactKind::WorkflowProposalDraft,
            actual: ArtifactKind::WorkflowProposal,
        }) if purpose == PLANNER_RECIPE_ID
    ));
}

#[test]
fn active_catalogue_rejects_candidate_or_unknown_contract_recipe() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contracts = canonical_active_contracts(&store).unwrap();
    let mut candidate = contracts
        .iter()
        .find(|contract| contract.purpose.as_str() == "research.analyst")
        .unwrap()
        .clone();
    candidate.contract_id = ContractId("akzio.v2.research.candidate".to_owned());
    candidate.version = 2;
    candidate.purpose = ContractPurpose::new("research.candidate").unwrap();
    candidate.responsibility = "candidate data only".to_owned();
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();
    contracts.push(candidate);
    let catalogue = ContractCatalogue::install(&store, contracts, Utc::now()).unwrap();

    assert!(matches!(
        catalogue.active_recipe_catalogue(&store),
        Err(ResearchError::UnexpectedActiveContractPurpose(purpose))
            if purpose == "research.candidate"
    ));
}

#[test]
fn contract_catalogue_rejects_duplicate_hash_and_identity_version() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let contract = contract(&store);
    let catalogue = ContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
    assert_eq!(
        catalogue.contract_hash_for(&contract.contract_id, contract.version),
        Some(&contract.contract_hash)
    );

    assert!(matches!(
        ContractCatalogue::install(&store, [contract.clone(), contract.clone()], Utc::now(),),
        Err(ResearchError::DuplicateContract(_))
    ));

    let mut changed = contract.clone();
    changed.responsibility = "different responsibility".to_owned();
    changed.contract_hash = changed.expected_hash().unwrap();
    changed.validate().unwrap();
    assert!(matches!(
        ContractCatalogue::install(&store, [contract, changed], Utc::now()),
        Err(ResearchError::DuplicateContractVersion { .. })
    ));
}

#[test]
fn contract_catalogue_rejects_candidate_capability_expansion() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let active = contract(&store);
    let catalogue = ContractCatalogue::install(&store, [active.clone()], Utc::now()).unwrap();

    let mut candidate = active.clone();
    candidate
        .context
        .permitted_source_families
        .insert("news".to_owned());
    candidate.tool_grants[0]
        .allowed_sources
        .push("news".to_owned());
    candidate.candidate_capability_ceiling = akzio_domain::CandidateCapabilityCeiling {
        context: candidate.context.clone(),
        tool_grants: candidate.tool_grants.clone(),
    };
    candidate.contract_hash = candidate.expected_hash().unwrap();
    candidate.validate().unwrap();

    assert!(matches!(
        catalogue.validate_candidate(&active.contract_hash, &candidate),
        Err(ResearchError::CandidateCapabilityExpansion { .. })
    ));

    let mut narrowed = active.clone();
    narrowed.budget.max_input_tokens /= 2;
    narrowed.contract_hash = narrowed.expected_hash().unwrap();
    narrowed.validate().unwrap();
    catalogue
        .validate_candidate(&active.contract_hash, &narrowed)
        .unwrap();
}

#[tokio::test]
async fn model_client_adapter_debug_trace_retains_the_provider_request_and_result() {
    let artifact_hash = akzio_domain::ContentHash::of_bytes(b"fixture-artifact");
    let adapter = ModelClientAdapter::with_debug(
        akzio_model::ModelClient::Fixture(json!({
            "output_text": "",
            "tool_calls": [{
                "call_id": "fixture-tool",
                "name": "read_artifact",
                "arguments": {"artifact_id": artifact_hash.as_str()},
            }],
        })),
        true,
    );
    let response = adapter
        .turn(AgentModelRequest {
            contract_hash: akzio_domain::ContentHash::of_bytes(b"fixture-contract"),
            purpose: "research.analyst".to_owned(),
            phase: AgentTurnPhase::Draft,
            prompt: "fixture prompt".to_owned(),
            objective: "fixture objective".to_owned(),
            manifest_artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(
                b"fixture-manifest",
            )),
            context: vec![],
            continuation: None,
            tool_outputs: vec![],
            continuation_instruction: None,
            max_output_tokens: 32,
            tools: vec![AgentToolDefinition {
                name: "read_artifact".to_owned(),
                description: "fixture".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"artifact_id": {"type": "string"}},
                    "required": ["artifact_id"],
                    "additionalProperties": false,
                }),
                strict: true,
            }],
            terminal: None,
        })
        .await
        .unwrap();

    assert!(response.assistant_text.is_none());
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].name, "read_artifact");
    assert_eq!(
        response.tool_calls[0].arguments["artifact_id"],
        artifact_hash.to_string()
    );
    let trace = response.model_debug.expect("debug trace is retained");
    assert_eq!(trace.request["model"], "fixture");
    assert_eq!(trace.request["tool_choice"], "auto");
    assert_eq!(trace.request["tools"][0]["strict"], true);
    assert_eq!(trace.result["tool_calls"][0]["call_id"], "fixture-tool");
}

#[tokio::test]
async fn agent_runtime_rejects_a_grant_that_expires_during_a_model_turn() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::milliseconds(1));
    let model = DelayedToolModel {
        evidence_id: evidence.artifact_id.clone(),
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
        Err(ResearchError::Context(ContextError::GrantDenied { .. }))
    ));
    store.verify_integrity().unwrap();
}

#[tokio::test]
async fn agent_runtime_rejects_a_stale_permit_before_model_call() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store, catalogue, Duration::minutes(5));
    let model = ToolThenOutputModel {
        evidence_id: evidence.artifact_id.clone(),
        calls: AtomicU8::new(0),
    };
    let mut stale_permit = claimed.permit.clone();
    stale_permit.epoch = stale_permit.epoch.saturating_add(1);

    assert!(matches!(
        runtime
            .run(
                &stale_permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::Store(StoreError::StalePermit(_)))
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn agent_runtime_rejects_a_node_from_another_task() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store, catalogue, Duration::minutes(5));
    let model = FixedModel(submission_turn(json!({"summary": "should not run"})));
    let mut foreign_node = claimed.node.clone();
    foreign_node.task_id = akzio_domain::TaskId::new();

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &foreign_node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::TaskMismatch)
    ));
}

#[tokio::test]
async fn agent_runtime_enforces_tool_source_family_scope() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        ..
    } = fixture_with(|contract| {
        contract
            .context
            .permitted_source_families
            .insert("news".to_owned());
    });
    let news = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store
            .put_bytes(br#"{"headline":"fixture"}"#, "application/json")
            .unwrap(),
        "fixture.news",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "news".to_owned(),
            ..provenance()
        },
        Some(claimed.permit.artifact_origin()),
        vec![],
        Utc::now(),
    )
    .unwrap();
    store
        .write_task_artifact(
            &claimed.permit,
            &news,
            LifecycleEventType::EvidenceNormalized,
            Utc::now(),
        )
        .unwrap();
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let model = FixedModel(tool_turn(AgentToolCall {
        call_id: "fixture-news-denied".to_owned(),
        name: "read_artifact".to_owned(),
        arguments: json!({"artifact_id": news.artifact_id.0.as_str()}),
    }));

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: news.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &model,
                Utc::now(),
            )
            .await,
        Err(ResearchError::ToolSourceNotGranted { .. })
    ));
    let failure_id = store
        .events_after(&claimed.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .find(|event| event.event_type == "tool.failed")
        .and_then(|event| event.artifact_id)
        .expect("failed tool result is durable");
    let failure = store.artifact(&failure_id).unwrap();
    let trace: Value = serde_json::from_slice(&store.read_blob(&failure.blob).unwrap()).unwrap();
    assert_eq!(trace["ok"], false);
    assert_eq!(trace["error"]["code"], "tool_source_not_granted");
    assert!(failure
        .source_refs
        .iter()
        .any(|reference| reference.kind == ArtifactKind::ToolCall));
    store.verify_integrity().unwrap();
}

#[tokio::test]
async fn agent_runtime_records_invalid_tool_arguments_before_rejecting() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|_| {});
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let model = FixedModel(tool_turn(AgentToolCall {
        call_id: "fixture-invalid-arguments".to_owned(),
        name: "read_artifact".to_owned(),
        arguments: json!({"unexpected": true}),
    }));

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
        Err(ResearchError::InvalidOutput(_))
    ));

    let failure_id = store
        .events_after(&claimed.run_id, 0, 100)
        .unwrap()
        .into_iter()
        .find(|event| event.event_type == "tool.failed")
        .and_then(|event| event.artifact_id)
        .expect("invalid tool result is durable");
    let failure = store.artifact(&failure_id).unwrap();
    let trace: Value = serde_json::from_slice(&store.read_blob(&failure.blob).unwrap()).unwrap();
    assert_eq!(trace["ok"], false);
    assert_eq!(trace["error"]["code"], "invalid_tool_arguments");
    let call = failure
        .source_refs
        .iter()
        .find(|reference| reference.kind == ArtifactKind::ToolCall)
        .and_then(|reference| store.artifact(&reference.artifact_id).ok())
        .expect("invalid tool call is durable");
    let call_trace: Value = serde_json::from_slice(&store.read_blob(&call.blob).unwrap()).unwrap();
    assert_eq!(call_trace["call"]["arguments"], json!({"unexpected": true}));
}

#[tokio::test]
async fn agent_runtime_records_and_rejects_an_overdue_model_turn() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|contract| contract.budget.max_wall_time_secs = 1);
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));

    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &SlowOutputModel,
                Utc::now(),
            )
            .await,
        Err(ResearchError::WallTimeExceeded { maximum_secs: 1 })
    ));
    store.verify_integrity().unwrap();
}

#[tokio::test]
async fn agent_runtime_records_complete_tool_trace_and_contract_validated_claim() {
    let root = tempdir().unwrap();
    let store = V2Store::open(root.path()).unwrap();
    let mut contract = contract(&store);
    contract.budget.max_output_tokens = 512;
    contract.contract_hash = contract.expected_hash().unwrap();
    let catalogue = ContractCatalogue::install(&store, [contract.clone()], Utc::now()).unwrap();
    let node = WorkflowNode {
        task_id: akzio_domain::TaskId::new(),
        recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
        contract_hash: Some(contract.contract_hash.clone()),
        objective: "claim".to_owned(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget: contract.budget.clone(),
        retry: contract.retry.clone(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "test".to_owned(),
        nodes: vec![node.clone()],
    };
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&graph).unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        provenance(),
        None,
        vec![],
        Utc::now(),
    )
    .unwrap();
    let run = StoredRun {
        run_id: akzio_domain::RunId::new(),
        purpose: RunPurpose::Paper,
        topology_id: graph.topology_id.clone(),
        graph_artifact_id: graph_artifact.artifact_id.clone(),
        created_at: Utc::now(),
    };
    store
        .commit_workflow(&WorkflowCommit {
            run,
            graph: graph_artifact,
            nodes: graph.nodes,
        })
        .unwrap();
    let claimed = store
        .claim_next_task("fixture", Utc::now(), Duration::seconds(60))
        .unwrap()
        .unwrap();
    let evidence = Artifact::new(
        ArtifactKind::NormalizedEvidence,
        store
            .put_bytes(br#"{"price":100}"#, "application/json")
            .unwrap(),
        "fixture",
        ArtifactLifecycle::RunScoped,
        provenance(),
        Some(claimed.permit.artifact_origin()),
        vec![],
        Utc::now(),
    )
    .unwrap();
    store
        .write_task_artifact(
            &claimed.permit,
            &evidence,
            LifecycleEventType::EvidenceNormalized,
            Utc::now(),
        )
        .unwrap();
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let model = ToolThenOutputModel {
        evidence_id: evidence.artifact_id.clone(),
        calls: AtomicU8::new(0),
    };
    let output = runtime
        .run(
            &claimed.permit,
            &claimed.node,
            [ArtifactRef {
                artifact_id: evidence.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &model,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(output.kind, ArtifactKind::Claim);
    assert!(matches!(
        store.artifact(&output.artifact_id),
        Err(StoreError::MissingArtifact(id)) if id == output.artifact_id
    ));
    assert_eq!(model.calls.load(Ordering::SeqCst), 4);
    assert!(output
        .source_refs
        .iter()
        .any(|source| source.kind == ArtifactKind::ContextManifest));
    assert!(output
        .source_refs
        .iter()
        .any(|source| source.kind == ArtifactKind::AgentTurn));
    assert!(output.source_refs.iter().any(|source| {
        source.kind == ArtifactKind::NormalizedEvidence
            && source.artifact_id == evidence.artifact_id
    }));
    let tool_result = output
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ToolResult)
        .expect("output retains the tool result trace");
    let tool_result = store.artifact(&tool_result.artifact_id).unwrap();
    assert!(tool_result
        .source_refs
        .iter()
        .any(|source| source.kind == ArtifactKind::ToolCall));
    assert!(tool_result
        .source_refs
        .iter()
        .any(|source| source.artifact_id == evidence.artifact_id));
    let tool_trace: Value =
        serde_json::from_slice(&store.read_blob(&tool_result.blob).unwrap()).unwrap();
    assert!(tool_trace["request_hash"].as_str().is_some());
    let tool_call = tool_result
        .source_refs
        .iter()
        .find(|source| source.kind == ArtifactKind::ToolCall)
        .and_then(|source| store.artifact(&source.artifact_id).ok())
        .expect("tool call trace is durable");
    let tool_call_trace: Value =
        serde_json::from_slice(&store.read_blob(&tool_call.blob).unwrap()).unwrap();
    assert_eq!(
        tool_call_trace["call"]["arguments"]["artifact_id"],
        evidence.artifact_id.0.as_str()
    );
    let turn_trace = output
        .source_refs
        .iter()
        .filter(|source| source.kind == ArtifactKind::AgentTurn)
        .filter_map(|source| store.artifact(&source.artifact_id).ok())
        .map(|artifact| {
            serde_json::from_slice::<Value>(&store.read_blob(&artifact.blob).unwrap()).unwrap()
        })
        .find(|trace| trace["response"]["model_debug"]["request"]["fixture"] == "provider-request")
        .expect("agent turn trace retains request and response");
    assert!(turn_trace["request_hash"].as_str().is_some());
    let capability_snapshot = turn_trace["capability_snapshot"].clone();
    assert_eq!(capability_snapshot["provider_id"], "fixture");
    assert_eq!(
        turn_trace["capability_snapshot_hash"],
        serde_json::to_value(capability_snapshot_hash(&fixture_capabilities()).unwrap()).unwrap()
    );
    assert!(turn_trace["tool_set_hash"].as_str().is_some());
    assert_eq!(turn_trace["request"]["tools"][0]["strict"], true);
    assert_eq!(
        turn_trace["response"]["model_debug"]["request"]["fixture"],
        "provider-request"
    );
    assert_eq!(
        turn_trace["response"]["model_debug"]["result"]["fixture"],
        "provider-result"
    );
    let failed_turn_trace = output
        .source_refs
        .iter()
        .filter(|source| source.kind == ArtifactKind::AgentTurn)
        .filter_map(|source| store.artifact(&source.artifact_id).ok())
        .map(|artifact| {
            serde_json::from_slice::<Value>(&store.read_blob(&artifact.blob).unwrap()).unwrap()
        })
        .find(|trace| trace["error_class"] == "transport")
        .expect("failed agent turn trace is durable");
    assert_eq!(
        failed_turn_trace["model_debug"]["request"]["fixture"],
        "failed-provider-request"
    );
    assert_eq!(
        failed_turn_trace["model_debug"]["result"]["error"],
        "fixture-transport"
    );
    assert_eq!(
        failed_turn_trace["capability_snapshot"],
        capability_snapshot
    );
    assert_eq!(
        failed_turn_trace["capability_snapshot_hash"],
        turn_trace["capability_snapshot_hash"]
    );

    let malformed = FixedModel(submission_turn(json!({"summary": 42})));
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &malformed,
                Utc::now(),
            )
            .await,
        Err(ResearchError::InvalidOutput(_))
    ));

    let too_many_basis_ids = FixedModel(submission_turn(json!({
        "result": {
            "schema_version": V2_DOMAIN_SCHEMA_VERSION,
            "topic": "bounded deliberation",
            "statement": "The governed evidence supports a bounded neutral claim.",
            "horizon": "t1",
            "stance": "neutral",
            "materiality_ppm": 100_000,
            "confidence_ppm": 900_000,
            "grounds": [{
                "evidence": {
                    "artifact_id": evidence.artifact_id.0.as_str(),
                    "kind": "normalized_evidence"
                },
                "support": "The governed evidence is selected."
            }],
            "evidence_gaps": []
        },
        "deliberation": {
            "selected_path": "Use governed evidence.",
            "alternatives": [],
            "alternative_match_ppm": [],
            "uncertainties": [],
            "uncertainty_weight_ppm": [],
            "basis_artifact_ids": vec![evidence.artifact_id.0.as_str(); 9],
            "confidence_ppm": 900_000
        }
    })));
    let error = runtime
        .run(
            &claimed.permit,
            &claimed.node,
            [ArtifactRef {
                artifact_id: evidence.artifact_id.clone(),
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &too_many_basis_ids,
            Utc::now(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ResearchError::InvalidOutput(_)),
        "{error:?}"
    );

    let denied_tool = FixedModel(tool_turn(AgentToolCall {
        call_id: "fixture-denied-raw".to_owned(),
        name: "read_raw_evidence".to_owned(),
        arguments: json!({"artifact_id": evidence.artifact_id.0.as_str()}),
    }));
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &denied_tool,
                Utc::now(),
            )
            .await,
        Err(ResearchError::ToolNotGranted(_))
    ));

    let over_budget_tools = FixedModel(AgentModelTurn {
        assistant_text: None,
        tool_calls: (0..3)
            .map(|index| AgentToolCall {
                call_id: format!("fixture-over-budget-{index}"),
                name: "read_artifact".to_owned(),
                arguments: json!({"artifact_id": evidence.artifact_id.0.as_str()}),
            })
            .collect(),
        terminal_submission: None,
        continuation: fixture_continuation("over budget tools"),
        telemetry: None,
        model_debug: None,
    });
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &over_budget_tools,
                Utc::now(),
            )
            .await,
        Err(ResearchError::ToolBudgetExceeded)
    ));

    let mut mismatched_node = claimed.node.clone();
    mismatched_node.budget.max_tool_calls = 1;
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &mismatched_node,
                std::iter::empty::<ArtifactRef>(),
                &malformed,
                Utc::now(),
            )
            .await,
        Err(ResearchError::NodePolicyMismatch)
    ));
    store
        .commit_attempt(
            &claimed.permit,
            std::slice::from_ref(&output),
            TaskStatus::Succeeded,
            Utc::now(),
        )
        .unwrap();
    let persisted = store.artifact(&output.artifact_id).unwrap();
    assert_eq!(persisted.artifact_id, output.artifact_id);
    assert_eq!(persisted.kind, output.kind);
    store.verify_integrity().unwrap();
}

#[tokio::test]
async fn agent_runtime_enforces_input_and_output_token_budgets() {
    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|contract| contract.budget.max_input_tokens = 1);
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    let output = FixedModel(submission_turn(json!({"summary":"source-linked claim"})));
    let result = runtime
        .run(
            &claimed.permit,
            &claimed.node,
            [ArtifactRef {
                artifact_id: evidence.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            }],
            &output,
            Utc::now(),
        )
        .await;
    assert!(
        matches!(&result, Err(ResearchError::InputBudgetExceeded { .. })),
        "{result:?}"
    );
    store.verify_integrity().unwrap();

    let Fixture {
        _root,
        store,
        catalogue,
        claimed,
        evidence,
    } = fixture_with(|contract| contract.budget.max_output_tokens = 1);
    let runtime = AgentRuntime::new(store.clone(), catalogue, Duration::minutes(5));
    assert!(matches!(
        runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [ArtifactRef {
                    artifact_id: evidence.artifact_id,
                    kind: ArtifactKind::NormalizedEvidence,
                }],
                &output,
                Utc::now(),
            )
            .await,
        Err(ResearchError::OutputBudgetExceeded { .. })
    ));
    store.verify_integrity().unwrap();
}
