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
fn stream_idle_timeout_keeps_transport_retry_semantics() {
    let timeout = std::time::Duration::from_millis(40);
    let error = ModelError::StreamIdleTimeout {
        idle_timeout: timeout,
    };

    assert_eq!(model_error_result(&error)["error"], "stream_idle_timeout");
    assert_eq!(model_error_result(&error)["idle_timeout_ms"], 40);
    assert!(matches!(
        model_client_error(error, None),
        ResearchError::Model(message) if message.contains("response stream idle")
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
    assert!(prompt.contains("research.analyst priority 1-90"));
    assert!(prompt.contains("research.synthesizer priority 1-100"));
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
    assert!(prompt.contains("domain=null"));
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

    assert_eq!(
        candidate.contract.version,
        super::catalogue::ANALYST_FRESHNESS_CANDIDATE_VERSION
    );
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
