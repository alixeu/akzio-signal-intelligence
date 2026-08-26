use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use super::*;
use crate::scheduler::paper_snapshot_resources;
use akzio_domain::{
    ArtifactLifecycle, ArtifactOrigin, ArtifactProvenance, ArtifactRef, Asset,
    ContextManifestPayload, EvidenceNeed, MarketClockSnapshot, MoneyMicros, Outcome,
    OutcomeExecutionLineage, OutcomeSchedule, PaperApprovalScope, PaperLaunchApproval, Quote,
    RetrospectiveStatus, RuntimeManifest, TaskRecipeId, WorkflowProposal, WorkflowProposalDraft,
    WorkflowProposalDraftTask, WorkflowProposalTask, V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_execution::paper::{CommittedPaperBroker, PaperError, PaperExecution, PaperOrderReceipt};
use akzio_ingest::{
    runtime::EvidenceAdapterError, AcquiredEvidence, AsyncEvidenceAdapter, EvidenceProvenance,
    EvidenceQuality, EvidenceRequest, NormalizedEvidencePayload, PaperDecodeError,
};
use akzio_research::{
    fixture_claim_output, fixture_critique_output, fixture_model_client, AgentModel,
};
use akzio_store::v2::AlertSeverity;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use chrono::{Datelike, Duration as ChronoDuration, Weekday};
use futures::{future::BoxFuture, StreamExt};
use tempfile::tempdir;
use tower::ServiceExt;

fn config(root: PathBuf) -> DaemonConfig {
    DaemonConfig {
        store_root: root,
        http_token: "fixture-token".to_owned(),
        observer_token: Some("fixture-observer-token".to_owned()),
        worker_count: 1,
        auto_paper: false,
        market_data_feed: Some(AlpacaMarketDataFeed::Iex),
        outcome_cost_model: OutcomeCostModel::default(),
        runtime_identity_hash: None,
    }
}

fn runtime_identity(seed: &str) -> RuntimeIdentity {
    RuntimeIdentity {
        code_revision: format!("revision-{seed}"),
        cargo_lock_hash: ContentHash::of_bytes(format!("cargo-{seed}").as_bytes()),
        config_hash: ContentHash::of_bytes(format!("config-{seed}").as_bytes()),
        provider_id: format!("provider-{seed}"),
        model_id: format!("model-{seed}"),
        prompt_hash: ContentHash::of_bytes(format!("prompt-{seed}").as_bytes()),
        contract_hash: ContentHash::of_bytes(format!("contract-{seed}").as_bytes()),
        topology_hash: ContentHash::of_bytes(format!("topology-{seed}").as_bytes()),
        decision_policy_hash: ContentHash::of_bytes(format!("decision-{seed}").as_bytes()),
        execution_policy_hash: ContentHash::of_bytes(format!("execution-{seed}").as_bytes()),
        evaluation_policy_hash: ContentHash::of_bytes(format!("evaluation-{seed}").as_bytes()),
        market_data_feed: "iex".to_owned(),
    }
}

#[tokio::test]
async fn paper_approval_rejects_a_mismatched_runtime_identity_before_broker_io() {
    let directory = tempdir().unwrap();
    let expected = runtime_identity("expected");
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    daemon_config.runtime_identity_hash = Some(expected.identity_hash().unwrap());
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();

    let error = daemon
        .approve_paper(PaperApprovalRequest {
            session_key: "2026-08-25".to_owned(),
            operator: "fixture-operator".to_owned(),
            reason: "identity mismatch test".to_owned(),
            max_notional_usd_cents: 10_000,
            valid_hours: 1,
            identity: runtime_identity("other"),
        })
        .await
        .unwrap_err();

    assert!(
        matches!(error, DaemonError::InvalidInput(message) if message.contains("runtime identity"))
    );
    assert!(daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::PaperLaunchApproval)
        .unwrap()
        .is_none());
}

#[test]
fn daemon_selects_the_configured_model_for_each_stage() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::open(
        config(directory.path().to_path_buf()),
        akzio_model::ModelConfig {
            base_url: "http://fixture/v1".to_owned(),
            model: "global-model".to_owned(),
            api_key: "fixture-key".to_owned(),
            reasoning_effort: "low".to_owned(),
            response_language: "English".to_owned(),
            debug: false,
            routes: std::collections::BTreeMap::from([(
                "research.critic".to_owned(),
                akzio_model::ModelRouteConfig {
                    model: "critic-model".to_owned(),
                    reasoning_effort: "high".to_owned(),
                    response_language: Some("简体中文".to_owned()),
                },
            )]),
        },
    )
    .unwrap();

    let global = daemon.model_for("research.planner").capability_snapshot();
    let critic = daemon.model_for("research.critic").capability_snapshot();
    assert_eq!(global.model_id, "global-model");
    assert_eq!(global.reasoning_effort, "low");
    assert_eq!(critic.model_id, "critic-model");
    assert_eq!(critic.reasoning_effort, "high");
    assert_eq!(
        daemon.model_for("research.planner").response_language(),
        Some("English")
    );
    assert_eq!(
        daemon.model_for("research.critic").response_language(),
        Some("简体中文")
    );
}

fn install_test_paper_approval(
    store: &V2Store,
    session: NaiveDate,
    now: DateTime<Utc>,
) -> (Artifact, Artifact) {
    let manifest_payload = RuntimeManifest {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        code_revision: "fixture-revision".to_owned(),
        cargo_lock_hash: ContentHash::of_bytes(b"fixture-cargo-lock"),
        config_hash: ContentHash::of_bytes(b"fixture-config"),
        provider_id: "fixture-provider".to_owned(),
        model_id: "fixture-model".to_owned(),
        prompt_hash: ContentHash::of_bytes(b"fixture-prompts"),
        contract_hash: ContentHash::of_bytes(b"fixture-contracts"),
        topology_hash: ContentHash::of_bytes(b"fixture-topology"),
        decision_policy_hash: ContentHash::of_bytes(b"fixture-decision-policy"),
        execution_policy_hash: ContentHash::of_bytes(b"fixture-execution-policy"),
        evaluation_policy_hash: ContentHash::of_bytes(b"fixture-evaluation-policy"),
        market_data_feed: "iex".to_owned(),
        broker_account_id: "fixture-paper-account".to_owned(),
        maximum_notional: MoneyMicros::from_usd_cents(2_000_000),
        allowed_session_start: session,
        allowed_session_end: session,
        expires_at: now + ChronoDuration::hours(8),
        created_at: now,
    };
    let manifest_hash = manifest_payload.manifest_hash().unwrap();
    let manifest = Artifact::new(
        ArtifactKind::RuntimeManifest,
        store.put_json(&manifest_payload).unwrap(),
        "runtime.manifest",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![],
        now,
    )
    .unwrap();
    let mut approval_payload = PaperLaunchApproval {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        operator_identity: "fixture-operator".to_owned(),
        runtime_manifest: ArtifactRef {
            artifact_id: manifest.artifact_id.clone(),
            kind: ArtifactKind::RuntimeManifest,
        },
        runtime_manifest_hash: manifest_hash,
        scope: PaperApprovalScope::Canary,
        reason: "fixture canary".to_owned(),
        approved_at: now,
        expires_at: now + ChronoDuration::hours(8),
        approval_hash: ContentHash::of_bytes(b"pending"),
    };
    approval_payload.approval_hash = approval_payload.unsigned_hash().unwrap();
    let approval = Artifact::new(
        ArtifactKind::PaperLaunchApproval,
        store.put_json(&approval_payload).unwrap(),
        "operator.paper_approval",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.operator".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        vec![approval_payload.runtime_manifest.clone()],
        now,
    )
    .unwrap();
    store
        .write_paper_approval_binding(&manifest, &approval)
        .unwrap();
    (manifest, approval)
}

#[test]
fn evidence_transport_failure_requests_transport_retry() {
    let error = DaemonError::Evidence(EvidenceRuntimeError::Adapter(
        akzio_ingest::runtime::EvidenceAdapterError::Transport("connection reset".to_owned()),
    ));

    assert_eq!(
        retry_cause_for_daemon_error(&error),
        Some(RetryCause::Transport)
    );
}

#[test]
fn evidence_quality_failure_stays_terminal() {
    let error = DaemonError::Evidence(EvidenceRuntimeError::InvalidAcquisition);

    assert_eq!(retry_cause_for_daemon_error(&error), None);
}

#[test]
fn paper_provider_payloads_are_mapped_to_domain_snapshots() {
    let now = Utc::now();
    let session = "2026-08-14".to_owned();
    let account = serde_json::json!({
        "equity": "100000",
        "buying_power": "400000",
        "status": "ACTIVE",
        "trading_blocked": false,
    });
    let account = decode_paper_account(&account, session.clone(), now).unwrap();
    assert_eq!(
        account.schema_version,
        akzio_domain::V2_DOMAIN_SCHEMA_VERSION
    );
    assert!(account.validate().is_ok());

    let quotes = serde_json::json!({
        "quotes": {
            "TQQQ": { "bp": 76.28, "ap": 76.29, "t": "2026-08-14T18:02:07Z" },
            "QQQ": { "bp": 729.38, "ap": 729.41, "t": "2026-08-14T18:02:07Z" },
            "SOXX": { "bp": 544.54, "ap": 544.78, "t": "2026-08-14T18:02:08Z" },
            "SOXL": { "bp": 140.14, "ap": 140.22, "t": "2026-08-14T18:02:07Z" },
        },
    });
    let quotes = decode_paper_quotes(&quotes, session.clone(), now).unwrap();
    assert_eq!(quotes.quotes.len(), 4);
    assert!(quotes.validate().is_ok());

    let clock = serde_json::json!({
        "is_open": true,
        "timestamp": "2026-08-14T18:02:08Z",
        "next_close": "2026-08-14T20:00:00Z",
    });
    let clock = decode_paper_clock(&clock, session, now).unwrap();
    assert!(clock.is_open);
    assert!(clock.validate().is_ok());
}

#[test]
fn paper_session_inputs_include_bounded_directional_bars() {
    let resources = paper_snapshot_resources("2026-08-17");
    assert_eq!(resources.len(), 10);
    for asset in Asset::EXECUTABLE {
        assert!(resources.contains(&format!("bars:{}:1d:2026-07-20:32", asset.symbol())));
    }
}

fn two_phase_responses(output: serde_json::Value) -> Vec<serde_json::Value> {
    let output = serde_json::json!({
        "result": output,
        "deliberation": {
            "selected_path": "fixture path",
            "alternatives": [],
            "alternative_match_ppm": [],
            "uncertainties": [],
            "uncertainty_weight_ppm": [],
            "basis_artifact_ids": [],
            "confidence_ppm": 1000000
        }
    });
    vec![
        serde_json::json!({
            "output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": "fixture research memo"}]
            }]
        }),
        serde_json::json!({
            "output": [{
                "type": "function_call",
                "call_id": "fixture-submit",
                "name": "submit_result",
                "arguments": serde_json::to_string(&output).unwrap()
            }]
        }),
    ]
}

fn planner_with_alpaca_need() -> ModelClient {
    let draft = WorkflowProposalDraft {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalDraftTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "Assess TQQQ fixture evidence".to_owned(),
                depends_on: vec![],
                priority: 80,
                evidence_needs: vec![EvidenceNeed {
                    schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
                    source_family: "alpaca".to_owned(),
                    resource: "bars:TQQQ:1d".to_owned(),
                    max_age_secs: 86_400,
                }],
                research_intents: vec![],
            },
        )]),
        stop_reason: Some("fixture".to_owned()),
    };
    ModelClient::fixture_by_purpose(BTreeMap::from([
        (
            "research.planner".to_owned(),
            two_phase_responses(serde_json::to_value(draft).unwrap()),
        ),
        (
            "research.analyst".to_owned(),
            two_phase_responses(fixture_claim_output()),
        ),
        (
            "research.critic".to_owned(),
            two_phase_responses(fixture_critique_output()),
        ),
    ]))
}

fn paper_proposal() -> WorkflowProposal {
    WorkflowProposal {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "paper-fixture".to_owned(),
        tasks: BTreeMap::from([(
            "synthesizer".to_owned(),
            WorkflowProposalTask {
                recipe_id: TaskRecipeId::new("research.synthesizer").unwrap(),
                objective: "Create a fixture Paper decision proposal".to_owned(),
                depends_on: vec![],
                priority: 100,
                evidence_needs: vec![],
            },
        )]),
        stop_reason: Some("fixture Paper workflow".to_owned()),
    }
}

fn accepted_paper_decision(
    claim: ArtifactRef,
    evidence: Vec<ArtifactRef>,
) -> Vec<serde_json::Value> {
    let forecasts = Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                    serde_json::json!({
                        "asset": asset.symbol(),
                        "horizon": horizon,
                        "positive_return_probability_ppm": if asset == Asset::Qqq { 900000 } else { 500000 },
                        "expected_return_ppm": if asset == Asset::Qqq { 100000 } else { 0 },
                    })
                })
            })
            .collect::<Vec<_>>();
    two_phase_responses(serde_json::json!({
        "summary": "fixture accepted Paper decision",
        "confidence_ppm": 900000,
        "forecasts": forecasts,
        "claims": [claim],
        "critiques": [],
            "evidence": evidence,
        "material_conflicts": [],
        "hard_blockers": [],
        "soft_warnings": []
    }))
}

fn blocked_paper_decision() -> Vec<serde_json::Value> {
    let forecasts = Asset::EXECUTABLE
        .into_iter()
        .flat_map(|asset| {
            ["t1", "t3", "t5"].into_iter().map(move |horizon| {
                serde_json::json!({
                    "asset": asset.symbol(),
                    "horizon": horizon,
                    "positive_return_probability_ppm": 500000,
                    "expected_return_ppm": 0,
                })
            })
        })
        .collect::<Vec<_>>();
    two_phase_responses(serde_json::json!({
        "summary": "fixture blocked Paper decision",
        "confidence_ppm": 0,
        "forecasts": forecasts,
        "claims": [],
        "critiques": [],
        "evidence": [],
        "material_conflicts": [],
        "hard_blockers": ["missing_evidence"],
        "soft_warnings": []
    }))
}

fn scheduler_fixture_model_client() -> ModelClient {
    ModelClient::fixture_by_purpose(BTreeMap::from([(
        "research.synthesizer".to_owned(),
        blocked_paper_decision(),
    )]))
}

fn scheduler_snapshot_need(
    store: &V2Store,
    run_id: &RunId,
    resource: &str,
    now: DateTime<Utc>,
) -> Artifact {
    let need = EvidenceNeed {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        source_family: "alpaca".to_owned(),
        resource: resource.to_owned(),
        max_age_secs: 5,
    };
    Artifact::new(
        ArtifactKind::EvidenceNeed,
        store.put_json(&need).unwrap(),
        "scheduler.paper_snapshot",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.scheduler".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap()
}

#[derive(Clone)]
struct StaticSessionClock(Option<String>);

impl BrokerSessionClock for StaticSessionClock {
    fn open_session_key<'a>(
        &'a self,
    ) -> Pin<
        Box<dyn Future<Output = std::result::Result<Option<String>, SchedulerError>> + Send + 'a>,
    > {
        Box::pin(async move { Ok(self.0.clone()) })
    }

    fn paper_account_id<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<String, SchedulerError>> + Send + 'a>>
    {
        Box::pin(async move { Ok("fixture-paper-account".to_owned()) })
    }
}

#[derive(Clone)]
struct OutcomeBarsAdapter {
    responses: BTreeMap<String, AcquiredEvidence>,
}

impl OutcomeBarsAdapter {
    fn new(baseline: NaiveDate, observed_at: DateTime<Utc>) -> Self {
        let mut responses = BTreeMap::new();
        for asset in Asset::EXECUTABLE {
            let resource = format!(
                "bars:{}:1d:{}:6",
                asset.symbol(),
                baseline.format("%Y-%m-%d")
            );
            let mut bars = Vec::new();
            let mut date = baseline;
            let mut index = 0_u64;
            while bars.len() < 6 {
                date += ChronoDuration::days(1);
                if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
                    continue;
                }
                let price = 100.0 + index as f64;
                bars.push(serde_json::json!({
                    "t": format!("{}T20:00:00Z", date.format("%Y-%m-%d")),
                    "o": price,
                    "h": price + 1.0,
                    "l": price - 1.0,
                    "c": price + 0.5,
                    "v": 1_000,
                    "adjustment": "all",
                }));
                index += 1;
            }
            let normalized = serde_json::json!({"bars": bars});
            let raw = serde_json::to_vec(&normalized).unwrap();
            let source_uri = format!(
                    "https://paper-api.alpaca.markets/v2/stocks/{}/bars?timeframe=1Day&limit=6&adjustment=all&start={}",
                    asset.symbol(),
                    baseline.format("%Y-%m-%d")
                );
            responses.insert(
                resource.clone(),
                AcquiredEvidence {
                    raw,
                    media_type: "application/json".to_owned(),
                    source_uri: source_uri.clone(),
                    observed_at,
                    normalized,
                    provenance: EvidenceProvenance {
                        document_id: Some(resource.clone()),
                        published_at: None,
                        observed_at,
                        revision: Some("fixture-bars-v1".to_owned()),
                        source_uri,
                        dedupe_key: resource,
                        citations: Vec::new(),
                    },
                    quality: EvidenceQuality::default(),
                },
            );
        }
        Self { responses }
    }

    fn with_responses(mut self, responses: BTreeMap<String, AcquiredEvidence>) -> Self {
        self.responses.extend(responses);
        self
    }
}

impl AsyncEvidenceAdapter for OutcomeBarsAdapter {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::Alpaca
    }

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, std::result::Result<AcquiredEvidence, EvidenceAdapterError>> {
        let result = if request.source != EvidenceSource::Alpaca {
            Err(EvidenceAdapterError::SourceMismatch)
        } else {
            self.responses
                .get(&request.resource)
                .cloned()
                .ok_or_else(|| EvidenceAdapterError::MissingFixture(request.resource.clone()))
        };
        Box::pin(async move { result })
    }
}

#[derive(Default)]
struct FakePaperBroker {
    submissions: AtomicUsize,
}

impl CommittedPaperBroker for FakePaperBroker {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a akzio_domain::PaperCommitment,
        plan: &'a akzio_execution::ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
    {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        let execution = PaperExecution {
            plan_hash: plan.plan_hash.clone(),
            orders: plan
                .orders
                .iter()
                .map(|order| PaperOrderReceipt {
                    client_order_id: commitment.client_order_ids[&order.asset].clone(),
                    broker_order_id: format!("fixture-{}", order.asset.symbol()),
                    symbol: order.asset.symbol().to_owned(),
                    status: "filled".to_owned(),
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: 1_000_000,
                    remaining_quantity_micros: 0,
                    average_fill_price: Some(order.limit_price),
                    broker_updated_at: Utc::now(),
                    reason: None,
                    reused: false,
                    reprice_count: 0,
                })
                .collect(),
        };
        Box::pin(async move { Ok(execution) })
    }

    fn replace_commitment_once<'a>(
        &'a self,
        _commitment: &'a akzio_domain::PaperCommitment,
        _reprice: &'a akzio_domain::PaperReprice,
        _replacement: &'a akzio_execution::OrderIntent,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperOrderReceipt>> + Send + 'a>>
    {
        Box::pin(async {
            Err(PaperError::InvalidCommitment(
                "fixture has no reprice".to_owned(),
            ))
        })
    }

    fn reconcile_commitment<'a>(
        &'a self,
        _commitment: &'a akzio_domain::PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
    {
        let execution = execution.clone();
        Box::pin(async move { Ok(execution) })
    }
}

#[tokio::test]
async fn planner_task_runs_agent_runtime_and_commits_graph_patch() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

    assert!(daemon.run_one("fixture").await.unwrap());

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    assert!(snapshot
        .revision
        .graph
        .nodes
        .iter()
        .any(|node| node.recipe_id.as_str() == "research.analyst"));
    assert!(daemon
        .store()
        .events_after(&run_id, 0, 64)
        .unwrap()
        .iter()
        .any(|event| event.event_type == "task.succeeded"));
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn planner_accepts_a_real_debug_shape_with_one_analyst_task() {
    let directory = tempdir().unwrap();
    let planner = serde_json::json!({
        "schema_version": akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
    "topology_id": "active",
        "tasks": {
            "research_analyst": {
                "depends_on": [],
                "evidence_needs": [],
                "objective": "Identify bounded evidence needs.",
                "priority": 1,
                "recipe_id": "research.analyst",
                "research_intents": [],
            },
        },
        "stop_reason": "proposal_complete",
    });
    let model = ModelClient::fixture_by_purpose(BTreeMap::from([(
        "research.planner".to_owned(),
        two_phase_responses(planner),
    )]));
    let daemon = Daemon::with_model(config(directory.path().to_path_buf()), model).unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let task = daemon
        .store()
        .claim_next_task("fixture", Utc::now(), ChronoDuration::seconds(30))
        .unwrap()
        .expect("planner task");
    let result = daemon.execute_task_inner(&task, Utc::now()).await;
    println!("planner result: {result:?}");
    assert!(result.is_ok(), "planner result: {result:?}");
    daemon.store().workflow_snapshot(&run_id).unwrap();
}

#[tokio::test]
async fn debug_evidence_gate_uses_controlled_fixture_when_planner_has_no_needs() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    daemon.submit_default(RunPurpose::Debug).unwrap();
    assert!(daemon.run_one("fixture").await.unwrap());
    let task = daemon
        .store()
        .claim_next_task("fixture", Utc::now(), ChronoDuration::seconds(30))
        .unwrap()
        .expect("evidence gate task");
    let artifacts = daemon.acquire_evidence(&task, Utc::now()).await.unwrap();
    assert!(artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence));
}

#[tokio::test]
async fn invalid_agent_output_requests_task_retry() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        ModelClient::fixture_sequence({
            let mut responses = two_phase_responses(serde_json::json!({}));
            responses.push(responses[1].clone());
            responses
        }),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let task = daemon
        .store()
        .claim_next_task("invalid-output", Utc::now(), ChronoDuration::seconds(30))
        .unwrap()
        .expect("planner task");

    assert_eq!(task.node.recipe_id.as_str(), "research.planner");
    assert_eq!(
        daemon.execute_task(task).await,
        TaskCompletion::Retry(RetryCause::InvalidOutput)
    );
    assert!(daemon
        .store()
        .events_after(&run_id, 0, 64)
        .unwrap()
        .iter()
        .any(|event| event.event_type == "agent.turn_completed"));
    assert!(!daemon
        .store()
        .events_after(&run_id, 0, 64)
        .unwrap()
        .iter()
        .any(|event| event.event_type == "task.failed"));
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn evidence_gate_resolves_need_with_fixture_adapter_and_keeps_provenance() {
    let directory = tempdir().unwrap();
    let observed_at = Utc::now();
    let fixture_evidence = BTreeMap::from([(
        EvidenceSource::Alpaca,
        BTreeMap::from([(
            "bars:TQQQ:1d".to_owned(),
            AcquiredEvidence {
                raw: br#"{\"bars\":[{\"close\":100}]}"#.to_vec(),
                media_type: "application/json".to_owned(),
                source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                observed_at,
                normalized: serde_json::json!({"close": 100}),
                provenance: EvidenceProvenance {
                    document_id: Some("fixture-bars".to_owned()),
                    published_at: None,
                    observed_at,
                    revision: Some("1".to_owned()),
                    source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                    dedupe_key: "fixture:alpaca:bars:TQQQ:1d".to_owned(),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        )]),
    )]);
    let daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        planner_with_alpaca_need(),
        fixture_evidence,
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

    for _ in 0..16 {
        if !daemon.run_one("fixture").await.unwrap() {
            break;
        }
    }
    let artifacts = daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .into_iter()
        .filter_map(|event| event.artifact_id)
        .filter_map(|artifact_id| daemon.store().artifact(&artifact_id).ok())
        .collect::<Vec<_>>();
    let normalized = artifacts
        .iter()
        .find(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence)
        .expect("evidence gate committed normalized fixture evidence");
    let payload: NormalizedEvidencePayload =
        serde_json::from_slice(&daemon.store().read_blob(&normalized.blob).unwrap()).unwrap();
    assert_eq!(payload.resource, "bars:TQQQ:1d");
    assert_eq!(payload.need.kind, ArtifactKind::EvidenceNeed);
    assert!(artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::Claim));

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    let task_status = |recipe_id: &str| {
        snapshot
            .tasks
            .iter()
            .find(|task| task.node.recipe_id.as_str() == recipe_id)
            .map(|task| task.status)
    };
    assert_eq!(task_status("research.analyst"), Some(TaskStatus::Succeeded));
    assert_eq!(task_status("gate.decision"), Some(TaskStatus::Failed));
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn scheduler_owned_paper_run_forwards_no_order_and_schedules_outcome() {
    let directory = tempdir().unwrap();
    let observed_at = Utc::now();
    let fixture_evidence = BTreeMap::from([(
        EvidenceSource::Alpaca,
        BTreeMap::from([(
            "bars:TQQQ:1d".to_owned(),
            AcquiredEvidence {
                raw: br#"{\"bars\":[{\"close\":100}]}"#.to_vec(),
                media_type: "application/json".to_owned(),
                source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                observed_at,
                normalized: serde_json::json!({"close": 100}),
                provenance: EvidenceProvenance {
                    document_id: Some("fixture-bars".to_owned()),
                    published_at: None,
                    observed_at,
                    revision: Some("1".to_owned()),
                    source_uri: "fixture://alpaca/bars/TQQQ/1d".to_owned(),
                    dedupe_key: "fixture:alpaca:bars:TQQQ:1d".to_owned(),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        )]),
    )]);
    let daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        scheduler_fixture_model_client(),
        fixture_evidence,
    )
    .unwrap();
    let now = Utc::now();
    let paper_run_id = RunId::new();
    let need = EvidenceNeed {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        source_family: "alpaca".to_owned(),
        resource: "bars:TQQQ:1d".to_owned(),
        max_age_secs: 86_400,
    };
    let need_artifact = Artifact::new(
        ArtifactKind::EvidenceNeed,
        daemon.store().put_json(&need).unwrap(),
        "scheduler.fixture",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.scheduler".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(ArtifactOrigin {
            run_id: Some(paper_run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    let mut proposal = paper_proposal();
    proposal
        .tasks
        .get_mut("synthesizer")
        .unwrap()
        .evidence_needs = vec![ArtifactRef {
        artifact_id: need_artifact.artifact_id.clone(),
        kind: ArtifactKind::EvidenceNeed,
    }];
    let session_key = now.date_naive().to_string();
    let slot = daemon
        .reserve_paper_session_with_inputs_for_run(
            paper_run_id,
            &session_key,
            &proposal,
            &[need_artifact],
            now,
        )
        .unwrap();
    assert!(slot.newly_reserved);
    let run_id = slot.slot.workflow.run.run_id.clone();

    for _ in 0..32 {
        if !daemon.run_one("paper-fixture").await.unwrap() {
            break;
        }
    }

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    assert_eq!(daemon.workflow.replay_run(&run_id).unwrap(), snapshot);
    assert!(
        snapshot
            .tasks
            .iter()
            .all(|task| task.status == TaskStatus::Succeeded),
        "statuses: {:?}",
        snapshot
            .tasks
            .iter()
            .map(|task| format!("{}={:?}", task.node.recipe_id, task.status))
            .collect::<Vec<_>>()
    );
    let schedule = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
        .unwrap()
        .expect("Paper terminal chain must schedule future outcome");
    let payload: OutcomeSchedule =
        serde_json::from_slice(&daemon.store().read_blob(&schedule.blob).unwrap()).unwrap();
    assert_eq!(payload.baseline_trading_day, now.date_naive());
    assert!(matches!(
        payload.execution,
        OutcomeExecutionLineage::NoOrder { .. }
    ));
    assert!(daemon.store().session_slot(&session_key).unwrap().is_some());
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn paper_fixture_snapshots_reach_accepted_commit_reconcile_and_outcome_schedule() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let account = serde_json::json!({
        "status": "ACTIVE",
        "equity": "10000",
        "buying_power": "10000",
        "trading_blocked": false
    });
    let quotes = QuoteSnapshot {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        broker_session: session_key.clone(),
        observed_at: now,
        quotes: Asset::EXECUTABLE
            .into_iter()
            .map(|asset| {
                (
                    asset,
                    Quote {
                        bid: MoneyMicros::from_usd_cents(10_000),
                        ask: MoneyMicros::from_usd_cents(10_010),
                        observed_at: now,
                    },
                )
            })
            .collect(),
    };
    let clock = MarketClockSnapshot {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        broker_session: session_key.clone(),
        is_open: true,
        observed_at: now,
    };
    let mut evidence = [
        (
            PAPER_ACCOUNT_RESOURCE,
            serde_json::to_value(&account).unwrap(),
        ),
        (
            PAPER_QUOTES_RESOURCE,
            serde_json::to_value(&quotes).unwrap(),
        ),
        (PAPER_CLOCK_RESOURCE, serde_json::to_value(&clock).unwrap()),
    ]
    .into_iter()
    .map(|(resource, normalized)| {
        (
            resource.to_owned(),
            AcquiredEvidence {
                raw: serde_json::to_vec(&normalized).unwrap(),
                media_type: "application/json".to_owned(),
                source_uri: format!("fixture://alpaca/{resource}"),
                observed_at: now,
                normalized,
                provenance: EvidenceProvenance {
                    document_id: Some(format!("fixture-{resource}")),
                    published_at: None,
                    observed_at: now,
                    revision: Some("1".to_owned()),
                    source_uri: format!("fixture://alpaca/{resource}"),
                    dedupe_key: format!("fixture:alpaca:{resource}"),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        )
    })
    .collect::<BTreeMap<_, _>>();
    let fills_resource = format!("paper.fills:{session_key}");
    for resource in [
        PAPER_POSITIONS_RESOURCE.to_owned(),
        PAPER_OPEN_ORDERS_RESOURCE.to_owned(),
        fills_resource.clone(),
    ] {
        let normalized = serde_json::json!([]);
        evidence.insert(
            resource.clone(),
            AcquiredEvidence {
                raw: serde_json::to_vec(&normalized).unwrap(),
                media_type: "application/json".to_owned(),
                source_uri: format!("fixture://alpaca/{resource}"),
                observed_at: now,
                normalized,
                provenance: EvidenceProvenance {
                    document_id: Some(format!("fixture-{resource}")),
                    published_at: None,
                    observed_at: now,
                    revision: Some("1".to_owned()),
                    source_uri: format!("fixture://alpaca/{resource}"),
                    dedupe_key: format!("fixture:alpaca:{resource}"),
                    citations: vec![],
                },
                quality: EvidenceQuality::default(),
            },
        );
    }
    let execution_evidence = evidence.clone();
    let responses = Arc::new(Mutex::new(VecDeque::from(two_phase_responses(
        fixture_claim_output(),
    ))));
    let broker = Arc::new(FakePaperBroker::default());
    let daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        ModelClient::FixtureSequence(responses.clone()),
        BTreeMap::from([(EvidenceSource::Alpaca, evidence)]),
    )
    .unwrap();
    let mut daemon = daemon.with_paper_broker(broker.clone());
    daemon.production_evidence = Arc::new(BTreeMap::from([(
        EvidenceSource::Alpaca,
        Arc::new(OutcomeBarsAdapter::new(now.date_naive(), now).with_responses(execution_evidence))
            as Arc<dyn AsyncEvidenceAdapter>,
    )]));
    let paper_run_id = RunId::new();
    let setup_artifacts = [
        scheduler_snapshot_need(daemon.store(), &paper_run_id, PAPER_ACCOUNT_RESOURCE, now),
        scheduler_snapshot_need(daemon.store(), &paper_run_id, PAPER_POSITIONS_RESOURCE, now),
        scheduler_snapshot_need(
            daemon.store(),
            &paper_run_id,
            PAPER_OPEN_ORDERS_RESOURCE,
            now,
        ),
        scheduler_snapshot_need(daemon.store(), &paper_run_id, &fills_resource, now),
        scheduler_snapshot_need(daemon.store(), &paper_run_id, PAPER_QUOTES_RESOURCE, now),
        scheduler_snapshot_need(daemon.store(), &paper_run_id, PAPER_CLOCK_RESOURCE, now),
    ];
    let snapshot_refs = setup_artifacts
        .iter()
        .map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        })
        .collect::<Vec<_>>();
    let mut proposal = paper_proposal();
    proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Assess governed Paper snapshots".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: snapshot_refs,
        },
    );
    proposal.tasks.get_mut("synthesizer").unwrap().depends_on = vec!["analyst".to_owned()];
    let (manifest, approval) = install_test_paper_approval(
        daemon.store(),
        NaiveDate::parse_from_str(&session_key, "%Y-%m-%d").unwrap(),
        now,
    );
    let lease = daemon.paper.scheduler.active_lease(now).unwrap();
    let slot = daemon
        .workflow
        .reserve_paper_session_with_inputs_for_run_approved(
            &lease,
            paper_run_id,
            &session_key,
            &proposal,
            &setup_artifacts,
            &manifest,
            &approval,
            now,
        )
        .unwrap();
    let run_id = slot.slot.workflow.run.run_id.clone();

    let evidence_task = daemon
        .store()
        .claim_next_task("accepted-paper-evidence", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    let evidence_outputs = daemon
        .acquire_evidence(&evidence_task, now)
        .await
        .expect("fixture snapshots must be valid governed evidence");
    daemon
        .store()
        .commit_attempt(
            &evidence_task.permit,
            &evidence_outputs,
            TaskStatus::Succeeded,
            now,
        )
        .unwrap();
    daemon.outcome_scheduling_runtime =
        OutcomeSchedulingRuntime::new(daemon.store.clone()).with_worker_enabled(true);

    assert!(daemon.run_one("accepted-paper-analyst").await.unwrap());
    let analyst_task = daemon
        .store()
        .workflow_snapshot(&run_id)
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| task.node.recipe_id.as_str() == "research.analyst")
        .expect("fixture workflow must contain analyst")
        .node
        .task_id;
    let claim = daemon
        .store()
        .committed_task_outputs(&run_id, &analyst_task)
        .unwrap()
        .into_iter()
        .find(|artifact| artifact.kind == ArtifactKind::Claim)
        .expect("analyst must emit a Claim");
    let claim_payload: akzio_domain::ResearchClaim =
        serde_json::from_slice(&daemon.store().read_blob(&claim.blob).unwrap()).unwrap();
    responses.lock().unwrap().extend(accepted_paper_decision(
        ArtifactRef {
            artifact_id: claim.artifact_id,
            kind: ArtifactKind::Claim,
        },
        claim_payload.source_refs(),
    ));
    assert!(daemon.run_one("accepted-paper-synthesizer").await.unwrap());
    let synthesizer_task = daemon
        .store()
        .workflow_snapshot(&run_id)
        .unwrap()
        .tasks
        .into_iter()
        .find(|task| task.node.recipe_id.as_str() == "research.synthesizer")
        .unwrap()
        .node
        .task_id;
    let synthesizer_manifest = daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .into_iter()
        .find(|event| {
            event.task_id.as_ref() == Some(&synthesizer_task)
                && event.event_type == LifecycleEventType::ContextManifestCreated.as_str()
        })
        .and_then(|event| event.artifact_id)
        .and_then(|artifact_id| daemon.store().artifact(&artifact_id).ok())
        .unwrap();
    let synthesizer_manifest: ContextManifestPayload = serde_json::from_slice(
        &daemon
            .store()
            .read_blob(&synthesizer_manifest.blob)
            .unwrap(),
    )
    .unwrap();
    assert!(synthesizer_manifest
        .selections
        .iter()
        .any(|selection| selection.artifact.kind == ArtifactKind::NormalizedEvidence));

    for _ in 0..5 {
        assert!(daemon.run_one("accepted-paper-gates").await.unwrap());
    }
    for _ in 0..32 {
        if !daemon.run_one("accepted-paper-fixture").await.unwrap() {
            break;
        }
    }

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    assert!(
        snapshot
            .tasks
            .iter()
            .all(|task| task.status == TaskStatus::Succeeded),
        "statuses: {:?}",
        snapshot
            .tasks
            .iter()
            .map(|task| format!("{}={:?}", task.node.recipe_id, task.status))
            .collect::<Vec<_>>()
    );
    let outcome_task = snapshot
        .tasks
        .iter()
        .find(|task| {
            task.node.recipe_id.as_str() == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID
        })
        .expect("Paper run must retain an outcome worker task");
    let outcome_contract_hash = outcome_task
        .node
        .contract_hash
        .as_ref()
        .expect("Paper outcome worker must retain its contract hash");
    let outcome_contract = daemon
        .agents
        .catalogue()
        .get(outcome_contract_hash)
        .unwrap();
    assert_eq!(outcome_task.node.budget, outcome_contract.contract.budget);
    assert_eq!(outcome_task.node.retry, outcome_contract.contract.retry);
    assert_eq!(
        outcome_task.node.on_failure,
        outcome_contract.contract.on_failure
    );
    assert!(outcome_task
        .node
        .input_artifacts
        .iter()
        .any(|reference| reference.kind == ArtifactKind::DeliberationNote));
    let outcome_manifest_artifact = daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .into_iter()
        .filter(|event| {
            event.task_id.as_ref() == Some(&outcome_task.node.task_id)
                && event.event_type == LifecycleEventType::ContextManifestCreated.as_str()
        })
        .filter_map(|event| event.artifact_id)
        .next_back()
        .and_then(|artifact_id| daemon.store().artifact(&artifact_id).ok())
        .expect("outcome worker must assemble a governed context manifest");
    let outcome_manifest_payload: ContextManifestPayload = serde_json::from_slice(
        &daemon
            .store()
            .read_blob(&outcome_manifest_artifact.blob)
            .unwrap(),
    )
    .unwrap();
    assert!(outcome_manifest_payload
        .selections
        .iter()
        .any(|selection| selection.artifact.kind == ArtifactKind::DeliberationNote));
    assert_eq!(broker.submissions.load(Ordering::SeqCst), 1);
    let schedule = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::OutcomeSchedule)
        .unwrap()
        .expect("accepted fixture Paper chain must schedule an outcome");
    let payload: OutcomeSchedule =
        serde_json::from_slice(&daemon.store().read_blob(&schedule.blob).unwrap()).unwrap();
    assert!(matches!(
        payload.execution,
        OutcomeExecutionLineage::ReconciledPaper { .. }
    ));
    assert!(daemon
        .store()
        .artifacts_referencing(&schedule.artifact_id, None)
        .unwrap()
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::Outcome));
    let outcome = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::Outcome)
        .unwrap()
        .expect("outcome worker must seal a Paper Outcome");
    let outcome: Outcome =
        serde_json::from_slice(&daemon.store().read_blob(&outcome.blob).unwrap()).unwrap();
    assert_eq!(outcome.windows.len(), 3);
    assert!(daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::Evaluation)
        .unwrap()
        .is_none());
    let final_retrospective = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::Retrospective)
        .unwrap()
        .expect("model-unavailable Paper run must retain a final retrospective");
    let final_retrospective: Retrospective =
        serde_json::from_slice(&daemon.store().read_blob(&final_retrospective.blob).unwrap())
            .unwrap();
    assert_eq!(
        final_retrospective.status,
        RetrospectiveStatus::ModelUnavailable
    );
    let retrospectives = daemon.store().retrospectives(&run_id).unwrap();
    let mut horizons = retrospectives
        .iter()
        .map(|artifact| {
            let payload: Retrospective =
                serde_json::from_slice(&daemon.store().read_blob(&artifact.blob).unwrap()).unwrap();
            (payload.horizon, artifact.lifecycle)
        })
        .collect::<Vec<_>>();
    horizons.sort_by_key(|(horizon, _)| *horizon);
    assert_eq!(horizons.len(), 3);
    assert_eq!(horizons[0].0, OutcomeHorizon::T1);
    assert_eq!(horizons[1].0, OutcomeHorizon::T3);
    assert_eq!(horizons[2].0, OutcomeHorizon::T5);
    assert_eq!(horizons[0].1, ArtifactLifecycle::RunScoped);
    assert_eq!(horizons[1].1, ArtifactLifecycle::RunScoped);
    assert_eq!(horizons[2].1, ArtifactLifecycle::Canonical);
    assert!(daemon
        .store()
        .events_after(&run_id, 0, 256)
        .unwrap()
        .iter()
        .any(|event| event.event_type == "execution.committed"));
    let observer = daemon.observer_snapshot().await.unwrap();
    let observer_outcome = observer.outcome.data.expect("sealed Outcome is observable");
    assert!(observer_outcome
        .horizons
        .iter()
        .all(|horizon| horizon.window.is_some()));
    let observer_learning = observer
        .learning
        .data
        .expect("durable learning artifacts are observable");
    assert!(observer_learning
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::Outcome));
    assert!(observer_learning
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == ArtifactKind::Retrospective));
    daemon.store().verify_integrity().unwrap();
}

#[test]
fn scheduler_fences_stale_daemon_and_reuses_frozen_session_workflow() {
    let directory = tempdir().unwrap();
    let first = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let second = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let first_slot = first
        .reserve_paper_session(&session_key, &paper_proposal(), now)
        .unwrap();
    assert!(matches!(
        second.reserve_paper_session(&session_key, &paper_proposal(), now),
        Err(DaemonError::Scheduler(SchedulerError::NotLeader))
    ));

    let recovered = second
        .reserve_paper_session(&session_key, &paper_proposal(), now + Duration::seconds(31))
        .unwrap();
    assert!(!recovered.newly_reserved);
    assert_eq!(
        recovered.slot.workflow.run.run_id,
        first_slot.slot.workflow.run.run_id
    );
    assert!(matches!(
        first.reserve_paper_session(&session_key, &paper_proposal(), now + Duration::seconds(31),),
        Err(DaemonError::Scheduler(SchedulerError::NotLeader))
    ));
    first.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn auto_paper_requires_an_injected_scheduler_loop() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let (_shutdown, receiver) = watch::channel(false);

    assert!(matches!(
        daemon.serve_workers(receiver).await,
        Err(DaemonError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn paper_scheduler_does_not_reserve_when_clock_is_closed() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let session_key = Utc::now().date_naive().to_string();
    let clock = StaticSessionClock(None);
    let source = StaticPaperWorkflowSource::new(paper_proposal());

    let reservation = daemon
        .paper
        .scheduler
        .tick(&clock, &source, Utc::now())
        .await
        .unwrap();

    assert!(reservation.is_none());
    assert!(daemon.store().session_slot(&session_key).unwrap().is_none());
}

#[tokio::test]
async fn auto_paper_supervisor_reserves_an_open_broker_session() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let session_key = Utc::now().date_naive().to_string();
    install_test_paper_approval(
        daemon.store(),
        NaiveDate::parse_from_str(&session_key, "%Y-%m-%d").unwrap(),
        Utc::now(),
    );
    let clock = Arc::new(StaticSessionClock(Some(session_key.clone())));
    let source = Arc::new(StaticPaperWorkflowSource::new(paper_proposal()));
    let (shutdown, receiver) = watch::channel(false);
    let supervised = daemon.clone();
    let task = tokio::spawn(async move {
        supervised
            .serve_with_paper_scheduler(
                clock.as_ref(),
                source.as_ref(),
                std::time::Duration::from_millis(1),
                receiver,
            )
            .await
    });

    let mut reserved = None;
    for _ in 0..50 {
        reserved = daemon.store().session_slot(&session_key).unwrap();
        if reserved.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    shutdown.send(true).unwrap();
    assert!(task.await.unwrap().is_ok());
    assert!(reserved.is_some());
    let run_id = reserved.unwrap().workflow.run.run_id;
    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    let snapshot_resources = snapshot
        .tasks
        .iter()
        .flat_map(|task| task.node.input_artifacts.iter())
        .filter_map(|reference| {
            let artifact = daemon.store().artifact(&reference.artifact_id).unwrap();
            (artifact.producer == "scheduler.paper_snapshot").then(|| {
                let need: EvidenceNeed =
                    serde_json::from_slice(&daemon.store().read_blob(&artifact.blob).unwrap())
                        .unwrap();
                assert_eq!(
                    artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref()),
                    Some(&run_id)
                );
                need.resource
            })
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        snapshot_resources,
        paper_snapshot_resources(&session_key)
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn auto_paper_requires_a_durable_workflow_proposal() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    let session_key = Utc::now().date_naive().to_string();
    let clock = StaticSessionClock(Some(session_key.clone()));
    let source = StorePaperWorkflowSource::new(daemon.store().clone());

    assert!(matches!(
        source.proposal("preflight"),
        Err(SchedulerError::WorkflowUnavailable)
    ));
    assert!(daemon
        .paper
        .scheduler
        .tick(&clock, &source, Utc::now())
        .await
        .unwrap()
        .is_none());
    assert!(daemon.store().session_slot(&session_key).unwrap().is_none());
    daemon.store().verify_integrity().unwrap();
}

#[test]
fn auto_paper_source_bootstraps_the_first_approved_proposal() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();

    let proposal = daemon
        .paper_workflow_source()
        .proposal("preflight")
        .unwrap();

    assert_eq!(proposal.topology_id, "active");
    assert_eq!(proposal.tasks.len(), 2);
    assert_eq!(
        proposal.tasks["analyst"].recipe_id.as_str(),
        "research.analyst"
    );
    assert_eq!(
        proposal.tasks["synthesizer"].recipe_id.as_str(),
        "research.synthesizer"
    );
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn auto_paper_source_ignores_a_newer_debug_proposal() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();
    daemon.submit_default(RunPurpose::Debug).unwrap();
    assert!(daemon.run_one("debug-proposal-fixture").await.unwrap());

    let proposal = daemon
        .paper_workflow_source()
        .proposal("preflight")
        .unwrap();

    assert_eq!(proposal.topology_id, "active");
    assert_eq!(proposal.tasks.len(), 2);
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn paper_scheduler_rejects_cross_run_run_scoped_evidence_needs() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let old_run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let now = Utc::now();
    let claimed = daemon
        .store()
        .claim_next_task("cross-run-fixture", now, Duration::seconds(30))
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run_id, old_run_id);
    let need = EvidenceNeed {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        source_family: "alpaca".to_owned(),
        resource: "bars:TQQQ:1d".to_owned(),
        max_age_secs: 86_400,
    };
    let need_artifact = Artifact::new(
        ArtifactKind::EvidenceNeed,
        daemon.store().put_json(&need).unwrap(),
        "runtime.planner.evidence_need",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.workflow.planner".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: claimed.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(old_run_id.clone()),
            task_id: Some(claimed.permit.task_id.clone()),
            attempt_id: Some(claimed.permit.attempt_id.clone()),
            contract_hash: claimed.permit.contract_hash.clone(),
        }),
        Vec::new(),
        now,
    )
    .unwrap();
    daemon
        .store()
        .write_task_artifact(
            &claimed.permit,
            &need_artifact,
            LifecycleEventType::PlannerEvidenceNeedCreated,
            now,
        )
        .unwrap();

    let mut proposal = paper_proposal();
    proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Assess stale fixture evidence".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: vec![ArtifactRef {
                artifact_id: need_artifact.artifact_id,
                kind: ArtifactKind::EvidenceNeed,
            }],
        },
    );
    proposal.tasks.get_mut("synthesizer").unwrap().depends_on = vec!["analyst".to_owned()];
    let session_key = now.date_naive().to_string();
    install_test_paper_approval(daemon.store(), now.date_naive(), now);
    let clock = StaticSessionClock(Some(session_key.clone()));
    let source = StaticPaperWorkflowSource::new(proposal);
    assert!(matches!(
        daemon.paper.scheduler.tick(&clock, &source, now).await,
        Err(SchedulerError::WorkflowUnavailable)
    ));
    assert!(daemon.store().session_slot(&session_key).unwrap().is_none());
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn paper_scheduler_does_not_carry_scheduler_snapshots_into_new_run() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = Utc::now();
    let old_session = now.date_naive().to_string();
    let old_run_id = RunId::new();
    let old_snapshot =
        scheduler_snapshot_need(daemon.store(), &old_run_id, PAPER_ACCOUNT_RESOURCE, now);
    daemon
        .reserve_paper_session_with_inputs_for_run(
            old_run_id.clone(),
            &old_session,
            &paper_proposal(),
            std::slice::from_ref(&old_snapshot),
            now,
        )
        .unwrap();

    let old_snapshot_ref = ArtifactRef {
        artifact_id: old_snapshot.artifact_id.clone(),
        kind: ArtifactKind::EvidenceNeed,
    };
    let mut new_proposal = paper_proposal();
    new_proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Refresh scheduler-owned Paper snapshots".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: vec![old_snapshot_ref.clone()],
        },
    );
    new_proposal
        .tasks
        .get_mut("synthesizer")
        .unwrap()
        .depends_on = vec!["analyst".to_owned()];

    let new_session = (now.date_naive() + chrono::Days::new(1)).to_string();
    install_test_paper_approval(
        daemon.store(),
        NaiveDate::parse_from_str(&new_session, "%Y-%m-%d").unwrap(),
        now,
    );
    let clock = StaticSessionClock(Some(new_session));
    let source = StaticPaperWorkflowSource::new(new_proposal);
    let reservation = daemon
        .paper
        .scheduler
        .tick(&clock, &source, now + Duration::seconds(1))
        .await
        .unwrap()
        .expect("new Paper session must be reserved");
    let new_run_id = reservation.slot.workflow.run.run_id;
    assert_ne!(new_run_id, old_run_id);

    let snapshot = daemon.store().workflow_snapshot(&new_run_id).unwrap();
    let snapshot_refs = snapshot
        .tasks
        .iter()
        .flat_map(|task| task.node.input_artifacts.iter())
        .filter(|reference| reference.kind == ArtifactKind::EvidenceNeed)
        .cloned()
        .collect::<Vec<_>>();
    assert!(!snapshot_refs.contains(&old_snapshot_ref));
    assert!(snapshot_refs.iter().all(|reference| {
        daemon
            .store()
            .artifact(&reference.artifact_id)
            .unwrap()
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            == Some(&new_run_id)
    }));
    daemon.store().verify_integrity().unwrap();
}

#[test]
fn cancellation_and_freeze_are_durable_store_owned_transitions() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    assert_eq!(
        daemon
            .request_cancel(&run_id, "fixture cancellation request")
            .unwrap(),
        1
    );
    assert!(daemon.store().run_cancel_requested(&run_id).unwrap());

    assert!(
        daemon
            .set_freeze(true, "fixture freeze".to_owned())
            .unwrap()
            .frozen
    );
    let reopened = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    assert!(reopened.health().unwrap().frozen);
    assert!(
        !reopened
            .set_freeze(false, "fixture operator unfreeze".to_owned())
            .unwrap()
            .frozen
    );
    reopened.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn http_control_rejects_non_loopback_bind() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let (_shutdown, receiver) = watch::channel(false);
    assert!(matches!(
        daemon
            .serve_http("0.0.0.0:0".parse().unwrap(), receiver)
            .await,
        Err(DaemonError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn readiness_requires_auth_and_injected_paper_broker() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(daemon_config, fixture_model_client()).unwrap();

    let unauthorized = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let not_ready = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);

    let daemon = daemon.with_paper_broker(Arc::new(FakePaperBroker::default()));
    let ready = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::OK);
}

#[tokio::test]
async fn readiness_keeps_historical_failures_observable() {
    let directory = tempdir().unwrap();
    let mut daemon_config = config(directory.path().to_path_buf());
    daemon_config.auto_paper = true;
    let daemon = Daemon::with_model(
        daemon_config,
        ModelClient::fixture_sequence({
            let mut responses = two_phase_responses(serde_json::json!({}));
            responses.push(responses[1].clone());
            responses
        }),
    )
    .unwrap();
    daemon.submit_default(RunPurpose::Debug).unwrap();
    assert!(daemon.run_one("failed-run-fixture").await.unwrap());
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(daemon.run_one("failed-run-fixture").await.unwrap());
    let daemon = daemon.with_paper_broker(Arc::new(FakePaperBroker::default()));

    let health = daemon.health().unwrap();
    assert!(health
        .alerts
        .iter()
        .any(|alert| matches!(alert.severity, AlertSeverity::Critical)));
    assert!(daemon.ready().is_ok());
}

#[tokio::test]
async fn http_control_auth_cancel_retry_and_freeze_are_governed() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();

    let unauthorized = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorized.status(), StatusCode::OK);

    let submitted = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runs")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"purpose":"debug"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(submitted.status(), StatusCode::OK);
    let run_id = serde_json::from_slice::<RunSubmissionResponse>(
        &to_bytes(submitted.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap()
    .run_id;

    let cancelled = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/runs/{run_id}/cancel"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::OK);
    assert!(daemon.store().run_cancel_requested(&run_id).unwrap());

    let retried = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/runs/{run_id}/retry"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(retried.status(), StatusCode::OK);
    let retry = serde_json::from_slice::<RunRetryResponse>(
        &to_bytes(retried.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(retry.source_run_id, run_id);
    let retry_run_id = retry.run_id;
    assert_eq!(
        daemon.store().run_purpose(&retry_run_id).unwrap(),
        RunPurpose::Debug
    );

    let frozen = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/control/freeze")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"reason":"fixture freeze"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(frozen.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(frozen.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap()["frozen"]
            .as_bool(),
        Some(true)
    );

    let unfrozen = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/control/unfreeze")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"reason":"fixture unfreeze"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unfrozen.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(unfrozen.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap()["frozen"]
            .as_bool(),
        Some(false)
    );
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn observer_snapshot_uses_a_separate_read_only_credential() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

    let control_token = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/v1/observer/snapshot")
                .header("x-akzio-observer-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(control_token.status(), StatusCode::UNAUTHORIZED);

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/v1/observer/snapshot")
                .header("x-akzio-observer-token", "fixture-observer-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(body["schema_version"].as_u64(), Some(2));
    assert!(body["core"]["readiness_ppm"].as_u64().is_some());
    assert_eq!(body["recent_runs"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["run_summaries"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["outcome"]["status"], "pending");
    assert_eq!(body["recent_runs"][0]["run"]["run_id"], run_id.0);
    assert_eq!(body["current_run"]["workflow"]["run"]["run_id"], run_id.0);
    assert_eq!(body["portfolio"]["status"], "unavailable");
    assert_eq!(body["core"]["approval"]["status"], "missing");
    assert!(body["event_cursor"].as_i64().unwrap() > 0);

    let run_detail = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/observer/runs/{run_id}"))
                .header("x-akzio-observer-token", "fixture-observer-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(run_detail.status(), StatusCode::OK);

    let observer_cannot_use_control_api = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .header("x-akzio-observer-token", "fixture-observer-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        observer_cannot_use_control_api.status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn http_sse_resumes_from_the_requested_cursor() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    daemon
        .request_cancel(&run_id, "fixture cancellation request")
        .unwrap();
    let events = daemon.store().events_after(&run_id, 0, 16).unwrap();
    assert!(events.len() >= 2);
    let after = events[0].cursor;
    let expected = &events[1];

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/events?after={after}"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame = String::from_utf8(frame.to_vec()).unwrap();
    assert!(frame.contains(&format!("id: {}", expected.cursor)));
    assert!(frame.contains(&expected.event_type));
}

#[tokio::test]
async fn http_sse_forwards_transient_reasoning_events() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let after = daemon.store().event_cursor().unwrap();
    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/events?after={after}"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    daemon
        .reasoning_events
        .send(AgentReasoningEvent::ReasoningDelta {
            run_id,
            task_id: TaskId::new(),
            attempt_id: akzio_domain::AttemptId::new(),
            purpose: "research.analyst".to_owned(),
            turn: 0,
            delta: "bounded summary".to_owned(),
        })
        .unwrap();

    let mut body = response.into_body().into_data_stream();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let frame = body.next().await.unwrap().unwrap();
            let frame = String::from_utf8(frame.to_vec()).unwrap();
            if frame.contains("event: reasoning-delta") {
                break frame;
            }
        }
    })
    .await
    .unwrap();
    assert!(frame.contains("bounded summary"));
    assert!(frame.contains("research.analyst"));
}

#[tokio::test]
async fn http_trajectory_is_authenticated_and_read_only() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    let before = daemon.store().events_after(&run_id, 0, 32).unwrap();

    let unauthorized = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/trajectory"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/trajectory"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let entries: Vec<akzio_store::v2::TrajectoryEntry> =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(entries.is_empty());
    let after = daemon.store().events_after(&run_id, 0, 32).unwrap();
    assert_eq!(before, after);
}

#[tokio::test]
async fn http_replay_reports_the_durable_snapshot() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .uri(format!("/runs/{run_id}/replay"))
                .header("x-akzio-token", "fixture-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let report = serde_json::from_slice::<ReplayReport>(
        &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap();
    assert_eq!(report.run_id, run_id);
    assert_eq!(report.purpose, RunPurpose::Debug);
    assert!(report.task_count > 0);
    assert_eq!(report.revision, 0);
}

#[test]
fn paper_submit_and_direct_retry_fail_closed() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    assert!(matches!(
        daemon.submit_default(RunPurpose::Paper),
        Err(DaemonError::InvalidInput(_))
    ));
    assert!(daemon.retry_run(&RunId::new()).is_err());
}

#[tokio::test]
async fn retry_starts_a_fresh_terminal_nonpaper_run() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let source_run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    daemon
        .request_cancel(&source_run_id, "fixture cancellation request")
        .unwrap();

    let run_id = daemon.retry_run(&source_run_id).unwrap();
    assert_ne!(run_id, source_run_id);
    assert_eq!(
        daemon.store().run_purpose(&run_id).unwrap(),
        RunPurpose::Debug
    );
    daemon.store().verify_integrity().unwrap();
}

#[test]
fn direct_submit_allows_only_debug_and_paper_dry_run() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();

    for purpose in [RunPurpose::Paper, RunPurpose::Replay, RunPurpose::Shadow] {
        assert!(matches!(
            daemon.submit_default(purpose),
            Err(DaemonError::InvalidInput(_))
        ));
    }

    assert!(daemon.submit_default(RunPurpose::Debug).is_ok());
    assert!(daemon.submit_default(RunPurpose::PaperDryRun).is_ok());
}

#[test]
fn operator_retry_rejects_paper_run() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let run_id = daemon
        .reserve_paper_session(&session_key, &paper_proposal(), now)
        .unwrap()
        .slot
        .workflow
        .run
        .run_id;

    assert!(matches!(
        daemon.retry_run(&run_id),
        Err(DaemonError::InvalidInput(message)) if message.contains("scheduler-owned")
    ));
}

#[tokio::test]
async fn http_submit_rejects_replay_before_workflow_creation() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();

    let response = daemon
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runs")
                .header("x-akzio-token", "fixture-token")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"purpose":"replay"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn daily_bar_parser_is_decimal_safe_and_rejects_duplicate_dates() {
    let observed_at = DateTime::parse_from_rfc3339("2026-08-11T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let bars = parse_daily_bars(
        &serde_json::json!({
            "bars": [
                {"t": "2026-08-10T20:00:00Z", "c": 100.25},
                {"t": "2026-08-11T20:00:00Z", "c": "-0.5"}
            ]
        }),
        observed_at,
    )
    .unwrap();
    assert_eq!(
        bars[&NaiveDate::from_ymd_opt(2026, 8, 10).unwrap()],
        MoneyMicros(100_250_000)
    );
    assert_eq!(
        bars[&NaiveDate::from_ymd_opt(2026, 8, 11).unwrap()],
        MoneyMicros(-500_000)
    );

    let duplicate = parse_daily_bars(
        &serde_json::json!({
            "bars": [
                {"t": "2026-08-10T20:00:00Z", "c": 100.25},
                {"t": "2026-08-10T20:00:00Z", "c": 101.25}
            ]
        }),
        observed_at,
    );
    assert!(matches!(
        duplicate,
        Err(PaperDecodeError::Unavailable(message)) if message == "daily bar date is duplicated"
    ));
}
