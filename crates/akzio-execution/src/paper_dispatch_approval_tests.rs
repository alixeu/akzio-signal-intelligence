use super::*;
use akzio_domain::{
    ArtifactOrigin, ArtifactProvenance, FailureDisposition, RetryPolicy, RunId, TaskBudget, TaskId,
    TaskRecipeId, WorkflowGraph, WorkflowNode, DOMAIN_SCHEMA_VERSION,
};
use akzio_store::{StoredRun, WorkflowCommit};
use std::sync::Mutex;

pub(super) fn paper_workflow() -> (Store, WorkflowCommit) {
    let now = Utc::now();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/paper-approval-dispatch-tests")
        .join(RunId::new().0);
    let store = Store::open(root).unwrap();
    let run_id = RunId::new();
    let node = WorkflowNode {
        spec: None,
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("gate.reconcile").unwrap(),
        contract_hash: None,
        objective: "Test only; no broker I/O".into(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 1,
        budget: TaskBudget {
            max_input_tokens: 1,
            max_output_tokens: 1,
            max_wall_time_secs: 1,
            max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(0),
        },
        retry: RetryPolicy::none(),
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        definition_version: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        topology_id: "paper-approval-dispatch-test".into(),
        nodes: vec![node.clone()],
        agent_budgets: Default::default(),
    };
    let artifact = Artifact::new(
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
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    let workflow = WorkflowCommit {
        run: StoredRun {
            run_id,
            purpose: RunPurpose::Paper,
            topology_id: graph.topology_id,
            graph_artifact_id: artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: artifact,
        nodes: vec![node],
    };
    (store, workflow)
}

fn reserve_fixture_session(store: &Store, workflow: WorkflowCommit, lease: &DaemonLease) {
    use akzio_domain::{
        ModelQualificationEvidence, ModelQualificationReport, PaperApprovalScope,
        PaperLaunchApproval, RuntimeManifest, WorkflowProposal,
    };
    let now = workflow.run.created_at;
    let hash = ContentHash::of_bytes(b"offline fixture");
    let expires_at = now + chrono::Duration::minutes(10);
    let components = [("default".to_owned(), hash.clone())].into();
    let bundle_hash = akzio_domain::runtime_component_bundle_hash(&components).unwrap();
    let manifest: RuntimeManifest = serde_json::from_value(serde_json::json!({
        "schema_version": DOMAIN_SCHEMA_VERSION,
        "code_revision": "fixture", "rust_toolchain": "1.96.0",
        "experiment_profile": "paper-engineering",
        "model_capability_hashes": components, "model_capability_bundle_hash": bundle_hash,
        "governance_component_hashes": components, "governance_bundle_hash": bundle_hash,
        "cargo_lock_hash": hash, "config_hash": hash,
        "provider_id": "fixture", "model_id": "fixture",
        "prompt_hash": hash, "contract_hash": hash, "topology_hash": hash,
        "decision_policy_hash": hash, "execution_policy_hash": paper_execution_policy_hash().unwrap(),
        "evaluation_policy_hash": hash,
        "market_data_feed": "iex", "broker_account_id": "offline-account",
        "maximum_notional": 10_000_000,
        "allowed_session_start": now.date_naive(), "allowed_session_end": now.date_naive(),
        "expires_at": expires_at, "created_at": now,
    }))
    .unwrap();
    let provenance = ArtifactProvenance {
        source_family: "fixture".into(),
        observed_at: None,
        retrieved_at: now,
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: None,
    };
    let manifest_artifact = Artifact::new(
        ArtifactKind::RuntimeManifest,
        store.stage_json(&manifest).unwrap(),
        "runtime.manifest",
        ArtifactLifecycle::Canonical,
        provenance.clone(),
        None,
        vec![],
        now,
    )
    .unwrap();
    let evidence_ref = |kind| ArtifactRef {
        artifact_id: akzio_domain::ArtifactId(hash.clone()),
        kind,
    };
    let evaluation = evidence_ref(ArtifactKind::Evaluation);
    let mut qualification = ModelQualificationReport {
        key: manifest.runtime_identity().qualification_key().unwrap(),
        behavior_fingerprint: hash.clone(),
        evidence: ModelQualificationEvidence {
            frozen_context_manifest: evidence_ref(ArtifactKind::ContextManifest),
            replay_evaluation: evaluation.clone(),
            adversarial_evaluation: evaluation.clone(),
            execution_simulation_evaluation: evaluation.clone(),
            shadow_evaluation: evaluation.clone(),
            canary_evaluation: evaluation,
        },
        replay_passed: true,
        adversarial_passed: true,
        execution_simulation_passed: true,
        shadow_passed: true,
        canary_passed: true,
        critical_regressions: 0,
        approved_by: "fixture".into(),
        qualified_at: now,
        expires_at,
        report_hash: hash.clone(),
    };
    qualification.report_hash = qualification.unsigned_hash();
    let mut approval = PaperLaunchApproval {
        schema_version: DOMAIN_SCHEMA_VERSION,
        operator_identity: "fixture".into(),
        runtime_manifest: artifact_ref(&manifest_artifact),
        runtime_manifest_hash: manifest.manifest_hash().unwrap(),
        scope: PaperApprovalScope::Canary,
        reason: "offline approval binding regression".into(),
        behavior_bundle_hash: None,
        approved_at: now,
        expires_at,
        qualification,
        mandate: crate::ExecutionGatePolicy::default().mandate,
        approval_hash: hash,
    };
    approval.approval_hash = approval.unsigned_hash().unwrap();
    let approval = Artifact::new(
        ArtifactKind::PaperLaunchApproval,
        store.stage_json(&approval).unwrap(),
        "operator.paper_approval",
        ArtifactLifecycle::Canonical,
        provenance.clone(),
        None,
        vec![artifact_ref(&manifest_artifact)],
        now,
    )
    .unwrap();
    let proposal = Artifact::new(
        ArtifactKind::WorkflowProposal,
        store
            .stage_json(&WorkflowProposal {
                schema_version: DOMAIN_SCHEMA_VERSION,
                topology_id: workflow.run.topology_id.clone(),
                tasks: Default::default(),
                stop_reason: None,
            })
            .unwrap(),
        "runtime.paper_provisioning",
        ArtifactLifecycle::RunScoped,
        provenance,
        workflow.graph.origin.clone(),
        vec![],
        now,
    )
    .unwrap();
    let reservation = akzio_store::SessionReservation {
        session_key: now.date_naive().to_string(),
        workflow,
        setup_artifacts: vec![],
        reserved_at: now,
    };
    store
        .reserve_paper_session_with_approval(
            lease,
            &reservation,
            &proposal,
            &manifest_artifact,
            &approval,
        )
        .unwrap();
}

pub(super) fn paper_run_with_commitment(decision_matches: bool) -> (Store, PaperDispatchInput) {
    build_paper_commitment_fixture(decision_matches, false)
}

fn build_paper_commitment_fixture(
    decision_matches: bool,
    transport_client_ids: bool,
) -> (Store, PaperDispatchInput) {
    use akzio_domain::{
        DependencyClosure, ExecutionVerdict, FactorExposure, LifecycleEventType, MandateAssessment,
        PreTradeSafetySnapshot, TargetPortfolio,
    };
    let (store, mut workflow) = paper_workflow();
    let now = workflow.run.created_at;
    let run_id = workflow.run.run_id.clone();
    let mut commit_node = workflow.nodes[0].clone();
    commit_node.task_id = TaskId::new();
    commit_node.recipe_id = TaskRecipeId::new("gate.paper_commit").unwrap();
    workflow.nodes[0]
        .dependencies
        .push(commit_node.task_id.clone());
    workflow.nodes.insert(0, commit_node);
    let mut graph: WorkflowGraph =
        serde_json::from_slice(&store.read_blob(&workflow.graph.blob).unwrap()).unwrap();
    graph.nodes = workflow.nodes.clone();
    workflow.graph = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.stage_json(&graph).unwrap(),
        "runtime.workflow",
        ArtifactLifecycle::RunScoped,
        workflow.graph.provenance.clone(),
        workflow.graph.origin.clone(),
        vec![],
        now,
    )
    .unwrap();
    workflow.run.graph_artifact_id = workflow.graph.artifact_id.clone();
    let until = now + chrono::Duration::minutes(2);
    let lease = store
        .acquire_daemon_lease("scheduler", "fixture", now, until)
        .unwrap()
        .unwrap();
    reserve_fixture_session(&store, workflow, &lease);
    let claim = || {
        store
            .claim_next_task_for_workload(
                "approval-fixture",
                Utc::now(),
                chrono::Duration::minutes(1),
                akzio_store::TaskWorkload::Session,
            )
            .unwrap()
            .unwrap()
            .permit
    };
    let permit = claim();
    let stage = |kind, blob, source_refs| {
        let lifecycle = if matches!(kind, ArtifactKind::EvidenceNeed | ArtifactKind::RawEvidence) {
            ArtifactLifecycle::RunScoped
        } else {
            ArtifactLifecycle::Canonical
        };
        Artifact::new(
            kind,
            blob,
            "fixture.execution",
            lifecycle,
            crate::trusted_execution_provenance(&permit, now),
            Some(permit.artifact_origin()),
            source_refs,
            now,
        )
        .unwrap()
    };
    let persist = |artifact: Artifact| {
        store
            .write_task_artifact(
                &permit,
                &artifact,
                LifecycleEventType::ArtifactCommitted,
                now,
            )
            .unwrap();
        artifact_ref(&artifact)
    };
    let broker_session = now.date_naive().to_string();
    let hash = ContentHash::of_bytes(b"offline fixture");
    let decision_policy_hash = if decision_matches {
        hash.clone()
    } else {
        ContentHash::of_bytes(b"different DecisionPolicy")
    };
    let target = TargetPortfolio::zeroed();
    let decision = persist(stage(
        ArtifactKind::DecisionContext,
        store.stage_json(&serde_json::json!({
            "schema_version": DOMAIN_SCHEMA_VERSION,
            "decision_id": "fixture-decision", "run_id": run_id,
            "claims": [], "critiques": [], "evidence": [], "policy_influences": [],
            "material_conflicts": [], "hard_blockers": [akzio_domain::HardBlocker::MissingEvidence],
            "soft_warnings": [],
            "decision_policy_hash": decision_policy_hash, "target": target, "created_at": now,
            "validity": {
                "evidence_cutoff": now, "generated_at": now,
                "valid_until": now + chrono::Duration::minutes(1),
                "maximum_execution_delay_ms": 60_000, "market_state_hash": hash,
            },
        })).unwrap(),
        vec![],
    ));
    let snapshot = |value: serde_json::Value| {
        persist(stage(
            ArtifactKind::NormalizedEvidence,
            store.stage_json(&value).unwrap(),
            vec![],
        ))
    };
    let need = persist(stage(
        ArtifactKind::EvidenceNeed,
        store.stage_json(&serde_json::json!({})).unwrap(),
        vec![],
    ));
    let raw = persist(stage(
        ArtifactKind::RawEvidence,
        store.stage_json(&serde_json::json!({})).unwrap(),
        vec![],
    ));
    let account_evidence = |producer, lifecycle, value, sources| {
        persist(
            Artifact::new(
                ArtifactKind::NormalizedEvidence,
                store.stage_json(&value).unwrap(),
                producer,
                lifecycle,
                ArtifactProvenance {
                    source_family: "alpaca".into(),
                    observed_at: Some(now),
                    retrieved_at: now,
                    source_uri: Some("fixture:paper.account".into()),
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: None,
                },
                Some(permit.artifact_origin()),
                sources,
                now,
            )
            .unwrap(),
        )
    };
    let component = account_evidence(
        "akzio.ingest.alpaca.normalized",
        ArtifactLifecycle::RunScoped,
        serde_json::json!({"source": "alpaca", "resource": "paper.account", "observed_at": now, "need": need, "raw": raw}),
        vec![need, raw.clone()],
    );
    let account = account_evidence(
        "execution.snapshot.account",
        ArtifactLifecycle::Canonical,
        serde_json::json!({
            "schema_version": DOMAIN_SCHEMA_VERSION,
            "broker_session": broker_session, "observed_at": now,
            "equity": 10_000_000, "buying_power": 10_000_000, "day_turnover": 0,
            "active": true, "trading_blocked": false,
            "positions": {}, "external_positions": [], "open_order_ids": [],
        }),
        vec![component, raw],
    );
    let quote = serde_json::json!({"bid": 1_000_000, "ask": 1_000_100, "observed_at": now});
    let quotes = snapshot(serde_json::json!({
        "schema_version": DOMAIN_SCHEMA_VERSION,
        "broker_session": broker_session, "observed_at": now,
        "quotes": {"QQQ": quote, "SOXX": quote},
    }));
    let clock = snapshot(serde_json::json!({
        "schema_version": DOMAIN_SCHEMA_VERSION,
        "broker_session": broker_session, "observed_at": now, "is_open": true,
    }));
    let sources = vec![
        decision.clone(),
        account.clone(),
        quotes.clone(),
        clock.clone(),
    ];
    let mut plan = ExecutionPlan {
        schema_version: DOMAIN_SCHEMA_VERSION,
        decision_context: decision.clone(),
        account_snapshot: account.clone(),
        quote_snapshot: quotes.clone(),
        market_clock_snapshot: clock.clone(),
        policy_hash: crate::ExecutionPolicy::default().policy_hash().unwrap(),
        maximum_total_notional: MoneyMicros(10_000_000),
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        target,
        orders: [Asset::Qqq, Asset::Soxx]
            .into_iter()
            .map(|asset| OrderIntent {
                asset,
                extended_hours: false,
                side: if transport_client_ids {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                notional: MoneyMicros(1_000_000),
                limit_price: MoneyMicros(1_000_000),
            })
            .collect(),
        gross_exposure_ppm: 0,
        net_exposure_ppm: 0,
        turnover_ppm: 0,
        broker_session: broker_session.clone(),
        created_at: now,
        plan_hash: hash.clone(),
    };
    plan.refresh_hash().unwrap();
    let plan_ref = persist(stage(
        ArtifactKind::ExecutionPlan,
        store.stage_json(&plan).unwrap(),
        sources.clone(),
    ));
    let context = ExecutionContext {
        schema_version: DOMAIN_SCHEMA_VERSION,
        run_id,
        decision_context: decision,
        account_snapshot: Some(account),
        quote_snapshot: Some(quotes),
        market_clock_snapshot: Some(clock),
        execution_plan: Some(plan_ref.clone()),
        factor_exposure: Some(plan.factor_exposure),
        turnover_ppm: Some(0),
        plan_hash: Some(plan.plan_hash.clone()),
        broker_session: Some(broker_session.clone()),
        mandate_assessment: Some(MandateAssessment {
            mandate_hash: hash,
            violations: Default::default(),
            distance_before_ppm: 0,
            distance_after_ppm: 0,
        }),
        pretrade_safety: Some(PreTradeSafetySnapshot {
            ordered_assets: [Asset::Qqq, Asset::Soxx].into(),
            information_classifications: Default::default(),
            recent_compliance_activity: Default::default(),
            dependency_closure: DependencyClosure {
                captured_at: now,
                dependencies: Default::default(),
            },
            capacity_scenario: None,
            missing_average_daily_dollar_volume: Default::default(),
            compliance_violations_by_asset: [
                (Asset::Qqq, Default::default()),
                (Asset::Soxx, Default::default()),
            ]
            .into(),
            missing_information_classifications: Default::default(),
            risk_increasing: false,
            dependency_permits_new_risk: false,
            dependency_permits_risk_reduction: true,
            permits_execution: true,
            permits_risk_increasing: false,
        }),
        final_process_quality: None,
        frozen: false,
        created_at: now,
    };
    let context = persist(stage(
        ArtifactKind::ExecutionContext,
        store.stage_json(&context).unwrap(),
        [sources, vec![plan_ref]].concat(),
    ));
    let verdict = persist(stage(
        ArtifactKind::ExecutionVerdict,
        store
            .stage_json(&ExecutionVerdict::Accepted {
                execution_context: context.clone(),
            })
            .unwrap(),
        vec![context.clone()],
    ));
    let commitment = PaperCommitment {
        commitment_id: akzio_domain::PaperCommitmentId::new(),
        execution_context: context.clone(),
        plan_hash: plan.plan_hash.clone(),
        broker_session: broker_session.clone(),
        client_order_ids: if transport_client_ids {
            plan.orders
                .iter()
                .enumerate()
                .map(|(index, order)| {
                    (
                        order.asset,
                        client_order_id(&broker_session, &plan.plan_hash, index, 0),
                    )
                })
                .collect()
        } else {
            [
                (Asset::Qqq, "original-qqq".into()),
                (Asset::Soxx, "original-soxx".into()),
            ]
            .into()
        },
        created_at: now,
    };
    let commitment = stage(
        ArtifactKind::ExecutionCommitment,
        store.stage_json(&commitment).unwrap(),
        vec![context, verdict],
    );
    let reference = artifact_ref(&commitment);
    store
        .commit_execution(
            &lease,
            &akzio_store::ExecutionCommit {
                session_key: broker_session,
                permit,
                commitment,
                committed_at: now,
            },
        )
        .unwrap();
    let permit = claim();
    (
        store,
        PaperDispatchInput {
            lease,
            permit,
            commitment: reference,
            now,
        },
    )
}

struct RecordingBroker {
    account: &'static str,
    observation_unavailable: bool,
    calls: Mutex<Vec<&'static str>>,
}

impl RecordingBroker {
    fn new(account: &'static str) -> Self {
        Self {
            account,
            observation_unavailable: false,
            calls: Mutex::new(vec![]),
        }
    }

    fn execution(commitment: &PaperCommitment) -> PaperExecution {
        PaperExecution {
            plan_hash: commitment.plan_hash.clone(),
            orders: commitment
                .client_order_ids
                .iter()
                .map(|(asset, client_id)| PaperOrderReceipt {
                    client_order_id: client_id.clone(),
                    broker_order_id: format!("broker-{client_id}"),
                    symbol: asset.symbol().into(),
                    status: "canceled".into(),
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: 0,
                    remaining_quantity_micros: 1_000_000,
                    average_fill_price: None,
                    broker_updated_at: Utc::now(),
                    reason: None,
                    reused: true,
                    reprice_count: 0,
                })
                .collect(),
        }
    }
}

impl CommittedPaperBroker for RecordingBroker {
    fn paper_account_id(
        &self,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<String>> + Send + '_>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("account");
            Ok(self.account.to_owned())
        })
    }

    fn observe_existing_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        journal: Option<&'a PaperSubmissionJournal<'a>>,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("observe");
            if self.observation_unavailable {
                return Err(PaperError::Http {
                    url: "offline-order-lookup".into(),
                    status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    body: "temporarily unavailable".into(),
                });
            }
            let execution = Self::execution(commitment);
            if let Some(journal) = journal {
                for receipt in &execution.orders {
                    journal.record(receipt)?;
                }
            }
            Ok(execution)
        })
    }

    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
        authorization: &'a PaperSubmissionAuthorization,
        journal: &'a PaperSubmissionJournal<'a>,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            authorization.assert_current(&plan.plan_hash, Utc::now())?;
            self.calls.lock().unwrap().push("execute");
            let execution = Self::execution(commitment);
            for receipt in &execution.orders {
                journal.record(receipt)?;
            }
            Ok(execution)
        })
    }

    fn reconcile_commitment<'a>(
        &'a self,
        _: &'a PaperCommitment,
        execution: &'a PaperExecution,
        _journal: &'a PaperSubmissionJournal<'a>,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("reconcile");
            Ok(execution.clone())
        })
    }

    fn cancel_order<'a>(
        &'a self,
        _: &'a PaperCancel,
        _journal: Option<&'a PaperSubmissionJournal<'a>>,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<PaperOrderReceipt>> + Send + 'a>> {
        panic!("fixture must not cancel an order")
    }

    fn replace_order<'a>(
        &'a self,
        _: &'a PaperReprice,
        _: &'a PaperSubmissionAuthorization,
        _journal: &'a PaperSubmissionJournal<'a>,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<PaperOrderReceipt>> + Send + 'a>> {
        panic!("fixture must not replace an order")
    }
}

/// WHY THIS TEST EXISTS (Task #4): Both dispatch entry points must verify the original account before touching orders.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn wrong_account_cannot_dispatch_or_observe_an_existing_commitment() {
    let (store, input) = paper_run_with_commitment(true);
    let runtime = PaperDispatchRuntime::new(store.clone());
    let wrong = RecordingBroker::new("another-account");
    let result = runtime.dispatch(&wrong, &input).await;
    assert_eq!(*wrong.calls.lock().unwrap(), ["account"]);
    assert!(matches!(
        result,
        Err(PaperDispatchError::Broker(PaperError::AccountMismatch))
    ));
    let unavailable = RecordingBroker::new("");
    assert!(matches!(
        runtime.dispatch(&unavailable, &input).await,
        Err(PaperDispatchError::Broker(
            PaperError::AccountIdentityUnavailable
        ))
    ));
    assert_eq!(*unavailable.calls.lock().unwrap(), ["account"]);
    assert!(!store
        .paper_effect_intent_recorded(&input.permit.run_id, &input.commitment.artifact_id)
        .unwrap());

    store
        .record_paper_effect_intent(&input.lease, &input.permit, &input.commitment, input.now)
        .unwrap();
    let wrong = RecordingBroker::new("another-account");
    let result = runtime.reconcile_existing_only(&wrong, &input).await;
    assert_eq!(*wrong.calls.lock().unwrap(), ["account"]);
    assert!(matches!(
        result,
        Err(PaperDispatchError::Broker(PaperError::AccountMismatch))
    ));
}

/// WHY THIS TEST EXISTS (Task #4): An approval for a different Policy may only observe a durable effect on the verified account.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn original_account_with_different_decision_policy_can_only_observe_recorded_orders() {
    let (store, input) = paper_run_with_commitment(false);
    let runtime = PaperDispatchRuntime::new(store.clone());
    let broker = RecordingBroker::new("offline-account");
    assert!(runtime.dispatch(&broker, &input).await.is_err());
    assert_eq!(*broker.calls.lock().unwrap(), ["account"]);
    assert!(!store
        .paper_effect_intent_recorded(&input.permit.run_id, &input.commitment.artifact_id)
        .unwrap());

    store
        .record_paper_effect_intent(&input.lease, &input.permit, &input.commitment, input.now)
        .unwrap();
    let stored = store.artifact(&input.commitment.artifact_id).unwrap();
    let commitment: PaperCommitment =
        serde_json::from_slice(&store.read_blob(&stored.blob).unwrap()).unwrap();
    let mut known = RecordingBroker::execution(&commitment)
        .orders
        .into_iter()
        .find(|receipt| receipt.symbol == Asset::Qqq.symbol())
        .unwrap();
    known.status = "accepted".into();
    let reconciliation = ReconciliationRuntime::new(store.clone());
    let progress = reconciliation
        .reconcile(&ReconciliationInput {
            permit: input.permit.clone(),
            commitment: input.commitment.clone(),
            reprices: vec![],
            cancels: vec![],
            broker_receipts: vec![broker_receipt(&known, &commitment, input.now).unwrap()],
            now: input.now,
        })
        .unwrap();
    reconciliation
        .write_progress(&input.lease, &input.permit, &progress, input.now)
        .unwrap();
    let prior = artifact_ref(&progress.receipts[0]);
    let cancel = PaperCancel {
        schema_version: DOMAIN_SCHEMA_VERSION,
        cancel_id: PaperCancelId::new(),
        commitment: input.commitment.clone(),
        prior_receipt: prior.clone(),
        asset: Asset::Qqq,
        client_order_id: known.client_order_id,
        broker_order_id: known.broker_order_id,
        reason: PaperCancelReason::RiskReduction,
        created_at: input.now,
    };
    let cancel = Artifact::new(
        ArtifactKind::ExecutionCancel,
        store.stage_json(&cancel).unwrap(),
        "execution.cancel",
        ArtifactLifecycle::Canonical,
        crate::trusted_execution_provenance(&input.permit, input.now),
        Some(input.permit.artifact_origin()),
        vec![input.commitment.clone(), prior],
        input.now,
    )
    .unwrap();
    store
        .commit_execution_cancel_intent(&input.lease, &input.permit, &cancel, input.now)
        .unwrap();

    let mut unavailable = RecordingBroker::new("offline-account");
    unavailable.observation_unavailable = true;
    let error = runtime.dispatch(&unavailable, &input).await.unwrap_err();
    assert!(matches!(
        error,
        PaperDispatchError::RecordedEffectObservationUnavailable(PaperError::Http {
            status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
            ..
        })
    ));
    assert_eq!(*unavailable.calls.lock().unwrap(), ["account", "observe"]);
    assert!(!store
        .paper_effect_intent_recorded(&input.permit.run_id, &cancel.artifact_id)
        .unwrap());

    let broker = RecordingBroker::new("offline-account");
    let result = runtime.dispatch(&broker, &input).await.unwrap();
    assert_eq!(*broker.calls.lock().unwrap(), ["account", "observe"]);
    assert!(result.settled);
    assert_eq!(result.execution.orders.len(), 2);
    assert!(!store
        .paper_effect_intent_recorded(&input.permit.run_id, &cancel.artifact_id)
        .unwrap());
}

/// WHY THIS TEST EXISTS (Task #4): A coherent original approval and a verified account still permit ordinary dispatch.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn matching_original_account_and_policy_continue_normal_dispatch() {
    let (store, input) = paper_run_with_commitment(true);
    let broker = RecordingBroker::new("offline-account");
    let result = PaperDispatchRuntime::new(store)
        .dispatch(&broker, &input)
        .await
        .unwrap();
    assert_eq!(
        *broker.calls.lock().unwrap(),
        ["account", "execute", "reconcile"]
    );
    assert!(result.settled);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PaperTransportReply {
    FilledPost,
    UnknownPost,
    UnsupportedPostStatus,
    UnknownPatch,
    UnsupportedPatchStatus,
    TerminalPatchWhileOriginalLive,
    ReplacedDuringCancel,
    LegacyPartialThenUnavailable,
    LegacyReplacedDuringCancel,
    LegacyReplacementOriginalMissing,
}

impl PaperTransportReply {
    fn replaces(self) -> bool {
        matches!(
            self,
            Self::UnknownPatch
                | Self::UnsupportedPatchStatus
                | Self::TerminalPatchWhileOriginalLive
                | Self::ReplacedDuringCancel
        )
    }
}

async fn paper_transport_fixture(
    store: &Store,
    reference: &ArtifactRef,
    commitment: &PaperCommitment,
    reply: PaperTransportReply,
) -> (
    AlpacaPaper,
    Arc<Mutex<Vec<String>>>,
    Arc<std::sync::atomic::AtomicBool>,
    tokio::task::JoinHandle<()>,
) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let visible = Arc::new(AtomicBool::new(false));
    let client_order_id = commitment.client_order_ids[&Asset::Qqq].clone();
    let successor_id = replacement_client_order_id(&client_order_id);
    let sibling_id = commitment.client_order_ids[&Asset::Soxx].clone();
    let run_id = store
        .artifact(&reference.artifact_id)
        .unwrap()
        .origin
        .unwrap()
        .run_id
        .unwrap();
    let broker_session = commitment.broker_session.clone();
    let terminal = serde_json::json!({
        "id": "offline-order-one",
        "symbol": "QQQ",
        "status": "filled",
        "client_order_id": client_order_id,
        "qty": "1",
        "filled_qty": "1",
        "filled_avg_price": "1",
        "updated_at": commitment.created_at,
    });
    let server = tokio::spawn({
        let requests = requests.clone();
        let visible = visible.clone();
        let store = store.clone();
        let reference = reference.clone();
        async move {
            let mut ambiguous_response_delivered = false;
            let mut replacement_requested = false;
            let mut malformed_post_observations = 0;
            let mut replacement_original_observations = 0;
            let mut original = terminal.clone();
            original["status"] = serde_json::json!("accepted");
            original["filled_qty"] = serde_json::json!("0");
            original["filled_avg_price"] = Value::Null;
            let mut successor = terminal.clone();
            successor["id"] = serde_json::json!("offline-replacement");
            successor["client_order_id"] = serde_json::json!(successor_id);
            let mut canceled_sibling = terminal.clone();
            canceled_sibling["id"] = serde_json::json!("offline-order-two");
            canceled_sibling["symbol"] = serde_json::json!("SOXX");
            canceled_sibling["client_order_id"] = serde_json::json!(sibling_id);
            canceled_sibling["status"] = serde_json::json!("canceled");
            canceled_sibling["filled_qty"] = serde_json::json!("0");
            canceled_sibling["filled_avg_price"] = Value::Null;
            let mut open_successor = successor.clone();
            open_successor["status"] = serde_json::json!("accepted");
            open_successor["filled_qty"] = serde_json::json!("0");
            open_successor["filled_avg_price"] = Value::Null;
            let mut cancel_original_reads = 0;
            let unusable_status = |mut receipt: Value| {
                receipt["status"] = serde_json::json!("unrecognized_future_status");
                receipt
            };
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut request_line = String::new();
                reader.read_line(&mut request_line).await.unwrap();
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap();
                let path = parts.next().unwrap();
                let mut length = 0;
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).await.unwrap();
                    if header == "\r\n" {
                        break;
                    }
                    assert!(!header.is_empty());
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).await.unwrap();
                requests.lock().unwrap().push(format!("{method} {path}"));
                let (status, payload) = match (method, path) {
                    ("GET", "/v2/account") => {
                        ("200 OK", serde_json::json!({"id":"offline-account"}))
                    }
                    ("GET", "/v2/clock") => (
                        "200 OK",
                        serde_json::json!({"is_open":true,"timestamp":format!("{broker_session}T17:00:00Z")}),
                    ),
                    ("GET", path) if path.starts_with("/v2/calendar?") => (
                        "200 OK",
                        serde_json::json!([{"date":broker_session,"open":"09:30","close":"16:00"}]),
                    ),
                    ("GET", path) if path.starts_with("/v2/orders:by_client_order_id?") => {
                        if ambiguous_response_delivered {
                            assert!(
                                matches!(
                                    store.paper_submission_batch(&reference).unwrap(),
                                    Some(PaperSubmissionBatch::Journaled {
                                        observation_only: true,
                                        ..
                                    })
                                ),
                                "recovery GET started before the durable observation-only stop"
                            );
                        }
                        if reply == PaperTransportReply::LegacyReplacementOriginalMissing {
                            if path.ends_with(&successor_id) {
                                ("200 OK", successor.clone())
                            } else if path.ends_with(&sibling_id) {
                                ("200 OK", canceled_sibling.clone())
                            } else {
                                ("404 Not Found", serde_json::json!({}))
                            }
                        } else if path.ends_with(&client_order_id)
                            && reply == PaperTransportReply::LegacyPartialThenUnavailable
                        {
                            if visible.load(Ordering::SeqCst) {
                                ("404 Not Found", serde_json::json!({}))
                            } else {
                                let mut partial = terminal.clone();
                                partial["status"] = serde_json::json!("partially_filled");
                                partial["filled_qty"] = serde_json::json!("0.4");
                                ("200 OK", partial)
                            }
                        } else if path.ends_with(&sibling_id)
                            && reply == PaperTransportReply::LegacyPartialThenUnavailable
                        {
                            let already_saved = store
                                .run_artifacts_by_kind(&run_id, ArtifactKind::OrderReceipt)
                                .unwrap()
                                .iter()
                                .any(|artifact| {
                                    artifact.producer == "execution.submission_receipt"
                                });
                            (
                                "503 Service Unavailable",
                                serde_json::json!({"first_receipt_already_saved":already_saved}),
                            )
                        } else if path.ends_with(&client_order_id)
                            && reply == PaperTransportReply::LegacyReplacedDuringCancel
                        {
                            if visible.load(Ordering::SeqCst) {
                                cancel_original_reads += 1;
                            }
                            if cancel_original_reads >= 2 {
                                original["status"] = serde_json::json!("replaced");
                                original["updated_at"] = serde_json::json!(Utc::now());
                            }
                            ("200 OK", original.clone())
                        } else if path.ends_with(&client_order_id) && reply.replaces() {
                            let mut receipt = original.clone();
                            if replacement_requested
                                && reply == PaperTransportReply::UnsupportedPatchStatus
                            {
                                replacement_original_observations += 1;
                                if replacement_original_observations == 1 {
                                    receipt["id"] = serde_json::json!("different-broker-order");
                                }
                            }
                            if reply == PaperTransportReply::ReplacedDuringCancel
                                && visible.load(Ordering::SeqCst)
                            {
                                cancel_original_reads += 1;
                            }
                            if (reply == PaperTransportReply::TerminalPatchWhileOriginalLive
                                && visible.load(Ordering::SeqCst))
                                || cancel_original_reads >= 2
                            {
                                receipt["status"] = serde_json::json!("replaced");
                                receipt["updated_at"] = serde_json::json!(Utc::now());
                            }
                            ("200 OK", receipt)
                        } else if path.ends_with(&client_order_id) && visible.load(Ordering::SeqCst)
                        {
                            ("200 OK", terminal.clone())
                        } else if path.ends_with(&client_order_id)
                            && ambiguous_response_delivered
                            && reply == PaperTransportReply::UnsupportedPostStatus
                        {
                            malformed_post_observations += 1;
                            let malformed = if malformed_post_observations == 1 {
                                serde_json::json!({"client_order_id":client_order_id})
                            } else {
                                let mut wrong_order = terminal.clone();
                                wrong_order["client_order_id"] = serde_json::json!("another-order");
                                wrong_order
                            };
                            ("200 OK", malformed)
                        } else if path.ends_with(&successor_id) && replacement_requested {
                            if reply == PaperTransportReply::ReplacedDuringCancel {
                                match cancel_original_reads {
                                    0 => ("200 OK", open_successor.clone()),
                                    1 | 2 => ("404 Not Found", serde_json::json!({})),
                                    _ => {
                                        let mut filled = successor.clone();
                                        filled["updated_at"] = serde_json::json!(Utc::now());
                                        ("200 OK", filled)
                                    }
                                }
                            } else if visible.load(Ordering::SeqCst)
                                || reply == PaperTransportReply::TerminalPatchWhileOriginalLive
                            {
                                ("200 OK", successor.clone())
                            } else if reply == PaperTransportReply::UnsupportedPatchStatus {
                                ("200 OK", unusable_status(successor.clone()))
                            } else {
                                ("404 Not Found", serde_json::json!({}))
                            }
                        } else {
                            ("404 Not Found", serde_json::json!({}))
                        }
                    }
                    ("GET", "/v2/orders/offline-order-one") => {
                        assert!(reply.replaces());
                        ("200 OK", original.clone())
                    }
                    ("GET", "/v2/orders/offline-replacement") => match reply {
                        PaperTransportReply::TerminalPatchWhileOriginalLive => {
                            ("200 OK", successor.clone())
                        }
                        PaperTransportReply::ReplacedDuringCancel => {
                            ("200 OK", open_successor.clone())
                        }
                        _ => panic!("unknown successor must use exact client ID readback"),
                    },
                    ("POST", "/v2/orders") => {
                        let order: Value = serde_json::from_slice(&body).unwrap();
                        assert_eq!(order["client_order_id"], client_order_id);
                        match reply {
                            PaperTransportReply::FilledPost => ("200 OK", terminal.clone()),
                            // The remote write arrived, but no decodable receipt reached Akzio.
                            PaperTransportReply::UnknownPost => {
                                ambiguous_response_delivered = true;
                                (
                                    "200 OK",
                                    serde_json::json!({"client_order_id":client_order_id}),
                                )
                            }
                            PaperTransportReply::UnsupportedPostStatus => {
                                ambiguous_response_delivered = true;
                                ("200 OK", unusable_status(terminal.clone()))
                            }
                            PaperTransportReply::UnknownPatch
                            | PaperTransportReply::UnsupportedPatchStatus
                            | PaperTransportReply::TerminalPatchWhileOriginalLive
                            | PaperTransportReply::ReplacedDuringCancel
                            | PaperTransportReply::LegacyPartialThenUnavailable
                            | PaperTransportReply::LegacyReplacedDuringCancel
                            | PaperTransportReply::LegacyReplacementOriginalMissing => {
                                panic!("existing orders must not POST again")
                            }
                        }
                    }
                    ("PATCH", "/v2/orders/offline-order-one") => {
                        assert!(reply.replaces());
                        assert!(!replacement_requested, "ambiguous replacement was resent");
                        let order: Value = serde_json::from_slice(&body).unwrap();
                        assert_eq!(order["client_order_id"], successor_id);
                        replacement_requested = true;
                        if reply == PaperTransportReply::TerminalPatchWhileOriginalLive {
                            ("200 OK", successor.clone())
                        } else if reply == PaperTransportReply::ReplacedDuringCancel {
                            ("200 OK", open_successor.clone())
                        } else {
                            ambiguous_response_delivered = true;
                            original["status"] = serde_json::json!("canceled");
                            original["updated_at"] = serde_json::json!(Utc::now());
                            let payload = if reply == PaperTransportReply::UnsupportedPatchStatus {
                                unusable_status(successor.clone())
                            } else {
                                serde_json::json!({"client_order_id":successor_id})
                            };
                            ("200 OK", payload)
                        }
                    }
                    other => panic!("unexpected test transport request {other:?}"),
                };
                let payload = payload.to_string();
                let response = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}", payload.len());
                reader
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();
            }
        }
    });
    let mut client = AlpacaPaper::new(
        "https://paper-api.alpaca.markets",
        PaperCredentials {
            key_id: "fixture-key".into(),
            secret_key: "fixture-secret".into(),
        },
    )
    .unwrap();
    client.base_url = format!("http://{address}");
    client.client = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .http1_only()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();
    (client, requests, visible, server)
}

fn journaled_commitment(store: &Store, input: &PaperDispatchInput) -> PaperCommitment {
    let artifact = store.artifact(&input.commitment.artifact_id).unwrap();
    serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap()
}

fn assert_partial_fill_closed(
    store: &Store,
    output: &PaperDispatchOutput,
    reason: akzio_domain::PaperUnsubmittedReason,
) {
    assert!(output.settled, "{output:?}");
    let payload: akzio_domain::Reconciliation = serde_json::from_slice(
        &store
            .read_blob(&output.reconciliation.reconciliation.blob)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(payload.state, akzio_domain::ReconciliationState::Complete);
    assert_eq!(payload.unsubmitted_orders.len(), 1);
    assert_eq!(payload.unsubmitted_orders[0].asset, Asset::Soxx);
    assert_eq!(payload.unsubmitted_orders[0].reason, reason);
    assert_eq!(payload.broker_receipts.len(), 1);
    let receipt: OrderReceipt = serde_json::from_slice(
        &store
            .read_blob(&output.reconciliation.receipts[0].blob)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(receipt.asset, Asset::Qqq);
    assert_eq!(receipt.filled_quantity_micros, 1_000_000);
    assert_eq!(receipt.average_fill_price, Some(MoneyMicros(1_000_000)));
    let achieved = payload.achieved_target.unwrap();
    assert_eq!(achieved.weights[&Asset::Soxx].0, 0);
    assert_eq!(achieved.weights[&Asset::Qqq].0, 100_000);
}

/// WHY THIS TEST EXISTS (Task #30): A verified first fill and durable cancellation of the unsent sibling must close the exact batch without inventing a receipt.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_transport_settles_first_fill_and_proven_unsent_sibling() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (store, input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    let (client, requests, _, server) = paper_transport_fixture(
        &store,
        &input.commitment,
        &commitment,
        PaperTransportReply::FilledPost,
    )
    .await;
    let admissions = Arc::new(AtomicUsize::new(0));
    let client = client
        .with_execution_freeze_store(store.clone())
        .with_write_guard({
            let admissions = admissions.clone();
            let store = store.clone();
            let reference = input.commitment.clone();
            let run_id = input.permit.run_id.clone();
            Arc::new(move |kind| {
                assert_eq!(kind, PaperWriteKind::Submit);
                if admissions.fetch_add(1, Ordering::SeqCst) == 1 {
                    let Some(PaperSubmissionBatch::Journaled { orders, .. }) =
                        store.paper_submission_batch(&reference).unwrap()
                    else {
                        panic!("expected exact order journal before the second admission");
                    };
                    assert!(orders.iter().any(|order| {
                        order.asset == Asset::Qqq && order.state == PaperSubmissionState::Terminal
                    }));
                    assert!(orders.iter().any(|order| {
                        order.asset == Asset::Soxx && order.state == PaperSubmissionState::Ready
                    }));
                    store
                        .request_run_cancel(
                            &run_id,
                            "offline cancellation before sibling send",
                            Utc::now(),
                        )
                        .unwrap();
                    return Err(PaperError::RunCancelled);
                }
                Ok(())
            })
        });
    let output = PaperDispatchRuntime::new(store.clone())
        .dispatch(&client, &input)
        .await
        .unwrap();
    assert_partial_fill_closed(
        &store,
        &output,
        akzio_domain::PaperUnsubmittedReason::RunCancelled,
    );
    assert_eq!(admissions.load(Ordering::SeqCst), 2);
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.starts_with("POST "))
            .count(),
        1
    );
    server.abort();
}

/// WHY THIS TEST EXISTS (Task #30): A received POST with an unusable response and repeated 404s must never resubmit either original, while a later exact receipt can settle.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_transport_never_resends_after_unusable_post_response() {
    use std::sync::atomic::Ordering;

    let (store, mut input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    let (client, requests, visible, server) = paper_transport_fixture(
        &store,
        &input.commitment,
        &commitment,
        PaperTransportReply::UnknownPost,
    )
    .await;
    let runtime = PaperDispatchRuntime::new(store.clone());
    let output = runtime.dispatch(&client, &input).await.unwrap();
    assert!(!output.settled);
    assert!(output.reconciliation.receipts.is_empty());
    let Some(PaperSubmissionBatch::Journaled {
        orders,
        observation_only: true,
        ..
    }) = store.paper_submission_batch(&input.commitment).unwrap()
    else {
        panic!("uncertain POST must close sibling admission before observation");
    };
    assert!(orders.iter().any(|order| {
        order.asset == Asset::Qqq && order.state == PaperSubmissionState::PossiblySubmitted
    }));
    assert!(orders.iter().any(|order| {
        order.asset == Asset::Soxx && order.state == PaperSubmissionState::ProvenUnsubmitted
    }));
    for found in [false, true] {
        claim_paper_reattempt(&store, &mut input);
        visible.store(found, Ordering::SeqCst);
        let output = runtime.dispatch(&client, &input).await.unwrap();
        assert_eq!(output.settled, found);
        if found {
            assert_partial_fill_closed(
                &store,
                &output,
                akzio_domain::PaperUnsubmittedReason::BatchObservationOnly,
            );
        } else {
            assert!(output.reconciliation.receipts.is_empty());
        }
    }
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("POST "))
            .count(),
        1
    );
    assert!(!requests.iter().any(|request| request.starts_with("PATCH ")));
    server.abort();
}

fn claim_paper_reattempt(store: &Store, input: &mut PaperDispatchInput) {
    let now = Utc::now();
    let ready_at = now + chrono::Duration::milliseconds(1);
    store.defer_task(&input.permit, ready_at, now).unwrap();
    let previous = input.permit.attempt_id.clone();
    input.permit = store
        .claim_next_task_for_workload(
            "restarted-paper-fixture",
            ready_at,
            chrono::Duration::minutes(1),
            akzio_store::TaskWorkload::Session,
        )
        .unwrap()
        .unwrap()
        .permit;
    assert_ne!(input.permit.attempt_id, previous);
}

fn seed_replaceable_paper_orders(
    store: &Store,
    input: &PaperDispatchInput,
    commitment: &PaperCommitment,
) {
    let authority = PaperSubmissionAuthority {
        lease: &input.lease,
        permit: &input.permit,
        commitment: &input.commitment,
    };
    store.begin_paper_submission_batch(&authority).unwrap();
    for (asset, broker_order_id, state) in [
        (Asset::Qqq, "offline-order-one", OrderReceiptState::Accepted),
        (
            Asset::Soxx,
            "offline-order-two",
            OrderReceiptState::Canceled,
        ),
    ] {
        store
            .record_paper_submission_receipt(
                &authority,
                &OrderReceipt {
                    plan_hash: commitment.plan_hash.clone(),
                    asset,
                    client_order_id: commitment.client_order_ids[&asset].clone(),
                    broker_order_id: broker_order_id.into(),
                    state,
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: 0,
                    remaining_quantity_micros: 1_000_000,
                    average_fill_price: None,
                    broker_updated_at: commitment.created_at,
                    reason: None,
                    observed_at: commitment.created_at,
                },
            )
            .unwrap();
    }
}

/// WHY THIS TEST EXISTS (Task #30): An admitted PATCH with a lost receipt remains uncertain after 404 even when every original is terminal, and later settles through the exact successor without another PATCH.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_transport_never_resends_an_unobserved_replacement() {
    for reply in [
        PaperTransportReply::UnknownPatch,
        PaperTransportReply::UnsupportedPatchStatus,
    ] {
        assert_replacement_recovery(reply).await;
    }
}

async fn assert_replacement_recovery(reply: PaperTransportReply) {
    use std::sync::atomic::Ordering;

    let (store, mut input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    seed_replaceable_paper_orders(&store, &input, &commitment);
    let (client, requests, visible, server) =
        paper_transport_fixture(&store, &input.commitment, &commitment, reply).await;
    let runtime = PaperDispatchRuntime {
        settlement_timeout: std::time::Duration::ZERO,
        settlement_action_grace: std::time::Duration::ZERO,
        ..PaperDispatchRuntime::new(store.clone())
    };
    for (attempt, observe_successor) in [false, false, true].into_iter().enumerate() {
        visible.store(observe_successor, Ordering::SeqCst);
        let output = runtime.dispatch(&client, &input).await;
        if !observe_successor && reply == PaperTransportReply::UnsupportedPatchStatus {
            assert!(
                matches!(
                    &output,
                    Err(PaperDispatchError::RecordedEffectObservationUnavailable(
                        PaperError::BrokerReceiptUnavailable { .. }
                    ))
                ),
                "{output:?}"
            );
        } else {
            assert_eq!(output.as_ref().unwrap().settled, observe_successor);
        }
        let Some(PaperSubmissionBatch::Journaled {
            observation_only: true,
            orders,
            ..
        }) = store.paper_submission_batch(&input.commitment).unwrap()
        else {
            panic!("admitted replacement must survive in the exact journal");
        };
        let replacement = orders.iter().find(|order| order.reprice.is_some()).unwrap();
        assert_eq!(
            replacement.state,
            if observe_successor {
                PaperSubmissionState::Terminal
            } else {
                PaperSubmissionState::PossiblySubmitted
            }
        );
        if !observe_successor {
            for original in orders.iter().filter(|order| order.reprice.is_none()) {
                let expected = if reply == PaperTransportReply::UnsupportedPatchStatus
                    && attempt == 0
                    && original.asset == Asset::Qqq
                {
                    PaperSubmissionState::Observed
                } else {
                    PaperSubmissionState::Terminal
                };
                assert_eq!(original.state, expected);
            }
            claim_paper_reattempt(&store, &mut input);
        } else {
            let output = output.unwrap();
            let payload: akzio_domain::Reconciliation = serde_json::from_slice(
                &store
                    .read_blob(&output.reconciliation.reconciliation.blob)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(payload.state, akzio_domain::ReconciliationState::Complete);
            assert!(payload.unsubmitted_orders.is_empty());
            assert_eq!(payload.broker_receipts.len(), 2);
            assert_eq!(
                payload.achieved_target.unwrap().weights[&Asset::Qqq].0,
                100_000
            );
        }
    }
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("PATCH "))
            .count(),
        1
    );
    assert!(!requests.iter().any(|request| request.starts_with("POST ")));
    server.abort();
}

/// WHY THIS TEST EXISTS (Task #30): A structurally valid POST response with an unknown status, followed by malformed exact-ID GET responses, must defer until a genuine receipt is available.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_transport_defers_unusable_receipts_until_exact_readback() {
    use std::sync::atomic::Ordering;

    let (store, mut input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    let (client, requests, visible, server) = paper_transport_fixture(
        &store,
        &input.commitment,
        &commitment,
        PaperTransportReply::UnsupportedPostStatus,
    )
    .await;
    let runtime = PaperDispatchRuntime::new(store.clone());
    for _ in 0..2 {
        let result = runtime.dispatch(&client, &input).await;
        assert!(
            matches!(
                &result,
                Err(PaperDispatchError::RecordedEffectObservationUnavailable(
                    PaperError::BrokerReceiptUnavailable { .. }
                ))
            ),
            "{result:?}"
        );
        let Some(PaperSubmissionBatch::Journaled {
            observation_only: true,
            orders,
            ..
        }) = store.paper_submission_batch(&input.commitment).unwrap()
        else {
            panic!("unusable broker response must leave durable observation-only recovery");
        };
        assert!(orders.iter().any(|order| {
            order.asset == Asset::Qqq
                && order.state == PaperSubmissionState::PossiblySubmitted
                && order.receipt.is_none()
        }));
        assert!(orders.iter().any(|order| {
            order.asset == Asset::Soxx && order.state == PaperSubmissionState::ProvenUnsubmitted
        }));
        claim_paper_reattempt(&store, &mut input);
    }
    visible.store(true, Ordering::SeqCst);
    let output = runtime.dispatch(&client, &input).await.unwrap();
    assert_partial_fill_closed(
        &store,
        &output,
        akzio_domain::PaperUnsubmittedReason::BatchObservationOnly,
    );
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("POST "))
            .count(),
        1
    );
    assert!(!requests
        .iter()
        .any(|request| request.starts_with("PATCH ") || request.starts_with("DELETE ")));
    server.abort();
}

/// WHY THIS TEST EXISTS (Task #30): A terminal r1 does not hide a live original r0; both normal and read-only dispatch must preserve original progress until exact r0 closure.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_transport_keeps_terminal_replacement_pending_until_original_closes() {
    use std::sync::atomic::Ordering;

    let (store, mut input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    seed_replaceable_paper_orders(&store, &input, &commitment);
    let (client, requests, original_closed, server) = paper_transport_fixture(
        &store,
        &input.commitment,
        &commitment,
        PaperTransportReply::TerminalPatchWhileOriginalLive,
    )
    .await;
    let runtime = PaperDispatchRuntime {
        settlement_timeout: std::time::Duration::ZERO,
        settlement_action_grace: std::time::Duration::ZERO,
        ..PaperDispatchRuntime::new(store.clone())
    };
    let mut output = runtime.dispatch(&client, &input).await.unwrap();
    for attempt in 0..2 {
        assert!(!output.settled);
        let original = output
            .reconciliation
            .receipts
            .iter()
            .map(|artifact| {
                serde_json::from_slice::<OrderReceipt>(&store.read_blob(&artifact.blob).unwrap())
                    .unwrap()
            })
            .find(|receipt| receipt.asset == Asset::Qqq)
            .unwrap();
        assert_eq!(
            original.client_order_id,
            commitment.client_order_ids[&Asset::Qqq]
        );
        assert_eq!(original.state, OrderReceiptState::Accepted);
        let Some(PaperSubmissionBatch::Journaled { orders, .. }) =
            store.paper_submission_batch(&input.commitment).unwrap()
        else {
            panic!("expected exact order journal");
        };
        assert!(orders
            .iter()
            .any(|order| order.reprice.is_some() && order.state == PaperSubmissionState::Terminal));
        claim_paper_reattempt(&store, &mut input);
        if attempt == 1 {
            original_closed.store(true, Ordering::SeqCst);
        }
        output = runtime
            .reconcile_existing_only(&client, &input)
            .await
            .unwrap();
    }
    assert!(output.settled);
    let payload: akzio_domain::Reconciliation = serde_json::from_slice(
        &store
            .read_blob(&output.reconciliation.reconciliation.blob)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(payload.state, akzio_domain::ReconciliationState::Complete);
    assert_eq!(payload.broker_receipts.len(), 2);
    assert_eq!(
        payload.achieved_target.unwrap().weights[&Asset::Qqq].0,
        100_000
    );
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("PATCH "))
            .count(),
        1
    );
    assert!(!requests
        .iter()
        .any(|request| request.starts_with("POST ") || request.starts_with("DELETE ")));
    server.abort();
}

/// WHY THIS TEST EXISTS (Task #30): A durable protective cancel whose second exact read finds r0 Replaced must preserve the raw receipt, leave DELETE unsent, and keep observing the existing r1.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_transport_defers_cancel_when_original_becomes_replaced() {
    use akzio_domain::LifecycleEventType;
    use std::sync::atomic::Ordering;

    let (store, mut input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    seed_replaceable_paper_orders(&store, &input, &commitment);
    let (client, requests, start_cancel, server) = paper_transport_fixture(
        &store,
        &input.commitment,
        &commitment,
        PaperTransportReply::ReplacedDuringCancel,
    )
    .await;
    let runtime = PaperDispatchRuntime {
        settlement_timeout: std::time::Duration::ZERO,
        settlement_action_grace: std::time::Duration::ZERO,
        ..PaperDispatchRuntime::new(store.clone())
    };
    let output = runtime.dispatch(&client, &input).await.unwrap();
    assert!(!output.settled);
    let original_id = commitment.client_order_ids[&Asset::Qqq].clone();
    let artifact = stage_protective_cancel(&store, &input, &output, &original_id);
    claim_paper_reattempt(&store, &mut input);
    start_cancel.store(true, Ordering::SeqCst);
    let stopped = runtime.reconcile_existing_only(&client, &input).await;
    assert!(
        matches!(
            &stopped,
            Err(PaperDispatchError::RecordedEffectObservationUnavailable(_))
        ),
        "{stopped:?}"
    );
    let Some(PaperSubmissionBatch::Journaled { orders, .. }) =
        store.paper_submission_batch(&input.commitment).unwrap()
    else {
        panic!("expected exact journal after the deferred cancel");
    };
    let original = orders
        .iter()
        .find(|order| order.client_order_id == original_id)
        .unwrap();
    let observed = store
        .artifact(&original.receipt.as_ref().unwrap().artifact_id)
        .unwrap();
    let observed: OrderReceipt =
        serde_json::from_slice(&store.read_blob(&observed.blob).unwrap()).unwrap();
    assert_eq!(observed.state, OrderReceiptState::Replaced);
    assert!(store
        .paper_effect_intent_recorded(&input.permit.run_id, &artifact.artifact_id)
        .unwrap());
    claim_paper_reattempt(&store, &mut input);
    assert!(
        runtime
            .reconcile_existing_only(&client, &input)
            .await
            .unwrap()
            .settled
    );
    assert!(!store
        .recent_events(&input.permit.run_id, 100)
        .unwrap()
        .iter()
        .any(|event| {
            event.artifact_id.as_ref() == Some(&artifact.artifact_id)
                && (event.event_type == LifecycleEventType::ExecutionEffectSettled.as_str()
                    || event.event_type == LifecycleEventType::ExecutionEffectRecovered.as_str())
        }));
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.starts_with("PATCH "))
            .count(),
        1
    );
    assert!(!requests
        .iter()
        .any(|request| request.starts_with("POST ") || request.starts_with("DELETE ")));
    server.abort();
}

fn stage_protective_cancel(
    store: &Store,
    input: &PaperDispatchInput,
    output: &PaperDispatchOutput,
    original_id: &str,
) -> Artifact {
    let original = output
        .reconciliation
        .receipts
        .iter()
        .find(|artifact| {
            let receipt: OrderReceipt =
                serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
            receipt.client_order_id == original_id
        })
        .unwrap();
    let prior_receipt = artifact_ref(original);
    let now = Utc::now();
    let cancel = PaperCancel {
        schema_version: DOMAIN_SCHEMA_VERSION,
        cancel_id: PaperCancelId::new(),
        commitment: input.commitment.clone(),
        prior_receipt: prior_receipt.clone(),
        asset: Asset::Qqq,
        client_order_id: original_id.to_owned(),
        broker_order_id: "offline-order-one".into(),
        reason: PaperCancelReason::RiskReduction,
        created_at: now,
    };
    let artifact = Artifact::new(
        ArtifactKind::ExecutionCancel,
        store.stage_json(&cancel).unwrap(),
        "execution.cancel",
        ArtifactLifecycle::Canonical,
        crate::trusted_execution_provenance(&input.permit, now),
        Some(input.permit.artifact_origin()),
        vec![input.commitment.clone(), prior_receipt],
        now,
    )
    .unwrap();
    store
        .commit_execution_cancel_intent(&input.lease, &input.permit, &artifact, now)
        .unwrap();
    artifact
}

/// WHY THIS TEST EXISTS (Task #30): A valid first legacy GET receipt must be committed before a later sibling GET can fail, without enrolling legacy IDs.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_legacy_transport_persists_partial_before_second_lookup_fails() {
    use std::sync::atomic::Ordering;

    let (store, mut input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    store
        .record_paper_effect_intent(&input.lease, &input.permit, &input.commitment, input.now)
        .unwrap();
    let (client, requests, original_is_missing, server) = paper_transport_fixture(
        &store,
        &input.commitment,
        &commitment,
        PaperTransportReply::LegacyPartialThenUnavailable,
    )
    .await;
    let runtime = PaperDispatchRuntime::new(store.clone());
    for original_is_missing_this_attempt in [false, false, true] {
        if original_is_missing_this_attempt {
            claim_paper_reattempt(&store, &mut input);
            original_is_missing.store(true, Ordering::SeqCst);
        }
        let result = runtime.reconcile_existing_only(&client, &input).await;
        let Err(PaperDispatchError::RecordedEffectObservationUnavailable(PaperError::Http {
            body,
            ..
        })) = result
        else {
            panic!("{result:?}");
        };
        let evidence: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(evidence["first_receipt_already_saved"], true);
        assert_legacy_progress(&store, &input, akzio_domain::ReconciliationState::Partial);
    }
    let raw = legacy_raw_receipts(&store, &input);
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].state, OrderReceiptState::PartiallyFilled);
    assert_eq!(raw[0].filled_quantity_micros, 400_000);
    assert_eq!(
        raw[0].client_order_id,
        commitment.client_order_ids[&Asset::Qqq]
    );
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .all(|request| request.starts_with("GET ")));
    assert_eq!(
        store.paper_submission_batch(&input.commitment).unwrap(),
        Some(PaperSubmissionBatch::LegacyUnknown)
    );
    server.abort();
}

/// WHY THIS TEST EXISTS (Task #30): A legacy protective cancel must durably preserve the second GET's real Replaced receipt and leave the original cancel unsent and unsettled.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_legacy_transport_preserves_replaced_readback_before_cancel_deferral() {
    use akzio_domain::LifecycleEventType;
    use std::sync::atomic::Ordering;

    let (store, mut input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    store
        .record_paper_effect_intent(&input.lease, &input.permit, &input.commitment, input.now)
        .unwrap();
    let (client, requests, start_cancel, server) = paper_transport_fixture(
        &store,
        &input.commitment,
        &commitment,
        PaperTransportReply::LegacyReplacedDuringCancel,
    )
    .await;
    let runtime = PaperDispatchRuntime::new(store.clone());
    let output = runtime
        .reconcile_existing_only(&client, &input)
        .await
        .unwrap();
    assert!(!output.settled);
    let original_id = &commitment.client_order_ids[&Asset::Qqq];
    let cancel = stage_protective_cancel(&store, &input, &output, original_id);
    claim_paper_reattempt(&store, &mut input);
    start_cancel.store(true, Ordering::SeqCst);
    let stopped = runtime.reconcile_existing_only(&client, &input).await;
    assert!(
        matches!(
            stopped,
            Err(PaperDispatchError::RecordedEffectObservationUnavailable(_))
        ),
        "{stopped:?}"
    );
    let raw = legacy_raw_receipts(&store, &input);
    assert_eq!(raw.len(), 2);
    assert!(raw
        .iter()
        .all(|receipt| receipt.client_order_id == *original_id));
    assert!(raw
        .iter()
        .any(|receipt| receipt.state == OrderReceiptState::Replaced));
    assert_legacy_progress(&store, &input, akzio_domain::ReconciliationState::Pending);
    assert!(store
        .paper_effect_intent_recorded(&input.permit.run_id, &cancel.artifact_id)
        .unwrap());
    assert!(!store
        .recent_events(&input.permit.run_id, 100)
        .unwrap()
        .iter()
        .any(|event| {
            event.artifact_id.as_ref() == Some(&cancel.artifact_id)
                && (event.event_type == LifecycleEventType::ExecutionEffectSettled.as_str()
                    || event.event_type == LifecycleEventType::ExecutionEffectRecovered.as_str())
        }));
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .all(|request| request.starts_with("GET ")));
    assert_eq!(
        store.paper_submission_batch(&input.commitment).unwrap(),
        Some(PaperSubmissionBatch::LegacyUnknown)
    );
    server.abort();
}

/// WHY THIS TEST EXISTS (Task #30): A visible legacy r1 is durable even when r0 returns 404, but a historical Accepted r0 does not permit settlement or resend.
/// Do not weaken or delete this assertion.
#[tokio::test]
async fn real_legacy_transport_keeps_replacement_pending_without_original_closure() {
    let (store, mut input) = build_paper_commitment_fixture(true, true);
    let commitment = journaled_commitment(&store, &input);
    let original = commitment.client_order_ids[&Asset::Qqq].clone();
    let successor = replacement_client_order_id(&original);
    store
        .record_paper_effect_intent(&input.lease, &input.permit, &input.commitment, input.now)
        .unwrap();
    let reprice_ref = stage_legacy_reprice(&store, &input, &commitment, &original);
    let (client, requests, _, server) = paper_transport_fixture(
        &store,
        &input.commitment,
        &commitment,
        PaperTransportReply::LegacyReplacementOriginalMissing,
    )
    .await;
    let runtime = PaperDispatchRuntime::new(store.clone());
    for _ in 0..2 {
        claim_paper_reattempt(&store, &mut input);
        let output = runtime
            .reconcile_existing_only(&client, &input)
            .await
            .unwrap();
        assert!(!output.settled);
        let progress: akzio_domain::Reconciliation = serde_json::from_slice(
            &store
                .read_blob(&output.reconciliation.reconciliation.blob)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(progress.state, akzio_domain::ReconciliationState::Pending);
        assert!(progress.unsubmitted_orders.is_empty());
        let raw = store
            .legacy_paper_submission_receipts(&input.commitment)
            .unwrap();
        assert_eq!(raw.len(), 2);
        assert!(raw
            .iter()
            .all(|(_, receipt)| receipt.client_order_id != original));
        let (reference, receipt) = raw
            .iter()
            .find(|(_, receipt)| receipt.client_order_id == successor)
            .unwrap();
        assert_eq!(receipt.state, OrderReceiptState::Filled);
        let artifact = store.artifact(&reference.artifact_id).unwrap();
        assert_eq!(artifact.source_refs.len(), 2);
        assert!(artifact.source_refs.contains(&input.commitment));
        assert!(artifact.source_refs.contains(&reprice_ref));
    }
    assert_eq!(
        store.paper_submission_batch(&input.commitment).unwrap(),
        Some(PaperSubmissionBatch::LegacyUnknown)
    );
    let requests = requests.lock().unwrap();
    assert!(requests.iter().all(|request| request.starts_with("GET ")));
    for id in [&original, &successor] {
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.ends_with(id))
                .count(),
            2
        );
    }
    server.abort();
}

fn stage_legacy_reprice(
    store: &Store,
    input: &PaperDispatchInput,
    commitment: &PaperCommitment,
    original: &str,
) -> ArtifactRef {
    let now = input.now;
    let runtime = ReconciliationRuntime::new(store.clone());
    let output = runtime
        .reconcile(&ReconciliationInput {
            permit: input.permit.clone(),
            commitment: input.commitment.clone(),
            reprices: Vec::new(),
            cancels: Vec::new(),
            broker_receipts: vec![OrderReceipt {
                plan_hash: commitment.plan_hash.clone(),
                asset: Asset::Qqq,
                client_order_id: original.into(),
                broker_order_id: "offline-order-one".into(),
                state: OrderReceiptState::Accepted,
                requested_quantity_micros: 1_000_000,
                filled_quantity_micros: 0,
                remaining_quantity_micros: 1_000_000,
                average_fill_price: None,
                broker_updated_at: now,
                reason: None,
                observed_at: now,
            }],
            now,
        })
        .unwrap();
    let prior = artifact_ref(&output.receipts[0]);
    runtime
        .write_progress(&input.lease, &input.permit, &output, now)
        .unwrap();
    let reprice = PaperReprice {
        schema_version: DOMAIN_SCHEMA_VERSION,
        reprice_id: akzio_domain::PaperRepriceId::new(),
        commitment: input.commitment.clone(),
        prior_receipt: prior.clone(),
        asset: Asset::Qqq,
        prior_client_order_id: original.into(),
        replacement_client_order_id: replacement_client_order_id(original),
        prior_broker_order_id: "offline-order-one".into(),
        replacement_limit_price: MoneyMicros(1_000_000),
        created_at: now,
    };
    let artifact = Artifact::new(
        ArtifactKind::ExecutionReprice,
        store.stage_json(&reprice).unwrap(),
        "execution.reprice",
        ArtifactLifecycle::Canonical,
        crate::trusted_execution_provenance(&input.permit, now),
        Some(input.permit.artifact_origin()),
        vec![input.commitment.clone(), prior],
        now,
    )
    .unwrap();
    store
        .commit_execution_reprice_intent(&input.lease, &input.permit, &artifact, now)
        .unwrap();
    let reference = artifact_ref(&artifact);
    store
        .record_paper_effect_intent(&input.lease, &input.permit, &reference, now)
        .unwrap();
    reference
}

fn assert_legacy_progress(
    store: &Store,
    input: &PaperDispatchInput,
    state: akzio_domain::ReconciliationState,
) {
    let latest = store
        .legacy_paper_submission_receipts(&input.commitment)
        .unwrap();
    let [(raw_ref, raw)] = latest.as_slice() else {
        panic!("{latest:?}");
    };
    let progress = store
        .run_artifacts_by_kind(&input.permit.run_id, ArtifactKind::Reconciliation)
        .unwrap()
        .into_iter()
        .find(|artifact| artifact.origin.as_ref() == Some(&input.permit.artifact_origin()))
        .unwrap();
    let progress: akzio_domain::Reconciliation =
        serde_json::from_slice(&store.read_blob(&progress.blob).unwrap()).unwrap();
    assert_eq!(progress.state, state);
    assert!(progress.unsubmitted_orders.is_empty());
    let [derived_ref] = progress.broker_receipts.as_slice() else {
        panic!("{progress:?}");
    };
    let derived = store.artifact(&derived_ref.artifact_id).unwrap();
    assert_eq!(derived.producer, "execution.order_receipt");
    assert!(derived.source_refs.contains(raw_ref));
    let derived: OrderReceipt =
        serde_json::from_slice(&store.read_blob(&derived.blob).unwrap()).unwrap();
    assert_eq!(&derived, raw);
}

fn legacy_raw_receipts(store: &Store, input: &PaperDispatchInput) -> Vec<OrderReceipt> {
    store
        .run_artifacts_by_kind(&input.permit.run_id, ArtifactKind::OrderReceipt)
        .unwrap()
        .iter()
        .filter(|artifact| artifact.producer == "execution.submission_receipt")
        .map(|artifact| {
            assert_eq!(
                artifact.source_refs,
                std::slice::from_ref(&input.commitment)
            );
            let origin = artifact.origin.as_ref().unwrap();
            assert_eq!(origin.run_id.as_ref(), Some(&input.permit.run_id));
            assert_eq!(origin.task_id.as_ref(), Some(&input.permit.task_id));
            serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap()
        })
        .collect()
}
