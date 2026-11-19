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
                side: OrderSide::Sell,
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
        plan_hash: plan.plan_hash,
        broker_session: broker_session.clone(),
        client_order_ids: [
            (Asset::Qqq, "original-qqq".into()),
            (Asset::Soxx, "original-soxx".into()),
        ]
        .into(),
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
            Ok(Self::execution(commitment))
        })
    }

    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
        authorization: &'a PaperSubmissionAuthorization,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            authorization.assert_current(&plan.plan_hash, Utc::now())?;
            self.calls.lock().unwrap().push("execute");
            Ok(Self::execution(commitment))
        })
    }

    fn reconcile_commitment<'a>(
        &'a self,
        _: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("reconcile");
            Ok(execution.clone())
        })
    }

    fn cancel_order<'a>(
        &'a self,
        _: &'a PaperCancel,
    ) -> Pin<Box<dyn Future<Output = crate::paper::Result<PaperOrderReceipt>> + Send + 'a>> {
        panic!("fixture must not cancel an order")
    }

    fn replace_order<'a>(
        &'a self,
        _: &'a PaperReprice,
        _: &'a PaperSubmissionAuthorization,
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
