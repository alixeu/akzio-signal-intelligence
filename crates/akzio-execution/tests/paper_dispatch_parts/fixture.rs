fn reserve_approved_slot(
    store: &V2Store,
    lease: &DaemonLease,
    workflow: WorkflowCommit,
    now: DateTime<Utc>,
) {
    let run_id = workflow.run.run_id.clone();
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: workflow.run.topology_id.clone(),
        tasks: BTreeMap::new(),
        stop_reason: Some("fixture approved Paper session".to_owned()),
    };
    let proposal = Artifact::new(
        ArtifactKind::WorkflowProposal,
        store.put_json(&proposal_payload).unwrap(),
        "runtime.paper_provisioning",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(ArtifactOrigin {
            run_id: Some(run_id),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    let session = chrono::NaiveDate::parse_from_str(SESSION_KEY, "%Y-%m-%d").unwrap();
    let manifest_payload = RuntimeManifest {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        code_revision: "fixture-revision".to_owned(),
        cargo_lock_hash: ContentHash::of_bytes(b"fixture-cargo"),
        config_hash: ContentHash::of_bytes(b"fixture-config"),
        provider_id: "fixture-provider".to_owned(),
        model_id: "fixture-model".to_owned(),
        prompt_hash: ContentHash::of_bytes(b"fixture-prompt"),
        contract_hash: ContentHash::of_bytes(b"fixture-contract"),
        topology_hash: ContentHash::of_bytes(b"fixture-topology"),
        decision_policy_hash: ContentHash::of_bytes(b"fixture-decision"),
        execution_policy_hash: ContentHash::of_bytes(b"fixture-execution"),
        evaluation_policy_hash: ContentHash::of_bytes(b"fixture-evaluation"),
        market_data_feed: "iex".to_owned(),
        broker_account_id: "fixture-account".to_owned(),
        maximum_notional: MoneyMicros::from_usd_cents(100_000),
        allowed_session_start: session,
        allowed_session_end: session,
        expires_at: now + Duration::hours(8),
        created_at: now,
    };
    let manifest_hash = manifest_payload.manifest_hash().unwrap();
    let manifest = Artifact::new(
        ArtifactKind::RuntimeManifest,
        store.put_json(&manifest_payload).unwrap(),
        "runtime.manifest",
        ArtifactLifecycle::Canonical,
        provenance(now),
        None,
        vec![],
        now,
    )
    .unwrap();
    let mut approval_payload = PaperLaunchApproval {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        operator_identity: "fixture-operator".to_owned(),
        runtime_manifest: artifact_ref(&manifest),
        runtime_manifest_hash: manifest_hash,
        scope: PaperApprovalScope::Canary,
        reason: "fixture approval".to_owned(),
        approved_at: now,
        expires_at: manifest_payload.expires_at,
        approval_hash: ContentHash::of_bytes(b"pending"),
    };
    approval_payload.approval_hash = approval_payload.unsigned_hash().unwrap();
    let approval = Artifact::new(
        ArtifactKind::PaperLaunchApproval,
        store.put_json(&approval_payload).unwrap(),
        "operator.paper_approval",
        ArtifactLifecycle::Canonical,
        provenance(now),
        None,
        vec![approval_payload.runtime_manifest.clone()],
        now,
    )
    .unwrap();
    store
        .reserve_paper_session_with_approval(
            lease,
            &SessionReservation {
                session_key: SESSION_KEY.to_owned(),
                workflow,
                setup_artifacts: vec![],
                reserved_at: now,
            },
            &proposal,
            &manifest,
            &approval,
        )
        .unwrap();
}

struct PreparedCommitment {
    _directory: TempDir,
    store: V2Store,
    now: DateTime<Utc>,
    lease: DaemonLease,
    commitment: Artifact,
}

fn prepared_commitment() -> PreparedCommitment {
    let directory = tempdir().unwrap();
    let store = V2Store::open(directory.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease(
            "scheduler",
            "fixture-daemon",
            now,
            now + Duration::seconds(30),
        )
        .unwrap()
        .unwrap();
    let graph = workflow();
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.put_json(&graph).unwrap(),
        "fixture.workflow",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        None,
        vec![],
        now,
    )
    .unwrap();
    reserve_approved_slot(
        &store,
        &lease,
        WorkflowCommit {
            run: StoredRun {
                run_id: RunId::new(),
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        },
        now,
    );
    let permit = store
        .claim_next_task("commitment-worker", now, Duration::seconds(30))
        .unwrap()
        .unwrap()
        .permit;
    let decision_context = write_source(
        &store,
        &permit,
        ArtifactKind::DecisionContext,
        "decision-context",
        now,
    );
    let account_snapshot = write_source(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        "account",
        now,
    );
    let quote_snapshot = write_source(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        "quote",
        now,
    );
    let market_clock_snapshot = write_source(
        &store,
        &permit,
        ArtifactKind::NormalizedEvidence,
        "clock",
        now,
    );
    let allocation = plan(
        now,
        decision_context,
        account_snapshot,
        quote_snapshot,
        market_clock_snapshot,
    );
    let allocation_artifact = Artifact::new(
        ArtifactKind::ExecutionPlan,
        store.put_json(&allocation).unwrap(),
        "fixture.allocation",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&permit)),
        vec![
            allocation.decision_context.clone(),
            allocation.account_snapshot.clone(),
            allocation.quote_snapshot.clone(),
            allocation.market_clock_snapshot.clone(),
        ],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &permit,
            &allocation_artifact,
            LifecycleEventType::ExecutionAllocationCreated,
            now,
        )
        .unwrap();
    let allocation_ref = artifact_ref(&allocation_artifact);
    let context = ExecutionContext {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        run_id: permit.run_id.clone(),
        decision_context: allocation.decision_context.clone(),
        account_snapshot: Some(allocation.account_snapshot.clone()),
        quote_snapshot: Some(allocation.quote_snapshot.clone()),
        market_clock_snapshot: Some(allocation.market_clock_snapshot.clone()),
        execution_plan: Some(allocation_ref.clone()),
        factor_exposure: Some(allocation.factor_exposure.clone()),
        turnover_ppm: Some(allocation.turnover_ppm),
        plan_hash: Some(allocation.plan_hash.clone()),
        broker_session: Some(allocation.broker_session.clone()),
        frozen: false,
        created_at: now,
    };
    context.validate_complete_plan_closure().unwrap();
    let context_artifact = Artifact::new(
        ArtifactKind::ExecutionContext,
        store.put_json(&context).unwrap(),
        "fixture.execution_context",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&permit)),
        vec![
            context.decision_context.clone(),
            context.account_snapshot.clone().unwrap(),
            context.quote_snapshot.clone().unwrap(),
            context.market_clock_snapshot.clone().unwrap(),
            allocation_ref,
        ],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &permit,
            &context_artifact,
            LifecycleEventType::ExecutionContextCreated,
            now,
        )
        .unwrap();
    let context_ref = artifact_ref(&context_artifact);
    let verdict = Artifact::new(
        ArtifactKind::ExecutionVerdict,
        store
            .put_json(&ExecutionVerdict::Accepted {
                execution_context: context_ref.clone(),
            })
            .unwrap(),
        "fixture.execution_verdict",
        ArtifactLifecycle::RunScoped,
        provenance(now),
        Some(origin(&permit)),
        vec![context_ref],
        now,
    )
    .unwrap();
    store
        .write_task_artifact(
            &permit,
            &verdict,
            LifecycleEventType::ExecutionVerdictCreated,
            now,
        )
        .unwrap();
    let commitment = V2PaperCommitmentRuntime::new(store.clone())
        .commit(&PaperCommitmentInput {
            lease: lease.clone(),
            permit,
            verdict: artifact_ref(&verdict),
            session_key: SESSION_KEY.to_owned(),
            now,
        })
        .unwrap()
        .commitment;
    PreparedCommitment {
        _directory: directory,
        store,
        now,
        lease,
        commitment,
    }
}
