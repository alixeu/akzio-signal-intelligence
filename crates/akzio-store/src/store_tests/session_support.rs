fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn reserve_approved_test_session(
    store: &V2Store,
    lease: &DaemonLease,
    reservation: &SessionReservation,
) -> SessionSlotReservation {
    reserve_approved_test_session_with_limits(
        store,
        lease,
        reservation,
        MoneyMicros::from_usd_cents(100_000),
        reservation.reserved_at + Duration::hours(8),
    )
}

fn reserve_approved_test_session_with_limits(
    store: &V2Store,
    lease: &DaemonLease,
    reservation: &SessionReservation,
    maximum_notional: MoneyMicros,
    expires_at: DateTime<Utc>,
) -> SessionSlotReservation {
    let now = reservation.reserved_at;
    let run = &reservation.workflow.run;
    let provenance = ArtifactProvenance {
        source_family: "fixture.paper_approval".to_owned(),
        observed_at: None,
        retrieved_at: now,
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: None,
    };
    let proposal_payload = WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: run.topology_id.clone(),
        tasks: BTreeMap::new(),
        stop_reason: Some("fixture approved Paper session".to_owned()),
    };
    let proposal = Artifact::new(
        ArtifactKind::WorkflowProposal,
        store.put_json(&proposal_payload).unwrap(),
        "runtime.paper_provisioning",
        ArtifactLifecycle::RunScoped,
        provenance.clone(),
        Some(ArtifactOrigin {
            run_id: Some(run.run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )
    .unwrap();
    let session = NaiveDate::parse_from_str(&reservation.session_key, "%Y-%m-%d").unwrap();
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
        maximum_notional,
        allowed_session_start: session,
        allowed_session_end: session,
        expires_at,
        created_at: now,
    };
    let manifest_hash = manifest_payload.manifest_hash().unwrap();
    let manifest = Artifact::new(
        ArtifactKind::RuntimeManifest,
        store.put_json(&manifest_payload).unwrap(),
        "runtime.manifest",
        ArtifactLifecycle::Canonical,
        provenance.clone(),
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
        provenance,
        None,
        vec![approval_payload.runtime_manifest.clone()],
        now,
    )
    .unwrap();
    store
        .reserve_paper_session_with_approval(lease, reservation, &proposal, &manifest, &approval)
        .unwrap()
}

fn valid_execution_commitment(
    store: &V2Store,
    permit: &TaskWritePermit,
    session_key: &str,
    now: DateTime<Utc>,
) -> Artifact {
    let source = |kind, name: &'static [u8]| {
        let artifact = permit_artifact(
            store,
            permit,
            kind,
            &serde_json::json!({"fixture": String::from_utf8_lossy(name)}),
            vec![],
            ArtifactLifecycle::RunScoped,
            now,
        );
        store
            .write_task_artifact(
                permit,
                &artifact,
                LifecycleEventType::FixtureSourceCreated,
                now,
            )
            .unwrap();
        artifact_ref(&artifact)
    };
    let decision_context = source(ArtifactKind::DecisionContext, b"decision-context");
    let account_snapshot = source(ArtifactKind::NormalizedEvidence, b"account");
    let quote_snapshot = source(ArtifactKind::NormalizedEvidence, b"quote");
    let market_clock_snapshot = source(ArtifactKind::NormalizedEvidence, b"market-clock");

    let mut target = TargetPortfolio::zeroed();
    target.weights.insert(Asset::Qqq, WeightPpm(100_000));
    let mut plan_payload = ExecutionPlan {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        decision_context: decision_context.clone(),
        account_snapshot: account_snapshot.clone(),
        quote_snapshot: quote_snapshot.clone(),
        market_clock_snapshot: market_clock_snapshot.clone(),
        policy_hash: ContentHash::of_bytes(b"fixture-policy"),
        maximum_total_notional: MoneyMicros::from_usd_cents(100_000),
        target: target.clone(),
        orders: vec![OrderIntent {
            asset: Asset::Qqq,
            side: OrderSide::Buy,
            notional: MoneyMicros::from_usd_cents(10_000),
            limit_price: MoneyMicros::from_usd_cents(5_000),
        }],
        gross_exposure_ppm: 100_000,
        net_exposure_ppm: 100_000,
        factor_exposure: FactorExposure::from_target(&target).unwrap(),
        turnover_ppm: 100_000,
        broker_session: session_key.to_owned(),
        created_at: now,
        plan_hash: ContentHash::of_bytes(b"pending"),
    };
    plan_payload.refresh_hash().unwrap();
    let plan_hash = plan_payload.plan_hash.clone();
    let plan = permit_artifact(
        store,
        permit,
        ArtifactKind::ExecutionPlan,
        &plan_payload,
        vec![
            decision_context.clone(),
            account_snapshot.clone(),
            quote_snapshot.clone(),
            market_clock_snapshot.clone(),
        ],
        ArtifactLifecycle::RunScoped,
        now,
    );
    store
        .write_task_artifact(permit, &plan, LifecycleEventType::ExecutionPlanCreated, now)
        .unwrap();
    let plan_ref = artifact_ref(&plan);
    let context = permit_artifact(
        store,
        permit,
        ArtifactKind::ExecutionContext,
        &ExecutionContext {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            run_id: permit.run_id.clone(),
            decision_context: decision_context.clone(),
            account_snapshot: Some(account_snapshot.clone()),
            quote_snapshot: Some(quote_snapshot.clone()),
            market_clock_snapshot: Some(market_clock_snapshot.clone()),
            execution_plan: Some(plan_ref.clone()),
            factor_exposure: Some(plan_payload.factor_exposure.clone()),
            turnover_ppm: Some(plan_payload.turnover_ppm),
            plan_hash: Some(plan_hash.clone()),
            broker_session: Some(session_key.to_owned()),
            frozen: false,
            created_at: now,
        },
        vec![
            decision_context,
            account_snapshot,
            quote_snapshot,
            market_clock_snapshot,
            plan_ref,
        ],
        ArtifactLifecycle::RunScoped,
        now,
    );
    store
        .write_task_artifact(
            permit,
            &context,
            LifecycleEventType::ExecutionContextCreated,
            now,
        )
        .unwrap();
    let context_ref = artifact_ref(&context);
    let verdict = permit_artifact(
        store,
        permit,
        ArtifactKind::ExecutionVerdict,
        &ExecutionVerdict::Accepted {
            execution_context: context_ref.clone(),
        },
        vec![context_ref.clone()],
        ArtifactLifecycle::RunScoped,
        now,
    );
    store
        .write_task_artifact(
            permit,
            &verdict,
            LifecycleEventType::ExecutionVerdictCreated,
            now,
        )
        .unwrap();
    permit_artifact(
        store,
        permit,
        ArtifactKind::ExecutionCommitment,
        &PaperCommitment {
            commitment_id: PaperCommitmentId::new(),
            execution_context: context_ref.clone(),
            plan_hash,
            broker_session: session_key.to_owned(),
            client_order_ids: std::collections::BTreeMap::from([(
                Asset::Qqq,
                "fixture-order".to_owned(),
            )]),
            created_at: now,
        },
        vec![artifact_ref(&verdict), context_ref],
        ArtifactLifecycle::Canonical,
        now,
    )
}

struct ExecutionCommitFixture {
    _root: tempfile::TempDir,
    store: V2Store,
    lease: DaemonLease,
    permit: TaskWritePermit,
    commitment: Artifact,
    now: DateTime<Utc>,
}
