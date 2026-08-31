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
    install_test_paper_approval_range(store, session, session, now, ChronoDuration::hours(8))
}

fn install_test_paper_approval_range(
    store: &V2Store,
    session_start: NaiveDate,
    session_end: NaiveDate,
    now: DateTime<Utc>,
    validity: ChronoDuration,
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
        maximum_notional: MoneyMicros::from_usd_cents(100_000),
        allowed_session_start: session_start,
        allowed_session_end: session_end,
        expires_at: now + validity,
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
        expires_at: now + validity,
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
fn evidence_policy_failure_stays_terminal() {
    let error = DaemonError::Evidence(EvidenceRuntimeError::Adapter(
        akzio_ingest::runtime::EvidenceAdapterError::Policy {
            evidence_source: EvidenceSource::NewsWeb,
            resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
            reason: "native web citation URI is not allowlisted: https://example.com/story"
                .to_owned(),
        },
    ));
    assert_eq!(retry_cause_for_daemon_error(&error), None);
}
