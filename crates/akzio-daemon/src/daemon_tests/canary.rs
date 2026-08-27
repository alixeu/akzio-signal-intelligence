#[tokio::test]
async fn staged_canary_campaign_blocks_paper_scheduler() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = Utc::now();
    let (manifest, approval) = install_test_paper_approval(daemon.store(), now.date_naive(), now);
    let active_contract = daemon
        .store()
        .active_contract(&akzio_domain::ContractPurpose::new("research.analyst").unwrap())
        .unwrap()
        .unwrap();
    let candidate_topology = daemon
        .workflow
        .submit(
            RunId::new(),
            RunPurpose::Debug,
            daemon
                .workflow
                .bootstrap(RunPurpose::Debug, "canary-candidate")
                .unwrap(),
            now,
        )
        .unwrap();
    let spec = akzio_domain::CanaryCampaignSpec {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        campaign_id: ContentHash::of_bytes(b"staged-canary"),
        active_contract_hash: active_contract.contract.contract_hash.clone(),
        candidate_contract: ArtifactRef {
            artifact_id: active_contract.artifact.artifact_id.clone(),
            kind: ArtifactKind::Contract,
        },
        active_topology_id: akzio_domain::TopologyId("paper-fixture".to_owned()),
        candidate_topology: ArtifactRef {
            artifact_id: candidate_topology.artifact_id.clone(),
            kind: ArtifactKind::WorkflowGraph,
        },
        runtime_manifest: ArtifactRef {
            artifact_id: manifest.artifact_id.clone(),
            kind: ArtifactKind::RuntimeManifest,
        },
        paper_approval: ArtifactRef {
            artifact_id: approval.artifact_id.clone(),
            kind: ArtifactKind::PaperLaunchApproval,
        },
        source_revision: "fixture-revision".to_owned(),
        maximum_total_notional: MoneyMicros::from_usd_cents(100_000),
        created_at: now,
    };
    let staged = daemon.stage_canary_campaign(spec.clone()).unwrap();
    assert_eq!(staged.status, akzio_domain::CanaryCampaignStatus::Staged);

    let session_key = now.date_naive().to_string();
    let clock = StaticSessionClock(Some(session_key.clone()));
    let source = StaticPaperWorkflowSource::new(paper_proposal());
    let reservation = daemon
        .paper
        .scheduler
        .tick(&clock, &source, now)
        .await
        .unwrap();

    assert!(reservation.is_none());
    assert!(daemon.store().session_slot(&session_key).unwrap().is_none());
    assert!(daemon
        .store()
        .canary_session(
            &spec.campaign_id,
            akzio_domain::CanaryCampaignStatus::Staged
        )
        .unwrap()
        .is_none());
    daemon.store().verify_integrity().unwrap();
}

#[tokio::test]
async fn canary_scheduler_commits_parent_and_shadow_workflows_atomically() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = Utc::now();
    let (manifest, approval) = install_test_paper_approval(daemon.store(), now.date_naive(), now);
    let active_contract = daemon
        .store()
        .active_contract(&akzio_domain::ContractPurpose::new("research.analyst").unwrap())
        .unwrap()
        .unwrap();
    let mut candidate_contract = active_contract.contract.clone();
    candidate_contract.version += 1;
    candidate_contract.responsibility.push_str(" candidate");
    candidate_contract.contract_hash = candidate_contract.expected_hash().unwrap();
    let candidate_contract = daemon
        .store()
        .install_candidate_contract(
            &active_contract.contract.contract_hash,
            &candidate_contract,
            now,
        )
        .unwrap();
    let mut candidate_graph = daemon
        .workflow
        .lower_shadow(
            &daemon
                .workflow
                .approved_paper_proposal(akzio_domain::STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID)
                .unwrap(),
            None,
        )
        .unwrap();
    candidate_graph
        .nodes
        .iter_mut()
        .find(|node| node.recipe_id.as_str() == akzio_domain::RESEARCH_CRITIC_RECIPE_ID)
        .unwrap()
        .objective = "candidate-topology-marker".to_owned();
    let candidate_topology = daemon
        .workflow
        .submit(RunId::new(), RunPurpose::Shadow, candidate_graph, now)
        .unwrap();
    let spec = akzio_domain::CanaryCampaignSpec {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        campaign_id: ContentHash::of_bytes(b"atomic-canary"),
        active_contract_hash: active_contract.contract.contract_hash.clone(),
        candidate_contract: ArtifactRef {
            artifact_id: candidate_contract.artifact.artifact_id.clone(),
            kind: ArtifactKind::Contract,
        },
        active_topology_id: akzio_domain::TopologyId("paper-fixture".to_owned()),
        candidate_topology: ArtifactRef {
            artifact_id: candidate_topology.artifact_id.clone(),
            kind: ArtifactKind::WorkflowGraph,
        },
        runtime_manifest: ArtifactRef {
            artifact_id: manifest.artifact_id.clone(),
            kind: ArtifactKind::RuntimeManifest,
        },
        paper_approval: ArtifactRef {
            artifact_id: approval.artifact_id.clone(),
            kind: ArtifactKind::PaperLaunchApproval,
        },
        source_revision: "fixture-revision".to_owned(),
        maximum_total_notional: MoneyMicros::from_usd_cents(100_000),
        created_at: now,
    };
    daemon.stage_canary_campaign(spec.clone()).unwrap();
    let resumed = daemon.resume_canary_campaign(&spec.campaign_id).unwrap();
    assert_eq!(resumed.status, akzio_domain::CanaryCampaignStatus::Canary10);

    let session_key = now.date_naive().to_string();
    let clock = StaticSessionClock(Some(session_key.clone()));
    let source = StaticPaperWorkflowSource::new(paper_proposal());
    let reservation = daemon
        .paper
        .scheduler
        .tick(&clock, &source, now)
        .await
        .unwrap()
        .unwrap();
    let stored_canary = daemon
        .store()
        .canary_session(
            &spec.campaign_id,
            akzio_domain::CanaryCampaignStatus::Canary10,
        )
        .unwrap()
        .unwrap();

    assert!(reservation.newly_reserved);
    assert_eq!(
        stored_canary.reservation.parent_run_id,
        reservation.slot.workflow.run.run_id
    );
    assert_eq!(
        daemon
            .store()
            .run_purpose(&stored_canary.reservation.contract_shadow_run_id)
            .unwrap(),
        RunPurpose::Shadow
    );
    assert_eq!(
        daemon
            .store()
            .run_purpose(&stored_canary.reservation.topology_shadow_run_id)
            .unwrap(),
        RunPurpose::Shadow
    );
    assert_eq!(
        daemon
            .store()
            .run_purpose(&stored_canary.reservation.bundle_shadow_run_id)
            .unwrap(),
        RunPurpose::Shadow
    );
    assert!(daemon
        .store()
        .workflow_snapshot(&stored_canary.reservation.topology_shadow_run_id)
        .unwrap()
        .revision
        .graph
        .nodes
        .iter()
        .any(|node| node.objective == "candidate-topology-marker"));
    daemon.store().verify_integrity().unwrap();
}
