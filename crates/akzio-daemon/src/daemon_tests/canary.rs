fn paired_canary_fields(
    campaign_id: &ContentHash,
    parent_contract_hash: &ContentHash,
    candidate_contract_hash: &ContentHash,
    parent_topology_id: &str,
    candidate_topology_id: &str,
    first_market_day: chrono::NaiveDate,
) -> (
    akzio_domain::CanaryPromotionPolicy,
    Vec<akzio_domain::CanaryCohortManifest>,
) {
    let policy = akzio_domain::CanaryPromotionPolicy {
        minimum_evidence_completeness_ppm: 900_000,
        minimum_risk_recall_ppm: 900_000,
        required_paired_sessions_per_horizon: [2, 2, 2],
        minimum_distinct_market_days: 2,
        required_regimes: ["risk_on".to_owned(), "risk_off".to_owned()]
            .into_iter()
            .collect(),
        minimum_cost_adjusted_utility_delta_ppm: 100,
        maximum_drawdown_delta_ppm: 1_000,
        maximum_tail_loss_delta_ppm: 1_000,
        minimum_confidence_ppm: 800_000,
    };
    let second_market_day = first_market_day.succ_opt().unwrap();
    let cohorts = akzio_domain::CanaryCampaignStatus::LEVELS
        .into_iter()
        .map(|stage| {
            akzio_domain::CanaryCohortManifest {
                schema_version: V2_DOMAIN_SCHEMA_VERSION,
                cohort_id: ContentHash::of_bytes(b"pending"),
                campaign_id: campaign_id.clone(),
                parent_contract_hash: parent_contract_hash.clone(),
                candidate_contract_hash: candidate_contract_hash.clone(),
                parent_topology_id: akzio_domain::TopologyId(parent_topology_id.to_owned()),
                candidate_topology_id: akzio_domain::TopologyId(candidate_topology_id.to_owned()),
                validation_stage: stage,
                observation_start: first_market_day,
                observation_end: second_market_day,
                asset_universe: akzio_domain::Asset::EXECUTABLE.into_iter().collect(),
                cost_model: akzio_domain::OutcomeCostModel::default(),
                market_calendar_id: ContentHash::of_bytes(b"fixture-market-calendar"),
                market_regimes: std::collections::BTreeMap::from([
                    (first_market_day, "risk_on".to_owned()),
                    (second_market_day, "risk_off".to_owned()),
                ]),
                generation_dataset_id: ContentHash::of_bytes(b"fixture-generation-dataset"),
                promotion_dataset_id: ContentHash::of_bytes(b"fixture-promotion-dataset"),
                promotion_policy_hash: policy.identity_hash(),
            }
            .seal()
        })
        .collect();
    (policy, cohorts)
}

fn paired_observations(
    spec: &akzio_domain::CanaryCampaignSpec,
    stage: akzio_domain::CanaryCampaignStatus,
    sessions: &[akzio_store::v2::StoredCanarySession],
) -> Vec<akzio_domain::CanaryPairedObservation> {
    let cohort = spec.cohort(stage).unwrap();
    sessions
        .iter()
        .flat_map(|session| {
            OutcomeHorizon::ALL.into_iter().map(move |horizon| {
                let market_day = session.reservation.market_day.unwrap();
                let offset = match horizon {
                    OutcomeHorizon::T1 => 1,
                    OutcomeHorizon::T3 => 3,
                    OutcomeHorizon::T5 => 7,
                };
                let observed_trading_day = market_day + chrono::Duration::days(offset);
                let parent = akzio_domain::CanaryPairedOutcomeMetrics {
                    observed_trading_day,
                    evidence_completeness_ppm: 950_000,
                    risk_recall_ppm: 950_000,
                    cost_adjusted_utility_ppm: 1_000,
                    drawdown_ppm: 1_000,
                    tail_loss_ppm: 1_000,
                    confidence_ppm: 900_000,
                };
                let candidate = akzio_domain::CanaryPairedOutcomeMetrics {
                    cost_adjusted_utility_ppm: 1_200,
                    drawdown_ppm: 1_100,
                    tail_loss_ppm: 1_100,
                    ..parent
                };
                let comparison = akzio_domain::CanaryPairedSubjectMetrics { parent, candidate };
                akzio_domain::CanaryPairedObservation {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    cohort_id: cohort.cohort_id.clone(),
                    session_key: session.reservation.session_key.clone(),
                    market_day,
                    regime: session.reservation.regime.clone().unwrap(),
                    horizon,
                    asset_universe: cohort.asset_universe.clone(),
                    cost_model: cohort.cost_model,
                    market_calendar_id: cohort.market_calendar_id.clone(),
                    generation_dataset_id: cohort.generation_dataset_id.clone(),
                    promotion_dataset_id: cohort.promotion_dataset_id.clone(),
                    contract: comparison,
                    topology: comparison,
                    bundle: comparison,
                }
            })
        })
        .collect()
}

#[tokio::test]
async fn staged_canary_campaign_blocks_paper_scheduler() {
    let directory = tempdir().unwrap();
    let daemon = Daemon::with_model(
        config(directory.path().to_path_buf()),
        fixture_model_client(),
    )
    .unwrap();
    let now = chrono::DateTime::parse_from_rfc3339("2026-08-31T14:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let (manifest, approval) = install_test_paper_approval_range(
        daemon.store(),
        now.date_naive(),
        now.date_naive().succ_opt().unwrap(),
        now,
        chrono::Duration::hours(48),
    );
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
    let campaign_id = ContentHash::of_bytes(b"staged-canary");
    let (promotion_policy, cohorts) = paired_canary_fields(
        &campaign_id,
        &active_contract.contract.contract_hash,
        &active_contract.contract.contract_hash,
        "paper-fixture",
        "canary-candidate",
        now.date_naive(),
    );
    let spec = akzio_domain::CanaryCampaignSpec {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        campaign_id,
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
        promotion_policy: Some(promotion_policy),
        cohorts,
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
    let now = chrono::DateTime::parse_from_rfc3339("2026-08-31T14:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let (manifest, approval) = install_test_paper_approval_range(
        daemon.store(),
        now.date_naive(),
        now.date_naive().succ_opt().unwrap(),
        now,
        chrono::Duration::hours(48),
    );
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
    let campaign_id = ContentHash::of_bytes(b"atomic-canary");
    let (promotion_policy, cohorts) = paired_canary_fields(
        &campaign_id,
        &active_contract.contract.contract_hash,
        &candidate_contract.contract.contract_hash,
        "paper-fixture",
        akzio_domain::STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID,
        now.date_naive(),
    );
    let spec = akzio_domain::CanaryCampaignSpec {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        campaign_id,
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
        promotion_policy: Some(promotion_policy),
        cohorts,
        created_at: now,
    };
    daemon.stage_canary_campaign(spec.clone()).unwrap();
    let resumed = daemon.resume_canary_campaign(&spec.campaign_id).unwrap();
    assert_eq!(
        resumed.status,
        akzio_domain::CanaryCampaignStatus::ValidationStage1
    );

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
            akzio_domain::CanaryCampaignStatus::ValidationStage1,
        )
        .unwrap()
        .unwrap();

    assert!(reservation.newly_reserved);
    assert_eq!(
        stored_canary.reservation.parent_run_id,
        reservation.slot.workflow.run.run_id
    );
    assert_eq!(
        stored_canary.reservation.cohort_id.as_ref(),
        Some(
            &spec
                .cohort(akzio_domain::CanaryCampaignStatus::ValidationStage1)
                .unwrap()
                .cohort_id
        )
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

    let restarted = PaperScheduler::new(
        daemon.store().clone(),
        daemon.workflow.clone(),
        "canary-restart-owner".to_owned(),
    )
    .unwrap();
    let same_session = restarted
        .tick(&clock, &source, now + chrono::Duration::seconds(31))
        .await
        .unwrap()
        .unwrap();
    assert!(!same_session.newly_reserved);
    assert_eq!(
        same_session.slot.workflow.run.run_id,
        reservation.slot.workflow.run.run_id
    );
    assert_eq!(
        same_session
            .slot
            .workflow
            .nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<Vec<_>>(),
        reservation
            .slot
            .workflow
            .nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<Vec<_>>()
    );

    let second_market_day = now.date_naive().succ_opt().unwrap();
    install_test_paper_approval(
        daemon.store(),
        second_market_day,
        now + chrono::Duration::days(1),
    );
    let second_clock = StaticSessionClock(Some(second_market_day.to_string()));
    let second = restarted
        .tick(&second_clock, &source, now + chrono::Duration::days(1))
        .await
        .unwrap()
        .unwrap();
    assert!(second.newly_reserved);
    assert_ne!(
        second.slot.workflow.run.run_id,
        reservation.slot.workflow.run.run_id
    );
    assert_eq!(
        daemon
            .store()
            .canary_sessions(
                &spec.campaign_id,
                akzio_domain::CanaryCampaignStatus::ValidationStage1,
            )
            .unwrap()
            .len(),
        2
    );
    let stage = akzio_domain::CanaryCampaignStatus::ValidationStage1;
    let sessions = daemon
        .store()
        .canary_sessions(&spec.campaign_id, stage)
        .unwrap();
    let observations = paired_observations(&spec, stage, &sessions);
    let lease = daemon
        .store()
        .daemon_lease(SCHEDULER_LEASE_NAME)
        .unwrap()
        .unwrap();
    let evaluation_time = now + chrono::Duration::days(1) + chrono::Duration::seconds(1);
    let first_session = observations
        .iter()
        .filter(|observation| observation.session_key == sessions[0].reservation.session_key)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        daemon
            .store()
            .record_canary_observations(
                &lease,
                &spec.campaign_id,
                stage,
                &first_session,
                evaluation_time,
            )
            .unwrap()
            .len(),
        3
    );
    let policy = spec.promotion_policy.as_ref().unwrap();
    let cohort = spec.cohort(stage).unwrap();
    assert_eq!(
        evaluate_canary_cohort(cohort, policy, &first_session, evaluation_time)
            .unwrap()
            .verdict,
        akzio_domain::CanaryVerdict::Defer
    );
    let stored_observations = daemon
        .store()
        .record_canary_observations(
            &lease,
            &spec.campaign_id,
            stage,
            &observations,
            evaluation_time,
        )
        .unwrap();
    assert_eq!(stored_observations.len(), 6);
    let mut conflicting = observations[0].clone();
    conflicting.contract.candidate.cost_adjusted_utility_ppm += 1;
    assert!(daemon
        .store()
        .record_canary_observations(
            &lease,
            &spec.campaign_id,
            stage,
            &[conflicting],
            evaluation_time,
        )
        .is_err());
    let evaluation = evaluate_canary_cohort(cohort, policy, &stored_observations, evaluation_time)
        .unwrap();
    assert_eq!(evaluation.verdict, akzio_domain::CanaryVerdict::Advance);
    let stale_lease = DaemonLease {
        epoch: lease.epoch.saturating_sub(1),
        ..lease.clone()
    };
    assert!(daemon
        .store()
        .transition_canary_campaign_with_evaluation(
            &stale_lease,
            &spec.campaign_id,
            stage,
            &evaluation,
            evaluation_time,
        )
        .is_err());
    let stale_owner = DaemonLease {
        owner_id: "stale-canary-owner".to_owned(),
        ..lease.clone()
    };
    assert!(daemon
        .store()
        .transition_canary_campaign_with_evaluation(
            &stale_owner,
            &spec.campaign_id,
            stage,
            &evaluation,
            evaluation_time,
        )
        .is_err());
    let advanced = daemon
        .store()
        .transition_canary_campaign_with_evaluation(
            &lease,
            &spec.campaign_id,
            stage,
            &evaluation,
            evaluation_time,
        )
        .unwrap();
    assert_eq!(
        advanced.status,
        akzio_domain::CanaryCampaignStatus::ValidationStage2
    );
    let repeated = daemon
        .store()
        .transition_canary_campaign_with_evaluation(
            &lease,
            &spec.campaign_id,
            stage,
            &evaluation,
            evaluation_time,
        )
        .unwrap();
    assert_eq!(repeated.revision, advanced.revision);
    daemon.store().verify_integrity().unwrap();
}
