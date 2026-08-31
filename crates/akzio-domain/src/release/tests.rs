use super::*;
use crate::{
    ArtifactId, ArtifactKind, CandidatePolicyState, FailureDisposition, RetryPolicy, TaskBudget,
    TaskId, TaskRecipeId, WorkflowGraph, WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
};

fn hash(seed: &str) -> ContentHash {
    ContentHash::of_bytes(seed.as_bytes())
}

fn reference(kind: ArtifactKind, seed: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: ArtifactId(hash(seed)),
        kind,
    }
}

fn workflow_plan() -> WorkflowGraph {
    WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        nodes: vec![WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash: Some(hash("contract")),
            objective: "fixture release evidence".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 50,
            budget: TaskBudget {
                max_input_tokens: 100,
                max_output_tokens: 100,
                max_wall_time_secs: 60,
                max_tool_calls: 1,
            },
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        }],
    }
}

fn complete_body(
    now: DateTime<Utc>,
    environment: ReleaseEvidenceEnvironment,
) -> ReleaseEvidenceBody {
    let trust = match environment {
        ReleaseEvidenceEnvironment::OfflineFixture => ReleaseBrokerEvidenceTrust::OfflineFixture,
        ReleaseEvidenceEnvironment::Real => ReleaseBrokerEvidenceTrust::RealBroker,
    };
    ReleaseEvidenceBody {
        run_id: RunId::new(),
        purpose: RunPurpose::Paper,
        environment,
        materialized_at: now,
        runtime: Some(ReleaseRuntimeEvidence {
            repository_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            dirty_worktree: false,
            config_hash: hash("config"),
            prompt_hash: hash("prompt"),
            contract_hash: hash("contract"),
            topology_hash: hash("topology"),
        }),
        workflow: Some(ReleaseWorkflowEvidence {
            graph: reference(ArtifactKind::WorkflowGraph, "graph"),
            workflow_hash: hash("graph"),
            plan: workflow_plan(),
        }),
        contracts: ReleaseContractEvidence {
            contract_hashes: BTreeSet::from([hash("contract")]),
            tool_set_hashes: BTreeSet::from([hash("tools")]),
            context_manifest_hashes: BTreeSet::from([hash("context")]),
        },
        provider_routes: BTreeSet::from([ReleaseProviderRouteEvidence {
            provider_id: "openai_responses".to_owned(),
            model_id: "fixture-model".to_owned(),
            reasoning_effort: Some("high".to_owned()),
            capability_snapshot_hash: hash("capability"),
            supports_tool_calls: true,
            supports_stateless_continuation: true,
            native_web_tool: true,
            streaming: Some(true),
            declared_context_limit: None,
            declared_max_output_tokens: None,
            source: "fixture_static_declared_unverified".to_owned(),
        }]),
        source_snapshots: BTreeSet::from([ReleaseSourceSnapshotEvidence {
            artifact: reference(ArtifactKind::NormalizedEvidence, "evidence"),
            blob_hash: hash("evidence-blob"),
            source_family: "fixture.market".to_owned(),
            observed_at: Some(now),
            retrieved_at: now,
        }]),
        broker: Some(ReleaseBrokerEvidence {
            account_fingerprint: hash("account"),
            trust,
            orders: vec![ReleaseOrderIdentity {
                client_order_id: "client-order-1".to_owned(),
                broker_order_id: "broker-order-1".to_owned(),
            }],
        }),
        session: Some(ReleaseSessionEvidence {
            session_key: "2026-08-31".to_owned(),
            scheduler_epoch: 7,
            reserved_at: now,
            committed_at: Some(now),
        }),
        daemon: Some(ReleaseDaemonEvidence {
            lease_name: "akzio.local.scheduler".to_owned(),
            owner_id: "fixture-daemon".to_owned(),
            epoch: 7,
            expires_at: now + chrono::Duration::minutes(5),
        }),
        execution: Some(ReleaseExecutionEvidence {
            execution_plan: reference(ArtifactKind::ExecutionPlan, "plan"),
            plan_hash: hash("plan-hash"),
            commitment: reference(ArtifactKind::ExecutionCommitment, "commitment"),
            commitment_id: "commitment-1".to_owned(),
            reconciliation: Some(reference(ArtifactKind::Reconciliation, "reconciliation")),
            reconciliation_receipts: vec![reference(ArtifactKind::OrderReceipt, "receipt")],
        }),
        outcomes: OutcomeHorizon::ALL
            .into_iter()
            .map(|horizon| {
                (
                    horizon,
                    ReleaseOutcomeEvidence {
                        outcome: reference(ArtifactKind::Outcome, "outcome"),
                        sealed_at: now,
                        observed_on: now.date_naive(),
                    },
                )
            })
            .collect(),
        learning: Some(ReleaseLearningEvidence {
            transition_id: "transition-1".to_owned(),
            from: PolicyState::Topology(CandidatePolicyState::Canary10),
            to: PolicyState::Topology(CandidatePolicyState::Canary25),
            evaluation: reference(ArtifactKind::Evaluation, "evaluation"),
            transitioned_at: now,
        }),
        canary: Some(ReleaseCanaryEvidence {
            campaign_id: hash("campaign"),
            status: CanaryCampaignStatus::ValidationStage2,
            revision: 2,
        }),
        human_approval: Some(ReleaseHumanApprovalEvidence {
            status: ReleaseHumanApprovalStatus::Approved,
            operator_identity: "fixture-operator".to_owned(),
            approved_at: Some(now),
            approval_hash: hash("approval"),
        }),
        integrity: ReleaseIntegrityEvidence {
            config_hash_matches: true,
            workflow_hash_matches: true,
            broker_account_matches: true,
            daemon_epoch_current: true,
        },
    }
}

#[test]
fn complete_fixture_is_deterministic_but_never_approvable() {
    let body = complete_body(Utc::now(), ReleaseEvidenceEnvironment::OfflineFixture);
    let first = ReleaseEvidenceBundle::materialize(body.clone()).unwrap();
    let second = ReleaseEvidenceBundle::materialize(body).unwrap();
    assert_eq!(first.bundle_hash, second.bundle_hash);
    assert_eq!(first.status, ReleaseEvidenceStatus::NotApprovable);
    assert_eq!(
        first.issues,
        BTreeSet::from([
            ReleaseEvidenceIssue::OfflineFixture,
            ReleaseEvidenceIssue::FakeBrokerEvidence,
        ])
    );
}

#[test]
fn complete_real_bundle_is_approvable() {
    let bundle = ReleaseEvidenceBundle::materialize(complete_body(
        Utc::now(),
        ReleaseEvidenceEnvironment::Real,
    ))
    .unwrap();
    assert_eq!(bundle.status, ReleaseEvidenceStatus::Approvable);
    assert!(bundle.issues.is_empty());
}

#[test]
fn missing_reconciliation_or_horizon_is_incomplete() {
    let mut body = complete_body(Utc::now(), ReleaseEvidenceEnvironment::Real);
    body.execution
        .as_mut()
        .unwrap()
        .reconciliation_receipts
        .clear();
    body.outcomes.remove(&OutcomeHorizon::T3);
    let bundle = ReleaseEvidenceBundle::materialize(body).unwrap();
    assert_eq!(bundle.status, ReleaseEvidenceStatus::Incomplete);
    assert!(bundle
        .issues
        .contains(&ReleaseEvidenceIssue::MissingReconciliation));
    assert!(bundle
        .issues
        .contains(&ReleaseEvidenceIssue::MissingOutcome {
            horizon: OutcomeHorizon::T3,
        }));
}

#[test]
fn noncanonical_dirty_or_drifted_bundle_is_not_approvable() {
    for purpose in [
        RunPurpose::Debug,
        RunPurpose::Replay,
        RunPurpose::Shadow,
        RunPurpose::PaperDryRun,
    ] {
        let mut body = complete_body(Utc::now(), ReleaseEvidenceEnvironment::Real);
        body.purpose = purpose;
        assert_eq!(
            ReleaseEvidenceBundle::materialize(body).unwrap().status,
            ReleaseEvidenceStatus::NotApprovable
        );
    }

    let mut body = complete_body(Utc::now(), ReleaseEvidenceEnvironment::Real);
    body.runtime.as_mut().unwrap().dirty_worktree = true;
    body.integrity = ReleaseIntegrityEvidence {
        config_hash_matches: false,
        workflow_hash_matches: false,
        broker_account_matches: false,
        daemon_epoch_current: false,
    };
    let bundle = ReleaseEvidenceBundle::materialize(body).unwrap();
    assert_eq!(bundle.status, ReleaseEvidenceStatus::NotApprovable);
    for issue in [
        ReleaseEvidenceIssue::DirtyWorktree,
        ReleaseEvidenceIssue::ConfigHashDrift,
        ReleaseEvidenceIssue::WorkflowHashDrift,
        ReleaseEvidenceIssue::BrokerAccountMismatch,
        ReleaseEvidenceIssue::StaleDaemonEpoch,
    ] {
        assert!(bundle.issues.contains(&issue));
    }
}
