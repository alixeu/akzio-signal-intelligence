use super::super::insert_artifact;
use super::*;
use akzio_domain::{
    AgentContract, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ContextPolicy,
    ContractId, ContractPurpose, FailureDisposition, MoneyMicros, OutputContract,
    PaperApprovalScope, PaperLaunchApproval, PromptBundle, RetryPolicy, RuntimeManifest,
    TaskBudget, TaskId, TaskRecipeId, TerminationPolicy, ToolGrant, ToolKind, ToolSpec, TopologyId,
    WorkflowGraph, WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
};
use chrono::Duration;
use serde::Serialize;
use std::collections::BTreeSet;
use tempfile::tempdir;

fn canonical_json_artifact<T: Serialize>(
    store: &V2Store,
    kind: ArtifactKind,
    payload: &T,
) -> akzio_domain::ArtifactRef {
    let now = Utc::now();
    let lifecycle = if kind == ArtifactKind::WorkflowGraph {
        ArtifactLifecycle::RunScoped
    } else {
        ArtifactLifecycle::Canonical
    };
    let artifact = Artifact::new(
        kind,
        store.put_json(payload).unwrap(),
        "canary.test",
        lifecycle,
        ArtifactProvenance {
            source_family: "canary.test".to_owned(),
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
    let mut connection = store.connection().unwrap();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    insert_artifact(&transaction, &artifact).unwrap();
    transaction.commit().unwrap();
    akzio_domain::ArtifactRef {
        artifact_id: artifact.artifact_id,
        kind,
    }
}

fn spec(store: &V2Store, value: &[u8]) -> CanaryCampaignSpec {
    let now = Utc::now();
    let maximum_total_notional = MoneyMicros::from_usd_cents(100_000);
    let active_contract_hash = ContentHash::of_bytes(b"active-contract");
    let candidate_contract_payload = AgentContract::new(
        ContractId::new(),
        1,
        ContractPurpose::new("research.candidate").unwrap(),
        "candidate contract",
        PromptBundle {
            version: 1,
            governance: store.put_bytes(b"governance", "text/plain").unwrap(),
            role: store.put_bytes(b"role", "text/plain").unwrap(),
        },
        ContextPolicy {
            permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
            permitted_source_families: BTreeSet::from(["market".to_owned()]),
            min_artifacts: 1,
            max_artifacts: 4,
            max_bytes: 4096,
            max_tokens: 1024,
            allow_raw_reread: false,
        },
        vec![ToolGrant {
            kind: ToolKind::ReadEvidence,
            allowed_sources: vec!["market".to_owned()],
        }],
        vec![ToolSpec {
            name: "read_artifact".to_owned(),
            description: "read market artifact".to_owned(),
            kind: ToolKind::ReadEvidence,
            input_schema: store.put_bytes(b"{}", "application/json").unwrap(),
            strict: true,
        }],
        OutputContract {
            artifact_kind: ArtifactKind::Claim,
            schema: store.put_bytes(b"{}", "application/json").unwrap(),
        },
        TaskBudget {
            max_input_tokens: 32,
            max_output_tokens: 16,
            max_wall_time_secs: 10,
            max_tool_calls: 1,
        },
        RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        },
        TerminationPolicy::leaf(),
        FailureDisposition::FailRun,
    )
    .unwrap();
    let candidate_contract =
        canonical_json_artifact(store, ArtifactKind::Contract, &candidate_contract_payload);
    let node = WorkflowNode {
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
        contract_hash: None,
        objective: "candidate topology".to_owned(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget: TaskBudget {
            max_input_tokens: 32,
            max_output_tokens: 16,
            max_wall_time_secs: 10,
            max_tool_calls: 1,
        },
        retry: RetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 1,
            retry_transport: true,
            retry_rate_limited: true,
            retry_invalid_output: false,
        },
        on_failure: FailureDisposition::FailRun,
        parent_task_id: None,
    };
    let candidate_topology_payload = WorkflowGraph {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "candidate-topology".to_owned(),
        nodes: vec![node],
    };
    let candidate_topology = canonical_json_artifact(
        store,
        ArtifactKind::WorkflowGraph,
        &candidate_topology_payload,
    );
    let manifest_payload = RuntimeManifest {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        code_revision: "revision-1".to_owned(),
        cargo_lock_hash: ContentHash::of_bytes(b"cargo-lock"),
        config_hash: ContentHash::of_bytes(b"config"),
        provider_id: "provider".to_owned(),
        model_id: "model".to_owned(),
        prompt_hash: ContentHash::of_bytes(b"prompt"),
        contract_hash: active_contract_hash.clone(),
        topology_hash: ContentHash::of_bytes(b"active-topology"),
        decision_policy_hash: ContentHash::of_bytes(b"decision"),
        execution_policy_hash: ContentHash::of_bytes(b"execution"),
        evaluation_policy_hash: ContentHash::of_bytes(b"evaluation"),
        market_data_feed: "iex".to_owned(),
        broker_account_id: "paper-account".to_owned(),
        maximum_notional: maximum_total_notional,
        allowed_session_start: now.date_naive(),
        allowed_session_end: now.date_naive(),
        expires_at: now + Duration::hours(8),
        created_at: now,
    };
    let runtime_manifest =
        canonical_json_artifact(store, ArtifactKind::RuntimeManifest, &manifest_payload);
    let mut approval_payload = PaperLaunchApproval {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        operator_identity: "operator:test".to_owned(),
        runtime_manifest: runtime_manifest.clone(),
        runtime_manifest_hash: manifest_payload.manifest_hash().unwrap(),
        scope: PaperApprovalScope::Canary,
        reason: "test campaign".to_owned(),
        approved_at: now,
        expires_at: manifest_payload.expires_at,
        approval_hash: ContentHash::of_bytes(b"pending"),
    };
    approval_payload.approval_hash = approval_payload.unsigned_hash().unwrap();
    let paper_approval =
        canonical_json_artifact(store, ArtifactKind::PaperLaunchApproval, &approval_payload);
    CanaryCampaignSpec {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        campaign_id: ContentHash::of_bytes(value),
        active_contract_hash: ContentHash::of_bytes(b"active-contract"),
        candidate_contract,
        active_topology_id: TopologyId("active-topology".to_owned()),
        candidate_topology,
        runtime_manifest,
        paper_approval,
        source_revision: "revision-1".to_owned(),
        maximum_total_notional: akzio_domain::MoneyMicros::from_usd_cents(100_000),
        created_at: Utc::now(),
    }
}

#[test]
fn campaign_head_is_fenced_and_advances_idempotently_by_status() {
    let directory = tempdir().unwrap();
    let store = V2Store::open(directory.path()).unwrap();
    let now = Utc::now();
    let lease = store
        .acquire_daemon_lease("campaign", "owner", now, now + chrono::Duration::minutes(5))
        .unwrap()
        .unwrap();
    let campaign = store
        .stage_canary_campaign(&lease, &spec(&store, b"campaign"), now)
        .unwrap();
    assert_eq!(campaign.status, CanaryCampaignStatus::Staged);
    let campaign = store
        .transition_canary_campaign(
            &lease,
            &campaign.spec.campaign_id,
            CanaryCampaignStatus::Staged,
            CanaryVerdict::Advance,
            now,
        )
        .unwrap();
    assert_eq!(campaign.status, CanaryCampaignStatus::Canary10);
    assert_eq!(store.active_canary_campaign().unwrap().unwrap().revision, 1);
}
