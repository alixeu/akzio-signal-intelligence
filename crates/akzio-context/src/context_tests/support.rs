use std::collections::BTreeSet;

use akzio_domain::{
    ArtifactKind, CandidatePolicyState, ContractId, ContractPurpose, FailureDisposition,
    MemoryLifecycle, OutputContract, PromptBundle, RetryPolicy, RunPurpose, TaskBudget,
    TerminationPolicy, ToolGrant, ToolKind, ToolSpec, WorkflowGraph, WorkflowNode,
    V2_DOMAIN_SCHEMA_VERSION,
};
use akzio_store::v2::{StoredRun, WorkflowCommit};
use tempfile::tempdir;

use super::*;

fn contract(store: &V2Store) -> AgentContract {
    AgentContract::new(
        ContractId::new(),
        1,
        ContractPurpose::new("research.analyst").unwrap(),
        "analyze",
        PromptBundle {
            version: 1,
            governance: store.put_bytes(b"governance", "text/plain").unwrap(),
            role: store.put_bytes(b"prompt", "text/plain").unwrap(),
        },
        ContextPolicy {
            permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
            permitted_source_families: BTreeSet::from(["market".to_owned()]),
            min_artifacts: 1,
            max_artifacts: 4,
            max_bytes: 4096,
            max_tokens: 1024,
            allow_raw_reread: true,
        },
        vec![ToolGrant {
            kind: ToolKind::ReadRawEvidence,
            allowed_sources: vec!["market".to_owned()],
        }],
        vec![ToolSpec {
            name: "read_raw_evidence".to_owned(),
            description: "read granted raw evidence".to_owned(),
            kind: ToolKind::ReadRawEvidence,
            input_schema: store.put_bytes(b"tool schema", "application/json").unwrap(),
            strict: true,
        }],
        OutputContract {
            artifact_kind: ArtifactKind::Claim,
            schema: store.put_bytes(b"schema", "application/json").unwrap(),
        },
        TaskBudget {
            max_input_tokens: 1024,
            max_output_tokens: 128,
            max_wall_time_secs: 30,
            max_tool_calls: 2,
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
    .unwrap()
}
