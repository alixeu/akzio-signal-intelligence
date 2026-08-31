use std::collections::{BTreeMap, BTreeSet};

use akzio_domain::{
    ArtifactId, ArtifactOrigin, ArtifactRef, ClaimStance, ContentHash, ContractPurpose,
    DecisionHorizon, EvidenceGap, EvidenceGapImpact, EvidenceNeed, FailureDisposition,
    LifecycleEventType,
    ResearchClaim, RetryPolicy, TaskBudget, WorkflowProposalDraft, WorkflowProposalDraftTask,
    WorkflowProposalTask,
};
use tempfile::tempdir;

use super::*;
use crate::runtime_v2::task::TaskFailpoint;

fn budget() -> TaskBudget {
    TaskBudget {
        max_input_tokens: 100,
        max_output_tokens: 50,
        max_wall_time_secs: 30,
        max_tool_calls: 2,
    }
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 2,
        initial_backoff_ms: 0,
        retry_transport: true,
        retry_rate_limited: true,
        retry_invalid_output: false,
    }
}

fn claim(
    stance: ClaimStance,
    materiality_ppm: u32,
    confidence_ppm: u32,
    has_gap: bool,
) -> ResearchClaim {
    ResearchClaim {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topic: "TQQQ regime".to_owned(),
        statement: "fixture claim".to_owned(),
        horizon: DecisionHorizon::T5,
        stance,
        materiality_ppm,
        confidence_ppm,
        grounds: vec![],
        evidence_gaps: has_gap
        .then(|| EvidenceGap {
            topic: "fixture gap".to_owned(),
            rationale: "fixture uncertainty".to_owned(),
            impact: EvidenceGapImpact::Warning,
            supplemental_needs: vec![],
        })
            .into_iter()
            .collect(),
    }
}

fn recipe(id: &str, class: RuntimeTaskClass, agent: bool) -> TaskRecipe {
    TaskRecipe {
        recipe_id: TaskRecipeId::new(id).unwrap(),
        purpose: ContractPurpose::new(id).unwrap(),
        contract_hash: agent.then(|| ContentHash::of_bytes(id.as_bytes())),
        task_class: class,
        allowed_evidence_sources: if agent {
            BTreeSet::from(["alpaca".to_owned()])
        } else {
            BTreeSet::new()
        },
        max_children: 8,
        max_depth: 2,
        priority_ceiling: 100,
        budget: budget(),
        retry: retry(),
        on_failure: FailureDisposition::FailRun,
    }
}

fn catalogue() -> RecipeCatalogue {
    let mut analyst = recipe("research.analyst", RuntimeTaskClass::Agent, true);
    analyst.max_children = 1;
    analyst.max_depth = 1;
    RecipeCatalogue::new(
        [
            recipe("research.planner", RuntimeTaskClass::Agent, true),
            analyst,
            recipe("research.critic", RuntimeTaskClass::Agent, true),
            recipe("research.synthesizer", RuntimeTaskClass::Agent, true),
            recipe("gate.evidence", RuntimeTaskClass::Evidence, false),
            recipe("gate.decision", RuntimeTaskClass::DecisionGate, false),
            recipe("gate.execution", RuntimeTaskClass::ExecutionGate, false),
            recipe("gate.paper", RuntimeTaskClass::PaperCommit, false),
            recipe("gate.reconcile", RuntimeTaskClass::Reconcile, false),
            recipe("gate.evaluate", RuntimeTaskClass::Evaluate, false),
            recipe("learning.outcome_worker", RuntimeTaskClass::Evaluate, false),
        ],
        TaskRecipeId::new("research.planner").unwrap(),
        TerminalRecipeSet {
            evidence_gate: TaskRecipeId::new("gate.evidence").unwrap(),
            decision_gate: TaskRecipeId::new("gate.decision").unwrap(),
            execution_gate: TaskRecipeId::new("gate.execution").unwrap(),
            paper_commit: TaskRecipeId::new("gate.paper").unwrap(),
            reconcile: TaskRecipeId::new("gate.reconcile").unwrap(),
            evaluate: TaskRecipeId::new("gate.evaluate").unwrap(),
        },
        32,
    )
    .unwrap()
}

fn proposal() -> WorkflowProposal {
    WorkflowProposal {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: BTreeMap::from([
            (
                "analyst".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                    objective: "analyse evidence".to_owned(),
                    depends_on: vec![],
                    priority: 80,
                    evidence_needs: vec![],
                },
            ),
            (
                "critic".to_owned(),
                WorkflowProposalTask {
                    recipe_id: TaskRecipeId::new("research.critic").unwrap(),
                    objective: "challenge claim".to_owned(),
                    depends_on: vec!["analyst".to_owned()],
                    priority: 70,
                    evidence_needs: vec![],
                },
            ),
        ]),
        stop_reason: None,
    }
}

fn planner_output_artifact(
    store: &V2Store,
    planner: &ClaimedAttempt,
    now: DateTime<Utc>,
) -> Artifact {
    let draft = WorkflowProposalDraft {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        topology_id: "active".to_owned(),
        tasks: BTreeMap::from([(
            "analyst".to_owned(),
            WorkflowProposalDraftTask {
                recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
                objective: "analyse evidence".to_owned(),
                depends_on: vec![],
                priority: 80,
                evidence_needs: vec![EvidenceNeed {
                    schema_version: V2_DOMAIN_SCHEMA_VERSION,
                    source_family: "alpaca".to_owned(),
                    resource: "bars:TQQQ:1d".to_owned(),
                    max_age_secs: 86_400,
                }],
                research_intents: vec![],
            },
        )]),
        stop_reason: None,
    };
    Artifact::new(
        ArtifactKind::WorkflowProposalDraft,
        store.put_json(&draft).unwrap(),
        "agent.planner",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.agent".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: planner.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(planner.run_id.clone()),
            task_id: Some(planner.node.task_id.clone()),
            attempt_id: Some(planner.permit.attempt_id.clone()),
            contract_hash: planner.permit.contract_hash.clone(),
        }),
        vec![],
        now,
    )
    .unwrap()
}

fn task_artifact(store: &V2Store, task: &ClaimedAttempt, now: DateTime<Utc>) -> Artifact {
    let blob = store
        .put_bytes(b"task output", "text/plain; charset=utf-8")
        .unwrap();
    Artifact::new(
        ArtifactKind::AgentTurn,
        blob,
        "runtime.fixture",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "fixture".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: task.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(task.run_id.clone()),
            task_id: Some(task.node.task_id.clone()),
            attempt_id: Some(task.permit.attempt_id.clone()),
            contract_hash: task.permit.contract_hash.clone(),
        }),
        vec![],
        now,
    )
    .unwrap()
}
