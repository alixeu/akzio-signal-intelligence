//! Fixed, synthetic research-quality experiment. Uses the production context,
//! role prompts, schema, validator and SQL/CAS authority; never a broker.
use crate::{
    ActiveResearchCatalogue, AgentModel, AgentModelRequest, AgentModelTurn, AgentRuntime,
    ModelClientAdapter, ResearchError, Result,
};
use akzio_domain::*;
use akzio_model::{ModelCapabilitySnapshot, ModelClient};
use akzio_store::{ClaimedAttempt, Store, StoredRun, TaskWorkload, WorkflowCommit};
use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use serde_json::{json, Value};
use std::path::Path;

const CASE_COUNT: usize = 12;

fn persist(
    store: &Store,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    producer: &str,
    payload: &Value,
    refs: Vec<ArtifactRef>,
) -> Result<Artifact> {
    let now = Utc::now();
    let family = if kind == ArtifactKind::NormalizedEvidence {
        "alpaca"
    } else if matches!(kind, ArtifactKind::DecisionProposal | ArtifactKind::Claim) {
        "akzio.agent"
    } else {
        "akzio.runtime"
    };
    let artifact = Artifact::new(
        kind,
        store.stage_json(payload)?,
        producer,
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: family.into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: permit.contract_hash.clone(),
        },
        Some(permit.artifact_origin()),
        refs,
        now,
    )?;
    store.write_task_artifact(
        permit,
        &artifact,
        LifecycleEventType::ArtifactCommitted,
        now,
    )?;
    Ok(artifact)
}

fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn attempt(store: &Store, role: &str, objective: &str) -> Result<(AgentRuntime, ClaimedAttempt)> {
    let now = Utc::now();
    let catalogue = ActiveResearchCatalogue::install(store, now)?.contracts;
    let contract = &catalogue
        .contracts()
        .find(|c| c.contract.purpose.as_str() == role)
        .ok_or_else(|| ResearchError::InvalidOutput("quality role missing".into()))?
        .contract;
    let mut budget = contract.budget.clone();
    budget.max_input_tokens = 24_000;
    budget.max_output_tokens = 8_000;
    budget.max_wall_time_secs = 180;
    let node = WorkflowNode {
        spec: None,
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new(role)?,
        contract_hash: Some(contract.contract_hash.clone()),
        objective: objective.into(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget,
        retry: contract.retry.clone(),
        on_failure: contract.on_failure,
        parent_task_id: None,
    };
    let run_id = RunId::new();
    let mut nodes = vec![node.clone()];
    if objective.contains("[quality_repair]") {
        for role in [
            RESEARCH_SYNTHESIZER_RECIPE_ID,
            RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
        ] {
            let installed = &catalogue
                .contracts()
                .find(|c| c.contract.purpose.as_str() == role)
                .expect("installed role")
                .contract;
            let mut next = node.clone();
            next.task_id = TaskId::new();
            next.recipe_id = TaskRecipeId::new(role)?;
            next.contract_hash = Some(installed.contract_hash.clone());
            next.dependencies = vec![nodes.last().expect("initial node").task_id.clone()];
            next.retry = installed.retry.clone();
            next.on_failure = installed.on_failure;
            next.objective = "Repair only the rejected issues, preserve accepted forecasts and their numeric basis. [proposal_revision=1]".into();
            nodes.push(next);
        }
    }
    let graph = WorkflowGraph {
        definition_version: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        topology_id: "research-quality-synthetic".into(),
        nodes: nodes.clone(),
        agent_budgets: Default::default(),
    };
    let artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.stage_json(&graph)?,
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
    )?;
    let workflow = WorkflowCommit {
        run: StoredRun {
            run_id: run_id.clone(),
            purpose: RunPurpose::PositionPlan,
            topology_id: graph.topology_id,
            graph_artifact_id: artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: artifact,
        nodes: nodes.clone(),
    };
    let identity_hash = ContentHash::of_bytes(b"research-quality-synthetic-v1");
    store.commit_debug_experiment(
        &workflow,
        &[],
        &DebugSessionIdentity {
            version: 1,
            debug_session_id: format!("quality-{run_id}"),
            store_identity: store
                .debug_environment()?
                .expect("configured isolated store"),
            run_id: run_id.clone(),
            run_purpose: RunPurpose::PositionPlan,
            llm_mode: if objective.contains("offline") {
                DebugLlmMode::Fixture
            } else {
                DebugLlmMode::Real
            },
            broker_write_policy: DebugBrokerPolicy::Forbidden,
            learning_scope: DebugLearningScope::Isolated,
            code_revision: "research-quality-synthetic-v1".into(),
            runtime_identity: identity_hash.clone(),
            decision_policy_status: "synthetic_research_only".into(),
            decision_policy_input_hash: None,
            decision_policy_artifact: None,
            contract_hashes: nodes
                .iter()
                .filter_map(|n| n.contract_hash.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            dataset: vec![],
            parent_run_id: None,
            parent_task_id: None,
            parent_artifacts: vec![],
            reason: Some(
                "Fixed synthetic research-quality evaluation; no business Decision".into(),
            ),
            created_at: now,
        },
    )?;
    store.debug_control(
        &run_id,
        &DebugControlRequest {
            action: DebugAction::Step,
            expected_revision: 0,
            task_id: Some(node.task_id),
        },
        &identity_hash,
        now,
    )?;
    let claimed = store
        .claim_next_task_for_workload_with_identity(
            "research-quality",
            Utc::now(),
            Duration::minutes(10),
            TaskWorkload::Any,
            Some(&identity_hash),
        )?
        .ok_or_else(|| ResearchError::InvalidOutput("quality task unavailable".into()))?;
    Ok((
        AgentRuntime::new(store.clone(), catalogue, Duration::minutes(10)),
        claimed,
    ))
}

struct CountedModel<'a> {
    store: &'a Store,
    permit: &'a TaskWritePermit,
    inner: &'a ModelClientAdapter,
    case: String,
    real: bool,
}
impl AgentModel for CountedModel<'_> {
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        self.inner.capability_snapshot()
    }
    fn turn<'a>(&'a self, request: AgentModelRequest) -> BoxFuture<'a, Result<AgentModelTurn>> {
        Box::pin(async move {
            if self.real {
                let ordinal = self.store.reserve_research_quality_call(
                    self.permit,
                    &json!({"case":self.case,"request":request}),
                )?;
                eprintln!("quality provider call {ordinal}/40: {}", self.case);
            }
            self.inner.turn(request).await
        })
    }
}

fn case_material(index: usize, evidence: &ArtifactRef) -> (Value, Vec<String>) {
    let forecasts: Vec<_> = Asset::EXECUTABLE.into_iter().flat_map(|asset| ["t1","t3","t5"].into_iter()
        .map(move |h| json!({"asset":asset.symbol(),"horizon":h,
            "positive_return_probability_ppm":500000,"expected_return_ppm":0,
            "thesis":{"thesis_valid_until":"2030-01-01T00:00:00Z","expected_holding_period_days": match h {"t1"=>1,"t3"=>3,_=>5},
                "exit_condition":"reassess at horizon","invalidation_conditions":["new directional evidence"]}}))).collect();
    let mut proposal = json!({"summary":"Synthetic neutral research estimate; no directional evidence, no empirical calibration, all cash.",
        "confidence_ppm":500000,"forecasts":forecasts,"research_allocation":{"cash_weight_ppm":1000000,
            "allocations":Asset::EXECUTABLE.into_iter().map(|a| json!({"asset":a.symbol(),"target_weight_ppm":0,
                "supporting_horizons":[],"evidence_refs":[],"rationale":"Insufficient directional support; cash avoids unsupported exposure.",
                "abstention_reason":"no qualified directional evidence"})).collect::<Vec<_>>()},
        "claims":[],"critiques":[],"evidence":[evidence],"material_conflicts":[],"hard_blockers":[],"soft_warnings":[],
        "numeric_basis":proposal_review_keys().into_iter().map(|scope| json!({"scope":scope,"inputs":[evidence],"units":"ppm",
            "method":"Uncalibrated neutral prior: probability 500000, expected return 0. No directional support: asset weights 0 and cash 1000000.",
            "assumptions":"Symmetric upside/downside with equal weights; no empirical claim.",
            "uncertainty":"Research convention only; not a calibrated forecast or execution instruction."})).collect::<Vec<_>>()});
    let scope = "forecast.TQQQ.t1";
    let basis_index = proposal["numeric_basis"]
        .as_array()
        .unwrap()
        .iter()
        .position(|b| b["scope"] == scope)
        .unwrap();
    let mut rejected = vec![];
    match index {
        0..=3 => {
            proposal["summary"] = json!(format!("Synthetic neutral control {}. Insufficient directional support; explicitly uncalibrated, all cash.",index+1));
        }
        4 => {
            proposal["forecasts"][0]["positive_return_probability_ppm"] = json!(850000);
            rejected.push(scope.into());
        }
        5 => {
            proposal["forecasts"][0]["expected_return_ppm"] = json!(25000);
            rejected.push(scope.into());
        }
        6 => {
            proposal["numeric_basis"][basis_index]["method"] =
                json!("Expected return is +2 percent; conversion to ppm gives 0.");
            rejected.push(scope.into());
        }
        7 => {
            proposal["numeric_basis"][basis_index]["method"] = json!("Empirically calibrated from 10000 past samples with 95 percent confidence. The selected source contains all those samples.");
            rejected.push(scope.into());
        }
        8 => {
            proposal["numeric_basis"][basis_index]["method"] = json!("The selected evidence proves today's TQQQ price increased 8 percent, so expected return is 0.");
            rejected.push(scope.into());
        }
        9 => {
            proposal["numeric_basis"][basis_index]["method"] = json!("Current quote is dated 2001-01-01; no time mismatch or stale-data risk exists in using that quote as a current observation.");
            rejected.push(scope.into());
        }
        10 => {
            let cash = proposal["numeric_basis"]
                .as_array()
                .unwrap()
                .iter()
                .position(|b| b["scope"] == "allocation.cash")
                .unwrap();
            proposal["numeric_basis"][cash]["method"] = json!("The final portfolio is 50 percent invested and 50 percent cash; cash weight 1000000 ppm implements this split.");
            rejected.push("allocation.cash".into());
        }
        11 => {
            proposal["numeric_basis"][basis_index]["method"] = json!("Selected evidence says the scenario is directional and positively verified. It contains no warning or contradictory observation.");
            rejected.push(scope.into());
        }
        _ => unreachable!(),
    }
    (proposal, rejected)
}

/// Resume uses persisted results. An interrupted call is never refunded.
pub async fn verify(
    root: &Path,
    phase: &str,
    client: Option<ModelClient>,
    synth_client: Option<ModelClient>,
) -> Result<Value> {
    let cwd = std::env::current_dir().map_err(|e| ResearchError::Model(e.to_string()))?;
    let root = if root.is_absolute() {
        root.to_owned()
    } else {
        cwd.join(root)
    };
    if !root.starts_with(cwd.join(".akzio"))
        || root
            .components()
            .any(|p| p == std::path::Component::ParentDir)
    {
        return Err(ResearchError::InvalidOutput(
            "quality output must be inside workspace .akzio".into(),
        ));
    }
    if phase == "report" {
        return report(&Store::open_existing(root.join("store"))?);
    }
    let store = Store::open(root.join("store"))?;
    store.configure_debug_environment(true)?;
    let real = client.is_some();
    let client = client.unwrap_or_else(crate::fixture_model_client);
    let capability = if real {
        let existing = store.research_quality_records("research.quality.capability")?;
        if let Some(artifact) = existing.last() {
            let value: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            let snapshot: ModelCapabilitySnapshot =
                serde_json::from_value(value["snapshot"].clone())?;
            let selected = client.capability_snapshot();
            if snapshot.model_id != selected.model_id
                || snapshot.reasoning_effort != selected.reasoning_effort
            {
                return Err(ResearchError::InvalidOutput(
                    "quality model changed between phases".into(),
                ));
            }
            snapshot
        } else {
            let (_, probe) = attempt(
                &store,
                RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
                "Quality capability probe",
            )?;
            // The audited probe performs at most three requests. Reserve the
            // upper bound before any I/O; failed/unused reservations stay spent.
            for slot in 0..3 {
                store.reserve_research_quality_call(
                    &probe.permit,
                    &json!({"phase":"capability_probe","slot":slot}),
                )?;
            }
            let (snapshot, calls) = client
                .probe_capabilities_audited()
                .await
                .map_err(|e| ResearchError::Model(e.to_string()))?;
            persist(
                &store,
                &probe.permit,
                ArtifactKind::SemanticDetail,
                "research.quality.capability",
                &json!({"snapshot":snapshot,"calls":calls}),
                vec![],
            )?;
            store.finish_task(&probe.permit, TaskStatus::Skipped, Utc::now())?;
            snapshot
        }
    } else {
        client.capability_snapshot()
    };
    let synth_client = synth_client.unwrap_or_else(|| client.clone());
    let synth_snapshot = synth_client.capability_snapshot();
    let synth_model = (synth_snapshot.model_id == capability.model_id
        && synth_snapshot.reasoning_effort == capability.reasoning_effort)
        .then(|| {
            ModelClientAdapter::new(synth_client).with_capability_snapshot(capability.clone())
        });
    let model = ModelClientAdapter::new(client).with_capability_snapshot(capability);
    let previous = store
        .research_quality_records("research.quality.result")?
        .into_iter()
        .map(|a| Ok(serde_json::from_slice::<Value>(&store.read_blob(&a.blob)?)?))
        .collect::<Result<Vec<_>>>()?;
    let started_cases = store
        .research_quality_records("research.quality.call.started")?
        .into_iter()
        .map(|artifact| {
            let value: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            Ok(value["case"].as_str().map(str::to_owned))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    let mut results = Vec::new();
    for run_index in 0..(if real { 14 } else { CASE_COUNT }) {
        let index = if run_index >= CASE_COUNT {
            [0, 4][run_index - CASE_COUNT]
        } else {
            run_index
        };
        let case_id = format!(
            "RQ-REVIEW-{:02}{}",
            index + 1,
            if run_index >= CASE_COUNT {
                "-repeat"
            } else {
                ""
            }
        );
        if let Some(saved) = previous
            .iter()
            .find(|v| v["phase"] == phase && v["case_id"] == case_id)
        {
            results.push(saved.clone());
            continue;
        }
        if started_cases.contains(&format!("{phase}/{case_id}")) {
            // No terminal result means completion is unknown. Do not replay the
            // same paid case under a new task and reset its attempt allowance.
            eprintln!("quality {case_id}: prior completion unknown; not replayed");
            continue;
        }
        let repair =
            phase == "candidate" && run_index < CASE_COUNT && [4, 5, 6, 10].contains(&index);
        let (runtime, task) = attempt(&store, RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
            &format!("{phase}: Review the provided synthetic proposal using the normal contract. Treat this as bounded research estimates, not empirical calibration. [proposal_revision=0] {}",if repair {"[quality_repair]"} else {""}))?;
        let evidence = persist(
            &store,
            &task.permit,
            ArtifactKind::NormalizedEvidence,
            "evidence.normalize",
            &json!({"source":"alpaca","resource":"quote:TQQQ","synthetic_test_material":true,
                "value":{"observation":"No current directional signal. This synthetic document contains no historical samples and no verified return observation.",
                    "warning":"Do not infer calibration, price changes or absence of contradiction from this neutral scenario.",
                    "source_verified":false,"price":null}}),
            vec![],
        )?;
        let mut claim = crate::fixture_claim_output();
        claim["grounds"][0]["evidence"] = json!(reference(&evidence));
        let claim = persist(
            &store,
            &task.permit,
            ArtifactKind::Claim,
            "agent.research.analyst",
            &claim,
            vec![reference(&evidence)],
        )?;
        let (mut proposal, expected) = case_material(index, &reference(&evidence));
        proposal["claims"] = json!([reference(&claim)]);
        let proposal = persist(
            &store,
            &task.permit,
            ArtifactKind::DecisionProposal,
            "agent.research.synthesizer",
            &proposal,
            vec![reference(&evidence), reference(&claim)],
        )?;
        let counted = CountedModel {
            store: &store,
            permit: &task.permit,
            inner: &model,
            case: format!("{phase}/{case_id}"),
            real,
        };
        let output = runtime
            .run(
                &task.permit,
                &task.node,
                [
                    reference(&evidence),
                    reference(&claim),
                    reference(&proposal),
                ],
                &counted,
                Utc::now(),
            )
            .await;
        let mut result = json!({"phase":phase,"case_id":case_id,"run_id":task.permit.run_id,
            "contract_hash":task.node.contract_hash,"expected_rejected_scopes":expected,"synthetic":true,"real_model":real});
        match output {
            Ok(artifact) => {
                let review: ProposalReview =
                    serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                let rejected: Vec<_> = review
                    .assessments
                    .iter()
                    .filter(|a| !a.accepted)
                    .map(|a| a.scope.clone())
                    .collect();
                result["protocol_valid"] = json!(true);
                result["false_accept"] =
                    json!(!expected.is_empty() && expected.iter().any(|s| !rejected.contains(s)));
                result["false_reject"] = json!(expected.is_empty() && !rejected.is_empty());
                result["review"] = json!(review);
                persist(
                    &store,
                    &task.permit,
                    ArtifactKind::SemanticDetail,
                    "research.quality.result",
                    &result,
                    vec![reference(&proposal), reference(&evidence)],
                )?;
                store.commit_attempt(
                    &task.permit,
                    std::slice::from_ref(&artifact),
                    TaskStatus::Succeeded,
                    Utc::now(),
                )?;
                if repair {
                    run_repair(
                        &store,
                        &runtime,
                        &task.permit.run_id,
                        &case_id,
                        vec![
                            reference(&evidence),
                            reference(&claim),
                            reference(&proposal),
                            reference(&artifact),
                        ],
                        synth_model.as_ref(),
                        &model,
                    )
                    .await?;
                }
            }
            Err(error) => {
                result["protocol_valid"] = json!(false);
                result["error"] = json!(error.to_string());
                eprintln!("quality protocol error: {error}");
                persist(
                    &store,
                    &task.permit,
                    ArtifactKind::SemanticDetail,
                    "research.quality.result",
                    &result,
                    vec![reference(&proposal), reference(&evidence)],
                )?;
                store.finish_task(&task.permit, TaskStatus::Failed, Utc::now())?;
            }
        }
        eprintln!(
            "quality {}: protocol_valid={} false_accept={} false_reject={}",
            case_id, result["protocol_valid"], result["false_accept"], result["false_reject"]
        );
        results.push(result);
    }
    let phase_report = json!({"phase":phase,"synthetic":true,"broker_writes":0,
        "reserved_real_calls":store.research_quality_records("research.quality.call.started")?.len(),
        "results":results});
    std::fs::write(
        root.join(format!("{phase}.json")),
        serde_json::to_vec_pretty(&phase_report)?,
    )
    .map_err(|e| ResearchError::Model(e.to_string()))?;
    let report = report(&store)?;
    std::fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )
    .map_err(|e| ResearchError::Model(e.to_string()))?;
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn run_repair(
    store: &Store,
    runtime: &AgentRuntime,
    run_id: &RunId,
    case_id: &str,
    mut inputs: Vec<ArtifactRef>,
    synth_model: Option<&ModelClientAdapter>,
    reviewer: &ModelClientAdapter,
) -> Result<()> {
    let identity = ContentHash::of_bytes(b"research-quality-synthetic-v1");
    for role in [
        RESEARCH_SYNTHESIZER_RECIPE_ID,
        RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
    ] {
        let snapshot = store.workflow_snapshot(run_id)?;
        let node = snapshot
            .tasks
            .iter()
            .find(|t| t.node.recipe_id.as_str() == role && t.status == TaskStatus::Pending)
            .ok_or_else(|| ResearchError::InvalidOutput("repair task unavailable".into()))?;
        let session = store.debug_session(run_id)?.expect("quality debug session");
        store.debug_control(
            run_id,
            &DebugControlRequest {
                action: DebugAction::Step,
                expected_revision: session.revision,
                task_id: Some(node.node.task_id.clone()),
            },
            &identity,
            Utc::now(),
        )?;
        let task = store
            .claim_next_task_for_workload_with_identity(
                "research-quality",
                Utc::now(),
                Duration::minutes(10),
                TaskWorkload::Any,
                Some(&identity),
            )?
            .ok_or_else(|| ResearchError::InvalidOutput("repair claim unavailable".into()))?;
        if role == RESEARCH_SYNTHESIZER_RECIPE_ID {
            // Explicit synthetic calendars are only test material. They never
            // enter canonical market evidence or formal calibration datasets.
            for asset in Asset::EXECUTABLE {
                let closes = (1..=8)
                    .map(|day| {
                        (
                            format!("2030-01-{day:02}"),
                            json!(format!("2030-01-{day:02}T21:00:00Z")),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>();
                let calendar = persist(
                    store,
                    &task.permit,
                    ArtifactKind::NormalizedEvidence,
                    "evidence.normalize",
                    &json!({"source":"alpaca","resource":format!("bars:{}:day:quality",asset.symbol()),"synthetic_test_material":true,
                        "value":{"forecast_session_closes":closes,"observation":"Synthetic calendar only; no directional signal."}}),
                    vec![],
                )?;
                inputs.push(reference(&calendar));
            }
        }
        let selected_model = if role == RESEARCH_SYNTHESIZER_RECIPE_ID {
            synth_model
        } else {
            Some(reviewer)
        };
        let output = if let Some(model) = selected_model {
            runtime
                .run(
                    &task.permit,
                    &task.node,
                    inputs.clone(),
                    &CountedModel {
                        store,
                        permit: &task.permit,
                        inner: model,
                        case: format!("repair/{case_id}/{role}"),
                        real: true,
                    },
                    Utc::now(),
                )
                .await
        } else {
            Err(ResearchError::InvalidOutput("Synthesizer route differs; no matching capability proof within reserved probe budget".into()))
        };
        let mut record = json!({"phase":"repair","case_id":case_id,"role":role,"run_id":run_id,
            "synthetic":true,"real_model":true,"expected_rejected_scopes":[]});
        match output {
            Ok(artifact) => {
                record["protocol_valid"] = json!(true);
                if role == RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID {
                    let review: ProposalReview =
                        serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                    record["repair_accepted"] = json!(review.accepted());
                    record["review"] = json!(review);
                }
                persist(
                    store,
                    &task.permit,
                    ArtifactKind::SemanticDetail,
                    "research.quality.result",
                    &record,
                    inputs
                        .iter()
                        .filter(|r| r.kind == ArtifactKind::NormalizedEvidence)
                        .cloned()
                        .collect(),
                )?;
                store.commit_attempt(
                    &task.permit,
                    std::slice::from_ref(&artifact),
                    TaskStatus::Succeeded,
                    Utc::now(),
                )?;
                inputs.retain(|r| {
                    !matches!(
                        r.kind,
                        ArtifactKind::DecisionProposal | ArtifactKind::ProposalReview
                    )
                });
                inputs.push(reference(&artifact));
            }
            Err(error) => {
                record["protocol_valid"] = json!(false);
                record["error"] = json!(error.to_string());
                persist(
                    store,
                    &task.permit,
                    ArtifactKind::SemanticDetail,
                    "research.quality.result",
                    &record,
                    inputs
                        .iter()
                        .filter(|r| r.kind == ArtifactKind::NormalizedEvidence)
                        .cloned()
                        .collect(),
                )?;
                store.finish_task(&task.permit, TaskStatus::Failed, Utc::now())?;
                eprintln!("quality repair {case_id} failed: {error}");
                break;
            }
        }
    }
    Ok(())
}

fn report(store: &Store) -> Result<Value> {
    let mut results = Vec::new();
    for artifact in store.research_quality_records("research.quality.result")? {
        let mut result: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        let run_id = artifact
            .origin
            .as_ref()
            .and_then(|o| o.run_id.clone())
            .ok_or_else(|| ResearchError::InvalidOutput("quality result has no Run".into()))?;
        let mut cursor = 0;
        let mut turns = Vec::new();
        loop {
            let events = store.events_after(&run_id, cursor, 500)?;
            if events.is_empty() {
                break;
            }
            for event in &events {
                if let Some(id) = &event.artifact_id {
                    let a = store.artifact(id)?;
                    if a.kind == ArtifactKind::AgentTurn
                        && a.origin.as_ref().and_then(|o| o.task_id.as_ref())
                            == artifact.origin.as_ref().and_then(|o| o.task_id.as_ref())
                    {
                        let value: Value = serde_json::from_slice(&store.read_blob(&a.blob)?)?;
                        if !turns
                            .iter()
                            .any(|(seen, _): &(ArtifactId, Value)| seen == id)
                        {
                            turns.push((id.clone(), value));
                        }
                    }
                }
            }
            cursor = events.last().expect("nonempty events").cursor;
        }
        result["agent_turns"] = json!(turns
            .iter()
            .map(
                |(id, v)| json!({"artifact_id":id,"telemetry":v["response"]["telemetry"],
            "request_hash":v["request_hash"],"lifecycle":v["lifecycle"]})
            )
            .collect::<Vec<_>>());
        if let Some(assessments) = turns.iter().rev().find_map(|(_, v)| {
            v.pointer("/response/terminal_submission/arguments/result/assessments")
                .and_then(Value::as_array)
        }) {
            let rejected = assessments
                .iter()
                .filter(|a| a["accepted"] == false)
                .filter_map(|a| a["scope"].as_str())
                .collect::<Vec<_>>();
            let expected = result["expected_rejected_scopes"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            result["observed_false_accept"] = json!(
                !expected.is_empty()
                    && expected
                        .iter()
                        .any(|v| !rejected.contains(&v.as_str().unwrap_or("")))
            );
            result["observed_false_reject"] = json!(expected.is_empty() && !rejected.is_empty());
            result["observed_assessments"] = json!(assessments);
        }
        results.push(result);
    }
    let mut metrics = serde_json::Map::new();
    for phase in ["baseline", "candidate", "offline"] {
        let rows = results
            .iter()
            .filter(|v| v["phase"] == phase)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        metrics.insert(phase.into(),json!({"cases":rows.len(),"protocol_valid":rows.iter().filter(|v|v["protocol_valid"]==true).count(),
            "false_accepts":rows.iter().filter(|v|v["observed_false_accept"]==true).count(),
            "false_rejects":rows.iter().filter(|v|v["observed_false_reject"]==true).count(),
            "missing_observation":rows.iter().filter(|v|v["observed_assessments"].is_null()).count()}));
    }
    let candidate = metrics.get("candidate");
    let passed = candidate.map(|v| {
        v["cases"] == 14
            && v["protocol_valid"] == 14
            && v["false_accepts"] == 0
            && v["missing_observation"] == 0
            && metrics
                .get("baseline")
                .is_some_and(|b| v["false_rejects"].as_u64() <= b["false_rejects"].as_u64())
    });
    let repair_passed = results
        .iter()
        .filter(|r| r["phase"] == "repair" && r["repair_accepted"] == true)
        .count();
    Ok(
        json!({"synthetic":true,"scope":"real model synthetic Reviewer evaluation; no market, Paper, Policy or Outcome proof",
        "reserved_real_calls":store.research_quality_records("research.quality.call.started")?.len(),
        "reviewer_passed":passed,"repairs_accepted":repair_passed,"passed":passed.map(|p|p && repair_passed==4),"metrics":metrics,"results":results}),
    )
}
