//! Persist real acquisition audits using the ordinary Store permit and EvidenceRuntime.
//! This evidence-only graph cannot invoke a model, DecisionGate or broker.
use super::*;

pub fn persist_market_audit_capture(
    store: &Store,
    request: &EvidenceRequest,
    acquired: AcquiredEvidence,
) -> Result<Value> {
    let now = Utc::now();
    let (recipes, terminals) = akzio_runtime::rust_terminal_recipes()?;
    let recipe = recipes
        .into_iter()
        .find(|r| r.recipe_id == terminals.evidence_gate)
        .ok_or_else(|| DaemonError::InvalidInput("evidence recipe missing".into()))?;
    let run_id = RunId::new();
    let node = akzio_domain::WorkflowNode {
        spec: None,
        task_id: TaskId::new(),
        recipe_id: recipe.recipe_id,
        contract_hash: recipe.contract_hash,
        objective: "Persist an acquired real market audit; no research or execution".into(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: recipe.priority_ceiling,
        budget: recipe.budget,
        retry: akzio_domain::RetryPolicy::none(),
        on_failure: recipe.on_failure,
        parent_task_id: None,
    };
    let graph = WorkflowGraph {
        definition_version: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        topology_id: "market-acquisition-audit".into(),
        nodes: vec![node],
        agent_budgets: BTreeMap::new(),
    };
    let provenance = ArtifactProvenance {
        source_family: "runtime.market_audit".into(),
        observed_at: Some(now),
        retrieved_at: now,
        source_uri: None,
        confidence_ppm: 1_000_000,
        producer_contract_hash: None,
    };
    let graph_artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.stage_json(&graph)?,
        "runtime.market_audit",
        ArtifactLifecycle::RunScoped,
        provenance.clone(),
        None,
        vec![],
        now,
    )?;
    store.commit_workflow(&akzio_store::WorkflowCommit {
        run: akzio_store::StoredRun {
            run_id: run_id.clone(),
            purpose: RunPurpose::Debug,
            topology_id: graph.topology_id,
            graph_artifact_id: graph_artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: graph_artifact,
        nodes: graph.nodes,
    })?;
    let task = store
        .claim_next_task_for_workload(
            "market-audit",
            now,
            Duration::minutes(2),
            akzio_store::TaskWorkload::Any,
        )?
        .filter(|task| task.run_id == run_id)
        .ok_or_else(|| {
            DaemonError::InvalidInput("market audit requires its isolated Store".into())
        })?;
    let need = EvidenceNeed {
        schema_version: DOMAIN_SCHEMA_VERSION,
        source_family: request.source.as_str().into(),
        resource: request.resource.clone(),
        max_age_secs: request.max_age.num_seconds() as u64,
    };
    need.validate()?;
    let need_artifact = Artifact::new(
        ArtifactKind::EvidenceNeed,
        store.stage_json(&need)?,
        "runtime.market_audit",
        ArtifactLifecycle::RunScoped,
        provenance,
        Some(task.permit.artifact_origin()),
        vec![],
        now,
    )?;
    store.write_task_artifact(
        &task.permit,
        &need_artifact,
        LifecycleEventType::ArtifactCommitted,
        now,
    )?;
    let need_ref = ArtifactRef {
        artifact_id: need_artifact.artifact_id,
        kind: ArtifactKind::EvidenceNeed,
    };
    let result = EvidenceRuntime::new(store.clone(), [request.source]).materialize_validated(
        &task.permit,
        &need_ref,
        request,
        acquired,
        Utc::now(),
    );
    match result {
        Ok(bundle) => {
            let ids = serde_json::json!({"run_id":run_id,"raw_artifact_id":bundle.raw.artifact_id,
                "normalized_artifact_id":bundle.normalized.artifact_id,"status":"canonical_store_committed"});
            store.commit_attempt(
                &task.permit,
                &[bundle.raw, bundle.normalized],
                TaskStatus::Succeeded,
                Utc::now(),
            )?;
            Ok(ids)
        }
        Err(error) => {
            store.commit_attempt(&task.permit, &[], TaskStatus::Failed, Utc::now())?;
            Err(error.into())
        }
    }
}
