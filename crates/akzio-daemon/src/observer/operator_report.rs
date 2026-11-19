use super::*;
use akzio_runtime::PAPER_COMMIT_RECIPE_ID;
use akzio_store::PaperSubmissionBatch;
use std::collections::BTreeMap;

pub(super) fn build(store: &Store, workflow: &WorkflowSnapshot) -> Result<Value> {
    let run_id = &workflow.run.run_id;
    if !matches!(
        workflow.run.purpose,
        RunPurpose::PositionPlan | RunPurpose::Paper
    ) || workflow.revision.graph.definition_version.is_none()
    {
        return Ok(json!({
            "version": 1,
            "run_id": run_id,
            "revision": workflow.revision.revision,
            "event_cursor": workflow.event_cursor,
            "status": "unsupported_workflow",
        }));
    }
    let decisions = committed_outputs(store, workflow, DECISION_GATE_RECIPE_ID)?;
    let decision_artifact = one_artifact(&decisions, ArtifactKind::Decision)?;
    let decision: Option<Decision> = decision_artifact.map(|a| decode(store, a)).transpose()?;
    let mut decision_blockers = Vec::new();
    let mut claims = Vec::new();
    let mut critiques = Vec::new();
    let mut draft = None;
    if let Some((artifact, payload)) = decision_artifact.zip(decision.as_ref()) {
        payload.validate()?;
        let context_artifact = source(store, run_id, artifact, &payload.decision_context)?;
        let context: DecisionContext = decode(store, &context_artifact)?;
        context.validate()?;
        if context.run_id != *run_id || context.target != payload.targets {
            return Err(invalid_lineage("DecisionContext does not match Decision"));
        }
        decision_blockers = context.hard_blockers;
        for reference in context.claims {
            let claim = source(store, run_id, &context_artifact, &reference)?;
            claims.push((reference, decode::<ResearchClaim>(store, &claim)?));
        }
        for reference in context.critiques {
            let critique = source(store, run_id, &context_artifact, &reference)?;
            critiques.push((reference, decode::<ResearchCritique>(store, &critique)?));
        }
        let mut proposals = artifact
            .source_refs
            .iter()
            .filter(|source| source.kind == ArtifactKind::DecisionProposal);
        if let Some(proposal) = proposals.next() {
            if proposals.next().is_some() {
                return Err(invalid_lineage("Decision has multiple proposal sources"));
            }
            let proposal = source(store, run_id, artifact, proposal)?;
            draft = Some(decode::<DecisionDraft>(store, &proposal)?);
        }
    }

    let (execution, plan, reconciliation) = execution(store, workflow, decision.as_ref())?;
    let research_plan = decision.as_ref().and_then(|d| d.research_plan.as_ref());
    let allocations = Asset::EXECUTABLE.map(|asset| {
        let formal = decision.as_ref().and_then(|d| d.targets.weights.get(&asset));
        let execution = plan.as_ref().and_then(|p| p.target.weights.get(&asset));
        let achieved = reconciliation
            .as_ref()
            .and_then(|r| r.achieved_target.as_ref())
            .and_then(|target| target.weights.get(&asset));
        json!({
            "asset": asset,
            "research_raw_ppm": research_plan.and_then(|p| allocation_weight(&p.raw, asset)),
            "research_validated_ppm": research_plan.and_then(|p| allocation_weight(&p.validated, asset)),
            "formal_target_ppm": formal,
            "execution_target_ppm": execution,
            "achieved_ppm": achieved,
        })
    });
    let mut counterevidence_without_asset_scope = BTreeMap::<_, BTreeSet<_>>::new();
    let forecasts = Asset::EXECUTABLE.into_iter().flat_map(|asset| {
        DecisionHorizon::ALL.map(|horizon| {
            let forecast = decision.as_ref().and_then(|d| {
                d.forecasts.iter().find(|f| f.asset == asset && f.horizon == horizon)
            });
            let scope = format!("forecast.{}.t{}", asset.symbol(), horizon.trading_days());
            let basis = draft.as_ref().and_then(|d| d.numeric_basis.iter().find(|b| b.scope == scope));
            let mut evidence = BTreeSet::new();
            if let Some(basis) = basis {
                evidence.extend(basis.inputs.iter());
            }
            let mut counterevidence = BTreeSet::new();
            let mut evidence_blocked = false;
            for (reference, claim) in &claims {
                evidence_blocked |= claim.evidence_gaps.iter().any(|gap| gap.blocks_slot(asset, horizon, claim.horizon));
                if claim.horizon == horizon {
                    evidence.extend(claim.grounds.iter().filter(|ground| ground.assets.is_empty() || ground.assets.contains(&asset)).map(|ground| &ground.evidence));
                }
                for (critique_ref, critique) in critiques.iter().filter(|(_, c)| &c.target == reference) {
                    let blocks = critique.blocks_slot(asset, horizon, claim.horizon);
                    evidence_blocked |= blocks;
                    let scoped_block = blocks && critique.evidence_gaps.iter().any(|gap| {
                        !gap.assets.is_empty() && gap.blocks_slot(asset, horizon, claim.horizon)
                    });
                    let mut scoped_conflict = false;
                    let mut unscoped_conflict = false;
                    if claim.horizon == horizon {
                        for ground in critique.grounds.iter().filter(|ground| {
                            critique.conflicting_refs.iter().any(|reference| reference.evidence == ground.evidence)
                        }) {
                            scoped_conflict |= ground.assets.contains(&asset);
                            unscoped_conflict |= ground.assets.is_empty();
                        }
                    }
                    if scoped_block || scoped_conflict {
                        counterevidence.insert(critique_ref);
                    }
                    if (blocks && !scoped_block) || unscoped_conflict {
                        counterevidence_without_asset_scope.entry(horizon).or_default().insert(critique_ref);
                    }
                }
            }
            json!({
                "asset": asset,
                "horizon": horizon,
                "status": match forecast {
                    Some(forecast) if forecast.is_neutral() => "neutral",
                    Some(_) => "directional",
                    None => "unavailable",
                },
                "evidence_coverage": if evidence_blocked { "blocked" } else if evidence.is_empty() { "unknown" } else { "referenced" },
                "expected_return_ppm": forecast.map(|f| f.expected_return_ppm),
                "positive_return_probability_ppm": forecast.map(|f| f.positive_return_probability_ppm),
                "evidence": evidence,
                "counterevidence": counterevidence,
            })
        })
    }).collect::<Vec<_>>();
    let approval = store.paper_approval_for_run(run_id)?.map(|(_, approval)| {
        json!({
            "approval_hash": approval.approval_hash,
            "scope": approval.scope,
            "runtime_manifest": approval.runtime_manifest,
            "approved_at": approval.approved_at,
            "expires_at": approval.expires_at,
        })
    });
    let outcome = sealed_outcome(store, run_id, decision_artifact)?;
    let stages = workflow
        .tasks
        .iter()
        .map(|task| {
            json!({
                "task_id": task.node.task_id,
                "recipe": task.node.recipe_id,
                "status": task.status,
                "horizon": task.node.execution_spec().horizon,
                "proposal_revision": task.node.execution_spec().proposal_revision,
                "attempts": task.attempt_count,
            })
        })
        .collect::<Vec<_>>();
    let usage = store.run_model_usage(run_id)?;
    let turns_with_usage = usage.turns - usage.turns_missing_usage;
    let complete_usage = usage.is_complete();
    let coverage = if complete_usage {
        "complete"
    } else if usage.turns == 0 {
        "not_observed"
    } else if turns_with_usage == 0 {
        "unknown"
    } else {
        "partial"
    };
    Ok(json!({
        "version": 1,
        "run_id": run_id,
        "revision": workflow.revision.revision,
        "event_cursor": workflow.event_cursor,
        "decision": decision_artifact.map(artifact_ref),
        "decision_blockers": decision_blockers,
        "forecasts": forecasts,
        "counterevidence_without_asset_scope": counterevidence_without_asset_scope,
        "allocations": allocations,
        "cash": research_plan.map(|plan| json!({
            "research_raw_ppm": plan.raw.cash_weight_ppm,
            "research_validated_ppm": plan.validated.cash_weight_ppm,
        })),
        "execution": execution,
        "approval": approval,
        "outcome": outcome,
        "stages": stages,
        "model_usage": {
            "source": "store_agent_turn_journal",
            "coverage": coverage,
            "token_accounting_version": usage.token_accounting_version,
            "turns": usage.turns,
            "turns_with_usage": turns_with_usage,
            "turns_missing_usage": usage.turns_missing_usage,
            "input_tokens": complete_usage.then_some(usage.input_tokens),
            "output_tokens": complete_usage.then_some(usage.output_tokens),
            "total_tokens": usage.billable_tokens_if_complete(),
        },
    }))
}

fn execution(
    store: &Store,
    workflow: &WorkflowSnapshot,
    decision: Option<&Decision>,
) -> Result<(Value, Option<ExecutionPlan>, Option<Reconciliation>)> {
    let outputs = committed_outputs(store, workflow, EXECUTION_GATE_RECIPE_ID)?;
    let Some(artifact) = one_artifact(&outputs, ArtifactKind::ExecutionVerdict)? else {
        let status = if workflow
            .tasks
            .iter()
            .any(|task| task.node.recipe_id.as_str() == EXECUTION_GATE_RECIPE_ID)
        {
            "unavailable"
        } else {
            "not_applicable"
        };
        return Ok((json!({"status": status}), None, None));
    };
    let run_id = &workflow.run.run_id;
    let verdict: ExecutionVerdict = decode(store, artifact)?;
    verdict.validate()?;
    let (context_ref, status, blockers) = match &verdict {
        ExecutionVerdict::Accepted { execution_context } => {
            (execution_context, "accepted", &[][..])
        }
        ExecutionVerdict::NoOrder { no_order } => (
            &no_order.execution_context,
            "no_order",
            no_order.blockers.as_slice(),
        ),
    };
    let context_artifact = source(store, run_id, artifact, context_ref)?;
    let context: ExecutionContext = decode(store, &context_artifact)?;
    context.validate()?;
    if context.run_id != *run_id
        || decision.is_none_or(|decision| decision.decision_context != context.decision_context)
    {
        return Err(invalid_lineage("ExecutionContext does not match Decision"));
    }
    let plan = if let Some(reference) = &context.execution_plan {
        let artifact = source(store, run_id, &context_artifact, reference)?;
        let plan: ExecutionPlan = decode(store, &artifact)?;
        plan.validate()?;
        if context.plan_hash.as_ref() != Some(&plan.plan_hash)
            || plan.decision_context != context.decision_context
        {
            return Err(invalid_lineage(
                "ExecutionPlan does not match ExecutionContext",
            ));
        }
        Some(plan)
    } else {
        None
    };
    let outputs = committed_outputs(store, workflow, RECONCILE_RECIPE_ID)?;
    let reconciliation_artifact = one_artifact(&outputs, ArtifactKind::Reconciliation)?;
    let mut receipts = Vec::new();
    let reconciliation = if let Some(artifact) = reconciliation_artifact {
        let reconciliation: Reconciliation = decode(store, artifact)?;
        reconciliation.validate()?;
        let commitment = source(store, run_id, artifact, &reconciliation.commitment)?;
        let commitment: PaperCommitment = decode(store, &commitment)?;
        commitment.validate()?;
        if commitment.execution_context != *context_ref
            || context.plan_hash.as_ref() != Some(&commitment.plan_hash)
        {
            return Err(invalid_lineage(
                "Reconciliation does not match ExecutionContext",
            ));
        }
        for reference in &reconciliation.broker_receipts {
            let receipt_artifact = source(store, run_id, artifact, reference)?;
            let receipt: OrderReceipt = decode(store, &receipt_artifact)?;
            receipt.validate()?;
            if receipt.plan_hash != commitment.plan_hash {
                return Err(invalid_lineage(
                    "OrderReceipt does not match PaperCommitment",
                ));
            }
            receipts.push(receipt_view(reference, &receipt));
        }
        Some(reconciliation)
    } else {
        None
    };
    let (submission_journal, observations) = if reconciliation.is_none() && status == "accepted" {
        pending_submissions(store, workflow, artifact, &context, context_ref)?
    } else {
        (None, None)
    };
    let mut receipt_source = reconciliation.as_ref().map(|_| "reconciliation");
    if let Some(observations) = observations {
        receipt_source = Some(observations.source);
        receipts = observations.receipts;
    }
    let observed_receipt_count =
        (status == "no_order" || receipt_source.is_some()).then_some(receipts.len());
    Ok((
        json!({
            "status": status,
            "verdict": artifact_ref(artifact),
            "context": context_ref,
            "plan": context.execution_plan,
            "planned_order_count": plan.as_ref().map(|plan| plan.orders.len()).or((status == "no_order").then_some(0)),
            "reconciliation": reconciliation_artifact.map(artifact_ref),
            "reconciliation_state": reconciliation.as_ref().map(|r| r.state),
            "submission_journal": submission_journal,
            "receipt_source": receipt_source,
            "observed_receipt_count": observed_receipt_count,
            "receipts": receipts,
            "blockers": blockers,
        }),
        plan,
        reconciliation,
    ))
}

struct ReceiptObservations {
    source: &'static str,
    receipts: Vec<Value>,
}

fn pending_submissions(
    store: &Store,
    workflow: &WorkflowSnapshot,
    verdict: &Artifact,
    context: &ExecutionContext,
    context_ref: &ArtifactRef,
) -> Result<(Option<Value>, Option<ReceiptObservations>)> {
    let outputs = committed_outputs(store, workflow, PAPER_COMMIT_RECIPE_ID)?;
    let Some(artifact) = one_artifact(&outputs, ArtifactKind::ExecutionCommitment)? else {
        return Ok((None, None));
    };
    let run_id = &workflow.run.run_id;
    let commitment_ref = artifact_ref(artifact);
    let commitment: PaperCommitment = decode(store, artifact)?;
    commitment.validate()?;
    if &commitment.execution_context != context_ref
        || context.plan_hash.as_ref() != Some(&commitment.plan_hash)
        || context.broker_session.as_deref() != Some(&commitment.broker_session)
        || !artifact.source_refs.contains(context_ref)
        || !artifact.source_refs.contains(&artifact_ref(verdict))
    {
        return Err(invalid_lineage(
            "PaperCommitment does not match ExecutionContext and Verdict",
        ));
    }

    let (observation_only, orders) = match store.paper_submission_batch(&commitment_ref)? {
        None => {
            return Ok((
                Some(json!({"state":"not_started", "commitment":commitment_ref})),
                None,
            ))
        }
        Some(PaperSubmissionBatch::LegacyUnknown) => {
            let receipts: Vec<_> = store
                .legacy_paper_submission_receipts(&commitment_ref)?
                .into_iter()
                .map(|(reference, receipt)| {
                    let mut view = receipt_view(&reference, &receipt);
                    view["client_order_id"] = json!(receipt.client_order_id);
                    view
                })
                .collect();
            let observations = (!receipts.is_empty()).then_some(ReceiptObservations {
                source: "legacy_submission_receipts",
                receipts,
            });
            return Ok((
                Some(json!({"state":"legacy_unknown", "commitment":commitment_ref})),
                observations,
            ));
        }
        Some(PaperSubmissionBatch::Journaled {
            observation_only,
            orders,
            ..
        }) => (observation_only, orders),
    };
    let mut receipts = Vec::new();
    let mut journal = Vec::with_capacity(orders.len());
    for order in orders {
        if let Some(reference) = &order.receipt {
            let artifact = store.artifact(&reference.artifact_id)?;
            let source_refs = std::iter::once(&commitment_ref).chain(order.reprice.iter());
            if artifact.kind != ArtifactKind::OrderReceipt
                || reference.kind != artifact.kind
                || artifact.lifecycle != ArtifactLifecycle::Canonical
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(run_id)
                || artifact.source_refs.iter().collect::<BTreeSet<_>>() != source_refs.collect()
            {
                return Err(invalid_lineage(
                    "journal receipt sources do not match exact order",
                ));
            }
            let receipt: OrderReceipt = decode(store, &artifact)?;
            receipt.validate()?;
            if receipt.plan_hash != commitment.plan_hash
                || receipt.asset != order.asset
                || receipt.client_order_id != order.client_order_id
            {
                return Err(invalid_lineage(
                    "journal receipt payload does not match exact order",
                ));
            }
            let mut view = receipt_view(reference, &receipt);
            view["client_order_id"] = json!(order.client_order_id);
            view["reprice"] = json!(order.reprice);
            receipts.push(view);
        }
        journal.push(json!({
            "asset": order.asset,
            "client_order_id": order.client_order_id,
            "transition_index": order.transition_index,
            "state": order.state,
            "receipt": order.receipt,
            "reprice": order.reprice,
            "reason": order.reason,
        }));
    }
    Ok((
        Some(json!({
            "state": "journaled",
            "commitment": commitment_ref,
            "observation_only": observation_only,
            "orders": journal,
        })),
        Some(ReceiptObservations {
            source: "submission_journal",
            receipts,
        }),
    ))
}

fn receipt_view(reference: &ArtifactRef, receipt: &OrderReceipt) -> Value {
    json!({
        "artifact": reference,
        "asset": receipt.asset,
        "state": receipt.state,
        "requested_quantity_micros": receipt.requested_quantity_micros,
        "filled_quantity_micros": receipt.filled_quantity_micros,
        "remaining_quantity_micros": receipt.remaining_quantity_micros,
    })
}

fn sealed_outcome(
    store: &Store,
    run_id: &RunId,
    decision: Option<&Artifact>,
) -> Result<Option<Value>> {
    let Some(artifact) = store.outcome_for_run(run_id)? else {
        return Ok(None);
    };
    let outcome: Outcome = decode(store, &artifact)?;
    outcome.validate_sealed()?;
    let schedule = source(store, run_id, &artifact, &outcome.schedule)?;
    let schedule: OutcomeSchedule = decode(store, &schedule)?;
    schedule.validate()?;
    if outcome.outcome_id != schedule.outcome_id
        || decision.is_none_or(|decision| artifact_ref(decision) != schedule.decision)
    {
        return Err(invalid_lineage("sealed Outcome does not match Decision"));
    }
    Ok(Some(json!({
        "artifact": artifact_ref(&artifact),
        "schedule": outcome.schedule,
        "sealed_at": outcome.sealed_at,
        "horizons": outcome.windows.iter().map(|window| window.horizon).collect::<Vec<_>>(),
    })))
}

fn committed_outputs(
    store: &Store,
    workflow: &WorkflowSnapshot,
    recipe: &str,
) -> Result<Vec<Artifact>> {
    let mut tasks = workflow
        .tasks
        .iter()
        .filter(|task| task.node.recipe_id.as_str() == recipe);
    let Some(task) = tasks.next() else {
        return Ok(Vec::new());
    };
    if tasks.next().is_some() {
        return Err(invalid_lineage("workflow has duplicate terminal recipe"));
    }
    if task.status != TaskStatus::Succeeded {
        return Ok(Vec::new());
    }
    Ok(store.succeeded_task_outputs_or_empty(&workflow.run.run_id, &task.node.task_id)?)
}

fn one_artifact(artifacts: &[Artifact], kind: ArtifactKind) -> Result<Option<&Artifact>> {
    let mut matching = artifacts.iter().filter(|a| a.kind == kind);
    let artifact = matching.next();
    if matching.next().is_some() {
        return Err(invalid_lineage("committed task has duplicate output kind"));
    }
    Ok(artifact)
}

fn source(
    store: &Store,
    run_id: &RunId,
    parent: &Artifact,
    reference: &ArtifactRef,
) -> Result<Artifact> {
    if !parent.source_refs.contains(reference) {
        return Err(invalid_lineage("artifact source is missing"));
    }
    let artifact = store.artifact(&reference.artifact_id)?;
    if artifact.kind != reference.kind
        || artifact.origin.as_ref().and_then(|o| o.run_id.as_ref()) != Some(run_id)
    {
        return Err(invalid_lineage("artifact source has the wrong kind or Run"));
    }
    Ok(artifact)
}

fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn decode<T: DeserializeOwned>(store: &Store, artifact: &Artifact) -> Result<T> {
    artifact.validate()?;
    Ok(serde_json::from_slice(&store.read_blob(&artifact.blob)?)?)
}

fn allocation_weight(plan: &ResearchAllocationPlan, asset: Asset) -> Option<WeightPpm> {
    plan.allocations
        .iter()
        .find(|row| row.asset == asset)
        .map(|row| row.target_weight_ppm)
}

fn invalid_lineage(reason: &str) -> DaemonError {
    StoreError::Integrity(format!("operator report lineage: {reason}")).into()
}
