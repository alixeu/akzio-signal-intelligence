use super::*;
use akzio_domain::{ProposalReview, ResearchSettings, RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID};
use akzio_model::ModelClient;
use akzio_research::ModelClientAdapter;
use serde_json::json;

fn review_model(accepted: bool) -> ModelClientAdapter {
    let arguments = json!({"result":{"assessments":akzio_domain::proposal_review_keys().into_iter().map(|scope|
        json!({"scope":scope,"accepted":accepted,"rationale":"Offline review scenario", "evidence_refs":[],
            "issues":if accepted {vec![]} else {vec![json!({"category":"estimate_basis_mismatch","field_path":scope,
                "correction_criterion":"Repair the rejected estimate","evidence_refs":[]})]}})).collect::<Vec<_>>()},
        "deliberation":{"selected_path":"Offline review scenario","alternatives":[],"alternative_match_ppm":[],
            "uncertainties":[],"uncertainty_weight_ppm":[],"basis_artifact_ids":[],"confidence_ppm":1000000}});
    ModelClientAdapter::new(ModelClient::fixture_by_purpose_phase(BTreeMap::from([(
        RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID.to_owned(),
        [
            serde_json::Value::Null,
            json!({"output":[{"type":"function_call","call_id":"review","name":"submit_result","arguments":arguments.to_string()}]}),
        ],
    )])))
}

#[tokio::test]
async fn bounded_review_revisions_and_direct_gate_rejection() {
    for (limit, accept_at, skip_review) in [
        (0, Some(0), false),
        (2, Some(1), false),
        (2, None, false),
        (0, None, true),
    ] {
        let mut d = daemon(true);
        d.workflow = d
            .workflow
            .clone()
            .with_research_settings(&ResearchSettings {
                max_proposal_revisions: limit,
            })
            .unwrap();
        let session = d
            .prepare_debug(&DebugPrepareRequest {
                purpose: RunPurpose::PositionPlan,
                session_key: Utc::now().format("%Y-%m-%d").to_string(),
                paper_allowed: false,
            })
            .unwrap();
        let run = session.identity.run_id;
        let frozen = d.workflow.recover(&run).unwrap().revision.graph;
        assert_eq!(frozen.nodes.len(), 17 + 2 * usize::from(limit));
        // Reloading settings cannot allocate a fresh revision allowance.
        d.workflow = d
            .workflow
            .clone()
            .with_research_settings(&ResearchSettings {
                max_proposal_revisions: 0,
            })
            .unwrap();
        let mut reviewed = 0;
        for _ in 0..frozen.nodes.len() {
            let view = d.inspect_debug(&run, None, None).unwrap();
            let node = view
                .nodes
                .iter()
                .find(|n| n.step_eligible)
                .expect("bounded task ready");
            let role = node.role.clone();
            let rejects_gate = role == "gate.decision" && accept_at.is_none();
            d.model = if role == RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID {
                review_model(accept_at.is_some_and(|n| reviewed >= n))
            } else {
                ModelClientAdapter::new(fixture_model_client())
            };
            if role == RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID {
                reviewed += 1;
            }
            step(&d, &run, &node.task.node.task_id);
            let executor = d.clone();
            d.task_runtime
                .run_one("review-regression", move |task| async move {
                    if skip_review
                        && task.node.recipe_id.as_str() == RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
                    {
                        return TaskCompletion::Skipped;
                    }
                    if rejects_gate {
                        let proposal = executor
                            .store
                            .final_proposal_review(&task.run_id, &task.node.task_id)
                            .unwrap()
                            .map(|(_, r)| r.proposal)
                            .unwrap_or_else(|| {
                                let a = executor
                                    .store
                                    .run_artifacts_by_kind(
                                        &task.run_id,
                                        ArtifactKind::DecisionProposal,
                                    )
                                    .unwrap()
                                    .pop()
                                    .unwrap();
                                ArtifactRef {
                                    artifact_id: a.artifact_id,
                                    kind: a.kind,
                                }
                            });
                        // Invoke the Gate directly, bypassing daemon proposal selection.
                        let result = executor.decision_runtime.decide(&DecisionGateInput {
                            permit: task.permit.clone(),
                            proposal,
                            now: Utc::now(),
                        });
                        assert!(
                            matches!(
                                result,
                                Err(akzio_execution::DecisionGateError::ProposalReviewRequired)
                            ),
                            "{result:?}"
                        );
                        return TaskCompletion::Failed;
                    }
                    executor
                        .execute_task_inner(&task, Utc::now())
                        .await
                        .unwrap()
                })
                .await
                .unwrap();
        }
        assert_eq!(d.workflow.recover(&run).unwrap().revision.graph, frozen);
        let proposals = d
            .store
            .run_artifacts_by_kind(&run, ArtifactKind::DecisionProposal)
            .unwrap();
        let reviews = d
            .store
            .run_artifacts_by_kind(&run, ArtifactKind::ProposalReview)
            .unwrap();
        let expected = accept_at.map_or((limit + 1).min(2), |n| n + 1);
        assert_eq!(proposals.len(), usize::from(expected));
        let audit = d.store.research_audit(&run).unwrap();
        if limit == 2 && accept_at.is_none() && !skip_review {
            assert_eq!(
                audit
                    .records
                    .iter()
                    .filter(|r| r.producer == "research.revision.stop")
                    .count(),
                1
            );
            assert_eq!(audit.progress["review_status"], "rejected");
        }
        assert_eq!(
            reviews.len(),
            if skip_review {
                0
            } else {
                usize::from(expected)
            }
        );
        for artifact in reviews {
            let review: ProposalReview = d
                .read_artifact_payload(&ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: ArtifactKind::ProposalReview,
                })
                .unwrap();
            let proposal = d.store.artifact(&review.proposal.artifact_id).unwrap();
            assert_eq!(proposal.blob.hash, review.proposal_hash);
            let original: serde_json::Value =
                serde_json::from_slice(&d.store.read_blob(&proposal.blob).unwrap()).unwrap();
            assert_eq!(
                akzio_domain::content_hash_json(&original).unwrap(),
                proposal.blob.hash
            );
            for (pointer, replacement) in [
                ("/forecasts/0/expected_return_ppm", json!(1)),
                ("/forecasts/0/positive_return_probability_ppm", json!(1)),
                ("/research_allocation/cash_weight_ppm", json!(1)),
                (
                    "/numeric_basis/0/method",
                    json!("changed numeric reasoning"),
                ),
            ] {
                let mut changed = original.clone();
                let field = changed.pointer_mut(pointer).unwrap();
                assert_ne!(*field, replacement);
                *field = replacement;
                assert!(
                    !review.authorizes(
                        &review.proposal,
                        &akzio_domain::content_hash_json(&changed).unwrap()
                    ),
                    "old review authorized modified {pointer}"
                );
            }
        }
        let view = d.inspect_debug(&run, None, None).unwrap();
        assert_eq!(view.research["research_status"], "completed");
        assert_eq!(view.research["max_proposal_revisions"], limit);
        assert_eq!(
            d.store
                .run_artifacts_by_kind(&run, ArtifactKind::Decision)
                .unwrap()
                .len(),
            usize::from(accept_at.is_some())
        );
        assert!(view
            .nodes
            .iter()
            .filter(|n| n.task.node.execution_spec().research_round == Some(1))
            .all(|n| n.task.status == TaskStatus::Skipped));
        d.store.verify_integrity().unwrap();
    }
}

fn analyst_with_scoped_gap() -> ModelClientAdapter {
    let mut result = akzio_research::fixture_claim_output();
    result["horizon"] = json!("$fixture.task.horizon");
    result["grounds"][0]["evidence"] = json!(
        "$fixture.schema.first_ref:/properties/result/properties/grounds/items/properties/evidence"
    );
    result["evidence_gaps"] = json!([{"topic":"coverage","rationale":"Asset coverage needs review","impact":"warning","retriable":false,
        "assets":["TQQQ","QQQ","SOXX","SOXL"],"horizons":[],"supplemental_requests":[]}]);
    let args = json!({"result":result,"deliberation":{"selected_path":"Scoped coverage","alternatives":[],"alternative_match_ppm":[],
        "uncertainties":[],"uncertainty_weight_ppm":[],"basis_artifact_ids":[],"confidence_ppm":1000000}});
    ModelClientAdapter::new(ModelClient::fixture_by_purpose_phase(BTreeMap::from([(
        "research.analyst".into(),
        [
            serde_json::Value::Null,
            json!({"output":[{"type":"function_call","call_id":"analyst","name":"submit_result","arguments":args.to_string()}]}),
        ],
    )])))
}

fn critic_with_requests() -> ModelClientAdapter {
    let mut result = akzio_research::fixture_critique_output();
    result["target"] = json!("$fixture.schema.first_ref:/properties/result/properties/target");
    result["blocker"] = json!(true);
    let intent = |kind, series| json!({"kind":kind,"assets":["TQQQ","QQQ","SOXX","SOXL"],"series":series,"query":"Resolve the scoped blocker"});
    result["evidence_gaps"] = json!([
        {"topic":"retry data","rationale":"material retriable blocker","impact":"blocks_directional_forecast","retriable":true,
         "assets":["TQQQ","QQQ","SOXX","SOXL"],"horizons":[],"supplemental_requests":[
            intent("price",json!([])),intent("news",json!([])),intent("macro",json!(["DFF","DFII10","VIXCLS","DGS2","DGS10"]))]},
        {"topic":"optional context","rationale":"warning is not an acquisition authorization","impact":"warning","retriable":true,
         "assets":[],"horizons":[],"supplemental_requests":[intent("news",json!([]))]}]);
    let args = json!({"result":result,"deliberation":{"selected_path":"Request governed evidence","alternatives":[],"alternative_match_ppm":[],
        "uncertainties":[],"uncertainty_weight_ppm":[],"basis_artifact_ids":[],"confidence_ppm":1000000}});
    ModelClientAdapter::new(ModelClient::fixture_by_purpose_phase(BTreeMap::from([(
        "research.critic".into(),
        [
            serde_json::Value::Null,
            json!({"output":[{"type":"function_call","call_id":"critic","name":"submit_result","arguments":args.to_string()}]}),
        ],
    )])))
}

#[tokio::test]
async fn critic_supplement_is_shared_deduplicated_and_recovery_keeps_eight_resource_budget() {
    for new_fact in [false, true] {
        let mut d = daemon(true);
        let session = d
            .prepare_debug(&DebugPrepareRequest {
                purpose: RunPurpose::PositionPlan,
                session_key: Utc::now().format("%Y-%m-%d").to_string(),
                paper_allowed: false,
            })
            .unwrap();
        let run = session.identity.run_id;
        let fixed = Utc::now() - Duration::minutes(2);
        for r in session
            .identity
            .dataset
            .iter()
            .filter(|r| r.kind == ArtifactKind::EvidenceNeed)
        {
            let need: EvidenceNeed = d.read_artifact_payload(r).unwrap();
            let source = evidence_source(&need.source_family).unwrap();
            Arc::make_mut(&mut d.fixture_evidence)
                .entry(source)
                .or_default()
                .insert(
                    need.resource.clone(),
                    debug_fixture_evidence(source, &need.resource, fixed),
                );
        }
        for _ in 0..21 {
            let view = d.inspect_debug(&run, None, None).unwrap();
            let node = view.nodes.iter().find(|n| n.step_eligible).unwrap();
            d.model = if node.role == "research.critic"
                && node.task.node.execution_spec().research_round == Some(0)
            {
                critic_with_requests()
            } else if node.role == "research.analyst"
                && node.task.node.execution_spec().research_round == Some(0)
            {
                analyst_with_scoped_gap()
            } else {
                ModelClientAdapter::new(fixture_model_client())
            };
            if new_fact && node.role == akzio_domain::RESEARCH_SUPPLEMENT_RECIPE_ID {
                let evidence = Arc::make_mut(&mut d.fixture_evidence)
                    .get_mut(&EvidenceSource::Alpaca)
                    .unwrap()
                    .iter_mut()
                    .find(|(r, _)| r.starts_with("bars:QQQ:"))
                    .unwrap()
                    .1;
                evidence.normalized["bars"][0]["c"] = json!(100.8);
                evidence.raw = serde_json::to_vec(&evidence.normalized).unwrap();
            }
            step(&d, &run, &node.task.node.task_id);
            let executor = d.clone();
            d.task_runtime
                .run_one("supplement-regression", move |task| async move {
                    if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_SUPPLEMENT_RECIPE_ID {
                        if !new_fact {
                            let requester = executor
                                .ancestor_outputs(&task)
                                .unwrap()
                                .into_iter()
                                .find(|a| a.kind == ArtifactKind::Critique)
                                .unwrap();
                            let key = executor.fixture_evidence[&EvidenceSource::Alpaca]
                                .keys()
                                .find(|r| r.starts_with("bars:QQQ:"))
                                .unwrap();
                            let marker = Artifact::new(
                                ArtifactKind::SemanticDetail,
                                executor.store.stage_json(&json!({"resource":key})).unwrap(),
                                "research.supplement.started",
                                ArtifactLifecycle::RunScoped,
                                ArtifactProvenance {
                                    source_family: "akzio.ingest".into(),
                                    observed_at: None,
                                    retrieved_at: Utc::now(),
                                    source_uri: None,
                                    confidence_ppm: 1_000_000,
                                    producer_contract_hash: None,
                                },
                                Some(task.permit.artifact_origin()),
                                vec![ArtifactRef {
                                    artifact_id: requester.artifact_id,
                                    kind: requester.kind,
                                }],
                                Utc::now(),
                            )
                            .unwrap();
                            executor
                                .store
                                .write_task_artifact(
                                    &task.permit,
                                    &marker,
                                    LifecycleEventType::ArtifactCommitted,
                                    Utc::now(),
                                )
                                .unwrap();
                        }
                        let read_round = |completion: TaskCompletion| {
                            let TaskCompletion::Succeeded(outputs) = completion else {
                                panic!("supplement output missing")
                            };
                            let round: akzio_domain::SupplementalRound = serde_json::from_slice(
                                &executor.store.read_blob(&outputs[0].blob).unwrap(),
                            )
                            .unwrap();
                            (round, outputs)
                        };
                        let (first, _) = read_round(
                            executor
                                .shared_research_supplement(&task, Utc::now())
                                .await
                                .unwrap(),
                        );
                        assert_eq!(
                            first.evidence.len(),
                            usize::from(new_fact),
                            "only changed admissible price facts trigger reruns"
                        );
                        assert_eq!(first.affected_horizons.len(), if new_fact { 3 } else { 0 });
                        if !new_fact {
                            assert!(first
                                .dispositions
                                .iter()
                                .any(|r| r.status == "unknown_after_crash"));
                        }
                        assert!(first
                            .dispositions
                            .iter()
                            .any(|r| r.status == "deduplicated"));
                        assert!(first
                            .dispositions
                            .iter()
                            .any(|r| r.status == "budget_exhausted"));
                        assert!(first
                            .dispositions
                            .iter()
                            .any(|r| r.status == "skipped_impact"));
                        assert!(first
                            .dispositions
                            .iter()
                            .any(|r| r.status == "no_new_facts"));
                        // Replay the Rust coordination after durable records exist but before
                        // the task completion; acquisition usage must remain eight resources.
                        let (again, outputs) = read_round(
                            executor
                                .shared_research_supplement(&task, Utc::now())
                                .await
                                .unwrap(),
                        );
                        assert_eq!(again.evidence, first.evidence);
                        let starts = executor
                            .store
                            .run_artifacts_by_kind(&task.run_id, ArtifactKind::SemanticDetail)
                            .unwrap()
                            .into_iter()
                            .filter(|a| a.producer == "research.supplement.started")
                            .count();
                        assert_eq!(starts, 8);
                        return TaskCompletion::Succeeded(outputs);
                    }
                    executor
                        .execute_task_inner(&task, Utc::now())
                        .await
                        .unwrap()
                })
                .await
                .unwrap();
        }
        let view = d.inspect_debug(&run, None, None).unwrap();
        assert!(view
            .nodes
            .iter()
            .filter(|n| n.task.node.execution_spec().research_round == Some(1))
            .all(|n| n.task.status
                == if new_fact {
                    TaskStatus::Succeeded
                } else {
                    TaskStatus::Skipped
                }));
        assert_eq!(
            d.store
                .run_artifacts_by_kind(&run, ArtifactKind::Claim)
                .unwrap()
                .len(),
            if new_fact { 6 } else { 3 }
        );
        let proposal = d
            .store
            .run_artifacts_by_kind(&run, ArtifactKind::DecisionProposal)
            .unwrap()
            .pop()
            .unwrap();
        let proposal: akzio_domain::DecisionDraft = d
            .read_artifact_payload(&ArtifactRef {
                artifact_id: proposal.artifact_id,
                kind: ArtifactKind::DecisionProposal,
            })
            .unwrap();
        assert_eq!(proposal.claims.len(), 3);
        for claim in proposal.claims {
            let artifact = d.store.artifact(&claim.artifact_id).unwrap();
            let task = artifact.origin.unwrap().task_id.unwrap();
            let node = view
                .nodes
                .iter()
                .find(|n| n.task.node.task_id == task)
                .unwrap();
            assert_eq!(
                node.task.node.execution_spec().research_round,
                Some(if new_fact { 1 } else { 0 })
            );
        }
        d.store.verify_integrity().unwrap();
    }
}
