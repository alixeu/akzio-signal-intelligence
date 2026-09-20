use super::*;
use akzio_domain::{ContextManifestPayload, ProposalReview};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchAudit {
    pub version: u32,
    pub progress: serde_json::Value,
    pub records: Vec<ResearchAuditRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchAuditRecord {
    pub artifact_id: ArtifactId,
    pub kind: ArtifactKind,
    pub producer: String,
    pub origin: Option<akzio_domain::ArtifactOrigin>,
    pub task_id: Option<TaskId>,
    pub proposal_revision: Option<u8>,
    pub payload: serde_json::Value,
    pub source_refs: Vec<ArtifactRef>,
}

pub(super) fn build_research_audit(
    workflow: &serde_json::Value,
    progress: serde_json::Value,
    artifacts: &[(&Artifact, &serde_json::Value)],
) -> ResearchAudit {
    let records = artifacts
        .iter()
        .filter(|(a, _)| {
            matches!(
                a.kind,
                ArtifactKind::DecisionProposal | ArtifactKind::ProposalReview
            ) || a.producer.starts_with("research.supplement.")
                || matches!(
                    a.producer.as_str(),
                    "research.revision.stop"
                        | "context.coverage"
                        | "learning.retrieval.audit"
                        | "learning.revalidation.suggestion"
                )
        })
        .map(|(a, payload)| {
            let task_id = a.origin.as_ref().and_then(|o| o.task_id.clone());
            let revision = task_id
                .as_ref()
                .and_then(|id| {
                    workflow["tasks"]
                        .as_array()?
                        .iter()
                        .find(|t| t["node"]["task_id"] == id.0)
                })
                .and_then(|t| akzio_domain::projected_node_spec(&t["node"]))
                .and_then(|s| s.proposal_revision);
            let mut payload = (*payload).clone();
            if a.kind == ArtifactKind::ProposalReview {
                if let Ok(review) = serde_json::from_value::<ProposalReview>(payload.clone()) {
                    payload["issue_ids"] = serde_json::json!(review
                        .assessments
                        .iter()
                        .flat_map(|assessment| assessment
                            .issues
                            .iter()
                            .map(move |issue| issue.stable_id(&assessment.scope)))
                        .collect::<Vec<_>>());
                    for (assessment_index, assessment) in review.assessments.iter().enumerate() {
                        for (issue_index, issue) in assessment.issues.iter().enumerate() {
                            payload["assessments"][assessment_index]["issues"][issue_index]
                                ["issue_id"] =
                                serde_json::json!(issue.stable_id(&assessment.scope));
                        }
                    }
                }
            }
            ResearchAuditRecord {
                artifact_id: a.artifact_id.clone(),
                kind: a.kind,
                producer: a.producer.clone(),
                origin: a.origin.clone(),
                task_id,
                proposal_revision: revision,
                payload,
                source_refs: a.source_refs.clone(),
            }
        })
        .collect();
    ResearchAudit {
        version: 1,
        progress,
        records,
    }
}

impl Store {
    /// Complete run-scoped audit, independent of the Observer trajectory window.
    /// Immutable CAS payloads and scheduling state are read from one SQL snapshot.
    pub fn research_audit(&self, run_id: &RunId) -> StoreResult<ResearchAudit> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let workflow = serde_json::to_value(self.workflow_snapshot_with_connection(&tx, run_id)?)?;
        let ids = tx.prepare("SELECT DISTINCT a.artifact_id FROM rebuild_artifacts a JOIN rebuild_events e ON e.artifact_id=a.artifact_id WHERE e.run_id=?1 AND (a.kind IN ('decision_proposal','proposal_review') OR a.producer IN ('context.coverage','learning.retrieval.audit','learning.revalidation.suggestion','research.revision.stop') OR a.producer LIKE 'research.supplement.%') ORDER BY a.created_at,a.artifact_id")?
            .query_map(params![run_id.0], |row| row.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
        let mut artifacts = Vec::new();
        for id in ids {
            let artifact = read_artifact(&tx, &ArtifactId(ContentHash::new(id)?))?;
            let value: serde_json::Value = serde_json::from_slice(&blob::read_blob_bytes(
                &tx,
                &artifact.blob.hash,
                artifact.blob.bytes,
            )?)?;
            artifacts.push((artifact, value));
        }
        let committed = tx.prepare("SELECT o.artifact_id FROM rebuild_attempt_outputs o JOIN rebuild_attempts a ON a.attempt_id=o.attempt_id WHERE a.run_id=?1 AND a.status='succeeded'")?
            .query_map(params![run_id.0],|row| row.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?
            .into_iter().map(|id| Ok(ArtifactId(ContentHash::new(id)?))).collect::<StoreResult<BTreeSet<_>>>()?;
        let missing = debug::read_session(&tx, run_id)?
            .is_some_and(|s| s.identity.research_only_without_policy());
        let refs = artifacts.iter().map(|(a, v)| (a, v)).collect::<Vec<_>>();
        let progress = research_progress(
            workflow["tasks"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default(),
            &refs,
            &committed,
            missing,
        );
        let audit = build_research_audit(&workflow, progress, &refs);
        tx.commit()?;
        Ok(audit)
    }
}

pub(super) fn research_progress(
    tasks: &[serde_json::Value],
    reviews: &[(&Artifact, &serde_json::Value)],
    committed: &BTreeSet<ArtifactId>,
    policy_missing: bool,
) -> serde_json::Value {
    use serde_json::json;
    let research = tasks
        .iter()
        .filter(|t| {
            t["node"]["recipe_id"]
                .as_str()
                .is_some_and(|s| s.starts_with("research.") || s == "gate.evidence")
        })
        .collect::<Vec<_>>();
    let complete = !research.is_empty()
        && research
            .iter()
            .all(|t| matches!(t["status"].as_str(), Some("succeeded" | "skipped")));
    let failed = research
        .iter()
        .any(|t| matches!(t["status"].as_str(), Some("failed" | "cancelled")));
    let revision = |node: &serde_json::Value| {
        akzio_domain::projected_node_spec(node).and_then(|s| s.proposal_revision)
    };
    let max_revisions = research.iter().filter_map(|t| revision(&t["node"])).max();
    let latest = reviews
        .iter()
        .filter(|(a, _)| {
            a.kind == ArtifactKind::ProposalReview && committed.contains(&a.artifact_id)
        })
        .filter_map(|(a, v)| {
            let id = a.origin.as_ref()?.task_id.as_ref()?;
            let task = tasks.iter().find(|t| t["node"]["task_id"] == id.0)?;
            Some((
                revision(&task["node"])?,
                serde_json::from_value::<ProposalReview>((*v).clone()).ok()?,
            ))
        })
        .max_by_key(|(revision, _)| *revision);
    let review_failed = research.iter().any(|t| {
        t["node"]["recipe_id"] == akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
            && t["status"] == "failed"
    });
    let review_status = match &latest {
        _ if review_failed => "failed",
        Some((_, r)) if r.accepted() => "accepted",
        Some(_) => "rejected",
        None if max_revisions.is_none() => "not_required_historical",
        None => "not_reached",
    };
    let mut reasons = Vec::new();
    if policy_missing {
        reasons.push("policy_missing");
    }
    if complete && review_status == "rejected" {
        reasons.push("proposal_review_rejected");
    }
    if complete && review_status == "not_reached" {
        reasons.push("proposal_review_missing");
    }
    if review_failed {
        reasons.push("proposal_review_failed");
    }
    if failed {
        reasons.push("research_failed");
    }
    let decision = tasks
        .iter()
        .find(|t| t["node"]["recipe_id"] == "gate.decision");
    if decision.is_some_and(|t| t["status"] == "failed") {
        reasons.push("decision_gate_failed");
    }
    let decision_status = if decision.is_some_and(|t| t["status"] == "succeeded") {
        "completed"
    } else if !reasons.is_empty() {
        "blocked"
    } else if complete {
        "eligible"
    } else {
        "not_reached"
    };
    let research_status = if failed {
        "failed"
    } else if complete {
        "completed"
    } else {
        "in_progress"
    };
    let display = format!(
        "{} · {}{}",
        match research_status {
            "completed" => "研究完成",
            "failed" => "研究失败",
            _ => "研究进行中",
        },
        match review_status {
            "accepted" => "终稿审查通过",
            "failed" => "终稿审查失败",
            "rejected" if complete => "终稿审查未通过",
            "rejected" => "终稿待修订",
            "not_required_historical" => "历史协议",
            _ => "终稿待审查",
        },
        if policy_missing {
            " · 缺 Policy，Decision 阻断"
        } else if decision_status == "blocked" {
            " · Decision 阻断"
        } else {
            ""
        }
    );
    json!({"research_status":research_status,"review_status":review_status,"revision":latest.as_ref().map(|v| v.0),
        "max_proposal_revisions":max_revisions,"decision_status":decision_status,"blocked_reasons":reasons,"display":display,
        "proposal_count":reviews.iter().filter(|(a,_)| a.kind == ArtifactKind::DecisionProposal && committed.contains(&a.artifact_id)).count(),
        "review_count":reviews.iter().filter(|(a,_)| a.kind == ArtifactKind::ProposalReview && committed.contains(&a.artifact_id)).count(),
        "proposal":latest.as_ref().map(|(_,r)| &r.proposal),"review_is_empirical_calibration":false})
}

impl Store {
    /// Only committed successful outputs of dependency tasks can authorize Decision.
    /// Staged, failed, cross-Run or earlier reviews cannot substitute for the final one.
    pub fn final_proposal_review(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<Option<(Artifact, ProposalReview)>> {
        let snapshot = self.workflow_snapshot(run_id)?;
        let current = snapshot
            .tasks
            .iter()
            .find(|t| &t.node.task_id == task_id)
            .ok_or_else(|| StoreError::Integrity("review target task missing".into()))?;
        let mut pending = current.node.dependencies.clone();
        let mut visited = BTreeSet::new();
        let mut reviews = Vec::new();
        while let Some(id) = pending.pop() {
            if !visited.insert(id.clone()) {
                continue;
            }
            let task = snapshot
                .tasks
                .iter()
                .find(|t| t.node.task_id == id)
                .ok_or_else(|| StoreError::Integrity("review dependency missing".into()))?;
            pending.extend(task.node.dependencies.iter().cloned());
            if task.status != TaskStatus::Succeeded {
                continue;
            }
            for artifact in self.succeeded_task_outputs_or_empty(run_id, &id)? {
                if artifact.kind != ArtifactKind::ProposalReview {
                    continue;
                }
                let revision = task
                    .node
                    .execution_spec()
                    .proposal_revision
                    .ok_or_else(|| StoreError::Integrity("review has no frozen revision".into()))?;
                let review: ProposalReview =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                let contract = self
                    .contract_installation(&review.contract_hash)?
                    .ok_or_else(|| {
                        StoreError::Integrity("review contract is not installed".into())
                    })?;
                review.validate_for_contract(contract.contract.version)?;
                if artifact.producer != "agent.research.proposal_reviewer"
                    || artifact.origin.as_ref().and_then(|o| o.run_id.as_ref()) != Some(run_id)
                    || artifact
                        .origin
                        .as_ref()
                        .and_then(|o| o.contract_hash.as_ref())
                        != Some(&review.contract_hash)
                    || task.node.contract_hash.as_ref() != Some(&review.contract_hash)
                    || !artifact.source_refs.contains(&review.proposal)
                    || !artifact.source_refs.contains(&review.manifest)
                {
                    return Err(StoreError::Integrity(
                        "proposal review provenance mismatch".into(),
                    ));
                }
                let proposal = self.artifact(&review.proposal.artifact_id)?;
                let manifest = self.artifact(&review.manifest.artifact_id)?;
                let context: ContextManifestPayload =
                    serde_json::from_slice(&self.read_blob(&manifest.blob)?)?;
                let proposal_task = proposal
                    .origin
                    .as_ref()
                    .and_then(|o| o.task_id.as_ref())
                    .and_then(|id| snapshot.tasks.iter().find(|t| &t.node.task_id == id))
                    .ok_or_else(|| StoreError::Integrity("review proposal task missing".into()))?;
                if proposal.kind != ArtifactKind::DecisionProposal
                    || proposal.blob.hash != review.proposal_hash
                    || proposal.origin.as_ref().and_then(|o| o.run_id.as_ref()) != Some(run_id)
                    || task.node.recipe_id.as_str()
                        != akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
                    || !task.node.dependencies.contains(&proposal_task.node.task_id)
                    || proposal_task.node.recipe_id.as_str()
                        != akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
                    || proposal_task.status != TaskStatus::Succeeded
                    || proposal
                        .origin
                        .as_ref()
                        .and_then(|o| o.contract_hash.as_ref())
                        != proposal_task.node.contract_hash.as_ref()
                    || !self
                        .succeeded_task_outputs_or_empty(run_id, &proposal_task.node.task_id)?
                        .iter()
                        .any(|a| a.artifact_id == proposal.artifact_id)
                    || manifest.kind != ArtifactKind::ContextManifest
                    || context.input_hash != akzio_domain::manifest_input_hash(&context.selections)?
                    || context.contract_hash != review.contract_hash
                    || manifest.origin != artifact.origin
                    || !context
                        .selections
                        .iter()
                        .any(|s| s.artifact == review.proposal)
                {
                    return Err(StoreError::Integrity(
                        "proposal review content or context mismatch".into(),
                    ));
                }
                reviews.push((revision, artifact, review));
            }
        }
        reviews.sort_by_key(|(revision, _, _)| *revision);
        if reviews.windows(2).any(|v| v[0].0 == v[1].0) {
            return Err(StoreError::Integrity(
                "ambiguous proposal review revision".into(),
            ));
        }
        Ok(reviews
            .pop()
            .map(|(_, artifact, review)| (artifact, review)))
    }
}
