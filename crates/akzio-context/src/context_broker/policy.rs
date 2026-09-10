impl ContextBroker {
    /// Record an explicitly source-linked Context repair. This is intentionally a
    /// normal artifact write, so repair is observable and may itself be cited.
    pub fn record_repair<T: Serialize>(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        source_refs: Vec<ArtifactRef>,
        value: &T,
        now: DateTime<Utc>,
    ) -> ContextResult<Artifact> {
        if !grant.matches_permit(permit) || grant.contract_hash != contract.contract_hash {
            return Err(ContextError::InvalidManifestClosure);
        }
        self.validate_persisted_grant(permit, contract, grant, now)?;
        for source in &source_refs {
            if !grant.permits(
                &source.artifact_id,
                source.kind == ArtifactKind::RawEvidence,
                now,
            ) {
                return Err(ContextError::GrantDenied {
                    manifest_id: grant.manifest_artifact_id.clone(),
                    artifact_id: source.artifact_id.clone(),
                });
            }
        }
        let artifact = Artifact::new(
            ArtifactKind::ContextRepair,
            self.store.stage_json(value)?,
            format!("context.repair.{}", contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.context_repair".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(contract.contract_hash.clone()),
            },
            Some(permit.artifact_origin()),
            source_refs,
            now,
        )?;
        self.store.write_task_artifact(
            permit,
            &artifact,
            LifecycleEventType::ContextRepaired,
            now,
        )?;
        Ok(artifact)
    }

    fn assert_context_permitted(
        &self,
        policy: &ContextPolicy,
        artifact: &Artifact,
    ) -> ContextResult<()> {
        if artifact.kind == ArtifactKind::RawEvidence {
            return Err(ContextError::RawEvidenceInManifest);
        }
        if artifact.kind == ArtifactKind::RegimeSnapshot {
            let snapshot: RegimeSnapshot = self.read_payload(artifact)?;
            snapshot.validate()?;
            if snapshot.classification_kind == RegimeClassificationKind::ExPost {
                return Err(ContextError::ForbiddenArtifact {
                    artifact_id: artifact.artifact_id.clone(),
                });
            }
        }
        if !policy.permitted_kinds.contains(&artifact.kind)
            || !governed_internal_source(artifact)
            || (!policy.permitted_source_families.is_empty()
                && !policy
                    .permitted_source_families
                    .contains(&artifact.provenance.source_family))
        {
            return Err(ContextError::ForbiddenArtifact {
                artifact_id: artifact.artifact_id.clone(),
            });
        }
        Ok(())
    }

    fn assert_context_run(
        &self,
        permit: &TaskWritePermit,
        artifact: &Artifact,
    ) -> ContextResult<()> {
        if artifact.kind == ArtifactKind::NormalizedEvidence
            && self.store.is_canary_parent_evidence(&permit.run_id,&artifact.artifact_id)?
        {
            return Ok(());
        }
        if matches!(
            artifact.kind,
            ArtifactKind::Lesson | ArtifactKind::Experience | ArtifactKind::CandidatePolicy
        ) {
            if self.overlay_is_eligible(artifact)? {
                return Ok(());
            }
        } else if artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            == Some(&permit.run_id)
        {
            return Ok(());
        }
        Err(ContextError::ForbiddenArtifact {
            artifact_id: artifact.artifact_id.clone(),
        })
    }

    fn overlay_is_eligible(&self, artifact: &Artifact) -> ContextResult<bool> {
        match artifact.kind {
            ArtifactKind::Lesson => {
                let lesson: Lesson = self.read_payload(artifact)?;
                lesson.validate()?;
                Ok(lesson.lifecycle == LessonLifecycle::Active)
            }
            ArtifactKind::Experience => {
                if !self.is_canonical_paper_artifact(artifact)? {
                    return Ok(false);
                }
                let experience: Experience = self.read_payload(artifact)?;
                experience.validate()?;
                if experience
                    .evaluation_context
                    .as_ref()
                    .is_none_or(|context| !context.learning_eligible)
                    || self
                        .store
                        .recorded_policy_influence_subject(&artifact.artifact_id)?
                        .as_ref()
                        != Some(&experience.subject)
                {
                    return Ok(false);
                }
                Ok(self
                    .store
                    .policy_head(&experience.subject)?
                    .is_some_and(|head| overlay_state_is_eligible(artifact.kind, head.state)))
            }
            ArtifactKind::CandidatePolicy => {
                if !self.is_canonical_paper_artifact(artifact)? {
                    return Ok(false);
                }
                let candidate: CandidatePolicy = self.read_payload(artifact)?;
                candidate.validate()?;
                if self
                    .store
                    .recorded_policy_influence_subject(&artifact.artifact_id)?
                    .as_ref()
                    != Some(&candidate.subject)
                {
                    return Ok(false);
                }
                let evaluation = self
                    .store
                    .artifact(&candidate.source_evaluation.artifact_id)?;
                if evaluation.kind != ArtifactKind::Evaluation
                    || !self.is_canonical_paper_artifact(&evaluation)?
                {
                    return Ok(false);
                }
                let evaluation_payload: akzio_domain::Evaluation =
                    self.read_payload(&evaluation)?;
                let experience_artifact = self
                    .store
                    .artifact(&evaluation_payload.experience.artifact_id)?;
                if experience_artifact.kind != ArtifactKind::Experience
                    || !self.is_canonical_paper_artifact(&experience_artifact)?
                {
                    return Ok(false);
                }
                let experience: Experience = self.read_payload(&experience_artifact)?;
                experience.validate()?;
                if experience.subject != candidate.subject
                    || experience
                        .evaluation_context
                        .as_ref()
                        .is_none_or(|context| !context.learning_eligible)
                    || self
                        .store
                        .recorded_policy_influence_subject(&experience_artifact.artifact_id)?
                        .as_ref()
                        != Some(&candidate.subject)
                {
                    return Ok(false);
                }
                Ok(self
                    .store
                    .policy_head(&candidate.subject)?
                    .is_some_and(|head| overlay_state_is_eligible(artifact.kind, head.state)))
            }
            _ => Ok(true),
        }
    }

    fn is_canonical_paper_artifact(&self, artifact: &Artifact) -> ContextResult<bool> {
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            return Ok(false);
        }
        let Some(run_id) = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
        else {
            return Ok(false);
        };
        Ok(self.store.run_purpose(run_id)?.is_canonical_learning())
    }

    fn read_payload<T: DeserializeOwned>(&self, artifact: &Artifact) -> ContextResult<T> {
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }

    fn raw_closure(
        &self,
        policy: &ContextPolicy,
        selections: &[ContextSelection],
    ) -> ContextResult<BTreeSet<ArtifactId>> {
        if !policy.allow_raw_reread {
            return Ok(BTreeSet::new());
        }
        let mut closure = BTreeSet::new();
        let mut queue = selections
            .iter()
            .map(|selection| selection.artifact.artifact_id.clone())
            .collect::<VecDeque<_>>();
        let mut seen = BTreeSet::new();
        while let Some(artifact_id) = queue.pop_front() {
            if !seen.insert(artifact_id.clone()) {
                continue;
            }
            let artifact = self.store.artifact(&artifact_id)?;
            for source in artifact.source_refs {
                let source_artifact = self.store.artifact(&source.artifact_id)?;
                if source_artifact.kind == ArtifactKind::RawEvidence {
                    if policy.permitted_source_families.is_empty()
                        || policy
                            .permitted_source_families
                            .contains(&source_artifact.provenance.source_family)
                    {
                        closure.insert(source_artifact.artifact_id);
                    }
                } else {
                    queue.push_back(source_artifact.artifact_id);
                }
            }
        }
        Ok(closure)
    }
}

/// Exact internal producer/kind pairs. External evidence retains the contract's
/// source allowlist and untrusted-content policy; no namespace wildcard grants.
fn governed_internal_source(artifact: &Artifact) -> bool {
    use ArtifactKind::*;
    match artifact.provenance.source_family.as_str() {
        "akzio.ingest" => {
            artifact.kind == SemanticDetail
                && matches!(
                    artifact.producer.as_str(),
                    "evidence.collection_status"
                        | "canary.evidence_snapshot"
                        | "evidence.option_projection"
                )
        }
        "akzio.agent" => {
            matches!(
                (artifact.kind, artifact.producer.as_str()),
                (Claim, "agent.research.analyst")
                    | (Critique, "agent.research.critic")
                    | (DecisionProposal, "agent.research.synthesizer")
                    | (RetrospectiveDraft, "agent.learning.outcome_worker")
            ) || (artifact.kind == DeliberationNote
                && artifact.provenance.producer_contract_hash.is_some()
                && matches!(
                    artifact.producer.as_str(),
                    "agent.deliberation.research.analyst"
                        | "agent.deliberation.research.critic"
                        | "agent.deliberation.research.synthesizer"
                        | "agent.deliberation.learning.outcome_worker"
                ))
        }
        "akzio.execution" => matches!(
            (artifact.kind, artifact.producer.as_str()),
            (Decision, "decision.bound")
                | (DecisionContext, "decision.context")
                | (ExecutionContext, "execution.context")
                | (ExecutionVerdict, "execution.verdict")
                | (ExecutionCommitment, "execution.paper_commitment")
                | (ExecutionPlan, "execution.plan")
                | (OrderReceipt, "execution.order_receipt")
                | (Reconciliation, "execution.reconciliation")
        ),
        "akzio-learning" => {
            (artifact.kind == SemanticDetail && artifact.producer == "learning.outcome_stage")
                || (artifact.kind == OutcomeSchedule
                    && artifact.producer == "learning.outcome_schedule")
                || (matches!(
                    artifact.kind,
                    Outcome | Retrospective | Experience | Evaluation | CandidatePolicy
                ) && artifact.producer == "akzio-learning.evaluation")
        }
        "akzio.learning" | "akzio.operator" => {
            artifact.kind == Lesson
                && matches!(
                    artifact.producer.as_str(),
                    "learning.lesson.outcome"
                        | "learning.lesson.operator"
                        | "learning.lesson.lifecycle"
                )
        }
        _ => matches!(
            artifact.kind,
            NormalizedEvidence | SemanticDetail | RegimeSnapshot
        ),
    }
}
