use crate::*;

pub(crate) struct ResearchRun<'a> {
    daemon: &'a Daemon,
}

impl<'a> ResearchRun<'a> {
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    /// Executes the explicit research workflow contract, including at most one
    /// governed supplemental analyst round for a blocking Paper evidence gap.
    pub(crate) async fn execute(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let candidates = self.daemon.agent_session().candidates(task)?;
        if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_CRITIC_RECIPE_ID {
            let claims = candidates
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::Claim)
                .map(|reference| {
                    self.daemon
                        .read_artifact_payload::<ResearchClaim>(reference)
                })
                .collect::<Result<Vec<_>>>()?;
            if !should_run_structured_critique(&claims) {
                return Ok(TaskCompletion::NoOutput);
            }
        }
        if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_ANALYST_RECIPE_ID {
            self.validate_canonical_evidence_manifest(task, &candidates)?;
        }
        let mut agent_budget = AgentRunBudget::new(&task.node.budget, &task.node.retry);
        let output = self
            .daemon
            .agent_session()
            .run(task, candidates.clone(), now, &mut agent_budget)
            .await?;
        if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_ANALYST_RECIPE_ID
            && self.daemon.store.run_purpose(&task.run_id)? == RunPurpose::Paper
        {
            let claim: ResearchClaim =
                serde_json::from_slice(&self.daemon.store.read_blob(&output.blob)?)?;
            let has_supplemental_request = claim.evidence_gaps.iter().any(|gap| {
                gap.impact == akzio_domain::EvidenceGapImpact::BlocksDirectionalForecast
                    && !gap.supplemental_needs.is_empty()
            });
            if has_supplemental_request {
                if !self
                    .daemon
                    .paper_execution()
                    .session_is_current(task)
                    .await?
                {
                    return Err(DaemonError::InvalidInput(
                        "Paper broker session changed before supplemental evidence collection"
                            .to_owned(),
                    ));
                }
                let request_source = output
                    .source_refs
                    .iter()
                    .find(|reference| reference.kind == ArtifactKind::AgentTurn)
                    .cloned()
                    .ok_or_else(|| {
                        DaemonError::InvalidInput(
                            "analyst refinement has no durable AgentTurn source".to_owned(),
                        )
                    })?;
                let prepared = match self.daemon.evidence_acquisition().prepare_supplemental(
                    task,
                    &claim,
                    &request_source,
                    &candidates,
                    now,
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        self.daemon.evidence_acquisition().note_abandoned(
                            task,
                            "supplemental evidence request rejected",
                            &error,
                        )?;
                        Vec::new()
                    }
                };
                if !prepared.is_empty() {
                    match self
                        .daemon
                        .evidence_acquisition()
                        .supplemental(task, &prepared, now)
                        .await
                    {
                        Ok(supplemental_refs) => {
                            if !self
                                .daemon
                                .paper_execution()
                                .session_is_current(task)
                                .await?
                            {
                                return Err(DaemonError::InvalidInput(
                                            "Paper broker session changed before supplemental analyst round"
                                                .to_owned(),
                                        ));
                            }
                            let mut refined_candidates = candidates;
                            refined_candidates.extend(supplemental_refs);
                            let refinement_now = Utc::now();
                            match self
                                .daemon
                                .agent_session()
                                .run(task, refined_candidates, refinement_now, &mut agent_budget)
                                .await
                            {
                                Ok(refined) => return Ok(TaskCompletion::Succeeded(vec![refined])),
                                Err(error) => self.daemon.evidence_acquisition().note_abandoned(
                                    task,
                                    "supplemental analyst round failed",
                                    &error,
                                )?,
                            }
                        }
                        Err(error) => self.daemon.evidence_acquisition().note_abandoned(
                            task,
                            "supplemental evidence collection failed",
                            &error,
                        )?,
                    }
                }
            }
        }
        if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_PLANNER_RECIPE_ID {
            let revision = self.daemon.workflow.recover(&task.run_id)?.revision;
            self.daemon.workflow.apply_planner_output(
                task,
                &revision.graph_artifact,
                &revision.graph,
                &output,
                now,
            )?;
            Ok(TaskCompletion::Committed)
        } else {
            Ok(TaskCompletion::Succeeded(vec![output]))
        }
    }

    fn validate_canonical_evidence_manifest(
        &self,
        task: &ClaimedAttempt,
        candidates: &[ArtifactRef],
    ) -> Result<()> {
        let purpose = self.daemon.store.run_purpose(&task.run_id)?;
        if !matches!(
            purpose,
            RunPurpose::Paper | RunPurpose::Replay | RunPurpose::Shadow
        ) {
            return Ok(());
        }

        let payloads = candidates
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::NormalizedEvidence)
            .filter_map(|reference| self.daemon.store.artifact(&reference.artifact_id).ok())
            .filter_map(|artifact| self.daemon.store.read_blob(&artifact.blob).ok())
            .filter_map(|bytes| serde_json::from_slice::<NormalizedEvidencePayload>(&bytes).ok())
            .collect::<Vec<_>>();
        let session_key = if purpose == RunPurpose::Paper {
            self.daemon
                .store
                .session_slot_for_run(&task.run_id)?
                .map(|slot| slot.session_key)
                .ok_or_else(|| {
                    DaemonError::InvalidInput(
                        "canonical Paper evidence manifest has no session slot".to_owned(),
                    )
                })?
        } else {
            let cutoffs = payloads
                .iter()
                .map(|payload| {
                    payload
                        .time_basis
                        .decision_clock
                        .decision_cutoff
                        .date_naive()
                })
                .collect::<BTreeSet<_>>();
            if cutoffs.len() != 1 {
                return Err(DaemonError::InvalidInput(format!(
                    "canonical historical evidence manifest must have one DecisionClock cutoff, found {}",
                    cutoffs.len()
                )));
            }
            cutoffs
                .into_iter()
                .next()
                .expect("one cutoff checked")
                .format("%Y-%m-%d")
                .to_string()
        };

        let actual = payloads
            .iter()
            .map(|payload| (payload.source.as_str(), payload.resource.as_str()))
            .collect::<BTreeSet<_>>();
        let blockers = akzio_domain::instrument_evidence_requirements_for_session(&session_key)
            .into_iter()
            .filter(|requirement| {
                !actual.contains(&(
                    requirement.need.source_family.as_str(),
                    requirement.need.resource.as_str(),
                ))
            })
            .collect::<Vec<_>>();
        if blockers.is_empty() {
            return Ok(());
        }
        let summary = blockers
            .iter()
            .take(8)
            .map(|blocker| {
                format!(
                    "{}:{}:{}",
                    blocker
                        .key
                        .asset
                        .map_or_else(|| "GLOBAL".to_owned(), |asset| asset.symbol().to_owned()),
                    blocker.key.category.as_str(),
                    blocker.need.resource
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        tracing::warn!(run_id=%task.run_id, missing=blockers.len(), resources=%summary,
            "directional evidence coverage incomplete; retain research with scoped neutral forecasts");
        Ok(())
    }
}
