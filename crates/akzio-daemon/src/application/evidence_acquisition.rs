use crate::*;

/// Governed evidence collection, including the single supplemental round.
pub(crate) struct EvidenceAcquisition<'a> {
    daemon: &'a Daemon,
}

impl<'a> EvidenceAcquisition<'a> {
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    pub(crate) async fn execute(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.daemon.store.run_purpose(&task.run_id)? == RunPurpose::Shadow
            && self
                .daemon
                .store
                .canary_session_for_run(&task.run_id)?
                .is_some()
        {
            let Some(evidence) = self.daemon.store.canary_parent_evidence(&task.run_id)? else {
                return Ok(TaskCompletion::DeferredUntil(now + Duration::seconds(1)));
            };
            let status = evidence
                .iter()
                .find(|a| a.producer == "evidence.collection_status")
                .ok_or_else(|| {
                    DaemonError::InvalidInput("canary parent collection status missing".to_owned())
                })?;
            let value: Value = serde_json::from_slice(&self.daemon.store.read_blob(&status.blob)?)?;
            let artifact = Artifact::new(
                ArtifactKind::SemanticDetail,
                self.daemon.store.stage_json(&value)?,
                "canary.evidence_snapshot",
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: "akzio.ingest".to_owned(),
                    observed_at: status.provenance.observed_at,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: None,
                },
                Some(task.permit.artifact_origin()),
                evidence
                    .iter()
                    .map(|a| ArtifactRef {
                        artifact_id: a.artifact_id.clone(),
                        kind: a.kind,
                    })
                    .collect(),
                now,
            )?;
            return Ok(TaskCompletion::Succeeded(vec![artifact]));
        }
        let artifacts = self.daemon.acquire_evidence(task, now).await?;
        Ok(if artifacts.is_empty() {
            TaskCompletion::NoOutput
        } else {
            TaskCompletion::Succeeded(artifacts)
        })
    }

    pub(crate) fn prepare_supplemental(
        &self,
        task: &ClaimedAttempt,
        claim: &ResearchClaim,
        claim_reference: &ArtifactRef,
        candidates: &[ArtifactRef],
        now: DateTime<Utc>,
    ) -> Result<Vec<(ArtifactRef, Artifact, EvidenceNeed)>> {
        self.daemon
            .prepare_supplemental_needs(task, claim, claim_reference, candidates, now)
    }

    pub(crate) async fn supplemental(
        &self,
        task: &ClaimedAttempt,
        needs: &[(ArtifactRef, Artifact, EvidenceNeed)],
        now: DateTime<Utc>,
    ) -> Result<Vec<ArtifactRef>> {
        self.daemon
            .acquire_supplemental_evidence(task, needs, now)
            .await
    }

    pub(crate) fn note_abandoned(
        &self,
        task: &ClaimedAttempt,
        reason: &str,
        error: &dyn std::fmt::Display,
    ) -> Result<()> {
        self.daemon
            .note_supplemental_round_abandoned(task, reason, error)
    }
}
