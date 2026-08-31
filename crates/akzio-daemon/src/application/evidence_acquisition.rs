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
