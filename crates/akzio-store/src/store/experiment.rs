use super::*;

impl Store {
    pub fn experiment_trial_ledger(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<Vec<(ArtifactRef, ExperimentTrial)>> {
        subject.validate()?;
        let connection = self.connection()?;
        let mut ledger = read_kind_artifacts(&connection, ArtifactKind::ExperimentTrial)?
            .into_iter()
            .map(|artifact| {
                let trial: ExperimentTrial =
                    self.read_artifact_payload_with_connection(&connection, &artifact)?;
                trial.validate()?;
                Ok((artifact, trial))
            })
            .collect::<StoreResult<Vec<_>>>()?;
        ledger.retain(|(_, trial)| &trial.subject == subject);
        ledger.sort_by(|(left_artifact, left), (right_artifact, right)| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left_artifact.artifact_id.cmp(&right_artifact.artifact_id))
        });
        Ok(ledger
            .into_iter()
            .map(|(artifact, trial)| {
                (
                    ArtifactRef {
                        artifact_id: artifact.artifact_id,
                        kind: ArtifactKind::ExperimentTrial,
                    },
                    trial,
                )
            })
            .collect())
    }

    pub(super) fn verify_experiment_history(&self, connection: &Connection) -> StoreResult<()> {
        for artifact in read_kind_artifacts(connection, ArtifactKind::ExperimentTrial)? {
            if artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(
                    "experiment trial must be canonical".to_owned(),
                ));
            }
            let trial: ExperimentTrial = self.read_artifact_payload(&artifact)?;
            trial.validate()?;
            if artifact.source_refs != trial.source_refs {
                return Err(StoreError::Integrity(
                    "experiment trial source closure mismatch".to_owned(),
                ));
            }
        }
        for artifact in read_kind_artifacts(connection, ArtifactKind::SearchBiasCertificate)? {
            if artifact.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(
                    "search bias certificate must be canonical".to_owned(),
                ));
            }
            let certificate: SearchBiasCertificate = self.read_artifact_payload(&artifact)?;
            certificate.validate()?;
            if artifact.source_refs != certificate.trial_refs {
                return Err(StoreError::Integrity(
                    "search bias certificate source closure mismatch".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
