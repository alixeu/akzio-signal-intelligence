use super::blob::put_blob_bytes;
use super::*;

impl Store {
    pub fn record_experiment_trial(
        &self,
        lease: &DaemonLease,
        trial: &ExperimentTrial,
        now: DateTime<Utc>,
    ) -> StoreResult<Artifact> {
        trial.validate()?;
        if trial.created_at > now {
            return Err(StoreError::InvalidLearningCommit(
                "experiment_trial.created_at",
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;

        let earliest_holdout_open =
            read_kind_artifacts(&transaction, ArtifactKind::ExperimentTrial)?
                .into_iter()
                .map(|artifact| self.read_artifact_payload::<ExperimentTrial>(&artifact))
                .collect::<StoreResult<Vec<_>>>()?
                .into_iter()
                .filter(|recorded| {
                    recorded.subject == trial.subject
                        && recorded.holdout_dataset_id == trial.holdout_dataset_id
                })
                .filter_map(|recorded| recorded.holdout_first_opened_at)
                .min();

        let mut recorded = trial.clone();
        if earliest_holdout_open.is_some_and(|opened_at| recorded.candidate_frozen_at >= opened_at)
            && recorded.status != ExperimentTrialStatus::Invalidated
        {
            recorded.status = ExperimentTrialStatus::Invalidated;
            recorded.metrics = None;
            recorded.failure_reason = Some(
                "holdout was already opened before this candidate was frozen; rotate holdout"
                    .to_owned(),
            );
            recorded.holdout_first_opened_at = earliest_holdout_open;
            recorded.holdout_access_count = recorded.holdout_access_count.max(2);
            recorded.trial_id = recorded.identity_hash()?;
            recorded.validate()?;
        }
        // Keep the CAS blob and immutable artifact in the same SQLite write
        // transaction. Calling `Store::put_json` here would open a second
        // connection while this immediate transaction owns the write lock,
        // causing the experiment writer to wait on itself until busy timeout.
        let blob = put_blob_bytes(
            &transaction,
            &serde_json::to_vec(&recorded)?,
            "application/json".to_owned(),
        )?;
        let artifact = Artifact::new(
            ArtifactKind::ExperimentTrial,
            blob,
            "learning.experiment_trial",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.experiment".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            recorded.source_refs.clone(),
            recorded.created_at,
        )?;
        insert_artifact(&transaction, &artifact)?;
        transaction.commit()?;
        Ok(artifact)
    }

    pub fn record_search_bias_certificate(
        &self,
        lease: &DaemonLease,
        certificate: &SearchBiasCertificate,
        now: DateTime<Utc>,
    ) -> StoreResult<Artifact> {
        certificate.validate()?;
        if certificate.created_at > now {
            return Err(StoreError::InvalidLearningCommit(
                "search_bias_certificate.created_at",
            ));
        }
        let artifact = Artifact::new(
            ArtifactKind::SearchBiasCertificate,
            self.stage_json(certificate)?,
            "learning.search_bias_certificate",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.experiment".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            certificate.trial_refs.clone(),
            certificate.created_at,
        )?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        insert_artifact(&transaction, &artifact)?;
        transaction.commit()?;
        Ok(artifact)
    }

    pub fn experiment_trial_ledger(
        &self,
        subject: &PolicySubject,
    ) -> StoreResult<Vec<(ArtifactRef, ExperimentTrial)>> {
        subject.validate()?;
        let connection = self.connection()?;
        let mut ledger = read_kind_artifacts(&connection, ArtifactKind::ExperimentTrial)?
            .into_iter()
            .map(|artifact| {
                let trial: ExperimentTrial = self.read_artifact_payload(&artifact)?;
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
