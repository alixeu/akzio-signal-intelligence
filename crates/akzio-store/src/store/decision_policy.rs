use super::*;

/// Identity columns indexed beside the immutable CAS envelope. The Store does
/// not interpret calibration math; akzio-execution validates the typed payload
/// before this descriptor reaches the persistence seam.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionPolicyDescriptor {
    pub policy_hash: ContentHash,
    pub envelope_hash: ContentHash,
    pub provider_id: String,
    pub model_id: String,
    pub model_version_hash: ContentHash,
    pub model_route: String,
    pub contract_hash: ContentHash,
}

impl DecisionPolicyDescriptor {
    fn validate(&self, artifact: &Artifact) -> StoreResult<()> {
        if artifact.kind != ArtifactKind::DecisionPolicy
            || artifact.lifecycle != ArtifactLifecycle::Canonical
            || artifact.origin.is_some()
            || !artifact.source_refs.is_empty()
            || artifact.blob.hash != self.envelope_hash
            || self.provider_id.trim().is_empty()
            || self.model_id.trim().is_empty()
            || self.model_route.trim().is_empty()
        {
            return Err(StoreError::DecisionPolicyConflict(self.policy_hash.clone()));
        }
        artifact.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDecisionPolicy {
    pub descriptor: DecisionPolicyDescriptor,
    pub artifact: Artifact,
    pub installed_at: DateTime<Utc>,
    pub activated_at: Option<DateTime<Utc>>,
}

impl Store {
    /// Persist calibration inputs or a built policy without selecting an active
    /// policy. The CLI/execution layer validates typed calibration math; this
    /// seam owns canonical provenance and prohibits isolated Debug promotion.
    pub fn write_calibration_artifact(&self, artifact: &Artifact) -> StoreResult<()> {
        artifact.validate()?;
        if self.debug_environment()?.is_some()
            || artifact.origin.is_some()
            || artifact.lifecycle != ArtifactLifecycle::Canonical
            || !matches!(
                artifact.kind,
                ArtifactKind::CalibrationRiskLimits
                    | ArtifactKind::CalibrationDataset
                    | ArtifactKind::DecisionPolicy
            )
            || (artifact.kind != ArtifactKind::CalibrationDataset
                && !artifact.source_refs.is_empty())
        {
            return Err(StoreError::PermitOriginMismatch);
        }
        if artifact.kind == ArtifactKind::CalibrationDataset
            && [
                ArtifactKind::CalibrationRiskLimits,
                ArtifactKind::Decision,
                ArtifactKind::Outcome,
            ]
            .into_iter()
            .any(|kind| {
                !artifact
                    .source_refs
                    .iter()
                    .any(|source| source.kind == kind)
            })
        {
            return Err(StoreError::PermitOriginMismatch);
        }
        self.read_blob(&artifact.blob)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for reference in &artifact.source_refs {
            let source = read_artifact(&transaction, &reference.artifact_id)?;
            if source.kind != reference.kind
                || source.lifecycle != ArtifactLifecycle::Canonical
                || !matches!(
                    source.kind,
                    ArtifactKind::CalibrationRiskLimits
                        | ArtifactKind::Decision
                        | ArtifactKind::Outcome
                )
            {
                return Err(StoreError::PermitOriginMismatch);
            }
        }
        insert_artifact(&transaction, artifact)?;
        transaction.commit()?;
        Ok(())
    }

    /// Install one validated immutable policy and atomically select it as the
    /// active DecisionGate policy. Re-activating the current hash is idempotent.
    pub fn activate_decision_policy(
        &self,
        artifact: &Artifact,
        descriptor: &DecisionPolicyDescriptor,
        now: DateTime<Utc>,
    ) -> StoreResult<StoredDecisionPolicy> {
        descriptor.validate(artifact)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = read_decision_policy(&transaction, &descriptor.policy_hash)?;
        match existing {
            Some(ref value) if value.descriptor == *descriptor && value.artifact == *artifact => {}
            Some(_) => {
                return Err(StoreError::DecisionPolicyConflict(
                    descriptor.policy_hash.clone(),
                ));
            }
            None => {
                insert_artifact(&transaction, artifact)?;
                transaction.execute(
                    r#"INSERT INTO rebuild_decision_policy_installations
                       (policy_hash, artifact_id, envelope_hash, provider_id, model_id,
                        model_version_hash, model_route, contract_hash, installed_at)
                       VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"#,
                    params![
                        descriptor.policy_hash.as_str(),
                        artifact.artifact_id.0.as_str(),
                        descriptor.envelope_hash.as_str(),
                        descriptor.provider_id,
                        descriptor.model_id,
                        descriptor.model_version_hash.as_str(),
                        descriptor.model_route,
                        descriptor.contract_hash.as_str(),
                        now.to_rfc3339(),
                    ],
                )?;
            }
        }
        let current = decision_policy_head_hash(&transaction)?;
        if current.as_ref() != Some(&descriptor.policy_hash) {
            let prior_activation = transaction
                .query_row(
                    r#"SELECT a.activated_at
                         FROM rebuild_decision_policy_head h
                         JOIN rebuild_decision_policy_activations a
                           ON a.activation_id=h.activation_id
                        WHERE h.singleton_id=1"#,
                    [],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .as_deref()
                .map(parse_time)
                .transpose()?;
            if prior_activation.is_some_and(|prior| now < prior) {
                return Err(StoreError::DecisionPolicyConflict(
                    descriptor.policy_hash.clone(),
                ));
            }
            transaction.execute(
                "INSERT INTO rebuild_decision_policy_activations (previous_policy_hash, policy_hash, activated_at) VALUES (?1,?2,?3)",
                params![current.as_ref().map(ContentHash::as_str), descriptor.policy_hash.as_str(), now.to_rfc3339()],
            )?;
            let activation_id = transaction.last_insert_rowid();
            transaction.execute(
                r#"INSERT INTO rebuild_decision_policy_head (singleton_id, policy_hash, activation_id)
                   VALUES (1,?1,?2) ON CONFLICT(singleton_id) DO UPDATE SET
                   policy_hash=excluded.policy_hash, activation_id=excluded.activation_id"#,
                params![descriptor.policy_hash.as_str(), activation_id],
            )?;
        }
        transaction.commit()?;
        drop(connection);
        self.active_decision_policy()?
            .ok_or_else(|| StoreError::DecisionPolicyConflict(descriptor.policy_hash.clone()))
    }

    pub fn active_decision_policy(&self) -> StoreResult<Option<StoredDecisionPolicy>> {
        let connection = self.connection()?;
        let Some(hash) = decision_policy_head_hash(&connection)? else {
            return Ok(None);
        };
        read_decision_policy(&connection, &hash)
    }

    /// Copy only the selected immutable policy into an isolated Store. No Run,
    /// Outcome, credential or mutable policy state crosses this seam.
    pub fn bootstrap_active_decision_policy_from(
        &self,
        source: &Store,
        now: DateTime<Utc>,
    ) -> StoreResult<Option<StoredDecisionPolicy>> {
        let Some(active) = source.active_decision_policy()? else {
            return Ok(None);
        };
        let bytes = source.read_blob(&active.artifact.blob)?;
        let blob = self.stage_bytes(&bytes, active.artifact.blob.media_type.clone())?;
        if blob.hash != active.artifact.blob.hash {
            return Err(StoreError::DecisionPolicyConflict(
                active.descriptor.policy_hash,
            ));
        }
        let mut artifact = active.artifact;
        artifact.blob = blob;
        self.activate_decision_policy(&artifact, &active.descriptor, now)
            .map(Some)
    }
}

fn decision_policy_head_hash(connection: &Connection) -> StoreResult<Option<ContentHash>> {
    connection
        .query_row(
            "SELECT policy_hash FROM rebuild_decision_policy_head WHERE singleton_id=1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(ContentHash::new)
        .transpose()
        .map_err(Into::into)
}

fn read_decision_policy(
    connection: &Connection,
    policy_hash: &ContentHash,
) -> StoreResult<Option<StoredDecisionPolicy>> {
    let row = connection
        .query_row(
            r#"SELECT i.artifact_id, i.envelope_hash, i.provider_id, i.model_id,
                      i.model_version_hash, i.model_route, i.contract_hash, i.installed_at,
                      a.activated_at
               FROM rebuild_decision_policy_installations i
               LEFT JOIN rebuild_decision_policy_head h ON h.policy_hash=i.policy_hash
               LEFT JOIN rebuild_decision_policy_activations a ON a.activation_id=h.activation_id
               WHERE i.policy_hash=?1"#,
            params![policy_hash.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            },
        )
        .optional()?;
    let Some((
        artifact_id,
        envelope_hash,
        provider_id,
        model_id,
        model_version_hash,
        model_route,
        contract_hash,
        installed_at,
        activated_at,
    )) = row
    else {
        return Ok(None);
    };
    let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(artifact_id)?))?;
    let descriptor = DecisionPolicyDescriptor {
        policy_hash: policy_hash.clone(),
        envelope_hash: ContentHash::new(envelope_hash)?,
        provider_id,
        model_id,
        model_version_hash: ContentHash::new(model_version_hash)?,
        model_route,
        contract_hash: ContentHash::new(contract_hash)?,
    };
    descriptor.validate(&artifact)?;
    Ok(Some(StoredDecisionPolicy {
        descriptor,
        artifact,
        installed_at: parse_time(&installed_at)?,
        activated_at: activated_at.as_deref().map(parse_time).transpose()?,
    }))
}

pub(super) fn verify_decision_policy_history(connection: &Connection) -> StoreResult<()> {
    let installed = connection
        .prepare(
            "SELECT policy_hash FROM rebuild_decision_policy_installations ORDER BY installed_at, policy_hash",
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    for policy_hash in &installed {
        let policy_hash = ContentHash::new(policy_hash.clone())?;
        read_decision_policy(connection, &policy_hash)?.ok_or_else(|| {
            StoreError::Integrity(format!(
                "DecisionPolicy installation {policy_hash} cannot be reconstructed"
            ))
        })?;
        let activated: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_decision_policy_activations WHERE policy_hash=?1)",
            params![policy_hash.as_str()],
            |row| row.get(0),
        )?;
        if !activated {
            return Err(StoreError::Integrity(format!(
                "DecisionPolicy installation {policy_hash} was never activated"
            )));
        }
    }

    let activations = connection
        .prepare(
            r#"SELECT activation_id, previous_policy_hash, policy_hash, activated_at
                 FROM rebuild_decision_policy_activations ORDER BY activation_id"#,
        )?
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut expected_previous: Option<ContentHash> = None;
    let mut last_activation_id = None;
    let mut last_activated_at = None;
    for (activation_id, previous, policy, activated_at) in activations {
        let previous = previous.map(ContentHash::new).transpose()?;
        let policy = ContentHash::new(policy)?;
        let activated_at = parse_time(&activated_at)?;
        if previous != expected_previous || previous.as_ref() == Some(&policy) {
            return Err(StoreError::Integrity(format!(
                "DecisionPolicy activation {activation_id} breaks the active-head chain"
            )));
        }
        if last_activated_at.is_some_and(|prior| activated_at < prior) {
            return Err(StoreError::Integrity(format!(
                "DecisionPolicy activation {activation_id} has a regressing timestamp"
            )));
        }
        expected_previous = Some(policy);
        last_activation_id = Some(activation_id);
        last_activated_at = Some(activated_at);
    }

    let head = connection
        .query_row(
            "SELECT policy_hash, activation_id FROM rebuild_decision_policy_head WHERE singleton_id=1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    match (expected_previous, last_activation_id, head) {
        (None, None, None) => Ok(()),
        (Some(expected_hash), Some(expected_id), Some((actual_hash, actual_id)))
            if expected_hash == ContentHash::new(actual_hash.clone())?
                && expected_id == actual_id =>
        {
            Ok(())
        }
        _ => Err(StoreError::Integrity(
            "DecisionPolicy active head does not match activation history".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store(label: &str) -> Store {
        Store::open(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/decision-policy-store-tests")
                .join(format!("{label}-{}", RunId::new().0)),
        )
        .unwrap()
    }

    fn policy(
        store: &Store,
        name: &str,
        now: DateTime<Utc>,
    ) -> (Artifact, DecisionPolicyDescriptor) {
        let bytes = format!(r#"{{"policy":"{name}"}}"#);
        let blob = store
            .stage_bytes(bytes.as_bytes(), "application/json")
            .unwrap();
        let policy_hash = ContentHash::of_bytes(name.as_bytes());
        let descriptor = DecisionPolicyDescriptor {
            policy_hash,
            envelope_hash: blob.hash.clone(),
            provider_id: "openai-responses".to_owned(),
            model_id: "test-model".to_owned(),
            model_version_hash: ContentHash::of_bytes(b"model-version"),
            model_route: "research.synthesizer".to_owned(),
            contract_hash: ContentHash::of_bytes(b"contract"),
        };
        let artifact = Artifact::new(
            ArtifactKind::DecisionPolicy,
            blob,
            "decision-policy-test",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "test".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        (artifact, descriptor)
    }

    #[test]
    fn built_policy_is_durable_but_inactive_until_explicit_activation() {
        let store = test_store("candidate");
        let now = Utc::now();
        let (artifact, descriptor) = policy(&store, "candidate", now);
        store.write_calibration_artifact(&artifact).unwrap();
        assert_eq!(store.artifact(&artifact.artifact_id).unwrap(), artifact);
        assert!(store.active_decision_policy().unwrap().is_none());
        store.verify_integrity().unwrap();
        let reopened = Store::open_existing(store.root()).unwrap();
        assert_eq!(reopened.artifact(&artifact.artifact_id).unwrap(), artifact);
        assert!(reopened.active_decision_policy().unwrap().is_none());
        let active = store
            .activate_decision_policy(&artifact, &descriptor, now)
            .unwrap();
        assert_eq!(active.artifact.artifact_id, artifact.artifact_id);
        store.verify_integrity().unwrap();
    }

    #[test]
    fn activation_history_and_isolated_bootstrap_preserve_exact_policy() {
        let source = test_store("source");
        let now = Utc::now();
        let (first_artifact, first_descriptor) = policy(&source, "first", now);
        let first = source
            .activate_decision_policy(&first_artifact, &first_descriptor, now)
            .unwrap();
        let repeated = source
            .activate_decision_policy(&first_artifact, &first_descriptor, now)
            .unwrap();
        assert_eq!(first.activated_at, repeated.activated_at);

        let (second_artifact, second_descriptor) =
            policy(&source, "second", now + Duration::seconds(1));
        let active = source
            .activate_decision_policy(
                &second_artifact,
                &second_descriptor,
                now + Duration::seconds(1),
            )
            .unwrap();
        assert_eq!(active.descriptor, second_descriptor);
        let (regressing_artifact, regressing_descriptor) =
            policy(&source, "regressing", now - Duration::seconds(1));
        assert!(source
            .activate_decision_policy(
                &regressing_artifact,
                &regressing_descriptor,
                now - Duration::seconds(1),
            )
            .is_err());
        assert_eq!(
            source.active_decision_policy().unwrap().unwrap().descriptor,
            second_descriptor
        );
        source.verify_integrity().unwrap();

        let target = test_store("target");
        let copied = target
            .bootstrap_active_decision_policy_from(&source, now + Duration::seconds(2))
            .unwrap()
            .unwrap();
        assert_eq!(copied.descriptor, active.descriptor);
        assert_eq!(copied.artifact.artifact_id, active.artifact.artifact_id);
        assert_eq!(
            target.read_blob(&copied.artifact.blob).unwrap(),
            source.read_blob(&active.artifact.blob).unwrap()
        );
        target.verify_integrity().unwrap();
    }
}
