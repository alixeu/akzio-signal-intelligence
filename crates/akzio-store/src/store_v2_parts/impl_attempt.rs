impl V2Store {
    fn record_attempt_relation_in_transaction(
        &self,
        transaction: &Transaction<'_>,
        permit: &TaskWritePermit,
        parent_attempt_id: &AttemptId,
        relation: AttemptRelationKind,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let payload = AttemptRelation {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            run_id: permit.run_id.clone(),
            task_id: permit.task_id.clone(),
            parent_attempt_id: parent_attempt_id.clone(),
            child_attempt_id: permit.attempt_id.clone(),
            relation,
            created_at: now,
        };
        payload.validate()?;
        let artifact = Artifact::new(
            ArtifactKind::AttemptRelation,
            blob::put_blob_bytes(
                transaction,
                &serde_json::to_vec(&payload)?,
                "application/json".to_owned(),
            )?,
            "akzio-store.attempt_relation",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio-store".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(ArtifactOrigin {
                run_id: Some(permit.run_id.clone()),
                task_id: Some(permit.task_id.clone()),
                attempt_id: Some(permit.attempt_id.clone()),
                contract_hash: permit.contract_hash.clone(),
            }),
            Vec::new(),
            now,
        )?;
        insert_artifact(transaction, &artifact)?;
        append_event(
            transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::AttemptRelationCreated,
            Some(&artifact.artifact_id),
            now,
        )?;
        Ok(())
    }
}
