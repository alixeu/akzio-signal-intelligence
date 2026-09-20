use super::*;

impl Store {
    pub fn research_quality_records(&self, producer: &str) -> StoreResult<Vec<Artifact>> {
        if !matches!(
            producer,
            "research.quality.result"
                | "research.quality.capability"
                | "research.quality.call.started"
        ) {
            return Err(StoreError::Integrity(
                "unknown research quality record kind".into(),
            ));
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT artifact_id FROM rebuild_artifacts WHERE producer=?1 ORDER BY created_at, artifact_id")?;
        let ids = statement
            .query_map(params![producer], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| read_artifact(&connection, &ArtifactId(ContentHash::new(id)?)))
            .collect()
    }
    /// Reserve before provider I/O. Interrupted calls remain consumed; every
    /// phase and resumed invocation shares the same isolated Store allowance.
    pub fn reserve_research_quality_call(
        &self,
        permit: &TaskWritePermit,
        request: &serde_json::Value,
    ) -> StoreResult<u64> {
        let now = Utc::now();
        let artifact = Artifact::new(
            ArtifactKind::SemanticDetail,
            self.stage_json(request)?,
            "research.quality.call.started",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: permit.contract_hash.clone(),
            },
            Some(permit.artifact_origin()),
            vec![],
            now,
        )?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&tx, permit)?;
        let isolated: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_metadata WHERE key='debug_environment')",
            [],
            |row| row.get(0),
        )?;
        let count: u64 = tx.query_row(
            "SELECT count(*) FROM rebuild_artifacts WHERE producer='research.quality.call.started'",
            [],
            |row| row.get(0),
        )?;
        if !isolated || count >= 40 {
            return Err(StoreError::Integrity(
                "research quality requires an isolated Store and fewer than 40 reserved calls"
                    .into(),
            ));
        }
        insert_artifact(&tx, &artifact)?;
        append_event(
            &tx,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&artifact.artifact_id),
            now,
        )?;
        tx.commit()?;
        Ok(count + 1)
    }
}
