use super::*;

impl V2Store {
    pub fn attempt_relation(
        &self,
        child_attempt_id: &AttemptId,
    ) -> StoreResult<Option<AttemptRelation>> {
        let artifact = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                r#"SELECT artifact_id
                   FROM rebuild_events
                   WHERE attempt_id = ?1 AND event_type = ?2
                   ORDER BY event_id ASC
                   LIMIT 2"#,
            )?;
            let artifact_ids = statement
                .query_map(
                    params![
                        child_attempt_id.0,
                        LifecycleEventType::AttemptRelationCreated.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            match artifact_ids.as_slice() {
                [] => return Ok(None),
                [artifact_id] => read_artifact(
                    &connection,
                    &ArtifactId(ContentHash::new(artifact_id.clone())?),
                )?,
                _ => {
                    return Err(StoreError::Integrity(format!(
                        "attempt {} has multiple relations",
                        child_attempt_id.0
                    )))
                }
            }
        };

        if artifact.kind != ArtifactKind::AttemptRelation
            || artifact.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::Integrity(format!(
                "attempt {} relation event references invalid artifact",
                child_attempt_id.0
            )));
        }
        artifact.validate()?;
        let relation: AttemptRelation = self.read_artifact_payload(&artifact)?;
        relation.validate()?;
        let origin = artifact.origin.as_ref();
        if &relation.child_attempt_id != child_attempt_id
            || origin.is_none_or(|origin| {
                origin.run_id.as_ref() != Some(&relation.run_id)
                    || origin.task_id.as_ref() != Some(&relation.task_id)
                    || origin.attempt_id.as_ref() != Some(child_attempt_id)
            })
        {
            return Err(StoreError::Integrity(format!(
                "attempt {} relation lineage mismatch",
                child_attempt_id.0
            )));
        }
        Ok(Some(relation))
    }

    pub fn attempt_events(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        attempt_id: &AttemptId,
    ) -> StoreResult<Vec<StoredEvent>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events
               WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3
               ORDER BY event_id ASC"#,
        )?;
        let events = statement
            .query_map(
                params![run_id.0, task_id.0, attempt_id.0],
                trajectory::stored_event_from_row,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        for event in &events {
            let event_type = event.lifecycle_kind()?;
            validate_event_shape(
                event_type,
                event.task_id.is_some(),
                event.attempt_id.is_some(),
                event.artifact_id.is_some(),
            )?;
        }
        Ok(events)
    }
}
