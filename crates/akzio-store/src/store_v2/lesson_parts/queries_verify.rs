impl V2Store {
    pub(super) fn ensure_lesson_tables(&self) -> StoreResult<()> {
        self.connection()?.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS rebuild_lesson_heads (
                lesson_id TEXT PRIMARY KEY,
                artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
                lifecycle TEXT NOT NULL,
                revision INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rebuild_lesson_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                lesson_id TEXT NOT NULL,
                artifact_id TEXT NOT NULL REFERENCES rebuild_artifacts(artifact_id),
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS rebuild_lesson_events_cursor
                ON rebuild_lesson_events(event_id);
            "#,
        )?;
        Ok(())
    }

    pub(super) fn verify_lesson_history(&self, connection: &Connection) -> StoreResult<()> {
        if ensure_lesson_table_set(connection)? == 0 {
            return Ok(());
        }

        let mut statement = connection.prepare(
            "SELECT lesson_id, artifact_id, lifecycle, revision, updated_at FROM rebuild_lesson_heads ORDER BY lesson_id",
        )?;
        let heads = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);

        let mut head_ids = BTreeSet::new();
        for (lesson_id, artifact_id, lifecycle, revision, updated_at) in heads {
            let lesson_id = LessonId(lesson_id);
            if !head_ids.insert(lesson_id.clone()) {
                return Err(StoreError::Integrity(format!(
                    "duplicate lesson head {lesson_id}"
                )));
            }
            if revision == 0 {
                return Err(StoreError::Integrity(format!(
                    "lesson {lesson_id} has zero revision"
                )));
            }
            let lifecycle: LessonLifecycle = parse_enum(&lifecycle)?;
            let updated_at = parse_time(&updated_at)?;
            let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(artifact_id)?))?;
            artifact.validate()?;
            if artifact.kind != ArtifactKind::Lesson
                || artifact.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::Integrity(format!(
                    "lesson {lesson_id} head has invalid artifact"
                )));
            }
            let payload =
                blob::read_blob_bytes(connection, &artifact.blob.hash, artifact.blob.bytes)?;
            let lesson: Lesson = serde_json::from_slice(&payload)?;
            lesson.validate()?;
            if lesson.lesson_id != lesson_id
                || lesson.lifecycle != lifecycle
                || lesson.updated_at != updated_at
            {
                return Err(StoreError::Integrity(format!(
                    "lesson {lesson_id} head disagrees with its payload"
                )));
            }
            self.verify_lesson_payload(connection, &artifact, &lesson)?;

            let event_count = connection.query_row(
                "SELECT COUNT(*) FROM rebuild_lesson_events WHERE lesson_id = ?1",
                params![lesson_id.0.as_str()],
                |row| row.get::<_, u64>(0),
            )?;
            if event_count != revision {
                return Err(StoreError::Integrity(format!(
                    "lesson {lesson_id} revision {revision} has {event_count} events"
                )));
            }
        }

        let mut events = connection.prepare(
            "SELECT event_id, lesson_id, artifact_id, event_type, actor, reason, created_at FROM rebuild_lesson_events ORDER BY event_id",
        )?;
        let event_rows = events
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (event_id, lesson_id, artifact_id, event_type, actor, reason, created_at) in event_rows
        {
            let lesson_id = LessonId(lesson_id);
            if !head_ids.contains(&lesson_id) {
                return Err(StoreError::Integrity(format!(
                    "lesson event {event_id} has no head"
                )));
            }
            if actor.trim().is_empty()
                || !matches!(
                    event_type.as_str(),
                    "lesson.created" | "lesson.lifecycle_changed"
                )
                || (event_type == "lesson.created" && reason.is_some())
                || (event_type == "lesson.lifecycle_changed"
                    && reason.as_deref().is_none_or(str::is_empty))
            {
                return Err(StoreError::Integrity(format!(
                    "lesson event {event_id} has invalid metadata"
                )));
            }
            parse_time(&created_at)?;
            let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(artifact_id)?))?;
            artifact.validate()?;
            if artifact.kind != ArtifactKind::Lesson
                || artifact.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::Integrity(format!(
                    "lesson event {event_id} has invalid artifact"
                )));
            }
            let payload: Lesson = serde_json::from_slice(&blob::read_blob_bytes(
                connection,
                &artifact.blob.hash,
                artifact.blob.bytes,
            )?)?;
            payload.validate()?;
            if payload.lesson_id != lesson_id {
                return Err(StoreError::Integrity(format!(
                    "lesson event {event_id} payload identity mismatch"
                )));
            }
            self.verify_lesson_payload(connection, &artifact, &payload)?;
        }
        Ok(())
    }

    fn verify_lesson_payload(
        &self,
        connection: &Connection,
        artifact: &Artifact,
        lesson: &Lesson,
    ) -> StoreResult<()> {
        let mut expected_refs = BTreeSet::new();
        for reference in lesson
            .source_refs
            .iter()
            .chain(lesson.supersedes.iter())
            .chain(lesson.conflicts_with.iter())
        {
            let referenced = read_artifact(connection, &reference.artifact_id)?;
            referenced.validate()?;
            if referenced.kind != reference.kind {
                return Err(StoreError::Integrity(format!(
                    "lesson {} source kind mismatch",
                    lesson.lesson_id
                )));
            }
            if reference.kind == ArtifactKind::Lesson {
                let payload: Lesson = serde_json::from_slice(&blob::read_blob_bytes(
                    connection,
                    &referenced.blob.hash,
                    referenced.blob.bytes,
                )?)?;
                payload.validate()?;
            } else if referenced.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(format!(
                    "lesson {} references non-canonical source",
                    lesson.lesson_id
                )));
            }
            expected_refs.insert(reference.clone());
        }
        let actual_refs = artifact
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if actual_refs != expected_refs {
            return Err(StoreError::Integrity(format!(
                "lesson {} artifact source closure disagrees with payload",
                lesson.lesson_id
            )));
        }
        Ok(())
    }
}
