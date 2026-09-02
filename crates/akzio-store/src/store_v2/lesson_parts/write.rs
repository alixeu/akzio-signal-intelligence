impl V2Store {
    /// Persist an operator or outcome-derived Lesson with its source artifact.
    /// The source and Lesson are inserted atomically and a dedicated immutable
    /// lesson event records the actor without inventing a synthetic Run.
    pub fn write_lesson(
        &self,
        lesson: &Lesson,
        source: &Artifact,
        now: DateTime<Utc>,
    ) -> StoreResult<LessonWriteResult> {
        self.ensure_lesson_tables()?;
        lesson.validate()?;
        source.validate()?;
        if source.kind == ArtifactKind::Lesson || source.lifecycle != ArtifactLifecycle::Canonical {
            return Err(StoreError::InvalidLearningCommit("lesson.source"));
        }
        let source_ref = ArtifactRef {
            artifact_id: source.artifact_id.clone(),
            kind: source.kind,
        };
        if !lesson.source_refs.contains(&source_ref) {
            return Err(StoreError::InvalidLearningCommit("lesson.source_refs"));
        }

        let blob = self.put_json(lesson)?;
        let producer = match lesson.origin {
            LessonOrigin::Operator => "learning.lesson.operator",
            LessonOrigin::OutcomeDerived => "learning.lesson.outcome",
        };
        let artifact = Artifact::new(
            ArtifactKind::Lesson,
            blob,
            producer,
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: match lesson.origin {
                    LessonOrigin::Operator => "akzio.operator".to_owned(),
                    LessonOrigin::OutcomeDerived => "akzio.learning".to_owned(),
                },
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: lesson.confidence_ppm,
                producer_contract_hash: None,
            },
            None,
            lesson
                .source_refs
                .iter()
                .cloned()
                .chain(lesson.supersedes.iter().cloned())
                .chain(lesson.conflicts_with.iter().cloned())
                .collect(),
            now,
        )?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if transaction
            .query_row(
                "SELECT 1 FROM rebuild_lesson_heads WHERE lesson_id = ?1",
                params![lesson.lesson_id.0.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some()
        {
            let current = self
                .read_lesson_from_transaction(&transaction, &lesson.lesson_id)?
                .ok_or_else(|| StoreError::Integrity("lesson head disappeared".to_owned()))?;
            if current.lesson != *lesson {
                return Err(StoreError::Integrity(format!(
                    "lesson {} conflicts with its immutable head",
                    lesson.lesson_id
                )));
            }
            transaction.commit()?;
            return Ok(LessonWriteResult {
                lesson: current,
                newly_created: false,
            });
        }

        insert_artifact(&transaction, source)?;
        self.validate_related_refs(&transaction, lesson)?;
        insert_artifact(&transaction, &artifact)?;
        transaction.execute(
            "INSERT INTO rebuild_lesson_heads (lesson_id, artifact_id, lifecycle, revision, updated_at) VALUES (?1, ?2, ?3, 1, ?4)",
            params![
                lesson.lesson_id.0.as_str(),
                artifact.artifact_id.0.as_str(),
                enum_name(lesson.lifecycle),
                lesson.updated_at.to_rfc3339(),
            ],
        )?;
        self.insert_lesson_event(
            &transaction,
            lesson,
            &artifact,
            "lesson.created",
            lesson.authored_by.as_deref().unwrap_or("learning.runtime"),
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(LessonWriteResult {
            lesson: StoredLesson {
                artifact,
                lesson: lesson.clone(),
                revision: 1,
            },
            newly_created: true,
        })
    }

    /// Append observational evidence linking Lesson revisions to sealed outcomes.
    ///
    /// Idempotent on `(lesson_id, decision_context, outcome)`: reprocessing the
    /// same triple is a no-op, and a *different* payload on an existing triple is
    /// an integrity error because the ledger is immutable. Keyed on the stable
    /// `lesson_id` rather than the Lesson artifact so that a later lifecycle
    /// transition, which writes a new artifact, does not orphan prior evidence.
    pub fn record_lesson_evidence(
        &self,
        records: &[LessonEvidence],
        now: DateTime<Utc>,
    ) -> StoreResult<u64> {
        self.ensure_lesson_tables()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut inserted = 0_u64;
        for record in records {
            record.validate()?;
            if record.recorded_at > now {
                return Err(StoreError::InvalidLearningCommit(
                    "lesson_evidence.recorded_at",
                ));
            }
            let (lesson_id, decision_context_id, outcome_id) = record.idempotency_key();
            if transaction
                .query_row(
                    "SELECT 1 FROM rebuild_lesson_heads WHERE lesson_id = ?1",
                    params![lesson_id.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .is_none()
            {
                return Err(StoreError::InvalidLearningCommit("lesson_evidence.lesson"));
            }
            // Provenance closure: every reference must resolve to a real artifact
            // of the declared kind before the record becomes durable.
            for reference in [
                &record.lesson_artifact,
                &record.decision_context,
                &record.outcome,
            ] {
                let artifact = read_artifact(&transaction, &reference.artifact_id)?;
                artifact.validate()?;
                if artifact.kind != reference.kind {
                    return Err(StoreError::InvalidLearningCommit(
                        "lesson_evidence.reference_kind",
                    ));
                }
            }

            let evidence_id = record.identity_hash()?;
            let evidence_json = serde_json::to_string(record)?;
            let existing: Option<String> = transaction
                .query_row(
                    "SELECT evidence_json FROM rebuild_lesson_evidence WHERE lesson_id = ?1 AND decision_context_artifact_id = ?2 AND outcome_artifact_id = ?3",
                    params![
                        lesson_id.as_str(),
                        decision_context_id.as_str(),
                        outcome_id.as_str(),
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(existing) = existing {
                let existing: LessonEvidence = serde_json::from_str(&existing)?;
                // Compared on the observation, not on `recorded_at`: a genuine
                // reprocess of the same sealed decision arrives with a later
                // timestamp and must stay a no-op that keeps the first record.
                if !existing.describes_same_observation(record) {
                    return Err(StoreError::Integrity(format!(
                        "lesson evidence for {lesson_id} is immutable"
                    )));
                }
                continue;
            }
            transaction.execute(
                "INSERT INTO rebuild_lesson_evidence (evidence_id, lesson_id, lesson_artifact_id, decision_context_artifact_id, outcome_artifact_id, attribution, evidence_json, recorded_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    evidence_id.as_str(),
                    lesson_id.as_str(),
                    record.lesson_artifact.artifact_id.0.as_str(),
                    decision_context_id.as_str(),
                    outcome_id.as_str(),
                    record.attribution.as_str(),
                    evidence_json,
                    record.recorded_at.to_rfc3339(),
                ],
            )?;
            inserted += 1;
        }
        transaction.commit()?;
        Ok(inserted)
    }

    pub fn lesson_evidence(&self, lesson_id: &LessonId) -> StoreResult<Vec<LessonEvidence>> {
        let connection = self.connection()?;
        if ensure_lesson_table_set(&connection)? != 3 {
            return Ok(Vec::new());
        }
        let mut statement = connection.prepare(
            "SELECT evidence_json FROM rebuild_lesson_evidence WHERE lesson_id = ?1 ORDER BY recorded_at, evidence_id",
        )?;
        let rows = statement
            .query_map(params![lesson_id.0.as_str()], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(connection);
        rows.into_iter()
            .map(|row| {
                let record: LessonEvidence = serde_json::from_str(&row)?;
                record.validate()?;
                Ok(record)
            })
            .collect()
    }

    /// Observational rollup only. See `LessonEvidence` for why these counts
    /// cannot be read as the Lesson's causal effect.
    pub fn lesson_evidence_summary(
        &self,
        lesson_id: &LessonId,
    ) -> StoreResult<LessonEvidenceSummary> {
        Ok(LessonEvidenceSummary::from_records(
            &self.lesson_evidence(lesson_id)?,
        ))
    }

    pub fn lesson(&self, lesson_id: &LessonId) -> StoreResult<Option<StoredLesson>> {
        let mut connection = self.connection()?;
        if ensure_lesson_table_set(&connection)? == 0 {
            return Ok(None);
        }
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let value = self.read_lesson_from_transaction(&transaction, lesson_id)?;
        transaction.commit()?;
        Ok(value)
    }

    pub fn lessons(
        &self,
        lifecycle: Option<LessonLifecycle>,
        limit: usize,
    ) -> StoreResult<Vec<StoredLesson>> {
        let connection = self.connection()?;
        if ensure_lesson_table_set(&connection)? == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit.clamp(1, 500)).expect("bounded lesson limit fits i64");
        let mut statement = connection.prepare(
            "SELECT lesson_id FROM rebuild_lesson_heads WHERE (?1 IS NULL OR lifecycle = ?1) ORDER BY updated_at DESC, lesson_id DESC LIMIT ?2",
        )?;
        let ids = statement
            .query_map(params![lifecycle.map(enum_name), limit], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(connection);
        let mut lessons = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(lesson) = self.lesson(&LessonId(id))? {
                lessons.push(lesson);
            }
        }
        Ok(lessons)
    }

    pub fn lesson_usage(&self, lesson_id: &LessonId) -> StoreResult<LessonUsage> {
        let lesson = self
            .lesson(lesson_id)?
            .ok_or_else(|| StoreError::Integrity(format!("missing lesson {lesson_id}")))?;
        let manifests = self.artifacts_referencing(
            &lesson.artifact.artifact_id,
            Some(ArtifactKind::ContextManifest),
        )?;
        let decisions = self.artifacts_referencing(
            &lesson.artifact.artifact_id,
            Some(ArtifactKind::DecisionContext),
        )?;
        let latest_used_at = manifests
            .iter()
            .chain(decisions.iter())
            .map(|artifact| artifact.created_at)
            .max();
        Ok(LessonUsage {
            context_manifests: manifests.len() as u64,
            decision_contexts: decisions.len() as u64,
            latest_used_at,
        })
    }

    /// Lifecycle changes create a successor artifact; prior revisions remain
    /// immutable and are linked through the Lesson supersedes field.
    pub fn transition_lesson(
        &self,
        lesson_id: &LessonId,
        lifecycle: LessonLifecycle,
        actor: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> StoreResult<StoredLesson> {
        self.ensure_lesson_tables()?;
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(StoreError::InvalidLearningCommit(
                "lesson.transition_actor_reason",
            ));
        }
        let current = self
            .lesson(lesson_id)?
            .ok_or_else(|| StoreError::Integrity(format!("missing lesson {lesson_id}")))?;
        if current.lesson.lifecycle == lifecycle {
            return Ok(current);
        }
        if matches!(current.lesson.lifecycle, LessonLifecycle::Retired) {
            return Err(StoreError::InvalidLearningCommit("lesson.retired"));
        }
        let mut next = current.lesson.clone();
        next.lifecycle = lifecycle;
        next.updated_at = now;
        next.supersedes.push(ArtifactRef {
            artifact_id: current.artifact.artifact_id.clone(),
            kind: ArtifactKind::Lesson,
        });
        next.supersedes.sort();
        next.supersedes.dedup();
        if matches!(
            lifecycle,
            LessonLifecycle::Active | LessonLifecycle::Contested
        ) {
            next.approved_by = Some(actor.to_owned());
        }
        next.validate()?;

        let blob = self.put_json(&next)?;
        let artifact = Artifact::new(
            ArtifactKind::Lesson,
            blob,
            "learning.lesson.lifecycle",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: match next.origin {
                    LessonOrigin::Operator => "akzio.operator".to_owned(),
                    LessonOrigin::OutcomeDerived => "akzio.learning".to_owned(),
                },
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: next.confidence_ppm,
                producer_contract_hash: None,
            },
            None,
            current
                .lesson
                .source_refs
                .iter()
                .cloned()
                .chain(next.supersedes.iter().cloned())
                .chain(next.conflicts_with.iter().cloned())
                .collect(),
            now,
        )?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = transaction
            .query_row(
                "SELECT artifact_id, revision FROM rebuild_lesson_heads WHERE lesson_id = ?1",
                params![lesson_id.0.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?;
        let Some((head_artifact, revision)) = head else {
            return Err(StoreError::Integrity(format!("missing lesson {lesson_id}")));
        };
        if head_artifact != current.artifact.artifact_id.0.as_str() {
            return Err(StoreError::Integrity(
                "lesson head changed concurrently".to_owned(),
            ));
        }
        self.validate_related_refs(&transaction, &next)?;
        if lifecycle == LessonLifecycle::Active {
            for conflict in &next.conflicts_with {
                let conflict_lesson = self.read_lesson_artifact(&transaction, conflict)?;
                let active = transaction
                    .query_row(
                        "SELECT 1 FROM rebuild_lesson_heads WHERE lesson_id = ?1 AND lifecycle = 'active'",
                        params![conflict_lesson.lesson.lesson_id.0.as_str()],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    .is_some();
                if active && conflict_lesson.lesson.lesson_id != next.lesson_id {
                    return Err(StoreError::InvalidLearningCommit("lesson.active_conflict"));
                }
            }
        }
        insert_artifact(&transaction, &artifact)?;
        transaction.execute(
            "UPDATE rebuild_lesson_heads SET artifact_id = ?1, lifecycle = ?2, revision = ?3, updated_at = ?4 WHERE lesson_id = ?5",
            params![
                artifact.artifact_id.0.as_str(),
                enum_name(next.lifecycle),
                revision.saturating_add(1),
                now.to_rfc3339(),
                lesson_id.0.as_str(),
            ],
        )?;
        self.insert_lesson_event(
            &transaction,
            &next,
            &artifact,
            "lesson.lifecycle_changed",
            actor,
            Some(reason),
            now,
        )?;
        transaction.commit()?;
        Ok(StoredLesson {
            artifact,
            lesson: next,
            revision: revision.saturating_add(1),
        })
    }

    fn read_lesson_from_transaction(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        lesson_id: &LessonId,
    ) -> StoreResult<Option<StoredLesson>> {
        let Some((artifact_id, revision)) = transaction
            .query_row(
                "SELECT artifact_id, revision FROM rebuild_lesson_heads WHERE lesson_id = ?1",
                params![lesson_id.0.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?
        else {
            return Ok(None);
        };
        let artifact = read_artifact(transaction, &ArtifactId(ContentHash::new(artifact_id)?))?;
        if artifact.kind != ArtifactKind::Lesson {
            return Err(StoreError::Integrity(
                "lesson head has wrong artifact kind".to_owned(),
            ));
        }
        let lesson: Lesson = serde_json::from_slice(&blob::read_blob_bytes(
            transaction,
            &artifact.blob.hash,
            artifact.blob.bytes,
        )?)?;
        lesson.validate()?;
        if &lesson.lesson_id != lesson_id {
            return Err(StoreError::Integrity(
                "lesson head payload identity mismatch".to_owned(),
            ));
        }
        Ok(Some(StoredLesson {
            artifact,
            lesson,
            revision,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_lesson_event(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        lesson: &Lesson,
        artifact: &Artifact,
        event_type: &str,
        actor: &str,
        reason: Option<&str>,
        created_at: DateTime<Utc>,
    ) -> StoreResult<()> {
        transaction.execute(
            "INSERT INTO rebuild_lesson_events (lesson_id, artifact_id, event_type, actor, reason, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                lesson.lesson_id.0.as_str(),
                artifact.artifact_id.0.as_str(),
                event_type,
                actor,
                reason,
                created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    fn read_lesson_artifact(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        reference: &ArtifactRef,
    ) -> StoreResult<StoredLesson> {
        if reference.kind != ArtifactKind::Lesson {
            return Err(StoreError::InvalidLearningCommit("lesson.related_refs"));
        }
        let artifact = read_artifact(transaction, &reference.artifact_id)?;
        if artifact.kind != ArtifactKind::Lesson {
            return Err(StoreError::InvalidLearningCommit("lesson.related_refs"));
        }
        let lesson: Lesson = serde_json::from_slice(&blob::read_blob_bytes(
            transaction,
            &artifact.blob.hash,
            artifact.blob.bytes,
        )?)?;
        lesson.validate()?;
        Ok(StoredLesson {
            artifact,
            lesson,
            revision: 0,
        })
    }

    fn validate_related_refs(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        lesson: &Lesson,
    ) -> StoreResult<()> {
        for reference in lesson.supersedes.iter().chain(lesson.conflicts_with.iter()) {
            self.read_lesson_artifact(transaction, reference)?;
        }
        Ok(())
    }
}
