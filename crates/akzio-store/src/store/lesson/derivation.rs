use super::*;
use akzio_domain::{DebugLearningScope, LessonScope, Retrospective, RetrospectiveStatus};

const EVALUATION_BATCH: usize = 16;

impl Store {
    /// Read pending and blocked derivations from the canonical Evaluation ledger.
    pub(in crate::store) fn outcome_lesson_derivations(
        &self,
        run_id: &RunId,
    ) -> StoreResult<Vec<OutcomeLessonDerivation>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT evaluation_artifact_id FROM rebuild_policy_evaluations WHERE run_id=?1 ORDER BY event_cursor",
        )?;
        let ids = statement
            .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| {
                let evaluation = ArtifactId(ContentHash::new(id)?);
                let outcome = self.outcome_lesson_plan(&connection, run_id, &evaluation);
                let mut status = OutcomeLessonDerivation {
                    evaluation,
                    expected: None,
                    pending: None,
                    blocked_reason: None,
                    draft_failures: Vec::new(),
                };
                match outcome {
                    Ok((retrospective, lessons)) => {
                        status.expected = Some(lessons.len());
                        let (pending, failures) =
                            pending_outcome_lesson_drafts(&connection, &retrospective, lessons);
                        status.pending = Some(pending.len() + failures.len());
                        status.draft_failures = failures;
                    }
                    Err(error) => status.blocked_reason = Some(error.to_string()),
                }
                Ok(status)
            })
            .collect()
    }

    /// Recreate missing Draft genesis records from one committed Evaluation.
    /// Returns the number created and individual Draft failures. Evaluation lineage errors
    /// prevent any writes, while a conflicting Draft permits recovery of its siblings.
    pub fn repair_outcome_lesson_drafts(
        &self,
        run_id: &RunId,
        evaluation: &ArtifactId,
        max_new_drafts: usize,
    ) -> StoreResult<(usize, Vec<OutcomeLessonDraftFailure>)> {
        let (retrospective, pending, mut failures) = {
            let connection = self.connection()?;
            let (retrospective, lessons) =
                self.outcome_lesson_plan(&connection, run_id, evaluation)?;
            let (pending, failures) =
                pending_outcome_lesson_drafts(&connection, &retrospective, lessons);
            (retrospective, pending, failures)
        };
        let mut created = 0;
        for lesson in pending {
            if created == max_new_drafts {
                break;
            }
            match self.write_lesson_preserving_genesis(&lesson, &retrospective) {
                Ok(result) => created += usize::from(result.newly_created),
                Err(error) => failures.push(OutcomeLessonDraftFailure {
                    lesson_id: lesson.lesson_id,
                    reason: error.to_string(),
                }),
            }
        }
        Ok((created, failures))
    }

    /// Scan a bounded page of immutable Evaluation records. A blocked evaluation does not
    /// stop the page; the next sweep retries it and the Run lifecycle shows its status.
    pub fn repair_outcome_lesson_batch(&self, after: i64) -> StoreResult<(i64, Vec<String>)> {
        let rows = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                "SELECT event_cursor,run_id,evaluation_artifact_id FROM rebuild_policy_evaluations WHERE event_cursor>?1 ORDER BY event_cursor LIMIT ?2",
            )?;
            let rows = statement
                .query_map(params![after, EVALUATION_BATCH], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        let mut blocked = Vec::new();
        for (_, run, evaluation) in &rows {
            let repair = ContentHash::new(evaluation)
                .map_err(StoreError::from)
                .and_then(|id| {
                    self.repair_outcome_lesson_drafts(
                        &RunId(run.clone()),
                        &ArtifactId(id),
                        usize::MAX,
                    )
                });
            match repair {
                Ok((_, failures)) => blocked.extend(failures.into_iter().map(|failure| {
                    format!(
                        "Run {run}, Evaluation {evaluation}, Lesson {}: {}",
                        failure.lesson_id, failure.reason
                    )
                })),
                Err(error) => blocked.push(format!("Run {run}, Evaluation {evaluation}: {error}")),
            }
        }
        let next_cursor = if rows.len() == EVALUATION_BATCH {
            rows.last().map_or(0, |(cursor, _, _)| *cursor)
        } else {
            0
        };
        Ok((next_cursor, blocked))
    }

    fn outcome_lesson_plan(
        &self,
        connection: &Connection,
        run_id: &RunId,
        evaluation: &ArtifactId,
    ) -> StoreResult<(Artifact, Vec<Lesson>)> {
        if debug::environment_identity(connection)?.is_some()
            || debug::read_session(connection, run_id)?.is_some_and(|session| {
                session.identity.learning_scope == DebugLearningScope::Isolated
            })
        {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_lesson.isolated_learning",
            ));
        }
        let ledger = read_policy_evaluation(connection, evaluation)?.ok_or(
            StoreError::InvalidLearningCommit("outcome_lesson.evaluation_ledger"),
        )?;
        if ledger.run_id != *run_id {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_lesson.evaluation_run",
            ));
        }
        let evaluation_ref = ArtifactRef {
            artifact_id: evaluation.clone(),
            kind: ArtifactKind::Evaluation,
        };
        let experience_ref = ArtifactRef {
            artifact_id: ledger.experience_artifact_id,
            kind: ArtifactKind::Experience,
        };
        let outcome_ref = ArtifactRef {
            artifact_id: ledger.outcome_artifact_id,
            kind: ArtifactKind::Outcome,
        };
        let evaluation_artifact = outcome_lesson_artifact(connection, run_id, &evaluation_ref)?;
        let experience_artifact = outcome_lesson_artifact(connection, run_id, &experience_ref)?;
        let outcome_artifact = outcome_lesson_artifact(connection, run_id, &outcome_ref)?;
        let evaluation: Evaluation =
            self.read_artifact_payload_with_connection(connection, &evaluation_artifact)?;
        let experience: Experience =
            self.read_artifact_payload_with_connection(connection, &experience_artifact)?;
        let outcome: Outcome =
            self.read_artifact_payload_with_connection(connection, &outcome_artifact)?;
        evaluation.validate()?;
        experience.validate()?;
        outcome.validate_sealed()?;
        if evaluation.experience != experience_ref
            || evaluation.outcome != outcome_ref
            || experience.outcome != outcome_ref
            || experience.subject != ledger.subject
            || evaluation.created_at != ledger.completed_at
            || evaluation_artifact.created_at != ledger.completed_at
            || evaluation_artifact.origin != experience_artifact.origin
            || !experience_artifact.source_refs.contains(&outcome_ref)
            || !evaluation_artifact.source_refs.contains(&experience_ref)
            || !evaluation_artifact.source_refs.contains(&outcome_ref)
        {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_lesson.evaluation_sources",
            ));
        }
        let origin =
            evaluation_artifact
                .origin
                .as_ref()
                .ok_or(StoreError::InvalidLearningCommit(
                    "outcome_lesson.evaluation_origin",
                ))?;
        let event_matches: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE event_id=?1 AND run_id=?2 AND task_id=?3 AND attempt_id=?4 AND event_type=?5 AND artifact_id=?6)",
            params![ledger.event_cursor, run_id.0, origin.task_id.as_ref().map(|id| id.0.as_str()), origin.attempt_id.as_ref().map(|id| id.0.as_str()), LifecycleEventType::PolicyEvaluated.as_str(), evaluation_ref.artifact_id.0.as_str()],
            |row| row.get(0),
        )?;
        if !event_matches {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_lesson.evaluation_event",
            ));
        }
        let retrospective_ref = unique_retrospective_source(&evaluation_artifact)?;
        if unique_retrospective_source(&experience_artifact)? != retrospective_ref {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_lesson.retrospective_sources",
            ));
        }
        let retrospective_artifact =
            outcome_lesson_artifact(connection, run_id, retrospective_ref)?;
        let retrospective: Retrospective =
            self.read_artifact_payload_with_connection(connection, &retrospective_artifact)?;
        retrospective.validate()?;
        if retrospective.horizon != OutcomeHorizon::T5
            || retrospective.status != RetrospectiveStatus::Complete
            || retrospective.outcome != outcome_ref
            || retrospective.outcome_id != outcome.outcome_id
            || !retrospective_artifact.source_refs.contains(&outcome_ref)
        {
            return Err(StoreError::InvalidLearningCommit(
                "outcome_lesson.retrospective",
            ));
        }
        // Multiple Canary subjects can reference the same frozen retrospective; its first
        // recorded Evaluation owns the original derivation timestamp.
        let first_evaluation = connection.query_row(
            "SELECT p.evaluation_artifact_id FROM rebuild_policy_evaluations p JOIN rebuild_artifact_refs r ON r.artifact_id=p.evaluation_artifact_id JOIN rebuild_artifact_refs e ON e.artifact_id=p.experience_artifact_id AND e.source_artifact_id=r.source_artifact_id WHERE p.run_id=?1 AND p.outcome_artifact_id=?2 AND r.source_artifact_id=?3 AND r.source_kind=?4 AND e.source_kind=?4 ORDER BY p.event_cursor LIMIT 1",
            params![run_id.0, outcome_ref.artifact_id.0.as_str(), retrospective_ref.artifact_id.0.as_str(), enum_name(ArtifactKind::Retrospective)],
            |row| row.get::<_, String>(0),
        )?;
        let first_evaluation = ArtifactId(ContentHash::new(first_evaluation)?);
        if first_evaluation != evaluation_ref.artifact_id {
            return self.outcome_lesson_plan(connection, run_id, &first_evaluation);
        }
        let created_at = ledger.completed_at;
        let mut lessons = Vec::new();
        for (index, candidate) in retrospective.lesson_proposals.iter().enumerate() {
            let statement = candidate.statement.trim();
            let lesson = Lesson {
                schema_version: DOMAIN_SCHEMA_VERSION,
                lesson_id: LessonId(
                    akzio_domain::content_hash_json(&serde_json::json!({
                        "retrospective": retrospective_artifact.artifact_id,
                        "index": index,
                        "statement": statement,
                    }))?
                    .as_str()
                    .to_owned(),
                ),
                origin: LessonOrigin::OutcomeDerived,
                lifecycle: LessonLifecycle::Draft,
                title: format!("Outcome lesson {}", index + 1),
                statement: statement.to_owned(),
                rationale: retrospective.summary.clone(),
                recommended_behavior: candidate.recommended_behavior.clone(),
                exclusions: candidate.exclusions.clone(),
                scope: LessonScope {
                    assets: candidate.assets.clone(),
                    horizons: candidate.horizons.clone(),
                    ..LessonScope::default()
                },
                source_refs: std::iter::once(retrospective_ref.clone())
                    .chain(candidate.evidence_refs.iter().cloned())
                    .collect(),
                supersedes: Vec::new(),
                conflicts_with: Vec::new(),
                confidence_ppm: 500_000,
                authored_by: None,
                approved_by: None,
                created_at,
                updated_at: created_at,
                governance: Some(LessonGovernance::outcome_quarantined(created_at)),
            };
            lessons.push(lesson);
        }
        Ok((retrospective_artifact, lessons))
    }
}

fn pending_outcome_lesson_drafts(
    connection: &Connection,
    retrospective: &Artifact,
    lessons: Vec<Lesson>,
) -> (Vec<Lesson>, Vec<OutcomeLessonDraftFailure>) {
    let mut pending = Vec::new();
    let mut failures = Vec::new();
    for lesson in lessons {
        match outcome_lesson_draft_pending(connection, retrospective, &lesson) {
            Ok(true) => pending.push(lesson),
            Ok(false) => {}
            Err(error) => failures.push(OutcomeLessonDraftFailure {
                lesson_id: lesson.lesson_id,
                reason: error.to_string(),
            }),
        }
    }
    (pending, failures)
}

fn outcome_lesson_draft_pending(
    connection: &Connection,
    retrospective: &Artifact,
    lesson: &Lesson,
) -> StoreResult<bool> {
    lesson.validate()?;
    for reference in lesson.source_refs.iter().skip(1) {
        let artifact = read_artifact(connection, &reference.artifact_id)?;
        artifact.validate()?;
        if artifact.kind != reference.kind || !retrospective.source_refs.contains(reference) {
            return Err(StoreError::InvalidLearningCommit("outcome_lesson.evidence"));
        }
    }
    Ok(!outcome_lesson_genesis_exists(connection, lesson)?)
}

fn unique_retrospective_source(artifact: &Artifact) -> StoreResult<&ArtifactRef> {
    let mut sources = artifact
        .source_refs
        .iter()
        .filter(|r| r.kind == ArtifactKind::Retrospective);
    let first = sources.next().ok_or(StoreError::InvalidLearningCommit(
        "outcome_lesson.retrospective_source",
    ))?;
    if sources.next().is_some() {
        return Err(StoreError::InvalidLearningCommit(
            "outcome_lesson.retrospective_source",
        ));
    }
    Ok(first)
}

fn outcome_lesson_artifact(
    connection: &Connection,
    run_id: &RunId,
    reference: &ArtifactRef,
) -> StoreResult<Artifact> {
    let artifact = read_artifact(connection, &reference.artifact_id)?;
    artifact.validate()?;
    assert_canonical_paper_artifact(connection, &artifact)?;
    if artifact.kind != reference.kind
        || artifact.origin.as_ref().and_then(|o| o.run_id.as_ref()) != Some(run_id)
    {
        return Err(StoreError::InvalidLearningCommit(
            "outcome_lesson.artifact_origin",
        ));
    }
    Ok(artifact)
}

pub(super) fn outcome_lesson_genesis_exists(
    connection: &Connection,
    lesson: &Lesson,
) -> StoreResult<bool> {
    if ensure_lesson_table_set(connection)? == 0 {
        return Ok(false);
    }
    let genesis: Option<String> = connection.query_row(
        "SELECT artifact_id FROM rebuild_lesson_events WHERE lesson_id=?1 AND event_type='lesson.created' ORDER BY event_id LIMIT 1",
        params![lesson.lesson_id.0], |row| row.get(0),
    ).optional()?;
    let head_exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_lesson_heads WHERE lesson_id=?1)",
        params![lesson.lesson_id.0],
        |row| row.get(0),
    )?;
    match (head_exists, genesis) {
        (false, None) => Ok(false),
        (true, Some(genesis)) => {
            let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(genesis)?))?;
            artifact.validate()?;
            let persisted: Lesson = serde_json::from_slice(&blob::read_blob_bytes(
                connection,
                &artifact.blob.hash,
                artifact.blob.bytes,
            )?)?;
            if artifact.kind == ArtifactKind::Lesson
                && artifact.lifecycle == ArtifactLifecycle::Canonical
                && artifact.producer == "learning.lesson.outcome"
                && artifact.created_at == lesson.created_at
                && has_exact_source_refs(&artifact, &lesson.source_refs)
                && persisted == *lesson
            {
                return Ok(true);
            }
            Err(StoreError::InvalidLearningCommit(
                "outcome_lesson.conflicting_genesis",
            ))
        }
        _ => Err(StoreError::InvalidLearningCommit(
            "outcome_lesson.missing_head_or_genesis",
        )),
    }
}
