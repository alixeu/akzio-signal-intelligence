fn record_attempt_output(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    artifact_id: &ArtifactId,
    event_id: i64,
) -> StoreResult<()> {
    transaction.execute(
        r#"INSERT OR IGNORE INTO rebuild_attempt_outputs
            (attempt_id, task_id, artifact_id, event_id)
          VALUES (?1, ?2, ?3, ?4)"#,
        params![
            permit.attempt_id.0,
            permit.task_id.0,
            artifact_id.0.as_str(),
            event_id,
        ],
    )?;
    Ok(())
}

fn read_committed_attempt_outputs(
    connection: &Connection,
    expected_run_id: Option<&RunId>,
    task_id: &TaskId,
    attempt_id: &AttemptId,
) -> StoreResult<Vec<Artifact>> {
    let attempt = connection
        .query_row(
            r#"SELECT a.run_id, a.task_id, a.status, t.status
                 FROM rebuild_attempts AS a
                 JOIN rebuild_tasks AS t ON t.task_id = a.task_id
                WHERE a.attempt_id = ?1"#,
            params![attempt_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((attempt_run_id, attempt_task_id, attempt_status, task_status)) = attempt else {
        return Err(StoreError::CommittedOutputAttempt {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        });
    };
    if attempt_task_id != task_id.0
        || attempt_status != "succeeded"
        || task_status != "succeeded"
        || expected_run_id.is_some_and(|run_id| attempt_run_id != run_id.0)
    {
        return Err(StoreError::CommittedOutputAttempt {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        });
    }

    let mut statement = connection.prepare(
        r#"SELECT o.artifact_id
              FROM rebuild_attempt_outputs AS o
              JOIN rebuild_events AS e ON e.event_id = o.event_id
             WHERE o.attempt_id = ?1
               AND o.task_id = ?2
               AND e.run_id = ?3
               AND e.task_id = o.task_id
               AND e.attempt_id = o.attempt_id
               AND e.event_type = 'artifact.committed'
               AND e.artifact_id = o.artifact_id
             ORDER BY o.event_id ASC"#,
    )?;
    let ids = statement
        .query_map(params![attempt_id.0, task_id.0, attempt_run_id], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if ids.is_empty() {
        return Err(StoreError::CommittedOutputAttempt {
            task_id: task_id.clone(),
            attempt_id: attempt_id.clone(),
        });
    }
    drop(statement);
    ids.into_iter()
        .map(|id| read_artifact(connection, &ArtifactId(ContentHash::new(id)?)))
        .collect()
}

fn refresh_run_status(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let statuses = transaction
        .prepare("SELECT status FROM rebuild_tasks WHERE run_id = ?1")?
        .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    if statuses.is_empty()
        || statuses
            .iter()
            .any(|status| status == "running" || status == "queued")
    {
        return Ok(());
    }
    let status = if statuses.iter().any(|status| status == "failed") {
        "failed"
    } else if statuses.iter().all(|status| status == "cancelled") {
        "cancelled"
    } else {
        "completed"
    };
    transaction.execute(
        "UPDATE rebuild_runs SET status = ?1, finished_at = ?2 WHERE run_id = ?3",
        params![status, now.to_rfc3339(), run_id.0],
    )?;
    Ok(())
}

fn read_artifact(connection: &Connection, artifact_id: &ArtifactId) -> StoreResult<Artifact> {
    let row = connection
        .query_row(
            r#"SELECT kind, blob_hash, media_type, bytes, producer, lifecycle, provenance_json, origin_json, created_at
               FROM rebuild_artifacts WHERE artifact_id = ?1"#,
            params![artifact_id.0.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                ))
            },
        )
        .optional()?;
    let Some((kind, hash, media_type, bytes, producer, lifecycle, provenance, origin, created_at)) =
        row
    else {
        return Err(StoreError::MissingArtifact(artifact_id.clone()));
    };
    let mut statement = connection.prepare(
        r#"SELECT source_artifact_id, source_kind
           FROM rebuild_artifact_refs WHERE artifact_id = ?1
           ORDER BY source_artifact_id"#,
    )?;
    let source_refs = statement
        .query_map(params![artifact_id.0.as_str()], |row| {
            Ok(ArtifactRef {
                artifact_id: ArtifactId(
                    ContentHash::new(row.get::<_, String>(0)?).map_err(|error| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                    })?,
                ),
                kind: parse_enum(&row.get::<_, String>(1)?)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Artifact {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        artifact_id: artifact_id.clone(),
        kind: parse_enum(&kind)?,
        blob: BlobRef {
            hash: ContentHash::new(hash)?,
            media_type,
            bytes,
        },
        producer,
        lifecycle: parse_enum(&lifecycle)?,
        provenance: serde_json::from_str(&provenance)?,
        origin: origin
            .map(|encoded| serde_json::from_str::<Option<ArtifactOrigin>>(&encoded))
            .transpose()?
            .flatten(),
        source_refs,
        created_at: parse_time(&created_at)?,
    })
}

fn read_kind_artifacts(connection: &Connection, kind: ArtifactKind) -> StoreResult<Vec<Artifact>> {
    let mut statement = connection.prepare(
        "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY created_at ASC, artifact_id ASC",
    )?;
    let ids = statement
        .query_map(params![enum_name(kind)], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    ids.into_iter()
        .map(|id| read_artifact(connection, &ArtifactId(ContentHash::new(id)?)))
        .collect()
}

fn verify_retrospective_history(store: &V2Store, connection: &Connection) -> StoreResult<()> {
    let mut identities = BTreeSet::new();
    for artifact in read_kind_artifacts(connection, ArtifactKind::Retrospective)? {
        artifact.validate()?;
        let payload: Retrospective = store.read_artifact_payload(&artifact)?;
        payload.validate()?;
        let run_id = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            .ok_or_else(|| StoreError::Integrity("retrospective has no run lineage".to_owned()))?;
        let identity = (run_id.clone(), payload.outcome_id.clone(), payload.horizon);
        if !identities.insert(identity) {
            return Err(StoreError::Integrity(
                "duplicate retrospective identity".to_owned(),
            ));
        }
        if payload.horizon == OutcomeHorizon::T5
            && artifact.lifecycle != ArtifactLifecycle::Canonical
        {
            return Err(StoreError::Integrity(
                "T5 retrospective is not canonical".to_owned(),
            ));
        }
        if payload.horizon != OutcomeHorizon::T5
            && artifact.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::Integrity(
                "intermediate retrospective is not RunScoped".to_owned(),
            ));
        }
        let outcome = read_artifact(connection, &payload.outcome.artifact_id)?;
        if outcome.kind != ArtifactKind::Outcome {
            return Err(StoreError::Integrity(
                "retrospective outcome closure is invalid".to_owned(),
            ));
        }
        let outcome_payload: Outcome = store.read_artifact_payload(&outcome)?;
        if payload.horizon == OutcomeHorizon::T5 {
            outcome_payload.validate_sealed().map_err(|error| {
                StoreError::Integrity(format!("sealed outcome is invalid: {error}"))
            })?;
            if outcome.lifecycle != ArtifactLifecycle::Canonical {
                return Err(StoreError::Integrity(
                    "T5 retrospective points to non-canonical outcome".to_owned(),
                ));
            }
        } else {
            outcome_payload.validate().map_err(|error| {
                StoreError::Integrity(format!("partial outcome is invalid: {error}"))
            })?;
            if outcome.lifecycle != ArtifactLifecycle::RunScoped
                || outcome_payload.sealed_at.is_some()
            {
                return Err(StoreError::Integrity(
                    "intermediate retrospective points to sealed outcome".to_owned(),
                ));
            }
        }
        if payload.horizon == OutcomeHorizon::T5
            && payload.status == RetrospectiveStatus::Complete
            && artifact.lifecycle != ArtifactLifecycle::Canonical
        {
            return Err(StoreError::Integrity(
                "complete T5 retrospective is not canonical".to_owned(),
            ));
        }
    }
    Ok(())
}

fn verify_attempt_relation_history(store: &V2Store, connection: &Connection) -> StoreResult<()> {
    let mut parent_by_child = BTreeMap::<(RunId, TaskId, AttemptId), AttemptId>::new();
    for artifact in read_kind_artifacts(connection, ArtifactKind::AttemptRelation)? {
        artifact.validate()?;
        let relation: AttemptRelation = store.read_artifact_payload(&artifact)?;
        relation.validate()?;
        let parent_exists = connection
            .query_row(
                "SELECT 1 FROM rebuild_attempts WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3",
                params![relation.run_id.0, relation.task_id.0, relation.parent_attempt_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !parent_exists {
            return Err(StoreError::Integrity(
                "attempt relation parent is missing".to_owned(),
            ));
        }
        let child_exists = connection
            .query_row(
                "SELECT 1 FROM rebuild_attempts WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3",
                params![relation.run_id.0, relation.task_id.0, relation.child_attempt_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !child_exists {
            return Err(StoreError::Integrity(
                "attempt relation child missing".to_owned(),
            ));
        }
        let key = (
            relation.run_id.clone(),
            relation.task_id.clone(),
            relation.child_attempt_id.clone(),
        );
        if parent_by_child
            .insert(key.clone(), relation.parent_attempt_id.clone())
            .is_some()
        {
            return Err(StoreError::Integrity(
                "attempt relation child has multiple parents".to_owned(),
            ));
        }
    }
    for (run_id, task_id, child) in parent_by_child.keys() {
        let mut cursor = child.clone();
        let mut hops = 0_u16;
        while let Some(parent) =
            parent_by_child.get(&(run_id.clone(), task_id.clone(), cursor.clone()))
        {
            cursor = parent.clone();
            hops = hops.saturating_add(1);
            if cursor == *child || hops > 1_024 {
                return Err(StoreError::Integrity("attempt relation cycle".to_owned()));
            }
        }
    }
    Ok(())
}

fn row_to_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<(RunId, WorkflowNode)> {
    let task_id = TaskId(row.get(0)?);
    let run_id = RunId(row.get(1)?);
    let recipe_id = TaskRecipeId::new(row.get::<_, String>(2)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let budget = serde_json::from_str(&row.get::<_, String>(6)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let retry = serde_json::from_str(&row.get::<_, String>(7)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let on_failure = parse_enum(&row.get::<_, String>(8)?)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    Ok((
        run_id,
        WorkflowNode {
            task_id,
            recipe_id,
            contract_hash: row
                .get::<_, Option<String>>(4)?
                .map(ContentHash::new)
                .transpose()
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            objective: row.get(3)?,
            dependencies: Vec::new(),
            input_artifacts: serde_json::from_str(&row.get::<_, String>(10)?)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
            priority: row.get(5)?,
            budget,
            retry,
            on_failure,
            parent_task_id: row.get::<_, Option<String>>(9)?.map(TaskId),
        },
    ))
}

fn parse_time(value: &str) -> StoreResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| StoreError::Integrity(format!("invalid time {value}: {error}")))
}

/// The indexed subject ID is derived from this typed JSON, never accepted as
/// an independent authority. A corrupt or hand-edited row must therefore
/// fail closed rather than silently changing a policy namespace.
fn parse_persisted_subject(subject_id: &str, subject_json: &str) -> StoreResult<PolicySubject> {
    let subject: PolicySubject = serde_json::from_str(subject_json)?;
    subject.validate()?;
    if subject.subject_id() != subject_id {
        return Err(StoreError::Integrity(format!(
            "policy subject JSON does not match indexed identity {subject_id}"
        )));
    }
    Ok(subject)
}

fn read_policy_evaluation(
    connection: &Connection,
    evaluation_artifact_id: &ArtifactId,
) -> StoreResult<Option<StoredPolicyEvaluation>> {
    let row = connection
        .query_row(
            r#"SELECT subject_id, subject_json, outcome_artifact_id, experience_artifact_id,
                      candidate_policy_artifact_id, from_state_json, to_state_json,
                      transition_id, run_id, consumed_pair_cursor, event_cursor, completed_at
               FROM rebuild_policy_evaluations WHERE evaluation_artifact_id = ?1"#,
            params![evaluation_artifact_id.0.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, String>(11)?,
                ))
            },
        )
        .optional()?;
    let Some((
        subject_id,
        subject_json,
        outcome_artifact_id,
        experience_artifact_id,
        candidate_policy_artifact_id,
        from,
        to,
        transition_id,
        run_id,
        consumed_pair_cursor,
        event_cursor,
        completed_at,
    )) = row
    else {
        return Ok(None);
    };
    Ok(Some(StoredPolicyEvaluation {
        subject: parse_persisted_subject(&subject_id, &subject_json)?,
        outcome_artifact_id: ArtifactId(ContentHash::new(outcome_artifact_id)?),
        experience_artifact_id: ArtifactId(ContentHash::new(experience_artifact_id)?),
        evaluation_artifact_id: evaluation_artifact_id.clone(),
        candidate_policy_artifact_id: candidate_policy_artifact_id
            .map(ContentHash::new)
            .transpose()?
            .map(ArtifactId),
        from: serde_json::from_str(&from)?,
        to: serde_json::from_str(&to)?,
        transition_id: transition_id.map(PolicyTransitionId),
        run_id: RunId(run_id),
        consumed_pair_cursor,
        event_cursor,
        completed_at: parse_time(&completed_at)?,
    }))
}
