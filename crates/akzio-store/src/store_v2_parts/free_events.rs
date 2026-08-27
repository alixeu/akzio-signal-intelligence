fn validate_gate_lifecycle_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
             AND event_type IN (
                    ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10
             )
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::ExecutionAllocationCreated.as_str(),
            LifecycleEventType::ExecutionCommitted.as_str(),
            LifecycleEventType::ExecutionCommitmentRecovered.as_str(),
            LifecycleEventType::ExecutionContextCreated.as_str(),
            LifecycleEventType::ExecutionPlanCreated.as_str(),
            LifecycleEventType::ExecutionRepriceCommitted.as_str(),
            LifecycleEventType::ExecutionRepriceRecovered.as_str(),
            LifecycleEventType::ExecutionVerdictCreated.as_str(),
            LifecycleEventType::ExecutionVerdictNoOrder.as_str(),
        ],
        decode_lifecycle_row,
    )?;

    for row in rows {
        let LifecycleRow {
            cursor,
            run_id: event_run_id,
            task_id,
            attempt_id,
            event_type,
            artifact_id,
        } = row?;
        let task_id = task_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no task lineage",
                event_type.as_str()
            ))
        })?;
        let attempt_id = attempt_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no attempt lineage",
                event_type.as_str()
            ))
        })?;
        let artifact_id = artifact_id.ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} has no artifact",
                event_type.as_str()
            ))
        })?;
        let expected_kind = match event_type {
            LifecycleEventType::ExecutionAllocationCreated
            | LifecycleEventType::ExecutionPlanCreated => ArtifactKind::ExecutionPlan,
            LifecycleEventType::ExecutionContextCreated => ArtifactKind::ExecutionContext,
            LifecycleEventType::ExecutionVerdictCreated
            | LifecycleEventType::ExecutionVerdictNoOrder => ArtifactKind::ExecutionVerdict,
            LifecycleEventType::ExecutionCommitted
            | LifecycleEventType::ExecutionCommitmentRecovered => ArtifactKind::ExecutionCommitment,
            LifecycleEventType::ExecutionRepriceCommitted
            | LifecycleEventType::ExecutionRepriceRecovered => ArtifactKind::ExecutionReprice,
            _ => unreachable!("gate lifecycle query emits fixed event types"),
        };
        let artifact = read_artifact(connection, &artifact_id)?;
        artifact.validate()?;
        if artifact.kind != expected_kind {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} references {:?}, expected {:?}",
                event_type.as_str(),
                artifact.kind,
                expected_kind
            )));
        }
        let origin = artifact.origin.as_ref().ok_or_else(|| {
            StoreError::Integrity(format!(
                "{} cursor {cursor} artifact has no origin",
                event_type.as_str()
            ))
        })?;
        let recovered = matches!(
            event_type,
            LifecycleEventType::ExecutionCommitmentRecovered
                | LifecycleEventType::ExecutionRepriceRecovered
        );
        if origin.run_id.as_ref() != Some(&event_run_id)
            || origin.task_id.as_ref() != Some(&task_id)
            || (!recovered && origin.attempt_id.as_ref() != Some(&attempt_id))
        {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} artifact origin does not match event lineage",
                event_type.as_str()
            )));
        }
        for source in &artifact.source_refs {
            let source_artifact = read_artifact(connection, &source.artifact_id)?;
            if source_artifact.kind != source.kind {
                return Err(StoreError::Integrity(format!(
                    "{} cursor {cursor} source {} kind {:?} disagrees with ref {:?}",
                    event_type.as_str(),
                    source.artifact_id.0,
                    source_artifact.kind,
                    source.kind
                )));
            }
        }
    }
    Ok(())
}

fn ensure_no_pending_tool_calls(
    connection: &Connection,
    run_id: &RunId,
    task_id: &TaskId,
    attempt_id: &akzio_domain::AttemptId,
) -> StoreResult<()> {
    let called = connection.query_row(
        r#"SELECT COUNT(*)
               FROM rebuild_events
               WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3
                 AND event_type = ?4"#,
        params![
            run_id.0,
            task_id.0,
            attempt_id.0,
            LifecycleEventType::ToolCalled.as_str(),
        ],
        |row| row.get::<_, u64>(0),
    )?;
    let terminal = connection.query_row(
        r#"SELECT COUNT(*)
               FROM rebuild_events AS terminal
               JOIN rebuild_artifact_refs AS reference
                 ON reference.artifact_id = terminal.artifact_id
               WHERE terminal.run_id = ?1
                 AND terminal.task_id = ?2
                 AND terminal.attempt_id = ?3
                 AND terminal.event_type IN (?4, ?5)
                 AND reference.source_kind = ?6"#,
        params![
            run_id.0,
            task_id.0,
            attempt_id.0,
            LifecycleEventType::ToolCompleted.as_str(),
            LifecycleEventType::ToolFailed.as_str(),
            enum_name(ArtifactKind::ToolCall),
        ],
        |row| row.get::<_, u64>(0),
    )?;
    if called > terminal {
        return Err(StoreError::Integrity(format!(
            "attempt {attempt_id} has pending tool calls"
        )));
    }
    Ok(())
}

fn paper_effect_terminal_exists(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    effect_id: &ArtifactId,
) -> StoreResult<bool> {
    let found = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE run_id = ?1 AND artifact_id = ?2 AND event_type IN (?3, ?4))",
        params![
            run_id.0,
            effect_id.0.as_str(),
            LifecycleEventType::ExecutionEffectSettled.as_str(),
            LifecycleEventType::ExecutionEffectRecovered.as_str(),
        ],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(found != 0)
}

fn assert_idempotent_outcome_schedule_commit(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    schedule: &Artifact,
) -> StoreResult<()> {
    let attempt = transaction
        .query_row(
            r#"SELECT a.run_id, a.task_id, a.lease_id, a.epoch, a.status,
                      t.status, t.contract_hash
                 FROM rebuild_attempts AS a
                 JOIN rebuild_tasks AS t ON t.task_id = a.task_id
                WHERE a.attempt_id = ?1"#,
            params![permit.attempt_id.0],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((run_id, task_id, lease_id, epoch, attempt_status, task_status, contract_hash)) =
        attempt
    else {
        return Err(StoreError::StalePermit(permit.task_id.clone()));
    };
    if run_id != permit.run_id.0
        || task_id != permit.task_id.0
        || lease_id != permit.lease_id.0
        || epoch != permit.epoch
        || attempt_status != "succeeded"
        || task_status != "succeeded"
        || contract_hash.as_deref().map(ContentHash::new).transpose()? != permit.contract_hash
    {
        return Err(StoreError::StalePermit(permit.task_id.clone()));
    }
    assert_origin_matches(schedule.origin.as_ref(), permit)?;
    let stored = read_artifact(transaction, &schedule.artifact_id)?;
    if stored != *schedule {
        return Err(StoreError::Integrity(
            "outcome schedule retry does not match committed artifact".to_owned(),
        ));
    }
    let output_count = transaction.query_row(
        r#"SELECT COUNT(*) FROM rebuild_attempt_outputs
           WHERE attempt_id = ?1 AND task_id = ?2 AND artifact_id = ?3"#,
        params![
            permit.attempt_id.0,
            permit.task_id.0,
            schedule.artifact_id.0.as_str()
        ],
        |row| row.get::<_, u64>(0),
    )?;
    if output_count != 1 {
        return Err(StoreError::CommittedOutputAttempt {
            task_id: permit.task_id.clone(),
            attempt_id: permit.attempt_id.clone(),
        });
    }
    Ok(())
}

fn assert_origin_matches(
    origin: Option<&ArtifactOrigin>,
    permit: &TaskWritePermit,
) -> StoreResult<()> {
    let Some(origin) = origin else {
        return Err(StoreError::PermitOriginMismatch);
    };
    if origin.run_id.as_ref() != Some(&permit.run_id)
        || origin.task_id.as_ref() != Some(&permit.task_id)
        || origin.attempt_id.as_ref() != Some(&permit.attempt_id)
        || origin.contract_hash != permit.contract_hash
    {
        return Err(StoreError::PermitOriginMismatch);
    }
    Ok(())
}

fn task_retry_policy(
    transaction: &Transaction<'_>,
    task_id: &TaskId,
) -> StoreResult<(RetryPolicy, FailureDisposition)> {
    let (retry_json, on_failure) = transaction
        .query_row(
            "SELECT retry_json, on_failure FROM rebuild_tasks WHERE task_id = ?1",
            params![task_id.0],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .ok_or_else(|| StoreError::MissingTask(task_id.clone()))?;
    Ok((serde_json::from_str(&retry_json)?, parse_enum(&on_failure)?))
}

fn commit_attempt_transaction(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    artifacts: &[Artifact],
    status: TaskStatus,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    commit_attempt_transaction_with_effect(transaction, permit, artifacts, status, None, now)
}

fn commit_attempt_transaction_with_effect(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    artifacts: &[Artifact],
    status: TaskStatus,
    effect_event: Option<(&ArtifactRef, LifecycleEventType)>,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    assert_permit(transaction, permit)?;
    for artifact in artifacts {
        assert_task_artifact_lifecycle(transaction, &permit.run_id, artifact)?;
    }
    let (_, on_failure) = task_retry_policy(transaction, &permit.task_id)?;
    for artifact in artifacts {
        assert_origin_matches(artifact.origin.as_ref(), permit)?;
    }
    if !artifacts.is_empty() {
        insert_artifact_batch(transaction, artifacts)?;
    }
    for artifact in artifacts {
        let event_id = append_event(
            transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&artifact.artifact_id),
            now,
        )?;
        if status == TaskStatus::Succeeded {
            record_attempt_output(transaction, permit, &artifact.artifact_id, event_id)?;
        }
    }
    if let Some((effect, event_type)) = effect_event {
        append_event(
            transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            event_type,
            Some(&effect.artifact_id),
            now,
        )?;
    }
    finish_permitted_task(
        transaction,
        permit,
        status,
        on_failure,
        artifacts.last().map(|artifact| &artifact.artifact_id),
        now,
    )?;
    Ok(())
}
