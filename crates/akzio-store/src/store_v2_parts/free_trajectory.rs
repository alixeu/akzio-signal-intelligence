struct LifecycleRow {
    cursor: i64,
    run_id: RunId,
    task_id: Option<TaskId>,
    attempt_id: Option<akzio_domain::AttemptId>,
    event_type: LifecycleEventType,
    artifact_id: Option<ArtifactId>,
}

fn decode_lifecycle_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LifecycleRow> {
    Ok(LifecycleRow {
        cursor: row.get(0)?,
        run_id: RunId(row.get(1)?),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId),
        attempt_id: row
            .get::<_, Option<String>>(3)?
            .map(akzio_domain::AttemptId),
        event_type: LifecycleEventType::parse(&row.get::<_, String>(4)?)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
        artifact_id: row
            .get::<_, Option<String>>(5)?
            .map(|value| {
                ContentHash::new(value)
                    .map(ArtifactId)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
            })
            .transpose()?,
    })
}

fn validate_tool_lifecycle_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
                 AND event_type IN (?2, ?3, ?4, ?5)
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::ToolCalled.as_str(),
            LifecycleEventType::ToolCompleted.as_str(),
            LifecycleEventType::ToolFailed.as_str(),
            LifecycleEventType::TaskSucceeded.as_str(),
        ],
        decode_lifecycle_row,
    )?;

    #[derive(Clone)]
    struct CalledEvent {
        cursor: i64,
        run_id: RunId,
        task_id: TaskId,
        attempt_id: akzio_domain::AttemptId,
    }

    let mut called_by_key =
        BTreeMap::<(RunId, TaskId, akzio_domain::AttemptId, ArtifactId), CalledEvent>::new();
    let mut terminal_by_call = BTreeSet::new();
    let mut pending_by_task =
        BTreeMap::<(RunId, TaskId, akzio_domain::AttemptId), BTreeSet<ArtifactId>>::new();
    let mut succeeded_tasks = BTreeSet::<(RunId, TaskId, akzio_domain::AttemptId)>::new();

    for row in rows {
        let LifecycleRow {
            cursor,
            run_id: event_run_id,
            task_id,
            attempt_id,
            event_type,
            artifact_id,
        } = row?;
        let Some(task_id) = task_id else {
            return Err(StoreError::Integrity(format!(
                "tool event at cursor {cursor} has no task"
            )));
        };
        let Some(attempt_id) = attempt_id else {
            return Err(StoreError::Integrity(format!(
                "tool event at cursor {cursor} has no attempt"
            )));
        };
        let task_key = (event_run_id.clone(), task_id.clone(), attempt_id.clone());
        match event_type {
            LifecycleEventType::TaskSucceeded => {
                if pending_by_task
                    .get(&task_key)
                    .is_some_and(|pending| !pending.is_empty())
                {
                    return Err(StoreError::Integrity(format!(
                        "task.succeeded cursor {cursor} has pending tool calls"
                    )));
                }
                if !succeeded_tasks.insert(task_key) {
                    return Err(StoreError::Integrity(format!(
                        "task.succeeded cursor {cursor} repeats task terminal"
                    )));
                }
            }
            LifecycleEventType::ToolCalled => {
                let Some(artifact_id) = artifact_id else {
                    return Err(StoreError::Integrity(format!(
                        "tool event at cursor {cursor} has no artifact"
                    )));
                };
                if succeeded_tasks.contains(&task_key) {
                    return Err(StoreError::Integrity(format!(
                        "tool.called cursor {cursor} occurs after task.succeeded"
                    )));
                }
                let event_key = (
                    event_run_id.clone(),
                    task_id.clone(),
                    attempt_id.clone(),
                    artifact_id.clone(),
                );
                let artifact = read_artifact(connection, &artifact_id)?;
                if artifact.kind != ArtifactKind::ToolCall {
                    return Err(StoreError::Integrity(format!(
                        "tool.called cursor {cursor} references {:?}, expected tool_call",
                        artifact.kind
                    )));
                }
                if called_by_key
                    .insert(
                        event_key,
                        CalledEvent {
                            cursor,
                            run_id: event_run_id,
                            task_id,
                            attempt_id,
                        },
                    )
                    .is_some()
                {
                    return Err(StoreError::Integrity(format!(
                        "duplicate tool.called event for {} at cursor {cursor}",
                        artifact_id.0
                    )));
                }
                pending_by_task
                    .entry(task_key.clone())
                    .or_default()
                    .insert(artifact_id.clone());
            }
            LifecycleEventType::ToolCompleted | LifecycleEventType::ToolFailed => {
                let Some(artifact_id) = artifact_id else {
                    return Err(StoreError::Integrity(format!(
                        "tool event at cursor {cursor} has no artifact"
                    )));
                };
                if succeeded_tasks.contains(&task_key) {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} occurs after task.succeeded",
                        event_type.as_str()
                    )));
                }
                let artifact = read_artifact(connection, &artifact_id)?;
                if artifact.kind != ArtifactKind::ToolResult {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} references {:?}, expected tool_result",
                        event_type.as_str(),
                        artifact.kind
                    )));
                }
                let tool_call_refs = artifact
                    .source_refs
                    .iter()
                    .filter(|reference| reference.kind == ArtifactKind::ToolCall)
                    .collect::<Vec<_>>();
                if tool_call_refs.len() != 1 {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} must reference exactly one tool_call",
                        event_type.as_str()
                    )));
                }
                let call_artifact_id = tool_call_refs[0].artifact_id.clone();
                let call_key = (
                    event_run_id.clone(),
                    task_id.clone(),
                    attempt_id.clone(),
                    call_artifact_id.clone(),
                );
                let Some(called) = called_by_key.get(&call_key) else {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} has no prior tool.called for {}",
                        event_type.as_str(),
                        call_artifact_id.0
                    )));
                };
                if called.cursor >= cursor
                    || called.run_id != event_run_id
                    || called.task_id != task_id
                    || called.attempt_id != attempt_id
                {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} does not match its tool.called lineage",
                        event_type.as_str()
                    )));
                }
                let terminal_key = (event_run_id, task_id, attempt_id, call_artifact_id.clone());
                if !terminal_by_call.insert(terminal_key) {
                    return Err(StoreError::Integrity(format!(
                        "tool call already has a terminal event at cursor {cursor}"
                    )));
                }
                let Some(pending) = pending_by_task.get_mut(&task_key) else {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} has no pending tool call",
                        event_type.as_str()
                    )));
                };
                if !pending.remove(&call_artifact_id) {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} repeats or misses pending tool call",
                        event_type.as_str()
                    )));
                }
                if pending.is_empty() {
                    pending_by_task.remove(&task_key);
                }
            }
            _ => unreachable!("tool lifecycle query emits fixed event types"),
        }
    }
    Ok(())
}

fn validate_agent_turn_lifecycle_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
               AND event_type IN (?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::AgentTurnStarted.as_str(),
            LifecycleEventType::AgentTurnCompleted.as_str(),
            LifecycleEventType::AgentTurnRetryableFailed.as_str(),
            LifecycleEventType::AgentTurnFailed.as_str(),
            LifecycleEventType::TaskDeferred.as_str(),
            LifecycleEventType::TaskRetryScheduled.as_str(),
            LifecycleEventType::TaskRetryExhausted.as_str(),
            LifecycleEventType::TaskRecovered.as_str(),
            LifecycleEventType::TaskRecoveryExhausted.as_str(),
            LifecycleEventType::TaskCancelled.as_str(),
            LifecycleEventType::TaskSucceeded.as_str(),
            LifecycleEventType::TaskFailed.as_str(),
            LifecycleEventType::TaskSkipped.as_str(),
        ],
        decode_lifecycle_row,
    )?;

    #[derive(Default)]
    struct TurnState {
        pending_start: bool,
        saw_started: bool,
        terminal_artifacts: BTreeSet<ArtifactId>,
        last_terminal: Option<LifecycleEventType>,
    }

    let mut states = BTreeMap::<(RunId, TaskId, akzio_domain::AttemptId), TurnState>::new();
    for row in rows {
        let LifecycleRow {
            cursor,
            run_id: event_run_id,
            task_id,
            attempt_id,
            event_type,
            artifact_id,
        } = row?;
        let key = if let (Some(task_id), Some(attempt_id)) = (&task_id, &attempt_id) {
            (event_run_id.clone(), task_id.clone(), attempt_id.clone())
        } else {
            if matches!(
                event_type,
                LifecycleEventType::AgentTurnStarted
                    | LifecycleEventType::AgentTurnCompleted
                    | LifecycleEventType::AgentTurnRetryableFailed
                    | LifecycleEventType::AgentTurnFailed
            ) {
                return Err(StoreError::Integrity(format!(
                    "agent lifecycle event at cursor {cursor} has incomplete task attempt lineage"
                )));
            }
            continue;
        };
        let state = states.entry(key.clone()).or_default();
        match event_type {
            LifecycleEventType::AgentTurnStarted => {
                if artifact_id.is_some() {
                    return Err(StoreError::Integrity(format!(
                        "agent.turn_started cursor {cursor} unexpectedly has an artifact"
                    )));
                }
                if state.pending_start {
                    return Err(StoreError::Integrity(format!(
                        "agent.turn_started cursor {cursor} follows an unresolved model turn"
                    )));
                }
                state.pending_start = true;
                state.saw_started = true;
            }
            LifecycleEventType::AgentTurnCompleted
            | LifecycleEventType::AgentTurnRetryableFailed
            | LifecycleEventType::AgentTurnFailed => {
                let Some(artifact_id) = artifact_id else {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} has no AgentTurn artifact",
                        event_type.as_str()
                    )));
                };
                let artifact = read_artifact(connection, &artifact_id)?;
                if artifact.kind != ArtifactKind::AgentTurn {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} references {:?}, expected agent_turn",
                        event_type.as_str(),
                        artifact.kind
                    )));
                }
                let origin = artifact.origin.as_ref().ok_or_else(|| {
                    StoreError::Integrity(format!(
                        "{} cursor {cursor} AgentTurn artifact has no origin",
                        event_type.as_str()
                    ))
                })?;
                if origin.run_id.as_ref() != Some(&key.0)
                    || origin.task_id.as_ref() != Some(&key.1)
                    || origin.attempt_id.as_ref() != Some(&key.2)
                {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} AgentTurn artifact origin does not match task attempt",
                        event_type.as_str()
                    )));
                }
                if !state.terminal_artifacts.insert(artifact_id) {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} repeats an AgentTurn terminal artifact",
                        event_type.as_str()
                    )));
                }
                // Legacy/audit terminal artifacts without a started event remain
                // readable for existing v2 stores.  The current no-model retry
                // path is a capability preflight failure after a retryable
                // terminal; retain that one compatibility exception without
                // coupling the store to research artifact payloads.
                let capability_preflight_retry = state.last_terminal
                    == Some(LifecycleEventType::AgentTurnRetryableFailed)
                    && event_type == LifecycleEventType::AgentTurnFailed;
                if !state.pending_start && state.saw_started && !capability_preflight_retry {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} has no pending AgentTurn start",
                        event_type.as_str()
                    )));
                }
                state.pending_start = false;
                state.last_terminal = Some(event_type);
            }
            LifecycleEventType::TaskDeferred
            | LifecycleEventType::TaskRetryScheduled
            | LifecycleEventType::TaskRetryExhausted
            | LifecycleEventType::TaskRecovered
            | LifecycleEventType::TaskRecoveryExhausted
            | LifecycleEventType::TaskCancelled => {
                // These events abandon the in-flight attempt during retry or
                // recovery; they are the durable close for a crashed turn.
                state.pending_start = false;
                state.last_terminal = None;
            }
            LifecycleEventType::TaskSucceeded
            | LifecycleEventType::TaskFailed
            | LifecycleEventType::TaskSkipped => {
                if state.pending_start {
                    return Err(StoreError::Integrity(format!(
                        "{} cursor {cursor} closes a task with a pending AgentTurn",
                        event_type.as_str()
                    )));
                }
            }
            _ => unreachable!("agent lifecycle query emits fixed event types"),
        }
    }
    Ok(())
}

fn validate_context_lifecycle_events(
    connection: &Connection,
    run_id: Option<&RunId>,
) -> StoreResult<()> {
    let mut statement = connection.prepare(
        r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id
           FROM rebuild_events
           WHERE (?1 IS NULL OR run_id = ?1)
                 AND event_type IN (?2, ?3, ?4)
           ORDER BY event_id ASC"#,
    )?;
    let rows = statement.query_map(
        params![
            run_id.map(|value| value.0.as_str()),
            LifecycleEventType::ContextManifestCreated.as_str(),
            LifecycleEventType::ContextChildManifestCreated.as_str(),
            LifecycleEventType::ContextRepaired.as_str(),
        ],
        decode_lifecycle_row,
    )?;
    let mut seen = BTreeSet::<ArtifactId>::new();

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
        if !seen.insert(artifact_id.clone()) {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} repeats artifact {}",
                event_type.as_str(),
                artifact_id.0
            )));
        }
        let artifact = read_artifact(connection, &artifact_id)?;
        let expected_kind = if event_type == LifecycleEventType::ContextRepaired {
            ArtifactKind::ContextRepair
        } else {
            ArtifactKind::ContextManifest
        };
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
        if origin.run_id.as_ref() != Some(&event_run_id)
            || origin.task_id.as_ref() != Some(&task_id)
            || origin.attempt_id.as_ref() != Some(&attempt_id)
        {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} artifact origin does not match task attempt",
                event_type.as_str()
            )));
        }
        if event_type == LifecycleEventType::ContextChildManifestCreated {
            let parents = artifact
                .source_refs
                .iter()
                .filter(|reference| reference.kind == ArtifactKind::ContextManifest)
                .collect::<Vec<_>>();
            if parents.len() != 1 {
                return Err(StoreError::Integrity(format!(
                    "{} cursor {cursor} must reference exactly one parent context manifest",
                    event_type.as_str()
                )));
            }
            let parent = read_artifact(connection, &parents[0].artifact_id)?;
            let parent_origin = parent.origin.as_ref().ok_or_else(|| {
                StoreError::Integrity(format!(
                    "{} cursor {cursor} parent context manifest has no origin",
                    event_type.as_str()
                ))
            })?;
            if parent.kind != ArtifactKind::ContextManifest
                || parent_origin.run_id.as_ref() != Some(&event_run_id)
            {
                return Err(StoreError::Integrity(format!(
                    "{} cursor {cursor} parent context manifest is from another run",
                    event_type.as_str()
                )));
            }
        }
        if event_type == LifecycleEventType::ContextRepaired && artifact.source_refs.is_empty() {
            return Err(StoreError::Integrity(format!(
                "{} cursor {cursor} repair has no source refs",
                event_type.as_str()
            )));
        }
    }
    Ok(())
}
