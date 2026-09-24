// 文件导读：任务结束 helper 负责把 terminal event、Task/Attempt 状态、失败传播、Debug settle
// 放进调用方事务；它只收束已经通过 permit 检查的 Attempt，不决定模型或业务是否“成功”。
fn finish_permitted_task(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    requested_status: TaskStatus,
    on_failure: FailureDisposition,
    terminal_artifact_id: Option<&ArtifactId>,
    now: DateTime<Utc>,
) -> StoreResult<TaskStatus> {
    let post_terminal_worker = transaction.query_row(
        "SELECT recipe_id = ?1 FROM rebuild_tasks WHERE task_id = ?2",
        params![POST_TERMINAL_WORKER_RECIPE_ID, permit.task_id.0],
        |row| row.get::<_, bool>(0),
    )?;
    let status =
        if requested_status == TaskStatus::Failed && on_failure == FailureDisposition::SkipTask {
            TaskStatus::Skipped
        } else {
            requested_status
        };
    let terminal_event = match status {
        TaskStatus::Succeeded => LifecycleEventType::TaskSucceeded,
        TaskStatus::Failed => LifecycleEventType::TaskFailed,
        TaskStatus::Cancelled => LifecycleEventType::TaskCancelled,
        TaskStatus::Skipped => LifecycleEventType::TaskSkipped,
        _ => unreachable!("terminal status checked above"),
    };
    // Append the task terminal inside this transaction before lifecycle
    // validation.  The validator must see the terminal event itself so it can
    // reject a normal terminal status that leaves an AgentTurnStarted pending;
    // cancellation is the explicit abort-close for a crashed turn.
    append_event(
        transaction,
        &permit.run_id,
        Some(&permit.task_id),
        Some(&permit.attempt_id),
        terminal_event,
        terminal_artifact_id,
        now,
    )?;
    validate_tool_lifecycle_events(transaction, Some(&permit.run_id))?;
    validate_agent_turn_lifecycle_events(transaction, Some(&permit.run_id))?;
    validate_context_lifecycle_events(transaction, Some(&permit.run_id))?;
    validate_gate_lifecycle_events(transaction, Some(&permit.run_id))?;
    if status == TaskStatus::Succeeded {
        ensure_no_pending_tool_calls(
            transaction,
            &permit.run_id,
            &permit.task_id,
            &permit.attempt_id,
        )?;
    }
    transaction.execute(
        r#"UPDATE rebuild_tasks
           SET status = ?1, lease_id = NULL, active_attempt_id = NULL, worker_id = NULL,
               lease_until = NULL, finished_at = ?2
           WHERE task_id = ?3"#,
        params![enum_name(status), now.to_rfc3339(), permit.task_id.0],
    )?;
    transaction.execute(
        "UPDATE rebuild_attempts SET status = ?1, finished_at = ?2 WHERE attempt_id = ?3",
        params![enum_name(status), now.to_rfc3339(), permit.attempt_id.0],
    )?;
    if status == TaskStatus::Failed && !post_terminal_worker {
        match on_failure {
            FailureDisposition::FailRun => cancel_queued_tasks(transaction, &permit.run_id, now)?,
            FailureDisposition::FailTask => {
                cancel_failed_dependents(transaction, &permit.run_id, now)?
            }
            FailureDisposition::SkipTask => unreachable!("failed status is converted to skipped"),
        }
    }
    if !post_terminal_worker {
        refresh_run_status(transaction, &permit.run_id, now)?;
    }
    debug::settle_attempt(transaction, permit, &enum_name(status), now)?;
    Ok(status)
}

// FailRun 时只取消 queued Task，running Attempt 仍由其 permit/lease 自己收束。
fn cancel_queued_tasks(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let task_ids = {
        let mut statement = transaction.prepare(
            "SELECT task_id FROM rebuild_tasks WHERE run_id = ?1 AND status = 'queued' ORDER BY task_id",
        )?;
        let rows = statement
            .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
            .map(|row| row.map(TaskId))
            .collect::<Result<Vec<_>, _>>()?;
        rows
    };
    for task_id in task_ids {
        let changed = transaction.execute(
            "UPDATE rebuild_tasks SET status = 'cancelled', finished_at = ?1 WHERE task_id = ?2 AND status = 'queued'",
            params![now.to_rfc3339(), task_id.0],
        )?;
        if changed == 1 {
            append_event(
                transaction,
                run_id,
                Some(&task_id),
                None,
                LifecycleEventType::TaskCancelled,
                None,
                now,
            )?;
        }
    }
    Ok(())
}

// FailTask 逐轮传播 failed/cancelled parent，直到没有新的 queued dependent。
fn cancel_failed_dependents(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    loop {
        let task_ids = {
            let mut statement = transaction.prepare(
                r#"SELECT DISTINCT child.task_id
                   FROM rebuild_tasks AS child
                   JOIN rebuild_task_dependencies AS dependency
                     ON dependency.task_id = child.task_id
                   JOIN rebuild_tasks AS parent
                     ON parent.task_id = dependency.depends_on_task_id
                   WHERE child.run_id = ?1
                     AND child.status = 'queued'
                     AND parent.status IN ('failed', 'cancelled')
                   ORDER BY child.task_id"#,
            )?;
            let rows = statement
                .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
                .map(|row| row.map(TaskId))
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        if task_ids.is_empty() {
            return Ok(());
        }
        for task_id in task_ids {
            let changed = transaction.execute(
                "UPDATE rebuild_tasks SET status = 'cancelled', finished_at = ?1 WHERE task_id = ?2 AND status = 'queued'",
                params![now.to_rfc3339(), task_id.0],
            )?;
            if changed == 1 {
                append_event(
                    transaction,
                    run_id,
                    Some(&task_id),
                    None,
                    LifecycleEventType::TaskCancelled,
                    None,
                    now,
                )?;
            }
        }
    }
}

// 统一校验事件列形状后插入自增 cursor，并在同一事务中更新 RunControl checkpoint boundary。
fn append_event(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    task_id: Option<&TaskId>,
    attempt_id: Option<&akzio_domain::AttemptId>,
    event_type: LifecycleEventType,
    artifact_id: Option<&ArtifactId>,
    created_at: DateTime<Utc>,
) -> StoreResult<i64> {
    validate_event_shape(
        event_type,
        task_id.is_some(),
        attempt_id.is_some(),
        artifact_id.is_some(),
    )?;
    transaction.execute(
        r#"INSERT INTO rebuild_events
           (run_id, task_id, attempt_id, event_type, artifact_id, created_at)
           VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
        params![
            run_id.0,
            task_id.map(|id| id.0.as_str()),
            attempt_id.map(|id| id.0.as_str()),
            event_type.as_str(),
            artifact_id.map(|id| id.0.as_str()),
            created_at.to_rfc3339(),
        ],
    )?;
    let cursor = transaction.last_insert_rowid();
    run_control::checkpoint_boundary(transaction, run_id, cursor, event_type, task_id, attempt_id, artifact_id, created_at)?;
    Ok(cursor)
}

// 只允许无 Artifact 的 started/abandoned Attempt 事实通过专用入口写入。
fn append_task_event(
    transaction: &Transaction<'_>,
    permit: &TaskWritePermit,
    event_type: LifecycleEventType,
    created_at: DateTime<Utc>,
) -> StoreResult<i64> {
    // Only artifact-less attempt facts may enter through this door; anything
    // that carries a payload must go through an artifact write.
    if !matches!(
        event_type,
        LifecycleEventType::AgentTurnStarted | LifecycleEventType::SupplementalRoundAbandoned
    ) {
        return Err(StoreError::InvalidLifecycleEventShape {
            event_type: event_type.as_str().to_owned(),
        });
    }
    append_event(
        transaction,
        &permit.run_id,
        Some(&permit.task_id),
        Some(&permit.attempt_id),
        event_type,
        None,
        created_at,
    )
}

// 用事件类型定义 task/attempt/artifact 三列的允许组合，拒绝半成品 lineage。
fn validate_event_shape(
    event_type: LifecycleEventType,
    has_task_id: bool,
    has_attempt_id: bool,
    has_artifact_id: bool,
) -> StoreResult<()> {
    let effect_event = matches!(
        event_type,
        LifecycleEventType::ExecutionEffectIntent
            | LifecycleEventType::ExecutionEffectRecovered
            | LifecycleEventType::ExecutionEffectSettled
    );
    if effect_event && !(has_task_id && has_attempt_id && has_artifact_id) {
        return Err(StoreError::InvalidLifecycleEventShape {
            event_type: event_type.as_str().to_owned(),
        });
    }
    if has_attempt_id && !has_task_id {
        return Err(StoreError::Domain(DomainError::AttemptOriginWithoutTask));
    }

    let valid = match event_type {
        LifecycleEventType::RunCheckpointSaved | LifecycleEventType::DebugControlChanged => !has_task_id && !has_attempt_id && has_artifact_id,
        LifecycleEventType::StageAcceptanceRecorded => has_task_id && has_attempt_id && has_artifact_id,
        LifecycleEventType::DebugBudgetObserved => has_task_id && has_attempt_id && has_artifact_id,
        LifecycleEventType::WorkflowCreated => !has_task_id && !has_attempt_id && has_artifact_id,
        LifecycleEventType::SchedulerSnapshotNeedCreated
        | LifecycleEventType::SchedulerWorkflowProposalCreated => {
            !has_task_id && !has_attempt_id && has_artifact_id
        }
        LifecycleEventType::RunCancelRequested => {
            !has_task_id && !has_attempt_id && !has_artifact_id
        }
        LifecycleEventType::OutcomeWorkerEnqueued => {
            has_task_id && !has_attempt_id && has_artifact_id
        }
        LifecycleEventType::TaskCancelled => has_task_id && (!has_artifact_id || has_attempt_id),
        LifecycleEventType::TaskStarted
        | LifecycleEventType::AgentTurnStarted
        | LifecycleEventType::SupplementalRoundAbandoned
        | LifecycleEventType::TaskDeferred
        | LifecycleEventType::TaskRecovered
        | LifecycleEventType::TaskRecoveryExhausted
        | LifecycleEventType::TaskRetryExhausted
        | LifecycleEventType::TaskRetryScheduled => {
            has_task_id && has_attempt_id && !has_artifact_id
        }
        LifecycleEventType::TaskFailed
        | LifecycleEventType::TaskSkipped
        | LifecycleEventType::TaskSucceeded => has_task_id && has_attempt_id,
        _ => has_task_id && has_attempt_id && has_artifact_id,
    };

    if !valid {
        return Err(StoreError::InvalidLifecycleEventShape {
            event_type: event_type.as_str().to_owned(),
        });
    }

    Ok(())
}
