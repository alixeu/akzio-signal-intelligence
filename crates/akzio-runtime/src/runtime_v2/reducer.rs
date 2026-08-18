use super::*;

impl WorkflowRuntime {
    pub(super) fn reduce_event(
        &self,
        run_id: &RunId,
        replay: &mut ReplayedWorkflow,
        event: &StoredEvent,
    ) -> RuntimeResult<()> {
        if event.run_id != *run_id {
            return Err(Self::replay_error(
                run_id,
                format!("event {} belongs to another run", event.cursor),
            ));
        }
        let event_type = event.lifecycle_kind().map_err(|error| {
            Self::replay_error(
                run_id,
                format!("invalid lifecycle event {}: {error}", event.cursor),
            )
        })?;
        match event_type {
            LifecycleEventType::WorkflowCreated => {
                self.reduce_graph_event(run_id, replay, event, true)?
            }
            LifecycleEventType::WorkflowPatched => {
                self.reduce_graph_event(run_id, replay, event, false)?
            }
            LifecycleEventType::OutcomeWorkerEnqueued => {
                if event.attempt_id.is_some() {
                    return Err(Self::replay_error(
                        run_id,
                        "outcome.worker.enqueued must not carry an attempt id",
                    ));
                }
                let task_id = event.task_id.as_ref().ok_or_else(|| {
                    Self::replay_error(run_id, "outcome.worker.enqueued is missing its task id")
                })?;
                let schedule_id = event.artifact_id.as_ref().ok_or_else(|| {
                    Self::replay_error(
                        run_id,
                        "outcome.worker.enqueued is missing its schedule artifact",
                    )
                })?;
                let schedule = self.store.artifact(schedule_id)?;
                if schedule.kind != ArtifactKind::OutcomeSchedule {
                    return Err(Self::replay_error(
                        run_id,
                        "outcome.worker.enqueued does not reference an OutcomeSchedule",
                    ));
                }
                let snapshot = self.store.workflow_snapshot(run_id)?;
                let stored = snapshot
                    .tasks
                    .into_iter()
                    .find(|task| &task.node.task_id == task_id)
                    .ok_or_else(|| {
                        Self::replay_error(
                            run_id,
                            format!("outcome.worker.enqueued references unknown task {task_id}"),
                        )
                    })?;
                if stored.node.recipe_id.as_str() != POST_TERMINAL_WORKER_RECIPE_ID
                    || !stored.node.input_artifacts.iter().any(|reference| {
                        reference.artifact_id == *schedule_id
                            && reference.kind == ArtifactKind::OutcomeSchedule
                    })
                    || replay.tasks.contains_key(task_id)
                {
                    return Err(Self::replay_error(
                        run_id,
                        "outcome.worker.enqueued task metadata is invalid",
                    ));
                }
                replay.tasks.insert(
                    task_id.clone(),
                    ReplayedTask {
                        node: stored.node,
                        status: TaskStatus::Pending,
                        active_attempt_id: None,
                        attempt_count: 0,
                        finished_at: None,
                    },
                );
            }
            LifecycleEventType::ExecutionEffectIntent
            | LifecycleEventType::ExecutionEffectRecovered
            | LifecycleEventType::ExecutionEffectSettled => {
                self.reduce_execution_effect_event(run_id, replay, event)?;
            }
            LifecycleEventType::TaskStarted => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                let attempt_id = event.attempt_id.clone().ok_or_else(|| {
                    Self::replay_error(run_id, "task.started is missing its attempt id")
                })?;
                if task.status != TaskStatus::Pending || task.active_attempt_id.is_some() {
                    return Err(Self::replay_error(
                        run_id,
                        format!(
                            "task {} started from a non-pending state",
                            task.node.task_id
                        ),
                    ));
                }
                task.status = TaskStatus::Running;
                task.active_attempt_id = Some(attempt_id);
                task.attempt_count += 1;
                task.finished_at = None;
                replay.saw_task_start = true;
            }
            LifecycleEventType::AgentTurnStarted => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                Self::assert_active_attempt(run_id, task, event)?;
            }
            LifecycleEventType::TaskDeferred
            | LifecycleEventType::TaskRetryScheduled
            | LifecycleEventType::TaskRecovered => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                Self::assert_active_attempt(run_id, task, event)?;
                task.status = TaskStatus::Pending;
                task.active_attempt_id = None;
                task.finished_at = None;
            }
            LifecycleEventType::TaskSucceeded
            | LifecycleEventType::TaskFailed
            | LifecycleEventType::TaskSkipped => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                Self::assert_active_attempt(run_id, task, event)?;
                task.status = match event_type {
                    LifecycleEventType::TaskSucceeded => TaskStatus::Succeeded,
                    LifecycleEventType::TaskFailed => TaskStatus::Failed,
                    LifecycleEventType::TaskSkipped => TaskStatus::Skipped,
                    _ => unreachable!("matched terminal task event"),
                };
                task.active_attempt_id = None;
                task.finished_at = Some(event.created_at);
            }
            LifecycleEventType::TaskCancelled => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                if event.attempt_id.is_some() {
                    Self::assert_active_attempt(run_id, task, event)?;
                } else if task.status != TaskStatus::Pending {
                    return Err(Self::replay_error(
                        run_id,
                        format!(
                            "queued cancellation for non-pending task {}",
                            task.node.task_id
                        ),
                    ));
                }
                task.status = TaskStatus::Cancelled;
                task.active_attempt_id = None;
                task.finished_at = Some(event.created_at);
            }
            LifecycleEventType::TaskRetryExhausted | LifecycleEventType::TaskRecoveryExhausted => {
                let task = Self::replay_task_mut(run_id, replay, event)?;
                Self::assert_active_attempt(run_id, task, event)?;
            }
            LifecycleEventType::RunCancelRequested => {
                if event.task_id.is_some() || event.attempt_id.is_some() {
                    return Err(Self::replay_error(
                        run_id,
                        "run.cancel_requested unexpectedly names a task attempt",
                    ));
                }
                if replay.cancel_requested {
                    return Err(Self::replay_error(
                        run_id,
                        "run.cancel_requested appears more than once",
                    ));
                }
                replay.cancel_requested = true;
            }
            _ if event.artifact_id.is_some() => {
                self.reduce_artifact_trace_event(run_id, replay, event)?;
            }
            _ => {
                return Err(Self::replay_error(
                    run_id,
                    format!("unhandled durable event type {}", event.event_type),
                ));
            }
        }
        replay.event_cursor = event.cursor;
        Ok(())
    }

    fn reduce_execution_effect_event(
        &self,
        run_id: &RunId,
        replay: &mut ReplayedWorkflow,
        event: &StoredEvent,
    ) -> RuntimeResult<()> {
        let task = Self::replay_task_mut(run_id, replay, event)?;
        Self::assert_active_attempt(run_id, task, event)?;
        let artifact_id = event.artifact_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its effect", event.event_type),
            )
        })?;
        let artifact = self.store.artifact(artifact_id)?;
        artifact.validate()?;
        if !matches!(
            artifact.kind,
            ArtifactKind::ExecutionCommitment | ArtifactKind::ExecutionReprice
        ) || artifact.lifecycle != ArtifactLifecycle::Canonical
            || artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(Self::replay_error(
                run_id,
                format!("{} references an invalid Paper effect", event.event_type),
            ));
        }
        Ok(())
    }

    /// Artifact-bearing events are intentionally extensible: task runtimes
    /// emit domain-specific trace events through `write_task_artifact`. Their
    /// authority is the artifact origin, never an event-type allowlist.
    fn reduce_artifact_trace_event(
        &self,
        run_id: &RunId,
        replay: &ReplayedWorkflow,
        event: &StoredEvent,
    ) -> RuntimeResult<()> {
        if event.event_type.trim().is_empty() {
            return Err(Self::replay_error(
                run_id,
                "artifact trace event has an empty event type",
            ));
        }
        let task_id = event.task_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its task id", event.event_type),
            )
        })?;
        let attempt_id = event.attempt_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its attempt id", event.event_type),
            )
        })?;
        let artifact_id = event.artifact_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its artifact id", event.event_type),
            )
        })?;
        let task = replay.tasks.get(task_id).ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} references unknown task {task_id}", event.event_type),
            )
        })?;
        Self::assert_active_attempt(run_id, task, event)?;
        let artifact = self.store.artifact(artifact_id)?;
        artifact.validate()?;
        let origin = artifact.origin.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} artifact has no task origin", event.event_type),
            )
        })?;
        if origin.run_id.as_ref() != Some(run_id)
            || origin.task_id.as_ref() != Some(task_id)
            || origin.attempt_id.as_ref() != Some(attempt_id)
            || origin.contract_hash.as_ref() != task.node.contract_hash.as_ref()
        {
            return Err(Self::replay_error(
                run_id,
                format!(
                    "{} artifact origin does not match task attempt",
                    event.event_type
                ),
            ));
        }
        Ok(())
    }
}
