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
            LifecycleEventType::RunCheckpointSaved => {
                self.store.validate_checkpoint_event(event)?;
            }
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
            LifecycleEventType::AgentTurnStarted
            | LifecycleEventType::SupplementalRoundAbandoned
            | LifecycleEventType::TaskRetryExhausted
            | LifecycleEventType::TaskRecoveryExhausted => {
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
            LifecycleEventType::SchedulerSnapshotNeedCreated
            | LifecycleEventType::SchedulerWorkflowProposalCreated => {
                self.reduce_session_setup_event(run_id, event)?;
            }
            LifecycleEventType::DebugControlChanged
            | LifecycleEventType::DebugBudgetObserved
            | LifecycleEventType::StageAcceptanceRecorded => {
                self.reduce_debug_record_event(run_id, replay, event, event_type)?;
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

    /// Debug records describe Rust control and observations, not contract outputs.
    /// Acceptance also records canonical submission rejections needed by retries;
    /// unlike debug controls and budgets, it does not require a DebugSession.
    /// Acceptance may follow a terminal attempt; only budget observations require
    /// the attempt to still be active at this point in the event stream.
    fn reduce_debug_record_event(
        &self,
        run_id: &RunId,
        replay: &ReplayedWorkflow,
        event: &StoredEvent,
        event_type: LifecycleEventType,
    ) -> RuntimeResult<()> {
        use akzio_domain::{DebugSession, DebugSessionIdentity, StageAcceptance};

        let invalid =
            || Self::replay_error(run_id, format!("invalid {} DebugRecord", event.event_type));
        let artifact = self
            .store
            .artifact(event.artifact_id.as_ref().ok_or_else(invalid)?)?;
        Self::validate_debug_record_envelope(run_id, event, &artifact)?;
        let session = self.store.debug_session(run_id)?;
        let expected_refs: BTreeSet<ArtifactRef> = match (event_type, artifact.producer.as_str()) {
            (LifecycleEventType::DebugControlChanged, "debug.session_identity") => {
                let session = session.as_ref().ok_or_else(invalid)?;
                if event.task_id.is_some() || event.attempt_id.is_some() {
                    return Err(invalid());
                }
                let identity: DebugSessionIdentity =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                if identity != session.identity || identity.created_at != event.created_at {
                    return Err(invalid());
                }
                identity
                    .dataset
                    .into_iter()
                    .chain(identity.parent_artifacts)
                    .collect()
            }
            (LifecycleEventType::DebugControlChanged, "debug.control") => {
                let session = session.as_ref().ok_or_else(invalid)?;
                if event.task_id.is_some() || event.attempt_id.is_some() {
                    return Err(invalid());
                }
                let control: DebugSession =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                if control.identity != session.identity
                    || control.revision == 0
                    || control.revision > session.revision
                    || control.updated_at != event.created_at
                    || control
                        .permitted_task_id
                        .iter()
                        .chain(control.paused_at_task_id.iter())
                        .any(|task| !replay.tasks.contains_key(task))
                {
                    return Err(invalid());
                }
                BTreeSet::new()
            }
            (LifecycleEventType::DebugBudgetObserved, "debug.budget_snapshot") => {
                session.as_ref().ok_or_else(invalid)?;
                let task_id = event.task_id.as_ref().ok_or_else(invalid)?;
                let task = replay.tasks.get(task_id).ok_or_else(invalid)?;
                Self::assert_active_attempt(run_id, task, event)?;
                let _: serde_json::Value =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                BTreeSet::new()
            }
            (LifecycleEventType::StageAcceptanceRecorded, "debug.stage_acceptance") => {
                let acceptance: StageAcceptance =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                if acceptance.version != 1
                    || acceptance.stage.is_empty()
                    || acceptance
                        .checks
                        .iter()
                        .any(|check| check.check_id.is_empty())
                    || acceptance.run_id != *run_id
                    || event.task_id.as_ref() != Some(&acceptance.task_id)
                    || event.attempt_id.as_ref() != Some(&acceptance.attempt_id)
                    || acceptance.created_at != event.created_at
                    || !replay.tasks.contains_key(&acceptance.task_id)
                    || !self
                        .store
                        .attempt_events(run_id, &acceptance.task_id, &acceptance.attempt_id)?
                        .iter()
                        .any(|prior| {
                            prior.cursor < event.cursor
                                && prior.event_type == LifecycleEventType::TaskStarted.as_str()
                        })
                {
                    return Err(invalid());
                }
                acceptance
                    .checks
                    .into_iter()
                    .flat_map(|check| check.evidence_refs)
                    .collect()
            }
            _ => return Err(invalid()),
        };
        if artifact
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected_refs
        {
            return Err(invalid());
        }
        for reference in &artifact.source_refs {
            if self.store.artifact(&reference.artifact_id)?.kind != reference.kind {
                return Err(invalid());
            }
        }
        Ok(())
    }

    fn validate_debug_record_envelope(
        run_id: &RunId,
        event: &StoredEvent,
        artifact: &Artifact,
    ) -> RuntimeResult<()> {
        artifact.validate()?;
        if artifact.kind != ArtifactKind::DebugRecord
            || artifact.lifecycle != ArtifactLifecycle::RunScoped
            || artifact.provenance.source_family != "akzio.debug"
            || artifact.provenance.producer_contract_hash.is_some()
            || artifact.created_at != event.created_at
            || artifact.origin.as_ref()
                != Some(&ArtifactOrigin {
                    run_id: Some(run_id.clone()),
                    task_id: event.task_id.clone(),
                    attempt_id: event.attempt_id.clone(),
                    contract_hash: None,
                })
        {
            return Err(Self::replay_error(run_id, "invalid DebugRecord envelope"));
        }
        Ok(())
    }

    /// The scheduler freezes a session's `EvidenceNeed`s and `WorkflowProposal`
    /// while it reserves the slot, before any task row exists, so these facts
    /// are run-level and touch no replayed task state. Replay still checks the
    /// lineage so the event cannot smuggle in a foreign artifact.
    fn reduce_session_setup_event(&self, run_id: &RunId, event: &StoredEvent) -> RuntimeResult<()> {
        if event.task_id.is_some() || event.attempt_id.is_some() {
            return Err(Self::replay_error(
                run_id,
                format!("{} unexpectedly names a task attempt", event.event_type),
            ));
        }
        let artifact_id = event.artifact_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its artifact id", event.event_type),
            )
        })?;
        let artifact = self.store.artifact(artifact_id)?;
        artifact.validate()?;
        if !matches!(
            artifact.kind,
            ArtifactKind::EvidenceNeed | ArtifactKind::WorkflowProposal
        ) || artifact.lifecycle != ArtifactLifecycle::RunScoped
            || artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(Self::replay_error(
                run_id,
                format!("{} references a foreign artifact", event.event_type),
            ));
        }
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
            ArtifactKind::ExecutionCommitment
                | ArtifactKind::ExecutionCancel
                | ArtifactKind::ExecutionReprice
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_record_envelope_rejects_foreign_origin_and_source() {
        let run_id = RunId::new();
        let now = Utc::now();
        let make = |origin_run: RunId, source: &str| {
            Artifact::new(
                ArtifactKind::DebugRecord,
                akzio_domain::BlobRef {
                    hash: ContentHash::of_bytes(b"{}"),
                    media_type: "application/json".into(),
                    bytes: 2,
                },
                "debug.control",
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: source.into(),
                    observed_at: None,
                    retrieved_at: now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: None,
                },
                Some(ArtifactOrigin {
                    run_id: Some(origin_run),
                    task_id: None,
                    attempt_id: None,
                    contract_hash: None,
                }),
                vec![],
                now,
            )
            .unwrap()
        };
        let valid = make(run_id.clone(), "akzio.debug");
        let event = StoredEvent {
            cursor: 1,
            run_id: run_id.clone(),
            task_id: None,
            attempt_id: None,
            event_type: LifecycleEventType::DebugControlChanged.as_str().into(),
            artifact_id: Some(valid.artifact_id.clone()),
            created_at: now,
        };
        WorkflowRuntime::validate_debug_record_envelope(&run_id, &event, &valid).unwrap();
        for invalid in [
            make(RunId::new(), "akzio.debug"),
            make(run_id.clone(), "model"),
        ] {
            assert!(matches!(
                WorkflowRuntime::validate_debug_record_envelope(&run_id, &event, &invalid),
                Err(RuntimeError::ReplayDiverged { .. })
            ));
        }
        let mut wrong_event = event;
        wrong_event.task_id = Some(TaskId::new());
        assert!(
            WorkflowRuntime::validate_debug_record_envelope(&run_id, &wrong_event, &valid).is_err()
        );
    }
}
