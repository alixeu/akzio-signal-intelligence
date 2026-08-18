use super::*;

impl WorkflowRuntime {
    pub fn recover(&self, run_id: &RunId) -> RuntimeResult<WorkflowSnapshot> {
        let snapshot = self.store.workflow_snapshot(run_id)?;
        self.validate_compiled_graph(snapshot.run.purpose, &snapshot.revision.graph)?;
        Ok(snapshot)
    }

    /// Reconstruct a Run from its append-only event stream and durable task
    /// history, rejecting a snapshot that cannot be derived from that history.
    /// Create a fresh noncanonical rerun. Paper, Shadow and Replay workloads
    /// have distinct owner flows and can never be retried through HTTP.
    pub fn retry_run(&self, source_run_id: &RunId, now: DateTime<Utc>) -> RuntimeResult<RunId> {
        let source = self.replay_run(source_run_id)?;
        if !matches!(
            source.status,
            WorkflowStatus::Completed
                | WorkflowStatus::CompletedWithExecutionRejection
                | WorkflowStatus::Failed
                | WorkflowStatus::Cancelled
        ) {
            return Err(RuntimeError::RetryRunNotTerminal(source_run_id.clone()));
        }
        if !matches!(
            source.run.purpose,
            RunPurpose::Debug | RunPurpose::PaperDryRun
        ) {
            return Err(RuntimeError::RetryPurpose(source.run.purpose));
        }
        let run_id = RunId::new();
        let graph = self.bootstrap(source.run.purpose, source.run.topology_id.clone())?;
        self.submit(run_id.clone(), source.run.purpose, graph, now)?;
        Ok(run_id)
    }

    pub fn replay_run(&self, run_id: &RunId) -> RuntimeResult<WorkflowSnapshot> {
        let replay = self.reduce_history(run_id)?;
        self.validate_replay_revisions(run_id, &replay)?;
        let snapshot = self.store.workflow_snapshot(run_id)?;
        self.validate_replay_snapshot(run_id, &replay, &snapshot)?;
        self.validate_compiled_graph(snapshot.run.purpose, &snapshot.revision.graph)?;
        Ok(snapshot)
    }

    /// Replay an immutable graph revision through the event reducer and the
    /// current v2 invariants. This never trusts a revision row by itself.
    pub fn replay_revision(
        &self,
        run_id: &RunId,
        revision: u64,
    ) -> RuntimeResult<WorkflowRevision> {
        let replay = self.reduce_history(run_id)?;
        self.validate_replay_revisions(run_id, &replay)?;
        let reduced = replay.revisions.get(revision as usize).ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("missing graph revision {revision} in event stream"),
            )
        })?;
        let durable = self.store.workflow_revision(run_id, revision)?;
        if durable.graph_artifact != reduced.graph_artifact
            || durable.graph != reduced.graph
            || durable.created_at != reduced.created_at
        {
            return Err(Self::replay_error(
                run_id,
                format!("revision {revision} differs from its workflow event"),
            ));
        }
        self.validate_compiled_graph(self.store.run_purpose(run_id)?, &durable.graph)?;
        Ok(durable)
    }

    pub(super) fn reduce_history(&self, run_id: &RunId) -> RuntimeResult<ReplayedWorkflow> {
        let events = self.replay_events(run_id)?;
        let mut replay = ReplayedWorkflow::default();
        for event in &events {
            self.reduce_event(run_id, &mut replay, event)?;
        }
        if replay.revisions.is_empty() {
            return Err(Self::replay_error(
                run_id,
                "workflow.created is missing from durable event history",
            ));
        }
        Ok(replay)
    }

    pub(super) fn replay_events(&self, run_id: &RunId) -> RuntimeResult<Vec<StoredEvent>> {
        const PAGE_SIZE: usize = 256;

        let mut events = Vec::new();
        let mut after = 0;
        loop {
            let page = self.store.events_after(run_id, after, PAGE_SIZE)?;
            let Some(last) = page.last() else {
                break;
            };
            if last.cursor <= after {
                return Err(Self::replay_error(
                    run_id,
                    "event cursor did not advance while paging history",
                ));
            }
            after = last.cursor;
            events.extend(page);
        }
        Ok(events)
    }

    pub(super) fn reduce_graph_event(
        &self,
        run_id: &RunId,
        replay: &mut ReplayedWorkflow,
        event: &StoredEvent,
        initial: bool,
    ) -> RuntimeResult<()> {
        if initial && (event.task_id.is_some() || event.attempt_id.is_some()) {
            return Err(Self::replay_error(
                run_id,
                format!("{} unexpectedly names a task attempt", event.event_type),
            ));
        }
        if !initial && (event.task_id.is_some() || event.attempt_id.is_some()) {
            let task = Self::replay_task_mut(run_id, replay, event)?;
            Self::assert_active_attempt(run_id, task, event)?;
        }
        if initial != replay.revisions.is_empty() {
            return Err(Self::replay_error(
                run_id,
                format!("{} appears out of graph revision order", event.event_type),
            ));
        }
        let artifact_id = event.artifact_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its graph artifact", event.event_type),
            )
        })?;
        let graph_artifact = self.store.artifact(artifact_id)?;
        if graph_artifact.kind != ArtifactKind::WorkflowGraph {
            return Err(Self::replay_error(
                run_id,
                format!(
                    "{} references a non-workflow graph artifact",
                    event.event_type
                ),
            ));
        }
        let graph: WorkflowGraph =
            serde_json::from_slice(&self.store.read_blob(&graph_artifact.blob)?)?;
        graph.validate()?;

        if let Some(previous) = replay.revisions.last() {
            if previous.graph.topology_id != graph.topology_id {
                return Err(Self::replay_error(
                    run_id,
                    "workflow.patched changed the topology id",
                ));
            }
            let next_ids = graph
                .nodes
                .iter()
                .map(|node| node.task_id.clone())
                .collect::<BTreeSet<_>>();
            for node in &previous.graph.nodes {
                if !next_ids.contains(&node.task_id) {
                    return Err(Self::replay_error(
                        run_id,
                        format!("workflow.patched removed task {}", node.task_id),
                    ));
                }
            }
        }

        for node in &graph.nodes {
            match replay.tasks.get_mut(&node.task_id) {
                Some(task) => {
                    if task.status != TaskStatus::Pending && task.node != *node {
                        return Err(Self::replay_error(
                            run_id,
                            format!("workflow.patched rewrote non-pending task {}", node.task_id),
                        ));
                    }
                    task.node = node.clone();
                }
                None => {
                    replay.tasks.insert(
                        node.task_id.clone(),
                        ReplayedTask {
                            node: node.clone(),
                            status: TaskStatus::Pending,
                            active_attempt_id: None,
                            attempt_count: 0,
                            finished_at: None,
                        },
                    );
                }
            }
        }
        replay.revisions.push(ReplayedWorkflowRevision {
            cursor: event.cursor,
            graph_artifact,
            graph,
            created_at: event.created_at,
        });
        Ok(())
    }

    pub(super) fn replay_task_mut<'a>(
        run_id: &RunId,
        replay: &'a mut ReplayedWorkflow,
        event: &StoredEvent,
    ) -> RuntimeResult<&'a mut ReplayedTask> {
        let task_id = event.task_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its task id", event.event_type),
            )
        })?;
        replay.tasks.get_mut(task_id).ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} references unknown task {task_id}", event.event_type),
            )
        })
    }

    pub(super) fn assert_active_attempt(
        run_id: &RunId,
        task: &ReplayedTask,
        event: &StoredEvent,
    ) -> RuntimeResult<()> {
        let attempt_id = event.attempt_id.as_ref().ok_or_else(|| {
            Self::replay_error(
                run_id,
                format!("{} is missing its attempt id", event.event_type),
            )
        })?;
        if task.status != TaskStatus::Running || task.active_attempt_id.as_ref() != Some(attempt_id)
        {
            return Err(Self::replay_error(
                run_id,
                format!(
                    "{} does not match task {} active attempt",
                    event.event_type, task.node.task_id
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn validate_replay_revisions(
        &self,
        run_id: &RunId,
        replay: &ReplayedWorkflow,
    ) -> RuntimeResult<()> {
        let purpose = self.store.run_purpose(run_id)?;
        for (index, reduced) in replay.revisions.iter().enumerate() {
            let revision = u64::try_from(index).map_err(|_| {
                Self::replay_error(run_id, "workflow revision index does not fit u64")
            })?;
            let durable = self.store.workflow_revision(run_id, revision)?;
            if durable.graph_artifact != reduced.graph_artifact
                || durable.graph != reduced.graph
                || durable.created_at != reduced.created_at
            {
                return Err(Self::replay_error(
                    run_id,
                    format!("revision {revision} differs from event history"),
                ));
            }
            self.validate_compiled_graph(purpose, &reduced.graph)?;
        }
        Ok(())
    }

    pub(super) fn validate_replay_snapshot(
        &self,
        run_id: &RunId,
        replay: &ReplayedWorkflow,
        snapshot: &WorkflowSnapshot,
    ) -> RuntimeResult<()> {
        let latest = replay.revisions.last().ok_or_else(|| {
            Self::replay_error(run_id, "workflow snapshot has no reduced graph revision")
        })?;
        let expected_revision = u64::try_from(replay.revisions.len() - 1)
            .map_err(|_| Self::replay_error(run_id, "workflow revision count does not fit u64"))?;
        if snapshot.revision.revision != expected_revision
            || snapshot.revision.graph_artifact != latest.graph_artifact
            || snapshot.revision.graph != latest.graph
            || snapshot.revision.created_at != latest.created_at
        {
            return Err(Self::replay_error(
                run_id,
                "latest workflow snapshot differs from reduced graph history",
            ));
        }
        if snapshot.event_cursor != replay.event_cursor {
            return Err(Self::replay_error(
                run_id,
                "workflow snapshot event cursor differs from reduced history",
            ));
        }
        if snapshot.cancel_requested != replay.cancel_requested {
            return Err(Self::replay_error(
                run_id,
                "workflow cancellation marker differs from reduced history",
            ));
        }

        let stored_tasks = snapshot
            .tasks
            .iter()
            .map(|task| (task.node.task_id.clone(), task))
            .collect::<BTreeMap<_, _>>();
        if stored_tasks.len() != replay.tasks.len() {
            return Err(Self::replay_error(
                run_id,
                "workflow task count differs from reduced graph history",
            ));
        }
        for (task_id, reduced) in &replay.tasks {
            let stored = stored_tasks.get(task_id).ok_or_else(|| {
                Self::replay_error(run_id, format!("snapshot is missing task {task_id}"))
            })?;
            let stored_attempt = stored
                .active_attempt
                .as_ref()
                .map(|attempt| attempt.permit.attempt_id.clone());
            if stored.node != reduced.node
                || stored.status != reduced.status
                || stored.attempt_count != reduced.attempt_count
                || stored_attempt != reduced.active_attempt_id
                || stored.finished_at != reduced.finished_at
            {
                return Err(Self::replay_error(
                    run_id,
                    format!("task {task_id} differs from reduced event/task history"),
                ));
            }
        }

        let workflow_tasks = replay
            .tasks
            .values()
            .filter(|task| task.node.recipe_id.as_str() != POST_TERMINAL_WORKER_RECIPE_ID)
            .collect::<Vec<_>>();
        let expected_status = if workflow_tasks
            .iter()
            .any(|task| matches!(task.status, TaskStatus::Pending | TaskStatus::Running))
        {
            if replay.saw_task_start {
                WorkflowStatus::Running
            } else {
                WorkflowStatus::Queued
            }
        } else if workflow_tasks
            .iter()
            .any(|task| task.status == TaskStatus::Failed)
        {
            WorkflowStatus::Failed
        } else if workflow_tasks
            .iter()
            .all(|task| task.status == TaskStatus::Cancelled)
        {
            WorkflowStatus::Cancelled
        } else {
            WorkflowStatus::Completed
        };
        if snapshot.status != expected_status
            || (expected_status == WorkflowStatus::Running && snapshot.finished_at.is_some())
            || (expected_status == WorkflowStatus::Queued && snapshot.finished_at.is_some())
            || (matches!(
                expected_status,
                WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
            ) && snapshot.finished_at.is_none())
        {
            return Err(Self::replay_error(
                run_id,
                "workflow status differs from reduced task history",
            ));
        }
        Ok(())
    }

    pub(super) fn replay_error(run_id: &RunId, reason: impl Into<String>) -> RuntimeError {
        RuntimeError::ReplayDiverged {
            run_id: run_id.clone(),
            reason: reason.into(),
        }
    }
}
