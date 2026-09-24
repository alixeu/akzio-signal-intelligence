use super::*;

// Replay 读取 immutable graph/Contract installation 和事件 journal，重建历史状态并
// 与 Store snapshot 对照。它使用历史安装而不是今天的 active recipe，保留退休任务的
// 只读审计能力；Replay 通过不代表旧 Planner/ PaperDryRun 又能被领取或执行。
impl WorkflowRuntime {
    /// History is checked against its immutable installations, never today's active recipes.
    fn validate_historical_graph(&self, graph: &WorkflowGraph) -> RuntimeResult<()> {
        // 每个带 Contract 的历史 Node 必须和安装时的 purpose/retry/budget/failure 一致；
        // 当前配置变化不能重写旧 Run 的执行语义。
        graph.validate()?;
        for node in &graph.nodes {
            if let Some(hash) = &node.contract_hash {
                let stored = self
                    .store
                    .contract_installation(hash)?
                    .ok_or_else(|| StoreError::MissingContractInstallation(hash.clone()))?;
                let contract = stored.contract;
                contract.validate()?;
                if contract.purpose.as_str() != node.recipe_id.as_str()
                    || node.retry != contract.retry
                    || node.on_failure != contract.on_failure
                    || node.budget
                        != graph
                            .agent_budgets
                            .get(contract.purpose.as_str())
                            .cloned()
                            .unwrap_or(contract.budget)
                {
                    return Err(RuntimeError::NodeRecipeMismatch(node.task_id.clone()));
                }
            }
        }
        Ok(())
    }

    pub fn recover(&self, run_id: &RunId) -> RuntimeResult<WorkflowSnapshot> {
        let snapshot = self.store.recovery_snapshot(run_id)?;
        self.validate_historical_graph(&snapshot.revision.graph)?;
        Ok(snapshot)
    }

    pub fn replay_run(&self, run_id: &RunId) -> RuntimeResult<WorkflowSnapshot> {
        // reduce→revision→snapshot 三段都成功才返回；任一 cursor/status/task 差异都
        // 是 ReplayDiverged，而不是选择“看起来更新”的一方继续。
        let replay = self.reduce_history(run_id)?;
        self.validate_replay_revisions(run_id, &replay)?;
        let snapshot = self.store.workflow_snapshot(run_id)?;
        self.validate_replay_snapshot(run_id, &replay, &snapshot)?;
        self.validate_historical_graph(&snapshot.revision.graph)?;
        Ok(snapshot)
    }

    /// Replay an immutable graph revision through the event reducer and the
    /// current invariants. This never trusts a revision row by itself.
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
        // 分页读取只允许 cursor 单调前进；固定 PAGE_SIZE 保持内存有界且不会因分页
        // 重新排序丢掉跨页的 Task/Attempt 事件。
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
        // WorkflowCreated 只能出现一次；WorkflowPatched 只能追加 Task 或更新 pending
        // Task，不能删除/改写已运行节点，从而保留已产生 Artifact 的来源稳定性。
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
        // 事件的 AttemptId 必须同时匹配 Running 状态和 active_attempt；只看 task_id
        // 会让旧 lease 在恢复后继续写入，破坏 epoch fencing。
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
            self.validate_historical_graph(&reduced.graph)?;
        }
        Ok(())
    }

    pub(super) fn validate_replay_snapshot(
        &self,
        run_id: &RunId,
        replay: &ReplayedWorkflow,
        snapshot: &WorkflowSnapshot,
    ) -> RuntimeResult<()> {
        // 最后按 replayed task 状态推导 WorkflowStatus；Outcome worker 被排除在 T0
        // 终态计算之外，避免后续 T+1/T+3/T+5 任务把 T0 的完成时间线拉长或改写。
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
