// 文件导读：Trajectory 只从事件日志和 AgentTurn/Tool Artifact 生成脱敏投影；
// unmatched dispatch 保留为 unknown，不能从缺失 terminal 事件推断 provider 失败或零成本。
use super::*;

/// Pair the serialized dispatch lifecycle without inventing a call identity for
/// starts that have no artifact. This is a read-only projection shared with the
/// diagnostic exporter; retries close an attempt, not the unknown provider call.
#[derive(Default)]
pub(super) struct AgentTurnPairing {
    pending: BTreeMap<(RunId, Option<TaskId>, Option<AttemptId>), StoredEvent>,
    unmatched: Vec<StoredEvent>,
    terminal_artifacts: BTreeSet<ArtifactId>,
}

impl AgentTurnPairing {
    // 按 Run/Task/Attempt 保存最近未闭合的 started；terminal alias 只消费同一 Artifact。
    pub(super) fn observe(&mut self, event: &StoredEvent) {
        let key = (
            event.run_id.clone(),
            event.task_id.clone(),
            event.attempt_id.clone(),
        );
        match event.event_type.as_str() {
            "agent.turn_started" => {
                if let Some(previous) = self.pending.insert(key, event.clone()) {
                    self.unmatched.push(previous);
                }
            }
            "agent.turn"
            | "agent.turn_completed"
            | "agent.turn_failed"
            | "agent.turn_retryable_failed" => {
                if let Some(id) = &event.artifact_id {
                    // A legacy alias for an already observed terminal cannot
                    // consume a newer dispatch's start.
                    if self.terminal_artifacts.insert(id.clone()) {
                        self.pending.remove(&key);
                    }
                }
            }
            "task.deferred"
            | "task.retry_scheduled"
            | "task.retry_exhausted"
            | "task.recovered"
            | "task.recovery_exhausted"
            | "task.cancelled" => {
                if let Some(start) = self.pending.remove(&key) {
                    self.unmatched.push(start);
                }
            }
            _ => {}
        }
    }

    // 把仍未闭合或因 retry/recovery 放弃的 started 按 cursor 返回为未知调用。
    pub(super) fn unmatched_starts(self) -> Vec<StoredEvent> {
        let mut starts = self.unmatched;
        starts.extend(self.pending.into_values());
        starts.sort_by_key(|event| event.cursor);
        starts
    }
}

impl Store {
    // 读取状态计数、事件总数和当前 daemon lease 数；now 只用于 lease expiry 过滤。
    pub fn metrics(&self, now: DateTime<Utc>) -> StoreResult<StoreMetrics> {
        let connection = self.connection()?;
        let run_counts = status_counts(&connection, "rebuild_runs")?;
        let task_counts = status_counts(&connection, "rebuild_tasks")?;
        let attempt_counts = status_counts(&connection, "rebuild_attempts")?;
        let event_count =
            connection.query_row("SELECT COUNT(*) FROM rebuild_events", [], |row| {
                row.get::<_, u64>(0)
            })?;
        let active_daemon_leases = connection.query_row(
            "SELECT COUNT(*) FROM rebuild_daemon_leases WHERE expires_at > ?1",
            params![now.to_rfc3339()],
            |row| row.get::<_, u64>(0),
        )?;
        Ok(StoreMetrics {
            run_counts,
            task_counts,
            attempt_counts,
            event_count,
            active_daemon_leases,
        })
    }

    // 从 exclusive cursor 之后分页读取事件，并在返回前重跑该 Run 的生命周期校验。
    pub fn events_after(
        &self,
        run_id: &RunId,
        after: i64,
        limit: usize,
    ) -> StoreResult<Vec<StoredEvent>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events WHERE run_id = ?1 AND event_id > ?2
               ORDER BY event_id ASC LIMIT ?3"#,
        )?;
        let rows = statement.query_map(
            params![run_id.0, after, limit as i64],
            stored_event_from_row,
        )?;
        let events = rows.collect::<Result<Vec<_>, _>>()?;
        for event in &events {
            let event_type = event.lifecycle_kind()?;
            validate_event_shape(
                event_type,
                event.task_id.is_some(),
                event.attempt_id.is_some(),
                event.artifact_id.is_some(),
            )?;
        }
        validate_tool_lifecycle_events(&connection, Some(run_id))?;
        validate_agent_turn_lifecycle_events(&connection, Some(run_id))?;
        validate_context_lifecycle_events(&connection, Some(run_id))?;
        validate_gate_lifecycle_events(&connection, Some(run_id))?;
        validate_paper_effect_events(&connection, Some(run_id))?;
        Ok(events)
    }

    // 获取最近事件后恢复正序，读到的投影仍按事件/Attempt lineage 检查。
    pub fn recent_events(&self, run_id: &RunId, limit: usize) -> StoreResult<Vec<StoredEvent>> {
        let connection = self.connection()?;
        let limit = i64::try_from(limit.clamp(1, 500)).expect("bounded event limit fits i64");
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events
               WHERE run_id = ?1
               ORDER BY event_id DESC
               LIMIT ?2"#,
        )?;
        let mut events = statement
            .query_map(params![run_id.0, limit], stored_event_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        events.reverse();
        for event in &events {
            let event_type = event.lifecycle_kind()?;
            validate_event_shape(
                event_type,
                event.task_id.is_some(),
                event.attempt_id.is_some(),
                event.artifact_id.is_some(),
            )?;
        }
        validate_tool_lifecycle_events(&connection, Some(run_id))?;
        validate_agent_turn_lifecycle_events(&connection, Some(run_id))?;
        validate_context_lifecycle_events(&connection, Some(run_id))?;
        validate_gate_lifecycle_events(&connection, Some(run_id))?;
        validate_paper_effect_events(&connection, Some(run_id))?;
        Ok(events)
    }

    /// Return a read-only, redacted trajectory projection for one run.
    /// Pagination follows the durable event cursor; no model, market, broker,
    /// task, or artifact mutation is performed by this query.
    pub fn trajectory(&self, run_id: &RunId) -> StoreResult<Vec<TrajectoryEntry>> {
        const PAGE_SIZE: usize = 256;
        let mut after = 0_i64;
        let mut entries = Vec::new();
        loop {
            let page = self.events_after(run_id, after, PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after = page.last().expect("non-empty trajectory page").cursor;
            for event in &page {
                if let Some(entry) = self.trajectory_entry(event)? {
                    entries.push(entry);
                }
            }
            if page.len() < PAGE_SIZE {
                break;
            }
        }
        entries.sort_by(|left, right| {
            left.cursor
                .cmp(&right.cursor)
                .then_with(|| left.task_id.cmp(&right.task_id))
                .then_with(|| left.attempt_id.cmp(&right.attempt_id))
                .then_with(|| left.turn.cmp(&right.turn))
        });
        Ok(entries)
    }

    /// Aggregate the real provider usage persisted for one run.
    ///
    /// Counted per distinct `AgentTurn` artifact, not per event: a single turn is
    /// announced by `AgentTurnStarted` and closed by one of `AgentTurn`,
    /// `AgentTurnCompleted`, `AgentTurnFailed` or `AgentTurnRetryableFailed`, and
    /// several of those can name the same artifact. Unmatched dispatch starts
    /// count as calls with unknown usage, without estimating their token cost.
    pub fn run_model_usage(&self, run_id: &RunId) -> StoreResult<RunModelUsage> {
        self.model_usage_for_tasks(run_id, None)
    }

    // 按可选 Task 闭包去重 AgentTurn Artifact，缺 usage 的调用计入 turns_missing_usage 而不估算 token。
    fn model_usage_for_tasks(
        &self,
        run_id: &RunId,
        tasks: Option<&BTreeSet<TaskId>>,
    ) -> StoreResult<RunModelUsage> {
        const PAGE_SIZE: usize = 256;
        let mut after = 0_i64;
        let mut counted = BTreeSet::new();
        let mut pairing = AgentTurnPairing::default();
        let mut usage = RunModelUsage::default();
        loop {
            let page = self.events_after(run_id, after, PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after = page.last().expect("non-empty usage page").cursor;
            for event in &page {
                if tasks.is_some_and(|tasks| {
                    event.task_id.as_ref().is_none_or(|id| !tasks.contains(id))
                }) {
                    continue;
                }
                pairing.observe(event);
                if !matches!(
                    event.lifecycle_kind()?,
                    LifecycleEventType::AgentTurn
                        | LifecycleEventType::AgentTurnCompleted
                        | LifecycleEventType::AgentTurnFailed
                        | LifecycleEventType::AgentTurnRetryableFailed
                ) {
                    continue;
                }
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    continue;
                };
                if !counted.insert(artifact_id.clone()) {
                    continue;
                }
                let artifact = self.artifact(artifact_id)?;
                if artifact.kind != ArtifactKind::AgentTurn {
                    return Err(StoreError::Integrity(format!(
                        "usage event {} references {:?}, expected agent_turn",
                        event.cursor, artifact.kind
                    )));
                }
                usage.turns += 1;
                let Ok(payload) = serde_json::from_slice::<StoredTrajectoryTurn>(
                    &self.read_blob(&artifact.blob)?,
                ) else {
                    // An unreadable turn payload is an unaccounted call, not a
                    // free one.
                    usage.turns_missing_usage += 1;
                    continue;
                };
                let telemetry = payload
                    .response
                    .as_ref()
                    .and_then(|response| response.telemetry.as_ref())
                    .or(payload.telemetry.as_ref());
                let Some(telemetry) = telemetry.filter(|telemetry| telemetry.reported_usage())
                else {
                    usage.turns_missing_usage += 1;
                    continue;
                };
                usage.input_tokens = usage
                    .input_tokens
                    .saturating_add(telemetry.input_tokens.unwrap_or_default());
                usage.cached_input_tokens = usage
                    .cached_input_tokens
                    .saturating_add(telemetry.cached_input_tokens.unwrap_or_default());
                usage.output_tokens = usage
                    .output_tokens
                    .saturating_add(telemetry.output_tokens.unwrap_or_default());
                usage.reasoning_tokens = usage
                    .reasoning_tokens
                    .saturating_add(telemetry.reasoning_tokens.unwrap_or_default());
                usage.latency_millis = usage
                    .latency_millis
                    .saturating_add(telemetry.latency_millis.unwrap_or_default());
            }
            if page.len() < PAGE_SIZE {
                break;
            }
        }
        let unknown_calls = pairing.unmatched_starts().len() as u64;
        usage.turns = usage.turns.saturating_add(unknown_calls);
        usage.turns_missing_usage = usage.turns_missing_usage.saturating_add(unknown_calls);
        Ok(usage)
    }

    /// Decision production cost includes all retries in its dependency closure.
    /// Post-terminal Outcome tasks share the run but cannot enter that closure.
    pub fn model_usage_for_producing_run(
        &self,
        artifact: &ArtifactRef,
    ) -> StoreResult<RunModelUsage> {
        let stored = self.artifact(&artifact.artifact_id)?;
        if stored.kind != artifact.kind {
            return Err(StoreError::Integrity(format!(
                "artifact {} is {:?}, expected {:?}",
                artifact.artifact_id.0, stored.kind, artifact.kind
            )));
        }
        let Some(origin) = stored.origin else {
            return Ok(RunModelUsage::default());
        };
        let (Some(run_id), Some(task_id)) = (origin.run_id, origin.task_id) else {
            return Ok(RunModelUsage::default());
        };
        let snapshot = self.workflow_snapshot(&run_id)?;
        let mut pending = vec![task_id];
        let mut tasks = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !tasks.insert(id.clone()) {
                continue;
            }
            let task = snapshot
                .tasks
                .iter()
                .find(|task| task.node.task_id == id)
                .ok_or_else(|| {
                    StoreError::Integrity("producer task closure is incomplete".to_owned())
                })?;
            pending.extend(task.node.dependencies.iter().cloned());
        }
        self.model_usage_for_tasks(&run_id, Some(&tasks))
    }

    /// Retrospective cost remains separately queryable from lifecycle total.
    pub fn outcome_model_usage(&self, run_id: &RunId) -> StoreResult<RunModelUsage> {
        let tasks = self
            .workflow_snapshot(run_id)?
            .tasks
            .into_iter()
            .filter(|task| {
                task.node.recipe_id.as_str() == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID
            })
            .map(|task| task.node.task_id)
            .collect();
        self.model_usage_for_tasks(run_id, Some(&tasks))
    }

    /// Return the newest redacted trajectory entries in durable cursor order.
    /// The hard cap keeps observer reads bounded even for long-running tasks.
    pub fn recent_trajectory(
        &self,
        run_id: &RunId,
        limit: usize,
    ) -> StoreResult<Vec<TrajectoryEntry>> {
        self.recent_events(run_id, limit.clamp(1, 200))?
            .into_iter()
            .filter_map(|event| self.trajectory_entry(&event).transpose())
            .collect()
    }
}

// 将 SQL event row 解码为 StoreEvent；时间/hash 解析失败通过 rusqlite 转换错误向上传播。
pub(super) fn stored_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredEvent> {
    Ok(StoredEvent {
        cursor: row.get(0)?,
        run_id: RunId(row.get(1)?),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId),
        attempt_id: row
            .get::<_, Option<String>>(3)?
            .map(akzio_domain::AttemptId),
        event_type: row.get(4)?,
        artifact_id: row
            .get::<_, Option<String>>(5)?
            .map(ContentHash::new)
            .transpose()
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
            .map(ArtifactId),
        created_at: parse_time(&row.get::<_, String>(6)?)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
    })
}

#[cfg(test)]
mod pairing_tests {
    use super::*;

    // 构造最小事件值，测试只关注配对状态，不依赖真实 SQLite。
    fn event(cursor: i64, attempt: &str, kind: &str, artifact: Option<&str>) -> StoredEvent {
        StoredEvent {
            cursor,
            run_id: RunId("run".into()),
            task_id: Some(TaskId("task".into())),
            attempt_id: Some(AttemptId(attempt.into())),
            event_type: kind.into(),
            artifact_id: artifact.map(|value| ArtifactId(ContentHash::of_bytes(value.as_bytes()))),
            created_at: Utc::now(),
        }
    }

    // 运行 pairing helper 并只返回未知 started 的 cursor，便于断言不误配。
    fn unmatched(events: &[StoredEvent]) -> Vec<i64> {
        let mut pairing = AgentTurnPairing::default();
        for event in events {
            pairing.observe(event);
        }
        pairing
            .unmatched_starts()
            .iter()
            .map(|event| event.cursor)
            .collect()
    }

    #[test]
    // 新 started 不能被旧 Attempt 或不同 Attempt 的 terminal 关闭。
    fn pairing_never_closes_new_start_with_old_or_other_attempt_terminal() {
        assert_eq!(
            unmatched(&[
                event(1, "a", "agent.turn_completed", Some("legacy")),
                event(2, "a", "agent.turn_started", None),
                event(3, "b", "agent.turn_completed", Some("recovered")),
                event(4, "a", "agent.turn", Some("legacy")),
            ]),
            vec![2]
        );
    }

    #[test]
    // retry 放弃的 started 保留为未知，而后续 Attempt 可正常完成配对。
    fn pairing_retains_abandoned_start_while_matching_later_attempt_normally() {
        assert_eq!(
            unmatched(&[
                event(1, "a", "agent.turn_started", None),
                event(2, "a", "task.retry_scheduled", None),
                event(3, "b", "agent.turn_started", None),
                event(4, "b", "agent.turn_completed", Some("retry")),
            ]),
            vec![1]
        );
    }

    #[test]
    // Draft/Submit 两次调用可完整配对，并且同 Artifact 的历史 alias 不重复计费。
    fn pairing_accepts_complete_draft_submit_and_deduplicates_terminal_alias() {
        assert!(unmatched(&[
            event(1, "a", "agent.turn_started", None),
            event(2, "a", "agent.turn_completed", Some("draft")),
            event(3, "a", "agent.turn_started", None),
            event(4, "a", "agent.turn", Some("draft")),
            event(5, "a", "agent.turn_completed", Some("submit")),
        ])
        .is_empty());
    }
}
