// 文件导读：这些辅助查询只识别已退役的 workflow 和 Contract 升级阻断，
// 并为 Outcome worker 计算当前阶段失败次数；它们不恢复旧创建/执行入口。
// 上层 claim/retry/迁移路径复用下列只读 helper；run/task IDs 作为 SQL 参数绑定，返回布尔或 blocker 明细。
// 按 run_id 检查已退役 purpose 或 planner/旧版研究 Contract Task；EXISTS 查询只返回 bool，不读 Artifact payload。
pub(super) fn legacy_workflow(connection: &Connection, run: &RunId) -> StoreResult<bool> {
    Ok(connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_runs r WHERE r.run_id=?1 AND (r.purpose='paper_dry_run' OR EXISTS(SELECT 1 FROM rebuild_tasks t LEFT JOIN rebuild_contract_installations c ON c.contract_hash=t.contract_hash WHERE t.run_id=r.run_id AND (t.recipe_id='research.planner' OR (c.purpose IN ('research.analyst','research.critic','research.synthesizer') AND c.contract_version < 65)))))",
        params![run.0], |row| row.get(0))?)
}

pub(super) fn assert_workflow_executable(connection: &Connection, run: &RunId) -> StoreResult<()> {
    // 退役标记在任何 claim/retry/recovery 写入前检查，避免旧图继续产生新状态。
    if legacy_workflow(connection, run)? { return Err(StoreError::DebugControl("legacy_workflow_retired".into())); }
    Ok(())
}

pub(super) fn contract_upgrade_blockers(
    connection: &Connection,
    active_contract_hash: &ContentHash,
) -> StoreResult<Vec<String>> {
    // 先列出使用旧 Contract 且仍 queued/leased/running 的 Task，再列出含该 Contract 的未提交 Paper session；
    // UNION ALL 保留每个 blocker 明细，ORDER BY 1 稳定排序，空列表才表示可升级。
    let mut statement = connection.prepare(
        r#"
        SELECT 'task:' || run_id || ':' || task_id || ':' || status
        FROM rebuild_tasks
        WHERE contract_hash = ?1
          AND status IN ('queued', 'leased', 'running')
        UNION ALL
        SELECT 'session:' || session_key || ':' || run_id
        FROM rebuild_session_slots AS slot
        WHERE committed_at IS NULL
          AND EXISTS (SELECT 1 FROM rebuild_runs AS run WHERE run.run_id = slot.run_id
                      AND run.status IN ('queued', 'leased', 'running'))
          AND EXISTS (
              SELECT 1
              FROM rebuild_tasks AS task
              WHERE task.run_id = slot.run_id
                AND task.contract_hash = ?1
          )
        ORDER BY 1
        "#,
    )?;
    let rows = statement.query_map(params![active_contract_hash.as_str()], |row| row.get(0))?;
    let blockers = rows.collect::<Result<Vec<String>, _>>()?;
    Ok(blockers)
}

/// Each completed Outcome stage establishes a fresh failure budget. The cursor
/// is derived from committed stage events, never from wall-clock dates/defer polls.
// 查询最近一次 OutcomeWorker RetrospectiveCreated 边界（SQL 不按 horizon 再筛）之后该 Task 的 Attempt 状态数；
// 用 `a.rowid` 作阶段边界，deferred 不计失败额度；这不是按日历日或 caller 自报次数重置。
fn task_failure_attempt_count(connection: &Connection, task_id: &TaskId) -> StoreResult<u64> {
    let boundary: i64 = connection.query_row(
        "SELECT COALESCE(MAX(a.rowid), 0) FROM rebuild_events e JOIN rebuild_attempts a ON a.attempt_id=e.attempt_id JOIN rebuild_tasks t ON t.task_id=e.task_id WHERE e.task_id=?1 AND t.recipe_id=?2 AND e.event_type=?3",
        params![task_id.0, POST_TERMINAL_WORKER_RECIPE_ID, LifecycleEventType::RetrospectiveCreated.as_str()], |row| row.get(0))?;
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM rebuild_attempts WHERE task_id=?1 AND rowid > ?2 AND status IN ('running', 'retried', 'failed', 'abandoned')",
        params![task_id.0, boundary], |row| row.get(0))?)
}
