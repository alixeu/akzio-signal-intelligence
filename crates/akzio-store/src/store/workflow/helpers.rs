pub(super) fn contract_upgrade_blockers(
    connection: &Connection,
    active_contract_hash: &ContentHash,
) -> StoreResult<Vec<String>> {
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
fn task_failure_attempt_count(connection: &Connection, task_id: &TaskId) -> StoreResult<u64> {
    let boundary: i64 = connection.query_row(
        "SELECT COALESCE(MAX(a.rowid), 0) FROM rebuild_events e JOIN rebuild_attempts a ON a.attempt_id=e.attempt_id JOIN rebuild_tasks t ON t.task_id=e.task_id WHERE e.task_id=?1 AND t.recipe_id=?2 AND e.event_type=?3",
        params![task_id.0, POST_TERMINAL_WORKER_RECIPE_ID, LifecycleEventType::RetrospectiveCreated.as_str()], |row| row.get(0))?;
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM rebuild_attempts WHERE task_id=?1 AND rowid > ?2 AND status IN ('running', 'retried', 'failed', 'abandoned')",
        params![task_id.0, boundary], |row| row.get(0))?)
}
