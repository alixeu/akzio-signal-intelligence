fn contract_upgrade_blockers(
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
