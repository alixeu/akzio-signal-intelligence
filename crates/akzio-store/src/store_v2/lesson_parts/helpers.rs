fn ensure_lesson_table_set(connection: &Connection) -> StoreResult<u64> {
    let table_count = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('rebuild_lesson_heads', 'rebuild_lesson_events')",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    match table_count {
        0 | 2 => Ok(table_count),
        _ => Err(StoreError::Integrity(
            "lesson table set is incomplete".to_owned(),
        )),
    }
}
