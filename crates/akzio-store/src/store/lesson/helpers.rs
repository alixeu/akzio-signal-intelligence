fn ensure_lesson_table_set(connection: &Connection) -> StoreResult<u64> {
    let table_count = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('rebuild_lesson_heads', 'rebuild_lesson_events', 'rebuild_lesson_evidence')",
        [],
        |row| row.get::<_, u64>(0),
    )?;
    match table_count {
        0 | 3 => Ok(table_count),
        // A Store written before the evidence ledger existed has the two
        // original tables and no evidence rows. That is a valid older shape, not
        // corruption; treat it as present so Doctor keeps verifying history.
        2 if !lesson_evidence_table_exists(connection)? => Ok(table_count),
        _ => Err(StoreError::Integrity(
            "lesson table set is incomplete".to_owned(),
        )),
    }
}

fn lesson_evidence_table_exists(connection: &Connection) -> StoreResult<bool> {
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'rebuild_lesson_evidence'",
        [],
        |row| row.get::<_, u64>(0),
    )? > 0)
}
