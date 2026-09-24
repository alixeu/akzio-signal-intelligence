// 文件导读：这里仅探测 Lesson 三张表的形状，兼容没有 evidence ledger 的旧 Store；
// 它不会创建表，创建动作由 lesson/queries_verify.rs 的显式入口负责。
// 表名是固定 SQL 字面量；两个函数都借用既有 Connection，只返回 schema 存在性，不修改数据库。
// `2 if ...` 是带 guard 的 match 分支：仅在 evidence 表不存在时接纳旧版两表组合，
// 其它不完整集合走 Integrity，而不是自动补表。
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

// sqlite_master 精确按表名查 ledger 表；COUNT=0 才是 false，SQL 错误仍传播为 Err。
fn lesson_evidence_table_exists(connection: &Connection) -> StoreResult<bool> {
    Ok(connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'rebuild_lesson_evidence'",
        [],
        |row| row.get::<_, u64>(0),
    )? > 0)
}
