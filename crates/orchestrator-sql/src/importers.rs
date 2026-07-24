use crate::schema::{ensure_run_exists, now_ms};
use anyhow::{bail, Result};
use rusqlite::{params, Connection};
use serde_json::Value;
use uuid::Uuid;

#[cfg(test)]
use serde_json::json;

/// Import Jin10 flash items into the dedicated `jin10_items` table.
///
/// - `id` = md5(time + "\n" + content)
///
/// Fixed fields are stored as columns; only non-fixed source fields are kept in
/// `metadata_json`. The latest attention cache is preserved across re-imports.
pub fn import_jin10_payload(conn: &mut Connection, payload: &Value) -> Result<usize> {
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let tx = conn.transaction()?;
    let imported_at_ms = now_ms();
    let mut insert = tx.prepare_cached(
        r#"
        INSERT INTO jin10_items
            (id,content,time_raw,item_time_ms,latest_attention_score,imported_at_ms,
             metadata_json,legacy_attention)
        VALUES (?1,?2,?3,?4,0.0,?5,?6,0)
        ON CONFLICT(id) DO UPDATE SET
            content=excluded.content,
            time_raw=excluded.time_raw,
            item_time_ms=excluded.item_time_ms,
            imported_at_ms=excluded.imported_at_ms,
            metadata_json=excluded.metadata_json
        "#,
    )?;
    let mut count = 0;
    for item in items {
        let item_time_str = item.get("time").and_then(Value::as_str).unwrap_or_default();
        let content = item
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if item_time_str.is_empty() || content.is_empty() {
            continue;
        }
        let item_time_ms = parse_jin10_time_ms(item_time_str);
        let id = jin10_item_id(item_time_str, content);
        let mut metadata = item.as_object().cloned().unwrap_or_default();
        metadata.remove("time");
        metadata.remove("content");
        insert.execute(params![
            id,
            content,
            item_time_str,
            item_time_ms,
            imported_at_ms,
            serde_json::to_string(&metadata)?,
        ])?;
        count += 1;
    }
    drop(insert);
    tx.commit()?;
    Ok(count)
}

/// Import only the specified jin10 items (by id) into the database.
/// Used when LLM has scored items — only scored items get persisted.
pub fn import_scored_jin10_items(
    conn: &Connection,
    items: &[orchestrator_core::Jin10CsvRow],
) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let imported_at_ms = now_ms();
    let mut insert = tx.prepare_cached(
        r#"
        INSERT INTO jin10_items
            (id,content,time_raw,item_time_ms,latest_attention_score,imported_at_ms,
             metadata_json,legacy_attention)
        VALUES (?1,?2,?3,?4,0.0,?5,'{}',0)
        ON CONFLICT(id) DO UPDATE SET
            content=excluded.content,
            time_raw=excluded.time_raw,
            item_time_ms=excluded.item_time_ms,
            imported_at_ms=excluded.imported_at_ms
        "#,
    )?;
    let mut count = 0;
    for item in items {
        if item.id.is_empty() || item.content.is_empty() {
            continue;
        }
        let item_time_ms = parse_jin10_time_ms(&item.time);
        insert.execute(params![
            item.id,
            item.content,
            item.time,
            item_time_ms,
            imported_at_ms
        ])?;
        count += 1;
    }
    drop(insert);
    tx.commit()?;
    Ok(count)
}

/// Return the subset of `ids` that currently exist in `jin10_items`.
///
/// The news analyst occasionally references jin10 ids that are truncated or
/// hallucinated. Callers use this to validate LLM-provided ids before the
/// authoritative attention write, which requires every target row to exist.
pub fn existing_jin10_ids(
    conn: &Connection,
    ids: &[String],
) -> Result<std::collections::BTreeSet<String>> {
    let mut stmt = conn.prepare_cached("SELECT 1 FROM jin10_items WHERE id = ?1")?;
    let mut present = std::collections::BTreeSet::new();
    for id in ids {
        let id = id.trim();
        if id.is_empty() || present.contains(id) {
            continue;
        }
        if stmt.exists(params![id])? {
            present.insert(id.to_string());
        }
    }
    Ok(present)
}

/// A single Jin10 attention assignment from the news analyst.
#[derive(Debug, Clone)]
pub struct Jin10Attention {
    pub id: String,
    /// Attention weight in [0.0, 1.0].
    pub score: f64,
}

/// Record Jin10 attention into the unified ledger and cache on `jin10_items`.
///
/// Prefer `record_jin10_attention_for_turn` when `run_id` / `turn_id` / `role` are known.
pub fn record_jin10_attention(conn: &Connection, items: &[Jin10Attention]) -> Result<usize> {
    record_jin10_attention_for_turn(conn, "", "", "analyst.news_macro", Some(1), items)
}

/// Authoritative attention write: ledger + cached `jin10_items.attention_score`.
pub fn record_jin10_attention_for_turn(
    conn: &Connection,
    run_id: &str,
    turn_id: &str,
    role: &str,
    phase: Option<i64>,
    items: &[Jin10Attention],
) -> Result<usize> {
    let mut seen = std::collections::BTreeSet::new();
    let tx = conn.unchecked_transaction()?;
    if !run_id.is_empty() {
        ensure_run_exists(
            &tx,
            run_id,
            &chrono::Utc::now()
                .date_naive()
                .format("%Y-%m-%d")
                .to_string(),
        )?;
    }
    let mut insert_ledger = tx.prepare_cached(
        r#"INSERT INTO attention_ledger
           (id,run_id,turn_id,role,subject_kind,subject_id,score,phase,created_at_ms)
           VALUES (?1,?2,NULLIF(?3,''),?4,'jin10',?5,?6,?7,?8)"#,
    )?;
    let mut update_cache = tx.prepare_cached(
        "UPDATE jin10_items SET latest_attention_score=?1, legacy_attention=?2 WHERE id=?3",
    )?;
    let mut updated = 0usize;
    for item in items {
        let id = item.id.trim();
        if id.is_empty() || !seen.insert(id.to_string()) {
            continue;
        }
        let score = item.score.clamp(0.0, 1.0);
        if !run_id.is_empty() {
            if update_cache.execute(params![score, 0, id])? != 1 {
                bail!("cannot record attention for missing Jin10 item {id}");
            }
            insert_ledger.execute(params![
                Uuid::new_v4().to_string(),
                run_id,
                turn_id,
                role,
                id,
                score,
                phase,
                now_ms(),
            ])?;
        } else {
            if update_cache.execute(params![score, 1, id])? != 1 {
                bail!("cannot record legacy attention for missing Jin10 item {id}");
            }
        }
        updated += 1;
    }
    drop(insert_ledger);
    drop(update_cache);
    tx.commit()?;
    Ok(updated)
}

/// Stable Jin10 primary key: md5(time_raw + "\\n" + content).
pub fn jin10_item_id(time_raw: &str, content: &str) -> String {
    orchestrator_core::jin10_item_id(time_raw, content)
}

fn parse_jin10_time_ms(s: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .map(|dt| dt.and_utc().timestamp_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{connect, ensure_schema};

    #[test]
    fn jin10_id_is_stable_md5() {
        let a = jin10_item_id("2026-06-19 09:00:00", "rate cut odds move");
        let b = jin10_item_id("2026-06-19 09:00:00", "rate cut odds move");
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
        assert_ne!(a, jin10_item_id("2026-06-19 09:00:00", "other"));
    }

    #[test]
    fn import_preserves_attention_and_record_updates_score() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("t.sqlite");
        let mut conn = connect(&db_path).unwrap();
        ensure_schema(&conn).unwrap();
        let payload = json!({
            "items": [
                {"time": "2026-06-19 09:00:00", "content": "rate cut odds move"}
            ]
        });
        assert_eq!(import_jin10_payload(&mut conn, &payload).unwrap(), 1);
        let id = jin10_item_id("2026-06-19 09:00:00", "rate cut odds move");
        assert_eq!(
            record_jin10_attention(
                &conn,
                &[Jin10Attention {
                    id: id.clone(),
                    score: 0.82,
                }]
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row(
                "SELECT attention_score FROM jin10_items WHERE id = ?",
                [&id],
                |row| row.get::<_, f64>(0)
            )
            .unwrap(),
            0.82
        );
        // Re-import must not reset attention.
        assert_eq!(import_jin10_payload(&mut conn, &payload).unwrap(), 1);
        assert_eq!(
            conn.query_row(
                "SELECT attention_score FROM jin10_items WHERE id = ?",
                [&id],
                |row| row.get::<_, f64>(0)
            )
            .unwrap(),
            0.82
        );
        // Latest score wins.
        assert_eq!(
            record_jin10_attention(
                &conn,
                &[Jin10Attention {
                    id: id.clone(),
                    score: 0.35,
                }]
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row(
                "SELECT attention_score FROM jin10_items WHERE id = ?",
                [&id],
                |row| row.get::<_, f64>(0)
            )
            .unwrap(),
            0.35
        );
    }

    #[test]
    fn existing_jin10_ids_filters_missing() {
        let temp = tempfile::tempdir().unwrap();
        let mut conn = connect(temp.path().join("existing.sqlite")).unwrap();
        ensure_schema(&conn).unwrap();
        let payload = json!({
            "items": [
                {"time": "2026-06-19 09:00:00", "content": "rate cut odds move"}
            ]
        });
        assert_eq!(import_jin10_payload(&mut conn, &payload).unwrap(), 1);
        let real = jin10_item_id("2026-06-19 09:00:00", "rate cut odds move");
        let present = existing_jin10_ids(
            &conn,
            &[real.clone(), "13b67309".to_string(), "  ".to_string()],
        )
        .unwrap();
        assert!(present.contains(&real));
        assert!(!present.contains("13b67309"));
        assert_eq!(present.len(), 1);
    }
}
