use anyhow::Result;
use md5::{Digest, Md5};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

/// Import Jin10 flash items into the dedicated `jin10_items` table.
///
/// - `id` = md5(time + "\n" + content)
/// - `content_json` = compact JSON payload passed to the LLM (`{id,time,content}`)
/// - `attention_score` is preserved across re-imports (0.0-1.0 LLM attention)
/// - `item_time` / `imported_at` are unix timestamps
pub fn import_jin10_payload(conn: &mut Connection, payload: &Value) -> Result<usize> {
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let tx = conn.transaction()?;
    let imported_at = chrono::Utc::now().timestamp();
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
        let item_time = parse_jin10_time(item_time_str);
        let id = jin10_item_id(item_time_str, content);
        let content_json = serde_json::to_string(&json!({
            "id": id,
            "time": item_time,
            "time_raw": item_time_str,
            "content": content,
        }))?;
        tx.execute(
            r#"
            INSERT INTO jin10_items (id, content_json, attention_score, item_time, imported_at)
            VALUES (?1, ?2, 0.0, ?3, ?4)
            ON CONFLICT(id) DO UPDATE SET
                content_json = excluded.content_json,
                item_time = excluded.item_time,
                imported_at = excluded.imported_at
            "#,
            params![id, content_json, item_time, imported_at],
        )?;
        count += 1;
    }
    tx.commit()?;
    Ok(count)
}

/// Import only the specified jin10 items (by id) into the database.
/// Used when LLM has scored items — only scored items get persisted.
pub fn import_scored_jin10_items(
    conn: &Connection,
    items: &[orchestrator_core::Jin10CsvRow],
) -> Result<usize> {
    let imported_at = chrono::Utc::now().timestamp();
    let mut count = 0;
    for item in items {
        if item.id.is_empty() || item.content.is_empty() {
            continue;
        }
        let item_time = parse_jin10_time(&item.time);
        let content_json = serde_json::to_string(&json!({
            "id": item.id,
            "time": item_time,
            "time_raw": item.time,
            "content": item.content,
        }))?;
        conn.execute(
            r#"
            INSERT INTO jin10_items (id, content_json, attention_score, item_time, imported_at)
            VALUES (?1, ?2, 0.0, ?3, ?4)
            ON CONFLICT(id) DO NOTHING
            "#,
            params![item.id, content_json, item_time, imported_at],
        )?;
        count += 1;
    }
    Ok(count)
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
    use crate::phase_index::{record_attention, AttentionEvent};
    let mut seen = std::collections::BTreeSet::new();
    let mut updated = 0usize;
    for item in items {
        let id = item.id.trim();
        if id.is_empty() || !seen.insert(id.to_string()) {
            continue;
        }
        let score = item.score.clamp(0.0, 1.0);
        if !run_id.is_empty() {
            record_attention(
                conn,
                &AttentionEvent {
                    run_id: run_id.to_string(),
                    turn_id: turn_id.to_string(),
                    role: role.to_string(),
                    subject_kind: "jin10".to_string(),
                    subject_id: id.to_string(),
                    score,
                    phase,
                },
            )?;
        } else {
            let _ = conn.execute(
                "UPDATE jin10_items SET attention_score = ?1 WHERE id = ?2",
                params![score, id],
            )?;
        }
        updated += 1;
    }
    Ok(updated)
}

/// Stable Jin10 primary key: md5(time_raw + "\\n" + content).
pub fn jin10_item_id(time_raw: &str, content: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(time_raw.as_bytes());
    hasher.update(b"\n");
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn parse_jin10_time(s: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .and_then(|dt| dt.and_utc().timestamp().into())
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
}
