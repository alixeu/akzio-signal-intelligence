use crate::schema::ensure_schema;
use anyhow::Result;
use chrono::Utc;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub fn import_jin10_payload(conn: &mut Connection, payload: &Value) -> Result<usize> {
    ensure_schema(conn)?;
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let tx = conn.transaction()?;
    let imported_at = Utc::now().to_rfc3339();
    let mut count = 0;
    for item in items {
        let item_time = item.get("time").and_then(Value::as_str).unwrap_or_default();
        let content = item
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if item_time.is_empty() || content.is_empty() {
            continue;
        }
        let item_json = serde_json::to_string(&item)?;
        let event_key = jin10_event_key(&item);
        let content_hash = sha256_hex(&item_json);
        tx.execute(
            r#"
            INSERT OR REPLACE INTO jin10_items
                (event_key, item_time, content, item_json, content_hash, imported_at)
            VALUES (?, ?, ?, ?, ?, ?)
            "#,
            params![
                event_key,
                item_time,
                content,
                item_json,
                content_hash,
                imported_at
            ],
        )?;
        tx.execute(
            r#"
            INSERT OR REPLACE INTO external_source_items
                (source, source_key, ticker, item_time, title, content, item_json, content_hash, imported_at)
            VALUES ('jin10', ?, '', ?, '', ?, ?, ?, ?)
            "#,
            params![event_key, item_time, content, item_json, content_hash, imported_at],
        )?;
        count += 1;
    }
    tx.commit()?;
    Ok(count)
}

fn jin10_event_key(item: &Value) -> String {
    let mut seed = BTreeMap::new();
    seed.insert("time", item.get("time").cloned().unwrap_or(Value::Null));
    seed.insert(
        "content",
        item.get("content").cloned().unwrap_or(Value::Null),
    );
    sha256_hex(&serde_json::to_string(&json!(seed)).unwrap_or_default())
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}
