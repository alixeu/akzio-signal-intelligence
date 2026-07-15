use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

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
        let metadata_json = serde_json::to_string(&item)?;
        let item_key = jin10_event_key(&item);
        let content_hash = sha256_hex(&metadata_json);
        tx.execute(
            r#"
            INSERT OR REPLACE INTO external_items
                (source, item_key, ticker, item_time, title, content, metadata_json, content_hash, imported_at)
            VALUES ('jin10', ?, '', ?, '', ?, ?, ?, ?)
            "#,
            params![
                item_key,
                item_time,
                content,
                metadata_json,
                content_hash,
                imported_at
            ],
        )?;
        count += 1;
    }
    tx.commit()?;
    Ok(count)
}

fn parse_jin10_time(s: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .and_then(|dt| dt.and_utc().timestamp().into())
        .unwrap_or(0)
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
