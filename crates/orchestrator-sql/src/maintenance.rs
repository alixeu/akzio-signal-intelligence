use crate::schema::{APPLICATION_ID, CURRENT_SCHEMA_VERSION};
use anyhow::{bail, Result};
use chrono::{Duration, Utc};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde_json::{json, Value};
use std::path::Path;

const TABLES: &[&str] = &[
    "runs",
    "agent_events",
    "role_turn_summaries",
    "memory_items",
    "memory_versions",
    "memory_history",
    "jin10_items",
    "technical_bars",
    "phase_summaries",
    "phase_summary_details",
    "attention_ledger",
    "predictions",
    "outcomes",
    "candidate_experiences",
    "schema_archive",
];

const JSON_COLUMNS: &[(&str, &str)] = &[
    ("runs", "prompt_versions_json"),
    ("agent_events", "full_context_json"),
    ("agent_events", "context_delta_json"),
    ("role_turn_summaries", "summary_json"),
    ("memory_items", "market_regime_json"),
    ("memory_versions", "body_json"),
    ("memory_versions", "evidence_refs_json"),
    ("jin10_items", "metadata_json"),
    ("technical_bars", "values_json"),
    ("phase_summaries", "summary_json"),
    ("phase_summary_details", "detail_json"),
    ("predictions", "market_regime_json"),
    ("predictions", "agent_probabilities_json"),
    ("candidate_experiences", "market_regime_json"),
    ("candidate_experiences", "evidence_json"),
    ("candidate_experiences", "counter_evidence_json"),
    ("candidate_experiences", "metrics_json"),
    ("candidate_experiences", "sample_run_ids_json"),
];

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub context_checkpoint_days: i64,
    pub jin10_unreferenced_days: i64,
    pub attention_ledger_days: Option<i64>,
    pub debug_agent_events_days: Option<i64>,
    pub technical_bar_days: i64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            context_checkpoint_days: 30,
            jin10_unreferenced_days: 30,
            attention_ledger_days: None,
            debug_agent_events_days: None,
            technical_bar_days: 90,
        }
    }
}

pub fn open_read_only(path: impl AsRef<Path>) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")?;
    Ok(conn)
}

/// Read-only database diagnostics. This function never runs migrations,
/// checkpoints, optimize, vacuum, or repairs.
pub fn database_doctor(conn: &Connection) -> Result<Value> {
    let schema_version = pragma_i64(conn, "user_version")?;
    let application_id = pragma_i64(conn, "application_id")?;
    let page_size = pragma_i64(conn, "page_size")?;
    let page_count = pragma_i64(conn, "page_count")?;
    let freelist_count = pragma_i64(conn, "freelist_count")?;
    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    let wal_autocheckpoint = pragma_i64(conn, "wal_autocheckpoint")?;

    let mut table_rows = serde_json::Map::new();
    for table in TABLES {
        if table_exists(conn, table)? {
            table_rows.insert((*table).to_string(), json!(count(conn, table, "1")?));
        }
    }

    let mut json_lengths = Vec::new();
    for (table, column) in JSON_COLUMNS {
        if table_exists(conn, table)? && column_exists(conn, table, column)? {
            let sql = format!(
                "SELECT COALESCE(AVG(length({column})),0), COALESCE(MAX(length({column})),0) FROM {table}"
            );
            let (average, maximum): (f64, i64) =
                conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?)))?;
            json_lengths.push(json!({
                "table": table,
                "column": column,
                "average_bytes": average,
                "maximum_bytes": maximum,
            }));
        }
    }

    let (context_total_bytes, context_average_bytes): (i64, f64) = if table_exists(
        conn,
        "agent_events",
    )? {
        conn.query_row(
            r#"SELECT COALESCE(SUM(length(COALESCE(full_context_json,'')) + length(context_delta_json)),0),
                      COALESCE(AVG(length(COALESCE(full_context_json,'')) + length(context_delta_json)),0)
               FROM agent_events"#,
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?
    } else {
        (0, 0.0)
    };

    let duplicate_payload_groups: i64 = if table_exists(conn, "role_turn_summaries")? {
        conn.query_row(
            r#"SELECT COUNT(*) FROM (
                   SELECT payload_hash FROM role_turn_summaries
                   GROUP BY payload_hash HAVING COUNT(*) > 1
               )"#,
            [],
            |row| row.get(0),
        )?
    } else {
        0
    };
    let empty_identity_fields = empty_identity_count(conn)?;
    let foreign_key_violations: i64 =
        conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    let quick_check: String = conn.query_row("PRAGMA quick_check", [], |row| row.get(0))?;

    Ok(json!({
        "schema": {
            "version": schema_version,
            "current_version": CURRENT_SCHEMA_VERSION,
            "application_id": application_id,
            "expected_application_id": APPLICATION_ID,
        },
        "storage": {
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "estimated_database_bytes": page_size.saturating_mul(page_count),
        },
        "wal": {
            "journal_mode": journal_mode,
            "wal_autocheckpoint": wal_autocheckpoint,
            "checkpoint_status": "not executed by read-only doctor; use explicit db-checkpoint",
        },
        "table_rows": table_rows,
        "json_lengths": json_lengths,
        "agent_context": {
            "total_bytes": context_total_bytes,
            "average_bytes": context_average_bytes,
        },
        "technical_bar_count": table_rows.get("technical_bars").cloned().unwrap_or(json!(0)),
        "duplicate_payload_hash_groups": duplicate_payload_groups,
        "empty_identity_fields": empty_identity_fields,
        "orphan_foreign_keys": foreign_key_violations,
        "foreign_key_check": if foreign_key_violations == 0 { "ok" } else { "failed" },
        "quick_check": quick_check,
        "query_plans": core_query_plans(conn)?,
    }))
}

pub fn cleanup_database(
    conn: &Connection,
    policy: &RetentionPolicy,
    dry_run: bool,
    optimize: bool,
) -> Result<Value> {
    for days in [
        policy.context_checkpoint_days,
        policy.jin10_unreferenced_days,
        policy.technical_bar_days,
    ] {
        if days < 0 {
            bail!("retention days cannot be negative");
        }
    }
    let now = Utc::now();
    let context_cutoff = (now - Duration::days(policy.context_checkpoint_days)).timestamp_millis();
    let jin10_cutoff = (now - Duration::days(policy.jin10_unreferenced_days)).timestamp_millis();
    let technical_cutoff = (now - Duration::days(policy.technical_bar_days))
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();
    let empty_hash = sha256_hex("[]");

    let context_where = format!(
        r#"created_at_ms < {context_cutoff}
            AND (full_context_json IS NOT NULL OR context_delta_json != '[]')
            AND EXISTS (
                SELECT 1 FROM agent_events newer
                WHERE newer.run_id=agent_events.run_id
                  AND newer.role=agent_events.role
                  AND newer.full_context_json IS NOT NULL
                  AND (newer.turn_number > agent_events.turn_number
                       OR (newer.turn_number=agent_events.turn_number AND newer.id > agent_events.id))
            )"#
    );
    let jin10_where = format!(
        r#"imported_at_ms < {jin10_cutoff}
            AND NOT EXISTS (
                SELECT 1 FROM attention_ledger a
                WHERE a.subject_kind='jin10' AND a.subject_id=jin10_items.id
            )"#
    );
    let technical_where = format!(
        r#"bar_time < '{technical_cutoff}'
            AND NOT EXISTS (
                SELECT 1 FROM predictions p
                LEFT JOIN outcomes o ON o.prediction_id=p.id
                WHERE o.id IS NULL
                  AND p.ticker=technical_bars.ticker
                  AND technical_bars.bar_time BETWEEN p.prediction_date AND p.outcome_due_date
            )"#
    );
    let attention_where = policy.attention_ledger_days.map(|days| {
        let cutoff = (now - Duration::days(days)).timestamp_millis();
        format!("created_at_ms < {cutoff}")
    });
    let debug_where = policy.debug_agent_events_days.map(|days| {
        let cutoff = (now - Duration::days(days)).timestamp_millis();
        format!("created_at_ms < {cutoff} AND context_warning=1")
    });

    let mut counts = serde_json::Map::new();
    counts.insert(
        "agent_context_payloads".into(),
        json!(count(conn, "agent_events", &context_where)?),
    );
    counts.insert(
        "jin10_items".into(),
        json!(count(conn, "jin10_items", &jin10_where)?),
    );
    counts.insert(
        "technical_bars".into(),
        json!(count(conn, "technical_bars", &technical_where)?),
    );
    counts.insert(
        "attention_ledger".into(),
        json!(match &attention_where {
            Some(predicate) => count(conn, "attention_ledger", predicate)?,
            None => 0,
        }),
    );
    counts.insert(
        "debug_agent_events".into(),
        json!(match &debug_where {
            Some(predicate) => count(conn, "agent_events", predicate)?,
            None => 0,
        }),
    );

    if !dry_run {
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            &format!(
                "UPDATE agent_events SET full_context_json=NULL, context_delta_json='[]', context_hash=?1 WHERE {context_where}"
            ),
            params![empty_hash],
        )?;
        tx.execute(&format!("DELETE FROM jin10_items WHERE {jin10_where}"), [])?;
        tx.execute(
            &format!("DELETE FROM technical_bars WHERE {technical_where}"),
            [],
        )?;
        if let Some(predicate) = attention_where {
            tx.execute(
                &format!("DELETE FROM attention_ledger WHERE {predicate}"),
                [],
            )?;
        }
        if let Some(predicate) = debug_where {
            tx.execute(&format!("DELETE FROM agent_events WHERE {predicate}"), [])?;
        }
        tx.commit()?;
        if optimize {
            conn.execute_batch("PRAGMA optimize")?;
        }
    }

    Ok(json!({
        "dry_run": dry_run,
        "optimize": optimize && !dry_run,
        "affected": counts,
        "preserved_long_term": ["runs", "role_turn_summaries", "phase_summaries", "predictions", "outcomes", "memory_items", "memory_versions"],
    }))
}

pub fn wal_checkpoint(conn: &Connection, truncate: bool) -> Result<Value> {
    let mode = if truncate { "TRUNCATE" } else { "PASSIVE" };
    let sql = format!("PRAGMA wal_checkpoint({mode})");
    let (busy, log_frames, checkpointed_frames): (i64, i64, i64) =
        conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    Ok(json!({
        "mode": mode,
        "busy": busy,
        "log_frames": log_frames,
        "checkpointed_frames": checkpointed_frames,
    }))
}

pub fn vacuum(conn: &Connection) -> Result<()> {
    conn.execute_batch("VACUUM")?;
    Ok(())
}

pub fn core_query_plans(conn: &Connection) -> Result<Value> {
    let queries = [
        (
            "latest_run",
            "SELECT run_id FROM runs ORDER BY \"current_date\" DESC, created_at_ms DESC LIMIT 1",
        ),
        (
            "latest_agent_turn",
            "SELECT turn_id FROM agent_events WHERE run_id='__plan__' ORDER BY turn_number DESC LIMIT 1",
        ),
        (
            "run_ticker_summaries",
            "SELECT id FROM role_turn_summaries WHERE run_id='__plan__' AND ticker='QQQ' ORDER BY created_at_ms DESC",
        ),
        (
            "jin10_attention",
            "SELECT id FROM jin10_items ORDER BY latest_attention_score DESC, item_time_ms DESC LIMIT 20",
        ),
        (
            "expired_predictions",
            "SELECT id FROM predictions WHERE outcome_due_date <= '2099-01-01' ORDER BY outcome_due_date,id LIMIT 100",
        ),
        (
            "technical_close_before",
            "SELECT close FROM technical_bars WHERE ticker='QQQ' AND interval='daily' AND bar_time <= '2099-01-01' ORDER BY bar_time DESC LIMIT 1",
        ),
    ];
    let mut output = serde_json::Map::new();
    for (name, sql) in queries {
        if let Ok(plan) = explain(conn, sql) {
            output.insert(name.to_string(), json!(plan));
        }
    }
    Ok(Value::Object(output))
}

fn explain(conn: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
    let plan = stmt
        .query_map([], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(plan)
}

fn empty_identity_count(conn: &Connection) -> Result<i64> {
    let checks = [
        ("runs", "run_id"),
        ("agent_events", "turn_id"),
        ("agent_events", "run_id"),
        ("agent_events", "role"),
        ("role_turn_summaries", "run_id"),
        ("role_turn_summaries", "turn_id"),
        ("role_turn_summaries", "role"),
        ("attention_ledger", "subject_kind"),
        ("attention_ledger", "subject_id"),
    ];
    checks.iter().try_fold(0, |total, (table, column)| {
        if !table_exists(conn, table)? {
            return Ok(total);
        }
        let found: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE trim({column})=''"),
            [],
            |row| row.get(0),
        )?;
        Ok(total + found)
    })
}

fn count(conn: &Connection, table: &str, predicate: &str) -> Result<i64> {
    if !TABLES.contains(&table) {
        bail!("unsupported maintenance table {table}");
    }
    Ok(conn.query_row(
        &format!("SELECT COUNT(*) FROM {table} WHERE {predicate}"),
        [],
        |row| row.get(0),
    )?)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_xinfo({table})"))?;
    let found = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|name| name == column);
    Ok(found)
}

fn pragma_i64(conn: &Connection, name: &str) -> Result<i64> {
    Ok(conn.query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))?)
}

fn sha256_hex(value: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{connect, write::write_run_record, RunRecordInput};

    #[test]
    fn doctor_is_read_only_and_reports_health() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("doctor.sqlite");
        let mut conn = connect(&path).unwrap();
        write_run_record(
            &mut conn,
            &RunRecordInput {
                run_id: "doctor-run",
                current_date: "2026-01-01",
            },
        )
        .unwrap();
        drop(conn);
        let conn = open_read_only(&path).unwrap();
        let report = database_doctor(&conn).unwrap();
        assert_eq!(report["schema"]["version"], CURRENT_SCHEMA_VERSION);
        assert_eq!(report["quick_check"], "ok");
        assert_eq!(report["orphan_foreign_keys"], 0);
        assert_eq!(report["table_rows"]["runs"], 1);
    }

    #[test]
    fn cleanup_dry_run_does_not_mutate() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("cleanup.sqlite")).unwrap();
        let before = database_doctor(&conn).unwrap();
        let result = cleanup_database(&conn, &RetentionPolicy::default(), true, true).unwrap();
        let after = database_doctor(&conn).unwrap();
        assert_eq!(result["dry_run"], true);
        assert_eq!(before["table_rows"], after["table_rows"]);
    }
}
