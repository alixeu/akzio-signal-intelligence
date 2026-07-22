use anyhow::{bail, Context, Result};
use orchestrator_core::{read_technical_csv, storage_interval, TechnicalCsvRow};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{collections::HashMap, path::Path};

/// Replace one ticker/interval series with a single compact SQLite row.
/// The CSV is an ingestion interchange only; live workflow consumers read this table.
pub fn import_technical_csv(
    conn: &mut Connection,
    ticker: &str,
    interval: &str,
    path: &Path,
) -> Result<usize> {
    let rows = read_technical_csv(path)
        .with_context(|| format!("failed to read technical import {}", path.display()))?;
    if rows.is_empty() {
        bail!("technical import is empty for {ticker} @ {interval}");
    }
    let interval = storage_interval(interval)
        .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
    let ticker = ticker.trim().to_ascii_uppercase();
    if ticker.is_empty() {
        bail!("technical import ticker is empty");
    }
    let rows_json = rows
        .iter()
        .map(|row| json!({"date": row.date, "values": row.values}))
        .collect::<Vec<_>>();
    let as_of = rows.last().map(|row| row.date.as_str()).unwrap_or_default();
    let tx = conn.transaction()?;
    tx.execute(
        r#"
        INSERT INTO technical_series
            (ticker, interval, as_of, row_count, rows_json, imported_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(ticker, interval) DO UPDATE SET
            as_of = excluded.as_of,
            row_count = excluded.row_count,
            rows_json = excluded.rows_json,
            imported_at = excluded.imported_at
        "#,
        params![
            ticker,
            interval,
            as_of,
            rows.len() as i64,
            serde_json::to_string(&rows_json)?,
            chrono::Utc::now().timestamp()
        ],
    )?;
    tx.commit()?;
    Ok(rows.len())
}

pub fn load_technical_series(
    conn: &Connection,
    ticker: &str,
    interval: &str,
) -> Result<Vec<TechnicalCsvRow>> {
    let interval = storage_interval(interval)
        .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
    let raw = conn
        .query_row(
            "SELECT rows_json FROM technical_series WHERE ticker = ?1 AND interval = ?2",
            params![ticker.trim().to_ascii_uppercase(), interval],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let values: Vec<Value> = serde_json::from_str(&raw).context("invalid technical rows_json")?;
    values
        .into_iter()
        .map(|row| {
            let date = row
                .get("date")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| anyhow::anyhow!("technical row missing date"))?
                .to_string();
            let values = row
                .get("values")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow::anyhow!("technical row missing values"))?
                .iter()
                .filter_map(|(key, value)| {
                    value
                        .as_f64()
                        .filter(|number| number.is_finite())
                        .map(|number| (key.clone(), number))
                })
                .collect::<HashMap<_, _>>();
            if values.is_empty() {
                bail!("technical row {date} has no finite values");
            }
            Ok(TechnicalCsvRow { date, values })
        })
        .collect()
}

pub fn technical_row_count(conn: &Connection, interval: Option<&str>) -> Result<i64> {
    match interval {
        Some(interval) => {
            let interval = storage_interval(interval)
                .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
            Ok(conn.query_row(
                "SELECT COALESCE(SUM(row_count), 0) FROM technical_series WHERE interval = ?1",
                [interval],
                |row| row.get(0),
            )?)
        }
        None => Ok(conn.query_row(
            "SELECT COALESCE(SUM(row_count), 0) FROM technical_series",
            [],
            |row| row.get(0),
        )?),
    }
}

use rusqlite::OptionalExtension;
