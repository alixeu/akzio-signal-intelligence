use crate::schema::now_ms;
use anyhow::{bail, Context, Result};
use orchestrator_core::{read_technical_csv, storage_interval, TechnicalCsvRow};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::{collections::HashMap, path::Path};

/// Atomically replace one ticker/interval window with one row per technical bar.
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

    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM technical_bars WHERE ticker=?1 AND interval=?2",
        params![ticker, interval],
    )?;
    let imported_at_ms = now_ms();
    {
        let mut insert = tx.prepare_cached(
            r#"
            INSERT INTO technical_bars
                (ticker, interval, bar_time, close, values_json, imported_at_ms)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(ticker, interval, bar_time) DO UPDATE SET
                close = excluded.close,
                values_json = excluded.values_json,
                imported_at_ms = excluded.imported_at_ms
            "#,
        )?;
        for row in &rows {
            let mut values = row.values.clone();
            let close = values.remove("Close").or_else(|| values.remove("close"));
            insert.execute(params![
                ticker,
                interval,
                row.date,
                close,
                serde_json::to_string(&values)?,
                imported_at_ms,
            ])?;
        }
    }
    tx.commit()?;
    Ok(rows.len())
}

pub fn load_technical_series(
    conn: &Connection,
    ticker: &str,
    interval: &str,
) -> Result<Vec<TechnicalCsvRow>> {
    load_technical_range(conn, ticker, interval, None, None)
}

pub fn load_technical_range(
    conn: &Connection,
    ticker: &str,
    interval: &str,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<Vec<TechnicalCsvRow>> {
    let interval = storage_interval(interval)
        .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
    let ticker = ticker.trim().to_ascii_uppercase();
    let mut stmt = conn.prepare(
        r#"
        SELECT bar_time, close, values_json
        FROM technical_bars
        WHERE ticker=?1 AND interval=?2
          AND (?3 IS NULL OR bar_time >= ?3)
          AND (?4 IS NULL OR bar_time <= ?4)
        ORDER BY bar_time ASC
        "#,
    )?;
    let rows = stmt.query_map(params![ticker, interval, start, end], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<f64>>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    rows.map(|row| {
        let (date, close, raw) = row?;
        let parsed: Value = serde_json::from_str(&raw).context("invalid technical values_json")?;
        let mut values = parsed
            .as_object()
            .context("technical values_json must be an object")?
            .iter()
            .filter_map(|(key, value)| {
                value
                    .as_f64()
                    .filter(|number| number.is_finite())
                    .map(|number| (key.clone(), number))
            })
            .collect::<HashMap<_, _>>();
        if let Some(close) = close.filter(|value| value.is_finite()) {
            values.insert("Close".to_string(), close);
        }
        if values.is_empty() {
            bail!("technical row {date} has no finite values");
        }
        Ok(TechnicalCsvRow { date, values })
    })
    .collect()
}

pub fn latest_technical_bar(
    conn: &Connection,
    ticker: &str,
    interval: &str,
) -> Result<Option<TechnicalCsvRow>> {
    let interval = storage_interval(interval)
        .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
    let ticker = ticker.trim().to_ascii_uppercase();
    let row = conn
        .query_row(
            r#"SELECT bar_time, close, values_json FROM technical_bars
               WHERE ticker=?1 AND interval=?2 ORDER BY bar_time DESC LIMIT 1"#,
            params![ticker, interval],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<f64>>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    row.map(|(date, close, raw)| technical_row(date, close, &raw))
        .transpose()
}

pub fn close_on_or_before(
    conn: &Connection,
    ticker: &str,
    interval: &str,
    date: &str,
) -> Result<Option<(String, f64)>> {
    close_near_date(conn, ticker, interval, date, true)
}

pub fn close_on_or_after(
    conn: &Connection,
    ticker: &str,
    interval: &str,
    date: &str,
) -> Result<Option<(String, f64)>> {
    close_near_date(conn, ticker, interval, date, false)
}

fn close_near_date(
    conn: &Connection,
    ticker: &str,
    interval: &str,
    date: &str,
    before: bool,
) -> Result<Option<(String, f64)>> {
    let interval = storage_interval(interval)
        .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
    let comparator = if before { "<=" } else { ">=" };
    let order = if before { "DESC" } else { "ASC" };
    let sql = format!(
        "SELECT bar_time, close FROM technical_bars \
         WHERE ticker=?1 AND interval=?2 AND bar_time {comparator} ?3 AND close IS NOT NULL \
         ORDER BY bar_time {order} LIMIT 1"
    );
    Ok(conn
        .query_row(
            &sql,
            params![ticker.trim().to_ascii_uppercase(), interval, date],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?)),
        )
        .optional()?)
}

pub fn technical_row_count(conn: &Connection, interval: Option<&str>) -> Result<i64> {
    match interval {
        Some(interval) => {
            let interval = storage_interval(interval)
                .ok_or_else(|| anyhow::anyhow!("unsupported technical interval {interval}"))?;
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM technical_bars WHERE interval=?1",
                [interval],
                |row| row.get(0),
            )?)
        }
        None => Ok(conn.query_row("SELECT COUNT(*) FROM technical_bars", [], |row| row.get(0))?),
    }
}

fn technical_row(date: String, close: Option<f64>, raw: &str) -> Result<TechnicalCsvRow> {
    let parsed: Value = serde_json::from_str(raw).context("invalid technical values_json")?;
    let mut values = parsed
        .as_object()
        .context("technical values_json must be an object")?
        .iter()
        .filter_map(|(key, value)| value.as_f64().map(|number| (key.clone(), number)))
        .collect::<HashMap<_, _>>();
    if let Some(close) = close {
        values.insert("Close".to_string(), close);
    }
    Ok(TechnicalCsvRow { date, values })
}
