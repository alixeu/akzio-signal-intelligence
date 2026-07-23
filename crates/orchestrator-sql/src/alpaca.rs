use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn record_account_snapshot(
    conn: &Connection,
    run_id: Option<&str>,
    phase: i64,
    payload: &Value,
) -> Result<i64> {
    let positions = payload
        .get("positions")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let cash = payload.get("cash").and_then(Value::as_f64);
    let observed_positions_value = positions
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|position| {
            Some(position.get("quantity")?.as_f64()? * position.get("current_price")?.as_f64()?)
        })
        .sum::<f64>();
    let observed_equity = cash.map(|cash| cash + observed_positions_value);
    conn.execute(
        r#"
        INSERT INTO ai4trade_account_snapshots
            (run_id,phase,captured_at_ms,cash,points,unrealized_pnl,observed_equity,
             positions_json,source_json)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
        "#,
        params![
            run_id,
            phase,
            crate::schema::now_ms(),
            cash,
            payload.get("points").and_then(Value::as_f64),
            payload.get("unrealized_pnl").and_then(Value::as_f64),
            observed_equity,
            serde_json::to_string(&positions)?,
            serde_json::to_string(payload)?,
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

pub struct ExecutionRecord<'a> {
    pub run_id: Option<&'a str>,
    pub ticker: &'a str,
    pub action: &'a str,
    pub quantity: f64,
    pub requested_price: f64,
    pub executed_at: &'a str,
    pub response: &'a Value,
}

pub fn record_exact_execution(conn: &Connection, input: &ExecutionRecord<'_>) -> Result<i64> {
    let signal_id = input
        .response
        .get("id")
        .or_else(|| input.response.get("signal_id"))
        .and_then(value_as_id)
        .context("trade response is missing an Alpaca order id or legacy signal_id")?;
    let executed_price = input
        .response
        .get("filled_avg_price")
        .or_else(|| input.response.get("price"))
        .or_else(|| input.response.get("executed_price"))
        .and_then(value_as_f64);
    let executed_at_ms = input
        .response
        .get("filled_at")
        .or_else(|| input.response.get("submitted_at"))
        .and_then(Value::as_str)
        .and_then(parse_time_ms)
        .or_else(|| parse_time_ms(input.executed_at))
        .unwrap_or_else(crate::schema::now_ms);
    let raw = serde_json::to_string(input.response)?;
    let response_hash = format!("{:x}", Sha256::digest(raw.as_bytes()));
    conn.execute(
        r#"
        INSERT INTO ai4trade_executions
            (signal_id,run_id,ticker,source_phase,action,quantity,requested_price,
             executed_price,executed_at_ms,attribution_method,attribution_confidence,
             response_hash,raw_json,created_at_ms)
        VALUES (?1,?2,?3,6,?4,?5,?6,?7,?8,'exact_signal',1.0,?9,?10,?11)
        ON CONFLICT(signal_id) DO UPDATE SET
            run_id=COALESCE(excluded.run_id,ai4trade_executions.run_id),
            attribution_method='exact_signal',
            attribution_confidence=1.0,
            raw_json=excluded.raw_json
        "#,
        params![
            signal_id,
            input.run_id,
            input.ticker.trim().to_ascii_uppercase(),
            input.action,
            input.quantity,
            input.requested_price,
            executed_price,
            executed_at_ms,
            response_hash,
            raw,
            crate::schema::now_ms(),
        ],
    )?;
    Ok(conn.query_row(
        "SELECT id FROM ai4trade_executions WHERE signal_id=?1",
        [signal_id],
        |row| row.get(0),
    )?)
}

pub fn import_legacy_executions(
    conn: &Connection,
    signals: &Value,
    _current_run_id: Option<&str>,
) -> Result<usize> {
    let items = signals
        .get("signals")
        .and_then(Value::as_array)
        .or_else(|| signals.as_array())
        .cloned()
        .unwrap_or_default();
    let mut imported = 0;
    for signal in items {
        let Some(signal_id) = signal.get("id").and_then(value_as_id) else {
            continue;
        };
        let action = signal
            .get("action")
            .or_else(|| signal.get("side"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        if !matches!(action.as_str(), "buy" | "sell" | "short" | "cover") {
            continue;
        }
        let ticker = signal
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_ascii_uppercase();
        let quantity = signal
            .get("quantity")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        if ticker.is_empty() || quantity <= 0.0 {
            continue;
        }
        let executed_at_ms = signal
            .get("executed_at")
            .and_then(Value::as_str)
            .and_then(parse_time_ms)
            .or_else(|| {
                signal
                    .get("timestamp")
                    .and_then(Value::as_i64)
                    .map(|value| value * 1000)
            })
            .unwrap_or_else(crate::schema::now_ms);
        let (run_id, method, confidence) =
            infer_run_attribution(conn, &ticker, &action, executed_at_ms)?;
        let raw = serde_json::to_string(&signal)?;
        let response_hash = format!("{:x}", Sha256::digest(raw.as_bytes()));
        imported += conn.execute(
            r#"
            INSERT INTO ai4trade_executions
                (signal_id,run_id,ticker,source_phase,action,quantity,requested_price,
                 executed_price,executed_at_ms,attribution_method,attribution_confidence,
                 response_hash,raw_json,created_at_ms)
            VALUES (?1,?2,?3,6,?4,?5,NULL,?6,?7,?8,?9,?10,?11,?12)
            ON CONFLICT(signal_id) DO NOTHING
            "#,
            params![
                signal_id,
                run_id,
                ticker,
                action,
                quantity,
                signal
                    .get("price")
                    .or_else(|| signal.get("entry_price"))
                    .and_then(Value::as_f64),
                executed_at_ms,
                method,
                confidence,
                response_hash,
                raw,
                crate::schema::now_ms(),
            ],
        )?;
    }
    Ok(imported)
}

fn infer_run_attribution(
    conn: &Connection,
    ticker: &str,
    action: &str,
    executed_at_ms: i64,
) -> Result<(Option<String>, &'static str, f64)> {
    let four_hours_ms = 4 * 60 * 60 * 1000_i64;
    let inferred = conn
        .query_row(
            r#"
            SELECT run_id
            FROM decision_snapshots
            WHERE ticker=?1
              AND lower(action)=?2
              AND abs(created_at_ms-?3) <= ?4
            ORDER BY abs(created_at_ms-?3) ASC
            LIMIT 1
            "#,
            params![ticker, action, executed_at_ms, four_hours_ms],
            |row| row.get::<_, String>(0),
        )
        .ok();
    if let Some(run_id) = inferred {
        Ok((Some(run_id), "time_window", 0.5))
    } else {
        Ok((None, "legacy_unattributed", 0.2))
    }
}

fn value_as_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .or_else(|| value.as_i64().map(|value| value.to_string()))
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn parse_time_ms(value: &str) -> Option<i64> {
    if value.eq_ignore_ascii_case("now") {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.timestamp_millis())
}
