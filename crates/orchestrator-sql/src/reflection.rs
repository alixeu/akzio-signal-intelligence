use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use uuid::Uuid;

use crate::{
    outcome::{latest_close_on_or_before, upsert_outcome, OutcomeInput},
    prediction::expired_unscored_predictions,
    technical_close_after_trading_days, technical_minimum_close_between,
};

pub const PREDICTION_HORIZON_TRADING_DAYS: i64 = 3;

#[derive(Debug, Clone, Copy)]
pub struct ReflectionThresholds {
    pub loss_return: f64,
    pub excess_return: f64,
    pub high_confidence: f64,
    pub calibration_error: f64,
    pub repeated_error_count: i64,
}

impl Default for ReflectionThresholds {
    fn default() -> Self {
        Self {
            loss_return: -0.02,
            excess_return: -0.015,
            high_confidence: 0.70,
            calibration_error: 0.40,
            repeated_error_count: 2,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ReflectionScoreSummary {
    pub scored: usize,
    pub queued: usize,
    pub deep: usize,
    pub skipped: usize,
    pub errors: usize,
}

#[derive(Debug, Clone)]
pub struct DecisionSnapshotInput {
    pub run_id: String,
    pub ticker: String,
    pub action: String,
    pub decision_date: String,
    pub position_id: Option<String>,
    pub long_probability: Option<f64>,
    pub short_probability: Option<f64>,
    pub decision_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingReflectionTask {
    pub task_id: i64,
    pub source_run_id: String,
    pub ticker: String,
    pub reflection_level: String,
    pub decision: Value,
    pub outcome: Value,
}

pub fn upsert_decision_snapshot(conn: &Connection, input: &DecisionSnapshotInput) -> Result<i64> {
    for (name, probability) in [
        ("long_probability", input.long_probability),
        ("short_probability", input.short_probability),
    ] {
        if probability.is_some_and(|value| !(0.0..=1.0).contains(&value)) {
            bail!("{name} must be between 0 and 1");
        }
    }
    conn.execute(
        r#"
        INSERT INTO decision_snapshots
            (run_id,ticker,decision_date,source_phase,action,position_id,
             prediction_horizon_trading_days,long_probability,short_probability,
             decision_json,created_at_ms)
        VALUES (?1,?2,?3,6,?4,?5,?6,?7,?8,?9,?10)
        ON CONFLICT(run_id,ticker) DO UPDATE SET
            decision_date=excluded.decision_date,
            action=excluded.action,
            position_id=excluded.position_id,
            prediction_horizon_trading_days=excluded.prediction_horizon_trading_days,
            long_probability=excluded.long_probability,
            short_probability=excluded.short_probability,
            decision_json=excluded.decision_json
        "#,
        params![
            input.run_id,
            input.ticker.trim().to_ascii_uppercase(),
            input.decision_date,
            input.action,
            input.position_id,
            PREDICTION_HORIZON_TRADING_DAYS,
            input.long_probability,
            input.short_probability,
            serde_json::to_string(&input.decision_json)?,
            crate::schema::now_ms(),
        ],
    )?;
    Ok(conn.query_row(
        "SELECT id FROM decision_snapshots WHERE run_id=?1 AND ticker=?2",
        params![input.run_id, input.ticker.trim().to_ascii_uppercase()],
        |row| row.get(0),
    )?)
}

#[allow(clippy::too_many_arguments)]
pub fn score_mature_predictions(
    conn: &Connection,
    as_of: &str,
    interval: &str,
    limit: usize,
    thresholds: ReflectionThresholds,
    current_run_id: Option<&str>,
    reflection_version: &str,
    task_limit: usize,
) -> Result<ReflectionScoreSummary> {
    let predictions = expired_unscored_predictions(conn, as_of, limit)?;
    let mut summary = ReflectionScoreSummary::default();

    for prediction in predictions {
        let Some((_, baseline_close)) = latest_close_on_or_before(
            conn,
            &prediction.ticker,
            &prediction.prediction_date,
            interval,
        )?
        else {
            summary.skipped += 1;
            continue;
        };
        let Some((outcome_date, outcome_close)) = technical_close_after_trading_days(
            conn,
            &prediction.ticker,
            interval,
            &prediction.prediction_date,
            prediction.window_days,
            Some(as_of),
        )?
        else {
            summary.skipped += 1;
            continue;
        };

        let ticker_return = (outcome_close - baseline_close) / baseline_close;
        let predicted_long = prediction.long_probability >= prediction.short_probability;
        let predicted_return = if predicted_long {
            ticker_return
        } else {
            -ticker_return
        };
        let actual_long = ticker_return >= 0.0;
        let direction_correct = predicted_long == actual_long;
        let probability_error = prediction.long_probability - if actual_long { 1.0 } else { 0.0 };
        let confidence = prediction
            .long_probability
            .max(prediction.short_probability);

        upsert_outcome(
            conn,
            &OutcomeInput {
                prediction_id: prediction.id,
                run_id: prediction.run_id.clone(),
                ticker: prediction.ticker.clone(),
                prediction_date: prediction.prediction_date.clone(),
                outcome_date: outcome_date.clone(),
                window_days: prediction.window_days,
                baseline_close,
                outcome_close,
                actual_return: ticker_return,
                direction_correct,
                probability_error,
            },
        )?;

        let snapshot = ensure_decision_snapshot(
            conn,
            &prediction.run_id,
            &prediction.ticker,
            &prediction.prediction_date,
            predicted_long,
            prediction.long_probability,
            prediction.short_probability,
            &prediction.market_regime_json,
        )?;
        let snapshot_action: String = conn.query_row(
            "SELECT action FROM decision_snapshots WHERE id=?1",
            [snapshot],
            |row| row.get(0),
        )?;
        let execution = execution_context(conn, &prediction.run_id, &prediction.ticker)?;
        let decision_proxy_return =
            decision_return(&snapshot_action, ticker_return, predicted_return);
        let actual_return = execution
            .as_ref()
            .and_then(|execution| execution.platform_return)
            .unwrap_or(decision_proxy_return);
        let counterfactual_return = snapshot_action
            .eq_ignore_ascii_case("hold")
            .then_some(ticker_return);
        let qqq_return = benchmark_return(
            conn,
            "QQQ",
            interval,
            &prediction.prediction_date,
            prediction.window_days,
            as_of,
        )?;
        let observed_max_drawdown = technical_minimum_close_between(
            conn,
            &prediction.ticker,
            interval,
            &prediction.prediction_date,
            &outcome_date,
        )?
        .map(|low| ((low - baseline_close) / baseline_close).min(0.0));
        let excess_return = actual_return - ticker_return;
        let mut triggers = reflection_triggers(
            actual_return,
            excess_return,
            direction_correct,
            confidence,
            probability_error.abs(),
            false,
            thresholds,
        );
        if !direction_correct {
            let prior_direction_errors: i64 = conn.query_row(
                "SELECT COUNT(*) FROM decision_outcomes WHERE ticker=?1 AND direction_correct=0",
                [&prediction.ticker],
                |row| row.get(0),
            )?;
            if prior_direction_errors + 1 >= thresholds.repeated_error_count {
                triggers.push("repeated_direction_error".to_string());
            }
        }
        let outcome_id = insert_decision_outcome(
            conn,
            snapshot,
            prediction.id,
            &prediction.run_id,
            &prediction.ticker,
            &outcome_date,
            actual_return,
            counterfactual_return,
            ticker_return,
            excess_return,
            direction_correct,
            probability_error.abs(),
            observed_max_drawdown,
            qqq_return,
            &triggers,
            execution.as_ref(),
        )?;
        summary.scored += 1;

        let _ = outcome_id;
    }
    if let Some(current_run_id) = current_run_id {
        let (queued, deep) =
            enqueue_unassigned_outcomes(conn, current_run_id, reflection_version, task_limit)?;
        summary.queued = queued;
        summary.deep = deep;
    }
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn ensure_decision_snapshot(
    conn: &Connection,
    run_id: &str,
    ticker: &str,
    prediction_date: &str,
    predicted_long: bool,
    long_probability: f64,
    short_probability: f64,
    market_regime: &Value,
) -> Result<i64> {
    let existing = conn
        .query_row(
            "SELECT id FROM decision_snapshots WHERE run_id=?1 AND ticker=?2",
            params![run_id, ticker],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(id) = existing {
        return Ok(id);
    }
    upsert_decision_snapshot(
        conn,
        &DecisionSnapshotInput {
            run_id: run_id.to_string(),
            ticker: ticker.to_string(),
            action: if predicted_long { "long" } else { "short" }.to_string(),
            decision_date: prediction_date.to_string(),
            position_id: None,
            long_probability: Some(long_probability),
            short_probability: Some(short_probability),
            decision_json: json!({
                "source": "legacy_prediction",
                "market_regime": market_regime
            }),
        },
    )
}

fn decision_return(action: &str, ticker_return: f64, predicted_return: f64) -> f64 {
    match action.trim().to_ascii_lowercase().as_str() {
        "hold" | "wait" => 0.0,
        "short" => -ticker_return,
        "long" | "buy" | "cover" | "sell" => predicted_return,
        _ => predicted_return,
    }
}

#[derive(Debug)]
struct ExecutionContext {
    attribution_method: String,
    attribution_confidence: f64,
    platform_return: Option<f64>,
    raw: Value,
}

fn execution_context(
    conn: &Connection,
    run_id: &str,
    ticker: &str,
) -> Result<Option<ExecutionContext>> {
    let row = conn
        .query_row(
            r#"
            SELECT attribution_method,attribution_confidence,requested_price,
                   executed_price,quantity,raw_json
            FROM ai4trade_executions
            WHERE run_id=?1 AND ticker=?2
            ORDER BY executed_at_ms DESC LIMIT 1
            "#,
            params![run_id, ticker],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, f64>(1)?,
                    row.get::<_, Option<f64>>(2)?,
                    row.get::<_, Option<f64>>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()?;
    Ok(row.map(
        |(method, confidence, requested_price, executed_price, quantity, raw)| {
            let raw: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
            let platform_return = raw
                .get("return_pct")
                .and_then(Value::as_f64)
                .map(|value| {
                    if value.abs() > 1.0 {
                        value / 100.0
                    } else {
                        value
                    }
                })
                .or_else(|| {
                    let pnl = raw.get("pnl").and_then(Value::as_f64)?;
                    let price = executed_price.or(requested_price)?;
                    let notional = price * quantity;
                    (notional > 0.0).then_some(pnl / notional)
                });
            ExecutionContext {
                attribution_method: method,
                attribution_confidence: confidence,
                platform_return,
                raw,
            }
        },
    ))
}

#[allow(clippy::too_many_arguments)]
fn insert_decision_outcome(
    conn: &Connection,
    snapshot_id: i64,
    prediction_id: i64,
    source_run_id: &str,
    ticker: &str,
    outcome_date: &str,
    actual_return: f64,
    counterfactual_return: Option<f64>,
    ticker_return: f64,
    excess_return: f64,
    direction_correct: bool,
    calibration_error: f64,
    observed_max_drawdown: Option<f64>,
    qqq_return: Option<f64>,
    triggers: &[String],
    execution: Option<&ExecutionContext>,
) -> Result<i64> {
    let attribution_method = execution
        .map(|value| value.attribution_method.as_str())
        .unwrap_or("decision_snapshot");
    let attribution_confidence = execution
        .map(|value| value.attribution_confidence)
        .unwrap_or(1.0);
    conn.execute(
        r#"
        INSERT INTO decision_outcomes
            (decision_snapshot_id,prediction_id,source_run_id,ticker,outcome_date,
             actual_return,counterfactual_return,benchmark_return,excess_return,
             direction_correct,calibration_error,observed_max_drawdown,
             attribution_method,attribution_confidence,metrics_json,created_at_ms)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,
                ?13,?14,?15,?16)
        ON CONFLICT(decision_snapshot_id) DO NOTHING
        "#,
        params![
            snapshot_id,
            prediction_id,
            source_run_id,
            ticker,
            outcome_date,
            actual_return,
            counterfactual_return,
            ticker_return,
            excess_return,
            direction_correct as i64,
            calibration_error,
            observed_max_drawdown,
            attribution_method,
            attribution_confidence,
            serde_json::to_string(&json!({
                "ticker_return": ticker_return,
                "account_benchmark_ticker": "QQQ",
                "account_benchmark_return": qqq_return,
                "return_source": if execution.and_then(|value| value.platform_return).is_some() {
                    "ai4trade"
                } else {
                    "three_trading_day_decision_proxy"
                },
                "ai4trade_execution": execution.map(|value| value.raw.clone()),
                "trigger_reasons": triggers,
                "observed_max_drawdown_note": "Observed from stored daily closes during the evaluation window; not intraday drawdown."
            }))?,
            crate::schema::now_ms(),
        ],
    )?;
    Ok(conn.query_row(
        "SELECT id FROM decision_outcomes WHERE decision_snapshot_id=?1",
        [snapshot_id],
        |row| row.get(0),
    )?)
}

fn benchmark_return(
    conn: &Connection,
    ticker: &str,
    interval: &str,
    prediction_date: &str,
    horizon: i64,
    as_of: &str,
) -> Result<Option<f64>> {
    let Some((_, baseline)) = latest_close_on_or_before(conn, ticker, prediction_date, interval)?
    else {
        return Ok(None);
    };
    let Some((_, outcome)) = technical_close_after_trading_days(
        conn,
        ticker,
        interval,
        prediction_date,
        horizon,
        Some(as_of),
    )?
    else {
        return Ok(None);
    };
    Ok(Some((outcome - baseline) / baseline))
}

fn reflection_triggers(
    actual_return: f64,
    excess_return: f64,
    direction_correct: bool,
    confidence: f64,
    calibration_error: f64,
    risk_violation: bool,
    thresholds: ReflectionThresholds,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if actual_return < thresholds.loss_return {
        reasons.push("absolute_loss".to_string());
    }
    if excess_return < thresholds.excess_return {
        reasons.push("underperformed_ticker_benchmark".to_string());
    }
    if !direction_correct {
        reasons.push("direction_wrong".to_string());
    }
    if confidence >= thresholds.high_confidence && calibration_error >= thresholds.calibration_error
    {
        reasons.push("confidence_outcome_mismatch".to_string());
    }
    if risk_violation {
        reasons.push("risk_violation".to_string());
    }
    reasons
}

fn enqueue_reflection_task(
    conn: &Connection,
    decision_outcome_id: i64,
    current_run_id: &str,
    reflection_version: &str,
    level: &str,
    priority: i64,
) -> Result<bool> {
    let now = crate::schema::now_ms();
    Ok(conn.execute(
        r#"
        INSERT INTO reflection_tasks
            (decision_outcome_id,current_run_id,reflection_version,reflection_level,
             priority,status,attempt_count,created_at_ms,updated_at_ms)
        VALUES (?1,?2,?3,?4,?5,'pending',0,?6,?6)
        ON CONFLICT(decision_outcome_id,reflection_version) DO NOTHING
        "#,
        params![
            decision_outcome_id,
            current_run_id,
            reflection_version,
            level,
            priority,
            now
        ],
    )? > 0)
}

fn enqueue_unassigned_outcomes(
    conn: &Connection,
    current_run_id: &str,
    reflection_version: &str,
    limit: usize,
) -> Result<(usize, usize)> {
    let mut stmt = conn.prepare(
        r#"
        SELECT o.id,o.metrics_json,
               CASE
                 WHEN json_array_length(
                   COALESCE(json_extract(o.metrics_json,'$.trigger_reasons'),json('[]'))
                 ) > 0 THEN 1
                 ELSE 0
               END AS is_deep
        FROM decision_outcomes o
        WHERE NOT EXISTS (
            SELECT 1 FROM reflection_tasks t
            WHERE t.decision_outcome_id=o.id AND t.reflection_version=?1
        )
        ORDER BY is_deep DESC,o.created_at_ms ASC
        LIMIT ?2
        "#,
    )?;
    let rows = stmt
        .query_map(params![reflection_version, limit.max(1) as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut queued = 0;
    let mut deep_count = 0;
    for (outcome_id, metrics, deep) in rows {
        let metrics: Value = serde_json::from_str(&metrics).unwrap_or(Value::Null);
        let triggers_non_empty = metrics
            .get("trigger_reasons")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty());
        let deep = deep || triggers_non_empty;
        if enqueue_reflection_task(
            conn,
            outcome_id,
            current_run_id,
            reflection_version,
            if deep { "deep" } else { "routine" },
            if deep { 100 } else { 10 },
        )? {
            queued += 1;
            deep_count += usize::from(deep);
        }
    }
    Ok((queued, deep_count))
}

pub fn pending_reflection_tasks(
    conn: &Connection,
    current_run_id: &str,
    limit: usize,
) -> Result<Vec<PendingReflectionTask>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT t.id,t.reflection_level,o.source_run_id,o.ticker,
               d.action,d.position_id,d.long_probability,d.short_probability,d.decision_json,
               o.outcome_date,o.actual_return,o.counterfactual_return,o.benchmark_return,
               o.excess_return,o.direction_correct,o.calibration_error,
               o.observed_max_drawdown,o.attribution_method,o.attribution_confidence,o.metrics_json
        FROM reflection_tasks t
        JOIN decision_outcomes o ON o.id=t.decision_outcome_id
        JOIN decision_snapshots d ON d.id=o.decision_snapshot_id
        WHERE t.status IN ('pending','running') AND t.current_run_id=?1
        ORDER BY t.priority DESC,t.created_at_ms ASC
        LIMIT ?2
        "#,
    )?;
    let rows = stmt.query_map(params![current_run_id, limit.max(1) as i64], |row| {
        let mut decision: Value =
            serde_json::from_str(&row.get::<_, String>(8)?).unwrap_or(Value::Null);
        decision["action"] = json!(row.get::<_, String>(4)?);
        decision["position_id"] = json!(row.get::<_, Option<String>>(5)?);
        decision["long_probability"] = json!(row.get::<_, Option<f64>>(6)?);
        decision["short_probability"] = json!(row.get::<_, Option<f64>>(7)?);
        let metrics: Value =
            serde_json::from_str(&row.get::<_, String>(19)?).unwrap_or(Value::Null);
        Ok(PendingReflectionTask {
            task_id: row.get(0)?,
            reflection_level: row.get(1)?,
            source_run_id: row.get(2)?,
            ticker: row.get(3)?,
            decision,
            outcome: json!({
                "outcome_date": row.get::<_, String>(9)?,
                "actual_return": row.get::<_, f64>(10)?,
                "counterfactual_return": row.get::<_, Option<f64>>(11)?,
                "ticker_benchmark_return": row.get::<_, Option<f64>>(12)?,
                "excess_return": row.get::<_, Option<f64>>(13)?,
                "direction_correct": row.get::<_, bool>(14)?,
                "calibration_error": row.get::<_, f64>(15)?,
                "observed_max_drawdown": row.get::<_, Option<f64>>(16)?,
                "attribution_method": row.get::<_, String>(17)?,
                "attribution_confidence": row.get::<_, f64>(18)?,
                "metrics": metrics
            }),
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn set_reflection_task_status(
    conn: &Connection,
    task_id: i64,
    status: &str,
    error: Option<&str>,
) -> Result<()> {
    if !matches!(status, "pending" | "running" | "completed" | "failed") {
        bail!("unsupported reflection task status {status}");
    }
    let changed = conn.execute(
        r#"
        UPDATE reflection_tasks
        SET status=?1,
            attempt_count=attempt_count + CASE WHEN ?1='running' THEN 1 ELSE 0 END,
            last_error=?2,
            updated_at_ms=?3
        WHERE id=?4
        "#,
        params![status, error, crate::schema::now_ms(), task_id],
    )?;
    if changed == 0 {
        bail!("reflection task {task_id} does not exist");
    }
    Ok(())
}

pub fn reflection_source_context(conn: &Connection, task_id: i64) -> Result<Value> {
    let row = conn
        .query_row(
            r#"
            SELECT t.current_run_id,o.source_run_id
            FROM reflection_tasks t
            JOIN decision_outcomes o ON o.id=t.decision_outcome_id
            WHERE t.id=?1
            "#,
            [task_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .with_context(|| format!("reflection task {task_id} not found"))?;
    let task = pending_reflection_tasks(conn, &row.0, 100)?
        .into_iter()
        .find(|task| task.task_id == task_id)
        .with_context(|| format!("pending reflection task {task_id} not found"))?;
    let summaries = crate::list_phase_summaries(conn, &row.1, 8, None)?;
    let details = (1..=7)
        .map(|phase| crate::list_phase_details_for_phase(conn, &row.1, phase))
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "task": task,
        "phase_summaries": summaries,
        "phase_summary_details_by_phase": details,
        "source_policy": "Only this task's source_run_id is readable."
    }))
}

pub fn read_experience(
    conn: &Connection,
    phase: i64,
    ticker: Option<&str>,
    limit: usize,
) -> Result<Value> {
    if !(1..=6).contains(&phase) {
        bail!("experience retrieval phase must be 1-6");
    }
    let ticker = ticker.unwrap_or("").trim().to_ascii_uppercase();
    let limit = limit.clamp(1, 50) as i64;

    let mut active_stmt = conn.prepare(
        r#"
        SELECT i.memory_id,i.ticker,i.memory_type,i.confidence,i.quality_score,
               i.sample_count,i.source_phase,i.applies_to_phases_json,i.pattern_key,
               i.experience_level,v.summary
        FROM memory_items i
        JOIN memory_versions v ON v.version_id=i.current_version_id
        WHERE i.status='active'
          AND (?1='' OR i.ticker=?1 OR i.ticker='__ALL__')
          AND (
            json_array_length(i.applies_to_phases_json)=0
            OR EXISTS (
                SELECT 1 FROM json_each(i.applies_to_phases_json)
                WHERE CAST(value AS INTEGER)=?2
            )
          )
        ORDER BY i.quality_score DESC,i.updated_at_ms DESC
        LIMIT ?3
        "#,
    )?;
    let active = active_stmt
        .query_map(params![ticker, phase, limit], |row| {
            Ok(json!({
                "experience_id": row.get::<_, String>(0)?,
                "ticker": row.get::<_, String>(1)?,
                "experience_type": row.get::<_, String>(2)?,
                "confidence": row.get::<_, f64>(3)?,
                "quality_score": row.get::<_, f64>(4)?,
                "sample_count": row.get::<_, i64>(5)?,
                "source_phase": row.get::<_, i64>(6)?,
                "applies_to_phases": serde_json::from_str::<Value>(&row.get::<_, String>(7)?).unwrap_or_else(|_| json!([])),
                "pattern_key": row.get::<_, String>(8)?,
                "level": row.get::<_, String>(9)?,
                "summary": row.get::<_, String>(10)?,
                "policy": "rebuttable"
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut episode_stmt = conn.prepare(
        r#"
        SELECT reflection_id,ticker,source_phase,applies_to_phases_json,
               experience_type,pattern_key,finding,recommendation,confidence,
               attribution_confidence,created_at_ms
        FROM reflection_episodes
        WHERE reusable=1
          AND (?1='' OR ticker=?1)
          AND EXISTS (
              SELECT 1 FROM json_each(applies_to_phases_json)
              WHERE CAST(value AS INTEGER)=?2
          )
        ORDER BY created_at_ms DESC
        LIMIT ?3
        "#,
    )?;
    let recent = episode_stmt
        .query_map(params![ticker, phase, limit], |row| {
            Ok(json!({
                "experience_id": row.get::<_, String>(0)?,
                "ticker": row.get::<_, String>(1)?,
                "source_phase": row.get::<_, i64>(2)?,
                "applies_to_phases": serde_json::from_str::<Value>(&row.get::<_, String>(3)?).unwrap_or_else(|_| json!([])),
                "experience_type": row.get::<_, String>(4)?,
                "pattern_key": row.get::<_, String>(5)?,
                "finding": row.get::<_, String>(6)?,
                "recommendation": row.get::<_, String>(7)?,
                "confidence": row.get::<_, f64>(8)?,
                "attribution_confidence": row.get::<_, f64>(9)?,
                "level": "recent_episode",
                "policy": "low_weight_rebuttable"
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut repeated_stmt = conn.prepare(
        r#"
        SELECT id,scope_value,experience_type,source_phase,applies_to_phases_json,
               pattern_key,finding,recommendation,sample_count,confidence,
               attribution_confidence,experience_level
        FROM candidate_experiences
        WHERE review_status IN ('pending','pending_human')
          AND sample_count >= 2
          AND (?1='' OR scope_value=?1)
          AND EXISTS (
              SELECT 1 FROM json_each(applies_to_phases_json)
              WHERE CAST(value AS INTEGER)=?2
          )
        ORDER BY sample_count DESC,created_at_ms DESC
        LIMIT ?3
        "#,
    )?;
    let repeated = repeated_stmt
        .query_map(params![ticker, phase, limit], |row| {
            Ok(json!({
                "experience_id": format!("candidate-{}", row.get::<_, i64>(0)?),
                "ticker": row.get::<_, String>(1)?,
                "experience_type": row.get::<_, String>(2)?,
                "source_phase": row.get::<_, i64>(3)?,
                "applies_to_phases": serde_json::from_str::<Value>(&row.get::<_, String>(4)?).unwrap_or_else(|_| json!([])),
                "pattern_key": row.get::<_, String>(5)?,
                "finding": row.get::<_, String>(6)?,
                "recommendation": row.get::<_, String>(7)?,
                "sample_count": row.get::<_, i64>(8)?,
                "confidence": row.get::<_, f64>(9)?,
                "attribution_confidence": row.get::<_, f64>(10)?,
                "level": row.get::<_, String>(11)?,
                "policy": "warning_rebuttable"
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({
        "query": "experience",
        "phase": phase,
        "ticker": if ticker.is_empty() { Value::Null } else { json!(ticker) },
        "active_policy": active,
        "repeated_warnings": repeated,
        "recent_episodes": recent,
        "usage_contract": "Consider each item, cite experience_id when it changes analysis, and record a reason when rejecting it. Experience is advisory, not a hard rule."
    }))
}

pub fn persist_reflection_artifact(
    conn: &Connection,
    task_id: i64,
    reflection_version: &str,
    artifact: &Value,
) -> Result<usize> {
    if artifact.get("artifact_type").and_then(Value::as_str) != Some("historical_reflection_bundle")
    {
        bail!("reflection artifact_type must be historical_reflection_bundle");
    }
    if artifact.get("task_id").and_then(Value::as_i64) != Some(task_id) {
        bail!("reflection artifact task_id does not match task {task_id}");
    }
    let experiences = artifact
        .get("experiences")
        .and_then(Value::as_array)
        .context("reflection artifact experiences must be an array")?;
    let (source_run_id, ticker, attribution_confidence): (String, String, f64) = conn
        .query_row(
            r#"
            SELECT o.source_run_id,o.ticker,o.attribution_confidence
            FROM reflection_tasks t
            JOIN decision_outcomes o ON o.id=t.decision_outcome_id
            WHERE t.id=?1 AND t.status IN ('pending','running')
            "#,
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?
        .with_context(|| format!("writable reflection task {task_id} not found"))?;
    let tx = conn.unchecked_transaction()?;
    let mut written = 0;
    for experience in experiences {
        let source_phase = required_phase(experience, "source_phase", 0, 8)?;
        let applies = required_phases(experience, "applies_to_phases", 1, 6)?;
        let propagation = required_phases(experience, "propagation_path", 0, 8)?;
        let experience_type = controlled_value(
            experience,
            "experience_type",
            &[
                "evidence_quality",
                "timing",
                "calibration",
                "risk_sizing",
                "decision_process",
                "execution",
                "data_integrity",
            ],
        )?;
        let failure_mode = controlled_value(
            experience,
            "failure_mode",
            &[
                "stale_evidence",
                "duplicate_evidence",
                "missing_evidence",
                "direction_error",
                "confidence_miscalibration",
                "timing_error",
                "sizing_error",
                "risk_violation",
                "lucky_profit",
                "correct_logic_bad_execution",
                "other",
            ],
        )?;
        let recommendation_class = controlled_value(
            experience,
            "recommendation_class",
            &[
                "verify_freshness",
                "deduplicate_sources",
                "require_evidence",
                "calibrate_confidence",
                "adjust_timing",
                "adjust_sizing",
                "enforce_risk",
                "preserve_success_pattern",
                "revise_process",
            ],
        )?;
        let finding = required_text(experience, "finding", 2048)?;
        let recommendation = required_text(experience, "recommendation", 2048)?;
        let summary_ids = string_array(experience, "evidence_summary_ids")?;
        let detail_ids = string_array(experience, "evidence_detail_ids")?;
        validate_evidence_ids(&tx, &source_run_id, &summary_ids, &detail_ids)?;
        let counter_evidence = experience
            .get("counter_evidence")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let confidence = experience
            .get("confidence")
            .and_then(Value::as_f64)
            .filter(|value| (0.0..=1.0).contains(value))
            .context("reflection experience confidence must be between 0 and 1")?;
        let reusable = experience
            .get("reusable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let pattern_key = pattern_key(
            source_phase,
            experience_type,
            failure_mode,
            recommendation_class,
        );
        let reflection_id = format!("refl-exp-{}", Uuid::new_v4());
        tx.execute(
            r#"
            INSERT INTO reflection_episodes
                (task_id,reflection_id,source_run_id,ticker,source_phase,
                 applies_to_phases_json,propagation_path_json,experience_type,
                 failure_mode,recommendation_class,pattern_key,finding,recommendation,
                 evidence_summary_ids_json,evidence_detail_ids_json,counter_evidence_json,
                 confidence,attribution_confidence,reusable,artifact_json,created_at_ms)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,
                    ?16,?17,?18,?19,?20,?21)
            ON CONFLICT(task_id,source_phase,pattern_key) DO NOTHING
            "#,
            params![
                task_id,
                reflection_id,
                source_run_id,
                ticker,
                source_phase,
                serde_json::to_string(&applies)?,
                serde_json::to_string(&propagation)?,
                experience_type,
                failure_mode,
                recommendation_class,
                pattern_key,
                finding,
                recommendation,
                serde_json::to_string(&summary_ids)?,
                serde_json::to_string(&detail_ids)?,
                serde_json::to_string(&counter_evidence)?,
                confidence,
                attribution_confidence,
                reusable as i64,
                serde_json::to_string(experience)?,
                crate::schema::now_ms(),
            ],
        )?;
        if reusable {
            upsert_candidate_from_episode(
                &tx,
                &source_run_id,
                &ticker,
                source_phase,
                &applies,
                &propagation,
                experience_type,
                &pattern_key,
                finding,
                recommendation,
                &summary_ids,
                &detail_ids,
                &counter_evidence,
                confidence,
                attribution_confidence,
                reflection_version,
            )?;
        }
        written += 1;
    }
    tx.execute(
        "UPDATE reflection_tasks SET status='completed',updated_at_ms=?1 WHERE id=?2",
        params![crate::schema::now_ms(), task_id],
    )?;
    tx.commit()?;
    Ok(written)
}

#[allow(clippy::too_many_arguments)]
fn upsert_candidate_from_episode(
    conn: &Connection,
    source_run_id: &str,
    ticker: &str,
    source_phase: i64,
    applies: &[i64],
    propagation: &[i64],
    experience_type: &str,
    pattern_key: &str,
    finding: &str,
    recommendation: &str,
    summary_ids: &[String],
    detail_ids: &[String],
    counter_evidence: &Value,
    confidence: f64,
    attribution_confidence: f64,
    reflection_version: &str,
) -> Result<()> {
    let existing: Option<(i64, Value)> = conn
        .query_row(
            r#"
            SELECT id,sample_run_ids_json
            FROM candidate_experiences
            WHERE pattern_key=?1 AND scope='ticker' AND scope_value=?2
              AND review_status IN ('pending','pending_human')
            ORDER BY id DESC LIMIT 1
            "#,
            params![pattern_key, ticker],
            |row| {
                let raw: String = row.get(1)?;
                Ok((
                    row.get(0)?,
                    serde_json::from_str(&raw).unwrap_or_else(|_| json!([])),
                ))
            },
        )
        .optional()?;
    if let Some((id, run_ids)) = existing {
        let mut unique = run_ids
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        unique.insert(source_run_id.to_string());
        let sample_count = unique.len() as i64;
        let experience_level = if sample_count >= 3 {
            "validated"
        } else if sample_count >= 2 {
            "repeated_warning"
        } else {
            "recent_episode"
        };
        conn.execute(
            r#"
            UPDATE candidate_experiences
            SET sample_count=?1,sample_run_ids_json=?2,
                confidence=MAX(confidence,?3),
                attribution_confidence=MIN(attribution_confidence,?4),
                experience_level=?5
            WHERE id=?6
            "#,
            params![
                sample_count,
                serde_json::to_string(&unique)?,
                confidence,
                attribution_confidence,
                experience_level,
                id,
            ],
        )?;
        return Ok(());
    }
    conn.execute(
        r#"
        INSERT INTO candidate_experiences
            (scope,scope_value,experience_type,market_regime_json,finding,recommendation,
             evidence_json,counter_evidence_json,metrics_json,sample_count,
             sample_run_ids_json,confidence,effect_size,distiller_version,
             reflection_version,source_window,review_status,created_at_ms,source_phase,
             applies_to_phases_json,propagation_path_json,pattern_key,experience_level,
             attribution_confidence)
        VALUES ('ticker',?1,?2,'{}',?3,?4,?5,?6,'{}',1,?7,?8,0.0,
                'phase0',?9,?10,'pending',?11,?12,?13,?14,?15,'recent_episode',?16)
        "#,
        params![
            ticker,
            experience_type,
            finding,
            recommendation,
            serde_json::to_string(&json!({
                "summary_ids": summary_ids,
                "detail_ids": detail_ids
            }))?,
            serde_json::to_string(counter_evidence)?,
            serde_json::to_string(&vec![source_run_id])?,
            confidence,
            reflection_version,
            source_run_id,
            crate::schema::now_ms(),
            source_phase,
            serde_json::to_string(applies)?,
            serde_json::to_string(propagation)?,
            pattern_key,
            attribution_confidence,
        ],
    )?;
    Ok(())
}

fn validate_evidence_ids(
    conn: &Connection,
    source_run_id: &str,
    summary_ids: &[String],
    detail_ids: &[String],
) -> Result<()> {
    for id in summary_ids {
        let exists = conn
            .query_row(
                "SELECT 1 FROM phase_summaries WHERE id=?1 AND run_id=?2",
                params![id, source_run_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            bail!("reflection evidence summary_id {id} is outside the source run");
        }
    }
    for id in detail_ids {
        let exists = conn
            .query_row(
                "SELECT 1 FROM phase_summary_details WHERE id=?1 AND run_id=?2",
                params![id, source_run_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            bail!("reflection evidence detail_id {id} is outside the source run");
        }
    }
    Ok(())
}

fn pattern_key(
    source_phase: i64,
    experience_type: &str,
    failure_mode: &str,
    recommendation_class: &str,
) -> String {
    let canonical =
        format!("{source_phase}|{experience_type}|{failure_mode}|{recommendation_class}");
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}

fn required_phase(value: &Value, field: &str, min: i64, max: i64) -> Result<i64> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .filter(|phase| (min..=max).contains(phase))
        .with_context(|| format!("{field} must be between {min} and {max}"))
}

fn required_phases(value: &Value, field: &str, min: i64, max: i64) -> Result<Vec<i64>> {
    let phases = value
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("{field} must be an array"))?
        .iter()
        .map(|item| {
            item.as_i64()
                .filter(|phase| (min..=max).contains(phase))
                .with_context(|| format!("{field} contains an invalid phase"))
        })
        .collect::<Result<Vec<_>>>()?;
    if phases.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(phases)
}

fn controlled_value<'a>(value: &'a Value, field: &str, allowed: &[&str]) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|item| allowed.contains(item))
        .with_context(|| format!("{field} must be one of {}", allowed.join(", ")))
}

fn required_text<'a>(value: &'a Value, field: &str, max_chars: usize) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty() && text.chars().count() <= max_chars)
        .with_context(|| format!("{field} must be non-empty and at most {max_chars} characters"))
}

fn string_array(value: &Value, field: &str) -> Result<Vec<String>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("{field} must be an array"))?
        .iter()
        .map(|item| {
            item.as_str()
                .filter(|text| !text.trim().is_empty())
                .map(ToString::to_string)
                .with_context(|| format!("{field} must contain strings"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connect,
        prediction::{upsert_prediction, PredictionInput},
        write_run_record, RunRecordInput,
    };

    #[test]
    fn third_trading_bar_matures_and_every_outcome_queues_reflection() {
        let temp = tempfile::tempdir().unwrap();
        let mut conn = connect(temp.path().join("reflection.sqlite")).unwrap();
        upsert_prediction(
            &conn,
            &PredictionInput {
                run_id: "source-run".to_string(),
                ticker: "QQQ".to_string(),
                prediction_date: "2026-01-01".to_string(),
                long_probability: 0.7,
                short_probability: 0.3,
                rating: "Buy".to_string(),
                window_days: PREDICTION_HORIZON_TRADING_DAYS,
                market_regime_json: json!({}),
                agent_probabilities_json: json!({}),
                weighted_base_probability: None,
            },
        )
        .unwrap();
        write_run_record(
            &mut conn,
            &RunRecordInput {
                run_id: "current-run",
                current_date: "2026-01-07",
            },
        )
        .unwrap();
        for (date, close) in [
            ("2026-01-01", 100.0),
            ("2026-01-02", 101.0),
            ("2026-01-05", 102.0),
            ("2026-01-06", 105.0),
        ] {
            conn.execute(
                r#"
                INSERT INTO technical_bars
                    (ticker,interval,bar_time,close,values_json,imported_at_ms)
                VALUES ('QQQ','daily',?1,?2,'{}',1)
                "#,
                params![date, close],
            )
            .unwrap();
        }

        let summary = score_mature_predictions(
            &conn,
            "2026-01-07",
            "1d",
            10,
            ReflectionThresholds::default(),
            Some("current-run"),
            "v1",
            10,
        )
        .unwrap();
        assert_eq!(summary.scored, 1);
        assert_eq!(summary.queued, 1);
        assert_eq!(summary.deep, 0);
        let tasks = pending_reflection_tasks(&conn, "current-run", 10).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].reflection_level, "routine");

        let written = persist_reflection_artifact(
            &conn,
            tasks[0].task_id,
            "v1",
            &json!({
                "artifact_type": "historical_reflection_bundle",
                "task_id": tasks[0].task_id,
                "experiences": []
            }),
        )
        .unwrap();
        assert_eq!(written, 0);
        assert!(pending_reflection_tasks(&conn, "current-run", 10)
            .unwrap()
            .is_empty());
    }
}
