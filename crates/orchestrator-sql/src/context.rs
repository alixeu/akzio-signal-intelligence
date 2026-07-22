use crate::{
    memory::{read_prior_memory, PriorMemoryQuery},
    outcome::track_record,
    technical_store::{load_technical_series, technical_row_count},
    AGGREGATE_TICKER,
};
use anyhow::Result;
use orchestrator_core::{latest_snapshot, MarketRegime, RetrievalBudget};
use rusqlite::{params, params_from_iter, Connection, Row};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct RuntimeContext {
    pub run_id: String,
    pub ticker: String,
    pub tickers: Vec<String>,
    pub phase: i64,
    pub role: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RunContextReadRequest {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub ticker: Option<String>,
    #[serde(default)]
    pub tickers: Vec<String>,
    #[serde(default)]
    pub phase: Option<i64>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub topic_id: Option<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default = "default_persist_context")]
    pub persist_context: bool,
    #[serde(default)]
    pub token_budget: Option<usize>,
}

fn default_persist_context() -> bool {
    true
}

pub fn read_run_context(conn: &mut Connection, request: &RunContextReadRequest) -> Result<Value> {
    let kind = match request.kind.trim() {
        ""
        | "all"
        | "key"
        | "keys"
        | "value"
        | "run_metadata"
        | "role_status"
        | "probability_base"
        | "weighted_probability_base" => "research_inputs",
        kind => kind,
    };
    match kind {
        "jin10" | "jin10_context" => return jin10_context(conn),
        "technical" | "technical_context" => {
            return technical_context(conn, &request.tickers, request.ticker.as_deref())
        }
        "technical_daily" => {
            return technical_interval_context(
                conn,
                "daily",
                &request.tickers,
                request.ticker.as_deref(),
            )
        }
        "technical_3h" => {
            return technical_interval_context(
                conn,
                "3h",
                &request.tickers,
                request.ticker.as_deref(),
            )
        }
        "technical_20min" => {
            return technical_interval_context(
                conn,
                "20min",
                &request.tickers,
                request.ticker.as_deref(),
            )
        }
        _ => {}
    }
    let run_id = request
        .run_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
        .map(Ok)
        .unwrap_or_else(|| latest_run_id(conn))?;
    let ticker = request.ticker.clone().unwrap_or_default();
    let ctx = RuntimeContext {
        run_id: run_id.clone(),
        ticker: ticker.clone(),
        tickers: request.tickers.clone(),
        phase: request.phase.unwrap_or_default(),
        role: request.role.clone().unwrap_or_default(),
    };
    match kind {
        "analyst_reports" => handle_read_command(conn, "get-analyst-reports", &ctx, None),
        "debate_history" => handle_read_command(
            conn,
            "get-debate-history",
            &ctx,
            request.topic_id.as_deref(),
        ),
        "research_inputs" => handle_read_command(conn, "get-research-inputs", &ctx, None),
        "topic_state" => {
            handle_read_command(conn, "get-topic-brief", &ctx, request.topic_id.as_deref())
        }
        "mediator_reviews" => handle_read_command(
            conn,
            "get-mediator-reviews",
            &ctx,
            request.topic_id.as_deref(),
        ),
        "role_summaries" => role_summaries_context(conn, &ctx),
        "prior_memory" => prior_memory_context(conn, request, &ctx),
        "track_record" => track_record_context(conn, &ctx),
        "agent_accuracy" => agent_accuracy_context(conn),
        "compose_context" => compose_context(conn, request, &ctx),
        "phase_summaries" | "prior_phase_summaries" => {
            // If caller sets phase=N, return summaries for phases < N (prior only).
            let max_source_phase = request.phase.filter(|p| *p > 0).map(|p| p - 1);
            crate::phase_index::list_phase_summaries(
                conn,
                &ctx.run_id,
                max_source_phase,
                request.ticker.as_deref().filter(|t| !t.is_empty()),
            )
        }
        "phase_summary_details" => {
            // summary_id is passed in topic_id for this kind.
            let summary_id = request
                .topic_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .or_else(|| request.turn_id.as_deref().filter(|s| !s.is_empty()))
                .unwrap_or_default();
            if summary_id.is_empty() {
                anyhow::bail!(
                    "phase_summary_details requires topic_id set to the phase_summaries.id"
                );
            }
            crate::phase_index::list_phase_summary_details(conn, summary_id)
        }
        "attention" => crate::phase_index::list_attention(
            conn,
            &ctx.run_id,
            request.role.as_deref().filter(|r| !r.is_empty()),
            request.turn_id.as_deref().filter(|t| !t.is_empty()),
            None,
            request.token_budget.unwrap_or(50).max(1),
        ),
        "attention_expand" => {
            // Expect request to encode subjects in tickers as "kind:id" pairs, or role as JSON.
            let subjects = parse_attention_expand_subjects(request);
            crate::phase_index::expand_attention_subjects(conn, &subjects)
        }
        other => anyhow::bail!("unsupported read_run_context kind {other:?}"),
    }
}

fn parse_attention_expand_subjects(request: &RunContextReadRequest) -> Vec<(String, String)> {
    // Prefer tickers entries shaped as "jin10:<id>" / "summary:<id>" / "detail:<id>".
    let mut subjects = Vec::new();
    for entry in &request.tickers {
        if let Some((kind, id)) = entry.split_once(':') {
            let kind = kind.trim();
            let id = id.trim();
            if !kind.is_empty() && !id.is_empty() {
                subjects.push((kind.to_string(), id.to_string()));
            }
        }
    }
    if subjects.is_empty() {
        if let Some(id) = request.turn_id.as_deref().filter(|s| !s.is_empty()) {
            // Fallback: treat as summary expand
            subjects.push(("summary".to_string(), id.to_string()));
        }
    }
    subjects
}

#[derive(Debug, Clone)]
struct ContextBlock {
    context_type: String,
    context_ref: String,
    ticker: String,
    item_time: i64,
    title: String,
    content: String,
    weight: f64,
    source_table: String,
    item_json: Value,
}

impl ContextBlock {
    fn tokens(&self) -> usize {
        (self.content.chars().count() / 4).max(1)
    }

    fn value(&self) -> Value {
        json!({
            "context_type": self.context_type,
            "context_ref": self.context_ref,
            "ticker": self.ticker,
            "item_time": self.item_time,
            "title": self.title,
            "content": self.content,
            "weight": self.weight,
            "source_table": self.source_table,
            "item_json": self.item_json
        })
    }
}

fn compose_context(
    conn: &mut Connection,
    request: &RunContextReadRequest,
    ctx: &RuntimeContext,
) -> Result<Value> {
    let mut blocks = Vec::new();
    collect_prior_memory_blocks(conn, request, ctx, &mut blocks)?;
    collect_summary_blocks(conn, ctx, request.topic_id.as_deref(), &mut blocks)?;
    // Always include source/evidence blocks for compose_context. Downstream roles
    // rely on this as the default empty-kind payload; gating on phase previously
    // returned near-empty context when phase was unset on the tool request.
    collect_jin10_blocks(conn, &mut blocks)?;
    collect_technical_blocks(conn, ctx, &mut blocks)?;
    collect_turn_history_blocks(conn, ctx, request.topic_id.as_deref(), &mut blocks)?;

    for block in &mut blocks {
        block.weight += ticker_weight(&block.ticker, &ctx.ticker);
        block.weight += freshness_weight(block.item_time);
    }

    blocks.sort_by(|a, b| {
        b.weight
            .partial_cmp(&a.weight)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.item_time.cmp(&a.item_time))
    });

    let budget = request.token_budget.unwrap_or(4096).max(1);
    let mut used_tokens = 0;
    let mut selected = Vec::new();
    for block in blocks {
        let tokens = block.tokens();
        if used_tokens + tokens > budget {
            continue;
        }
        used_tokens += tokens;
        selected.push(block);
    }

    Ok(json!({
        "query": "compose-context",
        "run_id": ctx.run_id,
        "turn_id": request.turn_id,
        "role": ctx.role,
        "phase": ctx.phase,
        "ticker": ctx.ticker,
        "topic_id": request.topic_id,
        "token_budget": budget,
        "estimated_tokens": used_tokens,
        "blocks": selected.iter().map(ContextBlock::value).collect::<Vec<_>>()
    }))
}

fn prior_memory_context(
    conn: &Connection,
    request: &RunContextReadRequest,
    ctx: &RuntimeContext,
) -> Result<Value> {
    read_prior_memory(
        conn,
        &PriorMemoryQuery {
            ticker: context_ticker(ctx),
            market_regime: MarketRegime::default(),
            budget: RetrievalBudget {
                token_budget: request.token_budget.unwrap_or(1024).max(1),
                max_items: 8,
                min_quality: 0.0,
            },
            include_body: false,
        },
    )
}

fn track_record_context(conn: &Connection, ctx: &RuntimeContext) -> Result<Value> {
    let ticker = ctx.ticker.trim();
    let aggregate = track_record(conn, None)?;
    let ticker_record = if ticker.is_empty() {
        Value::Null
    } else {
        track_record(conn, Some(ticker))?
    };
    Ok(json!({
        "query": "track_record",
        "ticker": ticker,
        "aggregate": aggregate,
        "ticker_record": ticker_record,
    }))
}

fn agent_accuracy_context(conn: &Connection) -> Result<Value> {
    let mut stmt = conn.prepare(
        r#"
        SELECT p.agent_probabilities_json, o.direction_correct, o.probability_error
        FROM outcomes o
        JOIN predictions p ON p.id = o.prediction_id
        ORDER BY o.scored_at DESC
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)? != 0,
            row.get::<_, f64>(2)?,
        ))
    })?;

    let mut stats: BTreeMap<String, RoleAccuracy> = BTreeMap::new();
    for row in rows {
        let (raw, direction_correct, probability_error) = row?;
        for role in roles_from_agent_probabilities(&raw) {
            stats
                .entry(role)
                .or_default()
                .record(direction_correct, probability_error);
        }
    }

    Ok(json!({
        "query": "agent_accuracy",
        "roles": Value::Object(
            stats
                .into_iter()
                .map(|(role, stat)| (role, stat.value()))
                .collect()
        )
    }))
}

fn collect_prior_memory_blocks(
    conn: &Connection,
    request: &RunContextReadRequest,
    ctx: &RuntimeContext,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    let memory = prior_memory_context(conn, request, ctx)?;
    let Some(items) = memory.get("items").and_then(Value::as_array) else {
        return Ok(());
    };
    for item in items {
        let summary = item
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if summary.is_empty() {
            continue;
        }
        let memory_id = item
            .get("memory_id")
            .and_then(Value::as_str)
            .unwrap_or("memory");
        let quality_score = item
            .get("quality_score")
            .and_then(Value::as_f64)
            .unwrap_or_default();
        blocks.push(ContextBlock {
            context_type: "prior_memory".to_string(),
            context_ref: format!("memory_items:{memory_id}"),
            ticker: item
                .get("ticker")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            item_time: item
                .get("observed_at")
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            title: item
                .get("memory_type")
                .and_then(Value::as_str)
                .unwrap_or("prior_memory")
                .to_string(),
            content: summary,
            weight: 2.0 + quality_score,
            source_table: "memory_items".to_string(),
            item_json: item.clone(),
        });
    }
    Ok(())
}

fn context_ticker(ctx: &RuntimeContext) -> Option<String> {
    if ctx.ticker.trim().is_empty() {
        None
    } else {
        Some(ctx.ticker.clone())
    }
}

#[derive(Debug, Default)]
struct RoleAccuracy {
    total: usize,
    correct: usize,
    probability_error_sum: f64,
    brier_sum: f64,
}

impl RoleAccuracy {
    fn record(&mut self, direction_correct: bool, probability_error: f64) {
        self.total += 1;
        if direction_correct {
            self.correct += 1;
        }
        self.probability_error_sum += probability_error;
        self.brier_sum += probability_error * probability_error;
    }

    fn value(self) -> Value {
        if self.total == 0 {
            return json!({
                "total_predictions": 0,
                "direction_accuracy": 0.0,
                "mean_probability_error": 0.0,
                "mean_brier_score": 0.0,
            });
        }
        let total = self.total as f64;
        json!({
            "total_predictions": self.total,
            "direction_accuracy": self.correct as f64 / total,
            "mean_probability_error": self.probability_error_sum / total,
            "mean_brier_score": self.brier_sum / total,
        })
    }
}

fn roles_from_agent_probabilities(raw: &str) -> Vec<String> {
    match serde_json::from_str::<Value>(raw).unwrap_or(Value::Null) {
        Value::Object(map) => map.keys().cloned().collect(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                item.get("role")
                    .or_else(|| item.get("agent"))
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn collect_summary_blocks(
    conn: &Connection,
    ctx: &RuntimeContext,
    topic_id: Option<&str>,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, turn_id, phase, role, ticker, item_time, topic_id, summary_type,
               summary, summary_json, confidence, created_at
        FROM role_turn_summaries
        WHERE run_id = ? AND (? = '' OR ticker = ? OR ticker = ?)
        ORDER BY created_at DESC
        LIMIT 120
        "#,
    )?;
    let rows = stmt.query_map(
        params![ctx.run_id, ctx.ticker, ctx.ticker, AGGREGATE_TICKER],
        |row| {
            let id: i64 = row.get("id")?;
            let phase = row.get::<_, Option<i64>>("phase")?;
            let row_topic = row.get::<_, Option<String>>("topic_id")?;
            let confidence = row.get::<_, f64>("confidence")?;
            let summary_json: String = row.get("summary_json")?;
            let summary: String = row.get("summary")?;
            let title = format!(
                "{} {}",
                row.get::<_, String>("role")?,
                row.get::<_, String>("summary_type")?
            );
            let mut weight = 1.0 + confidence;
            if phase.is_some_and(|value| value < ctx.phase) {
                weight += 2.0;
            }
            if topic_id.is_some()
                && row_topic
                    .as_deref()
                    .is_some_and(|value| Some(value) == topic_id)
            {
                weight += 3.0;
            }
            Ok(ContextBlock {
                context_type: "role_summary".to_string(),
                context_ref: format!("role_turn_summaries:{id}"),
                ticker: row.get("ticker")?,
                item_time: row
                    .get::<_, i64>("item_time")
                    .or_else(|_| row.get::<_, i64>("created_at"))?,
                title,
                content: summary,
                weight,
                source_table: "role_turn_summaries".to_string(),
                item_json: serde_json::from_str(&summary_json)
                    .unwrap_or(Value::String(summary_json)),
            })
        },
    )?;
    blocks.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    Ok(())
}

fn collect_jin10_blocks(conn: &Connection, blocks: &mut Vec<ContextBlock>) -> Result<()> {
    const MAX_CONTENT_CHARS: usize = 400;
    let mut stmt = conn.prepare(
        r#"
        SELECT id, content_json, attention_score, item_time
        FROM jin10_items
        ORDER BY attention_score DESC, item_time DESC
        LIMIT 20
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        let id: String = row.get("id")?;
        let content_json: String = row.get("content_json")?;
        let attention_score: f64 = row.get("attention_score")?;
        let item_time: i64 = row.get("item_time")?;
        let parsed: Value = serde_json::from_str(&content_json).unwrap_or(Value::Null);
        let content = parsed
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or(content_json.as_str())
            .to_string();
        let clipped = if content.chars().count() > MAX_CONTENT_CHARS {
            let clipped: String = content.chars().take(MAX_CONTENT_CHARS).collect();
            format!("{clipped}…")
        } else {
            content
        };
        let mut item_json = if parsed.is_object() {
            parsed
        } else {
            json!({ "content": clipped.clone() })
        };
        if let Some(object) = item_json.as_object_mut() {
            object.insert("id".to_string(), json!(id));
            object.insert("attention_score".to_string(), json!(attention_score));
            if !object.contains_key("time") {
                object.insert("time".to_string(), json!(item_time));
            }
        }
        Ok(ContextBlock {
            context_type: "jin10".to_string(),
            context_ref: format!("jin10_items:{id}"),
            ticker: String::new(),
            item_time,
            title: format!("Jin10 {id}"),
            content: clipped,
            // Prefer high-attention items in compose budget selection.
            weight: 1.0 + attention_score.clamp(0.0, 1.0),
            source_table: "jin10_items".to_string(),
            item_json,
        })
    })?;
    blocks.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    Ok(())
}

fn collect_technical_blocks(
    conn: &Connection,
    ctx: &RuntimeContext,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    let tickers = effective_technical_tickers(&ctx.tickers, Some(ctx.ticker.as_str()));
    for (interval, context_type) in [
        ("daily", "technical_daily"),
        ("3h", "technical_3h"),
        ("20min", "technical_20min"),
    ] {
        for snapshot in technical_snapshots_from_db(conn, interval, &tickers)? {
            let ticker = snapshot
                .get("ticker")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let kline_time = snapshot
                .get("kline_time")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let item_time = date_str_to_timestamp(&kline_time);
            let indicators = snapshot
                .get("indicators")
                .cloned()
                .unwrap_or(Value::Object(Default::default()));
            let content = format!("{context_type} {ticker} @{kline_time} indicators={indicators}");
            blocks.push(ContextBlock {
                context_type: context_type.to_string(),
                context_ref: format!("technical_snapshot:{interval}:{ticker}:{kline_time}"),
                ticker,
                item_time,
                title: context_type.to_string(),
                content,
                weight: 1.5,
                source_table: "technical_series".to_string(),
                item_json: snapshot,
            });
        }
    }
    Ok(())
}

fn collect_turn_history_blocks(
    conn: &Connection,
    ctx: &RuntimeContext,
    _topic_id: Option<&str>,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, turn_id, role, created_at, summary
        FROM agent_events
        WHERE run_id = ?
        ORDER BY turn_number DESC
        LIMIT 12
        "#,
    )?;
    let rows = stmt.query_map(params![&ctx.run_id], |row| {
        let id: i64 = row.get("id")?;
        let role: String = row.get("role")?;
        Ok(ContextBlock {
            context_type: "turn_history".to_string(),
            context_ref: format!("agent_events:{id}"),
            ticker: String::new(),
            item_time: row.get("created_at")?,
            title: format!("{role} turn"),
            content: row.get("summary")?,
            weight: 0.5,
            source_table: "agent_events".to_string(),
            item_json: Value::Null,
        })
    })?;
    blocks.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    Ok(())
}

fn ticker_weight(block_ticker: &str, request_ticker: &str) -> f64 {
    if request_ticker.is_empty() {
        0.0
    } else if block_ticker == request_ticker {
        3.0
    } else if block_ticker == AGGREGATE_TICKER {
        1.0
    } else {
        0.0
    }
}

fn freshness_weight(item_time: i64) -> f64 {
    let age_days = (chrono::Utc::now().timestamp() - item_time).max(0) as f64 / 86400.0;
    (2.0 / (1.0 + age_days / 7.0)).max(0.1)
}

fn date_str_to_timestamp(s: &str) -> i64 {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| {
            d.and_hms_opt(0, 0, 0)
                .and_then(|dt| dt.and_utc().timestamp().into())
        })
        .unwrap_or(0)
}

fn latest_run_id(conn: &Connection) -> Result<String> {
    conn.query_row(
        "SELECT run_id FROM runs ORDER BY current_date DESC, created_at DESC LIMIT 1",
        [],
        |row| row.get::<_, String>(0),
    )
    .map_err(Into::into)
}

pub fn messages_for_run(
    conn: &Connection,
    run_id: &str,
    phases: &[i64],
    kinds: &[&str],
) -> Result<Vec<Value>> {
    let mut sql = String::from(
        "SELECT run_id, phase, role, ticker, turn_id, topic_id, summary_type, summary, summary_json, confidence, created_at
         FROM role_turn_summaries
         WHERE run_id = ?",
    );
    let mut params: Vec<rusqlite::types::Value> =
        vec![rusqlite::types::Value::Text(run_id.to_string())];
    if !phases.is_empty() {
        sql.push_str(" AND phase IN (");
        sql.push_str(&vec!["?"; phases.len()].join(","));
        sql.push(')');
        params.extend(phases.iter().map(|value| (*value).into()));
    }
    if !kinds.is_empty() {
        sql.push_str(" AND summary_type IN (");
        sql.push_str(&vec!["?"; kinds.len()].join(","));
        sql.push(')');
        params.extend(
            kinds
                .iter()
                .map(|value| rusqlite::types::Value::Text((*value).to_string())),
        );
    }
    sql.push_str(" ORDER BY phase, role, id");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params_from_iter(params), row_to_message)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn row_to_message(row: &Row<'_>) -> rusqlite::Result<Value> {
    let content_text: String = row.get("summary_json")?;
    let content =
        serde_json::from_str::<Value>(&content_text).unwrap_or(Value::String(content_text));
    Ok(json!({
        "run_id": row.get::<_, String>("run_id")?,
        "phase": row.get::<_, Option<i64>>("phase")?.unwrap_or_default(),
        "role": row.get::<_, String>("role")?,
        "ticker": row.get::<_, String>("ticker")?,
        "message_group_id": row.get::<_, String>("turn_id")?,
        "topic_id": row.get::<_, Option<String>>("topic_id")?,
        "kind": row.get::<_, String>("summary_type")?,
        "valid": row.get::<_, f64>("confidence")? > 0.0,
        "content": content,
        "last_md": row.get::<_, String>("summary")?,
        "created_at": row.get::<_, i64>("created_at")?
    }))
}

pub fn messages_text(items: &[Value]) -> String {
    items
        .iter()
        .map(|item| {
            format!(
                "[phase {}] {} {} {}",
                item.get("phase")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
                item.get("role").and_then(Value::as_str).unwrap_or(""),
                item.get("ticker").and_then(Value::as_str).unwrap_or(""),
                item.get("content").unwrap_or(&Value::Null)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn session_history_items(conn: &Connection, run_id: &str, _limit: usize) -> Result<Vec<Value>> {
    let full_context_json: String = conn
        .query_row(
            "SELECT full_context_json FROM agent_events WHERE run_id = ? ORDER BY turn_number DESC LIMIT 1",
            params![run_id],
            |row| row.get(0),
        )
        .unwrap_or_default();
    if full_context_json.is_empty() || full_context_json == "[]" {
        return Ok(Vec::new());
    }
    let messages: Vec<Value> = serde_json::from_str(&full_context_json).unwrap_or_default();
    Ok(messages)
}

/// Load the full_context snapshot for a single agent-loop turn.
///
/// Prefer this over [`session_history_items`] when resuming multi-round steer
/// sessions: multiple roles share one `run_id`, so run-scoped latest-row reload
/// steals sibling history and drops role prompts / tool evidence.
pub fn turn_history_items(conn: &Connection, turn_id: &str) -> Result<Vec<Value>> {
    let full_context_json: String = conn
        .query_row(
            "SELECT full_context_json FROM agent_events WHERE turn_id = ?",
            params![turn_id],
            |row| row.get(0),
        )
        .unwrap_or_default();
    if full_context_json.is_empty() || full_context_json == "[]" {
        return Ok(Vec::new());
    }
    let messages: Vec<Value> = serde_json::from_str(&full_context_json).unwrap_or_default();
    Ok(messages)
}

pub fn handle_read_command(
    conn: &Connection,
    command: &str,
    ctx: &RuntimeContext,
    topic_id: Option<&str>,
) -> Result<Value> {
    let _ = topic_id;
    let (phases, kinds): (Vec<i64>, Vec<&str>) = match command {
        "get-analyst-reports" => (vec![1], vec!["artifact", "artifact_ticker"]),
        "get-debate-history" => (
            vec![2, 25],
            vec![
                "artifact",
                "artifact_ticker",
                "topic_final",
                "topic_final_ticker",
            ],
        ),
        "get-research-inputs" | "get-run-inputs" => (vec![1, 2, 25, 3], vec![]),
        "get-jin10-context" => return jin10_context(conn),
        "get-technical-context" => {
            return technical_context(conn, &ctx.tickers, Some(ctx.ticker.as_str()))
        }
        "get-previous-topics"
        | "get-opponent-last"
        | "get-topics"
        | "get-topic-finals-all"
        | "get-mediator-reviews"
        | "get-topic-brief"
        | "get-live-thread"
        | "get-unread-events"
        | "get-latest-checkpoint"
        | "get-topic-finals" => (vec![2, 25], vec![]),
        _ => anyhow::bail!("unsupported read command: {command}"),
    };
    let items = messages_for_run(conn, &ctx.run_id, &phases, &kinds)?;
    Ok(json!({
        "query": command,
        "run_id": ctx.run_id,
        "ticker": ctx.ticker,
        "tickers": ctx.tickers,
        "items": items,
        "text": messages_text(&items)
    }))
}

pub fn sqlite_context(conn: &Connection, run_id: &str) -> Result<Value> {
    let analyst_messages = messages_for_run(conn, run_id, &[1], &["artifact", "artifact_ticker"])?;
    let debate_messages = messages_for_run(conn, run_id, &[2, 25], &[])?;
    Ok(json!({
        "run_id": run_id,
        "analyst_messages": analyst_messages,
        "debate_messages": debate_messages,
        "technical": technical_context(conn, &[], None)?,
        "jin10": jin10_context(conn)?
    }))
}

pub fn context_count(conn: &Connection, name: &str) -> Result<i64> {
    if name.contains('\n') {
        let mut total = 0;
        for part in name.lines().map(str::trim).filter(|part| !part.is_empty()) {
            total += context_count(conn, part)?;
        }
        return Ok(total);
    }
    let sql = match name {
        "technical" | "technical-context" | "technical_context" => {
            return technical_row_count(conn, None);
        }
        "technical_daily" => {
            return technical_row_count(conn, Some("daily"));
        }
        "technical_3h" => {
            return technical_row_count(conn, Some("3h"));
        }
        "technical_20min" => {
            return technical_row_count(conn, Some("20min"));
        }
        "jin10" | "jin10-context" => "SELECT COUNT(*) FROM jin10_items",
        other => {
            // Only allow safe SQL identifiers so untrusted names cannot inject
            // via table interpolation. Existence is still checked separately.
            if !is_safe_sql_identifier(other) {
                return Ok(0);
            }
            return if table_exists(conn, other)? {
                conn.query_row(&format!("SELECT COUNT(*) FROM {other}"), [], |row| {
                    row.get(0)
                })
                .map_err(Into::into)
            } else {
                Ok(0)
            };
        }
    };
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(Into::into)
}

fn jin10_context(conn: &Connection) -> Result<Value> {
    const MAX_ITEMS: usize = 20;
    let mut stmt = conn.prepare(
        "SELECT id, content_json, attention_score, item_time FROM jin10_items \
         ORDER BY attention_score DESC, item_time DESC LIMIT ?",
    )?;
    let items = stmt
        .query_map(params![MAX_ITEMS as i64], |row| {
            let id: String = row.get(0)?;
            let content_json: String = row.get(1)?;
            let attention_score: f64 = row.get(2)?;
            let item_time: i64 = row.get(3)?;
            Ok((id, content_json, attention_score, item_time))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|(id, content_json, attention_score, item_time)| {
            // Pass the stored content_json to the LLM, ensuring id/attention fields are present.
            let mut payload: Value = serde_json::from_str(&content_json)
                .unwrap_or_else(|_| json!({ "raw": content_json }));
            if let Some(object) = payload.as_object_mut() {
                object.insert("id".to_string(), json!(id));
                object.insert("attention_score".to_string(), json!(attention_score));
                object.entry("time").or_insert(json!(item_time));
            }
            payload
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "query": "get-jin10-context",
        "item_count": items.len(),
        "items": items,
        "id_field": "id",
        "attention_note": "Return jin10_attention: [{id, score}] with score in 0.0-1.0 for items that actually influenced the analysis."
    }))
}

fn technical_context(conn: &Connection, tickers: &[String], ticker: Option<&str>) -> Result<Value> {
    let tickers = effective_technical_tickers(tickers, ticker);
    Ok(json!({
        "query": "get-technical-context",
        "source": "sqlite.technical_series",
        "daily": technical_snapshots_from_db(conn, "daily", &tickers)?,
        "three_hour": technical_snapshots_from_db(conn, "3h", &tickers)?,
        "twenty_minute": technical_snapshots_from_db(conn, "20min", &tickers)?
    }))
}

fn technical_interval_context(
    conn: &Connection,
    interval: &str,
    tickers: &[String],
    ticker: Option<&str>,
) -> Result<Value> {
    let tickers = effective_technical_tickers(tickers, ticker);
    Ok(json!({
        "query": interval,
        "source": "sqlite.technical_series",
        "items": technical_snapshots_from_db(conn, interval, &tickers)?
    }))
}

fn effective_technical_tickers(tickers: &[String], ticker: Option<&str>) -> Vec<String> {
    let mut out = tickers
        .iter()
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if out.is_empty() {
        if let Some(ticker) = ticker.map(str::trim).filter(|value| !value.is_empty()) {
            out.push(ticker.to_ascii_uppercase());
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Compact latest-per-ticker snapshots for one interval from the run database.
fn technical_snapshots_from_db(
    conn: &Connection,
    interval: &str,
    tickers: &[String],
) -> Result<Vec<Value>> {
    let mut snapshots = Vec::new();
    for ticker in tickers {
        let rows = load_technical_series(conn, ticker, interval)?;
        if let Some(snap) = latest_snapshot(ticker, interval, &rows, TECHNICAL_CONTEXT_KEYS) {
            snapshots.push(snap);
        }
    }
    Ok(snapshots)
}

const TECHNICAL_CONTEXT_KEYS: &[&str] = &[
    "Close",
    "Return",
    "LogReturn",
    "Gap",
    "Body",
    "BETA5",
    "BETA20",
    "CORR5",
    "CORR20",
    "CNTD5",
    "CNTD20",
    "CNTP5",
    "CNTP20",
    "RSQR5",
    "RSQR20",
    "VSTD5",
    "VSTD20",
    "WVMA5",
    "WVMA20",
    "IMAX5",
    "IMAX20",
    "IMIN5",
    "IMIN20",
];

fn role_summaries_context(conn: &Connection, ctx: &RuntimeContext) -> Result<Value> {
    let mut sql = String::from("SELECT * FROM role_turn_summaries WHERE run_id = ?");
    let mut params: Vec<rusqlite::types::Value> =
        vec![rusqlite::types::Value::Text(ctx.run_id.clone())];
    if ctx.phase != 0 {
        sql.push_str(" AND phase = ?");
        params.push(ctx.phase.into());
    }
    if !ctx.role.is_empty() {
        sql.push_str(" AND role = ?");
        params.push(rusqlite::types::Value::Text(ctx.role.clone()));
    }
    sql.push_str(" ORDER BY created_at DESC LIMIT 40");
    let items = query_rows(conn, &sql, params)?;
    Ok(json!({"query": "role-summaries", "run_id": ctx.run_id, "items": items}))
}

fn is_safe_sql_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
        [table],
        |row| row.get::<_, bool>(0),
    )
    .map_err(Into::into)
}

fn query_rows(
    conn: &Connection,
    sql: &str,
    params: Vec<rusqlite::types::Value>,
) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(sql)?;
    let columns = stmt
        .column_names()
        .into_iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let rows = stmt
        .query_map(params_from_iter(params), |row| {
            let mut item = serde_json::Map::new();
            for (index, column) in columns.iter().enumerate() {
                item.insert(column.clone(), sqlite_value(row, index)?);
            }
            Ok(Value::Object(item))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn sqlite_value(row: &Row<'_>, index: usize) -> rusqlite::Result<Value> {
    use rusqlite::types::ValueRef;
    Ok(match row.get_ref(index)? {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => json!(value),
        ValueRef::Real(value) => json!(value),
        ValueRef::Text(value) => {
            let text = String::from_utf8_lossy(value).to_string();
            serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text))
        }
        ValueRef::Blob(value) => json!(value),
    })
}
