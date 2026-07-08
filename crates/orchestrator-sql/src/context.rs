use crate::{
    memory::{read_prior_memory, PriorMemoryQuery},
    outcome::track_record,
    AGGREGATE_TICKER,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use orchestrator_core::{MarketRegime, RetrievalBudget};
use rusqlite::{params, params_from_iter, Connection, Row};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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
        "technical" | "technical_context" => return technical_context(conn),
        "technical_daily" => return technical_interval_context(conn, "daily"),
        "technical_3h" => return technical_interval_context(conn, "3h"),
        "technical_20min" => return technical_interval_context(conn, "20min"),
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
        "turn_context" => turn_context(conn, &ctx),
        "prior_memory" => prior_memory_context(conn, request, &ctx),
        "track_record" => track_record_context(conn, &ctx),
        "agent_accuracy" => agent_accuracy_context(conn),
        "compose_context" => compose_context(conn, request, &ctx),
        other => anyhow::bail!("unsupported read_run_context kind {other:?}"),
    }
}

#[derive(Debug, Clone)]
struct ContextBlock {
    context_type: String,
    context_ref: String,
    ticker: String,
    item_time: String,
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
    if ctx.phase >= 2 {
        collect_jin10_blocks(conn, &mut blocks)?;
        collect_technical_blocks(conn, ctx, &mut blocks)?;
        collect_external_blocks(conn, ctx, &mut blocks)?;
        collect_turn_history_blocks(conn, ctx, request.topic_id.as_deref(), &mut blocks)?;
    }

    for block in &mut blocks {
        block.weight += ticker_weight(&block.ticker, &ctx.ticker);
        block.weight += freshness_weight(&block.item_time);
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

    if request.persist_context {
        if let Some(turn_id) = request.turn_id.as_deref().filter(|value| !value.is_empty()) {
            write_turn_context_items(conn, turn_id, ctx, request.topic_id.as_deref(), &selected)?;
        }
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
                token_budget: request.token_budget.unwrap_or(2048).max(1),
                max_items: 20,
                min_quality: 0.0,
            },
            include_body: true,
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
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
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
                    .get::<_, String>("item_time")
                    .or_else(|_| row.get::<_, String>("created_at"))?,
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
    let mut stmt = conn.prepare(
        r#"
        SELECT event_key, item_time, content, item_json
        FROM jin10_items
        ORDER BY item_time DESC
        LIMIT 40
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        let item_json: String = row.get("item_json")?;
        Ok(ContextBlock {
            context_type: "jin10".to_string(),
            context_ref: format!("jin10_items:{}", row.get::<_, String>("event_key")?),
            ticker: String::new(),
            item_time: row.get("item_time")?,
            title: "Jin10".to_string(),
            content: row.get("content")?,
            weight: 1.0,
            source_table: "jin10_items".to_string(),
            item_json: serde_json::from_str(&item_json).unwrap_or(Value::String(item_json)),
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
    for (interval, context_type) in [
        ("daily", "technical_daily"),
        ("3h", "technical_3h"),
        ("20min", "technical_20min"),
    ] {
        let sql = "SELECT id, ticker, kline_time, indicator_name, indicator_value, unit, model, payload_json
             FROM technical_indicators
             WHERE interval = ? AND (? = '' OR ticker = ?)
             ORDER BY kline_time DESC
             LIMIT 40";
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(params![interval, ctx.ticker, ctx.ticker], |row| {
            let id: i64 = row.get("id")?;
            let payload_json: String = row.get("payload_json")?;
            let name: String = row.get("indicator_name")?;
            let value: f64 = row.get("indicator_value")?;
            let unit: String = row.get("unit")?;
            let model: String = row.get("model")?;
            Ok(ContextBlock {
                context_type: context_type.to_string(),
                context_ref: format!("technical_indicators:{id}"),
                ticker: row.get("ticker")?,
                item_time: row.get("kline_time")?,
                title: format!("{context_type} {name}"),
                content: format!("{name}={value}{unit} model={model}"),
                weight: 1.5,
                source_table: "technical_indicators".to_string(),
                item_json: serde_json::from_str(&payload_json)
                    .unwrap_or(Value::String(payload_json)),
            })
        })?;
        blocks.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(())
}

fn collect_external_blocks(
    conn: &Connection,
    ctx: &RuntimeContext,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    collect_simple_external_blocks(
        conn,
        ExternalTable {
            table: "youtube_videos",
            context_type: "youtube",
            key_column: "video_id",
            time_column: "published_at",
            title_column: "title",
            content_column: "title",
        },
        ctx,
        blocks,
    )?;
    collect_youtube_transcript_blocks(conn, ctx, blocks)?;
    collect_social_blocks(conn, ctx, blocks)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct ExternalTable {
    table: &'static str,
    context_type: &'static str,
    key_column: &'static str,
    time_column: &'static str,
    title_column: &'static str,
    content_column: &'static str,
}

fn collect_simple_external_blocks(
    conn: &Connection,
    source: ExternalTable,
    ctx: &RuntimeContext,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    let table = source.table;
    let key_column = source.key_column;
    let time_column = source.time_column;
    let title_column = source.title_column;
    let content_column = source.content_column;
    let sql = format!(
        "SELECT {key_column} AS item_key, ticker, {time_column} AS item_time,
                {title_column} AS title, {content_column} AS content, item_json
         FROM {table}
         WHERE (? = '' OR ticker = ?)
         ORDER BY {time_column} DESC
         LIMIT 40"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![ctx.ticker, ctx.ticker], |row| {
        let item_json: String = row.get("item_json")?;
        Ok(ContextBlock {
            context_type: source.context_type.to_string(),
            context_ref: format!("{table}:{}", row.get::<_, String>("item_key")?),
            ticker: row.get("ticker")?,
            item_time: row.get("item_time")?,
            title: row.get("title")?,
            content: row.get("content")?,
            weight: 1.0,
            source_table: table.to_string(),
            item_json: serde_json::from_str(&item_json).unwrap_or(Value::String(item_json)),
        })
    })?;
    blocks.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    Ok(())
}

fn collect_youtube_transcript_blocks(
    conn: &Connection,
    ctx: &RuntimeContext,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, video_id, ticker, transcript, segments_json, language, provider, imported_at
        FROM youtube_transcripts
        WHERE (? = '' OR ticker = ?)
        ORDER BY imported_at DESC
        LIMIT 20
        "#,
    )?;
    let rows = stmt.query_map(params![ctx.ticker, ctx.ticker], |row| {
        let id: i64 = row.get("id")?;
        let segments_json: String = row.get("segments_json")?;
        let video_id: String = row.get("video_id")?;
        Ok(ContextBlock {
            context_type: "youtube_transcript".to_string(),
            context_ref: format!("youtube_transcripts:{id}"),
            ticker: row.get("ticker")?,
            item_time: row.get("imported_at")?,
            title: format!("YouTube transcript {video_id}"),
            content: row.get("transcript")?,
            weight: 1.0,
            source_table: "youtube_transcripts".to_string(),
            item_json: json!({
                "video_id": video_id,
                "segments": serde_json::from_str::<Value>(&segments_json).unwrap_or(Value::String(segments_json)),
                "language": row.get::<_, String>("language")?,
                "provider": row.get::<_, String>("provider")?
            }),
        })
    })?;
    blocks.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    Ok(())
}

fn collect_social_blocks(
    conn: &Connection,
    ctx: &RuntimeContext,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        r#"
        SELECT source, item_key, ticker, item_time, title, content, item_json
        FROM social_items
        WHERE source IN ('reddit', 'x') AND (? = '' OR ticker = ?)
        ORDER BY item_time DESC
        LIMIT 80
        "#,
    )?;
    let rows = stmt.query_map(params![ctx.ticker, ctx.ticker], |row| {
        let item_json: String = row.get("item_json")?;
        let source: String = row.get("source")?;
        Ok(ContextBlock {
            context_type: source,
            context_ref: format!("social_items:{}", row.get::<_, String>("item_key")?),
            ticker: row.get("ticker")?,
            item_time: row.get("item_time")?,
            title: row.get("title")?,
            content: row.get("content")?,
            weight: 1.0,
            source_table: "social_items".to_string(),
            item_json: serde_json::from_str(&item_json).unwrap_or(Value::String(item_json)),
        })
    })?;
    blocks.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    Ok(())
}

fn collect_turn_history_blocks(
    conn: &Connection,
    ctx: &RuntimeContext,
    topic_id: Option<&str>,
    blocks: &mut Vec<ContextBlock>,
) -> Result<()> {
    let topic_filter = topic_id.unwrap_or_default();
    let mut stmt = conn.prepare(
        r#"
        SELECT id, turn_id, item_type, role, tool_name, content_json, content_text, created_at
        FROM agent_turn_items
        WHERE run_id = ?
          AND item_type IN ('user_message', 'assistant_message')
          AND (? = '' OR session_id LIKE '%' || ? || '%')
        ORDER BY id DESC
        LIMIT 12
        "#,
    )?;
    let rows = stmt.query_map(params![&ctx.run_id, topic_filter, topic_filter], |row| {
        let id: i64 = row.get("id")?;
        let content_json: String = row.get("content_json")?;
        let role: String = row.get("role")?;
        let item_type: String = row.get("item_type")?;
        Ok(ContextBlock {
            context_type: "turn_history".to_string(),
            context_ref: format!("agent_turn_items:{id}"),
            ticker: String::new(),
            item_time: row.get("created_at")?,
            title: format!("{role} {item_type}"),
            content: row.get("content_text")?,
            weight: 0.5,
            source_table: "agent_turn_items".to_string(),
            item_json: serde_json::from_str(&content_json).unwrap_or(Value::String(content_json)),
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

fn freshness_weight(item_time: &str) -> f64 {
    DateTime::parse_from_rfc3339(item_time)
        .map(|time| {
            let age_days = (Utc::now() - time.with_timezone(&Utc)).num_days().max(0) as f64;
            (2.0 / (1.0 + age_days / 7.0)).max(0.1)
        })
        .unwrap_or(0.5)
}

fn write_turn_context_items(
    conn: &mut Connection,
    turn_id: &str,
    ctx: &RuntimeContext,
    topic_id: Option<&str>,
    blocks: &[ContextBlock],
) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "DELETE FROM turn_context_items WHERE turn_id = ?",
        [turn_id],
    )?;
    for block in blocks {
        let item_json = block.value();
        let item_json_text = serde_json::to_string(&item_json)?;
        let content_hash = sha256_hex(&item_json_text);
        tx.execute(
            r#"
            INSERT INTO turn_context_items
                (run_id, turn_id, role, phase, ticker, item_time, topic_id,
                 context_type, context_ref, content, item_json, weight, content_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                ctx.run_id,
                turn_id,
                ctx.role,
                ctx.phase,
                block.ticker,
                block.item_time,
                topic_id,
                block.context_type,
                block.context_ref,
                block.content,
                item_json_text,
                block.weight,
                content_hash,
                Utc::now().to_rfc3339()
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
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
        "created_at": row.get::<_, String>("created_at")?
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

pub fn session_history_items(
    conn: &Connection,
    session_id: &str,
    limit: usize,
) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT turn_id, session_id, run_id, item_index, item_type, role, tool_call_id,
               tool_name, content_json, content_text, created_at
        FROM agent_turn_items
        WHERE session_id = ?
        ORDER BY id DESC
        LIMIT ?
        "#,
    )?;
    let mut rows = stmt
        .query_map(rusqlite::params![session_id, limit.max(1) as i64], |row| {
            let content_text: String = row.get("content_json")?;
            let content_json =
                serde_json::from_str::<Value>(&content_text).unwrap_or(Value::String(content_text));
            Ok(json!({
                "turn_id": row.get::<_, String>("turn_id")?,
                "session_id": row.get::<_, String>("session_id")?,
                "run_id": row.get::<_, String>("run_id")?,
                "item_index": row.get::<_, i64>("item_index")?,
                "item_type": row.get::<_, String>("item_type")?,
                "role": row.get::<_, String>("role")?,
                "tool_call_id": row.get::<_, String>("tool_call_id")?,
                "tool_name": row.get::<_, String>("tool_name")?,
                "content_json": content_json,
                "content_text": row.get::<_, String>("content_text")?,
                "created_at": row.get::<_, String>("created_at")?
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.reverse();
    Ok(rows)
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
        "get-technical-context" => return technical_context(conn),
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
        "technical": technical_context(conn)?,
        "jin10": jin10_context(conn)?,
        "sources": external_sources_context(conn)?
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
            "SELECT COUNT(*) FROM technical_indicators"
        }
        "technical_daily" => "SELECT COUNT(*) FROM technical_indicators WHERE interval = 'daily'",
        "technical_3h" => "SELECT COUNT(*) FROM technical_indicators WHERE interval = '3h'",
        "technical_20min" => "SELECT COUNT(*) FROM technical_indicators WHERE interval = '20min'",
        "jin10" | "jin10-context" => "SELECT COUNT(*) FROM jin10_items",
        "youtube" | "sources" => "SELECT COUNT(*) FROM youtube_videos",
        "youtube_transcripts" => "SELECT COUNT(*) FROM youtube_transcripts",
        "reddit" => "SELECT COUNT(*) FROM social_items WHERE source = 'reddit'",
        "x" => "SELECT COUNT(*) FROM social_items WHERE source = 'x'",
        other => {
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
    let rows = table_rows(conn, "jin10_items", &["item_time", "imported_at"])?;
    let items = rows
        .into_iter()
        .map(|item| {
            json!({
                "time": item.get("item_time").cloned().unwrap_or(Value::Null),
                "content": item.get("content").cloned().unwrap_or(Value::Null),
                "item": item
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"query": "get-jin10-context", "items": items}))
}

fn technical_context(conn: &Connection) -> Result<Value> {
    let daily = technical_rows(conn, "daily")?;
    let three_hour = technical_rows(conn, "3h")?;
    let twenty_minute = technical_rows(conn, "20min")?;
    Ok(json!({
        "query": "get-technical-context",
        "daily": daily,
        "three_hour": three_hour,
        "twenty_minute": twenty_minute
    }))
}

fn technical_interval_context(conn: &Connection, interval: &str) -> Result<Value> {
    Ok(json!({
        "query": interval,
        "items": technical_rows(conn, interval)?
    }))
}

fn technical_rows(conn: &Connection, interval: &str) -> Result<Vec<Value>> {
    if !table_exists(conn, "technical_indicators")? {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        "SELECT id, ticker, kline_time, indicator_name, indicator_value, unit, model, payload_json, imported_at
         FROM technical_indicators
         WHERE interval = ?
         ORDER BY kline_time DESC, indicator_name ASC
         LIMIT 80"
    )?;
    let rows = stmt
        .query_map([interval], |row| {
            let payload_json: String = row.get("payload_json")?;
            Ok(json!({
                "id": row.get::<_, i64>("id")?,
                "ticker": row.get::<_, String>("ticker")?,
                "kline_time": row.get::<_, String>("kline_time")?,
                "indicator_name": row.get::<_, String>("indicator_name")?,
                "indicator_value": row.get::<_, f64>("indicator_value")?,
                "unit": row.get::<_, String>("unit")?,
                "model": row.get::<_, String>("model")?,
                "payload": serde_json::from_str::<Value>(&payload_json).unwrap_or(Value::String(payload_json)),
                "imported_at": row.get::<_, String>("imported_at")?
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

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
    sql.push_str(" ORDER BY created_at DESC LIMIT 80");
    let items = query_rows(conn, &sql, params)?;
    Ok(json!({"query": "role-summaries", "run_id": ctx.run_id, "items": items}))
}

fn turn_context(conn: &Connection, ctx: &RuntimeContext) -> Result<Value> {
    let mut sql = String::from("SELECT * FROM turn_context_items WHERE run_id = ?");
    let params: Vec<rusqlite::types::Value> =
        vec![rusqlite::types::Value::Text(ctx.run_id.clone())];
    sql.push_str(" ORDER BY created_at DESC, id DESC LIMIT 80");
    let items = query_rows(conn, &sql, params)?;
    Ok(json!({"query": "turn-context", "run_id": ctx.run_id, "items": items}))
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
        [table],
        |row| row.get::<_, bool>(0),
    )
    .map_err(Into::into)
}

fn table_rows(conn: &Connection, table: &str, order_candidates: &[&str]) -> Result<Vec<Value>> {
    if !table_exists(conn, table)? {
        return Ok(Vec::new());
    }
    let columns = table_columns(conn, table)?;
    let order = order_candidates
        .iter()
        .find(|column| columns.iter().any(|existing| existing == **column))
        .map(|column| format!(" ORDER BY {column} DESC"))
        .unwrap_or_default();
    let sql = format!("SELECT * FROM {table}{order} LIMIT 80");
    query_rows(conn, &sql, Vec::new())
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

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>("name"))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into);
    columns
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

fn external_sources_context(conn: &Connection) -> Result<Value> {
    let mut rows = Vec::new();
    rows.extend(source_table_rows(
        conn,
        "youtube_videos",
        "youtube",
        "published_at",
        "title",
    )?);
    rows.extend(social_source_rows(conn)?);
    Ok(json!({"query": "source-items", "items": rows}))
}

fn source_table_rows(
    conn: &Connection,
    table: &str,
    source: &str,
    time_column: &str,
    content_column: &str,
) -> Result<Vec<Value>> {
    let sql = format!(
        "SELECT ticker, {time_column} AS item_time, {content_column} AS content, item_json
         FROM {table}
         ORDER BY imported_at DESC, {time_column} DESC
         LIMIT 40"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params![], |row| {
            let item_json: String = row.get("item_json")?;
            Ok(json!({
                "source": source,
                "ticker": row.get::<_, String>("ticker")?,
                "time": row.get::<_, String>("item_time")?,
                "content": row.get::<_, String>("content")?,
                "item": serde_json::from_str::<Value>(&item_json).unwrap_or(Value::String(item_json))
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn social_source_rows(conn: &Connection) -> Result<Vec<Value>> {
    let sql = "SELECT source, ticker, item_time, content, item_json
         FROM social_items
         WHERE source IN ('reddit', 'x')
         ORDER BY imported_at DESC, item_time DESC
         LIMIT 80";
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(params![], |row| {
            let item_json: String = row.get("item_json")?;
            Ok(json!({
                "source": row.get::<_, String>("source")?,
                "ticker": row.get::<_, String>("ticker")?,
                "time": row.get::<_, String>("item_time")?,
                "content": row.get::<_, String>("content")?,
                "item": serde_json::from_str::<Value>(&item_json).unwrap_or(Value::String(item_json))
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}
