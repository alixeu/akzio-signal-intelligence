//! Post-phase compressor index: summaries → details, and unified attention ledger.
//!
//! Runtime authority for phase00 rows is the in-memory [`Phase00MemoryIndex`].
//! Completed phase batches can also be persisted immediately with
//! [`persist_phase00_batch`].

use crate::schema::{canonical_json, ensure_run_exists, now_ms, payload_hash};
use anyhow::Result;
use md5::{Digest, Md5};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PhaseSummaryInput {
    pub run_id: String,
    pub source_phase: i64,
    pub role: String,
    pub ticker: String,
    pub topic_id: Option<String>,
    pub summary: String,
    pub summary_json: Value,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct PhaseSummaryDetailInput {
    pub summary_id: String,
    pub run_id: String,
    pub source_phase: i64,
    pub detail: String,
    pub detail_json: Value,
    pub source_ref: String,
    pub sort_order: i64,
}

#[derive(Debug, Clone)]
pub struct AttentionEvent {
    pub run_id: String,
    pub turn_id: String,
    pub role: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub score: f64,
    pub phase: Option<i64>,
}

/// Stable summary id from run/phase/role/ticker/text.
pub fn phase_summary_id(
    run_id: &str,
    source_phase: i64,
    role: &str,
    ticker: &str,
    summary: &str,
) -> String {
    let mut hasher = Md5::new();
    hasher.update(run_id.as_bytes());
    hasher.update(b"|");
    hasher.update(source_phase.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(role.as_bytes());
    hasher.update(b"|");
    hasher.update(ticker.as_bytes());
    hasher.update(b"|");
    hasher.update(summary.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn phase_detail_id(summary_id: &str, sort_order: i64, detail: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(summary_id.as_bytes());
    hasher.update(b"|");
    hasher.update(sort_order.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(detail.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// In-memory phase00 summary row (same shape as SQLite / tool JSON).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseSummaryRow {
    pub id: String,
    pub run_id: String,
    pub source_phase: i64,
    pub role: String,
    pub ticker: String,
    pub topic_id: Option<String>,
    pub summary: String,
    pub summary_json: Value,
    pub confidence: f64,
    pub created_at: i64,
}

/// In-memory phase00 detail row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseSummaryDetailRow {
    pub id: String,
    pub summary_id: String,
    pub run_id: String,
    pub source_phase: i64,
    pub detail: String,
    pub detail_json: Value,
    pub source_ref: String,
    pub sort_order: i64,
    pub created_at: i64,
}

/// One phase's compressor batch before SQLite flush.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Phase00PhaseBatch {
    pub source_phase: i64,
    pub summaries: Vec<PhaseSummaryRow>,
    pub details: Vec<PhaseSummaryDetailRow>,
}

impl Phase00PhaseBatch {
    pub fn written(&self) -> usize {
        self.summaries.len() + self.details.len()
    }

    pub fn push_summary(&mut self, input: &PhaseSummaryInput) -> String {
        let id = phase_summary_id(
            &input.run_id,
            input.source_phase,
            &input.role,
            &input.ticker,
            &input.summary,
        );
        let created_at = chrono::Utc::now().timestamp();
        let recency_weight = 1.0 + 0.15 * (input.source_phase as f64);
        let _ = recency_weight;
        self.summaries.push(PhaseSummaryRow {
            id: id.clone(),
            run_id: input.run_id.clone(),
            source_phase: input.source_phase,
            role: input.role.clone(),
            ticker: input.ticker.clone(),
            topic_id: input.topic_id.clone(),
            summary: input.summary.clone(),
            summary_json: input.summary_json.clone(),
            confidence: input.confidence.clamp(0.0, 1.0),
            created_at,
        });
        id
    }

    pub fn push_detail(&mut self, input: &PhaseSummaryDetailInput) -> String {
        let id = phase_detail_id(&input.summary_id, input.sort_order, &input.detail);
        let created_at = chrono::Utc::now().timestamp();
        self.details.push(PhaseSummaryDetailRow {
            id: id.clone(),
            summary_id: input.summary_id.clone(),
            run_id: input.run_id.clone(),
            source_phase: input.source_phase,
            detail: input.detail.clone(),
            detail_json: input.detail_json.clone(),
            source_ref: input.source_ref.clone(),
            sort_order: input.sort_order,
            created_at,
        });
        id
    }

    /// Debug / prompt snapshot for one phase (no DB).
    pub fn debug_snapshot(&self) -> Value {
        let written = self.written();
        let summary_items: Vec<Value> = self.summaries.iter().map(summary_row_to_value).collect();
        let detail_items: Vec<Value> = self.details.iter().map(detail_row_to_value).collect();
        json!({
            "role": "compressor",
            "kind": "phase_compress",
            "source_phase": self.source_phase,
            "written": written,
            "status": "done",
            "summaries": summary_items,
            "details": detail_items,
            "attention": [],
            "summary_count": self.summaries.len(),
            "detail_count": self.details.len(),
            "attention_count": 0,
            "persisted": false,
            "note": "In-memory phase00 batch; SQLite flush happens at run end."
        })
    }
}

/// Run-scoped phase00 memory index (authoritative during the run).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Phase00MemoryIndex {
    pub run_id: String,
    pub phases: BTreeMap<i64, Phase00PhaseBatch>,
}

impl Phase00MemoryIndex {
    pub fn new(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            phases: BTreeMap::new(),
        }
    }

    pub fn from_state_value(value: &Value) -> Self {
        serde_json::from_value(value.clone()).unwrap_or_default()
    }

    pub fn to_state_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(json!({}))
    }

    pub fn merge(&mut self, batch: Phase00PhaseBatch) {
        if self.run_id.is_empty() {
            if let Some(first) = batch.summaries.first() {
                self.run_id = first.run_id.clone();
            }
        }
        self.phases.insert(batch.source_phase, batch);
    }

    /// Compatibility entry point. Missing visibility bounds fail closed.
    pub fn list_summaries(&self, max_source_phase: Option<i64>, ticker: Option<&str>) -> Value {
        let Some(max_source_phase) = max_source_phase.filter(|phase| *phase >= 0) else {
            return empty_phase_summaries("phase visibility requires current_phase > 0");
        };
        let mut items = Vec::new();
        for (phase, batch) in &self.phases {
            if *phase > max_source_phase {
                continue;
            }
            for row in &batch.summaries {
                if row.run_id != self.run_id {
                    continue;
                }
                if let Some(t) = ticker.filter(|t| !t.is_empty()) {
                    if row.ticker != t && !row.ticker.is_empty() && row.ticker != "__ALL__" {
                        continue;
                    }
                }
                items.push(summary_row_to_value(row));
            }
        }
        json!({
            "query": "phase_summaries",
            "item_count": items.len(),
            "items": items,
            "source": "phase00_memory",
            "note": "Newer source_phase has higher recency_weight; prefer recent summaries."
        })
    }

    /// Run- and phase-scoped summary index. Only phases before `current_phase` are visible.
    pub fn list_visible_summaries(
        &self,
        run_id: &str,
        current_phase: i64,
        ticker: Option<&str>,
    ) -> Result<Value> {
        let max_source_phase = prior_phase_bound(run_id, current_phase)?;
        if self.run_id != run_id {
            return Ok(empty_phase_summaries("run not visible"));
        }
        Ok(self.list_summaries(Some(max_source_phase), ticker))
    }

    /// Compatibility entry point without a visibility scope. It intentionally returns no rows.
    pub fn list_details(&self, summary_id: &str) -> Value {
        empty_phase_details(summary_id, "run_id and current_phase are required")
    }

    /// Run- and phase-scoped details. The parent summary must be visible first.
    pub fn list_visible_details(
        &self,
        run_id: &str,
        current_phase: i64,
        summary_id: &str,
    ) -> Result<Value> {
        let max_source_phase = prior_phase_bound(run_id, current_phase)?;
        if summary_id.trim().is_empty() {
            anyhow::bail!("summary_id is required");
        }
        if self.run_id != run_id {
            return Ok(empty_phase_details(summary_id, "summary not found or not visible"));
        }
        let parent_phase = self.phases.iter().find_map(|(phase, batch)| {
            (*phase <= max_source_phase
                && batch.summaries.iter().any(|row| {
                    row.id == summary_id
                        && row.run_id == run_id
                        && row.source_phase == *phase
                }))
            .then_some(*phase)
        });
        let Some(parent_phase) = parent_phase else {
            return Ok(empty_phase_details(summary_id, "summary not found or not visible"));
        };
        let mut items = Vec::new();
        if let Some(batch) = self.phases.get(&parent_phase) {
            for row in &batch.details {
                if row.summary_id == summary_id
                    && row.run_id == run_id
                    && row.source_phase == parent_phase
                {
                    items.push(detail_row_to_value(row));
                }
            }
        }
        items.sort_by_key(|item| item.get("sort_order").and_then(Value::as_i64).unwrap_or(0));
        Ok(json!({
            "query": "phase_summary_details",
            "summary_id": summary_id,
            "item_count": items.len(),
            "items": items,
            "source": "phase00_memory"
        }))
    }

    pub fn expand_summary(&self, id: &str) -> Option<Value> {
        for batch in self.phases.values() {
            if let Some(row) = batch.summaries.iter().find(|r| r.id == id) {
                let mut v = summary_row_to_value(row);
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("subject_kind".into(), json!("summary"));
                    obj.insert("subject_id".into(), json!(id));
                }
                return Some(v);
            }
        }
        None
    }

    pub fn expand_detail(&self, id: &str) -> Option<Value> {
        for batch in self.phases.values() {
            if let Some(row) = batch.details.iter().find(|r| r.id == id) {
                let mut v = detail_row_to_value(row);
                if let Some(obj) = v.as_object_mut() {
                    obj.insert("subject_kind".into(), json!("detail"));
                    obj.insert("subject_id".into(), json!(id));
                }
                return Some(v);
            }
        }
        None
    }

    /// Persist all phases to SQLite (idempotent clear + upsert per phase).
    pub fn flush(&self, conn: &Connection) -> Result<usize> {
        let tx = conn.unchecked_transaction()?;
        let mut total = 0usize;
        for batch in self.phases.values() {
            total += persist_phase00_batch_inner(&tx, &self.run_id, batch)?;
        }
        tx.commit()?;
        Ok(total)
    }
}

fn prior_phase_bound(run_id: &str, current_phase: i64) -> Result<i64> {
    if run_id.trim().is_empty() {
        anyhow::bail!("run_id is required for phase summary access");
    }
    if current_phase <= 0 {
        anyhow::bail!("current_phase must be greater than zero");
    }
    Ok(current_phase - 1)
}

fn empty_phase_summaries(note: &str) -> Value {
    json!({
        "query": "phase_summaries",
        "item_count": 0,
        "items": [],
        "source": "phase00_memory",
        "note": note
    })
}

fn empty_phase_details(summary_id: &str, note: &str) -> Value {
    json!({
        "query": "phase_summary_details",
        "summary_id": summary_id,
        "item_count": 0,
        "items": [],
        "source": "phase00_memory",
        "note": note
    })
}

fn summary_row_to_value(row: &PhaseSummaryRow) -> Value {
    let recency_weight = 1.0 + 0.15 * (row.source_phase as f64);
    json!({
        "id": row.id,
        "run_id": row.run_id,
        "source_phase": row.source_phase,
        "role": row.role,
        "ticker": row.ticker,
        "topic_id": row.topic_id,
        "summary": row.summary,
        "summary_json": row.summary_json,
        "confidence": row.confidence,
        "created_at": row.created_at,
        "recency_weight": recency_weight,
    })
}

fn detail_row_to_value(row: &PhaseSummaryDetailRow) -> Value {
    json!({
        "id": row.id,
        "summary_id": row.summary_id,
        "run_id": row.run_id,
        "source_phase": row.source_phase,
        "detail": row.detail,
        "detail_json": row.detail_json,
        "source_ref": row.source_ref,
        "sort_order": row.sort_order,
        "created_at": row.created_at,
    })
}

pub fn upsert_phase_summary(conn: &Connection, input: &PhaseSummaryInput) -> Result<String> {
    let id = phase_summary_id(
        &input.run_id,
        input.source_phase,
        &input.role,
        &input.ticker,
        &input.summary,
    );
    ensure_run_exists(
        conn,
        &input.run_id,
        &chrono::Utc::now()
            .date_naive()
            .format("%Y-%m-%d")
            .to_string(),
    )?;
    let created_at_ms = now_ms();
    let summary_json = canonical_json(&input.summary_json)?;
    let hash = payload_hash(&input.summary_json)?;
    conn.execute(
        r#"
        INSERT INTO phase_summaries
            (id,run_id,source_phase,role,ticker,topic_id,summary,summary_json,
             payload_schema_version,payload_hash,confidence,created_at_ms)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,1,?9,?10,?11)
        ON CONFLICT(id) DO UPDATE SET
            summary = excluded.summary,
            summary_json = excluded.summary_json,
            payload_hash = excluded.payload_hash,
            confidence = excluded.confidence,
            topic_id = excluded.topic_id
        "#,
        params![
            id,
            input.run_id,
            input.source_phase,
            input.role,
            input.ticker,
            input.topic_id,
            input.summary.chars().take(2048).collect::<String>(),
            summary_json,
            hash,
            input.confidence.clamp(0.0, 1.0),
            created_at_ms,
        ],
    )?;
    Ok(id)
}

pub fn upsert_phase_summary_detail(
    conn: &Connection,
    input: &PhaseSummaryDetailInput,
) -> Result<String> {
    let id = phase_detail_id(&input.summary_id, input.sort_order, &input.detail);
    ensure_run_exists(
        conn,
        &input.run_id,
        &chrono::Utc::now()
            .date_naive()
            .format("%Y-%m-%d")
            .to_string(),
    )?;
    let created_at_ms = now_ms();
    let detail_json = canonical_json(&input.detail_json)?;
    let hash = payload_hash(&input.detail_json)?;
    conn.execute(
        r#"
        INSERT INTO phase_summary_details
            (id,summary_id,run_id,source_phase,detail,detail_json,payload_schema_version,
             payload_hash,source_ref,sort_order,created_at_ms)
        VALUES (?1,?2,?3,?4,?5,?6,1,?7,?8,?9,?10)
        ON CONFLICT(id) DO UPDATE SET
            detail = excluded.detail,
            detail_json = excluded.detail_json,
            payload_hash = excluded.payload_hash,
            source_ref = excluded.source_ref,
            sort_order = excluded.sort_order
        "#,
        params![
            id,
            input.summary_id,
            input.run_id,
            input.source_phase,
            input.detail.chars().take(2048).collect::<String>(),
            detail_json,
            hash,
            input.source_ref,
            input.sort_order,
            created_at_ms,
        ],
    )?;
    Ok(id)
}

/// Clear compressor rows for one phase of a run (idempotent re-compress).
pub fn clear_phase_compress(conn: &Connection, run_id: &str, source_phase: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM phase_summary_details WHERE run_id = ?1 AND source_phase = ?2",
        params![run_id, source_phase],
    )?;
    conn.execute(
        "DELETE FROM phase_summaries WHERE run_id = ?1 AND source_phase = ?2",
        params![run_id, source_phase],
    )?;
    Ok(())
}

/// Persist exactly one completed phase00 batch in one transaction.
///
/// Existing rows are cleared only for the same `(run_id, source_phase)` pair.
pub fn persist_phase00_batch(
    conn: &Connection,
    run_id: &str,
    batch: &Phase00PhaseBatch,
) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let written = persist_phase00_batch_inner(&tx, run_id, batch)?;
    tx.commit()?;
    Ok(written)
}

fn persist_phase00_batch_inner(
    conn: &Connection,
    run_id: &str,
    batch: &Phase00PhaseBatch,
) -> Result<usize> {
    if run_id.trim().is_empty() {
        anyhow::bail!("run_id is required to persist a phase00 batch");
    }
    if batch.source_phase <= 0 {
        anyhow::bail!("source_phase must be greater than zero");
    }
    if batch
        .summaries
        .iter()
        .any(|row| row.run_id != run_id || row.source_phase != batch.source_phase)
        || batch
            .details
            .iter()
            .any(|row| row.run_id != run_id || row.source_phase != batch.source_phase)
    {
        anyhow::bail!("phase00 batch rows must match run_id and source_phase");
    }
    if batch.details.iter().any(|detail| {
        !batch
            .summaries
            .iter()
            .any(|summary| summary.id == detail.summary_id)
    }) {
        anyhow::bail!("phase00 detail must reference a summary in the same batch");
    }

    clear_phase_compress(conn, run_id, batch.source_phase)?;
    for row in &batch.summaries {
        upsert_phase_summary(
            conn,
            &PhaseSummaryInput {
                run_id: row.run_id.clone(),
                source_phase: row.source_phase,
                role: row.role.clone(),
                ticker: row.ticker.clone(),
                topic_id: row.topic_id.clone(),
                summary: row.summary.clone(),
                summary_json: row.summary_json.clone(),
                confidence: row.confidence,
            },
        )?;
    }
    for row in &batch.details {
        upsert_phase_summary_detail(
            conn,
            &PhaseSummaryDetailInput {
                summary_id: row.summary_id.clone(),
                run_id: row.run_id.clone(),
                source_phase: row.source_phase,
                detail: row.detail.clone(),
                detail_json: row.detail_json.clone(),
                source_ref: row.source_ref.clone(),
                sort_order: row.sort_order,
            },
        )?;
    }
    Ok(batch.written())
}

pub fn record_attention(conn: &Connection, event: &AttentionEvent) -> Result<String> {
    let tx = conn.unchecked_transaction()?;
    let id = record_attention_inner(&tx, event)?;
    tx.commit()?;
    Ok(id)
}

fn record_attention_inner(conn: &Connection, event: &AttentionEvent) -> Result<String> {
    ensure_run_exists(
        conn,
        &event.run_id,
        &chrono::Utc::now()
            .date_naive()
            .format("%Y-%m-%d")
            .to_string(),
    )?;
    let id = Uuid::new_v4().to_string();
    let score = event.score.clamp(0.0, 1.0);
    conn.execute(
        r#"
        INSERT INTO attention_ledger
            (id,run_id,turn_id,role,subject_kind,subject_id,score,phase,created_at_ms)
        VALUES (?1,?2,NULLIF(?3,''),?4,?5,?6,?7,?8,?9)
        "#,
        params![
            id,
            event.run_id,
            event.turn_id,
            event.role,
            event.subject_kind,
            event.subject_id,
            score,
            event.phase,
            now_ms(),
        ],
    )?;
    // Cache latest score on jin10_items for convenience ordering.
    if event.subject_kind == "jin10" {
        let updated = conn.execute(
            "UPDATE jin10_items SET latest_attention_score=?1, legacy_attention=0 WHERE id=?2",
            params![score, event.subject_id],
        )?;
        if updated != 1 {
            anyhow::bail!(
                "cannot record attention for missing Jin10 item {}",
                event.subject_id
            );
        }
    }
    Ok(id)
}

pub fn record_attention_batch(conn: &Connection, events: &[AttentionEvent]) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let mut n = 0usize;
    for event in events {
        if event.subject_id.trim().is_empty() {
            continue;
        }
        record_attention_inner(&tx, event)?;
        n += 1;
    }
    tx.commit()?;
    Ok(n)
}

pub fn list_phase_summaries(
    conn: &Connection,
    run_id: &str,
    current_phase: i64,
    ticker: Option<&str>,
) -> Result<Value> {
    prior_phase_bound(run_id, current_phase)?;
    let mut sql = String::from(
        r#"
        SELECT id, run_id, source_phase, role, ticker, topic_id, summary, summary_json,
               confidence, created_at
        FROM phase_summaries
        WHERE run_id = ?1 AND source_phase < ?2
        "#,
    );
    let mut params: Vec<Value> = vec![json!(run_id), json!(current_phase)];
    if let Some(t) = ticker.filter(|t| !t.is_empty()) {
        sql.push_str(" AND (ticker = ? OR ticker = '' OR ticker = '__ALL__')");
        params.push(json!(t));
    }
    sql.push_str(" ORDER BY source_phase ASC, created_at ASC");

    let mut stmt = conn.prepare(&sql)?;
    let bind: Vec<Box<dyn rusqlite::types::ToSql>> = params
        .iter()
        .map(|v| -> Box<dyn rusqlite::types::ToSql> {
            match v {
                Value::String(s) => Box::new(s.clone()),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Box::new(i)
                    } else {
                        Box::new(n.as_f64().unwrap_or(0.0))
                    }
                }
                _ => Box::new(v.to_string()),
            }
        })
        .collect();
    let bind_refs: Vec<&dyn rusqlite::types::ToSql> = bind.iter().map(|b| b.as_ref()).collect();

    let rows = stmt
        .query_map(bind_refs.as_slice(), |row| {
            let summary_json: String = row.get("summary_json")?;
            let source_phase: i64 = row.get("source_phase")?;
            // Recency weight: newer source_phase → higher attention prior.
            let recency_weight = 1.0 + 0.15 * (source_phase as f64);
            Ok(json!({
                "id": row.get::<_, String>("id")?,
                "run_id": row.get::<_, String>("run_id")?,
                "source_phase": source_phase,
                "role": row.get::<_, String>("role")?,
                "ticker": row.get::<_, String>("ticker")?,
                "topic_id": row.get::<_, Option<String>>("topic_id")?,
                "summary": row.get::<_, String>("summary")?,
                "summary_json": serde_json::from_str::<Value>(&summary_json)
                    .unwrap_or(Value::String(summary_json)),
                "confidence": row.get::<_, f64>("confidence")?,
                "created_at": row.get::<_, i64>("created_at")?,
                "recency_weight": recency_weight,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(json!({
        "query": "phase_summaries",
        "item_count": rows.len(),
        "items": rows,
        "note": "Newer source_phase has higher recency_weight; prefer recent summaries."
    }))
}

/// Summaries for one exact `source_phase` (post-compress snapshot).
pub fn list_phase_summaries_for_phase(
    conn: &Connection,
    run_id: &str,
    source_phase: i64,
) -> Result<Value> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, run_id, source_phase, role, ticker, topic_id, summary, summary_json,
               confidence, created_at
        FROM phase_summaries
        WHERE run_id = ?1 AND source_phase = ?2
        ORDER BY created_at ASC
        "#,
    )?;
    let rows = stmt
        .query_map(params![run_id, source_phase], |row| {
            let summary_json: String = row.get("summary_json")?;
            let source_phase: i64 = row.get("source_phase")?;
            let recency_weight = 1.0 + 0.15 * (source_phase as f64);
            Ok(json!({
                "id": row.get::<_, String>("id")?,
                "run_id": row.get::<_, String>("run_id")?,
                "source_phase": source_phase,
                "role": row.get::<_, String>("role")?,
                "ticker": row.get::<_, String>("ticker")?,
                "topic_id": row.get::<_, Option<String>>("topic_id")?,
                "summary": row.get::<_, String>("summary")?,
                "summary_json": serde_json::from_str::<Value>(&summary_json)
                    .unwrap_or(Value::String(summary_json)),
                "confidence": row.get::<_, f64>("confidence")?,
                "created_at": row.get::<_, i64>("created_at")?,
                "recency_weight": recency_weight,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({
        "query": "phase_summaries_for_phase",
        "source_phase": source_phase,
        "item_count": rows.len(),
        "items": rows
    }))
}

/// Details for one exact `source_phase` (post-compress snapshot).
pub fn list_phase_details_for_phase(
    conn: &Connection,
    run_id: &str,
    source_phase: i64,
) -> Result<Value> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, summary_id, run_id, source_phase, detail, detail_json, source_ref, sort_order, created_at
        FROM phase_summary_details
        WHERE run_id = ?1 AND source_phase = ?2
        ORDER BY summary_id ASC, sort_order ASC, created_at ASC
        "#,
    )?;
    let rows = stmt
        .query_map(params![run_id, source_phase], |row| {
            let detail_json: String = row.get("detail_json")?;
            Ok(json!({
                "id": row.get::<_, String>("id")?,
                "summary_id": row.get::<_, String>("summary_id")?,
                "run_id": row.get::<_, String>("run_id")?,
                "source_phase": row.get::<_, i64>("source_phase")?,
                "detail": row.get::<_, String>("detail")?,
                "detail_json": serde_json::from_str::<Value>(&detail_json)
                    .unwrap_or(Value::String(detail_json)),
                "source_ref": row.get::<_, String>("source_ref")?,
                "sort_order": row.get::<_, i64>("sort_order")?,
                "created_at": row.get::<_, i64>("created_at")?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({
        "query": "phase_summary_details_for_phase",
        "source_phase": source_phase,
        "item_count": rows.len(),
        "items": rows
    }))
}

/// Full compressor debug snapshot for one source_phase.
pub fn compressor_debug_snapshot(
    conn: &Connection,
    run_id: &str,
    source_phase: i64,
    written: usize,
) -> Result<Value> {
    let summaries = list_phase_summaries_for_phase(conn, run_id, source_phase)?;
    let details = list_phase_details_for_phase(conn, run_id, source_phase)?;
    let attention = list_attention(conn, run_id, None, None, None, 100)?;
    Ok(json!({
        "role": "compressor",
        "kind": "phase_compress",
        "source_phase": source_phase,
        "written": written,
        "status": "done",
        "summaries": summaries.get("items").cloned().unwrap_or_else(|| json!([])),
        "details": details.get("items").cloned().unwrap_or_else(|| json!([])),
        "attention": attention.get("items").cloned().unwrap_or_else(|| json!([])),
        "summary_count": summaries.get("item_count").cloned().unwrap_or(json!(0)),
        "detail_count": details.get("item_count").cloned().unwrap_or(json!(0)),
        "attention_count": attention.get("item_count").cloned().unwrap_or(json!(0)),
    }))
}

pub fn list_phase_summary_details(
    conn: &Connection,
    run_id: &str,
    current_phase: i64,
    summary_id: &str,
) -> Result<Value> {
    prior_phase_bound(run_id, current_phase)?;
    if summary_id.trim().is_empty() {
        anyhow::bail!("summary_id is required");
    }
    let mut stmt = conn.prepare(
        r#"
        SELECT d.id, d.summary_id, d.run_id, d.source_phase, d.detail, d.detail_json,
               d.source_ref, d.sort_order, d.created_at
        FROM phase_summary_details d
        JOIN phase_summaries s
          ON s.id = d.summary_id
         AND s.run_id = d.run_id
         AND s.source_phase = d.source_phase
        WHERE d.summary_id = ?1
          AND d.run_id = ?2
          AND d.source_phase < ?3
        ORDER BY d.sort_order ASC, d.created_at ASC
        "#,
    )?;
    let rows = stmt
        .query_map(params![summary_id, run_id, current_phase], |row| {
            let detail_json: String = row.get("detail_json")?;
            Ok(json!({
                "id": row.get::<_, String>("id")?,
                "summary_id": row.get::<_, String>("summary_id")?,
                "run_id": row.get::<_, String>("run_id")?,
                "source_phase": row.get::<_, i64>("source_phase")?,
                "detail": row.get::<_, String>("detail")?,
                "detail_json": serde_json::from_str::<Value>(&detail_json)
                    .unwrap_or(Value::String(detail_json)),
                "source_ref": row.get::<_, String>("source_ref")?,
                "sort_order": row.get::<_, i64>("sort_order")?,
                "created_at": row.get::<_, i64>("created_at")?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({
        "query": "phase_summary_details",
        "summary_id": summary_id,
        "item_count": rows.len(),
        "items": rows
    }))
}

pub fn list_attention(
    conn: &Connection,
    run_id: &str,
    role: Option<&str>,
    turn_id: Option<&str>,
    min_score: Option<f64>,
    limit: usize,
) -> Result<Value> {
    let mut sql = String::from(
        r#"
        SELECT id, run_id, turn_id, role, subject_kind, subject_id, score, phase, created_at
        FROM attention_ledger
        WHERE run_id = ?1
        "#,
    );
    let mut vals: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(run_id.to_string())];
    if let Some(r) = role.filter(|r| !r.is_empty()) {
        sql.push_str(" AND role = ?");
        vals.push(Box::new(r.to_string()));
    }
    if let Some(t) = turn_id.filter(|t| !t.is_empty()) {
        sql.push_str(" AND turn_id = ?");
        vals.push(Box::new(t.to_string()));
    }
    if let Some(m) = min_score {
        sql.push_str(" AND score >= ?");
        vals.push(Box::new(m));
    }
    sql.push_str(" ORDER BY score DESC, created_at DESC LIMIT ?");
    vals.push(Box::new(limit.max(1) as i64));

    let mut stmt = conn.prepare(&sql)?;
    let refs: Vec<&dyn rusqlite::types::ToSql> = vals.iter().map(|v| v.as_ref()).collect();
    let rows = stmt
        .query_map(refs.as_slice(), |row| {
            Ok(json!({
                "id": row.get::<_, String>("id")?,
                "run_id": row.get::<_, String>("run_id")?,
                "turn_id": row.get::<_, String>("turn_id")?,
                "role": row.get::<_, String>("role")?,
                "subject_kind": row.get::<_, String>("subject_kind")?,
                "subject_id": row.get::<_, String>("subject_id")?,
                "score": row.get::<_, f64>("score")?,
                "phase": row.get::<_, Option<i64>>("phase")?,
                "created_at": row.get::<_, i64>("created_at")?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({
        "query": "attention",
        "item_count": rows.len(),
        "items": rows,
        "note": "Use attention_expand with subject_kind+subject_id to load full content."
    }))
}

/// Hydrate full content for attended subjects.
pub fn expand_attention_subjects(
    conn: &Connection,
    subjects: &[(String, String)],
) -> Result<Value> {
    let mut items = Vec::new();
    for (kind, id) in subjects {
        let kind = kind.trim();
        let id = id.trim();
        if kind.is_empty() || id.is_empty() {
            continue;
        }
        let payload = match kind {
            "jin10" => expand_jin10(conn, id)?,
            "summary" => expand_summary(conn, id)?,
            "detail" => expand_detail(conn, id)?,
            other => json!({
                "subject_kind": other,
                "subject_id": id,
                "error": "unsupported subject_kind"
            }),
        };
        items.push(payload);
    }
    Ok(json!({
        "query": "attention_expand",
        "item_count": items.len(),
        "items": items
    }))
}

fn expand_jin10(conn: &Connection, id: &str) -> Result<Value> {
    let row = conn
        .query_row(
            "SELECT id, content_json, attention_score, item_time, imported_at FROM jin10_items WHERE id = ?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, f64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()?;
    Ok(match row {
        Some((id, content_json, attention_score, item_time, imported_at)) => {
            let content: Value =
                serde_json::from_str(&content_json).unwrap_or(json!({ "raw": content_json }));
            json!({
                "subject_kind": "jin10",
                "subject_id": id,
                "attention_score": attention_score,
                "item_time": item_time,
                "imported_at": imported_at,
                "content": content
            })
        }
        None => json!({
            "subject_kind": "jin10",
            "subject_id": id,
            "error": "not_found"
        }),
    })
}

fn expand_summary(conn: &Connection, id: &str) -> Result<Value> {
    let row = conn
        .query_row(
            r#"
            SELECT id, run_id, source_phase, role, ticker, topic_id, summary, summary_json, confidence, created_at
            FROM phase_summaries WHERE id = ?1
            "#,
            params![id],
            |row| {
                let summary_json: String = row.get(7)?;
                let summary_json =
                    serde_json::from_str::<Value>(&summary_json).unwrap_or(Value::String(summary_json));
                Ok(json!({
                    "subject_kind": "summary",
                    "subject_id": row.get::<_, String>(0)?,
                    "run_id": row.get::<_, String>(1)?,
                    "source_phase": row.get::<_, i64>(2)?,
                    "role": row.get::<_, String>(3)?,
                    "ticker": row.get::<_, String>(4)?,
                    "topic_id": row.get::<_, Option<String>>(5)?,
                    "summary": row.get::<_, String>(6)?,
                    "summary_json": summary_json,
                    "confidence": row.get::<_, f64>(8)?,
                    "created_at": row.get::<_, i64>(9)?,
                }))
            },
        )
        .optional()?;
    Ok(row.unwrap_or_else(|| {
        json!({
            "subject_kind": "summary",
            "subject_id": id,
            "error": "not_found"
        })
    }))
}

fn expand_detail(conn: &Connection, id: &str) -> Result<Value> {
    let row = conn
        .query_row(
            r#"
            SELECT id, summary_id, run_id, source_phase, detail, detail_json, source_ref, sort_order, created_at
            FROM phase_summary_details WHERE id = ?1
            "#,
            params![id],
            |row| {
                let detail_json: String = row.get(5)?;
                let detail_json =
                    serde_json::from_str::<Value>(&detail_json).unwrap_or(Value::String(detail_json));
                Ok(json!({
                    "subject_kind": "detail",
                    "subject_id": row.get::<_, String>(0)?,
                    "summary_id": row.get::<_, String>(1)?,
                    "run_id": row.get::<_, String>(2)?,
                    "source_phase": row.get::<_, i64>(3)?,
                    "detail": row.get::<_, String>(4)?,
                    "detail_json": detail_json,
                    "source_ref": row.get::<_, String>(6)?,
                    "sort_order": row.get::<_, i64>(7)?,
                    "created_at": row.get::<_, i64>(8)?,
                }))
            },
        )
        .optional()?;
    Ok(row.unwrap_or_else(|| {
        json!({
            "subject_kind": "detail",
            "subject_id": id,
            "error": "not_found"
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{connect, ensure_schema};

    #[test]
    fn compress_summary_detail_and_attention_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("t.sqlite")).unwrap();
        ensure_schema(&conn).unwrap();
        let run_id = "run-1";
        clear_phase_compress(&conn, run_id, 1).unwrap();
        let sid = upsert_phase_summary(
            &conn,
            &PhaseSummaryInput {
                run_id: run_id.to_string(),
                source_phase: 1,
                role: "compressor".to_string(),
                ticker: "QQQ".to_string(),
                topic_id: None,
                summary: "QQQ mixed tech/news".to_string(),
                summary_json: json!({"direction": "mixed"}),
                confidence: 0.6,
            },
        )
        .unwrap();
        let did = upsert_phase_summary_detail(
            &conn,
            &PhaseSummaryDetailInput {
                summary_id: sid.clone(),
                run_id: run_id.to_string(),
                source_phase: 1,
                detail: "close above MA".to_string(),
                detail_json: json!({}),
                source_ref: "analyst.technical".to_string(),
                sort_order: 0,
            },
        )
        .unwrap();
        record_attention(
            &conn,
            &AttentionEvent {
                run_id: run_id.to_string(),
                turn_id: "turn-1".to_string(),
                role: "mediator.topic".to_string(),
                subject_kind: "summary".to_string(),
                subject_id: sid.clone(),
                score: 0.9,
                phase: Some(2),
            },
        )
        .unwrap();

        let summaries = list_phase_summaries(&conn, run_id, 2, Some("QQQ")).unwrap();
        assert_eq!(summaries["item_count"], 1);
        assert!(summaries["items"][0]["recency_weight"].as_f64().unwrap() > 1.0);

        let details = list_phase_summary_details(&conn, run_id, 2, &sid).unwrap();
        assert_eq!(details["item_count"], 1);
        assert_eq!(details["items"][0]["id"], did);

        let att = list_attention(&conn, run_id, None, None, None, 10).unwrap();
        assert_eq!(att["item_count"], 1);

        let expanded =
            expand_attention_subjects(&conn, &[("summary".into(), sid), ("detail".into(), did)])
                .unwrap();
        assert_eq!(expanded["item_count"], 2);
        assert!(expanded["items"][0].get("error").is_none());

        let snap = compressor_debug_snapshot(&conn, run_id, 1, 2).unwrap();
        assert_eq!(snap["role"], "compressor");
        assert_eq!(snap["kind"], "phase_compress");
        assert_eq!(snap["source_phase"], 1);
        assert_eq!(snap["summary_count"], 1);
        assert_eq!(snap["detail_count"], 1);
        assert!(snap["summaries"].as_array().unwrap().len() == 1);
    }
}
