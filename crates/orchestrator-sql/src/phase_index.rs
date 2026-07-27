//! Post-phase compressor index: summaries → details, and unified attention ledger.
//!
//! Runtime authority for phase_summary rows is the in-memory [`PhaseSummaryMemoryIndex`].
//! Completed phase batches can also be persisted immediately with
//! [`persist_phase_summary_batch`].

use crate::schema::{canonical_json, ensure_run_exists, now_ms, payload_hash};
use anyhow::Result;
use md5::{Digest, Md5};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

pub const DEFAULT_PHASE_SUMMARY_LIMIT: usize = 20;

#[derive(Debug, Clone)]
pub struct PhaseSummaryQuery<'a> {
    pub ticker: Option<&'a str>,
    pub source_phase: Option<i64>,
    pub role: Option<&'a str>,
    pub topic_id: Option<&'a str>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for PhaseSummaryQuery<'_> {
    fn default() -> Self {
        Self {
            ticker: None,
            source_phase: None,
            role: None,
            topic_id: None,
            limit: DEFAULT_PHASE_SUMMARY_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PhaseSummaryDetailQuery {
    pub limit: usize,
    pub offset: usize,
}

impl Default for PhaseSummaryDetailQuery {
    fn default() -> Self {
        Self {
            limit: DEFAULT_PHASE_SUMMARY_LIMIT,
            offset: 0,
        }
    }
}

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

/// In-memory phase summary row (same shape as SQLite / tool JSON).
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

/// In-memory phase_summary detail row.
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
pub struct PhaseSummaryPhaseBatch {
    pub source_phase: i64,
    pub summaries: Vec<PhaseSummaryRow>,
    pub details: Vec<PhaseSummaryDetailRow>,
}

impl PhaseSummaryPhaseBatch {
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
        let summary_items: Vec<Value> = self
            .summaries
            .iter()
            .map(|row| {
                let detail_count = self
                    .details
                    .iter()
                    .filter(|detail| detail.summary_id == row.id)
                    .count();
                summary_row_to_value(row, detail_count)
            })
            .collect();
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
            "note": "In-memory phase_summary batch; SQLite flush happens at run end."
        })
    }
}

/// Run-scoped phase_summary memory index (authoritative during the run).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PhaseSummaryMemoryIndex {
    pub run_id: String,
    pub phases: BTreeMap<i64, PhaseSummaryPhaseBatch>,
}

impl PhaseSummaryMemoryIndex {
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

    pub fn merge(&mut self, batch: PhaseSummaryPhaseBatch) {
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
                let detail_count = batch
                    .details
                    .iter()
                    .filter(|detail| detail.summary_id == row.id)
                    .count();
                items.push(summary_row_to_value(row, detail_count));
            }
        }
        json!({
            "query": "phase_summaries",
            "item_count": items.len(),
            "items": items,
            "source": "phase_summary_memory",
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
        self.query_visible_summaries(
            run_id,
            current_phase,
            &PhaseSummaryQuery {
                ticker,
                ..Default::default()
            },
        )
    }

    pub fn query_visible_summaries(
        &self,
        run_id: &str,
        current_phase: i64,
        query: &PhaseSummaryQuery<'_>,
    ) -> Result<Value> {
        let max_source_phase = prior_phase_bound(run_id, current_phase)?;
        if self.run_id != run_id {
            return Ok(empty_phase_summaries("run not visible"));
        }
        let mut items = self
            .phases
            .iter()
            .filter(|(phase, _)| **phase <= max_source_phase)
            .flat_map(|(_, batch)| batch.summaries.iter())
            .filter(|row| row.run_id == run_id)
            .filter(|row| {
                query
                    .source_phase
                    .is_none_or(|phase| row.source_phase == phase)
            })
            .filter(|row| {
                query
                    .ticker
                    .filter(|value| !value.is_empty())
                    .is_none_or(|ticker| {
                        row.ticker == ticker || row.ticker.is_empty() || row.ticker == "__ALL__"
                    })
            })
            .filter(|row| {
                query
                    .role
                    .filter(|value| !value.is_empty())
                    .is_none_or(|role| row.role == role)
            })
            .filter(|row| {
                query
                    .topic_id
                    .filter(|value| !value.is_empty())
                    .is_none_or(|topic_id| row.topic_id.as_deref() == Some(topic_id))
            })
            .collect::<Vec<_>>();
        items.sort_by_key(|row| (row.source_phase, row.created_at, row.id.clone()));
        let total_count = items.len();
        let limit = query.limit.clamp(1, 100);
        let rows = items
            .into_iter()
            .skip(query.offset)
            .take(limit)
            .map(|row| {
                let detail_count = self
                    .phases
                    .get(&row.source_phase)
                    .map(|batch| {
                        batch
                            .details
                            .iter()
                            .filter(|detail| detail.summary_id == row.id)
                            .count()
                    })
                    .unwrap_or(0);
                summary_row_to_value(row, detail_count)
            })
            .collect::<Vec<_>>();
        Ok(summary_query_response(
            rows,
            total_count,
            query,
            current_phase,
            "phase_summary_memory",
        ))
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
        self.query_visible_details(
            run_id,
            current_phase,
            summary_id,
            PhaseSummaryDetailQuery::default(),
        )
    }

    pub fn query_visible_details(
        &self,
        run_id: &str,
        current_phase: i64,
        summary_id: &str,
        query: PhaseSummaryDetailQuery,
    ) -> Result<Value> {
        let max_source_phase = prior_phase_bound(run_id, current_phase)?;
        if summary_id.trim().is_empty() {
            anyhow::bail!("summary_id is required");
        }
        if self.run_id != run_id {
            return Ok(empty_phase_details(
                summary_id,
                "summary not found or not visible",
            ));
        }
        let parent_phase = self.phases.iter().find_map(|(phase, batch)| {
            (*phase <= max_source_phase
                && batch.summaries.iter().any(|row| {
                    row.id == summary_id && row.run_id == run_id && row.source_phase == *phase
                }))
            .then_some(*phase)
        });
        let Some(parent_phase) = parent_phase else {
            return Ok(empty_phase_details(
                summary_id,
                "summary not found or not visible",
            ));
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
        let total_count = items.len();
        let limit = query.limit.clamp(1, 100);
        let items = items
            .into_iter()
            .skip(query.offset)
            .take(limit)
            .collect::<Vec<_>>();
        let next_offset = query.offset.saturating_add(items.len());
        Ok(json!({
            "query": "phase_summary_details",
            "summary_id": summary_id,
            "item_count": items.len(),
            "total_count": total_count,
            "truncated": next_offset < total_count,
            "next_cursor": (next_offset < total_count).then(|| next_offset.to_string()),
            "items": items,
            "source": "phase_summary_memory",
            "source_policy": "current_run_prior_phases_only",
            "status": if total_count == 0 { "empty" } else { "available" }
        }))
    }

    pub fn expand_summary(&self, id: &str) -> Option<Value> {
        for batch in self.phases.values() {
            if let Some(row) = batch.summaries.iter().find(|r| r.id == id) {
                let detail_count = self
                    .phases
                    .get(&row.source_phase)
                    .map(|batch| {
                        batch
                            .details
                            .iter()
                            .filter(|detail| detail.summary_id == row.id)
                            .count()
                    })
                    .unwrap_or(0);
                let mut v = summary_row_to_value(row, detail_count);
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
            total += persist_phase_summary_batch_inner(&tx, &self.run_id, batch)?;
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
        "total_count": 0,
        "truncated": false,
        "next_cursor": Value::Null,
        "applied_filters": {},
        "visible_phase_range": Value::Null,
        "source_policy": "current_run_prior_phases_only",
        "items": [],
        "source": "phase_summary_memory",
        "status": "empty",
        "note": note
    })
}

fn empty_phase_details(summary_id: &str, note: &str) -> Value {
    json!({
        "query": "phase_summary_details",
        "summary_id": summary_id,
        "item_count": 0,
        "total_count": 0,
        "truncated": false,
        "next_cursor": Value::Null,
        "items": [],
        "source": "phase_summary_memory",
        "source_policy": "current_run_prior_phases_only",
        "status": "not_visible",
        "note": note
    })
}

fn summary_row_to_value(row: &PhaseSummaryRow, detail_count: usize) -> Value {
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
        "detail_count": detail_count,
        "created_at": row.created_at,
        "recency_weight": recency_weight,
    })
}

fn summary_query_response(
    rows: Vec<Value>,
    total_count: usize,
    query: &PhaseSummaryQuery<'_>,
    current_phase: i64,
    source: &str,
) -> Value {
    let next_offset = query.offset.saturating_add(rows.len());
    json!({
        "query": "phase_summaries",
        "item_count": rows.len(),
        "total_count": total_count,
        "truncated": next_offset < total_count,
        "next_cursor": (next_offset < total_count).then(|| next_offset.to_string()),
        "applied_filters": {
            "ticker": query.ticker,
            "source_phase": query.source_phase,
            "role": query.role,
            "topic_id": query.topic_id,
            "limit": query.limit.clamp(1, 100),
            "cursor": query.offset.to_string()
        },
        "visible_phase_range": {
            "minimum": 1,
            "maximum": current_phase - 1
        },
        "source_policy": "current_run_prior_phases_only",
        "items": rows,
        "source": source,
        "status": if total_count == 0 { "empty" } else { "available" }
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

/// Persist exactly one completed phase_summary batch in one transaction.
///
/// Existing rows are cleared only for the same `(run_id, source_phase)` pair.
pub fn persist_phase_summary_batch(
    conn: &Connection,
    run_id: &str,
    batch: &PhaseSummaryPhaseBatch,
) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let written = persist_phase_summary_batch_inner(&tx, run_id, batch)?;
    tx.commit()?;
    Ok(written)
}

fn persist_phase_summary_batch_inner(
    conn: &Connection,
    run_id: &str,
    batch: &PhaseSummaryPhaseBatch,
) -> Result<usize> {
    if run_id.trim().is_empty() {
        anyhow::bail!("run_id is required to persist a phase_summary batch");
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
        anyhow::bail!("phase_summary batch rows must match run_id and source_phase");
    }
    if batch.details.iter().any(|detail| {
        !batch
            .summaries
            .iter()
            .any(|summary| summary.id == detail.summary_id)
    }) {
        anyhow::bail!("phase_summary detail must reference a summary in the same batch");
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
    query_phase_summaries(
        conn,
        run_id,
        current_phase,
        &PhaseSummaryQuery {
            ticker,
            ..Default::default()
        },
    )
}

pub fn query_phase_summaries(
    conn: &Connection,
    run_id: &str,
    current_phase: i64,
    query: &PhaseSummaryQuery<'_>,
) -> Result<Value> {
    prior_phase_bound(run_id, current_phase)?;
    let ticker = query.ticker.filter(|value| !value.trim().is_empty());
    let role = query.role.filter(|value| !value.trim().is_empty());
    let topic_id = query.topic_id.filter(|value| !value.trim().is_empty());
    let limit = query.limit.clamp(1, 100) as i64;
    let offset = query.offset as i64;
    let total_count: i64 = conn.query_row(
        r#"
        SELECT COUNT(*)
        FROM phase_summaries
        WHERE run_id = ?1 AND source_phase < ?2
          AND (?3 IS NULL OR source_phase = ?3)
          AND (?4 IS NULL OR ticker = ?4 OR ticker = '' OR ticker = '__ALL__')
          AND (?5 IS NULL OR role = ?5)
          AND (?6 IS NULL OR topic_id = ?6)
        "#,
        params![
            run_id,
            current_phase,
            query.source_phase,
            ticker,
            role,
            topic_id
        ],
        |row| row.get(0),
    )?;
    let mut stmt = conn.prepare(
        r#"
        SELECT s.id, s.run_id, s.source_phase, s.role, s.ticker, s.topic_id,
               s.summary, s.summary_json, s.confidence, s.created_at,
               (SELECT COUNT(*) FROM phase_summary_details d
                WHERE d.summary_id=s.id AND d.run_id=s.run_id
                  AND d.source_phase=s.source_phase) AS detail_count
        FROM phase_summaries s
        WHERE s.run_id = ?1 AND s.source_phase < ?2
          AND (?3 IS NULL OR s.source_phase = ?3)
          AND (?4 IS NULL OR s.ticker = ?4 OR s.ticker = '' OR s.ticker = '__ALL__')
          AND (?5 IS NULL OR s.role = ?5)
          AND (?6 IS NULL OR s.topic_id = ?6)
        ORDER BY s.source_phase ASC, s.created_at ASC, s.id ASC
        LIMIT ?7 OFFSET ?8
        "#,
    )?;
    let rows = stmt
        .query_map(
            params![
                run_id,
                current_phase,
                query.source_phase,
                ticker,
                role,
                topic_id,
                limit,
                offset
            ],
            |row| {
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
                    "detail_count": row.get::<_, i64>("detail_count")?,
                    "created_at": row.get::<_, i64>("created_at")?,
                    "recency_weight": recency_weight,
                }))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(summary_query_response(
        rows,
        total_count.max(0) as usize,
        query,
        current_phase,
        "sqlite",
    ))
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
    query_phase_summary_details(
        conn,
        run_id,
        current_phase,
        summary_id,
        PhaseSummaryDetailQuery::default(),
    )
}

pub fn query_phase_summary_details(
    conn: &Connection,
    run_id: &str,
    current_phase: i64,
    summary_id: &str,
    query: PhaseSummaryDetailQuery,
) -> Result<Value> {
    prior_phase_bound(run_id, current_phase)?;
    if summary_id.trim().is_empty() {
        anyhow::bail!("summary_id is required");
    }
    let parent_visible = conn
        .query_row(
            r#"
            SELECT 1 FROM phase_summaries
            WHERE id=?1 AND run_id=?2 AND source_phase < ?3
            "#,
            params![summary_id, run_id, current_phase],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !parent_visible {
        return Ok(empty_phase_details(
            summary_id,
            "summary not found or not visible",
        ));
    }
    let total_count: i64 = conn.query_row(
        r#"
        SELECT COUNT(*)
        FROM phase_summary_details d
        JOIN phase_summaries s
          ON s.id=d.summary_id AND s.run_id=d.run_id AND s.source_phase=d.source_phase
        WHERE d.summary_id=?1 AND d.run_id=?2 AND d.source_phase < ?3
        "#,
        params![summary_id, run_id, current_phase],
        |row| row.get(0),
    )?;
    let limit = query.limit.clamp(1, 100) as i64;
    let offset = query.offset as i64;
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
        ORDER BY d.sort_order ASC, d.created_at ASC, d.id ASC
        LIMIT ?4 OFFSET ?5
        "#,
    )?;
    let rows = stmt
        .query_map(
            params![summary_id, run_id, current_phase, limit, offset],
            |row| {
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
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let total_count = total_count.max(0) as usize;
    let next_offset = query.offset.saturating_add(rows.len());
    Ok(json!({
        "query": "phase_summary_details",
        "summary_id": summary_id,
        "item_count": rows.len(),
        "total_count": total_count,
        "truncated": next_offset < total_count,
        "next_cursor": (next_offset < total_count).then(|| next_offset.to_string()),
        "items": rows,
        "source": "sqlite",
        "source_policy": "current_run_prior_phases_only",
        "status": if total_count == 0 { "empty" } else { "available" }
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

    #[test]
    fn summary_queries_filter_paginate_and_hide_future_or_other_runs() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("filtered.sqlite")).unwrap();
        ensure_schema(&conn).unwrap();
        for (run_id, phase, role, ticker, topic) in [
            ("run-a", 1, "analyst.technical", "QQQ", None),
            ("run-a", 1, "analyst.news_macro", "QQQ", None),
            ("run-a", 2, "mediator.topic", "QQQ", Some("topic-1")),
            ("run-a", 3, "manager.research", "QQQ", None),
            ("run-b", 1, "analyst.technical", "QQQ", None),
        ] {
            upsert_phase_summary(
                &conn,
                &PhaseSummaryInput {
                    run_id: run_id.to_string(),
                    source_phase: phase,
                    role: role.to_string(),
                    ticker: ticker.to_string(),
                    topic_id: topic.map(ToString::to_string),
                    summary: format!("{run_id}-{phase}-{role}"),
                    summary_json: json!({}),
                    confidence: 0.5,
                },
            )
            .unwrap();
        }
        let first = query_phase_summaries(
            &conn,
            "run-a",
            3,
            &PhaseSummaryQuery {
                ticker: Some("QQQ"),
                source_phase: Some(1),
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(first["item_count"], 1);
        assert_eq!(first["total_count"], 2);
        assert_eq!(first["truncated"], true);
        assert_eq!(first["visible_phase_range"]["maximum"], 2);
        let cursor = first["next_cursor"].as_str().unwrap().parse().unwrap();
        let second = query_phase_summaries(
            &conn,
            "run-a",
            3,
            &PhaseSummaryQuery {
                ticker: Some("QQQ"),
                source_phase: Some(1),
                limit: 1,
                offset: cursor,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(second["item_count"], 1);
        assert_eq!(second["truncated"], false);
        assert!(first["items"][0]["id"] != second["items"][0]["id"]);
        assert!(first["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["source_phase"] == 1 && item["run_id"] == "run-a"));
    }

    #[test]
    fn invisible_detail_parent_returns_generic_not_visible() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("hidden.sqlite")).unwrap();
        ensure_schema(&conn).unwrap();
        let id = upsert_phase_summary(
            &conn,
            &PhaseSummaryInput {
                run_id: "other-run".to_string(),
                source_phase: 1,
                role: "analyst.technical".to_string(),
                ticker: "QQQ".to_string(),
                topic_id: None,
                summary: "hidden".to_string(),
                summary_json: json!({}),
                confidence: 0.5,
            },
        )
        .unwrap();
        let output =
            query_phase_summary_details(&conn, "run-a", 3, &id, Default::default()).unwrap();
        assert_eq!(output["status"], "not_visible");
        assert_eq!(output["item_count"], 0);
        assert!(!output.to_string().contains("other-run"));
    }
}
