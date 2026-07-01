use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, params_from_iter, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{ensure_schema, AGGREGATE_TICKER};

#[derive(Debug, Clone)]
pub struct PriorMemoryQuery {
    pub query: Option<String>,
    pub ticker: Option<String>,
    pub memory_types: Vec<String>,
    pub statuses: Vec<String>,
    pub include_expired: bool,
    pub limit: usize,
    pub include_body: bool,
}

#[derive(Debug, Clone)]
pub struct MemoryApplyResult {
    pub applied: usize,
    pub reused: usize,
    pub memory_ids: Vec<String>,
}

pub fn apply_memory_update_proposal(
    conn: &mut Connection,
    artifact: &Value,
) -> Result<MemoryApplyResult> {
    ensure_schema(conn)?;
    let proposals = artifact
        .get("proposals")
        .and_then(Value::as_array)
        .context("proposals must be an array")?;
    let source_role = artifact
        .get("source_role")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let run_id = artifact
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let tx = conn.transaction()?;
    let mut result = MemoryApplyResult {
        applied: 0,
        reused: 0,
        memory_ids: Vec::new(),
    };
    for proposal in proposals {
        let apply = apply_one_memory_proposal(&tx, proposal, run_id, source_role)?;
        result.applied += apply.applied;
        result.reused += apply.reused;
        result.memory_ids.extend(apply.memory_ids);
    }
    tx.commit()?;
    Ok(result)
}

fn apply_one_memory_proposal(
    conn: &Connection,
    proposal: &Value,
    run_id: &str,
    source_role: &str,
) -> Result<MemoryApplyResult> {
    let content_hash = memory_content_hash(proposal)?;
    if let Some(memory_id) = memory_id_for_hash(conn, &content_hash)? {
        return Ok(MemoryApplyResult {
            applied: 0,
            reused: 1,
            memory_ids: vec![memory_id],
        });
    }

    let memory_type = required_str(proposal, "update_type")?;
    let ticker = required_str(proposal, "ticker")?;
    let scope = required_str(proposal, "scope")?;
    let summary = required_str(proposal, "summary")?;
    let confidence = proposal
        .get("confidence")
        .and_then(Value::as_f64)
        .unwrap_or(0.5);
    let expires_at = proposal.get("expires_at").and_then(Value::as_str);
    let source_date = required_str(proposal, "source_date")?;
    let observed_at = required_str(proposal, "observed_at")?;
    let thesis = proposal.get("thesis").unwrap_or(&Value::Null);
    let memory_id = if memory_type == "thesis"
        && thesis
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "update")
    {
        let prior = thesis
            .get("prior_thesis_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .context("prior_thesis_id is required for thesis update")?;
        ensure_memory_exists(conn, prior)?;
        prior.to_string()
    } else {
        format!("mem-{}", Uuid::new_v4())
    };
    let version_id = format!("memv-{}", Uuid::new_v4());
    let version_index = next_version_index(conn, &memory_id)?;
    let now = Utc::now().to_rfc3339();

    if version_index == 1 {
        conn.execute(
            r#"
            INSERT INTO memory_items
                (memory_id, ticker, scope, memory_type, status, current_version_id, confidence,
                 expires_at, source_run_id, source_role, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                memory_id,
                ticker,
                scope,
                memory_type,
                version_id,
                confidence,
                expires_at,
                run_id,
                source_role,
                now,
                now
            ],
        )?;
    }

    conn.execute(
        r#"
        INSERT INTO memory_versions
            (version_id, memory_id, version_index, summary, body_json, evidence_refs_json,
             invalidation_conditions_json, follow_up_checks_json, source_run_id, source_role,
             source_date, observed_at, content_hash, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        params![
            version_id,
            memory_id,
            version_index,
            summary,
            serde_json::to_string(proposal)?,
            serde_json::to_string(proposal.get("evidence_refs").unwrap_or(&json!([])))?,
            serde_json::to_string(
                proposal
                    .get("invalidation_conditions")
                    .unwrap_or(&json!([]))
            )?,
            serde_json::to_string(proposal.get("follow_up_checks").unwrap_or(&json!([])))?,
            run_id,
            source_role,
            source_date,
            observed_at,
            content_hash,
            now
        ],
    )?;
    conn.execute(
        r#"
        UPDATE memory_items
        SET current_version_id = ?, confidence = ?, expires_at = ?, updated_at = ?, status = 'active'
        WHERE memory_id = ?
        "#,
        params![version_id, confidence, expires_at, now, memory_id],
    )?;
    if version_index > 1 {
        conn.execute(
            r#"
            INSERT OR IGNORE INTO memory_links (from_memory_id, to_memory_id, link_type, created_at)
            VALUES (?, ?, 'updates', ?)
            "#,
            params![memory_id, memory_id, now],
        )?;
    }
    refresh_memory_fts(conn, &memory_id)?;
    Ok(MemoryApplyResult {
        applied: 1,
        reused: 0,
        memory_ids: vec![memory_id],
    })
}

pub fn read_prior_memory(conn: &Connection, query: &PriorMemoryQuery) -> Result<Value> {
    ensure_schema(conn)?;
    let candidates = if query
        .query
        .as_deref()
        .is_some_and(|text| !text.trim().is_empty())
    {
        fts_memory_candidates(conn, query)?
    } else {
        recent_memory_candidates(conn, query)?
    };
    let mut candidates = candidates
        .into_iter()
        .filter(|candidate| candidate.allowed)
        .collect::<Vec<_>>();
    candidates.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.observed_at.cmp(&a.observed_at))
    });
    candidates.truncate(query.limit.max(1));
    Ok(json!({
        "query": "prior_memory",
        "items": candidates.into_iter().map(|item| item.value(query.include_body)).collect::<Vec<_>>()
    }))
}

pub fn rollback_memory_item(conn: &Connection, memory_id: &str, version_id: &str) -> Result<()> {
    ensure_schema(conn)?;
    let (confidence, expires_at): (f64, Option<String>) = conn.query_row(
        "SELECT json_extract(body_json, '$.confidence'), json_extract(body_json, '$.expires_at') FROM memory_versions WHERE memory_id = ? AND version_id = ?",
        params![memory_id, version_id],
        |row| Ok((row.get::<_, Option<f64>>(0)?.unwrap_or(0.5), row.get(1)?)),
    )?;
    let updated = conn.execute(
        "UPDATE memory_items SET current_version_id = ?, confidence = ?, expires_at = ?, updated_at = ? WHERE memory_id = ?",
        params![version_id, confidence, expires_at, Utc::now().to_rfc3339(), memory_id],
    )?;
    if updated == 0 {
        bail!("memory item/version not found for rollback");
    }
    refresh_memory_fts(conn, memory_id)
}

#[derive(Debug, Clone)]
struct MemoryCandidate {
    memory_id: String,
    version_id: String,
    ticker: String,
    scope: String,
    memory_type: String,
    summary: String,
    confidence: f64,
    observed_at: String,
    source_date: String,
    expires_at: Option<String>,
    status: String,
    score: f64,
    allowed: bool,
    evidence_refs: Value,
    invalidation_conditions: Value,
    follow_up_checks: Value,
    body: Value,
}

impl MemoryCandidate {
    fn value(self, include_body: bool) -> Value {
        let mut value = json!({
            "memory_id": self.memory_id,
            "version_id": self.version_id,
            "ticker": self.ticker,
            "scope": self.scope,
            "memory_type": self.memory_type,
            "summary": self.summary,
            "confidence": self.confidence,
            "observed_at": self.observed_at,
            "source_date": self.source_date,
            "expires_at": self.expires_at,
            "status": self.status,
            "score": self.score,
            "evidence_refs": self.evidence_refs,
            "invalidation_conditions": self.invalidation_conditions,
            "follow_up_checks": self.follow_up_checks
        });
        if include_body {
            value["body"] = self.body;
        }
        value
    }
}

fn fts_memory_candidates(
    conn: &Connection,
    query: &PriorMemoryQuery,
) -> Result<Vec<MemoryCandidate>> {
    let text = query.query.as_deref().unwrap_or_default();
    let (conditions, mut filter_params) = memory_filter_conditions(query);
    let filter_sql = if conditions.is_empty() {
        String::new()
    } else {
        format!(" AND {}", conditions.join(" AND "))
    };
    let sql = format!(
        r#"
        SELECT i.memory_id, v.version_id, i.ticker, i.scope, i.memory_type, v.summary,
               i.confidence, v.observed_at, v.source_date, i.expires_at, i.status,
               v.evidence_refs_json, v.invalidation_conditions_json, v.follow_up_checks_json,
               v.body_json, bm25(memory_search_fts) AS rank
        FROM memory_search_fts
        JOIN memory_items i ON i.memory_id = memory_search_fts.memory_id
        JOIN memory_versions v ON v.version_id = i.current_version_id
        WHERE memory_search_fts MATCH ?{filter_sql}
        "#
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut query_params = vec![fts_query(text)];
    query_params.append(&mut filter_params);
    let rows = stmt.query_map(params_from_iter(query_params), |row| {
        candidate_from_row(row, query, Some(row.get::<_, f64>("rank")?))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn recent_memory_candidates(
    conn: &Connection,
    query: &PriorMemoryQuery,
) -> Result<Vec<MemoryCandidate>> {
    let (conditions, filter_params) = memory_filter_conditions(query);
    let filter_sql = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    let sql = format!(
        r#"
        SELECT i.memory_id, v.version_id, i.ticker, i.scope, i.memory_type, v.summary,
               i.confidence, v.observed_at, v.source_date, i.expires_at, i.status,
               v.evidence_refs_json, v.invalidation_conditions_json, v.follow_up_checks_json,
               v.body_json
        FROM memory_items i
        JOIN memory_versions v ON v.version_id = i.current_version_id{filter_sql}
        "#
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(filter_params), |row| {
        candidate_from_row(row, query, None)
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn memory_filter_conditions(query: &PriorMemoryQuery) -> (Vec<String>, Vec<String>) {
    let mut conditions = Vec::new();
    let mut params = Vec::new();
    if let Some(ticker) = query.ticker.as_deref().filter(|value| !value.is_empty()) {
        conditions.push("(i.ticker = ? OR i.ticker = ?)".to_string());
        params.push(ticker.to_string());
        params.push(AGGREGATE_TICKER.to_string());
    }
    if !query.statuses.is_empty() {
        conditions.push(format!(
            "i.status IN ({})",
            placeholders(query.statuses.len())
        ));
        params.extend(query.statuses.iter().cloned());
    }
    if !query.memory_types.is_empty() {
        conditions.push(format!(
            "i.memory_type IN ({})",
            placeholders(query.memory_types.len())
        ));
        params.extend(query.memory_types.iter().cloned());
    }
    (conditions, params)
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

fn candidate_from_row(
    row: &rusqlite::Row<'_>,
    query: &PriorMemoryQuery,
    fts_rank: Option<f64>,
) -> rusqlite::Result<MemoryCandidate> {
    let status: String = row.get("status")?;
    let memory_type: String = row.get("memory_type")?;
    let ticker: String = row.get("ticker")?;
    let expires_at: Option<String> = row.get("expires_at")?;
    let confidence: f64 = row.get("confidence")?;
    let observed_at: String = row.get("observed_at")?;
    let mut score = confidence * 4.0 + freshness_score(&observed_at);
    if let Some(request_ticker) = query.ticker.as_deref().filter(|value| !value.is_empty()) {
        if ticker == request_ticker {
            score += 5.0;
        } else if ticker == AGGREGATE_TICKER {
            score += 1.0;
        }
    }
    if fts_rank.is_some() {
        score += 4.0;
    }
    if is_expired(expires_at.as_deref()) {
        score -= 10.0;
    }
    let allowed_status =
        query.statuses.is_empty() || query.statuses.iter().any(|item| item == &status);
    let allowed_type =
        query.memory_types.is_empty() || query.memory_types.iter().any(|item| item == &memory_type);
    let allowed_expiry = query.include_expired || !is_expired(expires_at.as_deref());
    let allowed_ticker = query
        .ticker
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(|value| ticker == value || ticker == AGGREGATE_TICKER)
        .unwrap_or(true);
    let allowed = allowed_status && allowed_type && allowed_expiry && allowed_ticker;
    Ok(MemoryCandidate {
        memory_id: row.get("memory_id")?,
        version_id: row.get("version_id")?,
        ticker,
        scope: row.get("scope")?,
        memory_type,
        summary: row.get("summary")?,
        confidence,
        observed_at,
        source_date: row.get("source_date")?,
        expires_at,
        status,
        score,
        allowed,
        evidence_refs: json_from_row(row, "evidence_refs_json"),
        invalidation_conditions: json_from_row(row, "invalidation_conditions_json"),
        follow_up_checks: json_from_row(row, "follow_up_checks_json"),
        body: json_from_row(row, "body_json"),
    })
}

fn json_from_row(row: &rusqlite::Row<'_>, column: &str) -> Value {
    row.get::<_, String>(column)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or(Value::Null)
}

fn refresh_memory_fts(conn: &Connection, memory_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM memory_search_fts WHERE memory_id = ?",
        [memory_id],
    )?;
    let row = conn.query_row(
        r#"
        SELECT i.memory_id, v.version_id, i.ticker, i.memory_type, i.status, v.summary, v.body_json
        FROM memory_items i
        JOIN memory_versions v ON v.version_id = i.current_version_id
        WHERE i.memory_id = ?
        "#,
        [memory_id],
        |row| {
            Ok((
                row.get::<_, String>("memory_id")?,
                row.get::<_, String>("version_id")?,
                row.get::<_, String>("ticker")?,
                row.get::<_, String>("memory_type")?,
                row.get::<_, String>("status")?,
                row.get::<_, String>("summary")?,
                row.get::<_, String>("body_json")?,
            ))
        },
    )?;
    if row.4 == "active" {
        conn.execute(
            "INSERT INTO memory_search_fts (memory_id, version_id, ticker, memory_type, summary, search_text) VALUES (?, ?, ?, ?, ?, ?)",
            params![row.0, row.1, row.2, row.3, row.5, row.6],
        )?;
    }
    Ok(())
}

fn ensure_memory_exists(conn: &Connection, memory_id: &str) -> Result<()> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM memory_items WHERE memory_id = ?",
        [memory_id],
        |row| row.get(0),
    )?;
    if count == 0 {
        bail!("prior memory {memory_id:?} does not exist");
    }
    Ok(())
}

fn next_version_index(conn: &Connection, memory_id: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COALESCE(MAX(version_index), 0) + 1 FROM memory_versions WHERE memory_id = ?",
        [memory_id],
        |row| row.get(0),
    )?)
}

fn memory_id_for_hash(conn: &Connection, content_hash: &str) -> Result<Option<String>> {
    let mut stmt =
        conn.prepare("SELECT memory_id FROM memory_versions WHERE content_hash = ? LIMIT 1")?;
    let mut rows = stmt.query([content_hash])?;
    Ok(rows.next()?.map(|row| row.get(0)).transpose()?)
}

fn memory_content_hash(value: &Value) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_string(value)?.as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .with_context(|| format!("{field} must be a non-empty string"))
}

fn is_expired(expires_at: Option<&str>) -> bool {
    expires_at
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.with_timezone(&Utc) < Utc::now())
        .unwrap_or(false)
}

fn freshness_score(observed_at: &str) -> f64 {
    DateTime::parse_from_rfc3339(observed_at)
        .map(|time| {
            let age_days = (Utc::now() - time.with_timezone(&Utc)).num_days().max(0) as f64;
            (2.0 / (1.0 + age_days / 30.0)).max(0.1)
        })
        .unwrap_or(0.5)
}

fn fts_query(text: &str) -> String {
    text.split_whitespace()
        .filter(|part| !part.is_empty())
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR ")
}
