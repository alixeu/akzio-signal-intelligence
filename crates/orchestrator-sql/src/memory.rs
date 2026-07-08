use anyhow::Result;
use chrono::{DateTime, Utc};
use orchestrator_core::{MarketRegime, RetrievalBudget};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{candidate::CandidateExperience, ensure_schema, AGGREGATE_TICKER};

#[derive(Debug, Clone)]
pub struct PriorMemoryQuery {
    pub ticker: Option<String>,
    pub market_regime: MarketRegime,
    pub budget: RetrievalBudget,
    pub include_body: bool,
}

#[derive(Debug, Clone)]
pub struct PromoteMemoryInput {
    pub candidate: CandidateExperience,
    pub quality_score: f64,
    pub recent_success_rate: f64,
}

pub fn promote_candidate_to_memory(
    conn: &Connection,
    input: &PromoteMemoryInput,
) -> Result<String> {
    ensure_schema(conn)?;
    let memory_id = format!("mem-{}", Uuid::new_v4());
    let version_id = format!("memv-{}", Uuid::new_v4());
    let now = Utc::now().to_rfc3339();
    let summary = format!(
        "Finding: {}\nRecommendation: {}",
        input.candidate.finding, input.candidate.recommendation
    );
    let body = json!({
        "candidate_id": input.candidate.id,
        "finding": input.candidate.finding,
        "recommendation": input.candidate.recommendation,
        "evidence": input.candidate.evidence_json,
        "counter_evidence": input.candidate.counter_evidence_json,
        "metrics": input.candidate.metrics_json,
    });
    let body_text = serde_json::to_string(&body)?;
    let content_hash = sha256_hex(&body_text);
    conn.execute(
        r#"
        INSERT INTO memory_items
            (memory_id, ticker, scope, memory_type, status, current_version_id, confidence,
             expires_at, source_run_id, source_role, created_at, updated_at, market_regime_json,
             quality_score, sample_count, recent_success_rate, reflection_version, promoted_from)
        VALUES (?, ?, ?, ?, 'active', ?, ?, NULL, '', 'reflection', ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        params![
            memory_id,
            scope_value_as_ticker(&input.candidate),
            input.candidate.scope,
            input.candidate.experience_type,
            version_id,
            input.candidate.confidence,
            now,
            now,
            serde_json::to_string(&input.candidate.market_regime_json)?,
            input.quality_score,
            input.candidate.sample_count,
            input.recent_success_rate,
            input.candidate.reflection_version,
            input.candidate.id,
        ],
    )?;
    conn.execute(
        r#"
        INSERT INTO memory_versions
            (version_id, memory_id, version_index, summary, body_json, evidence_refs_json,
             invalidation_conditions_json, follow_up_checks_json, source_run_id, source_role,
             source_date, observed_at, content_hash, created_at)
        VALUES (?, ?, 1, ?, ?, ?, '[]', '[]', '', 'reflection', ?, ?, ?, ?)
        "#,
        params![
            version_id,
            memory_id,
            summary,
            body_text,
            serde_json::to_string(&input.candidate.evidence_json)?,
            input.candidate.source_window,
            now,
            content_hash,
            now,
        ],
    )?;
    refresh_memory_fts(conn, &memory_id)?;
    Ok(memory_id)
}

pub fn degrade_stale_memories(
    conn: &Connection,
    scope: &str,
    scope_value: &str,
    memory_type: &str,
    min_quality: f64,
    except_promoted_from: Option<i64>,
) -> Result<usize> {
    ensure_schema(conn)?;
    let updated = conn.execute(
        r#"
        UPDATE memory_items
        SET status = 'inactive', updated_at = ?
        WHERE scope = ?
          AND (ticker = ? OR ? = '')
          AND memory_type = ?
          AND status = 'active'
          AND quality_score < ?
          AND (? IS NULL OR promoted_from IS NULL OR promoted_from != ?)
        "#,
        params![
            Utc::now().to_rfc3339(),
            scope,
            scope_value,
            scope_value,
            memory_type,
            min_quality,
            except_promoted_from,
            except_promoted_from,
        ],
    )?;
    Ok(updated)
}

pub fn read_prior_memory(conn: &Connection, query: &PriorMemoryQuery) -> Result<Value> {
    ensure_schema(conn)?;
    let mut candidates = active_memory_candidates(conn, query)?;
    candidates.sort_by(|a, b| {
        b.quality_score
            .partial_cmp(&a.quality_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.observed_at.cmp(&a.observed_at))
    });

    let mut used_tokens = 0usize;
    let mut selected = Vec::new();
    for candidate in candidates {
        if candidate.quality_score < query.budget.min_quality {
            continue;
        }
        if selected.len() >= query.budget.max_items {
            break;
        }
        let tokens = (candidate.summary.chars().count() / 4).max(1);
        if used_tokens + tokens > query.budget.token_budget {
            continue;
        }
        used_tokens += tokens;
        selected.push(candidate.value(query.include_body));
    }

    Ok(json!({
        "query": "prior_memory",
        "token_budget": query.budget.token_budget,
        "estimated_tokens": used_tokens,
        "items": selected,
    }))
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
    quality_score: f64,
    sample_count: i64,
    recent_success_rate: f64,
    market_regime_json: Value,
    observed_at: String,
    evidence_refs: Value,
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
            "quality_score": self.quality_score,
            "sample_count": self.sample_count,
            "recent_success_rate": self.recent_success_rate,
            "market_regime_json": self.market_regime_json,
            "observed_at": self.observed_at,
            "evidence_refs": self.evidence_refs,
        });
        if include_body {
            value["body"] = self.body;
        }
        value
    }
}

fn active_memory_candidates(
    conn: &Connection,
    query: &PriorMemoryQuery,
) -> Result<Vec<MemoryCandidate>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT i.memory_id, v.version_id, i.ticker, i.scope, i.memory_type, v.summary,
               i.confidence, i.quality_score, i.sample_count, i.recent_success_rate,
               i.market_regime_json, v.observed_at, v.evidence_refs_json, v.body_json, i.expires_at
        FROM memory_items i
        JOIN memory_versions v ON v.version_id = i.current_version_id
        WHERE i.status = 'active'
          AND (? = '' OR i.ticker = ? OR i.ticker = ?)
        "#,
    )?;
    let ticker = query.ticker.clone().unwrap_or_default();
    let rows = stmt.query_map(params![ticker, ticker, AGGREGATE_TICKER], |row| {
        let market_regime_json = parse_json(row.get::<_, String>(10)?);
        Ok((
            MemoryCandidate {
                memory_id: row.get(0)?,
                version_id: row.get(1)?,
                ticker: row.get(2)?,
                scope: row.get(3)?,
                memory_type: row.get(4)?,
                summary: row.get(5)?,
                confidence: row.get(6)?,
                quality_score: row.get(7)?,
                sample_count: row.get(8)?,
                recent_success_rate: row.get(9)?,
                market_regime_json,
                observed_at: row.get(11)?,
                evidence_refs: parse_json(row.get::<_, String>(12)?),
                body: parse_json(row.get::<_, String>(13)?),
            },
            row.get::<_, Option<String>>(14)?,
        ))
    })?;
    let mut candidates = Vec::new();
    for row in rows {
        let (candidate, expires_at) = row?;
        if is_expired(expires_at.as_deref()) {
            continue;
        }
        let regime: MarketRegime =
            serde_json::from_value(candidate.market_regime_json.clone()).unwrap_or_default();
        if regime.is_compatible_with(&query.market_regime) {
            candidates.push(candidate);
        }
    }
    Ok(candidates)
}

pub fn refresh_memory_fts(conn: &Connection, memory_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM memory_search_fts WHERE memory_id = ?",
        params![memory_id],
    )?;
    let row = conn.query_row(
        r#"
        SELECT i.memory_id, v.version_id, i.ticker, i.memory_type, i.status, v.summary, v.body_json
        FROM memory_items i
        JOIN memory_versions v ON v.version_id = i.current_version_id
        WHERE i.memory_id = ?
        "#,
        params![memory_id],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
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

fn scope_value_as_ticker(candidate: &CandidateExperience) -> String {
    if candidate.scope == "ticker" {
        candidate.scope_value.clone()
    } else {
        AGGREGATE_TICKER.to_string()
    }
}

fn is_expired(expires_at: Option<&str>) -> bool {
    expires_at
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|time| time.with_timezone(&Utc) < Utc::now())
        .unwrap_or(false)
}

fn parse_json(text: String) -> Value {
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        candidate::{insert_candidate_experience, pending_candidates, CandidateExperienceInput},
        connect,
    };

    #[test]
    fn promotes_candidate_and_reads_prior_memory() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("memory.sqlite")).unwrap();
        insert_candidate_experience(
            &conn,
            &CandidateExperienceInput {
                scope: "ticker".to_string(),
                scope_value: "QQQ".to_string(),
                experience_type: "calibration".to_string(),
                market_regime_json: json!({"volatility":"normal"}),
                finding: "pattern".to_string(),
                recommendation: "adjust".to_string(),
                evidence_json: json!([]),
                counter_evidence_json: json!([]),
                metrics_json: json!({}),
                sample_count: 6,
                sample_run_ids_json: json!(["run-1"]),
                confidence: 0.8,
                effect_size: 0.1,
                distiller_version: "v1".to_string(),
                reflection_version: "v1".to_string(),
                source_window: "2026-W01".to_string(),
            },
        )
        .unwrap();
        let candidate = pending_candidates(&conn).unwrap().remove(0);
        let memory_id = promote_candidate_to_memory(
            &conn,
            &PromoteMemoryInput {
                candidate,
                quality_score: 0.7,
                recent_success_rate: 0.75,
            },
        )
        .unwrap();
        assert!(memory_id.starts_with("mem-"));
        let result = read_prior_memory(
            &conn,
            &PriorMemoryQuery {
                ticker: Some("QQQ".to_string()),
                market_regime: MarketRegime {
                    volatility: "normal".to_string(),
                    ..Default::default()
                },
                budget: RetrievalBudget::default(),
                include_body: false,
            },
        )
        .unwrap();
        assert_eq!(result["items"].as_array().unwrap().len(), 1);
    }
}
