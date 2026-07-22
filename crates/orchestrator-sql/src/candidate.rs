use crate::schema::now_ms;
use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct CandidateExperienceInput {
    pub scope: String,
    pub scope_value: String,
    pub experience_type: String,
    pub market_regime_json: Value,
    pub finding: String,
    pub recommendation: String,
    pub evidence_json: Value,
    pub counter_evidence_json: Value,
    pub metrics_json: Value,
    pub sample_count: i64,
    pub sample_run_ids_json: Value,
    pub confidence: f64,
    pub effect_size: f64,
    pub distiller_version: String,
    pub reflection_version: String,
    pub source_window: String,
}

#[derive(Debug, Clone)]
pub struct CandidateExperience {
    pub id: i64,
    pub scope: String,
    pub scope_value: String,
    pub experience_type: String,
    pub market_regime_json: Value,
    pub finding: String,
    pub recommendation: String,
    pub evidence_json: Value,
    pub counter_evidence_json: Value,
    pub metrics_json: Value,
    pub sample_count: i64,
    pub sample_run_ids_json: Value,
    pub confidence: f64,
    pub effect_size: f64,
    pub distiller_version: String,
    pub reflection_version: String,
    pub source_window: String,
    pub review_status: String,
}

pub fn insert_candidate_experience(
    conn: &Connection,
    input: &CandidateExperienceInput,
) -> Result<i64> {
    let now = now_ms();
    conn.execute(
        r#"
        INSERT INTO candidate_experiences
            (scope, scope_value, experience_type, market_regime_json, finding, recommendation,
             evidence_json, counter_evidence_json, metrics_json, sample_count, sample_run_ids_json,
             confidence, effect_size, distiller_version, reflection_version, source_window,
             review_status,reviewed_at_ms,review_reason,created_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending',NULL,NULL,?)
        "#,
        params![
            input.scope,
            input.scope_value,
            input.experience_type,
            serde_json::to_string(&input.market_regime_json)?,
            input.finding,
            input.recommendation,
            serde_json::to_string(&input.evidence_json)?,
            serde_json::to_string(&input.counter_evidence_json)?,
            serde_json::to_string(&input.metrics_json)?,
            input.sample_count,
            serde_json::to_string(&input.sample_run_ids_json)?,
            input.confidence,
            input.effect_size,
            input.distiller_version,
            input.reflection_version,
            input.source_window,
            now,
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn pending_candidates(conn: &Connection) -> Result<Vec<CandidateExperience>> {
    candidates_by_status(conn, "pending")
}

pub fn candidates_by_status(conn: &Connection, status: &str) -> Result<Vec<CandidateExperience>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT id, scope, scope_value, experience_type, market_regime_json, finding, recommendation,
               evidence_json, counter_evidence_json, metrics_json, sample_count, sample_run_ids_json,
               confidence, effect_size, distiller_version, reflection_version, source_window, review_status
        FROM candidate_experiences
        WHERE review_status = ?
        ORDER BY id ASC
        "#,
    )?;
    let rows = stmt.query_map(params![status], candidate_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn update_candidate_status(
    conn: &Connection,
    id: i64,
    status: &str,
    reason: &str,
) -> Result<()> {
    conn.execute(
        r#"
        UPDATE candidate_experiences
        SET review_status = ?, reviewed_at_ms = ?, review_reason = ?
        WHERE id = ?
        "#,
        params![status, now_ms(), reason, id],
    )?;
    Ok(())
}

fn candidate_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CandidateExperience> {
    Ok(CandidateExperience {
        id: row.get(0)?,
        scope: row.get(1)?,
        scope_value: row.get(2)?,
        experience_type: row.get(3)?,
        market_regime_json: parse_json(row.get::<_, String>(4)?),
        finding: row.get(5)?,
        recommendation: row.get(6)?,
        evidence_json: parse_json(row.get::<_, String>(7)?),
        counter_evidence_json: parse_json(row.get::<_, String>(8)?),
        metrics_json: parse_json(row.get::<_, String>(9)?),
        sample_count: row.get(10)?,
        sample_run_ids_json: parse_json(row.get::<_, String>(11)?),
        confidence: row.get(12)?,
        effect_size: row.get(13)?,
        distiller_version: row.get(14)?,
        reflection_version: row.get(15)?,
        source_window: row.get(16)?,
        review_status: row.get(17)?,
    })
}

fn parse_json(text: String) -> Value {
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect;
    use serde_json::json;

    #[test]
    fn inserts_and_updates_candidate_status() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("candidate.sqlite")).unwrap();
        let id = insert_candidate_experience(
            &conn,
            &CandidateExperienceInput {
                scope: "ticker".to_string(),
                scope_value: "QQQ".to_string(),
                experience_type: "calibration".to_string(),
                market_regime_json: json!({}),
                finding: "finding".to_string(),
                recommendation: "recommendation".to_string(),
                evidence_json: json!([]),
                counter_evidence_json: json!([]),
                metrics_json: json!({"accuracy":0.4}),
                sample_count: 5,
                sample_run_ids_json: json!(["run-1"]),
                confidence: 0.7,
                effect_size: 0.2,
                distiller_version: "v1".to_string(),
                reflection_version: "v1".to_string(),
                source_window: "test".to_string(),
            },
        )
        .unwrap();
        assert_eq!(pending_candidates(&conn).unwrap().len(), 1);
        update_candidate_status(&conn, id, "rejected", "low quality").unwrap();
        assert_eq!(pending_candidates(&conn).unwrap().len(), 0);
        assert_eq!(candidates_by_status(&conn, "rejected").unwrap()[0].id, id);
    }
}
