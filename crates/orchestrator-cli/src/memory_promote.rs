use anyhow::Result;
use orchestrator_core::{DefaultQualityScorer, MemoryQualityInput, QualityScorer};
use orchestrator_sql::{
    candidate::{pending_candidates, update_candidate_status},
    memory::{degrade_stale_memories, promote_candidate_to_memory, PromoteMemoryInput},
};
use rusqlite::Connection;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromoteMode {
    Auto,
    Review,
}

impl PromoteMode {
    pub fn parse(value: &str) -> Self {
        if value == "review" {
            Self::Review
        } else {
            Self::Auto
        }
    }
}

#[derive(Debug, Clone)]
pub struct PromoteOptions {
    pub mode: PromoteMode,
    pub min_quality: f64,
    pub min_samples: usize,
    pub min_confidence: f64,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq, Eq)]
pub struct PromoteSummary {
    pub promoted: usize,
    pub pending_human: usize,
    pub rejected: usize,
    pub degraded: usize,
}

pub fn promote_memories(conn: &Connection, options: &PromoteOptions) -> Result<PromoteSummary> {
    let scorer = DefaultQualityScorer;
    let candidates = pending_candidates(conn)?;
    let mut summary = PromoteSummary::default();

    for candidate in candidates {
        let recent_success_rate = candidate
            .metrics_json
            .get("direction_accuracy")
            .and_then(|value| value.as_f64())
            .unwrap_or(candidate.confidence)
            .clamp(0.0, 1.0);
        let quality_score = scorer.score(&MemoryQualityInput {
            confidence: candidate.confidence,
            sample_count: candidate.sample_count.max(0) as usize,
            recent_success_rate,
            days_since_observed: 0.0,
        });

        if candidate.sample_count < options.min_samples as i64 {
            // Keep a one-off or repeated warning pending so later distinct runs
            // can accumulate into the same Rust-derived pattern key.
            continue;
        }
        if candidate.confidence < options.min_confidence {
            update_candidate_status(conn, candidate.id, "rejected", "confidence below threshold")?;
            summary.rejected += 1;
            continue;
        }
        if quality_score < options.min_quality {
            update_candidate_status(
                conn,
                candidate.id,
                "rejected",
                "quality_score below threshold",
            )?;
            summary.rejected += 1;
            continue;
        }

        if options.mode == PromoteMode::Review {
            update_candidate_status(conn, candidate.id, "pending_human", "passed auto gate")?;
            summary.pending_human += 1;
            continue;
        }

        let memory_type = candidate.experience_type.clone();
        let scope = candidate.scope.clone();
        let scope_value = candidate.scope_value.clone();
        let candidate_id = candidate.id;
        promote_candidate_to_memory(
            conn,
            &PromoteMemoryInput {
                candidate,
                quality_score,
                recent_success_rate,
            },
        )?;
        update_candidate_status(conn, candidate_id, "promoted", "promoted to memory")?;
        summary.promoted += 1;
        summary.degraded += degrade_stale_memories(
            conn,
            &scope,
            &scope_value,
            &memory_type,
            options.min_quality,
            Some(candidate_id),
        )?;
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_sql::{
        candidate::{insert_candidate_experience, CandidateExperienceInput},
        connect,
        memory::{promote_candidate_to_memory, PromoteMemoryInput},
    };
    use serde_json::json;

    #[test]
    fn keeps_candidates_pending_until_sample_gate() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("promote-reject.sqlite")).unwrap();
        insert_candidate(&conn, 2, 0.9, 0.9);

        let summary = promote_memories(&conn, &options(PromoteMode::Auto)).unwrap();
        assert_eq!(summary.rejected, 0);
        assert_eq!(status_count(&conn, "pending"), 1);
    }

    #[test]
    fn review_mode_marks_candidate_pending_human() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("promote-review.sqlite")).unwrap();
        insert_candidate(&conn, 10, 0.9, 0.9);

        let summary = promote_memories(&conn, &options(PromoteMode::Review)).unwrap();
        assert_eq!(summary.pending_human, 1);
        assert_eq!(status_count(&conn, "pending_human"), 1);
        assert_eq!(memory_count(&conn, "active"), 0);
    }

    #[test]
    fn auto_mode_promotes_candidate_and_refreshes_fts() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("promote-auto.sqlite")).unwrap();
        insert_candidate(&conn, 10, 0.95, 0.95);

        let summary = promote_memories(&conn, &options(PromoteMode::Auto)).unwrap();
        assert_eq!(summary.promoted, 1);
        assert_eq!(status_count(&conn, "promoted"), 1);
        assert_eq!(memory_count(&conn, "active"), 1);
        let active_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM memory_items WHERE status='active'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active_count, 1);
    }

    #[test]
    fn auto_mode_degrades_low_quality_old_memory() {
        let temp = tempfile::tempdir().unwrap();
        let conn = connect(temp.path().join("promote-degrade.sqlite")).unwrap();
        insert_candidate(&conn, 10, 0.95, 0.8);
        let old = orchestrator_sql::candidate::pending_candidates(&conn)
            .unwrap()
            .remove(0);
        let old_id = old.id;
        promote_candidate_to_memory(
            &conn,
            &PromoteMemoryInput {
                candidate: old,
                quality_score: 0.2,
                recent_success_rate: 0.2,
            },
        )
        .unwrap();
        orchestrator_sql::candidate::update_candidate_status(
            &conn,
            old_id,
            "promoted",
            "seed old memory",
        )
        .unwrap();
        insert_candidate(&conn, 10, 0.95, 0.95);

        let summary = promote_memories(&conn, &options(PromoteMode::Auto)).unwrap();
        assert_eq!(summary.promoted, 1);
        assert_eq!(summary.degraded, 1);
        assert_eq!(memory_count(&conn, "inactive"), 1);
        assert_eq!(memory_count(&conn, "active"), 1);
    }

    fn options(mode: PromoteMode) -> PromoteOptions {
        PromoteOptions {
            mode,
            min_quality: 0.6,
            min_samples: 5,
            min_confidence: 0.6,
        }
    }

    fn insert_candidate(conn: &Connection, sample_count: i64, confidence: f64, accuracy: f64) {
        insert_candidate_experience(
            conn,
            &CandidateExperienceInput {
                scope: "ticker".to_string(),
                scope_value: "QQQ".to_string(),
                experience_type: "calibration_strength".to_string(),
                market_regime_json: json!({"volatility":"normal"}),
                finding: "pattern".to_string(),
                recommendation: "use as prior".to_string(),
                evidence_json: json!([]),
                counter_evidence_json: json!([]),
                metrics_json: json!({"direction_accuracy": accuracy}),
                sample_count,
                sample_run_ids_json: json!(["run-1"]),
                confidence,
                effect_size: (accuracy - 0.5_f64).abs(),
                distiller_version: "v1".to_string(),
                reflection_version: "v1".to_string(),
                source_window: "2026-01-01..2026-01-07".to_string(),
            },
        )
        .unwrap();
    }

    fn status_count(conn: &Connection, status: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM candidate_experiences WHERE review_status = ?",
            [status],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn memory_count(conn: &Connection, status: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM memory_items WHERE status = ?",
            [status],
            |row| row.get(0),
        )
        .unwrap()
    }
}
