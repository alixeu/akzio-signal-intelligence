use orchestrator_core::{write_technical_csv, TechnicalCsvRow};
use orchestrator_sql::{
    candidate::{insert_candidate_experience, pending_candidates, CandidateExperienceInput},
    connect, import_jin10_payload, import_technical_csv,
    memory::{promote_candidate_to_memory, PromoteMemoryInput},
    record_attention_batch, turn_history_items, upsert_agent_turn, write_agent_message_scoped,
    AgentMessageInput, AgentTurnInput, AttentionEvent, PhaseSummaryDetailInput, PhaseSummaryInput,
    PhaseSummaryMemoryIndex, PhaseSummaryPhaseBatch,
};
use serde_json::json;
use std::collections::HashMap;

#[test]
fn candidate_promotion_rolls_back_all_four_writes() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("promotion.sqlite")).unwrap();
    insert_candidate_experience(
        &conn,
        &CandidateExperienceInput {
            scope: "ticker".into(),
            scope_value: "QQQ".into(),
            experience_type: "calibration".into(),
            market_regime_json: json!({}),
            finding: "finding".into(),
            recommendation: "recommendation".into(),
            evidence_json: json!([]),
            counter_evidence_json: json!([]),
            metrics_json: json!({}),
            sample_count: 3,
            sample_run_ids_json: json!([]),
            confidence: 0.8,
            effect_size: 0.2,
            distiller_version: "v1".into(),
            reflection_version: "v1".into(),
            source_window: "2026-01-01..2026-01-07".into(),
        },
    )
    .unwrap();
    let candidate = pending_candidates(&conn).unwrap().remove(0);
    conn.execute_batch(
        "CREATE TRIGGER fail_memory_history BEFORE INSERT ON memory_history
         BEGIN SELECT RAISE(ABORT, 'forced history failure'); END;",
    )
    .unwrap();

    let result = promote_candidate_to_memory(
        &conn,
        &PromoteMemoryInput {
            candidate,
            quality_score: 0.8,
            recent_success_rate: 0.7,
        },
    );
    assert!(result.is_err());
    for table in ["memory_items", "memory_versions", "memory_history"] {
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table} was not rolled back");
    }
    let status: String = conn
        .query_row(
            "SELECT review_status FROM candidate_experiences",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "pending");
}

#[test]
fn phase_summary_flush_rolls_back_clear_and_reinsert() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("phase.sqlite")).unwrap();
    let mut first = PhaseSummaryMemoryIndex::new("run-phase");
    let mut first_batch = PhaseSummaryPhaseBatch {
        source_phase: 1,
        ..Default::default()
    };
    first_batch.push_summary(&PhaseSummaryInput {
        run_id: "run-phase".into(),
        source_phase: 1,
        role: "compressor".into(),
        ticker: "QQQ".into(),
        topic_id: None,
        summary: "old summary".into(),
        summary_json: json!({"summary":"old summary"}),
        confidence: 0.7,
    });
    first.merge(first_batch);
    first.flush(&conn).unwrap();

    let mut replacement = PhaseSummaryMemoryIndex::new("run-phase");
    let mut batch = PhaseSummaryPhaseBatch {
        source_phase: 1,
        ..Default::default()
    };
    let summary_id = batch.push_summary(&PhaseSummaryInput {
        run_id: "run-phase".into(),
        source_phase: 1,
        role: "compressor".into(),
        ticker: "QQQ".into(),
        topic_id: None,
        summary: "new summary".into(),
        summary_json: json!({"summary":"new summary"}),
        confidence: 0.8,
    });
    batch.push_detail(&PhaseSummaryDetailInput {
        summary_id,
        run_id: "run-phase".into(),
        source_phase: 1,
        detail: "detail".into(),
        detail_json: json!({"detail":"detail"}),
        source_ref: "test".into(),
        sort_order: 0,
    });
    replacement.merge(batch);
    conn.execute_batch(
        "CREATE TRIGGER fail_phase_detail BEFORE INSERT ON phase_summary_details
         BEGIN SELECT RAISE(ABORT, 'forced detail failure'); END;",
    )
    .unwrap();
    assert!(replacement.flush(&conn).is_err());
    let summary: String = conn
        .query_row("SELECT summary FROM phase_summaries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(summary, "old summary");
}

#[test]
fn attention_batch_rolls_back_when_cache_target_is_missing() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("attention.sqlite")).unwrap();
    let result = record_attention_batch(
        &conn,
        &[
            AttentionEvent {
                run_id: "run-attention".into(),
                turn_id: "turn-1".into(),
                role: "analyst.news_macro".into(),
                subject_kind: "summary".into(),
                subject_id: "summary-1".into(),
                score: 0.5,
                phase: Some(1),
            },
            AttentionEvent {
                run_id: "run-attention".into(),
                turn_id: "turn-1".into(),
                role: "analyst.news_macro".into(),
                subject_kind: "jin10".into(),
                subject_id: "missing".into(),
                score: 0.8,
                phase: Some(1),
            },
        ],
    );
    assert!(result.is_err());
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM attention_ledger", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn technical_replace_rolls_back_delete_and_partial_inserts() {
    let temp = tempfile::tempdir().unwrap();
    let mut conn = connect(temp.path().join("technical.sqlite")).unwrap();
    let first_path = temp.path().join("first.csv");
    write_technical_csv(
        &first_path,
        &[TechnicalCsvRow {
            date: "2026-01-01".into(),
            values: HashMap::from([("Close".into(), 100.0)]),
        }],
    )
    .unwrap();
    import_technical_csv(&mut conn, "QQQ", "1d", &first_path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_last_bar BEFORE INSERT ON technical_bars
         WHEN NEW.bar_time='2026-01-03'
         BEGIN SELECT RAISE(ABORT, 'forced bar failure'); END;",
    )
    .unwrap();
    let replacement_path = temp.path().join("replacement.csv");
    write_technical_csv(
        &replacement_path,
        &[
            TechnicalCsvRow {
                date: "2026-01-02".into(),
                values: HashMap::from([("Close".into(), 101.0)]),
            },
            TechnicalCsvRow {
                date: "2026-01-03".into(),
                values: HashMap::from([("Close".into(), 102.0)]),
            },
        ],
    )
    .unwrap();
    assert!(import_technical_csv(&mut conn, "QQQ", "1d", &replacement_path).is_err());
    let bars: Vec<String> = conn
        .prepare("SELECT bar_time FROM technical_bars ORDER BY bar_time")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(bars, vec!["2026-01-01"]);
}

#[test]
fn jin10_batch_rolls_back_partial_import() {
    let temp = tempfile::tempdir().unwrap();
    let mut conn = connect(temp.path().join("jin10.sqlite")).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_second_jin10 BEFORE INSERT ON jin10_items
         WHEN NEW.content='second'
         BEGIN SELECT RAISE(ABORT, 'forced Jin10 failure'); END;",
    )
    .unwrap();
    let result = import_jin10_payload(
        &mut conn,
        &json!({"items":[
            {"time":"2026-01-01 09:00:00","content":"first"},
            {"time":"2026-01-01 09:01:00","content":"second"}
        ]}),
    );
    assert!(result.is_err());
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM jin10_items", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn agent_context_uses_role_scoped_delta_and_restores_every_turn() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("context.sqlite")).unwrap();
    let first = json!([{"event_type":"user_message","content_text":"one"}]);
    let second = json!([
        {"event_type":"user_message","content_text":"one"},
        {"event_type":"assistant_message","content_text":"two"}
    ]);
    upsert_agent_turn(
        &conn,
        &AgentTurnInput {
            turn_id: "turn-1".into(),
            run_id: "run-context".into(),
            phase: Some(1),
            turn_number: 1,
            role: "analyst.technical".into(),
            full_context_json: first,
            summary: "one".into(),
        },
    )
    .unwrap();
    upsert_agent_turn(
        &conn,
        &AgentTurnInput {
            turn_id: "turn-2".into(),
            run_id: "run-context".into(),
            phase: Some(1),
            turn_number: 2,
            role: "analyst.technical".into(),
            full_context_json: second,
            summary: "two".into(),
        },
    )
    .unwrap();
    upsert_agent_turn(
        &conn,
        &AgentTurnInput {
            turn_id: "turn-news".into(),
            run_id: "run-context".into(),
            phase: Some(1),
            turn_number: 3,
            role: "analyst.news_macro".into(),
            full_context_json: json!([{"event_type":"user_message","content_text":"news"}]),
            summary: "news".into(),
        },
    )
    .unwrap();
    let (checkpoint, delta): (Option<String>, String) = conn
        .query_row(
            "SELECT full_context_json,context_delta_json FROM agent_events WHERE turn_id='turn-2'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(checkpoint.is_none());
    assert_eq!(
        serde_json::from_str::<Vec<serde_json::Value>>(&delta)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(turn_history_items(&conn, "turn-1").unwrap().len(), 1);
    assert_eq!(turn_history_items(&conn, "turn-2").unwrap().len(), 2);
    assert_eq!(
        turn_history_items(&conn, "turn-news").unwrap()[0]["content_text"],
        "news"
    );
}

#[test]
fn identical_per_ticker_payload_is_stored_once_as_aggregate() {
    let temp = tempfile::tempdir().unwrap();
    let mut conn = connect(temp.path().join("artifact.sqlite")).unwrap();
    let written = write_agent_message_scoped(
        &mut conn,
        &AgentMessageInput {
            run_id: "run-artifact".into(),
            phase: 2,
            role: "researcher.bull".into(),
            ticker: "QQQ,VIX".into(),
            tickers: vec!["QQQ".into(), "VIX".into()],
            skill: "researcher.bull".into(),
            kind: "artifact_ticker".into(),
            topic_id: Some("topic-1".into()),
            round: Some(1),
            message_group_id: Some("turn-artifact".into()),
            valid: true,
            content: json!({
                "per_ticker": {
                    "QQQ": {"summary":"same","confidence":0.7},
                    "VIX": {"confidence":0.7,"summary":"same"}
                }
            }),
            last_md: String::new(),
        },
    )
    .unwrap();
    assert_eq!(written, 1);
    let (ticker, hash): (String, String) = conn
        .query_row(
            "SELECT ticker,payload_hash FROM role_turn_summaries",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(ticker, "__ALL__");
    assert_eq!(hash.len(), 64);
}
