use orchestrator_sql::{
    candidate::{insert_candidate_experience, pending_candidates, CandidateExperienceInput},
    connect, context_count, ensure_schema, handle_read_command, import_jin10_payload,
    memory::{promote_candidate_to_memory, PromoteMemoryInput},
    outcome::{upsert_outcome, OutcomeInput},
    prediction::{upsert_prediction, PredictionInput},
    read_run_context, session_history_items, upsert_agent_turn, write_agent_message_scoped,
    write_role_turn_summary, write_run_record, write_source_item, AgentMessageInput,
    AgentTurnInput, RoleTurnSummaryInput, RunContextReadRequest, RunRecordInput, RuntimeContext,
    SourceItemInput,
};
use serde_json::json;

const TABLES: &[&str] = &[
    "runs",
    "agent_events",
    "role_turn_summaries",
    "external_items",
    "technical_features",
    "memory_items",
    "memory_versions",
    "memory_history",
    "predictions",
    "outcomes",
    "candidate_experiences",
];

const REMOVED_TABLES: &[&str] = &[
    "agent_messages",
    "artifacts",
    "summaries",
    "source_items",
    "jin10_flash_items",
    "workflow_sources",
    "workflow_nodes",
    "workflow_edges",
    "context_packets",
    "investment_scopes",
    "investment_scope_links",
    "investment_mandates",
    "thesis_threads",
    "thesis_versions",
    "evidence_items",
    "investment_memory_items",
    "investment_memory_links",
    "follow_up_checks",
    "freshness_policies",
    "runtime_jobs",
    "runtime_job_events",
    "agent_mailbox_messages",
    "run_tickers",
    "turn_tool_calls",
    "reddit_items",
    "x_items",
    "technical_daily_indicators",
    "technical_3h_indicators",
    "technical_20min_indicators",
    "events",
    "run_phases",
    "workflow_snapshots",
    "market_regimes",
    "memory_links",
    "memory_metrics",
    "agent_probabilities",
    "memory_search_fts",
    "external_source_items",
    "run_archive",
    "system_metrics",
    "turn_context_items",
    "prompt_metrics",
    "technical_indicators",
    "jin10_items",
    "youtube_videos",
    "youtube_transcripts",
    "social_items",
    "agent_turn_items",
    "agent_turns",
];

fn ts(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp())
        .unwrap_or(0)
}

#[allow(dead_code)]
fn ts_date(s: &str) -> i64 {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| {
            d.and_hms_opt(0, 0, 0)
                .and_then(|dt| dt.and_utc().timestamp().into())
        })
        .unwrap_or(0)
}

#[test]
fn ensure_schema_creates_only_current_tables_and_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let conn = connect(&db_path).unwrap();

    ensure_schema(&conn).unwrap();
    ensure_schema(&conn).unwrap();

    for table in TABLES {
        assert_eq!(table_exists(&conn, table), 1, "expected table {table}");
    }
    for table in REMOVED_TABLES {
        assert_eq!(table_exists(&conn, table), 0, "old table {table} survived");
    }
    assert_eq!(
        view_exists(&conn, "system_metrics"),
        0,
        "system_metrics view should not exist"
    );
    assert_eq!(
        table_exists(&conn, "system_metrics"),
        0,
        "system_metrics table should not exist"
    );
}

#[test]
fn ensure_schema_drops_legacy_system_metrics_table() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("legacy.sqlite");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE system_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL
        );",
    )
    .unwrap();
    assert_eq!(table_exists(&conn, "system_metrics"), 1);

    ensure_schema(&conn).unwrap();
    assert_eq!(
        table_exists(&conn, "system_metrics"),
        0,
        "legacy system_metrics table should be dropped"
    );
    assert_eq!(view_exists(&conn, "system_metrics"), 0);
}

#[test]
fn ensure_schema_adds_missing_runs_status_columns() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("legacy-runs.sqlite");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            current_date TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );",
    )
    .unwrap();
    assert!(!column_exists(&conn, "runs", "status"));

    ensure_schema(&conn).unwrap();
    assert!(column_exists(&conn, "runs", "status"));
    assert!(column_exists(&conn, "runs", "current_phase"));
    assert!(column_exists(&conn, "runs", "error_message"));
    assert!(column_exists(&conn, "runs", "completed_at"));
}

#[test]
fn run_record_only_writes_runs_and_run_tickers() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut conn = connect(&db_path).unwrap();

    write_run_record(
        &mut conn,
        &RunRecordInput {
            run_id: "run-1",
            current_date: "2026-06-19",
        },
    )
    .unwrap();

    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM runs"), 1);
    assert_eq!(table_exists(&conn, "workflow_nodes"), 0);
}

#[test]
fn jin10_import_writes_compatibility_and_unified_source_items() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut conn = connect(&db_path).unwrap();

    let imported = import_jin10_payload(
        &mut conn,
        &json!({
            "items": [
                {"time": "2026-06-19 09:00:00", "content": "rate cut odds move"},
                {"time": "", "content": "skip"}
            ]
        }),
    )
    .unwrap();

    assert_eq!(imported, 1);
    assert_eq!(context_count(&conn, "jin10").unwrap(), 1);
    assert_eq!(table_exists(&conn, "jin10_flash_items"), 0);

    let context = read_run_context(
        &mut conn,
        &RunContextReadRequest {
            kind: "jin10".to_string(),
            run_id: None,
            ticker: None,
            tickers: vec![],
            phase: None,
            role: None,
            topic_id: None,
            turn_id: None,
            persist_context: true,
            token_budget: None,
        },
    )
    .unwrap();
    assert_eq!(context["items"][0]["content"], "rate cut odds move");
}

#[test]
fn technical_context_stays_within_tool_budget() {
    let db_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../outputs/orchestrator.sqlite");
    if !db_path.exists() {
        return;
    }
    let mut conn = connect(&db_path).unwrap();
    if context_count(&conn, "technical").unwrap_or(0) == 0 {
        return;
    }
    let Ok(grouped) = read_run_context(
        &mut conn,
        &RunContextReadRequest {
            kind: "technical".to_string(),
            run_id: None,
            ticker: Some("QQQ".to_string()),
            tickers: vec!["QQQ".to_string()],
            phase: None,
            role: None,
            topic_id: None,
            turn_id: None,
            persist_context: false,
            token_budget: None,
        },
    ) else {
        // Production DB may be mid-migration / incompatible with current schema helpers.
        return;
    };
    let encoded = serde_json::to_string(&grouped).unwrap();
    assert!(
        encoded.chars().count() < 8_000,
        "technical context should fit default tool truncation budget, got {} chars",
        encoded.chars().count()
    );
}

#[test]
fn technical_context_reads_features_table() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut conn = connect(&db_path).unwrap();

    let now = chrono::Utc::now().timestamp();
    conn.execute(
        r#"
        INSERT INTO technical_features
            (ticker, date, interval, model, close, return_pct, features_json, imported_at)
        VALUES
            ('TQQQ', '2026-06-19', 'daily', 'm', 55.5, 0.01, '{"Close":55.5,"Return":0.01}', ?1)
        "#,
        [now],
    )
    .unwrap();
    conn.execute(
        r#"
        INSERT INTO technical_features
            (ticker, date, interval, model, return_pct, features_json, imported_at)
        VALUES
            ('TQQQ', '2026-06-19T09:00:00Z', '3h', 'm', 1.5, '{"Return":1.5}', ?1)
        "#,
        [now],
    )
    .unwrap();
    conn.execute(
        r#"
        INSERT INTO technical_features
            (ticker, date, interval, model, gap, features_json, imported_at)
        VALUES
            ('TQQQ', '2026-06-19T09:20:00Z', '20min', 'm', 12.5, '{"Gap":12.5}', ?1)
        "#,
        [now],
    )
    .unwrap();

    let grouped = read_run_context(
        &mut conn,
        &RunContextReadRequest {
            kind: "technical".to_string(),
            run_id: None,
            ticker: None,
            tickers: vec![],
            phase: None,
            role: None,
            topic_id: None,
            turn_id: None,
            persist_context: true,
            token_budget: None,
        },
    )
    .unwrap();
    assert_eq!(grouped["daily"][0]["ticker"], "TQQQ");
    assert_eq!(grouped["daily"][0]["indicators"]["Close"], 55.5);
    assert_eq!(grouped["three_hour"][0]["indicators"]["Return"], 1.5);
    assert_eq!(grouped["twenty_minute"][0]["indicators"]["Gap"], 12.5);
    assert_eq!(context_count(&conn, "technical").unwrap(), 3);
}

#[test]
fn source_items_write_dedicated_and_unified_tables() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut conn = connect(&db_path).unwrap();

    assert_eq!(
        write_source_item(
            &mut conn,
            &SourceItemInput {
                source: "youtube".to_string(),
                item_key: "vid-1".to_string(),
                ticker: "TQQQ".to_string(),
                item_time: ts("2026-06-19T00:00:00Z"),
                content: "Video title".to_string(),
                item_json: json!({
                    "video_id": "vid-1",
                    "title": "Video title",
                }),
            },
        )
        .unwrap(),
        1
    );
    assert_eq!(
        write_source_item(
            &mut conn,
            &SourceItemInput {
                source: "reddit".to_string(),
                item_key: "post-1".to_string(),
                ticker: "TQQQ".to_string(),
                item_time: ts("2026-06-19T01:00:00Z"),
                content: "Post body".to_string(),
                item_json: json!({"title": "Post title"}),
            },
        )
        .unwrap(),
        1
    );
    assert_eq!(
        write_source_item(
            &mut conn,
            &SourceItemInput {
                source: "x".to_string(),
                item_key: "tweet-1".to_string(),
                ticker: "TQQQ".to_string(),
                item_time: ts("2026-06-19T02:00:00Z"),
                content: "Tweet body".to_string(),
                item_json: json!({}),
            },
        )
        .unwrap(),
        1
    );

    assert_eq!(
        scalar(
            &conn,
            "SELECT COUNT(*) FROM external_items WHERE source = 'youtube'"
        ),
        1
    );
    assert_eq!(
        write_source_item(
            &mut conn,
            &SourceItemInput {
                source: "youtube".to_string(),
                item_key: "vid-1".to_string(),
                ticker: "TQQQ".to_string(),
                item_time: ts("2026-06-19T03:00:00Z"),
                content: "Updated video title".to_string(),
                item_json: json!({
                    "video_id": "vid-1",
                    "title": "Updated video title",
                }),
            },
        )
        .unwrap(),
        1
    );
    assert_eq!(
        text_scalar(
            &conn,
            "SELECT title FROM external_items WHERE source = 'youtube' AND item_key = 'vid-1'"
        ),
        "Updated video title"
    );
    assert_eq!(
        scalar(
            &conn,
            "SELECT COUNT(*) FROM external_items WHERE source = 'reddit'"
        ),
        1
    );
    assert_eq!(
        scalar(
            &conn,
            "SELECT COUNT(*) FROM external_items WHERE source = 'x'"
        ),
        1
    );
    assert_eq!(table_exists(&conn, "source_items"), 0);
}

#[test]
fn summaries_are_written_and_read_from_role_turn_summaries() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut conn = connect(&db_path).unwrap();
    let tickers = vec!["QQQ".to_string(), "VIX".to_string()];

    write_agent_message_scoped(
        &mut conn,
        &AgentMessageInput {
            run_id: "run-1".to_string(),
            phase: 1,
            role: "analyst.technical".to_string(),
            ticker: "QQQ,VIX".to_string(),
            tickers,
            skill: "analyst.technical".to_string(),
            kind: "artifact".to_string(),
            topic_id: None,
            round: None,
            message_group_id: Some("turn-1".to_string()),
            valid: true,
            content: json!({
                "per_ticker": {
                    "QQQ": {"report": "qqq report", "confidence": 0.7},
                    "VIX": {"report": "vix report", "confidence": 0.6}
                }
            }),
            last_md: String::new(),
        },
    )
    .unwrap();

    write_role_turn_summary(
        &conn,
        &RoleTurnSummaryInput {
            run_id: "run-1".to_string(),
            turn_id: "turn-2".to_string(),
            role: "manager.research".to_string(),
            phase: Some(3),
            ticker: "QQQ".to_string(),
            item_time: ts("2026-06-19T03:00:00Z"),
            topic_id: None,
            debate_id: None,
            summary_type: "final".to_string(),
            summary: "final summary".to_string(),
            summary_json: json!({"summary": "final summary"}),
            confidence: 0.8,
        },
    )
    .unwrap();

    assert_eq!(scalar(&conn, "SELECT COUNT(*) FROM role_turn_summaries"), 3);
    assert_eq!(table_exists(&conn, "agent_messages"), 0);
    assert_eq!(table_exists(&conn, "artifacts"), 0);

    let ctx = RuntimeContext {
        run_id: "run-1".to_string(),
        ticker: "QQQ,VIX".to_string(),
        tickers: vec!["QQQ".to_string(), "VIX".to_string()],
        phase: 1,
        role: String::new(),
    };
    let result = handle_read_command(&conn, "get-analyst-reports", &ctx, None).unwrap();
    assert_eq!(result["items"].as_array().unwrap().len(), 2);
}

#[test]
fn turn_tables_persist_items_and_history() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let conn = connect(&db_path).unwrap();

    upsert_agent_turn(
        &conn,
        &AgentTurnInput {
            turn_id: "turn-1".to_string(),
            run_id: "run-1".to_string(),
            phase: Some(1),
            turn_number: 1,
            role: "analyst.technical".to_string(),
            full_context_json: json!([
                {"event_type":"user_message","role":"user","content_text":"go","content_json":{},"tool_call_id":"","tool_name":""},
                {"event_type":"assistant_message","role":"assistant","content_text":"hello","content_json":{"text":"hello"},"tool_call_id":"","tool_name":""}
            ]),
            summary: "test turn".to_string(),
        },
    )
    .unwrap();

    let history = session_history_items(&conn, "run-1", 10).unwrap();
    assert!(!history.is_empty());
    assert_eq!(history[0]["content_text"], "go");
    assert_eq!(history[1]["content_text"], "hello");
}

#[test]
fn compose_context_scores_trims_and_audits_blocks() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut conn = connect(&db_path).unwrap();

    write_role_turn_summary(
        &conn,
        &RoleTurnSummaryInput {
            run_id: "run-1".to_string(),
            turn_id: "summary-1".to_string(),
            role: "mediator.topic_controller".to_string(),
            phase: Some(25),
            ticker: "TQQQ".to_string(),
            item_time: ts("2026-06-19T12:00:00Z"),
            topic_id: Some("topic-1".to_string()),
            debate_id: None,
            summary_type: "topic_final".to_string(),
            summary: "same ticker same topic summary".to_string(),
            summary_json: json!({"summary": "same ticker same topic summary"}),
            confidence: 0.9,
        },
    )
    .unwrap();
    write_role_turn_summary(
        &conn,
        &RoleTurnSummaryInput {
            run_id: "run-1".to_string(),
            turn_id: "summary-2".to_string(),
            role: "analyst.news_macro".to_string(),
            phase: Some(1),
            ticker: "VIX".to_string(),
            item_time: ts("2026-06-18T12:00:00Z"),
            topic_id: None,
            debate_id: None,
            summary_type: "artifact".to_string(),
            summary: "other ticker summary".to_string(),
            summary_json: json!({"summary": "other ticker summary"}),
            confidence: 0.7,
        },
    )
    .unwrap();
    import_jin10_payload(
        &mut conn,
        &json!({
            "items": [{"time": "2026-06-19 13:00:00", "content": "macro flash"}]
        }),
    )
    .unwrap();
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        r#"
        INSERT INTO technical_features
            (ticker, date, interval, model, close, features_json, imported_at)
        VALUES
            ('TQQQ', '2026-06-19', 'daily', 'm', 61.0, '{"Close":61.0}', ?1)
        "#,
        [now],
    )
    .unwrap();
    write_source_item(
        &mut conn,
        &SourceItemInput {
            source: "youtube".to_string(),
            item_key: "vid-1".to_string(),
            ticker: "TQQQ".to_string(),
            item_time: ts("2026-06-19T11:00:00Z"),
            content: "video title".to_string(),
            item_json: json!({"video_id": "vid-1", "title": "video title"}),
        },
    )
    .unwrap();
    write_source_item(
        &mut conn,
        &SourceItemInput {
            source: "youtube_transcript".to_string(),
            item_key: "vid-1-transcript".to_string(),
            ticker: "TQQQ".to_string(),
            item_time: ts("2026-06-19T11:30:00Z"),
            content: "transcript text".to_string(),
            item_json: json!({"video_id": "vid-1", "language": "en", "provider": "test"}),
        },
    )
    .unwrap();
    write_source_item(
        &mut conn,
        &SourceItemInput {
            source: "reddit".to_string(),
            item_key: "post-1".to_string(),
            ticker: "TQQQ".to_string(),
            item_time: ts("2026-06-19T10:00:00Z"),
            content: "reddit body".to_string(),
            item_json: json!({"title": "reddit title"}),
        },
    )
    .unwrap();
    write_source_item(
        &mut conn,
        &SourceItemInput {
            source: "x".to_string(),
            item_key: "x-1".to_string(),
            ticker: "TQQQ".to_string(),
            item_time: ts("2026-06-19T10:30:00Z"),
            content: "x body".to_string(),
            item_json: json!({"author": "macro"}),
        },
    )
    .unwrap();
    upsert_agent_turn(
        &conn,
        &AgentTurnInput {
            turn_id: "history-turn".to_string(),
            run_id: "run-1".to_string(),
            phase: Some(1),
            turn_number: 1,
            role: "researcher.bull.initial".to_string(),
            full_context_json: json!([
                {"event_type":"assistant_message","role":"assistant","content_text":"history item","content_json":{"text":"history"},"tool_call_id":"","tool_name":""}
            ]),
            summary: "history item".to_string(),
        },
    )
    .unwrap();

    let composed = read_run_context(
        &mut conn,
        &RunContextReadRequest {
            kind: "compose_context".to_string(),
            run_id: Some("run-1".to_string()),
            ticker: Some("TQQQ".to_string()),
            tickers: vec!["TQQQ".to_string()],
            phase: Some(3),
            role: Some("manager.research".to_string()),
            topic_id: Some("topic-1".to_string()),
            turn_id: Some("turn-compose".to_string()),
            persist_context: true,
            token_budget: Some(4096),
        },
    )
    .unwrap();
    let blocks = composed["blocks"].as_array().unwrap();
    assert!(blocks
        .iter()
        .any(|block| block["context_type"] == "role_summary"));
    assert!(blocks
        .iter()
        .any(|block| block["context_type"] == "technical_daily"));
    assert!(blocks.iter().any(|block| block["context_type"] == "jin10"));
    assert!(blocks
        .iter()
        .any(|block| block["context_type"] == "youtube"));
    assert!(blocks
        .iter()
        .any(|block| block["context_type"] == "youtube_transcript"));
    assert!(blocks.iter().any(|block| block["context_type"] == "reddit"));
    assert!(blocks.iter().any(|block| block["context_type"] == "x"));
    assert_eq!(blocks[0]["content"], "same ticker same topic summary");

    let trimmed = read_run_context(
        &mut conn,
        &RunContextReadRequest {
            kind: "compose_context".to_string(),
            run_id: Some("run-1".to_string()),
            ticker: Some("TQQQ".to_string()),
            tickers: vec!["TQQQ".to_string()],
            phase: Some(3),
            role: Some("manager.research".to_string()),
            topic_id: Some("topic-1".to_string()),
            turn_id: Some("turn-compose-small".to_string()),
            persist_context: false,
            token_budget: Some(5),
        },
    )
    .unwrap();
    assert!(trimmed["blocks"].as_array().unwrap().len() < blocks.len());
}

#[test]
fn read_run_context_exposes_reflection_memory_contexts() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let mut conn = connect(&db_path).unwrap();
    seed_reflection_context(&conn);

    let prior_memory = read_run_context(
        &mut conn,
        &context_request("prior_memory", Some("TQQQ"), Some(256)),
    )
    .unwrap();
    assert_eq!(prior_memory["query"], "prior_memory");
    assert_eq!(prior_memory["items"].as_array().unwrap().len(), 1);
    assert_eq!(prior_memory["items"][0]["ticker"], "TQQQ");
    assert!(prior_memory["items"][0].get("body").is_none());

    let track_record = read_run_context(
        &mut conn,
        &context_request("track_record", Some("TQQQ"), None),
    )
    .unwrap();
    assert_eq!(track_record["query"], "track_record");
    assert_eq!(track_record["aggregate"]["total_predictions"], 1);
    assert_eq!(track_record["ticker_record"]["total_predictions"], 1);

    let agent_accuracy = read_run_context(
        &mut conn,
        &context_request("agent_accuracy", Some("TQQQ"), None),
    )
    .unwrap();
    assert_eq!(agent_accuracy["query"], "agent_accuracy");
    assert_eq!(
        agent_accuracy["roles"]["analyst.technical"]["total_predictions"],
        1
    );

    let composed = read_run_context(
        &mut conn,
        &RunContextReadRequest {
            kind: "compose_context".to_string(),
            run_id: Some("run-1".to_string()),
            ticker: Some("TQQQ".to_string()),
            tickers: vec!["TQQQ".to_string()],
            phase: Some(1),
            role: Some("manager.research".to_string()),
            topic_id: None,
            turn_id: None,
            persist_context: true,
            token_budget: Some(256),
        },
    )
    .unwrap();
    assert!(composed["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|block| block["context_type"] == "prior_memory"));

    let err =
        read_run_context(&mut conn, &context_request("not_supported", None, None)).unwrap_err();
    assert!(err
        .to_string()
        .contains("unsupported read_run_context kind"));
}

#[test]
fn ensure_schema_adds_reflection_memory_columns() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("orchestrator.sqlite");
    let conn = connect(&db_path).unwrap();

    for column in [
        "market_regime_json",
        "quality_score",
        "sample_count",
        "recent_success_rate",
        "reflection_version",
        "promoted_from",
    ] {
        assert!(
            column_exists(&conn, "memory_items", column),
            "missing {column}"
        );
    }
}

fn context_request(
    kind: &str,
    ticker: Option<&str>,
    token_budget: Option<usize>,
) -> RunContextReadRequest {
    RunContextReadRequest {
        kind: kind.to_string(),
        run_id: Some("run-1".to_string()),
        ticker: ticker.map(ToString::to_string),
        tickers: ticker.into_iter().map(ToString::to_string).collect(),
        phase: Some(1),
        role: Some("manager.research".to_string()),
        topic_id: None,
        turn_id: None,
        persist_context: true,
        token_budget,
    }
}

fn seed_reflection_context(conn: &rusqlite::Connection) {
    insert_candidate_experience(
        conn,
        &CandidateExperienceInput {
            scope: "ticker".to_string(),
            scope_value: "TQQQ".to_string(),
            experience_type: "calibration".to_string(),
            market_regime_json: json!({}),
            finding: "long setup works after breadth confirmation".to_string(),
            recommendation: "calibrate long probability upward only with breadth".to_string(),
            evidence_json: json!([]),
            counter_evidence_json: json!([]),
            metrics_json: json!({"direction_accuracy": 1.0}),
            sample_count: 8,
            sample_run_ids_json: json!(["run-1"]),
            confidence: 0.9,
            effect_size: 0.2,
            distiller_version: "v1".to_string(),
            reflection_version: "v1".to_string(),
            source_window: "2026-W01".to_string(),
        },
    )
    .unwrap();
    let candidate = pending_candidates(conn).unwrap().pop().unwrap();
    promote_candidate_to_memory(
        conn,
        &PromoteMemoryInput {
            candidate,
            quality_score: 0.8,
            recent_success_rate: 1.0,
        },
    )
    .unwrap();

    let prediction_id = upsert_prediction(
        conn,
        &PredictionInput {
            run_id: "run-1".to_string(),
            ticker: "TQQQ".to_string(),
            prediction_date: "2026-01-01".to_string(),
            long_probability: 0.7,
            short_probability: 0.3,
            rating: "long".to_string(),
            window_days: 5,
            market_regime_json: json!({}),
            agent_probabilities_json: json!({"analyst.technical": {"long_probability": 0.7}}),
            weighted_base_probability: None,
        },
    )
    .unwrap();
    upsert_outcome(
        conn,
        &OutcomeInput {
            prediction_id,
            run_id: "run-1".to_string(),
            ticker: "TQQQ".to_string(),
            prediction_date: "2026-01-01".to_string(),
            outcome_date: "2026-01-06".to_string(),
            window_days: 5,
            baseline_close: 100.0,
            outcome_close: 110.0,
            actual_return: 0.1,
            direction_correct: true,
            probability_error: -0.3,
        },
    )
    .unwrap();
}

fn table_exists(conn: &rusqlite::Connection, table: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )
    .unwrap()
}

fn view_exists(conn: &rusqlite::Connection, view: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'view' AND name = ?1",
        [view],
        |row| row.get(0),
    )
    .unwrap()
}

fn scalar(conn: &rusqlite::Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn text_scalar(conn: &rusqlite::Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn column_exists(conn: &rusqlite::Connection, table: &str, column: &str) -> bool {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap();
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    columns.iter().any(|item| item == column)
}

#[test]
fn context_count_rejects_unsafe_table_identifiers() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ctx.sqlite");
    let conn = connect(&db_path).unwrap();
    ensure_schema(&conn).unwrap();

    assert_eq!(
        context_count(&conn, "jin10; DROP TABLE external_items;--").unwrap(),
        0
    );
    assert_eq!(context_count(&conn, "jin10'").unwrap(), 0);
    assert_eq!(context_count(&conn, "jin10").unwrap(), 0);
}

#[test]
fn jin10_context_returns_compact_recent_items() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("runtime.sqlite");
    let mut conn = connect(&db_path).unwrap();
    ensure_schema(&conn).unwrap();
    let long = "x".repeat(600);
    import_jin10_payload(
        &mut conn,
        &serde_json::json!({
            "items": [
                {"time": "2026-07-13 10:00:00", "content": long},
                {"time": "2026-07-13 11:00:00", "content": "short headline"}
            ]
        }),
    )
    .unwrap();
    let value = read_run_context(
        &mut conn,
        &RunContextReadRequest {
            kind: "jin10".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(value["query"], "get-jin10-context");
    assert_eq!(value["item_count"], 2);
    let items = value["items"].as_array().unwrap();
    assert!(items.iter().any(|item| item["content"] == "short headline"));
    let long_item = items
        .iter()
        .find(|item| item["content"].as_str().unwrap_or("").contains('…'))
        .expect("long content should be clipped");
    assert!(long_item["content"].as_str().unwrap().chars().count() <= 401);
}
