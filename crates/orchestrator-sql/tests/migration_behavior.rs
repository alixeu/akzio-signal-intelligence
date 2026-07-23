use orchestrator_sql::{
    connect, core_query_plans, ensure_schema, load_technical_series, technical_row_count,
    turn_history_items, write_run_record, RunRecordInput,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

const RUN_ID: &str = "legacy-run-1";

struct ArchivedRun {
    current_date: String,
    created_at_ms: i64,
    status: String,
    current_phase: Option<i64>,
    error_message: Option<String>,
    completed_at_ms: Option<i64>,
    run_dir: Option<String>,
    db_path: Option<String>,
    git_sha: Option<String>,
    config_hash: Option<String>,
    artifact_path: Option<String>,
    workflow_version: Option<String>,
    prompt_versions_json: String,
    degraded: i64,
    phase_count: i64,
    total_elapsed_ms: i64,
}

fn count(conn: &Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |row| {
        row.get(0)
    })
    .unwrap()
}

fn user_version(conn: &Connection) -> i64 {
    conn.query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap()
}

fn table_exists(conn: &Connection, table: &str) -> bool {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |row| row.get::<_, i64>(0),
    )
    .unwrap()
        == 1
}

/// Build the schema shipped immediately before `PRAGMA user_version` migrations.
/// The fixture intentionally uses second-resolution timestamps and JSON snapshots,
/// exercising the compatibility conversions rather than the latest write APIs.
fn create_legacy_database(conn: &Connection, agent_context_json: &str) {
    conn.execute_batch(
        r#"
        CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            current_date TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            current_phase INTEGER,
            error_message TEXT NOT NULL DEFAULT '',
            completed_at INTEGER,
            run_dir TEXT NOT NULL DEFAULT '',
            db_path TEXT NOT NULL DEFAULT '',
            git_sha TEXT NOT NULL DEFAULT '',
            config_hash TEXT NOT NULL DEFAULT '',
            artifact_path TEXT NOT NULL DEFAULT '',
            workflow_version TEXT NOT NULL DEFAULT '',
            prompt_versions_json TEXT NOT NULL DEFAULT '{}',
            degraded INTEGER NOT NULL DEFAULT 0,
            phase_count INTEGER NOT NULL DEFAULT 0,
            total_elapsed_ms INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE agent_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            turn_id TEXT NOT NULL UNIQUE,
            run_id TEXT NOT NULL DEFAULT '',
            phase INTEGER,
            turn_number INTEGER NOT NULL DEFAULT 0,
            role TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            full_context_json TEXT NOT NULL DEFAULT '[]',
            summary TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL DEFAULT '',
            input_tokens INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            cached_tokens INTEGER NOT NULL DEFAULT 0,
            reasoning_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens INTEGER NOT NULL DEFAULT 0,
            non_cached_input_tokens INTEGER NOT NULL DEFAULT 0,
            visible_output_tokens INTEGER NOT NULL DEFAULT 0,
            cost_usd REAL NOT NULL DEFAULT 0.0,
            context_warning INTEGER NOT NULL DEFAULT 0,
            elapsed_ms INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE role_turn_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL DEFAULT '',
            turn_id TEXT NOT NULL DEFAULT '',
            phase INTEGER,
            role TEXT NOT NULL DEFAULT '',
            ticker TEXT NOT NULL DEFAULT '',
            item_time INTEGER NOT NULL DEFAULT 0,
            topic_id TEXT,
            debate_id TEXT,
            summary_type TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL,
            summary_json TEXT NOT NULL DEFAULT '{}',
            confidence REAL NOT NULL DEFAULT 0.0,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE candidate_experiences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope TEXT NOT NULL,
            scope_value TEXT NOT NULL,
            experience_type TEXT NOT NULL,
            market_regime_json TEXT NOT NULL DEFAULT '{}',
            finding TEXT NOT NULL,
            recommendation TEXT NOT NULL,
            evidence_json TEXT NOT NULL DEFAULT '[]',
            counter_evidence_json TEXT NOT NULL DEFAULT '[]',
            metrics_json TEXT NOT NULL DEFAULT '{}',
            sample_count INTEGER NOT NULL,
            sample_run_ids_json TEXT NOT NULL DEFAULT '[]',
            confidence REAL NOT NULL,
            effect_size REAL NOT NULL DEFAULT 0.0,
            distiller_version TEXT NOT NULL DEFAULT 'v1',
            reflection_version TEXT NOT NULL DEFAULT 'v1',
            source_window TEXT NOT NULL DEFAULT '',
            review_status TEXT NOT NULL DEFAULT 'pending',
            reviewed_at INTEGER,
            review_reason TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
        );
        CREATE TABLE memory_items (
            memory_id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL DEFAULT '',
            scope TEXT NOT NULL DEFAULT '',
            memory_type TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active',
            current_version_id TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.0,
            expires_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            market_regime_json TEXT NOT NULL DEFAULT '{}',
            quality_score REAL NOT NULL DEFAULT 0.0,
            sample_count INTEGER NOT NULL DEFAULT 0,
            recent_success_rate REAL NOT NULL DEFAULT 0.0,
            reflection_version TEXT NOT NULL DEFAULT 'v1',
            promoted_from INTEGER
        );
        CREATE TABLE memory_versions (
            version_id TEXT PRIMARY KEY,
            memory_id TEXT NOT NULL,
            version_index INTEGER NOT NULL,
            summary TEXT NOT NULL,
            body_json TEXT NOT NULL DEFAULT '{}',
            evidence_refs_json TEXT NOT NULL DEFAULT '[]',
            source_run_id TEXT NOT NULL DEFAULT '',
            source_role TEXT NOT NULL DEFAULT '',
            source_date TEXT NOT NULL DEFAULT '',
            observed_at INTEGER NOT NULL DEFAULT 0,
            content_hash TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(memory_id, version_index),
            FOREIGN KEY(memory_id) REFERENCES memory_items(memory_id)
        );
        CREATE TABLE memory_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            action TEXT NOT NULL,
            version_id TEXT NOT NULL DEFAULT '',
            old_status TEXT NOT NULL DEFAULT '',
            new_status TEXT NOT NULL DEFAULT '',
            quality_score REAL,
            reason TEXT NOT NULL DEFAULT '',
            source_run_id TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            FOREIGN KEY(memory_id) REFERENCES memory_items(memory_id)
        );
        CREATE TABLE jin10_items (
            id TEXT PRIMARY KEY,
            content_json TEXT NOT NULL,
            attention_score REAL NOT NULL DEFAULT 0.0,
            item_time INTEGER NOT NULL DEFAULT 0,
            imported_at INTEGER NOT NULL
        );
        CREATE TABLE technical_series (
            ticker TEXT NOT NULL,
            interval TEXT NOT NULL,
            as_of TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            rows_json TEXT NOT NULL,
            imported_at INTEGER NOT NULL,
            PRIMARY KEY (ticker, interval),
            CHECK (row_count > 0)
        );
        CREATE TABLE phase_summaries (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            source_phase INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'compressor',
            ticker TEXT NOT NULL DEFAULT '',
            topic_id TEXT,
            summary TEXT NOT NULL DEFAULT '',
            summary_json TEXT NOT NULL DEFAULT '{}',
            confidence REAL NOT NULL DEFAULT 0.0,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE phase_summary_details (
            id TEXT PRIMARY KEY,
            summary_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            source_phase INTEGER NOT NULL,
            detail TEXT NOT NULL DEFAULT '',
            detail_json TEXT NOT NULL DEFAULT '{}',
            source_ref TEXT NOT NULL DEFAULT '',
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(summary_id) REFERENCES phase_summaries(id)
        );
        CREATE TABLE attention_ledger (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            turn_id TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT '',
            subject_kind TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            score REAL NOT NULL DEFAULT 0.0,
            phase INTEGER,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            prediction_date TEXT NOT NULL,
            long_probability REAL NOT NULL,
            short_probability REAL NOT NULL,
            rating TEXT NOT NULL DEFAULT '',
            window_days INTEGER NOT NULL DEFAULT 5,
            market_regime_json TEXT NOT NULL DEFAULT '{}',
            agent_probabilities_json TEXT NOT NULL DEFAULT '{}',
            weighted_base_probability REAL,
            created_at INTEGER NOT NULL,
            UNIQUE(run_id, ticker)
        );
        CREATE TABLE outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER NOT NULL REFERENCES predictions(id),
            run_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            prediction_date TEXT NOT NULL,
            outcome_date TEXT NOT NULL,
            window_days INTEGER NOT NULL,
            baseline_close REAL NOT NULL,
            outcome_close REAL NOT NULL,
            actual_return REAL NOT NULL,
            direction_correct INTEGER NOT NULL,
            probability_error REAL NOT NULL,
            scored_at INTEGER NOT NULL,
            UNIQUE(prediction_id)
        );
        "#,
    )
    .unwrap();

    conn.execute(
        r#"INSERT INTO runs
           (run_id,current_date,created_at,status,current_phase,error_message,completed_at,
            run_dir,db_path,git_sha,config_hash,artifact_path,workflow_version,
            prompt_versions_json,degraded,phase_count,total_elapsed_ms)
           VALUES (?1,'2026-07-01',1700000000,'completed',7,'',1700003600,
                   '/tmp/run','/tmp/db','deadbeef','cfg-hash','report.json','workflow-v1',
                   '{"research":"prompt-v1"}',1,8,3600000)"#,
        [RUN_ID],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO agent_events
           (turn_id,run_id,phase,turn_number,role,created_at,full_context_json,summary,
            model,input_tokens,output_tokens,cached_tokens,reasoning_tokens,total_tokens,
            non_cached_input_tokens,visible_output_tokens,cost_usd,context_warning,elapsed_ms)
           VALUES ('turn-1',?1,1,1,'analyst.technical',1700000100,?2,'first turn',
                   'gpt-test',10,5,2,1,15,8,4,0.02,1,1234)"#,
        params![RUN_ID, agent_context_json],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO role_turn_summaries
           (run_id,turn_id,phase,role,ticker,item_time,topic_id,debate_id,summary_type,
            summary,summary_json,confidence,created_at)
           VALUES (?1,'turn-1',1,'analyst.technical','QQQ',1700000100,'rates','debate-1',
                   'analysis','trend summary','{"signal":"bullish"}',0.75,1700000101)"#,
        [RUN_ID],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO candidate_experiences
           (id,scope,scope_value,experience_type,market_regime_json,finding,recommendation,
            evidence_json,counter_evidence_json,metrics_json,sample_count,sample_run_ids_json,
            confidence,effect_size,distiller_version,reflection_version,source_window,
            review_status,reviewed_at,review_reason,created_at)
           VALUES (1,'ticker','QQQ','momentum','{"regime":"risk_on"}','momentum persisted',
                   'retain signal','["turn-1"]','[]','{"hit_rate":0.7}',4,
                   '["legacy-run-1"]',0.8,0.2,'distill-v1','reflect-v1','30d',
                   'promoted',1700000400,'validated',1700000200)"#,
        [],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO memory_items
           (memory_id,ticker,scope,memory_type,status,current_version_id,confidence,expires_at,
            created_at,updated_at,market_regime_json,quality_score,sample_count,
            recent_success_rate,reflection_version,promoted_from)
           VALUES ('memory-1','QQQ','ticker','momentum','active','memory-v1',0.8,NULL,
                   1700000200,1700000300,'{"regime":"risk_on"}',0.9,4,0.75,'reflect-v1',1)"#,
        [],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO memory_versions
           (version_id,memory_id,version_index,summary,body_json,evidence_refs_json,
            source_run_id,source_role,source_date,observed_at,content_hash,created_at)
           VALUES ('memory-v1','memory-1',1,'stable momentum','{"rule":"follow trend"}',
                   '["turn-1"]',?1,'analyst.technical','2026-07-01',1700000200,
                   'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',1700000200)"#,
        [RUN_ID],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO memory_history
           (memory_id,action,version_id,old_status,new_status,quality_score,reason,
            source_run_id,created_at)
           VALUES ('memory-1','promoted','memory-v1','','active',0.9,'validated',?1,1700000200)"#,
        [RUN_ID],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO jin10_items(id,content_json,attention_score,item_time,imported_at)
           VALUES ('jin10-1','{"id":"jin10-1","time":1700000000,
                    "time_raw":"2026-07-01 09:00:00","content":"Fed commentary",
                    "source":"jin10"}',0.8,1700000000,1700000050)"#,
        [],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO technical_series(ticker,interval,as_of,row_count,rows_json,imported_at)
           VALUES ('QQQ','daily','2026-07-02',2,
                   '[{"date":"2026-07-01","values":{"Close":100.0,"RSI":45.0}},
                     {"date":"2026-07-02","values":{"Close":102.5,"RSI":51.0}}]',
                   1700000500)"#,
        [],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO phase_summaries
           (id,run_id,source_phase,role,ticker,topic_id,summary,summary_json,confidence,created_at)
           VALUES ('phase-summary-1',?1,1,'compressor','QQQ','rates','compressed',
                   '{"summary":"compressed"}',0.7,1700000600)"#,
        [RUN_ID],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO phase_summary_details
           (id,summary_id,run_id,source_phase,detail,detail_json,source_ref,sort_order,created_at)
           VALUES ('phase-detail-1','phase-summary-1',?1,1,'detail',
                   '{"detail":"source evidence"}','turn-1',0,1700000601)"#,
        [RUN_ID],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO attention_ledger
           (id,run_id,turn_id,role,subject_kind,subject_id,score,phase,created_at)
           VALUES ('attention-1',?1,'turn-1','analyst.technical','jin10','jin10-1',0.8,1,1700000700)"#,
        [RUN_ID],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO predictions
           (id,run_id,ticker,prediction_date,long_probability,short_probability,rating,
            window_days,market_regime_json,agent_probabilities_json,
            weighted_base_probability,created_at)
           VALUES (1,?1,'QQQ','2026-07-01',0.65,0.35,'Buy',5,
                   '{"regime":"risk_on"}','{"technical":0.7}',0.66,1700000800)"#,
        [RUN_ID],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO outcomes
           (id,prediction_id,run_id,ticker,prediction_date,outcome_date,window_days,
            baseline_close,outcome_close,actual_return,direction_correct,probability_error,scored_at)
           VALUES (1,1,?1,'QQQ','2026-07-01','2026-07-06',5,
                   100.0,104.0,0.04,1,0.35,1700000900)"#,
        [RUN_ID],
    )
    .unwrap();
}

#[test]
fn migrates_representative_unversioned_database_and_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("legacy.sqlite");
    let conn = Connection::open(&db_path).unwrap();
    let expected_context = json!([
        {"role":"system","content":"technical instructions"},
        {"role":"user","content":"analyse QQQ"},
        {"role":"assistant","content":"bullish trend"}
    ]);
    create_legacy_database(&conn, &expected_context.to_string());
    assert_eq!(user_version(&conn), 0);

    ensure_schema(&conn).unwrap();

    assert_eq!(
        user_version(&conn),
        orchestrator_sql::schema::CURRENT_SCHEMA_VERSION
    );
    for (table, expected) in [
        ("runs", 1),
        ("agent_events", 1),
        ("role_turn_summaries", 1),
        ("jin10_items", 1),
        ("technical_bars", 2),
        ("predictions", 1),
        ("outcomes", 1),
        ("candidate_experiences", 1),
        ("memory_items", 1),
        ("memory_versions", 1),
        ("memory_history", 1),
        ("phase_summaries", 1),
        ("phase_summary_details", 1),
        ("attention_ledger", 1),
    ] {
        assert_eq!(count(&conn, table), expected, "unexpected {table} count");
    }
    assert!(!table_exists(&conn, "technical_series"));
    assert_eq!(technical_row_count(&conn, Some("daily")).unwrap(), 2);
    let bars = load_technical_series(&conn, "QQQ", "daily").unwrap();
    assert_eq!(bars.len(), 2);
    assert_eq!(bars[0].date, "2026-07-01");
    assert_eq!(bars[0].values.get("Close"), Some(&100.0));
    assert_eq!(bars[1].values.get("Close"), Some(&102.5));

    let restored = turn_history_items(&conn, "turn-1").unwrap();
    assert_eq!(Value::Array(restored), expected_context);
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        0
    );
    assert_eq!(
        conn.query_row(
            "SELECT current_version_id FROM memory_items WHERE memory_id='memory-1'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "memory-v1"
    );
    assert_eq!(
        conn.query_row(
            "SELECT outcome_due_date FROM predictions WHERE id=1",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "2026-07-06"
    );
    let (run_created_ms, event_created_ms, scored_at_ms): (i64, i64, i64) = conn
        .query_row(
            r#"SELECT r.created_at_ms,e.created_at_ms,o.scored_at_ms
               FROM runs r
               JOIN agent_events e ON e.run_id=r.run_id
               JOIN predictions p ON p.run_id=r.run_id
               JOIN outcomes o ON o.prediction_id=p.id
               WHERE r.run_id=?1"#,
            [RUN_ID],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(run_created_ms, 1_700_000_000_000);
    assert_eq!(event_created_ms, 1_700_000_100_000);
    assert_eq!(scored_at_ms, 1_700_000_900_000);
    let outcome_snapshot: (String, String, String, i64) = conn
        .query_row(
            r#"SELECT run_id_snapshot,ticker_snapshot,prediction_date_snapshot,
                      window_days_snapshot
               FROM outcomes WHERE prediction_id=1"#,
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        outcome_snapshot,
        (
            RUN_ID.to_string(),
            "QQQ".to_string(),
            "2026-07-01".to_string(),
            5
        )
    );
    let (legacy_attention, latest_score, content, metadata): (i64, f64, String, String) = conn
        .query_row(
            r#"SELECT legacy_attention,latest_attention_score,content,metadata_json
               FROM jin10_items WHERE id='jin10-1'"#,
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(legacy_attention, 1);
    assert!((latest_score - 0.8).abs() < f64::EPSILON);
    let ledger_score: f64 = conn
        .query_row(
            "SELECT score FROM attention_ledger WHERE subject_kind='jin10' AND subject_id='jin10-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!((latest_score - ledger_score).abs() < f64::EPSILON);
    assert_eq!(content, "Fed commentary");
    assert_eq!(
        serde_json::from_str::<Value>(&metadata).unwrap(),
        json!({"source":"jin10"})
    );

    let counts_before = [
        "runs",
        "agent_events",
        "role_turn_summaries",
        "jin10_items",
        "technical_bars",
        "predictions",
        "outcomes",
        "candidate_experiences",
        "memory_items",
        "memory_versions",
        "memory_history",
        "phase_summaries",
        "phase_summary_details",
        "attention_ledger",
    ]
    .map(|table| (table, count(&conn, table)));
    ensure_schema(&conn).unwrap();
    for (table, expected) in counts_before {
        assert_eq!(count(&conn, table), expected, "idempotency changed {table}");
    }
    assert_eq!(
        Value::Array(turn_history_items(&conn, "turn-1").unwrap()),
        expected_context
    );
}

#[test]
fn invalid_legacy_json_rolls_back_the_entire_migration() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("invalid-json.sqlite");
    let conn = Connection::open(&db_path).unwrap();
    create_legacy_database(&conn, "[{invalid-json]");

    let error = ensure_schema(&conn).unwrap_err();
    assert!(
        format!("{error:#}").contains("invalid legacy agent context for turn turn-1"),
        "unexpected migration error: {error:#}"
    );
    assert_eq!(user_version(&conn), 0);
    assert!(table_exists(&conn, "runs"));
    assert!(table_exists(&conn, "agent_events"));
    assert!(table_exists(&conn, "technical_series"));
    assert!(!table_exists(&conn, "technical_bars"));
    assert!(!table_exists(&conn, "__legacy_runs"));
    assert!(!table_exists(&conn, "__legacy_agent_events"));
    assert_eq!(count(&conn, "runs"), 1);
    assert_eq!(count(&conn, "agent_events"), 1);
    assert_eq!(count(&conn, "technical_series"), 1);
    assert_eq!(
        conn.query_row(
            "SELECT full_context_json FROM agent_events WHERE turn_id='turn-1'",
            [],
            |row| row.get::<_, String>(0)
        )
        .unwrap(),
        "[{invalid-json]"
    );
    assert_eq!(
        conn.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn connect_creates_a_restorable_pre_migration_backup() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("legacy.sqlite");
    let legacy_conn = Connection::open(&db_path).unwrap();
    create_legacy_database(
        &legacy_conn,
        &json!([{"role":"user","content":"preserve this context"}]).to_string(),
    );
    drop(legacy_conn);

    let migrated = connect(&db_path).unwrap();
    assert_eq!(
        user_version(&migrated),
        orchestrator_sql::schema::CURRENT_SCHEMA_VERSION
    );
    assert_eq!(count(&migrated, "technical_bars"), 2);
    drop(migrated);

    let backup_prefix = "legacy.sqlite.pre-migration-v0-";
    let backups = std::fs::read_dir(temp.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(backup_prefix) && name.ends_with(".bak"))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        backups.len(),
        1,
        "expected exactly one pre-migration backup"
    );

    let backup = Connection::open(&backups[0]).unwrap();
    assert_eq!(user_version(&backup), 0);
    assert!(table_exists(&backup, "technical_series"));
    assert!(!table_exists(&backup, "technical_bars"));
    assert_eq!(count(&backup, "runs"), 1);
    assert_eq!(count(&backup, "agent_events"), 1);
    assert_eq!(count(&backup, "technical_series"), 1);
    assert_eq!(
        backup
            .query_row(
                "SELECT workflow_version FROM runs WHERE run_id=?1",
                [RUN_ID],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "workflow-v1"
    );
    assert_eq!(
        backup
            .query_row(
                "SELECT rows_json FROM technical_series WHERE ticker='QQQ' AND interval='daily'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map(|raw| serde_json::from_str::<Value>(&raw).unwrap())
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn rerunning_a_run_preserves_archived_fields() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("current.sqlite");
    let mut conn = connect(&db_path).unwrap();
    conn.execute(
        r#"INSERT INTO runs
           (run_id,current_date,created_at_ms,status,current_phase,error_message,completed_at_ms,
            run_dir,db_path,git_sha,config_hash,artifact_path,workflow_version,
            prompt_versions_json,degraded,phase_count,total_elapsed_ms)
           VALUES ('rerun-1','2026-07-01',1111,'completed',8,'old error',2222,
                   '/archive/run','/archive/db','git-123','cfg-123','artifact.json',
                   'workflow-v7','{"manager":"prompt-v3"}',1,9,98765)"#,
        [],
    )
    .unwrap();

    write_run_record(
        &mut conn,
        &RunRecordInput {
            run_id: "rerun-1",
            current_date: "2026-07-22",
        },
    )
    .unwrap();

    let row = conn
        .query_row(
            r#"SELECT "current_date",created_at_ms,status,current_phase,error_message,completed_at_ms,
                      run_dir,db_path,git_sha,config_hash,artifact_path,workflow_version,
                      prompt_versions_json,degraded,phase_count,total_elapsed_ms
               FROM runs WHERE run_id='rerun-1'"#,
            [],
            |row| {
                Ok(ArchivedRun {
                    current_date: row.get(0)?,
                    created_at_ms: row.get(1)?,
                    status: row.get(2)?,
                    current_phase: row.get(3)?,
                    error_message: row.get(4)?,
                    completed_at_ms: row.get(5)?,
                    run_dir: row.get(6)?,
                    db_path: row.get(7)?,
                    git_sha: row.get(8)?,
                    config_hash: row.get(9)?,
                    artifact_path: row.get(10)?,
                    workflow_version: row.get(11)?,
                    prompt_versions_json: row.get(12)?,
                    degraded: row.get(13)?,
                    phase_count: row.get(14)?,
                    total_elapsed_ms: row.get(15)?,
                })
            },
        )
        .unwrap();
    assert_eq!(row.current_date, "2026-07-22");
    assert_eq!(row.created_at_ms, 1111);
    assert_eq!(row.status, "running");
    assert_eq!(row.current_phase, None);
    assert_eq!(row.error_message, None);
    assert_eq!(row.completed_at_ms, None);
    assert_eq!(row.run_dir.as_deref(), Some("/archive/run"));
    assert_eq!(row.db_path.as_deref(), Some("/archive/db"));
    assert_eq!(row.git_sha.as_deref(), Some("git-123"));
    assert_eq!(row.config_hash.as_deref(), Some("cfg-123"));
    assert_eq!(row.artifact_path.as_deref(), Some("artifact.json"));
    assert_eq!(row.workflow_version.as_deref(), Some("workflow-v7"));
    assert_eq!(row.prompt_versions_json, r#"{"manager":"prompt-v3"}"#);
    assert_eq!(row.degraded, 1);
    assert_eq!(row.phase_count, 9);
    assert_eq!(row.total_elapsed_ms, 98765);
}

#[test]
fn legacy_full_context_snapshots_are_compacted_to_checkpoint_and_delta() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE runs(run_id TEXT PRIMARY KEY,current_date TEXT NOT NULL,created_at INTEGER NOT NULL);
        CREATE TABLE agent_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            turn_id TEXT NOT NULL UNIQUE,
            run_id TEXT NOT NULL,
            phase INTEGER,
            turn_number INTEGER NOT NULL,
            role TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            full_context_json TEXT NOT NULL,
            summary TEXT NOT NULL
        );
        INSERT INTO runs VALUES ('context-run','2026-01-01',1700000000);
        INSERT INTO agent_events(turn_id,run_id,phase,turn_number,role,created_at,full_context_json,summary)
        VALUES
          ('context-1','context-run',1,1,'analyst.technical',1700000001,'[{"n":1}]','one'),
          ('context-2','context-run',1,2,'analyst.technical',1700000002,'[{"n":1},{"n":2}]','two'),
          ('context-news','context-run',1,3,'analyst.news_macro',1700000003,'[{"n":"news"}]','news');
        "#,
    )
    .unwrap();

    ensure_schema(&conn).unwrap();
    let second_checkpoint: Option<String> = conn
        .query_row(
            "SELECT full_context_json FROM agent_events WHERE turn_id='context-2'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(second_checkpoint.is_none());
    assert_eq!(
        turn_history_items(&conn, "context-2").unwrap(),
        vec![json!({"n":1}), json!({"n":2})]
    );
    assert_eq!(
        turn_history_items(&conn, "context-news").unwrap(),
        vec![json!({"n":"news"})]
    );
}

fn database_bytes(conn: &Connection) -> i64 {
    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    let page_size: i64 = conn
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    page_count * page_size
}

fn explain_details(conn: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    stmt.query_map([], |row| row.get(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

#[test]
fn representative_migration_reduces_context_density_and_improves_query_plans() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("density.sqlite");
    let conn = Connection::open(&path).unwrap();
    create_legacy_database(&conn, r#"[{"message":"seed"}]"#);

    let mut context = vec![json!({"message":"seed"})];
    for turn in 2..=120 {
        context.push(json!({
            "message": format!("turn-{turn}-{}", "representative-context-payload".repeat(4))
        }));
        conn.execute(
            r#"INSERT INTO agent_events
               (turn_id,run_id,phase,turn_number,role,created_at,full_context_json,summary)
               VALUES (?1,?2,1,?3,'analyst.technical',?4,?5,?6)"#,
            params![
                format!("density-turn-{turn}"),
                RUN_ID,
                turn,
                1_700_000_100 + turn,
                Value::Array(context.clone()).to_string(),
                format!("turn {turn}")
            ],
        )
        .unwrap();
    }
    conn.execute_batch("VACUUM").unwrap();
    let old_context_bytes: i64 = conn
        .query_row(
            "SELECT SUM(length(full_context_json)) FROM agent_events",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let old_database_bytes = database_bytes(&conn);
    let old_latest_run_plan = explain_details(
        &conn,
        "SELECT run_id FROM runs ORDER BY current_date DESC,created_at DESC LIMIT 1",
    );
    let old_latest_turn_plan = explain_details(
        &conn,
        "SELECT turn_id FROM agent_events WHERE run_id='legacy-run-1' ORDER BY turn_number DESC LIMIT 1",
    );

    ensure_schema(&conn).unwrap();
    conn.execute_batch("VACUUM").unwrap();
    let new_context_bytes: i64 = conn
        .query_row(
            r#"SELECT SUM(length(COALESCE(full_context_json,'')) + length(context_delta_json))
               FROM agent_events"#,
            [],
            |row| row.get(0),
        )
        .unwrap();
    let new_database_bytes = database_bytes(&conn);
    let plans = core_query_plans(&conn).unwrap();

    assert!(new_context_bytes * 2 < old_context_bytes);
    assert!(new_database_bytes < old_database_bytes);
    assert!(old_latest_run_plan
        .iter()
        .any(|detail| detail.contains("SCAN")));
    assert!(old_latest_run_plan
        .iter()
        .any(|detail| detail.contains("TEMP B-TREE")));
    assert!(old_latest_turn_plan
        .iter()
        .any(|detail| detail.contains("TEMP B-TREE")));
    assert!(plans["latest_run"]
        .as_array()
        .unwrap()
        .iter()
        .any(|detail| detail.as_str().unwrap().contains("idx_runs_latest")));
    assert!(plans["latest_agent_turn"]
        .as_array()
        .unwrap()
        .iter()
        .any(|detail| detail
            .as_str()
            .unwrap()
            .contains("idx_agent_events_run_turn")));

    println!(
        "density_metrics old_context_bytes={old_context_bytes} new_context_bytes={new_context_bytes} old_database_bytes={old_database_bytes} new_database_bytes={new_database_bytes}"
    );
    println!("old_latest_run_plan={old_latest_run_plan:?}");
    println!("old_latest_turn_plan={old_latest_turn_plan:?}");
    println!("new_query_plans={plans}");
}
