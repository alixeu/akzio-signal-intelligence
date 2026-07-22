use orchestrator_sql::{
    cleanup_database, connect, core_query_plans, database_doctor, open_read_only, wal_checkpoint,
    RetentionPolicy,
};
use rusqlite::{params, Connection};
use serde_json::Value;

const OLD_MS: i64 = 1;
const RECENT_MS: i64 = 4_102_444_800_000; // 2100-01-01T00:00:00Z

#[derive(Debug, PartialEq, Eq)]
struct MaintenanceState {
    agent_events: Vec<(String, Option<String>, String, String)>,
    jin10_ids: Vec<String>,
    technical_bars: Vec<String>,
    attention_ids: Vec<String>,
}

fn insert_run(conn: &Connection, run_id: &str) {
    conn.execute(
        r#"INSERT INTO runs
           (run_id,current_date,created_at_ms,status,prompt_versions_json,degraded,
            phase_count,total_elapsed_ms)
           VALUES (?1,'2020-01-01',?2,'completed','{}',0,1,10)"#,
        params![run_id, OLD_MS],
    )
    .unwrap();
}

fn insert_agent_event(
    conn: &Connection,
    turn_id: &str,
    run_id: &str,
    created_at_ms: i64,
    context_warning: bool,
) {
    conn.execute(
        r#"INSERT INTO agent_events
           (turn_id,run_id,turn_number,role,created_at_ms,full_context_json,
            context_delta_json,context_hash,summary,input_tokens,output_tokens,
            cached_tokens,reasoning_tokens,total_tokens,non_cached_input_tokens,
            visible_output_tokens,cost_usd,context_warning,elapsed_ms)
           VALUES (?1,?2,1,'analyst.technical',?3,'[{"message":"checkpoint"}]',
                   '[{"message":"delta"}]',?4,'summary',1,1,0,0,2,1,1,0.0,?5,5)"#,
        params![
            turn_id,
            run_id,
            created_at_ms,
            "a".repeat(64),
            i64::from(context_warning)
        ],
    )
    .unwrap();
}

fn insert_jin10(conn: &Connection, id: &str, imported_at_ms: i64, latest_attention_score: f64) {
    conn.execute(
        r#"INSERT INTO jin10_items
           (id,content,time_raw,item_time_ms,latest_attention_score,imported_at_ms,
            metadata_json,legacy_attention)
           VALUES (?1,?2,'2020-01-01 00:00:00',?3,?4,?5,'{}',0)"#,
        params![
            id,
            format!("content for {id}"),
            OLD_MS,
            latest_attention_score,
            imported_at_ms
        ],
    )
    .unwrap();
}

fn insert_technical_bar(conn: &Connection, ticker: &str, bar_time: &str, imported_at_ms: i64) {
    conn.execute(
        r#"INSERT INTO technical_bars
           (ticker,interval,bar_time,close,values_json,imported_at_ms)
           VALUES (?1,'daily',?2,100.0,'{"close":100.0}',?3)"#,
        params![ticker, bar_time, imported_at_ms],
    )
    .unwrap();
}

fn seed_retention_data(conn: &Connection) {
    insert_run(conn, "retention-run");
    insert_agent_event(conn, "old-context", "retention-run", OLD_MS, false);
    insert_agent_event(conn, "old-debug", "retention-run", OLD_MS, true);
    insert_agent_event(conn, "recent-context", "retention-run", RECENT_MS, false);

    insert_jin10(conn, "old-unreferenced", OLD_MS, 0.0);
    insert_jin10(conn, "old-referenced", OLD_MS, 0.8);
    insert_jin10(conn, "recent-unreferenced", RECENT_MS, 0.0);
    conn.execute(
        r#"INSERT INTO attention_ledger
           (id,run_id,turn_id,role,subject_kind,subject_id,score,phase,created_at_ms)
           VALUES ('old-attention','retention-run','old-context','analyst.news_macro',
                   'jin10','old-referenced',0.8,1,?1)"#,
        [OLD_MS],
    )
    .unwrap();

    insert_technical_bar(conn, "SOXX", "2020-01-05", OLD_MS);
    insert_technical_bar(conn, "QQQ", "2020-01-05", OLD_MS);
    insert_technical_bar(conn, "QQQ", "2099-01-01", RECENT_MS);
    conn.execute(
        r#"INSERT INTO predictions
           (run_id,ticker,prediction_date,outcome_due_date,long_probability,
            short_probability,window_days,market_regime_json,
            agent_probabilities_json,created_at_ms)
           VALUES ('retention-run','QQQ','2020-01-01','2020-01-10',0.6,0.4,9,
                   '{}','{}',?1)"#,
        [OLD_MS],
    )
    .unwrap();
}

fn retention_policy() -> RetentionPolicy {
    RetentionPolicy {
        context_checkpoint_days: 1,
        jin10_unreferenced_days: 1,
        attention_ledger_days: Some(1),
        debug_agent_events_days: Some(1),
        technical_bar_days: 1,
    }
}

fn maintenance_state(conn: &Connection) -> MaintenanceState {
    let agent_events = {
        let mut stmt = conn
            .prepare(
                "SELECT turn_id,full_context_json,context_delta_json,context_hash \
                 FROM agent_events ORDER BY turn_id",
            )
            .unwrap();
        stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
    };
    let jin10_ids = query_strings(conn, "SELECT id FROM jin10_items ORDER BY id");
    let technical_bars = query_strings(
        conn,
        "SELECT ticker || ':' || interval || ':' || bar_time \
         FROM technical_bars ORDER BY ticker,interval,bar_time",
    );
    let attention_ids = query_strings(conn, "SELECT id FROM attention_ledger ORDER BY id");
    MaintenanceState {
        agent_events,
        jin10_ids,
        technical_bars,
        attention_ids,
    }
}

fn query_strings(conn: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

fn affected(report: &Value, key: &str) -> i64 {
    report["affected"][key]
        .as_i64()
        .unwrap_or_else(|| panic!("missing affected count for {key}: {report}"))
}

#[test]
fn doctor_runs_on_a_read_only_connection_and_reports_health_and_sizes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("doctor.sqlite");
    let conn = connect(&path).unwrap();
    insert_run(&conn, "doctor-run");
    conn.execute(
        r#"UPDATE runs SET prompt_versions_json='{"prompt":1}' WHERE run_id='doctor-run'"#,
        [],
    )
    .unwrap();
    drop(conn);

    let read_only = open_read_only(&path).unwrap();
    let changes_before = read_only.total_changes();
    let report = database_doctor(&read_only).unwrap();

    assert_eq!(read_only.total_changes(), changes_before);
    assert_eq!(report["quick_check"], "ok");
    assert_eq!(report["foreign_key_check"], "ok");
    assert_eq!(report["orphan_foreign_keys"], 0);
    assert_eq!(report["table_rows"]["runs"], 1);
    assert!(report["storage"]["page_count"].as_i64().unwrap() > 0);
    assert!(
        report["storage"]["estimated_database_bytes"]
            .as_i64()
            .unwrap()
            > 0
    );

    let prompt_lengths = report["json_lengths"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["table"] == "runs" && entry["column"] == "prompt_versions_json")
        .expect("doctor should report runs.prompt_versions_json lengths");
    assert_eq!(prompt_lengths["average_bytes"], 12.0);
    assert_eq!(prompt_lengths["maximum_bytes"], 12);

    let write_error = read_only
        .execute("DELETE FROM runs WHERE run_id='doctor-run'", [])
        .expect_err("the doctor connection must remain read-only");
    assert!(write_error
        .to_string()
        .to_ascii_lowercase()
        .contains("readonly"));
}

#[test]
fn cleanup_dry_run_reports_candidates_without_changing_any_payload_or_row() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("cleanup-dry-run.sqlite")).unwrap();
    seed_retention_data(&conn);
    let before = maintenance_state(&conn);

    let report = cleanup_database(&conn, &retention_policy(), true, true).unwrap();

    assert_eq!(report["dry_run"], true);
    assert_eq!(report["optimize"], false);
    assert_eq!(affected(&report, "agent_context_payloads"), 2);
    assert_eq!(affected(&report, "jin10_items"), 1);
    assert_eq!(affected(&report, "technical_bars"), 1);
    assert_eq!(affected(&report, "attention_ledger"), 1);
    assert_eq!(affected(&report, "debug_agent_events"), 1);
    assert_eq!(maintenance_state(&conn), before);
}

#[test]
fn cleanup_applies_retention_rules_and_preserves_referenced_or_recent_data() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("cleanup.sqlite")).unwrap();
    seed_retention_data(&conn);

    let report = cleanup_database(&conn, &retention_policy(), false, false).unwrap();

    assert_eq!(report["dry_run"], false);
    assert_eq!(affected(&report, "agent_context_payloads"), 2);
    assert_eq!(
        query_strings(&conn, "SELECT turn_id FROM agent_events ORDER BY turn_id"),
        vec!["old-context", "recent-context"]
    );
    let (checkpoint, delta, hash): (Option<String>, String, String) = conn
        .query_row(
            "SELECT full_context_json,context_delta_json,context_hash \
             FROM agent_events WHERE turn_id='old-context'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(checkpoint, None);
    assert_eq!(delta, "[]");
    assert_eq!(hash.len(), 64);

    assert_eq!(
        query_strings(&conn, "SELECT id FROM jin10_items ORDER BY id"),
        vec!["old-referenced", "recent-unreferenced"]
    );
    assert_eq!(
        query_strings(
            &conn,
            "SELECT ticker || ':' || bar_time FROM technical_bars ORDER BY ticker,bar_time"
        ),
        vec!["QQQ:2020-01-05", "QQQ:2099-01-01"]
    );
    assert!(query_strings(&conn, "SELECT id FROM attention_ledger").is_empty());
}

#[test]
fn cleanup_rolls_back_earlier_changes_when_a_later_delete_fails() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("cleanup-rollback.sqlite")).unwrap();
    seed_retention_data(&conn);
    let before = maintenance_state(&conn);
    conn.execute_batch(
        r#"CREATE TRIGGER abort_retention_delete
           BEFORE DELETE ON jin10_items
           WHEN OLD.id = 'old-unreferenced'
           BEGIN
               SELECT RAISE(ABORT, 'forced retention failure');
           END;"#,
    )
    .unwrap();

    let error = cleanup_database(&conn, &retention_policy(), false, false)
        .expect_err("the trigger should abort cleanup");

    assert!(error.to_string().contains("forced retention failure"));
    assert_eq!(maintenance_state(&conn), before);
}

#[test]
fn core_query_plans_use_the_maintenance_indexes() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("query-plans.sqlite")).unwrap();
    let plans = core_query_plans(&conn).unwrap();

    for (query, access_paths) in [
        ("latest_run", &["idx_runs_latest"][..]),
        ("latest_agent_turn", &["idx_agent_events_run_turn"][..]),
        (
            "run_ticker_summaries",
            &["idx_role_summaries_run_ticker_created"][..],
        ),
        ("jin10_attention", &["idx_jin10_attention_time"][..]),
        ("expired_predictions", &["idx_predictions_due_unscored"][..]),
        (
            "technical_close_before",
            // `technical_bars` is WITHOUT ROWID, so SQLite may correctly
            // prefer its covering primary key over the equivalent DESC index.
            &["idx_technical_bars_lookup", "primary key"][..],
        ),
    ] {
        assert_plan_uses_access_path(&plans, query, access_paths);
    }
}

fn assert_plan_uses_access_path(plans: &Value, query: &str, expected_paths: &[&str]) {
    let lines = plans[query]
        .as_array()
        .unwrap_or_else(|| panic!("missing query plan {query}: {plans}"));
    assert!(
        lines.iter().filter_map(Value::as_str).any(|line| {
            let normalized = line.to_ascii_lowercase();
            normalized.contains("using")
                && expected_paths
                    .iter()
                    .any(|path| normalized.contains(&path.to_ascii_lowercase()))
        }),
        "query {query} did not use any of {expected_paths:?}; plan was {lines:?}"
    );
}

#[test]
fn explicit_passive_and_truncate_wal_checkpoints_can_run() {
    let temp = tempfile::tempdir().unwrap();
    let conn = connect(temp.path().join("checkpoint.sqlite")).unwrap();
    insert_run(&conn, "checkpoint-run");

    let passive = wal_checkpoint(&conn, false).unwrap();
    assert_eq!(passive["mode"], "PASSIVE");
    assert_checkpoint_counts_are_non_negative(&passive);

    conn.execute(
        "UPDATE runs SET total_elapsed_ms=20 WHERE run_id='checkpoint-run'",
        [],
    )
    .unwrap();
    let truncate = wal_checkpoint(&conn, true).unwrap();
    assert_eq!(truncate["mode"], "TRUNCATE");
    assert_checkpoint_counts_are_non_negative(&truncate);
}

fn assert_checkpoint_counts_are_non_negative(report: &Value) {
    for field in ["busy", "log_frames", "checkpointed_frames"] {
        assert!(
            report[field]
                .as_i64()
                .unwrap_or_else(|| panic!("missing checkpoint field {field}: {report}"))
                >= 0
        );
    }
}
