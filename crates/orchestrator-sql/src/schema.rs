use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

pub const AGGREGATE_TICKER: &str = "__ALL__";

pub fn connect(path: impl AsRef<Path>) -> Result<Connection> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create sqlite dir {}", parent.display()))?;
    }
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open sqlite database {}", path.display()))?;
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA busy_timeout=5000;
        PRAGMA synchronous=NORMAL;
        PRAGMA foreign_keys=ON;
    ",
    )?;
    ensure_schema(&conn)?;
    Ok(conn)
}

pub fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        -- Phase 1 cleanup: drop dead tables from prior schema versions
        DROP TABLE IF EXISTS events;
        DROP TABLE IF EXISTS run_phases;
        DROP TABLE IF EXISTS workflow_snapshots;
        DROP TABLE IF EXISTS market_regimes;
        DROP TABLE IF EXISTS memory_links;
        DROP TABLE IF EXISTS memory_metrics;
        DROP TABLE IF EXISTS agent_probabilities;
        DROP TABLE IF EXISTS memory_search_fts;
        DROP TABLE IF EXISTS external_source_items;
        DROP TABLE IF EXISTS run_archive;
        DROP TABLE IF EXISTS system_metrics;
        DROP VIEW IF EXISTS system_metrics;
        DROP TABLE IF EXISTS turn_context_items;
        DROP TABLE IF EXISTS prompt_metrics;
        DROP TABLE IF EXISTS agent_turns;
        DROP TABLE IF EXISTS agent_turn_items;

        -- Legacy multi-source tables are permanently removed. Jin10 uses jin10_items.
        DROP TABLE IF EXISTS external_items;
        DROP TABLE IF EXISTS youtube_videos;
        DROP TABLE IF EXISTS youtube_transcripts;
        DROP TABLE IF EXISTS social_items;
        DROP TABLE IF EXISTS jin10_flash_items;

        -- Row-per-feature legacy storage is replaced by compact technical_series snapshots.
        DROP TABLE IF EXISTS technical_features;

        -- Phase 4 cleanup: dead indexes
        DROP INDEX IF EXISTS idx_agent_turn_items_session;
        DROP INDEX IF EXISTS idx_agent_turn_items_turn;
        DROP INDEX IF EXISTS idx_agent_turn_items_run_type;
        DROP INDEX IF EXISTS idx_agent_turn_items_run_created;
        DROP INDEX IF EXISTS idx_agent_turns_session;
        DROP INDEX IF EXISTS idx_agent_turns_run_phase_role;
        DROP INDEX IF EXISTS idx_agent_turns_created;
        DROP INDEX IF EXISTS idx_role_turn_summaries_run_role;
        DROP INDEX IF EXISTS idx_jin10_items_hash;
        DROP INDEX IF EXISTS idx_jin10_items_usage;
        DROP INDEX IF EXISTS idx_youtube_videos_hash;
        DROP INDEX IF EXISTS idx_youtube_transcripts_hash;
        DROP INDEX IF EXISTS idx_jin10_items_time;
        DROP INDEX IF EXISTS idx_youtube_videos_ticker_time;
        DROP INDEX IF EXISTS idx_social_items_source_ticker;

        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            current_date TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            current_phase INTEGER,
            error_message TEXT NOT NULL DEFAULT '',
            completed_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS agent_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            turn_id TEXT NOT NULL UNIQUE,
            run_id TEXT NOT NULL DEFAULT '',
            phase INTEGER,
            turn_number INTEGER NOT NULL DEFAULT 0,
            role TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            full_context_json TEXT NOT NULL DEFAULT '[]',
            summary TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS role_turn_summaries (
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
        CREATE INDEX IF NOT EXISTS idx_role_turn_summaries_run_ticker_phase_role
            ON role_turn_summaries(run_id, ticker, phase, role);
        CREATE INDEX IF NOT EXISTS idx_role_turn_summaries_turn
            ON role_turn_summaries(turn_id);
        CREATE TABLE IF NOT EXISTS memory_items (
            memory_id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL DEFAULT '',
            scope TEXT NOT NULL DEFAULT '',
            memory_type TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active',
            current_version_id TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.0,
            expires_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_memory_items_lookup
            ON memory_items(ticker, status, memory_type, updated_at);
        CREATE TABLE IF NOT EXISTS memory_versions (
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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_versions_hash
            ON memory_versions(content_hash);
        CREATE TABLE IF NOT EXISTS memory_history (
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
        CREATE INDEX IF NOT EXISTS idx_memory_history_memory
            ON memory_history(memory_id, created_at);

        -- Dedicated Jin10 flash table.
        -- id = md5(time + content); content_json is the payload passed to the LLM;
        -- attention_score is the latest LLM attention weight (0.0-1.0).
        CREATE TABLE IF NOT EXISTS jin10_items (
            id TEXT PRIMARY KEY,
            content_json TEXT NOT NULL,
            attention_score REAL NOT NULL DEFAULT 0.0,
            item_time INTEGER NOT NULL DEFAULT 0,
            imported_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_jin10_items_time
            ON jin10_items(item_time DESC);
        CREATE INDEX IF NOT EXISTS idx_jin10_items_attention
            ON jin10_items(attention_score DESC);

        CREATE TABLE IF NOT EXISTS technical_series (
            ticker TEXT NOT NULL,
            interval TEXT NOT NULL,
            as_of TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            rows_json TEXT NOT NULL,
            imported_at INTEGER NOT NULL,
            PRIMARY KEY (ticker, interval),
            CHECK (row_count > 0)
        );
        CREATE INDEX IF NOT EXISTS idx_technical_series_as_of
            ON technical_series(interval, as_of DESC);

        -- Post-phase compressor: summary → detail index
        CREATE TABLE IF NOT EXISTS phase_summaries (
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
        CREATE INDEX IF NOT EXISTS idx_phase_summaries_run_phase
            ON phase_summaries(run_id, source_phase);
        CREATE INDEX IF NOT EXISTS idx_phase_summaries_run_ticker_phase
            ON phase_summaries(run_id, ticker, source_phase);

        CREATE TABLE IF NOT EXISTS phase_summary_details (
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
        CREATE INDEX IF NOT EXISTS idx_phase_summary_details_summary
            ON phase_summary_details(summary_id);
        CREATE INDEX IF NOT EXISTS idx_phase_summary_details_run_phase
            ON phase_summary_details(run_id, source_phase);

        -- Unified attention ledger (jin10 + summaries + details + future subjects)
        CREATE TABLE IF NOT EXISTS attention_ledger (
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
        CREATE INDEX IF NOT EXISTS idx_attention_ledger_run_turn
            ON attention_ledger(run_id, turn_id);
        CREATE INDEX IF NOT EXISTS idx_attention_ledger_run_role
            ON attention_ledger(run_id, role);
        CREATE INDEX IF NOT EXISTS idx_attention_ledger_subject
            ON attention_ledger(run_id, subject_kind, subject_id);
        CREATE INDEX IF NOT EXISTS idx_attention_ledger_run_score
            ON attention_ledger(run_id, score DESC);

        CREATE TABLE IF NOT EXISTS predictions (
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
        CREATE INDEX IF NOT EXISTS idx_predictions_ticker_date
            ON predictions(ticker, prediction_date);
        CREATE INDEX IF NOT EXISTS idx_predictions_ticker_pred_date_run
            ON predictions(ticker, prediction_date, run_id);
        CREATE TABLE IF NOT EXISTS outcomes (
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
        CREATE INDEX IF NOT EXISTS idx_outcomes_ticker_date
            ON outcomes(ticker, prediction_date);
        CREATE INDEX IF NOT EXISTS idx_outcomes_ticker_outcome_date
            ON outcomes(ticker, outcome_date);
        CREATE INDEX IF NOT EXISTS idx_outcomes_prediction
            ON outcomes(prediction_id, outcome_date);
        CREATE TABLE IF NOT EXISTS candidate_experiences (
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
        CREATE INDEX IF NOT EXISTS idx_candidate_exp_status
            ON candidate_experiences(review_status, scope, experience_type);
        CREATE INDEX IF NOT EXISTS idx_candidate_exp_scope_type
            ON candidate_experiences(scope, scope_value, experience_type);
        "#,
    )?;

    rebuild_agent_events_if_legacy(conn)?;
    migrate_jin10_items_attention_score(conn)?;

    for column_sql in [
        "status TEXT NOT NULL DEFAULT 'pending'",
        "current_phase INTEGER",
        "error_message TEXT NOT NULL DEFAULT ''",
        "completed_at INTEGER",
        "run_dir TEXT NOT NULL DEFAULT ''",
        "db_path TEXT NOT NULL DEFAULT ''",
        "git_sha TEXT NOT NULL DEFAULT ''",
        "config_hash TEXT NOT NULL DEFAULT ''",
        "artifact_path TEXT NOT NULL DEFAULT ''",
        "workflow_version TEXT NOT NULL DEFAULT ''",
        "prompt_versions_json TEXT NOT NULL DEFAULT '{}'",
        "degraded INTEGER NOT NULL DEFAULT 0",
        "phase_count INTEGER NOT NULL DEFAULT 0",
        "total_elapsed_ms INTEGER NOT NULL DEFAULT 0",
    ] {
        add_column_if_missing(conn, "runs", column_sql)?;
    }

    for column_sql in [
        "market_regime_json TEXT NOT NULL DEFAULT '{}'",
        "quality_score REAL NOT NULL DEFAULT 0.0",
        "sample_count INTEGER NOT NULL DEFAULT 0",
        "recent_success_rate REAL NOT NULL DEFAULT 0.0",
        "reflection_version TEXT NOT NULL DEFAULT 'v1'",
        "promoted_from INTEGER",
    ] {
        add_column_if_missing(conn, "memory_items", column_sql)?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_memory_items_quality_time \
         ON memory_items(status, quality_score, updated_at)",
    )?;

    for column_sql in [
        "phase INTEGER",
        "turn_number INTEGER NOT NULL DEFAULT 0",
        "full_context_json TEXT NOT NULL DEFAULT '[]'",
        "summary TEXT NOT NULL DEFAULT ''",
        "model TEXT NOT NULL DEFAULT ''",
        "input_tokens INTEGER NOT NULL DEFAULT 0",
        "output_tokens INTEGER NOT NULL DEFAULT 0",
        "cached_tokens INTEGER NOT NULL DEFAULT 0",
        "reasoning_tokens INTEGER NOT NULL DEFAULT 0",
        "total_tokens INTEGER NOT NULL DEFAULT 0",
        "non_cached_input_tokens INTEGER NOT NULL DEFAULT 0",
        "visible_output_tokens INTEGER NOT NULL DEFAULT 0",
        "cost_usd REAL NOT NULL DEFAULT 0.0",
        "context_warning INTEGER NOT NULL DEFAULT 0",
        "elapsed_ms INTEGER NOT NULL DEFAULT 0",
    ] {
        add_column_if_missing(conn, "agent_events", column_sql)?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_agent_events_run_phase ON agent_events(run_id, phase);
         CREATE INDEX IF NOT EXISTS idx_agent_events_run_role ON agent_events(run_id, role)",
    )?;

    drop_column_if_exists(conn, "predictions", "prediction_horizon")?;
    drop_column_if_exists(conn, "outcomes", "market_regime_json")?;
    drop_column_if_exists(conn, "memory_versions", "invalidation_conditions_json")?;
    drop_column_if_exists(conn, "memory_versions", "follow_up_checks_json")?;
    drop_column_if_exists(conn, "memory_items", "source_run_id")?;
    drop_column_if_exists(conn, "memory_items", "source_role")?;

    Ok(())
}

/// Migrate jin10_items from llm_usage_count → attention_score (0.0-1.0).
fn migrate_jin10_items_attention_score(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(jin10_items)")?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Ok(());
    }
    let has_attention = columns.iter().any(|c| c == "attention_score");
    let has_usage = columns.iter().any(|c| c == "llm_usage_count");
    if has_attention && !has_usage {
        return Ok(());
    }
    if has_attention && has_usage {
        drop_column_if_exists(conn, "jin10_items", "llm_usage_count")?;
        return Ok(());
    }
    // Rebuild: map old integer usage into a soft prior attention in [0,1].
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS jin10_items_attention_mig (
            id TEXT PRIMARY KEY,
            content_json TEXT NOT NULL,
            attention_score REAL NOT NULL DEFAULT 0.0,
            item_time INTEGER NOT NULL DEFAULT 0,
            imported_at INTEGER NOT NULL
        );
        INSERT OR REPLACE INTO jin10_items_attention_mig
            (id, content_json, attention_score, item_time, imported_at)
        SELECT
            id,
            content_json,
            CASE
                WHEN llm_usage_count IS NULL OR llm_usage_count <= 0 THEN 0.0
                ELSE MIN(1.0, 1.0 - 1.0 / (1.0 + CAST(llm_usage_count AS REAL)))
            END,
            item_time,
            imported_at
        FROM jin10_items;
        DROP TABLE jin10_items;
        ALTER TABLE jin10_items_attention_mig RENAME TO jin10_items;
        CREATE INDEX IF NOT EXISTS idx_jin10_items_time
            ON jin10_items(item_time DESC);
        CREATE INDEX IF NOT EXISTS idx_jin10_items_attention
            ON jin10_items(attention_score DESC);
        "#,
    )?;
    Ok(())
}

fn rebuild_agent_events_if_legacy(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("PRAGMA table_info(agent_events)")?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if columns.is_empty() {
        return Ok(());
    }
    let legacy = columns
        .iter()
        .any(|c| c == "event_index" || c == "content_json")
        || !columns.iter().any(|c| c == "full_context_json");
    if !legacy {
        return Ok(());
    }

    // Legacy event-stream schema is incompatible with turn-summary agent_events.
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS agent_events_legacy_stream;
        ALTER TABLE agent_events RENAME TO agent_events_legacy_stream;
        CREATE TABLE agent_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            turn_id TEXT NOT NULL UNIQUE,
            run_id TEXT NOT NULL DEFAULT '',
            phase INTEGER,
            turn_number INTEGER NOT NULL DEFAULT 0,
            role TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            full_context_json TEXT NOT NULL DEFAULT '[]',
            summary TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_agent_events_run_phase
            ON agent_events(run_id, phase);
        "#,
    )?;
    Ok(())
}

fn drop_column_if_exists(conn: &Connection, table: &str, column: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if columns.iter().any(|c| c == column) {
        conn.execute(&format!("ALTER TABLE {table} DROP COLUMN {column}"), [])?;
    }
    Ok(())
}

fn add_column_if_missing(conn: &Connection, table: &str, column_sql: &str) -> Result<()> {
    let column_name = column_sql
        .split_whitespace()
        .next()
        .context("column sql cannot be empty")?;
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !columns.iter().any(|column| column == column_name) {
        conn.execute(&format!("ALTER TABLE {table} ADD COLUMN {column_sql}"), [])?;
    }
    Ok(())
}
