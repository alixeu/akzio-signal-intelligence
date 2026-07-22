use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, DatabaseName, OptionalExtension, Transaction};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

pub const AGGREGATE_TICKER: &str = "__ALL__";
pub const APPLICATION_ID: i64 = 0x415A_5349; // ASCII "AZSI"
pub const CURRENT_SCHEMA_VERSION: i64 = 3;

const MANAGED_TABLES: &[&str] = &[
    "runs",
    "agent_events",
    "role_turn_summaries",
    "memory_items",
    "memory_versions",
    "memory_history",
    "jin10_items",
    "technical_series",
    "technical_bars",
    "phase_summaries",
    "phase_summary_details",
    "attention_ledger",
    "predictions",
    "outcomes",
    "candidate_experiences",
];

const DROP_LEGACY_ORDER: &[&str] = &[
    "outcomes",
    "attention_ledger",
    "phase_summary_details",
    "phase_summaries",
    "memory_history",
    "memory_versions",
    "memory_items",
    "role_turn_summaries",
    "agent_events",
    "predictions",
    "candidate_experiences",
    "technical_series",
    "technical_bars",
    "jin10_items",
    "runs",
];

const ARCHIVED_LEGACY_TABLES: &[&str] = &[
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
    "agent_turns",
    "agent_turn_items",
    "external_items",
    "youtube_videos",
    "youtube_transcripts",
    "social_items",
    "jin10_flash_items",
    "technical_features",
];

pub fn connect(path: impl AsRef<Path>) -> Result<Connection> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create sqlite dir {}", parent.display()))?;
    }
    let conn = Connection::open(path)
        .with_context(|| format!("failed to open sqlite database {}", path.display()))?;
    configure_connection(&conn)?;
    create_migration_backup_if_needed(&conn, path)?;
    migrate_schema(&conn)?;
    Ok(conn)
}

fn create_migration_backup_if_needed(conn: &Connection, path: &Path) -> Result<()> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version >= CURRENT_SCHEMA_VERSION || path == Path::new(":memory:") {
        return Ok(());
    }
    let user_tables: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if user_tables == 0 {
        return Ok(());
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("SQLite path has no valid file name")?;
    let backup_name = format!(
        "{file_name}.pre-migration-v{version}-{}.bak",
        chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
    );
    let backup_path = path.with_file_name(backup_name);
    conn.backup(DatabaseName::Main, &backup_path, None)
        .with_context(|| {
            format!(
                "failed to back up SQLite database to {}",
                backup_path.display()
            )
        })?;
    Ok(())
}

/// Ensure the database is at the current schema version.
///
/// This remains public for in-memory tests and callers that supply their own
/// connection. It is safe to call repeatedly: completed migrations are gated by
/// `PRAGMA user_version` and no normal startup path drops a current business table.
pub fn ensure_schema(conn: &Connection) -> Result<()> {
    configure_connection(conn)?;
    migrate_schema(conn)
}

fn configure_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;
        PRAGMA busy_timeout=5000;
        PRAGMA synchronous=NORMAL;
        PRAGMA foreign_keys=ON;
        PRAGMA journal_size_limit=67108864;
        PRAGMA wal_autocheckpoint=1000;
        PRAGMA temp_store=MEMORY;
        "#,
    )?;
    let application_id: i64 = conn.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    if application_id != 0 && application_id != APPLICATION_ID {
        bail!(
            "database application_id {application_id} does not belong to akzio-signal-intelligence"
        );
    }
    if application_id == 0 {
        conn.pragma_update(None, "application_id", APPLICATION_ID)?;
    }
    Ok(())
}

fn migrate_schema(conn: &Connection) -> Result<()> {
    let mut version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version > CURRENT_SCHEMA_VERSION {
        bail!(
            "database schema version {version} is newer than supported version {CURRENT_SCHEMA_VERSION}"
        );
    }
    if version == CURRENT_SCHEMA_VERSION {
        return Ok(());
    }

    let has_managed_tables = MANAGED_TABLES.iter().try_fold(false, |found, table| {
        Ok::<_, anyhow::Error>(found || table_exists(conn, table)?)
    })?;

    if version == 0 && !has_managed_tables {
        let tx = conn.unchecked_transaction()?;
        create_latest_schema(&tx)?;
        set_user_version(&tx, CURRENT_SCHEMA_VERSION)?;
        tx.commit()?;
        return Ok(());
    }

    // Version 1 is the inferred, pre-user_version schema shipped before the
    // migration system. Version 2 is the normalized relational schema.
    if version <= 1 {
        migrate_legacy_to_v2(conn)?;
        version = 2;
    }
    if version == 2 {
        let tx = conn.unchecked_transaction()?;
        create_indexes(&tx)?;
        archive_unmanaged_legacy_tables(&tx)?;
        set_user_version(&tx, 3)?;
        tx.commit()?;
    }
    Ok(())
}

fn migrate_legacy_to_v2(conn: &Connection) -> Result<()> {
    // Foreign-key enforcement cannot be toggled inside a transaction. The
    // complete rebuild is still atomic and is validated with foreign_key_check
    // before commit.
    conn.pragma_update(None, "foreign_keys", "OFF")?;
    let result = (|| -> Result<()> {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch("PRAGMA defer_foreign_keys=ON")?;

        for table in MANAGED_TABLES {
            if table_exists(&tx, table)? {
                let legacy = legacy_name(table);
                if table_exists(&tx, &legacy)? {
                    bail!("cannot migrate while temporary legacy table {legacy} exists");
                }
                tx.execute_batch(&format!(
                    "ALTER TABLE {} RENAME TO {}",
                    quote_ident(table),
                    quote_ident(&legacy)
                ))?;
            }
        }
        drop_legacy_indexes(&tx)?;
        create_tables(&tx)?;
        copy_legacy_data(&tx)?;
        validate_migrated_data(&tx)?;

        for table in DROP_LEGACY_ORDER {
            let legacy = legacy_name(table);
            if table_exists(&tx, &legacy)? {
                tx.execute_batch(&format!("DROP TABLE {}", quote_ident(&legacy)))?;
            }
        }
        set_user_version(&tx, 2)?;
        tx.commit()?;
        Ok(())
    })();
    conn.pragma_update(None, "foreign_keys", "ON")?;
    result?;
    let violations: i64 =
        conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if violations != 0 {
        bail!("migration committed with {violations} foreign-key violations");
    }
    Ok(())
}

fn create_latest_schema(tx: &Transaction<'_>) -> Result<()> {
    create_tables(tx)?;
    create_indexes(tx)?;
    archive_unmanaged_legacy_tables(tx)?;
    Ok(())
}

fn create_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE runs (
            run_id TEXT PRIMARY KEY CHECK(length(trim(run_id)) > 0),
            current_date TEXT NOT NULL CHECK(length("current_date") = 10 AND strftime('%Y-%m-%d', "current_date") = "current_date"),
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            status TEXT NOT NULL CHECK(status IN ('pending','running','completed','failed')),
            current_phase INTEGER,
            error_message TEXT,
            completed_at_ms INTEGER,
            completed_at INTEGER GENERATED ALWAYS AS (completed_at_ms / 1000) VIRTUAL,
            run_dir TEXT,
            db_path TEXT,
            git_sha TEXT,
            config_hash TEXT,
            artifact_path TEXT,
            workflow_version TEXT,
            prompt_versions_json TEXT NOT NULL CHECK(json_valid(prompt_versions_json)),
            degraded INTEGER NOT NULL CHECK(degraded IN (0, 1)),
            phase_count INTEGER NOT NULL CHECK(phase_count >= 0),
            total_elapsed_ms INTEGER NOT NULL CHECK(total_elapsed_ms >= 0)
        );

        CREATE TABLE agent_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            turn_id TEXT NOT NULL UNIQUE CHECK(length(trim(turn_id)) > 0),
            run_id TEXT NOT NULL CHECK(length(trim(run_id)) > 0),
            phase INTEGER,
            turn_number INTEGER NOT NULL CHECK(turn_number >= 0),
            role TEXT NOT NULL CHECK(length(trim(role)) > 0),
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            full_context_json TEXT CHECK(full_context_json IS NULL OR json_valid(full_context_json)),
            context_delta_json TEXT NOT NULL CHECK(json_valid(context_delta_json)),
            context_hash TEXT NOT NULL CHECK(length(context_hash) = 64),
            summary TEXT NOT NULL CHECK(length(summary) <= 2048),
            model TEXT,
            input_tokens INTEGER NOT NULL CHECK(input_tokens >= 0),
            output_tokens INTEGER NOT NULL CHECK(output_tokens >= 0),
            cached_tokens INTEGER NOT NULL CHECK(cached_tokens >= 0),
            reasoning_tokens INTEGER NOT NULL CHECK(reasoning_tokens >= 0),
            total_tokens INTEGER NOT NULL CHECK(total_tokens >= 0),
            non_cached_input_tokens INTEGER NOT NULL CHECK(non_cached_input_tokens >= 0),
            visible_output_tokens INTEGER NOT NULL CHECK(visible_output_tokens >= 0),
            cost_usd REAL NOT NULL CHECK(cost_usd >= 0.0),
            context_warning INTEGER NOT NULL CHECK(context_warning IN (0, 1)),
            elapsed_ms INTEGER NOT NULL CHECK(elapsed_ms >= 0),
            FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE role_turn_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL CHECK(length(trim(run_id)) > 0),
            turn_id TEXT NOT NULL CHECK(length(trim(turn_id)) > 0),
            phase INTEGER,
            role TEXT NOT NULL CHECK(length(trim(role)) > 0),
            ticker TEXT NOT NULL CHECK(length(trim(ticker)) > 0),
            item_time_ms INTEGER NOT NULL CHECK(item_time_ms >= 0),
            item_time INTEGER GENERATED ALWAYS AS (item_time_ms / 1000) VIRTUAL,
            topic_id TEXT,
            debate_id TEXT,
            summary_type TEXT NOT NULL CHECK(length(trim(summary_type)) > 0),
            summary TEXT NOT NULL CHECK(length(summary) <= 2048),
            summary_json TEXT NOT NULL CHECK(json_valid(summary_json)),
            payload_schema_version INTEGER NOT NULL CHECK(payload_schema_version > 0),
            payload_hash TEXT NOT NULL CHECK(length(payload_hash) = 64),
            confidence REAL NOT NULL CHECK(confidence BETWEEN 0.0 AND 1.0),
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE memory_items (
            memory_id TEXT PRIMARY KEY CHECK(length(trim(memory_id)) > 0),
            ticker TEXT NOT NULL CHECK(length(trim(ticker)) > 0),
            scope TEXT NOT NULL CHECK(scope IN ('ticker','aggregate','global')),
            memory_type TEXT NOT NULL CHECK(length(trim(memory_type)) > 0),
            status TEXT NOT NULL CHECK(status IN ('active','inactive','archived')),
            current_version_id TEXT,
            confidence REAL NOT NULL CHECK(confidence BETWEEN 0.0 AND 1.0),
            expires_at_ms INTEGER,
            expires_at INTEGER GENERATED ALWAYS AS (expires_at_ms / 1000) VIRTUAL,
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= 0),
            updated_at INTEGER GENERATED ALWAYS AS (updated_at_ms / 1000) VIRTUAL,
            market_regime_json TEXT NOT NULL CHECK(json_valid(market_regime_json)),
            quality_score REAL NOT NULL CHECK(quality_score BETWEEN 0.0 AND 1.0),
            sample_count INTEGER NOT NULL CHECK(sample_count >= 0),
            recent_success_rate REAL NOT NULL CHECK(recent_success_rate BETWEEN 0.0 AND 1.0),
            reflection_version TEXT NOT NULL,
            promoted_from INTEGER
        );

        CREATE TABLE memory_versions (
            version_id TEXT PRIMARY KEY CHECK(length(trim(version_id)) > 0),
            memory_id TEXT NOT NULL,
            version_index INTEGER NOT NULL CHECK(version_index > 0),
            summary TEXT NOT NULL CHECK(length(summary) <= 2048),
            body_json TEXT NOT NULL CHECK(json_valid(body_json)),
            evidence_refs_json TEXT NOT NULL CHECK(json_valid(evidence_refs_json)),
            payload_schema_version INTEGER NOT NULL CHECK(payload_schema_version > 0),
            payload_hash TEXT NOT NULL CHECK(length(payload_hash) = 64),
            source_run_id TEXT,
            source_role TEXT,
            source_date TEXT,
            observed_at_ms INTEGER,
            observed_at INTEGER GENERATED ALWAYS AS (observed_at_ms / 1000) VIRTUAL,
            content_hash TEXT NOT NULL CHECK(length(content_hash) = 64),
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            UNIQUE(memory_id, version_index),
            FOREIGN KEY(memory_id) REFERENCES memory_items(memory_id) ON DELETE CASCADE
        );

        CREATE TABLE memory_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id TEXT NOT NULL,
            action TEXT NOT NULL CHECK(length(trim(action)) > 0),
            version_id TEXT,
            old_status TEXT,
            new_status TEXT,
            quality_score REAL CHECK(quality_score BETWEEN 0.0 AND 1.0),
            reason TEXT,
            source_run_id TEXT,
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            FOREIGN KEY(memory_id) REFERENCES memory_items(memory_id) ON DELETE CASCADE,
            FOREIGN KEY(version_id) REFERENCES memory_versions(version_id) ON DELETE SET NULL
        );

        CREATE TABLE jin10_items (
            id TEXT PRIMARY KEY CHECK(length(trim(id)) > 0),
            content TEXT NOT NULL CHECK(length(trim(content)) > 0),
            time_raw TEXT,
            item_time_ms INTEGER NOT NULL CHECK(item_time_ms >= 0),
            item_time INTEGER GENERATED ALWAYS AS (item_time_ms / 1000) VIRTUAL,
            latest_attention_score REAL NOT NULL CHECK(latest_attention_score BETWEEN 0.0 AND 1.0),
            attention_score REAL GENERATED ALWAYS AS (latest_attention_score) VIRTUAL,
            imported_at_ms INTEGER NOT NULL CHECK(imported_at_ms >= 0),
            imported_at INTEGER GENERATED ALWAYS AS (imported_at_ms / 1000) VIRTUAL,
            metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json)),
            legacy_attention INTEGER NOT NULL CHECK(legacy_attention IN (0, 1)),
            content_json TEXT GENERATED ALWAYS AS (
                json_object('id', id, 'time', item_time, 'time_raw', time_raw, 'content', content)
            ) VIRTUAL
        );

        CREATE TABLE technical_bars (
            ticker TEXT NOT NULL CHECK(length(trim(ticker)) > 0),
            interval TEXT NOT NULL CHECK(interval IN ('daily','3h','20min')),
            bar_time TEXT NOT NULL CHECK(length(trim(bar_time)) > 0),
            close REAL,
            values_json TEXT NOT NULL CHECK(json_valid(values_json)),
            imported_at_ms INTEGER NOT NULL CHECK(imported_at_ms >= 0),
            imported_at INTEGER GENERATED ALWAYS AS (imported_at_ms / 1000) VIRTUAL,
            PRIMARY KEY(ticker, interval, bar_time)
        ) WITHOUT ROWID;

        CREATE TABLE phase_summaries (
            id TEXT PRIMARY KEY CHECK(length(trim(id)) > 0),
            run_id TEXT NOT NULL CHECK(length(trim(run_id)) > 0),
            source_phase INTEGER NOT NULL,
            role TEXT NOT NULL CHECK(length(trim(role)) > 0),
            ticker TEXT NOT NULL CHECK(length(trim(ticker)) > 0),
            topic_id TEXT,
            summary TEXT NOT NULL CHECK(length(summary) <= 2048),
            summary_json TEXT NOT NULL CHECK(json_valid(summary_json)),
            payload_schema_version INTEGER NOT NULL CHECK(payload_schema_version > 0),
            payload_hash TEXT NOT NULL CHECK(length(payload_hash) = 64),
            confidence REAL NOT NULL CHECK(confidence BETWEEN 0.0 AND 1.0),
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE phase_summary_details (
            id TEXT PRIMARY KEY CHECK(length(trim(id)) > 0),
            summary_id TEXT NOT NULL,
            run_id TEXT NOT NULL CHECK(length(trim(run_id)) > 0),
            source_phase INTEGER NOT NULL,
            detail TEXT NOT NULL CHECK(length(detail) <= 2048),
            detail_json TEXT NOT NULL CHECK(json_valid(detail_json)),
            payload_schema_version INTEGER NOT NULL CHECK(payload_schema_version > 0),
            payload_hash TEXT NOT NULL CHECK(length(payload_hash) = 64),
            source_ref TEXT,
            sort_order INTEGER NOT NULL CHECK(sort_order >= 0),
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            FOREIGN KEY(summary_id) REFERENCES phase_summaries(id) ON DELETE CASCADE,
            FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE attention_ledger (
            id TEXT PRIMARY KEY CHECK(length(trim(id)) > 0),
            run_id TEXT NOT NULL CHECK(length(trim(run_id)) > 0),
            turn_id TEXT,
            role TEXT NOT NULL CHECK(length(trim(role)) > 0),
            subject_kind TEXT NOT NULL CHECK(length(trim(subject_kind)) > 0),
            subject_id TEXT NOT NULL CHECK(length(trim(subject_id)) > 0),
            score REAL NOT NULL CHECK(score BETWEEN 0.0 AND 1.0),
            phase INTEGER,
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );

        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL CHECK(length(trim(run_id)) > 0),
            ticker TEXT NOT NULL CHECK(length(trim(ticker)) > 0),
            prediction_date TEXT NOT NULL CHECK(length(prediction_date) = 10 AND strftime('%Y-%m-%d', prediction_date) = prediction_date),
            outcome_due_date TEXT NOT NULL CHECK(length(outcome_due_date) = 10 AND strftime('%Y-%m-%d', outcome_due_date) = outcome_due_date),
            long_probability REAL NOT NULL CHECK(long_probability BETWEEN 0.0 AND 1.0),
            short_probability REAL NOT NULL CHECK(short_probability BETWEEN 0.0 AND 1.0),
            rating TEXT,
            window_days INTEGER NOT NULL CHECK(window_days > 0),
            market_regime_json TEXT NOT NULL CHECK(json_valid(market_regime_json)),
            agent_probabilities_json TEXT NOT NULL CHECK(json_valid(agent_probabilities_json)),
            weighted_base_probability REAL CHECK(weighted_base_probability BETWEEN 0.0 AND 1.0),
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL,
            UNIQUE(run_id, ticker),
            FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE RESTRICT
        );

        CREATE TABLE outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER NOT NULL UNIQUE,
            run_id_snapshot TEXT NOT NULL,
            ticker_snapshot TEXT NOT NULL,
            prediction_date_snapshot TEXT NOT NULL,
            window_days_snapshot INTEGER NOT NULL CHECK(window_days_snapshot > 0),
            outcome_date TEXT NOT NULL CHECK(length(outcome_date) = 10 AND strftime('%Y-%m-%d', outcome_date) = outcome_date),
            baseline_close REAL NOT NULL,
            outcome_close REAL NOT NULL,
            actual_return REAL NOT NULL,
            direction_correct INTEGER NOT NULL CHECK(direction_correct IN (0, 1)),
            probability_error REAL NOT NULL CHECK(probability_error BETWEEN -1.0 AND 1.0),
            scored_at_ms INTEGER NOT NULL CHECK(scored_at_ms >= 0),
            scored_at INTEGER GENERATED ALWAYS AS (scored_at_ms / 1000) VIRTUAL,
            FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE CASCADE
        );

        CREATE TABLE candidate_experiences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope TEXT NOT NULL CHECK(scope IN ('ticker','aggregate','global')),
            scope_value TEXT NOT NULL,
            experience_type TEXT NOT NULL CHECK(length(trim(experience_type)) > 0),
            market_regime_json TEXT NOT NULL CHECK(json_valid(market_regime_json)),
            finding TEXT NOT NULL,
            recommendation TEXT NOT NULL,
            evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
            counter_evidence_json TEXT NOT NULL CHECK(json_valid(counter_evidence_json)),
            metrics_json TEXT NOT NULL CHECK(json_valid(metrics_json)),
            sample_count INTEGER NOT NULL CHECK(sample_count >= 0),
            sample_run_ids_json TEXT NOT NULL CHECK(json_valid(sample_run_ids_json)),
            confidence REAL NOT NULL CHECK(confidence BETWEEN 0.0 AND 1.0),
            effect_size REAL NOT NULL,
            distiller_version TEXT NOT NULL,
            reflection_version TEXT NOT NULL,
            source_window TEXT,
            review_status TEXT NOT NULL CHECK(review_status IN ('pending','pending_human','promoted','rejected')),
            reviewed_at_ms INTEGER,
            reviewed_at INTEGER GENERATED ALWAYS AS (reviewed_at_ms / 1000) VIRTUAL,
            review_reason TEXT,
            created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
            created_at INTEGER GENERATED ALWAYS AS (created_at_ms / 1000) VIRTUAL
        );

        CREATE TABLE schema_archive (
            object_name TEXT PRIMARY KEY,
            object_type TEXT NOT NULL,
            archived_at_ms INTEGER NOT NULL,
            note TEXT NOT NULL
        );
        "#,
    )?;
    Ok(())
}

fn create_indexes(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_runs_latest
            ON runs("current_date" DESC, created_at_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_agent_events_run_turn
            ON agent_events(run_id, turn_number DESC);
        CREATE INDEX IF NOT EXISTS idx_agent_events_run_role_turn
            ON agent_events(run_id, role, turn_number DESC);
        CREATE INDEX IF NOT EXISTS idx_role_summaries_run_ticker_created
            ON role_turn_summaries(run_id, ticker, created_at_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_role_summaries_run_phase_type
            ON role_turn_summaries(run_id, phase, summary_type, role, id);
        CREATE INDEX IF NOT EXISTS idx_role_summaries_turn
            ON role_turn_summaries(turn_id);
        CREATE INDEX IF NOT EXISTS idx_role_summaries_payload_hash
            ON role_turn_summaries(payload_hash);
        CREATE INDEX IF NOT EXISTS idx_memory_items_lookup
            ON memory_items(ticker, status, memory_type, updated_at_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_memory_items_quality_time
            ON memory_items(status, quality_score, updated_at_ms DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_versions_hash
            ON memory_versions(content_hash);
        CREATE INDEX IF NOT EXISTS idx_memory_history_memory
            ON memory_history(memory_id, created_at_ms);
        CREATE INDEX IF NOT EXISTS idx_jin10_attention_time
            ON jin10_items(latest_attention_score DESC, item_time_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_phase_summaries_run_phase
            ON phase_summaries(run_id, source_phase);
        CREATE INDEX IF NOT EXISTS idx_phase_summaries_run_ticker_phase
            ON phase_summaries(run_id, ticker, source_phase);
        CREATE INDEX IF NOT EXISTS idx_phase_details_run_phase_order
            ON phase_summary_details(run_id, source_phase, summary_id, sort_order);
        CREATE INDEX IF NOT EXISTS idx_attention_ledger_run_turn
            ON attention_ledger(run_id, turn_id);
        CREATE INDEX IF NOT EXISTS idx_attention_ledger_subject
            ON attention_ledger(run_id, subject_kind, subject_id);
        CREATE INDEX IF NOT EXISTS idx_attention_run_score_time
            ON attention_ledger(run_id, score DESC, created_at_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_predictions_ticker_date_run
            ON predictions(ticker, prediction_date, run_id);
        CREATE INDEX IF NOT EXISTS idx_predictions_due_unscored
            ON predictions(outcome_due_date, id);
        CREATE INDEX IF NOT EXISTS idx_outcomes_ticker_date
            ON outcomes(ticker_snapshot, prediction_date_snapshot);
        CREATE INDEX IF NOT EXISTS idx_outcomes_ticker_outcome_date
            ON outcomes(ticker_snapshot, outcome_date);
        CREATE INDEX IF NOT EXISTS idx_candidate_status_id
            ON candidate_experiences(review_status, id);
        CREATE INDEX IF NOT EXISTS idx_candidate_exp_scope_type
            ON candidate_experiences(scope, scope_value, experience_type);
        "#,
    )?;
    Ok(())
}

fn copy_legacy_data(tx: &Transaction<'_>) -> Result<()> {
    copy_runs(tx)?;
    create_missing_legacy_runs(tx)?;
    copy_agent_events(tx)?;
    copy_role_summaries(tx)?;
    copy_candidates(tx)?;
    copy_memory(tx)?;
    copy_jin10(tx)?;
    copy_technical(tx)?;
    copy_phase_index(tx)?;
    copy_attention(tx)?;
    copy_predictions(tx)?;
    copy_outcomes(tx)?;
    archive_unmanaged_legacy_tables(tx)?;
    Ok(())
}

fn copy_runs(conn: &Connection) -> Result<()> {
    let table = legacy_name("runs");
    if !table_exists(conn, &table)? {
        return Ok(());
    }
    let columns = columns(conn, &table)?;
    let run_id = required_text(&columns, "run_id", "'__LEGACY_ORPHAN__'");
    let current_date = required_text(&columns, "current_date", "'1970-01-01'");
    let status = if columns.contains("status") {
        "CASE WHEN status IN ('pending','running','completed','failed') THEN status ELSE 'pending' END"
    } else {
        "'pending'"
    };
    let sql = format!(
        r#"INSERT INTO runs
        (run_id,current_date,created_at_ms,status,current_phase,error_message,completed_at_ms,
         run_dir,db_path,git_sha,config_hash,artifact_path,workflow_version,prompt_versions_json,
         degraded,phase_count,total_elapsed_ms)
        SELECT {run_id},{current_date},{created},{status},{phase},{error},{completed},
               {run_dir},{db_path},{git_sha},{config_hash},{artifact},{workflow},{prompts},
               {degraded},{phase_count},{elapsed}
        FROM {table}"#,
        created = millis_expr(&columns, "created_at", "0"),
        phase = nullable_col(&columns, "current_phase", "NULL"),
        error = nullable_col(&columns, "error_message", "NULL"),
        completed = millis_expr(&columns, "completed_at", "NULL"),
        run_dir = nullable_col(&columns, "run_dir", "NULL"),
        db_path = nullable_col(&columns, "db_path", "NULL"),
        git_sha = nullable_col(&columns, "git_sha", "NULL"),
        config_hash = nullable_col(&columns, "config_hash", "NULL"),
        artifact = nullable_col(&columns, "artifact_path", "NULL"),
        workflow = nullable_col(&columns, "workflow_version", "NULL"),
        prompts = json_col(&columns, "prompt_versions_json", "'{}'"),
        degraded = bool_expr(&columns, "degraded", "0"),
        phase_count = nonnegative_expr(&columns, "phase_count", "0"),
        elapsed = nonnegative_expr(&columns, "total_elapsed_ms", "0"),
        table = quote_ident(&table),
    );
    conn.execute_batch(&sql)?;
    Ok(())
}

fn create_missing_legacy_runs(conn: &Connection) -> Result<()> {
    for table in MANAGED_TABLES {
        if *table == "runs"
            || *table == "outcomes"
            || *table == "technical_series"
            || *table == "technical_bars"
            || *table == "jin10_items"
            || *table == "candidate_experiences"
            || *table == "memory_items"
            || *table == "memory_versions"
            || *table == "memory_history"
        {
            continue;
        }
        let legacy = legacy_name(table);
        if !table_exists(conn, &legacy)? || !columns(conn, &legacy)?.contains("run_id") {
            continue;
        }
        conn.execute_batch(&format!(
            r#"INSERT OR IGNORE INTO runs
               (run_id,current_date,created_at_ms,status,current_phase,error_message,completed_at_ms,
                run_dir,db_path,git_sha,config_hash,artifact_path,workflow_version,prompt_versions_json,
                degraded,phase_count,total_elapsed_ms)
               SELECT DISTINCT CASE WHEN trim(run_id) = '' THEN '__LEGACY_ORPHAN__' ELSE run_id END,
                      '1970-01-01',0,'pending',NULL,'legacy orphan',NULL,
                      NULL,NULL,NULL,NULL,NULL,NULL,'{{}}',0,0,0
               FROM {}"#,
            quote_ident(&legacy)
        ))?;
    }
    Ok(())
}

fn copy_agent_events(conn: &Connection) -> Result<()> {
    let table = legacy_name("agent_events");
    if !table_exists(conn, &table)? {
        return Ok(());
    }
    let cols = columns(conn, &table)?;
    let context_col = if cols.contains("full_context_json") {
        "full_context_json"
    } else if cols.contains("content_json") {
        "content_json"
    } else {
        "'[]'"
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} FROM {} ORDER BY {}",
        required_text(&cols, "turn_id", "printf('legacy-turn-%d', rowid)"),
        run_id_expr(&cols),
        nullable_col(&cols, "phase", "NULL"),
        nonnegative_expr(&cols, "turn_number", "0"),
        required_text(&cols, "role", "'legacy.unknown'"),
        millis_expr(&cols, "created_at", "0"),
        context_col,
        required_text(&cols, "summary", "''"),
        nullable_col(&cols, "model", "NULL"),
        nonnegative_expr(&cols, "input_tokens", "0"),
        nonnegative_expr(&cols, "output_tokens", "0"),
        nonnegative_expr(&cols, "cached_tokens", "0"),
        nonnegative_expr(&cols, "reasoning_tokens", "0"),
        nonnegative_expr(&cols, "total_tokens", "0"),
        nonnegative_expr(&cols, "non_cached_input_tokens", "0"),
        nonnegative_expr(&cols, "visible_output_tokens", "0"),
        nonnegative_expr(&cols, "cost_usd", "0.0"),
        bool_expr(&cols, "context_warning", "0"),
        nonnegative_expr(&cols, "elapsed_ms", "0"),
        quote_ident(&table),
        if cols.contains("id") { "id" } else { "rowid" },
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<i64>>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, i64>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, i64>(11)?,
            row.get::<_, i64>(12)?,
            row.get::<_, i64>(13)?,
            row.get::<_, i64>(14)?,
            row.get::<_, i64>(15)?,
            row.get::<_, f64>(16)?,
            row.get::<_, i64>(17)?,
            row.get::<_, i64>(18)?,
        ))
    })?;
    let mut previous_by_role: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();
    for row in rows {
        let (
            turn_id,
            run_id,
            phase,
            turn_number,
            role,
            created_at_ms,
            context,
            summary,
            model,
            input_tokens,
            output_tokens,
            cached_tokens,
            reasoning_tokens,
            total_tokens,
            non_cached_input_tokens,
            visible_output_tokens,
            cost_usd,
            context_warning,
            elapsed_ms,
        ) = row?;
        let parsed: Value = serde_json::from_str(&context)
            .with_context(|| format!("invalid legacy agent context for turn {turn_id}"))?;
        let messages = parsed
            .as_array()
            .cloned()
            .context("legacy agent context must be a JSON array")?;
        let normalized_run_id = normalized_run_id(&run_id).to_string();
        let normalized_role = nonempty_or(&role, "legacy.unknown").to_string();
        let key = (normalized_run_id.clone(), normalized_role.clone());
        let previous = previous_by_role.get(&key).cloned().unwrap_or_default();
        let can_delta = !previous.is_empty()
            && messages.len() >= previous.len()
            && messages[..previous.len()] == previous;
        let checkpoint = !can_delta || turn_number % 10 == 0;
        let checkpoint_json = checkpoint.then_some(context.clone());
        let delta_json = if checkpoint {
            "[]".to_string()
        } else {
            serde_json::to_string(&messages[previous.len()..])?
        };
        conn.execute(
            r#"INSERT INTO agent_events
               (turn_id,run_id,phase,turn_number,role,created_at_ms,full_context_json,
                context_delta_json,context_hash,summary,model,input_tokens,output_tokens,
                cached_tokens,reasoning_tokens,total_tokens,non_cached_input_tokens,
                visible_output_tokens,cost_usd,context_warning,elapsed_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"#,
            params![
                turn_id,
                normalized_run_id,
                phase,
                turn_number,
                normalized_role,
                created_at_ms,
                checkpoint_json,
                delta_json,
                sha256_hex(&context),
                truncate_chars(&summary, 2048),
                model,
                input_tokens,
                output_tokens,
                cached_tokens,
                reasoning_tokens,
                total_tokens,
                non_cached_input_tokens,
                visible_output_tokens,
                cost_usd,
                context_warning,
                elapsed_ms,
            ],
        )?;
        previous_by_role.insert(key, messages);
    }
    Ok(())
}

fn copy_role_summaries(conn: &Connection) -> Result<()> {
    let table = legacy_name("role_turn_summaries");
    if !table_exists(conn, &table)? {
        return Ok(());
    }
    let cols = columns(conn, &table)?;
    let sql = format!(
        r#"INSERT INTO role_turn_summaries
           (id,run_id,turn_id,phase,role,ticker,item_time_ms,topic_id,debate_id,summary_type,
            summary,summary_json,payload_schema_version,payload_hash,confidence,created_at_ms)
           SELECT {id},{run_id},{turn_id},{phase},{role},{ticker},{item_time},{topic},{debate},{kind},
                  substr({summary},1,2048),{payload},1,printf('%064d',0),{confidence},{created}
           FROM {table}"#,
        id = nullable_col(&cols, "id", "NULL"),
        run_id = run_id_expr(&cols),
        turn_id = required_text(&cols, "turn_id", "printf('legacy-summary-%d', rowid)"),
        phase = nullable_col(&cols, "phase", "NULL"),
        role = required_text(&cols, "role", "'legacy.unknown'"),
        ticker = required_text(&cols, "ticker", "'__ALL__'"),
        item_time = millis_expr(&cols, "item_time", &millis_expr(&cols, "created_at", "0")),
        topic = nullable_col(&cols, "topic_id", "NULL"),
        debate = nullable_col(&cols, "debate_id", "NULL"),
        kind = required_text(&cols, "summary_type", "'legacy'"),
        summary = nullable_col(&cols, "summary", "''"),
        payload = json_col(&cols, "summary_json", "'{}'"),
        confidence = probability_expr(&cols, "confidence", "0.0"),
        created = millis_expr(&cols, "created_at", "0"),
        table = quote_ident(&table),
    );
    conn.execute_batch(&sql)?;
    backfill_payload_hash(conn, "role_turn_summaries", "id", "summary_json")
}

fn copy_candidates(conn: &Connection) -> Result<()> {
    let table = legacy_name("candidate_experiences");
    if !table_exists(conn, &table)? {
        return Ok(());
    }
    let c = columns(conn, &table)?;
    conn.execute_batch(&format!(
        r#"INSERT INTO candidate_experiences
        (id,scope,scope_value,experience_type,market_regime_json,finding,recommendation,
         evidence_json,counter_evidence_json,metrics_json,sample_count,sample_run_ids_json,
         confidence,effect_size,distiller_version,reflection_version,source_window,
         review_status,reviewed_at_ms,review_reason,created_at_ms)
        SELECT {id},{scope},{scope_value},{kind},{regime},{finding},{recommendation},
               {evidence},{counter},{metrics},{samples},{run_ids},{confidence},{effect},
               {distiller},{reflection},{window},{status},{reviewed},{reason},{created}
        FROM {table}"#,
        id = nullable_col(&c, "id", "NULL"),
        scope = enum_expr(&c, "scope", "('ticker','aggregate','global')", "'global'"),
        scope_value = nullable_col(&c, "scope_value", "''"),
        kind = required_text(&c, "experience_type", "'legacy'"),
        regime = json_col(&c, "market_regime_json", "'{}'"),
        finding = nullable_col(&c, "finding", "''"),
        recommendation = nullable_col(&c, "recommendation", "''"),
        evidence = json_col(&c, "evidence_json", "'[]'"),
        counter = json_col(&c, "counter_evidence_json", "'[]'"),
        metrics = json_col(&c, "metrics_json", "'{}'"),
        samples = nonnegative_expr(&c, "sample_count", "0"),
        run_ids = json_col(&c, "sample_run_ids_json", "'[]'"),
        confidence = probability_expr(&c, "confidence", "0.0"),
        effect = nullable_col(&c, "effect_size", "0.0"),
        distiller = required_text(&c, "distiller_version", "'v1'"),
        reflection = required_text(&c, "reflection_version", "'v1'"),
        window = nullable_col(&c, "source_window", "NULL"),
        status = enum_expr(
            &c,
            "review_status",
            "('pending','pending_human','promoted','rejected')",
            "'pending'"
        ),
        reviewed = millis_expr(&c, "reviewed_at", "NULL"),
        reason = nullable_col(&c, "review_reason", "NULL"),
        created = millis_expr(&c, "created_at", "0"),
        table = quote_ident(&table),
    ))?;
    Ok(())
}

fn copy_memory(conn: &Connection) -> Result<()> {
    let items = legacy_name("memory_items");
    if table_exists(conn, &items)? {
        let c = columns(conn, &items)?;
        conn.execute_batch(&format!(
            r#"INSERT INTO memory_items
            (memory_id,ticker,scope,memory_type,status,current_version_id,confidence,expires_at_ms,
             created_at_ms,updated_at_ms,market_regime_json,quality_score,sample_count,
             recent_success_rate,reflection_version,promoted_from)
            SELECT {id},{ticker},{scope},{kind},{status},{current},{confidence},{expires},
                   {created},{updated},{regime},{quality},{samples},{success},{reflection},{promoted}
            FROM {table}"#,
            id = required_text(&c, "memory_id", "printf('legacy-memory-%d', rowid)"),
            ticker = required_text(&c, "ticker", "'__ALL__'"),
            scope = enum_expr(&c, "scope", "('ticker','aggregate','global')", "'global'"),
            kind = required_text(&c, "memory_type", "'legacy'"),
            status = enum_expr(&c, "status", "('active','inactive','archived')", "'active'"),
            current = if c.contains("current_version_id") {
                "NULLIF(current_version_id,'')".to_string()
            } else {
                "NULL".to_string()
            },
            confidence = probability_expr(&c, "confidence", "0.0"),
            expires = millis_expr(&c, "expires_at", "NULL"),
            created = millis_expr(&c, "created_at", "0"),
            updated = millis_expr(&c, "updated_at", "0"),
            regime = json_col(&c, "market_regime_json", "'{}'"),
            quality = probability_expr(&c, "quality_score", "0.0"),
            samples = nonnegative_expr(&c, "sample_count", "0"),
            success = probability_expr(&c, "recent_success_rate", "0.0"),
            reflection = required_text(&c, "reflection_version", "'v1'"),
            promoted = nullable_col(&c, "promoted_from", "NULL"),
            table = quote_ident(&items),
        ))?;
    }

    let versions = legacy_name("memory_versions");
    if table_exists(conn, &versions)? {
        let c = columns(conn, &versions)?;
        conn.execute_batch(&format!(
            r#"INSERT INTO memory_versions
            (version_id,memory_id,version_index,summary,body_json,evidence_refs_json,
             payload_schema_version,payload_hash,source_run_id,source_role,source_date,
             observed_at_ms,content_hash,created_at_ms)
            SELECT {id},{memory},{idx},substr({summary},1,2048),{body},{evidence},1,
                   printf('%064d',0),{run},{role},{date},{observed},{hash},{created}
            FROM {table}"#,
            id = required_text(&c, "version_id", "printf('legacy-version-%d', rowid)"),
            memory = required_text(&c, "memory_id", "''"),
            idx = nonnegative_expr(&c, "version_index", "1"),
            summary = nullable_col(&c, "summary", "''"),
            body = json_col(&c, "body_json", "'{}'"),
            evidence = json_col(&c, "evidence_refs_json", "'[]'"),
            run = nullable_col(&c, "source_run_id", "NULL"),
            role = nullable_col(&c, "source_role", "NULL"),
            date = nullable_col(&c, "source_date", "NULL"),
            observed = millis_expr(&c, "observed_at", "NULL"),
            hash = required_text(&c, "content_hash", "printf('%064d',0)"),
            created = millis_expr(&c, "created_at", "0"),
            table = quote_ident(&versions),
        ))?;
        backfill_payload_hash(conn, "memory_versions", "version_id", "body_json")?;
    }

    let history = legacy_name("memory_history");
    if table_exists(conn, &history)? {
        let c = columns(conn, &history)?;
        conn.execute_batch(&format!(
            r#"INSERT INTO memory_history
            (id,memory_id,action,version_id,old_status,new_status,quality_score,reason,
             source_run_id,created_at_ms)
            SELECT {id},{memory},{action},NULLIF({version},''),NULLIF({old},''),NULLIF({new},''),
                   {quality},NULLIF({reason},''),NULLIF({run},''),{created}
            FROM {table}"#,
            id = nullable_col(&c, "id", "NULL"),
            memory = required_text(&c, "memory_id", "''"),
            action = required_text(&c, "action", "'legacy'"),
            version = nullable_col(&c, "version_id", "''"),
            old = nullable_col(&c, "old_status", "''"),
            new = nullable_col(&c, "new_status", "''"),
            quality = probability_expr(&c, "quality_score", "NULL"),
            reason = nullable_col(&c, "reason", "''"),
            run = nullable_col(&c, "source_run_id", "''"),
            created = millis_expr(&c, "created_at", "0"),
            table = quote_ident(&history),
        ))?;
    }
    Ok(())
}

fn copy_jin10(conn: &Connection) -> Result<()> {
    let table = legacy_name("jin10_items");
    if !table_exists(conn, &table)? {
        return Ok(());
    }
    let cols = columns(conn, &table)?;
    let content_col = if cols.contains("content_json") {
        "content_json"
    } else {
        "'{}'"
    };
    let score_col = if cols.contains("attention_score") {
        "attention_score"
    } else if cols.contains("llm_usage_count") {
        "CASE WHEN llm_usage_count <= 0 THEN 0.0 ELSE MIN(1.0, 1.0 - 1.0 / (1.0 + llm_usage_count)) END"
    } else {
        "0.0"
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT id,{content_col},{score_col},{item_time},{imported_at} FROM {}",
        quote_ident(&table),
        item_time = millis_expr(&cols, "item_time", "0"),
        imported_at = millis_expr(&cols, "imported_at", "0"),
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;
    for row in rows {
        let (id, raw, score, item_time_ms, imported_at_ms) = row?;
        let payload: Value = serde_json::from_str(&raw)
            .with_context(|| format!("invalid legacy Jin10 content_json for {id}"))?;
        let content = payload
            .get("content")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .context("legacy Jin10 item is missing content")?;
        let time_raw = payload.get("time_raw").and_then(Value::as_str);
        let mut metadata = payload.as_object().cloned().unwrap_or_default();
        for key in ["id", "time", "time_raw", "content"] {
            metadata.remove(key);
        }
        conn.execute(
            r#"INSERT INTO jin10_items
               (id,content,time_raw,item_time_ms,latest_attention_score,imported_at_ms,
                metadata_json,legacy_attention)
               VALUES (?,?,?,?,?,?,?,?)"#,
            params![
                id,
                content,
                time_raw,
                item_time_ms,
                score.clamp(0.0, 1.0),
                imported_at_ms,
                serde_json::to_string(&metadata)?,
                i64::from(score > 0.0),
            ],
        )?;
    }
    Ok(())
}

fn copy_technical(conn: &Connection) -> Result<()> {
    let bars = legacy_name("technical_bars");
    if table_exists(conn, &bars)? {
        let c = columns(conn, &bars)?;
        conn.execute_batch(&format!(
            r#"INSERT INTO technical_bars(ticker,interval,bar_time,close,values_json,imported_at_ms)
               SELECT ticker,interval,bar_time,close,values_json,{} FROM {}"#,
            millis_expr(&c, "imported_at", "0"),
            quote_ident(&bars)
        ))?;
    }
    let series = legacy_name("technical_series");
    if !table_exists(conn, &series)? {
        return Ok(());
    }
    let c = columns(conn, &series)?;
    let mut stmt = conn.prepare(&format!(
        "SELECT ticker,interval,row_count,rows_json,{} FROM {}",
        millis_expr(&c, "imported_at", "0"),
        quote_ident(&series)
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
        ))
    })?;
    for row in rows {
        let (ticker, interval, expected, raw, imported_at_ms) = row?;
        let payload: Vec<Value> = serde_json::from_str(&raw).with_context(|| {
            format!("invalid technical_series rows_json for {ticker}/{interval}")
        })?;
        if payload.len() as i64 != expected {
            bail!(
                "technical_series row_count mismatch for {ticker}/{interval}: expected {expected}, decoded {}",
                payload.len()
            );
        }
        for bar in payload {
            let bar_time = bar
                .get("date")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .context("legacy technical bar is missing date")?;
            let mut values = bar
                .get("values")
                .and_then(Value::as_object)
                .cloned()
                .context("legacy technical bar is missing values")?;
            let close = values
                .remove("Close")
                .or_else(|| values.remove("close"))
                .and_then(|value| value.as_f64());
            conn.execute(
                r#"INSERT INTO technical_bars
                   (ticker,interval,bar_time,close,values_json,imported_at_ms)
                   VALUES (?,?,?,?,?,?)
                   ON CONFLICT(ticker,interval,bar_time) DO UPDATE SET
                     close=excluded.close, values_json=excluded.values_json,
                     imported_at_ms=excluded.imported_at_ms"#,
                params![
                    ticker,
                    interval,
                    bar_time,
                    close,
                    serde_json::to_string(&values)?,
                    imported_at_ms,
                ],
            )?;
        }
    }
    Ok(())
}

fn copy_phase_index(conn: &Connection) -> Result<()> {
    let summaries = legacy_name("phase_summaries");
    if table_exists(conn, &summaries)? {
        let c = columns(conn, &summaries)?;
        conn.execute_batch(&format!(
            r#"INSERT INTO phase_summaries
            (id,run_id,source_phase,role,ticker,topic_id,summary,summary_json,
             payload_schema_version,payload_hash,confidence,created_at_ms)
            SELECT {id},{run},{phase},{role},{ticker},{topic},substr({summary},1,2048),{payload},
                   1,printf('%064d',0),{confidence},{created} FROM {table}"#,
            id = required_text(&c, "id", "printf('legacy-phase-%d', rowid)"),
            run = run_id_expr(&c),
            phase = nullable_col(&c, "source_phase", "0"),
            role = required_text(&c, "role", "'compressor'"),
            ticker = required_text(&c, "ticker", "'__ALL__'"),
            topic = nullable_col(&c, "topic_id", "NULL"),
            summary = nullable_col(&c, "summary", "''"),
            payload = json_col(&c, "summary_json", "'{}'"),
            confidence = probability_expr(&c, "confidence", "0.0"),
            created = millis_expr(&c, "created_at", "0"),
            table = quote_ident(&summaries),
        ))?;
        backfill_payload_hash(conn, "phase_summaries", "id", "summary_json")?;
    }
    let details = legacy_name("phase_summary_details");
    if table_exists(conn, &details)? {
        let c = columns(conn, &details)?;
        conn.execute_batch(&format!(
            r#"INSERT INTO phase_summary_details
            (id,summary_id,run_id,source_phase,detail,detail_json,payload_schema_version,
             payload_hash,source_ref,sort_order,created_at_ms)
            SELECT {id},{summary_id},{run},{phase},substr({detail},1,2048),{payload},1,
                   printf('%064d',0),{source_ref},{sort_order},{created} FROM {table}"#,
            id = required_text(&c, "id", "printf('legacy-detail-%d', rowid)"),
            summary_id = required_text(&c, "summary_id", "''"),
            run = run_id_expr(&c),
            phase = nullable_col(&c, "source_phase", "0"),
            detail = nullable_col(&c, "detail", "''"),
            payload = json_col(&c, "detail_json", "'{}'"),
            source_ref = nullable_col(&c, "source_ref", "NULL"),
            sort_order = nonnegative_expr(&c, "sort_order", "0"),
            created = millis_expr(&c, "created_at", "0"),
            table = quote_ident(&details),
        ))?;
        backfill_payload_hash(conn, "phase_summary_details", "id", "detail_json")?;
    }
    Ok(())
}

fn copy_attention(conn: &Connection) -> Result<()> {
    let table = legacy_name("attention_ledger");
    if !table_exists(conn, &table)? {
        return Ok(());
    }
    let c = columns(conn, &table)?;
    conn.execute_batch(&format!(
        r#"INSERT INTO attention_ledger
           (id,run_id,turn_id,role,subject_kind,subject_id,score,phase,created_at_ms)
           SELECT {id},{run},NULLIF({turn},''),{role},{kind},{subject},{score},{phase},{created}
           FROM {table}"#,
        id = required_text(&c, "id", "printf('legacy-attention-%d', rowid)"),
        run = run_id_expr(&c),
        turn = nullable_col(&c, "turn_id", "''"),
        role = required_text(&c, "role", "'legacy.unknown'"),
        kind = required_text(&c, "subject_kind", "'legacy'"),
        subject = required_text(&c, "subject_id", "printf('legacy-%d', rowid)"),
        score = probability_expr(&c, "score", "0.0"),
        phase = nullable_col(&c, "phase", "NULL"),
        created = millis_expr(&c, "created_at", "0"),
        table = quote_ident(&table),
    ))?;
    Ok(())
}

fn copy_predictions(conn: &Connection) -> Result<()> {
    let table = legacy_name("predictions");
    if !table_exists(conn, &table)? {
        return Ok(());
    }
    let c = columns(conn, &table)?;
    let prediction_date = required_text(&c, "prediction_date", "'1970-01-01'");
    let window = positive_expr(&c, "window_days", "5");
    conn.execute_batch(&format!(
        r#"INSERT INTO predictions
        (id,run_id,ticker,prediction_date,outcome_due_date,long_probability,short_probability,
         rating,window_days,market_regime_json,agent_probabilities_json,
         weighted_base_probability,created_at_ms)
        SELECT {id},{run},{ticker},{date},date({date},'+' || {window} || ' days'),
               {long},{short},{rating},{window},{regime},{agents},{weighted},{created}
        FROM {table}"#,
        id = nullable_col(&c, "id", "NULL"),
        run = run_id_expr(&c),
        ticker = required_text(&c, "ticker", "'__ALL__'"),
        date = prediction_date,
        long = probability_expr(&c, "long_probability", "0.5"),
        short = probability_expr(&c, "short_probability", "0.5"),
        rating = nullable_col(&c, "rating", "NULL"),
        regime = json_col(&c, "market_regime_json", "'{}'"),
        agents = json_col(&c, "agent_probabilities_json", "'{}'"),
        weighted = probability_expr(&c, "weighted_base_probability", "NULL"),
        created = millis_expr(&c, "created_at", "0"),
        table = quote_ident(&table),
    ))?;
    Ok(())
}

fn copy_outcomes(conn: &Connection) -> Result<()> {
    let table = legacy_name("outcomes");
    if !table_exists(conn, &table)? {
        return Ok(());
    }
    let c = columns(conn, &table)?;
    conn.execute_batch(&format!(
        r#"INSERT INTO outcomes
        (id,prediction_id,run_id_snapshot,ticker_snapshot,prediction_date_snapshot,
         window_days_snapshot,outcome_date,baseline_close,outcome_close,actual_return,
         direction_correct,probability_error,scored_at_ms)
        SELECT {id},{prediction},{run},{ticker},{date},{window},{outcome},{baseline},{close},
               {actual},{correct},{error},{scored} FROM {table}"#,
        id = nullable_col(&c, "id", "NULL"),
        prediction = nullable_col(&c, "prediction_id", "0"),
        run = required_text(&c, "run_id", "'__LEGACY_ORPHAN__'"),
        ticker = required_text(&c, "ticker", "'__ALL__'"),
        date = required_text(&c, "prediction_date", "'1970-01-01'"),
        window = positive_expr(&c, "window_days", "1"),
        outcome = required_text(&c, "outcome_date", "'1970-01-01'"),
        baseline = nullable_col(&c, "baseline_close", "0.0"),
        close = nullable_col(&c, "outcome_close", "0.0"),
        actual = nullable_col(&c, "actual_return", "0.0"),
        correct = bool_expr(&c, "direction_correct", "0"),
        error = signed_probability_expr(&c, "probability_error", "0.0"),
        scored = millis_expr(&c, "scored_at", "0"),
        table = quote_ident(&table),
    ))?;
    Ok(())
}

fn validate_migrated_data(conn: &Connection) -> Result<()> {
    for table in MANAGED_TABLES {
        let legacy = legacy_name(table);
        if !table_exists(conn, &legacy)?
            || *table == "technical_series"
            || *table == "technical_bars"
        {
            continue;
        }
        let old_count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM {}", quote_ident(&legacy)),
            [],
            |row| row.get(0),
        )?;
        let new_count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM {}", quote_ident(table)),
            [],
            |row| row.get(0),
        )?;
        if old_count != new_count {
            bail!("migration row-count mismatch for {table}: old={old_count}, new={new_count}");
        }
    }
    let violations: i64 =
        conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if violations != 0 {
        bail!("migration has {violations} foreign-key violations");
    }
    let invalid_memory_versions: i64 = conn.query_row(
        r#"SELECT COUNT(*) FROM memory_items i
           WHERE current_version_id IS NOT NULL
             AND NOT EXISTS (SELECT 1 FROM memory_versions v
                             WHERE v.version_id=i.current_version_id AND v.memory_id=i.memory_id)"#,
        [],
        |row| row.get(0),
    )?;
    if invalid_memory_versions != 0 {
        bail!("migration has {invalid_memory_versions} invalid current memory versions");
    }
    Ok(())
}

fn archive_unmanaged_legacy_tables(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "schema_archive")? {
        return Ok(());
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    for table in ARCHIVED_LEGACY_TABLES {
        if table_exists(conn, table)? {
            conn.execute(
                r#"INSERT INTO schema_archive(object_name,object_type,archived_at_ms,note)
                   VALUES (?1,'table',?2,'Legacy table retained in place; not used by current workflow')
                   ON CONFLICT(object_name) DO NOTHING"#,
                params![table, now_ms],
            )?;
        }
    }
    Ok(())
}

fn drop_legacy_indexes(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name LIKE '__legacy_%' AND sql IS NOT NULL",
    )?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for name in names {
        conn.execute_batch(&format!("DROP INDEX {}", quote_ident(&name)))?;
    }
    Ok(())
}

fn backfill_payload_hash(
    conn: &Connection,
    table: &str,
    id_column: &str,
    payload_column: &str,
) -> Result<()> {
    let mut stmt = conn.prepare(&format!(
        "SELECT CAST({} AS TEXT), {} FROM {}",
        quote_ident(id_column),
        quote_ident(payload_column),
        quote_ident(table)
    ))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, payload) in rows {
        let parsed: Value = serde_json::from_str(&payload)
            .with_context(|| format!("invalid payload in {table}.{payload_column} for {id}"))?;
        let canonical = canonical_json(&parsed)?;
        conn.execute(
            &format!(
                "UPDATE {} SET payload_hash=?1 WHERE {}=?2",
                quote_ident(table),
                quote_ident(id_column)
            ),
            params![sha256_hex(&canonical), id],
        )?;
    }
    Ok(())
}

pub(crate) fn canonical_json(value: &Value) -> Result<String> {
    fn normalize(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let ordered = map
                    .iter()
                    .map(|(key, value)| (key.clone(), normalize(value)))
                    .collect::<std::collections::BTreeMap<_, _>>();
                Value::Object(ordered.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(normalize).collect()),
            other => other.clone(),
        }
    }
    Ok(serde_json::to_string(&normalize(value))?)
}

pub(crate) fn payload_hash(value: &Value) -> Result<String> {
    Ok(sha256_hex(&canonical_json(value)?))
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(crate) fn ensure_run_exists(conn: &Connection, run_id: &str, current_date: &str) -> Result<()> {
    if run_id.trim().is_empty() {
        bail!("run_id cannot be empty");
    }
    conn.execute(
        r#"INSERT INTO runs
           (run_id,current_date,created_at_ms,status,current_phase,error_message,completed_at_ms,
            run_dir,db_path,git_sha,config_hash,artifact_path,workflow_version,
            prompt_versions_json,degraded,phase_count,total_elapsed_ms)
           VALUES (?1,?2,?3,'pending',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'{}',0,0,0)
           ON CONFLICT(run_id) DO NOTHING"#,
        params![run_id, current_date, now_ms()],
    )?;
    Ok(())
}

fn set_user_version(conn: &Connection, version: i64) -> Result<()> {
    conn.pragma_update(None, "user_version", version)?;
    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            [table],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn columns(conn: &Connection, table: &str) -> Result<BTreeSet<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_xinfo({})", quote_ident(table)))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(columns)
}

fn legacy_name(table: &str) -> String {
    format!("__legacy_{table}")
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn nullable_col(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        quote_ident(name)
    } else {
        fallback.to_string()
    }
}

fn required_text(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!(
            "COALESCE(NULLIF(trim({}),''), {fallback})",
            quote_ident(name)
        )
    } else {
        fallback.to_string()
    }
}

fn json_col(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!("COALESCE({}, {fallback})", quote_ident(name))
    } else {
        fallback.to_string()
    }
}

fn run_id_expr(columns: &BTreeSet<String>) -> String {
    if columns.contains("run_id") {
        "CASE WHEN trim(run_id)='' THEN '__LEGACY_ORPHAN__' ELSE run_id END".to_string()
    } else {
        "'__LEGACY_ORPHAN__'".to_string()
    }
}

fn millis_expr(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(&format!("{name}_ms")) {
        quote_ident(&format!("{name}_ms"))
    } else if columns.contains(name) {
        format!(
            "CASE WHEN {0} IS NULL THEN NULL WHEN abs({0}) < 100000000000 THEN {0} * 1000 ELSE {0} END",
            quote_ident(name)
        )
    } else {
        fallback.to_string()
    }
}

fn nonnegative_expr(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!("MAX(0, COALESCE({}, {fallback}))", quote_ident(name))
    } else {
        fallback.to_string()
    }
}

fn positive_expr(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!(
            "CASE WHEN {} > 0 THEN {} ELSE {fallback} END",
            quote_ident(name),
            quote_ident(name)
        )
    } else {
        fallback.to_string()
    }
}

fn probability_expr(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!(
            "CASE WHEN {0} IS NULL THEN {fallback} WHEN {0} < 0 THEN 0.0 WHEN {0} > 1 THEN 1.0 ELSE {0} END",
            quote_ident(name)
        )
    } else {
        fallback.to_string()
    }
}

fn signed_probability_expr(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!(
            "CASE WHEN {0} IS NULL THEN {fallback} WHEN {0} < -1 THEN -1.0 WHEN {0} > 1 THEN 1.0 ELSE {0} END",
            quote_ident(name)
        )
    } else {
        fallback.to_string()
    }
}

fn bool_expr(columns: &BTreeSet<String>, name: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!(
            "CASE WHEN COALESCE({},0) = 0 THEN 0 ELSE 1 END",
            quote_ident(name)
        )
    } else {
        fallback.to_string()
    }
}

fn enum_expr(columns: &BTreeSet<String>, name: &str, values: &str, fallback: &str) -> String {
    if columns.contains(name) {
        format!(
            "CASE WHEN {} IN {values} THEN {} ELSE {fallback} END",
            quote_ident(name),
            quote_ident(name)
        )
    } else {
        fallback.to_string()
    }
}

fn normalized_run_id(value: &str) -> &str {
    if value.trim().is_empty() {
        "__LEGACY_ORPHAN__"
    } else {
        value
    }
}

fn nonempty_or<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

fn truncate_chars(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}
