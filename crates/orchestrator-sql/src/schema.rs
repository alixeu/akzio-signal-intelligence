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
    ensure_schema(&conn)?;
    Ok(conn)
}

pub fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            ticker TEXT NOT NULL,
            tickers_json TEXT NOT NULL,
            current_date TEXT NOT NULL,
            mode TEXT NOT NULL,
            run_dir TEXT NOT NULL DEFAULT '',
            db_path TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            config_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS run_tickers (
            run_id TEXT NOT NULL,
            ticker TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY (run_id, ticker)
        );
        CREATE TABLE IF NOT EXISTS agent_turns (
            turn_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            run_id TEXT NOT NULL DEFAULT '',
            phase INTEGER,
            role TEXT NOT NULL DEFAULT '',
            user_input TEXT NOT NULL DEFAULT '',
            model_context TEXT NOT NULL DEFAULT '',
            cancellation_state TEXT NOT NULL DEFAULT 'none',
            needs_follow_up INTEGER NOT NULL DEFAULT 0,
            end_reason TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_agent_turns_session
            ON agent_turns(session_id, updated_at);
        CREATE INDEX IF NOT EXISTS idx_agent_turns_run_phase_role
            ON agent_turns(run_id, phase, role);
        CREATE TABLE IF NOT EXISTS agent_turn_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            turn_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            run_id TEXT NOT NULL DEFAULT '',
            item_index INTEGER NOT NULL,
            item_type TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            tool_call_id TEXT NOT NULL DEFAULT '',
            tool_name TEXT NOT NULL DEFAULT '',
            content_json TEXT NOT NULL DEFAULT '{}',
            content_text TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_agent_turn_items_turn
            ON agent_turn_items(turn_id, item_index);
        CREATE INDEX IF NOT EXISTS idx_agent_turn_items_session
            ON agent_turn_items(session_id, id);
        CREATE INDEX IF NOT EXISTS idx_agent_turn_items_run_type
            ON agent_turn_items(run_id, item_type);
        CREATE TABLE IF NOT EXISTS turn_context_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL DEFAULT '',
            turn_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            phase INTEGER,
            ticker TEXT NOT NULL DEFAULT '',
            item_time TEXT NOT NULL DEFAULT '',
            topic_id TEXT,
            debate_id TEXT,
            context_type TEXT NOT NULL DEFAULT '',
            context_ref TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL DEFAULT '',
            item_json TEXT NOT NULL DEFAULT '{}',
            weight REAL NOT NULL DEFAULT 1,
            content_hash TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_turn_context_items_turn
            ON turn_context_items(turn_id, id);
        CREATE INDEX IF NOT EXISTS idx_turn_context_items_run_source
            ON turn_context_items(run_id, context_type);
        CREATE TABLE IF NOT EXISTS turn_tool_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL DEFAULT '',
            turn_id TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            phase INTEGER,
            ticker TEXT NOT NULL DEFAULT '',
            item_time TEXT NOT NULL DEFAULT '',
            topic_id TEXT,
            debate_id TEXT,
            tool_call_id TEXT NOT NULL,
            tool_type TEXT NOT NULL DEFAULT '',
            tool_name TEXT NOT NULL DEFAULT '',
            args_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT '',
            error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            UNIQUE(turn_id, tool_call_id)
        );
        CREATE INDEX IF NOT EXISTS idx_turn_tool_calls_turn
            ON turn_tool_calls(turn_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_turn_tool_calls_run_tool
            ON turn_tool_calls(run_id, tool_name);
        CREATE TABLE IF NOT EXISTS role_turn_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL DEFAULT '',
            turn_id TEXT NOT NULL DEFAULT '',
            phase INTEGER,
            role TEXT NOT NULL DEFAULT '',
            ticker TEXT NOT NULL DEFAULT '',
            item_time TEXT NOT NULL DEFAULT '',
            topic_id TEXT,
            debate_id TEXT,
            summary_type TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL,
            summary_json TEXT NOT NULL DEFAULT '{}',
            confidence REAL NOT NULL DEFAULT 0.5,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_role_turn_summaries_run_role
            ON role_turn_summaries(run_id, role, phase);
        CREATE INDEX IF NOT EXISTS idx_role_turn_summaries_turn
            ON role_turn_summaries(turn_id);
        CREATE TABLE IF NOT EXISTS jin10_items (
            event_key TEXT PRIMARY KEY,
            item_time TEXT NOT NULL,
            content TEXT NOT NULL,
            item_json TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            fetched_at TEXT NOT NULL DEFAULT '',
            imported_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_jin10_items_time
            ON jin10_items(item_time);
        CREATE INDEX IF NOT EXISTS idx_jin10_items_hash
            ON jin10_items(content_hash);
        CREATE TABLE IF NOT EXISTS youtube_videos (
            video_id TEXT NOT NULL,
            ticker TEXT NOT NULL DEFAULT '',
            published_at TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            channel TEXT NOT NULL DEFAULT '',
            url TEXT NOT NULL DEFAULT '',
            item_json TEXT NOT NULL DEFAULT '{}',
            content_hash TEXT NOT NULL DEFAULT '',
            imported_at TEXT NOT NULL,
            PRIMARY KEY(video_id, ticker)
        );
        CREATE INDEX IF NOT EXISTS idx_youtube_videos_ticker_time
            ON youtube_videos(ticker, published_at);
        CREATE INDEX IF NOT EXISTS idx_youtube_videos_hash
            ON youtube_videos(content_hash);
        CREATE TABLE IF NOT EXISTS youtube_transcripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            ticker TEXT NOT NULL DEFAULT '',
            transcript TEXT NOT NULL DEFAULT '',
            segments_json TEXT NOT NULL DEFAULT '[]',
            language TEXT NOT NULL DEFAULT '',
            provider TEXT NOT NULL DEFAULT '',
            content_hash TEXT NOT NULL DEFAULT '',
            imported_at TEXT NOT NULL,
            UNIQUE(video_id, ticker, provider, content_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_youtube_transcripts_hash
            ON youtube_transcripts(content_hash);
        CREATE TABLE IF NOT EXISTS reddit_items (
            item_key TEXT PRIMARY KEY,
            ticker TEXT NOT NULL DEFAULT '',
            item_time TEXT NOT NULL DEFAULT '',
            subreddit TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL DEFAULT '',
            item_json TEXT NOT NULL DEFAULT '{}',
            content_hash TEXT NOT NULL DEFAULT '',
            imported_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_reddit_items_ticker_time
            ON reddit_items(ticker, item_time);
        CREATE INDEX IF NOT EXISTS idx_reddit_items_subreddit_time
            ON reddit_items(subreddit, item_time);
        CREATE TABLE IF NOT EXISTS x_items (
            item_key TEXT PRIMARY KEY,
            ticker TEXT NOT NULL DEFAULT '',
            item_time TEXT NOT NULL DEFAULT '',
            author TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL DEFAULT '',
            item_json TEXT NOT NULL DEFAULT '{}',
            content_hash TEXT NOT NULL DEFAULT '',
            imported_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_x_items_ticker_time
            ON x_items(ticker, item_time);
        CREATE INDEX IF NOT EXISTS idx_x_items_author_time
            ON x_items(author, item_time);
        CREATE TABLE IF NOT EXISTS technical_daily_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            kline_time TEXT NOT NULL,
            indicator_name TEXT NOT NULL,
            indicator_value REAL NOT NULL,
            unit TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            imported_at TEXT NOT NULL,
            UNIQUE(ticker, kline_time, indicator_name, model)
        );
        CREATE INDEX IF NOT EXISTS idx_technical_daily_lookup
            ON technical_daily_indicators(ticker, indicator_name, kline_time);
        CREATE TABLE IF NOT EXISTS technical_3h_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            kline_time TEXT NOT NULL,
            indicator_name TEXT NOT NULL,
            indicator_value REAL NOT NULL,
            unit TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            imported_at TEXT NOT NULL,
            UNIQUE(ticker, kline_time, indicator_name, model)
        );
        CREATE INDEX IF NOT EXISTS idx_technical_3h_lookup
            ON technical_3h_indicators(ticker, indicator_name, kline_time);
        CREATE TABLE IF NOT EXISTS technical_20min_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            kline_time TEXT NOT NULL,
            indicator_name TEXT NOT NULL,
            indicator_value REAL NOT NULL,
            unit TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            imported_at TEXT NOT NULL,
            UNIQUE(ticker, kline_time, indicator_name, model)
        );
        CREATE INDEX IF NOT EXISTS idx_technical_20min_lookup
            ON technical_20min_indicators(ticker, indicator_name, kline_time);
        "#,
    )?;

    for column_sql in [
        "run_dir TEXT NOT NULL DEFAULT ''",
        "db_path TEXT NOT NULL DEFAULT ''",
    ] {
        add_column_if_missing(conn, "runs", column_sql)?;
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
