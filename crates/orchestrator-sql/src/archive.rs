use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct RunArchiveInput {
    pub run_id: String,
    pub workflow_version: String,
    pub prompt_versions_json: Value,
    pub git_sha: String,
    pub config_hash: String,
    pub artifact_path: String,
    pub degraded: bool,
    pub phase_count: i64,
    pub total_elapsed_ms: i64,
}

pub fn upsert_run_archive(conn: &Connection, input: &RunArchiveInput) -> Result<()> {
    conn.execute(
        r#"
        UPDATE runs
        SET workflow_version = ?,
            prompt_versions_json = ?,
            git_sha = ?,
            config_hash = ?,
            artifact_path = ?,
            degraded = ?,
            phase_count = ?,
            total_elapsed_ms = ?
        WHERE run_id = ?
        "#,
        params![
            input.workflow_version,
            serde_json::to_string(&input.prompt_versions_json)?,
            input.git_sha,
            input.config_hash,
            input.artifact_path,
            input.degraded as i64,
            input.phase_count,
            input.total_elapsed_ms,
            input.run_id,
        ],
    )?;
    Ok(())
}

pub fn run_archive_by_id(conn: &Connection, run_id: &str) -> Result<Value> {
    let text = conn.query_row(
        r#"
        SELECT json_object(
            'run_id', run_id,
            'workflow_version', workflow_version,
            'prompt_versions_json', json(prompt_versions_json),
            'git_sha', git_sha,
            'config_hash', config_hash,
            'artifact_path', artifact_path,
            'degraded', degraded,
            'phase_count', phase_count,
            'total_elapsed_ms', total_elapsed_ms,
            'created_at', created_at
        )
        FROM runs WHERE run_id = ?
        "#,
        params![run_id],
        |row| row.get::<_, String>(0),
    )?;
    Ok(serde_json::from_str(&text).unwrap_or_else(|_| json!({})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{connect, write::write_run_record, RunRecordInput};

    #[test]
    fn upserts_run_archive() {
        let temp = tempfile::tempdir().unwrap();
        let mut conn = connect(temp.path().join("archive.sqlite")).unwrap();
        write_run_record(
            &mut conn,
            &RunRecordInput {
                run_id: "run-1",
                current_date: "2026-01-01",
            },
        )
        .unwrap();
        upsert_run_archive(
            &conn,
            &RunArchiveInput {
                run_id: "run-1".to_string(),
                workflow_version: "v1".to_string(),
                prompt_versions_json: json!({}),
                git_sha: String::new(),
                config_hash: String::new(),
                artifact_path: "outputs/run".to_string(),
                degraded: false,
                phase_count: 8,
                total_elapsed_ms: 10,
            },
        )
        .unwrap();
        let row = run_archive_by_id(&conn, "run-1").unwrap();
        assert_eq!(row["artifact_path"], "outputs/run");
        assert_eq!(row["phase_count"], 8);
    }
}
