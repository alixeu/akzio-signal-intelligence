use super::*;

impl V2Store {
    pub fn metrics(&self, now: DateTime<Utc>) -> StoreResult<StoreMetrics> {
        let connection = self.connection()?;
        let run_counts = status_counts(&connection, "rebuild_runs")?;
        let task_counts = status_counts(&connection, "rebuild_tasks")?;
        let attempt_counts = status_counts(&connection, "rebuild_attempts")?;
        let event_count =
            connection.query_row("SELECT COUNT(*) FROM rebuild_events", [], |row| {
                row.get::<_, u64>(0)
            })?;
        let active_daemon_leases = connection.query_row(
            "SELECT COUNT(*) FROM rebuild_daemon_leases WHERE expires_at > ?1",
            params![now.to_rfc3339()],
            |row| row.get::<_, u64>(0),
        )?;
        Ok(StoreMetrics {
            run_counts,
            task_counts,
            attempt_counts,
            event_count,
            active_daemon_leases,
        })
    }

    pub fn events_after(
        &self,
        run_id: &RunId,
        after: i64,
        limit: usize,
    ) -> StoreResult<Vec<StoredEvent>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events WHERE run_id = ?1 AND event_id > ?2
               ORDER BY event_id ASC LIMIT ?3"#,
        )?;
        let rows = statement.query_map(
            params![run_id.0, after, limit as i64],
            stored_event_from_row,
        )?;
        let events = rows.collect::<Result<Vec<_>, _>>()?;
        for event in &events {
            let event_type = event.lifecycle_kind()?;
            validate_event_shape(
                event_type,
                event.task_id.is_some(),
                event.attempt_id.is_some(),
                event.artifact_id.is_some(),
            )?;
        }
        validate_tool_lifecycle_events(&connection, Some(run_id))?;
        validate_agent_turn_lifecycle_events(&connection, Some(run_id))?;
        validate_context_lifecycle_events(&connection, Some(run_id))?;
        validate_gate_lifecycle_events(&connection, Some(run_id))?;
        validate_paper_effect_events(&connection, Some(run_id))?;
        Ok(events)
    }

    pub fn recent_events(&self, run_id: &RunId, limit: usize) -> StoreResult<Vec<StoredEvent>> {
        let connection = self.connection()?;
        let limit = i64::try_from(limit.clamp(1, 500)).expect("bounded event limit fits i64");
        let mut statement = connection.prepare(
            r#"SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id, created_at
               FROM rebuild_events
               WHERE run_id = ?1
               ORDER BY event_id DESC
               LIMIT ?2"#,
        )?;
        let mut events = statement
            .query_map(params![run_id.0, limit], stored_event_from_row)?
            .collect::<Result<Vec<_>, _>>()?;
        events.reverse();
        for event in &events {
            let event_type = event.lifecycle_kind()?;
            validate_event_shape(
                event_type,
                event.task_id.is_some(),
                event.attempt_id.is_some(),
                event.artifact_id.is_some(),
            )?;
        }
        validate_tool_lifecycle_events(&connection, Some(run_id))?;
        validate_agent_turn_lifecycle_events(&connection, Some(run_id))?;
        validate_context_lifecycle_events(&connection, Some(run_id))?;
        validate_gate_lifecycle_events(&connection, Some(run_id))?;
        validate_paper_effect_events(&connection, Some(run_id))?;
        Ok(events)
    }

    /// Return a read-only, redacted trajectory projection for one run.
    /// Pagination follows the durable event cursor; no model, market, broker,
    /// task, or artifact mutation is performed by this query.
    pub fn trajectory(&self, run_id: &RunId) -> StoreResult<Vec<TrajectoryEntry>> {
        const PAGE_SIZE: usize = 256;
        let mut after = 0_i64;
        let mut entries = Vec::new();
        loop {
            let page = self.events_after(run_id, after, PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after = page.last().expect("non-empty trajectory page").cursor;
            for event in &page {
                if let Some(entry) = self.trajectory_entry(event)? {
                    entries.push(entry);
                }
            }
            if page.len() < PAGE_SIZE {
                break;
            }
        }
        entries.sort_by(|left, right| {
            left.cursor
                .cmp(&right.cursor)
                .then_with(|| left.task_id.cmp(&right.task_id))
                .then_with(|| left.attempt_id.cmp(&right.attempt_id))
                .then_with(|| left.turn.cmp(&right.turn))
        });
        Ok(entries)
    }

    /// Return the newest redacted trajectory entries in durable cursor order.
    /// The hard cap keeps observer reads bounded even for long-running tasks.
    pub fn recent_trajectory(
        &self,
        run_id: &RunId,
        limit: usize,
    ) -> StoreResult<Vec<TrajectoryEntry>> {
        self.recent_events(run_id, limit.clamp(1, 200))?
            .into_iter()
            .filter_map(|event| self.trajectory_entry(&event).transpose())
            .collect()
    }
}

pub(super) fn stored_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredEvent> {
    Ok(StoredEvent {
        cursor: row.get(0)?,
        run_id: RunId(row.get(1)?),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId),
        attempt_id: row
            .get::<_, Option<String>>(3)?
            .map(akzio_domain::AttemptId),
        event_type: row.get(4)?,
        artifact_id: row
            .get::<_, Option<String>>(5)?
            .map(ContentHash::new)
            .transpose()
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
            .map(ArtifactId),
        created_at: parse_time(&row.get::<_, String>(6)?)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?,
    })
}
