use super::*;

/// Canonical lease rows extended after one drained maintenance operation.
///
/// Maintenance never rewrites ownership or epochs. Only leases that were live
/// when maintenance started are extended by the time spent in maintenance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceLeaseDeferral {
    pub task_leases: u64,
    pub daemon_leases: u64,
}

impl Store {
    /// Preserve live task and daemon ownership across a drained maintenance
    /// window. Expired leases remain expired and can still be recovered.
    pub fn defer_live_leases_for_maintenance(
        &self,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    ) -> StoreResult<MaintenanceLeaseDeferral> {
        let elapsed = completed_at.signed_duration_since(started_at);
        if elapsed <= Duration::zero() {
            return Ok(MaintenanceLeaseDeferral::default());
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task_leases = {
            let mut statement = transaction.prepare(
                "SELECT task_id, lease_until FROM rebuild_tasks \
                 WHERE status = 'running' AND lease_until > ?1 ORDER BY task_id",
            )?;
            let rows = statement
                .query_map(params![started_at.to_rfc3339()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        let daemon_leases = {
            let mut statement = transaction.prepare(
                "SELECT lease_name, expires_at FROM rebuild_daemon_leases \
                 WHERE expires_at > ?1 ORDER BY lease_name",
            )?;
            let rows = statement
                .query_map(params![started_at.to_rfc3339()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };

        let mut deferred = MaintenanceLeaseDeferral::default();
        for (task_id, lease_until) in task_leases {
            let extended = parse_time(&lease_until)? + elapsed;
            deferred.task_leases += transaction.execute(
                "UPDATE rebuild_tasks SET lease_until = ?1 \
                 WHERE task_id = ?2 AND status = 'running' AND lease_until = ?3",
                params![extended.to_rfc3339(), task_id, lease_until],
            )? as u64;
        }
        for (lease_name, expires_at) in daemon_leases {
            let extended = parse_time(&expires_at)? + elapsed;
            deferred.daemon_leases += transaction.execute(
                "UPDATE rebuild_daemon_leases SET expires_at = ?1 \
                 WHERE lease_name = ?2 AND expires_at = ?3",
                params![extended.to_rfc3339(), lease_name, expires_at],
            )? as u64;
        }
        transaction.commit()?;
        Ok(deferred)
    }
}

impl RetentionPolicy {
    fn validate(self) -> StoreResult<()> {
        if [
            self.debug,
            self.position_plan,
            self.paper_dry_run,
            self.replay,
            self.shadow,
        ]
        .into_iter()
        .flatten()
        .any(|ttl| ttl < Duration::zero())
        {
            return Err(StoreError::Integrity(
                "retention TTL must not be negative".to_owned(),
            ));
        }
        Ok(())
    }

    fn ttl_for_purpose(self, purpose: &str) -> Option<Duration> {
        match purpose {
            "debug" => self.debug,
            "position_plan" => self.position_plan,
            "paper_dry_run" => self.paper_dry_run,
            "replay" => self.replay,
            "shadow" => self.shadow,
            _ => None,
        }
    }
}

impl Store {
    /// Build a deterministic retention plan without changing Store state.
    pub fn plan_retention(
        &self,
        policy: RetentionPolicy,
        now: DateTime<Utc>,
    ) -> StoreResult<RetentionPlan> {
        policy.validate()?;
        let connection = self.connection()?;
        build_retention_plan(&connection, policy, now)
    }

    /// Revalidate and atomically apply a previously planned retention change.
    pub fn apply_retention(
        &self,
        plan: &RetentionPlan,
        now: DateTime<Utc>,
    ) -> StoreResult<RetentionReport> {
        plan.policy.validate()?;
        if now < plan.planned_at {
            return Err(StoreError::Integrity(
                "retention apply time precedes plan time".to_owned(),
            ));
        }
        if retention_plan_hash(plan)? != plan.plan_hash {
            return Err(StoreError::Integrity(
                "retention plan hash is invalid".to_owned(),
            ));
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = build_retention_plan(&transaction, plan.policy, plan.planned_at)?;
        if current.event_cursor != plan.event_cursor {
            return Err(StoreError::Integrity(format!(
                "retention plan is stale: event cursor changed from {} to {}",
                plan.event_cursor, current.event_cursor
            )));
        }
        if current.plan_hash != plan.plan_hash || current != *plan {
            return Err(StoreError::Integrity(
                "retention plan is stale: protected closure or candidates changed".to_owned(),
            ));
        }

        transaction.execute_batch(
            "DROP TABLE IF EXISTS temp.akzio_retention_runs;
             DROP TABLE IF EXISTS temp.akzio_retention_artifacts;
             DROP TABLE IF EXISTS temp.akzio_retention_blobs;
             CREATE TEMP TABLE akzio_retention_runs (run_id TEXT PRIMARY KEY);
             CREATE TEMP TABLE akzio_retention_artifacts (artifact_id TEXT PRIMARY KEY);
             CREATE TEMP TABLE akzio_retention_blobs (blob_hash TEXT PRIMARY KEY);",
        )?;
        {
            let mut insert_run = transaction
                .prepare("INSERT INTO temp.akzio_retention_runs (run_id) VALUES (?1)")?;
            for run_id in &plan.run_ids {
                insert_run.execute(params![run_id.0])?;
            }
            let mut insert_artifact = transaction
                .prepare("INSERT INTO temp.akzio_retention_artifacts (artifact_id) VALUES (?1)")?;
            for artifact_id in &plan.artifact_ids {
                insert_artifact.execute(params![artifact_id.0.as_str()])?;
            }
            let mut insert_blob = transaction
                .prepare("INSERT INTO temp.akzio_retention_blobs (blob_hash) VALUES (?1)")?;
            for blob_hash in &plan.blob_hashes {
                insert_blob.execute(params![blob_hash.as_str()])?;
            }
        }

        let retained_artifact_edge: Option<(String, String)> = transaction
            .query_row(
                "SELECT artifact_id, source_artifact_id
                 FROM rebuild_artifact_refs
                 WHERE source_artifact_id IN (SELECT artifact_id FROM temp.akzio_retention_artifacts)
                   AND artifact_id NOT IN (SELECT artifact_id FROM temp.akzio_retention_artifacts)
                 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((artifact_id, source_artifact_id)) = retained_artifact_edge {
            return Err(StoreError::Integrity(format!(
                "retention would break artifact closure {artifact_id} -> {source_artifact_id}"
            )));
        }
        let retained_blob_dependency: Option<(String, String)> = transaction
            .query_row(
                "SELECT blob_hash, dependency_blob_hash
                 FROM rebuild_blob_dependencies
                 WHERE dependency_blob_hash IN (SELECT blob_hash FROM temp.akzio_retention_blobs)
                   AND blob_hash NOT IN (SELECT blob_hash FROM temp.akzio_retention_blobs)
                 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((blob_hash, dependency_blob_hash)) = retained_blob_dependency {
            return Err(StoreError::Integrity(format!(
                "retention would break blob dependency {blob_hash} -> {dependency_blob_hash}"
            )));
        }

        transaction.execute(
            "DELETE FROM rebuild_attempt_outputs
             WHERE task_id IN (
                 SELECT task_id FROM rebuild_tasks
                 WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)
             )",
            [],
        )?;
        transaction.execute(
            "DELETE FROM rebuild_task_dependencies
             WHERE task_id IN (
                     SELECT task_id FROM rebuild_tasks
                     WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)
                 )
                OR depends_on_task_id IN (
                     SELECT task_id FROM rebuild_tasks
                     WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)
                 )",
            [],
        )?;
        transaction.execute(
            "DELETE FROM rebuild_events
             WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)",
            [],
        )?;
        transaction.execute(
            "DELETE FROM rebuild_attempts
             WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)",
            [],
        )?;
        transaction.execute(
            "DELETE FROM rebuild_tasks
             WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)",
            [],
        )?;
        transaction.execute(
            "DELETE FROM rebuild_workflow_revisions
             WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)",
            [],
        )?;
        transaction.execute(
            "DELETE FROM rebuild_run_cancellations
             WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)",
            [],
        )?;
        let deleted_runs = transaction.execute(
            "DELETE FROM rebuild_runs
             WHERE run_id IN (SELECT run_id FROM temp.akzio_retention_runs)",
            [],
        )? as u64;

        transaction.execute(
            "DELETE FROM rebuild_embedded_blob_refs
             WHERE artifact_id IN (SELECT artifact_id FROM temp.akzio_retention_artifacts)",
            [],
        )?;
        transaction.execute(
            "DELETE FROM rebuild_artifact_refs
             WHERE artifact_id IN (SELECT artifact_id FROM temp.akzio_retention_artifacts)
                OR source_artifact_id IN (SELECT artifact_id FROM temp.akzio_retention_artifacts)",
            [],
        )?;
        let deleted_artifacts = transaction.execute(
            "DELETE FROM rebuild_artifacts
             WHERE artifact_id IN (SELECT artifact_id FROM temp.akzio_retention_artifacts)",
            [],
        )? as u64;
        transaction.execute(
            "DELETE FROM rebuild_blob_dependencies
             WHERE blob_hash IN (SELECT blob_hash FROM temp.akzio_retention_blobs)",
            [],
        )?;
        let deleted_blobs = transaction.execute(
            "DELETE FROM rebuild_blobs
             WHERE blob_hash IN (SELECT blob_hash FROM temp.akzio_retention_blobs)
               AND NOT EXISTS (
                   SELECT 1 FROM rebuild_artifacts
                   WHERE rebuild_artifacts.blob_hash = rebuild_blobs.blob_hash
               )
               AND NOT EXISTS (
                   SELECT 1 FROM rebuild_embedded_blob_refs
                   WHERE rebuild_embedded_blob_refs.blob_hash = rebuild_blobs.blob_hash
               )",
            [],
        )? as u64;

        if deleted_runs != plan.run_ids.len() as u64
            || deleted_artifacts != plan.artifact_ids.len() as u64
            || deleted_blobs != plan.blob_hashes.len() as u64
        {
            return Err(StoreError::Integrity(
                "retention row counts changed while applying plan".to_owned(),
            ));
        }

        transaction.execute_batch(
            "DROP TABLE temp.akzio_retention_runs;
             DROP TABLE temp.akzio_retention_artifacts;
             DROP TABLE temp.akzio_retention_blobs;",
        )?;
        transaction.commit()?;
        Ok(RetentionReport {
            plan_hash: plan.plan_hash.clone(),
            deleted_runs,
            deleted_artifacts,
            deleted_blobs,
            reclaimed_logical_blob_bytes: plan.logical_blob_bytes,
        })
    }
}

fn build_retention_plan(
    connection: &Connection,
    policy: RetentionPolicy,
    planned_at: DateTime<Utc>,
) -> StoreResult<RetentionPlan> {
    let event_cursor = connection.query_row(
        "SELECT COALESCE(MAX(event_id), 0) FROM rebuild_events",
        [],
        |row| row.get::<_, i64>(0),
    )?;
    let mut run_ids = initial_retention_runs(connection, policy, planned_at)?;
    for protected in externally_referenced_runs(connection, &run_ids)? {
        run_ids.remove(&protected);
    }

    loop {
        let protected_artifacts = protected_artifact_closure(connection, &run_ids)?;
        let origin_owners = candidate_origin_artifact_owners(connection, &run_ids)?;
        let newly_protected = origin_owners
            .into_iter()
            .filter_map(|(artifact_id, run_id)| {
                protected_artifacts.contains(&artifact_id).then_some(run_id)
            })
            .collect::<BTreeSet<_>>();
        if newly_protected.is_empty() {
            break;
        }
        for run_id in newly_protected {
            run_ids.remove(&run_id);
        }
    }

    let protected_artifacts = protected_artifact_closure(connection, &run_ids)?;
    let mut artifact_ids = candidate_owned_artifacts(connection, &run_ids)?;
    artifact_ids.retain(|artifact_id| !protected_artifacts.contains(artifact_id));
    let (blob_hashes, logical_blob_bytes) = blobs_unreferenced_after(connection, &artifact_ids)?;

    let run_ids = run_ids.into_iter().map(RunId).collect::<Vec<_>>();
    let artifact_ids = artifact_ids
        .into_iter()
        .map(|id| ContentHash::new(id).map(ArtifactId))
        .collect::<Result<Vec<_>, _>>()?;
    let blob_hashes = blob_hashes
        .into_iter()
        .map(ContentHash::new)
        .collect::<Result<Vec<_>, _>>()?;
    let mut plan = RetentionPlan {
        policy,
        planned_at,
        event_cursor,
        run_ids,
        artifact_ids,
        blob_hashes,
        logical_blob_bytes,
        plan_hash: akzio_domain::content_hash_json(&serde_json::json!({}))?,
    };
    plan.plan_hash = retention_plan_hash(&plan)?;
    Ok(plan)
}

fn retention_plan_hash(plan: &RetentionPlan) -> StoreResult<ContentHash> {
    let hash_input = serde_json::json!({
        "policy": {
            "debug": plan.policy.debug.map(duration_hash_parts),
            "position_plan": plan.policy.position_plan.map(duration_hash_parts),
            "paper_dry_run": plan.policy.paper_dry_run.map(duration_hash_parts),
            "replay": plan.policy.replay.map(duration_hash_parts),
            "shadow": plan.policy.shadow.map(duration_hash_parts),
        },
        "planned_at": plan.planned_at.to_rfc3339(),
        "event_cursor": plan.event_cursor,
        "run_ids": plan.run_ids.iter().map(|id| id.0.as_str()).collect::<Vec<_>>(),
        "artifact_ids": plan.artifact_ids.iter().map(|id| id.0.as_str()).collect::<Vec<_>>(),
        "blob_hashes": plan.blob_hashes.iter().map(ContentHash::as_str).collect::<Vec<_>>(),
        "logical_blob_bytes": plan.logical_blob_bytes,
    });
    Ok(akzio_domain::content_hash_json(&hash_input)?)
}

fn initial_retention_runs(
    connection: &Connection,
    policy: RetentionPolicy,
    now: DateTime<Utc>,
) -> StoreResult<BTreeSet<String>> {
    let mut statement = connection.prepare(
        "SELECT run_id, purpose, finished_at
         FROM rebuild_runs
         WHERE finished_at IS NOT NULL
           AND status IN ('completed', 'completed_with_execution_rejection', 'failed', 'cancelled')
           AND NOT EXISTS (
               SELECT 1 FROM rebuild_tasks AS t
               WHERE t.run_id = rebuild_runs.run_id
                 AND (t.status NOT IN ('succeeded', 'failed', 'cancelled', 'skipped')
                      OR t.active_attempt_id IS NOT NULL)
           )
           AND NOT EXISTS (
               SELECT 1 FROM rebuild_attempts AS a
               WHERE a.run_id = rebuild_runs.run_id
                 AND (a.finished_at IS NULL OR a.status = 'running')
           )
         ORDER BY run_id",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut candidates = BTreeSet::new();
    for (run_id, purpose, finished_at) in rows {
        let Some(ttl) = policy.ttl_for_purpose(&purpose) else {
            continue;
        };
        let Some(cutoff) = now.checked_sub_signed(ttl) else {
            continue;
        };
        if parse_time(&finished_at)? < cutoff {
            candidates.insert(run_id);
        }
    }
    Ok(candidates)
}

fn externally_referenced_runs(
    connection: &Connection,
    candidates: &BTreeSet<String>,
) -> StoreResult<BTreeSet<String>> {
    const OWNED_TABLES: &[&str] = &[
        "rebuild_runs",
        "rebuild_run_cancellations",
        "rebuild_workflow_revisions",
        "rebuild_tasks",
        "rebuild_task_dependencies",
        "rebuild_attempts",
        "rebuild_events",
        "rebuild_attempt_outputs",
    ];
    let mut protected = BTreeSet::new();
    for (table, from_column) in foreign_key_columns(connection, "rebuild_runs")? {
        if OWNED_TABLES.contains(&table.as_str()) {
            continue;
        }
        for value in distinct_text_column(connection, &table, &from_column)? {
            if candidates.contains(&value) {
                protected.insert(value);
            }
        }
    }
    let mut statement = connection.prepare(
        "SELECT run_id FROM rebuild_session_slots WHERE run_id IS NOT NULL ORDER BY run_id",
    )?;
    for run_id in statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
    {
        if candidates.contains(&run_id) {
            protected.insert(run_id);
        }
    }
    Ok(protected)
}

fn candidate_origin_artifact_owners(
    connection: &Connection,
    candidates: &BTreeSet<String>,
) -> StoreResult<Vec<(String, String)>> {
    let mut statement = connection.prepare(
        "SELECT artifact_id, json_extract(origin_json, '$.run_id')
         FROM rebuild_artifacts
         WHERE json_extract(origin_json, '$.run_id') IS NOT NULL
         ORDER BY artifact_id",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter(|(_, run_id)| candidates.contains(run_id))
        .collect())
}

fn candidate_owned_artifacts(
    connection: &Connection,
    candidates: &BTreeSet<String>,
) -> StoreResult<BTreeSet<String>> {
    let mut owned = candidate_origin_artifact_owners(connection, candidates)?
        .into_iter()
        .map(|(artifact_id, _)| artifact_id)
        .collect::<BTreeSet<_>>();
    let mut statement = connection.prepare(
        "SELECT run_id, graph_artifact_id FROM rebuild_runs
         UNION ALL
         SELECT run_id, graph_artifact_id FROM rebuild_workflow_revisions",
    )?;
    for (run_id, artifact_id) in statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    {
        if candidates.contains(&run_id) {
            owned.insert(artifact_id);
        }
    }
    Ok(owned)
}

fn protected_artifact_closure(
    connection: &Connection,
    candidates: &BTreeSet<String>,
) -> StoreResult<BTreeSet<String>> {
    let candidate_owned = candidate_owned_artifacts(connection, candidates)?;
    let mut roots = BTreeSet::new();
    let mut statement = connection
        .prepare("SELECT artifact_id, lifecycle FROM rebuild_artifacts ORDER BY artifact_id")?;
    for (artifact_id, lifecycle) in statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    {
        if lifecycle == "canonical" || !candidate_owned.contains(&artifact_id) {
            roots.insert(artifact_id);
        }
    }

    add_retained_workflow_artifact_roots(connection, candidates, &mut roots)?;
    for (table, from_column) in foreign_key_columns(connection, "rebuild_artifacts")? {
        if matches!(
            table.as_str(),
            "rebuild_artifact_refs"
                | "rebuild_runs"
                | "rebuild_workflow_revisions"
                | "rebuild_events"
                | "rebuild_attempt_outputs"
        ) {
            continue;
        }
        roots.extend(distinct_text_column(connection, &table, &from_column)?);
    }

    let mut edges = Vec::new();
    let mut statement = connection.prepare(
        "SELECT artifact_id, source_artifact_id
         FROM rebuild_artifact_refs
         ORDER BY artifact_id, source_artifact_id",
    )?;
    edges.extend(
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?,
    );
    loop {
        let mut changed = false;
        for (artifact_id, source_artifact_id) in &edges {
            if roots.contains(artifact_id) && roots.insert(source_artifact_id.clone()) {
                changed = true;
            }
        }
        if !changed {
            return Ok(roots);
        }
    }
}

fn add_retained_workflow_artifact_roots(
    connection: &Connection,
    candidates: &BTreeSet<String>,
    roots: &mut BTreeSet<String>,
) -> StoreResult<()> {
    for sql in [
        "SELECT run_id, graph_artifact_id FROM rebuild_runs",
        "SELECT run_id, graph_artifact_id FROM rebuild_workflow_revisions",
        "SELECT run_id, artifact_id FROM rebuild_events WHERE artifact_id IS NOT NULL",
        "SELECT t.run_id, o.artifact_id
         FROM rebuild_attempt_outputs AS o
         JOIN rebuild_tasks AS t ON t.task_id = o.task_id",
    ] {
        let mut statement = connection.prepare(sql)?;
        for (run_id, artifact_id) in statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        {
            if !candidates.contains(&run_id) {
                roots.insert(artifact_id);
            }
        }
    }
    let mut statement = connection
        .prepare("SELECT run_id, input_artifacts_json FROM rebuild_tasks ORDER BY task_id")?;
    for (run_id, input_artifacts_json) in statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    {
        if candidates.contains(&run_id) {
            continue;
        }
        let references: Vec<ArtifactRef> = serde_json::from_str(&input_artifacts_json)?;
        roots.extend(
            references
                .into_iter()
                .map(|reference| reference.artifact_id.0.to_string()),
        );
    }
    Ok(())
}

fn blobs_unreferenced_after(
    connection: &Connection,
    deleted_artifacts: &BTreeSet<String>,
) -> StoreResult<(BTreeSet<String>, u64)> {
    let mut references = BTreeMap::<String, BTreeSet<String>>::new();
    for sql in [
        "SELECT blob_hash, artifact_id FROM rebuild_artifacts",
        "SELECT blob_hash, artifact_id FROM rebuild_embedded_blob_refs",
    ] {
        let mut statement = connection.prepare(sql)?;
        for (blob_hash, artifact_id) in statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        {
            references.entry(blob_hash).or_default().insert(artifact_id);
        }
    }
    let mut retained_blobs = references
        .into_iter()
        .filter_map(|(blob_hash, artifact_ids)| {
            artifact_ids
                .iter()
                .any(|artifact_id| !deleted_artifacts.contains(artifact_id))
                .then_some(blob_hash)
        })
        .collect::<BTreeSet<_>>();
    let mut dependency_statement = connection.prepare(
        "SELECT blob_hash, dependency_blob_hash
         FROM rebuild_blob_dependencies
         ORDER BY blob_hash, ordinal",
    )?;
    let dependencies = dependency_statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    loop {
        let mut changed = false;
        for (blob_hash, dependency_blob_hash) in &dependencies {
            if retained_blobs.contains(blob_hash)
                && retained_blobs.insert(dependency_blob_hash.clone())
            {
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    let mut blob_hashes = BTreeSet::new();
    let mut logical_blob_bytes = 0_u64;
    let mut statement = connection
        .prepare("SELECT blob_hash, logical_bytes FROM rebuild_blobs ORDER BY blob_hash")?;
    for (blob_hash, logical_bytes) in statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    {
        if !retained_blobs.contains(&blob_hash) {
            blob_hashes.insert(blob_hash);
            logical_blob_bytes = logical_blob_bytes.saturating_add(logical_bytes);
        }
    }
    Ok((blob_hashes, logical_blob_bytes))
}

fn foreign_key_columns(
    connection: &Connection,
    referenced_table: &str,
) -> StoreResult<Vec<(String, String)>> {
    let mut table_statement = connection.prepare(
        "SELECT name FROM sqlite_master
         WHERE type = 'table' AND name LIKE 'rebuild_%'
         ORDER BY name",
    )?;
    let tables = table_statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(table_statement);
    let mut columns = Vec::new();
    for table in tables {
        let sql = format!("PRAGMA foreign_key_list({})", quote_identifier(&table));
        let mut statement = connection.prepare(&sql)?;
        for (target, from_column) in statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(2)?, row.get::<_, String>(3)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
        {
            if target == referenced_table {
                columns.push((table.clone(), from_column));
            }
        }
    }
    Ok(columns)
}

fn distinct_text_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> StoreResult<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL ORDER BY {column}",
        column = quote_identifier(column),
        table = quote_identifier(table),
    );
    let mut statement = connection.prepare(&sql)?;
    let values = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(values)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn duration_hash_parts(duration: Duration) -> (i64, i32) {
    (duration.num_seconds(), duration.subsec_nanos())
}
