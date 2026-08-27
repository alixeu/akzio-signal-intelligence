impl V2Store {
    /// Returns the latest succeeded attempt for the task, including only
    /// artifacts committed by that exact attempt. The query is intentionally
    /// task-level and attempt-level in one read so an older parent attempt
    /// cannot be projected after a later retry succeeds.
    pub fn current_succeeded_attempt(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<SucceededAttemptProof> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let current = transaction
            .query_row(
                r#"SELECT t.status, t.contract_hash, a.attempt_id, a.lease_id, a.epoch
                   FROM rebuild_tasks AS t
                   JOIN rebuild_attempts AS a ON a.task_id = t.task_id
                   WHERE t.run_id = ?1 AND t.task_id = ?2
                     AND t.status = 'succeeded' AND a.status = 'succeeded'
                   ORDER BY a.finished_at DESC, a.attempt_id DESC
                   LIMIT 1"#,
                params![run_id.0, task_id.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, u64>(4)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::CommittedOutputTask {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
            })?;
        let attempt_id = AttemptId(current.2);
        let outputs =
            read_committed_attempt_outputs(&transaction, Some(run_id), task_id, &attempt_id)?;
        let context_manifest = transaction
            .query_row(
                r#"SELECT artifact_id
                   FROM rebuild_events
                   WHERE run_id = ?1 AND task_id = ?2 AND attempt_id = ?3
                   AND event_type IN ('context.manifest_created',
                                        'context.child_manifest_created')
                     AND artifact_id IS NOT NULL
                   ORDER BY event_id DESC
                   LIMIT 1"#,
                params![run_id.0, task_id.0, attempt_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|artifact_id| {
                ContentHash::new(artifact_id).map(|artifact_id| ArtifactRef {
                    artifact_id: ArtifactId(artifact_id),
                    kind: ArtifactKind::ContextManifest,
                })
            })
            .transpose()?;
        let proof = SucceededAttemptProof {
            run_id: run_id.clone(),
            task_id: task_id.clone(),
            attempt_id,
            lease_id: LeaseId(current.3),
            epoch: current.4,
            contract_hash: current.1.map(ContentHash::new).transpose()?,
            context_manifest,
            outputs,
        };
        drop(transaction);
        Ok(proof)
    }

    /// Returns the durable purpose recorded with a run. Learning uses this
    /// instead of accepting a caller-provided purpose flag.
    pub fn run_purpose(&self, run_id: &RunId) -> StoreResult<RunPurpose> {
        let connection = self.connection()?;
        let purpose = connection
            .query_row(
                "SELECT purpose FROM rebuild_runs WHERE run_id = ?1",
                params![run_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::MissingRun(run_id.clone()))?;
        parse_enum(&purpose)
    }

    pub fn workflow_revision(
        &self,
        run_id: &RunId,
        revision: u64,
    ) -> StoreResult<WorkflowRevision> {
        let connection = self.connection()?;
        self.workflow_revision_with_connection(&connection, run_id, revision)
    }

    pub fn workflow_snapshot(&self, run_id: &RunId) -> StoreResult<WorkflowSnapshot> {
        let connection = self.connection()?;
        self.workflow_snapshot_with_connection(&connection, run_id)
    }

    /// Returns newest workflow snapshots for read-only observer clients.
    /// The Store remains the sole authority and bounds the query even when a
    /// caller supplies an excessive limit.
    pub fn recent_workflows(&self, limit: usize) -> StoreResult<Vec<WorkflowSnapshot>> {
        let connection = self.connection()?;
        let limit = i64::try_from(limit.clamp(1, 100)).expect("bounded observer limit fits i64");
        let run_ids = {
            let mut statement = connection.prepare(
                "SELECT run_id FROM rebuild_runs \
                 ORDER BY created_at DESC, run_id DESC LIMIT ?1",
            )?;
            let rows = statement
                .query_map(params![limit], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };

        run_ids
            .into_iter()
            .map(|run_id| self.workflow_snapshot_with_connection(&connection, &RunId(run_id)))
            .collect()
    }

    /// Monotonic cursor used by observer SSE as an invalidation signal.
    pub fn event_cursor(&self) -> StoreResult<i64> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT COALESCE(MAX(event_id), 0) FROM rebuild_events",
                [],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }
}
