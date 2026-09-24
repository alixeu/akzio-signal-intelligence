//! Run log status is recorded in the existing CAS and event journal, not in a
//! second business state file. Private ZIP payloads are deliberately excluded.

use super::*;
use serde_json::Value;

impl Store {
    pub fn record_run_log_status(
        &self,
        run_id: &RunId,
        phase: &str,
        status: &str,
        detail: &Value,
    ) -> StoreResult<Artifact> {
        if !matches!(phase, "T0" | "T1" | "T3" | "T5")
            || !matches!(status, "opened" | "blocked" | "archived")
        {
            return Err(StoreError::Integrity(
                "invalid Run log stage or status".into(),
            ));
        }
        let now = Utc::now();
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let artifact =
            record_run_log_status_in_transaction(&tx, run_id, phase, status, detail, now)?;
        tx.commit()?;
        Ok(artifact)
    }

    pub fn run_log_status(&self, run_id: &RunId) -> StoreResult<Vec<Value>> {
        let connection = self.connection()?;
        run_log_status_with_connection(&connection, run_id)
    }

    /// A provider call whose outcome is unknown cannot be silently replayed
    /// after its plaintext has been archived and removed. This projection is
    /// rebuilt entirely from the existing CAS event journal on every startup.
    pub fn uncertain_run_log_ids(&self) -> StoreResult<Vec<RunId>> {
        let ids = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                "SELECT DISTINCT run_id FROM rebuild_events WHERE event_type=?1 ORDER BY run_id",
            )?;
            let run_ids = statement
                .query_map(
                    params![LifecycleEventType::RunLogStatusChanged.as_str()],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            run_ids
        };
        let mut blocked = Vec::new();
        for id in ids {
            let run_id = RunId(id);
            if self.run_log_status(&run_id)?.iter().any(|record| {
                (record["status"] == "blocked"
                    && matches!(
                        record["detail"]["reason"].as_str(),
                        Some("unknown_provider_result" | "unreconciled_provider_result")
                    ))
                    || (record["status"] == "archived"
                        && matches!(
                            record["detail"]["integrity_status"].as_str(),
                            Some(
                                "partial_unknown_provider_result"
                                    | "partial_unreconciled_provider_result"
                            )
                        ))
            }) {
                blocked.push(run_id);
            }
        }
        Ok(blocked)
    }

    /// A new formal Run is marked as requiring T0 logging in the same
    /// transaction that publishes its graph. Only those newly marked Runs are
    /// candidates for this repair; pre-logging historical Runs are never
    /// backfilled with invented model bodies.
    pub fn unstarted_required_run_logs(&self) -> StoreResult<Vec<RunId>> {
        let ids = {
            let connection = self.connection()?;
            let mut statement = connection.prepare(
                "SELECT DISTINCT run_id FROM rebuild_events WHERE event_type=?1 ORDER BY run_id",
            )?;
            let run_ids = statement
                .query_map(
                    params![LifecycleEventType::RunLogStatusChanged.as_str()],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            run_ids
        };
        let mut unstarted = Vec::new();
        for id in ids {
            let run_id = RunId(id);
            let statuses = self.run_log_status(&run_id)?;
            if statuses
                .iter()
                .any(|record| record["phase"] == "T0" && record["status"] == "required")
                && !statuses.iter().any(|record| {
                    record["phase"] == "T0"
                        && matches!(record["status"].as_str(), Some("opened" | "archived"))
                })
            {
                unstarted.push(run_id);
            }
        }
        Ok(unstarted)
    }

    /// A provider terminal in the private file is not itself a durable
    /// AgentTurn or a consumed budget. Before resuming an orphaned Run, prove
    /// that the exact research turn was persisted, or that its entire task
    /// attempt finished successfully. News/review calls with no such proof
    /// remain blocked rather than being silently resent.
    pub fn run_log_model_call_accounted(
        &self,
        run_id: &RunId,
        task_id: Option<&str>,
        attempt_id: Option<&str>,
        call_id: Option<&str>,
    ) -> StoreResult<bool> {
        let (Some(task_id), Some(attempt_id)) = (task_id, attempt_id) else {
            return Ok(false);
        };
        let connection = self.connection()?;
        let status: Option<String> = connection
            .query_row(
                "SELECT status FROM rebuild_attempts \
                 WHERE run_id=?1 AND task_id=?2 AND attempt_id=?3",
                params![run_id.0, task_id, attempt_id],
                |row| row.get(0),
            )
            .optional()?;
        if status.as_deref() == Some("succeeded") {
            return Ok(true);
        }
        let Some(call_id) = call_id.and_then(|id| id.strip_prefix("agent-turn:")) else {
            return Ok(false);
        };
        let mut parts = call_id.rsplitn(3, ':');
        let (Some(attempt), Some(turn), Some(request_hash)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Ok(false);
        };
        let (Ok(attempt), Ok(turn), Ok(request_hash)) = (
            attempt.parse::<u64>(),
            turn.parse::<u64>(),
            ContentHash::new(request_hash),
        ) else {
            return Ok(false);
        };
        let mut statement = connection.prepare(
            "SELECT a.blob_hash, a.bytes FROM rebuild_events e \
             JOIN rebuild_artifacts a ON a.artifact_id=e.artifact_id \
             WHERE e.run_id=?1 AND e.task_id=?2 AND e.attempt_id=?3 \
               AND a.kind='agent_turn' \
               AND e.event_type IN (?4,?5,?6,?7)",
        )?;
        let records = statement
            .query_map(
                params![
                    run_id.0,
                    task_id,
                    attempt_id,
                    LifecycleEventType::AgentTurn.as_str(),
                    LifecycleEventType::AgentTurnCompleted.as_str(),
                    LifecycleEventType::AgentTurnFailed.as_str(),
                    LifecycleEventType::AgentTurnRetryableFailed.as_str(),
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for (hash, bytes) in records {
            let payload: Value = serde_json::from_slice(&blob::read_blob_bytes(
                &connection,
                &ContentHash::new(hash)?,
                bytes,
            )?)?;
            if payload["request_hash"] == request_hash.as_str()
                && payload["turn"].as_u64() == Some(turn)
                && payload["attempt"].as_u64() == Some(attempt)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(super) fn record_run_log_required_in_transaction(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    record_run_log_status_in_transaction(
        transaction,
        run_id,
        "T0",
        "required",
        &serde_json::json!({"reason":"formal_run_created"}),
        now,
    )?;
    Ok(())
}

fn record_run_log_status_in_transaction(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    phase: &str,
    status: &str,
    detail: &Value,
    now: DateTime<Utc>,
) -> StoreResult<Artifact> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "type": "run_log_status", "version": 1,
        "run_id": run_id, "phase": phase,
        "status": status, "detail": detail,
        "created_at_utc": now,
    }))?;
    let blob = blob::stage_blob_bytes(transaction, &bytes, "application/json".into())?;
    let artifact = Artifact::new(
        ArtifactKind::SemanticDetail,
        blob,
        "run.log.status",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.run_log".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )?;
    insert_artifact(transaction, &artifact)?;
    append_event(
        transaction,
        run_id,
        None,
        None,
        LifecycleEventType::RunLogStatusChanged,
        Some(&artifact.artifact_id),
        now,
    )?;
    Ok(artifact)
}

pub(super) fn run_log_status_with_connection(
    connection: &Connection,
    run_id: &RunId,
) -> StoreResult<Vec<Value>> {
    let mut statement = connection.prepare(
        "SELECT a.blob_hash, a.bytes FROM rebuild_events e \
             JOIN rebuild_artifacts a ON a.artifact_id=e.artifact_id \
             WHERE e.run_id=?1 AND e.event_type=?2 ORDER BY e.event_id",
    )?;
    let rows = statement
        .query_map(
            params![run_id.0, LifecycleEventType::RunLogStatusChanged.as_str()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(hash, bytes)| {
            let blob = BlobRef {
                hash: ContentHash::new(hash)?,
                bytes,
                media_type: "application/json".into(),
            };
            serde_json::from_slice(&blob::read_blob_bytes(connection, &blob.hash, blob.bytes)?)
                .map_err(StoreError::from)
        })
        .collect()
}
