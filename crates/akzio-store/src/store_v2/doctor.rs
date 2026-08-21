use super::*;

impl V2Store {
    pub fn verify_integrity(&self) -> StoreResult<()> {
        self.ensure_lesson_tables()?;
        let connection = self.connection()?;
        let quick_check =
            connection.query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))?;
        if quick_check != "ok" {
            return Err(StoreError::Integrity(format!(
                "SQLite quick_check failed: {quick_check}"
            )));
        }
        let mut event_statement = connection
            .prepare(
                "SELECT event_id, run_id, task_id, attempt_id, event_type, artifact_id FROM rebuild_events ORDER BY event_id ASC",
            )?;
        let event_rows = event_statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        for row in event_rows {
            let (cursor, run_id, task_id, attempt_id, event_type, artifact_id) = row?;
            let event_type = LifecycleEventType::parse(&event_type).map_err(|error| {
                StoreError::Integrity(format!(
                    "event {cursor} has invalid lifecycle type: {error}"
                ))
            })?;
            validate_event_shape(
                event_type,
                task_id.is_some(),
                attempt_id.is_some(),
                artifact_id.is_some(),
            )
            .map_err(|error| {
                StoreError::Integrity(format!(
                    "event {cursor} in run {run_id} has invalid shape: {error}"
                ))
            })?;
        }
        validate_tool_lifecycle_events(&connection, None)?;
        validate_agent_turn_lifecycle_events(&connection, None)?;
        validate_context_lifecycle_events(&connection, None)?;
        validate_gate_lifecycle_events(&connection, None)?;
        validate_paper_effect_events(&connection, None)?;
        verify_retrospective_history(self, &connection)?;
        verify_attempt_relation_history(self, &connection)?;
        let fk = connection
            .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
            .optional()?;
        if fk.is_some() {
            return Err(StoreError::Integrity("foreign key check failed".to_owned()));
        }
        let invalid_attempt_output = connection
            .query_row(
                r#"SELECT o.event_id
                     FROM rebuild_attempt_outputs AS o
                     JOIN rebuild_attempts AS a ON a.attempt_id = o.attempt_id
                     JOIN rebuild_tasks AS t ON t.task_id = o.task_id
                     JOIN rebuild_events AS e ON e.event_id = o.event_id
                    WHERE o.task_id != a.task_id
                       OR a.status != 'succeeded'
                       OR t.status != 'succeeded'
                       OR e.run_id != a.run_id
                       OR e.task_id != o.task_id
                       OR e.attempt_id != o.attempt_id
                       OR e.event_type != 'artifact.committed'
                       OR e.artifact_id != o.artifact_id
                    LIMIT 1"#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if invalid_attempt_output.is_some() {
            return Err(StoreError::Integrity(
                "attempt output has invalid terminal-event lineage".to_owned(),
            ));
        }
        let mut statement = connection.prepare(
            "SELECT artifact_id, blob_hash, media_type, bytes FROM rebuild_artifacts ORDER BY artifact_id",
        )?;
        let artifacts = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (artifact_id, hash, media_type, bytes) in artifacts {
            let artifact_id = ArtifactId(ContentHash::new(artifact_id)?);
            self.read_blob(&BlobRef {
                hash: ContentHash::new(hash)?,
                media_type,
                bytes,
            })?;
            let artifact = read_artifact(&connection, &artifact_id)?;
            artifact.validate()?;
            let mut expected = embedded_blob_refs(&connection, &artifact)?
                .into_iter()
                .map(|(role, ordinal, blob)| (role, ordinal, blob.hash))
                .collect::<Vec<_>>();
            expected.sort();
            let mut actual = connection
                .prepare(
                    "SELECT role, ordinal, blob_hash FROM rebuild_embedded_blob_refs WHERE artifact_id = ?1 ORDER BY role, ordinal",
                )?
                .query_map(params![artifact.artifact_id.0.as_str()], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, u64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .map(|row| {
                    let (role, ordinal, hash) = row?;
                    Ok((role, ordinal, ContentHash::new(hash)?))
                })
                .collect::<StoreResult<Vec<_>>>()?;
            actual.sort();
            if actual != expected {
                return Err(StoreError::Integrity(format!(
                    "artifact {} embedded blob index disagrees with payload",
                    artifact.artifact_id
                )));
            }
        }
        let mut statement = connection.prepare(
            "SELECT lease_name, owner_id, epoch, expires_at, heartbeat_at FROM rebuild_daemon_leases ORDER BY lease_name",
        )?;
        let leases = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (lease_name, owner_id, epoch, expires_at, heartbeat_at) in leases {
            if lease_name.trim().is_empty() || owner_id.trim().is_empty() || epoch == 0 {
                return Err(StoreError::Integrity(format!(
                    "invalid daemon lease {lease_name}"
                )));
            }
            let expires_at = parse_time(&expires_at)?;
            if parse_time(&heartbeat_at)? > expires_at {
                return Err(StoreError::Integrity(format!(
                    "daemon lease {lease_name} heartbeat exceeds expiry"
                )));
            }
        }

        let approval_rows = connection
            .prepare(
                "SELECT approval_artifact_id, runtime_manifest_artifact_id, session_key, consumed_at FROM rebuild_paper_approval_consumptions ORDER BY session_key",
            )?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (approval_id, manifest_id, session_key, consumed_at) in approval_rows {
            let approval = read_artifact(&connection, &ArtifactId(ContentHash::new(approval_id)?))?;
            let manifest = read_artifact(&connection, &ArtifactId(ContentHash::new(manifest_id)?))?;
            if approval.kind != ArtifactKind::PaperLaunchApproval
                || manifest.kind != ArtifactKind::RuntimeManifest
                || approval.lifecycle != ArtifactLifecycle::Canonical
                || manifest.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::Integrity(format!(
                    "Paper approval consumption {session_key} has invalid artifact kinds"
                )));
            }
            let manifest_ref = ArtifactRef {
                artifact_id: manifest.artifact_id.clone(),
                kind: ArtifactKind::RuntimeManifest,
            };
            if !has_exact_source_refs(&approval, std::slice::from_ref(&manifest_ref)) {
                return Err(StoreError::Integrity(format!(
                    "Paper approval consumption {session_key} has invalid source closure"
                )));
            }
            let manifest_payload: RuntimeManifest =
                serde_json::from_slice(&self.read_blob(&manifest.blob)?)?;
            let approval_payload: PaperLaunchApproval =
                serde_json::from_slice(&self.read_blob(&approval.blob)?)?;
            manifest_payload.validate()?;
            approval_payload.validate()?;
            let consumed_at = parse_time(&consumed_at)?;
            let session = chrono::NaiveDate::parse_from_str(&session_key, "%Y-%m-%d")
                .map_err(|_| StoreError::InvalidSessionSlot(session_key.clone()))?;
            if approval_payload.runtime_manifest != manifest_ref
                || approval_payload.runtime_manifest_hash != manifest_payload.manifest_hash()?
                || approval_payload.expires_at < consumed_at
                || !manifest_payload.permits(session, consumed_at)
            {
                return Err(StoreError::Integrity(format!(
                    "Paper approval consumption {session_key} is not bound to its manifest"
                )));
            }
        }

        let mut statement = connection.prepare(
            "SELECT session_key, run_id, topology_id, graph_artifact_id, run_created_at, scheduler_epoch, reserved_at, commitment_artifact_id, committed_at FROM rebuild_session_slots ORDER BY session_key",
        )?;
        let slots = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, u64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (
            session_key,
            run_id,
            topology_id,
            graph_artifact_id,
            run_created_at,
            scheduler_epoch,
            reserved_at,
            commitment_artifact_id,
            committed_at,
        ) in slots
        {
            if session_key.trim().is_empty() || scheduler_epoch == 0 {
                return Err(StoreError::Integrity(format!(
                    "invalid session slot {session_key}"
                )));
            }
            let graph_artifact_id = ArtifactId(ContentHash::new(graph_artifact_id)?);
            let graph_artifact = read_artifact(&connection, &graph_artifact_id)?;
            if graph_artifact.kind != ArtifactKind::WorkflowGraph {
                return Err(StoreError::Integrity(format!(
                    "session slot {session_key} graph kind is invalid"
                )));
            }
            let graph: WorkflowGraph =
                serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
            graph.validate()?;
            if graph.topology_id != topology_id {
                return Err(StoreError::Integrity(format!(
                    "session slot {session_key} graph topology mismatch"
                )));
            }
            parse_time(&run_created_at)?;
            parse_time(&reserved_at)?;
            match (commitment_artifact_id, committed_at) {
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => {
                    return Err(StoreError::Integrity(format!(
                        "session slot {session_key} has incomplete commitment state"
                    )));
                }
                (Some(commitment_artifact_id), Some(committed_at)) => {
                    let commitment_artifact_id =
                        ArtifactId(ContentHash::new(commitment_artifact_id)?);
                    let commitment_artifact = read_artifact(&connection, &commitment_artifact_id)?;
                    if commitment_artifact.kind != ArtifactKind::ExecutionCommitment {
                        return Err(StoreError::Integrity(format!(
                            "session slot {session_key} commitment kind is invalid"
                        )));
                    }
                    let payload: PaperCommitment =
                        serde_json::from_slice(&self.read_blob(&commitment_artifact.blob)?)?;
                    payload.validate()?;
                    self.validate_execution_commitment_lineage(
                        &connection,
                        &commitment_artifact,
                        &payload,
                        &RunId(run_id.clone()),
                        &session_key,
                    )
                    .map_err(|error| {
                        StoreError::Integrity(format!(
                            "session slot {session_key} commitment lineage is invalid: {error}"
                        ))
                    })?;
                    parse_time(&committed_at)?;
                }
            }
        }

        let mut statement = connection.prepare(
            "SELECT commitment_artifact_id, asset, reprice_artifact_id, created_at \
             FROM rebuild_execution_reprices ORDER BY commitment_artifact_id, asset",
        )?;
        let reprices = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (commitment_artifact_id, asset, reprice_artifact_id, created_at) in reprices {
            let commitment_artifact_id = ArtifactId(ContentHash::new(commitment_artifact_id)?);
            let reprice_artifact_id = ArtifactId(ContentHash::new(reprice_artifact_id)?);
            let asset = Asset::try_from(asset.as_str())?;
            let commitment_artifact = read_artifact(&connection, &commitment_artifact_id)?;
            let reprice_artifact = read_artifact(&connection, &reprice_artifact_id)?;
            if commitment_artifact.kind != ArtifactKind::ExecutionCommitment
                || reprice_artifact.kind != ArtifactKind::ExecutionReprice
            {
                return Err(StoreError::Integrity(
                    "execution reprice artifact kind is invalid".to_owned(),
                ));
            }
            let commitment: PaperCommitment =
                serde_json::from_slice(&self.read_blob(&commitment_artifact.blob)?)?;
            let reprice: PaperReprice =
                serde_json::from_slice(&self.read_blob(&reprice_artifact.blob)?)?;
            commitment.validate()?;
            reprice.validate()?;
            if reprice.commitment.artifact_id != commitment_artifact_id
                || reprice.asset != asset
                || !reprice_artifact
                    .source_refs
                    .iter()
                    .any(|source| source == &reprice.commitment)
                || !reprice_artifact
                    .source_refs
                    .iter()
                    .any(|source| source == &reprice.prior_receipt)
            {
                return Err(StoreError::Integrity(
                    "execution reprice provenance is invalid".to_owned(),
                ));
            }
            let prior_artifact = read_artifact(&connection, &reprice.prior_receipt.artifact_id)?;
            if prior_artifact.kind != ArtifactKind::OrderReceipt
                || !prior_artifact
                    .source_refs
                    .iter()
                    .any(|source| source == &reprice.commitment)
            {
                return Err(StoreError::Integrity(
                    "execution reprice prior receipt is invalid".to_owned(),
                ));
            }
            let prior: OrderReceipt =
                serde_json::from_slice(&self.read_blob(&prior_artifact.blob)?)?;
            if prior.plan_hash != commitment.plan_hash
                || prior.asset != reprice.asset
                || prior.client_order_id != reprice.prior_client_order_id
                || prior.broker_order_id != reprice.prior_broker_order_id
                || commitment.client_order_ids.get(&reprice.asset)
                    != Some(&reprice.prior_client_order_id)
            {
                return Err(StoreError::Integrity(
                    "execution reprice receipt lineage is invalid".to_owned(),
                ));
            }
            let durable = connection
                .query_row(
                    "SELECT 1 FROM rebuild_session_slots \
                     WHERE commitment_artifact_id = ?1",
                    params![commitment_artifact_id.0.as_str()],
                    |_| Ok(()),
                )
                .optional()?;
            if durable.is_none() {
                return Err(StoreError::Integrity(
                    "execution reprice commitment is not durable".to_owned(),
                ));
            }
            parse_time(&created_at)?;
        }

        let mut statement = connection.prepare(
            "SELECT subject_id, state_json, revision, transition_id, \
                    transition_event_cursor, updated_at \
             FROM rebuild_policy_heads ORDER BY subject_id",
        )?;
        let heads = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (subject_id, state_json, revision, transition_id, transition_cursor, updated_at) in
            heads
        {
            if subject_id.trim().is_empty() || revision == 0 {
                return Err(StoreError::Integrity(format!(
                    "policy head {subject_id} is invalid"
                )));
            }
            let state: PolicyState = serde_json::from_str(&state_json)?;
            let transition =
                read_policy_transition(&connection, &PolicyTransitionId(transition_id.clone()))?
                    .ok_or_else(|| {
                        StoreError::Integrity(format!(
                    "policy head {subject_id} references missing transition {transition_id}"
                ))
                    })?;
            if transition.transition.subject.subject_id() != subject_id
                || transition.revision != revision
                || transition.transition.to != state
                || transition.transition_cursor != transition_cursor
                || transition.transition.created_at != parse_time(&updated_at)?
            {
                return Err(StoreError::Integrity(format!(
                    "policy head {subject_id} disagrees with its transition"
                )));
            }
            let latest = connection.query_row(
                "SELECT transition_id, revision FROM rebuild_policy_transitions \
                 WHERE subject_id = ?1 ORDER BY revision DESC LIMIT 1",
                params![subject_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
            )?;
            if latest != (transition_id.clone(), revision) {
                return Err(StoreError::Integrity(format!(
                    "policy head {subject_id} is stale"
                )));
            }
            let evaluation =
                read_artifact(&connection, &transition.transition.evaluation.artifact_id)?;
            if evaluation.kind != ArtifactKind::Evaluation
                || artifact_run_purpose(&connection, &evaluation)? != RunPurpose::Paper
            {
                return Err(StoreError::Integrity(format!(
                    "policy transition {transition_id} is not Paper-backed"
                )));
            }
        }
        let orphan_transition = connection
            .query_row(
                r#"SELECT t.transition_id FROM rebuild_policy_transitions AS t
                   LEFT JOIN rebuild_policy_heads AS h ON h.subject_id = t.subject_id
                   WHERE h.subject_id IS NULL LIMIT 1"#,
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(transition_id) = orphan_transition {
            return Err(StoreError::Integrity(format!(
                "policy transition {transition_id} has no head"
            )));
        }

        let mut statement =
            connection.prepare("SELECT pair_key FROM rebuild_shadow_pairs ORDER BY pair_key")?;
        let pair_keys = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for value in pair_keys {
            let pair_key = ContentHash::new(value)?;
            let pair = read_shadow_pair(&connection, &pair_key)?.ok_or_else(|| {
                StoreError::Integrity(format!("shadow pair {pair_key} disappeared"))
            })?;
            pair.completion.validate()?;
            if pair.completion.pair_key()? != pair_key {
                return Err(StoreError::Integrity(format!(
                    "shadow pair {pair_key} key mismatch"
                )));
            }
            self.assert_shadow_pair_sources_with_connection(&connection, &pair.completion)
                .map_err(|error| {
                    StoreError::Integrity(format!(
                        "shadow pair {pair_key} lineage is invalid: {error}"
                    ))
                })?;
        }

        let orphan = connection
            .query_row(
                r#"SELECT t.task_id FROM rebuild_tasks AS t
                    LEFT JOIN rebuild_runs AS r ON r.run_id = t.run_id
                    WHERE r.run_id IS NULL LIMIT 1"#,
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(task_id) = orphan {
            return Err(StoreError::Integrity(format!("task {task_id} has no run")));
        }
        let run_ids = connection
            .prepare("SELECT run_id FROM rebuild_runs ORDER BY run_id")?
            .query_map([], |row| Ok(RunId(row.get(0)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        for run_id in run_ids {
            let snapshot = self.workflow_snapshot_with_connection(&connection, &run_id)?;
            self.verify_workflow_history(&connection, &snapshot)?;
        }
        self.verify_outcome_schedule_history(&connection)?;
        self.verify_contract_catalogue_history(&connection)?;
        self.verify_policy_evaluation_history(&connection)?;
        self.verify_candidate_policy_history(&connection)?;
        self.verify_lesson_history(&connection)?;
        Ok(())
    }

    /// Exercise Store Doctor against a corrupted temporary SQLite snapshot.
    /// The active Store is read-only throughout the diagnostic.
    pub fn diagnose_corruption_rejection(&self, artifact_id: &ArtifactId) -> StoreResult<bool> {
        let artifact = self.artifact(artifact_id)?;
        let temporary = std::env::temp_dir().join(format!(
            "akzio-corruption-diagnostic-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        self.backup_to(&temporary)?;
        let database = temporary.join(DATABASE_FILE);
        {
            let connection = Connection::open(&database)?;
            connection.execute(
                "UPDATE rebuild_blobs SET payload = X'00' WHERE blob_hash = ?1",
                params![artifact.blob.hash.as_str()],
            )?;
        }
        let rejected = Self::open_existing(&temporary)
            .and_then(|store| store.verify_integrity())
            .is_err();
        fs::remove_dir_all(&temporary).map_err(|source| StoreError::Io {
            path: temporary,
            source,
        })?;
        Ok(rejected)
    }
}
