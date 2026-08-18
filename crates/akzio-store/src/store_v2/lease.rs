use super::*;

impl V2Store {
    /// Commits the frozen workflow graph, Run row, nodes, dependencies, and creation
    /// event as one transaction. A process cannot observe a half-submitted graph.
    /// Atomically installs the approved Paper workflow, its proposal, its
    /// run-scoped inputs, and the broker session slot.
    pub fn reserve_paper_session_with_proposal(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
    ) -> StoreResult<SessionSlotReservation> {
        self.reserve_paper_session_with_binding(lease, reservation, proposal, None)
    }

    pub fn reserve_paper_session_with_approval(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        runtime_manifest: &Artifact,
        approval: &Artifact,
    ) -> StoreResult<SessionSlotReservation> {
        if runtime_manifest.kind != ArtifactKind::RuntimeManifest
            || approval.kind != ArtifactKind::PaperLaunchApproval
            || runtime_manifest.lifecycle != ArtifactLifecycle::Canonical
            || approval.lifecycle != ArtifactLifecycle::Canonical
            || approval.source_refs
                != vec![ArtifactRef {
                    artifact_id: runtime_manifest.artifact_id.clone(),
                    kind: ArtifactKind::RuntimeManifest,
                }]
        {
            return Err(StoreError::InvalidSessionSlot(
                reservation.session_key.clone(),
            ));
        }
        runtime_manifest.validate()?;
        approval.validate()?;
        let manifest_payload: RuntimeManifest =
            serde_json::from_slice(&self.read_blob(&runtime_manifest.blob)?)?;
        let approval_payload: PaperLaunchApproval =
            serde_json::from_slice(&self.read_blob(&approval.blob)?)?;
        manifest_payload.validate()?;
        approval_payload.validate()?;
        let session = chrono::NaiveDate::parse_from_str(&reservation.session_key, "%Y-%m-%d")
            .map_err(|_| StoreError::InvalidSessionSlot(reservation.session_key.clone()))?;
        if approval_payload.runtime_manifest.artifact_id != runtime_manifest.artifact_id
            || approval_payload.runtime_manifest_hash != manifest_payload.manifest_hash()?
            || !manifest_payload.permits(session, reservation.reserved_at)
            || approval_payload.expires_at < reservation.reserved_at
        {
            return Err(StoreError::InvalidSessionSlot(
                reservation.session_key.clone(),
            ));
        }
        self.reserve_paper_session_with_binding(
            lease,
            reservation,
            proposal,
            Some((runtime_manifest, approval)),
        )
    }

    /// Atomically elect one daemon scheduler. A successor always receives a
    /// higher epoch so stale leaders cannot mutate a Paper session slot.
    pub fn acquire_daemon_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<Option<DaemonLease>> {
        if lease_name.trim().is_empty() || owner_id.trim().is_empty() || expires_at <= now {
            return Err(StoreError::InvalidDaemonLease(lease_name.to_owned()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = transaction
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1",
                params![lease_name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?, row.get::<_, String>(2)?)),
            )
            .optional()?;
        let lease = match current {
            None => {
                transaction.execute(
                    "INSERT INTO rebuild_daemon_leases (lease_name, owner_id, epoch, expires_at, heartbeat_at) VALUES (?1, ?2, 1, ?3, ?4)",
                    params![lease_name, owner_id, expires_at.to_rfc3339(), now.to_rfc3339()],
                )?;
                DaemonLease {
                    lease_name: lease_name.to_owned(),
                    owner_id: owner_id.to_owned(),
                    epoch: 1,
                    expires_at,
                }
            }
            Some((_, _, current_expires_at)) if parse_time(&current_expires_at)? > now => {
                transaction.commit()?;
                return Ok(None);
            }
            Some((_, epoch, _)) => {
                let epoch = epoch.saturating_add(1);
                transaction.execute(
                    "UPDATE rebuild_daemon_leases SET owner_id = ?1, epoch = ?2, expires_at = ?3, heartbeat_at = ?4 WHERE lease_name = ?5",
                    params![owner_id, epoch, expires_at.to_rfc3339(), now.to_rfc3339(), lease_name],
                )?;
                DaemonLease {
                    lease_name: lease_name.to_owned(),
                    owner_id: owner_id.to_owned(),
                    epoch,
                    expires_at,
                }
            }
        };
        transaction.commit()?;
        Ok(Some(lease))
    }

    pub fn heartbeat_daemon_lease(
        &self,
        lease: &DaemonLease,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<bool> {
        if expires_at <= now {
            return Err(StoreError::InvalidDaemonLease(lease.lease_name.clone()));
        }
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE rebuild_daemon_leases SET expires_at = ?1, heartbeat_at = ?2 WHERE lease_name = ?3 AND owner_id = ?4 AND epoch = ?5 AND expires_at > ?2",
            params![expires_at.to_rfc3339(), now.to_rfc3339(), lease.lease_name, lease.owner_id, lease.epoch],
        )?;
        Ok(changed == 1)
    }

    pub fn release_daemon_lease(&self, lease: &DaemonLease) -> StoreResult<bool> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "DELETE FROM rebuild_daemon_leases WHERE lease_name = ?1 AND owner_id = ?2 AND epoch = ?3",
            params![lease.lease_name, lease.owner_id, lease.epoch],
        )?;
        Ok(changed == 1)
    }

    pub fn daemon_lease(&self, lease_name: &str) -> StoreResult<Option<DaemonLease>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT owner_id, epoch, expires_at FROM rebuild_daemon_leases WHERE lease_name = ?1",
                params![lease_name],
                |row| {
                    Ok(DaemonLease {
                        lease_name: lease_name.to_owned(),
                        owner_id: row.get(0)?,
                        epoch: row.get(1)?,
                        expires_at: parse_time(&row.get::<_, String>(2)?).map_err(|error| {
                            rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Verify that the caller still owns the current, unexpired daemon epoch.
    /// Broker adapters call this immediately before external Paper I/O.
    pub fn validate_daemon_lease(
        &self,
        lease: &DaemonLease,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        assert_daemon_lease(&transaction, lease, now)?;
        transaction.commit()?;
        Ok(())
    }

    /// Freeze the exact Paper graph before its Run is installed. A duplicate
    /// session returns the original graph and task IDs without recording the
    /// caller's replacement proposal.
    pub fn reserve_session_slot(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
    ) -> StoreResult<SessionSlotReservation> {
        if reservation.session_key.trim().is_empty()
            || reservation.workflow.run.purpose != RunPurpose::Paper
            || reservation.workflow.graph.kind != ArtifactKind::WorkflowGraph
            || reservation.workflow.graph.artifact_id != reservation.workflow.run.graph_artifact_id
        {
            return Err(StoreError::InvalidSessionSlot(
                reservation.session_key.clone(),
            ));
        }
        reservation.workflow.graph.validate()?;
        let graph: WorkflowGraph =
            serde_json::from_slice(&self.read_blob(&reservation.workflow.graph.blob)?)?;
        graph.validate()?;
        if graph.nodes != reservation.workflow.nodes
            || graph.topology_id != reservation.workflow.run.topology_id
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        for artifact in &reservation.setup_artifacts {
            artifact.validate()?;
            if artifact.kind != ArtifactKind::EvidenceNeed
                || artifact.lifecycle != ArtifactLifecycle::RunScoped
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&reservation.workflow.run.run_id)
            {
                return Err(StoreError::InvalidSessionSlot(
                    reservation.session_key.clone(),
                ));
            }
            self.read_blob(&artifact.blob)?;
        }

        let newly_reserved = {
            let mut connection = self.connection()?;
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            assert_daemon_lease(&transaction, lease, reservation.reserved_at)?;
            let exists = transaction
                .query_row(
                    "SELECT 1 FROM rebuild_session_slots WHERE session_key = ?1",
                    params![reservation.session_key],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            if exists.is_some() {
                transaction.commit()?;
                false
            } else {
                for artifact in &reservation.setup_artifacts {
                    insert_artifact(&transaction, artifact)?;
                }
                Self::commit_workflow_transaction(&transaction, &reservation.workflow)?;
                transaction.execute(
                    "INSERT INTO rebuild_session_slots (session_key, run_id, topology_id, graph_artifact_id, run_created_at, scheduler_epoch, reserved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        reservation.session_key,
                        reservation.workflow.run.run_id.0,
                        reservation.workflow.run.topology_id,
                        reservation.workflow.run.graph_artifact_id.0.as_str(),
                        reservation.workflow.run.created_at.to_rfc3339(),
                        lease.epoch,
                        reservation.reserved_at.to_rfc3339(),
                    ],
                )?;
                transaction.commit()?;
                true
            }
        };
        let slot = self
            .session_slot(&reservation.session_key)?
            .ok_or_else(|| StoreError::Integrity("session slot disappeared".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved,
        })
    }

    pub fn session_slot(&self, session_key: &str) -> StoreResult<Option<SessionSlot>> {
        let row = {
            let connection = self.connection()?;
            connection
                .query_row(
                    "SELECT run_id, topology_id, graph_artifact_id, run_created_at, scheduler_epoch, reserved_at, commitment_artifact_id, committed_at FROM rebuild_session_slots WHERE session_key = ?1",
                    params![session_key],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, u64>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, Option<String>>(6)?,
                            row.get::<_, Option<String>>(7)?,
                        ))
                    },
                )
                .optional()?
        };
        row.map(
            |(
                run_id,
                topology_id,
                graph_artifact_id,
                run_created_at,
                scheduler_epoch,
                reserved_at,
                commitment_artifact_id,
                committed_at,
            )| {
                let graph_artifact_id = ArtifactId(ContentHash::new(graph_artifact_id)?);
                let graph_artifact = self.artifact(&graph_artifact_id)?;
                if graph_artifact.kind != ArtifactKind::WorkflowGraph {
                    return Err(StoreError::InvalidSessionSlot(session_key.to_owned()));
                }
                let graph: WorkflowGraph =
                    serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
                graph.validate()?;
                if graph.topology_id != topology_id {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
                Ok(SessionSlot {
                    session_key: session_key.to_owned(),
                    workflow: WorkflowCommit {
                        run: StoredRun {
                            run_id: RunId(run_id),
                            purpose: RunPurpose::Paper,
                            topology_id,
                            graph_artifact_id,
                            created_at: parse_time(&run_created_at)?,
                        },
                        graph: graph_artifact,
                        nodes: graph.nodes,
                    },
                    scheduler_epoch,
                    reserved_at: parse_time(&reserved_at)?,
                    commitment_artifact_id: commitment_artifact_id
                        .map(ContentHash::new)
                        .transpose()?
                        .map(ArtifactId),
                    committed_at: committed_at.as_deref().map(parse_time).transpose()?,
                })
            },
        )
        .transpose()
    }

    pub fn paper_approval_for_run(
        &self,
        run_id: &RunId,
    ) -> StoreResult<Option<(RuntimeManifest, PaperLaunchApproval)>> {
        let row = {
            let connection = self.connection()?;
            connection
                .query_row(
                    "SELECT c.runtime_manifest_artifact_id, c.approval_artifact_id FROM rebuild_paper_approval_consumptions c JOIN rebuild_session_slots s ON s.session_key = c.session_key WHERE s.run_id = ?1",
                    params![run_id.0],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?
        };
        let Some((manifest_id, approval_id)) = row else {
            return Ok(None);
        };
        let manifest_artifact = self.artifact(&ArtifactId(ContentHash::new(manifest_id)?))?;
        let approval_artifact = self.artifact(&ArtifactId(ContentHash::new(approval_id)?))?;
        let manifest: RuntimeManifest =
            serde_json::from_slice(&self.read_blob(&manifest_artifact.blob)?)?;
        let approval: PaperLaunchApproval =
            serde_json::from_slice(&self.read_blob(&approval_artifact.blob)?)?;
        manifest.validate()?;
        approval.validate()?;
        if approval.runtime_manifest.artifact_id != manifest_artifact.artifact_id
            || approval.runtime_manifest_hash != manifest.manifest_hash()?
        {
            return Err(StoreError::InvalidSessionSlot(run_id.0.clone()));
        }
        Ok(Some((manifest, approval)))
    }

    /// Durably reserve the single broker-visible commitment for a Paper
    /// session and terminally completes the active task attempt in the same
    /// transaction. A crash therefore cannot leave a committed session slot
    /// paired with an active commitment task.
    /// Returns the frozen broker-session slot for one scheduler-owned Paper
    /// run. A run may never have more than one such slot.
    pub fn session_slot_for_run(&self, run_id: &RunId) -> StoreResult<Option<SessionSlot>> {
        let session_key = {
            let connection = self.connection()?;
            connection
                .query_row(
                    "SELECT session_key FROM rebuild_session_slots WHERE run_id = ?1",
                    params![run_id.0],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
        };
        Ok(session_key
            .as_deref()
            .map(|session_key| self.session_slot(session_key))
            .transpose()?
            .flatten())
    }
}
