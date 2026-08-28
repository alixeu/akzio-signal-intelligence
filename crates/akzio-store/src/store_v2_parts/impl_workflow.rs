impl V2Store {
    fn reserve_paper_session_with_binding(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        binding: Option<(&Artifact, &Artifact)>,
    ) -> StoreResult<SessionSlotReservation> {
        if reservation.session_key.trim().is_empty()
            || reservation.workflow.run.purpose != RunPurpose::Paper
            || reservation.workflow.graph.kind != ArtifactKind::WorkflowGraph
            || reservation.workflow.graph.artifact_id != reservation.workflow.run.graph_artifact_id
            || proposal.kind != ArtifactKind::WorkflowProposal
            || proposal.producer != "runtime.paper_provisioning"
            || proposal.lifecycle != ArtifactLifecycle::RunScoped
            || proposal
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&reservation.workflow.run.run_id)
            || reservation.setup_artifacts.iter().any(|artifact| {
                artifact.kind != ArtifactKind::EvidenceNeed
                    || artifact.lifecycle != ArtifactLifecycle::RunScoped
                    || artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref())
                        != Some(&reservation.workflow.run.run_id)
            })
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
        let proposal_payload: WorkflowProposal =
            serde_json::from_slice(&self.read_blob(&proposal.blob)?)?;
        let expected_sources = reservation
            .setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            })
            .collect::<BTreeSet<_>>();
        let actual_sources = proposal
            .source_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let payload_needs = proposal_payload
            .tasks
            .values()
            .flat_map(|task| task.evidence_needs.iter().cloned())
            .collect::<BTreeSet<_>>();
        let expected_sources = expected_sources
            .into_iter()
            .chain(payload_needs)
            .collect::<BTreeSet<_>>();
        if actual_sources != expected_sources {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        if proposal_payload.topology_id != reservation.workflow.run.topology_id {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        proposal.validate()?;
        for artifact in &reservation.setup_artifacts {
            artifact.validate()?;
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
                insert_artifact(&transaction, proposal)?;
                if let Some((runtime_manifest, approval)) = binding {
                    insert_artifact(&transaction, runtime_manifest)?;
                    insert_artifact(&transaction, approval)?;
                }
                Self::commit_workflow_transaction(&transaction, &reservation.workflow)?;
                Self::append_session_setup_events(&transaction, reservation, Some(proposal))?;
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
                if let Some((runtime_manifest, approval)) = binding {
                    transaction.execute(
                        "INSERT INTO rebuild_paper_approval_consumptions (approval_artifact_id, runtime_manifest_artifact_id, session_key, consumed_at) VALUES (?1, ?2, ?3, ?4)",
                        params![
                            approval.artifact_id.0.as_str(),
                            runtime_manifest.artifact_id.0.as_str(),
                            reservation.session_key,
                            reservation.reserved_at.to_rfc3339(),
                        ],
                    )?;
                }
                transaction.commit()?;
                true
            }
        };
        let slot = self
            .session_slot(&reservation.session_key)?
            .ok_or_else(|| StoreError::Integrity("session slot missing after commit".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved,
        })
    }

    pub(super) fn commit_session_slot_transaction(
        transaction: &Transaction<'_>,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        binding: Option<(&Artifact, &Artifact)>,
    ) -> StoreResult<()> {
        assert_daemon_lease(transaction, lease, reservation.reserved_at)?;
        for artifact in &reservation.setup_artifacts {
            insert_artifact(transaction, artifact)?;
        }
        insert_artifact(transaction, proposal)?;
        if let Some((runtime_manifest, approval)) = binding {
            insert_artifact(transaction, runtime_manifest)?;
            insert_artifact(transaction, approval)?;
        }
        Self::commit_workflow_transaction(transaction, &reservation.workflow)?;
        Self::append_session_setup_events(transaction, reservation, Some(proposal))?;
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
        if let Some((runtime_manifest, approval)) = binding {
            transaction.execute(
                "INSERT INTO rebuild_paper_approval_consumptions (approval_artifact_id, runtime_manifest_artifact_id, session_key, consumed_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    approval.artifact_id.0.as_str(),
                    runtime_manifest.artifact_id.0.as_str(),
                    reservation.session_key,
                    reservation.reserved_at.to_rfc3339(),
                ],
            )?;
        }
        Ok(())
    }

    /// Record run-level facts for the artifacts a session reservation writes
    /// before any task row exists: the scheduler's session-scoped
    /// `EvidenceNeed`s and, where present, the frozen `WorkflowProposal`. They
    /// are bound to the run, so without these events the Doctor cannot observe
    /// them in the run's event log at all.
    pub(super) fn append_session_setup_events(
        transaction: &Transaction<'_>,
        reservation: &SessionReservation,
        proposal: Option<&Artifact>,
    ) -> StoreResult<()> {
        for artifact in &reservation.setup_artifacts {
            append_event(
                transaction,
                &reservation.workflow.run.run_id,
                None,
                None,
                LifecycleEventType::SchedulerSnapshotNeedCreated,
                Some(&artifact.artifact_id),
                reservation.reserved_at,
            )?;
        }
        if let Some(proposal) = proposal {
            append_event(
                transaction,
                &reservation.workflow.run.run_id,
                None,
                None,
                LifecycleEventType::SchedulerWorkflowProposalCreated,
                Some(&proposal.artifact_id),
                reservation.reserved_at,
            )?;
        }
        Ok(())
    }

    fn commit_workflow_transaction(
        transaction: &Transaction<'_>,
        commit: &WorkflowCommit,
    ) -> StoreResult<()> {
        assert_workflow_input_artifacts(transaction, &commit.nodes)?;
        insert_artifact(transaction, &commit.graph)?;
        let inserted = transaction.execute(
            r#"INSERT INTO rebuild_runs
                (run_id, purpose, topology_id, graph_artifact_id, status, created_at)
                VALUES (?1, ?2, ?3, ?4, 'queued', ?5)"#,
            params![
                commit.run.run_id.0,
                enum_name(commit.run.purpose),
                commit.run.topology_id,
                commit.run.graph_artifact_id.0.as_str(),
                commit.run.created_at.to_rfc3339(),
            ],
        )?;
        if inserted != 1 {
            return Err(StoreError::DuplicateRun(commit.run.run_id.clone()));
        }
        for node in &commit.nodes {
            insert_task_node(transaction, &commit.run.run_id, node, commit.run.created_at)?;
        }
        for node in &commit.nodes {
            insert_node_dependencies(transaction, node)?;
        }
        transaction.execute(
            r#"INSERT INTO rebuild_workflow_revisions
                (run_id, revision, graph_artifact_id, created_at)
                VALUES (?1, 0, ?2, ?3)"#,
            params![
                commit.run.run_id.0,
                commit.run.graph_artifact_id.0.as_str(),
                commit.run.created_at.to_rfc3339(),
            ],
        )?;
        append_event(
            transaction,
            &commit.run.run_id,
            None,
            None,
            LifecycleEventType::WorkflowCreated,
            Some(&commit.graph.artifact_id),
            commit.run.created_at,
        )?;
        Ok(())
    }

    fn validate_execution_commitment_lineage(
        &self,
        connection: &Connection,
        commitment_artifact: &Artifact,
        commitment: &PaperCommitment,
        run_id: &RunId,
        session_key: &str,
    ) -> StoreResult<ExecutionPlan> {
        let invalid = || StoreError::InvalidSessionSlot(session_key.to_owned());
        if commitment_artifact.kind != ArtifactKind::ExecutionCommitment
            || commitment_artifact.lifecycle != ArtifactLifecycle::Canonical
            || commitment.broker_session != session_key
            || commitment_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }

        let verdict_refs = commitment_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionVerdict)
            .cloned()
            .collect::<Vec<_>>();
        let context_refs = commitment_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionContext)
            .cloned()
            .collect::<Vec<_>>();
        if verdict_refs.len() != 1
            || context_refs.len() != 1
            || context_refs[0] != commitment.execution_context
            || !has_exact_source_refs(
                commitment_artifact,
                &[verdict_refs[0].clone(), context_refs[0].clone()],
            )
        {
            return Err(invalid());
        }

        let context_ref = &context_refs[0];
        let verdict_artifact = read_artifact(connection, &verdict_refs[0].artifact_id)?;
        if verdict_artifact.kind != ArtifactKind::ExecutionVerdict
            || verdict_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            || !has_exact_source_refs(&verdict_artifact, std::slice::from_ref(context_ref))
        {
            return Err(invalid());
        }
        let verdict: ExecutionVerdict =
            serde_json::from_slice(&self.read_blob(&verdict_artifact.blob)?)?;
        let ExecutionVerdict::Accepted { execution_context } = verdict else {
            return Err(invalid());
        };
        if execution_context != *context_ref {
            return Err(invalid());
        }

        let context_artifact = read_artifact(connection, &context_ref.artifact_id)?;
        if context_artifact.kind != ArtifactKind::ExecutionContext
            || context_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }
        let context: ExecutionContext =
            serde_json::from_slice(&self.read_blob(&context_artifact.blob)?)?;
        context.validate_complete_plan_closure()?;
        if context.run_id != *run_id
            || context.broker_session.as_deref() != Some(session_key)
            || context.plan_hash.as_ref() != Some(&commitment.plan_hash)
        {
            return Err(invalid());
        }

        let context_sources = [
            context.decision_context.clone(),
            context.account_snapshot.clone().expect("validated closure"),
            context.quote_snapshot.clone().expect("validated closure"),
            context
                .market_clock_snapshot
                .clone()
                .expect("validated closure"),
            context.execution_plan.clone().expect("validated closure"),
        ];
        if !has_exact_source_refs(&context_artifact, &context_sources) {
            return Err(invalid());
        }

        let plan_refs = context_artifact
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ExecutionPlan)
            .collect::<Vec<_>>();
        if plan_refs.len() != 1 {
            return Err(invalid());
        }
        if context.execution_plan.as_ref() != Some(plan_refs[0]) {
            return Err(invalid());
        }
        let plan_artifact = read_artifact(connection, &plan_refs[0].artifact_id)?;
        if plan_artifact.kind != ArtifactKind::ExecutionPlan
            || plan_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(invalid());
        }
        let plan: ExecutionPlan = serde_json::from_slice(&self.read_blob(&plan_artifact.blob)?)?;
        plan.validate()?;
        if !has_exact_source_refs(
            &plan_artifact,
            &[
                plan.decision_context.clone(),
                plan.account_snapshot.clone(),
                plan.quote_snapshot.clone(),
                plan.market_clock_snapshot.clone(),
            ],
        ) || plan.decision_context != context.decision_context
            || Some(&plan.account_snapshot) != context.account_snapshot.as_ref()
            || Some(&plan.quote_snapshot) != context.quote_snapshot.as_ref()
            || Some(&plan.market_clock_snapshot) != context.market_clock_snapshot.as_ref()
            || plan.broker_session != session_key
            || context.plan_hash.as_ref() != Some(&plan.plan_hash)
            || plan.plan_hash != commitment.plan_hash
        {
            return Err(invalid());
        }
        Ok(plan)
    }

    fn validate_consumed_paper_approval(
        &self,
        connection: &Connection,
        session_key: &str,
        plan: &ExecutionPlan,
        committed_at: DateTime<Utc>,
    ) -> StoreResult<()> {
        let invalid = || StoreError::InvalidSessionSlot(session_key.to_owned());
        let binding = connection
            .query_row(
                "SELECT runtime_manifest_artifact_id, approval_artifact_id FROM rebuild_paper_approval_consumptions WHERE session_key = ?1",
                params![session_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((manifest_id, approval_id)) = binding else {
            return Err(invalid());
        };
        let manifest_artifact =
            read_artifact(connection, &ArtifactId(ContentHash::new(manifest_id)?))?;
        let approval_artifact =
            read_artifact(connection, &ArtifactId(ContentHash::new(approval_id)?))?;
        let (manifest, approval) =
            self.validate_paper_approval_binding(&manifest_artifact, &approval_artifact)?;
        let session =
            chrono::NaiveDate::parse_from_str(session_key, "%Y-%m-%d").map_err(|_| invalid())?;
        let total_notional = plan.orders.iter().try_fold(0_i64, |total, order| {
            total.checked_add(order.notional.0).ok_or_else(invalid)
        })?;
        if !manifest.permits(session, committed_at)
            || approval.expires_at < committed_at
            || total_notional > manifest.maximum_notional.0
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn validate_specialized_artifact(&self, artifact: &Artifact) -> StoreResult<()> {
        match artifact.kind {
            ArtifactKind::DeliberationNote => {
                let summary: akzio_domain::DeliberationSummary =
                    self.read_artifact_payload(artifact)?;
                summary.validate()?;
            }
            ArtifactKind::RetrospectiveDraft => {
                let draft: RetrospectiveDraft = self.read_artifact_payload(artifact)?;
                draft.validate()?;
                if artifact.lifecycle != ArtifactLifecycle::RunScoped {
                    return Err(StoreError::InvalidLearningCommit(
                        "retrospective_draft.lifecycle",
                    ));
                }
                let run_id = artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    .ok_or(StoreError::PermitOriginMismatch)?;
                for source in &artifact.source_refs {
                    let source_artifact = self.artifact(&source.artifact_id)?;
                    if source_artifact
                        .origin
                        .as_ref()
                        .and_then(|origin| origin.run_id.as_ref())
                        .is_some_and(|source_run| source_run != run_id)
                    {
                        return Err(StoreError::InvalidLearningCommit(
                            "retrospective_draft.cross_run_source",
                        ));
                    }
                }
            }
            ArtifactKind::Retrospective => {
                let retrospective: Retrospective = self.read_artifact_payload(artifact)?;
                retrospective.validate()?;
            }
            ArtifactKind::AttemptRelation => {
                let relation: AttemptRelation = self.read_artifact_payload(artifact)?;
                relation.validate()?;
                if artifact.lifecycle != ArtifactLifecycle::RunScoped {
                    return Err(StoreError::InvalidLearningCommit(
                        "attempt_relation.lifecycle",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_paper_effect_artifact(
        &self,
        effect: &ArtifactRef,
        run_id: &RunId,
    ) -> StoreResult<()> {
        let artifact = self.artifact(&effect.artifact_id)?;
        if effect.kind != artifact.kind
            || !matches!(
                artifact.kind,
                ArtifactKind::ExecutionCommitment | ArtifactKind::ExecutionReprice
            )
            || artifact.lifecycle != ArtifactLifecycle::Canonical
            || artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
        {
            return Err(StoreError::InvalidPaperEffect(effect.artifact_id.clone()));
        }
        match artifact.kind {
            ArtifactKind::ExecutionCommitment => {
                let payload: PaperCommitment =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                payload.validate()?;
            }
            ArtifactKind::ExecutionReprice => {
                let payload: PaperReprice =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                payload.validate()?;
            }
            _ => unreachable!("validated Paper effect kind"),
        }
        Ok(())
    }

    fn validate_attempt_commit(
        &self,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
    ) -> StoreResult<()> {
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        if status == TaskStatus::Succeeded && artifacts.is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "commit_attempt.artifacts",
            }));
        }
        for artifact in artifacts {
            artifact.validate()?;
            reject_generic_learning_artifact(artifact)?;
            self.read_blob(&artifact.blob)?;
            self.validate_specialized_artifact(artifact)?;
        }
        Ok(())
    }
}
