use super::*;

impl V2Store {
    pub fn active_contract(
        &self,
        purpose: &ContractPurpose,
    ) -> StoreResult<Option<StoredContract>> {
        let connection = self.connection()?;
        let Some((contract_hash, _)) = contract_catalogue_head(&connection, purpose)? else {
            return Ok(None);
        };
        self.stored_contract_with_connection(&connection, &contract_hash)
    }

    /// Return an installed Contract, whether it is an active head or a bounded
    /// candidate awaiting Paper-backed promotion.
    pub fn contract_installation(
        &self,
        contract_hash: &ContentHash,
    ) -> StoreResult<Option<StoredContract>> {
        let connection = self.connection()?;
        self.stored_contract_with_connection(&connection, contract_hash)
    }

    /// Install the first Rust-defined active Contract for a purpose. A later
    /// version must enter through `install_candidate_contract` and a canonical
    /// policy transition; this prevents a restart from silently replacing it.
    pub fn install_active_contract(
        &self,
        contract: &AgentContract,
        now: DateTime<Utc>,
    ) -> StoreResult<StoredContract> {
        contract.validate()?;
        let artifact = self.contract_artifact(contract, now)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) =
            self.stored_contract_with_connection(&transaction, &contract.contract_hash)?
        {
            if existing.contract != *contract || existing.activated_at.is_none() {
                return Err(StoreError::ContractActivationConflict(
                    contract.purpose.clone(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }
        assert_contract_identity_available(&transaction, contract)?;
        if contract_catalogue_head(&transaction, &contract.purpose)?.is_some() {
            return Err(StoreError::ContractActivationConflict(
                contract.purpose.clone(),
            ));
        }
        insert_artifact(&transaction, &artifact)?;
        insert_contract_installation(&transaction, contract, &artifact, None, now)?;
        let activation_id = append_contract_activation(
            &transaction,
            &contract.purpose,
            None,
            &contract.contract_hash,
            None,
            now,
        )?;
        set_contract_catalogue_head(
            &transaction,
            &contract.purpose,
            &contract.contract_hash,
            activation_id,
        )?;
        transaction.commit()?;
        drop(connection);
        self.contract_installation(&contract.contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(contract.contract_hash.clone()))
    }

    /// Activate an explicitly versioned Rust canonical Contract upgrade without
    /// mutating the prior installation. Capability expansion remains forbidden,
    /// and the activation history records no learning PolicyTransition.
    pub fn install_canonical_contract_upgrade(
        &self,
        active_contract_hash: &ContentHash,
        contract: &AgentContract,
        now: DateTime<Utc>,
    ) -> StoreResult<StoredContract> {
        contract.validate()?;
        let artifact = self.contract_artifact(contract, now)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = self
            .stored_contract_with_connection(&transaction, active_contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(active_contract_hash.clone()))?;
        let current_head =
            contract_catalogue_head(&transaction, &active.contract.purpose)?.map(|(hash, _)| hash);
        if current_head.as_ref() != Some(active_contract_hash)
            || active.activated_at.is_none()
            || contract.contract_id != active.contract.contract_id
            || contract.purpose != active.contract.purpose
            || contract.version <= active.contract.version
        {
            return Err(StoreError::ContractActivationConflict(
                contract.purpose.clone(),
            ));
        }
        if !candidate_is_bounded(&active.contract, contract) {
            return Err(StoreError::ContractCapabilityExpansion {
                active: active_contract_hash.clone(),
                candidate: contract.contract_hash.clone(),
            });
        }
        let blockers = contract_upgrade_blockers(&transaction, active_contract_hash)?;
        if !blockers.is_empty() {
            return Err(StoreError::ContractUpgradeBlocked {
                active: active_contract_hash.clone(),
                blockers: blockers.join(", "),
            });
        }

        match self.stored_contract_with_connection(&transaction, &contract.contract_hash)? {
            Some(existing)
                if existing.contract == *contract
                    && existing.baseline_contract_hash.as_ref() == Some(active_contract_hash)
                    && existing.activated_at.is_none() => {}
            Some(_) => {
                return Err(StoreError::ContractActivationConflict(
                    contract.purpose.clone(),
                ));
            }
            None => {
                assert_contract_identity_available(&transaction, contract)?;
                insert_artifact(&transaction, &artifact)?;
                insert_contract_installation(
                    &transaction,
                    contract,
                    &artifact,
                    Some(active_contract_hash),
                    now,
                )?;
            }
        }
        let activation_id = append_contract_activation(
            &transaction,
            &contract.purpose,
            Some(active_contract_hash),
            &contract.contract_hash,
            None,
            now,
        )?;
        set_contract_catalogue_head(
            &transaction,
            &contract.purpose,
            &contract.contract_hash,
            activation_id,
        )?;
        transaction.commit()?;
        drop(connection);
        self.contract_installation(&contract.contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(contract.contract_hash.clone()))
    }

    /// Persist a candidate relative to the current active Contract. This is an
    /// immutable install only: activation is coupled atomically to the
    /// candidate's canonical PolicyTransition in `record_policy_evaluation`.
    pub fn install_candidate_contract(
        &self,
        active_contract_hash: &ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> StoreResult<StoredContract> {
        candidate.validate()?;
        let artifact = self.contract_artifact(candidate, now)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = self
            .stored_contract_with_connection(&transaction, active_contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(active_contract_hash.clone()))?;
        if active.activated_at.is_none() || !candidate_is_bounded(&active.contract, candidate) {
            return Err(StoreError::ContractCapabilityExpansion {
                active: active_contract_hash.clone(),
                candidate: candidate.contract_hash.clone(),
            });
        }
        if let Some(existing) =
            self.stored_contract_with_connection(&transaction, &candidate.contract_hash)?
        {
            if existing.contract == *candidate
                && existing.baseline_contract_hash.as_ref() == Some(active_contract_hash)
                && existing.activated_at.is_none()
            {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(StoreError::ContractActivationConflict(
                candidate.purpose.clone(),
            ));
        }
        assert_contract_identity_available(&transaction, candidate)?;
        insert_artifact(&transaction, &artifact)?;
        insert_contract_installation(
            &transaction,
            candidate,
            &artifact,
            Some(active_contract_hash),
            now,
        )?;
        transaction.commit()?;
        drop(connection);
        self.contract_installation(&candidate.contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(candidate.contract_hash.clone()))
    }

    pub fn commit_workflow(&self, commit: &WorkflowCommit) -> StoreResult<()> {
        self.validate_workflow_commit(commit)?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::commit_workflow_transaction(&transaction, commit)?;
        transaction.commit()?;
        Ok(())
    }

    /// Commit a Planner proposal and the graph revision it lowers. The proposal,
    /// graph, task rows, events, and Planner completion become visible together.
    pub fn commit_workflow_patch(&self, commit: &WorkflowPatchCommit) -> StoreResult<()> {
        let permit = &commit.permit;
        let planner_output = &commit.planner_output;
        let evidence_needs = &commit.evidence_needs;
        let proposal_artifact = &commit.proposal;
        let previous_graph_artifact_id = &commit.previous_graph_artifact_id;
        let next_graph = &commit.next_graph;
        let added_nodes = &commit.added_nodes;
        let updated_nodes = &commit.updated_nodes;
        let now = commit.completed_at;
        if planner_output.kind != ArtifactKind::WorkflowProposalDraft
            || proposal_artifact.kind != ArtifactKind::WorkflowProposal
            || evidence_needs
                .iter()
                .any(|artifact| artifact.kind != ArtifactKind::EvidenceNeed)
        {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        if next_graph.kind != ArtifactKind::WorkflowGraph {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        if planner_output.lifecycle != ArtifactLifecycle::RunScoped
            || evidence_needs
                .iter()
                .any(|artifact| artifact.lifecycle != ArtifactLifecycle::RunScoped)
            || proposal_artifact.lifecycle != ArtifactLifecycle::RunScoped
            || next_graph.lifecycle != ArtifactLifecycle::RunScoped
        {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        planner_output.validate()?;
        proposal_artifact.validate()?;
        next_graph.validate()?;
        self.read_blob(&planner_output.blob)?;
        for evidence_need in evidence_needs {
            evidence_need.validate()?;
            self.read_blob(&evidence_need.blob)?;
        }
        self.read_blob(&proposal_artifact.blob)?;
        let proposal: WorkflowProposal =
            serde_json::from_slice(&self.read_blob(&proposal_artifact.blob)?)?;
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&next_graph.blob)?)?;
        graph.validate()?;
        if proposal.topology_id != graph.topology_id {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        let expected_proposal_sources = std::iter::once(ArtifactRef {
            artifact_id: planner_output.artifact_id.clone(),
            kind: ArtifactKind::WorkflowProposalDraft,
        })
        .chain(evidence_needs.iter().map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        }))
        .collect::<std::collections::BTreeSet<_>>();
        let proposal_sources = proposal_artifact
            .source_refs
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if planner_output.provenance.producer_contract_hash != permit.contract_hash
            || proposal_artifact.provenance.producer_contract_hash != permit.contract_hash
            || expected_proposal_sources.len() != evidence_needs.len() + 1
            || proposal_sources != expected_proposal_sources
            || next_graph.source_refs.len() != 2
            || !next_graph.source_refs.iter().any(|reference| {
                reference.artifact_id == *previous_graph_artifact_id
                    && reference.kind == ArtifactKind::WorkflowGraph
            })
            || !next_graph.source_refs.iter().any(|reference| {
                reference.artifact_id == proposal_artifact.artifact_id
                    && reference.kind == ArtifactKind::WorkflowProposal
            })
        {
            return Err(StoreError::InvalidWorkflowProposalArtifact);
        }
        let added_ids = added_nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let updated_ids = updated_nodes
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if added_ids.len() != added_nodes.len()
            || updated_ids.len() != updated_nodes.len()
            || !added_nodes
                .iter()
                .all(|node| graph.nodes.iter().any(|item| item == node))
            || !updated_nodes
                .iter()
                .all(|node| graph.nodes.iter().any(|item| item == node))
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        assert_origin_matches(planner_output.origin.as_ref(), permit)?;
        for evidence_need in evidence_needs {
            assert_origin_matches(evidence_need.origin.as_ref(), permit)?;
        }
        assert_origin_matches(proposal_artifact.origin.as_ref(), permit)?;
        let run_id = &permit.run_id;
        insert_artifact(&transaction, planner_output)?;
        append_event(
            &transaction,
            run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&planner_output.artifact_id),
            now,
        )?;
        for evidence_need in evidence_needs {
            insert_artifact(&transaction, evidence_need)?;
            append_event(
                &transaction,
                run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                LifecycleEventType::ArtifactCommitted,
                Some(&evidence_need.artifact_id),
                now,
            )?;
        }
        assert_workflow_input_artifacts(&transaction, &graph.nodes)?;
        let current = transaction
            .query_row(
                "SELECT graph_artifact_id, purpose FROM rebuild_runs WHERE run_id = ?1",
                params![run_id.0],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((current, purpose)) = current else {
            return Err(StoreError::MissingRun(run_id.clone()));
        };
        if parse_enum::<RunPurpose>(&purpose)? == RunPurpose::Paper {
            return Err(StoreError::FrozenPaperWorkflow(run_id.clone()));
        }
        if current != previous_graph_artifact_id.0.as_str() {
            return Err(StoreError::StaleWorkflowGraph);
        }
        let previous_graph_artifact = read_artifact(&transaction, previous_graph_artifact_id)?;
        if previous_graph_artifact.kind != ArtifactKind::WorkflowGraph {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        let previous_graph: WorkflowGraph =
            serde_json::from_slice(&self.read_blob(&previous_graph_artifact.blob)?)?;
        previous_graph.validate()?;
        let previous_nodes = previous_graph
            .nodes
            .iter()
            .map(|node| (node.task_id.clone(), node))
            .collect::<std::collections::BTreeMap<_, _>>();
        if added_ids.iter().any(|id| previous_nodes.contains_key(id))
            || updated_ids
                .iter()
                .any(|id| !previous_nodes.contains_key(id))
            || !added_ids.is_disjoint(&updated_ids)
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        for previous in &previous_graph.nodes {
            let Some(next) = graph
                .nodes
                .iter()
                .find(|node| node.task_id == previous.task_id)
            else {
                return Err(StoreError::WorkflowGraphMismatch);
            };
            if next != previous {
                if !updated_ids.contains(&previous.task_id) {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
                let mut permitted_update = previous.clone();
                permitted_update.dependencies = next.dependencies.clone();
                permitted_update.input_artifacts = next.input_artifacts.clone();
                if permitted_update != *next {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
            }
        }
        let existing_ids = transaction
            .prepare("SELECT task_id FROM rebuild_tasks WHERE run_id = ?1")?
            .query_map(params![run_id.0], |row| row.get::<_, String>(0))?
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        let next_ids = graph
            .nodes
            .iter()
            .map(|node| node.task_id.0.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let expected = existing_ids
            .iter()
            .cloned()
            .chain(added_ids.iter().map(|id| id.0.clone()))
            .collect::<std::collections::BTreeSet<_>>();
        if next_ids != expected {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        insert_artifact(&transaction, proposal_artifact)?;
        let proposal_event_id = append_event(
            &transaction,
            run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::ArtifactCommitted,
            Some(&proposal_artifact.artifact_id),
            now,
        )?;
        record_attempt_output(
            &transaction,
            permit,
            &proposal_artifact.artifact_id,
            proposal_event_id,
        )?;
        insert_artifact(&transaction, next_graph)?;
        for node in added_nodes {
            insert_task_node(&transaction, run_id, node, now)?;
        }
        for node in added_nodes {
            insert_node_dependencies(&transaction, node)?;
        }
        for node in updated_nodes {
            let status = transaction.query_row(
                "SELECT status FROM rebuild_tasks WHERE task_id = ?1",
                params![node.task_id.0],
                |row| row.get::<_, String>(0),
            )?;
            if status != "queued" {
                return Err(StoreError::TaskNotRunnable(node.task_id.clone()));
            }
            transaction.execute(
                "UPDATE rebuild_tasks SET input_artifacts_json = ?1 WHERE task_id = ?2",
                params![
                    serde_json::to_string(&node.input_artifacts)?,
                    node.task_id.0
                ],
            )?;
            transaction.execute(
                "DELETE FROM rebuild_task_dependencies WHERE task_id = ?1",
                params![node.task_id.0],
            )?;
            for dependency in &node.dependencies {
                transaction.execute(
                    "INSERT INTO rebuild_task_dependencies (task_id, depends_on_task_id) VALUES (?1, ?2)",
                    params![node.task_id.0, dependency.0],
                )?;
            }
        }
        transaction.execute(
            "UPDATE rebuild_runs SET graph_artifact_id = ?1 WHERE run_id = ?2",
            params![next_graph.artifact_id.0.as_str(), run_id.0],
        )?;
        let revision = transaction.query_row(
            "SELECT COALESCE(MAX(revision), -1) + 1 FROM rebuild_workflow_revisions WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, i64>(0),
        )?;
        transaction.execute(
            r#"INSERT INTO rebuild_workflow_revisions
               (run_id, revision, graph_artifact_id, created_at)
               VALUES (?1, ?2, ?3, ?4)"#,
            params![
                run_id.0,
                revision,
                next_graph.artifact_id.0.as_str(),
                now.to_rfc3339(),
            ],
        )?;
        append_event(
            &transaction,
            run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::WorkflowPatched,
            Some(&next_graph.artifact_id),
            now,
        )?;
        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        finish_permitted_task(
            &transaction,
            permit,
            TaskStatus::Succeeded,
            on_failure,
            Some(&proposal_artifact.artifact_id),
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Request cancellation once. Queued tasks are durably cancelled in the
    /// same transaction; running attempts observe this request through
    /// [`Self::run_cancel_requested`] and finish through their permit.
    pub fn request_run_cancel(
        &self,
        run_id: &RunId,
        reason: &str,
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        if reason.trim().is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "run_cancel.reason",
            }));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM rebuild_runs WHERE run_id = ?1",
                params![run_id.0],
                |_| Ok(()),
            )
            .optional()?;
        if exists.is_none() {
            return Err(StoreError::MissingRun(run_id.clone()));
        }
        let inserted = transaction.execute(
            r#"INSERT OR IGNORE INTO rebuild_run_cancellations (run_id, reason, requested_at)
               VALUES (?1, ?2, ?3)"#,
            params![run_id.0, reason, now.to_rfc3339()],
        )?;
        if inserted == 0 {
            transaction.commit()?;
            return Ok(false);
        }
        append_event(
            &transaction,
            run_id,
            None,
            None,
            LifecycleEventType::RunCancelRequested,
            None,
            now,
        )?;
        cancel_queued_tasks(&transaction, run_id, now)?;
        refresh_run_status(&transaction, run_id, now)?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn run_cancel_requested(&self, run_id: &RunId) -> StoreResult<bool> {
        let connection = self.connection()?;
        Ok(connection
            .query_row(
                "SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1",
                params![run_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// Close the active attempt as retried or terminal. The policy and
    /// attempt count are read from the durable task record, so a handler
    /// cannot make itself retryable or extend its retry budget.
    /// Durably defers a claimed task without consuming its failure retry
    /// budget. The attempt is closed and replay records the queued transition.
    pub fn defer_task(
        &self,
        permit: &TaskWritePermit,
        ready_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        if ready_at <= now {
            return Err(StoreError::InvalidTaskDeferral(permit.task_id.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        transaction.execute(
            r#"UPDATE rebuild_tasks
               SET status = 'queued', lease_id = NULL, active_attempt_id = NULL,
                   worker_id = NULL, lease_until = NULL, ready_at = ?1
               WHERE task_id = ?2"#,
            params![ready_at.to_rfc3339(), permit.task_id.0],
        )?;
        transaction.execute(
            "UPDATE rebuild_attempts SET status = 'deferred', finished_at = ?1 WHERE attempt_id = ?2",
            params![now.to_rfc3339(), permit.attempt_id.0],
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::TaskDeferred,
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn retry_task(
        &self,
        permit: &TaskWritePermit,
        retry_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> StoreResult<RetryTaskResult> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let (retry, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        let attempt_count = transaction.query_row(
            "SELECT COUNT(*) FROM rebuild_attempts WHERE task_id = ?1",
            params![permit.task_id.0],
            |row| row.get::<_, u64>(0),
        )?;
        if attempt_count < u64::from(retry.max_attempts) {
            transaction.execute(
                r#"UPDATE rebuild_tasks
                   SET status = 'queued', lease_id = NULL, active_attempt_id = NULL,
                       worker_id = NULL, lease_until = NULL, ready_at = ?1
                   WHERE task_id = ?2"#,
                params![retry_at.to_rfc3339(), permit.task_id.0],
            )?;
            transaction.execute(
                "UPDATE rebuild_attempts SET status = 'retried', finished_at = ?1 WHERE attempt_id = ?2",
                params![now.to_rfc3339(), permit.attempt_id.0],
            )?;
            append_event(
                &transaction,
                &permit.run_id,
                Some(&permit.task_id),
                Some(&permit.attempt_id),
                LifecycleEventType::TaskRetryScheduled,
                None,
                now,
            )?;
            transaction.commit()?;
            return Ok(RetryTaskResult::Requeued);
        }

        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::TaskRetryExhausted,
            None,
            now,
        )?;
        let status = finish_permitted_task(
            &transaction,
            permit,
            TaskStatus::Failed,
            on_failure,
            None,
            now,
        )?;
        transaction.commit()?;
        Ok(RetryTaskResult::Terminal(status))
    }

    pub fn claim_next_task(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> StoreResult<Option<ClaimedAttempt>> {
        if worker_id.trim().is_empty() {
            return Err(StoreError::Domain(DomainError::EmptyField {
                field: "worker_id",
            }));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let selected = transaction
            .query_row(
        r#"SELECT t.task_id, t.run_id, t.recipe_id, t.objective, t.contract_hash, t.priority,
        t.budget_json, t.retry_json, t.on_failure, t.parent_task_id, t.input_artifacts_json
                    FROM rebuild_tasks AS t
                    JOIN rebuild_runs AS r ON r.run_id = t.run_id
               WHERE t.status = 'queued' AND t.ready_at <= ?1
                 AND (r.status IN ('queued', 'running')
                      OR (r.status = 'completed' AND t.recipe_id = ?2))
              AND NOT EXISTS (
                  SELECT 1 FROM rebuild_run_cancellations AS c WHERE c.run_id = t.run_id
              )
              AND NOT EXISTS (
                        SELECT 1 FROM rebuild_task_dependencies AS d
                        JOIN rebuild_tasks AS p ON p.task_id = d.depends_on_task_id
                        WHERE d.task_id = t.task_id AND p.status NOT IN ('succeeded', 'skipped')
                      )
                    ORDER BY t.priority DESC, t.task_id ASC LIMIT 1"#,
                params![now.to_rfc3339(), POST_TERMINAL_WORKER_RECIPE_ID],
            row_to_node,
            )
            .optional()?;
        let Some((run_id, mut node)) = selected else {
            transaction.commit()?;
            return Ok(None);
        };
        node.dependencies = task_dependencies(&transaction, &node.task_id)?;
        let permit = TaskWritePermit {
            run_id: run_id.clone(),
            task_id: node.task_id.clone(),
            attempt_id: akzio_domain::AttemptId::new(),
            lease_id: akzio_domain::LeaseId::new(),
            epoch: transaction.query_row(
                "SELECT lease_epoch + 1 FROM rebuild_tasks WHERE task_id = ?1",
                params![node.task_id.0],
                |row| row.get(0),
            )?,
            contract_hash: node.contract_hash.clone(),
        };
        let previous_attempt = transaction
            .query_row(
                "SELECT attempt_id, status FROM rebuild_attempts WHERE task_id = ?1 ORDER BY started_at DESC, attempt_id DESC LIMIT 1",
                params![node.task_id.0],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let updated = transaction.execute(
            r#"UPDATE rebuild_tasks
               SET status = 'running', lease_id = ?1, lease_epoch = ?2, active_attempt_id = ?3,
                   lease_until = ?4, worker_id = ?5
               WHERE task_id = ?6 AND status = 'queued'"#,
            params![
                permit.lease_id.0,
                permit.epoch,
                permit.attempt_id.0,
                (now + lease_for).to_rfc3339(),
                worker_id,
                permit.task_id.0,
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::TaskNotRunnable(permit.task_id));
        }
        transaction.execute(
            r#"INSERT INTO rebuild_attempts
               (attempt_id, task_id, run_id, lease_id, epoch, worker_id, status, started_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)"#,
            params![
                permit.attempt_id.0,
                permit.task_id.0,
                permit.run_id.0,
                permit.lease_id.0,
                permit.epoch,
                worker_id,
                now.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            "UPDATE rebuild_runs SET status = 'running' WHERE run_id = ?1 AND status = 'queued'",
            params![permit.run_id.0],
        )?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::TaskStarted,
            None,
            now,
        )?;
        if let Some((parent_attempt_id, parent_status)) = previous_attempt {
            let relation = if parent_status == "abandoned" {
                AttemptRelationKind::Recovery
            } else {
                AttemptRelationKind::Retry
            };
            self.record_attempt_relation_in_transaction(
                &transaction,
                &permit,
                &AttemptId(parent_attempt_id),
                relation,
                now,
            )?;
        }
        transaction.commit()?;
        Ok(Some(ClaimedAttempt {
            run_id,
            node,
            permit,
        }))
    }

    pub fn heartbeat_task(
        &self,
        permit: &TaskWritePermit,
        expires_at: DateTime<Utc>,
    ) -> StoreResult<()> {
        let connection = self.connection()?;
        let updated = connection.execute(
            r#"UPDATE rebuild_tasks SET lease_until = ?1
               WHERE task_id = ?2 AND status = 'running' AND lease_id = ?3 AND lease_epoch = ?4
                 AND active_attempt_id = ?5"#,
            params![
                expires_at.to_rfc3339(),
                permit.task_id.0,
                permit.lease_id.0,
                permit.epoch,
                permit.attempt_id.0,
            ],
        )?;
        if updated != 1 {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        }
        Ok(())
    }

    /// Verifies that a handler still owns the active task attempt without
    /// creating an artifact or changing task state. External adapters use
    /// this immediately before side effects; final persistence rechecks the
    /// same permit in its own transaction.
    pub fn validate_task_permit(&self, permit: &TaskWritePermit) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        transaction.commit()?;
        Ok(())
    }

    /// Append a task-scoped lifecycle fact without creating an artifact.
    /// The permit check and event insert share one transaction so a stale
    /// attempt cannot publish an AgentTurnStarted fact after takeover.
    pub fn append_task_event(
        &self,
        permit: &TaskWritePermit,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        append_task_event(&transaction, permit, event_type, now)?;
        validate_agent_turn_lifecycle_events(&transaction, Some(&permit.run_id))?;
        transaction.commit()?;
        Ok(())
    }

    /// Verify a handler-owned transaction already closed this exact attempt.
    /// A merely stale permit is insufficient: task and attempt terminal state,
    /// run, lease, epoch, and contract must all still identify the caller.
    pub fn verify_attempt_terminal(
        &self,
        permit: &TaskWritePermit,
        status: TaskStatus,
    ) -> StoreResult<()> {
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let current = transaction
            .query_row(
                r#"SELECT t.run_id, t.status, t.active_attempt_id, t.contract_hash,
                          a.task_id, a.run_id, a.lease_id, a.epoch, a.status
                   FROM rebuild_attempts AS a
                   JOIN rebuild_tasks AS t ON t.task_id = a.task_id
                   WHERE a.attempt_id = ?1"#,
                params![permit.attempt_id.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, u64>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()?;
        let Some(current) = current else {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        };
        let expected_contract = permit.contract_hash.as_ref().map(ContentHash::as_str);
        if current.0 != permit.run_id.0
            || current.1 != enum_name(status)
            || current.2.is_some()
            || current.3.as_deref() != expected_contract
            || current.4 != permit.task_id.0
            || current.5 != permit.run_id.0
            || current.6 != permit.lease_id.0
            || current.7 != permit.epoch
            || current.8 != enum_name(status)
        {
            return Err(StoreError::StalePermit(permit.task_id.clone()));
        }
        validate_tool_lifecycle_events(&transaction, Some(&permit.run_id))?;
        if status == TaskStatus::Succeeded {
            ensure_no_pending_tool_calls(
                &transaction,
                &permit.run_id,
                &permit.task_id,
                &permit.attempt_id,
            )?;
        }
        Ok(())
    }

    pub fn write_task_artifact(
        &self,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.write_task_artifact_fenced(None, permit, artifact, event_type, now)
    }

    /// Persist a task artifact while optionally fencing a daemon-owned worker.
    /// The lease check is in the same transaction as the artifact/event write,
    /// so a takeover cannot leave a stale worker's output committed.
    pub fn write_task_artifact_fenced(
        &self,
        lease: Option<&DaemonLease>,
        permit: &TaskWritePermit,
        artifact: &Artifact,
        event_type: LifecycleEventType,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        artifact.validate()?;
        reject_generic_learning_artifact(artifact)?;
        self.read_blob(&artifact.blob)?;
        self.validate_specialized_artifact(artifact)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(lease) = lease {
            assert_daemon_lease(&transaction, lease, Utc::now())?;
        }
        assert_permit(&transaction, permit)?;
        assert_task_artifact_lifecycle(&transaction, &permit.run_id, artifact)?;
        assert_origin_matches(artifact.origin.as_ref(), permit)?;
        insert_artifact(&transaction, artifact)?;
        append_event(
            &transaction,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            event_type,
            Some(&artifact.artifact_id),
            now,
        )?;
        validate_tool_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_agent_turn_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_context_lifecycle_events(&transaction, Some(&permit.run_id))?;
        validate_gate_lifecycle_events(&transaction, Some(&permit.run_id))?;
        transaction.commit()?;
        Ok(())
    }

    /// Commit the final artifacts and terminal task state together. A reader
    /// cannot observe a completed attempt without every committed output and
    /// its corresponding durable events.
    pub fn commit_attempt(
        &self,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, status)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        commit_attempt_transaction(&transaction, permit, artifacts, status, now)?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically persist broker-visible task outputs only while both the
    /// daemon epoch and task attempt permit remain current.
    pub fn commit_fenced_attempt(
        &self,
        lease: &DaemonLease,
        permit: &TaskWritePermit,
        artifacts: &[Artifact],
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        self.validate_attempt_commit(permit, artifacts, status)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        commit_attempt_transaction(&transaction, permit, artifacts, status, now)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn finish_task(
        &self,
        permit: &TaskWritePermit,
        status: TaskStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        if !status.is_terminal() {
            return Err(StoreError::TaskNotRunnable(permit.task_id.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_permit(&transaction, permit)?;
        let (_, on_failure) = task_retry_policy(&transaction, &permit.task_id)?;
        finish_permitted_task(&transaction, permit, status, on_failure, None, now)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn recover_expired_tasks(&self, now: DateTime<Utc>) -> StoreResult<u64> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let expired = {
            let mut statement = transaction.prepare(
                r#"SELECT task_id, run_id, active_attempt_id, lease_id, lease_epoch, contract_hash
                   FROM rebuild_tasks
                   WHERE status = 'running' AND lease_until < ?1
                   ORDER BY task_id"#,
            )?;
            let rows = statement
                .query_map(params![now.to_rfc3339()], |row| {
                    Ok((
                        TaskId(row.get::<_, String>(0)?),
                        RunId(row.get::<_, String>(1)?),
                        akzio_domain::AttemptId(row.get::<_, String>(2)?),
                        akzio_domain::LeaseId(row.get::<_, String>(3)?),
                        row.get::<_, u64>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            rows
        };
        for (task_id, run_id, attempt_id, lease_id, epoch, contract_hash) in &expired {
            let permit = TaskWritePermit {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                attempt_id: attempt_id.clone(),
                lease_id: lease_id.clone(),
                epoch: *epoch,
                contract_hash: contract_hash.as_deref().map(ContentHash::new).transpose()?,
            };
            let cancelled = transaction
                .query_row(
                    "SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1",
                    params![run_id.0],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            let (retry, on_failure) = task_retry_policy(&transaction, task_id)?;
            if cancelled {
                finish_permitted_task(
                    &transaction,
                    &permit,
                    TaskStatus::Cancelled,
                    on_failure,
                    None,
                    now,
                )?;
                continue;
            }
            let attempts = transaction.query_row(
                "SELECT COUNT(*) FROM rebuild_attempts WHERE task_id = ?1",
                params![task_id.0],
                |row| row.get::<_, u64>(0),
            )?;
            if attempts < u64::from(retry.max_attempts) {
                transaction.execute(
                    r#"UPDATE rebuild_tasks
                       SET status = 'queued', lease_id = NULL, active_attempt_id = NULL,
                           worker_id = NULL, lease_until = NULL, ready_at = ?1
                       WHERE task_id = ?2"#,
                    params![now.to_rfc3339(), task_id.0],
                )?;
                transaction.execute(
                    "UPDATE rebuild_attempts SET status = 'abandoned', finished_at = ?1 WHERE attempt_id = ?2",
                    params![now.to_rfc3339(), attempt_id.0],
                )?;
                append_event(
                    &transaction,
                    run_id,
                    Some(task_id),
                    Some(attempt_id),
                    LifecycleEventType::TaskRecovered,
                    None,
                    now,
                )?;
            } else {
                append_event(
                    &transaction,
                    run_id,
                    Some(task_id),
                    Some(attempt_id),
                    LifecycleEventType::TaskRecoveryExhausted,
                    None,
                    now,
                )?;
                finish_permitted_task(
                    &transaction,
                    &permit,
                    TaskStatus::Failed,
                    on_failure,
                    None,
                    now,
                )?;
            }
        }
        transaction.commit()?;
        Ok(expired.len() as u64)
    }

    /// Returns final artifacts for the only succeeded attempt of an exact task
    /// in an exact run. Intermediate Agent/Tool artifacts are deliberately
    /// absent: only the atomic completion surface records attempt outputs.
    pub fn committed_task_outputs(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection()?;
        let attempt_id = connection
            .query_row(
                r#"SELECT a.attempt_id
                   FROM rebuild_tasks AS t
                   JOIN rebuild_attempts AS a ON a.task_id = t.task_id
                  WHERE t.run_id = ?1
                    AND t.task_id = ?2
                    AND t.status = 'succeeded'
                    AND a.status = 'succeeded'
                  ORDER BY a.finished_at DESC, a.attempt_id DESC
                  LIMIT 1"#,
                params![run_id.0, task_id.0],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::CommittedOutputTask {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
            })?;
        read_committed_attempt_outputs(&connection, Some(run_id), task_id, &AttemptId(attempt_id))
    }

    /// Returns final artifacts for one exact succeeded task attempt. This is
    /// intentionally stricter than an event-log query so callers cannot feed
    /// an AgentTurn, ToolCall, or failed-attempt artifact into another task.
    /// As [`Self::committed_task_outputs`], but permits an explicitly
    /// successful no-output gate. The task/attempt still had to reach durable
    /// `succeeded`; callers must never use this for arbitrary running work.
    pub fn succeeded_task_outputs_or_empty(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
    ) -> StoreResult<Vec<Artifact>> {
        match self.committed_task_outputs(run_id, task_id) {
            Ok(artifacts) => Ok(artifacts),
            Err(StoreError::CommittedOutputAttempt { .. }) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    pub fn committed_attempt_outputs(
        &self,
        task_id: &TaskId,
        attempt_id: &AttemptId,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection()?;
        read_committed_attempt_outputs(&connection, None, task_id, attempt_id)
    }

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

fn contract_upgrade_blockers(
    connection: &Connection,
    active_contract_hash: &ContentHash,
) -> StoreResult<Vec<String>> {
    let mut statement = connection.prepare(
        r#"
        SELECT 'task:' || run_id || ':' || task_id || ':' || status
        FROM rebuild_tasks
        WHERE contract_hash = ?1
          AND status IN ('queued', 'leased', 'running')
        UNION ALL
        SELECT 'session:' || session_key || ':' || run_id
        FROM rebuild_session_slots AS slot
        WHERE committed_at IS NULL
          AND EXISTS (
              SELECT 1
              FROM rebuild_tasks AS task
              WHERE task.run_id = slot.run_id
                AND task.contract_hash = ?1
          )
        ORDER BY 1
        "#,
    )?;
    let rows = statement.query_map(params![active_contract_hash.as_str()], |row| row.get(0))?;
    let blockers = rows.collect::<Result<Vec<String>, _>>()?;
    Ok(blockers)
}
