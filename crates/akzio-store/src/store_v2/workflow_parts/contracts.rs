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
}
