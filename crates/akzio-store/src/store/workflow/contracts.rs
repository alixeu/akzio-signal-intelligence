impl Store {
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
        self.assert_canonical_contract_upgrade(&transaction, active_contract_hash, contract)?;

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

    /// Retirement is read-only: unfinished work and live leases require an explicit operator resolution.
    pub fn check_legacy_workflow_retirement(&self, now: DateTime<Utc>) -> StoreResult<()> {
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT DISTINCT run_id FROM rebuild_tasks WHERE status IN ('queued','leased','running') OR lease_until > ?1")?;
        let runs = statement.query_map(params![now.to_rfc3339()], |row| row.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
        let mut blockers = Vec::new();
        for run in runs {
            if legacy_workflow(&connection, &RunId(run.clone()))? {
                let mut tasks = connection.prepare("SELECT task_id,status,lease_until FROM rebuild_tasks WHERE run_id=?1 AND (status IN ('queued','leased','running') OR lease_until > ?2)")?;
                for task in tasks.query_map(params![run,now.to_rfc3339()], |row| Ok(format!("run:{} task:{} status:{} lease:{:?}", run,row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,Option<String>>(2)?)))? { blockers.push(task?); }
            }
        }
        if blockers.is_empty() { Ok(()) } else { Err(StoreError::DebugControl(format!("legacy_workflow_retired: upgrade blocked: {}",blockers.join(", ")))) }
    }

    pub fn assert_workflow_executable(&self, run: &RunId) -> StoreResult<()> {
        assert_workflow_executable(&*self.connection()?, run)
    }

    /// Read-only release preflight, before any catalogue head is changed.
    pub fn check_canonical_contract_upgrade(
        &self,
        active_contract_hash: &ContentHash,
        contract: &AgentContract,
    ) -> StoreResult<()> {
        contract.validate()?;
        let connection = self.connection()?;
        self.assert_canonical_contract_upgrade(&connection, active_contract_hash, contract)
    }

    fn assert_canonical_contract_upgrade(
        &self,
        connection: &Connection,
        active_contract_hash: &ContentHash,
        contract: &AgentContract,
    ) -> StoreResult<()> {
        let active = self
            .stored_contract_with_connection(connection, active_contract_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(active_contract_hash.clone()))?;
        let current_head =
            contract_catalogue_head(connection, &active.contract.purpose)?.map(|(hash, _)| hash);
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
        let blockers = contract_upgrade_blockers(connection, active_contract_hash)?;
        if !blockers.is_empty() {
            return Err(StoreError::ContractUpgradeBlocked {
                active: active_contract_hash.clone(),
                blockers: blockers.join(", "),
            });
        }

        Ok(())
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

    /// Atomically publish bounded research inputs and a non-executing PositionPlan.
    pub fn commit_position_plan(&self, workflow: &WorkflowCommit, setup: &[Artifact]) -> StoreResult<()> {
        if workflow.run.purpose != RunPurpose::PositionPlan { return Err(StoreError::PermitOriginMismatch); }
        self.validate_workflow_commit(workflow)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_artifact_batch(&tx, setup)?;
        Self::commit_workflow_transaction(&tx, workflow)?;
        Self::append_run_setup_events(&tx, &workflow.run.run_id, setup, workflow.run.created_at)?;
        tx.commit()?;
        Ok(())
    }

    pub fn commit_workflow(&self, commit: &WorkflowCommit) -> StoreResult<()> {
        self.validate_workflow_commit(commit)?;

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::commit_workflow_transaction(&transaction, commit)?;
        transaction.commit()?;
        Ok(())
    }


}

/// Explicit release migration envelope. Candidate policy promotion continues
/// to use the unchanged subset test. Keep the explicit v18 envelope readable
/// for its historical activation records; v20 adds only the Outcome grant cap.
pub(super) fn canonical_context_repair_is_bounded(active: &AgentContract, next: &AgentContract) -> bool {
    if !matches!((next.version, next.prompt.version), (18, 13) | (20, 14))
        || active.version >= next.version
        || active.contract_id.0 != format!("akzio.{}", active.purpose.as_str())
    {
        return false;
    }
    let output_limit = match next.purpose.as_str() {
        "research.planner" => 2000,
        "research.analyst" => 6000,
        "research.critic" => 4000,
        "research.synthesizer" => 5000,
        "learning.outcome_worker" => 4000,
        _ => return false,
    };
    if next.budget.max_output_tokens > output_limit
        || next.budget.max_input_tokens > active.budget.max_input_tokens
        || next.budget.max_tool_calls > active.budget.max_tool_calls
        || next.budget.max_wall_time_secs > active.budget.max_wall_time_secs
        || next.context.allow_raw_reread
        || next.tool_specs != active.tool_specs
    {
        return false;
    }
    let internal = [
        "akzio.ingest",
        "akzio.agent",
        "akzio.operator",
        "akzio.execution",
        "akzio-learning",
        "akzio.learning",
    ];
    let mut envelope = active.clone();
    envelope
        .candidate_capability_ceiling
        .context
        .permitted_source_families
        .extend(internal.map(str::to_owned));
    if next.purpose.as_str() == "learning.outcome_worker" {
        envelope
            .candidate_capability_ceiling
            .context
            .permitted_kinds
            .extend([ArtifactKind::Claim, ArtifactKind::Critique]);
        // The authorized original documents may fill the existing 128 KiB
        // sandbox. This does not increase the Agent's model input budget.
        if next.context.max_bytes > 128 * 1024
            || next.context.max_artifacts > 24
            || next.context.max_tokens > 32 * 1024
        {
            return false;
        }
        if next.version == 20 {
            envelope.candidate_capability_ceiling.context.max_tokens = 32 * 1024;
        }
    }
    for grant in &mut envelope.candidate_capability_ceiling.tool_grants {
        if grant.allowed_sources.is_empty() {
            return false;
        }
        grant.allowed_sources.extend(internal.map(str::to_owned));
    }
    candidate_is_bounded(&envelope, next)
}
