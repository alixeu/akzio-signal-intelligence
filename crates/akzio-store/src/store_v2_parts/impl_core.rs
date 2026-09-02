impl V2Store {
    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    fn connection(&self) -> StoreResult<std::sync::MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| StoreError::Integrity("store connection poisoned".to_owned()))
    }

    pub fn observatory_configuration<T: DeserializeOwned>(&self) -> StoreResult<Option<T>> {
        let payload = self
            .connection()?
            .query_row(
                "SELECT configuration_json FROM rebuild_observatory_configuration WHERE singleton = 1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        payload
            .map(|payload| serde_json::from_slice(&payload))
            .transpose()
            .map_err(StoreError::from)
    }

    pub fn set_observatory_configuration<T: Serialize>(
        &self,
        configuration: &T,
    ) -> StoreResult<()> {
        let payload = serde_json::to_vec(configuration)?;
        self.connection()?.execute(
            "INSERT INTO rebuild_observatory_configuration (singleton, configuration_json) VALUES (1, ?1) ON CONFLICT(singleton) DO UPDATE SET configuration_json = excluded.configuration_json",
            params![payload],
        )?;
        Ok(())
    }

    pub fn clear_observatory_configuration(&self) -> StoreResult<bool> {
        Ok(self.connection()?.execute(
            "DELETE FROM rebuild_observatory_configuration WHERE singleton = 1",
            [],
        )? > 0)
    }

    fn read_all_events(&self, run_id: &RunId) -> StoreResult<Vec<StoredEvent>> {
        const PAGE_SIZE: usize = 256;
        let mut after = 0_i64;
        let mut events = Vec::new();
        loop {
            let page = self.events_after(run_id, after, PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            after = page.last().expect("non-empty event page").cursor;
            events.extend(page);
            if events.len() < PAGE_SIZE {
                break;
            }
        }
        Ok(events)
    }

    fn validate_paper_approval_binding(
        &self,
        runtime_manifest: &Artifact,
        approval: &Artifact,
    ) -> StoreResult<(RuntimeManifest, PaperLaunchApproval)> {
        if runtime_manifest.kind != ArtifactKind::RuntimeManifest
            || approval.kind != ArtifactKind::PaperLaunchApproval
            || runtime_manifest.lifecycle != ArtifactLifecycle::Canonical
            || approval.lifecycle != ArtifactLifecycle::Canonical
            || runtime_manifest.origin.is_some()
            || approval.origin.is_some()
            || approval.source_refs
                != vec![ArtifactRef {
                    artifact_id: runtime_manifest.artifact_id.clone(),
                    kind: ArtifactKind::RuntimeManifest,
                }]
        {
            return Err(StoreError::InvalidSessionSlot(
                "paper-approval-binding".to_owned(),
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
        if approval_payload.runtime_manifest.artifact_id != runtime_manifest.artifact_id
            || approval_payload.runtime_manifest_hash != manifest_payload.manifest_hash()?
            || approval_payload.expires_at > manifest_payload.expires_at
        {
            return Err(StoreError::InvalidSessionSlot(
                "paper-approval-binding".to_owned(),
            ));
        }
        Ok((manifest_payload, approval_payload))
    }

    pub(super) fn validate_paper_session_reservation(
        &self,
        reservation: &SessionReservation,
        proposal: &Artifact,
    ) -> StoreResult<()> {
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
        Ok(())
    }

    pub(super) fn validate_workflow_commit(&self, commit: &WorkflowCommit) -> StoreResult<()> {
        if commit.graph.kind != ArtifactKind::WorkflowGraph
            || commit.graph.artifact_id != commit.run.graph_artifact_id
        {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        commit.graph.validate()?;
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&commit.graph.blob)?)?;
        graph.validate()?;
        if graph.nodes != commit.nodes || graph.topology_id != commit.run.topology_id {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        Ok(())
    }

    /// Atomically publishes one runtime manifest and its operator approval.
    /// A scheduler can never observe a half-written approval binding.
    pub fn write_paper_approval_binding(
        &self,
        runtime_manifest: &Artifact,
        approval: &Artifact,
    ) -> StoreResult<()> {
        self.validate_paper_approval_binding(runtime_manifest, approval)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_artifact(&transaction, runtime_manifest)?;
        insert_artifact(&transaction, approval)?;
        transaction.commit()?;
        Ok(())
    }

    /// Writes a root artifact such as an installed Contract. Bootstrap is deliberately
    /// narrow: a task-origin artifact must use `write_task_artifact` instead.
    pub fn write_bootstrap_artifact(&self, artifact: &Artifact) -> StoreResult<()> {
        artifact.validate()?;
        if artifact.origin.is_some()
            || !matches!(
                artifact.kind,
                ArtifactKind::Contract | ArtifactKind::FreezeState
            )
        {
            return Err(StoreError::PermitOriginMismatch);
        }
        self.read_blob(&artifact.blob)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_artifact(&transaction, artifact)?;
        transaction.commit()?;
        Ok(())
    }

    /// Return the immutable Contract currently selected for a purpose.
    /// The mutable head is only a reconstruction cursor; each activation stays
    /// in `rebuild_contract_activations` for Doctor and restart recovery.
    /// Persist an immutable operator freeze transition. There is no mutable
    /// switch: execution consults the latest canonical `FreezeState` artifact.
    pub fn write_freeze_state(
        &self,
        frozen: bool,
        reason: impl Into<String>,
        changed_at: DateTime<Utc>,
    ) -> StoreResult<Artifact> {
        let payload = FreezeState {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            frozen,
            reason: reason.into(),
            changed_at,
        };
        payload.validate()?;
        let artifact = Artifact::new(
            ArtifactKind::FreezeState,
            self.put_json(&payload)?,
            "store.freeze_state",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".to_owned(),
                observed_at: Some(changed_at),
                retrieved_at: changed_at,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            Vec::new(),
            changed_at,
        )?;
        self.write_bootstrap_artifact(&artifact)?;
        Ok(artifact)
    }

    fn contract_artifact(
        &self,
        contract: &AgentContract,
        now: DateTime<Utc>,
    ) -> StoreResult<Artifact> {
        Ok(Artifact::new(
            ArtifactKind::Contract,
            self.put_json(contract)?,
            "research.contract_catalogue",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.contract_catalogue".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )?)
    }

    fn stored_contract_with_connection(
        &self,
        connection: &Connection,
        contract_hash: &ContentHash,
    ) -> StoreResult<Option<StoredContract>> {
        let row = connection
            .query_row(
                r#"SELECT contract_artifact_id, baseline_contract_hash, installed_at,
                          activation.activated_at
                   FROM rebuild_contract_installations AS installation
                   LEFT JOIN rebuild_contract_catalogue_heads AS head
                     ON head.contract_hash = installation.contract_hash
                   LEFT JOIN rebuild_contract_activations AS activation
                     ON activation.activation_id = head.activation_id
                   WHERE installation.contract_hash = ?1"#,
                params![contract_hash.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(artifact_id, baseline, installed_at, activated_at)| {
            let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(artifact_id)?))?;
            if artifact.kind != ArtifactKind::Contract
                || artifact.lifecycle != ArtifactLifecycle::Canonical
            {
                return Err(StoreError::Integrity(format!(
                    "contract {contract_hash} has an invalid artifact"
                )));
            }
            let contract: AgentContract = self.read_artifact_payload(&artifact)?;
            contract.validate()?;
            if contract.contract_hash != *contract_hash {
                return Err(StoreError::Integrity(format!(
                    "contract installation {contract_hash} payload hash diverges"
                )));
            }
            Ok(StoredContract {
                contract,
                artifact,
                baseline_contract_hash: baseline.map(ContentHash::new).transpose()?,
                installed_at: parse_time(&installed_at)?,
                activated_at: activated_at.map(|value| parse_time(&value)).transpose()?,
            })
        })
        .transpose()
    }

    fn apply_contract_catalogue_transition(
        &self,
        transaction: &Transaction<'_>,
        commit: &PolicyEvaluationCommit,
        transition: &PolicyTransition,
    ) -> StoreResult<()> {
        let PolicySubject::Contract(candidate_hash) = &commit.subject else {
            return Ok(());
        };
        let candidate = self
            .stored_contract_with_connection(transaction, candidate_hash)?
            .ok_or_else(|| StoreError::MissingContractInstallation(candidate_hash.clone()))?;
        let Some(baseline_hash) = candidate.baseline_contract_hash.as_ref() else {
            return Err(StoreError::ContractActivationConflict(
                candidate.contract.purpose,
            ));
        };

        match (transition.from, transition.to) {
            (_, PolicyState::Contract(CandidatePolicyState::Active)) => {
                let candidate_policy_artifact =
                    commit
                        .candidate_policy
                        .as_ref()
                        .ok_or(StoreError::InvalidLearningCommit(
                            "contract_catalogue.candidate_policy",
                        ))?;
                let candidate_policy: CandidatePolicy =
                    self.read_artifact_payload(candidate_policy_artifact)?;
                if candidate_policy.candidate.artifact_id != candidate.artifact.artifact_id
                    || candidate_policy.baseline.kind != ArtifactKind::Contract
                    || candidate_policy.subject != commit.subject
                {
                    return Err(StoreError::InvalidLearningCommit(
                        "contract_catalogue.candidate_policy_binding",
                    ));
                }
                let Some((current_hash, _)) =
                    contract_catalogue_head(transaction, &candidate.contract.purpose)?
                else {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                };
                let current = self
                    .stored_contract_with_connection(transaction, &current_hash)?
                    .ok_or_else(|| StoreError::MissingContractInstallation(current_hash.clone()))?;
                if current.contract.contract_hash != *baseline_hash
                    || candidate_policy.baseline.artifact_id != current.artifact.artifact_id
                    || !candidate_is_bounded(&current.contract, &candidate.contract)
                {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let activation_id = append_contract_activation(
                    transaction,
                    &candidate.contract.purpose,
                    Some(&current_hash),
                    candidate_hash,
                    Some(&transition.transition_id),
                    transition.created_at,
                )?;
                set_contract_catalogue_head(
                    transaction,
                    &candidate.contract.purpose,
                    candidate_hash,
                    activation_id,
                )?;
            }
            (PolicyState::Contract(CandidatePolicyState::Active), PolicyState::Contract(_)) => {
                let Some((current_hash, _)) =
                    contract_catalogue_head(transaction, &candidate.contract.purpose)?
                else {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                };
                if current_hash != *candidate_hash {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let baseline = self
                    .stored_contract_with_connection(transaction, baseline_hash)?
                    .ok_or_else(|| {
                        StoreError::MissingContractInstallation(baseline_hash.clone())
                    })?;
                if baseline.contract.purpose != candidate.contract.purpose {
                    return Err(StoreError::ContractActivationConflict(
                        candidate.contract.purpose.clone(),
                    ));
                }
                let activation_id = append_contract_activation(
                    transaction,
                    &candidate.contract.purpose,
                    Some(candidate_hash),
                    baseline_hash,
                    Some(&transition.transition_id),
                    transition.created_at,
                )?;
                set_contract_catalogue_head(
                    transaction,
                    &candidate.contract.purpose,
                    baseline_hash,
                    activation_id,
                )?;
            }
            _ => {}
        }
        Ok(())
    }
}
