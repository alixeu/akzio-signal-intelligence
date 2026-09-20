use super::*;

impl Daemon {
    pub async fn approve_paper(
        &self,
        request: PaperApprovalRequest,
    ) -> Result<PaperApprovalResponse> {
        request.identity.validate()?;
        if !(self.paper.auto_paper
            || self.debug_enabled()
            || (self.paper.paper_broker.is_some() && self.paper.runtime_identity_hash.is_some()))
        {
            return Err(DaemonError::InvalidInput(
                "Paper approval requires a configured Paper runtime or an isolated Debug Core"
                    .to_owned(),
            ));
        }
        let expected_identity_hash =
            self.paper.runtime_identity_hash.as_ref().ok_or_else(|| {
                DaemonError::InvalidInput(
                    "Paper approval requires the daemon runtime identity".to_owned(),
                )
            })?;
        if request.identity.identity_hash()? != *expected_identity_hash {
            return Err(DaemonError::InvalidInput(
                "Paper approval runtime identity does not match the running daemon".to_owned(),
            ));
        }
        let qualification = request.qualification.clone().ok_or_else(|| {
            DaemonError::InvalidInput(
                "Paper approval requires a current model qualification report".to_owned(),
            )
        })?;
        let qualification_key = request.identity.qualification_key()?;
        let qualification_now = Utc::now();
        if !ModelQualificationGate::permits(&qualification_key, &qualification, qualification_now) {
            return Err(DaemonError::InvalidInput(
                "Paper approval model qualification is stale, incomplete, or for a different runtime"
                    .to_owned(),
            ));
        }
        for reference in qualification.evidence.references() {
            let artifact = self.store.artifact(&reference.artifact_id).map_err(|_| {
                DaemonError::InvalidInput(
                    "Paper approval model qualification evidence is not persisted".to_owned(),
                )
            })?;
            if artifact.kind != reference.kind {
                return Err(DaemonError::InvalidInput(
                    "Paper approval model qualification evidence kind does not match".to_owned(),
                ));
            }
            artifact.validate()?;
        }
        let session = chrono::NaiveDate::parse_from_str(&request.session_key, "%Y-%m-%d")
            .map_err(|_| DaemonError::InvalidInput("session_key must be YYYY-MM-DD".to_owned()))?;
        if request.operator.trim().is_empty()
            || request.reason.trim().is_empty()
            || request.max_notional_usd_cents <= 0
            || request.valid_hours <= 0
            || request.valid_hours > 24 * 7
        {
            return Err(DaemonError::InvalidInput(
                "invalid Paper approval scope".to_owned(),
            ));
        }
        let execution_policy = ExecutionPolicy::default();
        let maximum_notional = MoneyMicros::from_usd_cents(request.max_notional_usd_cents);
        if maximum_notional.0 > execution_policy.max_new_notional.0 {
            return Err(DaemonError::InvalidInput(
                "approval max notional exceeds execution policy".to_owned(),
            ));
        }
        let paper = AlpacaPaper::from_env().map_err(|error| {
            DaemonError::Unavailable(format!("construct Paper broker for approval: {error}"))
        })?;
        let account = paper.account().await.map_err(|error| {
            DaemonError::Unavailable(format!("read Paper account for approval: {error}"))
        })?;
        let broker_account_id = account
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| DaemonError::InvalidInput("Paper account id missing".to_owned()))?
            .to_owned();
        let now = Utc::now();
        let identity = request.identity;
        let manifest_payload = RuntimeManifest {
            schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
            code_revision: identity.code_revision,
            cargo_lock_hash: identity.cargo_lock_hash,
            config_hash: identity.config_hash,
            provider_id: identity.provider_id,
            model_id: identity.model_id,
            prompt_hash: identity.prompt_hash,
            contract_hash: identity.contract_hash,
            topology_hash: identity.topology_hash,
            decision_policy_hash: identity.decision_policy_hash,
            execution_policy_hash: identity.execution_policy_hash,
            evaluation_policy_hash: identity.evaluation_policy_hash,
            experiment_profile: identity.experiment_profile,
            rust_toolchain: identity.rust_toolchain,
            model_release_date: identity.model_release_date,
            model_knowledge_cutoff: identity.model_knowledge_cutoff,
            historical_evaluation_condition: identity.historical_evaluation_condition,
            model_routes_hash: identity.model_routes_hash,
            model_capability_hashes: identity.model_capability_hashes,
            model_capability_bundle_hash: identity.model_capability_bundle_hash,
            governance_component_hashes: identity.governance_component_hashes,
            governance_bundle_hash: identity.governance_bundle_hash,
            behavior_bundle_hash: None,
            market_data_feed: identity.market_data_feed,
            broker_account_id,
            maximum_notional,
            allowed_session_start: session,
            allowed_session_end: session,
            expires_at: now + Duration::hours(request.valid_hours),
            created_at: now,
        };
        self.store_executor
            .execute(move |store| -> Result<_> {
                let manifest_hash = manifest_payload.manifest_hash()?;
                let manifest = Artifact::new(
                    ArtifactKind::RuntimeManifest,
                    store.stage_json(&manifest_payload)?,
                    "runtime.manifest",
                    ArtifactLifecycle::Canonical,
                    ArtifactProvenance {
                        source_family: "akzio.operator".to_owned(),
                        observed_at: None,
                        retrieved_at: now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: None,
                    },
                    None,
                    vec![],
                    now,
                )?;
                let mut approval_payload = PaperLaunchApproval {
                    schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
                    operator_identity: request.operator,
                    runtime_manifest: ArtifactRef {
                        artifact_id: manifest.artifact_id.clone(),
                        kind: ArtifactKind::RuntimeManifest,
                    },
                    runtime_manifest_hash: manifest_hash.clone(),
                    scope: PaperApprovalScope::Canary,
                    reason: request.reason,
                    behavior_bundle_hash: None,
                    approved_at: now,
                    expires_at: manifest_payload.expires_at,
                    qualification: qualification.clone(),
                    mandate: ExecutionGatePolicy::default().mandate,
                    approval_hash: ContentHash::of_bytes(b"pending"),
                };
                approval_payload.approval_hash = approval_payload.unsigned_hash()?;
                let approval = Artifact::new(
                    ArtifactKind::PaperLaunchApproval,
                    store.stage_json(&approval_payload)?,
                    "operator.paper_approval",
                    ArtifactLifecycle::Canonical,
                    ArtifactProvenance {
                        source_family: "akzio.operator".to_owned(),
                        observed_at: None,
                        retrieved_at: now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: None,
                    },
                    None,
                    vec![approval_payload.runtime_manifest.clone()],
                    now,
                )?;
                store.write_paper_approval_binding(&manifest, &approval)?;
                Ok(PaperApprovalResponse {
                    session_key: request.session_key,
                    runtime_manifest_artifact_id: manifest.artifact_id,
                    runtime_manifest_hash: manifest_hash,
                    approval_artifact_id: approval.artifact_id,
                    approval_hash: approval_payload.approval_hash,
                    expires_at: approval_payload.expires_at,
                })
            })
            .await?
    }

    pub(crate) fn prepare_position_plan(
        &self,
        session_key: &str,
        now: DateTime<Utc>,
    ) -> Result<(akzio_store::WorkflowCommit, Vec<Artifact>)> {
        let run_id = RunId::new();
        let setup = self
            .paper
            .scheduler
            .paper_snapshot_artifacts(&run_id, session_key, now)?
            .into_iter()
            .map(|a| {
                let need: EvidenceNeed = serde_json::from_slice(&self.store.read_blob(&a.blob)?)?;
                Ok((a, need))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|(_, need)| {
                need.criticality() != akzio_domain::EvidenceCriticality::ExecutionSafety
            })
            .map(|(a, _)| a)
            .collect::<Vec<_>>();
        let dataset = setup
            .iter()
            .map(|a| ArtifactRef {
                artifact_id: a.artifact_id.clone(),
                kind: a.kind,
            })
            .collect::<Vec<_>>();
        let mut proposal = self
            .workflow
            .approved_research_proposal("paper.approved.v1")?;
        for task in proposal
            .tasks
            .values_mut()
            .filter(|t| t.recipe_id.as_str() == akzio_domain::RESEARCH_ANALYST_RECIPE_ID)
        {
            task.evidence_needs = dataset.clone();
        }
        let graph = self.workflow.lower(RunPurpose::PositionPlan, &proposal)?;
        Ok((
            self.workflow
                .prepare_workflow_commit(run_id, RunPurpose::PositionPlan, graph, now)?,
            setup,
        ))
    }

    pub async fn run_one(&self, worker_id: &str) -> Result<bool> {
        Ok(self
            .task_runtime
            .execute_ready_node(worker_id, akzio_store::TaskWorkload::Any, self)
            .await?)
    }
}
