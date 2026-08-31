impl Daemon {
    pub async fn approve_paper(
        &self,
        request: PaperApprovalRequest,
    ) -> Result<PaperApprovalResponse> {
        request.identity.validate()?;
        if !self.paper.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper approval requires auto_paper=true".to_owned(),
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
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
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
                    store.put_json(&manifest_payload)?,
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
                    schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
                    operator_identity: request.operator,
                    runtime_manifest: ArtifactRef {
                        artifact_id: manifest.artifact_id.clone(),
                        kind: ArtifactKind::RuntimeManifest,
                    },
                    runtime_manifest_hash: manifest_hash.clone(),
                    scope: PaperApprovalScope::Canary,
                    reason: request.reason,
                    approved_at: now,
                    expires_at: manifest_payload.expires_at,
                    approval_hash: ContentHash::of_bytes(b"pending"),
                };
                approval_payload.approval_hash = approval_payload.unsigned_hash()?;
                let approval = Artifact::new(
                    ArtifactKind::PaperLaunchApproval,
                    store.put_json(&approval_payload)?,
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

    /// Paper sessions are scheduler-owned and require a frozen session slot.
    /// The R5 daemon does not construct one directly, so this public submit
    /// surface rejects Paper before any workflow or broker side effect.
    pub fn submit_default(&self, purpose: RunPurpose) -> Result<RunId> {
        match purpose {
            RunPurpose::Debug | RunPurpose::PositionPlan | RunPurpose::PaperDryRun => {}
            RunPurpose::Paper => {
                return Err(DaemonError::InvalidInput(
                    "Paper runs are scheduler-owned and unavailable until the fenced scheduler is wired"
                        .to_owned(),
                ));
            }
            RunPurpose::Replay | RunPurpose::Shadow => {
                return Err(DaemonError::InvalidInput(
                    "Replay and Shadow runs must be created by their owning runtimes".to_owned(),
                ));
            }
        }

        let run_id = RunId::new();
        let graph = self.workflow.bootstrap(purpose, "active")?;
        self.workflow
            .submit(run_id.clone(), purpose, graph, Utc::now())?;
        Ok(run_id)
    }

    pub async fn run_one(&self, worker_id: &str) -> Result<bool> {
        let daemon = self.clone();
        Ok(self
            .task_runtime
            .run_one(worker_id, move |task| async move {
                daemon.execute_task(task).await
            })
            .await?)
    }
}
