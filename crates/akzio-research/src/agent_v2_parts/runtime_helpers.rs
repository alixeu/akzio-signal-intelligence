impl AgentRuntime {
    fn extract_deliberation(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        output: Value,
        now: DateTime<Utc>,
    ) -> ResearchResult<(Value, Option<Artifact>)> {
        if contract.deliberation_policy == DeliberationPolicy::Disabled {
            return Ok((output, None));
        }
        let mut envelope: AgentOutputEnvelope =
            serde_json::from_value(output).map_err(|error| {
                ResearchError::InvalidOutput(format!("deliberation envelope: {error}"))
            })?;
        envelope.deliberation.assessment_source = Some("model_assessed".to_owned());
        envelope
            .deliberation
            .validate_model_assessment()
            .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;

        let selected = manifest
            .payload
            .selections
            .iter()
            .map(|selection| {
                (
                    selection.artifact.artifact_id.clone(),
                    selection.artifact.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut basis_refs = Vec::new();
        for basis_id in &envelope.deliberation.basis_artifact_ids {
            if *basis_id == manifest.artifact.artifact_id {
                basis_refs.push(ArtifactRef {
                    artifact_id: basis_id.clone(),
                    kind: ArtifactKind::ContextManifest,
                });
            } else if let Some(reference) = selected.get(basis_id) {
                basis_refs.push(reference.clone());
            } else {
                return Err(ResearchError::InvalidOutput(
                    "deliberation basis is outside the ContextManifest".to_owned(),
                ));
            }
        }
        let note = Artifact::new(
            ArtifactKind::DeliberationNote,
            self.store.put_json(&envelope.deliberation)?,
            format!("agent.deliberation.{}", contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                // Provenance confidence is Rust-owned. It must not carry the
                // model's self-reported `deliberation.confidence_ppm`, because
                // ContextManifest selection ranks candidates by this field
                // (`akzio-context` broker_parts/manifest.rs), which would let an
                // agent raise its own note's selection priority in later turns.
                // The self-report stays inside the note payload.
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(contract.contract_hash.clone()),
            },
            Some(permit.artifact_origin()),
            std::iter::once(ArtifactRef {
                artifact_id: manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            })
            .chain(basis_refs)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
            now,
        )?;
        Ok((envelope.result, Some(note)))
    }

    async fn context_values(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
    ) -> ResearchResult<Vec<Value>> {
        if !manifest.grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let context = self.context.clone();
        let permit = permit.clone();
        let contract = contract.clone();
        let manifest = manifest.clone();
        Ok(self
            .store_executor
            .execute(move |_| {
                manifest
                    .payload
                    .selections
                    .iter()
                    .map(|selection| {
                        let (artifact, value) = context.read_document(
                            &permit,
                            &contract,
                            &manifest.grant,
                            &selection.artifact.artifact_id,
                            now,
                        )?;
                        Ok(json!({
                            "artifact_id": artifact.artifact_id,
                            "kind": artifact.kind,
                            "provenance": artifact.provenance,
                            "value": value,
                        }))
                    })
                    .collect::<std::result::Result<Vec<_>, ContextError>>()
            })
            .await??)
    }

    async fn record_turn(
        &self,
        record: TurnRecord,
        request: &AgentModelRequest,
        response: &AgentModelTurn,
        runtime_snapshot: &AgentTurnRuntimeSnapshot,
    ) -> ResearchResult<Artifact> {
        let request_hash = model_request_hash(request)?;
        let request = request.clone();
        let response = response.clone();
        let runtime_snapshot = runtime_snapshot.clone();
        self.store_executor
            .execute(move |store| {
                let artifact = Artifact::new(
                    ArtifactKind::AgentTurn,
                    store.put_json(&json!({
                        "turn": record.turn,
                        "attempt": record.attempt,
                        "contract_hash": &record.contract.contract_hash,
                        "context_manifest": &record.manifest.artifact.artifact_id,
                        "request_hash": request_hash,
                        "capability_snapshot": runtime_snapshot.capability,
                        "capability_snapshot_hash": runtime_snapshot.capability_hash,
                        "budget_policy": runtime_snapshot.budget_policy,
                        "budget_policy_hash": runtime_snapshot.budget_policy_hash,
                        "tool_set_hash": runtime_snapshot.tool_set_hash,
                        "request": request,
                        "response": response,
                    }))?,
                    format!("agent.turn.{}", record.contract.purpose.as_str()),
                    ArtifactLifecycle::RunScoped,
                    ArtifactProvenance {
                        source_family: "akzio.agent".to_owned(),
                        observed_at: None,
                        retrieved_at: record.now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: Some(record.contract.contract_hash.clone()),
                    },
                    Some(record.permit.artifact_origin()),
                    vec![ArtifactRef {
                        artifact_id: record.manifest.artifact.artifact_id.clone(),
                        kind: ArtifactKind::ContextManifest,
                    }],
                    record.now,
                )?;
                store.write_task_artifact(
                    &record.permit,
                    &artifact,
                    LifecycleEventType::AgentTurnCompleted,
                    record.now,
                )?;
                Ok::<_, ResearchError>(artifact)
            })
            .await?
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_failed_turn(
        &self,
        record: TurnRecord,
        request: &AgentModelRequest,
        error_class: &str,
        error_detail: Option<Value>,
        model_debug: Option<&ModelCallTrace>,
        will_retry: bool,
        runtime_snapshot: &AgentTurnRuntimeSnapshot,
    ) -> ResearchResult<Artifact> {
        let request_hash = model_request_hash(request)?;
        let request = request.clone();
        let error_class = error_class.to_owned();
        let model_debug = model_debug.cloned();
        let runtime_snapshot = runtime_snapshot.clone();
        self.store_executor
            .execute(move |store| {
                let mut trace = json!({
                    "turn": record.turn,
                    "attempt": record.attempt,
                    "contract_hash": &record.contract.contract_hash,
                    "context_manifest": &record.manifest.artifact.artifact_id,
                    "request_hash": request_hash,
                    "capability_snapshot": runtime_snapshot.capability,
                    "capability_snapshot_hash": runtime_snapshot.capability_hash,
                    "budget_policy": runtime_snapshot.budget_policy,
                    "budget_policy_hash": runtime_snapshot.budget_policy_hash,
                    "tool_set_hash": runtime_snapshot.tool_set_hash,
                    "request": request,
                    "error_class": error_class,
                    "will_retry": will_retry,
                });
                if let Some(error_detail) = error_detail {
                    trace["error_detail"] = error_detail;
                }
                if let Some(model_debug) = model_debug {
                    trace["model_debug"] = serde_json::to_value(model_debug)?;
                }
                let artifact = Artifact::new(
                    ArtifactKind::AgentTurn,
                    store.put_json(&trace)?,
                    format!("agent.turn.{}", record.contract.purpose.as_str()),
                    ArtifactLifecycle::RunScoped,
                    ArtifactProvenance {
                        source_family: "akzio.agent".to_owned(),
                        observed_at: None,
                        retrieved_at: record.now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: Some(record.contract.contract_hash.clone()),
                    },
                    Some(record.permit.artifact_origin()),
                    vec![ArtifactRef {
                        artifact_id: record.manifest.artifact.artifact_id.clone(),
                        kind: ArtifactKind::ContextManifest,
                    }],
                    record.now,
                )?;
                store.write_task_artifact(
                    &record.permit,
                    &artifact,
                    if will_retry {
                        LifecycleEventType::AgentTurnRetryableFailed
                    } else {
                        LifecycleEventType::AgentTurnFailed
                    },
                    record.now,
                )?;
                Ok::<_, ResearchError>(artifact)
            })
            .await?
    }
}
