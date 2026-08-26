use super::*;

impl AgentRuntime {
    pub(super) fn execute_tool(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        call: &AgentToolCall,
        request_hash: &akzio_domain::ContentHash,
        now: DateTime<Utc>,
    ) -> ResearchResult<ToolResult> {
        let call_artifact = Artifact::new(
            ArtifactKind::ToolCall,
            self.store.put_json(&json!({
                "request_hash": request_hash,
                "call": call,
            }))?,
            "agent.tool",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.tool".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(contract.contract_hash.clone()),
            },
            Some(permit.artifact_origin()),
            vec![ArtifactRef {
                artifact_id: grant.manifest_artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            now,
        )?;
        self.store.write_task_artifact(
            permit,
            &call_artifact,
            LifecycleEventType::ToolCalled,
            now,
        )?;

        match self.execute_tool_inner(permit, contract, grant, call, now) {
            Ok((artifact, value)) => {
                let result_artifact = Artifact::new(
                    ArtifactKind::ToolResult,
                    self.store.put_json(&json!({
                        "request_hash": request_hash,
                        "call_id": call.call_id,
                        "name": call.name,
                        "ok": true,
                        "value": value,
                    }))?,
                    "agent.tool",
                    ArtifactLifecycle::RunScoped,
                    ArtifactProvenance {
                        source_family: "akzio.tool".to_owned(),
                        observed_at: None,
                        retrieved_at: now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: Some(contract.contract_hash.clone()),
                    },
                    Some(permit.artifact_origin()),
                    vec![
                        ArtifactRef {
                            artifact_id: call_artifact.artifact_id.clone(),
                            kind: ArtifactKind::ToolCall,
                        },
                        ArtifactRef {
                            artifact_id: artifact.artifact_id.clone(),
                            kind: artifact.kind,
                        },
                    ],
                    now,
                )?;
                self.store.write_task_artifact(
                    permit,
                    &result_artifact,
                    LifecycleEventType::ToolCompleted,
                    now,
                )?;
                Ok(ToolResult {
                    value: json!({
                        "call_id": call.call_id,
                        "artifact_id": artifact.artifact_id,
                        "kind": artifact.kind,
                        "ok": true,
                        "value": value,
                    }),
                    artifact: result_artifact,
                })
            }
            Err(error) => {
                let result_artifact = Artifact::new(
                    ArtifactKind::ToolResult,
                    self.store.put_json(&json!({
                        "request_hash": request_hash,
                        "call_id": call.call_id,
                        "name": call.name,
                        "ok": false,
                        "error": {
                            "code": tool_error_code(&error),
                            "message": error.to_string(),
                        },
                    }))?,
                    "agent.tool",
                    ArtifactLifecycle::RunScoped,
                    ArtifactProvenance {
                        source_family: "akzio.tool".to_owned(),
                        observed_at: None,
                        retrieved_at: now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: Some(contract.contract_hash.clone()),
                    },
                    Some(permit.artifact_origin()),
                    vec![ArtifactRef {
                        artifact_id: call_artifact.artifact_id.clone(),
                        kind: ArtifactKind::ToolCall,
                    }],
                    now,
                )?;
                self.store.write_task_artifact(
                    permit,
                    &result_artifact,
                    LifecycleEventType::ToolFailed,
                    now,
                )?;
                Err(error)
            }
        }
    }

    pub(super) fn execute_tool_inner(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        grant: &ReadGrant,
        call: &AgentToolCall,
        now: DateTime<Utc>,
    ) -> ResearchResult<(Artifact, Value)> {
        if !grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let tool = contract
            .tool_specs
            .iter()
            .find(|spec| spec.name == call.name)
            .ok_or_else(|| ResearchError::ToolNotGranted(call.name.clone()))?;
        let artifact_id = strict_artifact_id_argument(&call.arguments, &call.name)?;
        if !contract
            .tool_grants
            .iter()
            .any(|grant| grant.kind == tool.kind)
        {
            return Err(ResearchError::ToolNotGranted(call.name.clone()));
        }
        let raw = tool.kind == akzio_domain::ToolKind::ReadRawEvidence;
        let (artifact, value) = if raw {
            self.context
                .read_raw_document(permit, contract, grant, &artifact_id, now)?
        } else {
            self.context
                .read_document(permit, contract, grant, &artifact_id, now)?
        };
        if !contract
            .tool_grants
            .iter()
            .filter(|tool_grant| tool_grant.kind == tool.kind)
            .any(|tool_grant| {
                tool_grant.allowed_sources.is_empty()
                    || tool_grant
                        .allowed_sources
                        .iter()
                        .any(|source| source == &artifact.provenance.source_family)
            })
        {
            return Err(ResearchError::ToolSourceNotGranted {
                tool: call.name.clone(),
                source_family: artifact.provenance.source_family.clone(),
            });
        }
        Ok((artifact, value))
    }
}

pub(super) fn tool_error_code(error: &ResearchError) -> &'static str {
    match error {
        ResearchError::GrantPermitMismatch => "grant_permit_mismatch",
        ResearchError::ToolNotGranted(_) => "tool_not_granted",
        ResearchError::ToolSourceNotGranted { .. } => "tool_source_not_granted",
        ResearchError::InvalidOutput(_) => "invalid_tool_arguments",
        ResearchError::Context(_) => "context_read_rejected",
        _ => "tool_execution_failed",
    }
}

pub(super) fn strict_artifact_id_argument(
    arguments: &Value,
    tool_name: &str,
) -> ResearchResult<ArtifactId> {
    let object = arguments.as_object().ok_or_else(|| {
        ResearchError::InvalidOutput(format!(
            "tool {tool_name} arguments do not match its strict schema"
        ))
    })?;
    if object.len() != 1 {
        return Err(ResearchError::InvalidOutput(format!(
            "tool {tool_name} arguments do not match its strict schema"
        )));
    }
    let artifact_id = object
        .get("artifact_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!("tool {tool_name} omitted artifact_id"))
        })?;
    Ok(ArtifactId(akzio_domain::ContentHash::new(artifact_id)?))
}

pub(super) fn tool_set_hash(
    request: &AgentModelRequest,
) -> ResearchResult<akzio_domain::ContentHash> {
    Ok(akzio_domain::content_hash_json(&json!({
        "tools": request.tools,
        "terminal": request.terminal,
    }))?)
}

pub(super) fn model_tool_definitions(
    store: &V2Store,
    contract: &AgentContract,
) -> ResearchResult<Vec<AgentToolDefinition>> {
    contract
        .tool_specs
        .iter()
        .map(|spec| {
            if spec.name == TERMINAL_SUBMISSION_TOOL {
                return Err(ResearchError::InvalidToolSpec(format!(
                    "{} is reserved for terminal submission",
                    spec.name
                )));
            }
            let input_schema: Value =
                serde_json::from_slice(&store.read_blob(&spec.input_schema)?)?;
            if input_schema != artifact_id_tool_input_schema() {
                return Err(ResearchError::InvalidToolSpec(format!(
                    "{} must use the strict artifact_id input schema",
                    spec.name
                )));
            }
            Ok(AgentToolDefinition {
                name: spec.name.clone(),
                description: spec.description.clone(),
                input_schema,
                strict: spec.strict,
            })
        })
        .collect()
}
