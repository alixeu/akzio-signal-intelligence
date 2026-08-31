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
        #[cfg(test)]
        self.hit_failpoint(AgentFailpoint::BeforeToolCallPersist)?;
        self.store.write_task_artifact(
            permit,
            &call_artifact,
            LifecycleEventType::ToolCalled,
            now,
        )?;
        #[cfg(test)]
        self.hit_failpoint(AgentFailpoint::AfterToolCallPersist)?;

        match self.execute_tool_inner(permit, contract, grant, call, now) {
            Ok(result) => {
                let mut source_refs = vec![ArtifactRef {
                    artifact_id: call_artifact.artifact_id.clone(),
                    kind: ArtifactKind::ToolCall,
                }];
                source_refs.extend(result.artifacts.iter().map(|artifact| ArtifactRef {
                    artifact_id: artifact.artifact_id.clone(),
                    kind: artifact.kind,
                }));
                let result_artifact = Artifact::new(
                    ArtifactKind::ToolResult,
                    self.store.put_json(&json!({
                        "request_hash": request_hash,
                        "call_id": call.call_id,
                        "name": call.name,
                        "ok": true,
                        "value": result.value,
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
                    source_refs,
                    now,
                )?;
                #[cfg(test)]
                self.hit_failpoint(AgentFailpoint::BeforeToolResultPersist)?;
                self.store.write_task_artifact(
                    permit,
                    &result_artifact,
                    LifecycleEventType::ToolCompleted,
                    now,
                )?;
                #[cfg(test)]
                self.hit_failpoint(AgentFailpoint::AfterToolResultPersist)?;
                Ok(ToolResult {
                    value: json!({
                        "call_id": call.call_id,
                        "ok": true,
                        "value": result.value,
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
    ) -> ResearchResult<ContextReadResult> {
        if !grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let tool = contract
            .tool_specs
            .iter()
            .find(|spec| spec.name == call.name)
            .or_else(|| {
                (call.name == "read_artifact")
                    .then(|| {
                        contract
                            .tool_specs
                            .iter()
                            .find(|spec| spec.name == "read_document")
                    })
                    .flatten()
            })
            .ok_or_else(|| ResearchError::ToolNotGranted(call.name.clone()))?;
        if !contract
            .tool_grants
            .iter()
            .any(|grant| grant.kind == tool.kind)
        {
            return Err(ResearchError::ToolNotGranted(call.name.clone()));
        }
        let raw = tool.kind == akzio_domain::ToolKind::ReadRawEvidence;
        let result = match call.name.as_str() {
            "read_artifact" | "read_document" => {
                let artifact_id = strict_artifact_id_argument(&call.arguments, &call.name)?;
                if raw {
                    let (artifact, value) = self.context.read_raw_document(
                        permit,
                        contract,
                        grant,
                        &artifact_id,
                        now,
                    )?;
                    ContextReadResult {
                        artifacts: vec![artifact],
                        value,
                    }
                } else {
                    self.context
                        .read_document_result(permit, contract, grant, &artifact_id, now)?
                }
            }
            "read_range" => {
                let (artifact_id, start_byte, end_byte) =
                    strict_range_arguments(&call.arguments, &call.name)?;
                self.context.read_range(
                    permit,
                    contract,
                    grant,
                    &artifact_id,
                    start_byte,
                    end_byte,
                    now,
                )?
            }
            "search_context" => {
                let (query, max_results) = strict_search_arguments(&call.arguments, &call.name)?;
                self.context
                    .search_context(permit, contract, grant, &query, max_results, now)?
            }
            "read_claim_evidence" => {
                let claim_id = strict_artifact_id_argument(&call.arguments, &call.name)?;
                self.context
                    .read_claim_evidence(permit, contract, grant, &claim_id, now)?
            }
            "compare_sources" => {
                let artifact_ids = strict_artifact_ids_argument(&call.arguments, &call.name)?;
                self.context
                    .compare_sources(permit, contract, grant, &artifact_ids, now)?
            }
            _ => {
                let artifact_id = strict_artifact_id_argument(&call.arguments, &call.name)?;
                self.context
                    .read_document_result(permit, contract, grant, &artifact_id, now)?
            }
        };
        for artifact in &result.artifacts {
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
        }
        Ok(result)
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

fn strict_range_arguments(
    arguments: &Value,
    tool_name: &str,
) -> ResearchResult<(ArtifactId, usize, usize)> {
    let object = arguments
        .as_object()
        .filter(|object| object.len() == 3)
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!(
                "tool {tool_name} arguments do not match its strict schema"
            ))
        })?;
    let artifact_id = object
        .get("artifact_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!("tool {tool_name} omitted artifact_id"))
        })?;
    let start_byte = object
        .get("start_byte")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!("tool {tool_name} invalid start_byte"))
        })?;
    let end_byte = object
        .get("end_byte")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!("tool {tool_name} invalid end_byte"))
        })?;
    Ok((
        ArtifactId(akzio_domain::ContentHash::new(artifact_id)?),
        start_byte,
        end_byte,
    ))
}

fn strict_search_arguments(arguments: &Value, tool_name: &str) -> ResearchResult<(String, usize)> {
    let object = arguments
        .as_object()
        .filter(|object| object.len() == 2)
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!(
                "tool {tool_name} arguments do not match its strict schema"
            ))
        })?;
    let query = object
        .get("query")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ResearchError::InvalidOutput(format!("tool {tool_name} omitted query")))?;
    let max_results = object
        .get("max_results")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!("tool {tool_name} invalid max_results"))
        })?;
    Ok((query, max_results))
}

fn strict_artifact_ids_argument(
    arguments: &Value,
    tool_name: &str,
) -> ResearchResult<Vec<ArtifactId>> {
    let object = arguments
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!(
                "tool {tool_name} arguments do not match its strict schema"
            ))
        })?;
    object
        .get("artifact_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ResearchError::InvalidOutput(format!("tool {tool_name} omitted artifact_ids"))
        })?
        .iter()
        .map(|value| {
            let artifact_id = value.as_str().ok_or_else(|| {
                ResearchError::InvalidOutput(format!("tool {tool_name} invalid artifact_id"))
            })?;
            Ok(ArtifactId(akzio_domain::ContentHash::new(artifact_id)?))
        })
        .collect()
}

pub(super) fn tool_set_hash(
    request: &AgentModelRequest,
) -> ResearchResult<akzio_domain::ContentHash> {
    advertised_tool_set_hash(&request.tools, request.terminal.as_ref())
}

pub(super) fn advertised_tool_set_hash(
    tools: &[AgentToolDefinition],
    terminal: Option<&AgentTerminalDefinition>,
) -> ResearchResult<akzio_domain::ContentHash> {
    Ok(akzio_domain::content_hash_json(&json!({
        "tools": tools,
        "terminal": terminal,
    }))?)
}

pub(super) fn model_tool_definitions(
    context: &ContextBroker,
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
            let input_schema: Value = serde_json::from_slice(
                &context.read_authority_document(contract, &spec.input_schema)?,
            )?;
            let expected_schema =
                context_tool_input_schema(&spec.name).unwrap_or_else(artifact_id_tool_input_schema);
            if input_schema != expected_schema {
                return Err(ResearchError::InvalidToolSpec(format!(
                    "{} must use its strict context-tool input schema",
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
