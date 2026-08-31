#[derive(Debug, Clone, PartialEq, Eq)]
enum AgentRecoverySource {
    FreshRestart,
    Recovered(Vec<AttemptId>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AgentRecoveryUsage {
    latency_millis: u64,
    input_tokens: u64,
    output_tokens: u64,
}

impl AgentRecoveryUsage {
    fn record_failed(&mut self, request: &AgentModelRequest) -> Option<()> {
        self.input_tokens = self
            .input_tokens
            .saturating_add(u64::from(estimate_tokens(request).ok()?));
        Some(())
    }

    fn record(&mut self, request: &AgentModelRequest, response: &AgentModelTurn) -> Option<()> {
        let telemetry = response.telemetry.as_ref();
        let input_tokens = telemetry
            .and_then(|telemetry| telemetry.input_tokens)
            .or_else(|| estimate_tokens(request).ok().map(u64::from))?;
        let output_tokens = telemetry
            .and_then(|telemetry| telemetry.output_tokens)
            .or_else(|| estimate_turn_output_tokens(response).ok().map(u64::from))?;

        self.latency_millis = self.latency_millis.saturating_add(
            telemetry.map_or(0, |telemetry| telemetry.latency_millis),
        );
        self.input_tokens = self.input_tokens.saturating_add(input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(output_tokens);
        Some(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct AgentRecoveryCheckpoint {
    source: AgentRecoverySource,
    phase: AgentTurnPhase,
    next_model_turn: u16,
    continuation: Option<ModelContinuation>,
    pending_tool_outputs: Vec<ModelToolOutput>,
    trace_refs: Vec<ArtifactRef>,
    provider_calls: u32,
    tool_calls: u32,
    usage: AgentRecoveryUsage,
}

impl AgentRecoveryCheckpoint {
    fn fresh() -> Self {
        Self {
            source: AgentRecoverySource::FreshRestart,
            phase: AgentTurnPhase::Draft,
            next_model_turn: 0,
            continuation: None,
            pending_tool_outputs: vec![],
            trace_refs: vec![],
            provider_calls: 0,
            tool_calls: 0,
            usage: AgentRecoveryUsage::default(),
        }
    }

    #[cfg(test)]
    fn is_recovered(&self) -> bool {
        matches!(self.source, AgentRecoverySource::Recovered(_))
    }
}

#[derive(Debug, Clone)]
struct AgentRecoveryGuard {
    contract_hash: akzio_domain::ContentHash,
    context_manifest: akzio_domain::ContextManifestPayload,
    capability_snapshot_hash: akzio_domain::ContentHash,
    draft_tool_set_hash: akzio_domain::ContentHash,
    submit_tool_set_hash: akzio_domain::ContentHash,
}

impl AgentRecoveryGuard {
    fn tool_set_hash(&self, phase: AgentTurnPhase) -> &akzio_domain::ContentHash {
        match phase {
            AgentTurnPhase::Draft => &self.draft_tool_set_hash,
            AgentTurnPhase::Submit => &self.submit_tool_set_hash,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct StoredAgentTurnPayload {
    turn: u16,
    contract_hash: akzio_domain::ContentHash,
    context_manifest: ArtifactId,
    request_hash: akzio_domain::ContentHash,
    capability_snapshot: ModelCapabilitySnapshot,
    capability_snapshot_hash: akzio_domain::ContentHash,
    tool_set_hash: akzio_domain::ContentHash,
    request: AgentModelRequest,
    #[serde(default)]
    response: Option<AgentModelTurn>,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredToolCallPayload {
    request_hash: akzio_domain::ContentHash,
    call: AgentToolCall,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredToolResultPayload {
    request_hash: akzio_domain::ContentHash,
    call_id: String,
    name: String,
    ok: bool,
    value: Value,
}

#[derive(Debug, Clone)]
enum AgentRecoveryEvent {
    ProviderCallStarted,
    Turn {
        reference: ArtifactRef,
        manifest: akzio_domain::ContextManifestPayload,
        payload: Box<StoredAgentTurnPayload>,
        completed: bool,
    },
    ToolCall {
        reference: ArtifactRef,
        payload: StoredToolCallPayload,
    },
    ToolResult {
        reference: ArtifactRef,
        source_refs: Vec<ArtifactRef>,
        payload: StoredToolResultPayload,
    },
}

#[derive(Debug, Clone)]
struct ExpectedToolCall {
    request_hash: akzio_domain::ContentHash,
    call: AgentToolCall,
    artifact: Option<ArtifactRef>,
    output: Option<ModelToolOutput>,
}

struct AgentRecoveryReducer<'a> {
    guard: &'a AgentRecoveryGuard,
    checkpoint: AgentRecoveryCheckpoint,
    expected_tools: Vec<ExpectedToolCall>,
}

impl<'a> AgentRecoveryReducer<'a> {
    fn new(guard: &'a AgentRecoveryGuard) -> Self {
        Self {
            guard,
            checkpoint: AgentRecoveryCheckpoint::fresh(),
            expected_tools: vec![],
        }
    }

    fn fold(mut self, event: AgentRecoveryEvent) -> Option<Self> {
        match event {
            AgentRecoveryEvent::ProviderCallStarted => {
                self.checkpoint.provider_calls = self.checkpoint.provider_calls.saturating_add(1);
            }
            AgentRecoveryEvent::Turn {
                reference,
                manifest,
                payload,
                completed,
            } => self.fold_turn(reference, manifest, *payload, completed)?,
            AgentRecoveryEvent::ToolCall { reference, payload } => {
                let expected = self.expected_tools.iter_mut().find(|expected| {
                    expected.call.call_id == payload.call.call_id && expected.artifact.is_none()
                })?;
                if expected.request_hash != payload.request_hash || expected.call != payload.call {
                    return None;
                }
                expected.artifact = Some(reference);
                self.checkpoint.tool_calls = self.checkpoint.tool_calls.saturating_add(1);
            }
            AgentRecoveryEvent::ToolResult {
                reference,
                source_refs,
                payload,
            } => {
                let expected = self.expected_tools.iter_mut().find(|expected| {
                    expected.call.call_id == payload.call_id && expected.output.is_none()
                })?;
                let call = expected.artifact.as_ref()?;
                if !payload.ok
                    || expected.request_hash != payload.request_hash
                    || expected.call.name != payload.name
                    || !source_refs.contains(call)
                {
                    return None;
                }
                expected.output = Some(ModelToolOutput {
                    call_id: payload.call_id,
                    output: payload.value,
                });
                self.checkpoint.trace_refs.push(reference);
                self.finish_tool_batch();
            }
        }
        Some(self)
    }

    fn fold_turn(
        &mut self,
        reference: ArtifactRef,
        manifest: akzio_domain::ContextManifestPayload,
        payload: StoredAgentTurnPayload,
        completed: bool,
    ) -> Option<()> {
        if !self.expected_tools.is_empty()
            || payload.turn != self.checkpoint.next_model_turn
            || payload.contract_hash != self.guard.contract_hash
            || payload.request.contract_hash != self.guard.contract_hash
            || payload.context_manifest != payload.request.manifest_artifact_id
            || manifest != self.guard.context_manifest
            || payload.request_hash != model_request_hash(&payload.request).ok()?
            || payload.capability_snapshot_hash
                != capability_snapshot_hash(&payload.capability_snapshot).ok()?
            || payload.capability_snapshot_hash != self.guard.capability_snapshot_hash
            || payload.tool_set_hash != tool_set_hash(&payload.request).ok()?
            || &payload.tool_set_hash != self.guard.tool_set_hash(payload.request.phase)
            || payload.request.phase != self.checkpoint.phase
            || payload.request.continuation != self.checkpoint.continuation
            || payload.request.tool_outputs != self.checkpoint.pending_tool_outputs
        {
            return None;
        }

        self.checkpoint.trace_refs.push(reference);
        let Some(response) = payload.response else {
            (!completed).then_some(())?;
            return self.checkpoint.usage.record_failed(&payload.request);
        };
        if !completed {
            return None;
        }

        self.checkpoint.pending_tool_outputs.clear();
        self.checkpoint.continuation = Some(response.continuation.clone());
        self.checkpoint.usage.record(&payload.request, &response)?;

        match payload.request.phase {
            AgentTurnPhase::Draft if response.terminal_submission.is_none() => {
                self.checkpoint.next_model_turn = payload.turn.saturating_add(1);
                if response.tool_calls.is_empty() {
                    response
                        .assistant_text
                        .as_deref()
                        .is_some_and(|text| !text.trim().is_empty())
                        .then_some(())?;
                    self.checkpoint.phase = AgentTurnPhase::Submit;
                } else {
                    let mut call_ids = BTreeSet::new();
                    self.expected_tools = response
                        .tool_calls
                        .into_iter()
                        .map(|call| {
                            call_ids.insert(call.call_id.clone()).then_some(ExpectedToolCall {
                                request_hash: payload.request_hash.clone(),
                                call,
                                artifact: None,
                                output: None,
                            })
                        })
                        .collect::<Option<_>>()?;
                }
            }
            AgentTurnPhase::Draft | AgentTurnPhase::Submit => return None,
        }
        Some(())
    }

    fn finish_tool_batch(&mut self) {
        if !self.expected_tools.is_empty()
            && self
                .expected_tools
                .iter()
                .all(|expected| expected.output.is_some())
        {
            self.checkpoint.pending_tool_outputs = self
                .expected_tools
                .iter_mut()
                .filter_map(|expected| expected.output.take())
                .collect();
            self.expected_tools.clear();
        }
    }

    fn finish(mut self, lineage: Vec<AttemptId>) -> Option<AgentRecoveryCheckpoint> {
        self.finish_tool_batch();
        if !self.expected_tools.is_empty() || self.checkpoint.continuation.is_none() {
            return None;
        }
        self.checkpoint.source = AgentRecoverySource::Recovered(lineage);
        Some(self.checkpoint)
    }
}

fn agent_recovery_checkpoint(
    store: &V2Store,
    permit: &TaskWritePermit,
    guard: &AgentRecoveryGuard,
) -> ResearchResult<AgentRecoveryCheckpoint> {
    let Some(lineage) = recovery_lineage(store, permit)? else {
        return Ok(AgentRecoveryCheckpoint::fresh());
    };
    let Some(events) = load_recovery_events(store, permit, &lineage)? else {
        return Ok(AgentRecoveryCheckpoint::fresh());
    };
    let Some(reducer) = events
        .into_iter()
        .try_fold(AgentRecoveryReducer::new(guard), AgentRecoveryReducer::fold)
    else {
        return Ok(AgentRecoveryCheckpoint::fresh());
    };
    Ok(reducer
        .finish(lineage)
        .unwrap_or_else(AgentRecoveryCheckpoint::fresh))
}

fn recovery_lineage(
    store: &V2Store,
    permit: &TaskWritePermit,
) -> ResearchResult<Option<Vec<AttemptId>>> {
    let mut child = permit.attempt_id.clone();
    let mut seen = BTreeSet::from([child.clone()]);
    let mut lineage = vec![];
    while let Some(relation) = store.attempt_relation(&child)? {
        if relation.relation != akzio_domain::AttemptRelationKind::Recovery
            || relation.run_id != permit.run_id
            || relation.task_id != permit.task_id
            || !seen.insert(relation.parent_attempt_id.clone())
        {
            return Ok(None);
        }
        child = relation.parent_attempt_id;
        lineage.push(child.clone());
    }
    lineage.reverse();
    Ok((!lineage.is_empty()).then_some(lineage))
}

fn load_recovery_events(
    store: &V2Store,
    permit: &TaskWritePermit,
    lineage: &[AttemptId],
) -> ResearchResult<Option<Vec<AgentRecoveryEvent>>> {
    let mut loaded = vec![];
    for attempt_id in lineage {
        for event in store.attempt_events(&permit.run_id, &permit.task_id, attempt_id)? {
            let event_type = event.lifecycle_kind()?;
            if event_type == LifecycleEventType::AgentTurnStarted {
                loaded.push(AgentRecoveryEvent::ProviderCallStarted);
                continue;
            }
            let expected_kind = match event_type {
                LifecycleEventType::AgentTurnCompleted
                | LifecycleEventType::AgentTurnFailed
                | LifecycleEventType::AgentTurnRetryableFailed => ArtifactKind::AgentTurn,
                LifecycleEventType::ToolCalled => ArtifactKind::ToolCall,
                LifecycleEventType::ToolCompleted | LifecycleEventType::ToolFailed => {
                    ArtifactKind::ToolResult
                }
                _ => continue,
            };
            let Some(artifact_id) = event.artifact_id else {
                return Ok(None);
            };
            let artifact = store.artifact(&artifact_id)?;
            let expected_origin = artifact.origin.as_ref().is_some_and(|origin| {
                origin.run_id.as_ref() == Some(&permit.run_id)
                    && origin.task_id.as_ref() == Some(&permit.task_id)
                    && origin.attempt_id.as_ref() == Some(attempt_id)
                    && origin.contract_hash.as_ref() == permit.contract_hash.as_ref()
            });
            if artifact.kind != expected_kind || !expected_origin || artifact.validate().is_err() {
                return Ok(None);
            }
            let bytes = store.read_blob(&artifact.blob)?;
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            };
            let recovery_event = match event_type {
                LifecycleEventType::AgentTurnCompleted
                | LifecycleEventType::AgentTurnFailed
                | LifecycleEventType::AgentTurnRetryableFailed => {
                    let Ok(payload) = serde_json::from_slice::<StoredAgentTurnPayload>(&bytes) else {
                        return Ok(None);
                    };
                    let manifest_artifact = store.artifact(&payload.context_manifest)?;
                    if manifest_artifact.kind != ArtifactKind::ContextManifest {
                        return Ok(None);
                    }
                    let Ok(manifest) = serde_json::from_slice::<
                        akzio_domain::ContextManifestPayload,
                    >(&store.read_blob(&manifest_artifact.blob)?) else {
                        return Ok(None);
                    };
                    AgentRecoveryEvent::Turn {
                        reference,
                        manifest,
                        payload: Box::new(payload),
                        completed: event_type == LifecycleEventType::AgentTurnCompleted,
                    }
                }
                LifecycleEventType::ToolCalled => {
                    let Ok(payload) = serde_json::from_slice(&bytes) else {
                        return Ok(None);
                    };
                    AgentRecoveryEvent::ToolCall { reference, payload }
                }
                LifecycleEventType::ToolCompleted => {
                    let Ok(payload) = serde_json::from_slice(&bytes) else {
                        return Ok(None);
                    };
                    AgentRecoveryEvent::ToolResult {
                        reference,
                        source_refs: artifact.source_refs,
                        payload,
                    }
                }
                LifecycleEventType::ToolFailed => return Ok(None),
                _ => unreachable!("event type filtered above"),
            };
            loaded.push(recovery_event);
        }
    }
    Ok(Some(loaded))
}
