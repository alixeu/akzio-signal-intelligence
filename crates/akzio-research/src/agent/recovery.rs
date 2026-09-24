// 文件导读：Recovery 通过 Store/CAS 中的 Attempt、AgentTurn、ToolCall/Result 和 Acceptance 事件重放 Agent 状态；
// 它保留已发生的 provider/tool 预算与 lineage，任何哈希、lease、phase 或 payload 不一致都 fail closed。

#[derive(Debug, Clone, PartialEq, Eq)]
enum AgentRecoverySource {
    FreshRestart,
    Recovered(Vec<AttemptId>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecoveryUsageFailure {
    Unknown,
    Missing(ModelUsage),
    Inconsistent,
    OutputLimitExceeded { actual: u32, maximum: u32 },
    CostOverflow,
}

impl RecoveryUsageFailure {
    fn error(&self) -> ResearchError {
        // 失败枚举只把恢复时已确认的 usage/limit 问题转换为 ResearchError；Unknown 不会被降级成成功或免费重试。
        match self {
            Self::Unknown => ResearchError::ProviderUsageUnknown,
            Self::Missing(usage) => ResearchError::ProviderUsageMissing {
                usage: usage.clone(),
                trace: None,
            },
            Self::Inconsistent => ResearchError::InvalidProviderUsage,
            Self::OutputLimitExceeded { actual, maximum } => {
                ResearchError::ProviderOutputLimitExceeded {
                    actual: *actual,
                    maximum: *maximum,
                }
            }
            Self::CostOverflow => ResearchError::CostOverflow,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentRecoveryUsage {
    latency_millis: u64,
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    cost_micros: u64,
    cost_complete: bool,
    failure: Option<RecoveryUsageFailure>,
}

impl Default for AgentRecoveryUsage {
    fn default() -> Self {
        Self {
            latency_millis: 0,
            input_tokens: 0,
            cached_input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            cost_micros: 0,
            cost_complete: true,
            failure: None,
        }
    }
}

impl AgentRecoveryUsage {
    fn record_failed(
        &mut self,
        request: &AgentModelRequest,
        policy: &ModelBudgetPolicy,
    ) -> Option<()> {
        // 没有 provider usage 时仍累计可估算 input；pricing 存在则标记 cost 不完整，防止恢复获得额外预算。
        self.failure.get_or_insert(RecoveryUsageFailure::Unknown);
        self.input_tokens = self
            .input_tokens
            .saturating_add(u64::from(estimate_tokens(request).ok()?));
        if policy.pricing.is_some() {
            self.cost_complete = false;
        }
        Some(())
    }

    fn record_failed_usage(
        &mut self,
        request: &AgentModelRequest,
        usage: &ModelUsage,
        policy: &ModelBudgetPolicy,
    ) -> Option<()> {
        // 部分 usage 可以保留已知 token，但缺失/不一致字段和成本溢出必须留在 failure 中，不能用默认值掩盖。
        if usage.input_tokens.is_none() || usage.output_tokens.is_none() {
            self.failure
                .get_or_insert_with(|| RecoveryUsageFailure::Missing(usage.clone()));
        }
        if usage
            .cached_input_tokens
            .zip(usage.input_tokens)
            .is_some_and(|(detail, total)| detail > total)
            || usage
                .reasoning_tokens
                .zip(usage.output_tokens)
                .is_some_and(|(detail, total)| detail > total)
        {
            self.failure
                .get_or_insert(RecoveryUsageFailure::Inconsistent);
        }
        let resolved = resolve_model_usage(estimate_tokens(request).ok()?, 0, None);
        self.input_tokens = self
            .input_tokens
            .saturating_add(usage.input_tokens.unwrap_or(resolved.input_tokens));
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(usage.cached_input_tokens.unwrap_or_default());
        self.output_tokens = self
            .output_tokens
            .saturating_add(usage.output_tokens.unwrap_or_default());
        self.reasoning_tokens = self
            .reasoning_tokens
            .saturating_add(usage.reasoning_tokens.unwrap_or_default());
        if let Some(pricing) = &policy.pricing {
            let input_tokens = u32::try_from(usage.input_tokens.unwrap_or(resolved.input_tokens))
                .unwrap_or(u32::MAX);
            let output_tokens =
                u32::try_from(usage.output_tokens.unwrap_or_default()).unwrap_or(u32::MAX);
            match usage_cost_micros(
                resolve_model_usage(input_tokens, output_tokens, None),
                pricing,
            ) {
                Ok(cost) => self.cost_micros = self.cost_micros.saturating_add(cost),
                Err(_) => {
                    self.failure
                        .get_or_insert(RecoveryUsageFailure::CostOverflow);
                }
            }
            self.cost_complete = false;
        }
        Some(())
    }

    fn record(
        &mut self,
        request: &AgentModelRequest,
        response: &AgentModelTurn,
        policy: &ModelBudgetPolicy,
    ) -> Option<()> {
        // 已完成 Provider event 按同一 resolve_model_usage/price 规则重算；这确保 replay 与 live accounting 拒绝边界一致。
        let usage = resolve_model_usage(
            estimate_tokens(request).ok()?,
            estimate_turn_output_tokens(response).ok()?,
            response.telemetry.as_ref(),
        );
        // Replaying a completed Provider event must preserve the same rejection
        // boundary as live accounting, including when no pricing is configured.
        if usage
            .cached_input_tokens
            .is_some_and(|detail| detail > usage.input_tokens)
            || usage
                .reasoning_tokens
                .is_some_and(|detail| detail > usage.output_tokens)
        {
            self.failure
                .get_or_insert(RecoveryUsageFailure::Inconsistent);
        }
        if let Some(actual) = response
            .telemetry
            .as_ref()
            .and_then(|t| t.output_tokens)
            .filter(|actual| *actual > u64::from(request.max_output_tokens))
        {
            self.failure
                .get_or_insert(RecoveryUsageFailure::OutputLimitExceeded {
                    actual: u32::try_from(actual).unwrap_or(u32::MAX),
                    maximum: request.max_output_tokens,
                });
        }
        self.latency_millis = self.latency_millis.saturating_add(
            response
                .telemetry
                .as_ref()
                .map_or(0, |telemetry| telemetry.latency_millis),
        );
        self.input_tokens = self.input_tokens.saturating_add(usage.input_tokens);
        self.cached_input_tokens = self
            .cached_input_tokens
            .saturating_add(usage.cached_input_tokens.unwrap_or_default());
        self.output_tokens = self.output_tokens.saturating_add(usage.output_tokens);
        self.reasoning_tokens = self
            .reasoning_tokens
            .saturating_add(usage.reasoning_tokens.unwrap_or_default());
        if let Some(pricing) = &policy.pricing {
            match usage_cost_micros(usage, pricing) {
                Ok(cost) => self.cost_micros = self.cost_micros.saturating_add(cost),
                Err(_) => {
                    self.failure
                        .get_or_insert(RecoveryUsageFailure::CostOverflow);
                }
            }
        }
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
    submit_call_id: Option<String>,
    submission_attempts: u8,
    provider_calls: u32,
    tool_calls: u32,
    usage: AgentRecoveryUsage,
}

impl AgentRecoveryCheckpoint {
    fn fresh() -> Self {
        // Fresh checkpoint 从初始 phase 开始且没有 continuation/provider/tool 记录；一旦读到外部事件就不能再假装 fresh。
        Self {
            source: AgentRecoverySource::FreshRestart,
            phase: AgentTurnPhase::Draft,
            next_model_turn: 0,
            continuation: None,
            pending_tool_outputs: vec![],
            trace_refs: vec![],
            submit_call_id: None,
            submission_attempts: 0,
            provider_calls: 0,
            tool_calls: 0,
            usage: AgentRecoveryUsage::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct AgentRecoveryGuard {
    initial_phase: AgentTurnPhase,
    deliberation_repair_tool_set_hash: Option<akzio_domain::ContentHash>,
    contract_hash: akzio_domain::ContentHash,
    context_manifest: akzio_domain::ContextManifestPayload,
    read_grant_identity: akzio_domain::ContentHash,
    context_materialization_identity: akzio_domain::ContentHash,
    capability_snapshot_hash: akzio_domain::ContentHash,
    budget_policy_hash: akzio_domain::ContentHash,
    draft_tool_set_hash: akzio_domain::ContentHash,
    submit_tool_set_hash: akzio_domain::ContentHash,
}

impl AgentRecoveryGuard {
    fn fresh_checkpoint(&self) -> AgentRecoveryCheckpoint {
        // Guard 冻结 Contract、Context、capability、budget 和 phase/tool identity，恢复只能沿这组身份继续。
        AgentRecoveryCheckpoint { phase: self.initial_phase, ..AgentRecoveryCheckpoint::fresh() }
    }
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
    #[serde(default)]
    budget_policy: ModelBudgetPolicy,
    #[serde(default)]
    budget_policy_hash: Option<akzio_domain::ContentHash>,
    tool_set_hash: akzio_domain::ContentHash,
    request: AgentModelRequest,
    #[serde(default)]
    response: Option<AgentModelTurn>,
    #[serde(default)]
    error_detail: Option<Value>,
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
    ProviderCallStarted {
        attempt_id: AttemptId,
        cursor: i64,
    },
    ProviderCallFinished {
        attempt_id: AttemptId,
        start_cursor: i64,
    },
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
    StageAcceptance(akzio_domain::StageAcceptance),
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
    pending_provider_calls: BTreeSet<(AttemptId, i64)>,
}

impl<'a> AgentRecoveryReducer<'a> {
    fn new(guard: &'a AgentRecoveryGuard) -> Self {
        // Reducer 是按事件顺序消费的值状态；expected_tools/pending_provider_calls 用来阻止重复或跨 Attempt 关闭。
        Self {
            guard,
            checkpoint: guard.fresh_checkpoint(),
            expected_tools: vec![],
            pending_provider_calls: BTreeSet::new(),
        }
    }

    fn fold(mut self, event: AgentRecoveryEvent) -> Option<Self> {
        // 每类事件都必须匹配当前 checkpoint：provider start/finish、turn、tool 和 acceptance 不可交换或跨身份配对。
        match event {
            AgentRecoveryEvent::ProviderCallStarted { attempt_id, cursor } => {
                self.pending_provider_calls
                    .insert((attempt_id, cursor))
                    .then_some(())?;
                self.checkpoint.provider_calls = self.checkpoint.provider_calls.saturating_add(1);
            }
            AgentRecoveryEvent::ProviderCallFinished {
                attempt_id,
                start_cursor,
            } => {
                self.pending_provider_calls
                    .remove(&(attempt_id, start_cursor))
                    .then_some(())?;
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
            AgentRecoveryEvent::StageAcceptance(acceptance) => {
                if acceptance.business_result == "SubmitRejected"
                    && self.checkpoint.phase == AgentTurnPhase::Submit
                    && self.checkpoint.pending_tool_outputs.is_empty()
                {
                    let call_id = self.checkpoint.submit_call_id.clone()?;
                    let message = acceptance
                        .checks
                        .iter()
                        .find(|check| check.check_id == "agent.submit_validation")
                        .map(|check| check.actual.clone())
                        .unwrap_or_else(|| "previous submit_result was rejected".to_owned());
                    let feedback = if self.guard.initial_phase == AgentTurnPhase::Submit {
                        submission_rejection_feedback(call_id, message)
                    } else {
                        // Outcome keeps its existing two-phase recovery wire format.
                        ModelToolOutput { call_id, output: json!({
                            "ok":false, "error":"invalid_submission", "message":message,
                            "repair_policy":"reuse_previous_submission_and_change_only_rejected_fields",
                        }) }
                    };
                    self.checkpoint.pending_tool_outputs.push(feedback);
                }
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
        // turn 需要同时匹配 request/Contract/Context/capability/budget/tool hashes 和 phase；不完整 turn 只能进入失败计量。
        let payload_budget_policy_hash = budget_policy_hash(&payload.budget_policy).ok()?;
        let metadata_repair = self.guard.deliberation_repair_tool_set_hash.as_ref() == Some(&payload.tool_set_hash)
            && is_deliberation_repair(&self.checkpoint.pending_tool_outputs)
            && payload.request.continuation.is_none() && payload.request.tool_outputs.is_empty();
        if !self.expected_tools.is_empty()
            || payload.turn != self.checkpoint.next_model_turn
            || payload.contract_hash != self.guard.contract_hash
            || payload.request.contract_hash != self.guard.contract_hash
            || payload.request.read_grant_identity.as_ref() != Some(&self.guard.read_grant_identity)
            || payload.request.context_materialization_identity.as_ref()
                != Some(&self.guard.context_materialization_identity)
            || payload.context_manifest != payload.request.manifest_artifact_id
            || manifest != self.guard.context_manifest
            || payload.request_hash != model_request_hash(&payload.request).ok()?
            || payload.capability_snapshot_hash
                != capability_snapshot_hash(&payload.capability_snapshot).ok()?
            || payload.capability_snapshot_hash != self.guard.capability_snapshot_hash
            || payload
                .budget_policy_hash
                .as_ref()
                .is_some_and(|hash| hash != &payload_budget_policy_hash)
            || payload_budget_policy_hash != self.guard.budget_policy_hash
            || payload.tool_set_hash != tool_set_hash(&payload.request).ok()?
            || (!metadata_repair && &payload.tool_set_hash != self.guard.tool_set_hash(payload.request.phase))
            || payload.request.phase != self.checkpoint.phase
            || (!metadata_repair && payload.request.continuation != self.checkpoint.continuation)
            || (!metadata_repair && payload.request.tool_outputs != self.checkpoint.pending_tool_outputs)
        {
            return None;
        }

        self.checkpoint.trace_refs.push(reference);
        let Some(response) = payload.response else {
            (!completed).then_some(())?;
            if let Some(usage) = payload
                .error_detail
                .as_ref()
                .and_then(|detail| detail.get("usage"))
                .and_then(|usage| serde_json::from_value::<ModelUsage>(usage.clone()).ok())
            {
                return self.checkpoint.usage.record_failed_usage(
                    &payload.request,
                    &usage,
                    &payload.budget_policy,
                );
            }
            return self
                .checkpoint
                .usage
                .record_failed(&payload.request, &payload.budget_policy);
        };
        if !completed {
            return None;
        }

        self.checkpoint.pending_tool_outputs.clear();
        self.checkpoint.continuation = Some(response.continuation.clone());
        self.checkpoint
            .usage
            .record(&payload.request, &response, &payload.budget_policy)?;

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
                            call_ids
                                .insert(call.call_id.clone())
                                .then_some(ExpectedToolCall {
                                    request_hash: payload.request_hash.clone(),
                                    call,
                                    artifact: None,
                                    output: None,
                                })
                        })
                        .collect::<Option<_>>()?;
                }
            }
            AgentTurnPhase::Submit => {
                let submission = response.terminal_submission?;
                self.checkpoint.submit_call_id = Some(submission.call_id);
                self.checkpoint.submission_attempts =
                    self.checkpoint.submission_attempts.saturating_add(1);
                self.checkpoint.next_model_turn = payload.turn.saturating_add(1);
                self.checkpoint.phase = AgentTurnPhase::Submit;
            }
            AgentTurnPhase::Draft => return None,
        }
        Some(())
    }

    fn finish_tool_batch(&mut self) {
        // 只有所有 expected tool 都有结果，才把输出批次放进下一次 model request；部分工具结果不会提前推进 phase。
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
        // 未完成 provider/tool 或没有任何外部 provider call 时分别保留 unknown/fresh 边界；恢复不会凭空生成 continuation。
        self.finish_tool_batch();
        if !self.expected_tools.is_empty() {
            return None;
        }
        if !self.pending_provider_calls.is_empty() {
            self.checkpoint.usage.failure = Some(RecoveryUsageFailure::Unknown);
            self.checkpoint.usage.cost_complete = false;
        }
        // A failed first Draft has no accepted continuation, but its provider
        // cost still belongs to this task. Only a task with no provider call
        // and no accepted continuation may restart with an empty ledger.
        if self.checkpoint.continuation.is_none() && self.checkpoint.provider_calls == 0 {
            return None;
        }
        self.checkpoint.source = AgentRecoverySource::Recovered(lineage);
        Some(self.checkpoint)
    }
}

fn agent_recovery_checkpoint(
    store: &Store,
    permit: &TaskWritePermit,
    guard: &AgentRecoveryGuard,
) -> ResearchResult<AgentRecoveryCheckpoint> {
    // 先读 Attempt lineage，再解析事件；解析失败的历史仍需保留已发生的 provider expenditure 和不完整成本状态。
    let Some(lineage) = recovery_lineage(store, permit)? else {
        return Ok(guard.fresh_checkpoint());
    };
    // Establish whether external work began independently of parsing its audit
    // payloads. An incompatible/partial history must not erase that expenditure.
    let mut provider_calls = 0u32;
    for attempt_id in &lineage {
        for event in store.attempt_events(&permit.run_id, &permit.task_id, attempt_id)? {
            if event.lifecycle_kind()? == LifecycleEventType::AgentTurnStarted {
                provider_calls = provider_calls.saturating_add(1);
            }
        }
    }
    let fallback = || {
        let mut checkpoint = guard.fresh_checkpoint();
        if provider_calls > 0 {
            checkpoint.source = AgentRecoverySource::Recovered(lineage.clone());
            checkpoint.provider_calls = provider_calls;
            checkpoint.usage.failure = Some(RecoveryUsageFailure::Unknown);
            checkpoint.usage.cost_complete = false;
        }
        checkpoint
    };
    let Some(events) = load_recovery_events(store, permit, &lineage)? else {
        return Ok(fallback());
    };
    let Some(reducer) = events
        .into_iter()
        .try_fold(AgentRecoveryReducer::new(guard), AgentRecoveryReducer::fold)
    else {
        return Ok(fallback());
    };
    Ok(reducer.finish(lineage.clone()).unwrap_or_else(fallback))
}

fn recovery_lineage(
    store: &Store,
    permit: &TaskWritePermit,
) -> ResearchResult<Option<Vec<AttemptId>>> {
    // 只允许同一 Run/Task 的 Recovery 或 Retry 边；跨 task/run、重复父节点或其他 relation 都阻断恢复。
    let mut child = permit.attempt_id.clone();
    let mut seen = BTreeSet::from([child.clone()]);
    let mut lineage = vec![];
    while let Some(relation) = store.attempt_relation(&child)? {
        // A scheduler retry belongs to the same task's external expenditure,
        // just as lease recovery does. Never turn an interrupted provider call
        // into a fresh budget by crossing a Retry edge.
        if !matches!(
            relation.relation,
            akzio_domain::AttemptRelationKind::Recovery | akzio_domain::AttemptRelationKind::Retry
        ) || relation.run_id != permit.run_id
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
    store: &Store,
    permit: &TaskWritePermit,
    lineage: &[AttemptId],
) -> ResearchResult<Option<Vec<AgentRecoveryEvent>>> {
    // 每个 AgentTurnStarted 的 cursor 必须由同一 Attempt 的终态事件关闭；Artifact kind、origin、Contract 和 BLOB 也逐项复核。
    let mut loaded = vec![];
    for attempt_id in lineage {
        // Store trajectories serialize model calls within each attempt. The
        // immutable Started cursor identifies its slot; a terminal from another
        // attempt must never close it, even if aggregate counts happen to match.
        let mut pending_start = None;
        for event in store.attempt_events(&permit.run_id, &permit.task_id, attempt_id)? {
            let event_type = event.lifecycle_kind()?;
            if event_type == LifecycleEventType::AgentTurnStarted {
                if pending_start.replace(event.cursor).is_some() {
                    return Ok(None);
                }
                loaded.push(AgentRecoveryEvent::ProviderCallStarted {
                    attempt_id: attempt_id.clone(),
                    cursor: event.cursor,
                });
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
                LifecycleEventType::StageAcceptanceRecorded => ArtifactKind::DebugRecord,
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
                    && (expected_kind == ArtifactKind::DebugRecord
                        || origin.contract_hash.as_ref() == permit.contract_hash.as_ref())
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
                    let Ok(payload) = serde_json::from_slice::<StoredAgentTurnPayload>(&bytes)
                    else {
                        return Ok(None);
                    };
                    let manifest_artifact = store.artifact(&payload.context_manifest)?;
                    if manifest_artifact.kind != ArtifactKind::ContextManifest {
                        return Ok(None);
                    }
                    let Ok(manifest) = serde_json::from_slice::<akzio_domain::ContextManifestPayload>(
                        &store.read_blob(&manifest_artifact.blob)?,
                    ) else {
                        return Ok(None);
                    };
                    if let Some(start_cursor) = pending_start.take() {
                        loaded.push(AgentRecoveryEvent::ProviderCallFinished {
                            attempt_id: attempt_id.clone(),
                            start_cursor,
                        });
                    }
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
                LifecycleEventType::StageAcceptanceRecorded => {
                    let Ok(payload) = serde_json::from_slice(&bytes) else {
                        return Ok(None);
                    };
                    AgentRecoveryEvent::StageAcceptance(payload)
                }
                LifecycleEventType::ToolFailed => return Ok(None),
                _ => unreachable!("event type filtered above"),
            };
            loaded.push(recovery_event);
        }
    }
    Ok(Some(loaded))
}

#[cfg(test)]
mod recovery_tests {
    use super::*;

    fn guard() -> AgentRecoveryGuard {
        let hash = akzio_domain::ContentHash::of_bytes(b"recovery-test");
        AgentRecoveryGuard {
            initial_phase: AgentTurnPhase::Draft,
            deliberation_repair_tool_set_hash: None,
            contract_hash: hash.clone(),
            context_manifest: akzio_domain::ContextManifestPayload {
                schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
                contract_hash: hash.clone(),
                selections: vec![],
                quarantined: vec![],
                total_bytes: 0,
                projected_bytes: Some(0),
                estimated_tokens: 0,
                input_hash: hash.clone(),
            },
            read_grant_identity: hash.clone(),
            context_materialization_identity: hash.clone(),
            capability_snapshot_hash: hash.clone(),
            budget_policy_hash: hash.clone(),
            draft_tool_set_hash: hash.clone(),
            submit_tool_set_hash: hash,
        }
    }

    #[test]
    fn direct_submission_recovery_never_invents_a_draft_or_spent_budget() {
        let mut guard = guard();
        guard.initial_phase = AgentTurnPhase::Submit;
        let reducer = AgentRecoveryReducer::new(&guard);
        assert_eq!(reducer.checkpoint.phase, AgentTurnPhase::Submit);
        assert_eq!(reducer.checkpoint.provider_calls, 0);
        assert!(reducer.checkpoint.continuation.is_none());
        guard.initial_phase = AgentTurnPhase::Draft;
        assert_eq!(AgentRecoveryReducer::new(&guard).checkpoint.phase, AgentTurnPhase::Draft);
    }

    #[test]
    fn provider_terminal_must_close_its_exact_attempt_and_start_cursor() {
        let guard = guard();
        for (attempt_id, start_cursor) in [("other-attempt", 10), ("attempt", 11)] {
            let reducer = AgentRecoveryReducer::new(&guard)
                .fold(AgentRecoveryEvent::ProviderCallStarted {
                    attempt_id: AttemptId("attempt".into()),
                    cursor: 10,
                })
                .unwrap();
            assert!(
                reducer
                    .fold(AgentRecoveryEvent::ProviderCallFinished {
                        attempt_id: AttemptId(attempt_id.into()),
                        start_cursor,
                    })
                    .is_none(),
                "unrelated terminal cannot settle an outstanding call"
            );
        }
    }

    #[test]
    fn recovery_without_provider_work_remains_fresh() {
        let guard = guard();
        assert!(AgentRecoveryReducer::new(&guard)
            .finish(vec![AttemptId("no-model-call".into())])
            .is_none());
    }

    #[test]
    fn rejected_submit_recovery_reuses_call_and_sends_compact_feedback() {
        let mut guard = guard();
        guard.initial_phase = AgentTurnPhase::Submit;
        let mut reducer = AgentRecoveryReducer::new(&guard);
        reducer.checkpoint.phase = AgentTurnPhase::Submit;
        reducer.checkpoint.submit_call_id = Some("submit-1".to_owned());
        let acceptance = akzio_domain::StageAcceptance {
            version: 1,
            run_id: RunId("run-1".to_owned()),
            task_id: TaskId("task-1".to_owned()),
            attempt_id: AttemptId("attempt-1".to_owned()),
            stage: "research.critic".to_owned(),
            business_result: "SubmitRejected".to_owned(),
            test_result: akzio_domain::AcceptanceResult::Fail,
            checks: vec![akzio_domain::AcceptanceCheck {
                check_id: "agent.submit_validation".to_owned(),
                category: akzio_domain::AcceptanceCategory::Schema,
                expected: "valid Critique".to_owned(),
                actual: "research.grounds must be empty".to_owned(),
                result: akzio_domain::AcceptanceResult::Fail,
                evidence_refs: vec![],
                message: "reuse valid fields".to_owned(),
            }],
            created_at: Utc::now(),
        };

        let reducer = reducer
            .fold(AgentRecoveryEvent::StageAcceptance(acceptance.clone()))
            .expect("recovery reducer accepts persisted rejection");
        assert_eq!(reducer.checkpoint.pending_tool_outputs.len(), 1);
        assert_eq!(
            reducer.checkpoint.pending_tool_outputs[0].call_id,
            "submit-1"
        );
        assert_eq!(
            reducer.checkpoint.pending_tool_outputs[0].output,
            json!({"ok":false,"error":"invalid_submission",
                "message":"Agent output did not satisfy Contract schema: research.grounds must be empty"})
        );
        guard.initial_phase = AgentTurnPhase::Draft;
        let mut outcome = AgentRecoveryReducer::new(&guard);
        outcome.checkpoint.phase = AgentTurnPhase::Submit;
        outcome.checkpoint.submit_call_id = Some("submit-1".into());
        let outcome = outcome.fold(AgentRecoveryEvent::StageAcceptance(acceptance)).unwrap();
        assert_eq!(outcome.checkpoint.pending_tool_outputs[0].output,
            json!({"ok":false,"error":"invalid_submission","message":"research.grounds must be empty",
                "repair_policy":"reuse_previous_submission_and_change_only_rejected_fields"}));
    }

}
