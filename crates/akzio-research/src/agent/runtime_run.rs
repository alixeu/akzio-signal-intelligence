// Agent 主循环把固定 WorkflowNode、Store permit、ContextManifest、模型 Future 和
// 结构化 Submit 串成一条可恢复的 Attempt。研究角色从 Submit 开始；只有 Outcome
// 保留 Draft→受控读取→Submit。每次 provider/tool/validation/persist 都有独立的
// 错误边界，模型返回或 TaskSucceeded 仍不等于 Decision、Paper 成交或 Outcome 封存。
// The Attempt wall-time remains the Contract's fixed total. Draft needs the
// larger share because it may perform a real provider call plus bounded reads;
// Submit is deliberately kept within the remaining 30% and cannot extend the
// Attempt. The previous 55/45 split killed legitimate high-reasoning Drafts at
// 66s even when the Contract still had 54s left.
const DRAFT_WALL_TIME_FRACTION: f32 = 0.70;
fn outcome_phase_output_cap(budget: &AgentRunBudget, phase: AgentTurnPhase) -> ResearchResult<u32> {
    // Outcome 预留至少一半 output 给 Submit，并在 70% 墙钟处结束 Draft；这是同一
    // Attempt 的总预算切分，不会因阶段切换重置 usage 或延长 deadline。
    let reserve = if phase == AgentTurnPhase::Draft { (budget.max_output_tokens / 2).max(1) } else { 0 };
    if phase == AgentTurnPhase::Draft && (budget.remaining_output_tokens()? <= reserve
        || budget.started.elapsed() >= budget.wall_time.mul_f32(DRAFT_WALL_TIME_FRACTION)) {
        return Err(ResearchError::InvalidOutput("draft_incomplete: reserved Submit budget reached before a memo was completed".into()));
    }
    Ok(budget.remaining_output_tokens()?.saturating_sub(reserve).max(1))
}

fn phase_output_cap(
    budget: &AgentRunBudget,
    phase: AgentTurnPhase,
    purpose: &str,
    _submission_attempts: u8,
    _contract_version: u32,
    provider_cap: Option<u32>,
) -> ResearchResult<u32> {
    // 研究 Submit 使用剩余全局 output cap；Outcome 走上面的两阶段 cap。Provider
    // capability 只能进一步收紧请求，不会扩大 Contract 预算。
    if purpose == LEARNING_OUTCOME_WORKER_RECIPE_ID {
        return outcome_phase_output_cap(budget, phase);
    }
    // max_output_tokens is the frozen Task lifetime allowance. The request
    // receives only the unspent part, bounded independently by the application
    // ceiling and any explicitly declared Provider output capability.
    let remaining = budget.remaining_output_tokens()?;
    let cap = remaining
        .min(akzio_domain::budget::MAX_AGENT_OUTPUT_TOKENS)
        .min(provider_cap.unwrap_or(u32::MAX));
    if cap == 0 {
        return Err(ResearchError::OutputBudgetExceeded { actual: 1, maximum: 0 });
    }
    Ok(cap)
}

/// Minimum share of a task's wall-time allowance that must remain before a
/// bounded repair round is worth requesting. A repair is a second full provider
/// turn; entering one with almost no time left cannot succeed and replaces the
/// real validation rejection with a misleading `wall_time` failure. Observed
/// structured submissions take roughly half the 120s allowance, so a repair
/// needs at least that much again.
const REPAIR_ROUND_WALL_TIME_FRACTION: f32 = 0.5;

/// True when enough of the phase deadline remains to attempt a repair round.
/// The rejection itself is still recorded either way; this only decides whether
/// asking the model to fix it can plausibly finish inside the same allowance.
fn repair_round_fits(phase_deadline: StdDuration, elapsed: StdDuration) -> bool {
    // 结构化拒绝已经持久化；这里只判断同一 deadline 是否还容得下第二次 provider
    // Future，避免把原始 validation rejection 覆盖成无意义的 wall_time 错误。
    phase_deadline.saturating_sub(elapsed)
        >= phase_deadline.mul_f32(REPAIR_ROUND_WALL_TIME_FRACTION)
}

fn model_phase_deadline(
    wall_time: StdDuration,
    phase: AgentTurnPhase,
    contract_version: u32,
) -> StdDuration {
    let phase_deadline = if phase == AgentTurnPhase::Draft {
        wall_time.mul_f32(DRAFT_WALL_TIME_FRACTION)
    } else {
        wall_time
    };
    if contract_version < akzio_domain::budget::OUTPUT_BUDGET_RESEARCH_CONTRACT_VERSION {
        return phase_deadline;
    }
    // Accounting and a fenced failure commit must have time inside the same
    // task allowance. This does not guarantee a blocked Store can finish;
    // that case still leaves a durable Started with unknown usage.
    let audit_reserve = StdDuration::from_secs(1).min(wall_time / 10);
    phase_deadline.min(wall_time.saturating_sub(audit_reserve))
}

impl AgentRuntime {
    pub async fn run(
        &self,
        permit: &TaskWritePermit,
        node: &WorkflowNode,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        model: &dyn AgentModel,
        now: DateTime<Utc>,
    ) -> ResearchResult<Artifact> {
        let mut budget = AgentRunBudget::new(&node.budget, &node.retry);
        self.run_inner(permit, node, candidates, model, now, &mut budget)
            .await
    }

    pub async fn run_with_budget(
        &self,
        permit: &TaskWritePermit,
        node: &WorkflowNode,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        model: &dyn AgentModel,
        now: DateTime<Utc>,
        budget: &mut AgentRunBudget,
    ) -> ResearchResult<Artifact> {
        self.run_inner(permit, node, candidates, model, now, budget)
            .await
    }

    async fn run_inner(
        &self,
        permit: &TaskWritePermit,
        node: &WorkflowNode,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        model: &dyn AgentModel,
        now: DateTime<Utc>,
        budget: &mut AgentRunBudget,
    ) -> ResearchResult<Artifact> {
        // 入口先校验 permit、task、Contract 和冻结 Node，再创建任何 Model/Context
        // 副作用。NodePolicyMismatch 表示持久化图与调用方不一致，不是允许模型继续的提示。
        self.validate_authority_permit(permit).await?;
        if permit.task_id != node.task_id {
            return Err(ResearchError::TaskMismatch);
        }
        let contract_hash = node
            .contract_hash
            .as_ref()
            .ok_or(ResearchError::MissingContractHash)?;
        if permit.contract_hash.as_ref() != Some(contract_hash) {
            return Err(ResearchError::ContractMismatch);
        }
        let installed = self.catalogue.get(contract_hash)?;
        let run_id = permit.run_id.clone();
        let task_id = permit.task_id.clone();
        let frozen = self
            .store_executor
            .execute(move |store| {
                store.workflow_snapshot(&run_id).map(|snapshot| {
                    snapshot
                        .tasks
                        .into_iter()
                        .find(|task| task.node.task_id == task_id)
                        .map(|task| (task.node, task.active_attempt))
                })
            })
            .await??;
        // Candidate evidence is authorized by Context below and may be enriched
        // after claim. The immutable execution scope and policy must still match.
        let frozen_execution_matches = frozen.as_ref().is_some_and(|(stored, _)| {
            let mut execution = node.clone();
            execution.input_artifacts = stored.input_artifacts.clone();
            execution.dependencies.sort();
            execution == *stored
        });
        if !frozen_execution_matches
            || budget.resolved_policy() != node.budget
            || node.retry != installed.contract.retry
            || node.on_failure != installed.contract.on_failure
        {
            return Err(ResearchError::NodePolicyMismatch);
        }
        // Use the durable Attempt start, not a later handler-local start. The
        // controller's deadline otherwise wins before the Agent can audit a
        // timed-out call. Never extend either clock or reset cumulative usage.
        let event_time_origin = if installed.contract.version
            >= akzio_domain::budget::OUTPUT_BUDGET_RESEARCH_CONTRACT_VERSION
        {
            let active = frozen.and_then(|(_, active)| active)
                .filter(|active| active.permit == *permit)
                .ok_or(ResearchError::GrantPermitMismatch)?;
            let elapsed = Utc::now().signed_duration_since(active.started_at)
                .to_std().map_err(|_| ResearchError::InvalidOutput(
                    "Attempt start is in the future; cannot establish its deadline".into()
                ))?;
            let started = Instant::now().checked_sub(elapsed).ok_or_else(|| {
                ResearchError::InvalidOutput("Attempt elapsed time cannot be represented".into())
            })?;
            budget.started = budget.started.min(started);
            budget.check_wall()?;
            active.started_at
        } else {
            now
        };
        let query_scope = akzio_domain::ContextQueryScope::for_node(node);
        let candidates = candidates.into_iter().collect::<Vec<_>>();
        let manifest = if let Some(parent_task_id) = &node.parent_task_id {
            // 有 parent 时只从父成功 Attempt 证明组装 child Manifest；没有 parent 则
            // 从当前候选 EvidenceNeed 建立新的 Grant。两条路径都由 StoreExecutor 串行。
            if !node.dependencies.contains(parent_task_id) {
                return Err(ResearchError::InvalidOutput(
                    "parent task is not a declared dependency".to_owned(),
                ));
            }
            let proof = self
                .load_parent_succeeded_attempt(&permit.run_id, parent_task_id)
                .await?;
            let parent_contract_hash = proof.contract_hash.as_ref().ok_or_else(|| {
                ResearchError::InvalidOutput("parent attempt has no contract hash".to_owned())
            })?;
            let context = self.context.clone();
            let parent_contract = self.catalogue.get(parent_contract_hash)?.contract.clone();
            let contract = installed.contract.clone();
            let permit = permit.clone();
            let grant_ttl = self.grant_ttl;
            self.store_executor
                .execute(move |_| {
                    context.assemble_child_from_proof(
                        &proof,
                        &parent_contract,
                        &permit,
                        &contract,
                        now,
                        grant_ttl,
                    )
                })
                .await??
        } else {
            let context = self.context.clone();
            let contract = installed.contract.clone();
            let permit = permit.clone();
            let grant_ttl = self.grant_ttl;
            self.store_executor
                .execute(move |_| context.assemble(&permit, &contract, &query_scope, candidates, now, grant_ttl))
                .await??
        };
        if !manifest.grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let context_materialization = self
            .context_materialization(permit, &installed.contract, &manifest, &node.budget, now)
            .await?;
        let context = context_materialization.model_context();
        let governance = String::from_utf8(
            self.read_authority_document(
                &installed.contract,
                &installed.contract.prompt.governance,
            )
            .await?,
        )
        .map_err(|_| ResearchError::InvalidOutput("governance prompt is not UTF-8".to_owned()))?;
        let role = String::from_utf8(
            self.read_authority_document(&installed.contract, &installed.contract.prompt.role)
                .await?,
        )
        .map_err(|_| ResearchError::InvalidOutput("role prompt is not UTF-8".to_owned()))?;
        let response_language = model.response_language().unwrap_or("简体中文").trim();
        let reference_ledger = manifest
            .payload
            .selections
            .iter()
            .map(|selection| {
                json!({
                    "artifact_id": selection.artifact.artifact_id,
                    "kind": selection.artifact.kind,
                })
            })
            .collect::<Vec<_>>();
        let reference_ledger = serde_json::to_string(&reference_ledger)?;
        let direct_structured = match installed.contract.purpose.as_str() {
            RESEARCH_ANALYST_RECIPE_ID | RESEARCH_CRITIC_RECIPE_ID | RESEARCH_SYNTHESIZER_RECIPE_ID | akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID
                if installed.contract.version >= 65 => true,
            LEARNING_OUTCOME_WORKER_RECIPE_ID => false,
            _ => return Err(ResearchError::InvalidOutput("legacy_workflow_retired".into())),
        };
        // 这个分支是活动协议的硬选择：研究/Review Submit-only，Outcome Draft/Submit；
        // 旧 Planner 或旧研究 Contract 不会通过 fallback 获得运行能力。
        // Choose the research or Outcome protocol explicitly.
        let prompt = if direct_structured {
            prompts::structured_request_prompt(
                &governance,
                &role,
                response_language,
                &reference_ledger,
                installed.contract.output.artifact_kind,
                &node.budget,
                installed.contract.version,
            )?
        } else {
            prompts::outcome_request_prompt(
                &governance,
                &role,
                response_language,
                &reference_ledger,
                &node.budget,
            )?
        };
        let mut output_schema: Value = serde_json::from_slice(
            &self
                .read_authority_document(&installed.contract, &installed.contract.output.schema)
                .await?,
        )?;
        bind_reference_schema(
            &mut output_schema,
            &manifest
                .payload
                .selections
                .iter()
                .map(|s| json!(s.artifact))
                .collect::<Vec<_>>(),
        );
        if direct_structured {
            if installed.contract.version >= 61 {
                let contract_version = installed.contract.version;
                let manifest_for_schema = manifest.clone();
                output_schema = self.store_executor.execute(move |store| {
                    bind_ground_scope_schema(&store, &manifest_for_schema, &mut output_schema, contract_version)?;
                    Ok::<_, ResearchError>(output_schema)
                }).await??;
            }
            output_schema["properties"]["deliberation"]["properties"]["basis_artifact_ids"]["minItems"] = json!(1);
            if installed.contract.output.artifact_kind == ArtifactKind::DecisionProposal {
                remove_model_timing_fields(&mut output_schema);
                bind_synthesis_submission_schema(&mut output_schema);
            }
        }
        if installed.contract.output.artifact_kind == ArtifactKind::ProposalReview { remove_review_identity(&mut output_schema); }
        let tools = if direct_structured {
            Vec::new()
        } else {
            model_tool_definitions(&self.context, &installed.contract)?
        };
        let terminal = AgentTerminalDefinition {
            description: submit_tool_description(&installed.contract.purpose),
            input_schema: output_schema.clone(),
        };
        let deliberation_terminal = AgentTerminalDefinition {
            description: "Repair only the rejected deliberation. The original result is immutable and owned by Rust.".into(),
            input_schema: json!({"type":"object","properties":{"deliberation":terminal.input_schema["properties"]["deliberation"]},"required":["deliberation"],"additionalProperties":false}),
        };
        let prefetched_capabilities = model.capability_snapshot();
        let budget_policy = model.budget_policy();
        budget.attach_budget_policy(&budget_policy)?;
        let budget_policy_hash = budget_policy_hash(&budget_policy)?;
        let recovery_guard = AgentRecoveryGuard {
            initial_phase: if direct_structured { AgentTurnPhase::Submit } else { AgentTurnPhase::Draft },
            deliberation_repair_tool_set_hash: if direct_structured { Some(advertised_tool_set_hash(&[], Some(&deliberation_terminal))?) } else { None },
            contract_hash: installed.contract.contract_hash.clone(),
            context_manifest: manifest.payload.clone(),
            read_grant_identity: context_materialization.read_grant_identity.clone(),
            context_materialization_identity: context_materialization
                .materialization_identity
                .clone(),
            capability_snapshot_hash: capability_snapshot_hash(&prefetched_capabilities)?,
            budget_policy_hash: budget_policy_hash.clone(),
            draft_tool_set_hash: advertised_tool_set_hash(&tools, None)?,
            submit_tool_set_hash: advertised_tool_set_hash(&[], Some(&terminal))?,
        };
        let recovery_permit = permit.clone();
        let recovery = self
            .store_executor
            .execute(move |store| {
                agent_recovery_checkpoint(&store, &recovery_permit, &recovery_guard)
            })
            .await??;
        let provider_output_cap = prefetched_capabilities.declared_max_output_tokens;
        let mut prefetched_capabilities = Some(prefetched_capabilities);
        if matches!(&recovery.source, AgentRecoverySource::Recovered(_)) {
            // 恢复先充值已验证 checkpoint；未知用量会在 restore 中直接 fail closed，
            // 不会把中断的 Provider Future 当成尚未开始。
            budget.restore(&recovery)?;
        }
        self.observe_debug_budget(permit, budget, "AttemptBudgetReady")
            .await?;
        let mut continuation = recovery.continuation;
        let mut pending_tool_outputs = recovery.pending_tool_outputs;
        let mut trace_refs = recovery.trace_refs;
        let mut model_turn = recovery.next_model_turn;
        let mut phase = recovery.phase;
        // Research starts at Submit; Outcome recovery still requires a persisted Draft memo.
        let mut draft_completed = phase == AgentTurnPhase::Submit;
        let mut submission_attempts = recovery.submission_attempts;
        let started = budget.started;
        let wall_time = budget.wall_time;
        loop {
            // 每轮都在 dispatch 前检查共享 wall clock、phase deadline、output reservation
            // 和 cumulative budget；任何拒绝发生在 AgentTurnStarted 之前都不会留下假调用。
            budget.check_wall()?;
            if installed.contract.version >= akzio_domain::budget::OUTPUT_BUDGET_RESEARCH_CONTRACT_VERSION
                && started.elapsed() >= model_phase_deadline(wall_time, phase, installed.contract.version) {
                return Err(ResearchError::WallTimeExceeded {
                    maximum_secs: node.budget.max_wall_time_secs,
                });
            }
            // Reserve the effective request cap against the shared task ledger.
            // Only Outcome uses the phase-specific allocation below.
            // Release reservations before charging actual Provider usage, once.
            if phase == AgentTurnPhase::Submit && !draft_completed {
                return Err(ResearchError::MissingFinalOutput);
            }
            let max_output_tokens = phase_output_cap(
                budget,
                phase,
                installed.contract.purpose.as_str(),
                submission_attempts,
                installed.contract.version,
                provider_output_cap,
            )?;

            let mut request = AgentModelRequest {
                contract_hash: installed.contract.contract_hash.clone(),
                purpose: installed.contract.purpose.as_str().to_owned(),
                phase,
                prompt: if phase == AgentTurnPhase::Draft && !budget.max_tool_calls.allows(u64::from(budget.tool_calls) + 1) {
                    prompts::reads_exhausted_prompt(&prompt)
                } else { prompt.clone() },
                objective: node.model_objective(),
                manifest_artifact_id: manifest.artifact.artifact_id.clone(),
                read_grant_identity: Some(context_materialization.read_grant_identity.clone()),
                context_materialization_identity: Some(
                    context_materialization.materialization_identity.clone(),
                ),
                context: if continuation.is_none() {
                    context.clone()
                } else {
                    Vec::new()
                },
                continuation: continuation.clone(),
                tool_outputs: pending_tool_outputs.clone(),
                continuation_instruction: if direct_structured && pending_tool_outputs.is_empty() { None } else { prompts::continuation_instruction(
                    phase, installed.contract.version, !pending_tool_outputs.is_empty(),
                ) },
                max_output_tokens,
                // Outcome retains its Submit formatting override; research uses its configured route.
                reasoning_effort: if !direct_structured && phase == AgentTurnPhase::Submit {
                    Some("low".to_owned())
                } else { None },
                tools: if phase == AgentTurnPhase::Draft && budget.max_tool_calls.allows(u64::from(budget.tool_calls) + 1) {
                    tools.clone()
                } else {
                    Vec::new()
            },
            terminal: (phase == AgentTurnPhase::Submit).then(|| terminal.clone()),
        };
            if !direct_structured && installed.contract.version >= catalogue::COMPACT_SUBMISSION_CONTRACT_VERSION
                && phase == AgentTurnPhase::Submit {
                let instruction = request.continuation_instruction.get_or_insert_with(String::new);
                instruction.push_str(prompts::COMPACT_JSON_GUIDANCE);
            }
            if let Some(projection) = self.historical_projection {
                projection.project_request(&mut request);
            }
            if request.purpose == RESEARCH_ANALYST_RECIPE_ID {
                request.prompt.push_str(prompts::ANALYST_GROUNDS_GUIDANCE);
            }
            if request.purpose == akzio_domain::RESEARCH_CRITIC_RECIPE_ID {
                request.prompt.push_str(prompts::CRITIC_VERDICT_GUIDANCE);
            }
            let frozen_result = if direct_structured && is_deliberation_repair(&pending_tool_outputs) {
                // deliberation 修复只能复用上一份 immutable result；模型获得反馈和旧
                // deliberation，不得借修复机会重写 Claim/Forecast/Review 正式结果。
                let original = self.last_structured_submission(&trace_refs).await?;
                request.context = vec![json!({"type":"deliberation_repair","previous_deliberation":original["deliberation"],"validation_feedback":pending_tool_outputs,"frozen_result_hash":akzio_domain::ContentHash::of_bytes(&serde_json::to_vec(&original["result"])?)})];
                request.continuation = None;
                request.tool_outputs.clear();
                request.continuation_instruction = None;
                request.prompt = format!("{governance}\n本次仅修复此前被 Rust 拒绝的 deliberation。正式 result 已冻结，不得提交、重写或推断 result。保留原有依据和叙事，仅修复指出的错误；uncertainty_weight_ppm 的总和必须严格等于 1000000-confidence_ppm。恰好调用一次 submit_result，只包含 deliberation。引用 ID 必须来自此不可变 ledger：{reference_ledger}");
                request.terminal = Some(deliberation_terminal.clone());
                Some(original["result"].clone())
            } else { None };
            let provisional_input_tokens = estimate_tokens(&request)?;
            // A memo and a Submit must both fit. Optional schema repair is bounded
            // by remaining budget; insufficient input never fabricates a memo.
            if phase == AgentTurnPhase::Draft
                && budget
                    .input_tokens
                    .saturating_add(provisional_input_tokens.saturating_mul(2))
                    > budget.max_input_tokens
            {
                return Err(ResearchError::InputBudgetExceeded {
                    actual: budget
                        .input_tokens
                        .saturating_add(provisional_input_tokens.saturating_mul(2)),
                    maximum: budget.max_input_tokens,
                });
            }
            request.max_output_tokens = request
                .max_output_tokens
                .min(budget.output_tokens_for_call(provisional_input_tokens)?);
            let input_tokens = estimate_tokens(&request)?;
            request.max_output_tokens = request
                .max_output_tokens
                .min(budget.output_tokens_for_call(input_tokens)?);
            let tool_set_hash = tool_set_hash(&request)?;
            let mut turn_attempt = 1_u8;
            let (mut turn, runtime_snapshot, request_hash) = loop {
                let capability_snapshot = prefetched_capabilities
                    .take()
                    .unwrap_or_else(|| model.capability_snapshot());
                if capability_snapshot.declared_max_output_tokens
                    .is_some_and(|cap| request.max_output_tokens > cap)
                {
                    return Err(ResearchError::CapabilityMismatch {
                        capability: "max_output_tokens",
                        provider_id: capability_snapshot.provider_id.clone(),
                        model_id: capability_snapshot.model_id.clone(),
                    });
                }
                let capability_snapshot_hash = capability_snapshot_hash(&capability_snapshot)?;
                let runtime_snapshot = AgentTurnRuntimeSnapshot {
                    resolved_budget: node.budget.clone(),
                    budget_usage: budget.debug_observation("RequestProvenance"),
                    capability: capability_snapshot,
                    capability_hash: capability_snapshot_hash,
                    budget_policy: budget_policy.clone(),
                    budget_policy_hash: budget_policy_hash.clone(),
                    tool_set_hash: tool_set_hash.clone(),
                };
                if let Err(capability) =
                    validate_model_capabilities(&runtime_snapshot.capability, &request)
                {
                    let turn_now = logical_now(event_time_origin, started.elapsed());
                    let failed_turn = self
                        .record_failed_turn(
                            TurnRecord {
                                permit: permit.clone(),
                                contract: installed.contract.clone(),
                                manifest: manifest.clone(),
                                turn: model_turn,
                                attempt: turn_attempt,
                                now: turn_now,
                            },
                            &request,
                            "capability_mismatch",
                            None,
                            None,
                            false,
                            &runtime_snapshot,
                        )
                        .await?;
                    trace_refs.push(ArtifactRef {
                        artifact_id: failed_turn.artifact_id,
                        kind: ArtifactKind::AgentTurn,
                    });
                    return Err(capability);
                }
                // Reject locally before opening a durable Provider turn.
                // A budget rejection must not leave AgentTurnStarted unmatched.
                if installed.contract.version >= akzio_domain::budget::OUTPUT_BUDGET_RESEARCH_CONTRACT_VERSION
                && started.elapsed() >= model_phase_deadline(wall_time, phase, installed.contract.version) {
                    return Err(ResearchError::WallTimeExceeded {
                        maximum_secs: node.budget.max_wall_time_secs,
                    });
                }
                budget.authorize_model_call(input_tokens)?;
                let request_hash = model_request_hash(&request)?;
                self.validate_authority_permit(permit).await?;
                let reserved_output_tokens = request.max_output_tokens;
                budget.reserve_output_tokens(reserved_output_tokens)?;
                let event_permit = permit.clone();
                let event_now = logical_now(event_time_origin, started.elapsed());
                // Started 事件先入 Store，再 poll provider Future。这样取消/崩溃即使没有
                // response 也能由 recovery 识别为已消耗且 usage unknown 的调用。
                self.store_executor
                    .execute(move |store| {
                        store.append_task_event(
                            &event_permit,
                            LifecycleEventType::AgentTurnStarted,
                            event_now,
                        )
                    })
                    .await??;
                let sender = self.reasoning_events.clone();
                let run_id = permit.run_id.clone();
                let task_id = permit.task_id.clone();
                let attempt_id = permit.attempt_id.clone();
                let purpose = request.purpose.clone();
                let on_event: ModelEventSink = Arc::new(move |event| {
                    let Some(sender) = &sender else {
                        return;
                    };
                    let event = match event {
                        ModelStreamEvent::ReasoningStart => AgentReasoningEvent::ReasoningStart {
                            run_id: run_id.clone(),
                            task_id: task_id.clone(),
                            attempt_id: attempt_id.clone(),
                            purpose: purpose.clone(),
                            turn: model_turn,
                        },
                        ModelStreamEvent::ReasoningDelta(delta) => {
                            AgentReasoningEvent::ReasoningDelta {
                                run_id: run_id.clone(),
                                task_id: task_id.clone(),
                                attempt_id: attempt_id.clone(),
                                purpose: purpose.clone(),
                                turn: model_turn,
                                delta,
                            }
                        }
                        ModelStreamEvent::ReasoningEnd => AgentReasoningEvent::ReasoningEnd {
                            run_id: run_id.clone(),
                            task_id: task_id.clone(),
                            attempt_id: attempt_id.clone(),
                            purpose: purpose.clone(),
                            turn: model_turn,
                        },
                    };
                    let _ = sender.send(event);
                });
                self.observe_debug_budget(permit, budget, "BeforeLLM")
                    .await?;
                let phase_deadline = model_phase_deadline(
                    wall_time, phase, installed.contract.version,
                );
                let remaining = phase_deadline.saturating_sub(started.elapsed());
                let provider_started = Instant::now();
                let call = if remaining.is_zero() {
                    Err(ResearchError::WallTimeExceeded {
                        maximum_secs: node.budget.max_wall_time_secs,
                    })
                } else {
                    // timeout 只停止当前等待者；Store 的 Started/失败 trace 和预算记录
                    // 仍在同一个 Attempt 语义内，不能把取消当作 Provider 未发生。
                    tokio::time::timeout(
                        remaining,
                        model.turn_with_events(request.clone(), on_event),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        Err(ResearchError::WallTimeExceeded {
                            maximum_secs: node.budget.max_wall_time_secs,
                        })
                    })
                };
                self.record_pipeline_latency(permit, if direct_structured { "structured" } else if phase == AgentTurnPhase::Draft { "draft" } else { "submit" }, "provider", provider_started.elapsed(), call.is_ok(), model_turn).await?;
                budget.release_output_tokens(reserved_output_tokens);
                match call {
                    Ok(turn) => {
                        break (turn, runtime_snapshot, request_hash);
                    }
                    Err(error) => {
                        // Provider 错误先写失败 Turn、再计 usage、再按 RetryPolicy 和
                        // 剩余时间决定重试；输入/未知成本错误优先于 transport retry。
                        let retryable = retryable_model_error(&error, &installed.contract.retry);
                        let draft_deadline = phase == AgentTurnPhase::Draft
                            && matches!(error, ResearchError::WallTimeExceeded { .. });
                        let will_retry = !draft_deadline
                            && retryable
                            && turn_attempt < installed.contract.retry.max_attempts;
                        let turn_now = logical_now(event_time_origin, started.elapsed());
                        let failed_turn = self
                            .record_failed_turn(
                                TurnRecord {
                                    permit: permit.clone(),
                                    contract: installed.contract.clone(),
                                    manifest: manifest.clone(),
                                    turn: model_turn,
                                    attempt: turn_attempt,
                                    now: turn_now,
                                },
                                &request,
                                model_error_class(&error),
                                Some(research_error_detail(&error)),
                                model_debug_trace(&error),
                                will_retry,
                                &runtime_snapshot,
                            )
                            .await?;
                        trace_refs.push(ArtifactRef {
                            artifact_id: failed_turn.artifact_id,
                            kind: ArtifactKind::AgentTurn,
                        });
                        let provider_incomplete = matches!(
                            &error,
                            ResearchError::ProviderIncomplete { .. }
                                | ResearchError::ProviderUsageMissing { .. }
                        );
                        let accounting = match &error {
                            ResearchError::ProviderIncomplete { usage, .. }
                            | ResearchError::ProviderUsageMissing { usage, .. } => {
                                budget.record_failed_provider_usage(input_tokens, usage)
                            }
                            _ => budget.record_failed_turn(input_tokens),
                        };
                        if let Err(accounting_error) = accounting {
                            if !provider_incomplete {
                                return Err(accounting_error);
                            }
                        }
                        self.observe_debug_budget(permit, budget, "FailedTurnAccounted")
                            .await?;
                        if draft_deadline {
                            return Err(error);
                        }
                        if !will_retry {
                            return Err(error);
                        }
                        let backoff = StdDuration::from_millis(
                            installed
                                .contract
                                .retry
                                .initial_backoff_ms
                                .saturating_mul(u64::from(turn_attempt)),
                        );
                        if backoff > wall_time.saturating_sub(started.elapsed()) {
                            return Err(ResearchError::WallTimeExceeded {
                                maximum_secs: node.budget.max_wall_time_secs,
                            });
                        }
                        if !backoff.is_zero() {
                            tokio::time::sleep(backoff).await;
                        }
                        turn_attempt = turn_attempt.saturating_add(1);
                    }
                }
            };
            if let Some(projection) = self.historical_projection {
                projection.restore_turn(&mut turn);
            }
            if started.elapsed() > wall_time {
                // Future 返回得太晚时仍可能带有完整 telemetry；先把已知 usage 记入失败
                // trace，禁止把 late memo 当成已完成 Draft，更不能进入 Submit。
                // The provider completed, but the phase did not. Account known
                // usage and keep this as a failed turn so recovery cannot treat
                // the late memo as accepted Draft output.
                let resolved = resolve_model_usage(
                    input_tokens,
                    estimate_turn_output_tokens(&turn)?,
                    turn.telemetry.as_ref(),
                );
                let accounting = budget.record_resolved_usage(resolved);
                let usage = ModelUsage {
                    input_tokens: Some(resolved.input_tokens),
                    cached_input_tokens: resolved.cached_input_tokens,
                    output_tokens: Some(resolved.output_tokens),
                    reasoning_tokens: resolved.reasoning_tokens,
                };
                let turn_now = logical_now(event_time_origin, started.elapsed());
                let failed_turn = self
                    .record_failed_turn(
                        TurnRecord {
                            permit: permit.clone(),
                            contract: installed.contract.clone(),
                            manifest: manifest.clone(),
                            turn: model_turn,
                            attempt: turn_attempt,
                            now: turn_now,
                        },
                        &request,
                        "wall_time",
                        Some(json!({"kind": "wall_time", "usage": usage, "response": turn})),
                        turn.model_debug.as_ref(),
                        false,
                        &runtime_snapshot,
                    )
                    .await?;
                trace_refs.push(ArtifactRef {
                    artifact_id: failed_turn.artifact_id,
                    kind: ArtifactKind::AgentTurn,
                });
                self.observe_debug_budget(permit, budget, "LateTurnAccounted")
                    .await?;
                accounting?;
                return Err(ResearchError::WallTimeExceeded {
                    maximum_secs: node.budget.max_wall_time_secs,
                });
            }
            let turn_now = logical_now(event_time_origin, started.elapsed());
            let turn_artifact = self
                .record_turn(
                    TurnRecord {
                        permit: permit.clone(),
                        contract: installed.contract.clone(),
                        manifest: manifest.clone(),
                        turn: model_turn,
                        attempt: turn_attempt,
                        now: turn_now,
                    },
                    &request,
                    &turn,
                    &runtime_snapshot,
                )
                .await?;
            trace_refs.push(ArtifactRef {
                artifact_id: turn_artifact.artifact_id,
                kind: ArtifactKind::AgentTurn,
            });
            let provider_output_limit = turn
                .telemetry
                .as_ref()
                .and_then(|telemetry| telemetry.output_tokens)
                .and_then(|actual| u32::try_from(actual).ok())
                .filter(|actual| *actual > request.max_output_tokens)
                .map(|actual| ResearchError::ProviderOutputLimitExceeded {
                    actual,
                    maximum: request.max_output_tokens,
                });
            let account_result = budget.record_turn(
                input_tokens,
                estimate_turn_output_tokens(&turn)?,
                turn.telemetry.as_ref(),
            );
            continuation = Some(turn.continuation.clone());
            self.observe_debug_budget(permit, budget, "TurnAccounted")
                .await?;
            if let Err(error) = account_result {
                return Err(provider_output_limit.unwrap_or(error));
            }
            if let Some(error) = provider_output_limit {
                // Actual usage is already charged. Do not reformat or resubmit the result.
                return Err(error);
            }
            let validation_feedback = pending_tool_outputs.clone();
            pending_tool_outputs.clear();
            if phase == AgentTurnPhase::Draft && turn.terminal_submission.is_some() {
                return Err(ResearchError::AmbiguousSubmission);
            }
            if phase == AgentTurnPhase::Draft && !turn.tool_calls.is_empty() {
                // Outcome Draft 的每次读取都走持久化 ToolCall→ToolResult；ToolResult
                // 作为 continuation 输入继续占用同一预算，研究 Submit-only 不会进入此分支。
                budget.record_tool_calls(
                    u32::try_from(turn.tool_calls.len())
                        .map_err(|_| ResearchError::ToolBudgetExceeded)?,
                )?;
                self.observe_debug_budget(permit, budget, "ToolCallsAccounted")
                    .await?;
                for call in turn.tool_calls {
                    budget.check_wall()?;
                    let call_id = call.call_id.clone();
                    let tool_runtime = self.clone();
                    let tool_permit = permit.clone();
                    let tool_contract = installed.contract.clone();
                    let tool_grant = manifest.grant.clone();
                    let tool_request_hash = request_hash.clone();
                    let tool_result = self
                        .store_executor
                        .execute(move |_| {
                            tool_runtime.execute_tool(
                                &tool_permit,
                                &tool_contract,
                                &tool_grant,
                                &call,
                                &tool_request_hash,
                                turn_now,
                            )
                        })
                        .await??;
                    budget.check_wall()?;
                    trace_refs.push(ArtifactRef {
                        artifact_id: tool_result.artifact.artifact_id.clone(),
                        kind: ArtifactKind::ToolResult,
                    });
                    pending_tool_outputs.push(ModelToolOutput {
                        call_id,
                        output: tool_result.value,
                    });
                }
                model_turn = model_turn.saturating_add(1);
                continue;
            }
            if phase == AgentTurnPhase::Draft {
                if turn
                    .assistant_text
                    .as_deref()
                    .is_none_or(|text| text.trim().is_empty())
                {
                    return Err(ResearchError::MissingFinalOutput);
                }
                phase = AgentTurnPhase::Submit;
                draft_completed = true;
                model_turn = model_turn.saturating_add(1);
                continue;
            }

            if !turn.tool_calls.is_empty() || turn.assistant_text.is_some() {
                return Err(ResearchError::AmbiguousSubmission);
            }
            let submission = turn
                .terminal_submission
                .ok_or(ResearchError::MissingFinalOutput)?;

            if direct_structured && submission_attempts > 0 {
                // 第二次结构化提交先登记 immutable before/after revision；只有字段级
                // rejection 才允许有界修复，已通过字段和精确 ArtifactRef 不能被顺手润色。
                self.record_structured_revision(permit, &trace_refs, model_turn, &validation_feedback).await?;
            }

            let validation_runtime = self.clone();
            let validation_permit = permit.clone();
            let validation_contract = installed.contract.clone();
            let validation_manifest = manifest.clone();
            let mut validation_arguments = submission.arguments.clone();
            if let Some(result) = frozen_result {
                bind_frozen_result(&mut validation_arguments, result)?;
            }
            let validation_spec = node.execution_spec();
            let bound_wire_schema = (direct_structured && installed.contract.version >= 61)
                .then(|| output_schema.clone());
            let parse_started = Instant::now();
            let validated = self
                .store_executor
                .execute(move |_| {
                    // Schema、reference kind、Rust-owned calendar、Review identity、
                    // deliberation 和 source closure 在同一 Store snapshot 中校验，防止
                    // 校验看到的 Artifact 与最终写入的血缘分叉。
                    if let Some(schema) = &bound_wire_schema {
                        validate_schema_value(&validation_arguments, schema, "$")
                            .map_err(ResearchError::InvalidOutput)?;
                    }
                    resolve_reference_kinds(
                        &mut validation_arguments,
                        &validation_manifest
                            .payload
                            .selections
                            .iter()
                            .map(|s| json!(s.artifact))
                            .collect::<Vec<_>>(),
                    )?;
                    if direct_structured && validation_contract.output.artifact_kind == ArtifactKind::DecisionProposal {
                        bind_rust_forecast_times(&validation_runtime.store, &validation_manifest, &mut validation_arguments, turn_now)?;
                    }
                    if validation_contract.output.artifact_kind == ArtifactKind::ProposalReview {
                        bind_review_identity(&validation_runtime.store, &validation_manifest, &validation_contract, &mut validation_arguments)?;
                    }
                    validate_submission_schema(
                        &validation_runtime.store,
                        &validation_contract,
                        &validation_arguments,
                    )?;
                    let (output, deliberation_note) = validation_runtime.extract_deliberation(
                        &validation_permit,
                        &validation_contract,
                        &validation_manifest,
                        validation_arguments,
                        turn_now,
                    )?;
                    validate_output_schema(
                        &validation_runtime.store,
                        &validation_contract,
                        &output,
                    )?;
                    if validation_contract.output.artifact_kind == ArtifactKind::Claim {
                        if let Some(horizon) = validation_spec.horizon_name()
                        {
                            if output.get("horizon").and_then(Value::as_str) != Some(horizon) {
                                return Err(ResearchError::InvalidOutput(
                                    "Claim horizon does not match its Rust-owned task scope"
                                        .to_owned(),
                                ));
                            }
                        }
                    }
                    if validation_contract.output.artifact_kind == ArtifactKind::RetrospectiveDraft
                    {
                        if let Some(horizon) = validation_spec.horizon_name()
                        {
                            if output.get("horizon").and_then(Value::as_str) != Some(horizon) {
                                return Err(ResearchError::InvalidOutput(
                                    "Retrospective horizon must match the Rust-owned stage"
                                        .to_owned(),
                                ));
                            }
                        }
                    }
                    let research_sources = research_output_source_refs(
                        &validation_runtime.store,
                        validation_contract.output.artifact_kind,
                        &output,
                        &validation_manifest,
                        turn_now,
                        validation_contract.version,
                    )?;
                    Ok::<_, ResearchError>((output, deliberation_note, research_sources))
                })
                .await?;

            self.record_pipeline_latency(permit, if direct_structured { "structured" } else { "submit" }, "parse_validate", parse_started.elapsed(), validated.is_ok(), model_turn).await?;
            if let Err(ResearchError::InvalidOutput(message)) = &validated {
                self.record_submit_rejection(
                    permit,
                    installed.contract.purpose.as_str(),
                    message.clone(),
                    trace_refs.last().cloned().into_iter().collect(),
                    turn_now,
                )
                .await?;
            }

            let (output, deliberation_note, research_sources) = match validated {
                Ok(validated) => validated,
                Err(ResearchError::InvalidOutput(message))
                    if submission_attempts.saturating_add(1)
                        < installed.contract.retry.max_attempts
                        && repair_round_fits(
                            model_phase_deadline(
                                wall_time,
                                phase,
                                installed.contract.version,
                            ),
                            started.elapsed(),
                        ) =>
                {
                    // 修复只把结构化拒绝作为下一轮 ToolOutput 反馈；不会新建无限 Agent
                    // 或重置 submission_attempts/预算，时间不足时直接保留原始拒绝。
                    submission_attempts = submission_attempts.saturating_add(1);
                    pending_tool_outputs.push(submission_rejection_feedback(submission.call_id, message));
                    model_turn = model_turn.saturating_add(1);
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Some(note) = &deliberation_note {
                trace_refs.push(ArtifactRef {
                    artifact_id: note.artifact_id.clone(),
                    kind: ArtifactKind::DeliberationNote,
                });
            }
            let output_sources = std::iter::once(ArtifactRef {
                artifact_id: manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            })
            .chain(trace_refs)
            .chain(research_sources)
            .collect();
            let output_permit = permit.clone();
            let output_contract = installed.contract.clone();
            let persist_started = Instant::now();
            let output_artifact = self
                .store_executor
                .execute(move |store| {
                    // DeliberationNote 和正式 output 在同一个 StoreExecutor 队列中 staged，
                    // 但 Artifact 的 kind/producer/origin 仍区分“模型研究产物”和“解释元数据”。
                    if let Some(note) = deliberation_note {
                        store.write_task_artifact(
                            &output_permit,
                            &note,
                            LifecycleEventType::DeliberationNoteCreated,
                            turn_now,
                        )?;
                    }
                    Ok::<_, ResearchError>(Artifact::new(
                        output_contract.output.artifact_kind,
                        // The daemon must commit this output with the same shared Store.
                        store.stage_json(&output)?,
                        format!("agent.{}", output_contract.purpose.as_str()),
                        ArtifactLifecycle::RunScoped,
                        ArtifactProvenance {
                            source_family: "akzio.agent".to_owned(),
                            observed_at: None,
                            retrieved_at: turn_now,
                            source_uri: None,
                            confidence_ppm: 1_000_000,
                            producer_contract_hash: Some(output_contract.contract_hash.clone()),
                        },
                        Some(output_permit.artifact_origin()),
                        output_sources,
                        turn_now,
                    )?)
                })
                .await??;
            self.record_pipeline_latency(permit, if direct_structured { "structured" } else { "submit" }, "persist_stage", persist_started.elapsed(), true, model_turn).await?;
            return Ok(output_artifact);
        }
    }

    async fn record_pipeline_latency(&self, permit: &TaskWritePermit, phase: &str, stage: &str,
        elapsed: StdDuration, succeeded: bool, revision: u16) -> ResearchResult<()> {
        // PipelineLatency 仅是 Debug observation；即使 succeeded=true，也不改变 Task
        // 状态、Decision、Paper 或 Outcome 的业务接受度。
        let permit = permit.clone();
        let phase = phase.to_owned();
        let stage = stage.to_owned();
        self.store_executor.execute(move |store| {
            if store.debug_session(&permit.run_id)?.is_some() {
                store.record_stage_acceptance(&akzio_domain::StageAcceptance {
                    version: 1, run_id: permit.run_id.clone(), task_id: permit.task_id.clone(), attempt_id: permit.attempt_id.clone(),
                    stage: format!("agent.{phase}.{stage}"), business_result: "PipelineLatency".into(),
                    test_result: akzio_domain::AcceptanceResult::Pass,
                    checks: vec![akzio_domain::AcceptanceCheck {
                        check_id: "agent.pipeline_latency".into(), category: akzio_domain::AcceptanceCategory::Schema,
                        expected: "Measured elapsed time; revision identifies the immutable AgentTurn".into(),
                        actual: json!({"phase":phase,"stage":stage,"latency_micros":elapsed.as_micros(),"succeeded":succeeded,"revision":revision}).to_string(),
                        result: akzio_domain::AcceptanceResult::Pass, evidence_refs: vec![], message: "Observation only; does not grant business acceptance".into(),
                    }], created_at: Utc::now(),
                })?;
            }
            Ok::<_, StoreError>(())
        }).await??;
        Ok(())
    }

    async fn record_submit_rejection(
        &self,
        permit: &TaskWritePermit,
        stage: &str,
        message: String,
        evidence_refs: Vec<ArtifactRef>,
        now: DateTime<Utc>,
    ) -> ResearchResult<()> {
        // SubmitRejected 对 canonical Run 也必须写入 StageAcceptance，因为 recovery
        // 需要把同一个 call_id 的拒绝反馈送回模型；它不是 TaskFailed 的替代品。
        let permit = permit.clone();
        let stage = stage.to_owned();
        self.store_executor
            .execute(move |store| {
                // Retry recovery needs this tool result for every run, including
                // canonical runs without a DebugSession.
                store.record_stage_acceptance(&akzio_domain::StageAcceptance {
                    version: 1,
                    run_id: permit.run_id.clone(),
                    task_id: permit.task_id.clone(),
                    attempt_id: permit.attempt_id.clone(),
                    stage,
                    business_result: "SubmitRejected".into(),
                    test_result: akzio_domain::AcceptanceResult::Fail,
                    checks: vec![akzio_domain::AcceptanceCheck {
                        check_id: "agent.submit_validation".into(),
                        category: akzio_domain::AcceptanceCategory::Schema,
                        expected: "Budget, canonical schema, scope and evidence validation"
                            .into(),
                        actual: message,
                        result: akzio_domain::AcceptanceResult::Fail,
                        evidence_refs,
                        message:
                            "Original rejected submission; preserved before any repair request"
                                .into(),
                    }],
                    created_at: now,
                })?;
                Ok::<_, akzio_store::StoreError>(())
            })
            .await??;
        Ok(())
    }
}

#[cfg(test)]
mod wall_time_split_tests {
    use super::*;

    #[test]
    fn draft_phase_keeps_more_than_the_old_fifty_five_percent_cut() {
        let total = std::time::Duration::from_secs(120);
        let draft = total.mul_f32(DRAFT_WALL_TIME_FRACTION);
        assert_eq!(draft.as_secs(), 84);
        assert!(draft > total.mul_f32(0.55));
        assert!(draft < total);
    }

    /// Real failure in run 27a69f3ccfad464b (2026-09-21): a Critic submission
    /// was rejected for empty `ground_closure` after 85.4s of a 120s allowance.
    /// The repair round was requested anyway and died on wall_time at 111.6s,
    /// reporting `wall_time` instead of the validation rejection that caused it.
    #[test]
    fn repair_round_is_skipped_when_it_cannot_finish_in_the_allowance() {
        let deadline = std::time::Duration::from_secs(120);
        let elapsed = std::time::Duration::from_millis(85_400);
        assert!(
            !repair_round_fits(deadline, elapsed),
            "a repair with 34.6s left must not be attempted"
        );
        // A rejection arriving early still gets its bounded repair.
        assert!(repair_round_fits(
            deadline,
            std::time::Duration::from_secs(30)
        ));
        // Exactly half the allowance remaining is still enough.
        assert!(repair_round_fits(
            deadline,
            std::time::Duration::from_secs(60)
        ));
        // Past the deadline there is nothing left to spend.
        assert!(!repair_round_fits(deadline, deadline));
        assert!(!repair_round_fits(
            deadline,
            deadline + std::time::Duration::from_secs(5)
        ));
    }

    #[test]
    fn configured_million_output_reaches_research_submit_without_role_caps() {
        for purpose in [RESEARCH_ANALYST_RECIPE_ID, akzio_domain::RESEARCH_CRITIC_RECIPE_ID, RESEARCH_SYNTHESIZER_RECIPE_ID] {
            let mut policy = akzio_domain::budget::default_agent_budget(purpose).unwrap();
            policy.max_output_tokens = 1_000_000;
            let mut budget = AgentRunBudget::new(&policy, &RetryPolicy::none());
            assert_eq!(phase_output_cap(&budget, AgentTurnPhase::Submit, purpose, 0, 65, None).unwrap(), 1_000_000);
            budget.record_turn(10, 2_525, None).unwrap();
            assert_eq!(phase_output_cap(&budget, AgentTurnPhase::Submit, purpose, 0, 65, None).unwrap(), 997_475);
            assert_eq!(phase_output_cap(&budget, AgentTurnPhase::Submit, purpose, 0, 65, Some(128_000)).unwrap(), 128_000);
            assert_eq!(budget.max_input_tokens, policy.max_input_tokens);
            assert_eq!(budget.wall_time.as_secs(), 180);
        }
    }

    #[test]
    fn shared_deadline_reserves_audit_time_without_extending_task() {
        let total = StdDuration::from_secs(120);
        assert_eq!(model_phase_deadline(total, AgentTurnPhase::Submit, 49), StdDuration::from_secs(119));
        assert_eq!(model_phase_deadline(total, AgentTurnPhase::Draft, 49), StdDuration::from_secs(84));
        assert_eq!(model_phase_deadline(total, AgentTurnPhase::Submit, 47), total);
        let short = StdDuration::from_secs(2);
        assert_eq!(model_phase_deadline(short, AgentTurnPhase::Submit, 49), StdDuration::from_millis(1800));
    }

}
