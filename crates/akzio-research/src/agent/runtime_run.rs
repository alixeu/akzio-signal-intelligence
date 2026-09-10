// The Attempt wall-time remains the Contract's fixed total. Draft needs the
// larger share because it may perform a real provider call plus bounded reads;
// Submit is deliberately kept within the remaining 30% and cannot extend the
// Attempt. The previous 55/45 split killed legitimate high-reasoning Drafts at
// 66s even when the Contract still had 54s left.
const DRAFT_WALL_TIME_FRACTION: f32 = 0.70;

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
                        .map(|task| task.node.budget)
                })
            })
            .await??;
        if frozen.as_ref() != Some(&node.budget)
            || budget.resolved_policy() != node.budget
            || node.retry != installed.contract.retry
            || node.on_failure != installed.contract.on_failure
        {
            return Err(ResearchError::NodePolicyMismatch);
        }
        let candidates = candidates.into_iter().collect::<Vec<_>>();
        let manifest = if let Some(parent_task_id) = &node.parent_task_id {
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
                .execute(move |_| context.assemble(&permit, &contract, candidates, now, grant_ttl))
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
        let prompt = format!(
            "{governance}\n\n{role}\n\nDuring Draft, use granted read tools as needed, then return a concise, auditable research memo in {response_language}. State conclusions, evidence, counter-evidence, and uncertainty without exposing hidden chain-of-thought. During Submit, call submit_result exactly once; keep JSON property names, enum literals, identifiers, symbols, and cited source text unchanged.\n\nTop-level ContextManifest references (copy exact artifact_id and kind into result references; a blocked decision still preserves selected claims and their grounds):\n{reference_ledger}\n\nWire Submit references contain only artifact_id, not kind. Rust resolves kind from this immutable ledger. Select only IDs allowed by the specific field schema; a Claim is not an evidence ground."
        );
        let prompt = if installed.contract.output.artifact_kind == ArtifactKind::DecisionProposal {
            format!(
                "{prompt}\n\nSubmission invariant: result.claims and result.hard_blockers must not both be empty; copy at least one selected claim reference when claims are available. result.research_allocation is mandatory: include all four executable assets exactly once, explicit integer-ppm cash, and make the four asset weights plus cash sum to 1000000."
            )
        } else {
            prompt
        };
        let prompt = format!(
            "{prompt}\n\nResolved Attempt resource budget: {}. max_input_tokens is the cumulative input across all LLM requests in this Attempt, not the provider context window. Draft and Submit share this budget.",
            serde_json::to_string(&node.budget)?
        );
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
        let run_purpose = self.run_purpose_for(&permit.run_id).await?;
        let tools = if !should_advertise_read_tools(run_purpose) {
            Vec::new()
        } else {
            model_tool_definitions(&self.context, &installed.contract)?
        };
        let terminal = AgentTerminalDefinition {
            description: format!(
                "Submit the final {} contract output for Rust validation. Reference objects use only artifact_id: Rust resolves kind from the immutable Manifest before canonical validation. This has no side effects.",
                installed.contract.purpose.as_str()
            ),
            input_schema: output_schema.clone(),
        };
        let prefetched_capabilities = model.capability_snapshot();
        let budget_policy = model.budget_policy();
        budget.attach_budget_policy(&budget_policy)?;
        let budget_policy_hash = budget_policy_hash(&budget_policy)?;
        let recovery_guard = AgentRecoveryGuard {
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
        let mut prefetched_capabilities = Some(prefetched_capabilities);
        if matches!(&recovery.source, AgentRecoverySource::Recovered(_)) {
            budget.restore(&recovery)?;
        }
        self.observe_debug_budget(permit, budget, "AttemptBudgetReady")
            .await?;
        let mut continuation = recovery.continuation;
        let mut pending_tool_outputs = recovery.pending_tool_outputs;
        let mut trace_refs = recovery.trace_refs;
        let mut model_turn = recovery.next_model_turn;
        let mut phase = recovery.phase;
        // Recovery only enters Submit after replaying a persisted, nonempty memo.
        let mut draft_completed = phase == AgentTurnPhase::Submit;
        let mut submission_attempts = 0_u8;
        let started = budget.started;
        let wall_time = budget.wall_time;
        loop {
            budget.check_wall()?;
            // Reserve half the total output for submission and schema repair.
            // The cap is cumulative across draft tools and recovered turns.
            let submit_reserve = (budget.max_output_tokens / 2).max(1);
            if phase == AgentTurnPhase::Draft
                && (budget.remaining_output_tokens()? <= submit_reserve
                    || budget.started.elapsed()
                        >= budget.wall_time.mul_f32(DRAFT_WALL_TIME_FRACTION))
            {
                return Err(ResearchError::InvalidOutput(
                    "draft_incomplete: reserved Submit budget reached before a memo was completed"
                        .to_owned(),
                ));
            }
            if phase == AgentTurnPhase::Submit && !draft_completed {
                return Err(ResearchError::MissingFinalOutput);
            }
            let max_output_tokens = if phase == AgentTurnPhase::Draft {
                budget
                    .remaining_output_tokens()?
                    .saturating_sub(submit_reserve)
                    .max(1)
            } else {
                budget.remaining_output_tokens()?
            };
            let max_output_tokens = if phase == AgentTurnPhase::Draft
                && installed.contract.purpose.as_str() == akzio_domain::RESEARCH_CRITIC_RECIPE_ID
            {
                max_output_tokens.min(700)
            } else if phase == AgentTurnPhase::Draft
                && installed.contract.purpose.as_str()
                    == akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
            {
                max_output_tokens.min(1_800)
            } else {
                max_output_tokens
            };

            let mut request = AgentModelRequest {
                contract_hash: installed.contract.contract_hash.clone(),
                purpose: installed.contract.purpose.as_str().to_owned(),
                phase,
                prompt: if phase == AgentTurnPhase::Draft && !budget.max_tool_calls.allows(u64::from(budget.tool_calls) + 1) {
                    format!("{prompt}\nRead tool budget exhausted. Complete the concise Draft memo using available facts; explicitly preserve insufficient evidence. Rust will then request Submit.")
                } else { prompt.clone() },
                objective: node.objective.clone(),
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
                continuation_instruction: (phase == AgentTurnPhase::Submit
                    && pending_tool_outputs.is_empty())
                .then(|| {
                    "Draft memo is complete. Call submit_result exactly once. Before submitting: use exact IDs from the schema and their original kinds, exact task horizon, scoped evidence gaps, and matching source/resource pairs. Unavailable evidence may be reported with supplemental_needs=[]; do not invent replacement requests. Preserve uncertainty and missing support. No other tools or assistant text."
                        .to_owned()
                }),
                max_output_tokens,
                // Draft and Submit have separate, audited phase costs. Analyst
                // Drafts use medium reasoning so the fixed 120s Attempt can
                // still reach Submit; the model, evidence, output contract,
                // and target semantics are unchanged. Submit is deterministic
                // schema formatting and uses low reasoning.
                reasoning_effort: if phase == AgentTurnPhase::Submit
                    || installed.contract.purpose.as_str()
                        == akzio_domain::RESEARCH_CRITIC_RECIPE_ID
                {
                    Some("low".to_owned())
                } else if matches!(
                    installed.contract.purpose.as_str(),
                    RESEARCH_ANALYST_RECIPE_ID
                        | akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
                ) {
                    Some("medium".to_owned())
                } else {
                    None
                },
                tools: if phase == AgentTurnPhase::Draft && budget.max_tool_calls.allows(u64::from(budget.tool_calls) + 1) {
                    tools.clone()
                } else {
                    Vec::new()
            },
            terminal: (phase == AgentTurnPhase::Submit).then(|| terminal.clone()),
        };
            if let Some(projection) = self.historical_projection {
                projection.project_request(&mut request);
            }
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
                    let turn_now = logical_now(now, started.elapsed());
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
                budget.authorize_model_call(input_tokens)?;
                let request_hash = model_request_hash(&request)?;
                self.validate_authority_permit(permit).await?;
                let event_permit = permit.clone();
                let event_now = logical_now(now, started.elapsed());
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
                let phase_deadline = if phase == AgentTurnPhase::Draft {
                    wall_time.mul_f32(DRAFT_WALL_TIME_FRACTION)
                } else {
                    wall_time
                };
                let call = tokio::time::timeout(
                    phase_deadline.saturating_sub(started.elapsed()),
                    model.turn_with_events(request.clone(), on_event),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(ResearchError::WallTimeExceeded {
                        maximum_secs: node.budget.max_wall_time_secs,
                    })
                });
                match call {
                    Ok(turn) => {
                        break (turn, runtime_snapshot, request_hash);
                    }
                    Err(error) => {
                        let retryable = retryable_model_error(&error, &installed.contract.retry);
                        let draft_deadline = phase == AgentTurnPhase::Draft
                            && matches!(error, ResearchError::WallTimeExceeded { .. });
                        let will_retry = !draft_deadline
                            && retryable
                            && turn_attempt < installed.contract.retry.max_attempts;
                        let turn_now = logical_now(now, started.elapsed());
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
                        budget.record_failed_turn(input_tokens)?;
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
                let turn_now = logical_now(now, started.elapsed());
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
                return Err(ResearchError::WallTimeExceeded {
                    maximum_secs: node.budget.max_wall_time_secs,
                });
            }
            let turn_now = logical_now(now, started.elapsed());
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
            budget.record_turn(
                input_tokens,
                estimate_turn_output_tokens(&turn)?,
                turn.telemetry.as_ref(),
            )?;
            continuation = Some(turn.continuation.clone());
            self.observe_debug_budget(permit, budget, "TurnAccounted")
                .await?;
            pending_tool_outputs.clear();
            if phase == AgentTurnPhase::Draft && turn.terminal_submission.is_some() {
                return Err(ResearchError::AmbiguousSubmission);
            }
            if phase == AgentTurnPhase::Draft && !turn.tool_calls.is_empty() {
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

            let validation_runtime = self.clone();
            let validation_permit = permit.clone();
            let validation_contract = installed.contract.clone();
            let validation_manifest = manifest.clone();
            let mut validation_arguments = submission.arguments.clone();
            let validation_objective = node.objective.clone();
            let validated = self
                .store_executor
                .execute(move |_| {
                    resolve_reference_kinds(
                        &mut validation_arguments,
                        &validation_manifest
                            .payload
                            .selections
                            .iter()
                            .map(|s| json!(s.artifact))
                            .collect::<Vec<_>>(),
                    )?;
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
                        if let Some(horizon) = validation_objective
                            .strip_prefix("[research_horizon=")
                            .and_then(|s| s.split_once(']').map(|p| p.0))
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
                        if let Some(horizon) = validation_objective
                            .strip_prefix("[outcome_horizon=")
                            .and_then(|s| s.split_once(']').map(|p| p.0))
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
                    )?;
                    Ok::<_, ResearchError>((output, deliberation_note, research_sources))
                })
                .await?;

            if let Err(ResearchError::InvalidOutput(message)) = &validated {
                let permit = permit.clone();
                let message = message.clone();
                let stage = installed.contract.purpose.as_str().to_owned();
                let evidence_refs = trace_refs.last().cloned().into_iter().collect();
                self.store_executor.execute(move |store| {
                    if store.debug_session(&permit.run_id)?.is_some() {
                        store.record_stage_acceptance(&akzio_domain::StageAcceptance {
                            version: 1, run_id: permit.run_id.clone(), task_id: permit.task_id.clone(),
                            attempt_id: permit.attempt_id.clone(), stage,
                            business_result: "SubmitRejected".into(), test_result: akzio_domain::AcceptanceResult::Fail,
                            checks: vec![akzio_domain::AcceptanceCheck {
                                check_id: "agent.submit_validation".into(), category: akzio_domain::AcceptanceCategory::Schema,
                                expected: "Canonical schema, scope and evidence validation".into(), actual: message.clone(),
                                result: akzio_domain::AcceptanceResult::Fail, evidence_refs,
                                message: "Original rejected submission; preserved before any repair request".into(),
                            }], created_at: turn_now,
                        })?;
                    }
                    Ok::<_, akzio_store::StoreError>(())
                }).await??;
            }

            let (output, deliberation_note, research_sources) = match validated {
                Ok(validated) => validated,
                Err(error @ ResearchError::InvalidOutput(_))
                    if submission_attempts.saturating_add(1)
                        < installed.contract.retry.max_attempts =>
                {
                    submission_attempts = submission_attempts.saturating_add(1);
                    pending_tool_outputs.push(ModelToolOutput {
                        call_id: submission.call_id,
                        output: json!({
                            "ok": false,
                            "error": "invalid_submission",
                            "message": error.to_string(),
                        }),
                    });
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
            let output_artifact = self
                .store_executor
                .execute(move |store| {
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
            return Ok(output_artifact);
        }
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
}
