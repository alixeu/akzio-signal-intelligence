impl AgentRuntime {
    pub async fn run(
        &self,
        permit: &TaskWritePermit,
        node: &WorkflowNode,
        candidates: impl IntoIterator<Item = ArtifactRef>,
        model: &dyn AgentModel,
        now: DateTime<Utc>,
    ) -> ResearchResult<Artifact> {
        self.validate_authority_permit(permit)?;
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
        if node.budget != installed.contract.budget
            || node.retry != installed.contract.retry
            || node.on_failure != installed.contract.on_failure
        {
            return Err(ResearchError::NodePolicyMismatch);
        }
        let manifest = if let Some(parent_task_id) = &node.parent_task_id {
            if !node.dependencies.contains(parent_task_id) {
                return Err(ResearchError::InvalidOutput(
                    "parent task is not a declared dependency".to_owned(),
                ));
            }
            let proof = self.load_parent_succeeded_attempt(&permit.run_id, parent_task_id)?;
            let parent_contract_hash = proof.contract_hash.as_ref().ok_or_else(|| {
                ResearchError::InvalidOutput("parent attempt has no contract hash".to_owned())
            })?;
            let parent_contract = &self.catalogue.get(parent_contract_hash)?.contract;
            self.context.assemble_child_from_proof(
                &proof,
                parent_contract,
                permit,
                &installed.contract,
                now,
                self.grant_ttl,
            )?
        } else {
            self.context
                .assemble(permit, &installed.contract, candidates, now, self.grant_ttl)?
        };
        if !manifest.grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let context = self.context_values(permit, &installed.contract, &manifest, now)?;
        let governance = String::from_utf8(
            self.context.read_authority_document(
                &installed.contract,
                &installed.contract.prompt.governance,
            )?,
        )
                .map_err(|_| {
                    ResearchError::InvalidOutput("governance prompt is not UTF-8".to_owned())
                })?;
        let role = String::from_utf8(
            self.context.read_authority_document(
                &installed.contract,
                &installed.contract.prompt.role,
            )?,
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
            "{governance}\n\n{role}\n\nDuring Draft, use granted read tools as needed, then return a concise, auditable research memo in {response_language}. State conclusions, evidence, counter-evidence, and uncertainty without exposing hidden chain-of-thought. During Submit, call submit_result exactly once; keep JSON property names, enum literals, identifiers, symbols, and cited source text unchanged.\n\nTop-level ContextManifest references (copy exact artifact_id and kind into result references; a blocked decision still preserves selected claims and their grounds):\n{reference_ledger}"
        );
        let prompt = format!(
            "{prompt}\n\nSubmission invariant: result.claims and result.hard_blockers must not both be empty; copy at least one selected claim reference when claims are available."
        );
        let output_schema: Value = serde_json::from_slice(
            &self.context.read_authority_document(
                &installed.contract,
                &installed.contract.output.schema,
            )?,
        )?;
        let run_purpose = self.run_purpose_for(&permit.run_id)?;
        let tools = if !should_advertise_read_tools(
            run_purpose,
            context.len(),
            installed.contract.budget.max_tool_calls,
        ) {
            Vec::new()
        } else {
 model_tool_definitions(&self.context, &installed.contract)?
        };
        let mut continuation = None;
        let mut pending_tool_outputs = Vec::new();
        let mut trace_refs = Vec::new();
        let mut tool_calls = 0_u16;
        let mut model_turn = 0_u16;
        let mut provider_calls = 0_u32;
        let max_provider_calls = u32::from(installed.contract.retry.max_attempts)
            .saturating_mul(u32::from(installed.contract.budget.max_tool_calls) + 3);
        let mut phase = AgentTurnPhase::Draft;
        let mut submission_attempts = 0_u8;
        let started = Instant::now();
        let wall_time =
            StdDuration::from_secs(u64::from(installed.contract.budget.max_wall_time_secs));
        loop {
            if started.elapsed() > wall_time {
                return Err(ResearchError::WallTimeExceeded {
                    maximum_secs: installed.contract.budget.max_wall_time_secs,
                });
            }
            let request = AgentModelRequest {
                contract_hash: installed.contract.contract_hash.clone(),
                purpose: installed.contract.purpose.as_str().to_owned(),
                phase,
                prompt: prompt.clone(),
                objective: node.objective.clone(),
                manifest_artifact_id: manifest.artifact.artifact_id.clone(),
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
                    "The research memo is complete. Call submit_result exactly once with the final contract output. Do not call any other tool or add assistant text."
                        .to_owned()
                }),
                max_output_tokens: installed.contract.budget.max_output_tokens,
                tools: if phase == AgentTurnPhase::Draft {
                    tools.clone()
                } else {
                    Vec::new()
                },
                terminal: (phase == AgentTurnPhase::Submit).then(|| AgentTerminalDefinition {
                    description: format!(
                        "Submit the final {} contract output for Rust validation. This has no side effects.",
                        installed.contract.purpose.as_str()
                    ),
                    input_schema: output_schema.clone(),
                }),
            };
            let input_tokens = estimate_tokens(&request)?;
            if input_tokens > installed.contract.budget.max_input_tokens {
                return Err(ResearchError::InputBudgetExceeded {
                    actual: input_tokens,
                    maximum: installed.contract.budget.max_input_tokens,
                });
            }
            let tool_set_hash = tool_set_hash(&request)?;
            let mut turn_attempt = 1_u8;
            let (turn, capability_snapshot, capability_snapshot_hash, request_hash) = loop {
                let capability_snapshot = model.capability_snapshot();
                let capability_snapshot_hash = capability_snapshot_hash(&capability_snapshot)?;
                if let Err(capability) = validate_model_capabilities(&capability_snapshot, &request)
                {
                    let turn_now = logical_now(now, started.elapsed());
                    let failed_turn = self.record_failed_turn(
                        &TurnRecord {
                            permit,
                            contract: &installed.contract,
                            manifest: &manifest,
                            turn: model_turn,
                            attempt: turn_attempt,
                            now: turn_now,
                        },
                        &request,
                        "capability_mismatch",
                        None,
                        None,
                        false,
                        &capability_snapshot,
                        &capability_snapshot_hash,
                        &tool_set_hash,
                    )?;
                    trace_refs.push(ArtifactRef {
                        artifact_id: failed_turn.artifact_id,
                        kind: ArtifactKind::AgentTurn,
                    });
                    return Err(capability);
                }
                let request_hash = model_request_hash(&request)?;
                self.validate_authority_permit(permit)?;
                self.store.append_task_event(
                    permit,
                    LifecycleEventType::AgentTurnStarted,
                    logical_now(now, started.elapsed()),
                )?;
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
                if provider_calls >= max_provider_calls {
                    return Err(ResearchError::ModelCallBudgetExceeded);
                }
                provider_calls = provider_calls.saturating_add(1);
                match model.turn_with_events(request.clone(), on_event).await {
                    Ok(turn) => {
                        break (
                            turn,
                            capability_snapshot,
                            capability_snapshot_hash,
                            request_hash,
                        );
                    }
                    Err(error) => {
                        let retryable = retryable_model_error(&error, &installed.contract.retry);
                        let will_retry =
                            retryable && turn_attempt < installed.contract.retry.max_attempts;
                        let turn_now = logical_now(now, started.elapsed());
                        let failed_turn = self.record_failed_turn(
                            &TurnRecord {
                                permit,
                                contract: &installed.contract,
                                manifest: &manifest,
                                turn: model_turn,
                                attempt: turn_attempt,
                                now: turn_now,
                            },
                            &request,
                            model_error_class(&error),
                            Some(research_error_detail(&error)),
                            model_debug_trace(&error),
                            will_retry,
                            &capability_snapshot,
                            &capability_snapshot_hash,
                            &tool_set_hash,
                        )?;
                        trace_refs.push(ArtifactRef {
                            artifact_id: failed_turn.artifact_id,
                            kind: ArtifactKind::AgentTurn,
                        });
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
                                maximum_secs: installed.contract.budget.max_wall_time_secs,
                            });
                        }
                        if !backoff.is_zero() {
                            tokio::time::sleep(backoff).await;
                        }
                        turn_attempt = turn_attempt.saturating_add(1);
                    }
                }
            };
            if started.elapsed() > wall_time {
                let turn_now = logical_now(now, started.elapsed());
                let failed_turn = self.record_failed_turn(
                    &TurnRecord {
                        permit,
                        contract: &installed.contract,
                        manifest: &manifest,
                        turn: model_turn,
                        attempt: turn_attempt,
                        now: turn_now,
                    },
                    &request,
                    "wall_time",
                    None,
                    None,
                    false,
                    &capability_snapshot,
                    &capability_snapshot_hash,
                    &tool_set_hash,
                )?;
                trace_refs.push(ArtifactRef {
                    artifact_id: failed_turn.artifact_id,
                    kind: ArtifactKind::AgentTurn,
                });
                return Err(ResearchError::WallTimeExceeded {
                    maximum_secs: installed.contract.budget.max_wall_time_secs,
                });
            }
            let turn_now = logical_now(now, started.elapsed());
            let turn_artifact = self.record_turn(
                &TurnRecord {
                    permit,
                    contract: &installed.contract,
                    manifest: &manifest,
                    turn: model_turn,
                    attempt: turn_attempt,
                    now: turn_now,
                },
                &request,
                &turn,
                &capability_snapshot,
                &capability_snapshot_hash,
                &tool_set_hash,
            )?;
            trace_refs.push(ArtifactRef {
                artifact_id: turn_artifact.artifact_id,
                kind: ArtifactKind::AgentTurn,
            });
            continuation = Some(turn.continuation.clone());
            pending_tool_outputs.clear();
            if phase == AgentTurnPhase::Draft && turn.terminal_submission.is_some() {
                return Err(ResearchError::AmbiguousSubmission);
            }
            if phase == AgentTurnPhase::Draft && !turn.tool_calls.is_empty() {
                let next = tool_calls.saturating_add(turn.tool_calls.len() as u16);
                if next > installed.contract.budget.max_tool_calls {
                    return Err(ResearchError::ToolBudgetExceeded);
                }
                for call in turn.tool_calls {
                    let call_id = call.call_id.clone();
                    let tool_result = self.execute_tool(
                        permit,
                        &installed.contract,
                        &manifest.grant,
                        &call,
                        &request_hash,
                        turn_now,
                    )?;
                    trace_refs.push(ArtifactRef {
                        artifact_id: tool_result.artifact.artifact_id.clone(),
                        kind: ArtifactKind::ToolResult,
                    });
                    pending_tool_outputs.push(ModelToolOutput {
                        call_id,
                        output: tool_result.value,
                    });
                }
                tool_calls = next;
                model_turn = model_turn.saturating_add(1);
                continue;
            }
            if phase == AgentTurnPhase::Draft {
                let memo = turn
                    .assistant_text
                    .as_deref()
                    .filter(|text| !text.trim().is_empty())
                    .ok_or(ResearchError::MissingFinalOutput)?;
                let output_tokens = estimate_tokens(&memo)?;
                if output_tokens > installed.contract.budget.max_output_tokens {
                    return Err(ResearchError::OutputBudgetExceeded {
                        actual: output_tokens,
                        maximum: installed.contract.budget.max_output_tokens,
                    });
                }
                phase = AgentTurnPhase::Submit;
                model_turn = model_turn.saturating_add(1);
                continue;
            }

            if !turn.tool_calls.is_empty() || turn.assistant_text.is_some() {
                return Err(ResearchError::AmbiguousSubmission);
            }
            let submission = turn
                .terminal_submission
                .ok_or(ResearchError::MissingFinalOutput)?;
            let output_tokens = estimate_tokens(&submission.arguments)?;
            if output_tokens > installed.contract.budget.max_output_tokens {
                return Err(ResearchError::OutputBudgetExceeded {
                    actual: output_tokens,
                    maximum: installed.contract.budget.max_output_tokens,
                });
            }

            let validated = (|| {
                validate_submission_schema(
                    &self.store,
                    &installed.contract,
                    &submission.arguments,
                )?;
                let (output, deliberation_note) = self.extract_deliberation(
                    permit,
                    &installed.contract,
                    &manifest,
                    submission.arguments.clone(),
                    turn_now,
                )?;
                validate_output_schema(&self.store, &installed.contract, &output)?;
                let research_sources = research_output_source_refs(
                    &self.store,
                    installed.contract.output.artifact_kind,
                    &output,
                    &manifest,
                )?;
                Ok::<_, ResearchError>((output, deliberation_note, research_sources))
            })();

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
            if let Some(note) = deliberation_note {
                self.store.write_task_artifact(
                    permit,
                    &note,
                    LifecycleEventType::DeliberationNoteCreated,
                    turn_now,
                )?;
                trace_refs.push(ArtifactRef {
                    artifact_id: note.artifact_id,
                    kind: ArtifactKind::DeliberationNote,
                });
            }
            let output_artifact = Artifact::new(
                installed.contract.output.artifact_kind,
                self.store.put_json(&output)?,
                format!("agent.{}", installed.contract.purpose.as_str()),
                ArtifactLifecycle::RunScoped,
                ArtifactProvenance {
                    source_family: "akzio.agent".to_owned(),
                    observed_at: None,
                    retrieved_at: turn_now,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: Some(installed.contract.contract_hash.clone()),
                },
                Some(permit.artifact_origin()),
                std::iter::once(ArtifactRef {
                    artifact_id: manifest.artifact.artifact_id.clone(),
                    kind: ArtifactKind::ContextManifest,
                })
                .chain(trace_refs)
                .chain(research_sources)
                .collect(),
                turn_now,
            )?;
            return Ok(output_artifact);
        }
    }
}
