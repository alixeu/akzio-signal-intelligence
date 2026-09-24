// 这些辅助方法把模型输出转成可审计 Artifact：deliberation 是 RunScoped 说明，
// 不是新的权威证据；Turn trace 记录 request/capability/budget identity，供恢复和
// replay 对照，StoreExecutor 保证异步调用与同步 CAS 写入之间的顺序。
impl AgentRuntime {
    fn extract_deliberation(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        output: Value,
        now: DateTime<Utc>,
    ) -> ResearchResult<(Value, Option<Artifact>)> {
        // 先解析完整 envelope，再把 basis ID 限制在本次 Manifest。模型自报 confidence
        // 不能写入 Artifact provenance，否则会影响后续 Context 排序并形成自授信。
        if contract.deliberation_policy == DeliberationPolicy::Disabled {
            return Ok((output, None));
        }
        let mut envelope: AgentOutputEnvelope =
            serde_json::from_value(output).map_err(|error| {
                ResearchError::InvalidOutput(format!("deliberation envelope: {error}"))
            })?;
        envelope.deliberation.assessment_source = Some("model_assessed".to_owned());
        if contract.version >= 65 && matches!(contract.output.artifact_kind,
            ArtifactKind::Claim | ArtifactKind::Critique | ArtifactKind::DecisionProposal) {
            validate_research_deliberation(&envelope.deliberation)?;
        } else {
            envelope.deliberation.validate_model_assessment()
                .map_err(|error| ResearchError::InvalidOutput(error.to_string()))?;
        }

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
            self.store.stage_json(&envelope.deliberation)?,
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
// (`akzio-context` context_broker/manifest.rs), which would let an
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

    async fn context_materialization(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        effective_budget: &TaskBudget,
        now: DateTime<Utc>,
    ) -> ResearchResult<ContextMaterialization> {
        // materialization identity 同时绑定 Manifest、Grant、Contract 和预算；
        // 后续 continuation/recovery 只接受同一 identity，不会把新上下文拼进旧 Attempt。
        if !manifest.grant.matches_permit(permit) {
            return Err(ResearchError::GrantPermitMismatch);
        }
        let context = self.context.clone();
        let permit = permit.clone();
        let contract = contract.clone();
        let manifest = manifest.clone();
        let effective_budget = effective_budget.clone();
        Ok(self
            .store_executor
            .execute(move |_| {
                context.materialize_for_agent_with_budget(
                    &permit,
                    &contract,
                    &manifest,
                    &effective_budget,
                    now,
                )
            })
            .await??)
    }

    async fn observe_debug_budget(&self,permit:&TaskWritePermit,budget:&AgentRunBudget,boundary:&str)->ResearchResult<()> {
        // Debug budget 是 observation-only StageAcceptance，不改变预算，也不表示
        // 当前 Agent 或 Run 已通过业务验收。
        let permit=permit.clone();let snapshot=budget.debug_observation(boundary);
        self.store_executor.execute(move|store|store.observe_debug_budget(&permit,&snapshot,chrono::Utc::now())).await??;
        Ok(())
    }

    async fn record_turn(
        &self,
        record: TurnRecord,
        request: &AgentModelRequest,
        response: &AgentModelTurn,
        runtime_snapshot: &AgentTurnRuntimeSnapshot,
    ) -> ResearchResult<Artifact> {
        // completed trace 先写入 shared Store，再由 Store 发出 AgentTurnCompleted；
        // response 只是模型事实，正式输出 Artifact 仍要经过 Submit validation。
        let request_hash = model_request_hash(request)?;
        let request = request.clone();
        let response = response.clone();
        let runtime_snapshot = runtime_snapshot.clone();
        self.store_executor
            .execute(move |store| {
                let mut trace = record.request_trace(&request, &request_hash, &runtime_snapshot);
                trace["lifecycle"] = json!({
                    "status": "completed",
                    "completed_at_utc": record.now,
                });
                trace["response"] = json!(response);
                let artifact = record.stage_artifact(&store, &trace)?;
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
        // 失败 trace 保留 error_detail、debug trace 和 will_retry，便于恢复判断；
        // 即使可重试，也不会把这次已经发生的 Provider 调用从审计账本删除。
        let request_hash = model_request_hash(request)?;
        let request = request.clone();
        let error_class = error_class.to_owned();
        let model_debug = model_debug.cloned();
        let runtime_snapshot = runtime_snapshot.clone();
        self.store_executor
            .execute(move |store| {
                let mut trace = record.request_trace(&request, &request_hash, &runtime_snapshot);
                trace["lifecycle"] = json!({
                    "status": "failed",
                    "completed_at_utc": record.now,
                });
                trace["error_class"] = json!(error_class);
                trace["will_retry"] = json!(will_retry);
                if let Some(error_detail) = error_detail {
                    trace["error_detail"] = error_detail;
                }
                if let Some(model_debug) = model_debug {
                    trace["model_debug"] = serde_json::to_value(model_debug)?;
                }
                let artifact = record.stage_artifact(&store, &trace)?;
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

impl TurnRecord {
    fn request_trace(
        &self,
        request: &AgentModelRequest,
        request_hash: &akzio_domain::ContentHash,
        runtime_snapshot: &AgentTurnRuntimeSnapshot,
    ) -> Value {
        // request/domain_request 是领域请求快照；真实 Provider wire body 只有在
        // 明确 Debug 时才放进 model_debug，避免把凭据或未授权原文写进普通 trace。
        json!({
            "trace_schema_version": 1,
            "turn": self.turn,
            "attempt": self.attempt,
            "call_id": format!("agent-turn:{}", request_hash),
            "contract_hash": &self.contract.contract_hash,
            "context_manifest": &self.manifest.artifact.artifact_id,
            "read_grant_snapshot": {
                "authority": "observation_only; runtime derives a fresh grant from the persisted manifest",
                "run_id": self.manifest.grant.run_id,
                "task_id": self.manifest.grant.task_id,
                "attempt_id": self.manifest.grant.attempt_id,
                "contract_hash": self.manifest.grant.contract_hash,
                "readable": self.manifest.grant.readable,
                "expires_at": self.manifest.grant.expires_at,
            },
            "request_hash": request_hash,
            "capability_snapshot": runtime_snapshot.capability,
            "capability_snapshot_hash": runtime_snapshot.capability_hash,
            "resolved_budget": runtime_snapshot.resolved_budget,
            "budget_usage": runtime_snapshot.budget_usage,
            "budget_policy": runtime_snapshot.budget_policy,
            "budget_policy_hash": runtime_snapshot.budget_policy_hash,
            "tool_set_hash": runtime_snapshot.tool_set_hash,
            // `request` is retained for compatibility. `domain_request` is
            // the explicit audit name; the actual provider wire body, when
            // Debug is authorized, is `model_debug.request`.
            "domain_request": request,
            "request": request,
        })
    }

    fn stage_artifact(&self, store: &Store, trace: &Value) -> ResearchResult<Artifact> {
        // AgentTurn 的 origin 精确指向 Run/Task/Attempt/Contract，source_refs 只连
        // ContextManifest；模型输出和后续 Deliberation/Claim 的血缘由上层继续扩展。
        Ok(Artifact::new(
            ArtifactKind::AgentTurn,
            store.stage_json(trace)?,
            format!("agent.turn.{}", self.contract.purpose.as_str()),
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.agent".to_owned(),
                observed_at: None,
                retrieved_at: self.now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: Some(self.contract.contract_hash.clone()),
            },
            Some(self.permit.artifact_origin()),
            vec![ArtifactRef {
                artifact_id: self.manifest.artifact.artifact_id.clone(),
                kind: ArtifactKind::ContextManifest,
            }],
            self.now,
        )?)
    }
}
