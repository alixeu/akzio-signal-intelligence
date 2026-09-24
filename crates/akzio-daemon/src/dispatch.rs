//! Exhaustive runtime task dispatch.

// 文件导读：任务 dispatch 是 workflow recipe 到应用门面的唯一选择点。它先让
// `NodeExecutor` 接住 Store 已 claim 的 permit，再按 task class 进入 research/evidence/
// decision/execution/paper/reconcile/evaluate；错误由 runtime 转成 Deferred/Retry/Failed，
// 不把 handler 的 `Ok` 解释成业务完成。
// Rust 机制：`NodeExecutor` 返回带生命周期的 `Pin<Box<dyn Future + Send>>`，使 trait
// 可以在 Tokio worker 中执行；迭代器闭包收集祖先输出，`BTreeMap/BTreeSet` 保证去重和稳定
// 顺序，借用 `&ClaimedAttempt` 防止 handler 越权取得 permit 所有权。

use super::*;

impl Daemon {
    // 消费一个 Store 已 claim 的 Attempt，并把开始时间封装为 NodeContext 交给 Runtime trait；
    // Future 在 worker 调用处被 await，Attempt lease 的提交/失败仍由 TaskRuntime 持有。
    pub(crate) async fn execute_task(&self, task: ClaimedAttempt) -> TaskCompletion {
        akzio_runtime::NodeExecutor::execute(
            self,
            akzio_runtime::NodeContext {
                task,
                started_at: Utc::now(),
            },
        )
        .await
    }

    pub(crate) async fn execute_task_at(
        &self,
        task: ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> TaskCompletion {
        let Some(logger) = &self.run_log else {
            return self.execute_task_at_logged(task, now).await;
        };
        let phase = match self.log_phase_for(&task) {
            Ok(phase) => phase,
            Err(error) => {
                tracing::error!(%error, "cannot determine Run audit phase");
                return TaskCompletion::DeferredUntil(Utc::now() + Duration::minutes(1));
            }
        };
        let context = akzio_model::ModelAuditContext {
            run_id: task.run_id.0.clone(),
            phase,
            task_id: Some(task.node.task_id.0.clone()),
            attempt_id: Some(task.permit.attempt_id.0.clone()),
            call_id: None,
            retry_index: None,
        };
        let recipe = task.node.recipe_id.as_str().to_owned();
        akzio_model::MODEL_AUDIT_CONTEXT
            .scope(
                context.clone(),
                akzio_model::RUN_AUDIT_SINK.scope(
                    logger.clone() as Arc<dyn akzio_model::ModelAuditSink>,
                    async {
                        let log_result = logger.assert_available().and_then(|()| {
                            logger.record(
                                &context,
                                "rust.task.claimed",
                                serde_json::json!({"recipe": recipe, "task": task.node.task_id}),
                            )
                        });
                        if let Err(error) = log_result {
                            if self
                                .workflow
                                .recipe(&task.node.recipe_id)
                                .is_ok_and(|recipe| {
                                    recipe.task_class == RuntimeTaskClass::Reconcile
                                })
                            {
                                return match self
                                    .paper_execution()
                                    .reconcile_existing_only(&task, now)
                                    .await
                                {
                                    Ok(completion) => completion,
                                    Err(safety_error) => {
                                        tracing::warn!(
                                            %error,
                                            %safety_error,
                                            "read-only Paper safety reconciliation deferred"
                                        );
                                        TaskCompletion::DeferredUntil(
                                            Utc::now() + Duration::minutes(1),
                                        )
                                    }
                                };
                            }
                            return TaskCompletion::DeferredUntil(
                                Utc::now() + Duration::minutes(1),
                            );
                        }
                        let result = self.execute_task_at_logged(task, now).await;
                        if let Err(error) = logger.record(
                            &context,
                            "rust.task.result",
                            serde_json::json!({"outcome": format!("{result:?}")}),
                        ) {
                            tracing::error!(%error, "Run audit failed after task handler");
                            // A fenced handler may already have committed its success.
                            // Everything else must remain pending, not enter a retry
                            // budget or publish a new successful Attempt without its
                            // corresponding private audit record.
                            if !matches!(result, TaskCompletion::Committed) {
                                return TaskCompletion::DeferredUntil(
                                    Utc::now() + Duration::minutes(1),
                                );
                            }
                        }
                        result
                    },
                ),
            )
            .await
    }

    fn log_phase_for(&self, task: &ClaimedAttempt) -> Result<String> {
        if task.node.recipe_id.as_str() != akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID {
            return Ok("T0".into());
        }
        let schedule_ref = task
            .node
            .input_artifacts
            .iter()
            .find(|reference| reference.kind == ArtifactKind::OutcomeSchedule);
        let Some(schedule_ref) = schedule_ref else {
            return Ok("T1".into());
        };
        let schedule: OutcomeSchedule = self.read_artifact_payload(schedule_ref)?;
        for (horizon, phase) in [
            (OutcomeHorizon::T1, "T1"),
            (OutcomeHorizon::T3, "T3"),
            (OutcomeHorizon::T5, "T5"),
        ] {
            if self
                .store
                .retrospective_for(&task.run_id, &schedule.outcome_id, horizon)?
                .is_none()
            {
                return Ok(phase.into());
            }
        }
        Ok("T5".into())
    }

    async fn execute_task_at_logged(
        &self,
        task: ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> TaskCompletion {
        // Record the typed Rust failure before deciding whether it is a
        // deferral, a business retry, or a failed task. Process stderr is not
        // a substitute for the private Run ZIP.
        match self.execute_task_inner(&task, now).await {
            Ok(completion) => completion,
            Err(error) => {
                use akzio_ingest::runtime::EvidenceAdapterError;
                let error_source = match &error {
                    DaemonError::RunLogUnavailable(_) => "run_log",
                    DaemonError::Store(_) => "store",
                    DaemonError::Research(_) => "research",
                    DaemonError::Evidence(_) => "evidence",
                    DaemonError::PaperDispatch(_) => "paper_dispatch",
                    DaemonError::ExecutionGate(_) => "execution_gate",
                    DaemonError::DecisionGate(_) => "decision_gate",
                    DaemonError::PaperCommitment(_) => "paper_commitment",
                    DaemonError::OutcomeSchedule(_) => "outcome_schedule",
                    DaemonError::Model(_) => "model",
                    _ => "daemon",
                };
                if akzio_model::audit_rust_operation(
                    "rust.task.error",
                    serde_json::json!({
                        "recipe":task.node.recipe_id.as_str(),
                        "source":error_source,
                        "message":error.to_string()
                    }),
                )
                .is_err()
                {
                    return TaskCompletion::DeferredUntil(Utc::now() + Duration::minutes(1));
                }
                if matches!(
                    &error,
                    DaemonError::PaperDispatch(PaperDispatchError::Broker(
                        akzio_execution::paper::PaperError::RunCancelled
                    ))
                ) {
                    return if self
                        .store
                        .paper_effect_intent_for_run(&task.run_id)
                        .is_ok_and(|recorded| !recorded)
                    {
                        TaskCompletion::Cancelled
                    } else {
                        TaskCompletion::DeferredUntil(Utc::now() + Duration::minutes(1))
                    };
                }
                if matches!(
                    &error,
                    DaemonError::RunLogUnavailable(_)
                        | DaemonError::Research(ResearchError::RunLogUnavailable(_))
                        | DaemonError::PaperDispatch(PaperDispatchError::Broker(
                            akzio_execution::paper::PaperError::RunLogUnavailable(_)
                        ))
                        | DaemonError::Evidence(EvidenceRuntimeError::Adapter(
                            EvidenceAdapterError::RunLogUnavailable(_)
                        ))
                ) {
                    return TaskCompletion::DeferredUntil(Utc::now() + Duration::minutes(1));
                }
                if self
                    .run_log
                    .as_ref()
                    .is_some_and(|logger| logger.assert_available().is_err())
                {
                    return TaskCompletion::DeferredUntil(Utc::now() + Duration::minutes(1));
                }
                if let DaemonError::Evidence(EvidenceRuntimeError::Adapter(adapter)) = &error {
                    match adapter {
                        EvidenceAdapterError::Pending(_) => {
                            return TaskCompletion::DeferredUntil(
                                Utc::now() + Duration::minutes(20),
                            );
                        }
                        EvidenceAdapterError::RateLimited { retry_after_secs } => {
                            return TaskCompletion::RetryAfter(
                                RetryCause::RateLimited,
                                Utc::now()
                                    + Duration::seconds((*retry_after_secs).min(86_400) as i64),
                            );
                        }
                        _ => {}
                    }
                }
                eprintln!(
                    "daemon task failed closed run_id={} task_id={} recipe={} error={error}",
                    task.run_id, task.node.task_id, task.node.recipe_id
                );
                tracing::warn!(
                    run_id = %task.run_id,
                    task_id = %task.node.task_id,
                    recipe = %task.node.recipe_id,
                    error = %error,
                    "daemon task failed closed"
                );
                retry_cause_for_daemon_error(&error)
                    .map_or(TaskCompletion::Failed, TaskCompletion::Retry)
            }
        }
    }

    pub(super) async fn execute_task_inner(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        // Outcome recipe 先走独立两阶段 worker；其余任务按冻结 recipe 的 task_class 穷举路由，
        // 任何未接入的类都由 recipe/runtime 错误阻断，不回退到 Planner 或默认处理器。
        if task.node.recipe_id.as_str() == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID {
            return self.execute_outcome_worker(task, now).await;
        }
        let recipe = self.workflow.recipe(&task.node.recipe_id)?;
        match recipe.task_class {
            RuntimeTaskClass::Agent => self.research_run().execute(task, now).await,
            RuntimeTaskClass::ResearchControl => self.shared_research_supplement(task, now).await,
            RuntimeTaskClass::Evidence => self.evidence_acquisition().execute(task, now).await,
            RuntimeTaskClass::DecisionGate => self.paper_execution().decision_gate(task, now),
            RuntimeTaskClass::ExecutionGate => {
                self.paper_execution().execution_gate(task, now).await
            }
            RuntimeTaskClass::PaperCommit => self.paper_execution().commit(task, now),
            RuntimeTaskClass::Reconcile => {
                if self.store.run_cancel_requested(&task.run_id)? {
                    if self.store.paper_effect_intent_for_run(&task.run_id)? {
                        self.paper_execution()
                            .reconcile_existing_only(task, now)
                            .await
                    } else {
                        Ok(TaskCompletion::Cancelled)
                    }
                } else {
                    self.paper_execution().reconcile(task, now).await
                }
            }
            RuntimeTaskClass::Evaluate => self.outcome_sealing().execute(task, now).await,
        }
    }

    /// 从成功依赖的正式输出选取唯一类型化终态输入，供 Gate/Outcome 使用。
    /// Agent 的 ContextManifest/ReadGrant 由独立的 AgentRuntime 路径创建；
    /// 此方法不建立模型可读授权。
    pub(super) fn terminal_input(
        &self,
        task: &ClaimedAttempt,
        kind: ArtifactKind,
    ) -> Result<ArtifactRef> {
        let mut matching = self
            .ancestor_outputs(task)?
            .into_iter()
            .filter(|artifact| artifact.kind == kind)
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: artifact.kind,
            })
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
        matching.dedup_by(|left, right| left.artifact_id == right.artifact_id);
        match matching.as_slice() {
            [reference] => Ok(reference.clone()),
            [] => Err(DaemonError::InvalidInput(format!(
                "terminal task {} has no {:?} input",
                task.node.task_id, kind
            ))),
            _ => Err(DaemonError::InvalidInput(format!(
                "terminal task {} has ambiguous {:?} inputs",
                task.node.task_id, kind
            ))),
        }
    }

    /// 从已完成依赖读取受管执行快照，供没有生产 Alpaca 即时刷新路径时使用。
    /// 正式 Paper 的 ExecutionGate 会另行刷新；这里只接受明确的 producer/来源，
    /// 不把任意 NormalizedEvidence 重新解释为 Broker 状态。
    pub(super) fn execution_snapshot_inputs(
        &self,
        task: &ClaimedAttempt,
    ) -> Result<(
        Option<ArtifactRef>,
        Option<ArtifactRef>,
        Option<ArtifactRef>,
    )> {
        // 只从已成功依赖的 Artifact 输出中识别指定 producer；遇到来源/Run/kind/引用不符
        // 会立即失败，避免把研究证据重解释成 Broker 快照。
        let mut account = None;
        let mut quotes = None;
        let mut clock = None;

        for artifact in self.ancestor_outputs(task)? {
            let target = match artifact.producer.as_str() {
                "execution.snapshot.account" => &mut account,
                "execution.snapshot.quotes" => &mut quotes,
                "execution.snapshot.clock" => &mut clock,
                _ => continue,
            };
            if artifact.kind != ArtifactKind::NormalizedEvidence
                || artifact.lifecycle != ArtifactLifecycle::Canonical
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&task.run_id)
                || !artifact
                    .source_refs
                    .iter()
                    .any(|source| source.kind == ArtifactKind::RawEvidence)
                || !artifact
                    .source_refs
                    .iter()
                    .any(|source| source.kind == ArtifactKind::NormalizedEvidence)
            {
                return Err(DaemonError::InvalidInput(format!(
                    "execution snapshot {} has invalid provenance",
                    artifact.artifact_id
                )));
            }
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            };
            if target.replace(reference).is_some() {
                return Err(DaemonError::InvalidInput(
                    "execution gate received duplicate governed snapshot".to_owned(),
                ));
            }
        }

        Ok((account, quotes, clock))
    }

    pub(super) fn ancestor_outputs(&self, task: &ClaimedAttempt) -> Result<Vec<Artifact>> {
        // 从当前 workflow snapshot 沿依赖闭包向上遍历；Skipped 节点只递归展开其依赖，
        // Succeeded 节点才读取 Store 的已提交输出，其他状态拒绝终端任务继续。
        let snapshot = self.workflow.recover(&task.run_id)?;
        let tasks = snapshot
            .tasks
            .into_iter()
            .map(|stored| (stored.node.task_id.clone(), stored))
            .collect::<BTreeMap<_, _>>();
        let mut pending = task.node.dependencies.clone();
        let mut visited = BTreeSet::new();
        let mut outputs = BTreeMap::<ArtifactId, Artifact>::new();
        while let Some(task_id) = pending.pop() {
            if !visited.insert(task_id.clone()) {
                continue;
            }
            let dependency = tasks.get(&task_id).ok_or_else(|| {
                DaemonError::InvalidInput(format!(
                    "terminal task {} references unknown dependency {task_id}",
                    task.node.task_id
                ))
            })?;
            if !matches!(
                dependency.status,
                TaskStatus::Succeeded | TaskStatus::Skipped
            ) {
                return Err(DaemonError::UnfinishedDependency {
                    task_id: task.node.task_id.clone(),
                    dependency: task_id,
                });
            }
            if dependency.status == TaskStatus::Skipped {
                pending.extend(dependency.node.dependencies.iter().cloned());
                continue;
            }
            for artifact in self
                .store
                .succeeded_task_outputs_or_empty(&task.run_id, &dependency.node.task_id)?
            {
                outputs.insert(artifact.artifact_id.clone(), artifact);
            }
            pending.extend(dependency.node.dependencies.iter().cloned());
        }
        Ok(outputs.into_values().collect())
    }

    pub(super) fn read_artifact_payload<T: serde::de::DeserializeOwned>(
        &self,
        reference: &ArtifactRef,
    ) -> Result<T> {
        // 先把 ArtifactRef 声明的 kind 与 Store 当前索引核对，再读 CAS BLOB 并反序列化为 T；
        // `DeserializeOwned` 要求结果不借用 BLOB 缓冲，便于后续跨 await 使用。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if artifact.kind != reference.kind {
            return Err(DaemonError::InvalidInput(format!(
                "artifact {} kind changed from {:?} to {:?}",
                reference.artifact_id, reference.kind, artifact.kind
            )));
        }
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }
}

impl akzio_runtime::NodeExecutor for Daemon {
    // NodeExecutor 要求返回可跨线程运行的 boxed Future；`context.task` move 入 async 块，
    // Future 真正被 executor poll 后才进入 task handler。
    fn execute(
        &self,
        context: akzio_runtime::NodeContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = akzio_runtime::NodeOutcome> + Send + '_>>
    {
        Box::pin(async move { self.execute_task_at(context.task, context.started_at).await })
    }
}
