//! Thin application boundary over the Store execution policy.
use super::*;
use akzio_domain::{
    DebugBrokerPolicy, DebugControlRequest, DebugLearningScope, DebugLlmMode, DebugSession,
    DebugSessionIdentity,
};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DebugPrepareRequest {
    pub session_key: String,
    #[serde(default = "default_debug_purpose")]
    pub purpose: RunPurpose,
    #[serde(default)]
    pub paper_allowed: bool,
}

fn default_debug_purpose() -> RunPurpose {
    // 请求省略 purpose 时默认构造 Paper 研究图；该 serde 默认值不等于已获 Paper approval。
    RunPurpose::Paper
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DebugForkRequest {
    pub task_id: Option<TaskId>,
    pub experiment_id: RunId,
    pub reason: String,
}

impl Daemon {
    // 仅以 DebugControl 配置是否存在判断控制面是否启用，不代表当前 Run 已准备或可执行。
    pub fn debug_enabled(&self) -> bool {
        self.debug_control.is_some()
    }

    // 校验 Debug prepare 的 purpose、session 和 DecisionPolicy 条件，构造/保留隔离实验；
    // 返回 DebugSession 只表示图、身份和预约已持久化，尚未运行研究、Decision 或 Broker。
    pub fn prepare_debug(&self, request: &DebugPrepareRequest) -> Result<DebugSession> {
        let config = self
            .debug_control
            .as_ref()
            .ok_or_else(|| DaemonError::InvalidInput("debug_control_disabled".into()))?;
        if !matches!(
            request.purpose,
            RunPurpose::Paper | RunPurpose::PositionPlan
        ) || (request.purpose == RunPurpose::PositionPlan && request.paper_allowed)
        {
            // Debug 只允许 Paper 或不执行的 PositionPlan；PositionPlan 不能携带 paper_allowed。
            return Err(DaemonError::InvalidInput(
                "debug prepare requires Paper or non-executing PositionPlan".into(),
            ));
        }
        if request.purpose == RunPurpose::PositionPlan
            && !self.fixture_mode
            && !(config.decision_policy_status == "store_active_head_missing"
                && config.decision_policy_input_hash.is_none()
                && config.decision_policy_artifact.is_none())
            && (config.decision_policy_status != "ready_for_current_decision"
                || config.decision_policy_input_hash.is_none()
                || config.decision_policy_artifact.is_none())
        {
            // PositionPlan 要么使用 ready policy，要么显式标记 Store 尚未有 active head；
            // fixture 的例外只影响隔离验证，不把 policy 缺失变成 Decision/交易授权。
            return Err(DaemonError::InvalidInput(
                "PositionPlan requires a ready policy or an explicitly uncalibrated Store".into(),
            ));
        }
        NaiveDate::parse_from_str(&request.session_key, "%Y-%m-%d")
            .map_err(|_| DaemonError::InvalidInput("session_key must be YYYY-MM-DD".into()))?;
        if request.purpose == RunPurpose::PositionPlan {
            // PositionPlan 不占 Paper session slot；prepare_position_plan 返回冻结 setup，
            // 随后以隔离 identity 一次性提交实验图，Decision 之后没有 Execution 链。
            let (workflow, setup) = self.prepare_position_plan(&request.session_key, Utc::now())?;
            let dataset = setup
                .iter()
                .map(|a| ArtifactRef {
                    artifact_id: a.artifact_id.clone(),
                    kind: a.kind,
                })
                .collect();
            let identity = self.debug_identity(&workflow, dataset, false)?;
            return Ok(self
                .store
                .commit_debug_experiment(&workflow, &setup, &identity)?);
        }
        if let Some(slot) = self.store.session_slot(&request.session_key)? {
            // 同一 session 已有实验时只允许返回身份完全兼容的现有 Session；不静默复用
            // 不同 runtime 或 broker policy 的预约。
            let session = self
                .store
                .debug_session(&slot.workflow.run.run_id)?
                .ok_or_else(|| DaemonError::InvalidInput("session_is_not_debug".into()))?;
            if session.identity.runtime_identity != config.runtime_identity
                || (session.identity.broker_write_policy == DebugBrokerPolicy::PaperAllowed)
                    != request.paper_allowed
            {
                return Err(DaemonError::InvalidInput(
                    "existing_session_identity_differs; use a new experiment".into(),
                ));
            }
            return Ok(session);
        }
        // 新 Paper Debug 实验先创建 scheduler snapshot 和 approved proposal，再把同一
        // evidence dataset 绑定到 Analyst 节点；后续 worker 才按 permit 执行。
        let now = Utc::now();
        let run_id = RunId::new();
        let setup =
            self.paper
                .scheduler
                .paper_snapshot_artifacts(&run_id, &request.session_key, now)?;
        let mut proposal = self.workflow.approved_paper_proposal("paper.approved.v1")?;
        let dataset = setup
            .iter()
            .map(|a| ArtifactRef {
                artifact_id: a.artifact_id.clone(),
                kind: a.kind,
            })
            .collect::<Vec<_>>();
        for task in proposal
            .tasks
            .values_mut()
            .filter(|t| t.recipe_id.as_str() == akzio_domain::RESEARCH_ANALYST_RECIPE_ID)
        {
            task.evidence_needs = dataset.clone();
        }
        // prepare_approved... 只建立冻结 WorkflowCommit/Session reservation；lease 和
        // approval binding 进入 Store 身份，不能据此声称 Paper 订单已获准或已提交。
        let (reservation, proposal) = self
            .workflow
            .prepare_approved_paper_session_with_inputs_for_run(
                run_id.clone(),
                &request.session_key,
                &proposal,
                &setup,
                now,
            )?;
        let identity =
            self.debug_identity(&reservation.workflow, dataset, request.paper_allowed)?;
        let lease = self.paper.scheduler.active_lease(now)?;
        let binding = self.paper.scheduler.current_approval_binding()?;
        Ok(self.store.reserve_debug_session(
            &lease,
            &reservation,
            &proposal,
            &identity,
            binding.as_ref().map(|(m, a)| (m, a)),
        )?)
    }

    // 从隔离 Store、Workflow 和配置快照生成不可变 DebugSessionIdentity；其中 broker policy
    // 和 learning scope 只是实验身份约束，paper_allowed 不会绕过 PaperLaunchApproval。
    fn debug_identity(
        &self,
        workflow: &akzio_store::WorkflowCommit,
        dataset: Vec<ArtifactRef>,
        paper_allowed: bool,
    ) -> Result<DebugSessionIdentity> {
        let config = self
            .debug_control
            .as_ref()
            .ok_or_else(|| DaemonError::InvalidInput("debug_control_disabled".into()))?;
        Ok(DebugSessionIdentity {
            version: 1,
            debug_session_id: format!("debug-{}", workflow.run.run_id),
            store_identity: self
                .store
                .debug_environment()?
                .ok_or_else(|| DaemonError::InvalidInput("isolated_store_required".into()))?,
            run_id: workflow.run.run_id.clone(),
            run_purpose: workflow.run.purpose,
            llm_mode: if self.fixture_mode {
                DebugLlmMode::Fixture
            } else {
                DebugLlmMode::Real
            },
            broker_write_policy: if paper_allowed {
                DebugBrokerPolicy::PaperAllowed
            } else {
                DebugBrokerPolicy::Forbidden
            },
            learning_scope: DebugLearningScope::Isolated,
            code_revision: config.code_revision.clone(),
            runtime_identity: config.runtime_identity.clone(),
            decision_policy_status: config.decision_policy_status.clone(),
            decision_policy_input_hash: config.decision_policy_input_hash.clone(),
            decision_policy_artifact: config.decision_policy_artifact.clone(),
            contract_hashes: workflow
                .nodes
                .iter()
                .filter_map(|n| n.contract_hash.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            dataset,
            parent_run_id: None,
            parent_task_id: None,
            parent_artifacts: vec![],
            reason: (workflow.run.purpose == RunPurpose::PositionPlan
                && config.decision_policy_status == "store_active_head_missing")
                .then(|| "research_only_incomplete: DecisionPolicy missing; manual research only, Decision forbidden".to_owned()),
            created_at: workflow.run.created_at,
        })
    }

    // 在 Store 的 Debug 控制事务前增加 daemon 侧不可用边界：缺 policy 时阻止继续到
    // Decision，Outcome worker 不可用时阻止单步/重试；其余动作仍由 Store 决定并持久化。
    pub fn control_debug(
        &self,
        run_id: &RunId,
        request: &DebugControlRequest,
    ) -> Result<DebugSession> {
        let config = self
            .debug_control
            .as_ref()
            .ok_or_else(|| DaemonError::InvalidInput("debug_control_disabled".into()))?;
        if !self.fixture_mode
            && self
                .store
                .debug_session(run_id)?
                .is_some_and(|session| session.identity.research_only_without_policy())
        {
            // research_only_without_policy 只允许手动 Evidence/Analyst/Critic/Synthesizer；
            // Resume 或直接 step 到 gate.decision 都不能把研究提案提升为 Decision。
            let starts_decision = request.action == akzio_domain::DebugAction::Resume
                || self
                    .store
                    .workflow_snapshot(run_id)?
                    .tasks
                    .iter()
                    .any(|task| {
                        Some(&task.node.task_id) == request.task_id.as_ref()
                            && task.node.recipe_id.as_str() == "gate.decision"
                    });
            if starts_decision {
                return Err(DaemonError::InvalidInput("research_only_incomplete: missing DecisionPolicy; only manual Evidence/Analyst/Critic/Synthesizer steps are allowed".into()));
            }
        }
        if matches!(
            request.action,
            akzio_domain::DebugAction::Step | akzio_domain::DebugAction::RetryNode
        ) && !self.outcome_processing
            && self.store.workflow_snapshot(run_id)?.tasks.iter().any(|t| {
                Some(&t.node.task_id) == request.task_id.as_ref()
                    && t.node.recipe_id.as_str() == "learning.outcome_worker"
            })
        {
            // Outcome worker 关闭时不接受会推进该节点的控制动作，避免产生看似完成但没有
            // Outcome 处理能力的终态；普通 inspection/pause/abort 仍可继续。
            return Err(DaemonError::Unavailable(
                "outcome_processing_disabled_or_adapter_unavailable".into(),
            ));
        }
        Ok(self
            .store
            .debug_control(run_id, request, &config.runtime_identity, Utc::now())?)
    }

    // 读取 Store 的 DebugRunView 后按当前 runtime identity、Outcome 能力和生命周期状态
    // 修正可用动作/节点原因；这是观察投影，不会推进任务或改变原始 workflow 状态。
    pub fn inspect_debug(
        &self,
        run_id: &RunId,
        task_id: Option<&TaskId>,
        attempt_id: Option<&akzio_domain::AttemptId>,
    ) -> Result<akzio_store::DebugRunView> {
        let mut view = self
            .store
            .debug_inspect(run_id, task_id, attempt_id, Utc::now())?;
        let identity_matches = self
            .debug_control
            .as_ref()
            .is_some_and(|c| c.runtime_identity == view.session.identity.runtime_identity);
        if !identity_matches {
            // runtime identity 变化后只保留 pause/abort，禁止在旧身份上继续执行或重试。
            view.allowed_actions
                .retain(|a| matches!(a.as_str(), "pause" | "abort"));
        }
        view.inspection.allowed_actions = view.allowed_actions.clone();
        let lifecycle = self.store.run_lifecycle_health(run_id)?;
        for node in &mut view.nodes {
            let outcome = node.role == "learning.outcome_worker";
            if outcome {
                // Outcome 节点的 horizon 来自 Store lifecycle pending 状态，而不是节点名
                // 或自然日期推断；没有 pending 阶段就保持 None。
                node.horizon = ["t1", "t3", "t5"]
                    .into_iter()
                    .find(|h| {
                        lifecycle
                            .retrospective_status
                            .get(*h)
                            .is_some_and(|s| s == "pending")
                    })
                    .map(str::to_owned);
            }
            let reason = if node.blocked_reason.as_deref() == Some("legacy_workflow_retired") {
                Some("legacy_workflow_retired")
            } else if !identity_matches {
                Some("runtime_identity_changed: create a new experiment")
            } else if outcome && !self.outcome_processing {
                Some("outcome_processing_disabled_or_adapter_unavailable")
            } else {
                None
            };
            if let Some(reason) = reason {
                // 每个阻断原因同时关闭 step/retry，避免 UI 显示可操作但后端必然拒绝。
                node.step_eligible = false;
                node.retry_eligible = false;
                node.blocked_reason = Some(reason.into());
            }
        }
        Ok(view)
    }

    // 从一个可执行的非 Paper Debug Run 创建新的隔离实验：只复制 EvidenceNeed 的 setup
    // 作为新 Run 的采集入口，保留可选父 Attempt 作为 lineage，不复制成功状态或读取授权。
    pub fn fork_debug(&self, run_id: &RunId, request: &DebugForkRequest) -> Result<DebugSession> {
        self.store.assert_workflow_executable(run_id)?;
        if !self.debug_enabled() || request.reason.trim().is_empty() {
            // fork 需要启用控制面和非空实验理由，理由会进入新 identity 供审计。
            return Err(DaemonError::InvalidInput(
                "debug enabled and experiment reason required".into(),
            ));
        }
        let reason = request.reason.clone();
        if let Some(existing) = self.store.debug_session(&request.experiment_id)? {
            // 相同 parent/task/reason 的重复请求可幂等返回；同一 experiment_id 的其他
            // 组合视为冲突，不能覆盖既有实验。
            if existing.identity.parent_run_id.as_ref() == Some(run_id)
                && existing.identity.parent_task_id.as_ref() == request.task_id.as_ref()
                && existing.identity.reason.as_ref() == Some(&reason)
            {
                return Ok(existing);
            }
            return Err(DaemonError::InvalidInput("experiment_id_conflict".into()));
        }
        let source = self
            .store
            .debug_session(run_id)?
            .ok_or_else(|| DaemonError::InvalidInput("parent_is_not_debug".into()))?;
        let purpose = source.identity.run_purpose;
        if purpose == RunPurpose::Paper {
            // Paper Run 的 Session/approval 绑定不能通过 fork 降级成 Debug；需要新的
            // prepare 流程按 Paper 规则重新预约。
            return Err(DaemonError::InvalidInput("Paper experiment requires fresh prepare with a reserved Session; purpose cannot be downgraded to Debug".into()));
        }
        // proof 只作为父成功 Attempt 的只读 lineage；没有 task_id 时以父 WorkflowGraph
        // 作为来源证明，二者都不替代新 Run 的证据采集。
        let proof = request
            .task_id
            .as_ref()
            .map(|task| self.store.current_succeeded_attempt(run_id, task))
            .transpose()?;
        let now = Utc::now();
        // Fresh experiment deliberately recollects governed evidence. Frozen parent
        // outputs remain lineage, never a forged success or an implicit read grant.
        let mut setup = Vec::new();
        for reference in source.identity.dataset {
            // 新实验只复制 EvidenceNeed 的 payload 并重绑定新 run_id；父产出的 Claim、
            // RawEvidence 或其他结果不作为新 Run 的 setup 输入。
            let parent = self.store.artifact(&reference.artifact_id)?;
            if parent.kind != ArtifactKind::EvidenceNeed {
                continue;
            }
            setup.push(Artifact::new(
                parent.kind,
                parent.blob,
                "scheduler.paper_snapshot",
                ArtifactLifecycle::RunScoped,
                parent.provenance,
                Some(ArtifactOrigin {
                    run_id: Some(request.experiment_id.clone()),
                    task_id: None,
                    attempt_id: None,
                    contract_hash: None,
                }),
                vec![reference],
                now,
            )?);
        }
        let dataset = setup
            .iter()
            .map(|a| ArtifactRef {
                artifact_id: a.artifact_id.clone(),
                kind: a.kind,
            })
            .collect::<Vec<_>>();
        let mut proposal = self.workflow.approved_paper_proposal("paper.approved.v1")?;
        for task in proposal
            .tasks
            .values_mut()
            .filter(|t| t.recipe_id.as_str() == akzio_domain::RESEARCH_ANALYST_RECIPE_ID)
        {
            task.evidence_needs = dataset.clone();
        }
        // 新图重新 lower/prepare，保留 purpose；父输出仅写入 identity.parent_artifacts，
        // 不改变新任务的初始状态或授予隐式 read grant。
        let graph = self.workflow.lower(purpose, &proposal)?;
        let workflow = self.workflow.prepare_workflow_commit(
            request.experiment_id.clone(),
            purpose,
            graph,
            now,
        )?;
        let mut identity = self.debug_identity(&workflow, dataset, false)?;
        identity.parent_run_id = Some(run_id.clone());
        identity.parent_task_id = request.task_id.clone();
        identity.parent_artifacts = if let Some(proof) = proof {
            proof
                .outputs
                .into_iter()
                .filter(|a| a.kind != ArtifactKind::RawEvidence)
                .map(|a| ArtifactRef {
                    artifact_id: a.artifact_id,
                    kind: a.kind,
                })
                .collect()
        } else {
            vec![ArtifactRef {
                artifact_id: self.store.workflow_snapshot(run_id)?.run.graph_artifact_id,
                kind: ArtifactKind::WorkflowGraph,
            }]
        };
        identity.reason = Some(reason);
        // commit_debug_experiment 只持久化新隔离实验及其 setup；实际执行仍需后续控制动作。
        Ok(self
            .store
            .commit_debug_experiment(&workflow, &setup, &identity)?)
    }
}
