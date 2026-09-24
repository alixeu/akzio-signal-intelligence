use super::*;

// WorkflowRuntime 的写入口把已编译 graph/proposal 与 Session reservation 放进 Store
// 事务边界。重复 session 返回原有 Run/Task identity；配置重载、scheduler restart 或
// 重新调用函数都不能生成替代 graph。approved proposal 是研究拓扑，不是模型已完成的结论。
impl WorkflowRuntime {
    pub fn submit(
        &self,
        run_id: RunId,
        purpose: RunPurpose,
        graph: WorkflowGraph,
        now: DateTime<Utc>,
    ) -> RuntimeResult<Artifact> {
        // prepare 先做 graph/compiled invariants，commit 才让 Run/Task 出现在 durable
        // journal；返回 graph Artifact 仍只说明提交成功，不代表节点已领取或执行。
        let commit = self.prepare_workflow_commit(run_id, purpose, graph, now)?;
        let graph_artifact = commit.graph.clone();
        self.store.commit_workflow(&commit)?;
        Ok(graph_artifact)
    }

    pub fn prepare_workflow_commit(
        &self,
        run_id: RunId,
        purpose: RunPurpose,
        graph: WorkflowGraph,
        now: DateTime<Utc>,
    ) -> RuntimeResult<WorkflowCommit> {
        // 预备阶段不写 Store，只使用当前实例的 staging connection 生成不可变 graph；
        // 调用方可以把它和 Paper approval/evidence setup 组合进同一事务。
        graph.validate()?;
        self.validate_compiled_graph(purpose, &graph)?;
        let graph_artifact = self.graph_artifact(&graph, vec![], now)?;
        Ok(WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        })
    }

    /// Load the exact durable graph/task state for crash recovery. Recovery
    /// never re-lowers a proposal or allocates replacement task IDs.
    /// Freeze one fully compiled Paper workflow into its broker-session slot.
    /// A duplicate session returns the already durable graph and task IDs; it
    /// never regenerates a replacement graph after a scheduler restart.
    pub fn reserve_paper_session(
        &self,
        lease: &DaemonLease,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        self.reserve_paper_session_with_inputs(lease, session_key, proposal, &[], now)
    }

    /// As [`Self::reserve_paper_session`], but atomically installs the
    /// scheduler-owned immutable `EvidenceNeed` artifacts referenced by the
    /// compiled graph.
    pub fn reserve_paper_session_with_inputs(
        &self,
        lease: &DaemonLease,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        self.reserve_paper_session_with_inputs_for_run(
            lease,
            RunId::new(),
            session_key,
            proposal,
            setup_artifacts,
            now,
        )
    }

    /// Reserve the exact caller-allocated run identity. This exists for the
    /// scheduler's preflight transaction, which binds immutable evidence need
    /// artifacts to the same Run before it becomes visible.
    pub fn reserve_paper_session_with_inputs_for_run(
        &self,
        lease: &DaemonLease,
        run_id: RunId,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        self.reserve_paper_session_with_inputs_for_run_binding(
            lease,
            run_id,
            session_key,
            proposal,
            setup_artifacts,
            None,
            now,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reserve_paper_session_with_inputs_for_run_approved(
        &self,
        lease: &DaemonLease,
        run_id: RunId,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        runtime_manifest: &Artifact,
        approval: &Artifact,
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        self.reserve_paper_session_with_inputs_for_run_binding(
            lease,
            run_id,
            session_key,
            proposal,
            setup_artifacts,
            Some((runtime_manifest, approval)),
            now,
        )
    }

    /// Prepare an approved Paper session without publishing it. The caller
    /// may combine the returned immutable reservation with other workflow
    /// commits in one Store transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_approved_paper_session_with_inputs_for_run(
        &self,
        run_id: RunId,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> RuntimeResult<(SessionReservation, Artifact)> {
        let session_key = session_key.into();
        let proposal_artifact = self.paper_proposal_artifact(&run_id, proposal, now)?;
        let workflow =
            self.prepare_paper_workflow_commit(run_id, proposal, Some(&proposal_artifact), now)?;
        Ok((
            SessionReservation {
                session_key,
                workflow,
                setup_artifacts: setup_artifacts.to_vec(),
                reserved_at: now,
            },
            proposal_artifact,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn reserve_paper_session_with_inputs_for_run_binding(
        &self,
        lease: &DaemonLease,
        run_id: RunId,
        session_key: impl Into<String>,
        proposal: &WorkflowProposal,
        setup_artifacts: &[Artifact],
        binding: Option<(&Artifact, &Artifact)>,
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        // 先查 session slot，重复 scheduler tick 直接返回既有 reservation；新 session
        // 才编译一次 Paper graph，并在有 approval binding 时由 Store 原子消费审批。
        let session_key = session_key.into();
        if let Some(slot) = self.store.session_slot(&session_key)? {
            return Ok(SessionSlotReservation {
                slot,
                newly_reserved: false,
            });
        }

        let proposal_artifact = if binding.is_some() {
            Some(self.paper_proposal_artifact(&run_id, proposal, now)?)
        } else {
            None
        };
        let workflow =
            self.prepare_paper_workflow_commit(run_id, proposal, proposal_artifact.as_ref(), now)?;
        let reservation = SessionReservation {
            session_key,
            workflow,
            setup_artifacts: setup_artifacts.to_vec(),
            reserved_at: now,
        };
        Ok(match (binding, proposal_artifact.as_ref()) {
            (Some((runtime_manifest, approval)), Some(proposal_artifact)) => {
                self.store.reserve_paper_session_with_approval(
                    lease,
                    &reservation,
                    proposal_artifact,
                    runtime_manifest,
                    approval,
                )?
            }
            _ => self.store.reserve_session_slot(lease, &reservation)?,
        })
    }

    /// Build the Rust-owned, precompiled Paper proposal used for the first
    /// scheduler session. It contains no model output and cannot be patched
    /// after the Paper graph is frozen.
    pub fn approved_paper_proposal(
        &self,
        topology_id: impl Into<String>,
    ) -> RuntimeResult<WorkflowProposal> {
        self.approved_research_proposal(topology_id)
    }

    /// Shared Rust-owned T1/T3/T5 research. Purpose selects the terminal chain in lower().
    pub fn approved_research_proposal(
        &self,
        topology_id: impl Into<String>,
    ) -> RuntimeResult<WorkflowProposal> {
        Ok(self.research_definition(topology_id)?.proposal)
    }

    pub fn research_definition(
        &self,
        topology_id: impl Into<String>,
    ) -> RuntimeResult<akzio_domain::WorkflowDefinition> {
        // 固定生成 T1/T3/T5 Analyst/Critic、一次共享 supplement、refined pairs 和
        // 有界 Synthesizer/ProposalReviewer revisions；Outcome worker 不在这条 T0 图里。
        let topology_id = topology_id.into();
        let analyst = self
            .catalogue
            .recipe(&TaskRecipeId::new(ANALYST_RECIPE_ID)?)?;
        let critic = self
            .catalogue
            .recipe(&TaskRecipeId::new(CRITIC_RECIPE_ID)?)?;
        let synthesizer = self
            .catalogue
            .recipe(&TaskRecipeId::new(SYNTHESIZER_RECIPE_ID)?)?;
        self.research_settings.validate()?;
        let reviewer = self.catalogue.recipe(&TaskRecipeId::new(
            akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
        )?)?;
        let supplement = self.catalogue.recipe(&TaskRecipeId::new(
            akzio_domain::RESEARCH_SUPPLEMENT_RECIPE_ID,
        )?)?;
        let mut tasks = BTreeMap::new();
        let mut initial = Vec::new();
        let mut effective = Vec::new();
        let mut insert = |alias: String,
                          recipe: &TaskRecipe,
                          objective: String,
                          depends_on: Vec<String>,
                          horizon,
                          research_round,
                          proposal_revision| {
            tasks.insert(
                alias.clone(),
                akzio_domain::WorkflowProposalTask {
                    spec: Some(akzio_domain::NodeSpec {
                        key: alias,
                        horizon,
                        research_round,
                        proposal_revision,
                    }),
                    recipe_id: recipe.recipe_id.clone(),
                    objective,
                    depends_on,
                    priority: recipe.priority_ceiling,
                    evidence_needs: vec![],
                },
            );
        };
        for (horizon, scope) in [
            ("t1", akzio_domain::DecisionHorizon::T1),
            ("t3", akzio_domain::DecisionHorizon::T3),
            ("t5", akzio_domain::DecisionHorizon::T5),
        ] {
            // 每个 horizon 的 Critic 只审对应 Claim；依赖顺序表达 Rust-owned provenance，
            // 不是让模型动态创建新 Agent。
            let a = format!("analyst_{horizon}");
            let c = format!("critic_{horizon}");
            insert(
                a.clone(),
                analyst,
                "Assess the four assets using governed evidence. Preserve scoped gaps.".into(),
                vec![],
                Some(scope),
                Some(0),
                None,
            );
            insert(c.clone(), critic, "Independently review the Claim and request governed supplementation for retriable material blockers.".into(), vec![a.clone()], Some(scope), Some(0), None);
            initial.extend([a, c]);
        }
        insert(
            "supplement".into(),
            supplement,
            "Rust-owned single shared supplemental round; at most eight deduplicated resources"
                .into(),
            initial.clone(),
            None,
            None,
            None,
        );
        effective.extend(initial);
        for (horizon, scope) in [
            ("t1", akzio_domain::DecisionHorizon::T1),
            ("t3", akzio_domain::DecisionHorizon::T3),
            ("t5", akzio_domain::DecisionHorizon::T5),
        ] {
            let a = format!("analyst_{horizon}_refined");
            let c = format!("critic_{horizon}_refined");
            insert(a.clone(), analyst, "Revise only if Rust reports new admissible evidence. Read the supplemental dispositions; keep unresolved gaps.".into(), vec!["supplement".into()], Some(scope), Some(1), None);
            insert(
                c.clone(),
                critic,
                "Review the revised Claim. The shared supplemental budget has been consumed."
                    .into(),
                vec![a.clone(), "supplement".into()],
                Some(scope),
                Some(1),
                None,
            );
            effective.extend([a, c]);
        }
        let mut previous = None;
        for revision in 0..=self.research_settings.max_proposal_revisions {
            // 每个 revision 都是预先冻结的 Synthesizer→Reviewer 对；Review 通过后由
            // 上层选择有效版本，失败/耗尽不会自动激活 Policy。
            let synth = format!("synthesizer_{revision}");
            let review = format!("proposal_review_{revision}");
            let mut dependencies = effective.clone();
            if let Some(prior) = previous {
                dependencies.push(prior);
            }
            insert(synth.clone(), synthesizer, "Synthesize exactly twelve forecasts and four assets plus cash with numeric_basis for each scope. Address previous review findings if provided. Rust selects the effective horizon versions.".into(), dependencies, None, None, Some(revision));
            insert(review.clone(), reviewer, "Review every forecast and allocation in the exact supplied proposal. Assess evidence, numeric basis, uncertainty, overlap and cash rationale; do not claim empirical calibration.".into(), vec![synth], None, None, Some(revision));
            previous = Some(review);
        }
        let proposal = WorkflowProposal {
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id,
            tasks,
            stop_reason: Some(
                "rust-approved Paper coverage policy v2: three bounded horizon pairs".to_owned(),
            ),
        };
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(&proposal)?;
        Ok(akzio_domain::WorkflowDefinition {
            version: akzio_domain::WORKFLOW_DEFINITION_VERSION,
            proposal,
        })
    }

    fn paper_proposal_artifact(
        &self,
        run_id: &RunId,
        proposal: &WorkflowProposal,
        now: DateTime<Utc>,
    ) -> RuntimeResult<Artifact> {
        // proposal Artifact 仅保存 Rust 编译的研究计划和 EvidenceNeed refs，没有模型
        // 预测或订单；Paper session 仍需后续 Agent/Decision/Execution/Commit Gate。
        Ok(Artifact::new(
            ArtifactKind::WorkflowProposal,
            // The returned reservation is valid only with this Store's staging connection.
            self.store.stage_json(proposal)?,
            "runtime.paper_provisioning",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            Some(ArtifactOrigin {
                run_id: Some(run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            proposal
                .tasks
                .values()
                .flat_map(|task| task.evidence_needs.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            now,
        )?)
    }

    fn prepare_paper_workflow_commit(
        &self,
        run_id: RunId,
        proposal: &WorkflowProposal,
        proposal_artifact: Option<&Artifact>,
        now: DateTime<Utc>,
    ) -> RuntimeResult<WorkflowCommit> {
        // Paper 图复用 approved research proposal，再接正式执行链；PositionPlan 不会
        // 通过这个 helper 获得 PaperCommit/Reconcile/Evaluate 能力。
        let graph = self.lower(RunPurpose::Paper, proposal)?;
        let graph_artifact = self.graph_artifact(
            &graph,
            proposal_artifact
                .into_iter()
                .map(|artifact| ArtifactRef {
                    artifact_id: artifact.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowProposal,
                })
                .collect(),
            now,
        )?;
        Ok(WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        })
    }
}
