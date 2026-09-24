// 编译后校验把结构性事实锁定：每个图必须有唯一 Evidence/Decision 等适用 Gate，
// 研究不能依赖终端，Decision 只能接研究叶子。Shadow 的 candidate 只有在 Store 安装
// 记录满足 baseline/预算/termination 子集时才允许出现。
impl WorkflowRuntime {
    pub(super) fn validate_compiled_graph(
        &self,
        purpose: RunPurpose,
        graph: &WorkflowGraph,
    ) -> RuntimeResult<()> {
        // terminals/research 分离后，逐 node 对 recipe、Contract、预算、retry、failure
        // 和 priority 做精确比较；成功只是图合法，不是任何业务阶段已执行。
        self.validate_evidence_gate(graph)?;
        let mut terminals = BTreeMap::<TaskRecipeId, &WorkflowNode>::new();
        let mut research = Vec::new();
        for node in &graph.nodes {
            let recipe = self.catalogue.recipe(&node.recipe_id)?;
            let candidate_contract = if purpose == RunPurpose::Shadow {
                node.contract_hash
                    .as_ref()
                    .filter(|hash| recipe.contract_hash.as_ref() != Some(*hash))
                    .map(|hash| self.store.contract_installation(hash))
                    .transpose()?
                    .flatten()
                    .filter(|stored| {
                        stored.activated_at.is_none()
                            && stored.baseline_contract_hash.as_ref()
                                == recipe.contract_hash.as_ref()
                            && stored.contract.purpose == recipe.purpose
                            && stored.contract.budget == recipe.budget
                            && stored.contract.retry == recipe.retry
                            && stored.contract.on_failure == recipe.on_failure
                            && stored.contract.termination.max_child_tasks == recipe.max_children
                            && stored.contract.termination.max_depth == recipe.max_depth
                    })
            } else {
                None
            };
            if (node.contract_hash != recipe.contract_hash && candidate_contract.is_none())
                || node.budget != graph.agent_budgets.get(recipe.purpose.as_str()).cloned().unwrap_or_else(|| recipe.budget.clone())
                || node.retry != recipe.retry
                || node.on_failure != recipe.on_failure
                || node.priority > recipe.priority_ceiling
            {
                return Err(RuntimeError::NodeRecipeMismatch(node.task_id.clone()));
            }
            if matches!(
                recipe.task_class,
                RuntimeTaskClass::DecisionGate
                    | RuntimeTaskClass::ExecutionGate
                    | RuntimeTaskClass::PaperCommit
                    | RuntimeTaskClass::Reconcile
                    | RuntimeTaskClass::Evaluate
            ) {
                if !self.catalogue.is_terminal(&node.recipe_id)
                    || terminals.insert(node.recipe_id.clone(), node).is_some()
                {
                    return Err(RuntimeError::UnexpectedTerminalGate(node.recipe_id.clone()));
                }
            } else {
                research.push(node.clone());
            }
        }
        if research.is_empty() {
            return Err(RuntimeError::WorkflowNodeLimit);
        }

        let decision = required_terminal(&terminals, &self.catalogue.terminals.decision_gate)?;
        let paper = terminals
            .get(&self.catalogue.terminals.paper_commit)
            .copied();
        if purpose == RunPurpose::Paper && paper.is_none() {
            return Err(RuntimeError::MissingTerminalGate(
                self.catalogue.terminals.paper_commit.clone(),
            ));
        }
        if purpose != RunPurpose::Paper && paper.is_some() {
            return Err(RuntimeError::UnexpectedTerminalGate(
                self.catalogue.terminals.paper_commit.clone(),
            ));
        }

        let terminal_ids = terminals
            .values()
            .map(|node| node.task_id.clone())
            .collect::<BTreeSet<_>>();
        if research.iter().any(|node| {
            node.dependencies
                .iter()
                .any(|dependency| terminal_ids.contains(dependency))
        }) {
            let offender = research
                .iter()
                .find(|node| {
                    node.dependencies
                        .iter()
                        .any(|dependency| terminal_ids.contains(dependency))
                })
                .expect("research dependency predicate found an offender");
            return Err(RuntimeError::ResearchDependsOnTerminal(
                offender.task_id.clone(),
            ));
        }

        let expected_decision_dependencies =
            leaf_ids(&research).into_iter().collect::<BTreeSet<_>>();
        // Decision 必须等待所有研究叶子（包括 ProposalReview/补采后的有效节点），
        // 不能只依赖 Synthesizer 以外的一个“代表”节点。
        if decision
            .dependencies
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            != expected_decision_dependencies
        {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.decision_gate.clone(),
            ));
        }
        if purpose == RunPurpose::PositionPlan {
            for recipe_id in [
                &self.catalogue.terminals.execution_gate,
                &self.catalogue.terminals.reconcile,
                &self.catalogue.terminals.evaluate,
            ] {
                if terminals.contains_key(recipe_id) {
                    return Err(RuntimeError::UnexpectedTerminalGate(recipe_id.clone()));
                }
            }
            return Ok(());
        }
        let execution = required_terminal(&terminals, &self.catalogue.terminals.execution_gate)?;
        let reconcile = required_terminal(&terminals, &self.catalogue.terminals.reconcile)?;
        let evaluate = required_terminal(&terminals, &self.catalogue.terminals.evaluate)?;
        if execution.dependencies != vec![decision.task_id.clone()] {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.execution_gate.clone(),
            ));
        }
        let predecessor = if let Some(paper) = paper {
            if paper.dependencies != vec![execution.task_id.clone()] {
                return Err(RuntimeError::InvalidTerminalDependencies(
                    self.catalogue.terminals.paper_commit.clone(),
                ));
            }
            paper.task_id.clone()
        } else {
            execution.task_id.clone()
        };
        if reconcile.dependencies != vec![predecessor] {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.reconcile.clone(),
            ));
        }
        if evaluate.dependencies != vec![reconcile.task_id.clone()] {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.evaluate.clone(),
            ));
        }
        Ok(())
    }

    pub(super) fn validate_evidence_gate(&self, graph: &WorkflowGraph) -> RuntimeResult<()> {
        // Evidence Gate 是无依赖的唯一根，input_artifacts 必须精确等于全图聚合 Need；
        // 每个研究根都必须从它开始，防止绕过证据采集直接调用 Agent。
        let evidence_nodes = graph
            .nodes
            .iter()
            .filter(|node| node.recipe_id == self.catalogue.terminals.evidence_gate)
            .collect::<Vec<_>>();
        let Some(evidence) = evidence_nodes.first().copied() else {
            return Err(RuntimeError::MissingEvidenceGate(
                self.catalogue.terminals.evidence_gate.clone(),
            ));
        };
        if evidence_nodes.len() != 1 {
            return Err(RuntimeError::UnexpectedTerminalGate(
                self.catalogue.terminals.evidence_gate.clone(),
            ));
        }

        if !evidence.dependencies.is_empty() {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.evidence_gate.clone(),
            ));
        }

        let research = graph
            .nodes
            .iter()
            .filter(|node| !self.is_terminal_node(node))
            .cloned()
            .collect::<Vec<_>>();
        let research_ids = research
            .iter()
            .map(|node| node.task_id.clone())
            .collect::<BTreeSet<_>>();
        if evidence.input_artifacts != self.aggregate_evidence_needs(&research)? {
            return Err(RuntimeError::InvalidEvidencePlan);
        }
        for node in &research {
            let has_research_parent = node
                .dependencies
                .iter()
                .any(|dependency| research_ids.contains(dependency));
            if !has_research_parent && node.dependencies != vec![evidence.task_id.clone()] {
                return Err(RuntimeError::ResearchBypassesEvidence(node.task_id.clone()));
            }
        }
        Ok(())
    }
}
