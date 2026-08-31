impl WorkflowRuntime {
    pub(super) fn insert_structured_critic(
        &self,
        draft: &mut WorkflowProposalDraft,
    ) -> RuntimeResult<()> {
        if draft.topology_id != STRUCTURED_CRITIQUE_CANDIDATE_TOPOLOGY_ID {
            return Ok(());
        }
        let analyst_aliases = draft
            .tasks
            .iter()
            .filter_map(|(alias, task)| {
                (task.recipe_id.as_str() == ANALYST_RECIPE_ID).then_some(alias.clone())
            })
            .collect::<Vec<_>>();
        if analyst_aliases.is_empty() {
            return Ok(());
        }
        if draft
            .tasks
            .values()
            .any(|task| task.recipe_id.as_str() == CRITIC_RECIPE_ID)
        {
            return Err(RuntimeError::PlannerSchedulesCritic);
        }
        if draft
            .tasks
            .values()
            .filter(|task| task.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID)
            .count()
            > 1
        {
            return Err(RuntimeError::PlannerSchedulesMultipleSynthesizers);
        }

        let critic_recipe_id = TaskRecipeId::new(CRITIC_RECIPE_ID)?;
        let critic_recipe = self.catalogue.recipe(&critic_recipe_id)?;
        let mut suffix = 0;
        let critic_alias = loop {
            let alias = if suffix == 0 {
                STRUCTURED_CRITIC_ALIAS_PREFIX.to_owned()
            } else {
                format!("{STRUCTURED_CRITIC_ALIAS_PREFIX}_{suffix}")
            };
            if !draft.tasks.contains_key(&alias) {
                break alias;
            }
            suffix += 1;
        };

        draft.tasks.insert(
            critic_alias.clone(),
            akzio_domain::WorkflowProposalDraftTask {
                recipe_id: critic_recipe_id,
                objective: "Perform one evidence-bound critique of the analyst claims when Rust detects material uncertainty or directional conflict.".to_owned(),
                depends_on: analyst_aliases.clone(),
            priority: critic_recipe.priority_ceiling,
            evidence_needs: Vec::new(),
            research_intents: Vec::new(),
        },
        );
        for task in draft
            .tasks
            .values_mut()
            .filter(|task| task.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID)
        {
            task.depends_on
                .retain(|dependency| !analyst_aliases.contains(dependency));
            task.depends_on.push(critic_alias.clone());
            task.depends_on.sort();
            task.depends_on.dedup();
        }
        draft.validate(&self.catalogue.recipes)?;
        Ok(())
    }

    pub(super) fn assert_planner_attempt(&self, planner: &ClaimedAttempt) -> RuntimeResult<()> {
        let recipe = self.catalogue.recipe(&self.catalogue.planner)?;
        if planner.node.recipe_id != self.catalogue.planner
            || planner.node.contract_hash != recipe.contract_hash
            || planner.permit.contract_hash != recipe.contract_hash
        {
            return Err(RuntimeError::PlannerPermitRequired);
        }
        Ok(())
    }

    pub(super) fn validate_compiled_graph(
        &self,
        purpose: RunPurpose,
        graph: &WorkflowGraph,
    ) -> RuntimeResult<()> {
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
                || node.budget != recipe.budget
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

        let planner_nodes = graph
            .nodes
            .iter()
            .filter(|node| node.recipe_id == self.catalogue.planner)
            .collect::<Vec<_>>();
        if planner_nodes.len() > 1 {
            return Err(RuntimeError::DuplicatePlannerTask);
        }
        let expected_evidence_dependencies = planner_nodes
            .first()
            .map(|planner| vec![planner.task_id.clone()])
            .unwrap_or_default();
        if evidence.dependencies != expected_evidence_dependencies {
            return Err(RuntimeError::InvalidTerminalDependencies(
                self.catalogue.terminals.evidence_gate.clone(),
            ));
        }

        let research = graph
            .nodes
            .iter()
            .filter(|node| node.recipe_id != self.catalogue.planner && !self.is_terminal_node(node))
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
