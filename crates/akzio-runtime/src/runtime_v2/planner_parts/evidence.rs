impl WorkflowRuntime {
    pub(super) fn lower_research_nodes(
        &self,
        proposal: &WorkflowProposal,
    ) -> RuntimeResult<Vec<WorkflowNode>> {
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(proposal)?;
        if proposal.tasks.len() > self.catalogue.max_nodes {
            return Err(RuntimeError::WorkflowNodeLimit);
        }
        let ids = proposal
            .tasks
            .keys()
            .map(|alias| (alias.clone(), akzio_domain::TaskId::new()))
            .collect::<BTreeMap<_, _>>();
        proposal
            .tasks
            .iter()
            .map(|(alias, task)| {
                let recipe = self.catalogue.recipe(&task.recipe_id)?;
                if self.catalogue.is_rust_gate(&task.recipe_id)
                    || matches!(
                        recipe.task_class,
                        RuntimeTaskClass::DecisionGate
                            | RuntimeTaskClass::ExecutionGate
                            | RuntimeTaskClass::PaperCommit
                            | RuntimeTaskClass::Reconcile
                            | RuntimeTaskClass::Evaluate
                    )
                {
                    return Err(RuntimeError::TerminalRecipeInProposal(
                        task.recipe_id.clone(),
                    ));
                }
                let agent_parents = task
                    .depends_on
                    .iter()
                    .filter(|dependency| {
                        proposal
                            .tasks
                            .get(*dependency)
                            .and_then(|parent| self.catalogue.recipe(&parent.recipe_id).ok())
                            .is_some_and(|parent| parent.task_class == RuntimeTaskClass::Agent)
                    })
                    .collect::<Vec<_>>();
                if agent_parents.len() > 1 {
                    return Err(RuntimeError::MultipleAgentParents {
                        task: alias.clone(),
                    });
                }
                Ok(WorkflowNode {
                    task_id: ids[alias].clone(),
                    recipe_id: recipe.recipe_id.clone(),
                    contract_hash: recipe.contract_hash.clone(),
                    objective: task.objective.clone(),
                    dependencies: task
                        .depends_on
                        .iter()
                        .map(|dependency| ids[dependency].clone())
                        .collect(),
                    input_artifacts: task.evidence_needs.clone(),
                    priority: task.priority,
                    budget: recipe.budget.clone(),
                    retry: recipe.retry.clone(),
                    on_failure: recipe.on_failure,
                    parent_task_id: agent_parents
                        .first()
                        .map(|dependency| ids[*dependency].clone()),
                })
            })
            .collect()
    }

    pub(super) fn validate_proposal_limits(
        &self,
        proposal: &WorkflowProposal,
    ) -> RuntimeResult<()> {
        let mut children = BTreeMap::<String, Vec<String>>::new();
        for (alias, task) in &proposal.tasks {
            for dependency in &task.depends_on {
                let parent = proposal
                    .tasks
                    .get(dependency)
                    .expect("proposal validation checked dependencies");
                let parent_recipe = self.catalogue.recipe(&parent.recipe_id)?;
                let descendants = children.entry(dependency.clone()).or_default();
                descendants.push(alias.clone());
                if descendants.len() > usize::from(parent_recipe.max_children) {
                    return Err(RuntimeError::WorkflowFanoutLimit {
                        task: dependency.clone(),
                        recipe: parent.recipe_id.clone(),
                    });
                }
            }
        }
        for (root, task) in &proposal.tasks {
            let recipe = self.catalogue.recipe(&task.recipe_id)?;
            let mut stack = vec![(root.clone(), 0_u16)];
            while let Some((alias, depth)) = stack.pop() {
                if depth > recipe.max_depth {
                    return Err(RuntimeError::WorkflowDepthLimit {
                        task: alias,
                        recipe: recipe.recipe_id.clone(),
                    });
                }
                if let Some(descendants) = children.get(&alias) {
                    stack.extend(descendants.iter().cloned().map(|child| (child, depth + 1)));
                }
            }
        }
        Ok(())
    }

    pub(super) fn with_terminal_gates(
        &self,
        purpose: RunPurpose,
        topology_id: String,
        mut nodes: Vec<WorkflowNode>,
    ) -> RuntimeResult<WorkflowGraph> {
        let planners = nodes
            .iter()
            .filter(|node| node.recipe_id == self.catalogue.planner)
            .collect::<Vec<_>>();
        if planners.len() > 1 {
            return Err(RuntimeError::DuplicatePlannerTask);
        }

        let evidence_dependencies = planners
            .first()
            .map(|planner| vec![planner.task_id.clone()])
            .unwrap_or_default();
        let evidence_needs = self.aggregate_evidence_needs(&nodes)?;
        let mut evidence = self.gate_node(
            &self.catalogue.terminals.evidence_gate,
            evidence_dependencies,
        )?;
        evidence.input_artifacts = evidence_needs;
        let evidence_task_id = evidence.task_id.clone();
        for node in &mut nodes {
            if node.recipe_id != self.catalogue.planner
                && !node.dependencies.contains(&evidence_task_id)
            {
                node.dependencies.push(evidence_task_id.clone());
                node.dependencies.sort();
                if matches!(
                    node.recipe_id.as_str(),
                    SYNTHESIZER_RECIPE_ID | CRITIC_RECIPE_ID
                ) {
                    node.parent_task_id = None;
                }
            }
        }

        let research = nodes
            .iter()
            .filter(|node| node.recipe_id != self.catalogue.planner)
            .cloned()
            .collect::<Vec<_>>();
        let decision_dependencies = if research.is_empty() {
            vec![evidence_task_id]
        } else {
            leaf_ids(&research)
        };
        let decision = self.gate_node(
            &self.catalogue.terminals.decision_gate,
            decision_dependencies,
        )?;
        let execution = self.gate_node(
            &self.catalogue.terminals.execution_gate,
            vec![decision.task_id.clone()],
        )?;
        let predecessor = if purpose == RunPurpose::Paper {
            let paper = self.gate_node(
                &self.catalogue.terminals.paper_commit,
                vec![execution.task_id.clone()],
            )?;
            let task_id = paper.task_id.clone();
            nodes.push(paper);
            task_id
        } else {
            execution.task_id.clone()
        };
        let reconcile = self.gate_node(&self.catalogue.terminals.reconcile, vec![predecessor])?;
        let evaluate = self.gate_node(
            &self.catalogue.terminals.evaluate,
            vec![reconcile.task_id.clone()],
        )?;
        nodes.push(evidence);
        nodes.extend([decision, execution, reconcile, evaluate]);
        if nodes.len() > self.catalogue.max_nodes {
            return Err(RuntimeError::WorkflowNodeLimit);
        }
        let graph = WorkflowGraph {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id,
            nodes,
        };
        graph.validate()?;
        Ok(graph)
    }

    pub(super) fn attach_evidence_gate(
        &self,
        nodes: &mut [WorkflowNode],
        evidence_task_id: &akzio_domain::TaskId,
    ) {
        for node in nodes {
            if node.recipe_id != self.catalogue.planner && node.dependencies.is_empty() {
                node.dependencies.push(evidence_task_id.clone());
            }
        }
    }

    pub(super) fn aggregate_evidence_needs(
        &self,
        nodes: &[WorkflowNode],
    ) -> RuntimeResult<Vec<ArtifactRef>> {
        let mut needs = BTreeMap::new();
        for node in nodes {
            let mut task_needs = BTreeSet::new();
            for reference in &node.input_artifacts {
                if reference.kind != ArtifactKind::EvidenceNeed
                    || !task_needs.insert(reference.artifact_id.clone())
                {
                    return Err(RuntimeError::InvalidEvidenceNeed(node.task_id.clone()));
                }
                needs
                    .entry(reference.artifact_id.clone())
                    .or_insert_with(|| reference.clone());
            }
        }
        Ok(needs.into_values().collect())
    }

    pub(super) fn gate_node(
        &self,
        recipe_id: &TaskRecipeId,
        dependencies: Vec<akzio_domain::TaskId>,
    ) -> RuntimeResult<WorkflowNode> {
        let recipe = self.catalogue.recipe(recipe_id)?;
        Ok(WorkflowNode {
            task_id: akzio_domain::TaskId::new(),
            recipe_id: recipe.recipe_id.clone(),
            contract_hash: None,
            objective: format!("Rust-owned {}", recipe.purpose.as_str()),
            dependencies,
            input_artifacts: vec![],
            priority: recipe.priority_ceiling,
            budget: recipe.budget.clone(),
            retry: recipe.retry.clone(),
            on_failure: recipe.on_failure,
            parent_task_id: None,
        })
    }

    pub(super) fn graph_artifact(
        &self,
        graph: &WorkflowGraph,
        source_refs: Vec<ArtifactRef>,
        now: DateTime<Utc>,
    ) -> RuntimeResult<Artifact> {
        Ok(Artifact::new(
            ArtifactKind::WorkflowGraph,
            self.store.put_json(graph)?,
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            ArtifactProvenance {
                source_family: "akzio.runtime".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            source_refs,
            now,
        )?)
    }

    pub(super) fn is_terminal_node(&self, node: &WorkflowNode) -> bool {
        self.catalogue.recipe(&node.recipe_id).is_ok_and(|recipe| {
            matches!(
                recipe.task_class,
                RuntimeTaskClass::Evidence
                    | RuntimeTaskClass::DecisionGate
                    | RuntimeTaskClass::ExecutionGate
                    | RuntimeTaskClass::PaperCommit
                    | RuntimeTaskClass::Reconcile
                    | RuntimeTaskClass::Evaluate
            )
        })
    }
}
