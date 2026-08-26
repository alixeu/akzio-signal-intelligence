use super::*;

impl WorkflowRuntime {
    /// Compile a model proposal into a graph plus Rust-owned terminal gates. The
    /// proposal cannot name any gate recipe, create a contract, or omit final
    /// audit/evaluation transitions.
    pub fn lower(
        &self,
        purpose: RunPurpose,
        proposal: &WorkflowProposal,
    ) -> RuntimeResult<WorkflowGraph> {
        let nodes = self.lower_research_nodes(proposal)?;
        self.with_terminal_gates(purpose, proposal.topology_id.clone(), nodes)
    }

    /// Applies a Planner proposal to a bootstrap graph. It adds all research nodes
    /// and then one immutable terminal chain; a later Planner cannot patch gates.
    pub fn apply_planner_output(
        &self,
        planner: &ClaimedAttempt,
        previous_graph_artifact: &Artifact,
        previous_graph: &WorkflowGraph,
        planner_output: &Artifact,
        now: DateTime<Utc>,
    ) -> RuntimeResult<Artifact> {
        self.assert_planner_attempt(planner)?;
        if planner_output.kind != ArtifactKind::WorkflowProposalDraft {
            return Err(RuntimeError::Store(
                StoreError::InvalidWorkflowProposalArtifact,
            ));
        }
        let draft: WorkflowProposalDraft =
            serde_json::from_slice(&self.store.read_blob(&planner_output.blob)?)?;
        let run_id = &planner.run_id;
        let purpose = self.store.run_purpose(run_id)?;
        if purpose == RunPurpose::Paper {
            return Err(RuntimeError::FrozenPaperWorkflow(run_id.clone()));
        }
        let mut draft = draft;
        if matches!(purpose, RunPurpose::Debug | RunPurpose::PaperDryRun) {
            let synthesizer_recipe = TaskRecipeId::new(SYNTHESIZER_RECIPE_ID)?;
            prepare_debug_draft(
                &mut draft,
                self.catalogue.recipe(&synthesizer_recipe).is_ok(),
            )?;
        }
        draft.validate(&self.catalogue.recipes)?;
        let (proposal, evidence_needs, proposal_artifact) =
            self.materialize_planner_output(planner, planner_output, draft, purpose, now)?;
        previous_graph.validate()?;
        self.validate_compiled_graph(purpose, previous_graph)?;
        if previous_graph.topology_id != proposal.topology_id {
            return Err(RuntimeError::Domain(DomainError::EmptyField {
                field: "workflow_proposal.topology_id",
            }));
        }
        let mut nodes = previous_graph.nodes.clone();
        let evidence_index = nodes
            .iter()
            .position(|node| node.recipe_id == self.catalogue.terminals.evidence_gate)
            .ok_or_else(|| {
                RuntimeError::MissingEvidenceGate(self.catalogue.terminals.evidence_gate.clone())
            })?;
        let evidence_task_id = nodes[evidence_index].task_id.clone();
        let mut added_nodes = self.lower_research_nodes(&proposal)?;
        self.attach_evidence_gate(&mut added_nodes, &evidence_task_id);
        nodes.extend(added_nodes.iter().cloned());
        let decision_index = nodes
            .iter()
            .position(|node| node.recipe_id == self.catalogue.terminals.decision_gate)
            .ok_or_else(|| {
                RuntimeError::MissingTerminalGate(self.catalogue.terminals.decision_gate.clone())
            })?;
        let mut decision = nodes[decision_index].clone();
        let research = nodes
            .iter()
            .filter(|node| !self.is_terminal_node(node) && node.recipe_id != self.catalogue.planner)
            .cloned()
            .collect::<Vec<_>>();
        let mut evidence = nodes[evidence_index].clone();
        evidence.input_artifacts = self.aggregate_evidence_needs(&research)?;
        nodes[evidence_index] = evidence.clone();
        decision.dependencies = if research.is_empty() {
            vec![evidence_task_id]
        } else {
            leaf_ids(&research)
        };
        nodes[decision_index] = decision.clone();
        let graph = WorkflowGraph {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: previous_graph.topology_id.clone(),
            nodes,
        };
        graph.validate()?;
        self.validate_compiled_graph(purpose, &graph)?;
        let next_artifact = self.graph_artifact(
            &graph,
            vec![
                ArtifactRef {
                    artifact_id: previous_graph_artifact.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowGraph,
                },
                ArtifactRef {
                    artifact_id: proposal_artifact.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowProposal,
                },
            ],
            now,
        )?;
        self.store.commit_workflow_patch(&WorkflowPatchCommit {
            permit: planner.permit.clone(),
            previous_graph_artifact_id: previous_graph_artifact.artifact_id.clone(),
            planner_output: planner_output.clone(),
            evidence_needs,
            proposal: proposal_artifact.clone(),
            next_graph: next_artifact.clone(),
            added_nodes,
            updated_nodes: vec![evidence, decision],
            completed_at: now,
        })?;
        Ok(next_artifact)
    }

    pub(super) fn materialize_planner_output(
        &self,
        planner: &ClaimedAttempt,
        planner_output: &Artifact,
        mut draft: WorkflowProposalDraft,
        purpose: RunPurpose,
        now: DateTime<Utc>,
    ) -> RuntimeResult<(WorkflowProposal, Vec<Artifact>, Artifact)> {
        if matches!(purpose, RunPurpose::Debug | RunPurpose::PaperDryRun) {
            let synthesizer_recipe = TaskRecipeId::new(SYNTHESIZER_RECIPE_ID)?;
            prepare_debug_draft(
                &mut draft,
                self.catalogue.recipe(&synthesizer_recipe).is_ok(),
            )?;
        }
        self.insert_structured_critic(&mut draft)?;
        let planner_output_ref = ArtifactRef {
            artifact_id: planner_output.artifact_id.clone(),
            kind: ArtifactKind::WorkflowProposalDraft,
        };
        let origin = ArtifactOrigin {
            run_id: Some(planner.run_id.clone()),
            task_id: Some(planner.node.task_id.clone()),
            attempt_id: Some(planner.permit.attempt_id.clone()),
            contract_hash: planner.permit.contract_hash.clone(),
        };
        let provenance = ArtifactProvenance {
            source_family: "akzio.workflow.planner".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: planner.permit.contract_hash.clone(),
        };

        let mut need_artifacts = BTreeMap::<EvidenceNeed, Artifact>::new();
        let mut tasks = BTreeMap::new();
        for (alias, task) in draft.tasks {
            let mut declared_needs = task.evidence_needs;
            declared_needs.extend(
                task.research_intents
                    .into_iter()
                    .map(|intent| intent.evidence_need())
                    .collect::<Result<Vec<_>, _>>()?,
            );
            if purpose == RunPurpose::Paper {
                Self::normalize_paper_evidence_needs(&mut declared_needs);
            }
            let mut evidence_needs = Vec::with_capacity(declared_needs.len());
            for need in declared_needs {
                let artifact = if let Some(artifact) = need_artifacts.get(&need) {
                    artifact.clone()
                } else {
                    let artifact = Artifact::new(
                        ArtifactKind::EvidenceNeed,
                        self.store.put_json(&need)?,
                        "runtime.planner.evidence_need",
                        ArtifactLifecycle::RunScoped,
                        provenance.clone(),
                        Some(origin.clone()),
                        vec![planner_output_ref.clone()],
                        now,
                    )?;
                    need_artifacts.insert(need.clone(), artifact.clone());
                    artifact
                };
                evidence_needs.push(ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: ArtifactKind::EvidenceNeed,
                });
            }
            tasks.insert(
                alias,
                WorkflowProposalTask {
                    recipe_id: task.recipe_id,
                    objective: task.objective,
                    depends_on: task.depends_on,
                    priority: task.priority,
                    evidence_needs,
                },
            );
        }

        let proposal = WorkflowProposal {
            schema_version: draft.schema_version,
            topology_id: draft.topology_id,
            tasks,
            stop_reason: draft.stop_reason,
        };
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(&proposal)?;

        let evidence_needs = need_artifacts.into_values().collect::<Vec<_>>();
        let proposal_sources = std::iter::once(planner_output_ref)
            .chain(evidence_needs.iter().map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::EvidenceNeed,
            }))
            .collect();
        let proposal_artifact = Artifact::new(
            ArtifactKind::WorkflowProposal,
            self.store.put_json(&proposal)?,
            "runtime.planner.proposal",
            ArtifactLifecycle::RunScoped,
            provenance,
            Some(origin),
            proposal_sources,
            now,
        )?;
        Ok((proposal, evidence_needs, proposal_artifact))
    }

    fn normalize_paper_evidence_needs(needs: &mut [EvidenceNeed]) {
        for need in needs {
            if need.source_family != "alpaca" {
                continue;
            }
            let mut parts = need.resource.split(':').collect::<Vec<_>>();
            if parts.len() != 5 || parts[0] != "bars" || parts[2] != "1d" {
                continue;
            }
            let Ok(limit) = parts[4].parse::<u8>() else {
                continue;
            };
            if limit < 32 {
                parts[4] = "32";
                need.resource = parts.join(":");
            }
        }
    }

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
            if node.contract_hash != recipe.contract_hash
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
        let execution = required_terminal(&terminals, &self.catalogue.terminals.execution_gate)?;
        let reconcile = required_terminal(&terminals, &self.catalogue.terminals.reconcile)?;
        let evaluate = required_terminal(&terminals, &self.catalogue.terminals.evaluate)?;
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

pub(super) fn prepare_debug_draft(
    draft: &mut WorkflowProposalDraft,
    has_synthesizer_recipe: bool,
) -> RuntimeResult<()> {
    if draft.tasks.is_empty() {
        draft.tasks.insert(
            "debug_analyst".to_owned(),
            akzio_domain::WorkflowProposalDraftTask {
                recipe_id: TaskRecipeId::new(ANALYST_RECIPE_ID)?,
                objective: "Inspect the governed TQQQ debug fixture evidence.".to_owned(),
                depends_on: Vec::new(),
                priority: 80,
                evidence_needs: Vec::new(),
                research_intents: Vec::new(),
            },
        );
    }
    let first_analyst = draft
        .tasks
        .iter()
        .find(|(_, task)| task.recipe_id.as_str() == ANALYST_RECIPE_ID)
        .map(|(alias, _)| alias.clone());
    if let Some(first_analyst) = first_analyst.as_ref() {
        draft.tasks.retain(|alias, task| {
            task.recipe_id.as_str() != ANALYST_RECIPE_ID || alias == first_analyst
        });
    }
    let aliases = draft.tasks.keys().cloned().collect::<BTreeSet<_>>();
    for task in draft.tasks.values_mut() {
        task.depends_on
            .retain(|dependency| aliases.contains(dependency));
        if task.recipe_id.as_str() == ANALYST_RECIPE_ID {
            task.evidence_needs.retain(|need| {
                need.source_family == DEBUG_FIXTURE_SOURCE
                    && need.resource == DEBUG_FIXTURE_RESOURCE
                    && need.max_age_secs > 0
            });
            task.research_intents.clear();
        } else if task.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID {
            task.evidence_needs.clear();
            task.research_intents.clear();
        }
    }
    let analyst_aliases = draft
        .tasks
        .iter()
        .filter(|(_, task)| task.recipe_id.as_str() == ANALYST_RECIPE_ID)
        .map(|(alias, _)| alias.clone())
        .collect::<Vec<_>>();
    if analyst_aliases.is_empty() {
        return Ok(());
    }

    let debug_need = EvidenceNeed {
        schema_version: V2_DOMAIN_SCHEMA_VERSION,
        source_family: DEBUG_FIXTURE_SOURCE.to_owned(),
        resource: DEBUG_FIXTURE_RESOURCE.to_owned(),
        max_age_secs: DEBUG_FIXTURE_MAX_AGE_SECS,
    };
    let mut injected_need = false;
    for task in draft
        .tasks
        .values_mut()
        .filter(|task| task.recipe_id.as_str() == ANALYST_RECIPE_ID)
    {
        if task.evidence_needs.is_empty() && task.research_intents.is_empty() {
            task.evidence_needs.push(debug_need.clone());
            injected_need = true;
        }
    }

    if !injected_need
        || !has_synthesizer_recipe
        || draft
            .tasks
            .values()
            .any(|task| task.recipe_id.as_str() == SYNTHESIZER_RECIPE_ID)
    {
        return Ok(());
    }

    let synthesizer_recipe = TaskRecipeId::new(SYNTHESIZER_RECIPE_ID)?;
    let alias = (0..)
        .map(|suffix| {
            if suffix == 0 {
                "debug_synthesizer".to_owned()
            } else {
                format!("debug_synthesizer_{suffix}")
            }
        })
        .find(|candidate| !draft.tasks.contains_key(candidate))
        .expect("unbounded alias search must find a free debug synthesizer alias");
    draft.tasks.insert(
        alias,
        akzio_domain::WorkflowProposalDraftTask {
            recipe_id: synthesizer_recipe,
            objective: "Synthesize the debug analyst claim into a blocked decision proposal."
                .to_owned(),
            depends_on: analyst_aliases,
            priority: 100,
            evidence_needs: Vec::new(),
            research_intents: Vec::new(),
        },
    );
    Ok(())
}

/// Returns whether the bounded Critic task should consume the supplied claims.
/// Rust owns this decision so a planner or model cannot add debate rounds.
pub fn should_run_structured_critique(claims: &[ResearchClaim]) -> bool {
    claims.iter().any(|claim| {
        claim.materiality_ppm >= STRUCTURED_CRITIQUE_MATERIALITY_PPM
            && (!claim.evidence_gaps.is_empty()
                || claim.confidence_ppm <= STRUCTURED_CRITIQUE_CONFIDENCE_PPM)
    }) || claims.iter().enumerate().any(|(index, claim)| {
        claims[index + 1..].iter().any(|other| {
            claim.topic == other.topic
                && claim.horizon == other.horizon
                && matches!(
                    (claim.stance, other.stance),
                    (ClaimStance::Bullish, ClaimStance::Bearish)
                        | (ClaimStance::Bearish, ClaimStance::Bullish)
                )
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paper_bar_evidence_requests_use_the_full_window() {
        let mut needs = vec![EvidenceNeed {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source_family: "alpaca".to_owned(),
            resource: "bars:QQQ:1d:2026-07-24:12".to_owned(),
            max_age_secs: 86_400,
        }];

        WorkflowRuntime::normalize_paper_evidence_needs(&mut needs);

        assert_eq!(needs[0].resource, "bars:QQQ:1d:2026-07-24:32");
    }
}
