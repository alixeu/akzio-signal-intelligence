impl WorkflowRuntime {
    /// Compile a model proposal into a graph plus Rust-owned terminal gates. The
    /// proposal cannot name any gate recipe, create a contract, or omit final
    /// audit/evaluation transitions.
    pub fn lower(
        &self,
        purpose: RunPurpose,
        proposal: &WorkflowProposal,
    ) -> RuntimeResult<WorkflowGraph> {
        if !matches!(purpose,RunPurpose::Paper|RunPurpose::PositionPlan|RunPurpose::Shadow) { return Err(RuntimeError::LegacyWorkflowRetired); }
        let nodes = self.lower_research_nodes(proposal)?;
        self.with_terminal_gates(purpose, proposal.topology_id.clone(), nodes)
    }

    pub fn lower_shadow(
        &self,
        proposal: &WorkflowProposal,
        candidate_contract_hash: Option<&ContentHash>,
    ) -> RuntimeResult<WorkflowGraph> {
        let mut graph = self.lower(RunPurpose::Shadow, proposal)?;
        if let Some(candidate_contract_hash) = candidate_contract_hash {
            for node in graph
                .nodes
                .iter_mut()
                .filter(|node| node.recipe_id.as_str() == ANALYST_RECIPE_ID)
            {
                node.contract_hash = Some(candidate_contract_hash.clone());
            }
        }
        graph.validate()?;
        self.validate_compiled_graph(RunPurpose::Shadow, &graph)?;
        Ok(graph)
    }

    pub fn lower_shadow_from_graph(
        &self,
        candidate: &WorkflowGraph,
        evidence_inputs: &[ArtifactRef],
        analyst_contract_hash: Option<&ContentHash>,
    ) -> RuntimeResult<WorkflowGraph> {
        candidate.validate()?;
        let mut evidence_inputs = evidence_inputs.to_vec();
        evidence_inputs.sort();
        evidence_inputs.dedup();
        let id_map = candidate
            .nodes
            .iter()
            .map(|node| (node.task_id.clone(), TaskId::new()))
            .collect::<BTreeMap<_, _>>();
        let mut nodes = candidate.nodes.clone();
        for node in &mut nodes {
            let old_task_id = node.task_id.clone();
            node.task_id = id_map
                .get(&old_task_id)
                .cloned()
                .ok_or(RuntimeError::Domain(DomainError::EmptyField {
                    field: "workflow_graph.task_id",
                }))?;
            node.dependencies = node
                .dependencies
                .iter()
                .map(|dependency| {
                    id_map.get(dependency).cloned().ok_or(RuntimeError::Domain(
                        DomainError::UnknownDependency {
                            task: node.task_id.clone(),
                            dependency: dependency.clone(),
                        },
                    ))
                })
                .collect::<RuntimeResult<Vec<_>>>()?;
            node.parent_task_id = node
                .parent_task_id
                .as_ref()
                .map(|parent| {
                    id_map.get(parent).cloned().ok_or(RuntimeError::Domain(
                        DomainError::UnknownDependency {
                            task: node.task_id.clone(),
                            dependency: parent.clone(),
                        },
                    ))
                })
                .transpose()?;
            let is_terminal = node.recipe_id == self.catalogue.terminals.evidence_gate
                || node.recipe_id == self.catalogue.terminals.decision_gate
                || node.recipe_id == self.catalogue.terminals.execution_gate
                || node.recipe_id == self.catalogue.terminals.reconcile
                || node.recipe_id == self.catalogue.terminals.evaluate;
            if node.recipe_id == self.catalogue.terminals.evidence_gate || !is_terminal {
                node.input_artifacts = evidence_inputs.clone();
            }
            if node.recipe_id.as_str() == ANALYST_RECIPE_ID {
                if let Some(contract_hash) = analyst_contract_hash {
                    node.contract_hash = Some(contract_hash.clone());
                }
            }
        }
        let graph = WorkflowGraph {
            definition_version: candidate.definition_version,
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id: candidate.topology_id.clone(),
            agent_budgets: candidate.agent_budgets.clone(),
            nodes,
        };
        graph.validate()?;
        self.validate_compiled_graph(RunPurpose::Shadow, &graph)?;
        Ok(graph)
    }

}
