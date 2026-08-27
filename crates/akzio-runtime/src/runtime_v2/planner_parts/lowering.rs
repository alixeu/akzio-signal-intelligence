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
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: candidate.topology_id.clone(),
            nodes,
        };
        graph.validate()?;
        self.validate_compiled_graph(RunPurpose::Shadow, &graph)?;
        Ok(graph)
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
                self.fixture_mode || purpose == RunPurpose::PaperDryRun,
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
                self.fixture_mode || purpose == RunPurpose::PaperDryRun,
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
            let declared_needs = declared_needs.into_iter().collect::<BTreeSet<_>>();
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
}
