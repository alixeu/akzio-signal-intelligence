use super::*;

impl WorkflowRuntime {
    /// Creates the non-Paper bootstrap graph whose sole model task is the
    /// installed Planner. The Planner may extend this graph only while its
    /// active attempt still owns a valid write permit.
    pub fn bootstrap(
        &self,
        purpose: RunPurpose,
        topology_id: impl Into<String>,
    ) -> RuntimeResult<WorkflowGraph> {
        if purpose == RunPurpose::Paper {
            return Err(RuntimeError::PaperWorkflowRequiresPrecompiledProposal);
        }
        let topology_id = topology_id.into();
        if topology_id.trim().is_empty() {
            return Err(RuntimeError::Domain(DomainError::EmptyField {
                field: "workflow.topology_id",
            }));
        }
        let recipe = self.catalogue.recipe(&self.catalogue.planner)?;
        let objective = match purpose {
            RunPurpose::Debug | RunPurpose::PaperDryRun => {
                "Run a bounded real-LLM debug workflow against governed TQQQ fixture evidence."
            }
            _ => "Produce bounded workflow proposal",
        };
        let planner = WorkflowNode {
            task_id: akzio_domain::TaskId::new(),
            recipe_id: recipe.recipe_id.clone(),
            contract_hash: recipe.contract_hash.clone(),
            objective: objective.to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: recipe.priority_ceiling,
            budget: recipe.budget.clone(),
            retry: recipe.retry.clone(),
            on_failure: recipe.on_failure,
            parent_task_id: None,
        };
        self.with_terminal_gates(purpose, topology_id, vec![planner])
    }

    pub fn submit(
        &self,
        run_id: RunId,
        purpose: RunPurpose,
        graph: WorkflowGraph,
        now: DateTime<Utc>,
    ) -> RuntimeResult<Artifact> {
        graph.validate()?;
        self.validate_compiled_graph(purpose, &graph)?;
        let graph_artifact = self.graph_artifact(&graph, vec![], now)?;
        self.store.commit_workflow(&WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact.clone(),
            nodes: graph.nodes,
        })?;
        Ok(graph_artifact)
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
        let session_key = session_key.into();
        if let Some(slot) = self.store.session_slot(&session_key)? {
            return Ok(SessionSlotReservation {
                slot,
                newly_reserved: false,
            });
        }

        let proposal_artifact = if binding.is_some() {
            Some(Artifact::new(
                ArtifactKind::WorkflowProposal,
                self.store.put_json(proposal)?,
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
                    .collect(),
                now,
            )?)
        } else {
            None
        };
        let graph = self.lower(RunPurpose::Paper, proposal)?;
        let graph_artifact = self.graph_artifact(
            &graph,
            proposal_artifact
                .iter()
                .map(|artifact| ArtifactRef {
                    artifact_id: artifact.artifact_id.clone(),
                    kind: ArtifactKind::WorkflowProposal,
                })
                .collect(),
            now,
        )?;
        let workflow = WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        };
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
        let analyst = self
            .catalogue
            .recipe(&TaskRecipeId::new(ANALYST_RECIPE_ID)?)?;
        let synthesizer = self
            .catalogue
            .recipe(&TaskRecipeId::new(SYNTHESIZER_RECIPE_ID)?)?;
        let proposal = WorkflowProposal {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: topology_id.into(),
            tasks: BTreeMap::from([
                (
                    "analyst".to_owned(),
                    akzio_domain::WorkflowProposalTask {
                        recipe_id: analyst.recipe_id.clone(),
                        objective: "Assess governed Paper market evidence".to_owned(),
                        depends_on: Vec::new(),
                        priority: analyst.priority_ceiling,
                        evidence_needs: Vec::new(),
                    },
                ),
                (
                    "synthesizer".to_owned(),
                    akzio_domain::WorkflowProposalTask {
                        recipe_id: synthesizer.recipe_id.clone(),
                        objective:
                            "Synthesize approved Paper research into a bounded decision proposal"
                                .to_owned(),
                        depends_on: vec!["analyst".to_owned()],
                        priority: synthesizer.priority_ceiling,
                        evidence_needs: Vec::new(),
                    },
                ),
            ]),
            stop_reason: Some("rust-approved Paper provisioning".to_owned()),
        };
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(&proposal)?;
        Ok(proposal)
    }

    pub fn reserve_approved_paper_session(
        &self,
        lease: &DaemonLease,
        run_id: RunId,
        session_key: impl Into<String>,
        topology_id: impl Into<String>,
        setup_artifacts: &[Artifact],
        now: DateTime<Utc>,
    ) -> RuntimeResult<SessionSlotReservation> {
        let mut proposal = self.approved_paper_proposal(topology_id)?;
        let snapshot_refs = setup_artifacts
            .iter()
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
            })
            .collect::<Vec<_>>();
        proposal
            .tasks
            .get_mut("analyst")
            .ok_or(RuntimeError::MissingRecipe(TaskRecipeId::new(
                ANALYST_RECIPE_ID,
            )?))?
            .evidence_needs = snapshot_refs;
        proposal.validate(&self.catalogue.recipes)?;
        self.validate_proposal_limits(&proposal)?;
        let proposal_artifact = Artifact::new(
            ArtifactKind::WorkflowProposal,
            self.store.put_json(&proposal)?,
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
                .collect(),
            now,
        )?;
        let session_key = session_key.into();
        let graph = self.lower(RunPurpose::Paper, &proposal)?;
        let graph_artifact = self.graph_artifact(
            &graph,
            vec![ArtifactRef {
                artifact_id: proposal_artifact.artifact_id.clone(),
                kind: ArtifactKind::WorkflowProposal,
            }],
            now,
        )?;
        let workflow = WorkflowCommit {
            run: StoredRun {
                run_id,
                purpose: RunPurpose::Paper,
                topology_id: graph.topology_id.clone(),
                graph_artifact_id: graph_artifact.artifact_id.clone(),
                created_at: now,
            },
            graph: graph_artifact,
            nodes: graph.nodes,
        };
        Ok(self.store.reserve_paper_session_with_proposal(
            lease,
            &SessionReservation {
                session_key,
                workflow,
                setup_artifacts: setup_artifacts.to_vec(),
                reserved_at: now,
            },
            &proposal_artifact,
        )?)
    }
}
