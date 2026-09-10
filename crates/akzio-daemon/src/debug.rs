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
    #[serde(default)]
    pub fixture_controller: bool,
}

fn default_debug_purpose() -> RunPurpose {
    RunPurpose::Paper
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DebugForkRequest {
    pub task_id: Option<TaskId>,
    pub experiment_id: RunId,
    pub reason: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_range_probe: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

impl Daemon {
    pub fn debug_enabled(&self) -> bool {
        self.debug_control.is_some()
    }

    pub fn prepare_debug(&self, request: &DebugPrepareRequest) -> Result<DebugSession> {
        let config = self
            .debug_control
            .as_ref()
            .ok_or_else(|| DaemonError::InvalidInput("debug_control_disabled".into()))?;
        if !matches!(
            request.purpose,
            RunPurpose::Paper | RunPurpose::PositionPlan
        ) || (request.purpose == RunPurpose::PositionPlan
            && (request.paper_allowed || request.fixture_controller))
        {
            return Err(DaemonError::InvalidInput(
                "debug prepare requires Paper or non-executing PositionPlan".into(),
            ));
        }
        if request.fixture_controller {
            if !self.fixture_mode || request.paper_allowed {
                return Err(DaemonError::InvalidInput(
                    "controller fixture requires fixture Core and forbidden broker writes".into(),
                ));
            }
            let now = Utc::now();
            let graph = self.workflow.bootstrap(RunPurpose::PaperDryRun, "active")?;
            let workflow = self.workflow.prepare_workflow_commit(
                RunId::new(),
                RunPurpose::PaperDryRun,
                graph,
                now,
            )?;
            let identity = self.debug_identity(&workflow, vec![], false)?;
            return Ok(self
                .store
                .commit_debug_experiment(&workflow, &[], &identity)?);
        }
        NaiveDate::parse_from_str(&request.session_key, "%Y-%m-%d")
            .map_err(|_| DaemonError::InvalidInput("session_key must be YYYY-MM-DD".into()))?;
        if request.purpose == RunPurpose::PositionPlan {
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
            reason: None,
            created_at: workflow.run.created_at,
        })
    }

    pub fn control_debug(
        &self,
        run_id: &RunId,
        request: &DebugControlRequest,
    ) -> Result<DebugSession> {
        let config = self
            .debug_control
            .as_ref()
            .ok_or_else(|| DaemonError::InvalidInput("debug_control_disabled".into()))?;
        if matches!(
            request.action,
            akzio_domain::DebugAction::Step | akzio_domain::DebugAction::RetryNode
        ) && !self.outcome_processing
            && self.store.workflow_snapshot(run_id)?.tasks.iter().any(|t| {
                Some(&t.node.task_id) == request.task_id.as_ref()
                    && t.node.recipe_id.as_str() == "learning.outcome_worker"
            })
        {
            return Err(DaemonError::Unavailable(
                "outcome_processing_disabled_or_adapter_unavailable".into(),
            ));
        }
        Ok(self
            .store
            .debug_control(run_id, request, &config.runtime_identity, Utc::now())?)
    }

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
            view.allowed_actions
                .retain(|a| matches!(a.as_str(), "pause" | "abort"));
        }
        let lifecycle = self.store.run_lifecycle_health(run_id)?;
        for node in &mut view.nodes {
            let outcome = node.role == "learning.outcome_worker";
            if outcome {
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
            let reason = if !identity_matches {
                Some("runtime_identity_changed: create a new experiment")
            } else if outcome && !self.outcome_processing {
                Some("outcome_processing_disabled_or_adapter_unavailable")
            } else {
                None
            };
            if let Some(reason) = reason {
                node.step_eligible = false;
                node.retry_eligible = false;
                node.blocked_reason = Some(reason.into());
            }
        }
        Ok(view)
    }

    pub fn fork_debug(&self, run_id: &RunId, request: &DebugForkRequest) -> Result<DebugSession> {
        if !self.debug_enabled() || request.reason.trim().is_empty() {
            return Err(DaemonError::InvalidInput(
                "debug enabled and experiment reason required".into(),
            ));
        }
        let reason = if request.read_range_probe {
            format!("{} [read_range_probe]", request.reason)
        } else {
            request.reason.clone()
        };
        if let Some(existing) = self.store.debug_session(&request.experiment_id)? {
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
        if request.read_range_probe && source.identity.run_purpose != RunPurpose::PositionPlan {
            return Err(DaemonError::InvalidInput(
                "read_range probe requires PositionPlan parent".into(),
            ));
        }
        let purpose = source.identity.run_purpose;
        if purpose == RunPurpose::Paper {
            return Err(DaemonError::InvalidInput("Paper experiment requires fresh prepare with a reserved Session; purpose cannot be downgraded to Debug".into()));
        }
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
            if request.read_range_probe {
                task.objective.push_str(" Independent read_range coverage experiment: before writing the Draft memo, call read_range exactly once on a real authorized NormalizedEvidence artifact, start_byte=0 and end_byte=512. Inspect the returned bytes, then complete the normal memo and Submit. A partial JSON prefix is not complete market evidence. Do not broaden any directional claim from this probe; retain all evidence gaps.");
            }
        }
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
        Ok(self
            .store
            .commit_debug_experiment(&workflow, &setup, &identity)?)
    }
}
