//! Exhaustive runtime task dispatch.

use super::*;

impl Daemon {
    pub(crate) async fn execute_task(&self, task: ClaimedAttempt) -> TaskCompletion {
        match self.execute_task_inner(&task, Utc::now()).await {
            Ok(completion) => completion,
            Err(error) => {
                eprintln!(
                    "v2 daemon task failed closed run_id={} task_id={} recipe={} error={error}",
                    task.run_id, task.node.task_id, task.node.recipe_id
                );
                tracing::warn!(
                    run_id = %task.run_id,
                    task_id = %task.node.task_id,
                    recipe = %task.node.recipe_id,
                    error = %error,
                    "v2 daemon task failed closed"
                );
                retry_cause_for_daemon_error(&error)
                    .map_or(TaskCompletion::Failed, TaskCompletion::Retry)
            }
        }
    }

    pub(super) async fn execute_task_inner(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if task.node.recipe_id.as_str() == OUTCOME_WORKER_RECIPE_ID {
            return self.execute_outcome_worker(task, now).await;
        }
        let recipe = self.workflow.catalogue().recipe(&task.node.recipe_id)?;
        match recipe.task_class {
            RuntimeTaskClass::Agent => {
                let candidates = self.context_candidates(task)?;
                if task.node.recipe_id.as_str() == "research.critic" {
                    let claims = candidates
                        .iter()
                        .filter(|reference| reference.kind == ArtifactKind::Claim)
                        .map(|reference| self.read_artifact_payload::<ResearchClaim>(reference))
                        .collect::<Result<Vec<_>>>()?;
                    if !should_run_structured_critique(&claims) {
                        return Ok(TaskCompletion::NoOutput);
                    }
                }
                let output = self
                    .agents
                    .run(
                        &task.permit,
                        &task.node,
                        candidates,
                        self.model_for(task.node.recipe_id.as_str()),
                        now,
                    )
                    .await?;
                if task.node.recipe_id.as_str() == "research.planner" {
                    let revision = self.workflow.recover(&task.run_id)?.revision;
                    self.workflow.apply_planner_output(
                        task,
                        &revision.graph_artifact,
                        &revision.graph,
                        &output,
                        now,
                    )?;
                    Ok(TaskCompletion::Committed)
                } else {
                    Ok(TaskCompletion::Succeeded(vec![output]))
                }
            }
            RuntimeTaskClass::Evidence => {
                let artifacts = self.acquire_evidence(task, now).await?;
                Ok(if artifacts.is_empty() {
                    TaskCompletion::NoOutput
                } else {
                    TaskCompletion::Succeeded(artifacts)
                })
            }
            RuntimeTaskClass::DecisionGate => self.execute_decision_gate(task, now),
            RuntimeTaskClass::ExecutionGate => self.execute_execution_gate(task, now).await,
            RuntimeTaskClass::PaperCommit => self.execute_paper_commit(task, now),
            RuntimeTaskClass::Reconcile => self.execute_reconcile(task, now).await,
            RuntimeTaskClass::Evaluate => self.execute_evaluate(task, now),
        }
    }

    /// Build agent input strictly from its declared inputs and the Store's
    /// semantic committed-output query for declared, successful dependencies.
    /// This never scans a run's artifact set or exposes raw evidence;
    /// `AgentRuntime` then creates the task-bound ContextManifest and
    /// ReadGrant.
    pub(super) fn execute_decision_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let proposal = self.terminal_input(task, ArtifactKind::DecisionProposal)?;
        self.decision_runtime.decide(&DecisionGateInput {
            permit: task.permit.clone(),
            proposal,
            now,
        })?;
        Ok(TaskCompletion::Committed)
    }

    pub(super) async fn execute_execution_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let decision_context = self.terminal_input(task, ArtifactKind::DecisionContext)?;
        let (account_snapshot, quote_snapshot, market_clock_snapshot) = if self
            .production_evidence
            .contains_key(&EvidenceSource::Alpaca)
            && self.store.run_purpose(&task.run_id)? == RunPurpose::Paper
        {
            self.refresh_execution_snapshots(task, now).await?
        } else {
            self.execution_snapshot_inputs(task)?
        };
        let gate_now = Utc::now();
        // Snapshot acquisition is a separately governed Evidence path. Until a
        // provider returns typed, task-bound snapshots, the execution runtime
        // emits a durable NoOrder rather than guessing from arbitrary evidence.
        let output = self.execution_runtime.evaluate(&ExecutionGateInput {
            permit: task.permit.clone(),
            decision_context,
            account_snapshot,
            quote_snapshot,
            market_clock_snapshot,
            now: gate_now,
        })?;
        self.execution_runtime
            .commit(&task.permit, &output, gate_now)?;
        Ok(TaskCompletion::Committed)
    }

    pub(super) fn execute_paper_commit(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let verdict = self.terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let ExecutionVerdict::Accepted { execution_context } = verdict_payload else {
            return Ok(TaskCompletion::NoOutput);
        };
        let context: ExecutionContext = self.read_artifact_payload(&execution_context)?;
        context
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let session_key = context.broker_session.ok_or_else(|| {
            DaemonError::InvalidInput("accepted execution verdict has no broker session".to_owned())
        })?;
        if let Some((manifest, approval)) = self.store.paper_approval_for_run(&task.run_id)? {
            if approval.expires_at < now {
                return Err(DaemonError::InvalidInput(
                    "Paper approval expired before commitment".to_owned(),
                ));
            }
            let execution_plan = context
                .execution_plan
                .as_ref()
                .ok_or_else(|| DaemonError::InvalidInput("execution plan is missing".to_owned()))?;
            let plan: ExecutionPlan = self.read_artifact_payload(execution_plan)?;
            let total_notional = plan.orders.iter().try_fold(0_i64, |total, order| {
                total.checked_add(order.notional.0).ok_or_else(|| {
                    DaemonError::InvalidInput("execution plan notional overflow".to_owned())
                })
            })?;
            if total_notional > manifest.maximum_notional.0 {
                return Err(DaemonError::InvalidInput(
                    "execution plan exceeds approved maximum notional".to_owned(),
                ));
            }
        } else if self.auto_paper {
            return Err(DaemonError::InvalidInput(
                "Paper approval is missing".to_owned(),
            ));
        }
        let lease = self.scheduler.active_lease(now)?;
        self.paper_commitment_runtime
            .commit(&PaperCommitmentInput {
                lease,
                permit: task.permit.clone(),
                verdict,
                session_key,
                now,
            })?;
        Ok(TaskCompletion::Committed)
    }

    pub(super) async fn execute_reconcile(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(TaskCompletion::NoOutput);
        }
        let verdict = self.terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        if matches!(verdict_payload, ExecutionVerdict::NoOrder { .. }) {
            return Ok(TaskCompletion::NoOutput);
        }
        let commitment = self.terminal_input(task, ArtifactKind::ExecutionCommitment)?;
        let broker = self.paper_broker.as_ref().ok_or_else(|| {
            DaemonError::Unavailable(
                "Paper reconciliation requires an injected Alpaca Paper broker adapter".to_owned(),
            )
        })?;
        let lease = self.scheduler.active_lease(now)?;
        self.paper_dispatch_runtime
            .dispatch(
                broker.as_ref(),
                &PaperDispatchInput {
                    lease,
                    permit: task.permit.clone(),
                    commitment,
                    now,
                },
            )
            .await?;
        Ok(TaskCompletion::Committed)
    }

    pub(super) fn execute_evaluate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(TaskCompletion::NoOutput);
        }
        let decision = self.terminal_input(task, ArtifactKind::Decision)?;
        let decision_context = self.terminal_input(task, ArtifactKind::DecisionContext)?;
        let execution_context = self.terminal_input(task, ArtifactKind::ExecutionContext)?;
        let verdict = self.terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let execution = match verdict_payload {
            ExecutionVerdict::NoOrder { .. } => OutcomeExecutionLineage::NoOrder {
                execution_verdict: verdict,
            },
            ExecutionVerdict::Accepted { .. } => OutcomeExecutionLineage::ReconciledPaper {
                execution_verdict: verdict,
                commitment: self.terminal_input(task, ArtifactKind::ExecutionCommitment)?,
                reconciliation: self.terminal_input(task, ArtifactKind::Reconciliation)?,
            },
        };
        let baseline_trading_day = self.paper_baseline_day(&task.run_id)?;
        let output = self
            .outcome_scheduling_runtime
            .schedule(&OutcomeScheduleInput {
                permit: task.permit.clone(),
                decision,
                decision_context,
                execution_context,
                execution,
                baseline_trading_day,
                now,
            })?;
        self.outcome_scheduling_runtime
            .commit(&task.permit, &output, now)?;
        Ok(TaskCompletion::Committed)
    }

    pub(super) fn terminal_input(
        &self,
        task: &ClaimedAttempt,
        kind: ArtifactKind,
    ) -> Result<ArtifactRef> {
        let mut matching = self
            .ancestor_outputs(task)?
            .into_iter()
            .filter(|artifact| artifact.kind == kind)
            .map(|artifact| ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: artifact.kind,
            })
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
        matching.dedup_by(|left, right| left.artifact_id == right.artifact_id);
        match matching.as_slice() {
            [reference] => Ok(reference.clone()),
            [] => Err(DaemonError::InvalidInput(format!(
                "terminal task {} has no {:?} input",
                task.node.task_id, kind
            ))),
            _ => Err(DaemonError::InvalidInput(format!(
                "terminal task {} has ambiguous {:?} inputs",
                task.node.task_id, kind
            ))),
        }
    }

    /// Execution snapshots are produced only by the evidence gate from the
    /// scheduler-reserved Alpaca resources below. This prevents arbitrary
    /// normalized evidence from being reinterpreted as broker state.
    pub(super) fn execution_snapshot_inputs(
        &self,
        task: &ClaimedAttempt,
    ) -> Result<(
        Option<ArtifactRef>,
        Option<ArtifactRef>,
        Option<ArtifactRef>,
    )> {
        let mut account = None;
        let mut quotes = None;
        let mut clock = None;

        for artifact in self.ancestor_outputs(task)? {
            let target = match artifact.producer.as_str() {
                "execution.snapshot.account" => &mut account,
                "execution.snapshot.quotes" => &mut quotes,
                "execution.snapshot.clock" => &mut clock,
                _ => continue,
            };
            if artifact.kind != ArtifactKind::NormalizedEvidence
                || artifact.lifecycle != ArtifactLifecycle::Canonical
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&task.run_id)
                || !artifact
                    .source_refs
                    .iter()
                    .any(|source| source.kind == ArtifactKind::RawEvidence)
                || !artifact
                    .source_refs
                    .iter()
                    .any(|source| source.kind == ArtifactKind::NormalizedEvidence)
            {
                return Err(DaemonError::InvalidInput(format!(
                    "execution snapshot {} has invalid provenance",
                    artifact.artifact_id
                )));
            }
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            };
            if target.replace(reference).is_some() {
                return Err(DaemonError::InvalidInput(
                    "execution gate received duplicate governed snapshot".to_owned(),
                ));
            }
        }

        Ok((account, quotes, clock))
    }

    pub(super) fn ancestor_outputs(&self, task: &ClaimedAttempt) -> Result<Vec<Artifact>> {
        let snapshot = self.workflow.recover(&task.run_id)?;
        let tasks = snapshot
            .tasks
            .into_iter()
            .map(|stored| (stored.node.task_id.clone(), stored))
            .collect::<BTreeMap<_, _>>();
        let mut pending = task.node.dependencies.clone();
        let mut visited = BTreeSet::new();
        let mut outputs = BTreeMap::<ArtifactId, Artifact>::new();
        while let Some(task_id) = pending.pop() {
            if !visited.insert(task_id.clone()) {
                continue;
            }
            let dependency = tasks.get(&task_id).ok_or_else(|| {
                DaemonError::InvalidInput(format!(
                    "terminal task {} references unknown dependency {task_id}",
                    task.node.task_id
                ))
            })?;
            if dependency.status != TaskStatus::Succeeded {
                return Err(DaemonError::UnfinishedDependency {
                    task_id: task.node.task_id.clone(),
                    dependency: task_id,
                });
            }
            for artifact in self
                .store
                .succeeded_task_outputs_or_empty(&task.run_id, &dependency.node.task_id)?
            {
                outputs.insert(artifact.artifact_id.clone(), artifact);
            }
            pending.extend(dependency.node.dependencies.iter().cloned());
        }
        Ok(outputs.into_values().collect())
    }

    pub(super) fn read_artifact_payload<T: serde::de::DeserializeOwned>(
        &self,
        reference: &ArtifactRef,
    ) -> Result<T> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if artifact.kind != reference.kind {
            return Err(DaemonError::InvalidInput(format!(
                "artifact {} kind changed from {:?} to {:?}",
                reference.artifact_id, reference.kind, artifact.kind
            )));
        }
        Ok(serde_json::from_slice(
            &self.store.read_blob(&artifact.blob)?,
        )?)
    }

    pub(super) fn context_candidates(&self, task: &ClaimedAttempt) -> Result<Vec<ArtifactRef>> {
        let contract_hash = task.node.contract_hash.as_ref().ok_or_else(|| {
            DaemonError::InvalidInput(format!(
                "agent task {} has no contract hash",
                task.node.task_id
            ))
        })?;
        let policy = &self.agents.catalogue().get(contract_hash)?.contract.context;
        let mut candidates = BTreeMap::<ArtifactId, ArtifactRef>::new();

        for reference in &task.node.input_artifacts {
            self.admit_context_candidate(&mut candidates, policy, reference)?;
        }

        if let Some(parent_task_id) = &task.node.parent_task_id {
            if !task.node.dependencies.contains(parent_task_id) {
                return Err(DaemonError::InvalidInput(format!(
                    "agent task {} parent {parent_task_id} is not a dependency",
                    task.node.task_id
                )));
            }
            let snapshot = self.store.workflow_snapshot(&task.run_id)?;
            let parent = snapshot
                .tasks
                .iter()
                .find(|stored| stored.node.task_id == *parent_task_id)
                .ok_or_else(|| {
                    DaemonError::InvalidInput(format!(
                        "task {} references missing parent {parent_task_id}",
                        task.node.task_id
                    ))
                })?;
            if parent.status != TaskStatus::Succeeded {
                return Err(DaemonError::UnfinishedDependency {
                    task_id: task.node.task_id.clone(),
                    dependency: parent_task_id.clone(),
                });
            }
            // AgentRuntime performs the durable parent proof and child projection.
            // Keep daemon dispatch limited to dependency status/fencing checks.
        } else {
            let dependencies = task
                .node
                .dependencies
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            if !dependencies.is_empty() {
                let snapshot = self.store.workflow_snapshot(&task.run_id)?;
                for dependency in &dependencies {
                    let dependency_task = snapshot
                        .tasks
                        .iter()
                        .find(|stored| stored.node.task_id == *dependency)
                        .ok_or_else(|| {
                            DaemonError::InvalidInput(format!(
                                "task {} references missing dependency {dependency}",
                                task.node.task_id
                            ))
                        })?;
                    if dependency_task.status != TaskStatus::Succeeded {
                        return Err(DaemonError::UnfinishedDependency {
                            task_id: task.node.task_id.clone(),
                            dependency: dependency.clone(),
                        });
                    }
                }
                for dependency in dependencies {
                    for artifact in self
                        .store
                        .committed_task_outputs(&task.run_id, &dependency)?
                    {
                        self.admit_context_candidate(
                            &mut candidates,
                            policy,
                            &ArtifactRef {
                                artifact_id: artifact.artifact_id,
                                kind: artifact.kind,
                            },
                        )?;
                    }
                }
            }
        }

        if candidates.is_empty()
            && task.node.recipe_id.as_str() != "research.planner"
            && task.node.parent_task_id.is_none()
        {
            return Err(DaemonError::MissingTaskContext(task.node.task_id.clone()));
        }
        Ok(candidates.into_values().collect())
    }

    pub(super) fn admit_context_candidate(
        &self,
        candidates: &mut BTreeMap<ArtifactId, ArtifactRef>,
        policy: &ContextPolicy,
        reference: &ArtifactRef,
    ) -> Result<()> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if artifact.kind == ArtifactKind::RawEvidence {
            return Ok(());
        }
        if policy.permitted_kinds.contains(&artifact.kind)
            && policy
                .permitted_source_families
                .contains(&artifact.provenance.source_family)
        {
            candidates.insert(
                artifact.artifact_id.clone(),
                ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                },
            );
        }
        Ok(())
    }
}
