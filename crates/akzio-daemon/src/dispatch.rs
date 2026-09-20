//! Exhaustive runtime task dispatch.

use super::*;

impl Daemon {
    pub(crate) async fn execute_task(&self, task: ClaimedAttempt) -> TaskCompletion {
        akzio_runtime::NodeExecutor::execute(
            self,
            akzio_runtime::NodeContext {
                task,
                started_at: Utc::now(),
            },
        )
        .await
    }

    pub(crate) async fn execute_task_at(
        &self,
        task: ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> TaskCompletion {
        match self.execute_task_inner(&task, now).await {
            Ok(completion) => completion,
            Err(error) => {
                use akzio_ingest::runtime::EvidenceAdapterError;
                if let DaemonError::Evidence(EvidenceRuntimeError::Adapter(adapter)) = &error {
                    match adapter {
                        EvidenceAdapterError::Pending(_) => {
                            return TaskCompletion::DeferredUntil(
                                Utc::now() + Duration::minutes(20),
                            );
                        }
                        EvidenceAdapterError::RateLimited { retry_after_secs } => {
                            return TaskCompletion::RetryAfter(
                                RetryCause::RateLimited,
                                Utc::now()
                                    + Duration::seconds((*retry_after_secs).min(86_400) as i64),
                            );
                        }
                        _ => {}
                    }
                }
                if self.debug_enabled() {
                    // Persist the failure while the original attempt still owns its lease.
                    // The existing inspector redacts diagnostics before UI/API projection.
                    let acceptance = akzio_domain::StageAcceptance {
                        version: 1,
                        run_id: task.run_id.clone(),
                        task_id: task.node.task_id.clone(),
                        attempt_id: task.permit.attempt_id.clone(),
                        stage: task.node.recipe_id.as_str().into(),
                        business_result: "Failed".into(),
                        test_result: akzio_domain::AcceptanceResult::Blocked,
                        checks: vec![akzio_domain::AcceptanceCheck {
                            check_id: "runtime.failure".into(),
                            category: akzio_domain::AcceptanceCategory::Persistence,
                            expected: "stage reaches a valid business boundary".into(),
                            actual: error.to_string(),
                            result: akzio_domain::AcceptanceResult::Blocked,
                            evidence_refs: vec![],
                            message: "Original failed attempt; no downstream permission granted"
                                .into(),
                        }],
                        created_at: Utc::now(),
                    };
                    if let Err(record_error) = self.store.record_stage_acceptance(&acceptance) {
                        tracing::warn!(error = %record_error, "failed to persist Debug failure diagnostic");
                    }
                }
                eprintln!(
                    "daemon task failed closed run_id={} task_id={} recipe={} error={error}",
                    task.run_id, task.node.task_id, task.node.recipe_id
                );
                tracing::warn!(
                    run_id = %task.run_id,
                    task_id = %task.node.task_id,
                    recipe = %task.node.recipe_id,
                    error = %error,
                    "daemon task failed closed"
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
        if task.node.recipe_id.as_str() == akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID {
            return self.execute_outcome_worker(task, now).await;
        }
        let recipe = self.workflow.recipe(&task.node.recipe_id)?;
        match recipe.task_class {
            RuntimeTaskClass::Agent => self.research_run().execute(task, now).await,
            RuntimeTaskClass::ResearchControl => self.shared_research_supplement(task, now).await,
            RuntimeTaskClass::Evidence => self.evidence_acquisition().execute(task, now).await,
            RuntimeTaskClass::DecisionGate => self.paper_execution().decision_gate(task, now),
            RuntimeTaskClass::ExecutionGate => {
                self.paper_execution().execution_gate(task, now).await
            }
            RuntimeTaskClass::PaperCommit => self.paper_execution().commit(task, now),
            RuntimeTaskClass::Reconcile => self.paper_execution().reconcile(task, now).await,
            RuntimeTaskClass::Evaluate => self.outcome_sealing().execute(task, now).await,
        }
    }

    /// Build agent input strictly from its declared inputs and the Store's
    /// semantic committed-output query for declared, successful dependencies.
    /// This never scans a run's artifact set or exposes raw evidence;
    /// `AgentRuntime` then creates the task-bound ContextManifest and
    /// ReadGrant.
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
            if !matches!(
                dependency.status,
                TaskStatus::Succeeded | TaskStatus::Skipped
            ) {
                return Err(DaemonError::UnfinishedDependency {
                    task_id: task.node.task_id.clone(),
                    dependency: task_id,
                });
            }
            if dependency.status == TaskStatus::Skipped {
                pending.extend(dependency.node.dependencies.iter().cloned());
                continue;
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
}

impl akzio_runtime::NodeExecutor for Daemon {
    fn execute(
        &self,
        context: akzio_runtime::NodeContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = akzio_runtime::NodeOutcome> + Send + '_>>
    {
        Box::pin(async move { self.execute_task_at(context.task, context.started_at).await })
    }
}
