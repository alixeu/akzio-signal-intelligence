use crate::*;

/// Model-mediated session execution with daemon-owned routing and budget.
pub(crate) struct AgentSession<'a> {
    daemon: &'a Daemon,
}

impl<'a> AgentSession<'a> {
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    pub(crate) async fn run(
        &self,
        task: &ClaimedAttempt,
        candidates: Vec<ArtifactRef>,
        now: DateTime<Utc>,
        budget: &mut AgentRunBudget,
    ) -> Result<Artifact> {
        let model = self.daemon.model_for(task.node.recipe_id.as_str());
        Ok(self
            .daemon
            .agents
            .run_with_budget(&task.permit, &task.node, candidates, model, now, budget)
            .await?)
    }

    pub(crate) fn candidates(&self, task: &ClaimedAttempt) -> Result<Vec<ArtifactRef>> {
        let mut candidates = BTreeMap::<ArtifactId, ArtifactRef>::new();

        for reference in &task.node.input_artifacts {
            self.append_candidate(&mut candidates, reference)?;
        }

        if let Some(parent_task_id) = &task.node.parent_task_id {
            if !task.node.dependencies.contains(parent_task_id) {
                return Err(DaemonError::InvalidInput(format!(
                    "agent task {} parent {parent_task_id} is not dependency",
                    task.node.task_id
                )));
            }
            let snapshot = self.daemon.store.workflow_snapshot(&task.run_id)?;
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
            // AgentRuntime/ContextBroker owns parent projection and context policy.
        } else {
            let dependencies = task
                .node
                .dependencies
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            if !dependencies.is_empty() {
                let snapshot = self.daemon.store.workflow_snapshot(&task.run_id)?;
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
                        .daemon
                        .store
                        .committed_task_outputs(&task.run_id, &dependency)?
                    {
                        self.append_candidate(
                            &mut candidates,
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
            && task.node.recipe_id.as_str() != akzio_domain::RESEARCH_PLANNER_RECIPE_ID
            && task.node.parent_task_id.is_none()
        {
            return Err(DaemonError::MissingTaskContext(task.node.task_id.clone()));
        }

        Ok(candidates.into_values().collect())
    }

    fn append_candidate(
        &self,
        candidates: &mut BTreeMap<ArtifactId, ArtifactRef>,
        reference: &ArtifactRef,
    ) -> Result<()> {
        let artifact = self.daemon.store.artifact(&reference.artifact_id)?;
        if artifact.kind != reference.kind {
            return Err(DaemonError::InvalidInput(format!(
                "artifact {} kind changed from {:?} to {:?}",
                reference.artifact_id, reference.kind, artifact.kind
            )));
        }
        // Raw evidence is only a source closure; ContextBroker must not put it in
        // the model manifest directly.
        if artifact.kind != ArtifactKind::RawEvidence {
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
