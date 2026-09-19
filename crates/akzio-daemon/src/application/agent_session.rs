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
        let expand_research_sources = should_expand_research_sources(task.node.recipe_id.as_str());

        for reference in &task.node.input_artifacts {
            self.append_candidate(&mut candidates, reference, expand_research_sources)?;
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
                        .succeeded_task_outputs_or_empty(&task.run_id, &dependency)?
                    {
                        self.append_candidate(
                            &mut candidates,
                            &ArtifactRef {
                                artifact_id: artifact.artifact_id,
                                kind: artifact.kind,
                            },
                            expand_research_sources,
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
        expand_research_sources: bool,
    ) -> Result<()> {
        if let Some(existing) = candidates.get(&reference.artifact_id) {
            if existing.kind != reference.kind {
                return Err(DaemonError::InvalidInput(format!(
                    "artifact {} kind changed from {:?} to {:?}",
                    reference.artifact_id, existing.kind, reference.kind
                )));
            }
            return Ok(());
        }
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
            if artifact.kind == ArtifactKind::SemanticDetail
                && artifact.producer == "canary.evidence_snapshot"
            {
                for source in artifact
                    .source_refs
                    .iter()
                    .filter(|source| source.kind == ArtifactKind::NormalizedEvidence)
                {
                    self.append_candidate(candidates, source, expand_research_sources)?;
                }
            }
            candidates.insert(
                artifact.artifact_id.clone(),
                ArtifactRef {
                    artifact_id: artifact.artifact_id,
                    kind: artifact.kind,
                },
            );
            if expand_research_sources {
                for source in research_output_source_refs(artifact.kind, &artifact.source_refs) {
                    self.append_candidate(candidates, &source, expand_research_sources)?;
                }
            }
        }
        Ok(())
    }
}

fn research_output_source_refs(
    kind: ArtifactKind,
    source_refs: &[ArtifactRef],
) -> Vec<ArtifactRef> {
    if !matches!(kind, ArtifactKind::Claim | ArtifactKind::Critique) {
        return Vec::new();
    }
    source_refs
        .iter()
        .filter(|reference| {
            matches!(
                reference.kind,
                ArtifactKind::Claim
                    | ArtifactKind::Critique
                    | ArtifactKind::NormalizedEvidence
                    | ArtifactKind::SemanticDetail
            )
        })
        .cloned()
        .collect()
}

fn should_expand_research_sources(recipe_id: &str) -> bool {
    matches!(
        recipe_id,
        akzio_domain::RESEARCH_CRITIC_RECIPE_ID | akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_output_candidates_include_their_context_source_closure() {
        let source = ArtifactRef {
            artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(b"projection")),
            kind: ArtifactKind::SemanticDetail,
        };
        let claim = ArtifactRef {
            artifact_id: ArtifactId(akzio_domain::ContentHash::of_bytes(b"claim")),
            kind: ArtifactKind::Claim,
        };

        let refs =
            research_output_source_refs(ArtifactKind::Critique, &[claim.clone(), source.clone()]);

        assert!(refs.contains(&claim));
        assert!(refs.contains(&source));
    }

    #[test]
    fn critics_and_synthesizers_expand_research_output_sources() {
        assert!(should_expand_research_sources(
            akzio_domain::RESEARCH_CRITIC_RECIPE_ID
        ));
        assert!(should_expand_research_sources(
            akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
        ));
    }
}
