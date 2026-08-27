impl V2Store {
    pub fn artifact(&self, artifact_id: &ArtifactId) -> StoreResult<Artifact> {
        let connection = self.connection()?;
        read_artifact(&connection, artifact_id)
    }

    pub fn artifacts_referencing(
        &self,
        source_artifact_id: &ArtifactId,
        kind: Option<ArtifactKind>,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection()?;
        let kind = kind.map(enum_name);
        self.verify_contract_catalogue_history(&connection)?;
        self.verify_policy_evaluation_history(&connection)?;

        let mut statement = connection.prepare(
            r#"SELECT r.artifact_id
               FROM rebuild_artifact_refs AS r
               JOIN rebuild_artifacts AS a ON a.artifact_id = r.artifact_id
               WHERE r.source_artifact_id = ?1 AND (?2 IS NULL OR a.kind = ?2)
               ORDER BY r.artifact_id ASC"#,
        )?;
        let ids = statement
            .query_map(params![source_artifact_id.0.as_str(), kind], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        ids.into_iter()
            .map(|id| read_artifact(&connection, &ArtifactId(ContentHash::new(id)?)))
            .collect()
    }

    /// Return the newest immutable artifact of a kind. Mutable state such as
    /// execution freeze is represented as an append-only artifact history;
    /// callers never receive a writable row handle.
    pub fn latest_artifact_by_kind(&self, kind: ArtifactKind) -> StoreResult<Option<Artifact>> {
        let connection = self.connection()?;
        let artifact_id = connection
            .query_row(
            "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY CASE WHEN lifecycle = 'canonical' THEN 0 ELSE 1 END, created_at DESC, artifact_id DESC LIMIT 1",
                params![enum_name(kind)],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        artifact_id
            .map(ContentHash::new)
            .transpose()?
            .map(ArtifactId)
            .map(|artifact_id| read_artifact(&connection, &artifact_id))
            .transpose()
    }

    /// Return newest immutable artifacts of one kind, newest first.
    /// Observer callers cannot request an unbounded Store scan.
    pub fn recent_artifacts_by_kind(
        &self,
        kind: ArtifactKind,
        limit: usize,
    ) -> StoreResult<Vec<Artifact>> {
        let connection = self.connection()?;
        let limit = i64::try_from(limit.clamp(1, 500)).expect("bounded artifact limit fits i64");
        let mut statement = connection.prepare(
            "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY created_at DESC, artifact_id DESC LIMIT ?2",
        )?;
        let ids = statement
            .query_map(params![enum_name(kind), limit], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        ids.into_iter()
            .map(|id| read_artifact(&connection, &ArtifactId(ContentHash::new(id)?)))
            .collect()
    }

    fn workflow_revision_with_connection(
        &self,
        connection: &Connection,
        run_id: &RunId,
        revision: u64,
    ) -> StoreResult<WorkflowRevision> {
        let row = connection
            .query_row(
                r#"SELECT revision, graph_artifact_id, created_at
                   FROM rebuild_workflow_revisions
                   WHERE run_id = ?1 AND revision = ?2"#,
                params![run_id.0, revision],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::MissingWorkflowRevision {
                run_id: run_id.clone(),
                revision,
            })?;
        self.hydrate_workflow_revision(connection, row)
    }

    fn workflow_snapshot_with_connection(
        &self,
        connection: &Connection,
        run_id: &RunId,
    ) -> StoreResult<WorkflowSnapshot> {
        let run_row = connection
            .query_row(
                r#"SELECT purpose, topology_id, graph_artifact_id, status, created_at, finished_at
                   FROM rebuild_runs WHERE run_id = ?1"#,
                params![run_id.0],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::MissingRun(run_id.clone()))?;
        let (purpose, topology_id, graph_artifact_id, status, created_at, finished_at) = run_row;
        let run = StoredRun {
            run_id: run_id.clone(),
            purpose: parse_enum(&purpose)?,
            topology_id,
            graph_artifact_id: ArtifactId(ContentHash::new(graph_artifact_id)?),
            created_at: parse_time(&created_at)?,
        };
        let revision_row = connection
            .query_row(
                r#"SELECT revision, graph_artifact_id, created_at
                   FROM rebuild_workflow_revisions
                   WHERE run_id = ?1 ORDER BY revision DESC LIMIT 1"#,
                params![run_id.0],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::Integrity(format!("run {run_id} has no workflow revision"))
            })?;
        let revision = self.hydrate_workflow_revision(connection, revision_row)?;
        if revision.graph_artifact.artifact_id != run.graph_artifact_id
            || revision.graph.topology_id != run.topology_id
        {
            return Err(StoreError::WorkflowGraphMismatch);
        }

        let raw_tasks = connection
            .prepare(
                r#"SELECT t.task_id, t.run_id, t.recipe_id, t.objective, t.contract_hash,
                          t.priority, t.budget_json, t.retry_json, t.on_failure,
                          t.parent_task_id, t.input_artifacts_json, t.status, t.ready_at,
                          t.lease_id, t.lease_epoch, t.active_attempt_id, t.lease_until,
                          t.worker_id, t.finished_at,
                          (SELECT COUNT(*) FROM rebuild_attempts AS a WHERE a.task_id = t.task_id)
                   FROM rebuild_tasks AS t
                   WHERE t.run_id = ?1 ORDER BY t.task_id ASC"#,
            )?
            .query_map(params![run_id.0], |row| {
                Ok((
                    row_to_node(row)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, u64>(14)?,
                    row.get::<_, Option<String>>(15)?,
                    row.get::<_, Option<String>>(16)?,
                    row.get::<_, Option<String>>(17)?,
                    row.get::<_, Option<String>>(18)?,
                    row.get::<_, u64>(19)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut tasks = Vec::with_capacity(raw_tasks.len());
        for (
            (task_run_id, mut node),
            task_status,
            ready_at,
            lease_id,
            epoch,
            active_attempt_id,
            lease_until,
            worker_id,
            task_finished_at,
            attempt_count,
        ) in raw_tasks
        {
            if task_run_id != *run_id {
                return Err(StoreError::WorkflowGraphMismatch);
            }
            node.dependencies = task_dependencies(connection, &node.task_id)?;
            let task_status = parse_task_status(&task_status)?;
            let active_attempt = match (lease_id, active_attempt_id, lease_until, worker_id) {
                (Some(lease_id), Some(attempt_id), Some(lease_until), Some(worker_id))
                    if task_status == TaskStatus::Running =>
                {
                    let attempt = connection
                        .query_row(
                            r#"SELECT run_id, task_id, lease_id, epoch, worker_id, status, started_at
                               FROM rebuild_attempts WHERE attempt_id = ?1"#,
                            params![attempt_id],
                            |row| {
                                Ok((
                                    row.get::<_, String>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, String>(2)?,
                                    row.get::<_, u64>(3)?,
                                    row.get::<_, String>(4)?,
                                    row.get::<_, String>(5)?,
                                    row.get::<_, String>(6)?,
                                ))
                            },
                        )
                        .optional()?
                        .ok_or_else(|| {
                            StoreError::Integrity(format!(
                                "active attempt {attempt_id} does not exist"
                            ))
                        })?;
                    if attempt.0 != run_id.0
                        || attempt.1 != node.task_id.0
                        || attempt.2 != lease_id
                        || attempt.3 != epoch
                        || attempt.4 != worker_id
                        || attempt.5 != "running"
                    {
                        return Err(StoreError::Integrity(format!(
                            "active attempt {attempt_id} does not match task {}",
                            node.task_id
                        )));
                    }
                    Some(StoredActiveAttempt {
                        permit: TaskWritePermit {
                            run_id: run_id.clone(),
                            task_id: node.task_id.clone(),
                            attempt_id: AttemptId(attempt_id),
                            lease_id: LeaseId(lease_id),
                            epoch,
                            contract_hash: node.contract_hash.clone(),
                        },
                        worker_id,
                        lease_until: parse_time(&lease_until)?,
                        started_at: parse_time(&attempt.6)?,
                    })
                }
                (None, None, None, None) if task_status != TaskStatus::Running => None,
                _ => {
                    return Err(StoreError::Integrity(format!(
                        "task {} has partial active attempt state",
                        node.task_id
                    )))
                }
            };
            tasks.push(StoredTaskSnapshot {
                node,
                status: task_status,
                ready_at: parse_time(&ready_at)?,
                active_attempt,
                attempt_count,
                finished_at: task_finished_at.as_deref().map(parse_time).transpose()?,
            });
        }
        let graph_nodes = revision
            .graph
            .nodes
            .iter()
            .cloned()
            .map(canonical_workflow_node)
            .map(|node| (node.task_id.clone(), node))
            .collect::<std::collections::BTreeMap<_, _>>();
        let stored_nodes = tasks
            .iter()
            .filter(|task| task.node.recipe_id.as_str() != POST_TERMINAL_WORKER_RECIPE_ID)
            .map(|task| canonical_workflow_node(task.node.clone()))
            .map(|node| (node.task_id.clone(), node))
            .collect::<std::collections::BTreeMap<_, _>>();
        if graph_nodes != stored_nodes {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        let event_cursor = connection.query_row(
            "SELECT COALESCE(MAX(event_id), 0) FROM rebuild_events WHERE run_id = ?1",
            params![run_id.0],
            |row| row.get::<_, i64>(0),
        )?;
        let cancel_requested = connection
            .query_row(
                "SELECT 1 FROM rebuild_run_cancellations WHERE run_id = ?1",
                params![run_id.0],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(WorkflowSnapshot {
            run,
            status: parse_enum(&status)?,
            finished_at: finished_at.as_deref().map(parse_time).transpose()?,
            revision,
            tasks,
            event_cursor,
            cancel_requested,
        })
    }

    fn hydrate_workflow_revision(
        &self,
        connection: &Connection,
        row: (i64, String, String),
    ) -> StoreResult<WorkflowRevision> {
        let revision = u64::try_from(row.0)
            .map_err(|_| StoreError::Integrity(format!("invalid workflow revision {}", row.0)))?;
        let graph_artifact = read_artifact(connection, &ArtifactId(ContentHash::new(row.1)?))?;
        if graph_artifact.kind != ArtifactKind::WorkflowGraph {
            return Err(StoreError::InvalidWorkflowGraphArtifact);
        }
        let graph: WorkflowGraph = serde_json::from_slice(&self.read_blob(&graph_artifact.blob)?)?;
        graph.validate()?;
        Ok(WorkflowRevision {
            revision,
            graph_artifact,
            graph,
            created_at: parse_time(&row.2)?,
        })
    }

    fn verify_workflow_history(
        &self,
        connection: &Connection,
        snapshot: &WorkflowSnapshot,
    ) -> StoreResult<()> {
        let mut previous: Option<WorkflowRevision> = None;
        for revision_number in 0..=snapshot.revision.revision {
            let revision = self.workflow_revision_with_connection(
                connection,
                &snapshot.run.run_id,
                revision_number,
            )?;
            if revision.graph.topology_id != snapshot.run.topology_id {
                return Err(StoreError::WorkflowGraphMismatch);
            }
            if let Some(previous) = &previous {
                if revision.created_at < previous.created_at
                    || revision.graph_artifact.source_refs.len() != 2
                    || !revision.graph_artifact.source_refs.iter().any(|reference| {
                        reference.artifact_id == previous.graph_artifact.artifact_id
                            && reference.kind == ArtifactKind::WorkflowGraph
                    })
                    || !revision
                        .graph_artifact
                        .source_refs
                        .iter()
                        .any(|reference| reference.kind == ArtifactKind::WorkflowProposal)
                {
                    return Err(StoreError::WorkflowGraphMismatch);
                }
            }
            previous = Some(revision);
        }
        if previous.as_ref() != Some(&snapshot.revision) {
            return Err(StoreError::WorkflowGraphMismatch);
        }
        Ok(())
    }

    fn trajectory_entry(&self, event: &StoredEvent) -> StoreResult<Option<TrajectoryEntry>> {
        let lifecycle = event.lifecycle_kind()?;
        let base = |artifact: Option<&Artifact>| TrajectoryEntry {
            cursor: event.cursor,
            task_id: event.task_id.clone(),
            attempt_id: event.attempt_id.clone(),
            turn: None,
            phase: None,
            assistant_text: None,
            event_type: event.event_type.clone(),
            artifact_id: event.artifact_id.clone(),
            artifact_kind: artifact.map(|value| value.kind),
            model: None,
            latency_millis: None,
            input_tokens: None,
            output_tokens: None,
            tool: None,
            deliberation: None,
            output_refs: Vec::new(),
        };

        match lifecycle {
            LifecycleEventType::AgentTurnStarted => Ok(Some(base(None))),
            LifecycleEventType::AgentTurn
            | LifecycleEventType::AgentTurnCompleted
            | LifecycleEventType::AgentTurnFailed
            | LifecycleEventType::AgentTurnRetryableFailed => {
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    return Ok(Some(base(None)));
                };
                let artifact = self.artifact(artifact_id)?;
                if artifact.kind != ArtifactKind::AgentTurn {
                    return Err(StoreError::Integrity(format!(
                        "trajectory event {} references {:?}, expected agent_turn",
                        event.cursor, artifact.kind
                    )));
                }
                let payload: StoredTrajectoryTurn =
                    match serde_json::from_slice(&self.read_blob(&artifact.blob)?) {
                        Ok(payload) => payload,
                        Err(_) => return Ok(Some(base(Some(&artifact)))),
                    };
                let mut model = payload.capability_snapshot.unwrap_or_default();
                model.contract_hash = payload.contract_hash;
                model.request_hash = payload.request_hash;
                model.capability_snapshot_hash = payload.capability_snapshot_hash;
                model.tool_set_hash = payload.tool_set_hash;
                let mut entry = base(Some(&artifact));
                entry.turn = payload.turn;
                entry.phase = payload.request.and_then(|request| request.phase);
                entry.assistant_text = payload
                    .response
                    .as_ref()
                    .and_then(|response| response.assistant_text.clone());
                entry.model = Some(model);
                let telemetry = payload
                    .response
                    .as_ref()
                    .and_then(|response| response.telemetry.as_ref())
                    .or(payload.telemetry.as_ref());
                entry.latency_millis = telemetry.and_then(|telemetry| telemetry.latency_millis);
                entry.input_tokens = telemetry.and_then(|telemetry| telemetry.input_tokens);
                entry.output_tokens = telemetry.and_then(|telemetry| telemetry.output_tokens);
                Ok(Some(entry))
            }
            LifecycleEventType::ToolCalled
            | LifecycleEventType::ToolCompleted
            | LifecycleEventType::ToolFailed => {
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    return Ok(None);
                };
                let artifact = self.artifact(artifact_id)?;
                if !matches!(
                    artifact.kind,
                    ArtifactKind::ToolCall | ArtifactKind::ToolResult
                ) {
                    return Err(StoreError::Integrity(format!(
                        "trajectory event {} references {:?}, expected tool artifact",
                        event.cursor, artifact.kind
                    )));
                }
                let payload: StoredTrajectoryToolArtifact =
                    match serde_json::from_slice(&self.read_blob(&artifact.blob)?) {
                        Ok(payload) => payload,
                        Err(_) => return Ok(Some(base(Some(&artifact)))),
                    };
                let call_id = payload
                    .call_id
                    .or_else(|| payload.call.as_ref().and_then(|call| call.call_id.clone()));
                let name = payload
                    .name
                    .or_else(|| payload.call.as_ref().and_then(|call| call.name.clone()));
                let mut entry = base(Some(&artifact));
                entry.tool = Some(TrajectoryToolLifecycle {
                    call_id,
                    name,
                    lifecycle: event.event_type.clone(),
                });
                Ok(Some(entry))
            }
            LifecycleEventType::DeliberationNoteCreated => {
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    return Ok(None);
                };
                let artifact = self.artifact(artifact_id)?;
                if artifact.kind != ArtifactKind::DeliberationNote {
                    return Err(StoreError::Integrity(format!(
                        "trajectory event {} references {:?}, expected deliberation_note",
                        event.cursor, artifact.kind
                    )));
                }
                let deliberation: DeliberationSummary =
                    serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
                deliberation.validate()?;
                let mut entry = base(Some(&artifact));
                entry.deliberation = Some(deliberation);
                Ok(Some(entry))
            }
            LifecycleEventType::ArtifactCommitted => {
                let Some(artifact_id) = event.artifact_id.as_ref() else {
                    return Ok(None);
                };
                let artifact = self.artifact(artifact_id)?;
                if is_trajectory_redacted_kind(artifact.kind) {
                    return Ok(None);
                }
                let mut entry = base(Some(&artifact));
                entry.output_refs = trajectory_output_refs(&artifact);
                Ok(Some(entry))
            }
            _ => Ok(None),
        }
    }

    fn verify_outcome_schedule_history(&self, connection: &Connection) -> StoreResult<()> {
        let artifact_ids = connection
            .prepare(
                "SELECT artifact_id FROM rebuild_artifacts WHERE kind = ?1 ORDER BY artifact_id",
            )?
            .query_map(params![enum_name(ArtifactKind::OutcomeSchedule)], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for value in artifact_ids {
            let artifact_id = ArtifactId(ContentHash::new(value)?);
            let artifact = read_artifact(connection, &artifact_id)?;
            let (expected_lifecycle, allowed_purposes) =
                match artifact_run_purpose(connection, &artifact)? {
                    RunPurpose::Paper => (ArtifactLifecycle::Canonical, vec![RunPurpose::Paper]),
                    RunPurpose::Shadow => (
                        ArtifactLifecycle::RunScoped,
                        vec![RunPurpose::Paper, RunPurpose::Shadow],
                    ),
                    purpose => {
                        return Err(StoreError::Integrity(format!(
                            "outcome schedule {artifact_id} has invalid run purpose {purpose:?}"
                        )));
                    }
                };
            if artifact.lifecycle != expected_lifecycle {
                return Err(StoreError::Integrity(format!(
                    "outcome schedule {artifact_id} has invalid lifecycle"
                )));
            }
            let schedule: OutcomeSchedule =
                serde_json::from_slice(&self.read_blob(&artifact.blob)?).map_err(|error| {
                    StoreError::Integrity(format!(
                        "outcome schedule {artifact_id} has invalid payload: {error}"
                    ))
                })?;
            schedule.validate().map_err(|error| {
                StoreError::Integrity(format!(
                    "outcome schedule {artifact_id} fails validation: {error}"
                ))
            })?;
            let expected_sources = outcome_schedule_source_refs(&schedule);
            if !has_exact_source_refs(&artifact, &expected_sources) {
                return Err(StoreError::Integrity(format!(
                    "outcome schedule {artifact_id} has invalid source closure"
                )));
            }
            for reference in &expected_sources {
                let source = read_artifact(connection, &reference.artifact_id)?;
                if source.kind != reference.kind {
                    return Err(StoreError::Integrity(format!(
                        "outcome schedule {artifact_id} source kind is invalid"
                    )));
                }
                assert_artifact_from_allowed_purposes(connection, &source, &allowed_purposes)
                    .map_err(|error| {
                        StoreError::Integrity(format!(
                            "outcome schedule {artifact_id} source origin is invalid: {error}"
                        ))
                    })?;
            }
            self.validate_outcome_schedule_execution_lineage(
                connection,
                &schedule,
                &allowed_purposes,
            )
            .map_err(|error| {
                StoreError::Integrity(format!(
                    "outcome schedule {artifact_id} execution lineage is invalid: {error}"
                ))
            })?;
        }
        Ok(())
    }
}
