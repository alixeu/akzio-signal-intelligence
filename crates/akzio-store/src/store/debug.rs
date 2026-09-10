//! Persisted execution authority, shared by every claim path and every client.
use super::trajectory::stored_event_from_row;
use super::*;
use akzio_domain::{
    AcceptanceCategory, AcceptanceCheck, AcceptanceResult, DebugAction, DebugBrokerPolicy,
    DebugControlRequest, DebugExecutionMode, DebugLearningScope, DebugSession,
    DebugSessionIdentity, DebugStatus, StageAcceptance,
};

fn blocked(reason: impl Into<String>) -> StoreError {
    StoreError::DebugControl(reason.into())
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugAttemptView {
    pub attempt_id: AttemptId,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugNodeView {
    pub task: StoredTaskSnapshot,
    pub role: String,
    pub horizon: Option<String>,
    pub attempts: Vec<DebugAttemptView>,
    pub business_ready: bool,
    pub step_eligible: bool,
    pub retry_eligible: bool,
    pub blocked_reason: Option<String>,
    pub output_refs: Vec<ArtifactRef>,
    pub budget: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugArtifactView {
    pub artifact: Artifact,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct DebugRunView {
    pub session: DebugSession,
    pub workflow_status: WorkflowStatus,
    pub execution_evidence: String,
    pub nodes: Vec<DebugNodeView>,
    pub events: Vec<StoredEvent>,
    pub artifacts: Vec<DebugArtifactView>,
    pub acceptance: Vec<StageAcceptance>,
    pub observed_at: DateTime<Utc>,
    pub allowed_actions: Vec<String>,
}

impl Store {
    /// A Store can be marked isolated only before its first Run. The marker is
    /// permanent: opening this Store as an ordinary Core is subsequently denied.
    pub fn configure_debug_environment(&self, enabled: bool) -> StoreResult<Option<String>> {
        if enabled
            && std::env::var_os("HOME").is_some_and(|home| {
                let canonical = PathBuf::from(home).join(".akzio/store");
                self.root.canonicalize().ok() == canonical.canonicalize().ok()
            })
        {
            return Err(blocked(
                "Debug requires a separate Store, never ~/.akzio/store",
            ));
        }
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = environment_identity(&tx)?;
        if !enabled {
            if existing.is_some() {
                return Err(blocked("isolated Store requires debug_control=true"));
            }
            return Ok(None);
        }
        if let Some(existing) = existing {
            return Ok(Some(existing));
        }
        let count: u64 = tx.query_row("SELECT count(*) FROM rebuild_runs", [], |r| r.get(0))?;
        if count != 0 {
            return Err(blocked("debug_control requires a new isolated Store"));
        }
        let identity = format!("debug-store-{}", RunId::new());
        tx.execute(
            "INSERT INTO rebuild_metadata(key,value) VALUES('debug_environment',?1)",
            params![identity],
        )?;
        tx.commit()?;
        Ok(Some(identity))
    }

    pub fn debug_environment(&self) -> StoreResult<Option<String>> {
        environment_identity(&*self.connection()?)
    }

    /// Publish the frozen business graph and its paused control atomically.
    /// No worker can observe the new Run without observing its scheduling gate.
    pub fn reserve_debug_session(
        &self,
        lease: &DaemonLease,
        reservation: &SessionReservation,
        proposal: &Artifact,
        identity: &DebugSessionIdentity,
        approval_binding: Option<(&Artifact, &Artifact)>,
    ) -> StoreResult<DebugSession> {
        self.validate_paper_session_reservation(reservation, proposal)?;
        if let Some((manifest, approval)) = approval_binding {
            self.validate_paper_approval_binding(manifest, approval)?;
        }
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_identity(&tx, identity, &reservation.workflow)?;
        Self::commit_session_slot_transaction(&tx, lease, reservation, proposal, approval_binding)?;
        insert_session(&tx, identity)?;
        let session = read_session(&tx, &identity.run_id)?.expect("inserted session");
        tx.commit()?;
        Ok(session)
    }

    /// Noncanonical experiment: new IDs and immutable parent artifact lineage.
    /// It uses the ordinary non-Paper lowering and never acquires a session slot.
    pub fn commit_debug_experiment(
        &self,
        workflow: &WorkflowCommit,
        setup: &[Artifact],
        identity: &DebugSessionIdentity,
    ) -> StoreResult<DebugSession> {
        if !matches!(
            workflow.run.purpose,
            RunPurpose::Debug | RunPurpose::PositionPlan | RunPurpose::PaperDryRun
        ) {
            return Err(blocked("experiment must be noncanonical"));
        }
        self.validate_workflow_commit(workflow)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_identity(&tx, identity, workflow)?;
        insert_artifact_batch(&tx, setup)?;
        Self::commit_workflow_transaction(&tx, workflow)?;
        Self::append_run_setup_events(&tx, &workflow.run.run_id, setup, workflow.run.created_at)?;
        insert_session(&tx, identity)?;
        let session = read_session(&tx, &identity.run_id)?.expect("inserted session");
        tx.commit()?;
        Ok(session)
    }

    pub fn debug_session(&self, run_id: &RunId) -> StoreResult<Option<DebugSession>> {
        read_session(&*self.connection()?, run_id)
    }

    /// Compare-and-swap is mandatory. A repeated HTTP POST cannot issue a second
    /// permit, even if the first step has already finished on another worker.
    pub fn debug_control(
        &self,
        run_id: &RunId,
        request: &DebugControlRequest,
        runtime_identity: &ContentHash,
        now: DateTime<Utc>,
    ) -> StoreResult<DebugSession> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut session =
            read_session(&tx, run_id)?.ok_or_else(|| blocked("not a Debug session"))?;
        if session.revision != request.expected_revision {
            return Err(blocked("revision_conflict"));
        }
        if matches!(session.status, DebugStatus::Aborted) {
            return Err(blocked("session_aborted"));
        }
        if request.action != DebugAction::Pause
            && request.action != DebugAction::Abort
            && &session.identity.runtime_identity != runtime_identity
        {
            return Err(blocked("runtime_identity_changed: create a new experiment"));
        }
        let running = running_count(&tx, run_id)?;
        match request.action {
            DebugAction::Pause => {
                session.permitted_task_id = None;
                session.execution_mode = DebugExecutionMode::Manual;
                session.status = if running > 0 {
                    DebugStatus::PauseRequested
                } else {
                    DebugStatus::Paused
                };
            }
            DebugAction::Resume => {
                if session.status != DebugStatus::Paused {
                    return Err(blocked("resume_requires_paused"));
                }
                session.status = DebugStatus::Running;
                session.execution_mode = DebugExecutionMode::Continuous;
                session.permitted_task_id = None;
            }
            DebugAction::Step | DebugAction::RetryNode => {
                if session.status != DebugStatus::Paused || running != 0 {
                    return Err(blocked("step_requires_paused"));
                }
                let task_id = request
                    .task_id
                    .as_ref()
                    .ok_or_else(|| blocked("unique_task_id_required"))?;
                if let Some(reason) = task_blocked_reason(&tx, run_id, task_id, now)? {
                    return Err(blocked(reason));
                }
                if request.action == DebugAction::RetryNode && !retry_eligible(&tx, task_id)? {
                    return Err(blocked(
                        "not_retryable: terminal or exhausted stages require a new experiment",
                    ));
                }
                session.status = DebugStatus::Stepping;
                session.execution_mode = DebugExecutionMode::Manual;
                session.permitted_task_id = Some(task_id.clone());
                session.active_attempt_id = None;
            }
            DebugAction::Abort => {
                // Cooperative debug abort drains active work, never drops futures.
                if running != 0 {
                    return Err(blocked("pause_and_drain_before_abort"));
                }
                session.status = DebugStatus::Aborted;
                session.permitted_task_id = None;
            }
        }
        save_session(&tx, &mut session, now)?;
        tx.commit()?;
        Ok(session)
    }

    /// Defense at the actual effect-intent boundary, including cancel/reprice.
    pub fn assert_debug_broker_write(&self, run_id: &RunId) -> StoreResult<()> {
        assert_broker_write(&*self.connection()?, run_id)
    }

    pub fn block_debug_broker_task(
        &self,
        permit: &TaskWritePermit,
        evidence: &[ArtifactRef],
        now: DateTime<Utc>,
    ) -> StoreResult<bool> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        match assert_broker_write(&tx, &permit.run_id) {
            Ok(()) => return Ok(false),
            Err(StoreError::DebugBrokerWriteForbidden) => {}
            Err(error) => return Err(error),
        }
        assert_permit(&tx, permit)?;
        let mut session =
            read_session(&tx, &permit.run_id)?.ok_or(StoreError::DebugBrokerWriteForbidden)?;
        session.status = DebugStatus::PauseRequested;
        session.permitted_task_id = None;
        save_session(&tx, &mut session, now)?;
        write_acceptance(
            &tx,
            &StageAcceptance {
                version: 1,
                run_id: permit.run_id.clone(),
                task_id: permit.task_id.clone(),
                attempt_id: permit.attempt_id.clone(),
                stage: "broker_dispatch".into(),
                business_result: "Accepted".into(),
                test_result: AcceptanceResult::Blocked,
                checks: vec![AcceptanceCheck {
                    check_id: "debug.broker_write_policy".into(),
                    category: AcceptanceCategory::SideEffect,
                    expected: "explicit paper_allowed policy".into(),
                    actual: "forbidden".into(),
                    result: AcceptanceResult::Blocked,
                    evidence_refs: evidence.to_vec(),
                    message: "Business prerequisites: PASS; Debug broker write policy: BLOCK"
                        .into(),
                }],
                created_at: now,
            },
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn debug_learning_isolated(&self, run_id: &RunId) -> StoreResult<bool> {
        let connection = self.connection()?;
        Ok(environment_identity(&connection)?.is_some()
            || read_session(&connection, run_id)?
                .is_some_and(|s| s.identity.learning_scope == DebugLearningScope::Isolated))
    }

    /// One read transaction, no repairs, no staging, no claim and no network.
    pub fn debug_inspect(
        &self,
        run_id: &RunId,
        task_id: Option<&TaskId>,
        attempt_id: Option<&AttemptId>,
        now: DateTime<Utc>,
    ) -> StoreResult<DebugRunView> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let session = read_session(&tx, run_id)?.ok_or_else(|| blocked("not a Debug session"))?;
        let snapshot = self.workflow_snapshot_with_connection(&tx, run_id)?;
        if let Some(task_id) = task_id {
            if !snapshot.tasks.iter().any(|t| &t.node.task_id == task_id) {
                return Err(StoreError::MissingTask(task_id.clone()));
            }
        }
        if let Some(attempt) = attempt_id {
            let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM rebuild_attempts WHERE run_id=?1 AND attempt_id=?2 AND (?3 IS NULL OR task_id=?3))",
                params![run_id.0, attempt.0, task_id.map(|t| t.0.as_str())], |r| r.get(0))?;
            if !exists {
                return Err(blocked("attempt_not_in_selected_run_and_task"));
            }
        }
        let mut nodes = Vec::new();
        for task in snapshot.tasks {
            if task_id.is_some_and(|id| id != &task.node.task_id) {
                continue;
            }
            let attempts = tx.prepare("SELECT attempt_id,status,started_at,finished_at FROM rebuild_attempts WHERE task_id=?1 ORDER BY epoch")?
                .query_map(params![task.node.task_id.0], |r| Ok(DebugAttemptView {
                    attempt_id: AttemptId(r.get(0)?), status: r.get(1)?, started_at: r.get(2)?, finished_at: r.get(3)?,
                }))?.collect::<Result<Vec<_>, _>>()?;
            let reason = task_blocked_reason(&tx, run_id, &task.node.task_id, now)?;
            let retry = retry_eligible(&tx, &task.node.task_id)?;
            let output_refs = tx.prepare("SELECT a.artifact_id,a.kind FROM rebuild_attempt_outputs o JOIN rebuild_artifacts a ON a.artifact_id=o.artifact_id JOIN rebuild_attempts p ON p.attempt_id=o.attempt_id WHERE p.task_id=?1 ORDER BY o.event_id")?
                .query_map(params![task.node.task_id.0], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?.into_iter().map(|(id,kind)| Ok(ArtifactRef {artifact_id:ArtifactId(ContentHash::new(id)?),kind:parse_enum(&kind)?})).collect::<StoreResult<Vec<_>>>()?;
            let role = task.node.recipe_id.as_str().to_owned();
            let horizon = task
                .node
                .objective
                .split("[research_horizon=")
                .nth(1)
                .and_then(|s| s.split(']').next())
                .map(str::to_owned)
                .or_else(|| {
                    ["t1", "t3", "t5"]
                        .into_iter()
                        .find(|h| task.node.objective.contains(&format!(" {h} Claim")))
                        .map(str::to_owned)
                });
            let business_ready = reason.is_none();
            let step_eligible = business_ready && session.status == DebugStatus::Paused;
            let blocked_reason = reason.or_else(|| {
                (!step_eligible).then(|| format!("debug_{:?}", session.status).to_lowercase())
            });
            let budget = inspect_budget(&tx, &task, attempt_id)?;
            nodes.push(DebugNodeView {
                task,
                role,
                horizon,
                attempts,
                business_ready,
                step_eligible,
                retry_eligible: step_eligible && retry,
                blocked_reason,
                output_refs,
                budget,
            });
        }
        let events = tx.prepare("SELECT event_id,run_id,task_id,attempt_id,event_type,artifact_id,created_at FROM rebuild_events WHERE run_id=?1 AND (?2 IS NULL OR task_id=?2) AND (?3 IS NULL OR attempt_id=?3) ORDER BY event_id")?
            .query_map(params![run_id.0,task_id.map(|t|t.0.as_str()),attempt_id.map(|a|a.0.as_str())], stored_event_from_row)?
            .collect::<Result<Vec<_>,_>>()?;
        let mut ids = BTreeSet::new();
        let mut artifacts = Vec::new();
        let mut acceptance = Vec::new();
        for id in events.iter().filter_map(|e| e.artifact_id.as_ref()) {
            if !ids.insert(id.clone()) {
                continue;
            }
            let artifact = read_artifact(&tx, id)?;
            if artifact.kind == ArtifactKind::RawEvidence {
                continue;
            }
            let bytes = blob::read_blob_bytes(&tx, &artifact.blob.hash, artifact.blob.bytes)?;
            let mut payload: serde_json::Value =
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
            redact(&mut payload);
            if artifact.producer == "debug.stage_acceptance" {
                acceptance.push(serde_json::from_value(payload.clone())?);
            }
            artifacts.push(DebugArtifactView { artifact, payload });
        }
        let allowed_actions = match session.status {
            DebugStatus::Paused => vec!["resume", "step", "retry_node", "abort"],
            DebugStatus::Running | DebugStatus::Stepping => vec!["pause"],
            _ => vec![],
        }
        .into_iter()
        .map(str::to_owned)
        .collect();
        let execution_evidence = if session.identity.run_purpose == RunPurpose::PositionPlan {
            "not_applicable"
        } else if let Some(node) = nodes.iter().find(|n| n.role == "gate.execution") {
            match node.task.status {
                TaskStatus::Running => "refreshing",
                TaskStatus::Succeeded => {
                    if artifacts.iter().any(|a| {
                        a.artifact.kind == ArtifactKind::ExecutionVerdict
                            && a.artifact.origin.as_ref().and_then(|o| o.task_id.as_ref())
                                == Some(&node.task.node.task_id)
                            && a.payload["verdict"] == "accepted"
                    }) {
                        "pass"
                    } else {
                        "blocked"
                    }
                }
                TaskStatus::Failed => "blocked",
                _ if !node.attempts.is_empty() => "blocked",
                _ => "deferred",
            }
        } else {
            "not_applicable"
        }
        .to_owned();
        Ok(DebugRunView {
            execution_evidence,
            session,
            workflow_status: snapshot.status,
            nodes,
            events,
            artifacts,
            acceptance,
            observed_at: now,
            allowed_actions,
        })
    }

    /// Observation only: Agent recovery continues to use AgentTurn and Tool records.
    pub fn observe_debug_budget(
        &self,
        permit: &TaskWritePermit,
        snapshot: &serde_json::Value,
        now: DateTime<Utc>,
    ) -> StoreResult<()> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if read_session(&tx, &permit.run_id)?.is_none() {
            return Ok(());
        }
        assert_permit(&tx, permit)?;
        let artifact = detail_artifact(
            &tx,
            snapshot,
            "debug.budget_snapshot",
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            vec![],
            now,
        )?;
        insert_artifact(&tx, &artifact)?;
        append_event(
            &tx,
            &permit.run_id,
            Some(&permit.task_id),
            Some(&permit.attempt_id),
            LifecycleEventType::DebugBudgetObserved,
            Some(&artifact.artifact_id),
            now,
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn record_stage_acceptance(
        &self,
        acceptance: &StageAcceptance,
    ) -> StoreResult<ArtifactRef> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM rebuild_attempts WHERE run_id=?1 AND task_id=?2 AND attempt_id=?3)",
            params![acceptance.run_id.0,acceptance.task_id.0,acceptance.attempt_id.0], |r|r.get(0))?;
        if !exists {
            return Err(blocked("acceptance_attempt_lineage_mismatch"));
        }
        let reference = write_acceptance(&tx, acceptance)?;
        tx.commit()?;
        Ok(reference)
    }
}

pub(super) fn environment_identity(connection: &Connection) -> StoreResult<Option<String>> {
    Ok(connection
        .query_row(
            "SELECT value FROM rebuild_metadata WHERE key='debug_environment'",
            [],
            |r| r.get(0),
        )
        .optional()?)
}

fn validate_identity(
    tx: &Transaction<'_>,
    identity: &DebugSessionIdentity,
    workflow: &WorkflowCommit,
) -> StoreResult<()> {
    if environment_identity(tx)?.as_deref() != Some(identity.store_identity.as_str())
        || identity.run_id != workflow.run.run_id
        || identity.run_purpose != workflow.run.purpose
        || identity.learning_scope != DebugLearningScope::Isolated
        || identity.version != 1
        || identity.code_revision.trim().is_empty()
    {
        return Err(blocked("invalid_isolated_identity"));
    }
    let contracts = workflow
        .nodes
        .iter()
        .filter_map(|n| n.contract_hash.clone())
        .collect::<BTreeSet<_>>();
    if contracts != identity.contract_hashes.iter().cloned().collect() {
        return Err(blocked("contract_identity_mismatch"));
    }
    Ok(())
}

fn insert_session(tx: &Transaction<'_>, identity: &DebugSessionIdentity) -> StoreResult<()> {
    let mut sources = identity.dataset.clone();
    sources.extend(identity.parent_artifacts.iter().cloned());
    sources.sort();
    sources.dedup();
    let artifact = detail_artifact(
        tx,
        identity,
        "debug.session_identity",
        &identity.run_id,
        None,
        None,
        sources,
        identity.created_at,
    )?;
    insert_artifact(tx, &artifact)?;
    tx.execute("INSERT INTO rebuild_debug_sessions(run_id,identity_artifact_id,runtime_identity,revision,status,execution_mode,updated_at) VALUES(?1,?2,?4,0,'paused','manual',?3)",
        params![identity.run_id.0,artifact.artifact_id.0.as_str(),identity.created_at.to_rfc3339(),identity.runtime_identity.as_str()])?;
    // The same exact lineage rule is used at publication and by Doctor. This
    // records experiment provenance; it grants no Context access to the parent.
    for reference in &artifact.source_refs {
        let parent = read_artifact(tx, &reference.artifact_id)?;
        if parent.origin.as_ref().and_then(|o| o.run_id.as_ref()) != Some(&identity.run_id)
            && !cross_run_reference_allowed(tx, &artifact, &parent)?
        {
            return Err(blocked("invalid_debug_parent_lineage"));
        }
    }
    for reference in &identity.dataset {
        let need = read_artifact(tx, &reference.artifact_id)?;
        for source in &need.source_refs {
            let parent = read_artifact(tx, &source.artifact_id)?;
            if parent.origin.as_ref().and_then(|o| o.run_id.as_ref()) != Some(&identity.run_id)
                && !cross_run_reference_allowed(tx, &need, &parent)?
            {
                return Err(blocked("invalid_debug_dataset_lineage"));
            }
        }
    }
    append_event(
        tx,
        &identity.run_id,
        None,
        None,
        LifecycleEventType::DebugControlChanged,
        Some(&artifact.artifact_id),
        identity.created_at,
    )?;
    Ok(())
}

/// An explicit experiment edge is provenance, never an Agent read grant.
pub(super) fn cross_run_reference_allowed(
    connection: &Connection,
    child: &Artifact,
    parent: &Artifact,
) -> StoreResult<bool> {
    let Some(origin) = child.origin.as_ref() else {
        return Ok(false);
    };
    let Some(run) = origin.run_id.as_ref() else {
        return Ok(false);
    };
    if origin.task_id.is_some() || origin.attempt_id.is_some() {
        return Ok(false);
    }
    let Some(session) = read_session(connection, run)? else {
        return Ok(false);
    };
    let identity = session.identity;
    // WorkflowGraph CAS is shared and has no origin. Its owning Run is proved
    // by rebuild_runs.graph_artifact_id below, never guessed from content.
    let parent_run = parent
        .origin
        .as_ref()
        .and_then(|o| o.run_id.as_ref())
        .or_else(|| {
            (parent.kind == ArtifactKind::WorkflowGraph)
                .then_some(identity.parent_run_id.as_ref())
                .flatten()
        });
    let Some(parent_run) = parent_run else {
        return Ok(false);
    };
    let Some(source) = read_session(connection, parent_run)? else {
        return Ok(false);
    };
    if run == parent_run {
        return Ok(false);
    }
    if identity.parent_run_id.as_ref() != Some(parent_run)
        || identity.learning_scope != DebugLearningScope::Isolated
        || identity.store_identity != source.identity.store_identity
        || environment_identity(connection)?.as_ref() != Some(&identity.store_identity)
    {
        return Ok(false);
    }
    let reference = ArtifactRef {
        artifact_id: parent.artifact_id.clone(),
        kind: parent.kind,
    };
    if child.kind == ArtifactKind::EvidenceNeed {
        return Ok(parent.kind == ArtifactKind::EvidenceNeed
            && child.producer == "scheduler.paper_snapshot"
            && parent.producer == child.producer
            && child.blob == parent.blob
            && child.provenance == parent.provenance
            && child.source_refs == vec![reference.clone()]
            && source.identity.dataset.contains(&reference)
            && identity.dataset.contains(&ArtifactRef {
                artifact_id: child.artifact_id.clone(),
                kind: child.kind,
            }));
    }
    if child.kind != ArtifactKind::DebugRecord
        || child.producer != "debug.session_identity"
        || child.provenance.source_family != "akzio.debug"
        || !identity.parent_artifacts.contains(&reference)
    {
        return Ok(false);
    }
    let registered: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_debug_sessions WHERE run_id=?1 AND identity_artifact_id=?2)",
        params![run.0,child.artifact_id.0.as_str()],|r|r.get(0))?;
    if !registered {
        return Ok(false);
    }
    match identity.parent_task_id {
        Some(task) => Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_attempt_outputs o JOIN rebuild_attempts a ON a.attempt_id=o.attempt_id JOIN rebuild_tasks t ON t.task_id=a.task_id WHERE a.run_id=?1 AND a.task_id=?2 AND a.status='succeeded' AND t.status='succeeded' AND o.artifact_id=?3)",
            params![parent_run.0,task.0,parent.artifact_id.0.as_str()],|r|r.get(0))?),
        None => Ok(parent.kind == ArtifactKind::WorkflowGraph && connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM rebuild_runs WHERE run_id=?1 AND graph_artifact_id=?2)",
            params![parent_run.0,parent.artifact_id.0.as_str()],|r|r.get::<_,bool>(0))?),
    }
}

pub(super) fn read_session(
    connection: &Connection,
    run_id: &RunId,
) -> StoreResult<Option<DebugSession>> {
    let row=connection.query_row("SELECT identity_artifact_id,revision,status,execution_mode,permitted_task_id,active_attempt_id,paused_at_task_id,updated_at,runtime_identity FROM rebuild_debug_sessions WHERE run_id=?1",params![run_id.0],|r|Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,Option<String>>(5)?,r.get::<_,Option<String>>(6)?,r.get::<_,String>(7)?,r.get::<_,String>(8)?))).optional()?;
    row.map(
        |(id, revision, status, mode, task, attempt, paused, updated, runtime)| {
            let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(id)?))?;
            let identity: DebugSessionIdentity = serde_json::from_slice(&blob::read_blob_bytes(
                connection,
                &artifact.blob.hash,
                artifact.blob.bytes,
            )?)?;
            if identity.runtime_identity.as_str() != runtime || identity.run_id != *run_id {
                return Err(StoreError::Integrity(
                    "Debug identity head differs from CAS identity".into(),
                ));
            }
            Ok(DebugSession {
                identity,
                revision,
                status: parse_enum(&status)?,
                execution_mode: parse_enum(&mode)?,
                permitted_task_id: task.map(TaskId),
                active_attempt_id: attempt.map(AttemptId),
                paused_at_task_id: paused.map(TaskId),
                updated_at: parse_time(&updated)?,
            })
        },
    )
    .transpose()
}

fn save_session(
    tx: &Transaction<'_>,
    session: &mut DebugSession,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let old = session.revision;
    session.revision += 1;
    session.updated_at = now;
    let changed=tx.execute("UPDATE rebuild_debug_sessions SET revision=?1,status=?2,execution_mode=?3,permitted_task_id=?4,active_attempt_id=?5,paused_at_task_id=?6,updated_at=?7 WHERE run_id=?8 AND revision=?9",params![session.revision,enum_name(session.status),enum_name(session.execution_mode),session.permitted_task_id.as_ref().map(|x|x.0.as_str()),session.active_attempt_id.as_ref().map(|x|x.0.as_str()),session.paused_at_task_id.as_ref().map(|x|x.0.as_str()),now.to_rfc3339(),session.identity.run_id.0,old])?;
    if changed != 1 {
        return Err(blocked("revision_conflict"));
    }
    let artifact = detail_artifact(
        tx,
        session,
        "debug.control",
        &session.identity.run_id,
        None,
        None,
        vec![],
        now,
    )?;
    insert_artifact(tx, &artifact)?;
    append_event(
        tx,
        &session.identity.run_id,
        None,
        None,
        LifecycleEventType::DebugControlChanged,
        Some(&artifact.artifact_id),
        now,
    )?;
    Ok(())
}

fn running_count(connection: &Connection, run_id: &RunId) -> StoreResult<u64> {
    Ok(connection.query_row(
        "SELECT count(*) FROM rebuild_tasks WHERE run_id=?1 AND status='running'",
        params![run_id.0],
        |r| r.get(0),
    )?)
}

fn task_blocked_reason(
    connection: &Connection,
    run_id: &RunId,
    task_id: &TaskId,
    now: DateTime<Utc>,
) -> StoreResult<Option<String>> {
    let row=connection.query_row("SELECT t.status,t.ready_at,r.status,t.recipe_id FROM rebuild_tasks t JOIN rebuild_runs r ON r.run_id=t.run_id WHERE t.run_id=?1 AND t.task_id=?2",params![run_id.0,task_id.0],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?))).optional()?.ok_or_else(||StoreError::MissingTask(task_id.clone()))?;
    if row.0 != "queued" {
        return Ok(Some(format!("task_{}", row.0)));
    }
    let dependencies:u64=connection.query_row("SELECT count(*) FROM rebuild_task_dependencies d JOIN rebuild_tasks p ON p.task_id=d.depends_on_task_id WHERE d.task_id=?1 AND p.status NOT IN ('succeeded','skipped')",params![task_id.0],|r|r.get(0))?;
    if dependencies > 0 {
        return Ok(Some("dependencies_not_satisfied".into()));
    }
    let run_runnable = matches!(row.2.as_str(), "queued" | "running")
        || (row.2 == "completed" && row.3 == POST_TERMINAL_WORKER_RECIPE_ID);
    if !run_runnable {
        return Ok(Some(format!("run_{}", row.2)));
    }
    if parse_time(&row.1)? > now {
        return Ok(Some(format!("not_due_until:{}", row.1)));
    }
    let cancelled: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM rebuild_run_cancellations WHERE run_id=?1)",
        params![run_id.0],
        |r| r.get(0),
    )?;
    Ok(cancelled.then(|| "run_cancel_requested".into()))
}

fn retry_eligible(connection: &Connection, task_id: &TaskId) -> StoreResult<bool> {
    let last = connection
        .query_row(
            "SELECT status FROM rebuild_attempts WHERE task_id=?1 ORDER BY epoch DESC LIMIT 1",
            params![task_id.0],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    Ok(last.is_some_and(|s| matches!(s.as_str(), "retried" | "abandoned")))
}

fn inspect_budget(
    connection: &Connection,
    task: &StoredTaskSnapshot,
    attempt_id: Option<&AttemptId>,
) -> StoreResult<serde_json::Value> {
    let latest:Option<String>=connection.query_row("SELECT artifact_id FROM rebuild_events WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) AND event_type='debug.budget_observed' ORDER BY event_id DESC LIMIT 1",params![task.node.task_id.0, attempt_id.map(|id| id.0.as_str())],|r|r.get(0)).optional()?;
    if let Some(id) = latest {
        let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(id)?))?;
        let payload: serde_json::Value = serde_json::from_slice(&blob::read_blob_bytes(
            connection,
            &artifact.blob.hash,
            artifact.blob.bytes,
        )?)?;
        return Ok(
            serde_json::json!({"resolved":task.node.budget,"limits":task.node.budget,"last_runtime_observation":payload,"observed_at":artifact.created_at,"attempt":artifact.origin.and_then(|o|o.attempt_id),"authority":"AgentRuntime audit snapshot; recovery still derives from AgentTurn and Tool lifecycle"}),
        );
    }
    let ids=connection.prepare("SELECT DISTINCT artifact_id FROM rebuild_events WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) AND event_type IN ('agent.turn_completed','agent.turn_failed','agent.turn_retryable_failed') AND artifact_id IS NOT NULL")?
        .query_map(params![task.node.task_id.0, attempt_id.map(|id| id.0.as_str())],|r|r.get::<_,String>(0))?.collect::<Result<Vec<_>,_>>()?;
    let mut input = 0u64;
    let mut output = 0u64;
    let mut latency = 0u64;
    let mut complete = true;
    for id in &ids {
        let artifact = read_artifact(connection, &ArtifactId(ContentHash::new(id)?))?;
        let value: serde_json::Value = serde_json::from_slice(&blob::read_blob_bytes(
            connection,
            &artifact.blob.hash,
            artifact.blob.bytes,
        )?)?;
        if let (Some(i), Some(o), Some(l)) = (
            value
                .pointer("/response/telemetry/input_tokens")
                .and_then(serde_json::Value::as_u64),
            value
                .pointer("/response/telemetry/output_tokens")
                .and_then(serde_json::Value::as_u64),
            value
                .pointer("/response/telemetry/latency_millis")
                .and_then(serde_json::Value::as_u64),
        ) {
            input = input.saturating_add(i);
            output = output.saturating_add(o);
            latency = latency.saturating_add(l);
        } else {
            complete = false;
        }
    }
    let calls: u64 = connection.query_row(
        "SELECT count(*) FROM rebuild_events WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) AND event_type='agent.turn_started'",
        params![task.node.task_id.0, attempt_id.map(|id| id.0.as_str())],
        |r| r.get(0),
    )?;
    let tools: u64 = connection.query_row(
        "SELECT count(*) FROM rebuild_events WHERE task_id=?1 AND (?2 IS NULL OR attempt_id=?2) AND event_type='tool.called'",
        params![task.node.task_id.0, attempt_id.map(|id| id.0.as_str())],
        |r| r.get(0),
    )?;
    complete = complete && calls == ids.len() as u64;
    // Outcome budgets reset at committed horizon boundaries, so whole-task
    // lifetime usage is not presented as a current-horizon remainder.
    let remainder_known = calls == 0 && tools == 0;
    Ok(
        serde_json::json!({"resolved":task.node.budget,"limits":task.node.budget,"scope":"task_lifetime_observed_provider_usage","usage_complete":complete,
        "input_tokens_used":complete.then_some(input),"output_tokens_used":complete.then_some(output),"provider_latency_millis":complete.then_some(latency),
        "input_tokens_remaining":remainder_known.then_some(u64::from(task.node.budget.max_input_tokens).saturating_sub(input)),
        "output_tokens_remaining":remainder_known.then_some(u64::from(task.node.budget.max_output_tokens).saturating_sub(output)),
        "tool_calls_used":tools,"provider_calls_started":calls,"tool_calls_remaining":remainder_known.then(|| task.node.budget.max_tool_calls.remaining(tools)).flatten(),
        "authority":"AgentRuntime; missing usage and post-terminal stage remainder remain unknown"}),
    )
}

pub(super) fn consume_claim(
    tx: &Transaction<'_>,
    permit: &TaskWritePermit,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    if let Some(mut session) = read_session(tx, &permit.run_id)? {
        if session.status == DebugStatus::Stepping {
            if session.permitted_task_id.as_ref() != Some(&permit.task_id)
                || session.active_attempt_id.is_some()
            {
                return Err(blocked("step_permit_not_available"));
            }
            session.permitted_task_id = None;
            session.active_attempt_id = Some(permit.attempt_id.clone());
            save_session(tx, &mut session, now)?;
        } else if session.status != DebugStatus::Running {
            return Err(blocked("run_is_paused"));
        }
    }
    Ok(())
}

/// Called in every attempt-closing transaction, including defer and recovery.
pub(super) fn settle_attempt(
    tx: &Transaction<'_>,
    permit: &TaskWritePermit,
    result: &str,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    let Some(mut session) = read_session(tx, &permit.run_id)? else {
        return Ok(());
    };
    let acceptance = StageAcceptance {
        version: 1,
        run_id: permit.run_id.clone(),
        task_id: permit.task_id.clone(),
        attempt_id: permit.attempt_id.clone(),
        stage: tx.query_row(
            "SELECT recipe_id FROM rebuild_tasks WHERE task_id=?1",
            params![permit.task_id.0],
            |r| r.get(0),
        )?,
        business_result: result.into(),
        test_result: AcceptanceResult::NotRun,
        checks: vec![AcceptanceCheck {
            check_id: "controller.attempt_boundary".into(),
            category: AcceptanceCategory::Persistence,
            expected: "attempt closes within the Store transaction".into(),
            actual: result.into(),
            result: AcceptanceResult::Pass,
            evidence_refs: vec![],
            message: "Controller boundary verified; business acceptance checks have not been run."
                .into(),
        }],
        created_at: now,
    };
    write_acceptance(tx, &acceptance)?;
    if (session.status == DebugStatus::Stepping
        && session.active_attempt_id.as_ref() == Some(&permit.attempt_id))
        || session.status == DebugStatus::PauseRequested && running_count(tx, &permit.run_id)? == 0
    {
        session.status = DebugStatus::Paused;
        session.permitted_task_id = None;
        session.active_attempt_id = None;
        session.paused_at_task_id = Some(permit.task_id.clone());
        save_session(tx, &mut session, now)?;
    } else if session.status == DebugStatus::Running && running_count(tx, &permit.run_id)? == 0 {
        let pending: u64 = tx.query_row(
            "SELECT count(*) FROM rebuild_tasks WHERE run_id=?1 AND status='queued'",
            params![permit.run_id.0],
            |r| r.get(0),
        )?;
        if pending == 0 {
            session.status = DebugStatus::Completed;
            save_session(tx, &mut session, now)?;
        }
    }
    Ok(())
}

pub(super) fn assert_broker_write(connection: &Connection, run_id: &RunId) -> StoreResult<()> {
    let session = read_session(connection, run_id)?;
    if session
        .as_ref()
        .is_some_and(|s| s.identity.broker_write_policy == DebugBrokerPolicy::Forbidden)
        || environment_identity(connection)?.is_some() && session.is_none()
    {
        return Err(StoreError::DebugBrokerWriteForbidden);
    }
    Ok(())
}

pub(super) fn post_terminal_enqueued(
    tx: &Transaction<'_>,
    run_id: &RunId,
    now: DateTime<Utc>,
) -> StoreResult<()> {
    if let Some(mut session) = read_session(tx, run_id)? {
        if session.status == DebugStatus::Completed {
            session.status = if session.execution_mode == DebugExecutionMode::Continuous {
                DebugStatus::Running
            } else {
                DebugStatus::Paused
            };
            save_session(tx, &mut session, now)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn detail_artifact<T: Serialize>(
    connection: &Connection,
    payload: &T,
    producer: &str,
    run_id: &RunId,
    task_id: Option<&TaskId>,
    attempt_id: Option<&AttemptId>,
    sources: Vec<ArtifactRef>,
    now: DateTime<Utc>,
) -> StoreResult<Artifact> {
    Ok(Artifact::new(
        ArtifactKind::DebugRecord,
        blob::stage_blob_bytes(
            connection,
            &serde_json::to_vec(payload)?,
            "application/json".into(),
        )?,
        producer,
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.debug".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: task_id.cloned(),
            attempt_id: attempt_id.cloned(),
            contract_hash: None,
        }),
        sources,
        now,
    )?)
}

fn write_acceptance(tx: &Transaction<'_>, value: &StageAcceptance) -> StoreResult<ArtifactRef> {
    if value.version != 1
        || value.stage.is_empty()
        || value.checks.iter().any(|c| c.check_id.is_empty())
    {
        return Err(blocked("invalid_stage_acceptance"));
    }
    let sources = value
        .checks
        .iter()
        .flat_map(|c| c.evidence_refs.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let artifact = detail_artifact(
        tx,
        value,
        "debug.stage_acceptance",
        &value.run_id,
        Some(&value.task_id),
        Some(&value.attempt_id),
        sources,
        value.created_at,
    )?;
    insert_artifact(tx, &artifact)?;
    let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE event_type='debug.acceptance_recorded' AND artifact_id=?1)",params![artifact.artifact_id.0.as_str()],|r|r.get(0))?;
    if !exists {
        append_event(
            tx,
            &value.run_id,
            Some(&value.task_id),
            Some(&value.attempt_id),
            LifecycleEventType::StageAcceptanceRecorded,
            Some(&artifact.artifact_id),
            value.created_at,
        )?;
    }
    Ok(ArtifactRef {
        artifact_id: artifact.artifact_id,
        kind: artifact.kind,
    })
}

/// Exact credential field names, independent of token-usage field names.
#[allow(clippy::collapsible_match)]
fn redact(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                let key = key.to_ascii_lowercase();
                if [
                    "api_key",
                    "apikey",
                    "authorization",
                    "auth_header",
                    "secret",
                    "password",
                    "credential",
                    "access_token",
                    "refresh_token",
                    "token",
                    "headers",
                    "encrypted_content",
                ]
                .iter()
                .any(|s| key == *s || key.ends_with(&format!("_{s}")))
                {
                    *value = serde_json::Value::String("[REDACTED]".into());
                } else {
                    redact(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                redact(value);
            }
        }
        serde_json::Value::String(text) => {
            // Never project provider debug/raw payloads containing credential text.
            if [
                "Bearer ",
                " sk-",
                "\"sk-",
                "APCA-API-SECRET-KEY",
                "api_key=",
                "api_secret=",
            ]
            .iter()
            .any(|s| text.contains(s))
                || text.starts_with("sk-")
            {
                *text = "[REDACTED credential-bearing text]".into();
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inspect_redacts_credentials_but_retains_usage_and_risk_prose() {
        let mut value = serde_json::json!({"api_key":"secret","nested":{"Authorization":"Bearer secret","input_tokens":321,"output_tokens":12,"encrypted_content":"opaque"},"memo":"risk-aware research","error":"Bearer secret"});
        redact(&mut value);
        assert_eq!(value["api_key"], "[REDACTED]");
        assert_eq!(value["nested"]["Authorization"], "[REDACTED]");
        assert_eq!(value["nested"]["encrypted_content"], "[REDACTED]");
        assert_eq!(value["nested"]["input_tokens"], 321);
        assert_eq!(value["memo"], "risk-aware research");
        assert!(!value["error"].as_str().unwrap().contains("secret"));
    }
}
