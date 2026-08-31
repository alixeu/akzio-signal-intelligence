// Which acquisition mode dispatch asks for, and what canonical Paper does with
// the acquisition identity it finds on the evidence it reads back.

/// Records the acquisition mode dispatch requested per resource and returns
/// deterministic local payloads. It performs no network or model work.
#[derive(Clone)]
struct ModeRecordingAdapter {
    source: EvidenceSource,
    observed_at: DateTime<Utc>,
    recorded: Arc<Mutex<Vec<(String, EvidenceAcquisitionMode)>>>,
    news_document: Option<serde_json::Value>,
}

impl ModeRecordingAdapter {
    fn new(source: EvidenceSource, observed_at: DateTime<Utc>) -> Self {
        Self {
            source,
            observed_at,
            recorded: Arc::new(Mutex::new(Vec::new())),
            news_document: None,
        }
    }

    /// Attach the `source_document` block a real native-web acquisition would
    /// have written, so canonical Paper consumption has an identity to check.
    fn with_news_document(mut self, document: serde_json::Value) -> Self {
        self.news_document = Some(document);
        self
    }

    fn recorded_mode(&self, resource: &str) -> Option<EvidenceAcquisitionMode> {
        self.recorded
            .lock()
            .unwrap()
            .iter()
            .find(|(recorded, _)| recorded == resource)
            .map(|(_, mode)| *mode)
    }
}

impl AsyncEvidenceAdapter for ModeRecordingAdapter {
    fn source(&self) -> EvidenceSource {
        self.source
    }

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, std::result::Result<AcquiredEvidence, EvidenceAdapterError>> {
        self.recorded
            .lock()
            .unwrap()
            .push((request.resource.clone(), request.acquisition_mode));
        let mut evidence = debug_fixture_evidence(self.source, &request.resource, self.observed_at);
        if let Some(document) = self.news_document.clone() {
            evidence
                .normalized
                .as_object_mut()
                .unwrap()
                .insert("source_document".to_owned(), document);
            evidence.quality.citations_complete = false;
            evidence.quality.completeness_ppm = 0;
        }
        Box::pin(async move { Ok(evidence) })
    }
}

fn verified_news_document() -> serde_json::Value {
    serde_json::json!({
        "status": "source_snapshots_partial",
        "acquisition_mode": EvidenceAcquisitionMode::VerifiedSource.as_str(),
        "acquisition_policy_version": akzio_domain::EVIDENCE_ACQUISITION_POLICY_VERSION,
        "acquisition_policy_hash": akzio_domain::evidence_acquisition_policy_hash().to_string(),
        "source_closure": "partial",
        "required_source_count": 2,
        "verified_source_count": 1,
        "fetch_count": 2,
        "exact_quote_count": 1,
    })
}

/// Install production adapters for every family a Paper session needs, with
/// `fixture_mode` off so dispatch has to choose an acquisition mode.
fn install_production_recorders(
    daemon: &mut Daemon,
    now: DateTime<Utc>,
    news_document: Option<serde_json::Value>,
) -> (
    ModeRecordingAdapter,
    ModeRecordingAdapter,
    ModeRecordingAdapter,
) {
    let alpaca = ModeRecordingAdapter::new(EvidenceSource::Alpaca, now);
    let fred = ModeRecordingAdapter::new(EvidenceSource::Fred, now);
    let mut news = ModeRecordingAdapter::new(EvidenceSource::NewsWeb, now);
    if let Some(document) = news_document {
        news = news.with_news_document(document);
    }
    daemon.fixture_mode = false;
    daemon.production_evidence = Arc::new(BTreeMap::from([
        (
            EvidenceSource::Alpaca,
            Arc::new(alpaca.clone()) as Arc<dyn AsyncEvidenceAdapter>,
        ),
        (
            EvidenceSource::Fred,
            Arc::new(fred.clone()) as Arc<dyn AsyncEvidenceAdapter>,
        ),
        (
            EvidenceSource::NewsWeb,
            Arc::new(news.clone()) as Arc<dyn AsyncEvidenceAdapter>,
        ),
    ]));
    (alpaca, fred, news)
}

async fn paper_evidence_run(
    directory: &std::path::Path,
    now: DateTime<Utc>,
    news_document: Option<serde_json::Value>,
) -> (
    Daemon,
    ClaimedAttempt,
    ModeRecordingAdapter,
    ModeRecordingAdapter,
) {
    let session_key = now.date_naive().to_string();
    let mut daemon = Daemon::with_fixture_evidence(
        config(directory.to_path_buf()),
        scheduler_fixture_model_client(),
        BTreeMap::new(),
    )
    .unwrap();
    let paper_run_id = RunId::new();
    let setup_artifacts = paper_session_evidence_needs(&session_key)
        .iter()
        .map(|need| scheduler_snapshot_need(daemon.store(), &paper_run_id, &need.resource, now))
        .collect::<Vec<_>>();
    let snapshot_refs = setup_artifacts
        .iter()
        .map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        })
        .collect::<Vec<_>>();
    let mut proposal = paper_proposal();
    proposal.tasks.insert(
        "analyst".to_owned(),
        WorkflowProposalTask {
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            objective: "Assess governed Paper evidence".to_owned(),
            depends_on: vec![],
            priority: 90,
            evidence_needs: snapshot_refs,
        },
    );
    proposal.tasks.get_mut("synthesizer").unwrap().depends_on = vec!["analyst".to_owned()];
    daemon
        .reserve_paper_session_with_inputs_for_run(
            paper_run_id,
            &session_key,
            &proposal,
            &setup_artifacts,
            now,
        )
        .unwrap();
    let (_, fred, news) = install_production_recorders(&mut daemon, now, news_document);
    let task = daemon
        .store()
        .claim_next_task("acquisition-mode-worker", now, ChronoDuration::seconds(30))
        .unwrap()
        .unwrap();
    (daemon, task, fred, news)
}

#[tokio::test]
async fn canonical_paper_news_evidence_requests_verified_source() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let news_start = (now.date_naive() - ChronoDuration::days(14)).to_string();
    let (daemon, task, fred, news) =
        paper_evidence_run(directory.path(), now, Some(verified_news_document())).await;

    daemon.acquire_evidence(&task, now).await.unwrap();

    let news_resource = format!("news:QQQ:{news_start}:{session_key}:market");
    assert_eq!(
        news.recorded_mode(&news_resource),
        Some(EvidenceAcquisitionMode::VerifiedSource),
        "canonical Paper news must be independently verified"
    );
    assert_eq!(
        fred.recorded_mode(&format!("series:DFF:{}:{session_key}", {
            (now.date_naive() - ChronoDuration::days(366)).to_string()
        })),
        Some(EvidenceAcquisitionMode::VerifiedSource),
        "direct API families are their own independent source"
    );
}

#[tokio::test]
async fn canonical_paper_rejects_news_acquired_under_another_policy() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let mut document = verified_news_document();
    document["acquisition_policy_hash"] = serde_json::json!("stale-policy-hash");
    let (daemon, task, _, _) = paper_evidence_run(directory.path(), now, Some(document)).await;

    let error = daemon.acquire_evidence(&task, now).await.unwrap_err();

    assert!(
        matches!(&error, DaemonError::InvalidInput(message)
            if message.contains("different acquisition policy")),
        "policy drift must fail closed, got {error:?}"
    );
}

#[tokio::test]
async fn canonical_paper_rejects_discovery_only_news_evidence() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let mut document = verified_news_document();
    document["acquisition_mode"] =
        serde_json::json!(EvidenceAcquisitionMode::DiscoveryOnly.as_str());
    document["source_closure"] = serde_json::json!("provider_attributed");
    let (daemon, task, _, _) = paper_evidence_run(directory.path(), now, Some(document)).await;

    let error = daemon.acquire_evidence(&task, now).await.unwrap_err();

    assert!(
        matches!(&error, DaemonError::InvalidInput(message)
            if message.contains("acquired as discovery_only")),
        "provider-attributed news must not satisfy canonical Paper, got {error:?}"
    );
}

/// Partial source closure is recorded, not fatal: the evidence stays sealed with
/// `citations_complete == false`, which is the signal the research layer already
/// uses to refuse a directional ground.
#[tokio::test]
async fn partial_news_closure_stays_acquired_but_not_citation_complete() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let news_start = (now.date_naive() - ChronoDuration::days(14)).to_string();
    let (daemon, task, _, _) =
        paper_evidence_run(directory.path(), now, Some(verified_news_document())).await;

    let artifacts = daemon.acquire_evidence(&task, now).await.unwrap();

    let news_resource = format!("news:QQQ:{news_start}:{session_key}:market");
    let payload = artifacts
        .iter()
        .filter(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence)
        .filter_map(|artifact| daemon.store().read_blob(&artifact.blob).ok())
        .filter_map(|bytes| serde_json::from_slice::<NormalizedEvidencePayload>(&bytes).ok())
        .find(|payload| payload.resource == news_resource)
        .expect("news evidence is sealed even when its closure is partial");

    assert!(!payload.quality.citations_complete);
    assert_eq!(payload.value["source_document"]["source_closure"], "partial");
    assert_eq!(payload.value["source_document"]["verified_source_count"], 1);
    assert_eq!(payload.value["source_document"]["required_source_count"], 2);
}

/// Fixture and Debug evidence never claims an acquisition identity, and dispatch
/// asks for discovery instead of an independent fetch.
#[tokio::test]
async fn noncanonical_news_evidence_requests_discovery_only() {
    let directory = tempdir().unwrap();
    let mut daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        scheduler_fixture_model_client(),
        BTreeMap::new(),
    )
    .unwrap();
    let run_id = daemon.submit_default(RunPurpose::Debug).unwrap();
    // Claimed after submission, so the workflow's own submit timestamp is in the past.
    let now = Utc::now();
    let (_, _, news) = install_production_recorders(&mut daemon, now, None);
    let mut task = daemon
        .store()
        .claim_next_task("acquisition-mode-worker", now, ChronoDuration::seconds(30))
        .unwrap()
        .unwrap();
    let need = EvidenceNeed {
        schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
        source_family: "news_web".to_owned(),
        resource: "news:QQQ:2026-08-20:2026-08-27:market".to_owned(),
        max_age_secs: 300,
    };
    let need_artifact = Artifact::new(
        ArtifactKind::EvidenceNeed,
        daemon.store().put_json(&need).unwrap(),
        "runtime.planner.evidence_need",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.workflow.planner".to_owned(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: task.permit.contract_hash.clone(),
        },
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: Some(task.permit.task_id.clone()),
            attempt_id: Some(task.permit.attempt_id.clone()),
            contract_hash: task.permit.contract_hash.clone(),
        }),
        Vec::new(),
        now,
    )
    .unwrap();
    daemon
        .store()
        .write_task_artifact(
            &task.permit,
            &need_artifact,
            LifecycleEventType::PlannerEvidenceNeedCreated,
            now,
        )
        .unwrap();
    task.node.input_artifacts = vec![ArtifactRef {
        artifact_id: need_artifact.artifact_id.clone(),
        kind: ArtifactKind::EvidenceNeed,
    }];

    daemon.acquire_evidence(&task, now).await.unwrap();

    assert_eq!(
        news.recorded_mode(&need.resource),
        Some(EvidenceAcquisitionMode::DiscoveryOnly),
        "ordinary research discovery must not trigger independent fetches"
    );
}

/// A canonical Paper news acquisition whose independent verification only closed
/// one of the two sources the answer cited.
fn partial_closure_news_evidence(resource: &str, now: DateTime<Utc>) -> AcquiredEvidence {
    let mut evidence = debug_fixture_evidence(EvidenceSource::NewsWeb, resource, now);
    evidence
        .normalized
        .as_object_mut()
        .unwrap()
        .insert("source_document".to_owned(), verified_news_document());
    evidence.quality.citations_complete = false;
    evidence.quality.completeness_ppm = 500_000;
    evidence
}

/// Characterization, not a policy assertion: it records where the news source
/// closure signal actually stops today. A canonical Paper run whose news
/// evidence closed only partially still seals a canonical Outcome, and the
/// sealed learning input carries no trace of the degraded closure. Closing that
/// boundary is a separate change; this test exists to fail loudly when it is.
#[tokio::test]
async fn partial_news_closure_still_reaches_a_canonical_sealed_outcome() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let session_key = now.date_naive().to_string();
    let needs = paper_session_evidence_needs(&session_key);
    let news_resource = needs
        .iter()
        .find(|need| need.source_family == "news_web")
        .expect("a Paper session must need news evidence")
        .resource
        .clone();
    let mut daemon = Daemon::with_fixture_evidence(
        config(directory.path().to_path_buf()),
        scheduler_fixture_model_client(),
        BTreeMap::from([(
            EvidenceSource::NewsWeb,
            BTreeMap::from([(
                news_resource.clone(),
                partial_closure_news_evidence(&news_resource, now),
            )]),
        )]),
    )
    .unwrap();
    daemon.production_evidence = Arc::new(BTreeMap::from([(
        EvidenceSource::Alpaca,
        Arc::new(
            OutcomeBarsAdapter::new(now.date_naive(), now).with_responses(
                [
                    PAPER_ACCOUNT_RESOURCE.to_owned(),
                    PAPER_POSITIONS_RESOURCE.to_owned(),
                    PAPER_OPEN_ORDERS_RESOURCE.to_owned(),
                    PAPER_QUOTES_RESOURCE.to_owned(),
                    PAPER_CLOCK_RESOURCE.to_owned(),
                    format!("paper.fills:{session_key}"),
                ]
                .into_iter()
                .map(|resource| {
                    let evidence =
                        debug_fixture_evidence(EvidenceSource::Alpaca, &resource, now);
                    (resource, evidence)
                })
                .collect(),
            ),
        ) as Arc<dyn AsyncEvidenceAdapter>,
    )]));
    daemon.outcome_scheduling_runtime =
        OutcomeSchedulingRuntime::new(daemon.store.clone()).with_worker_enabled(true);
    let paper_run_id = RunId::new();
    let setup_artifacts = needs
        .iter()
        .map(|need| scheduler_snapshot_need(daemon.store(), &paper_run_id, &need.resource, now))
        .collect::<Vec<_>>();
    let snapshot_refs = setup_artifacts
        .iter()
        .map(|artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: ArtifactKind::EvidenceNeed,
        })
        .collect::<Vec<_>>();
    let mut proposal = paper_proposal();
    proposal
        .tasks
        .get_mut("synthesizer")
        .unwrap()
        .evidence_needs = snapshot_refs;
    let slot = daemon
        .reserve_paper_session_with_inputs_for_run(
            paper_run_id,
            &session_key,
            &proposal,
            &setup_artifacts,
            now,
        )
        .unwrap();
    let run_id = slot.slot.workflow.run.run_id.clone();

    for _ in 0..64 {
        if !daemon.run_one("partial-closure-paper").await.unwrap() {
            break;
        }
    }

    let snapshot = daemon.store().workflow_snapshot(&run_id).unwrap();
    assert!(
        snapshot
            .tasks
            .iter()
            .all(|task| task.status == TaskStatus::Succeeded),
        "a partial news closure must not fail the task graph, statuses: {:?}",
        snapshot
            .tasks
            .iter()
            .map(|task| format!("{}={:?}", task.node.recipe_id, task.status))
            .collect::<Vec<_>>()
    );
    let evidence_task = snapshot
        .tasks
        .iter()
        .find(|task| {
            task.node.recipe_id.as_str() == akzio_runtime::v2::EVIDENCE_GATE_RECIPE_ID
        })
        .expect("a Paper run keeps its evidence gate")
        .node
        .task_id
        .clone();
    let news_payload = daemon
        .store()
        .committed_task_outputs(&run_id, &evidence_task)
        .unwrap()
        .into_iter()
        .filter(|artifact| artifact.kind == ArtifactKind::NormalizedEvidence)
        .filter_map(|artifact| daemon.store().read_blob(&artifact.blob).ok())
        .filter_map(|bytes| serde_json::from_slice::<NormalizedEvidencePayload>(&bytes).ok())
        .find(|payload| payload.resource == news_resource)
        .expect("the news need is materialized despite its partial closure");
    assert!(!news_payload.quality.citations_complete);
    assert_eq!(
        news_payload.value["source_document"]["source_closure"],
        "partial"
    );

    // The open boundary: nothing between the degraded evidence and canonical
    // learning re-reads that closure.
    let outcome_artifact = daemon
        .store()
        .latest_artifact_by_kind(ArtifactKind::Outcome)
        .unwrap()
        .expect("today a partial news closure still seals a canonical Paper Outcome");
    let outcome_bytes = daemon.store().read_blob(&outcome_artifact.blob).unwrap();
    let outcome_text = String::from_utf8(outcome_bytes).unwrap();
    assert!(
        !outcome_text.contains("source_closure") && !outcome_text.contains("citations_complete"),
        "sealed Outcome unexpectedly carries evidence closure state: {outcome_text}"
    );
    let t5_lifecycle = daemon
        .store()
        .retrospectives(&run_id)
        .unwrap()
        .into_iter()
        .filter_map(|artifact| {
            let payload: Retrospective =
                serde_json::from_slice(&daemon.store().read_blob(&artifact.blob).ok()?).ok()?;
            (payload.horizon == OutcomeHorizon::T5).then_some(artifact.lifecycle)
        })
        .next()
        .expect("the outcome worker reaches T5 on the fixture bars");
    assert_eq!(
        t5_lifecycle,
        ArtifactLifecycle::Canonical,
        "the T5 retrospective is promoted to canonical without consulting news closure"
    );
    daemon.store().verify_integrity().unwrap();
}
