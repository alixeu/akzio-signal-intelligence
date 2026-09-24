//! Governed evidence acquisition and snapshot materialization.

// 文件导读：本文件实现 EvidenceNeed → adapter → Raw/NormalizedEvidence → 执行快照的受控
// 数据流。研究阶段只采集研究证据，并把 ExecutionSafety 标为 deferred；ExecutionGate
// 再独立刷新账户/报价/时钟。provider 可用、HTTP 成功或 artifact 已写入都不等于 Claim、
// Decision、Paper submission、fill 或 Outcome 完成，时间 cutoff/provenance 不匹配必须 fail closed。
// Rust 机制：`join_all` 并发 Future 仍由 Permit/Store 约束；借用的 trait object adapter
// 通过 `Arc` 共享；`BTreeMap/BTreeSet` 保证资源闭包稳定；`Option` 明确区分缺失快照、
// quote error 和成功值，`Result` 保留内部身份错误而不降级成普通 coverage gap。

use super::*;

#[derive(Debug)]
pub(super) struct ExecutionSnapshotRefresh {
    pub account: Option<ArtifactRef>,
    pub quotes: Option<ArtifactRef>,
    pub clock: Option<ArtifactRef>,
    pub quote_error: Option<String>,
}

struct ExecutionAcquisitionMaterialization {
    artifacts: BTreeMap<ArtifactId, Artifact>,
    account: Option<Artifact>,
    quote_error: Option<String>,
}

async fn bounded_research_acquisition<T>(
    acquisition: impl std::future::Future<Output = Result<T>>,
    allowance: std::time::Duration,
) -> Result<T> {
    tokio::time::timeout(allowance, acquisition)
        .await
        .unwrap_or_else(|_| {
            Err(DaemonError::Unavailable(
                "evidence_acquisition_timeout".into(),
            ))
        })
}

impl Daemon {
    pub(super) async fn acquire_evidence(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<Vec<akzio_domain::Artifact>> {
        if task.node.input_artifacts.is_empty() {
            return match self.store.run_purpose(&task.run_id)? {
                RunPurpose::Debug => Ok(Vec::new()),
                RunPurpose::Paper | RunPurpose::PositionPlan => Err(DaemonError::InvalidInput(
                    "Research evidence gate requires at least one EvidenceNeed".to_owned(),
                )),
                purpose => Err(DaemonError::InvalidInput(format!(
                    "unsupported empty evidence gate for {purpose:?} run"
                ))),
            };
        }
        let purpose = self.store.run_purpose(&task.run_id)?;
        if matches!(purpose, RunPurpose::Paper | RunPurpose::PositionPlan) {
            self.validate_paper_evidence_policy(task)?;
            let mut research_inputs = Vec::new();
            let mut statuses = Vec::new();
            for reference in &task.node.input_artifacts {
                let need: EvidenceNeed = self.read_artifact_payload(reference)?;
                if need.criticality() == akzio_domain::EvidenceCriticality::ExecutionSafety {
                    statuses.push(serde_json::json!({"need": reference, "resource": need.resource,
                        "criticality": need.criticality(), "status": "deferred_to_execution", "diagnostic": "none"}));
                } else {
                    research_inputs.push(reference.clone());
                }
            }
            // Live snapshots are frozen after acquisition, before any Agent can
            // consume them. Historical/fixture acquisition retains its supplied cutoff.
            let (results, now) = if self.fixture_mode {
                (
                    futures::future::join_all(
                        research_inputs
                            .iter()
                            .map(|reference| self.acquire_evidence_need(task, reference, now)),
                    )
                    .await,
                    now,
                )
            } else {
                // Keep room inside the frozen gate budget to validate and
                // persist completed sources and explicit coverage gaps.
                let allowance = std::time::Duration::from_secs(u64::from(
                    task.node
                        .budget
                        .max_wall_time_secs
                        .saturating_sub(15)
                        .max(1),
                ));
                let acquired = futures::future::join_all(research_inputs.iter().map(|reference| {
                    bounded_research_acquisition(
                        self.acquire_live_paper_evidence(task, reference, now),
                        allowance,
                    )
                }))
                .await;
                let cutoff = Utc::now();
                let results = research_inputs
                    .iter()
                    .zip(acquired)
                    .map(|(reference, result)| {
                        let (need, artifact, request, acquired) = result?;
                        let runtime = EvidenceRuntime::new(self.store.clone(), [request.source]);
                        let bundle = runtime.materialize_validated(
                            &task.permit,
                            reference,
                            &request,
                            acquired,
                            cutoff,
                        )?;
                        Ok((need, artifact, bundle))
                    })
                    .collect::<Vec<Result<_>>>();
                (results, cutoff)
            };
            let mut acquisitions = Vec::new();

            let mut safety_failure = None;
            for (reference, result) in research_inputs.iter().zip(results) {
                let need: EvidenceNeed = self.read_artifact_payload(reference)?;
                let criticality = need.criticality();
                let result = result.and_then(|bundle| {
                    if need.source_family == "news_web" {
                        self.validate_paper_news_acquisition(task, &need, &bundle.2.normalized)?;
                    }
                    Ok(bundle)
                });
                let status = match result {
                    Ok(bundle) => {
                        acquisitions.push(bundle);
                        ("available", "none")
                    }
                    Err(
                        error @ DaemonError::Evidence(
                            akzio_ingest::EvidenceRuntimeError::TemporalContamination,
                        ),
                    ) => {
                        // Keep every need's diagnostic, but fail the entire gate even
                        // when the contaminated need is optional research evidence.
                        if safety_failure.is_none() {
                            safety_failure = Some(error);
                        }
                        ("unavailable", "temporal_contamination")
                    }
                    Err(error) => {
                        // Only known provider availability/content failures are
                        // coverage gaps. Identity, policy, Store and internal
                        // errors must retain their original failure semantics.
                        let Some(category) = evidence_failure_category(&error) else {
                            return Err(error);
                        };
                        // ExecutionSafety needs were deferred before acquisition.
                        // Provider coverage failures here are research gaps; the
                        // temporal and provenance failures above still fail closed.
                        ("unavailable", category)
                    }
                };
                statuses.push(
                    serde_json::json!({"need": reference, "resource": need.resource,
                    "criticality": criticality, "status": status.0, "diagnostic": status.1}),
                );
            }
            let status_artifact = Artifact::new(ArtifactKind::SemanticDetail,
                self.store.stage_json(&serde_json::json!({"type":"evidence_collection_status", "version":1,
                    "requirements": statuses, "authority":"rust", "missing_directional_evidence":"neutralize_affected_slots"}))?,
                "evidence.collection_status", ArtifactLifecycle::RunScoped,
                ArtifactProvenance { source_family:"akzio.ingest".to_owned(), observed_at:Some(now),
                    retrieved_at:now, source_uri:None, confidence_ppm:1_000_000,
                    producer_contract_hash:task.permit.contract_hash.clone() },
                Some(task.permit.artifact_origin()), task.node.input_artifacts.clone(), now)?;
            self.store.write_task_artifact(
                &task.permit,
                &status_artifact,
                LifecycleEventType::EvidenceNormalized,
                now,
            )?;
            // Retain successful acquisitions for diagnosis; temporal contamination
            // still blocks research regardless of criticality.
            if let Some(error) = safety_failure {
                for (_, _, bundle) in &acquisitions {
                    for artifact in [&bundle.raw, &bundle.normalized] {
                        self.store.write_task_artifact(
                            &task.permit,
                            artifact,
                            LifecycleEventType::EvidenceNormalized,
                            now,
                        )?;
                    }
                }
                return Err(error);
            }
            let (mut artifacts, _) =
                self.materialize_paper_acquisitions(task, acquisitions, now)?;
            artifacts.insert(status_artifact.artifact_id.clone(), status_artifact);
            return Ok(artifacts.into_values().collect());
        }

        let mut artifacts = BTreeMap::new();
        let mut paper_account_components = BTreeMap::new();
        for need_reference in &task.node.input_artifacts {
            if need_reference.kind != ArtifactKind::EvidenceNeed {
                return Err(DaemonError::InvalidInput(format!(
                    "evidence task {} has non-EvidenceNeed input",
                    task.node.task_id
                )));
            }
            let need_artifact = self.store.artifact(&need_reference.artifact_id)?;
            let need: EvidenceNeed =
                serde_json::from_slice(&self.store.read_blob(&need_artifact.blob)?)?;
            need.validate()
                .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
            let source = evidence_source(&need.source_family)?;
            let max_age_secs = i64::try_from(need.max_age_secs).map_err(|_| {
                DaemonError::InvalidInput("EvidenceNeed max_age_secs exceeds i64".to_owned())
            })?;
            let runtime = EvidenceRuntime::new(self.store.clone(), [source]);
            let purpose = self.store.run_purpose(&task.run_id)?;
            let request = EvidenceRequest {
                source,
                resource: need.resource.clone(),
                max_age: Duration::seconds(max_age_secs),
                acquisition_mode: evidence_acquisition_mode(purpose, &need),
            };
            let use_fixture_adapter = self.fixture_mode;
            let production_adapter = (!use_fixture_adapter)
                .then(|| self.production_evidence.get(&source))
                .flatten();
            let bundle = if let Some(adapter) = production_adapter {
                runtime
                    .acquire_and_normalize_async(
                        &task.permit,
                        need_reference,
                        &request,
                        adapter.as_ref(),
                        now,
                    )
                    .await?
            } else {
                if matches!(purpose, RunPurpose::Debug | RunPurpose::PositionPlan)
                    && !self.fixture_mode
                {
                    return Err(DaemonError::Unavailable(format!(
                        "real Debug evidence adapter is not configured for source {}",
                        source.as_str()
                    )));
                }
                let mut responses = self
                    .fixture_evidence
                    .get(&source)
                    .cloned()
                    .unwrap_or_default();
                let allow_fixture_evidence = self.fixture_mode;
                if allow_fixture_evidence {
                    responses
                        .entry(need.resource.clone())
                        .or_insert_with(|| debug_fixture_evidence(source, &need.resource, now));
                }
                if responses.is_empty() {
                    return Err(DaemonError::Unavailable(format!(
                        "no governed evidence adapter configured for source {}",
                        source.as_str()
                    )));
                }
                let adapter = FixtureEvidenceAdapter::new(
                    source,
                    responses
                        .iter()
                        .map(|(resource, evidence)| (resource.clone(), evidence.clone())),
                );
                runtime.acquire_and_normalize(
                    &task.permit,
                    need_reference,
                    &request,
                    &adapter,
                    now,
                )?
            };
            if matches!(
                need.resource.as_str(),
                PAPER_ACCOUNT_RESOURCE | PAPER_POSITIONS_RESOURCE | PAPER_OPEN_ORDERS_RESOURCE
            ) || need.resource.starts_with("paper.fills:")
            {
                paper_account_components.insert(
                    need.resource.clone(),
                    (need_artifact.clone(), bundle.normalized.clone()),
                );
            } else if let Some(snapshot) = self.materialize_paper_single_snapshot(
                task,
                &need_artifact,
                &need,
                &bundle.normalized,
                now,
            )? {
                artifacts.insert(snapshot.artifact_id.clone(), snapshot);
            }
            artifacts.insert(bundle.raw.artifact_id.clone(), bundle.raw);
            artifacts.insert(bundle.normalized.artifact_id.clone(), bundle.normalized);
        }
        if !paper_account_components.is_empty() {
            if let Some(snapshot) =
                self.materialize_paper_account_components(task, &paper_account_components, now)?
            {
                artifacts.insert(snapshot.artifact_id.clone(), snapshot);
            }
        }
        if artifacts.is_empty() {
            return Err(DaemonError::InvalidInput(format!(
                "evidence task {} has no EvidenceNeed inputs",
                task.node.task_id
            )));
        }
        Ok(artifacts.into_values().collect())
    }

    /// Research identity is frozen in scheduler-owned needs, independent of market openness.
    pub(super) fn research_session_key(&self, run_id: &RunId) -> Result<String> {
        if self.store.run_purpose(run_id)? == RunPurpose::Paper {
            return self
                .store
                .session_slot_for_run(run_id)?
                .map(|slot| slot.session_key)
                .ok_or_else(|| DaemonError::InvalidInput("Paper run has no session slot".into()));
        }
        let snapshot = self.store.workflow_snapshot(run_id)?;
        let gate = snapshot
            .tasks
            .iter()
            .find(|t| t.node.recipe_id.as_str() == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
            .ok_or_else(|| DaemonError::InvalidInput("research evidence gate missing".into()))?;
        let mut sessions = BTreeSet::new();
        for reference in &gate.node.input_artifacts {
            let need: EvidenceNeed = self.read_artifact_payload(reference)?;
            if need.resource.starts_with("option_chain:") {
                if let Some(date) = need.resource.split(':').nth(2) {
                    NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(|_| {
                        DaemonError::InvalidInput("invalid research session".into())
                    })?;
                    sessions.insert(date.to_owned());
                }
            }
        }
        if sessions.len() != 1 {
            return Err(DaemonError::InvalidInput(
                "research needs require one frozen session".into(),
            ));
        }
        Ok(sessions.into_iter().next().expect("one session"))
    }

    fn validate_paper_evidence_policy(&self, task: &ClaimedAttempt) -> Result<()> {
        let session_key = self.research_session_key(&task.run_id)?;
        let purpose = self.store.run_purpose(&task.run_id)?;
        let expected = akzio_domain::paper_session_evidence_needs(&session_key)
            .into_iter()
            .filter(|need| {
                purpose != RunPurpose::PositionPlan
                    || need.criticality() != akzio_domain::EvidenceCriticality::ExecutionSafety
            })
            .collect::<BTreeSet<_>>();
        let mut actual = BTreeSet::new();

        for reference in &task.node.input_artifacts {
            if reference.kind != ArtifactKind::EvidenceNeed {
                return Err(DaemonError::InvalidInput(
                    "Paper evidence policy input is not an EvidenceNeed".to_owned(),
                ));
            }
            let artifact = self.store.artifact(&reference.artifact_id)?;
            let need: EvidenceNeed =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            need.validate()
                .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
            if artifact.producer != "scheduler.paper_snapshot"
                || artifact.lifecycle != ArtifactLifecycle::RunScoped
                || artifact
                    .origin
                    .as_ref()
                    .and_then(|origin| origin.run_id.as_ref())
                    != Some(&task.run_id)
            {
                return Err(DaemonError::InvalidInput(
                    "Paper evidence policy input is invalid or duplicated".to_owned(),
                ));
            }
            let resource = need.resource.clone();
            if !actual.insert(need) {
                return Err(DaemonError::InvalidInput(format!(
                    "Paper evidence policy input duplicated {resource}"
                )));
            }
        }

        if actual != expected {
            return Err(DaemonError::InvalidInput(
                "Paper evidence inputs do not match the mandatory collection policy".to_owned(),
            ));
        }
        Ok(())
    }

    /// Record that a requested supplemental round produced no refined Claim.
    /// The analyst task still succeeds on its first Claim, so without this
    /// event the Store keeps no trace that the coverage gap stayed open.
    pub(super) fn note_supplemental_round_abandoned(
        &self,
        task: &ClaimedAttempt,
        reason: &str,
        error: &dyn std::fmt::Display,
    ) -> Result<()> {
        eprintln!("{reason} for task {}: {error}", task.node.task_id);
        tracing::warn!(
            run_id = %task.run_id,
            task_id = %task.node.task_id,
            error = %error,
            "{reason}"
        );
        self.store.append_task_event(
            &task.permit,
            LifecycleEventType::SupplementalRoundAbandoned,
            Utc::now(),
        )?;
        Ok(())
    }

    pub(super) fn prepare_supplemental_needs(
        &self,
        task: &ClaimedAttempt,
        claim: &ResearchClaim,
        claim_reference: &ArtifactRef,
        _candidates: &[ArtifactRef],
        now: DateTime<Utc>,
    ) -> Result<Vec<(ArtifactRef, Artifact, EvidenceNeed)>> {
        let session_key = self.research_session_key(&task.run_id)?;
        let session_date = NaiveDate::parse_from_str(&session_key, "%Y-%m-%d").map_err(|_| {
            DaemonError::InvalidInput("Paper run has invalid session slot".to_owned())
        })?;
        let mut needs = BTreeMap::<EvidenceNeed, ()>::new();

        for intent in claim
            .evidence_gaps
            .iter()
            .filter(|gap| gap.impact == akzio_domain::EvidenceGapImpact::BlocksDirectionalForecast)
            .flat_map(|gap| gap.supplemental_needs.iter())
        {
            for expanded_intent in Self::expand_supplemental_intents(intent)? {
                let need = expanded_intent.evidence_need()?;
                Self::validate_supplemental_need(&expanded_intent, &need, &session_date)?;
                // A prior normalized payload can still lack the requested facts.
                // The durable one-round limit bounds a deliberate re-query.
                needs.insert(need, ());
                if needs.len() > 8 {
                    return Err(DaemonError::InvalidInput(
                        "supplemental round exceeds 8 expanded requests".into(),
                    ));
                }
            }
        }

        needs
            .into_keys()
            .map(|need| {
                let artifact = Artifact::new(
                    ArtifactKind::EvidenceNeed,
                    self.store.stage_json(&need)?,
                    "agent.supplemental.evidence_need",
                    ArtifactLifecycle::RunScoped,
                    ArtifactProvenance {
                        source_family: "akzio.agent".to_owned(),
                        observed_at: None,
                        retrieved_at: now,
                        source_uri: None,
                        confidence_ppm: 1_000_000,
                        producer_contract_hash: task.permit.contract_hash.clone(),
                    },
                    Some(task.permit.artifact_origin()),
                    vec![claim_reference.clone()],
                    now,
                )?;
                self.store.write_task_artifact(
                    &task.permit,
                    &artifact,
                    LifecycleEventType::SupplementalEvidenceNeedCreated,
                    now,
                )?;
                Ok((
                    ArtifactRef {
                        artifact_id: artifact.artifact_id.clone(),
                        kind: ArtifactKind::EvidenceNeed,
                    },
                    artifact,
                    need,
                ))
            })
            .collect()
    }

    fn expand_supplemental_intents(intent: &ResearchIntent) -> Result<Vec<ResearchIntent>> {
        if intent.source_family != "alpaca" || intent.resource != "bars" {
            return Ok(vec![intent.clone()]);
        }

        let start = intent
            .window_start
            .ok_or_else(|| {
                DaemonError::InvalidInput(
                    "supplemental Alpaca bars require window_start".to_owned(),
                )
            })?
            .date_naive();
        if intent.assets.is_empty() {
            return Err(DaemonError::InvalidInput(
                "supplemental Alpaca bars require assets".to_owned(),
            ));
        }

        Ok(intent
            .assets
            .iter()
            .map(|asset| {
                let mut expanded = intent.clone();
                expanded.assets = BTreeSet::from([*asset]);
                expanded.resource = format!(
                    "bars:{}:1d:{}:{}",
                    asset.symbol(),
                    start.format("%Y-%m-%d"),
                    intent.max_results
                );
                expanded
            })
            .collect())
    }

    pub(super) async fn acquire_supplemental_evidence(
        &self,
        task: &ClaimedAttempt,
        needs: &[(ArtifactRef, Artifact, EvidenceNeed)],
        now: DateTime<Utc>,
    ) -> Result<Vec<ArtifactRef>> {
        let purpose = self.store.run_purpose(&task.run_id)?;
        let bundles =
            futures::future::join_all(needs.iter().map(|(reference, _need_artifact, need)| {
                let reference = reference.clone();
                let need = need.clone();
                async move {
                    let source = evidence_source(&need.source_family)?;
                    let max_age_secs = i64::try_from(need.max_age_secs).map_err(|_| {
                        DaemonError::InvalidInput(
                            "EvidenceNeed max_age_secs exceeds i64".to_owned(),
                        )
                    })?;
                    let runtime = EvidenceRuntime::new(self.store.clone(), [source]);
                    let request = EvidenceRequest {
                        source,
                        resource: need.resource.clone(),
                        max_age: Duration::seconds(max_age_secs),
                        acquisition_mode: evidence_acquisition_mode(purpose, &need),
                    };
                    let bundle = if self.fixture_mode {
                        let mut responses = self
                            .fixture_evidence
                            .get(&source)
                            .cloned()
                            .unwrap_or_default();
                        responses
                            .entry(need.resource.clone())
                            .or_insert_with(|| debug_fixture_evidence(source, &need.resource, now));
                        let adapter = FixtureEvidenceAdapter::new(source, responses);
                        runtime
                            .acquire_and_normalize_async(
                                &task.permit,
                                &reference,
                                &request,
                                &adapter,
                                now,
                            )
                            .await?
                    } else {
                        let adapter =
                            self.production_evidence
                                .get(&source)
                                .cloned()
                                .ok_or_else(|| {
                                    DaemonError::Unavailable(format!(
                                        "supplemental evidence requires {} adapter",
                                        source.as_str()
                                    ))
                                })?;
                        let acquired = runtime
                            .acquire_validated_async(
                                &task.permit,
                                &reference,
                                &request,
                                adapter.as_ref(),
                                now,
                            )
                            .await?;
                        // Freeze availability after real retrieval, as in the initial
                        // collection; the refined Analyst gets this new evidence clock.
                        runtime.materialize_validated(
                            &task.permit,
                            &reference,
                            &request,
                            acquired,
                            if task.node.recipe_id.as_str()
                                == akzio_domain::RESEARCH_SUPPLEMENT_RECIPE_ID
                            {
                                now
                            } else {
                                Utc::now()
                            },
                        )?
                    };
                    if need.source_family == "news_web" {
                        self.validate_paper_news_acquisition(task, &need, &bundle.normalized)?;
                    }
                    Ok::<_, DaemonError>(bundle)
                }
            }))
            .await;

        let mut normalized = Vec::with_capacity(bundles.len());
        for result in bundles {
            let bundle = match result {
                Ok(bundle) => bundle,
                Err(error) => {
                    if task.node.recipe_id.as_str() == akzio_domain::RESEARCH_SUPPLEMENT_RECIPE_ID {
                        return Err(error);
                    }
                    self.note_supplemental_round_abandoned(
                        task,
                        "supplemental source returned no valid facts",
                        &error,
                    )?;
                    continue;
                }
            };
            self.store.write_task_artifact(
                &task.permit,
                &bundle.raw,
                LifecycleEventType::EvidenceRaw,
                now,
            )?;
            self.store.write_task_artifact(
                &task.permit,
                &bundle.normalized,
                LifecycleEventType::EvidenceNormalized,
                now,
            )?;
            normalized.push(ArtifactRef {
                artifact_id: bundle.normalized.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            });
        }
        Ok(normalized)
    }

    fn validate_supplemental_need(
        intent: &ResearchIntent,
        need: &EvidenceNeed,
        session_date: &NaiveDate,
    ) -> Result<()> {
        if intent
            .window_end
            .is_some_and(|end| end.date_naive() > *session_date)
        {
            return Err(DaemonError::InvalidInput(
                "supplemental evidence window reaches into the future".to_owned(),
            ));
        }
        match need.source_family.as_str() {
            "alpaca" => {
                let parts = need.resource.split(':').collect::<Vec<_>>();
                if parts.len() != 5
                    || parts[0] != "bars"
                    || parts[2] != "1d"
                    || intent.assets.len() != 1
                    || parts[1]
                        != intent
                            .assets
                            .iter()
                            .next()
                            .map(|asset| asset.symbol())
                            .unwrap_or_default()
                    || parts[4]
                        .parse::<u16>()
                        .ok()
                        .is_none_or(|limit| !(1..=252).contains(&limit))
                    || Self::parse_resource_date(parts[3])? > *session_date
                {
                    return Err(DaemonError::InvalidInput(
                        "unsupported supplemental Alpaca resource".to_owned(),
                    ));
                }
            }
            "fred" => {
                let parts = need.resource.split(':').collect::<Vec<_>>();
                if parts.len() != 5
                    || parts[0] != "series"
                    || !matches!(parts[1], "DFF" | "DFII10" | "VIXCLS" | "DGS2" | "DGS10")
                    || {
                        let start = Self::parse_resource_date(parts[2])?;
                        let end = Self::parse_resource_date(parts[3])?;
                        let vintage = Self::parse_resource_date(parts[4])?;
                        start > end || end > *session_date || vintage >= *session_date
                    }
                {
                    return Err(DaemonError::InvalidInput(
                        "unsupported supplemental FRED resource".to_owned(),
                    ));
                }
            }
            "news_web" => {
                let parts = need.resource.split(':').collect::<Vec<_>>();
                let asset = parts
                    .get(1)
                    .and_then(|symbol| Asset::try_from(*symbol).ok());
                if parts.len() != 5
                    || parts[0] != "news"
                    || asset.is_none()
                    || !intent.assets.contains(&asset.unwrap())
                    || {
                        let start = Self::parse_resource_date(parts[2])?;
                        let end = Self::parse_resource_date(parts[3])?;
                        start > end || end > *session_date
                    }
                    || !matches!(
                        parts[4],
                        "market"
                            | "rates"
                            | "semiconductor"
                            | "regulation"
                            | "earnings"
                            | "geopolitics"
                    )
                {
                    return Err(DaemonError::InvalidInput(
                        "unsupported supplemental NewsWeb resource".to_owned(),
                    ));
                }
            }
            "sec_edgar" => {
                return Err(DaemonError::InvalidInput(
                    "SEC supplemental evidence is not enabled for the ETF universe".to_owned(),
                ));
            }
            _ => {
                return Err(DaemonError::InvalidInput(
                    "unsupported supplemental evidence source".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn parse_resource_date(value: &str) -> Result<NaiveDate> {
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map_err(|_| DaemonError::InvalidInput("invalid supplemental evidence date".to_owned()))
    }

    async fn acquire_live_paper_evidence(
        &self,
        task: &ClaimedAttempt,
        reference: &ArtifactRef,
        acquisition_started_at: DateTime<Utc>,
    ) -> Result<(
        EvidenceNeed,
        Artifact,
        EvidenceRequest,
        akzio_ingest::AcquiredEvidence,
    )> {
        let artifact = self.store.artifact(&reference.artifact_id)?;
        let need: EvidenceNeed = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
        need.validate()
            .map_err(|e| DaemonError::InvalidInput(e.to_string()))?;
        let source = evidence_source(&need.source_family)?;
        let request = EvidenceRequest {
            source,
            resource: need.resource.clone(),
            max_age: Duration::seconds(i64::try_from(need.max_age_secs).map_err(|_| {
                DaemonError::InvalidInput("EvidenceNeed max_age_secs exceeds i64".into())
            })?),
            acquisition_mode: evidence_acquisition_mode(
                self.store.run_purpose(&task.run_id)?,
                &need,
            ),
        };
        let adapter = self.production_evidence.get(&source).ok_or_else(|| {
            DaemonError::Unavailable(format!(
                "Paper evidence requires {} adapter",
                source.as_str()
            ))
        })?;
        let acquired = EvidenceRuntime::new(self.store.clone(), [source])
            .acquire_validated_async(
                &task.permit,
                reference,
                &request,
                adapter.as_ref(),
                acquisition_started_at,
            )
            .await?;
        Ok((need, artifact, request, acquired))
    }

    async fn acquire_evidence_need(
        &self,
        task: &ClaimedAttempt,
        need_reference: &ArtifactRef,
        now: DateTime<Utc>,
    ) -> Result<(EvidenceNeed, Artifact, EvidenceBundle)> {
        if need_reference.kind != ArtifactKind::EvidenceNeed {
            return Err(DaemonError::InvalidInput(format!(
                "evidence task {} has non-EvidenceNeed input",
                task.node.task_id
            )));
        }
        let need_artifact = self.store.artifact(&need_reference.artifact_id)?;
        let need: EvidenceNeed =
            serde_json::from_slice(&self.store.read_blob(&need_artifact.blob)?)?;
        need.validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let source = evidence_source(&need.source_family)?;
        let max_age_secs = i64::try_from(need.max_age_secs).map_err(|_| {
            DaemonError::InvalidInput("EvidenceNeed max_age_secs exceeds i64".to_owned())
        })?;
        let runtime = EvidenceRuntime::new(self.store.clone(), [source]);
        let request = EvidenceRequest {
            source,
            resource: need.resource.clone(),
            max_age: Duration::seconds(max_age_secs),
            acquisition_mode: evidence_acquisition_mode(
                self.store.run_purpose(&task.run_id)?,
                &need,
            ),
        };
        let bundle = if self.fixture_mode {
            let mut responses = self
                .fixture_evidence
                .get(&source)
                .cloned()
                .unwrap_or_default();
            responses
                .entry(need.resource.clone())
                .or_insert_with(|| debug_fixture_evidence(source, &need.resource, now));
            let adapter = FixtureEvidenceAdapter::new(
                source,
                responses
                    .iter()
                    .map(|(resource, evidence)| (resource.clone(), evidence.clone())),
            );
            runtime
                .acquire_and_normalize_async(&task.permit, need_reference, &request, &adapter, now)
                .await?
        } else {
            let adapter = self
                .production_evidence
                .get(&source)
                .cloned()
                .ok_or_else(|| {
                    DaemonError::Unavailable(format!(
                        "Paper evidence requires {} adapter",
                        source.as_str()
                    ))
                })?;
            runtime
                .acquire_and_normalize_async(
                    &task.permit,
                    need_reference,
                    &request,
                    adapter.as_ref(),
                    now,
                )
                .await?
        };
        Ok((need, need_artifact, bundle))
    }

    async fn acquire_paper_need(
        &self,
        permit: &TaskWritePermit,
        reference: &ArtifactRef,
        need_artifact: Artifact,
        need: EvidenceNeed,
        adapter: &dyn AsyncEvidenceAdapter,
        now: DateTime<Utc>,
    ) -> Result<(EvidenceNeed, Artifact, EvidenceBundle)> {
        if reference.kind != ArtifactKind::EvidenceNeed {
            return Err(DaemonError::InvalidInput(
                "Paper evidence task has non-EvidenceNeed input".to_owned(),
            ));
        }
        if evidence_source(&need.source_family)? != EvidenceSource::Alpaca {
            return Err(DaemonError::InvalidInput(
                "Paper evidence input is not Alpaca".to_owned(),
            ));
        }
        let max_age_secs = i64::try_from(need.max_age_secs).map_err(|_| {
            DaemonError::InvalidInput("EvidenceNeed max_age_secs exceeds i64".to_owned())
        })?;
        let runtime = EvidenceRuntime::new(self.store.clone(), [EvidenceSource::Alpaca]);
        let request = EvidenceRequest {
            source: EvidenceSource::Alpaca,
            resource: need.resource.clone(),
            max_age: Duration::seconds(max_age_secs),
            // Broker evidence comes from the Paper API itself.
            acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
        };
        let acquired = runtime
            .acquire_validated_async(permit, reference, &request, adapter, now)
            .await?;
        // Execution snapshots describe the account at execution time. Their
        // availability can be the HTTP receipt time, after acquisition began.
        // Keep the actual receipt timestamp and validate against the current
        // host clock; never advance the cutoff to an untrusted provider time.
        let bundle =
            runtime.materialize_validated(permit, reference, &request, acquired, Utc::now())?;
        Ok((need, need_artifact, bundle))
    }

    fn materialize_paper_acquisitions(
        &self,
        task: &ClaimedAttempt,
        acquisitions: Vec<(EvidenceNeed, Artifact, EvidenceBundle)>,
        now: DateTime<Utc>,
    ) -> Result<(BTreeMap<ArtifactId, Artifact>, Option<Artifact>)> {
        let mut artifacts = BTreeMap::new();
        let mut account_components = BTreeMap::new();

        for (need, need_artifact, bundle) in acquisitions {
            let resource = need.resource.clone();
            if evidence_source(&need.source_family)? == EvidenceSource::NewsWeb {
                self.validate_paper_news_acquisition(task, &need, &bundle.normalized)?;
            }
            if matches!(
                resource.as_str(),
                PAPER_ACCOUNT_RESOURCE | PAPER_POSITIONS_RESOURCE | PAPER_OPEN_ORDERS_RESOURCE
            ) || resource.starts_with("paper.fills:")
            {
                account_components
                    .insert(resource, (need_artifact.clone(), bundle.normalized.clone()));
            } else if let Some(snapshot) = self.materialize_paper_single_snapshot(
                task,
                &need_artifact,
                &need,
                &bundle.normalized,
                now,
            )? {
                artifacts.insert(snapshot.artifact_id.clone(), snapshot);
            }
            artifacts.insert(bundle.raw.artifact_id.clone(), bundle.raw);
            artifacts.insert(bundle.normalized.artifact_id.clone(), bundle.normalized);
        }

        let account = self.materialize_paper_account_components(task, &account_components, now)?;
        if let Some(account) = &account {
            artifacts.insert(account.artifact_id.clone(), account.clone());
        }
        Ok((artifacts, account))
    }

    /// Canonical Paper consumption of provider-mediated news evidence.
    ///
    /// The acquisition identity Rust recorded at ingest time must still match the
    /// policy this build derives, otherwise the run is reading evidence that was
    /// acquired under a different rule and fails closed. Evidence that never
    /// closed every source stays acquired and fully attributed, but it is not
    /// canonical-verified: `citations_complete` is false, so the research layer
    /// refuses it as directional ground.
    fn validate_paper_news_acquisition(
        &self,
        task: &ClaimedAttempt,
        need: &EvidenceNeed,
        normalized: &Artifact,
    ) -> Result<()> {
        let payload: NormalizedEvidencePayload =
            serde_json::from_slice(&self.store.read_blob(&normalized.blob)?)?;
        let document = payload.value.get("source_document");
        let expected = evidence_acquisition_mode(self.store.run_purpose(&task.run_id)?, need);
        let recorded_mode = document
            .and_then(|document| document.get("acquisition_mode"))
            .and_then(serde_json::Value::as_str);
        let Some(recorded_mode) = recorded_mode else {
            // Absent identity is never upgraded into a verified acquisition.
            tracing::warn!(
                run_id = %task.run_id,
                resource = %need.resource,
                "news evidence carries no acquisition identity and stays unverified"
            );
            return Ok(());
        };
        if recorded_mode != expected.as_str() {
            return Err(DaemonError::InvalidInput(format!(
                "news evidence {} was acquired as {recorded_mode} but Paper policy requires {}",
                need.resource,
                expected.as_str()
            )));
        }
        let recorded_policy = document
            .and_then(|document| document.get("acquisition_policy_hash"))
            .and_then(serde_json::Value::as_str);
        let current_policy = akzio_domain::evidence_acquisition_policy_hash().to_string();
        if recorded_policy != Some(current_policy.as_str()) {
            return Err(DaemonError::InvalidInput(format!(
                "news evidence {} was acquired under a different acquisition policy",
                need.resource
            )));
        }
        if !payload.quality.citations_complete {
            tracing::warn!(
                run_id = %task.run_id,
                resource = %need.resource,
                source_closure = document
                    .and_then(|document| document.get("source_closure"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown"),
                verified_source_count = document
                    .and_then(|document| document.get("verified_source_count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                required_source_count = document
                    .and_then(|document| document.get("required_source_count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                "news evidence did not close every source and is unusable for canonical grounds"
            );
        }
        Ok(())
    }

    pub(super) async fn refresh_execution_snapshots(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<ExecutionSnapshotRefresh> {
        let adapter = self
            .production_evidence
            .get(&EvidenceSource::Alpaca)
            .cloned()
            .ok_or_else(|| {
                DaemonError::Unavailable(
                    "Paper execution refresh requires Alpaca Paper evidence".to_owned(),
                )
            })?;
        let session_key = self
            .store
            .session_slot_for_run(&task.run_id)?
            .map(|slot| slot.session_key)
            .ok_or_else(|| DaemonError::InvalidInput("Paper run has no session slot".to_owned()))?;
        let expected_resources = BTreeSet::from([
            PAPER_ACCOUNT_RESOURCE.to_owned(),
            PAPER_POSITIONS_RESOURCE.to_owned(),
            PAPER_OPEN_ORDERS_RESOURCE.to_owned(),
            format!("paper.fills:{session_key}"),
            PAPER_QUOTES_RESOURCE.to_owned(),
            PAPER_CLOCK_RESOURCE.to_owned(),
        ]);
        let snapshot = self.store.workflow_snapshot(&task.run_id)?;
        let evidence_task = snapshot
            .tasks
            .iter()
            .find(|stored| stored.node.recipe_id.as_str() == akzio_runtime::EVIDENCE_GATE_RECIPE_ID)
            .ok_or_else(|| DaemonError::InvalidInput("Paper evidence gate missing".to_owned()))?;
        let mut needs = Vec::new();
        for reference in &evidence_task.node.input_artifacts {
            if reference.kind != ArtifactKind::EvidenceNeed {
                continue;
            }
            let artifact = self.store.artifact(&reference.artifact_id)?;
            let need: EvidenceNeed =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            if artifact.producer == "scheduler.paper_snapshot"
                && expected_resources.contains(&need.resource)
            {
                needs.push((reference.clone(), artifact, need));
            }
        }
        let actual_resources = needs
            .iter()
            .map(|(_, _, need)| need.resource.clone())
            .collect::<BTreeSet<_>>();
        if actual_resources != expected_resources || needs.len() != expected_resources.len() {
            return Err(DaemonError::InvalidInput(
                "Paper execution refresh inputs are incomplete".to_owned(),
            ));
        }
        let (market_needs, account_needs): (Vec<_>, Vec<_>) =
            needs.into_iter().partition(|(_, _, need)| {
                matches!(
                    need.resource.as_str(),
                    PAPER_QUOTES_RESOURCE | PAPER_CLOCK_RESOURCE
                )
            });
        let account_now = Utc::now();
        let mut acquisitions = futures::future::try_join_all(account_needs.into_iter().map(
            |(reference, need_artifact, need)| {
                let adapter = adapter.clone();
                async move {
                    self.acquire_paper_need(
                        &task.permit,
                        &reference,
                        need_artifact,
                        need,
                        adapter.as_ref(),
                        account_now,
                    )
                    .await
                }
            },
        ))
        .await?;
        let market_now = Utc::now();
        acquisitions.extend(
            futures::future::try_join_all(market_needs.into_iter().map(
                |(reference, need_artifact, need)| {
                    let adapter = adapter.clone();
                    async move {
                        self.acquire_paper_need(
                            &task.permit,
                            &reference,
                            need_artifact,
                            need,
                            adapter.as_ref(),
                            market_now,
                        )
                        .await
                    }
                },
            ))
            .await?,
        );

        let materialized = self.materialize_execution_acquisitions(task, acquisitions, now)?;
        let ExecutionAcquisitionMaterialization {
            artifacts,
            account,
            quote_error,
        } = materialized;
        account.ok_or_else(|| {
            DaemonError::InvalidInput(
                "Paper execution refresh did not materialize account snapshot".to_owned(),
            )
        })?;
        let mut artifacts = artifacts.into_values().collect::<Vec<_>>();
        artifacts.sort_by_key(|artifact| match artifact.kind {
            ArtifactKind::RawEvidence => 0,
            ArtifactKind::NormalizedEvidence if artifact.producer.starts_with("akzio.ingest.") => 1,
            ArtifactKind::NormalizedEvidence => 2,
            _ => 3,
        });
        let mut account = None;
        let mut quotes = None;
        let mut clock = None;
        for artifact in artifacts {
            let event_type = if artifact.kind == ArtifactKind::RawEvidence {
                LifecycleEventType::EvidenceRaw
            } else {
                LifecycleEventType::EvidenceNormalized
            };
            self.store
                .write_task_artifact(&task.permit, &artifact, event_type, now)?;
            let target = match artifact.producer.as_str() {
                "execution.snapshot.account" => &mut account,
                "execution.snapshot.quotes" => &mut quotes,
                "execution.snapshot.clock" => &mut clock,
                _ => continue,
            };
            *target = Some(ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: ArtifactKind::NormalizedEvidence,
            });
        }
        if account.is_none() || clock.is_none() {
            return Err(DaemonError::InvalidInput(
                "Paper execution refresh did not seal account and clock snapshots".to_owned(),
            ));
        }
        Ok(ExecutionSnapshotRefresh {
            account,
            quotes,
            clock,
            quote_error,
        })
    }
    fn materialize_paper_single_snapshot(
        &self,
        task: &ClaimedAttempt,
        need_artifact: &Artifact,
        need: &EvidenceNeed,
        normalized: &Artifact,
        now: DateTime<Utc>,
    ) -> Result<Option<Artifact>> {
        if self.store.run_purpose(&task.run_id)? != RunPurpose::Paper
            || evidence_source(&need.source_family)? != EvidenceSource::Alpaca
            || need_artifact.producer != "scheduler.paper_snapshot"
            || !matches!(
                need.resource.as_str(),
                PAPER_ACCOUNT_RESOURCE | PAPER_QUOTES_RESOURCE | PAPER_CLOCK_RESOURCE
            )
        {
            return Ok(None);
        }
        let session_key = self
            .store
            .session_slot_for_run(&task.run_id)?
            .map(|slot| slot.session_key)
            .ok_or_else(|| DaemonError::InvalidInput("Paper run has no session slot".to_owned()))?;
        let payload: NormalizedEvidencePayload =
            serde_json::from_slice(&self.store.read_blob(&normalized.blob)?)?;
        self.validate_paper_normalized(task, need_artifact, need, normalized, &payload)?;
        let materialized = match need.resource.as_str() {
            PAPER_ACCOUNT_RESOURCE => materialize_snapshot_artifact(
                &self.store,
                &task.permit,
                &[normalized],
                "execution.snapshot.account",
                &decode_paper_account(&payload.value, session_key, payload.observed_at)?,
                payload.observed_at,
                Some(payload.provenance.source_uri.clone()),
                now,
            ),
            PAPER_QUOTES_RESOURCE => materialize_snapshot_artifact(
                &self.store,
                &task.permit,
                &[normalized],
                "execution.snapshot.quotes",
                &validated_paper_quotes(&payload.value, session_key, payload.observed_at)?,
                payload.observed_at,
                Some(payload.provenance.source_uri.clone()),
                now,
            ),
            PAPER_CLOCK_RESOURCE => materialize_snapshot_artifact(
                &self.store,
                &task.permit,
                &[normalized],
                "execution.snapshot.clock",
                &decode_paper_clock(&payload.value, session_key, payload.observed_at)?,
                payload.observed_at,
                Some(payload.provenance.source_uri.clone()),
                now,
            ),
            _ => return Ok(None),
        }
        .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        Ok(Some(materialized))
    }

    fn materialize_execution_acquisitions(
        &self,
        task: &ClaimedAttempt,
        acquisitions: Vec<(EvidenceNeed, Artifact, EvidenceBundle)>,
        now: DateTime<Utc>,
    ) -> Result<ExecutionAcquisitionMaterialization> {
        let mut artifacts = BTreeMap::new();
        let mut account_components = BTreeMap::new();
        let mut quote_error = None;
        for (need, need_artifact, bundle) in acquisitions {
            let resource = need.resource.clone();
            if matches!(
                resource.as_str(),
                PAPER_ACCOUNT_RESOURCE | PAPER_POSITIONS_RESOURCE | PAPER_OPEN_ORDERS_RESOURCE
            ) || resource.starts_with("paper.fills:")
            {
                account_components
                    .insert(resource, (need_artifact.clone(), bundle.normalized.clone()));
            } else if resource == PAPER_QUOTES_RESOURCE {
                match self.materialize_paper_single_snapshot(
                    task,
                    &need_artifact,
                    &need,
                    &bundle.normalized,
                    now,
                ) {
                    Ok(Some(snapshot)) => {
                        artifacts.insert(snapshot.artifact_id.clone(), snapshot);
                    }
                    Ok(None) => {}
                    Err(error)
                        if error
                            .to_string()
                            .contains("Execution BLOCKED: InvalidQuote") =>
                    {
                        quote_error = Some(error.to_string());
                    }
                    Err(error) => return Err(error),
                }
            } else if let Some(snapshot) = self.materialize_paper_single_snapshot(
                task,
                &need_artifact,
                &need,
                &bundle.normalized,
                now,
            )? {
                artifacts.insert(snapshot.artifact_id.clone(), snapshot);
            }
            artifacts.insert(bundle.raw.artifact_id.clone(), bundle.raw);
            artifacts.insert(bundle.normalized.artifact_id.clone(), bundle.normalized);
        }
        let account = self.materialize_paper_account_components(task, &account_components, now)?;
        if let Some(account) = &account {
            artifacts.insert(account.artifact_id.clone(), account.clone());
        }
        Ok(ExecutionAcquisitionMaterialization {
            artifacts,
            account,
            quote_error,
        })
    }

    fn materialize_paper_account_components(
        &self,
        task: &ClaimedAttempt,
        components: &BTreeMap<String, (Artifact, Artifact)>,
        now: DateTime<Utc>,
    ) -> Result<Option<Artifact>> {
        if self.store.run_purpose(&task.run_id)? != RunPurpose::Paper || components.is_empty() {
            return Ok(None);
        }
        let session_key = self
            .store
            .session_slot_for_run(&task.run_id)?
            .map(|slot| slot.session_key)
            .ok_or_else(|| DaemonError::InvalidInput("Paper run has no session slot".to_owned()))?;
        let expected_resources = BTreeSet::from([
            PAPER_ACCOUNT_RESOURCE.to_owned(),
            PAPER_POSITIONS_RESOURCE.to_owned(),
            PAPER_OPEN_ORDERS_RESOURCE.to_owned(),
            format!("paper.fills:{session_key}"),
        ]);
        if components.keys().cloned().collect::<BTreeSet<_>>() != expected_resources {
            return Err(DaemonError::InvalidInput(
                "Paper account snapshot inputs are incomplete".to_owned(),
            ));
        }
        let mut payloads = BTreeMap::new();
        for (resource, (need_artifact, normalized)) in components {
            let need: EvidenceNeed =
                serde_json::from_slice(&self.store.read_blob(&need_artifact.blob)?)?;
            let payload: NormalizedEvidencePayload =
                serde_json::from_slice(&self.store.read_blob(&normalized.blob)?)?;
            self.validate_paper_normalized(task, need_artifact, &need, normalized, &payload)?;
            payloads.insert(resource.clone(), (normalized, payload));
        }
        // This timestamp bounds freshness of the entire account view. The
        // newest response cannot renew older positions, orders or fills; the
        // materialization completion time remains the artifact's created_at.
        let observed_at = payloads
            .values()
            .map(|(_, payload)| payload.observed_at)
            .min()
            .ok_or_else(|| {
                DaemonError::InvalidInput("Paper account payloads are empty".to_owned())
            })?;
        let account = akzio_ingest::decode_paper_account_components(
            &payloads[PAPER_ACCOUNT_RESOURCE].1.value,
            &payloads[PAPER_POSITIONS_RESOURCE].1.value,
            &payloads[PAPER_OPEN_ORDERS_RESOURCE].1.value,
            &payloads[&format!("paper.fills:{session_key}")].1.value,
            session_key,
            observed_at,
        )?;
        let normalized_sources = payloads
            .values()
            .map(|(normalized, _)| *normalized)
            .collect::<Vec<_>>();
        Ok(Some(
            materialize_snapshot_artifact(
                &self.store,
                &task.permit,
                &normalized_sources,
                "execution.snapshot.account",
                &account,
                observed_at,
                None,
                now,
            )
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?,
        ))
    }

    fn validate_paper_normalized(
        &self,
        task: &ClaimedAttempt,
        need_artifact: &Artifact,
        need: &EvidenceNeed,
        normalized: &Artifact,
        payload: &NormalizedEvidencePayload,
    ) -> Result<()> {
        if payload.source != EvidenceSource::Alpaca
            || normalized.provenance.source_family != EvidenceSource::Alpaca.as_str()
            || payload.resource != need.resource
            || payload.need.artifact_id != need_artifact.artifact_id
            || payload.need.kind != ArtifactKind::EvidenceNeed
            || need_artifact.producer != "scheduler.paper_snapshot"
            || need_artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&task.run_id)
            || normalized
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(&task.run_id)
        {
            return Err(DaemonError::InvalidInput(
                "Paper normalized evidence provenance is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

// This classifies errors only from governed acquisition/materialization. Adapter
// payload failures have typed variants; InvalidInput here comes from the Rust
// EvidenceNeed/acquisition-identity checks, never unparsed third-party content.
fn evidence_failure_category(error: &DaemonError) -> Option<&'static str> {
    use akzio_ingest::{EvidenceAdapterError as A, EvidenceRuntimeError as R};
    let category = match error {
        DaemonError::Evidence(R::Adapter(A::Unauthorized(_))) => "authorization",
        DaemonError::Evidence(R::Adapter(A::RateLimited { .. })) => "rate_limited",
        DaemonError::Evidence(R::Adapter(A::Pending(_))) => "pending",
        DaemonError::Evidence(R::Adapter(A::NotConfigured(_))) => "adapter_unavailable",
        DaemonError::Evidence(R::Adapter(A::Transport(_))) => "transport",
        DaemonError::Evidence(R::Adapter(A::Permanent(_))) => "permanent_provider_error",
        DaemonError::Evidence(R::Adapter(A::NativeWeb { kind, .. })) => kind.as_str(),
        DaemonError::Evidence(R::Adapter(A::DataQuality(_) | A::MissingFixture(_)))
        | DaemonError::Evidence(
            R::InvalidAcquisition | R::InvalidQuality | R::MissingAvailability,
        ) => "data_quality",
        DaemonError::Evidence(R::StaleEvidence) => "stale_content",
        DaemonError::Unavailable(reason) if reason == "evidence_acquisition_timeout" => {
            "acquisition_timeout"
        }
        DaemonError::Unavailable(_) => "adapter_unavailable",
        // Includes invalid provenance/citations, source/policy mismatches,
        // invalid committed needs, serialization and Store failures.
        _ => return None,
    };
    Some(category)
}

fn validated_paper_quotes(
    value: &serde_json::Value,
    broker_session: String,
    observed_at: chrono::DateTime<chrono::Utc>,
) -> Result<QuoteSnapshot> {
    let snapshot = decode_paper_quotes(value, broker_session, observed_at)?;
    for (asset, quote) in &snapshot.quotes {
        if quote.bid.0 <= 0 || quote.ask.0 <= quote.bid.0 {
            return Err(DaemonError::InvalidInput(format!(
                "Execution BLOCKED: InvalidQuote({asset}): bid={} ask={}",
                quote.bid.0, quote.ask.0
            )));
        }
    }
    Ok(snapshot)
}

#[cfg(test)]
mod acquisition_deadline_tests {
    use super::*;

    #[test]
    fn acquisition_classification_does_not_downgrade_internal_or_identity_errors() {
        use akzio_ingest::{EvidenceAdapterError as A, EvidenceRuntimeError as R};
        let malformed_json = serde_json::from_str::<Value>("{").unwrap_err();
        let failures = [
            DaemonError::InvalidInput("Rust acquisition identity mismatch".into()),
            DaemonError::Json(malformed_json),
            DaemonError::Store(StoreError::StalePermit(TaskId::new())),
            DaemonError::Evidence(R::Store(StoreError::StalePermit(TaskId::new()))),
            DaemonError::Evidence(R::InvalidProvenance),
            DaemonError::Evidence(R::InvalidCitation),
            DaemonError::Evidence(R::Adapter(A::SourceMismatch)),
            DaemonError::Evidence(R::Adapter(A::Policy {
                evidence_source: EvidenceSource::NewsWeb,
                resource: "news:QQQ".into(),
                reason: "request violates the frozen acquisition policy".into(),
            })),
        ];
        for failure in failures {
            assert!(evidence_failure_category(&failure).is_none(), "{failure}");
        }
        assert_eq!(
            evidence_failure_category(&DaemonError::Evidence(R::Adapter(A::DataQuality(
                "provider returned malformed content".into()
            )))),
            Some("data_quality")
        );
        assert_eq!(
            evidence_failure_category(&DaemonError::Evidence(R::Adapter(A::Pending(
                "source not published yet".into()
            )))),
            Some("pending")
        );
    }

    #[tokio::test]
    async fn slow_source_does_not_discard_completed_sources_or_hide_temporal_errors() {
        let allowance = std::time::Duration::from_millis(10);
        let result = tokio::time::timeout(std::time::Duration::from_millis(100), async {
            tokio::join!(
                bounded_research_acquisition(async { Ok(7_u8) }, allowance),
                bounded_research_acquisition(std::future::pending::<Result<u8>>(), allowance),
                bounded_research_acquisition(
                    async {
                        Err::<u8, _>(DaemonError::Evidence(
                            akzio_ingest::EvidenceRuntimeError::TemporalContamination,
                        ))
                    },
                    allowance,
                ),
            )
        })
        .await
        .expect("one slow source must not consume the entire gate deadline");
        assert_eq!(result.0.unwrap(), 7);
        assert_eq!(
            evidence_failure_category(&result.1.unwrap_err()),
            Some("acquisition_timeout")
        );
        assert!(matches!(
            result.2,
            Err(DaemonError::Evidence(
                akzio_ingest::EvidenceRuntimeError::TemporalContamination
            ))
        ));
    }
}
