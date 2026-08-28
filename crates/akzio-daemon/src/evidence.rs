//! Governed evidence acquisition and snapshot materialization.

use super::*;

impl Daemon {
    pub(super) async fn acquire_evidence(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<Vec<akzio_domain::Artifact>> {
        if task.node.input_artifacts.is_empty() {
            return match self.store.run_purpose(&task.run_id)? {
                RunPurpose::Debug | RunPurpose::PaperDryRun => Ok(Vec::new()),
                RunPurpose::Paper => Err(DaemonError::InvalidInput(
                    "Paper evidence gate requires at least one EvidenceNeed".to_owned(),
                )),
                purpose => Err(DaemonError::InvalidInput(format!(
                    "unsupported empty evidence gate for {purpose:?} run"
                ))),
            };
        }
        if self.store.run_purpose(&task.run_id)? == RunPurpose::Paper {
            self.validate_paper_evidence_policy(task)?;
            let acquisitions = futures::future::try_join_all(
                task.node
                    .input_artifacts
                    .iter()
                    .map(|reference| self.acquire_evidence_need(task, reference, now)),
            )
            .await?;
            let (artifacts, _) = self.materialize_paper_acquisitions(task, acquisitions, now)?;
            if artifacts.is_empty() {
                return Err(DaemonError::InvalidInput(format!(
                    "evidence task {} has no EvidenceNeed inputs",
                    task.node.task_id
                )));
            }
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
            let request = EvidenceRequest {
                source,
                resource: need.resource.clone(),
                max_age: Duration::seconds(max_age_secs),
            };
            let purpose = self.store.run_purpose(&task.run_id)?;
            let use_fixture_adapter = self.fixture_mode || purpose == RunPurpose::PaperDryRun;
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
                if purpose == RunPurpose::Debug && !self.fixture_mode {
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
                let allow_fixture_evidence =
                    purpose == RunPurpose::PaperDryRun || self.fixture_mode;
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

    fn validate_paper_evidence_policy(&self, task: &ClaimedAttempt) -> Result<()> {
        let session_key = self
            .store
            .session_slot_for_run(&task.run_id)?
            .map(|slot| slot.session_key)
            .ok_or_else(|| DaemonError::InvalidInput("Paper run has no session slot".to_owned()))?;
        let expected = akzio_domain::paper_session_evidence_needs(&session_key)
            .into_iter()
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
        candidates: &[ArtifactRef],
        now: DateTime<Utc>,
    ) -> Result<Vec<(ArtifactRef, Artifact, EvidenceNeed)>> {
        let session_key = self
            .store
            .session_slot_for_run(&task.run_id)?
            .map(|slot| slot.session_key)
            .ok_or_else(|| DaemonError::InvalidInput("Paper run has no session slot".to_owned()))?;
        let session_date = NaiveDate::parse_from_str(&session_key, "%Y-%m-%d").map_err(|_| {
            DaemonError::InvalidInput("Paper run has invalid session slot".to_owned())
        })?;
        let existing_resources = candidates
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::NormalizedEvidence)
            .filter_map(|reference| self.store.artifact(&reference.artifact_id).ok())
            .filter_map(|artifact| self.store.read_blob(&artifact.blob).ok())
            .filter_map(|payload| {
                serde_json::from_slice::<NormalizedEvidencePayload>(&payload).ok()
            })
            .map(|payload| payload.resource)
            .collect::<BTreeSet<_>>();
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
                if !existing_resources.contains(&need.resource) {
                    needs.insert(need, ());
                }
            }
        }

        needs
            .into_keys()
            .map(|need| {
                let artifact = Artifact::new(
                    ArtifactKind::EvidenceNeed,
                    self.store.put_json(&need)?,
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
        let bundles =
            futures::future::try_join_all(needs.iter().map(|(reference, need_artifact, need)| {
                let reference = reference.clone();
                let need_artifact = need_artifact.clone();
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
                        runtime
                            .acquire_and_normalize_async(
                                &task.permit,
                                &reference,
                                &request,
                                adapter.as_ref(),
                                now,
                            )
                            .await?
                    };
                    Ok::<_, DaemonError>((need_artifact, bundle))
                }
            }))
            .await?;

        let mut normalized = Vec::with_capacity(bundles.len());
        for (_need_artifact, bundle) in bundles {
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
                if parts.len() != 4
                    || parts[0] != "series"
                    || !matches!(parts[1], "DFF" | "DFII10" | "VIXCLS" | "DGS2" | "DGS10")
                    || {
                        let start = Self::parse_resource_date(parts[2])?;
                        let end = Self::parse_resource_date(parts[3])?;
                        start > end || end > *session_date
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
        let bundle = runtime
            .acquire_and_normalize_async(
                permit,
                reference,
                &EvidenceRequest {
                    source: EvidenceSource::Alpaca,
                    resource: need.resource.clone(),
                    max_age: Duration::seconds(max_age_secs),
                },
                adapter,
                now,
            )
            .await?;
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

    pub(super) async fn refresh_execution_snapshots(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<(
        Option<ArtifactRef>,
        Option<ArtifactRef>,
        Option<ArtifactRef>,
    )> {
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
            .find(|stored| {
                stored.node.recipe_id.as_str() == akzio_runtime::v2::EVIDENCE_GATE_RECIPE_ID
            })
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

        let (artifacts, account) = self.materialize_paper_acquisitions(task, acquisitions, now)?;
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
        if account.is_none() || quotes.is_none() || clock.is_none() {
            return Err(DaemonError::InvalidInput(
                "Paper execution refresh did not seal all snapshots".to_owned(),
            ));
        }
        Ok((account, quotes, clock))
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
                &decode_paper_account(&payload.value, session_key.clone(), payload.observed_at)?,
                payload.observed_at,
                Some(payload.provenance.source_uri.clone()),
                now,
            ),
            PAPER_QUOTES_RESOURCE => materialize_snapshot_artifact(
                &self.store,
                &task.permit,
                &[normalized],
                "execution.snapshot.quotes",
                &decode_paper_quotes(&payload.value, session_key.clone(), payload.observed_at)?,
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
        let observed_at = payloads
            .values()
            .map(|(_, payload)| payload.observed_at)
            .max()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn intent(source_family: &str, resource: &str, assets: &[Asset]) -> ResearchIntent {
        ResearchIntent {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            source_family: source_family.to_owned(),
            resource: resource.to_owned(),
            query: "supplemental evidence".to_owned(),
            assets: assets.iter().copied().collect::<BTreeSet<_>>(),
            window_start: None,
            window_end: None,
            max_age_secs: 300,
            max_results: 1,
        }
    }

    fn need(source_family: &str, resource: &str) -> EvidenceNeed {
        EvidenceNeed {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            source_family: source_family.to_owned(),
            resource: resource.to_owned(),
            max_age_secs: 300,
        }
    }

    #[test]
    fn bars_supplemental_intents_expand_per_asset() {
        let mut bars = intent("alpaca", "bars", &[Asset::Soxl, Asset::Soxx]);
        bars.window_start = Some(
            DateTime::parse_from_rfc3339("2026-08-20T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        bars.window_end = Some(
            DateTime::parse_from_rfc3339("2026-08-27T23:59:59Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        bars.max_results = 20;

        let expanded = Daemon::expand_supplemental_intents(&bars).unwrap();
        let resources = expanded
            .iter()
            .map(|intent| intent.resource.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            resources,
            BTreeSet::from(["bars:SOXL:1d:2026-08-20:20", "bars:SOXX:1d:2026-08-20:20",])
        );
        assert!(expanded.iter().all(|intent| intent.assets.len() == 1));
    }

    #[test]
    fn supplemental_resource_windows_validate_start_and_end_dates() {
        let session_date = NaiveDate::from_ymd_opt(2026, 8, 27).expect("valid session date");
        let fred_intent = intent("fred", "series:DFF:2026-08-01:2026-08-27", &[]);
        assert!(Daemon::validate_supplemental_need(
            &fred_intent,
            &need("fred", "series:DFF:2026-08-01:2026-08-27"),
            &session_date,
        )
        .is_ok());
        for resource in [
            "series:DFF:2026-08-28:2026-08-27",
            "series:DFF:2026-08-01:2026-08-28",
        ] {
            assert!(Daemon::validate_supplemental_need(
                &fred_intent,
                &need("fred", resource),
                &session_date,
            )
            .is_err());
        }

        let news_intent = intent(
            "news_web",
            "news:QQQ:2026-08-01:2026-08-27:market",
            &[Asset::Qqq],
        );
        assert!(Daemon::validate_supplemental_need(
            &news_intent,
            &need("news_web", "news:QQQ:2026-08-01:2026-08-27:market"),
            &session_date,
        )
        .is_ok());
        assert!(Daemon::validate_supplemental_need(
            &news_intent,
            &need("news_web", "news:QQQ:2026-08-28:2026-08-27:market"),
            &session_date,
        )
        .is_err());
        assert!(Daemon::validate_supplemental_need(
            &intent("alpaca", "bars:QQQ:1d:2026-08-01:32", &[Asset::Qqq]),
            &need("alpaca", "bars:QQQ:1d:2026-08-28:32"),
            &session_date,
        )
        .is_err());
    }
}
