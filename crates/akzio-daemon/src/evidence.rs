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
        if self.store.run_purpose(&task.run_id)? == RunPurpose::Paper
            && self
                .production_evidence
                .contains_key(&EvidenceSource::Alpaca)
        {
            return self.acquire_paper_evidence_concurrently(task, now).await;
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
            let use_debug_fixture =
                purpose == RunPurpose::PaperDryRun && source == EvidenceSource::Alpaca;
            let production_adapter = (!use_debug_fixture)
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
                let allow_fixture_evidence = purpose == RunPurpose::PaperDryRun
                    || (purpose == RunPurpose::Debug && self.fixture_mode);
                if allow_fixture_evidence && source == EvidenceSource::Alpaca {
                    responses
                        .entry(need.resource.clone())
                        .or_insert_with(|| debug_fixture_evidence(&need.resource, now));
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

    async fn acquire_paper_evidence_concurrently(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<Vec<Artifact>> {
        let adapter = self
            .production_evidence
            .get(&EvidenceSource::Alpaca)
            .cloned()
            .ok_or_else(|| {
                DaemonError::Unavailable("Paper evidence requires Alpaca adapter".to_owned())
            })?;
        let acquisitions =
            futures::future::try_join_all(task.node.input_artifacts.iter().map(|reference| {
                let reference = reference.clone();
                let store = self.store.clone();
                let adapter = adapter.clone();
                let permit = task.permit.clone();
                async move {
                    if reference.kind != ArtifactKind::EvidenceNeed {
                        return Err(DaemonError::InvalidInput(
                            "Paper evidence task has non-EvidenceNeed input".to_owned(),
                        ));
                    }
                    let need_artifact = store.artifact(&reference.artifact_id)?;
                    let need: EvidenceNeed =
                        serde_json::from_slice(&store.read_blob(&need_artifact.blob)?)?;
                    need.validate()
                        .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
                    if evidence_source(&need.source_family)? != EvidenceSource::Alpaca {
                        return Err(DaemonError::InvalidInput(
                            "Paper evidence input is not Alpaca".to_owned(),
                        ));
                    }
                    let max_age_secs = i64::try_from(need.max_age_secs).map_err(|_| {
                        DaemonError::InvalidInput(
                            "EvidenceNeed max_age_secs exceeds i64".to_owned(),
                        )
                    })?;
                    let runtime = EvidenceRuntime::new(store, [EvidenceSource::Alpaca]);
                    let bundle = runtime
                        .acquire_and_normalize_async(
                            &permit,
                            &reference,
                            &EvidenceRequest {
                                source: EvidenceSource::Alpaca,
                                resource: need.resource.clone(),
                                max_age: Duration::seconds(max_age_secs),
                            },
                            adapter.as_ref(),
                            now,
                        )
                        .await?;
                    Ok::<_, DaemonError>((need, need_artifact, bundle))
                }
            }))
            .await?;

        let mut artifacts = BTreeMap::new();
        let mut account_components = BTreeMap::new();
        for (need, need_artifact, bundle) in acquisitions {
            if matches!(
                need.resource.as_str(),
                PAPER_ACCOUNT_RESOURCE | PAPER_POSITIONS_RESOURCE | PAPER_OPEN_ORDERS_RESOURCE
            ) || need.resource.starts_with("paper.fills:")
            {
                account_components.insert(
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
        if !account_components.is_empty() {
            if let Some(account) =
                self.materialize_paper_account_components(task, &account_components, now)?
            {
                artifacts.insert(account.artifact_id.clone(), account);
            }
        }
        Ok(artifacts.into_values().collect())
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

        let acquisitions = futures::future::try_join_all(needs.into_iter().map(
            |(reference, need_artifact, need)| {
                let runtime = EvidenceRuntime::new(self.store.clone(), [EvidenceSource::Alpaca]);
                let adapter = adapter.clone();
                let permit = task.permit.clone();
                async move {
                    let max_age_secs = i64::try_from(need.max_age_secs).map_err(|_| {
                        DaemonError::InvalidInput(
                            "EvidenceNeed max_age_secs exceeds i64".to_owned(),
                        )
                    })?;
                    let bundle = runtime
                        .acquire_and_normalize_async(
                            &permit,
                            &reference,
                            &EvidenceRequest {
                                source: EvidenceSource::Alpaca,
                                resource: need.resource.clone(),
                                max_age: Duration::seconds(max_age_secs),
                            },
                            adapter.as_ref(),
                            now,
                        )
                        .await?;
                    Ok::<_, DaemonError>((need, need_artifact, bundle))
                }
            },
        ))
        .await?;

        let mut artifacts = BTreeMap::new();
        let mut account_components = BTreeMap::new();
        for (need, need_artifact, bundle) in acquisitions {
            let resource = need.resource.clone();
            if matches!(
                resource.as_str(),
                PAPER_ACCOUNT_RESOURCE | PAPER_POSITIONS_RESOURCE | PAPER_OPEN_ORDERS_RESOURCE
            ) || resource.starts_with("paper.fills:")
            {
                account_components.insert(
                    resource.clone(),
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
        let account = self
            .materialize_paper_account_components(task, &account_components, now)?
            .ok_or_else(|| {
                DaemonError::InvalidInput(
                    "Paper execution refresh did not materialize account snapshot".to_owned(),
                )
            })?;
        artifacts.insert(account.artifact_id.clone(), account);

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
            PAPER_ACCOUNT_RESOURCE => SnapshotArtifactMaterializer::materialize(
                &self.store,
                &task.permit,
                &[normalized],
                "execution.snapshot.account",
                &decode_paper_account(&payload.value, session_key.clone(), payload.observed_at)?,
                payload.observed_at,
                Some(payload.provenance.source_uri.clone()),
                now,
            ),
            PAPER_QUOTES_RESOURCE => SnapshotArtifactMaterializer::materialize(
                &self.store,
                &task.permit,
                &[normalized],
                "execution.snapshot.quotes",
                &decode_paper_quotes(&payload.value, session_key.clone(), payload.observed_at)?,
                payload.observed_at,
                Some(payload.provenance.source_uri.clone()),
                now,
            ),
            PAPER_CLOCK_RESOURCE => SnapshotArtifactMaterializer::materialize(
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
            SnapshotArtifactMaterializer::materialize(
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
