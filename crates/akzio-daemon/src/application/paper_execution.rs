use crate::*;
use akzio_domain::{
    ComplianceActivitySnapshot, ComplianceControl, DependencyClosure, DependencyHealthStatus,
    DependencyKind, DependencySnapshot, FallbackPolicy, FinancialContentPolicy,
    InformationClassification, MarketClockSnapshot, OrderReceiptState, OrderSide, WeightPpm,
};
use akzio_execution::PreTradeSafetyEvidence;
use akzio_ingest::GovernedResource;

/// Deterministic decision, execution, commitment and reconciliation capability.
pub(crate) struct PaperExecution<'a> {
    daemon: &'a Daemon,
}

impl<'a> PaperExecution<'a> {
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    pub(crate) fn decision_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let proposal = self
            .daemon
            .terminal_input(task, ArtifactKind::DecisionProposal)?;
        self.daemon.decision_runtime.decide(&DecisionGateInput {
            permit: task.permit.clone(),
            proposal,
            now,
        })?;
        Ok(TaskCompletion::Committed)
    }

    pub(crate) async fn execution_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let decision_context = self
            .daemon
            .terminal_input(task, ArtifactKind::DecisionContext)?;
        let (account_snapshot, quote_snapshot, market_clock_snapshot, quote_validation_error) =
            if self
                .daemon
                .production_evidence
                .contains_key(&EvidenceSource::Alpaca)
                && self.daemon.store.run_purpose(&task.run_id)? == RunPurpose::Paper
            {
                let refreshed = self.daemon.refresh_execution_snapshots(task, now).await?;
                (
                    refreshed.account,
                    refreshed.quotes,
                    refreshed.clock,
                    refreshed.quote_error,
                )
            } else {
                let (account, quotes, clock) = self.daemon.execution_snapshot_inputs(task)?;
                (account, quotes, clock, None)
            };
        let gate_now = Utc::now();
        let pretrade_safety = self.pretrade_safety_evidence(
            task,
            &decision_context,
            account_snapshot.as_ref(),
            quote_snapshot.as_ref(),
            market_clock_snapshot.as_ref(),
            gate_now,
        )?;
        // Snapshot acquisition is a separately governed Evidence path. Until a
        // provider returns typed, task-bound snapshots, the execution runtime
        // emits a durable NoOrder rather than guessing from arbitrary evidence.
        let output = self
            .daemon
            .execution_runtime
            .evaluate(&ExecutionGateInput {
                permit: task.permit.clone(),
                decision_context,
                account_snapshot,
                quote_snapshot,
                quote_validation_error,
                market_clock_snapshot,
                pretrade_safety,
                now: gate_now,
            })?;
        self.daemon
            .execution_runtime
            .commit(&task.permit, &output, gate_now)?;
        Ok(TaskCompletion::Committed)
    }

    fn pretrade_safety_evidence(
        &self,
        task: &ClaimedAttempt,
        decision_context: &ArtifactRef,
        account_snapshot: Option<&ArtifactRef>,
        quote_snapshot: Option<&ArtifactRef>,
        market_clock_snapshot: Option<&ArtifactRef>,
        now: DateTime<Utc>,
    ) -> Result<Option<PreTradeSafetyEvidence>> {
        if self.daemon.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(None);
        }
        let (Some(account_snapshot), Some(quote_snapshot), Some(market_clock_snapshot)) =
            (account_snapshot, quote_snapshot, market_clock_snapshot)
        else {
            return Ok(None);
        };

        let decision: DecisionContext = self.daemon.read_artifact_payload(decision_context)?;
        decision
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let account: AccountSnapshot = self.daemon.read_artifact_payload(account_snapshot)?;
        let quotes: QuoteSnapshot = self.daemon.read_artifact_payload(quote_snapshot)?;
        let clock: MarketClockSnapshot =
            self.daemon.read_artifact_payload(market_clock_snapshot)?;
        account
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        quotes
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        clock
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let account_observation = self.persisted_observation(account_snapshot)?;
        let quote_observation = self.persisted_observation(quote_snapshot)?;
        let clock_observation = self.persisted_observation(market_clock_snapshot)?;

        let mut average_daily_dollar_volume = BTreeMap::new();
        let mut news_dependency = EvidenceDependencyAggregate::default();
        let mut macro_dependency = EvidenceDependencyAggregate::default();
        let mut network_successes = Vec::new();
        network_successes.extend(
            [&account_observation, &quote_observation, &clock_observation]
                .into_iter()
                .filter(|observation| observation.network_host.is_some())
                .cloned(),
        );
        let mut information_classifications = quotes
            .quotes
            .keys()
            .copied()
            .map(|asset| (asset, InformationClassification::Public))
            .collect::<BTreeMap<_, _>>();
        let content_policy = FinancialContentPolicy::default();
        for reference in &decision.evidence {
            if reference.kind != ArtifactKind::NormalizedEvidence {
                continue;
            }
            let payload: NormalizedEvidencePayload =
                self.daemon.read_artifact_payload(reference)?;
            let observation = self.persisted_observation(reference)?;
            if observation.network_host.is_some() {
                network_successes.push(observation.clone());
            }
            match payload.source {
                EvidenceSource::NewsWeb | EvidenceSource::SecEdgar => {
                    news_dependency.observe(&payload, &observation);
                }
                EvidenceSource::Fred => macro_dependency.observe(&payload, &observation),
                EvidenceSource::Alpaca => {}
            }
            if let Ok(GovernedResource::AlpacaBars { asset, .. }) =
                GovernedResource::parse(payload.source, &payload.resource)
            {
                if let Some(value) = payload
                    .quant_features
                    .as_ref()
                    .and_then(|features| features.average_dollar_volume_20d_micros)
                    .and_then(|value| i64::try_from(value).ok())
                {
                    average_daily_dollar_volume.insert(asset, MoneyMicros(value));
                }
                information_classifications
                    .entry(asset)
                    .or_insert(InformationClassification::Public);
            }
            if let Some(content) = payload.financial_content {
                let content_blocks_trading = content.blocks_trading(&content_policy);
                let classification = if content_blocks_trading {
                    InformationClassification::Unknown
                } else {
                    content.information_classification
                };
                if content_blocks_trading {
                    for asset in decision
                        .target
                        .weights
                        .iter()
                        .filter_map(|(asset, weight)| (weight.0 > 0).then_some(*asset))
                    {
                        information_classifications
                            .insert(asset, InformationClassification::Unknown);
                    }
                }
                for identifier in &content.canonical_entity_ids {
                    let Some(symbol) = identifier.strip_prefix("ticker:") else {
                        continue;
                    };
                    let Ok(asset) = Asset::try_from(symbol) else {
                        continue;
                    };
                    information_classifications
                        .entry(asset)
                        .and_modify(|current| *current = (*current).max(classification))
                        .or_insert(classification);
                }
            }
        }

        let (manifest, _) = self
            .daemon
            .store
            .paper_approval_for_run(&task.run_id)?
            .ok_or_else(|| {
                DaemonError::InvalidInput(
                    "Paper pre-trade safety requires an approved runtime manifest".to_owned(),
                )
            })?;
        let model_freshness_secs = decision
            .validity
            .as_ref()
            .map_or(1, |validity| validity.maximum_execution_delay_ms / 1_000)
            .max(1);
        let model_dependency =
            self.persisted_model_dependency(&task.run_id, &manifest, model_freshness_secs, now)?;
        let market_data_dependency =
            market_data_dependency(&manifest.market_data_feed, &quote_observation, now);
        let broker_dependency = alpaca_service_dependency(
            DependencyKind::Broker,
            "paper-account",
            &account_observation,
            now,
        );
        let market_clock_dependency = alpaca_service_dependency(
            DependencyKind::MarketClock,
            "market-session",
            &clock_observation,
            now,
        );
        let dns_dependency = dns_dependency(
            &network_successes,
            manifest.provider_id.contains("fixture") || manifest.model_id.contains("fixture"),
            now,
        );
        let dependency_closure = DependencyClosure {
            captured_at: now,
            dependencies: BTreeMap::from([
                model_dependency,
                news_dependency.snapshot(DependencyKind::NewsProvider, decision.created_at, now),
                market_data_dependency,
                macro_dependency.snapshot(DependencyKind::MacroData, decision.created_at, now),
                broker_dependency,
                dns_dependency,
                market_clock_dependency,
                internal_healthy_dependency(
                    DependencyKind::Storage,
                    "v2-store",
                    &DOMAIN_SCHEMA_VERSION.to_string(),
                    "durable-state",
                    now,
                ),
                internal_healthy_dependency(
                    DependencyKind::Identity,
                    "approved-runtime-manifest",
                    manifest.config_hash.as_str(),
                    "runtime-identity",
                    now,
                ),
                internal_healthy_dependency(
                    DependencyKind::ComplianceData,
                    "embedded-compliance-policy",
                    manifest.execution_policy_hash.as_str(),
                    "public-policy",
                    now,
                ),
            ]),
        };
        dependency_closure
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;

        Ok(Some(PreTradeSafetyEvidence {
            homogeneous_agent_count: 1,
            average_daily_dollar_volume,
            information_classifications,
            recent_compliance_activity: self.recent_compliance_activity(now)?,
            dependency_closure,
        }))
    }

    fn persisted_observation(&self, reference: &ArtifactRef) -> Result<PersistedObservation> {
        let artifact = self.daemon.store.artifact(&reference.artifact_id)?;
        if artifact.kind != reference.kind {
            return Err(DaemonError::InvalidInput(format!(
                "artifact {} expected {:?}, found {:?}",
                reference.artifact_id, reference.kind, artifact.kind
            )));
        }
        let observation = PersistedObservation::from_artifact(&artifact);
        if artifact.producer != "execution.snapshot.account" || observation.source_uri.is_some() {
            return Ok(observation);
        }
        // A composed account snapshot has no single URL. Its four immutable
        // inputs must all identify the same service; a missing or mixed origin
        // remains Partial. Use the oldest input to preserve the freshness bound.
        let sources = artifact
            .source_refs
            .iter()
            .filter(|source| source.kind == ArtifactKind::NormalizedEvidence)
            .collect::<Vec<_>>();
        if sources.len() != 4 {
            return Ok(observation);
        }
        let mut combined: Option<(String, PersistedObservation)> = None;
        for source in sources {
            let source = self.daemon.store.artifact(&source.artifact_id)?;
            let current = PersistedObservation::from_artifact(&source);
            let Some((scheme, remainder)) = current
                .source_uri
                .as_deref()
                .and_then(|uri| uri.split_once("://"))
            else {
                return Ok(observation);
            };
            let host = remainder.split(['/', '?', '#']).next().unwrap_or_default();
            if host.is_empty() {
                return Ok(observation);
            }
            let origin = format!("{scheme}://{}", host.to_ascii_lowercase());
            if let Some((prior_origin, prior)) = &mut combined {
                if *prior_origin != origin || prior.source_family != current.source_family {
                    return Ok(observation);
                }
                prior.retrieved_at = prior.retrieved_at.min(current.retrieved_at);
            } else {
                combined = Some((origin, current));
            }
        }
        Ok(combined
            .map(|(_, observation)| observation)
            .unwrap_or(observation))
    }

    fn persisted_model_dependency(
        &self,
        run_id: &RunId,
        manifest: &RuntimeManifest,
        expected_freshness_secs: u64,
        now: DateTime<Utc>,
    ) -> Result<(DependencyKind, DependencySnapshot)> {
        let mut latest = None;
        for artifact in self
            .daemon
            .store
            .recent_artifacts_by_kind(ArtifactKind::AgentTurn, 500)?
        {
            if artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            {
                continue;
            }
            let payload: serde_json::Value =
                serde_json::from_slice(&self.daemon.store.read_blob(&artifact.blob)?)?;
            if payload
                .pointer("/request/purpose")
                .and_then(serde_json::Value::as_str)
                != Some("research.synthesizer")
            {
                continue;
            }
            let capability: akzio_model::ModelCapabilitySnapshot = serde_json::from_value(
                payload.get("capability_snapshot").cloned().ok_or_else(|| {
                    DaemonError::InvalidInput(
                        "persisted synthesizer AgentTurn has no capability snapshot".to_owned(),
                    )
                })?,
            )?;
            if latest
                .as_ref()
                .is_none_or(|(prior, _): &(Artifact, _)| prior.created_at < artifact.created_at)
            {
                latest = Some((artifact, capability));
            }
        }

        let fixture_identity =
            manifest.provider_id.contains("fixture") && manifest.model_id.contains("fixture");
        let configured_service = if fixture_identity {
            "fixture:fixture".to_owned()
        } else {
            format!("{}:{}", manifest.provider_id, manifest.model_id)
        };
        let (provider, service, version, observed_at, status, actual_service) =
            if let Some((artifact, capability)) = latest {
                let actual_service = format!("{}:{}", capability.provider_id, capability.model_id);
                (
                    capability.provider_id,
                    capability.model_id.clone(),
                    capability.model_id,
                    artifact.provenance.retrieved_at,
                    DependencyHealthStatus::Healthy,
                    actual_service,
                )
            } else {
                (
                    manifest.provider_id.clone(),
                    manifest.model_id.clone(),
                    manifest.model_id.clone(),
                    now,
                    DependencyHealthStatus::Unavailable,
                    configured_service.clone(),
                )
            };
        Ok(dependency_snapshot(
            DependencyDescriptor {
                kind: DependencyKind::ModelProvider,
                provider,
                service,
                version,
                data_classification: "model-input-output".to_owned(),
                fallback_policy: FallbackPolicy::RiskReductionOnly,
            },
            observed_at,
            expected_freshness_secs,
            status,
            configured_service,
            actual_service,
            BTreeSet::new(),
        ))
    }

    fn recent_compliance_activity(&self, now: DateTime<Utc>) -> Result<ComplianceActivitySnapshot> {
        let cutoff = now - chrono::Duration::days(1);
        let mut latest_receipts = BTreeMap::<String, OrderReceipt>::new();
        for artifact in self
            .daemon
            .store
            .recent_artifacts_by_kind(ArtifactKind::OrderReceipt, 500)?
        {
            let receipt: OrderReceipt = self.daemon.read_artifact_payload(&ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: ArtifactKind::OrderReceipt,
            })?;
            if receipt.observed_at < cutoff || receipt.observed_at > now {
                continue;
            }
            receipt
                .validate()
                .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
            let replace = latest_receipts
                .get(&receipt.client_order_id)
                .is_none_or(|prior| prior.observed_at < receipt.observed_at);
            if replace {
                latest_receipts.insert(receipt.client_order_id.clone(), receipt);
            }
        }

        let recent_submissions = u32::try_from(latest_receipts.len()).unwrap_or(u32::MAX);
        let recent_cancellations = u32::try_from(
            latest_receipts
                .values()
                .filter(|receipt| {
                    matches!(
                        receipt.state,
                        OrderReceiptState::PendingCancel
                            | OrderReceiptState::PendingReplace
                            | OrderReceiptState::Canceled
                            | OrderReceiptState::Replaced
                    )
                })
                .count(),
        )
        .unwrap_or(u32::MAX);

        let mut active_sides = BTreeMap::<Asset, (bool, bool)>::new();
        let plans = self
            .daemon
            .store
            .recent_artifacts_by_kind(ArtifactKind::ExecutionPlan, 500)?
            .into_iter()
            .map(|artifact| {
                self.daemon
                    .read_artifact_payload::<ExecutionPlan>(&ArtifactRef {
                        artifact_id: artifact.artifact_id,
                        kind: ArtifactKind::ExecutionPlan,
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        for receipt in latest_receipts.values().filter(|receipt| {
            matches!(
                receipt.state,
                OrderReceiptState::Accepted
                    | OrderReceiptState::PartiallyFilled
                    | OrderReceiptState::PendingCancel
                    | OrderReceiptState::PendingReplace
                    | OrderReceiptState::DoneForDay
                    | OrderReceiptState::Stopped
                    | OrderReceiptState::Suspended
                    | OrderReceiptState::Calculated
            )
        }) {
            if let Some(side) = plans
                .iter()
                .find(|plan| plan.plan_hash == receipt.plan_hash)
                .and_then(|plan| {
                    plan.orders
                        .iter()
                        .find(|order| order.asset == receipt.asset)
                        .map(|order| order.side)
                })
            {
                let sides = active_sides.entry(receipt.asset).or_default();
                match side {
                    OrderSide::Buy => sides.0 = true,
                    OrderSide::Sell => sides.1 = true,
                }
            }
        }

        Ok(ComplianceActivitySnapshot {
            recent_submissions,
            recent_cancellations,
            self_trade_detected: active_sides.values().any(|(buy, sell)| *buy && *sell),
            spoofing_or_layering_pattern: recent_submissions >= 6
                && recent_cancellations.saturating_mul(2) >= recent_submissions,
            marking_the_close_risk: false,
            prearranged_trade_risk: false,
            front_running_risk: false,
            available_controls: ComplianceControl::PAPER_BASELINE.into_iter().collect(),
        })
    }

    pub(crate) fn commit(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let verdict = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.daemon.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let ExecutionVerdict::Accepted { execution_context } = verdict_payload else {
            return Ok(TaskCompletion::NoOutput);
        };
        let context: ExecutionContext = self.daemon.read_artifact_payload(&execution_context)?;
        context
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let session_key = context.broker_session.ok_or_else(|| {
            DaemonError::InvalidInput("accepted execution verdict has no broker session".to_owned())
        })?;
        let lease = self.daemon.paper.scheduler.active_lease(now)?;
        self.daemon
            .paper_commitment_runtime
            .commit(&PaperCommitmentInput {
                lease,
                permit: task.permit.clone(),
                verdict,
                session_key,
                now,
            })?;
        Ok(TaskCompletion::Committed)
    }

    pub(crate) async fn reconcile(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.daemon.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(TaskCompletion::NoOutput);
        }
        let verdict = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.daemon.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        if matches!(verdict_payload, ExecutionVerdict::NoOrder { .. }) {
            return Ok(TaskCompletion::NoOutput);
        }
        let commitment = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionCommitment)?;
        if self.daemon.store.block_debug_broker_task(
            &task.permit,
            &[verdict.clone(), commitment.clone()],
            now,
        )? {
            return Ok(TaskCompletion::DeferredUntil(now + Duration::seconds(1)));
        }
        let broker = self.daemon.paper.paper_broker.as_ref().ok_or_else(|| {
            DaemonError::Unavailable(
                "Paper reconciliation requires an injected Alpaca Paper broker adapter".to_owned(),
            )
        })?;
        let lease = self.daemon.paper.scheduler.active_lease(now)?;
        let output = self
            .daemon
            .paper_dispatch_runtime
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
        if output.settled {
            Ok(TaskCompletion::Committed)
        } else {
            Ok(TaskCompletion::DeferredUntil(
                now + chrono::Duration::seconds(1),
            ))
        }
    }
}

#[derive(Clone)]
struct PersistedObservation {
    retrieved_at: DateTime<Utc>,
    source_family: String,
    source_uri: Option<String>,
    network_host: Option<String>,
}

impl PersistedObservation {
    fn from_artifact(artifact: &Artifact) -> Self {
        let source_uri = artifact.provenance.source_uri.clone();
        Self {
            retrieved_at: artifact.provenance.retrieved_at,
            source_family: artifact.provenance.source_family.clone(),
            network_host: source_uri.as_deref().and_then(network_host),
            source_uri,
        }
    }
}

#[derive(Default)]
struct EvidenceDependencyAggregate {
    providers: BTreeSet<String>,
    services: BTreeSet<String>,
    versions: BTreeSet<String>,
    fourth_parties: BTreeSet<String>,
    latest_retrieval: Option<DateTime<Utc>>,
    partial: bool,
}

impl EvidenceDependencyAggregate {
    fn observe(&mut self, payload: &NormalizedEvidencePayload, observation: &PersistedObservation) {
        self.providers.insert(observation.source_family.clone());
        self.services.insert(payload.source.as_str().to_owned());
        self.versions.insert(
            payload
                .provenance
                .revision
                .clone()
                .unwrap_or_else(|| "unversioned".to_owned()),
        );
        self.fourth_parties
            .extend(observation.network_host.iter().cloned());
        self.latest_retrieval = Some(
            self.latest_retrieval
                .map_or(payload.time_basis.retrieved_at, |prior| {
                    prior.max(payload.time_basis.retrieved_at)
                }),
        );
        self.partial |= payload.quality.completeness_ppm < WeightPpm::SCALE
            || !payload.quality.citations_complete
            || !payload.quality.normalized;
    }

    fn snapshot(
        &self,
        kind: DependencyKind,
        decision_created_at: DateTime<Utc>,
        captured_at: DateTime<Utc>,
    ) -> (DependencyKind, DependencySnapshot) {
        let Some(observed_at) = self.latest_retrieval else {
            return dependency_snapshot(
                DependencyDescriptor {
                    kind,
                    provider: "akzio".to_owned(),
                    service: "not-required".to_owned(),
                    version: "not-observed".to_owned(),
                    data_classification: "not-read".to_owned(),
                    fallback_policy: FallbackPolicy::FailClosed,
                },
                decision_created_at.min(captured_at),
                300,
                DependencyHealthStatus::NotRequired,
                "not-required".to_owned(),
                "not-required".to_owned(),
                BTreeSet::new(),
            );
        };
        let provider = joined_identity(&self.providers);
        let service = joined_identity(&self.services);
        dependency_snapshot(
            DependencyDescriptor {
                kind,
                provider,
                service: service.clone(),
                version: joined_identity(&self.versions),
                data_classification: "decision-evidence".to_owned(),
                fallback_policy: FallbackPolicy::FailClosed,
            },
            observed_at,
            300,
            if self.partial {
                DependencyHealthStatus::Partial
            } else {
                DependencyHealthStatus::Healthy
            },
            service.clone(),
            service,
            self.fourth_parties.clone(),
        )
    }
}

struct DependencyDescriptor {
    kind: DependencyKind,
    provider: String,
    service: String,
    version: String,
    data_classification: String,
    fallback_policy: FallbackPolicy,
}

fn dependency_snapshot(
    descriptor: DependencyDescriptor,
    observed_at: DateTime<Utc>,
    expected_freshness_secs: u64,
    status: DependencyHealthStatus,
    configured_service: String,
    actual_service: String,
    fourth_party_dependencies: BTreeSet<String>,
) -> (DependencyKind, DependencySnapshot) {
    let DependencyDescriptor {
        kind,
        provider,
        service,
        version,
        data_classification,
        fallback_policy,
    } = descriptor;
    (
        kind,
        DependencySnapshot {
            kind,
            provider,
            service,
            version,
            data_classification,
            expected_freshness_secs,
            observed_at,
            status,
            fallback_policy,
            configured_service,
            actual_service,
            fourth_party_dependencies,
        },
    )
}

fn market_data_dependency(
    configured_feed: &str,
    observation: &PersistedObservation,
    _captured_at: DateTime<Utc>,
) -> (DependencyKind, DependencySnapshot) {
    let observed_feed = observation
        .source_uri
        .as_deref()
        .and_then(|uri| query_parameter(uri, "feed"))
        .map(str::to_ascii_lowercase);
    let fixture = observation
        .source_uri
        .as_deref()
        .is_some_and(|uri| uri.starts_with("fixture://"));
    let status = if !configured_feed.eq_ignore_ascii_case("sip") {
        DependencyHealthStatus::Degraded
    } else if observed_feed.is_none() && !fixture {
        DependencyHealthStatus::Partial
    } else {
        DependencyHealthStatus::Healthy
    };
    let actual_feed = observed_feed.unwrap_or_else(|| {
        if fixture {
            configured_feed.to_ascii_lowercase()
        } else {
            "unobserved-feed".to_owned()
        }
    });
    dependency_snapshot(
        DependencyDescriptor {
            kind: DependencyKind::MarketData,
            provider: observation.source_family.clone(),
            service: actual_feed.clone(),
            version: actual_feed.clone(),
            data_classification: "market-data".to_owned(),
            fallback_policy: FallbackPolicy::FailClosed,
        },
        observation.retrieved_at,
        30,
        status,
        configured_feed.to_ascii_lowercase(),
        actual_feed,
        observation.network_host.iter().cloned().collect(),
    )
}

fn internal_healthy_dependency(
    kind: DependencyKind,
    service: &str,
    version: &str,
    data_classification: &str,
    now: DateTime<Utc>,
) -> (DependencyKind, DependencySnapshot) {
    dependency_snapshot(
        DependencyDescriptor {
            kind,
            provider: "akzio".to_owned(),
            service: service.to_owned(),
            version: version.to_owned(),
            data_classification: data_classification.to_owned(),
            fallback_policy: FallbackPolicy::FailClosed,
        },
        now,
        30,
        DependencyHealthStatus::Healthy,
        service.to_owned(),
        service.to_owned(),
        BTreeSet::new(),
    )
}

fn alpaca_service_dependency(
    kind: DependencyKind,
    data_classification: &str,
    observation: &PersistedObservation,
    _captured_at: DateTime<Utc>,
) -> (DependencyKind, DependencySnapshot) {
    let fixture = observation
        .source_uri
        .as_deref()
        .is_some_and(|uri| uri.starts_with("fixture://"));
    let configured_service = if fixture {
        "fixture-alpaca".to_owned()
    } else {
        "paper-api.alpaca.markets".to_owned()
    };
    let actual_service = observation
        .network_host
        .clone()
        .unwrap_or_else(|| configured_service.clone());
    dependency_snapshot(
        DependencyDescriptor {
            kind,
            provider: observation.source_family.clone(),
            service: actual_service.clone(),
            version: "v2".to_owned(),
            data_classification: data_classification.to_owned(),
            fallback_policy: FallbackPolicy::FailClosed,
        },
        observation.retrieved_at,
        30,
        if observation.source_uri.is_some() {
            DependencyHealthStatus::Healthy
        } else {
            DependencyHealthStatus::Partial
        },
        configured_service,
        actual_service,
        observation.network_host.iter().cloned().collect(),
    )
}

fn dns_dependency(
    successful_network_observations: &[PersistedObservation],
    fixture_runtime: bool,
    captured_at: DateTime<Utc>,
) -> (DependencyKind, DependencySnapshot) {
    let hosts = successful_network_observations
        .iter()
        .filter_map(|observation| observation.network_host.clone())
        .collect::<BTreeSet<_>>();
    let observed_at = successful_network_observations
        .iter()
        .filter(|observation| observation.network_host.is_some())
        .map(|observation| observation.retrieved_at)
        .max()
        .unwrap_or(captured_at);
    let status = if !hosts.is_empty() {
        DependencyHealthStatus::Healthy
    } else if fixture_runtime {
        DependencyHealthStatus::NotApplicable
    } else {
        DependencyHealthStatus::Unavailable
    };
    dependency_snapshot(
        DependencyDescriptor {
            kind: DependencyKind::Dns,
            provider: "derived".to_owned(),
            service: "successful-network-io".to_owned(),
            version: "v1".to_owned(),
            data_classification: "network-health".to_owned(),
            fallback_policy: FallbackPolicy::FailClosed,
        },
        observed_at,
        30,
        status,
        "successful-network-io".to_owned(),
        "successful-network-io".to_owned(),
        hosts,
    )
}

fn network_host(uri: &str) -> Option<String> {
    let remainder = uri
        .strip_prefix("https://")
        .or_else(|| uri.strip_prefix("http://"))?;
    let host = remainder.split(['/', '?', '#']).next()?.trim();
    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

fn query_parameter<'a>(uri: &'a str, key: &str) -> Option<&'a str> {
    uri.split_once('?')?
        .1
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(candidate, value)| (candidate == key && !value.is_empty()).then_some(value))
}

fn joined_identity(values: &BTreeSet<String>) -> String {
    values.iter().cloned().collect::<Vec<_>>().join("+")
}
