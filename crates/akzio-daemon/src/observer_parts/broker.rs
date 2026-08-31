use super::*;

impl Daemon {
    pub(super) fn observer_artifacts(
        &self,
        trajectory: &[TrajectoryEntry],
        include: impl Fn(ArtifactKind) -> bool,
    ) -> Result<Vec<ObserverArtifactView>> {
        let mut seen = BTreeSet::new();
        let mut artifacts = Vec::new();
        for entry in trajectory {
            let (Some(artifact_id), Some(kind)) = (&entry.artifact_id, entry.artifact_kind) else {
                continue;
            };
            if !include(kind) || !seen.insert(artifact_id.clone()) {
                continue;
            }
            let artifact = self.store.artifact(artifact_id)?;
            if let Some(view) = self.observer_artifact_view(&artifact)? {
                artifacts.push(view);
            }
        }
        artifacts.sort_by_key(|artifact| artifact.created_at);
        Ok(artifacts)
    }

    pub(super) fn observer_artifact_view(
        &self,
        artifact: &Artifact,
    ) -> Result<Option<ObserverArtifactView>> {
        let payload = match artifact.kind {
            ArtifactKind::WorkflowProposalDraft => {
                self.typed_observer_payload::<WorkflowProposalDraft>(artifact)?
            }
            ArtifactKind::Claim => self.typed_observer_payload::<ResearchClaim>(artifact)?,
            ArtifactKind::Critique => self.typed_observer_payload::<ResearchCritique>(artifact)?,
            ArtifactKind::DecisionProposal => {
                self.typed_observer_payload::<DecisionProposal>(artifact)?
            }
            ArtifactKind::DecisionContext => {
                self.typed_observer_payload::<DecisionContext>(artifact)?
            }
            ArtifactKind::Decision => self.typed_observer_payload::<Decision>(artifact)?,
            ArtifactKind::ExecutionContext => {
                self.typed_observer_payload::<ExecutionContext>(artifact)?
            }
            ArtifactKind::ExecutionVerdict => {
                self.typed_observer_payload::<ExecutionVerdict>(artifact)?
            }
            ArtifactKind::ExecutionPlan => {
                self.typed_observer_payload::<ExecutionPlan>(artifact)?
            }
            ArtifactKind::OrderReceipt => self.typed_observer_payload::<OrderReceipt>(artifact)?,
            ArtifactKind::Reconciliation => {
                self.typed_observer_payload::<Reconciliation>(artifact)?
            }
            ArtifactKind::OutcomeSchedule => {
                self.typed_observer_payload::<OutcomeSchedule>(artifact)?
            }
            ArtifactKind::Outcome => self.typed_observer_payload::<Outcome>(artifact)?,
            ArtifactKind::RetrospectiveDraft => {
                self.typed_observer_payload::<RetrospectiveDraft>(artifact)?
            }
            ArtifactKind::Retrospective => {
                self.typed_observer_payload::<Retrospective>(artifact)?
            }
            ArtifactKind::Experience => self.typed_observer_payload::<Experience>(artifact)?,
            ArtifactKind::Evaluation => self.typed_observer_payload::<Evaluation>(artifact)?,
            _ => return Ok(None),
        };
        Ok(Some(ObserverArtifactView {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
            created_at: artifact.created_at,
            payload,
        }))
    }

    fn typed_observer_payload<T>(&self, artifact: &Artifact) -> Result<Value>
    where
        T: DeserializeOwned + Serialize,
    {
        Ok(serde_json::to_value(serde_json::from_slice::<T>(
            &self.store.read_blob(&artifact.blob)?,
        )?)?)
    }

    pub(super) fn observer_approval(&self, now: DateTime<Utc>) -> Result<ObserverApprovalStatus> {
        let Some(artifact) = self
            .store
            .latest_artifact_by_kind(ArtifactKind::PaperLaunchApproval)?
        else {
            return Ok(ObserverApprovalStatus {
                status: "missing".to_owned(),
                operator_identity: None,
                reason: None,
                expires_at: None,
            });
        };
        let approval: PaperLaunchApproval =
            serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
        let manifest_artifact = self
            .store
            .artifact(&approval.runtime_manifest.artifact_id)?;
        let manifest: RuntimeManifest =
            serde_json::from_slice(&self.store.read_blob(&manifest_artifact.blob)?)?;
        let manifest_identity_hash = manifest.runtime_identity_hash()?;
        let status = if approval.expires_at < now {
            "expired"
        } else if self
            .paper
            .runtime_identity_hash
            .as_ref()
            .is_some_and(|expected| expected != &manifest_identity_hash)
        {
            "mismatched"
        } else {
            "valid"
        };
        Ok(ObserverApprovalStatus {
            status: status.to_owned(),
            operator_identity: Some(approval.operator_identity),
            reason: Some(approval.reason),
            expires_at: Some(approval.expires_at),
        })
    }

    pub(super) async fn observer_portfolio(
        &self,
        observed_at: DateTime<Utc>,
        current_run: Option<&ObserverRunDetail>,
    ) -> ObserverSection<ObserverPortfolio> {
        let Some(paper) = self.paper.paper_observer.as_ref() else {
            return ObserverSection::unavailable("Alpaca Paper observer is not configured");
        };
        let current = tokio::time::timeout(OBSERVER_BROKER_TIMEOUT, async {
            tokio::try_join!(paper.account(), paper.positions(), paper.market_clock())
        })
        .await;
        let (account, positions, clock) = match current {
            Ok(Ok(values)) => values,
            Ok(Err(error)) => return ObserverSection::unavailable(error.to_string()),
            Err(_) => {
                return ObserverSection::unavailable("Alpaca Paper account snapshot timed out");
            }
        };
        let mut portfolio = match parse_portfolio(
            &account,
            &positions,
            &clock.session_date.to_string(),
            clock.is_open,
        ) {
            Ok(portfolio) => portfolio,
            Err(error) => return ObserverSection::unavailable(error.to_string()),
        };

        if let Some(run) = current_run {
            let operation = self.clone();
            let run_id = run.workflow.run.run_id.clone();
            if let Ok(Ok(sparklines)) = self
                .store_executor
                .execute(move |_| operation.observer_position_sparklines(&run_id))
                .await
            {
                for position in &mut portfolio.positions {
                    position.sparkline_ppm = sparklines
                        .get(&position.symbol.to_ascii_uppercase())
                        .cloned()
                        .unwrap_or_default();
                }
            }
        }

        let broker_session = clock.session_date.to_string();
        let optional = tokio::time::timeout(OBSERVER_BROKER_TIMEOUT, async {
            tokio::join!(
                paper.portfolio_history(PortfolioHistoryRange::ThreeMonths),
                self.observer_qqq_bars(ObserverPortfolioRange::ThreeMonths, observed_at),
                self.observer_fill_activities(&broker_session)
            )
        })
        .await;
        if let Ok((history_result, bars_result, fills_result)) = optional {
            portfolio.analytics = match (history_result, bars_result) {
                (Ok(history), Ok(bars)) => {
                    parse_portfolio_history(ObserverPortfolioRange::ThreeMonths, &history)
                        .and_then(|history| {
                            portfolio_analytics(
                                &history
                                    .points
                                    .iter()
                                    .map(|point| (point.timestamp, point.equity_micros))
                                    .collect::<Vec<_>>(),
                                &bars,
                                portfolio.equity_micros,
                            )
                            .map_err(DaemonError::Unavailable)
                        })
                        .map(|analytics| ObserverSection::available(observed_at, analytics))
                        .unwrap_or_else(|error| ObserverSection::unavailable(error.to_string()))
                }
                (Err(error), _) => ObserverSection::unavailable(error.to_string()),
                (_, Err(error)) => ObserverSection::unavailable(error.to_string()),
            };

            portfolio.fills = match fills_result {
                Ok(value) => {
                    let order_ids = current_run
                        .map(observer_broker_order_ids)
                        .unwrap_or_default();
                    match parse_fill_activities(&value, &order_ids) {
                        Ok(fills) => {
                            if let Some(run) = current_run {
                                let operation = self.clone();
                                let run_id = run.workflow.run.run_id.clone();
                                let normalized = self
                                    .store_executor
                                    .execute(move |_| {
                                        (
                                            operation.observer_normalized_resource(
                                                &run_id,
                                                PAPER_POSITIONS_RESOURCE,
                                            ),
                                            operation.observer_normalized_resource(
                                                &run_id,
                                                PAPER_ACCOUNT_RESOURCE,
                                            ),
                                        )
                                    })
                                    .await
                                    .ok();
                                if let Some((Some(opening_positions), Some(opening_equity))) =
                                    normalized.map(|(positions, account)| {
                                        (
                                            positions,
                                            account.and_then(|account| {
                                                account
                                                    .get("equity")
                                                    .and_then(parse_money_micros)
                                                    .map(|value| value.0)
                                            }),
                                        )
                                    })
                                {
                                    if let Ok(realized) =
                                        managed_realized_pnl(&opening_positions, &fills)
                                    {
                                        portfolio.realized_pnl_micros = Some(realized);
                                        portfolio.realized_pnl_ppm = (opening_equity != 0)
                                            .then(|| {
                                                i128::from(realized) * 1_000_000
                                                    / i128::from(opening_equity)
                                            })
                                            .and_then(|value| i64::try_from(value).ok());
                                    }
                                }
                            }
                            ObserverSection::available(observed_at, fills)
                        }
                        Err(error) => ObserverSection::unavailable(error),
                    }
                }
                Err(error) => ObserverSection::unavailable(error.to_string()),
            };
        } else {
            portfolio.analytics = ObserverSection::unavailable("Portfolio analytics timed out");
            portfolio.fills = ObserverSection::unavailable("Alpaca fill activities timed out");
        }
        ObserverSection::available(observed_at, portfolio)
    }

    async fn observer_fill_activities(&self, broker_session: &str) -> Result<Value> {
        let adapter = self
            .production_evidence
            .get(&EvidenceSource::Alpaca)
            .ok_or_else(|| {
                DaemonError::Unavailable("Alpaca evidence is not configured".to_owned())
            })?;
        let acquired = adapter
            .acquire(&EvidenceRequest {
                source: EvidenceSource::Alpaca,
                resource: format!("paper.fills:{broker_session}"),
                max_age: Duration::days(1),
                acquisition_mode: EvidenceAcquisitionMode::VerifiedSource,
            })
            .await
            .map_err(|error| DaemonError::Unavailable(error.to_string()))?;
        Ok(serde_json::from_slice(&acquired.raw)?)
    }

    fn observer_normalized_resource(&self, run_id: &RunId, resource: &str) -> Option<Value> {
        self.store
            .recent_artifacts_by_kind(ArtifactKind::NormalizedEvidence, 500)
            .ok()?
            .into_iter()
            .find_map(|artifact| {
                if artifact.origin.as_ref()?.run_id.as_ref()? != run_id {
                    return None;
                }
                let payload: NormalizedEvidencePayload =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob).ok()?).ok()?;
                (payload.resource == resource).then_some(payload.value)
            })
    }

    fn observer_position_sparklines(&self, run_id: &RunId) -> Result<BTreeMap<String, Vec<i64>>> {
        let mut sparklines = BTreeMap::new();
        for artifact in self
            .store
            .recent_artifacts_by_kind(ArtifactKind::NormalizedEvidence, 500)?
        {
            if artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            {
                continue;
            }
            let payload: NormalizedEvidencePayload =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            let mut parts = payload.resource.split(':');
            if parts.next() != Some("bars") {
                continue;
            }
            let Some(symbol) = parts.next() else {
                continue;
            };
            let bars = parse_daily_bars(&payload.value, payload.observed_at)?;
            let Some(opening) = bars.values().next().filter(|price| price.0 > 0) else {
                continue;
            };
            let values = bars
                .values()
                .filter_map(|price| {
                    i64::try_from(i128::from(price.0) * 1_000_000 / i128::from(opening.0)).ok()
                })
                .collect::<Vec<_>>();
            sparklines.insert(symbol.to_ascii_uppercase(), values);
        }
        Ok(sparklines)
    }
}
