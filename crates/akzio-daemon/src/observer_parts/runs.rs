impl Daemon {
    fn observer_outcome(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<ObserverSection<ObserverOutcome>> {
        let artifacts = self
            .store
            .recent_artifacts_by_kind(ArtifactKind::Outcome, 100)?;
        let mut decoded = artifacts
            .into_iter()
            .map(|artifact| {
                let outcome: Outcome =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                Ok::<_, DaemonError>((artifact, outcome))
            })
            .collect::<Result<Vec<_>>>()?;
        if decoded.is_empty() {
            return Ok(ObserverSection::pending(
                "No canonical Outcome is available yet",
            ));
        }
        decoded.sort_by_key(|(artifact, outcome)| {
            (
                outcome.windows.len(),
                outcome.sealed_at,
                artifact.created_at,
            )
        });
        let (_, current) = decoded.last().expect("non-empty Outcome list");
        let statistics = outcome_statistics(
            &decoded
                .iter()
                .filter(|(_, outcome)| outcome.is_sealed())
                .map(|(_, outcome)| outcome.clone())
                .collect::<Vec<_>>(),
        );
        let comparison = self
            .observer_outcome_comparison(current)
            .unwrap_or_default();
        let completed_trading_sessions = current
            .windows
            .iter()
            .map(|window| window.horizon.trading_days())
            .max()
            .unwrap_or(0);
        let horizons = OutcomeHorizon::ALL
            .into_iter()
            .map(|horizon| {
                let stats = statistics
                    .iter()
                    .find(|stats| stats.horizon == horizon)
                    .cloned()
                    .unwrap_or(ObserverOutcomeStatistics {
                        horizon,
                        sample_count: 0,
                        win_rate_ppm: None,
                        profit_factor_ppm: None,
                        sharpe_ppm: None,
                    });
                let window = current
                    .windows
                    .iter()
                    .find(|window| window.horizon == horizon)
                    .cloned();
                let horizon_comparison = window
                    .as_ref()
                    .map(|window| window.observed_trading_day)
                    .map(|day| {
                        comparison
                            .iter()
                            .filter(|point| point.trading_day <= day)
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                ObserverOutcomeHorizon {
                    horizon,
                    progress_ppm: u32::from(completed_trading_sessions.min(horizon.trading_days()))
                        * 1_000_000
                        / u32::from(horizon.trading_days()),
                    window,
                    sample_count: stats.sample_count,
                    win_rate_ppm: stats.win_rate_ppm,
                    profit_factor_ppm: stats.profit_factor_ppm,
                    sharpe_ppm: stats.sharpe_ppm,
                    max_drawdown_ppm: comparison_max_drawdown_ppm(&horizon_comparison),
                    comparison: horizon_comparison,
                }
            })
            .collect();
        Ok(ObserverSection::available(
            observed_at,
            ObserverOutcome {
                outcome_id: current.outcome_id.0.clone(),
                completed_trading_sessions,
                horizons,
            },
        ))
    }

    fn observer_outcome_comparison(
        &self,
        outcome: &Outcome,
    ) -> Result<Vec<ObserverOutcomeComparisonPoint>> {
        let schedule_artifact = self.store.artifact(&outcome.schedule.artifact_id)?;
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&self.store.read_blob(&schedule_artifact.blob)?)?;
        let execution_context_artifact = self
            .store
            .artifact(&schedule.execution_context.artifact_id)?;
        let execution_context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&execution_context_artifact.blob)?)?;
        let target = self.realized_execution_target(&schedule, &execution_context)?;
        let quote_reference = execution_context.quote_snapshot.as_ref().ok_or_else(|| {
            DaemonError::Unavailable("Outcome has no baseline QuoteSnapshot".to_owned())
        })?;
        let quote_artifact = self.store.artifact(&quote_reference.artifact_id)?;
        let quotes: QuoteSnapshot =
            serde_json::from_slice(&self.store.read_blob(&quote_artifact.blob)?)?;
        let baseline_prices = quotes
            .quotes
            .into_iter()
            .map(|(asset, quote)| {
                let midpoint = quote
                    .bid
                    .0
                    .checked_add(quote.ask.0)
                    .and_then(|value| value.checked_div(2))
                    .unwrap_or_default();
                (asset, MoneyMicros(midpoint))
            })
            .collect::<BTreeMap<_, _>>();
        let mut bars_by_asset = BTreeMap::new();
        for reference in &outcome.market_evidence {
            if reference.kind != ArtifactKind::NormalizedEvidence {
                continue;
            }
            let artifact = self.store.artifact(&reference.artifact_id)?;
            let payload: NormalizedEvidencePayload =
                serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
            let mut parts = payload.resource.split(':');
            if parts.next() != Some("bars") {
                continue;
            }
            let Some(symbol) = parts.next() else {
                continue;
            };
            let Ok(asset) = Asset::try_from(symbol) else {
                continue;
            };
            bars_by_asset.insert(
                asset,
                parse_daily_bars(&payload.value, payload.observed_at)?,
            );
        }
        outcome_comparison(
            &target,
            &baseline_prices,
            &bars_by_asset,
            schedule.baseline_trading_day,
        )
        .map_err(DaemonError::Unavailable)
    }

    fn observer_learning(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<ObserverSection<ObserverLearning>> {
        let mut artifacts = Vec::new();
        let mut seen = BTreeSet::new();
        for kind in [
            ArtifactKind::OutcomeSchedule,
            ArtifactKind::Outcome,
            ArtifactKind::Retrospective,
            ArtifactKind::Experience,
            ArtifactKind::Evaluation,
        ] {
            for artifact in self
                .store
                .recent_artifacts_by_kind(kind, OBSERVER_LEARNING_LIMIT)?
            {
                if seen.insert(artifact.artifact_id.clone()) {
                    if let Some(view) = self.observer_artifact_view(&artifact)? {
                        artifacts.push(view);
                    }
                }
            }
        }
        artifacts.sort_by_key(|artifact| artifact.created_at);
        if artifacts.len() > OBSERVER_LEARNING_LIMIT {
            artifacts.drain(..artifacts.len() - OBSERVER_LEARNING_LIMIT);
        }
        let mut subjects = Vec::new();
        for artifact in &artifacts {
            if artifact.kind == ArtifactKind::Experience {
                let experience: Experience = serde_json::from_value(artifact.payload.clone())?;
                if !subjects.contains(&experience.subject) {
                    subjects.push(experience.subject);
                }
            }
        }
        let mut policy_transitions = Vec::new();
        for subject in subjects {
            policy_transitions.extend(self.store.policy_transitions(&subject)?.into_iter().map(
                |record| ObserverPolicyTransition {
                    transition: record.transition,
                    run_id: record.run_id,
                    revision: record.revision,
                    transition_cursor: record.transition_cursor,
                },
            ));
        }
        policy_transitions.sort_by_key(|record| record.transition_cursor);
        let (summary, policy_metrics) =
            self.observer_learning_analytics(observed_at, &policy_transitions)?;
        if artifacts.is_empty() && policy_transitions.is_empty() {
            return Ok(ObserverSection::pending(
                "No canonical learning artifacts are available yet",
            ));
        }
        Ok(ObserverSection::available(
            observed_at,
            ObserverLearning {
                artifacts,
                policy_transitions,
                summary,
                policy_metrics,
            },
        ))
    }
}
