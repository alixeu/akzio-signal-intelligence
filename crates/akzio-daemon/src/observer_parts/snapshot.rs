impl Daemon {
    pub(crate) async fn observer_snapshot(&self) -> Result<ObserverSnapshot> {
        let generated_at = Utc::now();
        let operation = self.clone();
        let (recent_runs, current_run, run_summaries, health, ready, outcome, learning) = self
            .store_executor
            .execute(move |_| -> Result<_> {
                let recent_runs = operation.store.recent_workflows(OBSERVER_RUN_LIMIT)?;
                let current_run = recent_runs
                    .first()
                    .map(|workflow| operation.observer_run_detail(&workflow.run.run_id))
                    .transpose()?;
                let run_summaries = recent_runs
                    .iter()
                    .map(|workflow| operation.observer_run_summary(&workflow.run.run_id))
                    .collect::<Result<Vec<_>>>()?;
                let health = operation.health()?;
                let ready = operation.ready().is_ok();
                let outcome = operation.observer_outcome(generated_at)?;
                let learning = operation.observer_learning(generated_at)?;
                Ok((
                    recent_runs,
                    current_run,
                    run_summaries,
                    health,
                    ready,
                    outcome,
                    learning,
                ))
            })
            .await??;
        let portfolio = self
            .observer_portfolio(generated_at, current_run.as_ref())
            .await;
        let operation = self.clone();
        let (event_cursor, approval) = self
            .store_executor
            .execute(move |_| -> Result<_> {
                Ok((
                    operation.store.event_cursor()?,
                    operation.observer_approval(generated_at)?,
                ))
            })
            .await??;

        Ok(ObserverSnapshot {
            schema_version: 2,
            generated_at,
            event_cursor,
            core: ObserverCoreStatus {
                ready,
                readiness_ppm: readiness_ppm(
                    ready,
                    health.frozen,
                    self.paper.auto_paper,
                    health.scheduler_owner.is_some(),
                ),
                auto_paper: self.paper.auto_paper,
                health,
                approval,
            },
            current_run,
            recent_runs,
            run_summaries,
            portfolio,
            outcome,
            learning,
        })
    }

    pub(crate) fn observer_run_detail(&self, run_id: &RunId) -> Result<ObserverRunDetail> {
        let workflow = self.store.workflow_snapshot(run_id)?;
        let events = self
            .store
            .recent_events(run_id, OBSERVER_EVENT_LIMIT)?
            .into_iter()
            .map(|event| EventView {
                cursor: event.cursor,
                event_type: event.event_type,
                task_id: event.task_id.map(|task| task.0),
                created_at: event.created_at.to_rfc3339(),
            })
            .collect();
        let trajectory = self
            .store
            .recent_trajectory(run_id, OBSERVER_TRAJECTORY_LIMIT)?;
        let artifacts = self.observer_artifacts(&trajectory, |_| true)?;
        let telemetry = observer_run_telemetry(&trajectory);

        Ok(ObserverRunDetail {
            workflow,
            events,
            trajectory,
            artifacts,
            telemetry,
        })
    }

    fn observer_run_summary(&self, run_id: &RunId) -> Result<ObserverRunSummary> {
        let trajectory = self.store.recent_trajectory(run_id, 64)?;
        let telemetry = observer_run_telemetry(&trajectory);
        let result_utility_ppm = trajectory
            .iter()
            .rev()
            .find_map(|entry| {
                (entry.artifact_kind == Some(ArtifactKind::Outcome))
                    .then_some(entry.artifact_id.as_ref())
                    .flatten()
            })
            .map(|artifact_id| -> Result<Option<i64>> {
                let artifact = self.store.artifact(artifact_id)?;
                let outcome: Outcome =
                    serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                Ok(outcome
                    .windows
                    .iter()
                    .max_by_key(|window| window.horizon.trading_days())
                    .map(|window| window.utility_ppm))
            })
            .transpose()?
            .flatten();
        Ok(ObserverRunSummary {
            run_id: run_id.clone(),
            model_id: telemetry.model_id,
            latency_millis: telemetry.latency_millis,
            broker_session: self
                .store
                .session_slot_for_run(run_id)?
                .map(|slot| slot.session_key),
            result_utility_ppm,
        })
    }

    pub(crate) async fn observer_portfolio_history(
        &self,
        range: ObserverPortfolioRange,
    ) -> ObserverSection<ObserverPortfolioHistory> {
        let Some(paper) = self.paper.paper_observer.as_ref() else {
            return ObserverSection::unavailable("Alpaca Paper observer is not configured");
        };
        let now = Utc::now();
        let result = tokio::time::timeout(OBSERVER_BROKER_TIMEOUT, async {
            tokio::join!(
                paper.portfolio_history(range.paper_range()),
                self.observer_qqq_bars(range, now)
            )
        })
        .await;
        let Ok((history_result, benchmark_result)) = result else {
            return ObserverSection::unavailable("Alpaca Paper portfolio history timed out");
        };
        let value = match history_result {
            Ok(value) => value,
            Err(error) => return ObserverSection::unavailable(error.to_string()),
        };
        let mut history = match parse_portfolio_history(range, &value) {
            Ok(history) => history,
            Err(error) => return ObserverSection::unavailable(error.to_string()),
        };
        if let Ok(benchmark) = benchmark_result {
            let portfolio = history
                .points
                .iter()
                .map(|point| (point.timestamp, point.equity_micros))
                .collect::<Vec<_>>();
            for (point, benchmark_equity) in history
                .points
                .iter_mut()
                .zip(benchmark_equity_series(&portfolio, &benchmark))
            {
                point.benchmark_equity_micros = benchmark_equity;
            }
        }
        ObserverSection::available(now, history)
    }

    async fn observer_qqq_bars(
        &self,
        range: ObserverPortfolioRange,
        now: DateTime<Utc>,
    ) -> Result<Vec<crate::observer_analytics::ObserverBarPoint>> {
        let adapter = self
            .production_evidence
            .get(&EvidenceSource::Alpaca)
            .ok_or_else(|| {
                DaemonError::Unavailable("Alpaca market data is not configured".to_owned())
            })?;
        let resource = format!(
            "observer.qqq_history:{}:{}",
            range.as_query_value(),
            range.benchmark_start(now)
        );
        let acquired = adapter
            .acquire(&EvidenceRequest {
                source: EvidenceSource::Alpaca,
                resource,
                max_age: Duration::days(1),
            })
            .await
            .map_err(|error| DaemonError::Unavailable(error.to_string()))?;
        let value: Value = serde_json::from_slice(&acquired.raw)?;
        parse_bar_series(&value).map_err(DaemonError::Unavailable)
    }
}
