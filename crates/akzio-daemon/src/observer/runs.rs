// 文件导读：runs observer 读取 canonical Outcome 并按 horizon 展示进度、窗口、统计和
// portfolio/QQQ 比较；同时聚合 learning Artifact。读取旧 Outcome、部分窗口或 NoOrder
// 都保持其原始语义，不能把 Observer 的 progress 变成真实成交、账户 NAV 或 T+5 学习资格。
// Rust 机制：迭代器 `filter/map/collect` 组成稳定 projection；Artifact payload 通过泛型
// 读取并用 `Result` 校验；`Option` 区分没有窗口/没有基线/未计算 comparison。

use super::*;

impl Daemon {
    pub(super) fn observer_outcome(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<ObserverSection<ObserverOutcome>> {
        // 选择已有 Outcome 中窗口最完整/最新的一份作为当前投影，并只用 sealed Outcome
        // 计算统计；partial/NoOrder 的窗口仍展示其状态，不被升级成完成样本。
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
        let (current_artifact, current) = decoded.last().expect("non-empty Outcome list");
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
                metric_basis: current.metric_basis,
                actual_account_nav_available: false,
                lifecycle: current_artifact
                    .origin
                    .as_ref()
                    .and_then(|o| o.run_id.as_ref())
                    .map(|id| self.store.run_lifecycle_health(id))
                    .transpose()?,
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
        // 优先使用 Outcome 已冻结的 nav_path；否则从 baseline quotes + 四资产共同 bars
        // 重建比较曲线。缺 baseline/price 时返回 unavailable，而不是填 1.0。
        if let Some(window) = outcome.windows.iter().max_by_key(|w| w.horizon) {
            if !window.nav_path.is_empty() {
                let schedule: OutcomeSchedule = self.read_artifact_payload(&outcome.schedule)?;
                return Ok(std::iter::once(ObserverOutcomeComparisonPoint {
                    trading_day: schedule.baseline_trading_day,
                    portfolio_ppm: 1_000_000,
                    benchmark_ppm: 1_000_000,
                })
                .chain(
                    window
                        .nav_path
                        .iter()
                        .map(|point| ObserverOutcomeComparisonPoint {
                            trading_day: point.observed_trading_day,
                            portfolio_ppm: point.portfolio_nav_ppm,
                            benchmark_ppm: point.benchmark_nav_ppm,
                        }),
                )
                .collect());
            }
        }
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

    pub(super) fn observer_learning(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<ObserverSection<ObserverLearning>> {
        // 跨 Outcome/Retrospective/Experience/Evaluation 收集只读 Artifact 并按 subject 查
        // policy transitions；空集合保持 pending，不创建或推进任何学习状态。
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
