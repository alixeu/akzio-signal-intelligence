impl ExecutionRuntime {
    fn load_account(
        &self,
        input: &ExecutionGateInput,
        blockers: &mut BTreeSet<HardBlocker>,
    ) -> ExecutionGateResult<Option<(Artifact, AccountSnapshot)>> {
        // 缺账户直接累积 MissingAccount 并返回 None；存在时验证 NormalizedEvidence payload
        // 和 lifecycle，保留 artifact 与 typed snapshot 的配对供后续 provenance 使用。
        let Some(reference) = &input.account_snapshot else {
            blockers.insert(HardBlocker::MissingAccount);
            return Ok(None);
        };
        let artifact = self.load_expected(reference, ArtifactKind::NormalizedEvidence)?;
        let payload: AccountSnapshot = self.read_payload(&artifact)?;
        payload.validate()?;
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        Ok(Some((artifact, payload)))
    }

    fn load_quotes(
        &self,
        input: &ExecutionGateInput,
        blockers: &mut BTreeSet<HardBlocker>,
        invalid_quote: bool,
    ) -> ExecutionGateResult<Option<(Artifact, QuoteSnapshot)>> {
        // quote_validation_error 与真正缺引用分开映射，便于 NoOrder 区分“供应商回了坏报价”
        // 和“没有报价”；payload 合法不等于新鲜，freshness 在 derive_snapshot_blockers 检查。
        let Some(reference) = &input.quote_snapshot else {
            blockers.insert(if invalid_quote {
                HardBlocker::InvalidQuote
            } else {
                HardBlocker::MissingQuote
            });
            return Ok(None);
        };
        let artifact = self.load_expected(reference, ArtifactKind::NormalizedEvidence)?;
        let payload: QuoteSnapshot = self.read_payload(&artifact)?;
        payload.validate()?;
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        Ok(Some((artifact, payload)))
    }

    fn load_clock(
        &self,
        input: &ExecutionGateInput,
        blockers: &mut BTreeSet<HardBlocker>,
    ) -> ExecutionGateResult<Option<(Artifact, MarketClockSnapshot)>> {
        // 时钟缺失按 MarketClosed 处理；有效 payload 仍需在当前时间窗口、broker session 和
        // tradable 状态上复核。
        let Some(reference) = &input.market_clock_snapshot else {
            blockers.insert(HardBlocker::MarketClosed);
            return Ok(None);
        };
        let artifact = self.load_expected(reference, ArtifactKind::NormalizedEvidence)?;
        let payload: MarketClockSnapshot = self.read_payload(&artifact)?;
        payload.validate()?;
        if artifact.lifecycle != ArtifactLifecycle::Canonical {
            blockers.insert(HardBlocker::InvalidProvenance);
        }
        Ok(Some((artifact, payload)))
    }

    fn derive_snapshot_blockers(
        &self,
        account: Option<&AccountSnapshot>,
        quotes: Option<&QuoteSnapshot>,
        clock: Option<&MarketClockSnapshot>,
        now: DateTime<Utc>,
        blockers: &mut BTreeSet<HardBlocker>,
    ) {
        // 把账户、quotes、clock 的年龄、外部持仓、未托管订单、session 不一致和三源时间偏差
        // 累积为独立 blocker；任何一个来源过期都不会被其他来源的较新时间掩盖。
        if let Some(account) = account {
            if outside_freshness_window(
                account.observed_at,
                now,
                self.execution_policy().max_account_age_secs,
                self.execution_policy().max_future_skew_secs,
            ) {
                blockers.insert(HardBlocker::StaleAccount);
            }
            if !account.external_positions.is_empty() {
                blockers.insert(HardBlocker::ExternalPosition);
            }
            if !account.open_order_ids.is_empty() {
                blockers.insert(HardBlocker::UnmanagedOpenOrder);
            }
        }
        if let Some(quotes) = quotes {
            if outside_freshness_window(
                quotes.observed_at,
                now,
                self.execution_policy().max_quote_age_secs,
                self.execution_policy().max_future_skew_secs,
            ) {
                blockers.insert(HardBlocker::StaleQuote);
            }
        }
        if let Some(clock) = clock {
            if !clock.tradable()
                || outside_freshness_window(
                    clock.observed_at,
                    now,
                    self.execution_policy().max_clock_age_secs,
                    self.execution_policy().max_future_skew_secs,
                )
            {
                blockers.insert(HardBlocker::MarketClosed);
            }
        }
        if let (Some(account), Some(quotes)) = (account, quotes) {
            if account.broker_session != quotes.broker_session {
                blockers.insert(HardBlocker::StaleQuote);
            }
        }
        if let (Some(account), Some(clock)) = (account, clock) {
            if account.broker_session != clock.broker_session {
                blockers.insert(HardBlocker::MarketClosed);
            }
        }
        if let (Some(account), Some(quotes), Some(clock)) = (account, quotes, clock) {
            if snapshot_skewed(
                [account.observed_at, quotes.observed_at, clock.observed_at],
                self.execution_policy().max_snapshot_skew_secs,
            ) {
                blockers.insert(HardBlocker::InvalidProvenance);
            }
        }
    }

    fn allocation_blockers(&self, error: AllocationError, blockers: &mut BTreeSet<HardBlocker>) {
        // 将分配器的细粒度错误映射成稳定领域 blocker；错误仍保留在 gate 结果的原因集合，
        // 不通过“吞掉错误”制造可执行 plan。
        match error {
            AllocationError::DecisionRejected => {
                blockers.insert(HardBlocker::NoExecutableOrder);
            }
            AllocationError::SessionMismatch | AllocationError::Domain(_) => {
                blockers.insert(HardBlocker::InvalidProvenance);
            }
            AllocationError::MarketClosed => {
                blockers.insert(HardBlocker::MarketClosed);
            }
            AllocationError::Execution(error) => match error {
                ExecutionError::ForbiddenAsset(_) | ExecutionError::InvalidWeight(_) => {
                    blockers.insert(HardBlocker::UnsupportedUniverse);
                }
                ExecutionError::GrossExposureExceeded(_) => {
                    blockers.insert(HardBlocker::FactorLimit);
                }
                ExecutionError::MissingQuote(_) => {
                    blockers.insert(HardBlocker::MissingQuote);
                }
                ExecutionError::InvalidQuote(_) => {
                    blockers.insert(HardBlocker::InvalidQuote);
                }
                ExecutionError::StaleQuote(_) => {
                    blockers.insert(HardBlocker::StaleQuote);
                }
                ExecutionError::DailyTurnoverExceeded => {
                    blockers.insert(HardBlocker::TurnoverLimit);
                }
                ExecutionError::InvalidPolicy => {
                    blockers.insert(HardBlocker::InvalidProvenance);
                }
                ExecutionError::AccountBlocked
                | ExecutionError::InsufficientBuyingPower
                | ExecutionError::ShortPosition(_)
                | ExecutionError::NewNotionalExceeded
                | ExecutionError::NoExecutableOrder => {
                    blockers.insert(HardBlocker::NoExecutableOrder);
                }
            },
        }
    }

    fn load_expected(
        &self,
        reference: &ArtifactRef,
        expected: ArtifactKind,
    ) -> ExecutionGateResult<Artifact> {
        // Store 实际 Artifact kind 与引用声明必须同时匹配，保证后续 serde 类型和 lineage 一致。
        let artifact = self.store.artifact(&reference.artifact_id)?;
        if reference.kind != expected || artifact.kind != expected {
            return Err(ExecutionGateError::WrongArtifactKind {
                expected,
                actual: artifact.kind,
            });
        }
        Ok(artifact)
    }
}
