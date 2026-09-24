// 文件导读：这是被 `execution_gate.rs` include 的私有实现片段，负责从输入 ArtifactRef
// 读取账户、报价、时钟快照并将可预期缺失映射成 HardBlocker。payload 错误仍向 evaluate
// 传播；有效快照在此仅借用参与 freshness/session/skew 检查，不产生订单或 Store 写入。
impl ExecutionRuntime {
    fn load_account(
        &self,
        input: &ExecutionGateInput,
        blockers: &mut BTreeSet<HardBlocker>,
    ) -> ExecutionGateResult<Option<(Artifact, AccountSnapshot)>> {
        // 缺账户直接累积 MissingAccount 并返回 None；存在时验证 NormalizedEvidence payload
        // 和 lifecycle，保留 artifact 与 typed snapshot 的配对供后续 provenance 使用。
        // `let-else` 把 Option 的 None 分支显式提前返回；Some 分支借用引用继续查 Store，
        // 返回的 Option 让主流程知道该快照是否存在，而不是拿默认账户冒充真实账户。
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
        // 无 quote ref 时根据 adapter 是否报告了解析错误区分 MissingQuote 和 InvalidQuote；
        // Artifact 或 payload 损坏不是 blocker，而是 Err，会终止整次 Gate 求值。
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
        // 缺时钟无法证明可交易，因而 fail closed 为 MarketClosed；实际 payload 仍需校验。
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
        // 每种快照各自与 `now` 比较；账户额外拒绝外部持仓和未托管挂单，因为执行计划
        // 只管理系统认可的仓位与订单。
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
        // `if let` 对 Option 元组做引用模式匹配，不移动借来的 payload；只有两边都存在
        // 时才可比较 session，缺项的 blocker 已由 load_* 负责报告。
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
        // 将分配器的细粒度错误归并成稳定领域 blocker；这里不保留原错误的完整文本，
        // 但也不会在分配失败后制造可执行 plan。
        // match 按值消费这次 allocation error，并穷尽映射各类错误；Domain 结构错误、
        // 资产/金额/报价错误会落入对应 blocker，不把错误误认为成功计划。
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
        // reference 是调用方借来的身份；Store 返回拥有的 Artifact，便于后续同时读取 blob。
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
