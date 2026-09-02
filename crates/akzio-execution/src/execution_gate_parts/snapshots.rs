impl V2ExecutionRuntime {
    fn load_account(
        &self,
        input: &ExecutionGateInput,
        blockers: &mut BTreeSet<HardBlocker>,
    ) -> ExecutionGateResult<Option<(Artifact, AccountSnapshot)>> {
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
    ) -> ExecutionGateResult<Option<(Artifact, QuoteSnapshot)>> {
        let Some(reference) = &input.quote_snapshot else {
            blockers.insert(HardBlocker::MissingQuote);
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
            if !clock.is_open
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
                ExecutionError::MissingQuote(_) | ExecutionError::InvalidQuote(_) => {
                    blockers.insert(HardBlocker::MissingQuote);
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
