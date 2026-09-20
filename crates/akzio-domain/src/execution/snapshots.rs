#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub quantity_micros: i64,
    pub market_value: MoneyMicros,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub schema_version: u32,
    pub broker_session: String,
    pub observed_at: DateTime<Utc>,
    pub equity: MoneyMicros,
    pub buying_power: MoneyMicros,
    pub day_turnover: MoneyMicros,
    pub active: bool,
    pub trading_blocked: bool,
    pub positions: BTreeMap<Asset, Position>,
    pub external_positions: BTreeSet<String>,
    pub open_order_ids: BTreeSet<String>,
}

impl AccountSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "account_snapshot.identity",
            });
        }
        if self.equity.0 <= 0 || self.buying_power.0 < 0 || self.day_turnover.0 < 0 {
            return Err(DomainError::InvalidBudget {
                field: "account_snapshot.money",
            });
        }
        if self
            .positions
            .values()
            .any(|position| position.quantity_micros < 0 || position.market_value.0 < 0)
            || self
                .external_positions
                .iter()
                .any(|symbol| symbol.trim().is_empty())
            || self
                .open_order_ids
                .iter()
                .any(|order_id| order_id.trim().is_empty())
        {
            return Err(DomainError::InvalidBudget {
                field: "account_snapshot.positions",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quote {
    pub bid: MoneyMicros,
    pub ask: MoneyMicros,
    pub observed_at: DateTime<Utc>,
}

impl Quote {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.bid.0 <= 0 || self.ask.0 <= self.bid.0 {
            return Err(DomainError::InvalidBudget {
                field: "quote.price",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feed: Option<String>,
    pub schema_version: u32,
    pub broker_session: String,
    pub observed_at: DateTime<Utc>,
    pub quotes: BTreeMap<Asset, Quote>,
}

impl QuoteSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "quote_snapshot.identity",
            });
        }
        self.quotes.values().try_for_each(Quote::validate)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketClockSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<TradingSessionSnapshot>,
    pub schema_version: u32,
    pub broker_session: String,
    pub is_open: bool,
    pub observed_at: DateTime<Utc>,
}

impl MarketClockSnapshot {
    pub fn trading_session(&self) -> TradingSession {
        self.session.as_ref().map_or(
            if self.is_open {
                TradingSession::Regular
            } else {
                TradingSession::Closed
            },
            |session| session.kind,
        )
    }

    pub fn tradable(&self) -> bool {
        self.trading_session() != TradingSession::Closed
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "market_clock_snapshot.identity",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionClock {
    pub decision_cutoff: DateTime<Utc>,
}

impl DecisionClock {
    pub fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        timestamp <= self.decision_cutoff
    }

    pub fn contains_vintage(&self, vintage: chrono::NaiveDate) -> bool {
        vintage <= self.decision_cutoff.date_naive()
    }

    pub fn contains_completed_daily_bar(&self, event_time: DateTime<Utc>) -> bool {
        // Fixture-only conservative fallback. Production daily bars require
        // provider calendar close plus the acquisition availability delay.
        event_time.date_naive() < self.decision_cutoff.date_naive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorExposure {
    pub leveraged_equity_ppm: u32,
    pub nasdaq_ppm: u32,
    pub semiconductor_ppm: u32,
    pub tqqq_qqq_pair_ppm: u32,
    pub soxl_soxx_pair_ppm: u32,
}

impl FactorExposure {
    // TQQQ and SOXL target 3x of their daily benchmarks. This converts capital
    // weight into same-day economic exposure; multi-day compounding and path
    // dependence remain separate stress-test concerns.
    const DAILY_LEVERAGE_MULTIPLIER: u32 = 3;

    pub fn from_target(target: &TargetPortfolio) -> Result<Self, DomainError> {
        target.validate_universe()?;
        let weight = |asset| target.weights[&asset].0;
        let add = |left: u32, right: u32| {
            left.checked_add(right).ok_or(DomainError::InvalidBudget {
                field: "factor_exposure",
            })
        };
        let levered = |asset| {
            weight(asset)
                .checked_mul(Self::DAILY_LEVERAGE_MULTIPLIER)
                .ok_or(DomainError::InvalidBudget {
                    field: "factor_exposure",
                })
        };
        let tqqq_economic_ppm = levered(Asset::Tqqq)?;
        let soxl_economic_ppm = levered(Asset::Soxl)?;
        let leveraged_equity_ppm = add(tqqq_economic_ppm, soxl_economic_ppm)?;
        let nasdaq_ppm = add(tqqq_economic_ppm, weight(Asset::Qqq))?;
        let semiconductor_ppm = add(soxl_economic_ppm, weight(Asset::Soxx))?;
        Ok(Self {
            leveraged_equity_ppm,
            nasdaq_ppm,
            semiconductor_ppm,
            tqqq_qqq_pair_ppm: nasdaq_ppm,
            soxl_soxx_pair_ppm: semiconductor_ppm,
        })
    }

    /// Compatibility decoder for v2 plans persisted before leveraged ETF
    /// weights were converted to same-day economic exposure. New allocation
    /// and execution code must use `from_target`.
    pub(crate) fn from_legacy_capital_target(
        target: &TargetPortfolio,
    ) -> Result<Self, DomainError> {
        target.validate_universe()?;
        let weight = |asset| target.weights[&asset].0;
        let add = |left: u32, right: u32| {
            left.checked_add(right).ok_or(DomainError::InvalidBudget {
                field: "factor_exposure",
            })
        };
        let nasdaq_ppm = add(weight(Asset::Tqqq), weight(Asset::Qqq))?;
        let semiconductor_ppm = add(weight(Asset::Soxl), weight(Asset::Soxx))?;
        Ok(Self {
            leveraged_equity_ppm: add(weight(Asset::Tqqq), weight(Asset::Soxl))?,
            nasdaq_ppm,
            semiconductor_ppm,
            tqqq_qqq_pair_ppm: nasdaq_ppm,
            soxl_soxx_pair_ppm: semiconductor_ppm,
        })
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.leveraged_equity_ppm,
            self.nasdaq_ppm,
            self.semiconductor_ppm,
            self.tqqq_qqq_pair_ppm,
            self.soxl_soxx_pair_ppm,
        ]
        .into_iter()
        .any(|value| value > 2 * Self::DAILY_LEVERAGE_MULTIPLIER * 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "factor_exposure",
            });
        }
        Ok(())
    }
}
