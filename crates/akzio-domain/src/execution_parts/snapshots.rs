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
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty()
        {
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
    pub schema_version: u32,
    pub broker_session: String,
    pub observed_at: DateTime<Utc>,
    pub quotes: BTreeMap<Asset, Quote>,
}

impl QuoteSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "quote_snapshot.identity",
            });
        }
        self.quotes.values().try_for_each(Quote::validate)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketClockSnapshot {
    pub schema_version: u32,
    pub broker_session: String,
    pub is_open: bool,
    pub observed_at: DateTime<Utc>,
}

impl MarketClockSnapshot {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "market_clock_snapshot.identity",
            });
        }
        Ok(())
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
    pub fn from_target(target: &TargetPortfolio) -> Result<Self, DomainError> {
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
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "factor_exposure",
            });
        }
        Ok(())
    }
}
