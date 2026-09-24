// 文件导读：定义账户、报价、时钟和因子暴露快照，以及把决策 cutoff 与完成日线
// 进行比较的纯时间边界；快照只描述已观测事实，不访问 Broker。
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
    // 校验账户身份、金额符号、持仓数量/价值和外部持仓/订单字符串。
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
    // 要求 bid 为正且 ask 严格高于 bid，避免零价或倒置价差进入 Gate。
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
    // 校验快照身份，并逐项复用 Quote 的价格约束。
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
    // 优先使用 provider 细分 session；旧 payload 没有 session 时由 is_open 映射 Regular/Closed。
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

    // Closed 之外的细分 session 都表示存在可交易时段。
    pub fn tradable(&self) -> bool {
        self.trading_session() != TradingSession::Closed
    }

    // 校验 schema 和 broker session 文本，不在此处重新推断时钟状态。
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
    // 判断带时间戳的事实是否不晚于决策 cutoff。
    pub fn contains(&self, timestamp: DateTime<Utc>) -> bool {
        timestamp <= self.decision_cutoff
    }

    // 日期粒度 vintage 只需不晚于 cutoff 的自然日。
    pub fn contains_vintage(&self, vintage: chrono::NaiveDate) -> bool {
        vintage <= self.decision_cutoff.date_naive()
    }

    // fixture 保守规则：只有严格早于 cutoff 日期的日线事件才算已完成。
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

    // 将资产资本权重转换为同日经济因子暴露；TQQQ/SOXL 按 3x 计算并用 checked_add 防溢出。
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
        // 仅用于解码旧计划：按资本权重聚合，不把杠杆资产转换为 3x 经济暴露。
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

    // 检查五个因子暴露不超过两倍三倍杠杆的允许上界。
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
