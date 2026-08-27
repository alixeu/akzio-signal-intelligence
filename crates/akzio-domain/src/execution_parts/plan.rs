#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub asset: Asset,
    pub side: OrderSide,
    pub notional: MoneyMicros,
    pub limit_price: MoneyMicros,
}

impl OrderIntent {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.notional.0 <= 0 || self.limit_price.0 <= 0 {
            return Err(DomainError::InvalidBudget {
                field: "order_intent",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub schema_version: u32,
    pub decision_context: ArtifactRef,
    pub account_snapshot: ArtifactRef,
    pub quote_snapshot: ArtifactRef,
    pub market_clock_snapshot: ArtifactRef,
    pub policy_hash: ContentHash,
    /// Absolute order notional ceiling frozen into the plan hash.
    pub maximum_total_notional: MoneyMicros,
    pub target: TargetPortfolio,
    pub orders: Vec<OrderIntent>,
    pub gross_exposure_ppm: u32,
    pub net_exposure_ppm: i64,
    pub factor_exposure: FactorExposure,
    pub turnover_ppm: u32,
    pub broker_session: String,
    pub created_at: DateTime<Utc>,
    pub plan_hash: ContentHash,
}

#[derive(Serialize)]
struct ExecutionPlanHashPayload<'a> {
    schema_version: u32,
    decision_context: &'a ArtifactRef,
    account_snapshot: &'a ArtifactRef,
    quote_snapshot: &'a ArtifactRef,
    market_clock_snapshot: &'a ArtifactRef,
    policy_hash: &'a ContentHash,
    maximum_total_notional: MoneyMicros,
    target: &'a TargetPortfolio,
    orders: &'a [OrderIntent],
    gross_exposure_ppm: u32,
    net_exposure_ppm: i64,
    factor_exposure: &'a FactorExposure,
    turnover_ppm: u32,
    broker_session: &'a str,
    created_at: DateTime<Utc>,
}

impl ExecutionPlan {
    pub fn expected_hash(&self) -> Result<ContentHash, DomainError> {
        let payload = ExecutionPlanHashPayload {
            schema_version: self.schema_version,
            decision_context: &self.decision_context,
            account_snapshot: &self.account_snapshot,
            quote_snapshot: &self.quote_snapshot,
            market_clock_snapshot: &self.market_clock_snapshot,
            policy_hash: &self.policy_hash,
            maximum_total_notional: self.maximum_total_notional,
            target: &self.target,
            orders: &self.orders,
            gross_exposure_ppm: self.gross_exposure_ppm,
            net_exposure_ppm: self.net_exposure_ppm,
            factor_exposure: &self.factor_exposure,
            turnover_ppm: self.turnover_ppm,
            broker_session: &self.broker_session,
            created_at: self.created_at,
        };
        let value = serde_json::to_value(payload).map_err(|_| DomainError::InvalidContentHash)?;
        content_hash_json(&value).map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn refresh_hash(&mut self) -> Result<(), DomainError> {
        self.plan_hash = self.expected_hash()?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty()
        {
            return Err(DomainError::EmptyField {
                field: "execution_plan.identity",
            });
        }
        if self.maximum_total_notional.0 <= 0 {
            return Err(DomainError::InvalidBudget {
                field: "execution_plan.maximum_total_notional",
            });
        }
        if self.decision_context.kind != ArtifactKind::DecisionContext
            || self.account_snapshot.kind != ArtifactKind::NormalizedEvidence
            || self.quote_snapshot.kind != ArtifactKind::NormalizedEvidence
            || self.market_clock_snapshot.kind != ArtifactKind::NormalizedEvidence
        {
            return Err(DomainError::EmptyField {
                field: "execution_plan.references",
            });
        }
        self.target.validate_universe()?;
        self.factor_exposure.validate()?;
        if self.orders.is_empty()
            || self.gross_exposure_ppm > 1_000_000
            || self.net_exposure_ppm.unsigned_abs() > 1_000_000
            || self.turnover_ppm > 1_000_000
        {
            return Err(DomainError::InvalidBudget {
                field: "execution_plan.exposure",
            });
        }
        self.orders.iter().try_for_each(OrderIntent::validate)?;
        let total_notional = self
            .orders
            .iter()
            .try_fold(0_i64, |total, order| total.checked_add(order.notional.0))
            .ok_or(DomainError::InvalidBudget {
                field: "execution_plan.total_notional",
            })?;
        if total_notional > self.maximum_total_notional.0 {
            return Err(DomainError::InvalidBudget {
                field: "execution_plan.maximum_total_notional",
            });
        }
        if self
            .orders
            .iter()
            .map(|order| order.asset)
            .collect::<BTreeSet<_>>()
            .len()
            != self.orders.len()
        {
            return Err(DomainError::EmptyField {
                field: "execution_plan.orders",
            });
        }
        let gross = self
            .target
            .weights
            .values()
            .try_fold(0_u32, |sum, weight| {
                sum.checked_add(weight.0).ok_or(DomainError::InvalidBudget {
                    field: "execution_plan.gross_exposure_ppm",
                })
            })?;
        if self.gross_exposure_ppm != gross
            || self.net_exposure_ppm != i64::from(gross)
            || self.factor_exposure != FactorExposure::from_target(&self.target)?
        {
            return Err(DomainError::InvalidBudget {
                field: "execution_plan.derived_exposure",
            });
        }
        if self.plan_hash != self.expected_hash()? {
            return Err(DomainError::ExecutionPlanHashMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub schema_version: u32,
    pub run_id: RunId,
    pub decision_context: ArtifactRef,
    pub account_snapshot: Option<ArtifactRef>,
    pub quote_snapshot: Option<ArtifactRef>,
    pub market_clock_snapshot: Option<ArtifactRef>,
    pub execution_plan: Option<ArtifactRef>,
    pub factor_exposure: Option<FactorExposure>,
    pub turnover_ppm: Option<u32>,
    pub plan_hash: Option<ContentHash>,
    pub broker_session: Option<String>,
    pub frozen: bool,
    pub created_at: DateTime<Utc>,
}

impl ExecutionContext {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION || self.run_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "execution_context.identity",
            });
        }
        if self.decision_context.kind != ArtifactKind::DecisionContext
            || self
                .account_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || self
                .quote_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || self
                .market_clock_snapshot
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::NormalizedEvidence)
            || self
                .execution_plan
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::ExecutionPlan)
        {
            return Err(DomainError::EmptyField {
                field: "execution_context.references",
            });
        }
        if self
            .turnover_ppm
            .is_some_and(|turnover| turnover > 1_000_000)
            || self
                .broker_session
                .as_ref()
                .is_some_and(|session| session.trim().is_empty())
        {
            return Err(DomainError::InvalidBudget {
                field: "execution_context.derived",
            });
        }
        if let Some(exposure) = &self.factor_exposure {
            exposure.validate()?;
        }
        Ok(())
    }

    pub fn validate_complete_plan_closure(&self) -> Result<(), DomainError> {
        self.validate()?;
        if self.account_snapshot.is_none()
            || self.quote_snapshot.is_none()
            || self.market_clock_snapshot.is_none()
            || self.execution_plan.is_none()
            || self.factor_exposure.is_none()
            || self.turnover_ppm.is_none()
            || self.plan_hash.is_none()
            || self.broker_session.is_none()
            || self.frozen
        {
            return Err(DomainError::EmptyField {
                field: "execution_context.plan_closure",
            });
        }
        Ok(())
    }
}
