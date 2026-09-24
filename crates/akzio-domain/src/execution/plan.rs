// 文件导读：定义订单意图、带 hash 的 ExecutionPlan 和可逐步构建的 ExecutionContext。
// 这里只验证引用、风险派生值和 plan closure，不直接提交 Paper 订单。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub extended_hours: bool,
    pub asset: Asset,
    pub side: OrderSide,
    pub notional: MoneyMicros,
    pub limit_price: MoneyMicros,
}

impl OrderIntent {
    // 订单名义金额和限价必须为正；资产/side 的业务白名单由上层 Gate 继续处理。
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
    /// Legacy field name for the absolute buy-side notional ceiling frozen
    /// into the plan hash. Risk-reducing sells do not consume this budget.
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
    // 按不含 plan_hash 的固定字段投影计算确定性计划哈希。
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

    // 重算并写入 plan_hash；调用方随后仍应执行 validate。
    pub fn refresh_hash(&mut self) -> Result<(), DomainError> {
        self.plan_hash = self.expected_hash()?;
        Ok(())
    }

    // 比较保存的 factor_exposure 是否符合当前的 3x same-day 模型。
    pub fn uses_current_factor_exposure_model(&self) -> Result<bool, DomainError> {
        Ok(self.factor_exposure == FactorExposure::from_target(&self.target)?)
    }

    // 校验身份/引用、目标 universe、订单唯一性/买入预算、派生暴露和最终 hash。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.broker_session.trim().is_empty() {
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
        // 只累计 Buy notional；Sell 是风险减少动作，不消耗 maximum_total_notional。
        let buy_notional = self
            .orders
            .iter()
            .filter(|order| order.side == OrderSide::Buy)
            .try_fold(0_i64, |total, order| total.checked_add(order.notional.0))
            .ok_or(DomainError::InvalidBudget {
                field: "execution_plan.buy_notional",
            })?;
        if buy_notional > self.maximum_total_notional.0 {
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
        // 从 target 重新求 gross，拒绝调用方伪造派生 exposure。
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
            || (self.factor_exposure != FactorExposure::from_target(&self.target)?
                && self.factor_exposure
                    != FactorExposure::from_legacy_capital_target(&self.target)?)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mandate_assessment: Option<MandateAssessment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pretrade_safety: Option<PreTradeSafetySnapshot>,
    /// Complete process assessment after mandate, portfolio and pre-trade
    /// feasibility have been evaluated. Absence means the execution gate
    /// could not honestly finalize every dimension.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_process_quality: Option<crate::ProcessQualityAssessment>,
    pub frozen: bool,
    pub created_at: DateTime<Utc>,
}

impl ExecutionContext {
    // 校验执行上下文身份、可选快照引用、派生 ppm、风险和最终 process quality。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.run_id.0.trim().is_empty() {
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
        if let Some(safety) = &self.pretrade_safety {
            safety.validate()?;
        }
        if let Some(quality) = &self.final_process_quality {
            quality.validate()?;
            if quality.measured_floor().is_none() {
                return Err(DomainError::InvalidBudget {
                    field: "execution_context.final_process_quality",
                });
            }
        }
        Ok(())
    }

    // 在普通校验之上要求账户/报价/时钟/计划、风险评估、pre-trade 和冻结状态的完整闭包。
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
            || self
                .mandate_assessment
                .as_ref()
                .is_none_or(|assessment| !assessment.permitted())
            || self
                .pretrade_safety
                .as_ref()
                .is_none_or(|safety| !safety.permits_execution)
            || self.frozen
        {
            return Err(DomainError::EmptyField {
                field: "execution_context.plan_closure",
            });
        }
        Ok(())
    }
}
