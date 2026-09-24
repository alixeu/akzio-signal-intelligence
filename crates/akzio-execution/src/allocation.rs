//! Model-free conversion from a typed decision and broker snapshots to orders.

// 文件导读：这里是 Decision 到 ExecutionPlan 的纯 Rust 转换层。输入必须是已接受的
// DecisionContext，并且账户、报价、市场时钟属于同一 broker session；随后按目标权重
// 计算资产差额、验证报价和购买力、缩放买单、统计换手，最后把快照 Artifact 引用与
// 重新计算的 plan hash 一起返回。它不写 Store，也不调用 Broker。

use akzio_domain::{
    AccountSnapshot, ArtifactRef, Asset, ContentHash, DecisionContext, DomainError, ExecutionPlan,
    FactorExposure, MarketClockSnapshot, MoneyMicros, OrderIntent, OrderSide, QuoteSnapshot,
    TargetPortfolio, WeightPpm, DOMAIN_SCHEMA_VERSION,
};
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::{
    protected_limit_price, scaled_weight, validate_quote, ExecutionError, ExecutionPolicy,
    WEIGHT_SCALE,
};

#[derive(Debug, Error)]
pub enum AllocationError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error("execution allocation requires an accepted decision context")]
    DecisionRejected,
    #[error("execution snapshots do not describe the same broker session")]
    SessionMismatch,
    #[error("market is closed")]
    MarketClosed,
}

pub type AllocationResult<T> = std::result::Result<T, AllocationError>;

#[derive(Debug, Clone)]
pub struct AllocationInput {
    pub decision_context_ref: ArtifactRef,
    pub decision_context: DecisionContext,
    pub account_snapshot_ref: ArtifactRef,
    pub account: AccountSnapshot,
    pub quote_snapshot_ref: ArtifactRef,
    pub quotes: QuoteSnapshot,
    pub market_clock_snapshot_ref: ArtifactRef,
    pub clock: MarketClockSnapshot,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct AllocationRuntime {
    policy: ExecutionPolicy,
}

impl AllocationRuntime {
    // `policy` 按值传入：校验通过后由 runtime 持有，调用者不再能通过原变量改写这份策略。
    pub fn new(policy: ExecutionPolicy) -> AllocationResult<Self> {
        // 分配器创建时就冻结并校验执行策略，后续每次分配不会临时接受模型提供的限制。
        policy.validate()?;
        Ok(Self { policy })
    }

    // `&self` 只借用 runtime；返回的引用跟随该借用有效，调用方不能借此修改策略。
    pub fn policy(&self) -> &ExecutionPolicy {
        // 暴露只读策略引用，供 Gate 读取上限而不取得修改执行参数的所有权。
        &self.policy
    }

    // 输入以共享借用传递，`allocate_with_limit` 完成全部检查后返回新拥有的 plan。
    pub fn allocate(&self, input: &AllocationInput) -> AllocationResult<ExecutionPlan> {
        // 常规入口使用策略内的单次最大名义金额；需要审批上限时由 Gate 调用带 limit 的入口。
        self.allocate_with_limit(input, self.policy.max_new_notional)
    }

    pub fn allocate_with_limit(
        &self,
        input: &AllocationInput,
        maximum_total_notional: MoneyMicros,
    ) -> AllocationResult<ExecutionPlan> {
        // 这里先做领域校验、Decision 接受状态和 session 对齐，再进入订单计算；因此
        // 关闭市场或混合快照不会被后面的金额计算掩盖成可执行计划。
        // 下方每个 `?` 把领域错误经 `From` 转为 AllocationError，并在此函数返回；
        // 这些校验只读取借来的 input，不消费或修改原始快照。
        input.decision_context.validate()?;
        input.account.validate()?;
        input.quotes.validate()?;
        input.clock.validate()?;
        if !input.decision_context.accepted() {
            return Err(AllocationError::DecisionRejected);
        }
        if input.account.broker_session != input.quotes.broker_session
            || input.account.broker_session != input.clock.broker_session
        {
            return Err(AllocationError::SessionMismatch);
        }
        if !input.clock.tradable() {
            return Err(AllocationError::MarketClosed);
        }
        Ok(build_execution_plan(
            &self.policy,
            input,
            maximum_total_notional,
        )?)
    }
}

fn build_execution_plan(
    policy: &ExecutionPolicy,
    input: &AllocationInput,
    maximum_total_notional: MoneyMicros,
) -> std::result::Result<ExecutionPlan, ExecutionError> {
    // 逐资产把 target weight 映射为当前市值差额：先验证资产全集和 gross exposure，
    // 再只为非零差额生成限价单。订单生成完成后才统一处理买入上限、购买力、换手和
    // achieved target，避免把“计划目标”误写成“已成交结果”。
    // 参数全是共享借用；`target.weights[&asset]` 复制 ppm 数值，`quotes` 中取出的 Quote
    // 也是 Copy 值，因此不会从 input 的集合中移走任何快照数据。
    let target = &input.decision_context.target;
    let account = &input.account;
    let quotes = &input.quotes;
    let now = input.now;
    policy.validate()?;
    if !account.active || account.trading_blocked || account.equity.0 <= 0 {
        return Err(ExecutionError::AccountBlocked);
    }

    target
        .validate_universe()
        .map_err(|_| ExecutionError::InvalidPolicy)?;
    // `iter()` 逐项借出有序 map 项，`try_fold` 立即消费迭代器；闭包遇到非法资产、
    // 超范围权重或整数溢出就以 Err 停止，而不是只构造一个延迟执行的处理链。
    let gross = target
        .weights
        .iter()
        .try_fold(0_u32, |sum, (asset, weight)| {
            if !policy.assets.contains(asset) {
                return Err(ExecutionError::ForbiddenAsset(*asset));
            }
            if weight.0 > WeightPpm::SCALE {
                return Err(ExecutionError::InvalidWeight(*asset));
            }
            sum.checked_add(weight.0)
                .ok_or(ExecutionError::GrossExposureExceeded(
                    policy.max_gross_weight.0,
                ))
        })?;
    if gross > policy.max_gross_weight.0 {
        return Err(ExecutionError::GrossExposureExceeded(
            policy.max_gross_weight.0,
        ));
    }

    let mut orders = Vec::new();
    for asset in Asset::EXECUTABLE {
        // `map_or` 在无持仓时用零作为当前市值；delta 为零时不生成空订单。
        let target_value = scaled_weight(account.equity, target.weights[&asset]);
        let current_value = account
            .positions
            .get(&asset)
            .map_or(MoneyMicros::ZERO, |position| position.market_value);
        let delta = i128::from(target_value.0) - i128::from(current_value.0);
        if delta == 0 {
            continue;
        }
        if target_value.0 < 0 || current_value.0 < 0 {
            return Err(ExecutionError::ShortPosition(asset));
        }
        let quote = *quotes
            .quotes
            .get(&asset)
            .ok_or(ExecutionError::MissingQuote(asset))?;
        // `get` 先产生对快照内 Quote 的借用，解引用复制 Quote 后传给验证器；
        // 失效报价直接传播为错误，本轮不会留下可返回的部分 plan。
        validate_quote(
            policy.max_quote_age_secs,
            policy.max_future_skew_secs,
            policy.max_spread_bps,
            asset,
            quote,
            now,
        )?;
        let side = if delta > 0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let notional = MoneyMicros(
            i64::try_from(delta.unsigned_abs()).map_err(|_| ExecutionError::NewNotionalExceeded)?,
        );
        orders.push(OrderIntent {
            extended_hours: input.clock.trading_session().extended_hours(),
            asset,
            side,
            notional,
            limit_price: protected_limit_price(quote, side, policy.limit_protection_bps),
        });
    }

    let buy_limit = MoneyMicros(maximum_total_notional.0.min(account.buying_power.0));
    // 缩放只发生在本地可变 Vec 上；任何后续 Gate 失败都会丢弃这份尚未提交的 plan。
    scale_buy_orders_to_limit(&mut orders, buy_limit)?;
    if orders.is_empty() {
        return Err(ExecutionError::NoExecutableOrder);
    }
    let notionals = order_notionals(&orders)?;
    if notionals.risk_increasing > i128::from(maximum_total_notional.0) {
        return Err(ExecutionError::NewNotionalExceeded);
    }
    // Sell proceeds are not assumed available before the broker confirms fills.
    // `net_cash_required` is retained as a separate execution metric, while
    // buying power conservatively gates the full gross buy side.
    if notionals.gross_buy > i128::from(account.buying_power.0) {
        return Err(ExecutionError::InsufficientBuyingPower);
    }
    debug_assert!(notionals.net_cash_required <= notionals.gross_buy);

    let total_turnover = i128::from(account.day_turnover.0).saturating_add(notionals.turnover);
    let turnover_ppm = total_turnover
        .saturating_mul(WEIGHT_SCALE)
        .checked_div(i128::from(account.equity.0))
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(ExecutionError::DailyTurnoverExceeded)?;
    if turnover_ppm > policy.max_daily_turnover.0 {
        return Err(ExecutionError::DailyTurnoverExceeded);
    }

    orders.sort_by_key(|order| (matches!(order.side, OrderSide::Buy), order.asset));
    let achieved_target = target_after_orders(account, &orders)?;
    let gross = achieved_target
        .weights
        .values()
        .try_fold(0_u32, |total, weight| total.checked_add(weight.0))
        .ok_or(ExecutionError::InvalidPolicy)?;
    let factor_exposure =
        FactorExposure::from_target(&achieved_target).map_err(|_| ExecutionError::InvalidPolicy)?;
    let mut plan = ExecutionPlan {
        schema_version: DOMAIN_SCHEMA_VERSION,
        decision_context: input.decision_context_ref.clone(),
        account_snapshot: input.account_snapshot_ref.clone(),
        quote_snapshot: input.quote_snapshot_ref.clone(),
        market_clock_snapshot: input.market_clock_snapshot_ref.clone(),
        policy_hash: policy.policy_hash()?,
        maximum_total_notional,
        target: achieved_target,
        orders,
        gross_exposure_ppm: gross,
        net_exposure_ppm: i64::from(gross),
        factor_exposure,
        turnover_ppm,
        broker_session: account.broker_session.clone(),
        created_at: now,
        plan_hash: ContentHash::of_bytes(b"pending execution plan hash"),
    };
    // plan 先以占位 hash 构造，再根据其最终字段刷新哈希；失败时只返回错误，不触碰 Store。
    plan.refresh_hash()
        .map_err(|_| ExecutionError::InvalidPolicy)?;
    Ok(plan)
}

fn scale_buy_orders_to_limit(
    orders: &mut Vec<OrderIntent>,
    maximum_buy_notional: MoneyMicros,
) -> std::result::Result<(), ExecutionError> {
    // 只按比例压缩买单，卖单保持原值；整数余数按固定 Asset 顺序分配，保证相同输入
    // 仍得到相同的订单顺序和 plan hash。
    if maximum_buy_notional.0 < 0 {
        return Err(ExecutionError::NewNotionalExceeded);
    }
    let total = order_notionals(orders)?.gross_buy;
    let limit = i128::from(maximum_buy_notional.0);
    if total <= limit {
        return Ok(());
    }

    let mut remainder = limit;
    // `iter_mut` 暂时独占借用每个买单，只在循环体内更新名义金额；`filter` 的闭包
    // 仅检查 side，不捕获 orders 的第二个可变借用，因此不会与迭代器的借用冲突。
    for order in orders
        .iter_mut()
        .filter(|order| order.side == OrderSide::Buy)
    {
        let scaled = i128::from(order.notional.0)
            .checked_mul(limit)
            .and_then(|value| value.checked_div(total))
            .ok_or(ExecutionError::NewNotionalExceeded)?;
        order.notional =
            MoneyMicros(i64::try_from(scaled).map_err(|_| ExecutionError::NewNotionalExceeded)?);
        remainder = remainder
            .checked_sub(scaled)
            .ok_or(ExecutionError::NewNotionalExceeded)?;
    }

    // 前一轮对各买单分别做整数除法，余数合计可能超过 1 个微美元；按固定资产序
    // 把余数逐资产各加 1，
    // `break` 只结束这层资产循环，确保等价输入得到稳定的 plan hash。
    for asset in Asset::EXECUTABLE {
        if remainder == 0 {
            break;
        }
        if let Some(order) = orders
            .iter_mut()
            .find(|order| order.asset == asset && order.side == OrderSide::Buy)
        {
            order.notional = MoneyMicros(
                order
                    .notional
                    .0
                    .checked_add(1)
                    .ok_or(ExecutionError::NewNotionalExceeded)?,
            );
            remainder -= 1;
        }
    }
    // `retain` 原地移除缩放为零的订单；它不改变卖单金额，只改变本地 Vec 的成员。
    orders.retain(|order| order.notional.0 > 0);
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderNotionals {
    gross_buy: i128,
    gross_sell: i128,
    net_cash_required: i128,
    turnover: i128,
    risk_increasing: i128,
}

fn order_notionals(orders: &[OrderIntent]) -> std::result::Result<OrderNotionals, ExecutionError> {
    // 用迭代器输入逐单累加买、卖、换手和净现金需求，并在每次 checked_add 时阻断溢出。
    // risk_increasing 只代表买入侧，不把尚未成交的卖出所得提前算进购买力。
    let mut gross_buy = 0_i128;
    let mut gross_sell = 0_i128;
    // `&[OrderIntent]` 是只读切片，循环内 `order` 是元素引用；仅把 Copy 的金额转成
    // i128 累加，所以调用方仍拥有并可继续使用原订单集合。
    for order in orders {
        let notional = i128::from(order.notional.0);
        match order.side {
            OrderSide::Buy => {
                gross_buy = gross_buy
                    .checked_add(notional)
                    .ok_or(ExecutionError::NewNotionalExceeded)?;
            }
            OrderSide::Sell => {
                gross_sell = gross_sell
                    .checked_add(notional)
                    .ok_or(ExecutionError::NewNotionalExceeded)?;
            }
        }
    }
    let turnover = gross_buy
        .checked_add(gross_sell)
        .ok_or(ExecutionError::NewNotionalExceeded)?;
    Ok(OrderNotionals {
        gross_buy,
        gross_sell,
        net_cash_required: gross_buy.saturating_sub(gross_sell).max(0),
        turnover,
        risk_increasing: gross_buy,
    })
}

fn target_after_orders(
    account: &AccountSnapshot,
    orders: &[OrderIntent],
) -> std::result::Result<TargetPortfolio, ExecutionError> {
    // 按订单的名义金额推导“订单执行后假设目标”，用于风控和审计，不是 Broker 回报的
    // 实际持仓；负市值和超过 ppm 范围都在这里拒绝。
    // 结果从零组合开始并按四个允许资产重建；输入账户和订单均只借用，不会被“模拟成交”修改。
    let mut achieved = TargetPortfolio::zeroed();
    for asset in Asset::EXECUTABLE {
        let mut market_value = i128::from(
            account
                .positions
                .get(&asset)
                .map_or(MoneyMicros::ZERO, |position| position.market_value)
                .0,
        );
        if let Some(order) = orders.iter().find(|order| order.asset == asset) {
            market_value = match order.side {
                OrderSide::Buy => market_value.checked_add(i128::from(order.notional.0)),
                OrderSide::Sell => market_value.checked_sub(i128::from(order.notional.0)),
            }
            .ok_or(ExecutionError::InvalidWeight(asset))?;
        }
        if market_value < 0 {
            return Err(ExecutionError::ShortPosition(asset));
        }
        let weight = market_value
            .checked_mul(WEIGHT_SCALE)
            .and_then(|value| value.checked_div(i128::from(account.equity.0)))
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(ExecutionError::InvalidWeight(asset))?;
        if weight > WeightPpm::SCALE {
            return Err(ExecutionError::InvalidWeight(asset));
        }
        achieved.weights.insert(asset, WeightPpm(weight));
    }
    achieved
        .validate_universe()
        .map_err(|_| ExecutionError::InvalidPolicy)?;
    Ok(achieved)
}
