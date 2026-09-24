// 本文件把 Provider telemetry 解析为 Attempt 级累计预算。估算值只用于 fixture
// 或缺省输入，真实 Provider 缺少 token 总量时会留下 unknown 状态，禁止未经核算的
// retry；cached/reasoning 只是分项成本信息，不能突破总 input/output 上限。
const TOKENS_PER_MILLION: u128 = 1_000_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ResolvedModelUsage {
    input_tokens: u64,
    cached_input_tokens: Option<u64>,
    output_tokens: u64,
    reasoning_tokens: Option<u64>,
}

fn budget_policy_hash(
    policy: &ModelBudgetPolicy,
) -> ResearchResult<akzio_domain::ContentHash> {
    Ok(akzio_domain::content_hash_json(&serde_json::to_value(
        policy,
    )?)?)
}

fn validate_budget_policy(policy: &ModelBudgetPolicy) -> ResearchResult<()> {
    // 只要声明价格或硬成本上限，就必须同时冻结非空路由和 pricing snapshot，
    // 这样恢复时可以比较同一政策哈希，而不是用今天的价格重算历史消耗。
    if (policy.pricing.is_some() || policy.max_cost_micros.is_some())
        && policy
            .route_identity
            .as_deref()
            .is_none_or(|identity| identity.trim().is_empty())
    {
        return Err(ResearchError::InvalidPricingRoute);
    }
    let Some(pricing) = &policy.pricing else {
        if policy.max_cost_micros.is_some() {
            return Err(ResearchError::PricingUnavailable);
        }
        return Ok(());
    };
    if pricing.identity.trim().is_empty() || pricing.version.trim().is_empty() {
        return Err(ResearchError::InvalidPricingSnapshot);
    }
    Ok(())
}

fn resolve_model_usage(
    estimated_input: u32,
    estimated_output: u32,
    telemetry: Option<&AgentTurnTelemetry>,
) -> ResolvedModelUsage {
    // telemetry 优先；只有没有 telemetry 的离线 fixture 才回退到 JSON 估算。
    // 该回退不会伪造真实 Provider 的未知用量，因为真实适配器会先拒绝缺失总量。
    let usage = telemetry.map_or_else(ModelUsage::default, |telemetry| ModelUsage {
        input_tokens: telemetry.input_tokens,
        cached_input_tokens: telemetry.cached_input_tokens,
        output_tokens: telemetry.output_tokens,
        reasoning_tokens: telemetry.reasoning_tokens,
    });
    ResolvedModelUsage {
        input_tokens: usage
            .input_tokens
            .unwrap_or_else(|| u64::from(estimated_input)),
        cached_input_tokens: usage.cached_input_tokens,
        output_tokens: usage
            .output_tokens
            .unwrap_or_else(|| u64::from(estimated_output)),
        reasoning_tokens: usage.reasoning_tokens,
    }
}

fn category_cost_micros(tokens: u64, rate: u64) -> ResearchResult<u64> {
    let numerator = u128::from(tokens)
        .checked_mul(u128::from(rate))
        .ok_or(ResearchError::CostOverflow)?;
    let rounded = numerator
        .checked_add(TOKENS_PER_MILLION - 1)
        .ok_or(ResearchError::CostOverflow)?
        / TOKENS_PER_MILLION;
    u64::try_from(rounded).map_err(|_| ResearchError::CostOverflow)
}

fn weighted_cost_micros(categories: &[(u64, u64)]) -> ResearchResult<u64> {
    let numerator = categories.iter().try_fold(0_u128, |total, (tokens, rate)| {
        let category = u128::from(*tokens)
            .checked_mul(u128::from(*rate))
            .ok_or(ResearchError::CostOverflow)?;
        total
            .checked_add(category)
            .ok_or(ResearchError::CostOverflow)
    })?;
    let rounded = numerator
        .checked_add(TOKENS_PER_MILLION - 1)
        .ok_or(ResearchError::CostOverflow)?
        / TOKENS_PER_MILLION;
    u64::try_from(rounded).map_err(|_| ResearchError::CostOverflow)
}

fn usage_cost_micros(
    usage: ResolvedModelUsage,
    pricing: &ModelPricingSnapshot,
) -> ResearchResult<u64> {
    // cached input 与 reasoning output 只有在不超过各自总量时才可拆分计价；
    // 发现 Provider 自相矛盾即返回错误，不以较小值“修正”外部事实。
    let (uncached_input, cached_input, input_rate, cached_rate) = match usage.cached_input_tokens {
        Some(cached) if cached <= usage.input_tokens => (
            usage.input_tokens - cached,
            cached,
            pricing.input_micros_per_million_tokens,
            pricing.cached_input_micros_per_million_tokens,
        ),
        Some(_) => return Err(ResearchError::InvalidProviderUsage),
        None => (
            usage.input_tokens,
            0,
            pricing
                .input_micros_per_million_tokens
                .max(pricing.cached_input_micros_per_million_tokens),
            pricing.cached_input_micros_per_million_tokens,
        ),
    };
    let (regular_output, reasoning_output, output_rate, reasoning_rate) =
        match usage.reasoning_tokens {
        Some(reasoning) if reasoning <= usage.output_tokens => (
            usage.output_tokens - reasoning,
            reasoning,
            pricing.output_micros_per_million_tokens,
            pricing.reasoning_micros_per_million_tokens,
        ),
        Some(_) => return Err(ResearchError::InvalidProviderUsage),
        None => (
            usage.output_tokens,
            0,
            pricing
                .output_micros_per_million_tokens
                .max(pricing.reasoning_micros_per_million_tokens),
            pricing.reasoning_micros_per_million_tokens,
        ),
    };
    weighted_cost_micros(&[
        (uncached_input, input_rate),
        (cached_input, cached_rate),
        (regular_output, output_rate),
        (reasoning_output, reasoning_rate),
    ])
}

fn conservative_input_cost_micros(
    input_tokens: u32,
    pricing: &ModelPricingSnapshot,
) -> ResearchResult<u64> {
    category_cost_micros(
        u64::from(input_tokens),
        pricing
            .input_micros_per_million_tokens
            .max(pricing.cached_input_micros_per_million_tokens),
    )
}

fn affordable_output_tokens(
    remaining_cost_micros: u64,
    pricing: &ModelPricingSnapshot,
) -> u64 {
    // 输出 cap 是在预扣本次输入成本后计算的，且使用更保守的非零费率；零费率
    // 表示成本上限对输出没有额外约束，但整个 Attempt 的 token cap 仍由调用方执行。
    let rate = pricing
        .output_micros_per_million_tokens
        .max(pricing.reasoning_micros_per_million_tokens);
    if rate == 0 {
        return u64::MAX;
    }
    u64::try_from(
        u128::from(remaining_cost_micros)
            .saturating_mul(TOKENS_PER_MILLION)
            / u128::from(rate),
    )
    .unwrap_or(u64::MAX)
}
