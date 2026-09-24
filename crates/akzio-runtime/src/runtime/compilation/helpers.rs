// Critic 是否值得运行由 Rust 根据 Claim 的 materiality/stance/冲突决定；它是拓扑
// 编译选择，不是模型自我扩图。返回 true 只会保留审查节点，绝不等于 Claim 已被支持。
pub fn should_run_structured_critique(claims: &[ResearchClaim]) -> bool {
    claims.iter().any(|claim| {
        claim.materiality_ppm >= STRUCTURED_CRITIQUE_MATERIALITY_PPM
            || claim.stance != ClaimStance::Neutral
    }) || claims.iter().enumerate().any(|(index, claim)| {
        claims[index + 1..].iter().any(|other| {
            claim.topic == other.topic
                && claim.horizon == other.horizon
                && matches!(
                    (claim.stance, other.stance),
                    (ClaimStance::Bullish, ClaimStance::Bearish)
                        | (ClaimStance::Bearish, ClaimStance::Bullish)
                )
        })
    })
}
