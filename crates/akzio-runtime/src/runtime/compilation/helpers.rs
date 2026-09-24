// 文件导读：Rust 根据 Claim 的 materiality/stance/冲突判断固定 Critic 节点在执行期
// 是否可跳过；daemon 的研究 handler 会调用它，不是模型自我扩图。
// true 只表示应进行结构化审查，不等于 Claim 已被支持或 Critic 已完成。
pub fn should_run_structured_critique(claims: &[ResearchClaim]) -> bool {
    // `&[ResearchClaim]` 借用整个 slice；第二个 any 只查看 index 后面的子切片，
    // 每对同 topic/horizon 的相反 stance 只需比较一次，不移动 Claim。
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
