// 文件导读：这些小函数只处理 ArtifactRef 投影和三份执行快照的时间关系，供 Gate 主流程
// 复用；它们不访问 Store，也不改变 blocker 之外的执行状态。

fn artifact_ref(artifact: &Artifact) -> ArtifactRef {
    // 保留 Artifact 的 hash/kind 身份，不复制底层 blob。
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

fn outside_freshness_window(
    observed_at: DateTime<Utc>,
    now: DateTime<Utc>,
    max_age_secs: i64,
    max_future_skew_secs: i64,
) -> bool {
    // 同时限制最大滞后和允许的未来偏移，避免本机时钟或供应商时间异常带来隐式授权。
    let age = now.signed_duration_since(observed_at);
    age > Duration::seconds(max_age_secs) || age < -Duration::seconds(max_future_skew_secs)
}

fn snapshot_skewed(observed_at: [DateTime<Utc>; 3], max_skew_secs: i64) -> bool {
    // 用最旧与最新观察时间的差判断账户、报价、时钟是否来自同一可比窗口。
    let oldest = observed_at.into_iter().min().expect("three snapshots");
    let newest = observed_at.into_iter().max().expect("three snapshots");
    newest.signed_duration_since(oldest) > Duration::seconds(max_skew_secs)
}
