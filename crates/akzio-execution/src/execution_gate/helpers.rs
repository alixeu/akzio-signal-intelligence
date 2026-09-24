// 文件导读：这些小函数只处理 ArtifactRef 投影和三份执行快照的时间关系，供 Gate 主流程
// 复用；它们不访问 Store，也不改变 blocker 之外的执行状态。DateTime 按值传入仅用于
// 时间算术，artifact_ref 只克隆 CAS ID；三个 helper 都是同步纯计算，不创建 Future。

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
    // 固定数组确保正好有三份观察；into_iter 消费数组元素以取 min/max，expect 因数组
    // 长度已知为 3 不会遇到空迭代器，再以最旧/最新差检查可比窗口。
    let oldest = observed_at.into_iter().min().expect("three snapshots");
    let newest = observed_at.into_iter().max().expect("three snapshots");
    newest.signed_duration_since(oldest) > Duration::seconds(max_skew_secs)
}
