// 文件导读：Outcome helper 只提供 lease 的 RAII 释放和下一次轮询时间。轮询时间是后台
// worker 的调度提示，不是市场已闭市、Outcome 已成熟或 T+N 已完成的证据。
// Rust 机制：`Drop` 在正常返回、`?` 提前返回、panic 展开和 Future 取消时运行；结构体
// 拥有 Store/lease，避免借用生命周期结束后遗留未释放的 durable lease。

use super::*;

/// Also releases on worker error, task deadline cancellation, and unwinding.
pub(super) struct OutcomeLeaseGuard {
    pub store: Store,
    pub lease: akzio_store::DaemonLease,
}

impl Drop for OutcomeLeaseGuard {
    fn drop(&mut self) {
        // Drop 覆盖 `?`、Future 取消和 unwind；release 失败也不丢失 lease 的 expiry recovery
        // 边界，warn 只是诊断。
        if let Err(error) = self.store.release_daemon_lease(&self.lease, Utc::now()) {
            tracing::warn!(%error, "Outcome lease release failed; expiry remains the recovery bound");
        }
    }
}

// This is a polling deadline, never proof that a market session has closed.
pub(super) fn next_outcome_check_at(now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    // 固定轮询延迟只控制重试频率；它不根据自然日推断 T+1/T+3/T+5 已到期。
    Ok(now + Duration::minutes(20))
}
