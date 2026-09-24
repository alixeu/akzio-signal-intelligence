// 文件导读：serve 是 scheduler 的长期轮询壳，只调用有界 `tick`，在每次失败后保持
// fail-closed 并等待下一轮；它与 worker 共享 shutdown。poll loop 正常返回只代表后台
// Future 收到关闭信号，不代表最后一次 Run、Paper order、fill 或 Outcome 成功。
// Rust 机制：泛型 clock/source 以 `?Sized` 接受 trait object；`watch::Receiver` 借用/clone
// 传播停止状态；`tokio::select!` 在 sleep 和 shutdown.changed() 之间竞争，避免阻塞退出。

use super::*;

impl PaperScheduler {
    pub async fn serve<C, P>(
        &self,
        clock: &C,
        source: &P,
        poll_interval: StdDuration,
        mut shutdown: watch::Receiver<bool>,
    ) -> SchedulerResult<()>
    where
        C: BrokerSessionClock + ?Sized,
        P: PaperWorkflowSource + ?Sized,
    {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            if let Err(error) = self.tick(clock, source, Utc::now()).await {
                tracing::warn!(error = %error, "Paper scheduler tick failed closed");
            }
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }
}
