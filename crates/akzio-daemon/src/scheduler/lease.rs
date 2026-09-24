// 文件导读：lease helper 只负责 scheduler daemon lease 的取得、heartbeat、失效重取和
// StoreExecutor 异步排队。它保护唯一 scheduler owner，不代表 session 已产生或 Paper
// 订单已提交；测试还验证维护窗口延长 lease 时 heartbeat 不会回退截止时间。
// Rust 机制：`Mutex` guard 的借用范围覆盖同步 heartbeat，poison 转为显式错误；异步方法
// clone scheduler 后把闭包 move 进 executor，避免持有 MutexGuard 跨 await；Drop/过期恢复
// 由 Store 的 durable lease 逻辑负责。

use super::*;

impl PaperScheduler {
    pub(super) fn acquire_or_renew(&self, now: DateTime<Utc>) -> SchedulerResult<DaemonLease> {
        // 先在 Mutex 内尝试 heartbeat 现有 owner/epoch；durable heartbeat 返回 false 时清空
        // 本地缓存，再原子 acquire 新 epoch，避免使用过期 lease 写入 Paper 状态。
        let expires_at = now + self.lease_duration;
        let mut held = self
            .lease
            .lock()
            .map_err(|_| SchedulerError::LeasePoisoned)?;
        if let Some(lease) = held.as_mut() {
            if self.store.heartbeat_daemon_lease(lease, now, expires_at)? {
                lease.expires_at = self
                    .store
                    .daemon_lease(SCHEDULER_LEASE_NAME)?
                    .filter(|current| {
                        current.owner_id == lease.owner_id && current.epoch == lease.epoch
                    })
                    .ok_or(SchedulerError::NotLeader)?
                    .expires_at;
                return Ok(lease.clone());
            }
            *held = None;
        }

        let lease = self
            .store
            .acquire_daemon_lease(SCHEDULER_LEASE_NAME, &self.owner_id, now, expires_at)?
            .ok_or(SchedulerError::NotLeader)?;
        *held = Some(lease.clone());
        Ok(lease)
    }

    pub(super) async fn acquire_or_renew_async(&self) -> SchedulerResult<DaemonLease> {
        // 取得 lease 的同步 Store 操作排到共享 executor 后才开始计算有效期，队列等待不
        // 消耗新 lease 的有效窗口。
        let scheduler = self.clone();
        self.store_executor
            .execute(move |_| scheduler.acquire_or_renew(Utc::now()))
            .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduler() -> PaperScheduler {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.akzio/audit-20260919/followup-20260920/tests/scheduler")
            .join(RunId::new().0);
        let store = Store::open(root).unwrap();
        let catalogue =
            akzio_research::ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
        PaperScheduler::new(
            store.clone(),
            WorkflowRuntime::new(store, catalogue.recipes),
            "queue-test".into(),
        )
        .unwrap()
    }

    struct ColdStartClock {
        open: bool,
    }
    impl BrokerSessionClock for ColdStartClock {
        fn open_session_key<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = SchedulerResult<Option<String>>> + Send + 'a>> {
            Box::pin(async move { Ok(self.open.then(|| "2026-09-22".into())) })
        }
        fn paper_account_id<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = SchedulerResult<String>> + Send + 'a>> {
            Box::pin(async {
                panic!("cold-start scheduling must not request trading approval identity")
            })
        }
    }

    #[tokio::test]
    async fn cold_start_scheduler_reserves_canonical_paper_without_approval() {
        let scheduler = scheduler();
        let source = StorePaperWorkflowSource::new(scheduler.store.clone())
            .with_bootstrap(scheduler.workflow.clone(), "active");
        assert!(scheduler.store.active_decision_policy().unwrap().is_none());
        assert!(scheduler
            .tick(&ColdStartClock { open: false }, &source, Utc::now())
            .await
            .unwrap()
            .is_none());
        let first = scheduler
            .tick(&ColdStartClock { open: true }, &source, Utc::now())
            .await
            .unwrap()
            .expect("first canonical run must not wait for a policy trained on future outcomes");
        assert!(first.newly_reserved);
        let run = first.slot.workflow.run.run_id;
        assert_eq!(
            scheduler.store.run_purpose(&run).unwrap(),
            RunPurpose::Paper
        );
        assert!(scheduler
            .store
            .paper_approval_for_run(&run)
            .unwrap()
            .is_none());
        assert!(scheduler.store.debug_session(&run).unwrap().is_none());
        assert_eq!(
            scheduler
                .store
                .run_artifacts_by_kind(&run, ArtifactKind::EvidenceNeed)
                .unwrap()
                .len(),
            40
        );
        let repeated = scheduler
            .tick(&ColdStartClock { open: true }, &source, Utc::now())
            .await
            .unwrap()
            .unwrap();
        assert!(!repeated.newly_reserved);
        assert_eq!(repeated.slot.workflow.run.run_id, run);
        scheduler.store.verify_integrity().unwrap();
    }

    #[tokio::test]
    async fn queued_scheduler_lease_starts_when_store_work_can_execute() {
        let scheduler = scheduler();
        // Test both initial acquisition and renewal behind the shared Store queue.
        for _ in 0..2 {
            let executor = scheduler.store_executor.clone();
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = std::sync::mpsc::channel();
            let blocker = tokio::spawn(async move {
                executor
                    .execute(move |_| {
                        started.send(()).unwrap();
                        wait.recv().unwrap();
                    })
                    .await
                    .unwrap();
            });
            ready.await.unwrap();
            let worker = scheduler.clone();
            let renewal =
                tokio::spawn(async move { worker.acquire_or_renew_async().await.unwrap() });
            while scheduler.store_executor.telemetry().queued_operation_count == 0 {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(StdDuration::from_millis(20)).await;
            let released_at = Utc::now();
            release.send(()).unwrap();
            blocker.await.unwrap();
            let lease = renewal.await.unwrap();
            assert!(
                lease.expires_at >= released_at + scheduler.lease_duration,
                "time in the Store queue must not consume the newly issued lease"
            );
            assert_eq!(
                scheduler
                    .store
                    .daemon_lease(SCHEDULER_LEASE_NAME)
                    .unwrap()
                    .unwrap(),
                lease
            );
        }
    }

    #[test]
    fn daemon_heartbeat_preserves_maintenance_extended_deadline() {
        let scheduler = scheduler();
        let now = Utc::now();
        let first = scheduler.acquire_or_renew(now).unwrap();
        scheduler
            .store
            .defer_live_leases_for_maintenance(now, now + Duration::seconds(60))
            .unwrap();
        let renewed = scheduler
            .acquire_or_renew(now + Duration::seconds(1))
            .unwrap();
        let stored = scheduler
            .store
            .daemon_lease(SCHEDULER_LEASE_NAME)
            .unwrap()
            .unwrap();
        assert_eq!(stored.epoch, first.epoch);
        assert!(
            stored.expires_at >= first.expires_at + Duration::seconds(60),
            "heartbeat cannot undo maintenance lease preservation"
        );
        assert_eq!(
            renewed, stored,
            "cached lease must reflect the actual durable deadline"
        );
    }
}
