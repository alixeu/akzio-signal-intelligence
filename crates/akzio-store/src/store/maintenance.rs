// 文件导读：维护窗口按开始时刻选择仍有效的 Task/daemon lease，并按实际维护耗时延长到期时间；
// 它不改 owner、epoch、任务结果或历史事件。先读 defer_live_leases_for_maintenance 的
// Immediate 事务和旧值条件 UPDATE，理解这些写入不覆盖已被并发接管的 lease。
use super::*;

/// 一次已排空维护窗口内被延长的租约行数；不重写 owner 或 epoch。
/// 只选择维护开始时仍有效的租约；它们可能在维护结束前自然到期。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceLeaseDeferral {
    pub task_leases: u64,
    pub daemon_leases: u64,
}

impl Store {
    // 先在一个 Immediate 事务中锁定候选租约并按原值条件更新，避免覆盖并发接管或恢复。
    // started_at/completed_at 确定维护耗时；非正耗时直接返回两个零计数且不取连接。
    // 查询只选 started_at 时仍未过期的 running Task 与 daemon lease，UPDATE 再以旧 expiry 作 CAS；
    // 所有成功更新与计数共用一个事务，失败时整个批次回滚，返回值是实际受影响行数。
    /// 将维护耗时加到开始时仍有效的 Task/daemon lease 到期时间。
    /// `started_at` 时已到期的不延长；维护期间到期的会被这次延长覆盖。
    pub fn defer_live_leases_for_maintenance(
        &self,
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    ) -> StoreResult<MaintenanceLeaseDeferral> {
        let elapsed = completed_at.signed_duration_since(started_at);
        if elapsed <= Duration::zero() {
            return Ok(MaintenanceLeaseDeferral::default());
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task_leases = {
            let mut statement = transaction.prepare(
                "SELECT task_id, lease_until FROM rebuild_tasks \
                 WHERE status = 'running' AND lease_until > ?1 ORDER BY task_id",
            )?;
            let rows = statement
                .query_map(params![started_at.to_rfc3339()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        let daemon_leases = {
            let mut statement = transaction.prepare(
                "SELECT lease_name, expires_at FROM rebuild_daemon_leases \
                 WHERE expires_at > ?1 ORDER BY lease_name",
            )?;
            let rows = statement
                .query_map(params![started_at.to_rfc3339()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };

        let mut deferred = MaintenanceLeaseDeferral::default();
        for (task_id, lease_until) in task_leases {
            let extended = parse_time(&lease_until)? + elapsed;
            deferred.task_leases += transaction.execute(
                "UPDATE rebuild_tasks SET lease_until = ?1 \
                 WHERE task_id = ?2 AND status = 'running' AND lease_until = ?3",
                params![extended.to_rfc3339(), task_id, lease_until],
            )? as u64;
        }
        for (lease_name, expires_at) in daemon_leases {
            let extended = parse_time(&expires_at)? + elapsed;
            deferred.daemon_leases += transaction.execute(
                "UPDATE rebuild_daemon_leases SET expires_at = ?1 \
                 WHERE lease_name = ?2 AND expires_at = ?3",
                params![extended.to_rfc3339(), lease_name, expires_at],
            )? as u64;
        }
        transaction.commit()?;
        Ok(deferred)
    }
}
