use super::*;

/// Canonical lease rows extended after one drained maintenance operation.
///
/// Maintenance never rewrites ownership or epochs. Only leases that were live
/// when maintenance started are extended by the time spent in maintenance.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceLeaseDeferral {
    pub task_leases: u64,
    pub daemon_leases: u64,
}

impl V2Store {
    /// Preserve live task and daemon ownership across a drained maintenance
    /// window. Expired leases remain expired and can still be recovered.
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
