impl PaperScheduler {
    fn acquire_or_renew(&self, now: DateTime<Utc>) -> SchedulerResult<DaemonLease> {
        let expires_at = now + self.lease_duration;
        let mut held = self
            .lease
            .lock()
            .map_err(|_| SchedulerError::LeasePoisoned)?;
        if let Some(lease) = held.as_mut() {
            if self.store.heartbeat_daemon_lease(lease, now, expires_at)? {
                lease.expires_at = expires_at;
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

    async fn acquire_or_renew_async(&self, now: DateTime<Utc>) -> SchedulerResult<DaemonLease> {
        let expires_at = now + self.lease_duration;
        let current = self
            .lease
            .lock()
            .map_err(|_| SchedulerError::LeasePoisoned)?
            .clone();
        if let Some(mut lease) = current {
            let heartbeat_lease = lease.clone();
            let renewed = self
                .store_executor
                .execute(move |store| {
                    store.heartbeat_daemon_lease(&heartbeat_lease, now, expires_at)
                })
                .await??;
            if renewed {
                lease.expires_at = expires_at;
                *self
                    .lease
                    .lock()
                    .map_err(|_| SchedulerError::LeasePoisoned)? = Some(lease.clone());
                return Ok(lease);
            }
            *self
                .lease
                .lock()
                .map_err(|_| SchedulerError::LeasePoisoned)? = None;
        }

        let owner_id = self.owner_id.clone();
        let lease = self
            .store_executor
            .execute(move |store| {
                store.acquire_daemon_lease(
                    SCHEDULER_LEASE_NAME,
                    &owner_id,
                    now,
                    expires_at,
                )
            })
            .await??
            .ok_or(SchedulerError::NotLeader)?;
        *self
            .lease
            .lock()
            .map_err(|_| SchedulerError::LeasePoisoned)? = Some(lease.clone());
        Ok(lease)
    }
}
