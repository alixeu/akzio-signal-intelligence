use super::*;

/// Also releases on worker error, task deadline cancellation, and unwinding.
pub(super) struct OutcomeLeaseGuard {
    pub store: Store,
    pub lease: akzio_store::DaemonLease,
}

impl Drop for OutcomeLeaseGuard {
    fn drop(&mut self) {
        if let Err(error) = self.store.release_daemon_lease(&self.lease, Utc::now()) {
            tracing::warn!(%error, "Outcome lease release failed; expiry remains the recovery bound");
        }
    }
}

// This is a polling deadline, never proof that a market session has closed.
pub(super) fn next_outcome_check_at(now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    Ok(now + Duration::minutes(20))
}
