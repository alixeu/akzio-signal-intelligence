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
