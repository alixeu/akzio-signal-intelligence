use crate::*;

/// Sealed-outcome learning and canary evaluation entry point.
pub(crate) struct LearningEvaluation<'a> {
    daemon: &'a Daemon,
}

impl<'a> LearningEvaluation<'a> {
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    pub(crate) async fn execute(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        self.daemon.execute_outcome_worker(task, now).await
    }
}
