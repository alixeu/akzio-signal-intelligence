impl CommittedPaperBroker for AlpacaPaper {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        Box::pin(AlpacaPaper::execute_committed(self, commitment, plan))
    }


    fn reconcile_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        Box::pin(AlpacaPaper::reconcile_committed(
            self, commitment, execution,
        ))
    }

    fn cancel_order<'a>(
        &'a self,
        intent: &'a PaperCancel,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>> {
        Box::pin(AlpacaPaper::cancel_committed_order(self, intent))
    }

    fn replace_order<'a>(
        &'a self,
        intent: &'a PaperReprice,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>> {
        Box::pin(AlpacaPaper::replace_committed_order(self, intent))
    }
}
