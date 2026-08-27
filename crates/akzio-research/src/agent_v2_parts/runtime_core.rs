impl AgentRuntime {
    pub fn new(store: V2Store, catalogue: ContractCatalogue, grant_ttl: Duration) -> Self {
        Self {
            context: ContextBroker::new(store.clone()),
            store,
            catalogue,
            grant_ttl,
            reasoning_events: None,
        }
    }

    pub fn with_reasoning_events(
        mut self,
        reasoning_events: broadcast::Sender<AgentReasoningEvent>,
    ) -> Self {
        self.reasoning_events = Some(reasoning_events);
        self
    }

    pub fn contract(
        &self,
        hash: &akzio_domain::ContentHash,
    ) -> ResearchResult<&InstalledContract> {
        self.catalogue.get(hash)
    }

    pub fn context_policy(
        &self,
        hash: &akzio_domain::ContentHash,
    ) -> ResearchResult<&ContextPolicy> {
        Ok(&self.contract(hash)?.contract.context)
    }


    fn validate_authority_permit(&self, permit: &TaskWritePermit) -> ResearchResult<()> {
        Ok(self.store.validate_task_permit(permit)?)
    }

    fn load_parent_succeeded_attempt(
        &self,
        run_id: &RunId,
        parent_task_id: &TaskId,
    ) -> ResearchResult<akzio_store::v2::SucceededAttemptProof> {
        Ok(self
            .store
            .current_succeeded_attempt(run_id, parent_task_id)?)
    }

    fn run_purpose_for(&self, run_id: &RunId) -> ResearchResult<RunPurpose> {
        Ok(self.store.run_purpose(run_id)?)
    }
}
