impl AgentRuntime {
    pub fn new(store: V2Store, catalogue: ContractCatalogue, grant_ttl: Duration) -> Self {
        Self {
            context: ContextBroker::new(store.clone()),
            store_executor: StoreExecutor::new(store.clone()),
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

    pub fn with_store_executor(mut self, store_executor: StoreExecutor) -> Self {
        self.store_executor = store_executor;
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

    async fn read_authority_document(
        &self,
        contract: &AgentContract,
        document: &akzio_domain::BlobRef,
    ) -> ResearchResult<Vec<u8>> {
        let context = self.context.clone();
        let contract = contract.clone();
        let document = document.clone();
        Ok(self
            .store_executor
            .execute(move |_| context.read_authority_document(&contract, &document))
            .await??)
    }


    async fn validate_authority_permit(&self, permit: &TaskWritePermit) -> ResearchResult<()> {
        let permit = permit.clone();
        Ok(self
            .store_executor
            .execute(move |store| store.validate_task_permit(&permit))
            .await??)
    }

    async fn load_parent_succeeded_attempt(
        &self,
        run_id: &RunId,
        parent_task_id: &TaskId,
    ) -> ResearchResult<akzio_store::v2::SucceededAttemptProof> {
        let run_id = run_id.clone();
        let parent_task_id = parent_task_id.clone();
        Ok(self
            .store_executor
            .execute(move |store| store.current_succeeded_attempt(&run_id, &parent_task_id))
            .await??)
    }

    async fn run_purpose_for(&self, run_id: &RunId) -> ResearchResult<RunPurpose> {
        let run_id = run_id.clone();
        Ok(self
            .store_executor
            .execute(move |store| store.run_purpose(&run_id))
            .await??)
    }
}
