// AgentRuntime 持有 Store/StoreExecutor/ContextBroker 的共享 Arc/Mutex 边界；公开构造
// 只组装这些能力，真正的任务 permit、Contract 和 Manifest 校验仍在 run_inner 中做。
impl AgentRuntime {
    pub fn new(store: Store, catalogue: ContractCatalogue, grant_ttl: Duration) -> Self {
        Self {
            context: ContextBroker::new(store.clone()),
            store_executor: StoreExecutor::new(store.clone()),
            store,
            catalogue,
            grant_ttl,
            historical_projection: None,
            reasoning_events: None,
        }
    }

    pub fn with_historical_projection(
        mut self,
        condition: akzio_domain::ExperimentCondition,
        cutoff: chrono::NaiveDate,
    ) -> Self {
        self.historical_projection = Some(HistoricalProjection::new(condition, cutoff));
        self
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
        // 同步 Store 读取放到共享 StoreExecutor 的 blocking 队列，Future 被取消时
        // 也不会释放正在执行的 Store 工作或破坏 CAS 串行化。
        let context = self.context.clone();
        let contract = contract.clone();
        let document = document.clone();
        Ok(self
            .store_executor
            .execute(move |_| context.read_authority_document(&contract, &document))
            .await??)
    }


    async fn validate_authority_permit(&self, permit: &TaskWritePermit) -> ResearchResult<()> {
        // permit 在每个副作用前重新由 Store 认证，防止长时间模型调用后 lease/epoch
        // 已变化仍写入旧 Attempt。
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
    ) -> ResearchResult<akzio_store::SucceededAttemptProof> {
        // 子 Agent 只接收父 Attempt 的成功证明；它不是读取任意历史 Artifact 的通道，
        // ContextBroker 会继续按父 Contract、Run 和 lineage 收缩授权。
        let run_id = run_id.clone();
        let parent_task_id = parent_task_id.clone();
        Ok(self
            .store_executor
            .execute(move |store| store.current_succeeded_attempt(&run_id, &parent_task_id))
            .await??)
    }


}
