// AgentRuntime 持有 Store、StoreExecutor 和 ContextBroker 的句柄；构造与 with_*
// 仅组装能力，不进行模型调用、授权或持久化。真正的 permit、Contract、Manifest
// 和冻结预算校验在 run_inner 中执行；不要把这里的可克隆句柄当成模型读取权限。
impl AgentRuntime {
    /// 构造受 ContractCatalogue 约束的运行时；错误只会在具体任务的检查路径返回。
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
        // `mut self` 获取整个值的所有权后替换字段，再将它移回调用方；不修改别处
        // 已有的 AgentRuntime，亦不改变持久化 task/contract 身份。
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
        // 同步 Store 读取放到共享 StoreExecutor 的 blocking 队列；move 闭包拥有
        // clone 出来的句柄，不借用临时的 &contract。外层 Future 取消也不能假定
        // 已排入队列的 Store 工作撤销；双层 ? 依次传播队列与 Context 错误。
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
