// 文件导读：这里把 AlpacaPaper 的具体异步方法适配到 CommittedPaperBroker trait。
// 每个方法只转发到已在 paper/execute.rs 或 paper/reconcile.rs 中实现的路径，保留
// commitment、授权和恢复语义，不在 trait adapter 中增加新的业务分支。

impl CommittedPaperBroker for AlpacaPaper {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
        authorization: &'a PaperSubmissionAuthorization,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        // Box::pin 把借用 self/commitment/plan/authorization 的 async future 返回给调度器，
        // 生命周期覆盖整个 broker 请求，不复制大对象也不改变所有权；构造这个 Future
        // 本身不发请求，调用方 await 后才由 executor 轮询并执行请求体。
        Box::pin(AlpacaPaper::execute_committed(
            self,
            commitment,
            plan,
            authorization,
        ))
    }

    fn reconcile_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = Result<PaperExecution>> + Send + 'a>> {
        // 对账同样只借用已有 execution，异步刷新不会重新生成 commitment；Future 的生命周期
        // 与两份输入引用一致，完成后输出一份新拥有的 PaperExecution。
        Box::pin(AlpacaPaper::reconcile_committed(
            self, commitment, execution,
        ))
    }

    fn cancel_order<'a>(
        &'a self,
        intent: &'a PaperCancel,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>> {
        // 取消 intent 自带原始 client/broker ID，由 reconcile 实现做 durable identity 校验；
        // await 驱动异步 DELETE/GET 路径，trait adapter 不自行创建 Tokio task。
        Box::pin(AlpacaPaper::cancel_committed_order(self, intent))
    }

    fn replace_order<'a>(
        &'a self,
        intent: &'a PaperReprice,
        authorization: &'a PaperSubmissionAuthorization,
    ) -> Pin<Box<dyn Future<Output = Result<PaperOrderReceipt>> + Send + 'a>> {
        // 替换 intent 还需 submission authorization，确保过期窗口不能产生新的 PATCH；
        // 返回的 Send Future 可被 worker 驱动，错误仍作为 Result 传回 dispatch。
        Box::pin(AlpacaPaper::replace_committed_order(
            self,
            intent,
            authorization,
        ))
    }
}
