// 文件导读：集中保存模型可见的 Context 解释性提示；文本说明授权投影的语义，
// 本身不创建、扩大或替代 ReadGrant。
//! Interpretation of Rust-authorized context projections. Not an access grant.
pub(super) const PRODUCER_SCOPE: &str = "False 表示该证据是你当前上下文中的额外覆盖。它不反驳 Claim 关于该证据不在其生产者所选上下文中的表述。被选中不等于工具已经读取；仅属于 producer 的省略文档不会被披露。\n";
pub(super) const READING_POLICY: &str = "下面的 required document views 已经包含授权事实。省略的细节不表示不存在。只针对具体未解决的问题使用工具；工具限制是上限，不是目标。\n";
pub(super) const EVIDENCE_CLOSURE: &str = "提交这些 Claims 和 Critiques 时，将 required_proposal_evidence 中的每个精确引用复制到 result.evidence，包括描述性 grounds 和中性 forecasts。这是已选引用索引，不是方向性支持，也不是读取未选来源的许可。Rust 仍然会校验完整闭包。\n";
pub(super) const FULL_DOCUMENT: &str = "每个 must_read 项也会标识一个已授权文档；documents 列出可选 grants。Views 省略详细 grounds 和 policy traces。需要原始授权文档时，使用 document_id 配合 read_document/read_range。省略字段不表示为空或已核验。\n";
pub(super) const COLLECTION_SCOPE: &str = "Collection status 不是 ReadGrant。由于上下文限制，可用资源可能不在本任务选中的文档中。不要将未选中的数据标记为不可用。这里省略的 Artifact 引用仍保留在已授权的完整状态文档中。\n";
