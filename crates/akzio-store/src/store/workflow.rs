// 文件导读：Workflow 子模块负责 Contract 选择、Run/Task 创建、Attempt 输出和查询；
// graph Artifact、task 行、依赖关系与事件必须在同一提交边界内保持可重放。
use super::*;

include!("workflow/contracts.rs");
include!("workflow/commits.rs");
include!("workflow/tasks.rs");
include!("workflow/outputs.rs");
include!("workflow/queries.rs");
include!("workflow/helpers.rs");
