// 文件导读：Workflow 子模块负责 Contract 选择、Run/Task 创建、Attempt 输出和查询；
// graph Artifact、task 行、依赖关系与事件必须在同一提交边界内保持可重放。
// 这里的 `include!` 将六个源码片段在编译期展开到同一模块，不是六份独立的 Store 状态；
// 因而子文件内的私有 helper 可被这些片段互相调用。先读 contracts/commits
// 理解图与 Run 如何安装，再读 tasks/outputs/queries 跟踪领取、提交和重建快照。
use super::*;

include!("workflow/contracts.rs");
include!("workflow/commits.rs");
include!("workflow/tasks.rs");
include!("workflow/outputs.rs");
include!("workflow/queries.rs");
include!("workflow/helpers.rs");
