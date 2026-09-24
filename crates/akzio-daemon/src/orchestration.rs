// 文件导读：本文件只组织 daemon 的 bootstrap、control、health/canary 和 worker 子模块。
// 具体状态仍由 runtime/Store 持有；模块入口形成 CLI/HTTP → daemon → scheduler/worker 的
// 组装链，不把调度层的返回值夸大成研究、Decision、订单或 Outcome 业务结论。
// Rust 机制：`#[path] mod` 把实现拆成私有子模块；`super::*` 复用父模块导入，避免新增
// 并行状态，所有子模块共享同一 `Daemon` 借用和 StoreExecutor。

use super::*;

#[path = "orchestration/bootstrap.rs"]
mod bootstrap;
#[path = "orchestration/control.rs"]
mod control;
#[path = "orchestration/health_canary.rs"]
mod health_canary;
#[path = "orchestration/workers.rs"]
mod workers;
