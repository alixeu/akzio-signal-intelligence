//! Outcome collection and evaluation dispatch.

// 文件导读：Outcome 模块负责在 Paper Run 终态之后按独立 lease 推进 T+1/T+3/T+5，读取
// 真实共同交易 Session，生成阶段复盘和 T+5 sealed Outcome；Canary/Shadow 有额外的父子
// lineage。Outcome worker 的完成、narrative repair 或 NoOrder schedule 都不等于 Paper
// fill、账户 NAV 可用或 calibration/learning 已通过。
// Rust 机制：子模块以 `#[path] mod` 拆分实现并共享父模块私有类型；lease guard 用 Drop
// 做错误/取消/展开时的释放，`Option` 表示窗口尚未到期，Future/async I/O 受 bounded
// worker 和 Store fencing 约束。

use super::*;

#[path = "outcome/canary.rs"]
mod canary;
#[path = "outcome/collection.rs"]
mod collection;
#[path = "outcome/helpers.rs"]
mod helpers;
#[path = "outcome/materialization.rs"]
mod materialization;
#[path = "outcome/narrative_repair.rs"]
mod narrative_repair;
#[path = "outcome/shadow.rs"]
mod shadow;
#[path = "outcome/worker.rs"]
mod worker;

use helpers::*;
