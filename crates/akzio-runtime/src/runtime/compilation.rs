use super::*;

// 编译模块按职责拆成 lowering、validation、evidence 和 helper，但 include 后共享
// 同一 Runtime impl 作用域。所有函数只构造/检查 Rust-owned graph，不执行 Agent、Broker
// 或 Outcome；graph 提交仍需 Store 的 immutable CAS/event 事务。
include!("compilation/lowering.rs");
include!("compilation/validation.rs");
include!("compilation/evidence.rs");
include!("compilation/helpers.rs");
