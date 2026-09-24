//! Store implementation for source-incompatible Akzio authority.
//!
//! Store Root 使用固定的数据库文件名和版本标签；新旧版本能否升级由
//! `Store::open` 的初始化/迁移路径判定，不能仅凭文件名推断兼容。

// 文件导读：本模块把持久化能力分成表/子域模块与由 `include!` 拼接的 Store API 实现片段。
// `schema` 负责打开、初始化和升级，`blob`/`workflow`/`lease` 等子模块负责底层读写；
// `impl_*` 与 `free_*` 片段共享本模块的类型、私有函数和连接锁，因此不会形成第二份状态。
// Store 是 SQLite/CAS、workflow、policy、lease、debug 和 learning 的唯一持久化权威；include 片段共享父模块的类型与连接能力。
// 原子范围不是所有方法共用一个事务，而是每个写入口明确创建或借用的 SQLite Transaction。
// `rusqlite::params!` 把 Rust 值绑定到 SQL 的 ?1/?2 等占位符，值不会用字符串拼接进 SQL；
// SQL schema/查询本身仍是本 crate 的受维护字符串，事务由相应 Store 方法明确建立。
mod attempt;
mod blob;
mod canary;
mod debug;
mod debug_bundle;
mod decision_policy;
mod doctor;
mod execution;
mod experiment;
mod learning;
mod lease;
mod lesson;
mod maintenance;
mod migration;
mod release;
mod research_quality;
mod research_review;
mod run_control;
mod schema;
pub use run_control::{RunCheckpoint, RunControlView, RunEventPage, RunEventView, RunInspection};
mod trajectory;
mod workflow;

pub use canary::{CanaryCampaignHead, StoredCanarySession};
pub use debug::{DebugArtifactView, DebugAttemptView, DebugNodeView, DebugRunView};
pub use debug_bundle::{DebugBundleIntegrity, DebugBundleManifest, DebugBundleRawAccess};
pub use decision_policy::{DecisionPolicyDescriptor, StoredDecisionPolicy};
pub use lesson::{LessonRevalidationScan, LessonUsage, LessonWriteResult, StoredLesson};
pub use maintenance::MaintenanceLeaseDeferral;
pub use research_review::{ResearchAudit, ResearchAuditRecord};

// include! 在编译期把这些源文件内容放入当前模块：它们可直接访问这里的私有类型与导入，
// 但物理文件仍按职责拆分供维护者阅读。实现是同步 rusqlite；连接由 Arc<Mutex<_>> 共享，
// 锁只协调本 Store 实例内的连接访问，不替代 SQLite 锁、fencing 或跨进程 lease。
// prelude/public_types 定义共享类型；impl/free_* 按职责拆分方法，但不创建并行状态存储或绕过 Store。
include!("store/prelude.rs");
include!("store/public_types.rs");

include!("store/impl_core.rs");
include!("store/impl_workflow.rs");
include!("store/impl_queries.rs");
include!("store/impl_history.rs");
include!("store/impl_learning.rs");

include!("store/free_validation.rs");
include!("store/free_lifecycle.rs");
include!("store/free_trajectory.rs");
include!("store/free_events.rs");
include!("store/free_policy_helpers.rs");
include!("store/impl_attempt.rs");
include!("store/free_reads.rs");
include!("store/free_policy_reads.rs");
include!("store/free_paper_checks.rs");
