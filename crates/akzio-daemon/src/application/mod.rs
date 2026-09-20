//! Internal application capabilities used by daemon dispatch and transport.
//!
//! These seams keep orchestration out of the process supervisor without
//! introducing public framework abstractions. Policy and durable invariants
//! remain owned by their domain/runtime crates.
//!
//! Call flow:
//! - task dispatch -> `ResearchRun` -> `AgentSession` / `EvidenceAcquisition`;
//! - task dispatch -> `PaperExecution` for deterministic gates and reconciliation;
//! - task dispatch -> `OutcomeSealing` / the outcome worker for sealed outcomes;
//! - HTTP transport -> `Maintenance` -> the runtime-owned Store executor.

mod agent_session;
mod evidence_acquisition;
mod maintenance;
mod outcome_sealing;
mod paper_execution;
mod research_run;

pub(crate) use agent_session::AgentSession;
pub(crate) use evidence_acquisition::EvidenceAcquisition;
pub(crate) use maintenance::Maintenance;
pub(crate) use outcome_sealing::OutcomeSealing;
pub(crate) use paper_execution::PaperExecution;
pub(crate) use research_run::ResearchRun;

use crate::Daemon;

impl Daemon {
    // 为任务处理提供只读借用的 AgentSession；具体模型调用仍由 AgentRuntime 执行。
    pub(crate) const fn agent_session(&self) -> AgentSession<'_> {
        AgentSession::new(self)
    }

    // 为 Evidence Gate 和一次性补采提供统一的 Daemon 门面。
    pub(crate) const fn evidence_acquisition(&self) -> EvidenceAcquisition<'_> {
        EvidenceAcquisition::new(self)
    }

    // 克隆 StoreExecutor 的句柄，让 HTTP/维护调用进入运行时规定的串行 Store 通道。
    pub(crate) fn maintenance(&self) -> Maintenance {
        Maintenance::new(self.store_executor.clone())
    }

    // 为 OutcomeSchedule 的创建与提交提供当前 Daemon 的 OutcomeSealing 门面。
    pub(crate) const fn outcome_sealing(&self) -> OutcomeSealing<'_> {
        OutcomeSealing::new(self)
    }

    // 为 Paper Decision/Execution/Commit/Reconcile 阶段提供当前 Daemon 的执行门面。
    pub(crate) const fn paper_execution(&self) -> PaperExecution<'_> {
        PaperExecution::new(self)
    }

    // 为研究节点提供显式研究拓扑的执行门面；研究输出不会绕过后续 Decision Gate。
    pub(crate) const fn research_run(&self) -> ResearchRun<'_> {
        ResearchRun::new(self)
    }
}

// 新研究循环和补采协调以私有 Daemon 扩展实现，避免把编排细节暴露为公共 API。
mod research_loop;
mod research_supplement;
