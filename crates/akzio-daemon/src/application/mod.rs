//! Internal application capabilities used by daemon dispatch and transport.
//!
//! These seams keep orchestration out of the process supervisor without
//! introducing public framework abstractions. Policy and durable invariants
//! remain owned by their domain/runtime crates.
//!
//! Call flow:
//! - task dispatch -> `ResearchRun` -> `AgentSession` / `EvidenceAcquisition`;
//! - task dispatch -> `PaperExecution` for deterministic gates and reconciliation;
//! - task dispatch -> `OutcomeSealing` / `LearningEvaluation` for sealed outcomes;
//! - HTTP transport -> `Maintenance` -> the runtime-owned Store executor.

mod agent_session;
mod evidence_acquisition;
mod learning_evaluation;
mod maintenance;
mod outcome_sealing;
mod paper_execution;
mod research_run;

pub(crate) use agent_session::AgentSession;
pub(crate) use evidence_acquisition::EvidenceAcquisition;
pub(crate) use learning_evaluation::LearningEvaluation;
pub(crate) use maintenance::Maintenance;
pub(crate) use outcome_sealing::OutcomeSealing;
pub(crate) use paper_execution::PaperExecution;
pub(crate) use research_run::ResearchRun;

use crate::Daemon;

impl Daemon {
    pub(crate) const fn agent_session(&self) -> AgentSession<'_> {
        AgentSession::new(self)
    }

    pub(crate) const fn evidence_acquisition(&self) -> EvidenceAcquisition<'_> {
        EvidenceAcquisition::new(self)
    }

    pub(crate) const fn learning_evaluation(&self) -> LearningEvaluation<'_> {
        LearningEvaluation::new(self)
    }

    pub(crate) fn maintenance(&self) -> Maintenance {
        Maintenance::new(self.store_executor.clone())
    }

    pub(crate) const fn outcome_sealing(&self) -> OutcomeSealing<'_> {
        OutcomeSealing::new(self)
    }

    pub(crate) const fn paper_execution(&self) -> PaperExecution<'_> {
        PaperExecution::new(self)
    }

    pub(crate) const fn research_run(&self) -> ResearchRun<'_> {
        ResearchRun::new(self)
    }
}
