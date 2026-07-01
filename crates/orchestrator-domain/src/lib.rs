pub mod artifact;
pub mod degraded;
pub mod error;
pub mod phase;
pub mod role;

pub use artifact::{AgentTurnInfo, Rating, RoleJobResult};
pub use degraded::{ConfidenceImpact, DegradedEntry, DegradedReport};
pub use error::DomainError;
pub use phase::Phase;
pub use role::AnalystRole;
