//! The explicit source-of-truth registry for FileStore ToolManaged roles.
//!
//! A role/profile pair has one FileStore-backed typed artifact contract.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

pub const ROLE_PROFILE_REGISTRY_SCHEMA_VERSION: u32 = 1;
pub const BUILTIN_ROLE_PROFILE_REGISTRY_VERSION: u32 = 1;

/// The typed output contract a role is being migrated to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolManagedProfile {
    HistoricalReflection,
    AnalystReport,
    TopicGeneration,
    ResearcherWarmup,
    DebateSeed,
    DebateResponse,
    TopicControl,
    ResearchDecision,
    TradeIntent,
    RiskReview,
    PortfolioDecision,
    PhaseSummary,
}

impl ToolManagedProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HistoricalReflection => "historical_reflection",
            Self::AnalystReport => "analyst_report",
            Self::TopicGeneration => "topic_generation",
            Self::ResearcherWarmup => "researcher_warmup",
            Self::DebateSeed => "debate_seed",
            Self::DebateResponse => "debate_response",
            Self::TopicControl => "topic_control",
            Self::ResearchDecision => "research_decision",
            Self::TradeIntent => "trade_intent",
            Self::RiskReview => "risk_review",
            Self::PortfolioDecision => "portfolio_decision",
            Self::PhaseSummary => "phase_summary",
        }
    }
}

/// Rust-owned unit planner identity. It is persisted in snapshots so a run is
/// never resumed with a different unit boundary than the one that created it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitPlanner {
    ReflectionTask,
    AnalystTicker,
    PhaseAggregate,
    ResearcherWarmup,
    DebateSeed,
    DebateResponse,
    TopicControlRound,
    ResearchTicker,
    TradeTicker,
    RiskTicker,
    PortfolioAsset,
    PhaseSummaryUnit,
}

/// Exact lookup key. A role may legitimately have more than one profile, for
/// example `mediator.topic` has both topic generation and researcher warmup.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RoleProfileKey {
    pub role_id: String,
    pub profile: ToolManagedProfile,
}

impl RoleProfileKey {
    pub fn new(role_id: impl Into<String>, profile: ToolManagedProfile) -> Self {
        Self {
            role_id: role_id.into(),
            profile,
        }
    }
}

/// The complete tool and artifact contract for one exact role/profile pair.
/// `tool_allowlist` is sorted and duplicate-free by construction; this makes
/// its serialized form stable for run-manifest snapshots and hashes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleProfileRegistration {
    pub role_id: String,
    pub profile: ToolManagedProfile,
    pub profile_version: u32,
    pub builder_version: u32,
    pub unit_planner: UnitPlanner,
    pub tool_allowlist: Vec<String>,
}

impl RoleProfileRegistration {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        role_id: impl Into<String>,
        profile: ToolManagedProfile,
        profile_version: u32,
        builder_version: u32,
        unit_planner: UnitPlanner,
        tool_allowlist: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, RoleProfileRegistryError> {
        let role_id = role_id.into();
        let mut tool_allowlist = tool_allowlist
            .into_iter()
            .map(Into::into)
            .collect::<Vec<String>>();
        tool_allowlist.sort();
        if tool_allowlist.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(RoleProfileRegistryError::DuplicateTool {
                role_id: role_id.clone(),
                profile,
                tool: tool_allowlist
                    .windows(2)
                    .find(|pair| pair[0] == pair[1])
                    .expect("duplicate tool must exist")[0]
                    .clone(),
            });
        }

        let registration = Self {
            role_id,
            profile,
            profile_version,
            builder_version,
            unit_planner,
            tool_allowlist,
        };
        registration.validate()?;
        Ok(registration)
    }

    pub fn key(&self) -> RoleProfileKey {
        RoleProfileKey::new(self.role_id.clone(), self.profile)
    }

    pub fn allows_tool(&self, tool_name: &str) -> bool {
        self.tool_allowlist
            .binary_search_by(|candidate| candidate.as_str().cmp(tool_name))
            .is_ok()
    }

    pub fn validate(&self) -> Result<(), RoleProfileRegistryError> {
        validate_role_id(&self.role_id)?;
        if self.profile_version == 0 {
            return Err(RoleProfileRegistryError::ZeroProfileVersion {
                role_id: self.role_id.clone(),
                profile: self.profile,
            });
        }
        if self.builder_version == 0 {
            return Err(RoleProfileRegistryError::ZeroBuilderVersion {
                role_id: self.role_id.clone(),
                profile: self.profile,
            });
        }
        if self.tool_allowlist.is_empty() {
            return Err(RoleProfileRegistryError::EmptyToolAllowlist {
                role_id: self.role_id.clone(),
                profile: self.profile,
            });
        }
        for tool in &self.tool_allowlist {
            validate_tool_name(tool, &self.role_id, self.profile)?;
        }
        for pair in self.tool_allowlist.windows(2) {
            if pair[0] >= pair[1] {
                return Err(RoleProfileRegistryError::NonCanonicalToolAllowlist {
                    role_id: self.role_id.clone(),
                    profile: self.profile,
                });
            }
        }
        Ok(())
    }
}

/// Persistable, deterministic role-profile snapshot for a run manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleProfileRegistrySnapshot {
    pub schema_version: u32,
    pub registry_version: u32,
    pub registrations: Vec<RoleProfileRegistration>,
    pub content_hash: String,
}

impl RoleProfileRegistrySnapshot {
    pub fn verify(&self) -> Result<(), RoleProfileRegistryError> {
        if self.schema_version != ROLE_PROFILE_REGISTRY_SCHEMA_VERSION {
            return Err(RoleProfileRegistryError::UnsupportedSnapshotSchema {
                found: self.schema_version,
                supported: ROLE_PROFILE_REGISTRY_SCHEMA_VERSION,
            });
        }
        let registry = RoleProfileRegistry::from_registrations(
            self.registry_version,
            self.registrations.clone(),
        )?;
        let expected = registry.snapshot().content_hash;
        if self.content_hash != expected {
            return Err(RoleProfileRegistryError::SnapshotHashMismatch {
                expected,
                found: self.content_hash.clone(),
            });
        }
        Ok(())
    }
}

/// A deterministic map of exact role/profile mappings. The registry exposes
/// no fallback resolution API by design.
#[derive(Debug, Clone)]
pub struct RoleProfileRegistry {
    registry_version: u32,
    registrations: BTreeMap<RoleProfileKey, RoleProfileRegistration>,
}

impl RoleProfileRegistry {
    pub fn new(registry_version: u32) -> Result<Self, RoleProfileRegistryError> {
        if registry_version == 0 {
            return Err(RoleProfileRegistryError::ZeroRegistryVersion);
        }
        Ok(Self {
            registry_version,
            registrations: BTreeMap::new(),
        })
    }

    pub fn builtin() -> Self {
        let registrations = vec![
            registration(
                "reflector.historical",
                ToolManagedProfile::HistoricalReflection,
                UnitPlanner::ReflectionTask,
                &[
                    "append_index_detail",
                    "create_index",
                    "finalize_historical_reflection",
                    "finalize_index",
                    "read_index_details",
                    "read_indexes",
                    "read_reflection_source",
                ],
            ),
            registration(
                "analyst.technical",
                ToolManagedProfile::AnalystReport,
                UnitPlanner::AnalystTicker,
                &[
                    "append_analyst_data_gap",
                    "append_analyst_evidence",
                    "finalize_analyst_report",
                    "read_index_details",
                    "read_indexes",
                    "read_technical_snapshot",
                    "set_analyst_assessment",
                    "set_analyst_invalidation",
                ],
            ),
            registration(
                "analyst.news_macro",
                ToolManagedProfile::AnalystReport,
                UnitPlanner::AnalystTicker,
                &[
                    "append_analyst_data_gap",
                    "append_analyst_evidence",
                    "finalize_analyst_report",
                    "read_index_details",
                    "read_indexes",
                    "read_jin10_candidates",
                    "set_analyst_assessment",
                    "set_analyst_invalidation",
                    "verify_event",
                ],
            ),
            registration(
                "mediator.topic",
                ToolManagedProfile::TopicGeneration,
                UnitPlanner::PhaseAggregate,
                &[
                    "create_phase2_topic",
                    "finalize_topic_generation",
                    "read_index_details",
                    "read_indexes",
                    "set_phase2_common_ground",
                ],
            ),
            registration(
                "mediator.topic",
                ToolManagedProfile::ResearcherWarmup,
                UnitPlanner::ResearcherWarmup,
                &[
                    "finalize_researcher_warmup",
                    "read_index_details",
                    "read_indexes",
                ],
            ),
            registration(
                "researcher.bull.initial",
                ToolManagedProfile::DebateSeed,
                UnitPlanner::DebateSeed,
                &[
                    "create_debate_claim",
                    "finalize_debate_seed",
                    "read_index_details",
                    "read_indexes",
                ],
            ),
            registration(
                "researcher.bear.initial",
                ToolManagedProfile::DebateSeed,
                UnitPlanner::DebateSeed,
                &[
                    "create_debate_claim",
                    "finalize_debate_seed",
                    "read_index_details",
                    "read_indexes",
                ],
            ),
            registration(
                "researcher.bull.interaction",
                ToolManagedProfile::DebateResponse,
                UnitPlanner::DebateResponse,
                &[
                    "finalize_debate_response",
                    "read_index_details",
                    "read_indexes",
                    "respond_to_debate_claim",
                ],
            ),
            registration(
                "researcher.bear.interaction",
                ToolManagedProfile::DebateResponse,
                UnitPlanner::DebateResponse,
                &[
                    "finalize_debate_response",
                    "read_index_details",
                    "read_indexes",
                    "respond_to_debate_claim",
                ],
            ),
            registration(
                "mediator.topic_controller",
                ToolManagedProfile::TopicControl,
                UnitPlanner::TopicControlRound,
                &[
                    "add_agreed_fact",
                    "finalize_topic_control",
                    "read_index_details",
                    "read_indexes",
                    "route_debate_steer",
                    "set_claim_status",
                    "set_decision_hinge",
                    "set_topic_soft_control",
                ],
            ),
            registration(
                "manager.research",
                ToolManagedProfile::ResearchDecision,
                UnitPlanner::ResearchTicker,
                &[
                    "append_research_hinge",
                    "finalize_research_decision",
                    "read_index_details",
                    "read_indexes",
                    "set_research_decision",
                    "set_research_scenarios",
                ],
            ),
            registration(
                "trader",
                ToolManagedProfile::TradeIntent,
                UnitPlanner::TradeTicker,
                &[
                    "append_trade_blocker",
                    "finalize_trade_intent",
                    "read_index_details",
                    "read_indexes",
                    "set_trade_intent",
                ],
            ),
            registration(
                "risk.aggressive",
                ToolManagedProfile::RiskReview,
                UnitPlanner::RiskTicker,
                &[
                    "finalize_risk_review",
                    "read_index_details",
                    "read_indexes",
                    "set_risk_assessment",
                    "set_risk_constraints",
                ],
            ),
            registration(
                "risk.neutral",
                ToolManagedProfile::RiskReview,
                UnitPlanner::RiskTicker,
                &[
                    "finalize_risk_review",
                    "read_index_details",
                    "read_indexes",
                    "set_risk_assessment",
                    "set_risk_constraints",
                ],
            ),
            registration(
                "risk.conservative",
                ToolManagedProfile::RiskReview,
                UnitPlanner::RiskTicker,
                &[
                    "finalize_risk_review",
                    "read_index_details",
                    "read_indexes",
                    "set_risk_assessment",
                    "set_risk_constraints",
                ],
            ),
            registration(
                "portfolio.manager",
                ToolManagedProfile::PortfolioDecision,
                UnitPlanner::PortfolioAsset,
                &[
                    "append_binding_risk_control",
                    "finalize_portfolio_decision",
                    "read_index_details",
                    "read_indexes",
                    "set_portfolio_asset_decision",
                ],
            ),
            registration(
                "compressor.phase_summary",
                ToolManagedProfile::PhaseSummary,
                UnitPlanner::PhaseSummaryUnit,
                &["append_index_detail", "create_index", "finalize_index"],
            ),
        ];
        Self::from_registrations(BUILTIN_ROLE_PROFILE_REGISTRY_VERSION, registrations)
            .expect("builtin authority registry must be valid")
    }

    pub fn from_snapshot(
        snapshot: RoleProfileRegistrySnapshot,
    ) -> Result<Self, RoleProfileRegistryError> {
        snapshot.verify()?;
        Self::from_registrations(snapshot.registry_version, snapshot.registrations)
    }

    pub fn from_registrations(
        registry_version: u32,
        registrations: impl IntoIterator<Item = RoleProfileRegistration>,
    ) -> Result<Self, RoleProfileRegistryError> {
        let mut registry = Self::new(registry_version)?;
        for registration in registrations {
            registry.register(registration)?;
        }
        Ok(registry)
    }

    pub fn registry_version(&self) -> u32 {
        self.registry_version
    }

    pub fn register(
        &mut self,
        registration: RoleProfileRegistration,
    ) -> Result<(), RoleProfileRegistryError> {
        registration.validate()?;
        let key = registration.key();
        if self.registrations.contains_key(&key) {
            return Err(RoleProfileRegistryError::DuplicateRegistration {
                role_id: key.role_id,
                profile: key.profile,
            });
        }
        self.registrations.insert(key, registration);
        Ok(())
    }

    pub fn registration(
        &self,
        role_id: &str,
        profile: ToolManagedProfile,
    ) -> Result<&RoleProfileRegistration, RoleProfileRegistryError> {
        self.registrations
            .get(&RoleProfileKey::new(role_id, profile))
            .ok_or_else(|| RoleProfileRegistryError::MissingRegistration {
                role_id: role_id.to_string(),
                profile,
            })
    }

    pub fn tool_is_allowed(
        &self,
        role_id: &str,
        profile: ToolManagedProfile,
        tool_name: &str,
    ) -> Result<bool, RoleProfileRegistryError> {
        Ok(self.registration(role_id, profile)?.allows_tool(tool_name))
    }

    pub fn registrations(&self) -> impl Iterator<Item = &RoleProfileRegistration> {
        self.registrations.values()
    }

    pub fn validate(&self) -> Result<(), RoleProfileRegistryError> {
        if self.registry_version == 0 {
            return Err(RoleProfileRegistryError::ZeroRegistryVersion);
        }
        for registration in self.registrations.values() {
            registration.validate()?;
        }
        Ok(())
    }

    pub fn snapshot(&self) -> RoleProfileRegistrySnapshot {
        self.validate()
            .expect("authority registry must be valid before snapshotting");
        let registrations = self.registrations.values().cloned().collect::<Vec<_>>();
        let content_hash = snapshot_content_hash(self.registry_version, &registrations);
        RoleProfileRegistrySnapshot {
            schema_version: ROLE_PROFILE_REGISTRY_SCHEMA_VERSION,
            registry_version: self.registry_version,
            registrations,
            content_hash,
        }
    }
}

fn registration(
    role_id: &str,
    profile: ToolManagedProfile,
    unit_planner: UnitPlanner,
    tools: &[&str],
) -> RoleProfileRegistration {
    RoleProfileRegistration::new(role_id, profile, 1, 1, unit_planner, tools.iter().copied())
        .expect("builtin authority registration must be valid")
}

fn validate_role_id(role_id: &str) -> Result<(), RoleProfileRegistryError> {
    if role_id.is_empty() || role_id != role_id.trim() {
        return Err(RoleProfileRegistryError::InvalidRoleId {
            role_id: role_id.to_string(),
        });
    }
    if role_id.starts_with('.') || role_id.ends_with('.') || role_id.split('.').any(str::is_empty) {
        return Err(RoleProfileRegistryError::InvalidRoleId {
            role_id: role_id.to_string(),
        });
    }
    if !role_id.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    }) {
        return Err(RoleProfileRegistryError::InvalidRoleId {
            role_id: role_id.to_string(),
        });
    }
    Ok(())
}

fn validate_tool_name(
    tool_name: &str,
    role_id: &str,
    profile: ToolManagedProfile,
) -> Result<(), RoleProfileRegistryError> {
    if tool_name.is_empty()
        || !tool_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(RoleProfileRegistryError::InvalidToolName {
            role_id: role_id.to_string(),
            profile,
            tool_name: tool_name.to_string(),
        });
    }
    Ok(())
}

fn snapshot_content_hash(
    registry_version: u32,
    registrations: &[RoleProfileRegistration],
) -> String {
    #[derive(Serialize)]
    struct SnapshotPayload<'a> {
        schema_version: u32,
        registry_version: u32,
        registrations: &'a [RoleProfileRegistration],
    }

    let bytes = serde_json::to_vec(&SnapshotPayload {
        schema_version: ROLE_PROFILE_REGISTRY_SCHEMA_VERSION,
        registry_version,
        registrations,
    })
    .expect("authority registry snapshot must serialize");
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RoleProfileRegistryError {
    #[error("authority registry version must be positive")]
    ZeroRegistryVersion,
    #[error("invalid authority role id {role_id:?}")]
    InvalidRoleId { role_id: String },
    #[error("role {role_id:?} profile {profile:?} has profile_version 0")]
    ZeroProfileVersion {
        role_id: String,
        profile: ToolManagedProfile,
    },
    #[error("role {role_id:?} profile {profile:?} has builder_version 0")]
    ZeroBuilderVersion {
        role_id: String,
        profile: ToolManagedProfile,
    },
    #[error("role {role_id:?} profile {profile:?} has no allowed tools")]
    EmptyToolAllowlist {
        role_id: String,
        profile: ToolManagedProfile,
    },
    #[error("role {role_id:?} profile {profile:?} has invalid tool name {tool_name:?}")]
    InvalidToolName {
        role_id: String,
        profile: ToolManagedProfile,
        tool_name: String,
    },
    #[error("role {role_id:?} profile {profile:?} repeats tool {tool:?}")]
    DuplicateTool {
        role_id: String,
        profile: ToolManagedProfile,
        tool: String,
    },
    #[error(
        "role {role_id:?} profile {profile:?} tool allowlist must be sorted and duplicate-free"
    )]
    NonCanonicalToolAllowlist {
        role_id: String,
        profile: ToolManagedProfile,
    },
    #[error("duplicate authority registration for role {role_id:?} profile {profile:?}")]
    DuplicateRegistration {
        role_id: String,
        profile: ToolManagedProfile,
    },
    #[error("missing authority registration for role {role_id:?} profile {profile:?}")]
    MissingRegistration {
        role_id: String,
        profile: ToolManagedProfile,
    },
    #[error("unsupported authority snapshot schema {found}; this binary supports {supported}")]
    UnsupportedSnapshotSchema { found: u32, supported: u32 },
    #[error("authority snapshot hash mismatch: expected {expected}, found {found}")]
    SnapshotHashMismatch { expected: String, found: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_registry_covers_each_current_agent_role_profile_exactly_once() {
        let registry = RoleProfileRegistry::builtin();
        assert_eq!(registry.registrations().count(), 17);
        assert!(registry
            .registration("researcher.bull.initial", ToolManagedProfile::DebateSeed)
            .is_ok());
        assert!(registry
            .registration(
                "researcher.bull.initial",
                ToolManagedProfile::DebateResponse
            )
            .is_err());
    }

    #[test]
    fn registry_rejects_duplicate_exact_mapping() {
        let registration = RoleProfileRegistration::new(
            "analyst.test",
            ToolManagedProfile::AnalystReport,
            1,
            1,
            UnitPlanner::AnalystTicker,
            ["finalize_analyst_report"],
        )
        .unwrap();
        let err = RoleProfileRegistry::from_registrations(1, [registration.clone(), registration])
            .unwrap_err();
        assert!(matches!(
            err,
            RoleProfileRegistryError::DuplicateRegistration { .. }
        ));
    }

    #[test]
    fn registration_rejects_duplicate_or_invalid_tools() {
        let err = RoleProfileRegistration::new(
            "analyst.test",
            ToolManagedProfile::AnalystReport,
            1,
            1,
            UnitPlanner::AnalystTicker,
            ["set_analyst_assessment", "set_analyst_assessment"],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            RoleProfileRegistryError::DuplicateTool { .. }
        ));

        let err = RoleProfileRegistration::new(
            "analyst.test",
            ToolManagedProfile::AnalystReport,
            1,
            1,
            UnitPlanner::AnalystTicker,
            ["set analyst assessment"],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            RoleProfileRegistryError::InvalidToolName { .. }
        ));
    }

    #[test]
    fn direct_deserialization_must_keep_tool_allowlist_canonical() {
        let registration = RoleProfileRegistration {
            role_id: "analyst.test".to_string(),
            profile: ToolManagedProfile::AnalystReport,
            profile_version: 1,
            builder_version: 1,
            unit_planner: UnitPlanner::AnalystTicker,
            tool_allowlist: vec![
                "set_analyst_assessment".to_string(),
                "append_analyst_evidence".to_string(),
            ],
        };
        let err = registration.validate().unwrap_err();
        assert!(matches!(
            err,
            RoleProfileRegistryError::NonCanonicalToolAllowlist { .. }
        ));
    }

    #[test]
    fn snapshot_is_stable_and_verifies_after_round_trip() {
        let registry = RoleProfileRegistry::builtin();
        let first = registry.snapshot();
        let encoded = serde_json::to_string(&first).unwrap();
        let decoded: RoleProfileRegistrySnapshot = serde_json::from_str(&encoded).unwrap();
        decoded.verify().unwrap();
        assert_eq!(first, registry.snapshot());
        assert_eq!(
            RoleProfileRegistry::from_snapshot(decoded)
                .unwrap()
                .snapshot(),
            first
        );
    }

    #[test]
    fn snapshot_hash_detects_authority_drift() {
        let mut snapshot = RoleProfileRegistry::builtin().snapshot();
        snapshot.registrations[0].profile_version = 2;
        let err = snapshot.verify().unwrap_err();
        assert!(matches!(
            err,
            RoleProfileRegistryError::SnapshotHashMismatch { .. }
        ));
    }

    #[test]
    fn tool_permission_is_exact() {
        let registry = RoleProfileRegistry::builtin();
        assert!(registry
            .tool_is_allowed(
                "trader",
                ToolManagedProfile::TradeIntent,
                "set_trade_intent",
            )
            .unwrap());
        assert!(!registry
            .tool_is_allowed(
                "trader",
                ToolManagedProfile::TradeIntent,
                "set_risk_constraints",
            )
            .unwrap());
    }
}
