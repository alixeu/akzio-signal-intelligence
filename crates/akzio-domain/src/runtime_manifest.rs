use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    content_hash_json, ArtifactKind, ArtifactRef, ContentHash, DomainError, MoneyMicros,
    V2_DOMAIN_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaperApprovalScope {
    Canary,
    Scheduled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeManifest {
    pub schema_version: u32,
    pub code_revision: String,
    pub cargo_lock_hash: ContentHash,
    pub config_hash: ContentHash,
    pub provider_id: String,
    pub model_id: String,
    pub prompt_hash: ContentHash,
    pub contract_hash: ContentHash,
    pub topology_hash: ContentHash,
    pub decision_policy_hash: ContentHash,
    pub execution_policy_hash: ContentHash,
    pub evaluation_policy_hash: ContentHash,
    pub market_data_feed: String,
    pub broker_account_id: String,
    pub maximum_notional: MoneyMicros,
    pub allowed_session_start: NaiveDate,
    pub allowed_session_end: NaiveDate,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl RuntimeManifest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.code_revision.trim().is_empty()
            || self.provider_id.trim().is_empty()
            || self.model_id.trim().is_empty()
            || self.broker_account_id.trim().is_empty()
            || !matches!(self.market_data_feed.as_str(), "iex" | "sip")
            || self.maximum_notional.0 <= 0
            || self.allowed_session_end < self.allowed_session_start
            || self.expires_at <= self.created_at
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }

    pub fn manifest_hash(&self) -> Result<ContentHash, DomainError> {
        self.validate()?;
        content_hash_json(&serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?)
            .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn runtime_identity_hash(&self) -> Result<ContentHash, DomainError> {
        self.validate()?;
        content_hash_json(&serde_json::json!({
            "code_revision": self.code_revision,
            "cargo_lock_hash": self.cargo_lock_hash,
            "config_hash": self.config_hash,
            "provider_id": self.provider_id,
            "model_id": self.model_id,
            "prompt_hash": self.prompt_hash,
            "contract_hash": self.contract_hash,
            "topology_hash": self.topology_hash,
            "decision_policy_hash": self.decision_policy_hash,
            "execution_policy_hash": self.execution_policy_hash,
            "evaluation_policy_hash": self.evaluation_policy_hash,
            "market_data_feed": self.market_data_feed,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn permits(&self, session: NaiveDate, now: DateTime<Utc>) -> bool {
        self.validate().is_ok()
            && session >= self.allowed_session_start
            && session <= self.allowed_session_end
            && now <= self.expires_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperLaunchApproval {
    pub schema_version: u32,
    pub operator_identity: String,
    pub runtime_manifest: ArtifactRef,
    pub runtime_manifest_hash: ContentHash,
    pub scope: PaperApprovalScope,
    pub reason: String,
    pub approved_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approval_hash: ContentHash,
}

impl PaperLaunchApproval {
    pub fn unsigned_hash(&self) -> Result<ContentHash, DomainError> {
        content_hash_json(&serde_json::json!({
            "schema_version": self.schema_version,
            "operator_identity": self.operator_identity,
            "runtime_manifest": self.runtime_manifest,
            "runtime_manifest_hash": self.runtime_manifest_hash,
            "scope": self.scope,
            "reason": self.reason,
            "approved_at": self.approved_at,
            "expires_at": self.expires_at,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.operator_identity.trim().is_empty()
            || self.reason.trim().is_empty()
            || self.runtime_manifest.kind != ArtifactKind::RuntimeManifest
            || self.expires_at <= self.approved_at
            || self.unsigned_hash()? != self.approval_hash
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArtifactId;

    #[test]
    fn approval_hash_binds_every_authorized_field() {
        let now = Utc::now();
        let mut approval = PaperLaunchApproval {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            operator_identity: "operator@example.com".to_owned(),
            runtime_manifest: ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(b"manifest-artifact")),
                kind: ArtifactKind::RuntimeManifest,
            },
            runtime_manifest_hash: ContentHash::of_bytes(b"manifest-payload"),
            scope: PaperApprovalScope::Canary,
            reason: "one-session Paper canary".to_owned(),
            approved_at: now,
            expires_at: now + chrono::Duration::hours(8),
            approval_hash: ContentHash::of_bytes(b"placeholder"),
        };
        approval.approval_hash = approval.unsigned_hash().unwrap();
        approval.validate().unwrap();
        approval.reason.push_str(" changed");
        assert!(approval.validate().is_err());
    }
}
