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
pub struct RuntimeIdentity {
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
}

impl RuntimeIdentity {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.code_revision.trim().is_empty()
            || self.provider_id.trim().is_empty()
            || self.model_id.trim().is_empty()
            || !matches!(self.market_data_feed.as_str(), "iex" | "sip")
        {
            return Err(DomainError::InvalidContentHash);
        }
        Ok(())
    }

    pub fn identity_hash(&self) -> Result<ContentHash, DomainError> {
        self.validate()?;
        content_hash_json(&serde_json::to_value(self).map_err(|_| DomainError::InvalidContentHash)?)
            .map_err(|_| DomainError::InvalidContentHash)
    }
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
        self.runtime_identity().validate()?;
        if self.schema_version != V2_DOMAIN_SCHEMA_VERSION
            || self.broker_account_id.trim().is_empty()
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
        self.runtime_identity().identity_hash()
    }

    pub fn runtime_identity(&self) -> RuntimeIdentity {
        RuntimeIdentity {
            code_revision: self.code_revision.clone(),
            cargo_lock_hash: self.cargo_lock_hash.clone(),
            config_hash: self.config_hash.clone(),
            provider_id: self.provider_id.clone(),
            model_id: self.model_id.clone(),
            prompt_hash: self.prompt_hash.clone(),
            contract_hash: self.contract_hash.clone(),
            topology_hash: self.topology_hash.clone(),
            decision_policy_hash: self.decision_policy_hash.clone(),
            execution_policy_hash: self.execution_policy_hash.clone(),
            evaluation_policy_hash: self.evaluation_policy_hash.clone(),
            market_data_feed: self.market_data_feed.clone(),
        }
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

    fn hash(seed: &str) -> ContentHash {
        ContentHash::of_bytes(seed.as_bytes())
    }

    fn runtime_identity() -> RuntimeIdentity {
        RuntimeIdentity {
            code_revision: "revision-a".to_owned(),
            cargo_lock_hash: hash("cargo-lock-a"),
            config_hash: hash("config-a"),
            provider_id: "provider.example".to_owned(),
            model_id: "model-a".to_owned(),
            prompt_hash: hash("prompt-a"),
            contract_hash: hash("contract-a"),
            topology_hash: hash("topology-a"),
            decision_policy_hash: hash("decision-policy-a"),
            execution_policy_hash: hash("execution-policy-a"),
            evaluation_policy_hash: hash("evaluation-policy-a"),
            market_data_feed: "iex".to_owned(),
        }
    }

    fn runtime_manifest(now: DateTime<Utc>) -> RuntimeManifest {
        let identity = runtime_identity();
        RuntimeManifest {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            code_revision: identity.code_revision,
            cargo_lock_hash: identity.cargo_lock_hash,
            config_hash: identity.config_hash,
            provider_id: identity.provider_id,
            model_id: identity.model_id,
            prompt_hash: identity.prompt_hash,
            contract_hash: identity.contract_hash,
            topology_hash: identity.topology_hash,
            decision_policy_hash: identity.decision_policy_hash,
            execution_policy_hash: identity.execution_policy_hash,
            evaluation_policy_hash: identity.evaluation_policy_hash,
            market_data_feed: identity.market_data_feed,
            broker_account_id: "paper-account-a".to_owned(),
            maximum_notional: MoneyMicros::from_usd_cents(10_000),
            allowed_session_start: now.date_naive(),
            allowed_session_end: now.date_naive(),
            expires_at: now + chrono::Duration::hours(8),
            created_at: now,
        }
    }

    #[test]
    fn runtime_identity_hash_binds_exactly_the_identity_fields() {
        let base = runtime_identity();
        let base_hash = base.identity_hash().unwrap();
        let variants = [
            RuntimeIdentity {
                code_revision: "revision-b".to_owned(),
                ..base.clone()
            },
            RuntimeIdentity {
                cargo_lock_hash: hash("cargo-lock-b"),
                ..base.clone()
            },
            RuntimeIdentity {
                config_hash: hash("config-b"),
                ..base.clone()
            },
            RuntimeIdentity {
                provider_id: "provider-b.example".to_owned(),
                ..base.clone()
            },
            RuntimeIdentity {
                model_id: "model-b".to_owned(),
                ..base.clone()
            },
            RuntimeIdentity {
                prompt_hash: hash("prompt-b"),
                ..base.clone()
            },
            RuntimeIdentity {
                contract_hash: hash("contract-b"),
                ..base.clone()
            },
            RuntimeIdentity {
                topology_hash: hash("topology-b"),
                ..base.clone()
            },
            RuntimeIdentity {
                decision_policy_hash: hash("decision-policy-b"),
                ..base.clone()
            },
            RuntimeIdentity {
                execution_policy_hash: hash("execution-policy-b"),
                ..base.clone()
            },
            RuntimeIdentity {
                evaluation_policy_hash: hash("evaluation-policy-b"),
                ..base.clone()
            },
            RuntimeIdentity {
                market_data_feed: "sip".to_owned(),
                ..base.clone()
            },
        ];

        for variant in variants {
            assert_ne!(variant.identity_hash().unwrap(), base_hash);
        }
    }

    #[test]
    fn runtime_manifest_keeps_flat_json_and_excludes_approval_binding_from_identity() {
        let now = Utc::now();
        let first = runtime_manifest(now);
        let mut second = first.clone();
        second.broker_account_id = "paper-account-b".to_owned();
        second.maximum_notional = MoneyMicros::from_usd_cents(20_000);
        second.expires_at = now + chrono::Duration::hours(12);

        assert_eq!(
            first.runtime_identity_hash().unwrap(),
            second.runtime_identity_hash().unwrap()
        );
        assert_ne!(
            first.manifest_hash().unwrap(),
            second.manifest_hash().unwrap()
        );
        let value = serde_json::to_value(first).unwrap();
        assert!(value.get("identity").is_none());
        assert!(value.get("code_revision").is_some());
        assert!(value.get("broker_account_id").is_some());
    }

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
