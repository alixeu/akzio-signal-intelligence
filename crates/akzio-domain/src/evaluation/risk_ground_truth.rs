// 文件导读：定义独立风险真值评估及 reviewer/verifier 身份、来源闭包和密封时间校验。
// 该真值不能从 Decision 自身推导，只有独立来源并且仍在有效期内才能参与 Outcome。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskAssessmentAuthority {
    pub identity: String,
    pub version: String,
}

impl RiskAssessmentAuthority {
    // reviewer/verifier 的身份和版本必须非空。
    fn validate(&self, field: &'static str) -> Result<(), DomainError> {
        if self.identity.trim().is_empty() || self.version.trim().is_empty() {
            return Err(DomainError::EmptyField { field });
        }
        Ok(())
    }
}

/// Independently reviewed risk ground truth for one realized outcome horizon.
///
/// Expected risks come from `basis_refs`, not from the evaluated Decision.
/// Detected risks are the independently audited subset found in that Decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskGroundTruthAssessment {
    pub schema_version: u32,
    pub schedule: ArtifactRef,
    pub decision: ArtifactRef,
    pub horizon: OutcomeHorizon,
    pub observed_trading_day: NaiveDate,
    pub expected_risk_ids: std::collections::BTreeSet<String>,
    pub detected_risk_ids: std::collections::BTreeSet<String>,
    pub basis_refs: Vec<ArtifactRef>,
    pub decision_producer_identity: String,
    pub reviewer: RiskAssessmentAuthority,
    pub verifier: RiskAssessmentAuthority,
    pub assessed_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub sealed_at: Option<DateTime<Utc>>,
}

impl RiskGroundTruthAssessment {
    // 校验引用 kind、评估独立性、basis 排序/类型、风险集合关系和有效时间窗口。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION {
            return Err(DomainError::EmptyField {
                field: "risk_ground_truth.schema_version",
            });
        }
        if self.schedule.kind != ArtifactKind::OutcomeSchedule
            || self.decision.kind != ArtifactKind::Decision
        {
            return Err(DomainError::EmptyField {
                field: "risk_ground_truth.references",
            });
        }
        if self.decision_producer_identity.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "risk_ground_truth.decision_producer_identity",
            });
        }
        self.reviewer.validate("risk_ground_truth.reviewer")?;
        self.verifier.validate("risk_ground_truth.verifier")?;
        if identities_match(&self.reviewer.identity, &self.verifier.identity)
            || identities_match(
                &self.reviewer.identity,
                &self.decision_producer_identity,
            )
            || identities_match(
                &self.verifier.identity,
                &self.decision_producer_identity,
            )
        {
            return Err(DomainError::EmptyField {
                field: "risk_ground_truth.independence",
            });
        }
        if self.basis_refs.is_empty()
            || self.basis_refs.windows(2).any(|pair| pair[0] >= pair[1])
            || self.basis_refs.iter().any(|reference| {
                !matches!(
                    reference.kind,
                    ArtifactKind::RawEvidence
                        | ArtifactKind::NormalizedEvidence
                        | ArtifactKind::SemanticDetail
                ) || reference.artifact_id == self.schedule.artifact_id
                    || reference.artifact_id == self.decision.artifact_id
            })
        {
            return Err(DomainError::EmptyField {
                field: "risk_ground_truth.basis_refs",
            });
        }
        if self
            .expected_risk_ids
            .is_empty()
            || self
                .expected_risk_ids
            .iter()
            .chain(self.detected_risk_ids.iter())
            .any(|risk_id| risk_id.trim().is_empty())
            || !self.detected_risk_ids.is_subset(&self.expected_risk_ids)
        {
            return Err(DomainError::InvalidBudget {
                field: "risk_ground_truth.risk_ids",
            });
        }
        if self.assessed_at > self.valid_until {
            return Err(DomainError::InvalidBudget {
                field: "risk_ground_truth.validity",
            });
        }
        if let Some(sealed_at) = self.sealed_at {
            if sealed_at < self.assessed_at
                || sealed_at > self.valid_until
                || sealed_at.date_naive() < self.observed_trading_day
            {
                return Err(DomainError::InvalidBudget {
                    field: "risk_ground_truth.sealed_at",
                });
            }
        }
        Ok(())
    }

    // 先做普通校验，再确认已经 sealed，且 used_at 位于封存后和有效期内。
    pub fn validate_sealed_at(&self, used_at: DateTime<Utc>) -> Result<(), DomainError> {
        self.validate()?;
        let Some(sealed_at) = self.sealed_at else {
            return Err(DomainError::EmptyField {
                field: "risk_ground_truth.sealed_at",
            });
        };
        if sealed_at > used_at || used_at > self.valid_until {
            return Err(DomainError::InvalidBudget {
                field: "risk_ground_truth.stale",
            });
        }
        Ok(())
    }

    // 返回独立评估列出的期望风险数量。
    pub fn expected_count(&self) -> u64 {
        self.expected_risk_ids.len() as u64
    }

    // 返回独立评估实际检测到的风险数量。
    pub fn detected_count(&self) -> u64 {
        self.detected_risk_ids.len() as u64
    }
}

fn identities_match(left: &str, right: &str) -> bool {
    // 忽略首尾空白和大小写比较身份，避免同一主体通过格式差异绕过独立性检查。
    left.trim().eq_ignore_ascii_case(right.trim())
}
