// 文件导读：保存跨 crate 共用的领域 schema 版本和因子风险上限结构。
//! Canonical artifact, contract, workflow, and authority schema.
//!
//! This module is intentionally introduced beside the former vocabulary while the
//! workspace is migrated. Its types are the only types new runtime code may use;
//! the old document/role/task types are removed once every crate crosses this seam.

use serde::{Deserialize, Serialize};

use crate::DomainError;

pub const SCHEMA_VERSION: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorLimits {
    pub global_leveraged_equity_ppm: u32,
    pub nasdaq_ppm: u32,
    pub semiconductor_ppm: u32,
    pub paired_index_ppm: u32,
}

impl FactorLimits {
    // 校验四类因子上限都不超过 100%（以 ppm 表示）；失败只返回领域错误，不做截断。
    pub fn validate(&self) -> Result<(), DomainError> {
        // 数组迭代器配合 any 闭包集中检查“任一值越界”的条件。
        if [
            self.global_leveraged_equity_ppm,
            self.nasdaq_ppm,
            self.semiconductor_ppm,
            self.paired_index_ppm,
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "factor_limits",
            });
        }
        Ok(())
    }
}
