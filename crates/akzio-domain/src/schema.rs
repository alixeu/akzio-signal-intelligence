// 文件导读：保存跨 crate 共用的领域 schema 版本和因子风险上限结构。
// `FactorLimits::validate` 只读借用自身；任一 ppm 越界就以领域错误提前返回，不会自动裁剪输入值。
//! Canonical artifact, contract, workflow, and authority schema.
//!
//! `SCHEMA_VERSION` is the shared domain identity; `FactorLimits` carries
//! integer ppm bounds checked by its domain `validate` method.

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
        // 数组 `into_iter` 消费这个临时数组；`any` 的闭包逐值检查 u32，并在首个越界项短路。
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
