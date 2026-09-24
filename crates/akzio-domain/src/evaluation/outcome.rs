// 文件导读：定义 OutcomeSchedule、T+1/T+3/T+5 窗口、校准/基准指标、成本归因和
// 叙事复盘产物；量化结果由 Rust 校验，模型只提交 Retrospective 叙事草稿。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeHorizon {
    T1,
    T3,
    T5,
}

impl OutcomeHorizon {
    pub const ALL: [Self; 3] = [Self::T1, Self::T3, Self::T5];

    // 把 horizon 映射为 baseline 之后需要完成的交易 session 数。
    pub const fn trading_days(self) -> u8 {
        match self {
            Self::T1 => 1,
            Self::T3 => 3,
            Self::T5 => 5,
        }
    }

    /// Due means completed trading sessions after the baseline session, never
    /// elapsed wall-clock days.
    // 用已完成的实际交易 session 数判断到期，不使用墙钟天数。
    pub const fn is_due_after(self, completed_trading_sessions: u8) -> bool {
        completed_trading_sessions >= self.trading_days()
    }
}

/// Rust-owned execution lineage for a future Paper outcome.
///
/// A rejected decision has a durable `NoOrder` verdict and no broker
/// reconciliation. An accepted decision must retain both the commitment and
/// its reconciliation; an unreconciled commitment cannot be scheduled for
/// canonical learning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutcomeExecutionLineage {
    NoOrder {
        execution_verdict: ArtifactRef,
    },
    ReconciledPaper {
        execution_verdict: ArtifactRef,
        commitment: ArtifactRef,
        reconciliation: ArtifactRef,
    },
}

impl OutcomeExecutionLineage {
    // NoOrder 只需 ExecutionVerdict；真正 reconciled Paper 必须同时保留 commitment/reconciliation。
    pub fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::NoOrder { execution_verdict } => {
                if execution_verdict.kind != ArtifactKind::ExecutionVerdict {
                    return Err(DomainError::EmptyField {
                        field: "outcome_schedule.execution_verdict",
                    });
                }
            }
            Self::ReconciledPaper {
                execution_verdict,
                commitment,
                reconciliation,
            } => {
                if execution_verdict.kind != ArtifactKind::ExecutionVerdict
                    || commitment.kind != ArtifactKind::ExecutionCommitment
                    || reconciliation.kind != ArtifactKind::Reconciliation
                {
                    return Err(DomainError::EmptyField {
                        field: "outcome_schedule.reconciled_lineage",
                    });
                }
            }
        }
        Ok(())
    }
}

/// Durable intent to materialize T+1, T+3, and T+5 observations.
///
/// Store validation later proves that these references form one source
/// closure. The schedule fixes the immutable lineage and leaves market-clock
/// acquisition to the daemon-owned materializer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeSchedule {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub decision: ArtifactRef,
    pub decision_context: ArtifactRef,
    pub execution_context: ArtifactRef,
    pub execution: OutcomeExecutionLineage,
    pub baseline_trading_day: NaiveDate,
    pub created_at: DateTime<Utc>,
}

impl OutcomeSchedule {
    // 校验 schedule 身份和三类核心引用，再复用 execution lineage 的 kind 约束。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.outcome_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "outcome_schedule.identity",
            });
        }
        if self.decision.kind != ArtifactKind::Decision
            || self.decision_context.kind != ArtifactKind::DecisionContext
            || self.execution_context.kind != ArtifactKind::ExecutionContext
        {
            return Err(DomainError::EmptyField {
                field: "outcome_schedule.references",
            });
        }
        self.execution.validate()
    }

    // 返回截至指定交易 session 数已经到期的 horizon，保持 T1/T3/T5 固定顺序。
    pub fn due_horizons(&self, completed_trading_sessions: u8) -> Vec<OutcomeHorizon> {
        OutcomeHorizon::ALL
            .into_iter()
            .filter(|horizon| horizon.is_due_after(completed_trading_sessions))
            .collect()
    }
}

pub const FORECAST_CALIBRATION_BIN_COUNT: usize = 10;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastCalibrationBin {
    pub sample_count: u32,
    pub probability_sum_ppm: u64,
    pub positive_count: u32,
    pub brier_sum_ppm: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForecastScore {
    pub sample_count: u32,
    pub mean_brier_ppm: u32,
    pub bins: [ForecastCalibrationBin; FORECAST_CALIBRATION_BIN_COUNT],
}

impl ForecastScore {
    // 用 checked sum 汇总十个 bin 样本，并检查概率/Brier/正例计数的边界。
    pub fn validate(&self) -> Result<(), DomainError> {
        let sample_count = self
            .bins
            .iter()
            .try_fold(0_u64, |total, bin| {
                total.checked_add(u64::from(bin.sample_count))
            })
            .ok_or(DomainError::InvalidBudget {
                field: "forecast_score.samples",
            })?;
        if self.sample_count == 0
            || sample_count != u64::from(self.sample_count)
            || self.mean_brier_ppm > 1_000_000
            || self.bins.iter().any(|bin| {
                bin.positive_count > bin.sample_count
                    || bin.probability_sum_ppm
                        > u64::from(bin.sample_count).saturating_mul(1_000_000)
                    || bin.brier_sum_ppm > u64::from(bin.sample_count).saturating_mul(1_000_000)
            })
        {
            return Err(DomainError::InvalidBudget {
                field: "forecast_score",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationReport {
    pub sample_count: u64,
    pub mean_brier_ppm: u32,
    pub expected_calibration_error_ppm: u32,
}

impl CalibrationReport {
    // 校验样本非零，Brier 和校准误差均在 ppm 范围内。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.sample_count == 0
            || self.mean_brier_ppm > 1_000_000
            || self.expected_calibration_error_ppm > 1_000_000
        {
            return Err(DomainError::InvalidBudget {
                field: "calibration_report",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountedRatio {
    pub expected_count: u64,
    pub observed_count: u64,
    pub ratio_ppm: u32,
    #[serde(default)]
    pub lower_confidence_ppm: Option<u32>,
}

impl CountedRatio {
    // 校验分母、观测数、比例及下置信界之间的数量关系。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.expected_count == 0
            || self.observed_count > self.expected_count
            || self.ratio_ppm > 1_000_000
            || self
                .lower_confidence_ppm
                .is_some_and(|value| value > self.ratio_ppm)
        {
            return Err(DomainError::InvalidBudget {
                field: "counted_ratio",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeNavPoint {
    pub observed_trading_day: NaiveDate,
    pub portfolio_nav_ppm: i64,
    pub benchmark_nav_ppm: i64,
    pub portfolio_daily_return_ppm: i64,
    pub benchmark_daily_return_ppm: i64,
}

/// Version of the deterministic benchmark definitions sealed into outcomes.
/// Changing any definition requires a new version; historical attributions
/// retain their original version and content hash.
pub const OUTCOME_BENCHMARK_DEFINITION_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeBenchmark {
    Cash,
    Qqq,
    Soxx,
    FourAssetEqualWeight,
    BetaMatchedQqq,
    VolatilityTargetedQqq,
    NoLlmDeterministic,
}

impl OutcomeBenchmark {
    pub const ALL: [Self; 7] = [
        Self::Cash,
        Self::Qqq,
        Self::Soxx,
        Self::FourAssetEqualWeight,
        Self::BetaMatchedQqq,
        Self::VolatilityTargetedQqq,
        Self::NoLlmDeterministic,
    ];

    // 返回 benchmark 定义的稳定名称。
    pub const fn name(self) -> &'static str {
        match self {
            Self::Cash => "cash",
            Self::Qqq => "qqq",
            Self::Soxx => "soxx",
            Self::FourAssetEqualWeight => "four_asset_equal_weight",
            Self::BetaMatchedQqq => "beta_matched_qqq",
            Self::VolatilityTargetedQqq => "volatility_targeted_qqq",
            Self::NoLlmDeterministic => "no_llm_deterministic",
        }
    }

    // 派生 benchmark 所需的最小受治理样本数。
    pub const fn minimum_samples(self) -> u32 {
        match self {
            Self::BetaMatchedQqq | Self::VolatilityTargetedQqq => 5,
            _ => 1,
        }
    }

    // 将 benchmark 方法/权重/参考路径编码后计算其版本化定义哈希。
    pub fn definition_hash(self) -> Result<ContentHash, DomainError> {
        let definition = match self {
            Self::Cash => serde_json::json!({
                "method": "constant_cash_nav",
                "weights_ppm": {"TQQQ": 0, "QQQ": 0, "SOXX": 0, "SOXL": 0},
            }),
            Self::Qqq => serde_json::json!({
                "method": "static_buy_and_hold",
                "weights_ppm": {"TQQQ": 0, "QQQ": 1_000_000, "SOXX": 0, "SOXL": 0},
            }),
            Self::Soxx => serde_json::json!({
                "method": "static_buy_and_hold",
                "weights_ppm": {"TQQQ": 0, "QQQ": 0, "SOXX": 1_000_000, "SOXL": 0},
            }),
            Self::FourAssetEqualWeight => serde_json::json!({
                "method": "static_buy_and_hold",
                "weights_ppm": {"TQQQ": 250_000, "QQQ": 250_000, "SOXX": 250_000, "SOXL": 250_000},
            }),
            Self::BetaMatchedQqq => serde_json::json!({
                "method": "ols_beta_scaled_qqq_daily_path",
                "reference": "QQQ",
                "minimum_samples": self.minimum_samples(),
                "portfolio_path": "gross_governed_pit_nav",
            }),
            Self::VolatilityTargetedQqq => serde_json::json!({
                "method": "sample_volatility_scaled_qqq_daily_path",
                "reference": "QQQ",
                "minimum_samples": self.minimum_samples(),
                "portfolio_path": "gross_governed_pit_nav",
            }),
            Self::NoLlmDeterministic => serde_json::json!({
                "method": "static_buy_and_hold_no_llm_v1",
                "weights_ppm": {"TQQQ": 0, "QQQ": 500_000, "SOXX": 500_000, "SOXL": 0},
            }),
        };
        content_hash_json(&serde_json::json!({
            "schema": "akzio.outcome_benchmark_definition",
            "version": OUTCOME_BENCHMARK_DEFINITION_VERSION,
            "name": self.name(),
            "cost_assumption_ppm": 0,
            "definition": definition,
        }))
        .map_err(|_| DomainError::InvalidContentHash)
    }
}

/// Stable governance hash for the complete benchmark-definition bundle.
/// Runtime identity can include this without importing learning code.
pub fn outcome_benchmark_definition_bundle_hash() -> Result<ContentHash, DomainError> {
    // 枚举全部 benchmark，收集各自 definition_hash 后计算 bundle 级治理哈希。
    let definitions = OutcomeBenchmark::ALL
        .into_iter()
        .map(|benchmark| {
            Ok(serde_json::json!({
                "name": benchmark.name(),
                "definition_hash": benchmark.definition_hash()?,
            }))
        })
        .collect::<Result<Vec<_>, DomainError>>()?;
    content_hash_json(&serde_json::json!({
        "schema": "akzio.outcome_benchmark_definition_bundle",
        "version": OUTCOME_BENCHMARK_DEFINITION_VERSION,
        "definitions": definitions,
    }))
    .map_err(|_| DomainError::InvalidContentHash)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeBenchmarkUnavailableReason {
    InsufficientGovernedSamples,
    MissingObservedHorizonPath,
    DegenerateReferencePath,
    NonPositiveDerivedNav,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeBenchmarkNavPoint {
    pub observed_trading_day: NaiveDate,
    pub nav_ppm: i64,
    pub daily_return_ppm: i64,
}

impl OutcomeBenchmarkNavPoint {
    // 基准 NAV 必须为正，避免把不可定价路径当成有效收益。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.nav_ppm <= 0 {
            return Err(DomainError::InvalidBudget {
                field: "outcome_benchmark_nav_point",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OutcomeBenchmarkResult {
    Available {
        benchmark_return_ppm: i64,
        active_return_ppm: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scale_ppm: Option<i64>,
        nav_path: Vec<OutcomeBenchmarkNavPoint>,
    },
    Unavailable {
        reason: OutcomeBenchmarkUnavailableReason,
        observed_samples: u32,
        required_samples: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeBenchmarkAttribution {
    pub benchmark: OutcomeBenchmark,
    pub definition_version: u32,
    pub definition_hash: ContentHash,
    pub result: OutcomeBenchmarkResult,
}

impl OutcomeBenchmarkAttribution {
    // 校验版本化定义、可用路径长度/日期/缩放因子，及 benchmark/active return 算术一致性。
    fn validate(
        &self,
        portfolio_net_return_ppm: i64,
        window_required_samples: u32,
    ) -> Result<(), DomainError> {
        if self.definition_version != OUTCOME_BENCHMARK_DEFINITION_VERSION
            || self.definition_hash != self.benchmark.definition_hash()?
        {
            return Err(DomainError::InvalidContentHash);
        }
        match &self.result {
            OutcomeBenchmarkResult::Available {
                benchmark_return_ppm,
                active_return_ppm,
                scale_ppm,
                nav_path,
            } => {
                if nav_path.len() < window_required_samples as usize
                    || nav_path.is_empty()
                    || matches!(
                        self.benchmark,
                        OutcomeBenchmark::BetaMatchedQqq | OutcomeBenchmark::VolatilityTargetedQqq
                    ) != scale_ppm.is_some()
                    || scale_ppm.is_some_and(|scale| scale <= 0)
                {
                    return Err(DomainError::InvalidBudget {
                        field: "outcome_benchmark_attribution",
                    });
                }
                // 逐点检查 NAV 为正且观察日严格递增，防止重复或倒序路径。
                let mut previous_day = None;
                for point in nav_path {
                    point.validate()?;
                    if previous_day.is_some_and(|day| day >= point.observed_trading_day) {
                        return Err(DomainError::InvalidBudget {
                            field: "outcome_benchmark_attribution.nav_path",
                        });
                    }
                    previous_day = Some(point.observed_trading_day);
                }
                let final_nav = nav_path
                    .last()
                    .expect("non-empty benchmark path validated")
                    .nav_ppm;
                if final_nav.checked_sub(1_000_000) != Some(*benchmark_return_ppm)
                    || portfolio_net_return_ppm.checked_sub(*benchmark_return_ppm)
                        != Some(*active_return_ppm)
                {
                    return Err(DomainError::InvalidBudget {
                        field: "outcome_benchmark_attribution.return",
                    });
                }
            }
            OutcomeBenchmarkResult::Unavailable {
                reason,
                observed_samples,
                required_samples,
            } => {
                if *required_samples
                    != self
                        .benchmark
                        .minimum_samples()
                        .max(window_required_samples)
                    || (*reason == OutcomeBenchmarkUnavailableReason::InsufficientGovernedSamples
                        && observed_samples >= required_samples)
                {
                    return Err(DomainError::InvalidBudget {
                        field: "outcome_benchmark_attribution.unavailable",
                    });
                }
            }
        }
        Ok(())
    }
}

impl OutcomeNavPoint {
    // 投资组合和 benchmark NAV 都必须为正，才能表达收益路径。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.portfolio_nav_ppm <= 0 || self.benchmark_nav_ppm <= 0 {
            return Err(DomainError::InvalidBudget {
                field: "outcome_nav_point",
            });
        }
        Ok(())
    }
}

/// Per-order execution-cost observations. Quote-timestamp and counterfactual
/// fields remain absent until governed observations make them measurable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeOrderCostAttribution {
    pub asset: Asset,
    pub side: OrderSide,
    pub filled_quantity_micros: i64,
    pub unfilled_quantity_micros: i64,
    pub limit_price: MoneyMicros,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_mid: Option<MoneyMicros>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrival_mid: Option<MoneyMicros>,
    /// The immutable execution-context quote used by this frozen valuation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_mid: Option<MoneyMicros>,
    /// Signed per-share cash effect relative to the frozen execution quote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_to_fill_effect: Option<MoneyMicros>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill_vwap: Option<MoneyMicros>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_shortfall: Option<MoneyMicros>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_cost: Option<MoneyMicros>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_impact: Option<MoneyMicros>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unfilled_opportunity_cost: Option<MoneyMicros>,
}

impl OutcomeOrderCostAttribution {
    // 校验成交/未成交数量、价格/成本符号以及 fill/shortfall 与成交数量的配对关系。
    pub fn validate(&self) -> Result<(), DomainError> {
        let quantity = self
            .filled_quantity_micros
            .checked_add(self.unfilled_quantity_micros);
        if self.filled_quantity_micros < 0
            || self.unfilled_quantity_micros < 0
            || quantity.is_none_or(|quantity| quantity <= 0)
            || self.limit_price.0 <= 0
            || [
                self.decision_mid,
                self.arrival_mid,
                self.baseline_mid,
                self.fill_vwap,
            ]
            .into_iter()
            .flatten()
            .any(|price| price.0 <= 0)
            || [
                self.limit_shortfall,
                self.spread_cost,
                self.market_impact,
                self.unfilled_opportunity_cost,
            ]
            .into_iter()
            .flatten()
            .any(|cost| cost.0 < 0)
            || self.fill_vwap.is_some() != (self.filled_quantity_micros > 0)
            || self.limit_shortfall.is_some() != (self.filled_quantity_micros > 0)
        {
            return Err(DomainError::InvalidBudget {
                field: "outcome_order_cost_attribution",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeWindow {
    pub horizon: OutcomeHorizon,
    pub observed_trading_day: NaiveDate,
    /// Frozen exposure price effect, before implementation and fees.
    pub portfolio_return_ppm: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementation_effect_ppm: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_valuation_effect_ppm: Option<i64>,
    pub benchmark_return_ppm: i64,
    pub transaction_cost_ppm: u32,
    pub slippage_ppm: u32,
    pub utility_ppm: i64,
    /// Legacy field retained for serde compatibility. New outcomes keep this
    /// `None`: one binary event is a forecast score, not a calibration report.
    #[serde(default)]
    pub calibration_ppm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forecast_score: Option<ForecastScore>,
    #[serde(default)]
    pub evidence_completeness_ppm: Option<u32>,
    pub risk_recall_ppm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_counts: Option<CountedRatio>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_recall_counts: Option<CountedRatio>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_ground_truth: Option<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turnover_ppm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_drawdown_ppm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracking_error_ppm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub beta_ppm: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_shortfall_ppm: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sortino_ratio_ppm: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nav_path: Vec<OutcomeNavPoint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub benchmark_attributions: Vec<OutcomeBenchmarkAttribution>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_cost_attributions: Vec<OutcomeOrderCostAttribution>,
}

impl OutcomeWindow {
    // 校验窗口指标、计数绑定、风险真值引用、NAV 日期、benchmark 全集和订单成本。
    pub fn validate(&self) -> Result<(), DomainError> {
        if [
            self.calibration_ppm.unwrap_or_default(),
            self.evidence_completeness_ppm.unwrap_or_default(),
            self.risk_recall_ppm.unwrap_or_default(),
            self.transaction_cost_ppm,
            self.slippage_ppm,
            self.turnover_ppm.unwrap_or_default(),
            self.maximum_drawdown_ppm.unwrap_or_default(),
            self.tracking_error_ppm.unwrap_or_default(),
            self.expected_shortfall_ppm.unwrap_or_default(),
        ]
        .into_iter()
        .any(|value| value > 1_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "outcome_window.ppm",
            });
        }
        if self
            .beta_ppm
            .is_some_and(|value| value.unsigned_abs() > 10_000_000)
            || self
                .sortino_ratio_ppm
                .is_some_and(|value| value.unsigned_abs() > 100_000_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "outcome_window.ratio",
            });
        }
        if let Some(score) = self.forecast_score {
            score.validate()?;
        }
        if let Some(counts) = self.evidence_counts {
            counts.validate()?;
            if self.evidence_completeness_ppm != Some(counts.ratio_ppm) {
                return Err(DomainError::InvalidBudget {
                    field: "outcome_window.evidence_counts",
                });
            }
        }
        if let Some(counts) = self.risk_recall_counts {
            counts.validate()?;
            if self.risk_recall_ppm != Some(counts.ratio_ppm) {
                return Err(DomainError::InvalidBudget {
                    field: "outcome_window.risk_recall_counts",
                });
            }
        }
        // A legacy deserialized window may carry only `risk_recall_ppm`.
        // New materialization never creates that shape: counted recall must
        // bind the independent assessment Artifact that produced it.
        if self.risk_recall_counts.is_some() != self.risk_ground_truth.is_some()
            || self
                .risk_ground_truth
                .as_ref()
                .is_some_and(|reference| reference.kind != ArtifactKind::RiskGroundTruthAssessment)
        {
            return Err(DomainError::InvalidBudget {
                field: "outcome_window.risk_ground_truth",
            });
        }
        // NAV path 必须按观察日严格递增；没有路径不被这里强制视为错误。
        let mut previous_day = None;
        for point in &self.nav_path {
            point.validate()?;
            if previous_day.is_some_and(|day| day >= point.observed_trading_day) {
                return Err(DomainError::InvalidBudget {
                    field: "outcome_window.nav_path",
                });
            }
            previous_day = Some(point.observed_trading_day);
        }
        if !self.benchmark_attributions.is_empty() {
            let present = self
                .benchmark_attributions
                .iter()
                .map(|attribution| attribution.benchmark)
                .collect::<std::collections::BTreeSet<_>>();
            if self.benchmark_attributions.len() != OutcomeBenchmark::ALL.len()
                || present.len() != OutcomeBenchmark::ALL.len()
                || OutcomeBenchmark::ALL
                    .into_iter()
                    .any(|benchmark| !present.contains(&benchmark))
            {
                return Err(DomainError::InvalidBudget {
                    field: "outcome_window.benchmark_attributions",
                });
            }
            // 基准比较使用价格效应 + 实施/估值桥接 - 交易成本/滑点的 checked 算术。
            let portfolio_net_return_ppm = self
                .portfolio_return_ppm
                .checked_add(self.implementation_effect_ppm.unwrap_or(0))
                .and_then(|value| value.checked_add(self.initial_valuation_effect_ppm.unwrap_or(0)))
                .and_then(|value| value.checked_sub(i64::from(self.transaction_cost_ppm)))
                .and_then(|value| value.checked_sub(i64::from(self.slippage_ppm)))
                .ok_or(DomainError::InvalidBudget {
                    field: "outcome_window.benchmark_attributions",
                })?;
            for attribution in &self.benchmark_attributions {
                attribution.validate(
                    portfolio_net_return_ppm,
                    attribution
                        .benchmark
                        .minimum_samples()
                        .max(u32::from(self.horizon.trading_days())),
                )?;
            }
        }
        for attribution in &self.order_cost_attributions {
            attribution.validate()?;
        }
        Ok(())
    }
}

/// Rust-owned cost assumptions applied to every sealed outcome window.
/// Values are parts-per-million of notional; later Paper reconciliation may
/// replace them with observed fill costs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeCostModel {
    pub transaction_cost_ppm: u32,
    pub slippage_ppm: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrospectiveCategory {
    Research,
    Evidence,
    Risk,
    Decision,
    Execution,
    Topology,
    Contract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrospectiveConclusion {
    Worked,
    Failed,
    Mixed,
    Unresolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrospectiveStatus {
    Complete,
    ModelUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrospectiveFinding {
    pub category: RetrospectiveCategory,
    pub conclusion: RetrospectiveConclusion,
    pub statement: String,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    pub confidence_ppm: u32,
}

impl RetrospectiveFinding {
    // 约束复盘陈述、最多八条来源引用和 confidence ppm 范围。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.statement.trim().is_empty()
            || self.statement.chars().count() > 4_000
            || self.artifact_refs.len() > 8
            || self.confidence_ppm > 1_000_000
        {
            return Err(DomainError::InvalidBudget {
                field: "retrospective.finding",
            });
        }
        Ok(())
    }
}

/// A scoped, reviewable hypothesis. Legacy free text remains readable but is
/// never promoted into an unrestricted Lesson by the current contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrospectiveLessonProposal {
    pub statement: String,
    pub recommended_behavior: String,
    pub exclusions: Vec<String>,
    pub assets: std::collections::BTreeSet<Asset>,
    pub horizons: std::collections::BTreeSet<crate::DecisionHorizon>,
    pub evidence_refs: Vec<ArtifactRef>,
}
impl RetrospectiveLessonProposal {
    // Lesson 候选必须有范围化陈述、行为、排除条件、资产/horizon 和证据引用。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.statement.trim().is_empty()
            || self.statement.len() > 4000
            || self.recommended_behavior.trim().is_empty()
            || self.recommended_behavior.len() > 4000
            || self.assets.is_empty()
            || self.assets.len() > 4
            || self.horizons.is_empty()
            || self.horizons.len() > 3
            || self.exclusions.is_empty()
            || self.exclusions.len() > 4
            || self
                .exclusions
                .iter()
                .any(|s| s.trim().is_empty() || s.len() > 1200)
            || self.evidence_refs.is_empty()
            || self.evidence_refs.len() > 4
        {
            return Err(DomainError::EmptyField {
                field: "retrospective.lesson_proposal",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrospectiveDraft {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub horizon: OutcomeHorizon,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<RetrospectiveFinding>,
    #[serde(default)]
    pub counterfactuals: Vec<String>,
    #[serde(default)]
    pub lesson_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lesson_proposals: Vec<RetrospectiveLessonProposal>,
    #[serde(default)]
    pub diagnostic_gaps: Vec<String>,
    #[serde(default)]
    pub source_refs: Vec<ArtifactRef>,
    pub created_at: DateTime<Utc>,
}

impl RetrospectiveDraft {
    // 校验 Draft 身份、叙事数组数量/长度，并逐项校验 finding 和 Lesson proposal。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.outcome_id.0.trim().is_empty()
            || self.summary.chars().count() > 4_000
            || self.findings.len() > 12
            || self.source_refs.len() > 8
            || self.counterfactuals.len() > 3
            || self.lesson_candidates.len() > 8
            || self.lesson_proposals.len() > 4
            || self.diagnostic_gaps.len() > 8
            || self
                .counterfactuals
                .iter()
                .any(|item| item.chars().count() > 4_000)
            || self
                .lesson_candidates
                .iter()
                .any(|item| item.chars().count() > 4_000)
            || self
                .diagnostic_gaps
                .iter()
                .any(|item| item.chars().count() > 4_000)
        {
            return Err(DomainError::InvalidBudget {
                field: "retrospective.draft",
            });
        }
        for proposal in &self.lesson_proposals {
            proposal.validate()?;
        }
        for finding in &self.findings {
            finding.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retrospective {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub horizon: OutcomeHorizon,
    pub status: RetrospectiveStatus,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<RetrospectiveFinding>,
    #[serde(default)]
    pub counterfactuals: Vec<String>,
    #[serde(default)]
    pub lesson_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lesson_proposals: Vec<RetrospectiveLessonProposal>,
    #[serde(default)]
    pub diagnostic_gaps: Vec<String>,
    #[serde(default)]
    pub source_refs: Vec<ArtifactRef>,
    pub outcome: ArtifactRef,
    pub created_at: DateTime<Utc>,
    pub sealed_at: Option<DateTime<Utc>>,
}

impl Retrospective {
    // 先复用同形 Draft 校验，再检查 Outcome 引用和 T5 必须 sealed 的规则。
    pub fn validate(&self) -> Result<(), DomainError> {
        let draft = RetrospectiveDraft {
            schema_version: self.schema_version,
            outcome_id: self.outcome_id.clone(),
            horizon: self.horizon,
            summary: self.summary.clone(),
            findings: self.findings.clone(),
            counterfactuals: self.counterfactuals.clone(),
            lesson_candidates: self.lesson_candidates.clone(),
            lesson_proposals: self.lesson_proposals.clone(),
            diagnostic_gaps: self.diagnostic_gaps.clone(),
            source_refs: self.source_refs.clone(),
            created_at: self.created_at,
        };
        draft.validate()?;
        if self.outcome.kind != ArtifactKind::Outcome {
            return Err(DomainError::EmptyField {
                field: "retrospective.outcome",
            });
        }
        if self.horizon == OutcomeHorizon::T5 && self.sealed_at.is_none() {
            return Err(DomainError::EmptyField {
                field: "retrospective.sealed_at",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptRelationKind {
    Retry,
    Recovery,
    Replay,
    Shadow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRelation {
    pub schema_version: u32,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub parent_attempt_id: AttemptId,
    pub child_attempt_id: AttemptId,
    pub relation: AttemptRelationKind,
    pub created_at: DateTime<Utc>,
}

impl AttemptRelation {
    // 校验尝试关系的身份非空且 parent/child 不相同。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION
            || self.run_id.0.trim().is_empty()
            || self.task_id.0.trim().is_empty()
            || self.parent_attempt_id.0.trim().is_empty()
            || self.child_attempt_id.0.trim().is_empty()
            || self.parent_attempt_id == self.child_attempt_id
        {
            return Err(DomainError::EmptyField {
                field: "attempt_relation.identity",
            });
        }
        Ok(())
    }
}

impl OutcomeCostModel {
    // 交易成本和滑点均按 ppm 表达，不能超过 100%。
    pub fn validate(self) -> Result<(), DomainError> {
        if self.transaction_cost_ppm > 1_000_000 || self.slippage_ppm > 1_000_000 {
            return Err(DomainError::InvalidBudget {
                field: "outcome.cost_model",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub schema_version: u32,
    pub outcome_id: OutcomeId,
    pub schedule: ArtifactRef,
    pub market_evidence: Vec<ArtifactRef>,
    /// None denotes a legacy payload whose attribution semantics are unversioned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric_basis: Option<OutcomeMetricBasis>,
    pub windows: Vec<OutcomeWindow>,
    pub sealed_at: Option<DateTime<Utc>>,
}

impl Outcome {
    // sealed_at 存在即表示 Outcome 已封存；不等同于学习资格已通过。
    pub fn is_sealed(&self) -> bool {
        self.sealed_at.is_some()
    }

    // 从所有窗口收集风险真值引用，排序去重后返回 provenance 闭包。
    pub fn risk_ground_truth_refs(&self) -> Vec<ArtifactRef> {
        let mut references = self
            .windows
            .iter()
            .filter_map(|window| window.risk_ground_truth.clone())
            .collect::<Vec<_>>();
        references.sort();
        references.dedup();
        references
    }

    // 校验 Outcome 身份/引用、窗口数量、每个 horizon 唯一性和观察日递增。
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.schema_version != DOMAIN_SCHEMA_VERSION || self.outcome_id.0.trim().is_empty() {
            return Err(DomainError::EmptyField {
                field: "outcome.identity",
            });
        }
        if self.schedule.kind != ArtifactKind::OutcomeSchedule
            || self.market_evidence.is_empty()
            || self.market_evidence.iter().any(|evidence| {
                !matches!(
                    evidence.kind,
                    ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail
                )
            })
        {
            return Err(DomainError::EmptyField {
                field: "outcome.references",
            });
        }

        if self.windows.is_empty() || self.windows.len() > OutcomeHorizon::ALL.len() {
            return Err(DomainError::InvalidBudget {
                field: "outcome.windows",
            });
        }
        let mut observed_days = [None; 3];
        for window in &self.windows {
            window.validate()?;
            let index = match window.horizon {
                OutcomeHorizon::T1 => 0,
                OutcomeHorizon::T3 => 1,
                OutcomeHorizon::T5 => 2,
            };
            if observed_days[index].is_some() {
                return Err(DomainError::InvalidBudget {
                    field: "outcome.windows",
                });
            }
            observed_days[index] = Some(window.observed_trading_day);
        }
        let mut previous_day = None;
        for day in observed_days.into_iter().flatten() {
            if previous_day.is_some_and(|previous| previous >= day) {
                return Err(DomainError::InvalidBudget {
                    field: "outcome.window_trading_days",
                });
            }
            previous_day = Some(day);
        }
        Ok(())
    }

    // 在普通校验之外要求恰好三个 horizon 且存在 sealed_at。
    pub fn validate_sealed(&self) -> Result<(), DomainError> {
        self.validate()?;
        if self.windows.len() != OutcomeHorizon::ALL.len() {
            return Err(DomainError::InvalidBudget {
                field: "outcome.windows",
            });
        }
        self.sealed_at.ok_or(DomainError::EmptyField {
            field: "outcome.sealed_at",
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeMetricBasis {
    /// Static post-execution exposure; subsequent trades, dividends and cash
    /// flows are absent. Never represent this as realized broker account NAV.
    FrozenPostExecutionExposureV2,
    /// Frozen quantity price effect plus signed baseline/fill and initial
    /// valuation bridges, less estimated fees; subsequent account flows absent.
    FrozenPostExecutionExposureV3,
}
