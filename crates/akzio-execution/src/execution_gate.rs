//! Typed execution gate over the persisted Decision/snapshot closure and
//! separately supplied pre-trade safety observations.

// 文件导读：ExecutionGate 是 Decision → ExecutionVerdict 的第二道 Rust 闸门。它从 Store
// 读取 DecisionContext、账户/报价/时钟 NormalizedEvidence，并接收 daemon 组装的
// pre-trade 观察值，检查来源闭包、freshness、session、Paper approval、Policy influence、
// 容量/合规/依赖与订单可行性，生成可选 ExecutionPlan、ExecutionContext 和
// Accepted/NoOrder Verdict。本 runtime 的
// evaluate 不直接调用 Broker；daemon 在调用它之前仍可只读刷新外部快照。
// Accepted 之后仍必须经过 PaperCommitment 的订单写请求前持久化。
// ExecutionRuntime 持有 Store 与三套只读 Policy；本文件声明公开 I/O 形状，`include!` 的
// core/snapshots/validation/helpers 是同一模块内的实现分区，共享私有字段与错误类型。

use std::collections::BTreeSet;

use akzio_domain::{
    AccountSnapshot, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactRef, CandidatePolicy,
    ContextManifestPayload, DecisionContext, DomainError, ExecutionContext, ExecutionVerdict,
    Experience, FreezeState, HardBlocker, MarketClockSnapshot, ModelQualificationGate, MoneyMicros,
    NoOrder, PolicySubject, QuoteSnapshot, RunPurpose, TaskStatus, TaskWritePermit,
};
use akzio_store::{Store, StoreError};
use chrono::{DateTime, Duration, Utc};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{
    AllocationError, AllocationInput, AllocationRuntime, ExecutionError, ExecutionGatePolicy,
    ExecutionPolicy, PreTradeSafetyEvidence, PreTradeSafetyInput, PreTradeSafetyPolicy,
};

#[derive(Debug, Error)]
pub enum ExecutionGateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("expected {expected:?} artifact, found {actual:?}")]
    WrongArtifactKind {
        expected: ArtifactKind,
        actual: ArtifactKind,
    },
    #[error("decision context does not belong to the execution task run")]
    DecisionRunMismatch,
    #[error("execution gate integrity failure: {0}")]
    Integrity(&'static str),
}

pub type ExecutionGateResult<T> = std::result::Result<T, ExecutionGateError>;

#[derive(Debug, Clone)]
pub struct ExecutionGateInput {
    // 输入都是已持久化 ArtifactRef；缺失快照不是 Rust 的“默认安全”，会被转成明确 blocker。
    pub permit: TaskWritePermit,
    pub decision_context: ArtifactRef,
    pub account_snapshot: Option<ArtifactRef>,
    pub quote_snapshot: Option<ArtifactRef>,
    /// Set when the execution refresh received a quote payload but Rust
    /// rejected it before sealing a QuoteSnapshot.  This distinguishes an
    /// invalid quote from a missing broker response in the durable verdict.
    pub quote_validation_error: Option<String>,
    pub market_clock_snapshot: Option<ArtifactRef>,
    pub pretrade_safety: Option<PreTradeSafetyEvidence>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ExecutionGateOutput {
    // plan 只在全部分配前提满足时存在；context/verdict 即使 NoOrder 也会保存，供 Outcome
    // 继续评估“无订单研究意图”。
    pub execution_plan: Option<Artifact>,
    pub execution_context: Artifact,
    pub verdict: Artifact,
}

#[derive(Debug, Clone)]
pub struct ExecutionRuntime {
    store: Store,
    allocation: AllocationRuntime,
    gate_policy: ExecutionGatePolicy,
    pretrade_safety: PreTradeSafetyPolicy,
}

include!("execution_gate/core.rs");
include!("execution_gate/snapshots.rs");
include!("execution_gate/validation.rs");
include!("execution_gate/helpers.rs");

#[cfg(test)]
mod allocated_risk_tests {
    use super::*;
    use crate::{
        allocation::tests as fixtures,
        decision_gate::decision_trace_tests::policy_with_positive_calibration_bin, DecisionPolicy,
        DecisionPolicyArtifact, DecisionPolicyProvenance,
    };
    use akzio_domain::{
        content_hash_json, ArtifactProvenance, Asset, ComplianceActivitySnapshot,
        ComplianceControl, ContentHash, DecisionHorizon, DependencyClosure, DependencyHealthStatus,
        DependencyKind, DependencySnapshot, FallbackPolicy, InformationClassification, Position,
        RunId, WeightPpm,
    };
    use akzio_store::DecisionPolicyDescriptor;
    use std::collections::BTreeMap;

    fn runtime(drawdown_ppm: u32) -> ExecutionRuntime {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/execution-risk-tests")
            .join(RunId::new().0);
        let gate_policy = ExecutionGatePolicy {
            max_turnover_ppm: WeightPpm::SCALE,
            mandate: akzio_domain::MandateSnapshot {
                maximum_turnover: WeightPpm(WeightPpm::SCALE),
                maximum_drawdown_budget: WeightPpm(drawdown_ppm),
                maximum_gross_exposure: WeightPpm(800_000),
                minimum_cash_reserve: WeightPpm(200_000),
                ..Default::default()
            },
            ..Default::default()
        };
        ExecutionRuntime::new(
            Store::open(root).unwrap(),
            ExecutionPolicy::default(),
            gate_policy,
        )
        .unwrap()
    }

    fn install_policy(store: &Store, policy: &DecisionPolicy, now: DateTime<Utc>) -> ContentHash {
        let scope = policy.active_forecast_calibration.as_ref().unwrap();
        let contract_hash = ContentHash::of_bytes(b"gate fixture synth contract");
        let risk_model_hash =
            content_hash_json(&serde_json::to_value(&policy.portfolio_risk_model).unwrap())
                .unwrap();
        let policy_hash = policy.policy_hash().unwrap();
        let envelope = DecisionPolicyArtifact {
            schema_version: DecisionPolicyArtifact::SCHEMA_VERSION,
            provenance: DecisionPolicyProvenance {
                policy_version: "fixture".to_owned(),
                algorithm_version: "fixture".to_owned(),
                created_at: now,
                training_start: now - Duration::days(3),
                training_end: now - Duration::days(2),
                assets: Asset::EXECUTABLE.to_vec(),
                horizons: DecisionHorizon::ALL.to_vec(),
                sample_count: 1,
                source_runs: vec!["fixture-run".to_owned()],
                input_hash: ContentHash::of_bytes(b"fixture input"),
                output_hash: policy_hash.clone(),
                risk_model_hash,
                provider_id: Some("openai-responses".to_owned()),
                model_route: Some("research.synthesizer".to_owned()),
                contract_hash: Some(contract_hash.clone()),
            },
            policy: policy.clone(),
        };
        let blob = store.stage_json(&envelope).unwrap();
        let descriptor = DecisionPolicyDescriptor {
            policy_hash: policy_hash.clone(),
            envelope_hash: blob.hash.clone(),
            provider_id: "openai-responses".to_owned(),
            model_id: scope.model_id.clone(),
            model_version_hash: scope.model_version_hash.clone(),
            model_route: "research.synthesizer".to_owned(),
            contract_hash,
        };
        let artifact = Artifact::new(
            ArtifactKind::DecisionPolicy,
            blob,
            "decision-policy-test",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "fixture".to_owned(),
                observed_at: Some(now),
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: WeightPpm::SCALE,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        store
            .activate_decision_policy(&artifact, &descriptor, now)
            .unwrap();
        policy_hash
    }

    fn pretrade(now: DateTime<Utc>) -> PreTradeSafetyEvidence {
        let dependencies = DependencyKind::ALL
            .into_iter()
            .map(|kind| {
                (
                    kind,
                    DependencySnapshot {
                        kind,
                        provider: "fixture".into(),
                        service: "fixture".into(),
                        version: "fixture".into(),
                        data_classification: "fixture".into(),
                        expected_freshness_secs: 60,
                        observed_at: now,
                        status: DependencyHealthStatus::Healthy,
                        fallback_policy: FallbackPolicy::FailClosed,
                        configured_service: "fixture".into(),
                        actual_service: "fixture".into(),
                        fourth_party_dependencies: BTreeSet::new(),
                    },
                )
            })
            .collect();
        PreTradeSafetyEvidence {
            homogeneous_agent_count: 1,
            average_daily_dollar_volume: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| (asset, MoneyMicros(100_000_000_000)))
                .collect(),
            information_classifications: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| (asset, InformationClassification::Public))
                .collect(),
            recent_compliance_activity: ComplianceActivitySnapshot {
                recent_submissions: 0,
                recent_cancellations: 0,
                self_trade_detected: false,
                spoofing_or_layering_pattern: false,
                marking_the_close_risk: false,
                prearranged_trade_risk: false,
                front_running_risk: false,
                available_controls: ComplianceControl::PAPER_BASELINE.into_iter().collect(),
            },
            dependency_closure: DependencyClosure {
                captured_at: now,
                dependencies,
            },
        }
    }

    fn allocation_input(
        policy: &DecisionPolicy,
        positions: BTreeMap<Asset, Position>,
        targets: &[(Asset, u32)],
    ) -> AllocationInput {
        let now = fixtures::now();
        let mut context = fixtures::decision_context(now, 0);
        context.decision_policy_hash = policy.policy_hash().unwrap();
        context.portfolio_risk.risk_model_hash = Some(
            content_hash_json(&serde_json::to_value(&policy.portfolio_risk_model).unwrap())
                .unwrap(),
        );
        for (asset, ppm) in targets {
            context.target.weights.insert(*asset, WeightPpm(*ppm));
        }
        let mut quotes = fixtures::quotes(now, 10_000_000, 10_000_001);
        let quote = quotes.quotes[&Asset::Tqqq];
        quotes.quotes = Asset::EXECUTABLE
            .into_iter()
            .map(|asset| (asset, quote))
            .collect();
        let mut allocation = fixtures::input(
            now,
            context,
            fixtures::account(now, positions),
            quotes,
            fixtures::clock(now),
        );
        for reference in [
            &mut allocation.account_snapshot_ref,
            &mut allocation.quote_snapshot_ref,
            &mut allocation.market_clock_snapshot_ref,
        ] {
            reference.kind = ArtifactKind::NormalizedEvidence;
        }
        allocation
    }

    fn allocate(runtime: &ExecutionRuntime, input: &AllocationInput) -> crate::ExecutionPlan {
        let plan = runtime
            .allocation
            .allocate_with_limit(input, MoneyMicros(100_000_000))
            .unwrap();
        plan.validate().unwrap();
        plan
    }

    /// WHY THIS TEST EXISTS (Task #3): The approved target and full allocation
    /// can fit the frozen gross limit while the buy executing alone exceeds it.
    /// Do not weaken or delete this assertion.
    #[test]
    fn post_allocation_gate_checks_buy_before_sale_against_frozen_policy() {
        let runtime = runtime(150_000);
        let now = fixtures::now();
        let mut frozen = policy_with_positive_calibration_bin(now);
        frozen.max_gross_weight = WeightPpm(350_000);
        let frozen_hash = install_policy(&runtime.store, &frozen, now);
        let mut active = frozen.clone();
        active.max_gross_weight = WeightPpm(700_000);
        let active_hash = install_policy(&runtime.store, &active, now + Duration::seconds(1));
        assert_ne!(frozen_hash, active_hash);
        assert_eq!(
            runtime
                .store
                .active_decision_policy()
                .unwrap()
                .unwrap()
                .descriptor
                .policy_hash,
            active_hash
        );
        let allocation = allocation_input(
            &frozen,
            BTreeMap::from([(
                Asset::Qqq,
                Position {
                    quantity_micros: 3_000_000,
                    market_value: MoneyMicros(30_000_000),
                },
            )]),
            &[(Asset::Soxx, 250_000)],
        );
        let plan = allocate(&runtime, &allocation);
        assert_eq!(plan.orders.len(), 2);
        assert!(plan.gross_exposure_ppm < 350_000);
        assert_eq!(plan.target.weights[&Asset::Soxx], WeightPpm(250_000));
        let mut blockers = BTreeSet::new();
        runtime
            .assess_allocated_plan(&allocation, &plan, Some(&pretrade(now)), &mut blockers)
            .unwrap();
        assert!(
            blockers.contains(&HardBlocker::FactorLimit),
            "buy-only state reaches 550000 ppm gross, blockers={blockers:?}"
        );

        // The same plan would fit the newer limits, which is why the exact
        // Decision hash, not the current active head, is necessary.
        let mut newer = allocation;
        newer.decision_context.decision_policy_hash = active_hash;
        blockers.clear();
        runtime
            .assess_allocated_plan(&newer, &plan, Some(&pretrade(now)), &mut blockers)
            .unwrap();
        assert!(blockers.is_empty(), "{blockers:?}");
    }

    /// WHY THIS TEST EXISTS (Task #3): A cash decision has no initial risk
    /// estimate, but its frozen policy can still prove an ordinary sale reduces risk.
    /// Do not weaken or delete this assertion.
    #[test]
    fn corrective_sale_requires_a_frozen_policy_but_can_reduce_existing_excess() {
        let runtime = runtime(150_000);
        let now = fixtures::now();
        let mut frozen = policy_with_positive_calibration_bin(now);
        frozen.max_gross_weight = WeightPpm(250_000);
        let mut allocation = allocation_input(
            &frozen,
            BTreeMap::from([(
                Asset::Qqq,
                Position {
                    quantity_micros: 3_000_000,
                    market_value: MoneyMicros(30_000_000),
                },
            )]),
            &[],
        );
        allocation.decision_context.portfolio_risk = Default::default();
        let plan = allocate(&runtime, &allocation);
        assert_eq!(plan.orders[0].side, crate::OrderSide::Sell);
        assert_eq!(plan.orders.len(), 1);
        let evidence = pretrade(now);
        let mut blockers = BTreeSet::new();
        runtime
            .assess_allocated_plan(&allocation, &plan, Some(&evidence), &mut blockers)
            .unwrap();
        assert_eq!(blockers, BTreeSet::from([HardBlocker::InvalidProvenance]));

        install_policy(&runtime.store, &frozen, now);
        blockers.clear();
        let (mandate, safety) = runtime
            .assess_allocated_plan(&allocation, &plan, Some(&evidence), &mut blockers)
            .unwrap();
        assert!(mandate.unwrap().permitted());
        let safety = safety.unwrap();
        assert!(!safety.risk_increasing);
        assert!(safety.permits_execution);
        assert!(blockers.is_empty(), "{blockers:?}");

        allocation.decision_context.portfolio_risk.risk_model_hash =
            Some(ContentHash::of_bytes(b"wrong matrix"));
        runtime
            .assess_allocated_plan(&allocation, &plan, Some(&evidence), &mut blockers)
            .unwrap();
        assert_eq!(blockers, BTreeSet::from([HardBlocker::InvalidProvenance]));
    }

    /// WHY THIS TEST EXISTS (Task #3): Fractional corrective sales must clear
    /// an IncreaseRisk prohibition, and sub-ppm trades still obey a Trade prohibition.
    /// Do not weaken or delete this assertion.
    #[test]
    fn fractional_corrective_sale_obeys_mandate_action_prohibitions() {
        use akzio_domain::{MandateActionType, MandateViolation};

        let mut runtime = runtime(150_000);
        let now = fixtures::now();
        let frozen = policy_with_positive_calibration_bin(now);
        install_policy(&runtime.store, &frozen, now);
        let mut allocation = allocation_input(
            &frozen,
            BTreeMap::from([(
                Asset::Qqq,
                Position {
                    quantity_micros: 1_000_007,
                    market_value: MoneyMicros(100_000_700),
                },
            )]),
            &[(Asset::Qqq, 100_000)],
        );
        allocation.account.equity = MoneyMicros(1_000_000_000);
        let quote = allocation.quotes.quotes.get_mut(&Asset::Qqq).unwrap();
        quote.bid = MoneyMicros(101_000_000);
        quote.ask = MoneyMicros(101_000_001);
        let plan = allocate(&runtime, &allocation);
        assert_eq!(plan.orders.len(), 1);
        assert_eq!(plan.orders[0].side, crate::OrderSide::Sell);
        // Marked weight falls from 100000.7 to 100000.1 ppm. Both ceils
        // are identical, and either exceeds the baseline floor of 100000.
        assert_eq!(
            crate::paper::order_quantity_millionths(&plan.orders[0]).unwrap(),
            6
        );
        let evidence = pretrade(now);
        runtime.gate_policy.mandate.prohibited_actions =
            BTreeSet::from([MandateActionType::IncreaseRisk]);
        let mut blockers = BTreeSet::new();
        let (mandate, safety) = runtime
            .assess_allocated_plan(&allocation, &plan, Some(&evidence), &mut blockers)
            .unwrap();
        assert!(!safety.unwrap().risk_increasing);
        assert!(mandate.unwrap().permitted(), "{blockers:?}");
        assert!(blockers.is_empty(), "{blockers:?}");

        runtime.gate_policy.mandate.prohibited_actions = BTreeSet::from([MandateActionType::Trade]);
        let (mandate, _) = runtime
            .assess_allocated_plan(&allocation, &plan, Some(&evidence), &mut blockers)
            .unwrap();
        assert_eq!(
            mandate.unwrap().violations,
            BTreeSet::from([MandateViolation::ProhibitedAction])
        );
        assert_eq!(blockers, BTreeSet::from([HardBlocker::MandateViolation]));
    }

    /// WHY THIS TEST EXISTS (Task #3): Selling one side of a negative-covariance
    /// hedge can increase portfolio risk and requires the new-risk safety checks.
    /// Do not weaken or delete this assertion.
    #[test]
    fn hedge_sale_increasing_volatility_cannot_use_reduction_only_dependencies() {
        let runtime = runtime(150_000);
        let now = fixtures::now();
        let mut frozen = policy_with_positive_calibration_bin(now);
        for (left, right) in [(Asset::Qqq, Asset::Soxx), (Asset::Soxx, Asset::Qqq)] {
            frozen
                .portfolio_risk_model
                .covariance_ppm_squared
                .get_mut(&left)
                .unwrap()
                .insert(right, -9_000_000_000);
        }
        install_policy(&runtime.store, &frozen, now);
        let allocation = allocation_input(
            &frozen,
            [Asset::Qqq, Asset::Soxx]
                .into_iter()
                .map(|asset| {
                    (
                        asset,
                        Position {
                            quantity_micros: 2_500_000,
                            market_value: MoneyMicros(25_000_000),
                        },
                    )
                })
                .collect(),
            &[(Asset::Soxx, 250_000)],
        );
        let plan = allocate(&runtime, &allocation);
        assert_eq!(plan.orders.len(), 1);
        assert_eq!(plan.orders[0].side, crate::OrderSide::Sell);
        let mut evidence = pretrade(now);
        evidence
            .dependency_closure
            .dependencies
            .get_mut(&DependencyKind::ModelProvider)
            .unwrap()
            .status = DependencyHealthStatus::Stale;
        let mut blockers = BTreeSet::new();
        let (_, safety) = runtime
            .assess_allocated_plan(&allocation, &plan, Some(&evidence), &mut blockers)
            .unwrap();
        let safety = safety.unwrap();
        assert!(safety.risk_increasing);
        assert!(!safety.permits_execution);
        assert_eq!(blockers, BTreeSet::from([HardBlocker::DependencyDegraded]));
    }

    /// WHY THIS TEST EXISTS (Task #3): A clipped buy can fill while the paired
    /// sale is pending, exposing the mandate to more tail loss than the decision.
    /// Do not weaken or delete this assertion.
    #[test]
    fn clipped_buy_before_sale_passes_actual_expected_shortfall_to_mandate() {
        let runtime = runtime(50_000);
        let now = fixtures::now();
        let mut frozen = policy_with_positive_calibration_bin(now);
        frozen
            .asset_calibrations
            .get_mut(&Asset::Qqq)
            .unwrap()
            .expected_shortfall_ppm = 500_000;
        install_policy(&runtime.store, &frozen, now);
        let mut allocation = allocation_input(
            &frozen,
            BTreeMap::from([(
                Asset::Qqq,
                Position {
                    quantity_micros: 3_000_000,
                    market_value: MoneyMicros(30_000_000),
                },
            )]),
            &[(Asset::Soxx, 250_000)],
        );
        allocation.account.buying_power = MoneyMicros(5_000_000);
        let decision_shortfall = frozen
            .fill_risk(&allocation.decision_context.target)
            .unwrap()
            .expected_shortfall_ppm;
        assert!(decision_shortfall < 50_000);
        allocation
            .decision_context
            .portfolio_risk
            .expected_shortfall_ppm = Some(decision_shortfall);
        let plan = allocate(&runtime, &allocation);
        assert_eq!(plan.orders.len(), 2);
        assert_eq!(plan.target.weights[&Asset::Soxx], WeightPpm(50_000));
        let mut blockers = BTreeSet::new();
        let (mandate, _) = runtime
            .assess_allocated_plan(&allocation, &plan, Some(&pretrade(now)), &mut blockers)
            .unwrap();
        assert!(mandate
            .unwrap()
            .violations
            .contains(&akzio_domain::MandateViolation::DrawdownBudget));
        assert_eq!(blockers, BTreeSet::from([HardBlocker::MandateViolation]));
    }

    /// WHY THIS TEST EXISTS (Task #3): Pairwise covariance bounds alone cannot
    /// prove convex fill risk, and unproven risk must preserve static blockers.
    /// Do not weaken or delete this assertion.
    #[test]
    fn policy_with_indefinite_covariance_preserves_independent_blockers() {
        let mut runtime = runtime(150_000);
        runtime.gate_policy.factor_limits.semiconductor_ppm = 200_000;
        runtime.gate_policy.factor_limits.paired_index_ppm = 200_000;
        runtime.gate_policy.max_turnover_ppm = 200_000;
        let now = fixtures::now();
        let mut frozen = policy_with_positive_calibration_bin(now);
        for (left, row) in &mut frozen.portfolio_risk_model.covariance_ppm_squared {
            for (right, covariance) in row {
                if left != right {
                    *covariance = -9_000_000_000;
                }
            }
        }
        install_policy(&runtime.store, &frozen, now);
        let allocation = allocation_input(&frozen, BTreeMap::new(), &[(Asset::Soxx, 250_000)]);
        let plan = allocate(&runtime, &allocation);
        let mut blockers = BTreeSet::new();
        runtime
            .assess_allocated_plan(&allocation, &plan, Some(&pretrade(now)), &mut blockers)
            .unwrap();
        assert_eq!(
            blockers,
            BTreeSet::from([
                HardBlocker::InvalidProvenance,
                HardBlocker::FactorLimit,
                HardBlocker::PairExposureLimit,
                HardBlocker::TurnoverLimit,
            ]),
        );
    }

    /// WHY THIS TEST EXISTS (Task #3): Each frozen forecast risk limit must
    /// independently block the same otherwise admissible achieved position.
    /// Do not weaken or delete this assertion.
    #[test]
    fn each_frozen_risk_limit_blocks_an_achieved_buy() {
        let now = fixtures::now();
        let baseline = policy_with_positive_calibration_bin(now);
        let evidence = pretrade(now);
        for case in ["volatility", "beta", "tail", "gap", "asset"] {
            let runtime = runtime(150_000);
            let mut policy = baseline.clone();
            match case {
                "volatility" => policy.target_annualized_volatility_ppm = 30_000,
                "beta" => policy.max_portfolio_beta_ppm = 36_000,
                "tail" => policy.portfolio_risk_model.max_expected_shortfall_ppm = 3_600,
                "gap" => policy.portfolio_risk_model.max_gap_loss_ppm = 3_600,
                "asset" => {
                    let calibration = policy.asset_calibrations.get_mut(&Asset::Qqq).unwrap();
                    calibration.max_capital_weight = WeightPpm(220_000);
                    calibration.liquidity_weight_cap = WeightPpm(200_000);
                }
                _ => unreachable!("fixture limit"),
            }
            install_policy(&runtime.store, &policy, now);
            let allocation = allocation_input(
                &policy,
                BTreeMap::new(),
                &[(Asset::Qqq, 250_000), (Asset::Soxx, 250_000)],
            );
            let plan = allocate(&runtime, &allocation);
            assert_eq!(plan.gross_exposure_ppm, 500_000);
            let mut blockers = BTreeSet::new();
            runtime
                .assess_allocated_plan(&allocation, &plan, Some(&evidence), &mut blockers)
                .unwrap();
            assert_eq!(
                blockers,
                BTreeSet::from([HardBlocker::FactorLimit]),
                "{case}"
            );
        }
    }

    fn reserve_approved_execution(
        store: &Store,
        policy_hash: &ContentHash,
        now: DateTime<Utc>,
    ) -> TaskWritePermit {
        use akzio_domain::{
            ArtifactOrigin, FailureDisposition, ModelQualificationEvidence,
            ModelQualificationReport, PaperApprovalScope, PaperLaunchApproval, RetryPolicy,
            RuntimeManifest, TaskBudget, TaskId, TaskRecipeId, WorkflowGraph, WorkflowNode,
            WorkflowProposal, DOMAIN_SCHEMA_VERSION,
        };
        use akzio_store::{SessionReservation, StoredRun, TaskWorkload, WorkflowCommit};

        let expires_at = now + Duration::minutes(10);
        let run_id = RunId::new();
        let topology_id = "execution-risk-fixture";
        let run_origin = ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        };
        let provenance = ArtifactProvenance {
            source_family: "fixture".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: WeightPpm::SCALE,
            producer_contract_hash: None,
        };
        let stage = |kind, blob, producer, lifecycle, origin, sources| {
            Artifact::new(
                kind,
                blob,
                producer,
                lifecycle,
                provenance.clone(),
                origin,
                sources,
                now,
            )
            .unwrap()
        };
        let node = WorkflowNode {
            spec: None,
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("gate.execution").unwrap(),
            contract_hash: None,
            objective: "Evaluate frozen Paper execution evidence offline".into(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 1,
            budget: TaskBudget {
                max_input_tokens: 1,
                max_output_tokens: 1,
                max_wall_time_secs: 60,
                max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(0),
            },
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = stage(
            ArtifactKind::WorkflowGraph,
            store
                .stage_json(&WorkflowGraph {
                    definition_version: None,
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    topology_id: topology_id.into(),
                    nodes: vec![node.clone()],
                    agent_budgets: Default::default(),
                })
                .unwrap(),
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            Some(run_origin.clone()),
            vec![],
        );
        let hash = ContentHash::of_bytes(b"offline execution risk fixture");
        let components = [("default".to_owned(), hash.clone())].into();
        let bundle_hash = akzio_domain::runtime_component_bundle_hash(&components).unwrap();
        let manifest: RuntimeManifest = serde_json::from_value(serde_json::json!({
            "schema_version": DOMAIN_SCHEMA_VERSION,
            "code_revision": "fixture", "rust_toolchain": "1.96.0",
            "experiment_profile": "paper-engineering",
            "model_capability_hashes": components, "model_capability_bundle_hash": bundle_hash,
            "governance_component_hashes": components, "governance_bundle_hash": bundle_hash,
            "cargo_lock_hash": hash, "config_hash": hash,
            "provider_id": "fixture", "model_id": "fixture",
            "prompt_hash": hash, "contract_hash": hash, "topology_hash": hash,
            "decision_policy_hash": policy_hash,
            "execution_policy_hash": crate::paper::paper_execution_policy_hash().unwrap(),
            "evaluation_policy_hash": hash,
            "market_data_feed": "iex", "broker_account_id": "offline-account",
            "maximum_notional": 500_000_000,
            "allowed_session_start": now.date_naive(), "allowed_session_end": now.date_naive(),
            "expires_at": expires_at, "created_at": now,
        }))
        .unwrap();
        let manifest_artifact = stage(
            ArtifactKind::RuntimeManifest,
            store.stage_json(&manifest).unwrap(),
            "runtime.manifest",
            ArtifactLifecycle::Canonical,
            None,
            vec![],
        );
        let evidence_ref = |kind| ArtifactRef {
            artifact_id: akzio_domain::ArtifactId(hash.clone()),
            kind,
        };
        let evaluation = evidence_ref(ArtifactKind::Evaluation);
        let mut qualification = ModelQualificationReport {
            key: manifest.runtime_identity().qualification_key().unwrap(),
            behavior_fingerprint: hash.clone(),
            evidence: ModelQualificationEvidence {
                frozen_context_manifest: evidence_ref(ArtifactKind::ContextManifest),
                replay_evaluation: evaluation.clone(),
                adversarial_evaluation: evaluation.clone(),
                execution_simulation_evaluation: evaluation.clone(),
                shadow_evaluation: evaluation.clone(),
                canary_evaluation: evaluation,
            },
            replay_passed: true,
            adversarial_passed: true,
            execution_simulation_passed: true,
            shadow_passed: true,
            canary_passed: true,
            critical_regressions: 0,
            approved_by: "fixture".into(),
            qualified_at: now,
            expires_at,
            report_hash: hash.clone(),
        };
        qualification.report_hash = qualification.unsigned_hash();
        let manifest_ref = artifact_ref(&manifest_artifact);
        let mut approval = PaperLaunchApproval {
            schema_version: DOMAIN_SCHEMA_VERSION,
            operator_identity: "fixture".into(),
            runtime_manifest: manifest_ref.clone(),
            runtime_manifest_hash: manifest.manifest_hash().unwrap(),
            scope: PaperApprovalScope::Canary,
            reason: "offline execution risk regression".into(),
            behavior_bundle_hash: None,
            approved_at: now,
            expires_at,
            qualification,
            mandate: Default::default(),
            approval_hash: hash,
        };
        approval.approval_hash = approval.unsigned_hash().unwrap();
        let approval = stage(
            ArtifactKind::PaperLaunchApproval,
            store.stage_json(&approval).unwrap(),
            "operator.paper_approval",
            ArtifactLifecycle::Canonical,
            None,
            vec![manifest_ref],
        );
        let proposal = stage(
            ArtifactKind::WorkflowProposal,
            store
                .stage_json(&WorkflowProposal {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    topology_id: topology_id.into(),
                    tasks: Default::default(),
                    stop_reason: None,
                })
                .unwrap(),
            "runtime.paper_provisioning",
            ArtifactLifecycle::RunScoped,
            Some(run_origin),
            vec![],
        );
        let reservation = SessionReservation {
            session_key: now.date_naive().to_string(),
            workflow: WorkflowCommit {
                run: StoredRun {
                    run_id,
                    purpose: RunPurpose::Paper,
                    topology_id: topology_id.into(),
                    graph_artifact_id: graph.artifact_id.clone(),
                    created_at: now,
                },
                graph,
                nodes: vec![node],
            },
            setup_artifacts: vec![],
            reserved_at: now,
        };
        let lease = store
            .acquire_daemon_lease("scheduler", "fixture", now, expires_at)
            .unwrap()
            .unwrap();
        store
            .reserve_paper_session_with_approval(
                &lease,
                &reservation,
                &proposal,
                &manifest_artifact,
                &approval,
            )
            .unwrap();
        store
            .claim_next_task_for_workload(
                "fixture",
                now,
                Duration::minutes(1),
                TaskWorkload::Session,
            )
            .unwrap()
            .unwrap()
            .permit
    }

    /// WHY THIS TEST EXISTS (Task #3): A SOXX buy can fill before the QQQ sell,
    /// taking gross exposure to 350000.6 ppm against the frozen 350000 ppm cap.
    /// Do not weaken or delete this assertion.
    #[test]
    fn evaluate_blocks_sub_ppm_gross_excess_from_buy_before_sale() {
        use akzio_domain::{
            ClaimStance, DecisionValidity, EvidenceGround, HorizonDecisionTrace,
            InvestmentLogicTrace, LifecycleEventType, ResearchClaim, DOMAIN_SCHEMA_VERSION,
        };

        let now = Utc::now();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/execution-risk-tests")
            .join(RunId::new().0);
        let store = Store::open(root).unwrap();
        let runtime =
            ExecutionRuntime::new(store.clone(), Default::default(), Default::default()).unwrap();
        let mut frozen = policy_with_positive_calibration_bin(now);
        frozen.max_gross_weight = WeightPpm(350_000);
        let policy_hash = install_policy(&store, &frozen, now);
        let permit = reserve_approved_execution(&store, &policy_hash, now);
        let mut account = fixtures::account(
            now,
            BTreeMap::from([(
                Asset::Qqq,
                Position {
                    quantity_micros: 1_250_000,
                    market_value: MoneyMicros(125_000_600),
                },
            )]),
        );
        account.equity = MoneyMicros(1_000_000_000);
        account.buying_power = account.equity;
        let broker_session = now.date_naive().to_string();
        let mut quotes = fixtures::quotes(now, 99_999_999, 100_000_001);
        let quote = quotes.quotes[&Asset::Tqqq];
        quotes.quotes = BTreeMap::from([(Asset::Qqq, quote), (Asset::Soxx, quote)]);
        let mut clock = fixtures::clock(now);
        account.broker_session = broker_session.clone();
        quotes.broker_session = broker_session.clone();
        clock.broker_session = broker_session;

        let persist = |kind, blob, sources| {
            let lifecycle = match kind {
                ArtifactKind::ContextManifest | ArtifactKind::Claim => ArtifactLifecycle::RunScoped,
                _ => ArtifactLifecycle::Canonical,
            };
            let artifact = Artifact::new(
                kind,
                blob,
                "fixture.execution",
                lifecycle,
                crate::trusted_execution_provenance(&permit, now),
                Some(permit.artifact_origin()),
                sources,
                now,
            )
            .unwrap();
            store
                .write_task_artifact(
                    &permit,
                    &artifact,
                    LifecycleEventType::ArtifactCommitted,
                    now,
                )
                .unwrap();
            artifact_ref(&artifact)
        };
        let manifest = persist(
            ArtifactKind::ContextManifest,
            store
                .stage_json(&ContextManifestPayload {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    contract_hash: ContentHash::of_bytes(b"gate fixture synth contract"),
                    selections: vec![],
                    quarantined: vec![],
                    total_bytes: 0,
                    projected_bytes: None,
                    estimated_tokens: 0,
                    input_hash: ContentHash::of_bytes(b"execution risk evidence"),
                })
                .unwrap(),
            vec![],
        );
        let snapshot = |blob| persist(ArtifactKind::NormalizedEvidence, blob, vec![]);
        let quotes = snapshot(store.stage_json(&quotes).unwrap());
        let claim = persist(
            ArtifactKind::Claim,
            store
                .stage_json(&ResearchClaim {
                    schema_version: DOMAIN_SCHEMA_VERSION,
                    topic: "SOXX".into(),
                    statement: "Shift the target to SOXX".into(),
                    horizon: DecisionHorizon::T1,
                    stance: ClaimStance::Bullish,
                    materiality_ppm: WeightPpm::SCALE,
                    confidence_ppm: WeightPpm::SCALE,
                    grounds: vec![EvidenceGround {
                        evidence: quotes.clone(),
                        support: "Frozen executable SOXX quote".into(),
                        role: Default::default(),
                        assets: BTreeSet::from([Asset::Soxx]),
                        domain: None,
                    }],
                    evidence_gaps: vec![],
                })
                .unwrap(),
            vec![quotes.clone()],
        );
        let mut decision = fixtures::decision_context(now, 0);
        decision.decision_policy_hash = policy_hash;
        decision.portfolio_risk.risk_model_hash = Some(
            content_hash_json(&serde_json::to_value(&frozen.portfolio_risk_model).unwrap())
                .unwrap(),
        );
        decision
            .target
            .weights
            .insert(Asset::Soxx, WeightPpm(225_225));
        decision.run_id = permit.run_id.clone();
        decision.claims = vec![claim.clone()];
        decision.validity = Some(DecisionValidity {
            evidence_cutoff: now,
            generated_at: now,
            valid_until: now + Duration::seconds(60),
            maximum_execution_delay_ms: 60_000,
            market_state_hash: manifest.artifact_id.0.clone(),
        });
        decision.horizon_trace = Some(HorizonDecisionTrace {
            sleeves: vec![],
            conflicts: vec![],
        });
        decision.investment_logic = Some(InvestmentLogicTrace {
            observed_events: vec![],
            accepted_premises: vec![claim.clone()],
            rejected_premises: vec![],
            alternatives_considered: vec![],
            selected_action_hash: content_hash_json(
                &serde_json::to_value(&decision.target).unwrap(),
            )
            .unwrap(),
            invalidation_conditions: vec!["Quote expires".into()],
            quality: Default::default(),
            consensus_diversity: None,
            stage_latencies: Default::default(),
        });
        let input = ExecutionGateInput {
            decision_context: persist(
                ArtifactKind::DecisionContext,
                store.stage_json(&decision).unwrap(),
                vec![manifest, claim],
            ),
            account_snapshot: Some(snapshot(store.stage_json(&account).unwrap())),
            quote_snapshot: Some(quotes),
            market_clock_snapshot: Some(snapshot(store.stage_json(&clock).unwrap())),
            quote_validation_error: None,
            pretrade_safety: Some(pretrade(now)),
            permit,
            now,
        };
        let output = runtime.evaluate(&input).unwrap();
        let plan: crate::ExecutionPlan = runtime
            .read_payload(
                output
                    .execution_plan
                    .as_ref()
                    .expect("the allocation must be assessed"),
            )
            .unwrap();
        assert_eq!(plan.orders.len(), 2);
        for (asset, side, qty, limit_price) in [
            (Asset::Qqq, crate::OrderSide::Sell, "1.250000", "99.900000"),
            (Asset::Soxx, crate::OrderSide::Buy, "2.250000", "100.100000"),
        ] {
            let order = plan
                .orders
                .iter()
                .find(|order| order.asset == asset)
                .unwrap();
            assert_eq!(order.side, side);
            let request = crate::paper::order_request(order, "wire-risk-fixture").unwrap();
            assert_eq!(request["qty"], qty);
            assert_eq!(request["limit_price"], limit_price);
        }
        let verdict: ExecutionVerdict = runtime.read_payload(&output.verdict).unwrap();
        let ExecutionVerdict::NoOrder { no_order } = verdict else {
            panic!("the 350000.6 ppm buy-before-sale corner must be rejected, got {verdict:?}");
        };
        assert_eq!(no_order.blockers, [HardBlocker::FactorLimit]);
    }

    /// WHY THIS TEST EXISTS (Task #3): A protected sell limit can remove more
    /// shares than the notional projection suggests at the frozen account mark.
    /// Do not weaken or delete this assertion.
    #[test]
    fn protected_sell_uses_wire_shares_at_the_frozen_mark() {
        let mut runtime = runtime(150_000);
        runtime.allocation = AllocationRuntime::new(ExecutionPolicy {
            limit_protection_bps: 100,
            ..Default::default()
        })
        .unwrap();
        let now = fixtures::now();
        let mut frozen = policy_with_positive_calibration_bin(now);
        for (left, right) in [(Asset::Qqq, Asset::Soxx), (Asset::Soxx, Asset::Qqq)] {
            frozen
                .portfolio_risk_model
                .covariance_ppm_squared
                .get_mut(&left)
                .unwrap()
                .insert(right, -3_400_000_000);
        }
        install_policy(&runtime.store, &frozen, now);
        let mut allocation = allocation_input(
            &frozen,
            BTreeMap::from([
                (
                    Asset::Qqq,
                    Position {
                        quantity_micros: 2_000_000,
                        market_value: MoneyMicros(200_000_000),
                    },
                ),
                (
                    Asset::Soxx,
                    Position {
                        quantity_micros: 3_000_000,
                        market_value: MoneyMicros(300_000_000),
                    },
                ),
            ]),
            &[(Asset::Qqq, 5_000), (Asset::Soxx, 300_000)],
        );
        allocation.account.equity = MoneyMicros(1_000_000_000);
        for quote in allocation.quotes.quotes.values_mut() {
            quote.bid = MoneyMicros(100_000_000);
            quote.ask = MoneyMicros(100_000_001);
        }
        let plan = allocate(&runtime, &allocation);
        assert_eq!(plan.orders.len(), 1);
        assert_eq!(plan.orders[0].side, crate::OrderSide::Sell);
        assert_eq!(plan.orders[0].notional, MoneyMicros(195_000_000));
        assert_eq!(plan.target.weights[&Asset::Qqq], WeightPpm(5_000));
        let request = crate::paper::order_request(&plan.orders[0], "wire-risk-fixture").unwrap();
        assert_eq!(request["limit_price"], "99.000000");
        assert_eq!(request["qty"], "1.969696");

        let mut evidence = pretrade(now);
        evidence
            .dependency_closure
            .dependencies
            .get_mut(&DependencyKind::ModelProvider)
            .unwrap()
            .status = DependencyHealthStatus::Stale;
        let mut blockers = BTreeSet::new();
        let (_, safety) = runtime
            .assess_allocated_plan(&allocation, &plan, Some(&evidence), &mut blockers)
            .unwrap();
        assert!(safety.unwrap().risk_increasing);
        assert_eq!(blockers, BTreeSet::from([HardBlocker::DependencyDegraded]));
    }
}
