// 文件导读：本文件实现只读 readiness/preflight、风险限制写入、Outcome 数据集收集、
// Policy candidate build/validate/activate 和 evidence audit。它坚持 SQL Store/CAS 为
// 权威：成熟 Outcome 不是 active Policy，candidate 不是激活，Paper NoOrder 也不是校准
// 完成；CLI 的报告只反映已持久化事实和明确缺口。
// Rust 机制：泛型 `persist_calibration_artifact<T: Serialize>` 约束输入可序列化；闭包和
// 迭代器按 Artifact DAG 聚合样本；`Option`/`Result` 区分 no_schedule、pending、blocked
// 和真实错误；测试用 cfg(test) 只验证离线 Store 行为。

use akzio_domain::{
    Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef,
    Decision, DecisionContext, DecisionHorizon, MoneyMicros, Outcome, OutcomeHorizon,
    OutcomeSchedule,
};
use akzio_execution::{
    HistoricalForecastProvenance, HistoricalForecastSample, HistoricalPricePoint,
    HistoricalPriceSeries, OfflineCalibrationInput, OfflineRiskLimits,
    build_offline_decision_policy,
};
use akzio_ingest::{EvidenceAdapterError, NormalizedEvidencePayload, parse_daily_bars};
use chrono::{DateTime, Duration as ChronoDuration};

fn handle_calibration(command: &CalibrationCommand, config_path: &Path) -> Result<()> {
    // 统一分派校准生命周期：readiness/preflight 只读，collect/build 生成候选
    // Artifact，activate 才会改变 Store 的 active head；任何候选生成都不会自动激活。
    match command {
        CalibrationCommand::Readiness { store, min_samples } => {
            calibration_readiness(store, *min_samples, config_path)
        }
        CalibrationCommand::Preflight { scratch } => {
            // Preflight 只验证临时 Store 中已持久化的 Policy 与当前身份，失败时
            // 返回可消费的阻断 JSON，不启动模型或执行链。
            let mut config = read_config_file(config_path)?;
            resolve_model_configuration(&mut config)?;
            let store = Store::open(scratch)?;
            let loaded = match load_decision_policy_from_store(&config, &store) {
                Ok(loaded) => loaded,
                Err(_) => {
                    return print_json(&serde_json::json!({"decision_capable":false,
                    "calibration_readiness_hint":"akzio calibration readiness --store <canonical-store> --min-samples 30",
                    "decision_policy_status":"invalid_store_policy_or_identity", "decision_policy_input_hash":null,
                    "policy_source":"canonical_store_active_head", "llm_calls":0,
                    "reason":"active DecisionPolicy Store read or identity validation failed"}));
                }
            };
            let (ready, contract_match) = decision_policy_preflight(&loaded, &store)?;
            print_json(&serde_json::json!({"decision_capable":ready,
                "calibration_readiness_hint":"akzio calibration readiness --store <canonical-store> --min-samples 30",
                "research_capable":ready || loaded.status == "store_active_head_missing",
                "position_plan_mode":if ready {"decision_capable"} else if loaded.status == "store_active_head_missing" {"research_only_incomplete"} else {"blocked_before_llm"},
                "decision_policy_status":if ready {"ready_for_current_decision"} else if loaded.policy.decision_capable() {"contract_mismatch"} else {&loaded.status},
                "decision_policy_input_hash":loaded.input_hash,"contract_match":contract_match,
                "policy_artifact_id":loaded.artifact_id,"policy_source":loaded.source,"llm_calls":0}))
        }
        CalibrationCommand::SetRiskLimits { store, limits } => {
            // 风险限制先在 CLI 内校验，再以 canonical Artifact 写入 Store；返回 stored
            // 只表示已保存，operator 仍需后续 collect/build/inspect/validate/activate。
            let limits = OfflineRiskLimits::from(limits);
            limits.validate()?;
            let store = Store::open(store)?;
            let artifact = persist_calibration_artifact(
                &store,
                ArtifactKind::CalibrationRiskLimits,
                &limits,
                vec![],
                Utc::now(),
            )?;
            print_json(
                &serde_json::json!({"status":"stored", "risk_limits_artifact":artifact.artifact_id,
                "activation_requires_operator":true}),
            )
        }
        CalibrationCommand::Build { store, dataset } => {
            // Build 只消费指定的 canonical 数据集并持久化新的 DecisionPolicy 候选，
            // 之后的完整性检查不等同于 active head 已更新。
            let store = Store::open(store)?;
            let dataset_artifact =
                calibration_artifact(&store, dataset, ArtifactKind::CalibrationDataset)?;
            let dataset: StoredCalibrationDataset =
                serde_json::from_slice(&store.read_blob(&dataset_artifact.blob)?)?;
            let policy = build_offline_decision_policy(&dataset.input, Utc::now())
                .context("fit stored historical calibration dataset")?;
            let artifact = persist_calibration_artifact(
                &store,
                ArtifactKind::DecisionPolicy,
                &policy,
                vec![],
                policy.provenance.created_at,
            )?;
            store.verify_integrity()?;
            print_json(&serde_json::json!({
                "status":"built", "dataset_artifact":dataset_artifact.artifact_id,
                "policy_artifact":artifact.artifact_id, "policy_hash":policy.provenance.output_hash,
                "input_hash":policy.provenance.input_hash,
                "training_samples":policy.provenance.sample_count,
                "source_runs":policy.provenance.source_runs,
                "decision_capable":policy.policy.decision_capable(),
                "activated":false, "activation_requires_operator":true,
                "next_step":"inspect -> validate -> activate using --store and the stored Artifact ID"
            }))
        }
        CalibrationCommand::Activate { store, policy } => {
            // 激活是本分派器中唯一会切换 active DecisionPolicy 的路径；先拒绝隔离 Store，
            // 再检查模型身份、Artifact 和当前 Synthesizer Contract 的精确一致性。
            let mut config = read_config_file(config_path)?;
            resolve_model_configuration(&mut config)?;
            let store = Store::open(store)?;
            if store.debug_environment()?.is_some() {
                bail!("cannot activate canonical calibration in an isolated Debug Store");
            }
            let artifact = calibration_artifact(&store, policy, ArtifactKind::DecisionPolicy)?;
            let bytes = store.read_blob(&artifact.blob)?;
            let decoded = decode_loaded_decision_policy(&config, &bytes, None)?;
            let contract = store
                .active_contract(&akzio_domain::ContractPurpose::new(
                    akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID,
                )?)?
                .context("Store has no active Synthesizer Contract")?;
            if decoded.contract_hash.as_ref() != Some(&contract.contract.contract_hash) {
                bail!("refusing to activate a policy for a different Synthesizer Contract");
            }
            let active = activate_decision_policy_artifact(&store, &artifact, Utc::now())?;
            store.verify_integrity()?;
            print_json(
                &serde_json::json!({"status":"active","policy_hash":active.descriptor.policy_hash,
                "policy_artifact_id":active.artifact.artifact_id,"policy_input_hash":active.descriptor.envelope_hash,
                "activated_at":active.activated_at,"store_integrity":"passed"}),
            )
        }
        CalibrationCommand::Bootstrap {
            source_store,
            target_store,
        } => {
            // Bootstrap 只从源 Store 复制已有 active policy；源 Store 没有 active head
            // 时报告 unconfigured，不会凭空生成或激活默认 Policy。
            let (source, source_store_created) = Store::open_existing_or_initialize(source_store)?;
            source.verify_integrity()?;
            let target = Store::open(target_store)?;
            let active = target.bootstrap_active_decision_policy_from(&source, Utc::now())?;
            target.verify_integrity()?;
            print_json(&match active {
                Some(active) => {
                    serde_json::json!({"status":"bootstrapped","policy_hash":active.descriptor.policy_hash,
                    "policy_artifact_id":active.artifact.artifact_id,"policy_input_hash":active.descriptor.envelope_hash,
                    "source_store_created":source_store_created,"store_integrity":"passed"})
                }
                None => serde_json::json!({"status":"unconfigured",
                    "source_store_created":source_store_created,"store_integrity":"passed"}),
            })
        }
        CalibrationCommand::Collect {
            store,
            risk_limits,
            min_samples,
            training_start,
            training_end,
        } => collect_calibration_dataset(
            store,
            risk_limits,
            *min_samples,
            training_start.as_deref(),
            training_end.as_deref(),
            config_path,
        ),
        CalibrationCommand::Validate { store, policy } => {
            // Validate 只对指定 CAS Artifact 做严格解码和能力判断，不写 active head。
            let store = Store::open_existing(store)?;
            let stored = calibration_artifact(&store, policy, ArtifactKind::DecisionPolicy)?;
            let artifact = DecisionPolicyArtifact::decode_strict(&store.read_blob(&stored.blob)?)?;
            print_json(
                &serde_json::json!({"status":"valid", "policy_artifact":stored.artifact_id,
                "decision_capable":artifact.policy.decision_capable(),
                "policy_hash":artifact.provenance.output_hash, "activation_requires_operator":true}),
            )
        }
        CalibrationCommand::Inspect { store, artifact } => {
            // Inspect 只展示校准相关 Artifact 的元数据和正文；允许的 kind 仍受这里的
            // canonical 检查限制，展示本身不代表候选已通过激活流程。
            let store = Store::open_existing(store)?;
            let stored = store.artifact(&ArtifactId(ContentHash::new(artifact.clone())?))?;
            if !matches!(
                stored.kind,
                ArtifactKind::DecisionPolicy
                    | ArtifactKind::CalibrationDataset
                    | ArtifactKind::CalibrationRiskLimits
            ) {
                bail!("not a calibration Artifact");
            }
            let payload: serde_json::Value =
                serde_json::from_slice(&store.read_blob(&stored.blob)?)?;
            print_json(&serde_json::json!({"artifact":stored, "payload":payload,
                "activation_requires_operator":true}))
        }
    }
}

/// Identity is checked even for an otherwise decision-capable stored policy.
fn decision_policy_preflight(loaded: &LoadedDecisionPolicy, store: &Store) -> Result<(bool, Option<bool>)> {
    // 只有 Policy 能力、输入哈希、Artifact 身份和 persisted status 同时齐全时，
    // 才比较当前 Synthesizer Contract；返回值仅是资格投影，不改变 Store。
    let eligible = loaded.policy.decision_capable()
        && loaded.input_hash.is_some()
        && loaded.artifact_id.is_some()
        && loaded.status == "ready_for_current_decision";
    if !eligible { return Ok((false, None)); }
    let matches = loaded.contract_hash.as_ref()
        == Some(&akzio_daemon::canonical_synthesizer_contract_hash(store)?);
    Ok((matches, Some(matches)))
}

fn decision_policy_status(policy: &DecisionPolicy) -> &'static str {
    // 将已解码 Policy 的静态能力投影为 CLI 状态；这不是 active head 的存在性判断。
    if policy.decision_capable() {
        "ready_for_current_decision"
    } else if policy.asset_calibrations.is_empty() {
        "validated_but_no_asset_calibration"
    } else {
        "validated_but_insufficient_samples"
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredCalibrationDataset {
    input: OfflineCalibrationInput,
    report: serde_json::Value,
}

fn calibration_artifact(store: &Store, id: &str, kind: ArtifactKind) -> Result<Artifact> {
    // 校准命令只接受指定的 CAS ID，并要求类型和生命周期都属于 canonical，避免把
    // 草稿、隔离或其他业务 Artifact 当成正式输入。
    let artifact = store.artifact(&ArtifactId(ContentHash::new(id.to_owned())?))?;
    if artifact.kind != kind || artifact.lifecycle != ArtifactLifecycle::Canonical {
        bail!("expected canonical {kind:?} Artifact");
    }
    Ok(artifact)
}

fn persist_calibration_artifact<T: serde::Serialize>(
    store: &Store,
    kind: ArtifactKind,
    payload: &T,
    sources: Vec<ArtifactRef>,
    now: DateTime<Utc>,
) -> Result<Artifact> {
    // 先把不可变正文放入 CAS，再写入校准 Artifact 索引；这里不触碰 active policy head。
    let artifact = Artifact::new(
        kind,
        store.stage_json(payload)?,
        "calibration.sql",
        ArtifactLifecycle::Canonical,
        ArtifactProvenance {
            source_family: "akzio.offline_calibration".into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        None,
        sources,
        now,
    )?;
    store.write_calibration_artifact(&artifact)?;
    Ok(artifact)
}

/// Classification depends only on persisted facts, never elapsed calendar days.
fn classify_calibration_run(
    schedule: Option<&OutcomeSchedule>,
    baseline_snapshot_missing: bool,
    sealed_labels: Option<std::result::Result<(), String>>,
    completed_horizons: &BTreeSet<OutcomeHorizon>,
) -> serde_json::Value {
    // 分类优先使用已封存标签，其次报告不可修复的基线缺失，再区分无计划和仍待完成的
    // Outcome 窗口；等待 T+1/T+3/T+5 不依据自然日猜测成熟度。
    if let Some(labels) = sealed_labels {
        return match labels {
            Ok(()) => serde_json::json!({"status":"sealed"}),
            Err(reason) => serde_json::json!({"status":"blocked", "reason":reason}),
        };
    }
    if baseline_snapshot_missing {
        return serde_json::json!({
            "status":"blocked", "reason":"baseline_snapshot_missing",
            "sealable":false,
            "detail":"Frozen ExecutionContext lacks baseline quote/account snapshots; waiting cannot repair this run. Closed-market acquisition is one possible cause, not inferred from missing snapshots alone.",
            "remedy":"Rerun during an open trading session with valid baseline snapshots; do not rewrite historical CAS."
        });
    }
    let Some(schedule) = schedule else {
        return serde_json::json!({"status":"no_schedule", "reason":"decision_not_evaluated"});
    };
    let missing = schedule
        .due_horizons(OutcomeHorizon::T5.trading_days())
        .into_iter()
        .filter(|horizon| !completed_horizons.contains(horizon))
        .map(|horizon| {
            serde_json::json!({
                "horizon":horizon, "required_common_trading_sessions":horizon.trading_days()
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "status":"pending", "baseline_trading_day":schedule.baseline_trading_day,
        "missing_horizons":missing,
        "timing_basis":"Persisted Outcome windows; T+1/T+3/T+5 require actual completed sessions common to all four assets, not calendar days."
    })
}

fn calibration_readiness_gap(rows: &[serde_json::Value], min_samples: u32) -> serde_json::Value {
    // Readiness 以已封存的 Paper Run 数量计算四资产/三期限的统一槽位缺口；
    // 这是收集进度，不代表已有可激活 Policy 或已满足模型/Contract 身份条件。
    let mature_runs = rows.iter().filter(|row| row["status"] == "sealed").count();
    let remaining = (min_samples as usize).saturating_sub(mature_runs);
    let mut counts = BTreeMap::new();
    for asset in Asset::EXECUTABLE {
        for horizon in OutcomeHorizon::ALL {
            counts.insert(format!("{}:{horizon:?}", asset.symbol()), mature_runs);
        }
    }
    serde_json::json!({
        "min_samples":min_samples, "mature_runs":mature_runs,
        "remaining_mature_runs":remaining, "slot_counts":counts,
        "next_step":if remaining > 0 {"collect_real_outcomes"} else {"collect"},
        "build_readiness":"After collect persists a dataset, build that SQL Artifact. Maturity alone does not validate model/Contract identity, price panel, training window or risk limits."
    })
}

fn calibration_readiness(store_path: &Path, min_samples: u32, config_path: &Path) -> Result<()> {
    // 保持命令边界简单：完整报告由只读的 readiness_report 生成后直接打印。
    print_json(&calibration_readiness_report(store_path, min_samples, config_path)?)
}

fn calibration_readiness_report(
    store_path: &Path,
    min_samples: u32,
    config_path: &Path,
) -> Result<serde_json::Value> {
    // Readiness 只打开既有 Store，扫描有限数量的 canonical Paper Decision，并把
    // 隔离 Debug 运行明确排除在正式校准之外；它不会创建样本、候选 Policy 或 active head。
    if min_samples == 0 {
        bail!("calibration readiness requires min_samples > 0");
    }
    let store = Store::open_existing(store_path)?;
    let isolated = store.debug_environment()?.is_some();
    let policy_status = if store.active_decision_policy()?.is_none() {
        "store_active_head_missing".to_owned()
    } else {
        // 有 active head 时，状态仍需结合当前配置模型和 Store 中冻结的 Synthesizer
        // Contract；这里仅读取 active heads，不能用会引导启动的 catalogue helper。
        let mut config = read_config_file(config_path)?;
        resolve_model_configuration(&mut config)?;
        // Read persisted heads only. The daemon catalogue helper may bootstrap
        // Contracts and therefore must never be used by this read-only command.
        let contract = store.active_contract(&akzio_domain::ContractPurpose::new(
            akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID,
        )?)?;
        match load_decision_policy_from_store(&config, &store) {
            Ok(loaded) if loaded.policy.decision_capable() => match contract {
                None => "store_active_contract_missing".to_owned(),
                Some(contract)
                    if loaded.contract_hash.as_ref() == Some(&contract.contract.contract_hash) =>
                {
                    loaded.status
                }
                Some(_) => "contract_mismatch".to_owned(),
            },
            Ok(loaded) => loaded.status,
            Err(_) => "invalid_store_policy_or_identity".to_owned(),
        }
    };
    let artifacts = store.recent_artifacts_by_kind(ArtifactKind::Decision, 500)?;
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    // 每个 Run 只取一个 Decision 作为 readiness 入口；重复 Decision 或非 Paper purpose
    // 不进入成熟度统计，单个 Run 的细节由 calibration_run_readiness 继续校验。
    for artifact in &artifacts {
        let Some(run_id) = artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
        else {
            continue;
        };
        if store.run_purpose(run_id)? != RunPurpose::Paper || !seen.insert(run_id.clone()) {
            continue;
        }
        let mut row = if isolated {
            serde_json::json!({"status":"blocked", "reason":"isolated_debug_store",
                "detail":"Debug and simulated runs cannot become canonical calibration samples by waiting or completing Outcome."})
        } else {
            calibration_run_readiness(&store, artifact, run_id)?
        };
        row["run_id"] = serde_json::to_value(run_id)?;
        rows.push(row);
    }
    let mut stored_artifacts = Vec::new();
    // 展示最近少量校准 Artifact 便于 operator 找到后续命令的输入 ID；这是观察窗口，
    // 不是对 Store 中不存在的 Artifact 的推断。
    for kind in [
        ArtifactKind::CalibrationRiskLimits,
        ArtifactKind::CalibrationDataset,
        ArtifactKind::DecisionPolicy,
    ] {
        for artifact in store.recent_artifacts_by_kind(kind, 20)? {
            stored_artifacts.push(serde_json::json!({"artifact_id":artifact.artifact_id,
                "kind":artifact.kind, "created_at":artifact.created_at}));
        }
    }
    let mut gap = calibration_readiness_gap(&rows, min_samples);
    if isolated {
        gap["next_step"] = serde_json::json!("use_canonical_store");
    }
    Ok(serde_json::json!({
        "store":store_path, "decision_policy_status":policy_status,
        "store_scope":if isolated {"isolated_debug"} else {"canonical"},
        "calibration_eligible":!isolated,
        "calibration_blocked_reason":if isolated {Some("isolated_debug_store")} else {None},
        "stored_calibration_artifacts":stored_artifacts,
        "stored_artifact_limit_per_kind":20,
        "policy_status_scope":"configured_model_and_persisted_active_contract",
        "activation_requires_operator":true,
        "operator_sequence":"collect -> build -> inspect -> validate -> activate; inspect, validate and activate must be explicitly performed by the operator",
        "scan_limit":500, "decisions_scanned":artifacts.len(),
        "scan_limit_reached":artifacts.len() == 500,
        "gap":gap, "runs":rows
    }))
}

fn calibration_run_readiness(
    store: &Store,
    artifact: &Artifact,
    run_id: &RunId,
) -> Result<serde_json::Value> {
    // 沿 Decision -> Context -> Schedule/Execution -> Outcome 的血缘读取事实，分别验证
    // canonical 生命周期、冻结基线、共同交易日标签和窗口完成度，最后才分类该 Run。
    if artifact.lifecycle != ArtifactLifecycle::Canonical {
        return Ok(serde_json::json!({"status":"blocked", "reason":"decision_not_canonical"}));
    }
    let decision: Decision = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
    decision.validate()?;
    let schedule_artifact = store.outcome_schedule_for_run(run_id)?;
    let schedule: Option<OutcomeSchedule> = schedule_artifact
        .as_ref()
        .map(|a| -> Result<_> { Ok(serde_json::from_slice(&store.read_blob(&a.blob)?)?) })
        .transpose()?;
    if schedule
        .as_ref()
        .is_some_and(|s| s.decision.artifact_id != artifact.artifact_id)
    {
        return Ok(serde_json::json!({"status":"blocked", "reason":"schedule_decision_mismatch"}));
    }
    let execution_artifact = if let Some(schedule) = &schedule {
        Some(store.artifact(&schedule.execution_context.artifact_id)?)
    } else {
        store
            .run_artifacts_by_kind(run_id, ArtifactKind::ExecutionContext)?
            .pop()
    };
    let execution: Option<akzio_domain::ExecutionContext> = execution_artifact
        .as_ref()
        .map(|a| -> Result<_> { Ok(serde_json::from_slice(&store.read_blob(&a.blob)?)?) })
        .transpose()?;
    let missing_baseline = execution
        .as_ref()
        .is_some_and(|c| c.quote_snapshot.is_none() || c.account_snapshot.is_none());
    let sealed = store.outcome_for_run(run_id)?;
    let sealed_labels = sealed
        .as_ref()
        .map(|a| -> Result<std::result::Result<(), String>> {
            let outcome: Outcome = serde_json::from_slice(&store.read_blob(&a.blob)?)?;
            if a.lifecycle != ArtifactLifecycle::Canonical {
                return Ok(Err("outcome_not_canonical".into()));
            }
            if let Err(error) = outcome.validate_sealed() {
                return Ok(Err(error.to_string()));
            }
            let Some(schedule) = &schedule else {
                return Ok(Err("schedule_missing".into()));
            };
            if schedule_artifact.as_ref().map(|a| &a.artifact_id)
                != Some(&outcome.schedule.artifact_id)
            {
                return Ok(Err("outcome_schedule_mismatch".into()));
            }
            let bars = match outcome_bars(store, &outcome) {
                Ok(bars) => bars,
                Err(error) => return Ok(Err(format!("outcome_bars_unusable: {error}"))),
            };
            Ok(candidate_has_all_labels(
                &decision, schedule, &outcome, &bars,
            ))
        })
        .transpose()?;
    // 未封存时仍收集同一 Schedule 下已经存在的窗口，供 pending 报告精确列出缺口；
    // 这不会把部分 Outcome 当成 sealed 样本。
    let mut completed = BTreeSet::new();
    if let Some(schedule_artifact) = &schedule_artifact {
        for artifact in store.run_artifacts_by_kind(run_id, ArtifactKind::Outcome)? {
            let outcome: Outcome = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            outcome.validate()?;
            if outcome.schedule.artifact_id == schedule_artifact.artifact_id {
                completed.extend(outcome.windows.iter().map(|window| window.horizon));
            }
        }
    }
    Ok(classify_calibration_run(
        schedule.as_ref(),
        missing_baseline,
        sealed_labels,
        &completed,
    ))
}

#[cfg(test)]
mod calibration_readiness_tests {
    use super::*;

    use akzio_domain::WeightPpm;
    use akzio_execution::{ForecastCalibrationScope, AssetRiskCalibration, FrozenForecastCalibration, ForecastCalibrationBin, PortfolioRiskModel};

    #[test]
    fn isolated_store_readiness_cannot_recommend_canonical_collection() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.akzio/policy-readiness-tests").join(RunId::new().0);
        let store = Store::open(&path).unwrap();
        let canonical = calibration_readiness_report(&path, 30, Path::new("unused.toml")).unwrap();
        assert_eq!(canonical["calibration_eligible"], true);
        assert_eq!(canonical["gap"]["next_step"], "collect_real_outcomes");
        store.configure_debug_environment(true).unwrap();
        let report = calibration_readiness_report(&path, 30, Path::new("unused.toml")).unwrap();
        assert_eq!(report["calibration_eligible"], false);
        assert_eq!(report["calibration_blocked_reason"], "isolated_debug_store");
        assert_eq!(report["gap"]["next_step"], "use_canonical_store");
        assert_eq!(report["gap"]["mature_runs"], 0);
        assert!(store.active_decision_policy().unwrap().is_none());
    }

    fn policy_with_positive_calibration_bin(now: chrono::DateTime<Utc>) -> DecisionPolicy {
        let scope = ForecastCalibrationScope {
            model_id: "test-model".to_owned(),
            model_version_hash: ContentHash::of_bytes(b"test-model-version"),
            regime: "all".to_owned(),
        };
        let calibration = AssetRiskCalibration {
            sample_count: 1,
            brier_score_ppm: 0,
            annualized_volatility_ppm: 100_000,
            beta_ppm: 100_000,
            max_capital_weight: WeightPpm(500_000),
            liquidity_weight_cap: WeightPpm(500_000),
            expected_shortfall_ppm: 10_000,
            gap_loss_ppm: 10_000,
            daily_reset_decay_ppm: 0,
        };
        let mut covariance_ppm_squared = BTreeMap::new();
        for left in Asset::EXECUTABLE {
            let mut row = BTreeMap::new();
            for right in Asset::EXECUTABLE {
                row.insert(
                    right,
                    if left == right {
                        10_000_000_000
                    } else {
                        1_000_000_000
                    },
                );
            }
            covariance_ppm_squared.insert(left, row);
        }
        let forecast_calibrations = Asset::EXECUTABLE
            .into_iter()
            .flat_map(|asset| {
                DecisionHorizon::ALL.into_iter().map({
                    let scope = scope.clone();
                    move |horizon| FrozenForecastCalibration {
                        scope: scope.clone(),
                        asset,
                        horizon,
                        sample_count: 1,
                        mean_brier_score_ppm: 0,
                        fit_dataset_hash: ContentHash::of_bytes(b"test-fit"),
                        trained_through: now - chrono::Duration::days(2),
                        frozen_at: now - chrono::Duration::days(1),
                        bins: vec![ForecastCalibrationBin {
                            raw_probability_min_ppm: 0,
                            raw_probability_max_ppm: WeightPpm::SCALE,
                            sample_count: 1,
                            calibrated_probability_ppm: 700_000,
                            calibrated_expected_alpha_ppm: 10_000,
                        }],
                    }
                })
            })
            .collect();
        DecisionPolicy {
            min_confidence_ppm: 250_000,
            max_gross_weight: WeightPpm(500_000),
            horizon_weights: BTreeMap::from([
                (DecisionHorizon::T1, WeightPpm(333_333)),
                (DecisionHorizon::T3, WeightPpm(333_333)),
                (DecisionHorizon::T5, WeightPpm(333_334)),
            ]),
            maximum_execution_delay_ms: 300_000,
            minimum_process_quality_ppm: 900_000,
            min_probability_edge_ppm: 50_000,
            min_calibration_samples: 1,
            max_brier_score_ppm: 250_000,
            active_forecast_calibration: Some(scope),
            forecast_calibrations,
            target_annualized_volatility_ppm: 150_000,
            max_portfolio_beta_ppm: 500_000,
            asset_calibrations: Asset::EXECUTABLE
                .into_iter()
                .map(|asset| (asset, calibration.clone()))
                .collect(),
            portfolio_risk_model: PortfolioRiskModel {
                version: "test-risk-v1".to_owned(),
                sample_count: 1,
                covariance_ppm_squared,
                max_expected_shortfall_ppm: 500_000,
                max_gap_loss_ppm: 500_000,
                max_leveraged_holding_days: 5,
            },
        }
    }

    #[test]
    fn old_policy_is_blocked_by_new_synthesizer_contract_without_activation() {
        let store = Store::open(Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.akzio/policy-contract-tests").join(RunId::new().0)).unwrap();
        let policy = policy_with_positive_calibration_bin(Utc::now());
        policy.validate().unwrap();
        assert!(policy.decision_capable());
        let current = akzio_daemon::canonical_synthesizer_contract_hash(&store).unwrap();
        let mut loaded = LoadedDecisionPolicy {
            policy, status: "ready_for_current_decision".into(),
            input_hash: Some(ContentHash::of_bytes(b"synthetic-old-policy-envelope")),
            artifact_id: Some(ArtifactId(ContentHash::of_bytes(b"synthetic-old-policy-artifact"))),
            contract_hash: Some(ContentHash::new("62f26b115dca5a93a3da9398f9b39d13985d1f65d64ee84771f052d44637cb84").unwrap()),
            source: "test_fixture",
        };
        let frozen = loaded.clone();
        assert_eq!(decision_policy_preflight(&loaded, &store).unwrap(), (false, Some(false)));
        assert_eq!(loaded.contract_hash, frozen.contract_hash);
        assert_eq!(loaded.policy, frozen.policy);
        assert!(store.active_decision_policy().unwrap().is_none());
        // Same complete calibration data becomes eligible only with the exact current identity.
        loaded.contract_hash = Some(current);
        assert_eq!(decision_policy_preflight(&loaded, &store).unwrap(), (true, Some(true)));
        assert!(store.active_decision_policy().unwrap().is_none());
    }

    fn schedule() -> OutcomeSchedule {
        let reference = |kind| ArtifactRef {
            artifact_id: ArtifactId(ContentHash::of_bytes(
                format!("readiness-{kind:?}").as_bytes(),
            )),
            kind,
        };
        OutcomeSchedule {
            schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
            outcome_id: akzio_domain::OutcomeId::new(),
            decision: reference(ArtifactKind::Decision),
            decision_context: reference(ArtifactKind::DecisionContext),
            execution_context: reference(ArtifactKind::ExecutionContext),
            execution: akzio_domain::OutcomeExecutionLineage::NoOrder {
                execution_verdict: reference(ArtifactKind::ExecutionVerdict),
            },
            baseline_trading_day: NaiveDate::from_ymd_opt(2026, 9, 18).unwrap(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn legacy_policy_file_configuration_and_file_cli_inputs_are_rejected() {
        let execution = "assets = [\"TQQQ\", \"QQQ\", \"SOXX\", \"SOXL\"]\n";
        assert!(toml::from_str::<ExecutionSettings>(execution).is_ok());
        assert!(
            toml::from_str::<ExecutionSettings>(&format!(
                "{execution}decision_policy_path = \"old.json\"\n"
            ))
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "akzio",
                "calibration",
                "build",
                "--input",
                "old.json",
                "--output",
                "policy.json"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "akzio",
                "calibration",
                "activate",
                "--store",
                "store",
                "--input",
                "policy.json"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "akzio",
                "calibration",
                "build",
                "--store",
                "store",
                "--dataset",
                "artifact-id"
            ])
            .is_ok()
        );
    }

    #[test]
    fn readiness_classifies_maturity_and_frozen_baseline_without_calendar_guessing() {
        let schedule = schedule();
        let completed = BTreeSet::from([OutcomeHorizon::T1]);
        let pending = classify_calibration_run(Some(&schedule), false, None, &completed);
        assert_eq!(pending["status"], "pending");
        assert_eq!(pending["baseline_trading_day"], "2026-09-18");
        assert_eq!(
            pending["missing_horizons"],
            serde_json::json!([
                {"horizon":"t3", "required_common_trading_sessions":3},
                {"horizon":"t5", "required_common_trading_sessions":5}
            ])
        );
        let blocked = classify_calibration_run(Some(&schedule), true, None, &completed);
        assert_eq!(blocked["reason"], "baseline_snapshot_missing");
        assert_eq!(blocked["sealable"], false);
        assert_eq!(
            classify_calibration_run(None, false, None, &completed)["status"],
            "no_schedule"
        );
        let sealed = classify_calibration_run(Some(&schedule), false, Some(Ok(())), &completed);
        assert_eq!(sealed["status"], "sealed");
        let invalid = classify_calibration_run(
            Some(&schedule),
            false,
            Some(Err("missing labels".into())),
            &completed,
        );
        assert_eq!(invalid["status"], "blocked");
        let rows = [pending, blocked, sealed.clone(), invalid];
        let gap = calibration_readiness_gap(&rows, 3);
        assert_eq!(gap["mature_runs"], 1);
        assert_eq!(gap["remaining_mature_runs"], 2);
        assert_eq!(gap["slot_counts"].as_object().unwrap().len(), 12);
        assert!(
            gap["slot_counts"]
                .as_object()
                .unwrap()
                .values()
                .all(|count| *count == 1)
        );
        assert_eq!(gap["next_step"], "collect_real_outcomes");
        let ready = calibration_readiness_gap(&[sealed.clone(), sealed], 1);
        assert_eq!(ready["remaining_mature_runs"], 0);
        assert_eq!(ready["next_step"], "collect");
        assert_eq!(
            calibration_readiness_gap(&[], 30)["remaining_mature_runs"],
            30
        );
    }
}

struct CollectedCalibrationCandidate {
    run_id: RunId,
    decision_artifact: akzio_domain::Artifact,
    decision: Decision,
    outcome: Outcome,
    schedule: OutcomeSchedule,
    bars: BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    provider_id: String,
    model_id: String,
    contract_hash: ContentHash,
}

fn collect_calibration_dataset(
    store_path: &Path,
    risk_limits_id: &str,
    min_samples: u32,
    training_start: Option<&str>,
    training_end: Option<&str>,
    config_path: &Path,
) -> Result<()> {
    // Collect 从 canonical Store 中筛选已封存、身份一致且四资产/三期限标签完整的
    // Paper Run，组装一个可复现的数据集；只有全部门槛满足时才写 CalibrationDataset，
    // 本函数从不直接生成或激活 DecisionPolicy。
    if min_samples == 0 {
        bail!("calibration collect requires min_samples > 0");
    }
    let config = load_config(config_path).context("load model identity for calibration collect")?;
    let model_config = config
        .model
        .as_ref()
        .context("calibration collect requires [model] configuration")?;
    let synthesizer_route = model_config.routes.get("research.synthesizer");
    if synthesizer_route
        .and_then(|route| route.release_date.as_ref())
        .or(model_config.release_date.as_ref())
        .is_none()
        || synthesizer_route
            .and_then(|route| route.knowledge_cutoff.as_ref())
            .or(model_config.knowledge_cutoff.as_ref())
            .is_none()
    {
        bail!(
            "calibration collect requires release_date and knowledge_cutoff for the effective research.synthesizer route"
        );
    }
    let (model_id, model_version_hash) = configured_synthesizer_identity(model_config)?;
    let provider_id = model_config.provider_identity().as_str().to_owned();
    let store = Store::open_existing(store_path)?;
    if store.debug_environment()?.is_some() {
        bail!("calibration requires a canonical Store");
    }
    // 风险限制必须来自 Store 中 operator 已写入的 canonical Artifact，不能由本次
    // collect 的参数或默认值隐式补齐。
    let risk_artifact =
        calibration_artifact(&store, risk_limits_id, ArtifactKind::CalibrationRiskLimits)?;
    let risk_limits: OfflineRiskLimits =
        serde_json::from_slice(&store.read_blob(&risk_artifact.blob)?)?;
    risk_limits.validate()?;
    let requested_start = training_start
        .map(|value| parse_calibration_time(value, "training_start"))
        .transpose()?;
    let requested_end = training_end
        .map(|value| parse_calibration_time(value, "training_end"))
        .transpose()?;
    if requested_start
        .zip(requested_end)
        .is_some_and(|(start, end)| start > end)
    {
        bail!("calibration training_start must not be after training_end");
    }

    let decision_artifacts = store
        .recent_artifacts_by_kind(ArtifactKind::Decision, 500)
        .context("scan recent Decision artifacts for calibration collect")?;
    let mut skipped = Vec::new();
    let mut candidates = Vec::new();
    // 每个 Decision 都独立经过 Paper purpose、canonical、结构、Outcome、模型身份和
    // 市场标签筛选；单个 Run 的缺陷记录在 skipped 中，不会中断其他候选的扫描。
    for decision_artifact in decision_artifacts.iter().cloned() {
        let Some(run_id) = decision_artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.clone())
        else {
            continue;
        };
        if store.run_purpose(&run_id)? != RunPurpose::Paper {
            continue;
        }
        if decision_artifact.lifecycle != ArtifactLifecycle::Canonical {
            skipped.push(serde_json::json!({
                "run_id": run_id,
                "reason": "decision_not_canonical",
            }));
            continue;
        }
        let decision: Decision = match serde_json::from_slice(
            &store.read_blob(&decision_artifact.blob)?,
        ) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_decode","detail":error.to_string()}));
                continue;
            }
        };
        if let Err(error) = decision.validate() {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_invalid","detail":error.to_string()}));
            continue;
        }
        let context_artifact = match store.artifact(&decision.decision_context.artifact_id) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_context_missing","detail":error.to_string()}));
                continue;
            }
        };
        let context: DecisionContext = match serde_json::from_slice(
            &store.read_blob(&context_artifact.blob)?,
        ) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_context_decode","detail":error.to_string()}));
                continue;
            }
        };
        if let Err(error) = context.validate() {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_context_invalid","detail":error.to_string()}));
            continue;
        }
        let Some(outcome_artifact) = store.outcome_for_run(&run_id)? else {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_not_mature"}));
            continue;
        };
        if outcome_artifact.lifecycle != ArtifactLifecycle::Canonical {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_not_canonical"}));
            continue;
        }
        let outcome: Outcome = match serde_json::from_slice(
            &store.read_blob(&outcome_artifact.blob)?,
        ) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_decode","detail":error.to_string()}));
                continue;
            }
        };
        if let Err(error) = outcome.validate_sealed() {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_not_sealed","detail":error.to_string()}));
            continue;
        }
        let schedule_artifact = match store.artifact(&outcome.schedule.artifact_id) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"schedule_missing","detail":error.to_string()}));
                continue;
            }
        };
        let schedule: OutcomeSchedule = match serde_json::from_slice(
            &store.read_blob(&schedule_artifact.blob)?,
        ) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"schedule_decode","detail":error.to_string()}));
                continue;
            }
        };
        if schedule.decision
            != (ArtifactRef {
                artifact_id: decision_artifact.artifact_id.clone(),
                kind: ArtifactKind::Decision,
            })
        {
            skipped
                .push(serde_json::json!({"run_id":run_id,"reason":"schedule_decision_mismatch"}));
            continue;
        }
        let Some(identity) =
            synthesizer_identity_for_decision(&store, &decision_artifact, &decision, &run_id)?
        else {
            skipped
                .push(serde_json::json!({"run_id":run_id,"reason":"synthesizer_identity_missing"}));
            continue;
        };
        if identity.provider_id != provider_id || identity.model_id != model_id {
            skipped.push(serde_json::json!({
                "run_id":run_id,
                "reason":"model_identity_mismatch",
                "expected_model":model_id,
                "actual_model":identity.model_id,
                "expected_provider":provider_id,
                "actual_provider":identity.provider_id,
            }));
            continue;
        }
        let bars = match outcome_bars(&store, &outcome) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_bars_unusable","detail":error.to_string()}));
                continue;
            }
        };
        if let Err(reason) = candidate_has_all_labels(&decision, &schedule, &outcome, &bars) {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"maturity_label_missing","detail":reason}));
            continue;
        }
        candidates.push(CollectedCalibrationCandidate {
            run_id,
            decision_artifact,
            decision,
            outcome,
            schedule,
            bars,
            provider_id: identity.provider_id,
            model_id: identity.model_id,
            contract_hash: identity.contract_hash,
        });
    }
    candidates.sort_by(|left, right| left.run_id.cmp(&right.run_id));

    // 数据集必须绑定同一 Synthesizer Contract；不一致的候选被排除，而不是混合训练。
    let contract_hash = candidates
        .first()
        .map(|candidate| candidate.contract_hash.clone());
    let mut selected = Vec::new();
    for candidate in candidates {
        if contract_hash.as_ref() == Some(&candidate.contract_hash) {
            selected.push(candidate);
        } else {
            skipped.push(serde_json::json!({
                "run_id": candidate.run_id,
                "reason": "contract_identity_mismatch",
            }));
        }
    }
    let actual_start = requested_start.or_else(|| {
        selected
            .iter()
            .map(|candidate| candidate.decision.created_at)
            .min()
    });
    let actual_end = requested_end.or_else(|| {
        selected
            .iter()
            .flat_map(|candidate| candidate.outcome.windows.iter())
            .map(|window| session_close_time(window.observed_trading_day))
            .max()
    });
    let mut samples = Vec::new();
    let mut panel = BTreeMap::<Asset, BTreeMap<NaiveDate, MoneyMicros>>::new();
    let mut collected_runs = Vec::new();
    let mut sample_counts = BTreeMap::<String, usize>::new();
    let mut price_conflicts = Vec::new();
    // 每个候选贡献完整的 4×3 forecast 集和可合并的日线价格；窗口外数据不进入训练面板，
    // 同一资产/日期的不同收盘价则保留冲突并阻断写入。
    for candidate in &selected {
        let Some(start) = actual_start else { continue };
        let Some(end) = actual_end else { continue };
        let mut candidate_samples = Vec::new();
        for forecast in &candidate.decision.forecasts {
            let window_horizon = match forecast.horizon {
                DecisionHorizon::T1 => OutcomeHorizon::T1,
                DecisionHorizon::T3 => OutcomeHorizon::T3,
                DecisionHorizon::T5 => OutcomeHorizon::T5,
            };
            let Some(window) = candidate
                .outcome
                .windows
                .iter()
                .find(|window| window.horizon == window_horizon)
            else {
                continue;
            };
            let Some(base) = candidate
                .bars
                .get(&forecast.asset)
                .and_then(|bars| bars.get(&candidate.schedule.baseline_trading_day))
            else {
                continue;
            };
            let Some(future) = candidate
                .bars
                .get(&forecast.asset)
                .and_then(|bars| bars.get(&window.observed_trading_day))
            else {
                continue;
            };
            let realized_at = session_close_time(window.observed_trading_day);
            if candidate.decision.created_at < start
                || candidate.decision.created_at > end
                || realized_at > end
            {
                continue;
            }
            candidate_samples.push(HistoricalForecastSample {
                source_run_id: candidate.run_id.0.clone(),
                asset: forecast.asset,
                horizon: forecast.horizon,
                forecast_at: candidate.decision.created_at,
                realized_at,
                raw_probability_ppm: forecast.positive_return_probability_ppm,
                raw_expected_return_ppm: forecast.expected_return_ppm,
                realized_return_ppm: relative_return_ppm(*base, *future)?,
                provenance: HistoricalForecastProvenance {
                    provider_id: candidate.provider_id.clone(),
                    model_id: candidate.model_id.clone(),
                    model_version_hash: model_version_hash.clone(),
                    route: "research.synthesizer".to_owned(),
                    contract_hash: candidate.contract_hash.clone(),
                    source_decision: ArtifactRef {
                        artifact_id: candidate.decision_artifact.artifact_id.clone(),
                        kind: ArtifactKind::Decision,
                    },
                    source_decision_context: candidate.decision.decision_context.clone(),
                    forecast_cutoff: candidate.decision.created_at,
                },
            });
        }
        if candidate_samples.len() != Asset::EXECUTABLE.len() * DecisionHorizon::ALL.len() {
            skipped.push(serde_json::json!({
                "run_id": candidate.run_id,
                "reason": "training_window_excludes_complete_forecast_set",
                "sample_count": candidate_samples.len(),
            }));
            continue;
        }
        for (key, count) in
            candidate_samples
                .iter()
                .fold(BTreeMap::<String, usize>::new(), |mut counts, sample| {
                    *counts
                        .entry(format!("{}:{:?}", sample.asset, sample.horizon))
                        .or_default() += 1;
                    counts
                })
        {
            *sample_counts.entry(key).or_default() += count;
        }
        for (asset, bars) in &candidate.bars {
            for (date, price) in bars {
                if actual_start.is_some_and(|start| *date < start.date_naive())
                    || actual_end.is_some_and(|end| *date > end.date_naive())
                {
                    continue;
                }
                match panel.entry(*asset).or_default().entry(*date) {
                    std::collections::btree_map::Entry::Occupied(existing) => {
                        if *existing.get() != *price {
                            price_conflicts.push(serde_json::json!({
                                "run_id": candidate.run_id,
                                "asset": asset,
                                "date": date,
                                "existing_close_micros": existing.get(),
                                "incoming_close_micros": price,
                            }));
                        }
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(*price);
                    }
                }
            }
        }
        samples.extend(candidate_samples);
        collected_runs.push(candidate.run_id.0.clone());
    }
    samples.sort_by(|left, right| {
        left.source_run_id
            .cmp(&right.source_run_id)
            .then_with(|| left.asset.cmp(&right.asset))
            .then_with(|| left.horizon.cmp(&right.horizon))
    });
    collected_runs.sort();
    collected_runs.dedup();
    let common_dates = panel
        .values()
        .map(|bars| bars.keys().copied().collect::<BTreeSet<_>>())
        .reduce(|left, right| left.intersection(&right).copied().collect())
        .unwrap_or_default();
    let price_series = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| HistoricalPriceSeries {
            asset,
            points: common_dates
                .iter()
                .filter_map(|date| {
                    panel
                        .get(&asset)?
                        .get(date)
                        .map(|price| HistoricalPricePoint {
                            observed_at: *date,
                            close_micros: price.0,
                        })
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    let required_forecast_samples = usize::try_from(min_samples)
        .unwrap_or(usize::MAX)
        .saturating_mul(Asset::EXECUTABLE.len())
        .saturating_mul(DecisionHorizon::ALL.len());
    let enough_forecasts = sample_counts
        .values()
        .all(|count| *count >= usize::try_from(min_samples).unwrap_or(usize::MAX))
        && sample_counts.len() == Asset::EXECUTABLE.len() * DecisionHorizon::ALL.len();
    let enough_prices = price_series.iter().all(|series| {
        series.points.len()
            >= usize::try_from(min_samples)
                .unwrap_or(usize::MAX)
                .saturating_add(1)
    });
    let status = if contract_hash.is_some()
        && actual_start.is_some()
        && actual_end.is_some()
        && !samples.is_empty()
        && samples.len() >= required_forecast_samples
        && enough_forecasts
        && enough_prices
        && price_conflicts.is_empty()
    {
        "ready_for_build"
    } else {
        "BLOCKED"
    };
    // 只有状态为 ready_for_build 才构造正式 OfflineCalibrationInput；BLOCKED 报告仍会
    // 返回缺口和 skipped 详情，但不伪造 dataset Artifact。
    let mut dataset_input = None;
    let mut input_hash = None;
    if status == "ready_for_build" {
        let input = OfflineCalibrationInput {
            schema_version: 1,
            policy_version: "offline_historical_v2".to_owned(),
            algorithm_version: "offline_historical_risk_v1".to_owned(),
            provider_id: provider_id.clone(),
            model_id: model_id.clone(),
            model_version_hash,
            model_route: "research.synthesizer".to_owned(),
            contract_hash: contract_hash
                .clone()
                .expect("samples have contract identity"),
            regime: "all".to_owned(),
            training_start: actual_start.unwrap_or_else(Utc::now),
            training_end: actual_end.unwrap_or_else(Utc::now),
            min_samples,
            source_runs: collected_runs.clone(),
            forecasts: samples,
            price_series,
            risk_limits,
        };
        let canonical_input_hash = content_hash_json(&serde_json::to_value(&input)?)?;
        input_hash = Some(canonical_input_hash);
        dataset_input = Some(input);
    }
    let mut report = serde_json::json!({
        "status": status,
        "source_store": store_path,
        "risk_limits_artifact": risk_artifact.artifact_id,
        "input_hash": input_hash,
        "scan": {
            "decision_artifacts_considered": decision_artifacts.len(),
            "scan_limit": 500,
            "may_be_truncated": decision_artifacts.len() == 500,
            "matured_runs": collected_runs.len(),
            "collected_forecast_samples": sample_counts.values().sum::<usize>(),
            "sample_counts": sample_counts,
            "common_price_points": common_dates.len(),
        },
        "identity": {
            "provider_id": provider_id,
            "model_id": model_id,
            "model_route": "research.synthesizer",
            "contract_hash": contract_hash,
            "training_start": actual_start,
            "training_end": actual_end,
        },
        "gaps": {
            "min_samples": min_samples,
            "enough_forecasts": enough_forecasts,
            "enough_prices": enough_prices,
            "price_conflicts": price_conflicts,
            "skipped_runs": skipped,
        },
        "next_step": if status == "ready_for_build" {
            "akzio calibration build --store <store> --dataset <dataset-artifact-id>"
        } else {
            "collect additional canonical Paper outcomes; no policy was built"
        },
    });
    if let Some(input) = dataset_input {
        // Dataset 的 source_refs 保留风险限制、Decision 和对应 Outcome 的完整血缘；
        // 写入时重新打开 Store，随后用完整性检查确认 CAS 索引没有被破坏。
        let mut sources = vec![ArtifactRef {
            artifact_id: risk_artifact.artifact_id,
            kind: ArtifactKind::CalibrationRiskLimits,
        }];
        for candidate in &selected {
            if collected_runs.contains(&candidate.run_id.0) {
                sources.push(ArtifactRef {
                    artifact_id: candidate.decision_artifact.artifact_id.clone(),
                    kind: ArtifactKind::Decision,
                });
                let outcome = store
                    .outcome_for_run(&candidate.run_id)?
                    .context("selected Outcome disappeared")?;
                sources.push(ArtifactRef {
                    artifact_id: outcome.artifact_id,
                    kind: ArtifactKind::Outcome,
                });
            }
        }
        let write_store = Store::open(store_path)?;
        let stored = persist_calibration_artifact(
            &write_store,
            ArtifactKind::CalibrationDataset,
            &StoredCalibrationDataset {
                input,
                report: report.clone(),
            },
            sources,
            Utc::now(),
        )?;
        report["dataset_artifact"] = serde_json::to_value(stored.artifact_id)?;
        write_store.verify_integrity()?;
    }
    print_json(&report)
}

fn parse_calibration_time(value: &str, field: &str) -> Result<DateTime<Utc>> {
    // 训练边界只接受带时区的 RFC3339，并统一为 UTC，避免本地时区改变样本筛选。
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("{field} must be RFC3339"))
}

fn session_close_time(date: NaiveDate) -> DateTime<Utc> {
    // 日级标签使用固定的当日 23:59:59 作为“该交易日已结束”的比较点；
    // 这是校准窗口的统一时间基准，不是对交易所实际收盘钟点的重新测量。
    date.and_hms_opt(23, 59, 59)
        .expect("23:59:59 is a valid time")
        .and_utc()
}

fn relative_return_ppm(base: MoneyMicros, future: MoneyMicros) -> Result<i64> {
    // 只有正的基准和未来价格才能形成收益标签；整数运算按 ppm 保留并在除法/转换
    // 可能溢出时显式报错，避免用饱和或默认值掩盖无效样本。
    if base.0 <= 0 || future.0 <= 0 {
        bail!("calibration label requires positive baseline and future prices");
    }
    i64::try_from(
        (i128::from(future.0) - i128::from(base.0))
            .saturating_mul(1_000_000)
            .checked_div(i128::from(base.0))
            .context("calibration return arithmetic overflow")?,
    )
    .context("calibration return does not fit i64")
}

struct SynthesizerIdentity {
    provider_id: String,
    model_id: String,
    contract_hash: ContentHash,
}

fn synthesizer_identity_for_decision(
    store: &Store,
    decision_artifact: &akzio_domain::Artifact,
    decision: &Decision,
    run_id: &RunId,
) -> Result<Option<SynthesizerIdentity>> {
    // 从 Decision 沿 source_refs 反向遍历同一 Run 的 Artifact DAG，寻找 Decision 截止前
    // 最近且带完整 capability/Contract 身份的 research.synthesizer AgentTurn；遍历有界，
    // 缺失或越界引用只使身份不可用，不把不相关 AgentTurn 当作训练来源。
    let mut queue = vec![ArtifactRef {
        artifact_id: decision_artifact.artifact_id.clone(),
        kind: ArtifactKind::Decision,
    }];
    let mut seen = BTreeSet::new();
    let mut found = None;
    let mut found_at = None;
    while let Some(reference) = queue.pop() {
        // seen 和 256 节点上限同时防止异常/循环血缘让 CLI 无限读取 CAS。
        if !seen.insert(reference.artifact_id.clone()) || seen.len() > 256 {
            continue;
        }
        let Ok(artifact) = store.artifact(&reference.artifact_id) else {
            continue;
        };
        if artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.as_ref())
            != Some(run_id)
            || artifact.created_at > decision.created_at
        {
            continue;
        }
        if artifact.kind == ArtifactKind::AgentTurn {
            let payload: serde_json::Value =
                serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            let request = payload
                .get("request")
                .or_else(|| payload.get("domain_request"));
            if request
                .and_then(|request| request.get("purpose"))
                .and_then(serde_json::Value::as_str)
                == Some("research.synthesizer")
            {
                let capability = payload.get("capability_snapshot");
                let provider_id = capability
                    .and_then(|value| value.get("provider_id"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                let model_id = payload
                    .pointer("/response/telemetry/actual_model")
                    .and_then(serde_json::Value::as_str)
                    .or_else(|| {
                        capability
                            .and_then(|value| value.get("model_id"))
                            .and_then(serde_json::Value::as_str)
                    })
                    .map(str::to_owned);
                let contract_hash = payload
                    .get("contract_hash")
                    .or_else(|| request.and_then(|request| request.get("contract_hash")))
                    .cloned()
                    .and_then(|value| serde_json::from_value::<ContentHash>(value).ok());
                if let (Some(provider_id), Some(model_id), Some(contract_hash)) =
                    (provider_id, model_id, contract_hash)
                {
                    if found_at.is_none_or(|current| artifact.created_at > current) {
                        found = Some(SynthesizerIdentity {
                            provider_id,
                            model_id,
                            contract_hash,
                        });
                        found_at = Some(artifact.created_at);
                    }
                }
            }
        }
        queue.extend(artifact.source_refs);
    }
    Ok(found)
}

fn outcome_bars(
    store: &Store,
    outcome: &Outcome,
) -> Result<BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>> {
    // 只从 Outcome 已绑定的 Alpaca NormalizedEvidence 读取日线，并按资产/日期合并；
    // 来源不符、四资产不全或同一日期价格冲突都使该候选不可用于校准。
    let mut bars_by_asset = BTreeMap::new();
    for reference in &outcome.market_evidence {
        if reference.kind != ArtifactKind::NormalizedEvidence {
            continue;
        }
        let artifact = store.artifact(&reference.artifact_id)?;
        let payload: NormalizedEvidencePayload =
            serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        if payload.source != EvidenceSource::Alpaca || !payload.resource.starts_with("bars:") {
            continue;
        }
        let asset = payload
            .resource
            .split(':')
            .nth(1)
            .and_then(|value| Asset::try_from(value).ok())
            .context("outcome bar resource has no executable asset")?;
        let bars = parse_daily_bars(&payload.value, payload.observed_at)
            .with_context(|| format!("parse outcome bars for {asset}"))?;
        let destination = bars_by_asset.entry(asset).or_insert_with(BTreeMap::new);
        for (date, price) in bars {
            if let Some(existing) = destination.insert(date, price) {
                if existing != price {
                    bail!(
                        "outcome bars contain conflicting {} price on {}",
                        asset,
                        date
                    );
                }
            }
        }
    }
    if bars_by_asset.len() != Asset::EXECUTABLE.len() {
        bail!("outcome market evidence lacks all four executable asset bar series");
    }
    Ok(bars_by_asset)
}

fn candidate_has_all_labels(
    decision: &Decision,
    schedule: &OutcomeSchedule,
    outcome: &Outcome,
    bars: &BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
) -> std::result::Result<(), String> {
    // 对每个 Decision forecast 找到对应 Outcome horizon，并要求实现日位于 Decision
    // cutoff 之后，同时存在该资产的 baseline 与 realized 日线，避免用未来前的数据作标签。
    for forecast in &decision.forecasts {
        let horizon = match forecast.horizon {
            DecisionHorizon::T1 => OutcomeHorizon::T1,
            DecisionHorizon::T3 => OutcomeHorizon::T3,
            DecisionHorizon::T5 => OutcomeHorizon::T5,
        };
        let Some(window) = outcome
            .windows
            .iter()
            .find(|window| window.horizon == horizon)
        else {
            return Err(format!("missing {:?} Outcome window", horizon));
        };
        if session_close_time(window.observed_trading_day) <= decision.created_at {
            return Err(format!(
                "{:?} realized session is not after the frozen decision cutoff",
                horizon
            ));
        }
        let Some(asset_bars) = bars.get(&forecast.asset) else {
            return Err(format!("missing {} bar series", forecast.asset));
        };
        if !asset_bars.contains_key(&schedule.baseline_trading_day)
            || !asset_bars.contains_key(&window.observed_trading_day)
        {
            return Err(format!(
                "missing {} baseline or {:?} realized session bar",
                forecast.asset, horizon
            ));
        }
    }
    Ok(())
}

async fn run_evidence_command(command: &EvidenceCommand, config: &Config) -> Result<()> {
    // Evidence 子命令与校准/执行分离：MarketAudit 负责显式采集并物化审计资料，
    // Preflight 只探测 NewsWeb 可用性和质量，二者都不授予 Execution authorization。
    match command {
        EvidenceCommand::MarketAudit { output, option_feed } => market_audit(config, output, *option_feed).await,
        EvidenceCommand::Preflight { resource } => {
            let model_config = config
                .model
                .as_ref()
                .context("NewsWeb preflight requires [model] configuration")?;
            let route = if model_config.routes.contains_key("evidence.news_web") { "evidence.news_web" } else { "default" };
            let adapter = akzio_ingest::configured_news_evidence_transport(model_config)
                .context("construct governed NewsWeb router")?;
            let request = EvidenceRequest {
                source: EvidenceSource::NewsWeb,
                resource: resource.clone(),
                max_age: ChronoDuration::days(7),
                acquisition_mode: akzio_domain::evidence_acquisition_mode(RunPurpose::PositionPlan, &akzio_domain::EvidenceNeed {
                    schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
                    source_family: "news_web".into(), resource: resource.clone(), max_age_secs: 604800,
                }),
            };
            let acquired = match adapter.acquire(&request).await {
                Ok(acquired) => acquired,
                Err(error) => {
                    // Provider 错误被转换为稳定的 BLOCKED 诊断并立即返回；预检不写 Store
                    // Artifact，因此这里的成功/失败都不是研究 Run 的 EvidenceGate 结果。
                    let (diagnostic, detail) = evidence_preflight_error(&error);
                    return print_json(&serde_json::json!({
                        "status": "BLOCKED",
                        "source_family": "news_web",
                        "resource": resource,
                        "route": route,
                        "diagnostic": diagnostic,
                        "detail": detail,
                        "artifact_id": serde_json::Value::Null,
                        "artifact_note": "CLI preflight is read-only and does not write a Store artifact",
                    }));
                }
            };
            let raw_hash = akzio_domain::ContentHash::of_bytes(&acquired.raw);
            let normalized_bytes = serde_json::to_vec(&acquired.normalized)
                .context("encode normalized NewsWeb preflight payload")?;
            let normalized_hash = akzio_domain::ContentHash::of_bytes(&normalized_bytes);
            let asset = Asset::EXECUTABLE
                .into_iter()
                .find(|asset| resource.contains(asset.symbol()));
            let usable_at = acquired
                .provenance
                .published_at
                .unwrap_or(acquired.observed_at);
            let status = acquired.quality.citations_complete;
            // 即使拿到 payload，引用不完整也只报告 BLOCKED；返回的 hashes 是观测信息，
            // artifact_id 明确为空，不能被解释为已持久化或已授权的证据。
            print_json(&serde_json::json!({
                "status": if status { "available" } else { "BLOCKED" },
                "source_family": "news_web",
                "resource": resource,
                "asset": asset,
                "route": route,
                "evidence_domain": if resource.starts_with("news:") { Some("news_event") } else { None },
                "source_review": acquired.normalized.get("source_review"),
                "reviewed_facts": acquired.normalized.get("reviewed_facts"),
                "execution_authorization": "not_granted",
                "published_at": acquired.provenance.published_at,
                "retrieved_at": acquired.observed_at,
                "usable_at": usable_at,
                "source_uri": acquired.source_uri,
                "citations": acquired.provenance.citations,
                "quality": acquired.quality,
                "raw_hash": raw_hash,
                "normalized_hash": normalized_hash,
                "diagnostic": if status { "none" } else { "source_verification_incomplete" },
                "artifact_id": serde_json::Value::Null,
                "artifact_note": "CLI preflight is read-only and does not write a Store artifact",
            }))
        }
    }
}

fn evidence_preflight_error(error: &EvidenceAdapterError) -> (&'static str, &'static str) {
    // 将 adapter 的具体错误归一为 CLI 诊断类别，同时保留“未写 Artifact”的上层边界。
    match error {
        EvidenceAdapterError::NativeWeb { kind, .. } => (kind.as_str(), "native web contract failure"),
        EvidenceAdapterError::Unauthorized(_) => ("authorization", "provider authorization or entitlement denied"),
        EvidenceAdapterError::RateLimited { .. } => ("rate_limited", "provider rate limited the request"),
        EvidenceAdapterError::Pending(_) => ("pending", "provider data is pending"),
        EvidenceAdapterError::NotConfigured(_) => (
            "adapter_unavailable",
            "no authorized provider is configured",
        ),
        EvidenceAdapterError::Permanent(_) => ("permanent_provider_error", "provider rejected the request"),
        EvidenceAdapterError::Transport(_) => ("transport", "provider route or network transport failed"),
        EvidenceAdapterError::Policy { .. } => ("policy_rejected", "Rust evidence policy rejected the response"),
        EvidenceAdapterError::DataQuality(_) => ("data_quality", "provider payload failed data quality checks"),
        EvidenceAdapterError::MissingFixture(_) => ("fixture_missing", "fixture evidence is unavailable"),
        EvidenceAdapterError::SourceMismatch => ("source_mismatch", "adapter source does not match the request"),
    }
}

/// Real adapter audit, independent of model/policy availability. Only evidence tasks are created.
async fn market_audit(config: &Config, output: &Path, feed: akzio_ingest::AlpacaOptionDataFeed) -> Result<()> {
    use akzio_ingest::AsyncEvidenceAdapter;
    // 审计输出目录必须是新的；后续所有原始/规范化文件和 Store 都局限在该目录内，
    // 报告中的 broker_write_policy=forbidden 与 llm_calls=0 只描述本入口的作用域。
    if output.exists() { bail!("market audit output must be new"); }
    fs::create_dir_all(output)?;
    let store = Store::open(output.join("store"))?;
    let adapter = akzio_ingest::AlpacaPaperEvidenceTransport::new(
        "https://paper-api.alpaca.markets",
        resolve_env_placeholder(&config.credentials.alpaca_api_key, "credentials.alpaca_api_key")?,
        resolve_env_placeholder(&config.credentials.alpaca_api_secret, "credentials.alpaca_api_secret")?,
        config.execution.market_data_feed)?.with_option_feed(feed);
    let cutoff = Utc::now();
    let day = akzio_ingest::market_session_day(cutoff);
    let mut reports = Vec::new();
    // 每个资产分别采集 bars 和 option_chain；单项失败写入报告并继续其他项，
    // 因此整体报告可能是部分成功，最终完整性只以报告字段为准。
    for asset in Asset::EXECUTABLE {
        for resource in [format!("bars:{}:1d:{}:252",asset.symbol(),day-ChronoDuration::days(400)),
            format!("option_chain:{}:{}:{}",asset.symbol(),day,day+ChronoDuration::days(30))] {
            let request = EvidenceRequest {source:EvidenceSource::Alpaca,resource:resource.clone(),
                max_age:ChronoDuration::days(7),acquisition_mode:akzio_domain::EvidenceAcquisitionMode::VerifiedSource};
            match adapter.acquire_at(&request, cutoff).await {
                Ok(acquired) => {
                    let stem = format!("{}-{}",asset.symbol(), if resource.starts_with("bars:") {"stocks"} else {"options"});
                    fs::write(output.join(format!("{stem}-raw.json")), &acquired.raw)?;
                    fs::write(output.join(format!("{stem}-normalized.json")),serde_json::to_vec_pretty(&acquired.normalized)?)?;
                    let validation = akzio_ingest::EvidenceRuntime::validate_acquired_evidence(&request,&acquired,Utc::now())
                        .map(|_|"passed".to_owned()).unwrap_or_else(|e|e.to_string());
                    let materialization = akzio_daemon::persist_market_audit_capture(&store, &request, acquired.clone())
                        .unwrap_or_else(|e|serde_json::json!({"status":"failed","error":e.to_string()}));
                    reports.push(serde_json::json!({"asset":asset,"resource":resource,"status":"acquired",
                        "coverage":acquired.normalized.get("coverage"), "bars":acquired.normalized["bars"].as_array().map(Vec::len),
                        "feed":acquired.normalized["feed"],"latest_completed_session":acquired.normalized["latest_completed_session"],
                        "provenance":acquired.provenance,"validation":validation,
                        "raw_hash":ContentHash::of_bytes(&acquired.raw),"materialization":materialization}));
                }
                Err(error) => reports.push(serde_json::json!({"asset":asset,"resource":resource,"status":"unavailable","error":error.to_string()})),
            }
        }
    }
    // Store 完整性检查和报告写入发生在采集循环之后；它们证明审计物化结果可读，
    // 不证明 Paper Decision、ExecutionVerdict、订单或 Outcome 已产生。
    let integrity = store.verify_integrity().map(|_|"passed".to_owned()).unwrap_or_else(|e|e.to_string());
    let report=serde_json::json!({"decision_cutoff":cutoff,"broker_write_policy":"forbidden","auto_paper":false,
        "llm_calls":0,"reports":reports,"store_integrity":integrity});
    fs::write(output.join("report.json"),serde_json::to_vec_pretty(&report)?)?;
    print_json(&report)
}
