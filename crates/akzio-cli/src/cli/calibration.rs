// 文件导读：本文件负责校准 CLI：readiness/inspect/validate 只读既有 Store；
// preflight 使用可写 Store::open，可能初始化 scratch Store，核对 Contract 时也可能安装
// catalogue；set-risk-limits/collect/build/bootstrap/activate 分别有明确的写入边界。
// 成熟 Outcome 不是 active Policy，candidate 也不会由 build 自动激活。
// Rust 机制：泛型 `persist_calibration_artifact<T: Serialize>` 约束输入可序列化；闭包和
// 迭代器按 Artifact DAG 聚合样本；`Option`/`Result` 区分 no_schedule、pending、blocked
// 和真实错误；测试用 cfg(test) 只验证离线 Store 行为。

use akzio_domain::{
    Artifact, ArtifactId, ArtifactKind, ArtifactLifecycle, ArtifactProvenance, ArtifactRef,
    Decision, DecisionContext, DecisionHorizon, MoneyMicros, Outcome, OutcomeHorizon,
    OutcomeSchedule, ALPACA_DAILY_BAR_AVAILABILITY_LAG_MINUTES,
};
use akzio_execution::calibration::{
    HistoricalRiskCalendar, HistoricalRiskFeed, HistoricalRiskPair, ADJACENT_SESSION_RISK_VERSION,
};
use akzio_execution::{
    build_offline_decision_policy, HistoricalForecastProvenance, HistoricalForecastSample,
    HistoricalPricePoint, HistoricalPriceSeries, OfflineCalibrationInput, OfflineRiskLimits,
};
use akzio_ingest::{
    parse_scheduled_daily_bars, parse_venue_daily_bars, BarAdjustment, BarSessionCoverage,
    EvidenceSource, ForecastCalendars, GovernedResource, NormalizedEvidencePayload,
    ScheduledSession,
};
use chrono::DateTime;

const CALIBRATION_LABEL_TIMING: &str = "venue_calendar_and_observed_bars_v1";

fn handle_calibration(command: &CalibrationCommand, config_path: &Path) -> Result<()> {
    // `command: &CalibrationCommand` 只借用解析出的命令；match 的每个分支按各自权限
    // 打开 Store。Readiness/Inspect/Validate 只读已有库；Preflight 的 Store::open
    // 可创建或迁移 scratch，Bootstrap 在目标库激活源 Policy，不能归到只读一组。
    match command {
        CalibrationCommand::Readiness {
            store,
            min_samples,
            cohort,
            scope,
        } => calibration_readiness(store, *min_samples, config_path, *cohort, scope),
        CalibrationCommand::Preflight { scratch } => {
            // 不启动模型或执行链；但 open 会初始化/迁移 scratch，若 Policy 已就绪，
            // decision_policy_preflight 内的 catalogue helper 也可能写入 Contract 安装。
            // 因此“llm_calls:0”不能被误读成“文件和 Store 完全无副作用”。
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
                "activated_at":active.activated_at,"store_integrity":"passed",
                "core_activation":"on_restart"}),
            )
        }
        CalibrationCommand::Bootstrap {
            source_store,
            target_store,
        } => {
            // 源库不存在时 open_existing_or_initialize 会建立空库；目标用可写 open，
            // 有源 active Policy 时 bootstrap_active_decision_policy_from 会在目标激活
            // 那份冻结 Policy，而不复制源 Run/Outcome 或制造新校准样本。
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
            scope,
        } => print_json(&collect_calibration_report(store, risk_limits, *min_samples, scope, config_path)?),
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
            // Inspect 仅读取元数据/正文并检查三个允许的 kind；这个分支没有再次检查
            // lifecycle==Canonical，不能把它说成与 calibration_artifact 相同的资格验证。
            // 展示成功不代表结构已 validate、Policy decision-capable 或已激活。
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
    // `Option<bool>` 中 None 表示尚未进入 Contract 比较，而不是比较结果 false。
    // Policy 已就绪时 helper 会调用 ActiveResearchCatalogue::install，可能写入
    // scratch Store 的 Contract catalogue；返回的布尔值本身不激活 DecisionPolicy。
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
// Store 中的 Dataset Artifact 保存完整训练输入与报告；两者都按同一 CAS 正文持久化，
// `report` 便于审计收集时的筛选结果，不是 Policy 激活状态。
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
    // 泛型 T: Serialize 只约束 payload 能编码为 JSON。stage_json 先暂存 BLOB；
    // write_calibration_artifact 的事务才把它提升为 durable CAS 并写 Artifact 索引。
    // 此 helper 不更新 active Policy head，Bootstrap/Activate 走各自独立路径。
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
    // let-else 解包 Option<&OutcomeSchedule>；无 schedule 就提前返回，不编造未来到期日。
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

const CALIBRATION_SCAN_LIMIT: usize = 500;
const CALIBRATION_PAGE_SIZE: usize = 128;

struct CalibrationDecisionScan {
    artifacts: Vec<Artifact>,
    truncated: bool,
}

fn scan_calibration_decisions(
    store: &Store,
    window: (Option<DateTime<Utc>>, Option<DateTime<Utc>>),
) -> Result<CalibrationDecisionScan> {
    let mut artifacts: Vec<Artifact> = Vec::new();
    while artifacts.len() <= CALIBRATION_SCAN_LIMIT {
        let before = artifacts
            .last()
            .map(|artifact| (artifact.created_at, &artifact.artifact_id));
        let limit = CALIBRATION_PAGE_SIZE.min(CALIBRATION_SCAN_LIMIT + 1 - artifacts.len());
        let page = store.calibration_decision_page(window, before, limit)?;
        let exhausted = page.len() < limit;
        artifacts.extend(page);
        if exhausted {
            break;
        }
    }
    let truncated = artifacts.len() > CALIBRATION_SCAN_LIMIT;
    artifacts.truncate(CALIBRATION_SCAN_LIMIT);
    Ok(CalibrationDecisionScan {
        artifacts,
        truncated,
    })
}

fn calibration_readiness(
    store_path: &Path,
    min_samples: u32,
    config_path: &Path,
    cohort: bool,
    scope: &CalibrationScopeArgs,
) -> Result<()> {
    let scoped = cohort
        || scope.training_start.is_some()
        || scope.training_end.is_some()
        || scope.contract_hash.is_some();
    print_json(&calibration_readiness_report(
        store_path,
        min_samples,
        config_path,
        scoped.then_some(scope),
    )?)
}

fn calibration_readiness_report(
    store_path: &Path,
    min_samples: u32,
    config_path: &Path,
    cohort: Option<&CalibrationScopeArgs>,
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
    let (scan, rows, cohort_report) = if let Some(requested) = cohort.filter(|_| !isolated) {
        let scope = calibration_cohort(&store, config_path, requested)?;
        let CalibrationSelection {
            scan,
            selected,
            skipped,
        } = select_calibration_cohort(&store, &scope)?;
        let selected_run_ids: Vec<_> = selected.iter().map(|candidate| &candidate.run_id).collect();
        let mut rows: Vec<_> = selected
            .iter()
            .map(|candidate| {
                serde_json::json!({
                    "status": "sealed", "run_id": candidate.run_id,
                    "decision_artifact_id": candidate.decision_artifact.artifact_id,
                })
            })
            .collect();
        rows.extend(skipped.iter().cloned().map(|mut row| {
            row["status"] = serde_json::json!("excluded");
            row
        }));
        let mut summary = scope.identity_report();
        summary["selected_run_ids"] = serde_json::to_value(selected_run_ids)?;
        summary["excluded_decisions"] = serde_json::to_value(skipped.len())?;
        (scan, rows, Some(summary))
    } else {
        let scan = scan_calibration_decisions(&store, (None, None))?;
        let mut rows = Vec::new();
        let mut seen = BTreeSet::new();
        // 每个 Run 只取一个 Decision 作为 readiness 入口；重复 Decision 或非 Paper purpose
        // 不进入成熟度统计，单个 Run 的细节由 calibration_run_readiness 继续校验。
        for artifact in &scan.artifacts {
            let Some(run_id) = artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
            else {
                continue;
            };
            if !seen.insert(run_id.clone()) {
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
        (scan, rows, None)
    };
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
    } else if cohort.is_some() && scan.truncated {
        gap["next_step"] = serde_json::json!("narrow_training_window");
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
        "label_timing_basis":CALIBRATION_LABEL_TIMING,
        "scan_limit":CALIBRATION_SCAN_LIMIT, "decisions_scanned":scan.artifacts.len(),
        "scan_limit_reached":scan.truncated,
        "cohort":cohort_report,
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
            Ok(
                calibration_labels(store, run_id, &decision, schedule, a, &outcome)
                    .map(|_| ())
                    .map_err(|error| format!("{error:#}")),
            )
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

    use akzio_domain::{RunPurpose, WeightPpm};
    use akzio_execution::{ForecastCalibrationScope, AssetRiskCalibration, FrozenForecastCalibration, ForecastCalibrationBin, PortfolioRiskModel};

    // Canonical empty Store is not a source of fabricated mature Outcomes.
    // Historical isolated Store rejection is covered by akzio-store tests;
    // this CLI must not create a new Debug environment just for its fixture.
    #[test]
    fn empty_canonical_store_does_not_report_mature_calibration_samples() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.akzio/policy-readiness-tests").join(RunId::new().0);
        let store = Store::open(&path).unwrap();
        let canonical = calibration_readiness_report(&path, 30, Path::new("unused.toml"), None).unwrap();
        assert_eq!(canonical["calibration_eligible"], true);
        assert_eq!(canonical["gap"]["next_step"], "collect_real_outcomes");
        assert_eq!(canonical["gap"]["mature_runs"], 0);
        assert!(store.debug_environment().unwrap().is_none());
        assert!(store.active_decision_policy().unwrap().is_none());
    }

    fn calibration_fixture_attempt(
        store: &Store,
        purpose: RunPurpose,
    ) -> akzio_store::ClaimedAttempt {
        use akzio_domain::{
            ArtifactOrigin, FailureDisposition, RetryPolicy, TaskBudget, TaskId, TaskRecipeId,
            WorkflowGraph, WorkflowNode, DOMAIN_SCHEMA_VERSION,
        };
        let run_id = RunId::new();
        let now = Utc::now();
        let node = WorkflowNode {
            spec: None,
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("gate.decision").unwrap(),
            contract_hash: None,
            objective: "offline calibration selection fixture".into(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 1,
            budget: TaskBudget {
                max_input_tokens: 1,
                max_output_tokens: 1,
                max_wall_time_secs: 1,
                max_tool_calls: akzio_domain::budget::ToolCallLimit::Limited(0),
            },
            retry: RetryPolicy::none(),
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let graph = WorkflowGraph {
            definition_version: None,
            schema_version: DOMAIN_SCHEMA_VERSION,
            topology_id: "offline-calibration-selection-fixture".into(),
            nodes: vec![node],
            agent_budgets: Default::default(),
        };
        let provenance = ArtifactProvenance {
            source_family: "akzio.runtime".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        };
        let graph_artifact = Artifact::new(
            ArtifactKind::WorkflowGraph,
            store.stage_json(&graph).unwrap(),
            "runtime.workflow",
            ArtifactLifecycle::RunScoped,
            provenance.clone(),
            Some(ArtifactOrigin {
                run_id: Some(run_id.clone()),
                task_id: None,
                attempt_id: None,
                contract_hash: None,
            }),
            vec![],
            now,
        )
        .unwrap();
        store
            .commit_workflow(&akzio_store::WorkflowCommit {
                run: StoredRun {
                    run_id: run_id.clone(),
                    purpose,
                    topology_id: graph.topology_id,
                    graph_artifact_id: graph_artifact.artifact_id.clone(),
                    created_at: now,
                },
                graph: graph_artifact,
                nodes: graph.nodes,
            })
            .unwrap();
        store
            .claim_next_task_for_workload(
                "calibration-test",
                now,
                chrono::Duration::minutes(1),
                akzio_store::TaskWorkload::Any,
            )
            .unwrap()
            .unwrap()
    }

    fn calibration_fixture_decision(decided_at: DateTime<Utc>, context: ArtifactRef) -> Decision {
        use akzio_domain::{Forecast, ForecastThesis, TargetPortfolio, DOMAIN_SCHEMA_VERSION};
        Decision {
            schema_version: DOMAIN_SCHEMA_VERSION,
            decision_context: context,
            summary: "Synthetic calibration decision".into(),
            targets: TargetPortfolio::zeroed(),
            confidence_ppm: 0,
            forecasts: Asset::EXECUTABLE
                .into_iter()
                .flat_map(|asset| {
                    DecisionHorizon::ALL
                        .into_iter()
                        .map(move |horizon| Forecast {
                            asset,
                            horizon,
                            positive_return_probability_ppm: 500_000,
                            expected_return_ppm: 0,
                            thesis: Some(ForecastThesis {
                                thesis_valid_until: decided_at + chrono::Duration::days(30),
                                expected_holding_period_days: horizon.trading_days(),
                                exit_condition: "Review after the horizon".into(),
                                invalidation_conditions: vec!["New evidence".into()],
                            }),
                        })
                })
                .collect(),
            research_plan: None,
            created_at: decided_at,
        }
    }

    fn commit_calibration_fixture_decisions(
        store: &Store,
        purpose: RunPurpose,
        decided_at: DateTime<Utc>,
        count: usize,
    ) -> RunId {
        let attempt = calibration_fixture_attempt(store, purpose);
        let mut decision = calibration_fixture_decision(
            decided_at,
            ArtifactRef {
                artifact_id: ArtifactId(ContentHash::of_bytes(
                    b"unused context for maturity report",
                )),
                kind: ArtifactKind::DecisionContext,
            },
        );
        let provenance = ArtifactProvenance {
            source_family: "akzio.runtime".into(),
            observed_at: None,
            retrieved_at: decided_at,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        };
        let lifecycle = if purpose == RunPurpose::Paper {
            ArtifactLifecycle::Canonical
        } else {
            ArtifactLifecycle::RunScoped
        };
        let artifacts: Vec<_> = (0..count)
            .map(|index| {
                decision.summary = format!("Synthetic calibration decision {index}");
                Artifact::new(
                    ArtifactKind::Decision,
                    store.stage_json(&decision).unwrap(),
                    "decision.bound",
                    lifecycle,
                    provenance.clone(),
                    Some(attempt.permit.artifact_origin()),
                    vec![],
                    decided_at,
                )
                .unwrap()
            })
            .collect();
        store
            .commit_attempt(
                &attempt.permit,
                &artifacts,
                akzio_domain::TaskStatus::Succeeded,
                Utc::now(),
            )
            .unwrap();
        attempt.run_id
    }

    /// WHY THIS TEST EXISTS (Task #11): PositionPlan decisions used to fill the recent scan before Paper selection, hiding older committed calibration candidates.
    /// Do not weaken or delete this assertion.
    #[test]
    fn calibration_readiness_does_not_lose_paper_decisions_to_position_plan_history() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/calibration-selection-tests")
            .join(RunId::new().0);
        let store = Store::open(&path).unwrap();
        let decision_at = parse_calibration_time("2026-06-01T15:00:00Z", "fixture").unwrap();
        let paper = commit_calibration_fixture_decisions(&store, RunPurpose::Paper, decision_at, 1);
        commit_calibration_fixture_decisions(
            &store,
            RunPurpose::PositionPlan,
            decision_at + chrono::Duration::days(1),
            501,
        );
        let report =
            calibration_readiness_report(&path, 30, Path::new("unused.toml"), None).unwrap();
        assert_eq!(report["runs"][0]["run_id"], serde_json::json!(paper));
        assert_eq!(report["decisions_scanned"], 1);
        assert_eq!(report["scan_limit_reached"], false);
    }

    /// WHY THIS TEST EXISTS (Task #11): A full page with tied timestamps must advance by Artifact ID, while window filtering precedes the limit and actual truncation requires a lookahead row.
    /// Do not weaken or delete this assertion.
    #[test]
    fn calibration_pagination_preserves_ties_and_reports_actual_truncation() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/calibration-selection-tests")
            .join(RunId::new().0);
        let store = Store::open(&path).unwrap();
        let decided_at = parse_calibration_time("2026-06-01T15:00:00.123Z", "fixture").unwrap();
        let window = (Some(decided_at), Some(decided_at));
        commit_calibration_fixture_decisions(&store, RunPurpose::Paper, decided_at, 500);
        commit_calibration_fixture_decisions(
            &store,
            RunPurpose::Paper,
            decided_at + chrono::Duration::seconds(1),
            1,
        );
        let page = scan_calibration_decisions(&store, window).unwrap();
        assert_eq!(page.artifacts.len(), 500);
        assert!(!page.truncated);
        assert!(page
            .artifacts
            .windows(2)
            .all(|pair| pair[0].artifact_id > pair[1].artifact_id));
        commit_calibration_fixture_decisions(&store, RunPurpose::Paper, decided_at, 1);
        assert!(
            scan_calibration_decisions(&store, window)
                .unwrap()
                .truncated
        );
    }

    fn commit_calibration_fixture_outcome(
        store: &Store,
        decided_at: DateTime<Utc>,
        model_id: &str,
        contract_hash: &ContentHash,
        marked: bool,
    ) -> RunId {
        use akzio_domain::{
            HardBlocker, OutcomeExecutionLineage, TaskStatus, DOMAIN_SCHEMA_VERSION,
        };
        use serde_json::json;

        let attempt = calibration_fixture_attempt(store, RunPurpose::Paper);
        let run_id = attempt.run_id.clone();
        let now = Utc::now();
        let stage = |kind,
                     lifecycle,
                     payload: serde_json::Value,
                     sources,
                     permit: &akzio_domain::TaskWritePermit,
                     at| {
            Artifact::new(
                kind,
                store.stage_json(&payload).unwrap(),
                "calibration.fixture",
                lifecycle,
                ArtifactProvenance {
                    source_family: if kind == ArtifactKind::NormalizedEvidence {
                        "alpaca"
                    } else {
                        "akzio.runtime"
                    }
                    .into(),
                    observed_at: Some(at),
                    retrieved_at: at,
                    source_uri: None,
                    confidence_ppm: 1_000_000,
                    producer_contract_hash: permit.contract_hash.clone(),
                },
                Some(permit.artifact_origin()),
                sources,
                at,
            )
            .unwrap()
        };
        let reference = |artifact: &Artifact| ArtifactRef {
            artifact_id: artifact.artifact_id.clone(),
            kind: artifact.kind,
        };
        let baseline = decided_at.date_naive();
        let closes = if baseline == "2026-11-25".parse::<NaiveDate>().unwrap() {
            [
                "2026-11-27T18:00:00Z",
                "2026-11-30T21:00:00Z",
                "2026-12-01T21:00:00Z",
                "2026-12-02T21:00:00Z",
                "2026-12-03T21:00:00Z",
            ]
            .map(|at| at.parse::<DateTime<Utc>>().unwrap())
        } else {
            // The other fixture dates are in daylight time. Exclude the venue holidays in that span.
            use chrono::Datelike;
            let holidays =
                ["2026-06-19", "2026-07-03"].map(|date| date.parse::<NaiveDate>().unwrap());
            baseline
                .iter_days()
                .skip(1)
                .filter(|date| date.weekday().number_from_monday() <= 5 && !holidays.contains(date))
                .take(5)
                .map(|date| date.and_hms_opt(20, 0, 0).unwrap().and_utc())
                .collect::<Vec<_>>()
                .try_into()
                .unwrap()
        };
        let scheduled = closes
            .into_iter()
            .map(|at| (at.date_naive(), at))
            .collect::<BTreeMap<_, _>>();
        let research_bars = Asset::EXECUTABLE.into_iter().map(|asset| {
            let mut value = json!({});
            if marked {
                value["forecast_calendar"] = json!({"version":1,"availability_lag_minutes":20,"session_closes":scheduled});
            } else {
                value["forecast_session_closes"] = json!({"historical": "not a close"});
            }
            stage(ArtifactKind::NormalizedEvidence, ArtifactLifecycle::RunScoped, json!({
                "resource":format!("bars:{}:1d:2025-10-01:252", asset.symbol()), "source":"alpaca", "value":value,
            }), vec![], &attempt.permit, decided_at)
        }).collect::<Vec<_>>();
        let refs = research_bars.iter().map(reference).collect::<Vec<_>>();
        let manifest = stage(
            ArtifactKind::ContextManifest,
            ArtifactLifecycle::RunScoped,
            json!({
                "schema_version":DOMAIN_SCHEMA_VERSION, "contract_hash":ContentHash::of_bytes(b"fixture"),
                "selections":refs.iter().map(|artifact| json!({"artifact":artifact,"reason":"calendar fixture","estimated_tokens":1})).collect::<Vec<_>>(),
                "total_bytes":256,"estimated_tokens":4,"input_hash":ContentHash::of_bytes(b"fixture")
            }),
            refs,
            &attempt.permit,
            decided_at,
        );
        let turn = stage(
            ArtifactKind::AgentTurn,
            ArtifactLifecycle::RunScoped,
            json!({
                "request": {"purpose": "research.synthesizer"},
                "capability_snapshot": {"provider_id": OPENAI_RESPONSES_PROVIDER_ID},
                "response": {"telemetry": {"actual_model": model_id}},
                "contract_hash": contract_hash,
            }),
            vec![],
            &attempt.permit,
            decided_at,
        );
        let context = stage(
            ArtifactKind::DecisionContext,
            ArtifactLifecycle::Canonical,
            json!({
                "schema_version": DOMAIN_SCHEMA_VERSION, "decision_id": akzio_domain::DecisionId::new(),
                "run_id": run_id, "claims": [], "critiques": [], "evidence": [], "policy_influences": [],
                "material_conflicts": [], "hard_blockers": [HardBlocker::MissingEvidence], "soft_warnings": [],
                "decision_policy_hash": ContentHash::of_bytes(b"fixture-policy"),
                "target": akzio_domain::TargetPortfolio::zeroed(), "created_at": decided_at,
            }),
            vec![reference(&manifest), reference(&turn)],
            &attempt.permit,
            decided_at,
        );
        let mut frozen_decision = calibration_fixture_decision(decided_at, reference(&context));
        for forecast in &mut frozen_decision.forecasts {
            forecast.thesis.as_mut().unwrap().thesis_valid_until =
                closes[usize::from(forecast.horizon.trading_days()) - 1];
        }
        let decision = stage(
            ArtifactKind::Decision,
            ArtifactLifecycle::Canonical,
            json!(frozen_decision),
            vec![reference(&context)],
            &attempt.permit,
            decided_at,
        );
        let execution = stage(
            ArtifactKind::ExecutionContext,
            ArtifactLifecycle::Canonical,
            json!({
                "schema_version": DOMAIN_SCHEMA_VERSION, "run_id": run_id, "decision_context": reference(&context),
                "frozen": false, "created_at": decided_at,
            }),
            vec![reference(&context)],
            &attempt.permit,
            decided_at,
        );
        let verdict = stage(
            ArtifactKind::ExecutionVerdict,
            ArtifactLifecycle::Canonical,
            json!({
                "verdict": "no_order", "no_order": {"execution_context": reference(&execution),
                    "blockers": [HardBlocker::MissingEvidence], "created_at": decided_at},
            }),
            vec![reference(&execution)],
            &attempt.permit,
            decided_at,
        );
        let schedule = OutcomeSchedule {
            schema_version: DOMAIN_SCHEMA_VERSION,
            outcome_id: akzio_domain::OutcomeId::new(),
            decision: reference(&decision),
            decision_context: reference(&context),
            execution_context: reference(&execution),
            execution: OutcomeExecutionLineage::NoOrder {
                execution_verdict: reference(&verdict),
            },
            baseline_trading_day: decided_at.date_naive(),
            created_at: decided_at,
        };
        let schedule_artifact = stage(
            ArtifactKind::OutcomeSchedule,
            ArtifactLifecycle::Canonical,
            json!(&schedule),
            vec![
                reference(&decision),
                reference(&context),
                reference(&execution),
                reference(&verdict),
            ],
            &attempt.permit,
            decided_at,
        );
        let mut artifacts = research_bars;
        artifacts.extend([
            manifest,
            turn,
            context,
            decision,
            execution,
            verdict,
            schedule_artifact.clone(),
        ]);
        store
            .commit_attempt(&attempt.permit, &artifacts, TaskStatus::Succeeded, now)
            .unwrap();
        assert_eq!(store.ensure_pending_outcome_workers(now).unwrap(), 1);
        let worker = store
            .claim_next_task_for_workload(
                "calibration-outcome-fixture",
                now,
                chrono::Duration::minutes(1),
                akzio_store::TaskWorkload::Outcome,
            )
            .unwrap()
            .unwrap();
        let days = [
            schedule.baseline_trading_day,
            closes[0].date_naive(),
            closes[2].date_naive(),
            closes[4].date_naive(),
        ];
        let observed_at = days[3].and_hms_opt(21, 43, 0).unwrap().and_utc();
        let baseline_close = days[0].and_time(closes[4].time()).and_utc();
        let mut expected_closes = scheduled.clone();
        expected_closes.insert(days[0], baseline_close);
        let observed_closes: BTreeMap<_, _> = days
            .iter()
            .map(|day| (*day, expected_closes[day]))
            .collect();
        let missing_dates: Vec<_> = expected_closes
            .keys()
            .filter(|date| !observed_closes.contains_key(date))
            .copied()
            .collect();

        let need = stage(
            ArtifactKind::EvidenceNeed,
            ArtifactLifecycle::RunScoped,
            json!({}),
            vec![],
            &worker.permit,
            observed_at,
        );
        let raw = stage(
            ArtifactKind::RawEvidence,
            ArtifactLifecycle::RunScoped,
            json!({}),
            vec![],
            &worker.permit,
            observed_at,
        );
        let mut market_evidence = Vec::new();
        for asset in Asset::EXECUTABLE {
            let received_at = if asset == Asset::EXECUTABLE[3] {
                observed_at
            } else {
                observed_at - chrono::Duration::minutes(19)
            };
            let bars = days.map(|day| json!({"t": day, "c": "100"}));
            let uri = "fixture://cohort/bars?limit=252&adjustment=raw&feed=iex";
            let calendar_source = "fixture://cohort/calendar";
            let mut value = json!({"bars":bars, "session_closes":observed_closes});
            if marked {
                value["symbol"] = json!(asset.symbol());
                value["feed"] = json!("iex");
                value["decision_cutoff"] = json!(received_at);
                value["price_basis"] = json!("raw_requires_stage_action_check");
                value["request_url"] = json!(uri);
                value["calendar_source"] = json!(calendar_source);
                value["bar_session_coverage"] = json!({
                    "version":1, "expected_closes":expected_closes,
                    "observed_dates":days, "missing_dates":missing_dates,
                    "outside_window_dates":[], "feed":"iex", "adjustment":"raw",
                    "provider_revision":null, "calendar_source":calendar_source,
                    "cutoff":received_at, "requested_limit":252, "provider_has_more":false
                });
            }
            market_evidence.push(stage(ArtifactKind::NormalizedEvidence, ArtifactLifecycle::RunScoped, json!({
                "schema_version": DOMAIN_SCHEMA_VERSION, "source": "alpaca", "resource": format!("bars:{}:1d:{}:252:raw:{}", asset.symbol(), days[0], days[3]),
                "need": reference(&need), "raw": reference(&raw), "observed_at": received_at,
                "time_basis": {"available_at": received_at, "retrieved_at": received_at,
                    "decision_clock": {"decision_cutoff": received_at}},
                "contamination_certificate": {"available_at_or_before_cutoff": true, "event_at_or_before_cutoff": true,
                    "release_at_or_before_cutoff": true, "retrieval_at_or_after_availability": true, "vintage_at_or_before_cutoff": true},
                "provenance": {"observed_at": received_at, "source_uri": uri, "dedupe_key": asset.symbol(), "citations": []},
                "quality": akzio_ingest::EvidenceQuality::default(), "value": value,
            }), vec![reference(&need), reference(&raw)], &worker.permit, observed_at));
        }
        for evidence in std::iter::once(&need)
            .chain(std::iter::once(&raw))
            .chain(&market_evidence)
        {
            store
                .write_task_artifact(
                    &worker.permit,
                    evidence,
                    akzio_domain::LifecycleEventType::ArtifactCommitted,
                    now,
                )
                .unwrap();
        }
        let evidence_refs: Vec<_> = market_evidence.iter().map(reference).collect();
        let windows: Vec<_> = OutcomeHorizon::ALL.into_iter().zip(&days[1..]).map(|(horizon, day)| json!({
            "horizon": horizon, "observed_trading_day": day, "portfolio_return_ppm": 0, "benchmark_return_ppm": 0,
            "transaction_cost_ppm": 0, "slippage_ppm": 0, "utility_ppm": 0,
        })).collect();
        let outcome = stage(
            ArtifactKind::Outcome,
            ArtifactLifecycle::Canonical,
            json!({
                "schema_version": DOMAIN_SCHEMA_VERSION, "outcome_id": schedule.outcome_id, "schedule": reference(&schedule_artifact),
                "market_evidence": evidence_refs, "windows": windows, "sealed_at": observed_at,
            }),
            std::iter::once(reference(&schedule_artifact))
                .chain(evidence_refs)
                .collect(),
            &worker.permit,
            observed_at,
        );
        store
            .commit_outcomes(&worker.permit, &[outcome], now)
            .unwrap();
        run_id
    }

    /// WHY THIS TEST EXISTS (Task #11): Scoped readiness and collect must select the same exact Paper IDs, even with a former active Contract and a later decision outside the training window.
    /// Do not weaken or delete this assertion.
    #[test]
    fn calibration_readiness_and_collect_share_the_requested_cohort() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/calibration-selection-tests")
            .join(RunId::new().0);
        for command in [
            vec!["akzio", "calibration", "readiness", "--store", "store"],
            vec![
                "akzio",
                "calibration",
                "readiness",
                "--store",
                "store",
                "--cohort",
                "--contract-hash",
                "contract",
            ],
            vec![
                "akzio",
                "calibration",
                "collect",
                "--store",
                "store",
                "--risk-limits",
                "risk",
                "--training-end",
                "2026-06-30T23:59:59Z",
            ],
        ] {
            assert!(Cli::try_parse_from(command).is_ok());
        }
        let store = Store::open(&path).unwrap();
        let config_path = path.join("cohort-fixture.toml");
        fs::write(
            &config_path,
            r#"
[daemon]
store_root = "unused"
http_addr = "127.0.0.1:0"
[execution]
assets = ["TQQQ", "QQQ", "SOXX", "SOXL"]
[model]
provider = "openai_responses"
base_url = "http://127.0.0.1:0/v1"
model = "calibration-fixture"
api_key = "offline-test"
release_date = "2025-01-01"
knowledge_cutoff = "2025-01-01"
"#,
        )
        .unwrap();
        let now = Utc::now();
        akzio_research::ActiveResearchCatalogue::install(&store, now).unwrap();
        let purpose =
            akzio_domain::ContractPurpose::new(akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID)
                .unwrap();
        let mut active = store.active_contract(&purpose).unwrap().unwrap().contract;
        let historical_hash = active.contract_hash.clone();
        active.version += 1;
        active.contract_hash = active.expected_hash().unwrap();
        store
            .install_canonical_contract_upgrade(&historical_hash, &active, now)
            .unwrap();
        let mut config = read_config_file(&config_path).unwrap();
        resolve_model_configuration(&mut config).unwrap();
        let (model, _) = configured_synthesizer_identity(config.model.as_ref().unwrap()).unwrap();
        let historical_at = parse_calibration_time("2026-06-01T15:00:00Z", "fixture").unwrap();
        let current_at = historical_at + chrono::Duration::days(7);
        let historical = commit_calibration_fixture_outcome(
            &store,
            historical_at,
            &model,
            &historical_hash,
            true,
        );
        let current = commit_calibration_fixture_outcome(
            &store,
            current_at,
            &model,
            &active.contract_hash,
            true,
        );
        commit_calibration_fixture_outcome(
            &store,
            current_at + chrono::Duration::days(40),
            &model,
            &active.contract_hash,
            true,
        );
        commit_calibration_fixture_outcome(
            &store,
            current_at + chrono::Duration::days(19),
            &model,
            &active.contract_hash,
            true,
        );
        let legacy = commit_calibration_fixture_outcome(
            &store,
            historical_at + chrono::Duration::days(1),
            &model,
            &active.contract_hash,
            false,
        );
        commit_calibration_fixture_decisions(&store, RunPurpose::PositionPlan, current_at, 501);
        let risk = serde_json::from_value::<OfflineRiskLimits>(serde_json::json!({
            "min_confidence_ppm": 250000, "max_gross_weight_ppm": 500000, "maximum_execution_delay_ms": 300000,
            "minimum_process_quality_ppm": 900000, "min_probability_edge_ppm": 50000, "max_brier_score_ppm": 250000,
            "target_annualized_volatility_ppm": 300000, "max_portfolio_beta_ppm": 2000000, "max_expected_shortfall_ppm": 500000,
            "max_gap_loss_ppm": 500000, "max_capital_weight_ppm": 500000, "liquidity_weight_cap_ppm": 500000,
            "max_leveraged_holding_days": 1, "daily_reset_decay_ppm": 0,
        })).unwrap();
        let limits = persist_calibration_artifact(
            &store,
            ArtifactKind::CalibrationRiskLimits,
            &risk,
            vec![],
            now,
        )
        .unwrap();
        for (scope, expected) in [
            (None, &current),
            (Some(historical_hash.to_string()), &historical),
        ] {
            let requested = CalibrationScopeArgs {
                training_start: Some("2026-06-01T15:00:00Z".into()),
                training_end: Some("2026-06-30T23:59:59Z".into()),
                contract_hash: scope,
            };
            let readiness =
                calibration_readiness_report(&path, 30, &config_path, Some(&requested)).unwrap();
            let collect = collect_calibration_report(
                &path,
                limits.artifact_id.0.as_str(),
                30,
                &requested,
                &config_path,
            )
            .unwrap();
            assert_eq!(readiness["decisions_scanned"], 4);
            assert_eq!(collect["scan"]["decision_artifacts_considered"], 4);
            assert_eq!(readiness["cohort"]["excluded_decisions"], 3);
            let exclusions = collect["gaps"]["skipped_runs"].as_array().unwrap();
            assert!(exclusions
                .iter()
                .any(|row| row["reason"] == "contract_identity_mismatch"));
            assert_eq!(
                readiness["cohort"]["active_contract_hash"],
                serde_json::json!(active.contract_hash)
            );
            assert_eq!(
                readiness["cohort"]["active_contract_hash"],
                collect["identity"]["active_contract_hash"]
            );
            assert_eq!(
                readiness["cohort"]["selected_run_ids"],
                serde_json::json!([expected])
            );
            assert_eq!(
                readiness["cohort"]["selected_run_ids"],
                collect["scan"]["selected_run_ids"]
            );
        }

        // Both cutoffs follow the first three receipts and precede the old UTC day-end proxy.
        let cutoff = "2026-06-15T21:43:00Z";
        for (end, admitted) in [("2026-06-15T21:42:59Z", false), (cutoff, true)] {
            let requested = CalibrationScopeArgs {
                training_start: Some("2026-06-01T15:00:00Z".into()),
                training_end: Some(end.into()),
                contract_hash: None,
            };
            let readiness =
                calibration_readiness_report(&path, 1, &config_path, Some(&requested)).unwrap();
            let collect = collect_calibration_report(
                &path,
                limits.artifact_id.0.as_str(),
                1,
                &requested,
                &config_path,
            )
            .unwrap();
            let expected = if admitted {
                serde_json::json!([current])
            } else {
                serde_json::json!([])
            };
            assert_eq!(readiness["cohort"]["selected_run_ids"], expected);
            assert_eq!(collect["scan"]["selected_run_ids"], expected);
            let slot_counts = readiness["gap"]["slot_counts"].as_object().unwrap();
            assert_eq!(slot_counts.len(), 12);
            for (slot, count) in slot_counts {
                assert_eq!(count.as_u64().unwrap(), u64::from(admitted));
                assert_eq!(
                    collect["scan"]["sample_counts"][slot].as_u64().unwrap_or(0),
                    u64::from(admitted)
                );
            }
            assert_eq!(
                collect["scan"]["collected_forecast_samples"],
                12 * u64::from(admitted)
            );
            let excluded = readiness["runs"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|row| row["status"] == "excluded")
                .cloned()
                .map(|mut row| {
                    row.as_object_mut().unwrap().remove("status");
                    row
                })
                .collect::<Vec<_>>();
            assert_eq!(collect["gaps"]["skipped_runs"], serde_json::json!(excluded));
            let unmarked = excluded
                .iter()
                .find(|row| row["run_id"] == legacy.0)
                .unwrap();
            assert_eq!(unmarked["reason"], "maturity_label_missing");
            assert!(unmarked["detail"]
                .as_str()
                .unwrap()
                .starts_with("label_timing_unknown"));
            if admitted {
                let dataset_id = collect["dataset_artifact"].as_str().unwrap();
                let artifact =
                    calibration_artifact(&store, dataset_id, ArtifactKind::CalibrationDataset)
                        .unwrap();
                let outcome = store.outcome_for_run(&current).unwrap().unwrap();
                assert!(artifact.source_refs.contains(&ArtifactRef {
                    artifact_id: outcome.artifact_id,
                    kind: ArtifactKind::Outcome,
                }));
                let dataset: StoredCalibrationDataset =
                    serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
                assert_eq!(
                    dataset.input.training_end,
                    cutoff.parse::<DateTime<Utc>>().unwrap()
                );
                assert_eq!(dataset.input.forecasts.len(), 12);
                assert_eq!(collect["scan"]["common_price_points"], 4);
                assert_eq!(collect["scan"]["proven_adjacent_risk_returns"], 1);
                assert_eq!(
                    dataset.input.algorithm_version,
                    ADJACENT_SESSION_RISK_VERSION
                );
                let calendar = dataset.input.raw_price_coverage.as_ref().unwrap();
                assert_eq!(calendar.feed, HistoricalRiskFeed::Iex);
                assert_eq!(calendar.source_evidence.len(), 4);
                let (previous, pair) = calendar.adjacent_sessions.first_key_value().unwrap();
                assert_eq!(*previous, current_at.date_naive());
                assert_eq!(
                    pair.following_date,
                    current_at.date_naive().succ_opt().unwrap()
                );
                assert_eq!(pair.evidence_received_at, dataset.input.training_end);

                assert!(dataset
                    .input
                    .forecasts
                    .iter()
                    .all(|sample| sample.realized_at == dataset.input.training_end));
                assert_eq!(dataset.report["label_proofs"], collect["label_proofs"]);
            } else {
                let late = excluded
                    .iter()
                    .find(|row| row["run_id"] == current.0)
                    .unwrap();
                assert_eq!(late["reason"], "training_window_incomplete");
                assert_eq!(collect["status"], "BLOCKED");
                assert!(collect["dataset_artifact"].is_null());
            }
        }

        let requested = CalibrationScopeArgs {
            training_start: None,
            training_end: None,
            contract_hash: Some(historical_hash.to_string()),
        };
        let collect = collect_calibration_report(
            &path,
            limits.artifact_id.0.as_str(),
            30,
            &requested,
            &config_path,
        )
        .unwrap();
        assert_eq!(
            collect["scan"]["selected_run_ids"],
            serde_json::json!([historical])
        );
        assert_eq!(collect["identity"]["training_end"], "2026-06-08T21:43:00Z");
    }

    // 组装仅供身份比较测试的结构有效 Policy；合成数值不会写入或激活 Store。
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

    // 即使 Policy 本身 decision-capable，Synthesizer Contract 不匹配仍应阻断；
    // 将测试对象改成当前 hash 后只改变资格投影，不会更新 active head。
    #[test]
    fn old_policy_is_blocked_by_new_synthesizer_contract_without_activation() {
        let store = Store::open(Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../.akzio/policy-contract-tests").join(RunId::new().0)).unwrap();
        let policy = policy_with_positive_calibration_bin(Utc::now());
        policy.validate().unwrap();
        assert!(policy.decision_capable());
        let current = akzio_daemon::canonical_synthesizer_contract_hash(&store).unwrap();
        let mut loaded = LoadedDecisionPolicy {
            policy, active_hash: None, status: "ready_for_current_decision".into(),
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

    // 构造只供分类测试使用的 NoOrder schedule，日期是 fixture 标签，不是实际市场数据。
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

    // 固定旧文件入口被拒绝、Outcome 成熟度按已持久化 horizon 统计的回归边界。
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

    /// WHY THIS TEST EXISTS (Task #9): An early close and a delayed receipt set the concrete training cutoff, not UTC midnight.
    /// Do not weaken or delete this assertion.
    #[test]
    fn calibration_cutoff_uses_the_proven_receipt_of_the_exact_label() {
        let baseline = "2026-11-25".parse().unwrap();
        let session = ScheduledSession {
            date: "2026-11-27".parse().unwrap(),
            close_at: "2026-11-27T18:00:00Z".parse().unwrap(),
            available_at: "2026-11-27T18:20:00Z".parse().unwrap(),
        };
        let early: DateTime<Utc> = "2026-11-27T18:22:00Z".parse().unwrap();
        let latest: DateTime<Utc> = "2026-11-27T18:49:00Z".parse().unwrap();
        let final_receipt: DateTime<Utc> = "2026-12-04T22:00:00Z".parse().unwrap();
        let make_observations = |retrieved_at| {
            Asset::EXECUTABLE
                .into_iter()
                .map(|asset| {
                    (
                        asset,
                        BTreeMap::from([
                            (
                                baseline,
                                ObservedBar {
                                    price: MoneyMicros(100),
                                    retrieved_at,
                                },
                            ),
                            (
                                session.date,
                                ObservedBar {
                                    price: MoneyMicros(105),
                                    retrieved_at,
                                },
                            ),
                        ]),
                    )
                })
                .collect::<CalibrationBars>()
        };
        let sealed = make_observations(final_receipt);
        let mut partial = make_observations(early);
        let asset = Asset::EXECUTABLE[3];
        let bar = partial
            .get_mut(&asset)
            .unwrap()
            .get_mut(&session.date)
            .unwrap();
        bar.retrieved_at = latest;
        let actual = observed_label_time(&session, baseline, &sealed, &partial).unwrap();
        assert_eq!(actual, latest);
        let training_end: DateTime<Utc> = "2026-11-27T18:48:59Z".parse().unwrap();
        assert!(actual > training_end);
        let premature = make_observations(session.available_at - chrono::Duration::seconds(1));
        assert_eq!(
            observed_label_time(&session, baseline, &sealed, &premature),
            None
        );
        assert_eq!(
            observed_label_time(&session, baseline, &sealed, &sealed),
            Some(final_receipt)
        );
        partial
            .get_mut(&asset)
            .unwrap()
            .get_mut(&session.date)
            .unwrap()
            .price = MoneyMicros(106);
        assert_eq!(
            observed_label_time(&session, baseline, &sealed, &partial),
            None
        );
        partial.remove(&asset);
        assert_eq!(
            observed_label_time(&session, baseline, &sealed, &partial),
            None
        );
    }

    /// WHY THIS TEST EXISTS (Task #9): Readiness must derive a real receipt from fixed same-schedule Store evidence and exclude unmarked history.
    /// Do not weaken or delete this assertion.
    #[test]
    fn readiness_and_labels_use_the_committed_calendar_and_bar_proof() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/calibration-calendar-tests")
            .join(RunId::new().0);
        let store = Store::open(root).unwrap();
        let now = "2026-11-25T16:00:00Z".parse().unwrap();
        let contract_hash = ContentHash::of_bytes(b"fixture");
        let run_id =
            commit_calibration_fixture_outcome(&store, now, "fixture", &contract_hash, true);
        let decision_artifact = store
            .run_artifacts_by_kind(&run_id, ArtifactKind::Decision)
            .unwrap()
            .pop()
            .unwrap();
        let decision: Decision =
            serde_json::from_slice(&store.read_blob(&decision_artifact.blob).unwrap()).unwrap();
        let schedule_artifact = store.outcome_schedule_for_run(&run_id).unwrap().unwrap();
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&store.read_blob(&schedule_artifact.blob).unwrap()).unwrap();
        let sealed_artifact = store.outcome_for_run(&run_id).unwrap().unwrap();
        let sealed: Outcome =
            serde_json::from_slice(&store.read_blob(&sealed_artifact.blob).unwrap()).unwrap();
        let labels = calibration_labels(
            &store,
            &run_id,
            &decision,
            &schedule,
            &sealed_artifact,
            &sealed,
        )
        .unwrap();
        assert_eq!(
            labels.horizons[&DecisionHorizon::T5].realized_at,
            "2026-12-03T21:43:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            calibration_run_readiness(&store, &decision_artifact, &run_id).unwrap()["status"],
            "sealed"
        );
        let legacy =
            commit_calibration_fixture_outcome(&store, now, "fixture", &contract_hash, false);
        let decision = store
            .run_artifacts_by_kind(&legacy, ArtifactKind::Decision)
            .unwrap()
            .pop()
            .unwrap();
        let readiness = calibration_run_readiness(&store, &decision, &legacy).unwrap();
        assert_eq!(readiness["status"], "blocked");
        assert!(readiness["reason"]
            .as_str()
            .unwrap()
            .starts_with("label_timing_unknown"));
    }

    // 分别覆盖 pending、blocked、no_schedule、sealed 及统一 12 个槽位的样本计数。
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

// Collect 中经 Paper Run、Decision/Outcome、价格标签及 AgentTurn 最小模型身份筛选
// 的单个候选；这里尚未检查 active Policy 是否匹配，model_version_hash 取当前配置。
// 各字段拥有扫描结果，后续可据此按同一 Contract 合并样本。
struct CollectedCalibrationCandidate {
    run_id: RunId,
    decision_artifact: akzio_domain::Artifact,
    decision: Decision,
    outcome_ref: ArtifactRef,
    outcome: Outcome,
    labels: CalibrationLabels,
    provider_id: String,
    model_id: String,
    contract_hash: ContentHash,
}

type RiskPairReceipts = BTreeMap<(NaiveDate, NaiveDate), DateTime<Utc>>;

#[derive(Default)]
struct CalibrationRiskCoverage {
    feed: Option<HistoricalRiskFeed>,
    expected_closes: BTreeMap<NaiveDate, DateTime<Utc>>,
    expected_pairs: BTreeSet<(NaiveDate, NaiveDate)>,
    pairs_by_asset: BTreeMap<Asset, RiskPairReceipts>,
    prices: BTreeMap<Asset, BTreeMap<NaiveDate, MoneyMicros>>,
    price_conflicts: Vec<serde_json::Value>,
    sources: BTreeSet<ArtifactId>,
    excluded: BTreeMap<ArtifactId, &'static str>,
    conflicts: BTreeMap<ArtifactId, String>,
}

impl CalibrationRiskCoverage {
    fn observe_outcome(
        &mut self,
        store: &Store,
        outcome: &Outcome,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<()> {
        for reference in &outcome.market_evidence {
            if reference.kind != ArtifactKind::NormalizedEvidence
                || self.sources.contains(&reference.artifact_id)
                || self.excluded.contains_key(&reference.artifact_id)
                || self.conflicts.contains_key(&reference.artifact_id)
            {
                continue;
            }
            let artifact = store.artifact(&reference.artifact_id)?;
            let payload: NormalizedEvidencePayload =
                serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            let (asset, limit) =
                match akzio_ingest::GovernedResource::parse(payload.source, &payload.resource) {
                    Ok(akzio_ingest::GovernedResource::AlpacaBars {
                        asset,
                        limit,
                        raw_prices: true,
                        ..
                    }) => (asset, limit),
                    _ => continue,
                };
            match self.observe_source(asset, limit, &payload, start, end) {
                Ok(None) => {
                    self.sources.insert(reference.artifact_id.clone());
                }
                Ok(Some(reason)) => {
                    self.excluded.insert(reference.artifact_id.clone(), reason);
                }
                Err(error) => {
                    self.conflicts
                        .insert(reference.artifact_id.clone(), error.to_string());
                }
            }
        }
        Ok(())
    }

    fn observe_source(
        &mut self,
        asset: Asset,
        limit: u16,
        payload: &NormalizedEvidencePayload,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Option<&'static str>> {
        let Some(coverage) = BarSessionCoverage::read(&payload.value)? else {
            return Ok(Some("calendar_coverage_unknown"));
        };
        let bars = parse_venue_daily_bars(&payload.value)?;
        let received_at = payload.time_basis.retrieved_at;
        if coverage.adjustment != BarAdjustment::Raw
            || coverage.requested_limit != limit
            || coverage.cutoff != payload.time_basis.decision_clock.decision_cutoff
            || received_at != payload.observed_at
            || received_at < coverage.cutoff
            || payload.value["symbol"].as_str() != Some(asset.symbol())
            || coverage.provider_revision != payload.provenance.revision
            || payload.value["request_url"].as_str() != Some(payload.provenance.source_uri.as_str())
            || !bars.keys().eq(&coverage.observed_dates)
            || bars.values().any(|close| close.0 <= 0)
        {
            bail!("raw bar prices and frozen source coverage disagree for {asset}");
        }
        if received_at > end {
            return Ok(Some("received_after_training_end"));
        }
        let publication_lag = chrono::Duration::minutes(ALPACA_DAILY_BAR_AVAILABILITY_LAG_MINUTES);
        let closes = coverage
            .expected_closes
            .iter()
            .filter(|(date, close)| {
                **date >= start.date_naive()
                    && close
                        .checked_add_signed(publication_lag)
                        .is_some_and(|at| at <= end)
            })
            .map(|(&date, &close)| (date, close))
            .collect::<Vec<_>>();
        if closes.is_empty() {
            return Ok(Some("outside_training_window"));
        }
        let feed = match coverage.feed {
            Some(AlpacaMarketDataFeed::Iex) => HistoricalRiskFeed::Iex,
            Some(AlpacaMarketDataFeed::Sip) => HistoricalRiskFeed::Sip,
            None => return Ok(Some("market_data_feed_unknown")),
        };
        if self.feed.is_some_and(|existing| existing != feed) {
            bail!(
                "raw historical price feed conflicts: {:?} and {feed:?}",
                self.feed
            );
        }
        for &(date, close) in &closes {
            if self
                .expected_closes
                .get(&date)
                .is_some_and(|existing| *existing != close)
            {
                bail!("raw historical exchange close conflicts on {date}");
            }
        }
        let certified_prices = closes
            .iter()
            .filter_map(|(date, _)| bars.get(date).map(|price| (*date, *price)));
        let prices = self.prices.entry(asset).or_default();
        for (date, price) in certified_prices.clone() {
            if let Some(existing) = prices.get(&date).filter(|existing| **existing != price) {
                self.price_conflicts.push(serde_json::json!({
                    "asset": asset, "date": date,
                    "existing_close_micros": existing, "incoming_close_micros": price,
                }));
                bail!("raw historical prices conflict for {asset} on {date}");
            }
        }
        prices.extend(certified_prices);
        self.feed = Some(feed);
        self.expected_closes.extend(closes.iter().copied());
        let expected_pairs = closes.windows(2).map(|pair| (pair[0].0, pair[1].0));
        self.expected_pairs.extend(expected_pairs.clone());
        let pairs = self.pairs_by_asset.entry(asset).or_default();
        for pair in expected_pairs
            .filter(|(first, second)| bars.contains_key(first) && bars.contains_key(second))
        {
            pairs
                .entry(pair)
                .and_modify(|receipt| *receipt = (*receipt).min(received_at))
                .or_insert(received_at);
        }
        Ok(None)
    }

    fn calendar_conflicts(&self) -> Vec<(NaiveDate, NaiveDate, NaiveDate)> {
        use std::ops::Bound::Excluded;
        self.expected_pairs
            .iter()
            .filter_map(|&(first, second)| {
                let (&intervening, _) = self
                    .expected_closes
                    .range((Excluded(first), Excluded(second)))
                    .next()?;
                Some((first, intervening, second))
            })
            .collect()
    }

    fn calendar(&self) -> Option<HistoricalRiskCalendar> {
        // Intersect witnessed edges. Separate request windows cannot create a
        // synthetic edge. The last of four earliest eligible receipts is the
        // first time the raw four-asset return could be used.
        let dates = self.expected_closes.keys();
        let adjacent_sessions = dates
            .clone()
            .zip(dates.skip(1))
            .filter_map(|(&first, &second)| {
                let mut evidence_received_at = DateTime::<Utc>::MIN_UTC;
                for asset in Asset::EXECUTABLE {
                    let receipt = self.pairs_by_asset.get(&asset)?.get(&(first, second))?;
                    evidence_received_at = evidence_received_at.max(*receipt);
                }
                Some((
                    first,
                    HistoricalRiskPair {
                        following_date: second,
                        evidence_received_at,
                    },
                ))
            })
            .collect();
        Some(HistoricalRiskCalendar {
            version: 1,
            feed: self.feed?,
            expected_closes: self.expected_closes.clone(),
            adjacent_sessions,
            source_evidence: self
                .sources
                .iter()
                .map(|id| ArtifactRef {
                    artifact_id: id.clone(),
                    kind: ArtifactKind::NormalizedEvidence,
                })
                .collect(),
        })
    }
}

struct CalibrationCohort {
    provider_id: String,
    model_id: String,
    model_version_hash: ContentHash,
    active_contract_hash: Option<ContentHash>,
    contract_hash: Option<ContentHash>,
    window: (Option<DateTime<Utc>>, Option<DateTime<Utc>>),
}

impl CalibrationCohort {
    fn identity_report(&self) -> serde_json::Value {
        serde_json::json!({
            "provider_id": self.provider_id,
            "model_id": self.model_id,
            "model_route": "research.synthesizer",
            "configured_model_version_hash": self.model_version_hash,
            "contract_hash": self.contract_hash,
            "active_contract_hash": self.active_contract_hash,
            "requested_training_start": self.window.0,
            "requested_training_end": self.window.1,
        })
    }
}

fn calibration_cohort(
    store: &Store,
    config_path: &Path,
    requested: &CalibrationScopeArgs,
) -> Result<CalibrationCohort> {
    let mut config =
        read_config_file(config_path).context("load configured calibration cohort identity")?;
    resolve_model_configuration(&mut config)?;
    let model_config = config
        .model
        .as_ref()
        .context("calibration cohort requires [model] configuration")?;
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
            "calibration cohort requires release_date and knowledge_cutoff for the effective research.synthesizer route"
        );
    }
    let (model_id, model_version_hash) = configured_synthesizer_identity(model_config)?;
    let provider_id = model_config.provider_identity().as_str().to_owned();
    let purpose = akzio_domain::ContractPurpose::new(akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID)?;
    let active_contract_hash = store
        .active_contract(&purpose)?
        .map(|entry| entry.contract.contract_hash);
    let contract_hash = if let Some(raw) = &requested.contract_hash {
        let hash = ContentHash::new(raw.clone())?;
        let contract = store
            .contract_installation(&hash)?
            .context("calibration Contract is not installed")?;
        if contract.contract.purpose != purpose {
            bail!("calibration Contract must serve research.synthesizer");
        }
        Some(hash)
    } else {
        active_contract_hash.clone()
    };
    let start = requested
        .training_start
        .as_deref()
        .map(|value| parse_calibration_time(value, "training_start"))
        .transpose()?;
    let end = requested
        .training_end
        .as_deref()
        .map(|value| parse_calibration_time(value, "training_end"))
        .transpose()?;
    if start.zip(end).is_some_and(|(start, end)| start > end) {
        bail!("calibration training_start must not be after training_end");
    }
    Ok(CalibrationCohort {
        provider_id,
        model_id,
        model_version_hash,
        active_contract_hash,
        contract_hash,
        window: (start, end),
    })
}

struct CalibrationSelection {
    scan: CalibrationDecisionScan,
    selected: Vec<CollectedCalibrationCandidate>,
    skipped: Vec<serde_json::Value>,
}

fn select_calibration_cohort(
    store: &Store,
    scope: &CalibrationCohort,
) -> Result<CalibrationSelection> {
    let scan = scan_calibration_decisions(store, scope.window)?;
    let mut skipped = Vec::new();
    let mut candidates = Vec::new();
    for decision_artifact in scan.artifacts.iter().cloned() {
        let run_id = decision_artifact
            .origin
            .as_ref()
            .and_then(|origin| origin.run_id.clone())
            .context("committed calibration decision has no Run origin")?;
        let decision: Decision = match serde_json::from_slice(
            &store.read_blob(&decision_artifact.blob)?,
        ) {
            Ok(value) => value,
            Err(error) => {
                skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_decode","detail":error.to_string()}));
                continue;
            }
        };
        if decision.created_at != decision_artifact.created_at {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"decision_time_mismatch"}));
            continue;
        }
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
        if context_artifact.kind != ArtifactKind::DecisionContext
            || context.run_id != run_id
            || !decision_artifact
                .source_refs
                .contains(&decision.decision_context)
        {
            skipped.push(
                serde_json::json!({"run_id":run_id,"reason":"decision_context_lineage_mismatch"}),
            );
            continue;
        }
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
        let Some(schedule_artifact) = store.outcome_schedule_for_run(&run_id)? else {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"schedule_missing"}));
            continue;
        };
        if outcome.schedule.artifact_id != schedule_artifact.artifact_id {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"outcome_schedule_mismatch"}));
            continue;
        }
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
            synthesizer_identity_for_decision(store, &decision_artifact, &decision, &run_id)?
        else {
            skipped
                .push(serde_json::json!({"run_id":run_id,"reason":"synthesizer_identity_missing"}));
            continue;
        };
        if identity.provider_id != scope.provider_id || identity.model_id != scope.model_id {
            skipped.push(serde_json::json!({
                "run_id":run_id,
                "reason":"model_identity_mismatch",
                "expected_model":scope.model_id,
                "actual_model":identity.model_id,
                "expected_provider":scope.provider_id,
                "actual_provider":identity.provider_id,
            }));
            continue;
        }
        if scope.contract_hash.as_ref() != Some(&identity.contract_hash) {
            skipped.push(serde_json::json!({"run_id":run_id,"reason":"contract_identity_mismatch", "actual_contract_hash": identity.contract_hash}));
            continue;
        }
        if scope
            .window
            .0
            .is_some_and(|start| decision.created_at < start)
            || scope.window.1.is_some_and(|end| decision.created_at > end)
        {
            skipped
                .push(serde_json::json!({"run_id":run_id,"reason":"training_window_incomplete"}));
            continue;
        }
        let labels = match calibration_labels(
            store,
            &run_id,
            &decision,
            &schedule,
            &outcome_artifact,
            &outcome,
        ) {
            Ok(labels) => labels,
            Err(error) => {
                skipped.push(serde_json::json!({
                    "run_id": run_id, "reason": "maturity_label_missing", "detail": format!("{error:#}"),
                }));
                continue;
            }
        };
        if scope.window.1.is_some_and(|end| {
            labels
                .horizons
                .values()
                .any(|label| label.realized_at > end)
        }) {
            skipped
                .push(serde_json::json!({"run_id":run_id,"reason":"training_window_incomplete"}));
            continue;
        }
        candidates.push(CollectedCalibrationCandidate {
            run_id,
            decision_artifact,
            decision,
            outcome_ref: ArtifactRef {
                artifact_id: outcome_artifact.artifact_id,
                kind: ArtifactKind::Outcome,
            },
            outcome,
            labels,
            provider_id: identity.provider_id,
            model_id: identity.model_id,
            contract_hash: identity.contract_hash,
        });
    }
    candidates.sort_by(|left, right| left.run_id.cmp(&right.run_id));
    Ok(CalibrationSelection {
        scan,
        selected: candidates,
        skipped,
    })
}

fn collect_calibration_report(
    store_path: &Path,
    risk_limits_id: &str,
    min_samples: u32,
    requested: &CalibrationScopeArgs,
    config_path: &Path,
) -> Result<serde_json::Value> {
    // Collect 从 canonical Store 中筛选已封存、身份一致且四资产/三期限标签完整的
    // Paper Run，组装一个可复现的数据集；只有全部门槛满足时才写 CalibrationDataset，
    // 本函数从不直接生成或激活 DecisionPolicy。
    if min_samples == 0 {
        bail!("calibration collect requires min_samples > 0");
    }
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
    let scope = calibration_cohort(&store, config_path, requested)?;
    let CalibrationSelection {
        scan,
        selected,
        skipped,
    } = select_calibration_cohort(&store, &scope)?;
    let (requested_start, requested_end) = scope.window;
    let actual_start = requested_start.or_else(|| {
        selected
            .iter()
            .map(|candidate| candidate.decision.created_at)
            .min()
    });
    let actual_end = requested_end.or_else(|| {
        selected
            .iter()
            .flat_map(|candidate| candidate.labels.horizons.values())
            .map(|label| label.realized_at)
            .max()
    });
    let mut samples = Vec::new();
    let collected_runs: Vec<_> = selected
        .iter()
        .map(|candidate| candidate.run_id.0.clone())
        .collect();
    let mut sample_counts = BTreeMap::<String, usize>::new();
    let mut risk = CalibrationRiskCoverage::default();
    // 每个候选贡献完整的 4×3 forecast 集和可合并的日线价格；窗口外数据不进入训练面板，
    // 同一资产/日期的不同收盘价则保留冲突并阻断写入。
    for candidate in &selected {
        for forecast in &candidate.decision.forecasts {
            let label = &candidate.labels.horizons[&forecast.horizon];
            *sample_counts
                .entry(format!("{}:{:?}", forecast.asset, forecast.horizon))
                .or_default() += 1;
            samples.push(HistoricalForecastSample {
                source_run_id: candidate.run_id.0.clone(),
                asset: forecast.asset,
                horizon: forecast.horizon,
                forecast_at: candidate.decision.created_at,
                realized_at: label.realized_at,
                raw_probability_ppm: forecast.positive_return_probability_ppm,
                raw_expected_return_ppm: forecast.expected_return_ppm,
                realized_return_ppm: label.realized_returns[&forecast.asset],
                provenance: HistoricalForecastProvenance {
                    provider_id: candidate.provider_id.clone(),
                    model_id: candidate.model_id.clone(),
                    model_version_hash: scope.model_version_hash.clone(),
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
        if let (Some(start), Some(end)) = (actual_start, actual_end) {
            risk.observe_outcome(&store, &candidate.outcome, start, end)?;
        }
    }
    samples.sort_by(|left, right| {
        left.source_run_id
            .cmp(&right.source_run_id)
            .then_with(|| left.asset.cmp(&right.asset))
            .then_with(|| left.horizon.cmp(&right.horizon))
    });
    let common_price_points = risk
        .expected_closes
        .keys()
        .filter(|date| {
            Asset::EXECUTABLE.iter().all(|asset| {
                risk.prices
                    .get(asset)
                    .is_some_and(|bars| bars.contains_key(date))
            })
        })
        .count();
    let calendar_conflicts = risk.calendar_conflicts();
    let raw_price_coverage = risk.calendar();
    let mut dates_for_risk = BTreeSet::new();
    if let Some(coverage) = &raw_price_coverage {
        dates_for_risk.extend(
            coverage
                .adjacent_sessions
                .iter()
                .flat_map(|(&first, pair)| [first, pair.following_date]),
        );
    }
    let adjacent_return_count = raw_price_coverage
        .as_ref()
        .map_or(0, |coverage| coverage.adjacent_sessions.len());
    let price_series = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| HistoricalPriceSeries {
            asset,
            points: dates_for_risk
                .iter()
                .filter_map(|date| {
                    risk.prices
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
    // try_from 在极窄 usize 平台上可能失败；这里取 MAX 并饱和相乘，只会让
    // “足够样本”更难满足，不会整数溢出后错误地放行。
    let required_forecast_samples = usize::try_from(min_samples)
        .unwrap_or(usize::MAX)
        .saturating_mul(Asset::EXECUTABLE.len())
        .saturating_mul(DecisionHorizon::ALL.len());
    let enough_forecasts = sample_counts
        .values()
        .all(|count| *count >= usize::try_from(min_samples).unwrap_or(usize::MAX))
        && sample_counts.len() == Asset::EXECUTABLE.len() * DecisionHorizon::ALL.len();
    let enough_prices = adjacent_return_count >= usize::try_from(min_samples).unwrap_or(usize::MAX);
    let status = if !scan.truncated
        && scope.contract_hash.is_some()
        && actual_start.is_some()
        && actual_end.is_some()
        && !samples.is_empty()
        && samples.len() >= required_forecast_samples
        && enough_forecasts
        && enough_prices
        && risk.conflicts.is_empty()
        && calendar_conflicts.is_empty()
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
            policy_version: format!("offline_historical_v3:{CALIBRATION_LABEL_TIMING}"),
            algorithm_version: ADJACENT_SESSION_RISK_VERSION.to_owned(),
            provider_id: scope.provider_id.clone(),
            model_id: scope.model_id.clone(),
            model_version_hash: scope.model_version_hash.clone(),
            model_route: "research.synthesizer".to_owned(),
            contract_hash: scope
                .contract_hash
                .clone()
                .expect("samples have contract identity"),
            regime: "all".to_owned(),
            training_start: actual_start.unwrap_or_else(Utc::now),
            training_end: actual_end.unwrap_or_else(Utc::now),
            min_samples,
            source_runs: collected_runs.clone(),
            forecasts: samples,
            price_series,
            raw_price_coverage,
            risk_limits,
        };
        let canonical_input_hash = content_hash_json(&serde_json::to_value(&input)?)?;
        input_hash = Some(canonical_input_hash);
        dataset_input = Some(input);
    }
    let mut identity = scope.identity_report();
    identity["training_start"] = serde_json::to_value(actual_start)?;
    identity["training_end"] = serde_json::to_value(actual_end)?;
    identity["label_timing_basis"] = serde_json::json!(CALIBRATION_LABEL_TIMING);
    // Keep partial Outcome proofs in the hashed report and canonical refs in the Dataset DAG.
    let label_proofs: Vec<_> = selected
        .iter()
        .map(|candidate| {
            let horizons: Vec<_> = candidate
                .labels
                .horizons
                .iter()
                .map(|(horizon, label)| {
                    serde_json::json!({
                        "horizon": horizon,
                        "date": label.date,
                        "realized_at": label.realized_at,
                        "observed_outcome": label.outcome,
                    })
                })
                .collect();
            serde_json::json!({"run_id": candidate.run_id, "horizons": horizons})
        })
        .collect();
    let mut report = serde_json::json!({
        "status": status,
        "source_store": store_path,
        "risk_limits_artifact": risk_artifact.artifact_id,
        "input_hash": input_hash,
        "scan": {
            "decision_artifacts_considered": scan.artifacts.len(),
            "scan_limit": CALIBRATION_SCAN_LIMIT,
            "may_be_truncated": scan.truncated,
            "excluded_decisions": skipped.len(),
            "matured_runs": collected_runs.len(),
            "selected_run_ids": collected_runs,
            "collected_forecast_samples": sample_counts.values().sum::<usize>(),
            "sample_counts": sample_counts,
            "common_price_points": common_price_points,
            "proven_adjacent_risk_returns": adjacent_return_count,
            "raw_risk_feed": risk.feed,
        },
        "identity": identity,
        "label_proofs": label_proofs,
        "gaps": {
            "min_samples": min_samples,
            "enough_forecasts": enough_forecasts,
            "enough_prices": enough_prices,
            "price_conflicts": risk.price_conflicts,
            "raw_risk_excluded_sources": risk.excluded,
            "raw_risk_coverage_conflicts": risk.conflicts,
            "raw_risk_calendar_conflicts": calendar_conflicts,
            "skipped_runs": skipped,
        },
        "next_step": if status == "ready_for_build" {
            "akzio calibration build --store <store> --dataset <dataset-artifact-id>"
        } else if scan.truncated {
            "narrow the training window to scan the full cohort"
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
            sources.push(ArtifactRef {
                artifact_id: candidate.decision_artifact.artifact_id.clone(),
                kind: ArtifactKind::Decision,
            });
            sources.push(candidate.outcome_ref.clone());
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
    Ok(report)
}

fn parse_calibration_time(value: &str, field: &str) -> Result<DateTime<Utc>> {
    // 训练边界只接受带时区的 RFC3339，并统一为 UTC，避免本地时区改变样本筛选。
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("{field} must be RFC3339"))
}

fn relative_return_ppm(base: MoneyMicros, future: MoneyMicros) -> Result<i64> {
    // 只有正的基准和未来价格才能形成收益标签；先转 i128 再算 ppm，
    // 乘法实际用 saturating_mul（对两个 i64 价格的差而言不会接近 i128 极值），
    // checked_div 与 try_from 分别处理除法无结果及不能放进 i64 的情形。
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

// 从 Decision 血缘提取的最小模型身份；缺少其中任一字段时调用者不会把 Run 用作训练样本。
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

#[derive(Debug, Clone, Copy)]
struct ObservedBar {
    price: MoneyMicros,
    retrieved_at: DateTime<Utc>,
}

type CalibrationBars = BTreeMap<Asset, BTreeMap<NaiveDate, ObservedBar>>;

struct CalibrationLabel {
    date: NaiveDate,
    realized_at: DateTime<Utc>,
    outcome: ArtifactRef,
    realized_returns: BTreeMap<Asset, i64>,
}

struct CalibrationLabels {
    horizons: BTreeMap<DecisionHorizon, CalibrationLabel>,
}

fn calibration_labels(
    store: &Store,
    run_id: &RunId,
    decision: &Decision,
    schedule: &OutcomeSchedule,
    sealed_artifact: &Artifact,
    sealed: &Outcome,
) -> Result<CalibrationLabels> {
    let stored_schedule = store
        .outcome_schedule_for_run(run_id)?
        .context("outcome_schedule_missing")?;
    if stored_schedule.artifact_id != sealed.schedule.artifact_id
        || sealed.outcome_id != schedule.outcome_id
        || schedule.decision_context != decision.decision_context
        || !belongs_to_run(sealed_artifact, run_id)
        || !outcome_evidence_is_bound(sealed_artifact, sealed)
    {
        bail!("outcome_schedule_mismatch");
    }
    let calendar = ForecastCalendars::for_decision_context(store, &schedule.decision_context)?
        .context("label_timing_unknown: frozen venue calendar missing")?;
    let sessions = calendar.sessions_after(schedule.baseline_trading_day)?;
    let bars =
        calibration_bars(store, run_id, sealed, &sessions).context("outcome_bars_unusable")?;
    let mut horizons = BTreeMap::new();
    for (horizon, outcome_horizon) in DecisionHorizon::ALL.into_iter().zip(OutcomeHorizon::ALL) {
        let session = &sessions[usize::from(horizon.trading_days()) - 1];
        if !has_outcome_window(sealed, outcome_horizon, session.date)
            || decision.created_at >= session.close_at
            || decision
                .forecasts
                .iter()
                .filter(|forecast| forecast.horizon == horizon)
                .any(|forecast| {
                    forecast
                        .thesis
                        .as_ref()
                        .is_none_or(|thesis| thesis.thesis_valid_until != session.close_at)
                })
        {
            bail!("label_session_mismatch: {horizon:?}");
        }
        let realized_at = observed_label_time(session, schedule.baseline_trading_day, &bars, &bars)
            .with_context(|| format!("label_receipt_unknown: {horizon:?}"))?;
        // Receipt proof already requires positive baseline and endpoint prices for each asset.
        let realized_returns = Asset::EXECUTABLE
            .into_iter()
            .map(|asset| {
                let prices = &bars[&asset];
                relative_return_ppm(
                    prices[&schedule.baseline_trading_day].price,
                    prices[&session.date].price,
                )
                .with_context(|| format!("{asset} {horizon:?} realized return is invalid"))
                .map(|value| (asset, value))
            })
            .collect::<Result<_>>()?;
        horizons.insert(
            horizon,
            CalibrationLabel {
                date: session.date,
                realized_at,
                outcome: ArtifactRef {
                    artifact_id: sealed_artifact.artifact_id.clone(),
                    kind: ArtifactKind::Outcome,
                },
                realized_returns,
            },
        );
    }
    // A persisted partial can prove earlier availability only for the same
    // exact schedule and the same baseline/endpoint prices as the sealed label.
    for horizon in [OutcomeHorizon::T1, OutcomeHorizon::T3] {
        let Some(retrospective) = store.retrospective_for(run_id, &schedule.outcome_id, horizon)?
        else {
            continue;
        };
        let retrospective: akzio_domain::Retrospective =
            serde_json::from_slice(&store.read_blob(&retrospective.blob)?)?;
        if retrospective.validate().is_err() {
            continue;
        }
        let artifact = store.artifact(&retrospective.outcome.artifact_id)?;
        if artifact.kind != ArtifactKind::Outcome
            || artifact.lifecycle != ArtifactLifecycle::RunScoped
            || artifact.producer != sealed_artifact.producer
            || !belongs_to_run(&artifact, run_id)
        {
            continue;
        }
        let partial: Outcome = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        if partial.is_sealed()
            || partial.schedule != sealed.schedule
            || partial.outcome_id != sealed.outcome_id
            || partial.validate().is_err()
            || !outcome_evidence_is_bound(&artifact, &partial)
        {
            continue;
        }
        let Ok(receipts) = calibration_bars(store, run_id, &partial, &sessions) else {
            continue;
        };
        for (horizon, outcome_horizon) in DecisionHorizon::ALL.into_iter().zip(OutcomeHorizon::ALL)
        {
            let label = horizons.get_mut(&horizon).expect("sealed label validated");
            let session = &sessions[usize::from(horizon.trading_days()) - 1];
            if !has_outcome_window(&partial, outcome_horizon, label.date) {
                continue;
            }
            if let Some(at) =
                observed_label_time(session, schedule.baseline_trading_day, &bars, &receipts)
                    .filter(|at| *at < label.realized_at)
            {
                label.realized_at = at;
                label.outcome.artifact_id = artifact.artifact_id.clone();
            }
        }
    }
    Ok(CalibrationLabels { horizons })
}

fn has_outcome_window(outcome: &Outcome, horizon: OutcomeHorizon, date: NaiveDate) -> bool {
    outcome
        .windows
        .iter()
        .any(|window| window.horizon == horizon && window.observed_trading_day == date)
}

fn belongs_to_run(artifact: &Artifact, run_id: &RunId) -> bool {
    artifact
        .origin
        .as_ref()
        .and_then(|origin| origin.run_id.as_ref())
        == Some(run_id)
}

fn outcome_evidence_is_bound(artifact: &Artifact, outcome: &Outcome) -> bool {
    artifact.source_refs.contains(&outcome.schedule)
        && outcome
            .market_evidence
            .iter()
            .all(|reference| artifact.source_refs.contains(reference))
}

fn calibration_bars(
    store: &Store,
    run_id: &RunId,
    outcome: &Outcome,
    sessions: &[ScheduledSession; 5],
) -> Result<CalibrationBars> {
    let mut bars_by_asset = CalibrationBars::new();
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
        let GovernedResource::AlpacaBars {
            asset,
            raw_prices: true,
            ..
        } = GovernedResource::parse(payload.source, &payload.resource)?
        else {
            bail!("label bar must be an Alpaca raw daily series");
        };
        let retrieved_at = payload.time_basis.retrieved_at;
        if artifact.kind != reference.kind
            || !belongs_to_run(&artifact, run_id)
            || artifact.provenance.source_family != "alpaca"
            || retrieved_at != payload.observed_at
            || retrieved_at < payload.time_basis.available_at
        {
            bail!("label bar provenance unavailable for {asset}");
        }
        let bars = parse_scheduled_daily_bars(&payload.value, sessions)
            .with_context(|| format!("parse outcome bars for {asset}"))?;
        let destination = bars_by_asset.entry(asset).or_default();
        for (date, price) in bars {
            if sessions
                .iter()
                .any(|session| session.date == date && retrieved_at < session.available_at)
            {
                continue;
            }
            let entry = destination.entry(date).or_insert(ObservedBar {
                price,
                retrieved_at,
            });
            if entry.price != price {
                bail!("outcome bars contain conflicting {asset} price on {date}");
            }
            entry.retrieved_at = entry.retrieved_at.min(retrieved_at);
        }
    }
    Ok(bars_by_asset)
}

fn observed_label_time(
    session: &ScheduledSession,
    baseline: NaiveDate,
    sealed: &CalibrationBars,
    captured: &CalibrationBars,
) -> Option<DateTime<Utc>> {
    Asset::EXECUTABLE
        .into_iter()
        .try_fold(session.available_at, |latest, asset| {
            let sealed = sealed.get(&asset)?;
            let captured = captured.get(&asset)?;
            [baseline, session.date]
                .into_iter()
                .try_fold(latest, |latest, date| {
                    let expected = sealed.get(&date)?;
                    let observation = captured.get(&date)?;
                    (expected.price.0 > 0
                        && expected.price == observation.price
                        && (date != session.date || observation.retrieved_at >= session.available_at))
                        .then_some(latest.max(observation.retrieved_at))
                })
        })
}

#[cfg(test)]
mod risk_calendar_tests {
    use super::*;
    use serde_json::json;

    fn raw_bar_payload(
        asset: Asset,
        feed: &str,
        scheduled: &[(&str, u32)],
        missing: Option<&str>,
    ) -> NormalizedEvidencePayload {
        let cutoff: DateTime<Utc> = "2026-12-02T00:00:00Z".parse().unwrap();
        let expected_closes = scheduled
            .iter()
            .map(|(date, hour)| {
                let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
                (date, date.and_hms_opt(*hour, 0, 0).unwrap().and_utc())
            })
            .collect::<BTreeMap<_, _>>();
        let (mut observed_closes, missing_dates) = (
            expected_closes.clone(),
            missing.into_iter().collect::<Vec<_>>(),
        );
        if let Some(date) = missing {
            observed_closes.remove(&NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap());
        }
        let bars = observed_closes
            .keys()
            .map(|date| json!({"t":format!("{}T02:00:00Z", date.succ_opt().unwrap()), "c":100}))
            .collect::<Vec<_>>();
        let uri = format!("fixture://alpaca/bars?limit=252&adjustment=raw&feed={feed}");
        let id = ContentHash::of_bytes(b"risk-calendar-fixture");
        serde_json::from_value(json!({
            "schema_version":akzio_domain::DOMAIN_SCHEMA_VERSION,
            "source":"alpaca", "resource":format!("bars:{asset}:1d:2026-11-25:252:raw:2026-12-01"),
            "need":{"artifact_id":id,"kind":"evidence_need"}, "raw":{"artifact_id":id,"kind":"raw_evidence"},
            "observed_at":cutoff,
            "time_basis":{"available_at":cutoff,"retrieved_at":cutoff,"decision_clock":{"decision_cutoff":cutoff}},
            "contamination_certificate":{"available_at_or_before_cutoff":true,"event_at_or_before_cutoff":true,"release_at_or_before_cutoff":true,"retrieval_at_or_after_availability":true,"vintage_at_or_before_cutoff":true},
            "provenance":{"source_uri":uri,"observed_at":cutoff,"dedupe_key":"fixture","citations":[]},
            "quality":{"completeness_ppm":1_000_000,"citations_complete":true,"normalized":true},
            "value":{"symbol":asset.symbol(),"bars":bars,"session_closes":observed_closes,"feed":feed,"price_basis":"raw_requires_stage_action_check","request_url":uri,"decision_cutoff":cutoff,"calendar_source":"fixture://alpaca/calendar",
                "bar_session_coverage":{
                    "version":1,"expected_closes":expected_closes,
                    "observed_dates":observed_closes.keys().collect::<Vec<_>>(),"missing_dates":missing_dates,
                    "outside_window_dates":[],"feed":feed,"adjustment":"raw","provider_revision":null,
                    "calendar_source":"fixture://alpaca/calendar","cutoff":cutoff,"requested_limit":252,"provider_has_more":false
                }
            }
        })).unwrap()
    }

    /// WHY THIS TEST EXISTS (Task #33): Later receipt of a bar cannot contribute
    /// to an explicitly earlier risk training cutoff, even after market close.
    #[test]
    fn risk_window_excludes_closes_received_after_training_end() {
        let mut proof = CalibrationRiskCoverage::default();
        let start = "2026-11-25T00:00:00Z".parse().unwrap();
        let scheduled = [("2026-11-30", 21), ("2026-12-01", 21)];
        let source = raw_bar_payload(Asset::Qqq, "iex", &scheduled, None);
        for end in ["2026-12-01T21:05:00Z", "2026-12-01T21:21:00Z"] {
            assert_eq!(
                proof
                    .observe_source(Asset::Qqq, 252, &source, start, end.parse().unwrap())
                    .unwrap(),
                Some("received_after_training_end")
            );
            assert!(proof.calendar().is_none());
        }
        assert_eq!(
            proof
                .observe_source(Asset::Qqq, 252, &source, start, source.observed_at)
                .unwrap(),
            None
        );
        assert_eq!(proof.prices[&Asset::Qqq].len(), 2);
    }

    /// WHY THIS TEST EXISTS (Task #33): The CLI must preserve source feed and
    /// reject conflicts before fitting, even when the numerical closes agree.
    #[test]
    fn risk_prices_require_consistent_feed_and_witnessed_exchange_adjacency() {
        let mut proof = CalibrationRiskCoverage::default();
        let start = "2026-11-25T00:00:00Z".parse().unwrap();
        let end = "2026-12-02T00:00:00Z".parse().unwrap();
        let scheduled = [
            ("2026-11-25", 21),
            ("2026-11-27", 18),
            ("2026-11-30", 21),
            ("2026-12-01", 21),
        ];
        for asset in Asset::EXECUTABLE {
            let source = raw_bar_payload(
                asset,
                "iex",
                &scheduled,
                (asset == Asset::Qqq).then_some("2026-11-27"),
            );
            assert_eq!(
                proof
                    .observe_source(asset, 252, &source, start, end)
                    .unwrap(),
                None
            );
        }
        let calendar = proof.calendar().unwrap();
        assert_eq!(calendar.feed, HistoricalRiskFeed::Iex);
        assert_eq!(calendar.adjacent_sessions.len(), 1);
        assert_eq!(
            calendar
                .adjacent_sessions
                .first_key_value()
                .unwrap()
                .0
                .to_string(),
            "2026-11-30"
        );
        let sip = raw_bar_payload(Asset::Qqq, "sip", &scheduled, None);
        assert!(proof
            .observe_source(Asset::Qqq, 252, &sip, start, end)
            .unwrap_err()
            .to_string()
            .contains("feed conflicts"));
        let mut shifted_close = scheduled;
        shifted_close[1].1 = 21;
        let shifted = raw_bar_payload(Asset::Qqq, "iex", &shifted_close, None);
        assert!(proof
            .observe_source(Asset::Qqq, 252, &shifted, start, end)
            .unwrap_err()
            .to_string()
            .contains("close conflicts"));
        let contradictory = raw_bar_payload(Asset::Qqq, "iex", &[scheduled[0], scheduled[2]], None);
        assert_eq!(
            proof
                .observe_source(Asset::Qqq, 252, &contradictory, start, end)
                .unwrap(),
            None
        );
        assert_eq!(proof.calendar_conflicts().len(), 1);
        let mut unknown = raw_bar_payload(Asset::Qqq, "iex", &scheduled, None);
        let uri = unknown.provenance.source_uri.replace("&feed=iex", "");
        unknown.provenance.source_uri = uri.clone();
        unknown.value["request_url"] = json!(uri);
        unknown.value["feed"] = serde_json::Value::Null;
        unknown.value["bar_session_coverage"]["feed"] = serde_json::Value::Null;
        assert_eq!(
            proof
                .observe_source(Asset::Qqq, 252, &unknown, start, end)
                .unwrap(),
            Some("market_data_feed_unknown")
        );
        let mut legacy = raw_bar_payload(Asset::Qqq, "iex", &scheduled, None);
        legacy
            .value
            .as_object_mut()
            .unwrap()
            .remove("bar_session_coverage");
        assert_eq!(
            proof
                .observe_source(Asset::Qqq, 252, &legacy, start, end)
                .unwrap(),
            Some("calendar_coverage_unknown")
        );
    }
}
