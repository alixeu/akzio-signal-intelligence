// 文件导读：PaperExecution 串起 DecisionGate → ExecutionGate → PaperCommitment → Reconcile。
// Decision 只产出目标与 DecisionContext；ExecutionVerdict 仍可能是 NoOrder；Commitment 是
// 外部 I/O 前的确定性幂等记录；Reconcile 才接触 Paper broker，accepted/partially_filled
// 仍不是最终 fill。PositionPlan、Debug forbidden、缺 Policy 和闭市等待均在对应边界保留。
// Rust 机制：门面借用 `&Daemon`；async execution/reconcile 返回 Future；BTreeMap/Set 组装
// 依赖闭包，`Option` 表示快照/approval 缺失，枚举 `ExecutionVerdict`/`OrderSide` 保证
// 状态分支穷尽，`i128` 中间运算配合 `try_from` 防止金额溢出。

use crate::*;
use akzio_domain::{
    ComplianceActivitySnapshot, ComplianceControl, DependencyClosure, DependencyHealthStatus,
    DependencyKind, DependencySnapshot, FallbackPolicy, FinancialContentPolicy,
    InformationClassification, MarketClockSnapshot, OrderReceiptState, OrderSide, WeightPpm,
};
use akzio_execution::PreTradeSafetyEvidence;
use akzio_ingest::GovernedResource;

/// Deterministic decision, execution, commitment and reconciliation capability.
pub(crate) struct PaperExecution<'a> {
    daemon: &'a Daemon,
}

impl<'a> PaperExecution<'a> {
    // 绑定当前 Daemon 的 Store、Gate runtime 和 Paper 调度器；本门面不拥有独立状态。
    pub(crate) const fn new(daemon: &'a Daemon) -> Self {
        Self { daemon }
    }

    // 选择最终 ProposalReview 通过的提案（若存在），否则读取任务终态提案；拒绝审查
    // 会阻断 Decision。decide 返回并持久化 Decision 结果，但不代表已通过 ExecutionGate。
    pub(crate) fn decision_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let proposal = match self
            .daemon
            .store
            .final_proposal_review(&task.run_id, &task.node.task_id)?
        {
            // 审查通过时以 Review 绑定的精确 Proposal 为准；审查拒绝不能回退到旧提案，
            // 没有审查记录时才沿用任务的 DecisionProposal 终态输入。
            Some((_, review)) if review.accepted() => review.proposal,
            Some(_) => {
                return Err(DaemonError::InvalidInput(
                    "final proposal review rejected; revision limit exhausted".into(),
                ))
            }
            None => self
                .daemon
                .terminal_input(task, ArtifactKind::DecisionProposal)?,
        };
        self.daemon.decision_runtime.decide(&DecisionGateInput {
            permit: task.permit.clone(),
            proposal,
            now,
        })?;
        Ok(TaskCompletion::Committed)
    }

    // 获取 DecisionContext 和当前执行快照，先处理真实闭市延期，再把全部输入交给
    // ExecutionGate；Committed 只表示 ExecutionVerdict 已写入 Store，不等于订单受理或成交。
    pub(crate) async fn execution_gate(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let decision_context = self
            .daemon
            .terminal_input(task, ArtifactKind::DecisionContext)?;
        let (account_snapshot, quote_snapshot, market_clock_snapshot, quote_validation_error) =
            if self
                .daemon
                .production_evidence
                .contains_key(&EvidenceSource::Alpaca)
                && self.daemon.store.run_purpose(&task.run_id)? == RunPurpose::Paper
            {
                let refreshed = self.daemon.refresh_execution_snapshots(task, now).await?;
                (
                    refreshed.account,
                    refreshed.quotes,
                    refreshed.clock,
                    refreshed.quote_error,
                )
            } else {
                // 非生产 Alpaca 路径只读取任务已绑定的快照；生产 Paper 必须在此处
                // 重新刷新账户、报价和时钟，不能用研究阶段的旧观察替代执行时点数据。
                let (account, quotes, clock) = self.daemon.execution_snapshot_inputs(task)?;
                (account, quotes, clock, None)
            };
        let gate_now = Utc::now();
        // A real closed session is a scheduling wait, not execution permission.
        // No verdict/plan is committed; the next attempt reacquires all snapshots
        // and reevaluates the unchanged Decision validity and every Gate.
        if let Some(reference) = &market_clock_snapshot {
            // 仅对仍在 Decision validity 内、且时钟观察没有越过新鲜度边界的 Closed
            // session 延期；延期不提交 Verdict，下一次 Attempt 会重新取三类快照。
            let clock: MarketClockSnapshot = self.daemon.read_artifact_payload(reference)?;
            let decision: DecisionContext = self.daemon.read_artifact_payload(&decision_context)?;
            if let Some(wake) = closed_session_wake(
                &clock,
                decision.validity.as_ref(),
                self.daemon.execution_runtime.execution_policy(),
                gate_now,
            ) {
                return Ok(TaskCompletion::DeferredUntil(wake));
            }
        }
        // 缺少或不合格的执行安全输入由 execution runtime 形成持久化 NoOrder；这里不
        // 通过 daemon 层补造账户、报价、批准或风险证据。
        let pretrade_safety = self.pretrade_safety_evidence(
            task,
            &decision_context,
            account_snapshot.as_ref(),
            quote_snapshot.as_ref(),
            market_clock_snapshot.as_ref(),
            gate_now,
        )?;
        // Snapshot acquisition is a separately governed Evidence path. Until a
        // provider returns typed, task-bound snapshots, the execution runtime
        // emits a durable NoOrder rather than guessing from arbitrary evidence.
        let output = self
            .daemon
            .execution_runtime
            .evaluate(&ExecutionGateInput {
                permit: task.permit.clone(),
                decision_context,
                account_snapshot,
                quote_snapshot,
                quote_validation_error,
                market_clock_snapshot,
                pretrade_safety,
                now: gate_now,
            })?;
        self.daemon
            .execution_runtime
            .commit(&task.permit, &output, gate_now)?;
        Ok(TaskCompletion::Committed)
    }

    // 从已持久化的 Decision、执行快照和研究证据构造 PreTradeSafetyEvidence；缺少
    // Paper approval 或任一必需快照时返回 None，让 ExecutionGate 保持 fail-closed。
    fn pretrade_safety_evidence(
        &self,
        task: &ClaimedAttempt,
        decision_context: &ArtifactRef,
        account_snapshot: Option<&ArtifactRef>,
        quote_snapshot: Option<&ArtifactRef>,
        market_clock_snapshot: Option<&ArtifactRef>,
        now: DateTime<Utc>,
    ) -> Result<Option<PreTradeSafetyEvidence>> {
        if self.daemon.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(None);
        }
        let (Some(account_snapshot), Some(quote_snapshot), Some(market_clock_snapshot)) =
            (account_snapshot, quote_snapshot, market_clock_snapshot)
        else {
            return Ok(None);
        };

        let decision: DecisionContext = self.daemon.read_artifact_payload(decision_context)?;
        decision
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let account: AccountSnapshot = self.daemon.read_artifact_payload(account_snapshot)?;
        let quotes: QuoteSnapshot = self.daemon.read_artifact_payload(quote_snapshot)?;
        let clock: MarketClockSnapshot =
            self.daemon.read_artifact_payload(market_clock_snapshot)?;
        account
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        quotes
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        clock
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let account_observation = self.persisted_observation(account_snapshot)?;
        let quote_observation = self.persisted_observation(quote_snapshot)?;
        let clock_observation = self.persisted_observation(market_clock_snapshot)?;

        let mut average_daily_dollar_volume = BTreeMap::new();
        let mut news_dependency = EvidenceDependencyAggregate::default();
        let mut macro_dependency = EvidenceDependencyAggregate::default();
        let mut network_successes = Vec::new();
        network_successes.extend(
            [&account_observation, &quote_observation, &clock_observation]
                .into_iter()
                .filter(|observation| observation.network_host.is_some())
                .cloned(),
        );
        let mut information_classifications = quotes
            .quotes
            .keys()
            .copied()
            .map(|asset| (asset, InformationClassification::Public))
            .collect::<BTreeMap<_, _>>();
        let content_policy = FinancialContentPolicy::default();
        for reference in &decision.evidence {
            // 只汇总 Decision 已绑定的 NormalizedEvidence；研究证据的来源、质量和
            // 财经内容分类都保留到依赖闭包，RawEvidence 不在此处直接参与执行判定。
            if reference.kind != ArtifactKind::NormalizedEvidence {
                continue;
            }
            let payload: NormalizedEvidencePayload =
                self.daemon.read_artifact_payload(reference)?;
            let observation = self.persisted_observation(reference)?;
            if observation.network_host.is_some() {
                network_successes.push(observation.clone());
            }
            match payload.source {
                EvidenceSource::NewsWeb | EvidenceSource::SecEdgar => {
                    news_dependency.observe(&payload, &observation);
                }
                EvidenceSource::Fred => macro_dependency.observe(&payload, &observation),
                EvidenceSource::Alpaca => {}
            }
            if let Ok(GovernedResource::AlpacaBars { asset, .. }) =
                GovernedResource::parse(payload.source, &payload.resource)
            {
                // 量化流动性信息仅在已解析的 Alpaca bars 资源中提取；无法解析时保留
                // 其余证据，不把失败转换成任意流动性数值。
                if let Some(value) = payload
                    .quant_features
                    .as_ref()
                    .and_then(|features| features.average_dollar_volume_20d_micros)
                    .and_then(|value| i64::try_from(value).ok())
                {
                    average_daily_dollar_volume.insert(asset, MoneyMicros(value));
                }
                information_classifications
                    .entry(asset)
                    .or_insert(InformationClassification::Public);
            }
            if let Some(content) = payload.financial_content {
                // 会阻断交易的内容把相关目标标成 Unknown；其余实体分类按最严格值
                // 合并，避免一条较弱观察覆盖已存在的更高风险分类。
                let content_blocks_trading = content.blocks_trading(&content_policy);
                let classification = if content_blocks_trading {
                    InformationClassification::Unknown
                } else {
                    content.information_classification
                };
                if content_blocks_trading {
                    for asset in decision
                        .target
                        .weights
                        .iter()
                        .filter_map(|(asset, weight)| (weight.0 > 0).then_some(*asset))
                    {
                        information_classifications
                            .insert(asset, InformationClassification::Unknown);
                    }
                }
                for identifier in &content.canonical_entity_ids {
                    let Some(symbol) = identifier.strip_prefix("ticker:") else {
                        continue;
                    };
                    let Ok(asset) = Asset::try_from(symbol) else {
                        continue;
                    };
                    information_classifications
                        .entry(asset)
                        .and_modify(|current| *current = (*current).max(classification))
                        .or_insert(classification);
                }
            }
        }

        // Pre-trade safety is assessed against the approved runtime manifest.
        // Without approval no assessment can be made and none is asserted:
        // ExecutionGate already holds UnqualifiedRuntime and skips allocation,
        // so the verdict remains a durable NoOrder without execution authority.
        let Some((manifest, _)) = self.daemon.store.paper_approval_for_run(&task.run_id)? else {
            return Ok(None);
        };
        // Overnight 的执行行情必须与运行时批准的 feed 对齐：SIP 映射为 BOATS，
        // 其他配置使用 overnight；这只建立依赖快照，不放宽后续行情质量 Gate。
        let model_freshness_secs = decision
            .validity
            .as_ref()
            .map_or(1, |validity| validity.maximum_execution_delay_ms / 1_000)
            .max(1);
        let model_dependency =
            self.persisted_model_dependency(&task.run_id, &manifest, model_freshness_secs, now)?;
        let execution_feed = if clock.trading_session() == akzio_domain::TradingSession::Overnight {
            if manifest.market_data_feed.eq_ignore_ascii_case("sip") {
                "boats"
            } else {
                "overnight"
            }
        } else {
            &manifest.market_data_feed
        };
        let market_data_dependency =
            market_data_dependency(execution_feed, &quote_observation, now);
        let broker_dependency = alpaca_service_dependency(
            DependencyKind::Broker,
            "paper-account",
            &account_observation,
            now,
        );
        let market_clock_dependency = alpaca_service_dependency(
            DependencyKind::MarketClock,
            "market-session",
            &clock_observation,
            now,
        );
        let dns_dependency = dns_dependency(
            &network_successes,
            manifest.provider_id.contains("fixture") || manifest.model_id.contains("fixture"),
            now,
        );
        let dependency_closure = DependencyClosure {
            captured_at: now,
            dependencies: BTreeMap::from([
                model_dependency,
                news_dependency.snapshot(DependencyKind::NewsProvider, decision.created_at, now),
                market_data_dependency,
                macro_dependency.snapshot(DependencyKind::MacroData, decision.created_at, now),
                broker_dependency,
                dns_dependency,
                market_clock_dependency,
                internal_healthy_dependency(
                    DependencyKind::Storage,
                    "v2-store",
                    &DOMAIN_SCHEMA_VERSION.to_string(),
                    "durable-state",
                    now,
                ),
                internal_healthy_dependency(
                    DependencyKind::Identity,
                    "approved-runtime-manifest",
                    manifest.config_hash.as_str(),
                    "runtime-identity",
                    now,
                ),
                internal_healthy_dependency(
                    DependencyKind::ComplianceData,
                    "embedded-compliance-policy",
                    manifest.execution_policy_hash.as_str(),
                    "public-policy",
                    now,
                ),
            ]),
        };
        dependency_closure
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;

        // 返回的只是给 ExecutionGate 使用的依赖/安全观察集合；它本身不产生
        // ExecutionPlan、Commitment 或 Broker 写入。
        Ok(Some(PreTradeSafetyEvidence {
            homogeneous_agent_count: 1,
            average_daily_dollar_volume,
            information_classifications,
            recent_compliance_activity: self.recent_compliance_activity(now)?,
            dependency_closure,
        }))
    }

    // 校验引用对应的 Artifact kind 并提取 provenance；对组合账户快照额外检查四个
    // NormalizedEvidence 是否来自同一服务和 source family，并以最早检索时间约束新鲜度。
    fn persisted_observation(&self, reference: &ArtifactRef) -> Result<PersistedObservation> {
        let artifact = self.daemon.store.artifact(&reference.artifact_id)?;
        if artifact.kind != reference.kind {
            return Err(DaemonError::InvalidInput(format!(
                "artifact {} expected {:?}, found {:?}",
                reference.artifact_id, reference.kind, artifact.kind
            )));
        }
        let observation = PersistedObservation::from_artifact(&artifact);
        if artifact.producer != "execution.snapshot.account" || observation.source_uri.is_some() {
            return Ok(observation);
        }
        // A composed account snapshot has no single URL. Its four immutable
        // inputs must all identify the same service; a missing or mixed origin
        // remains Partial. Use the oldest input to preserve the freshness bound.
        let sources = artifact
            .source_refs
            .iter()
            .filter(|source| source.kind == ArtifactKind::NormalizedEvidence)
            .collect::<Vec<_>>();
        if sources.len() != 4 {
            // 组合快照来源不完整时保留原始 Partial 观察，不能把缺失来源当成同源成功。
            return Ok(observation);
        }
        let mut combined: Option<(String, PersistedObservation)> = None;
        for source in sources {
            let source = self.daemon.store.artifact(&source.artifact_id)?;
            let current = PersistedObservation::from_artifact(&source);
            let Some((scheme, remainder)) = current
                .source_uri
                .as_deref()
                .and_then(|uri| uri.split_once("://"))
            else {
                // 任一来源没有可解析的网络 URI，就无法证明组合快照的统一外部来源。
                return Ok(observation);
            };
            let host = remainder.split(['/', '?', '#']).next().unwrap_or_default();
            if host.is_empty() {
                return Ok(observation);
            }
            let origin = format!("{scheme}://{}", host.to_ascii_lowercase());
            if let Some((prior_origin, prior)) = &mut combined {
                // 不同 host/origin 或 source family 的输入不得合并成一个健康观察；
                // 同源时取最早 retrieved_at，保守保持整个组合的有效窗口。
                if *prior_origin != origin || prior.source_family != current.source_family {
                    return Ok(observation);
                }
                prior.retrieved_at = prior.retrieved_at.min(current.retrieved_at);
            } else {
                combined = Some((origin, current));
            }
        }
        Ok(combined
            .map(|(_, observation)| observation)
            .unwrap_or(observation))
    }

    // 在当前 Run 的 AgentTurn 中寻找最新 Synthesizer capability snapshot，比较配置的
    // RuntimeManifest 与实际模型身份；找不到 durable snapshot 时明确标为 Unavailable。
    fn persisted_model_dependency(
        &self,
        run_id: &RunId,
        manifest: &RuntimeManifest,
        expected_freshness_secs: u64,
        now: DateTime<Utc>,
    ) -> Result<(DependencyKind, DependencySnapshot)> {
        let mut latest = None;
        for artifact in self
            .daemon
            .store
            .recent_artifacts_by_kind(ArtifactKind::AgentTurn, 500)?
        {
            // 仅接受本 Run、research.synthesizer 请求的 AgentTurn，避免把其他 Run 或
            // 其他角色的模型调用伪装成当前 Decision 的模型依赖。
            if artifact
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run_id)
            {
                continue;
            }
            let payload: serde_json::Value =
                serde_json::from_slice(&self.daemon.store.read_blob(&artifact.blob)?)?;
            if payload
                .pointer("/request/purpose")
                .and_then(serde_json::Value::as_str)
                != Some("research.synthesizer")
            {
                continue;
            }
            let capability: akzio_model::ModelCapabilitySnapshot = serde_json::from_value(
                payload.get("capability_snapshot").cloned().ok_or_else(|| {
                    DaemonError::InvalidInput(
                        "persisted synthesizer AgentTurn has no capability snapshot".to_owned(),
                    )
                })?,
            )?;
            // 多次调用只取 created_at 最新的一次；读取 capability 失败直接传播，不能
            // 用“配置看起来正确”掩盖持久化模型身份缺失。
            if latest
                .as_ref()
                .is_none_or(|(prior, _): &(Artifact, _)| prior.created_at < artifact.created_at)
            {
                latest = Some((artifact, capability));
            }
        }

        let fixture_identity =
            manifest.provider_id.contains("fixture") && manifest.model_id.contains("fixture");
        let configured_service = if fixture_identity {
            "fixture:fixture".to_owned()
        } else {
            format!("{}:{}", manifest.provider_id, manifest.model_id)
        };
        let (provider, service, version, observed_at, status, actual_service) =
            if let Some((artifact, capability)) = latest {
                let actual_service = format!("{}:{}", capability.provider_id, capability.model_id);
                (
                    capability.provider_id,
                    capability.model_id.clone(),
                    capability.model_id,
                    artifact.provenance.retrieved_at,
                    DependencyHealthStatus::Healthy,
                    actual_service,
                )
            } else {
                // 没有当前 Run 的真实 AgentTurn 时，依赖仍记录配置身份，但健康状态
                // 为 Unavailable，供后续 Gate 继续 fail-closed。
                (
                    manifest.provider_id.clone(),
                    manifest.model_id.clone(),
                    manifest.model_id.clone(),
                    now,
                    DependencyHealthStatus::Unavailable,
                    configured_service.clone(),
                )
            };
        Ok(dependency_snapshot(
            DependencyDescriptor {
                kind: DependencyKind::ModelProvider,
                provider,
                service,
                version,
                data_classification: "model-input-output".to_owned(),
                fallback_policy: FallbackPolicy::RiskReductionOnly,
            },
            observed_at,
            expected_freshness_secs,
            status,
            configured_service,
            actual_service,
            BTreeSet::new(),
        ))
    }

    // 从最近持久化的回执和 ExecutionPlan 推导合规基线：每个 client_order_id 只保留
    // 最新状态，再计算提交/撤改单和同一资产买卖并存等指标；这是 Gate 输入快照，不是成交真值。
    fn recent_compliance_activity(&self, now: DateTime<Utc>) -> Result<ComplianceActivitySnapshot> {
        let cutoff = now - chrono::Duration::days(1);
        let mut latest_receipts = BTreeMap::<String, OrderReceipt>::new();
        for artifact in self
            .daemon
            .store
            .recent_artifacts_by_kind(ArtifactKind::OrderReceipt, 500)?
        {
            // 时间窗先限制在过去 24 小时到 now，随后按 client_order_id 去重，避免重放
            // 回执重复放大合规计数。
            let receipt: OrderReceipt = self.daemon.read_artifact_payload(&ArtifactRef {
                artifact_id: artifact.artifact_id,
                kind: ArtifactKind::OrderReceipt,
            })?;
            if receipt.observed_at < cutoff || receipt.observed_at > now {
                continue;
            }
            receipt
                .validate()
                .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
            let replace = latest_receipts
                .get(&receipt.client_order_id)
                .is_none_or(|prior| prior.observed_at < receipt.observed_at);
            if replace {
                latest_receipts.insert(receipt.client_order_id.clone(), receipt);
            }
        }

        let recent_submissions = u32::try_from(latest_receipts.len()).unwrap_or(u32::MAX);
        let recent_cancellations = u32::try_from(
            latest_receipts
                .values()
                .filter(|receipt| {
                    matches!(
                        receipt.state,
                        OrderReceiptState::PendingCancel
                            | OrderReceiptState::PendingReplace
                            | OrderReceiptState::Canceled
                            | OrderReceiptState::Replaced
                    )
                })
                .count(),
        )
        .unwrap_or(u32::MAX);

        let mut active_sides = BTreeMap::<Asset, (bool, bool)>::new();
        let plans = self
            .daemon
            .store
            .recent_artifacts_by_kind(ArtifactKind::ExecutionPlan, 500)?
            .into_iter()
            .map(|artifact| {
                self.daemon
                    .read_artifact_payload::<ExecutionPlan>(&ArtifactRef {
                        artifact_id: artifact.artifact_id,
                        kind: ArtifactKind::ExecutionPlan,
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        for receipt in latest_receipts.values().filter(|receipt| {
            matches!(
                receipt.state,
                OrderReceiptState::Accepted
                    | OrderReceiptState::PartiallyFilled
                    | OrderReceiptState::PendingCancel
                    | OrderReceiptState::PendingReplace
                    | OrderReceiptState::DoneForDay
                    | OrderReceiptState::Stopped
                    | OrderReceiptState::Suspended
                    | OrderReceiptState::Calculated
            )
        }) {
            // 回执只说明当前订单状态；通过 plan_hash 反查方向后再统计同资产双向活跃，
            // 找不到对应计划时不猜测订单方向。
            if let Some(side) = plans
                .iter()
                .find(|plan| plan.plan_hash == receipt.plan_hash)
                .and_then(|plan| {
                    plan.orders
                        .iter()
                        .find(|order| order.asset == receipt.asset)
                        .map(|order| order.side)
                })
            {
                let sides = active_sides.entry(receipt.asset).or_default();
                match side {
                    OrderSide::Buy => sides.0 = true,
                    OrderSide::Sell => sides.1 = true,
                }
            }
        }

        Ok(ComplianceActivitySnapshot {
            recent_submissions,
            recent_cancellations,
            self_trade_detected: active_sides.values().any(|(buy, sell)| *buy && *sell),
            spoofing_or_layering_pattern: recent_submissions >= 6
                && recent_cancellations.saturating_mul(2) >= recent_submissions,
            marking_the_close_risk: false,
            prearranged_trade_risk: false,
            front_running_risk: false,
            available_controls: ComplianceControl::PAPER_BASELINE.into_iter().collect(),
        })
    }

    // 仅接受 ExecutionGate 的 Accepted verdict，读取有效 broker session 后通过
    // PaperCommitment runtime 写入确定性 Commitment；NoOrder 不产生 Commitment。
    pub(crate) fn commit(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        let verdict = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.daemon.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let ExecutionVerdict::Accepted { execution_context } = verdict_payload else {
            return Ok(TaskCompletion::NoOutput);
        };
        let context: ExecutionContext = self.daemon.read_artifact_payload(&execution_context)?;
        context
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        let session_key = context.broker_session.ok_or_else(|| {
            DaemonError::InvalidInput("accepted execution verdict has no broker session".to_owned())
        })?;
        let lease = self.daemon.paper.scheduler.active_lease(now)?;
        self.daemon
            .paper_commitment_runtime
            .commit(&PaperCommitmentInput {
                lease,
                permit: task.permit.clone(),
                verdict,
                session_key,
                now,
            })?;
        // 这里的 Committed 是执行承诺已持久化并通过 lease 边界，不表示 Alpaca 已受理
        // 或已成交；实际外部 Broker I/O 只在后续 Reconcile 阶段发生。
        Ok(TaskCompletion::Committed)
    }

    // 对 Accepted Commitment 执行 Paper Broker 对账；NoOrder 直接结束该节点，调试
    // Broker 阻断和未结算回执都保留 Deferred 边界，不把受理状态写成最终成交。
    pub(crate) async fn reconcile(
        &self,
        task: &ClaimedAttempt,
        now: DateTime<Utc>,
    ) -> Result<TaskCompletion> {
        if self.daemon.store.run_purpose(&task.run_id)? != RunPurpose::Paper {
            return Ok(TaskCompletion::NoOutput);
        }
        let verdict = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionVerdict)?;
        let verdict_payload: ExecutionVerdict = self.daemon.read_artifact_payload(&verdict)?;
        verdict_payload
            .validate()
            .map_err(|error| DaemonError::InvalidInput(error.to_string()))?;
        if matches!(verdict_payload, ExecutionVerdict::NoOrder { .. }) {
            return Ok(TaskCompletion::NoOutput);
        }
        let commitment = self
            .daemon
            .terminal_input(task, ArtifactKind::ExecutionCommitment)?;
        if self.daemon.store.block_debug_broker_task(
            &task.permit,
            &[verdict.clone(), commitment.clone()],
            now,
        )? {
            // Debug 默认禁止 Broker 写入；保持任务可恢复等待，而不是绕过控制策略。
            return Ok(TaskCompletion::DeferredUntil(now + Duration::seconds(1)));
        }
        let broker = self.daemon.paper.paper_broker.as_ref().ok_or_else(|| {
            DaemonError::Unavailable(
                "Paper reconciliation requires an injected Alpaca Paper broker adapter".to_owned(),
            )
        })?;
        let lease = self.daemon.paper.scheduler.active_lease(now)?;
        let output = self
            .daemon
            .paper_dispatch_runtime
            .dispatch(
                broker.as_ref(),
                &PaperDispatchInput {
                    lease,
                    permit: task.permit.clone(),
                    commitment,
                    now,
                },
            )
            .await?;
        if output.settled {
            Ok(TaskCompletion::Committed)
        } else {
            // 短轮询未得到终态时保留已提交 Commitment，下一次 Reconcile 继续读取/对账。
            Ok(TaskCompletion::DeferredUntil(
                Utc::now() + chrono::Duration::seconds(1),
            ))
        }
    }
}

// 只有有效且未过期的 Decision 在可接受年龄范围内遇到 Closed session 才延期；唤醒时间
// 取下次开市与 Decision validity 的较早者，并至少向未来推进一秒，避免立即忙循环。
fn closed_session_wake(
    clock: &MarketClockSnapshot,
    validity: Option<&akzio_domain::DecisionValidity>,
    policy: &akzio_execution::ExecutionPolicy,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let session = clock.session.as_ref()?;
    let validity = validity.filter(|v| v.is_valid_at(now))?;
    let age = now.signed_duration_since(clock.observed_at).num_seconds();
    if session.kind != akzio_domain::TradingSession::Closed
        || age < -policy.max_future_skew_secs
        || age > policy.max_clock_age_secs
    {
        return None;
    }
    Some(
        session
            .next_open
            .unwrap_or(now + Duration::minutes(5))
            .min(validity.valid_until)
            .max(now + Duration::seconds(1)),
    )
}

#[cfg(test)]
mod session_wait_tests {
    use super::*;

    #[test]
    fn closed_wait_expires_with_decision_and_rechecks_new_session() {
        let now = "2026-09-05T16:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let mut clock = MarketClockSnapshot {
            schema_version: akzio_domain::DOMAIN_SCHEMA_VERSION,
            broker_session: "2026-09-05".into(),
            observed_at: now,
            is_open: false,
            session: Some(akzio_domain::TradingSessionSnapshot {
                kind: akzio_domain::TradingSession::Closed,
                trade_date: now.date_naive(),
                next_open: Some(now + Duration::days(2)),
                ends_at: None,
                overnight_assets: BTreeSet::new(),
            }),
        };
        let validity = akzio_domain::DecisionValidity {
            evidence_cutoff: now,
            generated_at: now,
            valid_until: now + Duration::minutes(5),
            maximum_execution_delay_ms: 300_000,
            market_state_hash: ContentHash::of_bytes(b"fixture"),
        };
        let policy = akzio_execution::ExecutionPolicy::default();
        assert_eq!(
            closed_session_wake(&clock, Some(&validity), &policy, now),
            Some(validity.valid_until)
        );
        assert_eq!(
            closed_session_wake(
                &clock,
                Some(&validity),
                &policy,
                validity.valid_until + Duration::seconds(1)
            ),
            None
        );
        clock.observed_at = now - Duration::seconds(policy.max_clock_age_secs + 1);
        assert_eq!(
            closed_session_wake(&clock, Some(&validity), &policy, now),
            None
        );
        clock.observed_at = now;
        clock.session.as_mut().unwrap().next_open = Some(now + Duration::minutes(1));
        assert_eq!(
            closed_session_wake(&clock, Some(&validity), &policy, now),
            Some(now + Duration::minutes(1))
        );
        clock.session.as_mut().unwrap().kind = akzio_domain::TradingSession::Overnight;
        assert_eq!(
            closed_session_wake(&clock, Some(&validity), &policy, now),
            None
        );
    }
}

#[derive(Clone)]
struct PersistedObservation {
    retrieved_at: DateTime<Utc>,
    source_family: String,
    source_uri: Option<String>,
    network_host: Option<String>,
}

impl PersistedObservation {
    // 从 Artifact provenance 提取检索时间、来源族、URI 和网络 host，供依赖闭包使用；
    // provenance 缺失时保留 None，后续状态判断负责区分 Partial/Unavailable。
    fn from_artifact(artifact: &Artifact) -> Self {
        let source_uri = artifact.provenance.source_uri.clone();
        Self {
            retrieved_at: artifact.provenance.retrieved_at,
            source_family: artifact.provenance.source_family.clone(),
            network_host: source_uri.as_deref().and_then(network_host),
            source_uri,
        }
    }
}

#[derive(Default)]
struct EvidenceDependencyAggregate {
    providers: BTreeSet<String>,
    services: BTreeSet<String>,
    versions: BTreeSet<String>,
    fourth_parties: BTreeSet<String>,
    latest_retrieval: Option<DateTime<Utc>>,
    partial: bool,
}

impl EvidenceDependencyAggregate {
    // 吸收一条标准化证据的来源、版本、网络依赖和质量缺口，后续 snapshot 再统一归并。
    fn observe(&mut self, payload: &NormalizedEvidencePayload, observation: &PersistedObservation) {
        self.providers.insert(observation.source_family.clone());
        self.services.insert(payload.source.as_str().to_owned());
        self.versions.insert(
            payload
                .provenance
                .revision
                .clone()
                .unwrap_or_else(|| "unversioned".to_owned()),
        );
        self.fourth_parties
            .extend(observation.network_host.iter().cloned());
        self.latest_retrieval = Some(
            self.latest_retrieval
                .map_or(payload.time_basis.retrieved_at, |prior| {
                    prior.max(payload.time_basis.retrieved_at)
                }),
        );
        self.partial |= payload.quality.completeness_ppm < WeightPpm::SCALE
            || !payload.quality.citations_complete
            || !payload.quality.normalized;
    }

    fn snapshot(
        &self,
        kind: DependencyKind,
        decision_created_at: DateTime<Utc>,
        captured_at: DateTime<Utc>,
    ) -> (DependencyKind, DependencySnapshot) {
        // 没有观察到该类证据时显式返回 NotRequired；有观察但质量不完整时只降为
        // Partial，不把部分 citations/coverage 提升为 Healthy。
        let Some(observed_at) = self.latest_retrieval else {
            return dependency_snapshot(
                DependencyDescriptor {
                    kind,
                    provider: "akzio".to_owned(),
                    service: "not-required".to_owned(),
                    version: "not-observed".to_owned(),
                    data_classification: "not-read".to_owned(),
                    fallback_policy: FallbackPolicy::FailClosed,
                },
                decision_created_at.min(captured_at),
                300,
                DependencyHealthStatus::NotRequired,
                "not-required".to_owned(),
                "not-required".to_owned(),
                BTreeSet::new(),
            );
        };
        let provider = joined_identity(&self.providers);
        let service = joined_identity(&self.services);
        dependency_snapshot(
            DependencyDescriptor {
                kind,
                provider,
                service: service.clone(),
                version: joined_identity(&self.versions),
                data_classification: "decision-evidence".to_owned(),
                fallback_policy: FallbackPolicy::FailClosed,
            },
            observed_at,
            300,
            if self.partial {
                DependencyHealthStatus::Partial
            } else {
                DependencyHealthStatus::Healthy
            },
            service.clone(),
            service,
            self.fourth_parties.clone(),
        )
    }
}

struct DependencyDescriptor {
    kind: DependencyKind,
    provider: String,
    service: String,
    version: String,
    data_classification: String,
    fallback_policy: FallbackPolicy,
}

// 将内部描述字段和运行时状态组装为带配置/实际服务身份的持久化依赖快照。
fn dependency_snapshot(
    descriptor: DependencyDescriptor,
    observed_at: DateTime<Utc>,
    expected_freshness_secs: u64,
    status: DependencyHealthStatus,
    configured_service: String,
    actual_service: String,
    fourth_party_dependencies: BTreeSet<String>,
) -> (DependencyKind, DependencySnapshot) {
    let DependencyDescriptor {
        kind,
        provider,
        service,
        version,
        data_classification,
        fallback_policy,
    } = descriptor;
    (
        kind,
        DependencySnapshot {
            kind,
            provider,
            service,
            version,
            data_classification,
            expected_freshness_secs,
            observed_at,
            status,
            fallback_policy,
            configured_service,
            actual_service,
            fourth_party_dependencies,
        },
    )
}

// 根据批准的执行 feed 和 provenance 判断行情依赖状态；未经观察的真实 feed 为
// Partial，fixture 只允许显式的 fixture 例外，夜间非 SIP/BOATS 继续 Degraded。
fn market_data_dependency(
    configured_feed: &str,
    observation: &PersistedObservation,
    _captured_at: DateTime<Utc>,
) -> (DependencyKind, DependencySnapshot) {
    let observed_feed = observation
        .source_uri
        .as_deref()
        .and_then(|uri| query_parameter(uri, "feed"))
        .map(str::to_ascii_lowercase);
    let fixture = observation
        .source_uri
        .as_deref()
        .is_some_and(|uri| uri.starts_with("fixture://"));
    // Indicative overnight data retains the existing degraded-data restriction.
    let status = if !matches!(
        configured_feed.to_ascii_lowercase().as_str(),
        "sip" | "boats"
    ) {
        DependencyHealthStatus::Degraded
    } else if observed_feed.is_none() && !fixture {
        DependencyHealthStatus::Partial
    } else {
        DependencyHealthStatus::Healthy
    };
    let actual_feed = observed_feed.unwrap_or_else(|| {
        if fixture {
            configured_feed.to_ascii_lowercase()
        } else {
            "unobserved-feed".to_owned()
        }
    });
    dependency_snapshot(
        DependencyDescriptor {
            kind: DependencyKind::MarketData,
            provider: observation.source_family.clone(),
            service: actual_feed.clone(),
            version: actual_feed.clone(),
            data_classification: "market-data".to_owned(),
            fallback_policy: FallbackPolicy::FailClosed,
        },
        observation.retrieved_at,
        30,
        status,
        configured_feed.to_ascii_lowercase(),
        actual_feed,
        observation.network_host.iter().cloned().collect(),
    )
}

// 记录由当前 Rust/Store/Manifest 直接提供、无需外部网络观察的健康内部依赖。
fn internal_healthy_dependency(
    kind: DependencyKind,
    service: &str,
    version: &str,
    data_classification: &str,
    now: DateTime<Utc>,
) -> (DependencyKind, DependencySnapshot) {
    dependency_snapshot(
        DependencyDescriptor {
            kind,
            provider: "akzio".to_owned(),
            service: service.to_owned(),
            version: version.to_owned(),
            data_classification: data_classification.to_owned(),
            fallback_policy: FallbackPolicy::FailClosed,
        },
        now,
        30,
        DependencyHealthStatus::Healthy,
        service.to_owned(),
        service.to_owned(),
        BTreeSet::new(),
    )
}

// 将 Alpaca 账户或时钟观察映射为配置服务与实际 host；缺少 source URI 时保留 Partial，
// 不凭服务名推断已经建立了外部连接。
fn alpaca_service_dependency(
    kind: DependencyKind,
    data_classification: &str,
    observation: &PersistedObservation,
    _captured_at: DateTime<Utc>,
) -> (DependencyKind, DependencySnapshot) {
    let fixture = observation
        .source_uri
        .as_deref()
        .is_some_and(|uri| uri.starts_with("fixture://"));
    let configured_service = if fixture {
        "fixture-alpaca".to_owned()
    } else {
        "paper-api.alpaca.markets".to_owned()
    };
    let actual_service = observation
        .network_host
        .clone()
        .unwrap_or_else(|| configured_service.clone());
    dependency_snapshot(
        DependencyDescriptor {
            kind,
            provider: observation.source_family.clone(),
            service: actual_service.clone(),
            version: "v2".to_owned(),
            data_classification: data_classification.to_owned(),
            fallback_policy: FallbackPolicy::FailClosed,
        },
        observation.retrieved_at,
        30,
        if observation.source_uri.is_some() {
            DependencyHealthStatus::Healthy
        } else {
            DependencyHealthStatus::Partial
        },
        configured_service,
        actual_service,
        observation.network_host.iter().cloned().collect(),
    )
}

// 从成功的网络观察中派生 DNS 依赖；fixture 没有网络 host 时是 NotApplicable，真实
// 运行没有任何 host 则是 Unavailable。
fn dns_dependency(
    successful_network_observations: &[PersistedObservation],
    fixture_runtime: bool,
    captured_at: DateTime<Utc>,
) -> (DependencyKind, DependencySnapshot) {
    let hosts = successful_network_observations
        .iter()
        .filter_map(|observation| observation.network_host.clone())
        .collect::<BTreeSet<_>>();
    let observed_at = successful_network_observations
        .iter()
        .filter(|observation| observation.network_host.is_some())
        .map(|observation| observation.retrieved_at)
        .max()
        .unwrap_or(captured_at);
    let status = if !hosts.is_empty() {
        DependencyHealthStatus::Healthy
    } else if fixture_runtime {
        DependencyHealthStatus::NotApplicable
    } else {
        DependencyHealthStatus::Unavailable
    };
    dependency_snapshot(
        DependencyDescriptor {
            kind: DependencyKind::Dns,
            provider: "derived".to_owned(),
            service: "successful-network-io".to_owned(),
            version: "v1".to_owned(),
            data_classification: "network-health".to_owned(),
            fallback_policy: FallbackPolicy::FailClosed,
        },
        observed_at,
        30,
        status,
        "successful-network-io".to_owned(),
        "successful-network-io".to_owned(),
        hosts,
    )
}

// 仅解析 http/https URI 的 host，并统一为小写；fixture 或其他 scheme 不被当作网络成功。
fn network_host(uri: &str) -> Option<String> {
    let remainder = uri
        .strip_prefix("https://")
        .or_else(|| uri.strip_prefix("http://"))?;
    let host = remainder.split(['/', '?', '#']).next()?.trim();
    (!host.is_empty()).then(|| host.to_ascii_lowercase())
}

// 从 URI query 中查找指定的非空参数；不解码或改写值，调用方按自身协议解释结果。
fn query_parameter<'a>(uri: &'a str, key: &str) -> Option<&'a str> {
    uri.split_once('?')?
        .1
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(candidate, value)| (candidate == key && !value.is_empty()).then_some(value))
}

// 以 BTreeSet 的稳定顺序合并多个 provider/service/version 标识，形成可复现的审计值。
fn joined_identity(values: &BTreeSet<String>) -> String {
    values.iter().cloned().collect::<Vec<_>>().join("+")
}
