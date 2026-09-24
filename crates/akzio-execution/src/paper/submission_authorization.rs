use super::*;
use akzio_domain::{AccountSnapshot, DecisionValidity, MarketClockSnapshot, QuoteSnapshot};

// 文件导读：授权对象是由不可变 plan、DecisionValidity、approval、账户/报价/时钟观察时间
// 和 TradingSession 交集计算出的临时许可。它不进入旧 plan hash，也不赋予 recovery 读
// 权之外的能力；window 为空时只能查询已有 Broker effect，不能产生新的 POST/PATCH。

/// Read projection of the existing governed normalized payload, not a second
/// persisted evidence format. Remaining provider content is irrelevant here.
#[derive(Debug, Deserialize)]
pub(super) struct FrozenAccountComponent {
    pub source: String,
    pub resource: String,
    pub observed_at: DateTime<Utc>,
    pub need: ArtifactRef,
    pub raw: ArtifactRef,
}

pub(super) fn frozen_account_observations(
    snapshot: &Artifact,
    account: &AccountSnapshot,
    components: &[(Artifact, FrozenAccountComponent)],
) -> Option<Vec<DateTime<Utc>>> {
    // 逐个核对 aggregate account snapshot 的四类 Alpaca normalized source，要求资源集合、
    // lineage、producer、观察时间和 permit origin 全部吻合；缺一项就返回 None，避免用
    // 聚合时间掩盖某个过期 positions/fills 组件。
    if snapshot.producer != "execution.snapshot.account" {
        return None;
    }
    if snapshot.kind != ArtifactKind::NormalizedEvidence
        || snapshot.lifecycle != ArtifactLifecycle::Canonical
        || snapshot.provenance.source_family != "alpaca"
        || snapshot.origin.is_none()
    {
        return None;
    }
    // 先从快照 source_refs 借出 normalized 引用并要求与解码后的 components 数量相等；
    // 数量不符时不能把 aggregate account.observed_at 当作全部来源的新鲜度证明。
    let references = snapshot
        .source_refs
        .iter()
        .filter(|reference| reference.kind == ArtifactKind::NormalizedEvidence)
        .collect::<Vec<_>>();
    if references.len() != components.len() {
        return None;
    }
    // The governed single-account snapshot is still produced by
    // materialize_paper_single_snapshot. A composed snapshot has four sources
    // and no single URI; neither path may silently fall back to aggregate time.
    // 单来源 legacy account 和四来源 composed account 使用不同的精确 resource 集合；
    // 其它 component 数量没有对应协议，返回 None。
    let expected: std::collections::BTreeSet<String> = match components.len() {
        1 if snapshot.provenance.source_uri.is_some() => ["paper.account".into()].into(),
        4 => [
            "paper.account".into(),
            "paper.positions".into(),
            "paper.open_orders".into(),
            format!("paper.fills:{}", account.broker_session),
        ]
        .into(),
        _ => return None,
    };
    // observed 先包含聚合账户观察时间，再为每个组件追加自身时间；resources 用于去重并
    // 最后与协议要求精确比较，防止漏项、重复项或混入其它资源。
    let mut resources = std::collections::BTreeSet::new();
    let mut observed = vec![account.observed_at];
    // components 是借用切片；任一 Artifact kind/lifecycle/producer/origin/provenance/ref
    // 不匹配就 None 提前返回，不产生部分可用时间列表。
    for (artifact, payload) in components {
        if artifact.kind != ArtifactKind::NormalizedEvidence
            || artifact.lifecycle != ArtifactLifecycle::RunScoped
            || artifact.producer != "akzio.ingest.alpaca.normalized"
            || artifact.origin != snapshot.origin
            || artifact.provenance.source_family != "alpaca"
            || artifact.provenance.observed_at != Some(payload.observed_at)
            || payload.source != "alpaca"
            || payload.need.kind != ArtifactKind::EvidenceNeed
            || payload.raw.kind != ArtifactKind::RawEvidence
            || !artifact.source_refs.contains(&payload.need)
            || !artifact.source_refs.contains(&payload.raw)
            || !snapshot.source_refs.contains(&payload.raw)
            || !references
                .iter()
                .any(|reference| reference.artifact_id == artifact.artifact_id)
            || !resources.insert(payload.resource.clone())
        {
            return None;
        }
        observed.push(payload.observed_at);
    }
    (resources == expected).then_some(observed)
}

/// Ephemeral permission to send an order, derived from the immutable plan's
/// sources. It is deliberately not serialized into or added to old plan hashes.
/// 无发送窗口时不能 POST/PATCH；已持久化订单的读取与风险降低取消另行处理。
#[derive(Debug, Clone)]
pub struct PaperSubmissionAuthorization {
    plan_hash: ContentHash,
    window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    session: Option<akzio_domain::TradingSessionSnapshot>,
}

impl PaperSubmissionAuthorization {
    pub(super) fn matches_session(
        &self,
        session: &akzio_domain::TradingSessionSnapshot,
        legacy_session: &str,
    ) -> bool {
        // 新 snapshot 使用冻结的 TradingSession kind/date；只有旧数据没有 session 时才按
        // Regular + legacy session 字符串兼容，不能用旧日期覆盖已识别的夜盘类型。
        // map_or_else 的两个闭包分别处理新 session snapshot 和旧缺省；只有 None 才允许
        // Regular + legacy 日期兼容，Some 则要求 kind/date 都完全相等。
        self.session.as_ref().map_or_else(
            || {
                session.kind == akzio_domain::TradingSession::Regular
                    && session.trade_date.to_string() == legacy_session
            },
            |frozen| frozen.kind == session.kind && frozen.trade_date == session.trade_date,
        )
    }
    pub(super) fn restrict_account_observations(
        &mut self,
        observations: Option<&[DateTime<Utc>]>,
        policy: &crate::ExecutionPolicy,
    ) {
        // 将各账户组件的 [最早可用, 最晚过期] 窗口逐个求交；任一时间算术失败或交集为空
        // 就取消发送窗口，而不是放宽到 aggregate observed_at。
        // observations 缺失时立即清空窗口并结束；存在时共享借用各时间点，对 self.window
        // 做唯一可变借用并逐项求交，不修改调用方 slice 或 policy。
        let Some(observations) = observations else {
            self.window = None;
            return;
        };
        let Some((mut start, mut end)) = self.window else {
            return;
        };
        // 每个组件生成 [observed_at - future_skew, observed_at + max_age]，zip 要求两端
        // 时间运算都成功；任一 overflow 会把 window 设 None 并提前返回。
        for observed_at in observations {
            let bounds = chrono::Duration::try_seconds(policy.max_account_age_secs)
                .and_then(|age| observed_at.checked_add_signed(age))
                .zip(
                    chrono::Duration::try_seconds(policy.max_future_skew_secs)
                        .and_then(|skew| observed_at.checked_sub_signed(skew)),
                );
            let Some((expiry, earliest)) = bounds else {
                self.window = None;
                return;
            };
            start = start.max(earliest);
            end = end.min(expiry);
        }
        self.window = (start <= end).then_some((start, end));
    }

    pub(super) fn from_frozen_sources(
        plan: &ExecutionPlan,
        policy: &crate::ExecutionPolicy,
        validity: Option<&DecisionValidity>,
        approval_expires_at: Option<DateTime<Utc>>,
        account: &AccountSnapshot,
        quotes: &QuoteSnapshot,
        clock: &MarketClockSnapshot,
    ) -> Result<Self> {
        // 先保留 plan hash/session，再依次检查 validity、approval、四份快照、policy hash、
        // session/tradable 和每个订单报价，最后取所有时效及交易 session 结束时间的交集。
        // 缺少 approval 时返回无 window 的只读授权，而不是伪造 Paper 发送资格。
        // 此处只组装临时授权，不写入 plan/Store。plan、policy、快照均共享借用，生成的
        // authorization 拥有 hash 和时间窗口；缺任一必要授权事实时返回 Ok(window=None)。
        let mut authorization = Self {
            plan_hash: plan.plan_hash.clone(),
            window: None,
            session: clock.session.clone(),
        };
        // Option 的 None 代表缺 Decision validity/approval：这是无发送资格而非解析错误，
        // 故保留可读回执的对象并立即返回。
        let Some(validity) = validity else {
            return Ok(authorization);
        };
        let Some(approval_expires_at) = approval_expires_at else {
            return Ok(authorization);
        };
        validity.validate()?;
        account.validate()?;
        quotes.validate()?;
        clock.validate()?;
        let policy_hash = policy
            .policy_hash()
            .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
        // 即使前述 payload 都结构有效，只要策略 hash、session 或 tradable 不匹配也保留
        // 无窗口状态；不会把旧快照“修正”为当前可发送。
        if policy_hash != plan.policy_hash
            || account.broker_session != plan.broker_session
            || quotes.broker_session != plan.broker_session
            || clock.broker_session != plan.broker_session
            || !clock.tradable()
        {
            return Ok(authorization);
        }
        // 发送窗口从 Decision 生成时刻开始，截止时间取 Decision、approval 与当前 session
        // 结束时间的最小值；下面再与账户/报价/时钟各自 freshness 窗口求交。
        let mut starts_at = validity.generated_at;
        let mut expires_at = validity.valid_until.min(approval_expires_at);
        if let Some(end) = clock.session.as_ref().and_then(|s| s.ends_at) {
            expires_at = expires_at.min(end);
        }
        let mut observations = vec![
            (account.observed_at, policy.max_account_age_secs),
            (quotes.observed_at, policy.max_quote_age_secs),
            (clock.observed_at, policy.max_clock_age_secs),
        ];
        for order in &plan.orders {
            let Some(quote) = quotes.quotes.get(&order.asset) else {
                return Ok(authorization);
            };
            observations.push((quote.observed_at, policy.max_quote_age_secs));
        }
        // 所有观察必须同时仍在可接受时间范围内；时间加减溢出时转成 Ok(None-window)，
        // 不扩宽窗口，也不伪造时间上限。
        for (observed_at, max_age) in observations {
            let Some(expiry) = chrono::Duration::try_seconds(max_age)
                .and_then(|age| observed_at.checked_add_signed(age))
            else {
                return Ok(authorization);
            };
            let Some(start) = chrono::Duration::try_seconds(policy.max_future_skew_secs)
                .and_then(|skew| observed_at.checked_sub_signed(skew))
            else {
                return Ok(authorization);
            };
            starts_at = starts_at.max(start);
            expires_at = expires_at.min(expiry);
        }
        // 只有交集非空（含单个边界时刻）时才安装发送窗口；空交集仍允许 lookup/reconcile。
        if starts_at <= expires_at {
            authorization.window = Some((starts_at, expires_at));
        }
        Ok(authorization)
    }

    pub(super) fn assert_current(&self, plan_hash: &ContentHash, now: DateTime<Utc>) -> Result<()> {
        // 新 Broker effect 前的最后一道时间/身份检查；恢复读取不调用这个方法，因此过期
        // 后仍能查找并保存已发生的订单效果。
        // 计划身份必须相同且 now 落在含端点区间；None window 经 is_some_and 直接判失败。
        if self.plan_hash != *plan_hash
            || !self
                .window
                .is_some_and(|(start, end)| start <= now && now <= end)
        {
            return Err(PaperError::SubmissionUnauthorized);
        }
        Ok(())
    }

    pub(super) fn assert_replacement_current(&self, now: DateTime<Utc>) -> Result<()> {
        // 替换动作沿用原 plan hash 和同一发送窗口，不因 successor 产生新的宽限期；
        // 这里再次借用 self，不创建新的授权窗口。
        self.assert_current(&self.plan_hash, now)
    }
}
