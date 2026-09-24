// 文件导读：这些 AlpacaPaper 方法是只读账户/持仓/历史/时钟接口，以及带恢复语义的
// commitment 执行入口。execute_committed 先校验 plan/commitment，再按 replacement
// 和原始 client_order_id 查找已存在订单；只有缺失订单且当前授权与交易 session 都仍
// 有效时才 POST。reconcile_committed 只刷新既有 broker order，不创建新订单。

impl AlpacaPaper {
    pub fn new(base_url: impl Into<String>, credentials: PaperCredentials) -> Result<Self> {
        // `impl Into<String>` 是编译期泛型输入，具体字符串类型由调用方决定；转换后取得
        // String 所有权。credentials 也按值移入，只有 exact Paper endpoint 通过才构造 client。
        let supplied = base_url.into();
        if !is_alpaca_paper_base_url(&supplied) {
            return Err(PaperError::NonPaperEndpoint(supplied));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .http1_only()
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            .connect_timeout(std::time::Duration::from_secs(15))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|source| PaperError::Transport {
                url: supplied.clone(),
                source,
            })?;
        Ok(Self {
            client,
            base_url: "https://paper-api.alpaca.markets".to_owned(),
            credentials,
        })
    }

    pub fn from_env() -> Result<Self> {
        // 缺少 base URL 时使用固定 Paper 地址；存在但格式/host 不合规则交给 new 拒绝。
        // 凭据读取的 `?` 先失败则不会构造 client 或发出 HTTP 请求。
        let base_url = env::var("ALPACA_PAPER_BASE_URL")
            .unwrap_or_else(|_| "https://paper-api.alpaca.markets".to_owned());
        Self::new(base_url, PaperCredentials::from_env()?)
    }

    pub async fn account(&self) -> Result<Value> {
        // async fn 调用先创建 Future；await 时才进入 GET，返回原始 JSON Value，不解释余额或授权。
        self.get_json("/v2/account").await
    }

    pub async fn positions(&self) -> Result<Value> {
        // async Future 被调用方 await 时执行 GET；持仓 JSON 保留给后续 ingest/快照标准化，
        // 本方法不改变 Broker 状态。
        self.get_json("/v2/positions").await
    }

    pub async fn portfolio_history(&self, range: PortfolioHistoryRange) -> Result<Value> {
        // range 按 Copy 枚举值映射为固定 URL path，避免用户任意字符串进入外部请求路径；
        // Future 完成只代表取得 history 响应，不代表校准样本合格。
        self.get_json(range.path()).await
    }

    pub async fn market_clock(&self) -> Result<MarketClock> {
        // 先取 Alpaca clock，再以其美东日期请求相邻日历，最后由领域 TradingSession
        // 计算 Regular/Extended/Overnight/Closed；is_open 本身不单独决定可提交性。
        // 两次 GET 按顺序 await；clock 或 calendar 任一请求/解析失败都会经 `?` 返回，
        // 不构造不完整时钟，也不采用本机时区/节假日默认值。
        let clock = self.get_json("/v2/clock").await?;
        let timestamp: DateTime<Utc> = required_string(&clock, "timestamp")?
            .parse()
            .map_err(|e: chrono::ParseError| PaperError::InvalidClock(e.to_string()))?;
        let date = timestamp
            .with_timezone(&chrono_tz::America::New_York)
            .date_naive();
        let calendar = self
            .get_json(&format!(
                "/v2/calendar?start={}&end={}",
                date - chrono::Duration::days(1),
                date + chrono::Duration::days(14)
            ))
            .await?;
        market_clock_with_calendar(&clock, &calendar)
    }

    async fn execute_committed(
        &self,
        commitment: &PaperCommitment,
        plan: &ExecutionPlan,
        authorization: &PaperSubmissionAuthorization,
    ) -> Result<PaperExecution> {
        // 恢复路径先查替换订单和原订单，已存在的 broker effect 只作为 reused 事实保留；
        // 逐单二次检查授权/时段后才允许 POST。部分恢复遇到窗口失效会停止补发，但不会
        // 丢弃已查到的 receipts。
        // async fn 返回 Future；dispatch await 后本函数逐单顺序查询/提交，不在这里 spawn。
        // Commitment 与 plan 闭包不匹配时在任何 Broker I/O 前失败。
        validate_commitment(commitment, plan)?;
        let mut orders = Vec::with_capacity(plan.orders.len());
        for order in &plan.orders {
            // 第一遍只 GET 查询 replacement r1 和原始 r0，收集已发生效果；缺失 ID 或
            // 查询错误中止，且不把不存在的订单伪装成已拒绝。
            let client_order_id = commitment
                .client_order_ids
                .get(&order.asset)
                .ok_or(PaperError::CommitmentClientOrderMismatch(order.asset))?;
            let replacement_client_order_id = replacement_client_order_id(client_order_id);
            let receipt = match self.lookup(&replacement_client_order_id).await? {
                Some(receipt) => Some(PaperOrderReceipt {
                    reused: true,
                    reprice_count: 1,
                    ..receipt
                }),
                None => self
                    .lookup(client_order_id)
                    .await?
                    .map(|receipt| PaperOrderReceipt {
                        reused: true,
                        reprice_count: 0,
                        ..receipt
                    }),
            };
            orders.push(receipt);
        }
        for (index, order) in plan.orders.iter().enumerate() {
            // 第二遍只对第一遍仍为 None 的订单考虑 POST；已找到回执的订单不重复提交。
            if orders[index].is_none() {
                let client_order_id = commitment
                    .client_order_ids
                    .get(&order.asset)
                    .ok_or(PaperError::CommitmentClientOrderMismatch(order.asset))?;
                // Lookup remains available after expiry. A partial recovery is
                // evidence of existing effects, not permission to fill gaps.
                // 在每个新订单前先检查计划发送窗口，读取最新交易 session/overnight 资格，
                // 再复查时间窗口以缩短检查与 POST 之间的授权过期竞态。
                let permission = match authorization.assert_current(&plan.plan_hash, Utc::now()) {
                    Ok(()) => {
                        self.assert_market_session(&commitment.broker_session, order, authorization)
                            .await
                    }
                    Err(error) => Err(error),
                }
                .and_then(|()| authorization.assert_current(&plan.plan_hash, Utc::now()));
                if let Err(error) = permission {
                    // 若已观察到任一外部订单效果，窗口关闭时停止补发并返回已知 receipts；
                    // 只有全部为空时才将错误交给 caller。Partial PaperExecution 由 dispatch
                    // 持久化进度并继续 reconcile，不等价于整个 commitment 已完成。
                    if matches!(
                        error,
                        PaperError::SubmissionUnauthorized
                            | PaperError::MarketClosed
                            | PaperError::InvalidCommitment(_)
                    ) && orders.iter().any(Option::is_some)
                    {
                        break;
                    }
                    return Err(error);
                }
                // POST 的传输错误可能留下远端未知状态；本层不重试，下一次 dispatch 用同一
                // client_order_id 查询恢复。
                orders[index] = Some(self.submit_order(order, client_order_id, 0).await?);
            }
        }
        // flatten 消费 Option 回执列表并丢弃仍未知的 None；返回可部分完成，settled 由外层
        // 按 commitment 全量资产与每个状态终态再判断。
        Ok(PaperExecution {
            plan_hash: plan.plan_hash.clone(),
            orders: orders.into_iter().flatten().collect(),
        })
    }

    async fn reconcile_committed(
        &self,
        commitment: &PaperCommitment,
        execution: &PaperExecution,
    ) -> Result<PaperExecution> {
        // 对已返回的 broker order 做只读刷新，并确认每个 client_order_id 是原始或唯一
        // durable replacement；不会因为对账缺数据而创建新的订单。
        // 执行和 commitment 均以共享借用输入；plan hash 不同立即拒绝，逐条 GET 只更新
        // 已知 client/broker identity，不会为缺失资产补发订单。
        if execution.plan_hash != commitment.plan_hash {
            return Err(PaperError::CommitmentPlanHashMismatch);
        }
        let mut orders = Vec::with_capacity(execution.orders.len());
        for receipt in &execution.orders {
            let asset = Asset::try_from(receipt.symbol.as_str())?;
            let original = commitment
                .client_order_ids
                .get(&asset)
                .ok_or(PaperError::CommitmentClientOrderMismatch(asset))?;
            if receipt.client_order_id != *original
                && receipt.client_order_id != replacement_client_order_id(original)
            {
                return Err(PaperError::CommitmentClientOrderMismatch(asset));
            }
            orders.push(
                self.get_order(
                    &receipt.broker_order_id,
                    &receipt.client_order_id,
                    receipt.reprice_count,
                )
                .await?,
            );
        }
        Ok(PaperExecution {
            plan_hash: execution.plan_hash.clone(),
            orders,
        })
    }
}
