// 文件导读：这些 AlpacaPaper 方法是只读账户/持仓/历史/时钟接口，以及带恢复语义的
// commitment 执行入口。execute_committed 先校验 plan/commitment，再按 replacement
// 和原始 client_order_id 查找已存在订单；只有缺失订单且当前授权与交易 session 都仍
// 有效时才 POST。reconcile_committed 只刷新既有 broker order，不创建新订单。

impl AlpacaPaper {
    pub fn new(base_url: impl Into<String>, credentials: PaperCredentials) -> Result<Self> {
        // 先验证 exact Paper endpoint，再构造禁止重定向的 HTTP client；任何 endpoint
        // 不匹配都在网络 I/O 前返回。
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
        // 环境只提供 endpoint/凭据来源，最终仍复用 new 的 Paper endpoint 检查。
        let base_url = env::var("ALPACA_PAPER_BASE_URL")
            .unwrap_or_else(|_| "https://paper-api.alpaca.markets".to_owned());
        Self::new(base_url, PaperCredentials::from_env()?)
    }

    pub async fn account(&self) -> Result<Value> {
        // 账户读取是执行快照的原始输入，不在 adapter 内解释余额或授权。
        self.get_json("/v2/account").await
    }

    pub async fn positions(&self) -> Result<Value> {
        // 持仓读取保留 broker 原始 JSON，后续 ingest/执行快照负责标准化和 provenance。
        self.get_json("/v2/positions").await
    }

    pub async fn portfolio_history(&self, range: PortfolioHistoryRange) -> Result<Value> {
        // 通过枚举窗口读取历史净值，避免把用户字符串直接变成外部路径。
        self.get_json(range.path()).await
    }

    pub async fn market_clock(&self) -> Result<MarketClock> {
        // 先取 Alpaca clock，再以其美东日期请求相邻日历，最后由领域 TradingSession
        // 计算 Regular/Extended/Overnight/Closed；is_open 本身不单独决定可提交性。
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
        validate_commitment(commitment, plan)?;
        let mut orders = Vec::with_capacity(plan.orders.len());
        for order in &plan.orders {
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
            if orders[index].is_none() {
                let client_order_id = commitment
                    .client_order_ids
                    .get(&order.asset)
                    .ok_or(PaperError::CommitmentClientOrderMismatch(order.asset))?;
                // Lookup remains available after expiry. A partial recovery is
                // evidence of existing effects, not permission to fill gaps.
                let permission = match authorization.assert_current(&plan.plan_hash, Utc::now()) {
                    Ok(()) => {
                        self.assert_market_session(&commitment.broker_session, order, authorization)
                            .await
                    }
                    Err(error) => Err(error),
                }
                .and_then(|()| authorization.assert_current(&plan.plan_hash, Utc::now()));
                if let Err(error) = permission {
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
                orders[index] = Some(self.submit_order(order, client_order_id, 0).await?);
            }
        }
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
