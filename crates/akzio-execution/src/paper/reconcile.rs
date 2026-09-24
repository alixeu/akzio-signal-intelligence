// 文件导读：此 include 片段核验 Commitment/plan 身份，并实现 Broker GET、POST、
// DELETE、PATCH 及只读刷新。它返回 PaperOrderReceipt；durable action intent 由上层
// Dispatch/Store 管，领域 OrderReceipt 聚合、Complete 判定及进度提交由
// ReconciliationRuntime 负责。HTTP accepted 或 DELETE 返回不等于实际成交/终态。

fn validate_commitment(commitment: &PaperCommitment, plan: &ExecutionPlan) -> Result<()> {
    // Commitment、plan hash、session、订单数量和每个确定性 client ID 必须逐项一致，防止
    // 对账把另一份计划的 broker 回执挂到当前 Run。
    // 两个参数均共享借用；Domain 错误先转换为 InvalidCommitment，计划校验错误沿 PaperError
    // 的 From 路径传播。之后按订单序重算 r0 ID 并逐资产比较。
    commitment
        .validate()
        .map_err(|error| PaperError::InvalidCommitment(error.to_string()))?;
    plan.validate()?;
    if commitment.plan_hash != plan.plan_hash {
        return Err(PaperError::CommitmentPlanHashMismatch);
    }
    if commitment.broker_session != plan.broker_session {
        return Err(PaperError::InvalidCommitment(
            "broker session does not match execution plan".to_owned(),
        ));
    }
    if commitment.client_order_ids.len() != plan.orders.len() {
        return Err(PaperError::InvalidCommitment(
            "client order count does not match allocation plan".to_owned(),
        ));
    }
    for (index, order) in plan.orders.iter().enumerate() {
        let expected = client_order_id(&commitment.broker_session, &plan.plan_hash, index, 0);
        if commitment.client_order_ids.get(&order.asset) != Some(&expected) {
            return Err(PaperError::CommitmentClientOrderMismatch(order.asset));
        }
    }
    Ok(())
}

impl AlpacaPaper {
    async fn assert_market_session(
        &self,
        broker_session: &str,
        order: &OrderIntent,
        authorization: &PaperSubmissionAuthorization,
    ) -> Result<()> {
        // 在每个尚未提交的订单前重新读取 clock；Closed、session/extended_hours 不匹配或
        // Overnight 资产资格失败都会阻断新 POST，但不影响已存在订单的只读恢复。
        // 此 async Future 由执行路径 await 驱动；每次只读取新时钟，期间不持有 Store 锁。
        let clock = self.market_clock().await?;
        if clock.session.kind == akzio_domain::TradingSession::Closed {
            return Err(PaperError::MarketClosed);
        }
        if !authorization.matches_session(&clock.session, broker_session)
            || order.extended_hours != clock.session.kind.extended_hours()
        {
            return Err(PaperError::InvalidCommitment(
                "unsubmitted order belongs to a different trading session".to_owned(),
            ));
        }
        // 只有 Overnight 额外查资产资格；GET 失败传播为 Err，资格不满足映射 Closed，
        // 两种情况都阻断新单，但不撤销此前已存在的 broker receipt。
        if clock.session.kind == akzio_domain::TradingSession::Overnight {
            let asset = self
                .get_json(&format!("/v2/assets/{}", order.asset.symbol()))
                .await?;
            if !akzio_domain::alpaca_overnight_asset_available(&asset, order.asset) {
                return Err(PaperError::MarketClosed);
            }
        }
        Ok(())
    }

    async fn lookup(&self, client_order_id: &str) -> Result<Option<PaperOrderReceipt>> {
        // 按 durable client ID 查询是幂等恢复的唯一入口；404 表示尚未观察到效果，不能等同取消。
        // client_order_id 仅借用来构造查询参数；send/text 都在 await 时执行并把网络错误
        // 映射为 Transport。响应体读取成功后再分 404、其它非成功和可解析回执三条路径。
        let url = self.url("/v2/orders:by_client_order_id");
        let response = self
            .authorized(
                self.client
                    .get(&url)
                    .query(&[("client_order_id", client_order_id)]),
            )
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        // 404 表示当前查不到，不代表被取消；其它 HTTP 失败保留 body 返回诊断错误。
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(PaperError::Http { url, status, body });
        }
        let value = parse_value(&body);
        Ok(Some(receipt_from_value(
            value,
            client_order_id,
            false,
            reprice_count_from_client_order_id(client_order_id),
        )?))
    }

    async fn submit_order(
        &self,
        order: &OrderIntent,
        client_order_id: &str,
        reprice_count: u8,
    ) -> Result<PaperOrderReceipt> {
        // 只在上层完成授权复核后发送一次 POST，并立即把 broker 返回解析成带原始 client ID 的 receipt。
        // `order_request` 先在内存中完成数量/价格编码；post_json 内部不重试，网络结果不确定
        // 时由上层下次按同一 durable client ID 查询恢复。
        let url = self.url("/v2/orders");
        let body = order_request(order, client_order_id)?;
        let value = self.post_json(&url, body).await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }

    async fn get_order(
        &self,
        broker_order_id: &str,
        client_order_id: &str,
        reprice_count: u8,
    ) -> Result<PaperOrderReceipt> {
        // 通过 broker order ID 刷新已有订单，保留 client ID 和 repricing 代数供后续闭包校验。
        // broker_order_id/client ID 作为借用拼入只读 GET 路径；receipt parser 仍核验返回
        // client ID 与 durable identity 一致。
        let value = self
            .get_json(&format!("/v2/orders/{broker_order_id}"))
            .await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }

    async fn cancel_committed_order(&self, intent: &PaperCancel) -> Result<PaperOrderReceipt> {
        // 取消先查原订单并处理终态/pending_cancel/replaced 分支；只有可取消的 durable 原单
        // 才执行 DELETE，随后再 GET 确认 broker 状态。
        // intent.validate 先做纯领域身份检查；随后 lookup/DELETE/GET 顺序执行，错误按 `?`
        // 返回，上层 effect intent 若未 settle 会留待恢复，而不是在此伪造取消成功。
        intent.validate()?;
        let existing = self.lookup(&intent.client_order_id).await?.ok_or_else(|| {
            PaperError::InvalidCommitment(
                "cancel intent order is not visible by durable client order ID".to_owned(),
            )
        })?;
        if existing.broker_order_id != intent.broker_order_id {
            return Err(PaperError::InvalidCommitment(
                "cancel intent broker order ID does not match lookup".to_owned(),
            ));
        }
        // 已终态或 pending_cancel 时返回 reused receipt，避免重复 DELETE；replaced 说明应由
        // durable successor 处理，因此不取消已替换的旧 broker order。
        if broker_status_is_final_without_successor(&existing.status) {
            return Ok(PaperOrderReceipt {
                reused: true,
                ..existing
            });
        }
        if existing.status.eq_ignore_ascii_case("pending_cancel") {
            return Ok(PaperOrderReceipt {
                reused: true,
                ..existing
            });
        }
        if existing.status.eq_ignore_ascii_case("replaced") {
            return Err(PaperError::InvalidCommitment(
                "replaced order must be canceled through its durable successor".to_owned(),
            ));
        }
        let url = self.url(&format!("/v2/orders/{}", intent.broker_order_id));
        // 404/422 表示 DELETE 时订单已消失或不可取消，仍继续 GET 获取当前权威状态；
        // 其余 HTTP/Transport 错误直接返回，不能声明 cancel 已完成。
        match self.delete_empty(&url).await {
            Ok(()) => {}
            Err(PaperError::Http { status, .. })
                if status == StatusCode::NOT_FOUND
                    || status == StatusCode::UNPROCESSABLE_ENTITY => {}
            Err(error) => return Err(error),
        }
        self.get_order(
            &intent.broker_order_id,
            &intent.client_order_id,
            reprice_count_from_client_order_id(&intent.client_order_id),
        )
        .await
    }

    async fn replace_committed_order(
        &self,
        intent: &PaperReprice,
        authorization: &PaperSubmissionAuthorization,
    ) -> Result<PaperOrderReceipt> {
        // 替换先复用已存在 successor，再两次检查授权与 prior order 状态，最后 PATCH 并用
        // r1 client ID 解析回执；替换中的不确定状态不会盲目再发。
        // 先验证 intent，再优先查确定性的 successor：已存在时作为 reused 事实返回，不再 PATCH。
        intent.validate()?;
        if let Some(existing) = self.lookup(&intent.replacement_client_order_id).await? {
            return Ok(PaperOrderReceipt {
                reused: true,
                ..existing
            });
        }
        // 在查询 prior 前后各检查一次相同发送窗口；prior 已终态或 replacement 正在路上时
        // 不再次改单，防止未知状态下创建新的外部效果。
        authorization.assert_replacement_current(Utc::now())?;
        let prior = self
            .lookup(&intent.prior_client_order_id)
            .await?
            .ok_or_else(|| {
                PaperError::InvalidCommitment(
                    "replace intent prior order is not visible by durable client order ID"
                        .to_owned(),
                )
            })?;
        if prior.broker_order_id != intent.prior_broker_order_id {
            return Err(PaperError::InvalidCommitment(
                "replace intent broker order ID does not match lookup".to_owned(),
            ));
        }
        if broker_status_is_final_without_successor(&prior.status) {
            return Err(PaperError::InvalidCommitment(
                "cannot replace a terminal broker order".to_owned(),
            ));
        }
        if matches!(
            prior.status.trim().to_ascii_lowercase().as_str(),
            "pending_replace" | "replaced"
        ) {
            return Err(PaperError::InvalidCommitment(
                "replacement is in flight but deterministic successor is not visible".to_owned(),
            ));
        }
        authorization.assert_replacement_current(Utc::now())?;
        let url = self.url(&format!("/v2/orders/{}", intent.prior_broker_order_id));
        // 仅身份和时效仍有效才 PATCH；传输错误不重试，恢复由后续 successor lookup 完成。
        let value = self
            .patch_json(
                &url,
                serde_json::json!({
                    "limit_price": money_string(intent.replacement_limit_price),
                    "client_order_id": intent.replacement_client_order_id,
                }),
            )
            .await?;
        receipt_from_value(value, &intent.replacement_client_order_id, false, 1)
    }
}
