// 文件导读：对账层把 broker receipts、durable reprice/cancel intents 和原始 Commitment
// 合成为可审计的 OrderReceipt 与 Reconciliation Artifact。它先验证 plan/client ID 闭包，
// 再合并替换订单的数量与加权成交价，按“所有订单终态且 successor 已观察”决定 Complete；
// 未完成或部分成交只写进度，不被包装成成交完成。

fn validate_commitment(commitment: &PaperCommitment, plan: &ExecutionPlan) -> Result<()> {
    // Commitment、plan hash、session、订单数量和每个确定性 client ID 必须逐项一致，防止
    // 对账把另一份计划的 broker 回执挂到当前 Run。
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
        let value = self
            .get_json(&format!("/v2/orders/{broker_order_id}"))
            .await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }

    async fn cancel_committed_order(&self, intent: &PaperCancel) -> Result<PaperOrderReceipt> {
        // 取消先查原订单并处理终态/pending_cancel/replaced 分支；只有可取消的 durable 原单
        // 才执行 DELETE，随后再 GET 确认 broker 状态。
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
        intent.validate()?;
        if let Some(existing) = self.lookup(&intent.replacement_client_order_id).await? {
            return Ok(PaperOrderReceipt {
                reused: true,
                ..existing
            });
        }
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
