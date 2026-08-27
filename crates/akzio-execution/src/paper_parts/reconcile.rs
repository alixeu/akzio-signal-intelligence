impl AlpacaPaper {
    fn validate_commitment(
        &self,
        commitment: &PaperCommitment,
        plan: &ExecutionPlan,
    ) -> Result<()> {
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


    async fn assert_market_open(&self) -> Result<()> {
        if !self.market_clock().await?.is_open {
            return Err(PaperError::MarketClosed);
        }
        Ok(())
    }

    async fn lookup(&self, client_order_id: &str) -> Result<Option<PaperOrderReceipt>> {
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
        Ok(Some(receipt_from_value(value, client_order_id, false, 0)?))
    }

    async fn submit_order(
        &self,
        order: &OrderIntent,
        client_order_id: &str,
        reprice_count: u8,
    ) -> Result<PaperOrderReceipt> {
        let url = self.url("/v2/orders");
        let body = serde_json::json!({
            "symbol": order.asset.symbol(),
            "qty": quantity_string(order)?,
            "side": side_name(order.side),
            "type": "limit",
            "time_in_force": "day",
            "limit_price": money_string(order.limit_price),
            "extended_hours": false,
            "client_order_id": client_order_id,
        });
        let value = self.post_json(&url, body).await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }

    async fn get_order(
        &self,
        broker_order_id: &str,
        client_order_id: &str,
        reprice_count: u8,
    ) -> Result<PaperOrderReceipt> {
        let value = self
            .get_json(&format!("/v2/orders/{broker_order_id}"))
            .await?;
        receipt_from_value(value, client_order_id, false, reprice_count)
    }
}
