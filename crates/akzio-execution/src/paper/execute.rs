impl AlpacaPaper {
    pub fn new(base_url: impl Into<String>, credentials: PaperCredentials) -> Result<Self> {
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
        let base_url = env::var("ALPACA_PAPER_BASE_URL")
            .unwrap_or_else(|_| "https://paper-api.alpaca.markets".to_owned());
        Self::new(base_url, PaperCredentials::from_env()?)
    }

    pub async fn account(&self) -> Result<Value> {
        self.get_json("/v2/account").await
    }

    pub async fn positions(&self) -> Result<Value> {
        self.get_json("/v2/positions").await
    }

    pub async fn portfolio_history(&self, range: PortfolioHistoryRange) -> Result<Value> {
        self.get_json(range.path()).await
    }

    pub async fn market_clock(&self) -> Result<MarketClock> {
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
