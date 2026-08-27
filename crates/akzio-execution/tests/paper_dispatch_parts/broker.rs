impl FakeCommittedBroker {
    fn new(statuses: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            state: Mutex::new(FakeBrokerState {
                statuses: statuses.into_iter().map(ToOwned::to_owned).collect(),
                orders: BTreeMap::new(),
            }),
            execute_calls: AtomicUsize::new(0),
            actual_submit_calls: AtomicUsize::new(0),
            lookup_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
            fail_reconcile_once: AtomicBool::new(false),
        }
    }

    fn fail_next_reconcile(&self) {
        self.fail_reconcile_once.store(true, Ordering::SeqCst);
    }

    fn receipt(
        client_order_id: String,
        broker_order_id: String,
        symbol: String,
        status: &str,
        reprice_count: u8,
        now: DateTime<Utc>,
    ) -> PaperOrderReceipt {
        let requested_quantity_micros = 4_000_000;
        let filled_quantity_micros = match status {
            "partially_filled" => 2_000_000,
            "filled" => requested_quantity_micros,
            _ => 0,
        };
        PaperOrderReceipt {
            client_order_id,
            broker_order_id,
            symbol,
            status: status.to_owned(),
            requested_quantity_micros,
            filled_quantity_micros,
            remaining_quantity_micros: requested_quantity_micros - filled_quantity_micros,
            average_fill_price: (filled_quantity_micros > 0)
                .then_some(MoneyMicros::from_usd_cents(2_500)),
            broker_updated_at: now,
            reason: match status {
                "canceled" => Some("fixture cancellation".to_owned()),
                "rejected" => Some("fixture rejection".to_owned()),
                _ => None,
            },
            reused: false,
            reprice_count,
        }
    }

    fn set_status(receipt: &mut PaperOrderReceipt, status: &str, now: DateTime<Utc>) {
        let next = Self::receipt(
            receipt.client_order_id.clone(),
            receipt.broker_order_id.clone(),
            receipt.symbol.clone(),
            status,
            receipt.reprice_count,
            now,
        );
        *receipt = next;
    }
}

impl CommittedPaperBroker for FakeCommittedBroker {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        plan: &'a ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = PaperResult<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            let now = Utc::now();
            let mut state = self.state.lock().unwrap();
            let orders = plan
                .orders
                .iter()
                .map(|order| {
                    let client_order_id = commitment.client_order_ids[&order.asset].clone();
                    if let Some(existing) = state.orders.get(&client_order_id) {
                        self.lookup_calls.fetch_add(1, Ordering::SeqCst);
                        return PaperOrderReceipt {
                            reused: true,
                            ..existing.clone()
                        };
                    }
                    self.actual_submit_calls.fetch_add(1, Ordering::SeqCst);
                    let receipt = Self::receipt(
                        client_order_id.clone(),
                        format!("fixture-{}", order.asset.symbol()),
                        order.asset.symbol().to_owned(),
                        "accepted",
                        0,
                        now,
                    );
                    state.orders.insert(client_order_id, receipt.clone());
                    receipt
                })
                .collect();
            Ok(PaperExecution {
                plan_hash: plan.plan_hash.clone(),
                orders,
            })
        })
    }


    fn reconcile_commitment<'a>(
        &'a self,
        commitment: &'a PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = PaperResult<PaperExecution>> + Send + 'a>> {
        Box::pin(async move {
            self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_reconcile_once.swap(false, Ordering::SeqCst) {
                return Err(PaperError::InvalidCommitment(
                    "fixture crash after submit".to_owned(),
                ));
            }
            if execution.plan_hash != commitment.plan_hash {
                return Err(PaperError::CommitmentPlanHashMismatch);
            }
            let now = Utc::now();
            let mut state = self.state.lock().unwrap();
            let status = state
                .statuses
                .pop_front()
                .unwrap_or_else(|| "filled".to_owned());
            let orders = execution
                .orders
                .iter()
                .map(|submitted| {
                    let mut receipt = state
                        .orders
                        .get(&submitted.client_order_id)
                        .cloned()
                        .ok_or_else(|| PaperError::InvalidCommitment("missing order".to_owned()))?;
                    receipt.reused = submitted.reused;
                    Self::set_status(&mut receipt, &status, now);
                    receipt.reused = submitted.reused;
                    state
                        .orders
                        .insert(receipt.client_order_id.clone(), receipt.clone());
                    Ok(receipt)
                })
                .collect::<PaperResult<Vec<_>>>()?;
            Ok(PaperExecution {
                plan_hash: execution.plan_hash.clone(),
                orders,
            })
        })
    }
}
