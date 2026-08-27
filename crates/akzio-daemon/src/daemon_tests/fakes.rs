#[derive(Clone)]
struct StaticSessionClock(Option<String>);

impl BrokerSessionClock for StaticSessionClock {
    fn open_session_key<'a>(
        &'a self,
    ) -> Pin<
        Box<dyn Future<Output = std::result::Result<Option<String>, SchedulerError>> + Send + 'a>,
    > {
        Box::pin(async move { Ok(self.0.clone()) })
    }

    fn paper_account_id<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = std::result::Result<String, SchedulerError>> + Send + 'a>>
    {
        Box::pin(async move { Ok("fixture-paper-account".to_owned()) })
    }
}

#[derive(Clone)]
struct OutcomeBarsAdapter {
    responses: BTreeMap<String, AcquiredEvidence>,
}

impl OutcomeBarsAdapter {
    fn new(baseline: NaiveDate, observed_at: DateTime<Utc>) -> Self {
        let mut responses = BTreeMap::new();
        for asset in Asset::EXECUTABLE {
            let resource = format!(
                "bars:{}:1d:{}:6",
                asset.symbol(),
                baseline.format("%Y-%m-%d")
            );
            let mut bars = Vec::new();
            let mut date = baseline;
            let mut index = 0_u64;
            while bars.len() < 6 {
                date += ChronoDuration::days(1);
                if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
                    continue;
                }
                let price = 100.0 + index as f64;
                bars.push(serde_json::json!({
                    "t": format!("{}T20:00:00Z", date.format("%Y-%m-%d")),
                    "o": price,
                    "h": price + 1.0,
                    "l": price - 1.0,
                    "c": price + 0.5,
                    "v": 1_000,
                    "adjustment": "all",
                }));
                index += 1;
            }
            let normalized = serde_json::json!({"bars": bars});
            let raw = serde_json::to_vec(&normalized).unwrap();
            let source_uri = format!(
                    "https://paper-api.alpaca.markets/v2/stocks/{}/bars?timeframe=1Day&limit=6&adjustment=all&start={}",
                    asset.symbol(),
                    baseline.format("%Y-%m-%d")
                );
            responses.insert(
                resource.clone(),
                AcquiredEvidence {
                    raw,
                    media_type: "application/json".to_owned(),
                    source_uri: source_uri.clone(),
                    observed_at,
                    normalized,
                    provenance: EvidenceProvenance {
                        document_id: Some(resource.clone()),
                        published_at: None,
                        observed_at,
                        revision: Some("fixture-bars-v1".to_owned()),
                        source_uri,
                        dedupe_key: resource,
                        citations: Vec::new(),
                    },
                    quality: EvidenceQuality::default(),
                },
            );
        }
        Self { responses }
    }

    fn with_responses(mut self, responses: BTreeMap<String, AcquiredEvidence>) -> Self {
        self.responses.extend(responses);
        self
    }
}

impl AsyncEvidenceAdapter for OutcomeBarsAdapter {
    fn source(&self) -> EvidenceSource {
        EvidenceSource::Alpaca
    }

    fn acquire<'a>(
        &'a self,
        request: &'a EvidenceRequest,
    ) -> BoxFuture<'a, std::result::Result<AcquiredEvidence, EvidenceAdapterError>> {
        let result = if request.source != EvidenceSource::Alpaca {
            Err(EvidenceAdapterError::SourceMismatch)
        } else {
            self.responses
                .get(&request.resource)
                .cloned()
                .ok_or_else(|| EvidenceAdapterError::MissingFixture(request.resource.clone()))
        };
        Box::pin(async move { result })
    }
}

#[derive(Default)]
struct FakePaperBroker {
    submissions: AtomicUsize,
}

impl CommittedPaperBroker for FakePaperBroker {
    fn execute_commitment<'a>(
        &'a self,
        commitment: &'a akzio_domain::PaperCommitment,
        plan: &'a akzio_execution::ExecutionPlan,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
    {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        let execution = PaperExecution {
            plan_hash: plan.plan_hash.clone(),
            orders: plan
                .orders
                .iter()
                .map(|order| PaperOrderReceipt {
                    client_order_id: commitment.client_order_ids[&order.asset].clone(),
                    broker_order_id: format!("fixture-{}", order.asset.symbol()),
                    symbol: order.asset.symbol().to_owned(),
                    status: "filled".to_owned(),
                    requested_quantity_micros: 1_000_000,
                    filled_quantity_micros: 1_000_000,
                    remaining_quantity_micros: 0,
                    average_fill_price: Some(order.limit_price),
                    broker_updated_at: Utc::now(),
                    reason: None,
                    reused: false,
                    reprice_count: 0,
                })
                .collect(),
        };
        Box::pin(async move { Ok(execution) })
    }

    fn reconcile_commitment<'a>(
        &'a self,
        _commitment: &'a akzio_domain::PaperCommitment,
        execution: &'a PaperExecution,
    ) -> Pin<Box<dyn Future<Output = akzio_execution::paper::Result<PaperExecution>> + Send + 'a>>
    {
        let execution = execution.clone();
        Box::pin(async move { Ok(execution) })
    }
}
