//! Rust-owned decision finalization and Paper execution transitions.

use std::collections::BTreeMap;

use akzio_context::legacy::{ContextBroker, NewJsonDocument};
use akzio_domain::{
    Asset, DocumentKind, DocumentLifecycle, DocumentOrigin, MoneyMicros, RunId, RunPurpose,
};
use akzio_execution::{
    paper::AlpacaPaper, AccountSnapshot, ExecutionPlan, ExecutionRunContext, ExecutionRuntime,
    ExecutionRuntimeError, Position, Quote,
};
use akzio_ingest::legacy::{IngestConfig, Ingestor};
use akzio_learning::LearningLedger;
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::value::parse_money;
use crate::{Daemon, DaemonError, Result};

impl Daemon {
    pub(super) fn finalize_decision(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        purpose: RunPurpose,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let draft = self.latest_document(run_id, DocumentKind::DecisionDraft)?;
        let context_manifest = self.source_document(&draft, DocumentKind::ContextManifest)?;
        let memory_refs = LearningLedger::new(broker.clone())
            .decision_prior_documents()?
            .into_iter()
            .map(|document| document.document_id)
            .collect();
        let context = ExecutionRunContext {
            run_id: run_id.clone(),
            purpose,
            origin: origin.clone(),
            now,
        };
        ExecutionRuntime::with_defaults(broker.clone()).finalize_decision(
            &context,
            &draft.document_id,
            &context_manifest.document_id,
            memory_refs,
        )?;
        Ok(())
    }
    pub(super) async fn build_execution_plan(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        purpose: RunPurpose,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let decision = self.latest_document(run_id, DocumentKind::Decision)?;
        let (account, quotes, source_refs, input) =
            if matches!(purpose, RunPurpose::Paper | RunPurpose::PaperDryRun) {
                let sealed = Ingestor::from_env(IngestConfig::default())?
                    .seal(broker, run_id, origin.clone(), now)
                    .await?;
                let input = broker.read_json(&sealed.normalized)?;
                let (account, quotes) = parse_execution_input(&input, now)?;
                (
                    account,
                    quotes,
                    vec![decision.document_id.clone(), sealed.normalized.document_id],
                    input,
                )
            } else {
                let (account, quotes) = debug_execution_input(now);
                (
                    account,
                    quotes,
                    vec![decision.document_id.clone()],
                    serde_json::json!({"mode":"synthetic", "observed_at": now}),
                )
            };
        let turnover = self.daily_paper_turnover(now)?;
        let execution_context = broker.record_json(NewJsonDocument {
            kind: DocumentKind::ExecutionContext,
            producer: "execution.context".to_owned(),
            run_id: Some(run_id.clone()),
            lifecycle: DocumentLifecycle::RunScoped,
            source_refs,
            origin: Some(origin.clone()),
            value: &serde_json::json!({
                "account": account,
                "quotes": quotes,
                "daily_turnover": turnover,
                "input": input,
            }),
            created_at: now,
        })?;
        let runtime = ExecutionRuntime::with_defaults(broker.clone());
        let context = ExecutionRunContext {
            run_id: run_id.clone(),
            purpose,
            origin: origin.clone(),
            now,
        };
        match runtime.build_plan(
            &context,
            &decision.document_id,
            &execution_context.document_id,
            &account,
            &quotes,
            turnover,
        ) {
            Ok(_) => Ok(()),
            Err(ExecutionRuntimeError::Policy(error)) => self.record_task_result(
                broker,
                run_id,
                origin,
                &format!("execution.rejected:{error}"),
                now,
            ),
            Err(ExecutionRuntimeError::ExpiredDecision(decision_id)) => self.record_task_result(
                broker,
                run_id,
                origin,
                &format!("execution.rejected:expired decision {decision_id}"),
                now,
            ),
            Err(error) => Err(error.into()),
        }
    }
    pub(super) async fn submit_paper(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        purpose: RunPurpose,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<()> {
        if purpose != RunPurpose::Paper {
            return self.record_task_result(broker, run_id, origin, "execution.dry_run", now);
        }
        let Some(plan) = self.latest_document_optional(run_id, DocumentKind::ExecutionPlan)? else {
            return self.record_task_result(broker, run_id, origin, "execution.no_plan", now);
        };
        let paper = AlpacaPaper::from_env().map_err(ExecutionRuntimeError::Paper)?;
        let context = ExecutionRunContext {
            run_id: run_id.clone(),
            purpose,
            origin,
            now,
        };
        ExecutionRuntime::with_defaults(broker.clone())
            .submit_paper(&context, &plan.document_id, &paper)
            .await?;
        Ok(())
    }
    pub(super) async fn reconcile_paper(
        &self,
        broker: &ContextBroker,
        run_id: &RunId,
        purpose: RunPurpose,
        origin: DocumentOrigin,
        now: DateTime<Utc>,
    ) -> Result<()> {
        if purpose != RunPurpose::Paper {
            return self.record_task_result(
                broker,
                run_id,
                origin,
                "execution.reconcile_dry_run",
                now,
            );
        }
        let Some(order_state) = self.latest_document_optional(run_id, DocumentKind::OrderState)?
        else {
            return self.record_task_result(broker, run_id, origin, "execution.no_orders", now);
        };
        let paper = AlpacaPaper::from_env().map_err(ExecutionRuntimeError::Paper)?;
        let context = ExecutionRunContext {
            run_id: run_id.clone(),
            purpose,
            origin,
            now,
        };
        ExecutionRuntime::with_defaults(broker.clone())
            .reconcile_paper(&context, &order_state.document_id, &paper)
            .await?;
        Ok(())
    }
    fn daily_paper_turnover(&self, now: DateTime<Utc>) -> Result<MoneyMicros> {
        let mut total = 0_i64;
        for document in self.store.documents_by_kind(DocumentKind::ExecutionPlan)? {
            if document.created_at.date_naive() != now.date_naive() {
                continue;
            }
            let Some(run_id) = &document.run_id else {
                continue;
            };
            if self.store.run_purpose(run_id)? != RunPurpose::Paper {
                continue;
            }
            let plan = serde_json::from_value::<ExecutionPlan>(
                ContextBroker::new(self.store.clone()).read_json(&document)?,
            )?;
            total = total.saturating_add(
                plan.orders
                    .iter()
                    .map(|order| order.notional.0)
                    .sum::<i64>(),
            );
        }
        Ok(MoneyMicros(total))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        sync::{Mutex, MutexGuard},
    };

    use akzio_context::legacy::ContextBroker;
    use akzio_domain::{AttemptId, DocumentKind, DocumentOrigin, RunId, RunPurpose, TaskId};
    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;
    use crate::{fixture_model_client, DaemonConfig};

    static ENVIRONMENT_LOCK: Mutex<()> = Mutex::new(());

    struct EnvironmentRestore {
        values: Vec<(&'static str, Option<OsString>)>,
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvironmentRestore {
        fn non_paper_broker() -> Self {
            let lock = ENVIRONMENT_LOCK.lock().unwrap();
            let replacements = [
                ("ALPACA_API_KEY", "dry-run-test-key"),
                ("ALPACA_API_SECRET", "dry-run-test-secret"),
                ("ALPACA_PAPER_BASE_URL", "https://api.alpaca.markets"),
            ];
            let values = replacements
                .iter()
                .map(|(name, _)| (*name, std::env::var_os(name)))
                .collect();
            for (name, value) in replacements {
                std::env::set_var(name, value);
            }
            Self {
                values,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvironmentRestore {
        fn drop(&mut self) {
            for (name, value) in &self.values {
                if let Some(value) = value {
                    std::env::set_var(name, value);
                } else {
                    std::env::remove_var(name);
                }
            }
        }
    }

    #[tokio::test]
    async fn paper_dry_run_records_non_submission_before_broker_construction() {
        let directory = tempdir().unwrap();
        let daemon = Daemon::with_model(
            DaemonConfig {
                store_root: directory.path().to_path_buf(),
                http_token: "test-token".to_owned(),
                worker_count: 1,
                auto_paper: false,
            },
            fixture_model_client(),
        )
        .unwrap();
        let now = Utc::now();
        let run_id = RunId::new();
        daemon
            .store()
            .create_run(&run_id, RunPurpose::PaperDryRun, "test", now)
            .unwrap();
        let broker = ContextBroker::new(daemon.store().clone());
        let _environment = EnvironmentRestore::non_paper_broker();

        daemon
            .submit_paper(
                &broker,
                &run_id,
                RunPurpose::PaperDryRun,
                DocumentOrigin::task(TaskId::new(), AttemptId::new(), None),
                now,
            )
            .await
            .unwrap();

        let result = daemon
            .store()
            .documents_for_run(&run_id)
            .unwrap()
            .into_iter()
            .find(|document| document.kind == DocumentKind::TaskResult)
            .unwrap();
        assert_eq!(
            broker.read_json(&result).unwrap(),
            serde_json::json!({"outcome": "execution.dry_run"})
        );
    }
}

fn debug_execution_input(now: DateTime<Utc>) -> (AccountSnapshot, BTreeMap<Asset, Quote>) {
    let account = AccountSnapshot {
        equity: MoneyMicros::from_usd_cents(1_000_000),
        buying_power: MoneyMicros::from_usd_cents(1_000_000),
        active: true,
        trading_blocked: false,
        positions: BTreeMap::new(),
    };
    let quotes = Asset::EXECUTABLE
        .into_iter()
        .map(|asset| {
            (
                asset,
                Quote {
                    bid: MoneyMicros::from_usd_cents(10_000),
                    ask: MoneyMicros::from_usd_cents(10_001),
                    observed_at: now,
                },
            )
        })
        .collect();
    (account, quotes)
}

fn parse_execution_input(
    input: &Value,
    fallback_observed_at: DateTime<Utc>,
) -> Result<(AccountSnapshot, BTreeMap<Asset, Quote>)> {
    let account_value = input
        .get("account")
        .ok_or_else(|| DaemonError::InvalidInput("missing account".to_owned()))?;
    let equity = money_field(account_value, &["/equity"])?;
    let buying_power = money_field(account_value, &["/buying_power"])?;
    let active = account_value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| status.eq_ignore_ascii_case("active"))
        .unwrap_or(true);
    let trading_blocked = account_value
        .get("trading_blocked")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || account_value
            .get("account_blocked")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let mut positions = BTreeMap::new();
    if let Some(items) = input.get("positions").and_then(Value::as_array) {
        for item in items {
            let Some(symbol) = item.get("symbol").and_then(Value::as_str) else {
                continue;
            };
            let Ok(asset) = Asset::try_from(symbol) else {
                continue;
            };
            positions.insert(
                asset,
                Position {
                    market_value: money_field(item, &["/market_value"])?,
                },
            );
        }
    }
    let mut quotes = BTreeMap::new();
    for asset in Asset::EXECUTABLE {
        let quote = input
            .pointer(&format!("/market/{}/quote", asset.symbol()))
            .ok_or_else(|| {
                DaemonError::InvalidInput(format!("missing quote for {}", asset.symbol()))
            })?;
        let observed_at = quote
            .pointer("/t")
            .or_else(|| quote.pointer("/timestamp"))
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or(fallback_observed_at);
        quotes.insert(
            asset,
            Quote {
                bid: money_field(quote, &["/bp", "/bid_price"])?,
                ask: money_field(quote, &["/ap", "/ask_price"])?,
                observed_at,
            },
        );
    }
    Ok((
        AccountSnapshot {
            equity,
            buying_power,
            active,
            trading_blocked,
            positions,
        },
        quotes,
    ))
}

fn money_field(value: &Value, paths: &[&str]) -> Result<MoneyMicros> {
    let value = paths
        .iter()
        .find_map(|path| value.pointer(path))
        .ok_or_else(|| DaemonError::InvalidInput(format!("missing money field {paths:?}")))?;
    parse_money(value)
}
