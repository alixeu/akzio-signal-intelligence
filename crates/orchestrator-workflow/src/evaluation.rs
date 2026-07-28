//! Deterministic, non-blocking materialization of matured Decision snapshots.
//!
//! This module has no LLM dependency. It accepts only Rust-owned Decision
//! snapshots and raw technical CSV bytes, persists those bytes first, then
//! publishes a canonical Outcome or a durable materialization gap.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use orchestrator_core::{
    AdjustmentPolicy, BenchmarkOutcome, BenchmarkSelectionV1, CorporateActionCapability,
    DecisionSnapshotV2, DocumentRef, EvaluationInputManifestV1, MarketOutcome,
    MaterializationBatchReportV1, MaterializationGapReason, MaterializationGapV1,
    MaterializationIntegrityFailureKind, MaterializationIntegrityIssueV1,
    MaterializationResultKind, MaterializationResultV1, MemoryAttributionItemV1,
    MemoryAttributionLabel, MemoryAttributionRecordV1, MemoryUsageReferenceStatus,
    MemoryUsageReportV1, OutcomeRecordV1, OutcomeRevisionReason, OutcomeSection,
    OutcomeSectionUnavailableReason, PolicyRef, PriceBasis, PricePoint,
    TechnicalSeriesProvenanceV1, EVALUATION_INPUT_MANIFEST_SCHEMA_VERSION,
    MATERIALIZATION_BATCH_REPORT_SCHEMA_VERSION, MATERIALIZATION_GAP_SCHEMA_VERSION,
    OUTCOME_RECORD_SCHEMA_VERSION, TECHNICAL_SERIES_PROVENANCE_SCHEMA_VERSION,
};
use orchestrator_store::{
    content_hash, content_hash_bytes, list_run_locations, EvaluationStore, FileStore, RunLocation,
};
use serde_json::json;

use crate::orchestration::config::RuntimeConfig;

const MATERIALIZER_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct MarketSeriesInput {
    pub ticker: String,
    pub interval: String,
    pub provider: String,
    pub price_basis: PriceBasis,
    pub adjustment_policy: AdjustmentPolicy,
    pub corporate_action_capability: CorporateActionCapability,
    pub csv: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct MaterializerPolicyV1 {
    pub materialization_policy_ref: PolicyRef,
}

#[derive(Debug, Clone)]
pub struct MarketInputConfigV1 {
    pub interval: String,
    pub provider: String,
    pub price_basis: PriceBasis,
    pub adjustment_policy: AdjustmentPolicy,
    pub corporate_action_capability: CorporateActionCapability,
}

/// Canonical catch-up entry point shared by the CLI and the workflow. It
/// parses the same strict project configuration and therefore cannot invent a
/// benchmark, provider, or canonical-write permission on the command line.
pub fn materialize_from_config(
    config: &serde_json::Value,
    store_root: &std::path::Path,
    evaluation_date: &str,
    evaluation_run_id: &str,
    run_purpose: orchestrator_core::RunPurpose,
) -> Result<MaterializationBatchReportV1> {
    if !matches!(
        run_purpose,
        orchestrator_core::RunPurpose::Paper
            | orchestrator_core::RunPurpose::Live
            | orchestrator_core::RunPurpose::Replay
            | orchestrator_core::RunPurpose::MigrationFixture
    ) {
        anyhow::bail!("materialization CLI does not accept Mock or Debug purpose");
    }
    let runtime = RuntimeConfig::from_value(config)?;
    if !runtime.evaluation.enabled {
        anyhow::bail!("orchestrator.evaluation.enabled is required");
    }
    if run_purpose.may_write_canonical_evaluation()
        && !runtime.evaluation.canonical_memory_writes_enabled
    {
        anyhow::bail!("canonical materialization requires canonical_memory_writes_enabled");
    }
    let evaluation_config = orchestrator_core::config_get(config, "orchestrator.evaluation")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let config_hash = content_hash(&evaluation_config)?;
    let namespace = match run_purpose {
        orchestrator_core::RunPurpose::Paper | orchestrator_core::RunPurpose::Live => {
            orchestrator_core::PersistenceNamespace::Canonical
        }
        orchestrator_core::RunPurpose::Replay => orchestrator_core::PersistenceNamespace::Replay {
            replay_id: evaluation_run_id.to_owned(),
        },
        orchestrator_core::RunPurpose::MigrationFixture => {
            orchestrator_core::PersistenceNamespace::MigrationFixture {
                fixture_id: evaluation_run_id.to_owned(),
            }
        }
        _ => unreachable!("validated purpose"),
    };
    let context = orchestrator_core::PersistenceContextV1 {
        run_purpose,
        namespace,
        canonical_memory_writes_enabled: run_purpose.may_write_canonical_evaluation()
            && runtime.evaluation.canonical_memory_writes_enabled,
        invocation_id: evaluation_run_id.to_owned(),
        config_ref: PolicyRef {
            policy_id: "orchestrator.evaluation".to_owned(),
            version: runtime.evaluation.policy_version,
            content_hash: config_hash.clone(),
        },
        source_store_fingerprint: config_hash,
    };
    let location = RunLocation::new(evaluation_date, evaluation_run_id)?;
    let store = FileStore::open(
        store_root,
        orchestrator_store::FileStoreOptions {
            atomic_fsync: runtime.store.atomic_fsync,
            stale_temp_age: Some(std::time::Duration::from_secs(
                runtime.store.stale_temp_age_sec,
            )),
        },
    )?;
    let evaluation_store = EvaluationStore::open(store.clone(), context.clone())?;
    let decision_reader = if matches!(
        context.namespace,
        orchestrator_core::PersistenceNamespace::Canonical
    ) {
        evaluation_store.clone()
    } else {
        EvaluationStore::open(
            store.clone(),
            orchestrator_core::PersistenceContextV1 {
                run_purpose: orchestrator_core::RunPurpose::Paper,
                namespace: orchestrator_core::PersistenceNamespace::Canonical,
                canonical_memory_writes_enabled: false,
                invocation_id: evaluation_run_id.to_owned(),
                config_ref: context.config_ref.clone(),
                source_store_fingerprint: context.source_store_fingerprint.clone(),
            },
        )?
    };
    materialize_pending_from_decisions(
        &store,
        &decision_reader,
        &evaluation_store,
        &location,
        &MaterializerPolicyV1 {
            materialization_policy_ref: context.config_ref,
        },
        &MarketInputConfigV1 {
            interval: "daily".to_owned(),
            provider: runtime.evaluation.market_data_provider,
            price_basis: runtime.evaluation.price_basis,
            adjustment_policy: runtime.evaluation.market_data_adjustment_policy,
            corporate_action_capability: runtime.evaluation.corporate_action_capability,
        },
    )
}

/// Scan every canonical DecisionSnapshot and write one evaluation-run batch
/// report. Missing local market files are normal gaps; a typed-ledger read or
/// canonical write failure is returned for fail-closed handling by the caller.
pub fn materialize_pending(
    store: &FileStore,
    evaluation_store: &EvaluationStore,
    evaluation_run: &RunLocation,
    policy: &MaterializerPolicyV1,
    market: &MarketInputConfigV1,
) -> Result<MaterializationBatchReportV1> {
    materialize_pending_from_decisions(
        store,
        evaluation_store,
        evaluation_store,
        evaluation_run,
        policy,
        market,
    )
}

pub fn materialize_pending_from_decisions(
    store: &FileStore,
    decision_reader: &EvaluationStore,
    evaluation_store: &EvaluationStore,
    evaluation_run: &RunLocation,
    policy: &MaterializerPolicyV1,
    market: &MarketInputConfigV1,
) -> Result<MaterializationBatchReportV1> {
    let mut results = Vec::new();
    for source_location in list_run_locations(store)? {
        for decision in decision_reader.list_decisions(&source_location)? {
            let mut tickers = vec![decision.ticker.clone()];
            if let BenchmarkSelectionV1::Configured { binding } =
                &decision.evaluation_spec.benchmark_selection
            {
                if !tickers.iter().any(|ticker| ticker == &binding.benchmark_id) {
                    tickers.push(binding.benchmark_id.clone());
                }
            }
            let inputs = tickers
                .into_iter()
                .filter_map(|ticker| market_input_from_local_csv(&ticker, market).ok())
                .collect();
            match materialize_decision(
                evaluation_store,
                decision_reader,
                evaluation_run,
                &source_location,
                &decision,
                policy,
                inputs,
            ) {
                Ok(result) => results.push(result),
                Err(error) => results.push(record_materialization_failure(
                    evaluation_store,
                    decision_reader,
                    &source_location,
                    &decision,
                    policy,
                    &error,
                )?),
            }
        }
    }
    materialize_memory_attributions(store, evaluation_store, &policy.materialization_policy_ref)?;
    let batch_id = content_hash(&json!({
        "evaluation_run": evaluation_run.run_id,
        "policy": policy.materialization_policy_ref,
        "results": results,
    }))?;
    evaluation_store
        .write_batch_report(
            evaluation_run,
            MaterializationBatchReportV1 {
                schema_version: MATERIALIZATION_BATCH_REPORT_SCHEMA_VERSION,
                batch_id,
                evaluation_run_id: evaluation_run.run_id.clone(),
                run_purpose: evaluation_store.context().run_purpose,
                results,
                created_at: chrono::Utc::now().to_rfc3339(),
                content_hash: String::new(),
            },
        )
        .map_err(Into::into)
}

fn record_materialization_failure(
    evaluation_store: &EvaluationStore,
    decision_reader: &EvaluationStore,
    decision_location: &RunLocation,
    decision: &DecisionSnapshotV2,
    policy: &MaterializerPolicyV1,
    error: &anyhow::Error,
) -> Result<MaterializationResultV1> {
    let evaluation_key = evaluation_key(decision)?;
    let decision_ref =
        decision_reader.decision_reference(decision_location, &decision.decision_id)?;
    let detail = error.to_string();
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<orchestrator_store::StoreError>()
            .is_some()
    }) {
        let kind = classify_integrity_failure(&detail);
        let issue_id = content_hash(&json!({
            "evaluation_key": evaluation_key,
            "decision": decision_ref,
            "kind": kind,
            "detail": detail,
            "policy": policy.materialization_policy_ref,
        }))?;
        let reference =
            evaluation_store.write_integrity_issue(MaterializationIntegrityIssueV1 {
                schema_version: orchestrator_core::MATERIALIZATION_INTEGRITY_ISSUE_SCHEMA_VERSION,
                issue_id,
                evaluation_key: Some(evaluation_key.clone()),
                decision_ref: Some(decision_ref),
                kind,
                detail,
                created_at: chrono::Utc::now().to_rfc3339(),
                content_hash: String::new(),
            })?;
        return Ok(MaterializationResultV1 {
            decision_id: decision.decision_id.clone(),
            evaluation_key: Some(evaluation_key),
            kind: MaterializationResultKind::IntegrityFailure,
            document_ref: Some(reference),
        });
    }
    write_gap(
        evaluation_store,
        &evaluation_key,
        &decision_ref,
        MaterializationGapReason::DataIncomplete,
        detail,
        &policy.materialization_policy_ref,
    )
}

fn classify_integrity_failure(detail: &str) -> MaterializationIntegrityFailureKind {
    let detail = detail.to_ascii_lowercase();
    if detail.contains("content hash") || detail.contains("hash mismatch") {
        MaterializationIntegrityFailureKind::HashMismatch
    } else if detail.contains("schema") {
        MaterializationIntegrityFailureKind::UnknownSchema
    } else if detail.contains("path") || detail.contains("escape") {
        MaterializationIntegrityFailureKind::PathEscape
    } else if detail.contains("provenance") || detail.contains("reference") {
        MaterializationIntegrityFailureKind::ProvenanceViolation
    } else if detail.contains("collision") || detail.contains("immutable identity") {
        MaterializationIntegrityFailureKind::OutcomeIdCollision
    } else {
        MaterializationIntegrityFailureKind::LedgerCorruption
    }
}

/// Generate only conservative attribution records. A matured profitable or
/// losing Outcome alone is not causal evidence that a retrieved memory helped
/// or harmed the decision, so every first-version item is Unverifiable until
/// a controlled Memory Eval provides a counterfactual comparison.
pub fn materialize_memory_attributions(
    store: &FileStore,
    evaluation_store: &EvaluationStore,
    policy_ref: &PolicyRef,
) -> Result<usize> {
    let mut written = 0;
    for outcome in evaluation_store.list_current_outcomes()? {
        let decision: DecisionSnapshotV2 = store.read_versioned_json(
            std::path::Path::new(&outcome.decision_ref.relative_path),
            orchestrator_store::FileSchemaKind::DecisionSnapshot,
        )?;
        let MemoryUsageReferenceStatus::Available { document_ref } = decision.memory_usage_ref
        else {
            continue;
        };
        let report: MemoryUsageReportV1 = store.read_versioned_json(
            std::path::Path::new(&document_ref.relative_path),
            orchestrator_store::FileSchemaKind::MemoryUsageReport,
        )?;
        let pattern_ids = report
            .events
            .iter()
            .filter(|event| {
                event
                    .ticker
                    .as_deref()
                    .is_none_or(|ticker| ticker == decision.ticker)
                    && event.application_disposition
                        == Some(orchestrator_core::MemoryApplicationDisposition::Applied)
            })
            .filter_map(|event| event.expanded_pattern_id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if pattern_ids.is_empty() {
            continue;
        }
        let outcome_ref = evaluation_store.outcome_reference(&outcome.outcome_id)?;
        let attribution_id = orchestrator_store::content_hash(&json!({
            "outcome": outcome_ref,
            "memory_usage_report": document_ref,
            "policy": policy_ref,
        }))?;
        let items = pattern_ids
            .into_iter()
            .map(|pattern_id| MemoryAttributionItemV1 {
                pattern_id,
                label: MemoryAttributionLabel::Unverifiable,
                reason: "matured outcome has no controlled counterfactual proving memory causality"
                    .to_owned(),
                usage_event_refs: vec![document_ref.clone()],
            })
            .collect();
        evaluation_store.write_memory_attribution(MemoryAttributionRecordV1 {
            schema_version: orchestrator_core::MEMORY_ATTRIBUTION_SCHEMA_VERSION,
            attribution_id,
            outcome_ref,
            decision_ref: outcome.decision_ref,
            memory_usage_report_ref: document_ref,
            policy_ref: policy_ref.clone(),
            items,
            created_at: chrono::Utc::now().to_rfc3339(),
            content_hash: String::new(),
        })?;
        written += 1;
    }
    Ok(written)
}

fn market_input_from_local_csv(
    ticker: &str,
    market: &MarketInputConfigV1,
) -> Result<MarketSeriesInput> {
    let path = orchestrator_core::technical_csv_path(
        &orchestrator_core::default_technical_csv_dir(),
        ticker,
        &market.interval,
    )
    .with_context(|| format!("unsupported technical interval {:?}", market.interval))?;
    let csv = std::fs::read(&path)
        .with_context(|| format!("market series is unavailable at {}", path.display()))?;
    Ok(MarketSeriesInput {
        ticker: ticker.to_owned(),
        interval: market.interval.clone(),
        provider: market.provider.clone(),
        price_basis: market.price_basis,
        adjustment_policy: market.adjustment_policy,
        corporate_action_capability: market.corporate_action_capability.clone(),
        csv,
    })
}

#[derive(Debug, Clone)]
struct CapturedSeries {
    input: MarketSeriesInput,
    provenance: TechnicalSeriesProvenanceV1,
    rows: Vec<orchestrator_core::TechnicalCsvRow>,
}

/// Materialize exactly one Decision. Ordinary availability problems become
/// gaps and leave the caller free to evaluate the remaining Decisions.
/// Corrupt CSV or a Store validation failure is returned as an error so the
/// caller can fail closed without publishing a partial authoritative Outcome.
pub fn materialize_decision(
    evaluation_store: &EvaluationStore,
    decision_reader: &EvaluationStore,
    evaluation_run: &RunLocation,
    decision_location: &RunLocation,
    decision: &DecisionSnapshotV2,
    policy: &MaterializerPolicyV1,
    inputs: Vec<MarketSeriesInput>,
) -> Result<MaterializationResultV1> {
    let evaluation_key = evaluation_key(decision)?;
    let decision_ref =
        decision_reader.decision_reference(decision_location, &decision.decision_id)?;
    let benchmark = match &decision.evaluation_spec.benchmark_selection {
        BenchmarkSelectionV1::Configured { binding } => binding,
        BenchmarkSelectionV1::Missing { .. } => {
            return write_gap(
                evaluation_store,
                &evaluation_key,
                &decision_ref,
                MaterializationGapReason::MissingBenchmark,
                "Decision was created without a strict benchmark binding".to_owned(),
                &policy.materialization_policy_ref,
            )
        }
    };

    let mut captured = BTreeMap::new();
    for input in inputs {
        let series = capture_series(evaluation_store, input)?;
        if captured
            .insert(series.input.ticker.to_ascii_uppercase(), series)
            .is_some()
        {
            anyhow::bail!("materializer received duplicate ticker input");
        }
    }
    let Some(asset) = captured.get(&decision.ticker.to_ascii_uppercase()) else {
        return write_gap(
            evaluation_store,
            &evaluation_key,
            &decision_ref,
            MaterializationGapReason::MarketDataUnavailable,
            format!("missing market series for {}", decision.ticker),
            &policy.materialization_policy_ref,
        );
    };
    let Some(benchmark_series) = captured.get(&benchmark.benchmark_id.to_ascii_uppercase()) else {
        return write_gap(
            evaluation_store,
            &evaluation_key,
            &decision_ref,
            MaterializationGapReason::MarketDataUnavailable,
            format!(
                "missing market series for benchmark {}",
                benchmark.benchmark_id
            ),
            &policy.materialization_policy_ref,
        );
    };
    if asset.input.price_basis != decision.evaluation_spec.price_basis
        || benchmark_series.input.price_basis != benchmark.price_basis
        || benchmark_series.input.provider != benchmark.provider
    {
        return write_gap(
            evaluation_store,
            &evaluation_key,
            &decision_ref,
            MaterializationGapReason::DataIncomplete,
            "input price basis or provider differs from frozen Decision policy".to_owned(),
            &policy.materialization_policy_ref,
        );
    }
    if !corporate_actions_resolved(asset) || !corporate_actions_resolved(benchmark_series) {
        return write_gap(
            evaluation_store,
            &evaluation_key,
            &decision_ref,
            MaterializationGapReason::CorporateActionUnresolved,
            "provider capability and price basis cannot prove corporate-action continuity"
                .to_owned(),
            &policy.materialization_policy_ref,
        );
    }

    let effective_date = decision
        .decided_at
        .get(..10)
        .unwrap_or(&decision.decided_at);
    let asset_prices = orchestrator_core::prices_between(
        &asset.rows,
        effective_date,
        "9999-12-31",
        decision.evaluation_spec.price_basis,
    );
    let horizon = decision.evaluation_spec.horizon_trading_days as usize;
    if asset_prices.len() <= horizon {
        return write_gap(
            evaluation_store,
            &evaluation_key,
            &decision_ref,
            MaterializationGapReason::NotMatured,
            format!(
                "only {} sessions available for a {horizon}-session horizon",
                asset_prices.len()
            ),
            &policy.materialization_policy_ref,
        );
    }
    let (anchor_session, anchor_price) = &asset_prices[0];
    let (exit_session, exit_price) = &asset_prices[horizon];
    let Some((benchmark_anchor_session, benchmark_anchor_price)) =
        orchestrator_core::price_on_or_after(
            &benchmark_series.rows,
            anchor_session,
            benchmark.price_basis,
        )
    else {
        return write_gap(
            evaluation_store,
            &evaluation_key,
            &decision_ref,
            MaterializationGapReason::DataIncomplete,
            "benchmark has no anchor price on the asset anchor session".to_owned(),
            &policy.materialization_policy_ref,
        );
    };
    let Some((benchmark_exit_session, benchmark_exit_price)) = orchestrator_core::price_on_or_after(
        &benchmark_series.rows,
        exit_session,
        benchmark.price_basis,
    ) else {
        return write_gap(
            evaluation_store,
            &evaluation_key,
            &decision_ref,
            MaterializationGapReason::DataIncomplete,
            "benchmark has no exit price on the asset exit session".to_owned(),
            &policy.materialization_policy_ref,
        );
    };
    if benchmark_anchor_session.get(..10) != anchor_session.get(..10)
        || benchmark_exit_session.get(..10) != exit_session.get(..10)
    {
        return write_gap(
            evaluation_store,
            &evaluation_key,
            &decision_ref,
            MaterializationGapReason::DataIncomplete,
            "asset and benchmark sessions do not align exactly".to_owned(),
            &policy.materialization_policy_ref,
        );
    }

    let manifest = write_input_manifest(
        evaluation_store,
        &captured,
        &policy.materialization_policy_ref,
    )?;
    let manifest_ref =
        evaluation_store.evaluation_input_manifest_reference(&manifest.manifest_id)?;
    let asset_return = stable_number(exit_price / anchor_price - 1.0);
    let benchmark_return = stable_number(benchmark_exit_price / benchmark_anchor_price - 1.0);
    let lowest = asset_prices[..=horizon]
        .iter()
        .map(|(_, price)| *price)
        .fold(*anchor_price, f64::min);
    let market = MarketOutcome {
        provider: asset.input.provider.clone(),
        price_basis: asset.input.price_basis,
        adjustment_policy: asset.input.adjustment_policy,
        anchor: PricePoint {
            session: anchor_session.clone(),
            price: *anchor_price,
            source_ref: asset.provenance.input_ref.clone(),
        },
        exit: PricePoint {
            session: exit_session.clone(),
            price: *exit_price,
            source_ref: asset.provenance.input_ref.clone(),
        },
        asset_return,
        max_adverse_excursion: stable_number(lowest / anchor_price - 1.0),
        corporate_action_resolved: true,
    };
    let benchmark_outcome = BenchmarkOutcome {
        benchmark_id: benchmark.benchmark_id.clone(),
        benchmark_policy_ref: benchmark.policy_ref.clone(),
        provider: benchmark_series.input.provider.clone(),
        price_basis: benchmark_series.input.price_basis,
        anchor: PricePoint {
            session: benchmark_anchor_session,
            price: benchmark_anchor_price,
            source_ref: benchmark_series.provenance.input_ref.clone(),
        },
        exit: PricePoint {
            session: benchmark_exit_session.clone(),
            price: benchmark_exit_price,
            source_ref: benchmark_series.provenance.input_ref.clone(),
        },
        benchmark_return,
        excess_return: stable_number(asset_return - benchmark_return),
    };
    // The immutable identity includes every semantic field that will be
    // persisted.  The mutable revision edge and write timestamp are excluded
    // deliberately: they describe publication, not evaluation content.
    let outcome_id = content_hash(&json!({
        "evaluation_key": evaluation_key,
        "decision_ref": decision_ref,
        "ticker": decision.ticker,
        "input_manifest_hash": manifest_ref.content_hash,
        "input_manifest_ref": manifest_ref,
        "market": market,
        "benchmark": benchmark_outcome,
        "allocation": "deferred_to_later_milestone",
        "execution": "no_reliable_order_fill_mapping",
        "materializer_version": MATERIALIZER_VERSION,
        "materialization_policy": policy.materialization_policy_ref,
        "benchmark_policy": decision.evaluation_spec.benchmark_policy_ref,
    }))?;
    let current = evaluation_store.read_current_outcome(&evaluation_key)?;
    let supersedes_outcome_id = current
        .as_ref()
        .filter(|existing| existing.outcome_id != outcome_id)
        .map(|existing| existing.outcome_id.clone());
    let outcome = OutcomeRecordV1 {
        schema_version: OUTCOME_RECORD_SCHEMA_VERSION,
        outcome_id,
        evaluation_key: evaluation_key.clone(),
        supersedes_outcome_id,
        decision_ref,
        ticker: decision.ticker.clone(),
        market: OutcomeSection::Available { value: market },
        benchmark: OutcomeSection::Available {
            value: benchmark_outcome,
        },
        allocation: OutcomeSection::Unavailable {
            reason: OutcomeSectionUnavailableReason::DeferredToLaterMilestone,
        },
        execution: OutcomeSection::Unavailable {
            reason: OutcomeSectionUnavailableReason::NoReliableOrderFillMapping,
        },
        evaluation_input_manifest_ref: manifest_ref,
        materialization_policy_ref: policy.materialization_policy_ref.clone(),
        benchmark_policy_ref: decision.evaluation_spec.benchmark_policy_ref.clone(),
        materializer_version: MATERIALIZER_VERSION,
        // Deterministic effective timestamp makes retry content-identical.
        created_at: format!(
            "{}T00:00:00Z",
            exit_session.get(..10).unwrap_or(exit_session)
        ),
        content_hash: String::new(),
    };
    let receipt = evaluation_store.publish_outcome(
        evaluation_run,
        outcome.clone(),
        current
            .map(|_| OutcomeRevisionReason::MarketDataRevision)
            .unwrap_or(OutcomeRevisionReason::InitialMaterialization),
    )?;
    Ok(MaterializationResultV1 {
        decision_id: decision.decision_id.clone(),
        evaluation_key: Some(evaluation_key),
        kind: MaterializationResultKind::Materialized,
        document_ref: Some(DocumentRef {
            document_id: receipt.outcome_id,
            relative_path: receipt.outcome_ref.relative_path,
            content_hash: receipt.outcome_ref.content_hash,
        }),
    })
}

fn capture_series(
    evaluation_store: &EvaluationStore,
    input: MarketSeriesInput,
) -> Result<CapturedSeries> {
    let input_ref = evaluation_store.write_evaluation_input_payload(
        &input.ticker,
        &input.interval,
        &input.csv,
    )?;
    let rows = orchestrator_core::parse_technical_csv(
        std::str::from_utf8(&input.csv).context("evaluation CSV must be UTF-8")?,
    )?;
    let coverage_start = rows
        .first()
        .map(|row| row.date.clone())
        .context("evaluation CSV has no rows")?;
    let coverage_end = rows
        .last()
        .map(|row| row.date.clone())
        .context("evaluation CSV has no rows")?;
    let provenance = TechnicalSeriesProvenanceV1 {
        schema_version: TECHNICAL_SERIES_PROVENANCE_SCHEMA_VERSION,
        ticker: input.ticker.clone(),
        interval: input.interval.clone(),
        provider: input.provider.clone(),
        feed: None,
        price_basis: input.price_basis,
        adjustment_policy: input.adjustment_policy,
        corporate_action_capability: input.corporate_action_capability.clone(),
        input_ref,
        payload_hash: content_hash_bytes(&input.csv),
        coverage_start,
        coverage_end,
        created_at: "deterministic-from-payload".to_owned(),
        content_hash: String::new(),
    };
    Ok(CapturedSeries {
        input,
        provenance,
        rows,
    })
}

fn write_input_manifest(
    evaluation_store: &EvaluationStore,
    captured: &BTreeMap<String, CapturedSeries>,
    materialization_policy_ref: &PolicyRef,
) -> Result<EvaluationInputManifestV1> {
    let series = captured
        .values()
        .map(|series| series.provenance.clone())
        .collect::<Vec<_>>();
    let manifest_id = content_hash(&json!({
        "series": series,
        "policy": materialization_policy_ref,
        "source_store_fingerprint": evaluation_store.context().source_store_fingerprint,
    }))?;
    evaluation_store
        .write_evaluation_input_manifest(EvaluationInputManifestV1 {
            schema_version: EVALUATION_INPUT_MANIFEST_SCHEMA_VERSION,
            manifest_id,
            run_purpose: evaluation_store.context().run_purpose,
            source_store_fingerprint: evaluation_store.context().source_store_fingerprint.clone(),
            series,
            materialization_policy_ref: materialization_policy_ref.clone(),
            // Deterministic: the manifest identity is entirely its captured data.
            created_at: "deterministic-from-payload".to_owned(),
            content_hash: String::new(),
        })
        .map_err(Into::into)
}

fn corporate_actions_resolved(series: &CapturedSeries) -> bool {
    series.input.price_basis == PriceBasis::AdjustedClose
        && series.input.adjustment_policy == AdjustmentPolicy::All
        && matches!(
            series.input.corporate_action_capability,
            CorporateActionCapability::ProviderAdjusted
                | CorporateActionCapability::ExternalMetadata
        )
}

fn stable_number(value: f64) -> f64 {
    // Provider prices are f64, but an IEEE rounding tail must not make an
    // otherwise identical materialization hash differently after JSON
    // round-trip. Ten decimal places exceed the data granularity here.
    (value * 10_000_000_000.0).round() / 10_000_000_000.0
}

fn evaluation_key(decision: &DecisionSnapshotV2) -> Result<String> {
    content_hash(&json!({
        "decision_id": decision.decision_id,
        "source_run_id": decision.source_run_id,
        "ticker": decision.ticker,
        "evaluation_contract_id": decision.evaluation_spec.evaluation_contract_id,
        "horizon": decision.evaluation_spec.horizon_trading_days,
    }))
    .map_err(Into::into)
}

fn write_gap(
    evaluation_store: &EvaluationStore,
    evaluation_key: &str,
    decision_ref: &DocumentRef,
    reason: MaterializationGapReason,
    detail: String,
    policy_ref: &PolicyRef,
) -> Result<MaterializationResultV1> {
    let gap_id = content_hash(&json!({
        "evaluation_key": evaluation_key,
        "decision": decision_ref.content_hash,
        "reason": reason,
        "detail": detail,
        "policy": policy_ref,
    }))?;
    let gap = evaluation_store.write_gap(MaterializationGapV1 {
        schema_version: MATERIALIZATION_GAP_SCHEMA_VERSION,
        gap_id: gap_id.clone(),
        evaluation_key: evaluation_key.to_owned(),
        decision_ref: decision_ref.clone(),
        reason,
        detail,
        policy_ref: policy_ref.clone(),
        created_at: "deterministic-from-policy-and-input".to_owned(),
        content_hash: String::new(),
    })?;
    let reference = evaluation_store.gap_reference(evaluation_key, &gap.gap_id)?;
    Ok(MaterializationResultV1 {
        decision_id: decision_ref.document_id.clone(),
        evaluation_key: Some(evaluation_key.to_owned()),
        kind: MaterializationResultKind::Gap,
        document_ref: Some(reference),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{
        BenchmarkBindingV1, DecisionSection, EvaluationSpec, MemoryUsageReferenceStatus,
        PersistenceContextV1, PersistenceNamespace, RunPurpose,
    };
    use orchestrator_store::{FileStore, FileStoreOptions};
    use tempfile::tempdir;

    fn policy() -> PolicyRef {
        PolicyRef {
            policy_id: "evaluation-policy".into(),
            version: 1,
            content_hash: "sha256:policy".into(),
        }
    }

    fn evaluation_store(root: &std::path::Path) -> EvaluationStore {
        EvaluationStore::open(
            FileStore::open(root, FileStoreOptions::default()).unwrap(),
            PersistenceContextV1 {
                run_purpose: RunPurpose::Paper,
                namespace: PersistenceNamespace::Canonical,
                canonical_memory_writes_enabled: true,
                invocation_id: "test".into(),
                config_ref: policy(),
                source_store_fingerprint: "fixture-store".into(),
            },
        )
        .unwrap()
    }

    fn replay_evaluation_store(root: &std::path::Path) -> EvaluationStore {
        EvaluationStore::open(
            FileStore::open(root, FileStoreOptions::default()).unwrap(),
            PersistenceContextV1 {
                run_purpose: RunPurpose::Replay,
                namespace: PersistenceNamespace::Replay {
                    replay_id: "replay-one".into(),
                },
                canonical_memory_writes_enabled: false,
                invocation_id: "replay-one".into(),
                config_ref: policy(),
                source_store_fingerprint: "fixture-store".into(),
            },
        )
        .unwrap()
    }

    fn decision() -> DecisionSnapshotV2 {
        let policy = policy();
        DecisionSnapshotV2 {
            schema_version: orchestrator_core::DECISION_SNAPSHOT_SCHEMA_VERSION,
            decision_id: "decision-one".into(),
            source_run_id: "source-run".into(),
            ticker: "QQQ".into(),
            thesis: DecisionSection::NotApplicable,
            trade: DecisionSection::NotApplicable,
            risk: DecisionSection::NotApplicable,
            allocation: DecisionSection::NotApplicable,
            execution_plan: DecisionSection::NotApplicable,
            evaluation_spec: EvaluationSpec {
                evaluation_contract_id: "contract".into(),
                horizon_trading_days: 2,
                benchmark_policy_ref: policy.clone(),
                benchmark_selection: BenchmarkSelectionV1::Configured {
                    binding: BenchmarkBindingV1 {
                        benchmark_id: "SPY".into(),
                        provider: "fixture".into(),
                        price_basis: PriceBasis::AdjustedClose,
                        policy_ref: policy.clone(),
                    },
                },
                price_basis: PriceBasis::AdjustedClose,
                materialization_policy_ref: policy,
            },
            source_artifact_refs: Vec::new(),
            source_input_refs: Vec::new(),
            memory_usage_ref: MemoryUsageReferenceStatus::NotCaptured,
            run_purpose: RunPurpose::Paper,
            decided_at: "2026-01-01T00:00:00Z".into(),
            content_hash: String::new(),
        }
    }

    fn series(ticker: &str, closes: &[f64]) -> MarketSeriesInput {
        let rows = closes
            .iter()
            .enumerate()
            .map(|(index, close)| format!("2026-01-0{}", index + 1) + &format!(",{close}"))
            .collect::<Vec<_>>()
            .join("\n");
        MarketSeriesInput {
            ticker: ticker.into(),
            interval: "daily".into(),
            provider: "fixture".into(),
            price_basis: PriceBasis::AdjustedClose,
            adjustment_policy: AdjustmentPolicy::All,
            corporate_action_capability: CorporateActionCapability::ProviderAdjusted,
            csv: format!("date,AdjustedClose\n{rows}\n").into_bytes(),
        }
    }

    #[test]
    fn materializes_matured_outcome_idempotently_across_evaluation_runs() {
        let temp = tempdir().unwrap();
        let store = evaluation_store(temp.path());
        let source = RunLocation::new("2026-01-01", "source-run").unwrap();
        let decision = store.write_decision(&source, decision()).unwrap();
        let first = RunLocation::new("2026-01-05", "eval-one").unwrap();
        let policy = MaterializerPolicyV1 {
            materialization_policy_ref: policy(),
        };
        let result = materialize_decision(
            &store,
            &store,
            &first,
            &source,
            &decision,
            &policy,
            vec![
                series("QQQ", &[100.0, 105.0, 110.0]),
                series("SPY", &[200.0, 202.0, 204.0]),
            ],
        )
        .unwrap();
        assert_eq!(result.kind, MaterializationResultKind::Materialized);
        let second = RunLocation::new("2026-01-06", "eval-two").unwrap();
        let repeat = materialize_decision(
            &store,
            &store,
            &second,
            &source,
            &decision,
            &policy,
            vec![
                series("QQQ", &[100.0, 105.0, 110.0]),
                series("SPY", &[200.0, 202.0, 204.0]),
            ],
        )
        .unwrap();
        assert_eq!(repeat.kind, MaterializationResultKind::Materialized);
        let current = store
            .read_current_outcome(&evaluation_key(&decision).unwrap())
            .unwrap()
            .unwrap();
        let OutcomeSection::Available { value: market } = current.market else {
            panic!("materialized outcome must contain market data");
        };
        assert_eq!(market.asset_return, 0.1);
        let raw_store = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        let doctor = orchestrator_store::inspect_store(&raw_store);
        assert!(
            doctor
                .issues
                .iter()
                .all(|issue| issue.code != "evaluation_ledger_invalid"),
            "doctor issues: {:?}",
            doctor.issues
        );
    }

    #[test]
    fn replay_materialization_reads_canonical_decisions_but_writes_only_replay_namespace() {
        let temp = tempdir().unwrap();
        let canonical = evaluation_store(temp.path());
        let replay = replay_evaluation_store(temp.path());
        let source = RunLocation::new("2026-01-01", "source-run").unwrap();
        let decision = canonical.write_decision(&source, decision()).unwrap();
        let evaluation_run = RunLocation::new("2026-01-05", "replay-run").unwrap();
        let result = materialize_decision(
            &replay,
            &canonical,
            &evaluation_run,
            &source,
            &decision,
            &MaterializerPolicyV1 {
                materialization_policy_ref: policy(),
            },
            vec![
                series("QQQ", &[100.0, 105.0, 110.0]),
                series("SPY", &[200.0, 202.0, 204.0]),
            ],
        )
        .unwrap();
        assert_eq!(result.kind, MaterializationResultKind::Materialized);
        let key = evaluation_key(&decision).unwrap();
        assert!(canonical.read_current_outcome(&key).unwrap().is_none());
        assert!(replay.read_current_outcome(&key).unwrap().is_some());
        let raw = FileStore::open(temp.path(), FileStoreOptions::default()).unwrap();
        assert!(!raw
            .exists(std::path::Path::new("knowledge/evaluation/outcomes"))
            .unwrap());
    }
}
