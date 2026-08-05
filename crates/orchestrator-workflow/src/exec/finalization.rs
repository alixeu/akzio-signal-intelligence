use super::*;

pub(super) fn run_phase8(
    store: &FileStore,
    location: &RunLocation,
    state: &mut Value,
    runtime: &RuntimeConfig,
    config: &Value,
    args: &ExecArgs,
) -> Result<BTreeMap<String, String>> {
    let mut decision_snapshots = BTreeMap::new();
    if runtime.evaluation.enabled && !args.mock {
        let context = evaluation_persistence_context(runtime, config, args, location)?;
        if !matches!(context.namespace, PersistenceNamespace::Canonical)
            || context.canonical_memory_writes_enabled
        {
            let evaluation = EvaluationStore::open(store.clone(), context.clone())?;
            let usage_ledger =
                orchestrator_store::MemoryUsageLedger::new(store.clone(), location.clone());
            let memory_usage_ref = if usage_ledger.read_all()?.is_empty() {
                MemoryUsageReferenceStatus::NotCaptured
            } else {
                MemoryUsageReferenceStatus::Available {
                    document_ref: usage_ledger.publish_report(&Utc::now().to_rfc3339())?,
                }
            };
            for ticker in investable_assets_from_state(state) {
                let decision = decision_snapshot(
                    store,
                    runtime,
                    location,
                    &ticker,
                    &context,
                    memory_usage_ref.clone(),
                )?;
                let decision = evaluation.write_decision(location, decision)?;
                decision_snapshots.insert(ticker, serde_json::to_value(decision)?);
            }
        }
    }
    state["phase8"] = json!({"status": "completed", "archive": "file_store"});
    write_final_decision_indexes(store, location, state, &decision_snapshots)
}

fn write_final_decision_indexes(
    store: &FileStore,
    location: &RunLocation,
    state: &Value,
    decision_snapshots: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, String>> {
    let mut summary_units = BTreeMap::new();
    let created_at = Utc::now().to_rfc3339();
    let unit_key = "phase8:final-decision:aggregate".to_owned();
    let payload = final_decision_payload(state, decision_snapshots);
    let source_payload_hash = content_hash(&payload)?;
    let index_id = derive_summary_index_id(
        &location.run_id,
        8,
        "rust.final_decision",
        None,
        None,
        &unit_key,
        &source_payload_hash,
    );
    let scope = IndexScope {
        kind: IndexKind::PhaseSummary,
        location: Some(location.clone()),
        index_id: index_id.clone(),
        run_id: location.run_id.clone(),
        source_run_id: None,
        source_phase: 8,
        role: "rust.final_decision".to_owned(),
        ticker: None,
        topic_id: None,
        source_payload_hash,
        authoritative_fields: payload
            .as_object()
            .expect("final decision payload is an object")
            .clone(),
        created_at,
    };
    create_index(
        store,
        CreateIndexInput {
            scope: scope.clone(),
            summary: "Final decision".to_owned(),
            confidence: 1.0,
            pattern_key: None,
            applies_to_phases: Vec::new(),
        },
    )?;
    let mut source_refs = state
        .get("allocation_artifact")
        .and_then(|artifact| artifact.get("index_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .into_iter()
        .collect::<Vec<_>>();
    source_refs.extend(
        read_all_indexes(
            store,
            Some(location),
            &IndexQuery {
                kind: Some(IndexKind::PhaseSummary),
                source_phase: Some(6),
                ..Default::default()
            },
        )?
        .into_iter()
        .map(|index| index.index_id),
    );
    source_refs.sort();
    source_refs.dedup();
    append_index_detail(
        store,
        AppendIndexDetailInput {
            scope: scope.clone(),
            section: DetailSection::Execution,
            detail: serde_json::to_string(&payload)?,
            source_refs,
        },
    )?;
    finalize_index(store, &scope)?;
    summary_units.insert(unit_key, index_id);
    Ok(summary_units)
}

pub(super) fn final_decision_payload(
    state: &Value,
    decision_snapshots: &BTreeMap<String, Value>,
) -> Value {
    json!({
        "final_trade_decision": state["final_trade_decision"],
        "allocation_context": state["allocation_context"],
        "portfolio_allocation": state["portfolio_allocation"],
        "allocation_result": state["allocation_result"],
        "account_snapshot": state.get("account_snapshot").cloned().unwrap_or(Value::Null),
        "order_plan": state.get("order_plan").cloned().unwrap_or(Value::Null),
        "execution_report": state.get("execution_report").cloned().unwrap_or(Value::Null),
        "decision_snapshots": decision_snapshots,
        "report_projection": crate::report::builder::report_projection(state),
    })
}

pub(super) fn evaluation_persistence_context(
    runtime: &RuntimeConfig,
    config: &Value,
    args: &ExecArgs,
    location: &RunLocation,
) -> Result<PersistenceContextV1> {
    let run_purpose = if args.mock {
        RunPurpose::Mock
    } else if args.debug {
        RunPurpose::Debug
    } else {
        args.run_purpose
            .map(Into::into)
            .unwrap_or(runtime.evaluation.default_run_purpose)
    };
    let namespace = match run_purpose {
        RunPurpose::Live | RunPurpose::Paper => PersistenceNamespace::Canonical,
        RunPurpose::Debug => PersistenceNamespace::Debug {
            invocation_id: location.run_id.clone(),
        },
        RunPurpose::Mock => PersistenceNamespace::Disabled,
        RunPurpose::Replay => PersistenceNamespace::Replay {
            replay_id: location.run_id.clone(),
        },
        RunPurpose::MigrationFixture => PersistenceNamespace::MigrationFixture {
            fixture_id: location.run_id.clone(),
        },
    };
    let evaluation_config = config_get(config, "orchestrator.evaluation")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let config_hash = content_hash(&evaluation_config)?;
    Ok(PersistenceContextV1 {
        run_purpose,
        namespace,
        canonical_memory_writes_enabled: runtime.evaluation.canonical_memory_writes_enabled,
        invocation_id: location.run_id.clone(),
        config_ref: PolicyRef {
            policy_id: "orchestrator.evaluation".to_owned(),
            version: runtime.evaluation.policy_version,
            content_hash: config_hash.clone(),
        },
        source_store_fingerprint: config_hash,
    })
}

/// Build the persisted evaluation record only from finalized Phase artifacts.
///
/// `state.json` is a convenient runtime projection, but it is not the
/// authoritative evidence boundary for later Outcome/Reflection work: a
/// recovery can rehydrate it and later phases can enrich it.  The Decision
/// therefore points to the sealed P1--P7 Indexes that supplied the observed
/// value for each section.  A missing or malformed section is represented as
/// such instead of silently replacing the entire decision with placeholders.
pub(super) fn decision_snapshot(
    store: &FileStore,
    runtime: &RuntimeConfig,
    location: &RunLocation,
    ticker: &str,
    context: &PersistenceContextV1,
    memory_usage_ref: MemoryUsageReferenceStatus,
) -> Result<DecisionSnapshotV2> {
    let policy = context.config_ref.clone();
    let benchmark_selection = runtime
        .evaluation
        .benchmarks
        .get(&ticker.to_ascii_uppercase())
        .map(|binding| BenchmarkSelectionV1::Configured {
            binding: BenchmarkBindingV1 {
                benchmark_id: binding.ticker.clone(),
                provider: binding.provider.clone(),
                price_basis: binding.price_basis,
                policy_ref: policy.clone(),
            },
        })
        .unwrap_or_else(|| BenchmarkSelectionV1::Missing {
            policy_ref: policy.clone(),
        });
    let decision_id = content_hash(&json!({
        "source_run_id": location.run_id,
        "ticker": ticker,
        "evaluation_contract_id": runtime.evaluation.evaluation_contract_id,
    }))?;

    let source_artifact_refs = finalized_phase_artifact_refs(store, location)?;
    let (source_input_refs, input_snapshot_available) = match finalized_input_refs(store, location)
    {
        Ok(refs) => (refs, true),
        Err(error) => {
            tracing::warn!(
                ticker,
                error = %error,
                "DecisionSnapshot cannot verify the run-local Phase 1 input manifest"
            );
            (Vec::new(), false)
        }
    };
    let phase3 = finalized_phase_index(store, location, 3, "manager.research")?;
    let phase4 = finalized_phase_index(store, location, 4, "trader")?;
    let phase6 = finalized_phase_index(store, location, 6, "portfolio.manager")?;
    let phase7 = finalized_phase_index(store, location, 7, "rust.allocation")?;
    let phase5_refs = finalized_phase_artifact_refs_for_phase(store, location, 5)?
        .into_iter()
        .map(|reference| (reference.document_id.clone(), reference))
        .collect::<BTreeMap<_, _>>();

    let thesis = if input_snapshot_available {
        match phase3.as_ref() {
            Some((index, reference)) => snapshot_section(
                vec![reference.clone()],
                thesis_decision_from_index(
                    index,
                    reference.clone(),
                    ticker,
                    runtime.evaluation.prediction_horizon_trading_days,
                ),
            ),
            None => unavailable_decision_section(
                DecisionSectionUnavailableReason::ArtifactMissing,
                Vec::new(),
            ),
        }
    } else {
        unavailable_decision_section(
            DecisionSectionUnavailableReason::UpstreamDataGap,
            phase3
                .as_ref()
                .map(|(_, reference)| vec![reference.clone()])
                .unwrap_or_default(),
        )
    };
    let trade = match phase4.as_ref() {
        Some((index, reference)) => snapshot_section(
            vec![reference.clone()],
            trade_decision_from_index(index, reference.clone(), ticker),
        ),
        None => unavailable_decision_section(
            DecisionSectionUnavailableReason::ArtifactMissing,
            Vec::new(),
        ),
    };
    let risk = match phase6.as_ref() {
        Some((index, reference)) => {
            let mut source_refs = vec![reference.clone()];
            if let Ok(refs) = cited_phase5_risk_refs(index, ticker, &phase5_refs) {
                source_refs.extend(refs);
            }
            normalize_document_refs(&mut source_refs);
            snapshot_section(
                source_refs,
                risk_decision_from_index(index, reference.clone(), ticker, &phase5_refs),
            )
        }
        None => unavailable_decision_section(
            DecisionSectionUnavailableReason::ArtifactMissing,
            Vec::new(),
        ),
    };
    let allocation = match (phase6.as_ref(), phase7.as_ref()) {
        (Some((phase6_index, phase6_ref)), Some((phase7_index, phase7_ref))) => snapshot_section(
            vec![phase6_ref.clone(), phase7_ref.clone()],
            allocation_decision_from_indexes(
                phase6_index,
                phase7_index,
                phase7_ref.clone(),
                ticker,
            ),
        ),
        _ => unavailable_decision_section(
            DecisionSectionUnavailableReason::ArtifactMissing,
            [phase6.as_ref(), phase7.as_ref()]
                .into_iter()
                .flatten()
                .map(|(_, reference)| reference.clone())
                .collect(),
        ),
    };
    let execution_plan = match (phase4.as_ref(), phase6.as_ref(), phase7.as_ref()) {
        (
            Some((phase4_index, phase4_ref)),
            Some((phase6_index, phase6_ref)),
            Some((phase7_index, phase7_ref)),
        ) => snapshot_section(
            vec![phase4_ref.clone(), phase6_ref.clone(), phase7_ref.clone()],
            execution_plan_from_indexes(
                phase4_index,
                phase6_index,
                phase7_index,
                phase7_ref.clone(),
                ticker,
            ),
        ),
        _ => unavailable_decision_section(
            DecisionSectionUnavailableReason::ArtifactMissing,
            [phase4.as_ref(), phase6.as_ref(), phase7.as_ref()]
                .into_iter()
                .flatten()
                .map(|(_, reference)| reference.clone())
                .collect(),
        ),
    };
    Ok(DecisionSnapshotV2 {
        schema_version: DECISION_SNAPSHOT_SCHEMA_VERSION,
        decision_id,
        source_run_id: location.run_id.clone(),
        ticker: ticker.to_owned(),
        thesis,
        trade,
        risk,
        allocation,
        execution_plan,
        evaluation_spec: EvaluationSpec {
            evaluation_contract_id: runtime.evaluation.evaluation_contract_id.clone(),
            horizon_trading_days: runtime.evaluation.prediction_horizon_trading_days,
            benchmark_policy_ref: policy.clone(),
            benchmark_selection,
            price_basis: runtime.evaluation.price_basis,
            materialization_policy_ref: policy,
        },
        source_artifact_refs,
        source_input_refs,
        memory_usage_ref,
        run_purpose: context.run_purpose,
        decided_at: format!("{}T00:00:00Z", location.current_date),
        content_hash: String::new(),
    })
}

fn finalized_phase_artifact_refs(
    store: &FileStore,
    location: &RunLocation,
) -> Result<Vec<DocumentRef>> {
    let indexes = read_all_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            ..Default::default()
        },
    )?;
    let mut refs = indexes
        .into_iter()
        .filter(|index| (1..=7).contains(&index.source_phase))
        .map(|index| finalized_index_document_ref(store, location, &index))
        .collect::<Result<Vec<_>>>()?;
    normalize_document_refs(&mut refs);
    Ok(refs)
}

fn finalized_phase_artifact_refs_for_phase(
    store: &FileStore,
    location: &RunLocation,
    phase: u8,
) -> Result<Vec<DocumentRef>> {
    let indexes = read_all_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(phase),
            ..Default::default()
        },
    )?;
    let mut refs = indexes
        .into_iter()
        .map(|index| finalized_index_document_ref(store, location, &index))
        .collect::<Result<Vec<_>>>()?;
    normalize_document_refs(&mut refs);
    Ok(refs)
}

pub(super) fn finalized_phase_index(
    store: &FileStore,
    location: &RunLocation,
    phase: u8,
    role: &str,
) -> Result<Option<(Index, DocumentRef)>> {
    let indexes = read_all_indexes(
        store,
        Some(location),
        &IndexQuery {
            kind: Some(IndexKind::PhaseSummary),
            source_phase: Some(phase),
            role: Some(role.to_owned()),
            ..Default::default()
        },
    )?;
    match indexes.len() {
        0 => Ok(None),
        1 => {
            let index = indexes.into_iter().next().expect("one Index is present");
            let reference = finalized_index_document_ref(store, location, &index)?;
            Ok(Some((index, reference)))
        }
        count => bail!(
            "DecisionSnapshot expected one finalized Phase {phase} {role} Index, found {count}"
        ),
    }
}

fn finalized_index_document_ref(
    store: &FileStore,
    location: &RunLocation,
    index: &Index,
) -> Result<DocumentRef> {
    let expanded_relative = location
        .relative_root()
        .join("index")
        .join(format!("phase{}", index.source_phase))
        .join(orchestrator_store::index_path_component(&index.index_id)?)
        .join("index.json");
    let (relative_path, content_hash) = if store.exists(&expanded_relative)? {
        (expanded_relative, index.content_hash.clone())
    } else {
        let archive_relative =
            IndexArchive::relative_path(location, index.source_phase, &index.index_id)?;
        let archive: IndexArchive = store.read_versioned_json(
            &archive_relative,
            FileSchemaKind::Artifact("index_archive".to_owned()),
        )?;
        archive.validate_for_location(location)?;
        if archive.index.index_id != index.index_id {
            bail!(
                "DecisionSnapshot archive identity differs from finalized Index {}",
                index.index_id
            );
        }
        (archive_relative, archive.content_hash)
    };
    Ok(DocumentRef {
        document_id: index.index_id.clone(),
        relative_path: relative_path.to_string_lossy().to_string(),
        content_hash,
    })
}

fn finalized_input_refs(store: &FileStore, location: &RunLocation) -> Result<Vec<DocumentRef>> {
    let manifest = read_input_snapshot_manifest(store, location)?;
    let manifest_relative = orchestrator_store::InputSnapshotManifest::relative_path(location)?;
    let mut refs = vec![DocumentRef {
        document_id: format!("input-manifest:{}", manifest.run_id),
        relative_path: manifest_relative.to_string_lossy().to_string(),
        content_hash: manifest.content_hash,
    }];
    refs.extend(manifest.inputs.into_iter().map(|snapshot| DocumentRef {
        document_id: format!("run-input:{}", snapshot.payload_relative_path),
        relative_path: snapshot.payload_relative_path,
        content_hash: snapshot.source_payload_hash,
    }));
    normalize_document_refs(&mut refs);
    Ok(refs)
}

fn thesis_decision_from_index(
    index: &Index,
    artifact_ref: DocumentRef,
    ticker: &str,
    horizon_trading_days: u32,
) -> Result<ThesisDecision> {
    let fields = indexed_ticker_fields(index, "decisions", ticker)?;
    let rating = snapshot_string(fields, "rating", ticker)?;
    let direction = match rating.as_str() {
        "Buy" | "Overweight" => ForecastDirection::Up,
        "Sell" | "Underweight" => ForecastDirection::Down,
        "Hold" => ForecastDirection::Neutral,
        _ => bail!("Phase 3 rating is invalid for DecisionSnapshot {ticker}: {rating:?}"),
    };
    Ok(ThesisDecision {
        artifact_ref,
        direction,
        probability: snapshot_unit_interval(fields, "long_probability", ticker)?,
        horizon: format!("{horizon_trading_days} trading days"),
        invalidation_conditions: snapshot_string_list(fields, "validation_plan", ticker)?,
    })
}

fn trade_decision_from_index(
    index: &Index,
    artifact_ref: DocumentRef,
    ticker: &str,
) -> Result<TradeDecision> {
    let fields = indexed_ticker_fields(index, "plans", ticker)?;
    let conditions = snapshot_string_list(fields, "execution_conditions", ticker)?;
    Ok(TradeDecision {
        artifact_ref,
        action: snapshot_trade_action(&snapshot_string(fields, "action", ticker)?, ticker)?,
        entry_condition: (!conditions.is_empty()).then(|| conditions.join("; ")),
        position_size_ceiling: Some(snapshot_unit_interval(
            fields,
            "position_size_pct_max",
            ticker,
        )?),
        blockers: snapshot_string_list(fields, "blockers", ticker)?,
    })
}

fn cited_phase5_risk_refs(
    index: &Index,
    ticker: &str,
    phase5_refs: &BTreeMap<String, DocumentRef>,
) -> Result<Vec<DocumentRef>> {
    let fields = indexed_ticker_fields(index, "per_asset", ticker)?;
    let controls = fields
        .get("binding_risk_controls")
        .and_then(Value::as_array)
        .with_context(|| format!("Phase 6 binding_risk_controls are missing for {ticker}"))?;
    // A Phase 6 plan may legitimately preserve the prior plan when all three
    // reviewers found no marginal, independently attributable constraint.
    // That is a real "no new risk control" outcome, not a malformed Decision
    // Snapshot.  Only a non-empty control list must prove its Phase 5 source.
    if controls.is_empty() {
        return Ok(Vec::new());
    }
    let mut refs = Vec::new();
    for control in controls {
        let source_refs = control
            .get("source_refs")
            .and_then(Value::as_array)
            .with_context(|| {
                format!("Phase 6 binding risk control source_refs are missing for {ticker}")
            })?;
        for source_ref in source_refs {
            let source_ref = source_ref
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .with_context(|| {
                    format!("Phase 6 binding risk control source_refs are invalid for {ticker}")
                })?;
            refs.push(phase5_refs.get(source_ref).cloned().with_context(|| {
                format!(
                    "Phase 6 binding risk control for {ticker} references non-finalized Phase 5 Index {source_ref}"
                )
            })?);
        }
    }
    normalize_document_refs(&mut refs);
    if refs.is_empty() {
        bail!("Phase 6 binding risk controls cite no finalized Phase 5 Index for {ticker}")
    }
    Ok(refs)
}

pub(super) fn risk_decision_from_index(
    index: &Index,
    phase6_ref: DocumentRef,
    ticker: &str,
    phase5_refs: &BTreeMap<String, DocumentRef>,
) -> Result<RiskDecision> {
    let fields = indexed_ticker_fields(index, "per_asset", ticker)?;
    let controls = fields
        .get("binding_risk_controls")
        .and_then(Value::as_array)
        .with_context(|| format!("Phase 6 binding_risk_controls are missing for {ticker}"))?;
    let binding_controls = controls
        .iter()
        .map(|control| {
            control
                .get("control")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!("Phase 6 binding risk control text is missing for {ticker}")
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut artifact_refs = vec![phase6_ref];
    if !binding_controls.is_empty() {
        artifact_refs.extend(cited_phase5_risk_refs(index, ticker, phase5_refs)?);
    }
    normalize_document_refs(&mut artifact_refs);
    Ok(RiskDecision {
        artifact_refs,
        direction_constraint: snapshot_string(fields, "direction_constraint", ticker)?,
        max_target_weight: Some(snapshot_unit_interval(fields, "max_target_weight", ticker)?),
        max_weight_delta: Some(snapshot_unit_interval(fields, "max_weight_delta", ticker)?),
        binding_controls,
    })
}

fn allocation_decision_from_indexes(
    phase6_index: &Index,
    phase7_index: &Index,
    phase7_ref: DocumentRef,
    ticker: &str,
) -> Result<AllocationDecision> {
    let phase6_fields = indexed_ticker_fields(phase6_index, "per_asset", ticker)?;
    let weights = phase7_index
        .authoritative_fields
        .get("allocation")
        .and_then(|allocation| allocation.get("weights"))
        .and_then(Value::as_object)
        .with_context(|| "Phase 7 allocation.weights are missing")?;
    let target_weight = weights
        .get(ticker)
        .and_then(|weight| weight.get("weight"))
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .with_context(|| format!("Phase 7 allocation target weight is invalid for {ticker}"))?;
    let cash_weight = weights
        .get("cash_hedge")
        .and_then(|weight| weight.get("weight"))
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value));
    Ok(AllocationDecision {
        artifact_ref: phase7_ref,
        current_weight: Some(snapshot_unit_interval(
            phase6_fields,
            "current_weight",
            ticker,
        )?),
        target_weight: Some(target_weight),
        cash_weight,
        allocation_policy_version: 1,
    })
}

fn execution_plan_from_indexes(
    phase4_index: &Index,
    phase6_index: &Index,
    phase7_index: &Index,
    phase7_ref: DocumentRef,
    ticker: &str,
) -> Result<ExecutionPlan> {
    let phase4_fields = indexed_ticker_fields(phase4_index, "plans", ticker)?;
    let phase6_fields = indexed_ticker_fields(phase6_index, "per_asset", ticker)?;
    let status = match snapshot_string(phase6_fields, "execution_status", ticker)?.as_str() {
        "execute" => ExecutionPlanStatus::Execute,
        "wait" => ExecutionPlanStatus::Wait,
        "downgrade" => ExecutionPlanStatus::Downgrade,
        value => bail!("Phase 6 execution_status is invalid for {ticker}: {value:?}"),
    };
    let order_plan = phase7_index
        .authoritative_fields
        .get("order_plan")
        .and_then(Value::as_object)
        .with_context(|| "Phase 7 order_plan is missing")?;
    let order_for_ticker = order_plan
        .get("orders")
        .and_then(Value::as_array)
        .with_context(|| "Phase 7 order_plan.orders are missing")?
        .iter()
        .any(|order| order.get("symbol").and_then(Value::as_str) == Some(ticker));
    Ok(ExecutionPlan {
        status,
        intended_action: snapshot_trade_action(
            &snapshot_string(phase4_fields, "action", ticker)?,
            ticker,
        )?,
        order_intent_refs: order_for_ticker.then_some(phase7_ref).into_iter().collect(),
        attributable_execution_expected: matches!(status, ExecutionPlanStatus::Execute)
            && order_for_ticker,
    })
}

fn indexed_ticker_fields<'a>(
    index: &'a Index,
    collection: &str,
    ticker: &str,
) -> Result<&'a Map<String, Value>> {
    index
        .authoritative_fields
        .get(collection)
        .and_then(Value::as_object)
        .and_then(|per_ticker| per_ticker.get(ticker))
        .and_then(Value::as_object)
        .with_context(|| {
            format!(
                "Phase {} {} Index is missing {collection}.{ticker}",
                index.source_phase, index.role
            )
        })
}

fn snapshot_string(fields: &Map<String, Value>, field: &str, ticker: &str) -> Result<String> {
    fields
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .with_context(|| format!("DecisionSnapshot {field} is missing for {ticker}"))
}

fn snapshot_string_list(
    fields: &Map<String, Value>,
    field: &str,
    ticker: &str,
) -> Result<Vec<String>> {
    fields
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("DecisionSnapshot {field} is missing for {ticker}"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!("DecisionSnapshot {field} contains an invalid value for {ticker}")
                })
        })
        .collect()
}

fn snapshot_unit_interval(fields: &Map<String, Value>, field: &str, ticker: &str) -> Result<f64> {
    fields
        .get(field)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
        .with_context(|| format!("DecisionSnapshot {field} is invalid for {ticker}"))
}

fn snapshot_trade_action(value: &str, ticker: &str) -> Result<TradeAction> {
    match value {
        "Buy" => Ok(TradeAction::Buy),
        "Sell" => Ok(TradeAction::Sell),
        "Hold" => Ok(TradeAction::Hold),
        _ => bail!("DecisionSnapshot action is invalid for {ticker}: {value:?}"),
    }
}

fn snapshot_section<T>(source_refs: Vec<DocumentRef>, result: Result<T>) -> DecisionSection<T> {
    match result {
        Ok(value) => DecisionSection::Available { value },
        Err(error) => {
            tracing::warn!(error = %error, "DecisionSnapshot section is unavailable");
            unavailable_decision_section(
                if source_refs.is_empty() {
                    DecisionSectionUnavailableReason::ArtifactMissing
                } else {
                    DecisionSectionUnavailableReason::ArtifactValidationFailed
                },
                source_refs,
            )
        }
    }
}

fn unavailable_decision_section<T>(
    reason: DecisionSectionUnavailableReason,
    source_refs: Vec<DocumentRef>,
) -> DecisionSection<T> {
    DecisionSection::Unavailable {
        reason,
        source_refs,
    }
}

fn normalize_document_refs(refs: &mut Vec<DocumentRef>) {
    refs.sort_by(|left, right| {
        (
            left.document_id.as_str(),
            left.relative_path.as_str(),
            left.content_hash.as_str(),
        )
            .cmp(&(
                right.document_id.as_str(),
                right.relative_path.as_str(),
                right.content_hash.as_str(),
            ))
    });
    refs.dedup();
}
