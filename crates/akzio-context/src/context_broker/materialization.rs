impl ContextBroker {
    pub fn materialize_for_agent(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextMaterialization> {
        self.materialize_for_agent_with_budget(permit, contract, manifest, &contract.budget, now)
    }

    pub fn materialize_for_agent_with_budget(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        effective_budget: &TaskBudget,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextMaterialization> {
        contract.validate()?;
        if !manifest.grant.matches_permit(permit)
            || manifest.grant.contract_hash != contract.contract_hash
            || manifest.artifact.artifact_id != manifest.grant.manifest_artifact_id
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        self.validate_persisted_grant(permit, contract, &manifest.grant, now)?;

        let read_grant_identity =
            stable_read_grant_identity(&manifest.grant, &manifest.payload.input_hash)?;
        let mut ledger = Vec::with_capacity(manifest.payload.selections.len());
        let mut must_read = Vec::new();
        for selection in &manifest.payload.selections {
            let artifact = self.read(
                permit,
                contract,
                &manifest.grant,
                &selection.artifact.artifact_id,
                now,
            )?;
            if artifact.kind != selection.artifact.kind {
                return Err(ContextError::InvalidManifestClosure);
            }
            let must_read_class = must_read_class(selection, &artifact);
            let metadata = ContextDocumentMetadata {
                document_id: artifact.artifact_id.clone(),
                kind: artifact.kind,
                source: artifact.provenance.source_family.clone(),
                observed_at: artifact.provenance.observed_at,
                published_at: None,
                estimated_tokens: selection.estimated_tokens,
                relevance: context_relevance(artifact.kind),
                reason: selection.reason.clone(),
                must_read: must_read_class.is_some(),
                read_grant_identity: read_grant_identity.clone(),
            };
            if let Some(class) = must_read_class {
                let mut value = compact_governed_projection(
                    artifact.kind,
                    self.document_value(&artifact)?,
                );
                if artifact.kind == ArtifactKind::Claim
                    && contract.purpose.as_str() == RESEARCH_CRITIC_RECIPE_ID
                {
                    value["producer_context_scope"] = self.claim_producer_scope(&artifact, manifest)?;
                }
                must_read.push(ContextMustReadDocument {
                    class: class.to_owned(),
                    metadata: metadata.clone(),
                    value,
                });
            }
            ledger.push(metadata);
        }

        let task_contract = serde_json::json!({
            "contract_hash": contract.contract_hash,
            "purpose": contract.purpose,
            "responsibility": contract.responsibility,
            "permitted_context_kinds": contract.context.permitted_kinds,
            "permitted_source_families": contract.context.permitted_source_families,
            "context_limits": {
                "max_artifacts": contract.context.max_artifacts,
                "max_bytes": contract.context.max_bytes,
                "max_source_bytes": contract.context.max_source_bytes,
                "max_tokens": contract.context.max_tokens,
            },
            "read_tools": contract.tool_specs.iter().map(|tool| tool.name.clone()).collect::<Vec<_>>(),
            "output_artifact_kind": contract.output.artifact_kind,
            "contract_budget": contract.budget,
            "budget": effective_budget,
            "budget_semantics": "budget is the resolved cumulative Attempt budget; provider context limits remain separate",
        });
        let materialization_identity = content_hash_json(&serde_json::json!({
            "context_manifest_input_hash": manifest.payload.input_hash,
            "read_grant_identity": read_grant_identity,
            "task_contract": task_contract,
            "ledger": ledger,
            "must_read": must_read,
        }))?;
        Ok(ContextMaterialization {
            manifest_artifact_id: manifest.artifact.artifact_id.clone(),
            read_grant_identity,
            materialization_identity,
            task_contract,
            ledger,
            must_read,
        })
    }

    // Rust reads provenance to describe selection overlap, never to extend the
    // model's grant. Only IDs already selected by the current manifest escape.
    fn claim_producer_scope(
        &self,
        claim: &Artifact,
        current: &ContextManifest,
    ) -> ContextResult<Value> {
        let refs = claim.source_refs.iter()
            .filter(|r| r.kind == ArtifactKind::ContextManifest).collect::<Vec<_>>();
        let [reference] = refs.as_slice() else {
            return Ok(serde_json::json!({"status":"unknown","reason":"no_unique_producer_manifest"}));
        };
        let producer = self.store.artifact(&reference.artifact_id)?;
        let origin = claim.origin.as_ref().ok_or(ContextError::InvalidManifestClosure)?;
        if producer.kind != ArtifactKind::ContextManifest || producer.origin.as_ref() != Some(origin)
            || origin.run_id.is_none() || origin.task_id.is_none() || origin.attempt_id.is_none()
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        let payload: ContextManifestPayload = self.read_payload(&producer)?;
        if Some(&payload.contract_hash) != origin.contract_hash.as_ref()
            || manifest_input_hash(&payload.selections)? != payload.input_hash
        {
            return Err(ContextError::InvalidManifestClosure);
        }
        Ok(producer_selection_overlap(&payload.selections, &current.payload.selections))
    }
}

fn producer_selection_overlap(producer: &[ContextSelection], current: &[ContextSelection]) -> Value {
    let selected = producer.iter().map(|s| &s.artifact).collect::<BTreeSet<_>>();
    serde_json::json!({
        "status":"verified_from_claim_provenance",
        "current_evidence":current.iter()
            .filter(|s| matches!(s.artifact.kind, ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail))
            .map(|s| serde_json::json!({"document_id":s.artifact.artifact_id,
                "selected_by_claim_producer":selected.contains(&s.artifact)})).collect::<Vec<_>>(),
        "interpretation":"False means additional coverage in YOUR context. It does not contradict a Claim saying this evidence was absent from its producer's selected context. Selection is not proof of tool reading; omitted producer-only documents are not disclosed."
    })
}

impl ContextMaterialization {
    pub fn model_context(&self) -> Vec<Value> {
        if self.task_contract.get("purpose").and_then(Value::as_str)
            == Some(akzio_domain::LEARNING_OUTCOME_WORKER_RECIPE_ID)
        {
            return self.outcome_model_context();
        }
        let mut context = Vec::with_capacity(self.must_read.len() + 2);
        context.push(serde_json::json!({
            "type": "context_metadata_ledger",
            "manifest_artifact_id": self.manifest_artifact_id,
            "read_grant_identity": self.read_grant_identity,
            "materialization_identity": self.materialization_identity,
            "projection_version": 3,
            "documents": self.ledger.iter().filter(|d| !d.must_read).collect::<Vec<_>>(),
            "reading_policy": "Required document views below already contain authorized facts. Omitted detail is not absent. Use tools only for a specific unresolved question; tool limits are ceilings, not targets.",
        }));
        context.push(serde_json::json!({
            "type": "must_read",
            "class": "task_contract",
            "value": self.task_contract,
        }));
        context.extend(self.must_read.iter().map(|document| {
            serde_json::json!({
                "type": "must_read",
                "class": document.class,
                "metadata": {"document_id":document.metadata.document_id,"kind":document.metadata.kind,
                    "source":document.metadata.source,"observed_at":document.metadata.observed_at},
                "value": document.value,
            })
        }));
        if self.task_contract.get("purpose").and_then(Value::as_str)
            == Some(RESEARCH_SYNTHESIZER_RECIPE_ID)
        {
            let claims = self
                .must_read
                .iter()
                .filter(|d| d.metadata.kind == ArtifactKind::Claim)
                .collect::<Vec<_>>();
            let critiques = self
                .must_read
                .iter()
                .filter(|d| d.metadata.kind == ArtifactKind::Critique)
                .collect::<Vec<_>>();
            let mut horizons = Vec::new();
            for horizon in ["t1", "t3", "t5"] {
                    let matching = claims
                        .iter()
                        .filter(|d| d.value.get("horizon").and_then(Value::as_str) == Some(horizon))
                        .collect::<Vec<_>>();
                    let claim_ids = matching
                        .iter()
                        .map(|d| &d.metadata.document_id)
                        .collect::<Vec<_>>();
                    let verifications = critiques.iter().filter(|d| matching.iter().any(|c|
                        d.value.pointer("/target/artifact_id") == Some(&serde_json::to_value(&c.metadata.document_id).unwrap_or(Value::Null))))
                        .map(|d| serde_json::json!({"critique":d.metadata.document_id,"target":d.value["target"],
                            "status":d.value["verification_status"],"blocker":d.value["blocker"]})).collect::<Vec<_>>();
                    let mut slots = Vec::new();
                    for asset in Asset::EXECUTABLE {
                    let domains = matching
                        .iter()
                        .flat_map(|d| {
                            d.value
                                .get("grounds")
                                .and_then(Value::as_array)
                                .into_iter()
                                .flatten()
                        })
                        .filter(|g| {
                            g.get("role").and_then(Value::as_str) == Some("directional")
                                && g.get("assets").and_then(Value::as_array).is_some_and(|a| {
                                    a.iter().any(|a| a.as_str() == Some(asset.symbol()))
                                })
                        })
                        .filter_map(|g| g.get("domain").and_then(Value::as_str))
                        .collect::<BTreeSet<_>>();
                    slots.push(
                        serde_json::json!({"asset":asset,"directional_domains":domains}),
                    );
                }
                horizons.push(serde_json::json!({"horizon":horizon,"claims":claim_ids,
                    "verifications":verifications,"assets":slots}));
            }
            context.push(
                serde_json::json!({"type":"must_read","class":"coverage_verification_matrix",
                "version":2,"manifest_artifact_id":self.manifest_artifact_id,"horizons":horizons,
                "missing_support_action":"neutralize_each_asset_horizon_without_complete_support"}),
            );
        }
        context
    }

    fn outcome_model_context(&self) -> Vec<Value> {
        // Keep the original manifest/grant and complete documents intact. The
        // model receives a purpose-specific view, with IDs to read details on
        // demand, rather than duplicated policy traces and evidence text.
        let mut context = vec![serde_json::json!({
            "type":"context_metadata_ledger", "projection_version":2,
            "manifest_artifact_id":self.manifest_artifact_id,
            "read_grant_identity":self.read_grant_identity,
            "materialization_identity":self.materialization_identity,
            "documents":self.ledger.iter().filter(|d|!d.must_read).map(|d|serde_json::json!({
                "document_id":d.document_id,"kind":d.kind,"source":d.source,
                "observed_at":d.observed_at,"must_read":d.must_read
            })).collect::<Vec<_>>(),
            "full_document":"Each must_read item also identifies a granted document; documents lists optional grants. Views omit detailed grounds and policy traces. Use read_document/read_range with document_id for the original authorized document. Omitted fields are not empty or verified."
        }),serde_json::json!({"type":"must_read","class":"task_contract","value":self.task_contract})];
        context.extend(self.must_read.iter().map(|d|serde_json::json!({
            "type":"must_read","class":d.class,"document_id":d.metadata.document_id,
            "kind":d.metadata.kind,"value":outcome_review_projection(d.metadata.kind,d.value.clone())
        })));
        context
    }
}

fn outcome_review_projection(kind: ArtifactKind, mut value: Value) -> Value {
    if kind == ArtifactKind::Decision {
        if let Some(forecasts)=value.get_mut("forecasts") {
            if let Some(rows)=forecasts.as_array() {
                let columns=["asset","horizon","positive_return_probability_ppm","expected_return_ppm","thesis"];
                *forecasts=serde_json::json!({"columns":columns,"rows":rows.iter().map(|row|columns.iter().map(|key|row[*key].clone()).collect::<Vec<_>>()).collect::<Vec<_>>()});
            }
        }
        return value;
    }
    if kind == ArtifactKind::Outcome {
        compact_outcome_numbers(&mut value);
        return value;
    }
    if kind == ArtifactKind::SemanticDetail && value.get("type").and_then(Value::as_str)==Some("outcome_stage_context") {
        if let Some(outcome)=value.get_mut("numeric_outcome") { compact_outcome_numbers(outcome); }
        return value;
    }
    let fields: &[&str] = match kind {
        ArtifactKind::Claim => &["topic","statement","horizon","stance","materiality_ppm","confidence_ppm","evidence_gaps"],
        ArtifactKind::Critique => &["target","topic","severity","blocker","rationale","verification_status","evidence_gaps"],
        ArtifactKind::Retrospective => &["outcome_id","horizon","status","summary","findings","counterfactuals","lesson_candidates","lesson_proposals","diagnostic_gaps"],
        ArtifactKind::DecisionContext => &["decision_id","run_id","target","research_plan","material_conflicts","hard_blockers","soft_warnings","portfolio_risk","validity","applied_learning_refs","rejected_learning_refs"],
        ArtifactKind::ExecutionContext => &["run_id","decision_context","account_snapshot","quote_snapshot","market_clock_snapshot","execution_plan","broker_session","turnover_ppm","factor_exposure","mandate_assessment","final_process_quality","frozen","created_at"],
        _ => return value,
    };
    let Some(object)=value.as_object_mut() else { return value; };
    let detail_counts=["grounds","supporting_refs","conflicting_refs"].into_iter().filter_map(|key|object.get(key).and_then(Value::as_array).map(|rows|(key.to_owned(),rows.len()))).collect::<std::collections::BTreeMap<_,_>>();
    object.retain(|key,_|fields.contains(&key.as_str()));
    object.insert("projection_version".to_owned(),serde_json::json!(2));
    if !detail_counts.is_empty() { object.insert("detail_counts".to_owned(),serde_json::json!(detail_counts)); }
    value
}

fn compact_outcome_numbers(value: &mut Value) {
    let Some(windows)=value.get_mut("windows").and_then(Value::as_array_mut) else { return; };
    for window in windows {
        let Some(window)=window.as_object_mut() else { continue; };
        let count=window.remove("nav_path").and_then(|v|v.as_array().map(Vec::len));
        window.insert("nav_path_detail_count".to_owned(),serde_json::json!(count));
        if let Some(score)=window.get_mut("forecast_score").and_then(Value::as_object_mut) { score.remove("bins"); }
        if let Some(benchmarks)=window.get_mut("benchmark_attributions").and_then(Value::as_array_mut) {
            for benchmark in benchmarks {
                if let Some(object)=benchmark.as_object_mut() { object.remove("definition_hash"); }
                if let Some(result)=benchmark.get_mut("result").and_then(Value::as_object_mut) {
                    result.remove("nav_path");
                }
            }
        }
    }
    if let Some(object)=value.as_object_mut() {
        let count=object.remove("market_evidence").and_then(|v|v.as_array().map(Vec::len));
        object.insert("market_evidence_detail_count".to_owned(),serde_json::json!(count));
        object.insert("projection_version".to_owned(),serde_json::json!(2));
        object.insert("omitted_details".to_owned(),serde_json::json!(["market_evidence refs","nav_path","benchmark result nav_path and definition_hash","forecast_score bins"]));
    }
}

fn stable_read_grant_identity(
    grant: &ReadGrant,
    manifest_input_hash: &ContentHash,
) -> ContextResult<ContentHash> {
    Ok(content_hash_json(&serde_json::json!({
        "manifest_input_hash": manifest_input_hash,
        "run_id": grant.run_id,
        "task_id": grant.task_id,
        "contract_hash": grant.contract_hash,
        "readable": grant.readable,
        "raw_source_closure": grant.raw_source_closure,
    }))?)
}

fn must_read_class(selection: &ContextSelection, artifact: &Artifact) -> Option<&'static str> {
    let reason = selection.reason.trim().to_ascii_lowercase();
    if reason == "must_read"
        || reason == "mandatory_observation"
        || reason.starts_with("must_read:")
        || reason.starts_with("mandatory_observation:")
    {
        return Some("mandatory_observation");
    }
    if artifact.kind == ArtifactKind::SemanticDetail
        && matches!(
            artifact.producer.as_str(),
            "learning.outcome_stage"
                | "evidence.collection_status"
                | "canary.evidence_snapshot"
        )
    {
        return Some("outcome_stage_facts");
    }
    if artifact.kind == ArtifactKind::SemanticDetail
        && artifact.producer == "evidence.option_projection"
    {
        return Some("option_chain_projection");
    }
    match artifact.kind {
        ArtifactKind::NormalizedEvidence => Some("evidence_projection"),
        ArtifactKind::Claim => Some("research_claim"),
        ArtifactKind::Critique => Some("research_verification"),
        ArtifactKind::Decision => Some("original_decision"),
        ArtifactKind::OutcomeSchedule | ArtifactKind::Outcome => Some("outcome_stage_facts"),
        ArtifactKind::Retrospective => Some("prior_retrospective"),
        ArtifactKind::DecisionContext => Some("portfolio"),
        ArtifactKind::ExecutionContext
        | ArtifactKind::ExecutionVerdict
        | ArtifactKind::ExecutionPlan
        | ArtifactKind::ExecutionCommitment
        | ArtifactKind::ExecutionReprice
        | ArtifactKind::PaperLaunchApproval
        | ArtifactKind::FreezeState => Some("risk_execution_constraint"),
        _ => None,
    }
}

const fn context_relevance(kind: ArtifactKind) -> u32 {
    match kind {
        ArtifactKind::DecisionContext
        | ArtifactKind::ExecutionContext
        | ArtifactKind::ExecutionVerdict
        | ArtifactKind::ExecutionPlan
        | ArtifactKind::ExecutionCommitment
        | ArtifactKind::ExecutionReprice
        | ArtifactKind::PaperLaunchApproval
        | ArtifactKind::FreezeState => 1_000_000,
        ArtifactKind::NormalizedEvidence => 950_000,
        ArtifactKind::SemanticDetail => 900_000,
        ArtifactKind::Claim | ArtifactKind::Critique | ArtifactKind::Resolution => 850_000,
        ArtifactKind::Lesson
        | ArtifactKind::Retrospective
        | ArtifactKind::Experience
        | ArtifactKind::CandidatePolicy
        | ArtifactKind::Evaluation => 700_000,
        _ => 500_000,
    }
}

/// Immutable model view. References and numerical values remain exact; long
/// narrative strings are explicitly abbreviated, with full documents readable
/// through their original grant and artifact identity.
fn compact_governed_projection(kind: ArtifactKind, value: Value) -> Value {
    if kind == ArtifactKind::NormalizedEvidence && value.get("resource").is_some() {
        if value
            .get("resource")
            .and_then(Value::as_str)
            .is_some_and(|resource| resource.starts_with("option_chain:"))
        {
            return option_chain_projection(value);
        }
        let mut summary = value.get("value").cloned().unwrap_or(Value::Null);
        if let Some(object) = summary.as_object_mut() {
            for key in ["bars", "observations"] {
                if let Some(rows) = object.get_mut(key).and_then(Value::as_array_mut) {
                    let full_count = rows.len();
                    if full_count > 5 {
                        rows.drain(..full_count - 5);
                    }
                    object.insert(
                        format!("{key}_projection"),
                        serde_json::json!({"original_count":full_count,"view":"latest_five"}),
                    );
                }
            }
            object.remove("session_closes");
        }
        let mut projected = serde_json::json!({"projection_version":1,"source":value["source"],
            "resource":value["resource"],"time_basis":value["time_basis"],"quality":value["quality"],
            "quant_features":value["quant_features"],"value_summary":summary,
            "full_document":"read_document or read_range using the metadata document_id"});
        abbreviate_narrative(&mut projected);
        return projected;
    }
    let mut value = value;
    if kind == ArtifactKind::SemanticDetail
        && value.get("type").and_then(Value::as_str) == Some("evidence_collection_status")
    {
            if let Some(rows) = value.get("requirements").and_then(Value::as_array) {
                let mut grouped = std::collections::BTreeMap::<String, Vec<Value>>::new();
                for row in rows {
                    grouped.entry(row["status"].as_str().unwrap_or("unknown").to_owned()).or_default()
                        .push(serde_json::json!({"resource":row["resource"],"criticality":row["criticality"],"diagnostic":row["diagnostic"]}));
                }
                return serde_json::json!({"type":"evidence_collection_status","projection_version":4,
                    "authority":value["authority"],"missing_directional_evidence":value["missing_directional_evidence"],
                    "collected_resources_by_status":grouped,
                    "scope_rule":"Collection status is not a ReadGrant. Available resources may be absent from this task's selected documents due to context limits. Do not label unselected data unavailable. Need artifact references omitted here remain in the authorized full status document."});
            }
    }
    if matches!(
        kind,
        ArtifactKind::Claim
            | ArtifactKind::Critique
            | ArtifactKind::Decision
            | ArtifactKind::Retrospective
    ) {
        abbreviate_narrative(&mut value);
    }
    value
}

/// Build a bounded, deterministic option-chain view. The complete chain stays
/// in its original CAS artifact; this view contains only exact aggregate
/// features, a few bounded examples, and explicit missing-field diagnostics.
/// It is intentionally not a replacement for the source artifact or for news
/// evidence.
fn option_chain_projection(value: Value) -> Value {
    let chain = value.get("value").cloned().unwrap_or(Value::Null);
    let snapshots = chain.get("snapshots").and_then(Value::as_object);
    let mut available_fields = BTreeSet::new();
    let mut iv_ppm = Vec::new();
    let mut contracts_with_bid_ask = 0_usize;
    let mut contracts_with_open_interest = 0_usize;
    let mut expirations = BTreeSet::new();
    let mut samples = Vec::new();

    if let Some(snapshots) = snapshots {
        for (contract, snapshot) in snapshots {
            let Some(object) = snapshot.as_object() else {
                continue;
            };
            available_fields.extend(object.keys().cloned());
            let implied_volatility = object
                .get("impliedVolatility")
                .or_else(|| object.get("implied_volatility"))
                .and_then(finite_number)
                .filter(|value| *value >= 0.0)
                .map(|value| (value * 1_000_000.0).round() as i64);
            if let Some(value) = implied_volatility {
                iv_ppm.push(value);
            }
            if object
                .get("bid")
                .and_then(finite_number)
                .is_some_and(|value| value >= 0.0)
                && object
                    .get("ask")
                    .and_then(finite_number)
                    .is_some_and(|value| value >= 0.0)
            {
                contracts_with_bid_ask += 1;
            }
            if object
                .get("openInterest")
                .or_else(|| object.get("open_interest"))
                .and_then(finite_number)
                .is_some_and(|value| value >= 0.0)
            {
                contracts_with_open_interest += 1;
            }
            if let Some(expiration) = object
                .get("expirationDate")
                .or_else(|| object.get("expiration_date"))
                .and_then(Value::as_str)
            {
                expirations.insert(expiration.to_owned());
            }
            if samples.len() < 8 {
                let mut sample = serde_json::Map::new();
                sample.insert("contract".to_owned(), Value::String(contract.clone()));
                for field in [
                    "bid",
                    "ask",
                    "impliedVolatility",
                    "implied_volatility",
                    "delta",
                    "gamma",
                    "theta",
                    "vega",
                    "openInterest",
                    "open_interest",
                    "volume",
                ] {
                    if let Some(value) = object.get(field).filter(|value| {
                        value.is_number() || value.is_string() || value.is_boolean()
                    }) {
                        sample.insert(field.to_owned(), value.clone());
                    }
                }
                samples.push(Value::Object(sample));
            }
        }
    }

    let mut missing_items = Vec::new();
    if snapshots.is_none() {
        missing_items.push("snapshots".to_owned());
    }
    if iv_ppm.is_empty() {
        missing_items.push("implied_volatility".to_owned());
    }
    if contracts_with_bid_ask == 0 {
        missing_items.push("bid_ask".to_owned());
    }
    if contracts_with_open_interest == 0 {
        missing_items.push("open_interest".to_owned());
    }
    let pagination_complete = chain
        .get("next_page_token")
        .is_none_or(|token| token.is_null() || token.as_str().is_some_and(str::is_empty));
    if !pagination_complete {
        missing_items.push("pagination_complete".to_owned());
    }

    let iv_stats = if iv_ppm.is_empty() {
        serde_json::json!({
            "min_ppm": null,
            "max_ppm": null,
            "mean_ppm": null,
        })
    } else {
        let min = iv_ppm.iter().copied().min().unwrap_or_default();
        let max = iv_ppm.iter().copied().max().unwrap_or_default();
        let mean = iv_ppm.iter().sum::<i64>() / i64::try_from(iv_ppm.len()).unwrap_or(1);
        serde_json::json!({"min_ppm":min,"max_ppm":max,"mean_ppm":mean})
    };
    let expiration_dates = expirations.iter().take(32).cloned().collect::<Vec<_>>();
    let available_fields = available_fields
        .iter()
        .take(64)
        .cloned()
        .collect::<Vec<_>>();
    serde_json::json!({
        "type": "option_chain_projection",
        "projection_version": 1,
        "source": value["source"],
        "resource": value["resource"],
        "need": value["need"],
        "observed_at": value["observed_at"],
        "time_basis": value["time_basis"],
        "quality": value["quality"],
        "provenance": value["provenance"],
        "features": {
            "contracts_total": snapshots.map_or(0, serde_json::Map::len),
            "contracts_with_iv": iv_ppm.len(),
            "contracts_with_bid_ask": contracts_with_bid_ask,
            "contracts_with_open_interest": contracts_with_open_interest,
            "expiration_count": expirations.len(),
            "iv": iv_stats,
            "pagination_complete": pagination_complete,
        },
        "available_fields": available_fields,
        "missing_items": missing_items,
        "expiration_dates": expiration_dates,
        "expiration_dates_truncated": expirations.len() > 32,
        "sample_contracts": samples,
        "full_document": "The complete option chain remains in the original authorized CAS document; this bounded projection is not a substitute for reading a permitted range.",
    })
}

impl ContextBroker {
    /// Materialize the bounded option view as a separate CAS artifact when the
    /// original NormalizedEvidence document is too large for a research
    /// manifest. The source artifact remains immutable and is retained in both
    /// CAS lineage and the explicit `source_artifact` metadata below; no source
    /// byte count is fabricated or silently replaced.
    fn option_projection_artifact(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        source: &Artifact,
        now: DateTime<Utc>,
    ) -> ContextResult<Artifact> {
    let source_ref = ArtifactRef {
        artifact_id: source.artifact_id.clone(),
        kind: source.kind,
    };
    let mut projection = option_chain_projection(self.document_value(source)?);
    projection["source_artifact"] = serde_json::json!({
        "artifact_id": source.artifact_id,
        "kind": source.kind,
        "blob_hash": source.blob.hash,
        "logical_bytes": source.blob.bytes,
        "producer": source.producer,
        "source_refs": source.source_refs,
    });
    let artifact = Artifact::new(
        ArtifactKind::SemanticDetail,
        self.store.stage_json(&projection)?,
        "evidence.option_projection",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.ingest".to_owned(),
            observed_at: source.provenance.observed_at,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: source.provenance.confidence_ppm,
            producer_contract_hash: Some(contract.contract_hash.clone()),
        },
        Some(permit.artifact_origin()),
        vec![source_ref],
        now,
    )?;
    self.store.write_task_artifact(
        permit,
        &artifact,
        LifecycleEventType::ArtifactCommitted,
        now,
    )?;
        Ok(artifact)
    }
}

fn finite_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
        .filter(|value| value.is_finite())
}
fn abbreviate_narrative(value: &mut Value) {
    match value {
        Value::String(s) if s.chars().count() > 600 => {
            *s = format!(
                "{}… [projection truncated; read original document]",
                s.chars().take(600).collect::<String>()
            );
        }
        Value::Array(rows) => {
            for row in rows {
                abbreviate_narrative(row);
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                abbreviate_narrative(value);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod compact_context_tests {
    use super::*;
    #[test]
    fn compact_coverage_preserves_all_asset_horizons_and_exact_claim_verification() {
        let hash = content_hash_json(&serde_json::json!("claim")).unwrap();
        let claim_id = ArtifactId(hash.clone());
        let metadata = ContextDocumentMetadata { document_id: claim_id.clone(), kind: ArtifactKind::Claim,
            source: "research.analyst".into(), observed_at: None, published_at: None, estimated_tokens: 1,
            relevance: 1, reason: "required".into(), must_read: true, read_grant_identity: hash.clone() };
        let claim = ContextMustReadDocument { class: "claim".into(), metadata: metadata.clone(), value: serde_json::json!({
            "horizon":"t3", "grounds":[{"role":"directional","assets":["QQQ"],"domain":"price_market_structure"}]}) };
        let mut critic_metadata = metadata;
        critic_metadata.kind = ArtifactKind::Critique;
        critic_metadata.document_id = ArtifactId(content_hash_json(&serde_json::json!("critique")).unwrap());
        let critic = ContextMustReadDocument { class: "critique".into(), metadata: critic_metadata, value: serde_json::json!({
            "target":{"artifact_id":claim_id,"kind":"claim"},"verification_status":"not_enough_information","blocker":true}) };
        let materialization = ContextMaterialization { manifest_artifact_id: ArtifactId(hash.clone()),
            read_grant_identity: hash.clone(), materialization_identity: hash,
            task_contract: serde_json::json!({"purpose":RESEARCH_SYNTHESIZER_RECIPE_ID}), ledger: vec![], must_read: vec![claim, critic] };
        let view = materialization.model_context();
        let matrix = view.last().unwrap();
        let horizons = matrix["horizons"].as_array().unwrap();
        assert_eq!(horizons.len(), 3);
        assert_eq!(horizons.iter().map(|h| h["assets"].as_array().unwrap().len()).sum::<usize>(), 12);
        assert_eq!(horizons[1]["claims"], serde_json::json!([claim_id]));
        assert_eq!(horizons[1]["verifications"][0]["blocker"], true);
        for slot in horizons[1]["assets"].as_array().unwrap() {
            assert_eq!(slot["directional_domains"].as_array().unwrap().len(), usize::from(slot["asset"] == "QQQ"));
        }
        assert_eq!(horizons[0]["claims"], serde_json::json!([]));
    }
    #[test]
    fn producer_scope_distinguishes_additional_coverage_without_disclosing_ungranted_ids() {
        let selection = |name| ContextSelection {
            artifact: ArtifactRef { artifact_id: ArtifactId(content_hash_json(&serde_json::json!(name)).unwrap()), kind: ArtifactKind::NormalizedEvidence },
            reason: "evidence".into(), estimated_tokens: 1, projected_bytes: None, trust: ContextTrust::UntrustedEvidence,
        };
        let shared = selection("shared");
        let additional = selection("critic_only");
        let private = selection("producer_only");
        let view = producer_selection_overlap(&[shared.clone(), private.clone()], &[shared, additional.clone()]);
        assert_eq!(view["current_evidence"][0]["selected_by_claim_producer"], true);
        assert_eq!(view["current_evidence"][1]["selected_by_claim_producer"], false);
        assert_eq!(view["current_evidence"][1]["document_id"], serde_json::to_value(additional.artifact.artifact_id).unwrap());
        assert!(!view.to_string().contains(&private.artifact.artifact_id.to_string()));
    }
    #[test]
    fn required_document_metadata_and_facts_are_not_duplicated() {
        let hash=content_hash_json(&serde_json::json!("test")).unwrap();
        let metadata=ContextDocumentMetadata {document_id:ArtifactId(hash.clone()),kind:ArtifactKind::NormalizedEvidence,source:"alpaca".into(),observed_at:None,published_at:None,estimated_tokens:100,relevance:1,reason:"must_read".into(),must_read:true,read_grant_identity:hash.clone()};
        let m=ContextMaterialization {manifest_artifact_id:ArtifactId(hash.clone()),read_grant_identity:hash.clone(),materialization_identity:hash,task_contract:serde_json::json!({"purpose":"research.analyst"}),ledger:vec![metadata.clone()],must_read:vec![ContextMustReadDocument {class:"evidence_projection".into(),metadata,value:serde_json::json!({"exact_fact":123})}]};
        let view=m.model_context();
        assert_eq!(view[0]["documents"],serde_json::json!([]));
        assert_eq!(view[2]["value"]["exact_fact"],123);
        assert_eq!(view[2]["metadata"]["document_id"],serde_json::to_value(&m.ledger[0].document_id).unwrap());
        assert_eq!(m.ledger.len(),1);
    }
}

#[cfg(test)]
mod availability_projection_tests {
    use super::*;
    #[test]
    fn collected_availability_is_not_context_selection() {
        let v=serde_json::json!({"type":"evidence_collection_status","authority":"rust","missing_directional_evidence":"neutralize_affected_slots","requirements":[
            {"need":{"artifact_id":"long-id"},"resource":"bars:TQQQ","criticality":"directional_research","status":"available","diagnostic":"none"},
            {"need":{"artifact_id":"other-id"},"resource":"news:TQQQ","criticality":"directional_research","status":"unavailable","diagnostic":"adapter_unavailable"}]});
        let projection=compact_governed_projection(ArtifactKind::SemanticDetail,v.clone());
        assert_eq!(projection["collected_resources_by_status"]["available"][0]["resource"],"bars:TQQQ");
        assert_eq!(projection["collected_resources_by_status"]["unavailable"][0]["resource"],"news:TQQQ");
        assert!(projection["scope_rule"].as_str().unwrap().contains("not a ReadGrant"));
        assert_eq!(v["requirements"][0]["need"]["artifact_id"],"long-id");
    }
}
