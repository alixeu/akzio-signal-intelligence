impl ContextBroker {
    pub fn materialize_for_agent(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
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
                must_read.push(ContextMustReadDocument {
                    class: class.to_owned(),
                    metadata: metadata.clone(),
                    value: compact_governed_projection(
                        artifact.kind,
                        self.document_value(&artifact)?,
                    ),
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
                "max_tokens": contract.context.max_tokens,
            },
            "read_tools": contract.tool_specs.iter().map(|tool| tool.name.clone()).collect::<Vec<_>>(),
            "output_artifact_kind": contract.output.artifact_kind,
            "budget": contract.budget,
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
            "documents": self.ledger,
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
                "metadata": document.metadata,
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
            let mut slots = Vec::new();
            for asset in Asset::EXECUTABLE {
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
                        serde_json::json!({"asset":asset,"horizon":horizon,"claims":claim_ids,
                        "directional_domains":domains,"verifications":verifications,
                        "missing_support_action":"neutralize_this_slot"}),
                    );
                }
            }
            context.push(
                serde_json::json!({"type":"must_read","class":"coverage_verification_matrix",
                "version":1,"manifest_artifact_id":self.manifest_artifact_id,"slots":slots}),
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
        ArtifactKind::DecisionContext => &["decision_id","run_id","target","material_conflicts","hard_blockers","soft_warnings","portfolio_risk","validity","applied_learning_refs","rejected_learning_refs"],
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
            "learning.outcome_stage" | "evidence.collection_status" | "canary.evidence_snapshot"
        )
    {
        return Some("outcome_stage_facts");
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
