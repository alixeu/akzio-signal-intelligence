// Field-level repair feedback for the structured research protocol. Keep the
// original assessment and result untouched; the model must correct its values.
fn validate_research_deliberation(summary: &akzio_domain::DeliberationSummary) -> ResearchResult<()> {
    let mut errors = Vec::new();
    for (field, scores, texts) in [
        ("alternative_match_ppm", summary.alternative_match_ppm.len(), summary.alternatives.len()),
        ("uncertainty_weight_ppm", summary.uncertainty_weight_ppm.len(), summary.uncertainties.len()),
    ] {
        if scores != texts {
            errors.push(format!("deliberation.{field}: got {scores} scores for {texts} text items; provide exactly {texts} scores"));
        }
    }
    let total: u64 = summary.uncertainty_weight_ppm.iter().map(|v| u64::from(*v)).sum();
    if let Some(expected) = 1_000_000_u32.checked_sub(summary.confidence_ppm) {
        if total != u64::from(expected) {
            errors.push(format!("deliberation.uncertainty_weight_ppm: sum={total}, expected={expected} (1000000-confidence_ppm={})", summary.confidence_ppm));
        }
    }
    if !errors.is_empty() {
        return Err(ResearchError::InvalidOutput(errors.join("; ")));
    }
    summary.validate_model_assessment().map_err(|e| ResearchError::InvalidOutput(e.to_string()))
}

fn bind_synthesis_submission_schema(schema: &mut Value) {
    for field in ["claims", "critiques"] {
        let array = &mut schema["properties"]["result"]["properties"][field];
        let count = array.pointer("/items/properties/artifact_id/enum")
            .and_then(Value::as_array).map_or(0, Vec::len);
        array["minItems"] = json!(count);
        array["maxItems"] = json!(count);
        array["uniqueItems"] = json!(true);
        array["description"] = json!("Retain every selected reference, including unsupported, blocked and neutral horizons. This is provenance closure, not endorsement.");
    }
    let row = &mut schema["properties"]["result"]["properties"]["research_allocation"]["properties"]["allocations"]["items"];
    let mut zero = row.clone();
    zero["properties"]["target_weight_ppm"]["maximum"] = json!(0);
    zero["properties"]["abstention_reason"] = json!({"type":"string","minLength":1});
    let mut invested = row.clone();
    invested["properties"]["target_weight_ppm"]["minimum"] = json!(1);
    invested["properties"]["abstention_reason"] = json!({"type":"null"});
    invested["properties"]["supporting_horizons"]["minItems"] = json!(1);
    invested["properties"]["evidence_refs"]["minItems"] = json!(1);
    *row = json!({"anyOf":[zero, invested]});
}

// Scope is bound from the same selected evidence inspected by the business
// validator. Grouping identical scopes avoids repeating a branch per document.
fn bind_ground_scope_schema(store: &Store, manifest: &ContextManifest, schema: &mut Value, contract_version: u32) -> ResearchResult<()> {
    let Some(items) = schema.pointer_mut("/properties/result/properties/grounds/items") else { return Ok(()); };
    let allowed = items.pointer("/properties/evidence/properties/artifact_id/enum")
        .and_then(Value::as_array).cloned().unwrap_or_default();
    let mut groups = BTreeMap::<Vec<String>, Vec<Value>>::new();
    for selection in &manifest.payload.selections {
        if !allowed.contains(&json!(selection.artifact.artifact_id)) { continue; }
        let artifact = store.artifact(&selection.artifact.artifact_id)?;
        let payload: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        let scope = if contract_version >= 63 && !news_source_verified(&payload) { BTreeSet::new() } else { evidence_asset_scope(&payload)?.unwrap_or_default() };
        let scope = scope.iter()
            .map(|asset| asset.symbol().to_owned()).collect::<Vec<_>>();
        groups.entry(scope).or_default().push(json!(selection.artifact.artifact_id));
    }
    if groups.is_empty() { return Ok(()); }
    *items = scoped_ground_alternatives(items, groups, contract_version >= 63);
    Ok(())
}

fn scoped_ground_alternatives(base: &Value, groups: BTreeMap<Vec<String>, Vec<Value>>, enforce_descriptive: bool) -> Value {
    let branches = groups.into_iter().map(|(scope, ids)| {
        let mut branch = base.clone();
        branch["properties"]["evidence"]["properties"]["artifact_id"]["enum"] = json!(ids);
        branch["properties"]["assets"]["maxItems"] = json!(scope.len());
        if scope.is_empty() && enforce_descriptive {
            branch["properties"]["role"]["enum"] = json!(["descriptive"]);
            branch["properties"]["domain"]["enum"] = json!([null]);
        } else if !scope.is_empty() {
            branch["properties"]["assets"]["items"]["enum"] = json!(scope);
        }
        branch
    }).collect::<Vec<_>>();
    json!({"anyOf":branches})
}

// Timing is Rust-owned metadata. The model never proposes an expiry that a
// later formatting call could change. Scheduled closes come from governed
// Alpaca calendars stored alongside each asset's completed bars.
fn remove_model_timing_fields(schema: &mut Value) {
    if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
        properties.remove("thesis_valid_until");
        properties.remove("expected_holding_period_days");
    }
    if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|key| {
            !matches!(
                key.as_str(),
                Some("thesis_valid_until" | "expected_holding_period_days")
            )
        });
    }
    match schema {
        Value::Object(object) => object.values_mut().for_each(remove_model_timing_fields),
        Value::Array(array) => array.iter_mut().for_each(remove_model_timing_fields),
        _ => {}
    }
}

fn bind_rust_forecast_times(
    store: &Store,
    manifest: &ContextManifest,
    arguments: &mut Value,
    now: DateTime<Utc>,
) -> ResearchResult<()> {
    let mut calendars = BTreeMap::new();
    for selection in &manifest.payload.selections {
        if selection.artifact.kind != ArtifactKind::NormalizedEvidence {
            continue;
        }
        let artifact = store.artifact(&selection.artifact.artifact_id)?;
        let payload: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        let Some(symbol) = payload["resource"]
            .as_str()
            .and_then(|r| r.strip_prefix("bars:"))
            .and_then(|r| r.split(':').next())
        else {
            continue;
        };
        let Some(closes) = payload.pointer("/value/forecast_session_closes") else {
            continue;
        };
        let closes: BTreeMap<String, DateTime<Utc>> = serde_json::from_value(closes.clone())?;
        if let Some(previous) = calendars.insert(symbol.to_owned(), closes.clone()) {
            if previous != closes {
                return Err(ResearchError::InvalidOutput(
                    "conflicting exchange calendars".into(),
                ));
            }
        }
    }
    bind_common_forecast_times(arguments, &calendars, now)
}

fn bind_common_forecast_times(
    arguments: &mut Value,
    calendars: &BTreeMap<String, BTreeMap<String, DateTime<Utc>>>,
    now: DateTime<Utc>,
) -> ResearchResult<()> {
    if Asset::EXECUTABLE
        .iter()
        .any(|asset| !calendars.contains_key(asset.symbol()))
    {
        return Err(ResearchError::InvalidOutput(
            "Rust forecast timing requires all four governed exchange calendars".into(),
        ));
    }
    let first = &calendars[Asset::EXECUTABLE[0].symbol()];
    let common = first
        .iter()
        .filter(|(date, close)| {
            **close > now
                && Asset::EXECUTABLE
                    .iter()
                    .all(|asset| calendars[asset.symbol()].get(*date) == Some(*close))
        })
        .map(|(_, close)| *close)
        .collect::<Vec<_>>();
    let forecasts = arguments
        .pointer_mut("/result/forecasts")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| ResearchError::InvalidOutput("missing structured forecasts".into()))?;
    for forecast in forecasts {
        let horizon: akzio_domain::DecisionHorizon =
            serde_json::from_value(forecast["horizon"].clone())
                .map_err(|e| ResearchError::InvalidOutput(e.to_string()))?;
        let days = horizon.trading_days();
        let expiry = common.get(usize::from(days) - 1).ok_or_else(|| {
            ResearchError::InvalidOutput(
                "governed common-session calendar does not cover forecast horizon".into(),
            )
        })?;
        let thesis = forecast
            .get_mut("thesis")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| ResearchError::InvalidOutput("missing forecast thesis".into()))?;
        if thesis.contains_key("thesis_valid_until")
            || thesis.contains_key("expected_holding_period_days")
        {
            return Err(ResearchError::InvalidOutput(
                "forecast timing fields are Rust-owned and must be omitted from wire submission"
                    .into(),
            ));
        }
        thesis.insert("thesis_valid_until".into(), json!(expiry));
        thesis.insert("expected_holding_period_days".into(), json!(days));
    }
    Ok(())
}

#[cfg(test)]
mod structured_timing_tests {
    use super::*;
    #[test]
    fn scoped_wire_schema_rejects_cross_asset_news_and_keeps_shared_macro() {
        let news = "a".repeat(64);
        let macro_id = "b".repeat(64);
        let descriptive = "c".repeat(64);
        let mut base = evidence_ground_schema();
        bind_reference_schema(&mut base, &[json!({"artifact_id":news,"kind":"normalized_evidence"}),
            json!({"artifact_id":macro_id,"kind":"normalized_evidence"}),
            json!({"artifact_id":descriptive,"kind":"semantic_detail"})]);
        let schema = scoped_ground_alternatives(&base, BTreeMap::from([
            (vec!["QQQ".into()], vec![json!(news)]),
            (vec!["QQQ".into(),"TQQQ".into()], vec![json!(macro_id)]),
            (vec![], vec![json!(descriptive)]),
        ]), true);
        let mut ground = json!({"evidence":{"artifact_id":news},"support":"observed",
            "role":"directional","assets":["QQQ","TQQQ"],"domain":"news_event"});
        assert!(validate_schema_value(&ground, &schema, "$.result.grounds[0]").is_err());
        ground["assets"] = json!(["QQQ"]);
        assert!(validate_schema_value(&ground, &schema, "$").is_ok());
        ground["evidence"]["artifact_id"] = json!(macro_id);
        ground["assets"] = json!(["QQQ","TQQQ"]);
        ground["domain"] = json!("macro");
        assert!(validate_schema_value(&ground, &schema, "$").is_ok());
        ground["evidence"]["artifact_id"] = json!(descriptive);
        assert!(validate_schema_value(&ground, &schema, "$").is_err());
        ground["assets"] = json!([]);
        ground["role"] = json!("descriptive");
        ground["domain"] = Value::Null;
        assert!(validate_schema_value(&ground, &schema, "$").is_ok());
        assert!(validate_schema_value(&ground, &json!({"anyOf":[]}), "$").is_err());
        assert!(validate_schema_value(&ground, &json!({"anyOf":[base],"additionalProperties":false}), "$").is_err());
    }
    #[test]
    fn common_exchange_sessions_own_expiry_and_reject_model_override() {
        let now: DateTime<Utc> = "2026-09-04T21:00:00Z".parse().unwrap();
        // Labor Day is absent, and this includes a shortened trading session.
        let dates = [
            "2026-09-08T20:00:00Z",
            "2026-09-09T20:00:00Z",
            "2026-09-10T17:00:00Z",
            "2026-09-11T20:00:00Z",
            "2026-09-14T20:00:00Z",
        ];
        let calendar = dates
            .iter()
            .map(|d| (d[..10].to_owned(), d.parse().unwrap()))
            .collect::<BTreeMap<_, _>>();
        let calendars = Asset::EXECUTABLE
            .iter()
            .map(|a| (a.symbol().to_owned(), calendar.clone()))
            .collect();
        let mut wire =
            json!({"result":{"forecasts":[{"horizon":"t3","thesis":{"exit_condition":"review"}}]}});
        bind_common_forecast_times(&mut wire, &calendars, now).unwrap();
        assert_eq!(
            wire["result"]["forecasts"][0]["thesis"]["thesis_valid_until"],
            dates[2]
        );
        assert!(bind_common_forecast_times(&mut wire, &calendars, now).is_err());
        let mut missing = calendars.clone();
        missing.remove("SOXL");
        assert!(bind_common_forecast_times(&mut json!({}), &missing, now).is_err());
    }
}

#[cfg(test)]
mod structured_boundary_tests {
    use super::*;
    #[test]
    fn synthesis_wire_requires_full_provenance_and_distinguishes_zero_allocations() {
        let refs = (1..=6).map(|n| json!({"artifact_id":format!("{n:064x}"),
            "kind":if n <= 3 {"claim"} else {"critique"}})).collect::<Vec<_>>();
        let mut schema = deliberation_output_schema(&decision_proposal_output_schema());
        bind_reference_schema(&mut schema, &refs);
        bind_synthesis_submission_schema(&mut schema);
        let claims = &schema["properties"]["result"]["properties"]["claims"];
        let ids = refs[..3].iter().map(|r| json!({"artifact_id":r["artifact_id"]})).collect::<Vec<_>>();
        validate_schema_value(&json!(ids), claims, "$.result.claims").unwrap();
        assert!(validate_schema_value(&json!(&ids[..2]), claims, "$.result.claims").is_err());
        assert!(validate_schema_value(&json!([ids[0],ids[1],ids[1]]), claims, "$.result.claims").is_err());
        let row_schema = &schema["properties"]["result"]["properties"]["research_allocation"]["properties"]["allocations"]["items"];
        let mut row = json!({"asset":"QQQ","target_weight_ppm":0,"supporting_horizons":[],
            "evidence_refs":[],"rationale":"bounded", "abstention_reason":null});
        assert!(validate_schema_value(&row,row_schema,"$.allocation").is_err());
        row["abstention_reason"] = json!("No directional eligibility");
        validate_schema_value(&row,row_schema,"$.allocation").unwrap();
        row["target_weight_ppm"] = json!(100000);
        assert!(validate_schema_value(&row,row_schema,"$.allocation").is_err());
        row["abstention_reason"] = Value::Null;
        row["supporting_horizons"] = json!(["t1"]);
        row["evidence_refs"] = json!([ids[0]]);
        validate_schema_value(&row,row_schema,"$.allocation").unwrap();
    }
    #[test]
    fn research_deliberation_reports_observed_cardinality_and_sum_without_mutation() {
        let mut summary: akzio_domain::DeliberationSummary = serde_json::from_value(json!({
            "selected_path":"bounded bullish", "alternatives":["A","B"],
            "alternative_match_ppm":[180000,120000,70000],
            "uncertainties":["X","Y","Z"], "uncertainty_weight_ppm":[160000,150000,110000],
            "confidence_ppm":610000,"assessment_source":"model_assessed","basis_artifact_ids":[]
        })).unwrap();
        let before = summary.clone();
        let message = validate_research_deliberation(&summary).unwrap_err().to_string();
        assert!(message.contains("got 3 scores for 2 text items"));
        assert!(message.contains("sum=420000, expected=390000"));
        assert_eq!(summary, before);
        summary.alternative_match_ppm.pop();
        summary.uncertainty_weight_ppm = vec![130000,150000,110000];
        validate_research_deliberation(&summary).unwrap();
    }
    #[test]
    fn critic_rejects_other_asset_and_horizon_gaps() {
        let claim: ResearchClaim = serde_json::from_value(json!({
            "schema_version":DOMAIN_SCHEMA_VERSION,"topic":"QQQ", "statement":"QQQ t1 price", "horizon":"t1", "stance":"neutral", "materiality_ppm":500000,"confidence_ppm":500000,
            "grounds":[{"evidence":{"artifact_id":"a".repeat(64),"kind":"normalized_evidence"},"support":"price", "role":"directional", "domain":"price_market_structure","assets":["QQQ"]}], "evidence_gaps":[]
        })).unwrap();
        let mut critique: ResearchCritique = serde_json::from_value(json!({
            "schema_version":DOMAIN_SCHEMA_VERSION,"target":{"artifact_id":"b".repeat(64),"kind":"claim"},"topic":"review", "severity":"high", "blocker":true,"rationale":"gap", "verification_status":"not_enough_information", "grounds":[],"supporting_refs":[],"conflicting_refs":[],
            "evidence_gaps":[{"topic":"other asset", "rationale":"SOXL missing", "impact":"blocks_directional_forecast","assets":["SOXL"],"horizons":["t1"]}]
        })).unwrap();
        assert!(validate_critique_claim_scope(&critique, &claim, akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION).is_err());
        critique.evidence_gaps[0].assets = BTreeSet::from([Asset::Qqq]);
        assert!(validate_critique_claim_scope(&critique, &claim, akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION).is_ok());
        critique.evidence_gaps[0].horizons = BTreeSet::from([akzio_domain::DecisionHorizon::T5]);
        assert!(validate_critique_claim_scope(&critique, &claim, akzio_domain::REVIEWED_RESEARCH_CONTRACT_VERSION).is_err());
    }
    #[test]
    fn wire_schema_has_no_model_owned_dates_and_legacy_schema_is_unchanged() {
        let canonical = deliberation_output_schema(&decision_proposal_output_schema());
        let mut wire = canonical.clone();
        remove_model_timing_fields(&mut wire);
        let path = "/properties/result/properties/forecasts/items/properties/thesis/properties";
        assert!(
            canonical
                .pointer(path)
                .unwrap()
                .get("thesis_valid_until")
                .is_some()
        );
        assert!(
            wire.pointer(path)
                .unwrap()
                .get("thesis_valid_until")
                .is_none()
        );
        assert!(
            wire.pointer(path)
                .unwrap()
                .get("expected_holding_period_days")
                .is_none()
        );
    }
}

fn semantic_changed_paths(before: &Value, after: &Value, path: &str, changes: &mut Vec<String>) {
    if before == after {
        return;
    }
    match (before, after) {
        (Value::Object(left), Value::Object(right)) => {
            for key in left.keys().chain(right.keys()).collect::<BTreeSet<_>>() {
                semantic_changed_paths(
                    left.get(key).unwrap_or(&Value::Null),
                    right.get(key).unwrap_or(&Value::Null),
                    &format!("{path}/{key}"),
                    changes,
                );
            }
        }
        _ => changes.push(path.to_owned()),
    }
}

fn bind_frozen_result(arguments: &mut Value, result: Value) -> ResearchResult<()> {
    if arguments.get("result").is_some() {
        return Err(ResearchError::InvalidOutput(
            "deliberation repair must not submit result".into(),
        ));
    }
    let object = arguments.as_object_mut().ok_or_else(|| {
        ResearchError::InvalidOutput("deliberation repair must be an object".into())
    })?;
    object.insert("result".into(), result);
    Ok(())
}

fn is_deliberation_repair(feedback: &[ModelToolOutput]) -> bool {
    feedback.iter().any(|f| {
        f.output["message"]
            .as_str()
            .is_some_and(|m| m.contains("deliberation"))
    })
}

fn validate_repair_semantics(
    before: &Value,
    after: &Value,
    feedback: &[ModelToolOutput],
) -> ResearchResult<()> {
    if before["result"] != after["result"]
        && feedback.iter().any(|f| {
            f.output["message"]
                .as_str()
                .is_some_and(|m| m.contains("deliberation"))
        })
    {
        return Err(ResearchError::InvalidOutput("deliberation-only repair changed result semantics; original and revision retained, output not accepted".into()));
    }
    Ok(())
}

impl AgentRuntime {
    async fn last_structured_submission(&self, traces: &[ArtifactRef]) -> ResearchResult<Value> {
        let traces = traces.to_vec();
        self.store_executor
            .execute(move |store| {
                let mut latest_deliberation = None;
                for reference in traces
                    .iter()
                    .rev()
                    .filter(|r| r.kind == ArtifactKind::AgentTurn)
                {
                    let artifact = store.artifact(&reference.artifact_id)?;
                    let payload: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                    if let Some(arguments) =
                        payload.pointer("/response/terminal_submission/arguments")
                    {
                        if latest_deliberation.is_none() {
                            latest_deliberation = arguments.get("deliberation").cloned();
                        }
                        if arguments.get("result").is_some() {
                            let mut original = arguments.clone();
                            if let Some(deliberation) = latest_deliberation {
                                original["deliberation"] = deliberation;
                            }
                            return Ok(original);
                        }
                    }
                }
                Err(ResearchError::InvalidOutput(
                    "deliberation repair missing immutable result".into(),
                ))
            })
            .await?
    }

    async fn record_structured_revision(
        &self,
        permit: &TaskWritePermit,
        traces: &[ArtifactRef],
        revision: u16,
        feedback: &[ModelToolOutput],
    ) -> ResearchResult<()> {
        let trace_candidates = traces
            .iter()
            .rev()
            .filter(|r| r.kind == ArtifactKind::AgentTurn)
            .cloned()
            .collect::<Vec<_>>();
        let permit = permit.clone();
        let feedback = feedback.to_vec();
        self.store_executor.execute(move |store| {
            let mut turns = Vec::new();
            for reference in trace_candidates {
                let artifact = store.artifact(&reference.artifact_id)?;
                let payload: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                if payload.pointer("/response/terminal_submission/arguments").is_some() { turns.push(reference); }
                if turns.len() == 2 { break; }
            }
            if turns.len() != 2 { return Err(ResearchError::InvalidOutput("structured revision is missing its immutable prior submission".into())); }
            let read = |r: &ArtifactRef| -> ResearchResult<Value> {
                let artifact = store.artifact(&r.artifact_id)?;
                let payload: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                payload.pointer("/response/terminal_submission/arguments").cloned().ok_or_else(|| ResearchError::InvalidOutput("structured revision has no prior result".into()))
            };
            let before = read(&turns[1])?;
            let mut after = read(&turns[0])?;
            let metadata_only = after.get("result").is_none() && is_deliberation_repair(&feedback);
            if metadata_only { after["result"] = before["result"].clone(); }
            let mut changes = vec![];
            semantic_changed_paths(&before["result"], &after["result"], "/result", &mut changes);
            if store.debug_session(&permit.run_id)?.is_some() {
                store.record_stage_acceptance(&akzio_domain::StageAcceptance {
                    version: 1, run_id: permit.run_id.clone(), task_id: permit.task_id.clone(), attempt_id: permit.attempt_id.clone(),
                    stage: "agent.structured.revision".into(), business_result: "SubmissionRevision".into(), test_result: akzio_domain::AcceptanceResult::Pass,
                    checks: vec![akzio_domain::AcceptanceCheck {
                        check_id: "agent.structured_revision".into(), category: akzio_domain::AcceptanceCategory::Schema,
                        expected: "Every repair has explicit immutable before/after lineage and semantic changes".into(),
                        actual: json!({"revision":revision,"metadata_only":metadata_only,"result_frozen_by_rust":metadata_only,"changed_paths":changes,"before_result_hash":akzio_domain::ContentHash::of_bytes(&serde_json::to_vec(&before["result"])?),"after_result_hash":akzio_domain::ContentHash::of_bytes(&serde_json::to_vec(&after["result"])?),"validation_feedback":feedback}).to_string(),
                        result: akzio_domain::AcceptanceResult::Pass, evidence_refs: turns, message: "Revision recorded before validation; this is not acceptance of changed semantics".into(),
                    }], created_at: Utc::now(),
                })?;
            }
            // If only deliberation was rejected, the proposed result must be
            // byte-for-byte semantic JSON equal. Reject, never silently restore it.
            validate_repair_semantics(&before, &after, &feedback)?;
            Ok(())
        }).await??;
        Ok(())
    }
}

#[cfg(test)]
mod semantic_revision_tests {
    use super::*;
    #[test]
    fn metadata_repair_reuses_exact_result_and_rejects_any_model_replacement() {
        let original = json!({"grounds":[{"assets":["QQQ","SOXL"]}],"stance":"bullish","confidence_ppm":700000});
        let mut metadata = json!({"deliberation":{"confidence_ppm":700000,"uncertainty_weight_ppm":[130000,100000,70000]}});
        bind_frozen_result(&mut metadata, original.clone()).unwrap();
        assert_eq!(metadata["result"], original);
        assert_eq!(
            metadata["deliberation"]["uncertainty_weight_ppm"],
            json!([130000, 100000, 70000])
        );
        assert!(
            bind_frozen_result(
                &mut json!({"result":{"stance":"neutral"},"deliberation":{}}),
                original.clone()
            )
            .is_err()
        );
        assert!(bind_frozen_result(&mut Value::Null, original).is_err());
    }

    #[test]
    fn deliberation_only_repair_cannot_change_formal_result() {
        let before = json!({"result":{"grounds":[{"assets":["QQQ"]}],"stance":"bullish","confidence_ppm":800000},"deliberation":{"basis_artifact_ids":[]}});
        let mut repair = before.clone();
        repair["deliberation"]["basis_artifact_ids"] = json!(["basis"]);
        let feedback = vec![ModelToolOutput {
            call_id: "original".into(),
            output: json!({"message":"deliberation.basis_artifact_ids must not be empty"}),
        }];
        assert!(validate_repair_semantics(&before, &repair, &feedback).is_ok());
        for field in ["grounds", "stance", "confidence_ppm"] {
            let mut drift = repair.clone();
            drift["result"][field] = Value::Null;
            assert!(
                validate_repair_semantics(&before, &drift, &feedback).is_err(),
                "{field} drift must be rejected"
            );
        }
    }

    #[test]
    fn revision_detects_removed_grounds_verdict_blocker_and_numbers() {
        let before = json!({"grounds":[{"assets":["QQQ","SOXL"]}],"stance":"bullish","verification_status":"supported","blocker":false,"confidence_ppm":800000});
        let after = json!({"grounds":[],"stance":"neutral","verification_status":"not_enough_information","blocker":true,"confidence_ppm":500000});
        let mut paths = vec![];
        semantic_changed_paths(&before, &after, "/result", &mut paths);
        assert_eq!(
            paths,
            [
                "/result/blocker",
                "/result/confidence_ppm",
                "/result/grounds",
                "/result/stance",
                "/result/verification_status"
            ]
        );
    }
}
