impl ContextBroker {
    pub fn materialize_for_agent_with_budget(
        &self,
        permit: &TaskWritePermit,
        contract: &AgentContract,
        manifest: &ContextManifest,
        effective_budget: &TaskBudget,
        now: DateTime<Utc>,
    ) -> ContextResult<ContextMaterialization> {
        // 将已持久化 Manifest 按当前 Contract/Attempt 的 grant 物化为模型输入。这里
        // 只读取 CAS、生成内存中的 ContextMaterialization，不提交 Agent 结果或任何
        // Decision/Execution/Paper 状态。
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
            // 本次同步物化已验证完整的不可变 CAS 闭包；逐项继续核验 live permit、
            // readable 集合及类型，避免每读一项又解码整份 Manifest 的所有文档。
            let artifact = self.read_from_validated_grant(
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
                // must_read 只放受控 projection；原始 CAS 文档仍由 grant 和显式读取工具
                // 管理。研究角色在新 Contract 下没有读取工具，所以 projection 会附带
                // “原文未开放”的边界提示，而不伪装成完整事实。
                let mut value = compact_governed_projection(
                    artifact.kind,
                    self.document_value(&artifact)?,
                );
                if contract.version >= 65 && matches!(contract.purpose.as_str(), akzio_domain::RESEARCH_ANALYST_RECIPE_ID | akzio_domain::RESEARCH_CRITIC_RECIPE_ID | akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID | akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID) {
                    projection_only_guidance(&mut value);
                }
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
            "read_tools": if contract.version >= 65 && matches!(contract.purpose.as_str(), akzio_domain::RESEARCH_ANALYST_RECIPE_ID | akzio_domain::RESEARCH_CRITIC_RECIPE_ID | akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID | akzio_domain::RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID) {
                Vec::<String>::new()
            } else {
                contract.tool_specs.iter().map(|tool| tool.name.clone()).collect::<Vec<_>>()
            },
            "output_artifact_kind": contract.output.artifact_kind,
            "contract_budget": contract.budget,
            "budget": effective_budget,
            "budget_semantics": "budget is the resolved cumulative Attempt budget; provider context limits remain separate",
        });
        // materialization_identity 绑定 Manifest 输入、grant、task contract 和实际 ledger；
        // 它是模型输入的确定性身份，不是模型接受、研究完成或 Gate 通过的凭证。
        let materialization_identity = content_hash_json(&serde_json::json!({
            "context_manifest_input_hash": manifest.payload.input_hash,
            "read_grant_identity": read_grant_identity,
            "task_contract": task_contract,
            "ledger": ledger,
            "must_read": must_read,
        }))?;
        self.store.validate_task_permit(permit)?;
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
        // 只读取 Claim provenance 中唯一的 producer Manifest，并把它与当前 Manifest
        // 的已选 evidence 做重合比较；返回的是范围说明，不会向当前 grant 追加 Artifact。
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
    // 仅公开当前 Manifest 已经选中的 evidence 及其是否也在 producer 选择中，避免借
    // 生产者 provenance 泄露当前 grant 之外的 document_id。
    let selected = producer.iter().map(|s| &s.artifact).collect::<BTreeSet<_>>();
    serde_json::json!({
        "status":"verified_from_claim_provenance",
        "current_evidence":current.iter()
            .filter(|s| matches!(s.artifact.kind, ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail))
            .map(|s| serde_json::json!({"document_id":s.artifact.artifact_id,
                "selected_by_claim_producer":selected.contains(&s.artifact)})).collect::<Vec<_>>(),
        "interpretation":guidance::PRODUCER_SCOPE
    })
}

impl ContextMaterialization {
    pub fn model_context(&self) -> Vec<Value> {
        // 根据 purpose 生成模型看到的消息列表；Outcome 使用独立的两阶段投影，研究
        // 角色使用 projections，Synthesizer 额外生成 coverage matrix。
        // 返回值仍只是输入视图，不代表下游接受。
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
            "reading_policy": if self.task_contract["read_tools"].as_array().is_some_and(Vec::is_empty) {
                "本次不提供读取工具；使用已授权 projections。省略的细节不表示不存在，无法核验的细节保留为缺口。"
            } else { guidance::READING_POLICY },
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
            // Synthesizer 的 coverage matrix 只从当前 ledger 中已存在的 Claim/Critique
            // 和证据引用生成；缺少完整闭包时提示中性化，不替模型或 DecisionGate 补事实。
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
            // Expose the exact closure already required by Submit validation.
            // Descriptive and neutral grounds still require provenance; this
            // index grants no additional documents or directional support.
            let selected = self
                .ledger
                .iter()
                .map(|d| ArtifactRef {
                    artifact_id: d.document_id.clone(),
                    kind: d.kind,
                })
                .collect::<BTreeSet<_>>();
            let required_proposal_evidence = claims
                .iter()
                .chain(critiques.iter())
                .flat_map(|d| {
                    let grounds = d.value["grounds"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|g| g.get("evidence"));
                    let verification = ["supporting_refs", "conflicting_refs"]
                        .into_iter()
                        .flat_map(|key| d.value[key].as_array().into_iter().flatten());
                    grounds.chain(verification)
                })
                .filter_map(|value| serde_json::from_value::<ArtifactRef>(value.clone()).ok())
                .filter(|reference| {
                    matches!(reference.kind, ArtifactKind::NormalizedEvidence | ArtifactKind::SemanticDetail)
                        && selected.contains(reference)
                })
                .collect::<BTreeSet<_>>();
            let typed_claims = claims.iter().filter_map(|d| {
                serde_json::from_value::<akzio_domain::ResearchClaim>(d.value.clone()).ok()
                    .map(|claim| (ArtifactRef { artifact_id: d.metadata.document_id.clone(), kind: ArtifactKind::Claim }, claim))
            }).collect::<Vec<_>>();
            let typed_critiques = critiques.iter().filter_map(|d| {
                serde_json::from_value::<akzio_domain::ResearchCritique>(d.value.clone()).ok()
            }).collect::<Vec<_>>();
            let horizons = akzio_domain::DecisionHorizon::ALL.into_iter().map(|horizon| {
                let matching = claims.iter().filter(|d| d.value["horizon"] == serde_json::json!(horizon)).collect::<Vec<_>>();
                let claim_ids = matching.iter().map(|d| &d.metadata.document_id).collect::<Vec<_>>();
                let verifications = critiques.iter().filter(|d| matching.iter().any(|c| d.value["target"]["artifact_id"] == serde_json::json!(c.metadata.document_id)))
                    .map(|d| serde_json::json!({"critique":d.metadata.document_id,"target":d.value["target"],"status":d.value["verification_status"],"blocker":d.value["blocker"]})).collect::<Vec<_>>();
                let slots = Asset::EXECUTABLE.into_iter().map(|asset| {
                    let eligible = typed_claims.iter().filter(|(reference, claim)| {
                        akzio_domain::claim_slot_eligible(reference, claim, &typed_critiques, asset, horizon)
                    }).map(|(reference, claim)| serde_json::json!({"claim":reference,"stance":claim.stance})).collect::<Vec<_>>();
                    let domains = matching.iter().flat_map(|d| d.value["grounds"].as_array().into_iter().flatten())
                        .filter(|g| g["role"] == "directional" && g["assets"].as_array().is_some_and(|a| a.contains(&serde_json::json!(asset))))
                        .filter_map(|g| g["domain"].as_str()).collect::<BTreeSet<_>>();
                    serde_json::json!({"asset":asset,"directional_domains":domains,"directional_eligible":!eligible.is_empty(),
                        "eligible_claims":eligible,"reason":if eligible.is_empty() {"no_single_claim_with_verified_price_macro_and_no_slot_blocker"} else {"same_claim_verified"}})
                }).collect::<Vec<_>>();
                serde_json::json!({"horizon":horizon,"claims":claim_ids,"verifications":verifications,"assets":slots})
            }).collect::<Vec<_>>();
            context.push(
                serde_json::json!({"type":"must_read","class":"coverage_verification_matrix",
                "version":3,"manifest_artifact_id":self.manifest_artifact_id,"horizons":horizons,
                "required_proposal_evidence":required_proposal_evidence,
                "evidence_closure_policy":guidance::EVIDENCE_CLOSURE,
                "missing_support_action":"neutralize_each_asset_horizon_without_complete_support"}),
            );
        }
        context
    }

    fn outcome_model_context(&self) -> Vec<Value> {
        // Outcome Worker 仍保留 Manifest/grant 身份和可读文档索引，但只暴露 Outcome
        // 复盘所需字段；Rust 计算的量化结果不会由模型重新生成或写回。
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
            "full_document":guidance::FULL_DOCUMENT
        }),serde_json::json!({"type":"must_read","class":"task_contract","value":self.task_contract})];
        context.extend(self.must_read.iter().map(|d|serde_json::json!({
            "type":"must_read","class":d.class,"document_id":d.metadata.document_id,
            "kind":d.metadata.kind,"value":outcome_review_projection(d.metadata.kind,d.value.clone())
        })));
        context
    }
}

fn outcome_review_projection(kind: ArtifactKind, mut value: Value) -> Value {
    // 按 Artifact kind 做 Outcome 专用的展示压缩：数值/时间字段保持 Rust 产出的值，
    // 只移除超出叙事复盘需要的长路径、分箱和引用细节。
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
    // 将每个窗口的大型路径和 benchmark 细节替换为计数/固定字段；输入是投影副本，
    // 不会修改 Store 中的原始 Outcome。
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
    // Grant 身份只包含授权边界和 Manifest 输入哈希，供 ledger/materialization 做稳定
    // 绑定；它不包含过期时间，因此同一逻辑授权的身份不因时间流逝而伪造新内容。
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
    // 将选择原因和 Artifact kind 映射成模型视图类别；None 表示只进入 metadata ledger，
    // 不是拒绝或删除 Artifact。
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
                | "canary.evidence_snapshot" | "research.supplement.result"
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
        ArtifactKind::DecisionProposal => Some("final_proposal"),
        ArtifactKind::ProposalReview => Some("proposal_review"),
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
    // 这是 UI/模型 metadata 的固定相关性排序值，不参与 Contract 预算、Gate 或选择授权。
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
    // 对模型视图做有界投影：NormalizedEvidence 保留资源、质量、时间和量化字段，
    // 大型数组/叙事被明确压缩；原 Value 只在本地副本上变换，CAS 中的完整证据不变。
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
            compact_fund_holdings(object);
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
                    "scope_rule":guidance::COLLECTION_SCOPE});
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

/// Column name fragments that identify a holdings row's portfolio weight, in
/// precedence order. The three ETF issuers publish different CSV headers for
/// the same concept, and iShares publishes both a percent weight and a notional
/// value; the explicit weight ranks the position, so it is preferred.
const HOLDINGS_WEIGHT_COLUMNS: [&str; 5] = [
    "weight",
    "holdingspercent",
    "percentageoftotalnetassets",
    "exposure value",
    "notional value",
];

/// Bound an official ETF holdings table to its largest positions in columnar
/// form. Constituent tables reach 127 rows of mostly-empty issuer CSV columns,
/// which crowds out directional price, macro and news evidence without adding
/// decision-relevant facts. Weights and identifiers stay exact; the complete
/// table remains readable through the original document grant.
fn compact_fund_holdings(object: &mut serde_json::Map<String, Value>) {
    const KEPT_ROWS: usize = 12;
    if object.get("category").and_then(Value::as_str) != Some("fund_holdings") {
        return;
    }
    let Some(data) = object.get_mut("data").and_then(Value::as_object_mut) else {
        return;
    };
    let rows_key = if data.get("rows").is_some_and(Value::is_array) { "rows" } else { "holdings" };
    let Some(rows) = data.get(rows_key).and_then(Value::as_array) else {
        return;
    };
    let original_count = rows.len();
    // Holdings 只在完整表格已识别为基金持仓时按发行方权重截取；找不到权重列时保持
    // 来源顺序并在 rows_projection 中标明未排序，不能从行号推断持仓权重。
    // Every issuer column is retained; only whole rows below the weight cutoff
    // are dropped, so no row is silently reshaped into a different meaning.
    let columns = rows
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|row| row.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    // Honour the fragment precedence above rather than column order, so an
    // issuer publishing both a percent weight and a notional value ranks by the
    // weight it actually declares.
    let weight_column = HOLDINGS_WEIGHT_COLUMNS.iter().find_map(|candidate| {
        columns
            .iter()
            .find(|column| column.to_lowercase().contains(candidate))
    });
    // Rank by the issuer's own weight column when one exists. Source order is
    // not a documented ranking, so an unrecognized schema keeps its first rows
    // and says so rather than inventing an ordering.
    let mut ranked = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let weight = weight_column
                .and_then(|column| row.get(column.as_str()))
                .and_then(|value| match value {
                    Value::String(text) => text.replace([',', '%', '$'], "").trim().parse().ok(),
                    other => finite_number(other),
                })
                .filter(|weight: &f64| weight.is_finite());
            (index, weight, row)
        })
        .collect::<Vec<_>>();
    if weight_column.is_some() {
        ranked.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.0.cmp(&right.0))
        });
    }
    let kept = ranked
        .iter()
        .take(KEPT_ROWS)
        .map(|(_, _, row)| {
            columns
                .iter()
                .map(|column| row.get(column.as_str()).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let omitted = original_count.saturating_sub(kept.len());
    data.insert(
        rows_key.to_owned(),
        serde_json::json!({"columns":columns,"rows":kept}),
    );
    data.insert(
        "rows_projection".to_owned(),
        serde_json::json!({
            "original_count": original_count,
            "retained_count": kept.len(),
            "omitted_count": omitted,
            "ranked_by": weight_column,
            "view": if weight_column.is_some() {
                "largest_positions_by_issuer_weight"
            } else {
                "leading_source_rows_unranked"
            },
            "full_document": "read_document or read_range using the metadata document_id",
        }),
    );
}

/// Build a bounded, deterministic option-chain view. The complete chain stays
/// in its original CAS artifact; this view contains only exact aggregate
/// features, a few bounded examples, and explicit missing-field diagnostics.
/// It is intentionally not a replacement for the source artifact or for news
/// evidence.
fn option_chain_projection(value: Value) -> Value {
    // 遍历完整期权快照只计算有限聚合、缺失项和前两个示例；此处不做交易判断，
    // 也不把截断后的 projection 当作原始 option-chain Artifact。
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
            let quote = object.get("latestQuote").or_else(|| object.get("latest_quote"));
            if quote.and_then(|q| q.get("bp").or_else(|| q.get("bid_price")))
                .or_else(|| object.get("bid"))
                .and_then(finite_number)
                .is_some_and(|value| value >= 0.0)
                && quote.and_then(|q| q.get("ap").or_else(|| q.get("ask_price")))
                    .or_else(|| object.get("ask"))
                    .and_then(finite_number)
                    .is_some_and(|value| value >= 0.0)
            {
                contracts_with_bid_ask += 1;
            }
            if object
                .get("openInterest")
                .or_else(|| object.get("open_interest"))
                .and_then(|v| finite_number(v).or_else(||v.as_str()?.parse::<f64>().ok()))
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
            if samples.len() < 2 {
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
                    "volume", "open_interest_date", "quote_timestamp", "trade_timestamp",
                    "iv_timestamp", "greeks_timestamp", "feed", "derived_fields_temporal_status",
                ] {
                    if let Some(value) = object.get(field).filter(|value| {
                        value.is_number() || value.is_string() || value.is_boolean() || value.is_null()
                    }) {
                        sample.insert(field.to_owned(), value.clone());
                    }
                }
                for field in ["latestQuote", "latestTrade", "greeks"] {
                    if let Some(value) = object.get(field) { sample.insert(field.to_owned(), value.clone()); }
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
    let pagination_complete = chain.get("pagination_complete").and_then(Value::as_bool).unwrap_or(true) && chain
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
        "projection_version": 3,
        "coverage": chain["coverage"], "feed": chain["feed"], "bounds": chain["bounds"],
        "decision_cutoff": chain["decision_cutoff"],
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
        "sample_contracts_omitted": snapshots.map_or(0, serde_json::Map::len).saturating_sub(samples.len()),
        "sample_selection": "first_two_in_contract_order_not_representative_of_distribution",
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
    // 为超大期权 NormalizedEvidence 创建新的 RunScoped SemanticDetail 投影，保留
    // source_artifact 元数据和 source_refs；源 Artifact 不改写，后续授权仍由新 Manifest
    // 决定。若同一 Attempt/Retry/Recovery 已有完全相同投影，则复用该 CAS 对象。
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
    let blob = self.store.stage_json(&projection)?;
    // A new Attempt needs a new grant, not new IDs for identical evidence.
    // Reuse only an exact projection from this task's retry/recovery lineage;
    // changed source bytes, Contract or projection still produce a new object.
    let mut ancestors = BTreeSet::from([permit.attempt_id.clone()]);
    let mut child = permit.attempt_id.clone();
    while let Some(relation) = self.store.attempt_relation(&child)? {
        if relation.run_id != permit.run_id || relation.task_id != permit.task_id
            || !matches!(relation.relation, akzio_domain::AttemptRelationKind::Retry | akzio_domain::AttemptRelationKind::Recovery)
            || !ancestors.insert(relation.parent_attempt_id.clone()) {
            break;
        }
        child = relation.parent_attempt_id;
    }
    for existing in self.store.artifacts_referencing(&source.artifact_id, Some(ArtifactKind::SemanticDetail))? {
        if existing.producer == "evidence.option_projection"
            && existing.lifecycle == ArtifactLifecycle::RunScoped
            && existing.blob == blob
            && existing.source_refs == vec![source_ref.clone()]
            && existing.provenance.producer_contract_hash.as_ref() == Some(&contract.contract_hash)
            && existing.origin.as_ref().is_some_and(|origin| {
                origin.run_id.as_ref() == Some(&permit.run_id)
                    && origin.task_id.as_ref() == Some(&permit.task_id)
                    && origin.contract_hash.as_ref() == Some(&contract.contract_hash)
                    && origin.attempt_id.as_ref().is_some_and(|id| ancestors.contains(id))
            }) {
            // 相同内容只复用已有 immutable Artifact；当前 Attempt 仍会在外层 mint 自己的
            // Manifest/ReadGrant，不因复用而继承旧授权。
            return Ok(existing);
        }
    }
    let artifact = Artifact::new(
        ArtifactKind::SemanticDetail,
        blob,
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
    // 接受 JSON number 或可解析字符串，但拒绝 NaN/无穷，供期权聚合和持仓排序使用；
    // 解析失败只让对应字段缺失，不中止整个投影。
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
        .filter(|value| value.is_finite())
}
fn abbreviate_narrative(value: &mut Value) {
    // 递归压缩超过上限的叙事字符串并保留截断标记；数组/对象结构保持不变，数值不改写。
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
    fn synthesizer_matrix_lists_exact_selected_evidence_closure() {
        let metadata = |name: &str, kind| ContextDocumentMetadata {
            document_id: ArtifactId(content_hash_json(&serde_json::json!(name)).unwrap()),
            kind,
            source: "test".into(),
            observed_at: None,
            published_at: None,
            estimated_tokens: 1,
            relevance: 1,
            reason: "required".into(),
            must_read: true,
            read_grant_identity: content_hash_json(&serde_json::json!("grant")).unwrap(),
        };
        let price = metadata("price", ArtifactKind::NormalizedEvidence);
        let descriptive = metadata("descriptive", ArtifactKind::SemanticDetail);
        let private = metadata("unselected", ArtifactKind::NormalizedEvidence);
        let unrelated = metadata("unrelated", ArtifactKind::NormalizedEvidence);
        let reference = |m: &ContextDocumentMetadata| serde_json::json!({
            "artifact_id": m.document_id, "kind": m.kind
        });
        let claim = ContextMustReadDocument {
            class: "claim".into(),
            metadata: metadata("claim", ArtifactKind::Claim),
            value: serde_json::json!({"horizon":"t5", "grounds":[
                {"evidence":reference(&price),"role":"directional"},
                {"evidence":reference(&descriptive),"role":"descriptive"},
                {"evidence":reference(&private),"role":"descriptive"}
            ]}),
        };
        let critique = ContextMustReadDocument {
            class: "critique".into(),
            metadata: metadata("critique", ArtifactKind::Critique),
            value: serde_json::json!({"grounds":[{"evidence":reference(&price)}],
                "supporting_refs":[reference(&descriptive)],"conflicting_refs":[reference(&price)]}),
        };
        let hash = content_hash_json(&serde_json::json!("manifest")).unwrap();
        let materialization = ContextMaterialization {
            manifest_artifact_id: ArtifactId(hash.clone()),
            read_grant_identity: hash.clone(),
            materialization_identity: hash,
            task_contract: serde_json::json!({"purpose":RESEARCH_SYNTHESIZER_RECIPE_ID}),
            ledger: vec![price.clone(), descriptive.clone(), unrelated],
            must_read: vec![claim, critique],
        };
        let view = materialization.model_context();
        let refs = view.last().unwrap()["required_proposal_evidence"].as_array().unwrap();
        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&reference(&price)));
        assert!(refs.contains(&reference(&descriptive)));
        assert!(!refs.contains(&reference(&private)));
    }
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
        assert!(projection["scope_rule"].as_str().unwrap().contains("不是 ReadGrant"));
        assert_eq!(v["requirements"][0]["need"]["artifact_id"],"long-id");
    }
}

#[cfg(test)]
mod fund_holdings_projection_tests {
    use super::*;

    #[test]
    fn invesco_json_holdings_are_bounded_and_ranked_by_exact_weight() {
        let rows = (0..106).map(|i| serde_json::json!({
            "ticker": format!("S{i}"), "percentageOfTotalNetAssets": i as f64 / 10.0,
            "currency":"USD", "units":100 + i
        })).collect::<Vec<_>>();
        let original = serde_json::json!({"resource":"research:etf_holdings:QQQ:2026-09-22",
            "value":{"category":"fund_holdings","data":{"holdings":rows,"totalNumberOfHoldings":106}}});
        let projected = compact_governed_projection(ArtifactKind::NormalizedEvidence, original);
        let data = &projected["value_summary"]["data"];
        assert_eq!(data["holdings"]["rows"].as_array().unwrap().len(), 12);
        assert_eq!(data["rows_projection"]["omitted_count"], 94);
        assert_eq!(data["rows_projection"]["ranked_by"], "percentageOfTotalNetAssets");
        assert_eq!(data["totalNumberOfHoldings"], 106);
        let columns = data["holdings"]["columns"].as_array().unwrap();
        let weight = columns.iter().position(|c| c == "percentageOfTotalNetAssets").unwrap();
        assert_eq!(data["holdings"]["rows"][0][weight], 10.5);
    }

    fn holdings(rows: Vec<Value>) -> Value {
        serde_json::json!({"source":"news_web","resource":"research:etf_holdings:TQQQ:2026-09-21",
            "time_basis":{},"quality":{},"quant_features":Value::Null,
            "value":{"asset":"TQQQ","category":"fund_holdings","coverage":"complete",
                "data":{"headers":{"redacted":true},"rows":rows}}})
    }

    /// ProShares publishes a notional exposure column; iShares a percent weight.
    /// Both must rank by that column and keep only the largest positions.
    #[test]
    fn issuer_holdings_are_ranked_and_bounded() {
        for (column, small, large) in [
            ("Exposure Value (Notional + G/L)", "1000.00", "10853353164.57"),
            ("Weight (%)", "0.11", "9.37"),
            ("HoldingsPercent", "0.3129085244", "5.6212606909"),
        ] {
            let rows = (0..40)
                .map(|index| {
                    serde_json::json!({
                        "Security Description": format!("HOLDING {index}"),
                        column: if index == 39 { large } else { small },
                    })
                })
                .collect::<Vec<_>>();
            let projection =
                compact_governed_projection(ArtifactKind::NormalizedEvidence, holdings(rows));
            let data = &projection["value_summary"]["data"];
            assert_eq!(data["rows"]["rows"].as_array().unwrap().len(), 12);
            assert_eq!(data["rows_projection"]["original_count"], 40);
            assert_eq!(data["rows_projection"]["omitted_count"], 28);
            assert_eq!(data["rows_projection"]["ranked_by"], column);
            assert_eq!(
                data["rows_projection"]["view"],
                "largest_positions_by_issuer_weight"
            );
            // The largest position survives regardless of its source position,
            // and its exact published value is never rewritten.
            let columns = data["rows"]["columns"].as_array().unwrap();
            let weight_at = columns.iter().position(|name| name == column).unwrap();
            assert_eq!(data["rows"]["rows"][0][weight_at], large);
        }
    }

    /// iShares publishes both a percent weight and a notional value. The
    /// declared weight must rank the rows; column order must not decide it.
    #[test]
    fn declared_weight_outranks_notional_value() {
        let rows = (0..20)
            .map(|index| {
                serde_json::json!({
                    "Name": format!("HOLDING {index}"),
                    "Notional Value": format!("{}.00", 1_000 + index),
                    "Weight (%)": format!("{}.00", 20 - index),
                })
            })
            .collect::<Vec<_>>();
        let projection =
            compact_governed_projection(ArtifactKind::NormalizedEvidence, holdings(rows));
        let data = &projection["value_summary"]["data"];
        assert_eq!(data["rows_projection"]["ranked_by"], "Weight (%)");
        let columns = data["rows"]["columns"].as_array().unwrap();
        let name_at = columns.iter().position(|name| name == "Name").unwrap();
        // Highest declared weight is the lowest notional here, so a notional
        // ranking would have put HOLDING 19 first.
        assert_eq!(data["rows"]["rows"][0][name_at], "HOLDING 0");
    }

    /// An unrecognized issuer schema has no documented ranking, so rows keep
    /// source order and the projection says the view is unranked.
    #[test]
    fn unknown_holdings_schema_keeps_source_order() {
        let rows = (0..20)
            .map(|index| serde_json::json!({"Name": format!("HOLDING {index}")}))
            .collect::<Vec<_>>();
        let projection =
            compact_governed_projection(ArtifactKind::NormalizedEvidence, holdings(rows));
        let data = &projection["value_summary"]["data"];
        assert_eq!(data["rows_projection"]["ranked_by"], Value::Null);
        assert_eq!(
            data["rows_projection"]["view"],
            "leading_source_rows_unranked"
        );
        assert_eq!(data["rows"]["rows"][0][0], "HOLDING 0");
    }

    /// A table already inside the bound is returned whole, so short holdings
    /// tables are not reported as truncated.
    #[test]
    fn short_holdings_table_is_not_reported_as_omitted() {
        let rows = vec![serde_json::json!({"Ticker":"AMD","Weight (%)":"9.37"})];
        let projection =
            compact_governed_projection(ArtifactKind::NormalizedEvidence, holdings(rows));
        let data = &projection["value_summary"]["data"];
        assert_eq!(data["rows_projection"]["omitted_count"], 0);
        assert_eq!(data["rows_projection"]["retained_count"], 1);
    }

    /// Non-holdings evidence must not be reshaped by the holdings bound.
    #[test]
    fn non_holdings_evidence_is_untouched() {
        let value = serde_json::json!({"source":"alpaca","resource":"bars:TQQQ:1d",
            "time_basis":{},"quality":{},"quant_features":Value::Null,
            "value":{"category":"market_bars","data":{"rows":[{"c":1},{"c":2}]}}});
        let projection = compact_governed_projection(ArtifactKind::NormalizedEvidence, value);
        assert_eq!(projection["value_summary"]["data"]["rows"][1]["c"], 2);
        assert!(projection["value_summary"]["data"]["rows_projection"].is_null());
    }
}

fn projection_only_guidance(value: &mut Value) {
    // 仅替换 projection 中已有的 full_document 提示，明确本轮没有原文读取工具；
    // 不新增事实，也不删除投影字段。
    if let Some(object) = value.as_object_mut() {
        if object.contains_key("full_document") {
            object.insert("full_document".into(), serde_json::json!("本轮仅提供此投影，原文未开放；省略项不代表不存在，无法核验的细节保留为不确定性。"));
        }
    }
}

#[cfg(test)]
mod projection_access_tests {
    use super::*;
    #[test]
    fn no_read_projection_preserves_facts_and_removes_tool_advice() {
        let mut value=serde_json::json!({"latest_observations":[{"value":"3.88"}],"full_document":"read_document or read_range using the metadata document_id"});
        projection_only_guidance(&mut value);
        assert_eq!(value["latest_observations"][0]["value"],"3.88");
        assert!(!value["full_document"].as_str().unwrap().contains("read_range"));
        assert!(value["full_document"].as_str().unwrap().contains("未开放"));
    }
}
