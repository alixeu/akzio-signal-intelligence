//! Fixed, synthetic research-quality experiment. Uses the production context,
//! role prompts, schema, validator and SQL/CAS authority; never a broker.

// 质量实验复用正式 Agent/Context/Contract 校验，市场材料与日历是隔离 Store
// 内的 synthetic 数据；offline 用 fixture，baseline/candidate 可调用真实模型，
// 不能把这些模型回答称为合成或正式市场结论。它能检验“协议是否拒绝已知坏样例”，不能证明
// Policy 校准、Paper 下单、成交、T+1/T+3/T+5 Outcome 或业务收益已经发生。
// 文件导读：CLI 的 `debug verify-research-quality` 调用公开入口 `verify`；offline
// 使用本 crate 的确定性 fixture，baseline/candidate 可调用配置的真实 provider，report 只读已有
// Store。`attempt` 为每个 case 建隔离 Run/Task，`persist` 保留 Artifact 血缘，`report`
// 再从 CAS 与 AgentTurn 事件重建指标。建议先读 verify 的阶段分支，再读 case_material
// 与 run_repair；注意异步 Future 由 await 驱动、真实调用先预留额度、各次 Store 写入
// 可留下部分进度。Contract 69 的 Reviewer/Synthesizer 仍直接 Submit，不会因实验
// 引入研究 Draft/读取工具；报告通过不等于正式 Decision、Paper 或 Outcome。
use crate::{
    ActiveResearchCatalogue, AgentModel, AgentModelRequest, AgentModelTurn, AgentRuntime,
    ModelClientAdapter, ResearchError, Result,
};
use akzio_domain::*;
use akzio_model::{ModelCapabilitySnapshot, ModelClient};
use akzio_store::{ClaimedAttempt, Store, StoredRun, TaskWorkload, WorkflowCommit};
use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use serde_json::{json, Value};
use std::path::Path;

const CASE_COUNT: usize = 12;

// 在当前隔离 Attempt 的 permit 下保存一份实验 Artifact：payload 只借用用于序列化，
// refs 被移动进 Artifact；单次调用成功表示该 Artifact 已由 Store 写入，不表示整个 case 完成。
fn persist(
    store: &Store,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    producer: &str,
    payload: &Value,
    refs: Vec<ArtifactRef>,
) -> Result<Artifact> {
    // 质量记录也走 Artifact + task permit；因此测试输出具有可重放血缘，但不会越过
    // 隔离 Debug 的 broker_write_policy=forbidden 边界。
    let now = Utc::now();
    let family = if kind == ArtifactKind::NormalizedEvidence {
        "alpaca"
    } else if matches!(kind, ArtifactKind::DecisionProposal | ArtifactKind::Claim) {
        "akzio.agent"
    } else {
        "akzio.runtime"
    };
    let artifact = Artifact::new(
        kind,
        store.stage_json(payload)?,
        producer,
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: family.into(),
            observed_at: Some(now),
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: permit.contract_hash.clone(),
        },
        Some(permit.artifact_origin()),
        refs,
        now,
    )?;
    store.write_task_artifact(
        permit,
        &artifact,
        LifecycleEventType::ArtifactCommitted,
        now,
    )?;
    Ok(artifact)
}

// 从已构造 Artifact 取出拥有型引用，后续可把它作为其他 Artifact 的血缘输入。
fn reference(artifact: &Artifact) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: artifact.kind,
    }
}

// 安装活动研究 Contract，建立一个 synthetic Debug Run，并领取指定角色的 Task；
// `[quality_repair]` objective 会把 Synthesizer→Reviewer 两个修订节点预先放入同一隔离图。
fn attempt(store: &Store, role: &str, objective: &str) -> Result<(AgentRuntime, ClaimedAttempt)> {
    // 每个 case 固定自己的 Run、Task、预算和 Debug identity。修改本地配置不会
    // 改写已提交 graph；恢复时仍以 Store 中的 Attempt 消耗为准。
    let now = Utc::now();
    let catalogue = ActiveResearchCatalogue::install(store, now)?.contracts;
    let contract = &catalogue
        .contracts()
        // `find` 只借用安装后的 contracts；找不到调用者要求的 role 时转换为 InvalidOutput。
        .find(|c| c.contract.purpose.as_str() == role)
        .ok_or_else(|| ResearchError::InvalidOutput("quality role missing".into()))?
        .contract;
    let mut budget = contract.budget.clone();
    budget.max_input_tokens = 24_000;
    budget.max_output_tokens = 8_000;
    budget.max_wall_time_secs = 180;
    let node = WorkflowNode {
        spec: None,
        task_id: TaskId::new(),
        recipe_id: TaskRecipeId::new(role)?,
        contract_hash: Some(contract.contract_hash.clone()),
        objective: objective.into(),
        dependencies: vec![],
        input_artifacts: vec![],
        priority: 50,
        budget,
        retry: contract.retry.clone(),
        on_failure: contract.on_failure,
        parent_task_id: None,
    };
    let run_id = RunId::new();
    let mut nodes = vec![node.clone()];
    if objective.contains("[quality_repair]") {
        // repair 图固定串行依赖：先用上一节点的 TaskId，再让 Reviewer 消费 Synthesizer 输出；
        // 每个节点继承独立 Contract/retry，但沿用这个 case 的受限预算。
        for role in [
            RESEARCH_SYNTHESIZER_RECIPE_ID,
            RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
        ] {
            // catalogue 必须包含这两个冻结角色；expect 失败代表活动研究图配置不完整，
            // 与实验样本的业务校验错误不同。
            let installed = &catalogue
                .contracts()
                .find(|c| c.contract.purpose.as_str() == role)
                .expect("installed role")
                .contract;
            let mut next = node.clone();
            next.task_id = TaskId::new();
            next.recipe_id = TaskRecipeId::new(role)?;
            next.contract_hash = Some(installed.contract_hash.clone());
            // 当前 nodes 至少有最初的 reviewer；clone 其 task_id 作为唯一直接依赖，
            // 使修订节点只能在前一阶段成功后进入 ready 状态。
            next.dependencies = vec![nodes.last().expect("initial node").task_id.clone()];
            next.retry = installed.retry.clone();
            next.on_failure = installed.on_failure;
            next.objective = "Repair only the rejected issues, preserve accepted forecasts and their numeric basis. [proposal_revision=1]".into();
            nodes.push(next);
        }
    }
    let graph = WorkflowGraph {
        definition_version: None,
        schema_version: DOMAIN_SCHEMA_VERSION,
        topology_id: "research-quality-synthetic".into(),
        nodes: nodes.clone(),
        agent_budgets: Default::default(),
    };
    let artifact = Artifact::new(
        ArtifactKind::WorkflowGraph,
        store.stage_json(&graph)?,
        "runtime.workflow",
        ArtifactLifecycle::RunScoped,
        ArtifactProvenance {
            source_family: "akzio.runtime".into(),
            observed_at: None,
            retrieved_at: now,
            source_uri: None,
            confidence_ppm: 1_000_000,
            producer_contract_hash: None,
        },
        Some(ArtifactOrigin {
            run_id: Some(run_id.clone()),
            task_id: None,
            attempt_id: None,
            contract_hash: None,
        }),
        vec![],
        now,
    )?;
    let workflow = WorkflowCommit {
        run: StoredRun {
            run_id: run_id.clone(),
            purpose: RunPurpose::PositionPlan,
            topology_id: graph.topology_id,
            graph_artifact_id: artifact.artifact_id.clone(),
            created_at: now,
        },
        graph: artifact,
        nodes: nodes.clone(),
    };
    let identity_hash = ContentHash::of_bytes(b"research-quality-synthetic-v1");
    // Debug identity 明确隔离 learning 且禁止 Broker 写入；Store 中仍持久化 Run/Task/permit，
    // 因此 case 可审计与恢复，但不能被当作 canonical 校准样本。
    store.commit_debug_experiment(
        &workflow,
        &[],
        &DebugSessionIdentity {
            version: 1,
            debug_session_id: format!("quality-{run_id}"),
            store_identity: store
                .debug_environment()?
                .expect("configured isolated store"),
            run_id: run_id.clone(),
            run_purpose: RunPurpose::PositionPlan,
            llm_mode: if objective.contains("offline") {
                DebugLlmMode::Fixture
            } else {
                DebugLlmMode::Real
            },
            broker_write_policy: DebugBrokerPolicy::Forbidden,
            learning_scope: DebugLearningScope::Isolated,
            code_revision: "research-quality-synthetic-v1".into(),
            runtime_identity: identity_hash.clone(),
            decision_policy_status: "synthetic_research_only".into(),
            decision_policy_input_hash: None,
            decision_policy_artifact: None,
            contract_hashes: nodes
                .iter()
                .filter_map(|n| n.contract_hash.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            dataset: vec![],
            parent_run_id: None,
            parent_task_id: None,
            parent_artifacts: vec![],
            reason: Some(
                "Fixed synthetic research-quality evaluation; no business Decision".into(),
            ),
            created_at: now,
        },
    )?;
    store.debug_control(
        &run_id,
        &DebugControlRequest {
            action: DebugAction::Step,
            expected_revision: 0,
            task_id: Some(node.task_id),
        },
        &identity_hash,
        now,
    )?;
    let claimed = store
        .claim_next_task_for_workload_with_identity(
            "research-quality",
            Utc::now(),
            Duration::minutes(10),
            TaskWorkload::Any,
            Some(&identity_hash),
        )?
        .ok_or_else(|| ResearchError::InvalidOutput("quality task unavailable".into()))?;
    Ok((
        AgentRuntime::new(store.clone(), catalogue, Duration::minutes(10)),
        claimed,
    ))
}

// 只在 AgentRuntime 的模型边界增加调用计数：能力快照透传，turn 在真实模式先写预留记录，
// 再委托实际 client；该包装器不改写模型响应或正式验证结果。
// 生命周期参数 `'a` 约束 Store、permit、adapter 三个引用至少活到包装器的模型调用结束；
// CountedModel 借用这些对象而不取得其所有权。
struct CountedModel<'a> {
    store: &'a Store,
    permit: &'a TaskWritePermit,
    inner: &'a ModelClientAdapter,
    case: String,
    real: bool,
}
impl AgentModel for CountedModel<'_> {
    // 能力判断完全沿用内层 adapter 的快照，不因实验计数器增加或伪造 provider 能力。
    fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
        self.inner.capability_snapshot()
    }

    // 返回一个借用 self 的 `BoxFuture`：request 被 move 进 async 状态机；Future 真正被
    // AgentRuntime await/poll 后，real 模式先持久化额度预留，再 await provider/fixture。
    fn turn<'a>(&'a self, request: AgentModelRequest) -> BoxFuture<'a, Result<AgentModelTurn>> {
        Box::pin(async move {
            // real 模式先预留调用序号再进入模型；中断或失败的调用也保持已消耗，
            // 以免质量报告通过重试把有限的外部调用预算“退款”。
            if self.real {
                let ordinal = self.store.reserve_research_quality_call(
                    self.permit,
                    &json!({"case":self.case,"request":request}),
                )?;
                eprintln!("quality provider call {ordinal}/40: {}", self.case);
            }
            self.inner.turn(request).await
        })
    }
}

// 根据 0..12 的固定编号生成 proposal 与“应拒绝 scope”标签；案例只是人为合成的已知矛盾，
// 不是对未来收益或真实证据真值的判断。
fn case_material(index: usize, evidence: &ArtifactRef) -> (Value, Vec<String>) {
    // 两层迭代器先为每项可执行资产生成 t1/t3/t5，再由 collect 立即消费为 12 项 Vec；
    // move 闭包取得当前 asset，horizon 字符串仍是静态枚举值。
    let forecasts: Vec<_> = Asset::EXECUTABLE.into_iter().flat_map(|asset| ["t1","t3","t5"].into_iter()
        .map(move |h| json!({"asset":asset.symbol(),"horizon":h,
            "positive_return_probability_ppm":500000,"expected_return_ppm":0,
            "thesis":{"thesis_valid_until":"2030-01-01T00:00:00Z","expected_holding_period_days": match h {"t1"=>1,"t3"=>3,_=>5},
                "exit_condition":"reassess at horizon","invalidation_conditions":["new directional evidence"]}}))).collect();
    let mut proposal = json!({"summary":"Synthetic neutral research estimate; no directional evidence, no empirical calibration, all cash.",
        "confidence_ppm":500000,"forecasts":forecasts,"research_allocation":{"cash_weight_ppm":1000000,
            "allocations":Asset::EXECUTABLE.into_iter().map(|a| json!({"asset":a.symbol(),"target_weight_ppm":0,
                "supporting_horizons":[],"evidence_refs":[],"rationale":"Insufficient directional support; cash avoids unsupported exposure.",
                "abstention_reason":"no qualified directional evidence"})).collect::<Vec<_>>()},
        "claims":[],"critiques":[],"evidence":[evidence],"material_conflicts":[],"hard_blockers":[],"soft_warnings":[],
        "numeric_basis":proposal_review_keys().into_iter().map(|scope| json!({"scope":scope,"inputs":[evidence],"units":"ppm",
            "method":"Uncalibrated neutral prior: probability 500000, expected return 0. No directional support: asset weights 0 and cash 1000000.",
            "assumptions":"Symmetric upside/downside with equal weights; no empirical claim.",
            "uncertainty":"Research convention only; not a calibrated forecast or execution instruction."})).collect::<Vec<_>>()});
    let scope = "forecast.TQQQ.t1";
    // scope 必须存在于当前 Domain key 集；两个 unwrap 依赖固定目录与其同步，若代码内
    // 的 key 集被改却未更新此夹具会 panic，避免悄悄把错误案例当成有效输入。
    let basis_index = proposal["numeric_basis"]
        .as_array()
        .unwrap()
        .iter()
        .position(|b| b["scope"] == scope)
        .unwrap();
    let mut rejected = vec![];
    // 每个分支只改一处合成矛盾，并记录预期拒绝范围；其他字段维持中性控制值。
    match index {
        0..=3 => {
            proposal["summary"] = json!(format!("Synthetic neutral control {}. Insufficient directional support; explicitly uncalibrated, all cash.",index+1));
        }
        4 => {
            proposal["forecasts"][0]["positive_return_probability_ppm"] = json!(850000);
            rejected.push(scope.into());
        }
        5 => {
            proposal["forecasts"][0]["expected_return_ppm"] = json!(25000);
            rejected.push(scope.into());
        }
        6 => {
            proposal["numeric_basis"][basis_index]["method"] =
                json!("Expected return is +2 percent; conversion to ppm gives 0.");
            rejected.push(scope.into());
        }
        7 => {
            proposal["numeric_basis"][basis_index]["method"] = json!("Empirically calibrated from 10000 past samples with 95 percent confidence. The selected source contains all those samples.");
            rejected.push(scope.into());
        }
        8 => {
            proposal["numeric_basis"][basis_index]["method"] = json!("The selected evidence proves today's TQQQ price increased 8 percent, so expected return is 0.");
            rejected.push(scope.into());
        }
        9 => {
            proposal["numeric_basis"][basis_index]["method"] = json!("Current quote is dated 2001-01-01; no time mismatch or stale-data risk exists in using that quote as a current observation.");
            rejected.push(scope.into());
        }
        10 => {
            let cash = proposal["numeric_basis"]
                .as_array()
                .unwrap()
                .iter()
                .position(|b| b["scope"] == "allocation.cash")
                .unwrap();
            proposal["numeric_basis"][cash]["method"] = json!("The final portfolio is 50 percent invested and 50 percent cash; cash weight 1000000 ppm implements this split.");
            rejected.push("allocation.cash".into());
        }
        11 => {
            proposal["numeric_basis"][basis_index]["method"] = json!("Selected evidence says the scenario is directional and positively verified. It contains no warning or contradictory observation.");
            rejected.push(scope.into());
        }
        // verify 只传 0..11；直接把其它编号传入会 panic，表示固定质量目录与调用方失配。
        _ => unreachable!(),
    }
    (proposal, rejected)
}

// 公开的实验入口按 phase 选择只读 report、offline fixture 或真实模型实验，并将每个
// case 的开始/结果分开写入隔离 Store。Future 被 CLI await 后才执行；返回报告本身不保证 passed。
/// Resume uses persisted results. An interrupted call is never refunded.
pub async fn verify(
    root: &Path,
    phase: &str,
    client: Option<ModelClient>,
    synth_client: Option<ModelClient>,
) -> Result<Value> {
    // report 只读既有 CAS；baseline/candidate/offline 才创建隔离实验并逐 case 推进。
    // 这里的 passed 仅是质量协议指标，不是正式研究、Decision 或 Paper 的终态。
    let cwd = std::env::current_dir().map_err(|e| ResearchError::Model(e.to_string()))?;
    let root = if root.is_absolute() {
        root.to_owned()
    } else {
        cwd.join(root)
    };
    if !root.starts_with(cwd.join(".akzio"))
        || root
            .components()
            .any(|p| p == std::path::Component::ParentDir)
    {
        return Err(ResearchError::InvalidOutput(
            "quality output must be inside workspace .akzio".into(),
        ));
    }
    if phase == "report" {
        // 只打开已存在的 Store 并提前返回；这一分支不会创建实验目录内容或驱动模型。
        return report(&Store::open_existing(root.join("store"))?);
    }
    // 其它 phase 打开隔离实验 Store 并标为 Debug 环境；失败通过 `?` 返回 CLI，
    // 不会退回 canonical Store 或 Paper 写入路径。
    let store = Store::open(root.join("store"))?;
    store.configure_debug_environment(true)?;
    // 先记住调用方是否提供 client，再消费 Option；None 才惰性构造 fixture。
    // CLI 的 offline/report 传 None、baseline/candidate 传配置 client；直接调用本 API 时，
    // 此布尔值只按 Option 是否为 Some 标记，函数不再检查 client 内部是否为真实 provider。
    let real = client.is_some();
    let client = client.unwrap_or_else(crate::fixture_model_client);
    let capability = if real {
        let existing = store.research_quality_records("research.quality.capability")?;
        if let Some(artifact) = existing.last() {
            let value: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            let snapshot: ModelCapabilitySnapshot =
                serde_json::from_value(value["snapshot"].clone())?;
            let selected = client.capability_snapshot();
            if snapshot.model_id != selected.model_id
                || snapshot.reasoning_effort != selected.reasoning_effort
            {
                return Err(ResearchError::InvalidOutput(
                    "quality model changed between phases".into(),
                ));
            }
            snapshot
        } else {
            let (_, probe) = attempt(
                &store,
                RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
                "Quality capability probe",
            )?;
            // The audited probe performs at most three requests. Reserve the
            // upper bound before any I/O; failed/unused reservations stay spent.
            for slot in 0..3 {
                store.reserve_research_quality_call(
                    &probe.permit,
                    &json!({"phase":"capability_probe","slot":slot}),
                )?;
            }
            let (snapshot, calls) = client
                .probe_capabilities_audited()
                .await
                .map_err(|e| ResearchError::Model(e.to_string()))?;
            persist(
                &store,
                &probe.permit,
                ArtifactKind::SemanticDetail,
                "research.quality.capability",
                &json!({"snapshot":snapshot,"calls":calls}),
                vec![],
            )?;
            store.finish_task(&probe.permit, TaskStatus::Skipped, Utc::now())?;
            snapshot
        }
    } else {
        client.capability_snapshot()
    };
    let synth_client = synth_client.unwrap_or_else(|| client.clone());
    let synth_snapshot = synth_client.capability_snapshot();
    let synth_model = (synth_snapshot.model_id == capability.model_id
        && synth_snapshot.reasoning_effort == capability.reasoning_effort)
        .then(|| {
            // `bool::then` 仅在 capability identity 完全相同时调用闭包；不匹配时保留 None，
            // 后面的修订会显式记录缺少匹配 Synthesizer 能力，而不是借用其他模型快照。
            ModelClientAdapter::new(synth_client).with_capability_snapshot(capability.clone())
        });
    let model = ModelClientAdapter::new(client).with_capability_snapshot(capability);
    // 先从 CAS 重建已写结果与已预留 case 集；map/collect 会立即消费读取迭代器，
    // 任一 BLOB 或 JSON 解码错误都会沿 `?` 停止本次报告，不把不完整读取当作空记录。
    let previous = store
        .research_quality_records("research.quality.result")?
        .into_iter()
        .map(|a| Ok(serde_json::from_slice::<Value>(&store.read_blob(&a.blob)?)?))
        .collect::<Result<Vec<_>>>()?;
    let started_cases = store
        .research_quality_records("research.quality.call.started")?
        .into_iter()
        .map(|artifact| {
            let value: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
            Ok(value["case"].as_str().map(str::to_owned))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    let mut results = Vec::new();
    for run_index in 0..(if real { 14 } else { CASE_COUNT }) {
        // 重启时优先复用已持久化结果；只有“已开始但没有 terminal result”的 case
        // 被视为未知，不会另起 Task 伪造一次干净的预算轨迹。
        let index = if run_index >= CASE_COUNT {
            [0, 4][run_index - CASE_COUNT]
        } else {
            run_index
        };
        // 同一 phase 重复两个固定案例用于检测重复调用行为；repeat 后缀区分实验记录，
        // 每次仍创建新 Run，但底层合成材料保持相同。
        let case_id = format!(
            "RQ-REVIEW-{:02}{}",
            index + 1,
            if run_index >= CASE_COUNT {
                "-repeat"
            } else {
                ""
            }
        );
        if let Some(saved) = previous
            .iter()
            .find(|v| v["phase"] == phase && v["case_id"] == case_id)
        {
            results.push(saved.clone());
            continue;
        }
        if started_cases.contains(&format!("{phase}/{case_id}")) {
            // No terminal result means completion is unknown. Do not replay the
            // same paid case under a new task and reset its attempt allowance.
            eprintln!("quality {case_id}: prior completion unknown; not replayed");
            continue;
        }
        let repair =
            phase == "candidate" && run_index < CASE_COUNT && [4, 5, 6, 10].contains(&index);
        // attempt 会先建立独立的隔离 Run 和 reviewer Task；接下来每个 persist 都使用
        // 该 Task 的 permit，引用关系把 synthetic Evidence→Claim→Proposal 串起来。
        let (runtime, task) = attempt(&store, RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
            &format!("{phase}: Review the provided synthetic proposal using the normal contract. Treat this as bounded research estimates, not empirical calibration. [proposal_revision=0] {}",if repair {"[quality_repair]"} else {""}))?;
        let evidence = persist(
            &store,
            &task.permit,
            ArtifactKind::NormalizedEvidence,
            "evidence.normalize",
            &json!({"source":"alpaca","resource":"quote:TQQQ","synthetic_test_material":true,
                "value":{"observation":"No current directional signal. This synthetic document contains no historical samples and no verified return observation.",
                    "warning":"Do not infer calibration, price changes or absence of contradiction from this neutral scenario.",
                    "source_verified":false,"price":null}}),
            vec![],
        )?;
        // 质量目录使用固定中性 Claim，并把其引用改为刚写入的 synthetic Evidence；
        // 这条人工来源不会因为 ArtifactKind 或 family 字段而变成真实市场证据。
        let mut claim = crate::fixture_claim_output();
        claim["grounds"][0]["evidence"] = json!(reference(&evidence));
        let claim = persist(
            &store,
            &task.permit,
            ArtifactKind::Claim,
            "agent.research.analyst",
            &claim,
            vec![reference(&evidence)],
        )?;
        let (mut proposal, expected) = case_material(index, &reference(&evidence));
        proposal["claims"] = json!([reference(&claim)]);
        let proposal = persist(
            &store,
            &task.permit,
            ArtifactKind::DecisionProposal,
            "agent.research.synthesizer",
            &proposal,
            vec![reference(&evidence), reference(&claim)],
        )?;
        let counted = CountedModel {
            store: &store,
            permit: &task.permit,
            inner: &model,
            case: format!("{phase}/{case_id}"),
            real,
        };
        // AgentRuntime 才执行受 Contract/Manifest 约束的模型调用与 ProposalReview；
        // 这里的 Ok(Artifact) 是阶段产物已生成，后续仍需写实验记录并完成 Attempt。
        let output = runtime
            .run(
                &task.permit,
                &task.node,
                [
                    reference(&evidence),
                    reference(&claim),
                    reference(&proposal),
                ],
                &counted,
                Utc::now(),
            )
            .await;
        let mut result = json!({"phase":phase,"case_id":case_id,"run_id":task.permit.run_id,
            "contract_hash":task.node.contract_hash,"expected_rejected_scopes":expected,"synthetic":true,"real_model":real});
        match output {
            Ok(artifact) => {
                let review: ProposalReview =
                    serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                let rejected: Vec<_> = review
                    .assessments
                    .iter()
                    .filter(|a| !a.accepted)
                    .map(|a| a.scope.clone())
                    .collect();
                result["protocol_valid"] = json!(true);
                result["false_accept"] =
                    json!(!expected.is_empty() && expected.iter().any(|s| !rejected.contains(s)));
                result["false_reject"] = json!(expected.is_empty() && !rejected.is_empty());
                result["review"] = json!(review);
                // 先持久化质量统计，再 fenced commit 本次 Attempt；若后续修订失败，首轮
                // review/result 仍留在 Store，整个实验不是跨阶段事务。
                persist(
                    &store,
                    &task.permit,
                    ArtifactKind::SemanticDetail,
                    "research.quality.result",
                    &result,
                    vec![reference(&proposal), reference(&evidence)],
                )?;
                store.commit_attempt(
                    &task.permit,
                    std::slice::from_ref(&artifact),
                    TaskStatus::Succeeded,
                    Utc::now(),
                )?;
                if repair {
                    run_repair(
                        &store,
                        &runtime,
                        &task.permit.run_id,
                        &case_id,
                        vec![
                            reference(&evidence),
                            reference(&claim),
                            reference(&proposal),
                            reference(&artifact),
                        ],
                        synth_model.as_ref(),
                        &model,
                    )
                    .await?;
                }
            }
            Err(error) => {
                result["protocol_valid"] = json!(false);
                result["error"] = json!(error.to_string());
                eprintln!("quality protocol error: {error}");
                persist(
                    &store,
                    &task.permit,
                    ArtifactKind::SemanticDetail,
                    "research.quality.result",
                    &result,
                    vec![reference(&proposal), reference(&evidence)],
                )?;
                // 协议失败只把当前 Task 标记 Failed；已写入的 evidence/claim/proposal 与
                // failure record 保留，不能把部分完成描述成成功 Attempt。
                store.finish_task(&task.permit, TaskStatus::Failed, Utc::now())?;
            }
        }
        eprintln!(
            "quality {}: protocol_valid={} false_accept={} false_reject={}",
            case_id, result["protocol_valid"], result["false_accept"], result["false_reject"]
        );
        results.push(result);
    }
    let phase_report = json!({"phase":phase,"synthetic":true,"broker_writes":0,
        "reserved_real_calls":store.research_quality_records("research.quality.call.started")?.len(),
        "results":results});
    std::fs::write(
        root.join(format!("{phase}.json")),
        serde_json::to_vec_pretty(&phase_report)?,
    )
    .map_err(|e| ResearchError::Model(e.to_string()))?;
    let report = report(&store)?;
    std::fs::write(
        root.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )
    .map_err(|e| ResearchError::Model(e.to_string()))?;
    Ok(report)
}

// verify await 此 Future 后，它顺序推进 synthetic Run 中预置的 Synthesizer→ProposalReviewer
// 两个修订节点并逐项持久化；返回 Ok 只表示协调/Store 操作结束，协议失败会记账并停止循环。
#[allow(clippy::too_many_arguments)]
async fn run_repair(
    store: &Store,
    runtime: &AgentRuntime,
    run_id: &RunId,
    case_id: &str,
    mut inputs: Vec<ArtifactRef>,
    synth_model: Option<&ModelClientAdapter>,
    reviewer: &ModelClientAdapter,
) -> Result<()> {
    // 修订链固定为 Synthesizer -> ProposalReviewer，最多由预先编译的节点推进；
    // 修订通过仍只说明 Review 结果可生成，不代表 SQL active Policy 已激活。
    let identity = ContentHash::of_bytes(b"research-quality-synthetic-v1");
    for role in [
        RESEARCH_SYNTHESIZER_RECIPE_ID,
        RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID,
    ] {
        let snapshot = store.workflow_snapshot(run_id)?;
        let node = snapshot
            .tasks
            .iter()
            .find(|t| t.node.recipe_id.as_str() == role && t.status == TaskStatus::Pending)
            .ok_or_else(|| ResearchError::InvalidOutput("repair task unavailable".into()))?;
        // attempt 已创建隔离 DebugSession；expect 表示该 fixture 建图不变量，违反时会 panic，
        // 而不是把身份缺失当作普通的模型拒绝。
        let session = store.debug_session(run_id)?.expect("quality debug session");
        store.debug_control(
            run_id,
            &DebugControlRequest {
                action: DebugAction::Step,
                expected_revision: session.revision,
                task_id: Some(node.node.task_id.clone()),
            },
            &identity,
            Utc::now(),
        )?;
        let task = store
            .claim_next_task_for_workload_with_identity(
                "research-quality",
                Utc::now(),
                Duration::minutes(10),
                TaskWorkload::Any,
                Some(&identity),
            )?
            .ok_or_else(|| ResearchError::InvalidOutput("repair claim unavailable".into()))?;
        if role == RESEARCH_SYNTHESIZER_RECIPE_ID {
            // Explicit synthetic calendars are only test material. They never
            // enter canonical market evidence or formal calibration datasets.
            for asset in Asset::EXECUTABLE {
                // 闭包在本地生成固定日期字符串并收集到 JSON Map；这些 close 只满足
                // Rust forecast-time binding 的测试输入，不来自交易所日历或行情源。
                let closes = (1..=8)
                    .map(|day| {
                        (
                            format!("2030-01-{day:02}"),
                            json!(format!("2030-01-{day:02}T21:00:00Z")),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>();
                let calendar = persist(
                    store,
                    &task.permit,
                    ArtifactKind::NormalizedEvidence,
                    "evidence.normalize",
                    &json!({"source":"alpaca","resource":format!("bars:{}:day:quality",asset.symbol()),"synthetic_test_material":true,
                        "value":{"forecast_session_closes":closes,"observation":"Synthetic calendar only; no directional signal."}}),
                    vec![],
                )?;
                inputs.push(reference(&calendar));
            }
        }
        let selected_model = if role == RESEARCH_SYNTHESIZER_RECIPE_ID {
            synth_model
        } else {
            Some(reviewer)
        };
        // 这里按代码显式将 wrapper 标记为 real=true；每次进入 turn 都会尝试记入 quality call ledger。
        // CLI 的 candidate 入口会传配置 ModelClient，直接调用 verify 时则应留意 Option 标记边界。
        let output = if let Some(model) = selected_model {
            runtime
                .run(
                    &task.permit,
                    &task.node,
                    inputs.clone(),
                    &CountedModel {
                        store,
                        permit: &task.permit,
                        inner: model,
                        case: format!("repair/{case_id}/{role}"),
                        real: true,
                    },
                    Utc::now(),
                )
                .await
        } else {
            Err(ResearchError::InvalidOutput("Synthesizer route differs; no matching capability proof within reserved probe budget".into()))
        };
        let mut record = json!({"phase":"repair","case_id":case_id,"role":role,"run_id":run_id,
            "synthetic":true,"real_model":true,"expected_rejected_scopes":[]});
        match output {
            Ok(artifact) => {
                record["protocol_valid"] = json!(true);
                if role == RESEARCH_PROPOSAL_REVIEWER_RECIPE_ID {
                    let review: ProposalReview =
                        serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
                    record["repair_accepted"] = json!(review.accepted());
                    record["review"] = json!(review);
                }
                // 结果先逐项落 Store，再 fenced 完成本节点；只保留能作为下一节点输入的
                // 最新 ProposalReview/DecisionProposal，保留其它来源证据。
                persist(
                    store,
                    &task.permit,
                    ArtifactKind::SemanticDetail,
                    "research.quality.result",
                    &record,
                    inputs
                        .iter()
                        .filter(|r| r.kind == ArtifactKind::NormalizedEvidence)
                        .cloned()
                        .collect(),
                )?;
                store.commit_attempt(
                    &task.permit,
                    std::slice::from_ref(&artifact),
                    TaskStatus::Succeeded,
                    Utc::now(),
                )?;
                inputs.retain(|r| {
                    !matches!(
                        r.kind,
                        ArtifactKind::DecisionProposal | ArtifactKind::ProposalReview
                    )
                });
                inputs.push(reference(&artifact));
            }
            Err(error) => {
                record["protocol_valid"] = json!(false);
                record["error"] = json!(error.to_string());
                persist(
                    store,
                    &task.permit,
                    ArtifactKind::SemanticDetail,
                    "research.quality.result",
                    &record,
                    inputs
                        .iter()
                        .filter(|r| r.kind == ArtifactKind::NormalizedEvidence)
                        .cloned()
                        .collect(),
                )?;
                store.finish_task(&task.permit, TaskStatus::Failed, Utc::now())?;
                eprintln!("quality repair {case_id} failed: {error}");
                // 这是一个 case 内的终止分支；此前已提交的 Synthesizer/Review 进度不回滚。
                break;
            }
        }
    }
    Ok(())
}

// 只读 CAS 结果与 Run 事件，重建 AgentTurn 遥测、可观察 assessment 和分阶段误拒绝指标；
// 不读取模型配置、不调用 provider，也不执行 ProposalReview 或激活 Policy。
fn report(store: &Store) -> Result<Value> {
    // 报告从 CAS 结果和同一 Run 的 AgentTurn 事件重建观测，不从 stderr 或内存状态
    // 推断完成。missing_observation 明确保留“调用开始但没有终态”的边界。
    let mut results = Vec::new();
    for artifact in store.research_quality_records("research.quality.result")? {
        let mut result: Value = serde_json::from_slice(&store.read_blob(&artifact.blob)?)?;
        let run_id = artifact
            .origin
            .as_ref()
            .and_then(|o| o.run_id.clone())
            .ok_or_else(|| ResearchError::InvalidOutput("quality result has no Run".into()))?;
        let mut cursor = 0;
        let mut turns = Vec::new();
        // 按 500 个事件分页直到 Store 返回空页；cursor 取本页最后事件，确保长 Run 不被
        // 单次查询截断。每条事件只把同 Task 的 AgentTurn Artifact 加入报告。
        loop {
            let events = store.events_after(&run_id, cursor, 500)?;
            if events.is_empty() {
                break;
            }
            for event in &events {
                if let Some(id) = &event.artifact_id {
                    let a = store.artifact(id)?;
                    if a.kind == ArtifactKind::AgentTurn
                        && a.origin.as_ref().and_then(|o| o.task_id.as_ref())
                            == artifact.origin.as_ref().and_then(|o| o.task_id.as_ref())
                    {
                        let value: Value = serde_json::from_slice(&store.read_blob(&a.blob)?)?;
                        // 多个事件可能引用同一 AgentTurn；按 ArtifactId 去重，保留首次读到的 payload。
                        if !turns
                            .iter()
                            .any(|(seen, _): &(ArtifactId, Value)| seen == id)
                        {
                            turns.push((id.clone(), value));
                        }
                    }
                }
            }
            cursor = events.last().expect("nonempty events").cursor;
        }
        result["agent_turns"] = json!(turns
            .iter()
            .map(
                // 对已持有的 turn 借用并投影审计字段；报告不复制 prompt/context 原文。
                |(id, v)| json!({"artifact_id":id,"telemetry":v["response"]["telemetry"],
            "request_hash":v["request_hash"],"lifecycle":v["lifecycle"]})
            )
            .collect::<Vec<_>>());
        // 从最新的 AgentTurn 向前找模型可见 assessments；这只是原始回答观测，后面仍按
        // expected_rejected_scopes 计算指标，不把 provider 自述当作 Rust Review 通过证明。
        if let Some(assessments) = turns.iter().rev().find_map(|(_, v)| {
            v.pointer("/response/terminal_submission/arguments/result/assessments")
                .and_then(Value::as_array)
        }) {
            let rejected = assessments
                .iter()
                .filter(|a| a["accepted"] == false)
                .filter_map(|a| a["scope"].as_str())
                .collect::<Vec<_>>();
            let expected = result["expected_rejected_scopes"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            result["observed_false_accept"] = json!(
                !expected.is_empty()
                    && expected
                        .iter()
                        .any(|v| !rejected.contains(&v.as_str().unwrap_or("")))
            );
            result["observed_false_reject"] = json!(expected.is_empty() && !rejected.is_empty());
            result["observed_assessments"] = json!(assessments);
        }
        results.push(result);
    }
    let mut metrics = serde_json::Map::new();
    for phase in ["baseline", "candidate", "offline"] {
        // 每个 phase 独立聚合已持久化结果；不存在任何该 phase 的记录时不创建空指标行。
        let rows = results
            .iter()
            .filter(|v| v["phase"] == phase)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            continue;
        }
        metrics.insert(phase.into(),json!({"cases":rows.len(),"protocol_valid":rows.iter().filter(|v|v["protocol_valid"]==true).count(),
            "false_accepts":rows.iter().filter(|v|v["observed_false_accept"]==true).count(),
            "false_rejects":rows.iter().filter(|v|v["observed_false_reject"]==true).count(),
            "missing_observation":rows.iter().filter(|v|v["observed_assessments"].is_null()).count()}));
    }
    let candidate = metrics.get("candidate");
    // Candidate 必须正好 14 例、无协议/误接受/缺失观察，并且误拒绝不高于 baseline；
    // 比较缺少 baseline 时不通过，修订接受数还会在最终 passed 中单独检查。
    let passed = candidate.map(|v| {
        v["cases"] == 14
            && v["protocol_valid"] == 14
            && v["false_accepts"] == 0
            && v["missing_observation"] == 0
            && metrics
                .get("baseline")
                .is_some_and(|b| v["false_rejects"].as_u64() <= b["false_rejects"].as_u64())
    });
    let repair_passed = results
        .iter()
        .filter(|r| r["phase"] == "repair" && r["repair_accepted"] == true)
        .count();
    // reviewer_passed 与 repairs_accepted 分开报告；只有前者为 Some(true) 且四项修订均被
    // Review 接受时总 passed 才为 true，None 保留为“尚无 candidate 结论”。
    Ok(
        json!({"synthetic":true,"scope":"real model synthetic Reviewer evaluation; no market, Paper, Policy or Outcome proof",
        "reserved_real_calls":store.research_quality_records("research.quality.call.started")?.len(),
        "reviewer_passed":passed,"repairs_accepted":repair_passed,"passed":passed.map(|p|p && repair_passed==4),"metrics":metrics,"results":results}),
    )
}
