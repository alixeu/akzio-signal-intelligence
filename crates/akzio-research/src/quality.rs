//! Fixed, synthetic research-quality experiment. Uses the production context,
//! role prompts, schema, validator and SQL/CAS authority; never a broker.

// 质量实验复用正式 Agent/Context/Contract 校验，市场材料与日历是一次性测试 Store
// 内的 synthetic 数据；offline 用 fixture，历史 baseline/candidate 路径曾调用真实模型，
// 不能把这些模型回答称为合成或正式市场结论。它能检验“协议是否拒绝已知坏样例”，不能证明
// Policy 校准、Paper 下单、成交、T+1/T+3/T+5 Outcome 或业务收益已经发生。
// 文件导读：旧 Debug CLI 已退役；本模块只在 Cargo 测试目标中编译。offline
// 使用本 crate 的确定性 fixture；历史 baseline/candidate 代码不进入生产二进制，report 只读已有
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
pub(crate) fn persist(
    store: &Store,
    permit: &TaskWritePermit,
    kind: ArtifactKind,
    producer: &str,
    payload: &Value,
    refs: Vec<ArtifactRef>,
) -> Result<Artifact> {
    // 质量记录也走 Artifact + task permit；测试 Run 不配置 Broker、Decision 或
    // Outcome，不能获得正式校准和 Paper 执行资格。
    let now = Utc::now();
    let family = match kind {
        ArtifactKind::NormalizedEvidence => payload["source"].as_str().unwrap_or("alpaca"),
        ArtifactKind::DecisionProposal | ArtifactKind::Claim | ArtifactKind::Critique => {
            "akzio.agent"
        }
        _ => "akzio.runtime",
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

// 安装活动研究 Contract，在一次性 Store 建 synthetic PositionPlan Run 并领取
// 指定角色的 Task；`[quality_repair]` 预置两个修订节点，不创建 Debug 控制。
pub(crate) fn attempt(
    store: &Store,
    role: &str,
    objective: &str,
) -> Result<(AgentRuntime, ClaimedAttempt)> {
    // 每个 case 固定自己的 Run、Task 和预算。修改本地配置不会
    // 改写已提交 graph；恢复时仍以 Store 中的 Attempt 消耗为准。
    let catalogue = ActiveResearchCatalogue::install(store, Utc::now())?.contracts;
    attempt_with_contract(store, catalogue, role, objective, None)
}

fn attempt_with_contract(
    store: &Store,
    catalogue: crate::ContractCatalogue,
    role: &str,
    objective: &str,
    candidate: Option<&ContentHash>,
) -> Result<(AgentRuntime, ClaimedAttempt)> {
    attempt_with_contract_and_lease(
        store,
        catalogue,
        role,
        objective,
        candidate,
        Duration::minutes(10),
    )
}

fn attempt_with_contract_and_lease(
    store: &Store,
    catalogue: crate::ContractCatalogue,
    role: &str,
    objective: &str,
    candidate: Option<&ContentHash>,
    lease_duration: Duration,
) -> Result<(AgentRuntime, ClaimedAttempt)> {
    let now = Utc::now();
    let installed = match candidate {
        Some(hash) => catalogue.get(hash)?,
        None => catalogue
            .contracts()
            .find(|installed| installed.contract.purpose.as_str() == role)
            .ok_or_else(|| ResearchError::InvalidOutput("quality role missing".into()))?,
    };
    let contract = &installed.contract;
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
    // Cargo-only fixture uses an ordinary PositionPlan Run in its own
    // disposable Store. It has no DebugSession, broker adapter, scheduler
    // slot, Decision/Outcome or path into canonical calibration.
    store.commit_workflow(&workflow)?;
    let claimed = store
        .claim_next_task_for_workload(
            "research-quality",
            Utc::now(),
            lease_duration,
            TaskWorkload::Any,
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
pub(crate) fn case_material(index: usize, evidence: &ArtifactRef) -> (Value, Vec<String>) {
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
    if !matches!(phase, "offline" | "report") || client.is_some() || synth_client.is_some() {
        return Err(ResearchError::InvalidOutput(
            "real-model quality experiments are retired; Cargo fixtures are offline only".into(),
        ));
    }
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
    // The test owns this disposable Store; it cannot submit broker orders or
    // supply a canonical Paper Outcome to calibration.
    let store = Store::open(root.join("store"))?;
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
        let task = store
            .claim_next_task_for_workload(
                "research-quality",
                Utc::now(),
                Duration::minutes(10),
                TaskWorkload::Any,
            )?
            .ok_or_else(|| ResearchError::InvalidOutput("repair claim unavailable".into()))?;
        if task.node.task_id != node.node.task_id {
            return Err(ResearchError::InvalidOutput(
                "quality repair claimed a different task".into(),
            ));
        }
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

#[cfg(test)]
mod offline_quality_tests {
    use super::*;
    use std::path::PathBuf;

    struct OwnedExperiment(PathBuf);
    impl Drop for OwnedExperiment {
        fn drop(&mut self) {
            if self.0.exists() {
                std::fs::remove_dir_all(&self.0).unwrap();
            }
        }
    }

    /// WHY THIS TEST EXISTS (Task #14): Persisted Responses usage feeds the producer token basis shared by Outcome and Canary. Cached and reasoning tokens are subsets, and partial reports cannot establish complete usage.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn persisted_responses_usage_is_counted_once_and_requires_both_totals() {
        let root = OwnedExperiment(
            std::env::current_dir()
                .unwrap()
                .join(".akzio")
                .join(format!("cargo-usage-{}", RunId::new())),
        );
        let store = Store::open(&root.0).unwrap();
        let full_usage = json!({
            "input_tokens": 100, "input_tokens_details": {"cached_tokens": 70},
            "output_tokens": 40, "output_tokens_details": {"reasoning_tokens": 30},
        });
        for (status, provider_usage, expected_turns, expected_total) in [
            ("completed", full_usage.clone(), 1, Some(140)),
            ("incomplete", full_usage.clone(), 1, Some(140)),
            (
                "completed",
                json!({"input_tokens_details": {"cached_tokens": 70}, "output_tokens": 40}),
                1,
                None,
            ),
            ("preflight_rejection", full_usage, 0, None),
        ] {
            let (runtime, task) = attempt(
                &store,
                RESEARCH_ANALYST_RECIPE_ID,
                "Observe synthetic usage",
            )
            .unwrap();
            let evidence = persist(
                &store,
                &task.permit,
                ArtifactKind::NormalizedEvidence,
                "evidence.normalize",
                &json!({"source": "alpaca", "resource": "quote:TQQQ", "value": {"price": 100}}),
                vec![],
            )
            .unwrap();
            let client = ModelClient::Fixture(json!({
                "status": status,
                "incomplete_details": {"reason": "max_output_tokens"},
                "output": [{"type": "message", "role": "assistant", "content": [
                    {"type": "output_text", "text": "Fixture intentionally omits submit_result"}
                ]}],
                "usage": provider_usage,
            }));
            let mut capability = client.capability_snapshot();
            capability.supports_tool_calls = status != "preflight_rejection";
            let model = ModelClientAdapter::new(client).with_capability_snapshot(capability);
            let result = runtime
                .run(
                    &task.permit,
                    &task.node,
                    [reference(&evidence)],
                    &model,
                    Utc::now(),
                )
                .await;
            assert!(
                matches!(
                    (status, &result),
                    ("completed", Err(ResearchError::AmbiguousSubmission))
                        | ("incomplete", Err(ResearchError::ProviderIncomplete { .. }))
                        | (
                            "preflight_rejection",
                            Err(ResearchError::CapabilityMismatch { .. })
                        )
                ),
                "{status}: {result:?}"
            );

            let health = store.run_lifecycle_health(&task.permit.run_id).unwrap();
            let usage = health.lifecycle_usage;
            let producer_usage = store
                .model_usage_for_producing_run(&reference(&evidence))
                .unwrap();
            assert_eq!(usage.turns, expected_turns, "{status}");
            assert_eq!(usage, producer_usage);
            assert_eq!(health.outcome_usage.turns, 0);
            assert_eq!(
                producer_usage.billable_tokens_if_complete(),
                expected_total,
                "{status}"
            );
            if status == "incomplete" {
                assert_eq!(usage.latency_millis_if_complete(), None);
            }
            if status == "completed" && expected_total.is_some() {
                assert_eq!(usage.cached_input_tokens, 70);
                assert_eq!(usage.reasoning_tokens, 30);
                assert!(usage.latency_millis_if_complete().is_some());

                let mut historical = serde_json::to_value(usage).unwrap();
                let fields = historical.as_object_mut().unwrap();
                fields.remove("token_accounting_version").unwrap();
                fields.remove("turns_with_latency").unwrap();
                let historical: akzio_store::RunModelUsage =
                    serde_json::from_value(historical).unwrap();
                assert_eq!(historical.billable_tokens_if_complete(), None);

                store
                    .append_task_event(
                        &task.permit,
                        LifecycleEventType::AgentTurnStarted,
                        Utc::now(),
                    )
                    .unwrap();
                let interrupted = store.run_model_usage(&task.permit.run_id).unwrap();
                assert_eq!(interrupted.turns, 2);
                assert_eq!(interrupted.turns_missing_usage, 1);
                assert_eq!(interrupted.billable_tokens_if_complete(), None);
            }
        }
    }

    /// WHY THIS TEST EXISTS (Task #14): A completed refusal, empty output or failed Responses event can report actual usage even though Agent execution rejects the output.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn terminal_responses_errors_keep_reported_usage() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

        let root = OwnedExperiment(
            std::env::current_dir()
                .unwrap()
                .join(".akzio")
                .join(format!("cargo-failed-usage-{}", RunId::new())),
        );
        let store = Store::open(&root.0).unwrap();
        let full_usage = json!({
            "input_tokens": 100, "input_tokens_details": {"cached_tokens": 70},
            "output_tokens": 40, "output_tokens_details": {"reasoning_tokens": 30},
        });
        let refusal = json!([{"type": "message", "role": "assistant", "content": [
            {"type": "refusal", "refusal": "Rejected by provider"}
        ]}]);
        for (event_type, status, output, usage, class, expected) in [
            (
                "response.completed",
                "completed",
                refusal,
                full_usage.clone(),
                "other",
                Some(140),
            ),
            (
                "response.completed",
                "completed",
                json!([]),
                full_usage.clone(),
                "invalid_output",
                Some(140),
            ),
            (
                "response.failed",
                "failed",
                json!([]),
                full_usage.clone(),
                "invalid_output",
                Some(140),
            ),
            (
                "response.failed",
                "failed",
                json!([]),
                json!({"input_tokens": 0, "output_tokens": 0}),
                "invalid_output",
                Some(0),
            ),
            (
                "response.failed",
                "in_progress",
                json!([]),
                full_usage,
                "invalid_output",
                None,
            ),
            (
                "response.failed",
                "failed",
                json!([]),
                json!({"input_tokens": 100}),
                "invalid_output",
                None,
            ),
        ] {
            let (runtime, task) =
                attempt(&store, RESEARCH_ANALYST_RECIPE_ID, "Observe failed usage").unwrap();
            let evidence = persist(
                &store,
                &task.permit,
                ArtifactKind::NormalizedEvidence,
                "evidence.normalize",
                &json!({"source": "alpaca", "resource": "quote:TQQQ", "value": {"price": 100}}),
                vec![],
            )
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let event = json!({"type": event_type, "response": {
                "id": "offline-response", "status": status, "output": output, "usage": usage,
            }});
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut content_length = 0;
                loop {
                    let mut line = String::new();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            content_length = value.trim().parse().unwrap();
                        }
                    }
                }
                reader
                    .read_exact(&mut vec![0; content_length])
                    .await
                    .unwrap();
                let body = format!("data: {event}\n\n");
                let reply = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                reader.get_mut().write_all(reply.as_bytes()).await.unwrap();
            });
            let client = akzio_model::OpenAIResponsesClient::new(
                format!("http://{address}/v1"),
                "offline-test-key",
                "offline-test",
                "low",
            )
            .unwrap();
            let model = ModelClientAdapter::with_response_language(
                ModelClient::OpenAIResponses(client),
                true,
                "en",
            )
            .with_capability_snapshot(ModelClient::Fixture(Value::Null).capability_snapshot());
            let result = runtime
                .run(
                    &task.permit,
                    &task.node,
                    [reference(&evidence)],
                    &model,
                    Utc::now(),
                )
                .await;
            assert!(result.is_err());
            tokio::time::timeout(std::time::Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
            let events = store.events_after(&task.permit.run_id, 0, 100).unwrap();
            let terminal = events
                .iter()
                .find(|event| {
                    matches!(
                        event.lifecycle_kind().unwrap(),
                        LifecycleEventType::AgentTurnFailed
                            | LifecycleEventType::AgentTurnRetryableFailed
                    )
                })
                .unwrap();
            let artifact = store
                .artifact(terminal.artifact_id.as_ref().unwrap())
                .unwrap();
            let payload: Value =
                serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
            assert_eq!(payload["error_class"], class);
            let usage = store
                .model_usage_for_producing_run(&reference(&evidence))
                .unwrap();
            assert_eq!(usage.turns, 1);
            assert_eq!(
                usage.billable_tokens_if_complete(),
                expected,
                "{event_type} {status}"
            );
        }
    }

    const PRICE_DAYS: [&str; 6] = [
        "2026-11-06",
        "2026-11-09",
        "2026-11-10",
        "2026-11-11",
        "2026-11-12",
        "2026-11-13",
    ];

    fn price_bar_fixture(
        store: &Store,
        permit: &TaskWritePermit,
        late: bool,
        missing_close: bool,
    ) -> Artifact {
        use akzio_ingest::{
            AcquiredEvidence, EvidenceProvenance, EvidenceRequest, EvidenceRuntime, EvidenceSource,
        };

        let resource = "bars:QQQ:1d:2026-11-06:6";
        let need = persist(
            store,
            permit,
            ArtifactKind::EvidenceNeed,
            "runtime.evidence.need",
            &json!({
                "schema_version": DOMAIN_SCHEMA_VERSION, "source_family": "alpaca",
                "resource": resource, "max_age_secs": 3600,
            }),
            vec![],
        )
        .unwrap();
        let cutoff = Utc::now();
        let observed_at = if late {
            cutoff + Duration::seconds(1)
        } else {
            cutoff - Duration::seconds(1)
        };
        let source_uri = "fixture://alpaca/bars?adjustment=all&feed=iex".to_owned();
        let bars = PRICE_DAYS
            .into_iter()
            .enumerate()
            .map(|(i, date)| {
                json!({
                    "t": format!("{date}T05:00:00Z"), "o": 100, "h": 110, "l": 90,
                    "c": format!("{}.000001", 100 + i), "v": 50,
                })
            })
            .collect::<Vec<_>>();
        let session_closes = PRICE_DAYS
            .into_iter()
            .filter(|date| !missing_close || *date != PRICE_DAYS[4])
            .map(|date| (date, format!("{date}T21:00:00Z")))
            .collect::<std::collections::BTreeMap<_, _>>();
        let normalized = json!({
            "market": {}, "symbol": "QQQ", "price_basis": "adjusted_research", "decision_cutoff": cutoff,
            "bars": bars, "session_closes": session_closes, "availability_lag_minutes": 20,
            "content_available_at": "2026-11-13T21:20:00Z",
        });
        let request = EvidenceRequest {
            source: EvidenceSource::Alpaca,
            resource: resource.into(),
            max_age: Duration::hours(1),
            acquisition_mode: EvidenceAcquisitionMode::ModelReviewed,
        };
        let acquired = AcquiredEvidence {
            raw: serde_json::to_vec(&json!({"bars":bars})).unwrap(),
            media_type: "application/json".into(),
            source_uri: source_uri.clone(),
            observed_at,
            normalized,
            provenance: EvidenceProvenance {
                document_id: None,
                published_at: None,
                observed_at,
                revision: None,
                source_uri,
                dedupe_key: "typed-close-fixture".into(),
                citations: Vec::new(),
            },
            quality: Default::default(),
        };
        let bundle = EvidenceRuntime::new(store.clone(), [EvidenceSource::Alpaca])
            .materialize_validated(permit, &reference(&need), &request, acquired, cutoff)
            .unwrap();
        for artifact in [&bundle.raw, &bundle.normalized] {
            store
                .write_task_artifact(
                    permit,
                    artifact,
                    LifecycleEventType::ArtifactCommitted,
                    Utc::now(),
                )
                .unwrap();
        }
        bundle.normalized
    }

    fn observed_close(index: usize) -> Value {
        json!({"bar_timestamp":format!("{}T05:00:00Z", PRICE_DAYS[index]),
            "value_micros": (100 + index) * 1_000_000 + 1})
    }

    /// WHY THIS TEST EXISTS (Task #28): An authorized evidence reference alone cannot prove a price, direction, unit, or exact row seen by the Analyst.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn analyst_price_fact_candidate_checks_the_actual_submit_projection() {
        let root = OwnedExperiment(
            std::env::current_dir()
                .unwrap()
                .join("../../target/analyst-price-fact-tests")
                .join(RunId::new().0),
        );
        let store = Store::open(&root.0).unwrap();
        let active = ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
        let freshness = active
            .install_analyst_freshness_candidate(&store, Utc::now())
            .unwrap();
        let candidate = active
            .install_analyst_price_fact_candidate(&store, Utc::now())
            .unwrap();
        assert_eq!(candidate.contract.version, 72);
        let after = ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
        assert_eq!(
            active.contracts.contracts().collect::<Vec<_>>(),
            after.contracts.contracts().collect::<Vec<_>>()
        );
        assert_eq!(
            freshness,
            after
                .install_analyst_freshness_candidate(&store, Utc::now())
                .unwrap()
        );
        let original = after
            .contracts
            .contracts()
            .find(|c| c.contract.purpose.as_str() == RESEARCH_ANALYST_RECIPE_ID)
            .unwrap();
        let mut comparable = candidate.contract.clone();
        comparable.version = original.contract.version;
        comparable.output.schema = original.contract.output.schema.clone();
        comparable.contract_hash = original.contract.contract_hash.clone();
        assert_eq!(comparable, original.contract);
        let candidate_hash = candidate.contract.contract_hash.clone();
        let active_hash = original.contract.contract_hash.clone();
        let catalogue = after.contracts.with_installed_candidate(candidate).unwrap();

        let level = json!({"kind":"alpaca_daily_close", "asset":"QQQ", "unit":"usd_micros", "bar":observed_close(5)});
        let comparison = json!({"kind":"alpaca_daily_close_comparison", "asset":"QQQ", "unit":"usd_micros",
            "earlier":observed_close(4), "later":observed_close(5), "direction":"increase"});
        let revise = |original: &Value, changes: Value| {
            let mut revised = original.clone();
            for (key, value) in changes.as_object().unwrap() {
                revised[key] = value.clone();
            }
            revised
        };
        for (case, fact, reason) in [
            (
                "wrong_value",
                revise(
                    &level,
                    json!({"bar":{"bar_timestamp":level["bar"]["bar_timestamp"],"value_micros":105_000_000}}),
                ),
                Some("value_mismatch"),
            ),
            (
                "hidden",
                revise(&level, json!({"bar":observed_close(0)})),
                Some("bar_not_presented"),
            ),
            (
                "wrong_timestamp",
                revise(
                    &level,
                    json!({"bar":{"bar_timestamp":"2026-11-13T05:00:01Z","value_micros":105_000_001}}),
                ),
                Some("bar_not_presented"),
            ),
            (
                "wrong_direction",
                revise(&comparison, json!({"direction":"decrease"})),
                Some("direction_mismatch"),
            ),
            (
                "reversed",
                revise(
                    &comparison,
                    json!({"earlier":observed_close(5),"later":observed_close(4),"direction":"decrease"}),
                ),
                Some("invalid_chronology"),
            ),
            (
                "wrong_unit",
                revise(&level, json!({"unit":"percent"})),
                Some("unsupported_price_unit"),
            ),
            (
                "wrong_asset",
                revise(&level, json!({"asset":"SOXX"})),
                Some("asset_mismatch"),
            ),
            ("late", level.clone(), Some("capture_after_cutoff")),
            (
                "missing_close",
                comparison.clone(),
                Some("session_close_unknown"),
            ),
            ("valid_close", level.clone(), None),
            ("valid_comparison", comparison.clone(), None),
            ("repair_value", level.clone(), None),
            ("repair_deliberation", level.clone(), None),
            ("unknown", Value::Null, None),
            ("legacy", Value::Null, None),
        ] {
            let (runtime, task) = attempt_with_contract(
                &store,
                catalogue.clone(),
                RESEARCH_ANALYST_RECIPE_ID,
                "Verify a typed observed price",
                Some(if case == "legacy" {
                    &active_hash
                } else {
                    &candidate_hash
                }),
            )
            .unwrap();
            let evidence = price_bar_fixture(
                &store,
                &task.permit,
                case == "late",
                case == "missing_close",
            );
            let mut claim = crate::fixture_claim_output();
            claim["statement"] = json!("The observed close does not establish the next return.");
            claim["grounds"][0]["evidence"] = json!({"artifact_id":evidence.artifact_id});
            claim["grounds"][0]["assets"] = json!(["QQQ"]);
            if case != "legacy" {
                claim["grounds"][0]["fact"] = fact.clone();
            }
            let akzio_model::ModelClient::FixtureByPurposePhase(templates) =
                crate::fixture_model_client()
            else {
                panic!("expected phase fixture");
            };
            let mut response = templates[RESEARCH_ANALYST_RECIPE_ID][1].clone();
            let mut arguments: Value =
                serde_json::from_str(response["output"][0]["arguments"].as_str().unwrap()).unwrap();
            arguments["result"] = claim;
            arguments["deliberation"]["basis_artifact_ids"] = json!([evidence.artifact_id]);
            response["output"][0]["arguments"] = json!(arguments.to_string());
            let client = match case {
                "repair_value" => {
                    let mut first = response.clone();
                    let mut rejected = arguments.clone();
                    rejected["result"]["grounds"][0]["fact"]["bar"]["value_micros"] = json!(1);
                    first["output"][0]["arguments"] = json!(rejected.to_string());
                    ModelClient::fixture_sequence([first, response])
                }
                "repair_deliberation" => {
                    let mut first = response.clone();
                    let mut rejected = arguments.clone();
                    rejected["deliberation"]["uncertainties"] =
                        json!(["The forecast is uncertain."]);
                    rejected["deliberation"]["uncertainty_weight_ppm"] = json!([10_000]);
                    first["output"][0]["arguments"] = json!(rejected.to_string());
                    let repair = json!({"deliberation": arguments["deliberation"]});
                    response["output"][0]["arguments"] = json!(repair.to_string());
                    ModelClient::fixture_sequence([first, response])
                }
                _ => ModelClient::Fixture(response),
            };
            let model = ModelClientAdapter::with_debug(client, true);
            let result = runtime
                .run(
                    &task.permit,
                    &task.node,
                    [reference(&evidence)],
                    &model,
                    Utc::now(),
                )
                .await;
            if case == "hidden" || case == "valid_close" {
                let evidence_id = json!(evidence.artifact_id);
                let input = store
                    .run_artifacts_by_kind(&task.run_id, ArtifactKind::AgentTurn)
                    .unwrap()
                    .into_iter()
                    .find_map(|turn| {
                        let trace: Value =
                            serde_json::from_slice(&store.read_blob(&turn.blob).unwrap()).unwrap();
                        let input = trace
                            .pointer("/response/model_debug/request/input")?
                            .as_str()?;
                        let input: Value = serde_json::from_str(input).unwrap();
                        assert_eq!(input["context"], trace["request"]["context"]);
                        assert_eq!(
                            input["context_manifest"],
                            trace["request"]["manifest_artifact_id"]
                        );
                        Some(input)
                    })
                    .unwrap();
                let document = input["context"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|value| {
                        value["type"] == "must_read"
                            && value["metadata"]["document_id"] == evidence_id
                    })
                    .unwrap();
                let shown = document
                    .pointer("/value/value_summary/bars")
                    .unwrap()
                    .as_array()
                    .unwrap();
                let original: Value =
                    serde_json::from_slice(&store.read_blob(&evidence.blob).unwrap()).unwrap();
                let sealed = original["value"]["bars"].as_array().unwrap();
                assert_eq!(sealed.len(), 6);
                assert_eq!(shown.as_slice(), &sealed[1..]);
                assert!(document
                    .pointer("/value/value_summary/session_closes")
                    .is_none());
            }
            if let Some(reason) = reason {
                assert!(
                    matches!(&result, Err(ResearchError::InvalidOutput(message)) if message.contains(reason)),
                    "{case}: {result:?}"
                );
                assert!(store
                    .run_artifacts_by_kind(&task.run_id, ArtifactKind::Claim)
                    .unwrap()
                    .is_empty());
                continue;
            }
            let artifact = result.unwrap_or_else(|error| panic!("{case}: {error}"));
            store
                .write_task_artifact(
                    &task.permit,
                    &artifact,
                    LifecycleEventType::ArtifactCommitted,
                    Utc::now(),
                )
                .unwrap();
            let persisted: Value =
                serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
            assert_eq!(persisted["grounds"][0]["fact"], fact, "{case}");
            assert_eq!(persisted["stance"], "neutral");
            assert_eq!(
                persisted["grounds"][0]["evidence"],
                json!(reference(&evidence))
            );
            assert!(artifact.source_refs.contains(&reference(&evidence)));
            if case == "legacy" {
                let decoded: ResearchClaim = serde_json::from_value(persisted).unwrap();
                assert!(serde_json::to_value(&decoded).unwrap()["grounds"][0]
                    .get("fact")
                    .is_none());
            }
        }
    }

    fn fred_fact_fixture(
        store: &Store,
        permit: &TaskWritePermit,
        cutoff: chrono::DateTime<Utc>,
        vintage: chrono::NaiveDate,
        days: &[chrono::NaiveDate; 6],
        case: &str,
    ) -> Artifact {
        use akzio_ingest::{
            AcquiredEvidence, EvidenceProvenance, EvidenceRequest, EvidenceRuntime, EvidenceSource,
            FredDirectTransport,
        };

        let start = days[0];
        let end = days[5];
        let resource = format!("series:DFII10:{start}:{end}:{vintage}");
        let need = persist(
            store,
            permit,
            ArtifactKind::EvidenceNeed,
            "runtime.evidence.need",
            &json!({
                "schema_version": DOMAIN_SCHEMA_VERSION, "source_family":"fred",
                "resource":resource, "max_age_secs":3600,
            }),
            vec![],
        )
        .unwrap();
        let mut rows = days
            .iter()
            .zip([
                "-2",
                "-1",
                "0",
                "0.125",
                "-9007199254740992.000000000002",
                "-9007199254740992.000000000001",
            ])
            .map(|(date, value)| {
                json!({
                    "date":date, "value":value, "realtime_start":vintage, "realtime_end":vintage,
                })
            })
            .collect::<Vec<_>>();
        if case == "duplicate" {
            rows.push(rows[5].clone());
        }
        if case == "wrong_row_vintage" {
            rows[5]["realtime_start"] = json!(vintage.succ_opt().unwrap());
            rows[5]["realtime_end"] = json!(vintage.succ_opt().unwrap());
        }
        if case == "missing_value" {
            rows[5]["value"] = json!(".");
        }
        if case == "overprecision" {
            rows[5]["value"] = json!("0.1250000000001");
        }
        let mut envelope = json!({
            "realtime_start":vintage, "realtime_end":vintage,
            "count":rows.len(), "offset":0, "limit":1000, "observations":rows,
        });
        match case {
            "missing_provider_unit" | "null_without_unit" => {}
            "transformed_provider_unit" => envelope["units"] = json!("pch"),
            _ => envelope["units"] = json!("lin"),
        }
        let raw = serde_json::to_vec(&envelope).unwrap();
        let (normalized, quality) =
            FredDirectTransport::normalize_response(&resource, &raw).unwrap();
        let source_uri = if case == "unproved_capture" {
            "fixture://fred/unproved-response".into()
        } else {
            format!("https://api.stlouisfed.org/fred/series/observations?series_id=DFII10&file_type=json&observation_start={start}&observation_end={end}&realtime_start={vintage}&realtime_end={vintage}")
        };
        let observed_at = cutoff + Duration::seconds(1);
        let request = EvidenceRequest {
            source: EvidenceSource::Fred,
            resource: resource.clone(),
            max_age: Duration::hours(1),
            acquisition_mode: EvidenceAcquisitionMode::ModelReviewed,
        };
        let provenance = EvidenceProvenance {
            document_id: Some(resource),
            published_at: None,
            observed_at,
            revision: None,
            source_uri: source_uri.clone(),
            dedupe_key: format!("fred:{}", ContentHash::of_bytes(&raw)),
            citations: vec![],
        };
        let acquired = AcquiredEvidence {
            raw,
            media_type: "application/json".into(),
            source_uri,
            observed_at,
            normalized,
            provenance,
            quality,
        };
        let bundle = EvidenceRuntime::new(store.clone(), [EvidenceSource::Fred])
            .materialize_with_frozen_cutoff(
                permit,
                &reference(&need),
                &request,
                acquired,
                cutoff,
                Utc::now(),
            )
            .unwrap();
        for artifact in [&bundle.raw, &bundle.normalized] {
            store
                .write_task_artifact(
                    permit,
                    artifact,
                    LifecycleEventType::ArtifactCommitted,
                    Utc::now(),
                )
                .unwrap();
        }
        bundle.normalized
    }

    /// WHY THIS TEST EXISTS (Task #28): A valid FRED reference cannot prove an unshown row, another vintage, or an inferred economic unit.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn analyst_candidate_verifies_fred_levels_and_trends_in_native_units() {
        let root = OwnedExperiment(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/analyst-fred-fact-tests")
                .join(RunId::new().0),
        );
        let store = Store::open(&root.0).unwrap();
        let active = ActiveResearchCatalogue::install(&store, Utc::now()).unwrap();
        let candidate = active
            .install_analyst_price_fact_candidate(&store, Utc::now())
            .unwrap();
        let candidate_hash = candidate.contract.contract_hash.clone();
        let catalogue = active
            .contracts
            .with_installed_candidate(candidate)
            .unwrap();
        let cutoff = Utc::now() - Duration::seconds(2);
        let vintage = cutoff.date_naive() - chrono::Days::new(2);
        let days = std::array::from_fn(|index| vintage - chrono::Days::new((6 - index) as u64));
        let observation = |index: usize, value: &str| json!({"date":days[index],"value":value});
        let level = json!({"kind":"fred_observation","series_id":"DFII10","vintage":vintage,
            "unit":"series_native","observation":observation(5,"-9007199254740992.000000000001")});
        let comparison = json!({"kind":"fred_observation_comparison","series_id":"DFII10","vintage":vintage,
            "unit":"series_native","earlier":observation(4,"-9007199254740992.000000000002"),
            "later":observation(5,"-9007199254740992.000000000001"),"direction":"increase"});
        for (case, reason) in [
            ("level", None),
            ("trailing_zeroes", None),
            ("trend", None),
            ("unknown", None),
            ("null_without_unit", None),
            (
                "transformed_provider_unit",
                Some("fred_native_units_unverified"),
            ),
            (
                "missing_provider_unit",
                Some("fred_native_units_unverified"),
            ),
            ("wrong_unit", Some("unsupported_fred_unit")),
            ("wrong_series", Some("fred_series_or_vintage_mismatch")),
            ("wrong_vintage", Some("fred_series_or_vintage_mismatch")),
            ("wrong_value", Some("fred_value_mismatch")),
            ("overprecision", Some("fred_decimal_out_of_range")),
            ("wrong_direction", Some("direction_mismatch")),
            ("reverse_dates", Some("invalid_chronology")),
            ("hidden_row", Some("fred_observation_not_presented")),
            ("missing_value", Some("fred_observation_not_presented")),
            (
                "duplicate",
                Some("availability_fred_point_snapshot_unverified"),
            ),
            (
                "wrong_row_vintage",
                Some("availability_fred_point_snapshot_unverified"),
            ),
            (
                "unproved_capture",
                Some("availability_fred_point_snapshot_unverified"),
            ),
        ] {
            let (runtime, task) = attempt_with_contract(
                &store,
                catalogue.clone(),
                RESEARCH_ANALYST_RECIPE_ID,
                "Verify a FRED series-native observation",
                Some(&candidate_hash),
            )
            .unwrap();
            let evidence = fred_fact_fixture(&store, &task.permit, cutoff, vintage, &days, case);
            let mut fact = level.clone();
            match case {
                "trend" => fact = comparison.clone(),
                "trailing_zeroes" => fact["observation"] = observation(3, "+0.125000000000"),
                "unknown" | "null_without_unit" => fact = Value::Null,
                "wrong_unit" => fact["unit"] = json!("percent"),
                "wrong_series" => fact["series_id"] = json!("DGS10"),
                "wrong_vintage" => fact["vintage"] = json!(vintage.succ_opt().unwrap()),
                "wrong_value" => {
                    fact["observation"]["value"] = json!("-9007199254740992.000000000002")
                }
                "hidden_row" => fact["observation"] = observation(0, "-2"),
                "wrong_direction" => {
                    fact = comparison.clone();
                    fact["direction"] = json!("decrease");
                }
                "reverse_dates" => {
                    fact = comparison.clone();
                    fact["earlier"] = comparison["later"].clone();
                    fact["later"] = comparison["earlier"].clone();
                }
                _ => {}
            }
            let mut claim = crate::fixture_claim_output();
            claim["statement"] = json!("The observed FRED levels do not establish a forecast.");
            claim["grounds"][0]["evidence"] = json!({"artifact_id":evidence.artifact_id});
            claim["grounds"][0]["fact"] = fact.clone();
            let ModelClient::FixtureByPurposePhase(templates) = crate::fixture_model_client()
            else {
                panic!("expected phase fixture");
            };
            let mut response = templates[RESEARCH_ANALYST_RECIPE_ID][1].clone();
            let mut arguments: Value =
                serde_json::from_str(response["output"][0]["arguments"].as_str().unwrap()).unwrap();
            arguments["result"] = claim;
            arguments["deliberation"]["basis_artifact_ids"] = json!([evidence.artifact_id]);
            response["output"][0]["arguments"] = json!(arguments.to_string());
            let model = ModelClientAdapter::with_debug(ModelClient::Fixture(response), true);
            let result = runtime
                .run(
                    &task.permit,
                    &task.node,
                    [reference(&evidence)],
                    &model,
                    Utc::now(),
                )
                .await;
            if let Some(reason) = reason {
                assert!(
                    matches!(&result, Err(ResearchError::InvalidOutput(message)) if message.contains(reason)),
                    "{case}: {result:?}"
                );
                assert!(store
                    .run_artifacts_by_kind(&task.run_id, ArtifactKind::Claim)
                    .unwrap()
                    .is_empty());
                continue;
            }
            let claim = result.unwrap_or_else(|error| panic!("{case}: {error}"));
            store
                .write_task_artifact(
                    &task.permit,
                    &claim,
                    LifecycleEventType::ArtifactCommitted,
                    Utc::now(),
                )
                .unwrap();
            let persisted: Value =
                serde_json::from_slice(&store.read_blob(&claim.blob).unwrap()).unwrap();
            assert_eq!(persisted["grounds"][0]["fact"], fact);
            assert!(claim.source_refs.contains(&reference(&evidence)));
        }
    }

    fn store_lesson(
        store: &Store,
        source: &Artifact,
        slug: &str,
        exclusions: Vec<String>,
        conflicts_with: Vec<ArtifactRef>,
    ) -> akzio_store::StoredLesson {
        store_lesson_with_governance(
            store,
            source,
            slug,
            exclusions,
            conflicts_with,
            LessonGovernance::operator_reviewed(source.created_at),
        )
    }

    fn store_lesson_with_governance(
        store: &Store,
        source: &Artifact,
        slug: &str,
        exclusions: Vec<String>,
        conflicts_with: Vec<ArtifactRef>,
        governance: LessonGovernance,
    ) -> akzio_store::StoredLesson {
        let now = source.created_at;
        let lesson = Lesson {
            schema_version: DOMAIN_SCHEMA_VERSION,
            lesson_id: LessonId(slug.to_owned()),
            origin: LessonOrigin::Operator,
            lifecycle: LessonLifecycle::Active,
            title: format!("Lesson {slug}"),
            statement: format!("Scope the price review ({slug})."),
            rationale: "private operational rationale".to_owned(),
            recommended_behavior: format!("Check the matching horizon ({slug})."),
            exclusions,
            scope: LessonScope {
                assets: [Asset::Qqq].into(),
                horizons: [DecisionHorizon::T1].into(),
                decision_stages: [RESEARCH_SYNTHESIZER_RECIPE_ID.to_owned()].into(),
                ..Default::default()
            },
            source_refs: vec![reference(source)],
            supersedes: vec![],
            conflicts_with,
            confidence_ppm: 1_000_000,
            authored_by: Some("lesson fixture author".to_owned()),
            approved_by: Some("lesson fixture approver".to_owned()),
            created_at: now,
            updated_at: now,
            governance: Some(governance),
        };
        store.write_lesson(&lesson, source, now).unwrap().lesson
    }

    async fn synthesizer_request(
        store: &Store,
        catalogue: crate::ContractCatalogue,
        contract_hash: &ContentHash,
    ) -> (Value, ContextManifestPayload) {
        let (runtime, claimed) = attempt_with_contract(
            store,
            catalogue,
            RESEARCH_SYNTHESIZER_RECIPE_ID,
            "Check how the model receives scoped Lessons.",
            Some(contract_hash),
        )
        .unwrap();
        // The normal Submit serializer and trace writer run with an offline response.
        // The fixture response is intentionally not an accepted proposal.
        let fixture = ModelClientAdapter::with_debug(
            ModelClient::fixture_sequence([json!({
                "output":[{"type":"message","content":[{"type":"output_text","text":"fixture only"}]}],
                "usage":{"input_tokens":40,"output_tokens":10}
            })]),
            true,
        );
        let evidence = persist(
            store,
            &claimed.permit,
            ArtifactKind::NormalizedEvidence,
            "quality.synthesizer.neutral_evidence",
            &json!({"resource":"bars:QQQ:1d", "source":"alpaca", "value":{"observation":"No directional signal."}}),
            vec![],
        ).unwrap();
        let result = runtime
            .run(
                &claimed.permit,
                &claimed.node,
                [reference(&evidence)],
                &fixture,
                Utc::now(),
            )
            .await;
        assert!(result.is_err());
        let (turn, trace) = store
            .recent_artifacts_by_kind(ArtifactKind::AgentTurn, 12)
            .unwrap()
            .into_iter()
            .filter(|artifact| artifact.origin.as_ref() == Some(&claimed.permit.artifact_origin()))
            .map(|artifact| {
                let trace: Value =
                    serde_json::from_slice(&store.read_blob(&artifact.blob).unwrap()).unwrap();
                (artifact, trace)
            })
            .find(|(_, trace)| {
                trace["lifecycle"]["status"] == "completed"
                    && trace["domain_request"]["continuation"].is_null()
            })
            .unwrap_or_else(|| panic!("Synthesizer did not dispatch a fresh request: {result:?}"));
        let request = trace["domain_request"].clone();
        assert_eq!(request["phase"], "submit");
        assert_eq!(request["tools"], json!([]));
        let provider_request = &trace["response"]["model_debug"]["request"];
        let provider_input: Value =
            serde_json::from_str(provider_request["input"].as_str().unwrap()).unwrap();
        assert_eq!(provider_input["context"], request["context"]);
        assert_eq!(
            provider_input["context_manifest"],
            request["manifest_artifact_id"]
        );
        assert_eq!(provider_request["tools"].as_array().unwrap().len(), 1);
        assert_eq!(provider_request["tools"][0]["name"], "submit_result");
        let manifest_id: ArtifactId =
            serde_json::from_value(request["manifest_artifact_id"].clone()).unwrap();
        assert!(turn
            .source_refs
            .iter()
            .any(|reference| reference.artifact_id == manifest_id));
        let manifest = store.artifact(&manifest_id).unwrap();
        let payload = serde_json::from_slice(&store.read_blob(&manifest.blob).unwrap()).unwrap();
        store
            .commit_attempt(&claimed.permit, &[], TaskStatus::Failed, Utc::now())
            .unwrap();
        (request, payload)
    }

    /// WHY THIS TEST EXISTS (Task #34): v69's historical references still exhaust its Lesson budget without claiming model presentation.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn legacy_lesson_references_preserve_recall_and_contestation_budget() {
        let root = OwnedExperiment(
            std::env::temp_dir().join(format!("akzio-legacy-lesson-budget-{}", RunId::new())),
        );
        let store = Store::open(&root.0).unwrap();
        let now = Utc::now();
        let source = Artifact::new(
            ArtifactKind::SemanticDetail,
            store
                .stage_json(&json!({"source":"legacy governance fixture"}))
                .unwrap(),
            "operator.lesson.source",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".into(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        let governance = LessonGovernance {
            max_usage_before_revalidation: 2,
            ..LessonGovernance::operator_reviewed(now)
        };
        let lesson = store_lesson_with_governance(
            &store,
            &source,
            "legacy-budget",
            vec![],
            vec![],
            governance,
        );
        let catalogue = ActiveResearchCatalogue::install(&store, now).unwrap();
        let purpose = ContractPurpose::new(RESEARCH_SYNTHESIZER_RECIPE_ID).unwrap();
        let active = store.active_contract(&purpose).unwrap().unwrap().contract;
        assert_eq!((active.version, active.prompt.version), (69, 38));
        for count in 1..=2 {
            let (request, manifest) =
                synthesizer_request(&store, catalogue.contracts.clone(), &active.contract_hash)
                    .await;
            assert!(manifest
                .selections
                .iter()
                .any(|selection| selection.artifact == reference(&lesson.artifact)));
            assert!(!request["context"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["class"] == "lesson_projection"));
            let usage = store.lesson_usage(&lesson.lesson.lesson_id).unwrap();
            assert_eq!((usage.context_manifests, usage.decision_contexts), (0, 0));
            assert_eq!(
                (usage.legacy_manifest_refs, usage.legacy_decision_refs),
                (count, 0)
            );
            assert!(usage.latest_used_at.is_none());
            if count == 1 {
                assert_eq!(
                    store
                        .contest_lessons_due_for_revalidation(Utc::now())
                        .unwrap()
                        .contested,
                    0
                );
            }
        }
        let (_, manifest) =
            synthesizer_request(&store, catalogue.contracts, &active.contract_hash).await;
        assert!(!manifest
            .selections
            .iter()
            .any(|selection| selection.artifact == reference(&lesson.artifact)));
        assert_eq!(
            store
                .contest_lessons_due_for_revalidation(Utc::now())
                .unwrap()
                .contested,
            1
        );
        let current = store.lesson(&lesson.lesson.lesson_id).unwrap().unwrap();
        assert_eq!(
            (current.revision, current.lesson.lifecycle),
            (2, LessonLifecycle::Contested)
        );
        assert_eq!(
            store
                .contest_lessons_due_for_revalidation(Utc::now())
                .unwrap()
                .contested,
            0
        );
    }

    /// WHY THIS TEST EXISTS (Task #34): a real Submit request must show full Lesson constraints or omit the group.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn synthesizer_request_projects_complete_lesson_groups_with_exact_budget() {
        let root =
            OwnedExperiment(std::env::temp_dir().join(format!("akzio-lesson-{}", RunId::new())));
        let store = Store::open(&root.0).unwrap();
        let now = Utc::now();
        let source = Artifact::new(
            ArtifactKind::SemanticDetail,
            store
                .stage_json(&json!({"source":"operator fixture"}))
                .unwrap(),
            "operator.lesson.source",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        let a = store_lesson(
            &store,
            &source,
            "a",
            vec![
                "skip early close".to_owned(),
                format!(
                    "{}unless the venue reopens",
                    "require corroboration; ".repeat(80)
                ),
            ],
            vec![],
        );
        let b = store_lesson(
            &store,
            &source,
            "b",
            vec!["skip a trading halt".to_owned()],
            vec![reference(&a.artifact)],
        );
        let c = store_lesson(&store, &source, "c", vec![], vec![]);
        let big_exclusion = format!(
            "{}when the final venue is unknown",
            "require verified price; ".repeat(11_000)
        );
        let d = store_lesson(
            &store,
            &source,
            "d",
            vec![big_exclusion],
            vec![reference(&c.artifact)],
        );

        let catalogue = ActiveResearchCatalogue::install(&store, now).unwrap();
        let purpose = ContractPurpose::new(RESEARCH_SYNTHESIZER_RECIPE_ID).unwrap();
        let active = store.active_contract(&purpose).unwrap().unwrap().contract;
        let candidate = catalogue
            .install_synthesizer_lesson_projection_candidate(&store, now)
            .unwrap();
        assert_eq!(
            store.active_contract(&purpose).unwrap().unwrap().contract,
            active
        );
        assert_eq!(
            candidate.contract.prompt.version,
            SYNTHESIZER_LESSON_PROJECTION_CONTRACT_VERSION
        );
        assert_eq!(
            candidate.contract.prompt.governance,
            active.prompt.governance
        );
        assert_eq!(candidate.contract.context, active.context);
        let revised = catalogue
            .contracts
            .with_installed_candidate(candidate.clone())
            .unwrap();
        let (request, manifest) =
            synthesizer_request(&store, revised, &candidate.contract.contract_hash).await;
        let context = request["context"].as_array().unwrap();
        let lesson_bodies = context
            .iter()
            .filter(|entry| entry["class"] == "lesson_projection")
            .collect::<Vec<_>>();
        assert_eq!(lesson_bodies.len(), 2);
        for (stored, expected_conflicts) in [(&a, vec![]), (&b, vec![reference(&a.artifact)])] {
            let id = json!(stored.artifact.artifact_id);
            let body = lesson_bodies
                .iter()
                .find(|entry| entry["metadata"]["document_id"] == id)
                .unwrap();
            assert_eq!(
                body["value"],
                json!({"projection_version":1,"title":stored.lesson.title,
                "statement":stored.lesson.statement,"recommended_behavior":stored.lesson.recommended_behavior,
                "scope":stored.lesson.scope,"exclusions":stored.lesson.exclusions,"conflicts_with":expected_conflicts})
            );
            let selection = manifest
                .selections
                .iter()
                .find(|selection| selection.artifact.artifact_id == stored.artifact.artifact_id)
                .unwrap();
            let actual_bytes = serde_json::to_vec(&body["value"]).unwrap().len() as u64;
            assert_eq!(selection.projected_bytes, Some(actual_bytes));
            assert_eq!(
                selection.estimated_tokens,
                estimate_tokens_from_bytes(actual_bytes)
            );
            assert_eq!(selection.artifact.kind, ArtifactKind::Lesson);
        }
        for omitted in [&c, &d] {
            assert!(!manifest
                .selections
                .iter()
                .any(|selection| selection.artifact.artifact_id == omitted.artifact.artifact_id));
        }

        let (old_request, old_manifest) =
            synthesizer_request(&store, catalogue.contracts, &active.contract_hash).await;
        assert!(!old_request["context"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["class"] == "lesson_projection"));
        assert!(old_manifest
            .selections
            .iter()
            .any(|selection| selection.artifact.artifact_id == a.artifact.artifact_id));
        assert_eq!(
            store.lesson(&a.lesson.lesson_id).unwrap().unwrap().lesson,
            a.lesson
        );
    }

    #[derive(Clone, Copy)]
    enum LessonReplayMutation {
        RemoveBody,
        ChangeItemType,
    }

    struct LessonSubmitModel {
        adapter: ModelClientAdapter,
        replay_mutation: Option<LessonReplayMutation>,
    }

    impl AgentModel for LessonSubmitModel {
        fn capability_snapshot(&self) -> ModelCapabilitySnapshot {
            self.adapter.capability_snapshot()
        }

        fn turn<'a>(&'a self, request: AgentModelRequest) -> BoxFuture<'a, Result<AgentModelTurn>> {
            Box::pin(async move {
                let fresh = request.continuation.is_none();
                let mut turn = self.adapter.turn(request).await?;
                if let Some(mutation) = self.replay_mutation.filter(|_| fresh) {
                    let mut replay = serde_json::to_value(&turn.continuation)?;
                    let first = &mut replay["items"][0];
                    match mutation {
                        LessonReplayMutation::RemoveBody => {
                            let mut input: Value =
                                serde_json::from_str(first["content"].as_str().unwrap())?;
                            input["context"]
                                .as_array_mut()
                                .unwrap()
                                .retain(|item| item["class"] != "lesson_projection");
                            first["content"] = json!(serde_json::to_string(&input)?);
                        }
                        LessonReplayMutation::ChangeItemType => {
                            first["type"] = json!("function_call_output");
                        }
                    }
                    // Leave fixture_input intact. Attribution must inspect the actual replay instead.
                    turn.continuation = serde_json::from_value(replay)?;
                }
                Ok(turn)
            })
        }
    }

    struct InterruptAfterSubmitRejection;

    impl akzio_model::ModelAuditSink for InterruptAfterSubmitRejection {
        fn record_model(
            &self,
            _context: &akzio_model::ModelAuditContext,
            kind: &str,
            _payload: Value,
        ) -> std::result::Result<(), String> {
            if kind == "rust.agent.submit_rejected" {
                return Err("interrupt after durable SubmitRejected".into());
            }
            Ok(())
        }
    }

    /// WHY THIS TEST EXISTS (Task #34): only the exact accepted Submit carrying the full body can count as a Lesson presentation.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn accepted_synthesizer_submit_authorizes_only_presented_lesson_usage() {
        verify_synthesizer_lesson_usage(&[
            "omitted",
            "forged_replay",
            "wrong_item_type",
            "unused",
            "fresh",
            "retry",
            "repair",
        ])
        .await;
    }

    /// WHY THIS TEST EXISTS (Task #34): a retried Attempt must verify the full user input replayed from the previous Manifest.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn cross_attempt_continuation_keeps_the_presented_lesson() {
        verify_synthesizer_lesson_usage(&["recovered_retry"]).await;
    }

    /// WHY THIS TEST EXISTS (Task #34): a recovered metadata repair must bind the original result and the new accepted Turn.
    /// Do not weaken or delete this assertion.
    #[tokio::test]
    async fn cross_attempt_metadata_repair_keeps_the_result_lesson() {
        verify_synthesizer_lesson_usage(&["recovered_repair"]).await;
    }

    async fn verify_synthesizer_lesson_usage(cases: &[&str]) {
        let root = OwnedExperiment(
            std::env::temp_dir().join(format!("akzio-lesson-submit-{}", RunId::new())),
        );
        let store = Store::open(&root.0).unwrap();
        let now = Utc::now();
        let source = Artifact::new(
            ArtifactKind::SemanticDetail,
            store
                .stage_json(&json!({"source":"lesson fixture"}))
                .unwrap(),
            "operator.lesson.source",
            ArtifactLifecycle::Canonical,
            ArtifactProvenance {
                source_family: "akzio.operator".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        let visible = store_lesson(
            &store,
            &source,
            "shown",
            vec!["exclude early closes".to_owned()],
            vec![],
        );
        let hidden = store_lesson(
            &store,
            &source,
            "omitted",
            vec!["restatement ".repeat(22_000)],
            vec![],
        );
        let catalogue = ActiveResearchCatalogue::install(&store, now).unwrap();
        let candidate = catalogue
            .install_synthesizer_lesson_projection_candidate(&store, now)
            .unwrap();
        let catalogue = catalogue
            .contracts
            .with_installed_candidate(candidate.clone())
            .unwrap();
        let mut accepted_presentations = 0;
        for &case in cases {
            let lesson = if case == "omitted" { &hidden } else { &visible };
            let lease_duration = if case == "recovered_repair" {
                Duration::seconds(5)
            } else {
                Duration::minutes(10)
            };
            let (mut runtime, mut claimed) = attempt_with_contract_and_lease(
                &store,
                catalogue.clone(),
                RESEARCH_SYNTHESIZER_RECIPE_ID,
                "Synthetic accepted Submit with no directional signal.",
                Some(&candidate.contract.contract_hash),
                lease_duration,
            )
            .unwrap();
            let closes = (1..=8)
                .map(|day| {
                    (
                        format!("2030-01-{day:02}"),
                        json!(format!("2030-01-{day:02}T21:00:00Z")),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            let calendars: Vec<_> = Asset::EXECUTABLE
                .into_iter()
                .map(|asset| {
                    let calendar = persist(
                        &store,
                        &claimed.permit,
                        ArtifactKind::NormalizedEvidence,
                        "evidence.normalize",
                        &json!({"source":"alpaca","resource":format!("bars:{}:1d",asset.symbol()),
                        "value":{"forecast_session_closes":closes}}),
                        vec![],
                    )
                    .unwrap();
                    reference(&calendar)
                })
                .collect();
            let (mut draft, _) = case_material(0, &calendars[0]);
            draft["evidence"] = json!([]);
            draft["hard_blockers"] = json!(["missing_evidence"]);
            for forecast in draft["forecasts"].as_array_mut().unwrap() {
                let thesis = forecast["thesis"].as_object_mut().unwrap();
                thesis.remove("thesis_valid_until");
                thesis.remove("expected_holding_period_days");
            }
            for basis in draft["numeric_basis"].as_array_mut().unwrap() {
                basis["inputs"] = json!([{"artifact_id":calendars[0].artifact_id}]);
            }
            draft["applied_learning_refs"] = json!([{"artifact_id":lesson.artifact.artifact_id}]);
            let deliberation = json!({"selected_path":"Conservative research allocation.","alternatives":[],
                "alternative_match_ppm":[],"uncertainties":[],"uncertainty_weight_ppm":[],
                "basis_artifact_ids":[calendars[0].artifact_id],"confidence_ppm":1_000_000});
            let arguments = json!({"result":draft,"deliberation":deliberation});
            let response = |call_id, arguments: &Value| {
                json!({
                    "output":[{"type":"function_call","name":"submit_result","call_id":call_id,
                        "arguments":serde_json::to_string(arguments).unwrap()}],
                    "usage":{"input_tokens":400,"output_tokens":1000},
                })
            };
            let mut initial = arguments.clone();
            let mut correction = arguments.clone();
            if matches!(
                case,
                "retry" | "recovered_retry" | "forged_replay" | "wrong_item_type" | "unused"
            ) {
                initial["result"]["hard_blockers"] = json!([]);
            } else if matches!(case, "repair" | "recovered_repair") {
                initial["deliberation"]["confidence_ppm"] = json!(999_000);
                correction = json!({"deliberation": arguments["deliberation"]});
            }
            if case == "unused" {
                correction["result"]["applied_learning_refs"] = json!([]);
            }
            let fixture = LessonSubmitModel {
                adapter: ModelClientAdapter::new(ModelClient::fixture_sequence([
                    response("lesson-submit", &initial),
                    response("lesson-retry", &correction),
                ])),
                replay_mutation: match case {
                    "forged_replay" | "unused" => Some(LessonReplayMutation::RemoveBody),
                    "wrong_item_type" => Some(LessonReplayMutation::ChangeItemType),
                    _ => None,
                },
            };
            let mut previous = None;
            if matches!(case, "recovered_retry" | "recovered_repair") {
                let context = akzio_model::ModelAuditContext {
                    run_id: claimed.run_id.0.clone(),
                    phase: "research".into(),
                    task_id: Some(claimed.permit.task_id.0.clone()),
                    attempt_id: Some(claimed.permit.attempt_id.0.clone()),
                    call_id: None,
                    retry_index: None,
                };
                let result = akzio_model::RUN_AUDIT_SINK
                    .scope(
                        std::sync::Arc::new(InterruptAfterSubmitRejection),
                        akzio_model::MODEL_AUDIT_CONTEXT.scope(
                            context,
                            runtime.run(
                                &claimed.permit,
                                &claimed.node,
                                calendars.clone(),
                                &fixture,
                                Utc::now(),
                            ),
                        ),
                    )
                    .await;
                assert!(
                    matches!(result, Err(ResearchError::RunLogUnavailable(message))
                    if message == "interrupt after durable SubmitRejected")
                );
                let events = store
                    .attempt_events(
                        &claimed.run_id,
                        &claimed.permit.task_id,
                        &claimed.permit.attempt_id,
                    )
                    .unwrap();
                let turns = events
                    .iter()
                    .filter(|event| {
                        event.lifecycle_kind().unwrap() == LifecycleEventType::AgentTurnCompleted
                    })
                    .collect::<Vec<_>>();
                let [turn] = turns.as_slice() else {
                    panic!("expected one durable original Turn");
                };
                let original = store.artifact(turn.artifact_id.as_ref().unwrap()).unwrap();
                assert!(events.iter().any(|event| event.lifecycle_kind().unwrap()
                    == LifecycleEventType::StageAcceptanceRecorded));
                let now = Utc::now();
                let (claim_time, relation_kind) = if case == "recovered_retry" {
                    assert!(matches!(
                        store.retry_task(&claimed.permit, now, now).unwrap(),
                        akzio_store::RetryTaskResult::Requeued
                    ));
                    (now, AttemptRelationKind::Retry)
                } else {
                    let active = store
                        .workflow_snapshot(&claimed.run_id)
                        .unwrap()
                        .tasks
                        .into_iter()
                        .find_map(|task| task.active_attempt)
                        .unwrap();
                    assert_eq!(active.permit, claimed.permit);
                    if let Ok(wait) =
                        (active.lease_until + Duration::milliseconds(5) - Utc::now()).to_std()
                    {
                        tokio::time::sleep(wait).await;
                    }
                    let recovery_time = Utc::now();
                    assert_eq!(store.recover_expired_tasks(recovery_time).unwrap(), 1);
                    (recovery_time, AttemptRelationKind::Recovery)
                };
                let next = store
                    .claim_next_task_for_workload(
                        "research-quality-recovered",
                        claim_time,
                        Duration::minutes(10),
                        TaskWorkload::Any,
                    )
                    .unwrap()
                    .unwrap();
                let relation = store
                    .attempt_relation(&next.permit.attempt_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(relation.relation, relation_kind);
                assert_eq!(relation.parent_attempt_id, claimed.permit.attempt_id);
                assert_eq!(next.run_id, claimed.run_id);
                assert_eq!(next.permit.task_id, claimed.permit.task_id);
                assert_eq!(next.permit.contract_hash, claimed.permit.contract_hash);
                previous = Some(original);
                claimed = next;
                runtime =
                    AgentRuntime::new(store.clone(), catalogue.clone(), Duration::minutes(10));
            }
            let proposal = runtime
                .run(
                    &claimed.permit,
                    &claimed.node,
                    calendars,
                    &fixture,
                    Utc::now(),
                )
                .await;
            if matches!(case, "omitted" | "forged_replay" | "wrong_item_type") {
                let error = proposal.expect_err(case);
                assert!(
                    matches!(&error, ResearchError::InvalidOutput(message) if
                    if case != "omitted" {
                        message == "Lesson was not presented to the result-producing Submit"
                    } else {
                        message.contains("allowed enum value") || message.contains("outside this attempt's Manifest")
                    }),
                    "{case}: {error:?}"
                );
                assert_eq!(
                    store
                        .lesson_usage(&lesson.lesson.lesson_id)
                        .unwrap()
                        .context_manifests,
                    0
                );
                store
                    .commit_attempt(&claimed.permit, &[], TaskStatus::Failed, Utc::now())
                    .unwrap();
                continue;
            }
            let proposal = proposal.unwrap_or_else(|error| panic!("{case}: {error:?}"));
            let note = proposal
                .source_refs
                .iter()
                .find(|reference| reference.kind == ArtifactKind::DeliberationNote)
                .unwrap();
            let note = store.artifact(&note.artifact_id).unwrap();
            let note: Value =
                serde_json::from_slice(&store.read_blob(&note.blob).unwrap()).unwrap();
            let submit = &note["accepted_synthesizer_submit"];
            let result_turn: ArtifactRef =
                serde_json::from_value(submit["result_turn"].clone()).unwrap();
            let accepted_turn: ArtifactRef =
                serde_json::from_value(submit["accepted_turn"].clone()).unwrap();
            let trace = |reference: &ArtifactRef| -> Value {
                let turn = store.artifact(&reference.artifact_id).unwrap();
                serde_json::from_slice(&store.read_blob(&turn.blob).unwrap()).unwrap()
            };
            if matches!(case, "retry" | "recovered_retry" | "unused") {
                let request = &trace(&result_turn)["domain_request"];
                assert_eq!(request["context"], json!([]));
                assert_eq!(request["continuation"]["items"][0]["role"], "user");
            }
            if let Some(original) = previous {
                let original_ref = reference(&original);
                let original_input = &trace(&original_ref)["domain_request"];
                let accepted_input = &trace(&accepted_turn)["domain_request"];
                assert_ne!(
                    original_input["manifest_artifact_id"],
                    accepted_input["manifest_artifact_id"]
                );
                for key in ["read_grant_identity", "context_materialization_identity"] {
                    assert!(original_input[key].is_string());
                    assert_eq!(original_input[key], accepted_input[key]);
                }
                if case == "recovered_repair" {
                    assert_eq!(result_turn, original_ref);
                } else {
                    let input: Value = serde_json::from_str(
                        accepted_input["continuation"]["items"][0]["content"]
                            .as_str()
                            .unwrap(),
                    )
                    .unwrap();
                    assert_eq!(
                        input["context_manifest"],
                        original_input["manifest_artifact_id"]
                    );
                    assert_eq!(input["context"], original_input["context"]);
                    assert_eq!(input["objective"], original_input["objective"]);
                }
            }
            if matches!(case, "repair" | "recovered_repair") {
                assert_ne!(result_turn, accepted_turn);
                assert_eq!(
                    trace(&accepted_turn)["domain_request"]["context"][0]["type"],
                    "deliberation_repair"
                );
            } else {
                assert_eq!(result_turn, accepted_turn);
            }
            let manifest_ref = proposal
                .source_refs
                .iter()
                .find(|reference| reference.kind == ArtifactKind::ContextManifest)
                .unwrap();
            let manifest = store.artifact(&manifest_ref.artifact_id).unwrap();
            assert_eq!(
                store
                    .lesson_usage(&lesson.lesson.lesson_id)
                    .unwrap()
                    .context_manifests,
                accepted_presentations
            );
            store
                .commit_attempt(
                    &claimed.permit,
                    std::slice::from_ref(&proposal),
                    TaskStatus::Succeeded,
                    Utc::now(),
                )
                .unwrap();
            let expected = if case == "unused" {
                Default::default()
            } else {
                accepted_presentations += 1;
                [reference(&lesson.artifact)].into()
            };
            assert_eq!(
                store
                    .proposal_lesson_presentation(&proposal, &manifest)
                    .unwrap(),
                Some(expected)
            );
            assert_eq!(
                store
                    .lesson_usage(&lesson.lesson.lesson_id)
                    .unwrap()
                    .context_manifests,
                accepted_presentations
            );
        }
    }

    #[tokio::test]
    async fn synthetic_quality_cases_run_inside_cargo_without_a_model_or_broker() {
        let root = OwnedExperiment(
            std::env::current_dir()
                .unwrap()
                .join(".akzio")
                .join(format!("cargo-quality-{}", RunId::new())),
        );
        let report = verify(&root.0, "offline", None, None).await.unwrap();
        let results = report["results"].as_array().unwrap();
        assert_eq!(results.len(), 12);
        assert!(results.iter().all(|row| row["phase"] == "offline"));
        assert!(results.iter().all(|row| row["synthetic"] == true));
        let store = Store::open_existing(root.0.join("store")).unwrap();
        assert!(store.debug_environment().unwrap().is_none());
        assert!(store.active_decision_policy().unwrap().is_none());
    }
}
