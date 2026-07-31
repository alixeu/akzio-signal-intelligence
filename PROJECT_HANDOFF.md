# Akzio Signal Intelligence 项目交接

> 用途：把本文档直接交给新的 Codex/开发会话，让它可以在不重新猜测架构的情况下继续工作。
>
> 更新时间：2026-07-30
>
> 工作目录：`/Users/alixeu/project/akzio-signal-intelligence`

## 0. 新会话启动指令（可直接复制）

```text
你正在继续维护 /Users/alixeu/project/akzio-signal-intelligence。

先阅读项目根目录的 AGENTS.md 和 PROJECT_HANDOFF.md，不要 reset、checkout 或覆盖现有未提交改动。这个项目是 Rust workspace，生产持久化是 FileStore，当前核心设计是：

1. analysis_universe 是一次角色对话的完整分析集合；当前是 QQQ、SOXX、VIX。
2. investable_assets 是可以做 rating、交易、仓位和 Decision 的集合；当前是 QQQ、SOXX。
3. VIX 只作为 regime/context signal，不能进入交易决策资产 map。
4. Phase 1 每个 analyst role 一次组合对话；Phase 3、4、6 各一次组合对话；Phase 5 每个 risk stance 一次组合对话；Phase 2 仍按 topic 做 Bull/Bear/Controller fork。
5. 每个业务角色输出自由文字，之后由对应 Phase Summary 提取固定字段；Rust 写一个聚合 Index，Detail 保存一次完整自由文字。
6. Phase 7/8 是 Rust-owned；Phase 7 按 QQQ/SOXX 分别应用 Trader/Risk/Portfolio 约束，VIX 只决定风险环境。

继续工作前先执行：

rtk git status --short
rtk cargo test
rtk cargo clippy --workspace --all-targets
rtk cargo run -p orchestrator-cli --bin orchestrator-prompt-lint

如果要做 live/debug 验证，先确认 LLM_GATEWAY_BASE_URL、LLM_GATEWAY_API_KEY、ALPACA_API_KEY、ALPACA_API_SECRET 已配置，并使用新的 --store-root；不要把 mock 成功当成 live 成功。先检查输入刷新、进程状态、outputs/debug、manifest 和最终 Index。
```

## 1. 项目是什么

这是一个 Rust 多阶段市场研究和 ETF 配置工作流。当前配置研究：

```yaml
analysis_universe: [QQQ, SOXX, VIX]
allocation:
  investable_assets: [QQQ, SOXX]
```

这两组列表不是重复配置：

| 名称 | 当前值 | 含义 |
|---|---|---|
| `analysis_universe` | `QQQ, SOXX, VIX` | 每个研究角色在同一轮对话中必须同时看到和比较的对象 |
| `investable_assets` | `QQQ, SOXX` | 可以产生 rating、Buy/Sell/Hold、风险上限、目标权重和 Decision 的资产 |
| context-only asset | `VIX` | 只解释风险环境、波动和传导，不能成为持仓或订单对象 |

启动时会校验：可投资资产非空、属于分析集合、不重复，且 regime signal（默认 VIX）不能成为可投资资产。

## 2. 当前阶段拓扑

```text
Phase 0  历史 Outcome 反思（Rust 选任务，模型只读历史来源）
   ↓
Phase 1  Technical Analyst 一次组合对话 + News/Macro Analyst 一次组合对话
   ↓
Phase 2  共享 Warm-up → Topic Generator → 每个 topic 的 Bull/Bear/Controller
   ↓
Phase 3  Research Manager 一次组合对话，输出 QQQ/SOXX decisions
   ↓
Phase 4  Trader 一次组合对话，输出 QQQ/SOXX plans
   ↓
Phase 5  Aggressive / Neutral / Conservative 各一次组合风险审查
   ↓
Phase 6  Portfolio Manager 一次组合对话，输出 QQQ/SOXX per_asset
   ↓
Phase 7  Rust inverse-vol + VIX + 逐资产约束
   ↓
Phase 8  Rust final decision Index（只覆盖 QQQ/SOXX）
```

没有 topic 时，Phase 2 仍会完成 warm-up、topic-generation 和 deterministic reducer；有 topic 时，topic 内部才会继续多空辩论轮次。

## 3. 代码入口和职责

| 文件 | 作用 |
|---|---|
| `crates/orchestrator-workflow/src/exec/mod.rs` | 主工作流、Phase 1–8 调度、聚合结果写入 state、Phase 8 输出 |
| `crates/orchestrator-workflow/src/orchestration/lifecycle.rs` | run ID、分析集合、可投资集合和启动范围校验 |
| `crates/orchestrator-workflow/src/orchestration/render.rs` | Prompt 变量和跨 Phase 控制上下文渲染 |
| `crates/orchestrator-workflow/src/orchestration/role_jobs.rs` | RoleJob、FileStore Index 只读绑定、工具范围和 LLM 运行 |
| `crates/orchestrator-workflow/src/orchestration/summary_store.rs` | Summary JSON 校验、Index 创建、Detail 写入和最终归档 |
| `crates/orchestrator-workflow/src/orchestration/allocation.rs` | VIX/技术快照投影、逐资产 Trader/Risk 上限、Rust allocation |
| `crates/orchestrator-workflow/src/orchestration/summary_units.rs` | 只保留聚合 Summary Index 的短 ID 生成函数；旧的逐 ticker planner 已删除 |
| `crates/orchestrator-llm/src/tools/index_tools.rs` | 模型只读 `read_indexes` / `read_index_details` 工具 |
| `crates/orchestrator-llm/src/tools/record_phase2_context.rs` | Phase 2 Rust-owned topic、round、round_num 和 debate context 工具 |
| `crates/orchestrator-workflow/src/orchestration/input_snapshot_runtime.rs` | Phase 1 技术/Jin10 输入封存和 hash 绑定 |
| `prompts/common/components/ticker/component.md` | 一次对话的 analysis/investable/context-only 范围合同 |
| `prompts/phaseN/*.md` | 各阶段角色和 Summary 提示词 |
| `config/config.yaml` | 默认运行、LLM、输入、Phase 和 FileStore 配置 |

## 4. 持久化规则

运行中的目录可能临时包含 `state.json`、`sessions`、`drafts`、`artifacts`、`inputs` 和 debug 记录。成功完成 Phase 8 后会 compact，只保留：

```text
outputs/store/
├── runs/YYYY-MM-DD/qqq-soxx-vix-<md5-3>/
│   ├── manifest.json
│   └── index/
│       ├── phase1/idx-<md5-3>.json
│       ├── phase2/idx-<md5-3>.json
│       ├── phase3/idx-<md5-3>.json
│       ├── phase4/idx-<md5-3>.json
│       ├── phase5/idx-<md5-3>.json
│       ├── phase6/idx-<md5-3>.json
│       ├── phase7/idx-<md5-3>.json
│       └── phase8/idx-<md5-3>.json
└── knowledge/
```

每个 `idx-*.json` 归档同时包含 Index 和 Detail。多资产 Phase 的固定字段如下：

| Phase | 聚合字段 | 是否允许 VIX 进入决策 map |
|---|---|---|
| 1 | `per_ticker` | 可以，作为分析报告和环境上下文 |
| 3 | `decisions` | 不可以 |
| 4 | `plans` | 不可以 |
| 5 | `per_asset` | 不可以 |
| 6 | `per_asset` | 不可以 |
| 7/8 | `weights` / final decision | 不可以 |

正常完成后不应留下 `.lock`。系统假设一个 workflow writer，不使用文件锁并发协调。

经验系统仍然是 outcome-backed：当前预测、mock、未评分结果不能写正式 Decision/Outcome/Experience。Debug 使用隔离 namespace；`orchestrator.evaluation.enabled` 当前默认关闭。

## 5. 输入数据

技术数据直接放在：

```text
outputs/store/data/technical/qqq/day.csv
outputs/store/data/technical/qqq/3h.csv
outputs/store/data/technical/qqq/20min.csv
outputs/store/data/technical/soxx/...
outputs/store/data/technical/vix/...
```

Jin10 放在 `outputs/store/data/jin10/`。非 mock Phase 1 会先将这些输入封存到当前 run 的 FileStore input snapshot，并校验 hash；运行中直接修改原始 CSV 会导致读取失败。

## 6. 常用命令

### 基础验证

```bash
rtk cargo fmt --all
rtk cargo test
rtk cargo clippy --workspace --all-targets
rtk cargo run -p orchestrator-cli --bin orchestrator-prompt-lint
rtk git diff --check
```

### Mock 全流程

```bash
rtk cargo run -p orchestrator-cli --bin orchestrator-exec -- \
  --mock \
  --store-root /tmp/akzio-mock-store
```

Mock 只能验证 Rust 流程、Summary schema、Index/Detail 归档和 allocation，不证明外部 LLM、Alpaca、Yahoo 或 Jin10 可用。

### 输入刷新

```bash
rtk cargo run -p orchestrator-cli --bin orchestrator-ingest -- \
  technical-indicators \
  --symbols QQQ,SOXX,VIX \
  --start YYYY-MM-DD \
  --end YYYY-MM-DD \
  --intervals 1d,3h,20min \
  --sleep 0 \
  --timeout 20

rtk cargo run -p orchestrator-cli --bin orchestrator-ingest -- \
  jin10-flash --pages 2 --lookback-hours 24 --timeout 20
```

### Live/debug 全流程

```bash
rtk cargo run -p orchestrator-cli --bin orchestrator-exec -- \
  --debug \
  --store-root /tmp/akzio-debug-YYYYMMDD \
  --to-phase 8
```

用户常用的无参数命令仍然是：

```bash
cargo run -p orchestrator-cli --bin orchestrator-exec -- --debug
```

但是交接验证应优先使用新的 `--store-root`，避免旧 run 的 state、manifest 或半成品影响判断。Debug 输出在 `outputs/debug/`；FileStore run 仍在 `outputs/store` 或指定 root。

## 7. Live 验证顺序

不要看到某个 JSON 文件就判定阶段完成。按下面顺序检查：

1. 确认进程是否仍在运行：`ps` 检查 `orchestrator-exec`。
2. 检查 debug 的 `time.json`、`token.json` 和阶段目录。
3. 检查 FileStore `manifest.json` 的 status/current_phase/degraded/error。
4. 检查 sessions 的最后一个 `turn-*.jsonl` 是否有 terminal response。
5. 检查 Phase 1 是否同时存在 QQQ、SOXX、VIX 三者证据，而不是只看一个旧文件。
6. 检查 Phase 3/4/5/6 是否是一个 aggregate Index，且决策 map 只包含 QQQ、SOXX。
7. 检查 Phase 7 allocation 的 `weights` 是否没有 VIX，且总权重为 1。
8. 只有 Phase 8 Index、manifest completed 和 compact 完成，才算完整 run。

`end_turn=false` 或 `needs_follow_up=true` 只能说明模型还有工具/后续轮次，不能说明阶段失败或完成。真正的完成要看 terminal response、manifest 和最终 Index。

## 8. 已知边界和下一步

当前实现已经通过本地 mock 和 workspace 测试，但最近一次改造后的非 mock 全 Phase live run 尚未在本交接时完成验证。新会话优先做：

1. 使用新的临时 `--store-root` 执行 Phase 1 非 mock，确认三种资产都被同一 Analyst 对话读取。
2. 确认 `market_snapshot.vix` 来自 VIX daily 技术快照，而不是 `data_gap` fallback。
3. 继续到 Phase 3，确认 Research Manager 一次输出 QQQ/SOXX 两个 decision，VIX 只在 `regime_context`。
4. 继续到 Phase 6/7，确认每个风险上限分别作用于 QQQ/SOXX。
5. 若 live 失败，记录第一个真实阻断错误，不要用 mock/degraded 输出替代结论。

### 需要谨慎的事项

- 不要把 `analysis_universe` 改成只包含 QQQ/SOXX；VIX 必须继续作为上下文输入。
- 不要在 Phase 3/4/5/6 恢复 ticker 循环；那会重新制造多个独立对话和重复 Index。
- 不要让 VIX 出现在 `decisions`、`plans`、`per_asset`、`weights` 或 Phase 8 Decision。
- 不要把自由文字重新改成角色直接调用 finalize/write 工具；角色结束后由 Summary 编译，再由 Rust 写 Index。
- 不要用 `--from-phase` 跳过前置阶段，除非确认前置聚合 Index、Detail、hash 和 manifest 完整且没有旧的 partial state。
- 不要提交 `outputs/`、FileStore 数据、debug 输出、真实凭证或本地配置。
- 工作区可能有用户已有未提交文件；开始修改前先保存 `git status --short`，只改任务相关文件。

## 9. 交接验收标准

新会话完成后，至少应能给出以下证据：

```text
[ ] cargo test 通过
[ ] cargo clippy --workspace --all-targets 通过
[ ] prompt lint errors=0
[ ] mock Phase 1–8 通过
[ ] live/debug 的实际退出状态已确认
[ ] Phase 1 一个 technical 对话覆盖 QQQ/SOXX/VIX
[ ] Phase 3/4/5/6 没有按 ticker 启动重复业务对话
[ ] Phase 3/4/5/6 的决策 map 只包含 QQQ/SOXX
[ ] VIX 只出现在上下文/regime，不出现在 weights/Decision
[ ] Phase 7 权重总和为 1，且没有 VIX
[ ] Phase 8 完成后只保留 manifest + Index/Detail archive
```

## 10. 相关文档

- `AGENTS.md`：仓库级开发约束。
- `README.md`：当前架构、运行方式和持久化说明。
- `config/config.yaml`：实际默认配置。
- `prompts/common/components/ticker/component.md`：资产范围合同。
- `prompts/phase1/summary.md`、`prompts/phase3/summary.md`、`prompts/phase4/summary.md`、`prompts/phase5/summary.md`、`prompts/phase6/summary.md`：聚合 Summary 字段合同。

