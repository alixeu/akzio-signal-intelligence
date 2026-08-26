# Akzio Learning Operations Plan

本文件是经验系统的操作规程和完成标准。它把“人工加入经验”“系统如何使用经验”“经验过多时如何处理”“如何组织经验”落实为可执行的 Store/CLI/审核流程。

## 0. 初始缺口与修复状态

| 初始缺口 | 当前修复 | 验收证据 |
| --- | --- | --- |
| `Experience` 和可复用规则混为一谈 | 新增独立 `Lesson` 类型和生命周期 | `akzio-domain/src/lesson.rs`、domain tests |
| 没有人工录入、审核和撤回流程 | `lesson` CLI + Draft/Active/Contested/Retired | CLI add/approve/list/show/usage 流程 |
| 模型看见经验后无法区分“采用”和“拒绝” | `applied_learning_refs` / `rejected_learning_refs`，DecisionGate 强制归因 | `akzio-execution` DecisionGate tests |
| 来源、替代和冲突关系不受 Store 约束 | immutable revision、source closure、`supersedes`、`conflicts_with` | Store lesson tests + Doctor |
| 经验会无限进入 prompt | scope/source-family 过滤和每类最多 4 条 | Context bound test |
| 经验使用效果不可观测 | ContextManifest/DecisionContext usage metrics | `lesson usage` CLI |
| outcome 复盘可能直接污染 active memory | 候选只生成 Draft，且只接受 sealed Paper T+1/T+3/T+5 | learning lifecycle tests |
| Doctor/CLI 读路径会误写只读 Store | 读取纯读，写入单独使用可写 Store | read-only Store tests + CLI Doctor |

## 1. 先区分两类记忆

| 类型 | 含义 | 谁产生 | 是否可人工直接改写 | 进入模型上下文的条件 |
| --- | --- | --- | --- | --- |
| `Experience` | 一次 Paper 决策及其 sealed outcome 的历史记录 | `akzio-learning` | 否；只能追加新历史 | canonical Paper 来源、权限允许、上下文预算允许 |
| `Lesson` | 从人工判断或复盘中提炼的可复用规则 | 操作者或 outcome worker | 只能追加 immutable revision | `Active` 生命周期、scope 匹配、来源闭包完整 |

人工想“加经验”时，应录入 `Lesson`，不要伪造 `Experience`。Debug、Replay、Paper Dry Run 不能产生 canonical learning。

## 2. 人工录入流程

### 2.1 创建 Draft

```bash
printf '%s\n' '{
  "title": "Opening volatility",
  "statement": "Require stronger evidence during the first quote window.",
  "rationale": "The opening window is noisy.",
  "recommended_behavior": "Wait for confirmation.",
  "assets": ["TQQQ"],
  "horizons": ["T1"],
  "regimes": [],
  "decision_stages": [],
  "supersedes": [],
  "conflicts_with": [],
  "confidence_ppm": 700000,
  "authored_by": "operator:alice"
}' | AKZIO_STORE_ROOT=outputs/akzio-v2-rebuild \
  cargo run -p akzio-cli -- store lesson add
```

`add` 原子写入 source artifact 和 Lesson，初始生命周期总是 `Draft`。输出中的 `lesson.lesson_id` 用于生命周期命令；输出中的 `artifact.artifact_id` 用于后续 `supersedes` 或 `conflicts_with`。

### 2.2 审核和维护

```bash
AKZIO_STORE_ROOT=outputs/akzio-v2-rebuild \
  cargo run -p akzio-cli -- store lesson list --lifecycle draft

AKZIO_STORE_ROOT=outputs/akzio-v2-rebuild \
  cargo run -p akzio-cli -- store lesson approve <lesson_id> \
  --actor operator:alice --reason "人工审核通过"

AKZIO_STORE_ROOT=outputs/akzio-v2-rebuild \
  cargo run -p akzio-cli -- store lesson show <lesson_id>

AKZIO_STORE_ROOT=outputs/akzio-v2-rebuild \
  cargo run -p akzio-cli -- store lesson usage <lesson_id>
```

有证据冲突时使用 `contest`；规则失效时使用 `retire`。不直接修改历史 JSON，不删除 canonical artifact。

## 3. 系统如何使用经验

```text
证据候选
  -> 推断 asset / horizon / regime / decision stage
  -> 选择 scope 匹配的 Active Lesson + 最近的 canonical Experience
  -> ContextManifest / ReadGrant
  -> 模型明确填写 applied_learning_refs 或 rejected_learning_refs
  -> Rust DecisionGate 校验来源闭包和影响归因
  -> ExecutionGate 继续执行权限、计划、幂等和 Paper 安全校验
  -> sealed Paper outcome / retrospective
  -> 候选 Lesson Draft，等待人工审核
```

模型不能因为“看见了”某条 Lesson 就自动宣称它影响了决策；只有 `applied_learning_refs` 才会记录为影响，`rejected_learning_refs` 用于保留明确排除理由。

## 4. 经验过多时的处理策略

### 在线选择

- Context 每次最多注入 4 条 Active Lesson 和 4 条 Experience。
- Lesson 先按 scope 和 source-family 过滤，再按置信度和稳定排序选择。
- Experience 只读取有界的最近候选，不把整个历史库塞进 prompt。
- `ContextManifest` 记录实际选择，`lesson usage` 提供使用次数和最近使用时间。

### 生命周期治理

- 低使用但仍有效：保留 Active，等待更多命中数据。
- 证据互相冲突：标记 `Contested`，补充 `conflicts_with` 和审核理由。
- 已被更好规则替代：创建新 revision，使用 `supersedes`，旧 revision 保留审计历史。
- 已失效：`Retired`，不再进入上下文，但不删除来源。

### 长期容量

canonical 历史不做破坏性清理；容量控制发生在“选择进入 Context”的边界。需要冷归档时，只能新增可验证的归档/索引层，不能改变 Store 的 immutable history、source closure 或 replay 结果。

## 5. Outcome-derived Lesson 晋升

`Retrospective.lesson_candidates` 只会物化为 `OutcomeDerived + Draft` Lesson。只有满足以下条件后才能人工批准并进入 `Active`：

1. 运行是 canonical Paper；
2. T+1、T+3、T+5 outcome 都已 sealed；
3. fresh paired outcomes 达到当前 evaluation policy 的最低样本数；
4. 没有未解决的 source closure、冲突或 policy blocker；
5. 操作者确认 scope、推荐行为和排除条件。

质量下降时走 `Contested -> Retired` 或 policy rollback，不覆盖旧记录。

## 6. 每次变更的验收清单

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --workspace --all-targets
cargo test --workspace
cargo run -p akzio-cli -- run fixture-debug
cargo run -p akzio-cli -- store doctor
```

最后两条命令应在新 Store Root 或已与当前 canonical contract 对齐的 Store Root 上执行。fixture 只能证明离线链路；真实 Paper、真实 T+1/T+3/T+5 和最终人工上线审批必须单独记录，不能互相替代。

## 7. 设计依据

- [目标与来源调研](../architecture/2026-08-09-v2-goal-source-research.md)：Paper-only、来源闭包、可回滚学习和 Rust 权威边界。
- [Paper Runbook](./AKZIO_V2_PAPER_RUNBOOK.md)：Paper-only、审批、冻结、reconciliation 和 outcome 证据边界。
- [Prompt 与 Debate 调研](../architecture/2026-08-11-v2-prompt-debate-research.md)：OpenAI/Anthropic tool-use 经验与受控评测、paired outcome、canary、rollback 约束。
