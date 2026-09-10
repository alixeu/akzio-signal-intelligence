# Akzio 开发 Workflow

本文件统一开发助手的任务推进、验证和交付流程。应用的 Paper Workflow、受控模型工具和运行时审批仍按 [运行时契约](agent-runtime-contract.md) 执行。

## 1. 定位与推进

1. 依据当前请求和已有决策确定交付，检查 `git status --short`，定位相关源码、调用方和测试。只读取 [AGENTS.md](../AGENTS.md) 索引中与任务有关的章节。
2. 在现有模块边界内实现直接、完整的方案，连同必要的调用方和校验修改。普通细节自行决定；缺失事实先查，只有影响结果或权限的未决选择才提问。
3. 独立只读操作可批量并行；修改、依赖验证、授权和副作用按顺序处理。等待未完成操作的结果后再重试。获准委派时交给子任务独立工作，主任务负责整合到最终交付。
4. 按下面的分支验证。保留仍适用的结果；只有代码变化、失败或新疑点才重跑或扩大范围。不要为常规修改追加与行为无关的测试、强制访谈或额外审阅轮次。
5. 完成已授权的准备工作，再为确需批准的具体操作请求批准。待批准或外部依赖阻塞时继续其他必要工作，并给出可审阅结果和准确的剩余项。

## 2. 文档、指令与 Skill

只改 Markdown、AGENTS 或 Skill 指令时，检查实际差异、引用路径、触发条件与正文的一致性，以及是否改变明确的审批或业务约束。运行适用的文档检查，不因此启动 Rust/Core、API、Paper 或 T+5 流程。

```bash
git diff --check
bash scripts/check_markdown_links.sh
```

该链接脚本扫描本地 Markdown；若本地生成资料导致失败，区分本次改动与已有文件，并对本次文件给出可核验的检查结果，不将全量失败表述为通过。

修改 Skill 时使用其现有验证器检查 frontmatter；检查相关引用文件和 UI 元数据是否存在矛盾。现有扩展字段与验证器不兼容时，对照修改前结果，不通过删除调用参数或放宽验证器来伪造通过。指令静态检查不等于真实模型行为验证。

## 3. 代码修改的离线检查

先运行所修改 crate 的具体测试，再完成项目原有的 workspace 检查。不要只测实现细节或用实现本身计算预期结果；验证受影响的外部行为。用户已确认的测试接口可复用；新增接口仍遵循适用 Skill 的明确确认要求。

CI 的实际配置见 [ci.yml](../.github/workflows/ci.yml)。与 CI 对齐的命令如下，按当前任务有效的已有结果可复用：

```bash
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
```

格式问题只修本任务相关内容，再检查；不得为了通过 `cargo fmt --all` 改动无关的脏工作区。独立的既有失败要说明，不顺带改掉或掩盖。CI 中的安全审计和 SBOM jobs 保留原定义。

原有 fixture 和 Store Doctor 验证要求保留，但必须使用正确的隔离环境：

```bash
mkdir -p .akzio
task_fixture_root="$(mktemp -d "$PWD/.akzio/fixture.XXXXXX")"
AKZIO_STORE_ROOT="$task_fixture_root" cargo run --locked -p akzio-cli -- --config config/task2-fixture.toml run fixture-debug
```

`fixture-debug` 使用离线 `PaperDryRun` fixture，内部启动临时服务、请求 Store Doctor 并关闭服务；该过程不是正式 Debug、真实 LLM、Paper 下单或完整三期限研究验收。源码入口见 [run_commands.rs](../crates/akzio-cli/src/cli/run_commands.rs)。不得把“请求过 Doctor”自动等同于检查过其完整报告。

独立 `store doctor` 是认证 HTTP 客户端，需要与所选配置一致且已经获准运行的隔离 daemon。执行前先确认配置、endpoint、Store 和该服务的状态，再使用显式配置调用，并检查返回报告：

```text
cargo run --locked -p akzio-cli -- --config <对应隔离配置路径> store doctor
```

fixture 退出后不能假设 daemon 仍在。缺少适用服务时报告独立 Doctor 验证受阻，继续其他检查；不得为凑齐结果改连正式 Store、启用 auto_paper 或额外调用模型。具体入口与边界见 [Task 2 Debug](task2-debug.md)。

## 4. Observatory App

SwiftUI 或 App/Core 接口修改还需运行与改动相关的 Swift 检查。当前可用的检查入口为 `swift run --package-path apps DebugContractChecks`；完整 Bundle 构建不能替代实际 UI 验收。

macOS 分发版本统一通过 `scripts/update_app_and_submit_debug.sh` 编译、打包与签名。Bundle 必须包含 `Contents/MacOS/akzio-core`。未取得构建产物删除授权时，默认保留构建产物，使用工作区内新的 Bundle 目标，例如：

```bash
AKZIO_PRESERVE_BUILD_PRODUCTS=1 AKZIO_APP_BUNDLE="$PWD/apps/dist/akzio-review-$(date -u +%Y%m%dT%H%M%SZ).app" bash scripts/update_app_and_submit_debug.sh
```

已有目标不可覆盖；清理构建目录、删除旧 Bundle 仍须明确授权。源码入口见 [打包脚本](../scripts/update_app_and_submit_debug.sh) 和 [构建脚本](../apps/Scripts/build_app.sh)。

普通 Observatory Core 使用 `~/.akzio/store` 与 `~/.akzio/config.toml`；Debug Core 必须使用新隔离 Store。`Paper scheduler waiting: broker market is closed` 是正常非交易状态，不视为服务异常。

## 5. 完成与证据

完成当前任务约定的交付和适用强制检查后收尾；不以继续执行无关检查代替交付。若必要验证缺失，明确写出已完成、被阻塞的步骤及其依赖，不宣称全部完成。

| 状态 | 可作出的结论 |
|---|---|
| `implemented` | 指定修改已经写入 |
| `offline-verified` | 实际运行的本地检查通过，注明覆盖范围 |
| `real-Paper-verified` | 本次获准的真实 Alpaca Paper 验证通过 |
| `outcome/learning-verified` | 本次实际跨交易日 Outcome/学习验收通过 |

真实 LLM 证据应另行注明调用、Run、配置和范围，不与 fixture 或 Paper 订单验证混为一谈。四级状态是证据标签，不要求文档或普通修复任务无条件执行到 T+5；模型推荐也不能替代原运行时审批、冻结预算或 Gate。

## 指导依据

2026-09-11 核对 [GPT-6 Astra 官方指导](https://developers.openai.com/api/docs/guides/latest-model)，采用明确任务完成条件、减少无效澄清、保留审批、复用有效验证的做法。按需加载资料与精确 Skill 触发参考 [官方 Skill 指南](https://learn.chatgpt.com/docs/build-skills)，指令入口分层参考 [AGENTS.md 指南](https://learn.chatgpt.com/docs/agent-configuration/agents-md)。这些是开发流程选择，不表示已证明模型性能提升或迁移了应用模型。
