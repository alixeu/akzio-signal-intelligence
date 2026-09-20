# 研究质量验证

研究质量的检查同时覆盖 Context、终稿评审、修订和 Lesson 召回。有效 JSON、有效 Rust 产物、语义上正确的评审和修订成功分别统计；其中任何一项不能代替真实行情、Paper 成交或 Outcome。

## 已实现的边界

| 环节 | 行为与证据 |
| --- | --- |
| Context | Manifest 创建时记录候选、选择结果、契约/来源/overlay 排除、原文/投影/token/条数预算排除及投影省略。导出时才发现的证据不回推历史可用性。 |
| Reviewer | Contract 69 / Prompt 38。17 个 scope 不变；拒绝项提供 1–3 个类型化 issue，通过项 issues 为空。问题 ID 由 Rust 根据 scope、category、field_path 计算，措辞不改变身份。 |
| 修订 | 已通过 forecast 的模型字段及 numeric_basis 不得顺带改写；Rust 仍绑定交易日历字段。配置及其 basis 允许随修复联动。全稿重新评审；连续两次拒绝的 scope、问题身份、引用和内容相同则停止，Decision 继续阻断。 |
| Lesson | 同一 SQL 快照分页扫描全部 Active heads，先检查 scope、治理有效期和使用量，再按 regime/stage/asset/horizon 匹配、更新时间和 ID 排序。最多四条；严格内容去重，显式冲突组整体入选或整体排除。 |
| 重验 | 已引用来源出现同资源、同来源族的新版本时，生成带精确引用的运行内建议。来源更新不是已证明矛盾，不改变 Active 生命周期，不根据单次亏损退役。 |
| App 与导出 | 同一个 Rust `ResearchAudit` 投影提供给 Observer 与 bundle。Workflow Inspector 显示问题、引用、选择和停止原因；Learning 展示当前运行的召回与重验建议，保留与正式 Outcome 学习的区别。 |

历史 Contract/CAS 不改写。旧 Review 缺少 issues 仍可读取；新规则按版本生效。Domain 10、Store 17 和 Outcome 63/35 保持不变。

## 固定离线目录

[24 项目录](../config/research-quality-cases.json) 分为 Context、Review、Lesson，各八项，使用稳定 ID 绑定到实际 Rust 测试。执行器保存命令、退出码、完整日志和每项结果；找不到测试、被忽略或失败均不算通过。

```bash
python3 scripts/verify_research_quality.py
```

额外回归验证第 25 份关键证据、预算耗尽、完整正式图、无进展停止后直接 DecisionGate 的拒绝，以及模型调用预算在重新打开 Store 后仍有效。固定目录不是整个 workspace 测试的替代品。

## 真实模型实验

```text
akzio debug verify-research-quality --out .akzio/<experiment> --phase offline
akzio --config <config> debug verify-research-quality --out .akzio/<experiment> --phase baseline
akzio --config <config> debug verify-research-quality --out .akzio/<experiment> --phase candidate
akzio debug verify-research-quality --out .akzio/<experiment> --phase report
```

baseline 必须由升级前保存的二进制执行，candidate 使用升级后的二进制。两者使用同一个实验 Store；`--phase baseline` 是实验标签，不会把当前代码切回旧版。report 只读取已有 Store，不读取模型配置或调用 provider。

实验使用人工构造、显式标记的研究材料：四个中性控制、八个有明确方法/数值/单位/来源矛盾的案例，再重复两个固定案例。它不声称知道未来收益的真值。candidate 的第 5、6、7、11 个案例预先安排 Synthesizer 修订及 Reviewer 复审；初次协议失败、修订被 Rust 拒绝或路由缺少匹配能力证明，都保留失败/缺失，不能算成一次成功修订。

所有模型 I/O 前先在 V2Store 预留调用。整个 Store 的上限固定为 40，包括能力探测、格式修复、重复和业务修订；未知完成状态不退回预算。能力探测保守预留三次。单任务输入上限 24000、输出上限 8000（包含 reasoning）、时间 180 秒。运行串行，报告保留模型响应、请求哈希和实际 telemetry。费用未知时不推算货币金额。

报告将 provider 的可见 assessment 与正式 Review 分开：模型回答可以已存在，但引用或 Schema 校验失败时仍没有有效业务产物。baseline 与 candidate 的协议有效率、误接受、误拒绝、缺失回答和修订结果分别展示。总体验收要求候选零关键误接受、误拒绝不高于基线、所有案例协议有效且四个修订均通过；缺失或预算耗尽不能被成功案例掩盖。

输出均在 `.akzio/`，包括 Store、二进制快照与报告，不提交 Git。此入口没有 Decision、Execution、Broker 或学习激活节点。

## 使用此次文章建议的方式

本次参考输入是用户提供的[文章一](https://ku.baidu-int.com/knowledge/HFVrC7hq1Q/_SKPgSwp2G/WjGjiS_SK_/aK7_4IYLpobCFs)和[文章二](https://ku.baidu-int.com/knowledge/HFVrC7hq1Q/_SKPgSwp2G/WjGjiS_SK_/TIc2OV1UdhxzUZ)。上述条目是针对本仓库的工程实现与验证标准，不把文章观点、测试材料或模型自述当成已经验证的投资效果。具体当次调用数量、模型结果与失败项保留在实验报告中。
