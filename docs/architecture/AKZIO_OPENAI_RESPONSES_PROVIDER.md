# OpenAI Responses Provider 边界

状态日期：2026-08-31。

## 支持矩阵

| Provider / 接口 | 实现状态 | capability 证据 | 说明 |
| --- | --- | --- | --- |
| OpenAI Responses 官方 endpoint | 已实现，等待真实验证 | static declared、unverified | `/responses`、SSE、reasoning items、encrypted continuation、native web、usage 与 citation 语义均由 `akzio-model` 处理 |
| 显式 OpenAI Responses 自定义 endpoint | 仅允许连接，能力默认关闭 | static declared minimal、unverified | 必须显式声明 provider；不会自动获得 tool、reasoning、continuation、native web 或 streaming capability |
| Anthropic Messages | 不支持 | 无 | 没有原生 adapter |
| xAI Responses 风格 endpoint | 不支持 | 无 | 没有经过验证的 adapter；相同路径或字段名称不构成语义兼容 |
| 未知 Provider | 拒绝 | 无 | 配置解析 fail closed |

离线 fixture 只证明本地编码、解析和状态转换行为，不是 Provider negotiation，也不是真实兼容验证。

## 配置

新配置必须明确选择 OpenAI Responses：

```toml
[model]
provider = "openai_responses"
base_url = "https://api.openai.com/v1"
model = "gpt-5.6-luna"
api_key = "$OPENAI_API_KEY"
reasoning_effort = "medium"
```

`OpenAIResponsesConfig`、`OpenAIResponsesRouteConfig` 和
`OpenAIResponsesClient` 是明确的 Provider 边界。`ModelConfig` 与
`ModelRouteConfig` 暂时保留为源码兼容别名，不代表 Provider-neutral
实现。

## 旧配置迁移

- 未声明 `provider` 且 `base_url = "https://api.openai.com/v1"` 的旧配置继续接受，并按 OpenAI Responses 处理。
- 未声明 `provider` 的自定义 gateway 配置有歧义，启动时 fail closed。迁移时必须增加 `provider = "openai_responses"`，再单独验证 gateway 的真实语义。
- `provider = "anthropic_messages"`、`provider = "xai_responses"` 或其他未知值会在配置解析阶段被拒绝。
- Observatory 初始化和写回配置时始终写入明确的 provider identity。

## Capability 证据等级

`ModelCapabilityBasis` 明确区分：

- `static_declared`：代码静态声明，未与 Provider 协商；
- `runtime_negotiated`：未来只有完成真实 handshake/conformance 后才可使用；
- `unknown`：没有能力证据。

当前实现只产生 `static_declared`，且 `verified = false`。持久化的
`ModelCapabilitySnapshot.source` 使用
`openai_responses_static_declared_unverified` 或
`custom_endpoint_unverified`，不能写成 negotiated 或 verified。

## 真实 Conformance 验证清单

在真实验证完成前，不得将 OpenAI 标记为 real-verified。最终验证至少应采集：

1. 官方 `/responses` 请求字段和拒绝未知字段的行为；
2. SSE event 顺序、分片、终止、失败和 idle timeout；
3. reasoning summary items 与 encrypted continuation 的回放；
4. function call ID、arguments 和 tool output continuation；
5. native web search action、sources、citation URL、excerpt 和 span；
6. input、cached input、output、reasoning usage 字段；
7. incomplete、refusal、429、5xx、连接失败和调用取消的错误分类；
8. `store=false` 下 stateless continuation 的稳定性；
9. model route、reasoning effort 和 capability snapshot 的一致性；
10. 自定义 gateway 不应在未经单独验证时提升 capability。
