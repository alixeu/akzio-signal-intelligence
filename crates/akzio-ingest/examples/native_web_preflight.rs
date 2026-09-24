//! Real native-web transport probe. No Store access and no research acceptance.
// 文件导读：这是独立的真实 provider hosted-web 诊断示例，硬编码示例模型和
// 两句探测指令；它不使用正式研究 Contract/Context，也不证明新闻来源真实、
// 研究通过或 Paper 可执行。源码被 PromptSource 作为 bytes 纳入 Prompt 身份。
use akzio_model::{
    ModelClient, ModelInput, ModelRequest, ModelToolChoice, NativeWebPolicy, OpenAIResponsesClient,
};
use serde_json::json;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 网关地址和密钥来自环境变量；ModelClient 的 Result 用 ? 直接阻断未配置或无效的 capability probe。
    let client = ModelClient::OpenAIResponses(OpenAIResponsesClient::new(
        std::env::var("LLM_GATEWAY_BASE_URL")?,
        std::env::var("LLM_GATEWAY_API_KEY")?,
        "gpt-5.6-luna",
        "low",
    )?);
    // 默认策略限制 hosted web 工具；Required 只在 provider 请求中设置
    // tool_choice=required，实际是否出现 completed web_search_call 仍由
    // validate_provider_response 检查，不能仅凭请求参数断言已搜索。
    let policy = NativeWebPolicy::default();
    // respond 是异步模型调用；该示例只等待 provider 返回并检查策略，不写 Store，也不把结果提升为研究证据。
    let result = client
        .respond(ModelRequest {
            instructions: "使用提供的 hosted web search 一次，定位 FRED VIXCLS series。引用其官方页面。这是 capability probe，不是投资研究。\n"
                .into(),
            input: ModelInput::Fresh {
                text: "使用 web search 在 fred.stlouisfed.org 查找 VIXCLS。\n"
                    .into(),
            },
            max_output_tokens: 1000,
            reasoning_effort: None,
            tools: vec![policy.tool_definition()],
            tool_choice: ModelToolChoice::Required,
            fixture_key: None,
        })
        .await;
    match result {
        // 收到 provider response 就输出 response_received；policy_validation 的
        // Err 只进入 JSON 字段，main 仍可返回 Ok，必须逐项看校验结果。
        Ok(response) => println!(
            "{}",
            json!({"state":"response_received", "policy_validation":policy.validate_provider_response(&response.raw).map_err(|e|e.to_string()), "response_id":response.raw.get("id"), "actual_model":response.raw.get("model"), "usage":response.raw.get("usage"), "web_calls":response.raw.get("output").and_then(|v|v.as_array()).map(|items|items.iter().filter(|i|i.get("type").and_then(|v|v.as_str())==Some("web_search_call")).collect::<Vec<_>>())})
        ),
        // HTTP 错误把 provider 原始 body 写入 stdout；可能含敏感上下文，运行者
        // 须保护日志。BLOCKED 只表明本例没有受理成功响应，不是来源真值判断。
        Err(akzio_model::ModelError::Http { status, body }) => println!(
            "{}",
            json!({"state":"BLOCKED", "http_status":status.as_u16(), "provider_error":body})
        ),
        Err(error) => println!(
            "{}",
            json!({"state":"BLOCKED", "error_class":format!("{:?}", std::mem::discriminant(&error))})
        ),
    }
    Ok(())
}
