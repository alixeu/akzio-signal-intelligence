//! Real native-web transport probe. No Store access and no research acceptance.
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
    // 默认策略限制 hosted web 的工具调用；Required 表示本次 probe 必须实际请求该工具，而不是退化为普通回答。
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
        // 成功输出 provider response 中的模型、usage、web_search_call 和策略校验结果，便于核对真实响应。
        Ok(response) => println!(
            "{}",
            json!({"state":"response_received", "policy_validation":policy.validate_provider_response(&response.raw).map_err(|e|e.to_string()), "response_id":response.raw.get("id"), "actual_model":response.raw.get("model"), "usage":response.raw.get("usage"), "web_calls":response.raw.get("output").and_then(|v|v.as_array()).map(|items|items.iter().filter(|i|i.get("type").and_then(|v|v.as_str())==Some("web_search_call")).collect::<Vec<_>>())})
        ),
        // HTTP 和其他模型错误都只输出 BLOCKED 诊断；错误分支不伪造搜索成功或来源验证。
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
