//! Real native-web transport probe. No Store access and no research acceptance.
use akzio_model::{
    ModelClient, ModelInput, ModelRequest, ModelToolChoice, NativeWebPolicy, OpenAIResponsesClient,
};
use serde_json::json;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ModelClient::OpenAIResponses(OpenAIResponsesClient::new(
        std::env::var("LLM_GATEWAY_BASE_URL")?,
        std::env::var("LLM_GATEWAY_API_KEY")?,
        "gpt-5.6-luna",
        "low",
    )?);
    let policy = NativeWebPolicy::default();
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
        Ok(response) => println!(
            "{}",
            json!({"state":"response_received", "policy_validation":policy.validate_provider_response(&response.raw).map_err(|e|e.to_string()), "response_id":response.raw.get("id"), "actual_model":response.raw.get("model"), "usage":response.raw.get("usage"), "web_calls":response.raw.get("output").and_then(|v|v.as_array()).map(|items|items.iter().filter(|i|i.get("type").and_then(|v|v.as_str())==Some("web_search_call")).collect::<Vec<_>>())})
        ),
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
