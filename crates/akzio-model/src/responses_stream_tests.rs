use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;

#[derive(Clone, Copy)]
enum Tail {
    HoldOpen,
    CorruptChunk,
    EndBody,
}

async fn loopback_response(events: &str, tail: Tail) -> Result<ModelResponse> {
    // 本地 TCP server 模拟 chunked SSE，并按 tail 控制“终态后保持连接、损坏 chunk
    // 或正常 EOF”，用于验证 adapter 是否以 Responses 终态而非 HTTP EOF 决定返回。
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let events = events.to_owned();
    let (release, held) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        // 先读完请求头和 body，避免测试 server 在客户端尚未发送完整请求时写响应。
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 2048];
        loop {
            let length = socket.read(&mut buffer).unwrap();
            assert!(length > 0);
            request.extend_from_slice(&buffer[..length]);
            if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if request.len() >= end + 4 + content_length {
                    break;
                }
            }
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nx-request-id: offline-loopback-request\r\n\r\n").unwrap();
        // 首个 chunk 包含完整事件；随后故意选择连接保持、非法 chunk 或 HTTP EOF。
        write!(socket, "{:x}\r\n{}\r\n", events.len(), events).unwrap();
        match tail {
            Tail::HoldOpen => {}
            Tail::CorruptChunk => socket.write_all(b"not-a-chunk-length\r\n").unwrap(),
            Tail::EndBody => socket.write_all(b"0\r\n\r\n").unwrap(),
        }
        socket.flush().unwrap();
        // No EOF until the client has returned. A completed event must suffice.
        held.recv_timeout(Duration::from_secs(3)).unwrap();
    });
    let client = OpenAIResponsesClient::with_timeouts(
        format!("http://{address}"),
        "offline-fixture-key",
        "fixture",
        "low",
        Duration::from_secs(1),
        Duration::from_millis(250),
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.respond(ModelRequest {
            instructions: "offline transport regression".into(),
            input: ModelInput::Fresh {
                text: "fixture".into(),
            },
            max_output_tokens: 100,
            reasoning_effort: None,
            tools: vec![],
            tool_choice: ModelToolChoice::None,
            fixture_key: None,
        }),
    )
    .await;
    // 通知 server 可以结束，随后 join 线程；先释放再 join 避免 server 等待自身未
    // 完成的客户端调用。
    release.send(()).unwrap();
    server.join().unwrap();
    result.expect("bounded local HTTP probe must return")
}

fn completed() -> Value {
    json!({"type":"response.completed","response":{
        "id":"offline-completed-response","model":"fixture","status":"completed",
        "output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"OK"}]}],
        "usage":{"input_tokens":111,"output_tokens":9,"output_tokens_details":{"reasoning_tokens":3}}
    }})
}

fn sse(event: Value) -> String {
    format!("data: {event}\n\n")
}

#[tokio::test]
async fn terminal_completed_returns_while_http_connection_is_still_open() {
    let result = loopback_response(&sse(completed()), Tail::HoldOpen)
        .await
        .unwrap();
    assert_eq!(result.output_text, "OK");
    assert_eq!(result.raw["id"], "offline-completed-response");
    assert_eq!(
        result.provider_request_id.as_deref(),
        Some("offline-loopback-request")
    );
    assert_eq!(result.usage.input_tokens, Some(111));
    assert_eq!(result.usage.output_tokens, Some(9));
    assert_eq!(result.usage.reasoning_tokens, Some(3));
}

#[tokio::test]
async fn terminal_response_is_not_overwritten_by_a_later_http_body_failure() {
    let result = loopback_response(&sse(completed()), Tail::CorruptChunk)
        .await
        .unwrap();
    assert_eq!(result.output_text, "OK");
    assert_eq!(result.usage.output_tokens, Some(9));
}

#[tokio::test]
async fn terminal_incomplete_keeps_usage_without_waiting_for_http_eof() {
    let event = json!({"type":"response.incomplete","response":{
        "status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},
        "usage":{"input_tokens":111,"output_tokens":9,"output_tokens_details":{"reasoning_tokens":3}}
    }});
    match loopback_response(&sse(event), Tail::HoldOpen)
        .await
        .unwrap_err()
    {
        ModelError::Incomplete { reason, usage } => {
            assert_eq!(reason, "max_output_tokens");
            assert_eq!(usage.input_tokens, Some(111));
            assert_eq!(usage.output_tokens, Some(9));
        }
        other => panic!("expected incomplete with known usage, got {other:?}"),
    }
}

#[tokio::test]
async fn terminal_refusal_remains_a_refusal_on_an_open_connection() {
    let event = json!({"type":"response.completed","response":{
        "status":"completed","output":[{"type":"message","content":[{"type":"refusal","refusal":"offline refusal"}]}]
    }});
    assert!(matches!(
        loopback_response(&sse(event), Tail::HoldOpen).await,
        Err(ModelError::Refused(_))
    ));
}

#[tokio::test]
async fn terminal_is_required_even_after_arguments_done_and_done_marker() {
    let events = sse(json!({"type":"response.function_call_arguments.done","arguments":"{}"}))
        + "data: [DONE]\n\n";
    assert!(matches!(
        loopback_response(&events, Tail::EndBody).await,
        Err(ModelError::InvalidStream(_))
    ));
    assert!(matches!(
        loopback_response(&events, Tail::HoldOpen).await,
        Err(ModelError::StreamIdleTimeout { .. })
    ));
}

#[tokio::test]
async fn terminal_failure_is_not_promoted_to_a_completed_response() {
    let event = json!({"type":"response.failed","response":{"status":"failed","error":{"message":"offline failure"}}});
    assert!(matches!(
        loopback_response(&sse(event), Tail::HoldOpen).await,
        Err(ModelError::InvalidStream(_))
    ));
}

#[tokio::test]
async fn terminal_event_requires_matching_response_status() {
    for status in ["in_progress", "failed", "incomplete"] {
        let mut event = completed();
        event["response"]["status"] = json!(status);
        assert!(
            matches!(
                loopback_response(&sse(event), Tail::EndBody).await,
                Err(ModelError::InvalidStream(_))
            ),
            "completed event with {status} body must be rejected"
        );
    }
    let mut event = completed();
    event["type"] = json!("response.incomplete");
    assert!(matches!(
        loopback_response(&sse(event), Tail::EndBody).await,
        Err(ModelError::InvalidStream(_))
    ));
}

#[tokio::test]
async fn terminal_missing_usage_is_preserved_as_unknown_for_runtime_validation() {
    let mut event = completed();
    event["response"].as_object_mut().unwrap().remove("usage");
    let result = loopback_response(&sse(event), Tail::HoldOpen)
        .await
        .unwrap();
    assert_eq!(result.usage.input_tokens, None);
    assert_eq!(result.usage.output_tokens, None);
}
