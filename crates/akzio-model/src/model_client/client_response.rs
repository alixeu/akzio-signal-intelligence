impl ModelClient {
    pub async fn respond(&self, request: ModelRequest) -> Result<ModelResponse> {
        // 普通调用复用带事件入口，但不向调用方暴露 reasoning 流事件。
        self.respond_with_events(request, |_| {}).await
    }

    pub async fn respond_with_events(
        &self,
        request: ModelRequest,
        on_event: impl FnMut(ModelStreamEvent),
    ) -> Result<ModelResponse> {
        // 真实 provider 负责网络、SSE 和终态解析；各 fixture 分支只在本地把 raw
        // 值规范化为同一个 ModelResponse 形状，且都保留本次 request body。
        match self {
            Self::OpenAIResponses(client) => client.respond_with_events(request, on_event).await,
            Self::Fixture(raw) => openai_response_from_raw(
                // 单值 fixture 可按当前 Context 替换占位符，但不会模拟 provider 请求。
                materialize_fixture(raw.clone(), &request),
                self.request_body(&request),
            )
            .map(|mut response| {
                response.continuation = response
                    .continuation
                    .with_fixture_input(fixture_input(&request));
                response
            }),
            Self::FixtureByPurpose(outputs) => {
                // purpose fixture 需要显式 fixture_key；Mutex 保护每个 purpose 的
                // FIFO，pop_front 让同一 purpose 的多次调用按测试预设消耗。
                let key = request
                    .fixture_key
                    .as_deref()
                    .ok_or(ModelError::MissingOutput)?;
                let raw = outputs
                    .lock()
                    .expect("fixture response map poisoned")
                    .get_mut(key)
                    .and_then(VecDeque::pop_front)
                    .ok_or(ModelError::FixtureExhausted)?;
                openai_response_from_raw(
                    materialize_fixture(raw, &request),
                    self.request_body(&request),
                )
                .map(|mut response| {
                    response.continuation = response
                        .continuation
                        .with_fixture_input(fixture_input(&request));
                    response
                })
            }
            Self::FixtureByPurposePhase(templates) => {
                // 阶段 fixture 是不可变的 [Draft, Submit] 模板；tool_choice 只决定
                // 读取哪一格，不会把 Draft 受理或 Submit 结果写入 durable Store。
                let purpose = request
                    .fixture_key
                    .as_deref()
                    .ok_or(ModelError::MissingOutput)?;
                let phase = match &request.tool_choice {
                    ModelToolChoice::Auto | ModelToolChoice::None => 0,
                    ModelToolChoice::RequiredFunction(name) if name == "submit_result" => 1,
                    _ => return Err(ModelError::MissingOutput),
                };
                let template = templates.get(purpose).ok_or(ModelError::FixtureExhausted)?;
                let raw = materialize_phase_fixture(template[phase].clone(), &request)?;
                openai_response_from_raw(raw, self.request_body(&request)).map(|mut response| {
                    response.continuation = response
                        .continuation
                        .with_fixture_input(fixture_input(&request));
                    response
                })
            }
            Self::FixtureSequence(values) => {
                // 序列 fixture 跨 purpose 共享一个受 Mutex 保护的 FIFO，耗尽时直接
                // 返回 FixtureExhausted，不能循环复用旧响应。
                let raw = values
                    .lock()
                    .expect("fixture response sequence poisoned")
                    .pop_front()
                    .ok_or(ModelError::FixtureExhausted)?;
                openai_response_from_raw(
                    materialize_fixture(raw, &request),
                    self.request_body(&request),
                )
                .map(|mut response| {
                    response.continuation = response
                        .continuation
                        .with_fixture_input(fixture_input(&request));
                    response
                })
            }
        }
    }
}
