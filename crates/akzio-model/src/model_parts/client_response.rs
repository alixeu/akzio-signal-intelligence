impl ModelClient {
    pub async fn respond(&self, request: ModelRequest) -> Result<ModelResponse> {
        self.respond_with_events(request, |_| {}).await
    }

    pub async fn respond_with_events(
        &self,
        request: ModelRequest,
        on_event: impl FnMut(ModelStreamEvent),
    ) -> Result<ModelResponse> {
        match self {
            Self::Responses(client) => client.respond_with_events(request, on_event).await,
            Self::Fixture(raw) => response_from_raw(
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
                response_from_raw(
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
            Self::FixtureSequence(values) => {
                let raw = values
                    .lock()
                    .expect("fixture response sequence poisoned")
                    .pop_front()
                    .ok_or(ModelError::FixtureExhausted)?;
                response_from_raw(
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
