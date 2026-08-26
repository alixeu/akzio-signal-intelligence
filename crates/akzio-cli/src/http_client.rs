use super::*;

pub(crate) struct ControlApiClient {
    base_url: Url,
    client: Client,
    token: String,
}

impl From<PurposeArg> for RunPurpose {
    fn from(value: PurposeArg) -> Self {
        match value {
            PurposeArg::Debug => Self::Debug,
            PurposeArg::PaperDryRun => Self::PaperDryRun,
        }
    }
}

impl ControlApiClient {
    pub(crate) fn from_config(config: &Config) -> Result<Self> {
        Self::new(config.daemon.http_addr, daemon_token(&config.daemon)?)
    }

    pub(crate) fn new(address: SocketAddr, token: String) -> Result<Self> {
        if !address.ip().is_loopback() {
            bail!("daemon.http_addr must be a loopback address");
        }
        if token.trim().is_empty() || token.contains('\r') || token.contains('\n') {
            bail!("daemon token must be nonempty and contain no newlines");
        }

        Ok(Self {
            base_url: Url::parse(&format!("http://{address}/"))
                .context("build loopback control API URL")?,
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .context("build loopback control API client")?,
            token,
        })
    }

    pub(crate) fn endpoint(&self, segments: &[&str]) -> Url {
        let mut url = self.base_url.clone();
        let mut path = url
            .path_segments_mut()
            .expect("loopback control API URL must be hierarchical");
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        url
    }

    pub(crate) fn request(&self, method: Method, url: Url) -> RequestBuilder {
        self.client
            .request(method, url)
            .header("x-akzio-token", &self.token)
    }

    pub(crate) async fn json<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        let response = request
            .send()
            .await
            .context("call loopback HTTP control API")?;
        require_success(response)
            .await?
            .json()
            .await
            .context("decode loopback control API response")
    }

    pub(crate) async fn health(&self) -> Result<DaemonHealth> {
        self.json(self.request(Method::GET, self.endpoint(&["health"])))
            .await
    }

    pub(crate) async fn ready(&self) -> Result<DaemonHealth> {
        self.json(self.request(Method::GET, self.endpoint(&["ready"])))
            .await
    }

    pub(crate) async fn submit(&self, purpose: RunPurpose) -> Result<RunSubmissionResponse> {
        self.json(
            self.request(Method::POST, self.endpoint(&["runs"]))
                .json(&SubmitRequest { purpose }),
        )
        .await
    }

    pub(crate) async fn replay(&self, run_id: &str) -> Result<ReplayReport> {
        self.json(self.request(Method::GET, self.endpoint(&["runs", run_id, "replay"])))
            .await
    }

    pub(crate) async fn retrospectives(&self, run_id: &str) -> Result<Vec<RetrospectiveView>> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["runs", run_id, "retrospectives"]),
        ))
        .await
    }

    pub(crate) async fn trajectory(&self, run_id: &str) -> Result<Vec<TrajectoryEntry>> {
        self.json(self.request(Method::GET, self.endpoint(&["runs", run_id, "trajectory"])))
            .await
    }

    pub(crate) async fn cancel(&self, run_id: &str) -> Result<RunCancellationResponse> {
        self.json(self.request(Method::POST, self.endpoint(&["runs", run_id, "cancel"])))
            .await
    }

    pub(crate) async fn retry(&self, run_id: &str) -> Result<RunRetryResponse> {
        self.json(self.request(Method::POST, self.endpoint(&["runs", run_id, "retry"])))
            .await
    }

    pub(crate) async fn approve_paper(
        &self,
        request: &PaperApprovalRequest,
    ) -> Result<PaperApprovalResponse> {
        self.json(
            self.request(Method::POST, self.endpoint(&["control", "paper-approval"]))
                .json(request),
        )
        .await
    }

    pub(crate) async fn canary_stage(
        &self,
        spec: &CanaryCampaignSpec,
    ) -> Result<CanaryCampaignHead> {
        self.json(
            self.request(Method::POST, self.endpoint(&["control", "canary", "stage"]))
                .json(spec),
        )
        .await
    }

    pub(crate) async fn canary_status(&self) -> Result<Option<CanaryCampaignHead>> {
        self.json(self.request(Method::GET, self.endpoint(&["control", "canary", "status"])))
            .await
    }

    pub(crate) async fn canary_resume(
        &self,
        campaign_id: &ContentHash,
    ) -> Result<CanaryCampaignHead> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "canary", "resume"]),
            )
            .json(&serde_json::json!({ "campaign_id": campaign_id })),
        )
        .await
    }

    pub(crate) async fn set_freeze(&self, frozen: bool, reason: &str) -> Result<DaemonHealth> {
        let action = if frozen { "freeze" } else { "unfreeze" };
        self.json(
            self.request(Method::POST, self.endpoint(&["control", action]))
                .json(&FreezeRequest { reason }),
        )
        .await
    }

    pub(crate) async fn events(&self, run_id: &str, after: i64) -> Result<()> {
        let mut url = self.endpoint(&["runs", run_id, "events"]);
        url.query_pairs_mut()
            .append_pair("after", &after.to_string());
        let response = self
            .request(Method::GET, url)
            .header("accept", "text/event-stream")
            .send()
            .await
            .context("open loopback event stream")?;
        let mut stream = require_success(response).await?.bytes_stream();
        let mut pending = String::new();
        let mut event_data = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("read loopback event stream")?;
            pending.push_str(
                std::str::from_utf8(chunk.as_ref())
                    .context("loopback control API emitted non-UTF-8 SSE")?,
            );

            while let Some(newline) = pending.find('\n') {
                let line = pending.drain(..=newline).collect::<String>();
                let line = line.trim_end_matches(&['\r', '\n'][..]);
                if line.is_empty() {
                    print_sse_data(&mut event_data);
                } else if let Some(data) = line.strip_prefix("data:") {
                    event_data.push(data.strip_prefix(' ').unwrap_or(data).to_owned());
                }
            }
        }
        print_sse_data(&mut event_data);
        Ok(())
    }
}

async fn require_success(response: Response) -> Result<Response> {
    if response.status().is_success() {
        Ok(response)
    } else {
        bail!("loopback control API returned HTTP {}", response.status());
    }
}

fn print_sse_data(event_data: &mut Vec<String>) {
    if !event_data.is_empty() {
        println!("{}", event_data.join("\n"));
        event_data.clear();
    }
}
