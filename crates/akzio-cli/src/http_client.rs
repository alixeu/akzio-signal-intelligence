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

impl ControlApiClient {
    pub(crate) async fn store_doctor(&self) -> Result<serde_json::Value> {
        self.json(self.request(Method::GET, self.endpoint(&["control", "store", "doctor"])))
            .await
    }

    pub(crate) async fn store_inventory(&self) -> Result<serde_json::Value> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "inventory"]),
        ))
        .await
    }

    pub(crate) async fn store_metrics(&self) -> Result<serde_json::Value> {
        self.json(self.request(Method::GET, self.endpoint(&["control", "store", "metrics"])))
            .await
    }

    pub(crate) async fn store_alerts(&self) -> Result<serde_json::Value> {
        self.json(self.request(Method::GET, self.endpoint(&["control", "store", "alerts"])))
            .await
    }

    pub(crate) async fn store_session(&self, session_key: &str) -> Result<Option<SessionSlot>> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "session", session_key]),
        ))
        .await
    }

    pub(crate) async fn store_release_evidence(
        &self,
        run_id: &RunId,
    ) -> Result<ReleaseEvidenceBundle> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "release-evidence", run_id.0.as_str()]),
        ))
        .await
    }

    pub(crate) async fn store_backup(&self, target: &Path) -> Result<serde_json::Value> {
        self.json(
            self.request(Method::POST, self.endpoint(&["control", "store", "backup"]))
                .json(&serde_json::json!({ "target": target })),
        )
        .await
    }

    pub(crate) async fn store_restore(
        &self,
        source: &Path,
        target: &Path,
    ) -> Result<serde_json::Value> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "restore"]),
            )
            .json(&serde_json::json!({ "source": source, "target": target })),
        )
        .await
    }

    pub(crate) async fn store_export_run(
        &self,
        run_id: &str,
        target: &Path,
        include_raw_model: bool,
    ) -> Result<serde_json::Value> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "export-run"]),
            )
            .json(&serde_json::json!({
                "run_id": run_id,
                "target": target,
                "include_raw_model": include_raw_model,
            })),
        )
        .await
    }
    pub(crate) async fn store_claim_next(
        &self,
        worker_id: &str,
        at: DateTime<Utc>,
        lease_seconds: i64,
    ) -> Result<bool> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "claim-next"]),
            )
            .json(&serde_json::json!({
                "worker_id": worker_id,
                "at": at,
                "lease_seconds": lease_seconds,
            })),
        )
        .await
    }

    pub(crate) async fn store_recover_expired(&self, at: DateTime<Utc>) -> Result<u64> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "recover-expired"]),
            )
            .json(&serde_json::json!({ "at": at })),
        )
        .await
    }

    pub(crate) async fn store_workflow(&self, run_id: &RunId) -> Result<StoreWorkflowView> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "workflow", run_id.0.as_str()]),
        ))
        .await
    }

    pub(crate) async fn store_events(
        &self,
        run_id: &RunId,
        after: i64,
        limit: usize,
    ) -> Result<Vec<StoreEventView>> {
        let mut url = self.endpoint(&["control", "store", "events", run_id.0.as_str()]);
        url.query_pairs_mut()
            .append_pair("after", &after.to_string())
            .append_pair("limit", &limit.to_string());
        self.json(self.request(Method::GET, url)).await
    }

    pub(crate) async fn store_artifact(&self, artifact_id: &ArtifactId) -> Result<Artifact> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "artifacts", artifact_id.0.as_str()]),
        ))
        .await
    }

    pub(crate) async fn store_diagnose_corruption(&self, artifact_id: &ArtifactId) -> Result<bool> {
        self.json(self.request(
            Method::POST,
            self.endpoint(&[
                "control",
                "store",
                "artifacts",
                artifact_id.0.as_str(),
                "diagnose",
            ]),
        ))
        .await
    }

    pub(crate) async fn store_freeze(
        &self,
        frozen: bool,
        reason: &str,
        at: DateTime<Utc>,
    ) -> Result<Artifact> {
        self.json(
            self.request(Method::POST, self.endpoint(&["control", "store", "freeze"]))
                .json(&serde_json::json!({
                    "frozen": frozen,
                    "reason": reason,
                    "at": at,
                })),
        )
        .await
    }

    pub(crate) async fn store_latest_artifact(
        &self,
        kind: ArtifactKind,
    ) -> Result<Option<Artifact>> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "latest-artifact"]),
            )
            .json(&serde_json::json!({ "kind": kind })),
        )
        .await
    }

    pub(crate) async fn store_acquire_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Option<DaemonLease>> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "lease", "acquire"]),
            )
            .json(&serde_json::json!({
                "lease_name": lease_name,
                "owner_id": owner_id,
                "at": at,
                "expires_at": expires_at,
            })),
        )
        .await
    }

    pub(crate) async fn store_validate_lease(
        &self,
        lease: &DaemonLease,
        at: DateTime<Utc>,
    ) -> Result<bool> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "lease", "validate"]),
            )
            .json(&serde_json::json!({ "lease": lease, "at": at })),
        )
        .await
    }

    pub(crate) async fn store_latest_retrospective(&self) -> Result<Option<Retrospective>> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "latest-retrospective"]),
        ))
        .await
    }

    pub(crate) async fn lesson_add(&self, input: &serde_json::Value) -> Result<serde_json::Value> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "lessons", "add"]),
            )
            .json(input),
        )
        .await
    }

    pub(crate) async fn lesson_list(
        &self,
        lifecycle: Option<&str>,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let mut url = self.endpoint(&["control", "store", "lessons"]);
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", &limit.to_string());
            if let Some(lifecycle) = lifecycle {
                query.append_pair("lifecycle", lifecycle);
            }
        }
        self.json(self.request(Method::GET, url)).await
    }

    pub(crate) async fn lesson_show(&self, lesson_id: &str) -> Result<serde_json::Value> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "lessons", lesson_id]),
        ))
        .await
    }

    pub(crate) async fn lesson_usage(&self, lesson_id: &str) -> Result<serde_json::Value> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "lessons", lesson_id, "usage"]),
        ))
        .await
    }

    pub(crate) async fn lesson_transition(
        &self,
        lesson_id: &str,
        lifecycle: LessonLifecycle,
        actor: &str,
        reason: &str,
    ) -> Result<serde_json::Value> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "lessons", lesson_id, "transition"]),
            )
            .json(&serde_json::json!({
                "lifecycle": lifecycle,
                "actor": actor,
                "reason": reason,
            })),
        )
        .await
    }
}
