// 文件导读：ControlApiClient 是 CLI 到 loopback daemon 的唯一 HTTP/SSE 传输层。它读取
// 已存在的 `.daemon-token`、限制 loopback、统一附加认证 header，并把 JSON/事件流交给
// 服务端权威处理；HTTP 200 只证明请求被响应，不能推断 workflow、Paper submission、
// fill 或 Outcome 状态。
// Rust 机制：`ControlApiClient` 拥有 Client/Url/token，方法借用 `&self`；泛型
// `json<T: DeserializeOwned>` 把响应解析为调用方类型；异步 `bytes_stream` 以 Future/Stream
// 分块处理 SSE，pending UTF-8 字节由可变 Vec 借用保留到下一 chunk。

use super::*;

pub(crate) struct ControlApiClient {
    base_url: Url,
    client: Client,
    token: String,
}

impl ControlApiClient {
    // 从当前 Store Root 的既有 token 构造客户端；不创建/改权限，错误通过 Result 阻止无认证请求。
    pub(crate) fn from_config(config: &Config) -> Result<Self> {
        // 只读取已经存在的 token；CLI 查询不会为了“方便”创建认证文件或修改权限，
        // 启动目标 Core 的 token 与当前 Store Root 不一致时直接失败。
        // A read-only inspect must not create credentials or chmod a Store file.
        let token = validate_daemon_token(
            fs::read_to_string(daemon_token_path(&config.daemon))
                .context("read existing daemon token; start the intended Core first")?,
            "existing daemon token",
        )?;
        Self::new(config.daemon.http_addr, token)
    }

    // 校验地址和 token 后拥有 Url、reqwest Client 与 token；Client 禁止代理和重定向。
    pub(crate) fn new(address: SocketAddr, token: String) -> Result<Self> {
        // Client 禁止 proxy/redirect，确保认证请求留在 loopback 且不会把 token 跟随重定向
        // 发送到其他 host。
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

    // 把固定 API 名称或一个动态 run/lesson ID 当作单独的路径段追加，而非拼接 URL 字符串；
    // 返回拥有型 Url，调用方可再加受 Url 编码的 query。
    pub(crate) fn endpoint(&self, segments: &[&str]) -> Url {
        // `path_segments_mut()` 的可变借用结束前，url 不能被返回；显式 drop(path)
        // 提前释放这次借用。push 负责对动态 ID 等路径段转义，不能把整个 URL 当段传入。
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

    // 生成统一带认证头的 RequestBuilder；此处仅构造请求，网络 I/O 要等后续 send/await。
    pub(crate) fn request(&self, method: Method, url: Url) -> RequestBuilder {
        // 所有请求在这里统一加 token，避免某个 handler 忘记认证 header。
        self.client
            .request(method, url)
            .header("x-akzio-token", &self.token)
    }

    // 驱动已构造的 HTTP Future，先拒绝非成功状态，再把响应 body 消费为拥有型 T。
    pub(crate) async fn json<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        // 泛型 T: DeserializeOwned 要求结果拥有解码后的数据，不借用即将被释放的
        // HTTP body。`send().await?` 驱动网络请求；第二个 await 再消费 body。
        // 先验状态码，避免把错误页误当成功的业务 JSON。
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

    // 查询进程和 Store 健康投影；GET 返回并解码成功不代表 worker 已完成某项 Run。
    pub(crate) async fn health(&self) -> Result<DaemonHealth> {
        self.json(self.request(Method::GET, self.endpoint(&["health"])))
            .await
    }

    // 请求隔离 Debug Core 创建/准备正式图；Run、paused head 的事务由 daemon 完成。
    pub(crate) async fn debug_prepare(
        &self,
        value: &akzio_daemon::DebugPrepareRequest,
    ) -> Result<serde_json::Value> {
        self.json(
            self.request(Method::POST, self.endpoint(&["v1", "debug", "runs"]))
                .json(value),
        )
        .await
    }
    // 只读检查指定 Run，可选 task/attempt 参数只缩小服务端投影范围。
    pub(crate) async fn debug_inspect(
        &self,
        run: &str,
        task: Option<&str>,
        attempt: Option<&str>,
    ) -> Result<serde_json::Value> {
        let mut url = self.endpoint(&["v1", "debug", "runs", run]);
        if let Some(task) = task {
            url.query_pairs_mut().append_pair("task", task);
        }
        if let Some(attempt) = attempt {
            url.query_pairs_mut().append_pair("attempt", attempt);
        }
        self.json(self.request(Method::GET, url)).await
    }
    // 以 POST 提交 revision-bound Debug 控制请求；是否允许动作由服务端 CAS 决定。
    pub(crate) async fn debug_control(
        &self,
        run: &str,
        value: &akzio_domain::DebugControlRequest,
    ) -> Result<serde_json::Value> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["v1", "debug", "runs", run, "control"]),
            )
            .json(value),
        )
        .await
    }
    // 从现有 Debug Run/Task 创建新 Run 身份；父输出和 Paper 权限不由客户端复制。
    pub(crate) async fn debug_fork(
        &self,
        run: &str,
        value: &akzio_daemon::DebugForkRequest,
    ) -> Result<serde_json::Value> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["v1", "debug", "runs", run, "fork"]),
            )
            .json(value),
        )
        .await
    }
    // 将结构化阶段验收记录交给 Store；该 POST 记录证据，不直接推进任务。
    pub(crate) async fn debug_acceptance(
        &self,
        run: &str,
        value: &akzio_domain::StageAcceptance,
    ) -> Result<serde_json::Value> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["v1", "debug", "runs", run, "acceptance"]),
            )
            .json(value),
        )
        .await
    }

    // 查询服务 readiness 投影；daemon 决定其含义，CLI 不据此推断 Run 成功。
    pub(crate) async fn ready(&self) -> Result<DaemonHealth> {
        self.json(self.request(Method::GET, self.endpoint(&["ready"])))
            .await
    }

    // 从持久化事件构建只读 replay 报告。
    pub(crate) async fn replay(&self, run_id: &str) -> Result<ReplayReport> {
        self.json(self.request(Method::GET, self.endpoint(&["runs", run_id, "replay"])))
            .await
    }

    // 查询已有 Run 的阶段叙事；响应是投影，不触发新的 Outcome 评估。
    pub(crate) async fn retrospectives(&self, run_id: &str) -> Result<Vec<RetrospectiveView>> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["runs", run_id, "retrospectives"]),
        ))
        .await
    }

    // 读取 Store 中的轨迹序列，不创建新的 attempt 或 evaluator 调用。
    pub(crate) async fn trajectory(&self, run_id: &str) -> Result<Vec<TrajectoryEntry>> {
        self.json(self.request(Method::GET, self.endpoint(&["runs", run_id, "trajectory"])))
            .await
    }

    // 请求取消指定 Run；响应计数表示服务端接受/标记的任务数，不保证外部请求已补偿。
    pub(crate) async fn cancel(&self, run_id: &str) -> Result<RunCancellationResponse> {
        self.json(self.request(Method::POST, self.endpoint(&["runs", run_id, "cancel"])))
            .await
    }

    // 请求在原 Run 上进行有界叙事修复；具体资格、重试和持久化由服务端校验。
    pub(crate) async fn repair_narrative(&self, run_id: &str) -> Result<serde_json::Value> {
        self.json(self.request(
            Method::POST,
            self.endpoint(&["runs", run_id, "repair-narrative"]),
        ))
        .await
    }

    // 请求为支持的 Run 创建 retry 关系；CLI 不自行复制 WorkflowGraph 或预算。
    pub(crate) async fn retry(&self, run_id: &str) -> Result<RunRetryResponse> {
        self.json(self.request(Method::POST, self.endpoint(&["runs", run_id, "retry"])))
            .await
    }

    // 提交经本地身份/资格准备的审批请求；服务端持久化 approval，仍不会因此下单。
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

    // 登记 Canary campaign spec；candidate 是否可 stage 由 daemon/Store 校验。
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

    // 查询当前 campaign head；None 表示尚无活动记录，不等于 candidate 已发布。
    pub(crate) async fn canary_status(&self) -> Result<Option<CanaryCampaignHead>> {
        self.json(self.request(Method::GET, self.endpoint(&["control", "canary", "status"])))
            .await
    }

    // 请求推进已存在的 campaign；CAS/lease 由服务端执行，返回 head 不是 Paper 完成证据。
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

    // 请求切换全局 freeze 状态；reason 借用到序列化完成，实际 Artifact 写入在 daemon。
    pub(crate) async fn set_freeze(&self, frozen: bool, reason: &str) -> Result<DaemonHealth> {
        let action = if frozen { "freeze" } else { "unfreeze" };
        self.json(
            self.request(Method::POST, self.endpoint(&["control", action]))
                .json(&FreezeRequest { reason }),
        )
        .await
    }

    // 持续读取 SSE 流，直到服务端关闭或传输失败；`after` 只定位回放游标，不会确认事件。
    // 此 CLI 只打印 data，不解析 event/id、不自动保存 cursor 或重连。
    pub(crate) async fn events(&self, run_id: &str, after: i64) -> Result<()> {
        // SSE client 只打印服务端已产生的事件；cursor 是观察起点，不是本地状态机的推进命令。
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
        let mut pending = Vec::new();
        let mut event_data = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("read loopback event stream")?;
            for line in sse_lines(&mut pending, chunk.as_ref())? {
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

// 将新 chunk 追加到拥有的 pending 缓冲，只消费完整换行行；末尾半行/半个 UTF-8 字符
// 留待下一 chunk，非法完整行以 Result 错误返回。
fn sse_lines(pending: &mut Vec<u8>, chunk: &[u8]) -> Result<Vec<String>> {
    // `&mut Vec<u8>` 让多次调用共用同一缓冲，`&[u8]` 只借用本次网络分块。
    // UTF-8 字符可能跨 chunk；只在找到换行后 drain 已完整的一行，尾部字节留待下次。
    // drain(..=newline) 同时移走已消费字节，不能在循环里重复打印旧行。
    pending.extend_from_slice(chunk);
    let mut lines = Vec::new();
    while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
        lines.push(
            String::from_utf8(pending.drain(..=newline).collect())
                .context("loopback control API emitted non-UTF-8 SSE")?,
        );
    }
    Ok(lines)
}

// 把 HTTP 非 2xx 统一转成错误；成功时把 Response 所有权交给调用方继续消费 body。
async fn require_success(response: Response) -> Result<Response> {
    // Response 仍由调用方拥有；失败分支只返回状态错误，不读取或猜测服务端 payload。
    if response.status().is_success() {
        Ok(response)
    } else {
        bail!("loopback control API returned HTTP {}", response.status());
    }
}

// 输出一个 SSE 事件的已收集 data 行，然后清空同一可变缓冲，避免跨事件串接。
fn print_sse_data(event_data: &mut Vec<String>) {
    // 空行是 SSE event 边界；join 后清空可变 Vec，避免下一个 event 复用旧 data。
    if !event_data.is_empty() {
        println!("{}", event_data.join("\n"));
        event_data.clear();
    }
}

impl ControlApiClient {
    // Store/Lesson 方法只是固定 endpoint 的薄包装；真正的 CAS/生命周期变更始终在 daemon。
    // 查询完整性报告，不在客户端直接打开或修复 Store。
    pub(crate) async fn store_doctor(&self) -> Result<serde_json::Value> {
        self.json(self.request(Method::GET, self.endpoint(&["control", "store", "doctor"])))
            .await
    }

    // 查询 Artifact/任务清单投影。
    pub(crate) async fn store_inventory(&self) -> Result<serde_json::Value> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "inventory"]),
        ))
        .await
    }

    // 查询 Store 指标和队列/容量投影。
    pub(crate) async fn store_metrics(&self) -> Result<serde_json::Value> {
        self.json(self.request(Method::GET, self.endpoint(&["control", "store", "metrics"])))
            .await
    }

    // 查询由 Store 状态派生的告警列表。
    pub(crate) async fn store_alerts(&self) -> Result<serde_json::Value> {
        self.json(self.request(Method::GET, self.endpoint(&["control", "store", "alerts"])))
            .await
    }

    // 按交易 Session key 查询唯一 slot；None 表示尚未 reservation。
    pub(crate) async fn store_session(&self, session_key: &str) -> Result<Option<SessionSlot>> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "session", session_key]),
        ))
        .await
    }

    // 请求只读导出已持久化的 evidence bundle。
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

    // 请求 daemon 将 Store 备份到明确目标路径；备份的文件副作用发生在服务端。
    pub(crate) async fn store_backup(&self, target: &Path) -> Result<serde_json::Value> {
        self.json(
            self.request(Method::POST, self.endpoint(&["control", "store", "backup"]))
                .json(&serde_json::json!({ "target": target })),
        )
        .await
    }

    // 请求 Store restore；source/target 路径只是服务端操作参数，不在 CLI 本地复制数据库。
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

    // 请求导出指定 Run 的 bundle，并显式传递是否包含受限 raw model 数据。
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

    // 读取已有 Debug diagnostics 并请求导出到 target；不重跑节点，Store 读取与目标目录写入
    // 分别在 daemon 的导出实现中处理。
    pub(crate) async fn store_export_debug_bundle(
        &self,
        run_id: &str,
        target: &Path,
    ) -> Result<serde_json::Value> {
        self.json(
            self.request(
                Method::POST,
                self.endpoint(&["control", "store", "export-debug-bundle"]),
            )
            .json(&serde_json::json!({
                "run_id": run_id,
                "target": target,
            })),
        )
        .await
    }

    // 把已在 CLI 解码的 Lesson JSON 提交为草稿输入；生命周期与来源由 daemon 校验。
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

    // 按可选 lifecycle 与 limit 查询 Lesson；None 不附加 lifecycle 过滤参数。
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

    // 按 Lesson ID 查询单条投影。
    pub(crate) async fn lesson_show(&self, lesson_id: &str) -> Result<serde_json::Value> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "lessons", lesson_id]),
        ))
        .await
    }

    // 查询 Lesson usage/revalidation 读数，不改变预算或生命周期。
    pub(crate) async fn lesson_usage(&self, lesson_id: &str) -> Result<serde_json::Value> {
        self.json(self.request(
            Method::GET,
            self.endpoint(&["control", "store", "lessons", lesson_id, "usage"]),
        ))
        .await
    }

    // 请求带 actor/reason 的状态转换；CAS、权限与持久化事务由服务端执行。
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

#[cfg(test)]
mod sse_tests {
    use super::*;

    // 对每个 UTF-8 字节切分边界复用 pending 缓冲，确保网络分块不会破坏事件文本。
    #[test]
    fn utf8_event_survives_every_network_chunk_boundary() {
        let event = "data: {\"message\":\"研究完成🦀\"}\r\n\r\n";
        for split in 0..=event.len() {
            let mut pending = Vec::new();
            let mut lines = sse_lines(&mut pending, &event.as_bytes()[..split]).unwrap();
            lines.extend(sse_lines(&mut pending, &event.as_bytes()[split..]).unwrap());
            assert_eq!(lines.concat(), event);
            assert!(pending.is_empty());
        }
    }

    // 完整行含非法 UTF-8 时必须失败，而非以替换字符继续输出。
    #[test]
    fn malformed_complete_sse_line_is_rejected() {
        assert!(sse_lines(&mut Vec::new(), b"data: \xff\n").is_err());
    }
}
