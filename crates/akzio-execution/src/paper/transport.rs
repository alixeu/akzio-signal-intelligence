// 文件导读：传输层把 GET/POST/PATCH/DELETE 的 HTTP 行为集中到 AlpacaPaper，统一附加
// Paper 凭据、检查状态码并保留错误正文。GET 只对网络传输失败做有限退避，写请求不
// 自动重试，以免把不确定的 Broker effect 变成重复副作用；重发由上层的 client_order_id
// 和 durable effect intent 负责。

impl AlpacaPaper {
    async fn get_json(&self, path: &str) -> Result<Value> {
        // 读请求最多五次线性短退避；最终响应统一由 response_json 解析，未知正文仍保留
        // 为字符串，方便上层诊断而不猜测 broker schema。
        // 每次调用生成一个惰性 Future；await 时建立 URL 并尝试 GET。局部 response 块只让
        // attempt 计数/循环临时存在，未持有跨 await 的锁或可变共享状态。
        let url = self.url(path);
        let response = {
            let mut attempt = 1_u64;
            // 重试仅包住 send 的连接错误；HTTP 非 2xx 在循环外统一返回，不把业务拒绝当网络抖动重试。
            loop {
                match self.authorized(self.client.get(&url)).send().await {
                    Ok(response) => break response,
                    Err(_source) if attempt < 5 => {
                        // Tokio sleep.await 会让出当前任务直到计时器就绪；attempt 从 1 起，
                        // 退避为 250/500/750/1000ms，之后最后一次错误返回。
                        tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
                        attempt += 1;
                    }
                    Err(source) => {
                        return Err(PaperError::Transport {
                            url: url.clone(),
                            source,
                        });
                    }
                }
            }
        };
        self.response_json(url, response).await
    }

    async fn post_json(&self, url: &str, body: Value) -> Result<Value> {
        // POST 不做自动重试；调用方必须已经完成 Commitment，网络不确定状态交给恢复查询。
        // body 所有权传入本 helper，但 reqwest 只借用它序列化本次请求；send 失败通过
        // `?` 返回，不保留本地重试循环，以免重复下单。
        let response = self
            .authorized(self.client.post(url).json(&body))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.to_owned(),
                source,
            })?;
        self.response_json(url.to_owned(), response).await
    }

    async fn patch_json(&self, url: &str, body: Value) -> Result<Value> {
        // PATCH 同样是显式替换副作用，失败时原样返回，避免传输层隐式重复改单。
        // PATCH 与 POST 一样把 body 作为单次请求内容；发送结果未知时交给确定性 successor ID 恢复。
        let response = self
            .authorized(self.client.patch(url).json(&body))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.to_owned(),
                source,
            })?;
        self.response_json(url.to_owned(), response).await
    }

    async fn delete_empty(&self, url: &str) -> Result<()> {
        // DELETE 只接受成功状态；取消的幂等/404 语义由 reconcile 层按 intent 处理。
        // Response 由本函数拥有；先读取完整正文，再按状态码返回 () 或保留正文的 Http 错误。
        let response = self
            .authorized(self.client.delete(url))
            .send()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.to_owned(),
                source,
            })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.to_owned(),
                source,
            })?;
        if !status.is_success() {
            return Err(PaperError::Http {
                url: url.to_owned(),
                status,
                body,
            });
        }
        Ok(())
    }

    async fn response_json(&self, url: String, response: reqwest::Response) -> Result<Value> {
        // 先读取完整 body 再按 HTTP 成功与否分支，成功的非 JSON body 也保留为 Value::String。
        // url 与 Response 按值移入，Response.text() 消费响应体；body 读取失败是 Transport，
        // 成功后仅 2xx 可进入 parse_value。
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| PaperError::Transport {
                url: url.clone(),
                source,
            })?;
        if !status.is_success() {
            return Err(PaperError::Http { url, status, body });
        }
        Ok(parse_value(&body))
    }

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        // RequestBuilder 按值接收并返回，凭据通过 header 借用复制到待发送请求；
        // 此 helper 不发请求、不写 Store。
        request
            .header("APCA-API-KEY-ID", &self.credentials.key_id)
            .header("APCA-API-SECRET-KEY", &self.credentials.secret_key)
    }

    fn url(&self, path: &str) -> String {
        // path 只读借用，format! 生成新的 owned URL；base_url 已在构造时精确验证为 Paper host。
        format!("{}{}", self.base_url, path)
    }
}
