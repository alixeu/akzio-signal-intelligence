// 文件导读：传输层把 GET/POST/PATCH/DELETE 的 HTTP 行为集中到 AlpacaPaper，统一附加
// Paper 凭据、检查状态码并保留错误正文。GET 只对网络传输失败做有限退避，写请求不
// 自动重试，以免把不确定的 Broker effect 变成重复副作用；重发由上层的 client_order_id
// 和 durable effect intent 负责。

impl AlpacaPaper {
    async fn get_json(&self, path: &str) -> Result<Value> {
        // 读请求最多五次线性短退避；最终响应统一由 response_json 解析，未知正文仍保留
        // 为字符串，方便上层诊断而不猜测 broker schema。
        let url = self.url(path);
        let response = {
            let mut attempt = 1_u64;
            loop {
                match self.authorized(self.client.get(&url)).send().await {
                    Ok(response) => break response,
                    Err(_source) if attempt < 5 => {
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
        // 仅在即将发送的 request 上挂载凭据，不把凭据复制到任何持久化对象。
        request
            .header("APCA-API-KEY-ID", &self.credentials.key_id)
            .header("APCA-API-SECRET-KEY", &self.credentials.secret_key)
    }

    fn url(&self, path: &str) -> String {
        // path 来自本模块固定入口，拼接到已验证的 Paper base URL。
        format!("{}{}", self.base_url, path)
    }
}
