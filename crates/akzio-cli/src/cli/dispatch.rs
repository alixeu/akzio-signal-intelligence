async fn dispatch_control(command: Command, config: &Config, config_path: &Path) -> Result<()> {
    // 将需要 daemon 控制面的 Workflow/Run/Canary/Daemon 命令统一转为 HTTP 请求；
    // 本地处理的 ObservatoryConfig、Calibration、Evidence 和 ModelQualification 在这里
    // 明确拒绝，避免在配置已加载后误走远端路径。
    match command {
        Command::Workflow { command: WorkflowCommand::Blueprint { purpose, format } } => {
            // Blueprint 是服务端根据 purpose 生成的拓扑投影；Mermaid 只改变输出格式，
            // 不改变服务端的 workflow 状态。
            let client = ControlApiClient::from_config(config)?;
            let blueprint: akzio_domain::WorkflowBlueprint = client.json(client.request(Method::GET,
                client.endpoint(&["v1", "workflows", "blueprint"])).query(&[("purpose", purpose)])).await?;
            if format == "mermaid" { print!("{}", blueprint.mermaid()); Ok(()) } else { print_json(&blueprint) }
        }
        Command::Debug { command } => dispatch_debug(command, config, config_path).await,
        Command::Daemon { command } => match command {
            DaemonAction::Serve => serve(config, config_path).await,
            DaemonAction::Health => {
                print_json(&ControlApiClient::from_config(config)?.health().await?)
            }
            DaemonAction::Ready => {
                print_json(&ControlApiClient::from_config(config)?.ready().await?)
            }
            DaemonAction::Freeze { reason } => print_json(
                &ControlApiClient::from_config(config)?
                    .set_freeze(true, &reason)
                    .await?,
            ),
            DaemonAction::Unfreeze { reason } => print_json(
                &ControlApiClient::from_config(config)?
                    .set_freeze(false, &reason)
                    .await?,
            ),
        },
        Command::Run { command } => {
            let client = ControlApiClient::from_config(config)?;
            // Checkpoint 复用 inspection 请求但只输出其中的 checkpoint 字段；其他 Run
            // 子命令保留服务端返回的完整研究/Decision/Execution 生命周期信息。
            let checkpoint_only = matches!(command, RunCommand::Checkpoint { .. });
            match command {
                RunCommand::Inspect { run_id } | RunCommand::Checkpoint { run_id } => {
                    let inspection: serde_json::Value = client.json(client.request(Method::GET,
                        client.endpoint(&["v1", "observer", "runs", &run_id, "inspection"]))).await?;
                    if checkpoint_only { print_json(&inspection["checkpoint"]) } else { print_json(&inspection) }
                }
                RunCommand::Journal { run_id, after, limit, task_id, attempt_id } => {
                    // 可选 task/attempt 只缩小服务端 journal 查询范围；after/limit 组成分页
                    // 边界，CLI 不在本地拼接或重排事件。
                    let mut query = vec![("after", after.to_string()), ("limit", limit.to_string())];
                    if let Some(task) = task_id { query.push(("task_id", task)); }
                    if let Some(attempt) = attempt_id { query.push(("attempt_id", attempt)); }
                    let page: serde_json::Value = client.json(client.request(Method::GET,
                        client.endpoint(&["v1", "observer", "runs", &run_id, "journal"])).query(&query)).await?;
                    print_json(&page)
                }
                RunCommand::Replay { run_id } => print_json(&client.replay(&run_id).await?),
                RunCommand::Retrospectives { run_id } => {
                    print_json(&client.retrospectives(&run_id).await?)
                }
                RunCommand::Trajectory { run_id } => print_json(&client.trajectory(&run_id).await?),
                RunCommand::Events { run_id, after } => client.events(&run_id, after).await,
                RunCommand::Cancel { run_id } => print_json(&client.cancel(&run_id).await?),
                RunCommand::RepairNarrative { run_id } => {
                    print_json(&client.repair_narrative(&run_id).await?)
                }
                RunCommand::Retry { run_id } => print_json(&client.retry(&run_id).await?),

            }
        }
        Command::Canary { command } => {
            let client = ControlApiClient::from_config(config)?;
            match command {
                CanaryCommand::Stage { spec } => {
                    // Stage 的 JSON 只在本地解析为强类型 spec，真正的登记/状态变更由 daemon
                    // 完成；解析失败不会发出远端请求。
                    let payload = fs::read_to_string(spec).context("read canary campaign spec")?;
                    let spec: CanaryCampaignSpec = serde_json::from_str(&payload)
                        .context("parse canary campaign spec JSON")?;
                    print_json(&client.canary_stage(&spec).await?)
                }
                CanaryCommand::Status => print_json(&client.canary_status().await?),
                CanaryCommand::Resume { campaign_id } => {
                    let campaign_id =
                        ContentHash::new(campaign_id).context("parse campaign hash")?;
                    print_json(&client.canary_resume(&campaign_id).await?)
                }
            }
        }
        Command::ObservatoryConfig { .. } => {
            // 配置初始化/读取/编辑必须在 main 的早期分支完成，避免加载旧配置或环境覆盖。
            bail!("ObservatoryConfig is handled before control dispatch")
        }
        Command::Store { .. } => {
            // Store 命令有单独的 dispatch_store 入口，以保持查询、批准和导出语义集中。
            bail!("store commands must be dispatched through the store handler")
        }
        Command::ModelQualification { .. } => {
            // 模型资格报告是本地离线组装的输入，不通过 daemon 控制面生成。
            bail!("model qualification commands are handled before control dispatch")
        }
        Command::Calibration { .. } => {
            // 校准操作直接使用 Store Artifact 生命周期，不能被普通控制面代为激活。
            bail!("calibration commands are handled before config loading")
        }
        Command::Evidence { .. } => {
            // Evidence 的本地预检/审计有独立的 provider 和输出边界。
            bail!("evidence commands are handled by the local CLI")
        }
    }
}

async fn dispatch_store(command: StoreCommand, config: &Config, config_path: &Path) -> Result<()> {
    // Store 查询和写入请求仍由 daemon 的认证 Store API 执行；CLI 只负责参数转换和
    // 输出，唯一的本地写盘例外是显式 --target 的 release evidence JSON 导出。
    let client = ControlApiClient::from_config(config)?;
    match command {
        StoreCommand::Doctor => print_json(&client.store_doctor().await?),
        StoreCommand::Inventory => print_json(&client.store_inventory().await?),
        StoreCommand::Metrics => print_json(&client.store_metrics().await?),
        StoreCommand::Alerts => print_json(&client.store_alerts().await?),
        StoreCommand::PaperSession { session_key } => {
            // session 查询返回 optional slot；None 表示该交易日尚无 Paper reservation，
            // 不把缺槽位解释为可直接下单。
            let slot = client
                .store_session(&session_key)
                .await?
                .map(PaperSessionView::from);
            print_json(&slot)
        }
        StoreCommand::ApprovePaper {
            session_key,
            operator,
            reason,
            max_notional_usd_cents,
            valid_hours,
            qualification_report,
        } => {
            // approve_paper 会在本地完成资格/身份准备后请求服务端持久化审批；审批受理
            // 不是订单提交，后续仍须正式 Decision/Execution Gate。
            approve_paper(
                config,
                config_path,
                &session_key,
                &operator,
                &reason,
                max_notional_usd_cents,
                valid_hours,
                &qualification_report,
            )
            .await
        }
        StoreCommand::Backup { target } => print_json(&client.store_backup(&target).await?),
        StoreCommand::Restore { source, target } => {
            print_json(&client.store_restore(&source, &target).await?)
        }
        StoreCommand::ExportRun {
            run_id,
            target,
            include_raw_model,
        } => print_json(
            &client
                .store_export_run(&run_id, &target, include_raw_model)
                .await?,
        ),
        StoreCommand::ReleaseEvidence { run_id, target } => {
            // 服务端先生成带 bundle hash 的释放结果；只有指定 target 时才额外写一个新的
            // 本地文件，未指定 target 则保持纯 API 输出。
            let run_id = RunId(run_id);
            let bundle = client.store_release_evidence(&run_id).await?;
            if let Some(target) = target {
                export_release_evidence_bundle(&bundle, &target)?;
                print_json(&serde_json::json!({
                    "run_id": run_id,
                    "target": target,
                    "bundle_hash": bundle.bundle_hash,
                    "status": bundle.status,
                }))
            } else {
                print_json(&bundle)
            }
        }
        StoreCommand::Lesson { command } => lesson::run(config, command).await,
    }
}

fn export_release_evidence_bundle(bundle: &ReleaseEvidenceBundle, target: &Path) -> Result<()> {
    // 导出拒绝覆盖已有路径，并在父目录创建后一次性写入序列化 bundle；源 Store 和
    // 服务端 evidence 状态不因该文件导出而改变。
    if target.exists() {
        bail!(
            "release evidence target already exists: {}",
            target.display()
        );
    }
    let parent = target
        .parent()
        .context("release evidence target has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create release evidence directory {}", parent.display()))?;
    let bytes = serde_json::to_vec_pretty(bundle).context("serialize release evidence bundle")?;
    fs::write(target, bytes)
        .with_context(|| format!("write release evidence bundle {}", target.display()))
}
