// 文件导读：Lesson 子命令通过认证 HTTP API 读取、创建和转换经验条目。CLI 只校验输入
// 形状并传输请求，Lesson 的 CAS provenance、生命周期、usage budget 和 revalidation
// 仍由 daemon/Store 决定；Active lesson 也不是 Policy、订单或 Outcome 的替代品。
// Rust 机制：`LessonCommand` 的 clap 宏生成枚举解析；`&ControlApiClient` 是共享借用，
// `async fn` 返回 Future，stdin/file 输入都归一为 `Result<String>`，Option lifecycle
// 保持“未筛选”与“指定状态”的语义差异。

use std::{
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use super::{Config, ControlApiClient};
use akzio_daemon::LessonInput;
use akzio_domain::LessonLifecycle;
use anyhow::{bail, Context, Result};
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub(crate) enum LessonCommand {
    Add {
        #[arg(long, default_value = "-")]
        file: PathBuf,
    },
    List {
        #[arg(long)]
        lifecycle: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    Show {
        lesson_id: String,
    },
    Usage {
        lesson_id: String,
    },
    Approve {
        lesson_id: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
    Contest {
        lesson_id: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
    Retire {
        lesson_id: String,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        reason: String,
    },
}

pub(crate) async fn run(config: &Config, command: LessonCommand) -> Result<()> {
    // 先构造认证 client，再按命令把本地 JSON/参数转成 Store API 请求；所有生命周期
    // 转换仍由 daemon 校验，CLI 只等待 Future 并打印响应。
    let client = ControlApiClient::from_config(config)?;
    match command {
        LessonCommand::Add { file } => add(&client, &file).await,
        LessonCommand::List { lifecycle, limit } => {
            list(&client, lifecycle.as_deref(), limit).await
        }
        LessonCommand::Show { lesson_id } => show(&client, &lesson_id).await,
        LessonCommand::Usage { lesson_id } => usage(&client, &lesson_id).await,
        LessonCommand::Approve {
            lesson_id,
            actor,
            reason,
        } => {
            transition(
                &client,
                &lesson_id,
                LessonLifecycle::Active,
                &actor,
                &reason,
            )
            .await
        }
        LessonCommand::Contest {
            lesson_id,
            actor,
            reason,
        } => {
            transition(
                &client,
                &lesson_id,
                LessonLifecycle::Contested,
                &actor,
                &reason,
            )
            .await
        }
        LessonCommand::Retire {
            lesson_id,
            actor,
            reason,
        } => {
            transition(
                &client,
                &lesson_id,
                LessonLifecycle::Retired,
                &actor,
                &reason,
            )
            .await
        }
    }
}

async fn add(client: &ControlApiClient, file: &Path) -> Result<()> {
    // Add 允许 stdin `-` 或显式文件，但只在本地完成反序列化和 authored_by 的基础检查；
    // 写入 Draft/CAS/source_refs 的原子语义由服务端负责。
    let input: LessonInput = serde_json::from_str(&read_input(file)?)
        .with_context(|| format!("parse Lesson input {}", file.display()))?;
    if input.authored_by.trim().is_empty() {
        bail!("authored_by must not be empty");
    }
    let payload = serde_json::to_value(input)?;
    crate::print_json(&client.lesson_add(&payload).await?)
}

async fn list(client: &ControlApiClient, lifecycle: Option<&str>, limit: usize) -> Result<()> {
    // lifecycle 是可选过滤条件；None 表示交给 Store 返回默认列表，而不是“没有生命周期”。
    if let Some(lifecycle) = lifecycle {
        parse_lifecycle(lifecycle)?;
    }
    crate::print_json(&client.lesson_list(lifecycle, limit).await?)
}

async fn show(client: &ControlApiClient, lesson_id: &str) -> Result<()> {
    // Show 是只读查询，返回缺失/错误由 HTTP client 的 Result 传播。
    crate::print_json(&client.lesson_show(lesson_id).await?)
}

async fn usage(client: &ControlApiClient, lesson_id: &str) -> Result<()> {
    // Usage 只展示当前使用预算与重验证状态，不把展示结果当作 Active lesson 权限。
    crate::print_json(&client.lesson_usage(lesson_id).await?)
}

async fn transition(
    client: &ControlApiClient,
    lesson_id: &str,
    lifecycle: LessonLifecycle,
    actor: &str,
    reason: &str,
) -> Result<()> {
    // 生命周期转换携带 actor/reason，作为可审计操作提交给 Store；命令成功只代表转换
    // 请求被服务端接受并持久化。
    crate::print_json(
        &client
            .lesson_transition(lesson_id, lifecycle, actor, reason)
            .await?,
    )
}

fn parse_lifecycle(value: &str) -> Result<LessonLifecycle> {
    // 先归一化大小写再交给 serde 的枚举解析，未知值返回错误而不猜测状态。
    serde_json::from_value(serde_json::Value::String(value.to_ascii_lowercase()))
        .with_context(|| format!("unsupported Lesson lifecycle {value}"))
}

fn read_input(path: &Path) -> Result<String> {
    // `-` 借用进程 stdin，其余路径读取文件；两条分支都返回拥有的 String，便于后续
    // 跨 async 调用传递且不会持有文件句柄。
    if path.as_os_str() == "-" {
        let mut input = String::new();
        io::stdin().read_to_string(&mut input)?;
        Ok(input)
    } else {
        Ok(fs::read_to_string(path)?)
    }
}
