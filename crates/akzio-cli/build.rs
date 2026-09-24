// 文件导读：本构建脚本只计算源码输入身份和 Rust 编译器版本，并把结果作为编译期环境
// 变量交给 CLI 的 RuntimeIdentity 使用。它不打开 Store、不启动 daemon，也不参与
// research、Decision、Execution、Paper submission、fill 或 Outcome 的运行时状态推进。
// Rust 机制：`build.rs` 在编译阶段运行；`PathBuf` 拥有路径，`&Path` 借用路径，`?`/`Option`
// 用于把外部命令和文件读取失败收敛为可控的回退，而 `println!("cargo:...")` 是 Cargo
// 识别的构建指令，不是应用运行时日志。

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

fn main() {
    // 构建脚本只把源树状态和编译器版本写入编译环境；它不读取运行时
    // Store，也不创建或激活任何研究、Decision 或 Execution 状态。
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let repository = manifest.join("../..");
    // 这些路径决定 Cargo 何时重新运行本脚本；目录变化会覆盖其中的源码、配置和文档。
    for watched in [
        "Cargo.lock",
        "Cargo.toml",
        "README.md",
        "rust-toolchain.toml",
        "crates",
        "config",
        "docs",
        "scripts",
        ".github",
        ".git/HEAD",
        ".git/index",
    ] {
        println!(
            "cargo:rerun-if-changed={}",
            repository.join(watched).display()
        );
    }

    // 优先保留 Git HEAD 及工作区内容的身份；Git 不可用时退回到受限源树哈希，
    // 使运行时仍能区分不同的源码输入。
    let source_revision = git_revision(&repository)
        .unwrap_or_else(|| format!("source-tree+{}", source_tree_hash(&repository)));
    println!("cargo:rustc-env=AKZIO_SOURCE_REVISION={source_revision}");

    let rustc = env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let rustc_version = Command::new(rustc)
        .arg("--version")
        .output()
        .expect("run rustc --version");
    assert!(rustc_version.status.success(), "rustc --version failed");
    println!(
        "cargo:rustc-env=AKZIO_RUSTC_VERSION={}",
        String::from_utf8(rustc_version.stdout)
            .expect("rustc version UTF-8")
            .trim()
    );
}

fn git_revision(repository: &Path) -> Option<String> {
    // HEAD 是稳定前缀，后缀同时覆盖已跟踪差异和未跟踪文件的路径与内容；
    // 任一 Git 查询失败都让调用方使用非 Git 的源树回退值。
    let head = String::from_utf8(git_bytes(repository, &["rev-parse", "HEAD"])?).ok()?;
    let mut state = Sha256::new();
    state.update(git_bytes(repository, &["diff", "--binary", "HEAD"])?);
    let untracked = git_bytes(
        repository,
        &["ls-files", "--others", "--exclude-standard", "-z"],
    )?;
    for relative in untracked
        .split(|byte| *byte == 0)
        .filter(|relative| !relative.is_empty())
    {
        state.update(relative);
        state.update([0]);
        if let Ok(bytes) = fs::read(repository.join(String::from_utf8_lossy(relative).as_ref())) {
            state.update(bytes);
        }
        state.update([0]);
    }
    Some(format!("{}+{}", head.trim(), hex(&state.finalize())))
}

fn git_bytes(repository: &Path, arguments: &[&str]) -> Option<Vec<u8>> {
    // 只接受成功退出的 Git 标准输出；命令启动失败、非零退出或 stderr
    // 都统一转成 None，由上层决定是否使用回退身份。
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .ok()?;
    output.status.success().then_some(output.stdout)
}

fn source_tree_hash(repository: &Path) -> String {
    // 回退哈希只覆盖与构建相关的源码、配置和脚本，并按相对路径排序，
    // 以消除目录遍历顺序差异；本地覆盖文件不参与身份计算。
    let mut files = Vec::new();
    for relative in [
        "Cargo.lock",
        "Cargo.toml",
        "README.md",
        "rust-toolchain.toml",
        "crates",
        "config",
        "docs",
        "scripts",
        ".github",
    ] {
        collect_files(&repository.join(relative), &mut files);
    }
    files.sort();

    let mut state = Sha256::new();
    for file in files {
        let Ok(relative) = file.strip_prefix(repository) else {
            continue;
        };
        let relative = relative.to_string_lossy();
        if relative.contains(".local.") || relative.ends_with(".env") {
            continue;
        }
        state.update(relative.as_bytes());
        state.update([0]);
        if let Ok(bytes) = fs::read(&file) {
            state.update(bytes);
        }
        state.update([0]);
    }
    hex(&state.finalize())
}

fn collect_files(candidate: &Path, files: &mut Vec<PathBuf>) {
    // 递归收集文件；无法读取的目录被跳过，因为该函数服务于 Git 回退路径，
    // 不能把目录读取失败误当成运行时业务错误。
    if candidate.is_file() {
        files.push(candidate.to_path_buf());
        return;
    }
    let Ok(entries) = fs::read_dir(candidate) else {
        return;
    };
    for entry in entries.flatten() {
        collect_files(&entry.path(), files);
    }
}

fn hex(bytes: &[u8]) -> String {
    // 将摘要的每个字节编码为两个小写十六进制字符，供环境变量和报告使用。
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
