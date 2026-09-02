use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let repository = manifest.join("../..");
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
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .ok()?;
    output.status.success().then_some(output.stdout)
}

fn source_tree_hash(repository: &Path) -> String {
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
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
