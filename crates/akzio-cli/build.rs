use std::{env, fs, path::PathBuf, process::Command};

use sha2::{Digest, Sha256};

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let repository = manifest.join("../..");
    println!(
        "cargo:rerun-if-changed={}",
        repository.join("Cargo.lock").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        repository.join("crates").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        repository.join(".git/HEAD").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        repository.join(".git/index").display()
    );

    let head = git(&repository, &["rev-parse", "HEAD"]);
    let mut state = Sha256::new();
    state.update(git_bytes(&repository, &["diff", "--binary", "HEAD"]));
    let untracked = git_bytes(
        &repository,
        &["ls-files", "--others", "--exclude-standard", "-z"],
    );
    for path in untracked
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        state.update(path);
        state.update([0]);
        let path = repository.join(String::from_utf8_lossy(path).as_ref());
        if let Ok(bytes) = fs::read(path) {
            state.update(bytes);
        }
        state.update([0]);
    }
    println!(
        "cargo:rustc-env=AKZIO_SOURCE_REVISION={}+{}",
        head.trim(),
        hex(&state.finalize())
    );
}

fn git(repository: &PathBuf, arguments: &[&str]) -> String {
    String::from_utf8(git_bytes(repository, arguments)).expect("git output UTF-8")
}

fn git_bytes(repository: &PathBuf, arguments: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .expect("run git");
    assert!(output.status.success(), "git command failed: {arguments:?}");
    output.stdout
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
