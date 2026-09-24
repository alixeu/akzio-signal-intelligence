//! Shell 分派回归：只运行临时 cargo 替身，不启动 Core、不访问真实 token/配置/网络。
//! 测试放在既有 Rust crate 内，scripts/ 只保留两个公开 .sh 入口。

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

struct Sandbox(PathBuf);

impl Sandbox {
    fn new() -> Self {
        let parent =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/command-script-tests");
        let path = parent.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(&path).unwrap();
        let cargo = path.join("cargo");
        fs::write(
            &cargo,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"$CAPTURE\"\nexit \"${MOCK_EXIT:-0}\"\n",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(cargo, fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self(path.canonicalize().unwrap())
    }

    fn run(&self, args: &[&str], status: &str) -> Output {
        let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/run_core.sh");
        Command::new("bash")
            .arg(script)
            .args(args)
            .current_dir(&self.0)
            .env("HOME", &self.0)
            .env("CAPTURE", self.0.join("args"))
            .env("MOCK_EXIT", status)
            .env(
                "PATH",
                format!("{}:{}", self.0.display(), std::env::var("PATH").unwrap()),
            )
            .output()
            .unwrap()
    }

    fn captured(&self) -> Vec<String> {
        fs::read_to_string(self.0.join("args"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn help_and_invalid_input_have_no_execution_side_effects() {
    let sandbox = Sandbox::new();
    for args in [
        vec![],
        vec!["help"],
        vec!["--help"],
        vec!["-h"],
        vec!["--config", "missing.toml", "--help"],
    ] {
        assert!(sandbox.run(&args, "99").status.success());
        assert!(!sandbox.0.join("args").exists());
    }
    for args in [
        vec!["--config"],
        vec!["unknown"],
        vec!["--dry-run", "paper"],
        vec!["--faker-online"],
        vec!["paper", "--research-only"],
        vec!["position-plan", "unexpected"],
        vec!["debug", "prepare"],
    ] {
        assert_eq!(sandbox.run(&args, "0").status.code(), Some(2));
        assert!(!sandbox.0.join("args").exists());
    }
    // 日常入口只保留 App 的两个动作；不留隐藏的运维/校准/任意 CLI 透传。
    for mode in [
        "build",
        "build-core",
        "check",
        "fixture",
        "serve",
        "ready",
        "health",
        "readiness",
        "detail",
        "inspect",
        "replay",
        "retrospectives",
        "events",
        "journal",
        "checkpoint",
        "trajectory",
        "export",
        "calibration",
        "store",
        "debug",
        "workflow",
        "evidence",
        "canary",
        "model-qualification",
        "daemon",
        "run",
        "cli",
        "cold-start",
    ] {
        assert_eq!(sandbox.run(&[mode], "0").status.code(), Some(2), "{mode}");
        assert!(!sandbox.0.join("args").exists());
    }
}

#[test]
fn shortcut_modes_map_to_one_native_cli_invocation() {
    for (mode, expected) in [
        (
            "position-plan",
            vec!["run", "start", "--purpose", "position-plan", "--auto-serve"],
        ),
        (
            "paper",
            vec!["run", "start", "--purpose", "paper", "--auto-serve"],
        ),
    ] {
        let sandbox = Sandbox::new();
        assert!(sandbox.run(&[mode], "0").status.success());
        let args = sandbox.captured();
        assert_eq!(
            &args[..7],
            [
                "run",
                "--locked",
                "--quiet",
                "-p",
                "akzio-cli",
                "--",
                "--config"
            ]
        );
        assert_eq!(
            args[7],
            sandbox.0.join(".akzio/config.toml").to_string_lossy()
        );
        assert_eq!(&args[8..], expected);
    }
}

#[test]
fn optional_config_preserves_paths_without_becoming_shell_commands() {
    for mode in ["position-plan", "paper"] {
        let sandbox = Sandbox::new();
        let relative = "my config;not-a-command.toml";
        assert!(sandbox
            .run(&["--config", relative, mode], "0")
            .status
            .success());
        let args = sandbox.captured();
        assert_eq!(args[7], sandbox.0.join(relative).to_string_lossy());
        assert_eq!(
            &args[8..],
            ["run", "start", "--purpose", mode, "--auto-serve"]
        );
    }
}

#[test]
fn submission_failure_propagates_without_retry_or_fallback() {
    for mode in ["position-plan", "paper"] {
        let sandbox = Sandbox::new();
        assert_eq!(sandbox.run(&[mode], "7").status.code(), Some(7));
        // 一次完整 argv 恰好 13 项；失败后不重试提交或切换模式。
        assert_eq!(sandbox.captured().len(), 13);
    }
}
