//! Architecture, dependency-direction, redline, and anti-slop guardrails.
//!
//! Enforces invariants from `AGENTS.md` mechanically in `cargo test` instead of
//! relying solely on prose rules:
//! 1. Crate dependency DAG & isolation (`akzio-domain` pure, `rusqlite` only in `akzio-store`, etc.).
//! 2. `[lints] workspace = true` inheritance across all workspace crates.
//! 3. Production redlines: no live Alpaca endpoint, no retired v1 names, `unsafe` quarantine.
//! 4. Anti-slop ratchets: zero new `#[allow(...)]`, zero new `#[ignore]`, `rustfmt` on formatted `include!` files.
//! 5. Repository hygiene & `AGENTS.md` size budget (`<= 6_000` bytes).

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crate sits under <workspace>/crates/<name>")
        .to_path_buf()
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut sorted: Vec<PathBuf> = entries.filter_map(Result::ok).map(|e| e.path()).collect();
    sorted.sort();
    for path in sorted {
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn rel_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("path inside workspace")
        .to_string_lossy()
        .replace('\\', "/")
}

#[derive(Default)]
struct CrateManifest {
    deps: BTreeSet<String>,
    dev_deps: BTreeSet<String>,
    has_lints_workspace: bool,
}

fn parse_crate_manifest(path: &Path) -> CrateManifest {
    let raw = fs::read_to_string(path).expect("read crate Cargo.toml");
    let mut manifest = CrateManifest::default();
    let mut section = String::new();
    let mut in_lints_section = false;

    for raw_line in raw.lines() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            section = line.trim_matches(&['[', ']'][..]).trim().to_owned();
            in_lints_section = section == "lints";
            continue;
        }
        if in_lints_section && line.replace(' ', "") == "workspace=true" {
            manifest.has_lints_workspace = true;
            continue;
        }
        if let Some((key, _)) = line.split_once('=') {
            let dep = key.trim().to_owned();
            if dep.is_empty() || dep.contains('.') {
                continue;
            }
            match section.as_str() {
                "dependencies" => {
                    manifest.deps.insert(dep);
                }
                "dev-dependencies" => {
                    manifest.dev_deps.insert(dep);
                }
                _ => {}
            }
        }
    }
    manifest
}

#[test]
fn workspace_crates_inherit_lints_and_keep_dependency_dag() {
    let root = workspace_root();
    let root_toml = fs::read_to_string(root.join("Cargo.toml")).expect("read root Cargo.toml");
    assert!(
        root_toml.contains("[workspace.lints.rust]")
            && root_toml.contains("[workspace.lints.clippy]"),
        "Root Cargo.toml must define [workspace.lints.rust] and [workspace.lints.clippy]"
    );
    assert!(
        root.join("clippy.toml").is_file(),
        "Root clippy.toml must exist for disallowed_* compile-time guardrails"
    );

    let expected_crates = [
        "akzio-domain",
        "akzio-store",
        "akzio-context",
        "akzio-ingest",
        "akzio-runtime",
        "akzio-execution",
        "akzio-learning",
        "akzio-model",
        "akzio-research",
        "akzio-daemon",
        "akzio-cli",
    ];

    let mut manifests = BTreeMap::new();
    for krate in expected_crates {
        let path = root.join("crates").join(krate).join("Cargo.toml");
        assert!(path.is_file(), "Missing workspace crate manifest: {krate}");
        let parsed = parse_crate_manifest(&path);
        assert!(
            parsed.has_lints_workspace,
            "crates/{krate}/Cargo.toml must inherit `[lints] workspace = true`"
        );
        manifests.insert(krate, parsed);
    }

    // Also verify no extra unapproved crate folders exist under crates/.
    let actual_dirs: BTreeSet<String> = fs::read_dir(root.join("crates"))
        .expect("read crates/")
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    let expected_set: BTreeSet<String> = expected_crates.iter().map(|s| (*s).to_owned()).collect();
    assert_eq!(
        actual_dirs, expected_set,
        "Workspace crate inventory changed unexpectedly"
    );

    // 1. rusqlite exclusivity: ONLY akzio-store may depend on rusqlite.
    for (krate, m) in &manifests {
        if *krate != "akzio-store" {
            assert!(
                !m.deps.contains("rusqlite") && !m.dev_deps.contains("rusqlite"),
                "{krate} must not depend on rusqlite; akzio-store is the sole persistence authority"
            );
        }
    }

    // 2. akzio-domain must stay pure (no akzio-* deps, no I/O/network/async/DB).
    let domain = &manifests["akzio-domain"];
    for dep in domain.deps.union(&domain.dev_deps) {
        assert!(
            !dep.starts_with("akzio-"),
            "akzio-domain must not depend on workspace crate {dep}"
        );
        assert!(
            !matches!(
                dep.as_str(),
                "rusqlite"
                    | "reqwest"
                    | "tokio"
                    | "tokio-util"
                    | "axum"
                    | "hyper"
                    | "libc"
                    | "zstd"
                    | "zip"
                    | "anyhow"
                    | "async-stream"
                    | "futures"
            ),
            "akzio-domain must stay I/O-free; forbidden dependency: {dep}"
        );
    }

    // 3. Leaf/core layer boundaries.
    let assert_allowed_akzio_deps = |krate: &str, allowed: &[&str], forbidden: &[&str]| {
        let m = &manifests[krate];
        let allowed_set: BTreeSet<&str> = allowed.iter().copied().collect();
        for dep in &m.deps {
            if dep.starts_with("akzio-") {
                assert!(
                    allowed_set.contains(dep.as_str()),
                    "{krate} has disallowed workspace dependency `{dep}` (allowed: {allowed:?})"
                );
            }
            assert!(
                !forbidden.contains(&dep.as_str()),
                "{krate} has forbidden dependency `{dep}`"
            );
        }
    };

    assert_allowed_akzio_deps("akzio-model", &[], &["rusqlite", "axum", "libc"]);
    assert_allowed_akzio_deps(
        "akzio-store",
        &["akzio-domain"],
        &["reqwest", "tokio", "axum", "libc"],
    );
    assert_allowed_akzio_deps(
        "akzio-runtime",
        &["akzio-domain", "akzio-store"],
        &["reqwest", "axum", "rusqlite", "libc"],
    );
    assert_allowed_akzio_deps(
        "akzio-context",
        &["akzio-domain", "akzio-store"],
        &["reqwest", "tokio", "axum", "rusqlite", "libc"],
    );
    assert_allowed_akzio_deps(
        "akzio-learning",
        &["akzio-domain", "akzio-store"],
        &["reqwest", "tokio", "axum", "rusqlite", "libc"],
    );
    assert_allowed_akzio_deps(
        "akzio-execution",
        &["akzio-domain", "akzio-store"],
        &["axum", "rusqlite", "libc"],
    );
    assert_allowed_akzio_deps(
        "akzio-ingest",
        &["akzio-domain", "akzio-model", "akzio-store"],
        &["axum", "rusqlite", "libc"],
    );
    assert_allowed_akzio_deps(
        "akzio-research",
        &[
            "akzio-context",
            "akzio-domain",
            "akzio-ingest",
            "akzio-model",
            "akzio-runtime",
            "akzio-store",
        ],
        &["reqwest", "axum", "rusqlite", "libc"],
    );
    assert_allowed_akzio_deps(
        "akzio-daemon",
        &[
            "akzio-context",
            "akzio-domain",
            "akzio-execution",
            "akzio-ingest",
            "akzio-learning",
            "akzio-model",
            "akzio-research",
            "akzio-runtime",
            "akzio-store",
        ],
        &["akzio-cli", "rusqlite"],
    );
    assert_allowed_akzio_deps(
        "akzio-cli",
        &[
            "akzio-daemon",
            "akzio-domain",
            "akzio-execution",
            "akzio-ingest",
            "akzio-learning",
            "akzio-model",
            "akzio-research",
            "akzio-store",
        ],
        &["rusqlite"],
    );
}

#[test]
fn rust_sources_enforce_redlines_and_anti_slop_ratchets() {
    let root = workspace_root();
    let mut files = Vec::new();
    collect_rs_files(&root.join("crates"), &mut files);

    // Frozen baseline counts for `#[allow(` / `#![allow(`:
    // Removing existing `#[allow(` is encouraged; adding ANY new `#[allow(` fails this test.
    let allow_ratchet: BTreeMap<&str, usize> = BTreeMap::from([
        ("crates/akzio-cli/src/cli/observatory_config.rs", 1),
        ("crates/akzio-context/src/context_broker/manifest.rs", 2),
        ("crates/akzio-context/src/context_broker/reads.rs", 1),
        ("crates/akzio-context/src/context_broker/selection.rs", 1),
        ("crates/akzio-domain/src/artifact.rs", 1),
        ("crates/akzio-domain/src/canary.rs", 3),
        ("crates/akzio-domain/src/contract.rs", 1),
        ("crates/akzio-execution/src/decision_gate.rs", 1),
        ("crates/akzio-execution/src/decision_gate/decide.rs", 1),
        ("crates/akzio-execution/src/snapshot.rs", 1),
        ("crates/akzio-learning/src/evaluation/outcomes.rs", 1),
        ("crates/akzio-learning/src/evaluation/policy_learning.rs", 2),
        ("crates/akzio-research/src/agent/model_types.rs", 1),
        ("crates/akzio-research/src/agent/runtime_helpers.rs", 1),
        ("crates/akzio-research/src/quality.rs", 1),
        ("crates/akzio-runtime/src/runtime/workflow.rs", 3),
        ("crates/akzio-store/src/store/canary/reservation.rs", 1),
        ("crates/akzio-store/src/store/debug.rs", 2),
        ("crates/akzio-store/src/store/execution.rs", 2),
        ("crates/akzio-store/src/store/free_paper_checks.rs", 1),
        ("crates/akzio-store/src/store/lesson.rs", 1),
        ("crates/akzio-store/src/store/lesson/write.rs", 1),
        ("crates/akzio-store/src/store/run_control.rs", 2),
    ]);

    let ignore_ratchet: BTreeMap<&str, usize> = BTreeMap::from([
        ("crates/akzio-ingest/src/adapters.rs", 1),
        ("crates/akzio-ingest/src/direct.rs", 1),
    ]);

    // Build forbidden tokens indirectly so this file never flags itself if scanned.
    let live_alpaca_host = ["https://", "api.alpaca.markets"].concat();
    let live_alpaca_http = ["http://", "api.alpaca.markets"].concat();
    let retired_v1_crate = ["orchestrator", "-"].concat();
    let retired_filestore = ["File", "Store"].concat();
    let retired_outputs_store = ["outputs/", "store"].concat();
    let allow_attr_prefix = ["#[", "allow("].concat();
    let crate_allow_attr_prefix = ["#![", "allow("].concat();
    let ignore_attr_prefix = ["#[", "ignore"].concat();
    let unsafe_kw = ["unsafe", " "].concat();
    let unsafe_brace = ["unsafe", "{"].concat();

    for path in files {
        let rel = rel_path(&root, &path);
        if rel == "crates/akzio-daemon/tests/architecture_guardrails.rs" {
            continue;
        }
        let text = fs::read_to_string(&path).expect("read .rs file");

        // Redline 1: Live Alpaca trading endpoint & retired v1 tokens never appear anywhere.
        assert!(
            !text.contains(&live_alpaca_host) && !text.contains(&live_alpaca_http),
            "Forbidden live Alpaca endpoint reference found in {rel}"
        );
        assert!(
            !text.contains(&retired_v1_crate)
                && !text.contains(&retired_filestore)
                && !text.contains(&retired_outputs_store),
            "Forbidden retired v1 identifier found in {rel}"
        );

        // Redline 2: akzio-domain stays I/O-free.
        if rel.starts_with("crates/akzio-domain/src/") {
            for forbidden in [
                "std::fs",
                "std::net",
                "std::process",
                "tokio::",
                "reqwest::",
                "rusqlite::",
            ] {
                assert!(
                    !text.contains(forbidden),
                    "akzio-domain must stay I/O-free; found `{forbidden}` in {rel}"
                );
            }
        }

        // Redline 3: rusqlite stays quarantined inside akzio-store implementation (and not exported).
        if !rel.starts_with("crates/akzio-store/") {
            assert!(
                !text.contains("rusqlite::"),
                "Only akzio-store may touch rusqlite; found in {rel}"
            );
        } else if rel == "crates/akzio-store/src/lib.rs" {
            for line in text.lines() {
                let code = line.split("//").next().unwrap_or("");
                assert!(
                    !code.contains("rusqlite"),
                    "akzio-store/src/lib.rs must never export rusqlite types"
                );
            }
        }

        // Redline 4: unsafe blocks quarantined strictly to akzio-daemon/src/run_log.rs.
        if rel != "crates/akzio-daemon/src/run_log.rs" {
            for (idx, raw_line) in text.lines().enumerate() {
                let code = raw_line.split("//").next().unwrap_or("");
                assert!(
                    !code.contains(&unsafe_kw) && !code.contains(&unsafe_brace),
                    "Unauthorized `unsafe` at {rel}:{} (only run_log.rs getpwuid_r is audited)",
                    idx + 1
                );
            }
        }

        // Ratchet 5: No new #[allow(...)] / #![allow(...)].
        let normalized: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        let allow_count = normalized.matches(&allow_attr_prefix).count()
            + normalized.matches(&crate_allow_attr_prefix).count();
        let max_allows = allow_ratchet.get(rel.as_str()).copied().unwrap_or(0);
        assert!(
            allow_count <= max_allows,
            "{rel} has {allow_count} allow attributes (ratchet max: {max_allows}); fix the code instead of adding #[allow]"
        );

        // Ratchet 6: No new #[ignore].
        let ignore_count = normalized.matches(&ignore_attr_prefix).count();
        let max_ignores = ignore_ratchet.get(rel.as_str()).copied().unwrap_or(0);
        assert!(
            ignore_count <= max_ignores,
            "{rel} has {ignore_count} #[ignore] attributes (ratchet max: {max_ignores}); do not silence tests"
        );
    }
}

#[test]
fn include_fragments_are_rustfmt_ratcheted() {
    let root = workspace_root();
    let mut files = Vec::new();
    collect_rs_files(&root.join("crates"), &mut files);

    // Frozen legacy unformatted `include!` fragments.
    // Removing entries from this list as files get formatted is valid; adding new unformatted
    // `include!` files is forbidden because `cargo fmt --all` does not check `include!` targets.
    let legacy_unformatted: BTreeSet<&str> = BTreeSet::from([
        "crates/akzio-cli/src/cli/calibration.rs",
        "crates/akzio-cli/src/cli/dispatch.rs",
        "crates/akzio-cli/src/cli/identity.rs",
        "crates/akzio-cli/src/cli/main.rs",
        "crates/akzio-cli/src/cli/observatory_config.rs",
        "crates/akzio-cli/src/cli/run_commands.rs",
        "crates/akzio-context/src/context_broker/grants.rs",
        "crates/akzio-context/src/context_broker/manifest.rs",
        "crates/akzio-context/src/context_broker/materialization.rs",
        "crates/akzio-context/src/context_broker/policy.rs",
        "crates/akzio-context/src/context_broker/reads.rs",
        "crates/akzio-context/src/context_broker/selection.rs",
        "crates/akzio-daemon/src/http_launch.rs",
        "crates/akzio-domain/src/evaluation/risk_ground_truth.rs",
        "crates/akzio-execution/src/decision_gate/decide.rs",
        "crates/akzio-execution/src/decision_gate/validate.rs",
        "crates/akzio-execution/src/execution_gate/validation.rs",
        "crates/akzio-ingest/src/materialization/materialize_normalized.rs",
        "crates/akzio-learning/src/evaluation/materialization.rs",
        "crates/akzio-learning/src/evaluation/risk_ground_truth.rs",
        "crates/akzio-learning/src/evaluation/runtime_setup.rs",
        "crates/akzio-model/src/model_client/capability_probe.rs",
        "crates/akzio-research/src/agent/budget.rs",
        "crates/akzio-research/src/agent/errors_catalogue.rs",
        "crates/akzio-research/src/agent/helpers.rs",
        "crates/akzio-research/src/agent/projection.rs",
        "crates/akzio-research/src/agent/recovery.rs",
        "crates/akzio-research/src/agent/runtime_core.rs",
        "crates/akzio-research/src/agent/runtime_helpers.rs",
        "crates/akzio-research/src/agent/runtime_run.rs",
        "crates/akzio-research/src/agent/structured.rs",
        "crates/akzio-runtime/src/runtime/compilation/evidence.rs",
        "crates/akzio-runtime/src/runtime/compilation/lowering.rs",
        "crates/akzio-runtime/src/runtime/compilation/validation.rs",
        "crates/akzio-store/src/store/canary/cohort.rs",
        "crates/akzio-store/src/store/canary/helpers.rs",
        "crates/akzio-store/src/store/canary/history.rs",
        "crates/akzio-store/src/store/canary/reservation.rs",
        "crates/akzio-store/src/store/canary/stage.rs",
        "crates/akzio-store/src/store/free_paper_checks.rs",
        "crates/akzio-store/src/store/free_policy_helpers.rs",
        "crates/akzio-store/src/store/free_policy_reads.rs",
        "crates/akzio-store/src/store/free_reads.rs",
        "crates/akzio-store/src/store/free_validation.rs",
        "crates/akzio-store/src/store/impl_core.rs",
        "crates/akzio-store/src/store/impl_history.rs",
        "crates/akzio-store/src/store/impl_learning.rs",
        "crates/akzio-store/src/store/impl_queries.rs",
        "crates/akzio-store/src/store/impl_workflow.rs",
        "crates/akzio-store/src/store/learning/history.rs",
        "crates/akzio-store/src/store/learning/outcome.rs",
        "crates/akzio-store/src/store/learning/policy.rs",
        "crates/akzio-store/src/store/learning/shadow.rs",
        "crates/akzio-store/src/store/lesson/write.rs",
        "crates/akzio-store/src/store/prelude.rs",
        "crates/akzio-store/src/store/workflow/contracts.rs",
        "crates/akzio-store/src/store/workflow/helpers.rs",
        "crates/akzio-store/src/store/workflow/outputs.rs",
    ]);

    let mut include_targets = BTreeSet::new();
    for path in files {
        let text = fs::read_to_string(&path).expect("read .rs file");
        let parent = path.parent().expect("file has parent dir");
        for line in text.lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("include!(\"") {
                if let Some(rel_inc) = rest.strip_suffix("\");") {
                    let target = parent.join(rel_inc);
                    assert!(
                        target.is_file(),
                        "Broken include! target `{rel_inc}` referenced by {}",
                        rel_path(&root, &path)
                    );
                    include_targets.insert(rel_path(&root, &target));
                }
            }
        }
    }

    assert!(
        !include_targets.is_empty(),
        "Expected include! fragments to be discovered"
    );

    let must_be_formatted: Vec<PathBuf> = include_targets
        .iter()
        .filter(|p| !legacy_unformatted.contains(p.as_str()))
        .map(|p| root.join(p))
        .collect();

    assert!(
        !must_be_formatted.is_empty(),
        "Expected formatted include! fragments in ratchet"
    );

    let output = Command::new("rustfmt")
        .arg("--edition")
        .arg("2021")
        .arg("--check")
        .args(&must_be_formatted)
        .current_dir(&root)
        .output()
        .expect("run rustfmt on formatted include! fragments");

    assert!(
        output.status.success(),
        "Formatted include! fragment regressed:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn repo_hygiene_and_agents_md_budget() {
    let root = workspace_root();

    // 1. No root `tests/` directory (`AGENTS.md` §4).
    assert!(
        !root.join("tests").exists(),
        "Do not add a root tests/ directory; keep offline tests inside crates/"
    );

    // 2. scripts/ must contain ONLY run_core.sh (no Python/extra shell drift).
    let scripts: BTreeSet<String> = fs::read_dir(root.join("scripts"))
        .expect("read scripts/")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        scripts,
        BTreeSet::from(["run_core.sh".to_owned()]),
        "scripts/ must stay minimal and contain only run_core.sh"
    );

    // 3. AGENTS.md must stay concise (anti-bloat rule).
    let agents_md = fs::read_to_string(root.join("AGENTS.md")).expect("read AGENTS.md");
    assert!(
        agents_md.len() <= 6_000,
        "AGENTS.md bloated to {} bytes (max 6000); move rules to lints/tests/docs instead of prose",
        agents_md.len()
    );
    assert!(
        agents_md.lines().count() <= 85,
        "AGENTS.md bloated to {} lines (max 85); keep AGENTS.md a compact index",
        agents_md.lines().count()
    );
}
