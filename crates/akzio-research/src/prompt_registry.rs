//! Unified source inventory; each feature owns its text and typed builders.
//! Includes both prose documents and Rust-owned short guidance. No runtime loading.

// 这里是 Prompt 的静态所有权登记，不是运行时的任意文件读取入口。每个 source
// 同时保存稳定 ID、物理路径和编译期字节；Contract/PromptBundle 只从这些已登记
// 内容建立身份，Outcome 的历史冻结文本也因此可以被完整性校验而不获得执行权限。
pub(crate) struct PromptSource {
    pub id: &'static str,
    pub path: &'static str,
    pub bytes: &'static [u8],
    pub contract: bool,
}

pub(crate) const SOURCES: &[PromptSource] = &[
    PromptSource {
        id: "research.governance",
        path: "crates/akzio-research/src/agent/prompts/shared.md",
        bytes: include_bytes!("agent/prompts/shared.md"),
        contract: true,
    },
    PromptSource {
        id: "research.analyst",
        path: "crates/akzio-research/src/agent/prompts/roles/analyst.md",
        bytes: include_bytes!("agent/prompts/roles/analyst.md"),
        contract: true,
    },
    PromptSource {
        id: "research.critic",
        path: "crates/akzio-research/src/agent/prompts/roles/critic.md",
        bytes: include_bytes!("agent/prompts/roles/critic.md"),
        contract: true,
    },
    PromptSource {
        id: "research.synthesizer",
        path: "crates/akzio-research/src/agent/prompts/roles/synthesizer.md",
        bytes: include_bytes!("agent/prompts/roles/synthesizer.md"),
        contract: true,
    },
    PromptSource {
        id: "research.proposal_reviewer",
        path: "crates/akzio-research/src/agent/prompts/roles/proposal_reviewer.md",
        bytes: include_bytes!("agent/prompts/roles/proposal_reviewer.md"),
        contract: true,
    },
    PromptSource {
        id: "research.review_schema",
        path: "crates/akzio-research/src/agent/proposal_review.rs",
        bytes: include_bytes!("agent/proposal_review.rs"),
        contract: true,
    },
    PromptSource {
        id: "research.outcome",
        path: "crates/akzio-research/src/agent/prompts/roles/outcome.md",
        bytes: include_bytes!("agent/prompts/roles/outcome.md"),
        contract: true,
    },
    PromptSource {
        id: "research.renderer",
        path: "crates/akzio-research/src/agent/prompts/mod.rs",
        bytes: include_bytes!("agent/prompts/mod.rs"),
        contract: true,
    },
    PromptSource {
        id: "research.phases",
        path: "crates/akzio-research/src/agent/prompts/phases.rs",
        bytes: include_bytes!("agent/prompts/phases.rs"),
        contract: false,
    },
    PromptSource {
        id: "research.tool_contracts",
        path: "crates/akzio-research/src/agent/schemas.rs",
        bytes: include_bytes!("agent/schemas.rs"),
        contract: true,
    },
    PromptSource {
        id: "research.tool_guidance",
        path: "crates/akzio-research/src/agent/tools.rs",
        bytes: include_bytes!("agent/tools.rs"),
        contract: true,
    },
    PromptSource {
        id: "ingest.source_verifier",
        path: "crates/akzio-ingest/src/prompts/source_verifier.md",
        bytes: include_bytes!("../../akzio-ingest/src/prompts/source_verifier.md"),
        contract: false,
    },
    PromptSource {
        id: "ingest.acquisition",
        path: "crates/akzio-ingest/src/prompts.rs",
        bytes: include_bytes!("../../akzio-ingest/src/prompts.rs"),
        contract: false,
    },
    PromptSource {
        id: "context.guidance",
        path: "crates/akzio-context/src/context_broker/guidance.rs",
        bytes: include_bytes!("../../akzio-context/src/context_broker/guidance.rs"),
        contract: false,
    },
    PromptSource {
        id: "model.probes",
        path: "crates/akzio-model/src/model_client/probe_prompts.rs",
        bytes: include_bytes!("../../akzio-model/src/model_client/probe_prompts.rs"),
        contract: false,
    },
    PromptSource {
        id: "ingest.preflight_example",
        path: "crates/akzio-ingest/examples/native_web_preflight.rs",
        bytes: include_bytes!("../../akzio-ingest/examples/native_web_preflight.rs"),
        contract: false,
    },
];

pub(crate) fn components(
    contract_only: bool,
) -> impl Iterator<Item = (&'static str, &'static [u8])> {
    // 将 ownership ID 与物理路径分别作为哈希组件，防止只换路径或只换拥有者
    // 却保持字节不变时悄悄复用旧 Contract 身份。
    SOURCES
        .iter()
        .filter(move |source| !contract_only || source.contract)
        .flat_map(|source| {
            // Bind ownership ID and physical source, not only the current file contents.
            [
                (source.id, source.path.as_bytes()),
                (source.path, source.bytes),
            ]
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeSet, path::Path};

    fn documents(path: &Path, root: &Path, paths: &mut BTreeSet<String>) {
        // 测试递归扫描 Prompt 目录，只比较登记覆盖范围；它不把目录扫描开放给 Agent。
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                documents(&path, root, paths);
            } else if path.extension().is_some_and(|ext| ext == "md") {
                paths.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned(),
                );
            }
        }
    }

    #[test]
    fn registry_covers_owned_documents_and_unique_source_identities() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut ids = BTreeSet::new();
        let mut paths = BTreeSet::new();
        for source in SOURCES {
            assert!(ids.insert(source.id), "duplicate ID: {}", source.id);
            assert!(
                paths.insert(source.path),
                "duplicate source: {}",
                source.path
            );
            assert!(!source.bytes.is_empty(), "{}", source.id);
            assert!(std::str::from_utf8(source.bytes).is_ok(), "{}", source.id);
            assert_eq!(std::fs::read(root.join(source.path)).unwrap(), source.bytes);
        }
        let mut actual = BTreeSet::new();
        documents(
            &root.join("crates/akzio-research/src/agent/prompts"),
            &root,
            &mut actual,
        );
        documents(
            &root.join("crates/akzio-ingest/src/prompts"),
            &root,
            &mut actual,
        );
        let registered = SOURCES
            .iter()
            .filter(|source| source.path.ends_with(".md"))
            .map(|source| source.path.to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, registered, "register every owned prompt document");
    }
}
