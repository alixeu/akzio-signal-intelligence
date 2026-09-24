use super::*;

// Catalogue 把 Store 持久化的 Active Contract head 与 Rust recipe catalogue 绑定；候选永远没有隐式执行路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledContract {
    // contract 是已验证的协议值，artifact 保留其 CAS/Store provenance。
    pub contract: AgentContract,
    pub artifact: Artifact,
}

/// The bounded initial topology is expressed as installed Contracts, never as
/// a role registry. Daemon bootstrap consumes this pair atomically at the API
/// boundary: contracts drive model turns; recipes drive Rust DAG lowering.
#[derive(Debug, Clone)]
pub struct ActiveResearchCatalogue {
    // contracts 决定模型 turns，recipes 决定 Rust DAG lowering；两者从同一 active head 原子恢复。
    pub contracts: ContractCatalogue,
    pub recipes: RecipeCatalogue,
}

impl ActiveResearchCatalogue {
    /// Restore the Store-owned active heads, bootstrapping only a fresh Store
    /// with the immutable Rust-defined defaults. Candidates deliberately have
    /// no execution path until a canonical Paper-backed transition promotes
    /// their persisted head.
    pub fn install(store: &Store, now: DateTime<Utc>) -> ResearchResult<Self> {
        // 安装前检查 legacy retirement；fresh Store 才允许 canonical bootstrap，已有 head 走 Store-owned upgrade 规则。
        store.check_legacy_workflow_retirement(now)?;
        let contracts = ContractCatalogue::load_or_bootstrap_active(
            store,
            canonical_active_contracts(store)?,
            now,
        )?;
        let recipes = contracts.active_recipe_catalogue(store)?;
        Ok(Self { contracts, recipes })
    }

    /// Persist a capability-bounded candidate beneath an installed Active
    /// Contract. The Store, rather than this process-local catalogue, owns its
    /// immutable installation and later policy-driven activation.
    pub fn install_candidate(
        &self,
        store: &Store,
        active_contract_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> ResearchResult<InstalledContract> {
        // candidate 先由 active contract 做 capability subset 校验，再由 Store 持久化为不可变候选。
        self.contracts
            .install_candidate(store, active_contract_hash, candidate, now)
    }

    pub fn install_analyst_freshness_candidate(
        &self,
        store: &Store,
        now: DateTime<Utc>,
    ) -> ResearchResult<InstalledContract> {
        // freshness candidate 只改变 analyst prompt/version 并重新计算 hash，不替换当前 active head。
        let active = self
            .contracts
            .contracts()
            .find(|installed| installed.contract.purpose.as_str() == RESEARCH_ANALYST_RECIPE_ID)
            .ok_or(ResearchError::MissingActiveContract(
                RESEARCH_ANALYST_RECIPE_ID,
            ))?;
        let mut candidate = active.contract.clone();
        candidate.version = ANALYST_FRESHNESS_CANDIDATE_VERSION;
        candidate.prompt.version = ANALYST_FRESHNESS_CANDIDATE_VERSION;
        let mut role = store.read_blob(&candidate.prompt.role)?;
        role.extend_from_slice(prompts::ANALYST_FRESHNESS_GUIDANCE.as_bytes());
        candidate.prompt.role = store.stage_bytes(&role, "text/plain")?;
        candidate.contract_hash = candidate.expected_hash()?;
        candidate.validate()?;
        self.install_candidate(store, &active.contract.contract_hash, &candidate, now)
    }
}

pub const ACTIVE_RESEARCH_MAX_NODES: usize = 32;

// Contract/Prompt 版本是冻结 wire identity；Candidate 版本独立于当前 active 版本，等待显式 policy transition。
pub(super) const COMPACT_SUBMISSION_CONTRACT_VERSION: u32 = 51;
pub(super) const ACTIVE_CONTRACT_VERSION: u32 = 69;
pub(super) const ACTIVE_PROMPT_BUNDLE_VERSION: u32 = 38;
pub const ANALYST_FRESHNESS_CANDIDATE_VERSION: u32 = 70;

pub(super) const RFC3339_TIMESTAMP_PATTERN: &str =
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$";

#[derive(Debug, Clone, Default)]
pub struct ContractCatalogue {
    by_hash: BTreeMap<akzio_domain::ContentHash, InstalledContract>,
    by_identity: BTreeMap<(akzio_domain::ContractId, u32), akzio_domain::ContentHash>,
}

impl ContractCatalogue {
    fn load_or_bootstrap_active(
        store: &Store,
        contracts: impl IntoIterator<Item = AgentContract>,
        now: DateTime<Utc>,
    ) -> ResearchResult<Self> {
        // load_or_bootstrap_active 先批量检查 canonical upgrade，再逐项加载，避免部分角色先升级而其他角色被阻断。
        store.check_legacy_workflow_retirement(now)?;
        let contracts = contracts.into_iter().collect::<Vec<_>>();
        validate_unique_contracts(&contracts)?;
        // Reject a blocked release before partially upgrading unrelated roles.
        // Each activation repeats these checks in its own write transaction.
        for contract in &contracts {
            match store.active_contract(&contract.purpose)? {
                Some(stored) if stored.contract.contract_hash == contract.contract_hash => {}
                Some(stored) if stored.contract.version < contract.version => store
                    .check_canonical_contract_upgrade(&stored.contract.contract_hash, contract)?,
                Some(_) => {
                    return Err(ResearchError::NonCanonicalActiveContract(
                        contract.purpose.as_str().to_owned(),
                    ));
                }
                None => {}
            }
        }
        let mut by_hash = BTreeMap::new();
        let mut by_identity = BTreeMap::new();
        for contract in contracts {
            let stored = match store.active_contract(&contract.purpose)? {
                Some(stored) if stored.contract.contract_hash == contract.contract_hash => stored,
                Some(stored) if stored.contract.version < contract.version => store
                    .install_canonical_contract_upgrade(
                        &stored.contract.contract_hash,
                        &contract,
                        now,
                    )?,
                Some(_) => {
                    return Err(ResearchError::NonCanonicalActiveContract(
                        contract.purpose.as_str().to_owned(),
                    ));
                }
                None => store.install_active_contract(&contract, now)?,
            };
            let contract = stored.contract;
            contract.validate()?;
            model_tool_definitions(&ContextBroker::new(store.clone()), &contract)?;
            if by_hash.contains_key(&contract.contract_hash) {
                return Err(ResearchError::DuplicateContract(contract.contract_hash));
            }
            let identity = (contract.contract_id.clone(), contract.version);
            if by_identity.contains_key(&identity) {
                return Err(ResearchError::DuplicateContractVersion {
                    contract_id: contract.contract_id.clone(),
                    version: contract.version,
                });
            }
            let contract_hash = contract.contract_hash.clone();
            by_hash.insert(
                contract_hash.clone(),
                InstalledContract {
                    contract,
                    artifact: stored.artifact,
                },
            );
            by_identity.insert(identity, contract_hash);
        }
        Ok(Self {
            by_hash,
            by_identity,
        })
    }

    pub fn get(&self, hash: &akzio_domain::ContentHash) -> ResearchResult<&InstalledContract> {
        // 只能按已安装 contract hash 读取；未知 hash 返回显式错误，不从 candidate 或本地默认猜测。
        self.by_hash
            .get(hash)
            .ok_or_else(|| ResearchError::UnknownContract(hash.clone()))
    }

    pub fn contracts(&self) -> impl Iterator<Item = &InstalledContract> {
        self.by_hash.values()
    }

    pub fn with_installed_candidate(&self, installed: InstalledContract) -> ResearchResult<Self> {
        // 返回新的值语义 catalogue，不修改当前实例；hash 已存在可幂等复用，identity/version 冲突则拒绝。
        let mut catalogue = self.clone();
        if catalogue
            .by_hash
            .contains_key(&installed.contract.contract_hash)
        {
            return Ok(catalogue);
        }
        let identity = (
            installed.contract.contract_id.clone(),
            installed.contract.version,
        );
        if catalogue.by_identity.contains_key(&identity) {
            return Err(ResearchError::DuplicateContractVersion {
                contract_id: identity.0,
                version: identity.1,
            });
        }
        catalogue
            .by_identity
            .insert(identity, installed.contract.contract_hash.clone());
        catalogue
            .by_hash
            .insert(installed.contract.contract_hash.clone(), installed);
        Ok(catalogue)
    }

    /// Lower only Store-owned Active Contract heads into agent recipes.
    /// The recipe limits come from each contract's termination/budget/retry
    /// policy; Rust owns the fixed priority ceilings and terminal gate recipes.
    ///
    /// This method rejects unknown purposes and candidates that are not the
    /// current durable head rather than silently granting a new recipe.
    pub fn active_recipe_catalogue(&self, store: &Store) -> ResearchResult<RecipeCatalogue> {
        // 只把 active contracts lower 成 recipes；active_recipe_catalogue 的错误仍由 Runtime 映射为 ResearchError。
        let contracts =
            self.contracts()
                .cloned()
                .map(|installed| akzio_runtime::ActiveContractRecipe {
                    contract: installed.contract,
                    artifact: installed.artifact,
                });
        akzio_runtime::active_recipe_catalogue(store, contracts, ACTIVE_RESEARCH_MAX_NODES)
            .map_err(map_active_recipe_error)
    }

    /// Candidate contracts are data for later shadow evaluation. This gate
    /// proves they cannot request a wider source or tool surface than the
    /// installed active contract that sponsors them.
    pub fn validate_candidate(
        &self,
        active_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
    ) -> ResearchResult<()> {
        // candidate 必须通过自身 schema 和 sponsor active contract 的 capability subset 检查。
        candidate.validate()?;
        let active = self.get(active_hash)?;
        if active.contract.permits_candidate(candidate) {
            Ok(())
        } else {
            Err(ResearchError::CandidateCapabilityExpansion {
                active: active_hash.clone(),
                candidate: candidate.contract_hash.clone(),
            })
        }
    }

    pub fn install_candidate(
        &self,
        store: &Store,
        active_contract_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> ResearchResult<InstalledContract> {
        // 候选在写入 Store 前还要验证其 tool definitions 能从受控 ContextBroker 构造。
        self.validate_candidate(active_contract_hash, candidate)?;
        model_tool_definitions(&ContextBroker::new(store.clone()), candidate)?;
        let stored = store.install_candidate_contract(active_contract_hash, candidate, now)?;
        Ok(installed_contract(stored))
    }
}

fn map_active_recipe_error(error: RuntimeError) -> ResearchError {
    // Runtime 的 active-contract 错误保持一一映射；未知错误不被压成“缺 contract”。
    match error {
        RuntimeError::UnexpectedActiveContractPurpose(purpose) => {
            ResearchError::UnexpectedActiveContractPurpose(purpose)
        }
        RuntimeError::DuplicateActiveContractPurpose(purpose) => {
            ResearchError::DuplicateActiveContractPurpose(purpose)
        }
        RuntimeError::MissingActiveContract(purpose) => {
            ResearchError::MissingActiveContract(purpose)
        }
        RuntimeError::ActiveContractOutputMismatch {
            purpose,
            expected,
            actual,
        } => ResearchError::ActiveContractOutputMismatch {
            purpose,
            expected,
            actual,
        },
        RuntimeError::NonCanonicalActiveContract(purpose) => {
            ResearchError::NonCanonicalActiveContract(purpose)
        }
        other => ResearchError::Runtime(other),
    }
}

fn installed_contract(stored: StoredContract) -> InstalledContract {
    // Store 返回的 contract/artifact 成对转为本地值，不复制或改写 CAS identity。
    InstalledContract {
        contract: stored.contract,
        artifact: stored.artifact,
    }
}

fn validate_unique_contracts(contracts: &[AgentContract]) -> ResearchResult<()> {
    // 同时按 content hash 与 contract_id/version 去重，保证 catalogue 两个索引不会互相覆盖。
    let mut hashes = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for contract in contracts {
        contract.validate()?;
        if !hashes.insert(contract.contract_hash.clone()) {
            return Err(ResearchError::DuplicateContract(
                contract.contract_hash.clone(),
            ));
        }
        let identity = (contract.contract_id.clone(), contract.version);
        if !identities.insert(identity) {
            return Err(ResearchError::DuplicateContractVersion {
                contract_id: contract.contract_id.clone(),
                version: contract.version,
            });
        }
    }
    Ok(())
}
