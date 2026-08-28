use super::*;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledContract {
    pub contract: AgentContract,
    pub artifact: Artifact,
}

/// The bounded initial topology is expressed as installed Contracts, never as
/// a role registry. Daemon bootstrap consumes this pair atomically at the API
/// boundary: contracts drive model turns; recipes drive Rust DAG lowering.
#[derive(Debug, Clone)]
pub struct ActiveResearchCatalogue {
    pub contracts: ContractCatalogue,
    pub recipes: RecipeCatalogue,
}

impl ActiveResearchCatalogue {
    /// Restore the Store-owned active heads, bootstrapping only a fresh Store
    /// with the immutable Rust-defined defaults. Candidates deliberately have
    /// no execution path until a canonical Paper-backed transition promotes
    /// their persisted head.
    pub fn install(store: &V2Store, now: DateTime<Utc>) -> ResearchResult<Self> {
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
        store: &V2Store,
        active_contract_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> ResearchResult<InstalledContract> {
        self.contracts
            .install_candidate(store, active_contract_hash, candidate, now)
    }

    pub fn install_analyst_freshness_candidate(
        &self,
        store: &V2Store,
        now: DateTime<Utc>,
    ) -> ResearchResult<InstalledContract> {
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
        role.extend_from_slice(
            b"\n\nCandidate freshness v5: treat each selected evidence item's observed_at and max_age_secs as hard freshness inputs. State stale or mixed-time evidence explicitly in evidence_gaps and never use stale evidence as support.",
        );
        candidate.prompt.role = store.put_bytes(&role, "text/plain")?;
        candidate.contract_hash = candidate.expected_hash()?;
        candidate.validate()?;
        self.install_candidate(store, &active.contract.contract_hash, &candidate, now)
    }
}

pub const ACTIVE_RESEARCH_MAX_NODES: usize = 32;

pub(super) const ACTIVE_CONTRACT_VERSION: u32 = 12;
pub(super) const ACTIVE_PROMPT_BUNDLE_VERSION: u32 = 9;
pub const ANALYST_FRESHNESS_CANDIDATE_VERSION: u32 = 13;
pub(super) const SHARED_GOVERNANCE_PROMPT: &str = "Follow the installed Akzio Contract exactly. Rust owns state, evidence access, budgets, workflow gates, and Paper-only execution. Use only ContextManifest-granted artifacts and the declared tools. Never access arbitrary files, network resources, credentials, databases, or execution controls. Work in two phases: produce an auditable natural-language research memo, then call submit_result exactly once when Rust requests submission. submit_result is a zero-side-effect proposal channel; Rust alone validates and persists the result.";
pub(super) const PLANNER_RECIPE_ID: &str = akzio_domain::RESEARCH_PLANNER_RECIPE_ID;
pub(super) const PLANNER_CHILD_RECIPE_IDS: [&str; 2] = [
    akzio_domain::RESEARCH_ANALYST_RECIPE_ID,
    akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID,
];
pub(super) const PLANNER_MAX_DRAFT_TASKS: u16 = 7;
pub(super) const RFC3339_TIMESTAMP_PATTERN: &str =
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$";

#[derive(Debug, Clone, Default)]
pub struct ContractCatalogue {
    by_hash: BTreeMap<akzio_domain::ContentHash, InstalledContract>,
    by_identity: BTreeMap<(akzio_domain::ContractId, u32), akzio_domain::ContentHash>,
}

impl ContractCatalogue {
    fn load_or_bootstrap_active(
        store: &V2Store,
        contracts: impl IntoIterator<Item = AgentContract>,
        now: DateTime<Utc>,
    ) -> ResearchResult<Self> {
        let contracts = contracts.into_iter().collect::<Vec<_>>();
        validate_unique_contracts(&contracts)?;
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
                return Err(ResearchError::DuplicateContract(
                    contract.contract_hash.clone(),
                ));
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

    #[cfg(test)]
    pub(super) fn install(
        store: &V2Store,
        contracts: impl IntoIterator<Item = AgentContract>,
        now: DateTime<Utc>,
    ) -> ResearchResult<Self> {
        Self::load_or_bootstrap_active(store, contracts, now)
    }

    pub fn get(&self, hash: &akzio_domain::ContentHash) -> ResearchResult<&InstalledContract> {
        self.by_hash
            .get(hash)
            .ok_or_else(|| ResearchError::UnknownContract(hash.clone()))
    }

    pub fn contracts(&self) -> impl Iterator<Item = &InstalledContract> {
        self.by_hash.values()
    }

    pub fn with_installed_candidate(&self, installed: InstalledContract) -> ResearchResult<Self> {
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

    pub fn contract_hash_for(
        &self,
        contract_id: &akzio_domain::ContractId,
        version: u32,
    ) -> Option<&akzio_domain::ContentHash> {
        self.by_identity.get(&(contract_id.clone(), version))
    }

    /// Lower only Store-owned Active Contract heads into agent recipes.
    /// The recipe limits come from each contract's termination/budget/retry
    /// policy; Rust owns the fixed priority ceilings and terminal gate recipes.
    ///
    /// This method rejects unknown purposes and candidates that are not the
    /// current durable head rather than silently granting a new recipe.
    pub fn active_recipe_catalogue(&self, store: &V2Store) -> ResearchResult<RecipeCatalogue> {
        let contracts =
            self.contracts()
                .cloned()
                .map(|installed| akzio_runtime::v2::ActiveContractRecipe {
                    contract: installed.contract,
                    artifact: installed.artifact,
                });
        akzio_runtime::v2::active_recipe_catalogue(
            store,
            contracts,
            TaskRecipeId::new(PLANNER_RECIPE_ID)?,
            ACTIVE_RESEARCH_MAX_NODES,
        )
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
        store: &V2Store,
        active_contract_hash: &akzio_domain::ContentHash,
        candidate: &AgentContract,
        now: DateTime<Utc>,
    ) -> ResearchResult<InstalledContract> {
        self.validate_candidate(active_contract_hash, candidate)?;
        model_tool_definitions(&ContextBroker::new(store.clone()), candidate)?;
        let stored = store.install_candidate_contract(active_contract_hash, candidate, now)?;
        Ok(installed_contract(stored))
    }
}

fn map_active_recipe_error(error: RuntimeError) -> ResearchError {
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

#[cfg(test)]
pub(super) fn recipe_evidence_sources(contract: &AgentContract) -> BTreeSet<String> {
    contract
        .tool_grants
        .iter()
        .filter(|grant| grant.kind == ToolKind::ReadEvidence)
        .flat_map(|grant| grant.allowed_sources.iter().cloned())
        .collect()
}

fn installed_contract(stored: StoredContract) -> InstalledContract {
    InstalledContract {
        contract: stored.contract,
        artifact: stored.artifact,
    }
}

fn validate_unique_contracts(contracts: &[AgentContract]) -> ResearchResult<()> {
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
