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
}

pub const ACTIVE_RESEARCH_MAX_NODES: usize = 32;

pub(super) const ACTIVE_CONTRACT_VERSION: u32 = 2;
pub(super) const ACTIVE_PROMPT_BUNDLE_VERSION: u32 = 2;
pub(super) const SHARED_GOVERNANCE_PROMPT: &str = "Follow the installed Akzio Contract exactly. Rust owns state, evidence access, budgets, workflow gates, and Paper-only execution. Use only ContextManifest-granted artifacts and the declared tools. Never access arbitrary files, network resources, credentials, databases, or execution controls. Return only the requested strict JSON output.";
pub(super) const PLANNER_RECIPE_ID: &str = "research.planner";
pub(super) const PLANNER_CHILD_RECIPE_IDS: [&str; 2] = ["research.analyst", "research.synthesizer"];
pub(super) const GOVERNED_EVIDENCE_SOURCE_FAMILIES: [&str; 4] =
    ["alpaca", "sec_edgar", "fred", "news_web"];
pub(super) const PLANNER_MAX_DRAFT_TASKS: u16 = 7;
pub(super) const RFC3339_TIMESTAMP_PATTERN: &str =
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$";
pub(super) const EVIDENCE_GATE_RECIPE_ID: &str = "gate.evidence";
pub(super) const DECISION_GATE_RECIPE_ID: &str = "gate.decision";
pub(super) const EXECUTION_GATE_RECIPE_ID: &str = "gate.execution";
pub(super) const PAPER_COMMIT_RECIPE_ID: &str = "gate.paper";
pub(super) const RECONCILE_RECIPE_ID: &str = "gate.reconcile";
pub(super) const EVALUATE_RECIPE_ID: &str = "gate.evaluate";
pub(super) const OUTCOME_WORKER_RECIPE_ID: &str = "learning.outcome_worker";

#[derive(Debug, Clone, Copy)]
pub(super) struct ActiveRecipePolicy {
    pub(super) purpose: &'static str,
    pub(super) output_kind: ArtifactKind,
    pub(super) priority_ceiling: u8,
}

pub(super) const ACTIVE_RECIPE_POLICIES: [ActiveRecipePolicy; 4] = [
    ActiveRecipePolicy {
        purpose: PLANNER_RECIPE_ID,
        output_kind: ArtifactKind::WorkflowProposalDraft,
        priority_ceiling: 100,
    },
    ActiveRecipePolicy {
        purpose: "research.analyst",
        output_kind: ArtifactKind::Claim,
        priority_ceiling: 90,
    },
    ActiveRecipePolicy {
        purpose: "research.critic",
        output_kind: ArtifactKind::Critique,
        priority_ceiling: 80,
    },
    ActiveRecipePolicy {
        purpose: "research.synthesizer",
        output_kind: ArtifactKind::DecisionProposal,
        priority_ceiling: 100,
    },
];

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
                Some(stored) => stored,
                None => store.install_active_contract(&contract, now)?,
            };
            let contract = stored.contract;
            contract.validate()?;
            model_tool_definitions(store, &contract)?;
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
        let mut installed_purposes = BTreeSet::new();
        let mut recipes = Vec::with_capacity(ACTIVE_RECIPE_POLICIES.len() + 6);

        let mut outcome_worker_installed = false;
        for installed in self.contracts() {
            let purpose = installed.contract.purpose.as_str();
            let Some(policy) = active_recipe_policy(purpose) else {
                if purpose != OUTCOME_WORKER_RECIPE_ID {
                    return Err(ResearchError::UnexpectedActiveContractPurpose(
                        purpose.to_owned(),
                    ));
                }
                if installed.contract.output.artifact_kind != ArtifactKind::RetrospectiveDraft {
                    return Err(ResearchError::ActiveContractOutputMismatch {
                        purpose: purpose.to_owned(),
                        expected: ArtifactKind::RetrospectiveDraft,
                        actual: installed.contract.output.artifact_kind,
                    });
                }
                outcome_worker_installed = true;
                recipes.push(TaskRecipe {
                    recipe_id: TaskRecipeId::new(purpose)?,
                    purpose: installed.contract.purpose.clone(),
                    contract_hash: Some(installed.contract.contract_hash.clone()),
                    task_class: RuntimeTaskClass::Evaluate,
                    allowed_evidence_sources: recipe_evidence_sources(&installed.contract),
                    max_children: 0,
                    max_depth: 0,
                    priority_ceiling: 100,
                    budget: installed.contract.budget.clone(),
                    retry: installed.contract.retry.clone(),
                    on_failure: installed.contract.on_failure,
                });
                continue;
            };
            if !installed_purposes.insert(purpose.to_owned()) {
                return Err(ResearchError::DuplicateActiveContractPurpose(
                    purpose.to_owned(),
                ));
            }
            if installed.contract.output.artifact_kind != policy.output_kind {
                return Err(ResearchError::ActiveContractOutputMismatch {
                    purpose: purpose.to_owned(),
                    expected: policy.output_kind,
                    actual: installed.contract.output.artifact_kind,
                });
            }
            let active = store
                .active_contract(&installed.contract.purpose)?
                .ok_or_else(|| ResearchError::NonCanonicalActiveContract(purpose.to_owned()))?;
            if active.contract.contract_hash != installed.contract.contract_hash
                || active.artifact != installed.artifact
            {
                return Err(ResearchError::NonCanonicalActiveContract(
                    purpose.to_owned(),
                ));
            }

            recipes.push(TaskRecipe {
                recipe_id: TaskRecipeId::new(purpose)?,
                purpose: installed.contract.purpose.clone(),
                contract_hash: Some(installed.contract.contract_hash.clone()),
                task_class: RuntimeTaskClass::Agent,
                allowed_evidence_sources: recipe_evidence_sources(&installed.contract),
                max_children: installed.contract.termination.max_child_tasks,
                max_depth: installed.contract.termination.max_depth,
                priority_ceiling: policy.priority_ceiling,
                budget: installed.contract.budget.clone(),
                retry: installed.contract.retry.clone(),
                on_failure: installed.contract.on_failure,
            });
        }

        for policy in ACTIVE_RECIPE_POLICIES {
            if !installed_purposes.contains(policy.purpose) {
                return Err(ResearchError::MissingActiveContract(policy.purpose));
            }
        }
        if !outcome_worker_installed {
            return Err(ResearchError::MissingActiveContract(
                OUTCOME_WORKER_RECIPE_ID,
            ));
        }

        let (terminal_recipes, terminals) = rust_terminal_recipes()?;
        recipes.extend(terminal_recipes);
        Ok(RecipeCatalogue::new(
            recipes,
            TaskRecipeId::new(PLANNER_RECIPE_ID)?,
            terminals,
            ACTIVE_RESEARCH_MAX_NODES,
        )?)
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
        model_tool_definitions(store, candidate)?;
        let stored = store.install_candidate_contract(active_contract_hash, candidate, now)?;
        Ok(installed_contract(stored))
    }
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
