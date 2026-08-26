//! Durable campaign head and session reservations.
//!
//! Campaign writes are fenced by the daemon lease in the same SQLite
//! transaction as the state change.  The learning runtime owns verdict
//! calculation; this module only persists the validated result.

use akzio_domain::{
    Artifact, ArtifactKind, ArtifactLifecycle, CanaryCampaignSpec, CanaryCampaignStatus,
    CanarySessionReservation, CanaryVerdict, ContentHash, PaperApprovalScope, PaperLaunchApproval,
    RunId, RunPurpose, RuntimeManifest,
};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use super::{
    assert_daemon_lease, parse_time, run_purpose_from_connection, DaemonLease, SessionReservation,
    SessionSlotReservation, StoreError, StoreResult, V2Store, WorkflowCommit,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryCampaignHead {
    pub spec: CanaryCampaignSpec,
    pub status: CanaryCampaignStatus,
    pub last_verdict: Option<CanaryVerdict>,
    pub revision: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredCanarySession {
    pub reservation: CanarySessionReservation,
}

impl V2Store {
    pub(crate) fn verify_canary_campaign_history(
        &self,
        connection: &Connection,
    ) -> StoreResult<()> {
        let active_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM rebuild_canary_campaigns WHERE active = 1",
            [],
            |row| row.get(0),
        )?;
        if active_count > 1 {
            return Err(StoreError::Integrity(
                "more than one canary campaign is active".to_owned(),
            ));
        }

        let campaign_ids = connection
            .prepare("SELECT campaign_id FROM rebuild_canary_campaigns ORDER BY campaign_id")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for campaign_id in campaign_ids {
            let campaign_id = ContentHash::new(campaign_id)?;
            let head = read_campaign(connection, &campaign_id)?.ok_or_else(|| {
                StoreError::Integrity(format!("canary campaign {campaign_id} disappeared"))
            })?;
            head.spec.validate()?;
            let expected_active = i64::from(!matches!(
                head.status,
                CanaryCampaignStatus::Completed | CanaryCampaignStatus::Frozen
            ));
            let active: i64 = connection.query_row(
                "SELECT active FROM rebuild_canary_campaigns WHERE campaign_id = ?1",
                params![campaign_id.as_str()],
                |row| row.get(0),
            )?;
            if active != expected_active {
                return Err(StoreError::Integrity(format!(
                    "canary campaign {campaign_id} active flag disagrees with status"
                )));
            }
        }

        let mut sessions = connection.prepare(
            "SELECT campaign_id, level_json, session_key, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_sessions ORDER BY campaign_id, level_json",
        )?;
        let rows = sessions.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, u64>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;
        for row in rows {
            let (
                campaign_id,
                level_json,
                session_key,
                parent_run_id,
                contract_shadow_run_id,
                topology_shadow_run_id,
                bundle_shadow_run_id,
                scheduler_epoch,
                reserved_at,
            ) = row?;
            let campaign_id = ContentHash::new(campaign_id)?;
            let level: CanaryCampaignStatus = serde_json::from_str(&level_json)?;
            let reservation = CanarySessionReservation {
                schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
                campaign_id: campaign_id.clone(),
                level,
                session_key,
                parent_run_id: akzio_domain::RunId(parent_run_id),
                contract_shadow_run_id: akzio_domain::RunId(contract_shadow_run_id),
                topology_shadow_run_id: akzio_domain::RunId(topology_shadow_run_id),
                bundle_shadow_run_id: akzio_domain::RunId(bundle_shadow_run_id),
                scheduler_epoch,
                reserved_at: parse_time(&reserved_at)?,
            };
            reservation.validate()?;
            let head = read_campaign(connection, &campaign_id)?.ok_or_else(|| {
                StoreError::Integrity(format!(
                    "canary session references missing campaign {campaign_id}"
                ))
            })?;
            if head.status != level
                || run_purpose_from_connection(connection, &reservation.parent_run_id)?
                    != RunPurpose::Paper
                || run_purpose_from_connection(connection, &reservation.contract_shadow_run_id)?
                    != RunPurpose::Shadow
                || run_purpose_from_connection(connection, &reservation.topology_shadow_run_id)?
                    != RunPurpose::Shadow
                || run_purpose_from_connection(connection, &reservation.bundle_shadow_run_id)?
                    != RunPurpose::Shadow
            {
                return Err(StoreError::Integrity(
                    "canary session lineage is invalid".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

impl V2Store {
    pub fn stage_canary_campaign(
        &self,
        lease: &DaemonLease,
        spec: &CanaryCampaignSpec,
        now: DateTime<Utc>,
    ) -> StoreResult<CanaryCampaignHead> {
        spec.validate()?;
        self.validate_campaign_artifacts(spec)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;

        if let Some(existing) = read_campaign(&transaction, &spec.campaign_id)? {
            if existing.spec != *spec {
                return Err(StoreError::CanaryCampaignConflict(
                    spec.campaign_id.to_string(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }

        let active_campaign: Option<String> = transaction
            .query_row(
                "SELECT campaign_id FROM rebuild_canary_campaigns WHERE active = 1 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if active_campaign.is_some() {
            return Err(StoreError::CanaryCampaignConflict(
                active_campaign.unwrap_or_default(),
            ));
        }

        transaction.execute(
            "INSERT INTO rebuild_canary_campaigns (campaign_id, spec_json, status_json, last_verdict_json, revision, active, created_at, updated_at) VALUES (?1, ?2, ?3, NULL, 0, 1, ?4, ?4)",
            params![
                spec.campaign_id.as_str(),
                serde_json::to_string(spec)?,
                serde_json::to_string(&CanaryCampaignStatus::Staged)?,
                now.to_rfc3339(),
            ],
        )?;
        transaction.commit()?;
        Ok(CanaryCampaignHead {
            spec: spec.clone(),
            status: CanaryCampaignStatus::Staged,
            last_verdict: None,
            revision: 0,
            updated_at: now,
        })
    }

    pub fn canary_campaign(
        &self,
        campaign_id: &ContentHash,
    ) -> StoreResult<Option<CanaryCampaignHead>> {
        let connection = self.connection()?;
        read_campaign(&connection, campaign_id)
    }

    pub fn active_canary_campaign(&self) -> StoreResult<Option<CanaryCampaignHead>> {
        let connection = self.connection()?;
        let Some(campaign_id) = connection
            .query_row(
                "SELECT campaign_id FROM rebuild_canary_campaigns WHERE active = 1 LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        else {
            return Ok(None);
        };
        let campaign_id = ContentHash::new(campaign_id)?;
        read_campaign(&connection, &campaign_id)
    }

    pub fn transition_canary_campaign(
        &self,
        lease: &DaemonLease,
        campaign_id: &ContentHash,
        expected_status: CanaryCampaignStatus,
        verdict: CanaryVerdict,
        now: DateTime<Utc>,
    ) -> StoreResult<CanaryCampaignHead> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, now)?;
        let current = read_campaign(&transaction, campaign_id)?
            .ok_or_else(|| StoreError::MissingCanaryCampaign(campaign_id.to_string()))?;
        if current.status != expected_status {
            return Err(StoreError::CanaryCampaignConflict(format!(
                "{} expected {:?}, found {:?}",
                campaign_id, expected_status, current.status
            )));
        }

        let next_status = match verdict {
            CanaryVerdict::Advance => current.status.next().ok_or_else(|| {
                StoreError::CanaryCampaignConflict(format!(
                    "{} cannot advance from {:?}",
                    campaign_id, current.status
                ))
            })?,
            CanaryVerdict::Hold | CanaryVerdict::Defer => current.status,
            CanaryVerdict::Rollback => CanaryCampaignStatus::Frozen,
        };
        let revision = current.revision.saturating_add(1);
        let active = i64::from(!matches!(
            next_status,
            CanaryCampaignStatus::Completed | CanaryCampaignStatus::Frozen
        ));
        transaction.execute(
            "UPDATE rebuild_canary_campaigns SET status_json = ?1, last_verdict_json = ?2, revision = ?3, active = ?4, updated_at = ?5 WHERE campaign_id = ?6",
            params![
                serde_json::to_string(&next_status)?,
                serde_json::to_string(&verdict)?,
                revision,
                active,
                now.to_rfc3339(),
                campaign_id.as_str(),
            ],
        )?;
        transaction.commit()?;
        Ok(CanaryCampaignHead {
            spec: current.spec,
            status: next_status,
            last_verdict: Some(verdict),
            revision,
            updated_at: now,
        })
    }

    pub fn freeze_canary_campaign(
        &self,
        lease: &DaemonLease,
        campaign_id: &ContentHash,
        expected_status: CanaryCampaignStatus,
        now: DateTime<Utc>,
    ) -> StoreResult<CanaryCampaignHead> {
        self.transition_canary_campaign(
            lease,
            campaign_id,
            expected_status,
            CanaryVerdict::Rollback,
            now,
        )
    }

    pub fn reserve_canary_session(
        &self,
        lease: &DaemonLease,
        reservation: &CanarySessionReservation,
    ) -> StoreResult<StoredCanarySession> {
        reservation.validate()?;
        if reservation.scheduler_epoch != lease.epoch {
            return Err(StoreError::SchedulerFenced(lease.lease_name.clone()));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, reservation.reserved_at)?;
        let current = read_campaign(&transaction, &reservation.campaign_id)?.ok_or_else(|| {
            StoreError::MissingCanaryCampaign(reservation.campaign_id.to_string())
        })?;
        if current.status != reservation.level {
            return Err(StoreError::CanaryCampaignConflict(format!(
                "{} session level {:?} does not match campaign {:?}",
                reservation.campaign_id, reservation.level, current.status
            )));
        }
        if run_purpose_from_connection(&transaction, &reservation.parent_run_id)?
            != RunPurpose::Paper
            || run_purpose_from_connection(&transaction, &reservation.contract_shadow_run_id)?
                != RunPurpose::Shadow
            || run_purpose_from_connection(&transaction, &reservation.topology_shadow_run_id)?
                != RunPurpose::Shadow
            || run_purpose_from_connection(&transaction, &reservation.bundle_shadow_run_id)?
                != RunPurpose::Shadow
        {
            return Err(StoreError::CanaryCampaignConflict(
                "canary session run purposes".to_owned(),
            ));
        }

        if let Some(existing) =
            read_session(&transaction, &reservation.campaign_id, reservation.level)?
        {
            if existing.reservation != *reservation {
                return Err(StoreError::CanaryCampaignConflict(format!(
                    "{} already has a different {:?} session",
                    reservation.campaign_id, reservation.level
                )));
            }
            transaction.commit()?;
            return Ok(existing);
        }

        let duplicate_session: Option<String> = transaction
            .query_row(
                "SELECT campaign_id FROM rebuild_canary_sessions WHERE session_key = ?1",
                params![reservation.session_key],
                |row| row.get(0),
            )
            .optional()?;
        if duplicate_session.is_some() {
            return Err(StoreError::CanaryCampaignConflict(
                reservation.session_key.clone(),
            ));
        }

        transaction.execute(
            "INSERT INTO rebuild_canary_sessions (campaign_id, level_json, session_key, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                reservation.campaign_id.as_str(),
                serde_json::to_string(&reservation.level)?,
                reservation.session_key,
                reservation.parent_run_id.0,
                reservation.contract_shadow_run_id.0,
                reservation.topology_shadow_run_id.0,
                reservation.bundle_shadow_run_id.0,
                reservation.scheduler_epoch,
                reservation.reserved_at.to_rfc3339(),
            ],
        )?;
        transaction.commit()?;
        Ok(StoredCanarySession {
            reservation: reservation.clone(),
        })
    }

    pub(super) fn commit_canary_session_transaction(
        transaction: &Transaction<'_>,
        reservation: &CanarySessionReservation,
    ) -> StoreResult<()> {
        let current = read_campaign(transaction, &reservation.campaign_id)?.ok_or_else(|| {
            StoreError::MissingCanaryCampaign(reservation.campaign_id.to_string())
        })?;
        if current.status != reservation.level {
            return Err(StoreError::CanaryCampaignConflict(format!(
                "{} session level {:?} does not match campaign {:?}",
                reservation.campaign_id, reservation.level, current.status
            )));
        }
        if run_purpose_from_connection(transaction, &reservation.parent_run_id)?
            != RunPurpose::Paper
            || run_purpose_from_connection(transaction, &reservation.contract_shadow_run_id)?
                != RunPurpose::Shadow
            || run_purpose_from_connection(transaction, &reservation.topology_shadow_run_id)?
                != RunPurpose::Shadow
            || run_purpose_from_connection(transaction, &reservation.bundle_shadow_run_id)?
                != RunPurpose::Shadow
        {
            return Err(StoreError::CanaryCampaignConflict(
                "canary session run purposes".to_owned(),
            ));
        }
        if let Some(existing) =
            read_session(transaction, &reservation.campaign_id, reservation.level)?
        {
            if existing.reservation != *reservation {
                return Err(StoreError::CanaryCampaignConflict(format!(
                    "{} already has a different {:?} session",
                    reservation.campaign_id, reservation.level
                )));
            }
            return Ok(());
        }
        let duplicate_session: Option<String> = transaction
            .query_row(
                "SELECT campaign_id FROM rebuild_canary_sessions WHERE session_key = ?1",
                params![reservation.session_key],
                |row| row.get(0),
            )
            .optional()?;
        if duplicate_session.is_some() {
            return Err(StoreError::CanaryCampaignConflict(
                reservation.session_key.clone(),
            ));
        }
        transaction.execute(
            "INSERT INTO rebuild_canary_sessions (campaign_id, level_json, session_key, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                reservation.campaign_id.as_str(),
                serde_json::to_string(&reservation.level)?,
                reservation.session_key,
                reservation.parent_run_id.0,
                reservation.contract_shadow_run_id.0,
                reservation.topology_shadow_run_id.0,
                reservation.bundle_shadow_run_id.0,
                reservation.scheduler_epoch,
                reservation.reserved_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reserve_canary_session_with_workflows(
        &self,
        lease: &DaemonLease,
        parent: &SessionReservation,
        proposal: &Artifact,
        runtime_manifest: &Artifact,
        approval: &Artifact,
        shadow_workflows: &[WorkflowCommit],
        reservation: &CanarySessionReservation,
    ) -> StoreResult<SessionSlotReservation> {
        if shadow_workflows.len() != 3 {
            return Err(StoreError::CanaryCampaignConflict(
                "canary session requires three shadow workflows".to_owned(),
            ));
        }
        self.validate_paper_session_reservation(parent, proposal)?;
        self.validate_paper_approval_binding(runtime_manifest, approval)?;
        reservation.validate()?;
        if reservation.scheduler_epoch != lease.epoch
            || reservation.session_key != parent.session_key
            || reservation.parent_run_id != parent.workflow.run.run_id
            || shadow_workflows
                .iter()
                .zip([
                    &reservation.contract_shadow_run_id,
                    &reservation.topology_shadow_run_id,
                    &reservation.bundle_shadow_run_id,
                ])
                .any(|(commit, expected)| {
                    commit.run.run_id != *expected
                        || commit.run.purpose != RunPurpose::Shadow
                        || commit.graph.kind != ArtifactKind::WorkflowGraph
                        || commit.graph.artifact_id != commit.run.graph_artifact_id
                })
        {
            return Err(StoreError::CanaryCampaignConflict(
                "canary workflow reservation binding".to_owned(),
            ));
        }
        for shadow in shadow_workflows {
            self.validate_workflow_commit(shadow)?;
        }
        if self.session_slot(&parent.session_key)?.is_some() {
            return Err(StoreError::CanaryCampaignConflict(
                "Paper session already exists without canary reservation".to_owned(),
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        assert_daemon_lease(&transaction, lease, parent.reserved_at)?;
        Self::commit_session_slot_transaction(
            &transaction,
            lease,
            parent,
            proposal,
            Some((runtime_manifest, approval)),
        )?;
        for shadow in shadow_workflows {
            Self::commit_workflow_transaction(&transaction, shadow)?;
        }
        Self::commit_canary_session_transaction(&transaction, reservation)?;
        transaction.commit()?;
        drop(connection);
        let slot = self
            .session_slot(&parent.session_key)?
            .ok_or_else(|| StoreError::Integrity("session slot missing after commit".to_owned()))?;
        Ok(SessionSlotReservation {
            slot,
            newly_reserved: true,
        })
    }

    pub fn canary_session(
        &self,
        campaign_id: &ContentHash,
        level: CanaryCampaignStatus,
    ) -> StoreResult<Option<StoredCanarySession>> {
        let connection = self.connection()?;
        read_session(&connection, campaign_id, level)
    }

    pub fn canary_session_for_run(
        &self,
        run_id: &RunId,
    ) -> StoreResult<Option<StoredCanarySession>> {
        let connection = self.connection()?;
        let row: Option<(String, String)> = connection
            .query_row(
                "SELECT campaign_id, level_json FROM rebuild_canary_sessions WHERE parent_run_id = ?1 OR contract_shadow_run_id = ?1 OR topology_shadow_run_id = ?1 OR bundle_shadow_run_id = ?1 LIMIT 1",
                params![run_id.0],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((campaign_id, level_json)) = row else {
            return Ok(None);
        };
        let campaign_id = ContentHash::new(campaign_id)?;
        let level: CanaryCampaignStatus = serde_json::from_str(&level_json)?;
        read_session(&connection, &campaign_id, level)
    }

    fn validate_campaign_artifacts(&self, spec: &CanaryCampaignSpec) -> StoreResult<()> {
        let references = [
            (
                &spec.candidate_contract,
                ArtifactKind::Contract,
                ArtifactLifecycle::Canonical,
            ),
            (
                &spec.candidate_topology,
                ArtifactKind::WorkflowGraph,
                ArtifactLifecycle::RunScoped,
            ),
            (
                &spec.runtime_manifest,
                ArtifactKind::RuntimeManifest,
                ArtifactLifecycle::Canonical,
            ),
            (
                &spec.paper_approval,
                ArtifactKind::PaperLaunchApproval,
                ArtifactLifecycle::Canonical,
            ),
        ];
        for (reference, expected_kind, expected_lifecycle) in references {
            let artifact = self.artifact(&reference.artifact_id)?;
            if artifact.kind != expected_kind
                || artifact.artifact_id != reference.artifact_id
                || artifact.lifecycle != expected_lifecycle
            {
                return Err(StoreError::CanaryCampaignConflict(
                    "campaign artifact closure".to_owned(),
                ));
            }
        }

        let manifest_artifact = self.artifact(&spec.runtime_manifest.artifact_id)?;
        let manifest: RuntimeManifest =
            serde_json::from_slice(&self.read_blob(&manifest_artifact.blob)?)?;
        manifest.validate()?;
        if manifest.code_revision != spec.source_revision
            || manifest.maximum_notional != spec.maximum_total_notional
        {
            return Err(StoreError::CanaryCampaignConflict(
                "campaign runtime manifest binding".to_owned(),
            ));
        }

        let approval_artifact = self.artifact(&spec.paper_approval.artifact_id)?;
        let approval: PaperLaunchApproval =
            serde_json::from_slice(&self.read_blob(&approval_artifact.blob)?)?;
        approval.validate()?;
        if approval.scope != PaperApprovalScope::Canary
            || approval.runtime_manifest != spec.runtime_manifest
            || approval.runtime_manifest_hash != manifest.manifest_hash()?
        {
            return Err(StoreError::CanaryCampaignConflict(
                "campaign Paper approval binding".to_owned(),
            ));
        }
        Ok(())
    }
}

fn read_campaign(
    connection: &Connection,
    campaign_id: &ContentHash,
) -> StoreResult<Option<CanaryCampaignHead>> {
    let row: Option<(String, String, Option<String>, i64, String)> = connection
        .query_row(
            "SELECT spec_json, status_json, last_verdict_json, revision, updated_at FROM rebuild_canary_campaigns WHERE campaign_id = ?1",
            params![campaign_id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let Some((spec_json, status_json, verdict_json, revision, updated_at)) = row else {
        return Ok(None);
    };
    let revision = u64::try_from(revision)
        .map_err(|_| StoreError::Integrity("negative canary revision".to_owned()))?;
    Ok(Some(CanaryCampaignHead {
        spec: serde_json::from_str(&spec_json)?,
        status: serde_json::from_str(&status_json)?,
        last_verdict: verdict_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?,
        revision,
        updated_at: parse_time(&updated_at)
            .map_err(|error| StoreError::Integrity(error.to_string()))?,
    }))
}

fn read_session(
    connection: &Connection,
    campaign_id: &ContentHash,
    level: CanaryCampaignStatus,
) -> StoreResult<Option<StoredCanarySession>> {
    let row: Option<(String, String, String, String, String, i64, String)> = connection
        .query_row(
            "SELECT session_key, parent_run_id, contract_shadow_run_id, topology_shadow_run_id, bundle_shadow_run_id, scheduler_epoch, reserved_at FROM rebuild_canary_sessions WHERE campaign_id = ?1 AND level_json = ?2",
            params![campaign_id.as_str(), serde_json::to_string(&level)?],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((session_key, parent, contract, topology, bundle, scheduler_epoch, reserved_at)) = row
    else {
        return Ok(None);
    };
    let scheduler_epoch = u64::try_from(scheduler_epoch)
        .map_err(|_| StoreError::Integrity("negative canary scheduler epoch".to_owned()))?;
    Ok(Some(StoredCanarySession {
        reservation: CanarySessionReservation {
            schema_version: akzio_domain::V2_DOMAIN_SCHEMA_VERSION,
            campaign_id: campaign_id.clone(),
            level,
            session_key,
            parent_run_id: akzio_domain::RunId(parent),
            contract_shadow_run_id: akzio_domain::RunId(contract),
            topology_shadow_run_id: akzio_domain::RunId(topology),
            bundle_shadow_run_id: akzio_domain::RunId(bundle),
            scheduler_epoch,
            reserved_at: parse_time(&reserved_at)
                .map_err(|error| StoreError::Integrity(error.to_string()))?,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::super::insert_artifact;
    use super::*;
    use akzio_domain::{
        AgentContract, Artifact, ArtifactKind, ArtifactLifecycle, ArtifactProvenance,
        ContextPolicy, ContractId, ContractPurpose, FailureDisposition, MoneyMicros,
        OutputContract, PaperApprovalScope, PaperLaunchApproval, PromptBundle, RetryPolicy,
        RuntimeManifest, TaskBudget, TaskId, TaskRecipeId, TerminationPolicy, ToolGrant, ToolKind,
        ToolSpec, TopologyId, WorkflowGraph, WorkflowNode, V2_DOMAIN_SCHEMA_VERSION,
    };
    use chrono::Duration;
    use serde::Serialize;
    use std::collections::BTreeSet;
    use tempfile::tempdir;

    fn canonical_json_artifact<T: Serialize>(
        store: &V2Store,
        kind: ArtifactKind,
        payload: &T,
    ) -> akzio_domain::ArtifactRef {
        let now = Utc::now();
        let lifecycle = if kind == ArtifactKind::WorkflowGraph {
            ArtifactLifecycle::RunScoped
        } else {
            ArtifactLifecycle::Canonical
        };
        let artifact = Artifact::new(
            kind,
            store.put_json(payload).unwrap(),
            "canary.test",
            lifecycle,
            ArtifactProvenance {
                source_family: "canary.test".to_owned(),
                observed_at: None,
                retrieved_at: now,
                source_uri: None,
                confidence_ppm: 1_000_000,
                producer_contract_hash: None,
            },
            None,
            vec![],
            now,
        )
        .unwrap();
        let mut connection = store.connection().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        insert_artifact(&transaction, &artifact).unwrap();
        transaction.commit().unwrap();
        akzio_domain::ArtifactRef {
            artifact_id: artifact.artifact_id,
            kind,
        }
    }

    fn spec(store: &V2Store, value: &[u8]) -> CanaryCampaignSpec {
        let now = Utc::now();
        let maximum_total_notional = MoneyMicros::from_usd_cents(100_000);
        let active_contract_hash = ContentHash::of_bytes(b"active-contract");
        let candidate_contract_payload = AgentContract::new(
            ContractId::new(),
            1,
            ContractPurpose::new("research.candidate").unwrap(),
            "candidate contract",
            PromptBundle {
                version: 1,
                governance: store.put_bytes(b"governance", "text/plain").unwrap(),
                role: store.put_bytes(b"role", "text/plain").unwrap(),
            },
            ContextPolicy {
                permitted_kinds: BTreeSet::from([ArtifactKind::NormalizedEvidence]),
                permitted_source_families: BTreeSet::from(["market".to_owned()]),
                min_artifacts: 1,
                max_artifacts: 4,
                max_bytes: 4096,
                max_tokens: 1024,
                allow_raw_reread: false,
            },
            vec![ToolGrant {
                kind: ToolKind::ReadEvidence,
                allowed_sources: vec!["market".to_owned()],
            }],
            vec![ToolSpec {
                name: "read_artifact".to_owned(),
                description: "read market artifact".to_owned(),
                kind: ToolKind::ReadEvidence,
                input_schema: store.put_bytes(b"{}", "application/json").unwrap(),
                strict: true,
            }],
            OutputContract {
                artifact_kind: ArtifactKind::Claim,
                schema: store.put_bytes(b"{}", "application/json").unwrap(),
            },
            TaskBudget {
                max_input_tokens: 32,
                max_output_tokens: 16,
                max_wall_time_secs: 10,
                max_tool_calls: 1,
            },
            RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            TerminationPolicy::leaf(),
            FailureDisposition::FailRun,
        )
        .unwrap();
        let candidate_contract =
            canonical_json_artifact(store, ArtifactKind::Contract, &candidate_contract_payload);
        let node = WorkflowNode {
            task_id: TaskId::new(),
            recipe_id: TaskRecipeId::new("research.analyst").unwrap(),
            contract_hash: None,
            objective: "candidate topology".to_owned(),
            dependencies: vec![],
            input_artifacts: vec![],
            priority: 50,
            budget: TaskBudget {
                max_input_tokens: 32,
                max_output_tokens: 16,
                max_wall_time_secs: 10,
                max_tool_calls: 1,
            },
            retry: RetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
                retry_transport: true,
                retry_rate_limited: true,
                retry_invalid_output: false,
            },
            on_failure: FailureDisposition::FailRun,
            parent_task_id: None,
        };
        let candidate_topology_payload = WorkflowGraph {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "candidate-topology".to_owned(),
            nodes: vec![node],
        };
        let candidate_topology = canonical_json_artifact(
            store,
            ArtifactKind::WorkflowGraph,
            &candidate_topology_payload,
        );
        let manifest_payload = RuntimeManifest {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            code_revision: "revision-1".to_owned(),
            cargo_lock_hash: ContentHash::of_bytes(b"cargo-lock"),
            config_hash: ContentHash::of_bytes(b"config"),
            provider_id: "provider".to_owned(),
            model_id: "model".to_owned(),
            prompt_hash: ContentHash::of_bytes(b"prompt"),
            contract_hash: active_contract_hash.clone(),
            topology_hash: ContentHash::of_bytes(b"active-topology"),
            decision_policy_hash: ContentHash::of_bytes(b"decision"),
            execution_policy_hash: ContentHash::of_bytes(b"execution"),
            evaluation_policy_hash: ContentHash::of_bytes(b"evaluation"),
            market_data_feed: "iex".to_owned(),
            broker_account_id: "paper-account".to_owned(),
            maximum_notional: maximum_total_notional,
            allowed_session_start: now.date_naive(),
            allowed_session_end: now.date_naive(),
            expires_at: now + Duration::hours(8),
            created_at: now,
        };
        let runtime_manifest =
            canonical_json_artifact(store, ArtifactKind::RuntimeManifest, &manifest_payload);
        let mut approval_payload = PaperLaunchApproval {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            operator_identity: "operator:test".to_owned(),
            runtime_manifest: runtime_manifest.clone(),
            runtime_manifest_hash: manifest_payload.manifest_hash().unwrap(),
            scope: PaperApprovalScope::Canary,
            reason: "test campaign".to_owned(),
            approved_at: now,
            expires_at: manifest_payload.expires_at,
            approval_hash: ContentHash::of_bytes(b"pending"),
        };
        approval_payload.approval_hash = approval_payload.unsigned_hash().unwrap();
        let paper_approval =
            canonical_json_artifact(store, ArtifactKind::PaperLaunchApproval, &approval_payload);
        CanaryCampaignSpec {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            campaign_id: ContentHash::of_bytes(value),
            active_contract_hash: ContentHash::of_bytes(b"active-contract"),
            candidate_contract,
            active_topology_id: TopologyId("active-topology".to_owned()),
            candidate_topology,
            runtime_manifest,
            paper_approval,
            source_revision: "revision-1".to_owned(),
            maximum_total_notional: akzio_domain::MoneyMicros::from_usd_cents(100_000),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn campaign_head_is_fenced_and_advances_idempotently_by_status() {
        let directory = tempdir().unwrap();
        let store = V2Store::open(directory.path()).unwrap();
        let now = Utc::now();
        let lease = store
            .acquire_daemon_lease("campaign", "owner", now, now + chrono::Duration::minutes(5))
            .unwrap()
            .unwrap();
        let campaign = store
            .stage_canary_campaign(&lease, &spec(&store, b"campaign"), now)
            .unwrap();
        assert_eq!(campaign.status, CanaryCampaignStatus::Staged);
        let campaign = store
            .transition_canary_campaign(
                &lease,
                &campaign.spec.campaign_id,
                CanaryCampaignStatus::Staged,
                CanaryVerdict::Advance,
                now,
            )
            .unwrap();
        assert_eq!(campaign.status, CanaryCampaignStatus::Canary10);
        assert_eq!(store.active_canary_campaign().unwrap().unwrap().revision, 1);
    }
}
