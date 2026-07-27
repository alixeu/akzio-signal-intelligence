use crate::{
    draft::{
        draft_relative, draft_unit_relative, read_draft, validate_existing_artifact_ref,
        write_draft,
    },
    write_run_manifest, ArtifactDraft, ArtifactScope, DraftLifecycle, FileStore,
    FinalizedArtifactRef, Result, RunLocation, RunManifest, RunManifestInit, StoreError,
};

/// Rebuild the lightweight run projection from finalized Artifact references
/// only. Drafts, session messages, and partial index directories are never
/// treated as evidence of completion.
pub fn rebuild_manifest_from_finalized_artifacts(
    store: &FileStore,
    init: RunManifestInit,
    artifacts: impl IntoIterator<Item = FinalizedArtifactRef>,
) -> Result<RunManifest> {
    let location = init.location.clone();
    let mut manifest = RunManifest::new(init)?;
    for artifact in artifacts {
        let relative = location.child_relative(&artifact.relative_path())?;
        if !store.exists(&relative)? {
            return Err(StoreError::FinalizedArtifactMismatch {
                path: store.root().join(relative),
                message: "manifest rebuild received a missing finalized artifact".to_owned(),
            });
        }
        validate_existing_artifact_ref(store, &relative, &artifact)?;
        manifest.record_finalized_artifact(artifact)?;
    }
    write_run_manifest(store, &location, manifest)
}

/// If a process crashed after the Artifact commit point but before the Draft
/// completion marker, restore the completed Draft without invoking an LLM.
pub fn recover_pending_finalization(
    store: &FileStore,
    location: &RunLocation,
    scope: &ArtifactScope,
) -> Result<Option<ArtifactDraft>> {
    let draft_relative = draft_relative(location, scope)?;
    if !store.exists(&draft_relative)? {
        return Ok(None);
    }
    let lock_relative = draft_unit_relative(location, scope)?.join("lifecycle.lock");
    store.with_exclusive_lock(&lock_relative, || {
        let mut draft = read_draft(store, location, &draft_relative, scope.profile)?;
        if draft.lifecycle != DraftLifecycle::Draft {
            return Ok(Some(draft));
        }
        let Some(pending) = draft.pending_artifact.clone() else {
            return Ok(Some(draft));
        };
        let artifact_relative = location.child_relative(&pending.relative_path())?;
        if !store.exists(&artifact_relative)? {
            return Ok(Some(draft));
        }
        validate_existing_artifact_ref(store, &artifact_relative, &pending)?;
        draft.lifecycle = DraftLifecycle::Completed;
        draft.pending_artifact = None;
        draft.finalized_artifact = Some(pending);
        draft.revision += 1;
        let updated_at = draft
            .finalized_artifact
            .as_ref()
            .expect("completed draft has artifact")
            .created_at
            .clone();
        draft.updated_at = updated_at;
        Ok(Some(write_draft(store, location, &draft_relative, draft)?))
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::Path};

    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    use super::{rebuild_manifest_from_finalized_artifacts, recover_pending_finalization};
    use crate::{
        create_or_recover_draft, draft_relative, ArtifactScope, ContentHashDocument,
        DraftLifecycle, DraftProfile, FileStore, FileStoreOptions, FinalizedArtifactRef,
        RunLocation, RunManifestInit,
    };

    fn location() -> RunLocation {
        RunLocation::new("2026-07-27", "run-one").unwrap()
    }

    fn scope() -> ArtifactScope {
        ArtifactScope {
            run_id: "run-one".to_owned(),
            current_date: "2026-07-27".to_owned(),
            phase: 1,
            role: "analyst.technical".to_owned(),
            profile: DraftProfile::AnalystReport,
            profile_version: 1,
            builder_version: 1,
            unit_key: "QQQ".to_owned(),
            source_payload_hash: "sha256:source".to_owned(),
            ticker: Some("QQQ".to_owned()),
            topic_id: None,
            side: None,
            stance: None,
            round: None,
            reflection_task: None,
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct FinalArtifact {
        schema_version: u32,
        artifact_id: String,
        phase: u8,
        role: String,
        profile: String,
        unit_key: String,
        source_payload_hash: String,
        content_hash: String,
    }

    impl ContentHashDocument for FinalArtifact {
        fn content_hash(&self) -> &str {
            &self.content_hash
        }

        fn set_content_hash(&mut self, hash: String) {
            self.content_hash = hash;
        }
    }

    fn artifact() -> FinalArtifact {
        FinalArtifact {
            schema_version: 2,
            artifact_id: "artifact-one".to_owned(),
            phase: 1,
            role: "analyst.technical".to_owned(),
            profile: "analyst_report".to_owned(),
            unit_key: "QQQ".to_owned(),
            source_payload_hash: "sha256:source".to_owned(),
            content_hash: String::new(),
        }
    }

    #[test]
    fn recovery_completes_pending_draft_when_artifact_commit_exists() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let scope = scope();
        let mut draft =
            create_or_recover_draft(&store, &location, scope.clone(), "2026-07-27T00:00:00Z")
                .unwrap();
        let reference = FinalizedArtifactRef::new(
            "artifact-one",
            Path::new("artifacts/phase1/qqq.json"),
            1,
            "analyst.technical",
            "analyst_report",
            "QQQ",
            "sha256:source",
            "2026-07-27T00:01:00Z",
        )
        .unwrap();
        let artifact_path = location.child_relative(&reference.relative_path()).unwrap();
        store
            .write_authoritative_json(&artifact_path, artifact())
            .unwrap();
        draft.pending_artifact = Some(reference);
        draft.updated_at = "2026-07-27T00:01:00Z".to_owned();
        let draft_path = draft_relative(&location, &scope).unwrap();
        store.write_authoritative_json(&draft_path, draft).unwrap();

        let recovered = recover_pending_finalization(&store, &location, &scope)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.lifecycle, DraftLifecycle::Completed);
    }

    #[test]
    fn manifest_rebuild_rejects_missing_or_unfinalized_artifacts() {
        let directory = tempdir().unwrap();
        let store = FileStore::open(directory.path(), FileStoreOptions::default()).unwrap();
        let location = location();
        let init = RunManifestInit {
            location: location.clone(),
            workflow_version: "workflow-v2".to_owned(),
            prompt_versions: BTreeMap::new(),
            git_sha: "deadbeef".to_owned(),
            config_hash: "sha256:config".to_owned(),
            authority_registry_hash: "sha256:authority".to_owned(),
            created_at: "2026-07-27T00:00:00Z".to_owned(),
        };
        let reference = FinalizedArtifactRef::new(
            "missing",
            Path::new("artifacts/phase1/missing.json"),
            1,
            "analyst.technical",
            "analyst_report",
            "QQQ",
            "sha256:source",
            "2026-07-27T00:01:00Z",
        )
        .unwrap();
        assert!(rebuild_manifest_from_finalized_artifacts(&store, init, [reference]).is_err());
    }
}
