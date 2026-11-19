const LESSON_PRESENTATION_VERSION: u32 =
    akzio_domain::SYNTHESIZER_LESSON_PROJECTION_CONTRACT_VERSION;

#[derive(Deserialize)]
struct AcceptedLessonSubmit {
    version: u32,
    result_turn: ArtifactRef,
    accepted_turn: ArtifactRef,
    result_hash: ContentHash,
    accepted_request_hash: ContentHash,
}

struct LessonSubmitProof {
    manifest: Artifact,
    result: Value,
    repair: Option<Value>,
}

impl Store {
    fn has_lesson_presentation_contract(&self, hash: &ContentHash) -> StoreResult<bool> {
        Ok(self.contract_installation(hash)?.is_some_and(|installed| {
            installed.contract.purpose.as_str() == akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
                && installed.contract.version == LESSON_PRESENTATION_VERSION
        }))
    }

    /// Recover full Lesson bodies from the exact input to a result-producing Submit.
    pub fn presented_lessons_for_result_turn(
        &self,
        manifest: &Artifact,
        origin: &akzio_domain::ArtifactOrigin,
        result_turn: &ArtifactRef,
        accepted_turn: &ArtifactRef,
    ) -> StoreResult<BTreeSet<ArtifactRef>> {
        let proof = self.lesson_submit_proof(manifest, origin, result_turn, accepted_turn)?;
        self.presented_lessons_for_submit(&proof.manifest, &proof.result)
    }

    fn lesson_submit_proof(
        &self,
        manifest: &Artifact,
        origin: &akzio_domain::ArtifactOrigin,
        result_turn: &ArtifactRef,
        accepted_turn: &ArtifactRef,
    ) -> StoreResult<LessonSubmitProof> {
        let accepted = self.synthesizer_submit_trace(manifest, origin, accepted_turn)?;
        if result_turn == accepted_turn {
            return Ok(LessonSubmitProof {
                manifest: manifest.clone(),
                result: accepted,
                repair: None,
            });
        }
        let result_artifact = self.artifact(&result_turn.artifact_id)?;
        let [reference] = result_artifact.source_refs.as_slice() else {
            return Err(invalid_lesson_presentation());
        };
        if reference.kind != ArtifactKind::ContextManifest {
            return Err(invalid_lesson_presentation());
        }
        let result_manifest = self.related_lesson_manifest(manifest, &reference.artifact_id)?;
        let result_origin = result_manifest
            .origin
            .as_ref()
            .ok_or_else(invalid_lesson_presentation)?;
        let result = self.synthesizer_submit_trace(&result_manifest, result_origin, result_turn)?;
        if !earlier_submit(&result, &accepted) || !same_input_authority(&result, &accepted) {
            return Err(invalid_lesson_presentation());
        }
        Ok(LessonSubmitProof {
            manifest: result_manifest,
            result,
            repair: Some(accepted),
        })
    }

    fn presented_lessons_for_submit(
        &self,
        manifest: &Artifact,
        result: &Value,
    ) -> StoreResult<BTreeSet<ArtifactRef>> {
        let request = &result["domain_request"];
        let continued;
        let context = if request["continuation"].is_null() {
            &request["context"]
        } else {
            // Only the input item replayed by the adapter counts. Neither provider output
            // resembling a Lesson nor fixture metadata can establish model input.
            let first = &request["continuation"]["items"][0];
            let Some(text) = first["content"].as_str() else {
                return Ok(BTreeSet::new());
            };
            if *first != serde_json::json!({"role": "user", "content": text}) {
                return Ok(BTreeSet::new());
            }
            let Ok(input) = serde_json::from_str::<Value>(text) else {
                return Ok(BTreeSet::new());
            };
            let Ok(input_manifest_id) = serde_json::from_value(input["context_manifest"].clone())
            else {
                return Ok(BTreeSet::new());
            };
            let input_manifest = self.related_lesson_manifest(manifest, &input_manifest_id)?;
            if !self.has_fresh_lesson_input(&input_manifest, result, text)? {
                return Ok(BTreeSet::new());
            }
            continued = input;
            &continued["context"]
        };
        let payload: ContextManifestPayload =
            serde_json::from_slice(&self.read_blob(&manifest.blob)?)?;
        let mut lessons = BTreeMap::new();
        for selection in payload
            .selections
            .into_iter()
            .filter(|selection| selection.artifact.kind == ArtifactKind::Lesson)
        {
            let artifact = self.artifact(&selection.artifact.artifact_id)?;
            let lesson: Lesson = serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
            lesson.validate()?;
            if artifact.kind != ArtifactKind::Lesson || lesson.lifecycle != LessonLifecycle::Active
            {
                return Err(invalid_lesson_presentation());
            }
            lessons.insert(selection.artifact.clone(), (selection, lesson));
        }
        let mut presented = BTreeSet::new();
        for entry in context
            .as_array()
            .into_iter()
            .flatten()
            .filter(|entry| entry["class"] == "lesson_projection")
        {
            if entry["type"] != "must_read" || entry["metadata"]["kind"] != "lesson" {
                return Err(invalid_lesson_presentation());
            }
            let reference = ArtifactRef {
                artifact_id: serde_json::from_value(entry["metadata"]["document_id"].clone())?,
                kind: ArtifactKind::Lesson,
            };
            let (selection, lesson) = lessons
                .get(&reference)
                .ok_or_else(invalid_lesson_presentation)?;
            let body = lesson.synthesizer_projection();
            let size = serde_json::to_vec(&body)?.len() as u64;
            if selection.projected_bytes != Some(size)
                || selection.estimated_tokens != akzio_domain::estimate_tokens_from_bytes(size)
            {
                return Err(invalid_lesson_presentation());
            }
            // Altered projections, including masked historical experiments, cannot
            // establish that the model received the complete immutable Lesson.
            if entry["value"] == body && !presented.insert(reference) {
                return Err(invalid_lesson_presentation());
            }
        }
        // A replay or mask may leave part of a conflict group visible. Count the
        // group only when every connected Lesson has its full projection.
        loop {
            let before = presented.len();
            for (reference, (_, lesson)) in &lessons {
                for other in &lesson.conflicts_with {
                    if presented.contains(reference) != presented.contains(other) {
                        presented.remove(reference);
                        presented.remove(other);
                    }
                }
            }
            if presented.len() == before {
                return Ok(presented);
            }
        }
    }

    /// Verify accepted Submit lineage and prove which v71 Lessons accompanied the result.
    /// Earlier Contracts have no presentation proof.
    pub fn proposal_lesson_presentation(
        &self,
        proposal: &Artifact,
        manifest: &Artifact,
    ) -> StoreResult<Option<BTreeSet<ArtifactRef>>> {
        let payload: ContextManifestPayload =
            serde_json::from_slice(&self.read_blob(&manifest.blob)?)?;
        let hash = &payload.contract_hash;
        if !self.has_lesson_presentation_contract(hash)? {
            return Ok(None);
        }
        let origin = proposal
            .origin
            .as_ref()
            .ok_or_else(invalid_lesson_presentation)?;
        let manifest_ref = ArtifactRef {
            artifact_id: manifest.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        };
        let (Some(run_id), Some(task_id), Some(attempt_id)) =
            (&origin.run_id, &origin.task_id, &origin.attempt_id)
        else {
            return Err(invalid_lesson_presentation());
        };
        let succeeded = self.current_succeeded_attempt(run_id, task_id)?;
        if proposal.kind != ArtifactKind::DecisionProposal
            || proposal.producer != "agent.research.synthesizer"
            || proposal.provenance.producer_contract_hash.as_ref() != Some(hash)
            || origin.contract_hash.as_ref() != Some(hash)
            || manifest.kind != ArtifactKind::ContextManifest
            || manifest.producer != "context.research.synthesizer"
            || manifest.provenance.producer_contract_hash.as_ref() != Some(hash)
            || manifest.origin.as_ref() != Some(origin)
            || succeeded.attempt_id != *attempt_id
            || succeeded.contract_hash.as_ref() != Some(hash)
            || !succeeded.outputs.contains(proposal)
            || succeeded.context_manifest.as_ref() != Some(&manifest_ref)
            || !proposal.source_refs.contains(&manifest_ref)
        {
            return Err(invalid_lesson_presentation());
        }
        let mut notes = proposal
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::DeliberationNote);
        let note_ref = notes.next().ok_or_else(invalid_lesson_presentation)?;
        let note = self.artifact(&note_ref.artifact_id)?;
        if notes.next().is_some()
            || note.kind != ArtifactKind::DeliberationNote
            || note.producer != "agent.deliberation.research.synthesizer"
            || note.origin.as_ref() != Some(origin)
            || note.provenance.producer_contract_hash.as_ref() != Some(hash)
            || !note.source_refs.contains(&manifest_ref)
        {
            return Err(invalid_lesson_presentation());
        }
        let note_payload: Value = serde_json::from_slice(&self.read_blob(&note.blob)?)?;
        let binding: AcceptedLessonSubmit =
            serde_json::from_value(note_payload["accepted_synthesizer_submit"].clone())?;
        if binding.version != 1
            || [&binding.result_turn, &binding.accepted_turn]
                .iter()
                .any(|reference| {
                    !proposal.source_refs.contains(reference)
                        || !note.source_refs.contains(reference)
                })
        {
            return Err(invalid_lesson_presentation());
        }
        let LessonSubmitProof {
            manifest: result_manifest,
            result,
            repair,
        } = self.lesson_submit_proof(
            manifest,
            origin,
            &binding.result_turn,
            &binding.accepted_turn,
        )?;
        let accepted_trace = repair.as_ref().unwrap_or(&result);
        let result_body = result
            .pointer("/response/terminal_submission/arguments/result")
            .ok_or_else(invalid_lesson_presentation)?;
        let draft: akzio_domain::DecisionDraft =
            serde_json::from_slice(&self.read_blob(&proposal.blob)?)?;
        let same_refs = |field, references: &[ArtifactRef]| -> StoreResult<bool> {
            let ids = references
                .iter()
                .map(|reference| serde_json::to_value(&reference.artifact_id))
                .collect::<Result<Vec<_>, _>>()?;
            let submitted = result_body[field]
                .as_array()
                .into_iter()
                .flatten()
                .map(|reference| &reference["artifact_id"]);
            Ok(submitted.eq(ids.iter()))
        };
        if accepted_trace["request_hash"] != serde_json::to_value(&binding.accepted_request_hash)?
            || ContentHash::of_bytes(&serde_json::to_vec(result_body)?) != binding.result_hash
            || !same_refs("applied_learning_refs", &draft.applied_learning_refs)?
            || !same_refs("rejected_learning_refs", &draft.rejected_learning_refs)?
        {
            return Err(invalid_lesson_presentation());
        }
        if let Some(repair) = repair {
            let request = &repair["domain_request"];
            let context = &request["context"][0];
            let submission = &repair["response"]["terminal_submission"]["arguments"];
            if !request["continuation"].is_null()
                || context["type"] != "deliberation_repair"
                || context["frozen_result_hash"] != serde_json::to_value(&binding.result_hash)?
                || submission.get("result").is_some()
                || !submission["deliberation"].is_object()
            {
                return Err(invalid_lesson_presentation());
            }
        }
        let presented = self.presented_lessons_for_submit(&result_manifest, &result)?;
        if draft
            .applied_learning_refs
            .iter()
            .chain(&draft.rejected_learning_refs)
            .any(|reference| {
                reference.kind == ArtifactKind::Lesson && !presented.contains(reference)
            })
        {
            return Err(invalid_lesson_presentation());
        }
        Ok(Some(presented))
    }

    /// Verify the presented Lesson revisions for a committed DecisionContext.
    pub fn decision_lesson_presentation(
        &self,
        decision: &Artifact,
    ) -> StoreResult<Option<BTreeSet<ArtifactRef>>> {
        let mut refs = decision
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::DecisionProposal);
        let Some(reference) = refs.next() else {
            return Ok(None);
        };
        if decision.kind != ArtifactKind::DecisionContext || refs.next().is_some() {
            return Err(invalid_lesson_presentation());
        }
        let proposal = self.artifact(&reference.artifact_id)?;
        let mut manifests = proposal
            .source_refs
            .iter()
            .filter(|reference| reference.kind == ArtifactKind::ContextManifest);
        let Some(manifest_ref) = manifests.next() else {
            if let Some(hash) = &proposal.provenance.producer_contract_hash {
                if self.has_lesson_presentation_contract(hash)? {
                    return Err(invalid_lesson_presentation());
                }
            }
            return Ok(None);
        };
        let manifest = self.artifact(&manifest_ref.artifact_id)?;
        let Some(presented) = self.proposal_lesson_presentation(&proposal, &manifest)? else {
            return Ok(None);
        };
        let origin = decision
            .origin
            .as_ref()
            .ok_or_else(invalid_lesson_presentation)?;
        let (Some(run), Some(task), Some(attempt)) =
            (&origin.run_id, &origin.task_id, &origin.attempt_id)
        else {
            return Err(invalid_lesson_presentation());
        };
        let succeeded = self.current_succeeded_attempt(run, task)?;
        let context: akzio_domain::DecisionContext =
            serde_json::from_slice(&self.read_blob(&decision.blob)?)?;
        let draft: akzio_domain::DecisionDraft =
            serde_json::from_slice(&self.read_blob(&proposal.blob)?)?;
        let manifest_payload: ContextManifestPayload =
            serde_json::from_slice(&self.read_blob(&manifest.blob)?)?;
        let expected_unpresented: BTreeSet<_> = manifest_payload
            .selections
            .into_iter()
            .map(|selection| selection.artifact)
            .filter(|reference| {
                reference.kind == ArtifactKind::Lesson && !presented.contains(reference)
            })
            .collect();
        if decision.producer != "decision.context"
            || manifests.next().is_some()
            || !decision.source_refs.contains(manifest_ref)
            || !succeeded.outputs.contains(decision)
            || succeeded.attempt_id != *attempt
            || context.run_id != *run
            || proposal
                .origin
                .as_ref()
                .and_then(|origin| origin.run_id.as_ref())
                != Some(run)
            || context.applied_learning_refs != draft.applied_learning_refs
            || context.rejected_learning_refs != draft.rejected_learning_refs
            || context
                .unpresented_lesson_refs
                .iter()
                .collect::<BTreeSet<_>>()
                != expected_unpresented.iter().collect()
        {
            return Err(invalid_lesson_presentation());
        }
        Ok(Some(presented))
    }

    pub(super) fn validate_lesson_evidence_presentation(
        &self,
        record: &LessonEvidence,
    ) -> StoreResult<()> {
        let decision = self.artifact(&record.decision_context.artifact_id)?;
        let shown = self
            .decision_lesson_presentation(&decision)?
            .ok_or_else(invalid_lesson_presentation)?;
        let context: akzio_domain::DecisionContext =
            serde_json::from_slice(&self.read_blob(&decision.blob)?)?;
        let attributed = match record.attribution {
            LessonAttribution::Applied => context.applied_learning_refs,
            LessonAttribution::Rejected => context.rejected_learning_refs,
        };
        let artifact = self.artifact(&record.lesson_artifact.artifact_id)?;
        let lesson: Lesson = serde_json::from_slice(&self.read_blob(&artifact.blob)?)?;
        if !shown.contains(&record.lesson_artifact)
            || !attributed.contains(&record.lesson_artifact)
            || lesson.lesson_id != record.lesson_id
        {
            return Err(invalid_lesson_presentation());
        }
        Ok(())
    }

    fn synthesizer_submit_trace(
        &self,
        manifest: &Artifact,
        origin: &akzio_domain::ArtifactOrigin,
        reference: &ArtifactRef,
    ) -> StoreResult<Value> {
        let turn = self.artifact(&reference.artifact_id)?;
        turn.validate()?;
        let manifest_ref = ArtifactRef {
            artifact_id: manifest.artifact_id.clone(),
            kind: ArtifactKind::ContextManifest,
        };
        if reference.kind != ArtifactKind::AgentTurn
            || turn.kind != ArtifactKind::AgentTurn
            || turn.origin.as_ref() != Some(origin)
            || turn.producer != "agent.turn.research.synthesizer"
            || turn.provenance.producer_contract_hash != origin.contract_hash
            || turn.source_refs.as_slice() != [manifest_ref]
        {
            return Err(invalid_lesson_presentation());
        }
        let completed = {
            let connection = self.connection()?;
            connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM rebuild_events WHERE artifact_id=?1 AND event_type=?2 AND run_id=?3 AND task_id=?4 AND attempt_id=?5)",
                params![turn.artifact_id.0.as_str(), LifecycleEventType::AgentTurnCompleted.as_str(),
                    origin.run_id.as_ref().map(|id| id.0.as_str()), origin.task_id.as_ref().map(|id| id.0.as_str()),
                    origin.attempt_id.as_ref().map(|id| id.0.as_str())],
                |row| row.get::<_, bool>(0),
            )?
        };
        if !completed {
            return Err(invalid_lesson_presentation());
        }
        let trace: Value = serde_json::from_slice(&self.read_blob(&turn.blob)?)?;
        let request = &trace["domain_request"];
        let hash = serde_json::to_value(&origin.contract_hash)?;
        let manifest_id = serde_json::to_value(&manifest.artifact_id)?;
        if trace["lifecycle"]["status"] != "completed"
            || request["phase"] != "submit"
            || request["purpose"] != akzio_domain::RESEARCH_SYNTHESIZER_RECIPE_ID
            || trace["contract_hash"] != hash
            || request["contract_hash"] != hash
            || trace["context_manifest"] != manifest_id
            || request["manifest_artifact_id"] != manifest_id
            || trace["request_hash"] != serde_json::to_value(content_hash_json(request)?)?
        {
            return Err(invalid_lesson_presentation());
        }
        Ok(trace)
    }

    fn related_lesson_manifest(
        &self,
        manifest: &Artifact,
        id: &ArtifactId,
    ) -> StoreResult<Artifact> {
        if &manifest.artifact_id == id {
            return Ok(manifest.clone());
        }
        let historical = self.artifact(id)?;
        historical.validate()?;
        let (Some(current), Some(older)) = (manifest.origin.as_ref(), historical.origin.as_ref())
        else {
            return Err(invalid_lesson_presentation());
        };
        let (Some(run), Some(task), Some(current_attempt), Some(older_attempt), Some(contract)) = (
            &current.run_id,
            &current.task_id,
            &current.attempt_id,
            &older.attempt_id,
            &current.contract_hash,
        ) else {
            return Err(invalid_lesson_presentation());
        };
        if current.run_id != older.run_id
            || current.task_id != older.task_id
            || current.contract_hash != older.contract_hash
            || current_attempt == older_attempt
            || historical.kind != ArtifactKind::ContextManifest
            || historical.producer != "context.research.synthesizer"
            || historical.provenance.producer_contract_hash.as_ref() != Some(contract)
            || historical.blob != manifest.blob
            || historical.source_refs != manifest.source_refs
        {
            return Err(invalid_lesson_presentation());
        }

        // A new Attempt mints its own Manifest. Only a Retry/Recovery ancestor
        // can supply the same immutable payload and selection closure.
        let mut child = current_attempt.clone();
        let mut seen = BTreeSet::from([child.clone()]);
        while let Some(relation) = self.attempt_relation(&child)? {
            if !matches!(
                relation.relation,
                akzio_domain::AttemptRelationKind::Retry
                    | akzio_domain::AttemptRelationKind::Recovery
            ) || &relation.run_id != run
                || &relation.task_id != task
                || !seen.insert(relation.parent_attempt_id.clone())
            {
                return Err(invalid_lesson_presentation());
            }
            child = relation.parent_attempt_id;
            if &child == older_attempt {
                let created = self
                    .attempt_events(run, task, older_attempt)?
                    .into_iter()
                    .any(|event| {
                        event.artifact_id.as_ref() == Some(id)
                            && event.event_type
                                == LifecycleEventType::ContextManifestCreated.as_str()
                    });
                return if created {
                    Ok(historical)
                } else {
                    Err(invalid_lesson_presentation())
                };
            }
        }
        Err(invalid_lesson_presentation())
    }

    fn has_fresh_lesson_input(
        &self,
        manifest: &Artifact,
        result: &Value,
        input: &str,
    ) -> StoreResult<bool> {
        let origin = manifest
            .origin
            .as_ref()
            .ok_or_else(invalid_lesson_presentation)?;
        for artifact in
            self.artifacts_referencing(&manifest.artifact_id, Some(ArtifactKind::AgentTurn))?
        {
            if artifact.origin.as_ref() != Some(origin) {
                continue;
            }
            let reference = ArtifactRef {
                artifact_id: artifact.artifact_id.clone(),
                kind: ArtifactKind::AgentTurn,
            };
            let Ok(earlier) = self.synthesizer_submit_trace(manifest, origin, &reference) else {
                continue;
            };
            let request = &earlier["domain_request"];
            if earlier_submit(&earlier, result)
                && same_input_authority(&earlier, result)
                && request["continuation"].is_null()
                && serde_json::to_string(&serde_json::json!({
                    "objective": request["objective"],
                    "context_manifest": request["manifest_artifact_id"],
                    "context": request["context"],
                }))? == input
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn same_input_authority(earlier: &Value, later: &Value) -> bool {
    ["read_grant_identity", "context_materialization_identity"]
        .into_iter()
        .all(|key| {
            let identity = &earlier["domain_request"][key];
            identity
                .as_str()
                .is_some_and(|hash| ContentHash::new(hash).is_ok())
                && identity == &later["domain_request"][key]
        })
}

fn earlier_submit(earlier: &Value, later: &Value) -> bool {
    earlier["turn"]
        .as_u64()
        .zip(later["turn"].as_u64())
        .is_some_and(|(before, after)| before < after)
}

fn invalid_lesson_presentation() -> StoreError {
    StoreError::InvalidLearningCommit("lesson_presentation")
}
