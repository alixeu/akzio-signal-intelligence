fn presents_lesson_bodies(contract: &AgentContract) -> bool {
    contract.purpose.as_str() == RESEARCH_SYNTHESIZER_RECIPE_ID
        && contract.version == akzio_domain::SYNTHESIZER_LESSON_PROJECTION_CONTRACT_VERSION
}

impl ContextBroker {
    fn projected_document(&self, artifact: &Artifact, lesson_bodies: bool) -> ContextResult<Value> {
        if lesson_bodies && artifact.kind == ArtifactKind::Lesson {
            let lesson: Lesson = self.read_payload(artifact)?;
            lesson.validate()?;
            // Both sizing and materialization use this complete semantic view. Dropping
            // an exclusion or shortening the action could change where the advice applies.
            return Ok(lesson.synthesizer_projection());
        }
        Ok(compact_governed_projection(
            artifact.kind,
            self.document_value(artifact)?,
        ))
    }

    fn omit_incomplete_lesson_groups(
        &self,
        selections: &mut Vec<ContextSelection>,
        source_bytes: &mut u64,
        tokens: &mut u32,
    ) -> ContextResult<BTreeSet<ArtifactId>> {
        let omitted = self.incomplete_lesson_groups(selections)?;
        for selection in selections
            .iter()
            .filter(|selection| omitted.contains(&selection.artifact.artifact_id))
        {
            let artifact = self.store.artifact(&selection.artifact.artifact_id)?;
            *source_bytes -= artifact.blob.bytes;
            *tokens -= selection.estimated_tokens;
        }
        selections.retain(|selection| !omitted.contains(&selection.artifact.artifact_id));
        Ok(omitted)
    }

    fn incomplete_lesson_groups(
        &self,
        selections: &[ContextSelection],
    ) -> ContextResult<BTreeSet<ArtifactId>> {
        let selected = selections
            .iter()
            .filter(|selection| selection.artifact.kind == ArtifactKind::Lesson)
            .map(|selection| selection.artifact.artifact_id.clone())
            .collect::<BTreeSet<_>>();
        if selected.is_empty() {
            return Ok(selected);
        }
        let active = self.store.active_lessons_snapshot()?;
        let mut complete = active
            .iter()
            .map(|stored| &stored.artifact.artifact_id)
            .filter(|id| selected.contains(*id))
            .cloned()
            .collect::<BTreeSet<_>>();
        // Edges are undirected for presentation. An active counterpart may be
        // absent due to scope, eligibility, capacity, or a narrowed child grant.
        // Propagate every incomplete edge to its entire selected component.
        loop {
            let mut omit = BTreeSet::new();
            for stored in &active {
                let id = &stored.artifact.artifact_id;
                for conflict in &stored.lesson.conflicts_with {
                    let counterpart = &conflict.artifact_id;
                    match (complete.contains(id), complete.contains(counterpart)) {
                        (true, false) => omit.insert(id.clone()),
                        (false, true) => omit.insert(counterpart.clone()),
                        _ => false,
                    };
                }
            }
            if omit.is_empty() {
                return Ok(selected.difference(&complete).cloned().collect());
            }
            complete.retain(|id| !omit.contains(id));
        }
    }
}
