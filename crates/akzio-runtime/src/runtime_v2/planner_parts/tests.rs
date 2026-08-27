#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paper_bar_evidence_requests_use_the_full_window() {
        let mut needs = vec![EvidenceNeed {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            source_family: "alpaca".to_owned(),
            resource: "bars:QQQ:1d:2026-07-24:12".to_owned(),
            max_age_secs: 86_400,
        }];

        WorkflowRuntime::normalize_paper_evidence_needs(&mut needs);

        assert_eq!(needs[0].resource, "bars:QQQ:1d:2026-07-24:32");
    }

    #[test]
    fn production_debug_draft_injects_governed_alpaca_evidence() {
        let mut draft = WorkflowProposalDraft {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                akzio_domain::WorkflowProposalDraftTask {
                    recipe_id: TaskRecipeId::new(ANALYST_RECIPE_ID).unwrap(),
                    objective: "Inspect production evidence".to_owned(),
                    depends_on: Vec::new(),
                    priority: 80,
                    evidence_needs: Vec::new(),
                    research_intents: Vec::new(),
                },
            )]),
            stop_reason: None,
        };

        prepare_debug_draft(&mut draft, false, false).unwrap();

        let needs = &draft.tasks["analyst"].evidence_needs;
        assert_eq!(needs.len(), 9);
        assert!(needs.iter().all(|need| need.source_family == "alpaca"));
        assert!(needs.iter().any(|need| need.resource == "paper.account"));
        assert!(needs.iter().any(|need| {
            need.resource.starts_with("bars:TQQQ:1d:") && need.resource.ends_with(":32")
        }));
        assert!(needs
            .iter()
            .all(|need| !need.resource.starts_with("fixture:")));
    }

    #[test]
    fn production_debug_draft_keeps_model_need_and_adds_alpaca_defaults() {
        let mut draft = WorkflowProposalDraft {
            schema_version: V2_DOMAIN_SCHEMA_VERSION,
            topology_id: "active".to_owned(),
            tasks: BTreeMap::from([(
                "analyst".to_owned(),
                akzio_domain::WorkflowProposalDraftTask {
                    recipe_id: TaskRecipeId::new(ANALYST_RECIPE_ID).unwrap(),
                    objective: "Inspect production evidence".to_owned(),
                    depends_on: Vec::new(),
                    priority: 80,
                    evidence_needs: vec![EvidenceNeed {
                        schema_version: V2_DOMAIN_SCHEMA_VERSION,
                        source_family: "news_web".to_owned(),
                        resource: "authorized production debugging artifacts".to_owned(),
                        max_age_secs: DEBUG_FIXTURE_MAX_AGE_SECS,
                    }],
                    research_intents: Vec::new(),
                },
            )]),
            stop_reason: None,
        };

        prepare_debug_draft(&mut draft, false, false).unwrap();

        let needs = &draft.tasks["analyst"].evidence_needs;
        assert_eq!(needs.len(), 10);
        assert!(needs.iter().any(|need| need.source_family == "news_web"));
        assert!(needs.iter().any(|need| need.resource == "paper.account"));
    }
}
