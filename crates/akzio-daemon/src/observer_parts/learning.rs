use super::*;

impl Daemon {
    pub(super) fn observer_learning_analytics(
        &self,
        now: DateTime<Utc>,
        visible_transitions: &[ObserverPolicyTransition],
    ) -> Result<(ObserverLearningSummary, Vec<ObserverPolicyMetrics>)> {
        let outcomes = self.recent_typed_artifacts::<Outcome>(ArtifactKind::Outcome, 100)?;
        let retrospectives =
            self.recent_typed_artifacts::<Retrospective>(ArtifactKind::Retrospective, 200)?;
        let experiences =
            self.recent_typed_artifacts::<Experience>(ArtifactKind::Experience, 200)?;
        let evaluations =
            self.recent_typed_artifacts::<Evaluation>(ArtifactKind::Evaluation, 200)?;
        let current_start = now - Duration::days(30);
        let previous_start = now - Duration::days(60);

        let outcome_by_id = outcomes
            .iter()
            .map(|(artifact, outcome)| (artifact.artifact_id.clone(), outcome.clone()))
            .collect::<BTreeMap<_, _>>();
        let current_outcomes = outcomes
            .iter()
            .filter(|(_, outcome)| {
                outcome
                    .sealed_at
                    .is_some_and(|sealed| sealed >= current_start)
            })
            .collect::<Vec<_>>();
        let current_utilities = current_outcomes
            .iter()
            .map(|(_, outcome)| outcome_average_utility(outcome))
            .collect::<Vec<_>>();
        let attributed_values = current_outcomes
            .iter()
            .filter_map(|(_, outcome)| {
                self.observer_outcome_baseline_equity(outcome)
                    .ok()
                    .and_then(|equity| {
                        i64::try_from(
                            i128::from(equity)
                                .saturating_mul(i128::from(outcome_average_utility(outcome)))
                                / 1_000_000,
                        )
                        .ok()
                    })
            })
            .collect::<Vec<_>>();
        let attributed_utility_micros = (!attributed_values.is_empty()).then(|| {
            attributed_values
                .iter()
                .fold(0_i64, |total, value| total.saturating_add(*value))
        });

        let lesson_count = |start: DateTime<Utc>, end: DateTime<Utc>| {
            retrospectives
                .iter()
                .filter(|(artifact, _)| artifact.created_at >= start && artifact.created_at < end)
                .map(|(_, retrospective)| retrospective.lesson_candidates.len())
                .sum::<usize>()
        };
        let lesson_candidates = lesson_count(current_start, now);
        let previous_lessons = lesson_count(previous_start, current_start);

        let mut all_transitions = visible_transitions
            .iter()
            .map(|record| record.transition.clone())
            .collect::<Vec<_>>();
        let mut transition_ids = all_transitions
            .iter()
            .map(|transition| transition.transition_id.0.clone())
            .collect::<BTreeSet<_>>();
        let mut subjects = experiences
            .iter()
            .map(|(_, experience)| experience.subject.clone())
            .collect::<Vec<_>>();
        subjects.sort();
        subjects.dedup();
        for subject in &subjects {
            for record in self.store.policy_transitions(subject)? {
                if transition_ids.insert(record.transition.transition_id.0.clone()) {
                    all_transitions.push(record.transition);
                }
            }
        }
        all_transitions.sort_by_key(|transition| transition.created_at);
        let transition_count = |start: DateTime<Utc>, end: DateTime<Utc>| {
            all_transitions
                .iter()
                .filter(|transition| transition.created_at >= start && transition.created_at < end)
                .count()
        };
        let policies_evolved = transition_count(current_start, now);
        let previous_policies = transition_count(previous_start, current_start);

        let utility_by_outcome = outcomes
            .iter()
            .map(|(artifact, outcome)| {
                (
                    artifact.artifact_id.clone(),
                    outcome_average_utility(outcome),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut impact_areas = Vec::<(RetrospectiveCategory, i128)>::new();
        for (artifact, retrospective) in &retrospectives {
            if artifact.created_at < current_start || artifact.created_at >= now {
                continue;
            }
            let Some(utility) = utility_by_outcome.get(&retrospective.outcome.artifact_id) else {
                continue;
            };
            let total_confidence = retrospective
                .findings
                .iter()
                .map(|finding| u64::from(finding.confidence_ppm))
                .sum::<u64>();
            if total_confidence == 0 {
                continue;
            }
            for finding in &retrospective.findings {
                let attributed = i128::from(*utility)
                    .saturating_mul(i128::from(finding.confidence_ppm))
                    / i128::from(total_confidence);
                if let Some((_, value)) = impact_areas
                    .iter_mut()
                    .find(|(category, _)| *category == finding.category)
                {
                    *value = value.saturating_add(attributed);
                } else {
                    impact_areas.push((finding.category, attributed));
                }
            }
        }
        impact_areas.sort_by_key(|(_, value)| std::cmp::Reverse(value.abs()));
        let impact_areas = impact_areas
            .into_iter()
            .filter_map(|(category, impact)| {
                i64::try_from(impact)
                    .ok()
                    .map(|impact_ppm| ObserverImpactArea {
                        category,
                        impact_ppm,
                    })
            })
            .collect();

        let experience_by_id = experiences
            .iter()
            .map(|(artifact, experience)| (artifact.artifact_id.clone(), experience.clone()))
            .collect::<BTreeMap<_, _>>();
        let policy = EvaluationPolicy::default();
        let mut grouped =
            Vec::<(PolicySubject, PolicyState, Vec<(DateTime<Utc>, i64, bool)>)>::new();
        for (artifact, evaluation) in &evaluations {
            let Some(experience) = experience_by_id.get(&evaluation.experience.artifact_id) else {
                continue;
            };
            let Some(outcome) = outcome_by_id.get(&evaluation.outcome.artifact_id) else {
                continue;
            };
            let degraded = policy.outcome_is_degraded(outcome);
            if let Some((_, state, values)) = grouped
                .iter_mut()
                .find(|(subject, _, _)| *subject == experience.subject)
            {
                *state = experience.policy_state;
                values.push((
                    artifact.created_at,
                    evaluation.marginal_utility_ppm,
                    degraded,
                ));
            } else {
                grouped.push((
                    experience.subject.clone(),
                    experience.policy_state,
                    vec![(
                        artifact.created_at,
                        evaluation.marginal_utility_ppm,
                        degraded,
                    )],
                ));
            }
        }
        let policy_metrics = grouped
            .into_iter()
            .map(
                |(subject, mut state, mut values)| -> Result<ObserverPolicyMetrics> {
                    if let Some(latest) = self.store.policy_transitions(&subject)?.last() {
                        state = latest.transition.to;
                    }
                    values.sort_by_key(|(created_at, _, _)| std::cmp::Reverse(*created_at));
                    values.truncate(20);
                    let utilities = values
                        .iter()
                        .map(|(_, utility, _)| *utility)
                        .collect::<Vec<_>>();
                    let win_rate_ppm = (!values.is_empty()).then(|| {
                        i64::try_from(
                            values.iter().filter(|(_, utility, _)| *utility > 0).count()
                                * 1_000_000
                                / values.len(),
                        )
                        .unwrap_or(1_000_000)
                    });
                    let stability_ppm = (!values.is_empty()).then(|| {
                        i64::try_from(
                            values.iter().filter(|(_, _, degraded)| !*degraded).count() * 1_000_000
                                / values.len(),
                        )
                        .unwrap_or(1_000_000)
                    });
                    Ok(ObserverPolicyMetrics {
                        subject,
                        state,
                        sample_count: values.len(),
                        win_rate_ppm,
                        net_impact_ppm: compounded_ppm(&utilities),
                        stability_ppm,
                        exposure_ppm: policy_exposure_ppm(state),
                    })
                },
            )
            .collect::<Result<Vec<_>>>()?;

        Ok((
            ObserverLearningSummary {
                range_days: 30,
                attributed_utility_micros,
                attributed_utility_ppm: compounded_ppm(&current_utilities),
                lesson_candidates,
                lesson_candidates_delta: i64::try_from(lesson_candidates).unwrap_or(i64::MAX)
                    - i64::try_from(previous_lessons).unwrap_or(i64::MAX),
                policies_evolved,
                policies_evolved_delta: i64::try_from(policies_evolved).unwrap_or(i64::MAX)
                    - i64::try_from(previous_policies).unwrap_or(i64::MAX),
                impact_areas,
            },
            policy_metrics,
        ))
    }

    fn observer_outcome_baseline_equity(&self, outcome: &Outcome) -> Result<i64> {
        let schedule_artifact = self.store.artifact(&outcome.schedule.artifact_id)?;
        let schedule: OutcomeSchedule =
            serde_json::from_slice(&self.store.read_blob(&schedule_artifact.blob)?)?;
        let context_artifact = self
            .store
            .artifact(&schedule.execution_context.artifact_id)?;
        let context: ExecutionContext =
            serde_json::from_slice(&self.store.read_blob(&context_artifact.blob)?)?;
        let account_reference = context.account_snapshot.as_ref().ok_or_else(|| {
            DaemonError::Unavailable("Outcome has no baseline AccountSnapshot".to_owned())
        })?;
        let account_artifact = self.store.artifact(&account_reference.artifact_id)?;
        let account: AccountSnapshot =
            serde_json::from_slice(&self.store.read_blob(&account_artifact.blob)?)?;
        Ok(account.equity.0)
    }

    fn recent_typed_artifacts<T>(
        &self,
        kind: ArtifactKind,
        limit: usize,
    ) -> Result<Vec<(Artifact, T)>>
    where
        T: DeserializeOwned,
    {
        self.store
            .recent_artifacts_by_kind(kind, limit)?
            .into_iter()
            .map(|artifact| {
                let payload = serde_json::from_slice(&self.store.read_blob(&artifact.blob)?)?;
                Ok((artifact, payload))
            })
            .collect()
    }
}
